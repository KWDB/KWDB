// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

// cmp-protocol connects to postgres and kwbase servers and compares
// the binary and text pgwire encodings of SQL statements. Statements can
// be specified in arguments (./cmp-protocol "select 1" "select 2") or will
// be generated randomly until a difference is found.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/cmp-protocol/pgconnect"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/pkg/errors"
)

var (
	pgAddr = flag.String("pg", "localhost:5432", "postgres address")
	pgUser = flag.String("pg-user", "postgres", "postgres user")
	crAddr = flag.String("cr", "localhost:26257", "kwbase address")
	crUser = flag.String("cr-user", "root", "kwbase user")
)

func main() {
	flag.Parse()

	stmtCh := make(chan string)
	if args := os.Args[1:]; len(args) > 0 {
		go func() {
			for _, arg := range os.Args[1:] {
				stmtCh <- arg
			}
			close(stmtCh)
		}()
	} else {
		go func() {
			rng, _ := randutil.NewPseudoRand()
			for {
				typ := sqlbase.RandType(rng)
				sem := typ.Family()
				switch sem {
				case types.DecimalFamily, // trailing zeros differ, ok
					types.CollatedStringFamily, // pg complains about utf8
					types.OidFamily,            // our 8-byte ints are usually out of range for pg
					types.FloatFamily,          // slight rounding differences at the end
					types.TimestampTZFamily,    // slight timezone differences
					types.UnknownFamily,
					// tested manually below:
					types.ArrayFamily,
					types.TupleFamily:
					continue
				}
				datum := sqlbase.RandDatum(rng, typ, false /* null ok */)
				if datum == tree.DNull {
					continue
				}
				for _, format := range []string{
					"SELECT %s::%s;",
					"SELECT ARRAY[%s::%s];",
					"SELECT (%s::%s, NULL);",
				} {
					input := fmt.Sprintf(format, datum, pgTypeName(sem))
					stmtCh <- input
					fmt.Printf("\nTYP: %v, DATUM: %v\n", sem, datum)
				}
			}
		}()
	}

	for input := range stmtCh {
		fmt.Println("INPUT", input)
		if err := compare(os.Stdout, input, *pgAddr, *crAddr, *pgUser, *crUser); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", input)
			fmt.Fprintf(os.Stderr, "%v\n", err)
		} else {
			fmt.Fprintln(os.Stderr, "OK", input)
		}
	}
}

func pgTypeName(sem types.Family) string {
	switch sem {
	case types.StringFamily:
		return "TEXT"
	case types.BytesFamily:
		return "BYTEA"
	case types.IntFamily:
		return "INT8"
	default:
		return sem.String()
	}
}

func compare(w io.Writer, input, pgAddr, crAddr, pgUser, crUser string) error {
	ctx := context.Background()
	for _, code := range []pgwirebase.FormatCode{
		pgwirebase.FormatText,
		pgwirebase.FormatBinary,
	} {
		// https://gitee.com/kwbasedb/kwbase/issues/31847
		if code == pgwirebase.FormatBinary && strings.HasPrefix(input, "SELECT (") {
			continue
		}
		results := map[string][]byte{}
		for _, s := range []struct {
			user string
			addr string
		}{
			{user: pgUser, addr: pgAddr},
			{user: crUser, addr: crAddr},
		} {
			user := s.user
			addr := s.addr
			res, err := pgconnect.Connect(ctx, input, addr, user, code)
			if err != nil {
				return errors.Wrapf(err, "addr: %s, code: %s", addr, code)
			}
			fmt.Printf("INPUT: %s, ADDR: %s, CODE: %s, res: %q, res: %v\n", input, addr, code, res, res)
			for k, v := range results {
				if !bytes.Equal(res, v) {
					return errors.Errorf("format: %s\naddr: %s\nstr: %q\nbytes: %[3]v\n!=\naddr: %s\nstr: %q\nbytes: %[5]v\n", code, k, v, addr, res)
				}
			}
			results[addr] = res
		}
	}
	return nil
}
