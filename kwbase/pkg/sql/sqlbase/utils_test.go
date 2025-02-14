// Copyright 2017 The Cockroach Authors.
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

package sqlbase

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/cockroachdb/apd"
)

var tableNames = map[string]bool{
	"parent1":     true,
	"child1":      true,
	"grandchild1": true,
	"child2":      true,
	"parent2":     true,
}

// This file contains test helper and utility functions for sqlbase.

// EncodeTestKey takes the short format representation of a key and transforms
// it into an actual roachpb.Key. Refer to ShortToLongKeyFmt for more info. on
// the short format.
// All tokens are interpreted as UVarint (ascending) unless they satisfy:
//    - '#' - interleaved sentinel
//    - 's' first byte - string/bytes (ascending)
//    - 'd' first byte - decimal (ascending)
//    - NULLASC, NULLDESC, NOTNULLASC, NOTNULLDESC
//    - PrefixEnd
func EncodeTestKey(tb testing.TB, kvDB *kv.DB, keyStr string) roachpb.Key {
	var key []byte
	tokens := strings.Split(keyStr, "/")
	if tokens[0] != "" {
		panic("missing '/' token at the beginning of long format")
	}

	// Omit the first empty string.
	tokens = tokens[1:]

	for _, tok := range tokens {
		if tok == "PrefixEnd" {
			key = roachpb.Key(key).PrefixEnd()
			continue
		}

		// Encode the table ID if the token is a table name.
		if tableNames[tok] {
			desc := GetTableDescriptor(kvDB, sqlutils.TestDB, tok)
			key = encoding.EncodeUvarintAscending(key, uint64(desc.ID))
			continue
		}

		switch tok[0] {
		case 's':
			key = encoding.EncodeStringAscending(key, tok[1:])
			continue
		case 'd':
			dec, cond, err := apd.NewFromString(tok[1:])
			if err != nil {
				tb.Fatal(err)
			}
			if cond.Any() {
				tb.Fatalf("encountered condition %s when parsing decimal", cond.String())
			}
			key = encoding.EncodeDecimalAscending(key, dec)
			continue
		}

		if tok == "NULLASC" {
			key = encoding.EncodeNullAscending(key)
			continue
		}

		if tok == "NOTNULLASC" {
			key = encoding.EncodeNotNullAscending(key)
			continue
		}

		if tok == "NULLDESC" {
			key = encoding.EncodeNullDescending(key)
			continue
		}

		// We make a distinction between this and the interleave
		// sentinel below.
		if tok == "NOTNULLDESC" {
			key = encoding.EncodeNotNullDescending(key)
			continue
		}

		// Interleaved sentinel.
		if tok == "#" {
			key = encoding.EncodeNotNullDescending(key)
			continue
		}

		// Assume any other value is an unsigned integer.
		tokInt, err := strconv.ParseInt(tok, 10, 64)
		if err != nil {
			tb.Fatal(err)
		}
		key = encoding.EncodeVarintAscending(key, tokInt)
	}

	return key
}

// See CreateTestInterleavedHierarchy for the longest chain used for the short
// format.
var shortFormTables = [3]string{"parent1", "child1", "grandchild1"}

// ShortToLongKeyFmt converts the short key format preferred in test cases
//    /1/#/3/4
// to its long form required by parseTestkey
//    parent1/1/1/#/child1/1/3/4
// The short key format can end in an interleave sentinel '#' (i.e. after
// TightenEndKey).
// The short key format can also be "/" or end in "#/" which will append
// the parent's table/index info. without a trailing index column value.
func ShortToLongKeyFmt(short string) string {
	tableOrder := shortFormTables
	curTableIdx := 0

	var long []byte
	tokens := strings.Split(short, "/")
	// Verify short format starts with '/'.
	if tokens[0] != "" {
		panic("missing '/' token at the beginning of short format")
	}
	// Skip the first element since short format has starting '/'.
	tokens = tokens[1:]

	// Always append parent1.
	long = append(long, []byte(fmt.Sprintf("/%s/1/", tableOrder[curTableIdx]))...)
	curTableIdx++

	for i, tok := range tokens {
		// Permits ".../#/" to append table name without a value
		if tok == "" {
			continue
		}

		if tok == "#" {
			long = append(long, []byte("#/")...)
			// It's possible for the short-format to end with a #.
			if i == len(tokens)-1 {
				break
			}

			// New interleaved table and primary keys follow.
			if curTableIdx >= len(tableOrder) {
				panic("too many '#' tokens specified in short format (max 2 for child1 and 3 for grandchild1)")
			}

			long = append(long, []byte(fmt.Sprintf("%s/1/", tableOrder[curTableIdx]))...)
			curTableIdx++

			continue
		}

		long = append(long, []byte(fmt.Sprintf("%s/", tok))...)
	}

	// Remove the last '/'.
	return string(long[:len(long)-1])
}
