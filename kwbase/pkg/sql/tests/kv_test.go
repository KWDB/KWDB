// Copyright 2016 The Cockroach Authors.
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

package tests_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	kv2 "gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"github.com/pkg/errors"
)

type kvInterface interface {
	Insert(rows, run int) error
	Update(rows, run int) error
	Delete(rows, run int) error
	Scan(rows, run int) error

	prep(rows int, initData bool) error
	done()
}

// kvNative uses the native client package to implement kvInterface.
type kvNative struct {
	db     *kv2.DB
	epoch  int
	prefix string
	doneFn func()
}

func newKVNative(b *testing.B) kvInterface {
	s, _, db := serverutils.StartServer(b, base.TestServerArgs{})

	// Note that using the local client.DB isn't a strictly fair
	// comparison with SQL as we want these client requests to be sent
	// over the network.
	return &kvNative{
		db: db,
		doneFn: func() {
			s.Stopper().Stop(context.TODO())
		},
	}
}

func (kv *kvNative) Insert(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(context.TODO(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) Update(rows, run int) error {
	perm := rand.Perm(rows)
	err := kv.db.Txn(context.TODO(), func(ctx context.Context, txn *kv2.Txn) error {
		// Read all values in a batch.
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Get(fmt.Sprintf("%s%08d", kv.prefix, perm[i]))
		}
		if err := txn.Run(ctx, b); err != nil {
			return err
		}
		// Now add one to each value and add as puts to write batch.
		wb := txn.NewBatch()
		for i, result := range b.Results {
			v := result.Rows[0].ValueInt()
			wb.Put(fmt.Sprintf("%s%08d", kv.prefix, perm[i]), v+1)
		}
		return txn.CommitInBatch(ctx, wb)
	})
	return err
}

func (kv *kvNative) Delete(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(context.TODO(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Del(fmt.Sprintf("%s%08d", kv.prefix, i))
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) Scan(rows, run int) error {
	var kvs []kv2.KeyValue
	err := kv.db.Txn(context.TODO(), func(ctx context.Context, txn *kv2.Txn) error {
		var err error
		kvs, err = txn.Scan(ctx, fmt.Sprintf("%s%08d", kv.prefix, 0), fmt.Sprintf("%s%08d", kv.prefix, rows), int64(rows))
		return err
	})
	if len(kvs) != rows {
		return errors.Errorf("expected %d rows; got %d", rows, len(kvs))
	}
	return err
}

func (kv *kvNative) prep(rows int, initData bool) error {
	kv.epoch++
	kv.prefix = fmt.Sprintf("%d/", kv.epoch)
	if !initData {
		return nil
	}
	err := kv.db.Txn(context.TODO(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) done() {
	kv.doneFn()
}

// kvSQL is a SQL-based implementation of the KV interface.
type kvSQL struct {
	db     *gosql.DB
	buf    bytes.Buffer
	doneFn func()
}

func newKVSQL(b *testing.B) kvInterface {
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "bench"})

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	kv := &kvSQL{}
	kv.db = db
	kv.doneFn = func() {
		s.Stopper().Stop(context.TODO())
	}
	return kv
}

func (kv *kvSQL) Insert(rows, run int) error {
	firstRow := rows * run
	defer kv.buf.Reset()
	kv.buf.WriteString(`INSERT INTO bench.kv VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, "('%08d', %d)", i+firstRow, i)
	}
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) Update(rows, run int) error {
	perm := rand.Perm(rows)
	defer kv.buf.Reset()
	kv.buf.WriteString(`UPDATE bench.kv SET v = v + 1 WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, `'%08d'`, perm[j])
	}
	kv.buf.WriteString(`)`)
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) Delete(rows, run int) error {
	firstRow := rows * run
	defer kv.buf.Reset()
	kv.buf.WriteString(`DELETE FROM bench.kv WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, `'%08d'`, j+firstRow)
	}
	kv.buf.WriteString(`)`)
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) Scan(count, run int) error {
	rows, err := kv.db.Query(fmt.Sprintf("SELECT * FROM bench.kv LIMIT %d", count))
	if err != nil {
		return err
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if n != count {
		return errors.Errorf("unexpected result count: %d (expected %d)", n, count)
	}
	return nil
}

func (kv *kvSQL) prep(rows int, initData bool) error {
	if _, err := kv.db.Exec(`DROP TABLE IF EXISTS bench.kv`); err != nil {
		return err
	}
	schema := `
CREATE TABLE IF NOT EXISTS bench.kv (
  k STRING PRIMARY KEY,
  v INT,
  FAMILY (k, v)
)
`
	if _, err := kv.db.Exec(schema); err != nil {
		return err
	}
	if !initData {
		return nil
	}
	defer kv.buf.Reset()
	kv.buf.WriteString(`INSERT INTO bench.kv VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, "('%08d', %d)", i, i)
	}
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) done() {
	kv.doneFn()
}

func BenchmarkKV(b *testing.B) {
	for i, opFn := range []func(kvInterface, int, int) error{
		kvInterface.Insert,
		kvInterface.Update,
		kvInterface.Delete,
		kvInterface.Scan,
	} {
		opName := runtime.FuncForPC(reflect.ValueOf(opFn).Pointer()).Name()
		opName = strings.TrimPrefix(opName, "gitee.com/kwbasedb/kwbase/pkg/sql/tests_test.kvInterface.")
		b.Run(opName, func(b *testing.B) {
			for _, kvFn := range []func(*testing.B) kvInterface{
				newKVNative,
				newKVSQL,
			} {
				kvTyp := runtime.FuncForPC(reflect.ValueOf(kvFn).Pointer()).Name()
				kvTyp = strings.TrimPrefix(kvTyp, "gitee.com/kwbasedb/kwbase/pkg/sql/tests_test.newKV")
				b.Run(kvTyp, func(b *testing.B) {
					for _, rows := range []int{1, 10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
							kv := kvFn(b)
							defer kv.done()

							if err := kv.prep(rows, i != 0 /* Insert */ && i != 2 /* Delete */); err != nil {
								b.Fatal(err)
							}
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if err := opFn(kv, rows, i); err != nil {
									b.Fatal(err)
								}
							}
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}
