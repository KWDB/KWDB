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

package sql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"golang.org/x/sync/errgroup"
)

func TestUpsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This filter increments scans and endTxn for every ScanRequest and
	// EndTxnRequest that hits user table data.
	var scans uint64
	var endTxn uint64
	filter := func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if bytes.Compare(filterArgs.Req.Header().Key, keys.UserTableDataMin) >= 0 {
			switch filterArgs.Req.Method() {
			case roachpb.Scan:
				atomic.AddUint64(&scans, 1)
			case roachpb.EndTxn:
				if filterArgs.Hdr.Txn.Status == roachpb.STAGING {
					// Ignore async explicit commits.
					return nil
				}
				atomic.AddUint64(&endTxn, 1)
			}
		}
		return nil
	}

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			EvalKnobs: storagebase.BatchEvalTestingKnobs{
				TestingEvalFilter: filter,
			},
		}},
	})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.kv (k INT PRIMARY KEY, v INT)`)

	// This should hit the fast path.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This could hit the fast path, but doesn't right now because of #14482.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `INSERT INTO d.kv VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET v=excluded.v`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should not hit the fast path because it doesn't set every column.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv (k) VALUES (1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should hit the fast path, but won't be a 1PC because of the explicit
	// transaction.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPSERT INTO d.kv VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 1 {
		t.Errorf("expected 1 end-txn (no 1PC) but got %d", s)
	}

	// This should not hit the fast path because kv has a secondary index.
	sqlDB.Exec(t, `CREATE INDEX vidx ON d.kv (v)`)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}
}

func TestConcurrentUpsert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)

	testCases := []struct {
		name       string
		updateStmt string
	}{
		// Upsert case.
		{
			name:       "upsert",
			updateStmt: `UPSERT INTO d.t VALUES (1, $1)`,
		},
		// Update case.
		{
			name:       "update",
			updateStmt: `UPDATE d.t SET b = $1 WHERE a = 1`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			g, ctx := errgroup.WithContext(context.Background())
			for i := 0; i < 2; i++ {
				g.Go(func() error {
					for j := 0; j < 100; j++ {
						if _, err := sqlDB.DB.ExecContext(ctx, test.updateStmt, j); err != nil {
							return err
						}
					}
					return nil
				})
			}
			// We select on both the primary key and the secondary
			// index to highlight the lost update anomaly, which used
			// to occur on 1PC snapshot-isolation upserts (and updates).
			// See #14099.
			if err := g.Wait(); err != nil {
				t.Errorf(`%+v
SELECT * FROM d.t@primary = %s
SELECT * FROM d.t@b_idx   = %s
`,
					err,
					sqlDB.QueryStr(t, `SELECT * FROM d.t@primary`),
					sqlDB.QueryStr(t, `SELECT * FROM d.t@b_idx`),
				)
			}
		})
	}
}
