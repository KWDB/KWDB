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
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/cockroach-go/crdb"
)

type mtRow struct {
	val      int64
	sts      string
	node, tb int64
}

type mtRows []mtRow

func (r mtRows) Len() int {
	return len(r)
}

func (r mtRows) Less(i, j int) bool {
	if r[i].val == r[j].val {
		return r[i].sts < r[j].sts
	}
	return r[i].val < r[j].val
}

func (r mtRows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r mtRows) String() string {
	var buf bytes.Buffer
	for i, row := range r {
		prefix := "ok"
		if i > 0 && r.Less(i, i-1) {
			prefix = "!!"
		}
		fmt.Fprintf(&buf, "%s %+v\n", prefix, row)
	}
	return buf.String()
}

type mtClient struct {
	*gosql.DB
	ID int
}

// TestMonotonicInserts replicates the 'monotonic' test from the Jepsen
// CockroachDB test suite:
//   https://github.com/jepsen-io/jepsen/blob/master/kwbasedb/src/jepsen/kwbase/monotonic.clj
func TestMonotonicInserts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.Scope(t)
	defer s.Close(t)

	for _, distSQLMode := range []sessiondata.DistSQLExecMode{
		sessiondata.DistSQLOff, sessiondata.DistSQLOn,
	} {
		t.Run(fmt.Sprintf("distsql=%s", distSQLMode), func(t *testing.T) {
			testMonotonicInserts(t, distSQLMode)
		})
	}
}

func testMonotonicInserts(t *testing.T, distSQLMode sessiondata.DistSQLExecMode) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      base.TestServerArgs{},
		},
	)
	defer tc.Stopper().Stop(ctx)

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		sql.DistSQLClusterExecMode.Override(&st.SV, int64(distSQLMode))
		// Let transactions push immediately to detect deadlocks. The test creates a
		// large amount of contention and dependency cycles, and could take a long
		// time to complete without this.
		concurrency.LockTableDeadlockDetectionPushDelay.Override(&st.SV, 0)
	}

	var clients []mtClient
	for i := range tc.Conns {
		clients = append(clients, mtClient{ID: i, DB: tc.Conns[i]})
	}
	// We will insert into this table by selecting MAX(val) and increasing by
	// one and expect that val and sts (the commit timestamp) are both
	// simultaneously increasing.
	if _, err := clients[0].Exec(`
CREATE DATABASE mono;
CREATE TABLE IF NOT EXISTS mono.mono (val INT, sts STRING, node INT, tb INT);
INSERT INTO mono.mono VALUES(-1, '0', -1, -1)`); err != nil {
		t.Fatal(err)
	}

	var idGen uint64

	invoke := func(client mtClient) {
		logPrefix := fmt.Sprintf("%03d.%03d: ", atomic.AddUint64(&idGen, 1), client.ID)
		l := func(msg string, args ...interface{}) {
			log.Infof(ctx, logPrefix+msg, args...)
		}
		l("begin")
		defer l("done")

		var exRow, insRow mtRow
		var attempt int
		if err := kwdb.ExecuteTx(ctx, client.DB, nil, func(tx *gosql.Tx) error {
			attempt++
			l("attempt %d", attempt)
			if err := tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(
				&insRow.sts,
			); err != nil {
				l(err.Error())
				return err
			}

			l("read max val")
			if err := tx.QueryRow(`SELECT max(val) AS m FROM mono.mono`).Scan(
				&exRow.val,
			); err != nil {
				l(err.Error())
				return err
			}

			l("read max row for val=%d", exRow.val)
			if err := tx.QueryRow(`SELECT sts, node, tb FROM mono.mono WHERE val = $1`,
				exRow.val,
			).Scan(
				&exRow.sts, &exRow.node, &exRow.tb,
			); err != nil {
				l(err.Error())
				return err
			}

			l("insert")
			if err := tx.QueryRow(`
INSERT INTO mono.mono (val, sts, node, tb) VALUES($1, $2, $3, $4)
RETURNING val, sts, node, tb`,
				exRow.val+1, insRow.sts, client.ID, 0,
			).Scan(
				&insRow.val, &insRow.sts, &insRow.node, &insRow.tb,
			); err != nil {
				l(err.Error())
				return err
			}
			l("commit")
			return nil
		}); err != nil {
			t.Errorf("%T: %v", err, err)
		}
	}

	verify := func() {
		client := clients[0]
		var numDistinct int
		if err := client.QueryRow("SELECT count(DISTINCT(val)) FROM mono.mono").Scan(
			&numDistinct,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := client.Query("SELECT val, sts, node, tb FROM mono.mono ORDER BY val ASC, sts ASC")
		if err != nil {
			t.Fatal(err)
		}
		var results mtRows
		for rows.Next() {
			var row mtRow
			if err := rows.Scan(&row.val, &row.sts, &row.node, &row.tb); err != nil {
				t.Fatal(err)
			}
			results = append(results, row)
		}

		if !sort.IsSorted(results) {
			t.Errorf("results are not sorted:\n%s", results)
		}

		if numDistinct != len(results) {
			t.Errorf("'val' column is not unique: %d results, but %d distinct:\n%s",
				len(results), numDistinct, results)
		}
	}

	sem := make(chan struct{}, 2*len(tc.Conns))
	timer := time.After(5 * time.Second)

	defer verify()
	defer func() {
		// Now that consuming has stopped, fill up the semaphore (i.e. wait for
		// still-running goroutines to stop)
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	}()

	for {
		select {
		case sem <- struct{}{}:
		case <-tc.Stopper().ShouldStop():
			return
		case <-timer:
			return
		}
		go func(client mtClient) {
			invoke(client)
			<-sem
		}(clients[rand.Intn(len(clients))])
	}
}
