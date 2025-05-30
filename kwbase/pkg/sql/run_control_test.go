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

package sql_test

import (
	"context"
	gosql "database/sql"
	gosqldriver "database/sql/driver"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

func TestCancelSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const queryToCancel = "SELECT * FROM generate_series(1,20000000)"

	var conn1 *gosql.DB
	var conn2 *gosql.DB

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sem := make(chan struct{})
	errChan := make(chan error)

	go func() {
		sem <- struct{}{}
		rows, err := conn2.Query(queryToCancel)
		if err != nil {
			errChan <- err
			return
		}
		for rows.Next() {
		}
		if err = rows.Err(); err != nil {
			errChan <- err
		}
	}()

	<-sem
	time.Sleep(time.Second * 2)

	const cancelQuery = "CANCEL QUERIES SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE node_id = 2"

	if _, err := conn1.Exec(cancelQuery); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errChan:
		if !isClientsideQueryCanceledErr(err) {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("no error received from query supposed to be canceled")
	}

}

// TestCancelDistSQLQuery runs a distsql query and cancels it randomly at
// various points of execution.
func TestCancelDistSQLQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const queryToCancel = "SELECT * FROM nums ORDER BY num"
	cancelQuery := fmt.Sprintf("CANCEL QUERIES SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query = '%s'", queryToCancel)

	// conn1 is used for the query above. conn2 is solely for the CANCEL statement.
	var conn1 *gosql.DB
	var conn2 *gosql.DB

	var queryLatency *time.Duration
	sem := make(chan struct{}, 1)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(_ context.Context, stmt string) {
							if strings.HasPrefix(stmt, queryToCancel) {
								// Wait for the race to start.
								<-sem
							} else if strings.HasPrefix(stmt, cancelQuery) {
								// Signal to start the race.
								sleepTime := time.Duration(rng.Int63n(int64(*queryLatency)))
								sem <- struct{}{}
								time.Sleep(sleepTime)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sqlutils.CreateTable(t, conn1, "nums", "num INT", 0, nil)
	if _, err := conn1.Exec("INSERT INTO nums SELECT generate_series(1,100)"); err != nil {
		t.Fatal(err)
	}

	if _, err := conn1.Exec("ALTER TABLE nums SPLIT AT VALUES (50)"); err != nil {
		t.Fatal(err)
	}

	// Make the second node the leaseholder for the first range to distribute the
	// query. This may have to retry if the second store's descriptor has not yet
	// propagated to the first store's StorePool.
	testutils.SucceedsSoon(t, func() error {
		_, err := conn1.Exec(fmt.Sprintf(
			"ALTER TABLE nums EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1)",
			tc.Server(1).GetFirstStoreID()))
		return err
	})

	// Run queryToCancel to be able to get an estimate of how long it should
	// take. The goroutine in charge of cancellation will sleep a random
	// amount of time within this bound. Signal sem so that it can run
	// unhindered.
	sem <- struct{}{}
	start := timeutil.Now()
	if _, err := conn1.Exec(queryToCancel); err != nil {
		t.Fatal(err)
	}
	execTime := timeutil.Since(start)
	queryLatency = &execTime

	errChan := make(chan error)
	go func() {
		_, err := conn1.Exec(queryToCancel)
		errChan <- err
	}()
	_, err := conn2.Exec(cancelQuery)
	if err != nil && !testutils.IsError(err, "query ID") {
		t.Fatal(err)
	}

	err = <-errChan
	if err == nil {
		// A successful cancellation does not imply that the query was canceled.
		return
	}
	if !isClientsideQueryCanceledErr(err) {
		t.Fatalf("expected error with specific error code, got: %s", err)
	}
}

func testCancelSession(t *testing.T, hasActiveSession bool) {
	ctx := context.TODO()

	numNodes := 2
	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	// Since we're testing session cancellation, use single connections instead of
	// connection pools.
	var err error
	conn1, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := tc.ServerConn(1).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for node 2 to know about both sessions.
	if err := retry.ForDuration(10*time.Second, func() error {
		rows, err := conn2.QueryContext(ctx, "SELECT * FROM [SHOW CLUSTER SESSIONS] WHERE application_name NOT LIKE '$%'")
		if err != nil {
			return err
		}

		m, err := sqlutils.RowsToStrMatrix(rows)
		if err != nil {
			return err
		}

		if numRows := len(m); numRows != numNodes {
			return fmt.Errorf("expected %d sessions but found %d\n%s",
				numNodes, numRows, sqlutils.MatrixToStr(m))
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Get node 1's session ID now, so that we don't need to serialize the session
	// later and race with the active query's type-checking and name resolution.
	rows, err := conn1.QueryContext(
		ctx, "SELECT session_id FROM [SHOW LOCAL SESSIONS]",
	)
	if err != nil {
		t.Fatal(err)
	}

	var id string
	if !rows.Next() {
		t.Fatal("no sessions on node 1")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Now that we've obtained the session ID, query planning won't race with
	// session serialization, so we can kick it off now.
	errChan := make(chan error, 1)
	if hasActiveSession {
		go func() {
			var err error
			_, err = conn1.ExecContext(ctx, "SELECT pg_sleep(1000000)")
			errChan <- err
		}()
	}

	// Cancel the session on node 1.
	if _, err = conn2.ExecContext(ctx, fmt.Sprintf("CANCEL SESSION '%s'", id)); err != nil {
		t.Fatal(err)
	}

	if hasActiveSession {
		// Verify that the query was canceled because the session closed.
		err = <-errChan
	} else {
		// Verify that the connection is closed.
		_, err = conn1.ExecContext(ctx, "SELECT 1")
	}

	if err != gosqldriver.ErrBadConn {
		t.Fatalf("session not canceled; actual error: %s", err)
	}
}

func TestCancelMultipleSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	// Open two connections on node 1.
	var conns [2]*gosql.Conn
	for i := 0; i < 2; i++ {
		var err error
		if conns[i], err = tc.ServerConn(0).Conn(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := conns[i].ExecContext(ctx, "SET application_name = 'killme'"); err != nil {
			t.Fatal(err)
		}
	}
	// Open a control connection on node 2.
	ctlconn, err := tc.ServerConn(1).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cancel the sessions on node 1.
	if _, err = ctlconn.ExecContext(ctx,
		`CANCEL SESSIONS SELECT session_id FROM [SHOW CLUSTER SESSIONS] WHERE application_name = 'killme'`,
	); err != nil {
		t.Fatal(err)
	}

	// Verify that the connections on node 1 are closed.
	for i := 0; i < 2; i++ {
		_, err := conns[i].ExecContext(ctx, "SELECT 1")
		if err != gosqldriver.ErrBadConn {
			t.Fatalf("session %d not canceled; actual error: %s", i, err)
		}
	}
}

func TestIdleCancelSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCancelSession(t, false /* hasActiveSession */)
}

func TestActiveCancelSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCancelSession(t, true /* hasActiveSession */)
}

func TestCancelIfExists(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := serverutils.StartTestCluster(t, 1, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	conn := tc.ServerConn(0)

	var err error

	// Try to cancel a query that doesn't exist.
	_, err = conn.Exec("CANCEL QUERY IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}

	// Try to cancel a session that doesn't exist.
	_, err = conn.Exec("CANCEL SESSION IF EXISTS '00000000000000000000000000000001'")
	if err != nil {
		t.Fatal(err)
	}
}

func isClientsideQueryCanceledErr(err error) bool {
	if pqErr, ok := errors.UnwrapAll(err).(*pq.Error); ok {
		return pqErr.Code == pgcode.QueryCanceled
	}
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled
}

func TestIdleInSessionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	numNodes := 1
	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	var err error
	conn, err := tc.ServerConn(0).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, `SET idle_in_session_timeout = '5s'`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// Make sure executing a statement resets the idle timer.
	_, err = conn.ExecContext(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	// Make sure executing BEGIN resets the idle timer.
	// BEGIN is the only statement that is not run by execStmtInOpenState.
	_, err = conn.ExecContext(ctx, `BEGIN`)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	// The connection should still be alive.
	err = conn.PingContext(ctx)
	if err != nil {
		t.Fatalf("expected the connection to be alive but the connection"+
			"is dead, %v", err)
	}

	time.Sleep(5 * time.Second)
	err = conn.PingContext(ctx)

	if err == nil {
		t.Fatal("expected the connection to be killed " +
			"but the connection is still alive")
	}
}
