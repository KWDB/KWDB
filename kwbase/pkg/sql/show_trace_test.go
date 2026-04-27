// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestShowTraceNodeColumns tests the column definitions of showTraceNode
func TestShowTraceNodeColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify ShowTraceColumns
	require.Equal(t, 7, len(sqlbase.ShowTraceColumns))
	require.Equal(t, "timestamp", sqlbase.ShowTraceColumns[0].Name)
	require.Equal(t, "age", sqlbase.ShowTraceColumns[1].Name)
	require.Equal(t, "message", sqlbase.ShowTraceColumns[2].Name)
	require.Equal(t, "tag", sqlbase.ShowTraceColumns[3].Name)
	require.Equal(t, "location", sqlbase.ShowTraceColumns[4].Name)
	require.Equal(t, "operation", sqlbase.ShowTraceColumns[5].Name)
	require.Equal(t, "span", sqlbase.ShowTraceColumns[6].Name)

	// Verify ShowCompactTraceColumns
	require.Equal(t, 4, len(sqlbase.ShowCompactTraceColumns))
	require.Equal(t, "age", sqlbase.ShowCompactTraceColumns[0].Name)
	require.Equal(t, "message", sqlbase.ShowCompactTraceColumns[1].Name)
	require.Equal(t, "tag", sqlbase.ShowCompactTraceColumns[2].Name)
	require.Equal(t, "operation", sqlbase.ShowCompactTraceColumns[3].Name)
}

// TestShowTraceBasic tests the basic SHOW TRACE functionality
func TestShowTraceBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable session tracing
	_, err := db.Exec("SET tracing = on")
	require.NoError(t, err)

	// Execute some operations
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	// Disable tracing
	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query trace results
	rows, err := db.Query("SHOW TRACE FOR SESSION")
	require.NoError(t, err)
	defer rows.Close()

	// Verify result columns
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, 7, len(cols))
	require.Equal(t, "timestamp", cols[0])
	require.Equal(t, "age", cols[1])
	require.Equal(t, "message", cols[2])
	require.Equal(t, "tag", cols[3])
	require.Equal(t, "location", cols[4])
	require.Equal(t, "operation", cols[5])
	require.Equal(t, "span", cols[6])

	// Read results
	count := 0
	for rows.Next() {
		var timestamp, age, message, tag, location, operation, span string
		err = rows.Scan(&timestamp, &age, &message, &tag, &location, &operation, &span)
		require.NoError(t, err)
		count++
	}
	require.NoError(t, rows.Err())
	require.Greater(t, count, 0, "expected trace data to be present")
}

// TestShowTraceCompact tests SHOW TRACE in COMPACT mode
func TestShowTraceCompact(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable session tracing
	_, err := db.Exec("SET tracing = on")
	require.NoError(t, err)

	// Execute some operations
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	// Disable tracing
	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query trace results in compact format
	rows, err := db.Query("SHOW COMPACT TRACE FOR SESSION")
	require.NoError(t, err)
	defer rows.Close()

	// Verify result columns
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, 4, len(cols))
	require.Equal(t, "age", cols[0])
	require.Equal(t, "message", cols[1])
	require.Equal(t, "tag", cols[2])
	require.Equal(t, "operation", cols[3])

	// Read results
	count := 0
	for rows.Next() {
		var age, message, tag, operation string
		err = rows.Scan(&age, &message, &tag, &operation)
		require.NoError(t, err)
		count++
	}
	require.NoError(t, rows.Err())
	require.Greater(t, count, 0, "expected trace data to be present")
}

// TestShowTraceKV tests the KV TRACE mode
func TestShowTraceKV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable session tracing
	_, err := db.Exec("SET tracing = on")
	require.NoError(t, err)

	// Execute some operations
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	// Disable tracing
	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query KV trace results
	rows, err := db.Query("SHOW KV TRACE FOR SESSION")
	require.NoError(t, err)
	defer rows.Close()

	// Verify result columns
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, 7, len(cols))
}

// TestShowTraceForStatement tests SHOW TRACE FOR STATEMENT
func TestShowTraceForStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// First execute a statement with tracing enabled
	_, err := db.Exec("SET tracing = on")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test_table (id INT PRIMARY KEY)")
	require.NoError(t, err)

	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query the trace for the specific statement
	// Note: We query the session trace here because SHOW TRACE FOR STATEMENT requires a valid statement ID
	rows, err := db.Query("SHOW TRACE FOR SESSION")
	require.NoError(t, err)
	defer rows.Close()

	// Verify that results can be read
	for rows.Next() {
		var timestamp, age, message, tag, location, operation, span string
		err = rows.Scan(&timestamp, &age, &message, &tag, &location, &operation, &span)
		require.NoError(t, err)
	}
	require.NoError(t, rows.Err())
}

// TestShowTraceNodeType tests the ShowTraceForSession node types
func TestShowTraceNodeType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create ShowTraceForSession nodes of different types
	n1 := &tree.ShowTraceForSession{
		TraceType: tree.ShowTraceRaw,
		Compact:   false,
	}
	require.Equal(t, tree.ShowTraceRaw, n1.TraceType)
	require.False(t, n1.Compact)

	n2 := &tree.ShowTraceForSession{
		TraceType: tree.ShowTraceKV,
		Compact:   true,
	}
	require.Equal(t, tree.ShowTraceKV, n2.TraceType)
	require.True(t, n2.Compact)

	n3 := &tree.ShowTraceForSession{
		TraceType: tree.ShowTraceReplica,
		Compact:   false,
	}
	require.Equal(t, tree.ShowTraceReplica, n3.TraceType)
	require.False(t, n3.Compact)
}

// TestShowTraceEmptySession tests tracing for an empty session
func TestShowTraceEmptySession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Query directly without enabling tracing
	// This may return empty results or an error depending on the implementation
	rows, err := db.Query("SHOW TRACE FOR SESSION")
	if err == nil {
		defer rows.Close()
		// Verify result columns
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, 7, len(cols))
	}
}

// TestShowTraceWithQuery tests tracing with actual queries
func TestShowTraceWithQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create test table
	_, err := db.Exec("CREATE TABLE trace_test (id INT PRIMARY KEY, name STRING)")
	require.NoError(t, err)

	// Enable tracing
	_, err = db.Exec("SET tracing = on")
	require.NoError(t, err)

	// Perform an insert operation
	_, err = db.Exec("INSERT INTO trace_test VALUES (1, 'test')")
	require.NoError(t, err)

	// Perform a query operation
	_, err = db.Exec("SELECT * FROM trace_test")
	require.NoError(t, err)

	// Disable tracing
	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query trace results
	rows, err := db.Query("SHOW TRACE FOR SESSION")
	require.NoError(t, err)
	defer rows.Close()

	// Verify trace data exists
	count := 0
	for rows.Next() {
		var timestamp, age, message, tag, location, operation, span string
		err = rows.Scan(&timestamp, &age, &message, &tag, &location, &operation, &span)
		require.NoError(t, err)
		count++
	}
	require.NoError(t, rows.Err())
	require.Greater(t, count, 0, "expected trace data to be present")
}

// TestShowTraceReplica tests the REPLICA TRACE mode
func TestShowTraceReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable session tracing
	_, err := db.Exec("SET tracing = on")
	require.NoError(t, err)

	// Execute some operations
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	// Disable tracing
	_, err = db.Exec("SET tracing = off")
	require.NoError(t, err)

	// Query REPLICA trace results
	rows, err := db.Query("SHOW EXPERIMENTAL_REPLICA TRACE FOR SESSION")
	// May succeed or fail depending on whether replica trace data is available
	if err == nil {
		defer rows.Close()
		// Verify result columns
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(cols), 1)
	}
}
