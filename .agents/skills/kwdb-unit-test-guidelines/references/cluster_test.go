// Package example demonstrates a multi-node cluster test.
// This is the reference example for Template C.
//
// Prerequisites: same main_test.go as Template B with TestMain registering
// both serverutils.InitTestServerFactory and serverutils.InitTestClusterFactory.
//
// Patterns shown:
//   - testcluster.StartTestCluster to launch multiple in-process servers
//   - tc.ServerConn(n) to get per-node database connections
//   - Stopper lifecycle: Stop before inspecting state to avoid races
//   - require/assert from testify
package example_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestClusterBasic demonstrates starting a 3-node cluster and running SQL
// against it. The cluster runs entirely in-process — no external binaries.
func TestClusterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start a 3-node cluster.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	// Get a SQL connection to node 0.
	db := tc.ServerConn(0)

	_, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, val STRING)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO t VALUES (1, 'distributed')")
	require.NoError(t, err)

	// Verify the data is visible from another node.
	db2 := tc.ServerConn(1)
	var val string
	err = db2.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "distributed", val)
}

// TestClusterSQLRunner demonstrates using sqlutils.SQLRunner to simplify
// SQL-heavy tests. SQLRunner handles error checking and result verification.
func TestClusterSQLRunner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Exec runs statements and fails the test on error.
	r.Exec(t, "CREATE TABLE items (id INT PRIMARY KEY, count INT)")

	// Check that rows match expectations.
	r.CheckQueryResults(t, "SELECT count(*) FROM items", [][]string{{"0"}})

	r.Exec(t, "INSERT INTO items VALUES (1, 10), (2, 20)")

	r.CheckQueryResults(t, "SELECT count FROM items ORDER BY id", [][]string{
		{"10"},
		{"20"},
	})
}

// TestClusterWithArgs demonstrates passing server-level arguments to each
// node in the cluster via base.TestClusterArgs.ServerArgs.
func TestClusterWithArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Custom configuration applied to all nodes.
			// See base.TestServerArgs for available fields.
		},
	})
	defer tc.Stopper().Stop(context.Background())

	// Count the nodes visible in the cluster.
	rows, err := tc.ServerConn(0).Query(
		"SELECT node_id FROM crdb_internal.gossip_nodes",
	)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 2, count, "should see 2 nodes in gossip")
}
