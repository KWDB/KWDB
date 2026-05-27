// Package example demonstrates a server-based unit test with TestMain.
// This is the reference example for Template B.
//
// Prerequisites in this package:
//   - main_test.go with TestMain registering serverutils factories
//   - go:generate line for leaktest add-leaktest.sh
//
// Patterns shown:
//   - serverutils.StartServer to launch an in-process server
//   - database/sql connection via the returned *gosql.DB handle
//   - require/assert from testify
//   - Stopper lifecycle management
package example_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestServerQuery demonstrates starting a single-node test server and
// running SQL queries against it. This is the most common pattern for
// testing SQL behavior, API endpoints, and session-level features.
func TestServerQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start an in-process test server. base.TestServerArgs{} uses defaults.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// db is a standard *gosql.DB — use it exactly like a real database connection.
	_, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, name STRING)")
	require.NoError(t, err, "CREATE TABLE should succeed")

	_, err = db.Exec("INSERT INTO t VALUES (1, 'hello')")
	require.NoError(t, err, "INSERT should succeed")

	row := db.QueryRow("SELECT name FROM t WHERE id = 1")
	var name string
	err = row.Scan(&name)
	require.NoError(t, err, "SELECT should succeed")
	require.Equal(t, "hello", name)
}

// TestServerWithArgs demonstrates custom server configuration via
// base.TestServerArgs. Use this when the test needs specific settings,
// memory limits, or feature flags.
func TestServerWithArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Override defaults as needed:
		// Knobs: base.TestingKnobs{...},  // inject test hooks
		// Insecure: true,                   // skip TLS
	})
	defer s.Stopper().Stop(context.Background())

	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 1, result)
}

// TestServerMultiple demonstrates using multiple independent test servers
// in the same test function. Each gets its own stopper.
func TestServerMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s1, db1, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s1.Stopper().Stop(context.Background())

	s2, db2, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s2.Stopper().Stop(context.Background())

	var v1, v2 int
	require.NoError(t, db1.QueryRow("SELECT 1").Scan(&v1))
	require.NoError(t, db2.QueryRow("SELECT 1").Scan(&v2))
}
