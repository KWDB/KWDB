package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDropAuditIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a test table
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_db.test_table (id INT PRIMARY KEY)`)
	require.NoError(t, err)

	// create audit
	_, err = db.Exec(`CREATE AUDIT my_audit ON TABLE test_db.test_table FOR ALL TO ALL`)
	require.NoError(t, err)

	// drop audit
	_, err = db.Exec(`DROP AUDIT my_audit`)
	require.NoError(t, err)

	// drop non-existent audit
	_, err = db.Exec(`DROP AUDIT non_existent_audit`)
	require.Error(t, err)

	// drop non-existent audit with IF EXISTS
	_, err = db.Exec(`DROP AUDIT IF EXISTS non_existent_audit`)
	require.NoError(t, err)
}
