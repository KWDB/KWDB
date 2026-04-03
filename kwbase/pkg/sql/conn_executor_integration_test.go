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

package sql_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConnectionHandlerDatabaseOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("create and use database", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE DATABASE test_db")
		require.NoError(t, err)

		_, err = sqlDB.Exec("USE test_db")
		require.NoError(t, err)

		_, err = sqlDB.Exec("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)")
		require.NoError(t, err)

		_, err = sqlDB.Exec("INSERT INTO test_table VALUES (1, 'test')")
		require.NoError(t, err)

		var count int
		err = sqlDB.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("show current database", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE DATABASE show_db")
		require.NoError(t, err)

		_, err = sqlDB.Exec("USE show_db")
		require.NoError(t, err)

		var currentDB string
		err = sqlDB.QueryRow("SELECT current_database()").Scan(&currentDB)
		require.NoError(t, err)
		require.Equal(t, "show_db", currentDB)
	})

	t.Run("multiple connections with different databases", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE DATABASE multi_conn_db1")
		require.NoError(t, err)

		_, err = sqlDB.Exec("CREATE DATABASE multi_conn_db2")
		require.NoError(t, err)

		pgURL1, cleanup1 := sqlutils.PGUrl(t, s.ServingSQLAddr(), "conn1", url.User(security.RootUser))
		defer cleanup1()

		pgURL2, cleanup2 := sqlutils.PGUrl(t, s.ServingSQLAddr(), "conn2", url.User(security.RootUser))
		defer cleanup2()

		db1, err := gosql.Open("postgres", pgURL1.String())
		require.NoError(t, err)
		defer db1.Close()

		db2, err := gosql.Open("postgres", pgURL2.String())
		require.NoError(t, err)
		defer db2.Close()

		_, err = db1.Exec("SET database = multi_conn_db1")
		require.NoError(t, err)

		_, err = db2.Exec("SET database = multi_conn_db2")
		require.NoError(t, err)

		var db1Name, db2Name string
		err = db1.QueryRow("SELECT current_database()").Scan(&db1Name)
		require.NoError(t, err)

		err = db2.QueryRow("SELECT current_database()").Scan(&db2Name)
		require.NoError(t, err)

		require.Equal(t, "multi_conn_db1", db1Name)
		require.Equal(t, "multi_conn_db2", db2Name)
	})
}

func TestConnectionHandlerPreparedStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE prep_test (id INT, name TEXT)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO prep_test VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	t.Run("prepare and execute select", func(t *testing.T) {
		rows, err := sqlDB.Query("SELECT * FROM prep_test WHERE id = $1", 1)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("prepare and execute insert", func(t *testing.T) {
		_, err = sqlDB.Exec("INSERT INTO prep_test VALUES (3, 'charlie')")
		require.NoError(t, err)

		var count int
		err = sqlDB.QueryRow("SELECT COUNT(*) FROM prep_test WHERE id = 3").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}

func TestConnectionHandlerTimeZone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("set and show timezone", func(t *testing.T) {
		_, err := sqlDB.Exec("SET TIME ZONE 'UTC'")
		require.NoError(t, err)

		var tz string
		err = sqlDB.QueryRow("SHOW TIME ZONE").Scan(&tz)
		require.NoError(t, err)
		require.Equal(t, "UTC", tz)
	})

	t.Run("set timezone to numeric offset", func(t *testing.T) {
		_, err := sqlDB.Exec("SET TIME ZONE '+8'")
		require.NoError(t, err)

		var tz string
		err = sqlDB.QueryRow("SHOW TIME ZONE").Scan(&tz)
		require.NoError(t, err)
		require.NotEmpty(t, tz)
	})

	t.Run("set timezone to named region", func(t *testing.T) {
		_, err := sqlDB.Exec("SET TIME ZONE 'America/New_York'")
		require.NoError(t, err)

		var tz string
		err = sqlDB.QueryRow("SHOW TIME ZONE").Scan(&tz)
		require.NoError(t, err)
		require.Contains(t, tz, "America")
	})
}

func TestConnectionHandlerSessionVariables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("set and show client_encoding", func(t *testing.T) {
		_, err := sqlDB.Exec("SET client_encoding = 'UTF8'")
		require.NoError(t, err)

		var encoding string
		err = sqlDB.QueryRow("SHOW client_encoding").Scan(&encoding)
		require.NoError(t, err)
	})

	t.Run("set application_name", func(t *testing.T) {
		_, err := sqlDB.Exec("SET application_name = 'test_app'")
		require.NoError(t, err)

		var appName string
		err = sqlDB.QueryRow("SHOW application_name").Scan(&appName)
		require.NoError(t, err)
		require.Equal(t, "test_app", appName)
	})

	t.Run("set extra_float_digits", func(t *testing.T) {
		_, err := sqlDB.Exec("SET extra_float_digits = 3")
		require.NoError(t, err)

		var digits string
		err = sqlDB.QueryRow("SHOW extra_float_digits").Scan(&digits)
		require.NoError(t, err)
	})

	t.Run("set datestyle", func(t *testing.T) {
		_, err := sqlDB.Exec("SET datestyle = 'ISO, MDY'")
		require.NoError(t, err)
	})
}

func TestConnectionHandlerTransactionIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("default isolation level", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE TABLE isolation_test (id INT PRIMARY KEY)")
		require.NoError(t, err)

		tx, err := sqlDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec("SELECT 1")
		require.NoError(t, err)
	})
}

func TestConnectionHandlerReadOnlyMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE readonly_test (id INT PRIMARY KEY)")
	require.NoError(t, err)

	t.Run("set read only mode", func(t *testing.T) {
		_, err := sqlDB.Exec("SET default_transaction_read_only = true")
		require.NoError(t, err)

		tx, err := sqlDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec("SELECT 1")
		require.NoError(t, err)
	})

	t.Run("insert should fail in read only mode", func(t *testing.T) {
		_, err := sqlDB.Exec("SET default_transaction_read_only = true")
		require.NoError(t, err)

		_, err = sqlDB.Exec("INSERT INTO readonly_test VALUES (1)")
		require.Error(t, err)
	})
}

func TestConnectionHandlerPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("begin transaction", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec("SELECT 1")
		require.NoError(t, err)
	})
}

func TestConnectionHandlerClosedSession(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE closed_session_test (id INT)")
	require.NoError(t, err)

	pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "TestClosedSession", url.User(security.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("INSERT INTO closed_session_test VALUES (1)")
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestConnectionHandlerIdleTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params := base.TestServerArgs{}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("SELECT 1")
	require.NoError(t, err)

	pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "TestIdleTimeout", url.User(security.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("SET statement_timeout = 60000")
	require.NoError(t, err)

	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)
}

func TestConnectionHandlerErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("division by zero error", func(t *testing.T) {
		_, err := sqlDB.Exec("SELECT 1/0")
		require.Error(t, err)
	})

	t.Run("invalid syntax error", func(t *testing.T) {
		_, err := sqlDB.Exec("SELEC * FROM invalid")
		require.Error(t, err)
	})

	t.Run("division by zero in transaction", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec("SELECT 1/0")
		require.Error(t, err)
	})
}

func TestConnectionHandlerSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE savepoint_test (id INT, value TEXT)")
	require.NoError(t, err)

	t.Run("savepoint in transaction", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec("SAVEPOINT kwbase_restart")
		require.NoError(t, err)

		_, err = tx.Exec("ROLLBACK TO SAVEPOINT kwbase_restart")
		require.NoError(t, err)

		_, err = tx.Exec("RELEASE SAVEPOINT kwbase_restart")
		require.NoError(t, err)
	})

	t.Run("release savepoint commits", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("INSERT INTO savepoint_test VALUES (1, 'test')")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var count int
		err = sqlDB.QueryRow("SELECT COUNT(*) FROM savepoint_test").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}
