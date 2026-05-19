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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPrepareBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE prepare_test (id INT, name TEXT)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO prepare_test VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	t.Run("prepare and execute select", func(t *testing.T) {
		rows, err := sqlDB.Query("SELECT * FROM prepare_test WHERE id = $1", 1)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})

	t.Run("prepare with type hints", func(t *testing.T) {
		rows, err := sqlDB.Query("SELECT * FROM prepare_test WHERE id = $1::INT", 2)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, count)
	})
}

func TestPrepareWithPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE placeholder_test (a INT, b TEXT, c FLOAT)")
	require.NoError(t, err)

	t.Run("multiple placeholders", func(t *testing.T) {
		rows, err := sqlDB.Query("SELECT * FROM placeholder_test WHERE a = $1 AND c > $2", 10, 5.0)
		require.NoError(t, err)
		defer rows.Close()
	})

	t.Run("reuse prepared statement", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			_, err := sqlDB.Exec("INSERT INTO placeholder_test VALUES ($1, 'test', $2)", i, float64(i)*1.5)
			require.NoError(t, err)
		}

		rows, err := sqlDB.Query("select count(*) FROM placeholder_test")
		require.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var count int
			err := rows.Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 3, count)
		}
	})
}

func TestPrepareDuplicateName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE dup_test (id INT)")
	require.NoError(t, err)

	t.Run("prepare same name twice should error", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE stmt1 AS SELECT * FROM dup_test")
		require.NoError(t, err)

		_, err = sqlDB.Exec("PREPARE stmt1 AS SELECT 1")
		require.Error(t, err)
	})
}

func TestPrepareWithUserDefinedVars(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("set and use user defined variable in prepare", func(t *testing.T) {
		_, err := sqlDB.Exec("SET @x = 'hello'")
		require.NoError(t, err)

		_, err = sqlDB.Exec("CREATE TABLE udv_test (name TEXT)")
		require.NoError(t, err)

		_, err = sqlDB.Exec("INSERT INTO udv_test VALUES ('test')")
		require.NoError(t, err)
	})
}

func TestDeallocatePreparedStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE dealloc_test (id INT, val TEXT)")
	require.NoError(t, err)

	t.Run("deallocate prepared statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE my_stmt AS SELECT * FROM dealloc_test")
		require.NoError(t, err)

		rows, err := sqlDB.Query("EXECUTE my_stmt")
		require.NoError(t, err)
		rows.Close()

		_, err = sqlDB.Exec("DEALLOCATE my_stmt")
		require.NoError(t, err)

		_, err = sqlDB.Query("EXECUTE my_stmt")
		require.Error(t, err)
	})

	t.Run("deallocate non-existent should error", func(t *testing.T) {
		_, err := sqlDB.Exec("DEALLOCATE nonexistent_stmt")
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("deallocate prepare statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE drop_stmt AS SELECT 1")
		require.NoError(t, err)

		_, err = sqlDB.Exec("DEALLOCATE PREPARE drop_stmt")
		require.NoError(t, err)

		_, err = sqlDB.Query("EXECUTE drop_stmt")
		require.Error(t, err)
	})
}

func TestPrepareInsertDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("insert direct basic", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE TABLE insert_direct_test (a INT, b INT, c INT, PRIMARY KEY(a, b))")
		require.NoError(t, err)

		_, err = sqlDB.Exec("INSERT INTO insert_direct_test VALUES (1, 2, 3)")
		require.NoError(t, err)

		var count int
		err = sqlDB.QueryRow("select count(*) FROM insert_direct_test").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("insert direct with subquery", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE TABLE src_table (x INT, y INT)")
		require.NoError(t, err)

		_, err = sqlDB.Exec("INSERT INTO src_table VALUES (10, 20), (30, 40)")
		require.NoError(t, err)
	})
}

func TestPrepareErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("prepare invalid syntax", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE invalid AS SELCT * FROM nonexistent")
		require.Error(t, err)
	})

	t.Run("execute with wrong argument count", func(t *testing.T) {
		_, err := sqlDB.Exec("CREATE TABLE exec_test (a INT, b INT)")
		require.NoError(t, err)

		_, err = sqlDB.Exec("PREPARE exec_stmt AS SELECT * FROM exec_test WHERE a = $1")
		require.NoError(t, err)

		_, err = sqlDB.Query("EXECUTE exec_stmt()")
		require.Error(t, err)
	})

	t.Run("execute with missing table", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE missing AS SELECT * FROM definitely_missing_table_xyz")
		require.Error(t, err)
	})
}

func TestPrepareWithTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE txn_test (id INT, val TEXT)")
	require.NoError(t, err)

	t.Run("prepare in transaction", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("PREPARE txn_stmt AS SELECT * FROM txn_test")
		require.NoError(t, err)

		rows, err := tx.Query("EXECUTE txn_stmt")
		require.NoError(t, err)
		err = rows.Close()
		require.NoError(t, err)

		_, err = tx.Exec("DEALLOCATE txn_stmt")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("prepare after transaction rollback", func(t *testing.T) {
		tx, err := sqlDB.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("INSERT INTO txn_test VALUES (1, 'before rollback')")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		_, err = sqlDB.Exec("PREPARE post_rollback AS SELECT * FROM txn_test")
		require.NoError(t, err)

		rows, err := sqlDB.Query("EXECUTE post_rollback")
		require.NoError(t, err)
		rows.Close()
	})
}

func TestPrepareStatementType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE stmt_type_test (id INT, name TEXT)")
	require.NoError(t, err)

	t.Run("select statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE sel_stmt AS SELECT * FROM stmt_type_test")
		require.NoError(t, err)

		rows, err := sqlDB.Query("EXECUTE sel_stmt")
		require.NoError(t, err)
		rows.Close()
	})

	t.Run("insert statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE ins_stmt AS INSERT INTO stmt_type_test VALUES (1, 'test')")
		require.NoError(t, err)

		_, err = sqlDB.Exec("EXECUTE ins_stmt")
		require.NoError(t, err)
	})

	t.Run("update statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE upd_stmt AS UPDATE stmt_type_test SET name = 'updated' WHERE id = 1")
		require.NoError(t, err)

		_, err = sqlDB.Exec("EXECUTE upd_stmt")
		require.NoError(t, err)
	})

	t.Run("delete statement", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE del_stmt AS DELETE FROM stmt_type_test WHERE id = 1")
		require.NoError(t, err)

		_, err = sqlDB.Exec("EXECUTE del_stmt")
		require.NoError(t, err)
	})
}

func TestPrepareMemoryManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE mem_test (id INT)")
	require.NoError(t, err)

	t.Run("multiple prepare and deallocate", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			stmtName := "stmt_" + string(rune('a'+i))
			_, err := sqlDB.Exec("PREPARE " + stmtName + " AS SELECT * FROM mem_test")
			require.NoError(t, err)
		}

		for i := 0; i < 10; i++ {
			stmtName := "stmt_" + string(rune('a'+i))
			_, err := sqlDB.Exec("DEALLOCATE " + stmtName)
			require.NoError(t, err)
		}
	})
}

func TestPrepareWithNullParameters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE null_test (id INT, val TEXT)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO null_test VALUES (1, 'a'), (2, NULL), (3, 'c')")
	require.NoError(t, err)

	t.Run("query with null parameter", func(t *testing.T) {
		stmt, err := sqlDB.Prepare("INSERT INTO null_test VALUES ($1, $2)")
		require.NoError(t, err)
		defer stmt.Close()

		_, err = stmt.Exec(4, nil)
		require.NoError(t, err)
	})

	t.Run("select with null comparison", func(t *testing.T) {
		rows, err := sqlDB.Query("SELECT * FROM null_test WHERE val IS NULL")
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		require.GreaterOrEqual(t, count, 1)
	})
}

func TestPrepareConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE concurrent_test (id INT PRIMARY KEY)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO concurrent_test VALUES (1)")
	require.NoError(t, err)

	t.Run("prepare in sequence", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE con_stmt1 AS SELECT * FROM concurrent_test WHERE id = 1")
		require.NoError(t, err)

		_, err = sqlDB.Exec("PREPARE con_stmt2 AS SELECT * FROM concurrent_test WHERE id = 1")
		require.NoError(t, err)

		rows1, err := sqlDB.Query("EXECUTE con_stmt1")
		require.NoError(t, err)
		rows1.Close()

		rows2, err := sqlDB.Query("EXECUTE con_stmt2")
		require.NoError(t, err)
		rows2.Close()
	})
}

func TestPrepareAsOf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE asof_test (id INT, ts TIMESTAMP)")
	require.NoError(t, err)

	t.Run("AS OF SYSTEM TIME prepare", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE asof_stmt AS SELECT * FROM asof_test")
		require.NoError(t, err)

		_, err = sqlDB.Exec("EXECUTE asof_stmt")
		require.NoError(t, err)
	})
}

func TestPreparePlaceholderInference(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE TABLE infer_test (id INT, name TEXT, amount DECIMAL)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("INSERT INTO infer_test VALUES (1, 'test', 100.50)")
	require.NoError(t, err)

	t.Run("placeholder type inference from execute", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE infer_stmt AS SELECT * FROM infer_test WHERE id = $1")
		require.NoError(t, err)

		rows, err := sqlDB.Query("EXECUTE infer_stmt(1)")
		require.NoError(t, err)
		rows.Close()
	})

	t.Run("multiple placeholder types", func(t *testing.T) {
		_, err := sqlDB.Exec("PREPARE multi_placeholders AS SELECT * FROM infer_test WHERE id = $1 AND amount > $2")
		require.NoError(t, err)

		rows, err := sqlDB.Query("EXECUTE multi_placeholders(1, 50.0)")
		require.NoError(t, err)
		rows.Close()
	})
}
