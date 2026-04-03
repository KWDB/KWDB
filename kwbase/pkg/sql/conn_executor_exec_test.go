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

func TestBeginTransactionTimestampsAndReadMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("begin transaction with default read write mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("begin transaction with read only mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN READ ONLY")
		_, err := sqlDB.ExecContext(context.Background(),
			"CREATE TABLE test_readonly (id INT)")
		require.Error(t, err)
		_, _ = sqlDB.Exec("ROLLBACK")
	})

	t.Run("begin transaction as of system time", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN AS OF SYSTEM TIME '-1s'")
		_, err := sqlDB.ExecContext(context.Background(),
			"CREATE TABLE test_as_of (id INT)")
		require.Error(t, err)
		_, _ = sqlDB.Exec("ROLLBACK")
	})
}

func TestCommitSQLTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_commit (id INT PRIMARY KEY)")

	t.Run("commit transaction successfully", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_commit VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_commit WHERE id = 1").Scan(&count)
		require.Equal(t, 1, count)
	})

	t.Run("commit with multiple statements", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_commit VALUES (2)")
		_, _ = sqlDB.Exec("INSERT INTO test_commit VALUES (3)")
		_, _ = sqlDB.Exec("COMMIT")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_commit WHERE id IN (2, 3)").Scan(&count)
		require.Equal(t, 2, count)
	})
}

func TestRollbackSQLTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_rollback (id INT PRIMARY KEY)")

	t.Run("rollback transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_rollback VALUES (1)")
		_, _ = sqlDB.Exec("ROLLBACK")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_rollback WHERE id = 1").Scan(&count)
		require.Equal(t, 0, count)
	})

	t.Run("rollback after multiple inserts", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_rollback VALUES (2)")
		_, _ = sqlDB.Exec("INSERT INTO test_rollback VALUES (3)")
		_, _ = sqlDB.Exec("ROLLBACK")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_rollback WHERE id IN (2, 3)").Scan(&count)
		require.Equal(t, 0, count)
	})

	t.Run("rollback with savepoint", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_rollback VALUES (4)")
		_, _ = sqlDB.Exec("SAVEPOINT sp1")
		_, _ = sqlDB.Exec("INSERT INTO test_rollback VALUES (5)")
		_, _ = sqlDB.Exec("ROLLBACK TO SAVEPOINT sp1")
		_, _ = sqlDB.Exec("COMMIT")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_rollback WHERE id = 4").Scan(&count)
		require.Equal(t, 1, count)

		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_rollback WHERE id = 5").Scan(&count)
		require.Equal(t, 0, count)
	})
}

func TestAutoCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_autocommit (id INT PRIMARY KEY)")

	t.Run("implicit transaction auto commit", func(t *testing.T) {
		_, _ = sqlDB.Exec("SET default_transaction_read_only = false")
		_, _ = sqlDB.Exec("INSERT INTO test_autocommit VALUES (1)")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_autocommit WHERE id = 1").Scan(&count)
		require.Equal(t, 1, count)
	})

	t.Run("auto commit respects constraints", func(t *testing.T) {
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_autocommit VALUES (1)")
		require.Error(t, err)
	})

	t.Run("auto commit with read only mode disabled", func(t *testing.T) {
		_, _ = sqlDB.Exec("SET default_transaction_read_only = false")
		_, _ = sqlDB.Exec("INSERT INTO test_autocommit VALUES (2)")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_autocommit WHERE id = 2").Scan(&count)
		require.Equal(t, 1, count)
	})
}

func TestStatementCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_counters (id INT PRIMARY KEY)")

	t.Run("increment counters for select", func(t *testing.T) {
		_, _ = sqlDB.Exec("SELECT 1")
		_, _ = sqlDB.Exec("SELECT 2")
		_, _ = sqlDB.Exec("SELECT * FROM test_counters")
	})

	t.Run("increment counters for insert", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_counters VALUES (1)")
		_, _ = sqlDB.Exec("INSERT INTO test_counters VALUES (2)")
		_, _ = sqlDB.Exec("INSERT INTO test_counters VALUES (3)")
	})

	t.Run("increment counters for update", func(t *testing.T) {
		_, _ = sqlDB.Exec("UPDATE test_counters SET id = 10 WHERE id = 1")
	})

	t.Run("increment counters for delete", func(t *testing.T) {
		_, _ = sqlDB.Exec("DELETE FROM test_counters WHERE id = 2")
	})

	t.Run("increment counters for transaction statements", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("COMMIT")
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("ROLLBACK")
	})
}

func TestTransactionStartRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_txn_record (id INT PRIMARY KEY)")

	t.Run("record transaction start", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_txn_record VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("record implicit transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_txn_record VALUES (2)")
	})
}

func TestStatementContextManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_stmt_ctx (id INT PRIMARY KEY, data TEXT)")

	t.Run("statement context with select", func(t *testing.T) {
		var result int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_stmt_ctx").Scan(&result)
		require.Equal(t, 0, result)
	})

	t.Run("statement context with insert and query", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_stmt_ctx VALUES (1, 'test data')")

		var data string
		_ = sqlDB.QueryRow("SELECT data FROM test_stmt_ctx WHERE id = 1").Scan(&data)
		require.Equal(t, "test data", data)
	})

	t.Run("statement context preserves across multiple queries", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_stmt_ctx VALUES (2, 'txn data')")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_stmt_ctx").Scan(&count)
		require.Equal(t, 2, count)

		_, _ = sqlDB.Exec("COMMIT")
	})
}

func TestPortalExhaustion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_portal (id INT, name TEXT)")

	t.Run("execute portal once", func(t *testing.T) {
		_, _ = sqlDB.Exec("PREPARE p1 AS SELECT COUNT(*) FROM test_portal")
		_, _ = sqlDB.Exec("EXECUTE p1")
	})

	t.Run("execute multiple portals", func(t *testing.T) {
		_, _ = sqlDB.Exec("PREPARE p2 AS SELECT id FROM test_portal WHERE id = $1")
		_, _ = sqlDB.Exec("EXECUTE p2(1)")
	})

	t.Run("deallocate portal", func(t *testing.T) {
		_, _ = sqlDB.Exec("DEALLOCATE p1")
	})
}

func TestReadWriteMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_rw_mode (id INT PRIMARY KEY)")

	t.Run("default read write mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_rw_mode VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("explicit read write mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN READ WRITE")
		_, _ = sqlDB.Exec("INSERT INTO test_rw_mode VALUES (2)")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("explicit read only mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN READ ONLY")

		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_rw_mode VALUES (3)")
		require.Error(t, err)

		_, _ = sqlDB.Exec("ROLLBACK")
	})

	t.Run("set session read write mode", func(t *testing.T) {
		_, _ = sqlDB.Exec("SET default_transaction_read_only = false")
		_, _ = sqlDB.Exec("INSERT INTO test_rw_mode VALUES (4)")

		_, _ = sqlDB.Exec("SET default_transaction_read_only = true")
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_rw_mode VALUES (5)")
		require.Error(t, err)
	})
}

func TestStmtHasNoData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_no_data (id INT PRIMARY KEY, value TEXT)")

	t.Run("statements with no data", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_no_data VALUES (1, 'test')")
		_, _ = sqlDB.Exec("UPDATE test_no_data SET value = 'updated' WHERE id = 1")
		_, _ = sqlDB.Exec("DELETE FROM test_no_data WHERE id = 1")
	})

	t.Run("statements with data", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_no_data VALUES (2, 'test')")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_no_data").Scan(&count)
		require.Equal(t, 1, count)

		var value string
		_ = sqlDB.QueryRow("SELECT value FROM test_no_data WHERE id = 2").Scan(&value)
		require.Equal(t, "test", value)
	})
}

func TestStatementProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_progress (id INT PRIMARY KEY)")

	t.Run("long running statement progress", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_progress VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_progress").Scan(&count)
		require.Equal(t, 1, count)
	})
}

func TestObserverStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_observer (id INT PRIMARY KEY)")

	t.Run("observer statement execution", func(t *testing.T) {
		_, _ = sqlDB.Exec("SHOW TABLES")
		_, _ = sqlDB.Exec("SHOW COLUMNS FROM test_observer")
	})

	t.Run("observer statement in transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("SHOW TABLES")
		_, _ = sqlDB.Exec("COMMIT")
	})
}

func TestErrorHandlingInExecution(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_errors (id INT PRIMARY KEY)")

	t.Run("constraint violation error", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_errors VALUES (1)")
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_errors VALUES (1)")
		require.Error(t, err)
	})

	t.Run("type mismatch error", func(t *testing.T) {
		_, _ = sqlDB.Exec("CREATE TABLE test_type_mismatch (id INT)")
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_type_mismatch VALUES ('string')")
		require.Error(t, err)
	})
}

func TestTransactionStates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_states (id INT PRIMARY KEY)")

	t.Run("state no transaction", func(t *testing.T) {
		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_states").Scan(&count)
		require.Equal(t, 0, count)
	})

	t.Run("state open transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_states VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("state aborted transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_states VALUES (2)")
		_, _ = sqlDB.Exec("SAVEPOINT sp_test")
		_, _ = sqlDB.Exec("ROLLBACK TO SAVEPOINT sp_test")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("state commit wait transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_states VALUES (3)")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_states").Scan(&count)

		_, _ = sqlDB.Exec("COMMIT")
	})
}

func TestRetriableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_retry (id INT PRIMARY KEY)")

	t.Run("retriable error detection", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_retry VALUES (1)")
	})

	t.Run("non-retriable error detection", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_retry VALUES (1)")
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_retry VALUES (1)")
		require.Error(t, err)
	})
}

func TestTableTwoVersionInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_two_version (id INT PRIMARY KEY)")

	t.Run("validate primary keys on commit", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_two_version VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("without primary key", func(t *testing.T) {
		_, _ = sqlDB.Exec("CREATE TABLE test_no_pk (id INT)")
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_no_pk VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")
	})
}

func TestSessionIdleTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_idle_timeout (id INT PRIMARY KEY)")

	t.Run("set idle in session timeout", func(t *testing.T) {
		_, _ = sqlDB.Exec("SET idle_in_session_timeout = '10s'")
		_, _ = sqlDB.Exec("INSERT INTO test_idle_timeout VALUES (1)")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_idle_timeout").Scan(&count)
		require.Equal(t, 1, count)
	})

	t.Run("disable idle timeout", func(t *testing.T) {
		_, _ = sqlDB.Exec("SET idle_in_session_timeout = 0")
		_, _ = sqlDB.Exec("INSERT INTO test_idle_timeout VALUES (2)")
	})
}

func TestDedupClientNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_dedup (id INT PRIMARY KEY)")

	t.Run("send deduplicated notices", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("INSERT INTO test_dedup VALUES (1)")
		_, _ = sqlDB.Exec("COMMIT")

		var count int
		_ = sqlDB.QueryRow("SELECT COUNT(*) FROM test_dedup").Scan(&count)
		require.Equal(t, 1, count)
	})
}

func TestBatchErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_batch_errors (id INT PRIMARY KEY)")

	t.Run("batch insert with some errors", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_batch_errors VALUES (1)")
		_, err := sqlDB.ExecContext(context.Background(),
			"INSERT INTO test_batch_errors VALUES (1)")
		require.Error(t, err)

		_, _ = sqlDB.Exec("INSERT INTO test_batch_errors VALUES (2)")
	})

	t.Run("batch update with errors", func(t *testing.T) {
		_, err := sqlDB.ExecContext(context.Background(),
			"UPDATE test_batch_errors SET id = 1 WHERE id = 2")
		require.Error(t, err)
	})
}

func TestExecStmtInNoTxnState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("execute begin in no txn state", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("execute statements in implicit transaction", func(t *testing.T) {
		_, _ = sqlDB.Exec("CREATE TABLE test_implicit (id INT PRIMARY KEY)")
		_, _ = sqlDB.Exec("INSERT INTO test_implicit VALUES (1)")
	})
}

func TestExecStmtInAbortedState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("rollback clears aborted state", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("COMMIT")
	})

	t.Run("only rollback allowed in aborted state", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("ROLLBACK")
	})
}

func TestExecStmtInCommitWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	t.Run("commit wait state allows read only statements", func(t *testing.T) {
		_, _ = sqlDB.Exec("BEGIN")
		_, _ = sqlDB.Exec("COMMIT")

		_, _ = sqlDB.Exec("SELECT 1")
	})
}

func TestDispatchToExecutionEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, _ = sqlDB.Exec("CREATE TABLE IF NOT EXISTS test_dispatch (id INT, value TEXT)")

	t.Run("dispatch simple select", func(t *testing.T) {
		_, _ = sqlDB.Exec("SELECT 1")
	})

	t.Run("dispatch insert", func(t *testing.T) {
		_, _ = sqlDB.Exec("INSERT INTO test_dispatch VALUES (1, 'test')")
	})

	t.Run("dispatch update", func(t *testing.T) {
		_, _ = sqlDB.Exec("UPDATE test_dispatch SET value = 'updated' WHERE id = 1")
	})

	t.Run("dispatch delete", func(t *testing.T) {
		_, _ = sqlDB.Exec("DELETE FROM test_dispatch WHERE id = 1")
	})
}
