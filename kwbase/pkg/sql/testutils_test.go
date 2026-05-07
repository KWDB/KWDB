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
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestStmtBufReader tests the StmtBufReader methods
func TestStmtBufReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a new StmtBuf
	buf := NewStmtBuf()

	// Create some test commands
	execStmt := &ExecStmt{
		Statement: parser.Statement{
			AST: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: []tree.SelectExpr{
						{Expr: tree.NewDInt(1)},
					},
				},
			},
		},
	}

	prepareStmt := &PrepareStmt{
		Name: "test_stmt",
		Statement: parser.Statement{
			AST: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: []tree.SelectExpr{
						{Expr: tree.NewDInt(1)},
					},
				},
			},
		},
	}

	// Push commands into the buffer
	ctx := context.Background()
	if err := buf.Push(ctx, execStmt); err != nil {
		t.Errorf("Push failed: %v", err)
	}
	if err := buf.Push(ctx, prepareStmt); err != nil {
		t.Errorf("Push failed: %v", err)
	}

	// Create a StmtBufReader
	reader := MakeStmtBufReader(buf)

	// Test CurCmd
	cmd, err := reader.CurCmd()
	if err != nil {
		t.Errorf("CurCmd failed: %v", err)
	}
	if cmd != execStmt {
		t.Error("CurCmd should return the first command")
	}

	// Test AdvanceOne
	reader.AdvanceOne()
	cmd, err = reader.CurCmd()
	if err != nil {
		t.Errorf("CurCmd after AdvanceOne failed: %v", err)
	}
	if cmd != prepareStmt {
		t.Error("CurCmd should return the second command after AdvanceOne")
	}

	// Close the buffer before advancing beyond the last command
	buf.Close()

	// Test that AdvanceOne doesn't panic
	reader.AdvanceOne()

	// Test that CurCmd returns EOF after buffer is closed
	cmd, err = reader.CurCmd()
	if err == nil {
		t.Error("CurCmd should return EOF after buffer is closed")
	}
}

// TestDistSQLPlannerExec tests the DistSQLPlanner.Exec method
func TestDistSQLPlannerExec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database and table
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db; CREATE TABLE test_db.test_table (k INT);")

	// Get the executor config and DistSQLPlanner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	dsp := execCfg.DistSQLPlanner

	// Test both distributed and non-distributed execution
	for _, distribute := range []bool{true, false} {
		t.Run(fmt.Sprintf("distribute=%t", distribute), func(t *testing.T) {
			// Create an internal planner
			planner, cleanup := NewInternalPlanner(
				"test",
				kv.NewTxn(ctx, s.DB(), s.NodeID()),
				security.RootUser,
				&MemoryMetrics{},
				&execCfg,
			)
			defer cleanup()

			// Test a simple SELECT statement
			sql := "SELECT k FROM test_db.test_table WHERE k=1"
			err := dsp.Exec(ctx, planner, sql, distribute)
			if err != nil {
				t.Fatalf("Exec failed: %v", err)
			}

			// Test a more complex statement
			sql = "SELECT count(*) FROM test_db.test_table"
			err = dsp.Exec(ctx, planner, sql, distribute)
			if err != nil {
				t.Fatalf("Exec failed: %v", err)
			}
		})
	}
}
