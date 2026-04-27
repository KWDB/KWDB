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

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
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
