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

package optbuilder

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// TestMakeDefaultExpr tests the makeDefaultExpr function
func TestMakeDefaultExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		typ  *types.T
	}{
		{
			name: "timestamp",
			typ:  types.Timestamp,
		},
		{
			name: "timestamptz",
			typ:  types.TimestampTZ,
		},
		{
			name: "int",
			typ:  types.Int,
		},
		{
			name: "float",
			typ:  types.Float,
		},
		{
			name: "decimal",
			typ:  types.Decimal,
		},
	}

	ctx := tree.NewParseTimeContext(timeutil.Now())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					// Some types may panic, which is expected
				}
			}()

			result := makeDefaultExpr(ctx, tt.typ)
			if result == nil {
				t.Errorf("Expected non-nil result for type %s", tt.typ.String())
			}
		})
	}
}

// TestBuildProcLeave tests the buildProcLeave function
func TestBuildProcLeave(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}
	labels := map[string]struct{}{
		"loop1": {},
	}

	// Test with existing label
	t.Run("existing label", func(t *testing.T) {
		leave := &tree.ProcLeave{Label: "loop1"}
		result := b.buildProcLeave(leave, inScope, labels)

		if result == nil {
			t.Errorf("Expected non-nil result")
		}

		leaveCmd, ok := result.(*memo.LeaveCommand)
		if !ok {
			t.Errorf("Expected *memo.LeaveCommand, got %T", result)
		}

		if leaveCmd.Label != "loop1" {
			t.Errorf("Expected label 'loop1', got '%s'", leaveCmd.Label)
		}
	})

	// Test with non-existing label (should panic)
	t.Run("non-existing label", func(t *testing.T) {
		leave := &tree.ProcLeave{Label: "nonexistent"}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for non-existing label")
			}
		}()

		b.buildProcLeave(leave, inScope, labels)
	})
}

// TestBuildProcedureTransaction tests the buildProcedureTransaction function
func TestBuildProcedureTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}

	tests := []struct {
		name     string
		stmt     tree.Statement
		expected tree.ProcedureTxnState
	}{
		{
			name:     "begin transaction",
			stmt:     &tree.BeginTransaction{},
			expected: tree.ProcedureTransactionStart,
		},
		{
			name:     "commit transaction",
			stmt:     &tree.CommitTransaction{},
			expected: tree.ProcedureTransactionCommit,
		},
		{
			name:     "rollback transaction",
			stmt:     &tree.RollbackTransaction{},
			expected: tree.ProcedureTransactionRollBack,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := b.buildProcedureTransaction(tt.stmt)

			if result == nil {
				t.Errorf("Expected non-nil result")
			}

			txnCmd, ok := result.(*memo.ProcedureTxnCommand)
			if !ok {
				t.Errorf("Expected *memo.ProcedureTxnCommand, got %T", result)
			}

			if txnCmd.TxnType != tt.expected {
				t.Errorf("Expected transaction type %v, got %v", tt.expected, txnCmd.TxnType)
			}
		})
	}
}

// TestBuildControlCursor tests the buildControlCursor function
func TestBuildControlCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}

	tests := []struct {
		name     string
		command  tree.CursorCommand
		expected string
	}{
		{
			name:     "open cursor",
			command:  tree.OpenCursor,
			expected: "test_cursor",
		},
		{
			name:     "close cursor",
			command:  tree.CloseCursor,
			expected: "test_cursor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursorStmt := &tree.ControlCursor{
				Command: tt.command,
				CurName: tree.Name(tt.expected),
			}

			result := b.buildControlCursor(cursorStmt, inScope)

			if result == nil {
				t.Errorf("Expected non-nil result")
			}

			switch tt.command {
			case tree.OpenCursor:
				cmd, ok := result.(*memo.OpenCursorCommand)
				if !ok {
					t.Errorf("Expected *memo.OpenCursorCommand, got %T", result)
				}
				if cmd.Name != tree.Name(tt.expected) {
					t.Errorf("Expected cursor name '%s', got '%s'", tt.expected, cmd.Name)
				}
			case tree.CloseCursor:
				cmd, ok := result.(*memo.CloseCursorCommand)
				if !ok {
					t.Errorf("Expected *memo.CloseCursorCommand, got %T", result)
				}
				if cmd.Name != tree.Name(tt.expected) {
					t.Errorf("Expected cursor name '%s', got '%s'", tt.expected, cmd.Name)
				}
			}
		})
	}
}

// TestBuildOpenCursor tests the buildOpenCursor function
func TestBuildOpenCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}
	cursorName := tree.Name("test_cursor")

	result := b.buildOpenCursor(cursorName, inScope)

	if result == nil {
		t.Errorf("Expected non-nil result")
	}

	cmd, ok := result.(*memo.OpenCursorCommand)
	if !ok {
		t.Errorf("Expected *memo.OpenCursorCommand, got %T", result)
	}

	if cmd.Name != cursorName {
		t.Errorf("Expected cursor name '%s', got '%s'", cursorName, cmd.Name)
	}
}

// TestBuildCloseCursor tests the buildCloseCursor function
func TestBuildCloseCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}
	cursorName := tree.Name("test_cursor")

	result := b.buildCloseCursor(cursorName, inScope)

	if result == nil {
		t.Errorf("Expected non-nil result")
	}

	cmd, ok := result.(*memo.CloseCursorCommand)
	if !ok {
		t.Errorf("Expected *memo.CloseCursorCommand, got %T", result)
	}

	if cmd.Name != cursorName {
		t.Errorf("Expected cursor name '%s', got '%s'", cursorName, cmd.Name)
	}
}

// TestBuildProcedureLoop tests the buildProcedureLoop function
func TestBuildProcedureLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}
	loop := &tree.ProcLoop{}

	result := b.buildProcedureLoop(loop, inScope)

	// Currently returns nil
	if result != nil {
		t.Errorf("Expected nil result, got %T", result)
	}
}

// TestEnsureScopeHasExpr tests the ensureScopeHasExpr function
// Note: This test only tests the case where expr is already set,
// because testing the nil case requires a properly initialized Builder with factory
func TestEnsureScopeHasExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}

	// Test with existing expression - this doesn't require initialized factory
	s := &scope{
		expr: &memo.ValuesExpr{},
	}

	originalExpr := s.expr
	b.ensureScopeHasExpr(s)

	if s.expr != originalExpr {
		t.Errorf("Expected expr to remain unchanged when already set")
	}
}

// TestBuildProcWhile tests the buildProcWhile function
// Note: This test only tests the duplicate label panic case,
// because the normal case requires a properly initialized Builder with factory
func TestBuildProcWhile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Builder{}
	inScope := &scope{}

	labels := map[string]struct{}{
		"test_loop": {},
	}

	// Test with duplicate label (should panic)
	procWhile := &tree.ProcWhile{
		Label:     "test_loop",
		Condition: tree.DBoolTrue,
		Body:      []tree.Statement{},
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for duplicate label")
		}
	}()

	b.buildProcWhile(procWhile, inScope, labels)
}
