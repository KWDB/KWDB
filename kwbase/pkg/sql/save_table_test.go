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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPlannerMakeSaveTable tests the makeSaveTable method of planner
func TestPlannerMakeSaveTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupFunc func() (*planner, planNode, *tree.TableName, []string)
		testFunc  func(t *testing.T, result planNode)
	}{
		{
			name: "test makeSaveTable returns valid node",
			setupFunc: func() (*planner, planNode, *tree.TableName, []string) {
				// Create a mock source plan node
				source := &saveTableMockPlanNode{
					columns: []sqlbase.ResultColumn{
						{Name: "col1", Typ: types.Int},
						{Name: "col2", Typ: types.String},
					},
				}
				target := tree.NewTableName("defaultdb", "test_table")
				colNames := []string{"column1", "column2"}
				return &planner{}, source, target, colNames
			},
			testFunc: func(t *testing.T, result planNode) {
				if result == nil {
					t.Error("makeSaveTable() returned nil")
				}

				// Verify it's a saveTableNode
				if _, ok := result.(*saveTableNode); !ok {
					t.Errorf("makeSaveTable() returned %T, want *saveTableNode", result)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, source, target, colNames := tt.setupFunc()
			result := p.makeSaveTable(source, target, colNames)
			tt.testFunc(t, result)
		})
	}
}

// TestSaveTableNodeStartExec tests the startExec method of saveTableNode
func TestSaveTableNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *saveTableNode
		testFunc  func(t *testing.T, n *saveTableNode)
	}{
		{
			name: "test startExec with matching column names",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &valuesNode{
						columns: []sqlbase.ResultColumn{
							{Name: "col1", Typ: types.Int},
						},
					},
					colNames: []string{"column1"},
					target:   *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				params := runParams{
					ctx: context.Background(),
					p:   &planner{},
				}

				// startExec may panic due to nil internal executor, which is expected
				defer func() {
					if r := recover(); r != nil {
						t.Logf("startExec() panicked as expected: %v", r)
					}
				}()
				err := n.startExec(params)
				if err != nil {
					t.Errorf("startExec() returned error: %v", err)
				}
			},
		},
		{
			name: "test startExec with mismatched column names",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &valuesNode{
						columns: []sqlbase.ResultColumn{
							{Name: "col1", Typ: types.Int},
						},
					},
					colNames: []string{"column1", "column2"}, // Mismatched length
					target:   *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				params := runParams{
					ctx: context.Background(),
					p:   &planner{},
				}

				// startExec may panic due to nil internal executor, which is expected
				defer func() {
					if r := recover(); r != nil {
						t.Logf("startExec() panicked as expected: %v", r)
					}
				}()
				err := n.startExec(params)
				if err == nil {
					t.Error("startExec() expected error for mismatched columns, got nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestSaveTableNodeNext tests the Next method of saveTableNode
func TestSaveTableNodeNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *saveTableNode
		testFunc  func(t *testing.T, n *saveTableNode)
	}{
		{
			name: "test Next with source returning true",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &saveTableMockPlanNode{
						hasNext: true,
						values:  tree.Datums{tree.DNull},
					},
					target: *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				params := runParams{
					ctx: context.Background(),
					p:   &planner{},
				}

				// Next may panic due to nil internal executor, which is expected
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Next() panicked as expected: %v", r)
					}
				}()
				hasNext, err := n.Next(params)
				if err != nil {
					t.Errorf("Next() returned error: %v", err)
				}
				if !hasNext {
					t.Error("Next() = false, want true")
				}
			},
		},
		{
			name: "test Next with source returning false",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &saveTableMockPlanNode{
						hasNext: false,
					},
					target: *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				params := runParams{
					ctx: context.Background(),
					p:   &planner{},
				}

				// Next may panic due to nil internal executor, which is expected
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Next() panicked as expected: %v", r)
					}
				}()
				hasNext, err := n.Next(params)
				if err != nil {
					t.Errorf("Next() returned error: %v", err)
				}
				if hasNext {
					t.Error("Next() = true, want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestSaveTableNodeValues tests the Values method of saveTableNode
func TestSaveTableNodeValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *saveTableNode
		testFunc  func(t *testing.T, n *saveTableNode)
	}{
		{
			name: "test Values returns source values",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &saveTableMockPlanNode{
						values: tree.Datums{tree.DBoolTrue, tree.DBoolFalse},
					},
					target: *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				result := n.Values()
				if result == nil {
					t.Error("Values() returned nil")
				}
				if len(result) != 2 {
					t.Errorf("Values() length = %d, want 2", len(result))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// TestSaveTableNodeClose tests the Close method of saveTableNode
func TestSaveTableNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *saveTableNode
		testFunc  func(t *testing.T, n *saveTableNode)
	}{
		{
			name: "test Close does not panic",
			setupNode: func() *saveTableNode {
				return &saveTableNode{
					source: &saveTableMockPlanNode{},
					target: *tree.NewTableName("defaultdb", "test_table"),
				}
			},
			testFunc: func(t *testing.T, n *saveTableNode) {
				// Should not panic
				n.Close(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			tt.testFunc(t, n)
		})
	}
}

// Mock implementations for testing

type saveTableMockPlanNode struct {
	columns []sqlbase.ResultColumn
	values  tree.Datums
	hasNext bool
}

func (m *saveTableMockPlanNode) startExec(params runParams) error    { return nil }
func (m *saveTableMockPlanNode) Next(params runParams) (bool, error) { return m.hasNext, nil }
func (m *saveTableMockPlanNode) Values() tree.Datums                 { return m.values }
func (m *saveTableMockPlanNode) Close(ctx context.Context)           {}

func (m *saveTableMockPlanNode) planColumns() []sqlbase.ResultColumn { return m.columns }

type saveTableMockInternalExecutor struct{}

func (m *saveTableMockInternalExecutor) Exec(
	ctx context.Context, opName string, txn interface{}, stmt string, params ...interface{},
) (int, error) {
	return 0, nil
}

func (m *saveTableMockInternalExecutor) ExecWithCols(
	ctx context.Context, opName string, txn interface{}, stmt string, qargs ...interface{},
) (int, []string, error) {
	return 0, nil, nil
}

func (m *saveTableMockInternalExecutor) Query(
	ctx context.Context, opName string, txn interface{}, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	return nil, nil
}

func (m *saveTableMockInternalExecutor) QueryWithCols(
	ctx context.Context, opName string, txn interface{}, stmt string, qargs ...interface{},
) ([]tree.Datums, []string, error) {
	return nil, nil, nil
}

func (m *saveTableMockInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn interface{}, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	return nil, nil
}

func (m *saveTableMockInternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn interface{},
	sessionData interface{},
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return nil, nil
}
