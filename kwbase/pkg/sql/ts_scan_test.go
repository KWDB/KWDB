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

// TestTsScanNodeIndexedVarMethods tests the IndexedVar-related methods of tsScanNode
func TestTsScanNodeIndexedVarMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		setupNode   func() *tsScanNode
		testFunc    func(t *testing.T, n *tsScanNode)
		expectedErr bool
	}{
		{
			name: "test IndexedVarResolvedType",
			setupNode: func() *tsScanNode {
				n := &tsScanNode{
					resultColumns: sqlbase.ResultColumns{
						{Name: "col1", Typ: types.Int},
						{Name: "col2", Typ: types.String},
					},
				}
				return n
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				result := n.IndexedVarResolvedType(0)
				if result != types.Int {
					t.Errorf("IndexedVarResolvedType(0) = %v, want %v", result, types.Int)
				}

				result = n.IndexedVarResolvedType(1)
				if result != types.String {
					t.Errorf("IndexedVarResolvedType(1) = %v, want %v", result, types.String)
				}
			},
		},
		{
			name: "test IndexedVarNodeFormatter",
			setupNode: func() *tsScanNode {
				n := &tsScanNode{
					resultColumns: sqlbase.ResultColumns{
						{Name: "test_column", Typ: types.Int},
					},
				}
				return n
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				result := n.IndexedVarNodeFormatter(0)
				if result == nil {
					t.Error("IndexedVarNodeFormatter(0) returned nil")
				}
			},
		},
		{
			name: "test IndexedVarEval panic",
			setupNode: func() *tsScanNode {
				return &tsScanNode{}
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("IndexedVarEval should panic")
					}
				}()
				n.IndexedVarEval(0, &tree.EvalContext{})
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

// TestTsScanNodeStartExec tests the startExec method of tsScanNode
func TestTsScanNodeStartExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		setupNode   func() *tsScanNode
		expectedErr bool
	}{
		{
			name: "test startExec returns warning",
			setupNode: func() *tsScanNode {
				return &tsScanNode{}
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setupNode()
			params := runParams{
				ctx: context.Background(),
			}

			err := n.startExec(params)
			if tt.expectedErr {
				if err == nil {
					t.Error("startExec() expected error, got nil")
				}
				// Check if it contains the expected warning message
				if err.Error() != "time series query is not supported in subquery" {
					t.Errorf("startExec() error message = %v, want %v", err.Error(), "time series query is not supported in subquery")
				}
			} else {
				if err != nil {
					t.Errorf("startExec() unexpected error: %v", err)
				}
			}
		})
	}
}

// TestTsScanNodeClose tests the Close method of tsScanNode
func TestTsScanNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *tsScanNode
		testFunc  func(t *testing.T, n *tsScanNode)
	}{
		{
			name: "test Close method",
			setupNode: func() *tsScanNode {
				return &tsScanNode{
					resultColumns: sqlbase.ResultColumns{
						{Name: "col1", Typ: types.Int},
					},
				}
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				// Call Close and verify the node is reset
				n.Close(context.Background())

				// Verify that the node was reset
				if len(n.resultColumns) != 0 {
					t.Errorf("Close() did not reset resultColumns, got length %d", len(n.resultColumns))
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

// TestTsScanNodeSkipClose tests the SkipClose method of tsScanNode
func TestTsScanNodeSkipClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *tsScanNode
		testFunc  func(t *testing.T, n *tsScanNode)
	}{
		{
			name: "test SkipClose returns true",
			setupNode: func() *tsScanNode {
				return &tsScanNode{}
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				result := n.SkipClose()
				if !result {
					t.Error("SkipClose() = false, want true")
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

// TestTsScanNodeNextAndValues tests the Next and Values methods of tsScanNode
func TestTsScanNodeNextAndValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *tsScanNode
		testFunc  func(t *testing.T, n *tsScanNode)
	}{
		{
			name: "test Next panic",
			setupNode: func() *tsScanNode {
				return &tsScanNode{}
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Next should panic")
					}
				}()
				params := runParams{
					ctx: context.Background(),
				}
				n.Next(params)
			},
		},
		{
			name: "test Values panic",
			setupNode: func() *tsScanNode {
				return &tsScanNode{}
			},
			testFunc: func(t *testing.T, n *tsScanNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Values should panic")
					}
				}()
				n.Values()
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

// TestPlannerTSScan tests the TSScan method of planner
func TestPlannerTSScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupFunc func() *planner
		testFunc  func(t *testing.T, p *planner)
	}{
		{
			name: "test TSScan returns valid node",
			setupFunc: func() *planner {
				return &planner{}
			},
			testFunc: func(t *testing.T, p *planner) {
				result := p.TSScan()
				if result == nil {
					t.Error("TSScan() returned nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setupFunc()
			tt.testFunc(t, p)
		})
	}
}

// TestSynchronizerNodeMethods tests the methods of synchronizerNode
func TestSynchronizerNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *synchronizerNode
		testFunc  func(t *testing.T, n *synchronizerNode)
	}{
		{
			name: "test IndexedVarResolvedType",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{
					columns: sqlbase.ResultColumns{
						{Name: "col1", Typ: types.Int},
					},
				}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				result := n.IndexedVarResolvedType(0)
				if result != types.Int {
					t.Errorf("IndexedVarResolvedType(0) = %v, want %v", result, types.Int)
				}
			},
		},
		{
			name: "test IndexedVarNodeFormatter",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{
					columns: sqlbase.ResultColumns{
						{Name: "test_column", Typ: types.Int},
					},
				}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				result := n.IndexedVarNodeFormatter(0)
				if result == nil {
					t.Error("IndexedVarNodeFormatter(0) returned nil")
				}
			},
		},
		{
			name: "test IndexedVarEval panic",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("IndexedVarEval should panic")
					}
				}()
				n.IndexedVarEval(0, &tree.EvalContext{})
			},
		},
		{
			name: "test startExec returns warning",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				params := runParams{
					ctx: context.Background(),
				}
				err := n.startExec(params)
				if err == nil {
					t.Error("startExec() expected error, got nil")
				}
				// Check if it contains the expected warning message
				if err.Error() != "time series query is not supported in subquery" {
					t.Errorf("startExec() error message = %v, want %v", err.Error(), "time series query is not supported in subquery")
				}
			},
		},
		{
			name: "test Next panic",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Next should panic")
					}
				}()
				params := runParams{
					ctx: context.Background(),
				}
				n.Next(params)
			},
		},
		{
			name: "test Values panic",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Values should panic")
					}
				}()
				n.Values()
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

// TestSynchronizerNodeClose tests the Close method of synchronizerNode
func TestSynchronizerNodeClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *synchronizerNode
		testFunc  func(t *testing.T, n *synchronizerNode)
	}{
		{
			name: "test Close with nil plan",
			setupNode: func() *synchronizerNode {
				return &synchronizerNode{}
			},
			testFunc: func(t *testing.T, n *synchronizerNode) {
				// Should not panic when plan is nil
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

// TestTsInsertSelectNodeMethods tests the methods of tsInsertSelectNode
func TestTsInsertSelectNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *tsInsertSelectNode
		testFunc  func(t *testing.T, n *tsInsertSelectNode)
	}{
		{
			name: "test startExec returns nil",
			setupNode: func() *tsInsertSelectNode {
				return &tsInsertSelectNode{}
			},
			testFunc: func(t *testing.T, n *tsInsertSelectNode) {
				params := runParams{
					ctx: context.Background(),
				}
				err := n.startExec(params)
				if err != nil {
					t.Errorf("startExec() returned error: %v", err)
				}
			},
		},
		{
			name: "test Next returns false",
			setupNode: func() *tsInsertSelectNode {
				return &tsInsertSelectNode{}
			},
			testFunc: func(t *testing.T, n *tsInsertSelectNode) {
				params := runParams{
					ctx: context.Background(),
				}
				hasNext, err := n.Next(params)
				if err != nil {
					t.Errorf("Next() returned error: %v", err)
				}
				if hasNext {
					t.Error("Next() = true, want false")
				}
			},
		},
		{
			name: "test Values returns nil",
			setupNode: func() *tsInsertSelectNode {
				return &tsInsertSelectNode{}
			},
			testFunc: func(t *testing.T, n *tsInsertSelectNode) {
				result := n.Values()
				if result != nil {
					t.Errorf("Values() = %v, want nil", result)
				}
			},
		},
		{
			name: "test Close with nil plan",
			setupNode: func() *tsInsertSelectNode {
				return &tsInsertSelectNode{}
			},
			testFunc: func(t *testing.T, n *tsInsertSelectNode) {
				// Should not panic when plan is nil
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
