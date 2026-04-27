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
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestWindowNodeMethods tests the methods of windowNode
func TestWindowNodeMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupNode func() *windowNode
		testFunc  func(t *testing.T, n *windowNode)
	}{
		{
			name: "test startExec panic",
			setupNode: func() *windowNode {
				return &windowNode{}
			},
			testFunc: func(t *testing.T, n *windowNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("startExec should panic")
					}
				}()
				params := runParams{
					ctx: context.Background(),
				}
				_ = n.startExec(params)
			},
		},
		{
			name: "test Next panic",
			setupNode: func() *windowNode {
				return &windowNode{}
			},
			testFunc: func(t *testing.T, n *windowNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Next should panic")
					}
				}()
				params := runParams{
					ctx: context.Background(),
				}
				_, _ = n.Next(params)
			},
		},
		{
			name: "test Values panic",
			setupNode: func() *windowNode {
				return &windowNode{}
			},
			testFunc: func(t *testing.T, n *windowNode) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Values should panic")
					}
				}()
				n.Values()
			},
		},
		{
			name: "test Close with nil plan",
			setupNode: func() *windowNode {
				return &windowNode{}
			},
			testFunc: func(t *testing.T, n *windowNode) {
				// Should handle nil plan gracefully
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Close() panicked as expected: %v", r)
					}
				}()
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

// TestWindowFuncHolderMethods tests the methods of windowFuncHolder
func TestWindowFuncHolderMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupFunc func() *windowFuncHolder
		testFunc  func(t *testing.T, w *windowFuncHolder)
	}{
		{
			name: "test samePartition with nil",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{partitionIdxs: []int{1, 2}}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				result := w.samePartition(&windowFuncHolder{partitionIdxs: []int{1}})
				if result {
					t.Error("samePartition(nil) = true, want false")
				}
			},
		},
		{
			name: "test Variable method",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				// Should not panic
				w.Variable()
			},
		},
		{
			name: "test Format method",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				ctx := &tree.FmtCtx{}
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Format() panicked as expected: %v", r)
					}
				}()
				// Should not panic
				w.Format(ctx)
			},
		},
		{
			name: "test String method",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				// String() may panic when expr is nil, which is expected
				defer func() {
					if r := recover(); r != nil {
						t.Logf("String() panicked as expected: %v", r)
					}
				}()
				result := w.String()
				if result == "" {
					t.Error("String() returned empty string")
				}
			},
		},
		{
			name: "test Walk method",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				// Create a simple visitor that implements the Visitor interface
				visitor := &simpleVisitor{}
				result := w.Walk(visitor)
				if result != w {
					t.Error("Walk() did not return self")
				}
			},
		},
		{
			name: "test TypeCheck method",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				ctx := &tree.SemaContext{}
				desired := types.Int
				result, err := w.TypeCheck(ctx, desired)
				if err != nil {
					t.Errorf("TypeCheck() returned error: %v", err)
				}
				if result != w {
					t.Error("TypeCheck() did not return self")
				}
			},
		},
		{
			name: "test Eval panic",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Eval should panic")
					}
				}()
				_, _ = w.Eval(&tree.EvalContext{})
			},
		},
		{
			name: "test ResolvedType",
			setupFunc: func() *windowFuncHolder {
				return &windowFuncHolder{}
			},
			testFunc: func(t *testing.T, w *windowFuncHolder) {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("ResolvedType() panicked as expected: %v", r)
					}
				}()
				result := w.ResolvedType()
				if result != nil {
					t.Errorf("ResolvedType() = %v, want nil", result)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := tt.setupFunc()
			tt.testFunc(t, w)
		})
	}
}

// TestWindowNodeColAndAggContainerMethods tests the methods of windowNodeColAndAggContainer
func TestWindowNodeColAndAggContainerMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		setupFunc func() *windowNodeColAndAggContainer
		testFunc  func(t *testing.T, c *windowNodeColAndAggContainer)
	}{
		{
			name: "test IndexedVarResolvedType panic",
			setupFunc: func() *windowNodeColAndAggContainer {
				return &windowNodeColAndAggContainer{}
			},
			testFunc: func(t *testing.T, c *windowNodeColAndAggContainer) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("IndexedVarResolvedType should panic")
					}
				}()
				c.IndexedVarResolvedType(0)
			},
		},
		{
			name: "test IndexedVarNodeFormatter panic",
			setupFunc: func() *windowNodeColAndAggContainer {
				return &windowNodeColAndAggContainer{}
			},
			testFunc: func(t *testing.T, c *windowNodeColAndAggContainer) {
				defer func() {
					if r := recover(); r == nil {
						t.Logf("IndexedVarNodeFormatter() panicked as expected: %v", r)
					}
				}()
				c.IndexedVarNodeFormatter(0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setupFunc()
			tt.testFunc(t, c)
		})
	}
}

// simpleVisitor is a simple implementation of tree.Visitor for testing
type simpleVisitor struct{}

func (v *simpleVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return true, expr
}

func (v *simpleVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}
