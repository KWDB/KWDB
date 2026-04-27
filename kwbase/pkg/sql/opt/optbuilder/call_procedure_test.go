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
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestBuildCallProcedure tests the buildCallProcedure function
func TestBuildCallProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()

	// Create a context and evaluation context
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.OptimizerFKs = true

	// Create an optimizer and factory
	var o xform.Optimizer
	o.Init(&evalCtx, catalog)
	o.DisableOptimizations()

	// Create a dummy statement for the Builder
	dummyStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				{
					Expr: tree.NewDInt(1),
				},
			},
		},
	}

	// Create a Builder
	b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), dummyStmt)

	// Create a CallProcedure statement
	callProc := &tree.CallProcedure{
		Name: tree.MakeTableName("t", "test_proc"),
		Exprs: tree.Exprs{
			tree.NewDInt(1),
			tree.NewDString("test"),
		},
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCallProcedure and expect a panic since testcat doesn't support procedures
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected a panic for non-existent procedure")
		}
	}()

	b.buildCallProcedure(callProc, inScope)
}

// TestBuildCallProcedureWithInvalidParams tests the buildCallProcedure function with invalid parameters
func TestBuildCallProcedureWithInvalidParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()

	// Create a context and evaluation context
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.OptimizerFKs = true

	// Create an optimizer and factory
	var o xform.Optimizer
	o.Init(&evalCtx, catalog)
	o.DisableOptimizations()

	// Create a dummy statement for the Builder
	dummyStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				{
					Expr: tree.NewDInt(1),
				},
			},
		},
	}

	// Create a Builder
	b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), dummyStmt)

	// Create a CallProcedure statement with wrong number of parameters
	callProc := &tree.CallProcedure{
		Name: tree.MakeTableName("t", "test_proc"),
		Exprs: tree.Exprs{
			tree.NewDInt(1),
			// Missing second parameter
		},
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCallProcedure and expect a panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected a panic for non-existent procedure")
		}
	}()

	b.buildCallProcedure(callProc, inScope)
}

// TestBuildCallProcedureWithNonExistentProc tests the buildCallProcedure function with a non-existent procedure
func TestBuildCallProcedureWithNonExistentProc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()

	// Create a context and evaluation context
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.OptimizerFKs = true

	// Create an optimizer and factory
	var o xform.Optimizer
	o.Init(&evalCtx, catalog)
	o.DisableOptimizations()

	// Create a dummy statement for the Builder
	dummyStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				{
					Expr: tree.NewDInt(1),
				},
			},
		},
	}

	// Create a Builder
	b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), dummyStmt)

	// Create a CallProcedure statement for a non-existent procedure
	callProc := &tree.CallProcedure{
		Name: tree.MakeTableName("t", "non_existent_proc"),
		Exprs: tree.Exprs{
			tree.NewDInt(1),
			tree.NewDString("test"),
		},
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCallProcedure and expect a panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected a panic for non-existent procedure")
		}
	}()

	b.buildCallProcedure(callProc, inScope)
}
