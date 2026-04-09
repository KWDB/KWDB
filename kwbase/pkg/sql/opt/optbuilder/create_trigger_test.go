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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestBuildCreateTrigger tests the buildCreateTrigger function
func TestBuildCreateTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()

	// Create test table before creating trigger
	_, err := catalog.ExecuteDDL("CREATE TABLE t.public.test_table (id INT PRIMARY KEY, data STRING)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

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

	// Create a CreateTrigger statement
	createTrigger := &tree.CreateTrigger{
		Name:       tree.Name("test_trigger"),
		ActionTime: tree.TriggerActionTimeBefore,
		Event:      tree.TriggerEventInsert,
		TableName:  tree.MakeTableName("t", "test_table"),
		Body: tree.TriggerBody{
			Body: []tree.Statement{
				&tree.Select{
					Select: &tree.SelectClause{
						Exprs: tree.SelectExprs{
							{
								Expr: tree.NewDInt(1),
							},
						},
					},
				},
			},
		},
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCreateTrigger
	outScope := b.buildCreateTrigger(createTrigger, inScope)

	// Verify the result
	if outScope == nil {
		t.Error("Expected non-nil outScope")
	}

	if outScope.expr == nil {
		t.Error("Expected non-nil outScope.expr")
	}

	// Check if the expression is a CreateTrigger operator
	if _, ok := outScope.expr.(*memo.CreateTriggerExpr); ok {
		// Success
	} else {
		t.Errorf("Expected CreateTriggerExpr, got %T", outScope.expr)
	}
}

// TestBuildCreateTriggerPG tests the buildCreateTriggerPG function
func TestBuildCreateTriggerPG(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()

	// Create test table before creating trigger
	_, err := catalog.ExecuteDDL("CREATE TABLE t.public.test_table (id INT PRIMARY KEY, data STRING)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

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

	// Create a CreateTriggerPG statement
	createTriggerPG := &tree.CreateTriggerPG{
		Name:       tree.Name("test_trigger_pg"),
		ActionTime: tree.TriggerActionTimeBefore,
		Event:      tree.TriggerEventInsert,
		TableName:  tree.MakeTableName("t", "test_table"),
		BodyStr:    "\nBEGIN\nEND\n",
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCreateTriggerPG
	outScope := b.buildCreateTriggerPG(createTriggerPG, inScope)

	// Verify the result
	if outScope == nil {
		t.Error("Expected non-nil outScope")
	}

	if outScope.expr == nil {
		t.Error("Expected non-nil outScope.expr")
	}

	// Check if the expression is a CreateTrigger operator
	if _, ok := outScope.expr.(*memo.CreateTriggerExpr); ok {
		// Success
	} else {
		t.Errorf("Expected CreateTriggerExpr, got %T", outScope.expr)
	}
}

// TestBuildCreateTriggerWithInvalidTable tests the buildCreateTrigger function with invalid table
func TestBuildCreateTriggerWithInvalidTable(t *testing.T) {
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

	// Create a CreateTrigger statement with non-existent table
	createTrigger := &tree.CreateTrigger{
		Name:       tree.Name("test_trigger"),
		ActionTime: tree.TriggerActionTimeBefore,
		Event:      tree.TriggerEventInsert,
		TableName:  tree.MakeTableName("t", "non_existent_table"),
		Body: tree.TriggerBody{
			Body: []tree.Statement{
				&tree.Select{
					Select: &tree.SelectClause{
						Exprs: tree.SelectExprs{
							{
								Expr: tree.NewDInt(1),
							},
						},
					},
				},
			},
		},
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCreateTrigger and expect a panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected a panic for non-existent table")
		}
	}()

	b.buildCreateTrigger(createTrigger, inScope)
}
