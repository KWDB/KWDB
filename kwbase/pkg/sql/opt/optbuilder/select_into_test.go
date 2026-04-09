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

// TestBuildSelectInto tests the buildSelectInto function
func TestBuildSelectInto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test catalog
	catalog := testcat.New()
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

	// Test case 1: Basic SELECT INTO with valid variables
	t.Run("basic select into with valid variables", func(t *testing.T) {
		// Create a SelectInto statement
		selectInto := &tree.SelectInto{
			SelectClause: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: tree.SelectExprs{
						{
							Expr: tree.NewDInt(1),
						},
						{
							Expr: tree.NewDString("test"),
						},
					},
				},
			},
			Targets: tree.SelectIntoTargets{
				{
					Udv: tree.UserDefinedVar{VarName: "@var1"},
				},
				{
					Udv: tree.UserDefinedVar{VarName: "@var2"},
				},
			},
		}

		// Create an input scope
		inScope := b.allocScope()

		// Call buildSelectInto
		outScope := b.buildSelectInto(selectInto, inScope)

		// Verify the result
		if outScope == nil {
			t.Error("Expected non-nil outScope")
		}

		if outScope.expr == nil {
			t.Error("Expected non-nil outScope.expr")
		}
	})

	// Test case 2: SELECT INTO with column count mismatch (should panic)
	t.Run("select into with column count mismatch", func(t *testing.T) {
		// Create a SelectInto statement with mismatched column counts
		selectInto := &tree.SelectInto{
			SelectClause: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: tree.SelectExprs{
						{
							Expr: tree.NewDInt(1),
						},
					},
				},
			},
			Targets: tree.SelectIntoTargets{
				{
					Udv: tree.UserDefinedVar{VarName: "@var1"},
				},
				{
					Udv: tree.UserDefinedVar{VarName: "@var2"},
				},
			},
		}

		// Create an input scope
		inScope := b.allocScope()

		// Call buildSelectInto (should panic)
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for column count mismatch")
			}
		}()
		b.buildSelectInto(selectInto, inScope)
	})

	// Test case 3: SELECT INTO with invalid variable name (should panic)
	t.Run("select into with invalid variable name", func(t *testing.T) {
		// Create a SelectInto statement with invalid variable name
		selectInto := &tree.SelectInto{
			SelectClause: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: tree.SelectExprs{
						{
							Expr: tree.NewDInt(1),
						},
					},
				},
			},
			Targets: tree.SelectIntoTargets{
				{
					Udv: tree.UserDefinedVar{VarName: "invalid_var"}, // Missing @ prefix
				},
			},
		}

		// Create an input scope
		inScope := b.allocScope()

		// Call buildSelectInto (should panic)
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for invalid variable name")
			}
		}()
		b.buildSelectInto(selectInto, inScope)
	})

	// Test case 4: SELECT INTO with empty variable name (should panic)
	t.Run("select into with empty variable name", func(t *testing.T) {
		// Create a SelectInto statement with empty variable name
		selectInto := &tree.SelectInto{
			SelectClause: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: tree.SelectExprs{
						{
							Expr: tree.NewDInt(1),
						},
					},
				},
			},
			Targets: tree.SelectIntoTargets{
				{
					Udv: tree.UserDefinedVar{VarName: ""}, // Empty variable name
				},
			},
		}

		// Create an input scope
		inScope := b.allocScope()

		// Call buildSelectInto (should panic)
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for empty variable name")
			}
		}()
		b.buildSelectInto(selectInto, inScope)
	})

	// Test case 5: SELECT INTO with DeclareVar set (should panic)
	t.Run("select into with declare var", func(t *testing.T) {
		// Create a SelectInto statement with DeclareVar set
		selectInto := &tree.SelectInto{
			SelectClause: &tree.Select{
				Select: &tree.SelectClause{
					Exprs: tree.SelectExprs{
						{
							Expr: tree.NewDInt(1),
						},
					},
				},
			},
			Targets: tree.SelectIntoTargets{
				{
					Udv:        tree.UserDefinedVar{VarName: "@var1"},
					DeclareVar: "declare_var", // DeclareVar should not be set
				},
			},
		}

		// Create an input scope
		inScope := b.allocScope()

		// Call buildSelectInto (should panic)
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for DeclareVar set")
			}
		}()
		b.buildSelectInto(selectInto, inScope)
	})
}
