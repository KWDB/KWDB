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
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestCheckProcParamType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test supported types
	supportedTypes := []*types.T{
		types.Int,
		types.Float,
		types.Decimal,
		types.String,
		types.Timestamp,
		types.TimestampTZ,
	}

	for _, typ := range supportedTypes {
		if err := checkProcParamType(typ); err != nil {
			t.Errorf("Expected no error for type %s, got %v", typ.SQLString(), err)
		}
	}

	// Test unsupported types
	unsupportedTypes := []*types.T{
		types.Bool,
		types.Bytes,
		types.Date,
		types.Interval,
	}

	for _, typ := range unsupportedTypes {
		if err := checkProcParamType(typ); err == nil {
			t.Errorf("Expected error for type %s, got nil", typ.SQLString())
		}
	}
}

// TestBuildCreateProcedure tests the buildCreateProcedure function
func TestBuildCreateProcedure(t *testing.T) {
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

	// Create a CreateProcedure statement
	createProc := &tree.CreateProcedure{
		Name: tree.MakeTableName("t", "test_proc"),
		Parameters: []*tree.ProcedureParameter{
			{
				Name: "param1",
				Type: types.Int,
			},
			{
				Name: "param2",
				Type: types.String,
			},
		},
		Block: tree.Block{
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

	// Call buildCreateProcedure
	outScope := b.buildCreateProcedure(createProc, inScope)

	// Verify the result
	if outScope == nil {
		t.Error("Expected non-nil outScope")
	}

	if outScope.expr == nil {
		t.Error("Expected non-nil outScope.expr")
	}

	// Check if the expression is a CreateProcedure operator
	if _, ok := outScope.expr.(*memo.CreateProcedureExpr); ok {
		// Success
	} else {
		t.Errorf("Expected CreateProcedureExpr, got %T", outScope.expr)
	}
}

// TestBuildCreateProcedurePG tests the buildCreateProcedurePG function
func TestBuildCreateProcedurePG(t *testing.T) {
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

	// Create a CreateProcedurePG statement
	createProcPG := &tree.CreateProcedurePG{
		Name: tree.MakeTableName("t", "test_proc_pg"),
		Parameters: []*tree.ProcedureParameter{
			{
				Name: "param1",
				Type: types.Int,
			},
		},
		BodyStr: "\nBEGIN\n\tSELECT 1;\nEND\n",
	}

	// Create an empty scope
	inScope := b.allocScope()

	// Call buildCreateProcedurePG
	outScope := b.buildCreateProcedurePG(createProcPG, inScope)

	// Verify the result
	if outScope == nil {
		t.Error("Expected non-nil outScope")
	}

	if outScope.expr == nil {
		t.Error("Expected non-nil outScope.expr")
	}

	// Check if the expression is a CreateProcedure operator
	if _, ok := outScope.expr.(*memo.CreateProcedureExpr); ok {
		// Success
	} else {
		t.Errorf("Expected CreateProcedureExpr, got %T", outScope.expr)
	}
}
