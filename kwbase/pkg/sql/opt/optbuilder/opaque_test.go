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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestStatement is a test statement type for opaque statement testing
type TestStatement struct{}

// StatementType implements the Statement interface for TestStatement
func (ts *TestStatement) StatementType() tree.StatementType { return tree.Rows }

// StatementTag implements the Statement interface for TestStatement
func (ts *TestStatement) StatementTag() string { return "TEST" }

// Format implements the Statement interface for TestStatement
func (ts *TestStatement) Format(ctx *tree.FmtCtx) { ctx.WriteString("TEST STATEMENT") }

// StatOp implements the Statement interface for TestStatement
func (ts *TestStatement) StatOp() string { return "TEST" }

// StatTargetType implements the Statement interface for TestStatement
func (ts *TestStatement) StatTargetType() string { return "TEST" }

// String implements the fmt.Stringer interface for TestStatement
func (ts *TestStatement) String() string { return "TEST STATEMENT" }

// TestStatement2 is another test statement type for opaque statement testing
type TestStatement2 struct{}

// StatementType implements the Statement interface for TestStatement2
func (ts *TestStatement2) StatementType() tree.StatementType { return tree.Rows }

// StatementTag implements the Statement interface for TestStatement2
func (ts *TestStatement2) StatementTag() string { return "TEST2" }

// Format implements the Statement interface for TestStatement2
func (ts *TestStatement2) Format(ctx *tree.FmtCtx) { ctx.WriteString("TEST STATEMENT 2") }

// StatOp implements the Statement interface for TestStatement2
func (ts *TestStatement2) StatOp() string { return "TEST2" }

// StatTargetType implements the Statement interface for TestStatement2
func (ts *TestStatement2) StatTargetType() string { return "TEST2" }

// String implements the fmt.Stringer interface for TestStatement2
func (ts *TestStatement2) String() string { return "TEST STATEMENT 2" }

// UnregisteredStatement is an unregistered statement type for opaque statement testing
type UnregisteredStatement struct{}

// StatementType implements the Statement interface for UnregisteredStatement
func (us *UnregisteredStatement) StatementType() tree.StatementType { return tree.Rows }

// StatementTag implements the Statement interface for UnregisteredStatement
func (us *UnregisteredStatement) StatementTag() string { return "UNREGISTERED" }

// Format implements the Statement interface for UnregisteredStatement
func (us *UnregisteredStatement) Format(ctx *tree.FmtCtx) { ctx.WriteString("UNREGISTERED STATEMENT") }

// StatOp implements the Statement interface for UnregisteredStatement
func (us *UnregisteredStatement) StatOp() string { return "UNREGISTERED" }

// StatTargetType implements the Statement interface for UnregisteredStatement
func (us *UnregisteredStatement) StatTargetType() string { return "UNREGISTERED" }

// String implements the fmt.Stringer interface for UnregisteredStatement
func (us *UnregisteredStatement) String() string { return "UNREGISTERED STATEMENT" }

// MutationStatement is a mutation statement type for opaque statement testing
type MutationStatement struct{}

// StatementType implements the Statement interface for MutationStatement
func (ms *MutationStatement) StatementType() tree.StatementType { return tree.Rows }

// StatementTag implements the Statement interface for MutationStatement
func (ms *MutationStatement) StatementTag() string { return "MUTATION" }

// Format implements the Statement interface for MutationStatement
func (ms *MutationStatement) Format(ctx *tree.FmtCtx) { ctx.WriteString("MUTATION STATEMENT") }

// StatOp implements the Statement interface for MutationStatement
func (ms *MutationStatement) StatOp() string { return "MUTATION" }

// StatTargetType implements the Statement interface for MutationStatement
func (ms *MutationStatement) StatTargetType() string { return "MUTATION" }

// String implements the fmt.Stringer interface for MutationStatement
func (ms *MutationStatement) String() string { return "MUTATION STATEMENT" }

// DDLStatement is a DDL statement type for opaque statement testing
type DDLStatement struct{}

// StatementType implements the Statement interface for DDLStatement
func (ds *DDLStatement) StatementType() tree.StatementType { return tree.Rows }

// StatementTag implements the Statement interface for DDLStatement
func (ds *DDLStatement) StatementTag() string { return "DDL" }

// Format implements the Statement interface for DDLStatement
func (ds *DDLStatement) Format(ctx *tree.FmtCtx) { ctx.WriteString("DDL STATEMENT") }

// StatOp implements the Statement interface for DDLStatement
func (ds *DDLStatement) StatOp() string { return "DDL" }

// StatTargetType implements the Statement interface for DDLStatement
func (ds *DDLStatement) StatTargetType() string { return "DDL" }

// String implements the fmt.Stringer interface for DDLStatement
func (ds *DDLStatement) String() string { return "DDL STATEMENT" }

// testOpaqueMetadata is a simple implementation of opt.OpaqueMetadata for testing
type testOpaqueMetadata struct {
	name string
}

// ImplementsOpaqueMetadata implements the opt.OpaqueMetadata interface
func (tom *testOpaqueMetadata) ImplementsOpaqueMetadata() {}

// String implements the opt.OpaqueMetadata interface
func (tom *testOpaqueMetadata) String() string {
	return tom.name
}

// TestOpaqueStatement tests the opaque statement functionality
func TestOpaqueStatement(t *testing.T) {
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

	// Test RegisterOpaque
	t.Run("RegisterOpaque", func(t *testing.T) {
		// Define a test build function
		testBuildFn := func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
			return nil, nil, nil
		}

		// Register the test statement
		RegisterOpaque(reflect.TypeOf(&TestStatement{}), OpaqueReadOnly, testBuildFn)

		// Try to register the same statement again (should panic)
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when registering the same statement type twice")
			}
		}()
		RegisterOpaque(reflect.TypeOf(&TestStatement{}), OpaqueReadOnly, testBuildFn)
	})

	// Test tryBuildOpaque with a registered statement
	t.Run("tryBuildOpaque with registered statement", func(t *testing.T) {
		// Define a test build function that returns valid metadata and columns
		testBuildFn := func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
			cols := sqlbase.ResultColumns{
				{Name: "col1", Typ: types.Int},
				{Name: "col2", Typ: types.String},
			}
			// Create a simple OpaqueMetadata implementation for testing
			metadata := &testOpaqueMetadata{name: "test_metadata"}
			return metadata, cols, nil
		}

		// Register the test statement with the new build function
		RegisterOpaque(reflect.TypeOf(&TestStatement2{}), OpaqueReadOnly, testBuildFn)

		// Create a test statement
		testStmt := &TestStatement2{}

		// Create an input scope
		inScope := b.allocScope()

		// Call tryBuildOpaque
		outScope := b.tryBuildOpaque(testStmt, inScope)

		// Verify the result
		if outScope == nil {
			t.Error("Expected non-nil outScope")
		}

		if outScope.expr == nil {
			t.Error("Expected non-nil outScope.expr")
		}
	})

	// Test tryBuildOpaque with an unregistered statement
	t.Run("tryBuildOpaque with unregistered statement", func(t *testing.T) {
		// Create an unregistered statement
		unregisteredStmt := &UnregisteredStatement{}

		// Create an input scope
		inScope := b.allocScope()

		// Call tryBuildOpaque
		outScope := b.tryBuildOpaque(unregisteredStmt, inScope)

		// Verify the result
		if outScope != nil {
			t.Error("Expected nil outScope for unregistered statement")
		}
	})

	// Test tryBuildOpaque with different opaque types
	t.Run("tryBuildOpaque with different opaque types", func(t *testing.T) {
		// Define test build function
		testBuildFn := func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
			// Create a simple OpaqueMetadata implementation for testing
			metadata := &testOpaqueMetadata{name: "test_metadata"}
			cols := sqlbase.ResultColumns{
				{Name: "col1", Typ: types.Int},
			}
			return metadata, cols, nil
		}

		// Test OpaqueMutation
		RegisterOpaque(reflect.TypeOf(&MutationStatement{}), OpaqueMutation, testBuildFn)

		// Test OpaqueDDL
		RegisterOpaque(reflect.TypeOf(&DDLStatement{}), OpaqueDDL, testBuildFn)

		// Create input scope
		inScope := b.allocScope()

		// Test MutationStatement
		mutationStmt := &MutationStatement{}
		mutationScope := b.tryBuildOpaque(mutationStmt, inScope)
		if mutationScope == nil {
			t.Error("Expected non-nil outScope for MutationStatement")
		}

		// Test DDLStatement
		ddlStmt := &DDLStatement{}
		ddlScope := b.tryBuildOpaque(ddlStmt, inScope)
		if ddlScope == nil {
			t.Error("Expected non-nil outScope for DDLStatement")
		}
	})
}
