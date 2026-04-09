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
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestProcPrepare tests the proc_prepare.go functions
func TestProcPrepare(t *testing.T) {
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

	// Test buildProcPrepare
	t.Run("buildProcPrepare", func(t *testing.T) {
		// Set up PrepareHelper
		b.PrepareHelper.Check = func(stmt *tree.Prepare, numPlaceholders int) (tree.PlaceholderTypes, error) {
			return nil, nil
		}
		b.PrepareHelper.Exec = func(ctx context.Context, stmtAST parser.Statement, placeholderHints tree.PlaceholderTypes, origin int, procUserDefinedVars map[string]tree.ProcUdvInfo, insidePrepareOfProcFlag uint8) (prepare.PreparedResult, error) {
			return "test result", nil
		}
		b.PrepareHelper.Add = func(ctx context.Context, prepareResult prepare.PreparedResult, name string) error {
			return nil
		}

		// Create a Prepare statement
		prepareStmt := &tree.Prepare{
			Name:            "test_prepare",
			Statement:       dummyStmt,
			NumPlaceholders: 0,
			NumAnnotations:  0,
		}

		// Call buildProcPrepare
		result := b.buildProcPrepare(prepareStmt)

		// Verify the result
		if result == nil {
			t.Error("Expected non-nil result")
		}

		// Check if the PrepareNamespaces map was updated
		if b.PrepareNamespaces == nil {
			t.Error("Expected PrepareNamespaces to be initialized")
		}

		if _, ok := b.PrepareNamespaces["test_prepare"]; !ok {
			t.Error("Expected test_prepare to be in PrepareNamespaces")
		}
	})

	// Test buildProcExecute
	t.Run("buildProcExecute", func(t *testing.T) {
		// Set up ExecuteHelper
		b.ExecuteHelper.Check = func(stmt *tree.Execute) (prepare.PreparedResult, error) {
			return "test prepared statement", nil
		}
		b.ExecuteHelper.GetReplacedMemo = func(ctx context.Context, s *tree.Execute, plInfo *tree.PlaceholderInfo) (prepare.MemoResult, error) {
			return nil, nil
		}
		b.ExecuteHelper.GetPlaceholder = func(psInterface prepare.PreparedResult, name string, params tree.Exprs, semaCtx *tree.SemaContext) (*tree.PlaceholderInfo, error) {
			return nil, nil
		}
		b.ExecuteHelper.GetStatement = func(ps prepare.PreparedResult) (int, string) {
			return int(tree.Rows), "SELECT * FROM test"
		}

		// Create an Execute statement
		executeStmt := &tree.Execute{
			Name:   "test_prepare",
			Params: tree.Exprs{},
		}

		// Call buildProcExecute
		result := b.buildProcExecute(executeStmt)

		// Verify the result
		if result == nil {
			t.Error("Expected non-nil result")
		}

		// Check if IsProcStatementTypeRows was set
		if !b.IsProcStatementTypeRows {
			t.Error("Expected IsProcStatementTypeRows to be true")
		}
	})

	// Test buildProcDeallocate
	t.Run("buildProcDeallocate", func(t *testing.T) {
		// Set up DeallocateHelper
		b.DeallocateHelper.Check = func(name string) bool {
			return true
		}

		// Create a Deallocate statement with a name
		deallocateStmt := &tree.Deallocate{
			Name: "test_prepare",
		}

		// Call buildProcDeallocate
		result := b.buildProcDeallocate(deallocateStmt)

		// Verify the result
		if result == nil {
			t.Error("Expected non-nil result")
		}

		// Check if the PrepareNamespaces map was updated
		if _, ok := b.PrepareNamespaces["test_prepare"]; ok {
			t.Error("Expected test_prepare to be removed from PrepareNamespaces")
		}

		// Test Deallocate with empty name (should clear all prepared statements)
		deallocateStmtEmpty := &tree.Deallocate{
			Name: "",
		}

		// Call buildProcDeallocate
		resultEmpty := b.buildProcDeallocate(deallocateStmtEmpty)

		// Verify the result
		if resultEmpty == nil {
			t.Error("Expected non-nil result for empty name")
		}

		// Check if PrepareNamespaces was cleared
		if b.PrepareNamespaces != nil {
			t.Error("Expected PrepareNamespaces to be cleared for empty name")
		}
	})
}
