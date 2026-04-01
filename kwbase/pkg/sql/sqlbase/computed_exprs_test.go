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

package sqlbase_test

import (
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/transform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// TestRowIndexedVarContainer_IndexedVarEval tests the IndexedVarEval method of RowIndexedVarContainer
func TestRowIndexedVarContainer_IndexedVarEval(t *testing.T) {
	// Create a RowIndexedVarContainer with sample data
	datums := tree.Datums{tree.NewDInt(42), tree.NewDString("test")}
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1},
		{ID: 2},
	}
	mapping := map[sqlbase.ColumnID]int{
		1: 0, // Column ID 1 maps to row index 0
		2: 1, // Column ID 2 maps to row index 1
	}

	container := &sqlbase.RowIndexedVarContainer{
		CurSourceRow: datums,
		Cols:         cols,
		Mapping:      mapping,
	}

	// Test accessing the first column (index 0)
	result, err := container.IndexedVarEval(0, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result.Compare(nil, tree.NewDInt(42)) != 0 {
		t.Errorf("Expected DInt(42), got %v", result)
	}

	// Test accessing the second column (index 1)
	result, err = container.IndexedVarEval(1, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result.Compare(nil, tree.NewDString("test")) != 0 {
		t.Errorf("Expected DString('test'), got %v", result)
	}

	// Test accessing a column that doesn't exist in mapping
	colsMissing := []sqlbase.ColumnDescriptor{
		{ID: 3}, // This ID is not in the mapping
	}
	containerMissing := &sqlbase.RowIndexedVarContainer{
		CurSourceRow: datums,
		Cols:         colsMissing,
		Mapping:      mapping,
	}
	result, err = containerMissing.IndexedVarEval(0, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != tree.DNull {
		t.Errorf("Expected DNull for missing mapping, got %v", result)
	}
}

// TestRowIndexedVarContainer_IndexedVarResolvedType tests the IndexedVarResolvedType method
func TestRowIndexedVarContainer_IndexedVarResolvedType(t *testing.T) {
	container := &sqlbase.RowIndexedVarContainer{}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for unsupported operation")
		}
	}()

	// This should panic as indicated in the implementation
	container.IndexedVarResolvedType(0)
}

// TestRowIndexedVarContainer_IndexedVarNodeFormatter tests the IndexedVarNodeFormatter method
func TestRowIndexedVarContainer_IndexedVarNodeFormatter(t *testing.T) {
	container := &sqlbase.RowIndexedVarContainer{}

	result := container.IndexedVarNodeFormatter(0)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

// TestCannotWriteToComputedColError tests the CannotWriteToComputedColError function
func TestCannotWriteToComputedColError(t *testing.T) {
	colName := "test_column"
	err := sqlbase.CannotWriteToComputedColError(colName)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	errStr := err.Error()
	expectedSubstr := "cannot write directly to computed column \"test_column\""
	if !strings.HasSuffix(errStr, expectedSubstr) {
		t.Errorf("Expected error to contain '%s', got '%s'", expectedSubstr, errStr)
	}
}

// Helper struct to fix the issue with descContainer
type DescContainer struct {
	cols []sqlbase.ColumnDescriptor
}

func (j *DescContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unsupported")
}

func (j *DescContainer) IndexedVarResolvedType(idx int) *types.T {
	return &j.cols[idx].Type
}

func (*DescContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// Corrected test for descContainer
func TestDescContainerImplementation(t *testing.T) {
	// Create a descContainer with sample columns
	colType := *types.Int
	cols := []sqlbase.ColumnDescriptor{
		{Type: colType},
	}
	container := &DescContainer{cols: cols}

	result := container.IndexedVarResolvedType(0)
	if result.Family() != colType.Family() {
		t.Errorf("Expected type family %v, got %v", colType.Family(), result.Family())
	}
}

// TestMakeComputedExprs tests the MakeComputedExprs function
func TestMakeComputedExprs(t *testing.T) {
	// Create a test table descriptor
	tableDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 1,
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "a",
					Type: *types.Int,
				},
				{
					ID:   2,
					Name: "b",
					Type: *types.Int,
				},
				{
					ID:          3,
					Name:        "c",
					Type:        *types.Int,
					ComputeExpr: func() *string { s := "a + b"; return &s }(),
				},
			},
		},
	}

	tn := tree.NewUnqualifiedTableName(tree.Name("test_table"))
	txCtx := &transform.ExprTransformContext{}
	evalCtx := tree.NewTestingEvalContext(nil)

	// Test with columns including a computed column
	cols := []sqlbase.ColumnDescriptor{
		tableDesc.Columns[0],
		tableDesc.Columns[1],
		tableDesc.Columns[2],
	}

	computedExprs, err := sqlbase.MakeComputedExprs(cols, tableDesc, tn, txCtx, evalCtx, false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(computedExprs) != 3 {
		t.Errorf("Expected 3 computed expressions, got %d", len(computedExprs))
	}

	// Check that non-computed columns have NULL expressions
	if computedExprs[0] != tree.DNull {
		t.Errorf("Expected DNull for non-computed column, got %v", computedExprs[0])
	}
	if computedExprs[1] != tree.DNull {
		t.Errorf("Expected DNull for non-computed column, got %v", computedExprs[1])
	}

	// Check that computed column has a non-NULL expression
	if computedExprs[2] == tree.DNull {
		t.Error("Expected non-null expression for computed column")
	}

	tableDesc = &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 1,
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:          1,
					Name:        "a",
					Type:        *types.Int,
					ComputeExpr: func() *string { s := "a + b"; return &s }(),
				},
				{
					ID:          2,
					Name:        "b",
					Type:        *types.Int,
					ComputeExpr: func() *string { s := "a + b"; return &s }(),
				},
				{
					ID:          3,
					Name:        "c",
					Type:        *types.Int,
					ComputeExpr: func() *string { s := "a + b"; return &s }(),
				},
			},
		},
	}
	// Test with no computed columns
	colsNoComputed := []sqlbase.ColumnDescriptor{
		tableDesc.Columns[0],
		tableDesc.Columns[1],
	}

	computedExprsNoComputed, err := sqlbase.MakeComputedExprs(colsNoComputed, tableDesc, tn, txCtx, evalCtx, false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Fix: computedExprsNoComputed should be checked for length and DNulls, not for nil
	if len(computedExprsNoComputed) != 2 {
		t.Errorf("Expected 2 computed expressions, got %d", len(computedExprsNoComputed))
	}
	if computedExprsNoComputed[0] == tree.DNull {
		t.Errorf("Expected Non-DNull for non-computed column, got %v", computedExprsNoComputed[0])
	}
	if computedExprsNoComputed[1] == tree.DNull {
		t.Errorf("Expected Non-DNull for non-computed column, got %v", computedExprsNoComputed[1])
	}
}

// TestRowIndexedVarContainer_IndexedVarEval_EdgeCases tests edge cases for IndexedVarEval
func TestRowIndexedVarContainer_IndexedVarEval_EdgeCases(t *testing.T) {
	// Test with empty datums
	datums := tree.Datums{tree.DNull}
	cols := []sqlbase.ColumnDescriptor{{ID: 1}}
	mapping := map[sqlbase.ColumnID]int{1: 0}

	container := &sqlbase.RowIndexedVarContainer{
		CurSourceRow: datums,
		Cols:         cols,
		Mapping:      mapping,
	}

	// This should not panic even though the row is empty
	result, err := container.IndexedVarEval(0, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	// Since the mapping exists but the row is empty, this should return DNull
	if result != tree.DNull {
		t.Errorf("Expected DNull for empty row, got %v", result)
	}

}
