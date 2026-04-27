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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/transform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestMakeDefaultExprs_NoDefaults(t *testing.T) {
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: true},
		{ID: 2, Name: "b", Type: *types.String, Nullable: true},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result when no columns have defaults, got %v", result)
	}
}

func TestMakeDefaultExprs_WithDefaultInt(t *testing.T) {
	defaultVal := "42"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &defaultVal},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}
	if result[0] == tree.DNull {
		t.Error("Expected non-NULL default expression")
	}
}

func TestMakeDefaultExprs_WithDefaultString(t *testing.T) {
	defaultVal := "'hello'"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.String, Nullable: true, DefaultExpr: &defaultVal},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}
}

func TestMakeDefaultExprs_MixedDefaults(t *testing.T) {
	defaultVal := "100"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &defaultVal},
		{ID: 2, Name: "b", Type: *types.Int, Nullable: true},
		{ID: 3, Name: "c", Type: *types.String, Nullable: true},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if len(result) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(result))
	}
	if result[0] == tree.DNull {
		t.Error("Expected non-NULL for column with default")
	}
	if result[1] != tree.DNull {
		t.Error("Expected NULL for column without default")
	}
	if result[2] != tree.DNull {
		t.Error("Expected NULL for column without default")
	}
}

func TestMakeDefaultExprs_InvalidExpression(t *testing.T) {
	invalidExpr := "invalid+syntax+"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &invalidExpr},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err == nil {
		t.Fatal("Expected error for invalid expression, got nil")
	}
	if result != nil {
		t.Error("Expected nil result for error case")
	}
}

func TestMakeDefaultExprs_EmptyColumns(t *testing.T) {
	cols := []sqlbase.ColumnDescriptor{}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result for empty columns, got %v", result)
	}
}

func TestMakeDefaultExprs_NilColumns(t *testing.T) {
	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(nil, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result for nil columns, got %v", result)
	}
}

func TestMakeDefaultExprs_AllColumnsWithDefaults(t *testing.T) {
	defaultVal1 := "1"
	defaultVal2 := "'default_value'"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &defaultVal1},
		{ID: 2, Name: "b", Type: *types.String, Nullable: true, DefaultExpr: &defaultVal2},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	result, err := sqlbase.MakeDefaultExprs(cols, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if len(result) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(result))
	}
	for i, expr := range result {
		if expr == tree.DNull {
			t.Errorf("Expected non-NULL for column %d with default", i)
		}
	}
}

func TestProcessDefaultColumns(t *testing.T) {
	defaultVal := "10"
	tableDesc := sqlbase.TableDescriptor{
		ID: 100,
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &defaultVal},
			{ID: 2, Name: "b", Type: *types.Int, Nullable: true},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, ColumnIDs: []sqlbase.ColumnID{1}},
	}

	immutableDesc := sqlbase.NewImmutableTableDescriptor(tableDesc)

	defaultVal = "20"
	cols := []sqlbase.ColumnDescriptor{
		{ID: 1, Name: "a", Type: *types.Int, Nullable: false, DefaultExpr: &defaultVal},
		{ID: 2, Name: "b", Type: *types.Int, Nullable: true},
	}

	txCtx := &transform.ExprTransformContext{}
	evalCtx := makeTestEvalContext()
	defer evalCtx.Stop(nil)

	resultCols, defaultExprs, err := sqlbase.ProcessDefaultColumns(cols, immutableDesc, txCtx, evalCtx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if defaultExprs == nil {
		t.Fatal("Expected non-nil default expressions")
	}
	if len(resultCols) != 2 {
		t.Errorf("Expected 2 columns after processing, got %d", len(resultCols))
	}
}

func makeTestEvalContext() *tree.EvalContext {
	return tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
}
