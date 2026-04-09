// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package execinfra

import (
	"fmt"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

type testVarContainer struct{}

func (d testVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return types.Int
}

func (d testVarContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return nil, nil
}

func (d testVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("var%d", idx))
	return &n
}

func TestProcessExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := execinfrapb.Expression{Expr: "@1 * (@2 + @3) + @1"}

	h := tree.MakeIndexedVarHelper(testVarContainer{}, 4)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext()
	expr, err := processExpression(e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	if !h.IndexedVarUsed(0) || !h.IndexedVarUsed(1) || !h.IndexedVarUsed(2) || h.IndexedVarUsed(3) {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			h.IndexedVarUsed(0), h.IndexedVarUsed(1), h.IndexedVarUsed(2), h.IndexedVarUsed(3))
	}

	str := expr.String()
	expectedStr := "(var0 * (var1 + var2)) + var0"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}
}

// Test that processExpression evaluates constant exprs into datums.
func TestProcessExpressionConstantEval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := execinfrapb.Expression{Expr: "ARRAY[1:::INT,2:::INT]"}

	h := tree.MakeIndexedVarHelper(nil, 0)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext()
	expr, err := processExpression(e, &evalCtx, &semaCtx, &h)
	if err != nil {
		t.Fatal(err)
	}

	expected := &tree.DArray{
		ParamTyp:    types.Int,
		Array:       tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
		HasNonNulls: true,
	}
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}

// TestProcessExpressionError tests processExpression with invalid expressions.
func TestProcessExpressionError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with invalid syntax
	e := execinfrapb.Expression{Expr: "@1 * @2 +"}
	h := tree.MakeIndexedVarHelper(testVarContainer{}, 2)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext()
	_, err := processExpression(e, &evalCtx, &semaCtx, &h)
	if err == nil {
		t.Error("expected error for invalid syntax, got nil")
	}

	// Test with out-of-bounds indexed var
	e = execinfrapb.Expression{Expr: "@3"}
	h = tree.MakeIndexedVarHelper(testVarContainer{}, 2)
	_, err = processExpression(e, &evalCtx, &semaCtx, &h)
	if err == nil {
		t.Error("expected error for out-of-bounds indexed var, got nil")
	}
}

// TestExprHelperInit tests the Init method of ExprHelper.
func TestExprHelperInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eh := &ExprHelper{}
	expr := execinfrapb.Expression{Expr: "@1 + @2"}
	typs := []types.T{*types.Int, *types.Int}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	if eh.Expr == nil {
		t.Error("expected Expr to be initialized, got nil")
	}

	if len(eh.Types) != 2 {
		t.Errorf("expected Types length 2, got %d", len(eh.Types))
	}
}

// TestExprHelperInitEmpty tests the Init method of ExprHelper with empty expression.
func TestExprHelperInitEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eh := &ExprHelper{}
	expr := execinfrapb.Expression{}
	typs := []types.T{*types.Int, *types.Int}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	if eh.Expr != nil {
		t.Error("expected Expr to be nil for empty expression")
	}
}

// TestExprHelperEval tests the Eval method of ExprHelper.
func TestExprHelperEval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eh := &ExprHelper{}
	expr := execinfrapb.Expression{Expr: "@1 + @2"}
	typs := []types.T{*types.Int, *types.Int}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	row := sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2)),
	}

	result, err := eh.Eval(row)
	if err != nil {
		t.Fatal(err)
	}

	expected := tree.NewDInt(3)
	if result.String() != expected.String() {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// TestExprHelperEvalFilter tests the EvalFilter method of ExprHelper.
func TestExprHelperEvalFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eh := &ExprHelper{}
	expr := execinfrapb.Expression{Expr: "@1 > @2"}
	typs := []types.T{*types.Int, *types.Int}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Test with row where @1 > @2
	row1 := sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(3)),
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2)),
	}

	pass, err := eh.EvalFilter(row1)
	if err != nil {
		t.Fatal(err)
	}
	if !pass {
		t.Error("expected filter to pass for row [3, 2]")
	}

	// Test with row where @1 <= @2
	row2 := sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2)),
	}

	pass, err = eh.EvalFilter(row2)
	if err != nil {
		t.Fatal(err)
	}
	if pass {
		t.Error("expected filter to fail for row [1, 2]")
	}
}

// TestExprHelperIndexedVarMethods tests the IndexedVarContainer methods of ExprHelper.
func TestExprHelperIndexedVarMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eh := &ExprHelper{}
	expr := execinfrapb.Expression{Expr: "@1 + @2"}
	typs := []types.T{*types.Int, *types.Float}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Test IndexedVarResolvedType
	typ := eh.IndexedVarResolvedType(0)
	if typ.String() != types.Int.String() {
		t.Errorf("expected type Int for index 0, got %v", typ)
	}

	typ = eh.IndexedVarResolvedType(1)
	if typ.String() != types.Float.String() {
		t.Errorf("expected type Float for index 1, got %v", typ)
	}

	// Test IndexedVarNodeFormatter
	formatter := eh.IndexedVarNodeFormatter(0)
	if formatter == nil {
		t.Error("expected IndexedVarNodeFormatter to return non-nil")
	}

	// Test IndexedVarEval
	eh.Row = sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42)),
		sqlbase.DatumToEncDatum(types.Float, tree.NewDFloat(3.14)),
	}

	datum, err := eh.IndexedVarEval(0, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}
	expected := tree.NewDInt(42)
	if datum.String() != expected.String() {
		t.Errorf("expected 42, got %v", datum)
	}
}

// TestExprHelperString tests the String method of ExprHelper.
func TestExprHelperString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with nil Expr
	eh := &ExprHelper{}
	if eh.String() != "none" {
		t.Errorf("expected 'none' for nil Expr, got '%s'", eh.String())
	}

	// Test with non-nil Expr
	expr := execinfrapb.Expression{Expr: "@1 + @2"}
	typs := []types.T{*types.Int, *types.Int}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	err := eh.Init(expr, typs, &evalCtx)
	if err != nil {
		t.Fatal(err)
	}

	str := eh.String()
	if str == "" {
		t.Error("expected non-empty string for non-nil Expr")
	}
}
