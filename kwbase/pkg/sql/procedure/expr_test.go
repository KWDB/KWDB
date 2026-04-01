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

package procedure

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestExprHelper_Init(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int, Data: tree.NewDInt(42)}
	rCtx.AddVariable(var1)

	evalCtx := &tree.EvalContext{}
	expr := tree.NewDInt(42)

	exprHelper := &ExprHelper{}
	exprHelper.Init(rCtx, evalCtx, expr)

	require.Equal(t, evalCtx, exprHelper.evalCtx)
	require.Equal(t, rCtx, exprHelper.pCtx)
	require.Equal(t, expr, exprHelper.Expr)
	require.NotNil(t, exprHelper.Vars)
}

func TestExprHelper_Eval(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int, Data: tree.NewDInt(42)}
	rCtx.AddVariable(var1)

	evalCtx := &tree.EvalContext{}
	expr := tree.NewDInt(42)

	exprHelper := &ExprHelper{}
	exprHelper.Init(rCtx, evalCtx, expr)

	// Test Eval
	result, err := exprHelper.Eval()
	require.NoError(t, err)
	require.Equal(t, tree.NewDInt(42), result)
}

func TestExprHelper_EvalCompare(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	evalCtx := &tree.EvalContext{}

	// Test with DBoolTrue
	exprHelper1 := &ExprHelper{}
	exprHelper1.Init(rCtx, evalCtx, tree.MakeDBool(true))

	success, err := exprHelper1.EvalCompare()
	require.NoError(t, err)
	require.True(t, success)

	// Test with DBoolFalse
	exprHelper2 := &ExprHelper{}
	exprHelper2.Init(rCtx, evalCtx, tree.MakeDBool(false))

	success, err = exprHelper2.EvalCompare()
	require.NoError(t, err)
	require.False(t, success)

	// Test with positive DInt
	exprHelper3 := &ExprHelper{}
	exprHelper3.Init(rCtx, evalCtx, tree.NewDInt(42))

	success, err = exprHelper3.EvalCompare()
	require.NoError(t, err)
	require.True(t, success)

	// Test with zero DInt
	exprHelper4 := &ExprHelper{}
	exprHelper4.Init(rCtx, evalCtx, tree.NewDInt(0))

	success, err = exprHelper4.EvalCompare()
	require.NoError(t, err)
	require.False(t, success)

	// Test with negative DInt
	exprHelper5 := &ExprHelper{}
	exprHelper5.Init(rCtx, evalCtx, tree.NewDInt(-1))

	success, err = exprHelper5.EvalCompare()
	require.NoError(t, err)
	require.False(t, success)
}

func TestExprHelper_IndexedVarEval(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int, Data: tree.NewDInt(42)}
	rCtx.AddVariable(var1)

	evalCtx := &tree.EvalContext{}
	expr := tree.NewDInt(42)

	exprHelper := &ExprHelper{}
	exprHelper.Init(rCtx, evalCtx, expr)

	// Test IndexedVarEval
	result, err := exprHelper.IndexedVarEval(0, evalCtx)
	require.NoError(t, err)
	require.Equal(t, tree.NewDInt(42), result)
}

func TestExprHelper_IndexedVarResolvedType(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int, Data: tree.NewDInt(42)}
	rCtx.AddVariable(var1)

	evalCtx := &tree.EvalContext{}
	expr := tree.NewDInt(42)

	exprHelper := &ExprHelper{}
	exprHelper.Init(rCtx, evalCtx, expr)

	// Test IndexedVarResolvedType
	typ := exprHelper.IndexedVarResolvedType(0)
	require.Equal(t, *types.Int, *typ)
}

func TestExprHelper_IndexedVarNodeFormatter(t *testing.T) {
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int, Data: tree.NewDInt(42)}
	rCtx.AddVariable(var1)

	evalCtx := &tree.EvalContext{}
	expr := tree.NewDInt(42)

	exprHelper := &ExprHelper{}
	exprHelper.Init(rCtx, evalCtx, expr)

	// Test IndexedVarNodeFormatter
	formatter := exprHelper.IndexedVarNodeFormatter(0)
	require.NotNil(t, formatter)
}
