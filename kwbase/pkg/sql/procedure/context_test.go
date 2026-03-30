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
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestSpExecContext_Init(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()

	require.Nil(t, ctx.localVariable)
	require.NotNil(t, ctx.localVarMap)
	require.Empty(t, ctx.localVarMap)
	require.NotNil(t, ctx.localCurSorMap)
	require.Empty(t, ctx.localCurSorMap)
	require.NotNil(t, ctx.results)
	require.NotNil(t, ctx.procedureTxn)
	require.NotNil(t, ctx.continueExec)
	require.False(t, *ctx.continueExec)
}

func TestSpExecContext_InitResult(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	ctx.InitResult()

	require.NotNil(t, ctx.results)
	require.True(t, *ctx.continueExec)
}

func TestSpExecContext_ContinueExec(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	require.False(t, ctx.ContinueExec())

	ctx.InitResult()
	require.True(t, ctx.ContinueExec())
}

func TestSpExecContext_ResetExecuteStatus(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	ctx.InitResult()
	require.True(t, ctx.ContinueExec())

	ctx.ResetExecuteStatus()
	require.False(t, ctx.ContinueExec())
}

func TestSpExecContext_Inherit(t *testing.T) {
	src := &SpExecContext{}
	src.Init()
	
	// Add some variables to source
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	src.AddVariable(var1)
	
	// Add some cursors to source
	cursor1 := &CursorHelper{}
	src.AddCursor("cursor1", cursor1)
	
	dest := &SpExecContext{}
	dest.Init()
	dest.Inherit(src)
	
	// Check that variables were inherited
	require.Len(t, dest.localVariable, 1)
	require.Equal(t, "var1", dest.localVariable[0].Name)
	
	// Check that cursors were inherited
	require.Len(t, dest.localCurSorMap, 1)
	require.NotNil(t, dest.localCurSorMap["cursor1"])
}

func TestSpExecContext_InheritResult(t *testing.T) {
	src := &SpExecContext{}
	src.Init()
	src.InitResult()
	
	dest := &SpExecContext{}
	dest.Init()
	dest.InheritResult(src)
	
	// Check that results were inherited
	require.Equal(t, src.results, dest.results)
	require.Equal(t, src.continueExec, dest.continueExec)
}

func TestSpExecContext_AddVariable(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	require.Len(t, ctx.localVariable, 1)
	require.Equal(t, "var1", ctx.localVariable[0].Name)
	require.Equal(t, 0, ctx.localVarMap["var1"])
	
	// Update the same variable
	var1Updated := &exec.LocalVariable{Name: "var1", Typ: *types.String}
	ctx.AddVariable(var1Updated)
	
	require.Len(t, ctx.localVariable, 1)
	require.Equal(t, *types.String, ctx.localVariable[0].Typ)
}

func TestSpExecContext_SetVariable(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	// Set variable value
	datum := tree.NewDInt(42)
	ctx.SetVariable(0, datum)
	
	require.Equal(t, datum, ctx.localVariable[0].Data)
}

type triggerHelperTest struct {
	beforeIns Instruction
	afterIns  Instruction
	execCtx   SpExecContext
	// helper function, which is used to do optimization and generate execPlan for stmts in trigger
	fn exec.ProcedurePlanFn
	// origin DML sourceValues, which can be mutated by trigger stmts.
	internalValues *tree.Datums
	// current placeHolderInfo in evalCtx, used to records values corresponding to placeHolder idx,
	// it should be mutated at the same time when origin DML sourceValue is changed.
	placeholders *tree.PlaceholderInfo
}

// GetValue get placeholder value
func (t *triggerHelperTest) GetValue(index tree.PlaceholderIdx) tree.Datum {
	return (*t.internalValues)[index]
}

// SetValue set placeholder value
func (t *triggerHelperTest) SetValue(index tree.PlaceholderIdx, v tree.Datum) {
	(*t.internalValues)[index] = v
	t.placeholders.Values[index] = v
}

// GetMaxIndex gets placeholder max index number
func (t *triggerHelperTest) GetMaxIndex() int {
	return len(*t.internalValues)
}

// GetInternalValues get placeholder values
func (t *triggerHelperTest) GetInternalValues() tree.Datums {
	return *t.internalValues
}

// InitContext inits context
func (t *triggerHelperTest) InitContext() {
	t.execCtx.Init()
	t.execCtx.Fn = t.fn
	//t.execCtx.GetResultFn = GetPlanResultColumn
	//t.execCtx.RunPlanFn = RunPlanInsideProcedure
	//t.execCtx.StartPlanFn = StartPlanInsideProcedure
	t.execCtx.TriggerReplaceValues = t
}

func TestSpExecContext_SetExternalVariable(t *testing.T) {
	sourceValues := make(tree.Datums, 1)
	var trigger triggerHelperTest
	trigger.internalValues = &sourceValues
	var testPlaceholer tree.PlaceholderInfo
	testPlaceholer.Types = append(testPlaceholer.Types, types.Int)
	testPlaceholer.Values = append(testPlaceholer.Values, tree.NewDInt(40))
	trigger.placeholders = &testPlaceholer
	ctx := &SpExecContext{TriggerReplaceValues: &trigger}
	ctx.Init()

	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)

	// Set variable value
	datum := tree.NewDInt(42)
	ctx.SetExternalVariable(0, datum)

	require.Equal(t, datum, sourceValues[0])
	require.Equal(t, datum, testPlaceholer.Values[0])
}

func TestSpExecContext_GetVariableName(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	require.Equal(t, "var1", ctx.GetVariableName(0))
	require.Empty(t, ctx.GetVariableName(999)) // Non-existent index
}

func TestSpExecContext_SetVariableType(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	// Set variable type
	ctx.SetVariableType(0, *types.String)
	
	require.Equal(t, *types.String, ctx.localVariable[0].Typ)
}

func TestSpExecContext_GetVariable(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	require.Equal(t, var1, ctx.GetVariable(0))
}

func TestSpExecContext_GetLocalVariable(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	
	vars := ctx.GetLocalVariable()
	require.Len(t, vars, 1)
	require.Equal(t, var1, vars[0])
}

func TestSpExecContext_GetLocalCurSorMap(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a cursor
	cursor1 := &CursorHelper{}
	ctx.AddCursor("cursor1", cursor1)
	
	cursors := ctx.GetLocalCurSorMap()
	require.Len(t, cursors, 1)
	require.NotNil(t, cursors["cursor1"])
}

func TestSpExecContext_GetVariableLen(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	require.Equal(t, 0, ctx.GetVariableLen())
	
	// Add a variable
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	ctx.AddVariable(var1)
	require.Equal(t, 1, ctx.GetVariableLen())
}

func TestSpExecContext_AddCursor(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a cursor
	cursor1 := &CursorHelper{}
	ctx.AddCursor("cursor1", cursor1)
	
	require.Len(t, ctx.localCurSorMap, 1)
	require.NotNil(t, ctx.localCurSorMap["cursor1"])
}

func TestSpExecContext_FindCursor(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a cursor
	cursor1 := &CursorHelper{}
	ctx.AddCursor("cursor1", cursor1)
	
	// Find existing cursor
	foundCursor := ctx.FindCursor("cursor1")
	require.NotNil(t, foundCursor)
	
	// Find non-existent cursor
	notFoundCursor := ctx.FindCursor("cursor999")
	require.Nil(t, notFoundCursor)
}

func TestSpExecContext_SetHandler(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Create a handler
	handler := &HandlerHelper{Typ: tree.NOTFOUND}
	ctx.SetHandler(tree.ModeContinue, handler)
	
	require.Equal(t, handler, ctx.handler[tree.ModeContinue])
}

func TestSpExecContext_SetExceptionErr(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Set an exception error
	testErr := pgerror.Newf(pgcode.DatatypeMismatch, "test error")
	ctx.SetExceptionErr(testErr)
	
	require.Equal(t, testErr, ctx.GetExceptionErr())
}

func TestSpExecContext_addReturn(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a return
	handler := &HandlerHelper{}
	testErr := pgerror.Newf(pgcode.DatatypeMismatch, "test error")
	ctx.addReturn(HandlerLabel, handler, testErr)
	
	require.True(t, ctx.NeedReturn())
	require.Equal(t, HandlerLabel, ctx.ReturnLabel())
	require.Equal(t, handler, ctx.GetReturnHandler())
	require.Equal(t, testErr, ctx.GetExceptionErr())
}

func TestSpExecContext_CheckAndRemoveReturn(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a return
	ctx.addReturn("testLabel", nil, nil)
	require.True(t, ctx.NeedReturn())
	
	// Check and remove return with matching label
	ctx.CheckAndRemoveReturn("testLabel")
	require.False(t, ctx.NeedReturn())
	require.Empty(t, ctx.ReturnLabel())
}

func TestSpExecContext_NeedReturn(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	require.False(t, ctx.NeedReturn())
	
	// Add a return
	ctx.addReturn("testLabel", nil, nil)
	require.True(t, ctx.NeedReturn())
}

func TestSpExecContext_ContinueStatus(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	require.False(t, ctx.ContinueStatus())
	
	// Add a return with default label
	ctx.addReturn(DefaultLabel, nil, nil)
	require.True(t, ctx.ContinueStatus())
}

func TestSpExecContext_ReturnLabel(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	require.Empty(t, ctx.ReturnLabel())
	
	// Add a return
	ctx.addReturn("testLabel", nil, nil)
	require.Equal(t, "testLabel", ctx.ReturnLabel())
}

func TestSpExecContext_GetProcedureTxn(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Get default transaction state
	defaultState := ctx.GetProcedureTxn()
	require.Equal(t, tree.ProcedureTransactionDefault, defaultState)
	
	// Set and get custom transaction state
	customState := tree.ProcedureTxnState(1) // START
	ctx.SetProcedureTxn(customState)
	require.Equal(t, customState, ctx.GetProcedureTxn())
}

func TestSpExecContext_IsSetChildStackIndex(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Test with default label
	ctx.addReturn(DefaultLabel, nil, nil)
	require.True(t, ctx.IsSetChildStackIndex())
	
	// Test with handler label
	ctx.addReturn(HandlerLabel, nil, nil)
	require.True(t, ctx.IsSetChildStackIndex())
	
	// Test with into label
	ctx.addReturn(IntoLabel, nil, nil)
	require.True(t, ctx.IsSetChildStackIndex())
	
	// Test with custom label
	ctx.addReturn("customLabel", nil, nil)
	require.False(t, ctx.IsSetChildStackIndex())
}

func TestSpExecContext_Close(t *testing.T) {
	ctx := &SpExecContext{}
	ctx.Init()
	
	// Add a return
	ctx.addReturn("testLabel", &HandlerHelper{}, nil)
	
	// Close the context
	ctx.Close(context.Background())
	
	// Check that resources were cleaned up
	require.Nil(t, ctx.results)
	require.Empty(t, ctx.ret.labelName)
	require.False(t, ctx.ret.retFlag)
	require.Nil(t, ctx.ret.handler)
}