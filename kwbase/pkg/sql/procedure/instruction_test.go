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

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestNewArrayIns(t *testing.T) {
	// Create mock instructions
	mockIns1 := &MockInstruction{}
	mockIns2 := &MockInstruction{}
	childs := []Instruction{mockIns1, mockIns2}

	// Create ArrayIns
	arrayIns := NewArrayIns("testOp", childs)

	require.Equal(t, "testOp", arrayIns.opName)
	require.Len(t, arrayIns.childs, 2)
	require.Equal(t, mockIns1, arrayIns.childs[0])
	require.Equal(t, mockIns2, arrayIns.childs[1])
}

func TestNewBlockIns(t *testing.T) {
	// Create ArrayIns
	arrayIns := ArrayIns{
		childs: []Instruction{&MockInstruction{}},
	}

	// Create BlockIns
	blockIns := NewBlockIns("testLabel", arrayIns)

	require.Equal(t, "testLabel", blockIns.labelName)
	require.Len(t, blockIns.childs, 1)
}

func TestNewBlockInsFromSlice(t *testing.T) {
	// Create mock instructions
	mockIns1 := &MockInstruction{}
	mockIns2 := &MockInstruction{}
	childs := []Instruction{mockIns1, mockIns2}

	// Create BlockIns
	blockIns := NewBlockInsFromSlice("testLabel", childs)

	require.Equal(t, "testLabel", blockIns.labelName)
	require.Len(t, blockIns.childs, 2)
	require.Equal(t, mockIns1, blockIns.childs[0])
	require.Equal(t, mockIns2, blockIns.childs[1])
}

func TestNewCaseWhenIns(t *testing.T) {
	// Create mock condition and result instruction
	condition := tree.MakeDBool(true)
	resultIns := &MockInstruction{}

	// Create CaseWhenIns
	caseWhenIns := NewCaseWhenIns(condition, true, resultIns)

	require.Equal(t, condition, caseWhenIns.condition)
	require.True(t, caseWhenIns.breakFlag)
	require.Equal(t, resultIns, caseWhenIns.resultIns)
}

func TestNewEmptyCaseWhenIns(t *testing.T) {
	// Create empty CaseWhenIns
	caseWhenIns := NewEmptyCaseWhenIns()

	require.Nil(t, caseWhenIns.condition)
	require.False(t, caseWhenIns.breakFlag)
	require.Nil(t, caseWhenIns.resultIns)
}

func TestNewCloseCursorIns(t *testing.T) {
	// Create CloseCursorIns
	name := tree.Name("testCursor")
	closeCursorIns := NewCloseCursorIns(name)

	require.Equal(t, name, closeCursorIns.name)
}

func TestNewFetchCursorIns(t *testing.T) {
	// Create FetchCursorIns
	name := tree.Name("testCursor")
	dstVarIdx := []int{0, 1, 2}
	fetchCursorIns := NewFetchCursorIns(name, dstVarIdx)

	require.Equal(t, name, fetchCursorIns.name)
	require.Equal(t, dstVarIdx, fetchCursorIns.dstVarIdx)
}

func TestNewLeaveIns(t *testing.T) {
	// Create LeaveIns
	label := "testLabel"
	leaveIns := NewLeaveIns(label)

	require.Equal(t, label, leaveIns.labelName)
}

func TestNewLoopIns(t *testing.T) {
	// Create mock instruction
	child := &MockInstruction{}

	// Create LoopIns
	label := "testLabel"
	loopIns := NewLoopIns(label, child)

	require.Equal(t, label, loopIns.labelName)
	require.Equal(t, child, loopIns.child)
}

func TestNewOpenCursorIns(t *testing.T) {
	// Create OpenCursorIns
	name := tree.Name("testCursor")
	openCursorIns := NewOpenCursorIns(name)

	require.Equal(t, name, openCursorIns.name)
}

func addMemAndSavePrepared(
	ctx context.Context, prepareResult prepare.PreparedResult, name string,
) error {
	return nil
}

func TestNewPrepareIns(t *testing.T) {
	// Create PrepareIns
	name := tree.Name("testPrepare")
	var res prepare.PreparedResult
	prepareIns := NewPrepareIns(name, res, addMemAndSavePrepared)

	require.Equal(t, name, prepareIns.prepareName)
	require.Equal(t, res, prepareIns.res)
	// require.Equal(t, addMemAndSavePrepared, &prepareIns.addFn)
}

func TestNewExecuteIns(t *testing.T) {
	// Create ExecuteIns
	exec := &tree.Execute{}
	var pInfo *tree.PlaceholderInfo
	var getMemoFn prepare.ExecuteGetMemoFn
	statementType := 1
	sqlStr := "SELECT * FROM test"
	executeIns := NewExecuteIns(exec, pInfo, getMemoFn, statementType, sqlStr)

	require.Equal(t, exec, executeIns.exec)
	require.Equal(t, pInfo, executeIns.phInfo)
	// require.Equal(t, getMemoFn, executeIns.getMemoFn)
	require.Equal(t, statementType, executeIns.statementType)
	require.Equal(t, sqlStr, executeIns.sqlStr)
}

func TestNewDeallocateIns(t *testing.T) {
	// Create DeallocateIns
	name := "testPrepare"
	deallocateIns := NewDeallocateIns(name)

	require.Equal(t, name, deallocateIns.name)
}

func TestNewSetCursorIns(t *testing.T) {
	// Create SetCursorIns
	name := "testCursor"
	cursor := Cursor{}
	setCursorIns := NewSetCursorIns(name, cursor)

	require.Equal(t, name, setCursorIns.name)
	require.Equal(t, cursor, setCursorIns.cur)
}

func TestNewSetHandlerIns(t *testing.T) {
	// Create SetHandlerIns
	forTyp := tree.NOTFOUND
	action := &MockInstruction{}
	mod := tree.ModeContinue
	setHandlerIns := NewSetHandlerIns(forTyp, action, mod)

	require.Equal(t, forTyp, setHandlerIns.Typ)
	require.Equal(t, action, setHandlerIns.Action)
	require.Equal(t, mod, setHandlerIns.handlerMode)
}

func TestNewSetIns(t *testing.T) {
	// Create SetValueIns
	name := "testVar"
	index := 0
	typ := &types.Int
	source := tree.NewDInt(42)
	mode := tree.DeclareValue
	setIns := NewSetIns(name, index, *typ, source, mode)

	require.Equal(t, name, setIns.name)
	require.Equal(t, index, setIns.index)
	require.Equal(t, *typ, setIns.typ)
	require.Equal(t, source, setIns.sourceExpr)
	require.Equal(t, mode, setIns.mode)
}

func TestNewStmtIns(t *testing.T) {
	// Create StmtIns
	stmt := "SELECT * FROM test"
	var rootExpr memo.RelExpr
	typ := tree.Rows
	var pr *physical.Required
	stmtIns := NewStmtIns(stmt, rootExpr, typ, pr)

	require.Equal(t, stmt, stmtIns.stmt)
	require.Equal(t, rootExpr, stmtIns.rootExpr)
	require.Equal(t, typ, stmtIns.typ)
	require.Equal(t, pr, stmtIns.pr)
}

func TestNewCursor(t *testing.T) {
	// Create Cursor
	stmt := "SELECT * FROM test"
	var rootExpr memo.RelExpr
	typ := tree.Rows
	var pr *physical.Required
	var execHelper CursorExec
	cursor := NewCursor(stmt, rootExpr, typ, pr, execHelper)

	require.Equal(t, stmt, cursor.stmt)
	require.Equal(t, rootExpr, cursor.rootExpr)
	require.Equal(t, typ, cursor.typ)
	require.Equal(t, pr, cursor.pr)
	require.Equal(t, execHelper, cursor.execHelper)
}

func TestNewTransactionIns(t *testing.T) {
	// Create TransactionIns
	txnType := tree.ProcedureTxnState(1) // START
	transactionIns := NewTransactionIns(txnType)

	require.Equal(t, txnType, transactionIns.TxnType)
}
