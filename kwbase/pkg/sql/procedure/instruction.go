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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// Instruction represents a well-typed expression.
type Instruction interface {
	// Execute starts execute instruction
	Execute(params RunParam, rCtx *SpExecContext) error

	// Close will close all resource
	Close()
}

// NewArrayIns creates array instruction
func NewArrayIns(opName string, childs []Instruction) *ArrayIns {
	return &ArrayIns{opName: opName, childs: childs}
}

// NewBlockIns creates block instruction
func NewBlockIns(label string, child ArrayIns) *BlockIns {
	return &BlockIns{labelName: label, ArrayIns: child}
}

// NewBlockInsFromSlice creates block instruction
func NewBlockInsFromSlice(label string, childs []Instruction) *BlockIns {
	return &BlockIns{labelName: label, ArrayIns: ArrayIns{childs: childs}}
}

// NewCaseWhenIns creates new CaseWhenIns instruction
func NewCaseWhenIns(condition tree.TypedExpr, breakFlag bool, resultIns Instruction) *CaseWhenIns {
	return &CaseWhenIns{condition: condition, breakFlag: breakFlag, resultIns: resultIns}
}

// NewEmptyCaseWhenIns creates new empty CaseWhenIns instruction
func NewEmptyCaseWhenIns() *CaseWhenIns {
	return &CaseWhenIns{}
}

// NewCloseCursorIns creates new close Cursor instruction
func NewCloseCursorIns(name tree.Name) *CloseCursorIns {
	return &CloseCursorIns{name: name}
}

// NewFetchCursorIns creates new FetchCursor instruction
func NewFetchCursorIns(name tree.Name, dstVarIdx []int) *FetchCursorIns {
	return &FetchCursorIns{name: name, dstVarIdx: dstVarIdx}
}

// NewIntoIns creates new IntoIns instruction
func NewIntoIns(query StmtIns, dstIdx []tree.IntoHelper) *IntoIns {
	return &IntoIns{StmtIns: query, dstVar: dstIdx}
}

// NewLeaveIns creates new leave instruction
func NewLeaveIns(label string) *LeaveIns {
	return &LeaveIns{labelName: label}
}

// NewLoopIns creates new loop instruction
func NewLoopIns(labelName string, child Instruction) *LoopIns {
	return &LoopIns{labelName: labelName, child: child}
}

// NewOpenCursorIns creates new Open instruction
func NewOpenCursorIns(name tree.Name) *OpenCursorIns {
	return &OpenCursorIns{name: name}
}

// NewPrepareIns creates new prepare instruction
func NewPrepareIns(
	name tree.Name, res prepare.PreparedResult, addFn prepare.PreparedAddFn,
) *PrepareIns {
	return &PrepareIns{prepareName: name, res: res, addFn: addFn}
}

// NewExecuteIns creates new execute instruction
func NewExecuteIns(
	e *tree.Execute,
	pInfo *tree.PlaceholderInfo,
	getMemoFn prepare.ExecuteGetMemoFn,
	statementType int,
	sqlStr string,
) *ExecuteIns {
	return &ExecuteIns{
		exec:          e,
		phInfo:        pInfo,
		getMemoFn:     getMemoFn,
		statementType: statementType,
		sqlStr:        sqlStr,
	}
}

// NewDeallocateIns creates new deallocate instruction
func NewDeallocateIns(name string) *DeallocateIns {
	return &DeallocateIns{name: name}
}

// NewSetCursorIns creates new set instruction
func NewSetCursorIns(name string, source Cursor) *SetCursorIns {
	return &SetCursorIns{name: name, cur: source}
}

// NewSetHandlerIns creates new SetHandler instruction
func NewSetHandlerIns(
	forTyp tree.HandlerType, action Instruction, mod tree.HandlerMode,
) *SetHandlerIns {
	var handler SetHandlerIns
	handler.Typ = forTyp
	handler.Action = action
	handler.handlerMode = mod
	return &handler
}

// NewSetIns creates new set instruction
func NewSetIns(
	name string, index int, typ *types.T, source tree.TypedExpr, mode tree.ProcedureValueMode,
) *SetValueIns {
	return &SetValueIns{name: name, index: index, typ: typ, sourceExpr: source, mode: mode}
}

// NewStmtIns creates new stmt instruction
func NewStmtIns(
	stmt string, p memo.RelExpr, typ tree.StatementType, pr *physical.Required,
) *StmtIns {
	return &StmtIns{stmt: stmt, rootExpr: p, typ: typ, pr: pr}
}

// NewCursor creates new cursor instruction
func NewCursor(
	stmt string, p memo.RelExpr, typ tree.StatementType, pr *physical.Required, execHelper CursorExec,
) *Cursor {
	return &Cursor{StmtIns: StmtIns{stmt: stmt, rootExpr: p, typ: typ, pr: pr}, execHelper: execHelper}
}

// NewTransactionIns creates Transaction instruction
func NewTransactionIns(txnType tree.ProcedureTxnState) *TransactionIns {
	return &TransactionIns{TxnType: txnType}
}
