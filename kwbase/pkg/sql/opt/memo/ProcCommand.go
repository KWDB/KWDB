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

package memo

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// CmdType is instruction type
type CmdType int

const (
	// ProcSet sets variable a new value
	ProcSet CmdType = iota
	// ProcSTMT is a logical plan
	ProcSTMT
	// ProcExpr is a tree.expr
	ProcExpr
	// ProcWhile is cmd loop operator
	ProcWhile
	// ProcIf is cmd if else condition operator
	ProcIf
	// ProcBLOCK is cmd slice
	ProcBLOCK
	// ProcLeave is cmd leave condition operator
	ProcLeave
	// ProcDeclareVar is cmd declare variable condition operator
	ProcDeclareVar
	// ProcDeclareCursor is cmd declare cursor condition operator
	ProcDeclareCursor
	// ProcDeclareHandler is cmd declare handle condition operator
	ProcDeclareHandler
	// ProcedureTxn is cmd transaction
	ProcedureTxn
	// ProcOpenCursor is cmd open cursor condition operator
	ProcOpenCursor
	// ProcFetchCursor is cmd fetch cursor condition operator
	ProcFetchCursor
	// ProcCloseCursor is cmd close cursor condition operator
	ProcCloseCursor
	// ProcedureInto is cmd select/delete/update into condition operator
	ProcedureInto
	// ProcArray  is cmd for proc array
	ProcArray
	// ProcPrepare is cmd for proc prepare
	ProcPrepare
	// ProcExecute is cmd for proc execute
	ProcExecute
	// ProcDeallocate is cmd for proc deallocate
	ProcDeallocate
)

// ProcComms represents an array of ProcCommand
type ProcComms ProcCommand

// ProcCommand represents a sql block in procedure.
type ProcCommand interface {
	// Type returns the instruction type
	Type() CmdType
}

// ArrayCommand creates ProcCommand slice
type ArrayCommand struct {
	Bodys []ProcCommand
}

// Type implements the ProcCommand interface for BlockCommand.
func (b *ArrayCommand) Type() CmdType {
	return ProcArray
}

// BlockCommand creates ProcCommand slice
type BlockCommand struct {
	Label string
	Body  ArrayCommand
	// params...
}

// Type implements the ProcCommand interface for BlockCommand.
func (b *BlockCommand) Type() CmdType {
	return ProcBLOCK
}

// StmtCommand creates instruction slice
type StmtCommand struct {
	Name         string
	Body         RelExpr
	PhysicalProp *physical.Required
	Typ          tree.StatementType
	// params...
}

// Type implements the ProcCommand interface for StmtCommand.
func (b *StmtCommand) Type() CmdType {
	return ProcSTMT
}

// IntoCommand creates instruction slice
type IntoCommand struct {
	Name         string
	Body         RelExpr
	PhysicalProp *physical.Required
	IntoValue    []tree.IntoHelper // represents the type of variable: Declared(in procedure ) or UDV
}

// Type implements the ProcCommand interface for IntoCommand.
func (b *IntoCommand) Type() CmdType {
	return ProcedureInto
}

// PrepareCommand creates instruction slice
type PrepareCommand struct {
	Name  tree.Name              // prepare name
	Res   prepare.PreparedResult // prepare struct
	AddFn prepare.PreparedAddFn  // the function that add prepare struct to map
}

// Type implements the ProcCommand interface for PrepareCommand.
func (b *PrepareCommand) Type() CmdType {
	return ProcPrepare
}

// ExecuteCommand execute instruction slice
type ExecuteCommand struct {
	Execute       *tree.Execute
	PhInfo        *tree.PlaceholderInfo
	GetMemoFn     prepare.ExecuteGetMemoFn
	StatementType int
	SQLStr        string
}

// Type implements the ProcCommand interface for ExecuteCommand.
func (b *ExecuteCommand) Type() CmdType {
	return ProcExecute
}

// DeallocateCommand deallocate instruction slice
type DeallocateCommand struct {
	Name string
}

// Type implements the ProcCommand interface for DeallocateCommand.
func (b *DeallocateCommand) Type() CmdType {
	return ProcDeallocate
}

// IfCommand creates instruction slice
type IfCommand struct {
	Cond opt.ScalarExpr
	Then ProcCommand
}

// Type implements the ProcCommand interface for IfCommand.
func (b *IfCommand) Type() CmdType {
	return ProcIf
}

// DeclareVarCommand creates instruction slice
type DeclareVarCommand struct {
	Col DeclareCol
}

// Type implements the ProcCommand interface for DeclareVarCommand.
func (b *DeclareVarCommand) Type() CmdType {
	return ProcDeclareVar
}

// DeclareCol creates instruction slice
type DeclareCol struct {
	Name    tree.Name
	Typ     *types.T
	Default tree.TypedExpr
	Idx     int
	Mode    tree.ProcedureValueMode // the value type for local variables inside procedure/trigger
}

// WhileCommand creates instruction slice
type WhileCommand struct {
	Label string
	Cond  tree.TypedExpr
	Then  []ProcCommand
}

// Type implements the ProcCommand interface for WhileCommand.
func (b *WhileCommand) Type() CmdType {
	return ProcWhile
}

// LeaveCommand creates instruction slice
type LeaveCommand struct {
	Label string
}

// Type implements the ProcCommand interface for LeaveCommand.
func (b *LeaveCommand) Type() CmdType {
	return ProcLeave
}

// Equal implements the ProcCommand interface for LeaveCommand.
func (b *LeaveCommand) Equal(o ProcCommand) bool {
	if t, ok := o.(*LeaveCommand); ok {
		return b.Label == t.Label
	}
	return false
}

// ProcedureTxnCommand creates instruction slice
type ProcedureTxnCommand struct {
	TxnType tree.ProcedureTxnState
}

// Type implements the ProcCommand interface for ProcedureTxnCommand.
func (b *ProcedureTxnCommand) Type() CmdType {
	return ProcedureTxn
}

// OpenCursorCommand creates instruction slice
type OpenCursorCommand struct {
	Name tree.Name
}

// Type implements the ProcCommand interface for OpenCursorCommand.
func (b *OpenCursorCommand) Type() CmdType {
	return ProcOpenCursor
}

// Equal implements the ProcCommand interface for OpenCursorCommand.
func (b *OpenCursorCommand) Equal(o ProcCommand) bool {
	if cmd, ok := o.(*OpenCursorCommand); ok {
		if b.Name == cmd.Name {
			return true
		}
	}
	return false
}

// FetchCursorCommand creates instruction slice
type FetchCursorCommand struct {
	Name   tree.Name
	DstVar []int
}

// Type implements the ProcCommand interface for FetchCursorCommand.
func (b *FetchCursorCommand) Type() CmdType {
	return ProcFetchCursor
}

// CloseCursorCommand creates instruction slice
type CloseCursorCommand struct {
	Name tree.Name
}

// Type implements the ProcCommand interface for CloseCursorCommand.
func (b *CloseCursorCommand) Type() CmdType {
	return ProcCloseCursor
}

// Equal implements the ProcCommand interface for CloseCursorCommand.
func (b *CloseCursorCommand) Equal(o ProcCommand) bool {
	if cmd, ok := o.(*CloseCursorCommand); ok {
		if b.Name == cmd.Name {
			return true
		}
	}
	return false
}

// DeclCursorCommand creates instruction slice
type DeclCursorCommand struct {
	Name tree.Name
	Body ProcCommand
}

// Type implements the ProcCommand interface for DeclCursorCommand.
func (b *DeclCursorCommand) Type() CmdType {
	return ProcDeclareCursor
}

// DeclHandlerCommand creates instruction slice
type DeclHandlerCommand struct {
	HandlerOp  tree.HandlerMode
	HandlerFor tree.HandlerType
	Block      ProcCommand
}

// Type implements the ProcCommand interface for DeclHandlerCommand.
func (b *DeclHandlerCommand) Type() CmdType {
	return ProcDeclareHandler
}
