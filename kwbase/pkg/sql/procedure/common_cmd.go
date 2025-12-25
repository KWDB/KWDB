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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// BlockIns creates instruction slice
type BlockIns struct {
	ArrayIns

	// labelName saves label name of block
	labelName string

	// rCtx run context
	rCtx SpExecContext
}

// Close that close resource
func (ins *BlockIns) Close() {
	for _, v := range ins.childs {
		v.Close()
	}

	for _, cursorHelper := range ins.rCtx.GetLocalCurSorMap() {
		cursor := cursorHelper.CursorPtr
		if cursor.isOpen {
			cursor.Clear()
		}
	}
}

// Execute for declare
func (ins *BlockIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	// return directly with an empty block
	if len(ins.childs) == 0 {
		return nil
	}

	if !rCtx.ContinueExec() {
		ins.rCtx.Init()
		// inherit variable from parent ctx
		ins.rCtx.Inherit(rCtx)
	} else {
		ins.rCtx.InheritResult(rCtx)
	}

	if err := ins.ArrayIns.Execute(params, &ins.rCtx); err != nil {
		// when starting a transaction within a WHILE/IF, it is necessary to pass the transaction status out.
		if ins.rCtx.GetProcedureTxn() != rCtx.GetProcedureTxn() {
			rCtx.SetProcedureTxn(ins.rCtx.GetProcedureTxn())
		}
		return err
	}

	// when starting a transaction within a WHILE/IF, it is necessary to pass the transaction status out.
	if ins.rCtx.GetProcedureTxn() != rCtx.GetProcedureTxn() {
		rCtx.SetProcedureTxn(ins.rCtx.GetProcedureTxn())
	}

	if ins.rCtx.NeedReturn() {
		if (ins.rCtx.ReturnLabel() != "" && ins.labelName != ins.rCtx.ReturnLabel()) || ins.rCtx.ReturnLabel() == "" {
			rCtx.addReturn(ins.rCtx.ReturnLabel(), ins.rCtx.GetReturnHandler(), ins.rCtx.GetExceptionErr())
		}
	} else {
		rCtx.CheckAndRemoveReturn(DefaultLabel)
		rCtx.CheckAndRemoveReturn(IntoLabel)
	}

	return nil
}

// ArrayIns creates array instruction
type ArrayIns struct {
	// childStackIndex marks the location where sub instructions are executed
	childStackIndex int

	// opName is used to distinguish which operation it is
	opName string

	// childs stores instruction array
	childs []Instruction
}

// Close that close resource
func (ins *ArrayIns) Close() {
	for _, v := range ins.childs {
		v.Close()
	}
}

// Execute for ArrayIns
func (ins *ArrayIns) Execute(params RunParam, rCtx *SpExecContext) error {
	i := 0
	if rCtx.ContinueExec() {
		i = ins.childStackIndex
	}

	for ; i < len(ins.childs); i++ {
		// check cancel status
		if errCancel := params.GetCtx().Err(); errCancel != nil {
			return errCancel
		}
		err := ins.childs[i].Execute(params, rCtx)
		if err != nil {
			return err
		}

		if rCtx.IsSetChildStackIndex() {
			ins.childStackIndex = i
		}

		// return flag is true, we need return
		if rCtx.NeedReturn() {
			rCtx.CheckAndRemoveReturn(EndIfLabel)
			break
		}
	}

	return nil
}

// NextStep for ArrayIns
func (ins *ArrayIns) NextStep(params RunParam, rCtx *SpExecContext) error {
	i := ins.childStackIndex
	for ; i < len(ins.childs); i++ {
		err := ins.childs[i].Execute(params, rCtx)
		if err != nil {
			return err
		}

		if rCtx.ReturnLabel() == DefaultLabel {
			ins.childStackIndex = i
		}

		// return flag is true, we need return
		if rCtx.NeedReturn() {
			rCtx.CheckAndRemoveReturn(EndIfLabel)
			break
		}
	}

	return nil
}

// SetValueIns creates a new local variable for procedure
type SetValueIns struct {
	name       string
	index      int
	typ        *types.T
	sourceExpr tree.TypedExpr
	mode       tree.ProcedureValueMode // the value type for local variables inside procedure/trigger
}

// Close that close resource
func (ins *SetValueIns) Close() {}

// Execute for SetValueIns
func (ins *SetValueIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	// before eval need create indexVal container
	h := ExprHelper{}
	h.Init(rCtx, params.EvalContext(), ins.sourceExpr)
	valueTmp, err := h.Eval()
	if err != nil {
		return err
	}

	// check value by type
	value := valueTmp
	var err1 error
	if ins.mode != tree.UDFValue {
		value, err1 = sqlbase.LimitValueWidth(ins.typ, valueTmp, &ins.name)
		if err1 != nil {
			return err1
		}
	}

	switch ins.mode {
	case tree.DeclareValue:
		rCtx.AddVariable(&exec.LocalVariable{Data: value, Typ: *ins.typ, Name: ins.name})
	case tree.UDFValue:
		err2 := params.SetUserDefinedVar(ins.name, value)
		if err2 != nil {
			return err2
		}
	case tree.InternalValue:
		if ins.index < rCtx.GetVariableLen() && !ins.typ.Equivalent(&rCtx.GetVariable(ins.index).Typ) {
			rCtx.SetVariableType(ins.index, *ins.typ) // update value type
		}
		if ins.typ.Equivalent(value.ResolvedType()) || value == tree.DNull {
			rCtx.SetVariable(ins.index, &value)
		} else {
			return pgerror.Newf(pgcode.DatatypeMismatch, "variable %s type %s not save type %s ", ins.name, ins.typ.String(),
				value.ResolvedType().String())
		}
	case tree.ExternalValue:
		// replace placeholder with values inside EvalCtx
		if ins.typ.Equivalent(value.ResolvedType()) || value == tree.DNull {
			rCtx.SetExternalVariable(ins.index, &value)
		} else {
			return pgerror.Newf(pgcode.DatatypeMismatch, "variable %s type %s not save type %s ", ins.name, ins.typ.String(),
				value.ResolvedType().String())
		}
	}

	return nil
}

// LeaveIns save leave label name
type LeaveIns struct {
	labelName string
}

// Close that close resource
func (ins *LeaveIns) Close() {}

// Execute for LeaveIns
func (ins *LeaveIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	rCtx.addReturn(ins.labelName, nil, nil)
	return nil
}

// CaseWhenIns record condition and resultIns
type CaseWhenIns struct {
	// condition for check
	condition tree.TypedExpr

	// breakFlag is used to mark when a condition needs to break at what result
	breakFlag bool

	// resultIns stores instruction executed when the condition is true
	resultIns Instruction

	// conditionResult saves the result of executing the condition
	conditionResult bool
}

// Close that close resource
func (ins *CaseWhenIns) Close() {
	ins.resultIns.Close()
}

// Execute for CaseWhenIns
func (ins *CaseWhenIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	var success bool
	var err error
	if !rCtx.ContinueExec() {
		h := ExprHelper{}
		h.Init(rCtx, params.EvalContext(), ins.condition)
		success, err = h.EvalCompare()
		if err != nil {
			return err
		}

		if success {
			err = ins.resultIns.Execute(params, rCtx)
		}
		ins.conditionResult = success
	} else {
		success = ins.conditionResult
		err = ins.resultIns.Execute(params, rCtx)
	}

	if ins.breakFlag == success && !rCtx.NeedReturn() {
		rCtx.addReturn(EndIfLabel, nil, nil)
	}

	return err
}

// LoopIns creates a loop for procedure
type LoopIns struct {
	labelName string

	// child stores instruction
	child Instruction
}

// Close that close resource
func (ins *LoopIns) Close() {
	ins.child.Close()
}

// Execute for loop
func (ins *LoopIns) Execute(params RunParam, rCtx *SpExecContext) error {
	for {
		// check cancel status
		if errCancel := params.GetCtx().Err(); errCancel != nil {
			return errCancel
		}
		err := ins.child.Execute(params, rCtx)
		if err != nil {
			return err
		}

		// block return flag is true, we need return
		if rCtx.NeedReturn() {
			if rCtx.ReturnLabel() == ins.labelName {
				// find leave instruction
				rCtx.CheckAndRemoveReturn(ins.labelName)
			} else if rCtx.ReturnLabel() == EndIfLabel {
				// while condition is false
				rCtx.CheckAndRemoveReturn(EndIfLabel)
			}

			return nil
		}
	}
}

// SetHandlerIns creates a new local handler
type SetHandlerIns struct {
	HandlerHelper
	handlerMode tree.HandlerMode
}

// Close that close resource
func (ins *SetHandlerIns) Close() {}

// Execute for SetHandlerIns
func (ins *SetHandlerIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	rCtx.SetHandler(ins.handlerMode, &HandlerHelper{Typ: ins.Typ, Action: ins.Action})
	return nil
}
