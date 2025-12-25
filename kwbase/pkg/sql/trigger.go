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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

type triggerHelper struct {
	beforeIns procedure.Instruction
	afterIns  procedure.Instruction
	execCtx   procedure.SpExecContext
	// helper function, which is used to do optimization and generate execPlan for stmts in trigger
	fn exec.ProcedurePlanFn
	// origin DML sourceValues, which can be mutated by trigger stmts.
	internalValues *tree.Datums
	// current placeHolderInfo in evalCtx, used to records values corresponding to placeHolder idx,
	// it should be mutated at the same time when origin DML sourceValue is changed.
	placeholders *tree.PlaceholderInfo
}

// GetValue get placeholder value
func (t *triggerHelper) GetValue(index tree.PlaceholderIdx) tree.Datum {
	return (*t.internalValues)[index]
}

// SetValue set placeholder value
func (t *triggerHelper) SetValue(index tree.PlaceholderIdx, v tree.Datum) {
	(*t.internalValues)[index] = v
	t.placeholders.Values[index] = v
}

// GetMaxIndex gets placeholder max index number
func (t *triggerHelper) GetMaxIndex() int {
	return len(*t.internalValues)
}

// GetInternalValues get placeholder values
func (t *triggerHelper) GetInternalValues() tree.Datums {
	return *t.internalValues
}

// InitContext inits context
func (t *triggerHelper) InitContext() {
	t.execCtx.Init()
	t.execCtx.Fn = t.fn
	t.execCtx.GetResultFn = GetPlanResultColumn
	t.execCtx.RunPlanFn = RunPlanInsideProcedure
	t.execCtx.StartPlanFn = StartPlanInsideProcedure
	t.execCtx.TriggerReplaceValues = t
}

// NeedExecuteTrigger returns whether a trigger needs to be executed
func (t *triggerHelper) NeedExecuteTrigger() bool {
	return t.beforeIns != nil || t.afterIns != nil
}

// NeedExecuteBeforeTrigger returns whether we need to execute beforeTrigger before
// DML execution or not.
func (t *triggerHelper) NeedExecuteBeforeTrigger() bool {
	return t.beforeIns != nil
}

// NeedExecuteAfterTrigger returns whether we need to execute afterTrigger after
// DML execution or not.
func (t *triggerHelper) NeedExecuteAfterTrigger() bool {
	return t.afterIns != nil
}

// ExecuteIns executes instruction
func (t *triggerHelper) ExecuteIns(
	params runParams, sourceValue *tree.Datums, ins procedure.Instruction,
) error {
	t.internalValues = sourceValue

	if ins == nil {
		return nil
	}

	t.InitContext()
	// Save old placeholder info. Must restore after instruction group execution
	// to prevent affecting subsequent instructions.
	oldValues := params.extendedEvalCtx.TriggerColHolders.Values
	oldTypes := params.extendedEvalCtx.TriggerColHolders.Types
	defer func() {
		params.extendedEvalCtx.TriggerColHolders.Values = oldValues
		params.extendedEvalCtx.TriggerColHolders.Types = oldTypes
	}()
	params.extendedEvalCtx.TriggerColHolders.Values = make(tree.QueryArguments, t.execCtx.TriggerReplaceValues.GetMaxIndex())
	params.extendedEvalCtx.TriggerColHolders.Types = make(tree.PlaceholderTypes, t.execCtx.TriggerReplaceValues.GetMaxIndex())
	// Assigns the node's source value to replace PlaceHolder information in evalCtx
	for i := 0; i < t.execCtx.TriggerReplaceValues.GetMaxIndex(); i++ {
		params.extendedEvalCtx.TriggerColHolders.Values[i] = t.execCtx.TriggerReplaceValues.GetValue(tree.PlaceholderIdx(i))
		params.extendedEvalCtx.TriggerColHolders.Types[i] = params.extendedEvalCtx.TriggerColHolders.Values[i].ResolvedType()
	}

	t.placeholders = params.extendedEvalCtx.TriggerColHolders
	t.execCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
	//params.p.txn.Step(params.ctx, false)
	params.EvalContext().IsTrigger = true
	defer func() {
		params.EvalContext().IsTrigger = false
	}()
	err := ins.Execute(&params, &t.execCtx)
	if err != nil {
		wrapErr := errors.Wrap(err, "TriggeredActionException")
		return pgerror.Newf(pgcode.TriggeredActionException, wrapErr.Error())
	}
	return nil
}

// ExecuteBeforeIns executes before instruction
func (t *triggerHelper) ExecuteBeforeIns(params runParams, sourceValue *tree.Datums) error {
	return t.ExecuteIns(params, sourceValue, t.beforeIns)
}

// ExecuteAfterIns executes after instruction
func (t *triggerHelper) ExecuteAfterIns(params runParams, sourceValue *tree.Datums) error {
	return t.ExecuteIns(params, sourceValue, t.afterIns)
}
