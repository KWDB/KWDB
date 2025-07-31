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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

// Instruction represents a well-typed expression.
type Instruction interface {
	// Execute starts execute instruction
	Execute(params runParams, rCtx *procedure.SpExecContext) error
}

// BlockIns creates instruction slice
type BlockIns struct {
	ArrayIns

	// labelName saves label name of block
	labelName string

	// rCtx run context
	rCtx procedure.SpExecContext
}

// Execute for declare
func (ins *BlockIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
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
			rCtx.AddReturn(ins.rCtx.ReturnLabel(), ins.rCtx.GetReturnHandler(), ins.rCtx.GetExceptionErr())
		}
	} else {
		rCtx.CheckAndRemoveReturn(procedure.DefaultLabel)
		rCtx.CheckAndRemoveReturn(procedure.IntoLabel)
	}

	return nil
}

// execHandler execute handler
func execHandler(
	params runParams, rCtx *procedure.SpExecContext, mod tree.HandlerMode, h *procedure.HandlerHelper,
) error {
	if err := h.Action.(Instruction).Execute(params, rCtx); err != nil {
		return err
	}
	if mod == tree.ModeExit {
		// if label is 'default', it cannot exit and must continue to fetch the result set. label should be set to 'Handler' instead.
		if rCtx.NeedReturn() && rCtx.ReturnLabel() == procedure.DefaultLabel {
			rCtx.AddReturn(procedure.HandlerLabel, h, nil)
		} else {
			rCtx.AddReturn(procedure.EndLabel, nil, nil)
		}
	}
	return nil
}

// dealWithHandleByTypeAndMode executes handler by type and mode
func dealWithHandleByTypeAndMode(
	params runParams, rCtx *procedure.SpExecContext, typ tree.HandlerType, mod tree.HandlerMode,
) (bool, error) {
	flag := false
	h := rCtx.GetHandlerByMode(mod)
	if h != nil && rCtx.GetHandlerByMode(mod).CheckFlag(typ) {
		flag = true
		rCtx.SetHandler(mod, nil)
		err := execHandler(params, rCtx, mod, h)
		rCtx.SetHandler(mod, h)
		if err != nil {
			return flag, err
		}
	}
	return flag, nil
}

// dealWithHandleByType executes handler by type
func dealWithHandleByType(
	params runParams, rCtx *procedure.SpExecContext, typ tree.HandlerType,
) (bool, bool, error) {
	continueIsExec, exitIsExec := false, false
	var err error
	if continueIsExec, err = dealWithHandleByTypeAndMode(params, rCtx, typ, tree.ModeContinue); err != nil {
		return continueIsExec, false, err
	}
	if !rCtx.NeedReturn() {
		if exitIsExec, err = dealWithHandleByTypeAndMode(params, rCtx, typ, tree.ModeExit); err != nil {
			return continueIsExec, exitIsExec, err
		}
	}
	return continueIsExec, exitIsExec, nil
}

// StmtIns creates a new sql for procedure
type StmtIns struct {
	// SQL string
	stmt string

	// rootExpr saves memo expr struct
	rootExpr memo.RelExpr // stmt plan node
	// pr saves physical Required
	pr *physical.Required
	//typ saves Statement type
	typ tree.StatementType

	// createDefault saves flag that find result
	createDefault bool
}

// NewStmtIns creates new stmt instruction
func NewStmtIns(
	stmt string, p memo.RelExpr, typ tree.StatementType, pr *physical.Required,
) *StmtIns {
	return &StmtIns{stmt: stmt, rootExpr: p, typ: typ, pr: pr}
}

// getPlanAndContainer gets plan and container
// memo.Expr generates planNode through CBO optimization and logical planning compilation
func (ins *StmtIns) getPlanAndContainer(
	rCtx *procedure.SpExecContext, cols *sqlbase.ResultColumns, mon *mon.BytesMonitor,
) (*planTop, *rowcontainer.RowContainer, error) {
	p, err := rCtx.Fn(ins.rootExpr, ins.pr, rCtx.GetLocalVariable())
	if err != nil {
		return nil, nil, err
	}
	plan := p.(*planTop)
	*cols = planColumns(plan.plan)
	return plan, getNewContainer(mon, *cols), err
}

func (ins *StmtIns) executeImplement(
	params runParams, rCtx *procedure.SpExecContext, newResult *procedure.QueryResult,
) error {
	plan, res, err := ins.getPlanAndContainer(rCtx, &newResult.ResultCols, params.EvalContext().Mon)
	if err != nil {
		return err
	}
	//defer plan.close(params.ctx)
	newResult.Result = res
	old := params.p.txn
	defer func() {
		if !params.p.txn.IsOpen() {
			params.p.txn = old
		}
	}()

	// implicit transaction is enabled by default.
	implicitTxnFlag := false
	if rCtx.GetProcedureTxn() == tree.ProcedureTransactionDefault {
		implicitTxnFlag = true
		rCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
		// NewTxnWithSteppingEnabled will create a snapshot of the current version,
		// and queries after the transaction is started will only be able to query data from the current version.
		// so we should use NewTxn.
		params.p.txn = kv.NewTxn(params.ctx, params.extendedEvalCtx.DB, params.extendedEvalCtx.NodeID)
	}

	rowsAffected, err := runPlanInsideProcedure(params, plan, newResult.Result, ins.typ)
	if err != nil {
		// check cancel status
		if errCancel := params.ctx.Err(); errCancel != nil {
			return errCancel
		}
		if err := dealImplicitTxn(params, rCtx, &implicitTxnFlag, false); err != nil {
			return err
		}
		contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
		if errHandler != nil {
			return errHandler
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		if exitIsExec && rCtx.NeedReturn() && rCtx.ReturnLabel() == procedure.HandlerLabel {
			rCtx.SetExceptionErr(err)
			return nil
		}
		newResult.Result.Close(params.ctx)
		newResult.Result = nil
		return err
	}
	// not found for exit handle
	if ins.typ == tree.Rows {
		if newResult.Result.Len() == 0 {
			// check cancel status
			if errCancel := params.ctx.Err(); errCancel != nil {
				return errCancel
			}
			// implicit transaction commit.
			if err := dealImplicitTxn(params, rCtx, &implicitTxnFlag, true); err != nil {
				return err
			}
			if _, _, err := dealWithHandleByType(params, rCtx, tree.NOTFOUND); err != nil {
				return err
			}
		}
	} else if ins.typ == tree.RowsAffected {
		newResult.RowsAffected = rowsAffected
	}
	// implicit transaction commit.
	if err := dealImplicitTxn(params, rCtx, &implicitTxnFlag, true); err != nil {
		return err
	}

	return nil
}

// Execute for sql plan
func (ins *StmtIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	if ins.createDefault {
		ins.createDefault = false
		rCtx.CheckAndRemoveReturn(procedure.DefaultLabel)
		rCtx.ResetExecuteStatus()
		return nil
	}
	newResult := procedure.QueryResult{StmtType: ins.typ}
	if err := ins.executeImplement(params, rCtx, &newResult); err != nil {
		return err
	}
	if ins.typ == tree.Rows && newResult.Result.Len() > 0 {
		rCtx.AddQueryResult(&newResult)
		ins.createDefault = true
		rCtx.AddReturn(procedure.DefaultLabel, nil, nil)
	}

	return nil
}

// Cursor creates a new sql for procedure cursor
type Cursor struct {
	//StmtIns for sql plan
	StmtIns

	// resultCols saves result columns
	resultCols sqlbase.ResultColumns

	isOpen bool

	// flow handle
	cursorHandle flowinfra.Flow

	// params for get next row data
	params   runParams
	ctx      context.Context
	planCtx  *PlanningCtx
	receiver *DistSQLReceiver
	plan     *planTop
}

// runSubqueryPlan runs sub query plans
// params : runParams
// plan : parent plan
// recv : row container for save results
// return run or not run and error
func runSubqueryPlan(params runParams, plan *planTop, recv *DistSQLReceiver) (bool, error) {
	if len(plan.subqueryPlans) > 0 {
		// curPlan is the outer plan, but *plan is the recompiled inner plan.
		// we must change curPlan to the inner plan If the inner plan refer to the subqueries(plan.subqueryPlan).
		// we must replace the curPlan and restore the original state before exiting.
		newPlan := *params.p
		newPlan.curPlan = *plan
		newPlan.extendedEvalCtx.Planner = &newPlan
		if !params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRunSubqueries(
			params.ctx,
			&newPlan,
			newPlan.extendedEvalCtx.copy,
			plan.subqueryPlans,
			recv,
			true,
		) {
			return true, recv.commErr
		}
	}

	return false, nil
}

// runPlanImplement implements logical for run plan
// Parameters:
// - params : run params
// - plan : logical plan for run
// - rowResultWriter : result container for save result
// - stType : StatementType
// - local : flags local plan
// - ignoreClose : flags ignore close plan
// Returns:
// - err : if has error
func runPlanImplement(
	params runParams,
	plan *planTop,
	rowResultWriter *RowResultWriter,
	stType tree.StatementType,
	local bool,
	ignoreClose bool,
) error {
	recv := getDistSQLReceiverByParam(params, rowResultWriter, stType)
	defer recv.Release()

	if ret, err := runSubqueryPlan(params, plan, recv); ret {
		if err1 := rowResultWriter.Err(); err1 != nil {
			return err1
		}
		return err
	}

	// Make a copy of the EvalContext, so it can be safely modified.
	evalCtx := params.p.ExtendedEvalContextCopy()
	evalCtx.IsProcedure = true
	defer func() {
		evalCtx.IsProcedure = false
	}()
	planCtx := getPlanCtxByParam(params, plan, evalCtx, recv.stmtType, local)

	// param for procedure, sql not close
	planCtx.ignoreClose = ignoreClose

	params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRun(
		params.ctx, evalCtx, planCtx, params.p.Txn(), plan.plan, recv, params.p.GetStmt(),
	)()
	if recv.commErr != nil {
		return recv.commErr
	}
	return rowResultWriter.err
}

// runPlanInsideProcedure runs plan in procedure
// Parameters:
// - params : run params
// - plan : logical plan for run
// - rowResultWriter : result container for save result
// - stType : StatementType
// Returns:
// - int : RowsAffected
// - err : if has error
func runPlanInsideProcedure(
	params runParams,
	plan *planTop,
	rowContainer *rowcontainer.RowContainer,
	stType tree.StatementType,
) (int, error) {
	rowResultWriter := NewRowResultWriter(rowContainer)

	distribute := shouldDistributePlan(
		params.ctx, params.p.SessionData().DistSQLMode, params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner, plan.plan)

	err := runPlanImplement(params, plan, rowResultWriter, stType, !distribute, false)
	if stType == tree.RowsAffected {
		return rowResultWriter.RowsAffected(), err
	}
	return 0, err
}

// getDistSQLReceiverByParam gets distsql receiver by param
// Parameters:
// - params : run params
// - rowResultWriter : result container for save result
// - stType : StatementType
// Returns:
// - DistSQLReceiver
func getDistSQLReceiverByParam(
	params runParams, rowResultWriter *RowResultWriter, stType tree.StatementType,
) *DistSQLReceiver {
	return MakeDistSQLReceiver(
		params.ctx, rowResultWriter, stType,
		params.extendedEvalCtx.ExecCfg.RangeDescriptorCache,
		params.extendedEvalCtx.ExecCfg.LeaseHolderCache,
		params.p.Txn(),
		func(ts hlc.Timestamp) {
			params.extendedEvalCtx.ExecCfg.Clock.Update(ts)
		},
		params.p.extendedEvalCtx.Tracing,
	)
}

// getPlanCtxByParam gets plan ctx by param
// Parameters:
// - params : run params
// - plan : logical plan for run
// - evalCtx : eval context
// - stmtType : StatementType
// - local : flag for local plan
// Returns:
// - PlanningCtx
func getPlanCtxByParam(
	params runParams,
	plan *planTop,
	evalCtx *extendedEvalContext,
	stmtType tree.StatementType,
	local bool,
) *PlanningCtx {
	var planCtx *PlanningCtx
	if local {
		planCtx = params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.newLocalPlanningCtx(params.ctx, evalCtx)
	} else {
		planCtx = params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.NewPlanningCtx(params.ctx, evalCtx, params.p.Txn())
	}

	// Always plan local.
	planCtx.isLocal = local
	plannerCopy := *params.p
	planCtx.planner = &plannerCopy
	planCtx.planner.curPlan = *plan
	planCtx.ExtendedEvalCtx.Planner = &plannerCopy
	planCtx.stmtType = stmtType
	planCtx.haveSubquery = true

	evalCtx.IsDisplayed = params.extendedEvalCtx.IsDisplayed
	return planCtx
}

// startInsidePlan is used to run a plan and gather the results in a row
// container, as part of the execution of an "outer" plan.
// Parameters:
// - params : run params
// - ins : cursor instruction
// - rowContainer : result container for save result
// - stType : StatementType
// - plan : logical plan for run
// Returns:
// - error if has
func startInsidePlan(
	params runParams,
	ins *Cursor,
	rowContainer *rowcontainer.RowContainer,
	stType tree.StatementType,
	plan *planTop,
) error {
	rowResultWriter := NewRowResultWriter(rowContainer)
	receiver := getDistSQLReceiverByParam(params, rowResultWriter, stType)

	if ret, err := runSubqueryPlan(params, plan, receiver); ret {
		if err1 := rowResultWriter.Err(); err1 != nil {
			return err1
		}
		return err
	}

	// Make a copy of the EvalContext, so it can be safely modified.
	evalCtx := params.p.ExtendedEvalContextCopy()
	planCtx := getPlanCtxByParam(params, plan, evalCtx, stType, false)

	flow, ctx := params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndStart(
		params.ctx, evalCtx, planCtx, params.p.Txn(), plan.plan, receiver, params.p.GetStmt(),
	)
	if receiver.commErr != nil {
		return receiver.commErr
	}
	ins.planCtx = planCtx
	ins.cursorHandle = flow
	ins.ctx = ctx
	ins.receiver = receiver
	ins.params = params
	return rowResultWriter.err
}

// getNewContainer gets new container
func getNewContainer(mon *mon.BytesMonitor, cols sqlbase.ResultColumns) *rowcontainer.RowContainer {
	colTypes := make([]types.T, len(cols))
	for i := range cols {
		colTypes[i] = *cols[i].Typ
	}
	typ := sqlbase.ColTypeInfoFromColTypes(colTypes)
	acc := mon.MakeBoundAccount()
	// TODO Consider processing large amounts of data
	return rowcontainer.NewRowContainer(acc, typ, 0 /* rowCapacity */)
}

// Start for sql plan
func (c *Cursor) Start(params runParams, rCtx *procedure.SpExecContext) error {
	plan, res, err := c.StmtIns.getPlanAndContainer(rCtx, &c.resultCols, params.EvalContext().Mon)
	if err != nil {
		return err
	}
	c.plan = plan
	err = startInsidePlan(params, c, res, c.typ, plan)
	if err != nil {
		// check cancel status
		if errCancel := params.ctx.Err(); errCancel != nil {
			return errCancel
		}
		contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
		if errHandler != nil {
			return errHandler
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		return err
	}
	c.isOpen = true
	return nil
}

// Next for Cursor
func (c *Cursor) Next(params runParams) (tree.Datums, error) {
	result := tree.Datums{}
	for {
		// check cancel status
		if errCancel := params.ctx.Err(); errCancel != nil {
			return result, errCancel
		}
		row, meta := c.cursorHandle.NextRow()
		if row != nil || meta != nil {
			if meta != nil {
				continue
			}

			for i := range row {
				if i < len(c.resultCols) {
					if err := row[i].EnsureDecoded(c.resultCols[i].Typ, &c.receiver.alloc); err != nil {
						return result, err
					}
					result = append(result, row[i].Datum)
				}
			}

			return result, nil
		}
		break
	}

	// todo consider ConsumerClosed for source
	return result, nil
}

// Clear for Cursor
func (c *Cursor) Clear() {
	for {
		row, meta := c.cursorHandle.NextRow()
		if row != nil || meta != nil {
			continue
		}
		break
	}
	if c.cursorHandle != nil {
		cleanup := c.params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.RunClearUp(c.ctx, c.planCtx, c.cursorHandle, c.receiver)
		c.cursorHandle.Wait()
		if cleanup != nil {
			cleanup()
		}
		c.receiver.Release()
		c.cursorHandle = nil
	}
	c.isOpen = false
}

// SetValueIns creates a new local variable for procedure
type SetValueIns struct {
	name       string
	typ        *types.T
	sourceExpr tree.TypedExpr
}

// NewSetIns creates new set instruction
func NewSetIns(name string, typ *types.T, source tree.TypedExpr) *SetValueIns {
	return &SetValueIns{name: name, typ: typ, sourceExpr: source}
}

// Execute for SetValueIns
func (ins *SetValueIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	// before eval need create indexVal container
	h := procedure.ExprHelper{}
	h.Init(rCtx, params.EvalContext(), ins.sourceExpr)
	valueTmp, err := h.Eval()
	if err != nil {
		return err
	}

	// check value by type
	value, err1 := sqlbase.LimitValueWidth(ins.typ, valueTmp, &ins.name)
	if err1 != nil {
		return err1
	}

	idx := rCtx.FindVariable(ins.name)
	if idx == -1 {
		rCtx.AddVariable(ins.name, &exec.LocalVariable{Data: value, Typ: *ins.typ})
	} else {
		if vars := rCtx.GetLocalVariable(); idx < len(vars) && !ins.typ.Equivalent(&vars[idx].Typ) {
			rCtx.SetVariableType(idx, *ins.typ)
		}
		if ins.typ.Equivalent(value.ResolvedType()) || value == tree.DNull {
			rCtx.SetVariable(idx, &value)
		} else { //
			return pgerror.Newf(pgcode.DatatypeMismatch, "variable %s type %s not save type %s ", ins.name, ins.typ.String(),
				value.ResolvedType().String())
		}
	}

	return nil
}

// SetHandlerIns creates a new local handler
type SetHandlerIns struct {
	procedure.HandlerHelper
	handlerMode tree.HandlerMode
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

// Execute for SetHandlerIns
func (ins *SetHandlerIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	rCtx.SetHandler(ins.handlerMode, &procedure.HandlerHelper{Typ: ins.Typ, Action: ins.Action})
	return nil
}

// LeaveIns save leave label name
type LeaveIns struct {
	labelName string
}

// Execute for LeaveIns
func (ins *LeaveIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	rCtx.AddReturn(ins.labelName, nil, nil)
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

// NewCaseWhenIns creates new CaseWhenIns instruction
func NewCaseWhenIns(condition tree.TypedExpr, breakFlag bool, resultIns Instruction) *CaseWhenIns {
	return &CaseWhenIns{condition: condition, breakFlag: breakFlag, resultIns: resultIns}
}

// NewEmptyCaseWhenIns creates new empty CaseWhenIns instruction
func NewEmptyCaseWhenIns() *CaseWhenIns {
	return &CaseWhenIns{}
}

// Execute for CaseWhenIns
func (ins *CaseWhenIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	var success bool
	var err error
	if !rCtx.ContinueExec() {
		h := procedure.ExprHelper{}
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
		rCtx.AddReturn(procedure.EndIfLabel, nil, nil)
	}

	return err
}

// LoopIns creates a loop for procedure
type LoopIns struct {
	labelName string

	// child stores instruction
	child Instruction
}

// NewLoopIns creates new loop instruction
func NewLoopIns(labelName string, child Instruction) *LoopIns {
	return &LoopIns{labelName: labelName, child: child}
}

// Execute for loop
func (ins *LoopIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	for {
		// check cancel status
		if errCancel := params.ctx.Err(); errCancel != nil {
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
			} else if rCtx.ReturnLabel() == procedure.EndIfLabel {
				// while condition is false
				rCtx.CheckAndRemoveReturn(procedure.EndIfLabel)
			}

			return nil
		}
	}
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

// NewArrayIns creates array instruction
func NewArrayIns(opName string, childs []Instruction) *ArrayIns {
	return &ArrayIns{opName: opName, childs: childs}
}

// Execute for ArrayIns
func (ins *ArrayIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	i := 0
	if rCtx.ContinueExec() {
		i = ins.childStackIndex
	}

	for ; i < len(ins.childs); i++ {
		// check cancel status
		if errCancel := params.ctx.Err(); errCancel != nil {
			return errCancel
		}
		err := ins.childs[i].Execute(params, rCtx)
		if err != nil {
			if fetchCursorIns, ok := ins.childs[i].(*FetchCursorIns); ok {
				if v := rCtx.FindCursor(string(fetchCursorIns.name)); v != nil {
					cursor := v.Action.(*Cursor)
					if cursor.isOpen {
						v.Action.(*Cursor).Clear()
					}
				}
			}
			return err
		}

		if rCtx.IsSetChildStackIndex() {
			ins.childStackIndex = i
		}

		// return flag is true, we need return
		if rCtx.NeedReturn() {
			rCtx.CheckAndRemoveReturn(procedure.EndIfLabel)
			break
		}
	}

	return nil
}

// NextStep for ArrayIns
func (ins *ArrayIns) NextStep(params runParams, rCtx *procedure.SpExecContext) error {
	i := ins.childStackIndex
	for ; i < len(ins.childs); i++ {
		err := ins.childs[i].Execute(params, rCtx)
		if err != nil {
			return err
		}

		if rCtx.ReturnLabel() == procedure.DefaultLabel {
			ins.childStackIndex = i
		}

		// return flag is true, we need return
		if rCtx.NeedReturn() {
			rCtx.CheckAndRemoveReturn(procedure.EndIfLabel)
			break
		}
	}

	return nil
}

// SetCursorIns creates a new local cursor
type SetCursorIns struct {
	name string
	cur  Cursor
}

// NewSetCursorIns creates new set instruction
func NewSetCursorIns(name string, source Cursor) *SetCursorIns {
	return &SetCursorIns{name: name, cur: source}
}

// Execute for SetCursorIns
func (ins *SetCursorIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	if ins.cur.typ != tree.Rows {
		return pgerror.Newf(pgcode.InvalidCursorState,
			"cursor %s StatementType %s only support Rows", ins.name, ins.cur.typ.String())
	}
	rCtx.AddCursor(ins.name, &procedure.CursorHelper{Action: &ins.cur})
	return nil
}

// OpenCursorIns saves open cursor name
type OpenCursorIns struct {
	name tree.Name
}

// NewOpenCursorIns creates new Open instruction
func NewOpenCursorIns(name tree.Name) *OpenCursorIns {
	return &OpenCursorIns{name: name}
}

// Execute for OpenCursorIns
func (ins *OpenCursorIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		c := v.Action.(*Cursor)
		if c.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is already open", ins.name)
		}
		return c.Start(params, rCtx)
	}
	return nil
}

// FetchCursorIns creates a fetch cursor ins
type FetchCursorIns struct {
	name      tree.Name
	dstVarIdx []int
}

// NewFetchCursorIns creates new FetchCursor instruction
func NewFetchCursorIns(name tree.Name, dstVarIdx []int) *FetchCursorIns {
	return &FetchCursorIns{name: name, dstVarIdx: dstVarIdx}
}

// Execute for SetCursorIns
func (ins *FetchCursorIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		cursor := v.Action.(*Cursor)
		if !cursor.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is not open", ins.name)
		}
		if len(cursor.resultCols) != len(ins.dstVarIdx) {
			return pgerror.Newf(pgcode.InvalidCursorState, "the number of columns fetched does not match the number of variables, cols: %v, variables:%v",
				len(cursor.resultCols), len(ins.dstVarIdx))
		}
		res, err := cursor.Next(params)
		if err != nil {
			// check cancel status
			if errCancel := params.ctx.Err(); errCancel != nil {
				return errCancel
			}
			contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
			if errHandler != nil {
				return errHandler
			}
			if contiuneIsExec && !exitIsExec {
				return nil
			}
			return err
		}

		// check next
		if len(res) == 0 {
			h := rCtx.GetHandlerByMode(tree.ModeContinue)
			if h != nil && h.CheckFlag(tree.NOTFOUND) {
				return h.Action.(Instruction).Execute(params, rCtx)
			}
			return pgerror.Newf(pgcode.InvalidCursorState, "the fetch cursor has no more data.")
		}
		// get next value into value
		if err := rCtx.SetVariables(ins.dstVarIdx, res, nil); err != nil {
			return err
		}
	}
	return nil
}

// CloseCursorIns creates a close cursor ins
type CloseCursorIns struct {
	name tree.Name
}

// NewCloseCursorIns creates new close Cursor instruction
func NewCloseCursorIns(name tree.Name) *CloseCursorIns {
	return &CloseCursorIns{name: name}
}

// Execute for SetCursorIns
func (ins *CloseCursorIns) Execute(_ runParams, rCtx *procedure.SpExecContext) error {
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		c := v.Action.(*Cursor)
		if !c.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is already close", ins.name)
		}
		c.Clear()
	}
	return nil
}

// TransactionIns creates a Transaction instruction for procedure
type TransactionIns struct {
	// TxnType is Transaction Start or End State.(1: START;2: COMMIT; 3: ROLLBACK)
	TxnType tree.ProcedureTxnState
}

// NewTransactionIns creates Transaction instruction
func NewTransactionIns(txnType tree.ProcedureTxnState) *TransactionIns {
	return &TransactionIns{TxnType: txnType}
}

func checkTransactionEnd(params runParams, rCtx *procedure.SpExecContext) error {
	if rCtx.GetProcedureTxn() != tree.ProcedureTransactionStart {
		if !params.p.txn.IsOpen() {
			return pgerror.Newf(pgcode.Syntax, "the explicit transaction of the procedure has either been ended, "+
				"please check if the transaction commit/rollback statements are used correctly.")
		}
		return pgerror.Newf(pgcode.Syntax, "the explicit transaction of the procedure was not opened")
	}
	if !params.EvalContext().TxnImplicit {
		return pgerror.Newf(pgcode.Syntax, "explicit transactions outside the procedure cannot be ended within the procedure.")
	}
	return nil
}

func dealImplicitTxn(
	params runParams, rCtx *procedure.SpExecContext, implicitTxnFlag *bool, commit bool,
) (err error) {
	if *implicitTxnFlag && rCtx.GetProcedureTxn() == tree.ProcedureTransactionStart {
		if commit {
			err = params.p.txn.CommitOrCleanup(params.ctx)
		} else {
			err = params.p.txn.Rollback(params.ctx)
		}
		if err != nil {
			return err
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
		*implicitTxnFlag = false
	}
	return nil
}

// Execute for TransactionIns
func (ins *TransactionIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	if ins.TxnType == tree.ProcedureTransactionStart {
		if rCtx.GetProcedureTxn() != tree.ProcedureTransactionDefault {
			return pgerror.Newf(pgcode.Syntax, "procedure explicit transaction has been started, please check if there are multiple explicit transaction start statements.")
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
		// NewTxnWithSteppingEnabled will create a snapshot of the current version,
		// and queries after the transaction is started will only be able to query data from the current version.
		// so we should use NewTxn.
		params.p.txn = kv.NewTxn(params.ctx, params.extendedEvalCtx.DB, params.extendedEvalCtx.NodeID)
	} else if ins.TxnType == tree.ProcedureTransactionRollBack {
		if err := checkTransactionEnd(params, rCtx); err != nil {
			return err
		}
		if err := params.p.txn.Rollback(params.ctx); err != nil {
			return err
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
	} else if ins.TxnType == tree.ProcedureTransactionCommit {
		if err := checkTransactionEnd(params, rCtx); err != nil {
			return err
		}
		if err := params.p.txn.CommitOrCleanup(params.ctx); err != nil {
			return err
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
	} else {
		return pgerror.Newf(pgcode.Syntax, "procedure explicit transaction state is incorrect: %v", ins.TxnType)
	}
	return nil
}

// IntoIns creates a stmt into value ins
type IntoIns struct {
	StmtIns
	dstVarIdx []int
}

// NewIntoIns creates new IntoIns instruction
func NewIntoIns(query StmtIns, dstIdx []int) *IntoIns {
	return &IntoIns{StmtIns: query, dstVarIdx: dstIdx}
}

// Execute for IntoIns
func (ins *IntoIns) Execute(params runParams, rCtx *procedure.SpExecContext) error {
	// check cancel status
	if errCancel := params.ctx.Err(); errCancel != nil {
		return errCancel
	}
	res := procedure.QueryResult{}
	defer func() {
		if res.Result != nil {
			res.Result.Close(params.ctx)
		}
	}()
	if err := ins.executeImplement(params, rCtx, &res); err != nil {
		return err
	}

	if rCtx.NeedReturn() && rCtx.ReturnLabel() == procedure.DefaultLabel {
		return nil
	}

	// get result into dest variable
	if res.Result == nil || res.Result.Len() == 0 {
		return nil
	}

	if res.Result.Len() > 1 {
		contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
		if errHandler != nil {
			return errHandler
		}
		if rCtx.NeedReturn() && rCtx.ReturnLabel() == procedure.DefaultLabel {
			rCtx.AddReturn(procedure.IntoLabel, nil, nil)
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		return pgerror.Newf(pgcode.StringDataLengthMismatch, "result consisted of more than one row for '%s'", ins.stmt)
	}

	// get next value into value
	if err := rCtx.SetVariables(ins.dstVarIdx, res.Result.At(0), res.ResultCols); err != nil {
		contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
		if errHandler != nil {
			return errHandler
		}
		if rCtx.NeedReturn() && rCtx.ReturnLabel() == procedure.DefaultLabel {
			rCtx.AddReturn(procedure.IntoLabel, nil, nil)
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		return err
	}

	return nil
}
