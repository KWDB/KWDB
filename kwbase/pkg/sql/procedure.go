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
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// GetCtx gets context
func (r *runParams) GetCtx() context.Context {
	return r.ctx
}

// SetUserDefinedVar sets user defined var
func (r *runParams) SetUserDefinedVar(name string, v tree.Datum) error {
	return r.p.sessionDataMutator.SetUserDefinedVar(name, v)
}

// DeallocatePrepare deallocate prepare
func (r *runParams) DeallocatePrepare(name string) error {
	if name == "" {
		r.p.preparedStatements.DeleteAll(r.ctx)
	} else {
		if found := r.p.preparedStatements.Delete(r.ctx, name); !found {
			return pgerror.Newf(pgcode.InvalidSQLStatementName,
				"prepared statement %q does not exist", name)
		}
	}
	return nil
}

// GetTxn returns txn
func (r *runParams) GetTxn() *kv.Txn {
	return r.p.Txn()
}

// SetTxn sets txn for plan
func (r *runParams) SetTxn(t *kv.Txn) {
	r.p.txn = t
}

// NewTxn creates new txn for plan
func (r *runParams) NewTxn() {
	r.p.txn = kv.NewTxn(r.GetCtx(), r.extendedEvalCtx.DB, r.extendedEvalCtx.NodeID)
}

// Rollback controls txn rollback
func (r *runParams) Rollback() error {
	return r.p.txn.Rollback(r.ctx)
}

// CommitOrCleanup controls txn Commit Or Cleanup
func (r *runParams) CommitOrCleanup() error {
	return r.p.txn.CommitOrCleanup(r.ctx)
}

// CursorExecHelper saves all info for execute cursor
type CursorExecHelper struct {
	// flow handle
	cursorHandle flowinfra.Flow

	// params for get next row data
	params   *runParams
	ctx      context.Context
	planCtx  *PlanningCtx
	receiver *DistSQLReceiver
	plan     *planTop
}

// RunClearUp controls txn Commit Or Cleanup
func (c *CursorExecHelper) RunClearUp() {
	if c.cursorHandle != nil {
		cleanup := c.params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.RunClearUp(c.ctx, c.planCtx, c.cursorHandle, c.receiver)
		c.cursorHandle.Wait()
		if cleanup != nil {
			cleanup()
		}
		c.receiver.Release()
		c.cursorHandle = nil
	}
}

// NextRow gets plan next row data for cursor
func (c *CursorExecHelper) NextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return c.cursorHandle.NextRow()
}

// GetDatumAlloc gets datum alloc
func (c *CursorExecHelper) GetDatumAlloc() *sqlbase.DatumAlloc {
	return &c.receiver.alloc
}

// GetPlanResultColumn gets plan result Column from procedure plan
func GetPlanResultColumn(p procedure.Plan) (procedure.Plan, sqlbase.ResultColumns) {
	plan := p.(*planTop)
	return plan, planColumns(plan.plan)
}

// runSubqueryPlan runs sub query plans
// params : runParams
// plan : parent plan
// recv : row container for save results
// return run or not run and error
func runSubqueryPlan(params *runParams, plan *planTop, recv *DistSQLReceiver) (bool, error) {
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
	recv := getDistSQLReceiverByParam(&params, rowResultWriter, stType)
	defer recv.Release()

	if ret, err := runSubqueryPlan(&params, plan, recv); ret {
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
	planCtx := getPlanCtxByParam(&params, plan, evalCtx, recv.stmtType, local)

	// param for procedure, sql not close
	planCtx.ignoreClose = ignoreClose

	params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRun(
		params.ctx, evalCtx, planCtx, params.p.Txn(), plan.plan, recv, params.p.GetStmt(), nil,
	)()
	if recv.commErr != nil {
		return recv.commErr
	}
	return rowResultWriter.err
}

// RunPlanInsideProcedure runs plan in procedure
// Parameters:
// - params : run params
// - plan : logical plan for run
// - rowResultWriter : result container for save result
// - stType : StatementType
// Returns:
// - int : RowsAffected
// - err : if has error
func RunPlanInsideProcedure(
	paramsInterface procedure.RunParam,
	planInterface procedure.Plan,
	rowContainer *rowcontainer.RowContainer,
	stType tree.StatementType,
) (int, error) {
	plan := planInterface.(*planTop)
	params := paramsInterface.(*runParams)
	rowResultWriter := NewRowResultWriter(rowContainer)

	distribute := shouldDistributePlan(
		params.ctx, params.p.SessionData().DistSQLMode, params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner, plan.plan)

	err := runPlanImplement(*params, plan, rowResultWriter, stType, !distribute, false)
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
	params *runParams, rowResultWriter *RowResultWriter, stType tree.StatementType,
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
	params *runParams,
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

// StartPlanInsideProcedure is used to run a plan and gather the results in a row
// container, as part of the execution of an "outer" plan.
// Parameters:
// - params : run params
// - ins : cursor instruction
// - rowContainer : result container for save result
// - stType : StatementType
// - plan : logical plan for run
// Returns:
// - error if has
func StartPlanInsideProcedure(
	paramsInterface procedure.RunParam,
	help procedure.CursorExec,
	rowContainer *rowcontainer.RowContainer,
	stType tree.StatementType,
	planInterface procedure.Plan,
) error {
	params := paramsInterface.(*runParams)
	rowResultWriter := NewRowResultWriter(rowContainer)
	receiver := getDistSQLReceiverByParam(params, rowResultWriter, stType)

	plan := planInterface.(*planTop)
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

	execHelper := help.(*CursorExecHelper)
	execHelper.planCtx = planCtx
	execHelper.cursorHandle = flow
	execHelper.ctx = ctx
	execHelper.receiver = receiver
	execHelper.params = params

	return rowResultWriter.err
}
