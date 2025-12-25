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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

// ExecHandler execute handler
func (h *HandlerHelper) ExecHandler(
	params RunParam, rCtx *SpExecContext, mod tree.HandlerMode,
) error {
	if err := h.Action.Execute(params, rCtx); err != nil {
		return err
	}
	if mod == tree.ModeExit {
		// if label is 'default', it cannot exit and must continue to fetch the result set. label should be set to 'Handler' instead.
		if rCtx.NeedReturn() && rCtx.ReturnLabel() == DefaultLabel {
			rCtx.addReturn(HandlerLabel, h, nil)
		} else {
			rCtx.addReturn(EndLabel, nil, nil)
		}
	}
	return nil
}

// dealWithHandleByTypeAndMode executes handler by type and mode
func dealWithHandleByTypeAndMode(
	params RunParam, rCtx *SpExecContext, typ tree.HandlerType, mod tree.HandlerMode,
) (bool, error) {
	flag := false
	h := rCtx.getHandlerByMode(mod)
	if h != nil && rCtx.getHandlerByMode(mod).checkFlag(typ) {
		flag = true
		rCtx.SetHandler(mod, nil)
		err := h.ExecHandler(params, rCtx, mod)
		rCtx.SetHandler(mod, h)
		if err != nil {
			return flag, err
		}
	}
	return flag, nil
}

// dealWithHandleByType executes handler by type
func dealWithHandleByType(
	params RunParam, rCtx *SpExecContext, typ tree.HandlerType,
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

func (ins *StmtIns) getPlan(rCtx *SpExecContext) (Plan, error) {
	p, err := rCtx.Fn(ins.rootExpr, ins.pr, rCtx.GetLocalVariable())
	if err != nil {
		return nil, err
	}

	return p, err
}

// getPlanAndContainer gets plan and container
// memo.Expr generates planNode through CBO optimization and logical planning compilation
func (ins *StmtIns) getPlanAndContainer(
	rCtx *SpExecContext, cols *sqlbase.ResultColumns, mon *mon.BytesMonitor,
) (Plan, *rowcontainer.RowContainer, error) {
	p, err := ins.getPlan(rCtx)
	if err != nil {
		return nil, nil, err
	}
	plan, col := rCtx.GetResultFn(p)
	*cols = col
	return plan, getNewContainer(mon, *cols), err
}

// Execute for sql plan
func (ins *StmtIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	if ins.createDefault {
		ins.createDefault = false
		rCtx.CheckAndRemoveReturn(DefaultLabel)
		rCtx.ResetExecuteStatus()
		return nil
	}
	newResult := QueryResult{StmtType: ins.typ}
	if err := ins.executeImplement(params, rCtx, &newResult); err != nil {
		return err
	}
	if ins.typ == tree.Rows && newResult.Result.Len() > 0 {
		rCtx.AddQueryResult(&newResult)
		ins.createDefault = true
		rCtx.addReturn(DefaultLabel, nil, nil)
	}

	return nil
}

// Close that close resource
func (ins *StmtIns) Close() {}

func (ins *StmtIns) executeImplement(
	params RunParam, rCtx *SpExecContext, newResult *QueryResult,
) error {
	plan, res, err := ins.getPlanAndContainer(rCtx, &newResult.ResultCols, params.EvalContext().Mon)
	if err != nil {
		return err
	}
	//defer plan.close(params.ctx)
	newResult.Result = res
	old := params.GetTxn()
	defer func() {
		if !params.GetTxn().IsOpen() {
			params.SetTxn(old)
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
		params.NewTxn()
	} else if params.EvalContext().IsTrigger && params.GetTxn().IsOpen() {
		// we should exec Step to flush seq of txn if trigger exec.
		if err := params.GetTxn().Step(params.GetCtx(), false); err != nil {
			rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
			return err
		}
	}

	rowsAffected, err := rCtx.RunPlanFn(params, plan, newResult.Result, ins.typ)
	if err != nil {
		// check cancel status
		if errCancel := params.GetCtx().Err(); errCancel != nil {
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
		if exitIsExec && rCtx.NeedReturn() && rCtx.ReturnLabel() == HandlerLabel {
			rCtx.SetExceptionErr(err)
			return nil
		}
		newResult.Result.Close(params.GetCtx())
		newResult.Result = nil
		return err
	}
	// not found for exit handle
	if ins.typ == tree.Rows {
		if newResult.Result.Len() == 0 {
			// check cancel status
			if errCancel := params.GetCtx().Err(); errCancel != nil {
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

// TransactionIns creates a Transaction instruction for procedure
type TransactionIns struct {
	// TxnType is Transaction Start or End State.(1: START;2: COMMIT; 3: ROLLBACK)
	TxnType tree.ProcedureTxnState
}

func checkTransactionEnd(params RunParam, rCtx *SpExecContext) error {
	if rCtx.GetProcedureTxn() != tree.ProcedureTransactionStart {
		if !params.GetTxn().IsOpen() {
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
	params RunParam, rCtx *SpExecContext, implicitTxnFlag *bool, commit bool,
) (err error) {
	if *implicitTxnFlag && rCtx.GetProcedureTxn() == tree.ProcedureTransactionStart {
		if commit {
			err = params.GetTxn().CommitOrCleanup(params.GetCtx())
		} else {
			err = params.GetTxn().Rollback(params.GetCtx())
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
func (ins *TransactionIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
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
		params.NewTxn()
	} else if ins.TxnType == tree.ProcedureTransactionRollBack {
		if err := checkTransactionEnd(params, rCtx); err != nil {
			return err
		}
		if err := params.Rollback(); err != nil {
			return err
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
	} else if ins.TxnType == tree.ProcedureTransactionCommit {
		if err := checkTransactionEnd(params, rCtx); err != nil {
			return err
		}
		if err := params.CommitOrCleanup(); err != nil {
			return err
		}
		rCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
	} else {
		return pgerror.Newf(pgcode.Syntax, "procedure explicit transaction state is incorrect: %v", ins.TxnType)
	}
	return nil
}

// Close that close resource
func (ins *TransactionIns) Close() {}

// IntoIns creates a stmt into value ins
type IntoIns struct {
	StmtIns
	dstVar []tree.IntoHelper
}

// Execute for IntoIns
func (ins *IntoIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	res := QueryResult{}
	defer func() {
		if res.Result != nil {
			res.Result.Close(params.GetCtx())
		}
	}()
	if err := ins.executeImplement(params, rCtx, &res); err != nil {
		return err
	}

	if rCtx.NeedReturn() && rCtx.ReturnLabel() == DefaultLabel {
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
		if rCtx.NeedReturn() && rCtx.ReturnLabel() == DefaultLabel {
			rCtx.addReturn(IntoLabel, nil, nil)
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		return pgerror.Newf(pgcode.StringDataLengthMismatch, "result consisted of more than one row for '%s'", ins.stmt)
	}

	// get next value into value
	for i, v := range ins.dstVar {
		var err error
		switch v.Type {
		case tree.IntoDeclareValue:
			err = rCtx.IntoDeclareVar(v.DeclareValueIdx, res.Result.At(0)[i], res.ResultCols[i])
		case tree.IntoUDFValue:
			value := res.Result.At(0)[i]
			t := value.ResolvedType()
			if idx, ok := rCtx.localVarMap[v.UDFValueName]; ok {
				rCtx.SetVariable(idx, &value)
			} else {
				rCtx.AddVariable(&exec.LocalVariable{Data: value, Typ: *t, Name: v.UDFValueName})
			}
			err = params.SetUserDefinedVar(v.UDFValueName, res.Result.At(0)[i])
		}

		if err != nil {
			contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
			if errHandler != nil {
				return errHandler
			}
			if rCtx.NeedReturn() && rCtx.ReturnLabel() == DefaultLabel {
				rCtx.addReturn(IntoLabel, nil, nil)
			}
			if contiuneIsExec && !exitIsExec {
				return nil
			}
			return err
		}
	}

	return nil
}

// Close that close resource
func (ins *IntoIns) Close() {}
