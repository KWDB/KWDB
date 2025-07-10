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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type callProcedureNode struct {
	procName string
	procCall string
	// run procedure params
	// execCtx execute context
	execCtx procedure.SpExecContext

	// ins execute block
	ins Instruction

	// params saves run param for next result execute
	params runParams

	// err saves error
	err error

	// fn saves function that optimize and build
	fn exec.ProcedurePlanFn
}

func (n *callProcedureNode) endTransaction(txnImplicit bool) {
	// the explicit transaction was not properly completed.
	if txnImplicit && n.execCtx.GetProcedureTxn() != tree.ProcedureTransactionDefault {
		if err := n.params.p.txn.Rollback(n.params.ctx); err != nil {
			n.err = err
		}
		n.execCtx.SetProcedureTxn(tree.ProcedureTransactionDefault)
		if n.err == nil {
			panic(pgerror.Newf(pgcode.Syntax,
				"procedure explicit transaction has not been ended, please check explicit transaction start statements."))
		}
	}
}

func (n *callProcedureNode) startExec(params runParams) error {
	// Ensure all nodes are the correct version.
	if !params.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionUDR) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"all nodes are not at the correct version to use Stored Procedures")
	}
	if !params.p.extendedEvalCtx.TxnImplicit {
		return pgerror.New(pgcode.FeatureNotSupported, "Call Procedure statement is not supported in explicit transaction")
	}
	n.execCtx.Init()
	n.execCtx.Fn = n.fn
	n.params = params
	if err := n.ins.Execute(params, &n.execCtx); err != nil {
		if params.p.extendedEvalCtx.TxnImplicit && n.execCtx.GetProcedureTxn() != tree.ProcedureTransactionDefault {
			if err := params.p.txn.Rollback(params.ctx); err != nil {
				n.err = err
				return err
			}
		}
		n.err = err
		return err
	}
	return nil
}

func (n *callProcedureNode) ReadingOwnWrites() {}

// CheckResultExist checks result exist
func (n *callProcedureNode) CheckResultExist() bool {
	return n.execCtx.CheckResultExist()
}

func (n *callProcedureNode) Next(params runParams) (bool, error) {
	return n.execCtx.Next()
}

func (n *callProcedureNode) Values() tree.Datums {
	return n.execCtx.Values()
}

func (n *callProcedureNode) Close(ctx context.Context) {
	n.execCtx.Close(ctx)
	if blockIns, ok := n.ins.(*BlockIns); ok {
		// When the procedure execution is completed, clear the unreleased cursor
		for _, cursorHelper := range blockIns.rCtx.GetLocalCurSorMap() {
			cursor := cursorHelper.Action.(*Cursor)
			if cursor.isOpen {
				cursor.Clear()
			}
		}
	}
}

// HasNextResult checks next result for procedure . The stored procedure will generate multiple result sets,
// and it is necessary to use this interface to determine whether there are result sets
func (n *callProcedureNode) HasNextResult(txnImplicit bool) (bool, error) {
	isExecHandler := false
	var returnHandler *procedure.HandlerHelper
	var exceptionErr error
	// since the ExitHandler execution ends with the termination of the procedure,
	// if the result set is generated from the ExitHandler, we only need to return to the ExitHandler and continue executing.
	// returnHandler record the ExitHandler.
	if n.execCtx.ReturnLabel() == procedure.HandlerLabel && n.execCtx.GetReturnHandler() != nil {
		isExecHandler = true
		returnHandler = n.execCtx.GetReturnHandler()
		exceptionErr = n.execCtx.GetExceptionErr()
	}
	n.execCtx.Close(n.params.ctx)
	if n.err != nil {
		return false, n.err
	}
	n.execCtx.InitResult()
	// if label is 'Handler', we should continue executing returnHandler.
	if isExecHandler {
		if err := execHandler(n.params, &n.execCtx, tree.ModeExit, returnHandler); err != nil {
			return false, err
		}
		if n.execCtx.NeedReturn() && n.execCtx.ReturnLabel() == procedure.EndLabel && exceptionErr != nil {
			n.err = exceptionErr
			n.endTransaction(txnImplicit)
			return false, n.err
		}
	} else if err := n.ins.Execute(n.params, &n.execCtx); err != nil {
		n.err = err
		n.endTransaction(txnImplicit)
		return false, n.err
	}

	ret := n.execCtx.CheckResultExist()

	if !ret {
		n.endTransaction(txnImplicit)
	}

	return ret, n.err
}

// GetNextResultCols get next result columns
func (n *callProcedureNode) GetNextResultCols() sqlbase.ResultColumns {
	return n.execCtx.GetNextResultCols()
}

// GetResultTypeAndAffected get next result query type
func (n *callProcedureNode) GetResultTypeAndAffected() (tree.StatementType, int) {
	return n.execCtx.GetResultTypeAndAffected()
}
