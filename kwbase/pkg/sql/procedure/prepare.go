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
	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// PrepareIns creates instruction slice
type PrepareIns struct {
	prepareName tree.Name
	res         prepare.PreparedResult
	addFn       prepare.PreparedAddFn
}

// Close that close resource
func (ins *PrepareIns) Close() {}

// Execute for PrepareIns
func (ins *PrepareIns) Execute(params RunParam, _ *SpExecContext) error {
	return ins.addFn(params.GetCtx(), ins.res, string(ins.prepareName))
}

// ExecuteIns creates a execute ins
type ExecuteIns struct {
	exec          *tree.Execute
	phInfo        *tree.PlaceholderInfo
	getMemoFn     prepare.ExecuteGetMemoFn
	statementType int
	sqlStr        string
	// createDefault saves flag that find result
	createDefault bool
}

// Close that close resource
func (ins *ExecuteIns) Close() {}

// Execute for ExecuteIns
func (ins *ExecuteIns) Execute(params RunParam, rCtx *SpExecContext) error {
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

	memoInterface, err := ins.getMemoFn(params.GetCtx(), ins.exec, ins.phInfo)
	if err != nil {
		return err
	}

	replacedMemo := memoInterface.(*memo.Memo)
	rootExpr := replacedMemo.RootExpr()
	r := rootExpr.(memo.RelExpr)

	stmtIns := StmtIns{
		stmt:     ins.sqlStr,
		rootExpr: r,
		pr:       replacedMemo.RootProps(),
		typ:      tree.StatementType(ins.statementType),
	}

	res := QueryResult{StmtType: stmtIns.typ}
	if err := stmtIns.executeImplement(params, rCtx, &res); err != nil {
		return err
	}

	if stmtIns.typ == tree.Rows && res.Result.Len() > 0 {
		rCtx.AddQueryResult(&res)
		ins.createDefault = true
		rCtx.addReturn(DefaultLabel, nil, nil)
	}

	return nil
}

// DeallocateIns creates a deallocate ins
type DeallocateIns struct {
	name string
}

// Close that close resource
func (ins *DeallocateIns) Close() {}

// Execute for DeallocateIns
func (ins *DeallocateIns) Execute(params RunParam, rCtx *SpExecContext) error {
	err := params.DeallocatePrepare(ins.name)
	return err
}
