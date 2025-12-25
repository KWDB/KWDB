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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// DefaultLabel default label name for break
const DefaultLabel = "Default"

// EndIfLabel flags endif
const EndIfLabel = "endif"

// EndLabel flags end
const EndLabel = "end"

// HandlerLabel flags handler
const HandlerLabel = "handler"

// IntoLabel flags into
const IntoLabel = "into"

// RunParam is the interface of runParams
type RunParam interface {
	GetCtx() context.Context

	EvalContext() *tree.EvalContext

	SetUserDefinedVar(name string, v tree.Datum) error

	DeallocatePrepare(name string) error

	GetTxn() *kv.Txn

	SetTxn(t *kv.Txn)

	NewTxn()

	Rollback() error

	CommitOrCleanup() error
}

// Plan flags plan top interface
type Plan interface {
}

// CursorExec flags cursor execute interface
type CursorExec interface {
	// RunClearUp controls txn Commit Or Cleanup
	RunClearUp()
	// NextRow gets plan next row data for cursor
	NextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata)
	// GetDatumAlloc gets datum alloc
	GetDatumAlloc() *sqlbase.DatumAlloc
}

// GetResultCallBKFn func for get result columns
type GetResultCallBKFn func(p Plan) (Plan, sqlbase.ResultColumns)

// RunPlanCallBKFn func for run plan
type RunPlanCallBKFn func(r RunParam, p Plan, rc *rowcontainer.RowContainer, s tree.StatementType) (int, error)

// StartPlanCallBKFn func for start plan
type StartPlanCallBKFn func(
	r RunParam, h CursorExec, rc *rowcontainer.RowContainer, s tree.StatementType, p Plan,
) error

// QueryResult saves a sql query result
type QueryResult struct {
	// ResultCols saves all result columns info
	ResultCols sqlbase.ResultColumns

	// Result saves all result data by row container
	Result *rowcontainer.RowContainer

	// RowIdx saves read row index
	RowIdx uint32

	// RowsAffected for affected rows by delete/update
	RowsAffected int

	// StmtType flags query type
	StmtType tree.StatementType
}

// Result save all sql query result
type Result struct {
	res []*QueryResult

	// readIdx flags index for get result
	readIdx uint32
}

// AddQueryResult adds a sql query result
func (r *Result) AddQueryResult(result *QueryResult) {
	r.res = append(r.res, result)
}

// HasNextResult returns true for has next result
func (r *Result) HasNextResult() bool {
	r.readIdx++
	if r.readIdx >= uint32(len(r.res)) {
		return false
	}
	return true
}

// CheckResultExist checks result exist
func (r *Result) CheckResultExist() bool {
	return r.res != nil && len(r.res) > 0
}

// GetNextResultCols get next result columns
func (r *Result) GetNextResultCols() sqlbase.ResultColumns {
	if r.res != nil && int(r.readIdx) < len(r.res) {
		return r.res[r.readIdx].ResultCols
	}
	return nil
}

// GetResultTypeAndAffected get next result query type
func (r *Result) GetResultTypeAndAffected() (tree.StatementType, int) {
	return r.res[r.readIdx].StmtType, r.res[r.readIdx].RowsAffected
}

// Next returns a row data
func (r *Result) Next() (bool, error) {
	if r.res == nil {
		return false, nil
	}
	res := r.res[r.readIdx]
	res.RowIdx++
	index := res.RowIdx - 1
	if int(index) >= res.Result.Len() {
		return false, nil
	}

	return true, nil
}

// Values returns a row data
func (r *Result) Values() tree.Datums {
	res := r.res[r.readIdx]
	return res.Result.At(int(res.RowIdx - 1))
}

// Close for delete row container
func (r *Result) Close(ctx context.Context) {
	for i := range r.res {
		r.res[i].Result.Close(ctx)
	}
}

// ReturnHelper save all block return info
type ReturnHelper struct {
	labelName string
	retFlag   bool
	handler   *HandlerHelper
	// SQLEXCEPTION error
	exceptionErr error
}

// AddReturnLabel add return label and flag return
func (r *ReturnHelper) AddReturnLabel(label string) {
	r.retFlag = true
	r.labelName = label
}

// AddReturnHandler if we should set label of 'handler', add handler to ReturnHelper
func (r *ReturnHelper) AddReturnHandler(handler *HandlerHelper) {
	r.handler = handler
}

// RemoveReturn removes return flag and label
func (r *ReturnHelper) RemoveReturn() {
	r.retFlag = false
	r.labelName = ""
}

// NeedReturn returns return flag
func (r *ReturnHelper) NeedReturn() bool {
	return r.retFlag
}

// HandlerHelper save exit handler type and action
type HandlerHelper struct {
	// Typ
	// 0000 0001  -> NOT FOUND
	// 0000 0010  -> SQLEXCEPTION
	// 0000 0011  -> NOT FOUND,SQLEXCEPTION
	Typ tree.HandlerType

	// Action saves Subsequent processing actions
	Action Instruction
}

// checkFlag returns flag
func (h *HandlerHelper) checkFlag(flag tree.HandlerType) bool {
	return h.Typ&flag != 0
}

// CursorHelper save cursor action and result
type CursorHelper struct {
	// CursorPtr saves Subsequent processing cursor
	CursorPtr *Cursor
}
