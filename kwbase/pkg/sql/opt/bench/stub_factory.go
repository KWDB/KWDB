// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package bench

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// stubFactory is a do-nothing implementation of exec.Factory, used for testing.
type stubFactory struct{}

var _ exec.Factory = &stubFactory{}

func (f *stubFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSynchronizer(input exec.Node, degree int64) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructTSScan is part of the exec.Factory interface.
func (f *stubFactory) ConstructTSScan(
	table cat.Table,
	private *memo.TSScanPrivate,
	tagFilter, primaryFilter, tagIndexFilter []tree.TypedExpr,
	rowCount float64,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructVirtualScan(table cat.Table) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructFilter is part of the exec.Factory interface.
func (f *stubFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering, execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (f *stubFactory) ConstructSimpleProject(
	n exec.Node,
	cols []exec.ColumnOrdinal,
	colNames []string,
	reqOrdering exec.OutputOrdering,
	execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRender(
	n exec.Node,
	exprs tree.TypedExprs,
	colNames []string,
	reqOrdering exec.OutputOrdering,
	execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

// for multiple model processing.
func (f *stubFactory) ConstructBatchLookUpJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	right memo.RelExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	funcs *opt.AggFuncNames,
	execInTSEngine bool,
	private *memo.GroupingPrivate,
	addSynchronizer bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScalarGroupBy(
	input exec.Node,
	aggregations []exec.AggInfo,
	funcs *opt.AggFuncNames,
	execInTSEngine bool,
	private *memo.GroupingPrivate,
	addSynchronizer bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.ColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
	execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int, execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.ColumnOrdinal,
	tableCols exec.ColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.ColumnOrdinal,
	eqColsAreKey bool,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []exec.ColumnOrdinal,
	rightEqCols []exec.ColumnOrdinal,
	leftCols exec.ColumnOrdinalSet,
	rightCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	fixedVals []exec.Node,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr, limitExpr memo.RelExpr, meta *opt.Metadata,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMax1Row(input exec.Node, errorText string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructWindow(
	n exec.Node, wi exec.WindowInfo, execInTSEngine bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) RenameColumns(input exec.Node, colNames []string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, postqueries []exec.Node,
) (exec.Plan, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan, mem *memo.Memo,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.ColumnOrdinalSet,
	returnCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructTSInsert(
	PayloadNodeInfo map[int]*sqlbase.PayloadForDistTSInsert,
) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructTSDelete construct ts delete method
func (f *stubFactory) ConstructTSDelete(
	nodeIDs []roachpb.NodeID,
	tblID uint64,
	spans []execinfrapb.Span,
	delTyp uint8,
	primaryTagKey, primaryTagValues [][]byte,
	isOutOfRange bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructTSTagUpdate construct ts update method
func (f *stubFactory) ConstructTSTagUpdate(
	nodeIDs []roachpb.NodeID,
	tblID, groupID uint64,
	primaryTagKey, TagValues [][]byte,
	pTagValueNotExist bool,
	startKey, endKey roachpb.Key,
) (exec.Node, error) {
	return struct{}{}, nil
}

// ConstructTsInsertSelect insert into time series table select time series data
func (f *stubFactory) ConstructTsInsertSelect(
	input exec.Node,
	tableID uint64,
	dbID uint64,
	cols opt.ColList,
	colIdxs opt.ColIdxs,
	tableName string,
	tableType int32,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMergeScan(input exec.Node, table cat.Table) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.ColumnOrdinalSet,
	returnCols exec.ColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	updateCols exec.ColumnOrdinalSet,
	returnCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.ColumnOrdinal,
	insertCols exec.ColumnOrdinalSet,
	fetchCols exec.ColumnOrdinalSet,
	updateCols exec.ColumnOrdinalSet,
	returnCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	returnCols exec.ColumnOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateTables(
	input exec.Node, schemas map[string]cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSequenceSelect(seq cat.Sequence) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSelectInto(input exec.Node, vars opt.VarNames) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructBuffer(value exec.Node, label string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScanBuffer(ref exec.Node, label string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateView(
	schema cat.Schema, columns sqlbase.ResultColumns, cv *memo.CreateViewExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) MakeTSSpans(e opt.Expr, n exec.Node, m *memo.Memo) (tight bool) { return false }

// for multiple model processing.
func (f *stubFactory) UpdatePlanColumns(input *exec.Node) bool { return false }

// for multiple model processing.
func (f *stubFactory) UpdateGroupInput(input *exec.Node) exec.Node {
	return struct{}{}
}

// for multiple model processing.
func (f *stubFactory) SetBljRightNode(blj, agg exec.Node) exec.Node {
	return struct{}{}
}

// for multiple model processing.
func (f *stubFactory) ProcessTsScanNode(
	node exec.Node,
	leftEq, rightEq *[]uint32,
	tsCols *[]sqlbase.TSCol,
	tableID opt.TableID,
	joinCols opt.ColList,
	mem *memo.Memo,
	evalCtx *tree.EvalContext,
) bool {
	return false
}

// for multiple model processing.
func (f *stubFactory) ProcessBljLeftColumns(
	node exec.Node, mem *memo.Memo,
) ([]sqlbase.TSCol, bool) {
	return nil, false
}

func (f *stubFactory) FindTsScanNode(node exec.Node, mem *memo.Memo) opt.TableID {
	return 0
}

func (f *stubFactory) FindTSTableID(expr memo.RelExpr, mem *memo.Memo) opt.TableID {
	return 0
}

func (f *stubFactory) MatchGroupingCols(
	mem *memo.Memo, tableGroupIndex int, node exec.Node,
) []opt.ColumnID {
	return nil
}

func (f *stubFactory) ProcessRenderNode(node exec.Node) error {
	return nil
}

func (f *stubFactory) AddAvgFuncColumns(node exec.Node, mem *memo.Memo, tableGroupIndex int) {

}

// for multiple model processing.
func (f *stubFactory) ResetTsScanAccessMode(
	expr memo.RelExpr, originalAccessMode execinfrapb.TSTableReadMode,
) {

}
