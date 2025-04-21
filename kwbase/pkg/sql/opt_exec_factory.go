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

package sql

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type execFactory struct {
	planner *planner
}

var _ exec.Factory = &execFactory{}

func makeExecFactory(p *planner) execFactory {
	return execFactory{planner: p}
}

// ConstructValues is part of the exec.Factory interface.
func (ef *execFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	if len(cols) == 0 && len(rows) == 1 {
		return &unaryNode{}, nil
	}
	if len(rows) == 0 {
		return &zeroNode{columns: cols}, nil
	}
	return &valuesNode{
		columns:          cols,
		tuples:           rows,
		specifiedInQuery: true,
	}, nil
}

// ConstructScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructScan(
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
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := makeScanColumnsConfig(table, needed)

	sb := span.MakeBuilder(tabDesc.TableDesc(), indexDesc)

	// initTable checks that the current user has the correct privilege to access
	// the table. However, the privilege has already been checked in optbuilder,
	// and does not need to be rechecked. In fact, it's an error to check the
	// privilege if the table was originally part of a view, since lower privilege
	// users might be able to access a view that uses a higher privilege table.
	ef.planner.skipSelectPrivilegeChecks = true
	defer func() { ef.planner.skipSelectPrivilegeChecks = false }()
	if err := scan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	if indexConstraint != nil && indexConstraint.IsContradiction() {
		return newZeroNode(scan.resultColumns), nil
	}

	scan.index = indexDesc
	scan.isSecondaryIndex = indexDesc != &tabDesc.PrimaryIndex
	scan.hardLimit = hardLimit
	scan.softLimit = softLimit

	scan.reverse = reverse
	scan.maxResults = maxResults
	scan.parallelScansEnabled = sqlbase.ParallelScans.Get(&ef.planner.extendedEvalCtx.Settings.SV)
	var err error
	scan.spans, err = sb.SpansFromConstraint(indexConstraint, needed, false /* forDelete */)
	if err != nil {
		return nil, err
	}
	for i := range reqOrdering {
		if reqOrdering[i].ColIdx >= len(colCfg.wantedColumns) {
			return nil, errors.Errorf("invalid reqOrdering: %v", reqOrdering)
		}
	}
	scan.reqOrdering = ReqOrdering(reqOrdering)
	scan.estimatedRowCount = uint64(rowCount)
	if locking != nil {
		scan.lockingStrength = sqlbase.ToScanLockingStrength(locking.Strength)
		scan.lockingWaitPolicy = sqlbase.ToScanLockingWaitPolicy(locking.WaitPolicy)
	}
	return scan, nil
}

// ConstructTSScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructTSScan(
	table cat.Table,
	private *memo.TSScanPrivate,
	tagFilter, primaryFilter, tagIndexFilter []tree.TypedExpr,
	rowCount float64,
) (exec.Node, error) {
	// Create a tsScanNode.
	tsScan := ef.planner.TSScan()

	ef.planner.skipSelectPrivilegeChecks = true
	defer func() { ef.planner.skipSelectPrivilegeChecks = false }()

	// Calculate the physical column ID and record it in the ScanSource.
	private.Cols.ForEach(func(i opt.ColumnID) {
		tsScan.ScanSource.Add(int(i) - int(private.Table.ColumnID(0)) + 1)
	})
	tsScan.Table = table
	resultCols := make(sqlbase.ResultColumns, 0)

	count := 0

	// Construct the resultCols which the column need to be scanned.
	tsScan.PrimaryTagValues = make(map[uint32][]string, 0)
	tsScan.TagIndex.TagIndexValues = make([]map[uint32][]string, len(private.TagIndex.TagIndexValues))
	tsScan.TagIndex.UnionType = private.TagIndex.UnionType
	tsScan.TagIndex.IndexID = private.TagIndex.IndexID
	for j := range private.TagIndex.TagIndexValues {
		tsScan.TagIndex.TagIndexValues[j] = make(map[uint32][]string, 0)
	}
	for i := 0; i < table.DeletableColumnCount(); i++ {
		colID := private.Table.ColumnID(count)
		col := table.Column(i)
		if v, ok := private.PrimaryTagValues[uint32(private.Table.ColumnID(i))]; ok {
			tsScan.PrimaryTagValues[uint32(table.Column(i).ColID())] = v
		}
		for j := range private.TagIndex.TagIndexValues {
			if v, ok := private.TagIndex.TagIndexValues[j][uint32(private.Table.ColumnID(i))]; ok {
				tsScan.TagIndex.TagIndexValues[j][uint32(table.Column(i).ColID())] = v
			}
		}
		if private.Cols.Contains(colID) {
			resultCols = append(
				resultCols,
				sqlbase.ResultColumn{
					Name:           string(col.ColName()),
					Typ:            col.DatumType(),
					TableID:        sqlbase.ID(table.ID()),
					PGAttributeNum: sqlbase.ColumnID(col.ColID()),
					TypeModifier:   col.DatumType().TypeModifier(),
				},
			)
		}
		count++
	}
	tsScan.filterVars = tree.MakeIndexedVarHelper(tsScan, len(resultCols))
	tsScan.resultColumns = resultCols
	tsScan.AccessMode = execinfrapb.TSTableReadMode(private.AccessMode)
	tsScan.HintType = private.HintType
	tsScan.orderedType = private.OrderedScanType
	tsScan.ScanAggArray = private.ScanAggs
	tsScan.TableMetaID = private.Table
	tsScan.estimatedRowCount = uint64(rowCount)

	// bind tag filter and primary filter to tsScanNode.
	bindFilter := func(filters []tree.TypedExpr, primaryTag bool, tagIndex bool) bool {
		for _, fil := range filters {
			expr := tsScan.filterVars.Rebind(fil, true /* alsoReset */, false /* normalizeToNonNil */)
			if expr == nil {
				// Filter statically evaluates to true. Just return the input plan.
				return false
			}
			if primaryTag {
				tsScan.PrimaryTagFilterArray = append(tsScan.PrimaryTagFilterArray, expr)
			} else if tagIndex {
				tsScan.TagIndexFilterArray = append(tsScan.TagIndexFilterArray, expr)
			} else {
				tsScan.TagFilterArray = append(tsScan.TagFilterArray, expr)
			}
		}
		return true
	}
	if !bindFilter(tagFilter, false, false) {
		return tsScan, nil
	}

	if !bindFilter(primaryFilter, true, false) {
		return tsScan, nil
	}

	if !bindFilter(tagIndexFilter, false, true) {
		return tsScan, nil
	}

	return tsScan, nil
}

// ConstructTsInsertSelect insert into time series table select time series data
func (ef *execFactory) ConstructTsInsertSelect(
	input exec.Node,
	tableID uint64,
	dbID uint64,
	cols opt.ColList,
	colIdxs opt.ColIdxs,
	tableName string,
	tableType int32,
) (exec.Node, error) {
	insSel := tsInsertSelectNodePool.Get().(*tsInsertSelectNode)
	insSel.plan = input.(planNode)
	insSel.TableID = tableID
	insSel.TableName = tableName
	insSel.DBID = dbID
	insSel.TableType = tableType
	resCols := make([]int32, len(cols))
	for i := range cols {
		resCols[i] = int32(cols[i])
	}
	resColIdxs := make([]int32, len(colIdxs))
	for i := range colIdxs {
		resColIdxs[i] = int32(colIdxs[i])
	}
	insSel.Cols = resCols
	insSel.ColIdxs = resColIdxs

	return insSel, nil
}

// ConstructSynchronizer construct synchronizer node
func (ef *execFactory) ConstructSynchronizer(input exec.Node, degree int64) (exec.Node, error) {
	plan := input.(planNode)
	resultCols := planColumns(plan)
	mergeScanNode := &synchronizerNode{
		columns: resultCols,
		plan:    plan,
		degree:  int32(degree),
	}
	return mergeScanNode, nil
}

// ConstructVirtualScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructVirtualScan(table cat.Table) (exec.Node, error) {
	tn := &table.(*optVirtualTable).name
	virtual, err := ef.planner.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return nil, err
	}
	columns, constructor := virtual.getPlanInfo(table.(*optVirtualTable).desc.TableDesc())

	return &delayedNode{
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			return constructor(ctx, p, tn.Catalog())
		},
	}, nil
}

func asDataSource(n exec.Node) planDataSource {
	plan := n.(planNode)
	return planDataSource{
		columns: planColumns(plan),
		plan:    plan,
	}
}

// ConstructFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering, execInTSEngine bool,
) (exec.Node, error) {
	// Push the filter into the scanNode. We cannot do this if the scanNode has a
	// limit (it would make the limit apply AFTER the filter).
	if s, ok := n.(*scanNode); ok && s.filter == nil && s.hardLimit == 0 {
		s.filter = s.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
		// Note: if the filter statically evaluates to true, s.filter stays nil.
		s.reqOrdering = ReqOrdering(reqOrdering)
		return s, nil
	}
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
		switch src := n.(type) {
		case *tsScanNode:
			src.filter = src.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
			return src, nil
		case *synchronizerNode:
			if s, ok := src.plan.(*tsScanNode); ok {
				s.filter = s.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
				return src, nil
			}
		case *sortNode:
			if s, ok := src.plan.(*synchronizerNode); ok {
				if s1, ok1 := s.plan.(*sortNode); ok1 {
					if s2, ok2 := s1.plan.(*tsScanNode); ok2 {
						s2.filter = s2.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
						return src, nil
					}
				}
			}
		}
	}

	// Create a filterNode.
	src := asDataSource(n)
	f := &filterNode{
		source: src,
		engine: engine,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.columns))
	f.filter = f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	if f.filter == nil {
		// Filter statically evaluates to true. Just return the input plan.
		return n, nil
	}
	f.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := f.source.plan.(*spoolNode); ok {
		f.source.plan = spool.source
		spool.source = f
		return spool, nil
	}
	return f, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSimpleProject(
	n exec.Node,
	cols []exec.ColumnOrdinal,
	colNames []string,
	reqOrdering exec.OutputOrdering,
	execInTSEngine bool,
) (exec.Node, error) {
	// If the top node is already a renderNode, just rearrange the columns. But
	// we don't want to duplicate a rendering expression (in case it is expensive
	// to compute or has side-effects); so if we have duplicates we avoid this
	// optimization (and add a new renderNode).
	if r, ok := n.(*renderNode); ok && !hasDuplicates(cols) {
		oldCols, oldRenders := r.columns, r.render
		r.columns = make(sqlbase.ResultColumns, len(cols))
		r.render = make([]tree.TypedExpr, len(cols))
		for i, ord := range cols {
			r.columns[i] = oldCols[ord]
			if colNames != nil {
				r.columns[i].Name = colNames[i]
			}
			r.render[i] = oldRenders[ord]
		}
		r.reqOrdering = ReqOrdering(reqOrdering)
		return r, nil
	}
	var inputCols sqlbase.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}

	var rb renderBuilder
	rb.init(n, reqOrdering, len(cols), execInTSEngine)
	for i, col := range cols {
		v := rb.r.ivarHelper.IndexedVar(int(col))
		if colNames == nil {
			rb.addExpr(v, inputCols[col].Name, inputCols[col].TableID, inputCols[col].PGAttributeNum, inputCols[col].GetTypeModifier())
		} else {
			rb.addExpr(v, colNames[i], 0 /* tableID */, 0 /* pgAttributeNum */, v.ResolvedType().TypeModifier())
		}
	}
	return rb.res, nil
}

func hasDuplicates(cols []exec.ColumnOrdinal) bool {
	var set util.FastIntSet
	for _, c := range cols {
		if set.Contains(int(c)) {
			return true
		}
		set.Add(int(c))
	}
	return false
}

// ConstructRender is part of the exec.Factory interface.
func (ef *execFactory) ConstructRender(
	n exec.Node,
	exprs tree.TypedExprs,
	colNames []string,
	reqOrdering exec.OutputOrdering,
	execInTSEngine bool,
) (exec.Node, error) {
	var rb renderBuilder
	rb.init(n, reqOrdering, len(exprs), execInTSEngine)
	for i, expr := range exprs {
		expr = rb.r.ivarHelper.Rebind(expr, false /* alsoReset */, true /* normalizeToNonNil */)
		rb.addExpr(expr, colNames[i], 0 /* tableID */, 0 /* pgAttributeNum */, -1 /* typeModifier */)
	}
	return rb.res, nil
}

// RenameColumns is part of the exec.Factory interface.
func (ef *execFactory) RenameColumns(n exec.Node, colNames []string) (exec.Node, error) {
	inputCols := planMutableColumns(n.(planNode))
	for i := range inputCols {
		inputCols[i].Name = colNames[i]
	}
	return n, nil
}

// ConstructBatchLookUpJoin is part of the exec.Factory interface.
// for multiple model processing.
func (ef *execFactory) ConstructBatchLookUpJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	// p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, err := makePredicate(joinType, leftSrc.columns, rightSrc.columns)
	if err != nil {
		return nil, err
	}

	numEqCols := len(leftEqCols)
	// Save some allocations by putting both sides in the same slice.
	intBuf := make([]int, 2*numEqCols)
	pred.leftEqualityIndices = intBuf[:numEqCols:numEqCols]
	pred.rightEqualityIndices = intBuf[numEqCols:]
	nameBuf := make(tree.NameList, 2*numEqCols)
	pred.leftColNames = nameBuf[:numEqCols:numEqCols]
	pred.rightColNames = nameBuf[numEqCols:]

	for i := range leftEqCols {
		pred.leftEqualityIndices[i] = int(leftEqCols[i])
		pred.rightEqualityIndices[i] = int(rightEqCols[i])
		pred.leftColNames[i] = tree.Name(leftSrc.columns[leftEqCols[i]].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.columns[rightEqCols[i]].Name)
	}
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	pred.onCond = pred.iVarHelper.Rebind(
		extraOnCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)
	// //lookup join
	// n := &lookupJoinNode{
	// 	input:        input.(planNode),
	// 	table:        tableScan,
	// 	joinType:     joinType,
	// 	eqColsAreKey: eqColsAreKey,
	// 	reqOrdering:  ReqOrdering(reqOrdering),
	// }
	// hash join
	n := &batchLookUpJoinNode{
		left:     leftSrc,
		right:    rightSrc,
		joinType: pred.joinType,
		pred:     pred,
		columns:  pred.cols,
	}

	return n, nil
}

// ConstructHashJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, err := makePredicate(joinType, leftSrc.columns, rightSrc.columns)
	if err != nil {
		return nil, err
	}

	numEqCols := len(leftEqCols)
	// Save some allocations by putting both sides in the same slice.
	intBuf := make([]int, 2*numEqCols)
	pred.leftEqualityIndices = intBuf[:numEqCols:numEqCols]
	pred.rightEqualityIndices = intBuf[numEqCols:]
	nameBuf := make(tree.NameList, 2*numEqCols)
	pred.leftColNames = nameBuf[:numEqCols:numEqCols]
	pred.rightColNames = nameBuf[numEqCols:]

	for i := range leftEqCols {
		pred.leftEqualityIndices[i] = int(leftEqCols[i])
		pred.rightEqualityIndices[i] = int(rightEqCols[i])
		pred.leftColNames[i] = tree.Name(leftSrc.columns[leftEqCols[i]].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.columns[rightEqCols[i]].Name)
	}
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	pred.onCond = pred.iVarHelper.Rebind(
		extraOnCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
}

// ConstructApplyJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	right memo.RelExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	leftSrc := asDataSource(left)
	pred, err := makePredicate(joinType, leftSrc.columns, rightColumns)
	if err != nil {
		return nil, err
	}
	pred.onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)
	apply, err := newApplyJoinNode(joinType, leftSrc, rightColumns, pred, right, planRightSideFn)
	if err != nil {
		return nil, err
	}
	// init TSWhiteListMap
	if a, ok := apply.(*applyJoinNode); ok {
		a.optimizer.Factory().TSWhiteListMap = ef.planner.ExecCfg().TSWhiteListMap
	}
	return apply, nil
}

// ConstructMergeJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, err := makePredicate(joinType, leftSrc.columns, rightSrc.columns)
	if err != nil {
		return nil, err
	}
	pred.onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	n := len(leftOrdering)
	if n == 0 || len(rightOrdering) != n {
		return nil, errors.Errorf("orderings from the left and right side must be the same non-zero length")
	}
	pred.leftEqualityIndices = make([]int, n)
	pred.rightEqualityIndices = make([]int, n)
	pred.leftColNames = make(tree.NameList, n)
	pred.rightColNames = make(tree.NameList, n)
	for i := 0; i < n; i++ {
		leftColIdx, rightColIdx := leftOrdering[i].ColIdx, rightOrdering[i].ColIdx
		pred.leftEqualityIndices[i] = leftColIdx
		pred.rightEqualityIndices[i] = rightColIdx
		pred.leftColNames[i] = tree.Name(leftSrc.columns[leftColIdx].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.columns[rightColIdx].Name)
	}

	node := p.makeJoinNode(leftSrc, rightSrc, pred)
	node.mergeJoinOrdering = make(sqlbase.ColumnOrdering, n)
	for i := 0; i < n; i++ {
		// The mergeJoinOrdering "columns" are equality column indices.  Because of
		// the way we constructed the equality indices, the ordering will always be
		// 0,1,2,3..
		node.mergeJoinOrdering[i].ColIdx = i
		node.mergeJoinOrdering[i].Direction = leftOrdering[i].Direction
	}

	// Set up node.props, which tells the distsql planner to maintain the
	// resulting ordering (if needed).
	node.reqOrdering = ReqOrdering(reqOrdering)

	return node, nil
}

// ConstructScalarGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructScalarGroupBy(
	input exec.Node,
	aggregations []exec.AggInfo,
	funcs *opt.AggFuncNames,
	execInTSEngine bool,
	private *memo.GroupingPrivate,
	addSynchronizer bool,
) (exec.Node, error) {
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
	}
	n := &groupNode{
		plan:               input.(planNode),
		funcs:              make([]*aggregateFuncHolder, 0, len(aggregations)),
		columns:            make(sqlbase.ResultColumns, 0, len(aggregations)),
		groupWindowID:      -1,
		groupWindowTSColID: -1,
		isScalar:           true,
		aggFuncs:           funcs,
		engine:             engine,
		addSynchronizer:    addSynchronizer,
		optType:            private.OptFlags,
	}
	if err := ef.addAggregations(n, aggregations); err != nil {
		return nil, err
	}
	return n, nil
}

// ConstructGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructGroupBy(
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
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
	}
	var groupWindowID int32 = -1
	if private.GroupWindowId > 0 {
		groupWindowID = int32(private.GroupWindowIdOrdinal)
	}
	n := &groupNode{
		plan:               input.(planNode),
		funcs:              make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:            make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations)+len(*funcs)),
		groupCols:          make([]int, len(groupCols)),
		groupColOrdering:   groupColOrdering,
		gapFillColID:       int32(private.TimeBucketGapFillColIdOrdinal),
		groupWindowID:      groupWindowID,
		groupWindowTSColID: int32(private.GroupWindowTSColOrdinal),
		isScalar:           false,
		reqOrdering:        ReqOrdering(reqOrdering),
		aggFuncs:           funcs,
		engine:             engine,
		//statisticIndex:   private.AggIndex,
		addSynchronizer: addSynchronizer,
		optType:         private.OptFlags,
	}
	inputCols := planColumns(n.plan)
	for i := range groupCols {
		col := int(groupCols[i])
		n.groupCols[i] = col

		// TODO(radu): only generate the grouping columns we actually need.
		f := n.newAggregateFuncHolder(
			builtins.AnyNotNull,
			inputCols[col].Typ,
			[]int{col},
			builtins.NewAnyNotNullAggregate,
			nil, /* arguments */
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, inputCols[col])
	}
	if err := ef.addAggregations(n, aggregations); err != nil {
		return nil, err
	}
	return n, nil
}

func (ef *execFactory) addAggregations(n *groupNode, aggregations []exec.AggInfo) error {
	inputCols := planColumns(n.plan)
	for i := range aggregations {
		agg := &aggregations[i]
		builtin := agg.Builtin
		renderIdxs := make([]int, len(agg.ArgCols))
		params := make([]*types.T, len(agg.ArgCols))
		for j, col := range agg.ArgCols {
			renderIdx := int(col)
			renderIdxs[j] = renderIdx
			params[j] = inputCols[renderIdx].Typ
		}
		aggFn := func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
			return builtin.AggregateFunc(params, evalCtx, arguments)
		}

		f := n.newAggregateFuncHolder(
			agg.FuncName,
			agg.ResultType,
			renderIdxs,
			aggFn,
			agg.ConstArgs,
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		if agg.Distinct {
			f.setDistinct()
		}

		if agg.Filter == -1 {
			// A value of -1 means the aggregate had no filter.
			f.filterRenderIdx = noRenderIdx
		} else {
			f.filterRenderIdx = int(agg.Filter)
		}

		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, sqlbase.ResultColumn{
			Name:         agg.FuncName,
			Typ:          agg.ResultType,
			TypeModifier: agg.ResultType.TypeModifier(),
		})
		// When it comes to interpolate, add a redundant output column for renderNode.ivarHelper.
		// reason: the subsequent plan will split the func interpolate.
		// interpolate(count(device_id), null) => interpolate($1,null), count(id)
		if agg.FuncName == "interpolate" {
			n.columns = append(n.columns, sqlbase.ResultColumn{})
		}
	}
	return nil
}

// ConstructDistinct is part of the exec.Factory interface.
func (ef *execFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.ColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
	execInTSEngine bool,
) (exec.Node, error) {
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
	}
	return &distinctNode{
		plan:              input.(planNode),
		distinctOnColIdxs: distinctCols,
		columnsInOrder:    orderedCols,
		reqOrdering:       ReqOrdering(reqOrdering),
		nullsAreDistinct:  nullsAreDistinct,
		errorOnDup:        errorOnDup,
		engine:            engine,
	}, nil
}

// ConstructSetOp is part of the exec.Factory interface.
func (ef *execFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return ef.planner.newUnionNode(typ, all, left.(planNode), right.(planNode))
}

// ConstructSort is part of the exec.Factory interface.
func (ef *execFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int, execInTSEngine bool,
) (exec.Node, error) {
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
	}
	return &sortNode{
		plan:                 input.(planNode),
		ordering:             ordering,
		alreadyOrderedPrefix: alreadyOrderedPrefix,
		engine:               engine,
	}, nil
}

// ConstructOrdinality is part of the exec.Factory interface.
func (ef *execFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	plan := input.(planNode)
	inputColumns := planColumns(plan)
	cols := make(sqlbase.ResultColumns, len(inputColumns)+1)
	copy(cols, inputColumns)
	cols[len(cols)-1] = sqlbase.ResultColumn{
		Name: colName,
		Typ:  types.Int,
	}
	return &ordinalityNode{
		source:  plan,
		columns: cols,
		run: ordinalityRun{
			row:    make(tree.Datums, len(cols)),
			curCnt: 1,
		},
	}, nil
}

// ConstructIndexJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.ColumnOrdinal,
	tableCols exec.ColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, tableCols)
	colDescs := makeColDescList(table, tableCols)

	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	primaryIndex := tabDesc.GetPrimaryIndex()
	tableScan.index = &primaryIndex
	tableScan.isSecondaryIndex = false
	tableScan.disableBatchLimit()

	n := &indexJoinNode{
		input:         input.(planNode),
		table:         tableScan,
		cols:          colDescs,
		resultColumns: sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), colDescs),
		reqOrdering:   ReqOrdering(reqOrdering),
	}

	n.keyCols = make([]int, len(keyCols))
	for i, c := range keyCols {
		n.keyCols[i] = int(c)
	}

	return n, nil
}

// ConstructLookupJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructLookupJoin(
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
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	tableScan.index = indexDesc
	tableScan.isSecondaryIndex = indexDesc != &tabDesc.PrimaryIndex

	n := &lookupJoinNode{
		input:        input.(planNode),
		table:        tableScan,
		joinType:     joinType,
		eqColsAreKey: eqColsAreKey,
		reqOrdering:  ReqOrdering(reqOrdering),
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	n.eqCols = make([]int, len(eqCols))
	for i, c := range eqCols {
		n.eqCols[i] = int(c)
	}
	// Build the result columns.
	inputCols := planColumns(input.(planNode))
	var scanCols sqlbase.ResultColumns
	if joinType != sqlbase.LeftSemiJoin && joinType != sqlbase.LeftAntiJoin {
		scanCols = planColumns(tableScan)
	}
	n.columns = make(sqlbase.ResultColumns, 0, len(inputCols)+len(scanCols))
	n.columns = append(n.columns, inputCols...)
	n.columns = append(n.columns, scanCols...)
	return n, nil
}

// Helper function to create a scanNode from just a table / index descriptor
// and requested cols.
func (ef *execFactory) constructScanForZigzag(
	indexDesc *sqlbase.IndexDescriptor,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	cols exec.ColumnOrdinalSet,
) (*scanNode, error) {

	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}

	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(tableDesc.Columns[c].ID))
	}

	scan := ef.planner.Scan()
	if err := scan.initTable(context.TODO(), ef.planner, tableDesc, nil, colCfg); err != nil {
		return nil, err
	}

	scan.index = indexDesc
	scan.isSecondaryIndex = indexDesc.ID != tableDesc.PrimaryIndex.ID

	return scan, nil
}

// ConstructZigzagJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructZigzagJoin(
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
	leftIndexDesc := leftIndex.(*optIndex).desc
	leftTabDesc := leftTable.(*optTable).desc
	rightIndexDesc := rightIndex.(*optIndex).desc
	rightTabDesc := rightTable.(*optTable).desc

	leftScan, err := ef.constructScanForZigzag(leftIndexDesc, leftTabDesc, leftCols)
	if err != nil {
		return nil, err
	}
	rightScan, err := ef.constructScanForZigzag(rightIndexDesc, rightTabDesc, rightCols)
	if err != nil {
		return nil, err
	}

	n := &zigzagJoinNode{
		reqOrdering: ReqOrdering(reqOrdering),
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	n.sides = make([]zigzagJoinSide, 2)
	n.sides[0].scan = leftScan
	n.sides[1].scan = rightScan
	n.sides[0].eqCols = make([]int, len(leftEqCols))
	n.sides[1].eqCols = make([]int, len(rightEqCols))

	if len(leftEqCols) != len(rightEqCols) {
		panic("creating zigzag join with unequal number of equated cols")
	}

	for i, c := range leftEqCols {
		n.sides[0].eqCols[i] = int(c)
		n.sides[1].eqCols[i] = int(rightEqCols[i])
	}
	// The resultant columns are identical to those from individual index scans; so
	// reuse the resultColumns generated in the scanNodes.
	n.columns = make(
		sqlbase.ResultColumns,
		0,
		len(leftScan.resultColumns)+len(rightScan.resultColumns),
	)
	n.columns = append(n.columns, leftScan.resultColumns...)
	n.columns = append(n.columns, rightScan.resultColumns...)

	// Fixed values are the values fixed for a prefix of each side's index columns.
	// See the comment in pkg/sql/rowexec/zigzagjoiner.go for how they are used.
	for i := range fixedVals {
		valNode, ok := fixedVals[i].(*valuesNode)
		if !ok {
			panic("non-values node passed as fixed value to zigzag join")
		}
		if i >= len(n.sides) {
			panic("more fixed values passed than zigzag join sides")
		}
		n.sides[i].fixedVals = valNode
	}
	return n, nil
}

// ConstructLimit is part of the exec.Factory interface.
func (ef *execFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr, limitExpr memo.RelExpr, meta *opt.Metadata,
) (exec.Node, error) {
	plan := input.(planNode)
	// If the input plan is also a limitNode that has just an offset, and we are
	// only applying a limit, update the existing node. This is useful because
	// Limit and Offset are separate operators which result in separate calls to
	// this function.
	if l, ok := plan.(*limitNode); ok && l.countExpr == nil && offset == nil {
		l.countExpr = limit
		return l, nil
	}
	// If the input plan is a spoolNode, then propagate any constant limit to it.
	if spool, ok := plan.(*spoolNode); ok {
		if val, ok := limit.(*tree.DInt); ok {
			spool.hardLimit = int64(*val)
		}
	}
	engine := tree.EngineTypeRelational
	if limitExpr.IsTSEngine() {
		engine = tree.EngineTypeTimeseries
	}

	node := &limitNode{
		plan:               plan,
		countExpr:          limit,
		offsetExpr:         offset,
		engine:             engine,
		pushLimitToAggScan: getLimitOpt(limitExpr),
	}
	if limit != nil {
		node.canOpt = tryOptLimitOrder(meta, limitExpr, limit, ef.planner.SessionData().MaxPushLimitNumber)
	} else {
		canOpt := (ef.planner.execCfg.StartMode == StartSingleNode) && tryOffsetOpt(meta, limitExpr, offset) &&
			checkFilterForOffsetOpt(input)
		if canOpt {
			// offset optimize must have limitNode and limit count less than MaxPushLimitNumber
			if v, ok := input.(*limitNode); ok {
				count, ok1 := v.countExpr.(*tree.DInt)
				offset, ok2 := offset.(*tree.DInt)
				if ok1 && ok2 && (int64(*count)-int64(*offset) < ef.planner.SessionData().MaxPushLimitNumber) {
					node.countExpr = v.countExpr
					node.plan = applyOffsetOptimize(v.plan, false).(planNode)
					node.canOpt = true
				}
			}
		}
	}
	return node, nil
}

// checkFilterForOffsetOpt  we can not use offset optimize that have normal col filter,
// normal col filter can exists tsScanNode filter or filterNode
func checkFilterForOffsetOpt(input exec.Node) bool {
	switch t := input.(type) {
	case *tsScanNode:
		return t.filter == nil
	case *synchronizerNode:
		return checkFilterForOffsetOpt(t.plan)
	case *sortNode:
		return checkFilterForOffsetOpt(t.plan)
	case *limitNode:
		return checkFilterForOffsetOpt(t.plan)
	case *renderNode:
		return checkFilterForOffsetOpt(t.source.plan)
	}

	return false
}

// applyOffsetOptimize apply offset optimize need deal
// 1 delete synchronizeNode, offset optimize can not support parrelle
// 2 delete limitNode, push limit count to tsscan node
// 2 add ordered type for tsScanNode according order by ts direction,
// reverse set OrderedScan, otherwise set ForceOrderedScan
func applyOffsetOptimize(src exec.Node, reverse bool) exec.Node {
	switch t := src.(type) {
	case *tsScanNode:
		if reverse {
			t.orderedType = opt.OrderedScan
		} else {
			t.orderedType = opt.ForceOrderedScan
		}
		return t
	case *sortNode:
		return applyOffsetOptimize(t.plan, t.ordering[0].Direction == encoding.Descending)
	case *synchronizerNode:
		return applyOffsetOptimize(t.plan, reverse).(planNode)
	case *renderNode:
		t.source.plan = applyOffsetOptimize(t.source.plan, reverse).(planNode)
		return t
	}

	return src
}

// ConstructMax1Row is part of the exec.Factory interface.
func (ef *execFactory) ConstructMax1Row(input exec.Node, errorText string) (exec.Node, error) {
	plan := input.(planNode)
	return &max1RowNode{
		plan:      plan,
		errorText: errorText,
	}, nil
}

// ConstructBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructBuffer(input exec.Node, label string) (exec.Node, error) {
	return &bufferNode{
		plan:  input.(planNode),
		label: label,
	}, nil
}

// ConstructScanBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructScanBuffer(ref exec.Node, label string) (exec.Node, error) {
	return &scanBufferNode{
		buffer: ref.(*bufferNode),
		label:  label,
	}, nil
}

// ConstructRecursiveCTE is part of the exec.Factory interface.
func (ef *execFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return &recursiveCTENode{
		initial:        initial.(planNode),
		genIterationFn: fn,
		label:          label,
	}, nil
}

// ConstructProjectSet is part of the exec.Factory interface.
func (ef *execFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	src := asDataSource(n)
	cols := append(src.columns, zipCols...)
	p := &projectSetNode{
		source:          src.plan,
		sourceCols:      src.columns,
		columns:         cols,
		numColsInSource: len(src.columns),
		exprs:           exprs,
		funcs:           make([]*tree.FuncExpr, len(exprs)),
		numColsPerGen:   numColsPerGen,
		run: projectSetRun{
			gens:      make([]tree.ValueGenerator, len(exprs)),
			done:      make([]bool, len(exprs)),
			rowBuffer: make(tree.Datums, len(cols)),
		},
	}

	for i, expr := range exprs {
		if tFunc, ok := expr.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Set-generating functions: generate_series() etc.
			p.funcs[i] = tFunc
		}
	}

	return p, nil
}

// ConstructWindow is part of the exec.Factory interface.
func (ef *execFactory) ConstructWindow(
	root exec.Node, wi exec.WindowInfo, execInTSEngine bool,
) (exec.Node, error) {
	p := &windowNode{
		plan:         root.(planNode),
		columns:      wi.Cols,
		windowRender: make([]tree.TypedExpr, len(wi.Cols)),
	}
	if execInTSEngine {
		p.engine = tree.EngineTypeTimeseries
	}

	partitionIdxs := make([]int, len(wi.Partition))
	for i, idx := range wi.Partition {
		partitionIdxs[i] = int(idx)
	}

	p.funcs = make([]*windowFuncHolder, len(wi.Exprs))
	for i := range wi.Exprs {
		argsIdxs := make([]uint32, len(wi.ArgIdxs[i]))
		for j := range argsIdxs {
			argsIdxs[j] = uint32(wi.ArgIdxs[i][j])
		}

		p.funcs[i] = &windowFuncHolder{
			expr:           wi.Exprs[i],
			args:           wi.Exprs[i].Exprs,
			argsIdxs:       argsIdxs,
			window:         p,
			filterColIdx:   wi.FilterIdxs[i],
			outputColIdx:   wi.OutputIdxs[i],
			partitionIdxs:  partitionIdxs,
			columnOrdering: wi.Ordering,
			frame:          wi.Exprs[i].WindowDef.Frame,
		}
		if len(wi.Ordering) == 0 {
			frame := p.funcs[i].frame
			if frame.Mode == tree.RANGE && frame.Bounds.HasOffset() {
				// Execution requires a single column to order by when there is
				// a RANGE mode frame with at least one 'offset' bound.
				return nil, errors.AssertionFailedf("a RANGE mode frame with an offset bound must have an ORDER BY column")
			}
		}

		p.windowRender[wi.OutputIdxs[i]] = p.funcs[i]
	}

	return p, nil
}

// ConstructPlan is part of the exec.Factory interface.
func (ef *execFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, postqueries []exec.Node,
) (exec.Plan, error) {
	// No need to spool at the root.
	if spool, ok := root.(*spoolNode); ok {
		root = spool.source
	}
	res := &planTop{
		plan: root.(planNode),
		// TODO(radu): these fields can be modified by planning various opaque
		// statements. We should have a cleaner way of plumbing these.
		avoidBuffering:  ef.planner.curPlan.avoidBuffering,
		instrumentation: ef.planner.curPlan.instrumentation,
	}
	if len(subqueries) > 0 {
		res.subqueryPlans = make([]subquery, len(subqueries))
		for i := range subqueries {
			in := &subqueries[i]
			out := &res.subqueryPlans[i]
			out.subquery = in.ExprNode
			switch in.Mode {
			case exec.SubqueryExists:
				out.execMode = rowexec.SubqueryExecModeExists
			case exec.SubqueryOneRow:
				out.execMode = rowexec.SubqueryExecModeOneRow
			case exec.SubqueryAnyRows:
				out.execMode = rowexec.SubqueryExecModeAllRowsNormalized
			case exec.SubqueryAllRows:
				out.execMode = rowexec.SubqueryExecModeAllRows
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			out.plan = in.Root.(planNode)
		}
	}
	if len(postqueries) > 0 {
		res.postqueryPlans = make([]postquery, len(postqueries))
		for i := range res.postqueryPlans {
			res.postqueryPlans[i].plan = postqueries[i].(planNode)
		}
	}

	return res, nil
}

// urlOutputter handles writing strings into an encoded URL for EXPLAIN (OPT,
// ENV). It also ensures that (in the text that is encoded by the URL) each
// entry gets its own line and there's exactly one blank line between entries.
type urlOutputter struct {
	buf bytes.Buffer
}

func (e *urlOutputter) writef(format string, args ...interface{}) {
	if e.buf.Len() > 0 {
		e.buf.WriteString("\n")
	}
	fmt.Fprintf(&e.buf, format, args...)
}

func (e *urlOutputter) finish() (url.URL, error) {
	// Generate a URL that encodes all the text.
	var compressed bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := e.buf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	return url.URL{
		Scheme:   "https",
		Host:     "",
		Path:     "text/decode.html",
		Fragment: compressed.String(),
	}, nil
}

// showEnv implements EXPLAIN (opt, env). It returns a node which displays
// the environment a query was run in.
func (ef *execFactory) showEnv(plan string, envOpts exec.ExplainEnvData) (exec.Node, error) {
	var out urlOutputter

	c := makeStmtEnvCollector(
		ef.planner.EvalContext().Context,
		ef.planner.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
	)

	// Show the version of Cockroach running.
	if err := c.PrintVersion(&out.buf); err != nil {
		return nil, err
	}
	out.writef("")
	// Show the values of any non-default session variables that can impact
	// planning decisions.
	if err := c.PrintSettings(&out.buf); err != nil {
		return nil, err
	}

	// Show the definition of each referenced catalog object.
	for i := range envOpts.Sequences {
		out.writef("")
		if err := c.PrintCreateSequence(&out.buf, &envOpts.Sequences[i]); err != nil {
			return nil, err
		}
	}

	// TODO(justin): it might also be relevant in some cases to print the create
	// statements for tables referenced via FKs in these tables.
	for i := range envOpts.Tables {
		out.writef("")
		if err := c.PrintCreateTable(&out.buf, &envOpts.Tables[i]); err != nil {
			return nil, err
		}
		out.writef("")

		// In addition to the schema, it's important to know what the table
		// statistics on each table are.

		// NOTE: We don't include the histograms because they take up a ton of
		// vertical space. Unfortunately this means that in some cases we won't be
		// able to reproduce a particular plan.
		err := c.PrintTableStats(&out.buf, &envOpts.Tables[i], true /* hideHistograms */)
		if err != nil {
			return nil, err
		}
	}

	for i := range envOpts.Views {
		out.writef("")
		if err := c.PrintCreateView(&out.buf, &envOpts.Views[i]); err != nil {
			return nil, err
		}
	}

	// Show the query running. Note that this is the *entire* query, including
	// the "EXPLAIN (opt, env)" preamble.
	out.writef("%s;\n----\n%s", ef.planner.stmt.AST.String(), plan)

	url, err := out.finish()
	if err != nil {
		return nil, err
	}
	return &valuesNode{
		columns:          sqlbase.ExplainOptColumns,
		tuples:           [][]tree.TypedExpr{{tree.NewDString(url.Fragment)}},
		specifiedInQuery: true,
	}, nil
}

// ConstructExplainOpt is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplainOpt(
	planText string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	// If this was an EXPLAIN (opt, env), we need to run a bunch of auxiliary
	// queries to fetch the environment info.
	if envOpts.ShowEnv {
		return ef.showEnv(planText, envOpts)
	}

	var rows [][]tree.TypedExpr
	ss := strings.Split(strings.Trim(planText, "\n"), "\n")
	for _, line := range ss {
		rows = append(rows, []tree.TypedExpr{tree.NewDString(line)})
	}

	return &valuesNode{
		columns:          sqlbase.ExplainOptColumns,
		tuples:           rows,
		specifiedInQuery: true,
	}, nil
}

// ConstructExplain is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan, mem *memo.Memo,
) (exec.Node, error) {
	p := plan.(*planTop)

	analyzeSet := options.Flags[tree.ExplainFlagAnalyze]

	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	switch options.Mode {
	case tree.ExplainDistSQL:
		return &explainDistSQLNode{
			options:        options,
			plan:           p.plan,
			subqueryPlans:  p.subqueryPlans,
			postqueryPlans: p.postqueryPlans,
			analyze:        analyzeSet,
			stmtType:       stmtType,
		}, nil

	case tree.ExplainVec:
		return &explainVecNode{
			options:       options,
			plan:          p.plan,
			subqueryPlans: p.subqueryPlans,
			stmtType:      stmtType,
		}, nil

	case tree.ExplainPlan:
		if analyzeSet {
			return nil, errors.New("EXPLAIN ANALYZE only supported with (DISTSQL) option")
		}
		return ef.planner.makeExplainPlanNodeWithPlan(
			context.TODO(),
			options,
			p.plan,
			p.subqueryPlans,
			p.postqueryPlans,
			stmtType,
			mem,
		)

	default:
		panic(fmt.Sprintf("unsupported explain mode %v", options.Mode))
	}
}

// ConstructShowTrace is part of the exec.Factory interface.
func (ef *execFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	var node planNode = ef.planner.makeShowTraceNode(compact, typ == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := sqlbase.GetTraceAgeColumnIdx(compact)
	node = &sortNode{
		plan: node,
		ordering: sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
	}

	if typ == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{plan: node}
	}
	return node, nil
}

func (ef *execFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	colDescs := makeColDescList(table, insertColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	var fkTables row.FkTableMetadata
	checkFKs := row.SkipFKs
	if !skipFKChecks {
		checkFKs = row.CheckFKs
		// Determine the foreign key tables involved in the update.
		var err error
		fkTables, err = ef.makeFkMetadata(tabDesc, row.CheckInserts)
		if err != nil {
			return nil, err
		}
	}
	// Create the table inserter, which does the bulk of the work.
	ri, err := row.MakeInserter(
		ctx, ef.planner.txn, tabDesc, colDescs, checkFKs, fkTables, &ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Regular path for INSERT.
	ins := insertNodePool.Get().(*insertNode)
	*ins = insertNode{
		source: input.(planNode),
		run: insertRun{
			ti:         tableInserter{ri: ri},
			checkOrds:  checkOrdSet,
			insertCols: ri.InsertCols,
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ins.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ins.run.rowsNeeded = true
	}

	if allowAutoCommit && ef.planner.autoCommit {
		ins.enableAutoCommit()
	}

	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: ins}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ins}, nil
}

func (ef *execFactory) ConstructTSInsert(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
) (exec.Node, error) {
	tsIns := tsInsertNodePool.Get().(*tsInsertNode)
	for _, payloadVals := range payloadNodeMap {
		tsIns.nodeIDs = append(tsIns.nodeIDs, payloadVals.NodeID)
		tsIns.allNodePayloadInfos = append(tsIns.allNodePayloadInfos, payloadVals.PerNodePayloads)
	}
	return tsIns, nil
}

func (ef *execFactory) ConstructTSDelete(
	nodeIDs []roachpb.NodeID,
	tblID uint64,
	spans []execinfrapb.Span,
	delTyp uint8,
	primaryTagKey, primaryTagValues [][]byte,
	isOutOfRange bool,
) (exec.Node, error) {
	tsDel := tsDeleteNodePool.Get().(*tsDeleteNode)
	tsDel.nodeIDs = nodeIDs
	tsDel.tableID = tblID
	tsDel.delTyp = delTyp
	tsDel.primaryTagKey = primaryTagKey
	tsDel.primaryTagValue = primaryTagValues
	tsDel.spans = spans
	tsDel.wrongPTag = isOutOfRange

	return tsDel, nil
}

func (ef *execFactory) ConstructTSTagUpdate(
	nodeIDs []roachpb.NodeID,
	tblID, groupID uint64,
	primaryTagKey, TagValues [][]byte,
	pTagValueNotExist bool,
	startKey, endKey roachpb.Key,
) (exec.Node, error) {
	tsTagUpdate := tsTagUpdateNodePool.Get().(*tsTagUpdateNode)
	tsTagUpdate.nodeIDs = nodeIDs
	tsTagUpdate.tableID = tblID
	tsTagUpdate.groupID = groupID
	tsTagUpdate.primaryTagKey = primaryTagKey
	tsTagUpdate.TagValue = TagValues
	tsTagUpdate.wrongPTag = pTagValueNotExist
	tsTagUpdate.startKey = startKey
	tsTagUpdate.endKey = endKey

	return tsTagUpdate, nil
}

func (ef *execFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	colDescs := makeColDescList(table, insertColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Create the table inserter, which does the bulk of the work.
	ri, err := row.MakeInserter(
		ctx, ef.planner.txn, tabDesc, colDescs, row.SkipFKs, nil /* fkTables */, &ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Regular path for INSERT.
	ins := insertFastPathNodePool.Get().(*insertFastPathNode)
	*ins = insertFastPathNode{
		input: rows,
		run: insertFastPathRun{
			insertRun: insertRun{
				ti:         tableInserter{ri: ri},
				checkOrds:  checkOrdSet,
				insertCols: ri.InsertCols,
			},
		},
	}

	if len(fkChecks) > 0 {
		ins.run.fkChecks = make([]insertFastPathFKCheck, len(fkChecks))
		for i := range fkChecks {
			ins.run.fkChecks[i].InsertFastPathFKCheck = fkChecks[i]
		}
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ins.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ins.run.rowsNeeded = true
	}

	if len(rows) == 0 {
		return &zeroNode{columns: ins.columns}, nil
	}

	if ef.planner.autoCommit {
		ins.enableAutoCommit()
	}

	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: ins}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ins}, nil
}

func (ef *execFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchColOrdSet exec.ColumnOrdinalSet,
	updateColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Add each column to update as a sourceSlot. The CBO only uses scalarSlot,
	// since it compiles tuples and subqueries into a simple sequence of target
	// columns.
	updateColDescs := makeColDescList(table, updateColOrdSet)
	sourceSlots := make([]sourceSlot, len(updateColDescs))
	for i := range sourceSlots {
		sourceSlots[i] = scalarSlot{column: updateColDescs[i], sourceIndex: len(fetchColDescs) + i}
	}

	var fkTables row.FkTableMetadata
	checkFKs := row.SkipFKs
	if !skipFKChecks {
		checkFKs = row.CheckFKs
		// Determine the foreign key tables involved in the update.
		var err error
		fkTables, err = ef.makeFkMetadata(tabDesc, row.CheckUpdates)
		if err != nil {
			return nil, err
		}
	}

	// Create the table updater, which does the bulk of the work.
	ru, err := row.MakeUpdater(
		ctx,
		ef.planner.txn,
		tabDesc,
		fkTables,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		checkFKs,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx[id] = i
	}

	upd := updateNodePool.Get().(*updateNode)
	*upd = updateNode{
		source: input.(planNode),
		run: updateRun{
			tu:        tableUpdater{ru: ru},
			checkOrds: checks,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				CurSourceRow: make(tree.Datums, len(ru.FetchCols)),
				Cols:         ru.FetchCols,
				Mapping:      ru.FetchColIDtoRowIndex,
			},
			sourceSlots:    sourceSlots,
			updateValues:   make(tree.Datums, len(ru.UpdateCols)),
			updateColsIdx:  updateColsIdx,
			numPassthrough: len(passthrough),
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)

		upd.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)
		// Add the passthrough columns to the returning columns.
		upd.columns = append(upd.columns, passthrough...)

		// Set the rowIdxToRetIdx for the mutation. Update returns the non-mutation
		// columns specified, in the same order they are defined in the table.
		//
		// The Updater derives/stores the fetch columns of the mutation and
		// since the return columns are always a subset of the fetch columns,
		// we can use use the fetch columns to generate the mapping for the
		// returned rows.
		upd.run.rowIdxToRetIdx = row.ColMapping(ru.FetchCols, returnColDescs)
		upd.run.rowsNeeded = true
	}

	if allowAutoCommit && ef.planner.autoCommit {
		upd.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: upd}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: upd}, nil
}

func (ef *execFactory) makeFkMetadata(
	tabDesc *sqlbase.ImmutableTableDescriptor, fkCheckType row.FKCheckType,
) (row.FkTableMetadata, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Create a CheckHelper, used in case of cascading actions that cause changes
	// in the original table. This is only possible with UPDATE (together with
	// cascade loops or self-references).
	var checkHelper *sqlbase.CheckHelper
	if fkCheckType == row.CheckUpdates {
		var err error
		checkHelper, err = sqlbase.NewEvalCheckHelper(ctx, ef.planner.analyzeExpr, tabDesc)
		if err != nil {
			return nil, err
		}
	}
	// Determine the foreign key tables involved in the upsert.
	return row.MakeFkMetadata(
		ef.planner.extendedEvalCtx.Context,
		tabDesc,
		fkCheckType,
		ef.planner.LookupTableByID,
		ef.planner.CheckPrivilege,
		ef.planner.analyzeExpr,
		checkHelper,
	)
}

func (ef *execFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.ColumnOrdinal,
	insertColOrdSet exec.ColumnOrdinalSet,
	fetchColOrdSet exec.ColumnOrdinalSet,
	updateColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	insertColDescs := makeColDescList(table, insertColOrdSet)
	fetchColDescs := makeColDescList(table, fetchColOrdSet)
	updateColDescs := makeColDescList(table, updateColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	var fkTables row.FkTableMetadata
	checkFKs := row.SkipFKs
	if !skipFKChecks {
		checkFKs = row.CheckFKs

		// Determine the foreign key tables involved in the upsert.
		var err error
		fkTables, err = ef.makeFkMetadata(tabDesc, row.CheckUpdates)
		if err != nil {
			return nil, err
		}
	}

	// Create the table inserter, which does the bulk of the insert-related work.
	ri, err := row.MakeInserter(
		ctx, ef.planner.txn, tabDesc, insertColDescs, checkFKs, fkTables, &ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Create the table updater, which does the bulk of the update-related work.
	ru, err := row.MakeUpdater(
		ctx,
		ef.planner.txn,
		tabDesc,
		fkTables,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		checkFKs,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx[id] = i
	}

	// Instantiate the upsert node.
	ups := upsertNodePool.Get().(*upsertNode)
	*ups = upsertNode{
		source: input.(planNode),
		run: upsertRun{
			checkOrds:  checks,
			insertCols: ri.InsertCols,
			tw: optTableUpserter{
				ri:            ri,
				canaryOrdinal: int(canaryCol),
				fkTables:      fkTables,
				fetchCols:     fetchColDescs,
				updateCols:    updateColDescs,
				ru:            ru,
			},
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ups.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Update the tabColIdxToRetIdx for the mutation. Upsert returns
		// non-mutation columns specified, in the same order they are defined
		// in the table.
		ups.run.tw.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ups.run.tw.returnCols = returnColDescs
		ups.run.tw.collectRows = true
	}

	if allowAutoCommit && ef.planner.autoCommit {
		ups.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: ups}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ups}, nil
}

func (ef *execFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Determine the foreign key tables involved in the delete.
	// This will include all the interleaved child tables as we need them
	// to see if we can execute the fast path delete.
	fkTables, err := ef.makeFkMetadata(tabDesc, row.CheckDeletes)
	if err != nil {
		return nil, err
	}

	fastPathInterleaved := canDeleteFastInterleaved(tabDesc, fkTables)
	if fastPathNode, ok := maybeCreateDeleteFastNode(
		ctx, input.(planNode), tabDesc, fkTables, fastPathInterleaved, rowsNeeded); ok {
		return fastPathNode, nil
	}

	checkFKs := row.CheckFKs
	if skipFKChecks {
		checkFKs = row.SkipFKs
	}
	// Create the table deleter, which does the bulk of the work. In the HP,
	// the deleter derives the columns that need to be fetched. By contrast, the
	// CBO will have already determined the set of fetch columns, and passes
	// those sets into the deleter (which will basically be a no-op).
	rd, err := row.MakeDeleter(
		ctx,
		ef.planner.txn,
		tabDesc,
		fkTables,
		fetchColDescs,
		checkFKs,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Now make a delete node. We use a pool.
	del := deleteNodePool.Get().(*deleteNode)
	*del = deleteNode{
		source: input.(planNode),
		run: deleteRun{
			td: tableDeleter{rd: rd, alloc: &ef.planner.alloc},
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		// Delete returns the non-mutation columns specified, in the same
		// order they are defined in the table.
		del.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		del.run.rowIdxToRetIdx = row.ColMapping(rd.FetchCols, returnColDescs)
		del.run.rowsNeeded = true
	}

	if allowAutoCommit && ef.planner.autoCommit {
		del.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: del}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: del}, nil
}

func (ef *execFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := &tabDesc.PrimaryIndex
	sb := span.MakeBuilder(tabDesc.TableDesc(), indexDesc)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Setting the "forDelete" flag includes all column families in case where a
	// single record is deleted.
	spans, err := sb.SpansFromConstraint(indexConstraint, needed, true /* forDelete */)
	if err != nil {
		return nil, err
	}

	// Permitting autocommit in DeleteRange is very important, because DeleteRange
	// is used for simple deletes from primary indexes like
	// DELETE FROM t WHERE key = 1000
	// When possible, we need to make this a 1pc transaction for performance
	// reasons. At the same time, we have to be careful, because DeleteRange
	// returns all of the keys that it deleted - so we have to set a limit on the
	// DeleteRange request. But, trying to set autocommit and a limit on the
	// request doesn't work properly if the limit is hit. So, we permit autocommit
	// here if we can guarantee that the number of returned keys is finite and
	// relatively small.
	autoCommitEnabled := allowAutoCommit && ef.planner.autoCommit
	// If maxReturnedKeys is 0, it indicates that we weren't able to determine
	// the maximum number of returned keys, so we'll give up and not permit
	// autocommit.
	if maxReturnedKeys == 0 || maxReturnedKeys > TableTruncateChunkSize {
		autoCommitEnabled = false
	}

	return &deleteRangeNode{
		interleavedFastPath: false,
		spans:               spans,
		desc:                tabDesc,
		autoCommitEnabled:   autoCommitEnabled,
	}, nil
}

// ConstructCreateTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	nd := &createTableNode{n: ct, dbDesc: schema.(*optSchema).database}
	if input != nil {
		nd.sourcePlan = input.(planNode)
	}
	return nd, nil
}

func (ef *execFactory) ConstructCreateTables(
	input exec.Node, schemas map[string]cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	nd := createMultiInstTableNode{}
	nd.dbDescs = make(map[string]*sqlbase.DatabaseDescriptor)
	for _, ctbl := range ct.Instances {
		if sqlbase.ContainsNonAlphaNumSymbol(ctbl.Name.String()) {
			return &nd, sqlbase.NewTSNameInvalidError(ctbl.Name.String())
		}
		nd.ns = append(nd.ns, &tree.CreateTable{
			Table:       ctbl.Name,
			OnCommit:    ct.OnCommit,
			TableType:   tree.InstanceTable,
			UsingSource: ctbl.UsingSource,
			Tags:        ctbl.Tags,
		})
	}
	for k, v := range schemas {
		nd.dbDescs[k] = v.(*optSchema).database
	}
	if input != nil {
		nd.sourcePlan = input.(planNode)
	}
	return &nd, nil
}

// ConstructCreateView is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateView(
	schema cat.Schema, columns sqlbase.ResultColumns, cv *memo.CreateViewExpr,
) (exec.Node, error) {
	deps := cv.Deps
	planDeps := make(planDependencies, len(deps))
	for _, d := range deps {
		desc, err := getDescForDataSource(d.DataSource)
		if err != nil {
			return nil, err
		}
		// Replicated table do not support view
		if desc.TableDescriptor.IsReplTable {
			return nil, errors.Errorf("Replicated table %s do not support view", desc.TableDescriptor.Name)
		}
		var ref sqlbase.TableDescriptor_Reference
		if d.SpecificIndex {
			idx := d.DataSource.(cat.Table).Index(d.Index)
			ref.IndexID = idx.(*optIndex).desc.ID
		}
		if !d.ColumnOrdinals.Empty() {
			ref.ColumnIDs = make([]sqlbase.ColumnID, 0, d.ColumnOrdinals.Len())
			d.ColumnOrdinals.ForEach(func(ord int) {
				ref.ColumnIDs = append(ref.ColumnIDs, desc.Columns[ord].ID)
			})
		}
		entry := planDeps[desc.ID]
		entry.desc = desc
		entry.deps = append(entry.deps, ref)
		planDeps[desc.ID] = entry
	}

	db := schema.Name().CatalogName
	sc := schema.Name().SchemaName
	viewTblName := tree.MakeTableNameWithSchema(db, sc, tree.Name(cv.ViewName))
	return &createViewNode{
		viewName:     &viewTblName,
		ifNotExists:  cv.IfNotExists,
		temporary:    cv.Temporary,
		materialized: cv.Materialized,
		viewQuery:    cv.ViewQuery,
		dbDesc:       schema.(*optSchema).database,
		columns:      columns,
		planDeps:     planDeps,
	}, nil
}

// ConstructSequenceSelect is part of the exec.Factory interface.
func (ef *execFactory) ConstructSequenceSelect(sequence cat.Sequence) (exec.Node, error) {
	return ef.planner.SequenceSelectNode(sequence.(*optSequence).desc)
}

// ConstructSaveTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return ef.planner.makeSaveTable(input.(planNode), table, colNames), nil
}

// ConstructErrorIfRows is part of the exec.Factory interface.
func (ef *execFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return &errorIfRowsNode{
		plan:  input.(planNode),
		mkErr: mkErr,
	}, nil
}

// ConstructOpaque is part of the exec.Factory interface.
func (ef *execFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	o, ok := metadata.(*opaqueMetadata)
	if !ok {
		return nil, errors.AssertionFailedf("unexpected OpaqueMetadata object type %T", metadata)
	}
	return o.plan, nil
}

// ConstructAlterTableSplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	expirationTime, err := parseExpirationTime(ef.planner.EvalContext(), expiration)
	if err != nil {
		return nil, err
	}

	return &splitNode{
		tableDesc:      &index.Table().(*optTable).desc.TableDescriptor,
		index:          index.(*optIndex).desc,
		rows:           input.(planNode),
		expirationTime: expirationTime,
	}, nil
}

// ConstructSelectInto is part of the exec.Factory interface.
func (ef *execFactory) ConstructSelectInto(input exec.Node, vars opt.VarNames) (exec.Node, error) {

	return &selectIntoNode{
		rows: input.(planNode),
		vars: vars,
	}, nil
}

// ConstructAlterTableUnsplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return &unsplitNode{
		tableDesc: &index.Table().(*optTable).desc.TableDescriptor,
		index:     index.(*optIndex).desc,
		rows:      input.(planNode),
	}, nil
}

// ConstructAlterTableUnsplitAll is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return &unsplitAllNode{
		tableDesc: &index.Table().(*optTable).desc.TableDescriptor,
		index:     index.(*optIndex).desc,
	}, nil
}

// ConstructAlterTableRelocate is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return &relocateNode{
		relocateLease: relocateLease,
		tableDesc:     &index.Table().(*optTable).desc.TableDescriptor,
		index:         index.(*optIndex).desc,
		rows:          input.(planNode),
	}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (ef *execFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return &controlJobsNode{
		rows:          input.(planNode),
		desiredStatus: jobCommandToDesiredStatus[command],
	}, nil
}

// ConstructCancelQueries is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelQueriesNode{
		rows:     input.(planNode),
		ifExists: ifExists,
	}, nil
}

// ConstructCancelSessions is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelSessionsNode{
		rows:     input.(planNode),
		ifExists: ifExists,
	}, nil
}

// renderBuilder encapsulates the code to build a renderNode.
type renderBuilder struct {
	r   *renderNode
	res planNode
}

// init initializes the renderNode with render expressions.
func (rb *renderBuilder) init(
	n exec.Node, reqOrdering exec.OutputOrdering, cap int, execInTSEngine bool,
) {
	src := asDataSource(n)
	engine := tree.EngineTypeRelational
	if execInTSEngine {
		engine = tree.EngineTypeTimeseries
	}
	rb.r = &renderNode{
		source:  src,
		render:  make([]tree.TypedExpr, 0, cap),
		columns: make([]sqlbase.ResultColumn, 0, cap),
		engine:  engine,
	}
	rb.r.ivarHelper = tree.MakeIndexedVarHelper(rb.r, len(src.columns))
	rb.r.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := rb.r.source.plan.(*spoolNode); ok {
		rb.r.source.plan = spool.source
		spool.source = rb.r
		rb.res = spool
	} else {
		rb.res = rb.r
	}
}

// addExpr adds a new render expression with the given name.
func (rb *renderBuilder) addExpr(
	expr tree.TypedExpr,
	colName string,
	tableID sqlbase.ID,
	pgAttributeNum sqlbase.ColumnID,
	typeModifier int32,
) {
	rb.r.render = append(rb.r.render, expr)
	rb.r.columns = append(
		rb.r.columns,
		sqlbase.ResultColumn{
			Name:           colName,
			Typ:            expr.ResolvedType(),
			TableID:        tableID,
			PGAttributeNum: pgAttributeNum,
			TypeModifier:   typeModifier,
		},
	)
}

// makeColDescList returns a list of table column descriptors. Columns are
// included if their ordinal position in the table schema is in the cols set.
func makeColDescList(table cat.Table, cols exec.ColumnOrdinalSet) []sqlbase.ColumnDescriptor {
	colDescs := make([]sqlbase.ColumnDescriptor, 0, cols.Len())
	for i, n := 0, table.DeletableColumnCount(); i < n; i++ {
		if !cols.Contains(i) {
			continue
		}
		colDescs = append(colDescs, *table.Column(i).(*sqlbase.ColumnDescriptor))
	}
	return colDescs
}

// makeScanColumnsConfig builds a scanColumnsConfig struct by constructing a
// list of descriptor IDs for columns in the given cols set. Columns are
// identified by their ordinal position in the table schema.
func makeScanColumnsConfig(table cat.Table, cols exec.ColumnOrdinalSet) scanColumnsConfig {
	// Set visibility=publicAndNonPublicColumns, since all columns in the "cols"
	// set should be projected, regardless of whether they're public or non-
	// public. The caller decides which columns to include (or not include). Note
	// that when wantedColumns is non-empty, the visibility flag will never
	// trigger the addition of more columns.
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
		visibility:    publicAndNonPublicColumns,
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		desc := table.Column(c).(*sqlbase.ColumnDescriptor)
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(desc.ID))
	}
	return colCfg
}

// MakeTSSpans make TSSpans and assign it to tsScanNode.
func (ef *execFactory) MakeTSSpans(e opt.Expr, n exec.Node, m *memo.Memo) (tight bool) {
	out := new(constraint.Constraint)
	if tn, ok := n.(*tsScanNode); ok {
		tabID := m.Metadata().GetTableIDByObjectID(tn.Table.ID())
		tight := ef.MakeTSSpansForExpr(e, out, tabID)
		typ := tn.Table.Column(0).DatumType()
		var precision int32
		if !typ.InternalType.TimePrecisionIsSet && typ.InternalType.Precision == 0 {
			precision = 3
		} else {
			precision = typ.Precision()
		}
		tn.tsSpans = out.TransformSpansToTsSpans(precision)
		tn.tsSpansPre = precision
		return tight
	}
	return false
}

// MakeTSSpansForExpr make TSSpans from expr.
// TODO: ruanzhijin, the function name is not good: it sounds like making sth, but it returns a bool flag?
// pls also handle the below functions like makeTSSpansForAnd, etc.
func (ef *execFactory) MakeTSSpansForExpr(
	e opt.Expr, out *constraint.Constraint, tblID opt.TableID,
) (tight bool) {
	switch t := e.(type) {
	case *memo.FiltersExpr:
		switch len(*t) {
		case 0:
			unconstrained(out)
			return true
		case 1:
			return ef.MakeTSSpansForExpr((*t)[0].Condition, out, tblID)
		default:
			return ef.makeTSSpansForAnd(t, out, tblID)
		}

	case *memo.FiltersItem:
		// Pass through the call.
		return ef.MakeTSSpansForExpr(t.Condition, out, tblID)

	case *memo.AndExpr:
		return ef.makeTSSpansForAnd(t, out, tblID)

	case *memo.OrExpr:
		return ef.makeTSSpansForOr(t, out, tblID)

	case *memo.RangeExpr:
		return ef.MakeTSSpansForExpr(t.And, out, tblID)
	}

	if e.ChildCount() < 2 {
		unconstrained(out)
		return false
	}
	child0, child1 := e.Child(0), e.Child(1)

	// Check for an operation where the left-hand side is a
	// timestamp column.
	if isTimestampColumn(child0, int(tblID.ColumnID(0))) {
		tight := ef.makeSpansForSingleColumn(e.Op(), child1, out)
		if !out.IsUnconstrained() || tight {
			return tight
		}
	}

	unconstrained(out)
	return false
}

// unconstrained make empty TSSPan.
func unconstrained(out *constraint.Constraint) {
	out.Spans.InitSingleSpan(&constraint.UnconstrainedSpan)
}

// makeSpansForAnd calculates spans for an AndOp or FiltersOp.
func (ef *execFactory) makeTSSpansForAnd(
	e opt.Expr, out *constraint.Constraint, tblID opt.TableID,
) (tight bool) {
	tight = ef.MakeTSSpansForExpr(e.Child(0), out, tblID)
	var expr *memo.FiltersExpr
	if fe, ok := e.(*memo.FiltersExpr); ok {
		expr = fe
	}
	if tight && expr != nil {
		*expr = (*expr)[1:]
	}

	var exprConstraint constraint.Constraint
	for i, n := 0, e.ChildCount(); i < n; i++ {
		childTight := ef.MakeTSSpansForExpr(e.Child(i), &exprConstraint, tblID)
		tight = tight && childTight
		out.IntersectWith(ef.planner.EvalContext(), &exprConstraint)
		if childTight && expr != nil {
			*expr = append((*expr)[:i], (*expr)[i+1:]...)
			i--
			n = e.ChildCount()
		}
	}
	if out.IsUnconstrained() {
		return tight
	}
	if tight {
		return true
	}

	return false
}

// makeSpansForOr calculates spans for an OrOp.
func (ef *execFactory) makeTSSpansForOr(
	e opt.Expr, out *constraint.Constraint, tblID opt.TableID,
) (tight bool) {
	out.Spans = constraint.Spans{}
	tight = true
	var exprConstraint constraint.Constraint
	for i, n := 0, e.ChildCount(); i < n; i++ {
		exprTight := ef.MakeTSSpansForExpr(e.Child(i), &exprConstraint, tblID)
		if exprConstraint.IsUnconstrained() {
			// If we can't generate spans for a disjunct, exit early.
			unconstrained(out)
			// ZDP-14846：Ensure that all timetamp in filter are converted to int,Send to ae
			//return false
		}
		// The OR is "tight" if all the spans are tight.
		tight = tight && exprTight
		out.UnionWith(ef.planner.EvalContext(), &exprConstraint)
	}
	return tight
}

// isTimestampColumn returns true if colid in expr and id are equal.
func isTimestampColumn(nd opt.Expr, id int) bool {
	if v, ok := nd.(*memo.VariableExpr); ok && v.Col == opt.ColumnID(id) {
		return true
	}
	return false
}

// makeSpansForSingleColumn creates spans for a single index column from a
// simple comparison expression. The arguments are the operator and right
// operand. The <tight> return value indicates if the spans are exactly
// equivalent to the expression (and not weaker).
func (ef *execFactory) makeSpansForSingleColumn(
	op opt.Operator, val opt.Expr, out *constraint.Constraint,
) (tight bool) {
	if op == opt.InOp && memo.CanExtractConstTuple(val) {
		tupVal := val.(*memo.TupleExpr)
		//keyCtx := &c.keyCtx[offset]
		var spans constraint.Spans
		spans.Alloc(len(tupVal.Elems))
		for _, child := range tupVal.Elems {
			datum := memo.ExtractConstDatum(child)
			if !(datum.ResolvedType() == types.Timestamp) {
				unconstrained(out)
				return false
			}
			if datum == tree.DNull {
				// Ignore NULLs - they can't match any values
				continue
			}
			var sp constraint.Span
			sp.Init(
				constraint.MakeKey(datum), constraint.IncludeBoundary,
				constraint.MakeKey(datum), constraint.IncludeBoundary,
			)
			spans.Append(&sp)
		}
		out.Spans = spans
		return true
	}
	scalar, ok := val.(opt.ScalarExpr)
	if opt.IsConstValueOp(val) || (ok && scalar.CheckConstDeductionEnabled()) {
		ivh := tree.MakeIndexedVarHelper(nil /* container */, 0)
		tExpr, err := execbuilder.BuildScalarByExpr(val.(opt.ScalarExpr), &ivh, ef.planner.EvalContext())
		if datum, ok := tExpr.(tree.Datum); ok && err == nil {
			return ef.makeSpansForSingleColumnDatum(op, datum, out)
		}
	}

	unconstrained(out)
	return false
}

// makeSpansForSingleColumnDatum creates spans for a single index column from a
// simple comparison expression with a constant value on the right-hand side.
func (ef *execFactory) makeSpansForSingleColumnDatum(
	op opt.Operator, datum tree.Datum, out *constraint.Constraint,
) (tight bool) {
	if !(datum.ResolvedType().Oid() == oid.T_timestamptz &&
		datum.ResolvedType().Precision() == 9) {
		unconstrained(out)
		return false
	}

	if datum == tree.DNull {
		switch op {
		case opt.EqOp, opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp, opt.NeOp:
			// The result of this expression is always NULL. Normally, this expression
			// should have been converted to NULL during type checking; but if the
			// NULL is coming from a placeholder, that doesn't happen.

			//contradiction(offset, out)
			return true

		case opt.IsOp:
			return true

		case opt.IsNotOp:
			return true
		}
		return false
	}

	switch op {
	case opt.EqOp, opt.IsOp:
		var s constraint.Span
		key := constraint.MakeKey(datum)
		s.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
		out.Spans.InitSingleSpan(&s)
		return true

	case opt.LtOp, opt.GtOp, opt.LeOp, opt.GeOp:
		startKey, startBoundary := constraint.EmptyKey, constraint.IncludeBoundary
		endKey, endBoundary := constraint.EmptyKey, constraint.IncludeBoundary
		k := constraint.MakeKey(datum)
		switch op {
		case opt.LtOp:
			endKey, endBoundary = k, constraint.ExcludeBoundary
		case opt.LeOp:
			endKey, endBoundary = k, constraint.IncludeBoundary
		case opt.GtOp:
			startKey, startBoundary = k, constraint.ExcludeBoundary
		case opt.GeOp:
			startKey, startBoundary = k, constraint.IncludeBoundary
		}

		ef.singleSpan(startKey, startBoundary, endKey, endBoundary, out)
		return true

	case opt.NeOp, opt.IsNotOp:
		// Build constraint that doesn't contain the key:
		//   if nullable or IsNotOp   : [ - key) (key - ]
		//   if not nullable and NeOp : (/NULL - key) (key - ]
		//
		// If the key is the minimum possible value for the column type, the span
		// (/NULL - key) will never contain any values and can be omitted. The span
		// [ - key) is similar if the column is not nullable.
		//
		// Similarly, if the key is the maximum possible value, the span (key - ]
		// can be omitted.

		startKey, startBoundary := constraint.EmptyKey, constraint.IncludeBoundary

		key := constraint.MakeKey(datum)

		if !(startKey.IsEmpty()) && datum.IsMin(ef.planner.EvalContext()) {
			// Omit the (/NULL - key) span by setting a contradiction, so that the
			// UnionWith call below will result in just the second span.
			out.Spans = constraint.Spans{}
		} else {
			ef.singleSpan(startKey, startBoundary, key, constraint.ExcludeBoundary, out)
		}
		if !datum.IsMax(ef.planner.EvalContext()) {
			var other constraint.Constraint
			ef.singleSpan(key, constraint.ExcludeBoundary, constraint.EmptyKey, constraint.IncludeBoundary, &other)
			out.UnionWith(ef.planner.EvalContext(), &other)
		}
		return true

	}
	unconstrained(out)
	return false
}

// singleSpan creates a constraint with a single span.
func (ef *execFactory) singleSpan(
	start constraint.Key,
	startBoundary constraint.SpanBoundary,
	end constraint.Key,
	endBoundary constraint.SpanBoundary,
	out *constraint.Constraint,
) {
	var s constraint.Span
	keyCtx := constraint.KeyContext{Columns: constraint.Columns{}, EvalCtx: ef.planner.EvalContext()}
	keyCtx.Columns.InitFirst()

	s.Init(start, startBoundary, end, endBoundary)
	//s.PreferInclusive(&keyCtx)

	out.Spans.InitSingleSpan(&s)
}

// get LimitOpt of LimitExpr
func getLimitOpt(e memo.RelExpr) bool {
	if l, ok := e.(*memo.LimitExpr); ok {
		return l.LimitOptFlag.TSPushLimitToAggScan()
	}
	return false
}

// tryOptLimitOrder check if limit and sort can optimize.
// meta is metadata.
// e is memo.LimitExpr
func tryOptLimitOrder(
	meta *opt.Metadata, e memo.RelExpr, limit tree.TypedExpr, pushLimitNumber int64,
) bool {
	if val, ok := limit.(*tree.DInt); ok && int64(*val) > pushLimitNumber {
		return false
	}
	if e.IsTSEngine() {
		if l, ok := e.(*memo.LimitExpr); ok {
			if sort, ok := l.Input.(*memo.SortExpr); ok {
				return walkSort(meta, sort)
			}
			return false
		}
	}
	return false
}

// tryOffsetOpt check if limit offset and sort can optimize.
// meta is metadata.
// e is memo.LimitExpr
// offset is offset value
func tryOffsetOpt(meta *opt.Metadata, e memo.RelExpr, offset tree.TypedExpr) bool {
	if _, ok := offset.(*tree.DInt); !ok {
		return false
	}
	if e.IsTSEngine() {
		if l, ok := e.(*memo.OffsetExpr); ok {
			// need order by ts col
			if limit, ok := l.Input.(*memo.LimitExpr); ok {
				if sort, ok := limit.Input.(*memo.SortExpr); ok {
					return walkSort(meta, sort)
				}
			}

			return false
		}
	}
	return false
}

// walkSort check if sort only order by timestamp column,
// and child are only ProjectExpr, SelectExpr, TSScanExpr.
// meta is metadata.
// e is memo.SortExpr
func walkSort(meta *opt.Metadata, expr memo.RelExpr) bool {
	switch t := expr.(type) {
	case *memo.SortExpr:
		orderTimestampCol := true
		for _, v := range t.ProvidedPhysical().Ordering {
			if v.Descending() {
				v = -v
			}
			col := meta.ColumnMeta(opt.ColumnID(v))
			if col.Table.ColumnID(0) != col.MetaID {
				orderTimestampCol = false
				break
			}
		}
		if orderTimestampCol {
			return walkSort(meta, t.Input)
		}
		return false
	case *memo.ProjectExpr:
		return walkSort(meta, t.Input)
	case *memo.SelectExpr:
		return walkSort(meta, t.Input)
	case *memo.TSScanExpr:
		return true
	default:
		return false
	}
}

// UpdatePlanColumns modifies the resultColumns  of tsScanNode, synchronizerNode,
// and filterNode if they are part of the plan tree under the batch lookup join.
//
// The function ensures that the right-side plan of the batchLookUpJoinNode correctly
// inherits the column structure from the join. If a tsScanNode is found, its resultColumns
// are updated directly. If a synchronizerNode wraps a tsScanNode, both the synchronizerNode
// and tsScanNode are updated.
//
// Special handling is applied to filterNode due to potential column index mismatches:
// - filterNode may have a different column ordering than batchLookUpJoinNode.
// - A column mapping is constructed to remap indexed variables in its comparison expression
// to the correct column indices in the new schema.
// - This ensures that filtering conditions remain valid after the column structure is updated.
func (ef *execFactory) UpdatePlanColumns(blj *exec.Node) bool {
	success := false
	updateColumns := func(tsNode *tsScanNode, columns sqlbase.ResultColumns) {
		tsNode.resultColumns = columns
		success = true
	}

	switch node := (*blj).(type) {
	case *batchLookUpJoinNode:
		if tsNode, ok := node.right.plan.(*tsScanNode); ok {
			updateColumns(tsNode, node.columns)
		}

		if syncNode, ok := node.right.plan.(*synchronizerNode); ok {
			if tsNode, ok := syncNode.plan.(*tsScanNode); ok {
				syncNode.columns = node.columns
				updateColumns(tsNode, node.columns)
			}
		}
		if filterNode, ok := node.right.plan.(*filterNode); ok {
			columnMapping := make(map[int]int)
			for i, srcCol := range filterNode.source.columns {
				for j, nodeCol := range node.columns {
					if srcCol.Name == nodeCol.Name {
						columnMapping[i] = j
						break
					}
				}
			}
			if comparisonExpr, ok := filterNode.filter.(*tree.ComparisonExpr); ok {
				if leftVar, ok := comparisonExpr.Left.(*tree.IndexedVar); ok {
					if newIdx, found := columnMapping[leftVar.Idx]; found {
						leftVar.Idx = newIdx
					}
				}
				if rightVar, ok := comparisonExpr.Right.(*tree.IndexedVar); ok {
					if newIdx, found := columnMapping[rightVar.Idx]; found {
						rightVar.Idx = newIdx
					}
				}
			}
			filterNode.source.columns = node.columns
			if tsNode, ok := filterNode.source.plan.(*tsScanNode); ok {
				updateColumns(tsNode, node.columns)
			}
			if syncNode, ok := filterNode.source.plan.(*synchronizerNode); ok {
				if tsNode, ok := syncNode.plan.(*tsScanNode); ok {
					syncNode.columns = node.columns
					updateColumns(tsNode, node.columns)
				}
			}
		}
		node.right.columns = node.columns
	}
	return success
}

// UpdateGroupInput updates the input to build groupNode for multiple model processing.
func (ef *execFactory) UpdateGroupInput(input *exec.Node) exec.Node {
	switch node := (*input).(type) {
	case *batchLookUpJoinNode:
		*input = node.right
		return node

	case *renderNode:
		if bljNode, ok := node.source.plan.(*batchLookUpJoinNode); ok {
			node.source.plan = bljNode.right.plan
			*input = node
			node.engine = tree.EngineTypeTimeseries
			return bljNode
		} else if sortNode, ok := node.source.plan.(*sortNode); ok {
			if bljNode, ok := sortNode.plan.(*batchLookUpJoinNode); ok {
				sortNode.engine = tree.EngineTypeTimeseries
				sortNode.plan = bljNode.right.plan
				node.engine = tree.EngineTypeTimeseries
				*input = node
				return bljNode
			}
		}
	case *sortNode:
		if renderNode, ok := node.plan.(*renderNode); ok {
			if bljNode, ok := renderNode.source.plan.(*batchLookUpJoinNode); ok {
				renderNode.engine = tree.EngineTypeTimeseries
				renderNode.source.plan = bljNode.right.plan
				node.engine = tree.EngineTypeTimeseries
				*input = node
				return bljNode
			}
		}
	}
	return nil
}

// deleteSynchronizer delete the added synchronizer to avoid conflicts.
func deleteSynchronizer(node *planNode) {
	switch n := (*node).(type) {
	case *synchronizerNode:
		*node = n.plan
	case *renderNode:
		deleteSynchronizer(&n.source.plan)
	case *sortNode:
		deleteSynchronizer(&n.plan)
	}
}

// parallelAgg parallel processing of agg executed in AE,
// and delete the added synchronizer to avoid conflicts.
func parallelAgg(agg *groupNode) {
	isDistinct := false
	for i := 0; i < len(agg.funcs); i++ {
		f := agg.funcs[i]
		if f.run.seen != nil {
			isDistinct = true
		}
	}
	if !isDistinct {
		deleteSynchronizer(&agg.plan)
		agg.addSynchronizer = true
	}
}

// SetBljRightNode sets the right child of a batchLookUpJoinNode to a groupNode
// for multiple model processing.
func (ef *execFactory) SetBljRightNode(node1, node2 exec.Node) exec.Node {
	if blj, ok := node1.(*batchLookUpJoinNode); ok {
		if agg, ok := node2.(*groupNode); ok {
			parallelAgg(agg)

			agg.engine = tree.EngineTypeTimeseries
			blj.right.plan = agg
			blj.columns = agg.columns
			return blj
		}
	}
	return nil
}

// ProcessTsScanNode sets relationalInfo to a tsScanNode for multiple model processing.
func (ef *execFactory) ProcessTsScanNode(
	node exec.Node,
	leftEq, rightEq *[]uint32,
	tsCols *[]sqlbase.TSCol,
	tableID opt.TableID,
	joinCols opt.ColList,
	mem *memo.Memo,
	evalCtx *tree.EvalContext,
) bool {
	// Helper function to process tsScanNode
	processTsNode := func(tsNode *tsScanNode) {
		tsNode.RelInfo.RelationalCols = tsCols
		tsNode.RelInfo.ProbeColIds = leftEq
		tsNode.RelInfo.HashColIds = rightEq
		resetAccessMode(tsNode, tableID, joinCols, mem, evalCtx)
	}

	switch n := node.(type) {
	case *tsScanNode:
		processTsNode(n)
		return true
	case *synchronizerNode:
		if tsNode, ok := n.plan.(*tsScanNode); ok {
			processTsNode(tsNode)
			return true
		}
	case *filterNode:
		switch srcPlan := n.source.plan.(type) {
		case *tsScanNode:
			processTsNode(srcPlan)
			return true
		case *synchronizerNode:
			if tsNode, ok := srcPlan.plan.(*tsScanNode); ok {
				processTsNode(tsNode)
				return true
			}
		}
	}
	return false
}

// resetAccessMode determines and sets the access mode for a tsScanNode
//
// - If a specific access mode is enforced via session settings, it is applied directly.
// - Otherwise, it determines the access mode based on primary tag coverage and whether HashTagScan is enabled:
//   - If all primary tags are present in the join columns, it uses `primaryHashTagScan`.
//   - If HashTagScan is enabled in MultimodelHelper, it uses `hashTagScan`.
//   - Otherwise, it defaults to `hashRelScan`.
func resetAccessMode(
	tsNode *tsScanNode,
	tableID opt.TableID,
	joinCols opt.ColList,
	mem *memo.Memo,
	evalCtx *tree.EvalContext,
) {
	md := mem.Metadata()
	var primaryTagCount int
	if value, ok := mem.MultimodelHelper.TableData.Load(tableID); ok {
		tableInfo := value.(memo.TableInfo)
		primaryTagCount = tableInfo.PrimaryTagCount
	}
	ptagSet := make(map[int]struct{})
	for _, joinCol := range joinCols {
		if md.ColumnMeta(opt.ColumnID(joinCol)).IsPrimaryTag() {
			ptagSet[int(joinCol)] = struct{}{}
		}
	}

	ptagCount := len(ptagSet)
	accessModeEnforced := evalCtx.SessionData.HashScanMode
	if accessModeEnforced > 0 && accessModeEnforced < 4 {
		switch accessModeEnforced {
		case 1:
			tsNode.AccessMode = execinfrapb.TSTableReadMode_hashTagScan
		case 2:
			tsNode.AccessMode = execinfrapb.TSTableReadMode_primaryHashTagScan
		case 3:
			tsNode.AccessMode = execinfrapb.TSTableReadMode_hashRelScan
		}
	} else if ptagCount == primaryTagCount {
		tsNode.AccessMode = execinfrapb.TSTableReadMode_primaryHashTagScan
	} else if mem.MultimodelHelper.HashTagScan {
		tsNode.AccessMode = execinfrapb.TSTableReadMode_hashTagScan
	} else {
		tsNode.AccessMode = execinfrapb.TSTableReadMode_hashRelScan
	}
}

// ResetTsScanAccessMode resets tsScanNode's accessMode when falling back
// to original plan for multiple model processing.
func (ef *execFactory) ResetTsScanAccessMode(
	expr memo.RelExpr, originalAccessMode execinfrapb.TSTableReadMode,
) {
	setAccessMode := func(e memo.RelExpr) {
		if tsScanExpr, ok := e.(*memo.TSScanExpr); ok {
			tsScanExpr.TSScanPrivate.AccessMode = int(originalAccessMode)
		}
	}

	var recursiveSetAccessMode func(memo.RelExpr)
	recursiveSetAccessMode = func(e memo.RelExpr) {
		if e == nil {
			return
		}
		switch expr := e.(type) {
		case *memo.TSScanExpr:
			setAccessMode(expr)
		case *memo.ProjectExpr:
			recursiveSetAccessMode(expr.Input)
		case *memo.SelectExpr:
			recursiveSetAccessMode(expr.Input)
		}
	}

	recursiveSetAccessMode(expr)
}

// ProcessBljLeftColumns helps build the tsScanNode's relaitonalInfo
// for multiple model processing.
func (ef *execFactory) ProcessBljLeftColumns(
	node exec.Node, mem *memo.Memo,
) ([]sqlbase.TSCol, bool) {
	tsCols := make([]sqlbase.TSCol, 0)
	fb := false
	var columns sqlbase.ResultColumns
	switch n := node.(type) {
	case *batchLookUpJoinNode:
		columns = n.columns
	case *exportNode:
		columns = n.columns
	case *groupNode:
		columns = n.columns
	case *indexJoinNode:
		columns = n.resultColumns
	case *joinNode:
		columns = n.columns
	case *lookupJoinNode:
		columns = n.columns
	case *ordinalityNode:
		columns = n.columns
	case *projectSetNode:
		columns = n.columns
	case *renderNode:
		columns = n.columns
	case *scanNode:
		columns = n.resultColumns
	case *tsScanNode:
		columns = n.resultColumns
	case *synchronizerNode:
		columns = n.columns
	case *unionNode:
		columns = n.columns
	case *valuesNode:
		columns = n.columns
	case *windowNode:
		columns = n.columns
	case *zeroNode:
		columns = n.columns
	case *zigzagJoinNode:
		columns = n.columns
	case *limitNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *applyJoinNode:
		return ef.ProcessBljLeftColumns(n.input.plan, mem)
	case *bufferNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *delayedNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *distinctNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *explainDistSQLNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *explainPlanNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *explainVecNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *filterNode:
		return ef.ProcessBljLeftColumns(n.source.plan, mem)
	case *max1RowNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *relocateNode:
		return ef.ProcessBljLeftColumns(n.rows, mem)
	case *sortNode:
		return ef.ProcessBljLeftColumns(n.plan, mem)
	case *splitNode:
		return ef.ProcessBljLeftColumns(n.rows, mem)
	case *spoolNode:
		return ef.ProcessBljLeftColumns(n.source, mem)
	case *unsplitNode:
		return ef.ProcessBljLeftColumns(n.rows, mem)
	case *updateNode:
		return ef.ProcessBljLeftColumns(n.source, mem)
	}

	varPos := 0
	for _, col := range columns {
		colType := col.Typ
		if !memo.CheckDataType(colType) {
			if colType.Name() == "text" || colType.Name() == "string" {
			} else {
				fb = true
			}
		}
		storageType := sqlbase.GetTSDataType(colType)
		if colType.Name() == "string" ||
			colType.Name() == "text" {
			storageType = sqlbase.DataType_VARCHAR
		}
		if storageType == sqlbase.DataType_UNKNOWN {
			storageType = sqlbase.DataType_VARCHAR
			fb = true
		}

		var storageLen uint64
		stLen := sqlbase.GetStorageLenForFixedLenTypes(storageType)
		if stLen != 0 {
			storageLen = uint64(stLen)
		}

		typeWidth := colType.Width()
		if storageType == sqlbase.DataType_NCHAR || storageType == sqlbase.DataType_NVARCHAR {
			typeWidth = colType.Width() * 4
		}

		if typeWidth == 0 {
			switch storageType {
			case sqlbase.DataType_CHAR:
				storageLen = 1
			case sqlbase.DataType_BYTES:
				storageLen = 3
			case sqlbase.DataType_NCHAR:
				storageLen = 4
			case sqlbase.DataType_VARCHAR, sqlbase.DataType_NVARCHAR, sqlbase.DataType_VARBYTES:
				storageLen = sqlbase.TSMaxVariableTupleLen
			}
		} else {
			switch storageType {
			case sqlbase.DataType_CHAR, sqlbase.DataType_NCHAR:
				if typeWidth < sqlbase.TSMaxFixedLen {
					storageLen = uint64(typeWidth) + 1
				} else {
					fb = true
				}
			case sqlbase.DataType_BYTES:
				if typeWidth < sqlbase.TSMaxFixedLen {
					storageLen = uint64(typeWidth) + 2
				} else {
					fb = true
				}
			case sqlbase.DataType_VARCHAR, sqlbase.DataType_NVARCHAR:
				if typeWidth <= sqlbase.TSMaxVariableLen {
					storageLen = uint64(typeWidth + 1)
				} else {
					fb = true
				}
			case sqlbase.DataType_VARBYTES:
				if typeWidth <= sqlbase.TSMaxVariableLen {
					storageLen = uint64(typeWidth)
				} else {
					fb = true
				}
			}
		}
		colNullable := true
		if col.TableID != 0 {
			colNullable = mem.Metadata().TableByStableID(cat.StableID(col.TableID)).Column(int(col.PGAttributeNum - 1)).IsNullable()
		} else {
			if n, ok := node.(*renderNode); ok {
				if e, ok := (n.render[varPos]).(tree.TypedExpr); ok {
					if v, ok := (e).(*tree.IndexedVar); ok {
						rcol := n.source.columns[v.Idx]
						if rcol.TableID != 0 {
							colNullable = mem.Metadata().TableByStableID(cat.StableID(rcol.TableID)).Column(int(rcol.PGAttributeNum - 1)).IsNullable()
						}
					} else if vs, ok := (e).(*tree.CastExpr); ok {
						if v, ok := vs.Expr.(*tree.IndexedVar); ok {
							rcol := n.source.columns[v.Idx]
							if rcol.TableID != 0 {
								colNullable = mem.Metadata().TableByStableID(cat.StableID(rcol.TableID)).Column(int(rcol.PGAttributeNum - 1)).IsNullable()
							}
						}
					} else if vs, ok := (e).(*tree.BinaryExpr); ok {
						if v, ok := vs.Left.(*tree.IndexedVar); ok {
							rcol := n.source.columns[v.Idx]
							if rcol.TableID != 0 {
								colNullable = mem.Metadata().TableByStableID(cat.StableID(rcol.TableID)).Column(int(rcol.PGAttributeNum - 1)).IsNullable()
							}
						} else if v, ok := vs.Right.(*tree.IndexedVar); ok {
							rcol := n.source.columns[v.Idx]
							if rcol.TableID != 0 {
								colNullable = mem.Metadata().TableByStableID(cat.StableID(rcol.TableID)).Column(int(rcol.PGAttributeNum - 1)).IsNullable()
							}
						}
					}
				}
			}
		}
		tsCols = append(tsCols, sqlbase.TSCol{
			StorageLen:  storageLen,
			StorageType: storageType,
			Nullable:    colNullable,
		})
		varPos++
	}
	return tsCols, fb
}

// FindTsScanNode traverses the execution plan tree to locate the occurrence of tsScanNode.
func (ef *execFactory) FindTsScanNode(node exec.Node, mem *memo.Memo) opt.TableID {
	switch n := node.(type) {
	case *batchLookUpJoinNode:
		if tableID := ef.FindTsScanNode(n.left.plan, mem); tableID != 0 {
			return tableID
		}
		if tableID := ef.FindTsScanNode(n.right.plan, mem); tableID != 0 {
			return tableID
		}
	case *exportNode:
		if tableID := ef.FindTsScanNode(n.source, mem); tableID != 0 {
			return tableID
		}
	case *filterNode:
		if tableID := ef.FindTsScanNode(n.source.plan, mem); tableID != 0 {
			return tableID
		}
	case *groupNode:
		if tableID := ef.FindTsScanNode(n.plan, mem); tableID != 0 {
			return tableID
		}
	case *indexJoinNode:
		if tableID := ef.FindTsScanNode(n.input, mem); tableID != 0 {
			return tableID
		}
	case *joinNode:
		if tableID := ef.FindTsScanNode(n.left.plan, mem); tableID != 0 {
			return tableID
		}
		if tableID := ef.FindTsScanNode(n.right.plan, mem); tableID != 0 {
			return tableID
		}
	case *lookupJoinNode:
		if tableID := ef.FindTsScanNode(n.input, mem); tableID != 0 {
			return tableID
		}
	case *ordinalityNode:
		if tableID := ef.FindTsScanNode(n.source, mem); tableID != 0 {
			return tableID
		}
	case *projectSetNode:
		if tableID := ef.FindTsScanNode(n.source, mem); tableID != 0 {
			return tableID
		}
	case *renderNode:
		if tableID := ef.FindTsScanNode(n.source.plan, mem); tableID != 0 {
			return tableID
		}
	case *scanNode:
	case *delayedNode:
		if tableID := ef.FindTsScanNode(n.plan, mem); tableID != 0 {
			return tableID
		}
	case *sortNode:
		if tableID := ef.FindTsScanNode(n.plan, mem); tableID != 0 {
			return tableID
		}
	case *synchronizerNode:
		if tableID := ef.FindTsScanNode(n.plan, mem); tableID != 0 {
			return tableID
		}
	case *tsScanNode:
		return n.TableMetaID
	case *unionNode:
		if tableID := ef.FindTsScanNode(n.left, mem); tableID != 0 {
			return tableID
		}
		if tableID := ef.FindTsScanNode(n.right, mem); tableID != 0 {
			return tableID
		}
	case *valuesNode:
	case *windowNode:
		if tableID := ef.FindTsScanNode(n.plan, mem); tableID != 0 {
			return tableID
		}
	case *zeroNode:
	case *zigzagJoinNode:
	}
	return 0
}

// FindTSTableID traverses a relational expression tree to find the table id of the TSScanExpr.
func (ef *execFactory) FindTSTableID(expr memo.RelExpr, mem *memo.Memo) opt.TableID {
	for i := 0; i < expr.ChildCount(); i++ {
		if v, ok := expr.Child(i).(memo.RelExpr); ok {
			if tsScanExpr, ok := v.(*memo.TSScanExpr); ok {
				return tsScanExpr.TSScanPrivate.Table
			}
			if tableID := ef.FindTSTableID(v, mem); tableID != 0 {
				return tableID
			}
		}
	}
	return 0
}

// MatchGroupingCols finds the corresponding column indices in the execution node that match
// the grouping columns
func (ef *execFactory) MatchGroupingCols(
	mem *memo.Memo, tableGroupIndex int, node exec.Node,
) []opt.ColumnID {
	groupingCols := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupingCols
	var columns sqlbase.ResultColumns
	var matchedIndices []opt.ColumnID
	if tsNode, ok := node.(*tsScanNode); ok {
		columns = tsNode.resultColumns
	} else if syncNode, ok := node.(*synchronizerNode); ok {
		columns = syncNode.columns
	} else if renderNode, ok := node.(*renderNode); ok {
		columns = renderNode.columns
	}

	metaData := mem.Metadata()
	for _, groupingCol := range groupingCols {
		colID := metaData.GetTagIDByColumnID(groupingCol) + 1
		tableMetaID := metaData.ColumnMeta(groupingCol).Table

		if tableMetaID != 0 {
			tableID := mem.Metadata().Table(tableMetaID).ID()
			for j, column := range columns {
				if int(column.PGAttributeNum) == colID && int(column.TableID) == int(tableID) {
					matchedIndices = append(matchedIndices, opt.ColumnID(j))
					break
				}
			}
		} else {
			for j, column := range columns {
				if column.Name == metaData.ColumnMeta(groupingCol).Alias {
					matchedIndices = append(matchedIndices, opt.ColumnID(j))
					break
				}
			}
		}
	}

	return matchedIndices
}

func contains(list []opt.ColumnID, target opt.ColumnID) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

// ProcessRenderNode updates the column metadata of a renderNode based on its source plan.
// for inside-out queries
func (ef *execFactory) ProcessRenderNode(node exec.Node) error {
	renderNode, ok := node.(*renderNode)
	if !ok {
		return fmt.Errorf("input node is not of type *renderNode")
	}

	sourcePlan := renderNode.source.plan
	var columns []sqlbase.ResultColumn

	if tsNode, ok := sourcePlan.(*tsScanNode); ok {
		columns = tsNode.resultColumns
	} else if syncNode, ok := sourcePlan.(*synchronizerNode); ok {
		columns = syncNode.columns
	} else {
		return fmt.Errorf("source.plan is of unsupported type: %T", sourcePlan)
	}

	renderNode.columns = append(renderNode.columns[:1], columns...)

	return nil
}

// AddAvgFuncColumns processes and updates the column mappings for average functions.
// This function modifies the AvgColumnRelations to include count and sum functions corresponding to avg functions.
// for inside-out queries
func (ef *execFactory) AddAvgFuncColumns(node exec.Node, mem *memo.Memo, tableGroupIndex int) {
	AggFuncs := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AggFuncs
	aggColumns := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AggColumns
	relations := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations

	for i, aggColumn := range aggColumns {
		if i < len(AggFuncs) && AggFuncs[i] == "AvgExpr" {
			relations = append(relations, []opt.ColumnID{aggColumn})
		}
	}

	mapping := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgToCountMapping
	mapping2 := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgToSumMapping
	avgFuncColumns := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns

	for i, avgColumnID := range avgFuncColumns {
		var countFuncID opt.ColumnID
		var sumFuncID opt.ColumnID
		for _, entry := range mapping {
			if entry.AvgFuncID == avgColumnID {
				countFuncID = entry.FuncID
				break
			}
		}
		for _, entry := range mapping2 {
			if entry.AvgFuncID == avgColumnID {
				sumFuncID = entry.FuncID
				break
			}
		}

		columnMeta := mem.Metadata().ColumnMeta(avgColumnID)
		index := 0
		if countFuncID != 0 && sumFuncID != 0 {
			relations[i] = append(relations[i], countFuncID)

			relations[i] = append(relations[i], sumFuncID)

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(count)", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(sum)", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
		} else if countFuncID != 0 {
			relations[i] = append(relations[i], countFuncID)

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
			mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns = append(mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns, opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(count)", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(sum)", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
		} else if sumFuncID != 0 {
			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("count", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))
			mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns = append(mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns, opt.ColumnID(index))

			relations[i] = append(relations[i], sumFuncID)

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(count)", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(sum)", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
		} else {
			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("count", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))
			mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns = append(mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns, opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
			mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns = append(mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns, opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(count)", types.Int)
			relations[i] = append(relations[i], opt.ColumnID(index))

			index = mem.Metadata().NumColumns() + 1
			mem.Metadata().AddColumn("sum(sum)", columnMeta.Type)
			relations[i] = append(relations[i], opt.ColumnID(index))
		}
	}

	mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations = relations
}
