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

package execbuilder

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/ordering"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type execPlan struct {
	root exec.Node

	// outputCols is a map from opt.ColumnID to exec.ColumnOrdinal. It maps
	// columns in the output set of a relational expression to indices in the
	// result columns of the exec.Node.
	//
	// The reason we need to keep track of this (instead of using just the
	// relational properties) is that the relational properties don't force a
	// single "schema": any ordering of the output columns is possible. We choose
	// the schema that is most convenient: for scans, we use the table's column
	// ordering. Consider:
	//   SELECT a, b FROM t WHERE a = b
	// and the following two cases:
	//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
	//      return (k, a, b).
	//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
	//      return (k, b, a).
	// In these two cases, the relational properties are effectively the same.
	//
	// An alternative to this would be to always use a "canonical" schema, for
	// example the output columns in increasing index order. However, this would
	// require a lot of otherwise unnecessary projections.
	//
	// Note: conceptually, this could be a ColList; however, the map is more
	// convenient when converting VariableOps to IndexedVars.
	outputCols opt.ColMap

	// exec engine type
	execEngine tree.EngineType
}

// numOutputCols returns the number of columns emitted by the execPlan's Node.
// This will typically be equal to ep.outputCols.Len(), but might be different
// if the node outputs the same optimizer ColumnID multiple times.
// TODO(justin): we should keep track of this instead of computing it each time.
func (ep *execPlan) numOutputCols() int {
	return numOutputColsInMap(ep.outputCols)
}

// numOutputColsInMap returns the number of slots required to fill in all of
// the columns referred to by this ColMap.
func numOutputColsInMap(m opt.ColMap) int {
	max, ok := m.MaxValue()
	if !ok {
		return 0
	}
	return max + 1
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func (ep *execPlan) makeBuildScalarCtx() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.numOutputCols()),
		ivarMap: ep.outputCols,
	}
}

func (ep *execPlan) makeBuildScalarCtx2() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.numOutputCols()),
		ivarMap: ep.outputCols.DeepCopyFastIntMap(),
	}
}

// getColumnOrdinal takes a column that is known to be produced by the execPlan
// and returns the ordinal index of that column in the result columns of the
// node.
func (ep *execPlan) getColumnOrdinal(col opt.ColumnID) exec.ColumnOrdinal {
	ord, ok := ep.outputCols.Get(int(col))
	if !ok {
		panic(errors.AssertionFailedf("column %d not in input", log.Safe(col)))
	}
	return exec.ColumnOrdinal(ord)
}

func (ep *execPlan) getColumnOrdinalSet(cols opt.ColSet) exec.ColumnOrdinalSet {
	var res exec.ColumnOrdinalSet
	cols.ForEach(func(colID opt.ColumnID) {
		res.Add(int(ep.getColumnOrdinal(colID)))
	})
	return res
}

// reqOrdering converts the provided ordering of a relational expression to an
// OutputOrdering (according to the outputCols map).
func (ep *execPlan) reqOrdering(expr memo.RelExpr) exec.OutputOrdering {
	return exec.OutputOrdering(ep.sqlOrdering(expr.ProvidedPhysical().Ordering))
}

// sqlOrdering converts an Ordering to a ColumnOrdering (according to the
// outputCols map).
func (ep *execPlan) sqlOrdering(ordering opt.Ordering) sqlbase.ColumnOrdering {
	if ordering.Empty() {
		return nil
	}
	colOrder := make(sqlbase.ColumnOrdering, len(ordering))
	for i := range ordering {
		colOrder[i].ColIdx = int(ep.getColumnOrdinal(ordering[i].ID()))
		if ordering[i].Descending() {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder
}

// checkIsTsScanExpr checks whether the given expression is a TSScanExpr or
// contains a TSScanExpr as its input within a SelectExpr or ProjectExpr.
func checkIsTsScanExpr(child opt.Expr) (bool, opt.TableID) {
	if tsScanExpr, ok := child.(*memo.TSScanExpr); ok {
		return true, tsScanExpr.TSScanPrivate.Table
	}

	if selectExpr, ok := child.(*memo.SelectExpr); ok {
		if tsScanExpr, ok := selectExpr.Input.(*memo.TSScanExpr); ok {
			return true, tsScanExpr.TSScanPrivate.Table
		}
	}
	if projectExpr, ok := child.(*memo.ProjectExpr); ok {
		if tsScanExpr, ok := projectExpr.Input.(*memo.TSScanExpr); ok {
			return true, tsScanExpr.TSScanPrivate.Table
		}
	}
	return false, opt.TableID(0)
}

func shouldBuildBatchLookUpJoin(e memo.RelExpr, mem *memo.Memo) bool {
	if innerJoinExpr, ok := e.(*memo.InnerJoinExpr); ok {
		leftFound, leftTableID := checkIsTsScanExpr(innerJoinExpr.Left)
		rightFound, rightTableID := checkIsTsScanExpr(innerJoinExpr.Right)

		if leftFound || rightFound {
			var targetTableID opt.TableID
			if leftFound {
				targetTableID = leftTableID
			} else {
				targetTableID = rightTableID
			}

			for i, group := range mem.MultimodelHelper.TableGroup {
				if len(group) > 0 && group[0] == targetTableID {
					if mem.MultimodelHelper.PlanMode[i] == memo.OutsideIn {
						joinOn := innerJoinExpr.On
						if len(joinOn) == 0 {
							mem.QueryType = memo.Unset
							mem.MultimodelHelper.ResetReasons[memo.UnsupportedCrossJoin] = struct{}{}
						} else {
							for _, jp := range joinOn {
								if _, ok := jp.Condition.(*memo.OrExpr); ok {
									mem.QueryType = memo.Unset
									mem.MultimodelHelper.ResetReasons[memo.UnsupportedCrossJoin] = struct{}{}
									return false
								} else if mem.IsTsColsJoinPredicate(jp) {
									mem.QueryType = memo.Unset
									return false
								}
							}
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func (b *Builder) buildRelational(e memo.RelExpr) (execPlan, error) {
	var ep execPlan
	var err error

	if opt.IsDDLOp(e) {
		// Mark the statement as containing DDL for use
		// in the SQL executor.
		b.IsDDL = true

		// This will set the system DB trigger for transactions containing
		// schema-modifying statements that have no effect, such as
		// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
		// where the table already exists. This will generate some false schema
		// cache refreshes, but that's expected to be quite rare in practice.
		if err := b.evalCtx.Txn.SetSystemConfigTrigger(); err != nil {
			return execPlan{}, errors.WithSecondaryError(
				unimplemented.NewWithIssuef(26508,
					"schema change statement cannot follow a statement that has written in the same transaction"),
				err)
		}
	}

	// Raise error if mutation op is part of a read-only transaction.
	if opt.IsMutationOp(e) && b.evalCtx.TxnReadOnly {
		return execPlan{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
			"cannot execute %s in a read-only transaction", b.statementTag(e))
	}

	// Collect usage telemetry for relational node, if appropriate.
	if !b.disableTelemetry {
		if c := opt.OpTelemetryCounters[e.Op()]; c != nil {
			telemetry.Inc(c)
		}
	}

	var saveTableName string
	if b.nameGen != nil {
		// Don't save tables for operators that don't produce any columns (most
		// importantly, for SET which is used to disable saving of tables).
		if !e.Relational().OutputCols.Empty() {
			// This function must be called in a pre-order traversal of the tree.
			saveTableName = b.nameGen.GenerateName(e.Op())
		}
	}

	addSynchronizer := true
	switch t := e.(type) {
	case *memo.ValuesExpr:
		ep, err = b.buildValues(t)

	case *memo.ScanExpr:
		ep, err = b.buildScan(t)

	case *memo.VirtualScanExpr:
		ep, err = b.buildVirtualScan(t)

	case *memo.SelectExpr:
		ep, addSynchronizer, err = b.buildSelect(t)

	case *memo.ProjectExpr:
		ep, err = b.buildProject(t)

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		ep, err = b.buildGroupBy(e)
		addSynchronizer = false

	case *memo.DistinctOnExpr, *memo.UpsertDistinctOnExpr:
		ep, err = b.buildDistinct(t)

	case *memo.LimitExpr, *memo.OffsetExpr:
		ep, addSynchronizer, err = b.buildLimitOffset(e)

	case *memo.SortExpr:
		ep, err = b.buildSort(t)

	case *memo.IndexJoinExpr:
		ep, err = b.buildIndexJoin(t)

	case *memo.LookupJoinExpr:
		ep, err = b.buildLookupJoin(t)

	case *memo.ZigzagJoinExpr:
		ep, err = b.buildZigzagJoin(t)

	case *memo.OrdinalityExpr:
		ep, err = b.buildOrdinality(t)

	case *memo.MergeJoinExpr:
		ep, err = b.buildMergeJoin(t)

	case *memo.Max1RowExpr:
		ep, err = b.buildMax1Row(t)

	case *memo.ProjectSetExpr:
		ep, err = b.buildProjectSet(t)

	case *memo.WindowExpr:
		ep, err = b.buildWindow(t)

	case *memo.SequenceSelectExpr:
		ep, err = b.buildSequenceSelect(t)

	case *memo.InsertExpr:
		ep, err = b.buildInsert(t)

	case *memo.TSInsertExpr:
		ep, err = b.buildTSInsert(t)

	case *memo.UpdateExpr:
		ep, err = b.buildUpdate(t)

	case *memo.TSUpdateExpr:
		ep, err = b.buildTSUpdate(t)

	case *memo.UpsertExpr:
		ep, err = b.buildUpsert(t)

	case *memo.DeleteExpr:
		ep, err = b.buildDelete(t)

	case *memo.TSDeleteExpr:
		ep, err = b.buildTSDelete(t)

	case *memo.CreateTableExpr:
		ep, err = b.buildCreateTable(t)

	case *memo.CreateViewExpr:
		ep, err = b.buildCreateView(t)

	case *memo.WithExpr:
		ep, err = b.buildWith(t)

	case *memo.WithScanExpr:
		ep, err = b.buildWithScan(t)

	case *memo.RecursiveCTEExpr:
		ep, err = b.buildRecursiveCTE(t)

	case *memo.ExplainExpr:
		ep, err = b.buildExplain(t)

	case *memo.ShowTraceForSessionExpr:
		ep, err = b.buildShowTrace(t)

	case *memo.OpaqueRelExpr, *memo.OpaqueMutationExpr, *memo.OpaqueDDLExpr:
		ep, err = b.buildOpaque(t.Private().(*memo.OpaqueRelPrivate))

	case *memo.AlterTableSplitExpr:
		ep, err = b.buildAlterTableSplit(t)

	case *memo.AlterTableUnsplitExpr:
		ep, err = b.buildAlterTableUnsplit(t)

	case *memo.AlterTableUnsplitAllExpr:
		ep, err = b.buildAlterTableUnsplitAll(t)

	case *memo.SelectIntoExpr:
		ep, err = b.buildSelectInto(t)

	case *memo.AlterTableRelocateExpr:
		ep, err = b.buildAlterTableRelocate(t)

	case *memo.ControlJobsExpr:
		ep, err = b.buildControlJobs(t)

	case *memo.CancelQueriesExpr:
		ep, err = b.buildCancelQueries(t)

	case *memo.CancelSessionsExpr:
		ep, err = b.buildCancelSessions(t)

	case *memo.ExportExpr:
		ep, err = b.buildExport(t)

	case *memo.TSScanExpr:
		ep, err = b.buildTimesScan(t)

	case *memo.TSInsertSelectExpr:
		ep, err = b.buildTsInsertSelect(t)

	case *memo.BatchLookUpJoinExpr:
		continueHashJoin := false
		ep, continueHashJoin, err = b.buildBatchLookUpJoin(e)
		if continueHashJoin {
			ep, err = b.buildHashJoin(e)
		}

	default:
		switch {
		case opt.IsSetOp(e):
			ep, err = b.buildSetOp(e)

		case opt.IsJoinNonApplyOp(e):
			// check if we should build batchLookUpJoinNode for multiple model processing.
			if b.mem.QueryType == memo.MultiModel &&
				!opt.CheckOptMode(opt.TSQueryOptMode.Get(&b.evalCtx.Settings.SV), opt.OutsideInUseCBO) &&
				shouldBuildBatchLookUpJoin(e, b.mem) {
				var continueHashJoin bool
				ep, continueHashJoin, err = b.buildBatchLookUpJoin(e)
				if continueHashJoin {
					ep, err = b.buildHashJoin(e)
				}
			} else {
				ep, err = b.buildHashJoin(e)
			}

		case opt.IsJoinApplyOp(e):
			ep, err = b.buildApplyJoin(e)

		default:
			err = errors.AssertionFailedf("no execbuild for %T", t)
		}
	}
	if err != nil {
		return execPlan{}, err
	}

	// In race builds, assert that the exec plan output columns match the opt
	// plan output columns.
	if util.RaceEnabled {
		optCols := e.Relational().OutputCols
		var execCols opt.ColSet
		ep.outputCols.ForEach(func(key, val int) {
			execCols.Add(opt.ColumnID(key))
		})
		if !execCols.Equals(optCols) {
			return execPlan{}, errors.AssertionFailedf(
				"exec columns do not match opt columns: expected %v, got %v", optCols, execCols)
		}
	}

	if addSynchronizer && e.GetAddSynchronizer() {
		ep = b.buildSynchronizer(ep)
	}

	if e.IsTSEngine() {
		ep.execEngine = tree.EngineTypeTimeseries
	}

	if saveTableName != "" {
		ep, err = b.applySaveTable(ep, e, saveTableName)
		if err != nil {
			return execPlan{}, err
		}
	}

	// Wrap the expression in a render expression if presentation requires it.
	if p := e.RequiredPhysical(); p != nil && !p.Presentation.Any() {
		ep, err = b.applyPresentation(ep, p)
	}

	return ep, err
}

func (b *Builder) buildValues(values *memo.ValuesExpr) (execPlan, error) {
	rows, err := b.buildValuesRows(values)
	if err != nil {
		return execPlan{}, err
	}
	return b.constructValues(rows, values.Cols)
}

func (b *Builder) buildValuesRows(values *memo.ValuesExpr) ([][]tree.TypedExpr, error) {
	numCols := len(values.Cols)

	rows := make([][]tree.TypedExpr, len(values.Rows))
	rowBuf := make([]tree.TypedExpr, len(rows)*numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		tup := values.Rows[i].(*memo.TupleExpr)
		if len(tup.Elems) != numCols {
			return nil, fmt.Errorf("inconsistent row length %d vs %d", len(tup.Elems), numCols)
		}
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
		var err error
		for j := 0; j < numCols; j++ {
			rows[i][j], err = b.buildScalar(&scalarCtx, tup.Elems[j])
			if err != nil {
				return nil, err
			}
		}
	}
	return rows, nil
}

func (b *Builder) constructValues(rows [][]tree.TypedExpr, cols opt.ColList) (execPlan, error) {
	md := b.mem.Metadata()
	resultCols := make(sqlbase.ResultColumns, len(cols))
	for i, col := range cols {
		colMeta := md.ColumnMeta(col)
		resultCols[i].Name = colMeta.Alias
		resultCols[i].Typ = colMeta.Type
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range cols {
		ep.outputCols.Set(int(col), i)
	}

	return ep, nil
}

// getColumns returns the set of column ordinals in the table for the set of
// column IDs, along with a mapping from the column IDs to output ordinals
// (starting with outputOrdinalStart).
func (b *Builder) getColumns(
	cols opt.ColSet, tableID opt.TableID,
) (exec.ColumnOrdinalSet, opt.ColMap) {
	needed := exec.ColumnOrdinalSet{}
	output := opt.ColMap{}

	columnCount := b.mem.Metadata().Table(tableID).DeletableColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := tableID.ColumnID(i)
		if cols.Contains(colID) {
			needed.Add(i)
			output.Set(int(colID), n)
			n++
		}
	}

	return needed, output
}

// getOutputColumns traverses all column id under the table,
// if included in ColSet, store in ColMap and return.
//
// Parameters:
// - needCols: columnIDs of need
// - tableID: table ID
//
// Returns:
// - opt.ColMap: output columns set
func (b *Builder) getOutputColumns(needCols opt.ColSet, tableID opt.TableID) opt.ColMap {
	output := opt.ColMap{}
	// get table descriptor.
	table := b.mem.Metadata().Table(tableID)
	n := 0
	for i := 0; i < table.DeletableColumnCount(); i++ {
		logicalID := tableID.ColumnID(i)
		if needCols.Contains(logicalID) {
			output.Set(int(logicalID), n)
			n++
		}
	}
	return output
}

// indexConstraintMaxResults returns the maximum number of results for a scan;
// the scan is guaranteed never to return more results than this. Iff this hint
// is invalid, 0 is returned.
func (b *Builder) indexConstraintMaxResults(scan *memo.ScanExpr) uint64 {
	c := scan.Constraint
	if c == nil || c.IsContradiction() || c.IsUnconstrained() {
		return 0
	}

	numCols := c.Columns.Count()
	var indexCols opt.ColSet
	for i := 0; i < numCols; i++ {
		indexCols.Add(c.Columns.Get(i).ID())
	}
	rel := scan.Relational()
	if !rel.FuncDeps.ColsAreLaxKey(indexCols) {
		return 0
	}

	return c.CalculateMaxResults(b.evalCtx, indexCols, rel.NotNullCols)
}

func (b *Builder) buildScan(scan *memo.ScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	// Check if we tried to force a specific index but there was no Scan with that
	// index in the memo.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		idx := tab.Index(scan.Flags.Index)
		var err error
		if idx.IsInverted() {
			err = fmt.Errorf("index \"%s\" is inverted and cannot be used for this query", idx.Name())
		} else {
			// This should never happen.
			err = fmt.Errorf("index \"%s\" cannot be used for this query", idx.Name())
		}
		return execPlan{}, err
	}

	needed, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	// Get the estimated row count from the statistics.
	// Note: if this memo was originally created as part of a PREPARE
	// statement or was stored in the query cache, the column stats would have
	// been removed by DetachMemo. Update that function if the column stats are
	// needed here in the future.
	rowCount := scan.Relational().Stats.RowCount
	if !scan.Relational().Stats.Available {
		// When there are no statistics available, we construct a scan node with
		// the estimated row count of zero rows.
		rowCount = 0
	}

	if scan.PartitionConstrainedScan {
		sqltelemetry.IncrementPartitioningCounter(sqltelemetry.PartitionConstrainedScan)
	}

	softLimit := int64(math.Ceil(scan.RequiredPhysical().LimitHint))
	hardLimit := scan.HardLimit.RowCount()

	locking := scan.Locking
	if b.forceForUpdateLocking {
		locking = forUpdateLocking
	}

	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		needed,
		scan.Constraint,
		hardLimit,
		softLimit,
		// HardLimit.Reverse() is taken into account by ScanIsReverse.
		ordering.ScanIsReverse(scan, &scan.RequiredPhysical().Ordering),
		b.indexConstraintMaxResults(scan),
		res.reqOrdering(scan),
		rowCount,
		locking,
	)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

// buildTimesScan build time series scanNode.
func (b *Builder) buildTimesScan(scan *memo.TSScanExpr) (execPlan, error) {
	b.PhysType = tree.TS
	md := b.mem.Metadata()
	table := md.Table(scan.Table)

	// get the mapping relationship between the
	// output column ID and the resultColumns index.

	res := execPlan{outputCols: b.getOutputColumns(scan.Cols, scan.Table)}
	outputHasTag := false
	scan.Cols.ForEach(func(colID opt.ColumnID) {
		colMeta := b.mem.Metadata().ColumnMeta(colID)
		outputHasTag = outputHasTag || colMeta.IsTag()
	})

	if scan.AccessMode < 0 {
		// accessMode default value is TSReaderSpec_metaTable
		scan.AccessMode = int(execinfrapb.TSTableReadMode_metaTable)
		if outputHasTag {
			scan.AccessMode = int(execinfrapb.TSTableReadMode_tableTableMeta)
		}
	}

	ctx := res.makeBuildScalarCtx()
	var tagFilter []tree.TypedExpr
	if !b.buildArrayTypedExpr(scan.TagFilter, &ctx, &tagFilter) {
		return execPlan{}, nil
	}

	var primaryFilter []tree.TypedExpr
	if !b.buildArrayTypedExpr(scan.PrimaryTagFilter, &ctx, &primaryFilter) {
		return execPlan{}, nil
	}

	var tagIndexFilter []tree.TypedExpr
	if !b.buildArrayTypedExpr(scan.TagIndexFilter, &ctx, &tagIndexFilter) {
		return execPlan{}, nil
	}

	if b.mem.CheckFlag(opt.DiffUseOrderScan) && b.mem.CheckFlag(opt.SingleMode) {
		scan.OrderedScanType = opt.ForceOrderedScan
	}
	value, ok := b.mem.MultimodelHelper.TableData.Load(scan.Table)
	var tableInfo memo.TableInfo
	if ok {
		tableInfo = value.(memo.TableInfo)
	} else {
		tableInfo = memo.TableInfo{}
	}
	tableInfo.PrimaryFilterLen = len(primaryFilter)
	tableInfo.PrimaryTagCount = md.TableMeta(scan.Table).PrimaryTagCount
	tableInfo.OriginalAccessMode = execinfrapb.TSTableReadMode(scan.AccessMode)
	b.mem.MultimodelHelper.TableData.Store(scan.Table, tableInfo)

	// Get the estimated row count from the statistics.
	// Note: if this memo was originally created as part of a PREPARE
	// statement or was stored in the query cache, the column stats would have
	// been removed by DetachMemo. Update that function if the column stats are
	// needed here in the future.
	rowCount := scan.Relational().Stats.RowCount
	if !scan.Relational().Stats.Available {
		// When there are no statistics available, we construct a scan node with
		// the estimated row count of zero rows.
		rowCount = 0
	}

	// build scanNode.
	root, err := b.factory.ConstructTSScan(table, &scan.TSScanPrivate, tagFilter, primaryFilter, tagIndexFilter, rowCount)
	if err != nil {
		return execPlan{}, err
	}
	err = b.processInsideoutForTimesScan(scan, &res, root)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

// processInsideoutForTimesScan processes insideout optimization.
func (b *Builder) processInsideoutForTimesScan(
	scan *memo.TSScanExpr, res *execPlan, root exec.Node,
) error {
	md := b.mem.Metadata()
	tableGroupIndex := b.mem.MultimodelHelper.GetTableIndexFromGroup(scan.TSScanPrivate.Table)
	if b.mem.QueryType == memo.MultiModel &&
		tableGroupIndex >= 0 &&
		b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut &&
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildingPreGrouping &&
		!b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter {
		if b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr != nil &&
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].HasTimeBucket {
			res.root = root
			if scan.GetAddSynchronizer() {
				scan.ResetAddSynchronizer()
			}

			prj := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr
			passthroughCopy := prj.(*memo.ProjectExpr).Passthrough
			passthroughCopy = opt.ColSet{}
			res.outputCols.ForEach(func(key, val int) {
				passthroughCopy.Add(opt.ColumnID(key))
			})

			projections := prj.(*memo.ProjectExpr).Projections
			exprs := make(tree.TypedExprs, 0, len(projections)+prj.(*memo.ProjectExpr).Passthrough.Len())
			colNames := make([]string, 0, len(exprs))
			ctx := res.makeBuildScalarCtx()

			for i := range projections {
				item := &projections[i]
				expr, err1 := b.buildScalar(&ctx, item.Element)
				if err1 != nil {
					return err1
				}
				res.outputCols.Set(int(item.Col), i)
				exprs = append(exprs, expr)
				colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
			}

			passthroughCopy.ForEach(func(colID opt.ColumnID) {
				res.outputCols.Set(int(colID), len(exprs))
				exprs = append(exprs, b.indexedVar(&ctx, md, colID))
				colNames = append(colNames, md.ColumnMeta(colID).Alias)
			})
			reqOrdering := res.reqOrdering(prj)
			var err error
			res.root, err = b.factory.ConstructRender(res.root, exprs, colNames, reqOrdering, true)
			if err != nil {
				return err
			}

			err = b.factory.ProcessRenderNode(res.root)
			if err != nil {
				return err
			}

			err = b.buildGroupNodeForInsideOut(res, tableGroupIndex)
			if err != nil {
				return err
			}
		} else {
			res.root = root
			if scan.GetAddSynchronizer() {
				scan.ResetAddSynchronizer()
			}

			err := b.buildGroupNodeForInsideOut(res, tableGroupIndex)
			if err != nil {
				return err
			}
		}
	} else {
		res.root = root
	}
	return nil
}

// processGroupByExpr processes the aggregation expressions inside a GroupBy or ScalarGroupBy expression.
// It applies the necessary transformations to the aggregation list based on pre-grouping rules
func processGroupByExpr(groupByExpr memo.RelExpr, mem *memo.Memo, tableGroupIndex int) {
	switch e := groupByExpr.(type) {
	case *memo.GroupByExpr:
		finalAggregations := processAggregations(e.Aggregations, mem, tableGroupIndex)
		e.Aggregations = finalAggregations
	case *memo.ScalarGroupByExpr:
		finalAggregations := processAggregations(e.Aggregations, mem, tableGroupIndex)
		e.Aggregations = finalAggregations
	default:
	}
}

// processAggregations processes a list of aggregation expressions and transforms them based on pre-grouping logic.
// for inside-out queries
func processAggregations(
	aggregations memo.AggregationsExpr, mem *memo.Memo, tableGroupIndex int,
) memo.AggregationsExpr {
	var newAggregations memo.AggregationsExpr
	var finalAggregations memo.AggregationsExpr
	indexes := mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupByExprPos
	for index, aggregation := range aggregations {
		isInIndexes := false
		for _, idx := range indexes {
			if index == idx {
				isInIndexes = true
				break
			}
		}
		if isInIndexes {
			if avgExpr, ok := aggregation.Agg.(*memo.AvgExpr); ok {
				var newAggregation1, newAggregation2 memo.AggregationsItem

				for _, relation := range mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations {
					if v, ok := avgExpr.Input.(*memo.VariableExpr); ok {
						if v.Col == relation[0] {
							expr1 := &memo.VariableExpr{
								Col:                 relation[1],
								IsConstForLogicPlan: v.IsConstForLogicPlan,
								Typ:                 types.Int,
							}
							newAggregation1.Agg = &memo.SumIntExpr{
								Input:               expr1,
								IsConstForLogicPlan: avgExpr.IsConstForLogicPlan,
								Typ:                 types.Int,
							}
							newAggregation1.Col = relation[3]
							newAggregation1.IsConstForLogicPlan = aggregation.IsConstForLogicPlan
							newAggregation1.Typ = aggregation.Typ

							expr2 := &memo.VariableExpr{
								Col:                 relation[2],
								IsConstForLogicPlan: v.IsConstForLogicPlan,
								Typ:                 v.Typ,
							}
							newAggregation2.Agg = &memo.SumExpr{
								Input:               expr2,
								IsConstForLogicPlan: avgExpr.IsConstForLogicPlan,
								Typ:                 avgExpr.Typ,
							}
							newAggregation2.Col = relation[4]
							newAggregation2.IsConstForLogicPlan = aggregation.IsConstForLogicPlan
							newAggregation2.Typ = aggregation.Typ
							break
						}
					}
				}

				newAggregations = append(newAggregations, newAggregation1, newAggregation2)
			} else {
				switch agg := aggregation.Agg.(type) {
				case *memo.CountExpr, *memo.CountRowsExpr, *memo.MinExpr, *memo.MaxExpr, *memo.SumExpr:
					newAggregations = append(newAggregations, createAggregationItem(agg, aggregation.Col, aggregation, mem))
				}
			}
		} else {
			newAggregations = append(newAggregations, aggregation)
		}
	}

	for _, newAgg := range newAggregations {
		if _, ok := newAgg.Agg.(*memo.AvgExpr); !ok {
			finalAggregations = append(finalAggregations, newAgg)
		}
	}
	return finalAggregations
}

// createAggregationItem constructs a new aggregation item based on the given aggregation type.
// for inside-out queries
func createAggregationItem(
	aggType interface{}, colID opt.ColumnID, aggregation memo.AggregationsItem, mem *memo.Memo,
) memo.AggregationsItem {
	expr := &memo.VariableExpr{
		Col:                 colID,
		IsConstForLogicPlan: false,
		Typ:                 mem.Metadata().ColumnMeta(colID).Type,
	}

	var newAgg memo.AggregationsItem
	switch agg := aggType.(type) {
	case *memo.CountExpr:
		newAgg.Agg = &memo.SumIntExpr{
			Input:               expr,
			IsConstForLogicPlan: agg.IsConstForLogicPlan,
			Typ:                 agg.Typ,
		}
	case *memo.CountRowsExpr:
		newAgg.Agg = &memo.SumExpr{
			Input:               expr,
			IsConstForLogicPlan: agg.IsConstForLogicPlan,
			Typ:                 types.Int,
		}
	case *memo.MinExpr:
		newAgg.Agg = &memo.MinExpr{
			Input:               expr,
			IsConstForLogicPlan: agg.IsConstForLogicPlan,
			Typ:                 agg.Typ,
		}
	case *memo.MaxExpr:
		newAgg.Agg = &memo.MaxExpr{
			Input:               expr,
			IsConstForLogicPlan: agg.IsConstForLogicPlan,
			Typ:                 agg.Typ,
		}
	case *memo.SumExpr:
		newAgg.Agg = &memo.SumExpr{
			Input:               expr,
			IsConstForLogicPlan: agg.IsConstForLogicPlan,
			Typ:                 agg.Typ,
		}
	}

	newAgg.Col = colID
	newAgg.IsConstForLogicPlan = aggregation.IsConstForLogicPlan
	newAgg.Typ = aggregation.Typ
	return newAgg
}

// convertToColumnOrdinal converts a slice of opt.ColumnID values into a slice of exec.ColumnOrdinal values.
func convertToColumnOrdinal(columns []opt.ColumnID) []exec.ColumnOrdinal {
	result := make([]exec.ColumnOrdinal, len(columns))
	for i, colID := range columns {
		result[i] = exec.ColumnOrdinal(colID)
	}
	return result
}

// transformAggInfos processes a list of aggregation functions, transforming "avg" functions into equivalent
// "sum" and/or "count" functions based on their indices in the given sets.
//   - If an "avg" aggregation appears in both `indices` and `indices2`, it is removed.
//   - (Since both "sum" and "count" already exist for the same column)
//   - If it appears only in `indices2`, it is converted to a "count" function.
//   - (The "sum" function already exists for the column)
//   - If it appears only in `indices`, it is converted to a "sum" function.
//   - (The "count" function already exists for the column)
//   - If it appears in neither, it is split into both "sum" and "count" functions.
func transformAggInfos(aggInfos []exec.AggInfo, indices []int, indices2 []int) []exec.AggInfo {
	var newAggInfos []exec.AggInfo
	var delayedAvgInfos []struct {
		index   int
		aggInfo exec.AggInfo
	}

	indexSet := make(map[int]bool)
	for _, idx := range indices {
		indexSet[idx] = true
	}

	indexSet2 := make(map[int]bool)
	for _, idx := range indices2 {
		indexSet2[idx] = true
	}

	for index, aggInfo := range aggInfos {
		if aggInfo.FuncName == "avg" {
			delayedAvgInfos = append(delayedAvgInfos, struct {
				index   int
				aggInfo exec.AggInfo
			}{
				index:   index,
				aggInfo: aggInfo,
			})
		} else {
			newAggInfos = append(newAggInfos, aggInfo)
		}
	}

	for _, item := range delayedAvgInfos {
		originalIndex := item.index
		aggInfo := item.aggInfo

		if indexSet[originalIndex] && indexSet2[originalIndex] {
			continue
		} else if indexSet2[originalIndex] {
			countAggInfo := aggInfo
			countAggInfo.FuncName = "count"
			countAggInfo.ResultType = types.Int
			newAggInfos = append(newAggInfos, countAggInfo)
		} else if indexSet[originalIndex] {
			sumAggInfo := aggInfo
			sumAggInfo.FuncName = "sum"
			newAggInfos = append(newAggInfos, sumAggInfo)
		} else {
			countAggInfo := aggInfo
			countAggInfo.FuncName = "count"
			countAggInfo.ResultType = types.Int

			sumAggInfo := aggInfo
			sumAggInfo.FuncName = "sum"

			newAggInfos = append(newAggInfos, countAggInfo, sumAggInfo)
		}
	}

	return newAggInfos
}

// buildTsInsertSelect build tsInsertSelectNode
func (b *Builder) buildTsInsertSelect(scan *memo.TSInsertSelectExpr) (execPlan, error) {
	b.PhysType = tree.TS
	tableDesc := b.mem.Metadata().Table(scan.STable)
	tableName := string(tableDesc.Name())
	insTableID := uint64(tableDesc.ID())
	tableType := tree.TimeseriesTable
	// if scanCTable and InstName exist, table is child table or super table
	if scan.CTable > 0 && len(scan.CName) > 0 {
		tableType = tree.InstanceTable
		if scan.CTable == scan.STable {
			tableType = tree.TemplateTable
		}
		tableName = scan.CName
		insTableID = uint64(scan.CTable)
	}

	// build planNode of subExpr
	input, err := b.buildRelational(scan.Input)
	if err != nil {
		return execPlan{}, err
	}

	// build tsInsertSelectNode
	root, err := b.factory.ConstructTsInsertSelect(
		input.root,
		insTableID,
		uint64(tableDesc.GetParentID()),
		scan.Cols,
		scan.ColIdxs,
		tableName,
		int32(tableType),
	)
	if err != nil {
		return execPlan{}, err
	}

	return execPlan{root: root}, nil
}

// buildSynchronizer build SynchronizerNode
func (b *Builder) buildSynchronizer(input execPlan) execPlan {
	// ConstructSynchronizer
	node, _ := b.factory.ConstructSynchronizer(input.root, opt.GetTSParallelDegree(b.evalCtx))
	return execPlan{root: node, outputCols: input.outputCols}
}

func (b *Builder) buildVirtualScan(scan *memo.VirtualScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	_, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	root, err := b.factory.ConstructVirtualScan(tab)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

// buildTypedExpr build expr as typed expr
func (b *Builder) buildArrayTypedExpr(
	array []opt.Expr, ctx *buildScalarCtx, res *[]tree.TypedExpr,
) bool {
	for _, val := range array {
		filter, err1 := b.buildScalar(ctx, val.(opt.ScalarExpr))
		if err1 != nil {
			return false
		}

		*res = append(*res, filter)
	}
	return true
}

func (b *Builder) buildSelect(sel *memo.SelectExpr) (execPlan, bool, error) {
	if tsScanExpr, ok := sel.Input.(*memo.TSScanExpr); ok {
		tableGroupIndex := b.mem.MultimodelHelper.GetTableIndexFromGroup(tsScanExpr.Table)
		if tableGroupIndex >= 0 && len(b.mem.MultimodelHelper.PreGroupInfos) >= tableGroupIndex+1 {
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter = true
		}
	}
	input, err := b.buildRelational(sel.Input)
	if err != nil {
		return execPlan{}, false, err
	}
	ctx := input.makeBuildScalarCtx()

	// some filter can not push down to ae engine
	var pushFilter memo.FiltersExpr
	var leaveFilter memo.FiltersExpr
	// self can not push but some filter can push
	_, ok := sel.Input.(*memo.TSScanExpr)
	if !sel.IsTSEngine() && sel.Input.IsTSEngine() && ok {
		for i := range sel.Filters {
			if sel.Filters[i].IsTSEngine() {
				pushFilter = append(pushFilter, sel.Filters[i])
			} else {
				leaveFilter = append(leaveFilter, sel.Filters[i])
			}
		}
	}

	if len(pushFilter) > 0 {
		// construct push filter node
		b.factory.MakeTSSpans(&pushFilter, input.root, b.mem)
		filter, err := b.buildScalar(&ctx, &pushFilter)
		if err != nil {
			return execPlan{}, false, err
		}

		// A filtering node does not modify the schema.
		res := execPlan{outputCols: input.outputCols}
		reqOrder := res.reqOrdering(sel)

		res.root, err = b.factory.ConstructFilter(input.root, filter, reqOrder, true)
		if err != nil {
			return execPlan{}, false, err
		}

		// add synchronizer
		if sel.GetAddSynchronizer() {
			res = b.buildSynchronizer(res)
		}

		// construct leave filter node
		b.factory.MakeTSSpans(&leaveFilter, input.root, b.mem)
		filter, err = b.buildScalar(&ctx, &leaveFilter)
		if err != nil {
			return execPlan{}, false, err
		}

		// A filtering node does not modify the schema.
		res1 := execPlan{outputCols: input.outputCols}
		res1.root, err = b.factory.ConstructFilter(res.root, filter, reqOrder, sel.IsTSEngine())
		if err != nil {
			return execPlan{}, false, err
		}

		return res1, false, nil
	}

	// MakeTSSpans will remove some filters when can be changed to span, but can not change the original memo expr.
	filters := sel.Filters
	filterChangeToSpan := b.factory.MakeTSSpans(&filters, input.root, b.mem)

	tsTableID := b.factory.FindTsScanNode(input.root, b.mem)
	tableGroupIndex := b.mem.MultimodelHelper.GetTableIndexFromGroup(tsTableID)

	// All filters have been converted to span, no need to build a filter node.
	if filterChangeToSpan {
		err := b.processInsideoutForSelect(sel, &input, tableGroupIndex)
		if err != nil {
			return execPlan{}, false, err
		}
		return input, true, nil
	}

	filter, err := b.buildScalar(&ctx, &filters)
	if err != nil {
		return execPlan{}, true, err
	}

	// A filtering node does not modify the schema.
	res := execPlan{outputCols: input.outputCols}
	reqOrder := res.reqOrdering(sel)

	res.root, err = b.factory.ConstructFilter(input.root, filter, reqOrder, sel.IsTSEngine())
	if err != nil {
		return execPlan{}, true, err
	}
	err = b.processTimeBucketInsideoutForSelect(sel, &res, tableGroupIndex)
	if err != nil {
		return execPlan{}, false, err
	}
	return res, true, nil
}

// processTimeBucketInsideoutForSelect processes insideout optimization for select
func (b *Builder) processTimeBucketInsideoutForSelect(
	sel *memo.SelectExpr, res *execPlan, tableGroupIndex int,
) error {
	if tableGroupIndex >= 0 && b.mem.QueryType == memo.MultiModel &&
		b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut &&
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildingPreGrouping &&
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter {
		if b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr != nil &&
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].HasTimeBucket {
			if sel.GetAddSynchronizer() {
				sel.ResetAddSynchronizer()
			}

			prj := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr
			passthroughCopy := prj.(*memo.ProjectExpr).Passthrough
			passthroughCopy = opt.ColSet{}
			res.outputCols.ForEach(func(key, val int) {
				passthroughCopy.Add(opt.ColumnID(key))
			})

			projections := prj.(*memo.ProjectExpr).Projections
			exprs := make(tree.TypedExprs, 0, len(projections)+prj.(*memo.ProjectExpr).Passthrough.Len())
			colNames := make([]string, 0, len(exprs))
			ctx := res.makeBuildScalarCtx()
			md := b.mem.Metadata()

			for i := range projections {
				item := &projections[i]
				expr, err1 := b.buildScalar(&ctx, item.Element)
				if err1 != nil {
					return err1
				}
				res.outputCols.Set(int(item.Col), i)
				exprs = append(exprs, expr)
				colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
			}

			passthroughCopy.ForEach(func(colID opt.ColumnID) {
				res.outputCols.Set(int(colID), len(exprs))
				exprs = append(exprs, b.indexedVar(&ctx, md, colID))
				colNames = append(colNames, md.ColumnMeta(colID).Alias)
			})
			reqOrdering := res.reqOrdering(prj)
			var err error
			res.root, err = b.factory.ConstructRender(res.root, exprs, colNames, reqOrdering, true)
			if err != nil {
				return err
			}

			err = b.factory.ProcessRenderNode(res.root)
			if err != nil {
				return err
			}

			err = b.buildGroupNodeForInsideOut(res, tableGroupIndex)
			if err != nil {
				return err
			}
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter = false
		} else {
			if sel.GetAddSynchronizer() {
				sel.ResetAddSynchronizer()
			}

			err := b.buildGroupNodeForInsideOut(res, tableGroupIndex)
			if err != nil {
				return err
			}
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter = false
		}
	}
	return nil
}

// processInsideoutForSelect processes insideout optimization for select
func (b *Builder) processInsideoutForSelect(
	sel *memo.SelectExpr, input *execPlan, tableGroupIndex int,
) error {
	if tableGroupIndex >= 0 && b.mem.QueryType == memo.MultiModel &&
		b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut &&
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildingPreGrouping &&
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter {
		if sel.GetAddSynchronizer() {
			sel.ResetAddSynchronizer()
		}

		prj := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr
		if prj != nil {
			md := b.mem.Metadata()
			passthroughCopy := prj.(*memo.ProjectExpr).Passthrough
			passthroughCopy = opt.ColSet{}
			input.outputCols.ForEach(func(key, val int) {
				passthroughCopy.Add(opt.ColumnID(key))
			})

			projections := prj.(*memo.ProjectExpr).Projections
			exprs := make(tree.TypedExprs, 0, len(projections)+prj.(*memo.ProjectExpr).Passthrough.Len())
			colNames := make([]string, 0, len(exprs))
			ctx := input.makeBuildScalarCtx2()
			for i := range projections {
				item := &projections[i]
				expr, err1 := b.buildScalar(&ctx, item.Element)
				if err1 != nil {
					return err1
				}
				input.outputCols.Set(int(item.Col), i)
				exprs = append(exprs, expr)
				colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
			}
			passthroughCopy.ForEach(func(colID opt.ColumnID) {
				input.outputCols.Set(int(colID), len(exprs))
				exprs = append(exprs, b.indexedVar(&ctx, md, colID))
				colNames = append(colNames, md.ColumnMeta(colID).Alias)
			})
			reqOrdering := input.reqOrdering(prj)
			var err error
			input.root, err = b.factory.ConstructRender(input.root, exprs, colNames, reqOrdering, true)
			if err != nil {
				return err
			}

			err = b.factory.ProcessRenderNode(input.root)
			if err != nil {
				return err
			}
		}

		err := b.buildGroupNodeForInsideOut(input, tableGroupIndex)
		if err != nil {
			return err
		}
		b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildGroupAfterFilter = false
	}
	return nil
}

// buildGroupNodeForInsideOut constructs pre-aggregation node
func (b *Builder) buildGroupNodeForInsideOut(res *execPlan, tableGroupIndex int) error {
	groupingCols := b.factory.MatchGroupingCols(b.mem, tableGroupIndex, res.root)
	groupingColIdx := convertToColumnOrdinal(groupingCols)

	aggregations := *b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupByExpr.Child(1).(*memo.AggregationsExpr)
	var aggInfos []exec.AggInfo
	var indices []int
	var indices2 []int
	pos := 0
	for i, agg := range aggregations {
		if b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildingPreGrouping {
			found := false
			for _, gPox := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupByExprPos {
				if gPox == i {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		aggInfos = append(aggInfos, exec.AggInfo{})
		if err := getAggInfos(agg.Agg, pos, res.getColumnOrdinal, &aggInfos); err != nil {
			return err
		}
		for _, mapping := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgToCountMapping {
			if opt.ColumnID(agg.Col) == mapping.AvgFuncID && mapping.FuncID != 0 {
				indices = append(indices, i)
				break
			}
		}
		for _, mapping := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgToSumMapping {
			if opt.ColumnID(agg.Col) == mapping.AvgFuncID && mapping.FuncID != 0 {
				indices2 = append(indices2, i)
				break
			}
		}
		pos++
	}
	newAggInfos := transformAggInfos(aggInfos, indices, indices2)

	var groupColOrdering sqlbase.ColumnOrdering
	var groupReqOrdering exec.OutputOrdering
	var funcs opt.AggFuncNames
	var private memo.GroupingPrivate

	groupNode, err := b.factory.ConstructGroupBy(res.root, groupingColIdx, groupColOrdering, newAggInfos, groupReqOrdering,
		&funcs, true, &private, true)
	if err != nil {
		return err
	}

	uniqueCols := make(map[opt.ColumnID]struct{})
	var colList opt.ColList
	appendUnique := func(cols []opt.ColumnID) {
		for _, col := range cols {
			if _, exists := uniqueCols[col]; !exists {
				uniqueCols[col] = struct{}{}
				colList = append(colList, col)
			}
		}
	}

	if len(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns) > 0 {
		appendUnique(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupingCols)
		appendUnique(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].OtherFuncColumns)
		b.factory.AddAvgFuncColumns(groupNode, b.mem, tableGroupIndex)
		appendUnique(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].NewAggColumns)

		var outCols util.FastIntMap
		for colIndex, outColID := range colList {
			outCols.Set(int(outColID), colIndex)
		}

		res.root = groupNode
		res.outputCols = outCols
	} else {
		colList = append(colList, b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupingCols...)
		colList = append(colList, b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].OtherFuncColumns...)
		var outCols util.FastIntMap
		for colIndex, outColID := range colList {
			outCols.Set(int(outColID), colIndex)
		}

		res.root = groupNode
		res.outputCols = outCols
	}

	processGroupByExpr(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupByExpr, b.mem, tableGroupIndex)
	if len(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns) > 0 {
		for _, relation := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations {
			if v, ok := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr.(*memo.ProjectExpr); ok {
				if v.Passthrough.Contains(relation[0]) {
					v.Passthrough.Remove(relation[0])
					v.Passthrough.Add(relation[1])
					v.Passthrough.Add(relation[2])
				}
			}
		}
	}
	if v, ok := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].ProjectExpr.(*memo.ProjectExpr); ok {
		for i := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].OtherFuncColumns {
			v.Passthrough.Remove(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AggColumns[i])
		}
		for i := range b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].OtherFuncColumns {
			v.Passthrough.Add(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].OtherFuncColumns[i])
		}
	}
	return nil
}

// applySimpleProject adds a simple projection on top of an existing plan.
func (b *Builder) applySimpleProject(
	input execPlan, cols opt.ColSet, providedOrd opt.Ordering, pushToTS bool,
) (execPlan, error) {
	// We have only pass-through columns.
	colList := make([]exec.ColumnOrdinal, 0, cols.Len())
	var res execPlan
	cols.ForEach(func(i opt.ColumnID) {
		res.outputCols.Set(int(i), len(colList))
		colList = append(colList, input.getColumnOrdinal(i))
	})
	var err error
	res.root, err = b.factory.ConstructSimpleProject(
		input.root, colList, nil /* colNames */, exec.OutputOrdering(res.sqlOrdering(providedOrd)), pushToTS,
	)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildProject(prj *memo.ProjectExpr) (execPlan, error) {
	md := b.mem.Metadata()
	input, err := b.buildRelational(prj.Input)
	if err != nil {
		return execPlan{}, err
	}

	tableID := b.factory.FindTsScanNode(input.root, b.mem)
	tableGroupIndex := b.mem.MultimodelHelper.GetTableIndexFromGroup(tableID)

	projections := prj.Projections
	if len(projections) == 0 {
		// We have only pass-through columns.
		return b.applySimpleProject(input, prj.Passthrough, prj.ProvidedPhysical().Ordering, prj.IsTSEngine())
	}

	var res execPlan
	exprs := make(tree.TypedExprs, 0, len(projections)+prj.Passthrough.Len())
	colNames := make([]string, 0, len(exprs))

	ctx := input.makeBuildScalarCtx()

	for i := range projections {
		if b.mem.QueryType == memo.MultiModel && tableGroupIndex >= 0 &&
			b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut &&
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].HasTimeBucket {
			if functionExpr, ok := projections[i].Element.(*memo.FunctionExpr); ok {
				if functionExpr.FunctionPrivate.Name == "time_bucket" {
					timebucketColID := projections[i].Col
					prj.Passthrough.Add(timebucketColID)
					continue
				}
			}
		}
		item := &projections[i]
		expr, err1 := b.buildScalar(&ctx, item.Element)
		if err1 != nil {
			return execPlan{}, err1
		}
		res.outputCols.Set(int(item.Col), i)
		exprs = append(exprs, expr)
		colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
	}
	prj.Passthrough.ForEach(func(colID opt.ColumnID) {
		res.outputCols.Set(int(colID), len(exprs))
		exprs = append(exprs, b.indexedVar(&ctx, md, colID))
		colNames = append(colNames, md.ColumnMeta(colID).Alias)
	})
	reqOrdering := res.reqOrdering(prj)
	res.root, err = b.factory.ConstructRender(input.root, exprs, colNames, reqOrdering, prj.IsTSEngine())
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildApplyJoin(join memo.RelExpr) (execPlan, error) {
	switch join.Op() {
	case opt.InnerJoinApplyOp, opt.LeftJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
	default:
		return execPlan{}, fmt.Errorf("couldn't execute correlated subquery with op %s", join.Op())
	}
	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)

	if len(memo.WithUses(rightExpr)) != 0 {
		return execPlan{}, fmt.Errorf("references to WITH expressions from correlated subqueries are unsupported")
	}

	leftPlan, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
	}

	// Make a copy of the required props.
	if rightExpr.RequiredPhysical() == nil {
		return execPlan{}, err
	}
	// Make a copy of the required props for the right side.
	rightRequiredProps := *rightExpr.RequiredPhysical()
	// The right-hand side will produce the output columns in order.
	rightRequiredProps.Presentation = b.makePresentation(rightExpr.Relational().OutputCols)
	// leftBoundCols is the set of columns that this apply join binds.
	leftBoundCols := leftExpr.Relational().OutputCols.Intersection(rightExpr.Relational().OuterCols)
	// leftBoundColMap is a map from opt.ColumnID to opt.ColumnOrdinal that maps
	// a column bound by the left side of this apply join to the column ordinal
	// in the left side that contains the binding.
	var leftBoundColMap opt.ColMap
	for col, ok := leftBoundCols.Next(0); ok; col, ok = leftBoundCols.Next(col + 1) {
		v, ok := leftPlan.outputCols.Get(int(col))
		if !ok {
			return execPlan{}, fmt.Errorf("couldn't find binding column %d in left output columns", col)
		}
		leftBoundColMap.Set(int(col), v)
	}

	// We set up an ApplyJoinPlanRightSideFn which plans the
	// right side given a particular left side row. We do this planning in a
	// separate memo, but we use the same exec.Factory.
	var o xform.Optimizer
	planRightSideFn := func(leftRow tree.Datums, listMap *sqlbase.WhiteListMap) (exec.Plan, error) {
		o.Init(b.evalCtx, b.catalog)
		o.Memo().SetWhiteList(listMap)
		f := o.Factory()

		// Copy the right expression into a new memo, replacing each bound column
		// with the corresponding value from the left row.
		var replaceFn norm.ReplaceFunc
		replaceFn = func(e opt.Expr) opt.Expr {
			switch t := e.(type) {
			case *memo.VariableExpr:
				if leftOrd, ok := leftBoundColMap.Get(int(t.Col)); ok {
					return f.ConstructConstVal(leftRow[leftOrd], t.Typ)
				}
			}
			return f.CopyAndReplaceDefault(e, replaceFn)
		}
		f.CopyAndReplace(rightExpr, &rightRequiredProps, replaceFn)

		newRightSide, err := o.Optimize()
		if err != nil {
			return nil, err
		}

		eb := New(b.factory, f.Memo(), b.catalog, newRightSide, b.evalCtx)
		eb.disableTelemetry = true
		plan, err := eb.Build()
		if err != nil {
			if errors.IsAssertionFailure(err) {
				// Enhance the error with the EXPLAIN (OPT, VERBOSE) of the inner
				// expression.
				fmtFlags := memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes
				explainOpt := o.FormatExpr(newRightSide, fmtFlags)
				err = errors.WithDetailf(err, "newRightSide:\n%s", explainOpt)
			}
			return nil, err
		}
		return plan, nil
	}

	// The right plan will always produce the columns in the presentation, in
	// the same order.
	var rightOutputCols opt.ColMap
	for i := range rightRequiredProps.Presentation {
		rightOutputCols.Set(int(rightRequiredProps.Presentation[i].ID), i)
	}
	allCols := joinOutputMap(leftPlan.outputCols, rightOutputCols)

	var onExpr tree.TypedExpr
	if len(*filters) != 0 {
		scalarCtx := buildScalarCtx{
			ivh:     tree.MakeIndexedVarHelper(nil /* container */, numOutputColsInMap(allCols)),
			ivarMap: allCols,
		}
		onExpr, err = b.buildScalar(&scalarCtx, filters)
		if err != nil {
			return execPlan{}, err
		}
	}

	var outputCols opt.ColMap
	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		outputCols = leftPlan.outputCols
	} else {
		outputCols = allCols
	}

	ep := execPlan{outputCols: outputCols}

	ep.root, err = b.factory.ConstructApplyJoin(
		joinType,
		leftPlan.root,
		b.presentationToResultColumns(rightRequiredProps.Presentation),
		onExpr,
		rightExpr,
		planRightSideFn,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// makePresentation creates a Presentation that contains the given columns, in
// order of their IDs.
func (b *Builder) makePresentation(cols opt.ColSet) physical.Presentation {
	md := b.mem.Metadata()
	result := make(physical.Presentation, 0, cols.Len())
	cols.ForEach(func(col opt.ColumnID) {
		result = append(result, opt.AliasedColumn{
			Alias: md.ColumnMeta(col).Alias,
			ID:    col,
		})
	})
	return result
}

// presentationToResultColumns returns ResultColumns corresponding to the
// columns in a presentation.
func (b *Builder) presentationToResultColumns(pres physical.Presentation) sqlbase.ResultColumns {
	md := b.mem.Metadata()
	result := make(sqlbase.ResultColumns, len(pres))
	for i := range pres {
		result[i] = sqlbase.ResultColumn{
			Name: pres[i].Alias,
			Typ:  md.ColumnMeta(pres[i].ID).Type,
		}
	}
	return result
}

func (b *Builder) buildHashJoin(join memo.RelExpr) (execPlan, error) {
	if f := join.Private().(*memo.JoinPrivate).Flags; !f.Has(memo.AllowHashJoinStoreRight) {
		// We need to do a bit of reverse engineering here to determine what the
		// hint was.
		hint := tree.AstLookup
		if f.Has(memo.AllowMergeJoin) {
			hint = tree.AstMerge
		}

		return execPlan{}, errors.Errorf(
			"could not produce a query plan conforming to the %s JOIN hint", hint,
		)
	}

	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftExpr.Relational().OutputCols,
		rightExpr.Relational().OutputCols,
		*filters,
	)
	if !b.disableTelemetry {
		if len(leftEq) > 0 {
			telemetry.Inc(sqltelemetry.JoinAlgoHashUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.JoinAlgoCrossUseCounter)
		}
		telemetry.Inc(opt.JoinTypeToUseCounter(join.Op()))
	}

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		leftExpr,
		rightExpr,
		memo.ExtractRemainingJoinFilters(*filters, leftEq, rightEq),
		joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{outputCols: outputCols}

	// Convert leftEq/rightEq to ordinals.
	eqColsBuf := make([]exec.ColumnOrdinal, 2*len(leftEq))
	leftEqOrdinals := eqColsBuf[:len(leftEq):len(leftEq)]
	rightEqOrdinals := eqColsBuf[len(leftEq):]
	for i := range leftEq {
		leftEqOrdinals[i] = left.getColumnOrdinal(leftEq[i])
		rightEqOrdinals[i] = right.getColumnOrdinal(rightEq[i])
	}

	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ToSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ToSet())

	ep.root, err = b.factory.ConstructHashJoin(
		joinType,
		left.root, right.root,
		leftEqOrdinals, rightEqOrdinals,
		leftEqColsAreKey, rightEqColsAreKey,
		onExpr,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func convertColumnOrdinalToUint32(colList []exec.ColumnOrdinal) []uint32 {
	uint32Slice := make([]uint32, len(colList))
	for i, v := range colList {
		uint32Slice[i] = uint32(v)
	}
	return uint32Slice
}

// buildBatchLookUpJoin build the batchLookUpJoin node for multiple model processing.
func (b *Builder) buildBatchLookUpJoin(join memo.RelExpr) (execPlan, bool, error) {
	var fallBack bool
	if f := join.Private().(*memo.JoinPrivate).Flags; !f.Has(memo.AllowHashJoinStoreRight) {
		// We need to do a bit of reverse engineering here to determine what the
		// hint was.
		hint := tree.AstLookup
		if f.Has(memo.AllowMergeJoin) {
			hint = tree.AstMerge
		}

		return execPlan{}, false, errors.Errorf(
			"could not produce a query plan conforming to the %s JOIN hint", hint,
		)
	}

	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)

	isTSScanExpr := func(expr memo.RelExpr) bool {
		switch e := expr.(type) {
		case *memo.TSScanExpr:
			return true
		case *memo.SelectExpr:
			_, ok := e.Input.(*memo.TSScanExpr)
			return ok
		case *memo.ProjectExpr:
			if _, ok := e.Input.(*memo.TSScanExpr); ok {
				return true
			}
			if selectExpr, ok := e.Input.(*memo.SelectExpr); ok {
				_, ok := selectExpr.Input.(*memo.TSScanExpr)
				return ok
			}
		}
		return false
	}
	if isTSScanExpr(leftExpr) {
		leftExpr, rightExpr = rightExpr, leftExpr
	}

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftExpr.Relational().OutputCols,
		rightExpr.Relational().OutputCols,
		*filters,
	)

	for i := 0; i < len(leftEq); i++ {
		md := b.mem.Metadata()
		if md.ColumnMeta(leftEq[i]).Type.Family() == types.IntFamily ||
			md.ColumnMeta(leftEq[i]).Type.Family() == types.FloatFamily {
			leftLength := md.ColumnMeta(leftEq[i]).Type.InternalType.Width
			rightLength := md.ColumnMeta(rightEq[i]).Type.InternalType.Width
			if leftLength != rightLength {
				b.mem.QueryType = memo.Unset
				b.mem.MultimodelHelper.ResetReasons[memo.JoinColsTypeOrLengthMismatch] = struct{}{}
				return execPlan{}, true, nil
			}
		}
	}

	if !b.disableTelemetry {
		if len(leftEq) > 0 {
			telemetry.Inc(sqltelemetry.JoinAlgoHashUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.JoinAlgoCrossUseCounter)
		}
		telemetry.Inc(opt.JoinTypeToUseCounter(join.Op()))
	}

	leftRowCount := leftExpr.Relational().Stats.RowCount
	var rightRowCount float64
	var tableID opt.TableID
	// Helper function to extract info from TSScanExpr
	extractTSScanInfo := func(expr *memo.TSScanExpr) {
		rightRowCount = expr.Relational().Stats.PTagCount
		tableID = expr.TSScanPrivate.Table
	}

	switch expr := rightExpr.(type) {
	case *memo.TSScanExpr:
		extractTSScanInfo(expr)
	case *memo.SelectExpr:
		if inputExpr, ok := expr.Input.(*memo.TSScanExpr); ok {
			extractTSScanInfo(inputExpr)
		}
	case *memo.ProjectExpr:
		switch input := expr.Input.(type) {
		case *memo.TSScanExpr:
			extractTSScanInfo(input)
		case *memo.SelectExpr:
			if inputExpr, ok := input.Input.(*memo.TSScanExpr); ok {
				extractTSScanInfo(inputExpr)
			}
		}
	}
	if rightRowCount > 0 && leftRowCount > rightRowCount {
		b.mem.MultimodelHelper.HashTagScan = true
	}

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		leftExpr,
		rightExpr,
		memo.ExtractRemainingJoinFilters(*filters, leftEq, rightEq),
		joinType,
	)
	if err != nil || filters.ChildCount() != len(leftEq) {
		fallBack = true
		b.mem.MultimodelHelper.ResetReasons[memo.LeftJoinColsPositionMismatch] = struct{}{}
	}

	tsCols, fb := b.factory.ProcessBljLeftColumns(left.root, b.mem)
	if fb {
		fallBack = true
		b.mem.MultimodelHelper.ResetReasons[memo.UnsupportedDataType] = struct{}{}
	}

	ep := execPlan{outputCols: outputCols}

	// Convert leftEq/rightEq to ordinals.
	eqColsBuf := make([]exec.ColumnOrdinal, 2*len(leftEq))
	leftEqOrdinals := eqColsBuf[:len(leftEq):len(leftEq)]
	rightEqOrdinals := eqColsBuf[len(leftEq):]
	rightEqValue := make([]uint32, len(leftEq))
	var OriginalAccessMode execinfrapb.TSTableReadMode
	if value, ok := b.mem.MultimodelHelper.TableData.Load(tableID); ok {
		tableInfo := value.(memo.TableInfo)
		OriginalAccessMode = tableInfo.OriginalAccessMode
	}
	for i := range leftEq {
		leftEqOrdinals[i] = left.getColumnOrdinal(leftEq[i])
		rightEqOrdinals[i] = right.getColumnOrdinal(rightEq[i])
		rightEqValue[i] = uint32(b.mem.Metadata().GetTagIDByColumnID(rightEq[i])) + 1
		if rightEqValue[i] == 0 {
			b.mem.QueryType = memo.Unset
			b.mem.MultimodelHelper.ResetReasons[memo.UnsupportedCastOnTSColumn] = struct{}{}
			b.factory.ResetTsScanAccessMode(rightExpr, OriginalAccessMode)
			return execPlan{}, true, err
		}
	}

	leftEqValue := convertColumnOrdinalToUint32(leftEqOrdinals)
	success := b.factory.ProcessTsScanNode(right.root, &leftEqValue, &rightEqValue, &tsCols, tableID, rightEq, b.mem, b.evalCtx)
	if !success {
		fallBack = true
		b.mem.MultimodelHelper.ResetReasons[memo.UnsupportedOperation] = struct{}{}
	}

	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ToSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ToSet())

	ep.root, err = b.factory.ConstructBatchLookUpJoin(
		joinType,
		left.root, right.root,
		leftEqOrdinals, rightEqOrdinals,
		leftEqColsAreKey, rightEqColsAreKey,
		onExpr,
	)
	if err != nil {
		return execPlan{}, false, err
	}

	update := b.factory.UpdatePlanColumns(&ep.root)
	if !update {
		fallBack = true
		b.mem.MultimodelHelper.ResetReasons[memo.UnsupportedOperation] = struct{}{}
	}

	if fallBack {
		b.mem.QueryType = memo.Unset
		b.factory.ResetTsScanAccessMode(rightExpr, OriginalAccessMode)
		return execPlan{}, true, err
	}

	return ep, false, nil
}

func (b *Builder) buildMergeJoin(join *memo.MergeJoinExpr) (execPlan, error) {
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoMergeUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))
	}

	joinType := joinOpToJoinType(join.JoinType)

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		join.Left, join.Right, join.On, joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	leftOrd := left.sqlOrdering(join.LeftEq)
	rightOrd := right.sqlOrdering(join.RightEq)
	ep := execPlan{outputCols: outputCols}
	reqOrd := ep.reqOrdering(join)
	leftEqColsAreKey := join.Left.Relational().FuncDeps.ColsAreStrictKey(join.LeftEq.ColSet())
	rightEqColsAreKey := join.Right.Relational().FuncDeps.ColsAreStrictKey(join.RightEq.ColSet())
	ep.root, err = b.factory.ConstructMergeJoin(
		joinType,
		left.root, right.root,
		onExpr,
		leftOrd, rightOrd, reqOrd,
		leftEqColsAreKey, rightEqColsAreKey,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// initJoinBuild builds the inputs to the join as well as the ON expression.
func (b *Builder) initJoinBuild(
	leftChild memo.RelExpr,
	rightChild memo.RelExpr,
	filters memo.FiltersExpr,
	joinType sqlbase.JoinType,
) (leftPlan, rightPlan execPlan, onExpr tree.TypedExpr, outputCols opt.ColMap, _ error) {
	leftPlan, err := b.buildRelational(leftChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}
	rightPlan, err = b.buildRelational(rightChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}

	allCols := joinOutputMap(leftPlan.outputCols, rightPlan.outputCols)

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, numOutputColsInMap(allCols)),
		ivarMap: allCols,
	}

	if len(filters) != 0 {
		onExpr, err = b.buildScalar(&ctx, &filters)
		if err != nil {
			return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
		}
	}

	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		return leftPlan, rightPlan, onExpr, leftPlan.outputCols, nil
	}
	return leftPlan, rightPlan, onExpr, allCols, nil
}

// joinOutputMap determines the outputCols map for a (non-semi/anti) join, given
// the outputCols maps for its inputs.
func joinOutputMap(left, right opt.ColMap) opt.ColMap {
	numLeftCols := numOutputColsInMap(left)

	res := left.Copy()
	right.ForEach(func(colIdx, rightIdx int) {
		res.Set(colIdx, rightIdx+numLeftCols)
	})
	return res
}

func joinOpToJoinType(op opt.Operator) sqlbase.JoinType {
	switch op {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.BatchLookUpJoinOp:
		return sqlbase.InnerJoin

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return sqlbase.LeftOuterJoin

	case opt.RightJoinOp:
		return sqlbase.RightOuterJoin

	case opt.FullJoinOp:
		return sqlbase.FullOuterJoin

	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return sqlbase.LeftSemiJoin

	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return sqlbase.LeftAntiJoin

	default:
		panic(errors.AssertionFailedf("not a join op %s", log.Safe(op)))
	}
}

type funcGetOrdinalCol func(col opt.ColumnID) exec.ColumnOrdinal

// getAggInfos does something  TODO: renyanzheng, add function prologues focusing on when this is needed
func getAggInfos(agg opt.ScalarExpr, i int, f funcGetOrdinalCol, aggInfos *[]exec.AggInfo) error {
	var filterOrd exec.ColumnOrdinal = -1
	if aggFilter, ok := agg.(*memo.AggFilterExpr); ok {
		filter, ok := aggFilter.Filter.(*memo.VariableExpr)
		if !ok {
			return errors.AssertionFailedf("only VariableOp args supported, illegal filter: %s \n", aggFilter.Filter)
		}
		filterOrd = f(filter.Col)
		agg = aggFilter.Input
		agg = aggFilter.Input
	}

	distinct := false
	if aggDistinct, ok := agg.(*memo.AggDistinctExpr); ok {
		distinct = true
		agg = aggDistinct.Input
	}

	name, overload := memo.FindAggregateOverload(agg)

	// Accumulate variable arguments in argCols and constant arguments in
	// constArgs. Constant arguments must follow variable arguments.
	var argCols []exec.ColumnOrdinal
	var constArgs tree.Datums
	for j, n := 0, agg.ChildCount(); j < n; j++ {
		child := agg.Child(j)
		if variable, ok := child.(*memo.VariableExpr); ok {
			if len(constArgs) != 0 {
				return errors.Errorf("constant args must come after variable args, illegal function: %s \n", agg)
			}
			argCols = append(argCols, f(variable.Col))
		} else {
			if len(argCols) == 0 {
				return errors.Errorf("a constant arg requires at least one variable arg, illegal function: %s \n", agg)
			}
			constArgs = append(constArgs, memo.ExtractConstDatum(child))
		}
	}

	(*aggInfos)[i] = exec.AggInfo{
		FuncName:   name,
		Builtin:    overload,
		Distinct:   distinct,
		ResultType: agg.DataType(),
		ArgCols:    argCols,
		ConstArgs:  constArgs,
		Filter:     filterOrd,
	}

	return nil
}

// buildGroupBy builds a plan for a groupNode
func (b *Builder) buildGroupBy(groupBy memo.RelExpr) (execPlan, error) {
	var tsScanNodeID opt.TableID
	tableGroupIndex := -1
	if b.mem.QueryType == memo.MultiModel {
		tsScanNodeID = b.factory.FindTSTableID(groupBy, b.mem)
		tableGroupIndex = b.mem.MultimodelHelper.GetTableIndexFromGroup(tsScanNodeID)
		if tableGroupIndex >= 0 && len(b.mem.MultimodelHelper.PreGroupInfos) >= tableGroupIndex &&
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].BuildingPreGrouping &&
			b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut {
			b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].GroupByExpr = groupBy
		}
	}
	input, err := b.buildGroupByInput(groupBy)
	if err != nil {
		return execPlan{}, err
	}

	// update the input for building groupNode for multiple model processing.
	var blj exec.Node
	if b.mem.QueryType == memo.MultiModel && tableGroupIndex >= 0 &&
		b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.OutsideIn &&
		!b.mem.MultimodelHelper.AggNotPushDown[tableGroupIndex] {
		blj = b.factory.UpdateGroupInput(&input.root)
	}

	var ep execPlan
	private := groupBy.Private().(*memo.GroupingPrivate)

	// case: outside-in use cbo ande agg can execute in ts engine.
	if opt.CheckOptMode(opt.TSQueryOptMode.Get(&b.evalCtx.Settings.SV), opt.OutsideInUseCBO) && private.CanApplyOutsideIn {
		blj = b.factory.UpdateGroupInput(&input.root)
	}

	groupingCols := private.GroupingCols
	funcs := private.Func
	groupingColIdx := make([]exec.ColumnOrdinal, 0, groupingCols.Len())
	var groupOrdering physical.OrderingChoice
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		ep.outputCols.Set(int(i), len(groupingColIdx))                     // add group by col to output
		groupingColIdx = append(groupingColIdx, input.getColumnOrdinal(i)) // record group by cols ordinal column id
		groupOrdering.AppendCol(i, false)
	}
	aggregations := *groupBy.Child(1).(*memo.AggregationsExpr)
	aggInfos := make([]exec.AggInfo, len(aggregations))
	outColIndex := len(groupingColIdx)

	for i := range aggregations {
		if err = getAggInfos(aggregations[i].Agg, i, input.getColumnOrdinal, &aggInfos); err != nil {
			return execPlan{}, err
		}
		ep.outputCols.Set(int(aggregations[i].Col), outColIndex)
		outColIndex++
		// When it comes to ImputationOp,
		// the subsequent plan will extract the aggregation function in the Imputation as the output column,
		// so the output column of the subsequent aggregation function should be skipped by+2.
		// case:
		// select time_bucket_gapfill(time,86400) as c,interpolate(count(device_id), null), interpolate(max(device_id), 1)
		// from t1 group by c order by c;
		// outputCols should be 1 2 4.
		if aggregations[i].Agg.Op() == opt.ImputationOp {
			outColIndex++
		}
	}

	if groupBy.Op() == opt.ScalarGroupByOp {
		ep.root, err = b.factory.ConstructScalarGroupBy(input.root, aggInfos, &funcs, groupBy.IsTSEngine(), private,
			groupBy.GetAddSynchronizer())
	} else {
		var groupingColOrder sqlbase.ColumnOrdering
		if groupBy.RequiredPhysical() != nil {
			if private.TimeBucketGapFillColId > 0 {
				// get ordinal col id from TimeBucketGapFillColId.
				private.TimeBucketGapFillColIdOrdinal = opt.ColumnID(input.getColumnOrdinal(private.TimeBucketGapFillColId))
			}
			if private.GroupWindowId > 0 {
				// we must use order aggregation if sql has group window function, so we must add group column to order column of aggregation.
				if !groupBy.IsTSEngine() {
					private.Ordering = groupOrdering
				}
				private.GroupWindowIdOrdinal = opt.ColumnID(input.getColumnOrdinal(private.GroupWindowId))
				// set ts col to GroupWindowTSColOrdinal
				if v, ok := input.outputCols.Get(memo.TsColID); ok {
					private.GroupWindowTSColOrdinal = opt.ColumnID(v)
				} else {
					private.GroupWindowTSColOrdinal = -1
				}
			}
			groupingColOrder = input.sqlOrdering(ordering.StreamingGroupingColOrdering(
				private, &groupBy.RequiredPhysical().Ordering,
			))
		}

		reqOrdering := ep.reqOrdering(groupBy)
		ep.root, err = b.factory.ConstructGroupBy(input.root, groupingColIdx, groupingColOrder, aggInfos, reqOrdering,
			&funcs, groupBy.IsTSEngine(), private, groupBy.GetAddSynchronizer())
	}
	if err != nil {
		return execPlan{}, err
	}

	// move groupNode below blj node for multiple model processing.
	var updateRoot exec.Node
	if blj != nil {
		updateRoot = b.factory.SetBljRightNode(blj, ep.root)
		ep.root = updateRoot
	}

	if b.mem.QueryType == memo.MultiModel && tableGroupIndex >= 0 && b.mem.MultimodelHelper.PlanMode[tableGroupIndex] == memo.InsideOut {
		avgNums := len(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns)
		if avgNums > 0 {
			ep, err = b.processAvgProjections(ep, input, tableGroupIndex)
			if err != nil {
				return execPlan{}, err
			}
			return ep, nil
		}
	}

	return ep, nil
}

// processAvgProjections constructs a renderNode for handling
// average functions in a multi-model query's inside-out plan mode.
// It transforms each avg function into a corresponding div(sum, count) expression,
// prepares the projection expressions, and updates the execution plan.
func (b *Builder) processAvgProjections(
	ep execPlan, input execPlan, tableGroupIndex int,
) (execPlan, error) {
	var ProjectionsExpr memo.ProjectionsExpr
	avgNums := len(b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns)
	for i := 0; i < avgNums; i++ {
		colID := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgFuncColumns[i]
		colType := b.mem.Metadata().ColumnMeta(colID).Type
		leftColID := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations[i][4]
		rightColID := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations[i][3]
		left := memo.VariableExpr{
			Col:                 leftColID,
			IsConstForLogicPlan: false,
			Typ:                 b.mem.Metadata().ColumnMeta(leftColID).Type,
		}
		right := memo.VariableExpr{
			Col:                 rightColID,
			IsConstForLogicPlan: false,
			Typ:                 b.mem.Metadata().ColumnMeta(rightColID).Type,
		}
		divExpr := memo.DivExpr{
			Left:                &left,
			Right:               &right,
			IsConstForLogicPlan: false,
			Typ:                 colType,
		}

		projectionItem := memo.ProjectionsItem{
			Element:             &divExpr,
			Col:                 colID,
			IsConstForLogicPlan: false,
			Typ:                 colType,
		}
		ProjectionsExpr = append(ProjectionsExpr, projectionItem)
	}
	var passThrough opt.ColSet

	ep.outputCols.ForEach(func(key int, val int) {
		shouldAdd := true

		for i := 0; i < avgNums; i++ {
			colID1 := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations[i][3]
			colID2 := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations[i][4]

			if key == int(colID1) || key == int(colID2) {
				shouldAdd = false
				break
			}
		}

		if shouldAdd {
			passThrough.Add(opt.ColumnID(key))
		}
	})

	prj := memo.ProjectExpr{
		Projections: ProjectionsExpr,
		Passthrough: passThrough,
	}

	relations := b.mem.MultimodelHelper.PreGroupInfos[tableGroupIndex].AvgColumnRelations
	for _, relation := range relations {

		key1 := int(relation[3])
		_, ok := input.outputCols.Get(key1)
		if !ok {
			input.outputCols.Set(key1, input.outputCols.Len())
		}

		key2 := int(relation[4])
		_, ok = input.outputCols.Get(key2)
		if !ok {
			input.outputCols.Set(key2, input.outputCols.Len())
		}
	}

	var res execPlan
	md := b.mem.Metadata()
	projections := prj.Projections
	exprs := make(tree.TypedExprs, 0, len(projections)+prj.Passthrough.Len())
	colNames := make([]string, 0, len(exprs))
	ctx := ep.makeBuildScalarCtx()

	for i := range projections {
		item := &projections[i]
		expr, err1 := b.buildScalar(&ctx, item.Element)
		if err1 != nil {
			return execPlan{}, err1
		}
		res.outputCols.Set(int(item.Col), i)
		exprs = append(exprs, expr)
		colNames = append(colNames, md.ColumnMeta(item.Col).Alias)
	}
	prj.Passthrough.ForEach(func(colID opt.ColumnID) {
		res.outputCols.Set(int(colID), len(exprs))
		exprs = append(exprs, b.indexedVar(&ctx, md, colID))
		colNames = append(colNames, md.ColumnMeta(colID).Alias)
	})
	var reqOrdering exec.OutputOrdering
	var err error
	res.root, err = b.factory.ConstructRender(ep.root, exprs, colNames, reqOrdering, false)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildDistinct(distinct memo.RelExpr) (execPlan, error) {
	private := distinct.Private().(*memo.GroupingPrivate)

	if private.GroupingCols.Empty() {
		// A DistinctOn with no grouping columns should have been converted to a
		// LIMIT 1 or Max1Row by normalization rules.
		return execPlan{}, fmt.Errorf("cannot execute distinct on no columns")
	}
	input, err := b.buildGroupByInput(distinct)
	if err != nil {
		return execPlan{}, err
	}

	distinctCols := input.getColumnOrdinalSet(private.GroupingCols)
	var orderedCols exec.ColumnOrdinalSet
	if distinct.RequiredPhysical() != nil { // add check for prevent memory nil
		ordering := ordering.StreamingGroupingColOrdering(
			private, &distinct.RequiredPhysical().Ordering,
		)
		for i := range ordering {
			orderedCols.Add(int(input.getColumnOrdinal(ordering[i].ID())))
		}
	}

	ep := execPlan{outputCols: input.outputCols}

	// If this is UpsertDistinctOn, then treat NULL values as distinct.
	var nullsAreDistinct bool
	if distinct.Op() == opt.UpsertDistinctOnOp {
		nullsAreDistinct = true
	}

	// If duplicate input rows are not allowed, raise an error at runtime if
	// duplicates are detected.
	var errorOnDup string
	if private.ErrorOnDup {
		errorOnDup = sqlbase.DuplicateUpsertErrText
	}

	reqOrdering := ep.reqOrdering(distinct)
	ep.root, err = b.factory.ConstructDistinct(
		input.root, distinctCols, orderedCols, reqOrdering, nullsAreDistinct, errorOnDup, distinct.IsTSEngine())
	if err != nil {
		return execPlan{}, err
	}

	// buildGroupByInput can add extra sort column(s), so discard those if they
	// are present by using an additional projection.
	outCols := distinct.Relational().OutputCols
	if input.outputCols.Len() == outCols.Len() {
		return ep, nil
	}
	return b.ensureColumns(
		ep, opt.ColSetToList(outCols), nil /* colNames */, distinct.ProvidedPhysical().Ordering,
	)
}

func (b *Builder) buildGroupByInput(groupBy memo.RelExpr) (execPlan, error) {
	groupByInput := groupBy.Child(0).(memo.RelExpr)
	input, err := b.buildRelational(groupByInput)
	if err != nil {
		return execPlan{}, err
	}

	// TODO(radu): this is a one-off fix for an otherwise bigger gap: we should
	// have a more general mechanism (through physical properties or otherwise) to
	// figure out unneeded columns and project them away as necessary. The
	// optimizer doesn't guarantee that it adds ProjectOps everywhere.
	//
	// We address just the GroupBy case for now because there is a particularly
	// important case with COUNT(*) where we can remove all input columns, which
	// leads to significant speedup.
	private := groupBy.Private().(*memo.GroupingPrivate)
	neededCols := private.GroupingCols.Copy()
	aggs := *groupBy.Child(1).(*memo.AggregationsExpr)
	for i := range aggs {
		neededCols.UnionWith(memo.ExtractAggInputColumns(aggs[i].Agg))
	}

	// In rare cases, we might need a column only for its ordering, for example:
	//   SELECT concat_agg(s) FROM (SELECT s FROM kv ORDER BY k)
	// In this case we can't project the column away as it is still needed by
	// distsql to maintain the desired ordering.
	for _, c := range groupByInput.ProvidedPhysical().Ordering {
		neededCols.Add(c.ID())
	}

	if neededCols.Equals(groupByInput.Relational().OutputCols) {
		// All columns produced by the input are used.
		return input, nil
	}
	// The input is producing columns that are not useful; set up a projection.
	cols := make([]exec.ColumnOrdinal, 0, neededCols.Len())
	var newOutputCols opt.ColMap
	for colID, ok := neededCols.Next(0); ok; colID, ok = neededCols.Next(colID + 1) {
		ordinal, ordOk := input.outputCols.Get(int(colID))
		if !ordOk {
			panic(errors.AssertionFailedf("needed column %s not produced by group-by input", b.mem.Metadata().ColumnMeta(colID).Alias))
		}
		newOutputCols.Set(int(colID), len(cols))
		cols = append(cols, exec.ColumnOrdinal(ordinal))
	}

	input.outputCols = newOutputCols
	reqOrdering := input.reqOrdering(groupByInput)
	input.root, err = b.factory.ConstructSimpleProject(
		input.root, cols, nil /* colNames */, reqOrdering, groupByInput.IsTSEngine(),
	)
	if err != nil {
		return execPlan{}, err
	}
	return input, nil
}

func (b *Builder) buildSetOp(set memo.RelExpr) (execPlan, error) {
	leftExpr := set.Child(0).(memo.RelExpr)
	left, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
	}
	rightExpr := set.Child(1).(memo.RelExpr)
	right, err := b.buildRelational(rightExpr)
	if err != nil {
		return execPlan{}, err
	}

	private := set.Private().(*memo.SetPrivate)

	// We need to make sure that the two sides render the columns in the same
	// order; otherwise we add projections.
	//
	// In most cases the projection is needed only to reorder the columns, but not
	// always. For example:
	//  (SELECT a, a, b FROM ab) UNION (SELECT x, y, z FROM xyz)
	// The left input could be just a scan that produces two columns.
	//
	// TODO(radu): we don't have to respect the exact order in the two ColLists;
	// if one side has the right columns but in a different permutation, we could
	// set up a matching projection on the other side. For example:
	//   (SELECT b, c, a FROM abc) UNION (SELECT z, y, x FROM xyz)
	// The expression for this could be a UnionOp on top of two ScanOps (any
	// internal projections could be removed by normalization rules).
	// The scans produce columns `a, b, c` and `x, y, z` respectively. We could
	// leave `b, c, a` as is and project the other side to `x, z, y`.
	// Note that (unless this is part of a larger query) the presentation property
	// will ensure that the columns are presented correctly in the output (i.e. in
	// the order `b, c, a`).
	left, err = b.ensureColumns(
		left, private.LeftCols, nil /* colNames */, leftExpr.ProvidedPhysical().Ordering,
	)
	if err != nil {
		return execPlan{}, err
	}
	right, err = b.ensureColumns(
		right, private.RightCols, nil /* colNames */, rightExpr.ProvidedPhysical().Ordering,
	)
	if err != nil {
		return execPlan{}, err
	}

	var typ tree.UnionType
	var all bool
	switch set.Op() {
	case opt.UnionOp:
		typ, all = tree.UnionOp, false
	case opt.UnionAllOp:
		typ, all = tree.UnionOp, true
	case opt.IntersectOp:
		typ, all = tree.IntersectOp, false
	case opt.IntersectAllOp:
		typ, all = tree.IntersectOp, true
	case opt.ExceptOp:
		typ, all = tree.ExceptOp, false
	case opt.ExceptAllOp:
		typ, all = tree.ExceptOp, true
	default:
		panic(errors.AssertionFailedf("invalid operator %s", log.Safe(set.Op())))
	}

	node, err := b.factory.ConstructSetOp(typ, all, left.root, right.root)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range private.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(e memo.RelExpr) (execPlan, bool, error) {
	input, err := b.buildRelational(e.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, true, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, e.Child(1).(opt.ScalarExpr))
	if err != nil {
		return execPlan{}, true, err
	}
	var node exec.Node
	add := true
	if e.Op() == opt.LimitOp {
		// limit add synchronizer need add  limit-synchronizer-limit
		if e.GetAddSynchronizer() {
			node, err = b.factory.ConstructLimit(input.root, expr, nil, e, b.mem.Metadata())
			if err != nil {
				return execPlan{}, true, err
			}
			// add synchronizer
			res := b.buildSynchronizer(execPlan{root: node, outputCols: input.outputCols})
			add = false
			node, err = b.factory.ConstructLimit(res.root, expr, nil, e, b.mem.Metadata())
		} else {
			node, err = b.factory.ConstructLimit(input.root, expr, nil, e, b.mem.Metadata())
		}
	} else {
		node, err = b.factory.ConstructLimit(input.root, nil, expr, e, b.mem.Metadata())
	}
	if err != nil {
		return execPlan{}, add, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, add, nil
}

func (b *Builder) buildSort(sort *memo.SortExpr) (execPlan, error) {
	input, err := b.buildRelational(sort.Input)
	if err != nil {
		return execPlan{}, err
	}

	ordering := sort.ProvidedPhysical().Ordering
	inputOrdering := sort.Input.ProvidedPhysical().Ordering
	alreadyOrderedPrefix := 0
	for i := range inputOrdering {
		if inputOrdering[i] != ordering[i] {
			break
		}
		alreadyOrderedPrefix = i + 1
	}

	if alreadyOrderedPrefix == len(ordering) {
		return input, nil
	}

	node, err := b.factory.ConstructSort(input.root, input.sqlOrdering(ordering), alreadyOrderedPrefix, sort.IsTSEngine())
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildOrdinality(ord *memo.OrdinalityExpr) (execPlan, error) {
	input, err := b.buildRelational(ord.Input)
	if err != nil {
		return execPlan{}, err
	}

	colName := b.mem.Metadata().ColumnMeta(ord.ColID).Alias

	node, err := b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	outputCols := input.outputCols.Copy()
	outputCols.Set(int(ord.ColID), outputCols.Len())

	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildIndexJoin(join *memo.IndexJoinExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()
	tab := md.Table(join.Table)

	// TODO(radu): the distsql implementation of index join assumes that the input
	// starts with the PK columns in order (#40749).
	pri := tab.Index(cat.PrimaryIndex)
	keyCols := make([]exec.ColumnOrdinal, pri.KeyColumnCount())
	for i := range keyCols {
		keyCols[i] = input.getColumnOrdinal(join.Table.ColumnID(pri.Column(i).Ordinal))
	}

	cols := join.Cols
	needed, output := b.getColumns(cols, join.Table)
	res := execPlan{outputCols: output}
	res.root, err = b.factory.ConstructIndexJoin(
		input.root, tab, keyCols, needed, res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildLookupJoin(join *memo.LookupJoinExpr) (execPlan, error) {
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoLookupUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))
	}

	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()

	keyCols := make([]exec.ColumnOrdinal, len(join.KeyCols))
	for i, c := range join.KeyCols {
		keyCols[i] = input.getColumnOrdinal(c)
	}

	inputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(inputCols)

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)
	allCols := joinOutputMap(input.outputCols, lookupColMap)

	res := execPlan{outputCols: allCols}
	if join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp {
		// For semi and anti join, only the left columns are output.
		res.outputCols = input.outputCols
	}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allCols.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)
	var eqCols opt.ColSet
	for i := range join.KeyCols {
		eqCols.Add(join.Table.ColumnID(idx.Column(i).Ordinal))
	}

	res.root, err = b.factory.ConstructLookupJoin(
		joinOpToJoinType(join.JoinType),
		input.root,
		tab,
		idx,
		keyCols,
		join.LookupColsAreTableKey,
		lookupOrdinals,
		onExpr,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	// Apply a post-projection if Cols doesn't contain all input columns.
	if !inputCols.SubsetOf(join.Cols) {
		return b.applySimpleProject(res, join.Cols, join.ProvidedPhysical().Ordering, false)
	}
	return res, nil
}

func (b *Builder) buildZigzagJoin(join *memo.ZigzagJoinExpr) (execPlan, error) {
	md := b.mem.Metadata()

	leftTable := md.Table(join.LeftTable)
	rightTable := md.Table(join.RightTable)
	leftIndex := leftTable.Index(join.LeftIndex)
	rightIndex := rightTable.Index(join.RightIndex)

	leftEqCols := make([]exec.ColumnOrdinal, len(join.LeftEqCols))
	rightEqCols := make([]exec.ColumnOrdinal, len(join.RightEqCols))
	for i := range join.LeftEqCols {
		leftEqCols[i] = exec.ColumnOrdinal(join.LeftTable.ColumnOrdinal(join.LeftEqCols[i]))
		rightEqCols[i] = exec.ColumnOrdinal(join.RightTable.ColumnOrdinal(join.RightEqCols[i]))
	}
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex).Intersection(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex).Intersection(join.Cols)
	// Remove duplicate columns, if any.
	rightCols.DifferenceWith(leftCols)

	leftOrdinals, leftColMap := b.getColumns(leftCols, join.LeftTable)
	rightOrdinals, rightColMap := b.getColumns(rightCols, join.RightTable)

	allCols := joinOutputMap(leftColMap, rightColMap)

	res := execPlan{outputCols: allCols}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, leftColMap.Len()+rightColMap.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	// Build the fixed value scalars. These are represented as one value node
	// per side of the join, containing one row/tuple with fixed values for
	// a prefix of that index's columns.
	fixedVals := make([]exec.Node, 2)
	fixedCols := []opt.ColList{join.LeftFixedCols, join.RightFixedCols}
	for i := range join.FixedVals {
		tup := join.FixedVals[i].(*memo.TupleExpr)
		valExprs := make([]tree.TypedExpr, len(tup.Elems))
		for j := range tup.Elems {
			valExprs[j], err = b.buildScalar(&ctx, tup.Elems[j])
			if err != nil {
				return execPlan{}, err
			}
		}
		valuesPlan, err1 := b.constructValues([][]tree.TypedExpr{valExprs}, fixedCols[i])
		if err1 != nil {
			return execPlan{}, err1
		}
		fixedVals[i] = valuesPlan.root
	}

	res.root, err = b.factory.ConstructZigzagJoin(
		leftTable,
		leftIndex,
		rightTable,
		rightIndex,
		leftEqCols,
		rightEqCols,
		leftOrdinals,
		rightOrdinals,
		onExpr,
		fixedVals,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildMax1Row(max1Row *memo.Max1RowExpr) (execPlan, error) {
	input, err := b.buildRelational(max1Row.Input)
	if err != nil {
		return execPlan{}, err
	}

	node, err := b.factory.ConstructMax1Row(input.root, max1Row.ErrorText)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildWith(with *memo.WithExpr) (execPlan, error) {
	value, err := b.buildRelational(with.Binding)
	if err != nil {
		return execPlan{}, err
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", with.ID)
	if with.Name != "" {
		fmt.Fprintf(&label, " (%s)", with.Name)
	}

	buffer, err := b.factory.ConstructBuffer(value.root, label.String())
	if err != nil {
		return execPlan{}, err
	}

	// TODO(justin): if the binding here has a spoolNode at its root, we can
	// remove it, since subquery execution also guarantees complete execution.

	// Add the buffer as a subquery so it gets executed ahead of time, and is
	// available to be referenced by other queries.
	b.subqueries = append(b.subqueries, exec.Subquery{
		ExprNode: with.OriginalExpr,
		// TODO(justin): this is wasteful: both the subquery and the bufferNode
		// will buffer up all the results.  This should be fixed by either making
		// the buffer point directly to the subquery results or adding a new
		// subquery mode that reads and discards all rows. This could possibly also
		// be fixed by ensuring that bufferNode exhausts its input (and forcing it
		// to behave like a spoolNode) and using the EXISTS mode.
		Mode: exec.SubqueryAllRows,
		Root: buffer,
	})

	b.addBuiltWithExpr(with.ID, value.outputCols, buffer)

	return b.buildRelational(with.Main)
}

func (b *Builder) buildRecursiveCTE(rec *memo.RecursiveCTEExpr) (execPlan, error) {
	initial, err := b.buildRelational(rec.Initial)
	if err != nil {
		return execPlan{}, err
	}

	// Make sure we have the columns in the correct order.
	initial, err = b.ensureColumns(initial, rec.InitialCols, nil /* colNames */, nil /* ordering */)
	if err != nil {
		return execPlan{}, err
	}

	// Renumber the columns so they match the columns expected by the recursive
	// query.
	initial.outputCols = util.FastIntMap{}
	for i, col := range rec.OutCols {
		initial.outputCols.Set(int(col), i)
	}

	// To implement exec.RecursiveCTEIterationFn, we create a special Builder.

	innerBldTemplate := &Builder{
		factory: b.factory,
		mem:     b.mem,
		catalog: b.catalog,
		evalCtx: b.evalCtx,
		// If the recursive query itself contains CTEs, building it in the function
		// below will add to withExprs. Cap the slice to force reallocation on any
		// appends, so that they don't overwrite overwrite later appends by our
		// original builder.
		withExprs: b.withExprs[:len(b.withExprs):len(b.withExprs)],
	}

	fn := func(bufferRef exec.Node) (exec.Plan, error) {
		// Use a separate builder each time.
		var err1 error
		innerBld := *innerBldTemplate
		innerBld.addBuiltWithExpr(rec.WithID, initial.outputCols, bufferRef)
		var plan execPlan
		plan, err1 = innerBld.build(rec.Recursive)
		if err1 != nil {
			return nil, err1
		}
		var err2 error
		// Ensure columns are output in the same order.
		plan, err2 = innerBld.ensureColumns(
			plan, rec.RecursiveCols, nil /* colNames */, nil, /* ordering */
		)
		if err2 != nil {
			return nil, err2
		}
		return innerBld.factory.ConstructPlan(plan.root, innerBld.subqueries, innerBld.postqueries)
	}

	label := fmt.Sprintf("working buffer (%s)", rec.Name)
	var ep execPlan
	ep.root, err = b.factory.ConstructRecursiveCTE(initial.root, fn, label)
	if err != nil {
		return execPlan{}, err
	}
	for i, col := range rec.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

func (b *Builder) buildWithScan(withScan *memo.WithScanExpr) (execPlan, error) {
	withID := withScan.With
	var e *builtWithExpr
	for i := range b.withExprs {
		if b.withExprs[i].id == withID {
			e = &b.withExprs[i]
			break
		}
	}
	if e == nil {
		return execPlan{}, errors.AssertionFailedf(
			"couldn't find With expression with ID %d", withScan.With,
		)
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", withScan.With)
	if withScan.Name != "" {
		fmt.Fprintf(&label, " (%s)", withScan.Name)
	}

	node, err := b.factory.ConstructScanBuffer(e.bufferNode, label.String())
	if err != nil {
		return execPlan{}, err
	}
	res := execPlan{root: node}

	if maxVal, _ := e.outputCols.MaxValue(); len(withScan.InCols) == maxVal+1 {
		// We are outputting all columns. Just set up the map.

		// The ColumnIDs from the With expression need to get remapped according to
		// the mapping in the withScan to get the actual colMap for this expression.
		for i := range withScan.InCols {
			idx, _ := e.outputCols.Get(int(withScan.InCols[i]))
			res.outputCols.Set(int(withScan.OutCols[i]), idx)
		}
	} else {
		// We need a projection.
		cols := make([]exec.ColumnOrdinal, len(withScan.InCols))
		for i := range withScan.InCols {
			col, ok := e.outputCols.Get(int(withScan.InCols[i]))
			if !ok {
				panic(errors.AssertionFailedf("column %d not in input", log.Safe(withScan.InCols[i])))
			}
			cols[i] = exec.ColumnOrdinal(col)
			res.outputCols.Set(int(withScan.OutCols[i]), i)
		}
		res.root, err = b.factory.ConstructSimpleProject(
			res.root, cols, nil, /* colNames */
			exec.OutputOrdering(res.sqlOrdering(withScan.ProvidedPhysical().Ordering)),
			withScan.IsTSEngine(),
		)
		if err != nil {
			return execPlan{}, err
		}
	}
	return res, nil

}

func (b *Builder) buildProjectSet(projectSet *memo.ProjectSetExpr) (execPlan, error) {
	input, err := b.buildRelational(projectSet.Input)
	if err != nil {
		return execPlan{}, err
	}

	zip := projectSet.Zip
	md := b.mem.Metadata()
	scalarCtx := input.makeBuildScalarCtx()

	exprs := make(tree.TypedExprs, len(zip))
	zipCols := make(sqlbase.ResultColumns, 0, len(zip))
	numColsPerGen := make([]int, len(zip))

	ep := execPlan{outputCols: input.outputCols}
	n := ep.numOutputCols()

	for i := range zip {
		item := &zip[i]
		exprs[i], err = b.buildScalar(&scalarCtx, item.Fn)
		if err != nil {
			return execPlan{}, err
		}

		for _, col := range item.Cols {
			colMeta := md.ColumnMeta(col)
			zipCols = append(zipCols, sqlbase.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})

			ep.outputCols.Set(int(col), n)
			n++
		}

		numColsPerGen[i] = len(item.Cols)
	}

	ep.root, err = b.factory.ConstructProjectSet(input.root, exprs, zipCols, numColsPerGen)
	if err != nil {
		return execPlan{}, err
	}

	return ep, nil
}

func (b *Builder) resultColumn(id opt.ColumnID) sqlbase.ResultColumn {
	colMeta := b.mem.Metadata().ColumnMeta(id)
	return sqlbase.ResultColumn{
		Name: colMeta.Alias,
		Typ:  colMeta.Type,
	}
}

// extractFromOffset extracts the start bound expression of a window function
// that uses the OFFSET windowing mode for its start bound.
func (b *Builder) extractFromOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowFromOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractFromOffset(e.Child(0).(opt.ScalarExpr))
}

// extractToOffset extracts the end bound expression of a window function
// that uses the OFFSET windowing mode for its end bound.
func (b *Builder) extractToOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowToOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractToOffset(e.Child(0).(opt.ScalarExpr))
}

// extractFilter extracts a FILTER expression from a window function tower.
// Returns the expression and true if there was a filter, and false otherwise.
func (b *Builder) extractFilter(e opt.ScalarExpr) (opt.ScalarExpr, bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if filter, ok := e.(*memo.AggFilterExpr); ok {
		return filter.Filter, true
	}
	return b.extractFilter(e.Child(0).(opt.ScalarExpr))
}

// extractWindowFunction extracts the window function being computed from a
// potential tower of modifiers attached to the Function field of a
// WindowsItem.
func (b *Builder) extractWindowFunction(e opt.ScalarExpr) opt.ScalarExpr {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return e
	}
	return b.extractWindowFunction(e.Child(0).(opt.ScalarExpr))
}

func (b *Builder) isOffsetMode(boundType tree.WindowFrameBoundType) bool {
	return boundType == tree.OffsetPreceding || boundType == tree.OffsetFollowing
}

func (b *Builder) buildFrame(input execPlan, w *memo.WindowsItem) (*tree.WindowFrame, error) {
	scalarCtx := input.makeBuildScalarCtx()
	newDef := &tree.WindowFrame{
		Mode: w.Frame.Mode,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{
				BoundType: w.Frame.StartBoundType,
			},
			EndBound: &tree.WindowFrameBound{
				BoundType: w.Frame.EndBoundType,
			},
		},
		Exclusion: w.Frame.FrameExclusion,
	}
	if boundExpr, ok := b.extractFromOffset(w.Function); ok {
		if !b.isOffsetMode(w.Frame.StartBoundType) {
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		if offset == tree.DNull {
			return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "frame starting offset must not be null")
		}
		newDef.Bounds.StartBound.OffsetExpr = offset
	}

	if boundExpr, ok := b.extractToOffset(w.Function); ok {
		if !b.isOffsetMode(newDef.Bounds.EndBound.BoundType) {
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		if offset == tree.DNull {
			return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "frame ending offset must not be null")
		}
		newDef.Bounds.EndBound.OffsetExpr = offset
	}
	return newDef, nil
}

func (b *Builder) buildWindow(w *memo.WindowExpr) (execPlan, error) {
	input, err := b.buildRelational(w.Input)
	if err != nil {
		return execPlan{}, err
	}

	// Rearrange the input so that the input has all the passthrough columns
	// followed by all the argument columns.

	passthrough := w.Input.Relational().OutputCols

	desiredCols := opt.ColList{}
	passthrough.ForEach(func(i opt.ColumnID) {
		desiredCols = append(desiredCols, i)
	})

	// TODO(justin): this call to ensureColumns is kind of unfortunate because it
	// can result in an extra render beneath each window function. Figure out a
	// way to alleviate this.
	input, err = b.ensureColumns(input, desiredCols, nil, opt.Ordering{})
	if err != nil {
		return execPlan{}, err
	}

	ctx := input.makeBuildScalarCtx()

	ord := w.Ordering.ToOrdering()

	orderingExprs := make(tree.OrderBy, len(ord))
	for i, c := range ord {
		direction := tree.Ascending
		if c.Descending() {
			direction = tree.Descending
		}
		orderingExprs[i] = &tree.Order{
			Expr:      b.indexedVar(&ctx, b.mem.Metadata(), c.ID()),
			Direction: direction,
		}
	}

	partitionIdxs := make([]exec.ColumnOrdinal, w.Partition.Len())
	partitionExprs := make(tree.Exprs, w.Partition.Len())

	i := 0
	w.Partition.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		partitionIdxs[i] = exec.ColumnOrdinal(ordinal)
		partitionExprs[i] = b.indexedVar(&ctx, b.mem.Metadata(), col)
		i++
	})

	argIdxs := make([][]exec.ColumnOrdinal, len(w.Windows))
	filterIdxs := make([]int, len(w.Windows))
	exprs := make([]*tree.FuncExpr, len(w.Windows))

	for i := range w.Windows {
		var err1 error
		item := &w.Windows[i]
		fn := b.extractWindowFunction(item.Function)
		name, overload := memo.FindWindowOverload(fn)
		if !b.disableTelemetry {
			telemetry.Inc(sqltelemetry.WindowFunctionCounter(name))
		}
		props, _ := builtins.GetBuiltinProperties(name)

		args := make([]tree.TypedExpr, fn.ChildCount())
		argIdxs[i] = make([]exec.ColumnOrdinal, fn.ChildCount())
		for j, n := 0, fn.ChildCount(); j < n; j++ {
			col := fn.Child(j).(*memo.VariableExpr).Col
			args[j] = b.indexedVar(&ctx, b.mem.Metadata(), col)
			idx, _ := input.outputCols.Get(int(col))
			argIdxs[i][j] = exec.ColumnOrdinal(idx)
		}
		var frame *tree.WindowFrame
		frame, err1 = b.buildFrame(input, item)
		if err1 != nil {
			return execPlan{}, err1
		}

		var builtFilter tree.TypedExpr
		filter, ok := b.extractFilter(item.Function)
		if ok {
			f, ok := filter.(*memo.VariableExpr)
			if !ok {
				panic(errors.AssertionFailedf("expected FILTER expression to be a VariableExpr in window"))
			}
			filterIdxs[i], _ = input.outputCols.Get(int(f.Col))

			builtFilter, err1 = b.buildScalar(&ctx, filter)
			if err1 != nil {
				return execPlan{}, err1
			}
		} else {
			filterIdxs[i] = -1
		}

		exprs[i] = tree.NewTypedFuncExpr(
			tree.WrapFunction(name),
			0,
			args,
			builtFilter,
			&tree.WindowDef{
				Partitions: partitionExprs,
				OrderBy:    orderingExprs,
				Frame:      frame,
			},
			overload.FixedReturnType(),
			props,
			overload,
		)
	}

	resultCols := make(sqlbase.ResultColumns, w.Relational().OutputCols.Len())

	// All the passthrough cols will keep their ordinal index.
	passthrough.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		resultCols[ordinal] = b.resultColumn(col)
	})

	var outputCols opt.ColMap
	input.outputCols.ForEach(func(key, val int) {
		if passthrough.Contains(opt.ColumnID(key)) {
			outputCols.Set(key, val)
		}
	})

	outputIdxs := make([]int, len(w.Windows))

	// Because of the way we arranged the input columns, we will be outputting
	// the window columns at the end (which is exactly what the execution engine
	// will do as well).
	windowStart := passthrough.Len()
	for i := range w.Windows {
		resultCols[windowStart+i] = b.resultColumn(w.Windows[i].Col)
		outputCols.Set(int(w.Windows[i].Col), windowStart+i)
		outputIdxs[i] = windowStart + i
	}

	node, err := b.factory.ConstructWindow(input.root, exec.WindowInfo{
		Cols:       resultCols,
		Exprs:      exprs,
		OutputIdxs: outputIdxs,
		ArgIdxs:    argIdxs,
		FilterIdxs: filterIdxs,
		Partition:  partitionIdxs,
		Ordering:   input.sqlOrdering(ord),
	}, w.IsTSEngine())
	if err != nil {
		return execPlan{}, err
	}

	return execPlan{
		root:       node,
		outputCols: outputCols,
	}, nil
}

func (b *Builder) buildSequenceSelect(seqSel *memo.SequenceSelectExpr) (execPlan, error) {
	seq := b.mem.Metadata().Sequence(seqSel.Sequence)
	node, err := b.factory.ConstructSequenceSelect(seq)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range seqSel.Cols {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

func (b *Builder) applySaveTable(
	input execPlan, e memo.RelExpr, saveTableName string,
) (execPlan, error) {
	name := tree.NewTableName(opt.SaveTablesDatabase, tree.Name(saveTableName))

	// Ensure that the column names are unique and match the names used by the
	// opttester.
	outputCols := e.Relational().OutputCols
	colNames := make([]string, outputCols.Len())
	colNameGen := memo.NewColumnNameGenerator(e)
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		ord, _ := input.outputCols.Get(int(col))
		colNames[ord] = colNameGen.GenerateName(col)
	}

	var err error
	input.root, err = b.factory.ConstructSaveTable(input.root, name, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return input, err
}

func (b *Builder) buildOpaque(opaque *memo.OpaqueRelPrivate) (execPlan, error) {
	node, err := b.factory.ConstructOpaque(opaque.Metadata)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range opaque.Columns {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

// needProjection figures out what projection is needed on top of the input plan
// to produce the given list of columns. If the input plan already produces
// the columns (in the same order), returns needProj=false.
func (b *Builder) needProjection(
	input execPlan, colList opt.ColList,
) (_ []exec.ColumnOrdinal, needProj bool) {
	if input.numOutputCols() == len(colList) {
		identity := true
		for i, col := range colList {
			if ord, ok := input.outputCols.Get(int(col)); !ok || ord != i {
				identity = false
				break
			}
		}
		if identity {
			return nil, false
		}
	}
	cols := make([]exec.ColumnOrdinal, 0, len(colList))
	for _, col := range colList {
		if col != 0 {
			cols = append(cols, input.getColumnOrdinal(col))
		}
	}
	return cols, true
}

// ensureColumns applies a projection as necessary to make the output match the
// given list of columns; colNames is optional.
func (b *Builder) ensureColumns(
	input execPlan, colList opt.ColList, colNames []string, provided opt.Ordering,
) (execPlan, error) {
	cols, needProj := b.needProjection(input, colList)
	if !needProj {
		// No projection necessary.
		if colNames != nil {
			var err error
			input.root, err = b.factory.RenameColumns(input.root, colNames)
			if err != nil {
				return execPlan{}, err
			}
		}
		return input, nil
	}
	var res execPlan
	for i, col := range colList {
		res.outputCols.Set(int(col), i)
	}
	reqOrdering := exec.OutputOrdering(res.sqlOrdering(provided))
	var err error
	res.root, err = b.factory.ConstructSimpleProject(input.root, cols, colNames, reqOrdering,
		input.execEngine == tree.EngineTypeTimeseries)
	return res, err
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(input execPlan, p *physical.Required) (execPlan, error) {
	pres := p.Presentation
	colList := make(opt.ColList, len(pres))
	colNames := make([]string, len(pres))
	for i := range pres {
		colList[i] = pres[i].ID
		colNames[i] = pres[i].Alias
	}
	// The ordering is not useful for a top-level projection (it is used by the
	// distsql planner for internal nodes); we might not even be able to represent
	// it because it can refer to columns not in the presentation.
	return b.ensureColumns(input, colList, colNames, nil /* provided */)
}

// getEnvData consolidates the information that must be presented in
// EXPLAIN (opt, env).
func (b *Builder) getEnvData() exec.ExplainEnvData {
	envOpts := exec.ExplainEnvData{ShowEnv: true}
	var err error
	envOpts.Tables, envOpts.Sequences, envOpts.Views, err = b.mem.Metadata().AllDataSourceNames(
		func(ds cat.DataSource) (cat.DataSourceName, error) {
			return b.catalog.FullyQualifiedName(context.TODO(), ds)
		},
	)
	if err != nil {
		panic(err)
	}

	return envOpts
}

// statementTag returns a string that can be used in an error message regarding
// the given expression.
func (b *Builder) statementTag(expr memo.RelExpr) string {
	switch expr.Op() {
	case opt.OpaqueRelOp, opt.OpaqueMutationOp, opt.OpaqueDDLOp:
		return expr.Private().(*memo.OpaqueRelPrivate).Metadata.String()

	default:
		return expr.Op().SyntaxTag()
	}
}
