// Copyright 2016 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type observeVerbosity int

const (
	observeMetadata observeVerbosity = iota
	observeAlways
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver struct {
	// replaceNode is invoked upon entering a tree node. It can replace the
	// current planNode in the tree by returning a non-nil planNode. Returning
	// nil will continue the recursion and not modify the current node.
	replaceNode func(ctx context.Context, nodeName string, plan planNode) (planNode, error)

	// enterNode is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	enterNode func(ctx context.Context, nodeName string, plan planNode) (bool, error)

	// expr is invoked for each expression field in each node.
	expr func(verbosity observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr)

	// spans is invoked for spans embedded in each node. hardLimitSet indicates
	// whether the node will "touch" a limited number of rows.
	spans func(nodeName, fieldName string, index *sqlbase.IndexDescriptor, spans []roachpb.Span, hardLimitSet bool)

	// attr is invoked for non-expression metadata in each node.
	attr func(nodeName, fieldName, attr string)

	addWarningMessage func(nodeName, fieldName, attr string)

	// leaveNode is invoked upon leaving a tree node.
	leaveNode func(nodeName string, plan planNode) error

	// followRowSourceToPlanNode controls whether the tree walker continues
	// walking when it encounters a rowSourceToPlanNode, which indicates that the
	// logical plan has been mutated for distribution. This should normally be
	// set to false, as normally the planNodeToRowSource on the other end will
	// take care of propagating signals via its own walker.
	followRowSourceToPlanNode bool
}

// walkPlan performs a depth-first traversal of the plan given as
// argument, informing the planObserver of the node details at each
// level.
func walkPlan(ctx context.Context, plan planNode, observer planObserver) error {
	v := makePlanVisitor(ctx, observer)
	v.visit(plan)
	return v.err
}

// planVisitor is the support structure for walkPlan().
type planVisitor struct {
	observer planObserver
	ctx      context.Context
	err      error
}

// makePlanVisitor creates a planVisitor instance.
// ctx will be stored in the planVisitor and used when visiting planNode's and
// expressions..
func makePlanVisitor(ctx context.Context, observer planObserver) planVisitor {
	return planVisitor{observer: observer, ctx: ctx}
}

// visit is the recursive function that supports walkPlan().
func (v *planVisitor) visit(plan planNode) planNode {
	if v.err != nil {
		return plan
	}

	name := nodeName(plan)

	if v.observer.replaceNode != nil {
		newNode, err := v.observer.replaceNode(v.ctx, name, plan)
		if err != nil {
			v.err = err
			return plan
		}
		if newNode != nil {
			return newNode
		}
	}
	v.visitInternal(plan, name)
	return plan
}

// visitConcrete is like visit, but provided for the case where a planNode is
// trying to recurse into a concrete planNode type, and not a planNode
// interface.
func (v *planVisitor) visitConcrete(plan planNode) {
	if v.err != nil {
		return
	}

	name := nodeName(plan)
	v.visitInternal(plan, name)
}

const (
	engineType = "engine type"
	ts         = "time series"
)

func (v *planVisitor) visitInternal(plan planNode, name string) {
	if v.err != nil {
		return
	}
	recurse := true

	if v.observer.enterNode != nil {
		recurse, v.err = v.observer.enterNode(v.ctx, name, plan)
		if v.err != nil {
			return
		}
	}
	if v.observer.leaveNode != nil {
		defer func() {
			if v.err != nil {
				return
			}
			v.err = v.observer.leaveNode(name, plan)
		}()
	}

	if !recurse {
		return
	}

	switch n := plan.(type) {
	case *valuesNode:
		if v.observer.attr != nil {
			numRows := len(n.tuples)
			if n.rows != nil {
				numRows = n.rows.Len()
			}
			v.observer.attr(name, "size", formatValuesSize(numRows, len(n.columns)))
		}

		if v.observer.expr != nil {
			v.metadataTuples(name, n.tuples)
		}

	case *synchronizerNode:
		n.plan = v.visit(n.plan)

	case *scanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", fmt.Sprintf("%s@%s", n.desc.Name, n.index.Name))
			if n.noIndexJoin {
				v.observer.attr(name, "hint", "no index join")
			}
			if n.specifiedIndex != nil {
				v.observer.attr(name, "hint", fmt.Sprintf("force index @%s", n.specifiedIndex.Name))
			}
		}
		if v.observer.spans != nil {
			v.observer.spans(name, "spans", n.index, n.spans, n.hardLimit != 0)
		}
		if v.observer.attr != nil {
			// Only print out "parallel" when it makes sense. i.e. don't print if
			// we know we will get only one result from the scan. There are cases
			// in which "parallel" will be printed out even though the spans cover
			// a single range, but there is nothing we can do about that.
			if n.canParallelize() && (len(n.spans) > 1 || n.maxResults > 1) {
				v.observer.attr(name, "parallel", "")
			}
			if n.hardLimit > 0 && isFilterTrue(n.filter) {
				v.observer.attr(name, "limit", fmt.Sprintf("%d", n.hardLimit))
			}
			if n.lockingStrength != sqlbase.ScanLockingStrength_FOR_NONE {
				strength := ""
				switch n.lockingStrength {
				case sqlbase.ScanLockingStrength_FOR_KEY_SHARE:
					strength = "for key share"
				case sqlbase.ScanLockingStrength_FOR_SHARE:
					strength = "for share"
				case sqlbase.ScanLockingStrength_FOR_NO_KEY_UPDATE:
					strength = "for no key update"
				case sqlbase.ScanLockingStrength_FOR_UPDATE:
					strength = "for update"
				default:
					panic(errors.AssertionFailedf("unexpected strength"))
				}
				v.observer.attr(name, "locking strength", strength)
			}
			if n.lockingWaitPolicy != sqlbase.ScanLockingWaitPolicy_BLOCK {
				wait := ""
				switch n.lockingWaitPolicy {
				case sqlbase.ScanLockingWaitPolicy_SKIP:
					wait = "skip locked"
				case sqlbase.ScanLockingWaitPolicy_ERROR:
					wait = "nowait"
				default:
					panic(errors.AssertionFailedf("unexpected wait policy"))
				}
				v.observer.attr(name, "locking wait policy", wait)
			}
		}
		if v.observer.expr != nil {
			v.expr(name, "filter", -1, n.filter)
		}

	case *tsScanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "ts-table", fmt.Sprintf("%s", n.Table.Name()))
			if n.orderedType == opt.OrderedScan || n.orderedType == opt.ForceOrderedScan {
				v.observer.attr(name, "ordered type", "order scan")
			} else if n.orderedType == opt.SortAfterScan {
				v.observer.attr(name, "ordered type", "sort after scan")
			}
			if n.HintType.OnlyTag() {
				v.observer.attr(name, "only tag", "true")
			} else if n.HintType.LastRowOpt() {
				v.observer.attr(name, "hint", "last row optimize")
			}
			v.observer.attr(name, "access mode", n.AccessMode.String())
			if n.ScanAggArray {
				v.observer.attr(name, "use statistic", "true")
			}
			//timeTmp1 and timeTmp2 need to synchronize changes due to tsspan conversion to zero zone time
			if nil != n.tsSpans {
				switch n.tsSpansPre {
				case 3:
					v.printTsSpans(n, name, 1e3)
				case 6:
					v.printTsSpans(n, name, 1e6)
				default:
					v.printTsSpans(n, name, 1e9)
				}
			}

		}
		if v.observer.expr != nil {
			v.expr(name, "filter", -1, n.filter)
			for i, fil := range n.TagFilterArray {
				v.expr(name, fmt.Sprintf("tag filter[%d]", i), -1, fil)
			}
			for i, fil := range n.PrimaryTagFilterArray {
				v.expr(name, fmt.Sprintf("ptag filter[%d]", i), -1, fil)
			}
			for i, fil := range n.TagIndexFilterArray {
				v.expr(name, fmt.Sprintf("tag index filter[%d]", i), -1, fil)
			}
		}

	case *filterNode:
		if v.observer.attr != nil {
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, engineType, ts)
			}
		}
		if v.observer.expr != nil {
			v.expr(name, "filter", -1, n.filter)
		}
		n.source.plan = v.visit(n.source.plan)

	case *renderNode:
		flag := CheckTsScanNode(n.source.plan)
		if v.observer.attr != nil {
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, engineType, ts)
			}
		}
		if v.observer.expr != nil && flag {
			for i, r := range n.render {
				exprStr := r.String()
				colName := n.columns[i].Name
				v.observer.attr(name, colName, fmt.Sprintf(exprStr))
				v.metadataExpr(name, "render", i, r)
			}
		}
		n.source.plan = v.visit(n.source.plan)

	case *indexJoinNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", fmt.Sprintf("%s@%s", n.table.desc.Name, n.table.index.Name))
			inputCols := planColumns(n.input)
			cols := make([]string, len(n.keyCols))
			for i, c := range n.keyCols {
				cols[i] = inputCols[c].Name
			}
			v.observer.attr(name, "key columns", strings.Join(cols, ", "))
			v.expr(name, "filter", -1, n.table.filter)
		}
		n.input = v.visit(n.input)

	case *lookupJoinNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", fmt.Sprintf("%s@%s", n.table.desc.Name, n.table.index.Name))
			v.observer.attr(name, "type", joinTypeStr(n.joinType))
		}
		var b bytes.Buffer
		b.WriteByte('(')
		inputCols := planColumns(n.input)
		for i, c := range n.eqCols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(inputCols[c].Name)
		}
		b.WriteString(") = (")
		for i := range n.eqCols {
			if i > 0 {
				b.WriteString(", ")
			}
			if i < len(n.table.index.ColumnNames) {
				b.WriteString(n.table.index.ColumnNames[i])
			} else {
				id := n.table.index.ExtraColumnIDs[i-len(n.table.index.ColumnNames)]
				col, err := n.table.desc.FindColumnByID(id)
				if err != nil {
					fmt.Fprintf(&b, "<error: %v>", err)
				} else {
					b.WriteString(col.Name)
				}
			}
		}
		b.WriteByte(')')
		v.observer.attr(name, "equality", b.String())
		if n.eqColsAreKey {
			v.observer.attr(name, "equality cols are key", "")
		}
		if n.CanParallelize() {
			v.observer.attr(name, "parallel", "")
		}
		if v.observer.expr != nil && n.onCond != nil && n.onCond != tree.DBoolTrue {
			v.expr(name, "pred", -1, n.onCond)
		}
		n.input = v.visit(n.input)

	case *zigzagJoinNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "type", joinTypeStr(sqlbase.InnerJoin))
			if v.observer.expr != nil && n.onCond != nil && n.onCond != tree.DBoolTrue {
				v.expr(name, "pred", -1, n.onCond)
			}
			for _, side := range n.sides {
				v.visitConcrete(side.scan)
				if side.fixedVals != nil {
					description := fmt.Sprintf(
						"%d column%s",
						len(side.fixedVals.columns),
						util.Pluralize(int64(len(side.fixedVals.columns))),
					)
					v.observer.attr(name, "fixedvals", description)
				}
			}
		}

	case *applyJoinNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "type", joinTypeStr(n.joinType))
		}
		if v.observer.expr != nil {
			v.expr(name, "pred", -1, n.pred.onCond)
		}
		n.input.plan = v.visit(n.input.plan)
		if v.observer.attr != nil {
			v.observer.attr(name, "right", n.right.SimpleString(
				int(memo.ExprFmtHideQualifications|memo.ExprFmtHideStats|memo.ExprFmtHideCost|memo.ExprFmtHideFuncDeps|memo.ExprFmtHidePhysProps|memo.ExprFmtHideRuleProps)))
		}

	case *joinNode:
		if v.observer.attr != nil {
			jType := joinTypeStr(n.joinType)
			if n.joinType == sqlbase.InnerJoin && len(n.pred.leftColNames) == 0 && n.pred.onCond == nil {
				jType = "cross"
			}
			v.observer.attr(name, "type", jType)

			if len(n.pred.leftColNames) > 0 {
				f := tree.NewFmtCtx(tree.FmtSimple)
				f.WriteByte('(')
				f.FormatNode(&n.pred.leftColNames)
				f.WriteString(") = (")
				f.FormatNode(&n.pred.rightColNames)
				f.WriteByte(')')
				v.observer.attr(name, "equality", f.CloseAndGetString())
				if n.pred.leftEqKey {
					v.observer.attr(name, "left cols are key", "")
				}
				if n.pred.rightEqKey {
					v.observer.attr(name, "right cols are key", "")
				}
			}
			if len(n.mergeJoinOrdering) > 0 {
				// The ordering refers to equality columns
				eqCols := make(sqlbase.ResultColumns, len(n.pred.leftEqualityIndices))
				for i := range eqCols {
					eqCols[i].Name = fmt.Sprintf("(%s=%s)", n.pred.leftColNames[i], n.pred.rightColNames[i])
				}
				v.observer.attr(name, "mergeJoinOrder", formatOrdering(n.mergeJoinOrdering, eqCols))
			}
		}
		if v.observer.expr != nil {
			v.expr(name, "pred", -1, n.pred.onCond)
		}
		n.left.plan = v.visit(n.left.plan)
		n.right.plan = v.visit(n.right.plan)

	case *batchLookUpJoinNode:
		if v.observer.attr != nil {
			jType := joinTypeStr(n.joinType)
			if n.joinType == sqlbase.InnerJoin && len(n.pred.leftColNames) == 0 && n.pred.onCond == nil {
				jType = "cross"
			}
			v.observer.attr(name, "type", jType)

			if len(n.pred.leftColNames) > 0 {
				f := tree.NewFmtCtx(tree.FmtSimple)
				f.WriteByte('(')
				f.FormatNode(&n.pred.leftColNames)
				f.WriteString(") = (")
				f.FormatNode(&n.pred.rightColNames)
				f.WriteByte(')')
				v.observer.attr(name, "equality", f.CloseAndGetString())
				if n.pred.leftEqKey {
					v.observer.attr(name, "left cols are key", "")
				}
				if n.pred.rightEqKey {
					v.observer.attr(name, "right cols are key", "")
				}
			}
		}
		if v.observer.expr != nil {
			v.expr(name, "pred", -1, n.pred.onCond)
		}
		n.left.plan = v.visit(n.left.plan)
		n.right.plan = v.visit(n.right.plan)

	case *limitNode:
		if v.observer.expr != nil {
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, "engine type", "time series")
			}
			if n.pushLimitToAggScan {
				v.observer.attr(name, "pushLimitToAggScan", "true")
			}
			v.expr(name, "count", -1, n.countExpr)
			v.expr(name, "offset", -1, n.offsetExpr)
			if n.canOpt {
				if n.offsetExpr != nil {
					v.observer.attr(name, "useOffsetOptimize", "true")
				} else {
					v.observer.attr(name, "useSorterScan", "true")
				}
			}
		}
		n.plan = v.visit(n.plan)

	case *max1RowNode:
		n.plan = v.visit(n.plan)

	case *distinctNode:
		if v.observer.attr == nil {
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, engineType, ts)
			}
			n.plan = v.visit(n.plan)
			break
		}

		if !n.distinctOnColIdxs.Empty() {
			var buf bytes.Buffer
			prefix := ""
			columns := planColumns(n.plan)
			n.distinctOnColIdxs.ForEach(func(col int) {
				buf.WriteString(prefix)
				buf.WriteString(columns[col].Name)
				prefix = ", "
			})
			v.observer.attr(name, "distinct on", buf.String())
			if n.nullsAreDistinct {
				v.observer.attr(name, "nulls are distinct", "")
			}
			if n.errorOnDup != "" {
				v.observer.attr(name, "error on duplicate", "")
			}
		}

		if !n.columnsInOrder.Empty() {
			var buf bytes.Buffer
			prefix := ""
			columns := planColumns(n.plan)
			for i, ok := n.columnsInOrder.Next(0); ok; i, ok = n.columnsInOrder.Next(i + 1) {
				buf.WriteString(prefix)
				buf.WriteString(columns[i].Name)
				prefix = ", "
			}
			v.observer.attr(name, "order key", buf.String())
		}

		n.plan = v.visit(n.plan)

	case *sortNode:
		if v.observer.attr != nil {
			columns := planColumns(n.plan)
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, engineType, ts)
			}
			v.observer.attr(name, "order", formatOrdering(n.ordering, columns))
			if p := n.alreadyOrderedPrefix; p > 0 {
				v.observer.attr(name, "already ordered", formatOrdering(n.ordering[:p], columns))
			}
		}
		n.plan = v.visit(n.plan)

	case *groupNode:
		if v.observer.attr != nil {
			if n.engine == tree.EngineTypeTimeseries {
				v.observer.attr(name, engineType, ts)
			}
			inputCols := planColumns(n.plan)
			for i, agg := range n.funcs {
				var buf bytes.Buffer
				if groupingCol, ok := n.aggIsGroupingColumn(i); ok {
					buf.WriteString(inputCols[groupingCol].Name)
				} else {
					fmt.Fprintf(&buf, "%s(", agg.funcName)
					if len(agg.argRenderIdxs) > 0 {
						if agg.isDistinct() {
							buf.WriteString("DISTINCT ")
						}

						for i, idx := range agg.argRenderIdxs {
							if i != 0 {
								buf.WriteString(", ")
							}
							buf.WriteString(inputCols[idx].Name)
						}
					}
					buf.WriteByte(')')
					if agg.filterRenderIdx != noRenderIdx {
						fmt.Fprintf(&buf, " FILTER (WHERE %s)", inputCols[agg.filterRenderIdx].Name)
					}
				}
				v.observer.attr(name, fmt.Sprintf("aggregate %d", i), buf.String())
			}
			if len(n.groupCols) > 0 {
				var cols []string
				for _, c := range n.groupCols {
					cols = append(cols, inputCols[c].Name)
				}
				v.observer.attr(name, "group by", strings.Join(cols, ", "))
			}
			if len(n.groupColOrdering) > 0 {
				v.observer.attr(name, "ordered", formatOrdering(n.groupColOrdering, inputCols))
			}
			if n.isScalar {
				v.observer.attr(name, "scalar", "")
			}
			if n.optType.PushLocalAggToScanOpt() {
				v.observer.attr(name, "pushLocalAggToScan ", "true")
			}
			if n.addSynchronizer {
				v.observer.attr(name, "addSynchronizer ", "true")
			}
			if n.optType.PruneLocalAggOpt() {
				v.observer.attr(name, "pruneLocalAgg ", "true")
			}
			if n.optType.PruneTSFinalAggOpt() {
				v.observer.attr(name, "pruneTSFinalAgg ", "true")
			}
			if n.optType.PruneFinalAggOpt() {
				v.observer.attr(name, "pruneFinalAgg ", "true")
			}
		}

		n.plan = v.visit(n.plan)

	case *windowNode:
		if v.observer.expr != nil {
			for i, agg := range n.funcs {
				v.metadataExpr(name, "window", i, agg.expr)
			}
			for i, rexpr := range n.windowRender {
				v.metadataExpr(name, "render", i, rexpr)
			}
		}
		n.plan = v.visit(n.plan)

	case *unionNode:
		n.left = v.visit(n.left)
		n.right = v.visit(n.right)

	case *splitNode:
		n.rows = v.visit(n.rows)

	case *unsplitNode:
		n.rows = v.visit(n.rows)

	case *relocateNode:
		n.rows = v.visit(n.rows)

	case *selectIntoNode:
		n.rows = v.visit(n.rows)

	case *tsInsertSelectNode:
		v.observer.attr(name, "into", n.TableName)
		n.plan = v.visit(n.plan)

	case *insertNode, *insertFastPathNode:
		var run *insertRun
		if ins, ok := n.(*insertNode); ok {
			run = &ins.run
		} else {
			run = &n.(*insertFastPathNode).run.insertRun
		}

		if v.observer.attr != nil {
			var buf bytes.Buffer
			buf.WriteString(run.ti.tableDesc().Name)
			buf.WriteByte('(')
			for i := range run.insertCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(run.insertCols[i].Name)
			}
			buf.WriteByte(')')
			v.observer.attr(name, "into", buf.String())
			v.observer.attr(name, "strategy", run.ti.desc())
			if run.ti.autoCommit == autoCommitEnabled {
				v.observer.attr(name, "auto commit", "")
			}
		}

		if ins, ok := n.(*insertNode); ok {
			ins.source = v.visit(ins.source)
		} else {
			ins := n.(*insertFastPathNode)
			if v.observer.attr != nil {
				for i := range ins.run.fkChecks {
					c := &ins.run.fkChecks[i]
					tabDesc := c.ReferencedTable.(*optTable).desc
					idxDesc := c.ReferencedIndex.(*optIndex).desc
					v.observer.attr(name, "FK check", fmt.Sprintf("%s@%s", tabDesc.Name, idxDesc.Name))
				}
			}
			if len(ins.input) != 0 {
				if v.observer.attr != nil {
					v.observer.attr(name, "size", formatValuesSize(len(ins.input), len(ins.input[0])))
				}

				if v.observer.expr != nil {
					v.metadataTuples(name, ins.input)
				}
			}
		}

	case *upsertNode:
		if v.observer.attr != nil {
			var buf bytes.Buffer
			buf.WriteString(n.run.tw.tableDesc().Name)
			buf.WriteByte('(')
			for i := range n.run.insertCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(n.run.insertCols[i].Name)
			}
			buf.WriteByte(')')
			v.observer.attr(name, "into", buf.String())
			v.observer.attr(name, "strategy", n.run.tw.desc())
			if n.run.tw.autoCommit == autoCommitEnabled {
				v.observer.attr(name, "auto commit", "")
			}
		}

		n.source = v.visit(n.source)

	case *updateNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", n.run.tu.tableDesc().Name)
			if len(n.run.tu.ru.UpdateCols) > 0 {
				var buf bytes.Buffer
				for i := range n.run.tu.ru.UpdateCols {
					if i > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(n.run.tu.ru.UpdateCols[i].Name)
				}
				v.observer.attr(name, "set", buf.String())
			}
			v.observer.attr(name, "strategy", n.run.tu.desc())
			if n.run.tu.autoCommit == autoCommitEnabled {
				v.observer.attr(name, "auto commit", "")
			}
		}
		if v.observer.expr != nil {
			for i, cexpr := range n.run.computeExprs {
				v.metadataExpr(name, "computed", i, cexpr)
			}
		}
		// An updater has no sub-expressions, so nothing special to do here.
		n.source = v.visit(n.source)

	case *deleteNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "from", n.run.td.tableDesc().Name)
			v.observer.attr(name, "strategy", n.run.td.desc())
			if n.run.td.autoCommit == autoCommitEnabled {
				v.observer.attr(name, "auto commit", "")
			}
		}
		// A deleter has no sub-expressions, so nothing special to do here.
		n.source = v.visit(n.source)

	case *deleteRangeNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "from", n.desc.Name)
			if n.autoCommitEnabled {
				v.observer.attr(name, "auto commit", "")
			}
		}
		if v.observer.spans != nil {
			v.observer.spans(name, "spans", &n.desc.PrimaryIndex, n.spans, false /* hardLimitSet */)
		}

	case *serializeNode:
		v.visitConcrete(n.source)

	case *rowCountNode:
		v.visitConcrete(n.source)

	case *createTableNode:
		if n.n.As() {
			n.sourcePlan = v.visit(n.sourcePlan)
		}

	case *createViewNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "query", n.viewQuery)
		}

	case *setVarNode:
		if v.observer.expr != nil {
			for i, texpr := range n.typedValues {
				v.metadataExpr(name, "value", i, texpr)
			}
		}

	case *setClusterSettingNode:
		if v.observer.expr != nil && n.value != nil {
			v.metadataExpr(name, "value", -1, n.value)
		}

	case *delayedNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "source", n.name)
		}
		if n.plan != nil {
			n.plan = v.visit(n.plan)
		}

	case *explainDistSQLNode:
		n.plan = v.visit(n.plan)

	case *explainVecNode:
		n.plan = v.visit(n.plan)

	case *ordinalityNode:
		n.source = v.visit(n.source)

	case *spoolNode:
		if n.hardLimit > 0 && v.observer.attr != nil {
			v.observer.attr(name, "limit", fmt.Sprintf("%d", n.hardLimit))
		}
		n.source = v.visit(n.source)

	case *saveTableNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "target", n.target.String())
		}
		n.source = v.visit(n.source)

	case *showTraceReplicaNode:
		n.plan = v.visit(n.plan)

	case *explainPlanNode:
		n.plan = v.visit(n.plan)

	case *cancelQueriesNode:
		n.rows = v.visit(n.rows)

	case *cancelSessionsNode:
		n.rows = v.visit(n.rows)

	case *controlJobsNode:
		n.rows = v.visit(n.rows)

	case *setZoneConfigNode:
		if v.observer.expr != nil {
			v.metadataExpr(name, "yaml", -1, n.yamlConfig)
		}

	case *projectSetNode:
		if v.observer.expr != nil {
			for i, texpr := range n.exprs {
				v.metadataExpr(name, "render", i, texpr)
			}
		}
		n.source = v.visit(n.source)

	case *rowSourceToPlanNode:
		if v.observer.followRowSourceToPlanNode && n.originalPlanNode != nil {
			v.visit(n.originalPlanNode)
		}

	case *errorIfRowsNode:
		n.plan = v.visit(n.plan)

	case *scanBufferNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "label", n.label)
		}

	case *bufferNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "label", n.label)
		}
		n.plan = v.visit(n.plan)

	case *recursiveCTENode:
		if v.observer.attr != nil {
			v.observer.attr(name, "label", n.label)
		}
		n.initial = v.visit(n.initial)

	case *exportNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "destination", n.fileName)
		}
		n.source = v.visit(n.source)
	case *callProcedureNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "call", n.procCall)
		}
	}
}

func (v *planVisitor) printTsSpans(n *tsScanNode, name string, precision int64) {
	var timeTmp1 time.Time
	if n.tsSpans[0].FromTimeStamp > math.MinInt64 {
		timeTmp1 = timeutil.Unix(n.tsSpans[0].FromTimeStamp/precision, n.tsSpans[0].FromTimeStamp%precision*(1e9/precision))
	} else {
		if precision == 1e9 {
			timeTmp1 = timeutil.Unix(tree.TsMinNanoTimestamp/precision, tree.TsMinNanoTimestamp%precision*(1e9/precision))
		} else {
			timeTmp1 = timeutil.Unix(tree.TsMinTimestamp/1000, tree.TsMinTimestamp%1000*1000000)
		}
	}
	var timeTmp2 time.Time
	if n.tsSpans[0].ToTimeStamp < math.MaxInt64 {
		timeTmp2 = timeutil.Unix(n.tsSpans[0].ToTimeStamp/precision, n.tsSpans[0].ToTimeStamp%precision*(1e9/precision))
	} else {
		if precision == 1e9 {
			timeTmp2 = timeutil.Unix(tree.TsMaxNanoTimestamp/precision, tree.TsMaxNanoTimestamp%precision*(1e9/precision))
		} else {
			timeTmp2 = timeutil.Unix(tree.TsMaxTimestamp/1000, tree.TsMaxTimestamp%1000*1000000)
		}
	}

	v.observer.attr(name, "spans:fromTime", timeTmp1.String())
	v.observer.attr(name, "spans:toTime", timeTmp2.String())
}

// expr wraps observer.expr() and provides it with the current node's
// name.
func (v *planVisitor) expr(nodeName string, fieldName string, n int, expr tree.Expr) {
	if v.err != nil {
		return
	}
	v.observer.expr(observeAlways, nodeName, fieldName, n, expr)
}

// metadataExpr wraps observer.expr() and provides it with the current node's
// name, with verbosity = metadata.
func (v *planVisitor) metadataExpr(nodeName string, fieldName string, n int, expr tree.Expr) {
	if v.err != nil {
		return
	}
	v.observer.expr(observeMetadata, nodeName, fieldName, n, expr)
}

// metadataTuples calls metadataExpr for each expression in a matrix.
func (v *planVisitor) metadataTuples(nodeName string, tuples [][]tree.TypedExpr) {
	for i := range tuples {
		for j, expr := range tuples[i] {
			var fieldName string
			if v.observer.attr != nil {
				fieldName = fmt.Sprintf("row %d, expr", i)
			}
			v.metadataExpr(nodeName, fieldName, j, expr)
		}
	}
}

func formatOrdering(ordering sqlbase.ColumnOrdering, columns sqlbase.ResultColumns) string {
	var buf bytes.Buffer
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i, o := range ordering {
		if i > 0 {
			buf.WriteByte(',')
		}
		prefix := byte('+')
		if o.Direction == encoding.Descending {
			prefix = byte('-')
		}
		buf.WriteByte(prefix)

		fmtCtx.FormatNameP(&columns[o.ColIdx].Name)
		_, _ = fmtCtx.WriteTo(&buf)
	}
	fmtCtx.Close()
	return buf.String()
}

// formatValuesSize returns a string of the form "5 columns, 1 row".
func formatValuesSize(numRows, numCols int) string {
	return fmt.Sprintf(
		"%d column%s, %d row%s",
		numCols, util.Pluralize(int64(numCols)),
		numRows, util.Pluralize(int64(numRows)),
	)
}

// nodeName returns the name of the given planNode as string.  The
// node's current state is taken into account, e.g. sortNode has
// either name "sort" or "nosort" depending on whether sorting is
// needed.
func nodeName(plan planNode) string {
	// Some nodes have custom names depending on attributes.
	switch n := plan.(type) {
	case *scanNode:
		if n.reverse {
			return "revscan"
		}
	case *unionNode:
		if n.emitAll {
			return "append"
		}

	case *joinNode:
		if len(n.mergeJoinOrdering) > 0 {
			return "merge-join"
		}
		if len(n.pred.leftEqualityIndices) == 0 {
			return "cross-join"
		}
		return "hash-join"

	case *batchLookUpJoinNode:
		return "batch-lookup-join"

	}

	name, ok := planNodeNames[reflect.TypeOf(plan)]
	if !ok {
		panic(fmt.Sprintf("name missing for type %T", plan))
	}

	return name
}

func joinTypeStr(t sqlbase.JoinType) string {
	switch t {
	case sqlbase.InnerJoin:
		return "inner"
	case sqlbase.LeftOuterJoin:
		return "left outer"
	case sqlbase.RightOuterJoin:
		return "right outer"
	case sqlbase.FullOuterJoin:
		return "full outer"
	case sqlbase.LeftSemiJoin:
		return "semi"
	case sqlbase.LeftAntiJoin:
		return "anti"
	}
	panic(fmt.Sprintf("unknown join type %s", t))
}

// planNodeNames is the mapping from node type to strings.  The
// strings are constant and not precomputed so that the type names can
// be changed without changing the output of "EXPLAIN".
var planNodeNames = map[reflect.Type]string{
	reflect.TypeOf(&alterTSDatabaseNode{}):      "alter ts database",
	reflect.TypeOf(&alterIndexNode{}):           "alter index",
	reflect.TypeOf(&alterSequenceNode{}):        "alter sequence",
	reflect.TypeOf(&alterTableNode{}):           "alter table",
	reflect.TypeOf(&alterScheduleNode{}):        "alter schedule",
	reflect.TypeOf(&alterRoleNode{}):            "alter role",
	reflect.TypeOf(&alterAuditNode{}):           "alter audit",
	reflect.TypeOf(&applyJoinNode{}):            "apply-join",
	reflect.TypeOf(&bufferNode{}):               "buffer node",
	reflect.TypeOf(&cancelQueriesNode{}):        "cancel queries",
	reflect.TypeOf(&cancelSessionsNode{}):       "cancel sessions",
	reflect.TypeOf(&changePrivilegesNode{}):     "change privileges",
	reflect.TypeOf(&commentOnColumnNode{}):      "comment on column",
	reflect.TypeOf(&commentOnDatabaseNode{}):    "comment on database",
	reflect.TypeOf(&commentOnProcedureNode{}):   "comment on procedure",
	reflect.TypeOf(&commentOnIndexNode{}):       "comment on index",
	reflect.TypeOf(&commentOnTableNode{}):       "comment on table",
	reflect.TypeOf(&controlJobsNode{}):          "control jobs",
	reflect.TypeOf(&createDatabaseNode{}):       "create database",
	reflect.TypeOf(&createFunctionNode{}):       "create function",
	reflect.TypeOf(&createIndexNode{}):          "create index",
	reflect.TypeOf(&createSequenceNode{}):       "create sequence",
	reflect.TypeOf(&createSchemaNode{}):         "create schema",
	reflect.TypeOf(&createScheduleNode{}):       "create schedule",
	reflect.TypeOf(&createStatsNode{}):          "create statistics",
	reflect.TypeOf(&createProcedureNode{}):      "create procedure",
	reflect.TypeOf(&callProcedureNode{}):        "call procedure",
	reflect.TypeOf(&createTableNode{}):          "create table",
	reflect.TypeOf(&createMultiInstTableNode{}): "create tables",
	reflect.TypeOf(&CreateRoleNode{}):           "create user/role",
	reflect.TypeOf(&createViewNode{}):           "create view",
	reflect.TypeOf(&createAuditNode{}):          "create audit",
	reflect.TypeOf(&delayedNode{}):              "virtual table",
	reflect.TypeOf(&deleteNode{}):               "delete",
	reflect.TypeOf(&tsDeleteNode{}):             "TSDelete",
	reflect.TypeOf(&tsTagUpdateNode{}):          "TSTagUpdate",
	reflect.TypeOf(&deleteRangeNode{}):          "delete range",
	reflect.TypeOf(&distinctNode{}):             "distinct",
	reflect.TypeOf(&dropDatabaseNode{}):         "drop database",
	reflect.TypeOf(&dropSchemaNode{}):           "drop schema",
	reflect.TypeOf(&dropIndexNode{}):            "drop index",
	reflect.TypeOf(&dropSequenceNode{}):         "drop sequence",
	reflect.TypeOf(&dropTableNode{}):            "drop table",
	reflect.TypeOf(&DropRoleNode{}):             "drop user/role",
	reflect.TypeOf(&dropViewNode{}):             "drop view",
	reflect.TypeOf(&dropFunctionNode{}):         "drop function",
	reflect.TypeOf(&dropProcedureNode{}):        "drop procedure",
	reflect.TypeOf(&dropAuditNode{}):            "drop audit",
	reflect.TypeOf(&errorIfRowsNode{}):          "error if rows",
	reflect.TypeOf(&explainDistSQLNode{}):       "explain distsql",
	reflect.TypeOf(&explainPlanNode{}):          "explain plan",
	reflect.TypeOf(&explainVecNode{}):           "explain vectorized",
	reflect.TypeOf(&exportNode{}):               "export",
	reflect.TypeOf(&filterNode{}):               "filter",
	reflect.TypeOf(&GrantRoleNode{}):            "grant role",
	reflect.TypeOf(&groupNode{}):                "group",
	reflect.TypeOf(&hookFnNode{}):               "plugin",
	reflect.TypeOf(&indexJoinNode{}):            "index-join",
	reflect.TypeOf(&insertNode{}):               "insert",
	reflect.TypeOf(&tsInsertNode{}):             "TSInsert",
	reflect.TypeOf(&tsDDLNode{}):                "TSDDL",
	reflect.TypeOf(&tsInsertSelectNode{}):       "TSInsertSelect",
	reflect.TypeOf(&synchronizerNode{}):         "synchronizer",
	reflect.TypeOf(&insertFastPathNode{}):       "insert-fast-path",
	reflect.TypeOf(&joinNode{}):                 "join",
	reflect.TypeOf(&limitNode{}):                "limit",
	reflect.TypeOf(&lookupJoinNode{}):           "lookup-join",
	reflect.TypeOf(&max1RowNode{}):              "max1row",
	reflect.TypeOf(&operateDataNode{}):          "clear or compress data",
	reflect.TypeOf(&ordinalityNode{}):           "ordinality",
	reflect.TypeOf(&projectSetNode{}):           "project set",
	reflect.TypeOf(&pauseScheduleNode{}):        "pause schedule",
	reflect.TypeOf(&dropScheduleNode{}):         "drop schedule",
	reflect.TypeOf(&recursiveCTENode{}):         "recursive cte node",
	reflect.TypeOf(&rebalanceTsDataNode{}):      "rebalance ts data",
	reflect.TypeOf(&relocateNode{}):             "relocate",
	reflect.TypeOf(&renameColumnNode{}):         "rename column",
	reflect.TypeOf(&renameDatabaseNode{}):       "rename database",
	reflect.TypeOf(&renameIndexNode{}):          "rename index",
	reflect.TypeOf(&renameTableNode{}):          "rename table",
	reflect.TypeOf(&resumeScheduleNode{}):       "resume schedule",
	reflect.TypeOf(&renderNode{}):               "render",
	reflect.TypeOf(&RevokeRoleNode{}):           "revoke role",
	reflect.TypeOf(&rowCountNode{}):             "count",
	reflect.TypeOf(&rowSourceToPlanNode{}):      "row source to plan node",
	reflect.TypeOf(&saveTableNode{}):            "save table",
	reflect.TypeOf(&scanBufferNode{}):           "scan buffer node",
	reflect.TypeOf(&scanNode{}):                 "scan",
	reflect.TypeOf(&tsScanNode{}):               "ts scan",
	reflect.TypeOf(&scatterNode{}):              "scatter",
	reflect.TypeOf(&scrubNode{}):                "scrub",
	reflect.TypeOf(&sequenceSelectNode{}):       "sequence select",
	reflect.TypeOf(&serializeNode{}):            "run",
	reflect.TypeOf(&setClusterSettingNode{}):    "set cluster setting",
	reflect.TypeOf(&setVarNode{}):               "set",
	reflect.TypeOf(&setZoneConfigNode{}):        "configure zone",
	reflect.TypeOf(&showFingerprintsNode{}):     "showFingerprints",
	reflect.TypeOf(&showTraceNode{}):            "show trace for",
	reflect.TypeOf(&showTraceReplicaNode{}):     "replica trace",
	reflect.TypeOf(&sortNode{}):                 "sort",
	reflect.TypeOf(&splitNode{}):                "split",
	reflect.TypeOf(&unsplitNode{}):              "unsplit",
	reflect.TypeOf(&unsplitAllNode{}):           "unsplit all",
	reflect.TypeOf(&selectIntoNode{}):           "select into",
	reflect.TypeOf(&spoolNode{}):                "spool",
	reflect.TypeOf(&truncateNode{}):             "truncate",
	reflect.TypeOf(&unaryNode{}):                "emptyrow",
	reflect.TypeOf(&unionNode{}):                "union",
	reflect.TypeOf(&updateNode{}):               "update",
	reflect.TypeOf(&upsertNode{}):               "upsert",
	reflect.TypeOf(&valuesNode{}):               "values",
	reflect.TypeOf(&virtualTableNode{}):         "virtual table values",
	reflect.TypeOf(&windowNode{}):               "window",
	reflect.TypeOf(&zeroNode{}):                 "norows",
	reflect.TypeOf(&zigzagJoinNode{}):           "zigzag-join",
	reflect.TypeOf(&importPortalNode{}):         "import portal",

	reflect.TypeOf(&refreshMaterializedViewNode{}): "refresh materialized view",
}
