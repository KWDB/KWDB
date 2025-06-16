// Copyright 2015 The Cockroach Authors.
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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	// The schema for this groupNode.
	columns sqlbase.ResultColumns

	// The source node (which returns values that feed into the aggregation).
	plan planNode

	// Indices of the group by columns in the source plan.
	groupCols []int

	// Set when we have an input ordering on (a subset of) grouping columns. Only
	// column indices in groupCols can appear in this ordering.
	groupColOrdering sqlbase.ColumnOrdering

	// gapFillColID representing the column ID of timebucketGapFill in the group.
	gapFillColID int32

	groupWindowID int32

	groupWindowTSColID int32

	groupWindowExtend []int32

	// isScalar is set for "scalar groupby", where we want a result
	// even if there are no input rows, e.g. SELECT MIN(x) FROM t.
	isScalar bool

	// funcs are the aggregation functions that the renders use.
	funcs []*aggregateFuncHolder

	reqOrdering ReqOrdering
	aggFuncs    *opt.AggFuncNames

	// engine is a bit set that indicates which engine to exec.
	engine tree.EngineType

	// ts engine parallel execute flag and degree
	addSynchronizer bool

	// optType is flags of group by type, type is opt.GroupOptType
	optType opt.GroupOptType
}

func (n *groupNode) startExec(params runParams) error {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Next(params runParams) (bool, error) {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Values() tree.Datums {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	for _, f := range n.funcs {
		f.close(ctx)
	}
}

// aggIsGroupingColumn returns true if the given output aggregation is an
// any_not_null aggregation for a grouping column. The grouping column
// index is also returned.
func (n *groupNode) aggIsGroupingColumn(aggIdx int) (colIdx int, ok bool) {
	if holder := n.funcs[aggIdx]; holder.funcName == builtins.AnyNotNull {
		for _, c := range n.groupCols {
			for _, renderIdx := range holder.argRenderIdxs {
				if c == renderIdx {
					return c, true
				}
			}
		}
	}
	return -1, false
}

type aggregateFuncHolder struct {
	// Name of the aggregate function. Empty if this column reproduces a bucket
	// key unchanged.
	funcName string

	resultType *types.T

	// The argument of the function is a single value produced by the renderNode
	// underneath. If the function has no argument (COUNT_ROWS), it is empty.
	argRenderIdxs []int
	// If there is a filter, the result is a single value produced by the
	// renderNode underneath. If there is no filter, it is set to noRenderIdx.
	filterRenderIdx int

	// create instantiates the built-in execution context for the
	// aggregation function.
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

	// arguments are constant expressions that can be optionally passed into an
	// aggregator.
	arguments tree.Datums

	run aggregateFuncRun
}

// aggregateFuncRun contains the run-time state for one aggregation function
// during local execution.
type aggregateFuncRun struct {
	buckets       map[string]tree.AggregateFunc
	bucketsMemAcc mon.BoundAccount
	seen          map[string]struct{}
}

const noRenderIdx = -1

// newAggregateFuncHolder creates an aggregateFuncHolder.
//
// If function is nil, this is an "ident" aggregation (meaning that the input is
// a group-by column and the "aggregation" returns its value)
//
// If the aggregation function takes no arguments (e.g. COUNT_ROWS),
// argRenderIdx is noRenderIdx.
func (n *groupNode) newAggregateFuncHolder(
	funcName string,
	resultType *types.T,
	argRenderIdxs []int,
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc,
	arguments tree.Datums,
	acc mon.BoundAccount,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		funcName:        funcName,
		resultType:      resultType,
		argRenderIdxs:   argRenderIdxs,
		filterRenderIdx: noRenderIdx,
		create:          create,
		arguments:       arguments,
		run: aggregateFuncRun{
			buckets:       make(map[string]tree.AggregateFunc),
			bucketsMemAcc: acc,
		},
	}
	return res
}

func (a *aggregateFuncHolder) hasFilter() bool {
	return a.filterRenderIdx != noRenderIdx
}

// setDistinct causes a to ignore duplicate values of the argument.
func (a *aggregateFuncHolder) setDistinct() {
	a.run.seen = make(map[string]struct{})
}

// isDistinct returns true if only distinct values are aggregated,
// e.g. SUM(DISTINCT x).
func (a *aggregateFuncHolder) isDistinct() bool {
	return a.run.seen != nil
}

func (a *aggregateFuncHolder) close(ctx context.Context) {
	for _, aggFunc := range a.run.buckets {
		aggFunc.Close(ctx)
	}

	a.run.buckets = nil
	a.run.seen = nil

	a.run.bucketsMemAcc.Close(ctx)
}
