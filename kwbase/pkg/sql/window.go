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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// A windowNode implements the PlanNode interface and handles windowing logic.
//
// windowRender will contain renders that will output the desired result
// columns (so len(windowRender) == len(columns)).
//  1. If ith render from the source node does not have any window functions,
//     then that column will be simply passed through and windowRender[i] is
//     nil. Notably, windowNode will rearrange renders in the source node so
//     that all such passed through columns are contiguous and in the beginning.
//     (This happens during extractWindowFunctions call.)
//  2. If ith render from the source node has any window functions, then the
//     render is stored in windowRender[i]. During
//     constructWindowFunctionsDefinitions all variables used in OVER clauses
//     of all window functions are being rendered, and during
//     setupWindowFunctions all arguments to all window functions are being
//     rendered (renders are reused if possible).
//
// Therefore, the schema of the source node will be changed to look as follows:
// pass through column | OVER clauses columns | arguments to window functions.
var _ PlanNode = &windowNode{}

type windowNode struct {
	// The source node.
	plan PlanNode
	// columns is the set of result columns.
	columns sqlbase.ResultColumns

	// A sparse array holding renders specific to this windowNode. This will
	// contain nil entries for renders that do not contain window functions,
	// and which therefore can be propagated directly from the "wrapped" node.
	windowRender []tree.TypedExpr

	// The window functions handled by this windowNode.
	funcs []*windowFuncHolder

	// colAndAggContainer is an IndexedVarContainer that provides indirection
	// to migrate IndexedVars and aggregate functions below the windowing level.
	colAndAggContainer windowNodeColAndAggContainer

	// engine is a bit set that indicates which engine to exec.
	engine tree.EngineType
}

func (n *windowNode) StartExec(params RunParams) error {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Next(params RunParams) (bool, error) {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Values() tree.Datums {
	panic("windowNode can't be run in local mode")
}

func (n *windowNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}

var _ tree.TypedExpr = &windowFuncHolder{}
var _ tree.VariableExpr = &windowFuncHolder{}

type windowFuncHolder struct {
	window *windowNode

	expr *tree.FuncExpr
	args []tree.Expr

	argsIdxs     []uint32 // indices of the columns that are arguments to the window function
	filterColIdx int      // optional index of filtering column, -1 if no filter
	outputColIdx int      // index of the column that the output should be put into

	partitionIdxs  []int
	columnOrdering sqlbase.ColumnOrdering
	frame          *tree.WindowFrame
}

// samePartition returns whether f and other have the same PARTITION BY clause.
func (w *windowFuncHolder) samePartition(other *windowFuncHolder) bool {
	if len(w.partitionIdxs) != len(other.partitionIdxs) {
		return false
	}
	for i, p := range w.partitionIdxs {
		if p != other.partitionIdxs[i] {
			return false
		}
	}
	return true
}

func (*windowFuncHolder) Variable() {}

func (w *windowFuncHolder) Format(ctx *tree.FmtCtx) {
	// Avoid duplicating the type annotation by calling .Format directly.
	w.expr.Format(ctx)
}

func (w *windowFuncHolder) String() string { return tree.AsString(w) }

func (w *windowFuncHolder) Walk(v tree.Visitor) tree.Expr { return w }

func (w *windowFuncHolder) TypeCheck(
	_ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx *tree.EvalContext) (tree.Datum, error) {
	panic("windowFuncHolder should not be evaluated directly")
}

func (w *windowFuncHolder) ResolvedType() *types.T {
	return w.expr.ResolvedType()
}

// windowNodeColAndAggContainer is an IndexedVarContainer providing indirection
// for IndexedVars and aggregation functions found above the windowing level.
// See replaceIndexVarsAndAggFuncs.
type windowNodeColAndAggContainer struct {
	// idxMap maps the index of IndexedVars created in replaceIndexVarsAndAggFuncs
	// to the index their corresponding results in this container. It permits us to
	// add a single render to the source plan per unique expression.
	idxMap map[int]int
	// sourceInfo contains information on the IndexedVars from the
	// source plan where they were originally created.
	sourceInfo *sqlbase.DataSourceInfo
	// aggFuncs maps the index of IndexedVars to their corresponding aggregate function.
	aggFuncs map[int]*tree.FuncExpr
	// startAggIdx indicates the smallest index to be used by an IndexedVar replacing
	// an aggregate function. We don't want to mix these IndexedVars with those
	// that replace "original" IndexedVars.
	startAggIdx int
}

func (c *windowNodeColAndAggContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	panic("IndexedVarEval should not be called on windowNodeColAndAggContainer")
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (c *windowNodeColAndAggContainer) IndexedVarResolvedType(idx int) *types.T {
	if idx >= c.startAggIdx {
		return c.aggFuncs[idx].ResolvedType()
	}
	return c.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (c *windowNodeColAndAggContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	if idx >= c.startAggIdx {
		// Avoid duplicating the type annotation by calling .Format directly.
		return c.aggFuncs[idx]
	}
	// Avoid duplicating the type annotation by calling .Format directly.
	return c.sourceInfo.NodeFormatter(idx)
}
