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
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// filterNode implements a filtering stage. It is intended to be used
// during plan optimizations in order to avoid instantiating a fully
// blown selectTopNode/renderNode pair.
var _ PlanNode = &filterNode{}

type filterNode struct {
	source      planDataSource
	filter      tree.TypedExpr
	ivarHelper  tree.IndexedVarHelper
	reqOrdering ReqOrdering

	// engine is a bit set that indicates which engine to exec.
	engine tree.EngineType
}

// filterNode implements tree.IndexedVarContainer
var _ tree.IndexedVarContainer = &filterNode{}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return f.source.plan.Values()[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarResolvedType(idx int) *types.T {
	return f.source.columns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (f *filterNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return f.source.columns.NodeFormatter(idx)
}

func (f *filterNode) StartExec(RunParams) error {
	return nil
}

// Next implements the PlanNode interface.
func (f *filterNode) Next(params RunParams) (bool, error) {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Values() tree.Datums {
	panic("filterNode cannot be run in local mode")
}

func (f *filterNode) Close(ctx context.Context) { f.source.plan.Close(ctx) }

func isFilterTrue(expr tree.TypedExpr) bool {
	return expr == nil || expr == tree.DBoolTrue
}
