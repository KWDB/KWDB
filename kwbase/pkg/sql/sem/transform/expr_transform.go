// Copyright 2017 The Cockroach Authors.
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

package transform

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

// ExprTransformContext supports the methods that test expression
// properties and can normalize expressions, defined below.  This
// should be used in planner instance to avoid re-allocation of these
// visitors between uses.
type ExprTransformContext struct {
	normalizeVisitor   tree.NormalizeVisitor
	isAggregateVisitor IsAggregateVisitor
}

// NormalizeExpr is a wrapper around EvalContext.NormalizeExpr which
// avoids allocation of a normalizeVisitor. See normalize.go for
// details.
func (t *ExprTransformContext) NormalizeExpr(
	ctx *tree.EvalContext, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	if t.shouldBypassNormalization(ctx) {
		return typedExpr, nil
	}
	return t.performNormalization(ctx, typedExpr)
}

// AggregateInExpr determines if an Expr contains an aggregate function.
// TODO(knz/radu): this is not the right way to go about checking
// these things. Instead whatever analysis occurs prior on the expression
// should collect scalar properties (see tree.ScalarProperties) and
// then the collected properties should be tested directly.
func (t *ExprTransformContext) AggregateInExpr(
	expr tree.Expr, searchPath sessiondata.SearchPath,
) bool {
	if expr == nil {
		return false
	}
	return t.walkExpressionForAggregates(expr, searchPath)
}

// shouldBypassNormalization checks whether normalization should be skipped
// based on the evaluation context's SkipNormalize flag.
func (t *ExprTransformContext) shouldBypassNormalization(ctx *tree.EvalContext) bool {
	return ctx.SkipNormalize
}

// performNormalization runs the normalization visitor over the typed expression
// and returns the normalized result or an error.
func (t *ExprTransformContext) performNormalization(
	ctx *tree.EvalContext, typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	t.normalizeVisitor = tree.MakeNormalizeVisitor(ctx)
	expr, _ := tree.WalkExpr(&t.normalizeVisitor, typedExpr)
	if err := t.normalizeVisitor.Err(); err != nil {
		return nil, err
	}
	return expr.(tree.TypedExpr), nil
}

// walkExpressionForAggregates prepares the aggregate-detection visitor and
// walks the expression tree to determine whether any aggregate function is
// present.
func (t *ExprTransformContext) walkExpressionForAggregates(
	expr tree.Expr, searchPath sessiondata.SearchPath,
) bool {
	t.isAggregateVisitor = IsAggregateVisitor{
		searchPath: searchPath,
	}
	tree.WalkExprConst(&t.isAggregateVisitor, expr)
	return t.isAggregateVisitor.Aggregated
}
