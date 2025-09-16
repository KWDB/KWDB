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

// This file contains helper code to populate execinfrapb.Expressions during
// planning.

package physicalplan

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// ExprContext is an interface containing objects necessary for creating
// execinfrapb.Expressions.
type ExprContext interface {
	// EvalContext returns the tree.EvalContext for planning.
	EvalContext() *tree.EvalContext

	// IsLocal returns true if the current plan is local.
	IsLocal() bool

	// EvaluateSubqueries returns true if subqueries should be evaluated before
	// creating the execinfrapb.Expression.
	EvaluateSubqueries() bool

	// IsTs return true if there is tsTableReader in this ctx
	IsTs() bool
}

// fakeExprContext is a fake implementation of ExprContext that always behaves
// as if it were part of a non-local query.
type fakeExprContext struct{}

var _ ExprContext = fakeExprContext{}

func (fakeExprContext) EvalContext() *tree.EvalContext {
	return &tree.EvalContext{}
}

func (fakeExprContext) IsLocal() bool {
	return false
}

func (fakeExprContext) EvaluateSubqueries() bool {
	return true
}

func (fakeExprContext) IsTs() bool {
	return false
}

// MakeExpression creates a execinfrapb.Expression.
//
// The execinfrapb.Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
//
// ctx can be nil in which case a fakeExprCtx will be used.
func MakeExpression(
	expr tree.TypedExpr, ctx ExprContext, indexVarMap []int, canLocal bool, execInTSEngine bool,
) (execinfrapb.Expression, error) {
	if expr == nil {
		return execinfrapb.Expression{}, nil
	}
	if ctx == nil {
		ctx = &fakeExprContext{}
	}

	if ctx.IsLocal() && canLocal {
		if indexVarMap != nil {
			// Remap our indexed vars.
			expr = sqlbase.RemapIVarsInTypedExpr(expr, indexVarMap)
		}
		return execinfrapb.Expression{LocalExpr: expr}, nil
	}

	evalCtx := ctx.EvalContext()
	subqueryVisitor := &evalAndReplaceSubqueryVisitor{
		evalCtx: evalCtx,
	}

	outExpr := expr.(tree.Expr)
	if ctx.EvaluateSubqueries() {
		outExpr, _ = tree.WalkExpr(subqueryVisitor, expr)
		if subqueryVisitor.err != nil {
			return execinfrapb.Expression{}, subqueryVisitor.err
		}
	}
	// We format the expression using the IndexedVar and Placeholder formatting interceptors.
	fmtCtx := execinfrapb.ExprFmtCtxBase(evalCtx)
	fmtCtx.ExecInTSEngine = ctx.IsTs() && execInTSEngine
	if indexVarMap != nil {
		fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			remappedIdx := indexVarMap[idx]
			if remappedIdx < 0 {
				panic(fmt.Sprintf("unmapped index %d", idx))
			}
			ctx.Printf("@%d", remappedIdx+1)
		})
	}
	fmtCtx.FormatNode(outExpr)
	fmtCtx.ExecInTSEngine = false
	if log.V(1) {
		log.Infof(evalCtx.Ctx(), "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(outExpr))
	}
	return execinfrapb.Expression{Expr: fmtCtx.CloseAndGetString()}, nil
}

// optimizeTSExpression optimize execinfrapb.Expression for tsReader.
// change case any as null , ts engine parse not support cast null as type
func optimizeTSExpression(expr tree.Expr) tree.Expr {
	switch src := expr.(type) {
	case *tree.CastExpr:
		expr = src.ChangeToNull()
	case *tree.OrExpr:
		src.Left = optimizeTSExpression(src.Left)
		src.Right = optimizeTSExpression(src.Right)
	case *tree.AndExpr:
		src.Left = optimizeTSExpression(src.Left)
		src.Right = optimizeTSExpression(src.Right)
	case *tree.ComparisonExpr:
		src.Left = optimizeTSExpression(src.Left)
		src.Right = optimizeTSExpression(src.Right)
	case *tree.BinaryExpr:
		src.Left = optimizeTSExpression(src.Left)
		src.Right = optimizeTSExpression(src.Right)
	}

	return expr
}

// MakeTSExpressionForArray creates a ts engine expr string for ts engine, ts engine not support tree.expr struct
func MakeTSExpressionForArray(
	expr []tree.TypedExpr, ctx ExprContext, indexVarMap []int,
) (execinfrapb.Expression, error) {
	var cond tree.TypedExpr
	for _, v := range expr {
		if cond == nil {
			cond = v
		} else {
			cond = tree.NewTypedAndExpr(cond, v)
		}
	}

	return MakeTSExpression(cond, ctx, indexVarMap)
}

// MakeTSExpression creates a execinfrapb.Expression for ts engine
func MakeTSExpression(
	exprOrig tree.TypedExpr, ctx ExprContext, indexVarMap []int,
) (execinfrapb.Expression, error) {
	if exprOrig == nil {
		return execinfrapb.Expression{}, nil
	}
	if ctx == nil {
		ctx = &fakeExprContext{}
	}

	evalCtx := ctx.EvalContext()
	subqueryVisitor := &evalAndReplaceSubqueryVisitor{
		evalCtx: evalCtx,
	}

	outExpr := exprOrig.(tree.Expr)
	if ctx.EvaluateSubqueries() {
		outExpr, _ = tree.WalkExpr(subqueryVisitor, exprOrig)
		if subqueryVisitor.err != nil {
			return execinfrapb.Expression{}, subqueryVisitor.err
		}
	}

	// change cast(null) to null
	outExpr = optimizeTSExpression(outExpr)

	// We format the expression using the IndexedVar and Placeholder formatting interceptors.
	fmtCtx := execinfrapb.ExprFmtCtxBase(evalCtx)
	fmtCtx.ExecInTSEngine = true
	if indexVarMap != nil {
		fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			remappedIdx := indexVarMap[idx]
			if remappedIdx < 0 {
				panic(fmt.Sprintf("unmapped index %d", idx))
			}
			ctx.Printf("@%d", remappedIdx+1)
		})
	}
	fmtCtx.FormatNode(outExpr)
	fmtCtx.ExecInTSEngine = false
	if log.V(1) {
		log.Infof(evalCtx.Ctx(), "Expr %s:\n%s", fmtCtx.String(), tree.ExprDebugString(outExpr))
	}
	return execinfrapb.Expression{Expr: fmtCtx.CloseAndGetString()}, nil
}

type evalAndReplaceSubqueryVisitor struct {
	evalCtx *tree.EvalContext
	err     error
}

var _ tree.Visitor = &evalAndReplaceSubqueryVisitor{}

func (e *evalAndReplaceSubqueryVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch expr := expr.(type) {
	case *tree.Subquery:
		val, err := e.evalCtx.Planner.EvalSubquery(expr)
		if err != nil {
			e.err = err
			return false, expr
		}
		var newExpr tree.Expr = val
		if _, isTuple := val.(*tree.DTuple); !isTuple && expr.ResolvedType().Family() != types.UnknownFamily {
			newExpr = &tree.CastExpr{
				Expr: val,
				Type: expr.ResolvedType(),
			}
		}
		return false, newExpr
	default:
		return true, expr
	}
}

func (evalAndReplaceSubqueryVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
