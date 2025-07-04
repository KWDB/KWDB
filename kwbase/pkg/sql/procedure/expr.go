// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package procedure

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// ExprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type ExprHelper struct {
	_ util.NoCopy

	Expr tree.TypedExpr

	evalCtx *tree.EvalContext

	// pCtx procedure context
	pCtx *SpExecContext

	// Vars are used to generate IndexedVars that are "backed" by the values in `Row`.
	Vars tree.IndexedVarHelper
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return eh.pCtx.IndexedVarEval(idx, ctx)
}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarResolvedType(idx int) *types.T {
	return eh.pCtx.IndexedVarResolvedType(idx)
}

// IndexedVarNodeFormatter is part of the parser.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return eh.pCtx.IndexedVarNodeFormatter(idx)
}

// Init initializes the ExprHelper.
func (eh *ExprHelper) Init(pCtx *SpExecContext, evalCtx *tree.EvalContext, expr tree.TypedExpr) {
	eh.evalCtx = evalCtx
	eh.Vars = tree.MakeIndexedVarHelper(eh, pCtx.GetVariableLen())
	eh.pCtx = pCtx

	eh.Expr = expr
	// Bind IndexedVars to our eh.Vars.
	eh.Vars.Rebind(eh.Expr, true /* alsoReset */, false /* normalizeToNonNil */)
}

// EvalCompare is used for compare expressions; it evaluates the expression and returns whether the filter passes.
func (eh *ExprHelper) EvalCompare() (bool, error) {
	eh.evalCtx.PushIVarContainer(eh)
	d, err := eh.Expr.Eval(eh.evalCtx)
	if err != nil {
		return false, err
	}
	eh.evalCtx.PopIVarContainer()
	succ := false
	switch s := d.(type) {
	case *tree.DBool:
		if s == tree.DBoolTrue {
			succ = true
		}
	case *tree.DInt:
		if *s > 0 {
			succ = true
		}
	}
	return succ, err
}

// Eval  given a row
func (eh *ExprHelper) Eval() (tree.Datum, error) {
	eh.evalCtx.PushIVarContainer(eh)
	d, err := eh.Expr.Eval(eh.evalCtx)
	eh.evalCtx.PopIVarContainer()
	return d, err
}
