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

package optbuilder

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// check whether expr convert const and set IsConstForLogicPlan
func (b *Builder) setConstDeductionEnabled(scalar opt.ScalarExpr, flag bool) opt.ScalarExpr {
	if flag {
		scalar.SetConstDeductionEnabled(flag)
		if f, ok1 := scalar.(*memo.FunctionExpr); ok1 {
			if _, ok2 := constraint.ConstScalarWhitelist[f.FunctionPrivate.Name]; !ok2 {
				ivh := tree.MakeIndexedVarHelper(nil /* container */, 0)
				tExpr, err := execbuilder.BuildScalarByExpr(scalar, &ivh, b.evalCtx)
				if _, ok := tExpr.(tree.Datum); !ok || err != nil {
					scalar.SetConstDeductionEnabled(false)
				}
			}
		}
	}
	return scalar
}

// buildScalar builds a set of memo groups that represent the given scalar
// expression. If outScope is not nil, then this is a projection context, and
// the resulting memo group will be projected as the output column outCol.
// Otherwise, the memo group is part of a larger expression that is not bound
// to a column.
//
// colRefs is the set of columns referenced so far by the scalar expression
// being built. If not nil, it is updated with any columns seen in
// finishBuildScalarRef.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScalar(
	scalar tree.TypedExpr, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {
	// If we are in a grouping context and this expression corresponds to a
	// GROUP BY expression, return a reference to the GROUP BY column.
	// Note that GROUP BY columns cannot be reused inside an aggregate input
	// expression (when inAgg=true) because the aggregate input expressions and
	// grouping expressions are built as part of the same projection.
	inGroupingContext := inScope.inGroupingContext() && !inScope.inAgg &&
		!inScope.groupby.buildingGroupingCols
	if inGroupingContext {
		// TODO(rytaft): This currently regenerates a string for each subexpression.
		// Change this to generate the string once for the top-level expression and
		// check the relevant slice for this subexpression.
		if col, ok := inScope.groupby.groupStrs[symbolicExprStr(scalar)]; ok {
			// We pass aggOutScope as the input scope because it contains all of
			// the aggregates and grouping columns that are available for projection.
			// finishBuildScalarRef wraps projected columns in a variable expression
			// with a new column ID if they are not contained in the input scope, so
			// passing in aggOutScope ensures we don't create new column IDs when not
			// necessary.
			if inScope.AggExHelper.bType == buildProjection && inScope.CanApplyAggExtend(nil) {
				if c, ok := scalar.(*scopeColumn); ok {
					inScope.AggExHelper.extendColSet.Add(c.id)
				}
			}
			return b.finishBuildScalarRef(col, inScope.groupby.aggOutScope, outScope, outCol, colRefs)
		}
	}

	switch t := scalar.(type) {
	case *scopeColumn:
		if inGroupingContext {
			// Non-grouping column was referenced. Note that a column that is part
			// of a larger grouping expression would have been detected by the
			// groupStrs checking code above.
			// Normally this would be a "column must appear in the GROUP BY clause"
			// error. The only cases where we allow this (for compatibility with
			// Postgres) is when this column is an outer column (and therefore
			// effectively constant) or it is part of a table and we are already
			// grouping on the entire PK of that table.
			g := inScope.groupby
			canApplyAggExtend := inScope.CanApplyAggExtend(nil)
			if !inScope.isOuterColumn(t.id) && !b.allowImplicitGroupingColumn(t.id, g) {
				if inScope.AggExHelper.bType == buildProjection {
					// 1. can apply agg extend, not need report error, eg:
					// select last(ts), e1,max(e2) from test.t
					// 2. time_window used with first(ts) or last(ts), not need report error,
					// because projection will add time_window_start(ts) or time_window_end(ts)
					// eg: select last(ts),avg(e2) from test.t group by tag1, time_window(ts, '10min')
					if !canApplyAggExtend {
						panic(newGroupingError(&t.name))
					} else {
						inScope.AggExHelper.extendColSet.Add(t.id)
					}
				} else {
					panic(newGroupingError(&t.name))
				}
			}

			if !canApplyAggExtend {
				// We add a new grouping column; these show up both in aggInScope and
				// aggOutScope.
				//
				// Note that normalization rules will trim down the list of grouping
				// columns based on FDs, so this is only for the purposes of building a
				// valid operator.
				aggInCol := b.addColumn(g.aggInScope, "" /* alias */, t, false)
				b.finishBuildScalarRef(t, inScope, g.aggInScope, aggInCol, nil)
				g.groupStrs[symbolicExprStr(t)] = aggInCol

				g.aggOutScope.appendColumn(aggInCol)

				res := b.finishBuildScalarRef(t, inScope, outScope, outCol, colRefs)
				if res != nil {
					res.SetConstDeductionEnabled(false)
				}
				return res
			}
		}
		res := b.finishBuildScalarRef(t, inScope, outScope, outCol, colRefs)
		if res != nil {
			res.SetConstDeductionEnabled(false)
		}
		return res

	case *aggregateInfo:
		var aggOutScope *scope
		if inScope.groupby != nil {
			aggOutScope = inScope.groupby.aggOutScope
		}
		return b.finishBuildScalarRef(t.col, aggOutScope, outScope, outCol, colRefs)

	case *windowInfo:
		return b.finishBuildScalarRef(t.col, inScope, outScope, outCol, colRefs)

	case *tree.AndExpr:
		left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
		right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)
		out = b.factory.ConstructAnd(left, right)
		if left.CheckConstDeductionEnabled() && right.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.Array:
		els := make(memo.ScalarListExpr, len(t.Exprs))
		arrayType := t.ResolvedType()
		elementType := arrayType.ArrayContents()
		if err := types.CheckArrayElementType(elementType); err != nil {
			panic(err)
		}
		flag := true
		for i := range t.Exprs {
			texpr := t.Exprs[i].(tree.TypedExpr)
			els[i] = b.buildScalar(texpr, inScope, nil, nil, colRefs)
			if flag {
				flag = els[i].CheckConstDeductionEnabled()
			}
		}
		out = b.factory.ConstructArray(els, arrayType)
		out = b.setConstDeductionEnabled(out, flag)

	case *tree.CollateExpr:
		in := b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructCollate(in, t.Locale)
		out = b.setConstDeductionEnabled(out, in.CheckConstDeductionEnabled())

	case *tree.ArrayFlatten:
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(t)
			break
		}

		s := t.Subquery.(*subquery)

		inCol := s.cols[0].id

		// This looks kind of arbitrary and strange, because it is:
		// We cannot array_agg over some types, but we can only decorrelate via array_agg.
		// Thus, we reject a query that is correlated and over a type that we can't array_agg.
		typ := b.factory.Metadata().ColumnMeta(inCol).Type
		if !s.outerCols.Empty() && !memo.AggregateOverloadExists(opt.ArrayAggOp, typ) {
			panic(unimplementedWithIssueDetailf(35710, "", "can't execute a correlated ARRAY(...) over %s", typ))
		}

		if err := types.CheckArrayElementType(typ); err != nil {
			panic(err)
		}

		// Perform correctness checks on the outer cols, update colRefs and
		// b.subquery.outerCols.
		b.checkSubqueryOuterCols(s.outerCols, inGroupingContext, inScope, colRefs)

		subqueryPrivate := memo.SubqueryPrivate{
			OriginalExpr: s.Subquery,
			Ordering:     s.ordering,
			RequestedCol: inCol,
		}
		out = b.factory.ConstructArrayFlatten(s.node, &subqueryPrivate)

	case *tree.IndirectionExpr:
		expr := b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)

		if len(t.Indirection) != 1 {
			panic(unimplementedWithIssueDetailf(32552, "ind", "multidimensional indexing is not supported"))
		}

		subscript := t.Indirection[0]
		if subscript.Slice {
			panic(unimplementedWithIssueDetailf(32551, "", "array slicing is not supported"))
		}

		out = b.factory.ConstructIndirection(
			expr,
			b.buildScalar(subscript.Begin.(tree.TypedExpr), inScope, nil, nil, colRefs),
		)

	case *tree.IfErrExpr:
		cond := b.buildScalar(t.Cond.(tree.TypedExpr), inScope, nil, nil, colRefs)

		orElse := memo.EmptyScalarListExpr
		if t.Else != nil {
			orElse = memo.ScalarListExpr{
				b.buildScalar(t.Else.(tree.TypedExpr), inScope, nil, nil, colRefs),
			}
		}

		errCode := memo.EmptyScalarListExpr
		if t.ErrCode != nil {
			errCode = memo.ScalarListExpr{
				b.buildScalar(t.ErrCode.(tree.TypedExpr), inScope, nil, nil, colRefs),
			}
		}
		out = b.factory.ConstructIfErr(cond, orElse, errCode)
		if cond.CheckConstDeductionEnabled() && orElse.CheckConstDeductionEnabled() && errCode.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}
	case *tree.AssignmentExpr:
		leftRes := b.buildScalar(t.Left.(tree.TypedExpr), inScope, nil, nil, colRefs)
		rightRes := b.buildScalar(t.Right.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructAssignment(leftRes, rightRes)
		if out.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}
	case *tree.BinaryExpr:
		// It's possible for an overload to be selected that expects different
		// types than the TypedExpr arguments return:
		//
		//   ARRAY[1, 2] || NULL
		//
		// This is a tricky case, because the type checker selects []int as the
		// type of the right argument, but then types it as unknown. This causes
		// issues for the execbuilder, which doesn't have enough information to
		// select the right overload. The solution is to wrap any mismatched
		// arguments with a CastExpr that preserves the static type.

		left := tree.ReType(t.TypedLeft(), t.ResolvedBinOp().LeftType)
		right := tree.ReType(t.TypedRight(), t.ResolvedBinOp().RightType)
		leftRes := b.buildScalar(left, inScope, nil, nil, colRefs)
		rightRes := b.buildScalar(right, inScope, nil, nil, colRefs)
		out = b.constructBinary(t.Operator,
			leftRes,
			rightRes,
			t.ResolvedType(),
		)
		if leftRes.CheckConstDeductionEnabled() && rightRes.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.CaseExpr:
		var input opt.ScalarExpr
		if t.Expr != nil {
			texpr := t.Expr.(tree.TypedExpr)
			input = b.buildScalar(texpr, inScope, nil, nil, colRefs)
		} else {
			input = memo.TrueSingleton
		}

		whens := make(memo.ScalarListExpr, 0, len(t.Whens)+1)
		for i := range t.Whens {
			texpr := t.Whens[i].Cond.(tree.TypedExpr)
			cond := b.buildScalar(texpr, inScope, nil, nil, colRefs)
			texpr = t.Whens[i].Val.(tree.TypedExpr)
			val := b.buildScalar(texpr, inScope, nil, nil, colRefs)
			whens = append(whens, b.factory.ConstructWhen(cond, val))
		}
		// Add the ELSE expression to the end of whens as a raw scalar expression.
		var orElse opt.ScalarExpr
		if t.Else != nil {
			texpr := t.Else.(tree.TypedExpr)
			orElse = b.buildScalar(texpr, inScope, nil, nil, colRefs)
		} else {
			orElse = memo.NullSingleton
		}
		out = b.factory.ConstructCase(input, whens, orElse)
		if input.CheckConstDeductionEnabled() && whens.CheckConstDeductionEnabled() && orElse.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.CastExpr:
		texpr := t.Expr.(tree.TypedExpr)
		arg := b.buildScalar(texpr, inScope, nil, nil, colRefs)
		out = b.factory.ConstructCast(arg, t.Type)
		// if CastExpr can convert ConstExpr, set flag to true, else set flag to false
		if _, ok := out.(*memo.ConstExpr); ok {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.CoalesceExpr:
		args := make(memo.ScalarListExpr, len(t.Exprs))
		flag := true
		for i := range args {
			args[i] = b.buildScalar(t.TypedExprAt(i), inScope, nil, nil, colRefs)
			if flag {
				flag = args[i].CheckConstDeductionEnabled()
			}
		}
		out = b.factory.ConstructCoalesce(args)
		out = b.setConstDeductionEnabled(out, flag)

	case *tree.ColumnAccessExpr:
		input := b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructColumnAccess(input, memo.TupleOrdinal(t.ColIndex))
		out = b.setConstDeductionEnabled(out, input.CheckConstDeductionEnabled())

	case *tree.ComparisonExpr:
		if sub, ok := t.Right.(*subquery); ok && sub.isMultiRow() {
			out, _ = b.buildMultiRowSubquery(t, inScope, colRefs)
			// Perform correctness checks on the outer cols, update colRefs and
			// b.subquery.outerCols.
			b.checkSubqueryOuterCols(sub.outerCols, inGroupingContext, inScope, colRefs)
		} else if b.hasSubOperator(t) {
			// Cases where the RHS is a multi-row subquery were handled above, so this
			// only handles explicit tuples and arrays.
			out = b.buildAnyScalar(t, inScope, colRefs)
		} else {
			left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
			right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)
			out = b.constructComparison(t.Operator, left, right)
			if left.CheckConstDeductionEnabled() && right.CheckConstDeductionEnabled() {
				out = b.setConstDeductionEnabled(out, true)
			} else {
				out.SetConstDeductionEnabled(false)
			}
		}

	case *tree.DTuple:
		els := make(memo.ScalarListExpr, len(t.D))
		flag := true
		for i := range t.D {
			els[i] = b.buildScalar(t.D[i], inScope, nil, nil, colRefs)
			if flag {
				flag = els[i].CheckConstDeductionEnabled()
			}
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())
		out = b.setConstDeductionEnabled(out, flag)

	case *tree.FuncExpr:
		return b.buildFunction(t, inScope, outScope, outCol, colRefs)

	case *tree.IfExpr:
		input := b.buildScalar(t.Cond.(tree.TypedExpr), inScope, nil, nil, colRefs)
		ifTrue := b.buildScalar(t.True.(tree.TypedExpr), inScope, nil, nil, colRefs)
		whens := memo.ScalarListExpr{b.factory.ConstructWhen(memo.TrueSingleton, ifTrue)}
		orElse := b.buildScalar(t.Else.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructCase(input, whens, orElse)
		if input.CheckConstDeductionEnabled() && whens.CheckConstDeductionEnabled() && orElse.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.IndexedVar:
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"invalid column ordinal: @%d", t.Idx+1))
		}
		out = b.factory.ConstructVariable(inScope.cols[t.Idx].id)
		out.SetConstDeductionEnabled(false)

	case *tree.NotExpr:
		input := b.buildScalar(t.TypedInnerExpr(), inScope, nil, nil, colRefs)
		out = b.factory.ConstructNot(input)
		out = b.setConstDeductionEnabled(out, input.CheckConstDeductionEnabled())

	case *tree.NullIfExpr:
		// Ensure that the type of the first expression matches the resolved type
		// of the NULLIF expression so that type inference will be correct in the
		// CASE expression constructed below. For example, the type of
		// NULLIF(NULL, 0) should be int.
		expr1 := tree.ReType(t.Expr1.(tree.TypedExpr), t.ResolvedType())
		input := b.buildScalar(expr1, inScope, nil, nil, colRefs)
		cond := b.buildScalar(t.Expr2.(tree.TypedExpr), inScope, nil, nil, colRefs)
		whens := memo.ScalarListExpr{b.factory.ConstructWhen(cond, memo.NullSingleton)}
		out = b.factory.ConstructCase(input, whens, input)
		if input.CheckConstDeductionEnabled() && whens.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.OrExpr:
		left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
		right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)
		out = b.factory.ConstructOr(left, right)
		if left.CheckConstDeductionEnabled() && right.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *tree.ParenExpr:
		// Treat ParenExpr as if it wasn't present.
		return b.buildScalar(t.TypedInnerExpr(), inScope, outScope, outCol, colRefs)

	case *tree.Placeholder:
		if !b.KeepPlaceholders && b.evalCtx.HasPlaceholders() {
			b.HadPlaceholders = true
			// Replace placeholders with their value.
			d, err := t.Eval(b.evalCtx)
			if err != nil {
				panic(err)
			}
			out = b.factory.ConstructConstVal(d, t.ResolvedType())
		} else {
			out = b.factory.ConstructPlaceholder(t)
		}

	case *tree.RangeCond:
		inputFrom := b.buildScalar(t.TypedLeftFrom(), inScope, nil, nil, colRefs)
		from := b.buildScalar(t.TypedFrom(), inScope, nil, nil, colRefs)
		inputTo := b.buildScalar(t.TypedLeftTo(), inScope, nil, nil, colRefs)
		to := b.buildScalar(t.TypedTo(), inScope, nil, nil, colRefs)
		out = b.buildRangeCond(t.Not, t.Symmetric, inputFrom, from, inputTo, to)
		if inputFrom.CheckConstDeductionEnabled() && from.CheckConstDeductionEnabled() &&
			inputTo.CheckConstDeductionEnabled() && to.CheckConstDeductionEnabled() {
			out = b.setConstDeductionEnabled(out, true)
		} else {
			out.SetConstDeductionEnabled(false)
		}

	case *srf:
		if len(t.cols) == 1 {
			if inGroupingContext {
				// Non-grouping column was referenced. Note that a column that is part
				// of a larger grouping expression would have been detected by the
				// groupStrs checking code above.
				panic(newGroupingError(&t.cols[0].name))
			}
			return b.finishBuildScalarRef(&t.cols[0], inScope, outScope, outCol, colRefs)
		}
		els := make(memo.ScalarListExpr, len(t.cols))
		// check whether if col is not convert const, flag is false
		flag := true
		for i := range t.cols {
			els[i] = b.buildScalar(&t.cols[i], inScope, nil, nil, colRefs)
			if flag {
				flag = els[i].CheckConstDeductionEnabled()
			}
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())
		out = b.setConstDeductionEnabled(out, flag)

	case *subquery:
		out, _ = b.buildSingleRowSubquery(t, inScope)
		// Perform correctness checks on the outer cols, update colRefs and
		// b.subquery.outerCols.
		b.checkSubqueryOuterCols(t.outerCols, inGroupingContext, inScope, colRefs)

	case *tree.Tuple:
		els := make(memo.ScalarListExpr, len(t.Exprs))
		flag := true
		for i := range t.Exprs {
			els[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope, nil, nil, colRefs)
			if flag {
				flag = els[i].CheckConstDeductionEnabled()
			}
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())
		out = b.setConstDeductionEnabled(out, flag)

	case *tree.UnaryExpr:
		input := b.buildScalar(t.TypedInnerExpr(), inScope, nil, nil, colRefs)
		out = b.constructUnary(t.Operator, input, t.ResolvedType())
		out = b.setConstDeductionEnabled(out, input.CheckConstDeductionEnabled())

	case *tree.IsOfTypeExpr:
		// IsOfTypeExpr is a little strange because its value can be determined
		// statically just from the type of the expression.
		actualType := t.Expr.(tree.TypedExpr).ResolvedType()

		found := false
		for _, typ := range t.Types {
			if actualType.Equivalent(typ) {
				found = true
				break
			}
		}

		if found != t.Not {
			out = b.factory.ConstructTrue()
		} else {
			out = b.factory.ConstructFalse()
		}
		out = b.setConstDeductionEnabled(out, true)

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		out = b.factory.ConstructConstVal(t, t.ResolvedType())
		out = b.setConstDeductionEnabled(out, true)

	case *tree.UserDefinedVar:
		b.DisableMemoReuse = true
		varName := strings.ToLower(t.VarName)
		if b.evalCtx.SessionData.UserDefinedVars[varName] == nil {
			panic(pgerror.Newf(pgcode.UndefinedObject, "%s is not defined", t.VarName))
		}
		d := b.evalCtx.SessionData.UserDefinedVars[varName].(tree.Datum)
		out = b.factory.ConstructConstVal(d, d.ResolvedType())
		out = b.setConstDeductionEnabled(out, true)

	default:
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(scalar)
		} else {
			panic(unimplemented.Newf(fmt.Sprintf("optbuilder.%T", scalar), "not yet implemented: scalar expression: %T", scalar))
		}
	}

	return b.finishBuildScalar(scalar, out, inScope, outScope, outCol)
}

func (b *Builder) hasSubOperator(t *tree.ComparisonExpr) bool {
	return t.Operator == tree.Any || t.Operator == tree.All || t.Operator == tree.Some
}

func (b *Builder) buildAnyScalar(
	t *tree.ComparisonExpr, inScope *scope, colRefs *opt.ColSet,
) opt.ScalarExpr {
	left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
	right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)

	subop := opt.ComparisonOpMap[t.SubOperator]

	if t.Operator == tree.All {
		subop = opt.NegateOpMap[subop]
	}

	out := b.factory.ConstructAnyScalar(left, right, subop)
	if t.Operator == tree.All {
		out = b.factory.ConstructNot(out)
	}
	return out
}

// buildFunction builds a set of memo groups that represent a function
// expression.
//
// f        The given function expression.
// outCol   The output column of the function being built.
// colRefs  The set of columns referenced so far by the scalar expression
//
//	being built. If not nil, it is updated with any columns seen in
//	finishBuildScalarRef.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFunction(
	f *tree.FuncExpr, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {
	if f.WindowDef != nil {
		if inScope.inAgg {
			panic(sqlbase.NewWindowInAggError())
		}
	}

	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(err)
	}

	if isAggregate(def) {
		panic(errors.AssertionFailedf("aggregate function should have been replaced, illegal function: %v", def.Name))
	}

	if isWindow(def) {
		panic(errors.AssertionFailedf("window function should have been replaced, illegal function: %v", def.Name))
	}

	args := make(memo.ScalarListExpr, len(f.Exprs))
	isConstForLogicPlanArg := true
	for i, pexpr := range f.Exprs {
		args[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope, nil, nil, colRefs)
		if isConstForLogicPlanArg {
			isConstForLogicPlanArg = checkScalarIsConst(args[i])
		}
	}

	// Construct a private FuncOpDef that refers to a resolved function overload.
	out = b.factory.ConstructFunction(args, &memo.FunctionPrivate{
		Name:       def.Name,
		Typ:        f.ResolvedType(),
		Properties: &def.FunctionProperties,
		Overload:   f.ResolvedOverload(),
	})

	out = b.setConstDeductionEnabled(out, isConstForLogicPlanArg)

	if isGenerator(def) {
		return b.finishBuildGeneratorFunction(f, out, inScope, outScope, outCol)
	}

	return b.finishBuildScalar(f, out, inScope, outScope, outCol)
}

// check IsConstForLogicPlan
func checkScalarIsConst(expr opt.ScalarExpr) bool {
	switch e := expr.(type) {
	case *memo.ConstExpr:
		return true
	default:
		return e.CheckConstDeductionEnabled()
	}
	return false
}

// buildRangeCond builds a RANGE clause as a simpler expression. Examples:
// x BETWEEN a AND b                ->  x >= a AND x <= b
// x NOT BETWEEN a AND b            ->  NOT (x >= a AND x <= b)
// x BETWEEN SYMMETRIC a AND b      ->  (x >= a AND x <= b) OR (x >= b AND x <= a)
// x NOT BETWEEN SYMMETRIC a AND b  ->  NOT ((x >= a AND x <= b) OR (x >= b AND x <= a))
//
// Note that x can be typed differently in the expressions (x >= a) and (x <= b)
// because a and b can have different types; the function takes both "variants"
// of x.
//
// Note that these expressions are subject to normalization rules (which can
// push down the negation).
// TODO(radu): this doesn't work when the expressions have side-effects.
func (b *Builder) buildRangeCond(
	not bool, symmetric bool, inputFrom, from, inputTo, to opt.ScalarExpr,
) opt.ScalarExpr {
	// Build "input >= from AND input <= to".
	out := b.factory.ConstructAnd(
		b.factory.ConstructGe(inputFrom, from),
		b.factory.ConstructLe(inputTo, to),
	)

	if symmetric {
		// Build "(input >= from AND input <= to) OR (input >= to AND input <= from)".
		lhs := out
		rhs := b.factory.ConstructAnd(
			b.factory.ConstructGe(inputTo, to),
			b.factory.ConstructLe(inputFrom, from),
		)
		out = b.factory.ConstructOr(lhs, rhs)
	}

	if not {
		out = b.factory.ConstructNot(out)
	}
	return out
}

// checkSubqueryOuterCols uses the subquery outer columns to update the given
// set of column references and the set of outer columns for any enclosing
// subuqery. It also performs the following checks:
//  1. If aggregates are not allowed in the current context (e.g., if we
//     are building the WHERE clause), it checks that the subquery does not
//     reference any aggregates from this scope.
//  2. If this is a grouping context, it checks that any outer columns from
//     the given subquery that reference inScope are either aggregate or
//     grouping columns in inScope.
func (b *Builder) checkSubqueryOuterCols(
	subqueryOuterCols opt.ColSet, inGroupingContext bool, inScope *scope, colRefs *opt.ColSet,
) {
	if subqueryOuterCols.Empty() {
		return
	}

	// Register the use of correlation to telemetry.
	// Note: we don't blindly increment the counter every time this
	// method is called, to avoid double counting the same query.
	if !b.isCorrelated {
		b.isCorrelated = true
		telemetry.Inc(sqltelemetry.CorrelatedSubqueryUseCounter)
	}

	var inScopeCols opt.ColSet
	if b.subquery != nil || inGroupingContext {
		// Only calculate the set of inScope columns if it will be used below.
		inScopeCols = inScope.colSet()
	}

	if colRefs != nil {
		colRefs.UnionWith(subqueryOuterCols)
	}
	if b.subquery != nil {
		b.subquery.outerCols.UnionWith(subqueryOuterCols.Difference(inScopeCols))
	}

	// Check 1 (see function comment).
	if b.semaCtx.Properties.IsSet(tree.RejectAggregates) && inScope.groupby != nil {
		aggCols := inScope.groupby.aggregateResultCols()
		for i := range aggCols {
			if subqueryOuterCols.Contains(aggCols[i].id) {
				panic(tree.NewInvalidFunctionUsageError(tree.AggregateClass, inScope.context.String()))
			}
		}
	}

	// Check 2 (see function comment).
	if inGroupingContext {
		subqueryOuterCols.IntersectionWith(inScopeCols)
		if !subqueryOuterCols.Empty() &&
			!subqueryOuterCols.SubsetOf(inScope.groupby.aggOutScope.colSet()) {
			subqueryOuterCols.DifferenceWith(inScope.groupby.aggOutScope.colSet())
			colID, _ := subqueryOuterCols.Next(0)
			col := inScope.getColumn(colID)
			panic(pgerror.Newf(
				pgcode.Grouping,
				"subquery uses ungrouped column \"%s\" from outer query",
				tree.ErrString(&col.name)))
		}
	}
}

func (b *Builder) constructComparison(
	cmp tree.ComparisonOperator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	switch cmp {
	case tree.EQ:
		return b.factory.ConstructEq(left, right)
	case tree.LT:
		return b.factory.ConstructLt(left, right)
	case tree.GT:
		return b.factory.ConstructGt(left, right)
	case tree.LE:
		return b.factory.ConstructLe(left, right)
	case tree.GE:
		return b.factory.ConstructGe(left, right)
	case tree.NE:
		return b.factory.ConstructNe(left, right)
	case tree.In:
		return b.factory.ConstructIn(left, right)
	case tree.NotIn:
		return b.factory.ConstructNotIn(left, right)
	case tree.Like:
		return b.factory.ConstructLike(left, right)
	case tree.NotLike:
		return b.factory.ConstructNotLike(left, right)
	case tree.ILike:
		return b.factory.ConstructILike(left, right)
	case tree.NotILike:
		return b.factory.ConstructNotILike(left, right)
	case tree.SimilarTo:
		return b.factory.ConstructSimilarTo(left, right)
	case tree.NotSimilarTo:
		return b.factory.ConstructNotSimilarTo(left, right)
	case tree.RegMatch:
		return b.factory.ConstructRegMatch(left, right)
	case tree.NotRegMatch:
		return b.factory.ConstructNotRegMatch(left, right)
	case tree.RegIMatch:
		return b.factory.ConstructRegIMatch(left, right)
	case tree.NotRegIMatch:
		return b.factory.ConstructNotRegIMatch(left, right)
	case tree.IsDistinctFrom:
		return b.factory.ConstructIsNot(left, right)
	case tree.IsNotDistinctFrom:
		return b.factory.ConstructIs(left, right)
	case tree.Contains:
		return b.factory.ConstructContains(left, right)
	case tree.ContainedBy:
		// This is just syntatic sugar that reverses the operands.
		return b.factory.ConstructContains(right, left)
	case tree.JSONExists:
		return b.factory.ConstructJsonExists(left, right)
	case tree.JSONAllExists:
		return b.factory.ConstructJsonAllExists(left, right)
	case tree.JSONSomeExists:
		return b.factory.ConstructJsonSomeExists(left, right)
	case tree.Overlaps:
		return b.factory.ConstructOverlaps(left, right)
	}
	panic(errors.AssertionFailedf("unhandled comparison operator: %s", log.Safe(cmp)))
}

func (b *Builder) constructBinary(
	bin tree.BinaryOperator, left, right opt.ScalarExpr, typ *types.T,
) opt.ScalarExpr {
	switch bin {
	case tree.Bitand:
		return b.factory.ConstructBitand(left, right)
	case tree.Bitor:
		return b.factory.ConstructBitor(left, right)
	case tree.Bitxor:
		return b.factory.ConstructBitxor(left, right)
	case tree.Plus:
		return b.factory.ConstructPlus(left, right)
	case tree.Minus:
		return b.factory.ConstructMinus(left, right)
	case tree.Mult:
		return b.factory.ConstructMult(left, right)
	case tree.Div:
		return b.factory.ConstructDiv(left, right)
	case tree.FloorDiv:
		return b.factory.ConstructFloorDiv(left, right)
	case tree.Mod:
		return b.factory.ConstructMod(left, right)
	case tree.Pow:
		return b.factory.ConstructPow(left, right)
	case tree.Concat:
		return b.factory.ConstructConcat(left, right)
	case tree.LShift:
		return b.factory.ConstructLShift(left, right)
	case tree.RShift:
		return b.factory.ConstructRShift(left, right)
	case tree.JSONFetchText:
		return b.factory.ConstructFetchText(left, right)
	case tree.JSONFetchVal:
		return b.factory.ConstructFetchVal(left, right)
	case tree.JSONFetchValPath:
		return b.factory.ConstructFetchValPath(left, right)
	case tree.JSONFetchTextPath:
		return b.factory.ConstructFetchTextPath(left, right)
	}
	panic(errors.AssertionFailedf("unhandled binary operator: %s", log.Safe(bin)))
}

func (b *Builder) constructUnary(
	un tree.UnaryOperator, input opt.ScalarExpr, typ *types.T,
) opt.ScalarExpr {
	switch un {
	case tree.UnaryMinus:
		return b.factory.ConstructUnaryMinus(input)
	case tree.UnaryComplement:
		return b.factory.ConstructUnaryComplement(input)
	}
	panic(errors.AssertionFailedf("unhandled unary operator: %s", log.Safe(un)))
}

// ScalarBuilder is a specialized variant of Builder that can be used to create
// a scalar from a TypedExpr. This is used to build scalar expressions for
// testing. It is also used temporarily to interface with the old planning code.
//
// TypedExprs can refer to columns in the current scope using IndexedVars (@1,
// @2, etc). When we build a scalar, we have to provide information about these
// columns.
type ScalarBuilder struct {
	Builder
	scope scope
}

// NewScalar creates a new ScalarBuilder. The columns in the metadata are accessible
// from scalar expressions via IndexedVars.
func NewScalar(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, factory *norm.Factory,
) *ScalarBuilder {
	md := factory.Metadata()
	sb := &ScalarBuilder{
		Builder: Builder{
			factory: factory,
			ctx:     ctx,
			semaCtx: semaCtx,
			evalCtx: evalCtx,
		},
	}
	sb.scope.builder = &sb.Builder

	// Put all the columns in the current scope.
	sb.scope.cols = make([]scopeColumn, 0, md.NumColumns())
	for colID := opt.ColumnID(1); int(colID) <= md.NumColumns(); colID++ {
		colMeta := md.ColumnMeta(colID)
		sb.scope.cols = append(sb.scope.cols, scopeColumn{
			name: tree.Name(colMeta.Alias),
			typ:  colMeta.Type,
			id:   colID,
		})
	}

	return sb
}

// Build a memo structure from a TypedExpr: the root group represents a scalar
// expression equivalent to expr.
func (sb *ScalarBuilder) Build(expr tree.Expr) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	typedExpr := sb.scope.resolveType(expr, types.Any)
	scalar := sb.buildScalar(typedExpr, &sb.scope, nil, nil, nil)
	sb.factory.Memo().SetScalarRoot(scalar)
	return nil
}
