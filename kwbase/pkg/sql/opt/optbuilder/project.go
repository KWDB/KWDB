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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// constructProjectForScope constructs a projection if it will result in a
// different set of columns than its input. Either way, it updates
// projectionsScope.group with the output memo group ID.
func (b *Builder) constructProjectForScope(inScope, projectionsScope *scope) {
	// Don't add an unnecessary "pass through" project.
	if projectionsScope.hasSameColumns(inScope) {
		projectionsScope.expr = inScope.expr
	} else {
		projectionsScope.expr = b.constructProject(
			inScope.expr.(memo.RelExpr),
			append(projectionsScope.cols, projectionsScope.extraCols...),
		)
	}
}

func (b *Builder) constructProject(input memo.RelExpr, cols []scopeColumn) memo.RelExpr {
	var passthrough opt.ColSet
	projections := make(memo.ProjectionsExpr, 0, len(cols))

	// Deduplicate the columns; we only need to project each column once.
	colSet := opt.ColSet{}
	for i := range cols {
		id, scalar := cols[i].id, cols[i].scalar
		if !colSet.Contains(id) {
			if scalar == nil {
				passthrough.Add(id)
			} else {
				projections = append(projections, b.factory.ConstructProjectionsItem(scalar, id))
			}
			colSet.Add(id)
		}
	}

	return b.factory.ConstructProject(input, projections, passthrough)
}

// constructProjectInAggExtend needs to passthrough the cols which can apply extend.
// eg: group :  max_extend(e3,e1),max(e3)
//       |
//     render :  e1,e3
//       |
//     tsscan :  ts,e1,e2,e3,...
// input always is TSSanExpr, cols are the cols need to project or passthrough, set
// is the col ids which can apply extend.
func (b *Builder) constructProjectInAggExtend(
	input memo.RelExpr, cols []scopeColumn, set opt.ColSet,
) memo.RelExpr {
	var passthrough opt.ColSet
	projections := make(memo.ProjectionsExpr, 0, len(cols))

	// Deduplicate the columns; we only need to project each column once.
	colSet := opt.ColSet{}
	for i := range cols {
		id, scalar := cols[i].id, cols[i].scalar
		if !colSet.Contains(id) {
			if scalar == nil {
				passthrough.Add(id)
			} else {
				projections = append(projections, b.factory.ConstructProjectionsItem(scalar, id))
			}
			colSet.Add(id)
		}
	}
	passthrough.UnionWith(set)

	return b.factory.ConstructProject(input, projections, passthrough)
}

// dropOrderingAndExtraCols removes the ordering in the scope and projects away
// any extra columns.
func (b *Builder) dropOrderingAndExtraCols(s *scope) {
	s.ordering = nil
	if len(s.extraCols) > 0 {
		var passthrough opt.ColSet
		for i := range s.cols {
			passthrough.Add(s.cols[i].id)
		}
		s.expr = b.factory.ConstructProject(s.expr, nil /* projections */, passthrough)
		s.extraCols = nil
	}
}

// analyzeProjectionList analyzes the given list of SELECT clause expressions,
// and adds the resulting aliases and typed expressions to outScope. See the
// header comment for analyzeSelectList.
func (b *Builder) analyzeProjectionList(
	selects *tree.SelectExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {
	// We need to save and restore the previous values of the replaceSRFs field
	// and the field in semaCtx in case we are recursively called within a
	// subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	defer func(replaceSRFs bool) { inScope.replaceSRFs = replaceSRFs }(inScope.replaceSRFs)

	b.semaCtx.Properties.Require(exprTypeSelect.String(), tree.RejectNestedGenerators)
	inScope.context = exprTypeSelect
	inScope.replaceSRFs = true

	b.analyzeSelectList(selects, desiredTypes, inScope, outScope)
}

// analyzeReturningList analyzes the given list of RETURNING clause expressions,
// and adds the resulting aliases and typed expressions to outScope. See the
// header comment for analyzeSelectList.
func (b *Builder) analyzeReturningList(
	returning tree.ReturningExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Ensure there are no special functions in the RETURNING clause.
	b.semaCtx.Properties.Require(exprTypeReturning.String(), tree.RejectSpecial)
	inScope.context = exprTypeReturning

	selectList := tree.SelectExprs(returning)
	b.analyzeSelectList(&selectList, desiredTypes, inScope, outScope)
}

func (b *Builder) findLastFunctionParam(e tree.Expr) (tree.Expr, string, string) {
	// handle CastExpr
	switch expr := e.(type) {
	case *tree.FuncExpr:
		if checkLastOrFirstAgg(expr.Func.FunctionName()) && expr.Exprs[0] != tree.DNull {
			return expr.Exprs[0], expr.Func.FunctionName(), expr.String()
		}
	case *tree.CastExpr:
		return b.findLastFunctionParam(expr.Expr)
	}
	return nil, "", ""
}

// AddLastColumn get all column expr and alias
func (b *Builder) AddLastColumn(tableName, funcName string, inScope, outScope *scope) int {
	count := 0
	// add table all column
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if string(col.table.TableName) != tableName {
			continue
		}
		if b.factory.Metadata().ColumnMeta(col.id).IsTag() {
			continue
		}
		if col.hidden {
			continue
		}

		b.addColumn(outScope, funcName+"("+string(col.name)+")",
			inScope.resolveType(&tree.FuncExpr{Func: tree.ResolvableFunctionReference{
				FunctionReference: &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{funcName}},
			}, Exprs: tree.Exprs{col}}, types.Any), false)
		count++
	}

	return count
}

// analyzeSelectList is a helper function used by analyzeProjectionList and
// analyzeReturningList. It normalizes names, expands wildcards, resolves types,
// and adds resulting columns to outScope. The desiredTypes slice contains
// target type hints for the resulting expressions.
//
// As a side-effect, the appropriate scopes are updated with aggregations
// (scope.groupby.aggs)
func (b *Builder) analyzeSelectList(
	selects *tree.SelectExprs, desiredTypes []*types.T, inScope, outScope *scope,
) {

	colIndexTmp := 0
	var expansions tree.SelectExprs
	for i := 0; i < len(*selects); i++ {
		e := (*selects)[i]
		expanded := false
		// Start with fast path, looking for simple column reference.
		texpr := b.resolveColRef(e.Expr, inScope)
		if texpr == nil {
			// Fall back to slow path. Pre-normalize any VarName so the work is
			// not done twice below.
			if err := e.NormalizeTopLevelVarName(); err != nil {
				panic(err)
			}
			var expr tree.Expr = e.Expr
			v, name, strExpr := b.findLastFunctionParam(expr)
			if v != nil {
				switch vv := v.(type) {
				case *tree.UnresolvedName:
					// deal with t1.*
					if vv.NumParts == 2 && vv.Parts[0] == "" {
						if len(inScope.cols) == 0 {
							panic(pgerror.Newf(pgcode.InvalidName,
								"cannot use %q without a FROM clause", tree.ErrString(expr)))
						}
						aliases, exprs := b.expandTableStar(vv.Parts[1], inScope)
						if len(exprs) == 0 {
							break
						}
						colIndexTmp += len(exprs)
						b.addLastFunction(aliases, exprs, name, inScope, outScope, expr)
						continue
					}
					break
				case tree.UnqualifiedStar, *tree.AllColumnsSelector, *tree.TupleStar:
					if e.As != "" {
						panic(pgerror.Newf(pgcode.Syntax, "%q cannot be aliased", strExpr))
					}
					// expand *
					aliases, exprs := b.expandStar(v, inScope, true)
					colIndexTmp += len(exprs)
					if outScope.cols == nil {
						outScope.cols = make([]scopeColumn, 0, len(*selects)+len(exprs)-1)
					}

					b.addLastFunction(aliases, exprs, name, inScope, outScope, expr)
					continue
				case *tree.ColumnItem, *tree.NumVal, *tree.StrVal: //, *tree.NumVal, *tree.dNull
					break
				default:
					panic(pgerror.Newf(pgcode.Syntax, "%q should be * or column name for last/last_row function", strExpr))
				}
			}

			// Special handling for "*", "<table>.*" and "(Expr).*".
			if v, ok := e.Expr.(tree.VarName); ok {
				switch v.(type) {
				case tree.UnqualifiedStar, *tree.AllColumnsSelector, *tree.TupleStar:
					if e.As != "" {
						panic(pgerror.Newf(pgcode.Syntax,
							"%q cannot be aliased", tree.ErrString(v)))
					}

					aliases, exprs := b.expandStar(e.Expr, inScope, false)
					if b.insideViewDef || b.insideProcDef {
						expanded = true
						for _, expr := range exprs {
							switch col := expr.(type) {
							case *scopeColumn:
								expansions = append(expansions, tree.SelectExpr{Expr: tree.NewColumnItem(&col.table, col.name)})
							case *tree.ColumnAccessExpr:
								expansions = append(expansions, tree.SelectExpr{Expr: col})
							default:
								panic(errors.AssertionFailedf("unexpected column type in expansion, illegal expr: %s", expr))
							}
						}
					}
					colIndexTmp += len(exprs)
					if outScope.cols == nil {
						outScope.cols = make([]scopeColumn, 0, len(*selects)+len(exprs)-1)
					}
					idxs := make([]int, 0)
					for j, e := range exprs {
						// b.addColumn(outScope, aliases[j], e)
						if metaCol := b.addColumn(outScope, aliases[j], e, true); metaCol == nil {
							idxs = append(idxs, j)
						}
					}
					for _, idx := range idxs {
						b.addColumn(outScope, aliases[idx], exprs[idx], false)
					}
					continue
				}
			}

			desired := types.Any
			// eg: insert into test_ts.ts_table2 select *,'a' from test_ts.ts_table;
			// colIndexTmp is the indexs of cols in select table(case *)
			// desiredTypes is types of insert cols
			// for the above example, we'll also check type of 'a' against desiredType,
			// so we need add colIndexTmp to index of desired
			if i < len(desiredTypes) {
				if colIndexTmp > 0 {
					desired = desiredTypes[i+colIndexTmp-1]
				} else {
					desired = desiredTypes[i]
				}
			}

			texpr = inScope.resolveType(e.Expr, desired)
		}

		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we perform type
		// checking.
		if outScope.cols == nil {
			outScope.cols = make([]scopeColumn, 0, len(*selects))
		}
		alias := b.getColName(e)
		if inScope.hasGapfill && alias == Gapfillinternal {
			alias = Gapfill
		}
		b.addColumn(outScope, alias, texpr, false)
		if (b.insideViewDef || b.insideProcDef) && !expanded {
			expansions = append(expansions, e)
		}
	}
	if inScope.checkAggExtendFlag(isSingleTsTable) && !inScope.checkAggExtendFlag(existOtherAgg) {
		if len(*selects) != inScope.AggExHelper.aggCount {
			inScope.setAggExtendFlag(existNonAggCol)
		}
	}
	if b.insideViewDef || b.insideProcDef {
		*selects = expansions
	}
}

// buildProjectionList builds a set of memo groups that represent the given
// expressions in projectionsScope.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildProjectionList(inScope *scope, projectionsScope *scope) {
	inScope.AggExHelper.bType = buildProjection
	for i := range projectionsScope.cols {
		col := &projectionsScope.cols[i]
		if scalar := b.buildScalar(col.getExpr(), inScope, projectionsScope, col, nil); scalar != nil {
			// group window function can be only used in groupby
			if name, ok := memo.CheckGroupWindowExist(scalar); ok {
				panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
			}
		} else if inScope.groupby != nil && len(inScope.groupby.groupStrs) > 0 {
			if groupCol, ok := inScope.groupby.groupStrs[symbolicExprStr(col.getExpr())]; ok {
				if name, ok := memo.CheckGroupWindowExist(groupCol.scalar); ok {
					panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
				}
			}
		}
	}
	inScope.AggExHelper.bType = buildNone
}

// resolveColRef looks for the common case of a standalone column reference
// expression, like this:
//
//	SELECT ..., c, ... FROM ...
//
// It resolves the column name to a scopeColumn and returns it as a TypedExpr.
func (b *Builder) resolveColRef(e tree.Expr, inScope *scope) tree.TypedExpr {
	unresolved, ok := e.(*tree.UnresolvedName)
	if ok && !unresolved.Star && unresolved.NumParts == 1 {
		colName := unresolved.Parts[0]
		_, srcMeta, _, err := inScope.FindSourceProvidingColumn(b.ctx, tree.Name(colName))
		if err != nil {
			panic(err)
		}

		return srcMeta.(tree.TypedExpr)
	}
	return nil
}

// getColName returns the output column name for a projection expression.
func (b *Builder) getColName(expr tree.SelectExpr) string {
	s, err := tree.GetRenderColName(b.semaCtx.SearchPath, expr)
	if err != nil {
		panic(err)
	}
	return s
}

// finishBuildScalar completes construction of a new scalar expression. If
// outScope is nil, then finishBuildScalar returns the result memo group, which
// can be nested within the larger expression being built. If outScope is not
// nil, then finishBuildScalar synthesizes a new output column in outScope with
// the expression as its value.
//
// texpr     The given scalar expression. The expression is any scalar
//
//	expression except for a bare variable or aggregate (those are
//	handled separately in buildVariableProjection and
//	buildFunction).
//
// scalar    The memo expression that has already been built for the given
//
//	typed expression.
//
// outCol    The output column of the scalar which is being built. It can be
//
//	nil if outScope is nil.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) finishBuildScalar(
	texpr tree.TypedExpr, scalar opt.ScalarExpr, inScope, outScope *scope, outCol *scopeColumn,
) (out opt.ScalarExpr) {
	if outScope == nil {
		return scalar
	}

	// Avoid synthesizing a new column if possible.
	if col := outScope.findExistingCol(texpr, false /* allowSideEffects */); col != nil && col != outCol {
		outCol.id = col.id
		outCol.scalar = scalar
		return scalar
	}

	b.populateSynthesizedColumn(outCol, scalar, inScope.ScopeTSProp)
	return scalar
}

// finishBuildScalarRef constructs a reference to the given column. If outScope
// is nil, then finishBuildScalarRef returns a Variable expression that refers
// to the column. This expression can be nested within the larger expression
// being constructed. If outScope is not nil, then finishBuildScalarRef adds the
// column to outScope, either as a passthrough column (if it already exists in
// the input scope), or a variable expression.
//
// col      Column containing the scalar expression that's been referenced.
// outCol   The output column which is being built. It can be nil if outScope is
//
//	nil.
//
// colRefs  The set of columns referenced so far by the scalar expression being
//
//	built. If not nil, it is updated with the ID of this column.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) finishBuildScalarRef(
	col *scopeColumn, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {

	b.trackReferencedColumnForViews(col)
	// Update the sets of column references and outer columns if needed.
	if colRefs != nil {
		colRefs.Add(col.id)
	}

	// Collect the outer columns of the current subquery, if any.
	//isOuterColumn := inScope == nil || (inScope.isOuterColumn(col.id) && col.colMatch == nil)
	isOuterColumn := inScope == nil || (inScope.isOuterColumn(col.id))
	if isOuterColumn && b.subquery != nil {
		b.subquery.outerCols.Add(col.id)
	}

	// If this is not a projection context, then wrap the column reference with
	// a Variable expression that can be embedded in outer expression(s).
	if outScope == nil {
		return b.factory.ConstructVariable(col.id)
	}

	// Outer columns must be wrapped in a variable expression and assigned a new
	// column id before projection.
	if isOuterColumn {
		// Avoid synthesizing a new column if possible.
		existing := outScope.findExistingCol(col, false /* allowSideEffects */)
		if existing == nil || existing == outCol {
			if outCol.name == "" {
				outCol.name = col.name
			}
			group := b.factory.ConstructVariable(col.id)
			b.populateSynthesizedColumn(outCol, group, ScopeTSPropDefault)
			return group
		}

		col = existing
	}

	// Project the column.
	b.projectColumn(outCol, col)
	return outCol.scalar
}
