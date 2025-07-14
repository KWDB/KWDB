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

// This file has builder code specific to aggregations (queries with GROUP BY,
// HAVING, or aggregate functions).
//
// We build such queries using three operators:
//
//  - a pre-projection: a ProjectOp which generates the columns needed for
//    the aggregation:
//      - group by expressions
//      - arguments to aggregation functions
//
//  - the aggregation: a GroupByOp which has the pre-projection as the
//    input and produces columns with the results of the aggregation
//    functions. The group by columns are also passed through.
//
//  - a post-projection: calculates expressions using the results of the
//    aggregations; this is analogous to the ProjectOp that we would use for a
//    no-aggregation Select.
//
// For example:
//   SELECT 1 + MIN(v*2) FROM kv GROUP BY k+3
//
//   pre-projection:  k+3 (as col1), v*2 (as col2)
//   aggregation:     group by col1, calculate MIN(col2) (as col3)
//   post-projection: 1 + col3

import (
	"strings"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// groupby information stored in scopes.

// aggInfoKey records hash values of aggregateInfo for check repeate agg project

//	type aggregateInfo struct {
//		*tree.FuncExpr                  ---> not use
//		def      memo.FunctionPrivate   ---> funcAddress(function overload point)
//		distinct bool                   ---> distinct
//		args     memo.ScalarListExpr    ---> param
//		filter   opt.ScalarExpr         ---> filterhash
//		col *scopeColumn                ---> not use
//		colRefs opt.ColSet              ---> not use
//		aggOfInterpolate string         ---> not use
//	}
type aggInfoKey struct {
	funcAddress unsafe.Pointer // agg function overload address
	distinct    bool           // agg function is distinct
	filterhash  uint64         // agg function filter hash value
	param       [3]uint64      // agg function param column hash value
	order       string         // agg function order string
}

// getAggInfoKey gets hash value from aggregateInfo to create aggInfoKey
func getAggInfoKey(evalCtx *tree.EvalContext, input *aggregateInfo) aggInfoKey {
	ret := aggInfoKey{
		funcAddress: unsafe.Pointer(input.def.Overload),
		distinct:    input.distinct,
	}

	if input.filter != nil {
		ret.filterhash = memo.GetScalarExprHash(input.filter)
	}

	for i := range input.args {
		ret.param[i] = memo.GetScalarExprHash(input.args[i])
	}

	fmtCtx := execinfrapb.ExprFmtCtxBase(evalCtx)
	input.OrderBy.Format(fmtCtx)
	ret.order += fmtCtx.String()

	return ret
}

type groupby struct {
	// We use two scopes:
	//   - aggInScope contains columns that are used as input by the
	//     GroupBy operator, specifically:
	//      - columns for the arguments to aggregate functions
	//      - grouping columns (from the GROUP BY)
	//     The grouping columns always come second.
	//
	//   - aggOutScope contains columns that are produced by the GroupBy operator,
	//     specifically:
	//      - columns for the results of the aggregate functions.
	//      - the grouping columns
	//
	// For example:
	//
	//   SELECT 1 + MIN(v*2) FROM kv GROUP BY k+3
	//
	//   aggInScope:   v*2 (as col2), k+3 (as col1)
	//   aggOutScope:  MIN(col2) (as col3), k+3 (as col1)
	//
	// Any aggregate functions which contain column references to this scope
	// trigger the creation of new grouping columns in the grouping scope. In
	// addition, if an aggregate function contains no column references, then the
	// aggregate will be added to the "nearest" grouping scope. For example:
	//   SELECT MAX(1) FROM t1
	//
	// TODO(radu): we aren't really using these as scopes, just as scopeColumn
	// containers. Perhaps separate the necessary functionality in a separate
	// structure and pass that instead of scopes.
	aggInScope  *scope
	aggOutScope *scope

	// aggs contains information about aggregate functions encountered.
	aggs []aggregateInfo

	aggsMap map[aggInfoKey]int

	// groupStrs contains a string representation of each GROUP BY expression
	// using symbolic notation. These strings are used to determine if SELECT
	// and HAVING expressions contain sub-expressions matching a GROUP BY
	// expression. This enables queries such as:
	//   SELECT x+y FROM t GROUP BY x+y
	// but not:
	//   SELECT x+y FROM t GROUP BY y+x
	//
	// Each string maps to the grouping column in an aggOutScope scope that
	// projects that expression.
	groupStrs groupByStrSet

	// buildingGroupingCols is true while the grouping columns are being built.
	// It is used to ensure that the builder does not throw a grouping error
	// prematurely.
	buildingGroupingCols bool
}

// groupByStrSet is a set of stringified GROUP BY expressions that map to the
// grouping column in an aggOutScope scope that projects that expression. It
// is used to enforce scoping rules, since any non-aggregate, variable
// expression in the SELECT list must be a GROUP BY expression or be composed
// of GROUP BY expressions. For example, this query is legal:
//
//	SELECT COUNT(*), k + v FROM kv GROUP by k, v
//
// but this query is not:
//
//	SELECT COUNT(*), k + v FROM kv GROUP BY k - v
type groupByStrSet map[string]*scopeColumn

// hasNonCommutativeAggregates checks whether any of the aggregates are
// non-commutative or ordering sensitive.
func (g *groupby) hasNonCommutativeAggregates() bool {
	for i := range g.aggs {
		if !g.aggs[i].isCommutative() {
			return true
		}
	}
	return false
}

// groupingCols returns the columns in the aggInScope corresponding to grouping
// columns.
func (g *groupby) groupingCols() []scopeColumn {
	// Grouping cols are always clustered at the end of the column list.
	return g.aggInScope.cols[len(g.aggInScope.cols)-len(g.groupStrs):]
}

// getAggregateArgCols returns the columns in the aggInScope corresponding to
// arguments to aggregate functions. If the aggregate has a filter, the column
// corresponding to the filter's input will immediately follow the arguments.
func (g *groupby) aggregateArgCols() []scopeColumn {
	return g.aggInScope.cols[:len(g.aggInScope.cols)-len(g.groupStrs)]
}

// getAggregateResultCols returns the columns in the aggOutScope corresponding
// to aggregate functions.
func (g *groupby) aggregateResultCols() []scopeColumn {
	// Aggregates are always clustered at the beginning of the column list, in
	// the same order as s.groupby.aggs.
	return g.aggOutScope.cols[:len(g.aggs)]
}

// hasAggregates returns true if the enclosing scope has aggregate functions.
func (g *groupby) hasAggregates() bool {
	return len(g.aggs) > 0
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (g *groupby) findAggregate(agg aggregateInfo, key *aggInfoKey) *scopeColumn {
	if g.aggs == nil {
		return nil
	}
	if v, ok := g.aggsMap[*key]; ok {
		if getAggName(agg) == Interpolate {
			return nil
		}
		return &g.aggregateResultCols()[v]
	}

	return nil
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	*tree.FuncExpr

	def      memo.FunctionPrivate
	distinct bool
	args     memo.ScalarListExpr
	filter   opt.ScalarExpr

	// col is the output column of the aggregation.
	col *scopeColumn

	// colRefs contains the set of columns referenced by the arguments of the
	// aggregation. It is used to determine the appropriate scope for this
	// aggregation.
	colRefs opt.ColSet

	// aggOfInterpolate is the parameter agg in Interpolate.
	aggOfInterpolate string
}

// Walk is part of the tree.Expr interface.
func (a *aggregateInfo) Walk(v tree.Visitor) tree.Expr {
	return a
}

// TypeCheck is part of the tree.Expr interface.
func (a *aggregateInfo) TypeCheck(ctx *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	if _, err := a.FuncExpr.TypeCheck(ctx, desired); err != nil {
		return nil, err
	}
	return a, nil
}

// isOrderingSensitive returns true if the given aggregate operator is
// ordering sensitive. That is, it can give different results based on the order
// values are fed to it.
func (a aggregateInfo) isOrderingSensitive() bool {
	switch a.def.Name {
	case "array_agg", "concat_agg", "string_agg", "json_agg", "jsonb_agg":
		return true
	default:
		return false
	}
}

// isCommutative checks whether the aggregate is commutative. That is, if it is
// ordering insensitive or if no ordering is specified.
func (a aggregateInfo) isCommutative() bool {
	return a.OrderBy == nil || !a.isOrderingSensitive()
}

// Eval is part of the tree.TypedExpr interface.
func (a *aggregateInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("aggregateInfo must be replaced before evaluation, illegal function: %v", a.def.Name))
}

var _ tree.Expr = &aggregateInfo{}
var _ tree.TypedExpr = &aggregateInfo{}

func (b *Builder) needsAggregation(sel *tree.SelectClause, scope *scope) bool {
	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the SELECT, DISTINCT ON and/or ORDER BY expressions.
	return len(sel.GroupBy) > 0 ||
		sel.Having != nil ||
		(scope.groupby != nil && scope.groupby.hasAggregates())
}

func (b *Builder) constructGroupBy(
	input memo.RelExpr,
	groupingColSet opt.ColSet,
	aggCols []scopeColumn,
	ordering opt.Ordering,
	timeBucketGapFillColID opt.ColumnID,
	groupWindowID opt.ColumnID,
	canApplyAggExtend bool,
	aggExHelper *aggExtendHelper,
) memo.RelExpr {
	aggs := make(memo.AggregationsExpr, 0, len(aggCols))

	// Deduplicate the columns; we don't need to produce the same aggregation
	// multiple times.
	colSet := opt.ColSet{}
	for i := range aggCols {
		if id, scalar := aggCols[i].id, aggCols[i].scalar; !colSet.Contains(id) {
			if scalar == nil {
				// A "pass through" column (i.e. a VariableOp) is not legal as an
				// aggregation.
				panic(errors.AssertionFailedf("variable as aggregation, illegal col: %v", aggCols[i].name))
			}
			aggs = append(aggs, b.factory.ConstructAggregationsItem(scalar, id))
			colSet.Add(id)
		}
	}

	if canApplyAggExtend {
		aggExHelper.extendColSet.ForEach(func(col opt.ColumnID) {
			if aggExHelper.maxCount == 1 {
				maxEx := b.factory.ConstructMaxExtend(b.factory.ConstructVariable(aggExHelper.minOrMaxColID), b.factory.ConstructVariable(col))
				aggs = append(aggs, b.factory.ConstructAggregationsItem(maxEx, col))
			} else if aggExHelper.minCount == 1 {
				minEx := b.factory.ConstructMinExtend(b.factory.ConstructVariable(aggExHelper.minOrMaxColID), b.factory.ConstructVariable(col))
				aggs = append(aggs, b.factory.ConstructAggregationsItem(minEx, col))
			}
		})
	}

	private := memo.GroupingPrivate{GroupingCols: groupingColSet}
	// The ordering of the GROUP BY is inherited from the input. This ordering is
	// only useful for intra-group ordering (for order-sensitive aggregations like
	// ARRAY_AGG). So we add the grouping columns as optional columns.
	private.Ordering.FromOrderingWithOptCols(ordering, groupingColSet)

	// Check Twa and elapsed aggregations to ensure they conform
	// to the requirements of being applied on time series tables.
	b.checkTwaAndElapsedAggregations(aggCols, &private, groupingColSet)

	if groupingColSet.Empty() {
		return b.factory.ConstructScalarGroupBy(input, aggs, &private)
	}
	// add order asc.
	if timeBucketGapFillColID > 0 {
		ord := make(opt.Ordering, 0, len(groupingColSet.Ordered()))
		for _, g := range groupingColSet.Ordered() {
			// time_bucket_gapfill need ascending order.
			ord = append(ord, opt.MakeOrderingColumn(opt.ColumnID(g), false))
		}
		private.Ordering.FromOrdering(ord)
		private.TimeBucketGapFillColId = timeBucketGapFillColID
	}
	private.GroupWindowId = groupWindowID
	return b.factory.ConstructGroupBy(input, aggs, &private)
}

// buildGroupingColumns builds the grouping columns and adds them to the
// groupby scopes that will be used to build the aggregation expression.
// Returns the slice of grouping columns.
func (b *Builder) buildGroupingColumns(sel *tree.SelectClause, projectionsScope, fromScope *scope) {
	if fromScope.groupby == nil {
		fromScope.initGrouping()
	}
	g := fromScope.groupby

	// The "from" columns are visible to any grouping expressions.
	b.buildGroupingList(sel.GroupBy, sel.Exprs, projectionsScope, fromScope)

	// Copy the grouping columns to the aggOutScope.
	g.aggOutScope.appendColumns(g.groupingCols())
}

// check whether group window function is valid
func (b *Builder) checkGroupWindow(expr *memo.FunctionExpr, scope *scope) (isGroupWiondow bool) {
	if _, ok := memo.CheckGroupWindowExist(expr); ok && b.factory.Memo().CheckFlag(opt.GroupWindowUseOrderScan) {
		panic(pgerror.Newf(pgcode.Syntax, "%s(): multiple group window functions are not supported.", expr.Name))
	} else if ok {
		if scope.TableType == nil || scope.TableType.HasRtable() || len(b.factory.Metadata().AllTables()) > 1 {
			panic(pgerror.Newf(pgcode.Syntax, "%s(): group window function is only supported for use in single time series table query.", expr.Name))
		}
		b.factory.Memo().SetFlag(opt.GroupWindowUseOrderScan)
		isGroupWiondow = true
	}
	var countValue int64
	for i, arg := range expr.Args {
		switch expr.Name {
		case memo.StateWindow:
			switch arg.(type) {
			case *memo.VariableExpr, *memo.CaseExpr:
			default:
				panic(pgerror.Newf(pgcode.Syntax, "parameter of %s() should be column of data or case...when...", expr.Name))
			}
		case memo.CountWindow:
			if e, ok := arg.(*memo.ConstExpr); !ok {
				panic(pgerror.Newf(pgcode.Syntax, "parameter of %s() should be const", expr.Name))
			} else if ee, ok1 := e.Value.(*tree.DInt); ok1 {
				if i == 0 {
					if int64(*ee) <= 0 {
						panic(pgerror.Newf(pgcode.Syntax, "%s(): invalid count value", expr.Name))
					}
					countValue = int64(*ee)
				} else if int64(*ee) <= 0 || int64(*ee) > countValue {
					panic(pgerror.Newf(pgcode.Syntax, "%s(): invalid sliding value", expr.Name))
				}
			}
		}
	}
	return isGroupWiondow
}

// buildAggregation builds the aggregation operators and constructs the
// GroupBy expression. Returns the output scope for the aggregation operation.
// if group cols contain group window function, we need to add the following restrictions:
// 1. we check whether group cols contain only one grouping window function.
// 2. if there is more than one grouping column and tableID is null, we should return error
// 3. if there is more than one grouping column and inconsistent number of primary tag, we should return error
// 4. we check the functional limitations of the grouping window function.
func (b *Builder) buildAggregation(having opt.ScalarExpr, fromScope *scope) (outScope *scope) {
	g := fromScope.groupby

	groupingCols := g.groupingCols()

	// Build ColSet of grouping columns.
	var groupingColSet opt.ColSet
	// Representing the column ID of timebucketGapFill in the group.
	var timeBucketGapFillColID opt.ColumnID
	var pTagNum int
	// columns other than group window function and pTag
	hasOhterCol := false
	tableID := opt.TableID(0)
	var groupWindowID opt.ColumnID = -1
	groupWindowIdx := -1
	for i := range groupingCols {
		groupingColSet.Add(groupingCols[i].id)
		colMeta := b.factory.Metadata().ColumnMeta(groupingCols[i].id)
		if colMeta.IsPrimaryTag() {
			tableID = colMeta.Table
			pTagNum++
			continue
		}
		if f, ok := groupingCols[i].scalar.(*memo.FunctionExpr); ok {
			if f.Name == "time_bucket_gapfill" {
				timeBucketGapFillColID = groupingCols[i].id
			}
			// if function is group windows, we should add groupWindowID and Idx to logical plan
			if b.checkGroupWindow(f, fromScope) {
				f.FunctionPrivate.Properties.ForbiddenExecInTSEngine = false
				groupWindowID = groupingCols[i].id
				groupWindowIdx = i
			} else {
				hasOhterCol = true
			}
		} else {
			hasOhterCol = true
		}
	}

	if b.factory.Memo().CheckFlag(opt.GroupWindowUseOrderScan) {
		fromScope.expr.ChildCount()
		if len(groupingCols) > 1 {
			// if there is more than one grouping column and tableID is null, we should return error
			// if there is more than one grouping column and inconsistent number of primary tag, we should return error
			if tableID == 0 || (pTagNum != b.factory.Metadata().TableMeta(tableID).PrimaryTagCount) ||
				(hasOhterCol && pTagNum == b.factory.Metadata().TableMeta(tableID).PrimaryTagCount) {
				panic(pgerror.Newf(pgcode.Syntax, "groupby cols are all ptags or without groupby cols for using group window function."))
			}
		} else if groupWindowIdx >= 0 {
			// function not push to AE if there are no other groupby cols except for the group window function
			if f, ok := groupingCols[groupWindowIdx].scalar.(*memo.FunctionExpr); ok {
				f.Properties.ForbiddenExecInTSEngine = true
			}
		}
	}

	// If there are any aggregates that are ordering sensitive, build the
	// aggregations as window functions over each group.
	if g.hasNonCommutativeAggregates() {
		return b.buildAggregationAsWindow(groupingColSet, having, fromScope)
	}

	aggInfos := g.aggs

	// Construct the aggregation operators.
	haveOrderingSensitiveAgg := false
	aggCols := g.aggregateResultCols()
	argCols := g.aggregateArgCols()
	var fromCols opt.ColSet
	if b.subquery != nil {
		// Only calculate the set of fromScope columns if it will be used below.
		fromCols = fromScope.colSet()
	}

	aggFuncs := []string{}
	for i, agg := range aggInfos {
		// First accumulate the arguments to the aggregate function. These are
		// always variables referencing columns in the GroupBy input expression,
		// except in the case of string_agg, where the second argument must be
		// a constant expression.
		args := make([]opt.ScalarExpr, 0, 2)
		for j := range agg.args {
			// Check the aggregation function can be optimized using constants.
			if canUseConstOptimize(j, argCols[0].scalar, agg.Func.FunctionName()) {
				args = append(args, argCols[0].scalar)
			} else {
				colID := argCols[0].id
				args = append(args, b.factory.ConstructVariable(colID))
			}

			// Skip past argCols that have been handled. There may be variable
			// number of them, so need to set up for next aggregate function.
			argCols = argCols[1:]
		}

		// add last/lastts second param
		if checkLastOrFirstAgg(agg.Func.FunctionName()) {
			if tmp, ok := args[0].(*memo.VariableExpr); ok {
				for _, val := range b.factory.Metadata().AllTables() {
					if val.HasColumn(tmp.Col) {
						args = append(args, b.factory.ConstructVariable(val.MetaID.ColumnID(0)))
						break
					}
				}
			}
		}

		// Construct the aggregate function from its name and arguments and store
		// it in the corresponding scope column.
		aggCols[i].scalar = b.constructAggregate(agg.def.Name, args)

		// Wrap the aggregate function with an AggDistinct operator if DISTINCT
		// was specified in the query.
		if agg.distinct {
			aggCols[i].scalar = b.factory.ConstructAggDistinct(aggCols[i].scalar)
		}

		// Wrap the aggregate function or the AggDistinct in an AggFilter operator
		// if FILTER (WHERE ...) was specified in the query.
		// TODO(justin): add a norm rule to push these filters below GroupBy where
		// possible.
		if agg.filter != nil {
			// Column containing filter expression is always after the argument
			// columns (which have already been processed).
			colID := argCols[0].id
			argCols = argCols[1:]
			variable := b.factory.ConstructVariable(colID)
			aggCols[i].scalar = b.factory.ConstructAggFilter(aggCols[i].scalar, variable)
		}

		if agg.isOrderingSensitive() {
			haveOrderingSensitiveAgg = true
		}

		if b.subquery != nil {
			// Update the subquery with any outer columns from the aggregate
			// arguments. The outer columns were not added in finishBuildScalarRef
			// because at the time the arguments were built, we did not know which
			// was the appropriate scope for the aggregation. (buildAggregateFunction
			// ensures that finishBuildScalarRef doesn't add the outer columns by
			// temporarily setting b.subquery to nil. See buildAggregateFunction
			// for more details.)
			b.subquery.outerCols.UnionWith(agg.colRefs.Difference(fromCols))
		}
		if agg.aggOfInterpolate != "" {
			aggFuncs = append(aggFuncs, agg.aggOfInterpolate)
		}
	}

	if haveOrderingSensitiveAgg {
		g.aggInScope.copyOrdering(fromScope)
	}

	// construct projection cols.
	b.dealTimeWindowWithFirstOrLast(fromScope, &aggCols)

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments, as well as any additional order by columns.
	canApplyAggExtend := fromScope.CanApplyAggExtend(&groupingColSet)
	if canApplyAggExtend {
		// construct pre-projection when the SQL can apply agg extend.
		g.aggInScope.expr = b.constructProjectInAggExtend(fromScope.expr, append(g.aggInScope.cols, g.aggInScope.extraCols...), fromScope.AggExHelper.extendColSet)
	} else {
		b.constructProjectForScope(fromScope, g.aggInScope)
	}

	g.aggOutScope.expr = b.constructGroupBy(g.aggInScope.expr, groupingColSet, aggCols,
		g.aggInScope.ordering, timeBucketGapFillColID, groupWindowID, canApplyAggExtend, &fromScope.AggExHelper)
	if len(aggFuncs) != 0 {
		if g, ok := g.aggOutScope.expr.(*memo.ScalarGroupByExpr); ok {
			g.GroupingPrivate.Func = aggFuncs
		}
		if g, ok := g.aggOutScope.expr.(*memo.GroupByExpr); ok {
			g.GroupingPrivate.Func = aggFuncs
		}
	}
	// Wrap with having filter if it exists.
	if having != nil {
		input := g.aggOutScope.expr.(memo.RelExpr)
		filters := memo.FiltersExpr{b.factory.ConstructFiltersItem(having)}
		g.aggOutScope.expr = b.factory.ConstructSelect(input, filters)
	}

	return g.aggOutScope
}

// analyzeHaving analyzes the having clause and returns it as a typed
// expression. fromScope contains the name bindings that are visible for this
// HAVING clause (e.g., passed in from an enclosing statement).
func (b *Builder) analyzeHaving(having *tree.Where, fromScope *scope) tree.TypedExpr {
	if having == nil {
		return nil
	}
	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(
		exprTypeHaving.String(), tree.RejectWindowApplications|tree.RejectGenerators,
	)
	fromScope.context = exprTypeHaving
	return fromScope.resolveAndRequireType(having.Expr, types.Bool)
}

// buildHaving builds a set of memo groups that represent the given HAVING
// clause. fromScope contains the name bindings that are visible for this HAVING
// clause (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// HAVING clause.
func (b *Builder) buildHaving(having tree.TypedExpr, fromScope *scope) opt.ScalarExpr {
	if having == nil {
		return nil
	}

	res := b.buildScalar(having, fromScope, nil, nil, nil)
	// group window function can be only used in groupby
	if name, ok := memo.CheckGroupWindowExist(res); ok {
		panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
	}

	return res
}

// buildGroupingList builds a set of memo groups that represent a list of
// GROUP BY expressions, adding the group-by expressions as columns to
// aggInScope and populating groupStrs.
//
// groupBy   The given GROUP BY expressions.
// selects   The select expressions are needed in case one of the GROUP BY
//
//	expressions is an index into to the select list. For example,
//	    SELECT count(*), k FROM t GROUP BY 2
//	indicates that the grouping is on the second select expression, k.
//
// fromScope The scope for the input to the aggregation (the FROM clause).
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, projectionsScope *scope, fromScope *scope,
) {
	g := fromScope.groupby
	g.groupStrs = make(groupByStrSet, len(groupBy))
	if g.aggInScope.cols == nil {
		g.aggInScope.cols = make([]scopeColumn, 0, len(groupBy))
	}

	// The buildingGroupingCols flag is used to ensure that a grouping error is
	// not called prematurely. For example:
	//   SELECT count(*), a FROM ab GROUP BY a
	// is legal, but
	//   SELECT count(*), b FROM ab GROUP BY a
	// will throw the error, `column "b" must appear in the GROUP BY clause or be
	// used in an aggregate function`. The builder cannot know whether there is
	// a grouping error until the grouping columns are fully built.
	g.buildingGroupingCols = true
	for _, e := range groupBy {
		b.buildGrouping(e, selects, projectionsScope, fromScope, g.aggInScope)
	}
	g.buildingGroupingCols = false
}

// buildGrouping builds a set of memo groups that represent a GROUP BY
// expression. The expression (or expressions, if we have a star) is added to
// groupStrs and to the aggInScope.
//
// groupBy          The given GROUP BY expression.
// selects          The select expressions are needed in case the GROUP BY
//
//	expression is an index into to the select list.
//
// projectionsScope The scope that contains the columns for the SELECT targets
//
//	(used when GROUP BY refers to a target by alias).
//
// fromScope        The scope for the input to the aggregation (the FROM
//
//	clause).
//
// aggInScope       The scope that will contain the grouping expressions as well
//
//	as the aggregate function arguments.
func (b *Builder) buildGrouping(
	groupBy tree.Expr, selects tree.SelectExprs, projectionsScope, fromScope, aggInScope *scope,
) {
	// Unwrap parenthesized expressions like "((a))" to "a".
	groupBy = tree.StripParens(groupBy)
	alias := ""

	// Comment below pasted from PostgreSQL (findTargetListEntrySQL92 in
	// src/backend/parser/parse_clause.c).
	//
	// Handle two special cases as mandated by the SQL92 spec:
	//
	// 1. Bare ColumnName (no qualifier or subscripts)
	//    For a bare identifier, we search for a matching column name
	//    in the existing target list.  Multiple matches are an error
	//    unless they refer to identical values; for example,
	//    we allow  SELECT a, a FROM table ORDER BY a
	//    but not   SELECT a AS b, b FROM table ORDER BY b
	//    If no match is found, we fall through and treat the identifier
	//    as an expression.
	//    For GROUP BY, it is incorrect to match the grouping item against
	//    targetlist entries: according to SQL92, an identifier in GROUP BY
	//    is a reference to a column name exposed by FROM, not to a target
	//    list column.  However, many implementations (including pre-7.0
	//    PostgreSQL) accept this anyway.  So for GROUP BY, we look first
	//    to see if the identifier matches any FROM column name, and only
	//    try for a targetlist name if it doesn't.  This ensures that we
	//    adhere to the spec in the case where the name could be both.
	//    DISTINCT ON isn't in the standard, so we can do what we like there;
	//    we choose to make it work like ORDER BY, on the rather flimsy
	//    grounds that ordinary DISTINCT works on targetlist entries.
	//
	// 2. IntegerConstant
	//    This means to use the n'th item in the existing target list.
	//    Note that it would make no sense to order/group/distinct by an
	//    actual constant, so this does not create a conflict with SQL99.
	//    GROUP BY column-number is not allowed by SQL92, but since
	//    the standard has no other behavior defined for this syntax,
	//    we may as well accept this common extension.

	// This function sets groupBy and alias in these special cases.
	func() {
		// Check whether the GROUP BY clause refers to a column in the SELECT list
		// by index, e.g. `SELECT a, SUM(b) FROM y GROUP BY 1` (case 2 above).
		if col := colIndex(len(selects), groupBy, "GROUP BY"); col != -1 {
			groupBy, alias = selects[col].Expr, string(selects[col].As)
			return
		}

		if name, ok := groupBy.(*tree.UnresolvedName); ok {
			if name.NumParts != 1 || name.Star {
				return
			}
			// Case 1 above.
			targetName := name.Parts[0]

			// We must prefer a match against a FROM-clause column (but ignore upper
			// scopes); in this case we let the general case below handle the reference.
			for i := range fromScope.cols {
				if string(fromScope.cols[i].name) == targetName {
					return
				}
			}
			// See if it matches exactly one of the target lists.
			var match *scopeColumn
			for i := range projectionsScope.cols {
				if col := &projectionsScope.cols[i]; string(col.name) == targetName {
					if match != nil {
						// Multiple matches are only allowed if they refer to identical
						// expressions.
						if match.getExprStr() != col.getExprStr() {
							panic(pgerror.Newf(pgcode.AmbiguousColumn, "GROUP BY %q is ambiguous", targetName))
						}
					}
					match = col
				}
			}
			if match != nil {
				if fromScope.hasGapfill {
					if info, ok := match.expr.(*aggregateInfo); ok {
						res := resetFuncExpr(info)
						if res != nil {
							groupBy, alias = res, targetName
							return
						}
					}
				}
				groupBy, alias = match.expr, targetName
			}
		}
	}()

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Make sure the GROUP BY columns have no special functions.
	b.semaCtx.Properties.Require(exprTypeGroupBy.String(), tree.RejectSpecial)
	fromScope.context = exprTypeGroupBy

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, fromScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	for _, e := range exprs {
		// If a grouping column has already been added, don't add it again.
		// GROUP BY a, a is semantically equivalent to GROUP BY a.
		exprStr := symbolicExprStr(e)
		if _, ok := fromScope.groupby.groupStrs[exprStr]; ok {
			continue
		}

		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//   SELECT x+y FROM t GROUP BY x+y
		col := b.addColumn(aggInScope, alias, e, false)
		b.buildScalar(e, fromScope, aggInScope, col, nil)
		fromScope.groupby.groupStrs[exprStr] = col
	}
}

// buildAggArg builds a scalar expression which is used as an input in some form
// to an aggregate expression. The scopeColumn for the built expression will
// be added to tempScope.
func (b *Builder) buildAggArg(
	e tree.TypedExpr, info *aggregateInfo, tempScope, fromScope *scope,
) opt.ScalarExpr {
	// This synthesizes a new tempScope column, unless the argument is a
	// simple VariableOp.
	col := b.addColumn(tempScope, "" /* alias */, e, false)
	b.buildScalar(e, fromScope, tempScope, col, &info.colRefs)
	if col.scalar != nil {
		return col.scalar
	}
	return b.factory.ConstructVariable(col.id)
}

// buildAggregateFunction is called when we are building a function which is an
// aggregate. Any non-trivial parameters (i.e. not column reference) to the
// aggregate function are extracted and added to aggInScope. The aggregate
// function expression itself is added to aggOutScope. For example:
//
//	SELECT SUM(x+1) FROM xy
//	=>
//	aggInScope : x+1 AS column1
//	aggOutScope: SUM(column1)
//
// buildAggregateFunction returns a pointer to the aggregateInfo containing
// the function definition, fully built arguments, and the aggregate output
// column.
//
// tempScope is a temporary scope which is used for building the aggregate
// function arguments before the correct scope is determined.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, def *memo.FunctionPrivate, tempScope, fromScope *scope,
) *aggregateInfo {
	tempScopeColsBefore := len(tempScope.cols)

	info := aggregateInfo{
		FuncExpr: f,
		def:      *def,
		distinct: (f.Type == tree.DistinctFuncType),
		args:     make(memo.ScalarListExpr, len(f.Exprs)),
	}

	// Special handling of interpolate function.Replace the first parameter for semantic parsing.
	// limit the first parameter must be agg.
	if def.Name == Interpolate {
		info.aggOfInterpolate = ""
		if agg, ok := f.Exprs[0].(*aggregateInfo); ok {
			if col, ok := agg.col.expr.(*tree.FuncExpr); ok {
				if col.Func.FunctionName() == sqlbase.LastAgg || col.Func.FunctionName() == sqlbase.LastRowAgg ||
					col.Func.FunctionName() == sqlbase.FirstAgg || col.Func.FunctionName() == sqlbase.FirstRowAgg {
					fromScope.interpolateWithLast.interpolateHasLast = true
					var column *scopeColumn
					for i := range agg.FuncExpr.Exprs {
						if col, ok := agg.FuncExpr.Exprs[i].(*scopeColumn); ok {
							if i == 0 {
								column = col
							}
						}
					}
					if col.Func.FunctionName() == sqlbase.LastAgg && len(agg.FuncExpr.Exprs) == 3 {
						if _, ok1 := agg.FuncExpr.Exprs[1].(*tree.CastExpr); !ok1 {
							panic(pgerror.Newf(pgcode.FeatureNotSupported, "%v in interpolate does not support multiple arguments", col.Func.FunctionName()))
						}
					}
					column.typ = agg.col.typ
					info.aggOfInterpolate = strings.ToLower(agg.def.Name)
					f.Exprs[0] = column
				} else if len(col.Exprs) > 0 {
					if c, ok1 := col.Exprs[0].(*scopeColumn); ok1 {
						c.typ = agg.col.typ
						info.aggOfInterpolate = strings.ToLower(agg.def.Name)
						f.Exprs[0] = col.Exprs[0]
					} else {
						panic(pgerror.Newf(pgcode.Warning,
							"%v as a parameter of the interpolate function, only supports columns as its parameters", agg.def.Name))
					}
				} else { // other is not only scopeColumn
					// TODO Subsequent versions will lift restrictions
					panic(pgerror.Newf(pgcode.Warning,
						"%v as a parameter of the interpolate function, only supports columns as its parameters", agg.def.Name))
				}
			}
		} else {
			panic(pgerror.New(pgcode.Warning, "the first parameter of interpolate must be agg func"))
		}
	}

	if def.Name == Gapfillinternal {
		if col, ok := f.Exprs[0].(*scopeColumn); ok {
			for _, val := range b.factory.Metadata().AllTables() {
				if val.HasColumn(col.id) {
					if val.Table.GetTableType() != tree.RelationalTable && col.id == val.MetaID.ColumnID(0) {
						fromScope.interpolateWithLast.gapfillWithFirstCol = true
						break
					}
				}
			}
		}
	}

	if def.Name == Gapfillinternal || def.Name == Interpolate {
		limitGapFillFirstArg(fromScope)
	}
	// Temporarily set b.subquery to nil so we don't add outer columns to the
	// wrong scope.
	subq := b.subquery
	b.subquery = nil
	defer func() { b.subquery = subq }()

	for i, pexpr := range f.Exprs {
		info.args[i] = b.buildAggArg(pexpr.(tree.TypedExpr), &info, tempScope, fromScope)
		funcName := f.Func.FunctionName()
		if funcName == Interpolate {
			if c, ok := f.Exprs[i].(*scopeColumn); ok {
				if v, ok := info.args[i].(*memo.VariableExpr); ok {
					v.Typ = c.typ
				}
			}
		}
		if funcName == sqlbase.FirstAgg || funcName == sqlbase.LastAgg {
			if i > 0 {
				continue
			}
		}
		fromScope.checkAggExtend(funcName, info.args[i])
	}

	// If we have a filter, add it to tempScope after all the arguments. We'll
	// later extract the column that gets added here in buildAggregation.
	if f.Filter != nil {
		info.filter = b.buildAggArg(f.Filter.(tree.TypedExpr), &info, tempScope, fromScope)
	}

	// If we have ORDER BY, add the ordering columns to the tempScope.
	if f.OrderBy != nil {
		for _, o := range f.OrderBy {
			b.buildAggArg(o.Expr.(tree.TypedExpr), &info, tempScope, fromScope)
		}
	}

	// Find the appropriate aggregation scopes for this aggregate now that we
	// know which columns it references. If necessary, we'll move the columns
	// for the arguments from tempScope to aggInScope below.
	g := fromScope.endAggFunc(info)

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	if len(info.args) > 3 {
		panic(pgerror.Newf(pgcode.Warning, "%s have %d param, agg function need have less than three param", info.def.Name, len(info.args)))
	}
	key := getAggInfoKey(b.evalCtx, &info)
	info.col = g.findAggregate(info, &key)
	if info.col == nil {
		// Use 0 as the group for now; it will be filled in later by the
		// buildAggregation method.
		info.col = b.synthesizeColumn(g.aggOutScope, def.Name, f.ResolvedType(), f, nil /* scalar */)

		// Move the columns for the aggregate input expressions to the correct scope.
		if g.aggInScope != tempScope {
			g.aggInScope.cols = append(g.aggInScope.cols, tempScope.cols[tempScopeColsBefore:]...)
			tempScope.cols = tempScope.cols[:tempScopeColsBefore]
		}

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		if g.aggsMap == nil {
			g.aggsMap = make(map[aggInfoKey]int)
		}
		g.aggsMap[key] = len(g.aggs)
		g.aggs = append(g.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		tempScope.cols = tempScope.cols[:tempScopeColsBefore]
	}

	return &info
}

// limitGapFillFirstArg is used to limit unsupported features.
// When the interpolate function has a parameter of last, the first argument
// to time_bucket_gapfill must be the first column of the time_series table.
func limitGapFillFirstArg(fromScope *scope) {
	if fromScope.interpolateWithLast.interpolateHasLast && !fromScope.interpolateWithLast.gapfillWithFirstCol {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"last/last_row as a parameter of the interpolate function, "+
				"the first parameter of time_bucket_gapfill must be the first column of timeseries table"))
	}
}

func (b *Builder) constructWindowFn(name string, args []opt.ScalarExpr) opt.ScalarExpr {
	switch name {
	case "rank":
		return b.factory.ConstructRank()
	case "row_number":
		return b.factory.ConstructRowNumber()
	case "dense_rank":
		return b.factory.ConstructDenseRank()
	case "percent_rank":
		return b.factory.ConstructPercentRank()
	case "cume_dist":
		return b.factory.ConstructCumeDist()
	case "ntile":
		return b.factory.ConstructNtile(args[0])
	case "lag":
		return b.factory.ConstructLag(args[0], args[1], args[2])
	case "lead":
		return b.factory.ConstructLead(args[0], args[1], args[2])
	case "first_value":
		return b.factory.ConstructFirstValue(args[0])
	case "last_value":
		return b.factory.ConstructLastValue(args[0])
	case "nth_value":
		return b.factory.ConstructNthValue(args[0], args[1])
	case "diff":
		return b.factory.ConstructDiff(args[0])
	default:
		return b.constructAggregate(name, args)
	}
}

func (b *Builder) constructAggregate(name string, args []opt.ScalarExpr) opt.ScalarExpr {
	switch name {
	case "array_agg":
		return b.factory.ConstructArrayAgg(args[0])
	case "avg":
		return b.factory.ConstructAvg(args[0])
	case "bit_and":
		return b.factory.ConstructBitAndAgg(args[0])
	case "bit_or":
		return b.factory.ConstructBitOrAgg(args[0])
	case "bool_and", "every":
		return b.factory.ConstructBoolAnd(args[0])
	case "bool_or":
		return b.factory.ConstructBoolOr(args[0])
	case "concat_agg":
		return b.factory.ConstructConcatAgg(args[0])
	case "corr":
		return b.factory.ConstructCorr(args[0], args[1])
	case "count":
		return b.factory.ConstructCount(args[0])
	case "count_rows":
		return b.factory.ConstructCountRows()
	case "max":
		return b.factory.ConstructMax(args[0])
	case "min":
		return b.factory.ConstructMin(args[0])
	case "sum_int":
		return b.factory.ConstructSumInt(args[0])
	case "sum":
		return b.factory.ConstructSum(args[0])
	case "first":
		return b.factory.ConstructFirst(args[0], args[1])
	case "firstts":
		return b.factory.ConstructFirstTimeStamp(args[0], args[1])
	case "first_row":
		return b.factory.ConstructFirstRow(args[0], args[1])
	case "first_row_ts":
		return b.factory.ConstructFirstRowTimeStamp(args[0], args[1])
	case "last_row_ts":
		return b.factory.ConstructLastRowTimeStamp(args[0], args[1])
	case "last":
		return b.factory.ConstructLast(args[0], args[2], args[1])
	case "matching":
		return b.factory.ConstructMatching(args[0], args[1], args[2], args[3], args[4])
	case "sqrdiff":
		return b.factory.ConstructSqrDiff(args[0])
	case "variance":
		return b.factory.ConstructVariance(args[0])
	case "stddev", "stddev_samp":
		return b.factory.ConstructStdDev(args[0])
	case "time_bucket_gapfill_internal":
		return b.factory.ConstructTimeBucketGapfill(args[0], args[1])
	case "interpolate":
		return b.factory.ConstructImputation(args[0], args[1])
	case "xor_agg":
		return b.factory.ConstructXorAgg(args[0])
	case "json_agg":
		return b.factory.ConstructJsonAgg(args[0])
	case "jsonb_agg":
		return b.factory.ConstructJsonbAgg(args[0])
	case "string_agg":
		return b.factory.ConstructStringAgg(args[0], args[1])
	case "last_row":
		return b.factory.ConstructLastRow(args[0], args[1])
	case "lastts":
		return b.factory.ConstructLastTimeStamp(args[0], args[2], args[1])
	case "elapsed":
		return b.factory.ConstructElapsed(args[0], args[1])
	case "twa":
		return b.factory.ConstructTwa(args[0], args[1])
	}
	panic(errors.AssertionFailedf("unhandled aggregate: %s", name))
}

func isAggregate(def *tree.FunctionDefinition) bool {
	return def.Class == tree.AggregateClass
}

func isWindow(def *tree.FunctionDefinition) bool {
	return def.Class == tree.WindowClass
}

func isGenerator(def *tree.FunctionDefinition) bool {
	return def.Class == tree.GeneratorClass
}

func newGroupingError(name *tree.Name) error {
	return pgerror.Newf(pgcode.Grouping,
		"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
		tree.ErrString(name),
	)
}

func newTimeBucketGroupingError() error {
	return pgerror.Newf(pgcode.Grouping,
		"The parameters of the time_bucket_gapfill() in order by and group by must be consistent",
	)
}

// allowImplicitGroupingColumn returns true if col is part of a table and the
// the groupby metadata indicates that we are grouping on the entire PK of that
// table. In that case, we can allow col as an "implicit" grouping column, even
// if it is not specified in the query.
func (b *Builder) allowImplicitGroupingColumn(colID opt.ColumnID, g *groupby) bool {
	md := b.factory.Metadata()
	colMeta := md.ColumnMeta(colID)
	if colMeta.Table == 0 {
		return false
	}
	// Get all the PK columns.
	tab := md.Table(colMeta.Table)
	var pkCols opt.ColSet

	// if table is ts-table, timestamp is not primary key, timestamp in group by return false
	if tab.GetTableType() != tree.RelationalTable {
		return false
	}

	if tab.IndexCount() == 0 {
		// Virtual tables have no indexes.
		return false
	}
	primaryIndex := tab.Index(cat.PrimaryIndex)
	for i := 0; i < primaryIndex.KeyColumnCount(); i++ {
		pkCols.Add(colMeta.Table.ColumnID(primaryIndex.Column(i).Ordinal))
	}

	// Remove PK columns that are grouping cols and see if there's anything left.
	groupingCols := g.groupingCols()
	for i := range groupingCols {
		pkCols.Remove(groupingCols[i].id)
	}
	return pkCols.Empty()
}

// checkTwaAndElapsedAggregations checks Twa and elapsed aggregations to ensure
// they conform to the requirements of being applied on time series tables.
func (b *Builder) checkTwaAndElapsedAggregations(
	aggCols []scopeColumn, private *memo.GroupingPrivate, groupingColSet opt.ColSet,
) {
	for _, aggCol := range aggCols {
		switch expr := aggCol.scalar.(type) {
		case *memo.TwaExpr:
			if _, isTsArg := expr.TsInput.(*memo.VariableExpr); isTsArg {
				b.checkTimeSeriesConstraints(nil, "twa")
			}
		case *memo.ElapsedExpr:
			if _, isTsArg := expr.Input.(*memo.VariableExpr); isTsArg {
				b.checkTimeSeriesConstraints(expr.TimeUnit, "elapsed")
			}
		}
	}
}

// checkTimeSeriesConstraints is used to check the time series usage limit.
func (b *Builder) checkTimeSeriesConstraints(dataArg opt.ScalarExpr, funcName string) {
	if funcName == "elapsed" && dataArg != nil {
		if timeUnit, isConstArg := dataArg.(*memo.ConstExpr); isConstArg {
			timeUnitStr := timeUnit.Value.String()

			validTimeUnits := map[string]bool{
				"'00:00:00.000000001'": true,
				"'00:00:00.000001'":    true,
				"'00:00:00.001'":       true,
				"'00:00:01'":           true,
				"'00:01:00'":           true,
				"'01:00:00'":           true,
				"'1 day'":              true,
				"'7 days'":             true,
			}

			if _, isValid := validTimeUnits[timeUnitStr]; !isValid {
				panic(pgerror.New(pgcode.InvalidParameterValue, "invalid time unit in elapsed function"))
			}
		} else {
			panic(pgerror.New(pgcode.InvalidParameterValue, "invalid time unit in elapsed function"))
		}
	}
}

// canUseConstOptimize checks the aggregation function can be optimized using constants.
func canUseConstOptimize(j int, scalar opt.ScalarExpr, funcName string) bool {
	if j > 0 && scalar != nil && scalar.Op() == opt.ConstOp {
		switch funcName {
		case "twa", "elapsed", "last", "lastts":
			return true
		}
	}
	return false
}

// constructProjectionCol constructs the AST of "time_window_start" or "time_window_end",
// constructs scopeColumn, and then add them to aggInScope.cols.
// alias is the name of "time_window_start" or "time_window_end"
// paramColName is the name of param of "time_window_start" or "time_window_end
// aggCols are all aggs.
// colSlice records the projection cols.
func (b *Builder) constructProjectionCol(
	fromScope *scope, alias, paramColName, op string, aggCols *[]scopeColumn,
) {
	// construct AST.
	e := &tree.FuncExpr{Func: tree.WrapFunction(alias), Exprs: tree.Exprs{&tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{paramColName}}}}

	// construct scopeColumn and memo function expr , then add it to metadata.
	texpr := fromScope.resolveType(e, types.Any)
	col := scopeColumn{
		name: tree.Name(alias),
		typ:  texpr.ResolvedType(),
		expr: texpr,
	}
	if f, ok := texpr.(*tree.FuncExpr); ok {
		def, err := f.Func.Resolve(b.semaCtx.SearchPath)
		if err != nil {
			panic(err)
		}
		args := make(memo.ScalarListExpr, 1)
		args[0] = b.factory.ConstructVariable(1)

		// the time accuracy of the function needs to be consistent with the column.
		col.typ = args[0].DataType()

		// Construct a private FuncOpDef that refers to a resolved function overload.
		out := b.factory.ConstructFunction(args, &memo.FunctionPrivate{
			Name:       def.Name,
			Typ:        col.typ,
			Properties: &def.FunctionProperties,
			Overload:   f.ResolvedOverload(),
		})
		b.populateSynthesizedColumn(&col, out, fromScope.ScopeTSProp)
	}

	// change id of param of first or last agg
	// eg: first(ts) -> first(time_window_start), last(ts) -> last(time_window_end)
	for i := 0; i < len(*aggCols); i++ {
		if (*aggCols)[i].name.String() == op {
			switch op {
			case sqlbase.LastAgg:
				if last, ok := (*aggCols)[i].scalar.(*memo.LastExpr); ok {
					if checkTimestampCol(b, last.Input) {
						last.Input = b.factory.ConstructVariable(col.id)
						last.IsExtend = true
					}
				}
			case sqlbase.FirstAgg:
				if first, ok := (*aggCols)[i].scalar.(*memo.FirstExpr); ok {
					if checkTimestampCol(b, first.Input) {
						first.Input = b.factory.ConstructVariable(col.id)
						first.IsExtend = true
					}
				}
			}

		}
	}
	fromScope.groupby.aggInScope.cols = append(fromScope.groupby.aggInScope.cols, col)
}

// checkTimestampCol check if the expr e is timestamp col.
func checkTimestampCol(b *Builder, e opt.ScalarExpr) bool {
	if v, ok := e.(*memo.VariableExpr); ok {
		// get table id base on logicl col id.
		tbl := b.factory.Metadata().ColumnMeta(v.Col).Table
		// get the id of first column of table.
		if tbl.ColumnID(0) == v.Col {
			return true
		}
	}
	return false
}

// dealTimeWindowWithFirstOrLast constructs projection cols when the time_window() is
// used with first(ts) or last(ts).
// aggCols are all aggs.
// colSlice records the projection cols.
func (b *Builder) dealTimeWindowWithFirstOrLast(fromScope *scope, aggCols *[]scopeColumn) {
	paramColName := b.factory.Memo().Metadata().ColumnMeta(1).Alias

	// if exist time_window() and first(ts) or last(ts), need to add
	// time_window_start() or time_window_end to projection, and then
	// change id of param of first or last agg, more details in constructProjectionCol.
	existTimeWindow := false
	for key := range fromScope.groupby.groupStrs {
		if strings.Contains(key, "time_window(") {
			existTimeWindow = true
		}
	}

	// construct projection cols and add them to fromScope.groupby.aggInScope.cols
	// when use time_window() with first(ts) of last(ts).
	if existTimeWindow {
		if fromScope.checkAggExtendFlag(existFirstTS) {
			fromScope.setAggExtendFlag(timeWindowWithFirstLast)
			b.constructProjectionCol(fromScope, "time_window_start", paramColName, sqlbase.FirstAgg, aggCols)
		}
		if fromScope.checkAggExtendFlag(existLastTS) {
			fromScope.setAggExtendFlag(timeWindowWithFirstLast)
			b.constructProjectionCol(fromScope, "time_window_end", paramColName, sqlbase.LastAgg, aggCols)
		}
	}
}
