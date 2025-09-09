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
	"bytes"
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const (
	// ScopeTSPropDefault default of scope time series property
	ScopeTSPropDefault = 0
	// ScopeTSPropIsAggExpr is true when there is the agg expr.
	ScopeTSPropIsAggExpr = 1 << 0
	// ScopeFirstAgg identify here is parsing first agg.
	ScopeFirstAgg = 1 << 1
	// ScopeLastAgg identify here is parsing last agg.
	ScopeLastAgg = 1 << 2
	// ScopeLastError whether output error in last/first.
	ScopeLastError = 1 << 3
)

// scopeOrdinal identifies an ordinal position with a list of scope columns.
type scopeOrdinal int

// scope is used for the build process and maintains the variables that have
// been bound within the current scope as columnProps. Variables bound in the
// parent scope are also visible in this scope.
//
// See builder.go for more details.
type scope struct {
	builder *Builder
	parent  *scope
	cols    []scopeColumn

	// groupby is the structure that keeps the grouping metadata when this scope
	// includes aggregate functions or GROUP BY.
	groupby *groupby

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	// TODO(radu): this, together with some other fields below, belongs in a
	// context that is threaded through the calls instead of setting and resetting
	// it in the scope.
	inAgg bool

	// windows contains the set of window functions encountered while building
	// the current SELECT statement.
	windows []scopeColumn

	// windowDefs is the set of named window definitions present in the nearest
	// SELECT.
	windowDefs []*tree.WindowDef

	// ordering records the ORDER BY columns associated with this scope. Each
	// column is either in cols or in extraCols.
	// Must not be modified in-place after being set.
	ordering opt.Ordering

	// when a LimitExpr, OffsetExpr or Ordinality exist in  fromSubquery,
	// there is no need to handle the OrderBy within the fromSubquery.
	noHandleOrderInFrom bool

	// distinctOnCols records the DISTINCT ON columns by ID.
	distinctOnCols opt.ColSet

	// extraCols contains columns specified by the ORDER BY or DISTINCT ON clauses
	// which don't appear in cols.
	extraCols []scopeColumn

	// expr is the SQL node built with this scope.
	expr memo.RelExpr

	procComm memo.ProcCommand

	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int

	// If replaceSRFs is true, replace raw SRFs with an srf struct. See
	// the replaceSRF() function for more details.
	replaceSRFs bool

	// singleSRFColumn is true if this scope has a single column that comes from
	// an SRF. The flag is used to allow renaming the column to the table alias.
	singleSRFColumn bool

	// srfs contains all the SRFs that were replaced in this scope. It will be
	// used by the Builder to convert the input from the FROM clause to a lateral
	// cross join between the input and a Zip of all the srfs in this slice.
	srfs []*srf

	// ctes contains the CTEs which were created at this scope. This set
	// is not exhaustive because expressions can reference CTEs from parent
	// scopes.
	ctes map[string]*cteSource

	// context is the current context in the SQL query (e.g., "SELECT" or
	// "HAVING"). It is used for error messages and to identify scoping errors
	// (e.g., aggregates are not allowed in the FROM clause of their own query
	// level).
	context exprType

	// atRoot is whether we are currently at a root context.
	atRoot bool

	// Gapfill function involved in this scope
	hasGapfill bool

	// interpolateWithLast is used to limit features not supported by last.
	// for specific restrictions, see the annotations to the struct definition.
	interpolateWithLast interpolateWithLast

	// TableType identifies what type the table is.
	TableType sqlbase.TableTypeMap

	// ScopeTSProp property of time series scope
	ScopeTSProp int

	//HasMultiTable is used only to disallow the use of last in join scenarios
	HasMultiTable bool

	AggExHelper aggExtendHelper
}

type aggExtendHelper struct {
	// minCount records the count of min agg functions,
	// only one min can apply agg extend.
	minCount int

	// maxCount records the count of max agg functions,
	// only one min can apply agg extend.
	maxCount int

	// flags helps check if can apply agg extend.
	flags int

	// aggCount records the count of all agg functions,
	// used to determine whether there are non agg columns.
	aggCount int

	// minOrMaxColID records the col id of the param of min or max agg,
	// used to construct min_extend of max_extend.
	minOrMaxColID opt.ColumnID

	// extendColSet records the col ids which can apply agg extend.
	extendColSet opt.ColSet

	// bType represents the stage of compilation.
	// only buildProjection stage add the extendColSet and skip
	// group by error.
	bType buildType
}

const (
	// existFirstTS is setted when use first(ts) in single ts table.
	existFirstTS = 1 << 0

	// existLastTS is setted when use last(ts) in single ts table.
	existLastTS = 1 << 1

	// existOtherAgg is true when there are appearing aggs other than min or max.
	// eg: min(tag col),max(tag col),first(non timestamp col),last(non timestamp col), other aggs.
	existOtherAgg = 1 << 2

	// existNonAggCol is true when there are appearing non agg cols.
	existNonAggCol = 1 << 3

	// singleTsTable is true when there is a single ts table.
	isSingleTsTable = 1 << 4

	// timeWindowWithFirstLast is true when use time_window() with last() or first().
	timeWindowWithFirstLast = 1 << 5
)

func (s *scope) setAggExtendFlag(flag int) {
	s.AggExHelper.flags |= flag
}

func (s *scope) checkAggExtendFlag(flag int) bool {
	return s.AggExHelper.flags&flag > 0
}

// interpolateWithLast is used to limit unsupported features.
// When the interpolate function has a parameter of last,
// the first argument to time_bucket_gapfill must be
// the first column of the time_series table.
type interpolateWithLast struct {
	// Whether last is an argument to interpolate function in this scope
	interpolateHasLast bool

	// Whether the first time_series column is an argument to
	// time_bucket_gapfill function in this scope
	gapfillWithFirstCol bool
}

// cteSource represents a CTE in the given query.
type cteSource struct {
	id           opt.WithID
	name         tree.AliasClause
	cols         physical.Presentation
	originalExpr tree.Statement
	expr         memo.RelExpr
	// If set, this function is called when a CTE is referenced. It can throw an
	// error.
	onRef func()
}

// exprType is used to represent the type of the current expression in the
// SQL query.
type exprType int8

const (
	exprTypeNone exprType = iota
	exprTypeAlterTableSplitAt
	exprTypeDistinctOn
	exprTypeFrom
	exprTypeGroupBy
	exprTypeHaving
	exprTypeLateralJoin
	exprTypeLimit
	exprTypeOffset
	exprTypeOn
	exprTypeOrderBy
	exprTypeReturning
	exprTypeSelect
	exprTypeValues
	exprTypeWhere
	exprTypeWindowFrameStart
	exprTypeWindowFrameEnd
	exprTypeProcedure
)

// buildType is used to represent the type of the current expression in the
// SQL query.
type buildType int8

const (
	buildNone buildType = iota
	buildProjection
	buildOrderBy
	buildAggregation
)

var exprTypeName = [...]string{
	exprTypeNone:              "",
	exprTypeAlterTableSplitAt: "ALTER TABLE SPLIT AT",
	exprTypeDistinctOn:        "DISTINCT ON",
	exprTypeFrom:              "FROM",
	exprTypeGroupBy:           "GROUP BY",
	exprTypeHaving:            "HAVING",
	exprTypeLateralJoin:       "LATERAL JOIN",
	exprTypeLimit:             "LIMIT",
	exprTypeOffset:            "OFFSET",
	exprTypeOn:                "ON",
	exprTypeOrderBy:           "ORDER BY",
	exprTypeReturning:         "RETURNING",
	exprTypeSelect:            "SELECT",
	exprTypeValues:            "VALUES",
	exprTypeWhere:             "WHERE",
	exprTypeWindowFrameStart:  "WINDOW FRAME START",
	exprTypeWindowFrameEnd:    "WINDOW FRAME END",
	exprTypeProcedure:         "PROCEDURE",
}

func (k exprType) String() string {
	if k < 0 || k > exprType(len(exprTypeName)-1) {
		return fmt.Sprintf("exprType(%d)", k)
	}
	return exprTypeName[k]
}

// initGrouping initializes the groupby information for this scope.
func (s *scope) initGrouping() {
	if s.groupby != nil {
		panic(errors.AssertionFailedf("grouping initialized twice"))
	}
	s.groupby = &groupby{
		aggInScope:  s.replace(),
		aggOutScope: s.replace(),
	}
}

// inGroupingContext returns true if initGrouping was called. This is the
// case when the builder is building expressions in a SELECT list, and
// aggregates, GROUP BY, or HAVING are present. This is also true when the
// builder is building expressions inside the HAVING clause. When
// inGroupingContext returns true, groupByStrSet will be utilized to enforce
// scoping rules. See the comment above groupByStrSet for more details.
func (s *scope) inGroupingContext() bool {
	return s.groupby != nil
}

// push creates a new scope with this scope as its parent.
func (s *scope) push() *scope {
	r := s.builder.allocScope()
	r.parent = s
	return r
}

// replace creates a new scope with the parent of this scope as its parent.
func (s *scope) replace() *scope {
	r := s.builder.allocScope()
	r.parent = s.parent
	return r
}

// appendColumnsFromScope adds newly bound variables to this scope.
// The expressions in the new columns are reset to nil.
func (s *scope) appendColumnsFromScope(src *scope) {
	l := len(s.cols)
	s.cols = append(s.cols, src.cols...)
	// We want to reset the expressions, as these become pass-through columns in
	// the new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// appendColumnsFromTable adds all columns from the given table metadata to this
// scope.
func (s *scope) appendColumnsFromTable(tabMeta *opt.TableMeta, alias *tree.TableName) {
	tab := tabMeta.Table
	if s.cols == nil {
		s.cols = make([]scopeColumn, 0, tab.ColumnCount())
	}
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabCol := tab.Column(i)
		s.cols = append(s.cols, scopeColumn{
			name:   tabCol.ColName(),
			table:  *alias,
			typ:    tabCol.DatumType(),
			id:     tabMeta.MetaID.ColumnID(i),
			hidden: tabCol.IsHidden(),
		})
	}
}

// appendColumns adds newly bound variables to this scope.
// The expressions in the new columns are reset to nil.
func (s *scope) appendColumns(cols []scopeColumn) {
	l := len(s.cols)
	s.cols = append(s.cols, cols...)
	// We want to reset the expressions, as these become pass-through columns in
	// the new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// appendColumn adds a newly bound variable to this scope.
// The expression in the new column is reset to nil.
func (s *scope) appendColumn(col *scopeColumn) {
	s.cols = append(s.cols, *col)
	// We want to reset the expression, as this becomes a pass-through column in
	// the new scope.
	s.cols[len(s.cols)-1].scalar = nil
}

// addExtraColumns adds the given columns as extra columns, ignoring any
// duplicate columns that are already in the scope.
func (s *scope) addExtraColumns(cols []scopeColumn) {
	existing := s.colSetWithExtraCols()
	for i := range cols {
		if !existing.Contains(cols[i].id) {
			s.extraCols = append(s.extraCols, cols[i])
		}
	}
}

// setOrdering sets the ordering in the physical properties and adds any new
// columns as extra columns.
func (s *scope) setOrdering(cols []scopeColumn, ord opt.Ordering) {
	s.addExtraColumns(cols)
	s.ordering = ord
}

// copyOrdering copies the ordering and the ORDER BY columns from the src scope.
// The groups in the new columns are reset to 0.
func (s *scope) copyOrdering(src *scope) {
	s.ordering = src.ordering
	if src.ordering.Empty() {
		return
	}
	// Copy any columns that the scope doesn't already have.
	existing := s.colSetWithExtraCols()
	for _, ordCol := range src.ordering {
		if !existing.Contains(ordCol.ID()) {
			col := *src.getColumn(ordCol.ID())
			// We want to reset the group, as this becomes a pass-through column in
			// the new scope.
			col.scalar = nil
			s.extraCols = append(s.extraCols, col)
		}
	}
}

// getColumn returns the scopeColumn with the given id (either in cols or
// extraCols).
func (s *scope) getColumn(col opt.ColumnID) *scopeColumn {
	for i := range s.cols {
		if s.cols[i].id == col {
			return &s.cols[i]
		}
	}
	for i := range s.extraCols {
		if s.extraCols[i].id == col {
			return &s.extraCols[i]
		}
	}
	return nil
}

func (s *scope) makeColumnTypes() []*types.T {
	res := make([]*types.T, len(s.cols))
	for i := range res {
		res[i] = s.cols[i].typ
	}
	return res
}

// makeOrderingChoice returns an OrderingChoice that corresponds to s.ordering.
func (s *scope) makeOrderingChoice() physical.OrderingChoice {
	var oc physical.OrderingChoice
	oc.FromOrdering(s.ordering)
	return oc
}

// makePhysicalProps constructs physical properties using the columns in the
// scope for presentation and s.ordering for required ordering.
func (s *scope) makePhysicalProps() *physical.Required {
	p := &physical.Required{
		Presentation: s.makePresentation(),
	}
	p.Ordering.FromOrdering(s.ordering)
	return p
}

func (s *scope) makePresentation() physical.Presentation {
	if len(s.cols) == 0 {
		return nil
	}
	presentation := make(physical.Presentation, 0, len(s.cols))
	for i := range s.cols {
		col := &s.cols[i]
		if !col.hidden {
			// time series precompute need typ attributes.
			presentation = append(presentation, opt.AliasedColumn{
				Alias: string(col.name),
				ID:    col.id,
				Typ:   col.typ,
			})
		}
	}
	return presentation
}

// makePresentationWithHiddenCols is only used when constructing the
// presentation for a [ ... ]-style data source.
func (s *scope) makePresentationWithHiddenCols() physical.Presentation {
	if len(s.cols) == 0 {
		return nil
	}
	presentation := make(physical.Presentation, 0, len(s.cols))
	for i := range s.cols {
		col := &s.cols[i]
		presentation = append(presentation, opt.AliasedColumn{
			Alias: string(col.name),
			ID:    col.id,
		})
	}
	return presentation
}

// walkExprTree walks the given expression and performs name resolution,
// replaces unresolved column names with columnProps, and replaces subqueries
// with typed subquery structs.
func (s *scope) walkExprTree(expr tree.Expr) tree.Expr {
	// TODO(peter): The caller should specify the desired number of columns. This
	// is needed when a subquery is used by an UPDATE statement.
	// TODO(andy): shouldn't this be part of the desired type rather than yet
	// another parameter?
	s.columns = 1

	expr, _ = tree.WalkExpr(s, expr)
	s.builder.semaCtx.IVarContainer = s
	return expr
}

// lookupCTE only looks up a CTE name in this and the parent scopes but doesn't reference to the CTE,
// returning nil if it's not found.
func (s *scope) lookupCTE(name *tree.TableName) *cteSource {
	var nameStr string
	seenCTEs := false
	for s != nil {
		if s.ctes != nil {
			// Only compute the stringified name if we see any CTEs.
			if !seenCTEs {
				nameStr = name.String()
				seenCTEs = true
			}
			if cte, ok := s.ctes[nameStr]; ok {
				return cte
			}
		}
		s = s.parent
	}
	return nil
}

// resolveCTE looks up a CTE name in this and the parent scopes, returning nil
// if it's not found.
func (s *scope) resolveCTE(name *tree.TableName) *cteSource {
	var nameStr string
	seenCTEs := false
	for s != nil {
		if s.ctes != nil {
			// Only compute the stringified name if we see any CTEs.
			if !seenCTEs {
				nameStr = name.String()
				seenCTEs = true
			}
			if cte, ok := s.ctes[nameStr]; ok {
				if cte.onRef != nil {
					cte.onRef()
				}
				return cte
			}
		}
		s = s.parent
	}
	return nil
}

// resolveType converts the given expr to a tree.TypedExpr. As part of the
// conversion, it performs name resolution, replaces unresolved column names
// with columnProps, and replaces subqueries with typed subquery structs.
//
// The desired type is a suggestion, but resolveType does not throw an error if
// the resolved type turns out to be different from desired (in contrast to
// resolveAndRequireType, which throws an error). If the result type is
// types.Unknown, then resolveType will wrap the expression in a type cast in
// order to produce the desired type.
func (s *scope) resolveType(expr tree.Expr, desired *types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheck(expr, s.builder.semaCtx, desired)
	if err != nil {
		panic(err)
	}
	return s.ensureNullType(texpr, desired)
}

// resolveAndRequireType converts the given expr to a tree.TypedExpr. As part
// of the conversion, it performs name resolution, replaces unresolved
// column names with columnProps, and replaces subqueries with typed subquery
// structs.
//
// If the resolved type does not match the desired type, resolveAndRequireType
// throws an error (in contrast to resolveType, which returns the typed
// expression with no error). If the result type is types.Unknown, then
// resolveType will wrap the expression in a type cast in order to produce the
// desired type.
func (s *scope) resolveAndRequireType(expr tree.Expr, desired *types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheckAndRequire(expr, s.builder.semaCtx, desired, s.context.String())
	if err != nil {
		panic(err)
	}

	return s.ensureNullType(texpr, desired)
}

// ensureNullType tests the type of the given expression. If types.Unknown, then
// ensureNullType wraps the expression in a CAST to the desired type (assuming
// it is not types.Any). types.Unknown is a special type used for null values,
// and can be cast to any other type.
func (s *scope) ensureNullType(texpr tree.TypedExpr, desired *types.T) tree.TypedExpr {
	if desired.Family() != types.AnyFamily && texpr.ResolvedType().Family() == types.UnknownFamily {
		var err error
		texpr, err = tree.NewTypedCastExpr(texpr, desired)
		if err != nil {
			panic(err)
		}
	}
	return texpr
}

// isOuterColumn returns true if the given column is not present in the current
// scope (it may or may not be present in an ancestor scope).
func (s *scope) isOuterColumn(id opt.ColumnID) bool {
	for i := range s.cols {
		col := &s.cols[i]
		if col.id == id {
			return false
		}
	}

	for i := range s.windows {
		w := &s.windows[i]
		if w.id == id {
			return false
		}
	}

	return true
}

// colSet returns a ColSet of all the columns in this scope,
// excluding orderByCols.
func (s *scope) colSet() opt.ColSet {
	var colSet opt.ColSet
	for i := range s.cols {
		colSet.Add(s.cols[i].id)
	}
	return colSet
}

// colSetWithExtraCols returns a ColSet of all the columns in this scope,
// including extraCols.
func (s *scope) colSetWithExtraCols() opt.ColSet {
	colSet := s.colSet()
	for i := range s.extraCols {
		colSet.Add(s.extraCols[i].id)
	}
	return colSet
}

// hasSameColumns returns true if this scope has the same columns
// as the other scope.
//
// NOTE: This function is currently only called by
// Builder.constructProjectForScope, which uses it to determine whether or not
// to construct a projection. Since the projection includes the extra columns,
// this check is sufficient to determine whether or not the projection is
// necessary. Be careful if using this function for another purpose.
func (s *scope) hasSameColumns(other *scope) bool {
	return s.colSetWithExtraCols().Equals(other.colSetWithExtraCols())
}

// removeHiddenCols removes hidden columns from the scope (and moves them to
// extraCols, in case they are referenced by ORDER BY or DISTINCT ON).
func (s *scope) removeHiddenCols() {
	n := 0
	for i := range s.cols {
		if s.cols[i].hidden {
			s.extraCols = append(s.extraCols, s.cols[i])
		} else {
			if n != i {
				s.cols[n] = s.cols[i]
			}
			n++
		}
	}
	s.cols = s.cols[:n]
}

// isAnonymousTable returns true if the table name of the first column
// in this scope is empty.
func (s *scope) isAnonymousTable() bool {
	return len(s.cols) > 0 && s.cols[0].table.TableName == ""
}

// setTableAlias qualifies the names of all columns in this scope with the
// given alias name, as if they were part of a table with that name. If the
// alias is the empty string, then setTableAlias removes any existing column
// qualifications, as if the columns were part of an "anonymous" table.
func (s *scope) setTableAlias(alias tree.Name) {
	tn := tree.MakeUnqualifiedTableName(alias)
	for i := range s.cols {
		s.cols[i].table = tn
	}
}

// See (*scope).findExistingCol.
func findExistingColInList(
	expr tree.TypedExpr, cols []scopeColumn, allowSideEffects bool,
) *scopeColumn {
	exprStr := symbolicExprStr(expr)
	for i := range cols {
		col := &cols[i]
		if expr == col {
			return col
		}
		if exprStr == col.getExprStr() {
			if allowSideEffects || col.scalar == nil {
				return col
			}
			var p props.Shared
			memo.BuildSharedProps(col.scalar, &p)
			if !p.CanHaveSideEffects {
				return col
			}
		}
	}
	return nil
}

// findExistingCol finds the given expression among the bound variables in this
// scope. Returns nil if the expression is not found (or an expression is found
// but it has side-effects and allowSideEffects is false).
func (s *scope) findExistingCol(expr tree.TypedExpr, allowSideEffects bool) *scopeColumn {
	return findExistingColInList(expr, s.cols, allowSideEffects)
}

// startAggFunc is called when the builder starts building an aggregate
// function. It is used to disallow nested aggregates and ensure that a
// grouping error is not called on the aggregate arguments. For example:
//
//	SELECT max(v) FROM kv GROUP BY k
//
// should not throw an error, even though v is not a grouping column.
// Non-grouping columns are allowed inside aggregate functions.
//
// startAggFunc returns a temporary scope for building the aggregate arguments.
// It is not possible to know the correct scope until the arguments are fully
// built. At that point, endAggFunc can be used to find the correct scope.
// If endAggFunc returns a different scope than startAggFunc, the columns
// will be transferred to the correct scope by buildAggregateFunction.
func (s *scope) startAggFunc() *scope {
	if s.inAgg {
		panic(sqlbase.NewAggInAggError())
	}
	s.inAgg = true

	if s.groupby == nil {
		return s.builder.allocScope()
	}
	return s.groupby.aggInScope
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and prevent grouping errors while building aggregate arguments.
//
// In addition, endAggFunc finds the correct groupby structure, given
// that the aggregate references the columns in cols. The reference scope
// is the one closest to the current scope which contains at least one of the
// variables referenced by the aggregate (or the current scope if the aggregate
// references no variables). endAggFunc also ensures that aggregate functions
// are only used in a groupings scope.
func (s *scope) endAggFunc(aggInfo aggregateInfo) (g *groupby) {
	if !s.inAgg {
		panic(errors.AssertionFailedf("mismatched calls to start/end aggFunc"))
	}
	s.inAgg = false

	for curr := s; curr != nil; curr = curr.parent {
		var colSet opt.ColSet = curr.colSet()

		if aggInfo.colRefs.Len() == 0 || aggInfo.colRefs.Intersects(colSet) || aggInfo.Func.FunctionName() == Interpolate {
			curr.verifyAggregateContext(aggInfo.def.Name)
			if curr.groupby == nil {
				curr.initGrouping()
			}
			return curr.groupby
		}
	}

	panic(errors.AssertionFailedf("aggregate function is not allowed in this context, illegal function: %v", aggInfo.def.Name))
}

// verifyAggregateContext checks that the current scope is allowed to contain
// aggregate functions.
func (s *scope) verifyAggregateContext(aggName string) {
	if s.inAgg {
		panic(sqlbase.NewAggInAggError())
	}

	switch s.context {
	case exprTypeLateralJoin:
		panic(pgerror.Newf(pgcode.Grouping,
			"aggregate functions are not allowed in FROM clause of their own query level, illegal function: %v", aggName))

	case exprTypeOn:
		panic(pgerror.Newf(pgcode.Grouping,
			"aggregate functions are not allowed in JOIN conditions, illegal function: %v", aggName))

	case exprTypeWhere, exprTypeProcedure:
		panic(tree.NewInvalidFunctionUsageError(tree.AggregateClass, s.context.String()))
	}
}

// scope implements the tree.Visitor interface so that it can walk through
// a tree.Expr tree, perform name resolution, and replace unresolved column
// names with a scopeColumn. The info stored in scopeColumn is necessary for
// Builder.buildScalar to construct a "variable" memo expression.
var _ tree.Visitor = &scope{}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scope) ColumnSourceMeta() {}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scopeColumn) ColumnSourceMeta() {}

// ColumnResolutionResult implements the tree.ColumnResolutionResult interface.
func (*scopeColumn) ColumnResolutionResult() {}

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colName tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	var candidateFromAnonSource *scopeColumn
	var candidateWithPrefix *scopeColumn
	var hiddenCandidate *scopeColumn
	var moreThanOneCandidateFromAnonSource bool
	var moreThanOneCandidateWithPrefix bool
	var moreThanOneHiddenCandidate bool

	// We only allow hidden columns in the current scope. Hidden columns
	// in parent scopes are not accessible.
	allowHidden := true

	// If multiple columns match c in the same scope, we return an error
	// due to ambiguity. If no columns match in the current scope, we
	// search the parent scope. If the column is not found in any of the
	// ancestor scopes, we return an error.
	reportBackfillError := false
	for ; s != nil; s, allowHidden = s.parent, false {
		for i := range s.cols {
			col := &s.cols[i]
			if col.name != colName {
				continue
			}

			s.limitLastFirst(col.id)

			// If the matching column is a mutation column, then act as if it's not
			// present so that matches in higher scopes can be found. However, if
			// no match is found in higher scopes, report a backfill error rather
			// than a "not found" error.
			if col.mutation {
				reportBackfillError = true
				continue
			}

			if col.table.TableName == "" && !col.hidden {
				if candidateFromAnonSource != nil {
					moreThanOneCandidateFromAnonSource = true
					break
				}
				candidateFromAnonSource = col
			} else if !col.hidden {
				if candidateWithPrefix != nil {
					moreThanOneCandidateWithPrefix = true
				}
				candidateWithPrefix = col
			} else if allowHidden {
				if hiddenCandidate != nil {
					moreThanOneHiddenCandidate = true
				}
				hiddenCandidate = col
			}
		}

		// The table name was unqualified, so if a single anonymous source exists
		// with a matching non-hidden column, use that.
		if moreThanOneCandidateFromAnonSource {
			return nil, nil, -1, s.newAmbiguousColumnError(
				colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}
		if candidateFromAnonSource != nil {
			return &candidateFromAnonSource.table, candidateFromAnonSource, int(candidateFromAnonSource.id), nil
		}

		// Else if a single named source exists with a matching non-hidden column,
		// use that.
		if candidateWithPrefix != nil && !moreThanOneCandidateWithPrefix {
			return &candidateWithPrefix.table, candidateWithPrefix, int(candidateWithPrefix.id), nil
		}
		if moreThanOneCandidateWithPrefix || moreThanOneHiddenCandidate {
			return nil, nil, -1, s.newAmbiguousColumnError(
				colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}

		// One last option: if a single source exists with a matching hidden
		// column, use that.
		if hiddenCandidate != nil {
			return &hiddenCandidate.table, hiddenCandidate, int(hiddenCandidate.id), nil
		}
	}

	// Make a copy of colName so that passing a reference to tree.ErrString does
	// not cause colName to be allocated on the heap in the happy (no error) path
	// above.
	tmpName := colName
	if reportBackfillError {
		return nil, nil, -1, makeBackfillError(tmpName)
	}

	return nil, nil, -1, sqlbase.NewUndefinedColumnError(tree.ErrString(&tmpName))
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	err error,
) {
	// If multiple sources match tn in the same scope, we return an error
	// due to ambiguity. If no sources match in the current scope, we
	// search the parent scope. If the source is not found in any of the
	// ancestor scopes, we return an error.
	var source tree.TableName
	for ; s != nil; s = s.parent {
		sources := make(map[tree.TableName]struct{})
		for i := range s.cols {
			sources[s.cols[i].table] = struct{}{}
		}

		found := false
		for src := range sources {
			if !sourceNameMatches(src, tn) {
				continue
			}
			if found {
				return tree.MoreThanOne, nil, s, newAmbiguousSourceError(&tn)
			}
			found = true
			source = src
		}

		if found {
			return tree.ExactlyOne, &source, s, nil
		}
	}

	return tree.NoResults, nil, s, nil
}

// sourceNameMatches checks whether a request for table name toFind
// can be satisfied by the FROM source name srcName.
//
// For example:
// - a request for "kv" is matched by a source named "db1.public.kv"
// - a request for "public.kv" is not matched by a source named just "kv"
func sourceNameMatches(srcName tree.TableName, toFind tree.TableName) bool {
	if srcName.TableName != toFind.TableName {
		return false
	}
	if toFind.ExplicitSchema {
		if srcName.SchemaName != toFind.SchemaName {
			return false
		}
		if toFind.ExplicitCatalog {
			if srcName.CatalogName != toFind.CatalogName {
				return false
			}
		}
	}
	return true
}

// Resolve is part of the tree.ColumnItemResolver interface.
func (s *scope) Resolve(
	_ context.Context,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	colName tree.Name,
) (tree.ColumnResolutionResult, error) {
	if colHint >= 0 {
		// Column was found by FindSourceProvidingColumn above.
		return srcMeta.(*scopeColumn), nil
	}

	// Otherwise, a table is known but not the column yet.
	inScope, ok := srcMeta.(*scope)
	if !ok {
		str, ok := srcMeta.(*tree.DString)
		if !ok {
			return nil, sqlbase.NewUndefinedColumnError(tree.ErrString(&colName))
		}
		return str, nil
	}
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if col.name == colName && sourceNameMatches(*prefix, col.table) {
			s.limitLastFirst(col.id)
			return col, nil
		}
	}

	return nil, sqlbase.NewUndefinedColumnError(tree.ErrString(tree.NewColumnItem(prefix, colName)))
}

func makeUntypedTuple(labels []string, texprs []tree.TypedExpr) *tree.Tuple {
	exprs := make(tree.Exprs, len(texprs))
	for i, e := range texprs {
		exprs[i] = e
	}
	return &tree.Tuple{Exprs: exprs, Labels: labels}
}

type varContainer []tree.Datum

func (d varContainer) Add(datum tree.Datum) { d = append(d, datum) }

func (d varContainer) Value(idx int) tree.Datum { return d[idx] }

func (d varContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return d[idx].Eval(ctx)
}

func (d varContainer) IndexedVarResolvedType(idx int) *types.T {
	return d[idx].ResolvedType()
}

func (d varContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("var%d", idx))
	return &n
}

// VisitPre is part of the Visitor interface.
//
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, *tree.TupleStar:
		// AllColumnsSelectors and TupleStars at the top level of a SELECT clause
		// are replaced when the select's renders are prepared. If we
		// encounter one here during expression analysis, it's being used
		// as an argument to an inner expression/function. In that case,
		// treat it as a tuple of the expanded columns.
		//
		// Hence:
		//    SELECT kv.* FROM kv                 -> SELECT k, v FROM kv
		//    SELECT (kv.*) FROM kv               -> SELECT (k, v) FROM kv
		//    SELECT COUNT(DISTINCT kv.*) FROM kv -> SELECT COUNT(DISTINCT (k, v)) FROM kv
		//
		labels, exprs := s.builder.expandStar(expr, s, false)
		// We return an untyped tuple because name resolution occurs
		// before type checking, and type checking will resolve the
		// tuple's type. However we need to preserve the labels in
		// case of e.g. `SELECT (kv.*).v`.
		return false, makeUntypedTuple(labels, exprs)

	case *tree.UnresolvedName:
		// if we are in trigger compilation, replace new/old expr with placeholder expr
		if s.builder.insideObjectDef.HasFlags(InsideTriggerDef) && t.NumParts == 2 {
			found := false
			if strings.ToLower(t.Parts[1]) == triggerColNew {
				// Perform semantic validation on NEW.col values using column information stored in the builder
				for index, v := range s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas {
					if t.Parts[0] == v.Name {
						found = true
						// avoid placeholder being optimized, maintain the placeholder until the execution phase for replacement.
						s.builder.KeepPlaceholders = true
						// Record the occurrence of the new.col expression in the current compilation phase for upper-layer validation of new/old compliance.
						// For example, insert can only use new
						s.builder.TriggerInfo.TriggerNewOldTyp |= newCol
						idx := index
						if s.builder.isTriggerUpdate() {
							// For update operations, special handling is required for the NEW row values in triggers.
							idx += int(opt.ColumnID(len(s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas)))
							for idx+1 > len(s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types) {
								s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = append(s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types, v.Type)
							}
						}
						return false, &tree.Placeholder{
							Idx:       tree.PlaceholderIdx(idx),
							IsTrigger: true,
						}
					}
				}
				if !found {
					panic(pgerror.Newf(pgcode.UndefinedColumn, "column new.%s not found", t.Parts[0]))
				}
			}
			if strings.ToLower(t.Parts[1]) == triggerColOld {
				// Perform semantic validation on OLD.col values using column information stored in the builder
				for index, v := range s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas {
					if t.Parts[0] == v.Name {
						found = true
						s.builder.KeepPlaceholders = true
						s.builder.TriggerInfo.TriggerNewOldTyp |= oldCol
						return false, &tree.Placeholder{
							Idx:       tree.PlaceholderIdx(index),
							IsTrigger: true,
						}
					}
				}
				if !found {
					panic(pgerror.Newf(pgcode.UndefinedColumn, "column old.%s not found", t.Parts[0]))
				}
			}
		}
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		ok, colI := s.VisitPre(vn)
		return ok, colI

	case *tree.ColumnItem:
		// if we are in trigger compilation, replace new/old expr with placeholder expr
		if s.builder.insideObjectDef.HasFlags(InsideTriggerDef) && t.TableName != nil && t.TableName.NumParts == 1 {
			found := false
			if strings.ToLower(t.TableName.Parts[0]) == triggerColNew {
				// Perform semantic validation on NEW.col values using column information stored in the builder
				for index, v := range s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas {
					if t.Column() == v.Name {
						found = true
						// avoid placeholder being optimized, maintain the placeholder until the execution phase for replacement.
						s.builder.KeepPlaceholders = true
						// Record the occurrence of the new.col expression in the current compilation phase for upper-layer validation of new/old compliance.
						// For example, insert can only use new
						s.builder.TriggerInfo.TriggerNewOldTyp |= newCol
						idx := index
						if s.builder.isTriggerUpdate() {
							// For update operations, special handling is required for the NEW row values in triggers.
							idx += int(opt.ColumnID(len(s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas)))
							for idx+1 > len(s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types) {
								s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types = append(s.builder.semaCtx.TriggerColHolders.PlaceholderTypesInfo.Types, v.Type)
							}
						}
						return false, &tree.Placeholder{
							Idx:       tree.PlaceholderIdx(idx),
							IsTrigger: true,
						}
					}
				}
				if !found {
					panic(pgerror.Newf(pgcode.UndefinedColumn, "column new.%s not found", t.Column()))
				}
			}
			if strings.ToLower(t.TableName.Parts[0]) == triggerColOld {
				// Perform semantic validation on OLD.col values using column information stored in the builder
				for index, v := range s.builder.TriggerInfo.TriggerTab[s.builder.TriggerInfo.CurTriggerTabID].ColMetas {
					if t.Column() == v.Name {
						found = true
						s.builder.KeepPlaceholders = true
						s.builder.TriggerInfo.TriggerNewOldTyp |= oldCol
						return false, &tree.Placeholder{
							Idx:       tree.PlaceholderIdx(index),
							IsTrigger: true,
						}
					}
				}
				if !found {
					panic(pgerror.Newf(pgcode.UndefinedColumn, "column old.%s not found", t.Column()))
				}
			}
		}
		colI, err := t.Resolve(s.builder.ctx, s)
		if err != nil {
			panic(err)
		}
		col := colI.(*scopeColumn)

		return false, col

	case *tree.FuncExpr:
		if n, ok := t.Func.FunctionReference.(*tree.UnresolvedName); ok && (s.context == exprTypeSelect) {
			if n.Parts[0] == Gapfill {
				if s.hasGapfill {
					panic(pgerror.Newf(pgcode.Warning, "%s can only be used once in the select list", Gapfill))
				} else {
					n.Parts[0] = Gapfillinternal
					s.hasGapfill = true
					s.builder.factory.Memo().SetFlag(opt.HasGapFill)
				}
			}
		}
		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			panic(err)
		}

		// 1.time_window_end() and time_window_start() are user invisible, only for internal use, however,
		// when timeWindowWithFirstLast is setted, means they are being used internally, not need for error reporting.
		// 2.min_extend() and max_extend() are user invisible, only for internal use.
		if ((def.Name == "time_window_end" || def.Name == "time_window_start") && !s.checkAggExtendFlag(timeWindowWithFirstLast)) ||
			(def.Name == "min_extend" || def.Name == "max_extend") {
			panic(pgerror.Wrapf(pgerror.New(pgcode.ReservedName, "function reserved for internal use"), pgcode.ReservedName,
				"%s()", errors.Safe(def.Name)))
		}

		// UDF can not use memo cache
		if def != nil && def.ForbiddenExecInTSEngine {
			s.builder.DisableMemoReuse = true
		}
		if isGenerator(def) && s.replaceSRFs {
			expr = s.replaceSRF(t, def)
			break
		}

		if isAggregate(def) && t.WindowDef == nil {
			expr = s.replaceAggregate(t, def)
			// check the parameter of interpolate.
			// ex: select time_bucket_gapfill(k_timestamp,1),interpolate(avg(e3), NEXT) from t1
			// group by time_bucket_gapfill(k_timestamp,1) order by time_bucket_gapfill(k_timestamp,1);
			// the first parameter of the interpolate function must be numeric type.
			if agg, ok := expr.(*aggregateInfo); ok {
				if getAggName(*agg) == Interpolate {
					if expr, ok := agg.args[0].(*memo.VariableExpr); ok {
						switch expr.Typ.InternalType.Family {
						case types.IntFamily, types.FloatFamily, types.DecimalFamily:
							break
						default:
							panic(pgerror.New(pgcode.Warning, "The type of the first parameter of interpolate must be of IntFamily, FloatFamily or DecimalFamily"))
						}
					}
				}
			}
			break
		}

		if t.WindowDef != nil {
			expr = s.replaceWindowFn(t, def)
			break
		}

	case *tree.ArrayFlatten:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			// Copy the ArrayFlatten expression so that the tree isn't mutated.
			copy := *t
			copy.Subquery = s.replaceSubquery(
				sub, false /* wrapInTuple */, 1 /* desiredNumColumns */, extraColsAllowed,
			)
			expr = &copy
		}

	case *tree.ComparisonExpr:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				// Copy the Comparison expression so that the tree isn't mutated.
				copy := *t
				copy.Right = s.replaceSubquery(
					sub, true /* wrapInTuple */, -1 /* desiredNumColumns */, noExtraColsAllowed,
				)
				expr = &copy
			}
		}

	case *tree.Subquery:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171, #24170 and #24225.
			return false, expr
		}

		if t.Exists {
			expr = s.replaceSubquery(
				t, true /* wrapInTuple */, -1 /* desiredNumColumns */, noExtraColsAllowed,
			)
		} else {
			expr = s.replaceSubquery(
				t, false /* wrapInTuple */, s.columns /* desiredNumColumns */, noExtraColsAllowed,
			)
		}
	}

	// Reset the desired number of columns since if the subquery is a child of
	// any other expression, type checking will verify the number of columns.
	s.columns = -1
	return true, expr
}

// replaceSRF returns an srf struct that can be used to replace a raw SRF. When
// this struct is encountered during the build process, it is replaced with a
// reference to the column returned by the SRF (if the SRF returns a single
// column) or a tuple of column references (if the SRF returns multiple
// columns).
//
// replaceSRF also stores a pointer to the new srf struct in this scope's srfs
// slice. The slice is used later by the Builder to convert the input from
// the FROM clause to a lateral cross join between the input and a Zip of all
// the srfs in the s.srfs slice. See Builder.buildProjectSet in srfs.go for
// more details.
func (s *scope) replaceSRF(f *tree.FuncExpr, def *tree.FunctionDefinition) *srf {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require(s.context.String(),
		tree.RejectAggregates|tree.RejectWindowApplications|tree.RejectNestedGenerators)

	expr := f.Walk(s)
	typedFunc, err := tree.TypeCheck(expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}

	srfScope := s.push()
	var outCol *scopeColumn

	var typedFuncExpr = typedFunc.(*tree.FuncExpr)
	if s.builder.shouldCreateDefaultColumn(typedFuncExpr) {
		outCol = s.builder.addColumn(srfScope, def.Name, typedFunc, false)
	}
	out := s.builder.buildFunction(typedFuncExpr, s, srfScope, outCol, nil)
	srf := &srf{
		FuncExpr: typedFuncExpr,
		cols:     srfScope.cols,
		fn:       out,
	}
	s.srfs = append(s.srfs, srf)

	// Add the output columns to this scope, so the column references added
	// by the build process will not be treated as outer columns.
	s.cols = append(s.cols, srf.cols...)
	return srf
}

// replaceAggregate returns an aggregateInfo that can be used to replace a raw
// aggregate function. When an aggregateInfo is encountered during the build
// process, it is replaced with a reference to the column returned by the
// aggregation.
//
// replaceAggregate also stores the aggregateInfo in the aggregation scope for
// this aggregate, using the aggOutScope.groupby.aggs slice. The aggregation
// scope is the one closest to the current scope which contains at least one of
// the variables referenced by the aggregate (or the current scope if the
// aggregate references no variables). The aggOutScope.groupby.aggs slice is
// used later by the Builder to build aggregations in the aggregation scope.
func (s *scope) replaceAggregate(f *tree.FuncExpr, def *tree.FunctionDefinition) tree.Expr {
	f, def = s.replaceCount(f, def)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)
	if def.Name != Interpolate {
		s.builder.semaCtx.Properties.Require("aggregate",
			tree.RejectNestedAggregates|tree.RejectWindowApplications|tree.RejectGenerators)
	}

	s.AddScopeProperty(def.Name)
	expr := f.Walk(s)

	// Here we restrict last/first related agg to use
	// only in sequential scenarios.
	// Last queries in relational tables,
	// last queries in result sets,
	// last queries in multi-table associations are disabled.
	// Only last queries in the source ts table are allowed.
	if checkLastOrFirstAgg(f.Func.FunctionName()) {
		if checkFirstAgg(f.Func.FunctionName()) {
			s.builder.factory.TSFlags |= opt.HasFirst
		}
		// do not push query config in first agg case and when generating topic plan.
		if checkLastAgg(f.Func.FunctionName()) {
			s.builder.factory.TSFlags |= opt.HasLast
		}
		handleLastAgg(s, expr, f.Func.FunctionName())
	}

	// Processes TWA and Elapsed aggregation functions.
	if checkTwaOrElapsedAgg(f.Func.FunctionName()) {
		s.builder.handleTwaOrElapsedAgg(expr, f.Func.FunctionName())
	}
	s.DelScopeProperty(def.Name)

	// Update this scope to indicate that we are now inside an aggregate function
	// so that any nested aggregates referencing this scope from a subquery will
	// return an appropriate error. The returned tempScope will be used for
	// building aggregate function arguments below in buildAggregateFunction.
	tempScope := s.startAggFunc()

	// We need to do this check here to ensure that we check the usage of special
	// functions with the right error message.
	if f.Filter != nil {
		func() {
			oldProps := s.builder.semaCtx.Properties
			defer func() { s.builder.semaCtx.Properties.Restore(oldProps) }()

			s.builder.semaCtx.Properties.Require("FILTER", tree.RejectSpecial)
			_, err := tree.TypeCheck(expr.(*tree.FuncExpr).Filter, s.builder.semaCtx, types.Any)
			if err != nil {
				panic(err)
			}
		}()
	}

	typedFunc, err := tree.TypeCheck(expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	if typedFunc == tree.DNull {
		return tree.DNull
	}

	f = typedFunc.(*tree.FuncExpr)

	private := memo.FunctionPrivate{
		Name:       def.Name,
		Properties: &def.FunctionProperties,
		Overload:   f.ResolvedOverload(),
	}

	return s.builder.buildAggregateFunction(f, &private, tempScope, s)
}

func (s *scope) lookupWindowDef(name tree.Name) *tree.WindowDef {
	for i := range s.windowDefs {
		if s.windowDefs[i].Name == name {
			return s.windowDefs[i]
		}
	}
	panic(pgerror.Newf(pgcode.UndefinedObject, "window %q does not exist", name))
}

func (s *scope) constructWindowDef(def tree.WindowDef) tree.WindowDef {
	switch {
	case def.RefName != "":
		// SELECT rank() OVER (w) FROM t WINDOW w AS (...)
		// We copy the referenced window specification, and modify it if necessary.
		result, err := tree.OverrideWindowDef(s.lookupWindowDef(def.RefName), def)
		if err != nil {
			panic(err)
		}
		return result

	case def.Name != "":
		// SELECT rank() OVER w FROM t WINDOW w AS (...)
		// Note the lack of parens around w, compared to the first case.
		// We use the referenced window specification directly, without modification.
		return *s.lookupWindowDef(def.Name)

	default:
		return def
	}
}

func (s *scope) replaceWindowFn(f *tree.FuncExpr, def *tree.FunctionDefinition) tree.Expr {
	f, def = s.replaceCount(f, def)

	if err := tree.CheckIsWindowOrAgg(def); err != nil {
		panic(err)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require("window",
		tree.RejectNestedWindowFunctions)

	// Make a copy of f so we can modify the WindowDef.
	fCopy := *f
	newWindowDef := s.constructWindowDef(*f.WindowDef)
	fCopy.WindowDef = &newWindowDef

	expr := fCopy.Walk(s)

	// Special processing for first/last query
	funcName := f.Func.FunctionName()
	// limit the use of some agg functions as window function
	checkUnsupportedWindowFunctions(funcName)

	typedFunc, err := tree.TypeCheck(expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	if typedFunc == tree.DNull {
		if funcName == tree.FunDiff {
			panic(pgerror.New(pgcode.Syntax, "invalid parameter data type: diff"))
		}
		return tree.DNull
	}

	f = typedFunc.(*tree.FuncExpr)

	// We will be performing type checking on expressions from PARTITION BY and
	// ORDER BY clauses below, and we need the semantic context to know that we
	// are in a window function. InWindowFunc is updated when type checking
	// FuncExpr above, but it is reset upon returning from that, so we need to do
	// this update manually.
	defer func(ctx *tree.SemaContext, prevWindow bool) {
		ctx.Properties.Derived.InWindowFunc = prevWindow
	}(
		s.builder.semaCtx,
		s.builder.semaCtx.Properties.Derived.InWindowFunc,
	)
	s.builder.semaCtx.Properties.Derived.InWindowFunc = true

	oldPartitions := f.WindowDef.Partitions
	f.WindowDef.Partitions = make(tree.Exprs, len(oldPartitions))
	for i, e := range oldPartitions {
		typedExpr := s.resolveType(e, types.Any)
		f.WindowDef.Partitions[i] = typedExpr
	}

	oldOrderBy := f.WindowDef.OrderBy
	f.WindowDef.OrderBy = make(tree.OrderBy, len(oldOrderBy))
	for i := range oldOrderBy {
		ord := *oldOrderBy[i]
		if ord.OrderType != tree.OrderByColumn {
			panic(errOrderByIndexInWindow)
		}
		typedExpr := s.resolveType(ord.Expr, types.Any)
		ord.Expr = typedExpr
		f.WindowDef.OrderBy[i] = &ord
	}

	if f.WindowDef.Frame != nil {
		if err := analyzeWindowFrame(s, f.WindowDef); err != nil {
			panic(err)
		}
	}

	info := windowInfo{
		FuncExpr: f,
		def: memo.FunctionPrivate{
			Name:       def.Name,
			Properties: &def.FunctionProperties,
			Overload:   f.ResolvedOverload(),
		},
	}

	if col := findExistingColInList(&info, s.windows, false /* allowSideEffects */); col != nil {
		return col.expr
	}

	info.col = &scopeColumn{
		name: tree.Name(def.Name),
		typ:  f.ResolvedType(),
		id:   s.builder.factory.Metadata().AddColumn(def.Name, f.ResolvedType()),
		expr: &info,
	}

	s.windows = append(s.windows, *info.col)

	return &info
}

var (
	errOrderByIndexInWindow = pgerror.New(pgcode.FeatureNotSupported, "ORDER BY INDEX in window definition is not supported")
)

// analyzeWindowFrame performs semantic analysis of offset expressions of
// the window frame.
func analyzeWindowFrame(s *scope, windowDef *tree.WindowDef) error {
	frame := windowDef.Frame
	bounds := frame.Bounds
	startBound, endBound := bounds.StartBound, bounds.EndBound
	var requiredType *types.T
	switch frame.Mode {
	case tree.ROWS:
		// In ROWS mode, offsets must be non-null, non-negative integers. Non-nullity
		// and non-negativity will be checked later.
		requiredType = types.Int
	case tree.RANGE:
		// In RANGE mode, offsets must be non-null and non-negative datums of a type
		// dependent on the type of the ordering column. Non-nullity and
		// non-negativity will be checked later.
		if bounds.HasOffset() {
			// At least one of the bounds is of type 'value' PRECEDING or 'value' FOLLOWING.
			// We require ordering on a single column that supports addition/subtraction.
			if len(windowDef.OrderBy) != 1 {
				return pgerror.Newf(pgcode.Windowing, "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column, illegal function: %v", windowDef.Name)
			}
			requiredType = windowDef.OrderBy[0].Expr.(tree.TypedExpr).ResolvedType()
			if !types.IsAdditiveType(requiredType) {
				return pgerror.Newf(pgcode.Windowing, fmt.Sprintf("RANGE with offset PRECEDING/FOLLOWING is not supported for column type %s", requiredType))
			}
			if types.IsDateTimeType(requiredType) {
				// Spec: for datetime ordering columns, the required type is an 'interval'.
				requiredType = types.Interval
			}
		}
	case tree.GROUPS:
		if len(windowDef.OrderBy) == 0 {
			return pgerror.Newf(pgcode.Windowing, "GROUPS mode requires an ORDER BY clause, illegal function: %v", windowDef.Name)
		}
		// In GROUPS mode, offsets must be non-null, non-negative integers.
		// Non-nullity and non-negativity will be checked later.
		requiredType = types.Int
	default:
		return errors.AssertionFailedf("unexpected WindowFrameMode: %d", errors.Safe(frame.Mode))
	}
	if startBound != nil && startBound.OffsetExpr != nil {
		oldContext := s.context
		s.context = exprTypeWindowFrameStart
		startBound.OffsetExpr = s.resolveAndRequireType(startBound.OffsetExpr, requiredType)
		s.context = oldContext
	}
	if endBound != nil && endBound.OffsetExpr != nil {
		oldContext := s.context
		s.context = exprTypeWindowFrameEnd
		endBound.OffsetExpr = s.resolveAndRequireType(endBound.OffsetExpr, requiredType)
		s.context = oldContext
	}
	return nil
}

// replaceCount replaces count(*) with count_rows().
func (s *scope) replaceCount(
	f *tree.FuncExpr, def *tree.FunctionDefinition,
) (*tree.FuncExpr, *tree.FunctionDefinition) {
	if len(f.Exprs) != 1 {
		return f, def
	}
	vn, ok := f.Exprs[0].(tree.VarName)
	if !ok {
		return f, def
	}
	vn, err := vn.NormalizeVarName()
	if err != nil {
		panic(err)
	}
	f.Exprs[0] = vn

	if strings.EqualFold(def.Name, "count") && f.Type == 0 {
		if _, ok := vn.(tree.UnqualifiedStar); ok {
			if f.Filter != nil {
				// If we have a COUNT(*) with a FILTER, we need to synthesize an input
				// for the aggregation to be over, because otherwise we have no input
				// to hang the AggFilter off of.
				// Thus, we convert
				//   COUNT(*) FILTER (WHERE foo)
				// to
				//   COUNT(true) FILTER (WHERE foo).
				cpy := *f
				e := &cpy
				e.Exprs = tree.Exprs{tree.DBoolTrue}

				newDef, err := e.Func.Resolve(s.builder.semaCtx.SearchPath)
				if err != nil {
					panic(err)
				}

				return e, newDef
			}

			// Special case handling for COUNT(*) with no FILTER. This is a special
			// construct to count the number of rows; in this case * does NOT refer
			// to a set of columns. A * is invalid elsewhere (and will be caught by
			// TypeCheck()).  Replace the function with COUNT_ROWS (which doesn't
			// take any arguments).
			e := &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: &tree.UnresolvedName{
						NumParts: 1, Parts: tree.NameParts{"count_rows"},
					},
				},
			}
			// We call TypeCheck to fill in FuncExpr internals. This is a fixed
			// expression; we should not hit an error here.
			if _, err := e.TypeCheck(&tree.SemaContext{}, types.Any); err != nil {
				panic(err)
			}
			newDef, err := e.Func.Resolve(s.builder.semaCtx.SearchPath)
			if err != nil {
				panic(err)
			}
			e.Filter = f.Filter
			e.WindowDef = f.WindowDef
			return e, newDef
		}
		// TODO(rytaft): Add handling for tree.AllColumnsSelector to support
		// expressions like SELECT COUNT(kv.*) FROM kv
		// Similar to the work done in PR #17833.
	}

	return f, def
}

func (s *scope) replacePredict(
	f *tree.FuncExpr, def *tree.FunctionDefinition,
) (recurse bool, newExpr tree.Expr) {
	if len(f.Exprs) != 1 {
		return true, f
	}

	vn, ok := f.Exprs[0].(tree.VarName)
	if !ok {
		return true, f
	}

	vn, err := vn.NormalizeVarName()
	if err != nil {
		panic(err)
	}

	return true, f
}

const (
	extraColsAllowed   = true
	noExtraColsAllowed = false
)

// Replace a raw tree.Subquery node with a lazily typed subquery. wrapInTuple
// specifies whether the return type of the subquery should be wrapped in a
// tuple. wrapInTuple is true for subqueries that may return multiple rows in
// comparison expressions (e.g., IN, ANY, ALL) and EXISTS expressions.
// desiredNumColumns specifies the desired number of columns for the subquery.
// Specifying -1 for desiredNumColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
// If extraColsAllowed is true, extra columns built from the subquery (such as
// columns for which orderings have been requested) will not be stripped away.
// It is the duty of the caller to ensure that those columns are eventually
// dealt with.
func (s *scope) replaceSubquery(
	sub *tree.Subquery, wrapInTuple bool, desiredNumColumns int, extraColsAllowed bool,
) *subquery {
	return &subquery{
		Subquery:          sub,
		wrapInTuple:       wrapInTuple,
		desiredNumColumns: desiredNumColumns,
		extraColsAllowed:  extraColsAllowed,
		scope:             s,
	}
}

// VisitPost is part of the Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// scope implements the IndexedVarContainer interface so it can be used as
// semaCtx.IVarContainer. This allows tree.TypeCheck to determine the correct
// type for any IndexedVars.
var _ tree.IndexedVarContainer = &scope{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("unimplemented: scope.IndexedVarEval"))
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarResolvedType(idx int) *types.T {
	if idx >= len(s.cols) {
		if len(s.cols) == 0 {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"column reference @%d not allowed in this context", idx+1))
		}
		panic(pgerror.Newf(pgcode.UndefinedColumn,
			"invalid column ordinal: @%d", idx+1))
	}
	return s.cols[idx].typ
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	panic(errors.AssertionFailedf("unimplemented: scope.IndexedVarNodeFormatter"))
}

// newAmbiguousColumnError returns an error with a helpful error message to be
// used in case of an ambiguous column reference.
func (s *scope) newAmbiguousColumnError(
	n tree.Name,
	allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate bool,
) error {
	colString := tree.ErrString(&n)
	var msgBuf bytes.Buffer
	sep := ""
	fmtCandidate := func(tn tree.TableName) {
		name := tree.ErrString(&tn)
		if len(name) == 0 {
			name = "<anonymous>"
		}
		fmt.Fprintf(&msgBuf, "%s%s.%s", sep, name, colString)
		sep = ", "
	}
	for i := range s.cols {
		col := &s.cols[i]
		if col.name == n && (allowHidden || !col.hidden) {
			if col.table.TableName == "" && !col.hidden {
				if moreThanOneCandidateFromAnonSource {
					// Only print first anonymous source, since other(s) are identical.
					fmtCandidate(col.table)
					break
				}
			} else if !col.hidden {
				if moreThanOneCandidateWithPrefix && !moreThanOneCandidateFromAnonSource {
					fmtCandidate(col.table)
				}
			} else {
				if moreThanOneHiddenCandidate && !moreThanOneCandidateWithPrefix && !moreThanOneCandidateFromAnonSource {
					fmtCandidate(col.table)
				}
			}
		}
	}

	return pgerror.Newf(pgcode.AmbiguousColumn,
		"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String(),
	)
}

// newAmbiguousSourceError returns an error with a helpful error message to be
// used in case of an ambiguous table name.
func newAmbiguousSourceError(tn *tree.TableName) error {
	if tn.Catalog() == "" {
		return pgerror.Newf(pgcode.AmbiguousAlias,
			"ambiguous source name: %q", tree.ErrString(tn))

	}
	return pgerror.Newf(pgcode.AmbiguousAlias,
		"ambiguous source name: %q (within database %q)",
		tree.ErrString(&tn.TableName), tree.ErrString(&tn.CatalogName))
}

func (s *scope) String() string {
	var buf bytes.Buffer

	if s.parent != nil {
		buf.WriteString(s.parent.String())
		buf.WriteString("->")
	}

	buf.WriteByte('(')
	for i, c := range s.cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%s:%d", c.name.String(), c.id)
	}
	for i, c := range s.extraCols {
		if i > 0 || len(s.cols) > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%s:%d!extra", c.name.String(), c.id)
	}
	buf.WriteByte(')')

	return buf.String()
}

// AddScopeProperty add the flag that ScopeFirstAgg and ScopeLastAgg to restrict first and last in the future.
// name is the name of agg function.
// ex: select last(a),first(b) from t; when t is not ts table, should report error
// this function is used when checking agg function.
func (s *scope) AddScopeProperty(name string) {
	// the flag ScopeFirstAgg and ScopeLastAgg used to limit first,last
	// first and last agg function can only be used in ts scenarios.
	switch name {
	case sqlbase.FirstAgg, sqlbase.FirstRowAgg, sqlbase.FirstTSAgg, sqlbase.FirstRowTSAgg:
		s.ScopeTSProp = opt.AddTSProperty(s.ScopeTSProp, ScopeFirstAgg)
	case sqlbase.LastAgg, sqlbase.LastRowAgg, sqlbase.LastTSAgg, sqlbase.LastRowTSAgg:
		s.ScopeTSProp = opt.AddTSProperty(s.ScopeTSProp, ScopeLastAgg)
	}
	s.ScopeTSProp = opt.AddTSProperty(s.ScopeTSProp, ScopeTSPropIsAggExpr)
}

// DelScopeProperty delete flag from time series property
// name is the name of agg function.
func (s *scope) DelScopeProperty(name string) {
	switch name {
	case sqlbase.FirstAgg, sqlbase.FirstRowAgg, sqlbase.FirstTSAgg, sqlbase.FirstRowTSAgg:
		s.ScopeTSProp = opt.DelTsProperty(s.ScopeTSProp, ScopeFirstAgg)
	case sqlbase.LastAgg, sqlbase.LastRowAgg, sqlbase.LastTSAgg, sqlbase.LastRowTSAgg:
		s.ScopeTSProp = opt.DelTsProperty(s.ScopeTSProp, ScopeLastAgg)
	}
	s.ScopeTSProp = opt.DelTsProperty(s.ScopeTSProp, ScopeTSPropIsAggExpr)
}

// limitLastFirst limit first() and last() can only be used in TIMESERIES scenarios.
// id is the column id.
// Here we restrict last/first related agg to use
// only in sequential scenarios.
// Last queries in relational tables,
// last queries in result sets,
// last queries in multi-table associations are disabled.
// Only last queries in the source ts table are allowed.
// Disable the use of last and first for these scenarios
// by checking the TSProp field of scope
func (s *scope) limitLastFirst(id opt.ColumnID) {
	if (s.ScopeTSProp&ScopeLastAgg > 0 || s.ScopeTSProp&ScopeFirstAgg > 0) &&
		s.builder.factory.Metadata().ColumnMeta(id).IsNormalCol() {
		s.ScopeTSProp = opt.AddTSProperty(s.ScopeTSProp, ScopeLastError)
	}
}

// checkAggExtend is used for the semantic parsing stage of the agg function.
// records count of min and max and all aggs, checks if the param of min or max
// is tag, sets the existOtherAgg.
func (s *scope) checkAggExtend(funName string, e opt.ScalarExpr) {
	if !s.checkAggExtendFlag(isSingleTsTable) {
		return
	}
	switch funName {
	case sqlbase.MinAgg:
		s.AggExHelper.minCount++
		s.AggExHelper.aggCount++
		if !s.checkCol(e, true) {
			s.setAggExtendFlag(existOtherAgg)
		}
	case sqlbase.MaxAgg:
		s.AggExHelper.maxCount++
		s.AggExHelper.aggCount++
		if !s.checkCol(e, true) {
			s.setAggExtendFlag(existOtherAgg)
		}
	case sqlbase.LastAgg:
		s.AggExHelper.aggCount++
		if !s.checkCol(e, false) {
			s.setAggExtendFlag(existOtherAgg)
		} else {
			s.setAggExtendFlag(existLastTS)
		}
	case sqlbase.FirstAgg:
		s.AggExHelper.aggCount++
		if !s.checkCol(e, false) {
			s.setAggExtendFlag(existOtherAgg)
		} else {
			s.setAggExtendFlag(existFirstTS)
		}
	default:
		s.AggExHelper.aggCount++
		s.setAggExtendFlag(existOtherAgg)
	}
}

// checkCol return true can apply agg extend.
func (s *scope) checkCol(e opt.ScalarExpr, minOrMax bool) bool {
	switch t := e.(type) {
	case *memo.VariableExpr:
		colMeta := s.builder.factory.Metadata().ColumnMeta(t.Col)
		tableID := colMeta.Table
		if tableID == 0 {
			return false
		}

		// tag col can not apply agg extend.
		if minOrMax {
			s.AggExHelper.minOrMaxColID = t.Col
			return !colMeta.IsTag()
		}

		// first agg or last agg only use timestamp col.
		if tableID.ColumnID(0) == t.Col {
			return true
		}
		return false
	case *memo.FunctionExpr:
		for i := 0; i < len(t.Args); i++ {
			if !s.checkCol(t.Args[i], minOrMax) {
				return false
			}
		}
		if minOrMax {
			s.AggExHelper.minOrMaxColID = opt.ColumnID(s.builder.factory.Metadata().NumColumns())
		}
		return true

	default:
		for i := 0; i < e.ChildCount(); i++ {
			if !s.checkCol(e.Child(i).(opt.ScalarExpr), minOrMax) {
				return false
			}
		}
		if minOrMax {
			s.AggExHelper.minOrMaxColID = opt.ColumnID(s.builder.factory.Metadata().NumColumns())
		}
		return true
	}
}

// CanApplyAggExtend checks if can applye agg extend.
// 1.must be single ts table
// 2.can not exist other agg functions except min,max,last,first
// 3.must exist non agg col.
// 4.group by cols can not contain non agg cols.
// 5.can only exist one min or one max.
func (s *scope) CanApplyAggExtend(groupingColSet *opt.ColSet) bool {
	helper := s.AggExHelper
	if groupingColSet != nil {
		if s.AggExHelper.extendColSet.Difference(*groupingColSet).Empty() {
			return false
		}
	}
	if s.checkAggExtendFlag(isSingleTsTable) && !s.checkAggExtendFlag(existOtherAgg) && s.checkAggExtendFlag(existNonAggCol) {
		if helper.minCount == 1 && helper.maxCount == 1 {
			return false
		}
		if helper.minCount > 1 || helper.maxCount > 1 {
			return false
		}
		if helper.minCount == 1 || helper.maxCount == 1 {
			return true
		}
	}
	return false
}
