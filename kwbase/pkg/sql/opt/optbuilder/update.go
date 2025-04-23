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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// buildUpdate builds a memo group for an UpdateOp expression. First, an input
// expression is constructed that outputs the existing values for all rows from
// the target table that match the WHERE clause. Additional column(s) that
// provide updated values are projected for each of the SET expressions, as well
// as for any computed columns. For example:
//
//	CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//	UPDATE abc SET b=1 WHERE a=2
//
// This would create an input expression similar to this SQL:
//
//	SELECT a AS oa, b AS ob, c AS oc, 1 AS nb FROM abc WHERE a=2
//
// The execution engine evaluates this relational expression and uses the
// resulting values to form the KV keys and values.
//
// Tuple SET expressions are decomposed into individual columns:
//
//	UPDATE abc SET (b, c)=(1, 2) WHERE a=3
//	=>
//	SELECT a AS oa, b AS ob, c AS oc, 1 AS nb, 2 AS nc FROM abc WHERE a=3
//
// Subqueries become correlated left outer joins:
//
//	UPDATE abc SET b=(SELECT y FROM xyz WHERE x=a)
//	=>
//	SELECT a AS oa, b AS ob, c AS oc, y AS nb
//	FROM abc
//	LEFT JOIN LATERAL (SELECT y FROM xyz WHERE x=a)
//	ON True
//
// Computed columns result in an additional wrapper projection that can depend
// on input columns.
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Update operator).
func (b *Builder) buildUpdate(upd *tree.Update, inScope *scope) (outScope *scope) {
	if upd.OrderBy != nil && upd.Limit == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"UPDATE statement requires LIMIT when ORDER BY is used"))
	}

	// UX friendliness safeguard.
	if upd.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.DangerousStatementf("UPDATE without WHERE clause"))
	}

	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(upd.Table, privilege.UPDATE)
	if tab.GetTableType() == tree.TemplateTable || tab.GetTableType() == tree.InstanceTable {
		panic(sqlbase.TemplateAndInstanceUnsupportedError("update"))
	}

	if tab.GetTableType() == tree.TimeseriesTable {
		_, ok := upd.Returning.(*tree.NoReturningClause)
		if upd.OrderBy != nil || len(upd.From) != 0 || upd.Limit != nil || !ok || upd.With != nil || upd.Where == nil {
			panic(sqlbase.UnsupportedUpdateConditionError("only supported where expression"))
		}
		// Check Select permission as well, since existing values must be read.
		b.checkPrivilege(depName, tab, privilege.SELECT)
		return b.buildTSUpdate(inScope, tab, upd, alias)
	}

	if refColumns != nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"cannot specify a list of column IDs with UPDATE"))
	}

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(depName, tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "update", tab, alias)

	// Build the input expression that selects the rows that will be updated:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the update table will be projected.
	mb.buildInputForUpdate(inScope, upd.Table, upd.From, upd.Where, upd.Limit, upd.OrderBy)

	// Derive the columns that will be updated from the SET expressions.
	mb.addTargetColsForUpdate(upd.Exprs)

	// Build each of the SET expressions.
	mb.addUpdateCols(upd.Exprs)

	// Build the final update statement, including any returned expressions.
	if resultsNeeded(upd.Returning) {
		mb.buildUpdate(*upd.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildUpdate(nil /* returning */)
	}

	// Verify whether the time-series table and relational table are mixed in the time-series query
	mb.CheckMixedTableRefWithTs()

	return mb.outScope
}

// addTargetColsForUpdate compiles the given SET expressions and adds the user-
// specified column names to the list of table columns that will be updated by
// the Update operation. Verify that the RHS of the SET expression provides
// exactly as many columns as are expected by the named SET columns.
func (mb *mutationBuilder) addTargetColsForUpdate(exprs tree.UpdateExprs) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetColsForUpdate cannot be called more than once"))
	}

	for _, expr := range exprs {
		mb.addTargetColsByName(expr.Names)

		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *tree.Subquery:
				// Build the subquery in order to determine how many columns it
				// projects, and store it for later use in the addUpdateCols method.
				// Use the data types of the target columns to resolve expressions
				// with ambiguous types (e.g. should 1 be interpreted as an INT or
				// as a FLOAT).
				desiredTypes := make([]*types.T, len(expr.Names))
				targetIdx := len(mb.targetColList) - len(expr.Names)
				for i := range desiredTypes {
					desiredTypes[i] = mb.md.ColumnMeta(mb.targetColList[targetIdx+i]).Type
				}
				outScope := mb.b.buildSelectStmt(t.Select, noRowLocking, desiredTypes, mb.outScope)
				mb.subqueries = append(mb.subqueries, outScope)
				n = len(outScope.cols)

			case *tree.Tuple:
				n = len(t.Exprs)
			}
			if n < 0 {
				panic(unimplementedWithIssueDetailf(35713, fmt.Sprintf("%T", expr.Expr),
					"source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression; not supported: %T", expr.Expr))
			}
			if len(expr.Names) != n {
				panic(pgerror.Newf(pgcode.Syntax,
					"number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n))
			}
		}
	}
}

// addUpdateCols builds nested Project and LeftOuterJoin expressions that
// correspond to the given SET expressions:
//
//	SET a=1 (single-column SET)
//	  Add as synthesized Project column:
//	    SELECT <fetch-cols>, 1 FROM <input>
//
//	SET (a, b)=(1, 2) (tuple SET)
//	  Add as multiple Project columns:
//	    SELECT <fetch-cols>, 1, 2 FROM <input>
//
//	SET (a, b)=(SELECT 1, 2) (subquery)
//	  Wrap input in Max1Row + LeftJoinApply expressions:
//	    SELECT * FROM <fetch-cols> LEFT JOIN LATERAL (SELECT 1, 2) ON True
//
// Multiple subqueries result in multiple left joins successively wrapping the
// input. A final Project operator is built if any single-column or tuple SET
// expressions are present.
func (mb *mutationBuilder) addUpdateCols(exprs tree.UpdateExprs) {
	// SET expressions should reject aggregates, generators, etc.
	scalarProps := &mb.b.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	mb.b.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	// UPDATE input columns are accessible to SET expressions.
	inScope := mb.outScope

	// Project additional column(s) for each update expression (can be multiple
	// columns in case of tuple assignment).
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	checkCol := func(sourceCol *scopeColumn, scopeOrd scopeOrdinal, targetColID opt.ColumnID) {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ, tree.RelationalTable)

		// Add ordinal of new scope column to the list of columns to update.
		mb.updateOrds[ord] = scopeOrd

		// Rename the column to match the target column being updated.
		sourceCol.name = mb.tab.Column(ord).ColName()
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID) {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		// Add new column to the projections scope.
		desiredType := mb.md.ColumnMeta(targetColID).Type
		texpr := inScope.resolveType(expr, desiredType)
		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr, false)
		scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)
		if scalar := mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil); scalar != nil {
			// group window function can be only used in groupby
			if name, ok := memo.CheckGroupWindowExist(scalar); ok {
				panic(pgerror.Newf(pgcode.Syntax, "%s(): group window function can be only used in single time series table query.", name))
			}
		}
		checkCol(scopeCol, scopeColOrd, targetColID)
	}

	n := 0
	subquery := 0
	for _, set := range exprs {
		if set.Tuple {
			switch t := set.Expr.(type) {
			case *tree.Subquery:
				// Get the subquery scope that was built by addTargetColsForUpdate.
				subqueryScope := mb.subqueries[subquery]
				subquery++

				// Type check and rename columns.
				for i := range subqueryScope.cols {
					scopeColOrd := scopeOrdinal(len(projectionsScope.cols) + i)
					checkCol(&subqueryScope.cols[i], scopeColOrd, mb.targetColList[n])
					n++
				}

				// Lazily create new scope to hold results of join.
				if mb.outScope == inScope {
					mb.outScope = inScope.replace()
					mb.outScope.appendColumnsFromScope(inScope)
					mb.outScope.expr = inScope.expr
				}

				// Wrap input with Max1Row + LOJ.
				mb.outScope.appendColumnsFromScope(subqueryScope)
				mb.outScope.expr = mb.b.factory.ConstructLeftJoinApply(
					mb.outScope.expr,
					mb.b.factory.ConstructMax1Row(subqueryScope.expr, multiRowSubqueryErrText),
					memo.TrueFilter,
					memo.EmptyJoinPrivate,
				)

				// Project all subquery output columns.
				projectionsScope.appendColumnsFromScope(subqueryScope)

			case *tree.Tuple:
				for _, expr := range t.Exprs {
					addCol(expr, mb.targetColList[n])
					n++
				}
			}
		} else {
			addCol(set.Expr, mb.targetColList[n])
			n++
		}
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope

	// Add additional columns for computed expressions that may depend on the
	// updated columns.
	mb.addSynthesizedColsForUpdate()
}

// addComputedColsForUpdate wraps an Update input expression with a Project
// operator containing any computed columns that need to be updated. This
// includes write-only mutation columns that are computed.
func (mb *mutationBuilder) addSynthesizedColsForUpdate() {
	// Allow mutation columns to be referenced by other computed mutation
	// columns (otherwise the scope will raise an error if a mutation column
	// is referenced). These do not need to be set back to true again because
	// mutation columns are not projected by the Update operator.
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].mutation = false
	}

	// Add non-computed columns that are being dropped or added (mutated) to the
	// table. These are not visible to queries, and will always be updated to
	// their default values. This is necessary because they may not yet have been
	// set by the backfiller.
	mb.addSynthesizedCols(
		mb.updateOrds,
		func(colOrd int) bool {
			return !mb.tab.Column(colOrd).IsComputed() && cat.IsMutationColumn(mb.tab, colOrd)
		},
	)

	// Possibly round DECIMAL-related columns containing update values. Do
	// this before evaluating computed expressions, since those may depend on
	// the inserted columns.
	mb.roundDecimalValues(mb.updateOrds, false /* roundComputedCols */)

	// Disambiguate names so that references in the computed expression refer to
	// the correct columns.
	mb.disambiguateColumns()

	// Add all computed columns in case their values have changed.
	mb.addSynthesizedCols(
		mb.updateOrds,
		func(colOrd int) bool { return mb.tab.Column(colOrd).IsComputed() },
	)

	// Possibly round DECIMAL-related computed columns.
	mb.roundDecimalValues(mb.updateOrds, true /* roundComputedCols */)
}

// buildUpdate constructs an Update operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpdate(returning tree.ReturningExprs) {
	mb.addCheckConstraintCols()

	mb.buildFKChecksForUpdate()

	private := mb.makeMutationPrivate(returning != nil)
	for _, col := range mb.extraAccessibleCols {
		if col.id != 0 {
			private.PassthroughCols = append(private.PassthroughCols, col.id)
		}
	}
	mb.outScope.expr = mb.b.factory.ConstructUpdate(mb.outScope.expr, mb.checks, private)
	mb.buildReturning(returning)
}

// checkUpdateFilter check if unsupported exprs such as cast expr exist in update stmt
func checkUpdateFilter(expr tree.Expr) error {
	switch exp := expr.(type) {
	case *tree.AndExpr:
		if err := checkUpdateFilter(exp.Left); err != nil {
			return err
		}
		if err := checkUpdateFilter(exp.Right); err != nil {
			return err
		}
	case *tree.ComparisonExpr:
		if err := checkUpdateFilter(exp.Left); err != nil {
			return err
		}
		if err := checkUpdateFilter(exp.Right); err != nil {
			return err
		}
	case *tree.ParenExpr:
		if err := checkUpdateFilter(exp.Expr); err != nil {
			return err
		}
	case *tree.UnresolvedName, *tree.NumVal, *tree.StrVal, *tree.DBool, *tree.Placeholder:
	default:
		return sqlbase.UnsupportedUpdateConditionError("unsupported filter expression")
	}
	return nil
}

// buildTSUpdate build update outScope of time-series table
// input: inScope, table, update, TableName
// output: outScope
// build update outScope of time-series table through table and update message
func (b *Builder) buildTSUpdate(
	inScope *scope, table cat.Table, upd *tree.Update, alias tree.TableName,
) (outScope *scope) {
	// prepare update need to rebuild memo
	b.DisableMemoReuse = true
	if err := checkUpdateFilter(upd.Where.Expr); err != nil {
		panic(err)
	}
	// get table metadata
	id := b.factory.Metadata().AddTable(table, &alias)
	colCount := table.ColumnCount()
	tagCols := make([]*sqlbase.ColumnDescriptor, 0)
	colMap := make(map[int]int, 0)
	primaryTagIDs := make(map[int]struct{})
	ordinaryTag := make(map[string]int, colCount)
	ptagType := make(map[int]types.T, colCount)
	var tagName []string

	for i := 0; i < colCount; i++ {
		col := table.Column(i)
		if col.IsTagCol() {
			tagCols = append(tagCols, col.(*sqlbase.ColumnDescriptor))
			if col.IsPrimaryTagCol() {
				// build primary tags' map，and check if scope column in filter meets the conditions
				primaryTagIDs[int(col.ColID())] = struct{}{}
				ptagType[int(col.ColID())] = *col.DatumType()
			} else {
				tagName = append(tagName, string(col.ColName()))
				ordinaryTag[string(col.ColName())] = int(col.(*sqlbase.ColumnDescriptor).ID)
			}
		}
	}

	exprs := make(map[int]tree.Datum, colCount)

	// resolve primary tag filter to datum
	var indexFlags *tree.IndexFlags
	if source, ok := upd.Table.(*tree.AliasedTableExpr); ok && source.IndexFlags != nil {
		indexFlags = source.IndexFlags
	}
	inScope = b.buildScan(
		b.addTable(table, &alias),
		nil, /* ordinals */
		indexFlags,
		noRowLocking,
		includeMutations,
		inScope,
	)

	var isPtagOutOfRange bool

	// resolve filter expr to get scope column and it's column id
	texpr := inScope.resolveType(upd.Where.Expr, types.Bool)
	meta := b.factory.Metadata()
	if !checkTSUpdateFilter(texpr, exprs, primaryTagIDs, meta) {
		panic(sqlbase.UnsupportedUpdateConditionError("unsupported binary operator or mismatching tag filter"))
	}
	if !checkUpdateTagIsComplete(exprs, primaryTagIDs) {
		panic(sqlbase.UnsupportedUpdateConditionError("unsupported binary operator or mismatching tag filter"))
	}
	for pid, value := range exprs {
		typ, ok := ptagType[pid]
		if ok && checkIfIntOutOfRange(typ, value) {
			isPtagOutOfRange = true
			break
		}
	}

	var mb mutationBuilder
	mb.init(b, "update", table, alias)
	mb.buildInputForUpdate(inScope, upd.Table, upd.From, upd.Where, upd.Limit, upd.OrderBy)
	// Derive the columns that will be updated from the SET expressions.
	mb.addTSTargetColsForUpdate(upd.Exprs)
	// Build each of the SET expressions.
	if err := mb.addTSUpdateCols(upd.Exprs, exprs); err != nil {
		panic(err)
	}
	outScope = inScope.push()

	// if statement is to execute a prepared statement, resolve and change placeholder
	if b.factory.CheckFlag(opt.IsPrepare) && b.evalCtx.Placeholders != nil {
		if err := changePlaceholder(b.evalCtx, &upd.Where.Expr); err != nil {
			panic(err)
		}
	}
	// build tag only query statement to get tag values
	fmtctx := tree.NewFmtCtx(tree.FmtSimple)
	upd.Where.Format(fmtctx)
	tagQueryFilter := fmtctx.CloseAndGetString()
	query := sqlbase.BuildTagHintQuery(tagName, string(alias.CatalogName), string(table.Name()))
	query += " " + tagQueryFilter
	// todo: add limit 1 to avoid finding multiple rows of values for the same PTAG in distributed scenarios
	query = "select distinct * from (" + query + ") limit 1"
	row, err := b.evalCtx.InternalExecutor.QueryRow(b.ctx, "queryTag", b.evalCtx.Txn, query)
	if err != nil && !b.factory.CheckFlag(opt.IsPrepare) {
		panic(err)
	}

	// if update tag value is out of range, return update 0
	if row == nil || isPtagOutOfRange {
		outScope.expr = b.factory.ConstructTSUpdate(
			&memo.TSUpdatePrivate{
				PTagValueNotExist: true,
			})
		return outScope
	}

	for i, datum := range row {
		if _, ok := exprs[ordinaryTag[tagName[i]]]; !ok {
			exprs[ordinaryTag[tagName[i]]] = datum
		}
	}

	var tagValue []tree.Datum
	for i, col := range tagCols {
		// check if tags completed
		if exp, ok := exprs[int(col.ColID())]; ok {
			tagValue = append(tagValue, exp)
			colMap[int(col.ID)] = i
		}
	}

	var hasTypeHints bool
	for i := range b.semaCtx.Placeholders.TypeHints {
		if b.semaCtx.Placeholders.TypeHints[i] != nil {
			hasTypeHints = true
			b.semaCtx.Placeholders.TypeHints[i] = nil
		}
	}

	for colID, expr := range exprs {
		if v, ok := expr.(*tree.Placeholder); ok {
			if hasTypeHints {
				b.semaCtx.Placeholders.TypeHints[v.Idx] = meta.ColumnMeta(opt.ColumnID(colID)).Type
			}
			b.semaCtx.Placeholders.Types[v.Idx] = meta.ColumnMeta(opt.ColumnID(colID)).Type
		}
	}

	outScope.expr = b.factory.ConstructTSUpdate(
		&memo.TSUpdatePrivate{
			UpdateRows:        tagValue,
			ColsMap:           colMap,
			ID:                id,
			PTagValueNotExist: false,
		})
	return outScope
}

// filters in TS update statement supports only one of primary tags
func checkTSUpdateFilter(
	filter tree.Expr, exprs map[int]tree.Datum, primaryTagIDs map[int]struct{}, meta *opt.Metadata,
) bool {

	switch f := filter.(type) {
	case *tree.AndExpr:
		if !(checkTSUpdateFilter(f.Right, exprs, primaryTagIDs, meta) && checkTSUpdateFilter(f.Left, exprs, primaryTagIDs, meta)) {
			return false
		}
	case *tree.ComparisonExpr:
		// resolve primary tag expr which only support =
		if f.Operator != tree.EQ {
			return false
		}
		if !addPrimaryTagExpr(f.Left, f.Right, exprs, meta) {
			return false
		}
	default:
		return false
	}
	return true
}

// addPrimaryTagExpr resolve primary tag expr and build exprs map of id and datum
// input: left expr, right expr, metadata
// output: bool(if the same primary tag match different value, return false)
func addPrimaryTagExpr(left, right tree.Expr, exprs map[int]tree.Datum, meta *opt.Metadata) bool {
	switch ltype := left.(type) {
	case *scopeColumn:
		// id in expr is column's logical id, need to get column's physical id
		t := meta.ColumnMeta(ltype.id).Table
		index := t.ColumnOrdinal(ltype.id)
		id := int(meta.Table(t).Column(index).ColID())
		if d, ok := right.(tree.Datum); ok {
			if val, ok := exprs[id]; ok {
				if val.String() != right.String() {
					return false
				}
			} else {
				exprs[id] = d
			}
		} else {
			return false
		}
	default:
		switch rtype := right.(type) {
		case *scopeColumn:
			t := meta.ColumnMeta(rtype.id).Table
			id := t.ColumnOrdinal(rtype.id) + 1
			if d, ok := left.(tree.Datum); ok {
				if val, ok := exprs[id]; ok {
					if val.String() != ltype.String() {
						return false
					}
				} else {
					exprs[id] = d
				}
			} else {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// checkUpdateTagIsComplete check if primary tags in filter is completed
func checkUpdateTagIsComplete(exprs map[int]tree.Datum, primaryTagIDs map[int]struct{}) bool {
	if len(exprs) != len(primaryTagIDs) {
		return false
	}
	for key := range primaryTagIDs {
		if _, ok := exprs[key]; !ok {
			return false
		}
	}
	return true
}

// addTSTargetColsForUpdate resolves target columns by name and add to mb
func (mb *mutationBuilder) addTSTargetColsForUpdate(exprs tree.UpdateExprs) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetColsForUpdate cannot be called more than once"))
	}

	for _, expr := range exprs {
		mb.addTargetColsByName(expr.Names)
	}
}

// addTSUpdateCols checks if target columns is tags except primary tags, add tag id and values into exprMap
func (mb *mutationBuilder) addTSUpdateCols(
	exprs tree.UpdateExprs, exprMap map[int]tree.Datum,
) error {
	// SET expressions should reject aggregates, generators, etc.
	scalarProps := &mb.b.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	mb.b.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	// UPDATE input columns are accessible to SET expressions.
	inScope := mb.outScope

	// Project additional column(s) for each update expression (can be multiple
	// columns in case of tuple assignment).
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	// check if values fit column type(including type check and nullable)
	checkCol := func(sourceCol *scopeColumn, scopeOrd scopeOrdinal, targetColID opt.ColumnID, d tree.Datum, expr tree.Expr) error {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		col := mb.tab.Column(ord)
		checkDatumTypeFitsColumnType(col, sourceCol.typ, tree.RelationalTable)
		if !col.IsNullable() && d == tree.DNull {
			return sqlbase.UnsupportedUpdateConditionError(fmt.Sprintf("tag %s is not null", mb.tab.Column(ord).ColName()))
		}
		if !(col.IsOrdinaryTagCol()) {
			return sqlbase.UnsupportedUpdateConditionError("primary tags and columns cannot be set")
		}
		typ := *col.DatumType()
		if checkIfIntOutOfRange(typ, d) {
			return sqlbase.IntOutOfRangeError(col.DatumType(), string(col.ColName()))
		}
		if s, ok := expr.(*tree.StrVal); ok {
			_, err := s.TSTypeCheck(string(col.ColName()), &typ, mb.b.semaCtx)
			if err != nil {
				return err
			}
		}
		return nil
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID, exprMap map[int]tree.Datum) error {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}
		if udv, ok := expr.(*tree.UserDefinedVar); ok {
			d, err := udv.Eval(mb.b.evalCtx)
			if err != nil {
				return err
			}
			expr = d
		}

		// Add new column to the projections scope.
		desiredType := mb.md.ColumnMeta(targetColID).Type
		texpr := inScope.resolveType(expr, desiredType)
		d, ok := texpr.(tree.Datum)
		if !ok {
			if exp, ok := texpr.(*tree.CastExpr); !(ok && exp.Expr == tree.DNull) {
				return sqlbase.UnsupportedUpdateConditionError("unsupported expr in set")
			}
			d = tree.DNull
		}
		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr, false)
		scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)
		mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil)
		if err := checkCol(scopeCol, scopeColOrd, targetColID, d, expr); err != nil {
			return err
		}
		colMeta := mb.tab.Column(mb.tabID.ColumnOrdinal(targetColID))
		exprMap[int(colMeta.ColID())] = d
		return nil
	}

	n := 0
	for _, set := range exprs {
		if !set.Tuple {
			if err := addCol(set.Expr, mb.targetColList[n], exprMap); err != nil {
				return err
			}
			n++
		} else {
			return sqlbase.UnsupportedUpdateConditionError("unsupported expr in set")
		}
	}
	return nil
}

// checkIfIntOutOfRange check if value oh type int out of range
func checkIfIntOutOfRange(typ types.T, d tree.Datum) bool {
	if v, ok := d.(*tree.DInt); ok {
		width := uint(typ.Width() - 1)

		// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
		shifted := *v >> width
		if (*v >= 0 && shifted > 0) || (*v < 0 && shifted < -1) {
			return true
		}
	}
	return false
}

// changePlaceholder change placeholder with value in EvalContext.
func changePlaceholder(evalCtx *tree.EvalContext, filter *tree.Expr) error {
	switch f := (*filter).(type) {
	case *tree.AndExpr:
		if err := changePlaceholder(evalCtx, &f.Left); err != nil {
			return err
		}
		if err := changePlaceholder(evalCtx, &f.Right); err != nil {
			return err
		}
	case *tree.ComparisonExpr:
		if err := changePlaceholder(evalCtx, &f.Left); err != nil {
			return err
		}
		if err := changePlaceholder(evalCtx, &f.Right); err != nil {
			return err
		}
	case *tree.Placeholder:
		resValue, err := f.Eval(evalCtx)
		if err != nil {
			return err
		}
		val, err := resValue.DatumToExpr()
		if err != nil {
			return err
		}
		*filter = val
	default:
	}
	return nil
}
