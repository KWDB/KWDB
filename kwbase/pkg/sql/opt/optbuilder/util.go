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
	"strings"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const (
	initByteAWidth   = 1
	initInt8Width    = 8
	initInt16Width   = 16
	initInt32Width   = 32
	initInt64Width   = 64
	initVarcharWidth = 254
)

// windowAggregateFrame() returns a frame that any aggregate built as a window
// can use.
func windowAggregateFrame() memo.WindowFrame {
	return memo.WindowFrame{
		StartBoundType: unboundedStartBound.BoundType,
		EndBoundType:   unboundedEndBound.BoundType,
	}
}

// getTypedExprs casts the exprs into TypedExps and returns them.
func getTypedExprs(exprs []tree.Expr) []tree.TypedExpr {
	argExprs := make([]tree.TypedExpr, len(exprs))
	for i, expr := range exprs {
		argExprs[i] = expr.(tree.TypedExpr)
	}
	return argExprs
}

// expandLastStar expands the star and places the data column in front of the tag column,
// which is specifically for last(*) expansion
func (b *Builder) expandLastStar(
	cols []scopeColumn, checkTblName bool, tableName string,
) (exprs []tree.TypedExpr, aliases []string) {
	tsCols := make([]tree.TypedExpr, 0)
	tsAliases := make([]string, 0)
	tagCols := make([]tree.TypedExpr, 0)
	tagAliases := make([]string, 0)
	for i := range cols {
		col := &cols[i]
		if checkTblName {
			if string(col.table.TableName) != tableName {
				continue
			}
		}
		if !col.hidden {
			if b.factory.Metadata().ColumnMeta(col.id).IsTag() {
				tagCols = append(tagCols, col)
				tagAliases = append(tagAliases, string(col.name))
			} else {
				tsCols = append(tsCols, col)
				tsAliases = append(tsAliases, string(col.name))
			}
		}
	}
	exprs = append(exprs, tsCols...)
	aliases = append(aliases, tsAliases...)
	exprs = append(exprs, tagCols...)
	aliases = append(aliases, tagAliases...)
	return exprs, aliases
}

// expandStar expands expr into a list of columns if expr
// corresponds to a "*", "<table>.*" or "(Expr).*".
func (b *Builder) expandStar(
	expr tree.Expr, inScope *scope, isLast bool,
) (aliases []string, exprs []tree.TypedExpr) {

	switch t := expr.(type) {
	case *tree.TupleStar:
		texpr := inScope.resolveType(t.Expr, types.Any)
		typ := texpr.ResolvedType()
		if typ.Family() != types.TupleFamily {
			panic(tree.NewTypeIsNotCompositeError(typ))
		}

		// If the sub-expression is a tuple constructor, we'll de-tuplify below.
		// Otherwise we'll re-evaluate the expression multiple times.
		//
		// The following query generates a tuple constructor:
		//     SELECT (kv.*).* FROM kv
		//     -- the inner star expansion (scope.VisitPre) first expands to
		//     SELECT (((kv.k, kv.v) as k,v)).* FROM kv
		//     -- then the inner tuple constructor detuplifies here to:
		//     SELECT kv.k, kv.v FROM kv
		//
		// The following query generates a scalar var with tuple type that
		// is not a tuple constructor:
		//
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).*
		//     -- does not detuplify, one gets instead:
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).word,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catcode,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catdesc
		//     -- (and we hope a later opt will merge the subqueries)
		tTuple, isTuple := texpr.(*tree.Tuple)

		aliases = typ.TupleLabels()
		exprs = make([]tree.TypedExpr, len(typ.TupleContents()))
		for i := range typ.TupleContents() {
			if isTuple {
				// De-tuplify: ((a,b,c)).* -> a, b, c
				exprs[i] = tTuple.Exprs[i].(tree.TypedExpr)
			} else {
				// Can't de-tuplify:
				// either (Expr).* -> (Expr).a, (Expr).b, (Expr).c if there are enough
				// labels, or (Expr).* -> (Expr).@1, (Expr).@2, (Expr).@3 if labels are
				// missing.
				//
				// We keep the labels if available so that the column name
				// generation still produces column label "x" for, e.g. (E).x.
				colName := ""
				if i < len(aliases) {
					colName = aliases[i]
				}
				// NewTypedColumnAccessExpr expects colName to be empty if the tuple
				// should be accessed by index.
				exprs[i] = tree.NewTypedColumnAccessExpr(texpr, colName, i)
			}
		}
		for i := len(aliases); i < len(typ.TupleContents()); i++ {
			// Add aliases for all the non-named columns in the tuple.
			aliases = append(aliases, "?column?")
		}

	case *tree.AllColumnsSelector:
		src, srcMeta, err := t.Resolve(b.ctx, inScope)
		if err != nil {
			panic(err)
		}
		refScope := srcMeta.(*scope)
		exprs = make([]tree.TypedExpr, 0, len(refScope.cols))
		aliases = make([]string, 0, len(refScope.cols))
		if isLast {
			exprs, aliases = b.expandLastStar(refScope.cols, false, "")
		} else {
			for i := range refScope.cols {
				col := &refScope.cols[i]
				if col.table == *src && !col.hidden {
					exprs = append(exprs, col)
					aliases = append(aliases, string(col.name))
				}
			}
		}

	case tree.UnqualifiedStar:
		if len(inScope.cols) == 0 {
			panic(pgerror.Newf(pgcode.InvalidName,
				"cannot use %q without a FROM clause", tree.ErrString(expr)))
		}
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		aliases = make([]string, 0, len(inScope.cols))
		if isLast {
			exprs, aliases = b.expandLastStar(inScope.cols, false, "")
		} else {
			for i := range inScope.cols {
				col := &inScope.cols[i]
				if !col.hidden {
					exprs = append(exprs, col)
					aliases = append(aliases, string(col.name))
				}
			}
		}

	default:
		panic(errors.AssertionFailedf("unhandled type: %T", expr))
	}

	return aliases, exprs
}

// expandTableStar expands expr into a list of columns if expr corresponds to a "<table>.*"
func (b *Builder) expandTableStar(
	tableName string, inScope *scope,
) (aliases []string, exprs []tree.TypedExpr) {
	exprs, aliases = b.expandLastStar(inScope.cols, true, tableName)
	return aliases, exprs
}

// expandStarAndResolveType expands expr into a list of columns if
// expr corresponds to a "*", "<table>.*" or "(Expr).*". Otherwise,
// expandStarAndResolveType resolves the type of expr and returns it
// as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, tree.UnqualifiedStar, *tree.TupleStar:
		_, exprs = b.expandStar(expr, inScope, false)

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		exprs := b.expandStarAndResolveType(vn, inScope)
		return exprs

	default:
		texpr := inScope.resolveType(t, types.Any)
		exprs = []tree.TypedExpr{texpr}
	}

	return exprs
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope  The scope is passed in so it can can be updated with the newly bound
//
//	variable.
//
// alias  This is an optional alias for the new column (e.g., if specified with
//
//	the AS keyword).
//
// typ    The type of the column.
// expr   The expression this column refers to (if any).
// scalar The scalar expression associated with this column (if any).
//
// The new column is returned as a scopeColumn object.
func (b *Builder) synthesizeColumn(
	scope *scope, alias string, typ *types.T, expr tree.TypedExpr, scalar opt.ScalarExpr,
) *scopeColumn {
	name := tree.Name(alias)
	colID := b.factory.Metadata().AddColumn(alias, typ)
	scope.cols = append(scope.cols, scopeColumn{
		name:   name,
		typ:    typ,
		id:     colID,
		expr:   expr,
		scalar: scalar,
	})
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) synthesizeDeclareColumn(
	scope *scope,
	alias string,
	typ *types.T,
	expr tree.TypedExpr,
	idx int,
	isPara bool,
	overWrite int,
) *scopeColumn {
	name := tree.Name(alias)
	colID := b.factory.Metadata().AddDeclareColumn(alias, typ, idx)
	if overWrite != -1 {
		scope.cols[overWrite] = scopeColumn{
			name:       name,
			typ:        typ,
			id:         colID,
			expr:       expr,
			isDeclared: true,
			isPara:     isPara,
			realIdx:    idx,
		}
		return &scope.cols[overWrite]
	}
	scope.cols = append(scope.cols, scopeColumn{
		name:       name,
		typ:        typ,
		id:         colID,
		expr:       expr,
		isDeclared: true,
		isPara:     isPara,
		realIdx:    idx,
	})
	return &scope.cols[len(scope.cols)-1]
}

// populateSynthesizedColumn is similar to synthesizeColumn, but it fills in
// the given existing column rather than allocating a new one.
func (b *Builder) populateSynthesizedColumn(
	col *scopeColumn, scalar opt.ScalarExpr, scopeTsProp int,
) {
	var name string
	name = string(col.name)
	if b.PhysType == tree.TS && name == "" {
		name = col.expr.String()
	}
	if col.isDeclared {
		col.scalar = scalar
		return
	}

	colID := b.factory.Metadata().AddTSColumn(name, col.typ, opt.TSColNormal)
	col.id = colID
	col.scalar = scalar
}

// projectColumn projects src by copying its column ID to dst. projectColumn
// also copies src.name to dst if an alias is not already set in dst. No other
// fields are copied, for the following reasons:
//   - We don't copy group, as dst becomes a pass-through column in the new
//     scope. dst already has group=0, so keep it as-is.
//   - We don't copy hidden, because projecting a column makes it visible.
//     dst already has hidden=false, so keep it as-is.
//   - We don't copy table, since the table becomes anonymous in the new scope.
//   - We don't copy descending, since we don't want to overwrite dst.descending
//     if dst is an ORDER BY column.
//   - expr, exprStr and typ in dst already correspond to the expression and type
//     of the src column.
func (b *Builder) projectColumn(dst *scopeColumn, src *scopeColumn) {
	if dst.name == "" {
		dst.name = src.name
	}
	dst.id = src.id
}

// shouldCreateDefaultColumn decides if we need to create a default
// column and default label for a function expression.
// Returns true if the function's return type is not an empty tuple and
// doesn't declare any tuple labels.
func (b *Builder) shouldCreateDefaultColumn(texpr tree.TypedExpr) bool {
	if texpr.ResolvedType() == types.EmptyTuple {
		// This is only to support kwdb_internal.unary_table().
		return false
	}

	// We need to create a default column with a default name when
	// the function return type doesn't declare any return labels.
	return len(texpr.ResolvedType().TupleLabels()) == 0
}

// addColumn adds a column to scope with the given alias, type, and
// expression. It returns a pointer to the new column. The column ID and group
// are left empty so they can be filled in later.
// onlyDataCol: whether add data column only
func (b *Builder) addColumn(
	scope *scope, alias string, expr tree.TypedExpr, onlyDataColFlag bool,
) *scopeColumn {
	name := tree.Name(alias)
	if onlyDataColFlag {
		if e, ok := expr.(*scopeColumn); ok && !b.factory.Metadata().ColumnMeta(e.id).IsTag() {
			scope.cols = append(scope.cols, scopeColumn{
				name: name,
				typ:  expr.ResolvedType(),
				expr: expr,
			})
		} else {
			return nil
		}
	} else {
		isDeclare := false
		idx := 0
		var id opt.ColumnID
		if col, ok := expr.(*scopeColumn); ok {
			isDeclare = col.isDeclared
			id = col.id
			idx = col.realIdx
		}
		scope.cols = append(scope.cols, scopeColumn{
			name:       name,
			id:         id,
			typ:        expr.ResolvedType(),
			expr:       expr,
			isDeclared: isDeclare,
			realIdx:    idx,
		})
	}
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) synthesizeResultColumns(scope *scope, cols sqlbase.ResultColumns) {
	for i := range cols {
		c := b.synthesizeColumn(scope, cols[i].Name, cols[i].Typ, nil /* expr */, nil /* scalar */)
		if cols[i].Hidden {
			c.hidden = true
		}
	}
}

// colIndex takes an expression that refers to a column using an integer,
// verifies it refers to a valid target in the SELECT list, and returns the
// corresponding column index. For example:
//
//	SELECT a from T ORDER by 1
//
// Here "1" refers to the first item in the SELECT list, "a". The returned
// index is 0.
func colIndex(numOriginalCols int, expr tree.Expr, context string) int {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				panic(err)
			}
			ord = val
		} else {
			panic(pgerror.Newf(
				pgcode.Syntax,
				"non-integer constant in %s: %s", context, expr,
			))
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(pgerror.Newf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		))
	case tree.Datum:
		panic(pgerror.Newf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		))
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(pgerror.Newf(
				pgcode.InvalidColumnReference,
				"%s position %s is not in select list", context, expr,
			))
		}
		ord--
	}
	return int(ord)
}

// colIdxByProjectionAlias returns the corresponding index in columns of an expression
// that may refer to a column alias.
// If there are no aliases in columns that expr refers to, then -1 is returned.
// This method is pertinent to ORDER BY and DISTINCT ON clauses that may refer
// to a column alias.
func colIdxByProjectionAlias(expr tree.Expr, op string, scope *scope) int {
	index := -1

	if vBase, ok := expr.(tree.VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			panic(err)
		}

		if c, ok := v.(*tree.ColumnItem); ok && c.TableName == nil {
			// Look for an output column that matches the name. This
			// handles cases like:
			//
			//   SELECT a AS b FROM t ORDER BY b
			//   SELECT DISTINCT ON (b) a AS b FROM t
			target := c.ColumnName
			for j := range scope.cols {
				col := &scope.cols[j]
				if col.name != target {
					continue
				}

				if col.mutation {
					panic(makeBackfillError(col.name))
				}

				if index != -1 {
					// There is more than one projection alias that matches the clause.
					// Here, SQL92 is specific as to what should be done: if the
					// underlying expression is known and it is equivalent, then just
					// accept that and ignore the ambiguity. This plays nice with
					// `SELECT b, * FROM t ORDER BY b`. Otherwise, reject with an
					// ambiguity error.
					if scope.cols[j].getExprStr() != scope.cols[index].getExprStr() {
						panic(pgerror.Newf(pgcode.AmbiguousAlias,
							"%s \"%s\" is ambiguous", op, target))
					}
					// Use the index of the first matching column.
					continue
				}
				index = j
			}
		}
	}

	return index
}

// makeBackfillError returns an error indicating that the column of the given
// name is currently being backfilled and cannot be referenced.
func makeBackfillError(name tree.Name) error {
	return pgerror.Newf(pgcode.InvalidColumnReference,
		"column %q is being backfilled", tree.ErrString(&name))
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(exprs []tree.TypedExpr) []tree.TypedExpr {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				copy(newExprs, exprs[:i])
			}

			newExprs = flattenTuple(t, newExprs)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
		}
	}
	if newExprs != nil {
		return newExprs
	}
	return exprs
}

// flattenTuple recursively extracts the members of a tuple into a list of
// expressions.
func flattenTuple(t *tree.Tuple, exprs []tree.TypedExpr) []tree.TypedExpr {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			exprs = flattenTuple(eT, exprs)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
		}
	}
	return exprs
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguates columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr tree.Expr) string {
	return tree.AsStringWithFlags(expr, tree.FmtCheckEquivalence)
}

func colsToColList(cols []scopeColumn) opt.ColList {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].id
	}
	return colList
}

// resolveAndBuildScalar is used to build a scalar with a required type.
func (b *Builder) resolveAndBuildScalar(
	expr tree.Expr,
	requiredType *types.T,
	context exprType,
	flags tree.SemaRejectFlags,
	inScope *scope,
) opt.ScalarExpr {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(context.String(), flags)

	inScope.context = context
	texpr := inScope.resolveAndRequireType(expr, requiredType)

	return b.buildScalar(texpr, inScope, nil, nil, nil)
}

// In Postgres, qualifying an object name with pg_temp is equivalent to explicitly
// specifying TEMP/TEMPORARY in the CREATE syntax. resolveTemporaryStatus returns
// true if either(or both) of these conditions are true.
func resolveTemporaryStatus(name *tree.TableName, explicitTemp bool) bool {
	// An explicit schema can only be provided in the CREATE TEMP TABLE statement
	// iff it is pg_temp.
	if explicitTemp && name.ExplicitSchema && name.SchemaName != sessiondata.PgTempSchemaName {
		panic(pgerror.Newf(pgcode.InvalidTableDefinition, "cannot create temporary relation in non-temporary schema, table name: %v", name))
	}
	return name.SchemaName == sessiondata.PgTempSchemaName || explicitTemp
}

// resolveSchemaForCreate returns the schema that will contain a newly created
// catalog object with the given name. If the current user does not have the
// CREATE privilege, then resolveSchemaForCreate raises an error.
func (b *Builder) resolveSchemaForCreate(
	name *tree.TableName, tableType tree.TableType,
) (cat.Schema, cat.SchemaName) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	sch, resName, err := b.catalog.ResolveSchema(b.ctx, flags, &name.TableNamePrefix)
	if err != nil {
		// Remap invalid schema name error text so that it references the catalog
		// object that could not be created.
		if code := pgerror.GetPGCode(err); code == pgcode.InvalidSchemaName {
			var newErr error
			newErr = pgerror.Newf(pgcode.InvalidSchemaName,
				"cannot create %q because the target database or schema does not exist",
				tree.ErrString(name))
			newErr = errors.WithSecondaryError(newErr, err)
			newErr = errors.WithHint(newErr, "verify that the current database and search_path are valid and/or the target database exists")
			panic(newErr)
		}
		panic(err)
	}

	if tableType != tree.InstanceTable {
		if err := b.catalog.CheckPrivilege(b.ctx, sch, privilege.CREATE); err != nil {
			panic(err)
		}
	}

	return sch, resName
}

// resolveTableForMutation is a helper method for building mutations. It returns
// the table in the catalog that matches the given TableExpr, along with the
// table's MDDepName and alias, and the IDs of any columns explicitly specified
// by the TableExpr (see tree.TableRef).
//
// If the name does not resolve to a table, then resolveTableForMutation raises
// an error. Privileges are checked when resolving the table, and an error is
// raised if the current user does not have the given privilege.
func (b *Builder) resolveTableForMutation(
	n tree.TableExpr, priv privilege.Kind,
) (tab cat.Table, depName opt.MDDepName, alias tree.TableName, columns []tree.ColumnID) {
	// Strip off an outer AliasedTableExpr if there is one.
	var outerAlias *tree.TableName
	if ate, ok := n.(*tree.AliasedTableExpr); ok {
		n = ate.Expr
		// It's okay to ignore the As columns here, as they're not permitted in
		// DML aliases where this function is used. The grammar does not allow
		// them, so the parser would have reported an error if they were present.
		if ate.As.Alias != "" {
			outerAlias = tree.NewUnqualifiedTableName(ate.As.Alias)
		}
	}

	switch t := n.(type) {
	case *tree.TableName:
		tab, alias = b.resolveTable(t, priv)
		depName = opt.DepByName(t)

	case *tree.TableRef:
		tab = b.resolveTableRef(t, priv)
		alias = tree.MakeUnqualifiedTableName(t.As.Alias)
		depName = opt.DepByID(cat.StableID(t.TableID))

		// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
		// (all columns). whereas an array of length 0 means 'zero columns'.
		// Lists of zero columns are not supported and will throw an error."
		if t.Columns != nil && len(t.Columns) == 0 {
			panic(pgerror.Newf(pgcode.Syntax,
				"an explicit list of column IDs must include at least one column, table name: %v", tab.Name()))
		}
		columns = t.Columns

	case *tree.InFlightCreateTableExpr:
		// resolve child table
		isExist := true
		ds, name, err := b.catalog.ResolveDataSource(b.ctx, cat.Flags{}, &t.TableDef.Table)
		if err != nil {
			log.Infof(b.ctx, "ResolveDataSource failed for %q with error: %q", t.TableDef.Table.TableName, err.Error())
			if strings.Index(err.Error(), "does not exist") != -1 {
				isExist = false
			}
		}
		if name.Schema() == "information_schema" || name.Schema() == "kwdb_internal" || name.Schema() == sessiondata.PgCatalogName {
			panic(pgerror.Newf(pgcode.InvalidName, "schema cannot be modified: %q", tree.ErrString(&name.SchemaName)))
		}

		tab, alias = b.resolveTable(&t.TableDef.UsingSource, privilege.CREATE)
		if tab.GetTableType() != tree.TemplateTable {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%s is not a template table", t.TableDef.UsingSource.Table()))
		}
		if !isExist {
			// get template table
			tab, alias = b.resolveTable(&t.TableDef.UsingSource, privilege.CREATE)
			// Insert prepare statement to automatically create a table and disable memo caching
			if b.factory.CheckFlag(opt.IsPrepare) {
				b.DisableMemoReuse = true
			}
			b.TSInfo.TSProp = opt.AddTSProperty(b.TSInfo.TSProp, TSPropInsertCreateTable)
		} else {
			stab, _ := b.resolveTable(&t.TableDef.UsingSource, privilege.CREATE)
			// check if the super table matches the child table.
			if ds.ID() != stab.ID() {
				panic(errors.Newf("relation '%s' already exists, but does not belong to '%s'.", t.TableDef.Table.Table(), stab.Name()))
			}
			// check tag
			tags := stab.GetTagMeta()
			// remove hidden tag
			tags = tags[1:]
			// fill the field `TagName` if it is empty
			if len(t.TableDef.Tags) > 0 && t.TableDef.Tags[0].TagName == "" {
				if len(t.TableDef.Tags) != len(tags) {
					panic(pgerror.Newf(pgcode.Syntax, "Tags number mismatch, number of table tags: %v, number of input tags:%v", len(t.TableDef.Tags), len(tags)))
				}
				for i := range t.TableDef.Tags {
					t.TableDef.Tags[i].TagName = tree.Name(tags[i].TagName)
					t.TableDef.Tags[i].TagType = &tags[i].TagType
				}
			} else {
				// check if field `TagName` is valid
				for i, tag := range t.TableDef.Tags {
					isFound := false
					for _, validTag := range tags {
						if string(tag.TagName) == validTag.TagName {
							t.TableDef.Tags[i].TagType = &validTag.TagType
							isFound = true
							break
						}
					}
					if !isFound {
						panic(pgerror.Newf(pgcode.UndefinedObject, "Tag %s does not exist", tag.TagName))
					}
				}
			}
			// check if type is matched
			for _, tag := range t.TableDef.Tags {
				typ, err := tag.TagVal.TypeCheck(b.semaCtx, tag.TagType)
				if err != nil {
					log.Errorf(b.ctx, "type check failed for %s: %s", tag.TagName, err.Error())
					panic(pgerror.New(pgcode.InvalidParameterValue, err.Error()))
				}
				datum, ok := typ.(tree.Datum)
				if !ok {
					panic(pgerror.Newf(pgcode.WrongObjectType, "wrong input value type for tag %s", tag.TagName))
				}
				if datum == tree.DNull {
					continue
				}
				if err = checkAttributeType(*tag.TagType, datum); err != nil {
					log.Errorf(b.ctx, "check attribute type failed for %s: %s", tag.TagName, err.Error())
					panic(err)
				}
			}
			tab, alias = b.resolveTable(&t.TableDef.Table, priv)
		}

	default:
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q does not resolve to a table", tree.ErrString(n)))
	}

	if outerAlias != nil {
		alias = *outerAlias
	}
	// We can't mutate materialized views.
	if tab.IsMaterializedView() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "cannot mutate materialized view %q", tab.Name()))
	}
	return tab, depName, alias, columns
}

// TODO: this is template/instance table only.  Need to catch up when enable this
// feature in the future.
func checkAttributeType(attrType types.T, inVal tree.Datum) error {
	switch attrType.Family() {
	case types.BytesFamily:
		var sv string
		if v, ok := tree.AsDBytes(inVal); ok {
			sv = string(v)
		} else {
			return pgerror.New(pgcode.DatatypeMismatch,
				"The data type of the input attribute does not match the predefined type")
		}
		if attrType.Width() > 0 && utf8.RuneCountInString(sv) > int(attrType.Width()) {
			return pgerror.Newf(pgcode.StringDataRightTruncation,
				"value too long for type %s",
				attrType.SQLString())
		}
		if attrType.Width() == 0 {
			switch attrType.Oid() {
			case types.T_varbytea:
				attrType.InternalType.Width = initVarcharWidth
			case oid.T_bytea:
				attrType.InternalType.Width = initByteAWidth
			}
			if len(sv) > int(attrType.Width()) {
				return pgerror.New(pgcode.DatatypeMismatch,
					"The value of the input is too long for predefined type length")
			}
		}
	case types.StringFamily:
		var sv string
		if v, ok := tree.AsDString(inVal); ok {
			sv = string(v)
		} else {
			return pgerror.New(pgcode.DatatypeMismatch,
				"The data type of the input attribute does not match the predefined type")
		}
		if attrType.Width() > 0 && utf8.RuneCountInString(sv) > int(attrType.Width()) {
			return pgerror.Newf(pgcode.StringDataRightTruncation,
				"value too long for type %s",
				attrType.SQLString())
		}
		if attrType.Width() == 0 {
			switch attrType.Oid() {
			case oid.T_varchar:
				attrType.InternalType.Width = initVarcharWidth
			case oid.T_bpchar, types.T_nchar:
				attrType.InternalType.Width = initByteAWidth
			}
			if len(sv) > int(attrType.Width()) {
				return pgerror.New(pgcode.DatatypeMismatch,
					"The value of the input is too long for predefined type length")
			}
		}
	case types.IntFamily:
		if v, ok := tree.AsDInt(inVal); ok {
			if attrType.Width() == initInt64Width || attrType.Width() == initInt32Width ||
				attrType.Width() == initInt16Width || attrType.Width() == initInt8Width {
				// Width is defined in bits.
				width := uint(attrType.Width() - 1)

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer out of range for type %s",
						attrType.Name())
				}
			}
		} else {
			return pgerror.New(pgcode.DatatypeMismatch,
				"The data type of the input attribute does not match the predefined type")
		}
	case types.FloatFamily:
		if _, ok := inVal.(*tree.DFloat); !ok {
			if _, ok := inVal.(*tree.DDecimal); !ok {
				return pgerror.New(pgcode.DatatypeMismatch,
					"The data type of the input attribute does not match the predefined type")
			}
		}
	case types.BoolFamily:
		if _, ok := inVal.(*tree.DBool); !ok {
			return pgerror.New(pgcode.DatatypeMismatch,
				"The data type of the input attribute does not match the predefined type")
		}
	case types.TimestampFamily:
		if _, ok := inVal.(*tree.DTimestamp); !ok {
			return pgerror.New(pgcode.DatatypeMismatch,
				"The data type of the input attribute does not match the predefined type")
		}
	default:
		return pgerror.New(pgcode.DatatypeMismatch,
			"The data type of the input attribute does not match the predefined type")
	}
	return nil
}

// resolveTable returns the table in the catalog with the given name. If the
// name does not resolve to a table, or if the current user does not have the
// given privilege, then resolveTable raises an error.
func (b *Builder) resolveTable(
	tn *tree.TableName, priv privilege.Kind,
) (cat.Table, tree.TableName) {
	ds, resName := b.resolveDataSource(tn, priv)
	tab, ok := ds.(cat.Table)
	if !ok {
		panic(sqlbase.NewWrongObjectTypeError(tn, "table"))
	}
	return tab, resName
}

// resolveTableRef returns the table in the catalog that matches the given
// TableRef spec. If the name does not resolve to a table, or if the current
// user does not have the given privilege, then resolveTableRef raises an error.
func (b *Builder) resolveTableRef(ref *tree.TableRef, priv privilege.Kind) cat.Table {
	ds := b.resolveDataSourceRef(ref, priv)
	tab, ok := ds.(cat.Table)
	if !ok {
		panic(sqlbase.NewWrongObjectTypeError(ref, "table"))
	}
	return tab
}

// InstanceTabName alias and name of instrance table
type InstanceTabName struct {
	Alias  string
	DBName string
	CName  string
}

// lookupDataSource returns the data source in the catalog with the given name.
// If the name does not resolve to a table, then resolveDataSource raises an error.
func (b *Builder) lookupDataSource(tn *tree.TableName) (cat.DataSource, cat.DataSourceName) {
	var flags cat.Flags
	if b.insideViewDef || b.insideProcDef {
		// Avoid taking table leases when we're creating a view.
		flags.AvoidDescriptorCaches = true
	}
	ds, resName, _ := b.catalog.ResolveDataSource(b.ctx, flags, tn)

	return ds, resName
}

// resolveDataSource returns the data source in the catalog with the given name.
// If the name does not resolve to a table, or if the current user does not have
// the given privilege, then resolveDataSource raises an error.
//
// If the b.qualifyDataSourceNamesInAST flag is set, tn is updated to contain
// the fully qualified name.
func (b *Builder) resolveDataSource(
	tn *tree.TableName, priv privilege.Kind,
) (cat.DataSource, cat.DataSourceName) {
	var flags cat.Flags
	if b.insideViewDef || b.insideProcDef {
		// Avoid taking table leases when we're creating a view.
		flags.AvoidDescriptorCaches = true
	}
	ds, resName, err := b.catalog.ResolveDataSource(b.ctx, flags, tn)
	if err != nil {
		panic(err)
	}

	b.checkPrivilege(opt.DepByName(tn), ds, priv)

	if b.qualifyDataSourceNamesInAST {
		*tn = resName
		tn.ExplicitCatalog = true
		tn.ExplicitSchema = true
	}

	return ds, resName
}

// resolveDataSourceFromRef returns the data source in the catalog that matches
// the given TableRef spec. If no data source matches, or if the current user
// does not have the given privilege, then resolveDataSourceFromRef raises an
// error.
func (b *Builder) resolveDataSourceRef(ref *tree.TableRef, priv privilege.Kind) cat.DataSource {
	var flags cat.Flags
	if b.insideViewDef || b.insideProcDef {
		// Avoid taking table leases when we're creating a view.
		flags.AvoidDescriptorCaches = true
	}
	ds, _, err := b.catalog.ResolveDataSourceByID(b.ctx, flags, cat.StableID(ref.TableID))
	if err != nil {
		panic(pgerror.Wrapf(err, pgcode.UndefinedObject, "%s", tree.ErrString(ref)))
	}
	b.checkPrivilege(opt.DepByID(cat.StableID(ref.TableID)), ds, priv)
	return ds
}

// checkPrivilege ensures that the current user has the privilege needed to
// access the given object in the catalog. If not, then checkPrivilege raises an
// error. It also adds the object and it's original unresolved name as a
// dependency to the metadata, so that the privileges can be re-checked on reuse
// of the memo.
func (b *Builder) checkPrivilege(name opt.MDDepName, ds cat.DataSource, priv privilege.Kind) {
	if !(priv == privilege.SELECT && b.skipSelectPrivilegeChecks) {
		err := b.catalog.CheckPrivilege(b.ctx, ds, priv)
		if err != nil {
			panic(err)
		}
	} else {
		// The check is skipped, so don't recheck when dependencies are checked.
		priv = 0
	}

	// Add dependency on this object to the metadata, so that the metadata can be
	// cached and later checked for freshness.
	b.factory.Metadata().AddDependency(name, ds, priv)
}

// if aggregation is last/lastts/last_row/first/firstts/first_row
func checkLastOrFirstAgg(name string) bool {
	return checkFirstAgg(name) || checkLastAgg(name)
}

// if aggregation is last/lastTs
func checkLastOrLastTsAgg(name string) bool {
	return name == sqlbase.LastAgg || name == sqlbase.LastTSAgg
}

// if aggregation is first/firstts/first_row
func checkFirstAgg(name string) bool {
	return name == sqlbase.FirstAgg || name == sqlbase.FirstRowAgg || name == sqlbase.FirstTSAgg || name == sqlbase.FirstRowTSAgg
}

// if aggregation is last/lastts/last_row
func checkLastAgg(name string) bool {
	return name == sqlbase.LastAgg || name == sqlbase.LastRowAgg || name == sqlbase.LastTSAgg || name == sqlbase.LastRowTSAgg
}

// checkTwaOrElapsedAgg is used to check twa or elapsed aggregation
func checkTwaOrElapsedAgg(name string) bool {
	return name == "twa" || name == "elapsed"
}

// handleLastAgg limit last agg can only use in pure time-series scenarios,and
// add a timestamp column as the second parameter to the last function
// Parameters:
// -s: record the table type.
// -e: is the FuncExpr.
// -funcName: the name of function.
//
// ex:
// select last(a) from ts-table;  (timeseries table)----seccess
// select last(b) from rdb-table; (relational table)----error
// select last(a) from rdb-table t join ts-table t1 on t.b=t1.a; (multi table) ----error
// select * from rdb-table join (select last(a) from ts-table) on t.b=t1.a;  (timeseries subquery)----seccess
// add the timestamp column:
// last(a)-> last(a,timestamp),when out of order,the latest time data can be retrieved based on the timestamp column.
func handleLastAgg(s *scope, e tree.Expr, funcName string) {
	if s.TableType == nil || ((s.TableType.HasRtable() || s.HasMultiTable) && !(s.builder.factory.Memo().QueryType == memo.MultiModel)) ||
		s.TableType.HasMultiTSTable() || opt.CheckTsProperty(s.ScopeTSProp, ScopeLastError) {
		panic(pgerror.Newf(pgcode.FeatureNotSupported, "%v() can only be used in timeseries table query or subquery", funcName))
	}
	s.builder.factory.Memo().MultimodelHelper.HasLastAgg = true

	// add timestamp column.
	if f, ok1 := e.(*tree.FuncExpr); ok1 {
		if col, ok := f.Exprs[0].(*scopeColumn); ok {
			tblID := s.builder.factory.Metadata().ColumnMeta(col.id).Table
			if tblID == 0 {
				panic(pgerror.Newf(pgcode.FeatureNotSupported, "%v() can only be used in timeseries table query or subquery", funcName))
			}
			tbl := s.builder.factory.Metadata().Table(tblID)
			typ := tbl.Column(0).DatumType()
			checkBoundary(s, *f, funcName, typ)
			// add const bound
			if (funcName == sqlbase.LastAgg || funcName == sqlbase.LastTSAgg) && len(f.Exprs) == 1 {
				boundary := tree.DString(tree.TsMaxTimestampString)
				if typ.Precision() == 9 {
					boundary = tree.TsMaxNanoTimestampString
				}
				cast, _ := tree.NewTypedCastExpr(&boundary, types.MakeTimestampTZ(typ.Precision()))
				f.Exprs = append(f.Exprs, cast)
			}
			// add ts column as param3
			f.Exprs = append(f.Exprs, &scopeColumn{name: tbl.Column(0).ColName(),
				table: col.table, typ: typ, id: tblID.ColumnID(0),
			})
		}
	}
}

// addLastFunction add last to outScope for last(*) or last(<table>.*)
func (b *Builder) addLastFunction(
	aliases []string,
	exprs []tree.TypedExpr,
	funcName string,
	inScope, outScope *scope,
	funcExpr tree.Expr,
) {
	var twiceParam tree.Expr
	if f, ok := funcExpr.(*tree.FuncExpr); ok && checkLastOrLastTsAgg(funcName) && len(f.Exprs) == 2 {
		twiceParam = f.Exprs[1]
	}

	unresolveExpr := &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{funcName}}

	if twiceParam != nil {
		for j, e := range exprs {
			lastCol := &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: unresolveExpr},
				Exprs: tree.Exprs{e},
			}
			lastCol.Exprs = append(lastCol.Exprs, twiceParam)
			resolvedLast := inScope.resolveType(lastCol, types.Any)
			b.addColumn(outScope, funcName+"("+aliases[j]+")", resolvedLast, false)
		}
	} else {
		for j, e := range exprs {
			lastCol := &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: unresolveExpr},
				Exprs: tree.Exprs{e},
			}
			resolvedLast := inScope.resolveType(lastCol, types.Any)
			b.addColumn(outScope, funcName+"("+aliases[j]+")", resolvedLast, false)
		}
	}
}

// checkBoundary checks if the second parameter of last is greater than '2970-01-01 00:00:00+00:00'
func checkBoundary(s *scope, f tree.FuncExpr, funcName string, t *types.T) {
	if funcName == sqlbase.LastAgg && len(f.Exprs) == 2 {
		maxTsString := tree.TsMaxTimestampString
		if t.Precision() == 9 {
			maxTsString = tree.TsMaxNanoTimestampString
		}
		typedExpr, err := tree.TypeCheck(f.Exprs[1], s.builder.semaCtx, types.MakeTimestampTZ(9))
		if err != nil || typedExpr == nil {
			panic(pgerror.Newf(pgcode.DatatypeMismatch, "the second parameter of last must be of timestamptz type"))
		}
		if datum, ok2 := typedExpr.(*tree.DTimestampTZ); ok2 {
			maxDatum, _ := tree.ParseDTimestampTZ(s.builder.evalCtx, maxTsString, time.Nanosecond)
			res := datum.Compare(s.builder.evalCtx, maxDatum)
			if res > 0 {
				panic(pgerror.Newf(pgcode.FeatureNotSupported, "the second parameter of last is out of range"))
			}
		} else {
			panic(pgerror.Newf(pgcode.DatatypeMismatch, "the second parameter of last must be of timestamptz type"))
		}
	}
}

// handleTwaOrElapsedAgg processes TWA and Elapsed aggregation functions.
func (b *Builder) handleTwaOrElapsedAgg(e tree.Expr, funcName string) {
	// Check if the expression is a function expression
	if f, isFuncExpr := e.(*tree.FuncExpr); isFuncExpr {
		// Handle each aggregation function based on the name
		switch funcName {
		case "twa":
			b.handleTwaAgg(f)
		case "elapsed":
			b.handleElapsedAgg(f)
		default:
			// If it's not a TWA or Elapsed function, return without modification
			return
		}
	}
}

// handleTwaAgg processes the twa function's arguments, ensuring that the first argument
// is a primary timestamp and applies to time series tables.
func (b *Builder) handleTwaAgg(f *tree.FuncExpr) {
	if len(f.Exprs) == 2 {
		// Extract the timestamp argument
		if tsArg, ok := f.Exprs[0].(*scopeColumn); ok {
			tableID := b.factory.Metadata().ColumnMeta(tsArg.id).Table
			// If table ID is 0, the timestamp argument is not a table column
			if tableID == 0 {
				panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "twa")))
			}

			tsTable := b.factory.Metadata().TableMeta(tableID).Table
			// Check if the table is a time series table.
			if tsTable.GetTableType() != tree.TimeseriesTable {
				panic(pgerror.New(pgcode.FeatureNotSupported, fmt.Sprintf("%s function can only be used in time series table", "twa")))
			}
			// Ensure the timestamp column is the primary timestamp column (index 0)
			if tableID.ColumnOrdinal(tsArg.id) != 0 {
				panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "twa")))
			}
		} else {
			panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "twa")))
		}
	} else {
		panic(pgerror.New(pgcode.UndefinedFunction, fmt.Sprintf("invalid parameter number in %s function", "twa")))
	}
}

// handleElapsedAgg processes the elapsed function, ensuring that the first argument
// is a primary timestamp and applies to time series tables.
func (b *Builder) handleElapsedAgg(f *tree.FuncExpr) {
	if len(f.Exprs) == 1 || len(f.Exprs) == 2 {
		// Extract the timestamp argument.
		if tsArg, ok := f.Exprs[0].(*scopeColumn); ok {
			tableID := b.factory.Metadata().ColumnMeta(tsArg.id).Table
			// If table ID is 0, the timestamp argument is not a table column.
			if tableID == 0 {
				panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "elapsed")))
			}

			tsTable := b.factory.Metadata().TableMeta(tableID).Table
			// Check if the table is a time series table.
			if tsTable.GetTableType() != tree.TimeseriesTable {
				panic(pgerror.New(pgcode.FeatureNotSupported, fmt.Sprintf("%s function can only be used in time series table", "elapsed")))
			}
			// Ensure the timestamp column is the primary timestamp column (index 0).
			if tableID.ColumnOrdinal(tsArg.id) != 0 {
				panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "elapsed")))
			}

			// If there's only one argument, add a constant time unit argument.
			if len(f.Exprs) == 1 {
				// Time unit of 1 millisecond as a constant value.
				tsTimeUnit := tree.DInterval{Duration: duration.MakeDuration(1000000, 0, 0)}
				f.Exprs = append(f.Exprs, &tsTimeUnit)
			}
		} else {
			panic(pgerror.New(pgcode.DatatypeMismatch, fmt.Sprintf("first parameter should be primary timestamp in %s function", "elapsed")))
		}
	} else {
		panic(pgerror.New(pgcode.UndefinedFunction, fmt.Sprintf("invalid parameter number in %s function", "elapsed")))
	}
}

// checkUnsupportedWindowFunctions checks if certain aggregate functions are used as window functions.
func checkUnsupportedWindowFunctions(funcName string) {
	if checkLastOrFirstAgg(funcName) || funcName == tree.FuncMatching || funcName == tree.FuncBucketFill || funcName == tree.FuncInterpolate ||
		checkTwaOrElapsedAgg(funcName) {
		panic(pgerror.Newf(pgcode.FeatureNotSupported, "%v() is not supported as a window function", funcName))
	}
}
