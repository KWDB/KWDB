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
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

const (
	excludeMutations = false
	includeMutations = true
)

// buildDataSource builds a set of memo groups that represent the given table
// expression. For example, if the tree.TableExpr consists of a single table,
// the resulting set of memo groups will consist of a single group with a
// scanOp operator. Joins will result in the construction of several groups,
// including two for the left and right table scans, at least one for the join
// condition, and one for the join itself.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildDataSource(
	texpr tree.TableExpr,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
	alias tree.Name,
) (outScope *scope) {
	defer func(prevAtRoot bool) {
		inScope.atRoot = prevAtRoot
	}(inScope.atRoot)
	inScope.atRoot = false
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		if source.IndexFlags != nil {
			telemetry.Inc(sqltelemetry.IndexHintUseCounter)
			telemetry.Inc(sqltelemetry.IndexHintSelectUseCounter)
			indexFlags = source.IndexFlags
		}
		if source.As.Alias != "" {
			locking = locking.filter(source.As.Alias)
		}

		outScope = b.buildDataSource(source.Expr, indexFlags, locking, inScope, source.As.Alias)

		if source.Ordinality {
			outScope = b.buildWithOrdinality("ordinality", outScope)
		}

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)

		return outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, locking, inScope)

	case *tree.TableName:
		tn := source

		// CTEs take precedence over other data sources.
		if cte := inScope.resolveCTE(tn); cte != nil {
			locking.ignoreLockingForCTE()
			outScope = inScope.push()
			inCols := make(opt.ColList, len(cte.cols))
			outCols := make(opt.ColList, len(cte.cols))
			outScope.cols = nil
			b.factory.Metadata().SetTblType(tree.RelationalTable)
			for i, col := range cte.cols {
				id := col.ID
				c := b.factory.Metadata().ColumnMeta(id)
				newCol := b.synthesizeColumn(outScope, col.Alias, c.Type, nil, nil)
				newCol.table = *tn
				inCols[i] = id
				outCols[i] = newCol.id
			}

			outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
				With:    cte.id,
				Name:    string(cte.name.Alias),
				InCols:  inCols,
				OutCols: outCols,
				ID:      b.factory.Metadata().NextUniqueID(),
			})

			return outScope
		}

		priv := privilege.SELECT
		locking = locking.filter(tn.TableName)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE requires UPDATE privileges.
			priv = privilege.UPDATE
		}

		ds, resName := b.resolveDataSource(tn, priv)
		switch t := ds.(type) {
		case cat.Table:
			switch t.GetTableType() {
			case tree.RelationalTable:
				tabMeta := b.addTable(t, &resName)
				// case relational table.
				return b.buildScan(tabMeta, nil /* ordinals */, indexFlags, locking, excludeMutations, inScope)
			case tree.InstanceTable:
				cDesc, isExist, err := sqlbase.ResolveInstanceName(b.ctx, b.evalCtx.Txn, string(resName.CatalogName), string(resName.TableName))
				if isExist && err == nil {
					t.SetTableName(cDesc.STableName)
				}
				tabMeta := b.addTable(t, &resName)
				instance := InstanceTabName{CName: string(resName.TableName), DBName: string(resName.CatalogName), Alias: string(alias)}
				b.InstanceTabNames = append(b.InstanceTabNames, instance)
				return b.buildTimeSeriesScan(tabMeta, indexFlags, &resName, inScope)
			case tree.TimeseriesTable, tree.TemplateTable:
				tabMeta := b.addTable(t, &resName)
				// case time series table.
				return b.buildTimeSeriesScan(tabMeta, indexFlags, &resName, inScope)
			default:
				panic(pgerror.Newf(pgcode.Warning, "unknown table type: %d", t.GetTableType()))
			}

		case cat.Sequence:
			return b.buildSequenceSelect(t, &resName, inScope)

		case cat.View:
			return b.buildView(t, &resName, locking, inScope)

		default:
			panic(errors.AssertionFailedf("unknown DataSource type %T", ds))
		}

	case *tree.ParenTableExpr:
		return b.buildDataSource(source.Expr, indexFlags, locking, inScope, "")

	case *tree.RowsFromExpr:
		return b.buildZip(source.Items, inScope)

	case *tree.Subquery:
		// Remove any target relations from the current scope's locking spec, as
		// those only apply to relations in this statement. Interestingly, this
		// would not be necessary if we required all subqueries to have aliases
		// like Postgres does.
		locking = locking.withoutTargets()

		outScope = b.buildSelectStmt(source.Select, locking, nil /* desiredTypes */, inScope)
		// Treat the subquery result as an anonymous data source (i.e. column names
		// are not qualified). Remove hidden columns, as they are not accessible
		// outside the subquery.
		outScope.setTableAlias("")
		outScope.removeHiddenCols()

		return outScope

	case *tree.StatementSource:
		if b.insideObjectDef.HasFlags(InsideProcedureDef) {
			panic(unimplemented.New("Stored Procedures",
				"statement source (square bracket syntax) within Stored Procedure"))
		}

		// This is the special '[ ... ]' syntax. We treat this as syntactic sugar
		// for a top-level CTE, so it cannot refer to anything in the input scope.
		// See #41078.
		emptyScope := b.allocScope()
		innerScope := b.buildStmt(source.Statement, nil /* desiredTypes */, emptyScope)
		if len(innerScope.cols) == 0 {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"statement source \"%v\" does not return any columns", source.Statement))
		}

		id := b.factory.Memo().NextWithID()
		b.factory.Metadata().AddWithBinding(id, innerScope.expr)
		cte := cteSource{
			name:         tree.AliasClause{},
			cols:         innerScope.makePresentationWithHiddenCols(),
			originalExpr: source.Statement,
			expr:         innerScope.expr,
			id:           id,
		}
		b.cteStack[len(b.cteStack)-1] = append(b.cteStack[len(b.cteStack)-1], cte)

		inCols := make(opt.ColList, len(cte.cols))
		outCols := make(opt.ColList, len(cte.cols))
		for i, col := range cte.cols {
			id := col.ID
			c := b.factory.Metadata().ColumnMeta(id)
			inCols[i] = id
			outCols[i] = b.factory.Metadata().AddColumn(col.Alias, c.Type)
		}

		locking.ignoreLockingForCTE()
		outScope = inScope.push()
		// Similar to appendColumnsFromScope, but with re-numbering the column IDs.
		for i, col := range innerScope.cols {
			col.scalar = nil
			col.id = outCols[i]
			outScope.cols = append(outScope.cols, col)
		}

		outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
			With:    cte.id,
			Name:    string(cte.name.Alias),
			InCols:  inCols,
			OutCols: outCols,
			ID:      b.factory.Metadata().NextUniqueID(),
		})

		return outScope

	case *tree.TableRef:
		priv := privilege.SELECT
		locking = locking.filter(source.As.Alias)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE requires UPDATE privileges.
			priv = privilege.UPDATE
		}

		ds := b.resolveDataSourceRef(source, priv)
		switch t := ds.(type) {
		case cat.Table:
			outScope = b.buildScanFromTableRef(t, source, indexFlags, locking, inScope)
		case cat.View:
			if source.Columns != nil {
				panic(pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot specify an explicit column list when accessing a view by reference, view name: %v", t.Name()))
			}
			tn := tree.MakeUnqualifiedTableName(t.Name())

			outScope = b.buildView(t, &tn, locking, inScope)
		case cat.Sequence:
			tn := tree.MakeUnqualifiedTableName(t.Name())
			// Any explicitly listed columns are ignored.
			outScope = b.buildSequenceSelect(t, &tn, inScope)
		default:
			panic(errors.AssertionFailedf("unsupported catalog object: %v", t.Name()))
		}
		b.renameSource(source.As, outScope)
		return outScope

	default:
		panic(errors.AssertionFailedf("unknown table expr: %T", texpr))
	}
}

// buildView parses the view query text and builds it as a Select expression.
func (b *Builder) buildView(
	view cat.View, viewName *tree.TableName, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	// Cache the AST so that multiple references won't need to reparse.
	if b.views == nil {
		b.views = make(map[cat.View]*tree.Select)
	}

	// Check whether view has already been parsed, and if not, parse now.
	sel, ok := b.views[view]
	if !ok {
		stmt, err := parser.ParseOne(view.Query())
		if err != nil {
			wrapped := pgerror.Wrapf(err, pgcode.Syntax,
				"failed to parse underlying query from view %q", view.Name())
			panic(wrapped)
		}

		sel, ok = stmt.AST.(*tree.Select)
		if !ok {
			panic(errors.AssertionFailedf("expected SELECT statement: %v", stmt.SQL))
		}

		b.views[view] = sel

		// Keep track of referenced views for EXPLAIN (opt, env).
		b.factory.Metadata().AddView(view)
	}

	// When building the view, we don't want to check for the SELECT privilege
	// on the underlying tables, just on the view itself. Checking on the
	// underlying tables as well would defeat the purpose of having separate
	// SELECT privileges on the view, which is intended to allow for exposing
	// some subset of a restricted table's data to less privileged users.
	if !b.skipSelectPrivilegeChecks {
		b.skipSelectPrivilegeChecks = true
		defer func() { b.skipSelectPrivilegeChecks = false }()
	}
	trackDeps := b.trackViewDeps
	if trackDeps {
		// We are only interested in the direct dependency on this view descriptor.
		// Any further dependency by the view's query should not be tracked.
		b.trackViewDeps = false
		defer func() { b.trackViewDeps = true }()
	}

	// We don't want the view to be able to refer to any outer scopes in the
	// query. This shouldn't happen if the view is valid but there may be
	// cornercases (e.g. renaming tables referenced by the view). To be safe, we
	// build the view with an empty scope. But after that, we reattach the scope
	// to the existing scope chain because we want the rest of the query to be
	// able to refer to the higher scopes (see #46180).
	emptyScope := b.allocScope()
	outScope = b.buildSelect(sel, locking, nil /* desiredTypes */, emptyScope)
	emptyScope.parent = inScope

	// Update data source name to be the name of the view. And if view columns
	// are specified, then update names of output columns.
	hasCols := view.ColumnNameCount() > 0
	for i := range outScope.cols {
		outScope.cols[i].table = *viewName
		if hasCols {
			outScope.cols[i].name = view.ColumnName(i)
		}
	}

	if trackDeps && !view.IsSystemView() {
		dep := opt.ViewDep{DataSource: view}
		for i := range outScope.cols {
			dep.ColumnOrdinals.Add(i)
		}
		b.viewDeps = append(b.viewDeps, dep)
	}

	return outScope
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	if as.Alias != "" {
		colAlias := as.Cols

		// Special case for Postgres compatibility: if a data source does not
		// currently have a name, and it is a set-generating function or a scalar
		// function with just one column, and the AS clause doesn't specify column
		// names, then use the specified table name both as the column name and
		// table name.
		noColNameSpecified := len(colAlias) == 0
		if scope.isAnonymousTable() && noColNameSpecified && scope.singleSRFColumn {
			colAlias = tree.NameList{as.Alias}
		}

		// If an alias was specified, use that to qualify the column names.
		tableAlias := tree.MakeUnqualifiedTableName(as.Alias)
		scope.setTableAlias(as.Alias)

		// If input expression is a ScanExpr, then override metadata aliases for
		// pretty-printing.
		scan, isScan := scope.expr.(*memo.ScanExpr)
		if isScan {
			tabMeta := b.factory.Metadata().TableMeta(scan.ScanPrivate.Table)
			tabMeta.Alias = tree.MakeUnqualifiedTableName(as.Alias)
		}

		if len(colAlias) > 0 {
			// The column aliases can only refer to explicit columns.
			for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
				if colIdx >= len(scope.cols) {
					srcName := tree.ErrString(&tableAlias)
					panic(pgerror.Newf(
						pgcode.InvalidColumnReference,
						"source %q has %d columns available but %d columns specified",
						srcName, aliasIdx, len(colAlias),
					))
				}
				col := &scope.cols[colIdx]
				if col.hidden {
					continue
				}
				col.name = colAlias[aliasIdx]
				if isScan {
					// Override column metadata alias.
					colMeta := b.factory.Metadata().ColumnMeta(col.id)
					colMeta.Alias = string(colAlias[aliasIdx])
				}
				aliasIdx++
			}
		}
	}
}

// buildScanFromTableRef adds support for numeric references in queries.
// For example:
// SELECT * FROM [53 as t]; (table reference)
// SELECT * FROM [53(1) as t]; (+columnar reference)
// SELECT * FROM [53(1) as t]@1; (+index reference)
// Note, the query SELECT * FROM [53() as t] is unsupported. Column lists must
// be non-empty
func (b *Builder) buildScanFromTableRef(
	tab cat.Table,
	ref *tree.TableRef,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
) (outScope *scope) {
	var ordinals []int
	if ref.Columns != nil {
		// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
		// (all columns). whereas an array of length 0 means 'zero columns'.
		// Lists of zero columns are not supported and will throw an error."
		if len(ref.Columns) == 0 {
			panic(pgerror.Newf(pgcode.Syntax,
				"an explicit list of column IDs must include at least one column, table name: %v", tab.Name()))
		}
		ordinals = cat.ConvertColumnIDsToOrdinals(tab, ref.Columns)
	}

	tn := tree.MakeUnqualifiedTableName(tab.Name())
	tabMeta := b.addTable(tab, &tn)
	return b.buildScan(tabMeta, ordinals, indexFlags, locking, excludeMutations, inScope)
}

// addTable adds a table to the metadata and returns the TableMeta. The table
// name is passed separately in order to preserve knowledge of whether the
// catalog and schema names were explicitly specified.
func (b *Builder) addTable(tab cat.Table, alias *tree.TableName) *opt.TableMeta {
	md := b.factory.Metadata()
	tabID := md.AddTable(tab, alias)
	return md.TableMeta(tabID)
}

// buildScan builds a memo group for a ScanOp or VirtualScanOp expression on the
// given table.
//
// If the ordinals slice is not nil, then only columns with ordinals in that
// list are projected by the scan. Otherwise, all columns from the table are
// projected.
//
// If scanMutationCols is true, then include columns being added or dropped from
// the table. These are currently required by the execution engine as "fetch
// columns", when performing mutation DML statements (INSERT, UPDATE, UPSERT,
// DELETE).
//
// NOTE: Callers must take care that these mutation columns are never used in
//
//	any other way, since they may not have been initialized yet by the
//	backfiller!
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScan(
	tabMeta *opt.TableMeta,
	ordinals []int,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	scanMutationCols bool,
	inScope *scope,
) (outScope *scope) {
	tab := tabMeta.Table
	tabID := tabMeta.MetaID

	if indexFlags != nil && indexFlags.IgnoreForeignKeys {
		tabMeta.IgnoreForeignKeys = true
	}

	colCount := len(ordinals)
	if colCount == 0 {
		// If scanning mutation columns, then include writable and deletable
		// columns in the output, in addition to public columns.
		if scanMutationCols {
			colCount = tab.DeletableColumnCount()
		} else {
			colCount = tab.ColumnCount()
		}
	}

	getOrdinal := func(i int) int {
		if ordinals == nil {
			return i
		}
		return ordinals[i]
	}

	var tabColIDs opt.ColSet
	outScope = inScope.push()
	outScope.cols = make([]scopeColumn, 0, colCount)
	for i := 0; i < colCount; i++ {
		ord := getOrdinal(i)
		col := tab.Column(ord)
		colID := tabID.ColumnID(ord)
		tabColIDs.Add(colID)
		name := col.ColName()
		isMutation := cat.IsMutationColumn(tab, ord)
		outScope.cols = append(outScope.cols, scopeColumn{
			id:       colID,
			name:     name,
			table:    tabMeta.Alias,
			typ:      col.DatumType(),
			hidden:   col.IsHidden() || isMutation,
			mutation: isMutation,
		})
	}

	if outScope.TableType == nil {
		outScope.TableType = make(map[tree.TableType]int)
	}
	outScope.TableType.Insert(tab.GetTableType())

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(pgerror.Newf(pgcode.Syntax,
				"index flags not allowed with virtual tables, table name: %v", tab.Name()))
		}
		if locking.isSet() {
			panic(pgerror.Newf(pgcode.Syntax,
				"%s not allowed with virtual tables, table name: %v", locking.get().Strength, tab.Name()))
		}
		private := memo.VirtualScanPrivate{Table: tabID, Cols: tabColIDs}
		outScope.expr = b.factory.ConstructVirtualScan(&private)

		// Virtual tables should not be collected as view dependencies.
	} else {
		private := memo.ScanPrivate{Table: tabID, Cols: tabColIDs}
		if indexFlags != nil {
			if indexFlags.FromHintTree == true {
				private.Flags.HintType = indexFlags.HintType
				private.Flags.TotalCardinality = indexFlags.TotalCardinality
				private.Flags.EstimatedCardinality = indexFlags.EstimatedCardinality
				private.Flags.LeadingTable = indexFlags.LeadingTable
				if len(indexFlags.ForestIndex) > 0 {
					var idx []int
					var idxName []tree.Name
					for i := 0; i < tab.IndexCount(); i++ {
						for _, indexName := range indexFlags.ForestIndex {
							if tab.Index(i).Name() == tree.Name(indexName) {
								idx = append(idx, i)
								idxName = append(idxName, tree.Name(indexName))
							}
						}
					}
					// If no index is found, hint is not forced, and an error warning is added
					if len(idx) == 0 {
						if indexFlags.HintType == keys.UseIndexScan || indexFlags.HintType == keys.UseIndexOnly ||
							indexFlags.HintType == keys.IgnoreIndexScan || indexFlags.HintType == keys.IgnoreIndexOnly {
							panic(pgerror.Newf(pgcode.ExternalBindScanIndex, "stmt hint err: error index for hint: %v, table name: %v", indexFlags.HintType, tab.Name()))
						}
						if indexFlags.HintType == keys.ForceIndexScan || indexFlags.HintType == keys.ForceIndexOnly {
							panic(pgerror.Newf(pgcode.ExternalBindScanIndex, "stmt hint err: error index for hint: %v, table name: %v", indexFlags.HintType, tab.Name()))
						}
					}
					private.Flags.HintIndexes = idx
					private.Flags.IndexName = idxName
					private.Flags.TableName = tab.Name()
				}
			}
			private.Flags.NoIndexJoin = indexFlags.NoIndexJoin
			if indexFlags.Index != "" || indexFlags.IndexID != 0 {
				idx := -1
				for i := 0; i < tab.IndexCount(); i++ {
					if tab.Index(i).Name() == tree.Name(indexFlags.Index) ||
						tab.Index(i).ID() == cat.StableID(indexFlags.IndexID) {
						idx = i
						break
					}
				}
				if idx == -1 {
					var err error
					if indexFlags.Index != "" {
						err = errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))
					} else {
						err = errors.Errorf("index [%d] not found", indexFlags.IndexID)
					}
					panic(err)
				}
				private.Flags.ForceIndex = true
				private.Flags.Index = idx
				private.Flags.Direction = indexFlags.Direction
			}
		}
		if locking.isSet() {
			private.Locking = locking.get()
		}
		outScope.expr = b.factory.ConstructScan(&private)

		b.addCheckConstraintsForTable(tabMeta)
		b.addComputedColsForTable(tabMeta)

		if b.trackViewDeps {
			dep := opt.ViewDep{DataSource: tab}
			dep.ColumnIDToOrd = make(map[opt.ColumnID]int)
			// We will track the ColumnID to Ord mapping so Ords can be added
			// when a column is referenced.
			for i, col := range outScope.cols {
				dep.ColumnIDToOrd[col.id] = getOrdinal(i)
			}
			if private.Flags.ForceIndex {
				dep.SpecificIndex = true
				dep.Index = private.Flags.Index
			}
			b.viewDeps = append(b.viewDeps, dep)
		}
	}
	return outScope
}

// addCheckConstraintsForTable finds all the check constraints that apply to the
// table and adds them to the table metadata. To do this, the scalar expression
// of the check constraints are built here.
func (b *Builder) addCheckConstraintsForTable(tabMeta *opt.TableMeta) {
	// Find all the check constraints that apply to the table and add them
	// to the table metadata. To do this, we must build them into scalar
	// expressions.
	var tableScope *scope
	tab := tabMeta.Table
	for i, n := 0, tab.CheckCount(); i < n; i++ {
		checkConstraint := tab.Check(i)

		// Only add validated check constraints to the table's metadata.
		if !checkConstraint.Validated {
			continue
		}
		expr, err := parser.ParseExpr(checkConstraint.Constraint)
		if err != nil {
			panic(err)
		}

		if tableScope == nil {
			tableScope = b.allocScope()
			tableScope.appendColumnsFromTable(tabMeta, &tabMeta.Alias)
		}

		if texpr := tableScope.resolveAndRequireType(expr, types.Bool); texpr != nil {
			scalar := b.buildScalar(texpr, tableScope, nil, nil, nil)
			tabMeta.AddConstraint(scalar)
		}
	}
}

// addComputedColsForTable finds all computed columns in the given table and
// caches them in the table metadata as scalar expressions.
func (b *Builder) addComputedColsForTable(tabMeta *opt.TableMeta) {
	var tableScope *scope
	tab := tabMeta.Table
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabCol := tab.Column(i)
		if !tabCol.IsComputed() {
			continue
		}
		expr, err := parser.ParseExpr(tabCol.ComputedExprStr())
		if err != nil {
			continue
		}

		if tableScope == nil {
			tableScope = b.allocScope()
			tableScope.appendColumnsFromTable(tabMeta, &tabMeta.Alias)
		}

		if texpr := tableScope.resolveAndRequireType(expr, types.Any); texpr != nil {
			colID := tabMeta.MetaID.ColumnID(i)
			scalar := b.buildScalar(texpr, tableScope, nil, nil, nil)
			tabMeta.AddComputedCol(colID, scalar)
		}
	}
}

func (b *Builder) buildSequenceSelect(
	seq cat.Sequence, seqName *tree.TableName, inScope *scope,
) (outScope *scope) {
	md := b.factory.Metadata()
	outScope = inScope.push()

	cols := make(opt.ColList, len(sqlbase.SequenceSelectColumns))

	for i, c := range sqlbase.SequenceSelectColumns {
		cols[i] = md.AddColumn(c.Name, c.Typ)
	}

	outScope.cols = make([]scopeColumn, 3)
	for i, c := range cols {
		col := md.ColumnMeta(c)
		outScope.cols[i] = scopeColumn{
			id:    c,
			name:  tree.Name(col.Alias),
			table: *seqName,
			typ:   col.Type,
		}
	}

	private := memo.SequenceSelectPrivate{
		Sequence: md.AddSequence(seq),
		Cols:     cols,
	}
	outScope.expr = b.factory.ConstructSequenceSelect(&private)

	if b.trackViewDeps {
		b.viewDeps = append(b.viewDeps, opt.ViewDep{DataSource: seq})
	}
	return outScope
}

// buildWithOrdinality builds a group which appends an increasing integer column to
// the output. colName optionally denotes the name this column is given, or can
// be blank for none.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildWithOrdinality(colName string, inScope *scope) (outScope *scope) {
	col := b.synthesizeColumn(inScope, colName, types.Int, nil, nil /* scalar */)

	// See https://www.cockroachlabs.com/docs/stable/query-order.html#order-preservation
	// for the semantics around WITH ORDINALITY and ordering.

	input := inScope.expr.(memo.RelExpr)
	inScope.expr = b.factory.ConstructOrdinality(input, &memo.OrdinalityPrivate{
		Ordering: inScope.makeOrderingChoice(),
		ColID:    col.id,
	})
	// ordering is stored by Ordinality, we need not handle orderBy in fromSubquery.
	inScope.noHandleOrderInFrom = true

	return inScope
}

func (b *Builder) buildCTEs(with *tree.With, inScope *scope) (outScope *scope) {
	if with == nil {
		return inScope
	}
	if b.insideObjectDef.HasFlags(InsideProcedureDef) {
		panic(unimplemented.New("Stored Procedures", "CTE usage inside a Stored Procedure"))
	}

	outScope = inScope.push()
	addedCTEs := make([]cteSource, len(with.CTEList))
	hasRecursive := false

	// Make a fake subquery to ensure that no CTEs are correlated.
	// TODO(justin): relax this restriction.
	outer := b.subquery
	defer func() { b.subquery = outer }()
	b.subquery = &subquery{}

	outScope.ctes = make(map[string]*cteSource)
	for i, cte := range with.CTEList {
		hasRecursive = hasRecursive || with.Recursive
		cteExpr, cteCols := b.buildCTE(cte, outScope, with.Recursive)

		// TODO(justin): lift this restriction when possible. WITH should be hoistable.
		if b.subquery != nil && !b.subquery.outerCols.Empty() {
			panic(pgerror.Newf(pgcode.FeatureNotSupported, "CTEs may not be correlated"))
		}

		aliasStr := cte.Name.Alias.String()
		if _, ok := outScope.ctes[aliasStr]; ok {
			panic(pgerror.Newf(
				pgcode.DuplicateAlias, "WITH query name %s specified more than once", aliasStr,
			))
		}

		id := b.factory.Memo().NextWithID()
		b.factory.Metadata().AddWithBinding(id, cteExpr)

		addedCTEs[i] = cteSource{
			name:         cte.Name,
			cols:         cteCols,
			originalExpr: cte.Stmt,
			expr:         cteExpr,
			id:           id,
		}
		cte := &addedCTEs[i]
		outScope.ctes[cte.name.Alias.String()] = cte
		b.cteStack[len(b.cteStack)-1] = append(b.cteStack[len(b.cteStack)-1], *cte)

		if cteExpr.Relational().CanMutate && !inScope.atRoot {
			panic(
				pgerror.Newf(
					pgcode.FeatureNotSupported,
					"WITH clause containing a data-modifying statement must be at the top level, SQL: %v", with.CTEList[i].Stmt.String(),
				),
			)
		}

	}

	telemetry.Inc(sqltelemetry.CteUseCounter)
	if hasRecursive {
		telemetry.Inc(sqltelemetry.RecursiveCteUseCounter)
	}

	return outScope
}

// A WITH constructed within an EXPLAIN should not be hoisted above it, and so
// we need to denote a boundary which blocks them.
func (b *Builder) pushWithFrame() {
	b.cteStack = append(b.cteStack, []cteSource{})
}

// popWithFrame wraps the given scope's expression in the CTEs that have been
// queued up at this level.
func (b *Builder) popWithFrame(s *scope) {
	s.expr = b.flushCTEs(s.expr)
}

// flushCTEs adds With expressions on top of an expression.
func (b *Builder) flushCTEs(expr memo.RelExpr) memo.RelExpr {
	ctes := b.cteStack[len(b.cteStack)-1]
	b.cteStack = b.cteStack[:len(b.cteStack)-1]

	if len(ctes) == 0 {
		return expr
	}

	// Since later CTEs can refer to earlier ones, we want to add these in
	// reverse order.
	for i := len(ctes) - 1; i >= 0; i-- {
		expr = b.factory.ConstructWith(
			ctes[i].expr,
			expr,
			&memo.WithPrivate{
				ID:           ctes[i].id,
				Name:         string(ctes[i].name.Alias),
				OriginalExpr: ctes[i].originalExpr,
			},
		)
	}
	return expr
}

// buildSelectStmt builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmt(
	stmt tree.SelectStatement, locking lockingSpec, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, locking, desiredTypes, inScope)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, nil /* orderBy */, nil, locking, desiredTypes, inScope)

	case *tree.UnionClause:
		return b.buildUnionClause(stmt, desiredTypes, inScope)

	case *tree.ValuesClause:
		return b.buildValuesClause(stmt, desiredTypes, inScope)

	default:
		panic(errors.AssertionFailedf("unknown select statement type: %T", stmt))
	}
}

// buildSelect builds a set of memo groups that represent the given select
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, locking lockingSpec, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	wrapped := stmt.Select
	with := stmt.With
	orderBy := stmt.OrderBy
	limit := stmt.Limit
	locking.apply(stmt.Locking)

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		wrapped = stmt.Select
		if stmt.With != nil {
			if with != nil {
				// (WITH ... (WITH ...))
				// Currently we are unable to nest the scopes inside ParenSelect, so we
				// must refuse the syntax so that the query does not get invalid results.
				panic(unimplemented.NewWithIssue(
					24303, "multiple WITH clauses in parentheses",
				))
			}
			with = s.Select.With
		}
		if stmt.OrderBy != nil {
			if orderBy != nil {
				panic(pgerror.Newf(
					pgcode.Syntax, "multiple ORDER BY clauses not allowed",
				))
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if limit != nil {
				panic(pgerror.Newf(
					pgcode.Syntax, "multiple LIMIT clauses not allowed",
				))
			}
			limit = stmt.Limit
		}
		if stmt.Locking != nil {
			locking.apply(stmt.Locking)
		}
	}

	return b.processWiths(with, inScope, func(inScope *scope) *scope {
		return b.buildSelectStmtWithoutParens(
			wrapped, orderBy, limit, locking, desiredTypes, inScope,
		)
	})
}

// buildSelectStmtWithoutParens builds a set of memo groups that represent
// the given select statement components. The wrapped select statement can
// be any variant except ParenSelect, which should be unwrapped by callers.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmtWithoutParens(
	wrapped tree.SelectStatement,
	orderBy tree.OrderBy,
	limit *tree.Limit,
	locking lockingSpec,
	desiredTypes []*types.T,
	inScope *scope,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch t := wrapped.(type) {
	case *tree.ParenSelect:
		panic(errors.AssertionFailedf(
			"%T in buildSelectStmtWithoutParens", wrapped))

	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, limit, locking, desiredTypes, inScope)

	case *tree.UnionClause:
		b.rejectIfLocking(locking, "UNION/INTERSECT/EXCEPT")
		outScope = b.buildUnionClause(t, desiredTypes, inScope)

	case *tree.ValuesClause:
		b.rejectIfLocking(locking, "VALUES")
		outScope = b.buildValuesClause(t, desiredTypes, inScope)

	default:
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"unknown select statement: %T", wrapped))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			expr := &outScope.cols[i]
			col := b.addColumn(projectionsScope, "" /* alias */, expr, false)
			b.buildScalar(expr, outScope, projectionsScope, col, nil)
		}
		orderByScope := b.analyzeOrderBy(orderBy, outScope, projectionsScope, tree.RejectGenerators|tree.RejectAggregates|tree.RejectWindowApplications)
		b.buildOrderBy(outScope, projectionsScope, orderByScope)
		b.constructProjectForScope(outScope, projectionsScope)
		outScope = projectionsScope
	}

	if limit != nil {
		if limit.IsAutoLimit {
			b.factory.Memo().SetFlag(opt.HasAutoLimit)
		}
		b.buildLimit(limit, inScope, outScope)
	}

	return outScope
}

// DealHint deal with hint some things
func (b *Builder) DealHint(sel *tree.SelectClause) {
	HintModel := sel.CheckHintType()
	if !HintModel {
		b.StmtHint = sel.HintSet.GetStmtHint()
	} else {
		b.HintID = -1
	}

	b.evalCtx.HintStmtEmbed = false
	if b.StmtHint != nil && !HintModel && !b.evalCtx.HintReoptimize {
		b.HintID = -1
		b.evalCtx.HintID = -1
		b.evalCtx.HintStmtEmbed = true
		fromtables := make([]tree.TableExpr, len(sel.From.Tables))
		copy(fromtables, sel.From.Tables)
		currentDatabase := b.evalCtx.SessionData.Database
		currentSchema := ""
		if b.evalCtx.SessionData.SearchPath.GetPathArray() != nil {
			currentSchema = (b.evalCtx.SessionData.SearchPath.GetPathArray())[0]
		}
		b.OrderedTables = b.StmtHint.HintForest.GetHintInfoFromHintForest(fromtables, currentDatabase, currentSchema)
		b.factory.Memo().CheckHelper.GroupHint = b.StmtHint.HintForest.GetGroupHint()
	}
	// If there is an embedded hint, external hints are not considered.
	if !b.evalCtx.HintStmtEmbed && !HintModel && !b.evalCtx.HintReoptimize {
		// Mark external hint.
		b.evalCtx.HintID = b.HintID
		// Get the external Hint for this query.
		if sel.HintForest == nil {
			HintForest, err := tree.HintHex2go(b.HintInfo)
			if err != nil {
				panic(errors.AssertionFailedf("hint info %s cannot be resolved to hintforest", b.HintInfo))
			}
			sel.HintForest = HintForest
		}
		//get hint from HintForest
		fromtables := make([]tree.TableExpr, len(sel.From.Tables))
		copy(fromtables, sel.From.Tables)
		currentDatabase := b.evalCtx.SessionData.Database
		currentSchema := ""
		if b.evalCtx.SessionData.SearchPath.GetPathArray() != nil {
			currentSchema = (b.evalCtx.SessionData.SearchPath.GetPathArray())[0]
		}
		b.OrderedTables = sel.HintForest.GetHintInfoFromHintForest(fromtables, currentDatabase, currentSchema)
	}
}

// GetlogicalOrderbyPlan get order by string
func (b *Builder) GetlogicalOrderbyPlan(src *scope) {
	//get the access pattern of type SORTING from orderByScope
	if b.evalCtx.IsWLICollect && src != nil {
		for i := range src.cols {
			scopeCol, ok := src.cols[i].expr.(*scopeColumn)
			if !ok {
				continue
			}
			colUsage := opt.ColumnUsage{
				ID:          scopeCol.id,
				PredicateNo: 0,
				UsageType:   "SORTING",
				Ordering:    "ASC",
			}
			if src.cols[i].descending {
				colUsage.Ordering = "DESC"
			}
			b.ColsUsage = append(b.ColsUsage, colUsage)
		}
	}
}

// GetlogicalProjectionsPlan get projection string
func (b *Builder) GetlogicalProjectionsPlan(src *scope) {
	//get the access pattern of type SELECT from the projectionsScope
	if b.evalCtx.IsWLICollect && src != nil {
		for i := range src.cols {
			scopeCol, ok := src.cols[i].expr.(*scopeColumn)
			if !ok {
				continue
			}
			colUsage := opt.ColumnUsage{
				ID:          scopeCol.id,
				PredicateNo: 0,
				UsageType:   "SELECT",
				Ordering:    "",
			}
			b.ColsUsage = append(b.ColsUsage, colUsage)
		}
	}
}

// if instance table query, need to add condition of ptag
func (b *Builder) addInstanceTablePTag(sel *tree.SelectClause) {
	var left tree.Expr
	var andExpr tree.Expr
	if sel.Where != nil {
		left = sel.Where.Expr
	}
	for _, t := range b.InstanceTabNames {
		var unresolvedName *tree.UnresolvedName
		if len(t.Alias) > 0 {
			unresolvedName = tree.NewUnresolvedName(t.Alias, `pTag`)
		} else {
			unresolvedName = tree.NewUnresolvedName(t.DBName, t.CName, `pTag`)
		}
		strVal := tree.NewStrVal(t.CName)
		comparsionExpr := &tree.ComparisonExpr{Operator: tree.EQ, Left: unresolvedName, Right: strVal}
		if left != nil {
			andExpr = &tree.AndExpr{Left: left, Right: comparsionExpr}
			left = andExpr
		} else {
			andExpr = comparsionExpr
			left = comparsionExpr
		}
	}
	if sel.Where == nil {
		sel.Where = &tree.Where{Type: "WHERE"}
	}
	sel.Where.Expr = andExpr
}

// buildSelectClause builds a set of memo groups that represent the given
// select clause. We pass the entire select statement rather than just the
// select clause in order to handle ORDER BY scoping rules. ORDER BY can sort
// results using columns from the FROM/GROUP BY clause and/or from the
// projection list.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectClause(
	sel *tree.SelectClause,
	orderBy tree.OrderBy,
	limit *tree.Limit,
	locking lockingSpec,
	desiredTypes []*types.T,
	inScope *scope,
) (outScope *scope) {
	//needPushDown := ComplexQueryEnable.Get(&b.evalCtx.Settings.SV)
	b.PhysType = tree.Invalid
	b.DealHint(sel)

	fromScope := b.buildFrom(sel.From, locking, inScope)
	if len(b.InstanceTabNames) > 0 {
		b.addInstanceTablePTag(sel)
		b.InstanceTabNames = nil
	}

	b.buildOrderByFromInSope(fromScope, sel, orderBy)

	if b.factory.Metadata().CheckSingleTsTable() {
		fromScope.setAggExtendFlag(isSingleTsTable)
	}

	b.processWindowDefs(sel, fromScope)
	b.buildWhere(sel.Where, fromScope)

	projectionsScope := fromScope.replace()

	// This is where the magic happens. When this call reaches an aggregate
	// function that refers to variables in fromScope or an ancestor scope,
	// buildAggregateFunction is called which adds columns to the appropriate
	// aggInScope and aggOutScope.
	b.analyzeProjectionList(&sel.Exprs, desiredTypes, fromScope, projectionsScope)

	hasInterpolate := checkoutInterpolate(sel.GroupBy, fromScope)

	// Any aggregates in the HAVING, ORDER BY and DISTINCT ON clauses (if they
	// exist) will be added here.
	havingExpr := b.analyzeHaving(sel.Having, fromScope)
	orderByScope := b.analyzeOrderBy(orderBy, fromScope, projectionsScope, tree.RejectGenerators)
	distinctOnScope := b.analyzeDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)
	b.GetlogicalOrderbyPlan(orderByScope)
	b.GetlogicalProjectionsPlan(projectionsScope)
	var having opt.ScalarExpr
	needsAgg := b.needsAggregation(sel, fromScope)
	if needsAgg {
		// Grouping columns must be built before building the projection list, so
		// we can check that any column references that appear in the SELECT list
		// outside aggregate functions are present in the grouping list.
		b.buildGroupingColumns(sel, projectionsScope, fromScope)
		having = b.buildHaving(havingExpr, fromScope)
	}

	if fromScope.hasGapfill {
		checkoutGroupByGapfill(fromScope, sel, hasInterpolate)
	}
	b.buildProjectionList(fromScope, projectionsScope)
	b.buildOrderBy(fromScope, projectionsScope, orderByScope)
	b.buildDistinctOnArgs(fromScope, projectionsScope, distinctOnScope)
	b.buildProjectSet(fromScope)
	if needsAgg {
		// We must wait to build the aggregation until after the above block since
		// any SRFs found in the SELECT list will change the FROM scope (they
		// create an implicit lateral join).
		outScope = b.buildAggregation(having, fromScope)
	} else {
		outScope = fromScope
	}

	b.buildWindow(outScope, fromScope)
	b.validateLockingInFrom(sel, locking, fromScope)

	if b.factory.Memo().EnableOrderedScan() && !sel.Distinct && orderBy.Empty() && b.PhysType == tree.TS {
		b.checkOrderedTSScan(outScope.expr)
	}

	// Construct the projection.
	b.constructProjectForScope(outScope, projectionsScope)
	outScope = projectionsScope

	if sel.Distinct {
		if projectionsScope.distinctOnCols.Empty() {
			outScope.expr = b.constructDistinct(outScope)
		} else {
			outScope = b.buildDistinctOn(
				projectionsScope.distinctOnCols,
				outScope,
				false, /* nullsAreDistinct */
				false, /* errorOnDup */
			)
		}
	}

	// b.TableType used to be checked during insert.
	for k, v := range fromScope.TableType {
		b.TableType[k] = v
	}
	return outScope
}

// buildFrom builds a set of memo groups that represent the given FROM clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildFrom(from tree.From, locking lockingSpec, inScope *scope) (outScope *scope) {
	// The root AS OF clause is recognized and handled by the executor. The only
	// thing that must be done at this point is to ensure that if any timestamps
	// are specified, the root SELECT was an AS OF SYSTEM TIME and that the time
	// specified matches the one found at the root.
	if from.AsOf.Expr != nil {
		b.validateAsOf(from.AsOf)
	}

	if len(b.OrderedTables) > 0 {
		outScope = b.buildFromTables(b.OrderedTables, locking, inScope)
	} else if len(from.Tables) > 0 {
		outScope = b.buildFromTables(from.Tables, locking, inScope)
	} else {
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.factory.Metadata().NextUniqueID(),
		})
	}

	return outScope
}

// processWindowDefs validates that any window defs have unique names and adds
// them to the given scope.
func (b *Builder) processWindowDefs(sel *tree.SelectClause, fromScope *scope) {
	// Just do an O(n^2) loop since the number of window defs is likely small.
	for i := range sel.Window {
		for j := i + 1; j < len(sel.Window); j++ {
			if sel.Window[i].Name == sel.Window[j].Name {
				panic(pgerror.Newf(
					pgcode.Windowing,
					"window %q is already defined",
					sel.Window[i].Name,
				))
			}
		}
	}

	// Pass down the set of window definitions so that they can be referenced
	// elsewhere in the SELECT.
	fromScope.windowDefs = sel.Window
}

// buildWhere builds a set of memo groups that represent the given WHERE clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildWhere(where *tree.Where, inScope *scope) {
	if where == nil {
		return
	}

	filter := b.resolveAndBuildScalar(
		where.Expr,
		types.Bool,
		exprTypeWhere,
		tree.RejectGenerators|tree.RejectWindowApplications,
		inScope,
	)

	if filter != nil {
		// group window function can be only used in groupby
		if name, ok := memo.CheckGroupWindowExist(filter); ok {
			panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
		}
		// Wrap the filter in a FiltersOp.
		inScope.expr = b.factory.ConstructSelect(
			inScope.expr.(memo.RelExpr),
			memo.FiltersExpr{b.factory.ConstructFiltersItem(filter)},
		)
	}
}

// buildFromTables builds a series of InnerJoin expressions that together
// represent the given FROM tables.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTables(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	// Only do the analysis when the switch is on and the server starts with single node mode
	// Also only do for the following types of SQLs for multiple model processing.
	// In future, the restriction may be removed.
	if b.evalCtx.SessionData.MultiModelEnabled &&
		!opt.CheckOptMode(opt.TSQueryOptMode.Get(&b.evalCtx.Settings.SV), opt.OutsideInUseCBO) &&
		!b.evalCtx.StartDistributeMode &&
		(b.stmt.StatementTag() == "SELECT" || b.stmt.StatementTag() == "EXPLAIN" ||
			b.stmt.StatementTag() == "EXPLAIN ANALYZE (DEBUG)" || b.stmt.StatementTag() == "UNION") {
		tables = b.adjustTableSequenceForJoinBuild(tables, inScope)
	}
	// If there are any lateral data sources, we need to build the join tree
	// left-deep instead of right-deep.
	for i := range tables {
		if b.exprIsLateral(tables[i]) {
			telemetry.Inc(sqltelemetry.LateralJoinUseCounter)
			return b.buildFromWithLateral(tables, locking, inScope)
		}
	}
	return b.buildFromTablesRightDeep(tables, locking, inScope)
}

// adjustTableSequenceForJoinBuild adjusts the table sequence before building
// the right deep join tree for multiple model processing.
//
// The function adjust the sequence of tables involved in a multiple model query
// to ensure the timeseries table to be joined last in the join tree. If the query
// is not a multiple model query, the function returns the original table sequence.
// The function also check if the query is a multiple model query and set the query type
// in the memo accordingly.
func (b *Builder) adjustTableSequenceForJoinBuild(
	tables tree.TableExprs, inScope *scope,
) (outTables tree.TableExprs) {
	var relTables tree.TableExprs
	var tsTables tree.TableExprs
	mem := b.factory.Memo()
	//traverse the tables to check if the query is a multiple model query and all the tables
	//involved in the query are either relational or timeseries tables.
	for i := range tables {
		switch source := tables[i].(type) {
		case *tree.AliasedTableExpr:
			switch t := source.Expr.(type) {
			case *tree.TableName:
				if cte := inScope.lookupCTE(t); cte != nil {
					relTables = append(relTables, tables[i])
				} else {
					ds, _ := b.lookupDataSource(t)
					switch dstype := ds.(type) {
					case cat.Table:
						switch dstype.GetTableType() {
						case tree.RelationalTable:
							// if the table is relational table, then check if the query already has at least one timeseries
							// table in the query. If not, then set the query type in the memo to "REL_ONLY". If the query already
							// has a timeseries table, then set the query type in the memo to MultiModel.
							if mem.QueryType == memo.Unset || mem.QueryType == memo.RelOnly {
								mem.QueryType = memo.RelOnly
							} else if mem.QueryType == memo.TSOnly {
								mem.QueryType = memo.MultiModel
							}
							relTables = append(relTables, tables[i])
						case tree.TimeseriesTable, tree.TemplateTable:
							//if the table is timeseries table, then check if the query already has at least one relational
							//table in the query. If not, then set the query type in the memo to "TS_ONLY". If the query already
							//has a relational table, then set the query type in the memo to MultiModel.
							if mem.QueryType == memo.Unset || mem.QueryType == memo.TSOnly {
								mem.QueryType = memo.TSOnly
							} else if mem.QueryType == memo.RelOnly {
								mem.QueryType = memo.MultiModel
							}
							//adjust the table sequence to ensure the timeseries table is joined last in the join tree.
							tsTables = append(tsTables, tables[i])
						case tree.InstanceTable:
							tsTables = append(tsTables, tables[i])
						}
					default:
						relTables = append(relTables, tables[i])
					}
				}
			case *tree.Subquery:
				// if the table is table expression, then check if the query already has at least one timeseries
				// table in the query. If not, then set the query type in the memo to "REL_ONLY". If the query already
				// has a timeseries table, then set the query type in the memo to MultiModel.
				if mem.QueryType == memo.Unset || mem.QueryType == memo.RelOnly {
					mem.QueryType = memo.RelOnly
				} else if mem.QueryType == memo.TSOnly {
					mem.QueryType = memo.MultiModel
				}
				relTables = append(relTables, tables[i])
			default:
				relTables = append(relTables, tables[i])
			}
		default:
			relTables = append(relTables, tables[i])
		}
	}

	//if the query is a multiple model query, then return the new table sequence;
	//otherwise return the original table sequence.
	if mem.QueryType == memo.MultiModel {
		mem.SetFlag(opt.FinishOptInsideOut)
		tsTables = append(tsTables, relTables...)
		return tsTables
	}
	return tables
}

// buildFromTablesRightDeep recursively builds a series of InnerJoin
// expressions that join together the given FROM tables. The tables are joined
// in the reverse order that they appear in the list, with the innermost join
// involving the tables at the end of the list. For example:
//
//	SELECT * FROM a,b,c
//
// is joined like:
//
//	SELECT * FROM a JOIN (b JOIN c ON true) ON true
//
// This ordering is guaranteed for queries not involving lateral joins for the
// time being, to ensure we don't break any queries which have been
// hand-optimized.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTablesRightDeep(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, locking, inScope, "")
	// Recursively build table join.
	tables = tables[1:]
	if len(tables) == 0 {
		return outScope
	}
	tableScope := b.buildFromTablesRightDeep(tables, locking, inScope)
	// Check that the same table name is not used multiple times.
	b.validateJoinTableNames(outScope, tableScope)

	outScope.appendColumnsFromScope(tableScope)
	left := outScope.expr.(memo.RelExpr)
	right := tableScope.expr.(memo.RelExpr)

	joinPrivate := &memo.JoinPrivate{}
	joinPrivate.HintInfo.HintIndex = -1
	//Check whether the left child of the current join contains leading table
	joinPrivate.HintInfo.LeadingTable = HasLeadingTable(left)
	if joinPrivate.HintInfo.LeadingTable == true {
		joinPrivate.HintInfo.FromHintTree = true
	}

	outScope.expr = b.factory.ConstructInnerJoin(left, right, memo.TrueFilter, joinPrivate)
	b.mergeTableType(outScope, tableScope, outScope)
	return outScope
}

// exprIsLateral returns whether the table expression should have access to the
// scope of the tables to the left of it.
func (b *Builder) exprIsLateral(t tree.TableExpr) bool {
	ate, ok := t.(*tree.AliasedTableExpr)
	if !ok {
		return false
	}
	// Expressions which explicitly use the LATERAL keyword are lateral.
	if ate.Lateral {
		return true
	}
	// SRFs are always lateral.
	_, ok = ate.Expr.(*tree.RowsFromExpr)
	return ok
}

// buildFromWithLateral builds a FROM clause in the case where it contains a
// LATERAL table.  This differs from buildFromTablesRightDeep because the
// semantics of LATERAL require that the join tree is built left-deep (from
// left-to-right) rather than right-deep (from right-to-left) which we do
// typically for perf backwards-compatibility.
//
//	SELECT * FROM a, b, c
//
//	buildFromTablesRightDeep: a JOIN (b JOIN c)
//	buildFromWithLateral:     (a JOIN b) JOIN c
func (b *Builder) buildFromWithLateral(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, locking, inScope, "")
	for i := 1; i < len(tables); i++ {
		scope := inScope
		// Lateral expressions need to be able to refer to the expressions that
		// have been built already.
		if b.exprIsLateral(tables[i]) {
			scope = outScope
			scope.context = exprTypeLateralJoin
		}
		tableScope := b.buildDataSource(tables[i], nil /* indexFlags */, locking, scope, "")

		// Check that the same table name is not used multiple times.
		b.validateJoinTableNames(outScope, tableScope)

		outScope.appendColumnsFromScope(tableScope)

		left := outScope.expr.(memo.RelExpr)
		right := tableScope.expr.(memo.RelExpr)
		outScope.expr = b.factory.ConstructInnerJoinApply(left, right, memo.TrueFilter, memo.EmptyJoinPrivate)
	}

	return outScope
}

// validateAsOf ensures that any AS OF SYSTEM TIME timestamp is consistent with
// that of the root statement.
func (b *Builder) validateAsOf(asOf tree.AsOfClause) {
	ts, err := tree.EvalAsOfTimestamp(asOf, b.semaCtx, b.evalCtx)
	if err != nil {
		panic(err)
	}

	if b.semaCtx.AsOfTimestamp == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"AS OF SYSTEM TIME must be provided on a top-level statement"))
	}

	if *b.semaCtx.AsOfTimestamp != ts {
		panic(unimplementedWithIssueDetailf(35712, "",
			"cannot specify AS OF SYSTEM TIME with different timestamps, first time: %v, second time:%v", *b.semaCtx.AsOfTimestamp, ts))
	}
}

// validateLockingInFrom checks for operations that are not supported with FOR
// [KEY] UPDATE/SHARE. If a locking clause was specified with the select and an
// incompatible operation is in use, a locking error is raised.
func (b *Builder) validateLockingInFrom(
	sel *tree.SelectClause, locking lockingSpec, fromScope *scope,
) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		return
	}

	switch {
	case sel.Distinct:
		b.raiseLockingContextError(locking, "DISTINCT clause")

	case sel.GroupBy != nil:
		b.raiseLockingContextError(locking, "GROUP BY clause")

	case sel.Having != nil:
		b.raiseLockingContextError(locking, "HAVING clause")

	case fromScope.groupby != nil && fromScope.groupby.hasAggregates():
		b.raiseLockingContextError(locking, "aggregate functions")

	case len(fromScope.windows) != 0:
		b.raiseLockingContextError(locking, "window functions")

	case len(fromScope.srfs) != 0:
		b.raiseLockingContextError(locking, "set-returning functions in the target list")
	}

	for _, li := range locking {
		// Validate locking strength.
		switch li.Strength {
		case tree.ForNone:
			// AST nodes should not be created with this locking strength.
			panic(errors.AssertionFailedf("locking item without strength"))
		case tree.ForUpdate, tree.ForNoKeyUpdate, tree.ForShare, tree.ForKeyShare:
			// CockroachDB treats all the FOR LOCKED modes as no-ops. Since all
			// transactions are serializable in CockroachDB, clients can't observe
			// regardless of whether FOR UPDATE (or any of the other weaker modes) actually
			// created a lock. This behavior may improve as the transaction model gains
			// more capabilities.
		default:
			panic(errors.AssertionFailedf("unknown locking strength"))
		}

		// Validating locking wait policy.
		switch li.WaitPolicy {
		case tree.LockWaitBlock:
			// Default.
		case tree.LockWaitSkip:
			panic(unimplementedWithIssueDetailf(40476, "",
				"SKIP LOCKED lock wait policy is not supported"))
		case tree.LockWaitError:
			panic(unimplementedWithIssueDetailf(40476, "",
				"NOWAIT lock wait policy is not supported"))
		default:
			panic(errors.AssertionFailedf("unknown locking wait policy: %s", li.WaitPolicy))
		}

		// Validate locking targets by checking that all targets are well-formed
		// and all point to real relations present in the FROM clause.
		for _, target := range li.Targets {
			// Insist on unqualified alias names here. We could probably do
			// something smarter, but it's better to just mirror Postgres
			// exactly. See transformLockingClause in Postgres' source.
			if target.CatalogName != "" || target.SchemaName != "" {
				panic(pgerror.Newf(pgcode.Syntax,
					"%s must specify unqualified relation names", li.Strength))
			}

			// Search for the target in fromScope. If a target is missing from
			// the scope then raise an error. This will end up looping over all
			// columns in scope for each of the locking targets. We could use a
			// more efficient data structure (e.g. a hash map of relation names)
			// to improve the time complexity here, but we expect the number of
			// columns to be small enough that doing so is likely not worth it.
			found := false
			for _, col := range fromScope.cols {
				if target.TableName == col.table.TableName {
					found = true
					break
				}
			}
			if !found {
				panic(pgerror.Newf(
					pgcode.UndefinedTable,
					"relation %q in %s clause not found in FROM clause",
					target.TableName, li.Strength,
				))
			}
		}
	}
}

// rejectIfLocking raises a locking error if a locking clause was specified.
func (b *Builder) rejectIfLocking(locking lockingSpec, context string) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		return
	}
	b.raiseLockingContextError(locking, context)
}

// raiseLockingContextError raises an error indicating that a row-level locking
// clause is not permitted in the specified context. locking.isSet() must be true.
func (b *Builder) raiseLockingContextError(locking lockingSpec, context string) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported,
		"%s is not allowed with %s", locking.get().Strength, context))
}

// HasLeadingTable returns whether a leading table exists
func HasLeadingTable(left memo.RelExpr) bool {
	switch expr := left.(type) {
	case *memo.ScanExpr:
		return expr.Flags.LeadingTable

	case *memo.InnerJoinExpr:
		return expr.HintInfo.LeadingTable

	default:
		return false
	}
}

// mergeTableType merge the tabletype of childs.
// lScope,rScope is the info of childs.
// outScope is the result.
func (b *Builder) mergeTableType(lScope, rScope, outScope *scope) {
	if lScope.TableType != nil {
		outScope.TableType = lScope.TableType
		for k, v := range rScope.TableType {
			if _, ok := outScope.TableType[k]; ok {
				outScope.TableType[k] = outScope.TableType[k] + 1
			} else {
				outScope.TableType[k] = v
			}
		}
	} else if rScope != nil {
		outScope.TableType = rScope.TableType
	}

	if rScope != nil {
		outScope.HasMultiTable = true
	}
}
