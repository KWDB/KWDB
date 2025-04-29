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
	"sort"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// excludedTableName is the name of a special Upsert data source. When a row
// cannot be inserted due to a conflict, the "excluded" data source contains
// that row, so that its columns can be referenced in the conflict clause:
//
//	INSERT INTO ab VALUES (1, 2) ON CONFLICT (a) DO UPDATE b=excluded.b+1
//
// It is located in the special kwdb_internal schema so that it never overlaps
// with user data sources.
var excludedTableName tree.TableName

func init() {
	// Clear explicit schema and catalog so that they're not printed in error
	// messages.
	excludedTableName = tree.MakeTableNameWithSchema("", "kwdb_internal", "excluded")
	excludedTableName.ExplicitSchema = false
	excludedTableName.ExplicitCatalog = false
}

// buildInsert builds a memo group for an InsertOp or UpsertOp expression. To
// begin, an input expression is constructed which outputs these columns to
// insert into the target table:
//
//  1. Columns explicitly specified by the user in SELECT or VALUES expression.
//
//  2. Columns not specified by the user, but having a default value declared
//     in schema (or being nullable).
//
//  3. Computed columns.
//
//  4. Mutation columns which are being added or dropped by an online schema
//     change.
//
// buildInsert starts by constructing the input expression, and then wraps it
// with Project operators which add default, computed, and mutation columns. The
// final input expression will project values for all columns in the target
// table. For example, if this is the schema and INSERT statement:
//
//	CREATE TABLE abcd (
//	  a INT PRIMARY KEY,
//	  b INT,
//	  c INT DEFAULT(10),
//	  d INT AS (b+c) STORED
//	)
//	INSERT INTO abcd (a) VALUES (1)
//
// Then an input expression equivalent to this would be built:
//
//	SELECT ins_a, ins_b, ins_c, ins_b + ins_c AS ins_d
//	FROM (VALUES (1, NULL, 10)) AS t(ins_a, ins_b, ins_c)
//
// If an ON CONFLICT clause is present (or if it was an UPSERT statement), then
// additional columns are added to the input expression:
//
//  1. Columns containing existing values fetched from the target table and
//     used to detect conflicts and to formulate the key/value update commands.
//
//  2. Columns containing updated values to set when a conflict is detected, as
//     specified by the user.
//
//  3. Computed columns which will be updated when a conflict is detected and
//     that are dependent on one or more updated columns.
//
// A LEFT OUTER JOIN associates each row to insert with the corresponding
// existing row (#1 above). If the row does not exist, then the existing columns
// will be null-extended, per the semantics of LEFT OUTER JOIN. This behavior
// allows the execution engine to test whether a given insert row conflicts with
// an existing row in the table. One of the existing columns that is declared as
// NOT NULL in the table schema is designated as a "canary column". When the
// canary column is null after the join step, then it must have been null-
// extended by the LEFT OUTER JOIN. Therefore, there is no existing row, and no
// conflict. If the canary column is not null, then there is an existing row,
// and a conflict.
//
// The canary column is used in CASE statements to toggle between the insert and
// update values for each row. If there is no conflict, the insert value is
// used. Otherwise, the update value is used (or the existing value if there is
// no update value for that column).
//
// In addition, upsert cases have another complication that arises from the
// requirement that no mutation statement updates the same row more than once.
// Primary key violations prevent INSERT statements from inserting the same row
// twice. DELETE statements do not encounter a problem because their input never
// contains duplicate rows. And UPDATE statements are equivalent to DELETE
// followed by an INSERT, so they're safe as well. By contrast, UPSERT and
// INSERT..ON CONFLICT statements can have duplicate input rows that trigger
// updates of the same row after insertion conflicts.
//
// Detecting (and raising an error) or ignoring (in case of DO NOTHING)
// duplicate rows requires wrapping the input with one or more DISTINCT ON
// operators that ensure the input is distinct on at least one unique index.
// Because the input is distinct on a unique index of the target table, the
// statement will never attempt to update the same row twice.
//
// Putting it all together, if this is the schema and INSERT..ON CONFLICT
// statement:
//
//	CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//	INSERT INTO abc VALUES (1, 2), (1, 3) ON CONFLICT (a) DO UPDATE SET b=10
//
// Then an input expression roughly equivalent to this would be built (note that
// the DISTINCT ON is really the UpsertDistinctOn operator, which behaves a bit
// differently than the DistinctOn operator):
//
//	SELECT
//	  fetch_a,
//	  fetch_b,
//	  fetch_c,
//	  CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//	  CASE WHEN fetch_a IS NULL ins_b ELSE 10 END AS ups_b,
//	  CASE WHEN fetch_a IS NULL ins_c ELSE fetch_c END AS ups_c,
//	FROM (
//	  SELECT DISTINCT ON (ins_a) *
//	  FROM (VALUES (1, 2, NULL), (1, 3, NULL)) AS ins(ins_a, ins_b, ins_c)
//	)
//	LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//	ON ins_a = fetch_a
//
// Here, the fetch_a column has been designated as the canary column, since it
// is NOT NULL in the schema. It is used as the CASE condition to decide between
// the insert and update values for each row. The CASE expressions will often
// prevent the unnecessary evaluation of the update expression in the case where
// an insertion needs to occur. In addition, it simplifies logical property
// calculation, since a 1:1 mapping to each target table column from a
// corresponding input column is maintained.
//
// If the ON CONFLICT clause contains a DO NOTHING clause, then each UNIQUE
// index on the target table requires its own DISTINCT ON to ensure that the
// input has no duplicates, and its own LEFT OUTER JOIN to check whether a
// conflict exists. For example:
//
//	CREATE TABLE ab (a INT PRIMARY KEY, b INT)
//	INSERT INTO ab (a, b) VALUES (1, 2), (1, 3) ON CONFLICT DO NOTHING
//
// Then an input expression roughly equivalent to this would be built:
//
//	SELECT x, y
//	FROM (SELECT DISTINCT ON (x) * FROM (VALUES (1, 2), (1, 3))) AS input(x, y)
//	LEFT OUTER JOIN ab
//	ON input.x = ab.a
//	WHERE ab.a IS NULL
//
// Note that an ordered input to the INSERT does not provide any guarantee about
// the order in which mutations are applied, or the order of any returned rows
// (i.e. it won't become a physical property required of the Insert or Upsert
// operator). Not propagating input orderings avoids an extra sort when the
// ON CONFLICT clause is present, since it joins a new set of rows to the input
// and thereby scrambles the input ordering.
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(ins.Table, privilege.INSERT)
	// Forward to buildTSInsert if tab is a ts table.
	tblTyp := tab.GetTableType()
	// insert into template table error
	if tblTyp == tree.TemplateTable && !opt.CheckTsProperty(b.TSInfo.TSProp, TSPropInsertCreateTable) {
		panic(pgerror.Newf(pgcode.FeatureNotSupported, "cannot insert into a TEMPLATE table, table name: %v", tab.Name()))
	}

	if ins.IsNoSchema {
		// INSERT NO SCHEMA is a special syntax.
		// When data is inserted using this syntax,
		// columns that do not exist are automatically added
		ins.Columns = ins.NoSchemaColumns.GetNameList()
		added, err := b.maybeAddNonExistsColumn(tab, ins, alias)
		if err != nil {
			panic(err)
		}
		if added {
			// Columns were added using InternalExecutor.
			// So we need to reset the txn so that it can resolve the new columns.
			retryOpt := retry.Options{
				InitialBackoff: 50 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				Multiplier:     2,
				MaxRetries:     60, // about 5 minutes
			}
			for r := retry.Start(retryOpt); r.Next(); {
				allGet := true
				//b.catalog.ResetTxn(b.ctx)
				clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
				b.evalCtx.Txn.SetFixedTimestamp(b.ctx, clock.Now())
				tab, depName, alias, refColumns = b.resolveTableForMutation(ins.Table, privilege.INSERT)
				cols := getColumnDescs(tab)
				for idx := range ins.NoSchemaColumns {
					colDef := ins.NoSchemaColumns[idx]
					_, err = getTSColumnByName(colDef.Name, cols)
					if err == nil {
						continue
					} else {
						allGet = false
						break
					}
				}
				if allGet {
					break
				} else {
					b.catalog.ReleaseTables(b.ctx)
				}
			}
		}
	}
	// handle INSERT INTO ts_table VALUES...
	if _, ok := ins.Rows.Select.(*tree.ValuesClause); ok {
		switch tblTyp {
		case tree.TimeseriesTable:
			sTableID := b.factory.Metadata().AddTable(tab, ins.Table.(*tree.TableName))
			return b.buildTSInsert(inScope, tab, ins, sTableID, nil)
		case tree.TemplateTable:
			// Insert automatic table creation
			var ct *tree.CreateTable
			ct = ins.Table.(*tree.InFlightCreateTableExpr).TableDef
			sTableID := b.factory.Metadata().AddTable(tab, &alias)
			return b.buildTSInsert(inScope, tab, ins, sTableID, ct)
		case tree.InstanceTable:
			sTableID := b.factory.Metadata().AddTable(tab, &alias)
			return b.buildTSInsert(inScope, tab, ins, sTableID, nil)
		default:
			// do nothing
		}
	}

	// It is possible to insert into specific columns using table reference
	// syntax:
	// INSERT INTO [<table_id>(<col1_id>,<col2_id>) AS <alias>] ...
	// is equivalent to
	// INSERT INTO [<table_id> AS <alias>] (col1_name, col2_name) ...
	if refColumns != nil {
		if len(ins.Columns) != 0 {
			panic(pgerror.New(pgcode.Syntax,
				"cannot specify both a list of column IDs and a list of column names"))
		}

		ins.Columns = make(tree.NameList, len(refColumns))
		for i, ord := range cat.ConvertColumnIDsToOrdinals(tab, refColumns) {
			ins.Columns[i] = tab.Column(ord).ColName()
		}
	}

	if ins.OnConflict != nil {
		// UPSERT and INDEX ON CONFLICT will read from the table to check for
		// duplicates.
		b.checkPrivilege(depName, tab, privilege.SELECT)

		if !ins.OnConflict.DoNothing {
			// UPSERT and INDEX ON CONFLICT DO UPDATE may modify rows if the
			// DO NOTHING clause is not present.
			b.checkPrivilege(depName, tab, privilege.UPDATE)
		}
	}

	var mb mutationBuilder
	if ins.OnConflict != nil && ins.OnConflict.IsUpsertAlias() {
		mb.init(b, "upsert", tab, alias)
	} else {
		mb.init(b, "insert", tab, alias)
	}

	// Compute target columns in two cases:
	//
	//   1. When explicitly specified by name:
	//
	//        INSERT INTO <table> (<col1>, <col2>, ...) ...
	//
	//   2. When implicitly targeted by VALUES expression:
	//
	//        INSERT INTO <table> VALUES (...)
	//
	// Target columns for other cases can't be derived until the input expression
	// is built, at which time the number of input columns is known. At the same
	// time, the input expression cannot be built until DEFAULT expressions are
	// replaced and named target columns are known. So this step must come first.
	if len(ins.Columns) != 0 {
		// Target columns are explicitly specified by name.
		mb.addTargetNamedColsForInsert(ins.Columns)
	} else {
		values := mb.extractValuesInput(ins.Rows)
		if values != nil {
			// Target columns are implicitly targeted by VALUES expression in the
			// same order they appear in the target table schema.
			mb.addTargetTableColsForInsert(len(values.Rows[0]))
		}
	}

	// Build the input rows expression if one was specified:
	//
	//   INSERT INTO <table> VALUES ...
	//   INSERT INTO <table> SELECT ... FROM ...
	//
	// or initialize an empty input if inserting default values (default values
	// will be added later):
	//
	//   INSERT INTO <table> DEFAULT VALUES
	//
	if !ins.DefaultValues() {
		// Replace any DEFAULT expressions in the VALUES clause, if a VALUES clause
		// exists:
		//
		//   INSERT INTO <table> VALUES (..., DEFAULT, ...)
		//
		rows := mb.replaceDefaultExprs(ins.Rows)

		mb.buildInputForInsert(inScope, rows)
	} else {
		mb.buildInputForInsert(inScope, nil /* rows */)
	}

	// get num of relational table and num of time series table from select sql
	selectRelationalTableNum := 0
	selectTimeSeriesTableNum := 0
	for k := range mb.b.TableType {
		if k == tree.RelationalTable {
			selectRelationalTableNum++
		} else {
			selectTimeSeriesTableNum++
		}
	}

	// Add default columns that were not explicitly specified by name or
	// implicitly targeted by input columns. Also add any computed columns. In
	// both cases, include columns undergoing mutations in the write-only state.
	mb.addSynthesizedColsForInsert()

	var returning tree.ReturningExprs
	if resultsNeeded(ins.Returning) {
		returning = *ins.Returning.(*tree.ReturningExprs)
	}

	switch {
	// Case 1: Simple INSERT statement.
	case ins.OnConflict == nil:
		// Build the final insert statement, including any returned expressions.
		if mb.tab.GetTableType() != tree.RelationalTable {
			mb.buildTSInsertSelect(selectRelationalTableNum, selectTimeSeriesTableNum, ins, b)
			// case: ts insert select with order by clause.
			if mb.outScope.ordering != nil {
				insert, ok := mb.outScope.expr.(*memo.TSInsertSelectExpr)
				if ok {
					insert.TSInsertSelectPrivate.NeedProvideCols = true
				}
			}
		} else {
			mb.buildInsert(returning)
		}

	// Case 2: INSERT..ON CONFLICT DO NOTHING.
	case ins.OnConflict.DoNothing:
		// Wrap the input in one LEFT OUTER JOIN per UNIQUE index, and filter out
		// rows that have conflicts. See the buildInputForDoNothing comment for
		// more details.
		conflictOrds := mb.mapColumnNamesToOrdinals(ins.OnConflict.Columns)
		mb.buildInputForDoNothing(inScope, conflictOrds)

		// Since buildInputForDoNothing filters out rows with conflicts, always
		// insert rows that are not filtered.
		mb.buildInsert(returning)

	// Case 3: UPSERT statement.
	case ins.OnConflict.IsUpsertAlias():
		// Add columns which will be updated by the Upsert when a conflict occurs.
		// These are derived from the insert columns.
		mb.setUpsertCols(ins.Columns)

		// Check whether the existing rows need to be fetched in order to detect
		// conflicts.
		if mb.needExistingRows() {
			// Left-join each input row to the target table, using conflict columns
			// derived from the primary index as the join condition.
			primaryOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
			mb.buildInputForUpsert(inScope, primaryOrds, nil /* whereClause */)

			// Add additional columns for computed expressions that may depend on any
			// updated columns, as well as mutation columns with default values.
			mb.addSynthesizedColsForUpdate()
		}

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)

	// Case 4: INSERT..ON CONFLICT..DO UPDATE statement.
	default:
		// Left-join each input row to the target table, using the conflict columns
		// as the join condition.
		conflictOrds := mb.mapColumnNamesToOrdinals(ins.OnConflict.Columns)
		mb.buildInputForUpsert(inScope, conflictOrds, ins.OnConflict.Where)

		// Derive the columns that will be updated from the SET expressions.
		mb.addTargetColsForUpdate(ins.OnConflict.Exprs)

		// Build each of the SET expressions.
		mb.addUpdateCols(ins.OnConflict.Exprs)

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)
	}

	return mb.outScope
}

/* buildColsForTsInsert
 * @Description：build columns of insert by input columns or all columns of table;
 * @In ins: represents an INSERT statement;
 * @In cols: insert columns
 * @In table: insert table
 * @In needMap: whether return ColsMap
 * @Return 1: the map of column id to the column index
 * @Return 2: list of column id
 * @Return 3: list of column index
 */
func buildColsForTsInsert(
	ins *tree.Insert, cols []*sqlbase.ColumnDescriptor, table cat.Table, needMap bool,
) (opt.ColsMap, opt.ColList, opt.ColIdxs) {
	columnMap := make(opt.ColsMap, 0)
	var resCols opt.ColList
	var colIdxs opt.ColIdxs

	if ins.Columns != nil {
		inColName := make(map[tree.Name]bool)
		// constructs the map of the metadata column to the input value based on the user-specified column name
		for idx := range ins.Columns {
			targetCol, err := getTSColumnByName(ins.Columns[idx], cols)
			if err != nil {
				panic(err)
			}
			// instance table does not support specifying tag column
			if targetCol.IsTagCol() && (table.GetTableType() == tree.InstanceTable ||
				table.GetTableType() == tree.TemplateTable) {
				panic(pgerror.Newf(pgcode.FeatureNotSupported, "cannot insert tag column: \"%s\" for INSTANCE table", targetCol.Name))
			}
			// check if duplicate columns are input.
			if _, ok := inColName[ins.Columns[idx]]; !ok {
				inColName[ins.Columns[idx]] = true
			} else {
				panic(pgerror.Newf(pgcode.DuplicateColumn, "multiple assignments to the same column \"%s\"", string(ins.Columns[idx])))
			}
			if needMap {
				columnMap[int(targetCol.ID)] = idx
			} else {
				resCols = append(resCols, opt.ColumnID(targetCol.ID))
				colIdxs = append(colIdxs, idx)
			}
		}
	} else {
		for i := range cols {
			if cols[i].IsTagCol() && (table.GetTableType() == tree.InstanceTable ||
				table.GetTableType() == tree.TemplateTable) {
				break
			}
			if needMap {
				columnMap[int(cols[i].ID)] = i
			} else {
				resCols = append(resCols, opt.ColumnID(cols[i].ID))
				colIdxs = append(colIdxs, i)
			}
		}
	}

	return columnMap, resCols, colIdxs
}

/* getChildIDAndName
 * @Description：get id and name of child table;
 * @In table: insert table
 * @In b: optbuilder.Builder
 * @Return cTableID: child table id
 * @Return cName: child table name
 */
func (mb *mutationBuilder) getChildIDAndName(
	table tree.TableExpr, b *Builder,
) (cTableID opt.TableID, cName string) {
	switch tn := table.(type) {
	case *tree.TableName:
		cn, found, err := sqlbase.ResolveInstanceName(b.ctx, b.evalCtx.Txn, tn.Schema(), tn.Table())
		if err != nil {
			panic(err)
		}
		if !found {
			panic(sqlbase.NewUndefinedTableError(mb.alias.Table()))
		}
		cName = tn.Table()
		cTableID = opt.TableID(cn.InstTableID)
	case *tree.InFlightCreateTableExpr:
		cTableID = mb.tabID
		cName = tn.TableDef.Table.Table()
	default:
		panic(pgerror.Newf(pgcode.WrongObjectType, "unsupported TableExpr type, table name: %v", cName))
	}
	return
}

// TSInsertSelectLimitEnable limit data of relational table insert into time series table.
var TSInsertSelectLimitEnable = settings.RegisterPublicBoolSetting(
	"sql.ts_insert_select_limit.enabled", "data of time series can insert into relational table if TSInsertSelectLimitEnable is true", false,
)

/* buildTSInsertSelect
 * @Description：build tsInsertSelectExpr;
 * @In rNum: num of relational table in select sql;
 * @In tNum: num of time series table in select sql;
 * @In ins: represents an INSERT statement;
 * @In b: optbuilder.Builder
 * @Return: true/false
 */
func (mb *mutationBuilder) buildTSInsertSelect(rNum, tNum int, ins *tree.Insert, b *Builder) bool {
	// if sql is prepare, cannot use memo cache
	if mb.b.factory.CheckFlag(opt.IsPrepare) {
		mb.b.DisableMemoReuse = true
	}
	table := mb.tab
	switch table.GetTableType() {
	case tree.TimeseriesTable, tree.InstanceTable:
		// select relational data or cross-module data insert into ts table
		if !TSInsertSelectLimitEnable.Get(&b.evalCtx.Settings.SV) && rNum > 0 && tNum == 0 {
			panic(pgerror.New(pgcode.Warning, "insert relational data into time series table is not supported"))
		}
	}

	colCount := table.ColumnCount()
	cols := make([]*sqlbase.ColumnDescriptor, 0)
	for i := 0; i < colCount; i++ {
		col := table.Column(i).(*sqlbase.ColumnDescriptor)
		cols = append(cols, col)
	}

	// user specifies insert field.
	_, resCols, colIdxs := buildColsForTsInsert(ins, cols, table, false)

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols()
	mb.buildFKChecksForInsert()

	var cTableID opt.TableID
	var cName string
	if mb.tab.GetTableType() == tree.InstanceTable || mb.tab.GetTableType() == tree.TemplateTable {
		if t, ok := ins.Table.(*tree.AliasedTableExpr); ok {
			cTableID, cName = mb.getChildIDAndName(t.Expr, b)
		} else {
			cTableID, cName = mb.getChildIDAndName(ins.Table, b)
		}
	}

	private := memo.TSInsertSelectPrivate{STable: mb.tabID, CTable: cTableID, CName: cName, Cols: resCols, ColIdxs: colIdxs}
	// construct memo.tsInsertSelectExpr.
	mb.outScope.expr = b.factory.ConstructTSInsertSelect(mb.outScope.expr, &private)
	return true
}

// needExistingRows returns true if an Upsert statement needs to fetch existing
// rows in order to detect conflicts. In some cases, it is not necessary to
// fetch existing rows, and then the KV Put operation can be used to blindly
// insert a new record or overwrite an existing record. This is possible when:
//
//  1. There are no secondary indexes. Existing values are needed to delete
//     secondary index rows when the update causes them to move.
//  2. All non-key columns (including mutation columns) have insert and update
//     values specified for them.
//  3. Each update value is the same as the corresponding insert value.
//
// TODO(radu): once FKs no longer require indexes, this function will have to
// take FKs into account explicitly.
//
// TODO(andyk): The fast path is currently only enabled when the UPSERT alias
// is explicitly selected by the user. It's possible to fast path some queries
// of the form INSERT ... ON CONFLICT, but the utility is low and there are lots
// of edge cases (that caused real correctness bugs #13437 #13962). As a result,
// this support was removed and needs to re-enabled. See #14482.
func (mb *mutationBuilder) needExistingRows() bool {
	if mb.tab.DeletableIndexCount() > 1 {
		return true
	}

	// Key columns are never updated and are assumed to be the same as the insert
	// values.
	// TODO(andyk): This is not true in the case of composite key encodings. See
	// issue #34518.
	keyOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		if keyOrds.Contains(i) {
			// #1: Don't consider key columns.
			continue
		}
		insertColID := mb.insertColID(i)
		if insertColID == 0 {
			// #2: Non-key column does not have insert value specified.
			return true
		}
		if insertColID != mb.scopeOrdToColID(mb.updateOrds[i]) {
			// #3: Update value is not same as corresponding insert value.
			return true
		}
	}
	return false
}

// addTargetNamedColsForInsert adds a list of user-specified column names to the
// list of table columns that are the target of the Insert operation.
func (mb *mutationBuilder) addTargetNamedColsForInsert(names tree.NameList) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetNamedColsForInsert cannot be called more than once"))
	}

	// Add target table columns by the names specified in the Insert statement.
	mb.addTargetColsByName(names)

	// Ensure that primary key columns are in the target column list, or that
	// they have default values.
	mb.checkPrimaryKeyForInsert()

	// Ensure that foreign keys columns are in the target column list, or that
	// they have default values.
	mb.checkForeignKeysForInsert()
}

// checkPrimaryKeyForInsert ensures that the columns of the primary key are
// either assigned values by the INSERT statement, or else have default/computed
// values. If neither condition is true, checkPrimaryKeyForInsert raises an
// error.
func (mb *mutationBuilder) checkPrimaryKeyForInsert() {
	primary := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, primary.KeyColumnCount(); i < n; i++ {
		col := primary.Column(i)
		if col.HasDefault() || col.IsComputed() {
			// The column has a default or computed value.
			continue
		}

		colID := mb.tabID.ColumnID(col.Ordinal)
		if mb.targetColSet.Contains(colID) {
			// The column is explicitly specified in the target name list.
			continue
		}

		panic(pgerror.Newf(pgcode.InvalidForeignKey,
			"missing %q primary key column", col.ColName()))
	}
}

// checkForeignKeysForInsert ensures that all composite foreign keys that
// specify the matching method as MATCH FULL have all of their columns assigned
// values by the INSERT statement, or else have default/computed values.
// Alternatively, all columns can be unspecified. If neither condition is true,
// checkForeignKeys raises an error. Here is an example:
//
//	CREATE TABLE orders (
//	  id INT,
//	  cust_id INT,
//	  state STRING,
//	  FOREIGN KEY (cust_id, state) REFERENCES customers (id, state) MATCH FULL
//	)
//
//	INSERT INTO orders (cust_id) VALUES (1)
//
// This INSERT statement would trigger a static error, because only cust_id is
// specified in the INSERT statement. Either the state column must be specified
// as well, or else neither column can be specified.
func (mb *mutationBuilder) checkForeignKeysForInsert() {
	for i, n := 0, mb.tab.OutboundForeignKeyCount(); i < n; i++ {
		fk := mb.tab.OutboundForeignKey(i)
		numCols := fk.ColumnCount()

		// This check should only be performed on composite foreign keys that use
		// the MATCH FULL method.
		if numCols < 2 || fk.MatchMethod() != tree.MatchFull {
			continue
		}

		var missingCols []string
		allMissing := true
		for j := 0; j < numCols; j++ {
			ord := fk.OriginColumnOrdinal(mb.tab, j)
			col := mb.tab.Column(ord)
			if col.HasDefault() || col.IsComputed() {
				// The column has a default value.
				allMissing = false
				continue
			}

			colID := mb.tabID.ColumnID(ord)
			if mb.targetColSet.Contains(colID) {
				// The column is explicitly specified in the target name list.
				allMissing = false
				continue
			}

			missingCols = append(missingCols, string(col.ColName()))
		}
		if allMissing {
			continue
		}

		switch len(missingCols) {
		case 0:
			// Do nothing.
		case 1:
			panic(pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing value for column %q in multi-part foreign key", missingCols[0]))
		default:
			sort.Strings(missingCols)
			panic(pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing values for columns %q in multi-part foreign key", missingCols))
		}
	}
}

// addTargetTableColsForInsert adds up to maxCols columns to the list of columns
// that will be set by an INSERT operation. Non-mutation columns are added from
// the target table in the same order they appear in its schema. This method is
// used when the target columns are not explicitly specified in the INSERT
// statement:
//
//	INSERT INTO t VALUES (1, 2, 3)
//
// In this example, the first three columns of table t would be added as target
// columns.
func (mb *mutationBuilder) addTargetTableColsForInsert(maxCols int) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetTableColsForInsert cannot be called more than once"))
	}

	// Only consider non-mutation columns, since mutation columns are hidden from
	// the SQL user.
	numCols := 0
	for i, n := 0, mb.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		// Skip hidden columns.
		if mb.tab.Column(i).IsHidden() {
			continue
		}

		mb.addTargetCol(i)
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	mb.checkNumCols(len(mb.targetColList), maxCols)
}

// buildInputForInsert constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (mb *mutationBuilder) buildInputForInsert(inScope *scope, inputRows *tree.Select) {
	// Handle DEFAULT VALUES case by creating a single empty row as input.
	if inputRows == nil {
		mb.outScope = inScope.push()
		mb.outScope.expr = mb.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   mb.md.NextUniqueID(),
		})
		return
	}

	// If there are already required target columns, then those will provide
	// desired input types. Otherwise, input columns are mapped to the table's
	// non-hidden columns by corresponding ordinal position. Exclude hidden
	// columns to prevent this statement from writing hidden columns:
	//
	//   INSERT INTO <table> VALUES (...)
	//
	// However, hidden columns can be written if the target columns were
	// explicitly specified:
	//
	//   INSERT INTO <table> (...) VALUES (...)
	//
	var desiredTypes []*types.T
	if len(mb.targetColList) != 0 {
		desiredTypes = make([]*types.T, len(mb.targetColList))
		for i, colID := range mb.targetColList {
			desiredTypes[i] = mb.md.ColumnMeta(colID).Type
		}
	} else {
		// Do not target mutation columns.
		desiredTypes = make([]*types.T, 0, mb.tab.ColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			tabCol := mb.tab.Column(i)
			if !tabCol.IsHidden() {
				desiredTypes = append(desiredTypes, tabCol.DatumType())
			}
		}
	}

	mb.outScope = mb.b.buildStmt(inputRows, desiredTypes, inScope)

	if len(mb.targetColList) != 0 {
		// Target columns already exist, so ensure that the number of input
		// columns exactly matches the number of target columns.
		mb.checkNumCols(len(mb.targetColList), len(mb.outScope.cols))
	} else {
		// No target columns have been added by previous steps, so add columns
		// that are implicitly targeted by the input expression.
		mb.addTargetTableColsForInsert(len(mb.outScope.cols))
	}

	// Loop over input columns and:
	//   1. Type check each column
	//   2. Assign name to each column
	//   3. Add scope column ordinal to the insertOrds list.
	for i := range mb.outScope.cols {
		inCol := &mb.outScope.cols[i]
		ord := mb.tabID.ColumnOrdinal(mb.targetColList[i])

		tableType := tree.RelationalTable
		if ord == 0 && i == 0 {
			// we need to handle column of ts specially if table is TimeseriesTable
			tableType = mb.tab.GetTableType()
		}
		// Type check the input column against the corresponding table column.
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), inCol.typ, tableType)

		// Assign name of input column.
		inCol.name = tree.Name(mb.md.ColumnMeta(mb.targetColList[i]).Alias)

		// Record the ordinal position of the scope column that contains the
		// value to be inserted into the corresponding target table column.
		mb.insertOrds[ord] = scopeOrdinal(i)
	}
}

// addSynthesizedColsForInsert wraps an Insert input expression with a Project
// operator containing any default (or nullable) columns and any computed
// columns that are not yet part of the target column list. This includes all
// write-only mutation columns, since they must always have default or computed
// values.
func (mb *mutationBuilder) addSynthesizedColsForInsert() {
	// Start by adding non-computed columns that have not already been explicitly
	// specified in the query. Do this before adding computed columns, since those
	// may depend on non-computed columns.
	mb.addSynthesizedCols(
		mb.insertOrds,
		func(colOrd int) bool { return !mb.tab.Column(colOrd).IsComputed() },
	)

	// Possibly round DECIMAL-related columns containing insertion values (whether
	// synthesized or not).
	mb.roundDecimalValues(mb.insertOrds, false /* roundComputedCols */)

	// Now add all computed columns.
	mb.addSynthesizedCols(
		mb.insertOrds,
		func(colOrd int) bool { return mb.tab.Column(colOrd).IsComputed() },
	)

	// Possibly round DECIMAL-related computed columns.
	mb.roundDecimalValues(mb.insertOrds, true /* roundComputedCols */)
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols()

	mb.buildFKChecksForInsert()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructInsert(mb.outScope.expr, mb.checks, private)

	mb.buildReturning(returning)
}

// buildInputForDoNothing wraps the input expression in LEFT OUTER JOIN
// expressions, one for each UNIQUE index on the target table. It then adds a
// filter that discards rows that have a conflict (by checking a not-null table
// column to see if it was null-extended by the left join). See the comment
// header for Builder.buildInsert for an example.
func (mb *mutationBuilder) buildInputForDoNothing(inScope *scope, conflictOrds util.FastIntSet) {
	// DO NOTHING clause does not require ON CONFLICT columns.
	var conflictIndex cat.Index
	if !conflictOrds.Empty() {
		// Check that the ON CONFLICT columns reference at most one target row by
		// ensuring they match columns of a UNIQUE index. Using LEFT OUTER JOIN
		// to detect conflicts relies upon this being true (otherwise result
		// cardinality could increase). This is also a Postgres requirement.
		conflictIndex = mb.ensureUniqueConflictCols(conflictOrds)
	}

	insertColSet := mb.outScope.expr.Relational().OutputCols

	// Ignore any ordering requested by the input.
	// TODO(andyk): do we need to do more here?
	mb.outScope.ordering = nil

	// Loop again over each UNIQUE index, potentially creating a left join +
	// filter for each one.
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)
		if !index.IsUnique() {
			continue
		}

		// If conflict columns were explicitly specified, then only check for a
		// conflict on a single index. Otherwise, check on all indexes.
		if conflictIndex != nil && conflictIndex != index {
			continue
		}

		// Build the right side of the left outer join. Use a new metadata instance
		// of the mutation table so that a different set of column IDs are used for
		// the two tables in the self-join.
		scanScope := mb.b.buildScan(
			mb.b.addTable(mb.tab, &mb.alias),
			nil, /* ordinals */
			nil, /* indexFlags */
			noRowLocking,
			excludeMutations,
			inScope,
		)

		// Remember the column ID of a scan column that is not null. This will be
		// used to detect whether a conflict was detected for a row. Such a column
		// must always exist, since the index always contains the primary key
		// columns, either explicitly or implicitly.
		notNullColID := scanScope.cols[findNotNullIndexCol(index)].id

		// Build the join condition by creating a conjunction of equality conditions
		// that test each conflict column:
		//
		//   ON ins.x = scan.a AND ins.y = scan.b
		//
		var on memo.FiltersExpr
		for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
			indexCol := index.Column(i)
			scanColID := scanScope.cols[indexCol.Ordinal].id

			condition := mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(mb.insertColID(indexCol.Ordinal)),
				mb.b.factory.ConstructVariable(scanColID),
			)
			on = append(on, mb.b.factory.ConstructFiltersItem(condition))
		}

		// Construct the left join + filter.
		// TODO(andyk): Convert this to use anti-join once we have support for
		// lookup anti-joins.
		mb.outScope.expr = mb.b.factory.ConstructProject(
			mb.b.factory.ConstructSelect(
				mb.b.factory.ConstructLeftJoin(
					mb.outScope.expr,
					scanScope.expr,
					on,
					memo.EmptyJoinPrivate,
				),
				memo.FiltersExpr{mb.b.factory.ConstructFiltersItem(
					mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(notNullColID),
						memo.NullSingleton,
					),
				)},
			),
			memo.EmptyProjectionsExpr,
			insertColSet,
		)
	}

	// Loop over each UNIQUE index, potentially creating an upsert-distinct-on for
	// each one. This must happen after all conflicting rows are removed with the
	// left-joins + filters created above, to avoid removing valid rows (see
	// #59125).
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)
		if !index.IsUnique() {
			continue
		}

		// If conflict columns were explicitly specified, then only check for a
		// conflict on a single index. Otherwise, check on all indexes.
		if conflictIndex != nil && conflictIndex != index {
			continue
		}

		// Add an UpsertDistinctOn operator to ensure there are no duplicate input
		// rows for this unique index. Duplicate rows can trigger conflict errors
		// at runtime, which DO NOTHING is not supposed to do. See issue #37880.
		var conflictCols opt.ColSet
		for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
			indexCol := index.Column(i)
			conflictCols.Add(mb.outScope.cols[mb.insertOrds[indexCol.Ordinal]].id)
		}

		// Treat NULL values as distinct from one another. And if duplicates are
		// detected, remove them rather than raising an error.
		mb.outScope = mb.b.buildDistinctOn(
			conflictCols, mb.outScope, true /* nullsAreDistinct */, false /* errorOnDup */)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.DeletableColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// buildInputForUpsert assumes that the output scope already contains the insert
// columns. It left-joins each insert row to the target table, using the given
// conflict columns as the join condition. It also selects one of the table
// columns to be a "canary column" that can be tested to determine whether a
// given insert row conflicts with an existing row in the table. If it is null,
// then there is no conflict.
func (mb *mutationBuilder) buildInputForUpsert(
	inScope *scope, conflictOrds util.FastIntSet, whereClause *tree.Where,
) {
	// Check that the ON CONFLICT columns reference at most one target row.
	// Using LEFT OUTER JOIN to detect conflicts relies upon this being true
	// (otherwise result cardinality could increase). This is also a Postgres
	// requirement.
	mb.ensureUniqueConflictCols(conflictOrds)

	// Ensure that input is distinct on the conflict columns. Otherwise, the
	// Upsert could affect the same row more than once, which can lead to index
	// corruption. See issue #44466 for more context.
	//
	// Ignore any ordering requested by the input. Since the UpsertDistinctOn
	// operator does not allow multiple rows in distinct groupings, the internal
	// ordering is meaningless (and can trigger a misleading error in
	// buildDistinctOn if present).
	var conflictCols opt.ColSet
	for ord, ok := conflictOrds.Next(0); ok; ord, ok = conflictOrds.Next(ord + 1) {
		conflictCols.Add(mb.outScope.cols[mb.insertOrds[ord]].id)
	}
	mb.outScope.ordering = nil
	mb.outScope = mb.b.buildDistinctOn(
		conflictCols, mb.outScope, true /* nullsAreDistinct */, true /* errorOnDup */)

	// Re-alias all INSERT columns so that they are accessible as if they were
	// part of a special data source named "kwdb_internal.excluded".
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].table = excludedTableName
	}

	// Build the right side of the left outer join. Use a different instance of
	// table metadata so that col IDs do not overlap.
	//
	// NOTE: Include mutation columns, but be careful to never use them for any
	//       reason other than as "fetch columns". See buildScan comment.
	// TODO(andyk): Why does execution engine need mutation columns for Insert?
	fetchScope := mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		nil, /* ordinals */
		nil, /* indexFlags */
		noRowLocking,
		includeMutations,
		inScope,
	)

	// Record a not-null "canary" column. After the left-join, this will be null
	// if no conflict has been detected, or not null otherwise. At least one not-
	// null column must exist, since primary key columns are not-null.
	canaryScopeCol := &fetchScope.cols[findNotNullIndexCol(mb.tab.Index(cat.PrimaryIndex))]
	mb.canaryColID = canaryScopeCol.id

	// Set fetchOrds to point to the scope columns created for the fetch values.
	for i := range fetchScope.cols {
		// Fetch columns come after insert columns.
		mb.fetchOrds[i] = scopeOrdinal(len(mb.outScope.cols) + i)
	}

	// Add the fetch columns to the current scope. It's OK to modify the current
	// scope because it contains only INSERT columns that were added by the
	// mutationBuilder, and which aren't needed for any other purpose.
	mb.outScope.appendColumnsFromScope(fetchScope)

	// Build the join condition by creating a conjunction of equality conditions
	// that test each conflict column:
	//
	//   ON ins.x = scan.a AND ins.y = scan.b
	//
	var on memo.FiltersExpr
	for i := range fetchScope.cols {
		// Include fetch columns with ordinal positions in conflictOrds.
		if conflictOrds.Contains(i) {
			condition := mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(mb.insertColID(i)),
				mb.b.factory.ConstructVariable(fetchScope.cols[i].id),
			)
			on = append(on, mb.b.factory.ConstructFiltersItem(condition))
		}
	}

	// Construct the left join.
	mb.outScope.expr = mb.b.factory.ConstructLeftJoin(
		mb.outScope.expr,
		fetchScope.expr,
		on,
		memo.EmptyJoinPrivate,
	)

	// Add a filter from the WHERE clause if one exists.
	if whereClause != nil {
		where := &tree.Where{
			Type: whereClause.Type,
			Expr: &tree.OrExpr{
				Left: &tree.ComparisonExpr{
					Operator: tree.IsNotDistinctFrom,
					Left:     canaryScopeCol,
					Right:    tree.DNull,
				},
				Right: whereClause.Expr,
			},
		}
		mb.b.buildWhere(where, mb.outScope)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.DeletableColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// buildTSInsert builds a memo group for an InsertOp expression. To
// begin, an input expression is constructed which outputs these columns to
// insert into the target ts table.
// Parameters:
//   - inScope:	This parameter contains the name bindings that are visible for this
//     statement/expression (e.g., passed in from an enclosing statement).
//   - table: ts table descriptor
//   - ins: AST of INSERT
//   - sTableID: template table ID
//   - ct:	If the inserted instance table does not exist, need to create it first
//
// Returns:
//   - outScope: This return value contains the newly bound variables that will be
//     visible to enclosing statements, as well as a pointer to any
//     "parent" scope that is still visible. The top-level memo expression
//     for the built statement/expression is returned in outScope.expr.
func (b *Builder) buildTSInsert(
	inScope *scope, table cat.Table, ins *tree.Insert, sTableID opt.TableID, ct *tree.CreateTable,
) (outScope *scope) {
	// Parameter validation
	if ins.OnConflict != nil {
		panic(sqlbase.TSUnsupportedError("on conflict"))
	}
	if _, ok := ins.Returning.(*tree.NoReturningClause); !ok {
		panic(sqlbase.TSUnsupportedError("returning"))
	}
	// Resolve the columns of the table, separating the data columns and tag columns.
	// Arrange them in order data columns + tag columns.
	colCount := table.ColumnCount()
	cols := make([]*sqlbase.ColumnDescriptor, 0)
	var dataCols, tagCols []*sqlbase.ColumnDescriptor
	priTagCols := make([]*sqlbase.ColumnDescriptor, 0)
	for i := 0; i < colCount; i++ {
		col := table.Column(i).(*sqlbase.ColumnDescriptor)
		if col.IsPrimaryTagCol() {
			priTagCols = append(priTagCols, col)
		}
		if col.IsTagCol() {
			tagCols = append(tagCols, col)
		} else {
			dataCols = append(dataCols, col)
		}
	}
	cols = append(cols, dataCols...)
	cols = append(cols, tagCols...)
	colMap := make(map[int]int, len(cols))

	if table.GetTableType() == tree.TemplateTable {
		analysisTmplTableTag(b, table, ct)
	}

	// user specifies insert field.
	colMap, _, _ = buildColsForTsInsert(ins, cols, table, true)
	notFound := false
	var priTagNames []string
	for _, col := range priTagCols {
		if _, ok := colMap[int(col.ID)]; !ok {
			notFound = true
		}
		priTagNames = append(priTagNames, col.Name)
	}
	if notFound && table.GetTableType() != tree.InstanceTable &&
		table.GetTableType() != tree.TemplateTable {
		panic(pgerror.Newf(pgcode.Syntax, "need to specify all primary tag %v", priTagNames))
	}
	var rowsValue []tree.Exprs
	var err error
	rowsValue, err = checkInputForTSInsert(b.semaCtx, ins, cols, colMap)
	if err != nil {
		panic(err)
	}
	// use table name as primary tag value for instance table insert
	if table.GetTableType() == tree.InstanceTable || table.GetTableType() == tree.TemplateTable {

		var childNamePriTag *tree.StrVal
		switch tn := ins.Table.(type) {
		case *tree.TableName:
			childNamePriTag = tree.NewStrVal(tn.Table())
		case *tree.InFlightCreateTableExpr:
			childNamePriTag = tree.NewStrVal(tn.TableDef.Table.Table())
		default:
			panic(pgerror.Newf(pgcode.WrongObjectType, "unsupported TableExpr type, table name: %v", table.Name()))
		}
		childRowsValue := make([]tree.Exprs, len(rowsValue))
		for i := range childRowsValue {
			childRowsValue[i] = make(tree.Exprs, len(rowsValue[i]))
			copy(childRowsValue[i], rowsValue[i])
			childRowsValue[i] = append(childRowsValue[i], childNamePriTag)
		}
		colMap[int(priTagCols[0].ID)] = len(childRowsValue[0]) - 1
		rowsValue = childRowsValue
	}

	outScope = inScope.push()
	outScope.expr = b.factory.ConstructTSInsert(
		&memo.TSInsertPrivate{
			InputRows:       rowsValue,
			ColsMap:         colMap,
			STable:          sTableID,
			NeedCreateTable: opt.CheckTsProperty(b.TSInfo.TSProp, TSPropInsertCreateTable),
			CT:              ct,
		})
	b.TSInfo.TSProp = opt.AddTSProperty(b.TSInfo.TSProp, TSPropNeedTSTypeCheck)
	return outScope
}

// setUpsertCols sets the list of columns to be updated in case of conflict.
// There are two cases to handle:
//
//  1. Target columns are explicitly specified:
//     UPSERT INTO abc (col1, col2, ...) <input-expr>
//
//  2. Target columns are implicitly derived:
//     UPSERT INTO abc <input-expr>
//
// In case #1, only the columns that were specified by the user will be updated.
// In case #2, all non-mutation columns in the table will be updated.
//
// Note that primary key columns (i.e. the conflict detection columns) are never
// updated. This can have an impact in unusual cases where equal SQL values have
// different representations. For example:
//
//	CREATE TABLE abc (a DECIMAL PRIMARY KEY, b DECIMAL)
//	INSERT INTO abc VALUES (1, 2.0)
//	UPSERT INTO abc VALUES (1.0, 2)
//
// The UPSERT statement will update the value of column "b" from 2 => 2.0, but
// will not modify column "a".
func (mb *mutationBuilder) setUpsertCols(insertCols tree.NameList) {
	if len(insertCols) != 0 {
		for _, name := range insertCols {
			// Table column must exist, since existence of insertCols has already
			// been checked previously.
			ord := cat.FindTableColumnByName(mb.tab, name)
			mb.updateOrds[ord] = mb.insertOrds[ord]
		}
	} else {
		copy(mb.updateOrds, mb.insertOrds)
	}

	// Never update mutation columns.
	for i, n := mb.tab.ColumnCount(), mb.tab.DeletableColumnCount(); i < n; i++ {
		mb.updateOrds[i] = -1
	}

	// Never update primary key columns.
	conflictIndex := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, conflictIndex.KeyColumnCount(); i < n; i++ {
		mb.updateOrds[conflictIndex.Column(i).Ordinal] = -1
	}
}

// buildUpsert constructs an Upsert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpsert(returning tree.ReturningExprs) {
	// Merge input insert and update columns using CASE expressions.
	mb.projectUpsertColumns()

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols()

	mb.buildFKChecksForUpsert()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructUpsert(mb.outScope.expr, mb.checks, private)

	mb.buildReturning(returning)
}

// projectUpsertColumns projects a set of merged columns that will be either
// inserted into the target table, or else used to update an existing row,
// depending on whether the canary column is null. For example:
//
//	UPSERT INTO ab VALUES (ins_a, ins_b) ON CONFLICT (a) DO UPDATE SET b=upd_b
//
// will cause the columns to be projected:
//
//	SELECT
//	  fetch_a,
//	  fetch_b,
//	  CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//	  CASE WHEN fetch_b IS NULL ins_b ELSE upd_b END AS ups_b,
//	FROM (SELECT ins_a, ins_b, upd_b, fetch_a, fetch_b FROM ...)
//
// For each column, a CASE expression is created that toggles between the insert
// and update values depending on whether the canary column is null. These
// columns can then feed into any constraint checking expressions, which operate
// on the final result values.
func (mb *mutationBuilder) projectUpsertColumns() {
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	// Add a new column for each target table column that needs to be upserted.
	// This can include mutation columns.
	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		insertScopeOrd := mb.insertOrds[i]
		updateScopeOrd := mb.updateOrds[i]
		if updateScopeOrd == -1 {
			updateScopeOrd = mb.fetchOrds[i]
		}

		// Skip columns that will only be inserted or only updated.
		if insertScopeOrd == -1 || updateScopeOrd == -1 {
			continue
		}

		// Skip columns where the insert value and update value are the same.
		if mb.scopeOrdToColID(insertScopeOrd) == mb.scopeOrdToColID(updateScopeOrd) {
			continue
		}

		// Generate CASE that toggles between insert and update column.
		caseExpr := mb.b.factory.ConstructCase(
			memo.TrueSingleton,
			memo.ScalarListExpr{
				mb.b.factory.ConstructWhen(
					mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(mb.canaryColID),
						memo.NullSingleton,
					),
					mb.b.factory.ConstructVariable(mb.outScope.cols[insertScopeOrd].id),
				),
			},
			mb.b.factory.ConstructVariable(mb.outScope.cols[updateScopeOrd].id),
		)

		alias := fmt.Sprintf("upsert_%s", mb.tab.Column(i).ColName())
		typ := mb.outScope.cols[insertScopeOrd].typ
		scopeCol := mb.b.synthesizeColumn(projectionsScope, alias, typ, nil /* expr */, caseExpr)
		scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)

		// Assign name to synthesized column.
		scopeCol.name = mb.tab.Column(i).ColName()

		// Update the scope ordinals for the update columns that are involved in
		// the Upsert. The new columns will be used by the Upsert operator in place
		// of the original columns. Also set the scope ordinals for the upsert
		// columns, as those columns can be used by RETURNING columns.
		if mb.updateOrds[i] != -1 {
			mb.updateOrds[i] = scopeColOrd
		}
		mb.upsertOrds[i] = scopeColOrd
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope
}

// ensureUniqueConflictCols tries to prove that the given set of column ordinals
// correspond to the columns of at least one UNIQUE index on the target table.
// If true, then ensureUniqueConflictCols returns the matching index. Otherwise,
// it reports an error.
func (mb *mutationBuilder) ensureUniqueConflictCols(conflictOrds util.FastIntSet) cat.Index {
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)

		// Skip non-unique indexes. Use lax key columns, which always contain
		// the minimum columns that ensure uniqueness. Null values are considered
		// to be *not* equal, but that's OK because the join condition rejects
		// nulls anyway.
		if !index.IsUnique() || index.LaxKeyColumnCount() != conflictOrds.Len() {
			continue
		}

		// Determine whether the conflict columns match the columns in the lax key.
		indexOrds := getIndexLaxKeyOrdinals(index)
		if indexOrds.Equals(conflictOrds) {
			return index
		}
	}
	panic(pgerror.New(pgcode.InvalidColumnReference,
		"there is no unique or exclusion constraint matching the ON CONFLICT specification"))
}

// mapColumnNamesToOrdinals returns the set of ordinal positions within the
// target table that correspond to the given names.
func (mb *mutationBuilder) mapColumnNamesToOrdinals(names tree.NameList) util.FastIntSet {
	var ords util.FastIntSet
	for _, name := range names {
		found := false
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			tabCol := mb.tab.Column(i)
			if tabCol.ColName() == name {
				ords.Add(i)
				found = true
				break
			}
		}

		if !found {
			panic(sqlbase.NewUndefinedColumnError(string(name)))
		}
	}
	return ords
}

// getTSColumnByName returns the creating index of endpoint corresponding user specified insertion order
func getTSColumnByName(
	inputName tree.Name, cols []*sqlbase.ColumnDescriptor,
) (*sqlbase.ColumnDescriptor, error) {
	for i := range cols {
		if string(inputName) == cols[i].Name {
			return cols[i], nil
		}
	}
	return nil, sqlbase.NewUndefinedColumnError(string(inputName))
}

// checkInputForTSInsert checks the correspondence against the input values and the specified columns.
// Parameters:
// - ins: AST of INSERT
// - cols: the column descriptors of data/tag columns
// - colMap: the column index of input values
// Returns:
// - return Datums
func checkInputForTSInsert(
	ctx *tree.SemaContext, ins *tree.Insert, cols []*sqlbase.ColumnDescriptor, colMap map[int]int,
) ([]tree.Exprs, error) {
	input, ok := ins.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return nil, pgerror.New(pgcode.Syntax, "Please use INSERT...VALUES...")
	}
	colLength := len(colMap)

	// Initialize the return values
	inputValues := input.Rows
	for i := range inputValues {
		// Checks that the length of each input row value is equal to the specified number of columns.
		if len(input.Rows[i]) > colLength {
			return nil, pgerror.Newf(
				pgcode.Syntax,
				"insert (row %d) has more expressions than target columns, %d expressions for %d targets",
				i+1, len(input.Rows[i]), colLength)
		} else if len(input.Rows[i]) < colLength {
			return nil, pgerror.Newf(
				pgcode.Syntax,
				"insert (row %d) has more target columns than expressions, %d expressions for %d targets",
				i+1, len(input.Rows[i]), colLength)
		}
		// The type check is performed according to the sequence of columns defined during creation.
		// The defined columns correspond to the input value one by one.
		for j := range cols {
			column := cols[j]
			// Retrieves the value of the current column from colMap.
			ord, ok := colMap[int(column.ID)]
			if !ok {
				continue
			}
			switch v := inputValues[i][ord].(type) {
			case *tree.Placeholder:
				ctx.Placeholders.Types[v.Idx] = &column.Type
			case *tree.DBool:
				switch column.Type.Family() {
				case types.BoolFamily, types.IntFamily:
					// do nothing
				default:
					return nil, tree.NewDatatypeMismatchError(column.Name, v.String(), column.Type.SQLString())
				}
			case *tree.FuncExpr:
				// Currently only the now() function is supported for inserting ts table.
				if v.Func.FunctionName() != "now" {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported function input \"%s\"", v.Func.FunctionName())
				}
				texpr, err := v.TypeCheck(ctx, &column.Type)
				if err != nil {
					return nil, err
				}
				inputValues[i][ord] = texpr
			case *tree.NumVal, *tree.StrVal, tree.DNullExtern:
				// do nothing
			case *tree.UnresolvedName:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\" (column %s)", v.String(), column.Name)
			case *tree.BinaryExpr:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type BinaryOperator")
			case *tree.UserDefinedVar:
				val, ok := ctx.UserDefinedVars[v.VarName].(tree.Datum)
				if !ok {
					return nil, pgerror.Newf(pgcode.Syntax, "%s is not defined", v.VarName)
				}
				texpr, err := val.TypeCheck(ctx, &column.Type)
				if err != nil {
					return nil, err
				}
				inputValues[i][ord] = texpr
			default:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type %T", v)
			}
		}
	}
	return inputValues, nil
}

// analysisTmplTableTag handles the use of placeholders when inserting template tables.
func analysisTmplTableTag(b *Builder, table cat.Table, ct *tree.CreateTable) {
	tagMeta := table.GetTagMeta()
	// remove hidden tag
	tagMeta = tagMeta[1:]
	for i := range ct.Tags {
		var tagType types.T
		// Only when no name is specified will there be a situation where TagName is empty
		if ct.Tags[i].TagName == "" {
			if len(ct.Tags) != len(tagMeta) {
				panic(errors.AssertionFailedf("Tags number not matched, number of tags: %v, number of tagMeta: %v", len(ct.Tags), len(tagMeta)))
			}
			ct.Tags[i].TagName = tree.Name(tagMeta[i].TagName)
			tagType = tagMeta[i].TagType
			if ph, ok := ct.Tags[i].TagVal.(*tree.Placeholder); ok {
				b.semaCtx.Placeholders.Types[ph.Idx] = &tagType
			}
			// Partial or fully specified names
		} else {
			var find bool
			for _, stag := range tagMeta {
				if string(ct.Tags[i].TagName) == stag.TagName {
					find = true
					tagType = stag.TagType
					if ph, ok := ct.Tags[i].TagVal.(*tree.Placeholder); ok {
						b.semaCtx.Placeholders.Types[ph.Idx] = &tagType
					}
					break
				}
			}
			if !find {
				panic(errors.AssertionFailedf("Tag %s does not exist", ct.Tags[i].TagName))
			}
		}
	}
}

// maybeAddNonExistsColumn can automatically add columns that do not exist.
func (b *Builder) maybeAddNonExistsColumn(
	tab cat.Table, ins *tree.Insert, name tree.TableName,
) (bool, error) {
	addStmts, err := constructAutoAddStmts(tab, ins, name)
	if err != nil {
		return false, err
	}
	if len(addStmts) != 0 {
		// ALTER statements will be blocked by table leases held by DML statements.
		// So release the table leases.
		b.catalog.ReleaseTables(b.ctx)
	}
	retryOpt := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     60, // about 5 minutes
	}
	for i := range addStmts {
		// Retry while execution returns error.
		for r := retry.Start(retryOpt); r.Next(); {
			_, err := b.evalCtx.InternalExecutor.Query(b.evalCtx.Context, "auto add column", nil, addStmts[i])
			if err != nil {
				if IsInsertNoSchemaRetryableError(err) {
					log.Warningf(b.ctx, "auto alter add failed: %s, err: %s\n", addStmts[i], err.Error())
				} else if strings.Contains(err.Error(), "schema version") {
					return false, err
				} else {
					break
				}
			} else {
				log.Infof(b.ctx, "auto alter add success: %s\n", addStmts[i])
				break
			}
		}
	}
	return len(addStmts) != 0, nil
}

// constructAutoAddStmts checks whether columns are exist and generate alter table stmt if necessary
func constructAutoAddStmts(tab cat.Table, ins *tree.Insert, name tree.TableName) ([]string, error) {
	cols := getColumnDescs(tab)
	const alterStmt = `ALTER TABLE %s ADD %s %s %s`
	const alterColumnTypeStmt = `ALTER TABLE %s ALTER COLUMN %s TYPE %s`
	const alterTagTypeStmt = `ALTER TABLE %s ALTER TAG %s TYPE %s`
	var addStmts []string
	inColName := make(map[tree.Name]bool)

	// constructs the map of the metadata column to the input value based on the user-specified column name
	for idx := range ins.NoSchemaColumns {
		colDef := ins.NoSchemaColumns[idx]
		// check if duplicate columns are input.
		if _, ok := inColName[colDef.Name]; !ok {
			inColName[colDef.Name] = true
		} else {
			return []string{}, pgerror.Newf(pgcode.DuplicateColumn, "multiple assignments to the same column \"%s\"", string(colDef.Name))
		}
		typ := `COLUMN`
		if colDef.IsTag {
			typ = `TAG`
		}
		targetCol, err := getTSColumnByName(colDef.Name, cols)
		if err != nil {
			if pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
				// If the column does not exist, generate a ALTER ... ADD ... statement.
				addStmts = append(addStmts, fmt.Sprintf(alterStmt, name.String(), typ, colDef.Name.String(), colDef.Type.SQLString()))
			} else {
				return []string{}, err
			}
		} else {
			if targetCol.IsTagCol() != colDef.IsTag {
				return []string{}, pgerror.Newf(pgcode.DuplicateColumn, "duplicate %s name: %q", typ, colDef.Name)
			}
			if targetCol.Type.Width() < ins.NoSchemaColumns[idx].Type.Width() {
				if ins.NoSchemaColumns[idx].IsTag {
					addStmts = append(addStmts, fmt.Sprintf(alterTagTypeStmt, name.String(), colDef.Name.String(), colDef.Type.SQLString()))
				} else {
					addStmts = append(addStmts, fmt.Sprintf(alterColumnTypeStmt, name.String(), colDef.Name.String(), colDef.Type.SQLString()))
				}
			}
		}

		// instance table does not support specifying tag column
		if targetCol != nil && targetCol.IsTagCol() && (tab.GetTableType() == tree.InstanceTable ||
			tab.GetTableType() == tree.TemplateTable) {
			return []string{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot insert tag column: \"%s\" for INSTANCE table", targetCol.Name)
		}
	}
	return addStmts, nil
}

func getColumnDescs(tab cat.Table) []*sqlbase.ColumnDescriptor {
	colCount := tab.ColumnCount()
	var cols []*sqlbase.ColumnDescriptor
	for i := 0; i < colCount; i++ {
		cols = append(cols, tab.Column(i).(*sqlbase.ColumnDescriptor))
	}
	return cols
}

// IsInsertNoSchemaRetryableError returns true if error contains 'wait for success'.
func IsInsertNoSchemaRetryableError(err error) bool {
	if strings.Contains(err.Error(), "Please wait for success") {
		return true
	}
	return false
}
