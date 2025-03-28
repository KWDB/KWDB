// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package importer

import (
	"context"
	"fmt"
	"io/ioutil"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// checkAndGetDetailsInTableInto checks relational table
// if table is interleaved or has default value column and return tableDetails.
func checkAndGetDetailsInTableInto(
	table *sqlbase.MutableTableDescriptor, intoCols []string,
) ([]sqlbase.ImportTable, error) {
	var tableDetails []sqlbase.ImportTable
	// IMPORT INTO does not currently support interleaved tables.
	if table.IsInterleaved() {
		// TODO(miretskiy): Handle import into when tables are interleaved.
		return tableDetails, pgerror.New(pgcode.FeatureNotSupported, "Cannot use IMPORT INTO with interleaved tables")
	}
	// IMPORT INTO does not support columns with DEFAULT expressions. Ensure
	// that all non-target columns are nullable until we support DEFAULT
	// expressions.
	for _, col := range table.VisibleColumnsWithTagCol() {
		if col.HasDefault() {
			return tableDetails, errors.Errorf("cannot IMPORT INTO a table with a DEFAULT expression for any of its columns")
		}
	}
	tableDetails = []sqlbase.ImportTable{{Desc: &table.TableDescriptor, IsNew: false, IntoCols: intoCols}}
	return tableDetails, nil
}

// checkAndGetDetailsInTableNew checks if relational table will be created is legal,
// and checks if user has create privilege on target database,
// and return tableDetails.
func checkAndGetDetailsInTableNew(
	ctx context.Context,
	p sql.PlanHookState,
	importStmt *tree.Import,
	createFileFn func() (string, error),
	OptComment bool,
	withPrivileges bool,
) (string, []sqlbase.ImportTable, []string, error) {
	var create *tree.CreateTable
	var table *tree.TableName
	var tableDetails []sqlbase.ImportTable
	var stmts parser.Statements
	var dbName string
	var createFile bool
	var hasTableComment bool
	var hasPrivileges bool
	var privileges []string
	// 1.IMPORT gets create stmt from createDefs or create file.
	if importStmt.CreateDefs != nil { /* Specify the table structure when importing */
		create = &tree.CreateTable{
			Table: *importStmt.Table,
			Defs:  importStmt.CreateDefs,
		}
		table = importStmt.Table
	} else { /* Specifies the table structure in sqlFile */
		createFile = true
		sqlFile, err := createFileFn()
		if err != nil {
			return "", tableDetails, nil, err
		}
		stmts, err = readCreateTableFromStore(ctx, sqlFile, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI)
		if err != nil {
			return "", tableDetails, nil, err
		}
		create, _ = stmts[0].AST.(*tree.CreateTable)
		table = &create.Table
	}
	if err := checkCreateTableLegal(ctx, create); err != nil {
		return "", tableDetails, nil, err
	}
	// 2.We have a target table, so it might specify a DB in its name.
	descI, err := checkDatabaseExist(ctx, p, table)
	if err != nil {
		return "", tableDetails, nil, err
	}
	// 3.If this is a non-INTO import that will thus be making a new table, we
	// need the CREATE priv in the target DB.
	dbDesc := descI.(*sqlbase.ResolvedObjectPrefix).Database
	if err := p.CheckPrivilege(ctx, &dbDesc, privilege.CREATE); err != nil {
		return "", tableDetails, nil, err
	}
	if !createFile {
		tableDetails = []sqlbase.ImportTable{{Create: create.String(), IsNew: true, SchemaName: table.Schema(), TableName: table.Table()}}
		return dbDesc.Name, tableDetails, nil, nil
	}
	if dbName, tableDetails, hasTableComment, privileges, hasPrivileges, err = checkAndGetDetailsInTable(ctx, p, stmts, OptComment, withPrivileges); err != nil {
		return "", tableDetails, nil, err
	}
	if OptComment && !hasTableComment {
		return "", tableDetails, nil, errors.New("NO COMMENT statement in the TABLE SQL file")
	}
	// Check if there are privileges in SQL
	if withPrivileges && !hasPrivileges {
		return "", tableDetails, nil, errors.New("NO GRANT statement in the SQL file")
	}
	return dbName, tableDetails, privileges, err
}

// readCreateTableFromStore read meta.sql at target path and parse it to stmts.
func readCreateTableFromStore(
	ctx context.Context, filename string, externalStorageFromURI cloud.ExternalStorageFromURIFactory,
) (parser.Statements, error) {
	store, err := externalStorageFromURI(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	_, ok := stmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return stmts, nil
}

// We have a target table, so it might specify a DB in its name.
func checkDatabaseExist(
	ctx context.Context, p sql.PlanHookState, table *tree.TableName,
) (tree.SchemaMeta, error) {
	found, descI, err := table.ResolveTarget(ctx,
		p, p.SessionData().Database, p.SessionData().SearchPath)
	if err != nil {
		return descI, pgerror.Wrap(err, pgcode.UndefinedTable,
			"resolving target import name")
	}
	if !found {
		// Check if database exists right now. It might not after the import is done,
		// but it's better to fail fast than wait until restore.
		return descI, pgerror.Newf(pgcode.UndefinedObject,
			"database does not exist: %q", table)
	}
	return descI, nil
}

// Many parts of the syntax are unsupported in import
// but this is enough for our csv IMPORT and for some unit tests.
func checkCreateTableLegal(ctx context.Context, create *tree.CreateTable) error {
	create.HoistConstraints()
	if create.IfNotExists {
		return unimplemented.NewWithIssueDetailf(42846, "import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return unimplemented.NewWithIssueDetailf(42846, "import.interleave", "interleaved not supported")
	}
	if create.AsSource != nil {
		return unimplemented.NewWithIssueDetailf(42846, "import.create-as", "CREATE AS not supported")
	}
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef,
			*tree.ForeignKeyConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return unimplemented.NewWithIssueDetailf(42846, "import.computed",
					"computed columns not supported: %s", tree.AsString(def))
			}
			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return err
			}
		default:
			return unimplemented.Newf(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
	}
	return nil
}

// checkTableMetaFile read createFile and parse it to stmts.
func checkTableMetaFile(
	ctx context.Context, p sql.PlanHookState, createFileFn func() (string, error),
) (parser.Statements, error) {
	sqlFile, err := createFileFn()
	if err != nil {
		return nil, err
	}
	store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, sqlFile)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	if stmts == nil {
		return nil, errors.Errorf("import table sql file is empty")
	}
	_, ok := stmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("import table sql file must have right create table sql")
	}
	return stmts, nil
}

// checkAndGetDetailsInTable generate ts tables' tableDetails and return.
func checkAndGetDetailsInTable(
	ctx context.Context,
	p sql.PlanHookState,
	stmts parser.Statements,
	OptComment bool,
	withPrivileges bool,
) (string, []sqlbase.ImportTable, bool, []string, bool, error) {
	tbCreate, _ := stmts[0].AST.(*tree.CreateTable)
	prefix, err := sql.ResolveTargetObject(ctx, p, &tbCreate.Table)
	if err != nil {
		return "", nil, false, nil, false, err
	}
	dbName := prefix.Database.Name
	var tableDetails []sqlbase.ImportTable
	// Gets information about all instance tables in the template table's meta.sql.
	var hasComment bool
	var hasPrivileges bool
	var privileges []string
	for i, stmt := range stmts {
		if tb, ok := stmt.AST.(*tree.CreateTable); ok {
			var columnName []string
			for _, def := range tbCreate.Defs {
				if d, ok := def.(*tree.ColumnTableDef); ok {
					columnName = append(columnName, string(d.Name))
				}
			}
			for _, tag := range tbCreate.Tags {
				if tag.TagName != "" {
					columnName = append(columnName, string(tag.TagName))
				}
			}
			tableDetails = append(tableDetails, sqlbase.ImportTable{Create: stmts[i].SQL, IsNew: true, TableName: tbCreate.Table.Table(),
				UsingSource: tb.UsingSource.Table(), TableType: tb.TableType, ColumnName: columnName, SchemaName: tb.Table.Schema()})
			continue
		}
		// Check and obtain comments in the SQL file if WITH COMMENT
		if OptComment {
			// Check if there is a COMMENT ON TABLE, and if so, whether the table has been established
			tableComment, comment := stmts[i].AST.(*tree.CommentOnTable)
			if comment {
				n, hasTable := isTableCreated(tableComment.Table.ToTableName().TableName, tableDetails)
				if !hasTable {
					return "", nil, false, nil, false, errors.New("The table for COMMENT ON was not created")
				}
				tableDetails[n].TableComment = stmts[i].SQL
				hasComment = true
				continue
			}

			// Check if there is a COMMENT ON COLUMN, and if so, whether the column has been established
			columnComment, comment := stmts[i].AST.(*tree.CommentOnColumn)
			if comment {
				n, hasTable := isTableCreated(columnComment.TableName.ToTableName().TableName, tableDetails)
				if !hasTable {
					return "", nil, false, nil, false, errors.New("The table containing this column for COMMENT has not been created")
				}
				hasColumn := isColumnCreated(columnComment.ColumnName, tableDetails[n])
				if !hasColumn {
					return "", nil, false, nil, false, errors.New("The column for COMMENT ON was not created")
				}
				tableDetails[n].ColumnComment = append(tableDetails[n].ColumnComment, stmts[i].SQL)
				hasComment = true
				continue
			}
		}
		if colIndex, ok := stmts[i].AST.(*tree.CreateIndex); ok {
			n, hasTable := isTableCreated(colIndex.Table.TableName, tableDetails)
			if !hasTable {
				return "", nil, false, nil, false, errors.New("The table containing this column for index has not been created")
			}
			tableDetails[n].ColumnIndex = append(tableDetails[n].ColumnIndex, stmts[i].SQL)
			continue
		}
		// Check and obtain comments in the SQL file if WITH Privileges
		if withPrivileges {
			// Check if there is a GRANT statement
			_, ok := stmts[i].AST.(*tree.Grant)
			if ok {
				privileges = append(privileges, stmts[i].SQL)
			}
			hasPrivileges = true
		}
	}
	return dbName, tableDetails, hasComment, privileges, hasPrivileges, nil
}

// execCreateTableMeta exec create table sql use InternalExecutor.
func execCreateTableMeta(
	ctx context.Context, p sql.PlanHookState, create string, dbName string,
) error {
	p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: dbName})
	defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	_, err := p.ExecCfg().InternalExecutor.Exec(ctx, "create table", nil, create)
	if err != nil {
		return err
	}
	return nil
}

// execCommentOnMeta exec create comments sql use InternalExecutor.
func execCommentOnMeta(
	ctx context.Context, p sql.PlanHookState, comment string, dbName string,
) error {
	p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: dbName})
	defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	_, err := p.ExecCfg().InternalExecutor.Exec(ctx, "comment on", nil, comment)
	if err != nil {
		return err
	}
	return nil
}

// execIndexOnMeta exec create index sql use InternalExecutor.
func execIndexOnMeta(ctx context.Context, p sql.PlanHookState, index string, dbName string) error {
	p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: dbName})
	defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	_, err := p.ExecCfg().InternalExecutor.Exec(ctx, "create index", nil, index)
	if err != nil {
		return err
	}
	return nil
}
