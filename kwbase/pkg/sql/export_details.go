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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"github.com/cockroachdb/errors"
)

type exportOptions struct {
	csvOpts        roachpb.CSVOptions
	chunkSize      int
	onlyMeta       bool
	onlyData       bool
	colName        bool
	withComment    bool
	withPrivileges bool
}

// checkBeforeExport is used to check some roles of export before running, such as privilege and options.
func checkBeforeExport(
	ctx context.Context,
	planner *planner,
	res RestrictedCommandResult,
	ex *connExecutor,
	exp *tree.Export,
) (exportOptions, error) {
	var expOpts exportOptions
	if !ex.planner.ExtendedEvalContext().TxnImplicit {
		err := errors.Errorf("EXPORT cannot be used inside a transaction")
		res.SetError(err)
		return expOpts, err
	}

	if exp.FileFormat != "CSV" && exp.FileFormat != "SQL" {
		err := errors.Errorf("unsupported export format: %q", exp.FileFormat)
		res.SetError(err)
		return expOpts, err
	}
	if err := ex.planner.RequireAdminRole(ctx, "EXPORT"); err != nil {
		res.SetError(err)
		return expOpts, err
	}
	optsFn, err := planner.TypeAsStringOpts(exp.Options, exportOptionExpectValues)
	if err != nil {
		res.SetError(err)
		return expOpts, err
	}
	opts, err := optsFn()
	if err != nil {
		res.SetError(err)
		return expOpts, err
	}
	expOpts, err = checkExportOptions(exp, res, opts)
	if err != nil {
		res.SetError(err)
		return expOpts, err
	}
	return expOpts, nil
}

// exportRelationalAndTsDatabase is used to distinguish and export relational database and time series database.
func exportRelationalAndTsDatabase(
	ctx context.Context,
	planner *planner,
	res RestrictedCommandResult,
	ex *connExecutor,
	exp *tree.Export,
	expOpts exportOptions,
) error {
	var err error
	var dbID sqlbase.ID
	defer planner.SetAuditTarget(uint32(dbID), string(exp.Database), nil)
	dbID, err = getDatabaseID(ctx, planner.txn, string(exp.Database), true)
	if err != nil {
		res.SetError(err)
		return err
	}

	var dbDesc *sqlbase.DatabaseDescriptor
	dbDesc, err = getDatabaseDescByID(ctx, planner.txn, dbID)
	if err != nil {
		res.SetError(err)
		return err
	}
	if dbDesc.EngineType == tree.EngineTypeTimeseries {
		// export ts database: export all meta and dispatch table
		return ex.dispatchExportTSDB(ctx, planner, res, expOpts)
	} else if dbDesc.EngineType == tree.EngineTypeRelational {
		return ex.dispatchExportDB(ctx, planner, res, expOpts)
	}
	return nil
}

// dispatchExportDB is used to export database.
// 1. writes down database and schema's meta file first.
// 2. rewrites the plan and recall the func dispatchToExecutionEngine for every table
func (ex *connExecutor) dispatchExportDB(
	ctx context.Context, planner *planner, res RestrictedCommandResult, expOpts exportOptions,
) error {
	exp := planner.stmt.AST.(*tree.Export)
	dbPath := exp.File.(*tree.StrVal).RawString()
	// createStmtFunc
	createStmtFunc := func(ctx context.Context, uri, filename, content string) error {
		if expOpts.onlyData {
			return nil
		}
		var buf bytes.Buffer
		conf, err := cloud.ExternalStorageConfFromURI(uri)
		if err != nil {
			return err
		}
		es, err := planner.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		defer es.Close()
		if _, err := buf.Write([]byte(content)); err != nil {
			return err
		}
		err = es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes()))
		return nil
	}
	// generate statement of create database,write databaseName.sql
	DatabaseName := string(exp.Database)
	if ContainsUpperCase(DatabaseName) {
		DatabaseName = "\"" + DatabaseName + "\""
	}
	planner.stmt.AST.(*tree.Export).IgnoreCheckComment = true
	var findComment bool
	sqlDB := "CREATE DATABASE " + DatabaseName + ";"
	if expOpts.withComment {
		// add database comment
		selectStmt := fmt.Sprintf("select shobj_description(oid, 'pg_database') from pg_catalog.pg_database where datname = '%s';", string(exp.Database))
		row, err := ex.planner.ExecCfg().InternalExecutor.QueryRowEx(ctx, "select database comment statement", nil,
			sqlbase.InternalExecutorSessionDataOverride{Database: "defaultdb", User: security.RootUser}, selectStmt)
		if err != nil {
			res.SetError(errors.Errorf("%v error: %v", selectStmt, err.Error()))
			return nil
		}
		if len(row) == 0 {
			res.SetError(errors.Errorf("The database %v has not been created or has been deleted.", string(exp.Database)))
			return nil
		}
		if row[0] != tree.DNull {
			comments := string(tree.MustBeDString(row[0]))
			commentSQL := "COMMENT ON DATABASE " + string(exp.Database) + " IS '" + comments + "';"
			sqlDB = sqlDB + "\n" + commentSQL
			findComment = true
		}
	}

	if expOpts.withPrivileges {
		// get GRANT ON DATABASE
		dbSQL, err := getDBPrivileges(ctx, planner, DatabaseName)
		if err != nil {
			res.SetError(err)
			return nil
		}
		if dbSQL != nil {
			for _, sql := range dbSQL {
				sqlDB = sqlDB + "\n" + sql
			}
		}
	}

	if ex.server.cfg.TestingKnobs.BeforeGetTablesName != nil {
		ex.server.cfg.TestingKnobs.BeforeGetTablesName(ctx, planner.stmt.String())
	}

	var tableNames []*tree.TableName
	var err error
	if expOpts.withComment && !findComment {
		tableNames, findComment, err = getTablesNameByDBWithFindComment(
			planner.ExtendedEvalContext().Ctx(),
			planner.execCfg.InternalExecutor,
			planner.txn,
			exp.Database)
		if err != nil {
			res.SetError(err)
			return nil
		}
		if !findComment {
			res.SetError(errors.Errorf("DATABASE or TABLE or COLUMN without COMMENTS cannot be used 'WITH COMMENT'"))
			return nil
		}
	} else {
		tableNames, err = getTablesNameByDatabase(
			planner.ExtendedEvalContext().Ctx(),
			planner.execCfg.InternalExecutor,
			planner.txn,
			exp.Database)
		if err != nil {
			res.SetError(err)
			return nil
		}
	}

	if err := createStmtFunc(
		planner.EvalContext().Ctx(),
		dbPath,
		strings.Replace(exportFilePatternSQL, exportFilePatternPart, "meta", -1),
		sqlDB,
	); err != nil {
		res.SetError(err)
		return nil
	}

	f := tree.NewFmtCtx(tree.FmtExport)
	uri, err := url.Parse(dbPath)
	if err != nil {
		res.SetError(err)
		return nil
	}
	relPath := uri.Path
	// set planner
	planner.tableName.CatalogName = exp.Database
	planner.tableName.ExplicitCatalog = true
	// create_stmt of schema
	schemas := make(map[string]struct{}, 0)
	for _, tbl := range tableNames {
		// skip public schema
		if tbl.SchemaName != tree.PublicSchemaName {
			_, found := schemas[string(tbl.SchemaName)]
			if !found {
				// create schema statement
				uri.Path = path.Join(relPath, string(tbl.SchemaName))
				schemaName := string(tbl.SchemaName)
				if ContainsUpperCase(schemaName) {
					schemaName = "\"" + schemaName + "\""
				}
				sqlSC := "CREATE SCHEMA " + schemaName + ";" + "\n"
				schemas[string(tbl.SchemaName)] = struct{}{}
				if expOpts.withPrivileges {
					// get GRANT ON SCHEMA
					scSQL, err := getSCPrivileges(ctx, planner, string(tbl.SchemaName), tbl.Catalog())
					if err != nil {
						res.SetError(err)
						return nil
					}
					if scSQL != nil {
						for _, sql := range scSQL {
							sqlSC = sqlSC + sql + "\n"
						}
					}
				}
				if err := createStmtFunc(
					planner.EvalContext().Ctx(),
					uri.String(),
					strings.Replace(exportFilePatternSQL, exportFilePatternPart, "meta", -1),
					sqlSC,
				); err != nil {
					res.SetError(err)
					return nil
				}
			}
		}
		uri.Path = path.Join(relPath, string(tbl.SchemaName), string(tbl.TableName))
		exp.File = tree.NewStrVal(uri.String())
		exp.Query = &tree.Select{
			Select: &tree.SelectClause{
				Exprs: []tree.SelectExpr{{
					Expr: tree.UnqualifiedStar{},
				}},
				From: tree.From{
					Tables: []tree.TableExpr{
						&tree.AliasedTableExpr{
							Expr: tbl,
						},
					},
				},
				TableSelect: true,
			},
		}
		// set planner
		exp.Format(f)
		planner.stmt.AST = exp
		planner.stmt.SQL = f.String()
		f.Reset()

		planner.tableName.TableName = tbl.TableName
		planner.tableName.SchemaName = tbl.SchemaName
		planner.tableName.ExplicitSchema = true

		err = ex.dispatchToExecutionEngine(ctx, planner, res)

		if res.Err() != nil {
			res.AppendNotice(res.Err())
		}
		if err != nil {
			res.AppendNotice(err)
		}
		// reset error
		res.SetError(nil)
	}
	return nil
}

// getTablesNameByDatabase is used to get all table names in the target database which name is dbName.
func getTablesNameByDatabase(
	ctx context.Context, exec *InternalExecutor, txn *kv.Txn, dbName tree.Name,
) ([]*tree.TableName, error) {
	// tables
	rows, err := exec.Query(ctx, "EXPORT", txn, fmt.Sprintf(`
				SELECT schema_name, descriptor_id, descriptor_name
				FROM %s.kwdb_internal.create_statements
				WHERE database_name = $1 AND descriptor_type = 'table'
				ORDER BY descriptor_id
			`, tree.NameString(string(dbName))), dbName)
	if err != nil {
		return nil, err
	}
	var tableNames []*tree.TableName
	for _, row := range rows {
		schema := string(tree.MustBeDString(row[0]))
		table := string(tree.MustBeDString(row[2]))
		tblName := &tree.TableName{}
		tblName.TableName = tree.Name(table)
		tblName.SchemaName = tree.Name(schema)
		tblName.CatalogName = dbName
		tblName.ExplicitCatalog = true
		tblName.ExplicitSchema = true
		tableNames = append(tableNames, tblName)
	}
	return tableNames, nil
}

// getTablesNameByDBWithFindComment is same as getTablesNameByDatabase. In addition, check whether comments exists.
func getTablesNameByDBWithFindComment(
	ctx context.Context, exec *InternalExecutor, txn *kv.Txn, dbName tree.Name,
) ([]*tree.TableName, bool, error) {
	rows, err := exec.Query(ctx, "EXPORT DB WITH COMMENTS", txn, fmt.Sprintf(`
				SELECT schema_name, descriptor_name, create_statement, descriptor_id
				FROM %s.kwdb_internal.create_statements
				WHERE database_name = $1 AND descriptor_type = 'table'
				ORDER BY descriptor_id
			`, tree.NameString(string(dbName))), dbName)
	if err != nil {
		return nil, false, err
	}
	var tableNames []*tree.TableName
	var findComment bool
	str := "COMMENT ON"
	for _, row := range rows {
		schema := string(tree.MustBeDString(row[0]))
		table := string(tree.MustBeDString(row[1]))
		tblName := &tree.TableName{}
		tblName.TableName = tree.Name(table)
		tblName.SchemaName = tree.Name(schema)
		tblName.CatalogName = dbName
		tblName.ExplicitCatalog = true
		tblName.ExplicitSchema = true
		tableNames = append(tableNames, tblName)
		tblCreate := new(string)
		*tblCreate = string(tree.MustBeDString(row[2]))
		if !findComment && strings.Contains(*tblCreate, str) {
			findComment = true
		}
	}
	return tableNames, findComment, nil
}

// checkExportOptions is used to check whether the export options is legal.
func checkExportOptions(
	exp *tree.Export, res RestrictedCommandResult, opts map[string]string,
) (exportOptions, error) {
	delimiter := ','
	var expOpts exportOptions
	chunkSize := ExportChunkSizeDefault
	var err error
	override, hasDelimiter := opts[exportOptionDelimiter]
	if hasDelimiter {
		delimiter, err = util.GetSingleRune(override)
		if err != nil {
			return expOpts, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid delimiter value")
		}
		if delimiter < 0 || delimiter > 127 {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, "delimiter exceeds the limit of the char type")
		}
		if err := CheckDelimiterValue(delimiter); err != nil {
			return expOpts, err
		}
	}
	_, onlyData := opts[exportOptionOnlyData]
	_, onlyMeta := opts[exportOptionOnlyMeta]
	_, colName := opts[exportOptionColumnsName]
	if onlyMeta && onlyData {
		return expOpts, errors.Errorf("can't use meta_only and data_only at the same time")
	}
	override, hasChunkSize := opts[exportOptionChunkSize]
	if hasChunkSize {
		chunkSize, err = strconv.Atoi(override)
		if err != nil {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, err.Error())
		}
		if chunkSize < 0 {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, "invalid csv chunk size")
		}
	}

	if onlyMeta && (hasDelimiter || hasChunkSize) {
		//warning delimiter/chunk_rows doesn't work
		res.WarningForExport(errors.New("delimiter/chunk_rows doesn't work with only_meta"))
	}
	NullEncoding := ""
	if override, ok := opts[exportOptionNullAs]; ok {
		if err := ImpExpCheckNullOpt(override); err != nil {
			return expOpts, err
		}
		NullEncoding = override
	}
	Enclosed := '"'
	override, hasEnclosed := opts[exportOptionEnclosed]
	if hasEnclosed {
		Enclosed, err = util.GetSingleRune(override)
		if err != nil {
			return expOpts, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid enclosed value")
		}
		if Enclosed < 0 || Enclosed > 127 {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, "enclosed exceeds the limit of the char type")
		}
		if err := CheckEnclosedValue(Enclosed); err != nil {
			return expOpts, err
		}
	}

	Escaped := '"'
	override, hasEscaped := opts[exportOptionEscaped]
	if hasEscaped {
		Escaped, err = util.GetSingleRune(override)
		if err != nil {
			return expOpts, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid escaped value")
		}
		if Escaped < 0 || Escaped > 127 {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, "escaped exceeds the limit of the char type")
		}
		if err := CheckEscapeValue(Escaped); err != nil {
			return expOpts, err
		}
	}

	Charset := "UTF-8"
	override, hasCharset := opts[exportOptionCharset]
	if hasCharset {
		_, ok := types.CsvOptionCharset[override]
		if !ok {
			return expOpts, pgerror.New(pgcode.InvalidParameterValue, "invalid charset value, value must be 'GBK''GB18030''BIG5''UTF-8'")
		}
		Charset = override
	}

	comment := false
	_, hasWithComment := opts[exportOptionComment]
	if hasWithComment {
		comment = true
	}

	_, privileges := opts[exportOptionPrivileges]

	if privileges && exp.Database == "" {
		selectClause, ok := exp.Query.Select.(*tree.SelectClause)
		if ok {
			if !selectClause.TableSelect {
				res.SetError(errors.Errorf("Export permission information does not support select queries"))
				return expOpts, err
			}
		}
	}
	optInfo := roachpb.CSVOptions{Comma: delimiter, NullEncoding: &NullEncoding, Enclosed: Enclosed, Escaped: Escaped, Charset: Charset}
	if err := CheckImpExpInfoConflict(optInfo); err != nil {
		return expOpts, err
	}
	expOpts = exportOptions{
		optInfo,
		chunkSize,
		onlyMeta,
		onlyData,
		colName,
		comment,
		privileges,
	}
	return expOpts, nil
}

// dispatchExportTSDB is same as dispatchExportDB, but for time series database.
// Time series database's create statement is different and its only include public schema.
// And the way to get time series table is also different from relational table, so we call the func getTablesNameByTSDatabase.
func (ex *connExecutor) dispatchExportTSDB(
	ctx context.Context, planner *planner, res RestrictedCommandResult, expOpts exportOptions,
) error {
	exp := planner.stmt.AST.(*tree.Export)
	dbPath := exp.File.(*tree.StrVal).RawString()
	// dispatch need super and normal tables, meta sql need child tables additionally
	disTableNames, creates, err := getTablesNameByTSDatabase(
		planner.ExtendedEvalContext().Ctx(),
		planner.execCfg.InternalExecutor,
		planner.txn,
		exp.Database)
	if err != nil {
		res.SetError(err)
		return nil
	}
	// write meta
	if !expOpts.onlyData {
		fileName := strings.Replace(exportFilePatternSQL, exportFilePatternPart, "meta", -1)
		// make storage
		var buf bytes.Buffer
		writer := csv.NewWriter(&buf)
		conf, err := cloud.ExternalStorageConfFromURI(dbPath)
		if err != nil {
			res.SetError(err)
			return nil
		}
		es, err := planner.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
		if err != nil {
			res.SetError(err)
			return nil
		}
		defer es.Close()
		// write create database
		DatabaseName := string(exp.Database)
		if ContainsUpperCase(DatabaseName) {
			DatabaseName = "\"" + DatabaseName + "\""
		}
		createDB := "CREATE TS DATABASE " + DatabaseName
		if _, err = writer.GetBufio().WriteString(createDB + ";" + "\n"); err != nil {
			res.SetError(err)
			return nil
		}

		findComment := false
		if expOpts.withComment {
			// add database comment
			selectStmt := fmt.Sprintf("select shobj_description(oid, 'pg_database') from pg_catalog.pg_database where datname = '%s';", string(exp.Database))
			row, err := ex.planner.ExecCfg().InternalExecutor.QueryRowEx(ctx, "select database comment statement", nil,
				sqlbase.InternalExecutorSessionDataOverride{Database: "defaultdb", User: security.RootUser}, selectStmt)
			if err != nil {
				res.SetError(errors.Errorf("%v error: %v", selectStmt, err.Error()))
				return nil
			}
			if len(row) == 0 {
				res.SetError(errors.Errorf("The database %v has not been created or has been deleted.", string(exp.Database)))
				return nil
			}
			if row[0] != tree.DNull {
				comments := string(tree.MustBeDString(row[0]))
				if _, err := writer.GetBufio().WriteString("COMMENT ON DATABASE " + string(exp.Database) + " IS '" + comments + "';" + "\n"); err != nil {
					res.SetError(err)
					return nil
				}
				findComment = true
			}
		}

		for _, create := range creates {
			if expOpts.withComment {
				str := "COMMENT ON"
				if !findComment && strings.Contains(*create, str) {
					findComment = true
				}
				if _, err = writer.GetBufio().WriteString(*create + ";" + "\n"); err != nil {
					res.SetError(err)
					return nil
				}
			} else {
				tableCreate := strings.Split(*create, ";")
				if _, err = writer.GetBufio().WriteString(tableCreate[0] + ";" + "\n"); err != nil {
					res.SetError(err)
					return nil
				}
			}
		}
		if expOpts.withComment && !findComment {
			res.SetError(errors.Errorf("DATABASE or TABLE or COLUMN without COMMENTS cannot be used 'WITH COMMENT'"))
			return nil
		}
		for _, tableName := range disTableNames {
			indexs, err := ExportCreateIndexStmtsWithoutTableDesc(ctx, tableName, *planner)
			if err != nil {
				res.SetError(err)
				return nil
			}
			for _, index := range indexs {
				if _, err = writer.GetBufio().WriteString(index + "\n"); err != nil {
					res.SetError(err)
					return nil
				}
			}
		}
		if expOpts.withPrivileges {
			// get GRANT ON DATABASE
			dbSQL, err := getDBPrivileges(ctx, planner, DatabaseName)
			if dbSQL != nil {
				for _, sql := range dbSQL {
					if _, err = writer.GetBufio().WriteString(sql + "\n"); err != nil {
						res.SetError(err)
						return nil
					}
				}
			}
			for _, tbName := range disTableNames {
				// get GRANT ON TABLE
				tbSQL, err := getTBPrivileges(ctx, planner, tbName.TableName.String(), tbName.SchemaName.String(), tbName.CatalogName.String())
				if err != nil {
					res.SetError(err)
					return nil
				}
				if tbSQL != nil {
					for _, sql := range tbSQL {
						if _, err = writer.GetBufio().WriteString(sql + "\n"); err != nil {
							res.SetError(err)
							return nil
						}
					}
				}
			}
		}

		writer.Flush()
		if err = es.WriteFile(ctx, fileName, bytes.NewReader(buf.Bytes())); err != nil {
			res.SetError(err)
			return nil
		}
	}
	f := tree.NewFmtCtx(tree.FmtExport)
	uri, err := url.Parse(dbPath)
	if err != nil {
		res.SetError(err)
		return nil
	}
	relPath := uri.Path
	// set planner
	planner.tableName.CatalogName = exp.Database
	planner.tableName.ExplicitCatalog = true
	isSucceed := true
	for _, tbl := range disTableNames {
		uri.Path = path.Join(relPath, string(tbl.SchemaName), string(tbl.TableName))
		exp.File = tree.NewStrVal(uri.String())
		exp.Query = &tree.Select{
			Select: &tree.SelectClause{
				Exprs: []tree.SelectExpr{{
					Expr: tree.UnqualifiedStar{},
				}},
				From: tree.From{
					Tables: []tree.TableExpr{
						&tree.AliasedTableExpr{
							Expr: tbl,
						},
					},
				},
				TableSelect: true,
			},
		}
		// set planner
		exp.Format(f)
		planner.stmt.AST = exp
		planner.stmt.SQL = f.String()
		f.Reset()

		planner.tableName.TableName = tbl.TableName
		// ts database only contains public schema
		planner.tableName.SchemaName = tbl.SchemaName
		planner.tableName.ExplicitSchema = true
		planner.stmt.AST.(*tree.Export).IsTS = true
		err = ex.dispatchToExecutionEngine(ctx, planner, res)
		if res.Err() != nil {
			isSucceed = false
			res.AppendNotice(res.Err())
		}
		if err != nil {
			isSucceed = false
			res.AppendNotice(err)
		}
		// reset error
		res.SetError(nil)
	}
	var row tree.Datums
	if isSucceed {
		row = tree.Datums{
			tree.NewDString("succeed"),
		}
	} else {
		row = tree.Datums{
			tree.NewDString("fail"),
		}
	}
	res.SetColumns(ctx, sqlbase.ExportTsColumns)
	if err := res.AddRow(ctx, row); err != nil {
		res.AppendNotice(err)
	}
	return nil
}

// getTablesNameByTSDatabase is same as getTablesNameByDatabase, but for getting time series table.
// Module(Super) tables and time series tables are in kwdb_internal.create_statements.
// Instance(Child) tables are in kwdb_internal.instance_statement.
func getTablesNameByTSDatabase(
	ctx context.Context, exec *InternalExecutor, txn *kv.Txn, dbName tree.Name,
) ([]*tree.TableName, []*string, error) {
	// timeseries table and super table
	rows, err := exec.Query(ctx, "EXPORT TS IN DB", txn, fmt.Sprintf(`
				SELECT schema_name, descriptor_name, create_statement, descriptor_id
				FROM %s.kwdb_internal.create_statements
				WHERE database_name = $1 AND descriptor_type = 'table'
				ORDER BY descriptor_id
			`, tree.NameString(string(dbName))), dbName)
	if err != nil {
		return nil, nil, err
	}
	var tableNames []*tree.TableName
	var tblCreates []*string
	for _, row := range rows {
		schema := string(tree.MustBeDString(row[0]))
		table := string(tree.MustBeDString(row[1]))
		tblName := &tree.TableName{}
		tblName.TableName = tree.Name(table)
		tblName.SchemaName = tree.Name(schema)
		tblName.CatalogName = dbName
		tblName.ExplicitCatalog = true
		tblName.ExplicitSchema = true
		tableNames = append(tableNames, tblName)
		tblCreate := new(string)
		*tblCreate = string(tree.MustBeDString(row[2]))
		tblCreates = append(tblCreates, tblCreate)
	}
	// TODO(xy): not support export super and child table now
	// child table
	/*exec.SetSessionData(&sessiondata.SessionData{Database: dbName.String(), DistSQLMode: sessiondata.DistSQLOn})
	  defer exec.SetSessionData(new(sessiondata.SessionData))
	  rows, err = exec.Query(ctx, "EXPORT CHILD IN DB", txn, fmt.Sprintf(`
	  			SELECT statement FROM %s.kwdb_internal.instance_statement
	  			WHERE db = $1
	  		`, tree.NameString(string(dbName))), dbName)
	  if err != nil {
	  	return nil, nil, err
	  }
	  for _, row := range rows {
	  	tblCreate := new(string)
	  	*tblCreate = string(tree.MustBeDString(row[0]))
	  	tblCreates = append(tblCreates, tblCreate)
	  }*/
	return tableNames, tblCreates, nil
}
