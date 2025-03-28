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
	"io/ioutil"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// checkAndGetDetailsInDatabase parse files[0] to stmts.
// The first return parameter indicates whether time series data is being imported.
// The second return parameter represents the database name.
// The third return parameter represents the schema names.
// The fourth return parameter represents tableDetails.
// If time series data is imported, all SQL statements are written in the meta.sql directory in the root directory.
// If the data is imported relationally, the SQL of each table is written in the meta.sql directory of the corresponding subdirectory/.
func checkAndGetDetailsInDatabase(
	ctx context.Context, p sql.PlanHookState, files []string, withComment bool, withPrivileges bool,
) (bool, string, []string, []sqlbase.ImportTable, string, []string, error) {
	var err error
	// The whole database import supports only one file directory
	if len(files) != 1 {
		return false, "", nil, nil, "", nil, errors.Errorf("Database import does not support multiple file paths，%q", files)
	}
	dbPath := files[0]

	externalStorageFromURI := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	dbStore, err := externalStorageFromURI(ctx, dbPath+string(os.PathSeparator))
	if err != nil {
		return false, "", nil, nil, "", nil, err
	}
	defer dbStore.Close()
	// read DB SQL file
	reader, err := dbStore.ReadFile(ctx, "meta.sql")
	if err != nil {
		return false, "", nil, nil, "", nil, err
	}
	defer reader.Close()
	databaseDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return false, "", nil, nil, "", nil, err
	}
	stmts, err := parser.Parse(string(databaseDefStr))
	if err != nil {
		return false, "", nil, nil, "", nil, err
	}
	dbCreate, ok := stmts[0].AST.(*tree.CreateDatabase)
	if !ok {
		return false, "", nil, nil, "", nil, errors.New("The first sql CREATE DATABASE SQL has errors")
	}
	databaseComment := ""
	var hasComment bool
	var hasPrivileges bool
	var scNames []string
	var tableDetails []sqlbase.ImportTable
	var hasTableComment bool
	var hasTbPrivileges bool
	var isTs bool
	var privileges []string
	// timeseries database
	if dbCreate.EngineType == tree.EngineTypeTimeseries {
		isTs = true
		for i := range stmts {
			if i == 0 {
				// skip create db stmt
				continue
			}
			// Determine if there is a table creation statement
			tbCreate, ok := stmts[i].AST.(*tree.CreateTable)
			if ok {
				// The name of the database needs to be semantically parsed before it can be obtained.
				// The current location obtained is the result of syntax parsing,
				// so it is impossible to determine whether the database name is consistent
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
				tableDetails = append(tableDetails, sqlbase.ImportTable{Create: stmts[i].SQL, IsNew: true, TableName: tbCreate.Table.Table(), UsingSource: tbCreate.UsingSource.Table(), TableType: tbCreate.TableType, ColumnName: columnName})
				continue
			}
			// Check and obtain comments in the SQL file if WITH COMMENT
			if withComment {
				// Check if there is a COMMENT ON DATABASE, and if so, whether the database has been established
				dbComment, ok := stmts[i].AST.(*tree.CommentOnDatabase)
				if ok {
					if dbComment.Name == dbCreate.Name {
						databaseComment = stmts[i].SQL
						hasComment = true
						continue
					} else {
						return false, "", nil, nil, "", nil, errors.New("The database for COMMENT ON was not created")
					}
				}
				// Check if there is a COMMENT ON TABLE, and if so, whether the table has been established
				tbComment, ok := stmts[i].AST.(*tree.CommentOnTable)
				if ok {
					n, hasTable := isTableCreated(tbComment.Table.ToTableName().TableName, tableDetails)
					if !hasTable {
						return false, "", nil, nil, "", nil, errors.New("The table for COMMENT ON was not created")
					}
					tableDetails[n].TableComment = stmts[i].SQL
					hasComment = true
					continue
				}

				// Check if there is a COMMENT ON COLUMN, and if so, whether the column has been established
				colComment, ok := stmts[i].AST.(*tree.CommentOnColumn)
				if ok {
					n, hasTable := isTableCreated(colComment.TableName.ToTableName().TableName, tableDetails)
					if !hasTable {
						return false, "", nil, nil, "", nil, errors.New("The table containing this column for COMMENT has not been created")
					}
					hasColumn := isColumnCreated(colComment.ColumnName, tableDetails[n])
					if !hasColumn {
						return false, "", nil, nil, "", nil, errors.New("The column for COMMENT ON was not created")
					}
					tableDetails[n].ColumnComment = append(tableDetails[n].ColumnComment, stmts[i].SQL)
					hasComment = true
					continue
				}
			}
			// Check column index and set them in detail.
			colIndex, ok := stmts[i].AST.(*tree.CreateIndex)
			if ok {
				n, hasTable := isTableCreated(colIndex.Table.TableName, tableDetails)
				if !hasTable {
					return false, "", nil, nil, "", nil, errors.New("The table containing this column for index has not been created")
				}
				tableDetails[n].ColumnIndex = append(tableDetails[n].ColumnIndex, stmts[i].SQL)
				continue
			}
			if withPrivileges {
				// Check if there is a GRANT statement
				_, ok := stmts[i].AST.(*tree.Grant)
				if ok {
					privileges = append(privileges, stmts[i].SQL)
				}
				hasPrivileges = true
			}
		}
	} else {
		// relational database
		// Check and obtain comments in the SQL file if WITH COMMENT
		if withComment {
			for i := range stmts {
				if i == 0 {
					// skip create db stmt
					continue
				}
				// Check if there is a COMMENT ON DATABASE, and if so, whether the database has been established
				dbComment, ok := stmts[i].AST.(*tree.CommentOnDatabase)
				if ok {
					if dbComment.Name == dbCreate.Name {
						databaseComment = stmts[i].SQL
						hasComment = true
						continue
					} else {
						return false, "", nil, nil, "", nil, errors.New("The database for COMMENT ON was not created")
					}
				}
			}
		}
		if withPrivileges {
			for i := range stmts {
				if i == 0 {
					// skip create db stmt
					continue
				}
				// Check if there is a GRANT statement
				_, ok := stmts[i].AST.(*tree.Grant)
				if ok {
					privileges = append(privileges, stmts[i].SQL)
				}
				hasPrivileges = true
			}
		}
		scNames, tableDetails, hasTableComment, privileges, hasTbPrivileges, err = readTablesInDbFromStore(ctx, p, dbPath, stmts, withComment, withPrivileges)
		if err != nil {
			return false, "", nil, nil, "", nil, err
		}
		hasPrivileges = hasPrivileges || hasTbPrivileges
		hasComment = hasComment || hasTableComment
	}
	// Check if there are comments in SQL
	if withComment && !hasComment {
		return false, "", nil, nil, "", nil, errors.New("NO COMMENT statement in the SQL file")
	}
	// Check if there are privileges in SQL
	if withPrivileges && !hasPrivileges {
		return false, "", nil, nil, "", nil, errors.New("NO GRANT statement in the SQL file")
	}
	return isTs, dbCreate.Name.String(), scNames, tableDetails, databaseComment, privileges, nil
}

// isTableCreated used to determine whether a table with a specific name has a table creation statement,
// if so, Return his position in the array.
func isTableCreated(tableName tree.Name, tableDetails []sqlbase.ImportTable) (int, bool) {
	for i, tableDetail := range tableDetails {
		if string(tableName) == tableDetail.TableName {
			return i, true
		}
	}
	return 0, false
}

// isColumnCreated used to determine whether a column with a specific name exists in a specific table creation statement.
func isColumnCreated(columnName tree.Name, tableDetail sqlbase.ImportTable) bool {
	for _, column := range tableDetail.ColumnName {
		if string(columnName) == column {
			return true
		}
	}
	return false
}

// readTablesInDbFromStore reads the SQL file from the subdirectory and generate tableDetails.
func readTablesInDbFromStore(
	ctx context.Context,
	p sql.PlanHookState,
	dbPath string,
	stmts parser.Statements,
	OptComment bool,
	withPrivileges bool,
) ([]string, []sqlbase.ImportTable, bool, []string, bool, error) {
	var scNames []string
	var tableDetails []sqlbase.ImportTable
	var hasTableComment bool
	var hasPrivileges bool
	var hasTbPrivileges bool
	var hasScPrivileges bool
	var privileges []string
	externalStorageFromURI := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	dbPath = dbPath + string(os.PathSeparator)
	dbStore, err := externalStorageFromURI(ctx, dbPath)
	if err != nil {
		return nil, nil, false, nil, false, err
	}
	defer dbStore.Close()
	// dbPath, get all files in dbPath
	filesInDbDir, err := dbStore.ListFiles(ctx, "*")
	if err != nil {
		return nil, nil, false, nil, false, err
	}
	for _, scFolderName := range filesInDbDir {
		// find dbSQL
		if scFolderName == "meta.sql" {
			continue
		}
		// skip public, because we don't need to create public schema
		// In the scenario of exporting DB, each schema does not read meta.sql
		// and names schemaName directly with the folder.
		if scFolderName != tree.PublicSchema {
			scNames = append(scNames, scFolderName)
		}
		// schemaPath
		scPath := dbPath + scFolderName + string(os.PathSeparator)
		scStore, err := externalStorageFromURI(ctx, scPath)
		if err != nil {
			return nil, nil, false, nil, false, err
		}
		defer scStore.Close()
		// dbPath, get all files in dbPath
		filesInScDir, err := scStore.ListFiles(ctx, "*")
		if err != nil {
			return nil, nil, false, nil, false, err
		}
		for _, tbFolderName := range filesInScDir {
			if tbFolderName == "meta.sql" { // SC SQL
				// read SQL statement of SC SQL
				tbReader, err := scStore.ReadFile(ctx, tbFolderName)
				if err != nil {
					return nil, nil, false, nil, false, err
				}
				defer tbReader.Close()
				tableDefStr, err := ioutil.ReadAll(tbReader)
				if err != nil {
					return nil, nil, false, nil, false, err
				}
				stmts, err = parser.Parse(string(tableDefStr))
				if err != nil {
					return nil, nil, false, nil, false, err
				}
				// Check and obtain privileges in the SQL file if WITH Privileges
				for i := range stmts {
					if withPrivileges {
						// Check if there is a GRANT statement
						_, ok := stmts[i].AST.(*tree.Grant)
						if ok {
							privileges = append(privileges, stmts[i].SQL)
						}
						hasScPrivileges = true
					}
				}
				continue
			}
			// table SQL
			tbReader, err := scStore.ReadFile(ctx, tbFolderName+string(os.PathSeparator)+"meta.sql")
			if err != nil {
				return nil, nil, false, nil, false, err
			}
			defer tbReader.Close()
			tableDefStr, err := ioutil.ReadAll(tbReader)
			if err != nil {
				return nil, nil, false, nil, false, err
			}
			stmts, err = parser.Parse(string(tableDefStr))
			if err != nil {
				return nil, nil, false, nil, false, err
			}
			tbCreate, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				return nil, nil, false, nil, false, errors.New("expected CREATE TABLE statement in database file")
			}
			if err = checkCreateTableLegal(ctx, tbCreate); err != nil {
				return nil, nil, false, nil, false, err
			}
			var tableDetail []sqlbase.ImportTable
			var hasComment bool
			var tbPrivileges []string
			var hasTbPri bool
			_, tableDetail, hasComment, tbPrivileges, hasTbPri, err = checkAndGetDetailsInTable(ctx, p, stmts, OptComment, withPrivileges)
			if err != nil {
				return nil, tableDetails, false, nil, false, err
			}
			if tableDetail != nil {
				for _, detail := range tableDetail {
					detail.SchemaName = scFolderName
					tableDetails = append(tableDetails, detail)
				}
			}
			if tbPrivileges != nil {
				for _, privilege := range tbPrivileges {
					privileges = append(privileges, privilege)
				}
			}
			if hasComment {
				hasTableComment = hasComment
			}
			if hasTbPri {
				hasTbPrivileges = hasTbPri
			}
		}
	}
	hasPrivileges = hasScPrivileges || hasTbPrivileges
	if tableDetails == nil {
		return nil, nil, false, nil, false, errors.Errorf("cannot import an empty database")
	}
	return scNames, tableDetails, hasTableComment, privileges, hasPrivileges, nil
}
