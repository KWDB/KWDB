// Copyright 2019 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"github.com/pkg/errors"
)

type exportNode struct {
	optColumnsSlot

	source planNode

	expOpts   exportOptions
	fileName  string
	rows      *rowcontainer.RowContainer
	queryName string
	isTS      bool
}

func (e *exportNode) startExec(params runParams) error {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Next(params runParams) (bool, error) {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Values() tree.Datums {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Close(ctx context.Context) {
	e.source.Close(ctx)
}

const (
	exportOptionDelimiter   = "delimiter"
	exportOptionNullAs      = "nullas"
	exportOptionChunkSize   = "chunk_rows"
	exportOptionOnlyData    = "data_only"
	exportOptionOnlyMeta    = "meta_only"
	exportOptionForeignKey  = "foreign_key"
	exportOptionEnclosed    = "enclosed"
	exportOptionEscaped     = "escaped"
	exportOptionColumnsName = "column_name"
	exportOptionCharset     = "charset"
	exportOptionComment     = "comment"
	exportOptionPrivileges  = "privileges"
)

var exportOptionExpectValues = map[string]KVStringOptValidate{
	exportOptionChunkSize:   KVStringOptRequireValue,
	exportOptionDelimiter:   KVStringOptRequireValue,
	exportOptionNullAs:      KVStringOptRequireValue,
	exportOptionOnlyData:    KVStringOptRequireNoValue,
	exportOptionOnlyMeta:    KVStringOptRequireNoValue,
	exportOptionForeignKey:  KVStringOptRequireNoValue,
	exportOptionEnclosed:    KVStringOptRequireValue,
	exportOptionEscaped:     KVStringOptRequireValue,
	exportOptionColumnsName: KVStringOptRequireNoValue,
	exportOptionCharset:     KVStringOptRequireValue,
	exportOptionComment:     KVStringOptRequireNoValue,
	exportOptionPrivileges:  KVStringOptRequireNoValue,
}

// ExportChunkSizeDefault The default limit for the number of rows in an exported file
const ExportChunkSizeDefault = 100000
const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"
const exportFilePatternSQL = exportFilePatternPart + ".sql"

const createUser = "CREATE USER "
const createRole = "CREATE ROLE "

// ConstructExport is part of the exec.Factory interface.
func (ef *execFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	fileNameDatum, err := fileName.Eval(ef.planner.EvalContext())
	if err != nil {
		return nil, err
	}
	fileNameStr, ok := fileNameDatum.(*tree.DString)
	if !ok {
		return nil, errors.Errorf("expected string value for the file location")
	}

	optVals, err := evalStringOptions(ef.planner.EvalContext(), options, exportOptionExpectValues)
	if err != nil {
		return nil, err
	}

	tableSelect := getTableSelect(ef.planner.stmt.AST)
	csvOpts := roachpb.CSVOptions{}
	_, onlyData := optVals[exportOptionOnlyData]
	_, onlyMeta := optVals[exportOptionOnlyMeta]
	_, fk := optVals[exportOptionForeignKey]
	_, colName := optVals[exportOptionColumnsName]
	if !tableSelect && (onlyData || onlyMeta || fk) {
		return nil, errors.Errorf("cannot use option in query export")
	}
	if onlyData && onlyMeta {
		return nil, errors.Errorf("cannot use only_data and only_schema at the same time")
	}
	if onlyData && fk {
		return nil, errors.Errorf("cannot use foreign_key and only_data at the same time")
	}
	onlyData = onlyData || !tableSelect

	if ef.planner.execCfg.TestingKnobs.InjectErrorInConstructExport != nil {
		sn, ok := input.(*scanNode)
		if !ok {
			return nil, errors.Errorf("input is not scan node")
		}
		err = ef.planner.execCfg.TestingKnobs.InjectErrorInConstructExport(ef.planner.EvalContext().Ctx(), sn.desc.Name)
		if err != nil {
			return nil, err
		}
	}
	var IgnoreCheckComment bool
	if exp, ok := ef.planner.stmt.AST.(*tree.Export); ok {
		IgnoreCheckComment = exp.IgnoreCheckComment
	}
	_, withComment := optVals[exportOptionComment]
	_, withPrivileges := optVals[exportOptionPrivileges]
	if !onlyData {
		if err := ef.writeCreateFile(input, string(*fileNameStr), fk, withComment, withPrivileges, IgnoreCheckComment); err != nil {
			return nil, err
		}
	}

	if override, ok := optVals[exportOptionDelimiter]; ok {
		csvOpts.Comma, err = util.GetSingleRune(override)
		if err != nil {
			return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid delimiter value")
		}
		if csvOpts.Comma < 0 || csvOpts.Comma > 127 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "delimiter exceeds the limit of the char type")
		}
	} else {
		csvOpts.Comma = ','
	}

	if override, ok := optVals[exportOptionEnclosed]; ok {
		csvOpts.Enclosed, err = util.GetSingleRune(override)
		if err != nil {
			return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid enclosed value")
		}
		if csvOpts.Enclosed < 0 || csvOpts.Enclosed > 127 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "enclosed exceeds the limit of the char type")
		}
	} else {
		csvOpts.Enclosed = '"'
	}
	if override, ok := optVals[exportOptionEscaped]; ok {
		csvOpts.Escaped, err = util.GetSingleRune(override)
		if err != nil {
			return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid enclosed value")
		}
		if csvOpts.Escaped < 0 || csvOpts.Escaped > 127 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "enclosed exceeds the limit of the char type")
		}
	} else {
		csvOpts.Escaped = '"'
	}
	csvOpts.Charset = "UTF-8"
	override, hasCharset := optVals[exportOptionCharset]
	if hasCharset {
		_, ok := types.CsvOptionCharset[override]
		if !ok {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid charset value, value must be 'GBK''GB18030''BIG5''UTF-8'")
		}
		csvOpts.Charset = override
	}

	if err := CheckImpExpInfoConflict(csvOpts); err != nil {
		return nil, err
	}
	if override, ok := optVals[exportOptionNullAs]; ok {
		csvOpts.NullEncoding = &override
	}

	chunkSize := ExportChunkSizeDefault
	if override, ok := optVals[exportOptionChunkSize]; ok {
		chunkSize, err = strconv.Atoi(override)
		if err != nil {
			return nil, pgerror.New(pgcode.InvalidParameterValue, err.Error())
		}
		if chunkSize < 0 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid csv chunk size")
		}
	}
	var queryName string
	export, ok := ef.planner.stmt.AST.(*tree.Export)
	if !ok {
		return nil, errors.Errorf("planner's statement is not Export")
	}
	if export.Query.Select == nil {
		return nil, errors.Errorf("cannot export nil or strange select")
	}
	queryName = export.Query.Select.String()
	expOpts := exportOptions{
		csvOpts:   csvOpts,
		chunkSize: chunkSize,
		onlyMeta:  onlyMeta,
		onlyData:  onlyData,
		colName:   colName,
	}
	return &exportNode{
		source:    input.(planNode),
		fileName:  string(*fileNameStr),
		expOpts:   expOpts,
		queryName: queryName,
		isTS:      export.IsTS,
	}, nil
}

// writeCreateFile is used to check time series table and call the func writeRelationalMeta to write relational meta.
func (ef *execFactory) writeCreateFile(
	input exec.Node,
	file string,
	foreignKey bool,
	withComment bool,
	withPrivileges bool,
	IgnoreCheckComment bool,
) error {
	// Get params from ef and input node.
	p := ef.planner
	ctx := p.EvalContext().Ctx()
	if _, ok := input.(*scanNode); ok {
		return writeRelationalMeta(ctx, p, input, file, foreignKey, withComment, withPrivileges, IgnoreCheckComment)
	}
	// judge ts
	if mergeNode, ok := input.(*synchronizerNode); ok {
		if _, ok2 := mergeNode.plan.(*tsScanNode); ok2 {
			return nil
		}
		return errors.Errorf("input is not ts scan node")
	}
	if _, ok := input.(*renderNode); ok {
		return nil
	}
	return errors.Errorf("input is not scan node or merge node")
}

// writeRelationalMeta is used to write relational create statement.
// It's only for table.
// file is the path for write the meta file.
// foreignKey is the judgement for whether include foreignKey in create statement.
func writeRelationalMeta(
	ctx context.Context,
	p *planner,
	input exec.Node,
	file string,
	foreignKey bool,
	withComment bool,
	withPrivileges bool,
	IgnoreCheckComment bool,
) error {
	desc := input.(*scanNode).desc.TableDesc()
	tn := &p.tableName.TableName
	catalog := p.tableName.Catalog()

	// Get create_stmt.
	allDescs, err := p.Tables().getAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(allDescs, nil /* want all tables */)
	var displayOptions ShowCreateDisplayOptions
	if withComment {
		displayOptions = ShowCreateDisplayOptions{FKDisplayMode: OmitFKClausesFromCreate, IgnoreComments: false}
	} else {
		displayOptions = ShowCreateDisplayOptions{FKDisplayMode: OmitFKClausesFromCreate, IgnoreComments: true}
	}
	if foreignKey {
		displayOptions.FKDisplayMode = IncludeFkClausesInCreate
	}
	create, err := ShowCreateTable(ctx, p, tn, catalog, desc, lCtx, displayOptions)
	if err != nil {
		return err
	}
	// Write create_stmt to a file.
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	conf, err := cloud.ExternalStorageConfFromURI(file)
	if err != nil {
		return err
	}
	es, err := p.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()

	if withComment {
		str := "COMMENT ON"
		if !strings.Contains(create, str) && !IgnoreCheckComment {
			return errors.Errorf("TABLE or COLUMN without COMMENTS cannot be used 'WITH COMMENT'")
		}
	}
	if _, err := writer.GetBufio().WriteString(create + ";" + "\n"); err != nil {
		return err
	}
	if withPrivileges {
		// get GRANT ON TABLE
		tbSQL, err := getTBPrivileges(ctx, p, p.tableName.TableName.String(), p.tableName.SchemaName.String(), p.tableName.CatalogName.String())
		if err != nil {
			return err
		}
		if tbSQL != nil {
			for _, sql := range tbSQL {
				if _, err := writer.GetBufio().WriteString(sql + "\n"); err != nil {
					return err
				}
			}
		}
	}
	writer.Flush()
	bufBytes := buf.Bytes()
	part := "meta"
	filename := strings.Replace(exportFilePatternSQL, exportFilePatternPart, part, -1)
	return es.WriteFile(ctx, filename, bytes.NewReader(bufBytes))
}

func getDBPrivileges(ctx context.Context, p *planner, database string) ([]string, error) {
	// set sessionData
	p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: strings.Trim(database, "\"")})
	defer p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	dbPrivQuery := `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type
FROM ` + database + `.` + `information_schema.schema_privileges 
WHERE table_schema = 'information_schema' and grantee != 'admin' and grantee != 'root'`
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select database grants", nil, dbPrivQuery)
	if err != nil {
		return nil, err
	}
	var dbSQL []string
	for _, grantRow := range row {
		/*
			  database_name |    schema_name     | grantee | privilege_type
			----------------+--------------------+---------+-----------------
			  ys            | information_schema | u1      | SELECT
		*/
		dbName := strings.Trim(grantRow[0].String(), "'")
		userName := strings.Trim(grantRow[2].String(), "'")
		privilegeType := strings.Trim(grantRow[3].String(), "'")
		if ContainsUpperCase(dbName) {
			dbName = "\"" + dbName + "\""
		}
		if ContainsUpperCase(userName) {
			userName = "\"" + userName + "\""
		}
		create := fmt.Sprintf(`GRANT %s ON DATABASE %s TO %s;`, privilegeType, dbName, userName)
		dbSQL = append(dbSQL, create)
	}
	return dbSQL, nil
}

func getSCPrivileges(
	ctx context.Context, p *planner, schema string, database string,
) ([]string, error) {
	p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: strings.Trim(database, "\"")})
	defer p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	scPrivQuery := `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type
FROM "` + database + `".` + `information_schema.schema_privileges 
WHERE table_schema = '` + schema + `' and grantee != 'admin' and grantee != 'root'`
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select schema grants", nil, scPrivQuery)
	if err != nil {
		return nil, err
	}
	var scSQL []string
	for _, grantRow := range row {
		/*
			  database_name | schema_name | grantee | privilege_type
			----------------+-------------+---------+-----------------
			  YS            | SC          | r1      | DELETE
		*/
		scName := strings.Trim(grantRow[1].String(), "'")
		userName := strings.Trim(grantRow[2].String(), "'")
		privilegeType := strings.Trim(grantRow[3].String(), "'")
		if ContainsUpperCase(scName) {
			scName = "\"" + scName + "\""
		}
		if ContainsUpperCase(userName) {
			userName = "\"" + userName + "\""
		}
		create := fmt.Sprintf(`GRANT %s ON SCHEMA %s TO %s;`, privilegeType, scName, userName)
		scSQL = append(scSQL, create)
	}
	return scSQL, nil
}

func getTBPrivileges(
	ctx context.Context, p *planner, table string, schema string, database string,
) ([]string, error) {
	searchPath := sessiondata.SetSearchPath(p.CurrentSearchPath(), []string{strings.Trim(schema, "\"")})
	p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: strings.Trim(database, "\""), SearchPath: searchPath})
	defer p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	tablePrivQuery := `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name,
       grantee,
       privilege_type 
FROM "` + database + `".information_schema.table_privileges 
WHERE table_schema = '` + schema + `' and table_name = '` + table + `' and grantee != 'admin' and grantee != 'root'; `
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select table grants", nil, tablePrivQuery)
	if err != nil {
		return nil, err
	}
	var tbSQL []string
	for _, grantRow := range row {
		/*
			  database_name | schema_name | table_name | grantee | privilege_type
			----------------+-------------+------------+---------+-----------------
			  YS            | SC          | T          | r1      | INSERT
			  YS            | SC          | T          | u1      | SELECT
		*/
		dbName := strings.Trim(grantRow[0].String(), "'")
		scName := strings.Trim(grantRow[1].String(), "'")
		tbName := strings.Trim(grantRow[2].String(), "'")
		userName := strings.Trim(grantRow[3].String(), "'")
		privilegeType := strings.Trim(grantRow[4].String(), "'")
		if ContainsUpperCase(dbName) {
			dbName = "\"" + dbName + "\""
		}
		if ContainsUpperCase(scName) {
			scName = "\"" + scName + "\""
		}
		if ContainsUpperCase(tbName) {
			tbName = "\"" + tbName + "\""
		}
		if ContainsUpperCase(userName) {
			userName = "\"" + userName + "\""
		}
		create := fmt.Sprintf(`GRANT %s ON TABLE %s.%s.%s TO %s;`, privilegeType, dbName, scName, tbName, userName)
		tbSQL = append(tbSQL, create)
	}
	return tbSQL, nil
}

// Get cluster_setting from system.settings. Write them into clustersetting.sql
func getClusterSettingSQL(
	ctx context.Context, p *planner, file string, res RestrictedCommandResult,
) error {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	conf, err := cloud.ExternalStorageConfFromURI(file)
	if err != nil {
		return err
	}
	es, err := p.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()
	selectStmt := fmt.Sprintf("SELECT * FROM system.settings")
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select cluster setting statement", nil, selectStmt)
	if err != nil {
		return err
	}
	var rows int
	for _, clusterRow := range row {
		if clusterRow[0].String() == "'cluster.secret'" || clusterRow[0].String() == "'diagnostics.reporting.enabled'" || clusterRow[0].String() == "'version'" {
			continue
		}
		clusterName := strings.Trim(clusterRow[0].String(), "'")
		value := strings.Trim(clusterRow[1].String(), "'")
		cluster := fmt.Sprintf(`SET CLUSTER SETTING %s = '%s'`, clusterName, value)
		if _, err := writer.GetBufio().WriteString(cluster + ";" + "\n"); err != nil {
			return err
		}
		rows++
	}

	// Write cluster_setting to a file.
	writer.Flush()
	part := "clustersetting"
	filename := strings.Replace(exportFilePatternSQL, exportFilePatternPart, part, -1)
	if err := es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
		return err
	}
	var result tree.Datums
	result = tree.Datums{
		tree.NewDString("CLUSTER SETTING"),
		tree.NewDInt(tree.DInt(rows)),
		tree.NewDInt(tree.DInt(p.ExtendedEvalContext().NodeID)),
		tree.NewDInt(tree.DInt(1)),
	}
	res.SetColumns(ctx, sqlbase.ExportColumns)
	if err := res.AddRow(ctx, result); err != nil {
		res.AppendNotice(err)
	}
	return nil
}

// Get user/role from system.users. Write them into users.sql
func getUserSQL(ctx context.Context, p *planner, file string, res RestrictedCommandResult) error {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	conf, err := cloud.ExternalStorageConfFromURI(file)
	if err != nil {
		return err
	}
	es, err := p.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()
	// get username and rolename
	sysUserName, sysRoleName, err := getUserAndRoleFromSysUsers(ctx, p)
	if err != nil {
		return err
	}
	// get username, options, member_of
	userOptionMap, err := getUserAndMemberFromSysMembers(ctx, p)
	if err != nil {
		return err
	}
	var createStmt string
	// Splicing the SQL statements of CREATE USER and writing them to a file
	for _, username := range sysUserName {
		option := userOptionMap[username]
		/*
			  username |                    options                     | member_of
			-----------+------------------------------------------------+------------
			  u2       | NOLOGIN, VALID UNTIL=2025-01-16 00:00:00+00:00 | {}

			CREATE USER u2 ---> WITH ---> NOLOGIN ---> VALID UNTIL---> '2025-01-16 00:00:00+00:00'
		*/
		if option == "" {
			createStmt = fmt.Sprintf(createUser + username)
		} else {
			parts := strings.Split(option, ",")
			withOption := " WITH "
			for _, options := range parts {
				index := strings.Index(options, "=")
				if index == -1 {
					withOption = fmt.Sprintf(withOption + options)
				} else {
					withOption = fmt.Sprintf(withOption + options[:index] + " '" + options[index+1:] + "'")
				}
			}
			createStmt = fmt.Sprintf(createUser + username + withOption)
		}
		if _, err := writer.GetBufio().WriteString(createStmt + ";" + "\n"); err != nil {
			return err
		}
	}
	// Splicing the SQL statements of CREATE ROLE and writing them to a file
	for _, rolename := range sysRoleName {
		option := userOptionMap[rolename]
		if option == "" {
			createStmt = fmt.Sprintf(createRole + rolename + " " + "WITH LOGIN")
		} else {
			parts := strings.Split(option, ",")
			withOption := " WITH "
			for _, options := range parts {
				index := strings.Index(options, "=")
				if index == -1 {
					withOption = fmt.Sprintf(withOption + options + " ")
				} else {
					withOption = fmt.Sprintf(withOption + options[:index] + " '" + options[index+1:] + "' ")
				}
			}
			if !strings.Contains(option, "NOLOGIN") {
				withOption = fmt.Sprintf(withOption + " LOGIN")
			}
			createStmt = fmt.Sprintf(createRole + rolename + withOption)
		}
		if _, err := writer.GetBufio().WriteString(createStmt + ";" + "\n"); err != nil {
			return err
		}
	}

	// Splicing the SQL statements of GRANT TO and writing them to a file
	/*
		  role  | member | isAdmin
		--------+--------+----------
		  admin | root   |  true
		  r1    | u1     |  false
	*/
	selectStmt := fmt.Sprintf("SELECT * FROM system.role_members;")
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select user members", nil, selectStmt)
	if err != nil {
		return err
	}
	for _, memberRow := range row {
		if memberRow[0].String() == "'admin'" && memberRow[1].String() == "'root'" && memberRow[2].String() == "true" {
			continue
		}
		roleName := strings.Trim(memberRow[0].String(), "'")
		memberName := strings.Trim(memberRow[1].String(), "'")
		isAdmin := strings.Trim(memberRow[2].String(), "'")
		var create string
		if isAdmin != "true" {
			create = fmt.Sprintf(`GRANT %s TO %s `, roleName, memberName)
		} else {
			create = fmt.Sprintf(`GRANT %s TO %s WITH ADMIN OPTION`, roleName, memberName)
		}
		if _, err := writer.GetBufio().WriteString(create + ";" + "\n"); err != nil {
			return err
		}
	}

	// Write users to a file.
	writer.Flush()
	part := "users"
	filename := strings.Replace(exportFilePatternSQL, exportFilePatternPart, part, -1)
	if err := es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
		return err
	}
	var result tree.Datums
	result = tree.Datums{
		tree.NewDString("USERS"),
		tree.NewDInt(tree.DInt(len(sysUserName))),
		tree.NewDInt(tree.DInt(p.ExtendedEvalContext().NodeID)),
		tree.NewDInt(tree.DInt(1)),
	}
	res.SetColumns(ctx, sqlbase.ExportColumns)
	if err := res.AddRow(ctx, result); err != nil {
		res.AppendNotice(err)
	}
	return nil
}

// Read system.users, return USERNAMES and ROLENAMES
func getUserAndRoleFromSysUsers(ctx context.Context, p *planner) ([]string, []string, error) {
	selectStmt := fmt.Sprintf("SELECT * FROM system.users")
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "select users statement", nil, selectStmt)
	if err != nil {
		return nil, nil, err
	}
	var userNames []string
	var roleNames []string
	for _, userRow := range row {
		if userRow[0].String() == "'admin'" || userRow[0].String() == "'root'" {
			continue
		}
		userName := strings.Trim(userRow[0].String(), "'")
		if userRow[2].String() == "true" {
			roleNames = append(roleNames, userName)
		} else {
			userNames = append(userNames, userName)
		}
	}
	return userNames, roleNames, nil
}

// Read show users, return userOptionMap
func getUserAndMemberFromSysMembers(ctx context.Context, p *planner) (map[string]string, error) {
	selectStmt := fmt.Sprintf("SHOW USERS")
	row, err := p.ExecCfg().InternalExecutor.Query(ctx, "show users", nil, selectStmt)
	if err != nil {
		return nil, err
	}
	userOptionMap := make(map[string]string)
	for _, userRow := range row {
		if userRow[0].String() == "'admin'" || userRow[0].String() == "'root'" {
			continue
		}
		userName := strings.Trim(userRow[0].String(), "'")
		options := strings.Trim(userRow[1].String(), "'")
		userOptionMap[userName] = options
	}
	return userOptionMap, nil
}

// writeTimeSeriesMeta is same as writeRelationalMeta, but for time series table.
// file is the path for write the meta file.
// tableType is the type of the time series table.
// It's different to get different type tables' create statement.
func writeTimeSeriesMeta(
	ctx context.Context,
	p *planner,
	file string,
	tableDesc *ImmutableTableDescriptor,
	res RestrictedCommandResult,
	withComment bool,
	withPrivileges bool,
) error {
	tableName := p.tableName.TableName.String()
	catalog := p.tableName.Catalog()
	// init storage
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	conf, err := cloud.ExternalStorageConfFromURI(file)
	if err != nil {
		return err
	}
	es, err := p.execCfg.DistSQLSrv.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()
	// set sessionData
	p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: catalog, DistSQLMode: sessiondata.DistSQLOn})
	defer p.ExtendedEvalContext().ExecCfg.InternalExecutor.SetSessionData(new(sessiondata.SessionData))
	// Get create_stmt
	//count := 1
	switch tableDesc.TableType {
	// only write itself
	case tree.TimeseriesTable:
		selectStmt := fmt.Sprintf("SELECT create_statement FROM  %s.kwdb_internal.create_statements where database_name = '%s' and descriptor_name = '%s'", catalog, catalog, tableName)
		row, err := p.ExecCfg().InternalExecutor.QueryRow(ctx, "select table create statement", nil, selectStmt)
		if err != nil {
			return err
		}
		if len(row) == 0 {
			return errors.Errorf("table %v maybe has been deleted", tableName)
		}
		create := string(tree.MustBeDString(row[0]))
		if withComment {
			str := "COMMENT ON"
			if !strings.Contains(create, str) {
				return errors.Errorf("TABLE or COLUMN without COMMENTS cannot be used 'WITH COMMENT'")
			}
			if _, err := writer.GetBufio().WriteString(create + ";" + "\n"); err != nil {
				return err
			}
		} else {
			tableCreate := strings.Split(create, ";")
			// Since comment is controlled by "with comment", it is not exported by default,
			// only sql of creating table is exported.
			if _, err := writer.GetBufio().WriteString(tableCreate[0] + ";" + "\n"); err != nil {
				return err
			}
		}
		indexs, err := ExportCreateIndexStmtsWithTableDesc(ctx, tableDesc, tableName)
		if err != nil {
			return err
		}
		if indexs != nil {
			for _, index := range indexs {
				if _, err := writer.GetBufio().WriteString(index + "\n"); err != nil {
					return err
				}
			}
		}
		if withPrivileges {
			// get GRANT ON TABLE
			tbSQL, err := getTBPrivileges(ctx, p, tableName, p.tableName.SchemaName.String(), p.tableName.CatalogName.String())
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
	// write itself and it's all child
	case tree.TemplateTable:
		// TODO(xy): not support SUPER_TABLE now, drop child table has some problems,
		// so we can't select kwdb_internal.instance_statement now.
		return errors.Errorf("not support export module table")
		/*
			// super table
			row, err := p.ExecCfg().InternalExecutor.QueryRow(ctx, "select table create statement", nil,
				`SELECT create_statement FROM kwdb_internal.create_statements where database_name = $1 and descriptor_name = $2`,
				catalog, tableName)
			if err != nil {
				return err
			}
			create := string(tree.MustBeDString(row[0]))
			if _, err := writer.GetBufio().WriteString(create + ";" + "\n"); err != nil {
				return err
			}
			// child table
			rows, err := p.ExecCfg().InternalExecutor.Query(ctx, "select child table create statement", nil,
				`SELECT statement FROM kwdb_internal.instance_statement where template = $1`,
				tableName)
			if err != nil {
				return err
			}
			for _, row := range rows {
				count++
				create := string(tree.MustBeDString(row[0]))
				if _, err := writer.GetBufio().WriteString(create + ";" + "\n"); err != nil {
					return err
				}
			}
		*/
	case tree.InstanceTable:
		return errors.Errorf("can't only export instance table")
	}
	// Write create_stmt to a file.
	writer.Flush()
	part := "meta"
	filename := strings.Replace(exportFilePatternSQL, exportFilePatternPart, part, -1)
	return es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes()))
}

// getTableSelect is used to judge whether export is only for one table.
// It only returns true when export's input SQL uses the key TABLE.
// such as:
// export into csv "nodelocal://1/tb" from TABLE tb1; -- return true
// export into csv "nodelocal://1/tb" from select * from tb1; -- return false
func getTableSelect(AST tree.Statement) bool {
	if export, ok := AST.(*tree.Export); ok {
		if selectClause, ok := export.Query.Select.(*tree.SelectClause); ok {
			return selectClause.TableSelect
		}
	}
	return false
}

// getOnlyOneTableName is used to get only one table name,
// because time series table export only supports export one table now.
func getOnlyOneTableName(exp *tree.Export) (*tree.TableName, bool) {
	if selectClause, ok := exp.Query.Select.(*tree.SelectClause); ok {
		if selectClause.From.Tables != nil {
			if tableExpr, ok := selectClause.From.Tables[0].(*tree.AliasedTableExpr); ok {
				if tableName, ok := tableExpr.Expr.(*tree.TableName); ok {
					// judge whether only one table
					if len(selectClause.From.Tables) == 1 {
						return tableName, true
					}
					return tableName, false
				}
			}
		}
	}
	return nil, false
}

// ContainsUpperCase determines whether the characters in the string are capitalized.
func ContainsUpperCase(s string) bool {
	for _, r := range s {
		if unicode.IsUpper(r) {
			return true
		}
	}
	return false
}

// ExportCreateIndexStmtsWithTableDesc is to export a common tag index statement.
func ExportCreateIndexStmtsWithTableDesc(
	ctx context.Context, tableDesc *ImmutableTableDescriptor, tblName string,
) ([]string, error) {
	var createIndexStmts []string
	for _, idx := range tableDesc.Indexes {
		idxName := idx.Name
		colNames := ""
		for k, v := range idx.ColumnNames {
			if k == len(idx.ColumnNames)-1 {
				colNames += v
				break
			}
			colNames = colNames + v + ","
		}
		createIdxStmt := fmt.Sprintf("CREATE INDEX %s on %s(%s);", idxName, tblName, colNames)
		createIndexStmts = append(createIndexStmts, createIdxStmt)
	}
	return createIndexStmts, nil
}

// ExportCreateIndexStmtsWithoutTableDesc is to obtain table information
// and assemble to create a common tag index statement.
func ExportCreateIndexStmtsWithoutTableDesc(
	ctx context.Context, tblName *tree.TableName, p planner,
) ([]string, error) {
	tableDesc, err := p.ResolveUncachedTableDescriptor(ctx, tblName, true, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	return ExportCreateIndexStmtsWithTableDesc(ctx, tableDesc, tblName.String())
}
