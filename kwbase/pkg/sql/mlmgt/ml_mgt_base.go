// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type mlManagement struct {
	ctx     context.Context
	planner GenericPlannerAdapter
}

func newMLManagement(ctx context.Context, p GenericPlannerAdapter) (*mlManagement, error) {
	mgt := &mlManagement{ctx: ctx, planner: p}

	if err := mgt.checkMLEnv(); err != nil {
		return nil, err
	}

	return mgt, nil
}

func newMLManagementWithoutTxn(
	ctx context.Context, p GenericPlannerAdapter,
) (*mlManagement, error) {
	mgt := &mlManagement{ctx: ctx, planner: p}

	if err := mgt.checkMLEnv(); err != nil {
		return nil, err
	}

	return mgt, nil
}

func (mgt *mlManagement) checkMLEnv() error {
	// TODO. complement basic dependencies checking
	mlEnabled := Enabled.Get(mgt.planner.Settings().SV())
	if !mlEnabled {
		if mgt.planner.User() == security.RootUser || mgt.planner.User() == pb.MLUser {
			return nil
		}

		return pgerror.Newf(pgcode.InvalidParameterValue, "ml is not enabled, set cluster setting ml.enabled=true first")
	}

	return nil
}

func (mgt *mlManagement) extractRoleMembersWithAdminFlag(user string) ([]string, bool, error) {
	if len(strings.TrimSpace(user)) == 0 {
		return nil, false, errors.New("user name is empty")
	}

	if mgt.planner.Txn() == nil || !mgt.planner.Txn().IsOpen() {
		return nil, false, errors.AssertionFailedf("cannot extract RoleMembers without a txn")
	}

	// This build has no separate (sys-admin) deployment tier, so the admin
	// role check is always the one that applies.
	isAdmin, err := mgt.planner.UserHasAdminRole(mgt.ctx, user)
	if err != nil {
		return nil, false, err
	}

	if isAdmin {
		return nil, true, nil
	}

	roles := []string{user}

	memberOf, err := mgt.planner.MemberOfWithAdminOption(mgt.ctx, user)
	if err != nil {
		return nil, false, err
	}

	for role, isAdmin := range memberOf {
		if isAdmin {
			return nil, true, nil
		}
		roles = append(roles, role)
	}

	return roles, false, nil
}

func (mgt *mlManagement) isUserExist(user string) (bool, error) {
	if len(user) == 0 {
		return false, errors.New("user name is empty")
	}

	user = strings.ToLower(user)

	if rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"check-user-exist",
		mgt.planner.Txn(),
		"SELECT username FROM system.users WHERE username = $1",
		user,
	); err != nil {
		return false, err
	} else if len(rows) > 0 {
		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) checkSchemaExist(schema, database string) error {
	if schema == tree.PublicSchema {
		return nil
	}

	found, _, err := mgt.planner.LookupSchema(mgt.ctx, database, schema)
	if err != nil {
		return err
	}

	if !found {
		return pgerror.Newf(
			pgcode.InvalidSchemaName, "schema '%s' does not exist", schema,
		)
	}

	return nil
}

// GetCurrentSchema get current schema FROM user session.
func (mgt *mlManagement) getCurrentSchema() (string, error) {
	curDB := mgt.planner.CurrentDatabase()
	iter := mgt.planner.SessionData().SearchPath.IterWithoutImplicitPGSchemas()

	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, _, err := mgt.planner.LookupSchema(mgt.ctx, curDB, scName); found || err != nil {
			if err != nil {
				return "", err
			}
			return scName, nil
		}
	}

	return tree.PublicSchema, nil
}

// extractSchemaObjectName resolving schema,  model name or pipeline name
func (mgt *mlManagement) extractSchemaObjectName(name string) (string, string, error) {
	if len(name) == 0 {
		return "", "", pgerror.Newf(pgcode.InvalidParameterValue, "object name is empty")
	}

	var checkedSchema, checkedName string

	name = strings.ToLower(name)
	res := strings.Split(name, ".")

	if len(res) == 2 {
		checkedSchema = res[0]
		checkedName = res[1]
	} else if len(res) == 1 {
		checkedSchema = ""
		checkedName = res[0]
	} else {
		return "", "", pgerror.Newf(pgcode.InvalidParameterValue, "object name has 2 or more dots")
	}

	if len(checkedSchema) == 0 {
		schema, err := mgt.getCurrentSchema()
		if err != nil {
			return "", "", err
		}

		checkedSchema = schema
	} else {
		if err := mgt.checkSchemaExist(checkedSchema, mgt.planner.CurrentDatabase()); err != nil {
			return "", "", err
		}
	}

	return checkedSchema, checkedName, nil
}

func (mgt *mlManagement) extractTablePath(path string) (string, string, string, error) {
	var database, schema, table string
	var err error
	tablePath := strings.Split(path, ".")

	switch len(tablePath) {
	case 1:
		database = mgt.planner.CurrentDatabase()
		schema, err = mgt.getCurrentSchema()
		if err != nil {
			return "", "", "", err
		}
		table = tablePath[0]
	case 2:
		database = mgt.planner.CurrentDatabase()
		schema = tablePath[0]
		found, _, err := mgt.planner.LookupSchema(mgt.ctx, database, schema)
		if err != nil {
			return "", "", "", err
		}

		if !found {
			database = tablePath[0]
			if err = mgt.checkDatabaseExist(database); err != nil {
				return "", "", "", err
			}

			schema, err = mgt.getCurrentSchema()
			if err != nil {
				return "", "", "", err
			}
		}

		table = tablePath[1]
	case 3:
		database = tablePath[0]
		if err = mgt.checkDatabaseExist(database); err != nil {
			return "", "", "", err
		}

		schema = tablePath[1]
		if err = mgt.checkSchemaExist(schema, database); err != nil {
			return "", "", "", pgerror.Newf(
				pgcode.InvalidSchemaName, "target database or schema does not exist",
			)
		}

		table = tablePath[2]
	default:
		return "", "", "", pgerror.Newf(
			pgcode.InvalidParameterValue, "the output table name '%s' is invalid", path,
		)
	}

	return database, schema, table, nil
}

func (mgt *mlManagement) generateDataColumns(features *[]*pb.Feature, target *[]*pb.Target) string {
	var columns strings.Builder
	for i, f := range *features {
		if i != 0 {
			columns.WriteString(",")
		}
		columns.WriteString(f.Name)
	}

	if len(*target) != 0 {
		columns.WriteString(",")
		columns.WriteString((*target)[0].Name)
	}

	return columns.String()
}

func (mgt *mlManagement) buildDataSourceQuery(
	features *[]*pb.Feature, target *[]*pb.Target, inputData string,
) (string, error) {
	if len(*features) == 0 {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue, "empty feature is unsupported",
		)
	}

	query := inputData
	reg := regexp.MustCompile(`select\s+(.*)`)
	if !reg.MatchString(strings.ToLower(inputData)) {
		columns := mgt.generateDataColumns(features, target)
		query = fmt.Sprintf("SELECT %s FROM %s", columns, query)
	}

	stmt, err := parser.ParseOne(query)
	if err != nil {
		return "", err
	}

	if _, ok := stmt.AST.(*tree.Select); !ok {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue, "input data source '%s' is unsupported", inputData)
	}

	if err = mgt.checkDataSourceQuery(features, target, query); err != nil {
		return "", err
	}

	tableNames := mlUtils.getStatementTables(&stmt)

	for _, tn := range tableNames {

		prefix, err := ResolveTargetObject(mgt.ctx, mgt.planner, tn)
		if err != nil {
			return "", err
		}

		tn.CatalogName = tree.Name(prefix.Database.Name)
		tn.SchemaName = tree.Name(prefix.Schema.Name)
		tn.ExplicitCatalog = true
		tn.ExplicitSchema = true
	}

	//query = stmt.AST.String()

	if err = mgt.grantPrivilegeToMLUser(); err != nil {
		return "", err
	}

	return query, nil
}

func (mgt *mlManagement) getExSession() sqlbase.InternalExecutorSessionDataOverride {
	session := mgt.planner.SessionData()
	return sqlbase.InternalExecutorSessionDataOverride{
		User:            session.User,
		Database:        session.Database,
		ApplicationName: session.ApplicationName,
		SearchPath:      &session.SearchPath,
	}
}

// checkDataSourceQuery Check whether the user has permission of input query
// and whether the result format are consistent with the features
func (mgt *mlManagement) checkDataSourceQuery(
	features *[]*pb.Feature, target *[]*pb.Target, query string,
) error {
	query = fmt.Sprintf("SELECT * FROM (%s) LIMIT 1", query)
	rows, err := mgt.planner.InternalExecutor().QueryEx(
		mgt.ctx, "fetch-data-source-sample", mgt.planner.Txn(), mgt.getExSession(), query,
	)
	if err != nil {
		return err
	}

	if len(rows) < 1 {
		return pgerror.Newf(
			pgcode.InvalidParameterValue, "the result set of '%s' is empty",
			query,
		)
	}
	row := rows[0]

	var totalSize = len(*features)
	if len(*target) != 0 {
		totalSize++
	}

	if len(row) != totalSize {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"the column number of input data is not match with the ML feature definition, expect '%d', but got '%d'",
			totalSize,
			len(row),
		)
	}

	for i, f := range *features {
		value := row[i]
		if err := mlUtils.checkSupportDataType(f.Name, f.Type, value); err != nil {
			return err
		}
	}

	if len(*target) != 0 {
		t := (*target)[0]
		targetValue := row[len(row)-1]
		if err := mlUtils.checkSupportDataType(t.Name, t.Type, targetValue); err != nil {
			return err
		}
	}

	return nil
}

func (mgt *mlManagement) applyTableAction(database, _, table, writeMode string) error {
	switch strings.ToUpper(writeMode) {
	case pb.OutputTableActionReplace:
		if _, err := mgt.planner.InternalExecutor().Exec(
			mgt.ctx,
			"truncate-output-table",
			mgt.planner.Txn(),
			fmt.Sprintf("DELETE FROM %s.%s", database, table),
		); err != nil {
			return err
		}
	case pb.OutputTableActionAppend:

	case pb.OutputTableActionError:
		return pgerror.Newf(
			pgcode.FileAlreadyExists,
			"target table %s.%s already exists (action_if_exists=Error)",
			database, table,
		)
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue, "action_if_exists '%s' is unsupported", writeMode)
	}

	return nil
}

func (mgt *mlManagement) checkOutputTable(
	features *[]*pb.Feature, target *[]*pb.Target, inputTable, outputTable, writeMode string,
) ([]string, error) {
	outputDatabase, outputSchema, outputTableName, err := mgt.extractTablePath(outputTable)
	if err != nil {
		return []string{}, err
	}

	outTableExist, err := mgt.checkTableExist(outputDatabase, outputSchema, outputTableName)
	if err != nil {
		return []string{}, err
	}

	var outputColumns, outputTypes []string

	if outTableExist {
		if err = mgt.CheckOutputTablePrivilege(outputDatabase, outputSchema, outputTableName, writeMode); err != nil {
			return []string{}, err
		}

		if err = mgt.applyTableAction(outputDatabase, outputSchema, outputTableName, writeMode); err != nil {
			return []string{}, err
		}

		outputColumns, _, err = mgt.getOutputColumns(
			outputDatabase, outputSchema, outputTableName, features, target)
		if err != nil {
			return []string{}, err
		}

		return outputColumns, nil
	}

	var inputHasPrimary bool
	var primaryColumns, primaryTypes []string

	if mlUtils.IsTable(inputTable) {
		inputDatabase, inputSchema, inputTableName, err := mgt.extractTablePath(inputTable)
		if err != nil {
			return []string{}, err
		}

		primaryColumns, primaryTypes, err = mgt.getPrimaryColumns(inputDatabase, inputSchema, inputTableName)
		if err != nil {
			return []string{}, err
		}

		if len(primaryColumns) > 0 {
			inputHasPrimary = true
		}
	}

	if inputHasPrimary {
		outputColumns, outputTypes = primaryColumns, primaryTypes
	} else {
		for _, f := range *features {
			outputColumns = append(outputColumns, strings.ToLower(f.Name))
			outputTypes = append(outputTypes, f.Type)
		}
	}

	for _, f := range *target {
		outputColumns = append(outputColumns, strings.ToLower(f.Name))
		outputTypes = append(outputTypes, f.Type)
	}

	if err = mgt.createOutputTable(outputTable, outputColumns, outputTypes); err != nil {
		return []string{}, err
	}

	return outputColumns, nil
}

func (mgt *mlManagement) createOutputTable(
	table string, outputColumns, outputTypes []string,
) error {
	var build strings.Builder
	build.WriteString("CREATE TABLE ")
	build.WriteString(table)
	build.WriteString(" (")
	for i := range outputColumns {
		build.WriteString(outputColumns[i])
		build.WriteString(" ")
		build.WriteString(outputTypes[i])
		if i < len(outputColumns)-1 {
			build.WriteString(",")
		}
	}
	build.WriteString(" );")

	if _, err := mgt.planner.InternalExecutor().ExecEx(
		mgt.ctx,
		"create-table",
		mgt.planner.Txn(),
		mgt.getExSession(),
		build.String(),
	); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) getPrimaryColumns(
	database, schema, table string,
) (cols []string, types []string, err error) {
	tableName := tree.MakeTableNameWithSchema(tree.Name(database), tree.Name(schema), tree.Name(table))
	tableDesc, err := mgt.planner.ResolveTableDescriptorWithTableName(mgt.ctx, &tableName, true, int(ResolveRequiredTableDesc))
	if err != nil {
		return []string{}, []string{}, err
	}

	if len(tableDesc.PrimaryIndex.ColumnNames) == 0 {
		return []string{}, []string{}, nil
	}

	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		col, err := tableDesc.FindColumnByID(id)
		if err != nil {
			return []string{}, []string{}, err
		}

		if col.Hidden {
			continue
		}

		cols = append(cols, col.Name)
		types = append(types, col.Type.Name())
	}

	return cols, types, nil
}

func (mgt *mlManagement) getOutputColumns(
	database, schema, table string, features *[]*pb.Feature, target *[]*pb.Target,
) ([]string, []string, error) {
	if len(*target) == 0 || len(*features) == 0 {
		return []string{}, []string{}, pgerror.Newf(pgcode.InvalidParameterValue, "model target/features is empty")
	}

	tableName := tree.MakeTableNameWithSchema(tree.Name(database), tree.Name(schema), tree.Name(table))
	tableDesc, err := mgt.planner.ResolveTableDescriptorWithTableName(
		mgt.ctx, &tableName, true, int(ResolveRequiredTableDesc))
	if err != nil {
		return []string{}, []string{}, err
	}

	// get primary columns first
	var primaryColumns map[string]int
	primaryColumns = make(map[string]int)
	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		col, err := tableDesc.FindColumnByID(id)
		if err != nil {
			return []string{}, []string{}, err
		}

		if col.Hidden {
			continue
		}

		primaryColumns[col.Name] = 1
	}

	var outputColumns, outputTypes []string
	var hasTargetColumn bool

	for _, col := range tableDesc.GetColumns() {
		if col.Hidden {
			continue
		}

		if _, isPrimaryColumn := primaryColumns[col.Name]; !isPrimaryColumn && !col.Nullable {
			return []string{}, []string{}, errors.Newf(
				"table '%s.%s.%s' columns %s must nullable", database, schema, table, col.Name,
			)
		}

		tpy := ""
		for _, f := range *features {
			if strings.ToLower(f.Name) == strings.ToLower(col.Name) {
				if err = mlUtils.checkSupportColumnType(f.Name, f.Type, col.Type.Name()); err != nil {
					return []string{}, []string{}, err
				}

				tpy = col.Type.Name()
				break
			}
		}

		for _, f := range *target {
			if strings.ToLower(f.Name) == strings.ToLower(col.Name) {
				if err = mlUtils.checkSupportColumnType(f.Name, f.Type, col.Type.Name()); err != nil {
					return []string{}, []string{}, err
				}

				tpy = col.Type.Name()
				hasTargetColumn = true
				break
			}
		}

		if tpy == "" {
			return []string{}, []string{}, errors.Newf("column '%s' does not exist in model features or target", col.Name)
		}

		outputColumns = append(outputColumns, col.Name)
		outputTypes = append(outputTypes, tpy)
	}

	if !hasTargetColumn {
		return []string{}, []string{}, pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"output table %s doesn't has target column",
			tableName.String(),
		)
	}

	return outputColumns, outputTypes, nil
}

func (mgt *mlManagement) checkDatabaseExist(database string) error {
	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-database",
		mgt.planner.Txn(),
		`SELECT id FROM system.namespace WHERE "parentID"=0 AND name=$1`,
		database,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return errors.Newf("database '%s' does not exist", database)
	}

	return nil
}

func (mgt *mlManagement) checkTableExist(database, schema, table string) (bool, error) {

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-database",
		mgt.planner.Txn(),
		`SELECT table_id FROM kwdb_internal.tables WHERE state != 'DROP' AND database_name=$1 
AND schema_name=$2 AND name=$3`,
		database,
		schema,
		table,
	)
	if err != nil {
		return false, err
	}

	if len(rows) == 0 {
		return false, nil
	}

	return true, nil
}

func (mgt *mlManagement) CheckOutputTablePrivilege(
	database, schema, table, writeMode string,
) error {
	tableName := tree.MakeTableNameWithSchema(tree.Name(database), tree.Name(schema), tree.Name(table))
	tableDesc, err := mgt.planner.ResolveTableDescriptorWithTableName(mgt.ctx, &tableName, true, int(ResolveRequiredTableDesc))
	if err != nil {
		return err
	}

	if err = mgt.planner.CheckPrivilege(mgt.ctx, tableDesc, privilege.INSERT); err != nil {
		return err
	}

	if writeMode == pb.OutputTableActionReplace {
		if err = mgt.planner.CheckPrivilege(mgt.ctx, tableDesc, privilege.DELETE); err != nil {
			return err
		}
	}

	return nil
}

func (mgt *mlManagement) grantPrivilegeToMLUser() error {
	stmt := fmt.Sprintf(`GRANT %s TO %s`, mgt.planner.User(), pb.MLUser)
	user := security.RootUser
	if isSeparate := false; /* separate mode check removed during refactor */ isSeparate {
		user = security.RootUser
	}

	if _, err := mgt.planner.InternalExecutor().ExecEx(
		mgt.ctx,
		"grant-mluser-privilege",
		mgt.planner.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: user},
		stmt,
	); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) isVisibleInternalModel(schema, name string) bool {
	if schema == "public" && name == TSLLMName {
		return true
	}

	return false
}
