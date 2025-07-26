// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
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
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// findStreamByName check Stream exist from system table by stream name.
func (p *planner) findStreamByName(ctx context.Context, streamName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-stream-existing",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id, name FROM system.kwdb_streams WHERE name = $1`,
		streamName,
	)
	if err != nil {
		return false, err
	}

	// stream does not exist
	if len(row) == 0 {
		return false, nil
	}
	return true, nil
}

// findStreamByName load Stream from system table by stream name.
func (p *planner) loadStreamByName(
	ctx context.Context, streamName tree.Name,
) (*StreamMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name, create_by, create_at, status, target_table_id, 
job_id, parameters, run_info, source_table_id FROM system.kwdb_streams WHERE name = '%s'`, streamName)
	return p.loadStreamMetadata(ctx, stmt)
}

// loadStreamByID load Stream from system table by stream ID.
func (p *planner) loadStreamByID(ctx context.Context, streamID uint64) (*StreamMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name, create_by, create_at, status, target_table_id,
job_id, parameters, run_info, source_table_id FROM system.kwdb_streams WHERE id = %d`, streamID)
	return p.loadStreamMetadata(ctx, stmt)
}

// loadStreamByID load Stream from system table by SQL.
func (p *planner) loadStreamMetadata(ctx context.Context, stmt string) (*StreamMetadata, error) {
	var metadata StreamMetadata
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"load-stream",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return nil, err
	}

	// stream does not exist
	if len(row) == 0 {
		return nil, nil
	}

	metadata.id = uint64(tree.MustBeDInt(row[0]))
	metadata.name = tree.Name(tree.MustBeDString(row[1]))
	metadata.createBy = string(tree.MustBeDString(row[2]))
	metadata.createAt = tree.MustBeDTimestamp(row[3])
	metadata.status = string(tree.MustBeDString(row[4]))
	metadata.targetTableID = uint64(tree.MustBeDInt(row[5]))
	metadata.jobID = int64(tree.MustBeDInt(row[6]))
	metadata.parameters = tree.MustBeDJSON(row[7]).JSON
	metadata.runInfo = tree.MustBeDJSON(row[8]).JSON
	metadata.sourceTableID = uint64(tree.MustBeDInt(row[9]))

	err = metadata.Decode()
	if err != nil {
		return nil, err
	}

	return &metadata, nil
}

// checkStreamPrivilege verifies if the user has `privilege` on `stream`.
func (p *planner) checkStreamPrivilege(
	ctx context.Context,
	tableDesc sqlbase.DescriptorProto,
	streamPrivilegeKind privilege.Kind,
	tablePrivilegeKind privilege.Kind,
	createBy string,
	streamName string,
) error {
	// Verify user has system admin role
	isAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	if isAdmin {
		return nil
	}

	// Verify if the current user is the stream creator
	if createBy != p.User() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege on stream %s",
			p.User(), streamPrivilegeKind, streamName)
	}

	// Verify the table privilege meets the permission requirement of stream action (CREATE/ALTER/DROP)
	if tableDesc != nil {
		if err = p.CheckPrivilege(ctx, tableDesc, tablePrivilegeKind); err != nil {
			return err
		}
	}

	return nil
}

// checkStreamMax verifies if the count of streams reach limit.
func (p *planner) checkStreamMax(ctx context.Context) error {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"count-streams",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.kwdb_streams WHERE status = $1`,
		sqlutil.StreamStatusEnable,
	)
	if err != nil {
		return err
	}

	dint, _ := tree.AsDInt(row[0])
	maxNumber := TsStreamMaxActiveNumber.Get(p.execCfg.SV())
	if int64(dint) > maxNumber {
		return pgerror.Newf(pgcode.ProgramLimitExceeded, "The number of running streams reaches the limitation (%d)", maxNumber)
	}

	return nil
}

// checkTableUsedByStream verifies if the table used by stream.
func (p *planner) checkTableUsedByStream(
	ctx context.Context,
	tableID uint64,
	tableName string,
	cmdList []tree.AlterTableCmd,
	isCascade bool,
) error {
	typ := typeDropStreamTable
	for _, cmd := range cmdList {
		switch cmd.(type) {
		case *tree.AlterTableDropColumn,
			*tree.AlterTableRenameColumn,
			tree.ColumnMutationCmd,
			*tree.AlterTableAlterTagType,
			*tree.AlterTableDropTag,
			*tree.AlterTableRenameTag:
			typ = typeAlterStreamTable
			break
		case *tree.AlterTableAddColumn, *tree.AlterTableAddTag:
			typ = typeAddStreamTable
		default:
			typ = typeOtherStreamTable
		}
	}

	if typ == typeOtherStreamTable {
		return nil
	}

	query := fmt.Sprintf(`SELECT name,create_by FROM system.kwdb_streams WHERE target_table_id = $1 OR (source_table_id = $1`)
	if typ == typeAddStreamTable {
		query += fmt.Sprintf(" AND status = '%s')", sqlutil.StreamStatusEnable)
	} else {
		query += ")"
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-stream",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		tableID,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	streamList := make([]string, 0, len(rows))

	var privilegeErr string
	for _, row := range rows {
		streamName := string(tree.MustBeDString(row[0]))
		createBy := string(tree.MustBeDString(row[1]))
		// When a drop table cascade occurs, if the current user does not have the permission for delete stream,
		// the user cannot cascade delete the stream.
		if isCascade {
			if err = p.checkStreamPrivilege(ctx, nil, privilege.DROP, privilege.ALL,
				createBy, streamName); err == nil {
				continue
			} else {
				privilegeErr += ", " + err.Error()
			}
		}
		streamList = append(streamList, streamName)
	}

	if len(streamList) == 0 {
		return nil
	}

	return errors.Newf(
		"relation %q is used by stream [ %s ]%s", tableName, strings.Join(streamList, ", "), privilegeErr)
}

// makeStreamTableCommonInfo constructs StreamTableInfo and extracts tableIds for StreamParameters
func (p *planner) makeStreamTableCommonInfo(
	ctx context.Context, tableDesc *MutableTableDescriptor,
) (*cdcpb.CDCTableInfo, uint64, error) {
	var tableInfo cdcpb.CDCTableInfo

	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, tableDesc.ParentID)
	if err != nil {
		return &tableInfo, 0, err
	}
	tableInfo.Database = dbDesc.Name

	// cannot create another schema in ts database.
	tableInfo.Schema = "public"
	tableInfo.Table = tableDesc.Name

	tableInfo.TsColumnName = tableDesc.Columns[0].Name
	tableInfo.IsTsTable = tableDesc.IsTSTable()

	var primaryTags []string
	for _, col := range tableDesc.Columns {
		if col.IsPrimaryTagCol() {
			primaryTags = append(primaryTags, col.Name)
		}
	}
	tableInfo.PrimaryTagCols = primaryTags

	return &tableInfo, uint64(tableDesc.ID), nil
}

// checkTableUsedByStream verifies the source table.
func (p *planner) checkStreamQuerySourceTable(
	ctx context.Context, query *tree.Select,
) (*MutableTableDescriptor, error) {
	tableName, ok := getSourceTableName(query)
	if ok {
		sourceTableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tableName, true /*required*/, ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !sourceTableDesc.IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "stream is only used on ts table")
		}

		if err := p.checkStreamQuery(query, sourceTableDesc); err != nil {
			return nil, err
		}

		return sourceTableDesc, nil
	}

	return nil, errors.Newf("failed to extract source table name for stream query: %s", query.String())
}

// checkStreamQuery verifies the query of stream.
func (p *planner) checkStreamQuery(query *tree.Select, tableDesc *MutableTableDescriptor) error {
	if len(query.OrderBy) != 0 {
		return errors.Newf("cannot use ORDER BY clause in stream query")
	}

	if query.Limit != nil {
		return errors.Newf("cannot use LIMIT clause in stream query")
	}

	if query.With != nil {
		return errors.Newf("cannot use WITH clause in stream query")
	}

	selectClause, ok := query.Select.(*tree.SelectClause)
	if !ok {
		return errors.Newf("stream query is invalid: %s", query.String())
	}

	if len(selectClause.GroupBy) != 0 {
		return p.checkStreamAggQuery(selectClause, tableDesc)
	}

	return nil
}

// checkStreamAggQuery verifies the query with aggregator of stream.
func (p *planner) checkStreamAggQuery(
	selectClause *tree.SelectClause, tableDesc *MutableTableDescriptor,
) error {
	funcName, err := p.checkStreamQueryGroupBy(selectClause, tableDesc)
	if err != nil {
		return err
	}

	if err := p.checkStreamQueryBeginAndEndClause(
		selectClause, tableDesc.Columns[0].Name, funcName); err != nil {
		return err
	}

	for _, expr := range selectClause.Exprs {
		funcExpr, ok1 := expr.Expr.(*tree.FuncExpr)
		if ok1 {
			if funcExpr.Type == tree.DistinctFuncType {
				return errors.Newf("cannot use DISTINCT function in stream query")
			}
			if err := p.checkStreamQueryDistinct(funcExpr.Exprs); err != nil {
				return err
			}
		}
	}

	if selectClause.DistinctOn != nil {
		return errors.Newf("cannot use DISTINCT function in stream query")
	}

	if selectClause.Distinct {
		return errors.Newf("cannot use DISTINCT function in stream query")
	}

	return nil
}

// checkStreamQueryBeginAndEndClause verifies the query with begin and end of stream.
func (p *planner) checkStreamQueryBeginAndEndClause(
	selectClause *tree.SelectClause, tsTimestampColName, funcName string,
) error {
	if len(selectClause.Exprs) <= 2 {
		return errors.Errorf("invalid stream query: %s", selectClause.String())
	}

	firstExpr := selectClause.Exprs[0].Expr
	lastExpr := selectClause.Exprs[1].Expr

	errNotFirstFunc := errors.Errorf(
		"the first select clause of stream query must be the begin-timestamp of aggregation, "+
			"for example first(%s)/first_row(%s)/time_bucket(%s,'60s')",
		tsTimestampColName, tsTimestampColName, tsTimestampColName)
	errNotFirstRow := errors.Errorf(
		"the first select clause of stream query must be the first_row(%s) of aggregation function %s",
		tsTimestampColName, funcName)
	errNotLastFunc := errors.Errorf(
		"the second select clause of stream query must be the end-timestamp of aggregation, for example last(%s)/last_row(%s)",
		tsTimestampColName, tsTimestampColName)
	errNotLastRow := errors.Errorf(
		"the second select clause of stream query must be the last_row(%s) of aggregation function %s",
		tsTimestampColName, funcName)
	isTimeWindow := funcName == memo.TimeWindow
	buildFirstError := func() error {
		if isTimeWindow {
			return errNotFirstRow
		}

		return errNotFirstFunc
	}

	buildLastError := func() error {
		if isTimeWindow {
			return errNotLastRow
		}

		return errNotLastFunc
	}

	// check first/first_row/time_bucket function begin-timestamp of aggregation
	firstFunc, ok := firstExpr.(*tree.FuncExpr)
	if !ok {
		return buildFirstError()
	}

	firstFunName := firstFunc.Func.FunctionReference.FunctionName()

	if funcName == memo.TimeWindow && firstFunName != "first_row" {
		return buildFirstError()
	} else if !(firstFunName == "first" || firstFunName == "first_row" || firstFunName == "time_bucket") {
		return buildFirstError()
	}

	if len(firstFunc.Exprs) != 1 {
		return buildFirstError()
	}

	colName, ok := firstFunc.Exprs[0].(*tree.UnresolvedName)
	if !ok {
		return buildFirstError()
	}
	if colName.Parts[0] != tsTimestampColName {
		return buildFirstError()
	}

	// check last/last_row function for end-timestamp of aggregation
	lastFunc, ok := lastExpr.(*tree.FuncExpr)
	if !ok {
		return buildLastError()
	}

	lastFunName := lastFunc.Func.FunctionReference.FunctionName()

	if funcName == memo.TimeWindow && lastFunName != "last_row" {
		return buildLastError()
	} else if !(lastFunName == "last" || lastFunName == "last_row") {
		return buildLastError()
	}

	if len(lastFunc.Exprs) != 1 {
		return buildLastError()
	}

	colName, ok = lastFunc.Exprs[0].(*tree.UnresolvedName)

	if !ok {
		return buildLastError()
	}
	if colName.Parts[0] != tsTimestampColName {
		return buildLastError()
	}

	return nil
}

// checkStreamQueryGroupBy verifies the query with distinct of stream.
func (p *planner) checkStreamQueryDistinct(exprs tree.Exprs) error {
	for _, expr := range exprs {
		funcExpr, ok := expr.(*tree.FuncExpr)
		if ok {
			if funcExpr.Type == tree.DistinctFuncType {
				return errors.Newf("cannot use DISTINCT function in stream query")
			}
			return p.checkStreamQueryDistinct(funcExpr.Exprs)
		}
	}

	return nil
}

// checkStreamQueryGroupBy verifies the query with Group of stream.
func (p *planner) checkStreamQueryGroupBy(
	selectClause *tree.SelectClause, tableDesc *MutableTableDescriptor,
) (string, error) {
	var funcName string
	if len(selectClause.GroupBy) == 0 {
		return funcName, nil
	}

	if selectClause.Having != nil {
		return funcName, errors.Errorf("unsupported having expr: %s", selectClause.Having.Expr.String())
	}

	var groupByColumns []string
	includeRequiredFunction := false

	for _, expr := range selectClause.GroupBy {
		switch expr.(type) {
		case *tree.FuncExpr:
			funcExpr := expr.(*tree.FuncExpr)
			funcName = funcExpr.Func.FunctionName()
			switch funcName {
			case tree.FuncTimeBucket, memo.StateWindow, memo.CountWindow, memo.TimeWindow, memo.EventWindow, memo.SessionWindow:
				includeRequiredFunction = true
			default:
				return funcName, errors.Errorf("unsupported group by function in stream query: %s", funcName)
			}
		case *tree.UnresolvedName:
			funcExpr := expr.(*tree.UnresolvedName)
			groupByColumns = append(groupByColumns, funcExpr.String())
		default:
			return funcName, errors.Errorf("unsupported group by clause in stream query: %s", selectClause)
		}
	}

	if !includeRequiredFunction {
		return funcName, errors.Errorf("missing time_bucket or window function in GROUP BY cluster")
	}

	primaryTags := make(map[string]string)

	for _, col := range tableDesc.Columns {
		if col.IsPrimaryTagCol() {
			primaryTags[col.Name] = ""
		}
	}

	if len(groupByColumns) == 0 {
		return funcName, nil
	}

	for _, col := range groupByColumns {
		_, ok := primaryTags[col]
		if !ok {
			return funcName, errors.Errorf("the group by column %q of stream query is not a primary tag", col)
		}
	}

	if len(groupByColumns) != len(primaryTags) {
		return funcName, errors.Errorf("should include all primary tags in stream query: %s", selectClause)
	}

	return funcName, nil
}

// checkStreamTargetTableInfo verifies the target table.
func (p *planner) checkStreamTargetTableInfo(
	ctx context.Context,
	tableDesc *MutableTableDescriptor,
	para *sqlutil.StreamParameters,
	outTypes []types.T,
	query *tree.Select,
) ([]types.T, error) {
	colTypes := make([]types.T, len(outTypes))
	colIDs := make([]uint32, len(outTypes))
	colNames := make([]string, len(outTypes))
	distinctColNames := make(map[string]string, len(outTypes))

	if para.StreamSink.HasAgg && para.Options.ProcessHistory == sqlutil.StreamOptOn {
		targetTableCheckingStmt := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", para.TargetTable.Database, para.TargetTable.Table)

		row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
			ctx,
			"check-stream-target",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			targetTableCheckingStmt,
		)
		if err != nil {
			return nil, err
		}

		// target table is not empty
		if len(row) != 0 {
			return nil, errors.Newf("target table \"%s.%s\" must be empty if stream option 'process_history' is 'on'",
				para.TargetTable.Database, para.TargetTable.Table,
			)
		}
	}

	selectClause, ok := query.Select.(*tree.SelectClause)

	if !ok {
		return nil, errors.Newf("query is not supported by stream: %s", query.String())
	}

	hasAs := false
	missingAs := false
	for idx, expr := range selectClause.Exprs {
		if len(expr.As) != 0 {
			colNames[idx] = expr.As.String()
			hasAs = true

			distinctColNames[colNames[idx]] = ""
		} else {
			missingAs = true
			colNames[idx] = expr.Expr.String()
		}
	}

	if hasAs {
		if len(colNames) != len(distinctColNames) {
			if missingAs {
				return nil, errors.Newf("should provide alias name for all output columns in the select list of stream query")
			}

			return nil, errors.Newf("cannot use duplicated alias name in the select list of stream query")
		}

		colNameMap, tsColName, totalPrimaryTag := createColumnMap(tableDesc.Columns)

		includeTsCol := false
		primaryTagCount := 0
		for idx, colName := range colNames {
			if colName == tsColName {
				includeTsCol = true
			}

			colDest, hasCol := colNameMap[colName]
			if !hasCol {
				return nil, errors.Newf("the column %q doesn't exist in target table", colName)
			}
			colIDs[idx] = uint32(colDest.ID)
			colTypes[idx] = colDest.Type

			if colDest.IsPrimaryTagCol() {
				primaryTagCount++
			}
		}

		if tableDesc.IsTSTable() {
			if !includeTsCol {
				return nil, errors.Newf("the output of stream query (select list) should include the ts column %q ", tsColName)
			}

			if primaryTagCount != 0 && primaryTagCount != totalPrimaryTag {
				return nil, errors.Newf("the output of stream query (select list) should include all the primary tag columns")
			}
		}
	} else {
		colDescs := tableDesc.Columns
		targetColNumber := len(tableDesc.Columns)
		lastColumn := colDescs[targetColNumber-1]

		if !tableDesc.IsTSTable() {
			if strings.Contains(lastColumn.Name, "rowid") {
				targetColNumber = len(tableDesc.Columns) - 1 // exclude the last rowid column
			}
		}

		if len(outTypes) != targetColNumber {
			return nil, errors.Newf(
				"the output number of stream query doesn't match the target table: expected %d, got %d",
				len(outTypes), targetColNumber,
			)
		}

		for idx := 0; idx < targetColNumber; idx++ {
			colDest := colDescs[idx]

			colIDs[idx] = uint32(colDest.ID)
			colNames[idx] = colDest.Name
			colTypes[idx] = colDest.Type
		}
	}
	if err := checkTypeCompatible(outTypes, colTypes, colNames, tableDesc.IsTSTable()); err != nil {
		return nil, err
	}

	para.TargetTable.ColNames = colNames
	para.TargetTable.ColIDs = colIDs
	para.TargetStartColName = tableDesc.Columns[0].Name
	para.TargetEndColName = tableDesc.Columns[1].Name
	return colTypes, nil
}

// checkTypeCompatible verifies the type compatible from target and source table.
func checkTypeCompatible(
	source []types.T, target []types.T, colNames []string, isTsTable bool,
) error {
	for idx, sourceType := range source {
		targetType := target[idx]
		if err := checkType(sourceType, targetType, colNames[idx], isTsTable); err != nil {
			return err
		}
	}
	return nil
}

// checkTypeCompatible verifies the type compatible of column.
func checkType(source types.T, target types.T, colName string, isTsTable bool) error {
	var isCompatible bool
	var sourcePrecision int32
	var targetPrecision int32

	if isTsTable {
		switch target.Family() {
		case types.TimestampTZFamily:
			switch source.Family() {
			case types.TimestampTZFamily:
				sourcePrecision = source.Precision()
				targetPrecision = target.Precision()

				if sourcePrecision == targetPrecision {
					isCompatible = true
				}
			default:
			}

		case types.TimestampFamily:
			switch source.Family() {
			case types.TimestampFamily:
				sourcePrecision = source.Precision()
				targetPrecision = target.Precision()

				if sourcePrecision == targetPrecision {
					isCompatible = true
				}
			default:
			}

		case types.IntFamily:
			switch source.Family() {
			case types.IntFamily:
				isCompatible = true
			case types.DecimalFamily:
				isCompatible = true
			default:
			}

		case types.FloatFamily:
			switch source.Family() {
			case types.FloatFamily:
				isCompatible = true
			case types.DecimalFamily:
				isCompatible = true
			default:
			}

		case types.DecimalFamily:
			switch source.Family() {
			case types.DecimalFamily:
				isCompatible = true
			default:
			}

		case types.StringFamily:
			switch source.Family() {
			case types.StringFamily:
				isCompatible = true
			default:
			}

		case types.BoolFamily:
			switch source.Family() {
			case types.BoolFamily:
				isCompatible = true
			default:
			}

		case types.BytesFamily:
			switch source.Family() {
			case types.BytesFamily:
				isCompatible = true
			default:
			}
		default:

		}
	} else {
		switch target.Family() {
		case types.TimestampTZFamily:
			switch source.Family() {
			case types.TimestampTZFamily:
				sourcePrecision = source.Precision()
				targetPrecision = target.Precision()

				if sourcePrecision == targetPrecision {
					isCompatible = true
				}
			default:
			}

		case types.TimestampFamily:
			switch source.Family() {
			case types.TimestampFamily:
				sourcePrecision = source.Precision()
				targetPrecision = target.Precision()

				if sourcePrecision == targetPrecision {
					isCompatible = true
				}
			default:
			}
		case types.DecimalFamily:
			switch source.Family() {
			case types.DecimalFamily:
				isCompatible = true
			case types.IntFamily:
				isCompatible = true
			case types.FloatFamily:
				isCompatible = true
			default:
			}

		case types.StringFamily:
			switch source.Family() {
			case types.StringFamily:
				isCompatible = true
			default:
			}
		case types.BoolFamily:
			switch source.Family() {
			case types.BoolFamily:
				isCompatible = true
			default:
			}
		case types.BytesFamily:
			switch source.Family() {
			case types.BytesFamily:
				isCompatible = true
			default:
			}
		case types.IntFamily, types.FloatFamily:
			return errors.Errorf(
				"stream output type %q is not supported by stream query, should use 'Decimal' in target table",
				target.Name(),
			)
		default:

		}
	}

	if !isCompatible {
		if sourcePrecision != 0 || targetPrecision != 0 {
			return errors.Errorf(
				"stream output type is not compatible with target table, column name: %q, source type: \"%s(%d)\", target type: \"%s(%d)\"",
				colName, source.Name(), sourcePrecision, target.Name(), targetPrecision,
			)
		}

		return errors.Errorf(
			"stream output type is not compatible with target table, column name: %q, source type: %q, target type: %q",
			colName, source.Name(), target.Name(),
		)
	}

	return nil
}

// extractTargetTableInfoForStream get the types of target table's columns.
func (p *planner) extractTargetTableInfoForStream(
	ctx context.Context, parameters *sqlutil.StreamParameters,
) ([]types.T, error) {
	colTypes := make([]types.T, len(parameters.TargetTable.ColIDs))

	tableName := tree.MakeTableName(tree.Name(parameters.TargetTable.Database), tree.Name(parameters.TargetTable.Table))
	targetTableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tableName, true /*required*/, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	colMap := targetTableDesc.ColumnIdxMap()
	for idx, colID := range parameters.TargetTable.ColIDs {
		colDest := targetTableDesc.Columns[colMap[sqlbase.ColumnID(colID)]]
		colTypes[idx] = colDest.Type
	}

	return colTypes, nil
}

// getSourceTableName is used to get only one table name,
// because stream only supports one table now.
func getSourceTableName(query *tree.Select) (*tree.TableName, bool) {
	if selectClause, ok := query.Select.(*tree.SelectClause); ok {
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

// createColumnMap create ColumnMap from ColumnDescriptor list.
func createColumnMap(
	cols []sqlbase.ColumnDescriptor,
) (map[string]*sqlbase.ColumnDescriptor, string, int) {
	tsColName := cols[0].Name
	primaryTagCount := 0
	columnMap := make(map[string]*sqlbase.ColumnDescriptor, len(cols))

	for i, col := range cols {
		columnMap[cols[i].Name] = &cols[i]

		if col.IsPrimaryTagCol() {
			primaryTagCount++
		}
	}

	return columnMap, tsColName, primaryTagCount
}
