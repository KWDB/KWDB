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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// FindStreamByName check Stream exist from system table by stream name.
func FindStreamByName(ctx context.Context, p PlanHookState, streamName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-stream-existing",
		p.Txn(),
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

// LoadStreamByName load Stream from system table by stream name.
func LoadStreamByName(
	ctx context.Context, p PlanHookState, streamName tree.Name,
) (*metadata.StreamMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name, create_by, create_at, status, target_table_id, 
job_id, parameters, run_info, source_table_id FROM system.kwdb_streams WHERE name = '%s'`, streamName)
	return loadStreamMetadata(ctx, p, stmt)
}

// loadStreamByID load Stream from system table by stream ID.
func loadStreamByID(
	ctx context.Context, p PlanHookState, streamID uint64,
) (*metadata.StreamMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name, create_by, create_at, status, target_table_id,
job_id, parameters, run_info, source_table_id FROM system.kwdb_streams WHERE id = %d`, streamID)
	return loadStreamMetadata(ctx, p, stmt)
}

// loadStreamByID load Stream from system table by SQL.
func loadStreamMetadata(
	ctx context.Context, p PlanHookState, stmt string,
) (*metadata.StreamMetadata, error) {
	var metadata metadata.StreamMetadata
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"load-stream",
		p.Txn(),
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

	metadata.ID = uint64(tree.MustBeDInt(row[0]))
	metadata.Name = tree.Name(tree.MustBeDString(row[1]))
	metadata.CreateBy = string(tree.MustBeDString(row[2]))
	metadata.CreateAt = tree.MustBeDTimestamp(row[3])
	metadata.Status = string(tree.MustBeDString(row[4]))
	metadata.TargetTableID = uint64(tree.MustBeDInt(row[5]))
	metadata.JobID = int64(tree.MustBeDInt(row[6]))
	metadata.Parameters = tree.MustBeDJSON(row[7]).JSON
	metadata.RunInfo = tree.MustBeDJSON(row[8]).JSON
	metadata.SourceTableID = uint64(tree.MustBeDInt(row[9]))

	err = metadata.Decode()
	if err != nil {
		return nil, err
	}

	return &metadata, nil
}

// CheckStreamPrivilege verifies if the user has `privilege` on `stream`.
func CheckStreamPrivilege(
	ctx context.Context,
	p *GenericPlanner,
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

// CheckStreamMax verifies if the count of streams reach limit.
func CheckStreamMax(ctx context.Context, p PlanHookState) error {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"count-streams",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.kwdb_streams WHERE status = $1`,
		sqlutil.StreamStatusEnable,
	)
	if err != nil {
		return err
	}

	dint, _ := tree.AsDInt(row[0])
	maxNumber := sqlconst.TsStreamMaxActiveNumber.Get(p.ExecCfg().SV())
	if int64(dint) > maxNumber {
		return pgerror.Newf(pgcode.ProgramLimitExceeded, "The number of running streams reaches the limitation (%d)", maxNumber)
	}

	return nil
}

// CheckTableUsedByStream verifies if the table used by stream.
func CheckTableUsedByStream(
	ctx context.Context,
	p *GenericPlanner,
	tableID uint64,
	tableName string,
	cmdList []tree.AlterTableCmd,
	isCascade bool,
) error {
	typ := sqlconst.TypeDropStreamTable
	for _, cmd := range cmdList {
		switch cmd.(type) {
		case *tree.AlterTableDropColumn,
			*tree.AlterTableRenameColumn,
			tree.ColumnMutationCmd,
			*tree.AlterTableAlterTagType,
			*tree.AlterTableDropTag,
			*tree.AlterTableRenameTag:
			typ = sqlconst.TypeAlterStreamTable
			break
		case *tree.AlterTableAddColumn, *tree.AlterTableAddTag:
			typ = sqlconst.TypeAddStreamTable
		default:
			typ = sqlconst.TypeOtherStreamTable
		}
	}

	if typ == sqlconst.TypeOtherStreamTable {
		return nil
	}

	query := fmt.Sprintf(`SELECT name,create_by FROM system.kwdb_streams WHERE target_table_id = $1 OR (source_table_id = $1`)
	if typ == sqlconst.TypeAddStreamTable {
		query += fmt.Sprintf(" AND status = '%s')", sqlutil.StreamStatusEnable)
	} else {
		query += ")"
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-stream",
		p.Txn(),
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
			if err = CheckStreamPrivilege(ctx, p, nil, privilege.DROP, privilege.ALL,
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

// MakeStreamTableCommonInfo constructs StreamTableInfo and extracts tableIds for StreamParameters
func MakeStreamTableCommonInfo(
	ctx context.Context, p PlanHookState, tableDesc *MutableTableDescriptor,
) (*cdcpb.CDCTableInfo, uint64, error) {
	var tableInfo cdcpb.CDCTableInfo

	if tableDesc.IsSparseTable() {
		return nil, 0, errors.Newf("sparse table is not supported by stream")
	}

	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.Txn(), tableDesc.ParentID)
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

// CheckStreamQuerySourceTable verifies the source table.
func CheckStreamQuerySourceTable(
	ctx context.Context, p PlanHookState, query *tree.Select,
) (*MutableTableDescriptor, error) {
	tableName, ok := GetSourceTableName(query)
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

		if err := CheckStreamQuery(query, p, sourceTableDesc); err != nil {
			return nil, err
		}

		return sourceTableDesc, nil
	}

	return nil, errors.Newf("failed to extract source table name for stream query: %s", query.String())
}

// CheckStreamQuery verifies the query of stream.
func CheckStreamQuery(
	query *tree.Select, p PlanHookState, tableDesc *MutableTableDescriptor,
) error {
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
		return CheckStreamAggQuery(p, selectClause, tableDesc)
	}

	return nil
}

// CheckStreamAggQuery verifies the query with aggregator of stream.
func CheckStreamAggQuery(
	p PlanHookState, selectClause *tree.SelectClause, tableDesc *MutableTableDescriptor,
) error {
	funcName, timeWindowHasSlide, err := checkStreamQueryGroupBy(selectClause, tableDesc)
	if err != nil {
		return err
	}

	if err := checkStreamQueryBeginAndEndClause(
		selectClause, tableDesc.Columns[0].Name, funcName, timeWindowHasSlide); err != nil {
		return err
	}

	for _, expr := range selectClause.Exprs {
		funcExpr, ok1 := expr.Expr.(*tree.FuncExpr)
		if ok1 {
			if funcExpr.Type == tree.DistinctFuncType {
				return errors.Newf("cannot use DISTINCT function in stream query")
			}
			if err := checkStreamQueryDistinct(p, funcExpr.Exprs); err != nil {
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
func checkStreamQueryBeginAndEndClause(
	selectClause *tree.SelectClause, tsTimestampColName, funcName string, timeWindowHasSlide bool,
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
	errNotFirst := errors.Errorf(
		"the first select clause of stream query must be the first(%s) of aggregation function %s with slide",
		tsTimestampColName, funcName)
	errNotLastFunc := errors.Errorf(
		"the second select clause of stream query must be the end-timestamp of aggregation, for example last(%s)/last_row(%s)",
		tsTimestampColName, tsTimestampColName)
	errNotLastRow := errors.Errorf(
		"the second select clause of stream query must be the last_row(%s) of aggregation function %s",
		tsTimestampColName, funcName)
	errNotLast := errors.Errorf(
		"the second select clause of stream query must be the last(%s) of aggregation function %s with slide",
		tsTimestampColName, funcName)
	isTimeWindow := funcName == memo.TimeWindow

	buildFirstError := func() error {
		if timeWindowHasSlide {
			return errNotFirst
		}

		if isTimeWindow {
			return errNotFirstRow
		}

		return errNotFirstFunc
	}

	buildLastError := func() error {
		if timeWindowHasSlide {
			return errNotLast
		}

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

	if timeWindowHasSlide && firstFunName != "first" {
		return buildFirstError()
	} else if funcName == memo.TimeWindow && firstFunName != "first_row" && !timeWindowHasSlide {
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

	if timeWindowHasSlide && lastFunName != "last" {
		return buildFirstError()
	} else if funcName == memo.TimeWindow && lastFunName != "last_row" && !timeWindowHasSlide {
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

// checkStreamQueryDistinct verifies the query with distinct of stream.
func checkStreamQueryDistinct(p PlanHookState, exprs tree.Exprs) error {
	for _, expr := range exprs {
		funcExpr, ok := expr.(*tree.FuncExpr)
		if ok {
			if funcExpr.Type == tree.DistinctFuncType {
				return errors.Newf("cannot use DISTINCT function in stream query")
			}
			return checkStreamQueryDistinct(p, funcExpr.Exprs)
		}
	}

	return nil
}

// checkStreamQueryGroupBy verifies the query with Group of stream.
func checkStreamQueryGroupBy(
	selectClause *tree.SelectClause, tableDesc *MutableTableDescriptor,
) (string, bool, error) {
	var funcName string
	if len(selectClause.GroupBy) == 0 {
		return funcName, false, nil
	}

	if selectClause.Having != nil {
		return funcName, false, errors.Errorf("unsupported having expr: %s", selectClause.Having.Expr.String())
	}

	var groupByColumns []string
	includeRequiredFunction := false
	timeWindowHasSlide := false

	for _, expr := range selectClause.GroupBy {
		switch expr.(type) {
		case *tree.FuncExpr:
			funcExpr := expr.(*tree.FuncExpr)
			funcName = funcExpr.Func.FunctionName()
			switch funcName {
			case tree.FuncTimeBucket, memo.StateWindow, memo.CountWindow, memo.TimeWindow, memo.EventWindow, memo.SessionWindow:
				includeRequiredFunction = true
				if funcName == memo.TimeWindow && len(funcExpr.Exprs) == 3 {
					timeWindowHasSlide = true
				}
			default:
				return funcName, timeWindowHasSlide, errors.Errorf("unsupported group by function in stream query: %s", funcName)
			}
		case *tree.UnresolvedName:
			funcExpr := expr.(*tree.UnresolvedName)
			groupByColumns = append(groupByColumns, funcExpr.String())
		default:
			return funcName, timeWindowHasSlide, errors.Errorf("unsupported group by clause in stream query: %s", selectClause)
		}
	}

	if !includeRequiredFunction {
		return funcName, timeWindowHasSlide, errors.Errorf("missing time_bucket or window function in GROUP BY cluster")
	}

	primaryTags := make(map[string]string)

	for _, col := range tableDesc.Columns {
		if col.IsPrimaryTagCol() {
			primaryTags[col.Name] = ""
		}
	}

	if len(groupByColumns) == 0 {
		return funcName, timeWindowHasSlide, nil
	}

	for _, col := range groupByColumns {
		_, ok := primaryTags[col]
		if !ok {
			return funcName, timeWindowHasSlide, errors.Errorf("the group by column %q of stream query is not a primary tag", col)
		}
	}

	if len(groupByColumns) != len(primaryTags) {
		return funcName, timeWindowHasSlide, errors.Errorf("should include all primary tags in stream query: %s", selectClause)
	}

	return funcName, timeWindowHasSlide, nil
}

// CheckStreamTargetTableInfo verifies the target table.
func CheckStreamTargetTableInfo(
	ctx context.Context,
	p PlanHookState,
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
			p.Txn(),
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

// ExtractTargetTableInfoForStream get the types of target table's columns.
func ExtractTargetTableInfoForStream(
	ctx context.Context, p PlanHookState, parameters *sqlutil.StreamParameters,
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

// GetSourceTableName is used to get only one table name,
// because stream only supports one table now.
func GetSourceTableName(query *tree.Select) (*tree.TableName, bool) {
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

// WaitCDCStatusChanged waits the cdc status changed.
func WaitCDCStatusChanged(
	ctx context.Context,
	cdc execinfra.CDCCoordinator,
	tableID, instanceID uint64,
	instanceType sqlbase.CDCInstanceType,
	enabled bool,
) {
	opts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     500 * time.Millisecond,
		MaxRetries:     10,
	}

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		cdcEnabled := cdc.HasTask(instanceType, tableID, instanceID)

		if (enabled && cdcEnabled) || (!enabled && !cdcEnabled) {
			return
		}
	}
}

// BuildStreamJobRecord builds the jobs.Record of stream.
// It also constructed filter expressions applicable to filtering at the SQL layer.
func BuildStreamJobRecord(
	params RunParams,
	name tree.Name,
	streamID uint64,
	parameters string,
	stmt string,
	sourceTable *cdcpb.CDCTableInfo,
	targetTableColTypes []types.T,
) (*jobs.Record, error) {
	metadata := &cdcpb.StreamMetadata{
		ID:         streamID,
		Name:       string(name),
		Parameters: parameters,
	}

	// extract and fill in the column ids, metrics and tag filter expressions.
	if err := MarshalStreamFilter(params, metadata, sourceTable); err != nil {
		return nil, err
	}

	nodeList, err := params.ExecCfg().CDCCoordinator.LiveNodeIDList(params.Ctx)
	if err != nil {
		return nil, err
	}

	jobNodeList := make([]int32, len(nodeList))
	for i, nodeID := range nodeList {
		jobNodeList[i] = int32(nodeID)
	}

	return &jobs.Record{
		Description: "stream lifecycle management",
		Statement:   stmt,
		Username:    params.GetPlanner().User(),
		Details: jobspb.StreamDetails{
			StreamMetadata:      metadata,
			ActiveNodeList:      jobNodeList,
			TargetTableColTypes: targetTableColTypes,
		},
		Progress: jobspb.StreamProgress{},
	}, nil
}

// CreateAndStartStreamJob starts stream job.
func CreateAndStartStreamJob(
	ctx context.Context,
	p PlanHookState,
	startCh chan tree.Datums,
	record jobs.Record,
	stream *metadata.StreamMetadata,
) error {
	job, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, startCh, record)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		return err
	case <-startCh:
	}

	return updateStreamRunInfo(ctx, p, job, err, stream)
}

// updateStreamRunInfo updates the running info of stream job.
func updateStreamRunInfo(
	ctx context.Context,
	p PlanHookState,
	job *jobs.Job,
	jobError error,
	stream *metadata.StreamMetadata,
) error {
	if stream == nil {
		return errors.Errorf("stream does not exist")
	}

	status, rInfo, err := constructStreamRunInfo(stream, job, jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"stream-update-job-info",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET status=$1, job_id=$2, run_info=$3 WHERE id=$4`,
		status,
		*job.ID(),
		rInfo,
		stream.ID); err != nil {
		return err
	}

	return nil
}

// UpdateStreamRunHistory updates the historical running info of stream job.
func UpdateStreamRunHistory(
	ctx context.Context, p PlanHookState, job *jobs.Job, jobError error, streamID uint64,
) error {
	var err error
	var stream *metadata.StreamMetadata
	stream, err = loadStreamByID(ctx, p, streamID)
	if err != nil {
		return err
	}

	if stream == nil {
		return errors.Errorf("stream with id %d does not exist", streamID)
	}

	if stream.JobID > 0 && stream.JobID != *job.ID() {
		return errors.Errorf("Job ID %d does not belong to stream %s (%d).", *job.ID(), stream.Name, streamID)
	}

	status, rInfo, err := constructStreamRunInfo(stream, job, jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"stream-update-run-history",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET status=$1, run_info=$2 WHERE id=$3`,
		status,
		rInfo,
		streamID); err != nil {
		return err
	}
	return nil
}

// CanRemoveAllTableOwnedStreams checks if all streams owned by a table can be safely removed
func CanRemoveAllTableOwnedStreams(
	ctx context.Context,
	p *GenericPlanner,
	desc *sqlbase.MutableTableDescriptor,
	behavior tree.DropBehavior,
) error {
	return CheckTableUsedByStream(
		ctx, p, uint64(desc.ID), desc.Name, nil, behavior == tree.DropCascade)
}

func removeTableStreams(
	ctx context.Context, p *GenericPlanner, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"load-streams",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id,job_id,source_table_id FROM system.kwdb_streams WHERE source_table_id = $1 OR target_table_id = $1`,
		tableDesc.ID,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		streamID := uint64(tree.MustBeDInt(row[0]))
		jobID := int64(tree.MustBeDInt(row[1]))
		tableID := int64(tree.MustBeDInt(row[2]))

		if err = RemoveStream(ctx, p, jobID, streamID, uint64(tableID)); err != nil {
			return err
		}
	}

	return nil
}

// RemoveStream removes a stream and its associated resources
func RemoveStream(
	ctx context.Context, p *GenericPlanner, jobID int64, streamID uint64, tableID uint64,
) error {
	if jobID != 0 {
		// Close the job by closing the CDC
		p.ExecCfg().CDCCoordinator.StopCDCByLocal(tableID, streamID, sqlbase.CDCInstanceType_Stream)
		WaitCDCStatusChanged(ctx, p.ExecCfg().CDCCoordinator, tableID, streamID, sqlbase.CDCInstanceType_Stream, false)

		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			job, _ := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
			// After CDC is closed, the job status is usually StatusFailed,
			// and if the job status is not StatusFailed, CancelRequested is used to close it,
			// which usually takes 30 seconds.
			if job != nil {
				if status, err := job.WithTxn(txn).CurrentStatus(ctx); err == nil {
					if status == jobs.StatusRunning || status == jobs.StatusPending {
						_ = p.ExecCfg().JobRegistry.CancelRequested(ctx, txn, jobID)
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_streams WHERE id = $1",
		streamID,
	); err != nil {
		return err
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream-water-mark",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_cdc_watermark WHERE table_id = $1 AND task_id = $2 AND task_type = $3",
		tableID,
		streamID,
		sqlbase.CDCInstanceType_Stream,
	); err != nil {
		return err
	}

	return nil
}

// constructStreamRunInfo constructs running info of stream job.
func constructStreamRunInfo(
	stream *metadata.StreamMetadata, job *jobs.Job, jobError error,
) (string, json.JSON, error) {
	status := sqlutil.StreamStatusEnable
	index := -1
	for i, v := range stream.RunInfoList {
		if v.JobID == *job.ID() {
			index = i
			break
		}
	}

	if index == -1 {
		stream.RunInfoList = append(stream.RunInfoList, sqlutil.RunInfo{
			JobID:     *job.ID(),
			StartTime: timeutil.Now().Format(time.RFC3339),
		})

		if len(stream.RunInfoList) > sqlutil.StreamMaxRunInfo {
			stream.RunInfoList = stream.RunInfoList[1:]
		}

		index = len(stream.RunInfoList) - 1
	}

	if jobError != nil {
		stream.RunInfoList[index].EndTime = timeutil.Now().Format(time.RFC3339)

		if sqlutil.ShouldLogError(jobError) {
			stream.RunInfoList[index].ErrorMessage = jobError.Error()
		}
		status = sqlutil.StreamStatusDisable
	}

	rInfo, err := sqlutil.MarshalStreamRunInfo(stream.RunInfoList)
	if err != nil {
		return "", nil, err
	}

	return status, rInfo, nil
}
