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
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// CheckPipeByName checks if a pipe with the given name already exists
func CheckPipeByName(ctx context.Context, p PlanHookState, pipeName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-pipe",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id, name FROM system.kwdb_pipes WHERE name = $1`,
		pipeName,
	)
	if err != nil {
		return false, err
	}

	// pipe does not exist
	if len(row) == 0 {
		return false, nil
	}
	return true, nil
}

// LoadPipeByName loads a pipe descriptor by its name from the system tables
func LoadPipeByName(
	ctx context.Context, p PlanHookState, pipeName tree.Name,
) (*metadata.PipeMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE name = '%s'`, pipeName)
	return loadPipe(ctx, p, stmt, p.Txn())
}

func loadPipeByID(
	ctx context.Context, p PlanHookState, pipeID uint64,
) (*metadata.PipeMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE id = %d`, pipeID)
	return loadPipe(ctx, p, stmt, p.Txn())
}

func loadPipe(
	ctx context.Context, p PlanHookState, stmt string, txn *kv.Txn,
) (*metadata.PipeMetadata, error) {
	var metadata metadata.PipeMetadata
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"load-pipe",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return nil, err
	}

	// pipe does not exist
	if len(row) == 0 {
		return nil, nil
	}

	metadata.ID = uint64(tree.MustBeDInt(row[0]))
	metadata.Name = tree.Name(tree.MustBeDString(row[1]))
	metadata.Parameters = tree.MustBeDJSON(row[2]).JSON
	metadata.CreateAt = tree.MustBeDTimestamp(row[3])
	metadata.CreateBy = string(tree.MustBeDString(row[4]))
	metadata.Status = string(tree.MustBeDString(row[5]))
	metadata.RunInfo = tree.MustBeDJSON(row[6]).JSON
	metadata.JobID = int64(tree.MustBeDInt(row[7]))
	metadata.DatabaseID = uint64(tree.MustBeDInt(row[8]))
	metadata.LowWaterMark = int64(tree.MustBeDInt(row[9]))

	if err = metadata.Decode(); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// MakeCDCTableInfo constructs CDCTableInfo and extracts tableIds for Pipe and Publication.
// It needs to check whether the specified columns in
// CREATE PIPE FOR TABLE table_name(column_name[, ...]), ALTER PIPE SET TABLE table_name(column_name[, ...]),
// CREATE PUB FOR TABLE table_name(column_name[, ...]), and ALTER PUB SET TABLE table_name(column_name[, ...])
// are still existed. And in these cases, the parameter needCheckColumns is true.
func MakeCDCTableInfo(
	ctx context.Context,
	p PlanHookState,
	tableDescList []*MutableTableDescriptor,
	star bool,
	cols tree.NameList,
	needCheckColumns bool,
) ([]cdcpb.CDCTableInfo, []uint64, error) {
	var tables []cdcpb.CDCTableInfo
	var tableIds []uint64

	for i := range tableDescList {
		var tableInfo cdcpb.CDCTableInfo
		var columnTypes []string
		var columnNames []string
		var columnIDs []uint32
		tableDesc := tableDescList[i]
		if star {
			for _, col := range tableDesc.Columns {
				columnIDs = append(columnIDs, uint32(col.ID))
				columnNames = append(columnNames, col.Name)
				// get all column type
				columnTypes = append(columnTypes, col.Type.SQLString())
				if col.IsTagCol() && !col.IsPrimaryTagCol() {
					tableInfo.NeedNormalTag = true
				}
			}
		} else {

			for _, colName := range cols {
				col, dropping, err := tableDesc.FindColumnByName(colName)
				if err != nil {
					if needCheckColumns {
						return nil, nil, err
					}
					// alter table drop column will trigger update columns automatically
					continue
				}
				if dropping {
					if needCheckColumns {
						return nil, nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"column %q being dropped, try again later", col.Name)
					}
					// alter table drop column will trigger update columns automatically
					continue
				}
				columnIDs = append(columnIDs, uint32(col.ID))
				columnNames = append(columnNames, string(colName))
				// get all column type
				columnTypes = append(columnTypes, col.Type.SQLString())
				if col.IsTagCol() && !col.IsPrimaryTagCol() {
					tableInfo.NeedNormalTag = true
				}
			}
		}

		dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.Txn(), tableDesc.ParentID)
		if err != nil {
			return nil, nil, err
		}
		tableInfo.Database = dbDesc.Name
		// can not create other schema in ts database.
		tableInfo.Schema = "public"
		tableInfo.ID = uint64(tableDesc.ID)
		tableInfo.Table = tableDesc.Name
		tableInfo.IsStar = star
		tableInfo.ColIDs = columnIDs
		tableInfo.ColNames = columnNames
		tableInfo.ColTypes = columnTypes
		tableInfo.LowWatermark = 0
		tableInfo.TsColumnName = tableDesc.Columns[0].Name
		tableInfo.TsColumnPrecision = tableDesc.Columns[0].Type.Precision()
		var primaryTags, normalTags []string
		for _, col := range tableDesc.Columns {
			if col.IsPrimaryTagCol() {
				primaryTags = append(primaryTags, col.Name)
				continue
			}
			if !col.IsPrimaryTagCol() && col.IsTagCol() {
				normalTags = append(normalTags, col.Name)
			}
		}
		tableInfo.PrimaryTagCols = primaryTags
		tableInfo.NormalTagCols = normalTags
		tables = append(tables, tableInfo)
		tableIds = append(tableIds, uint64(tableDesc.ID))
	}

	return tables, tableIds, nil
}

// CheckPipePrivilege verifies if the user has `privilege` on `pipe`.
func CheckPipePrivilege(
	ctx context.Context,
	p PlanHookState,
	tableDesc sqlbase.DescriptorProto,
	privilegeKind privilege.Kind,
	pipe *metadata.PipeMetadata,
) error {
	// Verify user has system admin role
	isAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	if isAdmin {
		return nil
	}

	// Verify if the user is pipe-creator for alter or drop case
	if pipe != nil && pipe.CreateBy != p.User() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege on pipe %s",
			p.User(), privilegeKind, pipe.Name)
	}

	// Verify table privilege for create or alter case
	// TODO: douzt double check if this step is actually needed
	// if tableDesc != nil {
	// 	if err = p.CheckDMLPrivilege(ctx, tableDesc, nil, privilege.SELECT); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// CheckCDCMax verifies that the maximum number of CDC feeds has not been exceeded
func CheckCDCMax(ctx context.Context, p PlanHookState, num int64) error {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"count-pipes",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c WHERE p.id=c.task_id AND status = $1`,
		sqlconst.StatusEnable,
	)
	if err != nil {
		return err
	}

	dint, _ := tree.AsDInt(row[0])
	maxNumber := cdcpb.TsCDCMaxActiveNumber.Get(p.ExecCfg().SV())
	if int64(dint)+num > maxNumber {
		return pgerror.Newf(
			pgcode.ProgramLimitExceeded, "The number of running cdc tasks reaches the limitation (%d)", maxNumber)
	}

	return nil
}

// CheckTableUsedByCDC checks if a table is currently being used by a CDC feed
func CheckTableUsedByCDC(
	ctx context.Context, p PlanHookState, tableID uint64, cmdList []tree.AlterTableCmd,
) error {
	typ := sqlconst.TypeDropCDCTable
	for _, cmd := range cmdList {
		switch cmd.(type) {
		case *tree.AlterTableDropColumn,
			*tree.AlterTableRenameColumn,
			tree.ColumnMutationCmd,
			*tree.AlterTableAlterTagType,
			*tree.AlterTableDropTag,
			*tree.AlterTableRenameTag:
			typ = sqlconst.TypeAlterCDCTable
			break
		case *tree.AlterTableAddColumn, *tree.AlterTableAddTag:
			typ = sqlconst.TypeAddCDCTable
		default:
			typ = sqlconst.TypeOtherCDCTable
		}
	}

	if typ == sqlconst.TypeOtherCDCTable {
		return nil
	}

	query := `SELECT name FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c WHERE p.id=c.task_id AND c.table_id = $1`
	if typ == sqlconst.TypeAddCDCTable {
		query += fmt.Sprintf(" AND status = '%s'", sqlconst.StatusEnable)
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-pipe",
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

	pipeList := make([]string, 0, len(rows))

	for _, row := range rows {
		pipeName := string(tree.MustBeDString(row[0]))
		pipeList = append(pipeList, pipeName)
	}

	return errors.Newf("relation is used by pipe [ %s ]", strings.Join(pipeList, ", "))
}

// CheckTableRelatedPipe finds and returns the table related pipe metadata list.
func CheckTableRelatedPipe(
	ctx context.Context, p PlanHookState, tableID uint64, databaseID uint64,
) ([]*metadata.PipeMetadata, error) {
	query := `SELECT distinct p.id,p.name,p.parameters,p.status FROM system.kwdb_pipes p 
  					LEFT JOIN system.kwdb_cdc_watermark c ON p.id=c.task_id
            WHERE task_type=$1 AND ( c.table_id = $2 OR p.source_id = $3 )`
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-pipe",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		sqlbase.CDCInstanceType_Pipe,
		tableID,
		databaseID,
	)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}
	pipeMetadataArray := make([]*metadata.PipeMetadata, 0, len(rows))
	for _, row := range rows {
		var md metadata.PipeMetadata
		md.ID = uint64(tree.MustBeDInt(row[0]))
		md.Name = tree.Name(tree.MustBeDString(row[1]))
		md.Parameters = tree.MustBeDJSON(row[2]).JSON
		md.Status = string(tree.MustBeDString(row[3]))

		if err = md.Decode(); err != nil {
			return nil, err
		}

		pipeMetadataArray = append(pipeMetadataArray, &md)
	}
	return pipeMetadataArray, nil
}

// StartPipeJob starts a pipe job for CDC data replication
func StartPipeJob(
	ctx context.Context,
	p PlanHookState,
	startCh chan tree.Datums,
	record jobs.Record,
	pipe *metadata.PipeMetadata,
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

	return updatePipeRunInfo(ctx, p, job, err, pipe)
}

func updatePipeRunInfo(
	ctx context.Context, p PlanHookState, job *jobs.Job, jobError error, pipe *metadata.PipeMetadata,
) error {
	if pipe == nil {
		return errors.Errorf("pipe does not exist")
	}

	status, rInfo, err := ConstructRunInfo(pipe.RunInfoList, *job.ID(), jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"update-pipe-job-info",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_pipes SET status=$1,job_id=$2,run_info=$3 WHERE id=$4`,
		status,
		*job.ID(),
		rInfo,
		pipe.ID,
	); err != nil {
		return err
	}

	return nil
}

// UpdatePipeRunHistory updates the run history for a pipe job
func UpdatePipeRunHistory(
	ctx context.Context, p PlanHookState, job *jobs.Job, jobError error, pipeID uint64,
) error {
	err := p.ExecCfg().DB.Txn(ctx, func(cxt context.Context, txn *kv.Txn) error {
		stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE id = %d`, pipeID)
		pipe, err := loadPipe(ctx, p, stmt, txn)
		if err != nil {
			return err
		}

		if pipe == nil {
			return errors.Errorf("pipe with id %d does not exist", pipeID)
		}

		// job id has changed, the pipe is restarted.
		if pipe.JobID != *job.ID() {
			return nil
		}

		status, rInfo, err := ConstructRunInfo(pipe.RunInfoList, *job.ID(), jobError)
		if err != nil {
			return err
		}

		if _, err = p.ExecCfg().InternalExecutor.ExecEx(
			ctx,
			"update pipe job info",
			txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_pipes SET status=$1,run_info=$2 WHERE id=$3`,
			status,
			rInfo,
			pipeID); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// CheckWhereExprForHistory validates the WHERE expression for historical data queries
func CheckWhereExprForHistory(
	ctx context.Context, p PlanHookState, table *cdcpb.CDCTableInfo,
) error {
	const queryFormat = "SELECT * FROM %s.%s WHERE %s LIMIT 1"
	query := fmt.Sprintf(
		queryFormat,
		table.Database,
		table.Table,
		table.Filter,
	)

	if _, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-where-expr",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
	); err != nil {
		return err
	}

	return nil
}

// MakePipeOptions constructs pipe options from the parsed SQL statement
func MakePipeOptions(
	pipeOpts map[string]string, originOpts *cdcpb.PipeOptions,
) (cdcpb.PipeOptions, int64, error) {
	var opts cdcpb.PipeOptions
	watermark := int64(cdcpb.InvalidWatermark)

	if value, ok := pipeOpts[sqlconst.OptSink]; ok {
		opts.Sink = value

		if err := cdcpb.CheckSink(opts.Sink, false); err != nil {
			return opts, watermark, err
		}
	} else {
		if originOpts != nil {
			opts.Sink = originOpts.Sink
		} else {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "option \"sink\" is required.")
		}
	}

	if value, ok := pipeOpts[sqlconst.OptEnable]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case sqlconst.OptOn, sqlconst.OptOff:
			opts.Enable = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "enable parameter %q is invalid", value)
		}
	} else {
		if originOpts != nil {
			opts.Enable = originOpts.Enable
		} else {
			opts.Enable = sqlconst.OptOn
		}
	}

	if value, ok := pipeOpts[sqlconst.OptMessageFormat]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case sqlconst.MessageFormatJSON:
			opts.MessageFormat = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "invalid message_format: %s", value)
		}
	} else {
		if originOpts != nil {
			opts.MessageFormat = originOpts.MessageFormat
		} else {
			opts.MessageFormat = sqlconst.MessageFormatJSON
		}
	}

	if value, ok := pipeOpts[sqlconst.OptIgnoreHistory]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case sqlconst.OptOn, sqlconst.OptOff:
			opts.IgnoreHistory = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptIgnoreHistory, value)
		}
	} else {
		if originOpts != nil {
			opts.IgnoreHistory = originOpts.IgnoreHistory
		} else {
			opts.IgnoreHistory = sqlconst.OptOn
		}
	}

	if value, ok := pipeOpts[sqlconst.OptBufferSize]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptBufferSize, value)
		}
		if num < 0 || num > 1024 {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", sqlconst.OptBufferSize, value)
		}
		opts.BufferSize = num
	} else {
		if originOpts != nil {
			opts.BufferSize = originOpts.BufferSize
		} else {
			opts.BufferSize = sqlconst.DefaultBufferSize
		}
	}

	if value, ok := pipeOpts[sqlconst.OptCheckTag]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case sqlconst.OptOn, sqlconst.OptOff:
			opts.CheckTag = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptCheckTag, value)
		}
	} else {
		if originOpts != nil {
			opts.CheckTag = originOpts.CheckTag
		} else {
			opts.CheckTag = sqlconst.OptOn
		}
	}

	if value, ok := pipeOpts[sqlconst.OptPublish]; ok {
		lowerValues := strings.Split(strings.ToLower(value), ",")
		keys := make(map[string]bool)
		var list []string
		containsAll := false
		for _, lowerValue := range lowerValues {
			if _, ok := keys[lowerValue]; ok {
				continue
			}

			keys[lowerValue] = true
			switch lowerValue {
			case cdcpb.EventAll:
				list = []string{cdcpb.EventAll}
				containsAll = true
			case cdcpb.EventInsert, cdcpb.EventDelete, cdcpb.EventUpdate, cdcpb.EventDDL:
				list = append(list, lowerValue)
			default:
				return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptPublish, lowerValue)
			}
			if containsAll {
				break
			}
		}
		if len(list) > 0 {
			opts.Publish = strings.Join(list, ",")
		} else {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptPublish, value)
		}
	} else {
		if originOpts != nil {
			opts.Publish = originOpts.Publish
		} else {
			opts.Publish = cdcpb.EventInsert
		}
	}

	if value, ok := pipeOpts[sqlconst.OptLowWatermark]; ok {
		watermarkTime, err := tree.ParseDTimestamp(nil, value, time.Nanosecond)
		if err != nil {
			return opts, watermark, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"%v, %s parameter %q is invalid, the format is like %s",
				err,
				sqlconst.OptLowWatermark,
				value,
				sqlconst.OptLowWatermarkFormat,
			)
		}

		watermark = watermarkTime.UTC().UnixNano()
		if watermark <= cdcpb.InvalidWatermark || watermark >= tree.TsMaxNanoTimestamp {
			return opts, watermark, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"%s parameter %q is invalid, must be from %v to %v",
				sqlconst.OptLowWatermark,
				value,
				timeutil.FromUnixNano(cdcpb.InvalidWatermark).UTC(),
				timeutil.FromUnixNano(tree.TsMaxNanoTimestamp).UTC(),
			)
		}
	}

	return opts, watermark, nil
}

// canRemoveAllTableOwnedPipes checks whether the specified table is used by pipe.
// In DROP DATABASE CASCADE, it returns an error if the specified table is used by more than one pipe.
// In DROP DATABASE without CASCADE, it will return an error if the specified table is used
// by	pipe, stream, subscription, and so on.
func canRemoveAllTableOwnedPipes(
	ctx context.Context,
	p PlanHookState,
	desc *sqlbase.MutableTableDescriptor,
	behavior tree.DropBehavior,
) error {
	if !desc.IsTSTable() {
		return nil
	}

	if behavior == tree.DropCascade {
		if err := checkTableHasRelPipes(ctx, p, uint64(desc.ID)); err != nil {
			return pgerror.Wrapf(err, pgcode.ObjectInUse, "relation %q is used by pipe", desc.Name)
		}

		return nil
	}

	if err := CheckTableUsedByCDC(ctx, p, uint64(desc.ID), nil); err != nil {
		return pgerror.Wrapf(err, pgcode.ObjectInUse, "relation %q is used by pipe", desc.Name)
	}

	return nil
}

// checkTableHasRelPipes checks whether the specified table is used by more than one pipe.
// It returns an error if the specified table is used by more than one pipe.
func checkTableHasRelPipes(ctx context.Context, p PlanHookState, tableID uint64) error {
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"query-table-pipes",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT p.name,count(*) FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c
WHERE p.id=c.task_id 
AND p.id in (SELECT a.id FROM system.kwdb_pipes a,system.kwdb_cdc_watermark b WHERE a.id=b.task_id AND b.table_id = $1)
GROUP BY p.name ORDER BY p.name`,
		tableID,
	)
	if err != nil {
		return err
	}

	if rows == nil {
		return nil
	}

	for _, row := range rows {
		if tree.MustBeDInt(row[1]) > 1 {
			return errors.Errorf("the pipe %s has more than one relation table", tree.MustBeDString(row[0]))
		}
	}

	return nil
}

// RemovePipe removes a pipe and its associated resources
func RemovePipe(
	ctx context.Context, p PlanHookState, jobID int64, meta *metadata.PipeMetadata,
) error {
	if _, err := p.ExecCfg().InternalExecutor.ExecEx(ctx, "delete-pipe-unpush", p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`DELETE FROM system.kwdb_unpush WHERE pusher_id=$1 AND type=$2`,
		meta.ID, sqlbase.CDCInstanceType_Pipe,
	); err != nil {
		log.Errorf(ctx, "pipe[%s] sends unsend failed. %v", meta.Name, err)
		return err
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-pipe",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_pipes WHERE id = $1",
		meta.ID,
	); err != nil {
		return err
	}

	if jobID != 0 {
		if len(meta.ParaInfo.Tables) > 0 && p.ExecCfg().CDCCoordinator.HasTask(
			sqlbase.CDCInstanceType_Pipe, meta.ParaInfo.Tables[0].ID, uint64(jobID)) {
			cdcID := uint64(jobID)
			for _, item := range meta.ParaInfo.Tables {
				p.ExecCfg().CDCCoordinator.StopCDCByLocal(item.ID, cdcID, sqlbase.CDCInstanceType_Pipe)
				WaitCDCStatusChanged(
					ctx,
					p.ExecCfg().CDCCoordinator,
					item.ID,
					cdcID,
					sqlbase.CDCInstanceType_Pipe,
					false,
				)
			}
		} else {
			job, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.Txn())
			if err != nil {
				return err
			}

			if job.WithTxn(p.Txn()).CheckRunningStatus(ctx) {
				if err = p.ExecCfg().JobRegistry.CancelRequested(ctx, p.Txn(), jobID); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
