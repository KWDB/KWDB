//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package sql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// states and behaviors of scheduled jobs
	optEnable        = "enable"
	optSink          = "sink"
	optMessageFormat = "message_format"
	optIgnoreHistory = "ignore_history"
	optBufferSize    = "buffer_size"
	// The parameter retrieve_tags to control whether to retrieve normal tags.
	// The default value is enabled. When the user-specified output columns and filter columns do not include normal tags,
	// tag retrieve will not be performed regardless of this parameter's value (i.e., retrieve_tags remains enabled).
	// When the output columns and filter columns contain normal tags, retrieve_tags determines whether CDC enables
	// tag retrieve.
	optCheckTag           = "retrieve_tags"
	optLowWatermark       = "low_watermark"
	optLowWatermarkFormat = "2006-01-02 15:04:05"

	statusEnable  = "Enable"
	statusDisable = "Disable"

	optOn  = "on"
	optOff = "off"

	messageFormatJSON = "json"
	cdcMaxRunInfo     = 5
	defaultBufferSize = 1

	typeDropCDCTable = iota
	typeAddCDCTable
	typeAlterCDCTable
	typeOtherCDCTable
)

// message kind to send to pipe about ddl
const (
	kafkaMsgKindCreateTable = "create_table" // create table
	kafkaMsgKindAlterTable  = "alter_table"  // alter table
	kafkaMsgKindDropTable   = "drop_table"   // drop table
)

var pipeOptionExpectValues = map[string]KVStringOptValidate{
	optEnable:        KVStringOptRequireValue,
	optSink:          KVStringOptRequireValue,
	optMessageFormat: KVStringOptRequireValue,
	optIgnoreHistory: KVStringOptRequireValue,
	optBufferSize:    KVStringOptRequireValue,
	optCheckTag:      KVStringOptRequireValue,
	optPublish:       KVStringOptRequireValue,
	optLowWatermark:  KVStringOptRequireValue,
}

type createPipeNode struct {
	n             *tree.CreatePipe
	databaseID    uint64
	tableDescList []*sqlbase.MutableTableDescriptor
	pipeOpts      func() (map[string]string, error)

	run pipeComputeRun
}

type pipeComputeRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

// PipeMetadata records a list of pipe info to run pipe
type PipeMetadata struct {
	id               uint64
	name             tree.Name
	parameters       json.JSON
	createAt         tree.DTimestamp
	createBy         string
	status           string
	runInfo          json.JSON
	jobID            int64
	databaseID       uint64
	lowWaterMark     int64
	cdcWatermarkList []CDCWatermark

	paraInfo    cdcpb.PipeParameters
	runInfoList []cdcpb.RunInfo
}

// Decode decode JSON to struct.
func (p *PipeMetadata) Decode() error {
	var err error
	p.paraInfo, err = cdcpb.UnmarshalPipeParameters(p.parameters)
	if err != nil {
		return err
	}

	if p.runInfo != nil {
		p.runInfoList, err = cdcpb.UnmarshalRunInfo(p.runInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreatePipe creates a pipe node for exec.
func (p *planner) CreatePipe(ctx context.Context, n *tree.CreatePipe) (planNode, error) {
	found, err := p.checkPipeByName(ctx, n.PipeName)
	if err != nil {
		return nil, err
	}
	if found {
		return nil, pgerror.Newf(pgcode.DuplicateObject, "pipe %q already exists", n.PipeName)
	}
	var databaseID uint64

	if n.Table.TableName != "" {
		// single table
		n.TableNames = append(n.TableNames, n.Table)
	} else if n.Database != "" {
		// single database
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on database")
		}

		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Database), true)
		if err != nil {
			return nil, err
		}
		if dbDesc.EngineType != tree.EngineTypeTimeseries {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts database")
		}
		databaseID = uint64(dbDesc.ID)
		schemas, err := p.Tables().getSchemasForDatabase(ctx, p.txn, dbDesc.ID)
		if err != nil {
			return nil, err
		}

		// the names of all objects in the target database
		for _, schema := range schemas {
			toAppend, err := GetObjectNames(
				ctx, p.txn, p, dbDesc, schema, true, /*explicitPrefix*/
			)
			if err != nil {
				return nil, err
			}
			n.TableNames = append(n.TableNames, toAppend...)
		}

	} else {
		// multi table
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on multi table")
		}
	}
	tableDescList := make([]*MutableTableDescriptor, len(n.TableNames))
	checkTableDuplicate := make(map[sqlbase.ID]struct{})

	for i := range n.TableNames {
		tableDescList[i], err = p.ResolveMutableTableDescriptor(
			ctx, &n.TableNames[i], true /*required*/, ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDescList[i].IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts table")
		}

		if _, ok := checkTableDuplicate[tableDescList[i].ID]; ok {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "duplicate table %s", tableDescList[i].Name)
		}
		checkTableDuplicate[tableDescList[i].ID] = struct{}{}

		if err = p.checkPipePrivilege(ctx, tableDescList[i], privilege.CREATE, nil); err != nil {
			return nil, err
		}
	}

	pipeOpts, err := p.TypeAsStringOpts(n.Options, pipeOptionExpectValues)
	if err != nil {
		return nil, err
	}

	return &createPipeNode{n: n, tableDescList: tableDescList, pipeOpts: pipeOpts, databaseID: databaseID}, nil
}

func (p *planner) checkPipeByName(ctx context.Context, pipeName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-pipe",
		p.txn,
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

func (p *planner) loadPipeByName(ctx context.Context, pipeName tree.Name) (*PipeMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE name = '%s'`, pipeName)
	return p.loadPipe(ctx, stmt, p.Txn())
}

func (p *planner) loadPipeByID(ctx context.Context, pipeID uint64) (*PipeMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE id = %d`, pipeID)
	return p.loadPipe(ctx, stmt, p.Txn())
}

func (p *planner) loadPipe(ctx context.Context, stmt string, txn *kv.Txn) (*PipeMetadata, error) {
	var metadata PipeMetadata
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

	metadata.id = uint64(tree.MustBeDInt(row[0]))
	metadata.name = tree.Name(tree.MustBeDString(row[1]))
	metadata.parameters = tree.MustBeDJSON(row[2]).JSON
	metadata.createAt = tree.MustBeDTimestamp(row[3])
	metadata.createBy = string(tree.MustBeDString(row[4]))
	metadata.status = string(tree.MustBeDString(row[5]))
	metadata.runInfo = tree.MustBeDJSON(row[6]).JSON
	metadata.jobID = int64(tree.MustBeDInt(row[7]))
	metadata.databaseID = uint64(tree.MustBeDInt(row[8]))
	metadata.lowWaterMark = int64(tree.MustBeDInt(row[9]))

	if err = metadata.Decode(); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// checkPipePrivilege verifies if the user has `privilege` on `pipe`.
func (p *planner) checkPipePrivilege(
	ctx context.Context,
	tableDesc sqlbase.DescriptorProto,
	privilegeKind privilege.Kind,
	pipe *PipeMetadata,
) error {
	// Verify user has system admin role
	var isAdmin bool
	var err error
	isAdmin, err = p.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	if isAdmin {
		return nil
	}

	// Verify if the user is pipe-creator for alter or drop case
	if pipe != nil && pipe.createBy != p.User() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege on pipe %s",
			p.User(), privilegeKind, pipe.name)
	}

	// Verify table privilege for create or alter case
	if tableDesc != nil {
		if err = p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
			return err
		}
	}

	return nil
}

func (p *planner) checkCDCMax(ctx context.Context, num int64) error {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"count-pipes",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c WHERE p.id=c.task_id AND status = $1`,
		statusEnable,
	)
	if err != nil {
		return err
	}

	dint, _ := tree.AsDInt(row[0])
	maxNumber := cdcpb.TsCDCMaxActiveNumber.Get(p.execCfg.SV())
	if int64(dint)+num > maxNumber {
		return pgerror.Newf(
			pgcode.ProgramLimitExceeded, "The number of running cdc tasks reaches the limitation (%d)", maxNumber)
	}

	return nil
}

func (p *planner) checkTableUsedByCDC(
	ctx context.Context, tableID uint64, cmdList []tree.AlterTableCmd,
) error {
	typ := typeDropCDCTable
	for _, cmd := range cmdList {
		switch cmd.(type) {
		case *tree.AlterTableDropColumn,
			*tree.AlterTableRenameColumn,
			tree.ColumnMutationCmd,
			*tree.AlterTableAlterTagType,
			*tree.AlterTableDropTag,
			*tree.AlterTableRenameTag:
			typ = typeAlterCDCTable
			break
		case *tree.AlterTableAddColumn, *tree.AlterTableAddTag:
			typ = typeAddCDCTable
		default:
			typ = typeOtherCDCTable
		}
	}

	if typ == typeOtherCDCTable {
		return nil
	}

	query := `SELECT name FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c WHERE p.id=c.task_id AND c.table_id = $1`
	if typ == typeAddCDCTable {
		query += fmt.Sprintf(" AND status = '%s'", statusEnable)
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-pipe",
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

	pipeList := make([]string, 0, len(rows))

	for _, row := range rows {
		pipeName := string(tree.MustBeDString(row[0]))
		pipeList = append(pipeList, pipeName)
	}

	return errors.Newf("relation is used by pipe [ %s ]", strings.Join(pipeList, ", "))
}

// checkTableRelatedPipe finds and returns the table related pipe metadata list.
func (p *planner) checkTableRelatedPipe(
	ctx context.Context, tableID uint64, databaseID uint64,
) ([]*PipeMetadata, error) {
	query := `SELECT distinct p.id,p.name,p.parameters,p.status FROM system.kwdb_pipes p 
  					LEFT JOIN system.kwdb_cdc_watermark c ON p.id=c.task_id
            WHERE task_type=$1 AND ( c.table_id = $2 OR p.source_id = $3 )`
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-table-pipe",
		p.txn,
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
	pipeMetadataArray := make([]*PipeMetadata, 0, len(rows))
	for _, row := range rows {
		var metadata PipeMetadata
		metadata.id = uint64(tree.MustBeDInt(row[0]))
		metadata.name = tree.Name(tree.MustBeDString(row[1]))
		metadata.parameters = tree.MustBeDJSON(row[2]).JSON
		metadata.status = string(tree.MustBeDString(row[3]))

		if err = metadata.Decode(); err != nil {
			return nil, err
		}

		pipeMetadataArray = append(pipeMetadataArray, &metadata)
	}
	return pipeMetadataArray, nil
}

// checkDatabaseUsedByCDC finds and returns the database related pipe metadata list.
func (p *planner) checkDatabaseUsedByCDC(
	ctx context.Context, databaseID uint64,
) ([]*PipeMetadata, error) {
	query := fmt.Sprintf(
		"SELECT name FROM system.kwdb_pipes WHERE source_id = $1 ")

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"count-database-pipe",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		databaseID,
	)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}
	pipeMetadataArray := make([]*PipeMetadata, 0, len(rows))
	for _, row := range rows {
		pipeName := string(tree.MustBeDString(row[0]))
		pipeMeta, err := p.loadPipeByName(ctx, tree.Name(pipeName))
		if err != nil {
			return nil, err
		}
		pipeMetadataArray = append(pipeMetadataArray, pipeMeta)
	}
	return pipeMetadataArray, nil
}

func (p *planner) startPipeJob(
	ctx context.Context, startCh chan tree.Datums, record jobs.Record, pipe *PipeMetadata,
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

	return p.updatePipeRunInfo(ctx, job, err, pipe)
}

func (p *planner) updatePipeRunInfo(
	ctx context.Context, job *jobs.Job, jobError error, pipe *PipeMetadata,
) error {
	if pipe == nil {
		return errors.Errorf("pipe does not exist")
	}

	status, rInfo, err := constructRunInfo(pipe.runInfoList, *job.ID(), jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"update-pipe-job-info",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_pipes SET status=$1,job_id=$2,run_info=$3 WHERE id=$4`,
		status,
		*job.ID(),
		rInfo,
		pipe.id,
	); err != nil {
		return err
	}

	return nil
}

func (p *planner) updatePipeRunHistory(
	ctx context.Context, job *jobs.Job, jobError error, pipeID uint64,
) error {
	err := p.execCfg.DB.Txn(ctx, func(cxt context.Context, txn *kv.Txn) error {
		stmt := fmt.Sprintf(`SELECT id, name,parameters,create_at,create_by,status,run_info,
job_id,source_id,low_water_mark FROM system.kwdb_pipes WHERE id = %d`, pipeID)
		pipe, err := p.loadPipe(ctx, stmt, txn)
		if err != nil {
			return err
		}

		if pipe == nil {
			return errors.Errorf("pipe with id %d does not exist", pipeID)
		}

		// job id has changed, the pipe is restarted.
		if pipe.jobID != *job.ID() {
			return nil
		}

		status, rInfo, err := constructRunInfo(pipe.runInfoList, *job.ID(), jobError)
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

func constructRunInfo(
	runInfoList []cdcpb.RunInfo, jobID int64, jobError error,
) (string, json.JSON, error) {
	status := statusEnable
	index := -1
	for i, v := range runInfoList {
		if v.JobID == jobID {
			index = i
			break
		}
	}

	if index == -1 {
		runInfoList = append(runInfoList, cdcpb.RunInfo{
			JobID:     jobID,
			StartTime: timeutil.Now().Format(time.RFC3339),
		})

		if len(runInfoList) > cdcMaxRunInfo {
			runInfoList = runInfoList[1:]
		}

		index = len(runInfoList) - 1
	}

	if jobError != nil {
		runInfoList[index].EndTime = timeutil.Now().Format(time.RFC3339)
		errMsg := jobError.Error()

		// error message 'stopped successfully' means the job is stopped by user,
		// consider it as a normal situation.
		if !strings.Contains(errMsg, "stopped successfully") {
			runInfoList[index].ErrorMessage = errMsg
		}
		status = statusDisable
	}

	rInfo, err := cdcpb.MarshalRunInfo(runInfoList)
	if err != nil {
		return "", nil, err
	}

	return status, rInfo, nil
}

func (n *createPipeNode) startExec(params runParams) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	pipeOpts, err := n.pipeOpts()
	if err != nil {
		return err
	}
	options, lowWatermark, err := makePipeOptions(pipeOpts, nil)
	if err != nil {
		return err
	}

	pipeTableInfos, pipeTableIDs, err := params.p.makeCDCTableInfo(
		params.ctx, n.tableDescList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := params.p.checkWhereExprForCDC(
			params.ctx, n.n.Table, n.tableDescList[0].TableDescriptor, n.n.Where.Expr)
		if err != nil {
			return err
		}

		if whereNeedNormalTag {
			pipeTableInfos[0].NeedNormalTag = true
		}

		pipeTableInfos[0].Filter = n.n.Where.Expr.String()
		if err = params.p.checkWhereExprForHistory(params.ctx, &pipeTableInfos[0]); err != nil {
			return err
		}
	}

	for i := range pipeTableInfos {
		pipeTableInfos[i].NeedNormalTag = pipeTableInfos[i].NeedNormalTag && options.CheckTag == optOn
	}

	para := cdcpb.PipeParameters{
		Tables:      pipeTableInfos,
		TableIDs:    pipeTableIDs,
		PipeOptions: options,
	}

	parameters, err := cdcpb.MarshalPipeParameters(para)
	if err != nil {
		return err
	}

	metadata := PipeMetadata{
		name:       n.n.PipeName,
		parameters: parameters,
		databaseID: n.databaseID,
		createBy:   params.p.User(),
		createAt:   tree.DTimestamp{Time: timeutil.Now()},
	}

	// set status of pipe metadata to 'Enable' if it is in primary cluster and option enable is 'on'.
	if options.Enable == optOn {
		if err = params.p.checkCDCMax(params.ctx, int64(len(pipeTableIDs))); err != nil {
			return err
		}

		// try to connect to the target sink to ensure it's available.
		if err = cdcpb.CheckSink(options.Sink, true); err != nil {
			return errors.Wrapf(err, "Kafka topic %q is not available", options.Sink)
		}

		metadata.status = statusEnable
	} else {
		metadata.status = statusDisable
	}

	jobID := 0

	if _, err = params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"write-pipe-metadata",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_pipes
(name,parameters,create_by,create_at,status,run_info,job_id,source_id,low_water_mark)
values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		metadata.name, metadata.parameters, metadata.createBy, metadata.createAt.Time,
		metadata.status, "[]", jobID, metadata.databaseID, math.MinInt64,
	); err != nil {
		return err
	}

	pipeSchema, err := params.p.loadPipeByName(params.ctx, n.n.PipeName)
	if err != nil {
		return err
	}

	for i := range para.TableIDs {
		if err = params.p.addCDCWatermark(params.ctx, CDCWatermark{
			TableID:      para.TableIDs[i],
			TaskID:       pipeSchema.id,
			TaskType:     sqlbase.CDCInstanceType_Pipe,
			InternalType: cdcpb.CDCInternalTypeUnknown,
			LowWatermark: lowWatermark,
			ClientID:     nil,
		}); err != nil {
			return err
		}
	}

	// launch pipe job if the enable parameter is 'on' in primary cluster.
	// the secondary cluster does not start pipe.
	if options.Enable == optOn && metadata.status == statusEnable {
		if len(pipeTableIDs) != 0 {
			for _, tableDesc := range n.tableDescList {
				if err := params.p.addCDCDescriptor(
					params.ctx, tableDesc, sqlbase.CDCInstanceType_Pipe, pipeSchema.id, []byte(pipeSchema.name),
				); err != nil {
					return err
				}
			}
			jobRecord, err := buildPipeJobRecord(
				params, n.n.PipeName, &options, pipeTableInfos, pipeTableIDs, pipeSchema.id)
			if err != nil {
				return err
			}
			n.run.resultsCh = make(chan tree.Datums)
			n.run.errCh = make(chan error)
			startCh := make(chan tree.Datums)
			go func() {
				err := params.p.startPipeJob(params.ctx, startCh, *jobRecord, pipeSchema)
				select {
				case <-params.ctx.Done():
				case n.run.errCh <- err:
				}
				close(n.run.errCh)
				close(n.run.resultsCh)
			}()
		}
	}
	params.p.SetAuditTarget(uint32(pipeSchema.id), pipeSchema.name.String(), nil)

	return err
}

func (n *createPipeNode) Next(params runParams) (bool, error) {
	if n.run.resultsCh != nil {
		select {
		case <-params.ctx.Done():
			return false, params.ctx.Err()
		case err := <-n.run.errCh:
			return false, err
		case <-n.run.resultsCh:
			return true, nil
		}
	} else {
		return false, nil
	}
}

func (n *createPipeNode) Values() tree.Datums { return tree.Datums{} }

func (n *createPipeNode) Close(context.Context) {}

// marshalPipeFilter extracts the filter expressions of metrics and tags from the physical plan
// and marshals them to bytes. They will be applied during the data capture phase.
func marshalPipeFilter(
	params runParams, metadata *cdcpb.PipeMetadata, pipeTableInfo *cdcpb.CDCTableInfo,
) error {
	// make a new local planner
	plan, cleanup := newInternalPlanner("pipe-filter-builder", params.p.txn, params.p.User(),
		&MemoryMetrics{}, params.p.execCfg)
	defer cleanup()

	// The column order in the filter must be consistent with that in the payload
	pipeQuery := fmt.Sprintf("SELECT * FROM %s.%s ",
		pipeTableInfo.Database,
		pipeTableInfo.Table)

	if metadata.Filter != "" {
		pipeQuery += " WHERE " + metadata.Filter
	}

	stmt, err := parser.ParseOne(pipeQuery)
	if err != nil {
		return err
	}
	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.SessionData().Database = params.p.CurrentDatabase()
	localPlanner.SessionData().SearchPath = params.p.CurrentSearchPath()

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(params.ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(params.ctx)
	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	isLocal := err != nil || rec == cannotDistribute
	if len(localPlanner.curPlan.subqueryPlans) != 0 {
		return pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the pipe filter")
	}
	evalCtx := localPlanner.ExtendedEvalContext()
	planCtx := localPlanner.DistSQLPlanner().NewPlanningCtx(params.ctx, evalCtx, params.p.txn)
	planCtx.isLocal = isLocal
	planCtx.cdcCtx = &CDCContext{}
	planCtx.planner = localPlanner
	planCtx.stmtType = tree.Rows

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtx, localPlanner.curPlan.plan)
	if err != nil {
		return err
	}

	localPlanner.DistSQLPlanner().FinalizePlan(planCtx, &physPlan)

	if len(physPlan.Processors) == 1 {
		if physPlan.Processors[0].Spec.Core.Values != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported, "pipe filter %q is invalid", metadata.Filter)
		}
	}

	if planCtx.cdcCtx.metricsFilter.Expr != "" {
		metadata.MetricsFilter, err = protoutil.Marshal(&planCtx.cdcCtx.metricsFilter)
		if err != nil {
			return err
		}
	}

	for _, tf := range planCtx.cdcCtx.tagFilter {
		filler, err := protoutil.Marshal(&tf)
		if err != nil {
			return err
		}
		metadata.TagFilter = append(metadata.TagFilter, filler)
	}
	return nil
}

func buildPipeJobRecord(
	params runParams,
	name tree.Name,
	opt *cdcpb.PipeOptions,
	pipeTableInfo []cdcpb.CDCTableInfo,
	tableIDList []uint64,
	pipeID uint64,
) (*jobs.Record, error) {
	metadata := &cdcpb.PipeMetadata{
		ID:            pipeID,
		Name:          string(name),
		Sink:          opt.Sink,
		MessageFormat: messageFormatJSON,
		IgnoreHistory: opt.IgnoreHistory == optOn,
		BufferSize:    uint64(opt.BufferSize * 1024 * 1024),
		Publish:       opt.Publish,
	}

	metadata.TableList = make([]*cdcpb.CDCTable, len(pipeTableInfo))
	for i := range pipeTableInfo {
		metadata.TableList[i] = &cdcpb.CDCTable{
			TableID:               tableIDList[i],
			Database:              pipeTableInfo[i].Database,
			Schema:                pipeTableInfo[i].Schema,
			Table:                 pipeTableInfo[i].Table,
			TsColumn:              pipeTableInfo[i].TsColumnName,
			TsColumnPrecision:     pipeTableInfo[i].TsColumnPrecision,
			OutputColumns:         pipeTableInfo[i].ColIDs,
			OutputColumnTypes:     pipeTableInfo[i].ColTypes,
			OutputColumnNames:     pipeTableInfo[i].ColNames,
			NeedNormalTag:         pipeTableInfo[i].NeedNormalTag,
			PrimaryTagColumnNames: pipeTableInfo[i].PrimaryTagCols,
			NormalTagColumnNames:  pipeTableInfo[i].NormalTagCols,
		}
	}

	if len(pipeTableInfo) == 1 {
		metadata.Filter = pipeTableInfo[0].Filter
		// extract and fill in the column ids, metrics and tag filter expressions.
		if err := marshalPipeFilter(params, metadata, &pipeTableInfo[0]); err != nil {
			return nil, err
		}
	}

	return &jobs.Record{
		Description: fmt.Sprintf(
			"computes and persists global low-water mark for pipe %v, id %d", name, pipeID),
		Statement: "",
		Username:  params.p.User(),
		Details: jobspb.PipeWatermarkDetails{
			PipeMetadata: metadata,
		},
		Progress: jobspb.PipeWatermarkProgress{},
	}, nil
}

// checkWhereExprForCDC checks if the filter is supported by pipe. Currently, only row-based simple
// Only supports immutable functions.
func (p *planner) checkWhereExprForCDC(
	ctx context.Context, tn tree.TableName, table sqlbase.TableDescriptor, expr tree.Expr,
) (hasNormalTag bool, err error) {
	var hasNormalTag1, hasNormalTag2 bool
	switch exp := expr.(type) {
	case *tree.AndExpr:
		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.OrExpr:
		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.ComparisonExpr:
		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.BinaryExpr:
		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.RangeCond:
		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.From); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.To); err != nil {
			return hasNormalTag, err
		}
	case *tree.UnresolvedName:
		v, err := exp.NormalizeVarName()
		if err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, v); err != nil {
			return hasNormalTag, err
		}
	case *tree.ColumnItem:
		col, dropped, err := table.FindColumnByName(exp.ColumnName)
		if err != nil || dropped {
			return hasNormalTag, sqlbase.NewUndefinedColumnError(string(exp.ColumnName))
		}
		if col.IsTagCol() && !col.IsPrimaryTagCol() {
			hasNormalTag = true
		}
	case tree.Constant, tree.DNullExtern, *tree.DBool, *tree.Tuple:
	case *tree.ParenExpr:
		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Expr); err != nil {
			return hasNormalTag, err
		}
	case *tree.FuncExpr:
		for i := range exp.Exprs {
			if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Exprs[i]); err != nil {
				return hasNormalTag, err
			}
		}
		f := p.optPlanningCtx.optimizer.Factory()
		catalog := p.optPlanningCtx.catalog
		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &catalog, f, p.stmt.AST)
		tExpr := bld.BuildFuncForPipe(exp, table, tn)
		if fExpr, ok := tExpr.(*tree.FuncExpr); ok {
			fn := fExpr.ResolvedOverload()
			if fn.AggregateFunc != nil || fn.WindowFunc != nil || strings.Contains(exp.Func.FunctionReference.FunctionName(), "time_bucket") {
				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
			}
			switch fn.Volatility {
			case tree.VolatilityImmutable, tree.VolatilityLeakProof:
			default:
				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
			}
		} else {
			return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
		}
	default:
		return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "expr %q is not supported by pipe filter", expr.String())
	}

	return hasNormalTag || hasNormalTag1 || hasNormalTag2, nil
}

func (p *planner) checkWhereExprForHistory(ctx context.Context, table *cdcpb.CDCTableInfo) error {
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
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
	); err != nil {
		return err
	}

	return nil
}

func makePipeOptions(
	pipeOpts map[string]string, originOpts *cdcpb.PipeOptions,
) (cdcpb.PipeOptions, int64, error) {
	var opts cdcpb.PipeOptions
	watermark := int64(cdcpb.InvalidWatermark)

	if value, ok := pipeOpts[optSink]; ok {
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

	if value, ok := pipeOpts[optEnable]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case optOn, optOff:
			opts.Enable = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "enable parameter %q is invalid", value)
		}
	} else {
		if originOpts != nil {
			opts.Enable = originOpts.Enable
		} else {
			opts.Enable = optOn
		}
	}

	if value, ok := pipeOpts[optMessageFormat]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case messageFormatJSON:
			opts.MessageFormat = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "invalid message_format: %s", value)
		}
	} else {
		if originOpts != nil {
			opts.MessageFormat = originOpts.MessageFormat
		} else {
			opts.MessageFormat = messageFormatJSON
		}
	}

	if value, ok := pipeOpts[optIgnoreHistory]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case optOn, optOff:
			opts.IgnoreHistory = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optIgnoreHistory, value)
		}
	} else {
		if originOpts != nil {
			opts.IgnoreHistory = originOpts.IgnoreHistory
		} else {
			opts.IgnoreHistory = optOn
		}
	}

	if value, ok := pipeOpts[optBufferSize]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optBufferSize, value)
		}
		if num < 0 || num > 1024 {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", optBufferSize, value)
		}
		opts.BufferSize = num
	} else {
		if originOpts != nil {
			opts.BufferSize = originOpts.BufferSize
		} else {
			opts.BufferSize = defaultBufferSize
		}
	}

	if value, ok := pipeOpts[optCheckTag]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case optOn, optOff:
			opts.CheckTag = lowerValue
		default:
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optCheckTag, value)
		}
	} else {
		if originOpts != nil {
			opts.CheckTag = originOpts.CheckTag
		} else {
			opts.CheckTag = optOn
		}
	}

	if value, ok := pipeOpts[optPublish]; ok {
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
				return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optPublish, lowerValue)
			}
			if containsAll {
				break
			}
		}
		if len(list) > 0 {
			opts.Publish = strings.Join(list, ",")
		} else {
			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optPublish, value)
		}
	} else {
		if originOpts != nil {
			opts.Publish = originOpts.Publish
		} else {
			opts.Publish = cdcpb.EventInsert
		}
	}

	if value, ok := pipeOpts[optLowWatermark]; ok {
		watermarkTime, err := tree.ParseDTimestamp(nil, value, time.Nanosecond)
		if err != nil {
			return opts, watermark, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"%v, %s parameter %q is invalid, the format is like %s",
				err,
				optLowWatermark,
				value,
				optLowWatermarkFormat,
			)
		}

		watermark = watermarkTime.UTC().UnixNano()
		if watermark <= cdcpb.InvalidWatermark || watermark >= tree.TsMaxNanoTimestamp {
			return opts, watermark, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"%s parameter %q is invalid, must be from %v to %v",
				optLowWatermark,
				value,
				timeutil.FromUnixNano(cdcpb.InvalidWatermark).UTC(),
				timeutil.FromUnixNano(tree.TsMaxNanoTimestamp).UTC(),
			)
		}
	}

	return opts, watermark, nil
}

// makeCDCTableInfo constructs CDCTableInfo and extracts tableIds for Pipe and Publication.
// It needs to check whether the specified columns in
// CREATE PIPE FOR TABLE table_name(column_name[, ...]), ALTER PIPE SET TABLE table_name(column_name[, ...]),
// CREATE PUB FOR TABLE table_name(column_name[, ...]), and ALTER PUB SET TABLE table_name(column_name[, ...])
// are still existed. And in these cases, the parameter needCheckColumns is true.
func (p *planner) makeCDCTableInfo(
	ctx context.Context,
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

		dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, tableDesc.ParentID)
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

type pipeWatermarkResumer struct {
	job *jobs.Job
}

func (s *pipeWatermarkResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*planner)
	pipeID := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.ID
	pipeName := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.Name

	dsp := p.DistSQLPlanner()
	// Prepare the planning context.
	evalCtx := p.ExtendedEvalContext()
	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{})
	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	var noTxn *kv.Txn
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
	planCtx.planner = p

	if err := dsp.planAndRunCreatePipe(
		ctx, evalCtx, planCtx, noTxn, s.job, NewRowResultWriter(rows), resultsCh,
	); err != nil {
		// ctx maybe canceled
		if strings.Contains(err.Error(), "stopped successfully") {
			log.Infof(ctx, "successful to stop pipe %s.", pipeName)
		} else {
			log.Infof(ctx, "pipe %q failed with error %s", pipeName, err.Error())
		}

		updateErr := p.updatePipeRunHistory(context.Background(), s.job, err, pipeID)
		if updateErr != nil {
			return errors.Wrap(err, updateErr.Error())
		}

		return err
	}

	return nil
}

func (s *pipeWatermarkResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	return nil
}

var _ jobs.Resumer = &pipeWatermarkResumer{}

func init() {
	pipeWatermarkResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &pipeWatermarkResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypePipeWatermark, pipeWatermarkResumerFn)
}
