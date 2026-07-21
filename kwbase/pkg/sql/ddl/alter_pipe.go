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

package ddl

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/pipe"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &alterPipeNode{}

type alterPipeNode struct {
	metadata.PipeMetadata
	n         *tree.AlterPipe
	tableDesc *MutableTableDescriptor
	pipeOpts  func() (map[string]string, error)
	run       pipeComputeRun
}

// NewAlterPipeNode creates a new alterPipeNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterPipeNode(
	pipeMetadata metadata.PipeMetadata,
	n *tree.AlterPipe,
	tableDesc *MutableTableDescriptor,
	pipeOpts func() (map[string]string, error),
	run pipeComputeRun,
) *alterPipeNode {
	return &alterPipeNode{
		PipeMetadata: pipeMetadata,
		n:            n,
		tableDesc:    tableDesc,
		pipeOpts:     pipeOpts,
		run:          run,
	}
}

// AlterPipe creates a alter pipe node for exec.
func AlterPipe(ctx context.Context, p *GenericPlanner, n *tree.AlterPipe) (sql.PlanNode, error) {
	pipeInfo, err := sql.LoadPipeByName(ctx, p, n.PipeName)
	if err != nil {
		return nil, err
	}
	if pipeInfo == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "pipe %q does not exist", n.PipeName)
	}
	if err = sql.CheckPipePrivilege(ctx, p, nil, privilege.UPDATE, pipeInfo); err != nil {
		return nil, err
	}
	var alterNode alterPipeNode
	alterNode.n = n
	alterNode.PipeMetadata = *pipeInfo

	var tableDesc *MutableTableDescriptor
	if n.Table.TableName != "" {
		// ALTER TABLE case
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, &n.Table, true /*required*/, sql.ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDesc.IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts table")
		}

		if err = sql.CheckPipePrivilege(ctx, p, tableDesc, privilege.UPDATE, pipeInfo); err != nil {
			return nil, err
		}
	}
	if n.Options != nil {
		pipeOpts, err := p.TypeAsStringOpts(n.Options, pipeOptionExpectValues)
		if err != nil {
			return nil, err
		}
		alterNode.pipeOpts = pipeOpts
	}
	alterNode.tableDesc = tableDesc

	return &alterNode, nil
}

func (n *alterPipeNode) StartExec(params RunParams) (err error) {
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
	originalStatus := n.PipeMetadata.Status
	originalTableIDs := n.ParaInfo.TableIDs
	var originalTableMap = make(map[uint64]string, len(n.ParaInfo.TableIDs))
	for idx, table := range n.ParaInfo.Tables {
		originalTableMap[n.ParaInfo.TableIDs[idx]] = table.Filter
	}
	changeTargetObj := false

	if n.pipeOpts == nil {
		// ALTER PIPE name SET TABLE
		if originalStatus == sqlconst.StatusEnable {
			return errors.Errorf("pipe %q is running", n.Name)
		}

		if err = n.alterPipeSetTable(params); err != nil {
			return err
		}

		changeTargetObj = true
	} else if n.DatabaseID > 0 && originalStatus == sqlconst.StatusDisable {
		// pipe is on database
		if err = n.alterPipeUpdateTableInfoOnDatabase(params, originalTableMap); err != nil {
			return err
		}
	}
	lowWatermark := int64(cdcpb.InvalidWatermark)
	if n.n.Options != nil {
		// ALTER PIPE name SET OPTIONS
		pipeOpts, err := n.pipeOpts()
		if err != nil {
			return err
		}
		if originalStatus == sqlconst.StatusEnable {
			// cannot alter 'sink' and 'message_format' if the pipe is still running.
			if pipeOpts[sqlconst.OptSink] != "" ||
				pipeOpts[sqlconst.OptMessageFormat] != "" ||
				pipeOpts[sqlconst.OptIgnoreHistory] != "" ||
				pipeOpts[sqlconst.OptBufferSize] != "" ||
				pipeOpts[sqlconst.OptLowWatermark] != "" ||
				pipeOpts[sqlconst.OptPublish] != "" {
				return errors.Errorf("pipe %q is running.", n.Name)
			}
			if len(pipeOpts) == 1 && pipeOpts[sqlconst.OptEnable] == sqlconst.OptOn {
				return nil
			}
		} else {
			if len(pipeOpts) == 1 && pipeOpts[sqlconst.OptEnable] == sqlconst.OptOff {
				return nil
			}
		}

		opts, watermark, err := sql.MakePipeOptions(pipeOpts, &n.ParaInfo.PipeOptions)
		if err != nil {
			return err
		}
		n.ParaInfo.PipeOptions = opts
		lowWatermark = watermark

		if n.DatabaseID == 0 {
			// pipe on database has been checked in n.DatabaseID > 0 branch.
			// check whether the table(s) in pipe is valid
			err = n.alterPipeUpdateTableList(params)
			if err != nil {
				return err
			}
		}
	}

	for i := range n.ParaInfo.Tables {
		n.ParaInfo.Tables[i].NeedNormalTag = n.ParaInfo.Tables[i].NeedNormalTag &&
			n.ParaInfo.PipeOptions.CheckTag == sqlconst.OptOn
		// fill table filter in pipe with original filter if change pipe options.
		if !changeTargetObj {
			n.ParaInfo.Tables[i].Filter = originalTableMap[n.ParaInfo.TableIDs[i]]
		}
	}

	// alter pipe options after migration
	if n.ParaInfo.PipeOptions.IgnoreHistory == "" {
		n.ParaInfo.PipeOptions.IgnoreHistory = sqlconst.OptOn
		n.ParaInfo.PipeOptions.BufferSize = sqlconst.DefaultBufferSize
		n.ParaInfo.PipeOptions.CheckTag = sqlconst.OptOn
		n.PipeMetadata.DatabaseID = 0
	}
	if n.ParaInfo.PipeOptions.Publish == "" {
		n.ParaInfo.PipeOptions.Publish = cdcpb.EventInsert
	}

	// ready to alter the pipe status or parameters.
	parameters, err := cdcpb.MarshalPipeParameters(n.ParaInfo)
	if err != nil {
		return err
	}

	var status string
	var jobID int64
	// set status of pipe to 'Enable' if it is in primary cluster and option enable is 'on'.
	if n.ParaInfo.PipeOptions.Enable == sqlconst.OptOn {
		status = sqlconst.StatusEnable
		jobID = n.PipeMetadata.JobID
	} else {
		status = sqlconst.StatusDisable
		jobID = 0
	}

	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"write-pipe-info",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_pipes SET parameters=$1,status=$2,source_id=$3,job_id=$4 WHERE id=$5`,
		parameters, status, n.PipeMetadata.DatabaseID, jobID, n.PipeMetadata.ID); err != nil {
		return err
	}

	// check if table list in database is changed
	err = n.checkDatabaseChanged(params, &n.PipeMetadata, lowWatermark)
	if err != nil {
		return err
	}

	// alter status 'on' to 'off'
	if originalStatus == sqlconst.StatusEnable && status == sqlconst.StatusDisable {
		for _, table := range n.ParaInfo.Tables {
			if err := params.GetPlanner().RemoveCDCDescriptorByTableID(
				params.Ctx, table.ID, sqlbase.CDCInstanceType_Pipe, n.PipeMetadata.ID,
			); err != nil {
				return err
			}
		}
	}

	// status has changed, stop the running CDC job.
	if (originalStatus == sqlconst.StatusEnable && status == sqlconst.StatusDisable) ||
		(originalStatus == sqlconst.StatusDisable && status == sqlconst.StatusEnable) {
		if n.JobID != 0 {
			cdcID := uint64(n.JobID)
			// If the CDC is connecting, send a stop command to the job, make the job end on its own;
			// if there is no connection, send a cancel command to the JobRegistry,
			// and the job mechanism will usually end within 30 seconds.
			if len(originalTableIDs) > 0 && params.ExecCfg().CDCCoordinator.HasTask(
				sqlbase.CDCInstanceType_Pipe, originalTableIDs[0], cdcID) {
				for _, id := range originalTableIDs {
					params.PlannerExecCfg().CDCCoordinator.StopCDCByLocal(id, cdcID, sqlbase.CDCInstanceType_Pipe)
					sql.WaitCDCStatusChanged(
						params.Ctx,
						params.ExecCfg().CDCCoordinator,
						id,
						cdcID,
						sqlbase.CDCInstanceType_Pipe,
						false,
					)
				}
			} else {
				job, err := params.ExecCfg().JobRegistry.LoadJobWithTxn(params.Ctx, jobID, params.PlannerTxn())
				if err != nil {
					log.Errorf(params.Ctx, "job load error: %v", err)
				} else {
					if job.WithTxn(params.PlannerTxn()).CheckRunningStatus(params.Ctx) {
						if err = params.ExecCfg().JobRegistry.CancelRequested(params.Ctx, params.PlannerTxn(), jobID); err != nil {
							log.Errorf(params.Ctx, "job cancel error: %v", err)
						}
					}
				}
			}
		}
	}

	// start pipe if alter status 'off' to 'on' in primary cluster.
	if originalStatus == sqlconst.StatusDisable && status == sqlconst.StatusEnable {
		if len(n.ParaInfo.TableIDs) == 0 {
			if _, err = params.ExecCfg().InternalExecutor.ExecEx(
				params.Ctx,
				"update-pipe-jobID",
				params.PlannerTxn(),
				InternalExecutorSessionDataOverride{User: security.RootUser},
				`UPDATE system.kwdb_pipes SET job_id=0 WHERE id=$1`,
				n.PipeMetadata.ID); err != nil {
				return err
			}
		} else {
			if err = sql.CheckCDCMax(params.Ctx, params.GetPlanner(), int64(len(n.ParaInfo.TableIDs))); err != nil {
				return err
			}

			if err := cdcpb.CheckSink(n.ParaInfo.PipeOptions.Sink, true); err != nil {
				if _, err := params.ExecCfg().InternalExecutor.ExecEx(
					params.Ctx,
					"write-pipe-info",
					params.PlannerTxn(),
					InternalExecutorSessionDataOverride{User: security.RootUser},
					`UPDATE system.kwdb_pipes SET status=$1 WHERE id=$2`,
					"Disable", n.PipeMetadata.ID); err != nil {
					return err
				}
				return errors.Errorf("failed to start pipe %q with error: %s", n.Name, err)
			}

			for _, table := range n.ParaInfo.Tables {
				if err := params.GetPlanner().AddCDCDescriptorByTableID(
					params.Ctx, table.ID, sqlbase.CDCInstanceType_Pipe,
					n.PipeMetadata.ID, []byte(n.PipeMetadata.Name),
				); err != nil {
					return err
				}
			}

			jobRecord, err := pipe.BuildPipeJobRecord(
				params, n.n.PipeName, &n.ParaInfo.PipeOptions, n.ParaInfo.Tables, n.ParaInfo.TableIDs, n.ID)
			if err != nil {
				return err
			}

			n.run.resultsCh = make(chan tree.Datums)
			n.run.errCh = make(chan error)
			startCh := make(chan tree.Datums)
			go func() {
				err := sql.StartPipeJob(params.Ctx, params.GetPlanner(), startCh, *jobRecord, &n.PipeMetadata)
				select {
				case <-params.Ctx.Done():
				case n.run.errCh <- err:
				}
				close(n.run.errCh)
				close(n.run.resultsCh)
			}()
		}
	}

	params.GetPlanner().SetAuditTarget(uint32(n.PipeMetadata.ID), n.PipeMetadata.Name.String(), nil)

	return err
}

// alterPipeSetTable reconstructs and updates table list and table id list of parameters in the pipe instance.
func (n *alterPipeNode) alterPipeSetTable(params RunParams) error {
	tableList := []*MutableTableDescriptor{n.tableDesc}
	tableInfos, tableIds, err := sql.MakeCDCTableInfo(
		params.Ctx, params.GetPlanner(), tableList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := sql.CheckWhereExprForCDC(
			params.Ctx, params.GetPlanner(), n.n.Table, n.tableDesc.TableDescriptor, n.n.Where.Expr)
		if err != nil {
			return err
		}
		if whereNeedNormalTag {
			tableInfos[0].NeedNormalTag = true
		}

		tableInfos[0].Filter = n.n.Where.Expr.String()
		if err = sql.CheckWhereExprForHistory(params.Ctx, params.GetPlanner(), &tableInfos[0]); err != nil {
			return err
		}
	}
	n.ParaInfo.Tables = tableInfos
	n.ParaInfo.TableIDs = tableIds

	return nil
}

// alterPipeUpdateTableInfoOnDatabase updates table infos in pipe, if pipe is on database.
func (n *alterPipeNode) alterPipeUpdateTableInfoOnDatabase(
	params RunParams, originalTableMap map[uint64]string,
) error {
	// pipe is on database
	dbDesc, err := sql.MustGetDatabaseDescByID(params.Ctx, params.PlannerTxn(), sqlbase.ID(n.DatabaseID))
	if err != nil {
		return err
	}
	schemas, err := params.GetPlanner().GetSchemasForDatabase(params.Ctx, params.PlannerTxn(), dbDesc.ID)
	if err != nil {
		return err
	}

	// the names of all objects in the target database
	var tableDescList []*MutableTableDescriptor
	for _, schema := range schemas {
		toAppend, err := sql.GetObjectNames(
			params.Ctx, params.PlannerTxn(), params.GetPlanner(), dbDesc, schema, true, /*explicitPrefix*/
		)
		if err != nil {
			return err
		}

		for _, tableName := range toAppend {
			tableDesc, err := params.GetPlanner().ResolveMutableTableDescriptor(
				params.Ctx, &tableName, true /*required*/, sql.ResolveRequireTableDesc,
			)
			if err != nil {
				if strings.Contains(err.Error(), "is being dropped") ||
					strings.Contains(err.Error(), "does not exist") {
					continue
				}
				return err
			}
			if tableDesc.Dropped() {
				continue
			}

			tableDescList = append(tableDescList, tableDesc)
			if _, ok := originalTableMap[uint64(tableDesc.ID)]; !ok {
				err = sql.CheckPipePrivilege(params.Ctx, params.GetPlanner(), tableDesc, privilege.CREATE, nil)
				if err != nil {
					return err
				}
			}
		}
	}
	tableInfos, tableIds, err := sql.MakeCDCTableInfo(
		params.Ctx, params.GetPlanner(), tableDescList, true, nil, false)
	if err != nil {
		return err
	}
	n.ParaInfo.Tables = tableInfos
	n.ParaInfo.TableIDs = tableIds
	return nil
}

// alterPipeUpdateTableList updates table infos in pipe, if pipe is on columns of table, one table, or table list.
func (n *alterPipeNode) alterPipeUpdateTableList(params RunParams) error {
	var newTableDescList []*MutableTableDescriptor
	isStar := false
	for _, table := range n.ParaInfo.Tables {
		isStar = isStar || table.IsStar
		tn := tree.MakeTableName(tree.Name(table.Database), tree.Name(table.Table))
		tableDesc, err := params.GetPlanner().ResolveMutableTableDescriptor(
			params.Ctx, &tn, true /*required*/, sql.ResolveRequireTSTableDesc,
		)
		if err != nil {
			if strings.Contains(err.Error(), "is being dropped") ||
				strings.Contains(err.Error(), "does not exist") {
				continue
			}
			return err
		}
		if tableDesc.Dropped() {
			continue
		}
		newTableDescList = append(newTableDescList, tableDesc)
	}

	var cols tree.NameList
	if len(n.ParaInfo.Tables) > 0 {
		for _, colName := range n.ParaInfo.Tables[0].ColNames {
			cols = append(cols, tree.Name(colName))
		}
	}
	tableInfos, tableIds, err := sql.MakeCDCTableInfo(
		params.Ctx, params.GetPlanner(), newTableDescList, isStar, cols, false)
	if err != nil {
		return err
	}
	n.ParaInfo.Tables = tableInfos
	n.ParaInfo.TableIDs = tableIds
	return nil
}

func (n *alterPipeNode) Next(params RunParams) (bool, error) {
	if n.run.resultsCh != nil {
		select {
		case <-params.Ctx.Done():
			return false, params.Ctx.Err()
		case err := <-n.run.errCh:
			return false, err
		case <-n.run.resultsCh:
			return true, nil
		}
	} else {
		return false, nil
	}
}

// checkDatabaseChanged check the tables of pipe is changed, and reset the table watermark
func (n *alterPipeNode) checkDatabaseChanged(
	params RunParams, md *metadata.PipeMetadata, lowWatermark int64,
) error {
	rows, err := params.ExecCfg().InternalExecutor.Query(
		params.Ctx,
		"pipe-count-history",
		params.PlannerTxn(),
		`SELECT table_id FROM system.kwdb_cdc_watermark WHERE task_id = $1`,
		md.ID,
	)
	if err != nil {
		return err
	}

	needAddTableMap := make(map[uint64]int)
	for _, id := range md.ParaInfo.TableIDs {
		needAddTableMap[id] = 0
	}

	for _, row := range rows {
		tableID := uint64(tree.MustBeDInt(row[0]))
		if _, exist := needAddTableMap[tableID]; exist {
			delete(needAddTableMap, tableID)
		} else {
			// remove dropped table watermark
			if err = params.GetPlanner().RemoveCDCWatermarks(
				params.Ctx,
				sqlbase.CDCInstanceType_Pipe,
				&tableID,
				&md.ID,
			); err != nil {
				return err
			}
		}
	}

	// add created table watermark
	for tableID := range needAddTableMap {
		if err = params.GetPlanner().AddCDCWatermark(params.Ctx, metadata.CDCWatermark{
			TableID:      tableID,
			TaskID:       md.ID,
			TaskType:     sqlbase.CDCInstanceType_Pipe,
			InternalType: cdcpb.CDCInternalTypeUnknown,
			LowWatermark: cdcpb.InvalidWatermark,
			ClientID:     nil,
		}); err != nil {
			return err
		}
	}

	if lowWatermark > cdcpb.InvalidWatermark {
		stmt := fmt.Sprintf(
			`UPDATE system.kwdb_cdc_watermark SET low_watermark=$1 WHERE task_type = %d  AND task_id = %d`,
			sqlbase.CDCInstanceType_Pipe,
			md.ID,
		)

		if _, err := params.ExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			"update-cdc-watermark",
			params.PlannerTxn(),
			InternalExecutorSessionDataOverride{User: security.RootUser},
			stmt,
			lowWatermark,
		); err != nil {
			return nil
		}
	}

	return nil
}

func (*alterPipeNode) Values() tree.Datums { return tree.Datums{} }

func (*alterPipeNode) Close(context.Context) {}
