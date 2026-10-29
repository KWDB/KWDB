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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type alterPipeNode struct {
	PipeMetadata
	n         *tree.AlterPipe
	tableDesc *sqlbase.MutableTableDescriptor
	pipeOpts  func() (map[string]string, error)
	run       pipeComputeRun
}

// AlterPipe creates a alter pipe node for exec.
func (p *planner) AlterPipe(ctx context.Context, n *tree.AlterPipe) (planNode, error) {
	pipe, err := p.loadPipeByName(ctx, n.PipeName)
	if err != nil {
		return nil, err
	}
	if pipe == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "pipe %q does not exist", n.PipeName)
	}
	if err = p.checkPipePrivilege(ctx, nil, privilege.UPDATE, pipe); err != nil {
		return nil, err
	}
	var alterNode alterPipeNode
	alterNode.n = n
	alterNode.PipeMetadata = *pipe

	var tableDesc *MutableTableDescriptor
	if n.Table.TableName != "" {
		// ALTER TABLE case
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, &n.Table, true /*required*/, ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDesc.IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts table")
		}

		if err = p.checkPipePrivilege(ctx, tableDesc, privilege.UPDATE, pipe); err != nil {
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

func (n *alterPipeNode) startExec(params runParams) (err error) {
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
	originalStatus := n.PipeMetadata.status
	originalTableIDs := n.paraInfo.TableIDs
	var originalTableMap = make(map[uint64]string, len(n.paraInfo.TableIDs))
	for idx, table := range n.paraInfo.Tables {
		originalTableMap[n.paraInfo.TableIDs[idx]] = table.Filter
	}
	changeTargetObj := false

	if n.pipeOpts == nil {
		// ALTER PIPE name SET TABLE
		if originalStatus == statusEnable {
			return errors.Errorf("pipe %q is running", n.name)
		}

		if err = n.alterPipeSetTable(params); err != nil {
			return err
		}

		changeTargetObj = true
	} else if n.databaseID > 0 && originalStatus == statusDisable {
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
		if originalStatus == statusEnable {
			// cannot alter 'sink' and 'message_format' if the pipe is still running.
			if pipeOpts[optSink] != "" ||
				pipeOpts[optMessageFormat] != "" ||
				pipeOpts[optIgnoreHistory] != "" ||
				pipeOpts[optBufferSize] != "" ||
				pipeOpts[optLowWatermark] != "" ||
				pipeOpts[optPublish] != "" {
				return errors.Errorf("pipe %q is running.", n.name)
			}
			if len(pipeOpts) == 1 && pipeOpts[optEnable] == optOn {
				return nil
			}
		} else {
			if len(pipeOpts) == 1 && pipeOpts[optEnable] == optOff {
				return nil
			}
		}

		opts, watermark, err := makePipeOptions(pipeOpts, &n.paraInfo.PipeOptions)
		if err != nil {
			return err
		}
		n.paraInfo.PipeOptions = opts
		lowWatermark = watermark

		if n.databaseID == 0 {
			// pipe on database has been checked in n.databaseID > 0 branch.
			// check whether the table(s) in pipe is valid
			err = n.alterPipeUpdateTableList(params)
			if err != nil {
				return err
			}
		}
	}

	for i := range n.paraInfo.Tables {
		n.paraInfo.Tables[i].NeedNormalTag = n.paraInfo.Tables[i].NeedNormalTag &&
			n.paraInfo.PipeOptions.CheckTag == optOn
		// fill table filter in pipe with original filter if change pipe options.
		if !changeTargetObj {
			n.paraInfo.Tables[i].Filter = originalTableMap[n.paraInfo.TableIDs[i]]
		}
	}

	// alter pipe options after migration
	if n.paraInfo.PipeOptions.IgnoreHistory == "" {
		n.paraInfo.PipeOptions.IgnoreHistory = optOn
		n.paraInfo.PipeOptions.BufferSize = defaultBufferSize
		n.paraInfo.PipeOptions.CheckTag = optOn
		n.PipeMetadata.databaseID = 0
	}
	if n.paraInfo.PipeOptions.Publish == "" {
		n.paraInfo.PipeOptions.Publish = cdcpb.EventInsert
	}

	// ready to alter the pipe status or parameters.
	parameters, err := cdcpb.MarshalPipeParameters(n.paraInfo)
	if err != nil {
		return err
	}

	var status string
	var jobID int64
	// set status of pipe to 'Enable' if it is in primary cluster and option enable is 'on'.
	if n.paraInfo.PipeOptions.Enable == optOn {
		status = statusEnable
		jobID = n.PipeMetadata.jobID
	} else {
		status = statusDisable
		jobID = 0
	}

	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"write-pipe-info",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_pipes SET parameters=$1,status=$2,source_id=$3,job_id=$4 WHERE id=$5`,
		parameters, status, n.PipeMetadata.databaseID, jobID, n.PipeMetadata.id); err != nil {
		return err
	}

	// check if table list in database is changed
	err = n.checkDatabaseChanged(params, &n.PipeMetadata, lowWatermark)
	if err != nil {
		return err
	}

	// alter status 'on' to 'off'
	if originalStatus == statusEnable && status == statusDisable {
		for _, table := range n.paraInfo.Tables {
			if err := params.p.removeCDCDescriptorByTableID(
				params.ctx, table.ID, sqlbase.CDCInstanceType_Pipe, n.PipeMetadata.id,
			); err != nil {
				return err
			}
		}
	}

	// status has changed, stop the running CDC job.
	if (originalStatus == statusEnable && status == statusDisable) ||
		(originalStatus == statusDisable && status == statusEnable) {
		if n.jobID != 0 {
			cdcID := uint64(n.jobID)
			// If the CDC is connecting, send a stop command to the job, make the job end on its own;
			// if there is no connection, send a cancel command to the JobRegistry,
			// and the job mechanism will usually end within 30 seconds.
			if len(originalTableIDs) > 0 && params.p.ExecCfg().CDCCoordinator.HasTask(
				sqlbase.CDCInstanceType_Pipe, originalTableIDs[0], cdcID) {
				for _, id := range originalTableIDs {
					params.p.ExecCfg().CDCCoordinator.StopCDCByLocal(id, cdcID, sqlbase.CDCInstanceType_Pipe)
					waitCDCStatusChanged(
						params.ctx,
						params.p.ExecCfg().CDCCoordinator,
						id,
						cdcID,
						sqlbase.CDCInstanceType_Pipe,
						false,
					)
				}
			} else {
				job, err := params.p.execCfg.JobRegistry.LoadJobWithTxn(params.ctx, jobID, params.p.txn)
				if err != nil {
					log.Errorf(params.ctx, "job load error: %v", err)
				} else {
					if job.WithTxn(params.p.txn).CheckRunningStatus(params.ctx) {
						if err = params.p.execCfg.JobRegistry.CancelRequested(params.ctx, params.p.txn, jobID); err != nil {
							log.Errorf(params.ctx, "job cancel error: %v", err)
						}
					}
				}
			}
		}
	}

	// start pipe if alter status 'off' to 'on' in primary cluster.
	if originalStatus == statusDisable && status == statusEnable {
		if len(n.paraInfo.TableIDs) == 0 {
			if _, err = params.ExecCfg().InternalExecutor.ExecEx(
				params.ctx,
				"update-pipe-jobID",
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				`UPDATE system.kwdb_pipes SET job_id=0 WHERE id=$1`,
				n.PipeMetadata.id); err != nil {
				return err
			}
		} else {
			if err = params.p.checkCDCMax(params.ctx, int64(len(n.paraInfo.TableIDs))); err != nil {
				return err
			}

			if err := cdcpb.CheckSink(n.paraInfo.PipeOptions.Sink, true); err != nil {
				if _, err := params.ExecCfg().InternalExecutor.ExecEx(
					params.ctx,
					"write-pipe-info",
					params.p.txn,
					sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
					`UPDATE system.kwdb_pipes SET status=$1 WHERE id=$2`,
					"Disable", n.PipeMetadata.id); err != nil {
					return err
				}
				return errors.Errorf("failed to start pipe %q with error: %s", n.name, err)
			}

			for _, table := range n.paraInfo.Tables {
				if err := params.p.addCDCDescriptorByTableID(
					params.ctx, table.ID, sqlbase.CDCInstanceType_Pipe,
					n.PipeMetadata.id, []byte(n.PipeMetadata.name),
				); err != nil {
					return err
				}
			}

			jobRecord, err := buildPipeJobRecord(
				params, n.n.PipeName, &n.paraInfo.PipeOptions, n.paraInfo.Tables, n.paraInfo.TableIDs, n.id)
			if err != nil {
				return err
			}

			n.run.resultsCh = make(chan tree.Datums)
			n.run.errCh = make(chan error)
			startCh := make(chan tree.Datums)
			go func() {
				err := params.p.startPipeJob(params.ctx, startCh, *jobRecord, &n.PipeMetadata)
				select {
				case <-params.ctx.Done():
				case n.run.errCh <- err:
				}
				close(n.run.errCh)
				close(n.run.resultsCh)
			}()
		}
	}

	params.p.SetAuditTarget(uint32(n.PipeMetadata.id), n.PipeMetadata.name.String(), nil)

	return err
}

// alterPipeSetTable reconstructs and updates table list and table id list of parameters in the pipe instance.
func (n *alterPipeNode) alterPipeSetTable(params runParams) error {
	tableList := []*MutableTableDescriptor{n.tableDesc}
	tableInfos, tableIds, err := params.p.makeCDCTableInfo(
		params.ctx, tableList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := params.p.checkWhereExprForCDC(
			params.ctx, n.n.Table, n.tableDesc.TableDescriptor, n.n.Where.Expr)
		if err != nil {
			return err
		}
		if whereNeedNormalTag {
			tableInfos[0].NeedNormalTag = true
		}

		tableInfos[0].Filter = n.n.Where.Expr.String()
		if err = params.p.checkWhereExprForHistory(params.ctx, &tableInfos[0]); err != nil {
			return err
		}
	}
	n.paraInfo.Tables = tableInfos
	n.paraInfo.TableIDs = tableIds

	return nil
}

// alterPipeUpdateTableInfoOnDatabase updates table infos in pipe, if pipe is on database.
func (n *alterPipeNode) alterPipeUpdateTableInfoOnDatabase(
	params runParams, originalTableMap map[uint64]string,
) error {
	// pipe is on database
	dbDesc, err := getDatabaseDescByID(params.ctx, params.p.txn, sqlbase.ID(n.databaseID))
	if err != nil {
		return err
	}
	schemas, err := params.p.Tables().getSchemasForDatabase(params.ctx, params.p.txn, dbDesc.ID)
	if err != nil {
		return err
	}

	// the names of all objects in the target database
	var tableDescList []*MutableTableDescriptor
	for _, schema := range schemas {
		toAppend, err := GetObjectNames(
			params.ctx, params.p.txn, params.p, dbDesc, schema, true, /*explicitPrefix*/
		)
		if err != nil {
			return err
		}

		for _, tableName := range toAppend {
			tableDesc, err := params.p.ResolveMutableTableDescriptor(
				params.ctx, &tableName, true /*required*/, ResolveRequireTableDesc,
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
				err = params.p.checkPipePrivilege(params.ctx, tableDesc, privilege.CREATE, nil)
				if err != nil {
					return err
				}
			}
		}
	}
	tableInfos, tableIds, err := params.p.makeCDCTableInfo(
		params.ctx, tableDescList, true, nil, false)
	if err != nil {
		return err
	}
	n.paraInfo.Tables = tableInfos
	n.paraInfo.TableIDs = tableIds
	return nil
}

// alterPipeUpdateTableList updates table infos in pipe, if pipe is on columns of table, one table, or table list.
func (n *alterPipeNode) alterPipeUpdateTableList(params runParams) error {
	var newTableDescList []*MutableTableDescriptor
	isStar := false
	for _, table := range n.paraInfo.Tables {
		isStar = isStar || table.IsStar
		tn := tree.MakeTableName(tree.Name(table.Database), tree.Name(table.Table))
		tableDesc, err := params.p.ResolveMutableTableDescriptor(
			params.ctx, &tn, true /*required*/, ResolveRequireTSTableDesc,
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
	if len(n.paraInfo.Tables) > 0 {
		for _, colName := range n.paraInfo.Tables[0].ColNames {
			cols = append(cols, tree.Name(colName))
		}
	}
	tableInfos, tableIds, err := params.p.makeCDCTableInfo(
		params.ctx, newTableDescList, isStar, cols, false)
	if err != nil {
		return err
	}
	n.paraInfo.Tables = tableInfos
	n.paraInfo.TableIDs = tableIds
	return nil
}

func (n *alterPipeNode) Next(params runParams) (bool, error) {
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

// checkDatabaseChanged check the tables of pipe is changed, and reset the table watermark
func (n *alterPipeNode) checkDatabaseChanged(
	params runParams, metadata *PipeMetadata, lowWatermark int64,
) error {
	rows, err := params.ExecCfg().InternalExecutor.Query(
		params.ctx,
		"pipe-count-history",
		params.p.txn,
		`SELECT table_id FROM system.kwdb_cdc_watermark WHERE task_id = $1`,
		metadata.id,
	)
	if err != nil {
		return err
	}

	needAddTableMap := make(map[uint64]int)
	for _, id := range metadata.paraInfo.TableIDs {
		needAddTableMap[id] = 0
	}

	for _, row := range rows {
		tableID := uint64(tree.MustBeDInt(row[0]))
		if _, exist := needAddTableMap[tableID]; exist {
			delete(needAddTableMap, tableID)
		} else {
			// remove dropped table watermark
			if err = params.p.removeCDCWatermarks(
				params.ctx,
				sqlbase.CDCInstanceType_Pipe,
				&tableID,
				&metadata.id,
			); err != nil {
				return err
			}
		}
	}

	// add created table watermark
	for tableID := range needAddTableMap {
		if err = params.p.addCDCWatermark(params.ctx, CDCWatermark{
			TableID:      tableID,
			TaskID:       metadata.id,
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
			metadata.id,
		)

		if _, err := params.p.ExecCfg().InternalExecutor.ExecEx(
			params.ctx,
			"update-cdc-watermark",
			params.p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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
