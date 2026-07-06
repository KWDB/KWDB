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
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// CDCWatermark saved the LowWatermark of table for cdc.
type CDCWatermark struct {
	TableID      uint64
	TaskID       uint64
	TaskType     sqlbase.CDCInstanceType
	InternalType int32
	LowWatermark int64
	ClientID     interface{}
}

// cdcComputeRun is used in subscription to hold the result or error from the corresponding publication.
type cdcComputeRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

// createTsInsertWithCDCNodeForSingleMode construct TsInsertWithCDCProSpec for SingeMode and processors.
func createTsInsertWithCDCNodeForSingleMode(n *tsInsertWithCDCNode) (PhysicalPlan, error) {
	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(n.nodeIDs))
	p.Processors = make([]physicalplan.Processor, 0, len(n.nodeIDs))

	// Construct a processor for the payload of each node.
	// For Single mode, n.nodeIDs only contain local node.
	for i := 0; i < len(n.nodeIDs); i++ {
		var tsInsert = &execinfrapb.TsInsertWithCDCProSpec{}
		tsInsert.PayLoad = make([][]byte, len(n.allNodePayloadInfos[i]))
		tsInsert.RowNums = make([]uint32, len(n.allNodePayloadInfos[i]))
		tsInsert.PrimaryTagKey = make([][]byte, len(n.allNodePayloadInfos[i]))
		tsInsert.CDCData = buildCDCDataProto(n.CDCData)

		for j := range n.allNodePayloadInfos[i] {
			tsInsert.PayLoad[j] = n.allNodePayloadInfos[i][j].Payload
			tsInsert.RowNums[j] = n.allNodePayloadInfos[i][j].RowNum
			tsInsert.PrimaryTagKey[j] = n.allNodePayloadInfos[i][j].PrimaryTagKey
		}

		proc := physicalplan.Processor{
			Node: n.nodeIDs[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{TsInsertWithCDC: tsInsert},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.GateNoopInput = len(n.nodeIDs)
	p.TsOperator = execinfrapb.OperatorType_TsInsert

	return p, nil
}

// createTsTsInsertWithCDCNodeForDistributeMode constructs TsInsertWithCDCProSpec for DistributeMode and processors.
// It is revoked when creating DistSQLPlanner for ts insert with CDC.
func createTsTsInsertWithCDCNodeForDistributeMode(n *tsInsertWithCDCNode) (PhysicalPlan, error) {
	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(n.nodeIDs))
	p.Processors = make([]physicalplan.Processor, 0, len(n.nodeIDs))

	// Construct a processor for the payload of each node.
	for i := 0; i < len(n.nodeIDs); i++ {
		var tsInsert = &execinfrapb.TsInsertWithCDCProSpec{}
		payloadNum := len(n.allNodePayloadInfos[i])
		tsInsert.RowNums = make([]uint32, payloadNum)
		tsInsert.PrimaryTagKey = make([][]byte, payloadNum)
		tsInsert.AllPayload = make([]*execinfrapb.PayloadForDistributeMode, payloadNum)
		tsInsert.PayloadPrefix = make([][]byte, payloadNum)
		tsInsert.CDCData = buildCDCDataProto(n.CDCData)

		for j := range n.allNodePayloadInfos[i] {
			tsInsert.RowNums[j] = n.allNodePayloadInfos[i][j].RowNum
			tsInsert.PrimaryTagKey[j] = n.allNodePayloadInfos[i][j].PrimaryTagKey
			tsInsert.PayloadPrefix[j] = n.allNodePayloadInfos[i][j].Payload
			payloadForDistributeMode := &execinfrapb.PayloadForDistributeMode{
				Row:       n.allNodePayloadInfos[i][j].RowBytes,
				StartKey:  n.allNodePayloadInfos[i][j].StartKey,
				EndKey:    n.allNodePayloadInfos[i][j].EndKey,
				ValueSize: n.allNodePayloadInfos[i][j].ValueSize,
			}
			tsInsert.AllPayload[j] = payloadForDistributeMode
		}
		proc := physicalplan.Processor{
			Node: n.nodeIDs[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{TsInsertWithCDC: tsInsert},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.GateNoopInput = len(n.nodeIDs)
	p.TsOperator = execinfrapb.OperatorType_TsInsert

	return p, nil
}

// buildCDCDataProto builds CDC data to Protobuf format.
func buildCDCDataProto(cdcData *sqlbase.CDCData) execinfrapb.CDCData {
	var reconstructData []*execinfrapb.CDCPushData
	for _, data := range cdcData.PushData {
		pushData := execinfrapb.CDCPushData{
			TaskID:   data.TaskID,
			TaskType: data.TaskType,
			Data:     data.Rows,
		}

		reconstructData = append(reconstructData, &pushData)
	}

	return execinfrapb.CDCData{
		TableID:      cdcData.TableID,
		MinTimestamp: cdcData.MinTimestamp,
		PushData:     reconstructData,
	}
}

// addCDCWatermark adds a record with low watermark in system.kwdb_cdc_watermark for the specified table and task
// (such as pipe, subscription, stream and so on). The low watermark is max timestamp of retrieved data from table,
// and if the task restarted, it will retrieve data from low watermark.
func (p *planner) addCDCWatermark(ctx context.Context, cdcWatermark CDCWatermark) error {
	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"insert-cdc-watermark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_cdc_watermark
(table_id,task_id,task_type,internal_type,low_watermark,client_id)
values ($1,$2,$3,$4,$5,$6) ON CONFLICT(table_id,task_id,task_type,internal_type) DO NOTHING`,
		cdcWatermark.TableID,
		cdcWatermark.TaskID,
		cdcWatermark.TaskType,
		cdcWatermark.InternalType,
		cdcWatermark.LowWatermark,
		cdcWatermark.ClientID,
	); err != nil {
		return err
	}

	return nil
}

// addCDCDescriptorByTableID add CDC descriptor to table descriptor.
func (p *planner) addCDCDescriptorByTableID(
	ctx context.Context,
	tableID uint64,
	instanceType sqlbase.CDCInstanceType,
	taskID uint64,
	parameters []byte,
) error {
	tableDesc, err := sqlbase.GetMutableTableDescFromID(
		ctx, p.txn, sqlbase.ID(tableID),
	)
	if err != nil {
		return err
	}

	return p.addCDCDescriptor(ctx, tableDesc, instanceType, taskID, parameters)
}

// addCDCDescriptor add CDC descriptor to table descriptor.
func (p *planner) addCDCDescriptor(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	instanceType sqlbase.CDCInstanceType,
	taskID uint64,
	parameters []byte,
) error {
	for _, cdc := range tableDesc.CDC {
		if taskID == cdc.ID && instanceType == cdc.CdcType {
			//return pgerror.Newf(pgcode.InvalidObjectDefinition, "CDC \"%d\" already exists on table %s", taskID, tableDesc.Name)
			return nil
		}
	}
	// create cdcDesc, add it to tableDesc
	tableDesc.AddCDC(sqlbase.CDCDescriptor{
		ID:         taskID,
		CdcType:    instanceType,
		Parameters: parameters,
		Version:    0,
	})

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, "create cdc",
	)
}

// loadCDCWatermarks loads cdc watermarks from system.kwdb_cdc_watermark, and returns slice of CDCWatermark.
func (p *planner) loadCDCWatermarks(
	ctx context.Context, instanceType sqlbase.CDCInstanceType, tableID, taskID *uint64,
) ([]CDCWatermark, error) {
	stmt := fmt.Sprintf(
		`SELECT table_id,task_id,task_type,low_watermark FROM system.kwdb_cdc_watermark WHERE task_type = %d`,
		instanceType)
	if tableID != nil {
		stmt += fmt.Sprintf(" AND table_id = %d", *tableID)
	}

	if taskID != nil {
		stmt += fmt.Sprintf(" AND task_id = %d", *taskID)
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"query-cdc-watermark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return nil, err
	}

	cdcWatermarks := make([]CDCWatermark, len(rows))

	for i, row := range rows {
		cdcWatermarks[i] = CDCWatermark{
			TableID:      uint64(tree.MustBeDInt(row[0])),
			TaskID:       uint64(tree.MustBeDInt(row[1])),
			TaskType:     sqlbase.CDCInstanceType(tree.MustBeDInt(row[2])),
			LowWatermark: int64(tree.MustBeDInt(row[1])),
		}
	}

	return cdcWatermarks, nil
}

// removeCDCDescriptorByTableName deletes the CDC from TableDescriptor by table name.
func (p *planner) removeCDCDescriptorByTableID(
	ctx context.Context, tableID uint64, instanceType sqlbase.CDCInstanceType, taskID uint64,
) error {
	tableDesc, err := sqlbase.GetMutableTableDescFromID(
		ctx, p.txn, sqlbase.ID(tableID),
	)
	if err != nil {
		return err
	}

	return p.removeCDCDescriptor(ctx, tableDesc, instanceType, taskID)
}

// removeCDC deletes the CDC from TableDescriptor.
func (p *planner) removeCDCDescriptor(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	instanceType sqlbase.CDCInstanceType,
	taskID uint64,
) error {
	cdcIdx := -1
	// find cdc which is being dropped
	for i := 0; i < len(tableDesc.CDC); i++ {
		if !tableDesc.Dropped() && tableDesc.CDC[i].ID == taskID && instanceType == tableDesc.CDC[i].CdcType {
			cdcIdx = i
			break
		}
	}

	if cdcIdx == -1 {
		return nil
	}

	tableDesc.CDC = append(tableDesc.CDC[:cdcIdx], tableDesc.CDC[cdcIdx+1:]...)

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, "drop cdc",
	)
}

// removeCDCWatermarks deletes the record(s) from system.kwdb_cdc_watermark.
func (p *planner) removeCDCWatermarks(
	ctx context.Context, instanceType sqlbase.CDCInstanceType, tableID, taskID *uint64,
) error {
	stmt := fmt.Sprintf(`DELETE FROM system.kwdb_cdc_watermark WHERE task_type = %d`, instanceType)
	if tableID != nil {
		stmt += fmt.Sprintf(" AND table_id = %d", *tableID)
	}

	if taskID != nil {
		stmt += fmt.Sprintf(" AND task_id = %d", *taskID)
	}

	_, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-cdc-watermark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)

	return err
}

// removeUnwantedCDCWatermarks deletes the unwanted record(s) from system.kwdb_cdc_watermark.
// wantedTableIDs is the wanted records, and delete the records with the same task_id and task_type but table_id are not
// in wantedTableIDs.
func (p *planner) removeUnwantedCDCWatermarks(
	ctx context.Context,
	instanceType sqlbase.CDCInstanceType,
	wantedTableIDs []string,
	taskID *uint64,
) error {
	if len(wantedTableIDs) == 0 {
		return nil
	}
	wantedTableIDStr := strings.Join(wantedTableIDs, ",")
	stmt := fmt.Sprintf(`DELETE FROM system.kwdb_cdc_watermark WHERE task_type = %d AND task_id = %d AND table_id NOT IN (%s)`,
		instanceType,
		*taskID,
		wantedTableIDStr,
	)

	_, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-unwanted-cdc-watermark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)

	return err
}

// tableHasCDC checks whether the specified table has cdc tasks with specified type.
func (p *planner) tableHasCDC(
	ctx context.Context, instanceType sqlbase.CDCInstanceType, tableID *uint64,
) (bool, error) {
	stmt := fmt.Sprintf(
		`SELECT table_id FROM system.kwdb_cdc_watermark WHERE task_type = %d`,
		instanceType)
	if tableID != nil {
		stmt += fmt.Sprintf(" AND table_id = %d", *tableID)
	}

	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"query-cdc-watermark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return false, err
	}

	return len(rows) > 0, nil
}

// sendDDLToPipe sends the ddl stmt to related pipe and restart pipe.
// 1. stop the running pipe.
// 2. send ddl statement to pipe. if send ddl failed, record it in system.kwdb_unpush for resending.
// 3. start the pipe again.
func sendDDLToPipe(
	params runParams,
	dbName string,
	schemaName string,
	tableName string,
	ddlType string,
	stmt string,
	pipeMetadata []*PipeMetadata,
	currentTxn bool,
) error {
	// record DDL stmt
	osn := sqlbase.TSIDToTime(params.ExecCfg().TsIDGen.GetNextID())
	for _, pipeMeta := range pipeMetadata {
		if needRecordDDL(pipeMeta) {
			if err := recordDDLtoUnpush(
				params, false, pipeMeta.id, dbName, schemaName, tableName, ddlType, stmt, osn,
			); err != nil {
				return err
			}
		}
	}

	// stop all pipes of table and send to pipes.
	query := ""
	var restartPipes []int
	for i, pipeMeta := range pipeMetadata {
		if !needToRestart(pipeMeta) {
			continue
		}

		if currentTxn {
			_ = params.p.Txn().Commit(params.ctx)
			currentTxn = false
		}

		log.Infof(params.ctx, "pipe[%s] is disabled by %s. stmt:%s", pipeMeta.name, ddlType, stmt)
		query = fmt.Sprintf(`ALTER PIPE %s SET OPTIONS(enable='off')`, pipeMeta.name)
		err := execStatementAboutPipe(params, currentTxn,
			"alter-pipe-off", query)
		if err != nil {
			log.Warningf(params.ctx, "alter pipe[%s] off in %s failed. err: %s", pipeMeta.name, ddlType, err)
		}

		restartPipes = append(restartPipes, i)

		if needSend(pipeMeta) {
			sinURI := pipeMeta.paraInfo.PipeOptions.Sink
			bufferSize := pipeMeta.paraInfo.PipeOptions.BufferSize
			err = params.ExecCfg().CDCCoordinator.SendToPipeImmediately(
				params.ctx,
				dbName,
				schemaName,
				tableName,
				ddlType,
				stmt,
				sinURI,
				bufferSize,
				osn,
			)
			if err != nil {
				log.Warningf(params.ctx, "pipe[%s] sends %s ddl failed. err: %s", pipeMeta.name, ddlType, err)
			}
		}
	}

	// restart pipes
	for _, i := range restartPipes {
		pipeMeta := pipeMetadata[i]

		log.Infof(params.ctx, "pipe[%s] is enabled by %s. stmt:%s", pipeMeta.name, ddlType, stmt)
		// restart pipe and reset low-watermark to osn
		query = fmt.Sprintf(`ALTER PIPE %s SET OPTIONS(enable='on',low_watermark=%v)`,
			pipeMeta.name, tree.MakeDTimestamp(osn, sqlbase.TSIDDPrecision))
		err := execStatementAboutPipe(params, currentTxn, "alter-pipe-on", query)
		if err != nil {
			log.Warningf(params.ctx, "alter pipe[%s] on in %s failed. err: %s", pipeMeta.name, ddlType, err)
		}
	}

	return nil
}

// recordDDLtoUnpush records the ddl to unpush.
func recordDDLtoUnpush(
	params runParams,
	currentTxn bool,
	id uint64,
	dbName, schemaName, tableName, ddlType, stmt string,
	osn time.Time,
) error {
	err := execStatementAboutPipe(
		params,
		currentTxn,
		"insert-pipe-unsend",
		`INSERT INTO system.kwdb_unpush(
    pusher_id, type, database_name, schema_name, table_name, operation, op_time, statement
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		id,
		sqlbase.CDCInstanceType_Pipe,
		dbName,
		schemaName,
		tableName,
		ddlType,
		osn,
		stmt,
	)

	if err != nil {
		log.Warningf(params.ctx, "inserts record in system.kwdb_unpush failed. stmt: %s. %v", stmt, err)
		return err
	}

	return nil
}

// execStatementAboutPipe executes the specified query with specified args.
// if withCurrentTxn is true, the query is executed with current txn. Otherwise, create new txn and execute the query.
func execStatementAboutPipe(
	params runParams, withCurrentTxn bool, op string, query string, args ...interface{},
) error {
	var err error
	if withCurrentTxn {
		if len(args) > 0 {
			_, err = params.ExecCfg().InternalExecutor.ExecEx(
				params.ctx,
				op,
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				query,
				args...,
			)
		} else {
			_, err = params.ExecCfg().InternalExecutor.ExecEx(
				params.ctx,
				op,
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				query,
			)
		}
	} else {
		if len(args) > 0 {
			err = params.p.ExecCfg().DB.Txn(params.ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, errInner := params.ExecCfg().InternalExecutor.ExecEx(
					params.ctx,
					op,
					txn,
					sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
					query,
					args...,
				)
				return errInner
			})
		} else {
			err = params.p.ExecCfg().DB.Txn(params.ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, errInner := params.ExecCfg().InternalExecutor.ExecEx(
					params.ctx,
					op,
					txn,
					sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
					query,
				)
				return errInner
			})
		}

	}
	return err
}

// needRecordDDL returns if the pipe need save DDL
func needRecordDDL(pipeMeta *PipeMetadata) bool {
	if pipeMeta.paraInfo.PipeOptions.IgnoreHistory == optOn {
		return false
	}

	if !strings.Contains(pipeMeta.paraInfo.PipeOptions.Publish, cdcpb.EventAll) &&
		!strings.Contains(pipeMeta.paraInfo.PipeOptions.Publish, cdcpb.EventDDL) {
		return false
	}

	return true
}

// When the pipe is in the disabled state, the table low_watermark is not InvalidWatermark,
// and the user has disabled ignore_history, it is necessary to record the DDL.
// After restarting the pipe, send the data.
func needSend(pipeMeta *PipeMetadata) bool {
	if pipeMeta.status == statusDisable {
		return false
	}

	if !strings.Contains(pipeMeta.paraInfo.PipeOptions.Publish, cdcpb.EventAll) &&
		!strings.Contains(pipeMeta.paraInfo.PipeOptions.Publish, cdcpb.EventDDL) {
		return false
	}

	return true
}

// needToRestart returns if the pipe need restart when DDL.
func needToRestart(pipeMeta *PipeMetadata) bool {
	return pipeMeta.status == statusEnable
}

// checkDatabaseRelatedPubsAndSubs checks whether the specified databases have been published or subscribed.
// Return error if anyone of the specified databases has been published or subscribed.
func checkDatabaseRelatedPubsAndSubs(
	ctx context.Context, p *planner, dbDesc *sqlbase.DatabaseDescriptor,
) error {
	if dbDesc == nil {
		return nil
	}
	// check whether the database to be deleted has been published.
	pubDBMap, _, err := p.fetchAllPublishedObjects(ctx, dbDesc)
	if err != nil {
		return err
	}
	dbName := dbDesc.GetName()
	if published, ok := pubDBMap[dbName]; ok && published {
		return pgerror.Newf(pgcode.ObjectInUse,
			"database %q is published and cannot add table", dbName)
	}

	return nil
}

// checkTableRelatedPubsAndSubs checks whether the specified table has been published or subscribed.
// Return error if the specified table has been published or subscribed.
func checkTableRelatedPubsAndSubs(
	ctx context.Context,
	p *planner,
	tableID uint64,
	tableName string,
	opName string,
	dbDesc *DatabaseDescriptor,
) error {
	// check whether the database to be deleted has been published.
	_, pubTableMap, err := p.fetchAllPublishedObjects(ctx, dbDesc)
	if err != nil {
		return err
	}
	if _, ok := pubTableMap[tableID]; ok {
		return pgerror.Newf(pgcode.ObjectInUse,
			"table %s is published and cannot be %s", tableName, opName)
	}

	return nil
}
