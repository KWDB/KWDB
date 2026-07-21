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
//

package cdc

import (
	"context"
	gojson "encoding/json"
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	onsCleanInterval = time.Second * 15
	onsCleanTimeout  = time.Second * 5
)

// Coordinator implements the CDCCoordinator interface
type Coordinator struct {
	settings                  *cluster.Settings
	stopper                   *stop.Stopper
	gossip                    *gossip.Gossip
	statusServer              serverpb.StatusServer
	internalExecutor          *sql.InternalExecutor
	partitionInternalExecutor sqlutil.InternalExecutor
	jobRegistry               *jobs.Registry
	tsEngine                  *tse.TsEngine

	lock syncutil.RWMutex
	// cdcTaskGroups stores the mapping cdcInstanceType->tableID->instanceID->Task.
	// for pipe, the instanceID is the job ID.
	cdcTaskGroups map[sqlbase.CDCInstanceType]map[uint64]map[uint64]Task
	// cdcInstanceMap stores the mapping pipeID->instanceID
	cdcInstanceMap map[uint64]uint64
	// lastSetOSN is the last time that set OSN to tsEngine
	lastSetOSN time.Time
	// setOSNRunning ensures that OSN setting is not executed concurrently.
	setOSNRunning chan struct{}
}

var _ execinfra.CDCCoordinator = &Coordinator{}

// NewCoordinator creates a CDCCoordinator instance
func NewCoordinator(
	settings *cluster.Settings,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
	internalExecutor *sql.InternalExecutor,
	statusServer serverpb.StatusServer,
	jobRegistry *jobs.Registry,
) *Coordinator {
	coordinator := &Coordinator{
		settings:         settings,
		stopper:          stopper,
		gossip:           gossip,
		internalExecutor: internalExecutor,
		statusServer:     statusServer,
		jobRegistry:      jobRegistry,
		cdcTaskGroups:    make(map[sqlbase.CDCInstanceType]map[uint64]map[uint64]Task),
		cdcInstanceMap:   make(map[uint64]uint64),
		lastSetOSN:       timeutil.Now(),
		setOSNRunning:    make(chan struct{}, 1),
	}

	cdcpb.RegisterCDCCoordinatorServer(grpcServer, coordinator)

	return coordinator
}

// TsCDC implements the grpc CDCCoordinator server.
// It receives request from CDC consumer.
func (c *Coordinator) TsCDC(server cdcpb.CDCCoordinator_TsCDCServer) error {
	var err error
	for {
		event, errRecv := server.Recv()
		if errRecv != nil {
			err = errRecv
			break
		}

		switch t := event.GetValue().(type) {
		case *cdcpb.TsChangeDataCaptureRequest:
			ctx := server.Context()
			go func() {
				select {
				case <-ctx.Done():
					return
				default:
					_ = c.StartTsCDC(t, server)
				}
			}()
			c.lock.Lock()
			c.lastSetOSN = timeutil.FromUnixMilli(0)
			c.lock.Unlock()
		case *cdcpb.TsChangeDataCaptureHeartbeat:
			c.lock.Lock()
			now := timeutil.Now()
			needOSN := now.Sub(c.lastSetOSN) > onsCleanInterval
			c.lock.Unlock()
			if needOSN {
				if err = c.SetCDCTableOSN(context.Background()); err == nil {
					c.lock.Lock()
					c.lastSetOSN = now
					c.lock.Unlock()
				}
			}

			if err != nil {
				return err
			}
		case *cdcpb.TsChangeDataCaptureValue:
		case *cdcpb.TsChangeDataCaptureStart:
			_ = c.enableTask(t.TableID, t.InstanceID, t.InstanceType)
		case *cdcpb.TsChangeDataCaptureStop:
			_, err = c.StopTsCDC(context.Background(), t)
			break
		case *cdcpb.TsChangeDataCaptureError:
		}
	}

	c.SetCDCTableOSNDelay(context.Background(), onsCleanTimeout)

	return err
}

// StartTsCDC implements the grpc CDCCoordinator server.
// It receives request from CDC consumer and init task.
func (c *Coordinator) StartTsCDC(
	request *cdcpb.TsChangeDataCaptureRequest, server cdcpb.CDCCoordinator_StartTsCDCServer,
) error {
	ctx := context.Background()
	var err error

	switch request.InstanceType {
	case sqlbase.CDCInstanceType_Stream:
		task, err := newStreamTask(ctx, request, server, int32(c.gossip.NodeID.Get()))
		if err != nil {
			return err
		}

		return c.runTask(ctx, task)
	case sqlbase.CDCInstanceType_Pipe:
		ctx, cancel := context.WithCancel(ctx)
		errCh := make(chan error)
		heartbeatInterval := TsPipeHeartbeatInterval.Get(&c.settings.SV)
		flushLimit := int(float64(request.PipeMetadata.BufferSize) * bufferFlushThreshold)
		var grpcMu syncutil.Mutex

		if err = c.stopper.RunAsyncTask(ctx, "pipe-processor-poller", func(ctx context.Context) {
			g := ctxgroup.WithContext(ctx)
			for i := range request.PipeMetadata.TableList {
				table := request.PipeMetadata.TableList[i]
				if c.HasTask(sqlbase.CDCInstanceType_Pipe, table.TableID, request.InstanceID) {
					continue
				}

				g.GoCtx(func(ctx context.Context) error {
					sink, err := CreateSink(
						context.Background(), request.PipeMetadata.Sink,
						int(TsPipeSinkMaxRetries.Get(&c.settings.SV)),
						int(request.PipeMetadata.BufferSize),
						false,
					)
					if err != nil {
						return err
					}

					task := NewPipeTask(
						ctx, request.PipeMetadata, server, int32(c.gossip.NodeID.Get()), sink,
						heartbeatInterval, flushLimit, table.TableID, request.InstanceID, table, &grpcMu,
					)

					return c.runTask(ctx, task)
				})
			}

			err = g.Wait()
			errCh <- err
			cancel()
		}); err != nil {
			log.Errorf(ctx, "pipe internal gRPC connections are disconnected with error: %s", err)
			errCh <- err
			cancel()
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err = <-errCh:
				return err
			}
		}
	case sqlbase.CDCInstanceType_Publication:
		// start cdc async task of publication.
		ctx, cancel := context.WithCancel(ctx)
		errCh := make(chan error)
		var grpcMu syncutil.Mutex
		// fetch ts.pipe.heartbeat.interval as the heartbeat interval between cdc tasks on different nodes.
		heartbeatInterval := TsPipeHeartbeatInterval.Get(&c.settings.SV)
		var params cdcpb.PubParameters
		if err = gojson.Unmarshal(request.PubMetadata.Parameters, &params); err != nil {
			errCh <- err
			cancel()
		}
		// flushLimit is the threshold of publication cdc buffer. If data in buffer is greater than 80% of
		// publication option buffer_size in megabyte, then flush it.
		flushLimit := int(float64(params.PubOptions.BufferSize*1<<20) * bufferFlushThreshold)
		if err = c.stopper.RunAsyncTask(ctx, "publication-processor-poller", func(ctx context.Context) {
			g := ctxgroup.WithContext(ctx)
			for i := range params.TableList {
				table := params.TableList[i]
				g.GoCtx(func(ctx context.Context) error {
					// construct publication task and run it.
					task := NewPublicationTask(
						ctx, request, server, int32(c.gossip.NodeID.Get()),
						&params, heartbeatInterval, flushLimit, &table, &grpcMu,
					)

					return c.runTask(ctx, task)
				})
			}

			err = g.Wait()
			errCh <- err
			cancel()
		}); err != nil {
			log.Errorf(ctx, "pipe internal gRPC connections are disconnected with error: %s", err)
			errCh <- err
			cancel()
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err = <-errCh:
				return err
			}
		}
	default:
		msg := fmt.Sprintf("unsupported CDC instance type: %s", request.InstanceType)
		log.Infof(ctx, msg)
		return errors.Errorf(msg)
	}
}

// StopTsCDC implements the grpc CDCCoordinator server.
// It sends the CLOSE request to CDC coordinator.
func (c *Coordinator) StopTsCDC(
	_ context.Context, request *cdcpb.TsChangeDataCaptureStop,
) (*cdcpb.Empty, error) {
	c.removeTaskWithID(request.TableID, request.InstanceID, request.InstanceType)
	return &cdcpb.Empty{}, nil
}

// StopCDCByLocal stops the specified CDC instance of local node.
func (c *Coordinator) StopCDCByLocal(
	tableID uint64, instanceID uint64, cdcType sqlbase.CDCInstanceType,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	task, ok := c.getCDCTask(cdcType, tableID, instanceID)
	if ok {
		err := task.Stop()
		if err != nil {
			return
		}
	}

}

// SendToPipeImmediately creates a new sink with parameters from pipe, and sends data immediately.
func (c *Coordinator) SendToPipeImmediately(
	ctx context.Context,
	dbName string,
	schemaName string,
	tableName string,
	ddlOperation string,
	stmt string,
	sinkURI string,
	sinkBufferSize int,
	osn time.Time,
) error {
	sinkBufferSizeInBytes := sinkBufferSize << 20
	maxRetries := int(TsPipeSinkMaxRetries.Get(&c.settings.SV))
	err := sendToPipeInner(
		ctx, dbName, schemaName, tableName, ddlOperation, stmt, sinkURI, sinkBufferSizeInBytes, maxRetries, osn,
	)

	return err
}

// sendToPipeInner creates a new sink, and sends data immediately.
func sendToPipeInner(
	ctx context.Context,
	dbName string,
	schemaName string,
	tableName string,
	ddlOperation string,
	stmt string,
	sinkURI string,
	sinkBufferSize int,
	maxRetries int,
	osn time.Time,
) error {
	sink, err := CreateSink(ctx, sinkURI, maxRetries, sinkBufferSize, false)
	if err != nil {
		return err
	}

	message := MessageFormat{
		Kind:      ddlOperation,
		Database:  dbName,
		Schema:    schemaName,
		Table:     tableName,
		Statement: stmt,
	}
	msg, err := gojson.Marshal(message)
	if err != nil {
		return err
	}

	if err = sink.Send(ctx, osn.String(), msg); err != nil {
		return err
	}

	if err = sink.Flush(ctx); err != nil {
		return err
	}

	if err = sink.Close(); err != nil {
		return err
	}

	return nil
}

// IsCDCEnabled returns if it has a running CDC instance based on the relation id.
func (c *Coordinator) IsCDCEnabled(tableID uint64) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, cdcTasks := range c.cdcTaskGroups {
		list, ok := cdcTasks[tableID]
		if ok && len(list) > 0 {
			return true
		}
	}

	return false
}

// WaitCDCEnabled returns if it all CDC tasks are active.
func (c *Coordinator) WaitCDCEnabled(tableID uint64, cdcDesc []sqlbase.CDCDescriptor) bool {
	if len(cdcDesc) == 0 {
		// stream only check task.
		return c.IsCDCEnabled(tableID)
	}

	const maxRetry = 15
	hasNotify := false
	enable := false

	// pipe and pub-sub need wait CDC,stream do not wait.
	for _, cdc := range cdcDesc {
		instanceID := cdc.ID
		for i := 0; i < maxRetry; i++ {
			c.lock.RLock()
			if cdc.CdcType == sqlbase.CDCInstanceType_Pipe {
				instanceID = c.cdcInstanceMap[cdc.ID]
			}
			_, ok := c.getCDCTask(cdc.CdcType, tableID, instanceID)
			c.lock.RUnlock()

			if ok {
				enable = ok
				break
			}

			if !c.checkInstanceEnable(&cdc) {
				break
			}

			if !hasNotify && cdc.CdcType == sqlbase.CDCInstanceType_Pipe {
				c.jobRegistry.TestingNudgeAdoptionQueue()
				hasNotify = true
			}

			time.Sleep(time.Second)
		}
	}

	return enable
}

// getCDCTask return task from cdcTaskGroups.
func (c *Coordinator) getCDCTask(
	instanceType sqlbase.CDCInstanceType, tableID uint64, taskID uint64,
) (Task, bool) {
	tables, ok := c.cdcTaskGroups[instanceType]
	if !ok {
		return nil, false
	}

	tasks, ok := tables[tableID]
	if !ok {
		return nil, false
	}

	task, ok := tasks[taskID]

	return task, ok
}

// checkInstanceEnable checks the CDC instance if enabled.
func (c *Coordinator) checkInstanceEnable(cdcDesc *sqlbase.CDCDescriptor) bool {
	switch cdcDesc.CdcType {
	case sqlbase.CDCInstanceType_Pipe:
		row, err := c.internalExecutor.QueryRow(
			context.Background(),
			"check-pipe-job-enable",
			nil,
			"SELECT j.ID,j.status FROM system.kwdb_pipes p,system.jobs j WHERE p.job_id = j.ID AND p.id=$1",
			cdcDesc.ID,
		)
		if err != nil {
			log.Warningf(context.Background(), "check-pipe-job-enable failed: %v", err)
			return false
		}
		if row == nil {
			// ("job is not exist")
			return false
		}

		status := string(tree.MustBeDString(row[1]))
		return status == string(jobs.StatusRunning)
	case sqlbase.CDCInstanceType_Publication:
		query := fmt.Sprintf(
			`SELECT application_name FROM [show queries] WHERE application_name='sub$$$%s'`,
			string(cdcDesc.Parameters))
		row, err := c.internalExecutor.QueryRow(
			context.Background(),
			"check-pub-subscribed",
			nil,
			query,
		)
		if err != nil {
			log.Warningf(context.Background(), "check-pub-sub-enable failed: %v", err)
			return false
		}
		if row == nil {
			return false
		}

		return true
	}

	return false
}

// SendRows pushes the captured data changes (aka CDC) to CDC Task.
func (c *Coordinator) SendRows(cdcData *execinfrapb.CDCData) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if cdcData == nil {
		return
	}

	for _, cdcTasks := range c.cdcTaskGroups {
		tasks, ok := cdcTasks[cdcData.TableID]
		if !ok {
			continue
		}

		for _, data := range cdcData.PushData {
			taskID := data.TaskID
			task, ok := tasks[taskID]
			if ok && data.Data != nil && task.getStatus() && task.isOperatorSupport(cdcpb.EventInsert) {

				err := task.Push(cdcData.MinTimestamp, cdcData.OSN, data.Data)
				if err != nil {
					log.Infof(task.getContext(), "failed to send CDC rows: %v", err)
				}
			}
		}
	}
}

// SendStatement used to send sql statement to sink.
// It uses the same sink as SendRows, and flush is executed before sending.
func (c *Coordinator) SendStatement(ons uint64, tableID uint64, operation string, stmt []byte) {
	if stmt == nil || len(stmt) == 0 {
		return
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	for typ, cdcTasks := range c.cdcTaskGroups {
		if typ == sqlbase.CDCInstanceType_Stream {
			continue
		}

		tasks, ok := cdcTasks[tableID]
		if !ok {
			continue
		}

		for _, task := range tasks {
			if !task.getStatus() || !task.isOperatorSupport(operation) {
				continue
			}

			err := task.SendStatement(ons, operation, stmt)
			if err != nil {
				log.Warningf(task.getContext(), "failed to send CDC statement: %v", err)
			}
		}
	}
}

// CaptureData is used to construct the data that needs to be captured, as well as to construct the Payload.
// It filters and formats the captured data changes (aka CDC).
// It also computes the low-water mark in current CDC batch (array of inputDatums) for breakpoint continuation.
// The CDC batch (inputDatums) maybe include multiple data timestamps (parsed from user's INSERT statement).
func (c *Coordinator) CaptureData(
	evalCtx *tree.EvalContext,
	tableID uint64,
	columns []*sqlbase.ColumnDescriptor,
	inputDatums []tree.Datums,
	colInputIndex map[int]int,
) (data []*sqlbase.CDCPushData, maxTimestamp int64) {
	// recover the cdc filter error, for example, if there is a division by 0 in the filtering conditions,
	// when the column value in the insert statement is 0.
	defer func() {
		if r := recover(); r != nil {
			// wrapper the cdc filter error to the caller
			if ok, e := errorutil.ShouldCatch(r); ok {
				log.Errorf(context.TODO(), "failed to catch the data change with error: %s", e)
				data = nil
			} else {
				panic(r)
			}
		}
	}()

	c.lock.RLock()
	defer c.lock.RUnlock()

	maxTimestamp = InvalidWatermark

	// fetch all CDC tasks for the current insert table.
	tasks, filters, needNormalTag, err := c.fetchCDCTasks(tableID, columns)
	if err != nil || len(tasks) == 0 {
		return nil, 0
	}

	var tagQueryStmt string
	if needNormalTag {
		tagQueryStmt = c.constructNormalTagStmt(tasks[0].getSourceTableName(), columns)
	}

	filteredRows := make(map[int][]interface{}, len(tasks))
	encRows := make(sqlbase.EncDatumRow, len(columns))
	colIDMap := make(map[uint32]int)

	var tagRowsCache = make(map[string]sqlbase.EncDatumRow)

	normalTagIndex := make(map[int]int)
	pTagIndex := make([]int, 0)
	colMap := make(map[uint32]sqlbase.ColumnDescriptor)
	for colIdx, col := range columns {
		if needNormalTag && col.IsTagCol() && !col.IsPrimaryTagCol() {
			normalTagIndex[int(col.ID)] = colIdx
		} else if col.IsPrimaryTagCol() {
			pTagIndex = append(pTagIndex, colIdx)
		}

		colIDMap[uint32(col.ID)] = colIdx
		colTmp := *col
		colMap[uint32(col.ID)] = colTmp
	}

	// encode and filter input rows
	for i := 0; i < len(inputDatums); i++ {
		encRows, maxTimestamp = c.encodeRow(encRows, columns, colInputIndex, maxTimestamp, inputDatums[i])

		// When needNormalTag is true: Omitted normal tags are automatically retrieved.
		// If the primary tag and normal tag in input do not match those from the first INSERT of this primary tag,
		// or the normal tags omitted in input, the system will read the normal tag value from the first write,
		// correct the current normal tag, and then proceed with pushing or filtering.
		// When tag needNormalTag is false: Omitted normal tags are treated as NULL values during INSERT.
		// If the user-input primary tag and normal tag do not match those from the first INSERT of this primary tag,
		// the system will use the currently written normal tag value for pushing or filtering.
		if needNormalTag {
			encRows, tagRowsCache = c.retrieveRowTags(evalCtx, encRows, tagRowsCache, pTagIndex, tagQueryStmt, normalTagIndex)
		}

		for idx, task := range tasks {
			pass, err := task.FilterRow(encRows, filters[idx])
			if err != nil {
				log.Errorf(task.getContext(), "filter error: %s", err)
				continue
			}

			// Encode data according to the type required by the current task.
			if pass {
				row := task.ConstructCDCRow(columns, colMap, colInputIndex, normalTagIndex, inputDatums[i], encRows, colIDMap)
				filteredRows[idx] = append(filteredRows[idx], row)
			}
		}
	}

	// converts the encoded rows to CDCPushData.
	for idx, task := range tasks {
		if len(filteredRows[idx]) == 0 {
			continue
		}

		pushData := task.FormatCDCRows(filteredRows[idx])
		if pushData != nil {
			data = append(data, pushData)
		}
	}

	return data, maxTimestamp
}

// encodeRow encode row from input rows.
func (c *Coordinator) encodeRow(
	encRows sqlbase.EncDatumRow,
	columns []*sqlbase.ColumnDescriptor,
	colInputIndex map[int]int,
	maxTimestamp int64,
	inputDatums tree.Datums,
) (sqlbase.EncDatumRow, int64) {
	for colIdx, col := range columns {
		inputIdx, ok := colInputIndex[int(col.ID)]
		if !ok || inputIdx == -1 {
			encRows[colIdx] = sqlbase.EncDatum{Datum: tree.DNull}
			continue
		}

		datum := inputDatums[inputIdx]
		if datum == nil || datum == tree.DNull {
			inputDatums[inputIdx] = tree.DNull
			encRows[colIdx] = sqlbase.EncDatum{Datum: tree.DNull}
			continue
		}

		// In time-series tables, datum of timestamp type is stored in INT64 format.
		switch col.Type.Oid() {
		case oid.T_timestamp, oid.T_timestamptz:
			var inputTime time.Time
			switch v := datum.(type) {
			case *tree.DInt:
				val := int64(*v)
				inputTime = timeutil.FromTimestamp(val, col.Type.Precision())
				encRows[colIdx] = sqlbase.EncDatum{Datum: &tree.DTimestampTZ{Time: inputTime}}
			case *tree.DTimestamp:
				inputTime = (*datum.(*tree.DTimestamp)).Time.UTC()
				encRows[colIdx] = sqlbase.EncDatum{Datum: datum}
			case *tree.DTimestampTZ:
				inputTime = (*datum.(*tree.DTimestampTZ)).Time.UTC()
				encRows[colIdx] = sqlbase.EncDatum{Datum: datum}
			}

			if colIdx == 0 {
				ms := timeutil.ToUnixMilli(inputTime)
				if maxTimestamp < ms || maxTimestamp == InvalidWatermark {
					maxTimestamp = ms
				}
			}

		default:
			encRows[colIdx] = sqlbase.EncDatum{Datum: datum}
		}
	}

	return encRows, maxTimestamp
}

// retrieveRowTags retrieve normal tags of input row.
func (c *Coordinator) retrieveRowTags(
	evalCtx *tree.EvalContext,
	encRows sqlbase.EncDatumRow,
	tagRowsCache map[string]sqlbase.EncDatumRow,
	pTagIndex []int,
	tagQueryStmt string,
	normalTagIndex map[int]int,
) (sqlbase.EncDatumRow, map[string]sqlbase.EncDatumRow) {
	var pTagsKey strings.Builder
	var pTags []interface{}
	for _, colIdx := range pTagIndex {
		value := sqlbase.DatumToString(encRows[colIdx].Datum)
		pTagsKey.WriteString(fmt.Sprintf("%d:%s", len(value), value))
		pTags = append(pTags, getDataFromDatum(encRows[colIdx].Datum))
	}
	tagRows, ok := tagRowsCache[pTagsKey.String()]
	if !ok {
		tagRows = getNormalTags(evalCtx, tagQueryStmt, pTags, encRows, normalTagIndex)
		tagRowsCache[pTagsKey.String()] = tagRows
	}
	for _, index := range normalTagIndex {
		encRows[index] = sqlbase.EncDatum{Datum: tagRows[index].Datum}
	}

	return encRows, tagRowsCache
}

// LiveNodeIDList implements the CDCCoordinator interface
func (c *Coordinator) LiveNodeIDList(ctx context.Context) ([]roachpb.NodeID, error) {
	nodeStatus, err := c.statusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return []roachpb.NodeID{}, err
	}

	var NodeIDList []roachpb.NodeID
	for _, n := range nodeStatus.Nodes {
		if _, err := c.gossip.GetInfo(gossip.MakeGossipClientsKey(n.Desc.NodeID)); err == nil {
			NodeIDList = append(NodeIDList, n.Desc.NodeID)
		}
	}

	return NodeIDList, nil
}

// DistInternalExecutor returns InternalExecutor.
func (c *Coordinator) DistInternalExecutor() sqlutil.InternalExecutor {
	return c.partitionInternalExecutor
}

// SetDistInternalExecutor set InternalExecutor.
func (c *Coordinator) SetDistInternalExecutor(executor sqlutil.InternalExecutor) {
	c.partitionInternalExecutor = executor
}

// runTask run a task.
func (c *Coordinator) runTask(ctx context.Context, task Task) error {
	// add the task to cdcTaskGroups
	c.addTask(task)
	log.Infof(ctx, "%s is connected", task.string())
	err := task.Run(c.stopper)
	if err != nil && !strings.Contains(err.Error(), context.Canceled.Error()) {
		log.Infof(ctx, "%s failed with error %v", task.string(), err)
		errStop := task.SendError(err)
		if errStop != nil && !strings.Contains(errStop.Error(), context.Canceled.Error()) {
			log.Infof(ctx, "failed to stop %s with error %v", task.string(), errStop)
		}
	}

	c.removeTask(task)
	log.Infof(ctx, "%s is disconnected", task.string())

	return err
}

// addTask add task to CDC cache.
func (c *Coordinator) addTask(task Task) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cdcTaskGroups[task.getInstanceType()]; !ok {
		c.cdcTaskGroups[task.getInstanceType()] = make(map[uint64]map[uint64]Task)
	}

	if _, ok := c.cdcTaskGroups[task.getInstanceType()][task.getTableID()]; !ok {
		c.cdcTaskGroups[task.getInstanceType()][task.getTableID()] = make(map[uint64]Task)
	}

	c.cdcTaskGroups[task.getInstanceType()][task.getTableID()][task.getInstanceID()] = task
	if task.getInstanceType() == sqlbase.CDCInstanceType_Pipe {
		c.cdcInstanceMap[task.getCDCID()] = task.getInstanceID()
	}
}

// EnableTask enable the CDC task.
func (c *Coordinator) enableTask(
	tableID uint64, instanceID uint64, instanceType sqlbase.CDCInstanceType,
) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	task, ok := c.cdcTaskGroups[instanceType][tableID][instanceID]
	if !ok {
		return errors.Errorf("CDC is not setup")
	}

	task.setStatus(true)
	log.Eventf(context.TODO(), "CDC instance %s, %d(table: %d) is start \n", task.getInstanceName(), instanceID, tableID)

	return nil
}

// removeTaskWithID remove task with id from CDC cache.
func (c *Coordinator) removeTaskWithID(
	tableID uint64, instanceID uint64, instanceType sqlbase.CDCInstanceType,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.cdcTaskGroups[instanceType][tableID], instanceID)
}

// removeTask remove task from CDC cache.
func (c *Coordinator) removeTask(task Task) {
	c.removeTaskWithID(task.getTableID(), task.getInstanceID(), task.getInstanceType())
}

// fetchCDCTasks get all tasks of the table.
func (c *Coordinator) fetchCDCTasks(
	tableID uint64, colDescriptors []*sqlbase.ColumnDescriptor,
) ([]Task, [][]*execinfra.ExprHelper, bool, error) {
	var results []Task
	var needNormalTag bool
	var filterList [][]*execinfra.ExprHelper
	for _, cdcTasks := range c.cdcTaskGroups {
		tasks, ok := cdcTasks[tableID]
		if ok && len(tasks) > 0 {
			for _, task := range tasks {
				needNormalTag = needNormalTag || task.needReplenishNormalTag()
				results = append(results, task)

				filter, err := task.InitFilter(colDescriptors)
				if err != nil {
					log.Errorf(task.getContext(), "init filter error: %s", err)
					return nil, nil, false, err
				}
				filterList = append(filterList, filter)
			}
		}
	}

	return results, filterList, needNormalTag, nil
}

// constructNormalTagStmt construct stmt for get normal tag.
func (c *Coordinator) constructNormalTagStmt(
	tableName string, colDescriptors []*sqlbase.ColumnDescriptor,
) string {
	var pTags []string
	var cols []string
	for i := range colDescriptors {
		col := colDescriptors[i]
		cols = append(cols, col.Name)
		if col.IsPrimaryTagCol() {
			pTags = append(pTags, fmt.Sprintf("%s=$%d", col.Name, len(pTags)+1))
			continue
		}
	}

	// need to add normal tag datum
	colList := strings.Join(cols, ",")
	where := strings.Join(pTags, " AND ")
	tagQueryStmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", colList, tableName, where)

	return tagQueryStmt
}

// HasTask checks whether the specified instance has task in CDC.
// instanceType, the type of instance, such as Stream, Pipe, Publication, and Subscription.
// tableID, the table id in the instance.
// instanceID, the id of the instance.
func (c *Coordinator) HasTask(
	instanceType sqlbase.CDCInstanceType, tableID uint64, instanceID uint64,
) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	tableGroup, ok := c.cdcTaskGroups[instanceType]
	if !ok {
		return false
	}

	pubGroup, ok := tableGroup[tableID]
	if !ok {
		return false
	}

	_, ok = pubGroup[instanceID]
	return ok
}

// CheckPubTasksCountAndSubscribed checks whether the publication has been subscribed and will exceed the limitation.
func (c *Coordinator) CheckPubTasksCountAndSubscribed(
	ctx context.Context,
	instanceType sqlbase.CDCInstanceType,
	pubMeta *cdcpb.PubMetadata,
	params cdcpb.PubParameters,
) error {
	newTasksCount := len(params.TableList)
	// fetch publication tasks limitation from cluster setting ts.cdc.max_active_number.
	pubLimitation := cdcpb.TsCDCMaxActiveNumber.Get(&c.settings.SV)
	// If the count of new publication tasks exceeds the limitation, raises an error.
	if int64(newTasksCount) > pubLimitation {
		errMsg := fmt.Sprintf("the number of publication tasks to be run exceeds the limitation (%d)", pubLimitation)
		log.Warningf(ctx, errMsg)
		return pgerror.Newf(
			pgcode.ProgramLimitExceeded, errMsg)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	tableTasksGroup, ok := c.cdcTaskGroups[instanceType]
	if !ok {
		return nil
	}
	// If the publication has been subscribed and the subscription is still on process, it cannot be subscribed again.
	for _, tableInfo := range params.TableList {
		tableID := tableInfo.ID
		if tasksOfTable, found := tableTasksGroup[tableID]; found {
			if _, found = tasksOfTable[pubMeta.ID]; found {
				errMsg := fmt.Sprintf("the publication %s has been subscribed, and can not be subscribed again", pubMeta.Name)
				log.Warningf(ctx, errMsg)
				return pgerror.Newf(
					pgcode.ObjectInUse, errMsg)
			}
		}
	}
	// If the total count of the old and new publication tasks exceeds the limitation, raises an error.
	tasksRunningCount := 0
	for _, tasksOfInstance := range tableTasksGroup {
		tasksRunningCount += len(tasksOfInstance)
	}
	if int64(tasksRunningCount+newTasksCount) > pubLimitation {
		errMsg := fmt.Sprintf("the number of publication tasks running and to be run exceeds the limitation (%d)", pubLimitation)
		log.Warningf(ctx, errMsg)
		return pgerror.Newf(
			pgcode.ProgramLimitExceeded, errMsg)
	}

	return nil
}

// SetTsEngine set tsEngine to Coordinator.
func (c *Coordinator) SetTsEngine(tse *tse.TsEngine) {
	c.tsEngine = tse
}

// SetCDCTableOSNDelay sleep for onsCleanTimeout to wait for the watermark in DDL to be updated.
func (c *Coordinator) SetCDCTableOSNDelay(ctx context.Context, delay time.Duration) {
	time.AfterFunc(delay, func() {
		if err := c.SetCDCTableOSN(ctx); err != nil {
			log.Errorf(ctx, err.Error())
		}
	})
}

// SetCDCTableOSN sets OSN of tables with CDC to tsEngine.
func (c *Coordinator) SetCDCTableOSN(ctx context.Context) error {
	if c.tsEngine == nil {
		return nil
	}

	select {
	case c.setOSNRunning <- struct{}{}:
	default:
		return nil
	}

	defer func() {
		<-c.setOSNRunning
	}()

	rows, err := c.internalExecutor.Query(
		ctx,
		"get-cdc-table-watermark",
		nil,
		`SELECT table_id, low_watermark from (
						SELECT table_id, low_watermark FROM system.kwdb_cdc_watermark c,system.kwdb_pipes p 
						WHERE p.id=c.task_id AND c.task_type = $1 AND low_watermark > $3
						UNION 
						SELECT table_id, low_watermark FROM system.kwdb_cdc_watermark c,system.kwdb_publications p 
						WHERE p.id=c.task_id AND c.task_type = $2 AND low_watermark > $3)
          `,
		sqlbase.CDCInstanceType_Pipe,
		sqlbase.CDCInstanceType_Publication,
		cdcpb.InvalidWatermark,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	osnMap := make(map[uint64]uint64)
	for _, row := range rows {
		if row[0] == tree.DNull || row[1] == tree.DNull {
			continue
		}

		tableID := uint64(tree.MustBeDInt(row[0]))
		osn := uint64(tree.MustBeDInt(row[1]))
		if old, exist := osnMap[(tableID)]; exist {
			if old <= osn {
				continue
			}
		}
		osnMap[tableID] = osn
	}

	if err = c.tsEngine.SetPublishedMaxOSN(ctx, osnMap); err != nil {
		return err
	}

	return nil
}
