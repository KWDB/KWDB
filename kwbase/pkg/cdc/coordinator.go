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
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// Coordinator implements the CDCCoordinator interface
type Coordinator struct {
	settings                  *cluster.Settings
	stopper                   *stop.Stopper
	gossip                    *gossip.Gossip
	statusServer              serverpb.StatusServer
	internalExecutor          *sql.InternalExecutor
	partitionInternalExecutor sqlutil.InternalExecutor

	lock          syncutil.RWMutex
	cdcTaskGroups map[cdcpb.TSCDCInstanceType]map[uint64]map[uint64]Task // cdcInstanceType->tableID->instanceID->Task
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
) *Coordinator {
	coordinator := &Coordinator{
		settings:         settings,
		stopper:          stopper,
		gossip:           gossip,
		internalExecutor: internalExecutor,
		statusServer:     statusServer,
	}
	coordinator.cdcTaskGroups = make(map[cdcpb.TSCDCInstanceType]map[uint64]map[uint64]Task)
	cdcpb.RegisterCDCCoordinatorServer(grpcServer, coordinator)
	return coordinator
}

// StartTsCDC implements the grpc CDCCoordinator server.
// It receives request from CDC consumer and init task.
func (c *Coordinator) StartTsCDC(
	request *cdcpb.TsChangeDataCaptureRequest, server cdcpb.CDCCoordinator_StartTsCDCServer,
) error {
	ctx := context.Background()

	switch request.InstanceType {
	case cdcpb.TSCDCInstanceType_Stream:
		task, err := newStreamTask(ctx, request, server, int32(c.gossip.NodeID.Get()))
		if err != nil {
			return err
		}
		c.addTask(task)
		log.Infof(ctx, "CDC instance %d(type: %s) is connected", task.getInstanceID(), task.getInstanceType())
		err = task.Run(c.stopper)

		if err != nil {
			if sqlutil.ShouldLogError(err) {
				log.Infof(ctx, "CDC instance %d(type: %s) failed with error %v", task.getInstanceID(), task.getInstanceType(), err)
			}
		}
		errStop := task.Stop()
		if errStop != nil {
			if sqlutil.ShouldLogError(errStop) {
				log.Infof(ctx, "failed to stop CDC instance %d(type: %s) with error %v", task.getInstanceID(), task.getInstanceType(), errStop)
			}
		}
		c.removeTask(task)
		log.Infof(ctx, "CDC instance %d(type: %s) is disconnected", task.getInstanceID(), task.getInstanceType())
		return err
	default:
		msg := fmt.Sprintf("unsuppoared CDC instance type: %s", request.InstanceType)
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
	tableID uint64, instanceID uint64, cdcType cdcpb.TSCDCInstanceType,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	cdcTasks, ok := c.cdcTaskGroups[cdcType][tableID]
	if ok {
		task, ok := cdcTasks[instanceID]
		if ok {
			err := task.Stop()
			if err != nil {
				return
			}
		}
	}
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

// SendRows pushes the captured data changes (aka CDC) to CDC Task.
func (c *Coordinator) SendRows(cdcData *execinfrapb.CDCData) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if cdcData == nil {
		return
	}

	for _, cdcTasks := range c.cdcTaskGroups {
		tasks, ok := cdcTasks[cdcData.TableID]
		if !ok || cdcData.PushData == nil {
			continue
		}

		for _, data := range cdcData.PushData {
			taskID := data.TaskID
			task, ok := tasks[taskID]
			if ok && data.Data != nil {
				err := task.Push(cdcData.MinTimestamp, data.Data)
				if err != nil {
					log.Infof(task.getContext(), "failed to send CDC rows: %v", err)
				}
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
	for colIdx, col := range columns {
		if needNormalTag && col.IsTagCol() && !col.IsPrimaryTagCol() {
			normalTagIndex[int(col.ID)] = colIdx
		} else if col.IsPrimaryTagCol() {
			pTagIndex = append(pTagIndex, colIdx)
		}

		colIDMap[uint32(col.ID)] = colIdx
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
				row := task.ConstructCDCRow(columns, colInputIndex, normalTagIndex, inputDatums[i], encRows, colIDMap)
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
	for id, n := range nodeStatus.LivenessByNodeID {
		switch n {
		case storagepb.NodeLivenessStatus_LIVE:
			NodeIDList = append(NodeIDList, id)
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
}

// removeTaskWithID remove task with id from CDC cache.
func (c *Coordinator) removeTaskWithID(
	tableID uint64, instanceID uint64, instanceType cdcpb.TSCDCInstanceType,
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
	for i := range colDescriptors {
		col := colDescriptors[i]
		if col.IsPrimaryTagCol() {
			pTags = append(pTags, fmt.Sprintf("%s=$%d", col.Name, len(pTags)+1))
			continue
		}
	}

	// need to add normal tag datum
	where := strings.Join(pTags, " AND ")
	tagQueryStmt := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", tableName, where)

	return tagQueryStmt
}

// HasTask checks whether the specified instance has task in CDC.
// instanceType, the type of instance, such as Stream, Pipe, Publication, and Subscription.
// tableID, the table id in the instance.
// instanceID, the id of the instance.
func (c *Coordinator) HasTask(
	instanceType cdcpb.TSCDCInstanceType, tableID uint64, instanceID uint64,
) bool {
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
