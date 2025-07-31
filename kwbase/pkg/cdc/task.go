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

package cdc

import (
	"context"
	"fmt"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

const (
	// InvalidWatermark indicates the invalid watermark.
	InvalidWatermark = math.MinInt64
)

// Task is CDC task.
type Task interface {
	// Run starts the CDC task with a heartbeat timer
	Run(stopper *stop.Stopper) error

	// Stop sends a STOP message to CDC consumer that will redirect it to all nodes to
	// remove the related CDC task from CDC coordinator.
	Stop() error

	// SendHeartbeat sends message to CDC consumer periodically,
	// subclass can overwrite it when need to run a periodical task
	SendHeartbeat() error

	// InitFilter initializes the 'filter helper' using filter expressions.
	InitFilter(columns []*sqlbase.ColumnDescriptor) ([]*execinfra.ExprHelper, error)

	// FilterRow filters the rows captured by CDC.
	FilterRow(row sqlbase.EncDatumRow, filter []*execinfra.ExprHelper) (bool, error)

	// ConstructCDCRow encodes the filtered rows, each subclass should implement it
	// based on the requirement of CDC consumer
	ConstructCDCRow(
		columns []*sqlbase.ColumnDescriptor, colInputIndex map[int]int, normalTagIndex map[int]int,
		inputDatum tree.Datums, encRows sqlbase.EncDatumRow, colIDMap map[uint32]int,
	) []interface{}

	// FormatCDCRows converts the encoded rows to CDCPushData, each subclass should implement it
	// based on the requirement of CDC consumer
	FormatCDCRows(data []interface{}) *sqlbase.CDCPushData

	// Push pushes a message to buffer and checks if it reaches the flush threshold.
	Push(ts int64, data [][]byte) error

	needReplenishNormalTag() bool
	getContext() context.Context
	getInstanceID() uint64
	getInstanceType() cdcpb.TSCDCInstanceType

	getTableID() uint64
	getSourceTableName() string
	getOutputColumnIDs() []uint32
	getOutColumnTypes() []types.T
}

// BaseTask filters and sends the CDC message to consumer (for example, kafka).
// It also sends the heartbeat message with low-water mark of pushed data to CDC consumer.
type BaseTask struct {
	// Ctx is current context
	Ctx context.Context

	// serverMu is mutex used to grpc server.
	serverMu syncutil.Mutex
	// server is grpc server of CDC.
	server cdcpb.CDCCoordinator_StartTsCDCServer
	errCh  chan error

	// nodeID is the ID of the node where the current task is running.
	nodeID int32
	// localWaterMark is watermark of the node.
	localWaterMark int64
	// heartbeatInterval is heartbeat interval time.
	heartbeatInterval time.Duration

	// tableID is table id of CDC.
	tableID uint64
	// sourceTableName is table name of CDC.
	sourceTableName string
	// instanceID is the id of stream or pipe.
	instanceID uint64
	// instanceType is the type of stream or pipe.
	instanceType cdcpb.TSCDCInstanceType
	// columnTypes is all types of table.
	columnTypes []types.T
	// outColumnTypes is output types of table.
	outColumnTypes []types.T
	// outputColumnIDs is output IDs of table.
	outputColumnIDs []uint32
	// needNormalTag If is true, it means that normal tags need to replenish.
	needNormalTag bool
}

var _ Task = &BaseTask{}

// Run starts the CDC task with a heartbeat timer
func (t *BaseTask) Run(stopper *stop.Stopper) error {
	panic("CDC task method 'Run' is not implemented")
}

// Stop sends a STOP message to CDC consumer that will redirect it to all nodes to
// remove the related CDC task from CDC coordinator.
func (t *BaseTask) Stop() error {
	t.serverMu.Lock()
	defer t.serverMu.Unlock()

	evt := &cdcpb.TsChangeDataCaptureEvent{
		Stop: &cdcpb.TsChangeDataCaptureStop{TableID: t.tableID, InstanceID: t.instanceID, InstanceType: t.instanceType},
	}
	return t.server.Send(evt)
}

// SendHeartbeat sends message to CDC consumer periodically,
// subclass can overwrite it when need to run a periodical task
func (t *BaseTask) SendHeartbeat() error {
	t.serverMu.Lock()
	defer t.serverMu.Unlock()

	// send the heartbeat message to CDC consumer with the local low-water mark.
	evt := &cdcpb.TsChangeDataCaptureEvent{
		Heartbeat: &cdcpb.TsChangeDataCaptureHeartbeat{
			NodeID:         t.nodeID,
			LocalWaterMark: t.localWaterMark,
			TableID:        t.tableID,
		},
	}

	return t.server.Send(evt)
}

// InitFilter implements the Task server.
func (t *BaseTask) InitFilter(
	columns []*sqlbase.ColumnDescriptor,
) ([]*execinfra.ExprHelper, error) {
	return nil, nil
}

// FilterRow implements the Task server.
func (t *BaseTask) FilterRow(
	row sqlbase.EncDatumRow, filter []*execinfra.ExprHelper,
) (bool, error) {
	return true, nil
}

// ConstructCDCRow implements the Task server.
func (t *BaseTask) ConstructCDCRow(
	columns []*sqlbase.ColumnDescriptor,
	colInputIndex map[int]int,
	normalTagIndex map[int]int,
	inputDatum tree.Datums,
	encRows sqlbase.EncDatumRow,
	colIDMap map[uint32]int,
) []interface{} {
	panic("CDC task method 'ConstructCDCRow' is not implemented")
}

// FormatCDCRows implements the Task server.
func (t *BaseTask) FormatCDCRows(data []interface{}) *sqlbase.CDCPushData {
	panic("CDC task method 'FormatCDCRows' is not implemented")
}

// Push implements the Task server.
func (t *BaseTask) Push(ts int64, data [][]byte) error {
	panic("CDC task method 'Push' is not implemented")
}

func (t *BaseTask) getContext() context.Context {
	return t.Ctx
}

func (t *BaseTask) needReplenishNormalTag() bool {
	return t.needNormalTag
}

func (t *BaseTask) getInstanceID() uint64 {
	return t.instanceID
}
func (t *BaseTask) getInstanceType() cdcpb.TSCDCInstanceType {
	return t.instanceType
}
func (t *BaseTask) getTableID() uint64 {
	return t.tableID
}

func (t *BaseTask) getSourceTableName() string {
	return t.sourceTableName
}

func (t *BaseTask) getOutputColumnIDs() []uint32 {
	return t.outputColumnIDs
}

func (t *BaseTask) getOutColumnTypes() []types.T {
	return t.outColumnTypes
}

func getNormalTags(
	evalCtx *tree.EvalContext,
	stmt string,
	pTags []interface{},
	encRows sqlbase.EncDatumRow,
	normalTagIndex map[int]int,
) sqlbase.EncDatumRow {
	tagRows := make(sqlbase.EncDatumRow, len(encRows))
	copy(tagRows, encRows)

	row, err := evalCtx.InternalExecutor.QueryRow(
		evalCtx.Ctx(),
		"fetch-normal-tag",
		evalCtx.Txn,
		stmt,
		pTags...,
	)

	if err != nil {
		log.Errorf(evalCtx.Ctx(), "filter error: %s", err)
		return tagRows
	}

	if row != nil {
		for _, index := range normalTagIndex {
			tagRows[index] = sqlbase.EncDatum{Datum: row[index]}
		}
	}

	return tagRows
}

func getDataFromDatum(datum tree.Datum) interface{} {
	if datum == tree.DNull {
		return nil
	}
	switch v := datum.(type) {
	case *tree.DInt:
		return int64(*v)
	case *tree.DTimestamp:
		return v.UnixMilli()
	case *tree.DTimestampTZ:
		return v.UnixMilli()
	case *tree.DFloat:
		return float64(*v)
	case *tree.DBool:
		return bool(*v)
	case *tree.DString:
		return string(*v)
	case *tree.DBytes:
		if *v == "" {
			return "\\x"
		}
		return fmt.Sprintf("\\x%02x", *v)
	default:
		return nil
	}
}
