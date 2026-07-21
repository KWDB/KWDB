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
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

const (
	// InvalidWatermark indicates the invalid watermark.
	InvalidWatermark = 0
)

// Task is CDC task.
type Task interface {
	// Run starts the CDC task with a heartbeat timer
	Run(stopper *stop.Stopper) error

	// Stop sends a STOP message to CDC consumer that will redirect it to all nodes to
	// remove the related CDC task from CDC coordinator.
	Stop() error
	// SendError sends a Error message to CDC consumer
	SendError(err error) error

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
		columns []*sqlbase.ColumnDescriptor, colMap map[uint32]sqlbase.ColumnDescriptor,
		colInputIndex map[int]int, normalTagIndex map[int]int,
		inputDatum tree.Datums, encRows sqlbase.EncDatumRow, colIDMap map[uint32]int,
	) []interface{}

	// FormatCDCRows converts the encoded rows to CDCPushData, each subclass should implement it
	// based on the requirement of CDC consumer
	FormatCDCRows(data []interface{}) *sqlbase.CDCPushData
	// SendStatement send the delete, update statement.
	SendStatement(ons uint64, operation string, stmt []byte) error

	// Push pushes a message to buffer and checks if it reaches the flush threshold.
	Push(ts int64, osn uint64, data [][]byte) error
	// getSink returns the sink of this task
	getSink() Sink
	needReplenishNormalTag() bool
	getContext() context.Context
	getInstanceID() uint64
	getInstanceName() string
	getInstanceType() sqlbase.CDCInstanceType

	getTableID() uint64
	getSourceTableName() string
	getOutputColumnIDs() []uint32
	getOutColumnTypes() []types.T

	setStatus(status bool)
	getStatus() bool
	isOperatorSupport(op string) bool
	checkUpdateTagsInOutput(stmt string) bool

	getCDCID() uint64
	string() string
}

// BaseTask filters and sends the CDC message to consumer (for example, kafka).
// It also sends the heartbeat message with low-water mark of pushed data to CDC consumer.
type BaseTask struct {
	// Ctx is current context
	Ctx context.Context

	// serverMu is mutex used to grpc server.
	serverMu *syncutil.Mutex
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
	// instanceID is the id of stream or publication or pipe job.
	instanceID uint64
	// instanceName is the name of stream or pipe.
	instanceName string
	// instanceType is the type of stream or pipe.
	instanceType sqlbase.CDCInstanceType
	// columnTypes is all types of table.
	columnTypes []types.T
	// outColumnTypes is output types of table.
	outColumnTypes []types.T
	// outputColumnIDs is output IDs of table.
	outputColumnIDs []uint32
	// outputColumnNames is output name of table.
	outputColumnNames []string
	// needNormalTag If is true, it means that normal tags need to replenish.
	needNormalTag bool
	// status is cdc task status, contains enable,pending,disable.
	status bool
	// operatorMap is the map that contains operator supported by cdc.
	operatorMap map[string]struct{}
	// cdcID is pipeID
	cdcID uint64
}

var _ Task = &BaseTask{}

// Run starts the CDC task with a heartbeat timer
func (t *BaseTask) Run(_ *stop.Stopper) error {
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

// SendError sends a Error message to CDC consumer
func (t *BaseTask) SendError(err error) error {
	t.serverMu.Lock()
	defer t.serverMu.Unlock()

	evt := &cdcpb.TsChangeDataCaptureEvent{
		Error: &cdcpb.TsChangeDataCaptureError{Error: *roachpb.NewError(err)},
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
func (t *BaseTask) InitFilter(_ []*sqlbase.ColumnDescriptor) ([]*execinfra.ExprHelper, error) {
	return nil, nil
}

// FilterRow implements the Task server.
func (t *BaseTask) FilterRow(_ sqlbase.EncDatumRow, _ []*execinfra.ExprHelper) (bool, error) {
	return true, nil
}

// ConstructCDCRow implements the Task server.
func (t *BaseTask) ConstructCDCRow(
	_ []*sqlbase.ColumnDescriptor,
	_ map[uint32]sqlbase.ColumnDescriptor,
	_ map[int]int,
	_ map[int]int,
	_ tree.Datums,
	_ sqlbase.EncDatumRow,
	_ map[uint32]int,
) []interface{} {
	panic("CDC task method 'ConstructCDCRow' is not implemented")
}

// FormatCDCRows implements the Task server.
func (t *BaseTask) FormatCDCRows(_ []interface{}) *sqlbase.CDCPushData {
	panic("CDC task method 'FormatCDCRows' is not implemented")
}

// Push implements the Task server.
func (t *BaseTask) Push(_ int64, _ uint64, _ [][]byte) error {
	panic("CDC task method 'Push' is not implemented")
}

// getSink implements the Task server.
func (t *BaseTask) getSink() Sink {
	return nil
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

func (t *BaseTask) getInstanceName() string {
	return t.instanceName
}

func (t *BaseTask) getInstanceType() sqlbase.CDCInstanceType {
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

// SendStatement used to send sql statement to sink.
func (t *BaseTask) SendStatement(_ uint64, _ string, _ []byte) error {
	panic("CDC task method 'SendStatement' is not implemented")
}

// setStatus updates the status.
func (t *BaseTask) setStatus(status bool) {
	t.status = status
}

// getStatus returns status of task.
func (t *BaseTask) getStatus() bool {
	return t.status
}

// isOperatorSupport returns if the operator is supported.
func (t *BaseTask) isOperatorSupport(op string) bool {
	_, ok := t.operatorMap[op]

	return ok
}

// checkUpdateTagsInOutput validates if a column being updated in an SQL UPDATE statement
// are present in the allowed output list.
func (t *BaseTask) checkUpdateTagsInOutput(stmt string) bool {
	// Find SET clause
	upper := strings.ToUpper(stmt)
	setIdx := strings.Index(upper, " SET ")
	if setIdx == -1 {
		return false
	}

	// Extract content between SET and WHERE
	start := setIdx + 5
	end := len(stmt)
	if whereIdx := strings.Index(upper[start:], " WHERE "); whereIdx != -1 {
		end = start + whereIdx
	}
	setClause := strings.TrimSpace(stmt[start:end])

	// Build allowed columns set
	allowed := make(map[string]bool)
	for _, col := range t.outputColumnNames {
		allowed[strings.ToLower(col)] = true
	}

	// Split and check each assignment expression
	for _, expr := range strings.Split(setClause, ",") {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}

		// Get column name from left side of equals sign
		parts := strings.SplitN(expr, "=", 2)
		if len(parts) != 2 {
			continue
		}

		field := strings.TrimSpace(parts[0])

		// Remove table alias prefix
		if dot := strings.LastIndex(field, "."); dot != -1 {
			field = field[dot+1:]
		}

		// Remove quotation marks
		field = strings.Trim(field, "`\"'")

		if allowed[strings.ToLower(field)] {
			return true
		}
	}

	return false
}

// getCDCID returns cdc id.
func (t *BaseTask) getCDCID() uint64 {
	return t.cdcID
}

// string format task
func (t *BaseTask) string() string {
	return fmt.Sprintf(
		"CDC instance(name: %s,id: %d,type: %s)",
		t.getInstanceName(),
		t.getInstanceID(),
		t.getInstanceType(),
	)
}

// getNormalTags returns normal tags.
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

func initFilter(bytes []byte, types []types.T) (*execinfra.ExprHelper, error) {
	filterExp := &execinfrapb.Expression{}
	if err := protoutil.Unmarshal(bytes, filterExp); err != nil {
		return nil, err
	}

	expr := &execinfra.ExprHelper{}
	evalCtx := &tree.EvalContext{
		SessionData: &sessiondata.SessionData{
			DataConversion: sessiondata.DataConversionConfig{Location: time.UTC},
		},
	}

	if err := expr.Init(*filterExp, types, evalCtx); err != nil {
		return nil, err
	}

	return expr, nil
}

type baseBuffer struct {
	// maxTs is the latest timestamp of data in buffer.
	maxTs int64
	// minTs is the oldest timestamp of data in buffer.
	minTs int64
	// messageCount is the count of data batches.
	messageCount int
	// flushLimit is the threshold to flush. If bufferLength is larger than or equal to flushLimit, then flush buffer
	// immediately. In publication task, it is the buffer_size * 1<<20 * cdc.bufferFlushThreshold in bytes.
	flushLimit int
	// bufferLength is the length of buffer in bytes.
	bufferLength int
}

// count return the count of messages in buffer.
func (t *baseBuffer) count() int {
	return t.messageCount
}

// maxTimestamp is the max timestamp of messages in buffer.
func (t *baseBuffer) maxTimestamp() int64 {
	return t.maxTs
}

// minTimestamp is the min timestamp of messages in buffer.
func (t *baseBuffer) minTimestamp() int64 {
	return t.minTs
}
