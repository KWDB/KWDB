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

package cdc

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// bufferFlushThreshold indicates the flush threshold of message buffer.
	bufferFlushThreshold = 0.8

	// outPutFormat is output massage format.
	outPutFormat = `{"kind": "%s","database": "%s","schema": "%s","table": "%s","columnnames": %s,"columntypes": %s,"columnvalues": [`

	kafkaMgsKindInsert   = "insert"
	kafkaMgsKindSnapshot = "snapshot"
)

var errMessageBytesExceedLimit = errors.Errorf("the bytes of insert data exceeds the limit")

// MessageFormat is used to format json string for sending to sink.
type MessageFormat struct {
	Kind         string   `json:"kind,omitempty"`
	Database     string   `json:"database,omitempty"`
	Schema       string   `json:"schema,omitempty"`
	Table        string   `json:"table,omitempty"`
	Statement    string   `json:"statement,omitempty"`
	ColumnNames  []string `json:"columnnames,omitempty"`
	ColumnTypes  []string `json:"columntypes,omitempty"`
	ColumnValues [][]any  `json:"columnvalues,omitempty"`
}

// pipeBuffer is used by pipe.
type pipeBuffer struct {
	baseBuffer
	outPutHead []byte
	buffer     bytes.Buffer
}

// newPipeBuffer return a pipeBuffer.
func newPipeBuffer(outPutHead []byte, flushLimit int) *pipeBuffer {
	buf := &pipeBuffer{
		baseBuffer: baseBuffer{flushLimit: flushLimit},
		outPutHead: outPutHead,
	}
	buf.reset()

	return buf
}

// push appends the received message into buffer and update the high-water mark of un-pushed data.
func (t *pipeBuffer) push(ts int64, msg []byte) {
	if t.maxTs < ts {
		t.maxTs = ts
	}
	if t.minTs > ts || t.minTs == cdcpb.InvalidWatermark {
		t.minTs = ts
	}

	if t.messageCount > 0 {
		t.buffer.WriteString(",")
	}

	t.buffer.Write(msg)
	t.messageCount++
}

// Bytes returns a copy slice of all cached messages.
func (t *pipeBuffer) bytes() []byte {
	mgs := make([]byte, t.buffer.Len())
	copy(mgs, t.buffer.Bytes())
	return mgs
}

// needFlush returns if the buffer reaches the limitation.
func (t *pipeBuffer) needFlush(incomingLen int) bool {
	return t.buffer.Len()+incomingLen >= t.flushLimit
}

// reset used to reset buffer.
func (t *pipeBuffer) reset() {
	t.buffer.Reset()
	t.buffer.Write(t.outPutHead)
	t.messageCount = 0
	t.maxTs = cdcpb.InvalidWatermark
	t.minTs = cdcpb.InvalidWatermark
}

// end appends end string of output json into buffer.
func (t *pipeBuffer) end() {
	t.buffer.WriteString("]}")
}

// PipeTask filters and sends the CDC message to sink (for example, kafka).
// It also sends the heartbeat message with low-water mark of pushed data to pipe job.
type PipeTask struct {
	BaseTask

	mutex  syncutil.Mutex
	buffer *pipeBuffer

	pipeMetadata *cdcpb.PipeMetadata
	sink         Sink
	// statementFormat is used to format json string for sending to sink.
	statementFormat MessageFormat
}

var _ Task = &PipeTask{}

// NewPipeTask create a new pipe task instance.
func NewPipeTask(
	ctx context.Context,
	meta *cdcpb.PipeMetadata,
	server cdcpb.CDCCoordinator_StartTsCDCServer,
	nodeID int32,
	sink Sink,
	heartbeatInterval time.Duration,
	flushLimit int,
	tableID uint64,
	instanceID uint64,
	pipeTable *cdcpb.CDCTable,
	lock *syncutil.Mutex,
) Task {
	sourceTable := pipeTable.Database + "." + pipeTable.Table

	pipeTask := &PipeTask{
		BaseTask: BaseTask{
			Ctx:               ctx,
			server:            server,
			localWaterMark:    InvalidWatermark,
			nodeID:            nodeID,
			heartbeatInterval: heartbeatInterval,
			errCh:             make(chan error, 1),
			outputColumnIDs:   pipeTable.OutputColumns,
			outputColumnNames: pipeTable.OutputColumnNames,
			instanceType:      sqlbase.CDCInstanceType_Pipe,
			instanceID:        instanceID,
			instanceName:      meta.Name,
			tableID:           tableID,
			needNormalTag:     pipeTable.NeedNormalTag,
			sourceTableName:   sourceTable,
			status:            false,
			operatorMap:       make(map[string]struct{}),
			cdcID:             meta.ID,
			serverMu:          lock,
		},
		sink:         sink,
		buffer:       newPipeBuffer(genOutPutHead(pipeTable, kafkaMgsKindInsert), flushLimit),
		pipeMetadata: meta,
		statementFormat: MessageFormat{
			Database: pipeTable.Database,
			Schema:   pipeTable.Schema,
			Table:    pipeTable.Table,
		},
	}

	for _, op := range strings.Split(meta.Publish, ",") {
		switch op {
		case cdcpb.EventAll:
			pipeTask.operatorMap[cdcpb.EventInsert] = struct{}{}
			pipeTask.operatorMap[cdcpb.EventUpdate] = struct{}{}
			pipeTask.operatorMap[cdcpb.EventDelete] = struct{}{}
			pipeTask.operatorMap[cdcpb.EventDDL] = struct{}{}
			break
		default:
			pipeTask.operatorMap[op] = struct{}{}
		}
	}

	return pipeTask
}

// Run listens the channels.
// 1. errCh handles the task error.
// 2. heartbeatTimer sends the heartbeat message to CDC consumer.
// 3. stopper stops the current CDC task.
func (t *PipeTask) Run(stopper *stop.Stopper) error {
	// Send the first heartbeat message indicates a successful connection.
	if err := t.BaseTask.SendHeartbeat(); err != nil {
		return err
	}

	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()
	heartbeatTimer.Reset(t.heartbeatInterval)

	for {
		select {
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
			if err := t.SendHeartbeat(); err != nil {
				return err
			}

			t.localWaterMark = InvalidWatermark

			heartbeatTimer.Reset(t.heartbeatInterval)
		case <-stopper.ShouldStop():
			return nil
		case <-t.Ctx.Done():
			return nil
		}
	}
}

// InitFilter init pipe filter.
func (t *PipeTask) InitFilter(
	columns []*sqlbase.ColumnDescriptor,
) ([]*execinfra.ExprHelper, error) {
	if t.columnTypes == nil {
		t.columnTypes = make([]types.T, len(columns))

		for i, col := range columns {
			t.columnTypes[i] = col.Type
		}
	}

	var filterHelper []*execinfra.ExprHelper

	if t.pipeMetadata.MetricsFilter != nil {
		filter, err := initFilter(t.pipeMetadata.MetricsFilter, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}
		filterHelper = append(filterHelper, filter)
	}

	for _, val := range t.pipeMetadata.TagFilter {
		filter, err := initFilter(val, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}

		filterHelper = append(filterHelper, filter)
	}

	return filterHelper, nil
}

// FilterRow filters the rows captured by CDC.
func (t *PipeTask) FilterRow(
	row sqlbase.EncDatumRow, filter []*execinfra.ExprHelper,
) (bool, error) {
	for _, exprHelper := range filter {
		if len(exprHelper.Types) != len(row) {
			_ = t.Stop()
			return false, errors.Errorf("filter and row column size is different, (%d, %d)", len(exprHelper.Types), len(row))
		}

		pass, err := exprHelper.EvalFilter(row)
		if err != nil {
			return false, err
		}

		if !pass {
			return false, nil
		}
	}

	return true, nil
}

// ConstructCDCRow encodes the filtered rows.
func (t *PipeTask) ConstructCDCRow(
	columns []*sqlbase.ColumnDescriptor,
	_ map[uint32]sqlbase.ColumnDescriptor,
	colInputIndex map[int]int,
	normalTagIndex map[int]int,
	inputDatum tree.Datums,
	encRows sqlbase.EncDatumRow,
	_ map[uint32]int,
) []interface{} {
	outValue := make([]interface{}, len(t.getOutputColumnIDs()))
	for j, colID := range t.getOutputColumnIDs() {
		inputIdx, ok := colInputIndex[int(colID)]
		if t.needNormalTag && columns[normalTagIndex[int(colID)]].IsTagCol() {
			outValue[j] = getDataFromDatum(encRows[normalTagIndex[int(colID)]].Datum)
		} else if !ok || inputIdx == -1 {
			outValue[j] = nil
		} else {
			outValue[j] = getDataFromDatum(inputDatum[inputIdx])
		}
	}
	return outValue
}

// FormatCDCRows converts the encoded rows to CDCPushData
func (t *PipeTask) FormatCDCRows(data []interface{}) *sqlbase.CDCPushData {
	if len(data) == 0 {
		return nil
	}

	jsonData, err := gojson.Marshal(data)
	if err != nil {
		log.Errorf(t.Ctx, "JSON marshal error: %s", err)
		return nil
	}

	// The format of jsonData is an array format, such as "[{row1},{row2},...]".
	// Before adding it to the buffer, "[]" needs to be removed, so the length must be greater than 2.
	if len(jsonData) >= 2 {
		pushData := &sqlbase.CDCPushData{
			TaskID:   t.getInstanceID(),
			TaskType: t.getInstanceType(),
			Rows:     [][]byte{jsonData[1 : len(jsonData)-1]},
		}
		return pushData
	}

	return nil
}

// Push pushes message to buffer and checks if it reaches the flush threshold.
func (t *PipeTask) Push(_ int64, osn uint64, data [][]byte) error {
	msg := data[0]
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.buffer.flushLimit <= 0 {
		t.buffer.push(int64(osn), msg)
		err := t.flush()
		if err != nil {
			t.errCh <- err
		}
		return nil
	}

	length := len(msg)
	if length >= t.buffer.flushLimit {
		t.errCh <- errMessageBytesExceedLimit
		return nil
	}

	if t.buffer.needFlush(length) {
		err := t.flush()
		if err != nil {
			t.errCh <- err
		}
	}

	t.buffer.push(int64(osn), msg)
	return nil
}

// Stop stops task.
func (t *PipeTask) Stop() error {
	if err := t.BaseTask.Stop(); err != nil {
		return err
	}

	return nil
}

// SendStatement used to send delete statement to sink.
// It uses the same sink as SendRows, and flush is executed before sending.
func (t *PipeTask) SendStatement(ons uint64, operation string, stmt []byte) error {
	if err := t.flush(); err != nil {
		return err
	}

	format := t.statementFormat
	format.Kind = operation
	format.Statement = string(stmt)
	if operation == cdcpb.EventUpdate && !t.checkUpdateTagsInOutput(format.Statement) {
		return nil
	}

	msg, err := gojson.Marshal(format)
	if err != nil {
		return err
	}

	if err := t.sink.Send(t.Ctx, strconv.FormatUint(ons, 10), msg); err != nil {
		return err
	}
	t.localWaterMark = int64(ons)

	return nil
}

func (t *PipeTask) getSink() Sink {
	return t.sink
}

func (t *PipeTask) flush() error {
	if t.buffer.count() == 0 {
		return nil
	}

	t.buffer.end()

	err := t.sink.Send(t.Ctx, strconv.FormatInt(t.buffer.minTimestamp(), 10), t.buffer.bytes())
	if err != nil {
		return err
	}

	if err := t.sink.Flush(t.Ctx); err != nil {
		return err
	}

	if t.localWaterMark > t.buffer.minTimestamp() || t.localWaterMark == InvalidWatermark {
		t.localWaterMark = t.buffer.minTimestamp()
	}

	t.buffer.reset()

	return nil
}

// SendHeartbeat sends message to CDC consumer periodically,
// subclass can overwrite it when need to run a periodical task
func (t *PipeTask) SendHeartbeat() error {
	t.mutex.Lock()

	if err := t.flush(); err != nil {
		return err
	}

	t.mutex.Unlock()

	return t.BaseTask.SendHeartbeat()
}

func genOutPutHead(metadata *cdcpb.CDCTable, kind string) []byte {
	output := fmt.Sprintf(
		outPutFormat,
		kind,
		metadata.Database,
		metadata.Schema,
		metadata.Table,
		"[\""+strings.Join(metadata.OutputColumnNames, "\",\"")+"\"]",
		"[\""+strings.Join(metadata.OutputColumnTypes, "\",\"")+"\"]",
	)

	return []byte(output)
}
