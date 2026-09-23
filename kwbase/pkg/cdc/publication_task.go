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
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/jackc/pgx/pgproto3"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

// pgBindBuffer is used to buffer the bind data to be sent.
type pgBindBuffer struct {
	baseBuffer
	buffer pgproto3.Bind
}

// newPGBindBuffer initializes and returns instance of pgBindBuffer.
func newPGBindBuffer(preparedStmt string, flushLimit int) *pgBindBuffer {
	buf := &pgBindBuffer{
		baseBuffer: baseBuffer{flushLimit: flushLimit, bufferLength: 0},
		buffer: pgproto3.Bind{
			DestinationPortal:    "",
			PreparedStatement:    preparedStmt,
			ParameterFormatCodes: []int16{pgproto3.BinaryFormat},
			ResultFormatCodes:    []int16{pgproto3.BinaryFormat},
		},
	}
	buf.reset()

	return buf
}

// pushBatch appends the received batch message into buffer and update the high-water mark of un-pushed data.
func (t *pgBindBuffer) pushBatch(ts int64, msg [][]byte, length int) {
	if t.maxTs < ts {
		t.maxTs = ts
	}
	if t.minTs > ts || t.minTs == cdcpb.InvalidWatermark {
		t.minTs = ts
	}

	t.buffer.Parameters = append(t.buffer.Parameters, msg...)
	t.messageCount++
	t.bufferLength += length
}

// bytes returns a copy slice of all cached messages.
func (t *pgBindBuffer) bytes() []byte {
	mgs := make([]byte, 0)
	return t.buffer.Encode(mgs)
}

// needFlush returns if the buffer reaches the limitation.
func (t *pgBindBuffer) needFlush(incomingLen int) bool {
	return t.bufferLength+incomingLen > t.flushLimit
}

// reset is to reset the buffer.
func (t *pgBindBuffer) reset() {
	t.buffer.Parameters = make([][]byte, 0)
	t.messageCount = 0
	t.bufferLength = 0
	t.maxTs = cdcpb.InvalidWatermark
	t.minTs = cdcpb.InvalidWatermark
}

// end appends suffix string of output json into buffer.
func (t *pgBindBuffer) end() {
}

// count return the count of messages in buffer.
func (t *pgBindBuffer) count() int {
	return t.messageCount
}

func (t *pgBindBuffer) setName(bufName string) {
	t.buffer.PreparedStatement = bufName
}

// PublicationTask filters and sends the CDC message to sink (for example, kafka).
// It also sends the heartbeat message with low-water mark of pushed data to publication.
type PublicationTask struct {
	BaseTask

	mutex  syncutil.Mutex
	buffer *pgBindBuffer

	clusterID   string
	pubMetadata *cdcpb.PubMetadata
	pubParams   *cdcpb.PubParameters
	format      pgwirebase.FormatCode
}

var _ Task = &PublicationTask{}

// NewPublicationTask creates and returns a new publication task instance.
func NewPublicationTask(
	ctx context.Context,
	request *cdcpb.TsChangeDataCaptureRequest,
	server cdcpb.CDCCoordinator_StartTsCDCServer,
	nodeID int32,
	pubParam *cdcpb.PubParameters,
	heartbeatInterval time.Duration,
	flushLimit int,
	pubTable *cdcpb.CDCTableInfo,
	lock *syncutil.Mutex,
) Task {
	sourceTable := pubTable.Database + "." + pubTable.Table
	prepareName := request.PubMetadata.Name + "_" + sourceTable

	publicationTask := &PublicationTask{
		BaseTask: BaseTask{
			Ctx:               ctx,
			server:            server,
			localWaterMark:    InvalidWatermark,
			nodeID:            nodeID,
			heartbeatInterval: heartbeatInterval,
			errCh:             make(chan error, 1),
			outputColumnIDs:   pubTable.ColIDs,
			outputColumnNames: pubTable.ColNames,
			instanceType:      sqlbase.CDCInstanceType_Publication,
			instanceID:        request.PubMetadata.ID,
			instanceName:      request.PubMetadata.Name,
			tableID:           pubTable.ID,
			needNormalTag:     pubTable.NeedNormalTag,
			sourceTableName:   sourceTable,
			status:            true,
			operatorMap:       map[string]struct{}{},
			serverMu:          lock,
		},
		pubParams:   pubParam,
		buffer:      newPGBindBuffer(prepareName, flushLimit),
		pubMetadata: request.PubMetadata,
		format:      pgwirebase.FormatBinary,
	}

	for _, op := range strings.Split(pubParam.PubOptions.Publish, ",") {
		switch op {
		case cdcpb.EventAll:
			publicationTask.operatorMap[cdcpb.EventInsert] = struct{}{}
			publicationTask.operatorMap[cdcpb.EventUpdate] = struct{}{}
			publicationTask.operatorMap[cdcpb.EventDelete] = struct{}{}
			break
		default:
			publicationTask.operatorMap[op] = struct{}{}
		}
	}

	return publicationTask
}

// Run listens the channels.
// 1. errCh handles the task error.
// 2. heartbeatTimer sends the heartbeat message to CDC consumer.
// 3. stopper stops the current CDC task.
func (t *PublicationTask) Run(stopper *stop.Stopper) error {
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

// InitFilter initializes the filter of publication.
func (t *PublicationTask) InitFilter(
	columns []*sqlbase.ColumnDescriptor,
) ([]*execinfra.ExprHelper, error) {
	if t.columnTypes == nil {
		t.columnTypes = make([]types.T, len(columns))

		for i, col := range columns {
			t.columnTypes[i] = col.Type
		}
	}

	var filterHelper []*execinfra.ExprHelper

	if t.pubMetadata.MetricsFilter != nil {
		filter, err := initFilter(t.pubMetadata.MetricsFilter, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}
		filterHelper = append(filterHelper, filter)
	}

	for _, val := range t.pubMetadata.TagFilter {
		filter, err := initFilter(val, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}

		filterHelper = append(filterHelper, filter)
	}

	return filterHelper, nil
}

// FilterRow filters the rows captured by CDC.
func (t *PublicationTask) FilterRow(
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

// ConstructCDCRow encodes the filtered rows into format of postgresql protocol message.
func (t *PublicationTask) ConstructCDCRow(
	columns []*sqlbase.ColumnDescriptor,
	colMap map[uint32]sqlbase.ColumnDescriptor,
	colInputIndex map[int]int,
	normalTagIndex map[int]int,
	inputDatum tree.Datums,
	encRows sqlbase.EncDatumRow,
	_ map[uint32]int,
) []interface{} {
	outValue := make([]interface{}, len(t.getOutputColumnIDs()))
	for j, colID := range t.getOutputColumnIDs() {
		inputIdx, ok := colInputIndex[int(colID)]
		col := *columns[normalTagIndex[int(colID)]]
		if t.needNormalTag && col.IsTagCol() {
			outValue[j] = t.encodePGFormat(encRows[normalTagIndex[int(colID)]].Datum, col.Type)
		} else if !ok || inputIdx == -1 {
			outValue[j] = t.encodePGFormat(tree.DNull, col.Type)
		} else {
			col, _ = colMap[colID]
			outValue[j] = t.encodePGFormat(inputDatum[inputIdx], col.Type)
		}
	}

	return outValue
}

// FormatCDCRows converts the encoded rows to CDCPushData
func (t *PublicationTask) FormatCDCRows(data []interface{}) *sqlbase.CDCPushData {
	if len(data) == 0 {
		return nil
	}

	pushData := &sqlbase.CDCPushData{
		TaskID:   t.getInstanceID(),
		TaskType: t.getInstanceType(),
		Rows:     make([][]byte, 0),
	}

	for _, row := range data {
		values := row.([]interface{})
		for _, val := range values {
			pushData.Rows = append(pushData.Rows, val.([]byte))
		}
	}

	return pushData
}

// SendStatement sends delete, update statement to subscription.
func (t *PublicationTask) SendStatement(ons uint64, operation string, stmt []byte) error {
	if err := t.flush(); err != nil {
		return err
	}

	format := int32(cdcpb.FormatSQL)
	rowNumber := int32(1)

	if operation == cdcpb.EventUpdate && !t.checkUpdateTagsInOutput(string(stmt)) {
		return nil
	}

	evt := &cdcpb.TsChangeDataCaptureEvent{
		Val: &cdcpb.TsChangeDataCaptureValue{
			Ts:        int64(ons),
			Val:       [][]byte{stmt},
			TableID:   t.tableID,
			Format:    &format,
			RowNumber: &rowNumber,
			Operation: &operation,
			OSN:       ons,
		},
	}
	t.localWaterMark = t.buffer.maxTimestamp()

	return t.server.Send(evt)
}

// Push pushes message to buffer and checks if it reaches the flush threshold.
// If the buffer reaches the flush threshold, will flush the cached data.
// Otherwise, only cache the specified data.
func (t *PublicationTask) Push(_ int64, osn uint64, cdcData [][]byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	fieldsPerRow := len(t.BaseTask.outputColumnIDs)
	bindBatchRows := BindParametersLimitation / fieldsPerRow
	sentBytesPerBatch := 0
	sentDataPerBatch := make([][]byte, 0)
	sentRowsPerBatch := 0
	for idx, field := range cdcData {
		sentBytesPerBatch += len(field)
		sentDataPerBatch = append(sentDataPerBatch, field)
		if (idx+1)%fieldsPerRow != 0 {
			continue
		}
		// the fields in rows in complete
		sentRowsPerBatch++
		// send data if the data bytes reach the buffer-limit or the rows reach the bind batch of postgresql.
		if t.buffer.needFlush(sentBytesPerBatch) || sentRowsPerBatch == bindBatchRows {
			t.buffer.pushBatch(int64(osn), sentDataPerBatch, sentRowsPerBatch)
			err := t.flush()
			if err != nil {
				t.errCh <- err
			}
			t.buffer.reset()
			sentDataPerBatch = make([][]byte, 0)
			sentBytesPerBatch = 0
			sentRowsPerBatch = 0
		}
	}
	if sentRowsPerBatch == 0 {
		return nil
	}
	// send the rest data
	t.buffer.pushBatch(int64(osn), sentDataPerBatch, sentRowsPerBatch)
	err := t.flush()
	if err != nil {
		t.errCh <- err
	}
	t.buffer.reset()
	return nil
}

// flush sends data cached in buffer and resets the buffer.
func (t *PublicationTask) flush() error {
	if t.buffer.count() == 0 {
		return nil
	}

	t.buffer.end()

	format := int32(cdcpb.FormatPGBindBinary)
	rowNum := int32(t.buffer.count())
	t.localWaterMark = t.buffer.maxTimestamp()
	evt := &cdcpb.TsChangeDataCaptureEvent{
		Val: &cdcpb.TsChangeDataCaptureValue{
			Ts:        t.localWaterMark,
			Val:       [][]byte{t.buffer.bytes()},
			TableID:   t.tableID,
			Format:    &format,
			RowNumber: &rowNum,
			OSN:       uint64(t.localWaterMark),
		},
	}

	t.localWaterMark = t.buffer.maxTimestamp()
	t.buffer.reset()

	t.serverMu.Lock()
	defer t.serverMu.Unlock()

	return t.server.Send(evt)
}

// SendHeartbeat sends message to CDC consumer periodically,
// subclass can overwrite it when need to run a periodical task.
func (t *PublicationTask) SendHeartbeat() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.flush(); err != nil {
		return err
	}

	return t.BaseTask.SendHeartbeat()
}

// encodePGFormat encodes the specified datum into format of postgresql protocol message.
func (t *PublicationTask) encodePGFormat(datum tree.Datum, typ types.T) []byte {
	switch t.format {
	case pgwirebase.FormatText:
		return getDataText(datum, typ)
	case pgwirebase.FormatBinary:
		return getDataBinary(datum, typ)
	default:
		return int32ToBytes(-1)
	}
}

// getDataText formats the specified datum into text format of postgresql protocol message.
func getDataText(datum tree.Datum, typ types.T) []byte {
	if datum == tree.DNull {
		return nil
	}

	switch v := datum.(type) {
	case *tree.DInt:
		switch typ.Oid() {
		case oid.T_timestamptz, oid.T_timestamp:
			t := timeutil.FromTimestamp(int64(*v), typ.InternalType.Precision)
			return pq.FormatTimestamp(t)
		default:
			return strconv.AppendInt(nil, int64(*v), 10)
		}
	case *tree.DTimestamp:
		return pq.FormatTimestamp(v.Time)
	case *tree.DTimestampTZ:
		return pq.FormatTimestamp(v.Time)
	case *tree.DFloat:
		return strconv.AppendFloat(nil, float64(*v), 'f', -1, 64)
	case *tree.DBool:
		return strconv.AppendBool(nil, bool(*v))
	case *tree.DString:
		return []byte(*v)
	case *tree.DBytes:
		return []byte(*v)
	default:
		return nil
	}
}

// getDataBinary formats the specified datum into binary format of postgresql protocol message.
func getDataBinary(datum tree.Datum, typ types.T) []byte {
	if datum == tree.DNull {
		return nil
	}

	switch v := datum.(type) {
	case *tree.DInt:
		switch typ.Oid() {
		case oid.T_timestamptz, oid.T_timestamp:
			t := timeutil.FromTimestamp(int64(*v), typ.InternalType.Precision)
			return timeToPgBinary(t, nil)
		case oid.T_int2:
			return int16ToBytes(int16(*v))
		case oid.T_int4:
			return int32ToBytes(int32(*v))
		case oid.T_int8:
			return int64ToBytes(int64(*v))
		}
	case *tree.DTimestamp:
		return timeToPgBinary(v.Time, nil)
	case *tree.DTimestampTZ:
		return timeToPgBinary(v.Time, nil)
	case *tree.DFloat:

		switch typ.Oid() {
		case oid.T_float4:
			return int32ToBytes(int32(math.Float32bits(float32(*v))))
		case oid.T_float8:
			return int64ToBytes(int64(math.Float64bits(float64(*v))))
		}
	case *tree.DBool:
		if *v {
			return []byte{1}
		}
		return []byte{0}
	case *tree.DString:
		return []byte(*v)
	case *tree.DBytes:
		return []byte(*v)
	}

	return nil
}

// timeToPgBinary calculates the Postgres binary format for a timestamp. The timestamp
// is represented as the number of microseconds between the given time and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int64.
func timeToPgBinary(t time.Time, offset *time.Location) []byte {
	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}

	x := make([]byte, 8)
	n := duration.DiffMicros(t, pgwirebase.PGEpochJDate)
	binary.BigEndian.PutUint64(x, uint64(n))

	return x
}

// int16ToBytes encodes int16 data into bytes in big endian.
func int16ToBytes(v int16) []byte {
	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(v))
	return x
}

// int32ToBytes encodes int32 data into bytes in big endian.
func int32ToBytes(v int32) []byte {
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(v))
	return x
}

// int64ToBytes encodes int64 data into bytes in big endian.
func int64ToBytes(v int64) []byte {
	x := make([]byte, 8)
	binary.BigEndian.PutUint64(x, uint64(v))
	return x
}
