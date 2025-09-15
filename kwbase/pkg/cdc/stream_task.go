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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// StreamTask consumes the CDC results and send them to Stream job.
type StreamTask struct {
	BaseTask

	// metricsFilter is filter of metrics.
	metricsFilter []byte
	// tagFilter is filter list of tags.
	tagFilter [][]byte
}

var _ Task = &StreamTask{}

func newStreamTask(
	ctx context.Context,
	request *cdcpb.TsChangeDataCaptureRequest,
	server cdcpb.CDCCoordinator_StartTsCDCServer,
	nodeID int32,
) (Task, error) {
	streamPara, err := sqlutil.ParseStreamParameters(request.StreamMetadata.Parameters)
	if err != nil {
		return nil, err
	}

	streamOpts, err := sqlutil.ParseStreamOpts(&streamPara.Options)
	if err != nil {
		return nil, err
	}

	sourceTable := streamPara.SourceTable.Database + "." + streamPara.SourceTable.Table

	streamTask := &StreamTask{
		BaseTask: BaseTask{
			Ctx:               ctx,
			server:            server,
			localWaterMark:    InvalidWatermark,
			nodeID:            nodeID,
			heartbeatInterval: streamOpts.HeartbeatInterval,
			errCh:             make(chan error, 1),
			outputColumnIDs:   request.CDCColumns.CDCColumnIDs,
			instanceType:      cdcpb.TSCDCInstanceType_Stream,
			instanceID:        request.StreamMetadata.ID,
			tableID:           streamPara.SourceTableID,
			outColumnTypes:    request.CDCColumns.CDCTypes,
			needNormalTag:     *request.CDCColumns.NeedNormalTags,
			sourceTableName:   sourceTable,
		},
		metricsFilter: request.StreamMetadata.MetricsFilter,
		tagFilter:     request.StreamMetadata.TagFilter,
	}

	return streamTask, nil
}

// Run listens the channels.
// 1. errCh handles the task error.
// 2. heartbeatTimer sends the heartbeat message to CDC consumer.
// 3. stopper stops the current CDC task.
func (t *StreamTask) Run(stopper *stop.Stopper) error {
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
		}
	}
}

// InitFilter init the CDC filter, convert filter from static bytes to dynamic expr that
// can be directly used for encRow filtering.
func (t *StreamTask) InitFilter(
	columns []*sqlbase.ColumnDescriptor,
) ([]*execinfra.ExprHelper, error) {
	if t.columnTypes == nil {
		t.columnTypes = make([]types.T, len(columns))

		for i, col := range columns {
			t.columnTypes[i] = col.Type
		}
	}

	var filterHelper []*execinfra.ExprHelper

	if t.metricsFilter != nil {
		filter, err := initFilter(t.metricsFilter, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}
		filterHelper = append(filterHelper, filter)
	}

	for _, val := range t.tagFilter {
		filter, err := initFilter(val, t.columnTypes)
		if err != nil {
			return filterHelper, err
		}

		filterHelper = append(filterHelper, filter)
	}

	return filterHelper, nil
}

// FilterRow filters the rows captured by CDC.
func (t *StreamTask) FilterRow(
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
func (t *StreamTask) ConstructCDCRow(
	_ []*sqlbase.ColumnDescriptor,
	_ map[int]int,
	_ map[int]int,
	_ tree.Datums,
	encRows sqlbase.EncDatumRow,
	colIDMap map[uint32]int,
) []interface{} {
	var buf []byte
	var err error
	var a sqlbase.DatumAlloc

	colTypes := t.getOutColumnTypes()
	for idx, colID := range t.getOutputColumnIDs() {
		colIdx := colIDMap[colID]
		buf, err = encRows[colIdx].Encode(&colTypes[idx], &a, sqlbase.DatumEncoding_VALUE, buf)
		if err != nil {
			return nil
		}
	}

	return []interface{}{buf}
}

// FormatCDCRows converts the encoded rows to CDCPushData.
func (t *StreamTask) FormatCDCRows(data []interface{}) *sqlbase.CDCPushData {
	var rows [][]byte

	for _, value := range data {
		tmp, _ := value.([]interface{})
		row, ok := tmp[0].([]byte)
		if ok && len(row) > 0 {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		return nil
	}

	pushData := &sqlbase.CDCPushData{
		TaskID:   t.getInstanceID(),
		TaskType: t.getInstanceType(),
		Rows:     rows,
		Types:    t.getOutColumnTypes(),
	}
	return pushData
}

// Push pushes a message to buffer and checks if it reaches the flush threshold.
func (t *StreamTask) Push(ts int64, data [][]byte) error {
	t.serverMu.Lock()
	defer t.serverMu.Unlock()

	evt := &cdcpb.TsChangeDataCaptureEvent{
		Val: &cdcpb.TsChangeDataCaptureValue{Ts: ts, Val: data},
	}

	// extract the max timestamp from input.
	if t.localWaterMark < ts {
		t.localWaterMark = ts
	}
	return t.server.Send(evt)
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
