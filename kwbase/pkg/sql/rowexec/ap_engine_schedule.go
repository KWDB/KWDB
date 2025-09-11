// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package rowexec

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
import "C"
import (
	"context"
	"math/rand"
	"sync"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/engine/ape"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// ApEngineSchedule is used to retrieve data from ap storage.
type ApEngineSchedule struct {
	execinfra.ProcessorBase

	handle     unsafe.Pointer
	Rev        []byte
	sid        execinfrapb.StreamID
	processors []execinfrapb.ProcessorSpec
	timeZone   int

	value0 bool
	rowNum int
	engine *ape.Engine
}

var _ execinfra.Processor = &ApEngineSchedule{}
var _ execinfra.RowSource = &ApEngineSchedule{}

var apFlowSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.FlowSpec{}
	},
}

// NewAPEngineSchedule creates a apEngineSchedule.
func NewAPEngineSchedule(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	typs []types.T,
	output execinfra.RowReceiver,
	sid execinfrapb.StreamID,
	processors []execinfrapb.ProcessorSpec,
) (*ApEngineSchedule, error) {
	ttr := &ApEngineSchedule{
		sid:    sid,
		handle: nil,
		value0: len(typs) == 0,
		engine: flowCtx.Cfg.EngineHelper.GetAPEngine().(*ape.Engine),
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ttr.FinishTrace = ttr.outputStatsToTrace
	}

	if err := ttr.Init(
		ttr,
		&execinfrapb.PostProcessSpec{},
		typs,
		flowCtx,
		processorID,
		output,
		nil,
		execinfra.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        nil,
			TrailingMetaCallback: nil,
		},
	); err != nil {
		return nil, err
	}

	ttr.processors = processors

	ttr.StartInternal(ctx, tsTableReaderProcName)
	if err := ttr.setupFlow(ttr.Ctx); err != nil {
		return nil, err
	}

	return ttr, nil
}

// NewFlowSpec get ap flow spec.
func NewFlowSpec(flowID execinfrapb.FlowID, gateway roachpb.NodeID) *execinfrapb.FlowSpec {
	spec := apFlowSpecPool.Get().(*execinfrapb.FlowSpec)
	spec.FlowID = flowID
	spec.Gateway = gateway
	return spec
}

// Start is part of the RowSource interface.
func (ttr *ApEngineSchedule) Start(ctx context.Context) context.Context {
	ctx = ttr.StartInternal(ctx, tsTableReaderProcName)
	return ctx
}

// setupFlow initializes the time series flow in the ap engine
func (ttr *ApEngineSchedule) setupFlow(ctx context.Context) error {
	rand.Seed(timeutil.Now().UnixNano())
	randomNumber := rand.Intn(100000) + 1

	flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}

	tsFlowSpec := NewFlowSpec(flowID, ttr.FlowCtx.NodeID)
	tsFlowSpec.Processors = ttr.processors

	msg, err := protoutil.Marshal(tsFlowSpec)
	if err != nil {
		return err
	}

	if log.V(3) {
		log.Infof(ctx, "node: %v,\nts_physical_plan: %v\n", ttr.EvalCtx.NodeID, tsFlowSpec)
	}

	timezone, err := ttr.setupTimezone(ctx)
	if err != nil {
		return err
	}

	// Create query info for the ap engine
	queryInfo := ape.QueryInfo{
		ID:       int(ttr.sid),
		Buf:      msg,
		UniqueID: randomNumber,
		Handle:   ttr.handle, // Will be set by the engine
		TimeZone: timezone,
		SQL:      ttr.EvalCtx.Planner.GetStmt(),
	}

	respInfo, err := ttr.engine.SetupFlow(&(ttr.Ctx), queryInfo)
	if err != nil {
		if ttr.FlowCtx != nil {
			ttr.FlowCtx.TsHandleBreak = true
		}
		return err
	}

	// Store the handle and register it in the flow context
	ttr.handle = respInfo.Handle

	return nil
}

// Next is part of the RowSource interface.
func (ttr *ApEngineSchedule) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ttr.State == execinfra.StateRunning {
		if len(ttr.Rev) == 0 || (ttr.value0 && ttr.rowNum == 0) {
			// Prepare query info
			queryInfo := ape.QueryInfo{
				ID:       int(ttr.sid),
				Handle:   ttr.handle,
				TimeZone: ttr.timeZone,
				Buf:      []byte(cmdExecNext),
			}

			// Execute query to get next batch of data
			respInfo, err := ttr.engine.NextFlow(&(ttr.Ctx), queryInfo)
			// Handle response codes
			switch respInfo.Code {
			case -1: // End of data
				return nil, ttr.handleEndOfData()
			case 1: // Success
				// Store the returned data and row count
				ttr.Rev = respInfo.Buf
				ttr.rowNum = respInfo.RowNum
			default: // Error
				return nil, ttr.handleFetchError(&respInfo, err)
			}
		}

		if ttr.value0 {
			if ttr.rowNum > 0 {
				var tmpRow sqlbase.EncDatumRow = make([]sqlbase.EncDatum, 0)
				ttr.rowNum--
				return tmpRow, nil
			}
			return nil, ttr.DrainHelper()
		}

		// Parse and return a row from the buffer
		return ttr.parseRowFromBuffer()
	}

	return nil, ttr.DrainHelper()
}

// NextPgWire get data for short circuit go pg encoding.
func (ttr *ApEngineSchedule) NextPgWire() (val []byte, code int, err error) {
	for ttr.State == execinfra.StateRunning {
		var queryInfo = ape.QueryInfo{
			ID:       int(ttr.sid),
			Handle:   ttr.handle,
			Buf:      []byte(cmdExecNext),
			TimeZone: ttr.timeZone,
		}

		respInfo, err := ttr.engine.NextFlowPgWire(&(ttr.Ctx), queryInfo)

		// Handle response codes
		switch respInfo.Code {
		case -1: // End of data
			return nil, respInfo.Code, nil
		case 1: // Success
			return respInfo.Buf, respInfo.Code, nil
		default: // Error
			if err != nil && ttr.FlowCtx != nil {
				ttr.FlowCtx.TsHandleBreak = true
			}
			log.Errorf(context.Background(), err.Error())
			return nil, respInfo.Code, err
		}
	}

	meta := ttr.DrainHelper()
	if meta.Err != nil {
		return nil, 0, meta.Err
	}
	return nil, -1, nil
}

// ConsumerClosed is part of the RowSource interface.
func (ttr *ApEngineSchedule) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ttr.InternalClose()
}

// cleanup releases resources used by the TsTableReader
func (ttr *ApEngineSchedule) cleanup(ctx context.Context) {
	// DropHandle(ctx)
}

// DropHandle is to close ap handle.
func (ttr *ApEngineSchedule) DropHandle(ctx context.Context) {
	if ttr.handle != nil {
		var CloseInfo ape.QueryInfo
		CloseInfo.Handle = ttr.handle
		CloseInfo.Buf = []byte("close flow")
		closeErr := ttr.engine.CloseFlow(&(ttr.Ctx), CloseInfo)
		if closeErr != nil {
			log.Warning(ctx, closeErr)
		}
		ttr.handle = nil
	}
}

// setupTimezone determines the timezone offset to use for the query
func (ttr *ApEngineSchedule) setupTimezone(ctx context.Context) (int, error) {
	locStr := ttr.EvalCtx.GetLocation().String()
	loc, err := timeutil.TimeZoneStringToLocation(locStr, timeutil.TimeZoneStringToLocationISO8601Standard)
	if err != nil {
		return 0, err
	}

	// Convert current time to the specified timezone and get the offset
	currentTime := timeutil.Now()
	timeInLocation := currentTime.In(loc)
	_, offset := timeInLocation.Zone()

	// Convert offset from seconds to hours
	timezone := offset / 3600
	ttr.timeZone = timezone

	return timezone, nil
}

// parseRowFromBuffer parses a row from the current buffer
func (ttr *ApEngineSchedule) parseRowFromBuffer() (
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	row := make([]sqlbase.EncDatum, len(ttr.Out.OutputTypes))

	// Parse each column from the buffer
	for i := range row {
		var err error
		row[i], ttr.Rev, err = sqlbase.EncDatumFromBuffer(nil, sqlbase.DatumEncoding_VALUE, ttr.Rev)

		// Handle parsing errors
		if err != nil {
			if ttr.FlowCtx != nil {
				ttr.FlowCtx.TsHandleBreak = true
			}
			log.Errorf(context.Background(), err.Error())
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
	}

	return row, nil
}

// handleEndOfData handles the end-of-data response code
func (ttr *ApEngineSchedule) handleEndOfData() *execinfrapb.ProducerMetadata {
	return nil
}

// handleFetchError handles error responses from the ap engine
func (ttr *ApEngineSchedule) handleFetchError(
	respInfo *ape.QueryInfo, err error,
) *execinfrapb.ProducerMetadata {
	if err == nil {
		err = errors.Newf("There is no error message for this error code. The err code is %d.\n", respInfo.Code)
	}

	ttr.MoveToDraining(err)
	log.Errorf(context.Background(), err.Error())
	if ttr.FlowCtx != nil {
		ttr.FlowCtx.TsHandleBreak = true
	}
	return &execinfrapb.ProducerMetadata{Err: err}
}

// InitProcessorProcedure init processor in procedure
func (ttr *ApEngineSchedule) InitProcessorProcedure(txn *kv.Txn) {}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (ttr *ApEngineSchedule) outputStatsToTrace() {
	var tsi TsInputStats
	//for _, stats := range ttr.statsList {
	//	tsi.SetTsInputStats(stats)
	//}

	sp := opentracing.SpanFromContext(ttr.PbCtx())
	if sp != nil {
		tracing.SetSpanStats(sp, &tsi)
	}
}
