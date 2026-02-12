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
// #cgo LDFLAGS: -lkwdbts2 -lrocksdb -lcommon -lsnappy -lm  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
import "C"
import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

const (
	tsTableReaderProcName = "ts table reader"
	// Maximum timeout for waiting BLJ operators to complete
	maxBLJWaitTimeout = 10 * time.Second
	// Ticker interval for checking BLJ completion
	bljCheckInterval = 10 * time.Millisecond
	// Command strings for TS engine
	cmdExecNext    = "exec next"
	cmdCloseTsFlow = "close tsflow"
)

// TsTableReader is used to retrieve data from ts storage.
type TsTableReader struct {
	execinfra.ProcessorBase

	tsHandle         unsafe.Pointer
	Rev              []byte
	sid              execinfrapb.StreamID
	tsProcessorSpecs []execinfrapb.ProcessorSpec
	timeZone         int
	timeZoneName     string // IANA timezone name for DST support
	useDST           bool   // Whether timezone observes DST

	value0 bool
	rowNum int

	manualAddTsCol  bool
	tsTableReaderID int32

	collected bool
	statsList []tse.TsFetcherStats

	tsInfo   execinfrapb.TsInfo
	waitChan chan struct{}
	mu       syncutil.Mutex
}

var _ execinfra.Processor = &TsTableReader{}
var _ execinfra.RowSource = &TsTableReader{}

var kwdbFlowSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.TSFlowSpec{}
	},
}

// contextTsTableKey is an empty type for the handle associated with the
// statement value (see context.Value).
type contextTsTableKey struct{}

// NewTsTableReader creates a TsTableReader.
func NewTsTableReader(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	typs []types.T,
	output execinfra.RowReceiver,
	sid execinfrapb.StreamID,
	tsProcessorSpecs []execinfrapb.ProcessorSpec,
	tsInfo execinfrapb.TsInfo,
) (*TsTableReader, error) {
	ttr := &TsTableReader{
		sid:      sid,
		tsHandle: nil,
		tsInfo:   tsInfo,
		value0:   len(typs) == 0,
	}
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ttr.collected = true
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
	if err := ttr.initTableReader(ctx, tsProcessorSpecs, sid, flowCtx); err != nil {
		return nil, err
	}
	ttr.StartInternal(ctx, tsTableReaderProcName)
	if err := ttr.setupTsFlow(ttr.Ctx); err != nil {
		return nil, err
	}
	ttr.waitChan = make(chan struct{})
	// to cancel ts flow when context is done.
	go func() {
		select {
		case <-ttr.Ctx.Done():
			ttr.cancelTsFlow()
		case <-ttr.waitChan:
			// Exit normally
		}
	}()
	return ttr, nil
}

func (ttr *TsTableReader) safeCloseWaitChan() {
	if ttr.waitChan == nil {
		return
	}
	close(ttr.waitChan)
	ttr.waitChan = nil
}

func (ttr *TsTableReader) initTableReader(
	ctx context.Context,
	tsProcessorSpecs []execinfrapb.ProcessorSpec,
	sid execinfrapb.StreamID,
	flowCtx *execinfra.FlowCtx,
) error {

	// setup ts flow.
	err := ttr.SetupTsFlow(sid, tsProcessorSpecs)
	if err != nil {
		return err
	}
	var info tse.TsQueryInfo
	info.Handle = nil
	info.Buf = []byte("init ts handle")
	respInfo, err := ttr.FlowCtx.Cfg.TsEngine.InitTsHandle(&ctx, info)
	if err != nil {
		log.Warning(ctx, err)
		return err
	}
	ttr.tsHandle = respInfo.Handle
	ttr.FlowCtx.TsHandleMap[ttr.tsTableReaderID] = ttr.tsHandle
	return nil
}

// initPipelineTableReader fill ttr.tsProcessorSpecs
func (ttr *TsTableReader) initPipelineTableReader(
	tsProcessorSpecs []execinfrapb.ProcessorSpec,
) error {
	for _, tsp := range tsProcessorSpecs {
		if tsp.Core.TableReader != nil {
			// blj cannot use pipeline
			ttr.tsTableReaderID = tsp.Core.TsTableReader.TsTablereaderId
		}
		ttr.tsProcessorSpecs = append(ttr.tsProcessorSpecs, tsp)
	}

	if log.V(3) {
		log.Infof(ttr.PbCtx(), "nodeID:%v, tsFlowSpec.Processors:%v\n",
			ttr.FlowCtx.NodeID, len(ttr.tsProcessorSpecs))
	}
	return nil
}

// SetupTsFlow connects the flowSpec of this node according to the stream
func (ttr *TsTableReader) SetupTsFlow(
	sid execinfrapb.StreamID, tsProcessorSpecs []execinfrapb.ProcessorSpec,
) error {
	// construct outStreamMap, key is outStreamID, value is input ProcessorSpec.
	outStreamMap := constructOutStreamMap(tsProcessorSpecs)
	// fill tsFlow.
	if err := ttr.fillTsProcessor(sid, outStreamMap); err != nil {
		return err
	}
	return nil
}

func constructOutStreamMap(
	tsProcessorSpecs []execinfrapb.ProcessorSpec,
) map[execinfrapb.StreamID]execinfrapb.ProcessorSpec {
	// construct outStreamMap
	outputMap := make(map[execinfrapb.StreamID]execinfrapb.ProcessorSpec)
	for _, proc := range tsProcessorSpecs {
		for _, stream := range proc.Output[0].Streams {
			outputMap[stream.StreamID] = proc
		}
	}
	return outputMap
}

func (ttr *TsTableReader) fillTsProcessor(
	sid execinfrapb.StreamID, OutStreamMap map[execinfrapb.StreamID]execinfrapb.ProcessorSpec,
) error {
	topProcessor, ok := OutStreamMap[sid]
	if !ok {
		return errors.Errorf("cannot find streamID %v\n", sid)
	}
	if topProcessor.Core.TsTableReader != nil {
		ttr.tsTableReaderID = topProcessor.Core.TsTableReader.TsTablereaderId
	}
	if topProcessor.Input == nil {
		ttr.tsProcessorSpecs = append(ttr.tsProcessorSpecs, topProcessor)
		return nil
	}
	for _, streamInput := range topProcessor.Input[0].Streams {
		if streamInput.Type != execinfrapb.StreamEndpointType_REMOTE {
			err := ttr.fillTsProcessor(streamInput.StreamID, OutStreamMap)
			if err != nil {
				return err
			}
		}
	}
	ttr.tsProcessorSpecs = append(ttr.tsProcessorSpecs, topProcessor)
	return nil
}

// NewTSFlowSpec get ts flow spec.
func NewTSFlowSpec(flowID execinfrapb.FlowID, gateway roachpb.NodeID) *execinfrapb.TSFlowSpec {
	spec := kwdbFlowSpecPool.Get().(*execinfrapb.TSFlowSpec)
	spec.FlowID = flowID
	spec.Gateway = gateway
	return spec
}

// Start is part of the RowSource interface.
func (ttr *TsTableReader) Start(ctx context.Context) context.Context {
	ctx = ttr.StartInternal(ctx, tsTableReaderProcName)
	return ctx
}

// Next is part of the RowSource interface.
func (ttr *TsTableReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ttr.State == execinfra.StateRunning {
		if len(ttr.Rev) == 0 || (ttr.value0 && ttr.rowNum == 0) {
			// Prepare query info
			tsQueryInfo := tse.TsQueryInfo{
				ID:           int(ttr.sid),
				Handle:       ttr.tsHandle,
				TimeZone:     ttr.timeZone,
				TimeZoneName: ttr.timeZoneName,
				UseDST:       ttr.useDST,
				Buf:          []byte(cmdExecNext),
				Fetcher:      tse.TsFetcher{Collected: ttr.collected},
			}
			// Init analyse fetcher.
			if ttr.collected {
				ttr.initStatsCollector(&tsQueryInfo)
			}

			// Execute query to get next batch of data
			respInfo, err := ttr.FlowCtx.Cfg.TsEngine.NextTsFlow(&(ttr.Ctx), tsQueryInfo)

			// Collect statistics (if enabled)
			if ttr.collected {
				ttr.updateStatsList(&respInfo)
			}

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
func (ttr *TsTableReader) NextPgWire() (val []byte, code int, err error) {
	for ttr.State == execinfra.StateRunning {
		var tsQueryInfo = tse.TsQueryInfo{
			ID:           int(ttr.sid),
			Handle:       ttr.tsHandle,
			Buf:          []byte(cmdExecNext),
			TimeZone:     ttr.timeZone,
			TimeZoneName: ttr.timeZoneName,
			UseDST:       ttr.useDST,
			Fetcher:      tse.TsFetcher{Collected: ttr.collected},
		}

		// Init analyse fetcher.
		if ttr.collected {
			ttr.initStatsCollector(&tsQueryInfo)
		}

		respInfo, err := ttr.FlowCtx.Cfg.TsEngine.NextTsFlowPgWire(&(ttr.Ctx), tsQueryInfo)

		// Collect statistics (if enabled)
		if ttr.collected {
			ttr.updateStatsList(&respInfo)
		}

		// Handle response codes
		switch respInfo.Code {
		case -1: // End of data
			if ttr.collected {
				ttr.MoveToDraining(nil)
				meta := ttr.DrainHelper()
				if meta.Err != nil {
					return nil, 0, meta.Err
				}
				if len(meta.TraceData) > 0 {
					span := opentracing.SpanFromContext(ttr.Ctx)
					if span == nil {
						return nil, 0, errors.New("trying to ingest remote spans but there is no recording span set up")
					} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
						return nil, 0, errors.Errorf("error ingesting remote spans: %s", err)
					}
				}
				ttr.Out.Output().Push(nil, meta)
			}
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
func (ttr *TsTableReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ttr.InternalClose()
}

// cleanup releases resources used by the TsTableReader
func (ttr *TsTableReader) cleanup(ctx context.Context) {
	// DropHandle(ctx)
}

// DropHandle is to close ts handle.
func (ttr *TsTableReader) DropHandle(ctx context.Context) {
	ttr.mu.Lock()
	defer ttr.mu.Unlock()
	if ttr.tsHandle != nil {
		var tsCloseInfo tse.TsQueryInfo
		tsCloseInfo.Handle = ttr.tsHandle
		tsCloseInfo.Buf = []byte("close tsflow")
		closeErr := ttr.FlowCtx.Cfg.TsEngine.CloseTsFlow(&(ttr.Ctx), tsCloseInfo)
		if closeErr != nil {
			log.Warning(ctx, closeErr)
		}
		ttr.tsHandle = nil
	}
	ttr.safeCloseWaitChan()
}

// cancelTsFlow is to cancel ts flow.
func (ttr *TsTableReader) cancelTsFlow() {
	ttr.mu.Lock()
	defer ttr.mu.Unlock()
	if ttr.tsHandle != nil {
		var tsInfo tse.TsQueryInfo
		tsInfo.Handle = ttr.tsHandle
		tsInfo.Buf = []byte("cancel tsflow")
		err := ttr.FlowCtx.Cfg.TsEngine.CancelTsFlow(&(ttr.Ctx), tsInfo)
		if err != nil {
			log.Warning(ttr.Ctx, err)
		}
	}
	ttr.safeCloseWaitChan()
}

// IsShortCircuitForPgEncode is part of the processor interface.
func (ttr *TsTableReader) IsShortCircuitForPgEncode() bool {
	return false
}

// setupTsFlow initializes the time series flow in the TS engine
func (ttr *TsTableReader) setupTsFlow(ctx context.Context) error {
	rand.Seed(timeutil.Now().UnixNano())
	randomNumber := rand.Intn(100000) + 1

	flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}

	tsFlowSpec := NewTSFlowSpec(flowID, ttr.FlowCtx.NodeID)
	tsFlowSpec.Processors = ttr.tsProcessorSpecs
	tsFlowSpec.BrpcAddrs = ttr.tsInfo.BrpcAddrs
	tsFlowSpec.QueryID = ttr.tsInfo.QueryID
	tsFlowSpec.Processors[len(tsFlowSpec.Processors)-1].FinalTsProcessor = true
	tsFlowSpec.IsDist = ttr.tsInfo.IsDist
	tsFlowSpec.UseQueryShortCircuit = ttr.tsInfo.UseQueryShortCircuit
	tsFlowSpec.UseCompressType = ttr.tsInfo.UseCompressType

	// get output type oid.
	finalProcessor := ttr.tsProcessorSpecs[len(tsFlowSpec.Processors)-1]
	OutputTypeOid := make([]uint32, len(finalProcessor.Post.OutputTypes))
	for i, typ := range finalProcessor.Post.OutputTypes {
		OutputTypeOid[i] = uint32(typ.Oid())
	}
	tsFlowSpec.OutputTypeOid = OutputTypeOid

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

	// Create query info for the TS engine
	tsQueryInfo := tse.TsQueryInfo{
		ID:           int(ttr.sid),
		Buf:          msg,
		UniqueID:     randomNumber,
		Handle:       ttr.tsHandle, // Will be set by the engine
		TimeZone:     timezone,
		TimeZoneName: ttr.timeZoneName,
		UseDST:       ttr.useDST,
		SQL:          ttr.EvalCtx.Planner.GetStmt(),
	}

	respInfo, err := ttr.FlowCtx.Cfg.TsEngine.SetupTsFlow(&(ttr.Ctx), tsQueryInfo)
	if err != nil {
		if ttr.FlowCtx != nil {
			ttr.FlowCtx.TsHandleBreak = true
		}
		return err
	}

	// Store the handle and register it in the flow context
	ttr.tsHandle = respInfo.Handle

	return nil
}

// setupTimezone determines the timezone offset and DST flag to use for the query
func (ttr *TsTableReader) setupTimezone(ctx context.Context) (int, error) {
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

	// Check if timezone observes DST by comparing winter and summer offsets
	// Only store timezone name when DST is supported (for DST calculation)
	ttr.useDST = ttr.checkDSTSupport(loc)
	if ttr.useDST {
		ttr.timeZoneName = loc.String()
	}

	return timezone, nil
}

// checkDSTSupport checks if a timezone observes Daylight Saving Time
func (ttr *TsTableReader) checkDSTSupport(loc *time.Location) bool {
	locName := loc.String()
	// Fast path for high-frequency timezones (UTC, GMT, China, fixed offset)
	switch locName {
	case "UTC", "GMT", "Asia/Shanghai", "Asia/Chongqing", "PRC":
		return false
	}
	if strings.HasPrefix(locName, "fixed offset:") {
		return false
	}

	// Comprehensive DST detection for all other timezones
	// Sample 4 points across the year to cover all DST patterns:
	// - Northern hemisphere: DST typically Mar-Nov
	// - Southern hemisphere: DST typically Oct-Apr
	// - Special cases: Iran (Mar-Sep), Morocco (variable)
	var samplePoints = [4]int64{
		1736942400, // 2025-01-15 12:00:00 UTC
		1744718400, // 2025-04-15 12:00:00 UTC
		1752667200, // 2025-07-15 12:00:00 UTC
		1760529600, // 2025-10-15 12:00:00 UTC
	}
	_, baseOffset := timeutil.Unix(samplePoints[0], 0).In(loc).Zone()
	for i := 1; i < len(samplePoints); i++ {
		_, compareOffset := timeutil.Unix(samplePoints[i], 0).In(loc).Zone()
		if compareOffset != baseOffset {
			return true
		}
	}

	return false
}

// parseRowFromBuffer parses a row from the current buffer
func (ttr *TsTableReader) parseRowFromBuffer() (
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
func (ttr *TsTableReader) handleEndOfData() *execinfrapb.ProducerMetadata {
	// If collecting statistics, return drain helper metadata
	if ttr.collected {
		ttr.MoveToDraining(nil)
		return ttr.DrainHelper()
	}

	return nil
}

// handleFetchError handles error responses from the TS engine
func (ttr *TsTableReader) handleFetchError(
	respInfo *tse.TsQueryInfo, err error,
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

// initStatsCollector initializes the statistics collector
func (ttr *TsTableReader) initStatsCollector(queryInfo *tse.TsQueryInfo) {
	tsFetchers := tse.NewTsFetcher(ttr.tsProcessorSpecs)
	queryInfo.Fetcher.CFetchers = tsFetchers
	queryInfo.Fetcher.Size = len(tsFetchers)

	if len(ttr.statsList) <= 0 {
		ttr.initStatsList()
	}
}

// initStatsList initializes the statistics list for all processors
func (ttr *TsTableReader) initStatsList() {
	ttr.statsList = make([]tse.TsFetcherStats, 0, len(ttr.tsProcessorSpecs))

	for j := len(ttr.tsProcessorSpecs) - 1; j >= 0; j-- {
		procSpec := &ttr.tsProcessorSpecs[j]
		ttr.statsList = append(ttr.statsList, tse.TsFetcherStats{
			ProcessorID:   procSpec.ProcessorID,
			ProcessorName: tse.TsGetNameValue(&procSpec.Core),
		})
	}
}

// updateStatsList updates the statistics list with data from response
func (ttr *TsTableReader) updateStatsList(respInfo *tse.TsQueryInfo) {
	if sp := opentracing.SpanFromContext(ttr.PbCtx()); sp != nil {
		ttr.statsList = tse.AddStatsList(respInfo.Fetcher, ttr.statsList)
	}
}

// InitProcessorProcedure init processor in procedure
func (ttr *TsTableReader) InitProcessorProcedure(txn *kv.Txn) {}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (ttr *TsTableReader) outputStatsToTrace() {
	var tsi TsInputStats
	for _, stats := range ttr.statsList {
		tsi.SetTsInputStats(stats)
	}

	sp := opentracing.SpanFromContext(ttr.PbCtx())
	if sp != nil {
		tracing.SetSpanStats(sp, &tsi)
	}
}

var _ execinfrapb.DistSQLSpanStats = &TsInputStats{}

// Stats implements the SpanStats interface.
func (tsi *TsInputStats) Stats() map[string]string {
	inputStatsMap := map[string]string{
		"time series": "analyse",
	}
	return inputStatsMap
}

// TsStats is stats of analyse in time series
func (tsi *TsInputStats) TsStats() map[int32]map[string]string {
	resultMap := make(map[int32]map[string]string, 0)
	for _, stats := range tsi.TsTableReaderStatss {
		resultMap[stats.PorcessorId] = stats.InputStats.Stats()
	}
	for _, stats := range tsi.TsAggregatorStatss {
		resultMap[stats.PorcessorId] = stats.InputStats.Stats()
	}
	for _, stats := range tsi.TsSorterStatss {
		resultMap[stats.PorcessorId] = stats.InputStats.Stats()
	}

	return resultMap
}

// GetSpanStatsType check type of spanStats
func (tsi *TsInputStats) GetSpanStatsType() int {
	if tsi.TsTableReaderStatss != nil || tsi.TsAggregatorStatss != nil || tsi.TsSorterStatss != nil {
		return tracing.SpanStatsTypeTime
	}
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (tsi *TsInputStats) StatsForQueryPlan() []string {
	res := make([]string, 0)
	return res
}

// TsStatsForQueryPlan implements the DistSQLSpanStats interface.
func (tsi *TsInputStats) TsStatsForQueryPlan() map[int32][]string {
	resultMap := make(map[int32][]string)
	for _, stats := range tsi.TsTableReaderStatss {
		resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], stats.InputStats.StatsForQueryPlan()...)
		if stats.PorcessorId == 1 {
			resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], fmt.Sprintf("block cache hit ratio: %f", stats.InputStats.BlockCacheHitRatio))
			resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], fmt.Sprintf("blocks<mem, last, entity>: <%d, %d, %d>", stats.InputStats.MemoryBlockCount, stats.InputStats.LastBlockCount, stats.InputStats.EntityBlockCount))
			resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], fmt.Sprintf("scan bytes<block, agg, header>: <%d, %d, %d>", stats.InputStats.BlockBytes, stats.InputStats.AggBytes, stats.InputStats.HeaderBytes))
		}
	}
	for _, stats := range tsi.TsAggregatorStatss {
		tempStats := stats.InputStats.TsStatsForQueryPlan()
		resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], tempStats[0]...)
	}
	for _, stats := range tsi.TsSorterStatss {
		resultMap[stats.PorcessorId] = append(resultMap[stats.PorcessorId], stats.InputStats.StatsForQueryPlan()...)
	}

	return resultMap
}

// SetTsInputStats set value to TsInputStats.
func (tsi *TsInputStats) SetTsInputStats(stats tse.TsFetcherStats) {
	is := InputStats{
		NumRows:   stats.RowNum,
		StallTime: time.Duration(stats.StallTime),
		BuildTime: time.Duration(stats.BuildTime),
	}
	switch stats.ProcessorName {
	case tse.TsTableReaderName, tse.TsTagReaderName, tse.TsStatisticReaderName:
		ts := TsTableReaderStats{
			InputStats: TableReaderStats{
				InputStats:         is,
				BytesRead:          stats.BytesRead,
				MemoryBlockCount:   stats.MemoryBlockCount,
				LastBlockCount:     stats.LastBlockCount,
				EntityBlockCount:   stats.EntityBlockCount,
				BlockCacheHitRatio: stats.BlockCacheHitRatio,
				BlockBytes:         stats.BlockBytes,
				AggBytes:           stats.AggBytes,
				HeaderBytes:        stats.HeaderBytes,
			},
			PorcessorId: stats.ProcessorID,
		}
		tsi.TsTableReaderStatss = append(tsi.TsTableReaderStatss, ts)
	case tse.TsAggregatorName, tse.TsDistinctName, tse.TsNoopName:
		ts := TsAggregatorStats{
			InputStats: AggregatorStats{
				InputStats:      is,
				MaxAllocatedMem: stats.MaxAllocatedMem,
				OutputRowNum:    stats.OutputRowNum,
			},
			PorcessorId: stats.ProcessorID,
		}
		tsi.TsAggregatorStatss = append(tsi.TsAggregatorStatss, ts)
	case tse.TsSorterName:
		ts := TsSorterStats{
			InputStats: SorterStats{
				InputStats:       is,
				MaxAllocatedMem:  stats.MaxAllocatedMem,
				MaxAllocatedDisk: stats.MaxAllocatedDisk,
			},
			PorcessorId: stats.ProcessorID,
		}
		tsi.TsSorterStatss = append(tsi.TsSorterStatss, ts)
	default:
		return
	}
}
