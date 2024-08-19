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
	"time"
	"unsafe"

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

// TsTableReader is used to retrieve data from ts storage.
type TsTableReader struct {
	execinfra.ProcessorBase

	tsHandle unsafe.Pointer
	Rev      []byte
	// The output streamID of the topmost operator in the temporal flow
	sid              execinfrapb.StreamID
	tsProcessorSpecs []execinfrapb.TSProcessorSpec
	timeZone         int
	collected        bool
	statsList        []tse.TsFetcherStats
	fetMu            syncutil.Mutex
}

var _ execinfra.Processor = &TsTableReader{}
var _ execinfra.RowSource = &TsTableReader{}

const tsTableReaderProcName = "ts table reader"

// NewTsTableReader creates a TsTableReader.
func NewTsTableReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	types []types.T,
	output execinfra.RowReceiver,
	sid execinfrapb.StreamID,
	tsProcessorSpecs []execinfrapb.TSProcessorSpec,
) (*TsTableReader, error) {
	tsi := &TsTableReader{sid: sid, tsProcessorSpecs: tsProcessorSpecs, tsHandle: nil}
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		tsi.collected = true
		tsi.FinishTrace = tsi.outputStatsToTrace
	}
	if err := tsi.Init(
		tsi,
		&execinfrapb.PostProcessSpec{},
		types,
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
	return tsi, nil
}

var kwdbFlowSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.TSFlowSpec{}
	},
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
	ttr.StartInternal(ctx, tsTableReaderProcName)

	var tsProcessorSpecs = ttr.tsProcessorSpecs
	var randomNumber int
	if tsProcessorSpecs != nil {
		rand.Seed(timeutil.Now().UnixNano())
		randomNumber = rand.Intn(100000) + 1
		flowID := execinfrapb.FlowID{}
		flowID.UUID = uuid.MakeV4()
		// ts processor output StreamID map
		outPutMap := make(map[execinfrapb.StreamID]int)

		// The timing operator has only one input and one output,
		// and each input and output has only one stream.
		for i, proc := range tsProcessorSpecs {
			if proc.Output != nil {
				outPutMap[proc.Output[0].Streams[0].StreamID] = i
			}

		}

		// The set of operators on the ts flow.
		var tsSpecs []execinfrapb.TSProcessorSpec

		tsTopProcessorIndex := outPutMap[ttr.sid]
		tsSpecs = append(tsSpecs, tsProcessorSpecs[tsTopProcessorIndex])
		for tsProcessorSpecs[tsTopProcessorIndex].Input != nil {
			streamID := tsProcessorSpecs[tsTopProcessorIndex].Input[0].Streams[0].StreamID
			tsTopProcessorIndex = outPutMap[streamID]
			tsSpecs = append(tsSpecs, tsProcessorSpecs[tsTopProcessorIndex])
		}

		tsFlowSpec := NewTSFlowSpec(flowID, ttr.FlowCtx.NodeID)
		for j := len(tsSpecs) - 1; j >= 0; j-- {
			tsFlowSpec.Processors = append(tsFlowSpec.Processors, tsSpecs[j])
		}
		msg, err := protoutil.Marshal(tsFlowSpec)
		if err != nil {
			ttr.MoveToDraining(err)
			return ctx
		}

		loc, err := timeutil.TimeZoneStringToLocation(ttr.EvalCtx.GetLocation().String(), timeutil.TimeZoneStringToLocationISO8601Standard)
		if err != nil {
			ttr.MoveToDraining(err)
			return ctx
		}
		currentTime := timeutil.Now()
		// Convert time to a specified time zone.
		timeInLocation := currentTime.In(loc)
		_, offSet := timeInLocation.Zone()
		timezone := offSet / 3600
		ttr.timeZone = timezone
		var tsHandle unsafe.Pointer
		var tsQueryInfo = tse.TsQueryInfo{
			ID:       int(ttr.sid),
			Buf:      msg,
			UniqueID: randomNumber,
			Handle:   tsHandle,
			TimeZone: timezone,
		}
		respInfo, setupErr := ttr.FlowCtx.Cfg.TsEngine.SetupTsFlow(&(ttr.Ctx), tsQueryInfo)
		if setupErr != nil {
			var tsCloseInfo tse.TsQueryInfo
			tsCloseInfo.Handle = respInfo.Handle
			tsCloseInfo.Buf = []byte("close tsflow")
			closeErr := ttr.FlowCtx.Cfg.TsEngine.CloseTsFlow(&(ttr.Ctx), tsCloseInfo)
			if closeErr != nil {
				log.Warning(ctx, closeErr)
			}
			ttr.MoveToDraining(setupErr)
			return ctx
		}
		ttr.tsHandle = respInfo.Handle
	}
	ctx = ttr.StartInternal(ctx, sqlbase.TsTableReaderProcName)
	return ctx
}

func (ttr *TsTableReader) cleanup(ctx context.Context) {
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
}

// name of processor in time series
const (
	tsUnknownName int8 = iota
	tsTableReaderName
	tsAggregatorName
	tsNoopName
	tsSorterName
	tsStatisticReaderName
	tsSynchronizerName
	tsSamplerName
	tsTagReaderName
	tsDistinctName
)

// tsGetNameValue get name of tsProcessor.
func tsGetNameValue(this *execinfrapb.TSProcessorCoreUnion) int8 {
	if this.TableReader != nil {
		return tsTableReaderName
	}
	if this.Aggregator != nil {
		return tsAggregatorName
	}
	if this.Noop != nil {
		return tsNoopName
	}
	if this.Sorter != nil {
		return tsSorterName
	}
	if this.StatisticReader != nil {
		return tsStatisticReaderName
	}
	if this.Synchronizer != nil {
		return tsSynchronizerName
	}
	if this.Sampler != nil {
		return tsSamplerName
	}
	if this.TagReader != nil {
		return tsTagReaderName
	}
	if this.Distinct != nil {
		return tsDistinctName
	}
	return tsUnknownName
}

// Next is part of the RowSource interface.
func (ttr *TsTableReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ttr.State == execinfra.StateRunning {
		if len(ttr.Rev) == 0 {
			var tsQueryInfo = tse.TsQueryInfo{
				ID:      int(ttr.sid),
				Handle:  ttr.tsHandle,
				Buf:     []byte("exec next"),
				Fetcher: tse.TsFetcher{Collected: ttr.collected},
			}
			// Init analyse fetcher.
			if ttr.collected {
				tsFetchers := tse.NewTsFetcher(ttr.tsProcessorSpecs)
				tsQueryInfo.Fetcher.CFetchers = tsFetchers
				tsQueryInfo.Fetcher.Size = len(tsFetchers)
				tsQueryInfo.Fetcher.Mu = &ttr.fetMu
				if ttr.statsList == nil || len(ttr.statsList) <= 0 {
					for j := len(ttr.tsProcessorSpecs) - 1; j >= 0; j-- {
						rowNumFetcherStats := tse.TsFetcherStats{ProcessorID: ttr.tsProcessorSpecs[j].ProcessorID, ProcessorName: tsGetNameValue(&ttr.tsProcessorSpecs[j].Core)}
						ttr.statsList = append(ttr.statsList, rowNumFetcherStats)
					}
				}
			}

			respInfo, err := ttr.FlowCtx.Cfg.TsEngine.NextTsFlow(&(ttr.Ctx), tsQueryInfo)
			if ttr.collected {
				if sp := opentracing.SpanFromContext(ttr.PbCtx()); sp != nil {
					ttr.statsList = tse.AddStatsList(respInfo.Fetcher, ttr.statsList)
				}
			}
			if respInfo.Code == -1 {
				// Data read completed.
				ttr.cleanup(ttr.PbCtx())
				if ttr.collected {
					ttr.MoveToDraining(nil)
					return nil, ttr.DrainHelper()
				}
				return nil, nil
			} else if respInfo.Code != 1 {
				if err == nil {
					err = errors.Newf("There is no error message for this error code. The err code is %d.\n", respInfo.Code)
				}
				ttr.MoveToDraining(err)
				log.Errorf(context.Background(), err.Error())
				ttr.cleanup(ttr.PbCtx())
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
			ttr.Rev = respInfo.Buf
		}

		row := make([]sqlbase.EncDatum, len(ttr.Out.OutputTypes))
		for i := range row {
			var err error
			row[i], ttr.Rev, err = sqlbase.EncDatumFromBuffer(nil, sqlbase.DatumEncoding_VALUE, ttr.Rev)
			if err != nil {
				log.Errorf(context.Background(), err.Error())
				ttr.cleanup(ttr.PbCtx())
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
		}
		return row, nil
	}
	return nil, ttr.DrainHelper()
}

// NextPgWire get data for short circuit go pg encoding.
func (ttr *TsTableReader) NextPgWire() (val []byte, code int, err error) {
	for ttr.State == execinfra.StateRunning {
		var tsQueryInfo = tse.TsQueryInfo{
			ID:       int(ttr.sid),
			Handle:   ttr.tsHandle,
			Buf:      []byte("exec next"),
			TimeZone: ttr.timeZone,
		}
		respInfo, err := ttr.FlowCtx.Cfg.TsEngine.NextTsFlowPgWire(&(ttr.Ctx), tsQueryInfo)
		if respInfo.Code == -1 {
			// Data read completed.
			ttr.cleanup(context.Background())
			return nil, respInfo.Code, nil
		} else if respInfo.Code != 1 {
			log.Errorf(context.Background(), err.Error())
			ttr.cleanup(context.Background())
			return nil, respInfo.Code, err
		}
		return respInfo.Buf, respInfo.Code, nil
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
	ttr.cleanup(ttr.PbCtx())
	ttr.InternalClose()
}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (ttr *TsTableReader) outputStatsToTrace() {
	var tsi TsInputStats
	for _, stats := range ttr.statsList {
		tsi.setTsInputStats(stats)
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

// setTsInputStats set value to TsInputStats.
func (tsi *TsInputStats) setTsInputStats(stats tse.TsFetcherStats) {
	is := InputStats{
		NumRows:   stats.RowNum,
		StallTime: time.Duration(stats.StallTime),
	}
	switch stats.ProcessorName {
	case tsTableReaderName, tsTagReaderName, tsStatisticReaderName:
		ts := TsTableReaderStats{
			InputStats: TableReaderStats{
				InputStats: is,
				BytesRead:  stats.BytesRead,
			},
			PorcessorId: stats.ProcessorID,
		}
		tsi.TsTableReaderStatss = append(tsi.TsTableReaderStatss, ts)
	case tsAggregatorName, tsDistinctName:
		ts := TsAggregatorStats{
			InputStats: AggregatorStats{
				InputStats:      is,
				MaxAllocatedMem: stats.MaxAllocatedMem,
				OutputRowNum:    stats.OutputRowNum,
			},
			PorcessorId: stats.ProcessorID,
		}
		tsi.TsAggregatorStatss = append(tsi.TsAggregatorStatss, ts)
	case tsSorterName:
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