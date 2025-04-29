// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"math/rand"
	"sync"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
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

// tsReaderOp is an operator that executes ts flow.
type tsReaderOp struct {
	execinfra.ProcessorBase
	internalBatch coldata.Batch
	NonExplainable
	tsHandle unsafe.Pointer
	// The output streamID of the topmost operator in the temporal flow
	sid              execinfrapb.StreamID
	tsProcessorSpecs []execinfrapb.TSProcessorSpec
	timeZone         int
	collected        bool
	statsList        []tse.TsFetcherStats
	fetMu            syncutil.Mutex
	done             bool
	FlowCtx          *execinfra.FlowCtx
	// EvalCtx is used for expression evaluation. It overrides the one in flowCtx.
	EvalCtx *tree.EvalContext
	// Ctx and span contain the tracing state while the processor is active
	// (i.e. hasn't been closed). Initialized using flowCtx.Ctx (which should not be otherwise
	// used).
	// Suggest call PbCtx().
	Ctx   context.Context
	types []types.T
	// Rev   []byte
	Rcv tse.TsDataChunkToGo
}

var _ Operator = &tsReaderOp{}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (tro *tsReaderOp) outputStatsToTrace() {
	var tsi rowexec.TsInputStats
	for _, stats := range tro.statsList {
		tsi.SetTsInputStats(stats)
	}

	sp := opentracing.SpanFromContext(tro.PbCtx())
	if sp != nil {
		tracing.SetSpanStats(sp, &tsi)
	}
}

// NewTsReaderOp returns a new tsReader operator that executes ts flow.
func NewTsReaderOp(
	ctx context.Context,
	allocator *Allocator,
	flowCtx *execinfra.FlowCtx,
	types []types.T,
	sid execinfrapb.StreamID,
	tsProcessorSpecs []execinfrapb.TSProcessorSpec,
) Operator {
	tro := &tsReaderOp{sid: sid, tsProcessorSpecs: tsProcessorSpecs, tsHandle: nil,
		FlowCtx: flowCtx, EvalCtx: flowCtx.NewEvalCtx(), Ctx: ctx, types: types,
	}
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		tro.collected = true
		tro.FinishTrace = tro.outputStatsToTrace
	}
	typs, err := typeconv.FromColumnTypes(types)
	if err != nil {
		return nil
	}
	tro.FlowCtx = flowCtx
	tro.ProcessorID = -1
	tro.internalBatch = allocator.NewMemBatch(typs)
	return tro
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

func (tro *tsReaderOp) Init() {
	tro.done = false
	var tsProcessorSpecs = tro.tsProcessorSpecs
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

		tsTopProcessorIndex := outPutMap[tro.sid]
		tsSpecs = append(tsSpecs, tsProcessorSpecs[tsTopProcessorIndex])
		for tsProcessorSpecs[tsTopProcessorIndex].Input != nil {
			streamID := tsProcessorSpecs[tsTopProcessorIndex].Input[0].Streams[0].StreamID
			tsTopProcessorIndex = outPutMap[streamID]
			tsSpecs = append(tsSpecs, tsProcessorSpecs[tsTopProcessorIndex])
		}
		tsFlowSpec := NewTSFlowSpec(flowID, tro.FlowCtx.NodeID)
		for j := len(tsSpecs) - 1; j >= 0; j-- {
			tsFlowSpec.Processors = append(tsFlowSpec.Processors, tsSpecs[j])
		}
		msg, err := protoutil.Marshal(tsFlowSpec)
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}

		loc, err := timeutil.TimeZoneStringToLocation(tro.EvalCtx.GetLocation().String(), timeutil.TimeZoneStringToLocationISO8601Standard)
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		currentTime := timeutil.Now()
		// Convert time to a specified time zone.
		timeInLocation := currentTime.In(loc)
		_, offSet := timeInLocation.Zone()
		timezone := offSet / 3600
		tro.timeZone = timezone
		var tsHandle unsafe.Pointer
		var tsQueryInfo = tse.TsQueryInfo{
			ID:       int(tro.sid),
			Buf:      msg,
			UniqueID: randomNumber,
			Handle:   tsHandle,
			TimeZone: timezone,
		}
		respInfo, setupErr := tro.FlowCtx.Cfg.TsEngine.SetupTsFlow(&(tro.Ctx), tsQueryInfo)
		if setupErr != nil {
			var tsCloseInfo tse.TsQueryInfo
			tsCloseInfo.Handle = respInfo.Handle
			tsCloseInfo.Buf = []byte("close tsflow")
			closeErr := tro.FlowCtx.Cfg.TsEngine.CloseTsFlow(&(tro.Ctx), tsCloseInfo)
			if closeErr != nil {
				// log.Warning(tro, closeErr)
			}
			execerror.VectorizedInternalPanic(setupErr)
		}
		tro.tsHandle = respInfo.Handle
	}

	tro.Rcv.NeedType = make([]coltypes.T, tro.internalBatch.Width())
	for i := 0; i < tro.internalBatch.Width(); i++ {
		tro.Rcv.NeedType[i] = tro.internalBatch.ColVec(i).Type()
	}
}

func (tro *tsReaderOp) Next(ctx context.Context) coldata.Batch {
	tro.internalBatch.ResetInternalBatch()
	if tro.done {
		return coldata.ZeroBatch
	}

	rowIdx := 0

	for {
		if tro.Rcv.IsReadComplete() {
			var tsQueryInfo = tse.TsQueryInfo{
				ID:      int(tro.sid),
				Handle:  tro.tsHandle,
				Buf:     []byte("exec next"),
				Fetcher: tse.TsFetcher{Collected: tro.collected},
			}
			// Init analyse fetcher.
			if tro.collected {
				tsFetchers := tse.NewTsFetcher(tro.tsProcessorSpecs)
				tsQueryInfo.Fetcher.CFetchers = tsFetchers
				tsQueryInfo.Fetcher.Size = len(tsFetchers)
				tsQueryInfo.Fetcher.Mu = &tro.fetMu
				if tro.statsList == nil || len(tro.statsList) <= 0 {
					for j := len(tro.tsProcessorSpecs) - 1; j >= 0; j-- {
						rowNumFetcherStats := tse.TsFetcherStats{ProcessorID: tro.tsProcessorSpecs[j].ProcessorID, ProcessorName: tse.TsGetNameValue(&tro.tsProcessorSpecs[j].Core)}
						tro.statsList = append(tro.statsList, rowNumFetcherStats)
					}
				}
			}

			respInfo, err := tro.FlowCtx.Cfg.TsEngine.NextVectorizedTsFlow(&(tro.Ctx), tsQueryInfo, &tro.Rcv)
			if tro.collected {
				if sp := opentracing.SpanFromContext(ctx); sp != nil {
					tro.statsList = tse.AddStatsList(respInfo.Fetcher, tro.statsList)
				}
			}
			if respInfo.Code == -1 {
				// Data read completed.
				if rowIdx == 0 {
					tro.cleanup(ctx)
					if tro.collected {
						tro.MoveToDraining(nil)
					}
					return coldata.ZeroBatch
				}
				if tro.collected {
					tro.MoveToDraining(nil)
					meta := tro.DrainHelper()
					if meta != nil && meta.Err != nil {
						return coldata.ZeroBatch
					}
				}
				tro.done = true
				tro.internalBatch.SetLength(rowIdx)
				return tro.internalBatch
			} else if respInfo.Code != 1 {
				if err == nil {
					err = errors.Newf("There is no error message for this error code. The err code is %d.\n", respInfo.Code)
				}
				log.Errorf(context.Background(), err.Error())
				tro.cleanup(ctx)
				execerror.VectorizedInternalPanic(err)
				return coldata.ZeroBatch
			}
			// tro.Rev = respInfo.Buf
			// tro.Rcv = respInfo.PullData
		}

		readRow := 0
		RemRow := coldata.BatchSize() - rowIdx
		if RemRow > (int(tro.Rcv.DataCount) - tro.Rcv.Begin) {
			readRow = int(tro.Rcv.DataCount) - tro.Rcv.Begin
		} else {
			readRow = RemRow
		}

		for i := 0; i < len(tro.types); i++ {
			vec := tro.internalBatch.ColVecs()[i]
			var args coldata.SliceArgs
			args.ColType = vec.Type()
			args.Src = tro.Rcv.Data[i]
			args.DestIdx = rowIdx
			args.SrcStartIdx = tro.Rcv.Begin
			args.SrcEndIdx = int(tro.Rcv.Begin) + readRow
			vec.Append(args)
		}

		tro.Rcv.Begin += readRow
		rowIdx += readRow

		if rowIdx >= coldata.BatchSize() {
			tro.internalBatch.SetLength(rowIdx)
			return tro.internalBatch
		}
	}
}

// NextPgWire get data from AE by short citcuit.
func (tro *tsReaderOp) NextPgWire() (val []byte, code int, err error) {
	var tsQueryInfo = tse.TsQueryInfo{
		ID:       int(tro.sid),
		Handle:   tro.tsHandle,
		Buf:      []byte("exec next"),
		TimeZone: tro.timeZone,
		Fetcher:  tse.TsFetcher{Collected: tro.collected},
	}
	// Init analyse fetcher.
	if tro.collected {
		tsFetchers := tse.NewTsFetcher(tro.tsProcessorSpecs)
		tsQueryInfo.Fetcher.CFetchers = tsFetchers
		tsQueryInfo.Fetcher.Size = len(tsFetchers)
		tsQueryInfo.Fetcher.Mu = &tro.fetMu
		if tro.statsList == nil || len(tro.statsList) <= 0 {
			for j := len(tro.tsProcessorSpecs) - 1; j >= 0; j-- {
				rowNumFetcherStats := tse.TsFetcherStats{ProcessorID: tro.tsProcessorSpecs[j].ProcessorID, ProcessorName: tse.TsGetNameValue(&tro.tsProcessorSpecs[j].Core)}
				tro.statsList = append(tro.statsList, rowNumFetcherStats)
			}
		}
	}
	respInfo, err := tro.FlowCtx.Cfg.TsEngine.NextTsFlowPgWire(&(tro.Ctx), tsQueryInfo)
	if tro.collected {
		if sp := opentracing.SpanFromContext(tro.Ctx); sp != nil {
			tro.statsList = tse.AddStatsList(respInfo.Fetcher, tro.statsList)
		}
	}
	if respInfo.Code == -1 {
		// Data read completed.
		tro.cleanup(context.Background())
		return nil, respInfo.Code, nil
	} else if respInfo.Code != 1 {
		if err != nil && tro.FlowCtx != nil {
			tro.FlowCtx.TsHandleBreak = true
		}
		log.Errorf(context.Background(), err.Error())
		tro.cleanup(context.Background())
		return nil, respInfo.Code, err
	}
	return respInfo.Buf, respInfo.Code, nil
}

func (tro *tsReaderOp) ChildCount(verbose bool) int {
	return 0
}

func (tro *tsReaderOp) cleanup(ctx context.Context) {
	if tro.tsHandle != nil {
		var tsCloseInfo tse.TsQueryInfo
		tsCloseInfo.Handle = tro.tsHandle
		tsCloseInfo.Buf = []byte("close tsflow")
		closeErr := tro.FlowCtx.Cfg.TsEngine.CloseTsFlow(&(tro.Ctx), tsCloseInfo)
		if closeErr != nil {
			log.Warning(ctx, closeErr)
		}
		tro.tsHandle = nil
		tro.done = true
	}
}

func (tro *tsReaderOp) Child(nth int, verbose bool) execinfra.OpNode {
	return nil
}
