// Copyright 2018 The Cockroach Authors.
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

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/scrub"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

const indexJoinerBatchSize = 10000

// indexJoiner performs a join between a secondary index, the `input`, and the
// primary index of the same table, `desc`, to retrieve columns which are not
// stored in the secondary index.
type indexJoiner struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	desc  sqlbase.TableDescriptor

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// indexJoiner to wrap the fetcher with a stat collector when necessary.
	fetcher rowFetcher
	// fetcherReady indicates that we have started an index scan and there are
	// potentially more rows to retrieve.
	fetcherReady bool
	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// spans is the batch of spans we will next retrieve from the index.
	spans roachpb.Spans

	alloc sqlbase.DatumAlloc

	spanBuilder *span.Builder
}

var _ execinfra.Processor = &indexJoiner{}
var _ execinfra.RowSource = &indexJoiner{}
var _ execinfrapb.MetadataSource = &indexJoiner{}
var _ execinfra.OpNode = &indexJoiner{}

const indexJoinerProcName = "index joiner"

// newIndexJoiner returns a new indexJoiner.
func newIndexJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	if spec.IndexIdx != 0 {
		return nil, errors.Errorf("index join must be against primary index")
	}
	ij := &indexJoiner{
		input:     input,
		desc:      spec.Table,
		batchSize: indexJoinerBatchSize,
	}
	needMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	if err := ij.Init(
		ij,
		post,
		ij.desc.ColumnTypesWithMutations(needMutations),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ij.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				ij.InternalClose()
				return ij.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}
	var fetcher row.Fetcher
	if _, _, err := initRowFetcher(
		&fetcher,
		&ij.desc,
		0, /* primary index */
		ij.desc.ColumnIdxMapWithMutations(needMutations),
		false, /* reverse */
		ij.Out.NeededColumns(),
		false, /* isCheck */
		&ij.alloc,
		spec.Visibility,
		spec.LockingStrength,
	); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		// Enable stats collection.
		ij.input = newInputStatCollector(ij.input)
		ij.fetcher = newRowFetcherStatCollector(&fetcher)
		ij.FinishTrace = ij.outputStatsToTrace
	} else {
		ij.fetcher = &fetcher
	}

	ij.spanBuilder = span.MakeBuilder(&spec.Table, &spec.Table.PrimaryIndex)
	ij.spanBuilder.SetNeededColumns(ij.Out.NeededColumns())

	return ij, nil
}

// SetBatchSize sets the desired batch size. It should only be used in tests.
func (ij *indexJoiner) SetBatchSize(batchSize int) {
	ij.batchSize = batchSize
}

// InitProcessorProcedure init processor in procedure
func (ij *indexJoiner) InitProcessorProcedure(txn *kv.Txn) {
	if ij.EvalCtx.IsProcedure {
		if ij.FlowCtx != nil {
			ij.FlowCtx.Txn = txn
		}
		ij.Closed = false
		ij.State = execinfra.StateRunning
		ij.Out.SetRowIdx(0)
	}
}

// Start is part of the RowSource interface.
func (ij *indexJoiner) Start(ctx context.Context) context.Context {
	ij.input.Start(ctx)
	return ij.StartInternal(ctx, indexJoinerProcName)
}

// Next is part of the RowSource interface.
func (ij *indexJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ij.State == execinfra.StateRunning {
		if !ij.fetcherReady {
			// Retrieve a batch of rows from the input.
			for len(ij.spans) < ij.batchSize {
				row, meta := ij.input.Next()
				if meta != nil {
					if meta.Err != nil {
						ij.MoveToDraining(nil /* err */)
					}
					return nil, meta
				}
				if row == nil {
					break
				}
				spans, err := ij.generateSpans(row)
				if err != nil {
					ij.MoveToDraining(err)
					return nil, ij.DrainHelper()
				}
				ij.spans = append(ij.spans, spans...)
			}
			if len(ij.spans) == 0 {
				// All done.
				ij.MoveToDraining(nil /* err */)
				return nil, ij.DrainHelper()
			}
			// Scan the primary index for this batch.
			err := ij.fetcher.StartScan(
				ij.PbCtx(), ij.FlowCtx.Txn, ij.spans, false /* limitBatches */, 0, /* limitHint */
				ij.FlowCtx.TraceKV)
			if err != nil {
				ij.MoveToDraining(err)
				return nil, ij.DrainHelper()
			}
			ij.fetcherReady = true
			ij.spans = ij.spans[:0]
		}
		row, _, _, err := ij.fetcher.NextRow(ij.PbCtx())
		if err != nil {
			ij.MoveToDraining(scrub.UnwrapScrubError(err))
			return nil, ij.DrainHelper()
		}
		if row == nil {
			// Done with this batch.
			ij.fetcherReady = false
		} else if outRow := ij.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, ij.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ij *indexJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ij.InternalClose()
}

func (ij *indexJoiner) generateSpans(row sqlbase.EncDatumRow) (roachpb.Spans, error) {
	numKeyCols := len(ij.desc.PrimaryIndex.ColumnIDs)
	if len(row) < numKeyCols {
		return nil, errors.Errorf(
			"index join input has %d columns, expected at least %d", len(row), numKeyCols)
	}
	// There may be extra values on the row, e.g. to allow an ordered
	// synchronizer to interleave multiple input streams. Will need at most
	// numKeyCols.
	span, containsNull, err := ij.spanBuilder.SpanFromEncDatums(row, numKeyCols)
	if err != nil {
		return nil, err
	}
	return ij.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
		nil /* appendTo */, span, numKeyCols, containsNull,
	), nil
}

// outputStatsToTrace outputs the collected indexJoiner stats to the trace. Will
// fail silently if the indexJoiner is not collecting stats.
func (ij *indexJoiner) outputStatsToTrace() {
	is, ok := getInputStats(ij.FlowCtx, ij.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(ij.FlowCtx, ij.fetcher)
	if !ok {
		return
	}
	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(ij.PbCtx()); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

func (ij *indexJoiner) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if tfs := execinfra.GetLeafTxnFinalState(ctx, ij.FlowCtx.Txn); tfs != nil {
		return []execinfrapb.ProducerMetadata{{LeafTxnFinalState: tfs}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (ij *indexJoiner) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return ij.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (ij *indexJoiner) ChildCount(verbose bool) int {
	if _, ok := ij.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ij *indexJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ij.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to indexJoiner is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
