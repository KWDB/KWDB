// Copyright 2024 The KWDB Authors.
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

package rowexec

import (
	"context"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowLookupJoinerProcessor is the Arrow-surface execution-stage processor for
// lookup joins (joinReader). The lookup join performs KV point lookups against
// the right table, which is not a stream of rows and therefore is not
// re-implemented as a native Arrow kernel. Instead this processor embeds the
// row-based joinReader, which drives the KV lookups and emits the matched
// left++right rows, and bridges that row stream into a single Arrow Record via
// NewRowSourceToArrow. The record is then decoded back into EncDatumRows and fed
// through the standard ProcOutputHelper, so the operator still lives in the
// single Arrow DAG (per the unification design, rowexec is the operator-level
// bridge backend for compute that cannot be Arrow-ized).
//
// See docs/arrow-unify-roadmap.md §6.11.2 (ArrowLookupJoiner).
type arrowLookupJoinerProcessor struct {
	execinfra.ProcessorBase

	input      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	rs         UnifiedProcessor
	jr         execinfra.RowSource
	instTypes  []types.T
	outputRows sqlbase.EncDatumRows
	rowIdx     int
}

var _ execinfra.RowSource = &arrowLookupJoinerProcessor{}

// arrowLookupJoinerRuns counts how many times the arrow lookup joiner processor
// has been executed. It lets tests confirm that a query actually went through
// the Arrow lookup-join path.
var arrowLookupJoinerRuns int64

// ArrowLookupJoinerRunCount returns the number of arrow lookup joiner processors
// that have run so far in this process.
func ArrowLookupJoinerRunCount() int64 {
	return atomic.LoadInt64(&arrowLookupJoinerRuns)
}

func newArrowLookupJoinerProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	// Mirror the row-based JoinReader path exactly: the embedded joinReader
	// consumes the planner's real post-process (which also drives the OnExpr
	// column-index mapping constructed by the planner). It emits the final
	// joined+filtered+projected rows. The ArrowLookupJoiner stage then only
	// passes those rows through via an identity post over the joinReader's
	// output schema, bridging them into Arrow Records and decoding them back.
	jr, err := newJoinReader(
		flowCtx, processorID, spec, input, post, &execinfra.RowDisposer{},
	)
	if err != nil {
		return nil, err
	}
	jrSrc := jr.(execinfra.RowSource)
	joinedTypes := jrSrc.OutputTypes()
	// Identity post over the joinReader's (already post-processed) output schema.
	identityPost := execinfrapb.PostProcessSpec{Projection: true}
	identityPost.OutputColumns = make([]uint32, len(joinedTypes))
	for i := range identityPost.OutputColumns {
		identityPost.OutputColumns[i] = uint32(i)
	}
	rs := NewRowSourceToArrow(memory.NewGoAllocator(), jrSrc, &sqlbase.DatumAlloc{})

	p := &arrowLookupJoinerProcessor{
		input:    input,
		alloc:    memory.NewGoAllocator(),
		da:       &sqlbase.DatumAlloc{},
		rs:       rs,
		jr:       jrSrc,
		instTypes: joinedTypes,
	}
	if err := p.Init(
		p, &identityPost, joinedTypes, flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: nil},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowLookupJoinerProcessor) Start(ctx context.Context) context.Context {
	atomic.AddInt64(&arrowLookupJoinerRuns, 1)
	// NOTE: the planner-injected left input is owned and started by the embedded
	// joinReader (newJoinReader -> jr.Start -> input.Start). Starting it again
	// here would double-Start the upstream RowSource and corrupt its state
	// (the lookup join would then emit 0 rows). So we must NOT call
	// p.input.Start(ctx) here.
	ctx = p.StartInternal(ctx, "arrowLookupJoiner")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowLookupJoinerProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		if p.rowIdx >= len(p.outputRows) {
			p.MoveToDraining(nil)
			break
		}
		row := p.outputRows[p.rowIdx]
		p.rowIdx++
		if outRow := p.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, p.DrainHelper()
}

// ConsumerClosed implements the RowSource interface.
func (p *arrowLookupJoinerProcessor) ConsumerClosed() {
	p.InternalClose()
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowLookupJoinerProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.ProcessorBase.EvalCtx != nil && p.ProcessorBase.EvalCtx.IsProcedure && p.ProcessorBase.FlowCtx != nil {
		p.ProcessorBase.FlowCtx.Txn = txn
	}
}

// compute bridges the embedded row-based joinReader output into an Arrow Record,
// decodes that record back into EncDatumRows, and buffers them for Next.
func (p *arrowLookupJoinerProcessor) compute(ctx context.Context) error {
	// The embedded joinReader must be started before the bridge reads from it
	// (NewRowSourceToArrow.Init is a no-op and does not start the upstream
	// RowSource).
	p.jr.Start(ctx)
	rec, _, err := p.rs.Next(ctx)
	if err != nil {
		return err
	}
	if rec == nil {
		return nil
	}
	// instTypes is the full joined schema (left++right), matching the record
	// produced by the bridge from the joinReader's pass-through output.
	rows, err := arrowRecordToEncDatumRows(p.instTypes, rec)
	if err != nil {
		return err
	}
	p.outputRows = rows
	return nil
}
