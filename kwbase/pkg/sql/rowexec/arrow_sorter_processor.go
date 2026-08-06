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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowexec

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// arrowSortPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowSorter.Expr. It mirrors the planner-side struct in
// pkg/sql/arrow_unification.go.
type arrowSortPlan struct {
	Ordering []arrowSortCol `json:"ordering"`
	MatchLen int            `json:"match_len"`
	Limit    int64          `json:"limit"`
	Offset   int64          `json:"offset"`
}

type arrowSortCol struct {
	Col int  `json:"col"`
	Asc bool `json:"asc"`
}

// arrowSorterRuns counts how many times the arrow sorter processor has run.
var arrowSorterRuns int64

// ArrowSorterRunCount returns the number of arrow sorter processors that have
// run so far in this process.
func ArrowSorterRunCount() int64 {
	return atomic.LoadInt64(&arrowSorterRuns)
}

// arrowSorterProcessor orders its single input over Arrow records. The input
// rows are bridged into an Arrow Record (so operator-to-operator arrow handoff
// works), decoded back into EncDatumRows, then sorted using the standard
// EncDatumRow.Compare ordering semantics. A future optimization may replace the
// scalar sort with the arrow compute sort kernel, but the processor's contract
// (input/output types, post-processing) is unchanged.
type arrowSorterProcessor struct {
	execinfra.ProcessorBase

	input    execinfra.RowSource
	alloc    memory.Allocator
	da       *sqlbase.DatumAlloc
	plan     arrowSortPlan
	ordering sqlbase.ColumnOrdering
	// matchLen is the length of an already-sorted prefix that is skipped.
	matchLen int
	outputRows sqlbase.EncDatumRows
	outputRec   arrow.Record
	rowIdx     int
}

var _ execinfra.RowSource = &arrowSorterProcessor{}

func newArrowSorterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowSortPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	inTypes := input.OutputTypes()
	ordering := make(sqlbase.ColumnOrdering, len(plan.Ordering))
	for i, c := range plan.Ordering {
		dir := encoding.Ascending
		if !c.Asc {
			dir = encoding.Descending
		}
		ordering[i] = sqlbase.ColumnOrderInfo{ColIdx: c.Col, Direction: dir}
		if c.Col >= len(inTypes) {
			return nil, fmt.Errorf("arrow sorter: order column %d out of range (ncols=%d)", c.Col, len(inTypes))
		}
	}

	matchLen := plan.MatchLen
	if matchLen > len(ordering) {
		matchLen = len(ordering)
	}
	p := &arrowSorterProcessor{
		input:    input,
		alloc:    memory.NewGoAllocator(),
		da:       &sqlbase.DatumAlloc{},
		plan:     plan,
		ordering: ordering,
		matchLen: matchLen,
	}
	if err := p.Init(
		p, post, inTypes, flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{p.input}},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowSorterProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowSorter")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowSorterProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		if p.rowIdx >= len(p.outputRows) {
			p.MoveToDraining(nil)
			break
		}
		row := p.outputRows[p.rowIdx]
		p.rowIdx++
		return row, nil
	}
	return nil, p.DrainHelper()
}

// ConsumerClosed implements the RowSource interface.
func (p *arrowSorterProcessor) ConsumerClosed() {
	p.ProcessorBase.InternalClose()
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowSorterProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.input.OutputTypes()
}

func (p *arrowSorterProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowSorterRuns, 1)

	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}
	conv.Init(ctx)
	rec, done, err := conv.Next(ctx)
	if err != nil {
		return err
	}
	if done || rec == nil {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	inTypes := p.input.OutputTypes()
	// Bridge back to EncDatumRows so we can sort with KWDB ordering semantics.
	rows, err := arrowRecordToEncDatumRows(inTypes, rec)
	if err != nil {
		return err
	}
	rec.Release()

	if p.matchLen < len(p.ordering) {
		sort.SliceStable(rows, func(i, j int) bool {
			cmp, err := rows[i].Compare(inTypes, p.da, p.ordering, p.EvalCtx, rows[j])
			if err != nil {
				// sort.SliceStable's less func cannot return an error; surface
				// it via panic and recover at the call site is overkill — instead
				// fall back to a stable no-op comparison error path.
				panic(err)
			}
			return cmp < 0
		})
	}

	// Apply the top-N early-stop optimization: when the planner has pushed a
	// limit into the plan (localLimit = count+offset), keep only the first
	// limit rows. The offset (and the final count) is applied downstream by the
	// PostProcess, so we intentionally only slice by Limit here and let
	// PostProcess handle the precise offset/limit semantics.
	if p.plan.Limit >= 0 && int64(len(rows)) > p.plan.Limit {
		rows = rows[:p.plan.Limit]
	}

	out := make(sqlbase.EncDatumRows, 0, len(rows))
	for _, row := range rows {
		processed, _, err := p.Out.ProcessRow(ctx, row)
		if err != nil {
			return err
		}
		if processed != nil {
			cp := make(sqlbase.EncDatumRow, len(processed))
			copy(cp, processed)
			out = append(out, cp)
		}
	}
	p.outputRows = out
	// Build the output Arrow Record so a downstream colexec operator can consume
	// it directly via the Arrow->colexec zero-copy bridge (RecordToBatch),
	// skipping the row round-trip used by the classic Next() path.
	outTypes := p.OutputTypes()
	ptrTypes := make([]*types.T, len(outTypes))
	for i := range outTypes {
		t := outTypes[i]
		ptrTypes[i] = &t
	}
	cols, err := buildArrowColumns(p.alloc, ptrTypes, p.outputRows, p.da)
	if err != nil {
		return err
	}
	p.outputRec = array.NewRecord(buildArrowSchema(ptrTypes), cols, int64(len(p.outputRows)))
	return nil
}

// ArrowOutput implements the ArrowRecordEmitter contract, exposing the sorted
// output Record for a downstream Arrow or colexec operator to consume without a
// row round-trip.
func (p *arrowSorterProcessor) ArrowOutput() arrow.Record {
	return p.outputRec
}
