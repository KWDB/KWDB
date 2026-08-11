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
	"bytes"
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

// arrowDistinctPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowDistinct.Expr. It mirrors the planner-side struct in
// pkg/sql/arrow_unification.go.
type arrowDistinctPlan struct {
	DistinctCols []int `json:"distinct_cols"`
	OrderedCols  []int `json:"ordered_cols"`
}

// arrowDistinctRuns counts how many times the arrow distinct processor has run.
var arrowDistinctRuns int64

// ArrowDistinctRunCount returns the number of arrow distinct processors run.
func ArrowDistinctRunCount() int64 {
	return atomic.LoadInt64(&arrowDistinctRuns)
}

// arrowDistinctProcessor removes duplicate rows over Arrow records. The input
// rows are bridged into an Arrow Record, decoded back into EncDatumRows, and
// deduped with the standard MemRowContainer ordering semantics (sorted by the
// ordered-then-distinct columns, then keeping the first row of each run).
type arrowDistinctProcessor struct {
	execinfra.ProcessorBase

	input        execinfra.RowSource
	alloc        memory.Allocator
	da           *sqlbase.DatumAlloc
	distinctCols []int
	orderedCols  []int
	outputRows   sqlbase.EncDatumRows
	outputRec    arrow.Record
	rowIdx       int
}

var _ execinfra.RowSource = &arrowDistinctProcessor{}

func newArrowDistinctProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowDistinctPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	inTypes := input.OutputTypes()
	for _, c := range plan.DistinctCols {
		if c >= len(inTypes) {
			return nil, fmt.Errorf("arrow distinct: distinct column %d out of range (ncols=%d)", c, len(inTypes))
		}
	}
	for _, c := range plan.OrderedCols {
		if c >= len(inTypes) {
			return nil, fmt.Errorf("arrow distinct: ordered column %d out of range (ncols=%d)", c, len(inTypes))
		}
	}
	p := &arrowDistinctProcessor{
		input:        input,
		alloc:        memory.NewGoAllocator(),
		da:           &sqlbase.DatumAlloc{},
		distinctCols: plan.DistinctCols,
		orderedCols:  plan.OrderedCols,
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
func (p *arrowDistinctProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowDistinct")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowDistinctProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
func (p *arrowDistinctProcessor) ConsumerClosed() {
	p.ProcessorBase.InternalClose()
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowDistinctProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.input.OutputTypes()
}

// buildOrdering returns the ColumnOrdering used to sort the rows so that equal
// distinct-keys become adjacent (optionally grouped by ordered columns first).
func (p *arrowDistinctProcessor) buildOrdering() sqlbase.ColumnOrdering {
	seen := make(map[int]bool, len(p.orderedCols)+len(p.distinctCols))
	ord := make(sqlbase.ColumnOrdering, 0, len(p.orderedCols)+len(p.distinctCols))
	for _, c := range p.orderedCols {
		if seen[c] {
			continue
		}
		seen[c] = true
		ord = append(ord, sqlbase.ColumnOrderInfo{ColIdx: c, Direction: encoding.Ascending})
	}
	for _, c := range p.distinctCols {
		if seen[c] {
			continue
		}
		seen[c] = true
		ord = append(ord, sqlbase.ColumnOrderInfo{ColIdx: c, Direction: encoding.Ascending})
	}
	return ord
}

func (p *arrowDistinctProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowDistinctRuns, 1)

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
	// The Arrow upstream may emit a record whose arity differs from the
	// planner-declared output types. Derive the comparison types from the record
	// itself so EncDatumRow.Compare's arity matches the actual rows.
	if len(inTypes) != int(rec.NumCols()) {
		recTypes := make([]types.T, rec.NumCols())
		for ci := 0; ci < int(rec.NumCols()); ci++ {
			if ci < len(inTypes) {
				recTypes[ci] = inTypes[ci]
			} else {
				recTypes[ci] = arrowDataTypeToKWType(rec.Column(ci).DataType())
			}
		}
		inTypes = recTypes
	}
	rows, err := arrowRecordToEncDatumRows(inTypes, rec)
	if err != nil {
		return err
	}
	rec.Release()

	// Sort so that distinct-keys become adjacent (grouped by ordered columns).
	ord := p.buildOrdering()
	if len(ord) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			cmp, err := rows[i].Compare(inTypes, p.da, ord, p.EvalCtx, rows[j])
			if err != nil {
				panic(err)
			}
			return cmp < 0
		})
	}

	out := make(sqlbase.EncDatumRows, 0, len(rows))
	// lastKey tracks the distinct-key of the previously emitted row, grouped by
	// the current ordered-columns run.
	var lastKey, lastGroup []byte
	first := true
	for _, row := range rows {
		group, err := encodeCols(row, p.orderedCols, inTypes)
		if err != nil {
			return err
		}
		key, err := encodeCols(row, p.distinctCols, inTypes)
		if err != nil {
			return err
		}
		emit := false
		if first {
			emit = true
		} else if !bytes.Equal(group, lastGroup) {
			// New ordered-columns run: always emit the first row of the run.
			emit = true
		} else if !bytes.Equal(key, lastKey) {
			emit = true
		}
		if emit {
			processed, _, err := p.Out.ProcessRow(ctx, row)
			if err != nil {
				return err
			}
			if processed != nil {
				cp := make(sqlbase.EncDatumRow, len(processed))
				copy(cp, processed)
				out = append(out, cp)
			}
			lastKey = key
			lastGroup = group
		}
		first = false
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
	// Only emit an Arrow Record when every output column can be materialized by
	// the Arrow engine; otherwise fall back to the row output (p.outputRows) so a
	// downstream operator still works (emitting an unsupported type such as
	// Oid/Unknown would make buildArrowColumns fail).
	if arrowTypesAllSupported(outTypes) {
		cols, err := buildArrowColumns(p.alloc, ptrTypes, p.outputRows, p.da)
		if err != nil {
			// Fall back to the row output instead of failing the whole flow.
			return nil
		}
		p.outputRec = array.NewRecord(buildArrowSchema(ptrTypes), cols, int64(len(p.outputRows)))
	}
	return nil
}

// ArrowOutput implements the ArrowRecordEmitter contract, exposing the computed
// output Record for a downstream Arrow or colexec operator to consume without a
// row round-trip.
func (p *arrowDistinctProcessor) ArrowOutput() arrow.Record {
	return p.outputRec
}

// encodeCols encodes the given columns of a row into a stable key for
// equality/de-dup comparison. NULLs sort first (matching rowexec distinct
// semantics).
func encodeCols(row sqlbase.EncDatumRow, cols []int, inTypes []types.T) ([]byte, error) {
	if len(cols) == 0 {
		return nil, nil
	}
	var key []byte
	for _, c := range cols {
		ed := row[c]
		buf, err := ed.Encode(&inTypes[c], &sqlbase.DatumAlloc{}, sqlbase.DatumEncoding_ASCENDING_KEY, nil)
		if err != nil {
			return nil, err
		}
		key = append(key, buf...)
	}
	return key, nil
}
