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
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowJoinPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowJoin.Expr.
type arrowJoinPlan struct {
	LeftKeys  []int  `json:"left_keys"`
	RightKeys []int  `json:"right_keys"`
	Type      string `json:"type"` // "inner", "left", "right" or "full"
	// OnFilter is the JSON-serialized non-equi onExpr post-filter plan (§7.4).
	OnFilter string `json:"on_filter,omitempty"`
}

// arrowJoinRuns counts how many times the arrow join processor has run.
var arrowJoinRuns int64

// ArrowJoinRunCount returns the number of arrow join processors that have run
// so far in this process.
func ArrowJoinRunCount() int64 {
	return atomic.LoadInt64(&arrowJoinRuns)
}

// arrowJoinOnFilterRuns counts how many times the arrow join processor has
// applied a non-equi onExpr post-filter stage (§7.4).
var arrowJoinOnFilterRuns int64

// ArrowJoinOnFilterRunCount returns the number of arrow join post-filter stages
// that have run so far in this process.
func ArrowJoinOnFilterRunCount() int64 {
	return atomic.LoadInt64(&arrowJoinOnFilterRuns)
}

// arrowJoinProcessor is a real execution-stage processor that performs an
// (inner/left/right/full) hash join over Arrow records using the Arrow compute
// engine for equality-key handling.
type arrowJoinProcessor struct {
	execinfra.ProcessorBase

	left       execinfra.RowSource
	right      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowJoinPlan
	outputRows sqlbase.EncDatumRows
	rowIdx     int
	// outputRec is the arrow.Record produced in compute, retained for
	// operator-to-operator buffering (§7.8). Released in ConsumerClosed.
	outputRec arrow.Record
	// onFilter is the decoded non-equi onExpr post-filter plan (§7.4); nil when
	// the join has no non-equi onExpr.
	onFilter *arrowFilterPlan
}

var _ execinfra.RowSource = &arrowJoinProcessor{}

func newArrowJoinProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	left execinfra.RowSource,
	right execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowJoinPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	leftTypes := left.OutputTypes()
	rightTypes := right.OutputTypes()
	outTypes := make([]types.T, 0, len(leftTypes)+len(rightTypes))
	outTypes = append(outTypes, leftTypes...)
	outTypes = append(outTypes, rightTypes...)

	p := &arrowJoinProcessor{
		left:  left,
		right: right,
		alloc: memory.NewGoAllocator(),
		da:    &sqlbase.DatumAlloc{},
		plan:  plan,
	}
	// §7.4: decode the optional non-equi onExpr post-filter plan.
	if plan.OnFilter != "" {
		var of arrowFilterPlan
		if err := json.Unmarshal([]byte(plan.OnFilter), &of); err != nil {
			return nil, err
		}
		if of.Root.Func != "" {
			p.onFilter = &of
		}
	}
	if err := p.Init(
		p, post, outTypes, flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{p.left, p.right}},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowJoinProcessor) Start(ctx context.Context) context.Context {
	p.left.Start(ctx)
	p.right.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowJoin")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowJoinProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
func (p *arrowJoinProcessor) ConsumerClosed() {
	if p.outputRec != nil {
		p.outputRec.Release()
		p.outputRec = nil
	}
	p.ProcessorBase.InternalClose()
}

// ArrowOutput implements ArrowRecordEmitter, exposing the computed Record for
// operator-to-operator buffering. Ownership transfers to the caller (Retain is
// applied here; the caller must Release). Returns nil when no record was
// produced.
func (p *arrowJoinProcessor) ArrowOutput() arrow.Record {
	if p.outputRec == nil {
		return nil
	}
	p.outputRec.Retain()
	return p.outputRec
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowJoinProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	leftTypes := p.left.OutputTypes()
	rightTypes := p.right.OutputTypes()
	outTypes := make([]types.T, 0, len(leftTypes)+len(rightTypes))
	outTypes = append(outTypes, leftTypes...)
	outTypes = append(outTypes, rightTypes...)
	p.Out.OutputTypes = outTypes
}

func (p *arrowJoinProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowJoinRuns, 1)

	leftConv, err := unifiedInputFrom(p.alloc, p.left, p.da)
	if err != nil {
		return err
	}
	rightConv, err := unifiedInputFrom(p.alloc, p.right, p.da)
	if err != nil {
		return err
	}

	spec := buildArrowJoinSpec(p.plan)
	join := NewArrowJoin(p.alloc, leftConv, rightConv, spec)
	join.Init(ctx)
	rec, done, err := join.Next(ctx)
	if err != nil {
		return err
	}
	if done || rec == nil {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	// Record handoff to a downstream arrow operator is handled via p.outputRec
	// (released in ConsumerClosed), so we don't release rec here.
	// §7.4: apply the non-equi onExpr as a post-filter over the equi-join result.
	// This matches inner-join semantics (no NULL-extended rows survive). The
	// filter reads from the merged record columns col0..col{nL+nR-1}, which carry
	// the left input columns followed by the right input columns.
	if p.onFilter != nil {
		atomic.AddInt64(&arrowJoinOnFilterRuns, 1)
		filtered, err := p.applyOnFilter(ctx, rec, p.onFilter)
		if err != nil {
			return err
		}
		// eval copies the surviving rows into a fresh record, so releasing the
		// original input is safe; p.outputRec (set below) holds `filtered` and is
		// released in ConsumerClosed.
		rec.Release()
		rec = filtered
	}
	// Hand the final record to a downstream arrow operator via operator-to-operator
	// buffering (§7.8); released in ConsumerClosed.
	p.outputRec = rec
	// The join is an identity transform on the union of both inputs' columns, so
	// the Arrow record carries all (left+right) columns. Decode using the full
	// output types, then run the stage's post-processing (projection/offset/limit)
	// to obtain the final output rows.
	//
	// Arity guard (§7.9): the planner-declared output column count
	// (left.OutputTypes()+right.OutputTypes()) must equal the Arrow record arity.
	// When an upstream Arrow operator emits a record whose column count differs
	// from its declared type (a known Arrow single-record-model limitation), trust
	// the actual record arity and derive each column's KWDB type from its Arrow
	// DataType. This keeps ProcessRow (which reads p.Out.OutputTypes) and the
	// downstream Arrow operator-to-operator handoff (p.outputRec) consistent with
	// the real record, instead of panicking on an out-of-range column index.
	fullTypes := make([]types.T, 0, len(p.left.OutputTypes())+len(p.right.OutputTypes()))
	fullTypes = append(fullTypes, p.left.OutputTypes()...)
	fullTypes = append(fullTypes, p.right.OutputTypes()...)
	// Arity guard (§7.9): the Arrow record arity must match the processor's
	// declared output arity (left+right columns). When an upstream Arrow operator
	// emits a degenerate record (a known single-record-model limitation where a
	// 0-row side collapses to 0 columns, or OutputTypes is not yet populated at
	// Start time), the declared output (p.Out.OutputTypes, set at Init) and the
	// planner-derived fullTypes may disagree with the actual record arity. In
	// that case trust the actual record arity and derive each column's KWDB type
	// from its Arrow DataType. Otherwise keep the planner-declared fullTypes:
	// arrowDataTypeToKWType collapses logical distinctions (e.g. TimestampTZ and
	// Timestamp both map to arrow.TIMESTAMP, but they are indistinguishable at the
	// Arrow storage layer), so preferring the planner type preserves TZ tags and
	// decimal precision/scale.
	if int(rec.NumCols()) != len(fullTypes) {
		// Degenerate record: realign both the output types and the decode types
		// off the actual record arity so ProcessRow / downstream handoff cannot
		// index out of range.
		deriveTypes := make([]types.T, rec.NumCols())
		for ci := 0; ci < int(rec.NumCols()); ci++ {
			deriveTypes[ci] = arrowDataTypeToKWType(rec.Column(ci).DataType())
		}
		p.Out.OutputTypes = deriveTypes
		fullTypes = deriveTypes
	}
	allRows, err := arrowRecordToEncDatumRows(fullTypes, rec)
	if err != nil {
		return err
	}
	out := make(sqlbase.EncDatumRows, 0, len(allRows))
	for _, row := range allRows {
		processed, _, err := p.Out.ProcessRow(ctx, row)
		if err != nil {
			return err
		}
		if processed == nil {
			continue
		}
		cp := make(sqlbase.EncDatumRow, len(processed))
		copy(cp, processed)
		out = append(out, cp)
	}
	// p.outputRows must carry the *post-processed* (projected) join columns so
	// that the row-based Next() path returns the stage's output schema (v,w),
	// not the raw left+right join columns (k_l,v_l,k_r,w_r). ArrowOutput() uses
	// projRec (built below from out) for operator-to-operator Arrow handoff, so
	// it is already projected; here we point outputRows at the same projected
	// rows so both Next() and ArrowOutput() agree on the schema.
	p.outputRows = out
	projRec, err := p.newPostRecord(out)
	if err != nil {
		rec.Release()
		return err
	}
	// The raw join record is no longer needed once projected into projRec.
	rec.Release()
	p.outputRec = projRec
	return nil
}

// newPostRecord rebuilds an Arrow record from the post-processed output rows so
// that the operator-to-operator consumer (which reads columns by p.Out's
// projected schema) sees exactly the same columns as the row-based Next() path.
// rows come from p.Out.ProcessRow (already projected), and typs is p.Out.OutputTypes
// (the stage's output schema, which the arity guard at §7.9 may have realigned
// to the actual record arity).
func (p *arrowJoinProcessor) newPostRecord(rows sqlbase.EncDatumRows) (arrow.Record, error) {
	return newPostRecordFromRows(p.alloc, p.da, p.Out.OutputTypes, rows)
}

func buildArrowJoinSpec(plan arrowJoinPlan) ArrowJoinSpec {
	lk := make([]string, len(plan.LeftKeys))
	for i, c := range plan.LeftKeys {
		lk[i] = fmt.Sprintf("col%d", c)
	}
	rk := make([]string, len(plan.RightKeys))
	for i, c := range plan.RightKeys {
		rk[i] = fmt.Sprintf("col%d", c)
	}
	typ := plan.Type
	if typ == "" {
		typ = "inner"
	}
	return ArrowJoinSpec{LeftKeys: lk, RightKeys: rk, Type: typ}
}

// applyOnFilter evaluates the decoded non-equi onExpr (§7.4) over the merged
// join record and returns a new record containing only the rows that satisfy it.
func (p *arrowJoinProcessor) applyOnFilter(ctx context.Context, rec arrow.Record, plan *arrowFilterPlan) (arrow.Record, error) {
	core := newArrowFilterCore(buildArrowFilterSpec(plan.Root), p.alloc, p.EvalCtx)
	return core.eval(ctx, rec)
}
