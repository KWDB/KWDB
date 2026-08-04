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
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// arrowFilterPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowFilter.Expr.
type arrowFilterPlan struct {
	Root arrowFilterNode `json:"root"`
}

// arrowFilterNode is one node of the boolean filter expression tree.
type arrowFilterNode struct {
	Func     string                 `json:"func"`
	Operands []arrowFilterOperandJS `json:"ops"`
}

// arrowFilterOperandJS is one operand: either a leaf (column/constant) or a
// nested boolean expression.
type arrowFilterOperandJS struct {
	Leaf *arrowFilterLeafJS `json:"leaf,omitempty"`
	Expr *arrowFilterNode   `json:"expr,omitempty"`
}

// arrowFilterLeafJS is a column reference or constant literal leaf. It may also
// carry a nested arithmetic expression (Binary) for computed-column filters.
type arrowFilterLeafJS struct {
	Col       int                 `json:"col"` // -1 for constant
	ConstInt  *int64              `json:"cint,omitempty"`
	ConstFloat *float64           `json:"cfloat,omitempty"`
	ConstBool *bool               `json:"cbool,omitempty"`
	ConstStr  *string             `json:"cstr,omitempty"`
	// ConstSetInt / ConstSetStr carry the member set of an IN / NOT IN
	// predicate (e.g. col IN (1,2,3)). Exactly one is set when the leaf is a
	// set, and both imply Col < 0.
	ConstSetInt []int64  `json:"csetint,omitempty"`
	ConstSetStr []string `json:"csetstr,omitempty"`
	Binary    *arrowFilterBinaryJS `json:"bin,omitempty"`
	// Cast is a type conversion leaf, supporting CAST(col AS ...) in predicates.
	Cast *arrowFilterCastJS `json:"cast,omitempty"`
}

// arrowFilterBinaryJS is a nested arithmetic expression leaf operand.
type arrowFilterBinaryJS struct {
	Func string             `json:"func"`
	Args []arrowFilterLeafJS `json:"args"`
}

// arrowFilterCastJS is the JSON shape of a type conversion leaf. Type is the
// compact tag ("STRING"/"INT"/"FLOAT") emitted by the planner.
type arrowFilterCastJS struct {
	Func string            `json:"func"`
	Type string            `json:"type"`
	Arg  arrowFilterLeafJS `json:"arg"`
}

// arrowFilterRuns counts how many times the arrow filter processor has run.
var arrowFilterRuns int64

// ArrowFilterRunCount returns the number of arrow filter processors that have
// run so far in this process.
func ArrowFilterRunCount() int64 {
	return atomic.LoadInt64(&arrowFilterRuns)
}

// arrowFilterProcessor is a real execution-stage processor that evaluates a
// boolean filter using the Arrow compute engine and emits the matching input
// rows (all columns preserved).
type arrowFilterProcessor struct {
	execinfra.ProcessorBase

	input      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowFilterPlan
	outputRows sqlbase.EncDatumRows
	rowIdx     int
	// outputRec is the arrow.Record produced in compute, retained for
	// operator-to-operator buffering (§7.8). Released in ConsumerClosed.
	outputRec arrow.Record
}

var _ execinfra.RowSource = &arrowFilterProcessor{}

func newArrowFilterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowFilterPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	p := &arrowFilterProcessor{
		input: input,
		alloc: memory.NewGoAllocator(),
		da:    &sqlbase.DatumAlloc{},
		plan:  plan,
	}
	if err := p.Init(
		p, post, input.OutputTypes(), flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{p.input}},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowFilterProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowFilter")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowFilterProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// outputRows already carry the stage post-processing applied once in
	// compute (via p.Out.ProcessRow); emit them directly to avoid applying
	// the post twice.
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
func (p *arrowFilterProcessor) ConsumerClosed() {
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
func (p *arrowFilterProcessor) ArrowOutput() arrow.Record {
	if p.outputRec == nil {
		return nil
	}
	p.outputRec.Retain()
	return p.outputRec
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowFilterProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.input.OutputTypes()
}

// compute drains the input, runs the arrow filter, and materializes rows.
func (p *arrowFilterProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowFilterRuns, 1)

	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}

	spec := buildArrowFilterSpec(p.plan.Root)
	filt := NewArrowFilter(p.alloc, conv, spec)
	filt.Init(ctx)
	rec, done, err := filt.Next(ctx)
	if err != nil {
		return err
	}
	if done || rec == nil {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	// Hand the record to a downstream arrow operator via operator-to-operator
	// buffering (§7.8); released in ConsumerClosed.
	p.outputRec = rec
	// The filter is an identity transform on the columns, so the Arrow record
	// carries all input columns; decode it using the input types, then run the
	// stage's post-processing (projection/offset/limit) to obtain the final
	// output rows.
	allRows, err := arrowRecordToEncDatumRows(p.input.OutputTypes(), rec)
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
		// ProcessRow returns a reusable buffer; copy it before retaining.
		cp := make(sqlbase.EncDatumRow, len(processed))
		copy(cp, processed)
		out = append(out, cp)
	}
	p.outputRows = out
	return nil
}

// buildArrowFilterSpec converts the JSON plan into the compute spec.
func buildArrowFilterSpec(n arrowFilterNode) ArrowFilterSpec {
	ops := make([]ArrowFilterOperand, len(n.Operands))
	for i, o := range n.Operands {
		if o.Expr != nil {
			ops[i] = ArrowFilterOperand{Expr: ptrArrowFilterNode(buildArrowFilterSpec(*o.Expr))}
		} else {
			ops[i] = ArrowFilterOperand{Leaf: leafToArrowArg(o.Leaf)}
		}
	}
	return ArrowFilterSpec{Func: n.Func, Operands: ops}
}

func ptrArrowFilterNode(n ArrowFilterSpec) *ArrowFilterSpec { return &n }

func leafToArrowArg(l *arrowFilterLeafJS) *ArrowArg {
	if l.Binary != nil {
		return &ArrowArg{Binary: binaryToArrowArg(l.Binary)}
	}
	if l.Cast != nil {
		return &ArrowArg{Cast: &ArrowArgCast{Type: arrowCastType(l.Cast.Type), Arg: *leafToArrowArg(&l.Cast.Arg)}}
	}
	if l.Col >= 0 {
		return &ArrowArg{ColName: fmt.Sprintf("col%d", l.Col)}
	}
	switch {
	case l.ConstInt != nil:
		return &ArrowArg{Scalar: compute.NewDatum(int64(*l.ConstInt))}
	case l.ConstFloat != nil:
		return &ArrowArg{Scalar: compute.NewDatum(float64(*l.ConstFloat))}
	case l.ConstBool != nil:
		return &ArrowArg{Scalar: compute.NewDatum(bool(*l.ConstBool))}
	case l.ConstStr != nil:
		return &ArrowArg{Scalar: compute.NewDatum(string(*l.ConstStr))}
	case len(l.ConstSetInt) > 0:
		set := make([]compute.Datum, len(l.ConstSetInt))
		for i, v := range l.ConstSetInt {
			set[i] = compute.NewDatum(int64(v))
		}
		return &ArrowArg{ConstSet: set}
	case len(l.ConstSetStr) > 0:
		set := make([]compute.Datum, len(l.ConstSetStr))
		for i, v := range l.ConstSetStr {
			set[i] = compute.NewDatum(string(v))
		}
		return &ArrowArg{ConstSet: set}
	}
	return &ArrowArg{Scalar: compute.NewDatum(nil)}
}

// arrowCastType maps the planner's compact tag to the target Arrow data type.
func arrowCastType(tag string) arrow.DataType {
	switch tag {
	case "STRING":
		return arrow.BinaryTypes.String
	case "INT":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
}

// binaryToArrowArg converts a JSON nested arithmetic expression into the compute
// spec representation.
func binaryToArrowArg(b *arrowFilterBinaryJS) *ArrowArgBinary {
	args := make([]ArrowArg, len(b.Args))
	for i := range b.Args {
		args[i] = *leafToArrowArg(&b.Args[i])
	}
	return &ArrowArgBinary{Func: b.Func, Args: args}
}

// ArrowRecordEmitter is implemented by arrow processors that can hand their
// computed arrow.Record directly to a downstream arrow operator, so data flows
// operator-to-operator as an Arrow Record instead of a round-trip through
// EncDatum rows (§7.8 operator-to-operator buffering).
//
// The returned Record transfers ownership to the caller, which must Release it.
// It is safe to call exactly once, after the operator has been Started (i.e.
// after compute has run). It returns nil when the operator produced no record
// (e.g. an empty result) or does not participate in chaining.
type ArrowRecordEmitter interface {
	ArrowOutput() arrow.Record
}

// unifiedInputFrom returns a UnifiedProcessor feeding the given arrow operator.
// When src is an ArrowRecordEmitter that produced a record, that record is
// passed through directly (operator-to-operator buffering, no row round-trip).
// Otherwise src is a legacy RowSource and is drained into an Arrow Record via
// newArrowInputSource — the scan bridge / fallback path.
//
// NewArrowRecordSource does not Retain, so the returned Record is owned by the
// caller; the upstream operator retains its own copy and releases it on Close.
func unifiedInputFrom(alloc memory.Allocator, src execinfra.RowSource, da *sqlbase.DatumAlloc) (UnifiedProcessor, error) {
	if em, ok := src.(ArrowRecordEmitter); ok {
		if rec := em.ArrowOutput(); rec != nil {
			return NewArrowRecordSource(alloc, rec), nil
		}
	}
	return newArrowScan(alloc, src, da), nil
}
