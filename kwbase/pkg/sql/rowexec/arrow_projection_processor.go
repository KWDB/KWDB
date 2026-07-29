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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowexec

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/apd"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowProjectionPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowProjection.Expr. It is intentionally a plain struct
// (no protobuf) so that it can be hand-encoded into the existing generated
// ProcessorCoreUnion without touching the wire format beyond the new field.
type arrowProjectionPlan struct {
	Cols []arrowProjectionCol `json:"cols"`
}

// arrowArg is one argument to a computed column. It refers either to an input
// (stream) column or carries a constant literal value.
type arrowArg struct {
	// Col is the index of the input (stream) column this argument reads from.
	// It is -1 when ConstInt/ConstFloat carry the value instead (a literal).
	Col int `json:"col"`
	// Constant literal values; exactly one is set when Col == -1.
	ConstInt   *int64   `json:"cint,omitempty"`
	ConstFloat *float64 `json:"cfloat,omitempty"`
}

// arrowProjectionCol describes how to compute one output column of the
// projection. A "compute" column applies an arrow/compute kernel (e.g. add) to
// its arguments; a "passthrough" column copies an input column verbatim.
type arrowProjectionCol struct {
	Kind   string     `json:"kind"`   // "compute" or "passthrough"
	Func   string     `json:"func"`   // for compute: add/sub/mul/div/negate/copy
	Inputs []arrowArg `json:"inputs"` // for compute: arguments (columns and/or constants)
	Input  int        `json:"input"`  // for passthrough: input column index
}

// arrowProjectionRuns counts how many times the arrow projection processor has
// been executed. It lets tests confirm that a query actually went through the
// Arrow compute path.
var arrowProjectionRuns int64

// ArrowProjectionRunCount returns the number of arrow projection processors
// that have run so far in this process.
func ArrowProjectionRunCount() int64 {
	return atomic.LoadInt64(&arrowProjectionRuns)
}

// arrowProjectionProcessor is a real execution-stage processor that evaluates
// an arrow-computable projection (e.g. a+b) using the Arrow compute engine.
// It buffers its entire input, converts the batch to an Arrow record, runs the
// projection, and emits the rendered rows back as EncDatumRows.
type arrowProjectionProcessor struct {
	execinfra.ProcessorBase

	input      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowProjectionPlan
	outputRows sqlbase.EncDatumRows
	rowIdx     int
	// outputRec is the arrow.Record produced in compute, retained for
	// operator-to-operator buffering (§7.8). Released in ConsumerClosed.
	outputRec arrow.Record
}

var _ execinfra.RowSource = &arrowProjectionProcessor{}

func newArrowProjectionProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowProjectionPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	p := &arrowProjectionProcessor{
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
func (p *arrowProjectionProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowProjection")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowProjectionProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
func (p *arrowProjectionProcessor) ConsumerClosed() {
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
func (p *arrowProjectionProcessor) ArrowOutput() arrow.Record {
	if p.outputRec == nil {
		return nil
	}
	p.outputRec.Retain()
	return p.outputRec
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowProjectionProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.ProcessorBase.EvalCtx != nil && p.ProcessorBase.EvalCtx.IsProcedure && p.ProcessorBase.FlowCtx != nil {
		p.ProcessorBase.FlowCtx.Txn = txn
	}
}

// compute drains the entire input, runs the arrow projection, and materializes
// the resulting rows. For the common all-integer input case it uses a
// zero-tree.Datum fast path (§7.6): integer values are decoded straight into
// preallocated []int64 slices and turned into Arrow arrays via a pre-reserved
// builder, avoiding the per-row EnsureDecoded + EncDatum boxing that dominated
// the unify bridge cost. Other inputs fall back to the generic row-materializing
// path.
func (p *arrowProjectionProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowProjectionRuns, 1)
	inTypes := p.input.OutputTypes()
	allInt := true
	for i := range inTypes {
		if inTypes[i].Family() != types.IntFamily {
			allInt = false
			break
		}
	}
	if allInt {
		return p.computeInt(ctx, inTypes)
	}
	return p.computeGeneric(ctx, inTypes)
}

// computeGeneric is the original all-types projection path: it materializes the
// input rows one at a time (decoding each value into a tree.Datum) before
// building the Arrow Record. Kept for non-integer inputs.
func (p *arrowProjectionProcessor) computeGeneric(ctx context.Context, inTypes []types.T) error {
	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}

	proj := NewArrowProjection(p.alloc, conv, p.buildProjectionSpecs())
	proj.Init(ctx)
	rec, done, err := proj.Next(ctx)
	if err != nil {
		return err
	}
	if done {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	// Hand the record to a downstream arrow operator via operator-to-operator
	// buffering (§7.8); released in ConsumerClosed.
	p.outputRec = rec
	p.outputRows, err = arrowRecordToEncDatumRows(p.Out.OutputTypes, rec)
	return err
}

// computeInt is the §7.6 fast path for all-integer inputs. Instead of decoding
// each value into a heap-allocated tree.Datum, it reads the integer directly via
// EncDatum.GetInt into a preallocated []int64 and builds the input Arrow Record
// from those slices with a single pre-reserved builder. This removes the
// per-row Datum allocation that previously dominated the projection's bridge
// cost, so the only remaining allocations are the Arrow buffers themselves.
func (p *arrowProjectionProcessor) computeInt(ctx context.Context, inTypes []types.T) error {
	nIn := len(inTypes)
	// Use unifiedInputFrom so that an upstream Arrow operator's record is passed
	// through without a row round-trip (§7.8 input-side chaining): a filter→
	// projection(INT) link now flows the Arrow record directly. In both the
	// Arrow-through and the EncDatumRows-bridge fallback paths the input columns
	// are *array.Int64, so we read the contiguous Int64Values() directly instead
	// of decoding each row through the per-row EncDatum bridge.
	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}
	conv.Init(ctx)

	vals := make([][]int64, nIn)
	nulls := make([][]bool, nIn)
	for i := 0; i < nIn; i++ {
		vals[i] = make([]int64, 0, 1024)
		nulls[i] = make([]bool, 0, 1024)
	}
	n := 0
	for {
		rec, done, err := conv.Next(ctx)
		if err != nil {
			return err
		}
		if done {
			break
		}
		nrows := int(rec.NumRows())
		if nrows == 0 {
			rec.Release()
			continue
		}
		cols := rec.Columns()
		if len(cols) != nIn {
			rec.Release()
			return fmt.Errorf("arrow projection input has %d columns, expected %d", len(cols), nIn)
		}
		for ci := 0; ci < nIn; ci++ {
			arr, ok := cols[ci].(*array.Int64)
			if !ok {
				rec.Release()
				return fmt.Errorf("arrow projection input column %d is %T, expected *array.Int64", ci, cols[ci])
			}
			data := arr.Int64Values()
			for ri := 0; ri < nrows; ri++ {
				if arr.IsNull(ri) {
					vals[ci] = append(vals[ci], 0)
					nulls[ci] = append(nulls[ci], true)
				} else {
					vals[ci] = append(vals[ci], data[ri])
					nulls[ci] = append(nulls[ci], false)
				}
			}
		}
		n += nrows
		rec.Release()
	}
	if n == 0 {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}

	cols := make([]arrow.Array, nIn)
	for i := 0; i < nIn; i++ {
		b := array.NewInt64Builder(p.alloc)
		b.Reserve(n)
		for ri := 0; ri < n; ri++ {
			if nulls[i][ri] {
				b.AppendNull()
			} else {
				b.Append(vals[i][ri])
			}
		}
		cols[i] = b.NewArray()
	}
	fields := make([]arrow.Field, nIn)
	for i := range inTypes {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	}
	rec := array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(n))

	proj := NewArrowProjection(p.alloc, NewArrowRecordSource(p.alloc, rec), p.buildProjectionSpecs())
	proj.Init(ctx)
	out, done, err := proj.Next(ctx)
	if err != nil {
		return err
	}
	if done {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	// Hand the record to a downstream arrow operator via operator-to-operator
	// buffering (§7.8); released in ConsumerClosed.
	p.outputRec = out
	p.outputRows, err = arrowRecordToEncDatumRows(p.Out.OutputTypes, out)
	return err
}

// buildProjectionSpecs expands the planner plan into ArrowProjectionSpecs.
func (p *arrowProjectionProcessor) buildProjectionSpecs() []ArrowProjectionSpec {
	specs := make([]ArrowProjectionSpec, len(p.plan.Cols))
	for i, c := range p.plan.Cols {
		outName := fmt.Sprintf("out%d", i)
		if c.Kind == "passthrough" {
			specs[i] = ArrowProjectionSpec{
				OutputName: outName,
				Func:       "copy",
				Args:       []ArrowArg{{ColName: fmt.Sprintf("col%d", c.Input)}},
			}
			continue
		}
		args := make([]ArrowArg, len(c.Inputs))
		for j, in := range c.Inputs {
			if in.Col >= 0 {
				args[j] = ArrowArg{ColName: fmt.Sprintf("col%d", in.Col)}
			} else {
				args[j] = ArrowArg{Scalar: arrowConstDatum(in, p.alloc)}
			}
		}
		specs[i] = ArrowProjectionSpec{
			OutputName: outName,
			Func:       c.Func,
			Args:       args,
		}
	}
	return specs
}

// ArrowProjectionResultInt64 runs the projection to completion and returns the
// int64 values of its first output column. It exists so that callers in other
// packages (notably tests) can inspect projection results without importing the
// Arrow package directly, which can otherwise resolve to an incompatible copy
// of the Arrow types under GOPATH/vendor mode.
func ArrowProjectionResultInt64(proj UnifiedProcessor, ctx context.Context) ([]int64, error) {
	all, err := ArrowProjectionResultsInt64(proj, ctx)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("arrow projection produced no columns")
	}
	return all[0], nil
}

// ArrowProjectionResultsInt64 runs the projection to completion and returns all
// output columns as int64 slices. It performs the *array.Int64 assertion inside
// rowexec (a single arrow instance) so that callers (e.g. tests in other
// packages) don't trip over the double-loaded arrow type incompatibility.
func ArrowProjectionResultsInt64(proj UnifiedProcessor, ctx context.Context) ([][]int64, error) {
	rec, done, err := proj.Next(ctx)
	if err != nil {
		return nil, err
	}
	if done || rec == nil {
		return nil, fmt.Errorf("arrow projection produced no record")
	}
	defer rec.Release()
	nCols := int(rec.NumCols())
	out := make([][]int64, nCols)
	for c := 0; c < nCols; c++ {
		arr, ok := rec.Column(c).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("expected Int64 column %d, got %T", c, rec.Column(c))
		}
		vals := make([]int64, arr.Len())
		for i := 0; i < arr.Len(); i++ {
			vals[i] = arr.Value(i)
		}
		out[c] = vals
	}
	return out, nil
}

// arrowConstDatum wraps a constant argument into a compute scalar datum so that
// it can be passed to an arrow/compute kernel alongside array arguments.
func arrowConstDatum(a arrowArg, _ memory.Allocator) compute.Datum {
	if a.ConstInt != nil {
		return compute.NewDatum(int64(*a.ConstInt))
	}
	return compute.NewDatum(float64(*a.ConstFloat))
}

// arrowRecordToEncDatumRows decodes an Arrow record produced by the projection
// back into EncDatumRows. The i-th column of the record corresponds to the i-th
// output type.
func arrowRecordToEncDatumRows(
	typs []types.T, rec arrow.Record,
) (sqlbase.EncDatumRows, error) {
	n := int(rec.NumRows())
	cols := rec.Columns()
	// §7.7: consolidate the per-row slice headers into a single flat buffer so
	// the bridge performs one allocation instead of n (one per row).
	flat := make([]sqlbase.EncDatum, n*len(typs))
	rows := make(sqlbase.EncDatumRows, n)
	for i := 0; i < n; i++ {
		rows[i] = flat[i*len(typs) : (i+1)*len(typs)]
	}
	for ci, col := range cols {
		t := typs[ci]
		switch arr := col.(type) {
		case *array.Int64:
			// §7.7: batch the per-value Datum allocations into one slice per
			// column and reference into it, instead of one heap escape per value.
			data := arr.Int64Values()
			vals := make([]tree.DInt, n)
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				vals[i] = tree.DInt(data[i])
				rows[i][ci] = sqlbase.EncDatum{Datum: &vals[i]}
			}
		case *array.Float64:
			data := arr.Float64Values()
			vals := make([]tree.DFloat, n)
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				vals[i] = tree.DFloat(data[i])
				rows[i][ci] = sqlbase.EncDatum{Datum: &vals[i]}
			}
		case *array.Boolean:
			vals := make([]tree.DBool, n)
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				vals[i] = tree.DBool(arr.Value(i))
				rows[i][ci] = sqlbase.EncDatum{Datum: &vals[i]}
			}
		case *array.String:
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				v := tree.DString(arr.Value(i))
				rows[i][ci] = sqlbase.EncDatum{Datum: &v}
			}
		case *array.Binary:
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				b := arr.Value(i)
				if t.Family() == types.StringFamily {
					v := tree.DString(string(b))
					rows[i][ci] = sqlbase.EncDatum{Datum: &v}
				} else {
					v := tree.DBytes(b)
					rows[i][ci] = sqlbase.EncDatum{Datum: &v}
				}
			}
		case *array.Decimal128:
			dt := col.DataType().(*arrow.Decimal128Type)
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				bi := arr.Value(i).BigInt()
				var dec apd.Decimal
				dec.Negative = bi.Sign() < 0
				dec.Coeff = *bi
				if dec.Negative {
					dec.Coeff.Abs(&dec.Coeff)
				}
				dec.Exponent = -dt.Scale
				rows[i][ci] = sqlbase.EncDatum{Datum: &tree.DDecimal{Decimal: dec}}
			}
		case *array.Timestamp:
			for i := 0; i < n; i++ {
				if arr.IsNull(i) {
					rows[i][ci] = sqlbase.EncDatum{Datum: tree.DNull}
					continue
				}
				tm := arr.Value(i).ToTime(arrow.Microsecond)
				switch t.Family() {
				case types.TimestampTZFamily:
					v := tree.DTimestampTZ{Time: tm}
					rows[i][ci] = sqlbase.EncDatum{Datum: &v}
				default:
					v := tree.DTimestamp{Time: tm}
					rows[i][ci] = sqlbase.EncDatum{Datum: &v}
				}
			}
		default:
			return nil, fmt.Errorf("arrow projection: unsupported output column type %T for family %s", col, t.Family())
		}
	}
	return rows, nil
}
