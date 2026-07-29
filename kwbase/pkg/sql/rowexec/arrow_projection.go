// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ArrowArg is a single argument to an Arrow compute function. It is either an
// input record column (ColName set), a constant literal (Scalar set), or a
// nested arithmetic expression (Binary set).
type ArrowArg struct {
	// ColName is the name of the input record column (e.g. "col0"). Non-empty
	// when the argument is a column reference.
	ColName string
	// Scalar is a constant argument. Non-nil when the argument is a literal
	// (takes precedence over ColName when both are set).
	Scalar compute.Datum
	// Binary is a nested arithmetic expression (add/sub/mul/div) whose arguments
	// are themselves ArrowArgs.
	Binary *ArrowArgBinary
	// Cast is a type conversion applied to Arg before the parent function
	// consumes it. It supports CAST(col AS ...) inside arrow-computable
	// expressions (e.g. CAST(i AS STRING) LIKE '1%').
	Cast *ArrowArgCast
}

// ArrowArgBinary is a nested arithmetic expression used as a filter operand.
type ArrowArgBinary struct {
	Func string
	Args []ArrowArg
}

// ArrowArgCast is a type conversion applied to a leaf operand. It is evaluated
// by the arrow filter/projection kernels to support CAST inside predicates and
// renders.
type ArrowArgCast struct {
	Type arrow.DataType
	Arg  ArrowArg
}

// ArrowProjectionSpec describes one projected output column computed from input
// record columns via an Arrow compute function (e.g. "add", "sub", "mul",
// "div", "negate", "copy"). These function names correspond to the kernels
// registered in github.com/apache/arrow/go/v17/arrow/compute.
type ArrowProjectionSpec struct {
	// OutputName is the name of the produced column.
	OutputName string
	// Func is the arrow/compute function name, e.g. "add".
	Func string
	// Args are the function arguments (columns and/or scalar constants).
	Args []ArrowArg
}

// arrowProjection is a UnifiedProcessor that evaluates projection expressions
// over Arrow Records using arrow/compute kernels. Per the unification plan this
// is the "deep unification" of a hot operator: the projection runs as a single
// vectorized pass over Arrow arrays (zero-copy ExecSpan views inside the
// kernel) instead of one scalar tree.Datum evaluation per row.
type arrowProjection struct {
	alloc memory.Allocator
	input UnifiedProcessor
	specs []ArrowProjectionSpec
}

// NewArrowProjection builds a projection operator over the given input.
func NewArrowProjection(alloc memory.Allocator, input UnifiedProcessor, specs []ArrowProjectionSpec) UnifiedProcessor {
	return &arrowProjection{alloc: alloc, input: input, specs: specs}
}

// Init implements UnifiedProcessor.
func (p *arrowProjection) Init(ctx context.Context) { p.input.Init(ctx) }

// Allocator implements UnifiedProcessor.
func (p *arrowProjection) Allocator() memory.Allocator { return p.alloc }

// Next implements UnifiedProcessor.
func (p *arrowProjection) Next(ctx context.Context) (arrow.Record, bool, error) {
	in, done, err := p.input.Next(ctx)
	if err != nil || done {
		return nil, done, err
	}
	defer in.Release()

	outFields := make([]arrow.Field, 0, len(p.specs))
	outCols := make([]arrow.Array, 0, len(p.specs))
	for _, spec := range p.specs {
		col, err := p.eval(ctx, in, spec)
		if err != nil {
			for _, c := range outCols {
				c.Release()
			}
			return nil, false, err
		}
		outFields = append(outFields, arrow.Field{Name: spec.OutputName, Type: col.DataType(), Nullable: true})
		outCols = append(outCols, col)
	}
	schema := arrow.NewSchema(outFields, nil)
	return array.NewRecord(schema, outCols, in.NumRows()), false, nil
}

// eval evaluates a single projection spec against the input Record by invoking
// the arrow/compute kernel. Input columns are wrapped as compute.ArrayDatum
// (a zero-copy view over the Arrow buffer) and the result is unwrapped back
// into an Arrow array.
func (p *arrowProjection) eval(ctx context.Context, in arrow.Record, spec ArrowProjectionSpec) (arrow.Array, error) {
	if spec.Func == "copy" {
		// Passthrough: copy an input column directly. Use a retained slice so the
		// result stays valid after the input record is released by the caller.
		idx := in.Schema().FieldIndices(spec.Args[0].ColName)
		if len(idx) == 0 {
			return nil, fmt.Errorf("projection input column %q not found", spec.Args[0].ColName)
		}
		col := in.Column(idx[0])
		return array.NewSlice(col, 0, int64(col.Len())), nil
	}
	args := make([]compute.Datum, len(spec.Args))
	for i, a := range spec.Args {
		if a.Scalar != nil {
			args[i] = a.Scalar
			continue
		}
		idx := in.Schema().FieldIndices(a.ColName)
		if len(idx) == 0 {
			return nil, fmt.Errorf("projection input column %q not found", a.ColName)
		}
		args[i] = compute.NewDatum(in.Column(idx[0]))
	}
	res, err := compute.CallFunction(ctx, spec.Func, nil, args...)
	if err != nil {
		return nil, err
	}
	ad, ok := res.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("expected array result from %q, got %T", spec.Func, res)
	}
	return ad.MakeArray(), nil
}
