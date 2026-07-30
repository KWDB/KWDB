// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
)

// ArrowAggSpec is the unified (compute) description of an aggregation stage.
// The output columns are: first the grouping columns (pass-through, one value
// per group), then the aggregate results in Aggs order.
type ArrowAggSpec struct {
	GroupCols []string
	Aggs      []ArrowAggExpr
}

// ArrowAggExpr is a single aggregate over an input column. Func is one of
// sum/count/min/max/mean (over a value column), count_all (COUNT(*), group
// size) or ident (pass-through of a grouping column).
type ArrowAggExpr struct {
	Func  string
	Input string
}

type arrowAggregatorCore struct {
	spec  ArrowAggSpec
	alloc memory.Allocator
}

// NewArrowAggregator builds an aggregation operator over input.
func NewArrowAggregator(alloc memory.Allocator, input UnifiedProcessor, spec ArrowAggSpec) UnifiedProcessor {
	return &arrowAggregator{arrowAggregatorCore: arrowAggregatorCore{spec: spec, alloc: alloc}, input: input}
}

type arrowAggregator struct {
	arrowAggregatorCore
	input UnifiedProcessor
	ha    *arrowHashAggregator
	done  bool
}

// Allocator implements UnifiedProcessor.
func (a *arrowAggregatorCore) Allocator() memory.Allocator { return a.alloc }

// Init implements UnifiedProcessor.
func (a *arrowAggregator) Init(ctx context.Context) { a.input.Init(ctx) }

// Next implements UnifiedProcessor. It accumulates every input batch into the
// grouped aggregation state and emits exactly one aggregated Record (one row
// per group, or a single row for global aggregation) once the input is
// exhausted. This makes the operator a proper streaming accumulator: it is
// correct whether the input arrives as a single record or many streaming
// records, and it always releases each input record after consuming it.
func (a *arrowAggregator) Next(ctx context.Context) (arrow.Record, bool, error) {
	if a.done {
		return nil, true, nil
	}
	if a.ha == nil {
		a.ha = newArrowHashAggregator(a.alloc, a.spec.GroupCols, a.spec.Aggs)
	}
	for {
		rec, done, err := a.input.Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if done {
			out, err := a.ha.Finalize()
			if err != nil {
				return nil, false, err
			}
			a.done = true
			return out, false, nil
		}
		if err := a.ha.Consume(ctx, rec); err != nil {
			rec.Release()
			return nil, false, err
		}
		rec.Release()
	}
}

func (a *arrowAggregatorCore) releaseCols(cols []arrow.Array, upTo int) {
	for i := 0; i < upTo; i++ {
		if cols[i] != nil {
			cols[i].Release()
		}
	}
}

func (a *arrowAggregatorCore) inputType(rec arrow.Record, colName string) arrow.DataType {
	if colName == "" {
		return arrow.PrimitiveTypes.Int64
	}
	return arrowOperandColumn(rec, colName).DataType()
}

// aggOutputType derives the Arrow result type for an aggregate function.
func aggOutputType(fn string, in arrow.DataType) arrow.DataType {
	switch fn {
	case "count", "count_all":
		return arrow.PrimitiveTypes.Int64
	case "mean":
		if in != nil && (in.ID() == arrow.INT64 || in.ID() == arrow.DECIMAL128) {
			return meanDecimalType
		}
		return arrow.PrimitiveTypes.Float64
	case "sum", "min", "max", "ident":
		return in
	default:
		return in
	}
}

// takeResultArray extracts an owned arrow.Array from a compute take result.
func takeResultArray(res compute.Datum) arrow.Array {
	ad := res.(*compute.ArrayDatum)
	arr := ad.MakeArray()
	arr.Retain()
	res.Release()
	return arr
}

func arrayScalarAt(arr arrow.Array, idx int) scalar.Scalar {
	switch arr.DataType().ID() {
	case arrow.INT64:
		return scalar.NewInt64Scalar(arr.(*array.Int64).Value(idx))
	case arrow.FLOAT64:
		return scalar.NewFloat64Scalar(arr.(*array.Float64).Value(idx))
	case arrow.BOOL:
		return scalar.NewBooleanScalar(arr.(*array.Boolean).Value(idx))
	case arrow.STRING:
		return scalar.NewStringScalar(arr.(*array.String).Value(idx))
	case arrow.DECIMAL128:
		return scalar.NewDecimal128Scalar(arr.(*array.Decimal128).Value(idx), arr.DataType())
	case arrow.TIMESTAMP:
		return scalar.NewTimestampScalar(arr.(*array.Timestamp).Value(idx), arr.DataType())
	case arrow.FIXED_SIZE_BINARY:
		return scalar.NewFixedSizeBinaryScalar(memory.NewBufferBytes(arr.(*array.FixedSizeBinary).Value(idx)), arr.DataType())
	default:
		return scalar.MakeNullScalar(arr.DataType())
	}
}

func appendScalar(b array.Builder, s scalar.Scalar, dt arrow.DataType) {
	if s == nil || !s.IsValid() {
		b.AppendNull()
		return
	}
	switch dt.ID() {
	case arrow.INT64:
		b.(*array.Int64Builder).Append(s.(*scalar.Int64).Value)
	case arrow.FLOAT64:
		b.(*array.Float64Builder).Append(s.(*scalar.Float64).Value)
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(s.(*scalar.Boolean).Value)
	case arrow.STRING:
		b.(*array.StringBuilder).Append(string(s.(*scalar.String).Value.Bytes()))
	case arrow.DECIMAL128:
		b.(*array.Decimal128Builder).Append(s.(*scalar.Decimal128).Value)
	case arrow.TIMESTAMP:
		b.(*array.TimestampBuilder).Append(s.(*scalar.Timestamp).Value)
	case arrow.FIXED_SIZE_BINARY:
		b.(*array.FixedSizeBinaryBuilder).Append(s.(*scalar.FixedSizeBinary).Value.Bytes())
	default:
		b.AppendNull()
	}
}

func int32Array(alloc memory.Allocator, vals []int32) *array.Int32 {
	b := array.NewInt32Builder(alloc)
	b.AppendValues(vals, nil)
	arr := b.NewArray().(*array.Int32)
	b.Release()
	return arr
}
