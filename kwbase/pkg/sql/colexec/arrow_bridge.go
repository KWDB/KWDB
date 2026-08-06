// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package colexec

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

// BatchToRecord converts a colexec columnar Batch into an Arrow Record. This is
// the bridge that lets a vectorized colexec operator feed directly into an
// Arrow-based (or rowexec-unified) downstream operator, skipping the
// row-materializer that would otherwise re-encode every value.
//
// The conversion copies values out of colexec's flat slices into Arrow's
// contiguous buffers. A truly zero-copy mapping would require colexec Vecs to
// be backed by Arrow buffers (sharing the same memory layout); that deeper
// change is described in docs/arrow-unification-architecture.md as future work.
func BatchToRecord(b coldata.Batch, alloc memory.Allocator) (arrow.Record, error) {
	n := b.Length()
	fields := make([]arrow.Field, b.Width())
	cols := make([]arrow.Array, b.Width())
	for i := range cols {
		field, arr, err := vecToArrow(b.ColVec(i), alloc)
		if err != nil {
			for _, c := range cols[:i] {
				c.Release()
			}
			return nil, err
		}
		fields[i] = field
		cols[i] = arr
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, int64(n)), nil
}

func vecToArrow(vec coldata.Vec, alloc memory.Allocator) (arrow.Field, arrow.Array, error) {
	n := vec.Length()
	nulls := vec.Nulls()
	switch vec.Type() {
	case coltypes.Int64:
		b := array.NewInt64Builder(alloc)
		for i := 0; i < n; i++ {
			if nulls.NullAt(i) {
				b.AppendNull()
				continue
			}
			b.Append(vec.Int64()[i])
		}
		return arrow.Field{Name: "col", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, b.NewArray(), nil
	case coltypes.Float64:
		b := array.NewFloat64Builder(alloc)
		for i := 0; i < n; i++ {
			if nulls.NullAt(i) {
				b.AppendNull()
				continue
			}
			b.Append(vec.Float64()[i])
		}
		return arrow.Field{Name: "col", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, b.NewArray(), nil
	case coltypes.Bool:
		b := array.NewBooleanBuilder(alloc)
		for i := 0; i < n; i++ {
			if nulls.NullAt(i) {
				b.AppendNull()
				continue
			}
			b.Append(vec.Bool()[i])
		}
		return arrow.Field{Name: "col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, b.NewArray(), nil
	case coltypes.Bytes:
		b := array.NewStringBuilder(alloc)
		bs := vec.Bytes()
		for i := 0; i < n; i++ {
			if nulls.NullAt(i) {
				b.AppendNull()
				continue
			}
			b.Append(string(bs.Get(i)))
		}
		return arrow.Field{Name: "col", Type: arrow.BinaryTypes.String, Nullable: true}, b.NewArray(), nil
	default:
		return arrow.Field{}, nil, fmt.Errorf("unsupported coltype %s for arrow bridge", vec.Type())
	}
}

// RecordToBatch converts an Arrow Record back into a colexec Batch. It is the
// reverse direction, used when an Arrow-based operator feeds a vectorized
// colexec operator so the two execution models can be mixed in one DAG.
func RecordToBatch(rec arrow.Record, allocator *Allocator) (coldata.Batch, error) {
	n := int(rec.NumRows())
	colTypes := make([]coltypes.T, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		colTypes[i] = arrowTypeToColType(rec.Column(i).DataType())
		if colTypes[i] == coltypes.Unhandled {
			return nil, fmt.Errorf("unsupported arrow type %s for arrow bridge", rec.Column(i).DataType())
		}
	}
	var b coldata.Batch
	if allocator != nil {
		b = allocator.NewMemBatchWithSize(colTypes, n)
	} else {
		b = coldata.NewMemBatchWithSize(colTypes, n)
	}
	b.SetLength(n)
	for i := 0; i < int(rec.NumCols()); i++ {
		if err := arrowToVec(rec.Column(i), b.ColVec(i)); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func arrowTypeToColType(dt arrow.DataType) coltypes.T {
	switch dt.ID() {
	case arrow.INT64:
		return coltypes.Int64
	case arrow.FLOAT64:
		return coltypes.Float64
	case arrow.BOOL:
		return coltypes.Bool
	case arrow.STRING, arrow.BINARY:
		return coltypes.Bytes
	default:
		return coltypes.Unhandled
	}
}

func arrowToVec(col arrow.Array, vec coldata.Vec) error {
	n := col.Len()
	nulls := vec.Nulls()
	switch vec.Type() {
	case coltypes.Int64:
		sl := make([]int64, n)
		arr := col.(*array.Int64)
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				nulls.SetNull(i)
				continue
			}
			sl[i] = arr.Value(i)
		}
		vec.SetCol(sl)
	case coltypes.Float64:
		sl := make([]float64, n)
		arr := col.(*array.Float64)
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				nulls.SetNull(i)
				continue
			}
			sl[i] = arr.Value(i)
		}
		vec.SetCol(sl)
	case coltypes.Bool:
		sl := make([]bool, n)
		arr := col.(*array.Boolean)
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				nulls.SetNull(i)
				continue
			}
			sl[i] = arr.Value(i)
		}
		vec.SetCol(sl)
	case coltypes.Bytes:
		bs := vec.Bytes()
		switch a := col.(type) {
		case *array.String:
			for i := 0; i < n; i++ {
				if col.IsNull(i) {
					nulls.SetNull(i)
					bs.Set(i, []byte{})
					continue
				}
				bs.Set(i, []byte(a.Value(i)))
			}
		case *array.Binary:
			for i := 0; i < n; i++ {
				if col.IsNull(i) {
					nulls.SetNull(i)
					bs.Set(i, []byte{})
					continue
				}
				bs.Set(i, a.Value(i))
			}
		default:
			return fmt.Errorf("unexpected string-like array %T for arrow bridge", col)
		}
	default:
		return fmt.Errorf("unsupported coltype %s for arrow bridge", vec.Type())
	}
	return nil
}
