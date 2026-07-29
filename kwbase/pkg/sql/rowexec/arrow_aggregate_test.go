// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
)

// int64Column builds an Int64 arrow column. A value at index i is null when
// nulls[i] is true.
func int64Column(alloc memory.Allocator, vals []int64, nulls []bool) arrow.Array {
	b := array.NewInt64Builder(alloc)
	for i, v := range vals {
		if nulls != nil && nulls[i] {
			b.AppendNull()
		} else {
			b.Append(v)
		}
	}
	arr := b.NewArray()
	b.Release()
	return arr
}

func mustScalarInt64(t *testing.T, sc scalar.Scalar) int64 {
	t.Helper()
	if !sc.IsValid() {
		t.Fatalf("expected a valid scalar, got null")
	}
	s, ok := sc.(*scalar.Int64)
	if !ok {
		t.Fatalf("expected Int64 scalar, got %T", sc)
	}
	return s.Value
}

func mustScalarFloat64(t *testing.T, sc scalar.Scalar) float64 {
	t.Helper()
	if !sc.IsValid() {
		t.Fatalf("expected a valid scalar, got null")
	}
	s, ok := sc.(*scalar.Float64)
	if !ok {
		t.Fatalf("expected Float64 scalar, got %T", sc)
	}
	return s.Value
}

// float64Column builds a Float64 arrow column.
func float64Column(alloc memory.Allocator, vals []float64, nulls []bool) arrow.Array {
	b := array.NewFloat64Builder(alloc)
	for i, v := range vals {
		if nulls != nil && nulls[i] {
			b.AppendNull()
		} else {
			b.Append(v)
		}
	}
	arr := b.NewArray()
	b.Release()
	return arr
}

// decimal128Column builds a decimal128 arrow column whose logical value is
// coeffs[i] * 10^-scale (i.e. coeffs already carry the scale). A value is null
// when nulls[i] is true.
func decimal128Column(alloc memory.Allocator, scale int32, coeffs []int64, nulls []bool) arrow.Array {
	dt := &arrow.Decimal128Type{Precision: 38, Scale: scale}
	b := array.NewDecimal128Builder(alloc, dt)
	for i, c := range coeffs {
		if nulls != nil && nulls[i] {
			b.AppendNull()
		} else {
			b.Append(decimal128.FromBigInt(new(big.Int).SetInt64(c)))
		}
	}
	arr := b.NewArray()
	b.Release()
	return arr
}

// timestampColumn builds a timestamp arrow column whose values are
// micros-since-epoch (arrow.Timestamp, microsecond unit).
func timestampColumn(alloc memory.Allocator, micros []int64, nulls []bool) arrow.Array {
	dt := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType)
	b := array.NewTimestampBuilder(alloc, dt)
	for i, m := range micros {
		if nulls != nil && nulls[i] {
			b.AppendNull()
		} else {
			b.Append(arrow.Timestamp(m))
		}
	}
	arr := b.NewArray()
	b.Release()
	return arr
}

// decNumToFloat interprets a decimal128.Num (with meanDecimalType.Scale) as a
// float64 so decimal aggregate results can be compared numerically.
func decNumToFloat(num decimal128.Num) float64 {
	bi := num.BigInt()
	rat := new(big.Rat).SetInt(bi)
	scale := big.NewInt(10)
	scale.Exp(scale, big.NewInt(int64(meanDecimalType.Scale)), nil)
	rat.Quo(rat, new(big.Rat).SetInt(scale))
	f, _ := rat.Float64()
	return f
}

// decNumToFloatWithScale is decNumToFloat but for a column carrying the given
// scale (rather than meanDecimalType.Scale), used for SUM/MIN/MAX over DECIMAL
// whose output type keeps the input column's scale.
func decNumToFloatWithScale(num decimal128.Num, scale int32) float64 {
	bi := num.BigInt()
	rat := new(big.Rat).SetInt(bi)
	s := big.NewInt(10)
	s.Exp(s, big.NewInt(int64(scale)), nil)
	rat.Quo(rat, new(big.Rat).SetInt(s))
	f, _ := rat.Float64()
	return f
}

func mustScalarDecimal128(t *testing.T, sc scalar.Scalar) float64 {
	t.Helper()
	if !sc.IsValid() {
		t.Fatalf("expected a valid scalar, got null")
	}
	s, ok := sc.(*scalar.Decimal128)
	if !ok {
		t.Fatalf("expected Decimal128 scalar, got %T", sc)
	}
	return decNumToFloat(s.Value)
}

func mustScalarDecimal128Scale(t *testing.T, sc scalar.Scalar, scale int32) float64 {
	t.Helper()
	if !sc.IsValid() {
		t.Fatalf("expected a valid scalar, got null")
	}
	s, ok := sc.(*scalar.Decimal128)
	if !ok {
		t.Fatalf("expected Decimal128 scalar, got %T", sc)
	}
	return decNumToFloatWithScale(s.Value, scale)
}

func mustScalarTimestamp(t *testing.T, sc scalar.Scalar) int64 {
	t.Helper()
	if !sc.IsValid() {
		t.Fatalf("expected a valid timestamp scalar, got null")
	}
	s, ok := sc.(*scalar.Timestamp)
	if !ok {
		t.Fatalf("expected Timestamp scalar, got %T", sc)
	}
	return int64(s.Value)
}

// TestAggregateArray exercises the scalar aggregate kernel (the pure-Arrow
// equivalent of C++ ScalarAggregateFunction) over a single column.
func TestAggregateArray(t *testing.T) {
	alloc := memory.DefaultAllocator

	t.Run("sum_int", func(t *testing.T) {
		col := int64Column(alloc, []int64{1, 2, 3}, nil)
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarInt64(t, sc); got != 6 {
			t.Fatalf("sum(1,2,3) = %d, want 6", got)
		}
	})

	t.Run("sum_skips_nulls", func(t *testing.T) {
		col := int64Column(alloc, []int64{1, 3}, []bool{false, true}) // 1, null
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarInt64(t, sc); got != 1 {
			t.Fatalf("sum(1,null) = %d, want 1", got)
		}
	})

	t.Run("sum_all_null_is_null", func(t *testing.T) {
		col := int64Column(alloc, []int64{0, 0}, []bool{true, true})
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if sc.IsValid() {
			t.Fatalf("sum over all-null should be null, got %v", sc)
		}
	})

	t.Run("count_skips_nulls", func(t *testing.T) {
		col := int64Column(alloc, []int64{1, 2, 3}, []bool{false, true, false})
		defer col.Release()
		sc, err := aggregateArray("count", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarInt64(t, sc); got != 2 {
			t.Fatalf("count(1,null,3) = %d, want 2", got)
		}
	})

	t.Run("min_max", func(t *testing.T) {
		col := int64Column(alloc, []int64{3, 1, 2}, nil)
		defer col.Release()
		mn, err := aggregateArray("min", col)
		if err != nil {
			t.Fatal(err)
		}
		mx, err := aggregateArray("max", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarInt64(t, mn); got != 1 {
			t.Fatalf("min = %d, want 1", got)
		}
		if got := mustScalarInt64(t, mx); got != 3 {
			t.Fatalf("max = %d, want 3", got)
		}
	})

	t.Run("min_all_null_is_null", func(t *testing.T) {
		col := int64Column(alloc, []int64{0}, []bool{true})
		defer col.Release()
		sc, err := aggregateArray("min", col)
		if err != nil {
			t.Fatal(err)
		}
		if sc.IsValid() {
			t.Fatalf("min over all-null should be null")
		}
	})

	t.Run("mean", func(t *testing.T) {
		col := int64Column(alloc, []int64{1, 2, 3}, nil)
		defer col.Release()
		sc, err := aggregateArray("mean", col)
		if err != nil {
			t.Fatal(err)
		}
		// AVG over integers returns DECIMAL, so the kernel emits decimal128.
		if got := mustScalarDecimal128(t, sc); math.Abs(got-2.0) > 1e-9 {
			t.Fatalf("mean(1,2,3) = %g, want 2", got)
		}
	})

	t.Run("mean_float", func(t *testing.T) {
		col := float64Column(alloc, []float64{1, 2, 3}, nil)
		defer col.Release()
		sc, err := aggregateArray("mean", col)
		if err != nil {
			t.Fatal(err)
		}
		// AVG over floats returns FLOAT, so the kernel emits float64.
		if got := mustScalarFloat64(t, sc); math.Abs(got-2.0) > 1e-9 {
			t.Fatalf("mean(1,2,3) = %g, want 2", got)
		}
	})

	t.Run("mean_all_null_is_null", func(t *testing.T) {
		col := int64Column(alloc, []int64{0}, []bool{true})
		defer col.Release()
		sc, err := aggregateArray("mean", col)
		if err != nil {
			t.Fatal(err)
		}
		if sc.IsValid() {
			t.Fatalf("mean over all-null should be null")
		}
	})
}

// TestAggregateArrayDecimal exercises the scalar aggregate kernel over DECIMAL
// (decimal128) input columns, covering the recent DECIMAL aggregation support:
// SUM/MIN/MAX keep the input column's scale while AVG emits a meanDecimalType
// (scale 9) value, exactly like SQL AVG over DECIMAL.
func TestAggregateArrayDecimal(t *testing.T) {
	alloc := memory.DefaultAllocator
	const scale = int32(2)

	t.Run("sum_dec", func(t *testing.T) {
		// 1.00, 2.00, 3.00
		col := decimal128Column(alloc, scale, []int64{100, 200, 300}, nil)
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarDecimal128Scale(t, sc, scale); math.Abs(got-6.0) > 1e-9 {
			t.Fatalf("sum(1,2,3) decimal = %g, want 6", got)
		}
	})

	t.Run("sum_dec_skips_nulls", func(t *testing.T) {
		col := decimal128Column(alloc, scale, []int64{100, 200}, []bool{false, true})
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarDecimal128Scale(t, sc, scale); math.Abs(got-1.0) > 1e-9 {
			t.Fatalf("sum(1,null) decimal = %g, want 1", got)
		}
	})

	t.Run("min_max_dec", func(t *testing.T) {
		col := decimal128Column(alloc, scale, []int64{300, 100, 200}, nil)
		defer col.Release()
		mn, err := aggregateArray("min", col)
		if err != nil {
			t.Fatal(err)
		}
		mx, err := aggregateArray("max", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarDecimal128Scale(t, mn, scale); math.Abs(got-1.0) > 1e-9 {
			t.Fatalf("min decimal = %g, want 1", got)
		}
		if got := mustScalarDecimal128Scale(t, mx, scale); math.Abs(got-3.0) > 1e-9 {
			t.Fatalf("max decimal = %g, want 3", got)
		}
	})

	t.Run("mean_dec", func(t *testing.T) {
		// (1.00 + 2.00 + 3.00) / 3 = 2.00, emitted with meanDecimalType.Scale (9).
		col := decimal128Column(alloc, scale, []int64{100, 200, 300}, nil)
		defer col.Release()
		sc, err := aggregateArray("mean", col)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustScalarDecimal128(t, sc); math.Abs(got-2.0) > 1e-9 {
			t.Fatalf("mean(1,2,3) decimal = %g, want 2", got)
		}
	})

	t.Run("all_null_is_null", func(t *testing.T) {
		col := decimal128Column(alloc, scale, []int64{0, 0}, []bool{true, true})
		defer col.Release()
		sc, err := aggregateArray("sum", col)
		if err != nil {
			t.Fatal(err)
		}
		if sc.IsValid() {
			t.Fatalf("sum over all-null decimal should be null, got %v", sc)
		}
	})
}

// TestArrowHashAggregatorDecimal exercises grouped aggregation where the
// grouping key itself is a DECIMAL column (the decimal group-key path), with a
// DECIMAL value column. It verifies the high-precision decimal accumulation and
// that the decimal group key round-trips unchanged in the output.
func TestArrowHashAggregatorDecimal(t *testing.T) {
	alloc := memory.DefaultAllocator
	const scale = int32(2)

	// group key g: 1.00, 1.00, 2.00, 2.00 ; value a: 1.00, 2.00, 3.00, 4.00
	gCol := decimal128Column(alloc, scale, []int64{100, 100, 200, 200}, nil)
	aCol := decimal128Column(alloc, scale, []int64{100, 200, 300, 400}, nil)
	rec := buildRecord(alloc, []string{"g", "a"}, []arrow.Array{gCol, aCol}, 4)
	defer rec.Release()
	defer gCol.Release()
	defer aCol.Release()

	aggs := []ArrowAggExpr{
		{Func: "sum", Input: "a"},
		{Func: "min", Input: "a"},
		{Func: "max", Input: "a"},
		{Func: "count", Input: "a"},
	}
	ha := newArrowHashAggregator(alloc, []string{"g"}, aggs)
	if err := ha.Consume(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	out, err := ha.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// Group order is first-seen: g=1.00 then g=2.00.
	if out.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", out.NumRows())
	}
	gOut := out.Column(0).(*array.Decimal128)
	sumCol := out.Column(1).(*array.Decimal128)
	minCol := out.Column(2).(*array.Decimal128)
	maxCol := out.Column(3).(*array.Decimal128)
	countCol := out.Column(4).(*array.Int64)

	if got := decNumToFloatWithScale(gOut.Value(0), scale); math.Abs(got-1.0) > 1e-9 {
		t.Errorf("group key[0] = %g, want 1", got)
	}
	if got := decNumToFloatWithScale(sumCol.Value(0), scale); math.Abs(got-3.0) > 1e-9 {
		t.Errorf("group g=1.00 sum = %g, want 3", got)
	}
	if got := decNumToFloatWithScale(minCol.Value(0), scale); math.Abs(got-1.0) > 1e-9 {
		t.Errorf("group g=1.00 min = %g, want 1", got)
	}
	if got := decNumToFloatWithScale(maxCol.Value(0), scale); math.Abs(got-2.0) > 1e-9 {
		t.Errorf("group g=1.00 max = %g, want 2", got)
	}
	if countCol.Value(0) != 2 {
		t.Errorf("group g=1.00 count = %d, want 2", countCol.Value(0))
	}

	if got := decNumToFloatWithScale(gOut.Value(1), scale); math.Abs(got-2.0) > 1e-9 {
		t.Errorf("group key[1] = %g, want 2", got)
	}
	if got := decNumToFloatWithScale(sumCol.Value(1), scale); math.Abs(got-7.0) > 1e-9 {
		t.Errorf("group g=2.00 sum = %g, want 7", got)
	}
	if got := decNumToFloatWithScale(minCol.Value(1), scale); math.Abs(got-3.0) > 1e-9 {
		t.Errorf("group g=2.00 min = %g, want 3", got)
	}
	if got := decNumToFloatWithScale(maxCol.Value(1), scale); math.Abs(got-4.0) > 1e-9 {
		t.Errorf("group g=2.00 max = %g, want 4", got)
	}
	if countCol.Value(1) != 2 {
		t.Errorf("group g=2.00 count = %d, want 2", countCol.Value(1))
	}
}

// buildRecord builds a Record with named Int64 columns from the given column
// data. nulls[i] (per column) marks null positions.
func buildRecord(alloc memory.Allocator, names []string, cols []arrow.Array, nrows int) arrow.Record {
	fields := make([]arrow.Field, len(cols))
	arrs := make([]arrow.Array, len(cols))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: cols[i].DataType(), Nullable: true}
		arrs[i] = cols[i]
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, arrs, int64(nrows))
}

// TestArrowHashAggregator exercises the grouped aggregate kernel (the pure-Arrow
// equivalent of C++ HashAggregateFunction).
func TestArrowHashAggregator(t *testing.T) {
	alloc := memory.DefaultAllocator
	ctx := context.Background()

	// Rows: (b=1, a=10), (b=1, a=20), (b=2, a=5), (b=2, a=null)
	bCol := int64Column(alloc, []int64{1, 1, 2, 2}, nil)
	aCol := int64Column(alloc, []int64{10, 20, 5, 0}, []bool{false, false, false, true})
	rec := buildRecord(alloc, []string{"b", "a"}, []arrow.Array{bCol, aCol}, 4)
	defer rec.Release()
	defer bCol.Release()
	defer aCol.Release()

	aggs := []ArrowAggExpr{
		{Func: "sum", Input: "a"},
		{Func: "count", Input: "a"},
		{Func: "count_all"},
		{Func: "min", Input: "a"},
		{Func: "max", Input: "a"},
	}
	ha := newArrowHashAggregator(alloc, []string{"b"}, aggs)
	if err := ha.Consume(ctx, rec); err != nil {
		t.Fatal(err)
	}
	out, err := ha.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", out.NumRows())
	}
	// out columns: b, sum, count, count_all, min, max
	sumCol := out.Column(1).(*array.Int64)
	countCol := out.Column(2).(*array.Int64)
	countAllCol := out.Column(3).(*array.Int64)
	minCol := out.Column(4).(*array.Int64)
	maxCol := out.Column(5).(*array.Int64)

	// Group order is first-seen: b=1 first, then b=2.
	if sumCol.Value(0) != 30 {
		t.Errorf("group b=1 sum = %d, want 30", sumCol.Value(0))
	}
	if countCol.Value(0) != 2 {
		t.Errorf("group b=1 count = %d, want 2", countCol.Value(0))
	}
	if countAllCol.Value(0) != 2 {
		t.Errorf("group b=1 count_all = %d, want 2", countAllCol.Value(0))
	}
	if minCol.Value(0) != 10 || maxCol.Value(0) != 20 {
		t.Errorf("group b=1 min/max = %d/%d, want 10/20", minCol.Value(0), maxCol.Value(0))
	}
	if sumCol.Value(1) != 5 {
		t.Errorf("group b=2 sum = %d, want 5", sumCol.Value(1))
	}
	if countCol.Value(1) != 1 {
		t.Errorf("group b=2 count = %d, want 1", countCol.Value(1))
	}
	if countAllCol.Value(1) != 2 {
		t.Errorf("group b=2 count_all = %d, want 2", countAllCol.Value(1))
	}
	// The only non-null value in group b=2 is 5 -> min = max = 5.
	if minCol.Value(1) != 5 || maxCol.Value(1) != 5 {
		t.Errorf("group b=2 min/max = %d/%d, want 5/5", minCol.Value(1), maxCol.Value(1))
	}
}

// TestArrowHashAggregatorGlobal verifies global (no group columns) aggregation.
func TestArrowHashAggregatorGlobal(t *testing.T) {
	alloc := memory.DefaultAllocator
	ctx := context.Background()

	aCol := int64Column(alloc, []int64{1, 2, 3}, nil)
	rec := buildRecord(alloc, []string{"a"}, []arrow.Array{aCol}, 3)
	defer rec.Release()
	defer aCol.Release()

	aggs := []ArrowAggExpr{
		{Func: "sum", Input: "a"},
		{Func: "count", Input: "a"},
		{Func: "count_all"},
		{Func: "mean", Input: "a"},
	}
	ha := newArrowHashAggregator(alloc, nil, aggs)
	if err := ha.Consume(ctx, rec); err != nil {
		t.Fatal(err)
	}
	out, err := ha.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.NumRows() != 1 {
		t.Fatalf("global aggregation should produce 1 row, got %d", out.NumRows())
	}
	sumCol := out.Column(0).(*array.Int64)
	countCol := out.Column(1).(*array.Int64)
	countAllCol := out.Column(2).(*array.Int64)
	meanCol := out.Column(3).(*array.Decimal128)
	if sumCol.Value(0) != 6 {
		t.Errorf("global sum = %d, want 6", sumCol.Value(0))
	}
	if countCol.Value(0) != 3 {
		t.Errorf("global count = %d, want 3", countCol.Value(0))
	}
	if countAllCol.Value(0) != 3 {
		t.Errorf("global count_all = %d, want 3", countAllCol.Value(0))
	}
	if got := decNumToFloat(meanCol.Value(0)); math.Abs(got-2.0) > 1e-9 {
		t.Errorf("global mean = %g, want 2", got)
	}
}

// TestArrowHashAggregatorEmptyGlobal verifies that aggregating an empty input
// still yields exactly one row (SQL semantics: COUNT(*) = 0, SUM/MIN/MAX/MEAN =
// NULL).
func TestArrowHashAggregatorEmptyGlobal(t *testing.T) {
	alloc := memory.DefaultAllocator
	ctx := context.Background()

	// An empty record still needs a column to derive the schema/type.
	b := array.NewInt64Builder(alloc)
	empty := b.NewArray()
	b.Release()
	rec := buildRecord(alloc, []string{"a"}, []arrow.Array{empty}, 0)
	empty.Release()
	defer rec.Release()

	aggs := []ArrowAggExpr{
		{Func: "sum", Input: "a"},
		{Func: "count", Input: "a"},
		{Func: "count_all"},
	}
	ha := newArrowHashAggregator(alloc, nil, aggs)
	if err := ha.Consume(ctx, rec); err != nil {
		t.Fatal(err)
	}
	out, err := ha.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.NumRows() != 1 {
		t.Fatalf("empty global aggregation should produce 1 row, got %d", out.NumRows())
	}
	countCol := out.Column(1).(*array.Int64)
	countAllCol := out.Column(2).(*array.Int64)
	if countCol.Value(0) != 0 {
		t.Errorf("empty count = %d, want 0", countCol.Value(0))
	}
	if countAllCol.Value(0) != 0 {
		t.Errorf("empty count_all = %d, want 0", countAllCol.Value(0))
	}
	if out.Column(0).IsNull(0) == false {
		t.Errorf("empty sum should be null")
	}
}

// TestAggregateArrayTimestamp exercises the scalar aggregate kernel over a
// TIMESTAMP (arrow.Timestamp, microsecond) input column: MIN/MAX compare the
// underlying instant, COUNT is type-agnostic.
func TestAggregateArrayTimestamp(t *testing.T) {
	alloc := memory.DefaultAllocator
	// micros: 1000, 2000, 3000
	col := timestampColumn(alloc, []int64{1000, 2000, 3000}, nil)
	defer col.Release()

	mn, err := aggregateArray("min", col)
	if err != nil {
		t.Fatal(err)
	}
	mx, err := aggregateArray("max", col)
	if err != nil {
		t.Fatal(err)
	}
	cnt, err := aggregateArray("count", col)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustScalarTimestamp(t, mn); got != 1000 {
		t.Errorf("min(ts) = %d, want 1000", got)
	}
	if got := mustScalarTimestamp(t, mx); got != 3000 {
		t.Errorf("max(ts) = %d, want 3000", got)
	}
	if got := mustScalarInt64(t, cnt); got != 3 {
		t.Errorf("count(ts) = %d, want 3", got)
	}

	// nulls must be skipped by MIN/MAX/COUNT.
	colN := timestampColumn(alloc, []int64{1000, 2000}, []bool{true, false})
	defer colN.Release()
	mnN, err := aggregateArray("min", colN)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustScalarTimestamp(t, mnN); got != 2000 {
		t.Errorf("min(ts) over {null,2000} = %d, want 2000", got)
	}
}

// TestArrowHashAggregatorTimestamp exercises grouped aggregation keyed by a
// TIMESTAMP column (the timestamp group-key path), with SUM/COUNT over an int
// value column and pass-through MIN/MAX over the timestamp key.
func TestArrowHashAggregatorTimestamp(t *testing.T) {
	alloc := memory.DefaultAllocator
	// group key ts: t1, t1, t2 ; value a: 10, 20, 30
	tsCol := timestampColumn(alloc, []int64{1000, 1000, 2000}, nil)
	aCol := int64Column(alloc, []int64{10, 20, 30}, nil)
	rec := buildRecord(alloc, []string{"ts", "a"}, []arrow.Array{tsCol, aCol}, 3)
	defer rec.Release()
	defer tsCol.Release()
	defer aCol.Release()

	aggs := []ArrowAggExpr{
		{Func: "sum", Input: "a"},
		{Func: "count", Input: "a"},
		{Func: "min", Input: "ts"},
		{Func: "max", Input: "ts"},
	}
	ha := newArrowHashAggregator(alloc, []string{"ts"}, aggs)
	if err := ha.Consume(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	out, err := ha.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// Group order is first-seen: ts=1000 then ts=2000.
	if out.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", out.NumRows())
	}
	tsOut := out.Column(0).(*array.Timestamp)
	sumCol := out.Column(1).(*array.Int64)
	countCol := out.Column(2).(*array.Int64)
	minCol := out.Column(3).(*array.Timestamp)
	maxCol := out.Column(4).(*array.Timestamp)

	if got := int64(tsOut.Value(0)); got != 1000 {
		t.Errorf("group key[0] = %d, want 1000", got)
	}
	if got := sumCol.Value(0); got != 30 {
		t.Errorf("group ts=1000 sum = %d, want 30", got)
	}
	if got := countCol.Value(0); got != 2 {
		t.Errorf("group ts=1000 count = %d, want 2", got)
	}
	if got := int64(minCol.Value(0)); got != 1000 {
		t.Errorf("group ts=1000 min = %d, want 1000", got)
	}
	if got := int64(maxCol.Value(0)); got != 1000 {
		t.Errorf("group ts=1000 max = %d, want 1000", got)
	}

	if got := int64(tsOut.Value(1)); got != 2000 {
		t.Errorf("group key[1] = %d, want 2000", got)
	}
	if got := sumCol.Value(1); got != 30 {
		t.Errorf("group ts=2000 sum = %d, want 30", got)
	}
	if got := countCol.Value(1); got != 1 {
		t.Errorf("group ts=2000 count = %d, want 1", got)
	}
	if got := int64(minCol.Value(1)); got != 2000 {
		t.Errorf("group ts=2000 min = %d, want 2000", got)
	}
	if got := int64(maxCol.Value(1)); got != 2000 {
		t.Errorf("group ts=2000 max = %d, want 2000", got)
	}
}
