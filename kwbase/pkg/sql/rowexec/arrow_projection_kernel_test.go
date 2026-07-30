// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// TestArrowProjectionStringKernels exercises the native string-function kernels
// (length/lower/upper/concat/substring) directly against the executor's arrow
// projection machinery. arrow/compute v17 ships no string kernels, so these are
// evaluated as Go vectorized loops; this test pins the column/constant handling,
// NULL propagation, and rune-aware length/substring semantics.
func TestArrowProjectionStringKernels(t *testing.T) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	c0 := func() arrow.Array {
		b := array.NewStringBuilder(alloc)
		b.Append("Hello")
		b.Append("World")
		a := b.NewArray()
		b.Release()
		return a
	}()
	c1 := func() arrow.Array {
		b := array.NewStringBuilder(alloc)
		b.Append("foo")
		b.Append("BAR")
		a := b.NewArray()
		b.Release()
		return a
	}()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col0", Type: arrow.BinaryTypes.String},
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{c0, c1}, 2)
	defer rec.Release()

	p := &arrowProjection{alloc: alloc}

	constStr := func(s string) compute.Datum { return compute.NewDatum(s) }
	constInt := func(i int64) compute.Datum { return compute.NewDatum(i) }

	cases := []struct {
		fn       string
		args     []ArrowArg
		wantStr  []string
		wantInt  []int64
		wantNull []bool
	}{
		{"length", []ArrowArg{{ColName: "col0"}}, nil, []int64{5, 5}, nil},
		{"octet_length", []ArrowArg{{ColName: "col0"}}, nil, []int64{5, 5}, nil},
		{"lower", []ArrowArg{{ColName: "col0"}}, []string{"hello", "world"}, nil, nil},
		{"upper", []ArrowArg{{ColName: "col0"}}, []string{"HELLO", "WORLD"}, nil, nil},
		{"concat", []ArrowArg{{ColName: "col0"}, {ColName: "col1"}}, []string{"Hellofoo", "WorldBAR"}, nil, nil},
		{"concat", []ArrowArg{{ColName: "col0"}, {Scalar: constStr("-x")}}, []string{"Hello-x", "World-x"}, nil, nil},
		{"substring", []ArrowArg{{ColName: "col0"}, {Scalar: constInt(2)}, {Scalar: constInt(3)}}, []string{"ell", "orl"}, nil, nil},
		// NULL propagation: concat with a NULL constant yields NULL.
		{"concat", []ArrowArg{{ColName: "col0"}, {Scalar: compute.NewDatum(nil)}}, nil, nil, []bool{true, true}},
	}

	for _, c := range cases {
		arr, err := p.evalArrowStringFunc(ctx, rec, ArrowProjectionSpec{Func: c.fn, Args: c.args})
		if err != nil {
			t.Fatalf("%s: %v", c.fn, err)
		}
		if c.wantNull != nil {
			sa, ok := arr.(*array.String)
			if !ok {
				t.Fatalf("%s: expected String result, got %T", c.fn, arr)
			}
			for i, w := range c.wantNull {
				if sa.IsNull(i) != w {
					t.Errorf("%s row %d: IsNull=%v want %v", c.fn, i, sa.IsNull(i), w)
				}
			}
		} else if c.wantInt != nil {
			ia, ok := arr.(*array.Int64)
			if !ok {
				t.Fatalf("%s: expected Int64 result, got %T", c.fn, arr)
			}
			for i, w := range c.wantInt {
				if ia.Value(i) != w {
					t.Errorf("%s row %d: got %d want %d", c.fn, i, ia.Value(i), w)
				}
			}
		} else {
			sa, ok := arr.(*array.String)
			if !ok {
				t.Fatalf("%s: expected String result, got %T", c.fn, arr)
			}
			for i, w := range c.wantStr {
				if sa.Value(i) != w {
					t.Errorf("%s row %d: got %q want %q", c.fn, i, sa.Value(i), w)
				}
			}
		}
		arr.Release()
	}

	// Rune-aware checks: non-ASCII input must count/substring by runes, not
	// bytes. "Émily" is 5 runes but 6 UTF-8 bytes.
	c2 := func() arrow.Array {
		b := array.NewStringBuilder(alloc)
		b.Append("Émily")
		b.Append("naïve")
		a := b.NewArray()
		b.Release()
		return a
	}()
	rec2 := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "col2", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{c2}, 2)
	defer rec2.Release()

	runeCases := []struct {
		fn      string
		args    []ArrowArg
		wantStr []string
		wantInt []int64
	}{
		{"length", []ArrowArg{{ColName: "col2"}}, nil, []int64{5, 5}},
		{"lower", []ArrowArg{{ColName: "col2"}}, []string{"émily", "naïve"}, nil},
		{"upper", []ArrowArg{{ColName: "col2"}}, []string{"ÉMILY", "NAÏVE"}, nil},
		{"substring", []ArrowArg{{ColName: "col2"}, {Scalar: constInt(1)}, {Scalar: constInt(2)}}, []string{"Ém", "na"}, nil},
	}
	for _, c := range runeCases {
		arr, err := p.evalArrowStringFunc(ctx, rec2, ArrowProjectionSpec{Func: c.fn, Args: c.args})
		if err != nil {
			t.Fatalf("%s (rune): %v", c.fn, err)
		}
		if c.wantInt != nil {
			ia, ok := arr.(*array.Int64)
			if !ok {
				t.Fatalf("%s (rune): expected Int64, got %T", c.fn, arr)
			}
			for i, w := range c.wantInt {
				if ia.Value(i) != w {
					t.Errorf("%s (rune) row %d: got %d want %d", c.fn, i, ia.Value(i), w)
				}
			}
		} else {
			sa, ok := arr.(*array.String)
			if !ok {
				t.Fatalf("%s (rune): expected String, got %T", c.fn, arr)
			}
			for i, w := range c.wantStr {
				if sa.Value(i) != w {
					t.Errorf("%s (rune) row %d: got %q want %q", c.fn, i, sa.Value(i), w)
				}
			}
		}
		arr.Release()
	}
}
