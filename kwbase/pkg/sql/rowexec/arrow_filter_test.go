// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestArrowFilterNonStringLeftLike exercises the LIKE path with a non-string
// (integer) left operand. The planner wraps such an operand in a CAST to STRING;
// evalLike must stringify each value before matching. This locks the casted
// left-operand behavior that the distributed/Arrow filter relies on for
// `col LIKE '2%'` on non-string columns.
func TestArrowFilterNonStringLeftLike(t *testing.T) {
	defer leaktest.AfterTest(t)()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	ctx := context.Background()

	b := array.NewInt64Builder(alloc)
	b.AppendValues([]int64{10, 20, 30, 40}, nil)
	col := b.NewArray()
	b.Release()
	defer col.Release()
	rec := array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: "col0", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{col},
		4,
	)
	defer rec.Release()

	// LIKE '2%' over the int column => each value cast to "10","20","30","40";
	// only 20 matches.
	spec := ArrowFilterSpec{
		Func: "like",
		Operands: []ArrowFilterOperand{
			{Leaf: &ArrowArg{
				ColName: "col0",
				Cast:    &ArrowArgCast{Type: arrow.BinaryTypes.String, Arg: ArrowArg{ColName: "col0"}},
			}},
			{Leaf: &ArrowArg{Scalar: &compute.ScalarDatum{Value: scalar.NewStringScalar("2%")}}},
		},
	}
	f := &arrowFilterCore{alloc: alloc}
	out, err := f.evalLike(ctx, rec, spec)
	if err != nil {
		t.Fatalf("evalLike: %v", err)
	}
	defer out.Release()
	want := []bool{false, true, false, false}
	for i := 0; i < out.Len(); i++ {
		if out.IsNull(i) {
			t.Fatalf("row %d: unexpected null", i)
		}
		if got := out.Value(i); got != want[i] {
			t.Errorf("row %d: got %v, want %v", i, got, want[i])
		}
	}
}

// TestLikeMatch locks the SQL LIKE semantics implemented by likeMatch:
// '%' matches any sequence, '_' matches exactly one character, and the
// default escape '\' quotes the next character.
func TestLikeMatch(t *testing.T) {
	cases := []struct {
		s, pat string
		ci     bool
		want   bool
	}{
		{"alice", "a%", false, true},
		{"alice", "a%e", false, true},
		{"alice", "b%", false, false},
		{"alice", "a__i%", false, false}, // too few chars before 'i'
		{"alice", "a_ice", false, true},
		{"alice", "_____", false, true}, // exactly five chars
		{"alice", "______", false, false},
		{"alice", "%i%", false, true},
		{"alice", "%x%", false, false},
		{"alice", "ALICE", false, false},
		{"alice", "ALICE", true, true}, // case-insensitive
		{"alice", "A%", true, true},
		{"100", "1%", false, true},
		{"a%b", `a\%b`, false, true}, // escaped '%' is a literal
		{"axb", `a\%b`, false, false},
		{"a_b", `a\_b`, false, true}, // escaped '_' is a literal
		{"axyb", `a\_b`, false, false},
		{"", "%", false, true}, // empty matches '%'
		{"", "a%", false, false},
		{"a", "_", false, true},
	}
	for _, c := range cases {
		if got := likeMatch(c.s, c.pat, c.ci); got != c.want {
			t.Errorf("likeMatch(%q, %q, ci=%v) = %v, want %v", c.s, c.pat, c.ci, got, c.want)
		}
	}
}
