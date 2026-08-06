// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package arrowpilot

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// TestArrowProjectionNumericFuncs verifies that the Arrow projection executor
// routes one- and two-argument numeric scalar functions through the vendored
// arrow/compute kernels (abs/sqrt/ln/sign/power) without a dedicated Go kernel.
// It is a pure unit test (no cluster), mirroring TestArrowProjectionPilot.
func TestArrowProjectionNumericFuncs(t *testing.T) {
	ctx := context.Background()
	alloc := memory.NewGoAllocator()

	floatTyp := types.Float
	mk := func(v float64) sqlbase.EncDatum { return sqlbase.DatumToEncDatum(floatTyp, tree.NewDFloat(tree.DFloat(v))) }

	// col0: positive/negative floats; col1: bases for power.
	rows := sqlbase.EncDatumRows{
		{mk(-4.0), mk(2.0)},
		{mk(9.0), mk(3.0)},
		{mk(-2.5), mk(0.5)},
		{mk(0.0), mk(4.0)},
	}

	conv := rowexec.NewRowToArrowConverter(alloc, []*types.T{floatTyp, floatTyp}, rows)
	conv.Init(ctx)
	rec, done, err := conv.Next(ctx)
	if err != nil {
		t.Fatalf("rowToArrow: %v", err)
	}
	if done || rec == nil {
		t.Fatal("expected one record from converter")
	}
	// The initial probe Record is not consumed here; release it. Each kernel is
	// re-run from a fresh Record inside runCol.
	rec.Release()

	// The projection executor emits one Record per Next() call, so to assert on
	// a single named function we build a fresh one-column projection per kernel
	// from a fresh Record sourced from the same rows.
	runCol := func(funcName string, args []rowexec.ArrowArg) []float64 {
		conv2 := rowexec.NewRowToArrowConverter(alloc, []*types.T{floatTyp, floatTyp}, rows)
		conv2.Init(ctx)
		rec2, _, _ := conv2.Next(ctx)
		src2 := rowexec.NewArrowRecordSource(alloc, rec2)
		p := rowexec.NewArrowProjection(alloc, src2, []rowexec.ArrowProjectionSpec{
			{OutputName: "out", Func: funcName, Args: args},
		}, nil)
		p.Init(ctx)
		v, err := rowexec.ArrowProjectionResultFloat64(p, ctx)
		if err != nil {
			t.Fatalf("projection %s: %v", funcName, err)
		}
		return v
	}

	absWant := []float64{4.0, 9.0, 2.5, 0.0}
	got := runCol("abs", []rowexec.ArrowArg{{ColName: "col0"}})
	assertFloatApprox(t, got, absWant)

	// sqrt on col0 would be NaN for negatives; test sqrt on abs(col0) instead via a
	// positive column: use col1.
	sqrtWant := []float64{math.Sqrt(2.0), math.Sqrt(3.0), math.Sqrt(0.5), math.Sqrt(4.0)}
	got = runCol("sqrt", []rowexec.ArrowArg{{ColName: "col1"}})
	assertFloatApprox(t, got, sqrtWant)

	signWant := []float64{-1.0, 1.0, -1.0, 0.0}
	got = runCol("sign", []rowexec.ArrowArg{{ColName: "col0"}})
	assertFloatApprox(t, got, signWant)

	// power(col1, col1)
	powerWant := []float64{math.Pow(2.0, 2.0), math.Pow(3.0, 3.0), math.Pow(0.5, 0.5), math.Pow(4.0, 4.0)}
	got = runCol("power", []rowexec.ArrowArg{{ColName: "col1"}, {ColName: "col1"}})
	assertFloatApprox(t, got, powerWant)

	lnWant := []float64{math.Log(2.0), math.Log(3.0), math.Log(0.5), math.Log(4.0)}
	got = runCol("ln", []rowexec.ArrowArg{{ColName: "col1"}})
	assertFloatApprox(t, got, lnWant)
}

// assertFloatApprox compares two float slices with a small tolerance.
func assertFloatApprox(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Fatalf("row %d: want %v, got %v", i, want[i], got[i])
		}
	}
}

// keep math import used.
var _ = math.Pow
