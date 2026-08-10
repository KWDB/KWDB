// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestArrowWindowerRangeOffsetEndToEnd exercises the full Arrow windower
// execution path for a RANGE offset frame (RANGE BETWEEN 1 PRECEDING AND 1
// FOLLOWING). It verifies two previously broken behaviours:
//
//  1. Partition handling / ordering: the upstream stream is only guaranteed to
//     be ordered by PARTITION BY, not by the window ORDER BY. computePartition
//     must sort each partition in place before evaluation; otherwise a RANGE
//     offset frame (which binary-searches the order column) degrades to a
//     single row / produces wrong, partition-crossing results. The input rows
//     below are intentionally *not* sorted by the order column within a
//     partition, so a missing partition sort would fail this test.
//  2. Value-based frame semantics: a RANGE offset frame spans every row whose
//     ORDER BY value lies within [cur-offset, cur+offset], independent of the
//     number of physical rows. Peer rows with equal order values all fall
//     inside each other's frame.
func TestArrowWindowerRangeOffsetEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	da := &sqlbase.DatumAlloc{}

	// inTypes: col0 = grp (partition), col1 = val (order + sum input).
	inTypes := []types.T{*types.Int, *types.Int}

	// Build a plan: PARTITION BY grp, SUM(val) RANGE BETWEEN 1 PRECEDING AND
	// 1 FOLLOWING ordered by val.
	plan := arrowWindowerPlan{
		PartitionBy: []int{0},
		Fns: []arrowWindowFnPlan{{
			Kind:     "agg",
			Func:     "sum",
			Input:    1,
			Ordering: []int{1},
			OutputIdx: 2,
			Frame: &arrowWindowFramePlan{
				Mode:        "range",
				Start:       "offset_preceding",
				End:         "offset_following",
				StartOffset: 1,
				EndOffset:   1,
			},
		}},
	}

	// Input rows: (grp, val). Partition A values intentionally out of order:
	// grp=1 -> val 3,1,5,2. Partition B has a duplicate peer value (10,10):
	// grp=2 -> val 10,10,12.
	rows := sqlbase.EncDatumRows{
		{sqlbase.EncDatum{Datum: tree.NewDInt(1)}, sqlbase.EncDatum{Datum: tree.NewDInt(3)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(1)}, sqlbase.EncDatum{Datum: tree.NewDInt(1)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(1)}, sqlbase.EncDatum{Datum: tree.NewDInt(5)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(1)}, sqlbase.EncDatum{Datum: tree.NewDInt(2)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(2)}, sqlbase.EncDatum{Datum: tree.NewDInt(10)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(2)}, sqlbase.EncDatum{Datum: tree.NewDInt(10)}},
		{sqlbase.EncDatum{Datum: tree.NewDInt(2)}, sqlbase.EncDatum{Datum: tree.NewDInt(12)}},
	}

	// out = copy of each input row plus one output column for the aggregate.
	outTypes := []types.T{*types.Int, *types.Int, *types.Int}
	out := make(sqlbase.EncDatumRows, len(rows))
	for i, row := range rows {
		nr := make(sqlbase.EncDatumRow, 3)
		copy(nr, row)
		out[i] = nr
	}

	p := &arrowWindowerProcessor{
		da:       da,
		plan:     plan,
		inTypes:  inTypes,
		outTypes: outTypes,
	}
	// EvalCtx is promoted from the embedded execinfra.ProcessorBase; it must be
	// set directly because it cannot appear in the struct literal of the outer
	// type. computePartition only needs EvalCtx/outTypes/da/inTypes/plan, none
	// of which require the processor to be fully initialised.
	p.ProcessorBase.EvalCtx = &evalCtx

	// Evaluate partition by partition (rows are already ordered by partition).
	start := 0
	for end := 1; end <= len(rows); end++ {
		if end == len(rows) || !samePartition(p, rows[start], rows[end]) {
			p.computePartition(out, rows[start:end], start)
			start = end
		}
	}

	// Expected SUM over the RANGE 1 PRECEDING..1 FOLLOWING frame, grouped by
	// partition and matched to the post-sort (val-ascending) order:
	//   Partition 1 (val order 1,2,3,5):
	//     val 1 -> [1,2]             sum 1+2 = 3
	//     val 2 -> [1,3]             sum 1+2+3 = 6
	//     val 3 -> [2,4]             sum 2+3 = 5
	//     val 5 -> [4,6]             sum 5 = 5
	//   Partition 2 (val order 10,10,12):
	//     val 10 -> [9,11]           sum 10+10 = 20 (both peer rows)
	//     val 10 -> [9,11]           sum 10+10 = 20
	//     val 12 -> [11,13]          sum 12 = 12
	want := []int64{3, 6, 5, 5, 20, 20, 12}
	get := func(r sqlbase.EncDatumRow) int64 {
		if err := r[2].EnsureDecoded(&outTypes[2], da); err != nil {
			t.Fatalf("decode output: %v", err)
		}
		d, ok := r[2].Datum.(*tree.DInt)
		if !ok {
			t.Fatalf("output datum %v is not *tree.DInt", r[2].Datum)
		}
		return int64(*d)
	}
	for i := range out {
		if got := get(out[i]); got != want[i] {
			t.Errorf("row %d: got sum %d, want %d (rows=%v)", i, got, want[i], rows[i])
		}
	}
}
