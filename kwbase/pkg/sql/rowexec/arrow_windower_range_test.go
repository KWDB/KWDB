// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package rowexec

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestArrowWindowerRangeFrameBounds verifies the value-based RANGE offset frame
// computation: a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING frame over an ordered
// integer column must include every row whose ORDER BY value lies within
// [cur-1, cur+1], NOT a fixed number of physical rows. This is the semantics
// that distinguishes RANGE offset frames from ROWS offset frames.
//
// Note: this exercises the frame-bounds arithmetic directly. The end-to-end
// path is covered by TestArrowWindowerRangeOffsetEndToEnd, which feeds rows
// through computePartition and asserts the partition-sorted, value-based
// RANGE frame results.
func TestArrowWindowerRangeFrameBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	vals := []int64{1, 2, 3, 5}
	orderVals := make([]tree.Datum, len(vals))
	for i, v := range vals {
		orderVals[i] = tree.NewDInt(tree.DInt(v))
	}
	f := &arrowWindowFramePlan{
		Mode:        "range",
		Start:       "offset_preceding",
		End:         "offset_following",
		StartOffset: 1,
		EndOffset:   1,
	}
	// Expected frame [start,end] inclusive per row (RANGE value-based):
	//   value 1 -> [1,2]   -> indices [0,1]  (values 1,2)
	//   value 2 -> [1,3]   -> indices [0,2]  (values 1,2,3)
	//   value 3 -> [2,4]   -> indices [1,2]  (values 2,3)
	//   value 5 -> [4,6]   -> indices [3,3]  (value 5 only)
	want := [][2]int{{0, 1}, {0, 2}, {1, 2}, {3, 3}}
	for i := range vals {
		s, e := rangeFrameBounds(i, len(vals), orderVals, nil, f)
		if s != want[i][0] || e != want[i][1] {
			t.Errorf("row %d (val %d): got [%d,%d] want [%d,%d]", i, vals[i], s, e, want[i][0], want[i][1])
		}
	}

	// Integer offset arithmetic sanity: offsetDatum cur=3, off=1, sign=-1 -> 2.
	if got := offsetDatum(tree.NewDInt(tree.DInt(3)), 1, -1); got == nil {
		t.Fatal("offsetDatum returned nil for int")
	} else if d, ok := got.(*tree.DInt); !ok || int64(*d) != 2 {
		t.Errorf("offsetDatum(3, -1) = %v, want 2", got)
	}
}
