// Copyright 2024 KWDB Contributors. All rights reserved.
// Use of this source code is governed by a license that can be found in the
// LICENSE file.

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
)

// TestIsSupportedWindowFrameRangeOffset verifies that the Arrow windower now
// accepts RANGE offset frames (RANGE BETWEEN n PRECEDING AND m FOLLOWING),
// which were previously bounced to the classic path due to the partition-sort
// bug in the Arrow windower's offset-frame execution.
func TestIsSupportedWindowFrameRangeOffset(t *testing.T) {
	rangeOffset := func(startOff, endOff int64) *execinfrapb.WindowerSpec_Frame {
		return &execinfrapb.WindowerSpec_Frame{
			Mode: execinfrapb.WindowerSpec_Frame_RANGE,
			Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
				Start: execinfrapb.WindowerSpec_Frame_Bound{
					BoundType: execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING,
					IntOffset: uint64(startOff),
				},
				End: &execinfrapb.WindowerSpec_Frame_Bound{
					BoundType: execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING,
					IntOffset: uint64(endOff),
				},
			},
		}
	}
	rowsOffset := func(startOff, endOff int64) *execinfrapb.WindowerSpec_Frame {
		f := rangeOffset(startOff, endOff)
		f.Mode = execinfrapb.WindowerSpec_Frame_ROWS
		return f
	}
	groups := &execinfrapb.WindowerSpec_Frame{
		Mode: execinfrapb.WindowerSpec_Frame_GROUPS,
		Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
			Start: execinfrapb.WindowerSpec_Frame_Bound{BoundType: execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING},
			End:   &execinfrapb.WindowerSpec_Frame_Bound{BoundType: execinfrapb.WindowerSpec_Frame_CURRENT_ROW},
		},
	}
	noEnd := &execinfrapb.WindowerSpec_Frame{
		Mode: execinfrapb.WindowerSpec_Frame_ROWS,
		Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
			Start: execinfrapb.WindowerSpec_Frame_Bound{BoundType: execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING},
		},
	}

	cases := []struct {
		name string
		f    *execinfrapb.WindowerSpec_Frame
		want bool
	}{
		{"range offset", rangeOffset(1, 1), true},
		{"range offset following only", rangeOffset(0, 2), true},
		{"rows offset", rowsOffset(1, 1), true},
		{"groups mode", groups, false},
		{"missing end bound", noEnd, false},
	}
	for _, c := range cases {
		if got := isSupportedWindowFrame(c.f); got != c.want {
			t.Errorf("%s: isSupportedWindowFrame = %v, want %v", c.name, got, c.want)
		}
	}
}
