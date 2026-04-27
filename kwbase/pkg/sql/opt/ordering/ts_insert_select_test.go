// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ordering

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

// TestTSInsertSelectCanProvideOrdering tests the tsInsertSelectCanProvideOrdering function
func TestTSInsertSelectCanProvideOrdering(t *testing.T) {
	tests := []struct {
		name     string
		required *physical.OrderingChoice
		want     bool
	}{
		{
			name:     "non-empty required ordering",
			required: &physical.OrderingChoice{Columns: []physical.OrderingColumnChoice{{Group: opt.MakeColSet(1)}}},
			want:     true,
		},
		{
			name:     "empty required ordering",
			required: &physical.OrderingChoice{},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic directly
			got := tsInsertSelectCanProvideOrdering(nil, tt.required) // tsInsertSelectCanProvideOrdering always returns true
			if got != tt.want {
				t.Errorf("tsInsertSelectCanProvideOrdering() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTSInsertSelectBuildChildReqOrdering tests the tsInsertSelectBuildChildReqOrdering function
func TestTSInsertSelectBuildChildReqOrdering(t *testing.T) {
	tests := []struct {
		name     string
		required *physical.OrderingChoice
		childIdx int
		want     physical.OrderingChoice
	}{
		{
			name: "non-empty required ordering",
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1), Descending: false},
					{Group: opt.MakeColSet(2), Descending: true},
				},
			},
			childIdx: 0,
			want: physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1), Descending: false},
					{Group: opt.MakeColSet(2), Descending: true},
				},
			},
		},
		{
			name:     "empty required ordering",
			required: &physical.OrderingChoice{},
			childIdx: 0,
			want:     physical.OrderingChoice{},
		},
		{
			name: "child index not 0",
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{{Group: opt.MakeColSet(1)}},
			},
			childIdx: 1,
			want: physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{{Group: opt.MakeColSet(1)}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic directly
			got := tsInsertSelectBuildChildReqOrdering(nil, tt.required, tt.childIdx)
			if len(got.Columns) != len(tt.want.Columns) {
				t.Errorf("tsInsertSelectBuildChildReqOrdering() column count = %d, want %d", len(got.Columns), len(tt.want.Columns))
				return
			}
			for i, col := range got.Columns {
				if !col.Group.Equals(tt.want.Columns[i].Group) || col.Descending != tt.want.Columns[i].Descending {
					t.Errorf("tsInsertSelectBuildChildReqOrdering() column %d = %v, want %v", i, col, tt.want.Columns[i])
				}
			}
		})
	}
}
