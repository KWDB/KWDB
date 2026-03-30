// Copyright 2019 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package opt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckOptMode(t *testing.T) {
	tests := []struct {
		name    string
		csValue int64
		mode    int
		want    bool
	}{
		{"valid mode", 111110, 1, false},
		{"invalid csValue", 123456, 1, false},
		{"mode not set", 111110, 16, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckOptMode(tt.csValue, tt.mode)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOrderedTableType_Ordered(t *testing.T) {
	tests := []struct {
		name string
		typ  OrderedTableType
		want bool
	}{
		{"NoOrdered", NoOrdered, false},
		{"OrderedScan", OrderedScan, true},
		{"SortAfterScan", SortAfterScan, true},
		{"ForceOrderedScan", ForceOrderedScan, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.typ.Ordered()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOrderedTableType_UserOrderedScan(t *testing.T) {
	tests := []struct {
		name string
		typ  OrderedTableType
		want bool
	}{
		{"NoOrdered", NoOrdered, false},
		{"OrderedScan", OrderedScan, true},
		{"SortAfterScan", SortAfterScan, false},
		{"ForceOrderedScan", ForceOrderedScan, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.typ.UserOrderedScan()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOrderedTableType_NeedReverse(t *testing.T) {
	tests := []struct {
		name string
		typ  OrderedTableType
		want bool
	}{
		{"NoOrdered", NoOrdered, false},
		{"OrderedScan", OrderedScan, true},
		{"SortAfterScan", SortAfterScan, false},
		{"ForceOrderedScan", ForceOrderedScan, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.typ.NeedReverse()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_TimeBucketOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no TimeBucketPushAgg", 0, false},
		{"with TimeBucketPushAgg", TimeBucketPushAgg, true},
		{"with other opts", PruneLocalAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.TimeBucketOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_PushLocalAggToScanOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no opts", 0, false},
		{"with TimeBucketPushAgg", TimeBucketPushAgg, true},
		{"with UseStatistic", UseStatistic, true},
		{"with other opts", PruneLocalAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.PushLocalAggToScanOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_PruneLocalAggOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no PruneLocalAgg", 0, false},
		{"with PruneLocalAgg", PruneLocalAgg, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.PruneLocalAggOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_PruneFinalAggOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no PruneFinalAgg", 0, false},
		{"with PruneFinalAgg", PruneFinalAgg, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.PruneFinalAggOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_PruneTSFinalAggOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no PruneTSFinalAgg", 0, false},
		{"with PruneTSFinalAgg", PruneTSFinalAgg, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.PruneTSFinalAggOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_WithSumInt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no ScalarGroupByWithSumInt", 0, false},
		{"with ScalarGroupByWithSumInt", ScalarGroupByWithSumInt, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.WithSumInt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_UseStatisticOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no UseStatistic", 0, false},
		{"with UseStatistic", UseStatistic, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.UseStatisticOpt()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_CanApplyInsideOut(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no ApplyInsideOut", 0, false},
		{"with ApplyInsideOut", ApplyInsideOut, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.CanApplyInsideOut()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupOptType_CanApplyOutsideIn(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOptType
		want bool
	}{
		{"no ApplyOutsideIn", 0, false},
		{"with ApplyOutsideIn", ApplyOutsideIn, true},
		{"with other opts", TimeBucketPushAgg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.CanApplyOutsideIn()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLimitOptType_TSPushLimitToAggScan(t *testing.T) {
	tests := []struct {
		name string
		opt  LimitOptType
		want bool
	}{
		{"no TSPushLimitToAggScan", 0, false},
		{"with TSPushLimitToAggScan", TSPushLimitToAggScan, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opt.TSPushLimitToAggScan()
			require.Equal(t, tt.want, got)
		})
	}
}
