// Copyright 2017 The Cockroach Authors.
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

package execinfra

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestLimitHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name          string
		specLimitHint int64
		post          *execinfrapb.PostProcessSpec
		expectedLimit int64
	}{
		{
			name:          "no limits",
			specLimitHint: 0,
			post:          &execinfrapb.PostProcessSpec{},
			expectedLimit: 0,
		},
		{
			name:          "post limit set",
			specLimitHint: 100,
			post:          &execinfrapb.PostProcessSpec{Limit: 50},
			expectedLimit: 50,
		},
		{
			name:          "spec limit set, no post limit",
			specLimitHint: 100,
			post:          &execinfrapb.PostProcessSpec{},
			expectedLimit: 100 + RowChannelBufSize + 1,
		},
		{
			name:          "post limit with filter",
			specLimitHint: 100,
			post:          &execinfrapb.PostProcessSpec{Limit: 50, Filter: execinfrapb.Expression{Expr: "true"}},
			expectedLimit: 100, // 50 * 2
		},
		{
			name:          "spec limit with filter",
			specLimitHint: 100,
			post:          &execinfrapb.PostProcessSpec{Filter: execinfrapb.Expression{Expr: "true"}},
			expectedLimit: (100 + RowChannelBufSize + 1) * 2,
		},
		{
			name:          "post limit exceeds overflow protection",
			specLimitHint: 100,
			post:          &execinfrapb.PostProcessSpec{Limit: readerOverflowProtection + 1},
			expectedLimit: 100 + RowChannelBufSize + 1,
		},
		{
			name:          "spec limit exceeds overflow protection",
			specLimitHint: readerOverflowProtection + 1,
			post:          &execinfrapb.PostProcessSpec{},
			expectedLimit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LimitHint(tt.specLimitHint, tt.post)
			if result != tt.expectedLimit {
				t.Errorf("Expected limit hint %d, got %d", tt.expectedLimit, result)
			}
		})
	}
}

func TestMisplannedRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name        string
		rangeInfos  []roachpb.RangeInfo
		nodeID      roachpb.NodeID
		expectedLen int
	}{
		{
			name: "no misplanned ranges",
			rangeInfos: []roachpb.RangeInfo{
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 1}},
					Desc:  roachpb.RangeDescriptor{RangeID: 1},
				},
			},
			nodeID:      1,
			expectedLen: 0,
		},
		{
			name: "some misplanned ranges",
			rangeInfos: []roachpb.RangeInfo{
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 1}},
					Desc:  roachpb.RangeDescriptor{RangeID: 1},
				},
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 2}},
					Desc:  roachpb.RangeDescriptor{RangeID: 2},
				},
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 3}},
					Desc:  roachpb.RangeDescriptor{RangeID: 3},
				},
			},
			nodeID:      1,
			expectedLen: 2,
		},
		{
			name: "all misplanned ranges",
			rangeInfos: []roachpb.RangeInfo{
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 2}},
					Desc:  roachpb.RangeDescriptor{RangeID: 1},
				},
				{
					Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{NodeID: 3}},
					Desc:  roachpb.RangeDescriptor{RangeID: 2},
				},
			},
			nodeID:      1,
			expectedLen: 2,
		},
		{
			name:        "empty range info",
			rangeInfos:  []roachpb.RangeInfo{},
			nodeID:      1,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MisplannedRanges(context.Background(), tt.rangeInfos, tt.nodeID)
			if len(result) != tt.expectedLen {
				t.Errorf("Expected %d misplanned ranges, got %d", tt.expectedLen, len(result))
			}
		})
	}
}
