// Copyright 2020 The Cockroach Authors.
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

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPlanReqOrdering tests the planReqOrdering function with different planNode types
func TestPlanReqOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		planNode planNode
		expected ReqOrdering
	}{
		{
			name:     "test explainPlanNode",
			planNode: &explainPlanNode{},
			expected: nil,
		},
		{
			name:     "test limitNode",
			planNode: &limitNode{},
			expected: nil,
		},
		{
			name:     "test max1RowNode",
			planNode: &max1RowNode{},
			expected: nil,
		},
		{
			name:     "test spoolNode",
			planNode: &spoolNode{},
			expected: nil,
		},
		{
			name:     "test saveTableNode",
			planNode: &saveTableNode{},
			expected: nil,
		},
		{
			name:     "test serializeNode",
			planNode: &serializeNode{},
			expected: nil,
		},
		{
			name:     "test deleteNode without rowsNeeded",
			planNode: &deleteNode{},
			expected: nil,
		},
		{
			name:     "test projectSetNode",
			planNode: &projectSetNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test filterNode",
			planNode: &filterNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test groupNode",
			planNode: &groupNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test distinctNode",
			planNode: &distinctNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test indexJoinNode",
			planNode: &indexJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test windowNode",
			planNode: &windowNode{},
			expected: nil,
		},
		{
			name:     "test joinNode",
			planNode: &joinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test unionNode",
			planNode: &unionNode{},
			expected: nil,
		},
		{
			name:     "test insertNode",
			planNode: &insertNode{},
			expected: nil,
		},
		{
			name:     "test insertFastPathNode",
			planNode: &insertFastPathNode{},
			expected: nil,
		},
		{
			name:     "test updateNode",
			planNode: &updateNode{},
			expected: nil,
		},
		{
			name:     "test upsertNode",
			planNode: &upsertNode{},
			expected: nil,
		},
		{
			name:     "test scanNode",
			planNode: &scanNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test ordinalityNode",
			planNode: &ordinalityNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test renderNode",
			planNode: &renderNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test sortNode",
			planNode: &sortNode{ordering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test lookupJoinNode",
			planNode: &lookupJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test zigzagJoinNode",
			planNode: &zigzagJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test unknown node type",
			planNode: &valuesNode{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := planReqOrdering(tt.planNode)

			if result == nil && tt.expected == nil {
				return
			}

			if result == nil && tt.expected != nil {
				t.Errorf("planReqOrdering() returned nil, expected %v", tt.expected)
				return
			}

			if result != nil && tt.expected == nil {
				t.Errorf("planReqOrdering() returned %v, expected nil", result)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("planReqOrdering() length = %d, want %d", len(result), len(tt.expected))
				return
			}

			for i, colOrder := range result {
				if colOrder.ColIdx != tt.expected[i].ColIdx {
					t.Errorf("planReqOrdering()[%d].ColIdx = %d, want %d", i, colOrder.ColIdx, tt.expected[i].ColIdx)
				}
				if colOrder.Direction != tt.expected[i].Direction {
					t.Errorf("planReqOrdering()[%d].Direction = %v, want %v", i, colOrder.Direction, tt.expected[i].Direction)
				}
			}
		})
	}
}

// TestPlanReqOrderingDeleteNodeWithRowsNeeded tests the deleteNode case when rowsNeeded is true
func TestPlanReqOrderingDeleteNodeWithRowsNeeded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		planNode planNode
		expected ReqOrdering
	}{
		{
			name: "test deleteNode with rowsNeeded",
			planNode: &deleteNode{
				run: deleteRun{
					rowsNeeded: true,
				},
				source: &scanNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := planReqOrdering(tt.planNode)

			if result == nil && tt.expected == nil {
				return
			}

			if result == nil && tt.expected != nil {
				t.Errorf("planReqOrdering() returned nil, expected %v", tt.expected)
				return
			}

			if result != nil && tt.expected == nil {
				t.Errorf("planReqOrdering() returned %v, expected nil", result)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("planReqOrdering() length = %d, want %d", len(result), len(tt.expected))
				return
			}

			for i, colOrder := range result {
				if colOrder.ColIdx != tt.expected[i].ColIdx {
					t.Errorf("planReqOrdering()[%d].ColIdx = %d, want %d", i, colOrder.ColIdx, tt.expected[i].ColIdx)
				}
				if colOrder.Direction != tt.expected[i].Direction {
					t.Errorf("planReqOrdering()[%d].Direction = %v, want %v", i, colOrder.Direction, tt.expected[i].Direction)
				}
			}
		})
	}
}

// TestPlanReqOrderingExplainPlanNodeWithResults tests the explainPlanNode case with results
func TestPlanReqOrderingExplainPlanNodeWithResults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		planNode planNode
		expected ReqOrdering
	}{
		{
			name: "test explainPlanNode with results",
			planNode: &explainPlanNode{
				run: explainPlanRun{
					results: &valuesNode{},
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := planReqOrdering(tt.planNode)

			if result == nil && tt.expected == nil {
				return
			}

			if result == nil && tt.expected != nil {
				t.Errorf("planReqOrdering() returned nil, expected %v", tt.expected)
				return
			}

			if result != nil && tt.expected == nil {
				t.Errorf("planReqOrdering() returned %v, expected nil", result)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("planReqOrdering() length = %d, want %d", len(result), len(tt.expected))
				return
			}

			for i, colOrder := range result {
				if colOrder.ColIdx != tt.expected[i].ColIdx {
					t.Errorf("planReqOrdering()[%d].ColIdx = %d, want %d", i, colOrder.ColIdx, tt.expected[i].ColIdx)
				}
				if colOrder.Direction != tt.expected[i].Direction {
					t.Errorf("planReqOrdering()[%d].Direction = %v, want %v", i, colOrder.Direction, tt.expected[i].Direction)
				}
			}
		})
	}
}
