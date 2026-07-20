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

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPlanReqOrdering tests the planReqOrdering function with different PlanNode types
func TestPlanReqOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		PlanNode PlanNode
		expected ReqOrdering
	}{
		{
			name:     "test explainPlanNode",
			PlanNode: &explainPlanNode{},
			expected: nil,
		},
		{
			name:     "test limitNode",
			PlanNode: &limitNode{},
			expected: nil,
		},
		{
			name:     "test max1RowNode",
			PlanNode: &max1RowNode{},
			expected: nil,
		},
		{
			name:     "test spoolNode",
			PlanNode: &spoolNode{},
			expected: nil,
		},
		{
			name:     "test saveTableNode",
			PlanNode: &saveTableNode{},
			expected: nil,
		},
		{
			name:     "test serializeNode",
			PlanNode: &serializeNode{},
			expected: nil,
		},
		{
			name:     "test deleteNode without rowsNeeded",
			PlanNode: &deleteNode{},
			expected: nil,
		},
		{
			name:     "test projectSetNode",
			PlanNode: &projectSetNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test filterNode",
			PlanNode: &filterNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test groupNode",
			PlanNode: &groupNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test distinctNode",
			PlanNode: &distinctNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test indexJoinNode",
			PlanNode: &indexJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test windowNode",
			PlanNode: &windowNode{},
			expected: nil,
		},
		{
			name:     "test joinNode",
			PlanNode: &joinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test unionNode",
			PlanNode: &unionNode{},
			expected: nil,
		},
		{
			name:     "test insertNode",
			PlanNode: &insertNode{},
			expected: nil,
		},
		{
			name:     "test insertFastPathNode",
			PlanNode: &insertFastPathNode{},
			expected: nil,
		},
		{
			name:     "test updateNode",
			PlanNode: &updateNode{},
			expected: nil,
		},
		{
			name:     "test upsertNode",
			PlanNode: &upsertNode{},
			expected: nil,
		},
		{
			name:     "test scanNode",
			PlanNode: &scanNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test ordinalityNode",
			PlanNode: &ordinalityNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test renderNode",
			PlanNode: &renderNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test sortNode",
			PlanNode: &sortNode{ordering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test lookupJoinNode",
			PlanNode: &lookupJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		},
		{
			name:     "test zigzagJoinNode",
			PlanNode: &zigzagJoinNode{reqOrdering: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
			expected: sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}},
		},
		{
			name:     "test unknown node type",
			PlanNode: &valuesNode{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := planReqOrdering(tt.PlanNode)

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
		PlanNode PlanNode
		expected ReqOrdering
	}{
		{
			name: "test deleteNode with rowsNeeded",
			PlanNode: &deleteNode{
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
			result := planReqOrdering(tt.PlanNode)

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
		PlanNode PlanNode
		expected ReqOrdering
	}{
		{
			name: "test explainPlanNode with results",
			PlanNode: &explainPlanNode{
				run: explainPlanRun{
					results: &valuesNode{},
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := planReqOrdering(tt.PlanNode)

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
