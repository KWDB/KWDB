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

package ordering

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestDistinctOnProvided(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())
	md := f.Metadata()
	for i := 1; i <= 5; i++ {
		md.AddColumn(fmt.Sprintf("c%d", i), types.Int)
	}
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	fd1eq5 := props.FuncDepSet{}
	fd1eq5.AddEquivalency(1, 5)

	// DistinctOn might not project all input columns, so we have three sets of
	// columns, corresponding to this SQL:
	//   SELECT <outCols> FROM
	//     SELECT DISTINCT ON(<groupingCols>) <inputCols>
	testCases := []struct {
		inCols       opt.ColSet
		inFDs        props.FuncDepSet
		outCols      opt.ColSet
		groupingCols opt.ColSet
		required     string
		internal     string
		input        string
		expected     string
	}{
		{ // case 1: Internal ordering is stronger; the provided ordering needs
			//         trimming.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        props.FuncDepSet{},
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+1",
			internal:     "+1,+5",
			input:        "+1,+5",
			expected:     "+1",
		},
		{ // case 2: Projecting all input columns; ok to pass through provided.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+5",
		},
		{ // case 3: Not projecting all input columns; the provided ordering
			//         needs remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+1",
		},
		{ // case 4: The provided ordering needs both trimming and remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "+5,+4",
			input:        "+5,+4",
			expected:     "+1",
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{
					OutputCols: tc.outCols,
					FuncDeps:   fd1eq5,
				},
				Provided: &physical.Provided{
					Ordering: physical.ParseOrdering(tc.input),
				},
			}
			p := memo.GroupingPrivate{
				GroupingCols: tc.groupingCols,
				Ordering:     physical.ParseOrderingChoice(tc.internal),
			}
			var aggs memo.AggregationsExpr
			tc.outCols.Difference(tc.groupingCols).ForEach(func(col opt.ColumnID) {
				aggs = append(aggs, f.ConstructAggregationsItem(
					f.ConstructFirstAgg(f.ConstructVariable(col)),
					col,
				))
			})
			distinctOn := f.Memo().MemoizeDistinctOn(input, aggs, &p)
			req := physical.ParseOrderingChoice(tc.required)
			res := distinctOnBuildProvided(distinctOn, &req).String()
			if res != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, res)
			}
		})
	}
}

// TestScalarGroupByBuildChildReqOrdering tests the scalarGroupByBuildChildReqOrdering function
func TestScalarGroupByBuildChildReqOrdering(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name     string
		ordering string
		childIdx int
		expected string
	}{
		{
			name:     "child index 0",
			ordering: "+1",
			childIdx: 0,
			expected: "+1",
		},
		{
			name:     "child index not 0",
			ordering: "+1",
			childIdx: 1,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
			}
			scalarGroupBy := f.Memo().MemoizeScalarGroupBy(input, nil, &memo.GroupingPrivate{
				Ordering: physical.ParseOrderingChoice(tt.ordering),
			})

			result := scalarGroupByBuildChildReqOrdering(scalarGroupBy, nil, tt.childIdx)
			if result.String() != tt.expected {
				t.Errorf("scalarGroupByBuildChildReqOrdering() = %s, want %s", result.String(), tt.expected)
			}
		})
	}
}

// TestGroupByCanProvideOrdering tests the groupByCanProvideOrdering function
func TestGroupByCanProvideOrdering(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		expected     bool
	}{
		{
			name:         "can provide ordering",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			expected:     true,
		},
		{
			name:         "cannot project columns",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+3",
			expected:     false,
		},
		{
			name:         "orderings do not intersect",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+2",
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
			}
			groupBy := f.Memo().MemoizeGroupBy(input, nil, &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			})

			required := physical.ParseOrderingChoice(tt.required)
			result := groupByCanProvideOrdering(groupBy, &required)
			if result != tt.expected {
				t.Errorf("groupByCanProvideOrdering() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestGroupByBuildChildReqOrdering tests the groupByBuildChildReqOrdering function
func TestGroupByBuildChildReqOrdering(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		childIdx     int
		expected     string
	}{
		{
			name:         "child index 0",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			childIdx:     0,
			expected:     "+1",
		},
		{
			name:         "child index not 0",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			childIdx:     1,
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
			}
			groupBy := f.Memo().MemoizeGroupBy(input, nil, &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			})

			required := physical.ParseOrderingChoice(tt.required)
			result := groupByBuildChildReqOrdering(groupBy, &required, tt.childIdx)
			if result.String() != tt.expected {
				t.Errorf("groupByBuildChildReqOrdering() = %s, want %s", result.String(), tt.expected)
			}
		})
	}
}

// TestGroupByBuildProvided tests the groupByBuildProvided function
func TestGroupByBuildProvided(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		input        string
		expected     string
	}{
		{
			name:         "basic provided ordering",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			input:        "+1",
			expected:     "+1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
				Provided: &physical.Provided{
					Ordering: physical.ParseOrdering(tt.input),
				},
			}
			groupBy := f.Memo().MemoizeGroupBy(input, nil, &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			})

			required := physical.ParseOrderingChoice(tt.required)
			result := groupByBuildProvided(groupBy, &required)
			if result.String() != tt.expected {
				t.Errorf("groupByBuildProvided() = %s, want %s", result.String(), tt.expected)
			}
		})
	}
}

// TestDistinctOnCanProvideOrdering tests the distinctOnCanProvideOrdering function
func TestDistinctOnCanProvideOrdering(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		expected     bool
	}{
		{
			name:         "can provide ordering",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			expected:     true,
		},
		{
			name:         "orderings do not intersect",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+2",
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
			}
			distinctOn := f.Memo().MemoizeDistinctOn(input, nil, &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			})

			required := physical.ParseOrderingChoice(tt.required)
			result := distinctOnCanProvideOrdering(distinctOn, &required)
			if result != tt.expected {
				t.Errorf("distinctOnCanProvideOrdering() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestDistinctOnBuildChildReqOrdering tests the distinctOnBuildChildReqOrdering function
func TestDistinctOnBuildChildReqOrdering(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())

	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		childIdx     int
		expected     string
	}{
		{
			name:         "child index 0",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			childIdx:     0,
			expected:     "+1",
		},
		{
			name:         "child index not 0",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			childIdx:     1,
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
			}
			distinctOn := f.Memo().MemoizeDistinctOn(input, nil, &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			})

			required := physical.ParseOrderingChoice(tt.required)
			result := distinctOnBuildChildReqOrdering(distinctOn, &required, tt.childIdx)
			if result.String() != tt.expected {
				t.Errorf("distinctOnBuildChildReqOrdering() = %s, want %s", result.String(), tt.expected)
			}
		})
	}
}

// TestStreamingGroupingColOrdering tests the StreamingGroupingColOrdering function
func TestStreamingGroupingColOrdering(t *testing.T) {
	tests := []struct {
		name         string
		groupingCols string
		internal     string
		required     string
		expected     string
	}{
		{
			name:         "basic streaming ordering",
			groupingCols: "1,2",
			internal:     "+1",
			required:     "+1",
			expected:     "+1",
		},
		{
			name:         "partial intersection",
			groupingCols: "1,2",
			internal:     "+1,+2",
			required:     "+1,+2",
			expected:     "+1,+2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &memo.GroupingPrivate{
				GroupingCols: opt.MakeColSet(1, 2),
				Ordering:     physical.ParseOrderingChoice(tt.internal),
			}

			required := physical.ParseOrderingChoice(tt.required)
			result := StreamingGroupingColOrdering(g, &required)
			if result.String() != tt.expected {
				t.Errorf("StreamingGroupingColOrdering() = %s, want %s", result.String(), tt.expected)
			}
		})
	}
}
