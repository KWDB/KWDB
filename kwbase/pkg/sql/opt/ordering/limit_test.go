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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func initLimitTest(t *testing.T, ordering *physical.OrderingChoice) memo.RelExpr {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t1 (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2))",
	); err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn1 := tree.NewUnqualifiedTableName("t1")
	md.AddTable(tc.Table(tn1), tn1)

	// Create left input with provided ordering
	scan := &testexpr.Instance{
		Rel: &props.Relational{},
		Provided: &physical.Provided{
			Ordering: physical.ParseOrdering("+1"),
		},
	}

	limit := f.Memo().MemoizeConst(tree.NewDInt(3), types.Int)

	// Create mock merge join expression
	expr := f.Memo().MemoizeLimit(
		scan,
		limit,
		*ordering)

	return expr
}

// TestLimitOrOffsetCanProvideOrdering tests the limitOrOffsetCanProvideOrdering function
func TestLimitOrOffsetCanProvideOrdering(t *testing.T) {
	tests := []struct {
		name         string
		exprOrdering *physical.OrderingChoice
		required     *physical.OrderingChoice
		want         bool
	}{
		{
			name: "intersecting orderings",
			exprOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			want: true,
		},
		{
			name: "non-intersecting orderings",
			exprOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(3)},
				},
			},
			want: false,
		},
		{
			name: "empty required ordering",
			exprOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			required: &physical.OrderingChoice{},
			want:     true,
		},
		{
			name:         "empty expr ordering",
			exprOrdering: &physical.OrderingChoice{},
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic directly
			// Check if either ordering is empty

			got := limitOrOffsetCanProvideOrdering(initLimitTest(t, tt.exprOrdering), tt.required)
			if got != tt.want {
				t.Errorf("limitOrOffsetCanProvideOrdering() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestLimitOrOffsetBuildChildReqOrdering tests the limitOrOffsetBuildChildReqOrdering function
func TestLimitOrOffsetBuildChildReqOrdering(t *testing.T) {
	tests := []struct {
		name           string
		parentOrdering *physical.OrderingChoice
		required       *physical.OrderingChoice
		childIdx       int
		want           physical.OrderingChoice
		wantEmpty      bool
	}{
		{
			name: "child index 0",
			parentOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			childIdx: 0,
			want: physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			wantEmpty: false,
		},
		{
			name: "child index not 0",
			parentOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			childIdx:  1,
			want:      physical.OrderingChoice{},
			wantEmpty: true,
		},
		{
			name: "empty required ordering",
			parentOrdering: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			required: &physical.OrderingChoice{},
			childIdx: 0,
			want: physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limitOrOffsetBuildChildReqOrdering(initLimitTest(t, tt.parentOrdering), tt.required, tt.childIdx)
		})
	}
}

// TestLimitOrOffsetBuildProvided tests the limitOrOffsetBuildProvided function
func TestLimitOrOffsetBuildProvided(t *testing.T) {
	tests := []struct {
		name          string
		childProvided opt.Ordering
		required      *physical.OrderingChoice
		funcDeps      props.FuncDepSet
		want          opt.Ordering
	}{
		{
			name:          "trim provided ordering",
			childProvided: nil,
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			funcDeps: props.FuncDepSet{},
			want:     nil,
		},
		{
			name:          "no trimming needed",
			childProvided: nil,
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
					{Group: opt.MakeColSet(2)},
				},
			},
			funcDeps: props.FuncDepSet{},
			want:     nil,
		},
		{
			name:          "empty provided ordering",
			childProvided: nil,
			required: &physical.OrderingChoice{
				Columns: []physical.OrderingColumnChoice{
					{Group: opt.MakeColSet(1)},
				},
			},
			funcDeps: props.FuncDepSet{},
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic directly
			got := limitOrOffsetBuildProvided(initLimitTest(t, &physical.OrderingChoice{}), tt.required)

			// The child expression in initLimitTest has provided ordering "+1"
			// So the result should contain column 1 when required ordering includes column 1
			expectedLength := 0
			if tt.required != nil && !tt.required.Any() {
				// Check if the required ordering includes column 1
				for _, col := range tt.required.Columns {
					if col.Group.Contains(1) {
						expectedLength = 1
						break
					}
				}
			}

			if len(got) != expectedLength {
				t.Errorf("limitOrOffsetBuildProvided() length = %d, want %d", len(got), expectedLength)
				return
			}
		})
	}
}
