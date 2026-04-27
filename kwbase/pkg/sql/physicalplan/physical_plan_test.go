// Copyright 2017 The Cockroach Authors.
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

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package physicalplan

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestProjectionAndRendering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We don't care about actual types, so we use ColumnType.Locale to store an
	// arbitrary string.
	strToType := func(s string) types.T {
		return *types.MakeCollatedString(types.String, s)
	}

	// For each test case we set up processors with a certain post-process spec,
	// run a function that adds a projection or a rendering, and verify the output
	// post-process spec (as well as ResultTypes, Ordering).
	testCases := []struct {
		// post-process spec of the last stage in the plan.
		post execinfrapb.PostProcessSpec
		// Comma-separated list of result "types".
		resultTypes string
		// ordering in a string like "0,1,-2" (negative values = descending). Can't
		// express descending on column 0, deal with it.
		ordering string

		// function that applies a projection or rendering.
		action func(p *PhysicalPlan)

		// expected post-process spec of the last stage in the resulting plan.
		expPost execinfrapb.PostProcessSpec
		// expected result types, same format and strings as resultTypes.
		expResultTypes string
		// expected ordeering, same format as ordering.
		expOrdering string
	}{
		{
			// Simple projection.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{1, 3, 2})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
				OutputTypes:   []types.T{strToType("B"), strToType("D"), strToType("C")},
			},
			expResultTypes: "B,D,C",
		},

		{
			// Projection with ordering.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "2",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
				OutputTypes:   []types.T{strToType("C")},
			},
			expResultTypes: "C",
			expOrdering:    "0",
		},

		{
			// Projection with ordering that refers to non-projected column.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "2,-1,3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2, 3})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2, 3, 1},
				OutputTypes:   []types.T{strToType("C"), strToType("D"), strToType("B")},
			},
			expResultTypes: "C,D,B",
			expOrdering:    "0,-2,1",
		},

		{
			// Projection after projection.
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{3, 1})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{8, 6},
				OutputTypes:   []types.T{strToType("D"), strToType("B")},
			},
			expResultTypes: "D,B",
			expOrdering:    "0",
		},

		{
			// Projection after projection; ordering refers to non-projected column.
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "0,3",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{3, 1})
			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{8, 6, 5},
				OutputTypes:   []types.T{strToType("D"), strToType("B"), strToType("A")},
			},
			expResultTypes: "D,B,A",
			expOrdering:    "2,0",
		},

		{
			// Projection after rendering.
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@5"}, {Expr: "@1 + @2"}, {Expr: "@6"}},
			},
			resultTypes: "A,B,C",
			ordering:    "2",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2, 0})
			},

			expPost: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@6"}, {Expr: "@5"}},
				OutputTypes: []types.T{strToType("C"), strToType("A")},
			},
			expResultTypes: "C,A",
			expOrdering:    "0",
		},

		{
			// Projection after rendering; ordering refers to non-projected column.
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@5"}, {Expr: "@1 + @2"}, {Expr: "@6"}},
			},
			resultTypes: "A,B,C",
			ordering:    "2,-1",

			action: func(p *PhysicalPlan) {
				p.AddProjection([]uint32{2})
			},

			expPost: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@6"}, {Expr: "@1 + @2"}},
				OutputTypes: []types.T{strToType("C"), strToType("B")},
			},
			expResultTypes: "C,B",
			expOrdering:    "0,-1",
		},

		{
			// Identity rendering.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 10},
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 12},
						&tree.IndexedVar{Idx: 13},
					},
					fakeExprContext{},
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]types.T{strToType("A"), strToType("B"), strToType("C"), strToType("D")},
					false,
				); err != nil {
					t.Fatal(err)
				}
			},

			expPost:        execinfrapb.PostProcessSpec{},
			expResultTypes: "A,B,C,D",
		},

		{
			// Rendering that becomes projection.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.IndexedVar{Idx: 11},
						&tree.IndexedVar{Idx: 13},
						&tree.IndexedVar{Idx: 12},
					},
					fakeExprContext{},
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3},
					[]types.T{strToType("B"), strToType("D"), strToType("C")},
					false,
				); err != nil {
					t.Fatal(err)
				}

			},

			expPost: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 3, 2},
				OutputTypes:   []types.T{strToType("B"), strToType("D"), strToType("C")},
			},
			expResultTypes: "B,D,C",
		},

		{
			// Rendering with ordering that refers to non-projected column.
			post:        execinfrapb.PostProcessSpec{},
			resultTypes: "A,B,C,D",
			ordering:    "3",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.BinaryExpr{
							Operator: tree.Plus,
							Left:     &tree.IndexedVar{Idx: 1},
							Right:    &tree.IndexedVar{Idx: 2},
						},
					},
					fakeExprContext{},
					[]int{0, 1, 2},
					[]types.T{strToType("X")},
					false,
				); err != nil {
					t.Fatal(err)
				}
			},

			expPost: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@2 + @3"}, {Expr: "@4"}},
				OutputTypes: []types.T{strToType("X"), strToType("D")},
			},
			expResultTypes: "X,D",
			expOrdering:    "1",
		},
		{
			// Rendering with ordering that refers to non-projected column after
			// projection.
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5, 6, 7, 8},
			},
			resultTypes: "A,B,C,D",
			ordering:    "0,-3",

			action: func(p *PhysicalPlan) {
				if err := p.AddRendering(
					[]tree.TypedExpr{
						&tree.BinaryExpr{
							Operator: tree.Plus,
							Left:     &tree.IndexedVar{Idx: 11},
							Right:    &tree.IndexedVar{Idx: 12},
						},
						&tree.IndexedVar{Idx: 10},
					},
					fakeExprContext{},
					[]int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2},
					[]types.T{strToType("X"), strToType("A")},
					false,
				); err != nil {
					t.Fatal(err)
				}
			},

			expPost: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@7 + @8"}, {Expr: "@6"}, {Expr: "@9"}},
				OutputTypes: []types.T{strToType("X"), strToType("A"), strToType("D")},
			},
			expResultTypes: "X,A,D",
			expOrdering:    "1,-2",
		},
	}

	for testIdx, tc := range testCases {
		p := PhysicalPlan{
			Processors: []Processor{
				{Spec: execinfrapb.ProcessorSpec{Post: tc.post}},
				{Spec: execinfrapb.ProcessorSpec{Post: tc.post}},
			},
			ResultRouters: []ProcessorIdx{0, 1},
		}

		if tc.ordering != "" {
			for _, s := range strings.Split(tc.ordering, ",") {
				var o execinfrapb.Ordering_Column
				col, _ := strconv.Atoi(s)
				if col >= 0 {
					o.ColIdx = uint32(col)
					o.Direction = execinfrapb.Ordering_Column_ASC
				} else {
					o.ColIdx = uint32(-col)
					o.Direction = execinfrapb.Ordering_Column_DESC
				}
				p.MergeOrdering.Columns = append(p.MergeOrdering.Columns, o)
			}
		}

		for _, s := range strings.Split(tc.resultTypes, ",") {
			p.ResultTypes = append(p.ResultTypes, strToType(s))
		}

		tc.action(&p)

		if post := p.GetLastStagePost(); !reflect.DeepEqual(post, tc.expPost) {
			t.Errorf("%d: incorrect post:\n%s\nexpected:\n%s", testIdx, &post, &tc.expPost)
		}
		var resTypes []string
		for _, t := range p.ResultTypes {
			resTypes = append(resTypes, t.Locale())
		}
		if r := strings.Join(resTypes, ","); r != tc.expResultTypes {
			t.Errorf("%d: incorrect result types: %s expected %s", testIdx, r, tc.expResultTypes)
		}

		var ord []string
		for _, c := range p.MergeOrdering.Columns {
			i := int(c.ColIdx)
			if c.Direction == execinfrapb.Ordering_Column_DESC {
				i = -i
			}
			ord = append(ord, strconv.Itoa(i))
		}
		if o := strings.Join(ord, ","); o != tc.expOrdering {
			t.Errorf("%d: incorrect ordering: '%s' expected '%s'", testIdx, o, tc.expOrdering)
		}
	}
}

func TestMergeResultTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	empty := []types.T{}
	null := []types.T{*types.Unknown}
	typeInt := []types.T{*types.Int}

	testData := []struct {
		name     string
		left     []types.T
		right    []types.T
		expected *[]types.T
		err      bool
	}{
		{"both empty", empty, empty, &empty, false},
		{"left empty", empty, typeInt, nil, true},
		{"right empty", typeInt, empty, nil, true},
		{"both null", null, null, &null, false},
		{"left null", null, typeInt, &typeInt, false},
		{"right null", typeInt, null, &typeInt, false},
		{"both int", typeInt, typeInt, &typeInt, false},
	}
	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			result, err := MergeResultTypes(td.left, td.right)
			if td.err {
				if err == nil {
					t.Fatalf("expected error, got %+v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(*td.expected, result) {
				t.Fatalf("expected %+v, got %+v", *td.expected, result)
			}
		})
	}
}

func TestPhysicalPlanBasicMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test NewStageID
	p := PhysicalPlan{}
	if id := p.NewStageID(); id != 1 {
		t.Fatalf("expected stage ID 1, got %d", id)
	}
	if id := p.NewStageID(); id != 2 {
		t.Fatalf("expected stage ID 2, got %d", id)
	}

	// Test AddProcessor
	nodeID := roachpb.NodeID(1)
	proc := Processor{
		Node: nodeID,
		Spec: execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		},
	}
	idx := p.AddProcessor(proc)
	if idx != 0 {
		t.Fatalf("expected processor index 0, got %d", idx)
	}
	if len(p.Processors) != 1 {
		t.Fatalf("expected 1 processor, got %d", len(p.Processors))
	}

	// Test SetMergeOrdering
	ordering := execinfrapb.Ordering{
		Columns: []execinfrapb.Ordering_Column{
			{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
		},
	}
	p.ResultRouters = []ProcessorIdx{0, 1} // Multiple result routers
	p.SetMergeOrdering(ordering)
	if len(p.MergeOrdering.Columns) != 1 {
		t.Fatalf("expected 1 ordering column, got %d", len(p.MergeOrdering.Columns))
	}

	// Test SetMergeOrdering with single result router
	p.ResultRouters = []ProcessorIdx{0} // Single result router
	p.SetMergeOrdering(ordering)
	if len(p.MergeOrdering.Columns) != 0 {
		t.Fatalf("expected 0 ordering columns for single result router, got %d", len(p.MergeOrdering.Columns))
	}
}

func TestPhysicalPlanChildIsExecInTSEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with non-TS processor
	p := PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
	}
	if p.ChildIsExecInTSEngine() {
		t.Fatal("expected false for non-TS processor")
	}

	// Test with TS processor
	p = PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Core:   execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
	}
	if !p.ChildIsExecInTSEngine() {
		t.Fatal("expected true for TS processor")
	}
}

func TestPhysicalPlanIsDistInTS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with local plan
	p := PhysicalPlan{
		remotePlan: false,
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Core:   execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
	}
	if p.IsDistInTS() {
		t.Fatal("expected false for local plan")
	}

	// Test with remote plan but no TS processors
	p = PhysicalPlan{
		remotePlan: true,
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				},
			},
		},
	}
	if p.IsDistInTS() {
		t.Fatal("expected false for remote plan with no TS processors")
	}

	// Test with remote plan and TS processors
	p = PhysicalPlan{
		remotePlan: true,
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Core:   execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
	}
	if !p.IsDistInTS() {
		t.Fatal("expected true for remote plan with TS processors")
	}
}

func TestPhysicalPlanPopulateEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a plan with two processors and a stream between them
	p := PhysicalPlan{
		Processors: []Processor{
			{
				Node: 1,
				Spec: execinfrapb.ProcessorSpec{
					Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				},
			},
			{
				Node: 1,
				Spec: execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{Type: execinfrapb.InputSyncSpec_UNORDERED}},
				},
			},
		},
		Streams: []Stream{
			{
				SourceProcessor:  0,
				DestProcessor:    1,
				SourceRouterSlot: 0,
				DestInput:        0,
			},
		},
	}

	// Test PopulateEndpoints
	nodeAddresses := map[roachpb.NodeID]string{
		1: "localhost:26257",
	}
	p.PopulateEndpoints(nodeAddresses)

	// Verify the stream was added to both processors
	if len(p.Processors[0].Spec.Output[0].Streams) != 1 {
		t.Fatalf("expected 1 stream in output router, got %d", len(p.Processors[0].Spec.Output[0].Streams))
	}
	if len(p.Processors[1].Spec.Input[0].Streams) != 1 {
		t.Fatalf("expected 1 stream in input synchronizer, got %d", len(p.Processors[1].Spec.Input[0].Streams))
	}
}

func TestPhysicalPlanAddNoInputStage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := PhysicalPlan{}
	corePlacements := []ProcessorCorePlacement{
		{
			SQLInstanceID: 1,
			Core:          execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		},
		{
			SQLInstanceID: 2,
			Core:          execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		},
	}

	post := execinfrapb.PostProcessSpec{}
	outputTypes := []types.T{*types.Int, *types.String}
	ordering := execinfrapb.Ordering{}

	p.AddNoInputStage(corePlacements, post, outputTypes, ordering)

	if len(p.Processors) != 2 {
		t.Fatalf("expected 2 processors, got %d", len(p.Processors))
	}
	if len(p.ResultRouters) != 2 {
		t.Fatalf("expected 2 result routers, got %d", len(p.ResultRouters))
	}
}

func TestPhysicalPlanAddTSRendering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Empty expressions
	p1 := PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   []types.T{*types.Int, *types.String},
	}
	err := p1.AddTSRendering(nil, fakeExprContext{}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error for empty expressions: %s", err)
	}

	// Test case 2: Basic rendering
	p2 := PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   []types.T{*types.Int, *types.String},
	}
	exprs := []tree.TypedExpr{
		&tree.IndexedVar{Idx: 0},
		&tree.IndexedVar{Idx: 1},
	}
	outTypes := []types.T{*types.Int, *types.String}
	err = p2.AddTSRendering(exprs, fakeExprContext{}, []int{0, 1}, outTypes)
	if err != nil {
		t.Fatalf("unexpected error for basic rendering: %s", err)
	}
	post := p2.GetLastStageTSPost()
	if len(post.RenderExprs) != 2 {
		t.Fatalf("expected 2 render expressions, got %d", len(post.RenderExprs))
	}

	// Test case 3: With existing render expressions
	p3 := PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
					Post:   execinfrapb.PostProcessSpec{RenderExprs: []execinfrapb.Expression{{Expr: "@1"}}},
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   []types.T{*types.Int},
	}
	p3.GetLastStageTSPost()
	err = p3.AddTSRendering(exprs, fakeExprContext{}, []int{0, 1}, outTypes)
	if err != nil {
		t.Fatalf("unexpected error with existing render expressions: %s", err)
	}
	if len(p3.Processors) != 2 {
		t.Fatalf("expected 2 processors after adding no-op stage, got %d", len(p3.Processors))
	}

	// Test case 4: With ordering
	p4 := PhysicalPlan{
		Processors: []Processor{
			{
				Spec: execinfrapb.ProcessorSpec{
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			},
		},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   []types.T{*types.Int, *types.String},
		MergeOrdering: execinfrapb.Ordering{
			Columns: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_ASC},
			},
		},
	}
	err = p4.AddTSRendering([]tree.TypedExpr{&tree.IndexedVar{Idx: 0}}, fakeExprContext{}, []int{0}, []types.T{*types.Int})
	if err != nil {
		t.Fatalf("unexpected error with ordering: %s", err)
	}
	post = p4.GetLastStageTSPost()
	if len(post.RenderExprs) != 2 {
		t.Fatalf("expected 2 render expressions (including ordering column), got %d", len(post.RenderExprs))
	}
	if len(p4.MergeOrdering.Columns) != 1 {
		t.Fatalf("expected 1 ordering column, got %d", len(p4.MergeOrdering.Columns))
	}
}

func TestPhysicalPlanAddDistinctSetOpStage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Single node
	t.Run("SingleNode", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{ // Left router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
				{ // Right router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
			},
			ResultRouters: []ProcessorIdx{0, 1},
		}

		nodes := []roachpb.NodeID{1}
		joinCore := execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}
		distinctCores := []execinfrapb.ProcessorCoreUnion{
			{Noop: &execinfrapb.NoopCoreSpec{}},
			{Noop: &execinfrapb.NoopCoreSpec{}},
		}
		post := execinfrapb.PostProcessSpec{}
		eqCols := []uint32{0}
		leftTypes := []types.T{*types.Int}
		rightTypes := []types.T{*types.Int}
		leftMergeOrd := execinfrapb.Ordering{}
		rightMergeOrd := execinfrapb.Ordering{}
		leftRouters := []ProcessorIdx{0}
		rightRouters := []ProcessorIdx{1}

		p.AddDistinctSetOpStage(nodes, joinCore, distinctCores, post, eqCols, leftTypes, rightTypes, leftMergeOrd, rightMergeOrd, leftRouters, rightRouters)

		// Verify distinct and join stages were created
		if len(p.Processors) != 5 { // 2 original + 2 distinct + 1 join = 5
			t.Fatalf("expected 5 processors, got %d", len(p.Processors))
		}

		// Verify result routers were updated
		if len(p.ResultRouters) != 1 {
			t.Fatalf("expected 1 result router, got %d", len(p.ResultRouters))
		}

		// Verify streams were created
		if len(p.Streams) != 4 { // 2 mergeResultStreams + 2 distinct to join = 4
			t.Fatalf("expected 4 streams, got %d", len(p.Streams))
		}
	})

	// Test case 2: Multiple nodes
	t.Run("MultipleNodes", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{ // Left router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
				{ // Right router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
			},
			ResultRouters: []ProcessorIdx{0, 1},
		}

		nodes := []roachpb.NodeID{1, 2}
		joinCore := execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}
		distinctCores := []execinfrapb.ProcessorCoreUnion{
			{Noop: &execinfrapb.NoopCoreSpec{}},
			{Noop: &execinfrapb.NoopCoreSpec{}},
		}
		post := execinfrapb.PostProcessSpec{}
		eqCols := []uint32{0}
		leftTypes := []types.T{*types.Int}
		rightTypes := []types.T{*types.Int}
		leftMergeOrd := execinfrapb.Ordering{}
		rightMergeOrd := execinfrapb.Ordering{}
		leftRouters := []ProcessorIdx{0}
		rightRouters := []ProcessorIdx{1}

		p.AddDistinctSetOpStage(nodes, joinCore, distinctCores, post, eqCols, leftTypes, rightTypes, leftMergeOrd, rightMergeOrd, leftRouters, rightRouters)

		// Verify distinct and join stages were created
		if len(p.Processors) != 8 { // 2 original + 4 distinct + 2 join = 8
			t.Fatalf("expected 8 processors, got %d", len(p.Processors))
		}

		// Verify result routers were updated
		if len(p.ResultRouters) != 2 {
			t.Fatalf("expected 2 result routers, got %d", len(p.ResultRouters))
		}

		// Verify hash routing was set up
		if p.Processors[0].Spec.Output[0].Type != execinfrapb.OutputRouterSpec_BY_HASH {
			t.Fatalf("expected left router to use BY_HASH, got %v", p.Processors[0].Spec.Output[0].Type)
		}
		if p.Processors[1].Spec.Output[0].Type != execinfrapb.OutputRouterSpec_BY_HASH {
			t.Fatalf("expected right router to use BY_HASH, got %v", p.Processors[1].Spec.Output[0].Type)
		}
		if !reflect.DeepEqual(p.Processors[0].Spec.Output[0].HashColumns, eqCols) {
			t.Fatalf("expected left router hash columns %v, got %v", eqCols, p.Processors[0].Spec.Output[0].HashColumns)
		}
		if !reflect.DeepEqual(p.Processors[1].Spec.Output[0].HashColumns, eqCols) {
			t.Fatalf("expected right router hash columns %v, got %v", eqCols, p.Processors[1].Spec.Output[0].HashColumns)
		}
	})
}

func TestPhysicalPlanPushAggToStatisticReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Basic SUM aggregation
	t.Run("BasicSUM", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							TsStatisticReader: &execinfrapb.TSStatisticReaderSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							OutputTypes:   []types.T{*types.Float, *types.Float},
							OutputColumns: []uint32{0, 1},
						},
					},
				},
			},
		}

		aggSpecs := &execinfrapb.AggregatorSpec{
			Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
				{
					Func:   execinfrapb.AggregatorSpec_SUM,
					ColIdx: []uint32{0},
					//OutputIdx: 0,
				},
			},
		}

		tsPostSpec := &execinfrapb.PostProcessSpec{
			OutputTypes: []types.T{*types.Float},
		}

		err := p.PushAggToStatisticReader(fakeExprContext{}, 0, aggSpecs, tsPostSpec, []types.T{*types.Float}, nil, false)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		tr := p.Processors[0].Spec.Core.TsStatisticReader
		if tr == nil {
			t.Fatal("expected TsStatisticReader to be set")
		}
		if len(tr.AggTypes) != 1 {
			t.Fatalf("expected 1 agg type, got %d", len(tr.AggTypes))
		}
		if tr.AggTypes[0] != int32(execinfrapb.AggregatorSpec_SUM) {
			t.Fatalf("expected SUM agg type, got %d", tr.AggTypes[0])
		}
		if len(p.Processors[0].Spec.Post.RenderExprs) != 1 {
			t.Fatalf("expected 1 render expression, got %d", len(p.Processors[0].Spec.Post.RenderExprs))
		}
	})

	// Test case 2: Basic COUNT aggregation
	t.Run("BasicCOUNT", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							TsStatisticReader: &execinfrapb.TSStatisticReaderSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							OutputTypes:   []types.T{*types.Float, *types.Float},
							OutputColumns: []uint32{0, 1},
						},
					},
				},
			},
		}

		aggSpecs := &execinfrapb.AggregatorSpec{
			Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
				{
					Func:   execinfrapb.AggregatorSpec_COUNT,
					ColIdx: []uint32{0},
					//OutputIdx: 0,
				},
			},
		}

		tsPostSpec := &execinfrapb.PostProcessSpec{
			OutputTypes: []types.T{*types.Int4},
		}

		err := p.PushAggToStatisticReader(fakeExprContext{}, 0, aggSpecs, tsPostSpec, []types.T{*types.Int4}, nil, false)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		tr := p.Processors[0].Spec.Core.TsStatisticReader
		if tr == nil {
			t.Fatal("expected TsStatisticReader to be set")
		}
		if len(tr.AggTypes) != 1 {
			t.Fatalf("expected 1 agg type, got %d", len(tr.AggTypes))
		}
		if tr.AggTypes[0] != int32(execinfrapb.AggregatorSpec_COUNT) {
			t.Fatalf("expected COUNT agg type, got %d", tr.AggTypes[0])
		}
		if len(p.Processors[0].Spec.Post.RenderExprs) != 1 {
			t.Fatalf("expected 1 render expression, got %d", len(p.Processors[0].Spec.Post.RenderExprs))
		}
	})

	// Test case 3: AVG aggregation (should be split into SUM and COUNT)
	t.Run("AVGAggregation", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							TsStatisticReader: &execinfrapb.TSStatisticReaderSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							OutputTypes:   []types.T{*types.Float, *types.Float},
							OutputColumns: []uint32{0},
						},
					},
				},
			},
		}

		aggSpecs := &execinfrapb.AggregatorSpec{
			Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
				{
					Func:   execinfrapb.AggregatorSpec_AVG,
					ColIdx: []uint32{0},
					//OutputIdx: 0,
				},
			},
		}

		tsPostSpec := &execinfrapb.PostProcessSpec{
			OutputTypes: []types.T{*types.Float},
		}

		err := p.PushAggToStatisticReader(fakeExprContext{}, 0, aggSpecs, tsPostSpec, []types.T{*types.Float}, nil, false)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		tr := p.Processors[0].Spec.Core.TsStatisticReader
		if tr == nil {
			t.Fatal("expected TsStatisticReader to be set")
		}
		if len(tr.AggTypes) != 2 {
			t.Fatalf("expected 2 agg types (SUM and COUNT), got %d", len(tr.AggTypes))
		}
		if tr.AggTypes[0] != int32(execinfrapb.AggregatorSpec_SUM) {
			t.Fatalf("expected SUM agg type as first, got %d", tr.AggTypes[0])
		}
		if tr.AggTypes[1] != int32(execinfrapb.AggregatorSpec_COUNT) {
			t.Fatalf("expected COUNT agg type as second, got %d", tr.AggTypes[1])
		}
		if len(p.Processors[0].Spec.Post.RenderExprs) != 1 {
			t.Fatalf("expected 1 render expression for AVG, got %d", len(p.Processors[0].Spec.Post.RenderExprs))
		}
	})

	// Test case 4: Error - not a TsStatisticReader
	t.Run("ErrorNotTsStatisticReader", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							Noop: &execinfrapb.NoopCoreSpec{},
						},
					},
				},
			},
		}

		aggSpecs := &execinfrapb.AggregatorSpec{
			Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
				{
					Func:   execinfrapb.AggregatorSpec_SUM,
					ColIdx: []uint32{0},
					//OutputIdx: 0,
				},
			},
		}

		tsPostSpec := &execinfrapb.PostProcessSpec{
			OutputTypes: []types.T{*types.Float},
		}

		err := p.PushAggToStatisticReader(fakeExprContext{}, 0, aggSpecs, tsPostSpec, []types.T{*types.Float}, nil, false)
		if err == nil {
			t.Fatal("expected error for non-TsStatisticReader processor")
		}
	})

	// Test case 5: Error - column index out of range
	t.Run("ErrorColumnIndexOutOfRange", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							TsStatisticReader: &execinfrapb.TSStatisticReaderSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							OutputTypes: []types.T{*types.Float}, // Only one column
						},
					},
				},
			},
		}

		aggSpecs := &execinfrapb.AggregatorSpec{
			Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
				{
					Func:   execinfrapb.AggregatorSpec_SUM,
					ColIdx: []uint32{1}, // Column index 1 is out of range
					//OutputIdx: 0,
				},
			},
		}

		tsPostSpec := &execinfrapb.PostProcessSpec{
			OutputTypes: []types.T{*types.Float},
		}

		err := p.PushAggToStatisticReader(fakeExprContext{}, 0, aggSpecs, tsPostSpec, []types.T{*types.Float}, nil, false)
		if err == nil {
			t.Fatal("expected error for column index out of range")
		}
	})
}

func TestPhysicalPlanAddBLJoinStage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Single node
	t.Run("SingleNode", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{ // Left router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
				{ // Right router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
			},
		}

		nodes := []roachpb.NodeID{1}
		core := execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}
		post := execinfrapb.PostProcessSpec{}
		leftTypes := []types.T{*types.Int}
		rightTypes := []types.T{*types.String}
		leftMergeOrd := execinfrapb.Ordering{}
		rightMergeOrd := execinfrapb.Ordering{}
		leftRouters := []ProcessorIdx{0}
		rightRouters := []ProcessorIdx{1}

		p.AddBLJoinStage(nodes, core, post, leftTypes, rightTypes, leftMergeOrd, rightMergeOrd, leftRouters, rightRouters)

		// Verify BLJ processor was created
		if len(p.Processors) != 3 { // 2 original + 1 BLJ = 3
			t.Fatalf("expected 3 processors, got %d", len(p.Processors))
		}

		// Verify result routers were updated
		if len(p.ResultRouters) != 1 {
			t.Fatalf("expected 1 result router, got %d", len(p.ResultRouters))
		}

		// Verify streams were created
		if len(p.Streams) != 2 { // 1 left to BLJ + 1 right to BLJ = 2
			t.Fatalf("expected 2 streams, got %d", len(p.Streams))
		}

		// Verify BLJ processor has two inputs
		bljProc := p.Processors[2]
		if len(bljProc.Spec.Input) != 2 {
			t.Fatalf("expected 2 inputs for BLJ processor, got %d", len(bljProc.Spec.Input))
		}
	})

	// Test case 2: Multiple nodes
	t.Run("MultipleNodes", func(t *testing.T) {
		p := PhysicalPlan{
			Processors: []Processor{
				{ // Left router
					Node: 1,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
				{ // Right router 1
					Node: 2,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
				{ // Right router 2
					Node: 3,
					Spec: execinfrapb.ProcessorSpec{
						Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					},
				},
			},
		}

		nodes := []roachpb.NodeID{2, 3}
		core := execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}
		post := execinfrapb.PostProcessSpec{}
		leftTypes := []types.T{*types.Int}
		rightTypes := []types.T{*types.String}
		leftMergeOrd := execinfrapb.Ordering{}
		rightMergeOrd := execinfrapb.Ordering{}
		leftRouters := []ProcessorIdx{0}
		rightRouters := []ProcessorIdx{1, 2}

		p.AddBLJoinStage(nodes, core, post, leftTypes, rightTypes, leftMergeOrd, rightMergeOrd, leftRouters, rightRouters)

		// Verify BLJ processors were created
		if len(p.Processors) != 5 { // 3 original + 2 BLJ = 5
			t.Fatalf("expected 5 processors, got %d", len(p.Processors))
		}

		// Verify result routers were updated
		if len(p.ResultRouters) != 2 {
			t.Fatalf("expected 2 result routers, got %d", len(p.ResultRouters))
		}

		// Verify streams were created
		if len(p.Streams) != 4 { // 2 left to BLJ + 2 right to BLJ = 4
			t.Fatalf("expected 4 streams, got %d", len(p.Streams))
		}

		// Verify left router uses MIRROR routing
		if p.Processors[0].Spec.Output[0].Type != execinfrapb.OutputRouterSpec_MIRROR {
			t.Fatalf("expected left router to use MIRROR, got %v", p.Processors[0].Spec.Output[0].Type)
		}

		// Verify BLJ processors have two inputs
		for i := 3; i < 5; i++ {
			bljProc := p.Processors[i]
			if len(bljProc.Spec.Input) != 2 {
				t.Fatalf("expected 2 inputs for BLJ processor %d, got %d", i, len(bljProc.Spec.Input))
			}
		}
	})
}
