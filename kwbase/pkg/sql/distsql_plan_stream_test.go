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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func makeTestStreamResultColumns() sqlbase.ResultColumns {
	return sqlbase.ResultColumns{
		{
			Name:           "ptag_col",
			Typ:            types.Int,
			PGAttributeNum: sqlbase.ColumnID(1),
		},
		{
			Name:           "tag_col",
			Typ:            types.String,
			PGAttributeNum: sqlbase.ColumnID(2),
		},
		{
			Name:           "data_col",
			Typ:            types.Float,
			PGAttributeNum: sqlbase.ColumnID(3),
		},
	}
}

func TestInitPhyPlanForStreamReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	planCtx := &PlanningCtx{
		streamSpec: &execinfrapb.StreamReaderSpec{
			JobID: 99,
		},
	}

	resultColumns := makeTestStreamResultColumns()
	resCols := []int{7, 8, 9}
	tsColMap := map[sqlbase.ColumnID]tsColIndex{
		1: {idx: 10, internalType: *types.Int, colType: sqlbase.ColumnType_TYPE_PTAG},
		2: {idx: 11, internalType: *types.String, colType: sqlbase.ColumnType_TYPE_TAG},
		3: {idx: 12, internalType: *types.Float, colType: sqlbase.ColumnType_TYPE_DATA},
	}

	var p PhysicalPlan
	err := p.initPhyPlanForStreamReader(planCtx, tsColMap, resultColumns, roachpb.NodeID(5), resCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(p.ResultRouters) != 1 {
		t.Fatalf("expected 1 result router, got %d", len(p.ResultRouters))
	}
	if len(p.Processors) != 1 {
		t.Fatalf("expected 1 processor, got %d", len(p.Processors))
	}
	if p.TsOperator != execinfrapb.OperatorType_TsSelect {
		t.Fatalf("expected TsOperator=%v, got %v", execinfrapb.OperatorType_TsSelect, p.TsOperator)
	}

	proc := p.Processors[p.ResultRouters[0]]
	if proc.Node != roachpb.NodeID(5) {
		t.Fatalf("expected processor on node 5, got %d", proc.Node)
	}
	if proc.Spec.Core.StreamReader == nil {
		t.Fatal("expected StreamReader core to be initialized")
	}

	spec := proc.Spec.Core.StreamReader
	if spec.JobID != 99 {
		t.Fatalf("expected JobID=99, got %d", spec.JobID)
	}
	if !reflect.DeepEqual(spec.OrderingColumnIDs, []uint32{0}) {
		t.Fatalf("unexpected OrderingColumnIDs: %v", spec.OrderingColumnIDs)
	}

	if spec.CDCColumns == nil {
		t.Fatal("expected CDCColumns to be initialized")
	}
	if !reflect.DeepEqual(spec.CDCColumns.CDCColumnIDs, []uint32{1, 2, 3}) {
		t.Fatalf("unexpected CDCColumnIDs: %v", spec.CDCColumns.CDCColumnIDs)
	}
	if !reflect.DeepEqual(spec.CDCColumns.CDCColumnIndexes, []uint32{7, 8, 9}) {
		t.Fatalf("unexpected CDCColumnIndexes: %v", spec.CDCColumns.CDCColumnIndexes)
	}
	if !reflect.DeepEqual(spec.CDCColumns.CDCColumnNames, []string{"ptag_col", "tag_col", "data_col"}) {
		t.Fatalf("unexpected CDCColumnNames: %v", spec.CDCColumns.CDCColumnNames)
	}
	if spec.CDCColumns.NeedNormalTags == nil || !*spec.CDCColumns.NeedNormalTags {
		t.Fatalf("expected NeedNormalTags=true, got %v", spec.CDCColumns.NeedNormalTags)
	}
}

func TestInitPhyPlanForStreamReaderWithoutNormalTags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	planCtx := &PlanningCtx{
		streamSpec: &execinfrapb.StreamReaderSpec{
			JobID: 7,
		},
	}

	resultColumns := sqlbase.ResultColumns{
		{
			Name:           "ptag_only",
			Typ:            types.Int,
			PGAttributeNum: sqlbase.ColumnID(11),
		},
		{
			Name:           "data_only",
			Typ:            types.Float,
			PGAttributeNum: sqlbase.ColumnID(12),
		},
	}
	resCols := []int{1, 2}
	tsColMap := map[sqlbase.ColumnID]tsColIndex{
		11: {idx: 3, internalType: *types.Int, colType: sqlbase.ColumnType_TYPE_PTAG},
		12: {idx: 4, internalType: *types.Float, colType: sqlbase.ColumnType_TYPE_DATA},
	}

	var p PhysicalPlan
	err := p.initPhyPlanForStreamReader(planCtx, tsColMap, resultColumns, roachpb.NodeID(2), resCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spec := p.Processors[p.ResultRouters[0]].Spec.Core.StreamReader
	if spec == nil || spec.CDCColumns == nil {
		t.Fatal("expected StreamReader / CDCColumns to be initialized")
	}
	if spec.CDCColumns.NeedNormalTags == nil {
		t.Fatal("expected NeedNormalTags to be set")
	}
	if *spec.CDCColumns.NeedNormalTags {
		t.Fatalf("expected NeedNormalTags=false, got true")
	}
	if !reflect.DeepEqual(spec.OrderingColumnIDs, []uint32{0}) {
		t.Fatalf("unexpected OrderingColumnIDs: %v", spec.OrderingColumnIDs)
	}
}

func TestInitPostSpecForStreamReaderNilFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	post, err := initPostSpecForStreamReader(&PlanningCtx{}, &tsScanNode{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(post, execinfrapb.PostProcessSpec{}) {
		t.Fatalf("expected empty PostProcessSpec, got %+v", post)
	}
}

func TestBuildPostSpecForStreamReadersEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs := []types.T{*types.Int, *types.String}
	outCols, planToStreamColMap, outTypes := buildPostSpecForStreamReaders(
		&tsScanNode{},
		typs,
		nil,
	)

	if len(outCols) != 0 {
		t.Fatalf("expected no output columns, got %v", outCols)
	}
	if len(planToStreamColMap) != 0 {
		t.Fatalf("expected empty PlanToStreamColMap, got %v", planToStreamColMap)
	}
	if !reflect.DeepEqual(outTypes, typs) {
		t.Fatalf("unexpected output types: %v", outTypes)
	}
}

func TestSetLastStageStreamPost(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := PhysicalPlan{
		PhysicalPlan: physicalplan.PhysicalPlan{
			Processors: []physicalplan.Processor{
				{},
				{},
			},
			ResultRouters: []physicalplan.ProcessorIdx{0, 1},
		},
	}

	post := execinfrapb.PostProcessSpec{Projection: true}
	outputTypes := []types.T{*types.Int, *types.String}

	p.SetLastStageStreamPost(post, outputTypes)

	for _, idx := range p.ResultRouters {
		if !reflect.DeepEqual(p.Processors[idx].Spec.Post, post) {
			t.Fatalf("processor %d post mismatch: %+v", idx, p.Processors[idx].Spec.Post)
		}
	}
	if !reflect.DeepEqual(p.ResultTypes, outputTypes) {
		t.Fatalf("unexpected ResultTypes: %v", p.ResultTypes)
	}
}

func TestAddStreamSingleGroupState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makePlan := func() PhysicalPlan {
		return PhysicalPlan{
			PhysicalPlan: physicalplan.PhysicalPlan{
				Processors: []physicalplan.Processor{
					{
						Node: 1,
						Spec: execinfrapb.ProcessorSpec{
							Output: []execinfrapb.OutputRouterSpec{
								{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH},
							},
						},
					},
				},
				ResultRouters: []physicalplan.ProcessorIdx{0},
			},
		}
	}

	t.Run("use gateway node when prevStageNode is zero", func(t *testing.T) {
		dsp := &DistSQLPlanner{
			nodeDesc: roachpb.NodeDescriptor{NodeID: 8},
		}
		p := makePlan()

		dsp.addStreamSingleGroupState(
			&p,
			0, /* prevStageNode */
			execinfrapb.StreamAggregatorSpec{},
			execinfrapb.PostProcessSpec{Projection: true},
			[]types.T{*types.Int},
		)

		last := p.Processors[p.ResultRouters[0]]
		if last.Node != 8 {
			t.Fatalf("expected final aggregator on gateway node 8, got %d", last.Node)
		}
		if last.Spec.Core.StreamAggregator == nil {
			t.Fatal("expected StreamAggregator core to be set")
		}
	})

	t.Run("use previous stage node when provided", func(t *testing.T) {
		dsp := &DistSQLPlanner{
			nodeDesc: roachpb.NodeDescriptor{NodeID: 8},
		}
		p := makePlan()

		dsp.addStreamSingleGroupState(
			&p,
			5, /* prevStageNode */
			execinfrapb.StreamAggregatorSpec{},
			execinfrapb.PostProcessSpec{Projection: true},
			[]types.T{*types.Int},
		)

		last := p.Processors[p.ResultRouters[0]]
		if last.Node != 5 {
			t.Fatalf("expected final aggregator on prevStageNode 5, got %d", last.Node)
		}
		if last.Spec.Core.StreamAggregator == nil {
			t.Fatal("expected StreamAggregator core to be set")
		}
	})
}
