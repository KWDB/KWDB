// Copyright 2026-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

package colexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type execPlanTestMetadataSource struct{}

func (execPlanTestMetadataSource) DrainMeta(context.Context) []execinfrapb.ProducerMetadata {
	return nil
}

type execPlanTestRowSourceProcessor struct {
	input execinfra.RowSource
	typs  []types.T
}

var _ execinfra.Processor = &execPlanTestRowSourceProcessor{}
var _ execinfra.RowSource = &execPlanTestRowSourceProcessor{}

func (p *execPlanTestRowSourceProcessor) OutputTypes() []types.T {
	return p.typs
}

func (p *execPlanTestRowSourceProcessor) Run(context.Context) execinfra.RowStats {
	return execinfra.RowStats{}
}

func (p *execPlanTestRowSourceProcessor) RunShortCircuit(
	context.Context, execinfra.TSReader,
) error {
	return nil
}

func (p *execPlanTestRowSourceProcessor) RunTS(context.Context) {}

func (p *execPlanTestRowSourceProcessor) Push(context.Context, []byte) error {
	return nil
}

func (p *execPlanTestRowSourceProcessor) Next() (
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	return p.input.Next()
}

func (p *execPlanTestRowSourceProcessor) Start(ctx context.Context) context.Context {
	return p.input.Start(ctx)
}

func (p *execPlanTestRowSourceProcessor) ConsumerDone() {
	p.input.ConsumerDone()
}

func (p *execPlanTestRowSourceProcessor) ConsumerClosed() {
	p.input.ConsumerClosed()
}

func (p *execPlanTestRowSourceProcessor) InitProcessorProcedure(txn *kv.Txn) {
	p.input.InitProcessorProcedure(txn)
}

func TestCreateAndWrapRowSourceRemovesUnwrappedColumnarizerMetadataSource(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{EvalCtx: &evalCtx}

	typs := []types.T{*types.Int}
	oldInput := execinfra.NewRepeatableRowSource(typs, nil /* rows */)
	oldColumnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 1, oldInput)
	require.NoError(t, err)

	otherMetadataSource := execPlanTestMetadataSource{}
	result := NewColOperatorResult{
		MetadataSources: []execinfrapb.MetadataSource{oldColumnarizer, otherMetadataSource},
	}
	acc := testMemMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	spec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{JoinReader: &execinfrapb.JoinReaderSpec{}},
	}
	constructor := func(
		_ context.Context,
		_ *execinfra.FlowCtx,
		_ int32,
		_ *execinfrapb.ProcessorCoreUnion,
		_ *execinfrapb.PostProcessSpec,
		inputs []execinfra.RowSource,
		_ []execinfra.RowReceiver,
		_ []execinfra.LocalProcessor,
	) (execinfra.Processor, error) {
		require.Len(t, inputs, 1)
		require.True(t, inputs[0] == oldInput)
		return &execPlanTestRowSourceProcessor{input: inputs[0], typs: typs}, nil
	}

	err = result.createAndWrapRowSource(
		ctx, flowCtx, []Operator{oldColumnarizer}, [][]types.T{typs}, &acc, spec, constructor,
	)
	require.NoError(t, err)

	newColumnarizer, ok := result.Op.(*Columnarizer)
	require.True(t, ok)
	require.True(t, oldColumnarizer != newColumnarizer)
	require.Equal(t, []types.T{*types.Int}, result.ColumnTypes)
	require.Len(t, result.MetadataSources, 2)
	require.True(t, result.MetadataSources[0] == otherMetadataSource)
	require.True(t, result.MetadataSources[1] == newColumnarizer)
	for _, src := range result.MetadataSources {
		require.False(t, src == oldColumnarizer)
	}
}

func TestCreateAndWrapRowSourcePreservesMetadataSourcesOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{EvalCtx: &evalCtx}

	typs := []types.T{*types.Int}
	oldInput := execinfra.NewRepeatableRowSource(typs, nil /* rows */)
	oldColumnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 1, oldInput)
	require.NoError(t, err)

	otherMetadataSource := execPlanTestMetadataSource{}
	result := NewColOperatorResult{
		MetadataSources: []execinfrapb.MetadataSource{oldColumnarizer, otherMetadataSource},
	}
	acc := testMemMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	spec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{JoinReader: &execinfrapb.JoinReaderSpec{}},
	}
	expectedErr := errors.New("test error")
	constructor := func(
		context.Context,
		*execinfra.FlowCtx,
		int32,
		*execinfrapb.ProcessorCoreUnion,
		*execinfrapb.PostProcessSpec,
		[]execinfra.RowSource,
		[]execinfra.RowReceiver,
		[]execinfra.LocalProcessor,
	) (execinfra.Processor, error) {
		return nil, expectedErr
	}

	err = result.createAndWrapRowSource(
		ctx, flowCtx, []Operator{oldColumnarizer}, [][]types.T{typs}, &acc, spec, constructor,
	)
	require.Equal(t, expectedErr, err)
	require.Len(t, result.MetadataSources, 2)
	require.True(t, result.MetadataSources[0] == oldColumnarizer)
	require.True(t, result.MetadataSources[1] == otherMetadataSource)
}
