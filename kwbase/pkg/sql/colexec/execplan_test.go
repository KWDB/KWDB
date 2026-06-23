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
	"sync/atomic"
	"testing"
	"time"

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

type execPlanTestBlockingRowSource struct {
	typs                   []types.T
	nextStarted            chan struct{}
	unblockNext            chan struct{}
	consumerDone           chan struct{}
	nextCalls              int32
	inNext                 int32
	consumerDoneCalled     int32
	concurrentConsumerDone int32
}

var _ execinfra.RowSource = &execPlanTestBlockingRowSource{}

func newExecPlanTestBlockingRowSource(typs []types.T) *execPlanTestBlockingRowSource {
	return &execPlanTestBlockingRowSource{
		typs:         typs,
		nextStarted:  make(chan struct{}),
		unblockNext:  make(chan struct{}),
		consumerDone: make(chan struct{}),
	}
}

func (s *execPlanTestBlockingRowSource) OutputTypes() []types.T {
	return s.typs
}

func (s *execPlanTestBlockingRowSource) Start(ctx context.Context) context.Context {
	return ctx
}

func (s *execPlanTestBlockingRowSource) Next() (
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if atomic.AddInt32(&s.nextCalls, 1) == 1 {
		atomic.StoreInt32(&s.inNext, 1)
		close(s.nextStarted)
		<-s.unblockNext
		atomic.StoreInt32(&s.inNext, 0)
	}
	return nil, nil
}

func (s *execPlanTestBlockingRowSource) ConsumerDone() {
	if atomic.LoadInt32(&s.inNext) != 0 {
		atomic.StoreInt32(&s.concurrentConsumerDone, 1)
	}
	if atomic.CompareAndSwapInt32(&s.consumerDoneCalled, 0, 1) {
		close(s.consumerDone)
	}
}

func (s *execPlanTestBlockingRowSource) ConsumerClosed() {}

func (s *execPlanTestBlockingRowSource) InitProcessorProcedure(*kv.Txn) {}

func TestCreateAndWrapRowSourceTransfersMetadataSourceOwnership(t *testing.T) {
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
	metadataSourcesQueue := []execinfrapb.MetadataSource{
		oldColumnarizer, otherMetadataSource,
	}
	result := NewColOperatorResult{}
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
	require.Len(t, result.MetadataSources, 1)
	require.True(t, result.MetadataSources[0] == newColumnarizer)

	metadataSourcesQueue = result.ReconcileMetadataSources(metadataSourcesQueue)
	require.Len(t, metadataSourcesQueue, 2)
	require.True(t, metadataSourcesQueue[0] == otherMetadataSource)
	require.True(t, metadataSourcesQueue[1] == newColumnarizer)
	for _, src := range metadataSourcesQueue {
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
	metadataSourcesQueue := []execinfrapb.MetadataSource{
		oldColumnarizer, otherMetadataSource,
	}
	result := NewColOperatorResult{}
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

	metadataSourcesQueue = result.ReconcileMetadataSources(metadataSourcesQueue)
	require.Len(t, metadataSourcesQueue, 2)
	require.True(t, metadataSourcesQueue[0] == oldColumnarizer)
	require.True(t, metadataSourcesQueue[1] == otherMetadataSource)
}

func TestReconcileMetadataSourcesSerializesDrainWithNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{EvalCtx: &evalCtx}

	typs := []types.T{*types.Int}
	input := newExecPlanTestBlockingRowSource(typs)
	oldColumnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 1, input)
	require.NoError(t, err)

	result := NewColOperatorResult{}
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
		require.True(t, inputs[0] == input)
		return &execPlanTestRowSourceProcessor{input: inputs[0], typs: typs}, nil
	}

	err = result.createAndWrapRowSource(
		ctx, flowCtx, []Operator{oldColumnarizer}, [][]types.T{typs}, &acc, spec, constructor,
	)
	require.NoError(t, err)

	newColumnarizer, ok := result.Op.(*Columnarizer)
	require.True(t, ok)
	metadataSourcesQueue := result.ReconcileMetadataSources(
		[]execinfrapb.MetadataSource{oldColumnarizer},
	)
	require.Len(t, metadataSourcesQueue, 1)
	require.True(t, metadataSourcesQueue[0] == newColumnarizer)

	newColumnarizer.Init()
	nextDone := make(chan struct{})
	go func() {
		newColumnarizer.Next(ctx)
		close(nextDone)
	}()

	select {
	case <-input.nextStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Next to start")
	}

	drainStarted := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		close(drainStarted)
		for _, src := range metadataSourcesQueue {
			src.DrainMeta(ctx)
		}
		close(drainDone)
	}()
	<-drainStarted

	// DrainMeta on the new Columnarizer must wait for its concurrent Next call.
	// If the old Columnarizer remained in the queue, it would call ConsumerDone
	// on the shared input immediately using a different mutex.
	var concurrentDrainErr string
	select {
	case <-input.consumerDone:
		concurrentDrainErr = "shared input was drained concurrently with Next"
	case <-drainDone:
		concurrentDrainErr = "metadata draining completed while Next was blocked"
	case <-time.After(100 * time.Millisecond):
	}

	close(input.unblockNext)
	select {
	case <-nextDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Next to finish")
	}
	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for metadata draining to finish")
	}
	require.Empty(t, concurrentDrainErr)
	require.Equal(t, int32(0), atomic.LoadInt32(&input.concurrentConsumerDone))
}
