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

package colflow

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colflow/colrpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type callbackRemoteComponentCreator struct {
	newOutboxFn func(*colexec.Allocator, colexec.Operator, []coltypes.T, []execinfrapb.MetadataSource) (*colrpc.Outbox, error)
	newInboxFn  func(allocator *colexec.Allocator, typs []coltypes.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error)
}

func (c callbackRemoteComponentCreator) newOutbox(
	allocator *colexec.Allocator,
	input colexec.Operator,
	typs []coltypes.T,
	metadataSources []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
) (*colrpc.Outbox, error) {
	return c.newOutboxFn(allocator, input, typs, metadataSources)
}

func (c callbackRemoteComponentCreator) newInbox(
	allocator *colexec.Allocator, typs []coltypes.T, streamID execinfrapb.StreamID,
) (*colrpc.Inbox, error) {
	return c.newInboxFn(allocator, typs, streamID)
}

func intCols(numCols int) []types.T {
	cols := make([]types.T, numCols)
	for i := range cols {
		cols[i] = *types.Int
	}
	return cols
}

// TestDrainOnlyInputDAG is a regression test for #39137 to ensure
// that queries don't hang using the following scenario:
// Consider two nodes n1 and n2, an outbox (o1) and inbox (i1) on n1, and an
// arbitrary flow on n2.
// At the end of the query, o1 will drain its metadata sources when it
// encounters a zero-length batch from its input. If one of these metadata
// sources is i1, there is a possibility that a cycle is unknowingly created
// since i1 (as an example) could be pulling from a remote operator that itself
// is pulling from o1, which is at this moment attempting to drain i1.
// This test verifies that no metadata sources are added to an outbox that are
// not explicitly known to be in its input DAG. The diagram below outlines
// the main point of this test. The outbox's input ends up being some inbox
// pulling from somewhere upstream (in this diagram, node 3, but this detail is
// not important). If it drains the depicted inbox, that is pulling from node 2
// which is in turn pulling from an outbox, a cycle is created and the flow is
// blocked.
//
//	    +------------+
//	    |  Node 3    |
//	    +-----+------+
//	          ^
//	Node 1    |           Node 2
//
// +------------------------+-----------------+
//
//	     +------------+  |
//	Spec C +--------+ |  |
//	     | |  noop  | |  |
//	     | +---+----+ |  |
//	     |     ^      |  |
//	     |  +--+---+  |  |
//	     |  |outbox|  +<----------+
//	     |  +------+  |  |        |
//	     +------------+  |        |
//
// Drain cycle!---+         |   +----+-----------------+
//
//	           v         |   |Any group of operators|
//	     +------------+  |   +----+-----------------+
//	     |  +------+  |  |        ^
//	Spec A  |inbox +--------------+
//	     |  +------+  |  |
//	     +------------+  |
//	           ^         |
//	           |         |
//	     +-----+------+  |
//	Spec B    noop    |  |
//	     |materializer|  +
//	     +------------+
func TestDrainOnlyInputDAG(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numInputTypesToOutbox       = 3
		numInputTypesToMaterializer = 1
	)
	// procs are the ProcessorSpecs that we pass in to create the flow. Note that
	// we order the inbox first so that the flow creator instantiates it before
	// anything else.
	procs := []execinfrapb.ProcessorSpec{
		{
			// This is i1, the inbox which should be drained by the materializer, not
			// o1.
			// Spec A in the diagram.
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams:     []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_REMOTE, StreamID: 1}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
					// We set up a local output so that the inbox is created independently.
					Streams: []execinfrapb.StreamEndpointSpec{
						{Type: execinfrapb.StreamEndpointType_LOCAL, StreamID: 2},
					},
				},
			},
		},
		// This is the root of the flow. The noop operator that will read from i1
		// and the materializer.
		// Spec B in the diagram.
		{
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams:     []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_LOCAL, StreamID: 2}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_SYNC_RESPONSE}},
				},
			},
		},
		{
			// Because creating a table reader is too complex (you need to create a
			// bunch of other state) we simulate this by creating a noop operator with
			// a remote input, which is treated as having no local edges during
			// topological processing.
			// Spec C in the diagram.
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_REMOTE}},
					// Use three Int columns as the types to be able to distinguish
					// between input DAGs when creating the inbox.
					ColumnTypes: intCols(numInputTypesToOutbox),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			// This is o1, the outbox that will drain metadata.
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_REMOTE}},
				},
			},
		},
	}

	inboxToNumInputTypes := make(map[*colrpc.Inbox][]coltypes.T)
	outboxCreated := false
	componentCreator := callbackRemoteComponentCreator{
		newOutboxFn: func(
			allocator *colexec.Allocator,
			op colexec.Operator,
			typs []coltypes.T,
			sources []execinfrapb.MetadataSource,
		) (*colrpc.Outbox, error) {
			require.False(t, outboxCreated)
			outboxCreated = true
			// Verify that there is only one metadata source: the inbox that is the
			// input to the noop operator. This is verified by first checking the
			// number of metadata sources and then that the input types are what we
			// expect from the input DAG.
			require.Len(t, sources, 1)
			require.Len(t, inboxToNumInputTypes[sources[0].(*colrpc.Inbox)], numInputTypesToOutbox)
			return colrpc.NewOutbox(allocator, op, typs, sources, nil /* toClose */)
		},
		newInboxFn: func(allocator *colexec.Allocator, typs []coltypes.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error) {
			inbox, err := colrpc.NewInbox(allocator, typs, streamID)
			inboxToNumInputTypes[inbox] = typs
			return inbox, err
		},
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	f := &flowinfra.FlowBase{FlowCtx: execinfra.FlowCtx{EvalCtx: &evalCtx, NodeID: roachpb.NodeID(1)}}
	var wg sync.WaitGroup
	vfc := newVectorizedFlowCreator(&vectorizedFlowCreatorHelper{f: f}, componentCreator, false, &wg, &execinfra.RowChannel{}, nil, execinfrapb.FlowID{}, colcontainer.DiskQueueCfg{}, nil)

	_, err := vfc.setupFlow(ctx, &f.FlowCtx, procs, nil, flowinfra.FuseNormally)
	err = nil
	defer func() {
		for _, memAcc := range vfc.streamingMemAccounts {
			memAcc.Close(ctx)
		}
	}()
	require.NoError(t, err)

	// Verify that an outbox was actually created.
	require.True(t, outboxCreated)
}

// TestVectorizedFlowTempDirectory tests a flow's interactions with the
// temporary directory that will be used when spilling execution. Refer to
// subtests for a more thorough explanation.
func TestVectorizedFlowTempDirectory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)

	// We use an on-disk engine for this test since we're testing FS interactions
	// and want to get the same behavior as a non-testing environment.
	tempPath, dirCleanup := testutils.TempDir(t)
	ngn, err := storage.NewDefaultEngine(0 /* cacheSize */, base.StorageConfig{Dir: tempPath})
	require.NoError(t, err)
	defer ngn.Close()
	defer dirCleanup()

	newVectorizedFlow := func() *vectorizedFlow {
		return NewVectorizedFlow(
			&flowinfra.FlowBase{
				FlowCtx: execinfra.FlowCtx{
					Cfg: &execinfra.ServerConfig{
						TempFS:          ngn,
						TempStoragePath: tempPath,
						VecFDSemaphore:  &colexec.TestingSemaphore{},
						Metrics:         &execinfra.DistSQLMetrics{},
					},
					EvalCtx: &evalCtx,
					NodeID:  roachpb.NodeID(1),
				},
			},
		).(*vectorizedFlow)
	}

	dirs, err := ngn.ListDir(tempPath)
	require.NoError(t, err)
	numDirsTheTestStartedWith := len(dirs)
	checkDirs := func(t *testing.T, numExtraDirs int) {
		t.Helper()
		dirs, err := ngn.ListDir(tempPath)
		require.NoError(t, err)
		expectedNumDirs := numDirsTheTestStartedWith + numExtraDirs
		require.Equal(t, expectedNumDirs, len(dirs), "expected %d directories but found %d: %s", expectedNumDirs, len(dirs), dirs)
	}

	// LazilyCreated asserts that a directory is not created during flow Setup
	// but is done so when an operator spills to disk.
	t.Run("LazilyCreated", func(t *testing.T) {
		vf := newVectorizedFlow()
		var creator *vectorizedFlowCreator
		vf.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator = c
		}

		_, err := vf.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		// No directory should have been created.
		checkDirs(t, 0)

		// After the call to Setup, creator should be non-nil (i.e. the testing knob
		// should have been called).
		require.NotNil(t, creator)

		// Now simulate an operator spilling to disk. The flow should have set this
		// up to create its directory.
		creator.diskQueueCfg.OnNewDiskQueueCb()

		// We should now have one directory, the flow's temporary storage directory.
		checkDirs(t, 1)

		// Another operator calling OnNewDiskQueueCb again should not create a new
		// directory
		creator.diskQueueCfg.OnNewDiskQueueCb()
		checkDirs(t, 1)

		// When the flow is Cleaned up, this directory should be removed.
		vf.Cleanup(ctx)
		checkDirs(t, 0)
	})

	// This subtest verifies that two local flows with the same ID create
	// different directories. This case happens regularly with local flows, since
	// they have an unset ID.
	t.Run("DirCreationHandlesUnsetIDCollisions", func(t *testing.T) {
		flowID := execinfrapb.FlowID{}
		vf1 := newVectorizedFlow()
		var creator1 *vectorizedFlowCreator
		vf1.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator1 = c
		}
		// Explicitly set an empty ID.
		vf1.ID = flowID
		_, err := vf1.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		checkDirs(t, 0)
		creator1.diskQueueCfg.OnNewDiskQueueCb()
		checkDirs(t, 1)

		// Now a new flow with the same ID gets set up.
		vf2 := newVectorizedFlow()
		var creator2 *vectorizedFlowCreator
		vf2.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator2 = c
		}
		vf2.ID = flowID
		_, err = vf2.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		// Still only 1 directory.
		checkDirs(t, 1)
		creator2.diskQueueCfg.OnNewDiskQueueCb()
		// A new directory should have been created for this flow.
		checkDirs(t, 2)

		vf1.Cleanup(ctx)
		checkDirs(t, 1)
		vf2.Cleanup(ctx)
		checkDirs(t, 0)
	})

	t.Run("DirCreationRace", func(t *testing.T) {
		vf := newVectorizedFlow()
		var creator *vectorizedFlowCreator
		vf.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator = c
		}

		_, err := vf.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		createTempDir := creator.diskQueueCfg.OnNewDiskQueueCb
		errCh := make(chan error)
		go func() {
			createTempDir()
			errCh <- ngn.CreateDir(filepath.Join(vf.tempStorage.path, "async"))
		}()
		createTempDir()
		// Both goroutines should be able to create their subdirectories within the
		// flow's temporary directory.
		require.NoError(t, ngn.CreateDir(filepath.Join(vf.tempStorage.path, "main_goroutine")))
		require.NoError(t, <-errCh)
		vf.Cleanup(ctx)
		checkDirs(t, 0)
	})
}
