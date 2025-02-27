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

package colflow_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colflow/colrpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/colcontainerutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type shutdownScenario struct {
	string
}

var (
	consumerDone      = shutdownScenario{"ConsumerDone"}
	consumerClosed    = shutdownScenario{"ConsumerClosed"}
	shutdownScenarios = []shutdownScenario{consumerDone, consumerClosed}
)

type callbackCloser struct {
	closeCb func() error
}

func (c callbackCloser) IdempotentClose(_ context.Context) error {
	return c.closeCb()
}

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely:
// - on a remote node, it creates an exec.HashRouter with 3 outputs (with a
// corresponding to each Outbox) as well as 3 standalone Outboxes;
// - on a local node, it creates 6 exec.Inboxes that feed into an unordered
// synchronizer which then outputs all the data into a materializer.
// The resulting scheme looks as follows:
//
//            Remote Node             |                  Local Node
//                                    |
//             -> output -> Outbox -> | -> Inbox -> |
//            |                       |
// Hash Router -> output -> Outbox -> | -> Inbox -> |
//            |                       |
//             -> output -> Outbox -> | -> Inbox -> |
//                                    |              -> Synchronizer -> materializer
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//
// Also, with 50% probability, another remote node with the chain of an Outbox
// and Inbox is placed between the synchronizer and materializer. The resulting
// scheme then looks as follows:
//
//            Remote Node             |            Another Remote Node             |         Local Node
//                                    |                                            |
//             -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
// Hash Router -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
//             -> output -> Outbox -> | -> Inbox ->                                |
//                                    |             | -> Synchronizer -> Outbox -> | -> Inbox -> materializer
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//
// Remote nodes are simulated by having separate contexts and separate outbox
// registries.
//
// Additionally, all Outboxes have a single metadata source. In ConsumerDone
// shutdown scenario, we check that the metadata has been successfully
// propagated from all of the metadata sources.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, execinfra.StaticNodeID,
	)
	require.NoError(t, err)
	dialer := &execinfrapb.MockDialer{Addr: addr}
	defer dialer.Close()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	for run := 0; run < 10; run++ {
		for _, shutdownOperation := range shutdownScenarios {
			t.Run(fmt.Sprintf("shutdownScenario=%s", shutdownOperation.string), func(t *testing.T) {
				ctxLocal := context.Background()
				ctxRemote, cancelRemote := context.WithCancel(context.Background())
				// Linter says there is a possibility of "context leak" because
				// cancelRemote variable may not be used, so we defer the call to it.
				// This does not change anything about the test since we're blocking on
				// the wait group and we will call cancelRemote() below, so this defer
				// is actually a noop.
				defer cancelRemote()
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctxLocal)
				flowCtx := &execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg:     &execinfra.ServerConfig{Settings: st},
				}
				rng, _ := randutil.NewPseudoRand()
				var (
					err             error
					wg              sync.WaitGroup
					typs            = []coltypes.T{coltypes.Int64}
					semtyps         = []types.T{*types.Int}
					hashRouterInput = colexec.NewRandomDataOp(
						testAllocator,
						rng,
						colexec.RandomDataOpArgs{
							DeterministicTyps: typs,
							// Set a high number of batches to ensure that the HashRouter is
							// very far from being finished when the flow is shut down.
							NumBatches: math.MaxInt64,
							Selection:  true,
						},
					)
					numHashRouterOutputs        = 3
					numInboxes                  = numHashRouterOutputs + 3
					inboxes                     = make([]*colrpc.Inbox, 0, numInboxes+1)
					handleStreamErrCh           = make([]chan error, numInboxes+1)
					synchronizerInputs          = make([]colexec.Operator, 0, numInboxes)
					materializerMetadataSources = make([]execinfrapb.MetadataSource, 0, numInboxes+1)
					streamID                    = 0
					addAnotherRemote            = rng.Float64() < 0.5
				)

				// Create an allocator for each output.
				allocators := make([]*colexec.Allocator, numHashRouterOutputs)
				diskAccounts := make([]*mon.BoundAccount, numHashRouterOutputs)
				for i := range allocators {
					acc := testMemMonitor.MakeBoundAccount()
					defer acc.Close(ctxRemote)
					allocators[i] = colexec.NewAllocator(ctxRemote, &acc)
					diskAcc := testDiskMonitor.MakeBoundAccount()
					diskAccounts[i] = &diskAcc
					defer diskAcc.Close(ctxRemote)
				}
				hashRouter, hashRouterOutputs := colexec.NewHashRouter(
					allocators, hashRouterInput, typs, []uint32{0}, 64<<20, /* 64 MiB */
					queueCfg, &colexec.TestingSemaphore{}, diskAccounts, nil, /* toClose */
				)
				for i := 0; i < numInboxes; i++ {
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxLocal)
					inbox, err := colrpc.NewInbox(
						colexec.NewAllocator(ctxLocal, &inboxMemAccount), typs, execinfrapb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					materializerMetadataSources = append(materializerMetadataSources, inbox)
					synchronizerInputs = append(synchronizerInputs, colexec.Operator(inbox))
				}
				synchronizer := colexec.NewParallelUnorderedSynchronizer(synchronizerInputs, typs, &wg)
				flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}

				// idToClosed keeps track of whether Close was called for a given id.
				idToClosed := struct {
					syncutil.Mutex
					mapping map[int]bool
				}{}
				idToClosed.mapping = make(map[int]bool)
				runOutboxInbox := func(
					ctx context.Context,
					cancelFn context.CancelFunc,
					outboxMemAcc *mon.BoundAccount,
					outboxInput colexec.Operator,
					inbox *colrpc.Inbox,
					id int,
					outboxMetadataSources []execinfrapb.MetadataSource,
				) {
					idToClosed.Lock()
					idToClosed.mapping[id] = false
					idToClosed.Unlock()
					outbox, err := colrpc.NewOutbox(colexec.NewAllocator(ctx, outboxMemAcc), outboxInput, typs, append(outboxMetadataSources,
						execinfrapb.CallbackMetadataSource{
							DrainMetaCb: func(ctx context.Context) []execinfrapb.ProducerMetadata {
								return []execinfrapb.ProducerMetadata{{Err: errors.Errorf("%d", id)}}
							},
						},
					), []colexec.IdempotentCloser{callbackCloser{closeCb: func() error {
						idToClosed.Lock()
						idToClosed.mapping[id] = true
						idToClosed.Unlock()
						return nil
					}}})
					require.NoError(t, err)
					wg.Add(1)
					go func(id int) {
						outbox.Run(ctx, dialer, execinfra.StaticNodeID, flowID, execinfrapb.StreamID(id), cancelFn)
						wg.Done()
					}(id)

					require.NoError(t, err)
					serverStreamNotification := <-mockServer.InboundStreams
					serverStream := serverStreamNotification.Stream
					handleStreamErrCh[id] = make(chan error, 1)
					doneFn := func() { close(serverStreamNotification.Donec) }
					wg.Add(1)
					go func(id int, stream execinfrapb.DistSQL_FlowStreamServer, doneFn func()) {
						handleStreamErrCh[id] <- inbox.RunWithStream(stream.Context(), stream)
						doneFn()
						wg.Done()
					}(id, serverStream, doneFn)
				}

				wg.Add(1)
				go func() {
					hashRouter.Run(ctxRemote)
					wg.Done()
				}()
				for i := 0; i < numInboxes; i++ {
					var outboxMetadataSources []execinfrapb.MetadataSource
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxRemote)
					if i < numHashRouterOutputs {
						if i == 0 {
							// Only one outbox should drain the hash router.
							outboxMetadataSources = append(outboxMetadataSources, hashRouter)
						}
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, hashRouterOutputs[i], inboxes[i], streamID, outboxMetadataSources)
					} else {
						sourceMemAccount := testMemMonitor.MakeBoundAccount()
						defer sourceMemAccount.Close(ctxRemote)
						remoteAllocator := colexec.NewAllocator(ctxRemote, &sourceMemAccount)
						batch := remoteAllocator.NewMemBatch(typs)
						batch.SetLength(coldata.BatchSize())
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, colexec.NewRepeatableBatchSource(remoteAllocator, batch), inboxes[i], streamID, outboxMetadataSources)
					}
					streamID++
				}

				var materializerInput colexec.Operator
				ctxAnotherRemote, cancelAnotherRemote := context.WithCancel(context.Background())
				if addAnotherRemote {
					// Add another "remote" node to the flow.
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxAnotherRemote)
					inbox, err := colrpc.NewInbox(
						colexec.NewAllocator(ctxAnotherRemote, &inboxMemAccount),
						typs, execinfrapb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxAnotherRemote)
					runOutboxInbox(ctxAnotherRemote, cancelAnotherRemote, &outboxMemAccount, synchronizer, inbox, streamID, materializerMetadataSources)
					streamID++
					// There is now only a single Inbox on the "local" node which is the
					// only metadata source.
					materializerMetadataSources = []execinfrapb.MetadataSource{inbox}
					materializerInput = inbox
				} else {
					materializerInput = synchronizer
				}

				ctxLocal, cancelLocal := context.WithCancel(ctxLocal)
				materializerCalledClose := false
				materializer, err := colexec.NewMaterializer(
					flowCtx,
					1, /* processorID */
					materializerInput,
					semtyps,
					&execinfrapb.PostProcessSpec{},
					nil, /* output */
					materializerMetadataSources,
					[]colexec.IdempotentCloser{callbackCloser{closeCb: func() error {
						materializerCalledClose = true
						return nil
					}}}, /* toClose */
					nil, /* outputStatsToTrace */
					func() context.CancelFunc { return cancelLocal },
				)
				require.NoError(t, err)
				materializer.Start(ctxLocal)

				for i := 0; i < 10; i++ {
					row, meta := materializer.Next()
					require.NotNil(t, row)
					require.Nil(t, meta)
				}
				switch shutdownOperation {
				case consumerDone:
					materializer.ConsumerDone()
					receivedMetaFromID := make([]bool, streamID)
					metaCount := 0
					for {
						row, meta := materializer.Next()
						require.Nil(t, row)
						if meta == nil {
							break
						}
						metaCount++
						require.NotNil(t, meta.Err)
						id, err := strconv.Atoi(meta.Err.Error())
						require.NoError(t, err)
						require.False(t, receivedMetaFromID[id])
						receivedMetaFromID[id] = true
					}
					require.Equal(t, streamID, metaCount, fmt.Sprintf("received metadata from Outbox %+v", receivedMetaFromID))
				case consumerClosed:
					materializer.ConsumerClosed()
				}

				// When Outboxes are setup through vectorizedFlowCreator, the latter
				// keeps track of how many outboxes are on the node. When the last one
				// exits (and if there is no materializer on that node),
				// vectorizedFlowCreator will cancel the flow context of the node. To
				// simulate this, we manually cancel contexts of both remote nodes.
				cancelRemote()
				cancelAnotherRemote()

				for i := range inboxes {
					err = <-handleStreamErrCh[i]
					// We either should get no error or a context cancellation error.
					if err != nil {
						require.True(t, testutils.IsError(err, "context canceled"), err)
					}
				}
				wg.Wait()
				// Ensure all the outboxes called Close.
				for id, closed := range idToClosed.mapping {
					require.True(t, closed, "outbox with ID %d did not call Close on closers", id)
				}
				require.True(t, materializerCalledClose)
			})
		}
	}
}
