// Copyright 2015 The Cockroach Authors.
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

package kvserver_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func TestGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	errors := make(chan error, 1)
	descs := make(chan *roachpb.RangeDescriptor)
	unregister := tc.Servers[0].Gossip().RegisterCallback(gossip.KeyFirstRangeDescriptor,
		func(_ string, content roachpb.Value) {
			var desc roachpb.RangeDescriptor
			if err := content.GetProto(&desc); err != nil {
				select {
				case errors <- err:
				default:
				}
			} else {
				select {
				case descs <- &desc:
				case <-time.After(45 * time.Second):
					t.Logf("had to drop descriptor %+v", desc)
				}
			}
		},
		// Redundant callbacks are required by this test.
		gossip.Redundant,
	)
	// Unregister the callback before attempting to stop the stopper to prevent
	// deadlock. This is still flaky in theory since a callback can fire between
	// the last read from the channels and this unregister, but testing has
	// shown this solution to be sufficiently robust for now.
	defer unregister()

	// Wait for the specified descriptor to be gossiped for the first range. We
	// loop because the timing of replica addition and lease transfer can cause
	// extra gossiping of the first range.
	waitForGossip := func(desc roachpb.RangeDescriptor) {
		for {
			select {
			case err := <-errors:
				t.Fatal(err)
			case gossiped := <-descs:
				if reflect.DeepEqual(&desc, gossiped) {
					return
				}
				log.Infof(context.TODO(), "expected\n%+v\nbut found\n%+v", desc, gossiped)
			}
		}
	}

	// Expect an initial callback of the first range descriptor.
	select {
	case err := <-errors:
		t.Fatal(err)
	case <-descs:
	}

	// Add two replicas. The first range descriptor should be gossiped after each
	// addition.
	var desc roachpb.RangeDescriptor
	firstRangeKey := keys.MinKey
	for i := 1; i <= 2; i++ {
		var err error
		if desc, err = tc.AddReplicas(firstRangeKey, tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		waitForGossip(desc)
	}

	// Transfer the lease to a new node. This should cause the first range to be
	// gossiped again.
	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// Remove a non-lease holder replica.
	desc, err := tc.RemoveReplicas(firstRangeKey, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// TODO(peter): Re-enable or remove when we've resolved the discussion
	// about removing the lease-holder replica. See #7872.

	// // Remove the lease holder replica.
	// leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	// desc, err = tc.RemoveReplicas(firstRangeKey, leaseHolder)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// select {
	// case err := <-errors:
	// 	t.Fatal(err)
	// case gossiped := <-descs:
	// 	if !reflect.DeepEqual(desc, gossiped) {
	// 		t.Fatalf("expected\n%+v\nbut found\n%+v", desc, gossiped)
	// 	}
	// }
}

// TestGossipHandlesReplacedNode tests that we can shut down a node and
// replace it with a new node at the same address (simulating a node getting
// restarted after losing its data) without the cluster breaking.
func TestGossipHandlesReplacedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		// As of Nov 2018 it takes 3.6s.
		t.Skip("short")
	}
	ctx := context.Background()

	// Shorten the raft tick interval and election timeout to make range leases
	// much shorter than normal. This keeps us from having to wait so long for
	// the replaced node's leases to time out, but has still shown itself to be
	// long enough to avoid flakes.
	serverArgs := base.TestServerArgs{
		Addr:     util.IsolatedTestAddr.String(),
		Insecure: true, // because our certs are only valid for 127.0.0.1
		RetryOptions: retry.Options{
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
	}
	serverArgs.RaftTickInterval = 50 * time.Millisecond
	serverArgs.RaftElectionTimeoutTicks = 10

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ServerArgs: serverArgs,
		})
	defer tc.Stopper().Stop(context.TODO())

	// Take down the first node and replace it with a new one.
	oldNodeIdx := 0
	newServerArgs := serverArgs
	newServerArgs.Addr = tc.Servers[oldNodeIdx].ServingRPCAddr()
	newServerArgs.SQLAddr = tc.Servers[oldNodeIdx].ServingSQLAddr()
	newServerArgs.PartOfCluster = true
	newServerArgs.JoinAddr = tc.Servers[1].ServingRPCAddr()
	log.Infof(ctx, "stopping server %d", oldNodeIdx)
	tc.StopServer(oldNodeIdx)
	tc.AddServer(t, newServerArgs)

	tc.WaitForStores(t, tc.Server(1).GossipI().(*gossip.Gossip))

	// Ensure that all servers still running are responsive. If the two remaining
	// original nodes don't refresh their connection to the address of the first
	// node, they can get stuck here.
	for i, server := range tc.Servers {
		if i == oldNodeIdx {
			continue
		}
		kvClient := server.DB()
		if err := kvClient.Put(ctx, fmt.Sprintf("%d", i), i); err != nil {
			t.Errorf("failed Put to node %d: %+v", i, err)
		}
	}
}

// TestGossipAfterAbortOfSystemConfigTransactionAfterFailureDueToIntents tests
// that failures to gossip the system config due to intents are rectified when
// later intents are aborted.
func TestGossipAfterAbortOfSystemConfigTransactionAfterFailureDueToIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	db := tc.Server(0).DB()

	txA := db.NewTxn(ctx, "a")
	txB := db.NewTxn(ctx, "b")

	require.NoError(t, txA.SetSystemConfigTrigger())
	require.NoError(t, txA.Put(ctx, keys.DescMetadataKey(1000), "foo"))

	require.NoError(t, txB.SetSystemConfigTrigger())
	require.NoError(t, txB.Put(ctx, keys.DescMetadataKey(2000), "bar"))

	const someTime = 10 * time.Millisecond
	clearNotifictions := func(ch <-chan struct{}) {
		for {
			select {
			case <-ch:
			case <-time.After(someTime):
				return
			}
		}
	}
	systemConfChangeCh := tc.Server(0).GossipI().(*gossip.Gossip).RegisterSystemConfigChannel()
	clearNotifictions(systemConfChangeCh)
	require.NoError(t, txB.Commit(ctx))
	select {
	case <-systemConfChangeCh:
		// This case is rare but happens sometimes. We gossip the node liveness
		// in a bunch of cases so we just let the test finish here. The important
		// thing is that sometimes we get to the next phase.
		t.Log("got unexpected update. This can happen for a variety of " +
			"reasons like lease transfers. The test is exiting without testing anything")
		return
	case <-time.After(someTime):
		// Did not expect an update so this is the happy case
	}
	// Roll back the transaction which had laid down the intent which blocked the
	// earlier gossip update, make sure we get a gossip notification now.
	const aLongTime = 20 * someTime
	require.NoError(t, txA.Rollback(ctx))
	select {
	case <-systemConfChangeCh:
		// Got an update.
	case <-time.After(aLongTime):
		t.Fatal("expected update")
	}
}
