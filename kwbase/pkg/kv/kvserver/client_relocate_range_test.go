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

package kvserver_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func relocateAndCheck(
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	targets []roachpb.ReplicationTarget,
) (retries int) {
	testutils.SucceedsSoon(t, func() error {
		err := tc.Servers[0].DB().
			AdminRelocateRange(context.Background(), startKey.AsRawKey(), targets)
		if err != nil {
			retries++
		}
		return err
	})
	desc, err := tc.Servers[0].LookupRange(startKey.AsRawKey())
	require.NoError(t, err)
	requireDescMembers(t, desc, targets)
	requireLeaseAt(t, tc, desc, targets[0])
	return retries
}

func relocateTsAndCheck(
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	targets []roachpb.ReplicationTarget,
) {
	testutils.SucceedsSoon(t, func() error {
		err := tc.Servers[0].DB().
			AdminRelocateRange(context.Background(), startKey.AsRawKey(), targets)
		require.NoError(t, err)
		return err
	})
	desc, err := tc.Servers[0].LookupRange(startKey.AsRawKey())
	require.NoError(t, err)
	requireTsDescMembers(t, desc, targets)
	requireLeaseAt(t, tc, desc, targets[0])
}

// The pre-distributed range of the time series range will add
// insufficient targets to the range.InternalReplicas when relocating.
// see store.AdminRelocateRange for more details.
func requireTsDescMembers(
	t *testing.T, desc roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) {
	t.Helper()
	targets = append([]roachpb.ReplicationTarget(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].StoreID < targets[j].StoreID })

	have := make([]roachpb.ReplicationTarget, 0, len(targets))
	for _, rDesc := range desc.Replicas().All() {
		have = append(have, roachpb.ReplicationTarget{
			NodeID:  rDesc.NodeID,
			StoreID: rDesc.StoreID,
		})
	}
	sort.Slice(have, func(i, j int) bool { return have[i].StoreID < have[j].StoreID })
	//require.Equal(t, targets, have)
	contains := func(slice []roachpb.ReplicationTarget, x roachpb.ReplicationTarget) bool {
		for _, v := range slice {
			if x == v {
				return true
			}
		}
		return false
	}
	for i := 0; i < len(targets); i++ {
		require.Equal(t, true, contains(have, targets[i]))
	}

}

func requireDescMembers(
	t *testing.T, desc roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) {
	t.Helper()
	targets = append([]roachpb.ReplicationTarget(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].StoreID < targets[j].StoreID })

	have := make([]roachpb.ReplicationTarget, 0, len(targets))
	for _, rDesc := range desc.Replicas().All() {
		have = append(have, roachpb.ReplicationTarget{
			NodeID:  rDesc.NodeID,
			StoreID: rDesc.StoreID,
		})
	}
	sort.Slice(have, func(i, j int) bool { return have[i].StoreID < have[j].StoreID })
	require.Equal(t, targets, have)
}

func requireLeaseAt(
	t *testing.T,
	tc *testcluster.TestCluster,
	desc roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
) {
	t.Helper()
	// NB: under stressrace the lease will sometimes be inactive by the time
	// it's returned here, so don't use FindRangeLeaseHolder which fails when
	// that happens.
	testutils.SucceedsSoon(t, func() error {
		lease, _, err := tc.FindRangeLease(desc, &target)
		if err != nil {
			return err
		}
		if target != (roachpb.ReplicationTarget{
			NodeID:  lease.Replica.NodeID,
			StoreID: lease.Replica.StoreID,
		}) {
			return errors.Errorf("lease %v is not held by %+v", lease, target)
		}
		return nil
	})
}

func TestAdminRelocateRangeForTsWithoutTSSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	type intercept struct {
		ops         []roachpb.ReplicationChange
		leaseTarget *roachpb.ReplicationTarget
		err         error
	}
	var intercepted []intercept

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			BeforeRelocateOne: func(ops []roachpb.ReplicationChange, leaseTarget *roachpb.ReplicationTarget, err error) {
				intercepted = append(intercepted, intercept{
					ops:         ops,
					leaseTarget: leaseTarget,
					err:         err,
				})
			},
		},
	}
	args := base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	const BeginHash = 0
	splits := roachpb.AdminSplitForTsRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: sqlbase.MakeTsHashPointKey(sqlbase.ID(78), uint64(BeginHash), 2000),
		},
		TableId: keys.MinUserDescID,
		Keys:    []int32{BeginHash},
		HashNum: 2000,
	}

	// s1 (LH) ---> s2 (LH) s1 s3
	k := keys.MustAddr(tc.ScratchRangeForTs(t, splits))
	{
		targets := tc.Targets(1, 0, 2)
		// Expect two single additions, and that's it.
		relocateTsAndCheck(t, tc, k, targets)
	}

	// s1 (LH) s2 s3 ---> s4 (LH) s5 s6.
	// This is trickier because the leaseholder gets removed, and so do all
	// other replicas (i.e. a simple lease transfer at the beginning won't solve
	// the problem).
	{
		// Should carry out three swaps. Note that the leaseholder gets removed
		// in the process (i.e. internally the lease must've been moved around
		// to achieve that).
		relocateTsAndCheck(t, tc, k, tc.Targets(3, 4, 5))
	}

	// s4 (LH) s5 s6 ---> s5 (LH)
	// Pure downreplication.
	{
		relocateTsAndCheck(t, tc, k, tc.Targets(4))
	}

	// s5 (LH) ---> s3 (LH)
	// Lateral movement while at replication factor one. In this case atomic
	// replication changes cannot be used; we add-then-remove instead.
	{
		relocateTsAndCheck(t, tc, k, tc.Targets(2))
	}

	// s3 (LH) ---> s2 (LH) s4 s1 --> s4 (LH) s2 s6 s1 --> s3 (LH) s5
	// A grab bag.
	{
		// s3 -(add)-> s3 s2 -(swap)-> s4 s2 -(add)-> s4 s2 s1 (=s2 s4 s1)
		relocateTsAndCheck(t, tc, k, tc.Targets(1, 3, 0))
		// s2 s4 s1 -(add)-> s2 s4 s1 s6 (=s4 s2 s6 s1)
		relocateTsAndCheck(t, tc, k, tc.Targets(3, 1, 5, 0))
		// s4 s2 s6 s1 -(swap)-> s3 s2 s6 s1 -(swap)-> s3 s5 s6 s1 -(del)-> s3 s5 s6 -(del)-> s3 s5
		relocateTsAndCheck(t, tc, k, tc.Targets(2, 4))
	}
}

func TestAdminRelocateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	type intercept struct {
		ops         []roachpb.ReplicationChange
		leaseTarget *roachpb.ReplicationTarget
		err         error
	}
	var intercepted []intercept

	requireNumAtomic := func(expAtomic int, expSingle int, f func() (retries int)) {
		t.Helper()
		intercepted = nil
		retries := f()
		var actAtomic, actSingle int
		for _, ic := range intercepted {
			if ic.err != nil {
				continue
			}
			if len(ic.ops) == 2 && ic.ops[0].ChangeType == roachpb.ADD_REPLICA && ic.ops[1].ChangeType == roachpb.REMOVE_REPLICA {
				actAtomic++
			} else {
				actSingle++
			}
		}
		actAtomic -= retries
		require.Equal(t, expAtomic, actAtomic, "wrong number of atomic changes: %+v", intercepted)
		require.Equal(t, expSingle, actSingle, "wrong number of single changes: %+v", intercepted)
	}

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			BeforeRelocateOne: func(ops []roachpb.ReplicationChange, leaseTarget *roachpb.ReplicationTarget, err error) {
				intercepted = append(intercepted, intercept{
					ops:         ops,
					leaseTarget: leaseTarget,
					err:         err,
				})
			},
		},
	}
	args := base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	// s1 (LH) ---> s2 (LH) s1 s3
	// Pure upreplication.
	k := keys.MustAddr(tc.ScratchRange(t))
	{
		targets := tc.Targets(1, 0, 2)
		// Expect two single additions, and that's it.
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, targets)
		})
	}

	// s1 (LH) s2 s3 ---> s4 (LH) s5 s6.
	// This is trickier because the leaseholder gets removed, and so do all
	// other replicas (i.e. a simple lease transfer at the beginning won't solve
	// the problem).
	{
		targets := tc.Targets(3, 4, 5)
		// Should carry out three swaps. Note that the leaseholder gets removed
		// in the process (i.e. internally the lease must've been moved around
		// to achieve that).
		requireNumAtomic(3, 0, func() int {
			return relocateAndCheck(t, tc, k, targets)
		})
	}

	// s4 (LH) s5 s6 ---> s5 (LH)
	// Pure downreplication.
	{
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(4))
		})
	}

	// s5 (LH) ---> s3 (LH)
	// Lateral movement while at replication factor one. In this case atomic
	// replication changes cannot be used; we add-then-remove instead.
	{
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2))
		})
	}

	// s3 (LH) ---> s2 (LH) s4 s1 --> s4 (LH) s2 s6 s1 --> s3 (LH) s5
	// A grab bag.
	{
		// s3 -(add)-> s3 s2 -(swap)-> s4 s2 -(add)-> s4 s2 s1 (=s2 s4 s1)
		requireNumAtomic(1, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(1, 3, 0))
		})
		// s2 s4 s1 -(add)-> s2 s4 s1 s6 (=s4 s2 s6 s1)
		requireNumAtomic(0, 1, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(3, 1, 5, 0))
		})
		// s4 s2 s6 s1 -(swap)-> s3 s2 s6 s1 -(swap)-> s3 s5 s6 s1 -(del)-> s3 s5 s6 -(del)-> s3 s5
		requireNumAtomic(2, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4))
		})
	}
}

// Regression test for https://gitee.com/kwbasedb/kwbase/issues/64325
// which makes sure an in-flight read operation during replica removal won't
// return empty results.
func TestReplicaRemovalDuringGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, key, evalDuringReplicaRemoval := setupReplicaRemovalTest(t, ctx)
	defer tc.Stopper().Stop(ctx)

	// Perform write.
	pArgs := putArgs(key, []byte("foo"))
	_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), pArgs)
	require.Nil(t, pErr)

	// Perform delayed read during replica removal.
	resp, pErr := evalDuringReplicaRemoval(ctx, getArgs(key))
	require.Nil(t, pErr)
	require.NotNil(t, resp)
	require.NotNil(t, resp.(*roachpb.GetResponse).Value)
	val, err := resp.(*roachpb.GetResponse).Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), val)
}

// Regression test for https://gitee.com/kwbasedb/kwbase/issues/46329
// which makes sure an in-flight conditional put operation during replica
// removal won't spuriously error due to an unexpectedly missing value.
func TestReplicaRemovalDuringCPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, key, evalDuringReplicaRemoval := setupReplicaRemovalTest(t, ctx)
	defer tc.Stopper().Stop(ctx)

	// Perform write.
	pArgs := putArgs(key, []byte("foo"))
	_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), pArgs)
	require.Nil(t, pErr)

	// Perform delayed conditional put during replica removal. This will cause
	// an ambiguous result error, as outstanding proposals in the leaseholder
	// replica's proposal queue will be aborted when the replica is removed.
	// If the replica was removed from under us, it would instead return a
	// ConditionFailedError since it finds nil in place of "foo".
	req := cPutArgs(key, []byte("bar"), []byte("foo"))
	_, pErr = evalDuringReplicaRemoval(ctx, req)
	require.NotNil(t, pErr)
	require.IsType(t, &roachpb.AmbiguousResultError{}, pErr.GetDetail())
}

// setupReplicaRemovalTest sets up a test cluster that can be used to test
// request evaluation during replica removal. It returns a running test
// cluster, the first key of a blank scratch range on the replica to be
// removed, and a function that can execute a delayed request just as the
// replica is being removed.
func setupReplicaRemovalTest(
	t *testing.T, ctx context.Context,
) (
	*testcluster.TestCluster,
	roachpb.Key,
	func(context.Context, roachpb.Request) (roachpb.Response, *roachpb.Error),
) {
	t.Helper()

	type magicKey struct{}

	requestReadyC := make(chan struct{}) // signals main thread that request is teed up
	requestEvalC := make(chan struct{})  // signals cluster to evaluate the request
	evalFilter := func(args storagebase.FilterArgs) *roachpb.Error {
		if args.Ctx.Value(magicKey{}) != nil {
			requestReadyC <- struct{}{}
			<-requestEvalC
		}
		return nil
	}

	manual := hlc.NewHybridManualClock()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: storagebase.BatchEvalTestingKnobs{
						TestingEvalFilter: evalFilter,
					},
				},
				Server: &server.TestingKnobs{
					ClockSource: manual.UnixNano,
				},
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, args)

	// Create range and upreplicate.
	key := tc.ScratchRange(t)
	tc.AddReplicasOrFatal(t, key, tc.Target(1))

	// Return a function that can be used to evaluate a delayed request
	// during replica removal.
	evalDuringReplicaRemoval := func(ctx context.Context, req roachpb.Request) (roachpb.Response, *roachpb.Error) {
		// Submit request and wait for it to block.
		type result struct {
			resp roachpb.Response
			err  *roachpb.Error
		}
		resultC := make(chan result)
		err := tc.Stopper().RunAsyncTask(ctx, "request", func(ctx context.Context) {
			reqCtx := context.WithValue(ctx, magicKey{}, struct{}{})
			resp, pErr := kv.SendWrapped(reqCtx, tc.Servers[0].DistSender(), req)
			resultC <- result{resp, pErr}
		})
		require.NoError(t, err)
		<-requestReadyC

		// Transfer leaseholder to other store.
		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)
		store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(tc.Server(0).GetFirstStoreID())
		require.NoError(t, err)
		repl, err := store.GetReplica(rangeDesc.RangeID)
		require.NoError(t, err)
		err = tc.MoveRangeLeaseNonCooperatively(rangeDesc, tc.Target(1), manual)
		require.NoError(t, err)

		// Remove first store from raft group.
		tc.RemoveReplicasOrFatal(t, key, tc.Target(0))

		// Wait for replica removal. This is a bit iffy. We want to make sure
		// that, in the buggy case, we will typically fail (i.e. the request
		// returns incorrect results because the replica was removed). However,
		// in the non-buggy case the in-flight request will be holding
		// readOnlyCmdMu until evaluated, blocking the replica removal, so
		// waiting for replica removal would deadlock. We therefore take the
		// easy way out by starting an async replica GC and sleeping for a bit.
		err = tc.Stopper().RunAsyncTask(ctx, "replicaGC", func(ctx context.Context) {
			assert.NoError(t, store.ManualReplicaGC(repl))
		})
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// Allow request to resume, and return the result.
		close(requestEvalC)
		r := <-resultC
		return r.resp, r.err
	}

	return tc, key, evalDuringReplicaRemoval
}
