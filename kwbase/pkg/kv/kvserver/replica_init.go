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

package kvserver

import (
	"bytes"
	"context"
	"math/rand"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/abortspan"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/split"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

const (
	splitQueueThrottleDuration = 5 * time.Second
	mergeQueueThrottleDuration = 5 * time.Second
)

// newReplica constructs a new Replica. If the desc is initialized, the store
// must be present in it and the corresponding replica descriptor must have
// replicaID as its ReplicaID.
func newReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	repl := newUnloadedReplica(ctx, desc, store, replicaID)
	repl.raftMu.Lock()
	defer repl.raftMu.Unlock()
	repl.mu.Lock()
	defer repl.mu.Unlock()
	if err := repl.loadRaftMuLockedReplicaMuLocked(desc, true); err != nil {
		return nil, err
	}
	return repl, nil
}

// newUnloadedReplica partially constructs a replica. The primary reason this
// function exists separately from Replica.loadRaftMuLockedReplicaMuLocked() is
// to avoid attempting to fully constructing a Replica prior to proving that it
// can exist during the delicate synchronization dance that occurs in
// Store.tryGetOrCreateReplica(). A Replica returned from this function must not
// be used in any way until it's load() method has been called.
func newUnloadedReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) *Replica {
	if replicaID == 0 {
		log.Fatalf(context.TODO(), "cannot construct a replica for range %d with a 0 replica ID", desc.RangeID)
	}
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        desc.RangeID,
		store:          store,
		abortSpan:      abortspan.New(desc.RangeID),
		concMgr: concurrency.NewManager(concurrency.Config{
			NodeDesc:          store.nodeDesc,
			RangeDesc:         desc,
			Settings:          store.ClusterSettings(),
			DB:                store.DB(),
			Clock:             store.Clock(),
			Stopper:           store.Stopper(),
			IntentResolver:    store.intentResolver,
			TxnWaitMetrics:    store.txnWaitMetrics,
			SlowLatchGauge:    store.metrics.SlowLatchRequests,
			DisableTxnPushing: store.TestingKnobs().DontPushOnWriteIntentError,
			TxnWaitKnobs:      store.TestingKnobs().TxnWaitKnobs,
		}),
	}
	r.mu.pendingLeaseRequest = makePendingLeaseRequest(r)
	r.mu.stateLoader = stateloader.Make(desc.RangeID)
	r.mu.quiescent = true
	r.mu.zone = store.cfg.DefaultZoneConfig
	r.mu.replicaID = replicaID
	split.Init(&r.loadBasedSplitter, rand.Intn, func() float64 {
		return float64(SplitByLoadQPSThreshold.Get(&store.cfg.Settings.SV))
	})
	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]ReplicaChecksum{}
	r.mu.proposalBuf.Init((*replicaProposer)(r))

	if leaseHistoryMaxEntries > 0 {
		r.leaseHistory = newLeaseHistory()
	}
	if store.cfg.StorePool != nil {
		r.leaseholderStats = newReplicaStats(store.Clock(), store.cfg.StorePool.getNodeLocalityString)
	}
	// Pass nil for the localityOracle because we intentionally don't track the
	// origin locality of write load.
	r.writeStats = newReplicaStats(store.Clock(), nil)

	// Init rangeStr with the range ID.
	r.rangeStr.store(replicaID, &roachpb.RangeDescriptor{RangeID: desc.RangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTag("@", fmt.Sprintf("%x", unsafe.Pointer(r)))
	r.raftMu.stateLoader = stateloader.Make(desc.RangeID)

	r.splitQueueThrottle = util.Every(splitQueueThrottleDuration)
	r.mergeQueueThrottle = util.Every(mergeQueueThrottleDuration)
	return r
}

// loadRaftMuLockedReplicaMuLocked will load the state of the replica from disk.
// If desc is initialized, the Replica will be initialized when this method
// returns. An initialized Replica may not be reloaded. If this method is called
// with an uninitialized desc it may be called again later with an initialized
// desc.
//
// This method is called in three places:
//
//  1. newReplica - used when the store is initializing and during testing
//  2. tryGetOrCreateReplica - see newUnloadedReplica
//  3. splitPostApply - this call initializes a previously uninitialized Replica.
func (r *Replica) loadRaftMuLockedReplicaMuLocked(
	desc *roachpb.RangeDescriptor, isInit bool,
) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	} else if r.mu.replicaID == 0 {
		// NB: This is just a defensive check as r.mu.replicaID should never be 0.
		log.Fatalf(ctx, "r%d: cannot initialize replica without a replicaID", desc.RangeID)
	}

	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	r.mu.inconsistent, _ = r.mu.stateLoader.LoadInconsistent(ctx, r.Engine())

	var err error
	if r.mu.state, err = r.mu.stateLoader.Load(ctx, r.Engine(), desc); err != nil {
		return err
	}
	r.mu.lastIndex, err = r.mu.stateLoader.LoadLastIndex(ctx, r.Engine())
	if err != nil {
		return err
	}
	r.mu.lastTerm = invalidLastTerm
	if r.isTsLocked() {
		// if we use the mode that raft log combines with WAL to guarantee the data integrity,
		// we need to adjust applied index with ts flushed index.
		var hasTsFlushedIndex bool
		if isInit {
			// start node, TsRaftLogCombineWAL is unavailable yet, check r.mu.tsFlushedIndex
			r.mu.tsFlushedIndex, err = r.mu.stateLoader.LoadTsFlushedIndex(ctx, r.Engine())
			if err != nil {
				return err
			}
			hasTsFlushedIndex = r.mu.tsFlushedIndex > 0
		} else {
			// create replica, r.mu.tsFlushedIndex is not set and TsRaftLogCombineWAL
			// is available, directly check TsRaftLogCombineWAL
			hasTsFlushedIndex = tse.TsRaftLogCombineWAL.Get(&r.store.ClusterSettings().SV)
			if hasTsFlushedIndex && r.mu.tsFlushedIndex == 0 {
				// just created, set r.mu.state.TruncatedState.Index to r.mu.tsFlushedIndex
				r.mu.tsFlushedIndex = r.mu.state.TruncatedState.Index
				if err := r.mu.stateLoader.SetTsFlushedIndex(ctx, r.Engine(), r.mu.tsFlushedIndex); err != nil {
					log.Warningf(ctx, "failed SetTsFlushedIndex, err: %v", err)
				}
			}
		}
		hs, err := r.mu.stateLoader.LoadHardState(ctx, r.store.Engine())
		// For uninitialized ranges, membership is unknown at this point.
		if err != nil {
			log.Errorf(ctx, "failed get hard state,err: %+v", err)
			return err
		}
		log.Infof(ctx, "init %v, truncate index is %d, tsFlushedIndex is %d, RaftAppliedIndex is %d, lastIndex: %d, "+
			"hardState: %s, inconsistent: %v", isInit, r.mu.state.TruncatedState.Index, r.mu.tsFlushedIndex,
			r.mu.state.RaftAppliedIndex, r.mu.lastIndex, hs, r.mu.inconsistent)
		if hasTsFlushedIndex && r.mu.tsFlushedIndex < r.mu.state.RaftAppliedIndex {
			// only set RaftAppliedIndex to tsFlushedIndex when node starts.
			r.mu.state.RaftAppliedIndex = r.mu.tsFlushedIndex
			if _, err := r.mu.stateLoader.Save(ctx, r.Engine(), r.mu.state, stateloader.TruncatedStateUnreplicated); err != nil {
				log.Warningf(ctx, "failed Save ReplicaState, err: %v", err)
			}
		}
		if hs.Commit < r.mu.state.RaftAppliedIndex {
			hs.Commit = r.mu.state.RaftAppliedIndex
			if hs.Term > r.mu.state.TruncatedState.Term {
				hs.Vote = 0
			}
			hs.Term = r.mu.state.TruncatedState.Term
			if err = r.mu.stateLoader.SetHardState(ctx, r.Engine(), hs); err != nil {
				log.Errorf(ctx, "failed set hard state,err: %+v", err)
				return err
			}
		}
		if r.mu.lastIndex < r.mu.state.TruncatedState.Index {
			r.mu.lastIndex = r.mu.state.TruncatedState.Index
		} else if r.mu.lastIndex < r.mu.state.RaftAppliedIndex {
			r.mu.lastIndex = r.mu.state.RaftAppliedIndex
		}
	}
	if isInit {
		r.store.nodeDesc.RangeIndex = append(r.store.nodeDesc.RangeIndex, roachpb.RangeIndex{
			RangeId:    int64(r.RangeID),
			ApplyIndex: int64(r.mu.state.RaftAppliedIndex),
		})
	}

	// Ensure that we're not trying to load a replica with a different ID than
	// was used to construct this Replica.
	replicaID := r.mu.replicaID
	if replicaDesc, found := r.mu.state.Desc.GetReplicaDescriptor(r.StoreID()); found {
		replicaID = replicaDesc.ReplicaID
	} else if desc.IsInitialized() {
		log.Fatalf(ctx, "r%d: cannot initialize replica which is not in descriptor %v", desc.RangeID, desc)
	}
	if r.mu.replicaID != replicaID {
		log.Fatalf(ctx, "attempting to initialize a replica which has ID %d with ID %d",
			r.mu.replicaID, replicaID)
	}

	r.setDescLockedRaftMuLocked(ctx, desc)

	// Init the minLeaseProposedTS such that we won't use an existing lease (if
	// any). This is so that, after a restart, we don't propose under old leases.
	// If the replica is being created through a split, this value will be
	// overridden.
	if !r.store.cfg.TestingKnobs.DontPreventUseOfOldLeaseOnStart {
		// Only do this if there was a previous lease. This shouldn't be important
		// to do but consider that the first lease which is obtained is back-dated
		// to a zero start timestamp (and this de-flakes some tests). If we set the
		// min proposed TS here, this lease could not be renewed (by the semantics
		// of minLeaseProposedTS); and since minLeaseProposedTS is copied on splits,
		// this problem would multiply to a number of replicas at cluster bootstrap.
		// Instead, we make the first lease special (which is OK) and the problem
		// disappears.
		if r.mu.state.Lease.Sequence > 0 {
			r.mu.minLeaseProposedTS = r.Clock().Now()
		}
	}

	ssBase := r.Engine().GetAuxiliaryDir()
	if r.raftMu.sideloaded, err = newDiskSideloadStorage(
		r.store.cfg.Settings,
		desc.RangeID,
		replicaID,
		ssBase,
		r.store.limiters.BulkIOWriteRate,
		r.store.engine,
	); err != nil {
		return errors.Wrap(err, "while initializing sideloaded storage")
	}
	r.assertStateLocked(ctx, r.store.Engine())
	return nil
}

// IsInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInitializedRLocked()
}

// isInitializedRLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedRLocked() bool {
	return r.mu.state.Desc.IsInitialized()
}

// maybeInitializeRaftGroup check whether the internal Raft group has
// not yet been initialized. If not, it is created and set to campaign
// if this replica is the most recent owner of the range lease.
func (r *Replica) maybeInitializeRaftGroup(ctx context.Context) {
	r.mu.RLock()
	// If this replica hasn't initialized the Raft group, create it and
	// unquiesce and wake the leader to ensure the replica comes up to date.
	initialized := r.mu.internalRaftGroup != nil
	// If this replica has been removed or is in the process of being removed
	// then it'll never handle any raft events so there's no reason to initialize
	// it now.
	removed := !r.mu.destroyStatus.IsAlive()
	r.mu.RUnlock()
	if initialized || removed {
		return
	}

	// Acquire raftMu, but need to maintain lock ordering (raftMu then mu).
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we raced on checking the destroyStatus above that's fine as
	// the below withRaftGroupLocked will no-op.
	if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil && err != errRemoved {
		log.VErrEventf(ctx, 1, "unable to initialize raft group: %s", err)
	}
}

// setDescRaftMuLocked atomically sets the replica's descriptor. It requires raftMu to be
// locked.
func (r *Replica) setDescRaftMuLocked(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setDescLockedRaftMuLocked(ctx, desc)
}

func (r *Replica) setDescLockedRaftMuLocked(ctx context.Context, desc *roachpb.RangeDescriptor) {
	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}
	if r.mu.state.Desc.IsInitialized() &&
		!r.mu.state.Desc.StartKey.Equal(desc.StartKey) {
		log.Fatalf(ctx, "attempted to change replica's start key from %s to %s",
			r.mu.state.Desc.StartKey, desc.StartKey)
	}

	// NB: It might be nice to assert that the current replica exists in desc
	// however we allow it to not be present for three reasons:
	//
	//   1) When removing the current replica we update the descriptor to the point
	//      of removal even though we will delete the Replica's data in the same
	//      batch. We could avoid setting the local descriptor in this case.
	//   2) When the DisableEagerReplicaRemoval testing knob is enabled. We
	//      could remove all tests which utilize this behavior now that there's
	//      no other mechanism for range state which does not contain the current
	//      store to exist on disk.
	//   3) Various unit tests do not provide a valid descriptor.
	replDesc, found := desc.GetReplicaDescriptor(r.StoreID())
	if found && replDesc.ReplicaID != r.mu.replicaID {
		log.Fatalf(ctx, "attempted to change replica's ID from %d to %d",
			r.mu.replicaID, replDesc.ReplicaID)
	}

	// Determine if a new replica was added. This is true if the new max replica
	// ID is greater than the old max replica ID.
	oldMaxID := maxReplicaIDOfAny(r.mu.state.Desc)
	newMaxID := maxReplicaIDOfAny(desc)
	if newMaxID > oldMaxID {
		r.mu.lastReplicaAdded = newMaxID
		r.mu.lastReplicaAddedTime = timeutil.Now()
	} else if r.mu.lastReplicaAdded > newMaxID {
		// The last replica added was removed.
		r.mu.lastReplicaAdded = 0
		r.mu.lastReplicaAddedTime = time.Time{}
	}

	r.rangeStr.store(r.mu.replicaID, desc)
	r.connectionClass.set(rpc.ConnectionClassForKey(desc.StartKey))
	r.concMgr.OnRangeDescUpdated(desc)
	r.mu.state.Desc = desc

	// Prioritize the NodeLiveness Range in the Raft scheduler above all other
	// Ranges to ensure that liveness never sees high Raft scheduler latency.
	if bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix) {
		r.store.scheduler.SetPriorityID(desc.RangeID)
	}
}
