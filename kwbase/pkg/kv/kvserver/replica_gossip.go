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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

const configGossipTTL = 0 // does not expire

func (r *Replica) gossipFirstRange(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Gossip is not provided for the bootstrap store and for some tests.
	if r.store.Gossip() == nil {
		return
	}
	log.Event(ctx, "gossiping sentinel and first range")
	if log.V(1) {
		log.Infof(ctx, "gossiping sentinel from store %d, r%d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(
		gossip.KeySentinel, r.store.ClusterID().GetBytes(),
		r.store.cfg.SentinelGossipTTL()); err != nil {
		log.Errorf(ctx, "failed to gossip sentinel: %+v", err)
	}
	if log.V(1) {
		log.Infof(ctx, "gossiping first range from store %d, r%d: %s",
			r.store.StoreID(), r.RangeID, r.mu.state.Desc.Replicas())
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.mu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %+v", err)
	}
}

// shouldGossip returns true if this replica should be gossiping. Gossip is
// inherently inconsistent and asynchronous, we're using the lease as a way to
// ensure that only one node gossips at a time.
func (r *Replica) shouldGossip() bool {
	return r.OwnsValidLease(r.store.Clock().Now())
}

// MaybeGossipSystemConfig scans the entire SystemConfig span and gossips it.
// Further calls come from the trigger on EndTxn or range lease acquisition.
//
// Note that MaybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a range lease
// here since RequestLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrAcquireLease.
//
// MaybeGossipSystemConfig must only be called from Raft commands
// (which provide the necessary serialization to avoid data races).
//
// TODO(nvanbenschoten,bdarnell): even though this is best effort, we
// should log louder when we continually fail to gossip system config.
func (r *Replica) MaybeGossipSystemConfig(ctx context.Context) error {
	if r.store.Gossip() == nil {
		log.VEventf(ctx, 2, "not gossiping system config because gossip isn't initialized")
		return nil
	}
	if !r.IsInitialized() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica isn't initialized")
		return nil
	}
	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		log.VEventf(ctx, 3,
			"not gossiping system config because the replica doesn't contain the system config's start key")
		return nil
	}
	if !r.shouldGossip() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica doesn't hold the lease")
		return nil
	}

	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	loadedCfg, err := r.loadSystemConfig(ctx)
	if err != nil {
		if err == errSystemConfigIntent {
			log.VEventf(ctx, 2, "not gossiping system config because intents were found on SystemConfigSpan")
			r.markSystemConfigGossipFailed()
			return nil
		}
		return errors.Wrap(err, "could not load SystemConfig span")
	}

	if gossipedCfg := r.store.Gossip().GetSystemConfig(); gossipedCfg != nil && gossipedCfg.Equal(loadedCfg) &&
		r.store.Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
		log.VEventf(ctx, 2, "not gossiping unchanged system config")
		// Clear the failure bit if all intents have been resolved but there's
		// nothing new to gossip.
		r.markSystemConfigGossipSuccess()
		return nil
	}

	log.VEventf(ctx, 2, "gossiping system config")
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, loadedCfg, 0); err != nil {
		return errors.Wrap(err, "failed to gossip system config")
	}
	r.markSystemConfigGossipSuccess()
	return nil
}

// MaybeGossipSystemConfigIfHaveFailure is a trigger to gossip the system config
// due to an abort of a transaction keyed in the system config span. It will
// call MaybeGossipSystemConfig if failureToGossipSystemConfig is true.
func (r *Replica) MaybeGossipSystemConfigIfHaveFailure(ctx context.Context) error {
	r.mu.RLock()
	failed := r.mu.failureToGossipSystemConfig
	r.mu.RUnlock()
	if !failed {
		return nil
	}
	return r.MaybeGossipSystemConfig(ctx)
}

// MaybeGossipNodeLiveness gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica
// must hold the lease to a range which contains some or all of the
// node liveness records. After scanning the records, it checks
// against what's already in gossip and only gossips records which
// are out of date.
func (r *Replica) MaybeGossipNodeLiveness(ctx context.Context, span roachpb.Span) error {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return nil
	}

	if !r.ContainsKeyRange(span.Key, span.EndKey) || !r.shouldGossip() {
		return nil
	}

	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
	// Call evaluateBatch instead of Send to avoid reacquiring latches.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	rw := r.Engine().NewReadOnly()
	defer rw.Close()

	br, result, pErr :=
		evaluateBatch(ctx, storagebase.CmdIDKey(""), rw, rec, nil, &ba, true /* readOnly */)
	if pErr != nil {
		return errors.Wrapf(pErr.GoError(), "couldn't scan node liveness records in span %s", span)
	}
	if len(result.Local.EncounteredIntents) > 0 {
		return errors.Errorf("unexpected intents on node liveness span %s: %+v", span, result.Local.EncounteredIntents)
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	log.VEventf(ctx, 2, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	for _, kv := range kvs {
		var kvLiveness, gossipLiveness storagepb.Liveness
		if err := kv.Value.GetProto(&kvLiveness); err != nil {
			return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
		}
		key := gossip.MakeNodeLivenessKey(kvLiveness.NodeID)
		// Look up liveness from gossip; skip gossiping anew if unchanged.
		if err := r.store.Gossip().GetInfoProto(key, &gossipLiveness); err == nil {
			if gossipLiveness == kvLiveness && r.store.Gossip().InfoOriginatedHere(key) {
				continue
			}
		}
		if err := r.store.Gossip().AddInfoProto(key, &kvLiveness, 0); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness (%+v)", kvLiveness)
		}
	}
	return nil
}

var errSystemConfigIntent = errors.New("must retry later due to intent on SystemConfigSpan")

// loadSystemConfig scans the system config span and returns the system
// config.
func (r *Replica) loadSystemConfig(ctx context.Context) (*config.SystemConfigEntries, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(keys.SystemConfigSpan)})
	// Call evaluateBatch instead of Send to avoid reacquiring latches.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	rw := r.Engine().NewReadOnly()
	defer rw.Close()

	br, result, pErr := evaluateBatch(
		ctx, storagebase.CmdIDKey(""), rw, rec, nil, &ba, true, /* readOnly */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	if intents := result.Local.DetachEncounteredIntents(); len(intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		// This is called from handleReadWriteLocalEvalResult (with raftMu
		// locked), so disallow synchronous processing (which blocks that mutex
		// for too long and is a potential deadlock).
		if err := r.store.intentResolver.CleanupIntentsAsync(ctx, intents, false /* allowSync */); err != nil {
			log.Warning(ctx, err)
		}
		return nil, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	sysCfg := &config.SystemConfigEntries{}
	sysCfg.Values = kvs
	return sysCfg, nil
}

// getLeaseForGossip tries to obtain a range lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *roachpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, roachpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *roachpb.Error
	if err := r.store.Stopper().RunTask(
		ctx, "storage.Replica: acquiring lease to gossip",
		func(ctx context.Context) {
			// Check for or obtain the lease, if none active.
			_, pErr = r.redirectOnOrAcquireLease(ctx)
			hasLease = pErr == nil
			if pErr != nil {
				switch e := pErr.GetDetail().(type) {
				case *roachpb.NotLeaseHolderError:
					// NotLeaseHolderError means there is an active lease, but only if
					// the lease holder is set; otherwise, it's likely a timeout.
					if e.LeaseHolder != nil {
						pErr = nil
					}
				default:
					// Any other error is worth being logged visibly.
					log.Warningf(ctx, "could not acquire lease for range gossip: %s", e)
				}
			}
		}); err != nil {
		pErr = roachpb.NewError(err)
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a range lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if gossipClusterID, err := r.store.Gossip().GetClusterID(); err == nil {
		if gossipClusterID != r.store.ClusterID() {
			log.Fatalf(
				ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
				r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster id %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddClusterID(r.store.ClusterID()); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %+v", err)
	}

	if r.store.cfg.TestingKnobs.DisablePeriodicGossips {
		return nil
	}

	hasLease, pErr := r.getLeaseForGossip(ctx)
	if pErr != nil {
		return pErr
	} else if !hasLease {
		return nil
	}
	r.gossipFirstRange(ctx)
	return nil
}
