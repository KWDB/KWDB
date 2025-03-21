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

package kvserver

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/tracker"
)

const (
	// raftSnapshotQueueTimerDuration is the duration between Raft snapshot of
	// queued replicas.
	raftSnapshotQueueTimerDuration = 0 // zero duration to process Raft snapshots greedily

	raftSnapshotPriority float64 = 0
)

// raftSnapshotQueue manages a queue of replicas which may need to catch a
// replica up with a snapshot to their range.
type raftSnapshotQueue struct {
	*baseQueue
}

// newRaftSnapshotQueue returns a new instance of raftSnapshotQueue.
func newRaftSnapshotQueue(store *Store, g *gossip.Gossip) *raftSnapshotQueue {
	rq := &raftSnapshotQueue{}
	rq.baseQueue = newBaseQueue(
		"raftsnapshot", rq, store, g,
		queueConfig{
			maxSize:        defaultQueueMaxSize,
			maxConcurrency: 10,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft snapshots cannot be performed by the replicateQueue.
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(recoverySnapshotRate),
			successes:            store.metrics.RaftSnapshotQueueSuccesses,
			failures:             store.metrics.RaftSnapshotQueueFailures,
			pending:              store.metrics.RaftSnapshotQueuePending,
			processingNanos:      store.metrics.RaftSnapshotQueueProcessingNanos,
		},
	)
	return rq
}

func (rq *raftSnapshotQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ *config.SystemConfig,
) (shouldQ bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {

		// raft.Status.Progress is only populated on the Raft group leader.
		for _, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(2) {
					log.Infof(ctx, "raft snapshot needed, enqueuing")
				}
				//if repl.isTs() {
				//	panic(fmt.Sprintf("into raft snapshot queue should queue"))
				//}
				return true, raftSnapshotPriority
			}
		}
	}
	return false, 0
}

func (rq *raftSnapshotQueue) process(
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) error {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(1) {
					log.Infof(ctx, "sending raft snapshot")
				}
				if err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id roachpb.ReplicaID,
) error {
	desc := repl.Desc()

	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas())
	}
	snapType := SnapshotRequest_RAFT
	// A learner replica is either getting a snapshot of type LEARNER by the node
	// that's adding it or it's been orphaned and it's about to be cleaned up by
	// the replicate queue. Either way, no point in also sending it a snapshot of
	// type RAFT.
	if repDesc.GetType() == roachpb.LEARNER {
		if fn := repl.store.cfg.TestingKnobs.ReplicaSkipLearnerSnapshot; fn != nil && fn() {
			return nil
		}
		snapType = SnapshotRequest_LEARNER
		if index := repl.getAndGCSnapshotLogTruncationConstraints(timeutil.Now(), repDesc.StoreID); index > 0 {
			// There is a snapshot being transferred. It's probably a LEARNER snap, so
			// bail for now and try again later.
			err := errors.Errorf(
				"skipping snapshot; replica is likely a learner in the process of being added: %s", repDesc)
			log.VEventf(ctx, 2, "%v", err)
			// TODO(dan): This is super brittle and non-obvious. In the common case,
			// this check avoids duplicate work, but in rare cases, we send the
			// learner snap at an index before the one raft wanted here. The raft
			// leader should be able to use logs to get the rest of the way, but it
			// doesn't try. In this case, skipping the raft snapshot would mean that
			// we have to wait for the next scanner cycle of the raft snapshot queue
			// to pick it up again. So, punt the responsibility back to raft by
			// telling it that the snapshot failed. If the learner snap ends up being
			// sufficient, this message will be ignored, but if we hit the case
			// described above, this will cause raft to keep asking for a snap and at
			// some point the snapshot lock above will be released and we'll fall
			// through to the below.
			repl.reportSnapshotStatus(ctx, repDesc.ReplicaID, err)
			return nil
		}
	}

	var err error
	if desc.GetRangeType() == roachpb.TS_RANGE {
		// The data volume of the current copy is much different from that of the leased
		// copy, and it is necessary to quickly supplement the data by sending snapshots.
		// In this case, needTSSnapshotData should be true.
		exist := false
		if repl.store.TsEngine != nil && desc.TableId != 0 {
			exist, _ = repl.store.TsEngine.TSIsTsTableExist(uint64(desc.TableId))
		}
		err = repl.sendTSSnapshot(ctx, repDesc, snapType, SnapshotRequest_RECOVERY, exist)
	} else if desc.GetRangeType() == roachpb.DEFAULT_RANGE {
		err = repl.sendSnapshot(ctx, repDesc, snapType, SnapshotRequest_RECOVERY)
	}

	// NB: if the snapshot fails because of an overlapping replica on the
	// recipient which is also waiting for a snapshot, the "smart" thing is to
	// send that other snapshot with higher priority. The problem is that the
	// leader for the overlapping range may not be this node. This happens
	// typically during splits and merges when overly aggressive log truncations
	// occur.
	//
	// For splits, the overlapping range will be a replica of the pre-split
	// range that needs a snapshot to catch it up across the split trigger.
	//
	// For merges, the overlapping replicas belong to ranges since subsumed by
	// this range. In particular, there can be many of them if merges apply in
	// rapid succession. The leftmost replica is the most important one to catch
	// up, as it will absorb all of the overlapping replicas when caught up past
	// all of the merges.
	//
	// We're currently not handling this and instead rely on the quota pool to
	// make sure that log truncations won't require snapshots for healthy
	// followers.
	return err
}

func (*raftSnapshotQueue) timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *raftSnapshotQueue) purgatoryChan() <-chan time.Time {
	return nil
}
