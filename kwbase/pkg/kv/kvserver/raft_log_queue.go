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
	"fmt"
	"sort"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

const (
	// raftLogQueueTimerDuration is the duration between truncations.
	raftLogQueueTimerDuration = 0 // zero duration to process truncations greedily
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	RaftLogQueueStaleThreshold = 100
	// RaftLogQueueStaleSize is the minimum size of the Raft log that we'll
	// truncate even if there are fewer than RaftLogQueueStaleThreshold entries
	// to truncate. The value of 64 KB was chosen experimentally by looking at
	// when Raft log truncation usually occurs when using the number of entries
	// as the sole criteria.
	RaftLogQueueStaleSize = 64 << 10
	// RaftLogQueueTsStaleSize is the minimum size with 512MiB. Ts Range is not split by size.
	RaftLogQueueTsStaleSize = 1 << 29
	// Allow a limited number of Raft log truncations to be processed
	// concurrently.
	raftLogQueueConcurrency = 10
	// While a snapshot is in flight, we won't truncate past the snapshot's log
	// index. This behavior is extended to a grace period after the snapshot is
	// marked as completed as it is applied at the receiver only a little later,
	// leaving a window for a truncation that requires another snapshot.
	raftLogQueuePendingSnapshotGracePeriod = 3 * time.Second
)

// raftLogQueue manages a queue of replicas slated to have their raft logs
// truncated by removing unneeded entries.
type raftLogQueue struct {
	*baseQueue
	db *kv.DB

	logSnapshots util.EveryN
}

// newRaftLogQueue returns a new instance of raftLogQueue. Replicas are passed
// to the queue both proactively (triggered by write load) and periodically
// (via the scanner). When processing a replica, the queue decides whether the
// Raft log can be truncated, which is a tradeoff between wanting to keep the
// log short overall and allowing slower followers to catch up before they get
// cut off by a truncation and need a snapshot. See newTruncateDecision for
// details on this decision making process.
func newRaftLogQueue(store *Store, db *kv.DB, gossip *gossip.Gossip) *raftLogQueue {
	rlq := &raftLogQueue{
		db:           db,
		logSnapshots: util.Every(10 * time.Second),
	}
	rlq.baseQueue = newBaseQueue(
		"raftlog", rlq, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			maxConcurrency:       raftLogQueueConcurrency,
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.RaftLogQueueSuccesses,
			failures:             store.metrics.RaftLogQueueFailures,
			pending:              store.metrics.RaftLogQueuePending,
			processingNanos:      store.metrics.RaftLogQueueProcessingNanos,
		},
	)
	return rlq
}

// newTruncateDecision returns a truncateDecision for the given Replica if no
// error occurs. If input data to establish a truncateDecision is missing, a
// zero decision is returned.
//
// At a high level, a truncate decision operates based on the Raft log size, the
// number of entries in the log, and the Raft status of the followers. In an
// ideal world and most of the time, followers are reasonably up to date, and a
// decision to truncate to the index acked on all replicas will be made whenever
// there is at least a little bit of log to truncate (think a hundred records or
// ~100kb of data). If followers fall behind, are offline, or are waiting for a
// snapshot, a second strategy is needed to make sure that the Raft log is
// eventually truncated: when the raft log size exceeds a limit (4mb at time of
// writing), truncations become willing and able to cut off followers as long as
// a quorum has acked the truncation index. The quota pool ensures that the delta
// between "acked by quorum" and "acked by all" is bounded, while Raft limits the
// size of the uncommitted, i.e. not "acked by quorum", part of the log; thus
// the "quorum" truncation strategy bounds the absolute size of the log on all
// followers.
//
// Exceptions are made for replicas for which information is missing ("probing
// state") as long as they are known to have been online recently, and for
// in-flight snapshots (in particular preemptive snapshots) which are not
// adequately reflected in the Raft status and would otherwise be cut off with
// regularity. Probing live followers should only remain in this state for a
// short moment and so we deny a log truncation outright (as there's no safe
// index to truncate to); for snapshots, we can still truncate, but not past
// the snapshot's index.
//
// A challenge for log truncation is to deal with sideloaded log entries, that
// is, entries which contain SSTables for direct ingestion into the storage
// engine. Such log entries are very large, and failing to account for them in
// the heuristics can trigger overly aggressive truncations.
//
// The raft log size used in the decision making process is principally updated
// in the main Raft command apply loop, and adds a Replica to this queue
// whenever the log size has increased by a non-negligible amount that would be
// worth truncating (~100kb).
//
// Unfortunately, the size tracking is not very robust as it suffers from two
// limitations at the time of writing:
//  1. it may undercount as it is in-memory and incremented only as proposals
//     are handled; that is, a freshly started node will believe its Raft log to be
//     zero-sized independent of its actual size, and
//  2. the addition and corresponding subtraction happen in very different places
//     and are difficult to keep bug-free, meaning that there is low confidence that
//     we maintain the delta in a completely accurate manner over time. One example
//     of potential errors are sideloaded proposals, for which the subtraction needs
//     to load the size of the file on-disk (i.e. supplied by the fs), whereas
//     the addition uses the in-memory representation of the file.
//
// Ideally, a Raft log that grows large for whichever reason (for instance the
// queue being stuck on another replica) wouldn't be more than a nuisance on
// nodes with sufficient disk space. Unfortunately, at the time of writing, the
// Raft log is included in Raft snapshots. On the other hand, IMPORT/RESTORE's
// split/scatter phase interacts poorly with overly aggressive truncations and
// can DDOS the Raft snapshot queue.
func newTruncateDecision(ctx context.Context, r *Replica) (truncateDecision, error) {
	rangeID := r.RangeID
	now := timeutil.Now()

	// NB: we need an exclusive lock due to grabbing the first index.
	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
	// A "cooperative" truncation (i.e. one that does not cut off followers from
	// the log) takes place whenever there are more than
	// RaftLogQueueStaleThreshold entries or the log's estimated size is above
	// RaftLogQueueStaleSize bytes. This is fairly aggressive, so under normal
	// conditions, the log is very small.
	//
	// If followers start falling behind, at some point the logs still need to
	// be truncated. We do this either when the size of the log exceeds
	// RaftLogTruncationThreshold (or, in eccentric configurations, the zone's
	// RangeMaxBytes). This captures the heuristic that at some point, it's more
	// efficient to catch up via a snapshot than via applying a long tail of log
	// entries.
	targetSize := r.store.cfg.RaftLogTruncationThreshold
	if targetSize > *r.mu.zone.RangeMaxBytes {
		targetSize = *r.mu.zone.RangeMaxBytes
	}
	if r.isTsLocked() {
		targetSize = 1 << 30 // 1024 MB
	}
	raftStatus := r.raftStatusRLocked()

	firstIndex, err := r.raftFirstIndexLocked()
	const anyRecipientStore roachpb.StoreID = 0
	pendingSnapshotIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now, anyRecipientStore)
	lastIndex := r.mu.lastIndex
	if r.mu.tsFlushedIndex > 0 {
		lastIndex = r.mu.tsFlushedIndex
	}

	logSizeTrusted := r.mu.raftLogSizeTrusted
	r.mu.Unlock()

	if err != nil {
		return truncateDecision{}, errors.Errorf("error retrieving first index for r%d: %s", rangeID, err)
	}

	if raftStatus == nil {
		if log.V(6) {
			log.Infof(ctx, "the raft group doesn't exist for r%d", rangeID)
		}
		return truncateDecision{}, nil
	}

	// Is this the raft leader? We only perform log truncation on the raft leader
	// which has the up to date info on followers.
	if raftStatus.RaftState != raft.StateLeader {
		return truncateDecision{}, nil
	}

	// For all our followers, overwrite the RecentActive field (which is always
	// true since we don't use CheckQuorum) with our own activity check.
	r.mu.RLock()
	log.Eventf(ctx, "raft status before lastUpdateTimes check: %+v", raftStatus.Progress)
	log.Eventf(ctx, "lastUpdateTimes: %+v", r.mu.lastUpdateTimes)
	updateRaftProgressFromActivity(
		ctx, raftStatus.Progress, r.descRLocked().Replicas().All(), r.mu.lastUpdateTimes, now,
	)
	log.Eventf(ctx, "raft status after lastUpdateTimes check: %+v", raftStatus.Progress)
	r.mu.RUnlock()

	if pr, ok := raftStatus.Progress[raftStatus.Lead]; ok {
		// TODO(tschottdorf): remove this line once we have picked up
		// https://github.com/etcd-io/etcd/pull/10279
		pr.State = tracker.StateReplicate
		raftStatus.Progress[raftStatus.Lead] = pr
	}

	input := truncateDecisionInput{
		RaftStatus:           *raftStatus,
		LogSize:              raftLogSize,
		MaxLogSize:           targetSize,
		LogSizeTrusted:       logSizeTrusted,
		FirstIndex:           firstIndex,
		LastIndex:            lastIndex,
		PendingSnapshotIndex: pendingSnapshotIndex,
	}

	decision := computeTruncateDecision(input)
	if r.isTs() {
		decision.IsTs = true
	}
	return decision, nil
}

func updateRaftProgressFromActivity(
	ctx context.Context,
	prs map[uint64]tracker.Progress,
	replicas []roachpb.ReplicaDescriptor,
	lastUpdate lastUpdateTimesMap,
	now time.Time,
) {
	for _, replDesc := range replicas {
		replicaID := replDesc.ReplicaID
		pr, ok := prs[uint64(replicaID)]
		if !ok {
			continue
		}
		pr.RecentActive = lastUpdate.isFollowerActive(ctx, replicaID, now)
		// Override this field for safety since we don't use it. Instead, we use
		// pendingSnapshotIndex from above which is also populated for preemptive
		// snapshots.
		//
		// NOTE: We don't rely on PendingSnapshot because PendingSnapshot is
		// initialized by the leader when it realizes the follower needs a snapshot,
		// and it isn't initialized with the index of the snapshot that is actually
		// sent by us (out of band), which likely is lower.
		pr.PendingSnapshot = 0
		prs[uint64(replicaID)] = pr
	}
}

const (
	truncatableIndexChosenViaCommitIndex     = "commit"
	truncatableIndexChosenViaFollowers       = "followers"
	truncatableIndexChosenViaProbingFollower = "probing follower"
	truncatableIndexChosenViaPendingSnap     = "pending snapshot"
	truncatableIndexChosenViaFirstIndex      = "first index"
	truncatableIndexChosenViaLastIndex       = "last index"
)

type truncateDecisionInput struct {
	RaftStatus            raft.Status
	LogSize, MaxLogSize   int64
	LogSizeTrusted        bool // false when LogSize might be off
	FirstIndex, LastIndex uint64
	PendingSnapshotIndex  uint64
}

func (input truncateDecisionInput) LogTooLarge() bool {
	return input.LogSize > input.MaxLogSize
}

type truncateDecision struct {
	Input       truncateDecisionInput
	CommitIndex uint64

	NewFirstIndex uint64 // first index of the resulting log after truncation
	ChosenVia     string
	IsTs          bool // ts range flag
}

func (td *truncateDecision) raftSnapshotsForIndex(index uint64) int {
	var n int
	for _, p := range td.Input.RaftStatus.Progress {
		if p.State != tracker.StateReplicate {
			// If the follower isn't replicating, we can't trust its Match in
			// the first place. But note that this shouldn't matter in practice
			// as we already take care to not cut off these followers when
			// computing the truncate decision. See:
			_ = truncatableIndexChosenViaProbingFollower // guru ref
			continue
		}

		// When a log truncation happens at the "current log index" (i.e. the
		// most recently committed index), it is often still in flight to the
		// followers not required for quorum, and it is likely that they won't
		// need a truncation to catch up. A follower in that state will have a
		// Match equaling committed-1, but a Next of committed+1 (indicating that
		// an append at 'committed' is already ongoing).
		if p.Match < index && p.Next <= index {
			n++
		}
	}
	if td.Input.PendingSnapshotIndex != 0 && td.Input.PendingSnapshotIndex < index {
		n++
	}

	return n
}

func (td *truncateDecision) NumNewRaftSnapshots() int {
	return td.raftSnapshotsForIndex(td.NewFirstIndex) - td.raftSnapshotsForIndex(td.Input.FirstIndex)
}

func (td *truncateDecision) String() string {
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "should truncate: %t [", td.ShouldTruncate())
	_, _ = fmt.Fprintf(
		&buf,
		"truncate %d entries to first index %d (chosen via: %s)",
		td.NumTruncatableIndexes(), td.NewFirstIndex, td.ChosenVia,
	)
	if td.Input.LogTooLarge() {
		_, _ = fmt.Fprintf(
			&buf,
			"; log too large (%s > %s)",
			humanizeutil.IBytes(td.Input.LogSize),
			humanizeutil.IBytes(td.Input.MaxLogSize),
		)
	}
	if n := td.NumNewRaftSnapshots(); n > 0 {
		_, _ = fmt.Fprintf(&buf, "; implies %d Raft snapshot%s", n, util.Pluralize(int64(n)))
	}
	if !td.Input.LogSizeTrusted {
		_, _ = fmt.Fprintf(&buf, "; log size untrusted")
	}
	buf.WriteRune(']')

	return buf.String()
}

func (td *truncateDecision) NumTruncatableIndexes() int {
	if td.NewFirstIndex < td.Input.FirstIndex {
		return 0
	}
	return int(td.NewFirstIndex - td.Input.FirstIndex)
}

func (td *truncateDecision) ShouldTruncate() bool {
	n := td.NumTruncatableIndexes()
	if td.IsTs {
		return n >= RaftLogQueueStaleThreshold ||
			(n > 0 && td.Input.LogSize >= RaftLogQueueTsStaleSize)
	}
	return n >= RaftLogQueueStaleThreshold ||
		(n > 0 && td.Input.LogSize >= RaftLogQueueStaleSize)
}

// ProtectIndex attempts to "protect" a position in the log by making sure it's
// not truncated away. Specifically it lowers the proposed truncation point
// (which will be the new first index after the truncation) to the given index
// if it would be truncating at a point past it. If a change is made, the
// ChosenVia is updated with the one given. This protection is not guaranteed if
// the protected index is outside of the existing [FirstIndex,LastIndex] bounds.
func (td *truncateDecision) ProtectIndex(index uint64, chosenVia string) {
	if td.NewFirstIndex > index {
		td.NewFirstIndex = index
		td.ChosenVia = chosenVia
	}
}

// computeTruncateDecision returns the oldest index that cannot be
// truncated. If there is a behind node, we want to keep old raft logs so it
// can catch up without having to send a full snapshot. However, if a node down
// is down long enough, sending a snapshot is more efficient and we should
// truncate the log to the next behind node or the quorum committed index. We
// currently truncate when the raft log size is bigger than the range
// size.
//
// Note that when a node is behind we continue to let the raft log build up
// instead of truncating to the commit index. Consider what would happen if we
// truncated to the commit index whenever a node is behind and thus needs to be
// caught up via a snapshot. While we're generating the snapshot, sending it to
// the behind node and waiting for it to be applied we would continue to
// truncate the log. If the snapshot generation and application takes too long
// the behind node will be caught up to a point behind the current first index
// and thus require another snapshot, likely entering a never ending loop of
// snapshots. See #8629.
func computeTruncateDecision(input truncateDecisionInput) truncateDecision {
	decision := truncateDecision{Input: input}
	decision.CommitIndex = input.RaftStatus.Commit

	// The last index is most aggressive possible truncation that we could do.
	// Everything else in this method makes the truncation less aggressive.
	decision.NewFirstIndex = input.LastIndex
	decision.ChosenVia = truncatableIndexChosenViaLastIndex

	// Start by trying to truncate at the commit index. Naively, you would expect
	// LastIndex to never be smaller than the commit index, but
	// RaftStatus.Progress.Match is updated on the leader when a command is
	// proposed and in a single replica Raft group this also means that
	// RaftStatus.Commit is updated at propose time.
	decision.ProtectIndex(decision.CommitIndex, truncatableIndexChosenViaCommitIndex)

	for _, progress := range input.RaftStatus.Progress {
		// Snapshots are expensive, so we try our best to avoid truncating past
		// where a follower is.

		// First, we never truncate off a recently active follower, no matter how
		// large the log gets. Recently active shares the (currently 10s) constant
		// as the quota pool, so the quota pool should put a bound on how much the
		// raft log can grow due to this.
		//
		// For live followers which are being probed (i.e. the leader doesn't know
		// how far they've caught up), the Match index is too large, and so the
		// quorum index can be, too. We don't want these followers to require a
		// snapshot since they are most likely going to be caught up very soon (they
		// respond with the "right index" to the first probe or don't respond, in
		// which case they should end up as not recently active). But we also don't
		// know their index, so we can't possible make a truncation decision that
		// avoids that at this point and make the truncation a no-op.
		//
		// The scenario in which this is most relevant is during restores, where we
		// split off new ranges that rapidly receive very large log entries while
		// the Raft group is still in a state of discovery (a new leader starts
		// probing followers at its own last index). Additionally, these ranges will
		// be split many times over, resulting in a flurry of snapshots with
		// overlapping bounds that put significant stress on the Raft snapshot
		// queue.
		if progress.RecentActive {
			if progress.State == tracker.StateProbe {
				decision.ProtectIndex(input.FirstIndex, truncatableIndexChosenViaProbingFollower)
			} else {
				decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
			}
			continue
		}

		// Second, if the follower has not been recently active, we don't
		// truncate it off as long as the raft log is not too large.
		if !input.LogTooLarge() {
			decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
		}

		// Otherwise, we let it truncate to the committed index.
	}

	// The pending snapshot index acts as a placeholder for a replica that is
	// about to be added to the range (or is in Raft recovery). We don't want to
	// truncate the log in a way that will require that new replica to be caught
	// up via yet another Raft snapshot.
	if input.PendingSnapshotIndex > 0 {
		decision.ProtectIndex(input.PendingSnapshotIndex, truncatableIndexChosenViaPendingSnap)
	}

	// If new first index dropped below first index, make them equal (resulting
	// in a no-op).
	if decision.NewFirstIndex < input.FirstIndex {
		decision.NewFirstIndex = input.FirstIndex
		decision.ChosenVia = truncatableIndexChosenViaFirstIndex
	}

	// TODO(irfansharif): See the discussion in #47143. Here, we previously
	// asserted on the invariants we expected the truncation decision to uphold.
	// This was removed from the v20.1.0 release due to the fragility of the
	// assert at the time. They're going to bake on master in the interim, and
	// should be pulled back into v20.1.1 when ruggedized.

	return decision
}

// shouldQueue determines whether a range should be queued for truncating. This
// is true only if the replica is the raft leader and if the total number of
// the range's raft log's stale entries exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, r *Replica, _ *config.SystemConfig,
) (shouldQ bool, priority float64) {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		log.Warning(ctx, err)
		return false, 0
	}

	shouldQ, _, prio := rlq.shouldQueueImpl(ctx, decision)
	return shouldQ, prio
}

// shouldQueueImpl returns whether the given truncate decision should lead to
// a log truncation. This is either the case if the decision says so or if
// we want to recompute the log size (in which case `recomputeRaftLogSize` and
// `shouldQ` are both true and a reasonable priority is returned).
func (rlq *raftLogQueue) shouldQueueImpl(
	ctx context.Context, decision truncateDecision,
) (shouldQ bool, recomputeRaftLogSize bool, priority float64) {
	if decision.ShouldTruncate() {
		return true, !decision.Input.LogSizeTrusted, float64(decision.Input.LogSize)
	}
	if decision.Input.LogSizeTrusted ||
		decision.Input.LastIndex == decision.Input.FirstIndex {

		return false, false, 0
	}
	// We have a nonempty log (first index != last index) and can't vouch that
	// the bytes in the log are known. Queue the replica; processing it will
	// force a recomputation. For the priority, we have to pick one as we
	// usually use the log size which is not available here. Going half-way
	// between zero and the MaxLogSize should give a good tradeoff between
	// processing the recomputation quickly, and not starving replicas which see
	// a significant amount of write traffic until they run over and truncate
	// more aggressively than they need to.
	return true, true, 1.0 + float64(decision.Input.MaxLogSize)/2.0
}

// process truncates the raft log of the range if the replica is the raft
// leader and if the total number of the range's raft log's stale entries
// exceeds RaftLogQueueStaleThreshold.
func (rlq *raftLogQueue) process(ctx context.Context, r *Replica, _ *config.SystemConfig) error {
	decision, err := newTruncateDecision(ctx, r)
	if err != nil {
		return err
	}

	if _, recompute, _ := rlq.shouldQueueImpl(ctx, decision); recompute {
		log.VEventf(ctx, 2, "recomputing raft log based on decision %+v", decision)

		// We need to hold raftMu both to access the sideloaded storage and to
		// make sure concurrent Raft activity doesn't foul up our update to the
		// cached in-memory values.
		if r.isTs() {
			// TODO by fyx: for ts range log maybe large, compute raft log size is too slow,
			// so temp not raftMu lock because ts log size is not very important
			n, err := ComputeRaftLogSize(ctx, r.RangeID, r.Engine(), r.raftMu.sideloaded)
			r.raftMu.Lock()
			if err == nil {
				r.mu.Lock()
				r.mu.raftLogSize = n
				r.mu.raftLogLastCheckSize = n
				r.mu.raftLogSizeTrusted = true
				r.mu.Unlock()
			}
			r.raftMu.Unlock()
			if err != nil {
				return errors.Wrap(err, "ts recomputing raft log size")
			}
			log.VEventf(ctx, 2, "ts recomputed raft log size to %s", humanizeutil.IBytes(n))
		} else {
			r.raftMu.Lock()
			n, err := ComputeRaftLogSize(ctx, r.RangeID, r.Engine(), r.raftMu.sideloaded)
			if err == nil {
				r.mu.Lock()
				r.mu.raftLogSize = n
				r.mu.raftLogLastCheckSize = n
				r.mu.raftLogSizeTrusted = true
				r.mu.Unlock()
			}
			r.raftMu.Unlock()
			if err != nil {
				return errors.Wrap(err, "recomputing raft log size")
			}
			log.VEventf(ctx, 2, "recomputed raft log size to %s", humanizeutil.IBytes(n))
		}

		// Override the decision, now that an accurate log size is available.
		decision, err = newTruncateDecision(ctx, r)
		if err != nil {
			return err
		}
	}

	// Can and should the raft logs be truncated?
	if decision.ShouldTruncate() {
		if n := decision.NumNewRaftSnapshots(); log.V(1) || n > 0 && rlq.logSnapshots.ShouldProcess(timeutil.Now()) {
			log.Info(ctx, decision.String())
		} else {
			log.VEvent(ctx, 1, decision.String())
		}
		b := &kv.Batch{}
		b.AddRawRequest(&roachpb.TruncateLogRequest{
			RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
			Index:         decision.NewFirstIndex,
			RangeID:       r.RangeID,
		})
		if err := rlq.db.Run(ctx, b); err != nil {
			return err
		}
		r.store.metrics.RaftLogTruncated.Inc(int64(decision.NumTruncatableIndexes()))
	} else {
		log.VEventf(ctx, 3, decision.String())
	}
	return nil
}

// timer returns interval between processing successive queued truncations.
func (*raftLogQueue) timer(_ time.Duration) time.Duration {
	return raftLogQueueTimerDuration
}

// purgatoryChan returns nil.
func (*raftLogQueue) purgatoryChan() <-chan time.Time {
	return nil
}

var _ sort.Interface = uint64Slice(nil)

// uint64Slice implements sort.Interface
type uint64Slice []uint64

// Len implements sort.Interface
func (a uint64Slice) Len() int { return len(a) }

// Swap implements sort.Interface
func (a uint64Slice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
