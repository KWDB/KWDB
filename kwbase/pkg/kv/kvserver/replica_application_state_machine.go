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
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/apply"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// applyCommittedEntriesStats returns stats about what happened during the
// application of a set of raft entries.
//
// TODO(ajwerner): add metrics to go with these stats.
type applyCommittedEntriesStats struct {
	batchesProcessed int
	entriesProcessed int
	stateAssertions  int
	numEmptyEntries  int
}

// nonDeterministicFailure is an error type that indicates that a state machine
// transition failed due to an unexpected error. Failure to perform a state
// transition is a form of non-determinism, so it can't be permitted for any
// reason during the application phase of state machine replication. The only
// acceptable recourse is to signal that the replica has become corrupted.
//
// All errors returned by replicaDecoder and replicaStateMachine will be instances
// of this type.
type nonDeterministicFailure struct {
	wrapped  error
	safeExpl string
}

// The provided format string should be safe for reporting.
func makeNonDeterministicFailure(format string, args ...interface{}) error {
	str := fmt.Sprintf(format, args...)
	return &nonDeterministicFailure{
		wrapped:  errors.New(str),
		safeExpl: str,
	}
}

// The provided msg should be safe for reporting.
func wrapWithNonDeterministicFailure(err error, msg string) error {
	return &nonDeterministicFailure{
		wrapped:  errors.Wrap(err, msg),
		safeExpl: msg,
	}
}

// Error implements the error interface.
func (e *nonDeterministicFailure) Error() string {
	return fmt.Sprintf("non-deterministic failure: %s", e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *nonDeterministicFailure) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *nonDeterministicFailure) Unwrap() error { return e.wrapped }

// replicaStateMachine implements the apply.StateMachine interface.
//
// The structure coordinates state transitions within the Replica state machine
// due to the application of replicated commands decoded from committed raft
// entries. Commands are applied to the state machine in a multi-stage process
// whereby individual commands are prepared for application relative to the
// current view of ReplicaState and staged in a replicaAppBatch, the batch is
// committed to the Replica's storage engine atomically, and finally the
// side-effects of each command is applied to the Replica's in-memory state.
type replicaStateMachine struct {
	r *Replica
	// batch is returned from NewBatch(false /* ephemeral */).
	batch replicaAppBatch
	// ephemeralBatch is returned from NewBatch(true /* ephemeral */).
	ephemeralBatch ephemeralReplicaAppBatch
	// stats are updated during command application and reset by moveStats.
	stats applyCommittedEntriesStats
}

// getStateMachine returns the Replica's apply.StateMachine. The Replica's
// raftMu is held for the entire lifetime of the replicaStateMachine.
func (r *Replica) getStateMachine() *replicaStateMachine {
	sm := &r.raftMu.stateMachine
	sm.r = r
	return sm
}

// shouldApplyCommand determines whether or not a command should be applied to
// the replicated state machine after it has been committed to the Raft log. It
// then sets the provided command's leaseIndex, proposalRetry, and forcedErr
// fields and returns whether command should be applied or rejected.
func (r *Replica) shouldApplyCommand(
	ctx context.Context, cmd *replicatedCmd, replicaState *storagepb.ReplicaState,
) bool {
	cmd.leaseIndex, cmd.proposalRetry, cmd.forcedErr = checkForcedErr(
		ctx, cmd.idKey, &cmd.raftCmd, cmd.IsLocal(), replicaState,
	)
	if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; cmd.forcedErr == nil && filter != nil {
		var newPropRetry int
		newPropRetry, cmd.forcedErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                cmd.idKey,
			ReplicatedEvalResult: *cmd.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if cmd.proposalRetry == 0 {
			cmd.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
	}
	return cmd.forcedErr == nil
}

// checkForcedErr determines whether or not a command should be applied to the
// replicated state machine after it has been committed to the Raft log. This
// decision is deterministic on all replicas, such that a command that is
// rejected "beneath raft" on one replica will be rejected "beneath raft" on
// all replicas.
//
// The decision about whether or not to apply a command is a combination of
// three checks:
//  1. verify that the command was proposed under the current lease. This is
//     determined using the proposal's ProposerLeaseSequence.
//  2. verify that the command hasn't been re-ordered with other commands that
//     were proposed after it and which already applied. This is determined
//     using the proposal's MaxLeaseIndex.
//  3. verify that the command isn't in violation of the Range's current
//     garbage collection threshold. This is determined using the proposal's
//     Timestamp.
//
// TODO(nvanbenschoten): Unit test this function now that it is stateless.
func checkForcedErr(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	raftCmd *storagepb.RaftCommand,
	isLocal bool,
	replicaState *storagepb.ReplicaState,
) (uint64, proposalReevaluationReason, *roachpb.Error) {
	leaseIndex := replicaState.LeaseAppliedIndex
	isLeaseRequest := raftCmd.ReplicatedEvalResult.IsLeaseRequest
	var requestedLease roachpb.Lease
	if isLeaseRequest {
		requestedLease = *raftCmd.ReplicatedEvalResult.State.Lease
	}
	if idKey == "" {
		// This is an empty Raft command (which is sent by Raft after elections
		// to trigger reproposals or during concurrent configuration changes).
		// Nothing to do here except making sure that the corresponding batch
		// (which is bogus) doesn't get executed (for it is empty and so
		// properties like key range are undefined).
		return leaseIndex, proposalNoReevaluation, roachpb.NewErrorf("no-op on empty Raft entry")
	}

	// Verify the lease matches the proposer's expectation. We rely on
	// the proposer's determination of whether the existing lease is
	// held, and can be used, or is expired, and can be replaced.
	// Verify checks that the lease has not been modified since proposal
	// due to Raft delays / reorderings.
	// To understand why this lease verification is necessary, see comments on the
	// proposer_lease field in the proto.
	leaseMismatch := false
	if raftCmd.DeprecatedProposerLease != nil {
		// VersionLeaseSequence must not have been active when this was proposed.
		//
		// This does not prevent the lease race condition described below. The
		// reason we don't fix this here as well is because fixing the race
		// requires a new cluster version which implies that we'll already be
		// using lease sequence numbers and will fall into the case below.
		leaseMismatch = !raftCmd.DeprecatedProposerLease.Equivalent(*replicaState.Lease)
	} else {
		leaseMismatch = raftCmd.ProposerLeaseSequence != replicaState.Lease.Sequence
		if !leaseMismatch && isLeaseRequest {
			// Lease sequence numbers are a reflection of lease equivalency
			// between subsequent leases. However, Lease.Equivalent is not fully
			// symmetric, meaning that two leases may be Equivalent to a third
			// lease but not Equivalent to each other. If these leases are
			// proposed under that same third lease, neither will be able to
			// detect whether the other has applied just by looking at the
			// current lease sequence number because neither will will increment
			// the sequence number.
			//
			// This can lead to inversions in lease expiration timestamps if
			// we're not careful. To avoid this, if a lease request's proposer
			// lease sequence matches the current lease sequence and the current
			// lease sequence also matches the requested lease sequence, we make
			// sure the requested lease is Equivalent to current lease.
			if replicaState.Lease.Sequence == requestedLease.Sequence {
				// It is only possible for this to fail when expiration-based
				// lease extensions are proposed concurrently.
				leaseMismatch = !replicaState.Lease.Equivalent(requestedLease)
			}

			// This is a check to see if the lease we proposed this lease request against is the same
			// lease that we're trying to update. We need to check proposal timestamps because
			// extensions don't increment sequence numbers. Without this check a lease could
			// be extended and then another lease proposed against the original lease would
			// be applied over the extension.
			if raftCmd.ReplicatedEvalResult.PrevLeaseProposal != nil &&
				(*raftCmd.ReplicatedEvalResult.PrevLeaseProposal != *replicaState.Lease.ProposedTS) {
				leaseMismatch = true
			}
		}
	}
	if leaseMismatch {
		log.VEventf(
			ctx, 1,
			"command with lease #%d incompatible to %v",
			raftCmd.ProposerLeaseSequence, *replicaState.Lease,
		)
		if isLeaseRequest {
			// For lease requests we return a special error that
			// redirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "proposed under invalid lease",
			})
		}
		// We return a NotLeaseHolderError so that the DistSender retries.
		// NB: we set proposerStoreID to 0 because we don't know who proposed the
		// Raft command. This is ok, as this is only used for debug information.
		nlhe := newNotLeaseHolderError(replicaState.Lease, 0 /* proposerStoreID */, replicaState.Desc)
		nlhe.CustomMsg = fmt.Sprintf(
			"stale proposal: command was proposed under lease #%d but is being applied "+
				"under lease: %s", raftCmd.ProposerLeaseSequence, replicaState.Lease)
		return leaseIndex, proposalNoReevaluation, roachpb.NewError(nlhe)
	}

	if isLeaseRequest {
		// Lease commands are ignored by the counter (and their MaxLeaseIndex is ignored). This
		// makes sense since lease commands are proposed by anyone, so we can't expect a coherent
		// MaxLeaseIndex. Also, lease proposals are often replayed, so not making them update the
		// counter makes sense from a testing perspective.
		//
		// However, leases get special vetting to make sure we don't give one to a replica that was
		// since removed (see #15385 and a comment in redirectOnOrAcquireLease).
		if _, ok := replicaState.Desc.GetReplicaDescriptor(requestedLease.Replica.StoreID); !ok {
			return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *replicaState.Lease,
				Requested: requestedLease,
				Message:   "replica not part of range",
			})
		}
	} else if replicaState.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
		// The happy case: the command is applying at or ahead of the minimal
		// permissible index. It's ok if it skips a few slots (as can happen
		// during rearrangement); this command will apply, but later ones which
		// were proposed at lower indexes may not. Overall though, this is more
		// stable and simpler than requiring commands to apply at their exact
		// lease index: Handling the case in which MaxLeaseIndex > oldIndex+1
		// is otherwise tricky since we can't tell the client to try again
		// (reproposals could exist and may apply at the right index, leading
		// to a replay), and assigning the required index would be tedious
		// seeing that it would have to rewind sometimes.
		leaseIndex = raftCmd.MaxLeaseIndex
	} else {
		// The command is trying to apply at a past log position. That's
		// unfortunate and hopefully rare; the client on the proposer will try
		// again. Note that in this situation, the leaseIndex does not advance.
		retry := proposalNoReevaluation
		if isLocal {
			log.Infof(
				ctx,
				"retry proposal %x: applied at lease index %d, required < %d",
				idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			retry = proposalIllegalLeaseIndex
		}
		return leaseIndex, retry, roachpb.NewErrorf(
			"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
		)
	}

	// Verify that the batch timestamp is after the GC threshold. This is
	// necessary because not all commands declare read access on the GC
	// threshold key, even though they implicitly depend on it. This means
	// that access to this state will not be serialized by latching,
	// so we must perform this check upstream and downstream of raft.
	// See #14833.
	ts := raftCmd.ReplicatedEvalResult.Timestamp
	if ts.LessEq(*replicaState.GCThreshold) {
		return leaseIndex, proposalNoReevaluation, roachpb.NewError(&roachpb.BatchTimestampBeforeGCError{
			Timestamp: ts,
			Threshold: *replicaState.GCThreshold,
		})
	}
	return leaseIndex, proposalNoReevaluation, nil
}

// NewBatch implements the apply.StateMachine interface.
func (sm *replicaStateMachine) NewBatch(ephemeral bool) apply.Batch {
	r := sm.r
	if ephemeral {
		mb := &sm.ephemeralBatch
		mb.r = r
		r.mu.RLock()
		mb.state = r.mu.state
		r.mu.RUnlock()
		return mb
	}
	b := &sm.batch
	b.r = r
	b.sm = sm
	b.batch = r.store.engine.NewBatch()
	r.mu.RLock()
	b.state = r.mu.state
	b.state.Stats = &b.stats
	*b.state.Stats = *r.mu.state.Stats
	r.mu.RUnlock()
	b.start = timeutil.Now()
	return b
}

// replicaAppBatch implements the apply.Batch interface.
//
// The structure accumulates state due to the application of raft commands.
// Committed raft commands are applied to the state machine in a multi-stage
// process whereby individual commands are prepared for application relative
// to the current view of ReplicaState and staged in the batch. The batch is
// committed to the state machine's storage engine atomically.
type replicaAppBatch struct {
	r  *Replica
	sm *replicaStateMachine

	// batch accumulates writes implied by the raft entries in this batch.
	batch storage.Batch
	// state is this batch's view of the replica's state. It is copied from
	// under the Replica.mu when the batch is initialized and is updated in
	// stageTrivialReplicatedEvalResult.
	state storagepb.ReplicaState
	// stats is stored on the application batch to avoid an allocation in
	// tracking the batch's view of replicaState. All pointer fields in
	// replicaState other than Stats are overwritten completely rather than
	// updated in-place.
	stats enginepb.MVCCStats
	// maxTS is the maximum timestamp that any command that was staged in this
	// batch was evaluated at.
	maxTS hlc.Timestamp
	// migrateToAppliedStateKey tracks whether any command in the batch
	// triggered a migration to the replica applied state key. If so, this
	// migration will be performed when the application batch is committed.
	migrateToAppliedStateKey bool
	// changeRemovesReplica tracks whether the command in the batch (there must
	// be only one) removes this replica from the range.
	changeRemovesReplica bool

	// Statistics.
	entries      int
	emptyEntries int
	mutations    int
	start        time.Time

	tableID      uint64
	rangeGroupID uint64
	TSTxnID      uint64
}

// Stage implements the apply.Batch interface. The method handles the first
// phase of applying a command to the replica state machine.
//
// The first thing the method does is determine whether the command should be
// applied at all or whether it should be rejected and replaced with an empty
// entry. The determination is based on the following rules: the command's
// MaxLeaseIndex must move the state machine's LeaseAppliedIndex forward, the
// proposer's lease (or rather its sequence number) must match that of the state
// machine, and lastly the GCThreshold must be below the timestamp that the
// command evaluated at. If any of the checks fail, the proposal's content is
// wiped and we apply an empty log entry instead. If a rejected command was
// proposed locally, the error will eventually be communicated to the waiting
// proposer. The two typical cases in which errors occur are lease mismatch (in
// which case the caller tries to send the command to the actual leaseholder)
// and violation of the LeaseAppliedIndex (in which case the proposal is retried
// if it was proposed locally).
//
// Assuming all checks were passed, the command's write batch is applied to the
// application batch. Its trivial ReplicatedState updates are then staged in
// the batch. This allows the batch to make an accurate determination about
// whether to accept or reject the next command that is staged without needing
// to actually update the replica state machine in between.
func (b *replicaAppBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx
	if cmd.ent.Index == 0 {
		return nil, makeNonDeterministicFailure("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.ent.Index, b.state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return nil, makeNonDeterministicFailure("applied index jumped from %d to %d", applied, idx)
	}
	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", cmd.idKey, cmd.raftCmd.MaxLeaseIndex)
	}

	// Determine whether the command should be applied to the replicated state
	// machine or whether it should be rejected (and replaced by an empty command).
	// This check is deterministic on all replicas, so if one replica decides to
	// reject a command, all will.
	if !b.r.shouldApplyCommand(ctx, cmd, &b.state) {
		if b.r.isTs() && cmd.IsLocal() {
			log.Infof(ctx, "r%d applying command with forced error: %s, proposal %x - %p", b.r.RangeID, cmd.forcedErr, cmd.idKey, cmd.proposal)
		} else {
			log.VEventf(ctx, 1, "applying command with forced error: %s", cmd.forcedErr)
		}

		// Apply an empty command.
		cmd.raftCmd.ReplicatedEvalResult = storagepb.ReplicatedEvalResult{}
		cmd.raftCmd.WriteBatch = nil
		cmd.raftCmd.LogicalOpLog = nil
		cmd.ent.Request = nil
	} else {
		log.Event(ctx, "applying command")
	}

	// Acquire the split or merge lock, if necessary. If a split or merge
	// command was rejected with a below-Raft forced error then its replicated
	// result was just cleared and this will be a no-op.
	if splitMergeUnlock, err := b.r.maybeAcquireSplitMergeLock(ctx, cmd.raftCmd); err != nil {
		kind := "merge"
		if cmd.raftCmd.ReplicatedEvalResult.Split != nil {
			kind = "split"
		}
		return nil, wrapWithNonDeterministicFailure(err, "unable to acquire "+kind+" lock")
	} else if splitMergeUnlock != nil {
		// Set the splitMergeUnlock on the replicaAppBatch to be called
		// after the batch has been applied (see replicaAppBatch.commit).
		cmd.splitMergeUnlock = splitMergeUnlock
	}

	// Update the batch's max timestamp.
	b.maxTS.Forward(cmd.replicatedResult().Timestamp)

	// Normalize the command, accounting for past migrations.
	b.migrateReplicatedResult(ctx, cmd)

	// Run any triggers that should occur before the batch is applied
	// and before the write batch is staged in the batch.
	if err := b.runPreApplyTriggersBeforeStagingWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Stage the command's write batch in the application batch.
	if err := b.stageWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Run any triggers that should occur before the batch is applied
	// but after the write batch is staged in the batch.
	if err := b.runPreApplyTriggersAfterStagingWriteBatch(ctx, cmd); err != nil {
		return nil, err
	}

	// Stage the command's trivial ReplicatedState updates in the batch. Any
	// non-trivial commands will be in their own batch, so delaying their
	// non-trivial ReplicatedState updates until later (without ever staging
	// them in the batch) is sufficient.
	b.stageTrivialReplicatedEvalResult(ctx, cmd)
	b.entries++
	if len(cmd.ent.Data) == 0 {
		b.emptyEntries++
	}

	// The command was checked by shouldApplyCommand, so it can be returned
	// as an apply.CheckedCommand.
	return cmd, nil
}

// migrateReplicatedResult performs any migrations necessary on the command to
// normalize it before applying it to the batch. This may modify the command.
func (b *replicaAppBatch) migrateReplicatedResult(ctx context.Context, cmd *replicatedCmd) {
	// If the command was using the deprecated version of the MVCCStats proto,
	// migrate it to the new version and clear out the field.
	res := cmd.replicatedResult()
	if deprecatedDelta := res.DeprecatedDelta; deprecatedDelta != nil {
		if res.Delta != (enginepb.MVCCStatsDelta{}) {
			log.Fatalf(ctx, "stats delta not empty but deprecated delta provided: %+v", cmd)
		}
		res.Delta = deprecatedDelta.ToStatsDelta()
		res.DeprecatedDelta = nil
	}
}

// CreateSnapshotForRead use CGO TsEngine create ts snapshot
func (r *Replica) CreateSnapshotForRead(
	ctx context.Context, startKey []byte, endKey []byte, hashNum uint64,
) (uint64, error) {
	tableID, startPoint, endPoint, startTs, endTs, err := sqlbase.DecodeTSRangeKey(startKey, endKey, hashNum)
	if err != nil {
		log.Errorf(ctx, "CreateSnapshotForRead failed: %v", err)
		return 0, err
	}
	tsSnapshotID, err := r.store.TsEngine.CreateSnapshotForRead(tableID, startPoint, endPoint, startTs, endTs)
	log.VEventf(ctx, 3, "TsEngine.CreateSnapshotForRead r%d, %d, %d, %d, %d, %d, %d, %d", r.RangeID, tableID, startPoint, endPoint, startTs, endTs, tsSnapshotID, err)
	if err != nil {
		return 0, errors.Wrapf(err, "Ts CreateSnapshotForRead err")
	}

	return tsSnapshotID, nil
}

// stateInfoConversion convert state info
func stateInfoConversion(state storagepb.ReplicaState) roachpb.ReplicaState {
	return roachpb.ReplicaState{
		RaftAppliedIndex:  state.RaftAppliedIndex,
		LeaseAppliedIndex: state.LeaseAppliedIndex,
		Desc:              state.Desc,
		Lease:             state.Lease,
		TruncatedState: &roachpb.TSRaftTruncatedState{
			Index: state.TruncatedState.Index,
			Term:  state.TruncatedState.Term,
		},
		GCThreshold:          state.GCThreshold,
		Stats:                state.Stats,
		UsingAppliedStateKey: state.UsingAppliedStateKey,
	}
}

// matchTsSpans gets the corresponding TsSpans that are in [startTs, endTs]
// (it includes both startTs and endTs), from the isSpans.
func matchTsSpans(inSpans []*roachpb.TsSpan, startTs, endTs int64) []*roachpb.TsSpan {
	var tsSpans []*roachpb.TsSpan
	for _, span := range inSpans {
		if span.TsStart > endTs || span.TsEnd < startTs {
			continue
		}
		if span.TsStart >= startTs && span.TsEnd <= endTs {
			tsSpans = append(tsSpans, span)
			continue
		}
		if span.TsStart >= startTs {
			tsSpans = append(tsSpans, &roachpb.TsSpan{TsStart: span.TsStart, TsEnd: endTs})
			continue
		}
		if span.TsEnd <= endTs {
			tsSpans = append(tsSpans, &roachpb.TsSpan{TsStart: startTs, TsEnd: span.TsEnd})
			continue
		}
		tsSpans = append(tsSpans, &roachpb.TsSpan{TsStart: startTs, TsEnd: endTs})
	}
	return tsSpans
}

// stageWriteBatch applies the command's write batch to the application batch's
// RocksDB batch. This batch is committed to RocksDB in replicaAppBatch.commit.
func (b *replicaAppBatch) stageWriteBatch(ctx context.Context, cmd *replicatedCmd) error {
	wb := cmd.raftCmd.WriteBatch
	// Check whether the TsRequest of the time series copy is stored on the disk,
	// call the corresponding storage processing interface according to the type
	// of entry.request, and encapsulate the Response
	if cmd.ent.Request != nil {
		var reqs roachpb.BatchRequest
		if err := protoutil.Unmarshal(cmd.ent.Request, &reqs); err == nil {
			var responses []roachpb.ResponseUnion
			isLocal := cmd.IsLocal()
			if isLocal && cmd.proposal.Local.Reply != nil {
				responses = cmd.proposal.Local.Reply.Responses
			}
			if b.tableID, b.rangeGroupID, b.TSTxnID, err = b.r.stageTsBatchRequest(
				ctx, &reqs, responses, isLocal, &b.state); err != nil {
				return err
			}
		}
	}
	if wb == nil {
		return nil
	}
	if mutations, err := storage.RocksDBBatchCount(wb.Data); err != nil {
		log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	} else {
		b.mutations += mutations
	}
	if err := b.batch.ApplyBatchRepr(wb.Data, false); err != nil {
		return wrapWithNonDeterministicFailure(err, "unable to apply WriteBatch")
	}
	return nil
}

// stageTsBatchRequest handles TsBatchRequest, write or delete data.
func (r *Replica) stageTsBatchRequest(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	responses []roachpb.ResponseUnion,
	isLocal bool,
	replicaState *storagepb.ReplicaState,
) (tableID, rangeGroupID, tsTxnID uint64, err error) {
	var isTsRequest bool
	for _, union := range ba.Requests {
		switch union.GetInner().(type) {
		case *roachpb.TsRowPutRequest,
			*roachpb.TsPutTagRequest,
			*roachpb.TsDeleteRequest,
			*roachpb.TsDeleteMultiEntitiesDataRequest,
			*roachpb.TsDeleteEntityRequest,
			*roachpb.TsTagUpdateRequest:
			tableID = uint64(r.Desc().TableId)
			isTsRequest = true
		case *roachpb.ClearRangeRequest:
			isTsRequest = true
		default:
			continue
		}
		break
	}
	if !isTsRequest {
		return tableID, rangeGroupID, tsTxnID, nil
	}

	rangeGroupID = 1 // storage only create RangeGroup 1
	var raftAppliedIndex uint64
	if replicaState != nil {
		raftAppliedIndex = replicaState.RaftAppliedIndex
	}
	if tableID != 0 {
		if tsTxnID, err = r.store.TsEngine.MtrBegin(tableID, rangeGroupID, uint64(r.RangeID), raftAppliedIndex); err != nil {
			var exist bool
			if exist, _ = r.store.TsEngine.TSIsTsTableExist(tableID); !exist {
				return tableID, rangeGroupID, tsTxnID, nil
			}
			return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to begin mini-transaction")
		}
	}

	for idx, union := range ba.Requests {
		switch req := union.GetInner().(type) {
		case *roachpb.TsPutTagRequest:
			{
				var payload [][]byte
				var dedupResult tse.DedupResult
				var entitiesAffect tse.EntitiesAffect
				payload = append(payload, req.Value.RawBytes)
				if payload != nil {
					if dedupResult, entitiesAffect, err = r.store.TsEngine.PutData(1, payload, tsTxnID, true); err != nil {
						errRollback := r.store.TsEngine.MtrRollback(tableID, rangeGroupID, tsTxnID)
						if errRollback != nil {
							return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to rollback mini-transaction")
						}
						return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to put data")
					}
					if isLocal && responses != nil {
						if req, ok := responses[idx].GetInner().(*roachpb.TsPutResponse); ok {
							if req.DedupRule != 0 {
								continue
							}
						}
						responses[idx] = roachpb.ResponseUnion{
							Value: &roachpb.ResponseUnion_TsPut{
								TsPut: &roachpb.TsPutResponse{
									ResponseHeader: roachpb.ResponseHeader{
										NumKeys: int64(dedupResult.DedupRows),
									},
									DedupRule:         int64(dedupResult.DedupRule),
									DiscardBitmap:     dedupResult.DiscardBitmap,
									EntitiesAffected:  uint32(entitiesAffect.EntityCount),
									UnorderedAffected: entitiesAffect.UnorderedCount,
								},
							},
						}
					}
				}
			}
		case *roachpb.TsRowPutRequest:
			{
				var dedupResult tse.DedupResult
				var entitiesAffect tse.EntitiesAffect
				if req.Values != nil {
					if dedupResult, entitiesAffect, err = r.store.TsEngine.PutRowData(1, req.HeaderPrefix, req.Values, req.ValueSize, tsTxnID, !req.CloseWAL); err != nil {
						errRollback := r.store.TsEngine.MtrRollback(tableID, rangeGroupID, tsTxnID)
						if errRollback != nil {
							return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to rollback mini-transaction")
						}
						return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to put data")
					}
					if isLocal && responses != nil {
						if res, ok := responses[idx].GetInner().(*roachpb.TsRowPutResponse); ok {
							if res.DedupRule != 0 {
								continue
							}
						}
						responses[idx] = roachpb.ResponseUnion{
							Value: &roachpb.ResponseUnion_TsRowPut{
								TsRowPut: &roachpb.TsRowPutResponse{
									ResponseHeader: roachpb.ResponseHeader{
										NumKeys: int64(len(req.Values) - dedupResult.DedupRows),
									},
									DedupRule:         int64(dedupResult.DedupRule),
									DiscardBitmap:     dedupResult.DiscardBitmap,
									EntitiesAffected:  uint32(entitiesAffect.EntityCount),
									UnorderedAffected: entitiesAffect.UnorderedCount,
								},
							},
						}
					}
				}
			}
		case *roachpb.TsDeleteRequest:
			{
				var delCnt uint64
				var ts1, ts2 int64
				hashNum := r.mu.state.Desc.HashNum
				if hashNum == 0 {
					hashNum = api.HashParamV2
				}
				_, _, ts1, err = sqlbase.DecodeTsRangeKey(r.mu.state.Desc.StartKey, true, hashNum)
				if err != nil {
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to get beginhash")
				}
				_, _, ts2, err = sqlbase.DecodeTsRangeKey(r.mu.state.Desc.EndKey, false, hashNum)
				if err != nil {
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to get endhash")
				}
				// exclude the EndKey
				ts2--
				tsSpans := matchTsSpans(req.TsSpans, ts1, ts2)
				if len(tsSpans) > 0 {
					if delCnt, err = r.store.TsEngine.DeleteData(
						req.TableId, rangeGroupID, req.PrimaryTags, tsSpans, tsTxnID); err != nil {
						errRollback := r.store.TsEngine.MtrRollback(tableID, rangeGroupID, tsTxnID)
						if errRollback != nil {
							return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to rollback mini-transaction")
						}
						return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to delete data")
					}
				}
				if isLocal && responses != nil {
					responses[idx] = roachpb.ResponseUnion{
						Value: &roachpb.ResponseUnion_TsDelete{
							TsDelete: &roachpb.TsDeleteResponse{
								ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(delCnt)},
							},
						},
					}
				}
			}
		case *roachpb.TsDeleteEntityRequest:
			{
				var delCnt uint64
				if delCnt, err = r.store.TsEngine.DeleteEntities(req.TableId, rangeGroupID, req.PrimaryTags, false, tsTxnID); err != nil {
					errRollback := r.store.TsEngine.MtrRollback(tableID, rangeGroupID, tsTxnID)
					if errRollback != nil {
						return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to rollback mini-transaction")
					}
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to delete entity")
				}
				if isLocal && responses != nil {
					responses[idx] = roachpb.ResponseUnion{
						Value: &roachpb.ResponseUnion_TsDeleteEntity{
							TsDeleteEntity: &roachpb.TsDeleteEntityResponse{
								ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(delCnt)},
							},
						},
					}
				}
			}

		case *roachpb.TsTagUpdateRequest:
			{
				var pld [][]byte
				pld = append(pld, req.Tags)
				if err = r.store.TsEngine.PutEntity(rangeGroupID, req.TableId, pld, tsTxnID); err != nil {
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "failed update tag")
				}
				if isLocal && responses != nil {
					responses[idx] = roachpb.ResponseUnion{
						Value: &roachpb.ResponseUnion_TsTagUpdate{
							TsTagUpdate: &roachpb.TsTagUpdateResponse{
								ResponseHeader: roachpb.ResponseHeader{NumKeys: 1},
							},
						},
					}
				}
			}
		case *roachpb.TsDeleteMultiEntitiesDataRequest:
			{
				var delCnt uint64
				var beginHash, endHash uint64
				var ts1, ts2 int64
				hashNum := r.mu.state.Desc.HashNum
				if hashNum == 0 {
					hashNum = api.HashParamV2
				}
				tableID, beginHash, ts1, err = sqlbase.DecodeTsRangeKey(r.mu.state.Desc.StartKey, true, hashNum)
				if err != nil {
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to get beginhash")
				}
				_, endHash, ts2, err = sqlbase.DecodeTsRangeKey(r.mu.state.Desc.EndKey, false, hashNum)
				if err != nil {
					return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to get endhash")
				}
				var tsSpans []*roachpb.TsSpan
				if beginHash == endHash {
					// exclude the EndKey
					ts2--
					// the hashPoint may be split, filter the ts spans
					tsSpans = matchTsSpans(req.TsSpans, ts1, ts2)
				} else {
					tsSpans = req.TsSpans
				}
				if len(tsSpans) > 0 {
					if delCnt, err = r.store.TsEngine.DeleteRangeData(req.TableId, uint64(1), beginHash, endHash, tsSpans, tsTxnID); err != nil {
						errRollback := r.store.TsEngine.MtrRollback(tableID, rangeGroupID, tsTxnID)
						if errRollback != nil {
							return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to rollback mini-transaction")
						}
						return tableID, rangeGroupID, tsTxnID, wrapWithNonDeterministicFailure(err, "unable to delete MultiEntities")
					}
				}
				if isLocal && responses != nil {
					responses[idx] = roachpb.ResponseUnion{
						Value: &roachpb.ResponseUnion_TsDeleteMultiEntitiesData{
							TsDeleteMultiEntitiesData: &roachpb.TsDeleteMultiEntitiesDataResponse{
								ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(delCnt)},
							},
						},
					}
				}
			}
		case *roachpb.ClearRangeRequest:
			if err = r.store.TsEngine.DropTsTable(req.TableId); err != nil {
				log.Errorf(ctx, "drop table %v failed: %v", req.TableId, err.Error())
				err = nil
			} else {
				log.Infof(ctx, "drop table %v succeed", req.TableId)
			}
		}
	}
	return
}

// changeRemovesStore returns true if any of the removals in this change have storeID.
func changeRemovesStore(
	desc *roachpb.RangeDescriptor, change *storagepb.ChangeReplicas, storeID roachpb.StoreID,
) (removesStore bool) {
	curReplica, existsInDesc := desc.GetReplicaDescriptor(storeID)
	// NB: if we're catching up from a preemptive snapshot then we won't
	// exist in the current descriptor and we can't be removed.
	if !existsInDesc {
		return false
	}

	// NB: We don't use change.Removed() because it will include replicas being
	// transitioned to VOTER_OUTGOING.

	// In 19.1 and before we used DeprecatedUpdatedReplicas instead of providing
	// a new range descriptor. Check first if this is 19.1 or earlier command which
	// uses DeprecatedChangeType and DeprecatedReplica
	if change.Desc == nil {
		return change.DeprecatedChangeType == roachpb.REMOVE_REPLICA && change.DeprecatedReplica.ReplicaID == curReplica.ReplicaID
	}
	// In 19.2 and beyond we supply the new range descriptor in the change.
	// We know we're removed if we do not appear in the new descriptor.
	_, existsInChange := change.Desc.GetReplicaDescriptor(storeID)
	return !existsInChange
}

// runPreApplyTriggersBeforeStagingWriteBatch runs any triggers that must fire
// before a command is applied to the state machine but after the command is
// staged in the replicaAppBatch's write batch. It may modify the command.
func (b *replicaAppBatch) runPreApplyTriggersBeforeStagingWriteBatch(
	ctx context.Context, cmd *replicatedCmd,
) error {
	if ops := cmd.raftCmd.LogicalOpLog; ops != nil {
		b.r.populatePrevValsInLogicalOpLogRaftMuLocked(ctx, ops, b.batch)
	}
	return nil
}

// runPreApplyTriggersAfterStagingWriteBatch runs any triggers that must fire
// before a command is applied to the state machine but after the command is
// staged in the replicaAppBatch's write batch. It may modify the command.
func (b *replicaAppBatch) runPreApplyTriggersAfterStagingWriteBatch(
	ctx context.Context, cmd *replicatedCmd,
) error {
	res := cmd.replicatedResult()

	// AddSSTable ingestions run before the actual batch gets written to the
	// storage engine. This makes sure that when the Raft command is applied,
	// the ingestion has definitely succeeded. Note that we have taken
	// precautions during command evaluation to avoid having mutations in the
	// WriteBatch that affect the SSTable. Not doing so could result in order
	// reversal (and missing values) here.
	//
	// NB: any command which has an AddSSTable is non-trivial and will be
	// applied in its own batch so it's not possible that any other commands
	// which precede this command can shadow writes from this SSTable.
	if res.AddSSTable != nil {
		copied := addSSTablePreApply(
			ctx,
			b.r.store.cfg.Settings,
			b.r.store.engine,
			b.r.raftMu.sideloaded,
			cmd.ent.Term,
			cmd.ent.Index,
			*res.AddSSTable,
			b.r.store.limiters.BulkIOWriteRate,
		)
		b.r.store.metrics.AddSSTableApplications.Inc(1)
		if copied {
			b.r.store.metrics.AddSSTableApplicationCopies.Inc(1)
		}
		if added := res.Delta.KeyCount; added > 0 {
			b.r.writeStats.recordCount(float64(added), 0)
		}
		res.AddSSTable = nil
	}

	if res.Split != nil {
		// Splits require a new HardState to be written to the new RHS
		// range (and this needs to be atomic with the main batch). This
		// cannot be constructed at evaluation time because it differs
		// on each replica (votes may have already been cast on the
		// uninitialized replica). Write this new hardstate to the batch too.
		// See https://gitee.com/kwbasedb/kwbase/issues/20629.
		//
		// Alternatively if we discover that the RHS has already been removed
		// from this store, clean up its data.
		splitPreApply(ctx, b.batch, res.Split.SplitTrigger, b.r)
	}

	if merge := res.Merge; merge != nil {
		// Merges require the subsumed range to be atomically deleted when the
		// merge transaction commits.

		// If our range currently has a non-zero replica ID then we know we're
		// safe to commit this merge because of the invariants provided to us
		// by the merge protocol. Namely if this committed we know that if the
		// command committed then all of the replicas in the range descriptor
		// are collocated when this command commits. If we do not have a non-zero
		// replica ID then the logic in Stage should detect that and destroy our
		// preemptive snapshot so we shouldn't ever get here.
		rhsRepl, err := b.r.store.GetReplica(merge.RightDesc.RangeID)
		if err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to get replica for merge")
		}

		// Use math.MaxInt32 (mergedTombstoneReplicaID) as the nextReplicaID as an
		// extra safeguard against creating new replicas of the RHS. This isn't
		// required for correctness, since the merge protocol should guarantee that
		// no new replicas of the RHS can ever be created, but it doesn't hurt to
		// be careful.
		const clearRangeIDLocalOnly = true
		const mustClearRange = false
		if err := rhsRepl.preDestroyRaftMuLocked(
			ctx, b.batch, b.batch, mergedTombstoneReplicaID, clearRangeIDLocalOnly, mustClearRange,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to destroy replica before merge")
		}
	}

	if res.State != nil && res.State.TruncatedState != nil {
		if err := b.r.adaptNewTruncateState(ctx, res.State.TruncatedState); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to adapt truncated state")
		}
		log.VEventf(ctx, 3, "try to apply truncate state %d, %d", res.State.TruncatedState.Index, res.State.TruncatedState.Term)
		if applied, err := handleTruncatedStateBelowRaft(
			ctx, b.state.TruncatedState, res.State.TruncatedState, b.r.raftMu.stateLoader, b.batch,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to handle truncated state")
		} else if !applied {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			res.State.TruncatedState = nil
			res.RaftLogDelta = 0
			// TODO(ajwerner): consider moving this code.
			// We received a truncation that doesn't apply to us, so we know that
			// there's a leaseholder out there with a log that has earlier entries
			// than ours. That leader also guided our log size computations by
			// giving us RaftLogDeltas for past truncations, and this was likely
			// off. Mark our Raft log size is not trustworthy so that, assuming
			// we step up as leader at some point in the future, we recompute
			// our numbers.
			b.r.mu.Lock()
			b.r.mu.raftLogSizeTrusted = false
			b.r.mu.Unlock()
		}
	}

	// Detect if this command will remove us from the range.
	// If so we stage the removal of all of our range data into this batch.
	// We'll complete the removal when it commits. Later logic detects the
	// removal by inspecting the destroy status.
	//
	// NB: This is the last step in the preApply which durably writes to the
	// replica state so that if it removes the replica it removes everything.
	if change := res.ChangeReplicas; change != nil &&
		changeRemovesStore(b.state.Desc, change, b.r.store.StoreID()) &&
		// Don't remove the data if the testing knobs ask us not to.
		!b.r.store.TestingKnobs().DisableEagerReplicaRemoval {

		// We mark the replica as destroyed so that new commands are not
		// accepted. This destroy status will be detected after the batch commits
		// by Replica.handleChangeReplicasTrigger() to finish the removal.
		//
		// NB: we must be holding the raftMu here because we're in the
		// midst of application.
		b.r.readOnlyCmdMu.Lock()
		b.r.mu.Lock()
		b.r.mu.destroyStatus.Set(
			roachpb.NewRangeNotFoundError(b.r.RangeID, b.r.store.StoreID()),
			destroyReasonRemoved)
		b.r.mu.Unlock()
		b.r.readOnlyCmdMu.Unlock()
		b.changeRemovesReplica = true

		// Delete all of the local data. We're going to delete the hard state too.
		// In order for this to be safe we need code above this to promise that we're
		// never going to write hard state in response to a message for a later
		// replica (with a different replica ID) to this range state.
		if err := b.r.preDestroyRaftMuLocked(
			ctx,
			b.batch,
			b.batch,
			change.NextReplicaID(),
			false, /* clearRangeIDLocalOnly */
			false, /* mustUseClearRange */
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to destroy replica before removal")
		}
	}

	// Provide the command's corresponding logical operations to the Replica's
	// rangefeed. Only do so if the WriteBatch is non-nil, in which case the
	// rangefeed requires there to be a corresponding logical operation log or
	// it will shut down with an error. If the WriteBatch is nil then we expect
	// the logical operation log to also be nil. We don't want to trigger a
	// shutdown of the rangefeed in that situation, so we don't pass anything to
	// the rangefed. If no rangefeed is running at all, this call will be a noop.
	if ops := cmd.raftCmd.LogicalOpLog; cmd.raftCmd.WriteBatch != nil {
		b.r.handleLogicalOpLogRaftMuLocked(ctx, ops, b.batch)
	} else if ops != nil && cmd.raftCmd.WriteBatch == nil {
		return nil
	} else if ops != nil {
		log.Fatalf(ctx, "non-nil logical op log with nil write batch: %v", cmd.raftCmd)
	}

	return nil
}

// stageTrivialReplicatedEvalResult applies the trivial portions of the
// command's ReplicatedEvalResult to the batch's ReplicaState. This function
// modifies the receiver's ReplicaState but does not modify ReplicatedEvalResult
// in order to give the TestingPostApplyFilter testing knob an opportunity to
// inspect the command's ReplicatedEvalResult.
func (b *replicaAppBatch) stageTrivialReplicatedEvalResult(
	ctx context.Context, cmd *replicatedCmd,
) {
	if raftAppliedIndex := cmd.ent.Index; raftAppliedIndex != 0 {
		b.state.RaftAppliedIndex = raftAppliedIndex
	}
	if leaseAppliedIndex := cmd.leaseIndex; leaseAppliedIndex != 0 {
		b.state.LeaseAppliedIndex = leaseAppliedIndex
	}
	res := cmd.replicatedResult()

	// Detect whether the incoming stats contain estimates that resulted from the
	// evaluation of a command under the 19.1 cluster version. These were either
	// evaluated on a 19.1 node (where ContainsEstimates is a bool, which maps
	// to 0 and 1 in 19.2+) or on a 19.2 node which hadn't yet had its cluster
	// version bumped.
	//
	// 19.2 nodes will never emit a ContainsEstimates outside of 0 or 1 until
	// the cluster version is active (during command evaluation). When the
	// version is active, they will never emit odd positive numbers (1, 3, ...).
	//
	// As a result, we can pinpoint exactly when the proposer of this command
	// has used the old cluster version: it's when the incoming
	// ContainsEstimates is 1. If so, we need to assume that an old node is processing
	// the same commands (as `true + true = true`), so make sure that `1 + 1 = 1`.
	_ = clusterversion.VersionContainsEstimatesCounter // see for info on ContainsEstimates migration
	deltaStats := res.Delta.ToStats()
	if deltaStats.ContainsEstimates == 1 && b.state.Stats.ContainsEstimates == 1 {
		deltaStats.ContainsEstimates = 0
	}

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	b.state.Stats.Add(deltaStats)
	// Exploit the fact that a split will result in a full stats
	// recomputation to reset the ContainsEstimates flag.
	// If we were running the new VersionContainsEstimatesCounter cluster version,
	// the consistency checker will be able to reset the stats itself, and splits
	// will as a side effect also remove estimates from both the resulting left and right hand sides.
	//
	// TODO(tbg): this can be removed in v20.2 and not earlier.
	// Consider the following scenario:
	// - all nodes are running 19.2
	// - all nodes rebooted into 20.1
	// - cluster version bumped, but node1 doesn't receive the gossip update for that
	// node1 runs a split that should emit ContainsEstimates=-1, but it clamps it to 0/1 because it
	// doesn't know that 20.1 is active.
	if res.Split != nil && deltaStats.ContainsEstimates == 0 {
		b.state.Stats.ContainsEstimates = 0
	}
	if res.State != nil && res.State.UsingAppliedStateKey && !b.state.UsingAppliedStateKey {
		b.migrateToAppliedStateKey = true
	}
}

// ApplyToStateMachine implements the apply.Batch interface. The method handles
// the second phase of applying a command to the replica state machine. It
// writes the application batch's accumulated RocksDB batch to the storage
// engine. This encompasses the persistent state transition portion of entry
// application.
func (b *replicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
	if log.V(4) {
		log.Infof(ctx, "flushing batch %v of %d entries", b.state, b.entries)
	}

	// Update the node clock with the maximum timestamp of all commands in the
	// batch. This maintains a high water mark for all ops serviced, so that
	// received ops without a timestamp specified are guaranteed one higher than
	// any op already executed for overlapping keys.
	r := b.r
	r.store.Clock().Update(b.maxTS)

	// Add the replica applied state key to the write batch if this change
	// doesn't remove us.
	if !b.changeRemovesReplica {
		if err := b.addAppliedStateKeyToBatch(ctx); err != nil {
			return err
		}
	}

	// Apply the write batch to RockDB. Entry application is done without
	// syncing to disk. The atomicity guarantees of the batch and the fact that
	// the applied state is stored in this batch, ensure that if the batch ends
	// up not being durably committed then the entries in this batch will be
	// applied again upon startup. However, if we're removing the replica's data
	// then we sync this batch as it is not safe to call postDestroyRaftMuLocked
	// before ensuring that the replica's data has been synchronously removed.
	// See handleChangeReplicasResult().
	sync := b.changeRemovesReplica
	if err := b.batch.Commit(sync, storage.NormalCommitType); err != nil {
		return wrapWithNonDeterministicFailure(err, "unable to commit Raft entry batch")
	}
	desc := b.state.Desc
	if desc.GetRangeType() == roachpb.TS_RANGE && b.changeRemovesReplica && r.store.TsEngine != nil && desc.TableId != 0 {
		exist, _ := r.store.TsEngine.TSIsTsTableExist(uint64(desc.TableId))
		log.VEventf(ctx, 3, "TsEngine.TSIsTsTableExist r%v, %v, %v", desc.RangeID, desc.TableId, exist)
		if exist {
			hashNum := desc.HashNum
			if hashNum == 0 {
				hashNum = api.HashParamV2
			}
			tableID, beginHash, endHash, startTs, endTs, err := sqlbase.DecodeTSRangeKey(desc.StartKey, desc.EndKey, hashNum)
			if err != nil {
				log.Errorf(ctx, "DecodeTSRangeKey failed: %v", err)
			} else {
				err = r.store.TsEngine.DeleteReplicaTSData(tableID, beginHash, endHash, startTs, endTs)
				log.VEventf(ctx, 3, "TsEngine.DeleteReplicaTSData %v, r%v, %v, %v, %v, %v, %v, %v", b.r.store.StoreID(), desc.RangeID, tableID, beginHash, endHash, startTs, endTs, err)
				if err != nil {
					log.Errorf(ctx, "DeleteReplicaTSData failed", err)
				}
			}
		}
	}
	if b.TSTxnID != 0 {
		err := b.r.store.TsEngine.MtrCommit(b.tableID, b.rangeGroupID, b.TSTxnID)
		if err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to commit mini-transaction")
		}
	}
	b.batch.Close()
	b.batch = nil

	// Update the replica's applied indexes and mvcc stats.
	r.mu.Lock()
	r.mu.state.RaftAppliedIndex = b.state.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = b.state.LeaseAppliedIndex
	prevStats := *r.mu.state.Stats
	*r.mu.state.Stats = *b.state.Stats

	// Check the queuing conditions while holding the lock.
	needsSplitBySize := r.needsSplitBySizeRLocked()
	needsMergeBySize := r.needsMergeBySizeRLocked()
	r.mu.Unlock()

	// Record the stats delta in the StoreMetrics.
	deltaStats := *b.state.Stats
	deltaStats.Subtract(prevStats)
	r.store.metrics.addMVCCStats(deltaStats)

	// Record the write activity, passing a 0 nodeID because replica.writeStats
	// intentionally doesn't track the origin of the writes.
	b.r.writeStats.recordCount(float64(b.mutations), 0 /* nodeID */)

	// NB: the bootstrap store has a nil split queue.
	// TODO(tbg): the above is probably a lie now.
	now := timeutil.Now()
	if r.store.splitQueue != nil && needsSplitBySize && r.splitQueueThrottle.ShouldProcess(now) {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	// The bootstrap store has a nil merge queue.
	// TODO(tbg): the above is probably a lie now.
	if r.store.mergeQueue != nil && needsMergeBySize && r.mergeQueueThrottle.ShouldProcess(now) {
		// TODO(tbg): for ranges which are small but protected from merges by
		// other means (zone configs etc), this is called on every command, and
		// fires off a goroutine each time. Make this trigger (and potentially
		// the split one above, though it hasn't been observed to be as
		// bothersome) less aggressive.
		r.store.mergeQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}

	b.recordStatsOnCommit()
	return nil
}

// addAppliedStateKeyToBatch adds the applied state key to the application
// batch's RocksDB batch. This records the highest raft and lease index that
// have been applied as of this batch. It also records the Range's mvcc stats.
func (b *replicaAppBatch) addAppliedStateKeyToBatch(ctx context.Context) error {
	loader := &b.r.raftMu.stateLoader
	if b.migrateToAppliedStateKey {
		// A Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		//
		// NB: entering this branch indicates that the batch contains only a
		// single non-trivial command.
		err := loader.MigrateToRangeAppliedStateKey(ctx, b.batch, b.state.Stats)
		if err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to migrate to range applied state")
		}
		b.state.UsingAppliedStateKey = true
	}
	if b.state.UsingAppliedStateKey {
		// Set the range applied state, which includes the last applied raft and
		// lease index along with the mvcc stats, all in one key.
		if err := loader.SetRangeAppliedState(
			ctx, b.batch, b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex, b.state.Stats,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to set range applied state")
		}
	} else {
		// Advance the last applied index. We use a blind write in order to avoid
		// reading the previous applied index keys on every write operation. This
		// requires a little additional work in order maintain the MVCC stats.
		var appliedIndexNewMS enginepb.MVCCStats
		if err := loader.SetLegacyAppliedIndexBlind(
			ctx, b.batch, &appliedIndexNewMS, b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex,
		); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to set applied index")
		}
		b.state.Stats.SysBytes += appliedIndexNewMS.SysBytes -
			loader.CalcAppliedIndexSysBytes(b.state.RaftAppliedIndex, b.state.LeaseAppliedIndex)

		// Set the legacy MVCC stats key.
		if err := loader.SetMVCCStats(ctx, b.batch, b.state.Stats); err != nil {
			return wrapWithNonDeterministicFailure(err, "unable to update MVCCStats")
		}
	}
	return nil
}

func (b *replicaAppBatch) recordStatsOnCommit() {
	b.sm.stats.entriesProcessed += b.entries
	b.sm.stats.numEmptyEntries += b.emptyEntries
	b.sm.stats.batchesProcessed++

	elapsed := timeutil.Since(b.start)
	b.r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
}

// Close implements the apply.Batch interface.
func (b *replicaAppBatch) Close() {
	if b.batch != nil {
		b.batch.Close()
	}
	*b = replicaAppBatch{}
}

// ephemeralReplicaAppBatch implements the apply.Batch interface.
//
// The batch performs the bare-minimum amount of work to be able to
// determine whether a replicated command should be rejected or applied.
type ephemeralReplicaAppBatch struct {
	r     *Replica
	state storagepb.ReplicaState
}

// Stage implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) Stage(cmdI apply.Command) (apply.CheckedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx

	mb.r.shouldApplyCommand(ctx, cmd, &mb.state)
	mb.state.LeaseAppliedIndex = cmd.leaseIndex
	return cmd, nil
}

// ApplyToStateMachine implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
	panic("cannot apply ephemeralReplicaAppBatch to state machine")
}

// Close implements the apply.Batch interface.
func (mb *ephemeralReplicaAppBatch) Close() {
	*mb = ephemeralReplicaAppBatch{}
}

// ApplySideEffects implements the apply.StateMachine interface. The method
// handles the third phase of applying a command to the replica state machine.
//
// It is called with commands whose write batches have already been committed
// to the storage engine and whose trivial side-effects have been applied to
// the Replica's in-memory state. This method deals with applying non-trivial
// side effects of commands, such as finalizing splits/merges and informing
// raft about applied config changes.
func (sm *replicaStateMachine) ApplySideEffects(
	cmdI apply.CheckedCommand,
) (apply.AppliedCommand, error) {
	cmd := cmdI.(*replicatedCmd)
	ctx := cmd.ctx

	// Deal with locking during side-effect handling, which is sometimes
	// associated with complex commands such as splits and merged.
	if unlock := cmd.splitMergeUnlock; unlock != nil {
		defer unlock()
	}

	// Set up the local result prior to handling the ReplicatedEvalResult to
	// give testing knobs an opportunity to inspect it. An injected corruption
	// error will lead to replica removal.
	sm.r.prepareLocalResult(ctx, cmd)
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEvent(ctx, 2, cmd.localResult.String())
	}

	// Handle the ReplicatedEvalResult, executing any side effects of the last
	// state machine transition.
	//
	// Note that this must happen after committing (the engine.Batch), but
	// before notifying a potentially waiting client.
	if cmd.replicatedResult().Split != nil {
		log.Infof(ctx, "%v", cmd)
	}
	clearTrivialReplicatedEvalResultFields(cmd.replicatedResult())
	if !cmd.IsTrivial() {
		shouldAssert, isRemoved := sm.handleNonTrivialReplicatedEvalResult(ctx, *cmd.replicatedResult())
		if isRemoved {
			// The proposal must not have been local, because we don't allow a
			// proposing replica to remove itself from the Range.
			cmd.FinishNonLocal(ctx)
			return nil, apply.ErrRemoved
		}
		// NB: Perform state assertion before acknowledging the client.
		// Some tests (TestRangeStatsInit) assumes that once the store has started
		// and the first range has a lease that there will not be a later hard-state.
		if shouldAssert {
			// Assert that the on-disk state doesn't diverge from the in-memory
			// state as a result of the side effects.
			sm.r.mu.Lock()
			sm.r.assertStateLocked(ctx, sm.r.store.Engine())
			sm.r.mu.Unlock()
			sm.stats.stateAssertions++
		}
	} else if res := cmd.replicatedResult(); !res.Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "failed to handle all side-effects of ReplicatedEvalResult: %v", res)
	}

	if cmd.replicatedResult().RaftLogDelta == 0 {
		sm.r.handleNoRaftLogDeltaResult(ctx)
	}
	if cmd.localResult != nil {
		sm.r.handleReadWriteLocalEvalResult(ctx, *cmd.localResult)
	}
	if err := sm.maybeApplyConfChange(ctx, cmd); err != nil {
		return nil, wrapWithNonDeterministicFailure(err, "unable to apply conf change")
	}

	// Mark the command as applied and return it as an apply.AppliedCommand.
	// NB: Commands which were reproposed at a higher MaxLeaseIndex will not be
	// considered local at this point as their proposal will have been detached
	// in prepareLocalResult().
	if cmd.IsLocal() {
		rejected := cmd.Rejected()
		higherReproposalsExist := cmd.raftCmd.MaxLeaseIndex != cmd.proposal.command.MaxLeaseIndex
		if !rejected && higherReproposalsExist {
			var reqs roachpb.BatchRequest
			if err := protoutil.Unmarshal(cmd.ent.Request, &reqs); err == nil {
				if len(reqs.Requests) == 0 {
					log.Infof(ctx, "higherReproposalsExist in empty request")
				} else {
					switch req := reqs.Requests[0].GetInner().(type) {
					default:
						log.Infof(ctx, "higherReproposalsExist in method(%d) [%v, %v], r%d, table%d",
							req.Method(), req.Header().Key, req.Header().EndKey, sm.r.RangeID, sm.r.Desc().TableId)
					}
				}
			}
			log.Fatalf(ctx, "finishing proposal [%s,%s] %p with outstanding reproposal at a higher max lease index, %d-%d",
				cmd.idKey, cmd.proposal.idKey, cmd.proposal, cmd.raftCmd.MaxLeaseIndex, cmd.proposal.command.MaxLeaseIndex)
		}
		if !rejected && cmd.proposal.applied {
			// If the command already applied then we shouldn't be "finishing" its
			// application again because it should only be able to apply successfully
			// once. We expect that when any reproposal for the same command attempts
			// to apply it will be rejected by the below raft lease sequence or lease
			// index check in checkForcedErr.
			log.Fatalf(ctx, "command already applied: %+v; unexpected successful result", cmd)
		}
		// If any reproposals at a higher MaxLeaseIndex exist we know that they will
		// never successfully apply, remove them from the map to avoid future
		// reproposals. If there is no command referencing this proposal at a higher
		// MaxLeaseIndex then it will already have been removed (see
		// shouldRemove in replicaDecoder.retrieveLocalProposals()). It is possible
		// that a later command in this batch referred to this proposal but it must
		// have failed because it carried the same MaxLeaseIndex.
		if higherReproposalsExist {
			sm.r.mu.Lock()
			delete(sm.r.mu.proposals, cmd.idKey)
			sm.r.mu.Unlock()
		}
		cmd.proposal.applied = true
	}
	return cmd, nil
}

// handleNonTrivialReplicatedEvalResult carries out the side-effects of
// non-trivial commands. It is run with the raftMu locked. It is illegal
// to pass a replicatedResult that does not imply any side-effects.
func (sm *replicaStateMachine) handleNonTrivialReplicatedEvalResult(
	ctx context.Context, rResult storagepb.ReplicatedEvalResult,
) (shouldAssert, isRemoved bool) {
	// Assert that this replicatedResult implies at least one side-effect.
	if rResult.Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "zero-value ReplicatedEvalResult passed to handleNonTrivialReplicatedEvalResult")
	}

	if rResult.State != nil {
		if rResult.State.TruncatedState != nil {
			rResult.RaftLogDelta += sm.r.handleTruncatedStateResult(ctx, rResult.State.TruncatedState)
			rResult.State.TruncatedState = nil
		}

		if (*rResult.State == storagepb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.RaftLogDelta != 0 {
		sm.r.handleRaftLogDeltaResult(ctx, rResult.RaftLogDelta)
		rResult.RaftLogDelta = 0
	}

	if rResult.SuggestedCompactions != nil {
		sm.r.handleSuggestedCompactionsResult(ctx, rResult.SuggestedCompactions)
		rResult.SuggestedCompactions = nil
	}

	// The rest of the actions are "nontrivial" and may have large effects on the
	// in-memory and on-disk ReplicaStates. If any of these actions are present,
	// we want to assert that these two states do not diverge.
	shouldAssert = !rResult.Equal(storagepb.ReplicatedEvalResult{})
	if !shouldAssert {
		return false, false
	}

	if rResult.Split != nil {
		sm.r.handleSplitResult(ctx, rResult.Split)
		rResult.Split = nil
	}

	if rResult.Merge != nil {
		sm.r.handleMergeResult(ctx, rResult.Merge)
		rResult.Merge = nil
	}

	if rResult.State != nil {
		if newDesc := rResult.State.Desc; newDesc != nil {
			sm.r.handleDescResult(ctx, newDesc)
			rResult.State.Desc = nil
		}

		if newLease := rResult.State.Lease; newLease != nil {
			sm.r.handleLeaseResult(ctx, newLease)
			rResult.State.Lease = nil
		}

		if newThresh := rResult.State.GCThreshold; newThresh != nil {
			sm.r.handleGCThresholdResult(ctx, newThresh)
			rResult.State.GCThreshold = nil
		}

		if rResult.State.UsingAppliedStateKey {
			sm.r.handleUsingAppliedStateKeyResult(ctx)
			rResult.State.UsingAppliedStateKey = false
		}

		if (*rResult.State == storagepb.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.ChangeReplicas != nil {
		isRemoved = sm.r.handleChangeReplicasResult(ctx, rResult.ChangeReplicas)
		rResult.ChangeReplicas = nil
	}

	if rResult.ComputeChecksum != nil {
		sm.r.handleComputeChecksumResult(ctx, rResult.ComputeChecksum)
		rResult.ComputeChecksum = nil
	}

	if !rResult.Equal(storagepb.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(rResult, storagepb.ReplicatedEvalResult{}))
	}
	return true, isRemoved
}

func (sm *replicaStateMachine) maybeApplyConfChange(ctx context.Context, cmd *replicatedCmd) error {
	switch cmd.ent.Type {
	case raftpb.EntryNormal:
		if cmd.replicatedResult().ChangeReplicas != nil {
			log.Fatalf(ctx, "unexpected replication change from command %s", &cmd.raftCmd)
		}
		return nil
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		if cmd.replicatedResult().ChangeReplicas == nil {
			// The command was rejected. There is no need to report a ConfChange
			// to raft.
			return nil
		}
		return sm.r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
			raftGroup.ApplyConfChange(cmd.confChange.ConfChangeI)
			return true, nil
		})
	default:
		panic("unexpected")
	}
}

func (sm *replicaStateMachine) moveStats() applyCommittedEntriesStats {
	stats := sm.stats
	sm.stats = applyCommittedEntriesStats{}
	return stats
}
