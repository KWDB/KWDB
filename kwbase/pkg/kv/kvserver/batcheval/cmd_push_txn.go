// Copyright 2014 The Cockroach Authors.
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

package batcheval

import (
	"bytes"
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/txnwait"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.PushTxn, declareKeysPushTransaction, PushTxn)
}

func declareKeysPushTransaction(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	pr := req.(*roachpb.PushTxnRequest)
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(pr.PusheeTxn.Key, pr.PusheeTxn.ID)})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, pr.PusheeTxn.ID)})
}

// PushTxn resolves conflicts between concurrent txns (or between
// a non-transactional reader or writer and a txn) in several ways,
// depending on the statuses and priorities of the conflicting
// transactions. The PushTxn operation is invoked by a "pusher"
// (args.PusherTxn -- the writer trying to abort a conflicting txn
// or the reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PusheeTxn -- the pushee txn whose intent(s) caused the
// conflict). A pusher is either transactional, in which case
// PusherTxn is completely initialized, or not, in which case the
// PusherTxn has only the priority set.
//
// The request arrives and immediately tries to determine the current
// disposition of the pushee transaction by reading its transaction
// record. If it finds one, it continues with the push. If not, it
// uses knowledge from the existence of the conflicting intent to
// determine the current state of the pushee. It's possible that the
// transaction record is missing either because it hasn't been written
// yet or because it has already been GCed after being finalized. Once
// the request determines which case its in, it decides whether to
// continue with the push. There are a number of different outcomes
// that a push can result in, based on the state that the pushee's
// transaction record is found in:
//
// Txn already committed/aborted: If the pushee txn is committed or
// aborted return success.
//
// Txn record expired: If the pushee txn is pending, its last
// heartbeat timestamp is observed to determine the latest client
// activity. This heartbeat is forwarded by the conflicting intent's
// timestamp because that timestamp also indicates definitive client
// activity. This time of "last activity" is compared against the
// current time to determine whether the transaction has expired.
// If so, it is aborted. NOTE: the intent timestamp used is not
// updated on intent pushes. This is important because it allows us
// to use its timestamp as an indication of recent activity. If this
// is ever changed, we don't run the risk of any correctness violations,
// but we do make it possible for intent pushes to look like client
// activity and extend the waiting period until a transaction is
// considered expired. This waiting period is a "courtesy" - if we
// simply aborted txns right away then we would see worse performance
// under contention, but everything would still be correct.
//
// Txn record not expired: If the pushee txn is not expired, its
// priority is compared against the pusher's (see CanPushWithPriority).
//
// Push cannot proceed: a TransactionPushError is returned.
//
// Push can proceed but txn record staging: if the transaction record
// is STAGING then it can't be changed by a pusher without going through
// the transaction recovery process. An IndeterminateCommitError is returned
// to kick off recovery.
//
// Push can proceed: the pushee's transaction record is modified and
// rewritten, based on the value of args.PushType. If args.PushType
// is PUSH_ABORT, txn.Status is set to ABORTED. If args.PushType is
// PUSH_TIMESTAMP, txn.Timestamp is set to just after args.PushTo.
//
// If the pushee is aborted, its timestamp will be forwarded to match
// its last client activity timestamp (i.e. last heartbeat), if available.
// This is done so that the updated timestamp populates the AbortSpan when
// the pusher proceeds to resolve intents, allowing the GC queue to purge
// records for which the transaction coordinator must have found out via
// its heartbeats that the transaction has failed.
func PushTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.PushTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.PushTxnResponse)

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if h.Timestamp.Less(args.PushTo) {
		// Verify that the PushTxn's timestamp is not less than the timestamp that
		// the request intends to push the transaction to. Transactions should not
		// be pushed into the future or their effect may not be fully reflected in
		// a future leaseholder's timestamp cache. This is analogous to how reads
		// should not be performed at a timestamp in the future.
		return result.Result{}, errors.Errorf("request timestamp %s less than PushTo timestamp %s", h.Timestamp, args.PushTo)
	}
	if h.Timestamp.Less(args.PusheeTxn.WriteTimestamp) {
		// This condition must hold for the timestamp cache access/update to be safe.
		return result.Result{}, errors.Errorf("request timestamp %s less than pushee txn timestamp %s", h.Timestamp, args.PusheeTxn.WriteTimestamp)
	}
	now := cArgs.EvalCtx.Clock().Now()
	if now.Less(h.Timestamp) {
		// The batch's timestamp should have been used to update the clock.
		return result.Result{}, errors.Errorf("request timestamp %s less than current clock time %s", h.Timestamp, now)
	}
	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return result.Result{}, errors.Errorf("request key %s should match pushee txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction; if missing, we're allowed to abort.
	var existTxn roachpb.Transaction
	ok, err := storage.MVCCGetProto(ctx, readWriter, key, hlc.Timestamp{}, &existTxn, storage.MVCCGetOptions{})
	if err != nil {
		return result.Result{}, err
	} else if !ok {
		log.VEventf(ctx, 2, "pushee txn record not found")
		// There are three cases in which there is no transaction record:
		//
		// * the pushee is still active but its transaction record has not
		//   been written yet. This is fairly common because transactions
		//   do not eagerly write their transaction record before writing
		//   intents, which another reader or writer might stumble upon and
		//   be forced to push.
		// * the pushee resolved its intents synchronously on successful commit;
		//   in this case, the transaction record of the pushee is also removed.
		//   Note that in this case, the intent which prompted this PushTxn
		//   doesn't exist any more.
		// * the pushee timed out or was aborted and the intent not cleaned up,
		//   but the transaction record was garbage collected.
		//
		// To determine which case we're in, we check whether the transaction could
		// ever write a transaction record. We do this by using the metadata from
		// the intent and attempting to synthesize a transaction record while
		// verifying that it would be possible for the transaction record to ever be
		// written. If a transaction record for the transaction could be written in
		// the future then we must be in the first case. If one could not be written
		// then we know we're in either the second or the third case.
		reply.PusheeTxn = SynthesizeTxnFromMeta(cArgs.EvalCtx, args.PusheeTxn)
		if reply.PusheeTxn.Status == roachpb.ABORTED {
			// If the transaction is uncommittable, we don't even need to
			// persist an ABORTED transaction record, we can just consider it
			// aborted. This is good because it allows us to obey the invariant
			// that only the transaction's own coordinator can create its
			// transaction record.
			result := result.Result{}
			result.Local.UpdatedTxns = []*roachpb.Transaction{&reply.PusheeTxn}
			return result, nil
		}
	} else {
		// Start with the persisted transaction record.
		reply.PusheeTxn = existTxn
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status.IsFinalized() {
		// Trivial noop.
		return result.Result{}, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == roachpb.PUSH_TIMESTAMP && args.PushTo.LessEq(reply.PusheeTxn.WriteTimestamp) {
		// Trivial noop.
		return result.Result{}, nil
	}

	// The pusher might be aware of a newer version of the pushee.
	increasedEpochOrTimestamp := false
	if reply.PusheeTxn.WriteTimestamp.Less(args.PusheeTxn.WriteTimestamp) {
		reply.PusheeTxn.WriteTimestamp = args.PusheeTxn.WriteTimestamp
		increasedEpochOrTimestamp = true
	}
	if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
		reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		increasedEpochOrTimestamp = true
	}
	reply.PusheeTxn.UpgradePriority(args.PusheeTxn.Priority)

	// If the pusher is aware that the pushee's currently recorded attempt at a
	// parallel commit failed, either because it found intents at a higher
	// timestamp than the parallel commit attempt or because it found intents at
	// a higher epoch than the parallel commit attempt, it should not consider
	// the pushee to be performing a parallel commit. Its commit status is not
	// indeterminate.
	if increasedEpochOrTimestamp && reply.PusheeTxn.Status == roachpb.STAGING {
		reply.PusheeTxn.Status = roachpb.PENDING
		reply.PusheeTxn.InFlightWrites = nil
	}

	pusheeStatus := reply.PusheeTxn.Status
	pushType := args.PushType
	var pusherWins bool
	var reason string

	switch {
	case txnwait.IsExpired(now, &reply.PusheeTxn):
		reason = "pushee is expired"
		// When cleaning up, actually clean up (as opposed to simply pushing
		// the garbage in the path of future writers).
		pushType = roachpb.PUSH_ABORT
		pusherWins = true
	case pushType == roachpb.PUSH_TOUCH:
		// If just attempting to cleanup old or already-committed txns,
		// pusher always fails.
		pusherWins = false
	case CanPushWithPriority(&args.PusherTxn, &reply.PusheeTxn, pusheeStatus, pushType):
		reason = "pusher has priority"
		pusherWins = true
	case args.Force:
		reason = "forced push"
		pusherWins = true
	}

	if log.V(1) && reason != "" {
		s := "pushed"
		if !pusherWins {
			s = "failed to push"
		}
		log.Infof(ctx, "%s "+s+" (push type=%s) %s: %s (pushee last active: %s)",
			args.PusherTxn.Short(), pushType, args.PusheeTxn.Short(),
			reason, reply.PusheeTxn.LastActive())
	}

	// If the pushed transaction is in the staging state, we can't change its
	// record without first going through the transaction recovery process and
	// attempting to finalize it.
	recoverOnFailedPush := cArgs.EvalCtx.EvalKnobs().RecoverIndeterminateCommitsOnFailedPushes
	if reply.PusheeTxn.Status == roachpb.STAGING && (pusherWins || recoverOnFailedPush) {
		err := roachpb.NewIndeterminateCommitError(reply.PusheeTxn)
		log.VEventf(ctx, 1, "%v", err)
		return result.Result{}, err
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(reply.PusheeTxn)
		log.VEventf(ctx, 1, "%v", err)
		return result.Result{}, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(args.PusherTxn.Priority - 1)

	// Determine what to do with the pushee, based on the push type.
	switch pushType {
	case roachpb.PUSH_ABORT:
		// If aborting the transaction, set the new status.
		reply.PusheeTxn.Status = roachpb.ABORTED
		// If the transaction record was already present, forward the timestamp
		// to accommodate AbortSpan GC. See method comment for details.
		if ok {
			reply.PusheeTxn.WriteTimestamp.Forward(reply.PusheeTxn.LastActive())
		}
	case roachpb.PUSH_TIMESTAMP:
		// Otherwise, update timestamp to be one greater than the request's
		// timestamp. This new timestamp will be use to update the read timestamp
		// cache. If the transaction record was not already present then we rely on
		// the timestamp cache to prevent the record from ever being written with a
		// timestamp beneath this timestamp.
		reply.PusheeTxn.WriteTimestamp.Forward(args.PushTo)
	default:
		return result.Result{}, errors.Errorf("unexpected push type: %v", pushType)
	}

	// If the transaction record was already present, persist the updates to it.
	// If not, then we don't want to create it. This could allow for finalized
	// transactions to be revived. Instead, we obey the invariant that only the
	// transaction's own coordinator can issue requests that create its
	// transaction record. To ensure that a timestamp push or an abort is
	// respected for transactions without transaction records, we rely on markers
	// in the timestamp cache.
	if ok {
		txnRecord := reply.PusheeTxn.AsRecord()
		if err := storage.MVCCPutProto(ctx, readWriter, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			return result.Result{}, err
		}
	}

	result := result.Result{}
	result.Local.UpdatedTxns = []*roachpb.Transaction{&reply.PusheeTxn}
	return result, nil
}
