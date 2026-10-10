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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// ErrTransactionUnsupported is returned when a non-transactional command is
// evaluated in the context of a transaction.
var ErrTransactionUnsupported = errors.New("not supported within a transaction")

// VerifyTransaction runs sanity checks verifying that the transaction in the
// header and the request are compatible.
func VerifyTransaction(
	h roachpb.Header, args roachpb.Request, permittedStatuses ...roachpb.TransactionStatus,
) error {
	if h.Txn == nil {
		return errors.Errorf("no transaction specified to %s", args.Method())
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return errors.Errorf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	statusPermitted := false
	for _, s := range permittedStatuses {
		if h.Txn.Status == s {
			statusPermitted = true
			break
		}
	}
	if !statusPermitted {
		return roachpb.NewTransactionStatusError(
			fmt.Sprintf("cannot perform %s with txn status %v", args.Method(), h.Txn.Status),
		)
	}
	return nil
}

// WriteAbortSpanOnResolve returns true if the abort span must be written when
// the transaction with the given status is resolved. It avoids instructing the
// caller to write to the abort span if the caller didn't actually remove any
// intents but intends to poison.
func WriteAbortSpanOnResolve(status roachpb.TransactionStatus, poison, removedIntents bool) bool {
	if status != roachpb.ABORTED {
		// Only update the AbortSpan for aborted transactions.
		return false
	}
	if !poison {
		// We can remove any entries from the AbortSpan.
		return true
	}
	// We only need to add AbortSpan entries for transactions that we have
	// invalidated by removing intents. This avoids leaking AbortSpan entries if
	// a request raced with txn record GC and mistakenly interpreted a committed
	// txn as aborted only to return to the intent it wanted to push and find it
	// already resolved. We're only required to write an entry if we do
	// something that could confuse/invalidate a zombie transaction.
	return removedIntents
}

// UpdateAbortSpan clears any AbortSpan entry if poison is false. Otherwise, if
// poison is true, it creates an entry for this transaction in the AbortSpan to
// prevent future reads or writes from spuriously succeeding on this range.
func UpdateAbortSpan(
	ctx context.Context,
	rec EvalContext,
	readWriter relational.ReadWriter,
	ms *enginepb.MVCCStats,
	txn enginepb.TxnMeta,
	poison bool,
) error {
	// Read the current state of the AbortSpan so we can detect when
	// no changes are needed. This can help us avoid unnecessary Raft
	// proposals.
	var curEntry roachpb.AbortSpanEntry
	exists, err := rec.AbortSpan().Get(ctx, readWriter, txn.ID, &curEntry)
	if err != nil {
		return err
	}

	if !poison {
		if !exists {
			return nil
		}
		return rec.AbortSpan().Del(ctx, readWriter, ms, txn.ID)
	}

	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.WriteTimestamp,
		Priority:  txn.Priority,
	}
	if exists && curEntry.Equal(entry) {
		return nil
	}
	// curEntry already escapes, so assign entry to curEntry and pass
	// that to Put instead of allowing entry to escape as well.
	curEntry = entry
	return rec.AbortSpan().Put(ctx, readWriter, ms, txn.ID, &curEntry)
}

// changePriority make normal user priority be a same txn priority because
// they are equal for pushing.
func changePriority(p enginepb.TxnPriority) enginepb.TxnPriority {
	if p == enginepb.MinTxnPriority || p == enginepb.MaxTxnPriority {
		return p
	}
	return enginepb.MinTxnPriority + 1
}

// CanPushWithPriority returns true if the given pusher can push the pushee
// based on its priority.
// For write-read conflicts, the read transaction is pusher and the write
// transaction is pushee, try to push the timestamp.
// Write-write conflicts and try to abort the transaction.
// Based on the type of pushee transaction being pushed, the isolation level
// and priority of the two transactions determine whether it can be pushed
func CanPushWithPriority(
	pusher, pushee *roachpb.Transaction,
	status roachpb.TransactionStatus,
	pushType roachpb.PushTxnType,
) bool {
	pusherPri := changePriority(pusher.Priority)
	pusheePri := changePriority(pushee.Priority)
	pusherIso := pusher.IsoLevel
	pusheeIso := pushee.IsoLevel
	switch pushType {
	case roachpb.PUSH_ABORT:
		return pusherPri > pusheePri
	case roachpb.PUSH_TIMESTAMP:
		// If the pushee's transaction state is "staging," the pusher's transaction must have a higher priority to push it.
		// If the two transactions have the same priority, a PUSH_TIMESTAMP operation must wait until the commit is complete.
		if status == roachpb.STAGING {
			return pusherPri > pusheePri
		}
		// For pushee transactions that are not in the "staging" state:
		//
		// If the pushee transaction's isolation level is RC (Read Committed), its timestamp can be pushed.
		return pusheeIso == enginepb.ReadCommitted ||
			// Else, if the pushee transaction does not tolerate write skew but the
			// pusher transaction does (and expects other to), let the PUSH_TIMESTAMP
			// proceed as long as the pusher has at least the same priority.
			(pusherIso == enginepb.ReadCommitted && pusherPri >= pusheePri) ||
			// Otherwise, if neither transaction tolerates write skew, let the
			// PUSH_TIMESTAMP proceed only if the pusher has a higher priority.
			(pusherPri > pusheePri)
	}
	return pusherPri > pusheePri
}

// CanCreateTxnRecord determines whether a transaction record can be created for
// the provided transaction. If not, the function will return an error. If so,
// the function may modify the provided transaction.
func CanCreateTxnRecord(ctx context.Context, rec EvalContext, txn *roachpb.Transaction) error {
	// Provide the transaction's minimum timestamp. The transaction could not
	// have written a transaction record previously with a timestamp below this.
	ok, minCommitTS, reason := rec.CanCreateTxnRecord(txn.ID, txn.Key, txn.MinTimestamp)
	if !ok {
		log.VEventf(ctx, 2, "txn tombstone present; transaction has been aborted")
		return roachpb.NewTransactionAbortedError(reason)
	}
	if bumped := txn.WriteTimestamp.Forward(minCommitTS); bumped {
		log.VEventf(ctx, 2, "write timestamp bumped by txn tombstone to: %s", txn.WriteTimestamp)
	}
	return nil
}

// SynthesizeTxnFromMeta creates a synthetic transaction object from
// the provided transaction metadata. The synthetic transaction is not
// meant to be persisted, but can serve as a representation of the
// transaction for outside observation. The function also checks
// whether it is possible for the transaction to ever create a
// transaction record in the future. If not, the returned transaction
// will be marked as ABORTED and it is safe to assume that the
// transaction record will never be written in the future.
//
// Note that the Transaction object returned by this function is
// inadequate to perform further KV reads or to perform intent
// resolution on its behalf, even if its state is PENDING. This is
// because the original Transaction object may have been partially
// rolled back and marked some of its intents as "ignored"
// (txn.IgnoredSeqNums != nil), but this state is not stored in
// TxnMeta. Proceeding to KV reads or intent resolution without this
// information would cause a partial rollback, if any, to be reverted
// and yield inconsistent data.
func SynthesizeTxnFromMeta(rec EvalContext, txn enginepb.TxnMeta) roachpb.Transaction {
	// Determine whether the transaction record could ever actually be written
	// in the future.
	txnMinTS := txn.MinTimestamp
	if txnMinTS.IsEmpty() {
		// If the transaction metadata's min timestamp is empty then provide its
		// provisional commit timestamp to CanCreateTxnRecord. If this timestamp
		// is larger than the transaction's real minimum timestamp then
		// CanCreateTxnRecord may return false positives (i.e. it determines
		// that the record could eventually be created when it actually
		// couldn't) but will never return false negatives (i.e. it will never
		// determine that the record could not be created when it actually
		// could). This is important, because it means that we may end up
		// failing to push a finalized transaction but will never determine that
		// a transaction is finalized when it still could end up committing.
		//
		// TODO(nvanbenschoten): This case is only possible for intents that
		// were written by a transaction coordinator before v19.2, which means
		// that we can remove it in v20.1 and replace it with:
		//
		//  synthTxnRecord.Status = roachpb.ABORTED
		//
		txnMinTS = txn.WriteTimestamp

		// If we don't need to worry about compatibility, disallow this case.
		if util.RaceEnabled {
			log.Fatalf(context.TODO(), "no minimum transaction timestamp provided: %v", txn)
		}
	}

	// Construct the transaction object.
	synthTxnRecord := roachpb.TransactionRecord{
		TxnMeta: txn,
		Status:  roachpb.PENDING,
		// Set the LastHeartbeat timestamp to the transactions's MinTimestamp. We
		// use this as an indication of client activity. Note that we cannot use
		// txn.WriteTimestamp for that purpose, as the WriteTimestamp could have
		// been bumped by other pushers.
		LastHeartbeat: txnMinTS,
	}

	ok, minCommitTS, _ := rec.CanCreateTxnRecord(txn.ID, txn.Key, txnMinTS)
	if ok {
		// Forward the provisional commit timestamp by the minimum timestamp that
		// the transaction would be able to create a transaction record at.
		synthTxnRecord.WriteTimestamp.Forward(minCommitTS)
	} else {
		// Mark the transaction as ABORTED because it is uncommittable.
		synthTxnRecord.Status = roachpb.ABORTED
	}
	return synthTxnRecord.AsTransaction()
}
