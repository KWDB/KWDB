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

package abortspan

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// An AbortSpan sets markers for aborted transactions to provide protection
// against an aborted but active transaction not reading values it wrote (due to
// its intents having been removed).
//
// The span is range-specific. It is updated when an intent for an aborted txn
// is cleared from a range, and is consulted before read commands are processed
// on a range.
//
// An AbortSpan is not thread safe.
type AbortSpan struct {
	rangeID roachpb.RangeID
}

// New returns a new AbortSpan. Every range replica
// maintains an AbortSpan, not just the lease holder.
func New(rangeID roachpb.RangeID) *AbortSpan {
	return &AbortSpan{
		rangeID: rangeID,
	}
}

func fillUUID(b byte) uuid.UUID {
	var ret uuid.UUID
	for i := range ret.GetBytes() {
		ret[i] = b
	}
	return ret
}

var txnIDMin = fillUUID('\x00')
var txnIDMax = fillUUID('\xff')

// MinKey returns the lower bound of the key span associated to an instance for the given RangeID.
func MinKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMin)
}

func (sc *AbortSpan) min() roachpb.Key {
	return MinKey(sc.rangeID)
}

// MaxKey returns the upper bound of the key span associated to an instance for the given RangeID.
func MaxKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMax)
}

func (sc *AbortSpan) max() roachpb.Key {
	return MaxKey(sc.rangeID)
}

// ClearData removes all persisted items stored in the cache.
func (sc *AbortSpan) ClearData(e storage.Engine) error {
	iter := e.NewIterator(storage.IterOptions{UpperBound: sc.max()})
	defer iter.Close()
	b := e.NewWriteOnlyBatch()
	defer b.Close()
	err := b.ClearIterRange(iter, sc.min(), sc.max())
	if err != nil {
		return err
	}
	return b.Commit(false /* sync */, storage.NormalCommitType)
}

// Get looks up an AbortSpan entry recorded for this transaction ID.
// Returns whether an abort record was found and any error.
func (sc *AbortSpan) Get(
	ctx context.Context, reader storage.Reader, txnID uuid.UUID, entry *roachpb.AbortSpanEntry,
) (bool, error) {
	// Pull response from disk and read into reply if available.
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	ok, err := storage.MVCCGetProto(ctx, reader, key, hlc.Timestamp{}, entry, storage.MVCCGetOptions{})
	return ok, err
}

// Iterate walks through the AbortSpan, invoking the given callback for
// each unmarshaled entry with the MVCC key and the decoded entry.
func (sc *AbortSpan) Iterate(
	ctx context.Context, reader storage.Reader, f func(roachpb.Key, roachpb.AbortSpanEntry) error,
) error {
	_, err := storage.MVCCIterate(ctx, reader, sc.min(), sc.max(), hlc.Timestamp{}, storage.MVCCScanOptions{},
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.AbortSpanEntry
			if _, err := keys.DecodeAbortSpanKey(kv.Key, nil); err != nil {
				return false, err
			}
			if err := kv.Value.GetProto(&entry); err != nil {
				return false, err
			}
			return false, f(kv.Key, entry)
		})
	return err
}

// Del removes all AbortSpan entries for the given transaction.
func (sc *AbortSpan) Del(
	ctx context.Context, reader storage.ReadWriter, ms *enginepb.MVCCStats, txnID uuid.UUID,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return storage.MVCCDelete(ctx, reader, ms, key, hlc.Timestamp{}, nil /* txn */)
}

// Put writes an entry for the specified transaction ID.
func (sc *AbortSpan) Put(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	txnID uuid.UUID,
	entry *roachpb.AbortSpanEntry,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return storage.MVCCPutProto(ctx, readWriter, ms, key, hlc.Timestamp{}, nil /* txn */, entry)
}

// CopyTo copies the abort span entries to the abort span for the range
// identified by newRangeID. Entries are read from r and written to w. It is
// safe for r and w to be the same object.
//
// CopyTo takes care to only copy records that are required: certain workloads
// create sizable abort spans, and repeated splitting can blow them up further.
// Once it reaches approximately the Raft MaxCommandSize, splits become
// impossible, which is pretty bad (see #25233).
func (sc *AbortSpan) CopyTo(
	ctx context.Context,
	r storage.Reader,
	w storage.ReadWriter,
	ms *enginepb.MVCCStats,
	ts hlc.Timestamp,
	newRangeID roachpb.RangeID,
) error {
	var abortSpanCopyCount, abortSpanSkipCount int
	// Abort span entries before this span are eligible for GC, so we don't
	// copy them into the new range. We could try to delete them from the LHS
	// as well, but that could create a large Raft command in itself. Plus,
	// we'd have to adjust the stats computations.
	threshold := ts.Add(-storagebase.TxnCleanupThreshold.Nanoseconds(), 0)
	var scratch [64]byte
	if err := sc.Iterate(ctx, r, func(k roachpb.Key, entry roachpb.AbortSpanEntry) error {
		if entry.Timestamp.Less(threshold) {
			// The entry would be garbage collected (if GC had run), so
			// don't bother copying it. Note that we can't filter on the key,
			// that is just where the txn record lives, but it doesn't tell
			// us whether the intents that triggered the abort span record
			// where on the LHS, RHS, or both.
			abortSpanSkipCount++
			return nil
		}

		abortSpanCopyCount++
		var txnID uuid.UUID
		txnID, err := keys.DecodeAbortSpanKey(k, scratch[:0])
		if err != nil {
			return err
		}
		return storage.MVCCPutProto(ctx, w, ms,
			keys.AbortSpanKey(newRangeID, txnID),
			hlc.Timestamp{}, nil, &entry,
		)
	}); err != nil {
		return roachpb.NewReplicaCorruptionError(errors.Wrap(err, "AbortSpan.CopyTo"))
	}
	log.Eventf(ctx, "abort span: copied %d entries, skipped %d", abortSpanCopyCount, abortSpanSkipCount)
	return nil
}
