// Copyright 2016 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/engine/tse"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/abortspan"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// SpanSetReplicaEvalContext is a testing-only implementation of
// ReplicaEvalContext which verifies that access to state is registered in the
// SpanSet if one is given.
type SpanSetReplicaEvalContext struct {
	i  batcheval.EvalContext
	ss spanset.SpanSet
}

var _ batcheval.EvalContext = &SpanSetReplicaEvalContext{}

// AbortSpan returns the abort span.
func (rec *SpanSetReplicaEvalContext) AbortSpan() *abortspan.AbortSpan {
	return rec.i.AbortSpan()
}

// EvalKnobs returns the batch evaluation Knobs.
func (rec *SpanSetReplicaEvalContext) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	return rec.i.EvalKnobs()
}

// StoreID returns the StoreID.
func (rec *SpanSetReplicaEvalContext) StoreID() roachpb.StoreID {
	return rec.i.StoreID()
}

// GetRangeID returns the RangeID.
func (rec *SpanSetReplicaEvalContext) GetRangeID() roachpb.RangeID {
	return rec.i.GetRangeID()
}

// ClusterSettings returns the cluster settings.
func (rec *SpanSetReplicaEvalContext) ClusterSettings() *cluster.Settings {
	return rec.i.ClusterSettings()
}

// Clock returns the Replica's clock.
func (rec *SpanSetReplicaEvalContext) Clock() *hlc.Clock {
	return rec.i.Clock()
}

// DB returns the Replica's client DB.
func (rec *SpanSetReplicaEvalContext) DB() *kv.DB {
	return rec.i.DB()
}

// GetConcurrencyManager returns the concurrency.Manager.
func (rec *SpanSetReplicaEvalContext) GetConcurrencyManager() concurrency.Manager {
	return rec.i.GetConcurrencyManager()
}

// NodeID returns the NodeID.
func (rec *SpanSetReplicaEvalContext) NodeID() roachpb.NodeID {
	return rec.i.NodeID()
}

// GetNodeLocality returns the node locality.
func (rec *SpanSetReplicaEvalContext) GetNodeLocality() roachpb.Locality {
	return rec.i.GetNodeLocality()
}

// Engine returns the engine.
func (rec *SpanSetReplicaEvalContext) Engine() storage.Engine {
	return rec.i.Engine()
}

// TsEngine returns the engine.
func (rec *SpanSetReplicaEvalContext) TsEngine() *tse.TsEngine {
	return rec.i.TsEngine()
}

// GetFirstIndex returns the first index.
func (rec *SpanSetReplicaEvalContext) GetFirstIndex() (uint64, error) {
	return rec.i.GetFirstIndex()
}

// GetTerm returns the term for the given index in the Raft log.
func (rec *SpanSetReplicaEvalContext) GetTerm(i uint64) (uint64, error) {
	return rec.i.GetTerm(i)
}

// GetLeaseAppliedIndex returns the lease index of the last applied command.
func (rec *SpanSetReplicaEvalContext) GetLeaseAppliedIndex() uint64 {
	return rec.i.GetLeaseAppliedIndex()
}

// IsFirstRange returns true iff the replica belongs to the first range.
func (rec *SpanSetReplicaEvalContext) IsFirstRange() bool {
	return rec.i.IsFirstRange()
}

// Desc returns the Replica's RangeDescriptor.
func (rec SpanSetReplicaEvalContext) Desc() *roachpb.RangeDescriptor {
	desc := rec.i.Desc()
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)},
	)
	return desc
}

// ContainsKey returns true if the given key is within the Replica's range.
//
// TODO(bdarnell): Replace this method with one on Desc(). See comment
// on Replica.ContainsKey.
func (rec SpanSetReplicaEvalContext) ContainsKey(key roachpb.Key) bool {
	desc := rec.Desc() // already asserts
	return storagebase.ContainsKey(desc, key)
}

// GetMVCCStats returns the Replica's MVCCStats.
func (rec SpanSetReplicaEvalContext) GetMVCCStats() enginepb.MVCCStats {
	// Thanks to commutativity, the spanlatch manager does not have to serialize
	// on the MVCCStats key. This means that the key is not included in SpanSet
	// declarations, so there's nothing to assert here.
	return rec.i.GetMVCCStats()
}

// GetSplitQPS returns the Replica's queries/s rate for splitting purposes.
func (rec SpanSetReplicaEvalContext) GetSplitQPS() float64 {
	return rec.i.GetSplitQPS()
}

// CanCreateTxnRecord determines whether a transaction record can be created
// for the provided transaction information. See Replica.CanCreateTxnRecord
// for details about its arguments, return values, and preconditions.
func (rec SpanSetReplicaEvalContext) CanCreateTxnRecord(
	txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
) (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.TransactionKey(txnKey, txnID)},
	)
	return rec.i.CanCreateTxnRecord(txnID, txnKey, txnMinTS)
}

// GetGCThreshold returns the GC threshold of the Range, typically updated when
// keys are garbage collected. Reads and writes at timestamps <= this time will
// not be served.
func (rec SpanSetReplicaEvalContext) GetGCThreshold() hlc.Timestamp {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeLastGCKey(rec.GetRangeID())},
	)
	return rec.i.GetGCThreshold()
}

// String implements Stringer.
func (rec SpanSetReplicaEvalContext) String() string {
	return rec.i.String()
}

// GetLastReplicaGCTimestamp returns the last time the Replica was
// considered for GC.
func (rec SpanSetReplicaEvalContext) GetLastReplicaGCTimestamp(
	ctx context.Context,
) (hlc.Timestamp, error) {
	if err := rec.ss.CheckAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeLastReplicaGCTimestampKey(rec.GetRangeID())},
	); err != nil {
		return hlc.Timestamp{}, err
	}
	return rec.i.GetLastReplicaGCTimestamp(ctx)
}

// GetLease returns the Replica's current and next lease (if any).
func (rec SpanSetReplicaEvalContext) GetLease() (roachpb.Lease, roachpb.Lease) {
	rec.ss.AssertAllowed(spanset.SpanReadOnly,
		roachpb.Span{Key: keys.RangeLeaseKey(rec.GetRangeID())},
	)
	return rec.i.GetLease()
}

// GetLimiters returns the per-store limiters.
func (rec *SpanSetReplicaEvalContext) GetLimiters() *batcheval.Limiters {
	return rec.i.GetLimiters()
}

// GetExternalStorage returns an ExternalStorage object, based on
// information parsed from a URI, stored in `dest`.
func (rec *SpanSetReplicaEvalContext) GetExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	return rec.i.GetExternalStorage(ctx, dest)
}

// GetExternalStorageFromURI returns an ExternalStorage object, based on the given URI.
func (rec *SpanSetReplicaEvalContext) GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (cloud.ExternalStorage, error) {
	return rec.i.GetExternalStorageFromURI(ctx, uri)
}
