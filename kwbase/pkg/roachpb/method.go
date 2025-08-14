// Copyright 2015 The Cockroach Authors.
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

package roachpb

// Method is the enumerated type for methods.
type Method int

//go:generate stringer -type=Method
const (
	// Get fetches the value for a key from the KV map, respecting a
	// possibly historical timestamp. If the timestamp is 0, returns
	// the most recent value.
	Get Method = iota
	// Put sets the value for a key at the specified timestamp. If the
	// timestamp is 0, the value is set with the current time as timestamp.
	Put
	// ConditionalPut sets the value for a key if the existing value
	// matches the value specified in the request. Specifying a null value
	// for existing means the value must not yet exist.
	ConditionalPut
	// Increment increments the value at the specified key. Once called
	// for a key, Put & ConditionalPut will return errors; only
	// Increment will continue to be a valid command. The value must be
	// deleted before it can be reset using Put.
	Increment
	// Delete creates a tombstone value for the specified key, indicating
	// the value has been deleted.
	Delete
	// DeleteRange creates tombstone values for keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with the
	// latter endpoint excluded.
	DeleteRange
	// ClearRange removes all values (including all of their versions)
	// for keys which fall between args.RequestHeader.Key and
	// args.RequestHeader.EndKey, with the latter endpoint excluded.
	ClearRange
	// RevertRange removes all versions of values more recent than the
	// TargetTime for keys which fall between args.RequestHeader.Key and
	// args.RequestHeader.EndKey, with the latter endpoint excluded.
	RevertRange
	// Scan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	Scan
	// ReverseScan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	ReverseScan
	// EndTxn either commits or aborts an ongoing transaction.
	EndTxn
	// AdminSplit is called to coordinate a split of a range.
	AdminSplit
	// AdminUnsplit is called to remove the sticky bit of a manually split range.
	AdminUnsplit
	// AdminMerge is called to coordinate a merge of two adjacent ranges.
	AdminMerge
	// AdminTransferLease is called to initiate a range lease transfer.
	AdminTransferLease
	// AdminChangeReplicas is called to add or remove replicas for a range.
	AdminChangeReplicas
	// AdminRelocateRange is called to relocate the replicas for a range onto a
	// specified list of stores.
	AdminRelocateRange
	// HeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	HeartbeatTxn
	// GC garbage collects values based on expired timestamps
	// for a list of keys in a range. This method is called by the
	// range lease holder after a snapshot scan. The call goes through Raft,
	// so all range replicas GC the exact same values.
	GC
	// PushTxn attempts to resolve read or write conflicts between
	// transactions. Both the pusher (args.Txn) and the pushee
	// (args.PushTxn) are supplied. However, args.Key should be set to the
	// transaction ID of the pushee, as it must be directed to the range
	// containing the pushee's transaction record in order to consult the
	// most up to date txn state. If the conflict resolution can be
	// resolved in favor of the pusher, returns success; otherwise returns
	// an error code either indicating the pusher must retry or abort and
	// restart the transaction.
	PushTxn
	// RecoverTxn attempts to recover an abandoned STAGING transaction. It
	// specifies whether all of the abandoned transaction's in-flight writes
	// succeeded or whether any failed. This is used to determine whether the
	// result of the recovery should be committing the abandoned transaction or
	// aborting it.
	RecoverTxn
	// QueryTxn fetches the current state of the designated transaction.
	QueryTxn
	// QueryIntent checks whether the specified intent exists.
	QueryIntent
	// ResolveIntent resolves existing write intents for a key.
	ResolveIntent
	// ResolveIntentRange resolves existing write intents for a key range.
	ResolveIntentRange
	// Merge merges a given value into the specified key. Merge is a
	// high-performance operation provided by underlying data storage for values
	// which are accumulated over several writes. Because it is not
	// transactional, Merge is currently not made available to external clients.
	//
	// The logic used to merge values of different types is described in more
	// detail by the "Merge" method of engine.Engine.
	Merge
	// TruncateLog discards a prefix of the raft log.
	TruncateLog
	// RequestLease requests a range lease for a replica.
	RequestLease
	// TransferLease transfers the range lease from a lease holder to a new one.
	TransferLease
	// LeaseInfo returns information about a range's lease.
	LeaseInfo
	// ComputeChecksum starts a checksum computation over a replica snapshot.
	ComputeChecksum
	// CheckConsistency verifies the consistency of all ranges falling within a
	// key span.
	CheckConsistency
	// InitPut sets the value for a key if the key doesn't exist. It returns
	// an error if the key exists and the existing value is different from the
	// supplied one.
	InitPut
	// WriteBatch applies the operations encoded in a BatchRepr.
	WriteBatch
	// Export dumps a keyrange into files.
	Export
	// Import bulk loads key/value entries.
	Import
	// AdminScatter moves replicas and leaseholders for a selection of ranges.
	// Best-effort.
	AdminScatter
	// AddSSTable links a file into the RocksDB log-structured merge-tree.
	AddSSTable
	// RecomputeStats applies a delta to a Range's MVCCStats to fix computational errors.
	RecomputeStats
	// Refresh verifies no writes to a key have occurred since the
	// transaction orig timestamp and sets a new entry in the timestamp
	// cache at the current transaction timestamp.
	Refresh
	// RefreshRange verifies no writes have occurred to a span of keys
	// since the transaction orig timestamp and sets a new span in the
	// timestamp cache at the current transaction timestamp.
	RefreshRange
	// Subsume freezes a range for merging with its left-hand neighbor.
	Subsume
	// RangeStats returns the MVCC statistics for a range.
	RangeStats
	// AdminVerifyProtectedTimestamp VerifyProtectedTimestamp determines whether the specified protection record
	// will be respected by this Range.
	AdminVerifyProtectedTimestamp
	// TsPut insert value to TS DB
	TsPut
	// AdminRequestLeaseForTS requests the range lease
	AdminRequestLeaseForTS
	// AdminRelocateRangeGroup is called to relocate the replicas for a range onto a
	// specified list of stores.
	AdminRelocateRangeGroup
	// TsDelete delete data from TS DB
	TsDelete
	// TsDeleteEntity delete entity from TS DB
	TsDeleteEntity
	// TsTagUpdate updates tag in timeseries database
	TsTagUpdate
	// GetReplicaStatus get replica status
	GetReplicaStatus
	// CreateTSSnapshot create TS snapshot
	CreateTSSnapshot
	// TsDeleteMultiEntitiesData delete multi entities data
	TsDeleteMultiEntitiesData
	// TsRowPut insert row values to TS DB
	TsRowPut
	// TsPutTag insert only tag for an entity
	TsPutTag
	// TsRollbackTag insert only tag for an entity
	TsRollbackTag
	// TsCommitTag insert only tag for an entity
	TsCommitTag
	// TsImportFlush flush data after IMPORT
	TsImportFlush
)
