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

package stateloader

import (
	"context"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// StateLoader contains accessor methods to read or write the
// fields of storagebase.ReplicaState. It contains an internal buffer
// which is reused to avoid an allocation on frequently-accessed code
// paths.
//
// Because of this internal buffer, this struct is not safe for
// concurrent use, and the return values of methods that return keys
// are invalidated the next time any method is called.
//
// It is safe to have multiple replicaStateLoaders for the same
// Replica. Reusable replicaStateLoaders are typically found in a
// struct with a mutex, and temporary loaders may be created when
// locking is less desirable than an allocation.
type StateLoader struct {
	keys.RangeIDPrefixBuf
}

// Make creates a a StateLoader.
func Make(rangeID roachpb.RangeID) StateLoader {
	rsl := StateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
	return rsl
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (rsl StateLoader) Load(
	ctx context.Context, reader storage.Reader, desc *roachpb.RangeDescriptor,
) (storagepb.ReplicaState, error) {
	var s storagepb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	lease, err := rsl.LoadLease(ctx, reader)
	if err != nil {
		return storagepb.ReplicaState{}, err
	}
	s.Lease = &lease

	if s.GCThreshold, err = rsl.LoadGCThreshold(ctx, reader); err != nil {
		return storagepb.ReplicaState{}, err
	}

	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return storagepb.ReplicaState{}, err
	} else if as != nil {
		s.UsingAppliedStateKey = true

		s.RaftAppliedIndex = as.RaftAppliedIndex
		s.LeaseAppliedIndex = as.LeaseAppliedIndex

		ms := as.RangeStats.ToStats()
		s.Stats = &ms
	} else {
		if s.RaftAppliedIndex, s.LeaseAppliedIndex, err = rsl.LoadAppliedIndex(ctx, reader); err != nil {
			return storagepb.ReplicaState{}, err
		}

		ms, err := rsl.LoadMVCCStats(ctx, reader)
		if err != nil {
			return storagepb.ReplicaState{}, err
		}
		s.Stats = &ms
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, _, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return storagepb.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// TruncatedStateType determines whether to use a replicated (legacy) or an
// unreplicated TruncatedState. See VersionUnreplicatedRaftTruncatedStateKey.
type TruncatedStateType int

const (
	// TruncatedStateLegacyReplicated means use the legacy (replicated) key.
	TruncatedStateLegacyReplicated TruncatedStateType = iota
	// TruncatedStateUnreplicated means use the new (unreplicated) key.
	TruncatedStateUnreplicated
)

// Save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing whenever save is called. Optional values should be reserved
// strictly for use in Result. Do before merge.
func (rsl StateLoader) Save(
	ctx context.Context,
	readWriter storage.ReadWriter,
	state storagepb.ReplicaState,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := rsl.SetLease(ctx, readWriter, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := rsl.SetGCThreshold(ctx, readWriter, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if truncStateType == TruncatedStateLegacyReplicated {
		if err := rsl.SetLegacyRaftTruncatedState(ctx, readWriter, ms, state.TruncatedState); err != nil {
			return enginepb.MVCCStats{}, err
		}
	} else {
		if err := rsl.SetRaftTruncatedState(ctx, readWriter, state.TruncatedState); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	if state.UsingAppliedStateKey {
		rai, lai := state.RaftAppliedIndex, state.LeaseAppliedIndex
		if err := rsl.SetRangeAppliedState(ctx, readWriter, rai, lai, ms); err != nil {
			return enginepb.MVCCStats{}, err
		}
	} else {
		if err := rsl.SetLegacyAppliedIndex(
			ctx, readWriter, ms, state.RaftAppliedIndex, state.LeaseAppliedIndex,
		); err != nil {
			return enginepb.MVCCStats{}, err
		}
		if err := rsl.SetLegacyMVCCStats(ctx, readWriter, ms); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (rsl StateLoader) LoadLease(
	ctx context.Context, reader storage.Reader,
) (roachpb.Lease, error) {
	var lease roachpb.Lease
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCGetOptions{})
	return lease, err
}

// SetLease persists a lease.
func (rsl StateLoader) SetLease(
	ctx context.Context, readWriter storage.ReadWriter, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeLeaseKey(),
		hlc.Timestamp{}, nil, &lease)
}

// LoadRangeAppliedState loads the Range applied state. The returned pointer
// will be nil if the applied state key is not found.
func (rsl StateLoader) LoadRangeAppliedState(
	ctx context.Context, reader storage.Reader,
) (*enginepb.RangeAppliedState, error) {
	var as enginepb.RangeAppliedState
	found, err := storage.MVCCGetProto(ctx, reader, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		storage.MVCCGetOptions{})
	if !found {
		return nil, err
	}
	return &as, err
}

// AssertNoRangeAppliedState asserts that no Range applied state key is present.
func (rsl StateLoader) AssertNoRangeAppliedState(ctx context.Context, reader storage.Reader) error {
	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return err
	} else if as != nil {
		log.Fatalf(ctx, "unexpected RangeAppliedState present: %v", as)
	}
	return nil
}

// LoadAppliedIndex returns the Raft applied index and the lease applied index.
func (rsl StateLoader) LoadAppliedIndex(
	ctx context.Context, reader storage.Reader,
) (raftAppliedIndex uint64, leaseAppliedIndex uint64, err error) {
	// Check the applied state key.
	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return 0, 0, err
	} else if as != nil {
		return as.RaftAppliedIndex, as.LeaseAppliedIndex, nil
	}

	// If the range applied state is not found, check the legacy Raft applied
	// index and the lease applied index keys. This is where these indices were
	// stored before the range applied state was introduced.
	v, _, err := storage.MVCCGet(ctx, reader, rsl.RaftAppliedIndexLegacyKey(),
		hlc.Timestamp{}, storage.MVCCGetOptions{})
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		raftAppliedIndex = uint64(int64AppliedIndex)
	}
	// TODO(tschottdorf): code duplication.
	v, _, err = storage.MVCCGet(ctx, reader, rsl.LeaseAppliedIndexLegacyKey(),
		hlc.Timestamp{}, storage.MVCCGetOptions{})
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64LeaseAppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		leaseAppliedIndex = uint64(int64LeaseAppliedIndex)
	}
	return raftAppliedIndex, leaseAppliedIndex, nil
}

// LoadMVCCStats loads the MVCC stats.
func (rsl StateLoader) LoadMVCCStats(
	ctx context.Context, reader storage.Reader,
) (enginepb.MVCCStats, error) {
	// Check the applied state key.
	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return enginepb.MVCCStats{}, err
	} else if as != nil {
		return as.RangeStats.ToStats(), nil
	}

	// If the range applied state is not found, check the legacy stats
	// key. This is where stats were stored before the range applied
	// state was introduced.
	var ms enginepb.MVCCStats
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeStatsLegacyKey(), hlc.Timestamp{}, &ms,
		storage.MVCCGetOptions{})
	return ms, err
}

// SetRangeAppliedState overwrites the range applied state. This state is a
// combination of the Raft and lease applied indices, along with the MVCC stats.
//
// The applied indices and the stats used to be stored separately in different
// keys. We now deem those keys to be "legacy" because they have been replaced
// by the range applied state key.
func (rsl StateLoader) SetRangeAppliedState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	appliedIndex, leaseAppliedIndex uint64,
	newMS *enginepb.MVCCStats,
) error {
	as := enginepb.RangeAppliedState{
		RaftAppliedIndex:  appliedIndex,
		LeaseAppliedIndex: leaseAppliedIndex,
		RangeStats:        newMS.ToPersistentStats(),
	}
	// The RangeAppliedStateKey is not included in stats. This is also reflected
	// in C.MVCCComputeStats and ComputeStatsGo.
	ms := (*enginepb.MVCCStats)(nil)
	return storage.MVCCPutProto(ctx, readWriter, ms, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, nil, &as)
}

// MigrateToRangeAppliedStateKey deletes the keys that were replaced by the
// RangeAppliedState key.
func (rsl StateLoader) MigrateToRangeAppliedStateKey(
	ctx context.Context, readWriter storage.ReadWriter, ms *enginepb.MVCCStats,
) error {
	noTS := hlc.Timestamp{}
	if err := storage.MVCCDelete(ctx, readWriter, ms, rsl.RaftAppliedIndexLegacyKey(), noTS, nil); err != nil {
		return err
	}
	if err := storage.MVCCDelete(ctx, readWriter, ms, rsl.LeaseAppliedIndexLegacyKey(), noTS, nil); err != nil {
		return err
	}
	return storage.MVCCDelete(ctx, readWriter, ms, rsl.RangeStatsLegacyKey(), noTS, nil)
}

// SetLegacyAppliedIndex sets the legacy {raft,lease} applied index values,
// properly accounting for existing keys in the returned stats.
//
// The range applied state key cannot already exist or an assetion will be
// triggered. See comment on SetRangeAppliedState for why this is "legacy".
func (rsl StateLoader) SetLegacyAppliedIndex(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex, leaseAppliedIndex uint64,
) error {
	if err := rsl.AssertNoRangeAppliedState(ctx, readWriter); err != nil {
		return err
	}

	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := storage.MVCCPut(ctx, readWriter, ms,
		rsl.RaftAppliedIndexLegacyKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return storage.MVCCPut(ctx, readWriter, ms,
		rsl.LeaseAppliedIndexLegacyKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

// SetLegacyAppliedIndexBlind sets the legacy {raft,lease} applied index values
// using a "blind" put which ignores any existing keys. This is identical to
// SetLegacyAppliedIndex but is used to optimize the writing of the applied
// index values during write operations where we definitively know the size of
// the previous values.
//
// The range applied state key cannot already exist or an assetion will be
// triggered. See comment on SetRangeAppliedState for why this is "legacy".
func (rsl StateLoader) SetLegacyAppliedIndexBlind(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	appliedIndex, leaseAppliedIndex uint64,
) error {
	if err := rsl.AssertNoRangeAppliedState(ctx, readWriter); err != nil {
		return err
	}

	var value roachpb.Value
	value.SetInt(int64(appliedIndex))
	if err := storage.MVCCBlindPut(ctx, readWriter, ms,
		rsl.RaftAppliedIndexLegacyKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return storage.MVCCBlindPut(ctx, readWriter, ms,
		rsl.LeaseAppliedIndexLegacyKey(),
		hlc.Timestamp{},
		value,
		nil /* txn */)
}

func inlineValueIntEncodedSize(v int64) int {
	var value roachpb.Value
	value.SetInt(v)
	meta := enginepb.MVCCMetadata{RawBytes: value.RawBytes}
	return meta.Size()
}

// CalcAppliedIndexSysBytes calculates the size (MVCCStats.SysBytes) of the {raft,lease} applied
// index keys/values.
func (rsl StateLoader) CalcAppliedIndexSysBytes(appliedIndex, leaseAppliedIndex uint64) int64 {
	return int64(storage.MakeMVCCMetadataKey(rsl.RaftAppliedIndexLegacyKey()).EncodedSize() +
		storage.MakeMVCCMetadataKey(rsl.LeaseAppliedIndexLegacyKey()).EncodedSize() +
		inlineValueIntEncodedSize(int64(appliedIndex)) +
		inlineValueIntEncodedSize(int64(leaseAppliedIndex)))
}

func (rsl StateLoader) writeLegacyMVCCStatsInternal(
	ctx context.Context, readWriter storage.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	// NB: newMS is copied to prevent conditional calls to this method from
	// causing the stats argument to escape. This is legacy code which does
	// not need to be optimized for performance.
	newMSCopy := *newMS
	return storage.MVCCPutProto(ctx, readWriter, nil, rsl.RangeStatsLegacyKey(), hlc.Timestamp{}, nil, &newMSCopy)
}

// SetLegacyMVCCStats overwrites the legacy MVCC stats key.
//
// The range applied state key cannot already exist or an assetion will be
// triggered. See comment on SetRangeAppliedState for why this is "legacy".
func (rsl StateLoader) SetLegacyMVCCStats(
	ctx context.Context, readWriter storage.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	if err := rsl.AssertNoRangeAppliedState(ctx, readWriter); err != nil {
		return err
	}

	return rsl.writeLegacyMVCCStatsInternal(ctx, readWriter, newMS)
}

// SetMVCCStats overwrites the MVCC stats. This needs to perform a read on the
// RangeAppliedState key before overwriting the stats. Use SetRangeAppliedState
// when performance is important.
func (rsl StateLoader) SetMVCCStats(
	ctx context.Context, readWriter storage.ReadWriter, newMS *enginepb.MVCCStats,
) error {
	if as, err := rsl.LoadRangeAppliedState(ctx, readWriter); err != nil {
		return err
	} else if as != nil {
		return rsl.SetRangeAppliedState(ctx, readWriter, as.RaftAppliedIndex, as.LeaseAppliedIndex, newMS)
	}

	return rsl.writeLegacyMVCCStatsInternal(ctx, readWriter, newMS)
}

// SetLegacyRaftTruncatedState overwrites the truncated state.
func (rsl StateLoader) SetLegacyRaftTruncatedState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	return storage.MVCCPutProto(ctx, readWriter, ms,
		rsl.RaftTruncatedStateLegacyKey(), hlc.Timestamp{}, nil, truncState)
}

// LoadGCThreshold loads the GC threshold.
func (rsl StateLoader) LoadGCThreshold(
	ctx context.Context, reader storage.Reader,
) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, reader, rsl.RangeLastGCKey(),
		hlc.Timestamp{}, &t, storage.MVCCGetOptions{})
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (rsl StateLoader) SetGCThreshold(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return storage.MVCCPutProto(ctx, readWriter, ms,
		rsl.RangeLastGCKey(), hlc.Timestamp{}, nil, threshold)
}

// The rest is not technically part of ReplicaState.

// LoadLastIndex loads the last index.
func (rsl StateLoader) LoadLastIndex(ctx context.Context, reader storage.Reader) (uint64, error) {
	prefix := rsl.RaftLogPrefix()
	iter := reader.NewIterator(storage.IterOptions{LowerBound: prefix})
	defer iter.Close()

	var lastIndex uint64
	iter.SeekLT(storage.MakeMVCCMetadataKey(rsl.RaftLogKey(math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.Key()
		var err error
		_, lastIndex, err = encoding.DecodeUint64Ascending(key.Key[len(prefix):])
		if err != nil {
			log.Fatalf(ctx, "unable to decode Raft log index key: %s", key)
		}
	}

	if lastIndex == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, _, err := rsl.LoadRaftTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

// LoadRaftTruncatedState loads the truncated state. The returned boolean returns
// whether the result was read from the TruncatedStateLegacyKey. If both keys
// are missing, it is false which is used to migrate into the unreplicated key.
//
// See VersionUnreplicatedRaftTruncatedState.
func (rsl StateLoader) LoadRaftTruncatedState(
	ctx context.Context, reader storage.Reader,
) (_ roachpb.RaftTruncatedState, isLegacy bool, _ error) {
	var truncState roachpb.RaftTruncatedState
	if found, err := storage.MVCCGetProto(
		ctx, reader, rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, &truncState, storage.MVCCGetOptions{},
	); err != nil {
		return roachpb.RaftTruncatedState{}, false, err
	} else if found {
		return truncState, false, nil
	}

	// If the "new" truncated state isn't there (yet), fall back to the legacy
	// truncated state. The next log truncation will atomically rewrite them
	// assuming the cluster version has advanced sufficiently.
	//
	// See VersionUnreplicatedRaftTruncatedState.
	legacyFound, err := storage.MVCCGetProto(
		ctx, reader, rsl.RaftTruncatedStateLegacyKey(), hlc.Timestamp{}, &truncState, storage.MVCCGetOptions{},
	)
	if err != nil {
		return roachpb.RaftTruncatedState{}, false, err
	}
	return truncState, legacyFound, nil
}

// SetRaftTruncatedState overwrites the truncated state.
func (rsl StateLoader) SetRaftTruncatedState(
	ctx context.Context, writer storage.Writer, truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	// "Blind" because ms == nil and timestamp == hlc.Timestamp{}.
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		nil, /* ms */
		rsl.RaftTruncatedStateKey(),
		hlc.Timestamp{}, /* timestamp */
		truncState,
		nil, /* txn */
	)
}

// LoadHardState loads the HardState.
func (rsl StateLoader) LoadHardState(
	ctx context.Context, reader storage.Reader,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := storage.MVCCGetProto(ctx, reader, rsl.RaftHardStateKey(),
		hlc.Timestamp{}, &hs, storage.MVCCGetOptions{})

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

// SetHardState overwrites the HardState.
func (rsl StateLoader) SetHardState(
	ctx context.Context, writer storage.Writer, hs raftpb.HardState,
) error {
	// "Blind" because ms == nil and timestamp == hlc.Timestamp{}.
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		nil, /* ms */
		rsl.RaftHardStateKey(),
		hlc.Timestamp{}, /* timestamp */
		&hs,
		nil, /* txn */
	)
}

// SynthesizeRaftState creates a Raft state which synthesizes both a HardState
// and a lastIndex from pre-seeded data in the engine (typically created via
// writeInitialReplicaState and, on a split, perhaps the activity of an
// uninitialized Raft group)
func (rsl StateLoader) SynthesizeRaftState(
	ctx context.Context, readWriter storage.ReadWriter,
) error {
	hs, err := rsl.LoadHardState(ctx, readWriter)
	if err != nil {
		return err
	}
	truncState, _, err := rsl.LoadRaftTruncatedState(ctx, readWriter)
	if err != nil {
		return err
	}
	raftAppliedIndex, _, err := rsl.LoadAppliedIndex(ctx, readWriter)
	if err != nil {
		return err
	}
	return rsl.SynthesizeHardState(ctx, readWriter, hs, truncState, raftAppliedIndex)
}

// SynthesizeHardState synthesizes an on-disk HardState from the given input,
// taking care that a HardState compatible with the existing data is written.
func (rsl StateLoader) SynthesizeHardState(
	ctx context.Context,
	readWriter storage.ReadWriter,
	oldHS raftpb.HardState,
	truncState roachpb.RaftTruncatedState,
	raftAppliedIndex uint64,
) error {
	newHS := raftpb.HardState{
		Term: truncState.Term,
		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: raftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return log.Safe(errors.Errorf("can't decrease HardState.Commit from %d to %d",
			oldHS.Commit, newHS.Commit))
	}
	if oldHS.Term > newHS.Term {
		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}
	// If the existing HardState voted in this term, remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
	}
	err := rsl.SetHardState(ctx, readWriter, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}

// SetTsFlushedIndex overwrites the ts flushed index.
func (rsl StateLoader) SetTsFlushedIndex(
	ctx context.Context, writer storage.Writer, flushedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(flushedIndex))
	// "Blind" because ms == nil and timestamp == hlc.Timestamp{}.
	return storage.MVCCBlindPut(
		ctx,
		writer,
		nil, /* ms */
		rsl.TsFlushedIndexKey(),
		hlc.Timestamp{}, /* timestamp */
		value,
		nil, /* txn */
	)
}

// LoadTsFlushedIndex overwrites the HardState.
func (rsl StateLoader) LoadTsFlushedIndex(
	ctx context.Context, reader storage.Reader,
) (uint64, error) {
	// If the range applied state is not found, check the legacy Raft applied
	// index and the lease applied index keys. This is where these indices were
	// stored before the range applied state was introduced.
	v, _, err := storage.MVCCGet(ctx, reader, rsl.TsFlushedIndexKey(),
		hlc.Timestamp{}, storage.MVCCGetOptions{})
	if err != nil {
		return 0, err
	}
	var tsFlushedIndex uint64
	if v != nil {
		int64TsFlushedIndex, err := v.GetInt()
		if err != nil {
			return 0, err
		}
		tsFlushedIndex = uint64(int64TsFlushedIndex)
	}
	return tsFlushedIndex, nil
}
