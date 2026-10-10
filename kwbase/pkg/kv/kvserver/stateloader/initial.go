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

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// raftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5
)

// WriteInitialReplicaState sets up a new Range, but without writing an
// associated Raft state (which must be written separately via
// synthesizeRaftState before instantiating a Replica). The main task is to
// persist a ReplicaState which does not start from zero but presupposes a few
// entries already having applied. The supplied MVCCStats are used for the Stats
// field after adjusting for persisting the state itself, and the updated stats
// are returned.
func WriteInitialReplicaState(
	ctx context.Context,
	readWriter relational.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	rsl := Make(desc.RangeID)
	var s storagepb.ReplicaState
	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = &ms
	s.Lease = &lease
	s.GCThreshold = &gcThreshold
	s.UsingAppliedStateKey = true

	if existingLease, err := rsl.LoadLease(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading lease")
	} else if (existingLease != roachpb.Lease{}) {
		log.Fatalf(ctx, "expected trivial lease, but found %+v", existingLease)
	}

	if existingGCThreshold, err := rsl.LoadGCThreshold(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "error reading GCThreshold")
	} else if (*existingGCThreshold != hlc.Timestamp{}) {
		log.Fatalf(ctx, "expected trivial GChreshold, but found %+v", existingGCThreshold)
	}

	newMS, err := rsl.Save(ctx, readWriter, s, truncStateType)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}

// WriteInitialState calls WriteInitialReplicaState followed by
// SynthesizeRaftState. It is typically called during bootstrap. The supplied
// MVCCStats are used for the Stats field after adjusting for persisting the
// state itself, and the updated stats are returned.
func WriteInitialState(
	ctx context.Context,
	readWriter relational.ReadWriter,
	ms enginepb.MVCCStats,
	desc roachpb.RangeDescriptor,
	lease roachpb.Lease,
	gcThreshold hlc.Timestamp,
	truncStateType TruncatedStateType,
) (enginepb.MVCCStats, error) {
	newMS, err := WriteInitialReplicaState(
		ctx, readWriter, ms, desc, lease, gcThreshold, truncStateType)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := Make(desc.RangeID).SynthesizeRaftState(ctx, readWriter); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return newMS, nil
}
