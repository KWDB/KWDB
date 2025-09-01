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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/engine/tse"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func TestReplicaUpdateLastReplicaAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := func(replicaIDs ...roachpb.ReplicaID) roachpb.RangeDescriptor {
		d := roachpb.RangeDescriptor{
			StartKey:         roachpb.RKey("a"),
			EndKey:           roachpb.RKey("b"),
			InternalReplicas: make([]roachpb.ReplicaDescriptor, len(replicaIDs)),
		}
		for i, id := range replicaIDs {
			d.InternalReplicas[i].ReplicaID = id
		}
		return d
	}
	testCases := []struct {
		oldDesc                  roachpb.RangeDescriptor
		newDesc                  roachpb.RangeDescriptor
		lastReplicaAdded         roachpb.ReplicaID
		expectedLastReplicaAdded roachpb.ReplicaID
	}{
		// Adding a replica. In normal operation, Replica IDs always increase.
		{desc(), desc(1), 0, 1},
		{desc(1), desc(1, 2), 0, 2},
		{desc(1, 2), desc(1, 2, 3), 0, 3},
		// Add a replica with an out-of-order ID (this shouldn't happen in practice).
		{desc(2, 3), desc(1, 2, 3), 0, 0},
		// Removing a replica has no-effect.
		{desc(1, 2, 3), desc(2, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 2), 3, 0},
	}

	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var r Replica
			r.mu.state.Desc = &c.oldDesc
			r.mu.lastReplicaAdded = c.lastReplicaAdded
			r.store = tc.store
			r.concMgr = tc.repl.concMgr
			r.setDescRaftMuLocked(context.Background(), &c.newDesc)
			if c.expectedLastReplicaAdded != r.mu.lastReplicaAdded {
				t.Fatalf("expected %d, but found %d",
					c.expectedLastReplicaAdded, r.mu.lastReplicaAdded)
			}
		})
	}
}

func TestLoadRaftMuLockedReplicaMuLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsRange := roachpb.TS_RANGE
	desc := &roachpb.RangeDescriptor{
		RangeType:        &tsRange,
		StartKey:         roachpb.RKey("a"),
		InternalReplicas: []roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
	}
	state := storagepb.ReplicaState{
		RaftAppliedIndex: 17,
		Desc:             desc,
		Lease:            &roachpb.Lease{},
		TruncatedState: &roachpb.RaftTruncatedState{
			Term:  2,
			Index: 15,
		},
		GCThreshold: &hlc.Timestamp{},
		Stats:       &enginepb.MVCCStats{},
	}

	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	var preSavedTsFlushedIndex uint64 = 12
	testCases := []struct {
		isInit                   bool
		hasState                 bool
		hasTsFlushedIndex        bool
		enabledRaftLogCombineWal bool
		expectedAppliedIndex     uint64
		expectTsFlushedIndex     uint64
	}{
		{isInit: false, hasState: false, hasTsFlushedIndex: false, enabledRaftLogCombineWal: false,
			expectedAppliedIndex: 0, expectTsFlushedIndex: 0},
		{isInit: false, hasState: true, hasTsFlushedIndex: false, enabledRaftLogCombineWal: false,
			expectedAppliedIndex: state.RaftAppliedIndex, expectTsFlushedIndex: 0},
		{isInit: false, hasState: true, hasTsFlushedIndex: false, enabledRaftLogCombineWal: true,
			expectedAppliedIndex: state.TruncatedState.Index, expectTsFlushedIndex: state.TruncatedState.Index},
		{isInit: true, hasState: true, hasTsFlushedIndex: false, enabledRaftLogCombineWal: false,
			expectedAppliedIndex: state.RaftAppliedIndex, expectTsFlushedIndex: 0},
		{isInit: true, hasState: true, hasTsFlushedIndex: true, enabledRaftLogCombineWal: false,
			expectedAppliedIndex: preSavedTsFlushedIndex, expectTsFlushedIndex: preSavedTsFlushedIndex},
	}
	for i, c := range testCases {
		var r Replica
		r.store = tc.store
		r.concMgr = tc.repl.concMgr
		r.mu.replicaID = 1
		r.mu.stateLoader = stateloader.Make(roachpb.RangeID(i + 10))
		if c.enabledRaftLogCombineWal {
			tse.TsRaftLogCombineWAL.Override(&tc.store.ClusterSettings().SV, true)
		} else {
			tse.TsRaftLogCombineWAL.Override(&tc.store.ClusterSettings().SV, false)
		}
		if c.hasState {
			if _, err := r.mu.stateLoader.Save(ctx, r.Engine(), state, stateloader.TruncatedStateUnreplicated); err != nil {
				t.Errorf("save state error %s", err)
			}
		}
		if c.hasTsFlushedIndex {
			if err := r.mu.stateLoader.SetTsFlushedIndex(ctx, r.Engine(), preSavedTsFlushedIndex); err != nil {
				t.Errorf("set ts flushed index error %s", err)
			}
		}
		if err := r.loadRaftMuLockedReplicaMuLocked(desc, c.isInit); err != nil {
			t.Errorf("function error %s", err)
		}
		if r.mu.state.Desc != desc {
			t.Errorf("expected same desc")
		}
		if r.mu.tsFlushedIndex != c.expectTsFlushedIndex {
			t.Errorf("%d, expected %d, but found %d", i, c.expectTsFlushedIndex, r.mu.tsFlushedIndex)
		}
		if r.mu.state.RaftAppliedIndex != c.expectedAppliedIndex {
			t.Errorf("%d, expected %d, but found %d", i, c.expectedAppliedIndex, r.mu.state.RaftAppliedIndex)
		}
	}
}
