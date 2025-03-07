// Copyright 2018 The Cockroach Authors.
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

package jobs

import (
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// FakeNodeID is a dummy node ID for use in tests. It always stores 1.
var FakeNodeID = func() *base.NodeIDContainer {
	nodeID := base.NodeIDContainer{}
	nodeID.Reset(1)
	return &nodeID
}()

// FakeNodeLiveness allows simulating liveness failures without the full
// storage.NodeLiveness machinery.
type FakeNodeLiveness struct {
	mu struct {
		syncutil.Mutex
		livenessMap map[roachpb.NodeID]*storagepb.Liveness
	}

	// A non-blocking send is performed over these channels when the corresponding
	// method is called.
	SelfCalledCh          chan struct{}
	GetLivenessesCalledCh chan struct{}
}

// NewFakeNodeLiveness initializes a new NodeLiveness with nodeCount live nodes.
func NewFakeNodeLiveness(nodeCount int) *FakeNodeLiveness {
	nl := &FakeNodeLiveness{
		SelfCalledCh:          make(chan struct{}),
		GetLivenessesCalledCh: make(chan struct{}),
	}
	nl.mu.livenessMap = make(map[roachpb.NodeID]*storagepb.Liveness)
	for i := 0; i < nodeCount; i++ {
		nodeID := roachpb.NodeID(i + 1)
		nl.mu.livenessMap[nodeID] = &storagepb.Liveness{
			Epoch:      1,
			Expiration: hlc.LegacyTimestamp(hlc.MaxTimestamp),
			NodeID:     nodeID,
		}
	}
	return nl
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (*FakeNodeLiveness) ModuleTestingKnobs() {}

// Self implements the implicit storage.NodeLiveness interface. It uses NodeID
// as the node ID. On every call, a nonblocking send is performed over nl.ch to
// allow tests to execute a callback.
func (nl *FakeNodeLiveness) Self() (storagepb.Liveness, error) {
	select {
	case nl.SelfCalledCh <- struct{}{}:
	default:
	}
	nl.mu.Lock()
	defer nl.mu.Unlock()
	return *nl.mu.livenessMap[FakeNodeID.Get()], nil
}

// GetLivenesses implements the implicit storage.NodeLiveness interface.
func (nl *FakeNodeLiveness) GetLivenesses() (out []storagepb.Liveness) {
	select {
	case nl.GetLivenessesCalledCh <- struct{}{}:
	default:
	}
	nl.mu.Lock()
	defer nl.mu.Unlock()
	for _, liveness := range nl.mu.livenessMap {
		out = append(out, *liveness)
	}
	return out
}

// FakeIncrementEpoch increments the epoch for the node with the specified ID.
func (nl *FakeNodeLiveness) FakeIncrementEpoch(id roachpb.NodeID) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.livenessMap[id].Epoch++
}

// FakeSetExpiration sets the expiration time of the liveness for the node with
// the specified ID to ts.
func (nl *FakeNodeLiveness) FakeSetExpiration(id roachpb.NodeID, ts hlc.Timestamp) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.livenessMap[id].Expiration = hlc.LegacyTimestamp(ts)
}
