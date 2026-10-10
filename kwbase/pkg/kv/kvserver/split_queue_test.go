// Copyright 2015 The Cockroach Authors.
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
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
)

// TestSplitQueueShouldQueue verifies shouldSplitRange method correctly
// combines splits in zone configs with the size of the range.
func TestSplitQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Set zone configs.
	config.TestingSetZoneConfig(2000, zonepb.ZoneConfig{RangeMaxBytes: proto.Int64(32 << 20)})
	config.TestingSetZoneConfig(2002, zonepb.ZoneConfig{RangeMaxBytes: proto.Int64(32 << 20)})

	testCases := []struct {
		start, end roachpb.RKey
		bytes      int64
		maxBytes   int64
		shouldQ    bool
		priority   float64
	}{
		// No intersection, no bytes, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 0, 64 << 20, false, 0},
		// Intersection in zone, no bytes, no load.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// Already split at largest ID, no load.
		{keys.MakeTablePrefix(2002), roachpb.RKeyMax, 0, 32 << 20, false, 0},
		// Multiple intersections, no bytes, no load.
		{roachpb.RKeyMin, roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// No intersection, max bytes, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 20, 64 << 20, false, 0},
		// No intersection, max bytes+1, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64<<20 + 1, 64 << 20, true, 1},
		// No intersection, max bytes * 2, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 21, 64 << 20, true, 2},
		// Intersection, max bytes +1, no load.
		{keys.MakeTablePrefix(2000), roachpb.RKeyMax, 32<<20 + 1, 32 << 20, true, 2},
		// Split needed at table boundary, but no zone config, no load.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 32<<20 + 1, 64 << 20, true, 1},
	}

	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}
	ctx := context.Background()
	for i, test := range testCases {
		// Create a replica for testing that is not hooked up to the store. This
		// ensures that the store won't be mucking with our replica concurrently
		// during testing (e.g. via the system config gossip update).
		cpy := *tc.repl.Desc()
		cpy.StartKey = test.start
		cpy.EndKey = test.end
		repl, err := newReplica(ctx, &cpy, tc.store, cpy.Replicas().Voters()[0].ReplicaID)
		if err != nil {
			t.Fatal(err)
		}

		repl.mu.Lock()
		repl.mu.state.Stats = &enginepb.MVCCStats{KeyBytes: test.bytes}
		repl.mu.Unlock()
		zoneConfig := zonepb.DefaultZoneConfig()
		zoneConfig.RangeMaxBytes = proto.Int64(test.maxBytes)
		repl.SetZoneConfig(&zoneConfig)

		// Testing using shouldSplitRange instead of shouldQueue to avoid using the splitFinder
		// This tests the merge queue behavior too as a result. For splitFinder tests,
		// see split/split_test.go.
		shouldQ, priority := shouldSplitRange(repl.Desc(), repl.GetMVCCStats(), repl.GetMaxBytes(), cfg)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}
