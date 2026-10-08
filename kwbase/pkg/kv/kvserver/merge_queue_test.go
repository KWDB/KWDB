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

package kvserver

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
)

func TestMergeQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testCtx := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testCtx.Start(t, stopper)

	mq := newMergeQueue(testCtx.store, testCtx.store.DB(), testCtx.gossip, normal)
	storagebase.MergeQueueEnabled.Override(&testCtx.store.ClusterSettings().SV, true)

	tableKey := func(i uint32) []byte {
		return keys.MakeTablePrefix(keys.MaxReservedDescID + i)
	}

	config.TestingSetZoneConfig(keys.MaxReservedDescID+1, *zonepb.NewZoneConfig())
	config.TestingSetZoneConfig(keys.MaxReservedDescID+2, *zonepb.NewZoneConfig())

	type testCase struct {
		startKey, endKey []byte
		minBytes         int64
		bytes            int64
		expShouldQ       bool
		expPriority      float64
	}

	testCases := []testCase{
		// The last range of table 1 should not be mergeable because table 2 exists.
		{
			startKey: tableKey(1),
			endKey:   tableKey(2),
			minBytes: 1,
		},
		{
			startKey: append(tableKey(1), 'z'),
			endKey:   tableKey(2),
			minBytes: 1,
		},

		// Unlike the last range of table 1, the last range of table 2 is mergeable
		// because there is no table that follows. (In this test, the system only
		// knows about tables on which TestingSetZoneConfig has been called.)
		{
			startKey:    tableKey(2),
			endKey:      tableKey(3),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    append(tableKey(2), 'z'),
			endKey:      tableKey(3),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},

		// The last range is never mergeable.
		{
			startKey: tableKey(3),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},
		{
			startKey: append(tableKey(3), 'z'),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},

		// An interior range of a table is not mergeable if it meets or exceeds the
		// minimum byte threshold.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		// Edge case: a minimum byte threshold of zero. This effectively disables
		// the threshold, as an empty range is no longer considered mergeable.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    0,
			bytes:       0,
			expShouldQ:  false,
			expPriority: 0,
		},

		// An interior range of a table is mergeable if it does not meet the minimum
		// byte threshold. Its priority is inversely related to its size.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       0,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       768,
			expShouldQ:  true,
			expPriority: 0.25,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			repl := &Replica{}
			repl.mu.state.Desc = &roachpb.RangeDescriptor{StartKey: tc.startKey, EndKey: tc.endKey}
			repl.mu.state.Stats = &enginepb.MVCCStats{KeyBytes: tc.bytes}
			zoneConfig := zonepb.DefaultZoneConfigRef()
			zoneConfig.RangeMinBytes = proto.Int64(tc.minBytes)
			repl.SetZoneConfig(zoneConfig)
			repl.store = testCtx.store
			shouldQ, priority := mq.shouldQueue(ctx, hlc.Timestamp{}, repl, config.NewSystemConfig(zoneConfig))
			if tc.expShouldQ != shouldQ {
				t.Errorf("incorrect shouldQ: expected %v but got %v", tc.expShouldQ, shouldQ)
			}
			if tc.expPriority != priority {
				t.Errorf("incorrect priority: expected %v but got %v", tc.expPriority, priority)
			}
		})
	}
}
