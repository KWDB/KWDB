// Copyright 2017 The Cockroach Authors.
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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

type wrappedBatch struct {
	relational.Batch
	clearCount      int
	clearRangeCount int
}

func (wb *wrappedBatch) Clear(key relational.MVCCKey) error {
	wb.clearCount++
	return wb.Batch.Clear(key)
}

func (wb *wrappedBatch) ClearRange(start, end relational.MVCCKey) error {
	wb.clearRangeCount++
	return wb.Batch.ClearRange(start, end)
}

// TestCmdClearRangeBytesThreshold verifies that clear range resorts to
// clearing keys individually if under the bytes threshold and issues a
// clear range command to the batch otherwise.
func TestCmdClearRangeBytesThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	valueStr := strings.Repeat("0123456789", 1024)
	var value roachpb.Value
	value.SetString(valueStr) // 10KiB
	halfFull := ClearRangeBytesThreshold / (2 * len(valueStr))
	overFull := ClearRangeBytesThreshold/len(valueStr) + 1
	tests := []struct {
		keyCount           int
		expClearCount      int
		expClearRangeCount int
	}{
		{
			keyCount:           1,
			expClearCount:      1,
			expClearRangeCount: 0,
		},
		// More than a single key, but not enough to use ClearRange.
		{
			keyCount:           halfFull,
			expClearCount:      halfFull,
			expClearRangeCount: 0,
		},
		// With key sizes requiring additional space, this will overshoot.
		{
			keyCount:           overFull,
			expClearCount:      0,
			expClearRangeCount: 1,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			eng := relational.NewDefaultInMem()
			defer eng.Close()

			var stats enginepb.MVCCStats
			for i := 0; i < test.keyCount; i++ {
				key := roachpb.Key(fmt.Sprintf("%04d", i))
				if err := relational.MVCCPut(ctx, eng, &stats, key, hlc.Timestamp{WallTime: int64(i % 2)}, value, nil); err != nil {
					t.Fatal(err)
				}
			}

			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			var h roachpb.Header
			h.RangeID = desc.RangeID

			cArgs := CommandArgs{Header: h}
			cArgs.EvalCtx = (&MockEvalCtx{Desc: &desc, Clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), Stats: stats}).EvalContext()
			cArgs.Args = &roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    startKey,
					EndKey: endKey,
				},
			}
			cArgs.Stats = &enginepb.MVCCStats{}

			if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
				t.Fatal(err)
			}

			// Verify cArgs.Stats is equal to the stats we wrote.
			newStats := stats
			newStats.SysBytes, newStats.SysCount = 0, 0       // ignore these values
			cArgs.Stats.SysBytes, cArgs.Stats.SysCount = 0, 0 // these too, as GC threshold is updated
			newStats.Add(*cArgs.Stats)
			newStats.AgeTo(0) // pin at LastUpdateNanos==0
			if !newStats.Equal(enginepb.MVCCStats{}) {
				t.Errorf("expected stats on original writes to be negated on clear range: %+v vs %+v", stats, *cArgs.Stats)
			}

			// Verify we see the correct counts for Clear and ClearRange.
			if a, e := batch.clearCount, test.expClearCount; a != e {
				t.Errorf("expected %d clears; got %d", e, a)
			}
			if a, e := batch.clearRangeCount, test.expClearRangeCount; a != e {
				t.Errorf("expected %d clear ranges; got %d", e, a)
			}

			// Now ensure that the data is gone, whether it was a ClearRange or individual calls to clear.
			if err := batch.Commit(true /* commit */, relational.NormalCommitType); err != nil {
				t.Fatal(err)
			}
			if err := eng.Iterate(startKey, endKey,
				func(kv relational.MVCCKeyValue) (bool, error) {
					return true, errors.New("expected no data in underlying engine")
				},
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}
