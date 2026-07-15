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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func testTSRangeDescriptor(
	rangeID roachpb.RangeID, startHash, endHash uint64,
) *roachpb.RangeDescriptor {
	tsRange := roachpb.TS_RANGE
	hashNum := uint64(200)
	return &roachpb.RangeDescriptor{
		RangeID:   rangeID,
		RangeType: &tsRange,
		HashNum:   hashNum,
		StartKey:  roachpb.RKey(sqlbase.MakeTsRangeKey(1, startHash, hashNum)),
		EndKey:    roachpb.RKey(sqlbase.MakeTsRangeKey(1, endHash, hashNum)),
	}
}

func withTSRangeDataVolumeHook(
	t *testing.T, hook func(desc *roachpb.RangeDescriptor) (uint64, error),
) {
	t.Helper()
	prev := tsRangeDataVolumeHook
	tsRangeDataVolumeHook = hook
	t.Cleanup(func() { tsRangeDataVolumeHook = prev })
}

func TestCorrectTSRangeMVCCStatsFromDataVolume(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	desc := testTSRangeDescriptor(10, 0, 10)

	for _, tc := range []struct {
		name       string
		desc       *roachpb.RangeDescriptor
		stats      *enginepb.MVCCStats
		hook       func(desc *roachpb.RangeDescriptor) (uint64, error)
		wantOK     bool
		wantStats  enginepb.MVCCStats
		wantMetric int64
	}{
		{
			name:      "default range is noop",
			desc:      &roachpb.RangeDescriptor{RangeID: 1},
			stats:     &enginepb.MVCCStats{ValBytes: 100},
			wantOK:    false,
			wantStats: enginepb.MVCCStats{ValBytes: 100},
		},
		{
			name:   "nil stats is noop",
			desc:   desc,
			stats:  nil,
			wantOK: false,
		},
		{
			name: "updates val and live bytes from data volume",
			desc: desc,
			stats: &enginepb.MVCCStats{
				ValBytes:  100,
				LiveBytes: 50,
			},
			hook: func(desc *roachpb.RangeDescriptor) (uint64, error) {
				require.Equal(t, roachpb.RangeID(10), desc.RangeID)
				return 5000, nil
			},
			wantOK: true,
			wantStats: enginepb.MVCCStats{
				ValBytes:  5000,
				LiveBytes: 5000,
			},
		},
		{
			name: "unchanged when volume already matches",
			desc: desc,
			stats: &enginepb.MVCCStats{
				ValBytes:  5000,
				LiveBytes: 5000,
			},
			hook: func(desc *roachpb.RangeDescriptor) (uint64, error) {
				return 5000, nil
			},
			wantOK: false,
			wantStats: enginepb.MVCCStats{
				ValBytes:  5000,
				LiveBytes: 5000,
			},
		},
		{
			name: "GetDataVolume failure keeps incremental stats",
			desc: desc,
			stats: &enginepb.MVCCStats{
				ValBytes:  100,
				LiveBytes: 100,
			},
			hook: func(desc *roachpb.RangeDescriptor) (uint64, error) {
				return 0, errors.New("GetDataVolume failed")
			},
			wantOK: false,
			wantStats: enginepb.MVCCStats{
				ValBytes:  100,
				LiveBytes: 100,
			},
			wantMetric: 1,
		},
		{
			name: "nil TsEngine without hook is noop",
			desc: desc,
			stats: &enginepb.MVCCStats{
				ValBytes:  100,
				LiveBytes: 100,
			},
			wantOK: false,
			wantStats: enginepb.MVCCStats{
				ValBytes:  100,
				LiveBytes: 100,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.hook != nil {
				withTSRangeDataVolumeHook(t, tc.hook)
			} else {
				withTSRangeDataVolumeHook(t, nil)
			}
			caseMetrics := newStoreMetrics(time.Hour)

			stats := enginepb.MVCCStats{}
			if tc.stats != nil {
				stats = *tc.stats
			}
			gotOK := correctTSRangeMVCCStatsFromDataVolume(ctx, caseMetrics, nil, tc.desc, &stats)

			require.Equal(t, tc.wantOK, gotOK)
			if tc.stats != nil {
				require.Equal(t, tc.wantStats, stats)
			}
			require.Equal(t, tc.wantMetric, caseMetrics.TsRangeStatsDataVolumeFailures.Count())
		})
	}
}

func TestTsRangeDataVolume(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := testTSRangeDescriptor(11, 0, 10)

	t.Run("nil TsEngine returns error", func(t *testing.T) {
		withTSRangeDataVolumeHook(t, nil)
		_, err := tsRangeDataVolume(nil, desc)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TsEngine is nil")
	})

	t.Run("hook bypasses TsEngine", func(t *testing.T) {
		withTSRangeDataVolumeHook(t, func(got *roachpb.RangeDescriptor) (uint64, error) {
			require.Equal(t, desc.RangeID, got.RangeID)
			return 1234, nil
		})
		volume, err := tsRangeDataVolume(nil, desc)
		require.NoError(t, err)
		require.Equal(t, uint64(1234), volume)
	})
}

func TestTotalBytesForQueueTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("non-replica uses GetMVCCStats", func(t *testing.T) {
		repl := mvccStatsReplicaInQueue{
			size: 100,
		}
		require.Equal(t, int64(100), totalBytesForQueueTimeout(ctx, repl))
	})

	t.Run("replica reconciles TS range before sizing", func(t *testing.T) {
		withTSRangeDataVolumeHook(t, func(desc *roachpb.RangeDescriptor) (uint64, error) {
			return 2000, nil
		})

		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(t, stopper)

		desc := testTSRangeDescriptor(tc.repl.RangeID, 0, 10)
		tc.repl.mu.Lock()
		tc.repl.mu.state.Desc = desc
		tc.repl.mu.state.Stats = &enginepb.MVCCStats{
			KeyBytes: 10,
			ValBytes: 100,
			SysBytes: 70,
		}
		tc.repl.mu.Unlock()

		want := int64(10 + 2000 + 70)
		require.Equal(t, want, totalBytesForQueueTimeout(ctx, tc.repl))
		require.Equal(t, int64(2000), tc.repl.GetMVCCStats().ValBytes)
	})
}

func TestGetMVCCStatsForDecisions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	withTSRangeDataVolumeHook(t, func(desc *roachpb.RangeDescriptor) (uint64, error) {
		return 4096, nil
	})

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	desc := testTSRangeDescriptor(tc.repl.RangeID, 0, 10)
	tc.repl.mu.Lock()
	tc.repl.mu.state.Desc = desc
	tc.repl.mu.state.Stats = &enginepb.MVCCStats{ValBytes: 128, LiveBytes: 128}
	tc.repl.mu.Unlock()

	stats := tc.repl.GetMVCCStatsForDecisions(ctx)
	require.Equal(t, int64(4096), stats.ValBytes)
	require.Equal(t, int64(4096), stats.LiveBytes)
	require.Equal(t, int64(4096), tc.repl.GetMVCCStats().ValBytes)
}

func TestReconcileTSRangeStatsForSnapshotMemoryOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	withTSRangeDataVolumeHook(t, func(desc *roachpb.RangeDescriptor) (uint64, error) {
		return 8192, nil
	})

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	desc := testTSRangeDescriptor(tc.repl.RangeID, 0, 10)
	tc.repl.mu.Lock()
	tc.repl.mu.state.Desc = desc
	tc.repl.mu.state.Stats = &enginepb.MVCCStats{ValBytes: 64, LiveBytes: 64}
	tc.repl.mu.Unlock()

	diskStatsBefore, err := stateloader.Make(tc.repl.RangeID).LoadMVCCStats(ctx, tc.engine)
	require.NoError(t, err)

	tc.repl.reconcileTSRangeStatsForSnapshot(ctx)

	tc.repl.mu.RLock()
	memStats := *tc.repl.mu.state.Stats
	tc.repl.mu.RUnlock()

	diskStatsAfter, err := stateloader.Make(tc.repl.RangeID).LoadMVCCStats(ctx, tc.engine)
	require.NoError(t, err)
	require.Equal(t, diskStatsBefore, diskStatsAfter)
	require.Equal(t, int64(8192), memStats.ValBytes)
	require.Equal(t, int64(8192), memStats.LiveBytes)
}

func TestTSRangeReplicatedValueForChecksum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := testTSRangeDescriptor(42, 0, 10)
	rasKey := keys.RangeAppliedStateKey(desc.RangeID)
	legacyKey := keys.RangeStatsLegacyKey(desc.RangeID)

	makeRASValue := func(valBytes int64) []byte {
		ras := enginepb.RangeAppliedState{
			RaftAppliedIndex:  7,
			LeaseAppliedIndex: 3,
			RangeStats: enginepb.MVCCPersistentStats{
				ValBytes: valBytes, LiveBytes: valBytes,
			},
		}
		var v roachpb.Value
		require.NoError(t, v.SetProto(&ras))
		return v.RawBytes
	}

	makeLegacyValue := func(valBytes int64) []byte {
		var v roachpb.Value
		require.NoError(t, v.SetProto(&enginepb.MVCCStats{ValBytes: valBytes, LiveBytes: valBytes}))
		return v.RawBytes
	}

	v1 := makeRASValue(100)
	v2 := makeRASValue(9999)
	n1 := tsRangeReplicatedValueForChecksum(desc, rasKey, v1)
	n2 := tsRangeReplicatedValueForChecksum(desc, rasKey, v2)
	require.Equal(t, n1, n2)
	require.NotEqual(t, v1, v2)

	l1 := makeLegacyValue(100)
	l2 := makeLegacyValue(9999)
	require.Equal(t,
		tsRangeReplicatedValueForChecksum(desc, legacyKey, l1),
		tsRangeReplicatedValueForChecksum(desc, legacyKey, l2),
	)

	// HashNum without RangeType still normalizes (older TS descriptors).
	hashOnlyDesc := &roachpb.RangeDescriptor{RangeID: desc.RangeID, HashNum: 200}
	require.Equal(t, n1, tsRangeReplicatedValueForChecksum(hashOnlyDesc, rasKey, v1))

	// Non-TS ranges and other keys pass through unchanged.
	regularDesc := roachpb.RangeDescriptor{RangeID: desc.RangeID}
	require.Equal(t, v1, tsRangeReplicatedValueForChecksum(&regularDesc, rasKey, v1))
	require.Equal(t, v1, tsRangeReplicatedValueForChecksum(desc, roachpb.Key("other"), v1))
}

func TestReplicaStateEqualForAssert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := testTSRangeDescriptor(77, 200, 600)
	disk := storagepb.ReplicaState{
		Stats: &enginepb.MVCCStats{ValBytes: 431638806, LiveBytes: 431638806, KeyBytes: 10},
	}
	mem := storagepb.ReplicaState{
		Stats: &enginepb.MVCCStats{ValBytes: 432626982, LiveBytes: 432626982, KeyBytes: 10},
	}
	require.True(t, replicaStateEqualForAssert(desc, disk, mem))

	regularDesc := &roachpb.RangeDescriptor{RangeID: 77}
	require.False(t, replicaStateEqualForAssert(regularDesc, disk, mem))
}

func TestReconcileTSRangeStatsInMemoryLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	withTSRangeDataVolumeHook(t, func(desc *roachpb.RangeDescriptor) (uint64, error) {
		return 5000, nil
	})

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	desc := testTSRangeDescriptor(tc.repl.RangeID, 0, 10)
	tc.repl.mu.Lock()
	tc.repl.mu.state.Desc = desc
	tc.repl.mu.state.Stats = &enginepb.MVCCStats{ValBytes: 100, LiveBytes: 100}
	tc.repl.mu.Unlock()

	diskStatsBefore, err := stateloader.Make(tc.repl.RangeID).LoadMVCCStats(ctx, tc.engine)
	require.NoError(t, err)

	tc.repl.reconcileTSRangeStatsForSnapshot(ctx)

	tc.repl.mu.RLock()
	memStats := *tc.repl.mu.state.Stats
	tc.repl.mu.RUnlock()

	diskStatsAfter, err := stateloader.Make(tc.repl.RangeID).LoadMVCCStats(ctx, tc.engine)
	require.NoError(t, err)
	require.Equal(t, diskStatsBefore, diskStatsAfter)
	require.Equal(t, int64(5000), memStats.ValBytes)
	require.Equal(t, int64(5000), memStats.LiveBytes)
}

func TestIsTSRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsType := roachpb.TS_RANGE
	require.True(t, isTSRangeDescriptor(&roachpb.RangeDescriptor{RangeType: &tsType}))
	require.True(t, isTSRangeDescriptor(&roachpb.RangeDescriptor{HashNum: 200}))
	require.False(t, isTSRangeDescriptor(&roachpb.RangeDescriptor{}))
	require.False(t, isTSRangeDescriptor(nil))
}
