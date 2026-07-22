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
	"bytes"
	"context"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// tsRangeDataVolumeHook, when non-nil, replaces TsEngine GetDataVolume lookup.
// Used by unit tests in this package only.
var tsRangeDataVolumeHook func(desc *roachpb.RangeDescriptor) (uint64, error)

// tsRangeDataVolume returns the logical data volume for a TS_RANGE from TsEngine.
func tsRangeDataVolume(tsEngine *tse.TsEngine, desc *roachpb.RangeDescriptor) (uint64, error) {
	if fn := tsRangeDataVolumeHook; fn != nil {
		return fn(desc)
	}
	if tsEngine == nil {
		return 0, errors.New("TsEngine is nil")
	}
	hashNum := desc.HashNum
	if hashNum == 0 {
		hashNum = api.HashParamV2
	}
	startTableID, startHashPoint, err := sqlbase.DecodeTsRangeKey(desc.StartKey, true, hashNum)
	if err != nil {
		return 0, err
	}
	if desc.EndKey.Equal(roachpb.RKeyMax) {
		return tsEngine.GetDataVolume(
			startTableID,
			startHashPoint,
			startHashPoint,
			math.MinInt64,
			math.MaxInt64,
		)
	}
	endTableID, endHashPoint, err := sqlbase.DecodeTsRangeKey(desc.EndKey, false, hashNum)
	if err != nil {
		return 0, err
	}
	if endTableID > startTableID {
		endHashPoint = hashNum
	}
	return tsEngine.GetDataVolume(
		startTableID,
		startHashPoint,
		endHashPoint,
		math.MinInt64,
		math.MaxInt64,
	)
}

func recordTSRangeStatsDataVolumeFallback(
	ctx context.Context, sm *StoreMetrics, desc *roachpb.RangeDescriptor, err error,
) {
	if sm != nil && sm.TsRangeStatsDataVolumeFailures != nil {
		sm.TsRangeStatsDataVolumeFailures.Inc(1)
	}
	log.Warningf(ctx, "TS_RANGE r%d: GetDataVolume failed, using incremental MVCCStats: %v",
		desc.RangeID, err)
}

// correctTSRangeMVCCStatsFromDataVolume sets ValBytes and LiveBytes on stats from
// TsEngine.GetDataVolume. Returns true if stats were updated. On failure it
// leaves stats unchanged (incremental MVCCStats fallback), increments
// TsRangeStatsDataVolumeFailures, and logs a warning.
func correctTSRangeMVCCStatsFromDataVolume(
	ctx context.Context,
	sm *StoreMetrics,
	tsEngine *tse.TsEngine,
	desc *roachpb.RangeDescriptor,
	stats *enginepb.MVCCStats,
) bool {
	if desc == nil || stats == nil || !isTSRangeDescriptor(desc) {
		return false
	}
	if tsRangeDataVolumeHook == nil && (tsEngine == nil || tsEngine.IsSingleNode()) {
		return false
	}
	volume, err := tsRangeDataVolume(tsEngine, desc)
	if err != nil {
		recordTSRangeStatsDataVolumeFallback(ctx, sm, desc, err)
		return false
	}
	newValBytes := int64(volume)
	if stats.ValBytes == newValBytes && stats.LiveBytes == newValBytes {
		return false
	}
	stats.ValBytes = newValBytes
	stats.LiveBytes = newValBytes
	return true
}

// GetMVCCStatsForDecisions returns MVCC stats suitable for split, merge, and
// rebalance decisions. For TS_RANGE it refreshes ValBytes/LiveBytes from
// TsEngine before returning stats.
func (r *Replica) GetMVCCStatsForDecisions(ctx context.Context) enginepb.MVCCStats {
	if desc := r.Desc(); desc != nil && r.store != nil && isTSRangeDescriptor(desc) {
		r.reconcileTSRangeStatsForSnapshot(ctx)
	}
	return r.GetMVCCStats()
}

// reconcileTSRangeStatsForSnapshot corrects TS_RANGE MVCCStats from TsEngine in
// memory only. Replicated RangeAppliedState must not be updated outside Raft;
// sendTSSnapshot overlays in-memory stats onto the outgoing snapshot header.
//
// GetDataVolume is invoked without holding r.mu so concurrent reads and writes
// are not blocked by TsEngine scans.
func (r *Replica) reconcileTSRangeStatsForSnapshot(ctx context.Context) {
	r.mu.RLock()
	store := r.store
	desc := r.mu.state.Desc
	if store == nil || desc == nil || !isTSRangeDescriptor(desc) || r.mu.state.Stats == nil {
		r.mu.RUnlock()
		return
	}
	before := *r.mu.state.Stats
	r.mu.RUnlock()

	stats := before
	if !correctTSRangeMVCCStatsFromDataVolume(ctx, store.metrics, store.TsEngine, desc, &stats) {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyTSRangeVolumeCorrectionLocked(stats.ValBytes, stats.LiveBytes)
}

// applyTSRangeVolumeCorrectionLocked updates ValBytes/LiveBytes on the current
// in-memory stats and store metrics. Requires r.mu held for writing.
func (r *Replica) applyTSRangeVolumeCorrectionLocked(valBytes, liveBytes int64) {
	if r.store == nil || r.mu.state.Stats == nil {
		return
	}
	before := *r.mu.state.Stats
	if before.ValBytes == valBytes && before.LiveBytes == liveBytes {
		return
	}
	updated := before
	updated.ValBytes = valBytes
	updated.LiveBytes = liveBytes
	r.store.metrics.subtractMVCCStats(before)
	r.store.metrics.addMVCCStats(updated)
	*r.mu.state.Stats = updated
}

// isTSRangeDescriptor reports whether desc is a TS hash range. KWDB may set
// HashNum without populating RangeType on older descriptors.
func isTSRangeDescriptor(desc *roachpb.RangeDescriptor) bool {
	if desc == nil {
		return false
	}
	if desc.GetRangeType() == roachpb.TS_RANGE {
		return true
	}
	return desc.HashNum > 0
}

// tsRangeReplicatedValueForChecksum returns MVCC value bytes used when hashing
// replicated stats keys for a TS_RANGE. ValBytes/LiveBytes may differ across
// replicas because TS volume is reconciled in memory only; zero them so
// consistency checks compare replicated state other than TS volume-derived stats.
func tsRangeReplicatedValueForChecksum(
	desc *roachpb.RangeDescriptor, key roachpb.Key, value []byte,
) []byte {
	if !isTSRangeDescriptor(desc) {
		return value
	}
	if bytes.Equal(key, keys.RangeAppliedStateKey(desc.RangeID)) {
		var v roachpb.Value
		v.RawBytes = value
		var ras enginepb.RangeAppliedState
		if err := v.GetProto(&ras); err != nil {
			return value
		}
		ras.RangeStats = enginepb.MVCCPersistentStats{}
		var out roachpb.Value
		if err := out.SetProto(&ras); err != nil {
			return value
		}
		return out.RawBytes
	}
	if bytes.Equal(key, keys.RangeStatsLegacyKey(desc.RangeID)) {
		var zero enginepb.MVCCStats
		var out roachpb.Value
		if err := out.SetProto(&zero); err != nil {
			return value
		}
		return out.RawBytes
	}
	return value
}

// totalBytesForQueueTimeout returns the byte count used to estimate queue
// processing timeout. For TS_RANGE replicas it refreshes MVCCStats from
// TsEngine before reading ValBytes so makeRateLimitedTimeoutFunc sees the
// current data volume on the first snapshot attempt. When GetDataVolume fails,
// the incremental MVCCStats already on the replica are used instead.
func totalBytesForQueueTimeout(ctx context.Context, r replicaInQueue) int64 {
	if repl, ok := r.(*Replica); ok {
		stats := repl.GetMVCCStatsForDecisions(ctx)
		return stats.KeyBytes + stats.ValBytes + stats.IntentBytes + stats.SysBytes
	}
	statsRepl, ok := r.(interface{ GetMVCCStats() enginepb.MVCCStats })
	if !ok {
		return 0
	}
	stats := statsRepl.GetMVCCStats()
	return stats.KeyBytes + stats.ValBytes + stats.IntentBytes + stats.SysBytes
}

// correctTSRangeStatsAfterSnapshot corrects TS_RANGE stats from local TsEngine
// data in the provided stats pointer. Does not update store metrics; prefer
// reconcileTSRangeStatsForSnapshot when updating replica in-memory stats.
func (r *Replica) correctTSRangeStatsAfterSnapshot(
	ctx context.Context, desc *roachpb.RangeDescriptor, stats *enginepb.MVCCStats,
) {
	if desc == nil || stats == nil || !isTSRangeDescriptor(desc) {
		return
	}
	corrected := *stats
	if !correctTSRangeMVCCStatsFromDataVolume(ctx, r.store.metrics, r.store.TsEngine, desc, &corrected) {
		return
	}
	*stats = corrected
}

// replicaStateEqualForAssert compares on-disk and in-memory replica state for
// assertStateLocked. TS range ValBytes/LiveBytes are maintained in memory from
// TsEngine and may differ from persisted RangeAppliedState stats.
func replicaStateEqualForAssert(
	desc *roachpb.RangeDescriptor, disk, mem storagepb.ReplicaState,
) bool {
	if isTSRangeDescriptor(desc) {
		disk = replicaStateForAssertCompare(disk)
		mem = replicaStateForAssertCompare(mem)
	}
	return disk.Equal(mem)
}

func replicaStateForAssertCompare(s storagepb.ReplicaState) storagepb.ReplicaState {
	if s.Stats == nil {
		return s
	}
	stats := *s.Stats
	stats.ValBytes = 0
	stats.LiveBytes = 0
	s.Stats = &stats
	return s
}
