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
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// splitQueueTimerDuration is the duration between splits of queued ranges.
	splitQueueTimerDuration = 0 // zero duration to process splits greedily.

	// splitQueuePurgatoryCheckInterval is the interval at which replicas in
	// purgatory make split attempts. Purgatory is used by the splitQueue to
	// store ranges that are large enough to require a split but are
	// unsplittable because they do not contain a suitable split key. Purgatory
	// prevents them from repeatedly attempting to split at an unbounded rate.
	splitQueuePurgatoryCheckInterval = 1 * time.Minute

	// splits should be relatively isolated, other than requiring expensive
	// RocksDB scans over part of the splitting range to recompute stats. We
	// allow a limitted number of splits to be processed at once.
	splitQueueConcurrency = 4
)

// splitQueue manages a queue of ranges slated to be split due to size
// or along intersecting zone config boundaries.
type splitQueue struct {
	*baseQueue
	db       *kv.DB
	purgChan <-chan time.Time

	// loadBasedCount counts the load-based splits performed by the queue.
	loadBasedCount telemetry.Counter
	mode           startMode
}

// newSplitQueue returns a new instance of splitQueue.
func newSplitQueue(store *Store, db *kv.DB, gossip *gossip.Gossip, mode startMode) *splitQueue {
	var purgChan <-chan time.Time
	if c := store.TestingKnobs().SplitQueuePurgatoryChan; c != nil {
		purgChan = c
	} else {
		purgTicker := time.NewTicker(splitQueuePurgatoryCheckInterval)
		purgChan = purgTicker.C
	}

	sq := &splitQueue{
		db:             db,
		purgChan:       purgChan,
		loadBasedCount: telemetry.GetCounter("kv.split.load"),
		mode:           mode,
	}
	sq.baseQueue = newBaseQueue(
		"split", sq, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			maxConcurrency:       splitQueueConcurrency,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.SplitQueueSuccesses,
			failures:             store.metrics.SplitQueueFailures,
			pending:              store.metrics.SplitQueuePending,
			processingNanos:      store.metrics.SplitQueueProcessingNanos,
			purgatory:            store.metrics.SplitQueuePurgatory,
		},
	)
	return sq
}

func shouldSplitRange(
	desc *roachpb.RangeDescriptor, ms enginepb.MVCCStats, maxBytes int64, sysCfg *config.SystemConfig,
) (shouldQ bool, priority float64) {
	if sysCfg.NeedsSplit(desc.StartKey, desc.EndKey) {
		// Set priority to 1 in the event the range is split by zone configs.
		priority = 1
		shouldQ = true
	}

	// Add priority based on the size of range compared to the max
	// size for the zone it's in.
	if ratio := float64(ms.Total()) / float64(maxBytes); ratio > 1 {
		priority += ratio
		shouldQ = true
	}

	return shouldQ, priority
}

// shouldQueue determines whether a range should be queued for
// splitting. This is true if the range is intersected by a zone config
// prefix or if the range's size in bytes exceeds the limit for the zone,
// or if the range has too much load on it.
func (sq *splitQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg *config.SystemConfig,
) (shouldQ bool, priority float64) {
	desc := repl.Desc()
	if desc.GetRangeType() == roachpb.TS_RANGE && (sq.mode == singleNode || sq.mode == singleReplica) {
		return false, 0
	}
	shouldQ, priority = shouldSplitRange(repl.Desc(), repl.GetMVCCStats(),
		repl.GetMaxBytes(), sysCfg)

	if !shouldQ && repl.SplitByLoadEnabled() {
		if splitKey := repl.loadBasedSplitter.MaybeSplitKey(timeutil.Now()); splitKey != nil {
			shouldQ, priority = true, 1.0 // default priority
		}
	}
	if shouldQ && desc.GetRangeType() == roachpb.TS_RANGE {
		// When the EndID and TableID of range are not equal
		// 1. If range is the last range, then its EndKey is /Max.
		//    At this time, endTableID, hashPoint, and endTimeStamp
		//    are math.MaxUint64, api.HashParamV2 - 1, math.MaxInt64.
		// 2. If range is not the last range, but it crosses TableID.
		//    For example, if the TableID of TsTable is 80, and then a
		//    new database is created, the new database is 81, and then
		//    a new table is created, while it's ID is 82, then the EndKey
		//    of the last Range of 80 is /Table/82, and it will appear
		//    that startTableID is 80 and endTableID is 81. In this case,
		//    hashPoint is api.HashParamV2 - 1
		// The hashPoint calculations in the above cases are all correct.

		// Note: The first Range of the created table will be directly
		// split after the SystemConfigTrigger is triggered,
		// and will not enter the shouldQueue
		startKey := desc.StartKey
		endKey := desc.EndKey
		hashNum := desc.HashNum
		if hashNum == 0 {
			hashNum = api.HashParamV2
		}
		tableID, startHashPoint, err := sqlbase.DecodeTsRangeKey(startKey, true, hashNum)
		if err != nil {
			return false, priority
		}
		// If the tableID of the range is 0, reset
		if desc.TableId == 0 {
			desc.TableId = uint32(tableID)
		}
		_, endHashPoint, err := sqlbase.DecodeTsRangeKey(endKey, false, hashNum)
		if err != nil {
			return false, priority
		}
		splitHashPoint := (startHashPoint + endHashPoint + 1) / 2
		sv := sq.store.ClusterSettings()
		splitInterval := splitRangeSettings.Get(&sv.SV)
		splitMode := splitModeSettings.Get(&sv.SV)
		if splitHashPoint-startHashPoint < uint64(splitInterval) && !splitMode {
			return false, priority
		}
	}

	return shouldQ, priority
}

// unsplittableRangeError indicates that a split attempt failed because a no
// suitable split key could be found.
type unsplittableRangeError struct{}

func (unsplittableRangeError) Error() string         { return "could not find valid split key" }
func (unsplittableRangeError) purgatoryErrorMarker() {}

var _ purgatoryError = unsplittableRangeError{}

// process synchronously invokes admin split for each proposed split key.
func (sq *splitQueue) process(ctx context.Context, r *Replica, sysCfg *config.SystemConfig) error {
	err := sq.processAttempt(ctx, r, sysCfg)
	switch errors.Cause(err).(type) {
	case nil:
	case *roachpb.ConditionFailedError:
		// ConditionFailedErrors are an expected outcome for range split
		// attempts because splits can race with other descriptor modifications.
		// On seeing a ConditionFailedError, don't return an error and enqueue
		// this replica again in case it still needs to be split.
		log.Infof(ctx, "split saw concurrent descriptor modification; maybe retrying")
		sq.MaybeAddAsync(ctx, r, sq.store.Clock().Now())
	default:
		return err
	}
	return nil
}

var splitModeSettings = settings.RegisterBoolSetting(
	"kv.kvserver.ts_split_by_timestamp.enabled",
	"if set to false, the time series range is split according to the hashpoint. Otherwise, it is split according to the hashpoint and timestamp.",
	false,
)

var splitRangeSettings = settings.RegisterNonNegativeIntSetting(
	"kv.kvserver.ts_split_interval",
	"the interval of range splitting",
	10,
)

func (sq *splitQueue) processAttempt(
	ctx context.Context, r *Replica, sysCfg *config.SystemConfig,
) error {
	desc := r.Desc()
	// First handle the case of splitting due to zone config maps.
	if splitKey := sysCfg.ComputeSplitKey(desc.StartKey, desc.EndKey); splitKey != nil {
		splitType, tableID := sysCfg.GetTsSplitType(splitKey)
		var tmpHashNum uint64
		if splitType == roachpb.TS_SPLIT {
			// DEFAULT_RANGE does not split through split queue
			if desc.GetRangeType() == roachpb.DEFAULT_RANGE {
				return nil
			}
			// splitting the range of the same table
			if desc.TableId != tableID {
				return nil
			}
			// TableID is 0, there may be an issue, return
			if tableID == 0 {
				return nil
			}
			if desc.HashNum == 0 {
				desc.HashNum = api.HashParamV2
			}
			tmpHashNum = desc.HashNum
		}
		if _, err := r.adminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: splitKey.AsRawKey(),
				},
				SplitType:      splitType,
				TableId:        tableID,
				HashNum:        tmpHashNum,
				SplitKey:       splitKey.AsRawKey(),
				ExpirationTime: hlc.Timestamp{},
				// Here, splitKey is calculated based on zone-related information, which means
				// that it is generated by creating a new table. The IsCreateTable flag is
				// marked to prevent the calculation of the time series rangeSIze during creation.
				IsCreateTsTable: splitType == roachpb.TS_SPLIT,
			},
			desc,
			false, /* delayable */
			"zone config",
		); err != nil {
			return errors.Wrapf(err, "unable to split %s at key %q", r, splitKey)
		}
		return nil
	}

	//if desc.GetRangeType() == roachpb.TS_RANGE {
	//	return nil
	//}

	// Next handle case of splitting due to size. Note that we don't perform
	// size-based splitting if maxBytes is 0 (happens in certain test
	// situations).
	size := r.GetMVCCStats().Total()
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && float64(size)/float64(maxBytes) > 1 {
		if desc.GetRangeType() == roachpb.DEFAULT_RANGE {
			_, err := r.adminSplitWithDescriptor(
				ctx,
				roachpb.AdminSplitRequest{},
				desc,
				false, /* delayable */
				fmt.Sprintf("%s above threshold size %s", humanizeutil.IBytes(size), humanizeutil.IBytes(maxBytes)),
			)
			return err
		} else if desc.GetRangeType() == roachpb.TS_RANGE {
			r.startKey()
			startKey := r.Desc().StartKey
			endKey := r.Desc().EndKey
			hashNum := r.Desc().HashNum
			if hashNum == 0 {
				hashNum = api.HashParamV2
			}
			startTableID, startHashPoint, err := sqlbase.DecodeTsRangeKey(startKey, true, hashNum)
			if err != nil {
				//fmt.Println("DecodeTsRangeKey StartKey failed", err)
				log.Errorf(ctx, "DecodeTsRangeKey StartKey failed", err)
			}
			// /Max endTableID = 0
			endTableID, endHashPoint, err := sqlbase.DecodeTsRangeKey(endKey, false, hashNum)
			if err != nil {
				log.Errorf(ctx, "DecodeTsRangeKey endKey failed", err)
			}
			var splitKey roachpb.Key
			sv := sq.store.ClusterSettings()

			splitInterval := splitRangeSettings.Get(&sv.SV)
			splitMode := splitModeSettings.Get(&sv.SV)

			if r.Desc().EndKey.Equal(roachpb.KeyMax) {
				splitHashPoint := (startHashPoint + endHashPoint + 1) / 2
				if splitHashPoint-startHashPoint < uint64(splitInterval) && !splitMode {
					return nil
				}
				splitKey = sqlbase.MakeTsHashPointKey(sqlbase.ID(startTableID), splitHashPoint, hashNum)
			} else if startTableID == endTableID && startHashPoint != endHashPoint && endHashPoint != 0 {
				splitHashPoint := (startHashPoint + endHashPoint + 1) / 2
				if splitHashPoint-startHashPoint < uint64(splitInterval) && !splitMode {
					return nil
				}
				splitKey = sqlbase.MakeTsHashPointKey(sqlbase.ID(startTableID), splitHashPoint, hashNum)
			} else {
				return nil
			}

			//理论上不会溢出，兼容
			tableID := uint32(startTableID)
			_, err = r.adminSplitWithDescriptor(
				ctx,
				roachpb.AdminSplitRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: splitKey,
					},
					TableId:   tableID, //由于为Split，所以TableID不变
					HashNum:   hashNum,
					SplitType: roachpb.TS_SPLIT,
					SplitKey:  splitKey,
				},
				desc,
				true, /* delayable */
				fmt.Sprintf("%s above threshold size %s", humanizeutil.IBytes(size), humanizeutil.IBytes(maxBytes)),
			)
			if err != nil {
				log.Errorf(ctx, "TsSplit Failed: ", r.RangeID, splitKey, err)
			}
			return err

		}

	}

	now := timeutil.Now()
	if splitByLoadKey := r.loadBasedSplitter.MaybeSplitKey(now); splitByLoadKey != nil {
		batchHandledQPS := r.QueriesPerSecond()
		raftAppliedQPS := r.WritesPerSecond()
		splitQPS := r.loadBasedSplitter.LastQPS(now)
		reason := fmt.Sprintf(
			"load at key %s (%.2f splitQPS, %.2f batches/sec, %.2f raft mutations/sec)",
			splitByLoadKey,
			splitQPS,
			batchHandledQPS,
			raftAppliedQPS,
		)
		if _, pErr := r.adminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: splitByLoadKey,
				},
				SplitKey: splitByLoadKey,
			},
			desc,
			false, /* delayable */
			reason,
		); pErr != nil {
			return errors.Wrapf(pErr, "unable to split %s at key %q", r, splitByLoadKey)
		}

		telemetry.Inc(sq.loadBasedCount)

		// Reset the splitter now that the bounds of the range changed.
		r.loadBasedSplitter.Reset()
		return nil
	}
	return nil
}

// timer returns interval between processing successive queued splits.
func (*splitQueue) timer(_ time.Duration) time.Duration {
	return splitQueueTimerDuration
}

// purgatoryChan returns the split queue's purgatory channel.
func (sq *splitQueue) purgatoryChan() <-chan time.Time {
	return sq.purgChan
}
