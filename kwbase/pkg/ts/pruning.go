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

package ts

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

var (
	firstTSRKey = roachpb.RKey(keys.TimeseriesPrefix)
	lastTSRKey  = firstTSRKey.PrefixEnd()
)

type timeSeriesResolutionInfo struct {
	Name       string
	Resolution Resolution
}

// findTimeSeries searches the supplied engine over the supplied key range,
// identifying time series which have stored data in the range, along with the
// resolutions at which time series data is stored. A unique name/resolution
// pair will only be identified once, even if the range contains keys for that
// name/resolution pair at multiple timestamps or from multiple sources.
//
// An engine snapshot is used, rather than a client, because this function is
// intended to be called by a storage queue which can inspect the local data for
// a single range without the need for expensive network calls.
func (tsdb *DB) findTimeSeries(
	snapshot storage.Reader, startKey, endKey roachpb.RKey, now hlc.Timestamp,
) ([]timeSeriesResolutionInfo, error) {
	var results []timeSeriesResolutionInfo

	// Set start boundary for the search, which is the lesser of the range start
	// key and the beginning of time series data.
	start := storage.MakeMVCCMetadataKey(startKey.AsRawKey())
	next := storage.MakeMVCCMetadataKey(keys.TimeseriesPrefix)
	if next.Less(start) {
		next = start
	}

	// Set end boundary for the search, which is the lesser of the range end key
	// and the end of time series data.
	end := storage.MakeMVCCMetadataKey(endKey.AsRawKey())
	lastTS := storage.MakeMVCCMetadataKey(keys.TimeseriesPrefix.PrefixEnd())
	if lastTS.Less(end) {
		end = lastTS
	}

	thresholds := tsdb.computeThresholds(now.WallTime)

	iter := snapshot.NewIterator(storage.IterOptions{UpperBound: endKey.AsRawKey()})
	defer iter.Close()

	for iter.SeekGE(next); ; iter.SeekGE(next) {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok || !iter.UnsafeKey().Less(end) {
			break
		}
		foundKey := iter.Key().Key

		// Extract the name and resolution from the discovered key.
		name, _, res, tsNanos, err := DecodeDataKey(foundKey)
		if err != nil {
			return nil, err
		}
		// Skip this time series if there's nothing to prune. We check the
		// oldest (first) time series record's timestamp against the
		// pruning threshold.
		if threshold, ok := thresholds[res]; !ok || threshold > tsNanos {
			results = append(results, timeSeriesResolutionInfo{
				Name:       name,
				Resolution: res,
			})
		}

		// Set 'next' is initialized to the next possible time series key
		// which could belong to a previously undiscovered time series.
		next = storage.MakeMVCCMetadataKey(makeDataKeySeriesPrefix(name, res).PrefixEnd())
	}

	return results, nil
}

// pruneTimeSeries will prune data for the supplied set of time series. Time
// series series are identified by name and resolution.
//
// For each time series supplied, the pruning operation will delete all data
// older than a constant threshold. The threshold is different depending on the
// resolution; typically, lower-resolution time series data will be retained for
// a longer period.
//
// If data is stored at a resolution which is not known to the system, it is
// assumed that the resolution has been deprecated and all data for that time
// series at that resolution will be deleted.
//
// As range deletion of inline data is an idempotent operation, it is safe to
// run this operation concurrently on multiple nodes at the same time.
func (tsdb *DB) pruneTimeSeries(
	ctx context.Context, db *kv.DB, timeSeriesList []timeSeriesResolutionInfo, now hlc.Timestamp,
) error {
	thresholds := tsdb.computeThresholds(now.WallTime)

	b := &kv.Batch{}
	for _, timeSeries := range timeSeriesList {
		// Time series data for a specific resolution falls in a contiguous key
		// range, and can be deleted with a DelRange command.
		// The start key is the prefix unique to this name/resolution pair.
		start := makeDataKeySeriesPrefix(timeSeries.Name, timeSeries.Resolution)

		// The end key can be created by generating a time series key with the
		// threshold timestamp for the resolution. If the resolution is not
		// supported, the start key's PrefixEnd is used instead (which will clear
		// the time series entirely).
		var end roachpb.Key
		threshold, ok := thresholds[timeSeries.Resolution]
		if ok {
			end = MakeDataKey(timeSeries.Name, "", timeSeries.Resolution, threshold)
		} else {
			end = start.PrefixEnd()
		}

		b.AddRawRequest(&roachpb.DeleteRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    start,
				EndKey: end,
			},
			Inline: true,
		})
	}

	return db.Run(ctx, b)
}
