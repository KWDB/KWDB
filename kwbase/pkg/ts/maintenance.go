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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

// ContainsTimeSeries returns true if the given key range overlaps the
// range of possible time series keys.
func (tsdb *DB) ContainsTimeSeries(start, end roachpb.RKey) bool {
	return !lastTSRKey.Less(start) && !end.Less(firstTSRKey)
}

// MaintainTimeSeries provides a function that can be called from an external
// process periodically in order to perform "maintenance" work on time series
// data. Currently, this includes computing rollups and pruning data which has
// exceeded its retention threshold, as well as computing low-resolution rollups
// of data. This system was designed specifically to be used by scanner queue
// from the storage package.
//
// The snapshot should be supplied by a local store, and is used only to
// discover the names of time series which are store in that snapshot. The KV
// client is then used to interact with data from the time series that are
// discovered; this may result in data being deleted, but may also write new
// data in the form of rollups.
//
// The snapshot is used for key discovery (as opposed to the KV client) because
// the task of pruning time series is distributed across the cluster to the
// individual ranges which contain that time series data. Because replicas of
// those ranges are guaranteed to have time series data locally, we can use the
// snapshot to quickly obtain a set of keys to be pruned with no network calls.
func (tsdb *DB) MaintainTimeSeries(
	ctx context.Context,
	snapshot storage.Reader,
	start, end roachpb.RKey,
	db *kv.DB,
	mem *mon.BytesMonitor,
	budgetBytes int64,
	now hlc.Timestamp,
) error {
	series, err := tsdb.findTimeSeries(snapshot, start, end, now)
	if err != nil {
		return err
	}
	if tsdb.WriteRollups() {
		qmc := MakeQueryMemoryContext(mem, mem, QueryMemoryOptions{
			BudgetBytes: budgetBytes,
		})
		if err := tsdb.rollupTimeSeries(ctx, series, now, qmc); err != nil {
			return err
		}
	}
	return tsdb.pruneTimeSeries(ctx, db, series, now)
}

// Assert that DB implements the necessary interface from the storage package.
var _ kvserver.TimeSeriesDataStore = (*DB)(nil)
