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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

var backpressureLogLimiter = log.Every(500 * time.Millisecond)

// backpressureRangeSizeMultiplier is the multiple of range_max_bytes that a
// range's size must grow to before backpressure will be applied on writes. Set
// to 0 to disable backpressure altogether.
var backpressureRangeSizeMultiplier = settings.RegisterValidatedFloatSetting(
	"kv.range.backpressure_range_size_multiplier",
	"multiple of range_max_bytes that a range is allowed to grow to without "+
		"splitting before writes to that range are blocked, or 0 to disable",
	2.0,
	func(v float64) error {
		if v != 0 && v < 1 {
			return errors.Errorf("backpressure multiplier cannot be smaller than 1: %f", v)
		}
		return nil
	},
)

// backpressurableSpans contains spans of keys where write backpressuring
// is permitted. Writes to any keys within these spans may cause a batch
// to be backpressured.
var backpressurableSpans = []roachpb.Span{
	{Key: keys.TimeseriesPrefix, EndKey: keys.TimeseriesKeyMax},
	// Backpressure from the end of the system config forward instead of
	// over all table data to avoid backpressuring unsplittable ranges.
	{Key: keys.SystemConfigTableDataMax, EndKey: keys.TableDataMax},
}

// canBackpressureBatch returns whether the provided BatchRequest is eligible
// for backpressure.
func canBackpressureBatch(ba *roachpb.BatchRequest) bool {
	// Don't backpressure splits themselves.
	if ba.Txn != nil && ba.Txn.Name == splitTxnName {
		return false
	}

	// Only backpressure batches containing a "backpressurable"
	// method that is within a "backpressurable" key span.
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		if !roachpb.CanBackpressure(req) {
			continue
		}

		for _, s := range backpressurableSpans {
			if s.Contains(req.Header().Span()) {
				return true
			}
		}
	}
	return false
}

// shouldBackpressureWrites returns whether writes to the range should be
// subject to backpressure. This is based on the size of the range in
// relation to the split size. The method returns true if the range is more
// than backpressureRangeSizeMultiplier times larger than the split size.
func (r *Replica) shouldBackpressureWrites() bool {
	// history ts range will never split.
	if r.Desc().GetRangeType() == roachpb.TS_RANGE {
		return false
	}
	mult := backpressureRangeSizeMultiplier.Get(&r.store.cfg.Settings.SV)
	if mult == 0 {
		// Disabled.
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.exceedsMultipleOfSplitSizeRLocked(mult)
}

// maybeBackpressureBatch blocks to apply backpressure if the replica deems
// that backpressure is necessary.
func (r *Replica) maybeBackpressureBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if !canBackpressureBatch(ba) {
		return nil
	}

	// If we need to apply backpressure, wait for an ongoing split to finish
	// if one exists. This does not place a hard upper bound on the size of
	// a range because we don't track all in-flight requests (like we do for
	// the quota pool), but it does create an effective soft upper bound.
	for first := true; r.shouldBackpressureWrites(); first = false {
		if first {
			r.store.metrics.BackpressuredOnSplitRequests.Inc(1)
			defer r.store.metrics.BackpressuredOnSplitRequests.Dec(1)

			if backpressureLogLimiter.ShouldLog() {
				log.Warningf(ctx, "applying backpressure to limit range growth on batch %s", ba)
			}
		}

		// Register a callback on an ongoing split for this range in the splitQueue.
		splitC := make(chan error, 1)
		if !r.store.splitQueue.MaybeAddCallback(r.RangeID, func(err error) {
			splitC <- err
		}) {
			// No split ongoing. We may have raced with its completion. There's
			// no good way to prevent this race, so we conservatively allow the
			// request to proceed instead of throwing an error that would surface
			// to the client.
			return nil
		}

		// Wait for the callback to be called.
		select {
		case <-ctx.Done():
			return errors.Wrapf(
				ctx.Err(), "aborted while applying backpressure to %s on range %s", ba, r.Desc(),
			)
		case err := <-splitC:
			if err != nil {
				return errors.Wrapf(
					err, "split failed while applying backpressure to %s on range %s", ba, r.Desc(),
				)
			}
		}
	}
	return nil
}
