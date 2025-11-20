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
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

func init() {
	RegisterReadOnlyCommand(roachpb.RecomputeStats, declareKeysRecomputeStats, RecomputeStats)
}

func declareKeysRecomputeStats(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	// We don't declare any user key in the range. This is OK since all we're doing is computing a
	// stats delta, and applying this delta commutes with other operations on the same key space.
	//
	// But we want two additional properties:
	// 1) prevent interleaving with splits, and
	// 2) prevent interleaving between different incarnations of `RecomputeStats`.
	//
	// This is achieved by declaring
	// 1) a read on the range descriptor key (thus blocking splits) and
	// 2) a write on a transaction anchored at the range descriptor key (thus blocking any other
	// incarnation of RecomputeStats).
	//
	// Note that we're also accessing the range stats key, but we don't declare it for the same
	// reasons as above.
	rdKey := keys.RangeDescriptorKey(desc.StartKey)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: rdKey})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rdKey, uuid.Nil)})
}

// RecomputeStats recomputes the MVCCStats stored for this range and adjust them accordingly,
// returning the MVCCStats delta obtained in the process.
func RecomputeStats(
	ctx context.Context, _ storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	desc := cArgs.EvalCtx.Desc()
	args := cArgs.Args.(*roachpb.RecomputeStatsRequest)
	if !desc.StartKey.AsRawKey().Equal(args.Key) {
		return result.Result{}, errors.New("descriptor mismatch; range likely merged")
	}
	dryRun := args.DryRun

	args = nil // avoid accidental use below

	// Open a snapshot from which we will read everything (including the
	// MVCCStats). This is necessary because a batch does not provide us
	// with a consistent view of the data -- reading from the batch, we
	// could see skew between the stats recomputation and the MVCCStats
	// we read from the range state if concurrent writes are inflight[1].
	//
	// Note that in doing so, we also circumvent the assertions (present in both
	// the EvalContext and the batch in some builds) which check that all reads
	// were previously declared. See the comment in `declareKeysRecomputeStats`
	// for details on this.
	//
	// [1]: see engine.TestBatchReadLaterWrite.
	snap := cArgs.EvalCtx.Engine().NewSnapshot()
	defer snap.Close()
	// as for ts range, ComputeStatsForRange does nothing
	actualMS, err := rditer.ComputeStatsForRange(desc, snap, cArgs.Header.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}

	currentStats, err := MakeStateLoader(cArgs.EvalCtx).LoadMVCCStats(ctx, snap)
	if err != nil {
		return result.Result{}, err
	}

	delta := actualMS
	delta.Subtract(currentStats)

	if desc.GetRangeType() == roachpb.TS_RANGE && cArgs.EvalCtx.TsEngine() != nil && !cArgs.EvalCtx.TsEngine().IsSingleNode() {
		// call GetDataVolume to re compute rangeSize for ts range.
		// GetDataVolume for relational range may cause err.
		hashNum := desc.HashNum
		if hashNum == 0 {
			hashNum = api.HashParamV2
		}
		startTableID, startHashPoint, err1 := sqlbase.DecodeTsRangeKey(desc.StartKey, true, hashNum)
		_, EndHashPoint, err2 := sqlbase.DecodeTsRangeKey(desc.EndKey, false, hashNum)

		if err1 != nil || err2 != nil {
			delta = enginepb.MVCCStats{}
		} else {
			rangeSize, err := cArgs.EvalCtx.TsEngine().GetDataVolume(
				startTableID,
				startHashPoint,
				EndHashPoint,
				math.MinInt64,
				math.MaxInt64,
			)
			if err == nil {
				delta = enginepb.MVCCStats{
					ValBytes:     int64(rangeSize),
					LiveBytes:    int64(rangeSize),
					TsPerRowSize: actualMS.TsPerRowSize,
				}
				delta.Subtract(currentStats)
			}
		}
	}

	if !dryRun {
		// TODO(tschottdorf): do we not want to run at all if we have estimates in
		// this range? I think we want to as this would give us much more realistic
		// stats for timeseries ranges (which go cold and the approximate stats are
		// wildly overcounting) and this is paced by the consistency checker, but it
		// means some extra engine churn.
		if !cArgs.EvalCtx.ClusterSettings().Version.IsActive(ctx, clusterversion.VersionContainsEstimatesCounter) {
			// We are running with the older version of MVCCStats.ContainsEstimates
			// which was a boolean, so we should keep it in {0,1} and not reset it
			// to avoid racing with another command that sets it to true.
			delta.ContainsEstimates = currentStats.ContainsEstimates
		}
		cArgs.Stats.Add(delta)
	}

	resp.(*roachpb.RecomputeStatsResponse).AddedDelta = enginepb.MVCCStatsDelta(delta)
	return result.Result{}, nil
}
