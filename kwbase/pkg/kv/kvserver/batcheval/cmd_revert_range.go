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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.RevertRange, declareKeysRevertRange, RevertRange)
}

func declareKeysRevertRange(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareIsolatedKeys(desc, header, req, latchSpans, lockSpans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(desc.RangeID)})
}

// isEmptyKeyTimeRange checks if the span has no writes in (since,until].
func isEmptyKeyTimeRange(
	readWriter relational.ReadWriter, from, to roachpb.Key, since, until hlc.Timestamp,
) (bool, error) {
	// Use a TBI to check if there is anything to delete -- the first key Seek hits
	// may not be in the time range but the fact the TBI found any key indicates
	// that there is *a* key in the SST that is in the time range. Thus we should
	// proceed to iteration that actually checks timestamps on each key.
	iter := readWriter.NewIterator(relational.IterOptions{
		LowerBound: from, UpperBound: to,
		MinTimestampHint: since.Next() /* make exclusive */, MaxTimestampHint: until,
	})
	defer iter.Close()
	iter.SeekGE(relational.MVCCKey{Key: from})
	ok, err := iter.Valid()
	return !ok, err
}

// RevertRange wipes all MVCC versions more recent than TargetTime (up to the
// command timestamp) of the keys covered by the specified span, adjusting the
// MVCC stats accordingly.
//
// Note: this should only be used when there is no user traffic writing to the
// target span at or above the target time.
func RevertRange(
	ctx context.Context, readWriter relational.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute RevertRange within a transaction")
	}
	log.VEventf(ctx, 2, "RevertRange %+v", cArgs.Args)

	args := cArgs.Args.(*roachpb.RevertRangeRequest)
	reply := resp.(*roachpb.RevertRangeResponse)
	var pd result.Result

	if gc := cArgs.EvalCtx.GetGCThreshold(); args.TargetTime.LessEq(gc) {
		return result.Result{}, errors.Errorf("cannot revert before replica GC threshold %v", gc)
	}

	if empty, err := isEmptyKeyTimeRange(
		readWriter, args.Key, args.EndKey, args.TargetTime, cArgs.Header.Timestamp,
	); err != nil {
		return result.Result{}, err
	} else if empty {
		log.VEventf(ctx, 2, "no keys to clear in specified time range")
		return result.Result{}, nil
	}

	log.VEventf(ctx, 2, "clearing keys with timestamp (%v, %v]", args.TargetTime, cArgs.Header.Timestamp)

	resume, err := relational.MVCCClearTimeRange(ctx, readWriter, cArgs.Stats, args.Key, args.EndKey,
		args.TargetTime, cArgs.Header.Timestamp, cArgs.Header.MaxSpanRequestKeys)
	if err != nil {
		return result.Result{}, err
	}

	if resume != nil {
		log.VEventf(ctx, 2, "hit limit while clearing keys, resume span [%v, %v)", resume.Key, resume.EndKey)
		reply.ResumeSpan = resume

		// If, and only if, we're returning a resume span do we want to return >0
		// NumKeys. Distsender will reduce the limit for subsequent requests by the
		// amount returned, but that doesn't really make sense for RevertRange:
		// there isn't some combined result set size we're trying to hit across many
		// requests; just because some earlier range ran X Clears that does not mean
		// we want the next range to run fewer than the limit chosen for the batch
		// size reasons. On the otherhand, we have to pass MaxKeys though if we
		// return a resume span to cause distsender to stop after this request, as
		// currently response combining's handling of resume spans prefers that
		// there only be one. Thus we just set it to MaxKeys when, and only when,
		// we're returning a ResumeSpan.
		reply.NumKeys = cArgs.Header.MaxSpanRequestKeys
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	return pd, nil
}
