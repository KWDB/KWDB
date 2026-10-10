// Copyright 2014 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.TruncateLog, declareKeysTruncateLog, TruncateLog)
}

func declareKeysTruncateLog(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateLegacyKey(header.RangeID)})
	prefix := keys.RaftLogPrefix(header.RangeID)
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func TruncateLog(
	ctx context.Context, readWriter relational.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.TruncateLogRequest)

	// After a merge, it's possible that this request was sent to the wrong
	// range based on the start key. This will cancel the request if this is not
	// the range specified in the request body.
	rangeID := cArgs.EvalCtx.GetRangeID()
	if rangeID != args.RangeID {
		log.Infof(ctx, "attempting to truncate raft logs for another range: r%d. Normally this is due to a merge and can be ignored.",
			args.RangeID)
		return result.Result{}, nil
	}
	log.Eventf(ctx, "start to truncate range %v raft log", rangeID)

	var legacyTruncatedState roachpb.RaftTruncatedState
	legacyKeyFound, err := relational.MVCCGetProto(
		ctx, readWriter, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
		hlc.Timestamp{}, &legacyTruncatedState, relational.MVCCGetOptions{},
	)
	if err != nil {
		return result.Result{}, err
	}

	firstIndex, err := cArgs.EvalCtx.GetFirstIndex()
	if err != nil {
		return result.Result{}, errors.Wrap(err, "getting first index")
	}
	// Have we already truncated this log? If so, just return without an error.
	// Note that there may in principle be followers whose Raft log is longer
	// than this node's, but to issue a truncation we also need the *term* for
	// the new truncated state, which we can't obtain if we don't have the log
	// entry ourselves.
	//
	// TODO(tbg): think about synthesizing a valid term. Can we use the next
	// existing entry's term?
	if firstIndex >= args.Index {
		if log.V(3) {
			log.Infof(ctx, "attempting to truncate previously truncated raft log. FirstIndex:%d, TruncateFrom:%d",
				firstIndex, args.Index)
		}
		return result.Result{}, nil
	}

	// args.Index is the first index to keep.
	term, err := cArgs.EvalCtx.GetTerm(args.Index - 1)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "getting term")
	}

	// Compute the number of bytes freed by this truncation. Note that this will
	// only make sense for the leaseholder as we base this off its own first
	// index (other replicas may have other first indexes assuming we're not
	// still using the legacy truncated state key). In principle, this could be
	// off either way, though in practice we don't expect followers to have
	// a first index smaller than the leaseholder's (see #34287), and most of
	// the time everyone's first index should be the same.
	start := keys.RaftLogKey(rangeID, firstIndex)
	end := keys.RaftLogKey(rangeID, args.Index)

	// Compute the stats delta that were to occur should the log entries be
	// purged. We do this as a side effect of seeing a new TruncatedState,
	// downstream of Raft.
	//
	// Note that any sideloaded payloads that may be removed by this truncation
	// don't matter; they're not tracked in the raft log delta.
	//
	// TODO(tbg): it's difficult to prove that this computation doesn't have
	// bugs that let it diverge. It might be easier to compute the stats
	// from scratch, stopping when 4mb (defaultRaftLogTruncationThreshold)
	// is reached as at that point we'll truncate aggressively anyway.
	iter := readWriter.NewIterator(relational.IterOptions{UpperBound: end})
	defer iter.Close()
	// We can pass zero as nowNanos because we're only interested in SysBytes.
	ms, err := iter.ComputeStats(start, end, 0 /* nowNanos */)
	log.Eventf(ctx, "TruncateLog ComputeStats mvcc stats: %+v", ms)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "while computing stats of Raft log freed by truncation")
	}
	ms.SysBytes = -ms.SysBytes // simulate the deletion

	tState := &roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}

	var pd result.Result
	pd.Replicated.State = &storagepb.ReplicaState{
		TruncatedState: tState,
	}

	pd.Replicated.RaftLogDelta = ms.SysBytes

	if legacyKeyFound {
		// Time to migrate by deleting the legacy key. The downstream-of-Raft
		// code will atomically rewrite the truncated state (supplied via the
		// side effect) into the new unreplicated key.
		if err := relational.MVCCDelete(
			ctx, readWriter, cArgs.Stats, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
			hlc.Timestamp{}, nil, /* txn */
		); err != nil {
			return result.Result{}, err
		}
	}

	return pd, nil
}
