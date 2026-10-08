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
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func declareKeysTransferLease(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	// Cover the entire addressable key space with a latch to prevent any writes
	// from overlapping with lease transfers. In principle we could just use the
	// current range descriptor (desc) but it could potentially change due to an
	// as of yet unapplied merge.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.LocalMax, EndKey: keys.MaxKey})
}

func init() {
	RegisterReadWriteCommand(roachpb.TransferLease, declareKeysTransferLease, TransferLease)
}

// TransferLease sets the lease holder for the range.
// Unlike with RequestLease(), the new lease is allowed to overlap the old one,
// the contract being that the transfer must have been initiated by the (soon
// ex-) lease holder which must have dropped all of its lease holder powers
// before proposing.
func TransferLease(
	ctx context.Context, readWriter relational.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	args := cArgs.Args.(*roachpb.TransferLeaseRequest)

	// For now, don't allow replicas of type LEARNER to be leaseholders. There's
	// no reason this wouldn't work in principle, but it seems inadvisable. In
	// particular, learners can't become raft leaders, so we wouldn't be able to
	// co-locate the leaseholder + raft leader, which is going to affect tail
	// latencies. Additionally, as of the time of writing, learner replicas are
	// only used for a short time in replica addition, so it's not worth working
	// out the edge cases. If we decide to start using long-lived learners at some
	// point, that math may change.
	//
	// If this check is removed at some point, the filtering of learners on the
	// sending side would have to be removed as well.
	if err := checkCanReceiveLease(&args.Lease, cArgs.EvalCtx); err != nil {
		return newFailedLeaseTrigger(true /* isTransfer */), err
	}

	prevLease, _ := cArgs.EvalCtx.GetLease()
	log.VEventf(ctx, 2, "lease transfer: prev lease: %+v, new lease: %+v", prevLease, args.Lease)
	return evalNewLease(ctx, cArgs.EvalCtx, readWriter, cArgs.Stats,
		args.Lease, prevLease, false /* isExtension */, true /* isTransfer */)
}
