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

package kvcoord

import (
	"context"
	"fmt"
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

type singleRangeInfo struct {
	desc  *roachpb.RangeDescriptor
	rs    roachpb.RSpan
	ts    hlc.Timestamp
	token *EvictionToken
}

// RangeFeed divides a RangeFeed request on range boundaries and establishes a
// RangeFeed to each of the individual ranges. It streams back results on the
// provided channel.
//
// Note that the timestamps in RangeFeedCheckpoint events that are streamed back
// may be lower than the timestamp given here.
func (ds *DistSender) RangeFeed(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	withDiff bool,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	startRKey, err := keys.Addr(span.Key)
	if err != nil {
		return err
	}
	endRKey, err := keys.Addr(span.EndKey)
	if err != nil {
		return err
	}
	rs := roachpb.RSpan{Key: startRKey, EndKey: endRKey}

	g := ctxgroup.WithContext(ctx)
	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	rangeCh := make(chan singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Spawn a child goroutine to process this feed.
				g.GoCtx(func(ctx context.Context) error {
					return ds.partialRangeFeed(ctx, &sri, withDiff, rangeCh, eventCh)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial set of ranges.
	g.GoCtx(func(ctx context.Context) error {
		return ds.divideAndSendRangeFeedToRanges(ctx, rs, ts, rangeCh, nil)
	})

	return g.Wait()
}

func (ds *DistSender) divideAndSendRangeFeedToRanges(
	ctx context.Context,
	rs roachpb.RSpan,
	ts hlc.Timestamp,
	rangeCh chan<- singleRangeInfo,
	rangeInfo *singleRangeInfo,
) error {
	// As RangeIterator iterates, it can return overlapping descriptors (and
	// during splits, this happens frequently), but divideAndSendRangeFeedToRanges
	// intends to split up the input into non-overlapping spans aligned to range
	// boundaries. So, as we go, keep track of the remaining uncovered part of
	// `rs` in `nextRS`.
	nextRS := rs
	ri := NewRangeIterator(ds)
	for ri.Seek(ctx, nextRS.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		partialRS, err := nextRS.Intersect(desc)
		if err != nil {
			return err
		}

		nextRS.Key = partialRS.EndKey
		select {
		case rangeCh <- singleRangeInfo{
			desc:  desc,
			rs:    partialRS,
			ts:    ts,
			token: ri.Token(),
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ri.NeedAnother(nextRS) {
			break
		}
	}
	return ri.Error()
}

// partialRangeFeed establishes a RangeFeed to the range specified by desc. It
// manages lifecycle events of the range in order to maintain the RangeFeed
// connection; this may involve instructing higher-level functions to retry
// this rangefeed, or subdividing the range further in the event of a split.
func (ds *DistSender) partialRangeFeed(
	ctx context.Context,
	rangeInfo *singleRangeInfo,
	withDiff bool,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rangeInfo.rs.AsRawSpanWithNoLocals()
	ts := rangeInfo.ts

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on a send failure, re-lookup.
		if rangeInfo.desc == nil {
			var err error
			rangeInfo.desc, rangeInfo.token, err = ds.getDescriptor(ctx, rangeInfo.rs.Key, nil, false)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				continue
			}
		}

		// Establish a RangeFeed for a single Range.
		maxTS, pErr := ds.singleRangeFeed(ctx, span, ts, withDiff, rangeInfo.desc, eventCh)

		// Forward the timestamp in case we end up sending it again.
		ts.Forward(maxTS)

		if pErr != nil {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(ts.GoTime()), pErr)
			}
			switch t := pErr.GetDetail().(type) {
			case *roachpb.StoreNotFoundError, *roachpb.NodeUnavailableError:
				// These errors are likely to be unique to the replica that
				// reported them, so no action is required before the next
				// retry.
			case *roachpb.SendError, *roachpb.RangeNotFoundError:
				// Evict the decriptor from the cache and reload on next attempt.
				if err := rangeInfo.token.Evict(ctx); err != nil {
					return err
				}
				rangeInfo.desc = nil
				continue
			case *roachpb.RangeKeyMismatchError:
				// Evict the decriptor from the cache.
				if err := rangeInfo.token.Evict(ctx); err != nil {
					return err
				}
				return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, ts, rangeCh, rangeInfo)
			case *roachpb.RangeFeedRetryError:
				switch t.Reason {
				case roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
					roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
					roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
					roachpb.RangeFeedRetryError_REASON_SLOW_CONSUMER:
					// Try again with same descriptor. These are transient
					// errors that should not show up again.
					continue
				case roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
					roachpb.RangeFeedRetryError_REASON_RANGE_MERGED:
					// Evict the decriptor from the cache.
					if err := rangeInfo.token.Evict(ctx); err != nil {
						return err
					}
					return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, ts, rangeCh, rangeInfo)
				default:
					log.Fatalf(ctx, "unexpected RangeFeedRetryError reason %v", t.Reason)
				}
			default:
				return t
			}
		}
	}
	return nil
}

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be send on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	withDiff bool,
	desc *roachpb.RangeDescriptor,
	eventCh chan<- *roachpb.RangeFeedEvent,
) (hlc.Timestamp, *roachpb.Error) {
	args := roachpb.RangeFeedRequest{
		Span: span,
		Header: roachpb.Header{
			Timestamp: ts,
			RangeID:   desc.RangeID,
		},
		WithDiff: withDiff,
	}

	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	// Learner replicas won't serve reads/writes, so send only to the `Voters`
	// replicas. This is just an optimization to save a network hop, everything
	// would still work if we had `All` here.
	replicas := NewReplicaSlice(ds.gossip, desc.Replicas().Voters())
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)
	// The RangeFeed is not used for system critical traffic so use a DefaultClass
	// connection regardless of the range.
	opts := SendOptions{class: rpc.DefaultClass}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return args.Timestamp, roachpb.NewError(err)
	}

	for {
		if transport.IsExhausted() {
			return args.Timestamp, roachpb.NewError(roachpb.NewSendError(
				fmt.Sprintf("sending to all %d replicas failed", len(replicas)),
			))
		}

		args.Replica = transport.NextReplica()
		clientCtx, client, err := transport.NextInternalClient(ctx)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}

		stream, err := client.RangeFeed(clientCtx, &args)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return args.Timestamp, nil
			}
			if err != nil {
				return args.Timestamp, roachpb.NewError(err)
			}
			switch t := event.GetValue().(type) {
			case *roachpb.RangeFeedCheckpoint:
				if t.Span.Contains(args.Span) {
					args.Timestamp.Forward(t.ResolvedTS)
				}
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				return args.Timestamp, &t.Error
			}
			select {
			case eventCh <- event:
			case <-ctx.Done():
				return args.Timestamp, roachpb.NewError(ctx.Err())
			}
		}
	}
}
