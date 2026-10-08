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
	"fmt"
	"sort"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/intentresolver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rangefeed"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/interval"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
var RangefeedEnabled = settings.RegisterPublicBoolSetting(
	"kv.rangefeed.enabled",
	"if set, rangefeed registration is enabled",
	true,
)

// RegisterState is rangefeed regisger state
type RegisterState int32

const (
	registerStateInit         RegisterState = 0 // not split or merge, or not register rangefeed
	registerStateDescConnect  RegisterState = 1 // now rangefeed desconnect, it will be register
	rigisterStateRigisterDone RegisterState = 2 // de register done
)

// lockedRangefeedStream is an implementation of rangefeed.Stream which provides
// support for concurrent calls to Send. Note that the default implementation of
// grpc.Stream is not safe for concurrent calls to Send.
type lockedRangefeedStream struct {
	wrapped roachpb.Internal_RangeFeedServer
	sendMu  syncutil.Mutex
}

func (s *lockedRangefeedStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedRangefeedStream) Send(e *roachpb.RangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(e)
}

// rangefeedTxnPusher is a shim around intentResolver that implements the
// rangefeed.TxnPusher interface.
type rangefeedTxnPusher struct {
	ir *intentresolver.IntentResolver
	r  *Replica
}

// PushTxns is part of the rangefeed.TxnPusher interface. It performs a
// high-priority push at the specified timestamp to each of the specified
// transactions.
func (tp *rangefeedTxnPusher) PushTxns(
	ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
) ([]*roachpb.Transaction, error) {
	pushTxnMap := make(map[uuid.UUID]*enginepb.TxnMeta, len(txns))
	for i := range txns {
		txn := &txns[i]
		pushTxnMap[txn.ID] = txn
	}

	h := roachpb.Header{
		Timestamp: ts,
		Txn: &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: enginepb.MaxTxnPriority,
			},
		},
	}

	pushedTxnMap, pErr := tp.ir.MaybePushTransactions(
		ctx, pushTxnMap, h, roachpb.PUSH_TIMESTAMP, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	pushedTxns := make([]*roachpb.Transaction, 0, len(pushedTxnMap))
	for _, txn := range pushedTxnMap {
		pushedTxns = append(pushedTxns, txn)
	}
	return pushedTxns, nil
}

// CleanupTxnIntentsAsync is part of the rangefeed.TxnPusher interface.
func (tp *rangefeedTxnPusher) CleanupTxnIntentsAsync(
	ctx context.Context, txns []*roachpb.Transaction,
) error {
	endTxns := make([]result.EndTxnIntents, len(txns))
	for i, txn := range txns {
		endTxns[i].Txn = txn
		endTxns[i].Poison = true
	}
	return tp.ir.CleanupTxnIntentsAsync(ctx, tp.r.RangeID, endTxns, true /* allowSyncProcessing */)
}

type iteratorWithCloser struct {
	relational.SimpleIterator
	close func()
}

func (i iteratorWithCloser) Close() {
	i.SimpleIterator.Close()
	i.close()
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete. The provided ConcurrentRequestLimiter is used to limit the number
// of rangefeeds using catchup iterators at the same time.
func (r *Replica) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	if !RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		return roachpb.NewErrorf("rangefeeds require the kv.rangefeed.enabled setting. See " +
			base.DocsURL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
	}
	ctx := r.AnnotateCtx(stream.Context())

	var rSpan roachpb.RSpan
	var err error
	rSpan.Key, err = keys.Addr(args.Span.Key)
	if err != nil {
		return roachpb.NewError(err)
	}
	rSpan.EndKey, err = keys.Addr(args.Span.EndKey)
	if err != nil {
		return roachpb.NewError(err)
	}

	if err := r.ensureClosedTimestampStarted(ctx); err != nil {
		return err
	}

	// If the RangeFeed is performing a catch-up scan then it will observe all
	// values above args.Timestamp. If the RangeFeed is requesting previous
	// values for every update then it will also need to look for the version
	// proceeding each value observed during the catch-up scan timestamp. This
	// means that the earliest value observed by the catch-up scan will be
	// args.Timestamp.Next and the earliest timestamp used to retrieve the
	// previous version of a value will be args.Timestamp, so this is the
	// timestamp we must check against the GCThreshold.
	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		// If no timestamp was provided then we're not going to run a catch-up
		// scan, so make sure the GCThreshold in requestCanProceed succeeds.
		checkTS = r.Clock().Now()
	}

	lockedStream := &lockedRangefeedStream{wrapped: stream}
	errC := make(chan *roachpb.Error, 1)

	// If we will be using a catch-up iterator, wait for the limiter here before
	// locking raftMu.
	usingCatchupIter := false
	var iterSemRelease func()
	if !args.Timestamp.IsEmpty() {
		usingCatchupIter = true
		lim := &r.store.limiters.ConcurrentRangefeedIters
		if err := lim.Begin(ctx); err != nil {
			return roachpb.NewError(err)
		}
		// Finish the iterator limit, but only if we exit before
		// creating the iterator itself.
		iterSemRelease = lim.Finish
		defer func() {
			if iterSemRelease != nil {
				iterSemRelease()
			}
		}()
	}

	// Lock the raftMu, then register the stream as a new rangefeed registration.
	// raftMu is held so that the catch-up iterator is captured in the same
	// critical-section as the registration is established. This ensures that
	// the registration doesn't miss any events.
	r.raftMu.Lock()
	if err := r.checkExecutionCanProceedForRangeFeed(rSpan, checkTS); err != nil {
		r.raftMu.Unlock()
		return roachpb.NewError(err)
	}

	// Register the stream with a catch-up iterator.
	var catchUpIter relational.SimpleIterator
	if usingCatchupIter {
		innerIter := r.Engine().NewIterator(relational.IterOptions{
			UpperBound: args.Span.EndKey,
			// RangeFeed originally intended to use the time-bound iterator
			// performance optimization. However, they've had correctness issues in
			// the past (#28358, #34819) and no-one has the time for the due-diligence
			// necessary to be confidant in their correctness going forward. Not using
			// them causes the total time spent in RangeFeed catchup on changefeed
			// over tpcc-1000 to go from 40s -> 4853s, which is quite large but still
			// workable. See #35122 for details.
			// MinTimestampHint: args.Timestamp,
		})
		catchUpIter = iteratorWithCloser{
			SimpleIterator: innerIter,
			close:          iterSemRelease,
		}
		// Responsibility for releasing the semaphore now passes to the iterator.
		iterSemRelease = nil
	}
	p := r.registerWithRangefeedRaftMuLocked(
		ctx, rSpan, args.Timestamp, catchUpIter, args.WithDiff, lockedStream, errC,
	)
	r.raftMu.Unlock()

	// When this function returns, attempt to clean up the rangefeed.
	defer r.maybeDisconnectEmptyRangefeed(p)

	// Block on the registration's error channel. Note that the registration
	// observes stream.Context().Done.
	return <-errC
}

func (r *Replica) getRangefeedProcessorAndFilter() (*rangefeed.Processor, *rangefeed.Filter) {
	r.rangefeedMu.RLock()
	defer r.rangefeedMu.RUnlock()
	return r.rangefeedMu.proc, r.rangefeedMu.opFilter
}

func (r *Replica) getRangefeedProcessor() *rangefeed.Processor {
	p, _ := r.getRangefeedProcessorAndFilter()
	return p
}

func (r *Replica) setRangefeedProcessor(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()

	defer r.rangefeedMu.Unlock()
	r.rangefeedMu.proc = p
	r.store.addReplicaWithRangefeed(r.RangeID)
}

func (r *Replica) unsetRangefeedProcessorLocked(p *rangefeed.Processor) {
	if r.rangefeedMu.proc != p {
		// The processor was already unset.
		return
	}
	r.rangefeedMu.proc = nil
	r.rangefeedMu.opFilter = nil
	r.store.removeReplicaWithRangefeed(r.RangeID)
}

func (r *Replica) unsetRangefeedProcessor(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	r.unsetRangefeedProcessorLocked(p)
}

func (r *Replica) setRangefeedFilterLocked(f *rangefeed.Filter) {
	if f == nil {
		panic("filter nil")
	}
	r.rangefeedMu.opFilter = f
}

func (r *Replica) updateRangefeedFilterLocked() bool {
	f := r.rangefeedMu.proc.Filter()
	// Return whether the update to the filter was successful or not. If
	// the processor was already stopped then we can't update the filter.
	if f != nil {
		r.setRangefeedFilterLocked(f)
		return true
	}
	return false
}

// The size of an event is 112 bytes, so this will result in an allocation on
// the order of ~512KB per RangeFeed. That's probably ok given the number of
// ranges on a node that we'd like to support with active rangefeeds, but it's
// certainly on the upper end of the range.
//
// TODO(dan): Everyone seems to agree that this memory limit would be better set
// at a store-wide level, but there doesn't seem to be an easy way to accomplish
// that.
const defaultEventChanCap = 4096

// registerWithRangefeedRaftMuLocked sets up a Rangefeed registration over the
// provided span. It initializes a rangefeed for the Replica if one is not
// already running. Requires raftMu be locked.
func (r *Replica) registerWithRangefeedRaftMuLocked(
	ctx context.Context,
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchupIter relational.SimpleIterator,
	withDiff bool,
	stream rangefeed.Stream,
	errC chan<- *roachpb.Error,
) *rangefeed.Processor {
	// Attempt to register with an existing Rangefeed processor, if one exists.
	// The locking here is a little tricky because we need to handle the case
	// of concurrent processor shutdowns (see maybeDisconnectEmptyRangefeed).

	r.rangefeedMu.Lock()
	p := r.rangefeedMu.proc
	if p != nil {
		reg, filter := p.Register(span, startTS, catchupIter, withDiff, stream, errC)

		if reg {
			// Registered successfully with an existing processor.
			// Update the rangefeed filter to avoid filtering ops
			// that this new registration might be interested in.
			r.setRangefeedFilterLocked(filter)
			r.rangefeedMu.Unlock()
			r.rangeFeedSplitOrMergeRegisterMu.Lock()

			if r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == registerStateDescConnect {
				if len(r.rangeFeedSplitOrMergeRegisterMu.preRspan) != 0 { // If preRspan is not empty, wait until the registration is complete then setting sig = 2
					rSpan := interval.Range{Start: interval.Comparable(r.Desc().RSpan().Key), End: interval.Comparable(r.Desc().RSpan().EndKey)}
					if p.RegSpanEqual(r.rangeFeedSplitOrMergeRegisterMu.preRspan, p.GetAll(), rSpan) {
						r.rangeFeedSplitOrMergeRegisterMu.preRspan = r.rangeFeedSplitOrMergeRegisterMu.preRspan[:0]
						r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = rigisterStateRigisterDone
					}
				} else {
					r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = rigisterStateRigisterDone
				}
			}

			r.rangeFeedSplitOrMergeRegisterMu.Unlock()
			return p
		}
		// If the registration failed, the processor was already being shut
		// down. Help unset it and then continue on with initializing a new
		// processor.
		r.unsetRangefeedProcessorLocked(p)
		p = nil
	}
	r.rangefeedMu.Unlock()

	// Create a new rangefeed.
	desc := r.Desc()
	tp := rangefeedTxnPusher{ir: r.store.intentResolver, r: r}
	cfg := rangefeed.Config{
		AmbientContext:   r.AmbientContext,
		Clock:            r.Clock(),
		Span:             desc.RSpan(),
		RangeID:          int64(r.RangeID),
		TxnPusher:        &tp,
		PushTxnsInterval: r.store.TestingKnobs().RangeFeedPushTxnsInterval,
		PushTxnsAge:      r.store.TestingKnobs().RangeFeedPushTxnsAge,
		EventChanCap:     defaultEventChanCap,
		EventChanTimeout: 0,
		Metrics:          r.store.metrics.RangeFeedMetrics,
	}
	p = rangefeed.NewProcessor(cfg)

	// Start it with an iterator to initialize the resolved timestamp.
	rtsIter := r.Engine().NewIterator(relational.IterOptions{
		UpperBound: desc.EndKey.AsRawKey(),
		// TODO(nvanbenschoten): To facilitate fast restarts of rangefeed
		// we should periodically persist the resolved timestamp so that we
		// can initialize the rangefeed using an iterator that only needs to
		// observe timestamps back to the last recorded resolved timestamp.
		// This is safe because we know that there are no unresolved intents
		// at times before a resolved timestamp.
		// MinTimestampHint: r.ResolvedTimestamp,
	})
	p.Start(r.store.Stopper(), rtsIter)

	// Register with the processor *before* we attach its reference to the
	// Replica struct. This ensures that the registration is in place before
	// any other goroutines are able to stop the processor. In other words,
	// this ensures that the only time the registration fails is during
	// server shutdown.
	reg, filter := p.Register(span, startTS, catchupIter, withDiff, stream, errC)

	if !reg {
		catchupIter.Close() // clean up
		select {
		case <-r.store.Stopper().ShouldQuiesce():
			errC <- roachpb.NewError(&roachpb.NodeUnavailableError{})
			return nil
		default:
			panic("unexpected Stopped processor")
		}
	}

	// Set the rangefeed processor and filter reference. We know that no other
	// registration process could have raced with ours because calling this
	// method requires raftMu to be exclusively locked.
	r.setRangefeedProcessor(p)
	r.setRangefeedFilterLocked(filter)
	r.rangeFeedSplitOrMergeRegisterMu.Lock()

	if r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == registerStateDescConnect {
		if len(r.rangeFeedSplitOrMergeRegisterMu.preRspan) != 0 { // If preRspan is not empty, wait until the registration is complete then setting sig = 2
			rSpan := interval.Range{Start: interval.Comparable(r.Desc().RSpan().Key), End: interval.Comparable(r.Desc().RSpan().EndKey)}
			if p.RegSpanEqual(r.rangeFeedSplitOrMergeRegisterMu.preRspan, p.GetAll(), rSpan) {
				r.rangeFeedSplitOrMergeRegisterMu.preRspan = r.rangeFeedSplitOrMergeRegisterMu.preRspan[:0]
				r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = rigisterStateRigisterDone
			}
		} else {
			r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = rigisterStateRigisterDone
		}
	}

	r.rangeFeedSplitOrMergeRegisterMu.Unlock()

	// Check for an initial closed timestamp update immediately to help
	// initialize the rangefeed's resolved timestamp as soon as possible.
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)

	return p
}

// maybeDisconnectEmptyRangefeed tears down the provided Processor if it is
// still active and if it no longer has any registrations.
func (r *Replica) maybeDisconnectEmptyRangefeed(p *rangefeed.Processor) {
	r.rangefeedMu.Lock()
	defer r.rangefeedMu.Unlock()
	if p == nil || p != r.rangefeedMu.proc {
		// The processor has already been removed or replaced.
		return
	}
	if p.Len() == 0 || !r.updateRangefeedFilterLocked() {
		// Stop the rangefeed processor if it has no registrations or if we are
		// unable to update the operation filter.
		p.Stop()
		r.unsetRangefeedProcessorLocked(p)
	}
}

// disconnectRangefeedWithErr broadcasts the provided error to all rangefeed
// registrations and tears down the provided rangefeed Processor.
func (r *Replica) disconnectRangefeedWithErr(p *rangefeed.Processor, pErr *roachpb.Error) {
	p.StopWithErr(pErr)
	r.unsetRangefeedProcessor(p)
}

// getNextRigisterSpan returns the span that needs to be registered again
func (r *Replica) getNextRigisterSpan(p *rangefeed.Processor) []interval.Range {
	r.rangeFeedSplitOrMergeRegisterMu.Lock()

	// If r needs to be registered before, but is not fully registered, then return preRspan.
	if len(r.rangeFeedSplitOrMergeRegisterMu.preRspan) != 0 && r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig != rigisterStateRigisterDone {
		r.rangeFeedSplitOrMergeRegisterMu.Unlock()
		return r.rangeFeedSplitOrMergeRegisterMu.preRspan
	}
	r.rangeFeedSplitOrMergeRegisterMu.Unlock()
	return p.GetAll()
}

// disconnectRangefeedWithReason broadcasts the provided rangefeed retry reason
// to all rangefeed registrations and tears down the active rangefeed Processor.
// No-op if a rangefeed is not active.
func (r *Replica) disconnectRangefeedWithReason(
	reason roachpb.RangeFeedRetryError_Reason, rightReplOrNil *Replica,
) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}

	pErr := roachpb.NewError(roachpb.NewRangeFeedRetryError(reason))

	splitHanlder := func() {
		if reason == roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT { // split
			// If there is a registration, send a signal to handleLogicalOpLogRaftMuLocked to stop sending events to the channel and cache the events.
			// [  left    ][  right    ]
			//  [] [         ] [  ]      inSpan
			//  []            [  ]       inSpan
			// 2 on the left, 2 on the right
			splitSpan := func(inSpan []interval.Range) ([]interval.Range, []interval.Range) {
				left := make([]interval.Range, 0)
				right := make([]interval.Range, 0)

				i := 0
				for i = 0; i < len(inSpan); i++ {
					if inSpan[i].End.Compare(interval.Comparable(r.Desc().EndKey)) < 0 { // span is on the left side of the replica end, then give left
						left = append(left, inSpan[i])
					} else if inSpan[i].End.Compare(interval.Comparable(r.Desc().EndKey)) == 0 {
						left = append(left, inSpan[i])
						break
					} else {
						if inSpan[i].Start.Compare(interval.Comparable(r.Desc().EndKey)) < 0 {
							left = append(left, interval.Range{Start: inSpan[i].Start, End: interval.Comparable(r.Desc().EndKey)})
						}

						rightStart := interval.Comparable(r.Desc().EndKey)
						if rightStart.Compare(inSpan[i].Start) < 0 {
							rightStart = inSpan[i].Start
						}
						right = append(right, interval.Range{Start: rightStart, End: inSpan[i].End})
						break
					}
				}

				if i < len(inSpan)-1 { // i == len-1; end, do nothing
					right = append(right, append([]interval.Range{}, inSpan[i+1:]...)...)
				}

				return left, right
			}

			// 1 Get the left registered span; split the span into two parts according to
			// the left and right ranges, and assign them to the left and right preRspans respectively
			pSpan := r.getNextRigisterSpan(p)
			r.rangeFeedSplitOrMergeRegisterMu.Lock()
			pSpanLeft, pSpanRight := splitSpan(pSpan)
			r.rangeFeedSplitOrMergeRegisterMu.preRspan = pSpanLeft
			//rets := p.GetAllSpanSlice() // print original registration, a range may contain multiple intervals;
			r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = registerStateDescConnect
			r.rangeFeedSplitOrMergeRegisterMu.notFirst = false // init, first time
			r.rangeFeedSplitOrMergeRegisterMu.Unlock()

			rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.Lock()
			rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.preRspan = pSpanRight // right
			rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = registerStateDescConnect
			rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.notFirst = false //  init, first time
			rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.Unlock()
			// todo: If buffer has kv, split buffer's kv to left and right
		}
	}

	mergeHanlder := func() {
		if reason == roachpb.RangeFeedRetryError_REASON_RANGE_MERGED { // merge
			// If there is a registration, send a signal to handleLogicalOpLogRaftMuLocked
			// to stop sending events to the channel and cache the events.
			if rightReplOrNil != nil {
				// left of merge
				pSpan1 := r.getNextRigisterSpan(p)
				var pSpan2 []interval.Range
				pr := rightReplOrNil.getRangefeedProcessor()
				if pr != nil {
					pSpan2 = rightReplOrNil.getNextRigisterSpan(pr)
					pSpan1 = append(pSpan1, pSpan2...)
				}
				r.rangeFeedSplitOrMergeRegisterMu.Lock()
				sort.Slice(pSpan1, func(i, j int) bool {
					if pSpan1[i].Start.Compare(pSpan1[j].Start) < 0 {
						return true
					}
					return false
				})
				pSpan := make([]interval.Range, 0)
				for i := 0; i < len(pSpan1); i++ {
					if len(pSpan) == 0 {
						pSpan = append(pSpan, pSpan1[i])
					} else {
						if pSpan1[i].Start.Compare(pSpan[len(pSpan)-1].End) <= 0 {
							if pSpan1[i].End.Compare(pSpan[len(pSpan)-1].End) > 0 {
								pSpan[len(pSpan)-1].End = pSpan1[i].End
							}
						} else {
							pSpan = append(pSpan, pSpan1[i])
						}
					}
				}
				r.rangeFeedSplitOrMergeRegisterMu.preRspan = pSpan // Span registered before disconnection
				r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = registerStateDescConnect
				r.rangeFeedSplitOrMergeRegisterMu.notFirst = false
				r.rangeFeedSplitOrMergeRegisterMu.Unlock()

				rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.Lock()
				rightBuft := make([]enginepb.MVCCLogicalOp, 0)
				if len(rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer) != 0 && // If right has unsent data and is not registered successfully, the buffer data is given to left
					rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == registerStateDescConnect {
					rightBuft = append(rightBuft, rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer...)
					rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer = rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer[:0]
				}
				rightReplOrNil.rangeFeedSplitOrMergeRegisterMu.Unlock()
				r.rangeFeedSplitOrMergeRegisterMu.Lock()
				r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer = append(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer, rightBuft...)
				r.rangeFeedSplitOrMergeRegisterMu.Unlock()
			} else {
				r.rangeFeedSplitOrMergeRegisterMu.Lock()
				r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = rigisterStateRigisterDone
				r.rangeFeedSplitOrMergeRegisterMu.preRspan = r.rangeFeedSplitOrMergeRegisterMu.preRspan[:0]
				r.rangeFeedSplitOrMergeRegisterMu.Unlock()
			}
		}
	}

	splitHanlder()
	mergeHanlder()

	r.disconnectRangefeedWithErr(p, pErr)
}

// numRangefeedRegistrations returns the number of registrations attached to the
// Replica's rangefeed processor.
func (r *Replica) numRangefeedRegistrations() int {
	p := r.getRangefeedProcessor()
	if p == nil {
		return 0
	}
	return p.Len()
}

// populatePrevValsInLogicalOpLogRaftMuLocked updates the provided logical op
// log with previous values read from the reader, which is expected to reflect
// the state of the Replica before the operations in the logical op log are
// applied. No-op if a rangefeed is not active. Requires raftMu to be locked.
func (r *Replica) populatePrevValsInLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagepb.LogicalOpLog, prevReader relational.Reader,
) {
	p, filter := r.getRangefeedProcessorAndFilter()
	if p == nil {
		return
	}

	// Read from the Reader to populate the PrevValue fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var prevValPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
		case *enginepb.MVCCCommitIntentOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
			//continue
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp:
			//key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
			continue
		case *enginepb.MVCCCommitTxnOp,
			*enginepb.MVCCAbortOp:
			//key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
			continue
		case *enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCAbortTxnOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Don't read previous values from the reader for operations that are
		// not needed by any rangefeed registration.
		if !filter.NeedPrevVal(roachpb.Span{Key: key}) {
			continue
		}

		// Read the previous value from the prev Reader. Unlike the new value
		// (see handleLogicalOpLogRaftMuLocked), this one may be missing.
		prevVal, _, err := relational.MVCCGet(
			ctx, prevReader, key, ts, relational.MVCCGetOptions{Tombstones: true, Inconsistent: true},
		)
		if err != nil {
			r.disconnectRangefeedWithErr(p, roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		if prevVal != nil {
			*prevValPtr = prevVal.RawBytes
		} else {
			*prevValPtr = nil
		}
	}
}

// WriteValue Read the value directly from the Reader
func WriteValue(
	ctx context.Context, ops *storagepb.LogicalOpLog, r *Replica, reader relational.Reader,
) {
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var valPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCCommitIntentOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCAbortTxnOp,
			*enginepb.MVCCCommitTxnOp,
			*enginepb.MVCCAbortOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Read the value directly from the Reader. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := relational.MVCCGet(ctx, reader, key, ts, relational.MVCCGetOptions{Tombstones: true})
		if val == nil && err == nil {
			err = errors.New("value missing in reader")
		}
		if err != nil {
			continue
		}
		*valPtr = val.RawBytes
	}
}

func (r *Replica) sendRightRangeFeedEvent(ctx context.Context) {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     1 * time.Second,
		MaxRetries:     100, // About 100 seconds
	}
	reSent := false
	unsetPorcessorF := false
	for re := retry.Start(opts); re.Next(); {
		p, _ := r.getRangefeedProcessorAndFilter()
		if p == nil {
			continue
		}
		r.rangeFeedSplitOrMergeRegisterMu.Lock()
		size := len(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer)
		if size > 50000 { //Exceeding the maximum limit
			log.VEventf(ctx, 3, "rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffe above 50000, size:%d", size)
		}
		if r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == rigisterStateRigisterDone {
			reSent = true

			if len(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer) != 0 {
				if !p.ConsumeLogicalOps(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer...) {
					// Consumption failed and the rangefeed was stopped.
					unsetPorcessorF = true
				}
				r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer = r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer[:0]
				r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = registerStateInit
				r.rangeFeedSplitOrMergeRegisterMu.Unlock()

				if unsetPorcessorF {
					r.unsetRangefeedProcessor(p)
				}
				break
			}
		}
		r.rangeFeedSplitOrMergeRegisterMu.Unlock()
	}

	// After the for loop exits, if there is no new registration, release rangeFeedBuffer
	if !reSent {
		r.rangeFeedSplitOrMergeRegisterMu.Lock()
		if len(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer) != 0 {
			r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer = r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer[:0]
		}
		r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig = registerStateInit
		r.rangeFeedSplitOrMergeRegisterMu.Unlock()
	}
}

// handleLogicalOpLogRaftMuLocked passes the logical op log to the active
// rangefeed, if one is running. The method accepts a reader, which is used to
// look up the values associated with key-value writes in the log before handing
// them to the rangefeed processor. No-op if a rangefeed is not active. Requires
// raftMu to be locked.
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagepb.LogicalOpLog, reader relational.Reader,
) {
	if ops == nil {
		// Rangefeeds can't be turned on unless RangefeedEnabled is set to true,
		// after which point new Raft proposals will include logical op logs.
		// However, there's a race present where old Raft commands without a
		// logical op log might be passed to a rangefeed. Since the effect of
		// these commands was not included in the catch-up scan of current
		// registrations, we're forced to throw an error. The rangefeed clients
		// can reconnect at a later time, at which point all new Raft commands
		// should have logical op logs.
		r.disconnectRangefeedWithReason(roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING, nil)
		return
	}
	if len(ops.Ops) == 0 {
		return
	}

	r.rangeFeedSplitOrMergeRegisterMu.Lock()
	// 1 After func receives the signal, it constructs a buffer and adds subsequent events to the buffer without adding them to the channel.
	// 2 When a new registration comes in, I will send the event in the buffer, and then send the user event in real time
	// 3 Only split and merge will result in new registrations; and in case of merge, only left should result in new registrations;
	// 4  So if there is no new registration for a long time, the buffer will be released.
	//0 Unsplit, default value
	//1 Split old registration
	//2 Split, New Registration
	// New registration, send buf, set the flag to 0
	// How to judge whether a new registration has been made? When a registration is made, if the mark
	// is 0, no operation will be performed; if the mark is 1, the mark is set to 2;
	// The thread polls the flag. Under normal circumstances, the flag is 1. After a period of time, the
	// flag is 2. This means that a new registration has been made, so buf is sent. After the sending is
	// completed, the flag is set to 0.
	// The thread sending needs to acquire the lock to prevent adding data to buf during the sending process;
	// The order is to send the kv of buf first, and then send the user's real-time kv.
	//r.rangeFeedSplitOrMergeRegisterMu.Lock()
	if (r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == registerStateDescConnect ||
		r.rangeFeedSplitOrMergeRegisterMu.splitMergeSig == rigisterStateRigisterDone) && // ==2 should also be added
		len(ops.Ops) != 0 {
		WriteValue(ctx, ops, r, reader)
		r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer = append(r.rangeFeedSplitOrMergeRegisterMu.rangeFeedbuffer, ops.Ops...)
		// first time, start the thread and detect new registrations.
		if !r.rangeFeedSplitOrMergeRegisterMu.notFirst {
			r.rangeFeedSplitOrMergeRegisterMu.notFirst = true
			go r.sendRightRangeFeedEvent(ctx)
		}
		r.rangeFeedSplitOrMergeRegisterMu.Unlock()
		return
	}
	r.rangeFeedSplitOrMergeRegisterMu.Unlock()
	//if checkSiginalofSplitorMerge() {
	//	if r is registered {
	//		cache ops
	//	} else {
	//		normal table, don't need to replicate, skip
	//	}
	//	When a user comes in for the first time, the thread is started to monitor the
	//	next re-registration;
	// 	if the user re-registers, the buffer is sent out;
	//	with a timeout mechanism
	//}
	// 1 If the replica is not registered, it cannot affect
	// 2 If the replica is registered, save it to bufer when it splits; wait for disconnection, then re-register, and then send
	// 3 How to determine whether it is a new registration after disconnection? Set a flag new when registering;
	//   set a flag old when splitting; determine whether it is old or new based on new and old.
	//
	p, filter := r.getRangefeedProcessorAndFilter()
	if p == nil {
		return
	}

	// When reading straight from the Raft log, some logical ops will not bes
	// fully populated. Read from the Reader to populate all fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var valPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCCommitIntentOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCAbortTxnOp,
			*enginepb.MVCCCommitTxnOp,
			*enginepb.MVCCAbortOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Don't read values from the reader for operations that are not needed
		// by any rangefeed registration. We still need to inform the rangefeed
		// processor of the changes to intents so that it can track unresolved
		// intents, but we don't need to provide values.
		//
		// We could filter out MVCCWriteValueOp operations entirely at this
		// point if they are not needed by any registration, but as long as we
		// avoid the value lookup here, doing any more doesn't seem worth it.
		if !filter.NeedVal(roachpb.Span{Key: key}) {
			continue
		}

		// Read the value directly from the Reader. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := relational.MVCCGet(ctx, reader, key, ts, relational.MVCCGetOptions{Tombstones: true})
		if val == nil && err == nil {
			err = errors.New("value missing in reader")
		}
		if err != nil {
			r.disconnectRangefeedWithErr(p, roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		*valPtr = val.RawBytes
	}

	// Pass the ops to the rangefeed processor.
	if !p.ConsumeLogicalOps(ops.Ops...) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// handleClosedTimestampUpdate determines the current maximum closed timestamp
// for the replica and informs the rangefeed, if one is running. No-op if a
// rangefeed is not active.
func (r *Replica) handleClosedTimestampUpdate(ctx context.Context) {
	ctx = r.AnnotateCtx(ctx)
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)
}

// handleClosedTimestampUpdateRaftMuLocked is like handleClosedTimestampUpdate,
// but it requires raftMu to be locked.
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(ctx context.Context) {
	p := r.getRangefeedProcessor()
	if p == nil {
		return
	}

	// Determine what the maximum closed timestamp is for this replica.
	closedTS, _ := r.maxClosed(ctx)

	// If the closed timestamp is sufficiently stale, signal that we want an
	// update to the leaseholder so that it will eventually begin to progress
	// again.
	slowClosedTSThresh := 5 * closedts.TargetDuration.Get(&r.store.cfg.Settings.SV)
	if d := timeutil.Since(closedTS.GoTime()); d > slowClosedTSThresh {
		m := r.store.metrics.RangeFeedMetrics
		if m.RangeFeedSlowClosedTimestampLogN.ShouldLog() {
			if closedTS.IsEmpty() {
				log.Infof(ctx, "RangeFeed closed timestamp is empty")
			} else {
				log.Infof(ctx, "RangeFeed closed timestamp %s is behind by %s", closedTS, d)
			}
		}

		// Asynchronously attempt to nudge the closed timestamp in case it's stuck.
		key := fmt.Sprintf(`rangefeed-slow-closed-timestamp-nudge-r%d`, r.RangeID)
		// Ignore the result of DoChan since, to keep this all async, it always
		// returns nil and any errors are logged by the closure passed to the
		// `DoChan` call.
		_, _ = m.RangeFeedSlowClosedTimestampNudge.DoChan(key, func() (interface{}, error) {
			// Also ignore the result of RunTask, since it only returns errors when
			// the task didn't start because we're shutting down.
			_ = r.store.stopper.RunTask(ctx, key, func(context.Context) {
				// Limit the amount of work this can suddenly spin up. In particular,
				// this is to protect against the case of a system-wide slowdown on
				// closed timestamps, which would otherwise potentially launch a huge
				// number of lease acquisitions all at once.
				select {
				case <-ctx.Done():
					// Don't need to do this anymore.
					return
				case m.RangeFeedSlowClosedTimestampNudgeSem <- struct{}{}:
				}
				defer func() { <-m.RangeFeedSlowClosedTimestampNudgeSem }()
				if err := r.ensureClosedTimestampStarted(ctx); err != nil {
					log.Infof(ctx, `RangeFeed failed to nudge: %s`, err)
				}
			})
			return nil, nil
		})
	}

	// If the closed timestamp is not empty, inform the Processor.
	if closedTS.IsEmpty() {
		return
	}
	if !p.ForwardClosedTS(closedTS) {
		// Consumption failed and the rangefeed was stopped.
		r.unsetRangefeedProcessor(p)
	}
}

// ensureClosedTimestampStarted does its best to make sure that this node is
// receiving closed timestamp updated for this replica's range. Note that this
// forces a valid lease to exist on the range and so can be reasonably expensive
// if there is not already a valid lease.
func (r *Replica) ensureClosedTimestampStarted(ctx context.Context) *roachpb.Error {
	// Make sure there's a leaseholder. If there's no leaseholder, there's no
	// closed timestamp updates.
	var leaseholderNodeID roachpb.NodeID
	_, err := r.redirectOnOrAcquireLease(ctx)
	if err == nil {
		// We have the lease. Request is essentially a wrapper for calling EmitMLAI
		// on a remote node, so cut out the middleman.
		r.EmitMLAI()
		return nil
	} else if lErr, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); ok {
		if lErr.LeaseHolder == nil {
			// It's possible for redirectOnOrAcquireLease to return
			// NotLeaseHolderErrors with LeaseHolder unset, but these should be
			// transient conditions. If this method is being called by RangeFeed to
			// nudge a stuck closedts, then essentially all we can do here is nothing
			// and assume that redirectOnOrAcquireLease will do something different
			// the next time it's called.
			return nil
		}
		leaseholderNodeID = lErr.LeaseHolder.NodeID
	} else {
		return err
	}
	// Request fixes any issues where we've missed a closed timestamp update or
	// where we're not connected to receive them from this node in the first
	// place.
	r.store.cfg.ClosedTimestamp.Clients.Request(leaseholderNodeID, r.RangeID)
	return nil
}
