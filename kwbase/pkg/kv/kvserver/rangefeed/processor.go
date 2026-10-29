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

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/interval"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

const (
	// defaultPushTxnsInterval is the default interval at which a Processor will
	// push all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	defaultPushTxnsInterval = 250 * time.Millisecond
	// defaultPushTxnsAge is the default age at which a Processor will begin to
	// consider a transaction old enough to push.
	defaultPushTxnsAge = 10 * time.Second
	// defaultCheckStreamsInterval is the default interval at which a Processor
	// will check all streams to make sure they have not been canceled.
	defaultCheckStreamsInterval = 1 * time.Second
)

// newErrBufferCapacityExceeded creates an error that is returned to subscribers
// if the rangefeed processor is not able to keep up with the flow of incoming
// events and is forced to drop events in order to not block.
func newErrBufferCapacityExceeded() *roachpb.Error {
	return roachpb.NewError(
		roachpb.NewRangeFeedRetryError(roachpb.RangeFeedRetryError_REASON_SLOW_CONSUMER),
	)
}

// Config encompasses the configuration required to create a Processor.
type Config struct {
	log.AmbientContext
	Clock   *hlc.Clock
	Span    roachpb.RSpan
	RangeID int64

	TxnPusher TxnPusher
	// PushTxnsInterval specifies the interval at which a Processor will push
	// all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	PushTxnsInterval time.Duration
	// PushTxnsAge specifies the age at which a Processor will begin to consider
	// a transaction old enough to push.
	PushTxnsAge time.Duration

	// EventChanCap specifies the capacity to give to the Processor's input
	// channel.
	EventChanCap int
	// EventChanTimeout specifies the maximum duration that methods will
	// wait to send on the Processor's input channel before giving up and
	// shutting down the Processor. 0 for no timeout.
	EventChanTimeout time.Duration

	// CheckStreamsInterval specifies interval at which a Processor will check
	// all streams to make sure they have not been canceled.
	CheckStreamsInterval time.Duration

	// Metrics is for production monitoring of RangeFeeds.
	Metrics *Metrics
}

// SetDefaults initializes unset fields in Config to values
// suitable for use by a Processor.
func (sc *Config) SetDefaults() {
	if sc.TxnPusher == nil {
		if sc.PushTxnsInterval != 0 {
			panic("nil TxnPusher with non-zero PushTxnsInterval")
		}
		if sc.PushTxnsAge != 0 {
			panic("nil TxnPusher with non-zero PushTxnsAge")
		}
	} else {
		if sc.PushTxnsInterval == 0 {
			sc.PushTxnsInterval = defaultPushTxnsInterval
		}
		if sc.PushTxnsAge == 0 {
			sc.PushTxnsAge = defaultPushTxnsAge
		}
	}
	if sc.CheckStreamsInterval == 0 {
		sc.CheckStreamsInterval = defaultCheckStreamsInterval
	}
}

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
//  1. it translates logical updates into rangefeed events.
//  2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//     timestamp.
type Processor struct {
	Config
	reg registry
	rts resolvedTimestamp

	regC       chan registration
	unregC     chan *registration
	lenReqC    chan struct{}
	lenResC    chan int
	filterReqC chan struct{}
	filterResC chan *Filter
	eventC     chan *event
	stopC      chan *roachpb.Error
	stoppedC   chan struct{}
}

var eventSyncPool = sync.Pool{
	New: func() interface{} {
		return new(event)
	},
}

func getPooledEvent(ev event) *event {
	e := eventSyncPool.Get().(*event)
	*e = ev
	return e
}

func putPooledEvent(ev *event) {
	*ev = event{}
	eventSyncPool.Put(ev)
}

// event is a union of different event types that the Processor goroutine needs
// to be informed of. It is used so that all events can be sent over the same
// channel, which is necessary to prevent reordering.
type event struct {
	ops            []enginepb.MVCCLogicalOp
	ct             hlc.Timestamp
	initRTS        bool
	syncC          chan struct{}
	regCatchupSpan roachpb.Span
	// This setting is used in conjunction with syncC in tests in order to ensure
	// that all registrations have fully finished outputting their buffers. This
	// has to be done by the processor in order to avoid race conditions with the
	// registry. Should be used only in tests.
	testRegCatchupSpan roachpb.Span
}

// NewProcessor creates a new rangefeed Processor. The corresponding goroutine
// should be launched using the Start method.
func NewProcessor(cfg Config) *Processor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	return &Processor{
		Config: cfg,
		reg:    makeRegistry(),
		rts:    makeResolvedTimestamp(),

		regC:       make(chan registration),
		unregC:     make(chan *registration),
		lenReqC:    make(chan struct{}),
		lenResC:    make(chan int),
		filterReqC: make(chan struct{}),
		filterResC: make(chan *Filter),
		eventC:     make(chan *event, cfg.EventChanCap),
		stopC:      make(chan *roachpb.Error, 1),
		stoppedC:   make(chan struct{}),
	}
}

// Start launches a goroutine to process rangefeed events and send them to
// registrations.
//
// The provided iterator is used to initialize the rangefeed's resolved
// timestamp. It must obey the contract of an iterator used for an
// initResolvedTSScan. The Processor promises to clean up the iterator by
// calling its Close method when it is finished. If the iterator is nil then
// no initialization scan will be performed and the resolved timestamp will
// immediately be considered initialized.
func (p *Processor) Start(stopper *stop.Stopper, rtsIter relational.SimpleIterator) {
	ctx := p.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		defer close(p.stoppedC)
		ctx, cancelOutputLoops := context.WithCancel(ctx)
		defer cancelOutputLoops()

		// Launch an async task to scan over the resolved timestamp iterator and
		// initialize the unresolvedIntentQueue. Ignore error if quiescing.
		if rtsIter != nil {
			initScan := newInitResolvedTSScan(p, rtsIter)
			err := stopper.RunAsyncTask(ctx, "rangefeed: init resolved ts", initScan.Run)
			if err != nil {
				initScan.Cancel()
			}
		} else {
			p.initResolvedTS(ctx)
		}

		// txnPushTicker periodically pushes the transaction record of all
		// unresolved intents that are above a certain age, helping to ensure
		// that the resolved timestamp continues to make progress.
		var txnPushTicker *time.Ticker
		var txnPushTickerC <-chan time.Time
		var txnPushAttemptC chan struct{}
		if p.PushTxnsInterval > 0 {
			txnPushTicker = time.NewTicker(p.PushTxnsInterval)
			txnPushTickerC = txnPushTicker.C
			defer txnPushTicker.Stop()
		}

		for {
			select {

			// Handle new registrations.
			case r := <-p.regC:
				if !p.Span.AsRawSpanWithNoLocals().Contains(r.span) {
					log.Fatalf(ctx, "registration %s not in Processor's key range %v", r, p.Span)
				}

				// Add the new registration to the registry.
				p.reg.Register(&r)

				// Publish an updated filter that includes the new registration.
				p.filterResC <- p.reg.NewFilter()

				// Immediately publish a checkpoint event to the registry. This will be
				// the first event published to this registration after its initial
				// catch-up scan completes.
				r.publish(p.newCheckpointEvent())

				// Run an output loop for the registry.
				runOutputLoop := func(ctx context.Context) {
					r.runOutputLoop(ctx)
					select {
					case p.unregC <- &r:
					case <-p.stoppedC:
					}
				}
				if err := stopper.RunAsyncTask(ctx, "rangefeed: output loop", runOutputLoop); err != nil {
					if r.catchupIter != nil {
						r.catchupIter.Close() // clean up
					}
					r.disconnect(roachpb.NewError(err))
					p.reg.Unregister(&r)
				}

			// Respond to unregistration requests; these come from registrations that
			// encounter an error during their output loop.
			case r := <-p.unregC:
				p.reg.Unregister(r)

			// Respond to answers about the processor goroutine state.
			case <-p.lenReqC:
				p.lenResC <- p.reg.Len()

			// Respond to answers about which operations can be filtered before
			// reaching the Processor.
			case <-p.filterReqC:
				p.filterResC <- p.reg.NewFilter()

			// Transform and route events.
			case e := <-p.eventC:
				p.consumeEvent(ctx, e)
				putPooledEvent(e)

			// Check whether any unresolved intents need a push.
			case <-txnPushTickerC:
				// Don't perform transaction push attempts until the resolved
				// timestamp has been initialized.
				if !p.rts.IsInit() {
					continue
				}

				now := p.Clock.Now()
				before := now.Add(-p.PushTxnsAge.Nanoseconds(), 0)
				oldTxns := p.rts.intentQ.Before(before)

				if len(oldTxns) > 0 {
					toPush := make([]enginepb.TxnMeta, len(oldTxns))
					for i, txn := range oldTxns {
						toPush[i] = txn.asTxnMeta()
					}

					// Set the ticker channel to nil so that it can't trigger a
					// second concurrent push. Create a push attempt response
					// channel that is closed when the push attempt completes.
					txnPushTickerC = nil
					txnPushAttemptC = make(chan struct{})

					// Launch an async transaction push attempt that pushes the
					// timestamp of all transactions beneath the push offset.
					// Ignore error if quiescing.
					pushTxns := newTxnPushAttempt(p, toPush, now, txnPushAttemptC)
					err := stopper.RunAsyncTask(ctx, "rangefeed: pushing old txns", pushTxns.Run)
					if err != nil {
						pushTxns.Cancel()
					}
				}

			// Update the resolved timestamp based on the push attempt.
			case <-txnPushAttemptC:
				// Reset the ticker channel so that it can trigger push attempts
				// again. Set the push attempt channel back to nil.
				txnPushTickerC = txnPushTicker.C
				txnPushAttemptC = nil

			// Close registrations and exit when signaled.
			case pErr := <-p.stopC:
				p.reg.DisconnectWithErr(all, pErr)
				return

			// Exit on stopper.
			case <-stopper.ShouldQuiesce():
				pErr := roachpb.NewError(&roachpb.NodeUnavailableError{})
				p.reg.DisconnectWithErr(all, pErr)
				return
			}
		}
	})
}

// Stop shuts down the processor and closes all registrations. Safe to call on
// nil Processor. It is not valid to restart a processor after it has been
// stopped.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr shuts down the processor and closes all registrations with the
// specified error. Safe to call on nil Processor. It is not valid to restart a
// processor after it has been stopped.
func (p *Processor) StopWithErr(pErr *roachpb.Error) {
	if p == nil {
		return
	}
	// Flush any remaining events before stopping.
	p.syncEventCAll()

	// Send the processor a stop signal.
	p.sendStop(pErr)
}

func (p *Processor) sendStop(pErr *roachpb.Error) {
	select {
	case p.stopC <- pErr:
		// stopC has non-zero capacity so this should not block unless
		// multiple callers attempt to stop the Processor concurrently.
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

// Register registers the stream over the specified span of keys.
//
// The registration will not observe any events that were consumed before this
// method was called. It is undefined whether the registration will observe
// events that are consumed concurrently with this call. The channel will be
// provided an error when the registration closes.
//
// The optionally provided "catch-up" iterator is used to read changes from the
// engine which occurred after the provided start timestamp.
//
// If the method returns false, the processor will have been stopped, so calling
// Stop is not necessary. If the method returns true, it will also return an
// updated operation filter that includes the operations required by the new
// registration.
//
// NOT safe to call on nil Processor.
func (p *Processor) Register(
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchupIter relational.SimpleIterator,
	withDiff bool,
	stream Stream,
	errC chan<- *roachpb.Error,
) (bool, *Filter) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch up scan.
	p.syncEventC()

	r := newRegistration(
		span.AsRawSpanWithNoLocals(), startTS, catchupIter, withDiff,
		p.Config.EventChanCap, p.Metrics, stream, errC,
	)
	select {
	case p.regC <- r:
		// Wait for response.
		return true, <-p.filterResC
	case <-p.stoppedC:
		return false, nil
	}
}

// Len returns the number of registrations attached to the processor.
func (p *Processor) Len() int {
	if p == nil {
		return 0
	}

	// Ask the processor goroutine.
	select {
	case p.lenReqC <- struct{}{}:
		// Wait for response.
		return <-p.lenResC
	case <-p.stoppedC:
		return 0
	}
}

// GetAllSpanSlice returns an array containing all spans
func (p *Processor) GetAllSpanSlice() []interval.Range {
	return p.reg.GetAllSpanSlice()
}

// GetAll return all spans an combine spans
func (p *Processor) GetAll() []interval.Range {
	return p.reg.GetAll()
}

// RegSpanEqual rSpan is fullly register, return true; span1 equle span2, return true
func (p *Processor) RegSpanEqual(span1, span2 []interval.Range, rSpan interval.Range) bool {
	// true if rSpan is fully registered
	//origin   [     ]
	//case1   [  ][  ]
	//case2   [             ]
	//case3   [   ]
	regFullRange := func() bool {
		for i := 0; i < len(span2); i++ {
			if span2[i].Start.Compare(rSpan.Start) <= 0 {
				if span2[i].End.Compare(rSpan.Start) > 0 && span2[i].End.Compare(rSpan.End) < 0 {
					rSpan.Start = span2[i].End
				} else if span2[i].End.Compare(rSpan.End) >= 0 {
					// rSpan is fully registered
					return true
				}
			} else {
				// unregistered span in the header
				return false
			}
		}
		// unregistered span at the end
		return false
	}

	if regFullRange() {
		return true
	}

	// 2 if span1 not eq span, then false
	if len(span1) != len(span2) {
		return false
	}

	for i := 0; i < len(span1); i++ {
		if !span1[i].Start.Equal(span2[i].Start) || !span1[i].End.Equal(span2[i].End) {
			return false
		}
	}
	return true
}

// Filter returns a new operation filter based on the registrations attached to
// the processor. Returns nil if the processor has been stopped already.
func (p *Processor) Filter() *Filter {
	if p == nil {
		return nil
	}

	// Ask the processor goroutine.
	select {
	case p.filterReqC <- struct{}{}:
		// Wait for response.
		return <-p.filterResC
	case <-p.stoppedC:
		return nil
	}
}

// ConsumeLogicalOps informs the rangefeed processor of the set of logical
// operations. It returns false if consuming the operations hit a timeout, as
// specified by the EventChanTimeout configuration. If the method returns false,
// the processor will have been stopped, so calling Stop is not necessary. Safe
// to call on nil Processor.
func (p *Processor) ConsumeLogicalOps(ops ...enginepb.MVCCLogicalOp) bool {
	if p == nil {
		return true
	}
	if len(ops) == 0 {
		return true
	}
	return p.sendEvent(event{ops: ops}, p.EventChanTimeout)
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the rangefeed processor's resolved timestamp has advanced. It returns
// false if forwarding the closed timestamp hit a timeout, as specified by the
// EventChanTimeout configuration. If the method returns false, the processor
// will have been stopped, so calling Stop is not necessary.  Safe to call on
// nil Processor.
func (p *Processor) ForwardClosedTS(closedTS hlc.Timestamp) bool {
	if p == nil {
		return true
	}
	if closedTS == (hlc.Timestamp{}) {
		return true
	}
	return p.sendEvent(event{ct: closedTS}, p.EventChanTimeout)
}

// sendEvent informs the Processor of a new event. If a timeout is specified,
// the method will wait for no longer than that duration before giving up,
// shutting down the Processor, and returning false. 0 for no timeout.
func (p *Processor) sendEvent(e event, timeout time.Duration) bool {
	ev := getPooledEvent(e)
	if timeout == 0 {
		select {
		case p.eventC <- ev:
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	} else {
		select {
		case p.eventC <- ev:
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		default:
			select {
			case p.eventC <- ev:
			case <-p.stoppedC:
				// Already stopped. Do nothing.
			case <-time.After(timeout):
				// Sending on the eventC channel would have blocked.
				// Instead, tear down the processor and return immediately.
				p.sendStop(newErrBufferCapacityExceeded())
				return false
			}
		}
	}
	return true
}

// setResolvedTSInitialized informs the Processor that its resolved timestamp has
// all the information it needs to be considered initialized.
func (p *Processor) setResolvedTSInitialized() {
	p.sendEvent(event{initRTS: true}, 0 /* timeout */)
}

// syncEventC synchronizes access to the Processor goroutine, allowing the
// caller to establish causality with actions taken by the Processor goroutine.
// It does so by flushing the event pipeline.
func (p *Processor) syncEventC() {
	syncC := make(chan struct{})
	ev := getPooledEvent(event{syncC: syncC})
	select {
	case p.eventC <- ev:
		select {
		case <-syncC:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

func (p *Processor) syncEventCAll() {
	var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}
	syncC := make(chan struct{})
	// Here we add a synchronization mechanism.
	// After the synchronization is completed,
	// that is, after the data in the stream is
	// completely sent, exit.
	ev := getPooledEvent(event{syncC: syncC, regCatchupSpan: all})
	select {
	case p.eventC <- ev:
		select {
		case <-syncC:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

func (p *Processor) consumeEvent(ctx context.Context, e *event) {
	switch {
	case len(e.ops) > 0:
		p.consumeLogicalOps(ctx, e.ops)
	case e.ct != hlc.Timestamp{}:
		p.forwardClosedTS(ctx, e.ct)
	case e.initRTS:
		p.initResolvedTS(ctx)
	case e.syncC != nil:
		if e.testRegCatchupSpan.Valid() {
			if err := p.reg.waitForCaughtUp(e.testRegCatchupSpan); err != nil {
				log.Errorf(
					ctx,
					"error waiting for registries to catch up during test, results might be impacted: %s",
					err,
				)
			}
		}

		if e.regCatchupSpan.Valid() {
			if err := p.reg.waitForCaughtUp(e.regCatchupSpan); err != nil {
				log.Errorf(
					ctx,
					"error waiting for registries to catch up during test, results might be impacted: %s",
					err,
				)
			}
		}
		close(e.syncC)
	default:
		panic("missing event variant")
	}
}

func (p *Processor) consumeLogicalOps(ctx context.Context, ops []enginepb.MVCCLogicalOp) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			txnID := uuid.MakeV4()
			p.publishValue(ctx, txnID, t.Key, t.Timestamp, t.Value, t.PrevValue, true)
		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.
			if !isSystemMetaKey(t.Key) {
				if t.Publish {
					if t.Value == nil {
						t.Value = []byte{}
					}
					p.publishValue(ctx, t.TxnID, t.Key, t.Timestamp, t.Value, nil, false)
				}
			}
		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.
			if !isSystemMetaKey(t.Key) {
				if t.Publish {
					if t.Value == nil {
						t.Value = []byte{}
					}
					p.publishValue(ctx, t.TxnID, t.Key, t.Timestamp, t.Value, nil, false)
				}
			}
		case *enginepb.MVCCCommitIntentOp:
			// Publish write intent here.No updates to publish.
			if isSystemMetaKey(t.Key) {
				p.publishValue(ctx, t.TxnID, t.Key, t.Timestamp, t.Value, nil, false)
			}
		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.
		case *enginepb.MVCCAbortTxnOp:
			// No updates to publish.
		case *enginepb.MVCCCommitTxnOp:
			//if t.WriteKeyCount != 0 {}
			p.publishCommitTxn(t.TxnID, t.Timestamp, t.WriteKeyCount)
		case *enginepb.MVCCAbortOp:
			//if t.WriteKeyCount != 0 {}
			p.publishAbort(t.TxnID, t.WriteKeyCount)
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
	}
}

func (p *Processor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *Processor) initResolvedTS(ctx context.Context) {
	if p.rts.Init() {
		p.publishCheckpoint(ctx)
	}
}

func (p *Processor) publishValue(
	ctx context.Context,
	txnID uuid.UUID,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value, prevValue []byte,
	noTxn bool,
) {
	if !p.Span.ContainsKey(roachpb.RKey(key)) {
		//log.Fatalf(ctx, "key %v not in Processor's key range %v", key, p.Span)
	}

	var prevVal roachpb.Value
	if prevValue != nil {
		prevVal.RawBytes = prevValue
	}
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
		PrevValue: prevVal,
		TxnID:     txnID,
		RangeID:   p.RangeID,
		NoTxn:     noTxn,
	})
	p.reg.PublishToOverlapping(roachpb.Span{Key: key}, &event)
}

func (p *Processor) publishCommitTxn(
	txnID uuid.UUID, timestamp hlc.Timestamp, writeKeyCount int64,
) {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.Commit{
		TxnID:         txnID,
		WriteKeyCount: writeKeyCount,
		Timestamp:     timestamp,
	})
	p.reg.PublishToOverlapping(roachpb.Span{Key: roachpb.Key(p.Span.Key)}, &event)
}

func (p *Processor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(all, event)
}

func (p *Processor) publishAbort(txnID uuid.UUID, writeKeyCount int64) {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.Abort{
		TxnID:         txnID,
		WriteKeyCount: writeKeyCount,
	})
	p.reg.PublishToOverlapping(roachpb.Span{Key: roachpb.Key(p.Span.Key)}, &event)
}

func (p *Processor) publishSplit(leftRangeID, newRightRangeID int64) {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.SplitRangeIDs{
		LeftRangeID:     leftRangeID,
		NewRightRangeID: newRightRangeID,
	})
}

func (p *Processor) publishMerge(LeftRangeID, oldRightRangeID int64) {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.MergeRangeIDs{
		LeftRangeID:     LeftRangeID,
		OldRightRangeID: oldRightRangeID,
	})
}

func (p *Processor) newCheckpointEvent() *roachpb.RangeFeedEvent {
	// Create a RangeFeedCheckpoint over the Processor's entire span. Each
	// individual registration will trim this down to just the key span that
	// it is listening on in registration.maybeStripEvent before publishing.
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.RangeFeedCheckpoint{
		Span:       p.Span.AsRawSpanWithNoLocals(),
		ResolvedTS: p.rts.Get(),
	})
	return &event
}
