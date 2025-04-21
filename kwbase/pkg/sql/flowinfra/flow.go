// Copyright 2019 The Cockroach Authors.
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

package flowinfra

import (
	"context"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type flowStatus int

// Flow status indicators.
const (
	FlowNotStarted flowStatus = iota
	FlowRunning
	FlowFinished
)

// Startable is any component that can be started (a router or an outbox).
type Startable interface {
	Start(ctx context.Context, wg *sync.WaitGroup, ctxCancel context.CancelFunc)
}

// StartableFn is an adapter when a customer function (i.e. a custom goroutine)
// needs to become Startable.
type StartableFn func(context.Context, *sync.WaitGroup, context.CancelFunc)

// Start is a part of the Startable interface.
func (f StartableFn) Start(ctx context.Context, wg *sync.WaitGroup, ctxCancel context.CancelFunc) {
	f(ctx, wg, ctxCancel)
}

// FuseOpt specifies options for processor fusing at Flow.Setup() time.
type FuseOpt bool

const (
	// FuseNormally means fuse what you can, but don't serialize unordered input
	// synchronizers.
	FuseNormally FuseOpt = false
	// FuseAggressively means serialize unordered input synchronizers.
	// This is useful for flows that might have mutations which can't have any
	// concurrency.
	FuseAggressively = true
)

// Flow represents a flow which consists of processors and streams.
type Flow interface {
	// Setup sets up all the infrastructure for the flow as defined by the flow
	// spec. The flow will then need to be started and run. A new context (along
	// with a context cancellation function) is derived. The new context must be
	// used when running a flow so that all components running in their own
	// goroutines could listen for a cancellation on the same context.
	Setup(ctx context.Context, spec *execinfrapb.FlowSpec, opt FuseOpt) (context.Context, error)

	// SetTxn is used to provide the transaction in which the flow will run.
	// It needs to be called after Setup() and before Start/Run.
	SetTxn(*kv.Txn)

	// SetTS is used to tell if there is time-series flow bounded to the local flow.
	SetTS(bool)

	// SetPushDown is used to tell if there is time-series flow bounded to the local flow.
	SetPushDown(bool)
	// SetVectorized is used to tell if there is vec flow.
	SetVectorized(bool)
	// SetFormat is used to tell if there is time-series flow bounded to the local flow.
	SetFormat(bool)
	// SetCloses is part of the Flow interface
	SetCloses(closes int)
	// Start starts the flow. Processors run asynchronously in their own goroutines.
	// Wait() needs to be called to wait for the flow to finish.
	// See Run() for a synchronous version.
	//
	// Generally if errors are encountered during the setup part, they're returned.
	// But if the flow is a synchronous one, then no error is returned; instead the
	// setup error is pushed to the syncFlowConsumer. In this case, a subsequent
	// call to f.Wait() will not block.
	Start(_ context.Context, doneFn func()) error

	// Run runs the flow to completion. The last processor is run in the current
	// goroutine; others may run in different goroutines depending on how the flow
	// was configured.
	// f.Wait() is called internally, so the call blocks until all the flow's
	// goroutines are done.
	// The caller needs to call f.Cleanup().
	Run(_ context.Context, doneFn func()) error

	// Wait waits for all the goroutines for this flow to exit. If the context gets
	// canceled before all goroutines exit, it calls f.cancel().
	Wait()

	// IsLocal returns whether this flow does not have any remote execution.
	IsLocal() bool

	// IsVectorized returns whether this flow will run with vectorized execution.
	IsVectorized() bool

	// IsTimeSeries returns whether this flow is bounded by time-series execution.
	IsTimeSeries() bool

	// GetFlowCtx returns the flow context of this flow.
	GetFlowCtx() *execinfra.FlowCtx

	// AddStartable accumulates a Startable object.
	AddStartable(Startable)

	// GetID returns the flow ID.
	GetID() execinfrapb.FlowID

	// Cleanup should be called when the flow completes (after all processors and
	// mailboxes exited).
	Cleanup(context.Context)

	// ConcurrentExecution returns true if multiple processors/operators in the
	// flow will execute concurrently (i.e. if not all of them have been fused).
	// Can only be called after Setup().
	ConcurrentExecution() bool

	// SetFlowSpecMemSize set flowSpecMemSize.
	SetFlowSpecMemSize(memSize int64)

	// GetFlowSpecMemSize get flowSpecMemSize.
	GetFlowSpecMemSize() int64

	// GetStats get RowStats
	GetStats() execinfra.RowStats
}

// FlowBase is the shared logic between row based and vectorized flows. It
// implements Flow interface for convenience and for usage in tests, but if
// FlowBase.Setup is called, it'll panic.
type FlowBase struct {
	execinfra.FlowCtx

	flowRegistry *FlowRegistry
	// processors contains a subset of the processors in the flow - the ones that
	// run in their own goroutines. Some processors that implement RowSource are
	// scheduled to run in their consumer's goroutine; those are not present here.
	processors []execinfra.Processor
	// startables are entities that must be started when the flow starts;
	// currently these are outboxes and routers.
	startables []Startable
	// syncFlowConsumer is a special outbox which instead of sending rows to
	// another host, returns them directly (as a result to a SetupSyncFlow RPC,
	// or to the local host).
	syncFlowConsumer execinfra.RowReceiver

	localProcessors []execinfra.LocalProcessor

	// startedGoroutines specifies whether this flow started any goroutines. This
	// is used in Wait() to avoid the overhead of waiting for non-existent
	// goroutines.
	startedGoroutines bool

	// IsTimeSeries tells whether this flow is bounded by time-series execution.
	isTimeSeries bool
	AllPush      bool
	Format       bool
	isVectorized bool
	closes       int

	// inboundStreams are streams that receive data from other hosts; this map
	// is to be passed to FlowRegistry.RegisterFlow.
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo

	// waitGroup is used to wait for async components of the flow:
	//  - processors
	//  - inbound streams
	//  - outboxes
	waitGroup sync.WaitGroup

	doneFn func()

	status flowStatus

	// Cancel function for ctx. Call this to cancel the flow (safe to be called
	// multiple times).
	ctxCancel context.CancelFunc
	ctxDone   <-chan struct{}

	// spec is the request that produced this flow. Only used for debugging.
	// TODO(yuzefovich): probably we can get rid off this field.
	spec *execinfrapb.FlowSpec

	flowSpecMemSize int64

	// rowStats record stallTime and number of rows
	rowStats execinfra.RowStats
}

// SetFlowSpecMemSize is part of the Flow interface.
func (f *FlowBase) SetFlowSpecMemSize(memSize int64) {
	f.flowSpecMemSize = memSize
}

// GetFlowSpecMemSize is part of the Flow interface.
func (f *FlowBase) GetFlowSpecMemSize() int64 {
	return f.flowSpecMemSize
}

// GetStats get RowStats
func (f *FlowBase) GetStats() execinfra.RowStats {
	if !f.Cfg.TestingKnobs.DeterministicStats {
		return f.rowStats
	}
	return execinfra.RowStats{}
}

// Setup is part of the Flow interface.
func (f *FlowBase) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, _ FuseOpt,
) (context.Context, error) {
	ctx, f.ctxCancel = contextutil.WithCancel(ctx)
	f.ctxDone = ctx.Done()
	f.spec = spec

	mutationsTestingMaxBatchSize := int64(0)
	if f.FlowCtx.Cfg.Settings != nil {
		mutationsTestingMaxBatchSize = mutations.MutationsTestingMaxBatchSize.Get(&f.FlowCtx.Cfg.Settings.SV)
	}
	if mutationsTestingMaxBatchSize != 0 {
		mutations.SetMaxBatchSizeForTests(int(mutationsTestingMaxBatchSize))
	} else {
		mutations.ResetMaxBatchSizeForTests()
	}

	return ctx, nil
}

// SetTxn is part of the Flow interface.
func (f *FlowBase) SetTxn(txn *kv.Txn) {
	f.FlowCtx.Txn = txn
	f.EvalCtx.Txn = txn
}

// SetTS is part of the Flow interface
func (f *FlowBase) SetTS(t bool) {
	f.isTimeSeries = t
}

// SetVectorized is part of the Flow interface
func (f *FlowBase) SetVectorized(t bool) {
	f.isVectorized = t
}

// SetPushDown is part of the Flow interface
func (f *FlowBase) SetPushDown(allPush bool) {
	f.AllPush = allPush
}

// SetFormat is part of the Flow interface
func (f *FlowBase) SetFormat(format bool) {
	f.Format = format
}

// SetCloses is part of the Flow interface
func (f *FlowBase) SetCloses(closes int) {
	f.closes = closes
}

var pgEncodeShortCircuitEnabled = settings.RegisterBoolSetting(
	"sql.pg_encode_short_circuit.enabled", "enable the short circuit optimization", false,
)

// IsShortCircuitForPgEncode is part of the Flow interface.
func (f *FlowBase) IsShortCircuitForPgEncode() bool {

	// f.AllPush: Push other operators down to noop.
	// f.Format: The flag for service output encoding format, true:FormatBinary, false:FormatText.
	// The relational operator only has noop, and noop has no filtering conditions or output column pruning.
	if pgEncodeShortCircuitEnabled.Get(&f.Cfg.Settings.SV) && len(f.processors) <= 1 &&
		f.AllPush && !f.Format && len(f.TsTableReaders) == 1 && f.processors[0].IsShortCircuitForPgEncode() {
		return true
	}
	return false
}

// VecIsShortCircuitForPgEncode is part of the Flow interface.
func (f *FlowBase) VecIsShortCircuitForPgEncode(ctx context.Context) (canShortCircuit bool) {
	// f.AllPush: Push other operators down to noop.
	// f.Format: The flag for service output encoding format, true:FormatBinary, false:FormatText.
	// The relational operator only has noop, and noop has no filtering conditions or output column pruning.
	if pgEncodeShortCircuitEnabled.Get(&f.Cfg.Settings.SV) && len(f.processors) <= 1 &&
		f.AllPush && !f.Format {
		canShortCircuit = true
	} else {
		canShortCircuit = false
	}
	return f.processors[0].SupportPgWire()
}

// VecShortCircuitForPgEncode is part of the Flow interface.
func (f *FlowBase) VecShortCircuitForPgEncode(ctx context.Context) error {
	m := f.processors[0]
	m.Start(ctx)
	for {
		rev, code, err := m.NextPgWire()
		if err != nil {
			return err
		}

		if code == -1 {
			return nil
		}

		err = m.Push(ctx, rev)
		if err != nil {
			return err
		}
	}
}

// ConcurrentExecution is part of the Flow interface.
func (f *FlowBase) ConcurrentExecution() bool {
	return len(f.processors) > 1
}

var _ Flow = &FlowBase{}

// NewFlowBase creates a new FlowBase.
func NewFlowBase(
	flowCtx execinfra.FlowCtx,
	flowReg *FlowRegistry,
	syncFlowConsumer execinfra.RowReceiver,
	localProcessors []execinfra.LocalProcessor,
) *FlowBase {
	base := &FlowBase{
		FlowCtx:          flowCtx,
		flowRegistry:     flowReg,
		syncFlowConsumer: syncFlowConsumer,
		localProcessors:  localProcessors,
	}
	base.status = FlowNotStarted
	return base
}

// GetFlowCtx is part of the Flow interface.
func (f *FlowBase) GetFlowCtx() *execinfra.FlowCtx {
	return &f.FlowCtx
}

// AddStartable is part of the Flow interface.
func (f *FlowBase) AddStartable(s Startable) {
	f.startables = append(f.startables, s)
}

// GetID is part of the Flow interface.
func (f *FlowBase) GetID() execinfrapb.FlowID {
	return f.ID
}

// CheckInboundStreamID takes a stream ID and returns an error if an inbound
// stream already exists with that ID in the inbound streams map, creating the
// inbound streams map if it is nil.
func (f *FlowBase) CheckInboundStreamID(sid execinfrapb.StreamID) error {
	if _, found := f.inboundStreams[sid]; found {
		return errors.Errorf("inbound stream %d already exists in map", sid)
	}
	if f.inboundStreams == nil {
		f.inboundStreams = make(map[execinfrapb.StreamID]*InboundStreamInfo)
	}
	return nil
}

// GetWaitGroup returns the wait group of this flow.
func (f *FlowBase) GetWaitGroup() *sync.WaitGroup {
	return &f.waitGroup
}

// GetCtxDone returns done channel of the context of this flow.
func (f *FlowBase) GetCtxDone() <-chan struct{} {
	return f.ctxDone
}

// GetCancelFlowFn returns the context cancellation function of the context of
// this flow.
func (f *FlowBase) GetCancelFlowFn() context.CancelFunc {
	return f.ctxCancel
}

// SetProcessors overrides the current f.processors with the provided
// processors. This is used to set up the vectorized flow.
func (f *FlowBase) SetProcessors(processors []execinfra.Processor) {
	f.processors = processors
}

// AddRemoteStream adds a remote stream to this flow.
func (f *FlowBase) AddRemoteStream(streamID execinfrapb.StreamID, streamInfo *InboundStreamInfo) {
	f.inboundStreams[streamID] = streamInfo
}

// GetSyncFlowConsumer returns the special syncFlowConsumer outbox.
func (f *FlowBase) GetSyncFlowConsumer() execinfra.RowReceiver {
	return f.syncFlowConsumer
}

// GetLocalProcessors return the execinfra.LocalProcessors of this flow.
func (f *FlowBase) GetLocalProcessors() []execinfra.LocalProcessor {
	return f.localProcessors
}

// startInternal starts the flow. All processors are started, each in their own
// goroutine. The caller must forward any returned error to syncFlowConsumer if
// set.
func (f *FlowBase) startInternal(
	ctx context.Context, processors []execinfra.Processor, doneFn func(),
) error {
	f.doneFn = doneFn
	log.VEventf(
		ctx, 1, "starting (%d processors, %d startables)", len(processors), len(f.startables),
	)

	// Only register the flow if there will be inbound stream connections that
	// need to look up this flow in the flow registry.
	if !f.IsLocal() {
		// Once we call RegisterFlow, the inbound streams become accessible; we must
		// set up the WaitGroup counter before.
		// The counter will be further incremented below to account for the
		// processors.
		f.waitGroup.Add(len(f.inboundStreams))

		if err := f.flowRegistry.RegisterFlow(
			ctx, f.ID, f, f.inboundStreams, SettingFlowStreamTimeout.Get(&f.FlowCtx.Cfg.Settings.SV),
		); err != nil {
			return err
		}
	}

	f.status = FlowRunning

	if log.V(1) {
		log.Infof(ctx, "registered flow %s", f.ID.Short())
	}
	for _, s := range f.startables {
		s.Start(ctx, &f.waitGroup, f.ctxCancel)
	}
	for i := 0; i < len(processors); i++ {
		f.waitGroup.Add(1)
		go func(i int) {
			processors[i].Run(ctx)
			f.waitGroup.Done()
		}(i)
	}
	f.startedGoroutines = len(f.startables) > 0 || len(processors) > 0 || !f.IsLocal()
	return nil
}

// IsLocal returns whether this flow does not have any remote execution.
func (f *FlowBase) IsLocal() bool {
	return len(f.inboundStreams) == 0
}

// IsTimeSeries returns whether this flow is bounded by time-series execution.
func (f *FlowBase) IsTimeSeries() bool {
	return f.isTimeSeries
}

// IsVectorized returns whether this flow will run with vectorized execution.
func (f *FlowBase) IsVectorized() bool {
	panic("IsVectorized should not be called on FlowBase")
}

// Start is part of the Flow interface.
func (f *FlowBase) Start(ctx context.Context, doneFn func()) error {
	if f.isTimeSeries {
		f.doneFn = doneFn
		if !f.IsLocal() {
			// Once we call RegisterFlow, the inbound streams become accessible; we must
			// set up the WaitGroup counter before.
			// The counter will be further incremented below to account for the
			// processors.
			f.waitGroup.Add(len(f.inboundStreams))

			if err := f.flowRegistry.RegisterFlow(
				ctx, f.ID, f, f.inboundStreams, SettingFlowStreamTimeout.Get(&f.FlowCtx.Cfg.Settings.SV),
			); err != nil {
				return err
			}
		}

		f.status = FlowRunning
		for _, s := range f.startables {
			s.Start(ctx, &f.waitGroup, f.ctxCancel)
		}
		for i := 0; i < len(f.TsTableReaders); i++ {
			f.waitGroup.Add(1)
			go func(i int) {
				f.TsTableReaders[i].RunTS(ctx)
				f.waitGroup.Done()
			}(i)
		}

		for i := 0; i < len(f.processors); i++ {
			f.waitGroup.Add(1)
			go func(i int) {
				f.processors[i].Run(ctx)
				f.waitGroup.Done()
			}(i)
		}
		f.startedGoroutines = len(f.startables) > 0 || len(f.processors) > 0 || !f.IsLocal() || len(f.TsTableReaders) > 0
		return nil
	}
	if err := f.startInternal(ctx, f.processors, doneFn); err != nil {
		// For sync flows, the error goes to the consumer.
		if f.syncFlowConsumer != nil {
			f.syncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
			f.syncFlowConsumer.ProducerDone()
			return nil
		}
		return err
	}
	return nil
}

// Run is part of the Flow interface.
func (f *FlowBase) Run(ctx context.Context, doneFn func()) error {
	defer f.Wait()
	if len(f.processors) == 0 {
		return errors.AssertionFailedf("no processors in flow")
	}

	var headProc execinfra.Processor = f.processors[len(f.processors)-1]
	otherProcs := f.processors[:len(f.processors)-1]
	if f.isTimeSeries {
		if f.isVectorized {
			if f.IsLocal() && f.VecIsShortCircuitForPgEncode(ctx) {
				err := f.VecShortCircuitForPgEncode(ctx)
				return err
			}
		}
		if f.IsShortCircuitForPgEncode() {
			// Output encoding optimization.
			var closed int
			f.TsTableReaders[0].Start(ctx)
			for {
				rev, code, err := f.TsTableReaders[0].NextPgWire()
				if err != nil {
					return err
				}

				if code == -1 {
					closed++
					if closed != f.closes {
						continue
					}
					return nil
				}

				err = headProc.Push(ctx, rev)
				if err != nil {
					return err
				}
			}
		}
		if f.syncFlowConsumer == nil {
			return nil
		}

		if !f.IsLocal() {
			// Once we call RegisterFlow, the inbound streams become accessible; we must
			// set up the WaitGroup counter before.
			// The counter will be further incremented below to account for the
			// processors.
			f.waitGroup.Add(len(f.inboundStreams))

			if err := f.flowRegistry.RegisterFlow(
				ctx, f.ID, f, f.inboundStreams, SettingFlowStreamTimeout.Get(&f.FlowCtx.Cfg.Settings.SV),
			); err != nil {
				return err
			}
		}

		f.status = FlowRunning

		for _, s := range f.startables {
			s.Start(ctx, &f.waitGroup, f.ctxCancel)
		}

		for i := 0; i < len(f.TsTableReaders); i++ {
			f.waitGroup.Add(1)
			go func(i int) {
				f.TsTableReaders[i].RunTS(ctx)
				f.waitGroup.Done()
			}(i)
			f.startedGoroutines = true
		}

		for i := 0; i < len(otherProcs); i++ {
			f.waitGroup.Add(1)
			go func(i int) {
				otherProcs[i].Run(ctx)
				f.waitGroup.Done()
			}(i)
		}

		//f.ReceiveMessageQueue(ctx)
		f.startedGoroutines = f.startedGoroutines || len(f.startables) > 0 || len(otherProcs) > 0 || !f.IsLocal()
		f.rowStats = headProc.Run(ctx)
		return nil
	}

	// We'll take care of the last processor in particular.
	var err error
	if err = f.startInternal(ctx, otherProcs, doneFn); err != nil {
		// For sync flows, the error goes to the consumer.
		if f.syncFlowConsumer != nil {
			f.syncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
			f.syncFlowConsumer.ProducerDone()
			return nil
		}
		return err
	}
	f.rowStats = headProc.Run(ctx)
	return nil
}

// Wait is part of the Flow interface.
func (f *FlowBase) Wait() {
	if !f.startedGoroutines {
		return
	}

	var panicVal interface{}
	if panicVal = recover(); panicVal != nil {
		// If Wait is called as part of stack unwinding during a panic, the flow
		// context must be canceled to ensure that all asynchronous goroutines get
		// the message that they must exit (otherwise we will wait indefinitely).
		f.ctxCancel()
	}
	waitChan := make(chan struct{})

	go func() {
		f.waitGroup.Wait()
		close(waitChan)
	}()

	select {
	case <-f.ctxDone:
		f.cancel()
		<-waitChan
	case <-waitChan:
		// Exit normally
	}
	if panicVal != nil {
		panic(panicVal)
	}
}

// Releasable is an interface for objects than can be Released back into a
// memory pool when finished.
type Releasable interface {
	// Release allows this object to be returned to a memory pool. Objects must
	// not be used after Release is called.
	Release()
}

// Cleanup is part of the Flow interface.
// NOTE: this implements only the shared clean up logic between row-based and
// vectorized flows.
func (f *FlowBase) Cleanup(ctx context.Context) {
	if f.status == FlowFinished {
		panic("flow cleanup called twice")
	}

	// This closes the monitor opened in ServerImpl.setupFlow.
	f.EvalCtx.Stop(ctx)
	for _, p := range f.processors {
		if d, ok := p.(Releasable); ok {
			d.Release()
		}
	}
	if log.V(1) {
		log.Infof(ctx, "cleaning up")
	}
	sp := opentracing.SpanFromContext(ctx)
	// Local flows do not get registered.
	if !f.IsLocal() && f.status != FlowNotStarted {
		f.flowRegistry.UnregisterFlow(f.ID)
	}
	f.status = FlowFinished
	f.ctxCancel()
	if f.doneFn != nil {
		f.doneFn()
	}
	if sp != nil {
		sp.Finish()
	}
}

// cancel iterates through all unconnected streams of this flow and marks them canceled.
// This function is called in Wait() after the associated context has been canceled.
// In order to cancel a flow, call f.ctxCancel() instead of this function.
//
// For a detailed description of the distsql query cancellation mechanism,
// read docs/RFCS/query_cancellation.md.
func (f *FlowBase) cancel() {
	// If the flow is local, there are no inbound streams to cancel.
	if f.IsLocal() {
		return
	}
	f.flowRegistry.Lock()
	timedOutReceivers := f.flowRegistry.cancelPendingStreamsLocked(f.ID)
	f.flowRegistry.Unlock()

	for _, receiver := range timedOutReceivers {
		go func(receiver InboundStreamHandler) {
			// Stream has yet to be started; send an error to its
			// receiver and prevent it from being connected.
			receiver.Timeout(sqlbase.QueryCanceledError)
		}(receiver)
	}
}
