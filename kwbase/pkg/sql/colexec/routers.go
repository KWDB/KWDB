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

package colexec

import (
	"context"
	"fmt"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// routerOutput is an interface implemented by router outputs. It exists for
// easier test mocking of outputs.
type routerOutput interface {
	execinfra.OpNode
	// addBatch adds the elements specified by the selection vector from batch to
	// the output. It returns whether or not the output changed its state to
	// blocked (see implementations).
	addBatch(context.Context, coldata.Batch, []int) bool
	// cancel tells the output to stop producing batches.
	cancel(ctx context.Context)
	// drain drains the output of any metadata.
	drain() []execinfrapb.ProducerMetadata
}

// getDefaultRouterOutputBlockedThreshold returns the number of unread values
// buffered by the routerOutputOp after which the output is considered blocked.
// It is a function rather than a variable so that in tests we could modify
// coldata.BatchSize() (if it were a variable, then its value would be
// evaluated before we set the desired batch size).
func getDefaultRouterOutputBlockedThreshold() int {
	return coldata.BatchSize() * 2
}

type routerOutputOpState int

const (
	// routerOutputOpRunning is the state in which routerOutputOp operates
	// normally. The router output transitions into the draining state when
	// either it is finished (when a zero-length batch was added or when it was
	// canceled) or it encounters an error.
	routerOutputOpRunning routerOutputOpState = iota
	// routerOutputOpDraining is the state in which routerOutputOp always
	// returns zero-length batches on calls to Next.
	routerOutputOpDraining
)

type routerOutputOp struct {
	// input is a reference to our router.
	input execinfra.OpNode

	types []coltypes.T

	// unblockedEventsChan is signaled when a routerOutput changes state from
	// blocked to unblocked.
	unblockedEventsChan chan<- struct{}

	mu struct {
		syncutil.Mutex
		state routerOutputOpState
		// unlimitedAllocator tracks the memory usage of this router output,
		// providing a signal for when it should spill to disk.
		// The memory lifecycle is as follows:
		//
		// o.mu.pendingBatch is allocated as a "staging" area. Tuples are copied
		// into it in addBatch.
		// A read may come in in this state, in which case pendingBatch is returned
		// and references to it are removed. Since batches are unsafe for reuse,
		// the batch is also manually released from the allocator.
		// If a read does not come in and the batch becomes full of tuples, that
		// batch is stored in o.mu.data, which is a queue with an in-memory circular
		// buffer backed by disk. If the batch fits in memory, a reference to it
		// is retained and a new pendingBatch is allocated.
		//
		// If a read comes in at this point, the batch is dequeued from o.mu.data
		// and returned, but the memory is still accounted for. In fact, memory use
		// increases up to when o.mu.data is full and must spill to disk.
		// Once it spills to disk, the spillingQueue (o.mu.data), will release
		// batches it spills to disk to stop accounting for them.
		// The tricky part comes when o.mu.data is dequeued from. In this case, the
		// reference for a previously-returned batch is overwritten with an on-disk
		// batch, so the memory for the overwritten batch is released, while the
		// new batch's memory is retained. Note that if batches are being dequeued
		// from disk, it must be the case that the circular buffer is now empty,
		// holding references to batches that have been previously returned.
		//
		// In short, batches whose references are retained are also retained in the
		// allocator, but if any references are overwritten or lost, those batches
		// are released.
		unlimitedAllocator *Allocator
		cond               *sync.Cond
		// pendingBatch is a partially-filled batch with data added through
		// addBatch. Once this batch reaches capacity, it is flushed to data. The
		// main use of pendingBatch is coalescing various fragmented batches into
		// one.
		pendingBatch coldata.Batch
		// data is a spillingQueue, a circular buffer backed by a disk queue.
		data      *spillingQueue
		numUnread int
		blocked   bool

		drainState struct {
			// err stores any error (if such occurs) when dequeueing from data
			// (one possible error could be hitting disk usage limit). Once
			// such error occurs, the output transitions into draining state.
			// The error, however, will not be propagated right away - in order to
			// prevent double reporting of the error by the hash router, it will be
			// returned either on the next call to addBatch (if such occurs) or
			// during draining of the router output.
			err error
		}
	}

	testingKnobs struct {
		// alwaysFlush, if set to true, will always flush o.mu.pendingBatch to
		// o.mu.data.
		alwaysFlush bool
	}

	// These fields default to defaultRouterOutputBlockedThreshold and
	// coldata.BatchSize() but are modified by tests to test edge cases.
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked.
	blockedThreshold int
	outputBatchSize  int
}

func (o *routerOutputOp) ChildCount(verbose bool) int {
	return 1
}

func (o *routerOutputOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return o.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ Operator = &routerOutputOp{}

// newRouterOutputOp creates a new router output. The caller must ensure that
// unblockedEventsChan is a buffered channel, as the router output will write to
// it. The provided allocator must not have a hard limit. The passed in
// memoryLimit will act as a soft limit to allow the router output to use disk
// when it is exceeded.
func newRouterOutputOp(
	unlimitedAllocator *Allocator,
	types []coltypes.T,
	unblockedEventsChan chan<- struct{},
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
) *routerOutputOp {
	return newRouterOutputOpWithBlockedThresholdAndBatchSize(unlimitedAllocator, types, unblockedEventsChan, memoryLimit, cfg, fdSemaphore, getDefaultRouterOutputBlockedThreshold(), coldata.BatchSize(), diskAcc)
}

func newRouterOutputOpWithBlockedThresholdAndBatchSize(
	unlimitedAllocator *Allocator,
	types []coltypes.T,
	unblockedEventsChan chan<- struct{},
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	blockedThreshold int,
	outputBatchSize int,
	diskAcc *mon.BoundAccount,
) *routerOutputOp {
	o := &routerOutputOp{
		types:               types,
		unblockedEventsChan: unblockedEventsChan,
		blockedThreshold:    blockedThreshold,
		outputBatchSize:     outputBatchSize,
	}
	o.mu.unlimitedAllocator = unlimitedAllocator
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = newSpillingQueue(unlimitedAllocator, types, memoryLimit, cfg, fdSemaphore, outputBatchSize, diskAcc)

	return o
}

func (o *routerOutputOp) Init() {}

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next
// is designed for only one concurrent caller and will block until data is
// ready.
func (o *routerOutputOp) Next(ctx context.Context) coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	for o.mu.state == routerOutputOpRunning && o.mu.pendingBatch == nil && o.mu.data.empty() {
		// Wait until there is data to read or the output is canceled.
		o.mu.cond.Wait()
	}
	if o.mu.state == routerOutputOpDraining {
		return coldata.ZeroBatch
	}
	var b coldata.Batch
	if o.mu.pendingBatch != nil && o.mu.data.empty() {
		// o.mu.data is empty (i.e. nothing has been flushed to the spillingQueue),
		// but there is a o.mu.pendingBatch that has not been flushed yet. Return
		// this batch directly.
		b = o.mu.pendingBatch
		o.mu.unlimitedAllocator.ReleaseBatch(b)
		o.mu.pendingBatch = nil
	} else {
		b, o.mu.drainState.err = o.mu.data.dequeue(ctx)
		if o.mu.drainState.err != nil {
			o.mu.state = routerOutputOpDraining
			return coldata.ZeroBatch
		}
	}
	o.mu.numUnread -= b.Length()
	if o.mu.numUnread <= o.blockedThreshold {
		o.maybeUnblockLocked()
	}
	if b.Length() == 0 {
		// This is the last batch. closeLocked will set done to protect against
		// further calls to Next since this is allowed by the interface as well as
		// cleaning up and releasing possible disk infrastructure.
		o.closeLocked(ctx)
	}
	return b
}

func (o *routerOutputOp) closeLocked(ctx context.Context) {
	o.mu.state = routerOutputOpDraining
	if err := o.mu.data.close(ctx); err != nil {
		// This log message is Info instead of Warning because the flow will also
		// attempt to clean up the parent directory, so this failure might not have
		// any effect.
		log.Infof(ctx, "error closing vectorized hash router output, files may be left over: %s", err)
	}
}

// cancel wakes up a reader in Next if there is one and results in the output
// returning zero length batches for every Next call after cancel. Note that
// all accumulated data that hasn't been read will not be returned.
func (o *routerOutputOp) cancel(ctx context.Context) {
	o.mu.Lock()
	o.closeLocked(ctx)
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
	o.mu.Unlock()
}

// addBatch copies the columns in batch according to selection into an internal
// buffer.
// The routerOutputOp only adds the elements specified by selection. Therefore,
// an empty selection slice will add no elements. Note that the selection vector
// on the batch is ignored. This is so that callers of addBatch can push the
// same batch with different selection vectors to many different outputs.
// True is returned if the the output changes state to blocked (note: if the
// output is already blocked, false is returned).
// TODO(asubiotto): We should explore pipelining addBatch if disk-spilling
//  performance becomes a concern. The main router goroutine will be writing to
//  disk as the code is written, meaning that we impact the performance of
//  writing rows to a fast output if we have to write to disk for a single
//  slow output.
func (o *routerOutputOp) addBatch(ctx context.Context, batch coldata.Batch, selection []int) bool {
	if len(selection) > batch.Length() {
		selection = selection[:batch.Length()]
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.state == routerOutputOpDraining {
		// We have encountered an error in Next() but have not yet propagated
		// it, so we do so now - the error will get to the hash router which
		// will cancel all of its outputs.
		err := o.mu.drainState.err
		// We set the error to nil so that it is not propagated again, during
		// drain() call.
		o.mu.drainState.err = nil
		execerror.VectorizedInternalPanic(err)
	}
	if batch.Length() == 0 {
		if o.mu.pendingBatch != nil {
			if err := o.mu.data.enqueue(ctx, o.mu.pendingBatch); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
		}
		o.mu.pendingBatch = coldata.ZeroBatch
		o.mu.cond.Signal()
		return false
	}

	if len(selection) == 0 {
		// Non-zero batch with no selection vector. Nothing to do.
		return false
	}

	// Increment o.mu.numUnread before going into the loop, as we will consume
	// selection.
	o.mu.numUnread += len(selection)

	for toAppend := len(selection); toAppend > 0; {
		if o.mu.pendingBatch == nil {
			o.mu.pendingBatch = o.mu.unlimitedAllocator.NewMemBatchWithSize(o.types, o.outputBatchSize)
		}
		available := o.outputBatchSize - o.mu.pendingBatch.Length()
		numAppended := toAppend
		if toAppend > available {
			numAppended = available
		}
		o.mu.unlimitedAllocator.PerformOperation(o.mu.pendingBatch.ColVecs(), func() {
			for i, t := range o.types {
				o.mu.pendingBatch.ColVec(i).Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							ColType:   t,
							Src:       batch.ColVec(i),
							Sel:       selection[:numAppended],
							DestIdx:   o.mu.pendingBatch.Length(),
							SrcEndIdx: numAppended,
						},
					},
				)
			}
		})
		newLength := o.mu.pendingBatch.Length() + numAppended
		o.mu.pendingBatch.SetLength(newLength)
		if o.testingKnobs.alwaysFlush || newLength >= o.outputBatchSize {
			// The capacity in o.mu.pendingBatch has been filled.
			if err := o.mu.data.enqueue(ctx, o.mu.pendingBatch); err != nil {
				execerror.VectorizedInternalPanic(err)
			}
			o.mu.pendingBatch = nil
		}
		toAppend -= numAppended
		selection = selection[numAppended:]
	}

	stateChanged := false
	if o.mu.numUnread > o.blockedThreshold && !o.mu.blocked {
		// The output is now blocked.
		o.mu.blocked = true
		stateChanged = true
	}
	o.mu.cond.Signal()
	return stateChanged
}

// maybeUnblockLocked unblocks the router output if it is in a blocked state. If the
// output was previously in a blocked state, an event will be sent on
// routerOutputOp.unblockedEventsChan.
func (o *routerOutputOp) maybeUnblockLocked() {
	if o.mu.blocked {
		o.mu.blocked = false
		o.unblockedEventsChan <- struct{}{}
	}
}

func (o *routerOutputOp) drain() []execinfrapb.ProducerMetadata {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.drainState.err != nil {
		err := o.mu.drainState.err
		o.mu.drainState.err = nil
		return []execinfrapb.ProducerMetadata{{Err: err}}
	}
	return nil
}

// reset resets the routerOutputOp for a benchmark run.
func (o *routerOutputOp) reset(ctx context.Context) {
	o.mu.Lock()
	o.mu.state = routerOutputOpRunning
	o.mu.data.reset(ctx)
	o.mu.drainState.err = nil
	o.mu.numUnread = 0
	o.mu.blocked = false
	o.mu.Unlock()
}

// HashRouter hashes values according to provided hash columns and computes a
// destination for each row. These destinations are exposed as Operators
// returned by the constructor.
type HashRouter struct {
	OneInputNode
	// types are the input coltypes.
	types []coltypes.T
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []uint32

	// One output for each stream.
	outputs []routerOutput
	// closers is a slice of IdempotentClosers that need to be closed when the
	// hash router terminates.
	closers []IdempotentCloser

	// unblockedEventsChan is a channel shared between the HashRouter and its
	// outputs. outputs send events on this channel when they are unblocked by a
	// read.
	unblockedEventsChan <-chan struct{}
	numBlockedOutputs   int

	mu struct {
		syncutil.Mutex
		bufferedMeta []execinfrapb.ProducerMetadata
	}

	// tupleDistributor is used to decide to which output a particular tuple
	// should be routed.
	tupleDistributor *tupleHashDistributor
}

// NewHashRouter creates a new hash router that consumes coldata.Batches from
// input and hashes each row according to hashCols to one of the outputs
// returned as Operators.
// The number of allocators provided will determine the number of outputs
// returned. Note that each allocator must be unlimited, memory will be limited
// by comparing memory use in the allocator with the memoryLimit argument. Each
// Operator must have an independent allocator (this means that each allocator
// should be linked to an independent mem account) as Operator.Next will usually
// be called concurrently between different outputs. Similarly, each output
// needs to have a separate disk account.
func NewHashRouter(
	unlimitedAllocators []*Allocator,
	input Operator,
	types []coltypes.T,
	hashCols []uint32,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAccounts []*mon.BoundAccount,
	toClose []IdempotentCloser,
) (*HashRouter, []Operator) {
	if diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		execerror.VectorizedInternalPanic(errors.Errorf("hash router instantiated with incompatible disk queue cache mode: %d", diskQueueCfg.CacheMode))
	}
	outputs := make([]routerOutput, len(unlimitedAllocators))
	outputsAsOps := make([]Operator, len(unlimitedAllocators))
	// unblockEventsChan is buffered to 2*numOutputs as we don't want the outputs
	// writing to it to block.
	// Unblock events only happen after a corresponding block event. Since these
	// are state changes and are done under lock (including the output sending
	// on the channel, which is why we want the channel to be buffered in the
	// first place), every time the HashRouter blocks an output, it *must* read
	// all unblock events preceding it since these *must* be on the channel.
	unblockEventsChan := make(chan struct{}, 2*len(unlimitedAllocators))
	memoryLimitPerOutput := memoryLimit / int64(len(unlimitedAllocators))
	for i := range unlimitedAllocators {
		op := newRouterOutputOp(unlimitedAllocators[i], types, unblockEventsChan, memoryLimitPerOutput, diskQueueCfg, fdSemaphore, diskAccounts[i])
		outputs[i] = op
		outputsAsOps[i] = op
	}
	router := newHashRouterWithOutputs(input, types, hashCols, unblockEventsChan, outputs, toClose)
	for i := range outputs {
		outputs[i].(*routerOutputOp).input = router
	}
	return router, outputsAsOps
}

func newHashRouterWithOutputs(
	input Operator,
	types []coltypes.T,
	hashCols []uint32,
	unblockEventsChan <-chan struct{},
	outputs []routerOutput,
	toClose []IdempotentCloser,
) *HashRouter {
	r := &HashRouter{
		OneInputNode:        NewOneInputNode(input),
		types:               types,
		hashCols:            hashCols,
		outputs:             outputs,
		closers:             toClose,
		unblockedEventsChan: unblockEventsChan,
		tupleDistributor:    newTupleHashDistributor(defaultInitHashValue, len(outputs)),
	}
	return r
}

// Run runs the HashRouter. Batches are read from the input and pushed to an
// output calculated by hashing columns. Cancel the given context to terminate
// early.
func (r *HashRouter) Run(ctx context.Context) {
	bufferErr := func(err error) {
		r.mu.Lock()
		r.mu.bufferedMeta = append(r.mu.bufferedMeta, execinfrapb.ProducerMetadata{Err: err})
		r.mu.Unlock()
	}
	defer func() {
		for _, closer := range r.closers {
			if err := closer.IdempotentClose(ctx); err != nil {
				if log.V(1) {
					log.Infof(ctx, "error closing IdempotentCloser: %v", err)
				}
			}
		}
	}()
	// Since HashRouter runs in a separate goroutine, we want to be safe and
	// make sure that we catch errors in all code paths, so we wrap the whole
	// method with a catcher. Note that we also have "internal" catchers as
	// well for more fine-grained control of error propagation.
	if err := execerror.CatchVectorizedRuntimeError(func() {
		r.input.Init()
		// cancelOutputs buffers non-nil error as metadata, cancels all of the
		// outputs additionally buffering any error if such occurs during the
		// outputs' cancellation as metadata as well. Note that it attempts to
		// cancel every output regardless of whether "previous" output's
		// cancellation succeeds.
		cancelOutputs := func(err error) {
			if err != nil {
				bufferErr(err)
			}
			for _, o := range r.outputs {
				if err := execerror.CatchVectorizedRuntimeError(func() {
					o.cancel(ctx)
				}); err != nil {
					bufferErr(err)
				}
			}
		}
		var done bool
		processNextBatch := func() {
			done = r.processNextBatch(ctx)
		}
		for {
			// Check for cancellation.
			select {
			case <-ctx.Done():
				cancelOutputs(ctx.Err())
				return
			default:
			}

			// Read all the routerOutput state changes that have happened since the
			// last iteration.
			for moreToRead := true; moreToRead; {
				select {
				case <-r.unblockedEventsChan:
					r.numBlockedOutputs--
				default:
					// No more routerOutput state changes to read without blocking.
					moreToRead = false
				}
			}

			if r.numBlockedOutputs == len(r.outputs) {
				// All outputs are blocked, wait until at least one output is unblocked.
				select {
				case <-r.unblockedEventsChan:
					r.numBlockedOutputs--
				case <-ctx.Done():
					cancelOutputs(ctx.Err())
					return
				}
			}

			if err := execerror.CatchVectorizedRuntimeError(processNextBatch); err != nil {
				cancelOutputs(err)
				return
			}
			if done {
				// The input was done and we have notified the routerOutputs that there
				// is no more data.
				return
			}
		}
	}); err != nil {
		bufferErr(err)
	}
}

// processNextBatch reads the next batch from its input, hashes it and adds
// each column to its corresponding output, returning whether the input is
// done.
func (r *HashRouter) processNextBatch(ctx context.Context) bool {
	b := r.input.Next(ctx)
	n := b.Length()
	if n == 0 {
		// Done. Push an empty batch to outputs to tell them the data is done as
		// well.
		for _, o := range r.outputs {
			o.addBatch(ctx, b, nil)
		}
		return true
	}

	selections := r.tupleDistributor.distribute(ctx, b, r.types, r.hashCols)
	for i, o := range r.outputs {
		if o.addBatch(ctx, b, selections[i]) {
			// This batch blocked the output.
			r.numBlockedOutputs++
		}
	}
	return false
}

// reset resets the HashRouter for a benchmark run.
func (r *HashRouter) reset(ctx context.Context) {
	if i, ok := r.input.(resetter); ok {
		i.reset(ctx)
	}
	r.numBlockedOutputs = 0
	for moreToRead := true; moreToRead; {
		select {
		case <-r.unblockedEventsChan:
		default:
			moreToRead = false
		}
	}
	for _, o := range r.outputs {
		o.(resetter).reset(ctx)
	}
}

// DrainMeta is part of the MetadataGenerator interface.
func (r *HashRouter) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, o := range r.outputs {
		r.mu.bufferedMeta = append(r.mu.bufferedMeta, o.drain()...)
	}
	meta := r.mu.bufferedMeta
	r.mu.bufferedMeta = r.mu.bufferedMeta[:0]
	return meta
}
