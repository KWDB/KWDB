// Copyright 2016 The Cockroach Authors.
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

package execinfra

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// RowChannelBufSize is the default buffer size of a RowChannel.
const RowChannelBufSize = 16

// ConsumerStatus is the type returned by RowReceiver.Push(), informing a
// producer of a consumer's state.
type ConsumerStatus uint32

//go:generate stringer -type=ConsumerStatus

const (
	// NeedMoreRows indicates that the consumer is still expecting more rows.
	NeedMoreRows ConsumerStatus = iota
	// DrainRequested indicates that the consumer will not process any more data
	// rows, but will accept trailing metadata from the producer.
	DrainRequested
	// ConsumerClosed indicates that the consumer will not process any more data
	// rows or metadata. This is also commonly returned in case the consumer has
	// encountered an error.
	ConsumerClosed
)

// RowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type RowReceiver interface {
	// Push sends a record to the consumer of this RowReceiver. Exactly one of the
	// row/meta must be specified (i.e. either row needs to be non-nil or meta
	// needs to be non-Empty()). May block.
	//
	// The return value indicates the current status of the consumer. Depending on
	// it, producers are expected to drain or shut down. In all cases,
	// ProducerDone() needs to be called (after draining is done, if draining was
	// requested).
	//
	// Unless specifically permitted by the underlying implementation, (see
	// copyingRowReceiver, for example), the sender must not modify the row
	// and the metadata after calling this function.
	//
	// After DrainRequested is returned, it is expected that all future calls only
	// carry metadata (however that is not enforced and implementations should be
	// prepared to discard non-metadata rows). If ConsumerClosed is returned,
	// implementations have to ignore further calls to Push() (such calls are
	// allowed because there might be multiple producers for a single RowReceiver
	// and they might not all be aware of the last status returned).
	//
	// Implementations of Push() must be thread-safe.
	Push(row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata) ConsumerStatus
	// PushPGResult sends a pg record to the consumer of this RowReceiver.
	PushPGResult(ctx context.Context, res []byte) error

	// AddPGComplete adds a pg complete flag.
	AddPGComplete(cmd string, typ tree.StatementType, rowsAffected int)

	// Types returns the types of the EncDatumRow that this RowReceiver expects
	// to be pushed.
	Types() []types.T

	// ProducerDone is called when the producer has pushed all the rows and
	// metadata; it causes the RowReceiver to process all rows and clean up.
	//
	// ProducerDone() cannot be called concurrently with Push(), and after it
	// is called, no other method can be called.
	ProducerDone()
	GetCols() int

	// AddStats record stallTime and number of rows
	AddStats(time time.Duration, isAddRows bool)

	// GetStats get RowStats
	GetStats() RowStats
}

// RowSource is any component of a flow that produces rows that can be consumed
// by another component.
//
// Communication components generally (e.g. RowBuffer, RowChannel) implement
// this interface. Some processors also implement it (in addition to
// implementing the Processor interface) - in which case those
// processors can be "fused" with their consumer (i.e. run in the consumer's
// goroutine).
type RowSource interface {
	// OutputTypes returns the schema for the rows in this source.
	OutputTypes() []types.T

	// Start prepares the RowSource for future Next() calls and takes in the
	// context in which these future calls should operate. Start needs to be
	// called before Next/ConsumerDone/ConsumerClosed.
	//
	// RowSources that consume other RowSources are expected to Start() their
	// inputs.
	//
	// Implementations are expected to hold on to the provided context. They may
	// choose to derive and annotate it (Processors generally do). For convenience,
	// the possibly updated context is returned.
	Start(context.Context) context.Context

	// Next returns the next record from the source. At most one of the return
	// values will be non-empty. Both of them can be empty when the RowSource has
	// been exhausted - no more records are coming and any further method calls
	// will be no-ops.
	//
	// EncDatumRows returned by Next() are only valid until the next call to
	// Next(), although the EncDatums inside them stay valid forever.
	//
	// A ProducerMetadata record may contain an error. In that case, this
	// interface is oblivious about the semantics: implementers may continue
	// returning different rows on future calls, or may return an empty record
	// (thus asking the consumer to stop asking for rows). In particular,
	// implementers are not required to only return metadata records from this
	// point on (which means, for example, that they're not required to
	// automatically ask every producer to drain, in case there's multiple
	// producers). Therefore, consumers need to be aware that some rows might have
	// been skipped in case they continue to consume rows. Usually a consumer
	// should react to an error by calling ConsumerDone(), thus asking the
	// RowSource to drain, and separately discard any future data rows. A consumer
	// receiving an error should also call ConsumerDone() on any other input it
	// has.
	Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata)

	// ConsumerDone lets the source know that we will not need any more data
	// rows. The source is expected to start draining and only send metadata
	// rows. May be called multiple times on a RowSource, even after
	// ConsumerClosed has been called.
	//
	// May block. If the consumer of the source stops consuming rows before
	// Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerDone()

	// ConsumerClosed informs the source that the consumer is done and will not
	// make any more calls to Next(). Must only be called once on a given
	// RowSource.
	//
	// Like ConsumerDone(), if the consumer of the source stops consuming rows
	// before Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerClosed()

	// InitProcessorProcedure init processor in procedure
	InitProcessorProcedure(txn *kv.Txn)
}

// RowSourcedProcessor is the union of RowSource and Processor.
type RowSourcedProcessor interface {
	RowSource
	Run(context.Context) RowStats
	RunTS(ctx context.Context)
	Push(ctx context.Context, res []byte) error
	Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata)
	NextPgWire() (val []byte, code int, err error)
	IsShortCircuitForPgEncode() bool
	SupportPgWire() bool
	Start(ctx context.Context) context.Context
}

// Run reads records from the source and outputs them to the receiver, properly
// draining the source of metadata and closing both the source and receiver.
//
// src needs to have been Start()ed before calling this.
func Run(ctx context.Context, src RowSource, dst RowReceiver, startTime time.Time) {
	defer func() {
		if p := recover(); p != nil {
			err := errors.Errorf("%v", p)
			if err.Error() == "" {
				// for test.
				panic(p)
			}
			metaErr := &execinfrapb.ProducerMetadata{Err: err}
			dst.Push(nil, metaErr)
			HandleConsumerClosed(src, dst, startTime, nil)
			return
		}
	}()
	for {
		row, meta := src.Next()
		// Emit the row; stop if no more rows are needed.
		if row != nil || meta != nil {
			switch dst.Push(row, meta) {
			case NeedMoreRows:
				dst.AddStats(0, row != nil)
				continue
			case DrainRequested:
				DrainAndForwardMetadata(ctx, src, dst)
				dst.ProducerDone()
				dst.AddStats(timeutil.Since(startTime), row != nil)
				return
			case ConsumerClosed:
				HandleConsumerClosed(src, dst, startTime, row)
				return
			}
		}
		// row == nil && meta == nil: the source has been fully drained.
		dst.ProducerDone()
		dst.AddStats(timeutil.Since(startTime), row != nil)
		return
	}
}

// HandleConsumerClosed handles consumer status
func HandleConsumerClosed(
	src RowSource, dst RowReceiver, startTime time.Time, row sqlbase.EncDatumRow,
) {
	src.ConsumerClosed()
	dst.ProducerDone()
	dst.AddStats(timeutil.Since(startTime), row != nil)
}

// Releasable is an interface for objects than can be Released back into a
// memory pool when finished.
type Releasable interface {
	// Release allows this object to be returned to a memory pool. Objects must
	// not be used after Release is called.
	Release()
}

// DrainAndForwardMetadata calls src.ConsumerDone() (thus asking src for
// draining metadata) and then forwards all the metadata to dst.
//
// When this returns, src has been properly closed (regardless of the presence
// or absence of an error). dst, however, has not been closed; someone else must
// call dst.ProducerDone() when all producers have finished draining.
//
// It is OK to call DrainAndForwardMetadata() multiple times concurrently on the
// same dst (as RowReceiver.Push() is guaranteed to be thread safe).
func DrainAndForwardMetadata(ctx context.Context, src RowSource, dst RowReceiver) {
	src.ConsumerDone()
	for {
		row, meta := src.Next()
		if meta == nil {
			if row == nil {
				return
			}
			continue
		}
		if row != nil {
			log.Fatalf(
				ctx, "both row data and metadata in the same record. row: %s meta: %+v",
				row.String(src.OutputTypes()), meta,
			)
		}

		switch dst.Push(nil /* row */, meta) {
		case ConsumerClosed:
			src.ConsumerClosed()
			return
		case NeedMoreRows:
		case DrainRequested:
		}
	}
}

// GetTraceData returns the trace data.
func GetTraceData(ctx context.Context) []tracing.RecordedSpan {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		return tracing.GetRecording(sp)
	}
	return nil
}

// SendTraceData collects the tracing information from the ctx and pushes it to
// dst. The ConsumerStatus returned by dst is ignored.
//
// Note that the tracing data is distinct between different processors, since
// each one gets its own trace "recording group".
func SendTraceData(ctx context.Context, dst RowReceiver) {
	if rec := GetTraceData(ctx); rec != nil {
		dst.Push(nil /* row */, &execinfrapb.ProducerMetadata{TraceData: rec})
	}
}

// GetLeafTxnFinalState returns the txn metadata from a transaction if
// it is present and the transaction is a leaf transaction. It returns
// nil when called on a Root. This is done as a convenience allowing
// DistSQL processors to be oblivious about whether they're running in
// a Leaf or a Root.
//
// NOTE(andrei): As of 04/2018, the txn is shared by all processors scheduled on
// a node, and so it's possible for multiple processors to send the same
// LeafTxnFinalState. The root TxnCoordSender doesn't care if it receives the same
// thing multiple times.
func GetLeafTxnFinalState(ctx context.Context, txn *kv.Txn) *roachpb.LeafTxnFinalState {
	if txn.Type() != kv.LeafTxn {
		return nil
	}
	txnMeta, err := txn.GetLeafTxnFinalState(ctx)
	if err != nil {
		// TODO(knz): plumb errors through the callers.
		panic(errors.Wrap(err, "in execinfra.GetLeafTxnFinalState"))
	}

	if txnMeta.Txn.ID == uuid.Nil {
		return nil
	}
	return &txnMeta
}

// DrainAndClose is a version of DrainAndForwardMetadata that drains multiple
// sources. These sources are assumed to be the only producers left for dst, so
// dst is closed once they're all exhausted (this is different from
// DrainAndForwardMetadata).
//
// If cause is specified, it is forwarded to the consumer before all the drain
// metadata. This is intended to have been the error, if any, that caused the
// draining.
//
// pushTrailingMeta is called after draining the sources and before calling
// dst.ProducerDone(). It gives the caller the opportunity to push some trailing
// metadata (e.g. tracing information and txn updates, if applicable).
//
// srcs can be nil.
//
// All errors are forwarded to the producer.
func DrainAndClose(
	ctx context.Context,
	dst RowReceiver,
	cause error,
	pushTrailingMeta func(context.Context),
	srcs ...RowSource,
) {
	if cause != nil {
		// We ignore the returned ConsumerStatus and rely on the
		// DrainAndForwardMetadata() calls below to close srcs in all cases.
		_ = dst.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: cause})
	}
	if len(srcs) > 0 {
		var wg sync.WaitGroup
		for _, input := range srcs[1:] {
			wg.Add(1)
			go func(input RowSource) {
				DrainAndForwardMetadata(ctx, input, dst)
				wg.Done()
			}(input)
		}
		DrainAndForwardMetadata(ctx, srcs[0], dst)
		wg.Wait()
	}
	pushTrailingMeta(ctx)
	dst.ProducerDone()
}

// NoMetadataRowSource is a wrapper on top of a RowSource that automatically
// forwards metadata to a RowReceiver. Data rows are returned through an
// interface similar to RowSource, except that, since metadata is taken care of,
// only the data rows are returned.
//
// The point of this struct is that it'd be burdensome for some row consumers to
// have to deal with metadata.
type NoMetadataRowSource struct {
	src          RowSource
	metadataSink RowReceiver
}

// MakeNoMetadataRowSource builds a NoMetadataRowSource.
func MakeNoMetadataRowSource(src RowSource, sink RowReceiver) NoMetadataRowSource {
	return NoMetadataRowSource{src: src, metadataSink: sink}
}

// NextRow is analogous to RowSource.Next. If the producer sends an error, we
// can't just forward it to metadataSink. We need to let the consumer know so
// that it's not under the impression that everything is hunky-dory and it can
// continue consuming rows. So, this interface returns the error. Just like with
// a raw RowSource, the consumer should generally call ConsumerDone() and drain.
func (rs *NoMetadataRowSource) NextRow() (sqlbase.EncDatumRow, error) {
	for {
		row, meta := rs.src.Next()
		if meta == nil {
			return row, nil
		}
		if meta.Err != nil {
			return nil, meta.Err
		}
		// We forward the metadata and ignore the returned ConsumerStatus. There's
		// no good way to use that status here; eventually the consumer of this
		// NoMetadataRowSource will figure out the same status and act on it as soon
		// as a non-metadata row is received.
		_ = rs.metadataSink.Push(nil /* row */, meta)
	}
}

// RowChannelMsg is the message used in the channels that implement
// local physical streams (i.e. the RowChannel's).
type RowChannelMsg struct {
	// Only one of these fields will be set.
	Row  sqlbase.EncDatumRow
	Meta *execinfrapb.ProducerMetadata
}

// rowSourceBase provides common functionality for RowSource implementations
// that need to track consumer status. It is intended to be used by RowSource
// implementations into which data is pushed by a producer async, as opposed to
// RowSources that pull data synchronously from their inputs, which don't need
// to deal with concurrent calls to ConsumerDone() / ConsumerClosed()).
// Things like the RowChannel falls in the first category; processors generally
// fall in the latter.
type rowSourceBase struct {
	// ConsumerStatus is an atomic used in implementation of the
	// RowSource.Consumer{Done,Closed} methods to signal that the consumer is
	// done accepting rows or is no longer accepting data.
	ConsumerStatus ConsumerStatus
}

// consumerDone helps processors implement RowSource.ConsumerDone.
func (rb *rowSourceBase) consumerDone() {
	atomic.CompareAndSwapUint32((*uint32)(&rb.ConsumerStatus),
		uint32(NeedMoreRows), uint32(DrainRequested))
}

// consumerClosed helps processors implement RowSource.ConsumerClosed. The name
// is only used for debug messages.
func (rb *rowSourceBase) consumerClosed(name string) {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if status == ConsumerClosed {
		log.ReportOrPanic(context.Background(), nil, "%s already closed", log.Safe(name))
	}
	atomic.StoreUint32((*uint32)(&rb.ConsumerStatus), uint32(ConsumerClosed))
}

// RowChannel is a thin layer over a RowChannelMsg channel, which can be used to
// transfer rows between goroutines.
type RowChannel struct {
	rowSourceBase

	types []types.T

	// The channel on which rows are delivered.
	C <-chan RowChannelMsg

	// dataChan is the same channel as C.
	dataChan chan RowChannelMsg

	// numSenders is an atomic counter that keeps track of how many senders have
	// yet to call ProducerDone().
	numSenders int32

	Ctx context.Context

	StreamID int32

	RevChan  chan []byte
	SendChan *chan int
	ErrChan  chan *execinfrapb.ProducerMetadata
	Rev      []byte

	Done bool

	IsTimeSeries bool
	NotVirtual   bool

	// Is the receiving of local timing data completed for LocalRevEnding
	LocalRevEnding bool

	// Single machine timing query, in order to reduce one channel pass
	TsTableReader Processor
}

// AddStats record stallTime and number of rows
func (rc *RowChannel) AddStats(time time.Duration, isAddRows bool) {}

// GetStats get RowStats
func (rc *RowChannel) GetStats() RowStats {
	return RowStats{}
}

// GetCols is part of the distsql.RowReceiver interface.
func (rc *RowChannel) GetCols() int {
	//TODO unimplemented
	panic("unimplemented")
}

var _ RowReceiver = &RowChannel{}
var _ RowSource = &RowChannel{}

// InitWithNumSenders initializes the RowChannel with the default buffer size.
// numSenders is the number of producers that will be pushing to this channel.
// RowChannel will not be closed until it receives numSenders calls to
// ProducerDone().
func (rc *RowChannel) InitWithNumSenders(types []types.T, numSenders int) {
	rc.InitWithBufSizeAndNumSenders(types, RowChannelBufSize, numSenders)
}

// InitWithBufSizeAndNumSenders initializes the RowChannel with a given buffer
// size and number of senders.
func (rc *RowChannel) InitWithBufSizeAndNumSenders(types []types.T, chanBufSize, numSenders int) {
	rc.types = types
	rc.dataChan = make(chan RowChannelMsg, chanBufSize)
	rc.C = rc.dataChan
	atomic.StoreInt32(&rc.numSenders, int32(numSenders))
}

// PushPGResult is part of the RowReceiver interface.
func (rc *RowChannel) PushPGResult(ctx context.Context, res []byte) error {
	return nil
}

// AddPGComplete is part of the RowReceiver interface.
func (rc *RowChannel) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

// Push is part of the RowReceiver interface.
func (rc *RowChannel) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) ConsumerStatus {
	consumerStatus := ConsumerStatus(
		atomic.LoadUint32((*uint32)(&rc.ConsumerStatus)))
	if meta != nil && (meta.TsInsert != nil || meta.TsDelete != nil || meta.TsTagUpdate != nil ||
		meta.TsCreate != nil || meta.TsPro != nil || meta.TsAlterColumn != nil) {
		consumerStatus = DrainRequested
	}
	switch consumerStatus {
	case NeedMoreRows:
		rc.dataChan <- RowChannelMsg{Row: row, Meta: meta}
	case DrainRequested:
		// If we're draining, only forward metadata.
		if meta != nil {
			rc.dataChan <- RowChannelMsg{Meta: meta}
		}
	case ConsumerClosed:
		// If the consumer is gone, swallow all the rows and the metadata.
	}
	return consumerStatus
}

// ProducerDone is part of the RowReceiver interface.
func (rc *RowChannel) ProducerDone() {
	newVal := atomic.AddInt32(&rc.numSenders, -1)
	if newVal < 0 {
		panic("too many ProducerDone() calls")
	}
	if newVal == 0 {
		close(rc.dataChan)
	}
}

// OutputTypes is part of the RowSource interface.
func (rc *RowChannel) OutputTypes() []types.T {
	return rc.types
}

// Start is part of the RowSource interface.
func (rc *RowChannel) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
func (rc *RowChannel) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {

	d, ok := <-rc.C
	if !ok {
		// No more rows.
		return nil, nil
	}
	return d.Row, d.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rc *RowChannel) ConsumerDone() {
	rc.consumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (rc *RowChannel) ConsumerClosed() {
	rc.consumerClosed("RowChannel")
	numSenders := atomic.LoadInt32(&rc.numSenders)
	// Drain (at most) numSenders messages in case senders are blocked trying to
	// emit a row.
	// Note that, if the producer is done, then it has also closed the
	// channel this will not block. The producer might be neither blocked nor
	// closed, though; hence the no data case.
	for i := int32(0); i < numSenders; i++ {
		select {
		case <-rc.dataChan:
		default:
		}
	}
}

// InitProcessorProcedure init processor in procedure
func (rc *RowChannel) InitProcessorProcedure(txn *kv.Txn) {}

// Types is part of the RowReceiver interface.
func (rc *RowChannel) Types() []types.T {
	return rc.types
}
