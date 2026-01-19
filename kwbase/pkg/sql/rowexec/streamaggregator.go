// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package rowexec

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	sizeOfStreamBucket = int64(unsafe.Sizeof(streamBucket{}))
	sizeOfGroupWindow  = int64(unsafe.Sizeof(groupWindow{}))

	sizeOfStateWindowHelper   = int64(unsafe.Sizeof(StateWindowHelper{}))
	sizeOfCountWindowHelper   = int64(unsafe.Sizeof(CountWindowHelper{}))
	sizeOfTimeWindowHelper    = int64(unsafe.Sizeof(TimeWindowHelper{}))
	sizeOfSessionWindowHelper = int64(unsafe.Sizeof(SessionWindowHelper{}))
)

// streamAggregator is a specialized streamAggregator that only used to handle
// the rows coming from Stream Reader.
type streamAggregator struct {
	aggregatorBase

	currentBucket         *streamBucket
	streamGroupWindow     *groupWindow
	currentBucketIdentity string

	streamBuckets        map[string]*streamBucket // metadata cache map of stream agg  (device -> metadata)
	streamAggFuncSize    int64
	streamBucketBaseSize int64

	streamMemMonitor *mon.BytesMonitor
	streamBucketsAcc mon.BoundAccount

	forceEmitRows sqlbase.EncDatumRows

	streamOpts *sqlutil.ParsedStreamOptions

	groupWindowColID   int32
	groupWindowTsColID int32
	groupWindowID      []int32

	// isSlidingWindowCompleted is true when all windows completed.
	isSlidingWindowCompleted bool
}

var _ execinfra.Processor = &streamAggregator{}
var _ execinfra.RowSource = &streamAggregator{}
var _ execinfra.OpNode = &streamAggregator{}

type streamBucket struct {
	bucket            aggregateFuncs
	lastOrdGroupCols  sqlbase.EncDatumRow
	eventOrdGroupCols sqlbase.EncDatumRow
	forceCloseTime    time.Time

	groupWindow *groupWindow
}

const streamAggregatorProcName = "stream aggregator"

func newStreamAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.StreamAggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*streamAggregator, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	streamAgg := &streamAggregator{
		streamBuckets: make(map[string]*streamBucket),
	}

	if err := streamAgg.init(
		streamAgg,
		flowCtx,
		processorID,
		spec.AggSpec,
		input,
		post,
		output,
		func(context.Context) []execinfrapb.ProducerMetadata {
			streamAgg.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	if spec.AggSpec.HasTimeBucketGapFill {
		return nil, errors.Errorf("TimeBucketGapFill is not supported.")
	}

	streamAgg.groupWindowColID = spec.AggSpec.GroupWindowId
	streamAgg.groupWindowTsColID = spec.AggSpec.Group_WindowTscolid
	streamAgg.groupWindowID = spec.AggSpec.Group_WindowId

	streamAgg.streamGroupWindow = newGroupWindowWithAllHelpers(
		streamAgg.groupWindowColID,
		spec.AggSpec.Group_WindowTscolid,
		spec.AggSpec.Group_WindowId,
	)
	locStr := flowCtx.EvalCtx.GetLocation().String()
	var err error
	streamAgg.streamGroupWindow.loc, err = timeutil.TimeZoneStringToLocation(locStr, timeutil.TimeZoneStringToLocationISO8601Standard)
	if err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	streamAgg.EvalCtx.SingleDatumAggMemAccount = &streamAgg.aggFuncsAcc
	streamPara, err := sqlutil.ParseStreamParameters(spec.Metadata.Parameters)
	if err != nil {
		return nil, err
	}
	streamOpts, err := sqlutil.ParseStreamOpts(&streamPara.Options)
	if err != nil {
		return nil, err
	}

	streamAgg.streamOpts = streamOpts

	monitor := mon.MakeMonitorInheritWithLimit("stream-aggregator-mem", int64(streamOpts.BufferSize) /* limit */, flowCtx.EvalCtx.Mon)
	monitor.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
	streamAgg.streamMemMonitor = &monitor

	streamAgg.streamBucketsAcc = streamAgg.streamMemMonitor.MakeBoundAccount()
	streamAgg.isSlidingWindowCompleted = true

	return streamAgg, nil
}

// Start is part of the RowSource interface.
func (streamAgg *streamAggregator) Start(ctx context.Context) context.Context {
	return streamAgg.start(ctx, streamAggregatorProcName)
}

func (streamAgg *streamAggregator) close() {
	if streamAgg.InternalClose() {
		log.VEventf(streamAgg.PbCtx(), 2, "exiting stream aggregator")
		for _, bucket := range streamAgg.streamBuckets {
			if bucket.bucket != nil {
				bucket.bucket.close(streamAgg.PbCtx())
			}
			streamAgg.gwClose(bucket.groupWindow)
			bucket.groupWindow = nil
		}

		// Make sure to release any remaining memory under 'streamBuckets'.
		streamAgg.streamBuckets = nil

		// Note that we should be closing accounts only after closing the
		// bucket since the latter might be releasing some precisely tracked
		// memory. If we were to close the accounts first, there would be
		// no memory to release for the bucket.
		streamAgg.streamBucketsAcc.Close(streamAgg.PbCtx())
		streamAgg.streamMemMonitor.Stop(streamAgg.PbCtx())

		streamAgg.bucketsAcc.Close(streamAgg.PbCtx())
		streamAgg.aggFuncsAcc.Close(streamAgg.PbCtx())
		streamAgg.MemMonitor.Stop(streamAgg.PbCtx())
	}
}

func (streamAgg *streamAggregator) gwClose(groupWindow *groupWindow) {
	if groupWindow != nil {
		if streamAgg.EvalCtx == nil || streamAgg.EvalCtx.GroupWindow == nil || groupWindow.memMonitor == nil {
			return
		}
		if streamAgg.EvalCtx.GroupWindow.GroupWindowFunc == tree.CountWindow && streamAgg.EvalCtx.GroupWindow.CountWindowHelper.IsSlide ||
			(streamAgg.EvalCtx.GroupWindow.GroupWindowFunc == tree.TimeWindow && streamAgg.EvalCtx.GroupWindow.TimeWindowHelper.IsSlide) {
			groupWindow.Close(streamAgg.PbCtx())
		}
	}
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (streamAgg *streamAggregator) accumulateRows() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	var identity string
	for {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		if streamAgg.isSlidingWindowCompleted {
			row, meta = streamAgg.input.Next()
			if meta != nil {
				if meta.Err != nil {
					streamAgg.MoveToDraining(nil /* err */)
					return aggStateUnknown, nil, meta
				}
				return aggAccumulating, nil, meta
			}
		} else {
			row = nil
		}

		if row != nil {
			identity = streamAgg.extractBucketIdentity(row)
			if streamAgg.currentBucketIdentity != identity || streamAgg.currentBucket == nil {
				bucket, ok := streamAgg.streamBuckets[identity]
				if ok {
					streamAgg.currentBucket = bucket
				} else {
					streamAgg.currentBucket = &streamBucket{}
					streamAgg.streamBuckets[identity] = streamAgg.currentBucket

					// add memory used by identity and streamBucket struct
					if streamAgg.streamBucketBaseSize == 0 {
						streamAgg.streamBucketBaseSize = streamBucketBaseSize(streamAgg.EvalCtx, row)
					}
					bucketFixedSize := streamAgg.streamBucketBaseSize + int64(unsafe.Sizeof(identity))

					if err := streamAgg.streamBucketsAcc.Grow(streamAgg.PbCtx(), bucketFixedSize); err != nil {
						streamAgg.MoveToDraining(errors.Wrap(err, "the limitation of stream BUFFER_SIZE has been reached") /* err */)
						return aggStateUnknown, nil, nil
					}
				}

				streamAgg.currentBucketIdentity = identity
			}

			streamAgg.currentBucket.forceCloseTime = timeutil.Now().UTC().Add(streamAgg.streamOpts.MaxDelay)
		} else {
			streamAgg.isSlidingWindowCompleted = streamAgg.slidingWindowCompleted()
			// force closes the Agg window based on MAX_DELAY option
			if streamAgg.isSlidingWindowCompleted {
				streamAgg.forceEmitRow()
				if len(streamAgg.forceEmitRows) != 0 {
					//streamAgg.forceCloseWindow = false
					return aggForceEmittingRows, nil, nil
				}

				continue
			}
		}

		if streamAgg.currentBucket == nil {
			continue
		}

		// WINDOW Function cases
		if haveGroupWindow(streamAgg.EvalCtx) {
			if streamAgg.currentBucket.groupWindow == nil {
				gw := newGroupWindow(
					streamAgg.EvalCtx,
					streamAgg.groupWindowColID,
					streamAgg.groupWindowTsColID,
					streamAgg.groupWindowID,
				)

				streamAgg.currentBucket.groupWindow = gw
			}

			streamAgg.streamGroupWindow = streamAgg.currentBucket.groupWindow

			if streamAgg.FlowCtx.EvalCtx.GroupWindow.GroupWindowFunc == tree.EventWindow {
				if streamAgg.currentBucket.eventOrdGroupCols == nil {
					streamAgg.currentBucket.eventOrdGroupCols = streamAgg.rowAlloc.CopyRow(row)
				} else {
					if row != nil {
						matched, err := streamAgg.matchLastOrdGroupColsForEvent(row, streamAgg.streamGroupWindow.groupWindowColID)
						if err != nil {
							streamAgg.MoveToDraining(err)
							return aggStateUnknown, nil, nil
						}
						if !matched {
							streamAgg.streamGroupWindow.startFlag = false
						}
					}
				}
			}

			if err := streamAgg.streamGroupWindow.CheckAndGetWindowDatum(streamAgg.inputTypes, streamAgg.FlowCtx, &row); err != nil {
				streamAgg.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}

			switch streamAgg.EvalCtx.GroupWindow.GroupWindowFunc {
			case tree.StateWindow:
				if streamAgg.streamGroupWindow.stateWindowHelper.IgnoreFlag {
					streamAgg.streamGroupWindow.stateWindowHelper.IgnoreFlag = false
					continue
				}
			case tree.EventWindow:
				if streamAgg.EvalCtx.GroupWindow.EventWindowHelper.IgnoreFlag {
					streamAgg.EvalCtx.GroupWindow.EventWindowHelper.IgnoreFlag = false
					continue
				}
			default:
			}
		}

		if row == nil {
			continue
		}

		if streamAgg.currentBucket.lastOrdGroupCols == nil {
			streamAgg.currentBucket.lastOrdGroupCols = streamAgg.rowAlloc.CopyRow(row)

			streamAgg.streamBuckets[identity] = streamAgg.currentBucket
		} else {
			matched, err := streamAgg.matchLastOrdGroupCols(row)
			if err != nil {
				streamAgg.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(streamAgg.currentBucket.lastOrdGroupCols, row)
				break
			}
		}

		if err := streamAgg.accumulateRow(row); err != nil {
			streamAgg.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// matchLastOrdGroupCols takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (streamAgg *streamAggregator) matchLastOrdGroupCols(row sqlbase.EncDatumRow) (bool, error) {
	for _, colIdx := range streamAgg.orderedGroupCols {
		res, err := streamAgg.currentBucket.lastOrdGroupCols[colIdx].Compare(
			&streamAgg.inputTypes[colIdx], &streamAgg.datumAlloc, streamAgg.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// matchLastOrdGroupColsForEvent takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (streamAgg *streamAggregator) matchLastOrdGroupColsForEvent(
	row sqlbase.EncDatumRow, groupWindowColID int32,
) (bool, error) {
	for _, colIdx := range streamAgg.orderedGroupCols {
		if colIdx == uint32(groupWindowColID) {
			continue
		}
		res, err := streamAgg.currentBucket.eventOrdGroupCols[colIdx].Compare(
			&streamAgg.inputTypes[colIdx], &streamAgg.datumAlloc, streamAgg.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (streamAgg *streamAggregator) emitRow() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if streamAgg.currentBucket == nil {
		err := errors.Errorf("internal error: failed to process steam aggregation")
		streamAgg.MoveToDraining(err)
		return aggStateUnknown, nil, nil
	}

	if streamAgg.currentBucket.bucket == nil {
		// We've exhausted all the aggregation buckets.
		if streamAgg.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			streamAgg.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by streamAgg.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := streamAgg.arena.UnsafeReset(streamAgg.PbCtx()); err != nil {
			streamAgg.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		for _, f := range streamAgg.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := streamAgg.accumulateRow(streamAgg.currentBucket.lastOrdGroupCols); err != nil {
			streamAgg.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := streamAgg.currentBucket.bucket

	streamAgg.streamBucketsAcc.Shrink(streamAgg.PbCtx(), streamAgg.streamAggFuncSize)
	streamAgg.currentBucket.bucket = nil

	return streamAgg.getAggResults(bucket)
}

// slidingWindowCompleted returns false when there is still a sliding window that is uncompleted,
// indicates that the calculation needs to continue.
// it returns true when the non-sliding window function or all windows have finished calculating,
// indicates that there is no need to continue the calculation.
func (streamAgg *streamAggregator) slidingWindowCompleted() bool {
	switch streamAgg.EvalCtx.GroupWindow.GroupWindowFunc {
	case tree.CountWindow:
		if !streamAgg.EvalCtx.GroupWindow.CountWindowHelper.IsSlide {
			return true
		}

		currentTime := timeutil.Now().UTC()
		for key, value := range streamAgg.streamBuckets {
			if value == nil || value.bucket == nil {
				continue
			}

			// The number of rows in the queue exceeds the window capacity.
			if value.groupWindow.getRowsLength() >= streamAgg.FlowCtx.EvalCtx.GroupWindow.CountWindowHelper.WindowNum {
				streamAgg.currentBucketIdentity = key
				streamAgg.currentBucket = value
				return false
			}

			// The number of rows in the queue more than zero when forceCloseTime coming.
			if currentTime.After(value.forceCloseTime) && value.groupWindow.getRowsLength() > 0 {
				streamAgg.currentBucketIdentity = key
				streamAgg.currentBucket = value
				return false
			}
		}
	case tree.TimeWindow:
		if !streamAgg.EvalCtx.GroupWindow.TimeWindowHelper.IsSlide {
			return true
		}

		currentTime := timeutil.Now().UTC()
		for key, value := range streamAgg.streamBuckets {
			if value == nil || value.bucket == nil {
				continue
			}

			// The number of rows in the queue can slided.
			if value.groupWindow.timeWindowHelper.canSlide {
				streamAgg.currentBucketIdentity = key
				streamAgg.currentBucket = value
				return false
			}

			// The number of rows in the queue more than zero.
			if currentTime.After(value.forceCloseTime) && value.groupWindow.getRowsLength() > 0 {
				streamAgg.currentBucketIdentity = key
				streamAgg.currentBucket = value
				return false
			}
		}
	default:
		return true
	}

	return true
}

func (streamAgg *streamAggregator) forceEmitRow() {
	streamAgg.forceEmitRows = make(sqlbase.EncDatumRows, 0)
	if streamAgg.streamBuckets == nil || len(streamAgg.streamBuckets) == 0 {
		return
	}

	currentTime := timeutil.Now().UTC()
	for key, value := range streamAgg.streamBuckets {
		if value == nil || value.bucket == nil {
			continue
		}

		if currentTime.After(value.forceCloseTime) {
			status, row, _ := streamAgg.getAggResults(value.bucket)
			if row != nil && status == aggEmittingRows {
				outRow := streamAgg.rowAlloc.CopyRow(row)
				streamAgg.forceEmitRows = append(streamAgg.forceEmitRows, outRow)
			}

			streamAgg.streamBucketsAcc.Shrink(streamAgg.PbCtx(), streamAgg.streamAggFuncSize)

			if value.bucket != nil {
				value.bucket.close(streamAgg.PbCtx())
			}
			streamAgg.gwClose(value.groupWindow)
			value.groupWindow = nil

			value.bucket = nil

			delete(streamAgg.streamBuckets, key)
			bucketFixedSize := streamAgg.streamBucketBaseSize + int64(unsafe.Sizeof(key))
			streamAgg.streamBucketsAcc.Shrink(streamAgg.PbCtx(), bucketFixedSize)
		}
	}

	streamAgg.currentBucketIdentity = ""
}

func (streamAgg *streamAggregator) getAggResults(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	defer bucket.close(streamAgg.PbCtx())
	streamAgg.Out.Gapfill = false
	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			streamAgg.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			result = tree.DNull
		}
		streamAgg.outputTypes[i] = *result.ResolvedType()
		streamAgg.row[i] = sqlbase.DatumToEncDatum(&streamAgg.outputTypes[i], result)
	}

	if outRow := streamAgg.ProcessRowHelper(streamAgg.row); outRow != nil {
		return aggEmittingRows, outRow, nil
	}
	// We might have switched to draining, we might not have. In case we
	// haven't, aggEmittingRows is accurate. If we have, it will be ignored by
	// the caller.
	return aggEmittingRows, nil, nil
}

func (streamAgg *streamAggregator) ProcessRowHelper(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
	outRow, ok, err := streamAgg.Out.ProcessRowForStream(streamAgg.PbCtx(), row)
	if err != nil {
		streamAgg.MoveToDraining(err)
		return nil
	}
	if !ok {
		streamAgg.MoveToDraining(nil /* err */)
	}

	return outRow
}

// Next is part of the RowSource interface.
func (streamAgg *streamAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for streamAgg.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch streamAgg.runningState {
		case aggAccumulating:
			streamAgg.runningState, row, meta = streamAgg.accumulateRows()
		case aggEmittingRows:
			streamAgg.runningState, row, meta = streamAgg.emitRow()
		case aggForceEmittingRows:
			if len(streamAgg.forceEmitRows) != 0 {
				row = streamAgg.forceEmitRows[0]
				streamAgg.forceEmitRows = streamAgg.forceEmitRows[1:]
			} else {
				streamAgg.runningState = aggAccumulating
			}
		default:
			log.Fatalf(streamAgg.PbCtx(), "unsupported state: %d", streamAgg.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, streamAgg.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (streamAgg *streamAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	streamAgg.close()
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (streamAgg *streamAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
	if err := streamAgg.cancelChecker.Check(); err != nil {
		return err
	}

	if streamAgg.currentBucket.bucket == nil {
		var err error
		currentAccUsed := streamAgg.bucketsAcc.Used()
		streamAgg.currentBucket.bucket, err = streamAgg.createAggregateFuncs()
		if err != nil {
			return err
		}

		if streamAgg.streamAggFuncSize == 0 {
			streamAgg.streamAggFuncSize = streamAgg.bucketsAcc.Used() - currentAccUsed
		}

		if err := streamAgg.streamBucketsAcc.Grow(streamAgg.PbCtx(), streamAgg.streamAggFuncSize); err != nil {
			return errors.Wrap(err, "the limitation of stream BUFFER_SIZE has been reached")
		}
	}

	return streamAgg.accumulateRowIntoBucket(row, nil /* groupKey */, streamAgg.currentBucket.bucket)
}

func (streamAgg *streamAggregator) extractBucketIdentity(row sqlbase.EncDatumRow) string {
	buf := strings.Builder{}

	for _, colIdx := range streamAgg.orderedGroupCols {
		// exclude the timestamp column from the group identity,
		// also exclude the 'group window column' used by WINDOW Functions
		if colIdx == uint32(streamAgg.streamGroupWindow.groupWindowColID) ||
			colIdx == uint32(streamAgg.streamGroupWindow.groupWindowTsColID) {
			continue
		}

		dString := sqlbase.DatumToString(row[colIdx].Datum)
		buf.WriteString(fmt.Sprintf("%d:%s", len(dString), dString))
	}
	return buf.String()
}

// InitProcessorProcedure init processor in procedure
func (streamAgg *streamAggregator) InitProcessorProcedure(txn *kv.Txn) {
	if streamAgg.EvalCtx.IsProcedure {
		if streamAgg.FlowCtx != nil {
			streamAgg.FlowCtx.Txn = txn
		}
		streamAgg.Closed = false
		streamAgg.State = execinfra.StateRunning
		streamAgg.Out.SetRowIdx(0)
	}
}

// rowSize computes the size of a single row.
func rowSize(row sqlbase.EncDatumRow) int64 {
	var rsz int64
	for _, col := range row {
		rsz += int64(col.Size())
	}
	return rsz
}

func streamBucketBaseSize(evalCtx *tree.EvalContext, row sqlbase.EncDatumRow) int64 {
	var rsz int64
	rsz += sizeOfStreamBucket
	rsz += sizeOfGroupWindow

	switch evalCtx.GroupWindow.GroupWindowFunc {
	case tree.StateWindow:
		rsz += sizeOfStateWindowHelper
	case tree.CountWindow:
		rsz += sizeOfCountWindowHelper
	case tree.TimeWindow:
		rsz += sizeOfTimeWindowHelper
	case tree.SessionWindow:
		rsz += sizeOfSessionWindowHelper
	default:
	}

	rsz += rowSize(row)

	return rsz
}
