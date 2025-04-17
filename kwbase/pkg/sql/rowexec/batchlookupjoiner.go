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

// batchlookupjoiner.go only be used for multiple model processing
// when the switch is on and the server starts with single node mode.

package rowexec

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/lib/pq/oid"
	"github.com/opentracing/opentracing-go"
)

// batchLookupJoinerInitialBufferSize controls the size of the initial buffering phase
// (see batchLookupJoiner). This only applies when falling back to disk is disabled.
const batchLookupJoinerInitialBufferSize = 4 * 1024 * 1024

const batchSize = 5000

// batchLookupJoinerState represents the state of the processor.
type batchLookupJoinerState int

const (
	bljStateUnknown batchLookupJoinerState = iota
	// bljBuildingAndConsumingStoredSide represents the state the batchLookupJoiner is in
	// left side. In this case, the batchLookupJoiner will fully consume the
	// right side. This state is skipped if the batchLookupJoiner determined the smallest
	// side, since it must have fully consumed that side.
	bljBuildAndConsumingStoredSide
	// bljPushingToProbeSide represents the state the batchLookupJoiner is in when it pushs
	// rows down to ProbeSide
	bljPushingToProbeSide
	// bljPullFromProbeSide represents the state the batchLookupJoiner is pull data from
	// rightSide, this row data is expected as rendered
	bljPullFromProbeSide
)

// batchLookupJoiner performs a hash join for relational data and time series data.
// There is no guarantee on the output ordering.
type batchLookupJoiner struct {
	joinerBase

	runningState batchLookupJoinerState

	diskMonitor *mon.BytesMonitor

	leftSource, rightSource execinfra.RowSource

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// batchLookupJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	rows [2]rowcontainer.MemRowContainer

	// storedSide is set by the initial buffering phase and indicates which
	// stream we store fully and build the hashRowContainer from.
	storedSide joinSide

	// nullEquality indicates that NULL = NULL should be considered true. Used for
	// INTERSECT and EXCEPT.
	nullEquality bool

	useTempStorage bool
	storedRows     rowcontainer.HashRowContainer

	// testingKnobMemFailPoint specifies a state in which the batchLookupJoiner will
	// fail at a random point during this phase.
	testingKnobMemFailPoint batchLookupJoinerState

	// Context cancellation checker.
	cancelChecker *sqlbase.CancelChecker

	// tsTableReaderID used to find the corresponding ttr
	tsTableReaderID int32

	leftRemains bool

	rowCount uint32
	// batch count for plan display
	batchCount uint32
}

var _ execinfra.Processor = &batchLookupJoiner{}
var _ execinfra.RowSource = &batchLookupJoiner{}
var _ execinfra.OpNode = &batchLookupJoiner{}

const batchLookupJoinerProcName = "batch lookup joiner"

// newBatchLookupJoiner helps create a batchlookup joiner in kwbase
func newBatchLookupJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.BatchLookupJoinerSpec,
	leftSource execinfra.RowSource,
	rightSource execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*batchLookupJoiner, error) {
	h := &batchLookupJoiner{
		initialBufferSize: batchLookupJoinerInitialBufferSize,
		leftSource:        leftSource,
		rightSource:       rightSource,
	}

	if err := h.joinerBase.BLJInit(
		h,
		flowCtx,
		processorID,
		leftSource.OutputTypes(),
		rightSource.OutputTypes(),
		spec.Type,
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	st := h.FlowCtx.Cfg.Settings
	ctx := h.FlowCtx.EvalCtx.Ctx()
	h.tsTableReaderID = spec.TstablereaderId
	h.useTempStorage = execinfra.SettingUseTempStorageJoins.Get(&st.SV) ||
		h.FlowCtx.Cfg.TestingKnobs.ForceDiskSpill ||
		h.FlowCtx.Cfg.TestingKnobs.MemoryLimitBytes > 0 ||
		h.testingKnobMemFailPoint != bljStateUnknown
	if h.useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The batchLookupJoiner will overflow to disk if this limit is not enough.
		limit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
		if h.FlowCtx.Cfg.TestingKnobs.ForceDiskSpill {
			limit = 1
		}
		h.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "batchlookupjoiner-limited")
		h.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "batchlookupjoiner-disk")
		// Override initialBufferSize to be half of this processor's memory
		// limit. We consume up to h.initialBufferSize bytes from each input
		// stream.
		h.initialBufferSize = limit / 2
	} else {
		h.MemMonitor = execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "batchlookupjoiner-mem")
	}

	// If the trace is recording, instrument the batchLookupJoiner to collect stats.
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		h.leftSource = newInputStatCollector(h.leftSource)
		h.rightSource = newInputStatCollector(h.rightSource)
		h.FinishTrace = h.outputStatsToTrace
	}

	h.rows[leftSide].InitWithMon(
		nil /* ordering */, h.leftSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)
	h.rows[rightSide].InitWithMon(
		nil /* ordering */, h.rightSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)

	if h.joinType == sqlbase.IntersectAllJoin || h.joinType == sqlbase.ExceptAllJoin {
		h.nullEquality = true
	}

	h.rowCount = 0
	h.batchCount = 0

	return h, nil
}

// Start is part of the RowSource interface.
func (h *batchLookupJoiner) Start(ctx context.Context) context.Context {
	h.leftSource.Start(ctx)
	h.cancelChecker = sqlbase.NewCancelChecker(ctx)
	// get relational data and push to AE for actual join process
	// start with buildAndConsumeStoredSide
	h.runningState = bljBuildAndConsumingStoredSide
	var leftStartDone = false
	for !leftStartDone {
		switch h.runningState {
		case bljBuildAndConsumingStoredSide:
			h.runningState, _, _ = h.buildAndConsumeStoredSide()
		case bljPushingToProbeSide:
			h.runningState, _, _ = h.pushToProbeSide()
		default:
			// bljPullFromProbeSide: pull data in Next()
			// bljStateUnknown: trigger log.Fatal and DrainHelper in Next()
			leftStartDone = true
		}
		// no need to double check meta errors, when meta.Err != nil
		// runningState is set to bljStateUnknown in buildAndConsumeStoredSide and pushToProbeSide
	}
	h.rightSource.Start(ctx)
	ctx = h.StartInternal(ctx, batchLookupJoinerProcName)
	return ctx
}

// Next is part of the RowSource interface.
func (h *batchLookupJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for h.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		if h.runningState != bljPullFromProbeSide {
			log.Fatalf(h.PbCtx(), "unsupported state: %d", h.runningState)
			break
		}
		h.runningState, row, meta = h.pullFromProbeSide()
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := h.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, h.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (h *batchLookupJoiner) ConsumerClosed() {
	h.close()
}

// buildAndConsumeStoredSide is used for pull data from relational data stream
// and construct a data batch wrt predefined row size
func (h *batchLookupJoiner) buildAndConsumeStoredSide() (
	batchLookupJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// for blj storedside is always leftSide
	h.storedSide = leftSide

	// Initialize the storedRows container if needed.
	if err := h.initStoredRows(); err != nil {
		h.MoveToDraining(err)
		return bljStateUnknown, nil, h.DrainHelper()
	}

	// Process rows for the left side and stored side.
	for h.rowCount < batchSize {
		row, meta, err := h.receiveNext(h.storedSide)
		if err != nil {
			h.MoveToDraining(err)
			return bljStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return bljStateUnknown, nil, meta
			}
			h.AppendTrailingMeta(*meta)
			continue
		}

		if row == nil {
			// The stored side has been fully consumed.
			h.leftRemains = false
			break
		}

		if err := h.storedRows.AddRow(h.PbCtx(), row); err != nil {
			if sqlbase.IsOutOfMemoryError(err) {
				if !h.useTempStorage {
					err = pgerror.Wrapf(err, pgcode.OutOfMemory,
						"error while attempting batchLookupJoiner disk spill: temp storage disabled")
				} else {
					if err := h.initStoredRows(); err != nil {
						h.MoveToDraining(err)
						return bljStateUnknown, nil, h.DrainHelper()
					}
					if addErr := h.storedRows.AddRow(h.PbCtx(), row); addErr != nil {
						err = pgerror.Wrapf(addErr, pgcode.OutOfMemory, "while spilling: %v", err)
					}
				}
			}
			h.MoveToDraining(err)
			return bljStateUnknown, nil, h.DrainHelper()
		}

		h.rowCount++
	}

	if h.rowCount == 0 {
		state := h.PushTsFlow(nil)
		if state == bljStateUnknown {
			return bljStateUnknown, nil, h.DrainHelper()
		}
		return bljPullFromProbeSide, nil, nil
	}
	// Check if we have more data left without making another call.
	if h.rowCount >= batchSize {
		// Use a flag to track if there's remaining data.
		if h.storedSide == leftSide {
			h.leftRemains = true
		} else {
			h.leftRemains = false
		}
	}
	// Move to pushing to the probe side.
	return bljPushingToProbeSide, nil, nil
}

// stringWide is used to add 2 more bytes for string types
const stringWide = 2

// InsertData is used to add a row to datachunk
func InsertData(
	dc tse.DataChunkGo, row uint32, col uint32, colPrecision int32, coldata sqlbase.EncDatum,
) {
	colOffset := row*dc.ColInfo[col].FixedStorageLen + dc.ColOffset[col]
	// set null bitmap first
	// get cur col's offset
	bitmapOffset := dc.BitmapOffset[col]
	// get cur col's data
	bitmap := dc.Data[bitmapOffset:]
	// get the byte where the row is in
	index := row >> 3 // row / 8
	// get the row pos in byte
	pos := uint8(1)          // binary 1000 0000
	mask := pos << (row & 7) // pos >> (row % 8)
	// Check NULL first
	if coldata.IsNull() {
		bitmap[index] |= mask // Set the bit to 1
		return                // Exit early since there's no data to insert
	}
	// Data is not null, proceed with type checking
	switch d := coldata.Datum.(type) {
	case *tree.DBool:
		data := bool(*d)
		value := []byte{0}
		if data {
			value[0] = 1
		}
		copy(dc.Data[colOffset:], value)
	case *tree.DInt:
		switch dc.ColInfo[col].StorageType {
		case oid.T_int2:
			binary.LittleEndian.PutUint16(dc.Data[colOffset:], uint16(*d))
		case oid.T_int4:
			binary.LittleEndian.PutUint32(dc.Data[colOffset:], uint32(*d))
		case oid.T_int8, oid.T_timestamp, oid.T_timestamptz:
			binary.LittleEndian.PutUint64(dc.Data[colOffset:], uint64(*d))
		}
	case *tree.DFloat:
		switch dc.ColInfo[col].StorageType {
		case oid.T_float4:
			binary.LittleEndian.PutUint32(dc.Data[colOffset:], uint32(int32(math.Float32bits(float32(*d)))))
		case oid.T_float8:
			binary.LittleEndian.PutUint64(dc.Data[colOffset:], uint64(int64(math.Float64bits(float64(*d)))))
		}
	case *tree.DString:
		switch dc.ColInfo[col].StorageType {
		case oid.T_char, types.T_nchar, oid.T_bpchar, types.T_geometry: // text in here
			copy(dc.Data[colOffset+stringWide:], *d)
		case oid.T_varchar, types.T_nvarchar, oid.T_text:
			str := *d
			value := []byte(str) // 转换为 []byte
			length := uint32(len(value))
			binary.LittleEndian.PutUint32(dc.Data[colOffset:], length) // encode to cpp readable format, add len
			copy(dc.Data[colOffset+stringWide:], *d)                   // add value
		}
	case *tree.DBytes:
		switch dc.ColInfo[col].StorageType {
		case oid.T_bytea:
			// Special handling: When assembling the payload related to the bytes type,
			// write the actual length of the bytes type data at the beginning of the byte array.
			binary.LittleEndian.PutUint16(dc.Data[colOffset:colOffset+2], uint16(len(*d)))
			copy(dc.Data[colOffset+2:], *d)
		case types.T_varbytea: // isString: bytes in kwbase -> binary in tse
			str := *d
			value := []byte(str) // 转换为 []byte
			length := uint32(len(value))
			binary.LittleEndian.PutUint32(dc.Data[colOffset:], length) // encode to cpp readable format, add len
			copy(dc.Data[colOffset+stringWide:], *d)                   // add value
		}
	case *tree.DTimestamp:
		var value int64
		switch colPrecision {
		case 0, 3:
			value = d.UnixMilli()
		case 6:
			value = d.UnixNano() / 1000
		case 9:
			value = d.UnixNano()
		default:
			value = d.UnixMilli()
		}
		binary.LittleEndian.PutUint64(dc.Data[colOffset:], uint64(value))
	case *tree.DTimestampTZ:
		var value int64
		switch colPrecision {
		case 0, 3:
			value = d.UnixMilli()
		case 6:
			value = d.UnixNano() / 1000
		case 9:
			value = d.UnixNano()
		default:
			value = d.UnixMilli()
		}
		binary.LittleEndian.PutUint64(dc.Data[colOffset:], uint64(value))
	}
	// SetNotNull(row, col)
	// set bit: clear corresponding null bit, set to 0
	bitmap[index] &^= mask
}

// pushToProbeSide helps construct datachunk for current row batch and push it down to tse
func (h *batchLookupJoiner) pushToProbeSide() (
	batchLookupJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	side := otherSide(h.storedSide)

	// First process the rows that were already buffered.
	if h.rows[side].Len() > 0 { // skip
		h.rows[side].PopFirst()
	} else {
		var meta *execinfrapb.ProducerMetadata
		// get relational data iterator
		i := h.storedRows.NewUnmarkedIterator(h.PbCtx())
		i.Rewind()
		// traverse iterator to get the rowNums
		rowNums := uint32(0)
		for {
			valid, err := i.Valid()
			if err != nil || !valid {
				break
			}
			_, err = i.Row()
			if err != nil {
				h.MoveToDraining(nil /* err */)
				return bljStateUnknown, nil, meta
			}
			rowNums++
			i.Next()
		}
		i.Rewind()
		// Create DataChunk, step1 prepare all info for DataChunk
		var rowSize uint32
		rowSize = 0 // bytes
		outputType := h.leftSource.OutputTypes()
		// bitmap
		bitmapSize := uint32((rowNums + 7) / 8)
		bitmapOffset := uint32(0)
		var bitmapOffsetSlice []uint32
		var colOffsetSlice []uint32
		var colInfo []tse.ColumnInfo
		colNums := uint32(len(outputType))
		for colIdx := 0; colIdx < int(colNums); colIdx++ {
			// Width is the size or scale of the type, such as number of bits or
			// characters.
			storeLen := uint32(0)
			fixedStoreLen := uint32(0)
			colType := outputType[colIdx].InternalType.Oid
			colFam := outputType[colIdx].Family()
			switch colFam {
			case types.BoolFamily:
				storeLen = 1
				fixedStoreLen = 1
			case types.IntFamily:
				switch colType {
				case oid.T_int2: // T__int2 is array of T_int2
					storeLen = 2
					fixedStoreLen = 2
				case oid.T_int4:
					storeLen = 4
					fixedStoreLen = 4
				case oid.T_int8:
					storeLen = 8
					fixedStoreLen = 8
				}
			case types.FloatFamily:
				switch colType {
				case oid.T_float4:
					storeLen = 4
					fixedStoreLen = 4
				case oid.T_float8: // double
					storeLen = 8
					fixedStoreLen = 8
				}
			case types.DecimalFamily: // equals to float according to table.go GetTSDataType
				// deprecate now
				storeLen = 4
				fixedStoreLen = 4
			// No storeLen for datefamily and intervalfamily in table.go, strLen will be 0
			case types.TimeFamily, types.TimestampFamily, types.TimestampTZFamily:
				storeLen = 8
				fixedStoreLen = 8
			// Lack of sde type, ignore for now
			case types.StringFamily, types.BytesFamily:
				colWidth := uint32(outputType[colIdx].InternalType.Width)
				if colType == oid.Oid(91002) || colType == oid.Oid(91004) {
					colWidth *= 4
				}
				if colWidth == 0 {
					switch colType {
					case oid.T_char, oid.T_bpchar:
						storeLen = 1
						fixedStoreLen = 3 // + stringwide
					case oid.T_bytea:
						storeLen = 3
						fixedStoreLen = 5 // + stringwide
					case oid.Oid(91002): // NCHAR in type.go
						storeLen = 4
						fixedStoreLen = 6 // + stringwide
					case oid.T_varchar, oid.T_varbytea, oid.Oid(91004), oid.T_text: // NVARCHAR in type.go
						storeLen = 255      // according to table.go TSMaxVariableTupleLen
						fixedStoreLen = 257 // + stringwide
					}
				} else {
					switch colType {
					case oid.T_char, oid.T_bpchar, oid.Oid(91002), oid.T_varchar, oid.Oid(91004):
						// 1 more bytes for TS decode wrt mutation.go & table.go, -> bug ZDP-31516
						storeLen = colWidth + 1
						fixedStoreLen = storeLen + stringWide // + STRING_WIDE in ee_data_chunk.cpp
					case oid.T_bytea:
						storeLen = colWidth + 2
						fixedStoreLen = storeLen + stringWide
					case oid.T_varbytea:
						storeLen = colWidth
						fixedStoreLen = storeLen + stringWide
					}
				}
				// DataType_DECIMAL, DataType_DATE never used in table.go:
				// - in create_table.go, in checkTSColValidity, seems like ts table doesn't support decimal col
				// currently, transfer decimal cols into float
			}
			curColInfo := tse.ColumnInfo{
				StorageLen:      storeLen,      // original length
				FixedStorageLen: fixedStoreLen, // + STRING_WIDE
				StorageType:     colType,       // VARCHAR = 10 in me_metadata.pb.h
			}
			colInfo = append(colInfo, curColInfo)
			bitmapOffsetSlice = append(bitmapOffsetSlice, bitmapOffset)
			colOffsetSlice = append(colOffsetSlice, bitmapOffset+bitmapSize)
			bitmapOffset += fixedStoreLen*rowNums + bitmapSize // change to fixed storage len
			rowSize += fixedStoreLen                           // get full bytes
		}
		// delete is required
		rowSize++
		// construct a buffer to store rowBatch
		// tse: k_uint64 data_len = (capacity_ + 7) / 8 * col_num_ + capacity_ * row_size_;
		bufferSize := (rowNums+7)/8*colNums + rowNums*rowSize
		if rowSize == 0 {
			bufferSize = 1024 * 10 // tmp magic number
		}
		// create a datachunk
		dataChunk := tse.DataChunkGo{
			Data:         make([]byte, bufferSize),
			ColInfo:      colInfo,
			ColOffset:    colOffsetSlice,
			BitmapOffset: bitmapOffsetSlice,
			RowCount:     rowNums,
		}
		// step 2: insert data
		// traverse the rowBatch
		rowID := uint32(0)
		for {
			valid, err := i.Valid()
			if err != nil || !valid {
				break
			}
			row, err := i.Row()
			if err != nil {
				h.MoveToDraining(nil /* err */)
				return bljStateUnknown, nil, meta
			}
			// append row data to each col's value
			for colID := range row {
				space := &sqlbase.DatumAlloc{}
				err := row[colID].EnsureDecoded(&h.leftSource.OutputTypes()[colID], space)
				if err != nil {
					return bljStateUnknown, nil, meta
				}
				InsertData(dataChunk, rowID, uint32(colID), outputType[colID].Precision(), row[colID])
			}
			i.Next()
			rowID++
		}
		// update all field to FlowCtx
		h.FlowCtx.TsBatchLookupInput = &dataChunk

		if dataChunk.RowCount > 0 {
			state := h.PushTsFlow(&dataChunk)
			if state == bljStateUnknown {
				return state, nil, nil
			}
			h.rowCount = 0
			h.batchCount++
		}

		i.Close()
	}
	// use bljReadingProbeSide, row, nil to enable normal hash join
	if h.leftRemains {
		h.rows[leftSide].Clear(h.PbCtx())
		h.rows[rightSide].Clear(h.PbCtx())
		h.storedRows.Close(h.PbCtx())
		h.storedRows = nil
		return bljBuildAndConsumingStoredSide, nil, nil
	}
	h.PushTsFlow(nil)
	return bljPullFromProbeSide, nil, nil
}

func (h *batchLookupJoiner) PushTsFlow(chunk *tse.DataChunkGo) batchLookupJoinerState {
	// when turned to pull mode, we already push all data down to tse
	// thus we need to send one more push end message to tse
	var tsQueryInfo tse.TsQueryInfo
	if h.FlowCtx.TsHandleBreak {
		log.Warning(h.PbCtx(), "tse returned an error, please check the error code")
		h.MoveToDraining(nil)
		return bljStateUnknown
	}
	handle, ok := h.FlowCtx.TsHandleMap[h.tsTableReaderID]
	if !ok {
		log.Warning(h.PbCtx(), "handle is nil, please check the error")
		h.MoveToDraining(nil)
		return bljStateUnknown
	}
	tsQueryInfo.Handle = handle
	tsQueryInfo.Buf = []byte("pushdata tsflow")
	tsQueryInfo.PushData = chunk
	tsQueryInfo.RowCount = 0
	if chunk != nil {
		tsQueryInfo.RowCount = int(chunk.RowCount)
	}
	pushErr := h.FlowCtx.Cfg.TsEngine.PushTsFlow(&(h.Ctx), tsQueryInfo)
	if pushErr != nil {
		log.Warning(h.PbCtx(), pushErr)
	}
	return bljPushingToProbeSide
}

// pullFromProbeSide helps pull batchlookup join results from tse
func (h *batchLookupJoiner) pullFromProbeSide() (
	batchLookupJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// read all data from probeside(right, ts results)
	source := h.rightSource
	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return bljStateUnknown, nil, h.DrainHelper()
	}
	row, meta := source.Next() // pull from right
	if meta != nil {
		if meta.Err != nil {
			h.MoveToDraining(nil /* err */)
			// force clean up all resources
			h.storedRows = nil
			h.rows[leftSide].Clear(h.PbCtx())
			h.rows[rightSide].Clear(h.PbCtx())
			h.MemMonitor.ForceRelease(h.PbCtx())
			return bljStateUnknown, nil, meta
		}
		return bljPullFromProbeSide, nil, meta // check back later
	} else if row == nil {
		// The probe side has been fully consumed.
		// cur batchlookupjoin.Next() reaches an end
		if h.MemMonitor.AllocBytes() != 0 {
			// force clean up all resources
			h.storedRows = nil
			h.rows[leftSide].Clear(h.PbCtx())
			h.rows[rightSide].Clear(h.PbCtx())
			h.MemMonitor.ForceRelease(h.PbCtx())
		}
		h.MoveToDraining(nil /* err */)
		return bljStateUnknown, nil, h.DrainHelper()
	}
	return bljPullFromProbeSide, row, nil
}

func (h *batchLookupJoiner) close() {
	if h.InternalClose() {
		// We need to close only memRowContainer of the probe side because the
		// stored side container will be closed by closing h.storedRows.
		if h.storedSide == rightSide {
			h.rows[leftSide].Close(h.PbCtx())
		} else {
			h.rows[rightSide].Close(h.PbCtx())
		}
		if h.storedRows != nil {
			h.storedRows.Close(h.PbCtx())
		} else {
			// h.storedRows has not been initialized, so we need to close the stored
			// side container explicitly.
			h.rows[h.storedSide].Close(h.PbCtx())
		}
		h.MemMonitor.Stop(h.PbCtx())
		if h.diskMonitor != nil {
			h.diskMonitor.Stop(h.PbCtx())
		}
	}
}

// receiveNext reads from the source specified by side and returns the next row
// or metadata to be processed by the batchLookupJoiner. Unless h.nullEquality is true,
// rows with NULLs in their equality columns are only returned if the joinType
// specifies that unmatched rows should be returned for the given side. In this
// case, a rendered row and true is returned, notifying the caller that the
// returned row may be emitted directly.
func (h *batchLookupJoiner) receiveNext(
	side joinSide,
) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata, error) {
	source := h.leftSource
	if side == rightSide {
		source = h.rightSource
	}
	for {
		if err := h.cancelChecker.Check(); err != nil {
			return nil, nil, err
		}
		row, meta := source.Next()
		if meta != nil {
			return nil, meta, nil
		} else if row == nil {
			return nil, nil, nil
		}
		hasNull := false
		for _, c := range h.eqCols[side] {
			if row[c].IsNull() {
				hasNull = true
				break
			}
		}
		// row has no NULLs in its equality columns (or we are considering NULLs to
		// be equal), so it might match a row from the other side.
		if !hasNull || h.nullEquality {
			return row, nil, nil
		}
		// If this point is reached, row had NULLs in its equality columns but
		// should not be emitted. Throw it away and get the next row.
	}
}

// initStoredRows initializes a hashRowContainer and sets h.storedRows.
func (h *batchLookupJoiner) initStoredRows() error {
	if h.useTempStorage {
		hrc := rowcontainer.NewHashDiskBackedRowContainer(
			&h.rows[h.storedSide],
			h.EvalCtx,
			h.MemMonitor,
			h.diskMonitor,
			h.FlowCtx.Cfg.TempStorage,
		)
		h.storedRows = hrc
	} else {
		hrc := rowcontainer.MakeHashMemRowContainer(&h.rows[h.storedSide], h.MemMonitor)
		h.storedRows = &hrc
	}
	return h.storedRows.Init(
		h.PbCtx(),
		false,
		h.rows[h.storedSide].Types(),
		h.eqCols[h.storedSide],
		h.nullEquality,
	)
}

var _ execinfrapb.DistSQLSpanStats = &BatchLookupJoinerStats{}

const batchLookupJoinerTagPrefix = "batchlookupjoiner."

// Stats implements the SpanStats interface.
func (bljs *BatchLookupJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := bljs.LeftInputStats.Stats(batchLookupJoinerTagPrefix + "left.")
	rightInputStatsMap := bljs.RightInputStats.Stats(batchLookupJoinerTagPrefix + "right.")
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}

	statsMap[batchLookupJoinerTagPrefix+"stored_side"] = bljs.StoredSide
	statsMap[batchLookupJoinerTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(bljs.MaxAllocatedMem)
	statsMap[batchLookupJoinerTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(bljs.MaxAllocatedDisk)
	statsMap[batchLookupJoinerTagPrefix+"batch_num"] = fmt.Sprintf("%d", bljs.BatchNum)
	return statsMap
}

// TsStats is stats of analyse in time series
func (bljs *BatchLookupJoinerStats) TsStats() map[int32]map[string]string {
	return nil
}

// GetSpanStatsType check type of spanStats
func (bljs *BatchLookupJoinerStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (bljs *BatchLookupJoinerStats) StatsForQueryPlan() []string {
	stats := bljs.LeftInputStats.StatsForQueryPlan("left ")
	stats = append(stats, bljs.RightInputStats.StatsForQueryPlan("right ")...)
	stats = append(stats, fmt.Sprintf("stored side: %s", bljs.StoredSide))

	if bljs.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(bljs.MaxAllocatedMem)))
	}

	if bljs.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(bljs.MaxAllocatedDisk)))
	}

	stats = append(stats, fmt.Sprintf("number of batches: %d", bljs.BatchNum))

	return stats
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (bljs *BatchLookupJoinerStats) TsStatsForQueryPlan() map[int32][]string {
	return nil
}

// outputStatsToTrace outputs the collected batchLookupJoiner stats to the trace. Will
// fail silently if the batchLookupJoiner is not collecting stats.
func (h *batchLookupJoiner) outputStatsToTrace() {
	lis, ok := getInputStats(h.FlowCtx, h.leftSource)
	if !ok {
		return
	}
	ris, ok := getInputStats(h.FlowCtx, h.rightSource)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(h.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp,
			&BatchLookupJoinerStats{
				LeftInputStats:   lis,
				RightInputStats:  ris,
				StoredSide:       h.storedSide.String(),
				MaxAllocatedMem:  h.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: h.diskMonitor.MaximumBytes(),
				BatchNum:         int64(h.batchCount),
			},
		)
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (h *batchLookupJoiner) ChildCount(verbose bool) int {
	if _, ok := h.leftSource.(execinfra.OpNode); ok {
		if _, ok := h.rightSource.(execinfra.OpNode); ok {
			return 2
		}
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (h *batchLookupJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		if n, ok := h.leftSource.(execinfra.OpNode); ok {
			return n
		}
		panic("left input to batchLookupJoiner is not an execinfra.OpNode")
	case 1:
		if n, ok := h.rightSource.(execinfra.OpNode); ok {
			return n
		}
		panic("right input to batchLookupJoiner is not an execinfra.OpNode")
	default:
		panic(fmt.Sprintf("invalid index %d", nth))
	}
}
