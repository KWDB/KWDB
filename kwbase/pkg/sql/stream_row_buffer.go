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

package sql

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// streamDiskBackedRowBuffer stores the data in disk.
type streamDiskBackedRowBuffer struct {
	rows        *rowcontainer.DiskBackedRowContainer
	iter        rowcontainer.RowIterator
	memMonitor  *mon.BytesMonitor
	diskMonitor *mon.BytesMonitor
}

// streamMemRowBuffer stores data in memory.
type streamMemRowBuffer struct {
	rows       *rowcontainer.MemRowContainer
	iter       rowcontainer.RowIterator
	memMonitor *mon.BytesMonitor
}

type streamReaderRowBuffer struct {
	// ctx is context.
	ctx context.Context
	// rowBufferRWMutex is used to ensure concurrent read and write operations on the cache.
	rowBufferRWMutex syncutil.RWMutex

	// streamOpts is options of stream.
	streamOpts *sqlutil.ParsedStreamOptions
	// recalculator is used to recalculate expired data.
	recalculator *streamRecalculator
	// hasAgg indicates that the current stream has aggregations.
	hasAgg bool

	// unordered received row buffer
	receivedRowsBuffer *streamDiskBackedRowBuffer

	// ordered in-memory buffer of out-of-order rows
	outOfOrderRowBuffer *streamMemRowBuffer

	// ordered output row buffer
	outputRowsBuffer *streamDiskBackedRowBuffer
}

func newStreamReaderRowBuffer(
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.StreamReaderSpec,
	ordering sqlbase.ColumnOrdering,
	streamOpts *sqlutil.ParsedStreamOptions,
	recalculator *streamRecalculator,
	hasAgg bool,
) *streamReaderRowBuffer {
	buffer := &streamReaderRowBuffer{}
	buffer.ctx = flowCtx.EvalCtx.Ctx()
	buffer.streamOpts = streamOpts
	buffer.recalculator = recalculator
	buffer.hasAgg = hasAgg

	// Limit the memory use by creating a child monitor with a hard limit.
	// The rows in receivedRowsBuffer and outputRowsBuffer will overflow to disk if this limit is not enough.

	{
		buffer.receivedRowsBuffer = &streamDiskBackedRowBuffer{}

		buffer.receivedRowsBuffer.memMonitor = execinfra.NewLimitedMonitor(
			buffer.ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg,
			"stream-reader-receive-limited",
		)
		buffer.receivedRowsBuffer.diskMonitor = execinfra.NewMonitor(
			flowCtx.EvalCtx.Ctx(), flowCtx.Cfg.DiskMonitor,
			"stream-reader-receive-disk",
		)

		buffer.receivedRowsBuffer.rows = &rowcontainer.DiskBackedRowContainer{}
		buffer.receivedRowsBuffer.rows.Init(
			nil,
			spec.CDCColumns.CDCTypes,
			flowCtx.EvalCtx,
			flowCtx.Cfg.TempStorage,
			buffer.receivedRowsBuffer.memMonitor,
			buffer.receivedRowsBuffer.diskMonitor,
			0, /* rowCapacity */
		)
	}

	{
		memRowBuffer := &streamMemRowBuffer{}
		memRowBuffer.memMonitor = execinfra.NewMonitor(buffer.ctx, flowCtx.EvalCtx.Mon, "stream-reader-disorder-buffer")

		memRowBuffer.rows = &rowcontainer.MemRowContainer{}
		memRowBuffer.rows.InitWithMon(
			ordering /* ordering */, spec.CDCColumns.CDCTypes, flowCtx.EvalCtx, memRowBuffer.memMonitor, 0, /* rowCapacity */
		)

		memRowBuffer.iter = memRowBuffer.rows.NewIterator(buffer.ctx)
		memRowBuffer.iter.Rewind()
		buffer.outOfOrderRowBuffer = memRowBuffer
	}

	{
		buffer.outputRowsBuffer = &streamDiskBackedRowBuffer{}

		buffer.outputRowsBuffer.memMonitor = execinfra.NewLimitedMonitor(
			buffer.ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg,
			"stream-reader-output-limited",
		)
		buffer.outputRowsBuffer.diskMonitor = execinfra.NewMonitor(
			flowCtx.EvalCtx.Ctx(), flowCtx.Cfg.DiskMonitor,
			"stream-reader-output-disk",
		)

		outRows := rowcontainer.DiskBackedRowContainer{}
		outRows.Init(
			ordering,
			spec.CDCColumns.CDCTypes,
			flowCtx.EvalCtx,
			flowCtx.Cfg.TempStorage,
			buffer.outputRowsBuffer.memMonitor,
			buffer.outputRowsBuffer.diskMonitor,
			0, /* rowCapacity */
		)
		buffer.outputRowsBuffer.rows = &outRows

		buffer.outputRowsBuffer.iter = buffer.outputRowsBuffer.rows.NewFinalIterator(buffer.ctx)
		buffer.outputRowsBuffer.iter.Rewind()
	}

	return buffer
}

// AddRowWithoutLock needs the caller requests/releases the lock of write via Lock()/Unlock() method
func (s *streamReaderRowBuffer) AddRowWithoutLock(
	ctx context.Context, row sqlbase.EncDatumRow,
) error {
	err := s.receivedRowsBuffer.rows.AddRow(ctx, row)
	if err != nil {
		return err
	}

	return nil
}

// AddRow adds the row to the received buffer.
func (s *streamReaderRowBuffer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	s.rowBufferRWMutex.Lock()
	defer s.rowBufferRWMutex.Unlock()
	err := s.receivedRowsBuffer.rows.AddRow(ctx, row)
	if err != nil {
		return err
	}

	return nil
}

// AddRowOutput directly adds the row to the output buffer, skipping the sorting process.
func (s *streamReaderRowBuffer) AddRowOutput(ctx context.Context, row sqlbase.EncDatumRow) error {
	err := s.outputRowsBuffer.rows.AddRow(ctx, row)
	if err != nil {
		return err
	}

	return nil
}

// EncFirstRow returns the first row as an EncDatumRow.
func (s *streamReaderRowBuffer) EncFirstRow() (sqlbase.EncDatumRow, error) {
	// The Read Lock is enough here since, on the emitting phase, the only caller is streamReaderProcessor.Next().
	s.rowBufferRWMutex.RLock()
	defer s.rowBufferRWMutex.RUnlock()
	if s.outputRowsBuffer.iter == nil {
		return nil, nil
	}

	valid, err := s.outputRowsBuffer.iter.Valid()
	if err != nil {
		return nil, err
	}

	if valid {
		row, err := s.outputRowsBuffer.iter.Row()
		if err != nil {
			return nil, err
		}
		return row, nil
	}

	return nil, nil
}

// Next points the iterator to the next piece of data.
func (s *streamReaderRowBuffer) Next() {
	// The Read Lock is enough here since, on the emitting phase, the only caller is streamReaderProcessor.Next().
	s.rowBufferRWMutex.RLock()
	defer s.rowBufferRWMutex.RUnlock()
	s.outputRowsBuffer.iter.Next()
}

// HasOutputRow checks if there is data ready for output.
func (s *streamReaderRowBuffer) HasOutputRow() (bool, error) {
	s.rowBufferRWMutex.RLock()
	defer s.rowBufferRWMutex.RUnlock()
	if s.outputRowsBuffer.iter == nil {
		return false, nil
	}
	return s.outputRowsBuffer.iter.Valid()
}

// HasOrderRow checks if there is sorted data.
func (s *streamReaderRowBuffer) HasOrderRow() bool {
	s.rowBufferRWMutex.Lock()
	defer s.rowBufferRWMutex.Unlock()

	return s.outOfOrderRowBuffer.rows.Len() > 0
}

func (s *streamReaderRowBuffer) emitReceivedRowsToOutputBuffer(
	expiredTime time.Time, emitTimestamp time.Time,
) error {
	s.rowBufferRWMutex.Lock()
	defer s.rowBufferRWMutex.Unlock()

	if s.outputRowsBuffer.iter != nil {
		validOut, err := s.outputRowsBuffer.iter.Valid()
		if err != nil {
			return err
		}
		// the outputRowsBuffer is not empty
		if validOut {
			return nil
		}

		s.outputRowsBuffer.iter.Close()
		s.outputRowsBuffer.iter = nil

		if err := s.outputRowsBuffer.rows.UnsafeReset(s.ctx); err != nil {
			return err
		}
	}

	s.receivedRowsBuffer.iter = s.receivedRowsBuffer.rows.NewFinalIterator(s.ctx)
	s.receivedRowsBuffer.iter.Rewind()

	validReceived, err := s.receivedRowsBuffer.iter.Valid()
	if err != nil {
		return err
	}

	var rowTs time.Time
	var row sqlbase.EncDatumRow

	// the received row buffer is not empty
	if validReceived {
		receivedIter := s.receivedRowsBuffer.iter

		for {
			valid, err := receivedIter.Valid()
			if err != nil {
				return err
			}
			if valid {
				row, err = receivedIter.Row()
				if err != nil {
					return err
				}

				err := s.outOfOrderRowBuffer.rows.AddRow(s.ctx, row)
				if err != nil {
					return err
				}

				receivedIter.Next()
			} else {
				break
			}
		}
		if err := s.receivedRowsBuffer.rows.UnsafeReset(s.ctx); err != nil {
			return err
		}
	}

	recordNum := s.outOfOrderRowBuffer.rows.Len()
	// the out-of-order buffer is empty
	if recordNum == 0 {
		return nil
	}

	// has new incoming rows
	if validReceived {
		s.outOfOrderRowBuffer.rows.Sort(s.ctx)
	}

	for idx := 0; idx < recordNum; idx++ {
		outRow := s.outOfOrderRowBuffer.rows.EncRow(0)
		s.outOfOrderRowBuffer.rows.PopFirst()

		switch outRow[0].Datum.ResolvedType().InternalType.Family {
		case types.TimestampFamily:
			rowTs = outRow[0].Datum.(*tree.DTimestamp).UTC()
		case types.TimestampTZFamily:
			rowTs = outRow[0].Datum.(*tree.DTimestampTZ).UTC()
		default:
			break
		}

		if s.hasAgg {
			// if it belongs to the previous emitted batch
			if rowTs.Before(expiredTime) {
				// the current row is expired one, add it to the stream recalculator
				if !s.streamOpts.IgnoreExpired {
					// copy the original row to a new EncDatumRow
					s.recalculator.HandleExpiredRows(outRow.Copy())
				}
				continue
			} else if rowTs.After(emitTimestamp) {
				err := s.outOfOrderRowBuffer.rows.AddRow(s.ctx, outRow)
				if err != nil {
					return err
				}
				continue
			}
		}

		if err := s.outputRowsBuffer.rows.AddRow(s.ctx, outRow); err != nil {
			return err
		}
	}

	s.outputRowsBuffer.iter = s.outputRowsBuffer.rows.NewFinalIterator(s.ctx)
	s.outputRowsBuffer.iter.Rewind()

	return nil
}

// Lock locks the buffer.
func (s *streamReaderRowBuffer) Lock() {
	s.rowBufferRWMutex.Lock()
}

// Unlock unlocks the buffer.
func (s *streamReaderRowBuffer) Unlock() {
	s.rowBufferRWMutex.Unlock()
}

// Close closes the buffer.
func (s *streamReaderRowBuffer) Close() {
	s.receivedRowsBuffer.rows.Close(s.ctx)
	s.receivedRowsBuffer.memMonitor.Stop(s.ctx)
	s.receivedRowsBuffer.diskMonitor.Stop(s.ctx)

	s.outOfOrderRowBuffer.rows.Close(s.ctx)
	s.outOfOrderRowBuffer.memMonitor.Stop(s.ctx)

	s.outputRowsBuffer.rows.Close(s.ctx)
	s.outputRowsBuffer.memMonitor.Stop(s.ctx)
	s.outputRowsBuffer.diskMonitor.Stop(s.ctx)
}
