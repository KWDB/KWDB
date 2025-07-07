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

package distsqlutils

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// BufferedRecord represents a row or metadata record that has been buffered
// inside a RowBuffer.
type BufferedRecord struct {
	row  sqlbase.EncDatumRow
	Meta *execinfrapb.ProducerMetadata
}

// RowBuffer is an implementation of RowReceiver that buffers (accumulates)
// results in memory, as well as an implementation of RowSource that returns
// records from a record buffer. Just for tests.
type RowBuffer struct {
	Mu struct {
		syncutil.Mutex

		// producerClosed is used when the RowBuffer is used as a RowReceiver; it is
		// set to true when the sender calls ProducerDone().
		producerClosed bool

		// records represent the data that has been buffered. Push appends a row
		// to the back, Next removes a row from the front.
		Records []BufferedRecord
	}

	// Done is used when the RowBuffer is used as a RowSource; it is set to true
	// when the receiver read all the rows.
	Done bool

	ConsumerStatus execinfra.ConsumerStatus

	// Schema of the rows in this buffer.
	types []types.T

	args RowBufferArgs
}

// GetCols is part of the distsql.RowReceiver interface.
func (rb *RowBuffer) GetCols() int {
	//TODO unimplemented
	panic("unimplemented")
}

// AddStats record stallTime and number of rows
func (rb *RowBuffer) AddStats(time time.Duration, isAddRows bool) {}

// GetStats get RowStats
func (rb *RowBuffer) GetStats() execinfra.RowStats {
	return execinfra.RowStats{}
}

var _ execinfra.RowReceiver = &RowBuffer{}
var _ execinfra.RowSource = &RowBuffer{}

// RowBufferArgs contains testing-oriented parameters for a RowBuffer.
type RowBufferArgs struct {
	// If not set, then the RowBuffer will behave like a RowChannel and not
	// accumulate rows after it's been put in draining mode. If set, rows will still
	// be accumulated. Useful for tests that want to observe what rows have been
	// pushed after draining.
	AccumulateRowsWhileDraining bool
	// OnConsumerDone, if specified, is called as the first thing in the
	// ConsumerDone() method.
	OnConsumerDone func(*RowBuffer)
	// OnConsumerClose, if specified, is called as the first thing in the
	// ConsumerClosed() method.
	OnConsumerClosed func(*RowBuffer)
	// OnNext, if specified, is called as the first thing in the Next() method.
	// If it returns an empty row and metadata, then RowBuffer.Next() is allowed
	// to run normally. Otherwise, the values are returned from RowBuffer.Next().
	OnNext func(*RowBuffer) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata)
	// OnPush, if specified, is called as the first thing in the Push() method.
	OnPush func(sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata)
}

// NewRowBuffer creates a RowBuffer with the given schema and initial rows.
func NewRowBuffer(types []types.T, rows sqlbase.EncDatumRows, hooks RowBufferArgs) *RowBuffer {
	if types == nil {
		panic("types required")
	}
	wrappedRows := make([]BufferedRecord, len(rows))
	for i, row := range rows {
		wrappedRows[i].row = row
	}
	rb := &RowBuffer{types: types, args: hooks}
	rb.Mu.Records = wrappedRows
	return rb
}

// PushPGResult is part of the RowReceiver interface.
func (rb *RowBuffer) PushPGResult(_ context.Context, _ []byte) error {
	return nil
}

// AddPGComplete implements the rowResultWriter interface.
func (rb *RowBuffer) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

// Push is part of the RowReceiver interface.
func (rb *RowBuffer) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if rb.args.OnPush != nil {
		rb.args.OnPush(row, meta)
	}
	rb.Mu.Lock()
	defer rb.Mu.Unlock()
	if rb.Mu.producerClosed {
		panic("Push called after ProducerDone")
	}
	// We mimic the behavior of RowChannel.
	storeRow := func() {
		rowCopy := append(sqlbase.EncDatumRow(nil), row...)
		rb.Mu.Records = append(rb.Mu.Records, BufferedRecord{row: rowCopy, Meta: meta})
	}
	status := execinfra.ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if rb.args.AccumulateRowsWhileDraining {
		storeRow()
	} else {
		switch status {
		case execinfra.NeedMoreRows:
			storeRow()
		case execinfra.DrainRequested:
			if meta != nil {
				storeRow()
			}
		case execinfra.ConsumerClosed:
		}
	}
	return status
}

// ProducerClosed is a utility function used by tests to check whether the
// RowBuffer has had ProducerDone() called on it.
func (rb *RowBuffer) ProducerClosed() bool {
	rb.Mu.Lock()
	c := rb.Mu.producerClosed
	rb.Mu.Unlock()
	return c
}

// ProducerDone is part of the RowSource interface.
func (rb *RowBuffer) ProducerDone() {
	rb.Mu.Lock()
	defer rb.Mu.Unlock()
	if rb.Mu.producerClosed {
		panic("RowBuffer already closed")
	}
	rb.Mu.producerClosed = true
}

// Types is part of the RowReceiver interface.
func (rb *RowBuffer) Types() []types.T {
	return rb.types
}

// OutputTypes is part of the RowSource interface.
func (rb *RowBuffer) OutputTypes() []types.T {
	if rb.types == nil {
		panic("not initialized")
	}
	return rb.types
}

// Start is part of the RowSource interface.
func (rb *RowBuffer) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
//
// There's no synchronization here with Push(). The assumption is that these
// two methods are not called concurrently.
func (rb *RowBuffer) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rb.args.OnNext != nil {
		row, meta := rb.args.OnNext(rb)
		if row != nil || meta != nil {
			return row, meta
		}
	}
	if len(rb.Mu.Records) == 0 {
		rb.Done = true
		return nil, nil
	}
	rec := rb.Mu.Records[0]
	rb.Mu.Records = rb.Mu.Records[1:]
	return rec.row, rec.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rb *RowBuffer) ConsumerDone() {
	if atomic.CompareAndSwapUint32((*uint32)(&rb.ConsumerStatus),
		uint32(execinfra.NeedMoreRows), uint32(execinfra.DrainRequested)) {
		if rb.args.OnConsumerDone != nil {
			rb.args.OnConsumerDone(rb)
		}
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rb *RowBuffer) ConsumerClosed() {
	status := execinfra.ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if status == execinfra.ConsumerClosed {
		log.Fatalf(context.Background(), "RowBuffer already closed")
	}
	atomic.StoreUint32((*uint32)(&rb.ConsumerStatus), uint32(execinfra.ConsumerClosed))
	if rb.args.OnConsumerClosed != nil {
		rb.args.OnConsumerClosed(rb)
	}
}

// InitProcessorProcedure init processor in procedure
func (rb *RowBuffer) InitProcessorProcedure(txn *kv.Txn) {}

// NextNoMeta is a version of Next which fails the test if
// it encounters any metadata.
func (rb *RowBuffer) NextNoMeta(tb testing.TB) sqlbase.EncDatumRow {
	row, meta := rb.Next()
	if meta != nil {
		tb.Fatalf("unexpected metadata: %v", meta)
	}
	return row
}

// GetRowsNoMeta returns the rows in the buffer; it fails the test if it
// encounters any metadata.
func (rb *RowBuffer) GetRowsNoMeta(t *testing.T) sqlbase.EncDatumRows {
	var res sqlbase.EncDatumRows
	for {
		row := rb.NextNoMeta(t)
		if row == nil {
			break
		}
		res = append(res, row)
	}
	return res
}
