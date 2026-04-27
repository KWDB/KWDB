// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// Test the behavior of Run in the presence of errors that switch to the drain
// and forward metadata mode.
func TestRunDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// A source with no rows and 2 ProducerMetadata messages.
	src := &RowChannel{}
	src.InitWithBufSizeAndNumSenders(nil, 10, 1)
	src.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: fmt.Errorf("test")})
	src.Push(nil /* row */, nil /* meta */)
	src.Start(ctx)

	// A receiver that is marked as done consuming rows so that Run will
	// immediately move from forwarding rows and metadata to draining metadata.
	buf := &RowChannel{}
	buf.InitWithBufSizeAndNumSenders(nil, 10, 1)
	buf.ConsumerDone()

	Run(ctx, src, buf, time.Time{})

	if src.ConsumerStatus != DrainRequested {
		t.Fatalf("expected DrainRequested, but found %d", src.ConsumerStatus)
	}
}

// Benchmark a pipeline of RowChannels.
func BenchmarkRowChannelPipeline(b *testing.B) {
	for _, length := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("length=%d", length), func(b *testing.B) {
			rc := make([]RowChannel, length)
			var wg sync.WaitGroup
			wg.Add(len(rc))

			for i := range rc {
				rc[i].InitWithNumSenders(sqlbase.OneIntCol, 1)

				go func(i int) {
					defer wg.Done()
					cur := &rc[i]
					var next *RowChannel
					if i+1 != len(rc) {
						next = &rc[i+1]
					}
					for {
						row, meta := cur.Next()
						if row == nil {
							if next != nil {
								next.ProducerDone()
							}
							break
						}
						if next != nil {
							_ = next.Push(row, meta)
						}
					}
				}(i)
			}

			row := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(1))),
			}
			b.SetBytes(int64(8 * 1 * 1))
			for i := 0; i < b.N; i++ {
				_ = rc[0].Push(row, nil /* meta */)
			}
			rc[0].ProducerDone()
			wg.Wait()
		})
	}
}

func BenchmarkMultiplexedRowChannel(b *testing.B) {
	numRows := 1 << 16
	row := sqlbase.EncDatumRow{sqlbase.IntEncDatum(0)}
	for _, senders := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("senders=%d", senders), func(b *testing.B) {
			b.SetBytes(int64(senders * numRows * 8))
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(senders + 1)
				mrc := &RowChannel{}
				mrc.InitWithNumSenders(sqlbase.OneIntCol, senders)
				go func() {
					for {
						if r, _ := mrc.Next(); r == nil {
							break
						}
					}
					wg.Done()
				}()
				for j := 0; j < senders; j++ {
					go func() {
						for k := 0; k < numRows; k++ {
							mrc.Push(row, nil /* meta */)
						}
						mrc.ProducerDone()
						wg.Done()
					}()
				}
				wg.Wait()
			}
		})
	}
}

// TestRowChannelPush tests the Push method of RowChannel
func TestRowChannelPush(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rc := &RowChannel{}
	rc.InitWithNumSenders(sqlbase.OneIntCol, 1)

	// Test Push with NeedMoreRows status
	row := sqlbase.EncDatumRow{sqlbase.IntEncDatum(1)}
	status := rc.Push(row, nil)
	if status != NeedMoreRows {
		t.Errorf("Expected NeedMoreRows, got %v", status)
	}

	// Test Push with metadata
	meta := &execinfrapb.ProducerMetadata{Err: fmt.Errorf("test error")}
	status = rc.Push(nil, meta)
	if status != NeedMoreRows {
		t.Errorf("Expected NeedMoreRows, got %v", status)
	}

	// Test Push after ConsumerDone
	rc.ConsumerDone()
	status = rc.Push(row, nil)
	if status != DrainRequested {
		t.Errorf("Expected DrainRequested, got %v", status)
	}

	// Test Push after ConsumerClosed
	rc.ConsumerClosed()
	status = rc.Push(row, nil)
	if status != ConsumerClosed {
		t.Errorf("Expected ConsumerClosed, got %v", status)
	}

	rc.ProducerDone()
}

// TestRowChannelNext tests the Next method of RowChannel
func TestRowChannelNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rc := &RowChannel{}
	rc.InitWithNumSenders(sqlbase.OneIntCol, 1)

	// Push a row and test Next
	row := sqlbase.EncDatumRow{sqlbase.IntEncDatum(1)}
	rc.Push(row, nil)

	resultRow, resultMeta := rc.Next()
	if resultRow == nil {
		t.Errorf("Expected row, got nil")
	}
	if resultMeta != nil {
		t.Errorf("Expected nil meta, got %v", resultMeta)
	}

	// Push metadata and test Next
	meta := &execinfrapb.ProducerMetadata{Err: fmt.Errorf("test error")}
	rc.Push(nil, meta)

	resultRow, resultMeta = rc.Next()
	if resultRow != nil {
		t.Errorf("Expected nil row, got %v", resultRow)
	}
	if resultMeta == nil {
		t.Errorf("Expected meta, got nil")
	}

	rc.ProducerDone()

	// Test Next after channel is closed
	resultRow, resultMeta = rc.Next()
	if resultRow != nil || resultMeta != nil {
		t.Errorf("Expected nil row and meta, got row=%v, meta=%v", resultRow, resultMeta)
	}
}

// TestDrainAndForwardMetadata tests the DrainAndForwardMetadata function
func TestDrainAndForwardMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a source with metadata
	src := &RowChannel{}
	src.InitWithNumSenders(nil, 1)
	src.Push(nil, &execinfrapb.ProducerMetadata{Err: fmt.Errorf("test")})
	src.Push(nil, nil)
	src.Start(ctx)

	// Create a receiver
	dst := &RowChannel{}
	dst.InitWithNumSenders(nil, 1)

	DrainAndForwardMetadata(ctx, src, dst)

	// Check that we received the metadata
	_, meta := dst.Next()
	if meta == nil || meta.Err == nil {
		t.Errorf("Expected metadata with error, got meta=%v", meta)
	}

	src.ProducerDone()
	dst.ProducerDone()
}

// TestNoMetadataRowSource tests the NoMetadataRowSource
func TestNoMetadataRowSource(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a source with rows and metadata
	src := &RowChannel{}
	src.InitWithNumSenders(sqlbase.OneIntCol, 1)
	row1 := sqlbase.EncDatumRow{sqlbase.IntEncDatum(1)}
	row2 := sqlbase.EncDatumRow{sqlbase.IntEncDatum(2)}
	src.Push(row1, nil)
	src.Push(nil, &execinfrapb.ProducerMetadata{Err: nil}) // Non-error metadata
	src.Push(row2, nil)
	src.Start(ctx)

	// Create a receiver for metadata
	metaSink := &RowChannel{}
	metaSink.InitWithNumSenders(nil, 1)

	// Create NoMetadataRowSource
	nmrs := MakeNoMetadataRowSource(src, metaSink)

	// Test NextRow
	resultRow, err := nmrs.NextRow()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if resultRow == nil {
		t.Errorf("Expected row, got nil")
	}

	// Test NextRow again (should skip metadata)
	resultRow, err = nmrs.NextRow()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if resultRow == nil {
		t.Errorf("Expected row, got nil")
	}

	src.ProducerDone()
	metaSink.ProducerDone()
}

// TestRowSourceBase tests the rowSourceBase methods
func TestRowSourceBase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rb := &rowSourceBase{}

	// Test consumerDone
	rb.consumerDone()
	if rb.ConsumerStatus != DrainRequested {
		t.Errorf("Expected DrainRequested, got %v", rb.ConsumerStatus)
	}

	// Test consumerClosed
	rb.consumerClosed("test")
	if rb.ConsumerStatus != ConsumerClosed {
		t.Errorf("Expected ConsumerClosed, got %v", rb.ConsumerStatus)
	}
}

// TestRunWithError tests the Run function with an error in the source
func TestRunWithError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a source that panics
	src := &panicRowSource{}
	src.Start(ctx)

	// Create a receiver
	dst := &RowChannel{}
	dst.InitWithNumSenders(nil, 1)

	Run(ctx, src, dst, time.Time{})

	// Check that we received the error
	_, meta := dst.Next()
	if meta == nil || meta.Err == nil {
		t.Errorf("Expected metadata with error, got meta=%v", meta)
	}

	// src.ConsumerClosed() is called by HandleConsumerClosed
	// dst.ProducerDone() is called by HandleConsumerClosed
}

// panicRowSource is a RowSource that panics when Next() is called

type panicRowSource struct{}

func (prs *panicRowSource) OutputTypes() []types.T {
	return nil
}

func (prs *panicRowSource) Start(ctx context.Context) context.Context {
	return ctx
}

func (prs *panicRowSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	panic("test panic")
}

func (prs *panicRowSource) ConsumerDone() {}

func (prs *panicRowSource) ConsumerClosed() {}

func (prs *panicRowSource) InitProcessorProcedure(txn *kv.Txn) {}
