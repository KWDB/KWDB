// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// mockRowSource is a mock implementation of RowSource for testing
func mockRowSource(rows []sqlbase.EncDatumRow, metadata []*execinfrapb.ProducerMetadata) RowSource {
	return &mockRowSourceImpl{
		rows:     rows,
		metadata: metadata,
		idx:      0,
	}
}

// newRowChannel creates a properly initialized RowChannel for testing
func newRowChannel() *RowChannel {
	rc := &RowChannel{}
	rc.InitWithNumSenders([]types.T{*types.Int}, 1)
	return rc
}

type mockRowSourceImpl struct {
	rows     []sqlbase.EncDatumRow
	metadata []*execinfrapb.ProducerMetadata
	idx      int
}

func (m *mockRowSourceImpl) Start(ctx context.Context) context.Context {
	return ctx
}

func (m *mockRowSourceImpl) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if m.idx < len(m.metadata) {
		meta := m.metadata[m.idx]
		m.idx++
		return nil, meta
	}
	if m.idx-len(m.metadata) < len(m.rows) {
		row := m.rows[m.idx-len(m.metadata)]
		m.idx++
		return row, nil
	}
	return nil, nil
}

func (m *mockRowSourceImpl) ConsumerDone() {}

func (m *mockRowSourceImpl) ConsumerClosed() {}

func (m *mockRowSourceImpl) OutputTypes() []types.T {
	return []types.T{*types.Int}
}

func (m *mockRowSourceImpl) Push(n sqlbase.EncDatumRow) bool {
	return true
}

func (m *mockRowSourceImpl) Close() {}

func (m *mockRowSourceImpl) InitProcessorProcedure(txn *kv.Txn) {}

func (m *mockRowSourceImpl) SetOutputTypes(types []types.T) {}

func (m *mockRowSourceImpl) GetOutputTypes() []types.T {
	return m.OutputTypes()
}

func TestNewMetadataTestReceiver(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	input := mockRowSource(nil, nil)
	senders := []string{"sender1", "sender2"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	if mtr == nil {
		t.Fatal("NewMetadataTestReceiver returned nil")
	}

	if len(mtr.senders) != len(senders) {
		t.Errorf("Expected %d senders, got %d", len(senders), len(mtr.senders))
	}

	if len(mtr.rowCounts) != 0 {
		t.Errorf("Expected empty rowCounts, got %d entries", len(mtr.rowCounts))
	}
}

func TestMetadataTestReceiverCheckRowNumMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	senders := []string{"sender1", "sender2"}
	input := mockRowSource(nil, nil)

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Test case 1: Missing senders
	meta := mtr.checkRowNumMetadata()
	if meta == nil {
		t.Error("Expected error metadata for missing senders")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}

	// Test case 2: Add row counts and test again
	mtr = &MetadataTestReceiver{
		senders: senders,
		rowCounts: map[string]rowNumCounter{
			"sender1": {expected: 2, actual: 2, seen: util.MakeFastIntSet(0, 1)},
			"sender2": {expected: 3, actual: 3, seen: util.MakeFastIntSet(0, 1, 2)},
		},
	}

	meta = mtr.checkRowNumMetadata()
	if meta != nil {
		t.Errorf("Expected no error metadata, got: %v", meta.Err)
	}

	// Test case 3: Mismatched expected and actual counts
	mtr = &MetadataTestReceiver{
		senders: senders,
		rowCounts: map[string]rowNumCounter{
			"sender1": {expected: 2, actual: 1, seen: util.MakeFastIntSet(0)},
			"sender2": {expected: 3, actual: 3, seen: util.MakeFastIntSet(0, 1, 2)},
		},
	}

	meta = mtr.checkRowNumMetadata()
	if meta == nil {
		t.Error("Expected error metadata for mismatched counts")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}

	// Test case 4: Missing row numbers
	mtr = &MetadataTestReceiver{
		senders: senders,
		rowCounts: map[string]rowNumCounter{
			"sender1": {expected: 2, actual: 2, seen: util.MakeFastIntSet(0)},
			"sender2": {expected: 3, actual: 3, seen: util.MakeFastIntSet(0, 1, 2)},
		},
	}

	meta = mtr.checkRowNumMetadata()
	if meta == nil {
		t.Error("Expected error metadata for missing row numbers")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}

	// Test case 5: Error in row count
	mtr = &MetadataTestReceiver{
		senders: senders,
		rowCounts: map[string]rowNumCounter{
			"sender1": {err: fmt.Errorf("test error")},
			"sender2": {expected: 3, actual: 3, seen: util.MakeFastIntSet(0, 1, 2)},
		},
	}

	meta = mtr.checkRowNumMetadata()
	if meta == nil {
		t.Error("Expected error metadata for row count error")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}
}

func TestMetadataTestReceiverInitProcessorProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	txn := &kv.Txn{}

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	// Test case 1: IsProcedure is false
	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	input := mockRowSource(nil, nil)
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Set IsProcedure to false
	if mtr.EvalCtx == nil {
		mtr.EvalCtx = &tree.EvalContext{}
	}
	mtr.EvalCtx.IsProcedure = false

	mtr.InitProcessorProcedure(txn)

	// Verify no changes were made
	if mtr.FlowCtx.Txn == txn {
		t.Error("FlowCtx.Txn should not be set when IsProcedure is false")
	}

	// Test case 2: IsProcedure is true
	mtr.EvalCtx.IsProcedure = true
	mtr.InitProcessorProcedure(txn)

	// Verify changes were made
	if mtr.FlowCtx.Txn != txn {
		t.Error("FlowCtx.Txn should be set when IsProcedure is true")
	}

	if mtr.Closed {
		t.Error("Closed should be false after InitProcessorProcedure")
	}

	if mtr.State != StateRunning {
		t.Errorf("State should be StateRunning, got %v", mtr.State)
	}
}

func TestMetadataTestReceiverStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	input := mockRowSource(nil, nil)
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	ctx := context.Background()
	newCtx := mtr.Start(ctx)

	if newCtx == nil {
		t.Error("Start should return a non-nil context")
	}
}

func TestMetadataTestReceiverNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	// Create test rows
	row1 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
	row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2))}

	// Create test metadata
	rowNumMeta1 := &execinfrapb.ProducerMetadata{
		RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
			SenderID: "sender1",
			RowNum:   1,
			LastMsg:  false,
		},
	}

	rowNumMeta2 := &execinfrapb.ProducerMetadata{
		RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
			SenderID: "sender1",
			RowNum:   2,
			LastMsg:  true,
		},
	}

	errMeta := &execinfrapb.ProducerMetadata{
		Err: fmt.Errorf("test error"),
	}

	input := mockRowSource([]sqlbase.EncDatumRow{row1, row2}, []*execinfrapb.ProducerMetadata{rowNumMeta1, rowNumMeta2, errMeta})
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mtr.Start(ctx)

	// Test Next() with rowNum metadata
	row, meta := mtr.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata for rowNum")
	}

	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata for rowNum")
	}

	// Error metadata is stored in trailingErrMeta and not returned immediately
	// So the next calls will return rows
	// Test Next() with rows
	row, meta = mtr.Next()
	if row == nil {
		t.Error("Expected row")
	}
	if meta != nil {
		t.Error("Expected nil metadata for row")
	}

	row, meta = mtr.Next()
	if row == nil {
		t.Error("Expected row")
	}
	if meta != nil {
		t.Error("Expected nil metadata for row")
	}

	// Test Next() with checkRowNumMetadata error (missing RowNum #2)
	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row for checkRowNumMetadata error")
	}
	if meta == nil {
		t.Error("Expected checkRowNumMetadata error metadata")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}

	// Test Next() with trailing error metadata
	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row for trailing error metadata")
	}
	if meta == nil {
		t.Error("Expected trailing error metadata")
	} else if meta.Err == nil {
		t.Error("Expected error in metadata")
	}

	// Test Next() with no more data
	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row")
	}
	if meta != nil {
		t.Error("Expected nil metadata")
	}
}

func TestMetadataTestReceiverConsumerDone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	input := mockRowSource(nil, nil)
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Just verify it doesn't panic
	mtr.ConsumerDone()
}

func TestMetadataTestReceiverConsumerClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	input := mockRowSource(nil, nil)
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Just verify it doesn't panic
	mtr.ConsumerClosed()
}

func TestMetadataTestReceiverRowNumValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}

	flowCtx := &FlowCtx{
		Cfg: &ServerConfig{
			Stopper: stopper,
		},
		EvalCtx: evalCtx,
	}

	// Test case: Repeated LastMsg
	repeatedLastMsgMeta1 := &execinfrapb.ProducerMetadata{
		RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
			SenderID: "sender1",
			RowNum:   2,
			LastMsg:  true,
		},
	}

	repeatedLastMsgMeta2 := &execinfrapb.ProducerMetadata{
		RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
			SenderID: "sender1",
			RowNum:   3,
			LastMsg:  true,
		},
	}

	input := mockRowSource(nil, []*execinfrapb.ProducerMetadata{repeatedLastMsgMeta1, repeatedLastMsgMeta2})
	senders := []string{"sender1"}

	mtr, err := NewMetadataTestReceiver(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{}, /* post */
		newRowChannel(),                /* output */
		senders,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestReceiver failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mtr.Start(ctx)

	// First metadata should be processed
	row, meta := mtr.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata")
	}

	// Second metadata with repeated LastMsg - error is stored but original meta is returned
	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata for repeated LastMsg")
	}

	// Third call - since error was stored in rowCounts, same meta is returned again
	row, meta = mtr.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata after error detection")
	}
}
