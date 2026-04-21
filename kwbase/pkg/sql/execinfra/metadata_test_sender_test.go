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
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// mockRowSourceForSender is a mock implementation of RowSource for testing MetadataTestSender
type mockRowSourceForSender struct {
	rows     []sqlbase.EncDatumRow
	metadata []*execinfrapb.ProducerMetadata
	idx      int
	started  bool
}

func (m *mockRowSourceForSender) Start(ctx context.Context) context.Context {
	m.started = true
	return ctx
}

func (m *mockRowSourceForSender) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

func (m *mockRowSourceForSender) ConsumerDone() {}

func (m *mockRowSourceForSender) ConsumerClosed() {}

func (m *mockRowSourceForSender) OutputTypes() []types.T {
	return []types.T{*types.Int}
}

func (m *mockRowSourceForSender) Push(n sqlbase.EncDatumRow) bool {
	return true
}

func (m *mockRowSourceForSender) Close() {}

func (m *mockRowSourceForSender) InitProcessorProcedure(txn *kv.Txn) {}

func (m *mockRowSourceForSender) SetOutputTypes(types []types.T) {}

func (m *mockRowSourceForSender) GetOutputTypes() []types.T {
	return m.OutputTypes()
}

func TestNewMetadataTestSender(t *testing.T) {
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

	input := &mockRowSourceForSender{}
	output := newRowChannel()
	senderID := "test-sender-1"

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		senderID,
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	if mts == nil {
		t.Fatal("NewMetadataTestSender returned nil")
	}

	if mts.id != senderID {
		t.Errorf("Expected id %s, got %s", senderID, mts.id)
	}

	if mts.rowNumCnt != 0 {
		t.Errorf("Expected rowNumCnt 0, got %d", mts.rowNumCnt)
	}

	if mts.sendRowNumMeta {
		t.Error("Expected sendRowNumMeta to be false initially")
	}
}

func TestMetadataTestSenderStart(t *testing.T) {
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

	input := &mockRowSourceForSender{}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	ctx := context.Background()
	newCtx := mts.Start(ctx)

	if newCtx == nil {
		t.Error("Start should return a non-nil context")
	}

	if !input.started {
		t.Error("Input should have been started")
	}
}

func TestMetadataTestSenderNext(t *testing.T) {
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

	input := &mockRowSourceForSender{
		rows: []sqlbase.EncDatumRow{row1, row2},
	}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mts.Start(ctx)

	// First call should return row1
	row, meta := mts.Next()
	if row == nil {
		t.Error("Expected row1")
	}
	if meta != nil {
		t.Error("Expected nil metadata for row")
	}

	// Second call should return RowNum metadata for row1
	row, meta = mts.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected RowNum metadata")
	} else {
		if meta.RowNum == nil {
			t.Error("Expected RowNum in metadata")
		} else {
			if meta.RowNum.RowNum != 1 {
				t.Errorf("Expected RowNum 1, got %d", meta.RowNum.RowNum)
			}
			if meta.RowNum.LastMsg {
				t.Error("Expected LastMsg to be false")
			}
			if meta.RowNum.SenderID != "test-sender" {
				t.Errorf("Expected SenderID 'test-sender', got %s", meta.RowNum.SenderID)
			}
		}
	}

	// Third call should return row2
	row, meta = mts.Next()
	if row == nil {
		t.Error("Expected row2")
	}
	if meta != nil {
		t.Error("Expected nil metadata for row")
	}

	// Fourth call should return RowNum metadata for row2
	row, meta = mts.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected RowNum metadata")
	} else {
		if meta.RowNum == nil {
			t.Error("Expected RowNum in metadata")
		} else {
			if meta.RowNum.RowNum != 2 {
				t.Errorf("Expected RowNum 2, got %d", meta.RowNum.RowNum)
			}
		}
	}

	// Fifth call should start draining and return trailing metadata
	row, meta = mts.Next()
	if row != nil {
		t.Error("Expected nil row for trailing metadata")
	}
	if meta == nil {
		t.Error("Expected trailing metadata")
	} else {
		if meta.RowNum == nil {
			t.Error("Expected RowNum in trailing metadata")
		} else {
			if !meta.RowNum.LastMsg {
				t.Error("Expected LastMsg to be true in trailing metadata")
			}
			if meta.RowNum.RowNum != 2 {
				t.Errorf("Expected RowNum 2 in trailing metadata, got %d", meta.RowNum.RowNum)
			}
		}
	}

	// Sixth call should return nil, nil
	row, meta = mts.Next()
	if row != nil {
		t.Error("Expected nil row")
	}
	if meta != nil {
		t.Error("Expected nil metadata")
	}
}

func TestMetadataTestSenderNextWithMetadata(t *testing.T) {
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

	// Create test metadata
	testMeta := &execinfrapb.ProducerMetadata{
		Err: fmt.Errorf("test error"),
	}

	input := &mockRowSourceForSender{
		metadata: []*execinfrapb.ProducerMetadata{testMeta},
	}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mts.Start(ctx)

	// First call should return the metadata from input
	row, meta := mts.Next()
	if row != nil {
		t.Error("Expected nil row for metadata")
	}
	if meta == nil {
		t.Error("Expected metadata")
	}
}

func TestMetadataTestSenderNextNoData(t *testing.T) {
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

	input := &mockRowSourceForSender{}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mts.Start(ctx)

	// First call should return trailing metadata with LastMsg and rowNumCnt=0
	row, meta := mts.Next()
	if row != nil {
		t.Error("Expected nil row for trailing metadata")
	}
	if meta == nil {
		t.Error("Expected trailing metadata")
	} else {
		if meta.RowNum == nil {
			t.Error("Expected RowNum in trailing metadata")
		} else {
			if !meta.RowNum.LastMsg {
				t.Error("Expected LastMsg to be true")
			}
			if meta.RowNum.RowNum != 0 {
				t.Errorf("Expected RowNum 0, got %d", meta.RowNum.RowNum)
			}
		}
	}

	// Second call should return nil, nil
	row, meta = mts.Next()
	if row != nil {
		t.Error("Expected nil row")
	}
	if meta != nil {
		t.Error("Expected nil metadata")
	}
}

func TestMetadataTestSenderConsumerClosed(t *testing.T) {
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

	input := &mockRowSourceForSender{}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Just verify it doesn't panic
	mts.ConsumerClosed()
}

func TestMetadataTestSenderInitProcessorProcedure(t *testing.T) {
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

	input := &mockRowSourceForSender{}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Set IsProcedure to false
	if mts.EvalCtx == nil {
		mts.EvalCtx = &tree.EvalContext{}
	}
	mts.EvalCtx.IsProcedure = false

	mts.InitProcessorProcedure(txn)

	// Verify no changes were made
	if mts.FlowCtx.Txn == txn {
		t.Error("FlowCtx.Txn should not be set when IsProcedure is false")
	}

	// Test case 2: IsProcedure is true
	mts.EvalCtx.IsProcedure = true
	mts.InitProcessorProcedure(txn)

	// Verify changes were made
	if mts.FlowCtx.Txn != txn {
		t.Error("FlowCtx.Txn should be set when IsProcedure is true")
	}

	if mts.Closed {
		t.Error("Closed should be false after InitProcessorProcedure")
	}

	if mts.State != StateRunning {
		t.Errorf("State should be StateRunning, got %v", mts.State)
	}
}

func TestMetadataTestSenderRowNumCounter(t *testing.T) {
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

	// Create multiple test rows
	rows := make([]sqlbase.EncDatumRow, 5)
	for i := 0; i < 5; i++ {
		rows[i] = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i+1)))}
	}

	input := &mockRowSourceForSender{
		rows: rows,
	}
	output := newRowChannel()

	mts, err := NewMetadataTestSender(
		flowCtx,
		1,
		input,
		&execinfrapb.PostProcessSpec{},
		output,
		"test-sender",
	)

	if err != nil {
		t.Fatalf("NewMetadataTestSender failed: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	mts.Start(ctx)

	// Process all rows and verify rowNumCnt
	expectedRowNum := int32(0)
	for i := 0; i < 5; i++ {
		// Get row
		row, meta := mts.Next()
		if row == nil {
			t.Errorf("Expected row %d", i+1)
		}
		if meta != nil {
			t.Errorf("Expected nil metadata for row %d", i+1)
		}

		// Get metadata
		row, meta = mts.Next()
		if row != nil {
			t.Errorf("Expected nil row for metadata %d", i+1)
		}
		if meta == nil {
			t.Errorf("Expected metadata for row %d", i+1)
		} else if meta.RowNum != nil {
			expectedRowNum++
			if meta.RowNum.RowNum != expectedRowNum {
				t.Errorf("Expected RowNum %d, got %d", expectedRowNum, meta.RowNum.RowNum)
			}
		}
	}

	if mts.rowNumCnt != 5 {
		t.Errorf("Expected rowNumCnt 5, got %d", mts.rowNumCnt)
	}

	// Get trailing metadata
	row, meta := mts.Next()
	if row != nil {
		t.Error("Expected nil row for trailing metadata")
	}
	if meta == nil {
		t.Error("Expected trailing metadata")
	} else if meta.RowNum != nil {
		if !meta.RowNum.LastMsg {
			t.Error("Expected LastMsg to be true in trailing metadata")
		}
		if meta.RowNum.RowNum != 5 {
			t.Errorf("Expected RowNum 5 in trailing metadata, got %d", meta.RowNum.RowNum)
		}
	}
}
