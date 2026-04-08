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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func TestProcOutputHelperInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}
	typs := []types.T{*types.Int, *types.String}
	output := newRowChannel()

	t.Run("empty post", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{}
		err := h.Init(post, typs, evalCtx, output)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		if len(h.OutputTypes) != 2 {
			t.Errorf("Expected 2 output types, got %d", len(h.OutputTypes))
		}
	})

	t.Run("projection", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: []uint32{0},
		}
		err := h.Init(post, typs, evalCtx, output)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		if len(h.OutputTypes) != 1 {
			t.Errorf("Expected 1 output type, got %d", len(h.OutputTypes))
		}
	})

	t.Run("projection without flag", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Projection:    false,
			OutputColumns: []uint32{0},
		}
		err := h.Init(post, typs, evalCtx, output)
		if err == nil {
			t.Error("Expected error for projection without flag")
		}
	})

	t.Run("projection and rendering", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Projection:  true,
			RenderExprs: []execinfrapb.Expression{{}},
		}
		err := h.Init(post, typs, evalCtx, output)
		if err == nil {
			t.Error("Expected error for both projection and rendering")
		}
	})

	t.Run("invalid column", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: []uint32{10},
		}
		err := h.Init(post, typs, evalCtx, output)
		if err == nil {
			t.Error("Expected error for invalid column")
		}
	})
}

func TestProcOutputHelperProcessRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}
	typs := []types.T{*types.Int}
	output := newRowChannel()

	t.Run("basic row processing", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{}
		err := h.Init(post, typs, evalCtx, output)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))}
		outRow, moreRowsOK, err := h.ProcessRow(context.Background(), row)
		if err != nil {
			t.Fatalf("ProcessRow failed: %v", err)
		}
		if outRow == nil {
			t.Error("Expected output row")
		}
		if !moreRowsOK {
			t.Error("Expected moreRowsOK to be true")
		}
	})

	t.Run("offset", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Offset: 1,
		}
		err := h.Init(post, typs, evalCtx, output)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// First row should be suppressed
		row1 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
		outRow, moreRowsOK, err := h.ProcessRow(context.Background(), row1)
		if err != nil {
			t.Fatalf("ProcessRow failed: %v", err)
		}
		if outRow != nil {
			t.Error("Expected first row to be suppressed")
		}
		if !moreRowsOK {
			t.Error("Expected moreRowsOK to be true")
		}

		// Second row should pass through
		row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2))}
		outRow, moreRowsOK, err = h.ProcessRow(context.Background(), row2)
		if err != nil {
			t.Fatalf("ProcessRow failed: %v", err)
		}
		if outRow == nil {
			t.Error("Expected second row")
		}
	})

	t.Run("limit", func(t *testing.T) {
		h := &ProcOutputHelper{}
		post := &execinfrapb.PostProcessSpec{
			Limit: 1,
		}
		err := h.Init(post, typs, evalCtx, output)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// First row should pass through
		row1 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
		outRow, moreRowsOK, err := h.ProcessRow(context.Background(), row1)
		if err != nil {
			t.Fatalf("ProcessRow failed: %v", err)
		}
		if outRow == nil {
			t.Error("Expected first row")
		}
		if moreRowsOK {
			t.Error("Expected moreRowsOK to be false after limit reached")
		}

		// Second row should be suppressed
		row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2))}
		outRow, moreRowsOK, err = h.ProcessRow(context.Background(), row2)
		if err != nil {
			t.Fatalf("ProcessRow failed: %v", err)
		}
		if outRow != nil {
			t.Error("Expected second row to be suppressed")
		}
	})
}

func TestProcOutputHelperSetRowIdx(t *testing.T) {
	defer leaktest.AfterTest(t)()

	h := &ProcOutputHelper{}
	h.SetRowIdx(42)
	if h.rowIdx != 42 {
		t.Errorf("Expected rowIdx 42, got %d", h.rowIdx)
	}
}

func TestProcOutputHelperReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	h := &ProcOutputHelper{}
	h.renderExprs = make([]ExprHelper, 2)
	h.OutputTypes = []types.T{*types.Int}

	h.Reset()

	if len(h.renderExprs) != 0 {
		t.Errorf("Expected renderExprs to be empty, got %d", len(h.renderExprs))
	}
	if len(h.OutputTypes) != 0 {
		t.Errorf("Expected OutputTypes to be empty, got %d", len(h.OutputTypes))
	}
}

func TestProcessorBaseInit(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	t.Run("basic init", func(t *testing.T) {
		pb := &ProcessorBase{}
		self := &mockProcessor{}
		post := &execinfrapb.PostProcessSpec{}

		err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		if pb.FlowCtx != flowCtx {
			t.Error("FlowCtx not set correctly")
		}
		if pb.EvalCtx == nil {
			t.Error("EvalCtx not set correctly")
		}
		if pb.processorID != 1 {
			t.Errorf("Expected processorID 1, got %d", pb.processorID)
		}
	})
}

func TestProcessorBaseMoveToDraining(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	t.Run("move to draining", func(t *testing.T) {
		pb := &ProcessorBase{}
		self := &mockProcessor{}
		post := &execinfrapb.PostProcessSpec{}

		err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		if pb.State != StateRunning {
			t.Errorf("Expected initial state StateRunning, got %v", pb.State)
		}

		pb.MoveToDraining(nil)

		// When there are no inputs to drain, MoveToDraining moves directly to StateTrailingMeta
		if pb.State != StateTrailingMeta {
			t.Errorf("Expected state StateTrailingMeta, got %v", pb.State)
		}
	})

	t.Run("move to draining with error", func(t *testing.T) {
		pb := &ProcessorBase{}
		self := &mockProcessor{}
		post := &execinfrapb.PostProcessSpec{}

		err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		testErr := context.Canceled
		pb.MoveToDraining(testErr)

		// When there are no inputs to drain, MoveToDraining moves directly to StateTrailingMeta
		if pb.State != StateTrailingMeta {
			t.Errorf("Expected state StateTrailingMeta, got %v", pb.State)
		}
	})
}

func TestProcessorBaseConsumerDone(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	pb.ConsumerDone()

	// ConsumerDone calls MoveToDraining, which moves directly to StateTrailingMeta when there are no inputs to drain
	if pb.State != StateTrailingMeta {
		t.Errorf("Expected state StateTrailingMeta after ConsumerDone, got %v", pb.State)
	}
}

func TestProcessorBaseInternalClose(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	closed := pb.InternalClose()
	if !closed {
		t.Error("Expected InternalClose to return true")
	}

	if !pb.Closed {
		t.Error("Expected Closed to be true")
	}

	// Second close should return false
	closed = pb.InternalClose()
	if closed {
		t.Error("Expected second InternalClose to return false")
	}
}

func TestProcessorBaseAppendTrailingMeta(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	meta := execinfrapb.ProducerMetadata{
		Err: context.Canceled,
	}
	pb.AppendTrailingMeta(meta)

	if len(pb.trailingMeta) != 1 {
		t.Errorf("Expected 1 trailing meta, got %d", len(pb.trailingMeta))
	}
}

func TestProcessorBaseAddInputToDrain(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()
	input := &mockRowSourceForSender{}

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{
		InputsToDrain: []RowSource{input},
	})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if len(pb.inputsToDrain) != 1 {
		t.Errorf("Expected 1 input to drain, got %d", len(pb.inputsToDrain))
	}

	input2 := &mockRowSourceForSender{}
	pb.AddInputToDrain(input2)

	if len(pb.inputsToDrain) != 2 {
		t.Errorf("Expected 2 inputs to drain, got %d", len(pb.inputsToDrain))
	}
}

// mockProcessor is a mock implementation of RowSource for testing
type mockProcessor struct {
	ProcessorBase
}

func (m *mockProcessor) OutputTypes() []types.T {
	return []types.T{*types.Int}
}

func (m *mockProcessor) Run(ctx context.Context) RowStats {
	return RowStats{}
}

func (m *mockProcessor) RunShortCircuit(ctx context.Context, tsReader TSReader) error {
	return nil
}

func (m *mockProcessor) RunTS(ctx context.Context) {}

func (m *mockProcessor) Push(ctx context.Context, res []byte) error {
	return nil
}

func (m *mockProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, nil
}

func (m *mockProcessor) Start(ctx context.Context) context.Context {
	return ctx
}

func (m *mockProcessor) ConsumerClosed() {
}

func (m *mockProcessor) InitProcessorProcedure(*kv.Txn) {
}

func TestProcOutputHelperEmitRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	evalCtx := &tree.EvalContext{
		Context: ctx,
	}
	typs := []types.T{*types.Int}
	output := newRowChannel()

	// Test basic emit
	h := &ProcOutputHelper{}
	post := &execinfrapb.PostProcessSpec{}
	err := h.Init(post, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))}
	status, err := h.EmitRow(ctx, row)
	if err != nil {
		t.Fatalf("EmitRow failed: %v", err)
	}
	if status != NeedMoreRows {
		t.Errorf("Expected NeedMoreRows, got %v", status)
	}

	// Test emit with limit
	h2 := &ProcOutputHelper{}
	post2 := &execinfrapb.PostProcessSpec{
		Limit: 1,
	}
	err = h2.Init(post2, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
	status, err = h2.EmitRow(ctx, row2)
	if err != nil {
		t.Fatalf("EmitRow failed: %v", err)
	}
	if status != DrainRequested {
		t.Errorf("Expected DrainRequested, got %v", status)
	}
}

func TestProcOutputHelperNeededColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}
	typs := []types.T{*types.Int, *types.String, *types.Float}
	output := newRowChannel()

	// Test no projection or rendering
	h := &ProcOutputHelper{}
	post := &execinfrapb.PostProcessSpec{}
	err := h.Init(post, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	cols := h.NeededColumns()
	if cols.Len() != 3 {
		t.Errorf("Expected 3 columns, got %d", cols.Len())
	}

	// Test projection
	h2 := &ProcOutputHelper{}
	post2 := &execinfrapb.PostProcessSpec{
		Projection:    true,
		OutputColumns: []uint32{0, 2},
	}
	err = h2.Init(post2, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	cols2 := h2.NeededColumns()
	if cols2.Len() != 2 {
		t.Errorf("Expected 2 columns, got %d", cols2.Len())
	}
}

func TestProcOutputHelperProcessRowForStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	evalCtx := &tree.EvalContext{
		Context: ctx,
	}
	typs := []types.T{*types.Int}
	output := newRowChannel()

	h := &ProcOutputHelper{}
	post := &execinfrapb.PostProcessSpec{}
	err := h.Init(post, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))}
	outRow, moreRowsOK, err := h.ProcessRowForStream(ctx, row)
	if err != nil {
		t.Fatalf("ProcessRowForStream failed: %v", err)
	}
	if outRow == nil {
		t.Error("Expected output row")
	}
	if !moreRowsOK {
		t.Error("Expected moreRowsOK to be true")
	}
}

func TestProcOutputHelperProcessRowWithOutLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	evalCtx := &tree.EvalContext{
		Context: ctx,
	}
	typs := []types.T{*types.Int}
	output := newRowChannel()

	h := &ProcOutputHelper{}
	post := &execinfrapb.PostProcessSpec{}
	err := h.Init(post, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))}
	outRow, err := h.ProcessRowWithOutLimit(ctx, row)
	if err != nil {
		t.Fatalf("ProcessRowWithOutLimit failed: %v", err)
	}
	if outRow == nil {
		t.Error("Expected output row")
	}
}

func TestProcOutputHelperOutputAndClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{
		Context: context.Background(),
	}
	typs := []types.T{*types.Int}
	output := newRowChannel()

	h := &ProcOutputHelper{}
	post := &execinfrapb.PostProcessSpec{}
	err := h.Init(post, typs, evalCtx, output)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if h.Output() != output {
		t.Error("Output() should return the output RowReceiver")
	}

	// Test Close
	h.Close()
	// No error expected
}

func TestProcessorBaseDrainHelper(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test DrainHelper in StateRunning (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when calling DrainHelper in StateRunning")
		}
	}()
	pb.DrainHelper()
}

func TestProcessorBaseProcessRowHelper(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))}
	outRow := pb.ProcessRowHelper(row)
	if outRow == nil {
		t.Error("Expected output row")
	}
}

func TestProcessorBaseStartInternal(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	ctx := pb.StartInternal(context.Background(), "test-processor")
	if ctx == nil {
		t.Error("Expected non-nil context")
	}
}

func TestProcessorBaseCheckTrailingMetaExist(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if pb.CheckTrailingMetaExist() {
		t.Error("Expected no trailing meta initially")
	}

	// Add trailing meta
	meta := execinfrapb.ProducerMetadata{}
	pb.AppendTrailingMeta(meta)

	if !pb.CheckTrailingMetaExist() {
		t.Error("Expected trailing meta to exist")
	}
}

func TestProcessorBaseInitWithEvalCtx(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.InitWithEvalCtx(self, post, typs, flowCtx, evalCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("InitWithEvalCtx failed: %v", err)
	}

	if pb.FlowCtx != flowCtx {
		t.Error("FlowCtx not set correctly")
	}
	if pb.EvalCtx != evalCtx {
		t.Error("EvalCtx not set correctly")
	}
	if pb.processorID != 1 {
		t.Errorf("Expected processorID 1, got %d", pb.processorID)
	}
}

func TestProcessorBaseRun(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test Run method
	stats := pb.Run(context.Background())
	if stats.NumRows != 0 {
		t.Errorf("Expected 0 rows, got %d", stats.NumRows)
	}
}

func TestProcessorBaseRunPanic(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test Run method with nil output (should panic)
	pb2 := &ProcessorBase{}
	err = pb2.Init(self, post, typs, flowCtx, 1, nil, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when output is nil")
		}
	}()
	pb2.Run(context.Background())
}

func TestProcessorBaseRunShortCircuit(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test RunShortCircuit method
	err = pb.RunShortCircuit(context.Background(), nil)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestProcessorBasePush(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test Push method
	res := []byte("test result")
	err = pb.Push(context.Background(), res)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestProcessorBaseReset(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Add trailing meta and inputs to drain to test reset
	meta := execinfrapb.ProducerMetadata{}
	pb.AppendTrailingMeta(meta)
	input := &mockRowSourceForSender{}
	pb.AddInputToDrain(input)

	// Verify initial state
	if len(pb.trailingMeta) != 1 {
		t.Errorf("Expected 1 trailing meta, got %d", len(pb.trailingMeta))
	}
	if len(pb.inputsToDrain) != 1 {
		t.Errorf("Expected 1 input to drain, got %d", len(pb.inputsToDrain))
	}

	// Reset the processor base
	pb.Reset()

	// Verify reset state
	if len(pb.trailingMeta) != 0 {
		t.Errorf("Expected 0 trailing meta after reset, got %d", len(pb.trailingMeta))
	}
	if len(pb.inputsToDrain) != 0 {
		t.Errorf("Expected 0 inputs to drain after reset, got %d", len(pb.inputsToDrain))
	}
}

func TestProcessorBaseRunTS(t *testing.T) {
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

	typs := []types.T{*types.Int}
	output := newRowChannel()

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, output, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test RunTS method
	pb.RunTS(context.Background())
	// No error expected
}

func TestProcessorBaseRunTSPanic(t *testing.T) {
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

	typs := []types.T{*types.Int}

	pb := &ProcessorBase{}
	self := &mockProcessor{}
	post := &execinfrapb.PostProcessSpec{}

	err := pb.Init(self, post, typs, flowCtx, 1, nil, nil, ProcStateOpts{})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Test RunTS method with nil output (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when output is nil")
		}
	}()
	pb.RunTS(context.Background())
}

func TestNewMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	parent := mon.MakeMonitor("parent", mon.MemoryResource, nil, nil, -1, 1000, /* noteworthy */
		cluster.MakeTestingClusterSettings())
	parent.Start(ctx, nil, mon.BoundAccount{})
	defer parent.Stop(ctx)

	// Test NewMonitor function
	monitor := NewMonitor(ctx, &parent, "test-monitor")
	if monitor == nil {
		t.Error("Expected non-nil monitor")
	}
	defer monitor.Stop(ctx)
}
