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
	"testing"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func TestFlowCtxNewEvalCtx(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	evalCtx := &tree.EvalContext{
		Context:       context.Background(),
		StmtTimestamp: timeutil.Now(),
		TxnTimestamp:  timeutil.Now(),
	}

	flowCtx := &FlowCtx{
		Cfg:     cfg,
		EvalCtx: evalCtx,
	}

	if flowCtx.EvalCtx == nil {
		t.Fatal("NewEvalCtx returned nil")
	}
	newEvalCtx := flowCtx.NewEvalCtx()
	if newEvalCtx == flowCtx.EvalCtx {
		t.Error("NewEvalCtx should return a copy, not the same instance")
	}
}

func TestFlowCtxNewEvalCtxNil(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	// Create a FlowCtx with non-nil EvalCtx
	flowCtx := &FlowCtx{
		Cfg: cfg,
		EvalCtx: &tree.EvalContext{
			Context:       context.Background(),
			StmtTimestamp: timeutil.Now(),
			TxnTimestamp:  timeutil.Now(),
		},
	}

	// Test that NewEvalCtx returns a non-nil copy
	newEvalCtx := flowCtx.NewEvalCtx()
	if newEvalCtx == nil {
		t.Error("NewEvalCtx should return a non-nil copy when EvalCtx is non-nil")
	}
}

func TestFlowCtxTestingKnobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	testingKnobs := TestingKnobs{
		ForceDiskSpill: true,
	}

	cfg := &ServerConfig{
		Stopper:      stopper,
		TestingKnobs: testingKnobs,
	}

	flowCtx := &FlowCtx{
		Cfg: cfg,
	}

	knobs := flowCtx.TestingKnobs()

	if !knobs.ForceDiskSpill {
		t.Error("TestingKnobs should return the same ForceDiskSpill value")
	}
}

func TestFlowCtxTestingKnobsDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg: cfg,
	}

	knobs := flowCtx.TestingKnobs()

	// Just verify that it returns a valid TestingKnobs struct
	_ = knobs
}

func TestFlowCtxStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg: cfg,
	}

	returnedStopper := flowCtx.Stopper()

	if returnedStopper == nil {
		t.Fatal("Stopper returned nil")
	}

	if returnedStopper != cfg.Stopper {
		t.Error("Stopper should return the same instance from Cfg")
	}
}

func TestFlowCtxStopperNil(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg := &ServerConfig{
		Stopper: nil,
	}

	flowCtx := &FlowCtx{
		Cfg: cfg,
	}

	returnedStopper := flowCtx.Stopper()

	if returnedStopper != nil {
		t.Error("Stopper should return nil when Cfg.Stopper is nil")
	}
}

func TestFlowCtxFields(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	evalCtx := &tree.EvalContext{
		Context:       context.Background(),
		StmtTimestamp: timeutil.Now(),
		TxnTimestamp:  timeutil.Now(),
	}

	flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	nodeID := roachpb.NodeID(54321)

	flowCtx := &FlowCtx{
		Cfg:                cfg,
		ID:                 flowID,
		EvalCtx:            evalCtx,
		Txn:                nil,
		NodeID:             nodeID,
		TraceKV:            true,
		Local:              false,
		NoopInputNums:      10,
		TsTableReaders:     []TSReader{},
		TsBatchLookupInput: nil,
		TsHandleMap:        make(map[int32]unsafe.Pointer),
		TsHandleBreak:      false,
	}

	if flowCtx.ID != flowID {
		t.Errorf("Expected ID %v, got %v", flowID, flowCtx.ID)
	}

	if flowCtx.EvalCtx != evalCtx {
		t.Error("EvalCtx mismatch")
	}

	if flowCtx.NodeID != nodeID {
		t.Errorf("Expected NodeID %v, got %v", nodeID, flowCtx.NodeID)
	}

	if !flowCtx.TraceKV {
		t.Error("Expected TraceKV to be true")
	}

	if flowCtx.Local {
		t.Error("Expected Local to be false")
	}

	if flowCtx.NoopInputNums != 10 {
		t.Errorf("Expected NoopInputNums 10, got %v", flowCtx.NoopInputNums)
	}

	if flowCtx.TsTableReaders == nil {
		t.Error("TsTableReaders should not be nil")
	}

	if flowCtx.TsHandleMap == nil {
		t.Error("TsHandleMap should not be nil")
	}

	if flowCtx.TsHandleBreak {
		t.Error("Expected TsHandleBreak to be false")
	}
}

func TestFlowCtxLocalFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg:   cfg,
		Local: true,
		ID:    execinfrapb.FlowID{},
	}

	if !flowCtx.Local {
		t.Error("Expected Local to be true")
	}

	// Local flows should have empty FlowID
	if flowCtx.ID != (execinfrapb.FlowID{}) {
		t.Errorf("Local flows should have empty FlowID, got %v", flowCtx.ID)
	}
}

func TestFlowCtxRemoteFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}

	flowCtx := &FlowCtx{
		Cfg:   cfg,
		Local: false,
		ID:    flowID,
	}

	if flowCtx.Local {
		t.Error("Expected Local to be false")
	}

	if flowCtx.ID != flowID {
		t.Errorf("Expected ID %v, got %v", flowID, flowCtx.ID)
	}
}

func TestFlowCtxTraceKV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg:     cfg,
		TraceKV: true,
	}

	if !flowCtx.TraceKV {
		t.Error("Expected TraceKV to be true")
	}

	flowCtx.TraceKV = false

	if flowCtx.TraceKV {
		t.Error("Expected TraceKV to be false after modification")
	}
}

func TestFlowCtxNoopInputNums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	testCases := []uint32{0, 1, 10, 100, 1000}

	for _, expectedNums := range testCases {
		flowCtx := &FlowCtx{
			Cfg:           cfg,
			NoopInputNums: expectedNums,
		}

		if flowCtx.NoopInputNums != expectedNums {
			t.Errorf("Expected NoopInputNums %v, got %v", expectedNums, flowCtx.NoopInputNums)
		}
	}
}

func TestFlowCtxTsTableReaders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg:            cfg,
		TsTableReaders: []TSReader{},
	}

	if flowCtx.TsTableReaders == nil {
		t.Error("TsTableReaders should not be nil")
	}

	if len(flowCtx.TsTableReaders) != 0 {
		t.Errorf("Expected empty TsTableReaders, got length %v", len(flowCtx.TsTableReaders))
	}

	flowCtx.TsTableReaders = nil

	if flowCtx.TsTableReaders != nil {
		t.Error("TsTableReaders should be nil after assignment")
	}
}

func TestFlowCtxTsHandleMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg:         cfg,
		TsHandleMap: make(map[int32]unsafe.Pointer),
	}

	if flowCtx.TsHandleMap == nil {
		t.Error("TsHandleMap should not be nil")
	}

	if len(flowCtx.TsHandleMap) != 0 {
		t.Errorf("Expected empty TsHandleMap, got length %v", len(flowCtx.TsHandleMap))
	}

	var dummyValue int
	flowCtx.TsHandleMap[1] = unsafe.Pointer(&dummyValue)

	if len(flowCtx.TsHandleMap) != 1 {
		t.Errorf("Expected TsHandleMap length 1, got %v", len(flowCtx.TsHandleMap))
	}

	if _, ok := flowCtx.TsHandleMap[1]; !ok {
		t.Error("Expected key 1 to exist in TsHandleMap")
	}
}

func TestFlowCtxTsHandleBreak(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	flowCtx := &FlowCtx{
		Cfg:           cfg,
		TsHandleBreak: false,
	}

	if flowCtx.TsHandleBreak {
		t.Error("Expected TsHandleBreak to be false")
	}

	flowCtx.TsHandleBreak = true

	if !flowCtx.TsHandleBreak {
		t.Error("Expected TsHandleBreak to be true after modification")
	}
}

func TestFlowCtxNilCfg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test that creating a FlowCtx with nil Cfg doesn't panic
	_ = &FlowCtx{
		Cfg: nil,
	}

	// Note: TestingKnobs() and Stopper() are not called here because they would panic when Cfg is nil
}

func TestFlowCtxMultipleNewEvalCtx(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := &ServerConfig{
		Stopper: stopper,
	}

	evalCtx := &tree.EvalContext{
		Context:       context.Background(),
		StmtTimestamp: timeutil.Now(),
		TxnTimestamp:  timeutil.Now(),
	}

	flowCtx := &FlowCtx{
		Cfg:     cfg,
		EvalCtx: evalCtx,
	}

	if flowCtx.EvalCtx == nil {
		t.Fatal("NewEvalCtx returned nil")
	}
	evalCtx1 := flowCtx.NewEvalCtx()
	evalCtx2 := flowCtx.NewEvalCtx()

	if evalCtx1 == evalCtx2 {
		t.Error("Multiple calls to NewEvalCtx should return different instances")
	}

	if evalCtx1 == flowCtx.EvalCtx {
		t.Error("NewEvalCtx should return a copy, not the original")
	}

	if evalCtx2 == flowCtx.EvalCtx {
		t.Error("NewEvalCtx should return a copy, not the original")
	}
}
