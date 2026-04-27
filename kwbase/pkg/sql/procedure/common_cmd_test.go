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

package procedure

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// MockRunParam implements RunParam interface for testing
type MockRunParam struct {
	ctx            context.Context
	evalCtx        *tree.EvalContext
	userDefinedVar map[string]tree.Datum
	txn            *kv.Txn
}

func (m *MockRunParam) GetCtx() context.Context {
	return m.ctx
}

func (m *MockRunParam) EvalContext() *tree.EvalContext {
	return m.evalCtx
}

func (m *MockRunParam) SetUserDefinedVar(name string, v tree.Datum) error {
	if m.userDefinedVar == nil {
		m.userDefinedVar = make(map[string]tree.Datum)
	}
	m.userDefinedVar[name] = v
	return nil
}

func (m *MockRunParam) DeallocatePrepare(name string) error {
	return nil
}

func (m *MockRunParam) GetTxn() *kv.Txn {
	return m.txn
}

func (m *MockRunParam) SetTxn(t *kv.Txn) {
	m.txn = t
}

func (m *MockRunParam) NewTxn() {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := kv.NewDB(
		testutils.MakeAmbientCtx(),
		kv.MakeMockTxnSenderFactory(func(
			ctx context.Context, txn *roachpb.Transaction, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		}), clock)
	m.txn = kv.NewTxn(context.Background(), db, 0 /* gatewayNodeID */)
}

func (m *MockRunParam) Rollback() error {
	return nil
}

func (m *MockRunParam) CommitOrCleanup() error {
	return nil
}

// MockInstruction implements Instruction interface for testing
type MockInstruction struct {
	executeCalled bool
	executeErr    error
}

func (m *MockInstruction) Execute(params RunParam, rCtx *SpExecContext) error {
	m.executeCalled = true
	return m.executeErr
}

func (m *MockInstruction) Close() {
}

func TestBlockIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Test empty block
	blockIns := &BlockIns{}
	rCtx := &SpExecContext{}
	rCtx.Init()

	err := blockIns.Execute(runParam, rCtx)
	require.NoError(t, err)

	// Test block with child instructions
	mockIns := &MockInstruction{}
	blockIns.childs = []Instruction{mockIns}

	err = blockIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, mockIns.executeCalled)
}

func TestBlockIns_Close(t *testing.T) {
	blockIns := &BlockIns{}
	mockIns := &MockInstruction{}
	blockIns.childs = []Instruction{mockIns}

	blockIns.Close()
	// Just ensure no panic
}

func TestArrayIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	arrayIns := &ArrayIns{}
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test with no child instructions
	err := arrayIns.Execute(runParam, rCtx)
	require.NoError(t, err)

	// Test with child instructions
	mockIns1 := &MockInstruction{}
	mockIns2 := &MockInstruction{}
	arrayIns.childs = []Instruction{mockIns1, mockIns2}

	err = arrayIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, mockIns1.executeCalled)
	require.True(t, mockIns2.executeCalled)
}

func TestArrayIns_NextStep(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	arrayIns := &ArrayIns{}
	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test with no child instructions
	err := arrayIns.NextStep(runParam, rCtx)
	require.NoError(t, err)

	// Test with child instructions
	mockIns1 := &MockInstruction{}
	mockIns2 := &MockInstruction{}
	arrayIns.childs = []Instruction{mockIns1, mockIns2}

	err = arrayIns.NextStep(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, mockIns1.executeCalled)
	require.True(t, mockIns2.executeCalled)
}

func TestArrayIns_Close(t *testing.T) {
	arrayIns := &ArrayIns{}
	mockIns := &MockInstruction{}
	arrayIns.childs = []Instruction{mockIns}

	arrayIns.Close()
	// Just ensure no panic
}

func TestSetValueIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Test DeclareValue mode
	intType := types.Int
	setValueIns := &SetValueIns{
		name:       "testVar",
		typ:        intType,
		sourceExpr: tree.NewDInt(42),
		mode:       tree.DeclareValue,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err := setValueIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.Len(t, rCtx.localVariable, 1)
	require.Equal(t, "testVar", rCtx.localVariable[0].Name)

	// Test UDFValue mode
	setValueIns.mode = tree.UDFValue
	err = setValueIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.Equal(t, tree.NewDInt(42), runParam.userDefinedVar["testVar"])

	// Test InternalValue mode
	setValueIns.mode = tree.InternalValue
	setValueIns.index = 0
	err = setValueIns.Execute(runParam, rCtx)
	require.NoError(t, err)
}

func TestSetValueIns_Close(t *testing.T) {
	setValueIns := &SetValueIns{}
	setValueIns.Close()
	// Just ensure no panic
}

func TestLeaveIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	leaveIns := &LeaveIns{
		labelName: "testLabel",
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err := leaveIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, rCtx.NeedReturn())
	require.Equal(t, "testLabel", rCtx.ReturnLabel())
}

func TestLeaveIns_Close(t *testing.T) {
	leaveIns := &LeaveIns{}
	leaveIns.Close()
	// Just ensure no panic
}

func TestCaseWhenIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Create a mock condition that returns true
	condition := tree.MakeDBool(true)
	mockIns := &MockInstruction{}

	caseWhenIns := &CaseWhenIns{
		condition: condition,
		breakFlag: true,
		resultIns: mockIns,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err := caseWhenIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, mockIns.executeCalled)
	require.True(t, caseWhenIns.conditionResult)
}

func TestCaseWhenIns_Close(t *testing.T) {
	mockIns := &MockInstruction{}
	caseWhenIns := &CaseWhenIns{
		resultIns: mockIns,
	}

	caseWhenIns.Close()
	// Just ensure no panic
}

func TestLoopIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Create a mock instruction that will trigger a leave
	// mockIns := &MockInstruction{}
	leaveIns := &LeaveIns{
		labelName: "testLoop",
	}
	arrayIns := &ArrayIns{
		childs: []Instruction{leaveIns},
	}

	loopIns := &LoopIns{
		labelName: "testLoop",
		child:     arrayIns,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err := loopIns.Execute(runParam, rCtx)
	require.NoError(t, err)
}

func TestLoopIns_Close(t *testing.T) {
	mockIns := &MockInstruction{}
	loopIns := &LoopIns{
		child: mockIns,
	}

	loopIns.Close()
	// Just ensure no panic
}

func TestSetHandlerIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	setHandlerIns := &SetHandlerIns{
		HandlerHelper: HandlerHelper{
			Typ:    tree.NOTFOUND,
			Action: &MockInstruction{},
		},
		handlerMode: tree.ModeContinue,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err := setHandlerIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.NotNil(t, rCtx.handler[tree.ModeContinue])
}

func TestSetHandlerIns_Close(t *testing.T) {
	setHandlerIns := &SetHandlerIns{}
	setHandlerIns.Close()
	// Just ensure no panic
}
