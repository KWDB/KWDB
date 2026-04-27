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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestTriggerHelperGetValue tests the GetValue method
func TestTriggerHelperGetValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	datums := tree.Datums{tree.NewDInt(1), tree.NewDString("test")}

	// Create triggerHelper
	h := &triggerHelper{
		internalValues: &datums,
	}

	// Test GetValue
	val := h.GetValue(0)
	if val != datums[0] {
		t.Errorf("GetValue(0) = %v, expected %v", val, datums[0])
	}

	val = h.GetValue(1)
	if val != datums[1] {
		t.Errorf("GetValue(1) = %v, expected %v", val, datums[1])
	}
}

// TestTriggerHelperSetValue tests the SetValue method
func TestTriggerHelperSetValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	datums := tree.Datums{tree.NewDInt(1), tree.NewDString("test")}
	placeholders := &tree.PlaceholderInfo{
		Values: make(tree.QueryArguments, 2),
	}

	// Create triggerHelper
	h := &triggerHelper{
		internalValues: &datums,
		placeholders:   placeholders,
	}

	// Test SetValue
	newVal := tree.NewDInt(2)
	h.SetValue(0, newVal)

	if (*h.internalValues)[0] != newVal {
		t.Errorf("internalValues[0] = %v, expected %v", (*h.internalValues)[0], newVal)
	}

	if h.placeholders.Values[0] != newVal {
		t.Errorf("placeholders.Values[0] = %v, expected %v", h.placeholders.Values[0], newVal)
	}
}

// TestTriggerHelperGetMaxIndex tests the GetMaxIndex method
func TestTriggerHelperGetMaxIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	datums := tree.Datums{tree.NewDInt(1), tree.NewDString("test"), tree.MakeDBool(true)}

	// Create triggerHelper
	h := &triggerHelper{
		internalValues: &datums,
	}

	// Test GetMaxIndex
	maxIndex := h.GetMaxIndex()
	if maxIndex != len(datums) {
		t.Errorf("GetMaxIndex() = %d, expected %d", maxIndex, len(datums))
	}
}

// TestTriggerHelperGetInternalValues tests the GetInternalValues method
func TestTriggerHelperGetInternalValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	datums := tree.Datums{tree.NewDInt(1), tree.NewDString("test")}

	// Create triggerHelper
	h := &triggerHelper{
		internalValues: &datums,
	}

	// Test GetInternalValues
	values := h.GetInternalValues()
	if len(values) != len(datums) {
		t.Errorf("GetInternalValues() length = %d, expected %d", len(values), len(datums))
	}

	for i, val := range values {
		if val != datums[i] {
			t.Errorf("GetInternalValues()[%d] = %v, expected %v", i, val, datums[i])
		}
	}
}

// TestTriggerHelperInitContext tests the InitContext method
func TestTriggerHelperInitContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	execCtx := procedure.SpExecContext{}

	// Create triggerHelper
	Plan := 0
	h := &triggerHelper{
		execCtx: execCtx,
		fn: func(expr memo.RelExpr, pr *physical.Required, src []*exec.LocalVariable) (exec.Plan, error) {
			return Plan, nil
		},
	}

	// Test InitContext
	h.InitContext()

	if h.execCtx.Fn == nil {
		t.Error("execCtx.Fn should not be nil")
	}

	if h.execCtx.GetResultFn == nil {
		t.Error("execCtx.GetResultFn should not be nil")
	}

	if h.execCtx.RunPlanFn == nil {
		t.Error("execCtx.RunPlanFn should not be nil")
	}

	if h.execCtx.StartPlanFn == nil {
		t.Error("execCtx.StartPlanFn should not be nil")
	}

	if h.execCtx.TriggerReplaceValues != h {
		t.Error("execCtx.TriggerReplaceValues not set correctly")
	}
}

// TestTriggerHelperNeedExecuteTrigger tests the NeedExecuteTrigger method
func TestTriggerHelperNeedExecuteTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test case 1: no triggers
	h1 := &triggerHelper{}
	if h1.NeedExecuteTrigger() {
		t.Error("NeedExecuteTrigger should return false when no triggers are set")
	}

	// Test case 2: with before trigger
	h2 := &triggerHelper{
		beforeIns: &procedure.BlockIns{},
	}
	if !h2.NeedExecuteTrigger() {
		t.Error("NeedExecuteTrigger should return true when before trigger is set")
	}

	// Test case 3: with after trigger
	h3 := &triggerHelper{
		afterIns: &procedure.BlockIns{},
	}
	if !h3.NeedExecuteTrigger() {
		t.Error("NeedExecuteTrigger should return true when after trigger is set")
	}

	// Test case 4: with both triggers
	h4 := &triggerHelper{
		beforeIns: &procedure.BlockIns{},
		afterIns:  &procedure.BlockIns{},
	}
	if !h4.NeedExecuteTrigger() {
		t.Error("NeedExecuteTrigger should return true when both triggers are set")
	}
}

// TestTriggerHelperNeedExecuteBeforeTrigger tests the NeedExecuteBeforeTrigger method
func TestTriggerHelperNeedExecuteBeforeTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test case 1: no before trigger
	h1 := &triggerHelper{}
	if h1.NeedExecuteBeforeTrigger() {
		t.Error("NeedExecuteBeforeTrigger should return false when no before trigger is set")
	}

	// Test case 2: with before trigger
	h2 := &triggerHelper{
		beforeIns: &procedure.BlockIns{},
	}
	if !h2.NeedExecuteBeforeTrigger() {
		t.Error("NeedExecuteBeforeTrigger should return true when before trigger is set")
	}
}

// TestTriggerHelperNeedExecuteAfterTrigger tests the NeedExecuteAfterTrigger method
func TestTriggerHelperNeedExecuteAfterTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test case 1: no after trigger
	h1 := &triggerHelper{}
	if h1.NeedExecuteAfterTrigger() {
		t.Error("NeedExecuteAfterTrigger should return false when no after trigger is set")
	}

	// Test case 2: with after trigger
	h2 := &triggerHelper{
		afterIns: &procedure.BlockIns{},
	}
	if !h2.NeedExecuteAfterTrigger() {
		t.Error("NeedExecuteAfterTrigger should return true when after trigger is set")
	}
}

// TestTriggerHelperExecuteIns tests the ExecuteIns method
func TestTriggerHelperExecuteIns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test data
	datums := tree.Datums{tree.NewDInt(1), tree.NewDString("test")}
	execCtx := procedure.SpExecContext{}

	// Create triggerHelper
	h := &triggerHelper{
		execCtx: execCtx,
		fn:      nil,
	}

	p := makeTestPlanner()
	ctx := context.TODO()
	params := runParams{ctx: ctx, p: p, extendedEvalCtx: &p.extendedEvalCtx}

	// Test case 1: ins is nil
	err := h.ExecuteIns(params, &datums, nil)
	if err != nil {
		t.Errorf("ExecuteIns should return nil when ins is nil, got %v", err)
	}

	// Test case 2: ins.Execute succeeds
	successIns := &procedure.BlockIns{}
	err = h.ExecuteIns(params, &datums, successIns)
	if err != nil {
		t.Errorf("ExecuteIns should return nil when ins.Execute succeeds, got %v", err)
	}
}
