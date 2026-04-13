// Copyright 2020 The Cockroach Authors.
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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestSetInput(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("set_input", func(t *testing.T) {
		// Create a mock planNode with columns
		source := &valuesNode{
			columns: sqlbase.ResultColumns{
				{Name: "col1", Typ: types.Int},
				{Name: "col2", Typ: types.String},
			},
		}

		// Create mock runParams
		extendedEvalCtx := &extendedEvalContext{
			EvalContext: tree.EvalContext{
				Annotations: &tree.Annotations{},
				SessionData: &sessiondata.SessionData{},
			},
			ExecCfg: &ExecutorConfig{},
		}

		params := runParams{
			ctx:             context.Background(),
			extendedEvalCtx: extendedEvalCtx,
			p:               &planner{},
		}

		// Test makePlanNodeToRowSource
		rowSource, err := makePlanNodeToRowSource(source, params, true)
		err = rowSource.SetInput(context.TODO(), nil)
		if err != nil {
			t.Errorf("makePlanNodeToRowSource() returned error: %v", err)
		}
	})
}

func TestInitProcessorProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("init_processor_procedure", func(t *testing.T) {
		// Create a mock planNode with columns
		source := &valuesNode{
			columns: sqlbase.ResultColumns{
				{Name: "col1", Typ: types.Int},
				{Name: "col2", Typ: types.String},
			},
		}

		// Create mock runParams
		extendedEvalCtx := &extendedEvalContext{
			EvalContext: tree.EvalContext{
				Annotations: &tree.Annotations{},
				SessionData: &sessiondata.SessionData{},
				IsProcedure: true,
			},
			ExecCfg: &ExecutorConfig{},
		}

		params := runParams{
			ctx:             context.Background(),
			extendedEvalCtx: extendedEvalCtx,
			p:               &planner{},
		}

		// Test makePlanNodeToRowSource
		rowSource, err := makePlanNodeToRowSource(source, params, true)
		rowSource.EvalCtx = &extendedEvalCtx.EvalContext
		if err != nil {
			t.Errorf("makePlanNodeToRowSource() returned error: %v", err)
		}
		rowSource.InitProcessorProcedure(nil)
	})
}

// TestMakePlanNodeToRowSource tests the makePlanNodeToRowSource function
func TestMakePlanNodeToRowSource(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		fastPath bool
	}{
		{
			name:     "test make plan node to row source with fast path",
			fastPath: true,
		},
		{
			name:     "test make plan node to row source without fast path",
			fastPath: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock planNode with columns
			source := &valuesNode{
				columns: sqlbase.ResultColumns{
					{Name: "col1", Typ: types.Int},
					{Name: "col2", Typ: types.String},
				},
			}

			// Create mock runParams
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					Annotations: &tree.Annotations{},
					SessionData: &sessiondata.SessionData{},
				},
				ExecCfg: &ExecutorConfig{},
			}

			params := runParams{
				ctx:             context.Background(),
				extendedEvalCtx: extendedEvalCtx,
				p:               &planner{},
			}

			// Test makePlanNodeToRowSource
			rowSource, err := makePlanNodeToRowSource(source, params, tt.fastPath)
			if err != nil {
				t.Errorf("makePlanNodeToRowSource() returned error: %v", err)
			}

			if rowSource == nil {
				t.Error("makePlanNodeToRowSource() returned nil")
			}

			// Verify the row source was created with correct properties
			if rowSource.node != source {
				t.Error("makePlanNodeToRowSource() did not set node correctly")
			}

			if rowSource.fastPath != tt.fastPath {
				t.Errorf("makePlanNodeToRowSource() fastPath = %v, want %v", rowSource.fastPath, tt.fastPath)
			}

			if rowSource.params.ctx != params.ctx {
				t.Error("makePlanNodeToRowSource() did not set params correctly")
			}

			// Verify output types were set correctly
			if len(rowSource.outputTypes) == 0 {
				t.Error("makePlanNodeToRowSource() did not set output types")
			}

			// Verify row buffer was created
			if len(rowSource.row) == 0 {
				t.Error("makePlanNodeToRowSource() did not create row buffer")
			}
		})
	}
}

// TestPlanNodeToRowSourceInterface tests that planNodeToRowSource implements required interfaces
func TestPlanNodeToRowSourceInterface(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
	}{
		{
			name: "test interface implementation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock planNode with columns
			source := &valuesNode{
				columns: sqlbase.ResultColumns{
					{Name: "col1", Typ: types.Int},
					{Name: "col2", Typ: types.String},
				},
			}

			// Create mock runParams
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					Annotations: &tree.Annotations{},
					SessionData: &sessiondata.SessionData{},
				},
				ExecCfg: &ExecutorConfig{},
			}

			params := runParams{
				ctx:             context.Background(),
				extendedEvalCtx: extendedEvalCtx,
				p:               &planner{},
			}

			rowSource, err := makePlanNodeToRowSource(source, params, false)
			if err != nil {
				t.Fatalf("makePlanNodeToRowSource() returned error: %v", err)
			}

			// Verify that planNodeToRowSource implements LocalProcessor interface
			var _ execinfra.LocalProcessor = rowSource

			// Verify the structure was properly initialized
			if rowSource.node == nil {
				t.Error("planNodeToRowSource.node is nil")
			}

			if rowSource.params.ctx == nil {
				t.Error("planNodeToRowSource.params.ctx is nil")
			}

			if len(rowSource.outputTypes) == 0 {
				t.Error("planNodeToRowSource.outputTypes is empty")
			}

			if len(rowSource.row) == 0 {
				t.Error("planNodeToRowSource.row is empty")
			}

			t.Logf("planNodeToRowSource properly implements required interfaces")
		})
	}
}

// TestPlanNodeToRowSourceCallProcedure tests the call procedure functionality
func TestPlanNodeToRowSourceCallProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
	}{
		{
			name: "test call procedure functionality",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock planNode with columns
			source := &valuesNode{
				columns: sqlbase.ResultColumns{
					{Name: "col1", Typ: types.Int},
					{Name: "col2", Typ: types.String},
				},
			}

			// Create mock runParams
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					Annotations: &tree.Annotations{},
					SessionData: &sessiondata.SessionData{},
				},
				ExecCfg: &ExecutorConfig{},
			}

			params := runParams{
				ctx:             context.Background(),
				extendedEvalCtx: extendedEvalCtx,
				p:               &planner{},
			}

			rowSource, err := makePlanNodeToRowSource(source, params, false)
			if err != nil {
				t.Fatalf("makePlanNodeToRowSource() returned error: %v", err)
			}

			// Test IsCallProcedure property
			if rowSource.IsCallProcedure {
				t.Error("IsCallProcedure should be false for valuesNode")
			}

			// Test callProcedure field
			if rowSource.callProcedure != nil {
				t.Error("callProcedure should be nil for valuesNode")
			}

			t.Logf("Call procedure functionality tested successfully")
		})
	}
}

// TestPlanNodeToRowSourceFastPath tests the fast path functionality
func TestPlanNodeToRowSourceFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		fastPath bool
	}{
		{
			name:     "test fast path enabled",
			fastPath: true,
		},
		{
			name:     "test fast path disabled",
			fastPath: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock planNode with columns
			source := &valuesNode{
				columns: sqlbase.ResultColumns{
					{Name: "col1", Typ: types.Int},
					{Name: "col2", Typ: types.String},
				},
			}

			// Create mock runParams
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					Annotations: &tree.Annotations{},
					SessionData: &sessiondata.SessionData{},
				},
				ExecCfg: &ExecutorConfig{},
			}

			params := runParams{
				ctx:             context.Background(),
				extendedEvalCtx: extendedEvalCtx,
				p:               &planner{},
			}

			rowSource, err := makePlanNodeToRowSource(source, params, tt.fastPath)
			if err != nil {
				t.Fatalf("makePlanNodeToRowSource() returned error: %v", err)
			}

			// Verify fast path property
			if rowSource.fastPath != tt.fastPath {
				t.Errorf("fastPath = %v, want %v", rowSource.fastPath, tt.fastPath)
			}

			// Verify started property
			if rowSource.started {
				t.Error("started should be false initially")
			}

			t.Logf("Fast path functionality tested successfully")
		})
	}
}

// TestPlanNodeToRowSourceInternalClose tests the InternalClose method
func TestPlanNodeToRowSourceInternalClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
	}{
		{
			name: "test InternalClose method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock planNode with columns
			source := &valuesNode{
				columns: sqlbase.ResultColumns{
					{Name: "col1", Typ: types.Int},
					{Name: "col2", Typ: types.String},
				},
			}

			// Create mock runParams
			extendedEvalCtx := &extendedEvalContext{
				EvalContext: tree.EvalContext{
					Annotations: &tree.Annotations{},
					SessionData: &sessiondata.SessionData{},
				},
				ExecCfg: &ExecutorConfig{},
			}

			params := runParams{
				ctx:             context.Background(),
				extendedEvalCtx: extendedEvalCtx,
				p:               &planner{},
			}

			rowSource, err := makePlanNodeToRowSource(source, params, false)
			if err != nil {
				t.Fatalf("makePlanNodeToRowSource() returned error: %v", err)
			}

			// Test InternalClose method
			rowSource.InternalClose()

			// Verify started property was set
			if !rowSource.started {
				t.Error("InternalClose() should set started to true")
			}

			t.Logf("InternalClose method tested successfully")
		})
	}
}
