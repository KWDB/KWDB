// Copyright 2019 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestRepeatableRowSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test rows
	row1 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
	row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2))}
	row3 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(3))}
	rows := []sqlbase.EncDatumRow{row1, row2, row3}

	// Create RepeatableRowSource
	types := []types.T{*types.Int}
	rs := NewRepeatableRowSource(types, rows)

	// Test OutputTypes
	outputTypes := rs.OutputTypes()
	if len(outputTypes) != 1 {
		t.Errorf("Expected 1 output type, got %d", len(outputTypes))
	}

	// Test Start
	ctx := context.Background()
	newCtx := rs.Start(ctx)
	if newCtx != ctx {
		t.Errorf("Expected Start to return the same context, got different context")
	}

	// Test Next
	row, meta := rs.Next()
	if row == nil || meta != nil {
		t.Errorf("Expected first row, got row=%v, meta=%v", row, meta)
	}

	row, meta = rs.Next()
	if row == nil || meta != nil {
		t.Errorf("Expected second row, got row=%v, meta=%v", row, meta)
	}

	row, meta = rs.Next()
	if row == nil || meta != nil {
		t.Errorf("Expected third row, got row=%v, meta=%v", row, meta)
	}

	// Test end of rows
	row, meta = rs.Next()
	if row != nil || meta != nil {
		t.Errorf("Expected nil row and nil meta at end, got row=%v, meta=%v", row, meta)
	}

	// Test Reset
	rs.Reset()

	// Test Next again after reset
	row, meta = rs.Next()
	if row == nil || meta != nil {
		t.Errorf("Expected first row after reset, got row=%v, meta=%v", row, meta)
	}

	// Test ConsumerDone and ConsumerClosed (they should do nothing)
	rs.ConsumerDone()
	rs.ConsumerClosed()

	// Test InitProcessorProcedure (it should do nothing)
	rs.InitProcessorProcedure(nil)
}

func TestNewTestMemMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	monitor := NewTestMemMonitor(ctx, st)
	if monitor == nil {
		t.Errorf("Expected non-nil memory monitor")
	}

	// Just verify the monitor is created and started
	_ = monitor
}

func TestNewTestDiskMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	monitor := NewTestDiskMonitor(ctx, st)
	if monitor == nil {
		t.Errorf("Expected non-nil disk monitor")
	}

	// Just verify the monitor is created and started
	_ = monitor
}

func TestGenerateValuesSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test rows
	row1 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))}
	row2 := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(2))}
	rows := []sqlbase.EncDatumRow{row1, row2}

	// Generate ValuesSpec
	colTypes := []types.T{*types.Int}
	spec, err := GenerateValuesSpec(colTypes, rows, 1)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(spec.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(spec.Columns))
	}

	if len(spec.RawBytes) != 2 {
		t.Errorf("Expected 2 chunks, got %d", len(spec.RawBytes))
	}

	// Test with zero rows
	spec, err = GenerateValuesSpec(colTypes, nil, 1)
	if err != nil {
		t.Errorf("Expected no error with zero rows, got %v", err)
	}

	if len(spec.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(spec.Columns))
	}

	if len(spec.RawBytes) != 0 {
		t.Errorf("Expected 0 chunks with zero rows, got %d", len(spec.RawBytes))
	}
}

func TestRowDisposer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rd := &RowDisposer{}

	// Test Push
	row := sqlbase.EncDatumRow{}
	status := rd.Push(row, nil)
	if status != NeedMoreRows {
		t.Errorf("Expected NeedMoreRows, got %v", status)
	}

	// Test AddStats
	rd.AddStats(0, true)

	// Test GetStats
	stats := rd.GetStats()
	_ = stats // Just verify it doesn't panic

	// Test PushPGResult
	err := rd.PushPGResult(context.Background(), nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test AddPGComplete
	rd.AddPGComplete("", 0, 0)

	// Test ProducerDone
	rd.ProducerDone()

	// Test Types
	types := rd.Types()
	if types != nil {
		t.Errorf("Expected nil types, got %v", types)
	}
}
