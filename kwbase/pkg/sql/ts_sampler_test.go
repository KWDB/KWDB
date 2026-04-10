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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestInitTsSamplerConfiguration tests the initTsSamplerConfiguration function
func TestInitTsSamplerConfiguration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock PlanningCtx
	planCtx := &PlanningCtx{}

	// Create a mock ImmutableTableDescriptor
	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 123,
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "col1",
				},
				{
					ID:   2,
					Name: "col2",
				},
			},
		},
	}

	// Create mock requestedStat
	reqStats := []requestedStat{
		{
			columns: []sqlbase.ColumnID{1, 2},
		},
	}

	// Test initTsSamplerConfiguration
	cfg, err := initTsSamplerConfiguration(planCtx, desc, reqStats)
	if err != nil {
		t.Errorf("initTsSamplerConfiguration should not return error, got %v", err)
	}

	// Verify the configuration
	if cfg.PlanCtx != planCtx {
		t.Error("PlanCtx not set correctly")
	}

	if cfg.TabDesc != desc {
		t.Error("TabDesc not set correctly")
	}

	if len(cfg.ColCfg.wantedColumns) != 2 {
		t.Errorf("wantedColumns length = %d, expected 2", len(cfg.ColCfg.wantedColumns))
	}

	if cfg.SampleSize != histogramSamples {
		t.Errorf("SampleSize = %d, expected %d", cfg.SampleSize, histogramSamples)
	}

	if len(cfg.ReqStats) != 1 {
		t.Errorf("ReqStats length = %d, expected 1", len(cfg.ReqStats))
	}
}

// TestNewTsSamplerSpec tests the NewTsSamplerSpec function
func TestNewTsSamplerSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test NewTsSamplerSpec
	spec := NewTsSamplerSpec()
	if spec == nil {
		t.Error("NewTsSamplerSpec should return a non-nil pointer")
	}
}

// TestCreateTsScanNode tests the createTsScanNode function
func TestCreateTsScanNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock TsSamplerConfig
	tsSamplerCfg := TsSamplerConfig{
		TabDesc: &sqlbase.ImmutableTableDescriptor{
			TableDescriptor: sqlbase.TableDescriptor{
				ID: 123,
				Columns: []sqlbase.ColumnDescriptor{
					{
						ID:   1,
						Name: "col1",
						TsCol: sqlbase.TSCol{
							ColumnType: sqlbase.ColumnType_TYPE_DATA,
						},
					},
				},
				TsTable: sqlbase.TSTable{
					HashNum: 16,
				},
			},
		},
		StatsCols: []sqlbase.ResultColumn{
			{
				PGAttributeNum: 1,
			},
		},
		ColumnTypes: []uint32{uint32(sqlbase.ColumnType_TYPE_DATA)},
	}

	// Test createTsScanNode
	tsScan := createTsScanNode(tsSamplerCfg)
	if tsScan == nil {
		t.Error("createTsScanNode should return a non-nil pointer")
	}

	if tsScan.hashNum != 16 {
		t.Errorf("hashNum = %d, expected 16", tsScan.hashNum)
	}

	if tsScan.AccessMode != execinfrapb.TSTableReadMode_metaTable {
		t.Errorf("AccessMode = %d, expected %d", tsScan.AccessMode, execinfrapb.TSTableReadMode_metaTable)
	}
}

// TestGetNeededColumns tests the getNeededColumns function
func TestGetNeededColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock TsSamplerConfig
	tsConfig := TsSamplerConfig{
		ColCfg: scanColumnsConfig{
			wantedColumns: []tree.ColumnID{1, 2},
			visibility:    publicColumns,
		},
		TabDesc: &sqlbase.ImmutableTableDescriptor{
			TableDescriptor: sqlbase.TableDescriptor{
				ID: 123,
				Columns: []sqlbase.ColumnDescriptor{
					{
						ID:   1,
						Name: "col1",
					},
					{
						ID:   2,
						Name: "col2",
						TsCol: sqlbase.TSCol{
							ColumnType: sqlbase.ColumnType_TYPE_TAG,
						},
					},
				},
			},
		},
	}

	// Test getNeededColumns
	neededCols, err := getNeededColumns(tsConfig)
	if err != nil {
		t.Errorf("getNeededColumns should not return error, got %v", err)
	}

	if len(neededCols) != 2 {
		t.Errorf("neededCols length = %d, expected 2", len(neededCols))
	}
}

// TestSetupStatsColumns tests the setupStatsColumns function
func TestSetupStatsColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create mock neededCols
	neededCols := []sqlbase.ColumnDescriptor{
		{
			ID:     1,
			Name:   "col1",
			Type:   *types.Int,
			Hidden: false,
			TsCol: sqlbase.TSCol{
				ColumnType: sqlbase.ColumnType_TYPE_DATA,
			},
		},
	}

	// Create mock descriptor
	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 123,
		},
	}

	// Test setupStatsColumns
	statsCols, statsColsTypes, columnTypes := setupStatsColumns(neededCols, desc)

	if len(statsCols) != 1 {
		t.Errorf("statsCols length = %d, expected 1", len(statsCols))
	}

	if len(statsColsTypes) != 1 {
		t.Errorf("statsColsTypes length = %d, expected 1", len(statsColsTypes))
	}

	if len(columnTypes) != 1 {
		t.Errorf("columnTypes length = %d, expected 1", len(columnTypes))
	}
}

// TestAppendExtraColumns tests the appendExtraColumns function
func TestAppendExtraColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create mock resCols
	resCols := []sqlbase.ResultColumn{
		{
			Name:           "col1",
			Typ:            types.Int,
			PGAttributeNum: 1,
		},
	}

	// Create mock descriptor
	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 123,
		},
	}

	// Test appendExtraColumns
	resultColumns := appendExtraColumns(resCols, desc)

	// Expect original column + 7 extra columns
	if len(resultColumns) != 8 {
		t.Errorf("resultColumns length = %d, expected 8", len(resultColumns))
	}
}

// TestSetupColumnConfiguration tests the setupColumnConfiguration function
func TestSetupColumnConfiguration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock TsSamplerConfig
	tsSamplerCfg := &TsSamplerConfig{
		ColCfg: scanColumnsConfig{
			wantedColumns: []tree.ColumnID{1, 2},
			visibility:    publicColumns,
		},
		TabDesc: &sqlbase.ImmutableTableDescriptor{
			TableDescriptor: sqlbase.TableDescriptor{
				ID: 123,
				Columns: []sqlbase.ColumnDescriptor{
					{
						ID:   1,
						Name: "col1",
					},
					{
						ID:   2,
						Name: "col2",
					},
				},
			},
		},
	}

	// Test setupColumnConfiguration
	colIdxMap, valNeededForCol, err := setupColumnConfiguration(tsSamplerCfg)
	if err != nil {
		t.Errorf("setupColumnConfiguration should not return error, got %v", err)
	}

	if len(colIdxMap) != 2 {
		t.Errorf("colIdxMap length = %d, expected 2", len(colIdxMap))
	}

	if valNeededForCol.Len() != 2 {
		t.Errorf("valNeededForCol length = %d, expected 2", valNeededForCol.Len())
	}
}
