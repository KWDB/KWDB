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

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestHandleMutationForTSTable tests the handleMutationForTSTable method
func TestHandleMutationForTSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a TSSchemaChangeWorker
	worker := &TSSchemaChangeWorker{
		tableID:    123,
		mutationID: 456,
	}

	// Test that the struct can be created without panicking
	if worker == nil {
		t.Error("TSSchemaChangeWorker should be created successfully")
	}

	// Test that fields are set correctly
	if worker.tableID != 123 {
		t.Errorf("tableID = %d, expected 123", worker.tableID)
	}

	if worker.mutationID != 456 {
		t.Errorf("mutationID = %d, expected 456", worker.mutationID)
	}

	// Test cases for different mutation types
	testCases := []struct {
		name        string
		typeValue   int32
		expectedErr bool
	}{
		{"add column", alterKwdbAddColumn, true},              // Expected to fail due to missing dependencies
		{"drop column", alterKwdbDropColumn, true},            // Expected to fail due to missing dependencies
		{"add tag", alterKwdbAddTag, true},                    // Expected to fail due to missing dependencies
		{"drop tag", alterKwdbDropTag, true},                  // Expected to fail due to missing dependencies
		{"alter column type", alterKwdbAlterColumnType, true}, // Expected to fail due to missing dependencies
		{"alter tag type", alterKwdbAlterTagType, true},       // Expected to fail due to missing dependencies
		{"create tag index", createTagIndex, true},            // Expected to fail due to missing dependencies
		{"drop tag index", dropTagIndex, true},                // Expected to fail due to missing dependencies
		{"unsupported type", 999, true},                       // Expected to fail with unsupported type error
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock SyncMetaCacheDetails
			d := &jobspb.SyncMetaCacheDetails{
				Type: tc.typeValue,
				AlterTag: sqlbase.ColumnDescriptor{
					ID:   789,
					Name: "test_column",
				},
			}

			// Test handleMutationForTSTable
			ctx := context.Background()
			err := worker.handleMutationForTSTable(ctx, d, nil)

			// We expect an error since we didn't set up proper dependencies
			if err == nil && tc.expectedErr {
				t.Errorf("handleMutationForTSTable should return an error for %s", tc.name)
			}
		})
	}
}

func TestGetDDLOpType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		opType   int32
		expected string
	}{
		{createKwdbTsTable, "create ts table"},
		{alterKwdbAddTag, "add tag"},
		{alterKwdbDropTag, "drop tag"},
		{alterKwdbAlterTagType, "alter tag type"},
		{alterKwdbAddColumn, "add column"},
		{alterKwdbDropColumn, "drop column"},
		{alterKwdbAlterColumnType, "alter column type"},
		{alterKwdbAlterPartitionInterval, "alter partition interval"},
		{alterKwdbAlterRetentions, "alter retentions"},
		{alterCompressInterval, "alter compress interval"},
		{autonomy, "autonomy"},
		{vacuum, "vacuum"},
		{createTagIndex, "create tag index"},
		{dropTagIndex, "drop tag index"},
		{999, ""}, // Test unknown op type
	}

	for _, tc := range testCases {
		result := getDDLOpType(tc.opType)
		if result != tc.expected {
			t.Errorf("getDDLOpType(%d) = %q, expected %q", tc.opType, result, tc.expected)
		}
	}
}

func TestMakeKObjectTableForTs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock SyncMetaCacheDetails
	d := jobspb.SyncMetaCacheDetails{
		SNTable: sqlbase.TableDescriptor{
			ID:       123,
			ParentID: 456,
			Name:     "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "col1",
					TsCol: sqlbase.TSCol{
						StorageType:        1,
						StorageLen:         4,
						ColOffset:          0,
						VariableLengthType: 0,
						ColumnType:         1,
					},
				},
				{
					ID:   2,
					Name: "col2",
					TsCol: sqlbase.TSCol{
						StorageType:        2,
						StorageLen:         8,
						ColOffset:          4,
						VariableLengthType: 0,
						ColumnType:         2,
					},
				},
			},
			Indexes: []sqlbase.IndexDescriptor{
				{
					ID:        101,
					ColumnIDs: []sqlbase.ColumnID{1, 2},
				},
			},
			TsTable: sqlbase.TSTable{
				Lifetime:          86400,
				RowSize:           12,
				BitmapOffset:      8,
				PartitionInterval: 3600,
				TsVersion:         1,
				HashNum:           16,
			},
		},
	}

	// Call makeKObjectTableForTs
	result := makeKObjectTableForTs(d)

	// Verify the result
	if result.TsTable.TsTableId != uint64(d.SNTable.ID) {
		t.Errorf("TsTableId = %d, expected %d", result.TsTable.TsTableId, uint64(d.SNTable.ID))
	}

	if result.TsTable.DatabaseId != uint32(d.SNTable.ParentID) {
		t.Errorf("DatabaseId = %d, expected %d", result.TsTable.DatabaseId, uint32(d.SNTable.ParentID))
	}

	if result.TsTable.TableName != d.SNTable.Name {
		t.Errorf("TableName = %q, expected %q", result.TsTable.TableName, d.SNTable.Name)
	}

	if len(result.KColumn) != len(d.SNTable.Columns) {
		t.Errorf("KColumn length = %d, expected %d", len(result.KColumn), len(d.SNTable.Columns))
	}

	if len(result.IndexInfo) != len(d.SNTable.Indexes) {
		t.Errorf("IndexInfo length = %d, expected %d", len(result.IndexInfo), len(d.SNTable.Indexes))
	}
}

// TestTsSchemaChangeResumer tests the tsSchemaChangeResumer struct
func TestTsSchemaChangeResumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a mock job
	job := &jobs.Job{}

	// Create a tsSchemaChangeResumer
	resumer := &tsSchemaChangeResumer{job: job}

	// Test OnFailOrCancel method
	err := resumer.OnFailOrCancel(context.Background(), nil)
	if err != nil {
		t.Errorf("OnFailOrCancel should return nil, got %v", err)
	}
}
