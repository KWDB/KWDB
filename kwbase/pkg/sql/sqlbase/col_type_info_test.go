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

package sqlbase_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestColTypeInfoFromResCols(t *testing.T) {
	// Create sample result columns
	resultCols := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.VarChar},
		{Name: "col3", Typ: types.Timestamp},
	}

	// Create ColTypeInfo from ResultColumns
	colTypeInfo := sqlbase.ColTypeInfoFromResCols(resultCols)

	// Verify the number of columns
	assert.Equal(t, len(resultCols), colTypeInfo.NumColumns())

	// Verify individual types
	for i := 0; i < len(resultCols); i++ {
		assert.Equal(t, resultCols[i].Typ, colTypeInfo.Type(i))
	}
}

func TestColTypeInfoFromColTypes(t *testing.T) {
	// Create sample column types
	colTypes := []types.T{
		*types.Int,
		*types.VarChar,
		*types.Timestamp,
	}

	// Create ColTypeInfo from column types
	colTypeInfo := sqlbase.ColTypeInfoFromColTypes(colTypes)

	// Verify the number of columns
	assert.Equal(t, len(colTypes), colTypeInfo.NumColumns())

	// Verify individual types
	for i := 0; i < len(colTypes); i++ {
		assert.Equal(t, &colTypes[i], colTypeInfo.Type(i))
	}
}

func TestColTypeInfoFromColDescs(t *testing.T) {
	// Create sample column descriptors
	colDescs := []sqlbase.ColumnDescriptor{
		{Name: "col1", Type: *types.Int},
		{Name: "col2", Type: *types.VarChar},
		{Name: "col3", Type: *types.Timestamp},
	}

	// Create ColTypeInfo from column descriptors
	colTypeInfo := sqlbase.ColTypeInfoFromColDescs(colDescs)

	// Verify the number of columns
	assert.Equal(t, len(colDescs), colTypeInfo.NumColumns())

	// Verify individual types
	for i := 0; i < len(colDescs); i++ {
		assert.Equal(t, &colDescs[i].Type, colTypeInfo.Type(i))
	}
}

func TestColTypeInfo_NumColumns(t *testing.T) {
	// Test with ResultColumns
	resultCols := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.VarChar},
	}
	colTypeInfoFromResCols := sqlbase.ColTypeInfoFromResCols(resultCols)
	assert.Equal(t, 2, colTypeInfoFromResCols.NumColumns())

	// Test with column types
	colTypes := []types.T{
		*types.Int,
		*types.VarChar,
		*types.Timestamp,
	}
	colTypeInfoFromColTypes := sqlbase.ColTypeInfoFromColTypes(colTypes)
	assert.Equal(t, 3, colTypeInfoFromColTypes.NumColumns())
}

func TestColTypeInfo_Type(t *testing.T) {
	// Test with ResultColumns
	resultCols := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.VarChar},
	}
	colTypeInfoFromResCols := sqlbase.ColTypeInfoFromResCols(resultCols)
	assert.Equal(t, types.Int, colTypeInfoFromResCols.Type(0))
	assert.Equal(t, types.VarChar, colTypeInfoFromResCols.Type(1))

	// Test with column types
	colTypes := []types.T{
		*types.Int,
		*types.VarChar,
		*types.Timestamp,
	}
	colTypeInfoFromColTypes := sqlbase.ColTypeInfoFromColTypes(colTypes)
	assert.Equal(t, &colTypes[0], colTypeInfoFromColTypes.Type(0))
	assert.Equal(t, &colTypes[1], colTypeInfoFromColTypes.Type(1))
	assert.Equal(t, &colTypes[2], colTypeInfoFromColTypes.Type(2))
}

func TestMakeColTypeInfo(t *testing.T) {
	// Create a sample table descriptor
	tableDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			Columns: []sqlbase.ColumnDescriptor{
				{Name: "col1", Type: *types.Int, ID: 1},
				{Name: "col2", Type: *types.VarChar, ID: 2},
				{Name: "col3", Type: *types.Timestamp, ID: 3},
			},
		},
	}

	// Create a column ID to row index map
	colIDToRowIndex := map[sqlbase.ColumnID]int{
		1: 0,
		2: 1,
		3: 2,
	}

	// Create ColTypeInfo using MakeColTypeInfo
	colTypeInfo, err := sqlbase.MakeColTypeInfo(tableDesc, colIDToRowIndex)
	assert.NoError(t, err)
	assert.Equal(t, 3, colTypeInfo.NumColumns())

	// Verify individual types
	assert.Equal(t, &tableDesc.Columns[0].Type, colTypeInfo.Type(0))
	assert.Equal(t, &tableDesc.Columns[1].Type, colTypeInfo.Type(1))
	assert.Equal(t, &tableDesc.Columns[2].Type, colTypeInfo.Type(2))

	// Test with invalid column ID
	invalidColIDToRowIndex := map[sqlbase.ColumnID]int{
		999: 0, // Non-existent column ID
	}
	_, err = sqlbase.MakeColTypeInfo(tableDesc, invalidColIDToRowIndex)
	assert.Error(t, err)
}

// Helper function to create a table descriptor for testing
func createTestTableDescriptor() *sqlbase.ImmutableTableDescriptor {
	return &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			Columns: []sqlbase.ColumnDescriptor{
				{Name: "id", Type: *types.Int, ID: 1},
				{Name: "name", Type: *types.VarChar, ID: 2},
				{Name: "created_at", Type: *types.Timestamp, ID: 3},
			},
		},
	}
}

// Additional test to verify the behavior of ColTypeInfo with different configurations
func TestColTypeInfo_EdgeCases(t *testing.T) {
	// Test with empty ResultColumns
	emptyResultCols := sqlbase.ResultColumns{}
	colTypeInfoEmptyResCols := sqlbase.ColTypeInfoFromResCols(emptyResultCols)
	assert.Equal(t, 0, colTypeInfoEmptyResCols.NumColumns())

	// Test with empty column types
	emptyColTypes := []types.T{}
	colTypeInfoEmptyColTypes := sqlbase.ColTypeInfoFromColTypes(emptyColTypes)
	assert.Equal(t, 0, colTypeInfoEmptyColTypes.NumColumns())

	// Test with empty column descriptors
	emptyColDescs := []sqlbase.ColumnDescriptor{}
	colTypeInfoEmptyColDescs := sqlbase.ColTypeInfoFromColDescs(emptyColDescs)
	assert.Equal(t, 0, colTypeInfoEmptyColDescs.NumColumns())
}
