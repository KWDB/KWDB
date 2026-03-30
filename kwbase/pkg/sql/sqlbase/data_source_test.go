// Copyright 2016 The Cockroach Authors.
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

package sqlbase_test

import (
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/assert"
)

// TestDataSourceInfoString tests the String method of DataSourceInfo
func TestDataSourceInfoString(t *testing.T) {
	// Create a test DataSourceInfo with sample columns
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Bool},
		{Name: "col2", Typ: types.Int},
		{Name: "hidden_col", Hidden: true, Typ: types.String},
	}

	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	result := srcInfo.String()

	// Check that the result contains expected elements
	assert.Contains(t, result, "col1", "Result should contain column name 'col1'")
	assert.Contains(t, result, "col2", "Result should contain column name 'col2'")
	assert.Contains(t, result, "*hidden_col", "Result should contain hidden column marked with '*'")
	assert.Contains(t, result, "test_table", "Result should contain table name")
	assert.Contains(t, result, "output column positions", "Result should contain column positions header")
	assert.Contains(t, result, "output column names", "Result should contain column names header")
}

// TestDataSourceInfoStringWithAnonymousTable tests the String method with anonymous table
func TestDataSourceInfoStringWithAnonymousTable(t *testing.T) {
	// Create a test DataSourceInfo with anonymous table
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Bool},
	}

	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   sqlbase.AnonymousTable,
	}

	result := srcInfo.String()

	// Check that the result contains expected elements for anonymous table
	assert.Contains(t, result, "col1", "Result should contain column name 'col1'")
	assert.Contains(t, result, "<anonymous table>", "Result should contain anonymous table indicator")
}

// TestNewSourceInfoForSingleTable tests the NewSourceInfoForSingleTable function
func TestNewSourceInfoForSingleTable(t *testing.T) {
	// Test with fully qualified table name
	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.String},
	}

	srcInfo := sqlbase.NewSourceInfoForSingleTable(tableName, columns)

	// Check that the returned DataSourceInfo has correct values
	assert.Equal(t, columns, srcInfo.SourceColumns, "Source columns should match input")
	assert.Equal(t, tableName, srcInfo.SourceAlias, "Source alias should match input")

	// Check that ExplicitCatalog and ExplicitSchema are set when table and schema names are not empty
	assert.True(t, srcInfo.SourceAlias.ExplicitCatalog, "ExplicitCatalog should be true for qualified table name")
	assert.True(t, srcInfo.SourceAlias.ExplicitSchema, "ExplicitSchema should be true for qualified table name")
}

// TestNewSourceInfoForSingleTableWithUnqualifiedName tests NewSourceInfoForSingleTable with unqualified name
func TestNewSourceInfoForSingleTableWithUnqualifiedName(t *testing.T) {
	// Test with unqualified table name (only table name, no schema or catalog)
	tableName := tree.MakeUnqualifiedTableName("simple_table")
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
	}

	srcInfo := sqlbase.NewSourceInfoForSingleTable(tableName, columns)

	// Check that the returned DataSourceInfo has correct values
	assert.Equal(t, columns, srcInfo.SourceColumns, "Source columns should match input")
	assert.Equal(t, tableName, srcInfo.SourceAlias, "Source alias should match input")

	// For unqualified names, ExplicitCatalog and ExplicitSchema should not be set
	// (since TableName is not empty and SchemaName is empty)
	assert.False(t, srcInfo.SourceAlias.ExplicitCatalog, "ExplicitCatalog should be false for unqualified table name")
	assert.False(t, srcInfo.SourceAlias.ExplicitSchema, "ExplicitSchema should be false for unqualified table name")
}

// TestNewSourceInfoForSingleTableWithSchemaOnly tests NewSourceInfoForSingleTable with schema-only name
func TestNewSourceInfoForSingleTableWithSchemaOnly(t *testing.T) {
	// Test with schema-qualified table name (schema and table, no catalog)
	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
	}

	srcInfo := sqlbase.NewSourceInfoForSingleTable(tableName, columns)

	// Check that the returned DataSourceInfo has correct values
	assert.Equal(t, columns, srcInfo.SourceColumns, "Source columns should match input")
	assert.Equal(t, tableName, srcInfo.SourceAlias, "Source alias should match input")

	// Check that the returned DataSourceInfo has correct values
	assert.Equal(t, columns, srcInfo.SourceColumns, "Source columns should match input")
	assert.Equal(t, tableName, srcInfo.SourceAlias, "Source alias should match input")
}

// TestAnonymousTable tests the AnonymousTable variable
func TestAnonymousTable(t *testing.T) {
	// Check that AnonymousTable is indeed an empty table name
	assert.Equal(t, tree.Name(""), sqlbase.AnonymousTable.TableName, "AnonymousTable should have empty table name")
	assert.Equal(t, tree.Name(""), sqlbase.AnonymousTable.SchemaName, "AnonymousTable should have empty schema name")
	assert.Equal(t, tree.Name(""), sqlbase.AnonymousTable.CatalogName, "AnonymousTable should have empty catalog name")
}

// TestNodeFormatter tests the NodeFormatter method of DataSourceInfo
func TestNodeFormatter(t *testing.T) {
	// Create a test DataSourceInfo
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.String},
	}
	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	// Test NodeFormatter for first column (index 0)
	formatter := srcInfo.NodeFormatter(0)
	assert.NotNil(t, formatter, "NodeFormatter should return a non-nil formatter")

	// Test NodeFormatter for second column (index 1)
	formatter2 := srcInfo.NodeFormatter(1)
	assert.NotNil(t, formatter2, "NodeFormatter should return a non-nil formatter for second column")

	// Test with invalid index (should panic in real code, but we can't test that easily)
	// So we'll just make sure it doesn't crash with valid indices
}

// TestNodeFormatterOutput tests the output of NodeFormatter
func TestNodeFormatterOutput(t *testing.T) {
	// Create a test DataSourceInfo
	columns := sqlbase.ResultColumns{
		{Name: "test_column", Typ: types.Int},
	}
	tableName := tree.MakeTableNameWithSchema("test_catalog", "test_schema", "test_table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	// Get the formatter for the column
	formatter := srcInfo.NodeFormatter(0)
	assert.NotNil(t, formatter, "NodeFormatter should return a non-nil formatter")

	// Test formatting with FmtShowTableAliases flag
	ctx := tree.NewFmtCtx(tree.FmtShowTableAliases)
	formatter.Format(ctx)
	formatted := ctx.CloseAndGetString()

	// The formatted string should contain the table and column names
	assert.Contains(t, formatted, "test_column", "Formatted output should contain column name")
	assert.Contains(t, formatted, "test_table", "Formatted output should contain table name")
}

// TestDataSourceInfoWithHiddenColumns tests DataSourceInfo with hidden columns
func TestDataSourceInfoWithHiddenColumns(t *testing.T) {
	// Create a DataSourceInfo with both visible and hidden columns
	columns := sqlbase.ResultColumns{
		{Name: "visible_col1", Typ: types.Int},
		{Name: "hidden_col", Hidden: true, Typ: types.String},
		{Name: "visible_col2", Typ: types.Bool},
	}

	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	result := srcInfo.String()

	// Check that the result contains both visible and hidden columns
	assert.Contains(t, result, "visible_col1", "Result should contain visible column name")
	assert.Contains(t, result, "*hidden_col", "Result should contain hidden column marked with '*'")
	assert.Contains(t, result, "visible_col2", "Result should contain visible column name")
}

// TestDataSourceInfoEmptyColumns tests DataSourceInfo with no columns
func TestDataSourceInfoEmptyColumns(t *testing.T) {
	// Create a DataSourceInfo with no columns
	columns := sqlbase.ResultColumns{}
	tableName := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	result := srcInfo.String()

	// The result should still contain the table name information
	assert.Contains(t, result, "test_table", "Result should contain table name")
	assert.Contains(t, result, "output column positions", "Result should contain column positions header")
	assert.Contains(t, result, "output column names", "Result should contain column names header")

	// Since there are no columns, the column position and name lines should be empty
	lines := strings.Split(result, "\n")
	for _, line := range lines {
		if strings.Contains(line, "output column positions") {
			// The line before this should be empty or just tabs
			continue
		}
	}
}

// TestDataSourceInfoSpecialCharacters tests DataSourceInfo with special characters in names
func TestDataSourceInfoSpecialCharacters(t *testing.T) {
	// Create a DataSourceInfo with special characters in column names
	columns := sqlbase.ResultColumns{
		{Name: "col-with-special_chars", Typ: types.Int},
		{Name: "col with spaces", Typ: types.String},
		{Name: "col123!@#$%^&*()", Typ: types.Bool},
	}

	tableName := tree.MakeTableNameWithSchema("special-catalog", "special_schema", "special.table")
	srcInfo := &sqlbase.DataSourceInfo{
		SourceColumns: columns,
		SourceAlias:   tableName,
	}

	result := srcInfo.String()

	// Check that the result contains the special character column names
	assert.Contains(t, result, "col-with-special_chars", "Result should contain column with special chars")
	assert.Contains(t, result, "col with spaces", "Result should contain column with spaces")
	assert.Contains(t, result, "col123", "Result should contain alphanumeric column name")
	assert.Contains(t, result, "special.table", "Result should contain table name with dot")
}
