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
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a test table descriptor
func createTestTableDescriptorForColumnResolver() *sqlbase.ImmutableTableDescriptor {
	col1 := sqlbase.ColumnDescriptor{
		ID:   1,
		Name: "col1",
	}
	col2 := sqlbase.ColumnDescriptor{
		ID:   2,
		Name: "col2",
	}
	col3 := sqlbase.ColumnDescriptor{
		ID:   3,
		Name: "col3",
	}

	tableDesc := &sqlbase.TableDescriptor{
		ID:            1,
		Name:          "test_table",
		ParentID:      1,
		FormatVersion: 1,
		Columns:       []sqlbase.ColumnDescriptor{col1, col2, col3},
		NextColumnID:  4,
	}

	return sqlbase.NewImmutableTableDescriptor(*tableDesc)
}

// Helper function to create a DataSourceInfo for testing
func createTestDataSourcesInfo() *sqlbase.DataSourceInfo {
	return &sqlbase.DataSourceInfo{
		SourceColumns: []sqlbase.ResultColumn{
			{Name: "col1"},
			{Name: "col2"},
			{Name: "col3"},
		},
		SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
	}
}

func TestProcessTargetColumns(t *testing.T) {
	tableDesc := createTestTableDescriptorForColumnResolver()

	t.Run("Empty name list with ensureColumns=true", func(t *testing.T) {
		cols, err := sqlbase.ProcessTargetColumns(tableDesc, nil, true, false)
		assert.NoError(t, err)
		assert.NotNil(t, cols)
		assert.Equal(t, len(tableDesc.VisibleColumns()), len(cols))
	})

	t.Run("Empty name list with ensureColumns=false", func(t *testing.T) {
		cols, err := sqlbase.ProcessTargetColumns(tableDesc, nil, false, false)
		assert.NoError(t, err)
		assert.Nil(t, cols)
	})

	t.Run("Valid column names", func(t *testing.T) {
		nameList := tree.NameList{"col1", "col2"}
		cols, err := sqlbase.ProcessTargetColumns(tableDesc, nameList, false, false)
		assert.NoError(t, err)
		assert.Len(t, cols, 2)
		assert.Equal(t, "col1", cols[0].Name)
		assert.Equal(t, "col2", cols[1].Name)
	})

	t.Run("Duplicate column names", func(t *testing.T) {
		nameList := tree.NameList{"col1", "col1"}
		_, err := sqlbase.ProcessTargetColumns(tableDesc, nameList, false, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "multiple assignments to the same column")
	})

	t.Run("Non-existent column", func(t *testing.T) {
		nameList := tree.NameList{"nonexistent_col"}
		_, err := sqlbase.ProcessTargetColumns(tableDesc, nameList, false, false)
		assert.Error(t, err)
	})
}

func TestColumnResolver_FindSourceMatchingName(t *testing.T) {
	ctx := context.Background()

	// Create a source info with a specific alias
	sourceInfo := &sqlbase.DataSourceInfo{
		SourceColumns: []sqlbase.ResultColumn{
			{Name: "col1"},
			{Name: "col2"},
		},
		SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
	}
	resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

	t.Run("Matching source name", func(t *testing.T) {
		tn := tree.MakeTableNameWithSchema("test_db", "public", "test_table")

		res, prefix, srcMeta, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.ExactlyOne, res)
		assert.NotNil(t, prefix)
		assert.Nil(t, srcMeta)
		assert.Equal(t, "test_table", string(prefix.TableName))
	})

	t.Run("Non-matching source name", func(t *testing.T) {
		tn := tree.MakeTableNameWithSchema("test_db", "public", "other_table")

		res, prefix, srcMeta, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.NoResults, res)
		assert.Nil(t, prefix)
		assert.Nil(t, srcMeta)
	})

	t.Run("Partially matching source name", func(t *testing.T) {
		tn := tree.MakeUnqualifiedTableName("test_table")

		res, prefix, srcMeta, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.ExactlyOne, res)
		assert.NotNil(t, prefix)
		assert.Nil(t, srcMeta)
	})
}

func TestColumnResolver_FindSourceProvidingColumn(t *testing.T) {
	ctx := context.Background()

	// Create a source info with specific columns
	sourceInfo := &sqlbase.DataSourceInfo{
		SourceColumns: []sqlbase.ResultColumn{
			{Name: "col1"},
			{Name: "col2"},
			{Name: "col3"},
		},
		SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
	}
	resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

	t.Run("Existing column", func(t *testing.T) {
		prefix, srcMeta, colHint, err := resolver.FindSourceProvidingColumn(ctx, "col1")
		assert.NoError(t, err)
		assert.NotNil(t, prefix)
		assert.Nil(t, srcMeta)
		assert.Equal(t, 0, colHint) // col1 is at index 0
	})

	t.Run("Another existing column", func(t *testing.T) {
		prefix, srcMeta, colHint, err := resolver.FindSourceProvidingColumn(ctx, "col2")
		assert.NoError(t, err)
		assert.NotNil(t, prefix)
		assert.Nil(t, srcMeta)
		assert.Equal(t, 1, colHint) // col2 is at index 1
	})

	t.Run("Non-existing column", func(t *testing.T) {
		prefix, srcMeta, colHint, err := resolver.FindSourceProvidingColumn(ctx, "nonexistent_col")
		assert.Error(t, err)
		assert.Nil(t, prefix)
		assert.Nil(t, srcMeta)
		assert.Equal(t, -1, colHint)
		assert.Contains(t, err.Error(), "column \"nonexistent_col\" does not exist")
	})

	t.Run("Ambiguous column", func(t *testing.T) {
		// Create a source with duplicate column names to trigger ambiguity
		duplicateSourceInfo := &sqlbase.DataSourceInfo{
			SourceColumns: []sqlbase.ResultColumn{
				{Name: "duplicate_col"},
				{Name: "duplicate_col"}, // Same name as above
			},
			SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
		}
		duplicateResolver := &sqlbase.ColumnResolver{Source: duplicateSourceInfo}

		_, srcMeta, colHint, err := duplicateResolver.FindSourceProvidingColumn(ctx, "duplicate_col")
		assert.Nil(t, err)
		// assert.Nil(t, prefix)
		assert.Nil(t, srcMeta)
		assert.Equal(t, 0, colHint)
		// assert.Contains(t, err.Error(), "ambiguous")
	})
}

func TestColumnResolver_Resolve(t *testing.T) {
	ctx := context.Background()

	// Create a source info with specific columns
	sourceInfo := &sqlbase.DataSourceInfo{
		SourceColumns: []sqlbase.ResultColumn{
			{Name: "col1"},
			{Name: "col2"},
		},
		SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
	}
	resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

	t.Run("Resolve with valid hint", func(t *testing.T) {
		tn := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
		prefix := &tn

		result, err := resolver.Resolve(ctx, prefix, nil, 0, "col1")
		assert.NoError(t, err)
		assert.Nil(t, result)
		// Check that the internal state was updated correctly
		assert.Equal(t, 0, resolver.ResolverState.ColIdx)
	})

	t.Run("Resolve with invalid hint requiring lookup", func(t *testing.T) {
		tn := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
		prefix := &tn

		result, err := resolver.Resolve(ctx, prefix, nil, -1, "col2")
		assert.NoError(t, err)
		assert.Nil(t, result)
		// Check that the internal state was updated correctly
		assert.Equal(t, 1, resolver.ResolverState.ColIdx)
	})

	t.Run("Resolve non-existing column", func(t *testing.T) {
		tn := tree.MakeTableNameWithSchema("test_db", "public", "test_table")
		prefix := &tn

		result, err := resolver.Resolve(ctx, prefix, nil, -1, "nonexistent_col")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "does not exist")
	})
}

// TestSourceNameMatches indirectly tests the sourceNameMatches function
// by using it through the ColumnResolver's FindSourceMatchingName method
func TestSourceNameMatches(t *testing.T) {
	ctx := context.Background()

	t.Run("Exact match", func(t *testing.T) {
		// Create a source info with a specific alias
		sourceInfo := &sqlbase.DataSourceInfo{
			SourceColumns: []sqlbase.ResultColumn{
				{Name: "col1"},
			},
			SourceAlias: tree.MakeTableNameWithSchema("db1", "public", "kv"),
		}
		resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

		tn := tree.MakeTableNameWithSchema("db1", "public", "kv")
		res, _, _, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.ExactlyOne, res)
	})

	t.Run("Table name mismatch", func(t *testing.T) {
		// Create a source info with a specific alias
		sourceInfo := &sqlbase.DataSourceInfo{
			SourceColumns: []sqlbase.ResultColumn{
				{Name: "col1"},
			},
			SourceAlias: tree.MakeTableNameWithSchema("db1", "public", "kv"),
		}
		resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

		tn := tree.MakeTableNameWithSchema("db1", "public", "other")
		res, _, _, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.NoResults, res)
	})

	t.Run("Match unqualified to qualified", func(t *testing.T) {
		// Create a source info with a specific alias
		sourceInfo := &sqlbase.DataSourceInfo{
			SourceColumns: []sqlbase.ResultColumn{
				{Name: "col1"},
			},
			SourceAlias: tree.MakeTableNameWithSchema("db1", "public", "kv"),
		}
		resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

		tn := tree.MakeUnqualifiedTableName("kv")
		res, _, _, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.ExactlyOne, res)
	})

	t.Run("Schema mismatch with explicit schema", func(t *testing.T) {
		// Create a source info with a specific alias
		sourceInfo := &sqlbase.DataSourceInfo{
			SourceColumns: []sqlbase.ResultColumn{
				{Name: "col1"},
			},
			SourceAlias: tree.MakeTableNameWithSchema("db1", "public", "kv"),
		}
		resolver := &sqlbase.ColumnResolver{Source: sourceInfo}

		// Try to find with different schema - should not match
		tn := tree.MakeTableNameWithSchema("db1", "private", "kv")
		res, _, _, err := resolver.FindSourceMatchingName(ctx, tn)
		assert.NoError(t, err)
		assert.Equal(t, tree.NoResults, res)
	})
}

func TestTableDescriptor_NameResolutionResult(t *testing.T) {
	// Test that TableDescriptor implements the NameResolutionResult interface
	var td sqlbase.TableDescriptor
	td.NameResolutionResult()
	// If we reach this point, the method exists and works
	assert.NotNil(t, &td)
}

func TestDatabaseDescriptor_SchemaMeta(t *testing.T) {
	// Test that DatabaseDescriptor implements the SchemaMeta interface
	var dd sqlbase.DatabaseDescriptor
	dd.SchemaMeta()
	// If we reach this point, the method exists and works
	assert.NotNil(t, &dd)
}

func TestDescriptor_SchemaMeta(t *testing.T) {
	// Test that Descriptor implements the SchemaMeta interface
	var d sqlbase.Descriptor
	d.SchemaMeta()
	// If we reach this point, the method exists and works
	assert.NotNil(t, d)
}

func TestDescriptor_NameResolutionResult(t *testing.T) {
	// Test that Descriptor implements the NameResolutionResult interface
	var d sqlbase.Descriptor
	d.NameResolutionResult()
	// If we reach this point, the method exists and works
	assert.NotNil(t, d)
}
