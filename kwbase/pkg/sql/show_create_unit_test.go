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
	"bytes"
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestFormatQuoteNames tests the formatQuoteNames function
func TestFormatQuoteName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		names    []string
		expected string
	}{
		{
			name:     "single name",
			names:    []string{"col1"},
			expected: `col1`,
		},
		{
			name:     "multiple names",
			names:    []string{"col1", "col2", "col3"},
			expected: `col1, col2, col3`,
		},
		{
			name:     "empty names",
			names:    []string{},
			expected: "",
		},
		{
			name:     "name with special chars",
			names:    []string{"col name"},
			expected: `"col name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			formatQuoteNames(&buf, tt.names...)
			require.Equal(t, tt.expected, buf.String())
		})
	}
}

// TestShowCreateView tests the ShowCreateView function
func TestShowCreateView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name        string
		tableName   string
		desc        *sqlbase.TableDescriptor
		expected    string
		expectedErr bool
	}{
		{
			name:      "simple view",
			tableName: "test_view",
			desc: &sqlbase.TableDescriptor{
				Name: "test_view",
				Columns: []sqlbase.ColumnDescriptor{
					{Name: "id"},
					{Name: "name"},
				},
				ViewQuery: "SELECT id, name FROM test_table",
			},
			expected: `CREATE VIEW test_view (id, name) AS SELECT id, name FROM test_table`,
		},
		{
			name:      "temporary view",
			tableName: "temp_view",
			desc: &sqlbase.TableDescriptor{
				Name:      "temp_view",
				Temporary: true,
				Columns: []sqlbase.ColumnDescriptor{
					{Name: "col1"},
				},
				ViewQuery: "SELECT col1 FROM test_table",
			},
			expected: `CREATE TEMP VIEW temp_view (col1) AS SELECT col1 FROM test_table`,
		},
		{
			name:      "materialized view",
			tableName: "mat_view",
			desc: &sqlbase.TableDescriptor{
				Name:               "mat_view",
				IsMaterializedView: true,
				Columns: []sqlbase.ColumnDescriptor{
					{Name: "col1"},
				},
				ViewQuery: "SELECT col1 FROM test_table",
			},
			expected: `CREATE MATERIALIZED VIEW mat_view (col1) AS SELECT col1 FROM test_table`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tn := tree.Name(tt.tableName)
			result, err := ShowCreateView(context.Background(), &tn, tt.desc)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestShowCreateSequence tests the ShowCreateSequence function
func TestShowCreateSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name        string
		tableName   string
		desc        *sqlbase.TableDescriptor
		expected    string
		expectedErr bool
	}{
		{
			name:      "simple sequence",
			tableName: "test_seq",
			desc: &sqlbase.TableDescriptor{
				Name: "test_seq",
				SequenceOpts: &sqlbase.TableDescriptor_SequenceOpts{
					MinValue:  1,
					MaxValue:  1000,
					Increment: 1,
					Start:     1,
				},
			},
			expected: `CREATE SEQUENCE test_seq MINVALUE 1 MAXVALUE 1000 INCREMENT 1 START 1`,
		},
		{
			name:      "temporary sequence",
			tableName: "temp_seq",
			desc: &sqlbase.TableDescriptor{
				Name:      "temp_seq",
				Temporary: true,
				SequenceOpts: &sqlbase.TableDescriptor_SequenceOpts{
					MinValue:  -100,
					MaxValue:  100,
					Increment: 10,
					Start:     0,
				},
			},
			expected: `CREATE TEMP SEQUENCE temp_seq MINVALUE -100 MAXVALUE 100 INCREMENT 10 START 0`,
		},
		{
			name:      "virtual sequence",
			tableName: "virt_seq",
			desc: &sqlbase.TableDescriptor{
				Name: "virt_seq",
				SequenceOpts: &sqlbase.TableDescriptor_SequenceOpts{
					MinValue:  1,
					MaxValue:  100,
					Increment: 1,
					Start:     1,
					Virtual:   true,
				},
			},
			expected: `CREATE SEQUENCE virt_seq MINVALUE 1 MAXVALUE 100 INCREMENT 1 START 1 VIRTUAL`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tn := tree.Name(tt.tableName)
			result, err := ShowCreateSequence(context.Background(), &tn, tt.desc)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestShowCreateInstanceTable tests the ShowCreateInstanceTable function
func TestShowCreateInstanceTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		sTable   string
		cTable   string
		tagName  []string
		tagValue []string
		typ      []types.T
		expected string
	}{
		{
			name:     "simple instance table",
			sTable:   "template_table",
			cTable:   "instance_table",
			tagName:  []string{"tag1", "tag2"},
			tagValue: []string{"value1", "value2"},
			typ:      []types.T{*types.String, *types.String},
			expected: `CREATE TABLE instance_table USING template_table (
	tag1, 
	tag2 ) TAGS (
	value1, 
	value2 )`,
		},
		{
			name:     "instance table with sde",
			sTable:   "template_table",
			cTable:   "instance_table_sde",
			tagName:  []string{"tag1"},
			tagValue: []string{"value1"},
			typ:      []types.T{*types.String},
			expected: `CREATE TABLE instance_table_sde USING template_table (
	tag1 ) TAGS (
	value1 )`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sTable := tree.Name(tt.sTable)
			result := ShowCreateInstanceTable(sTable, tt.cTable, tt.tagName, tt.tagValue, tt.typ)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestShowFamilyClause tests the showFamilyClause function
func TestShowFamilyClause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		desc     *sqlbase.TableDescriptor
		expected string
	}{
		{
			name: "single family",
			desc: &sqlbase.TableDescriptor{
				Families: []sqlbase.ColumnFamilyDescriptor{
					{
						Name:        "primary",
						ColumnNames: []string{"id", "name"},
						ColumnIDs:   []sqlbase.ColumnID{1, 2},
					},
				},
				Columns: []sqlbase.ColumnDescriptor{
					{ID: 1, Name: "id"},
					{ID: 2, Name: "name"},
				},
				PrimaryIndex: sqlbase.IndexDescriptor{
					ColumnIDs: []sqlbase.ColumnID{1},
				},
			},
			expected: `,
	FAMILY "primary" (id, name)`,
		},
		{
			name: "multiple families",
			desc: &sqlbase.TableDescriptor{
				Families: []sqlbase.ColumnFamilyDescriptor{
					{
						Name:        "f1",
						ColumnNames: []string{"id"},
						ColumnIDs:   []sqlbase.ColumnID{1},
					},
					{
						Name:        "f2",
						ColumnNames: []string{"name", "value"},
						ColumnIDs:   []sqlbase.ColumnID{2, 3},
					},
				},
				Columns: []sqlbase.ColumnDescriptor{
					{ID: 1, Name: "id"},
					{ID: 2, Name: "name"},
					{ID: 3, Name: "value"},
				},
				PrimaryIndex: sqlbase.IndexDescriptor{
					ColumnIDs: []sqlbase.ColumnID{1},
				},
			},
			expected: `,
	FAMILY f1 (id),
	FAMILY f2 (name, value)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tree.NewFmtCtx(tree.FmtSimple)
			showFamilyClause(tt.desc, f)
			require.Equal(t, tt.expected, f.CloseAndGetString())
		})
	}
}

// TestShowConstraintClause tests the showConstraintClause function
func TestShowConstraintClause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		desc     *sqlbase.TableDescriptor
		expected string
	}{
		{
			name: "check constraint",
			desc: &sqlbase.TableDescriptor{
				Checks: []*sqlbase.TableDescriptor_CheckConstraint{
					{
						Name: "check_age",
						Expr: "age > 0",
					},
				},
			},
			expected: `,
	CONSTRAINT check_age CHECK (age > 0)
)`,
		},
		{
			name: "unnamed check constraint",
			desc: &sqlbase.TableDescriptor{
				Checks: []*sqlbase.TableDescriptor_CheckConstraint{
					{
						Expr: "value > 0",
					},
				},
			},
			expected: `,
	CHECK (value > 0)
)`,
		},
		{
			name: "hidden constraint",
			desc: &sqlbase.TableDescriptor{
				Checks: []*sqlbase.TableDescriptor_CheckConstraint{
					{
						Name:   "hidden_check",
						Expr:   "value > 0",
						Hidden: true,
					},
				},
			},
			expected: `
)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tree.NewFmtCtx(tree.FmtSimple)
			showConstraintClause(tt.desc, f)
			require.Equal(t, tt.expected, f.CloseAndGetString())
		})
	}
}

// TestShowComments tests the showComments function
func TestShowComments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name      string
		tableDesc *sqlbase.TableDescriptor
		comments  *tableComments
		expected  string
		expectErr bool
	}{
		{
			name: "nil comments",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
			},
			comments: nil,
			expected: "",
		},
		{
			name: "table comment only",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
			},
			comments: &tableComments{
				comment: strPtr("This is a test table"),
			},
			expected: `;
COMMENT ON TABLE test_table IS 'This is a test table'`,
		},
		{
			name: "column comments",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
				Columns: []sqlbase.ColumnDescriptor{
					{ID: 1, Name: "id"},
					{ID: 2, Name: "name"},
				},
			},
			comments: &tableComments{
				columns: []comment{
					{subID: 1, comment: "The primary key"},
					{subID: 2, comment: "User name"},
				},
			},
			expected: `;
COMMENT ON COLUMN test_table.id IS 'The primary key';
COMMENT ON COLUMN test_table.name IS 'User name'`,
		},
		{
			name: "index comments",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
				Columns: []sqlbase.ColumnDescriptor{
					{ID: 1, Name: "id"},
				},
				Indexes: []sqlbase.IndexDescriptor{
					{ID: 1, Name: "idx_id"},
				},
			},
			comments: &tableComments{
				indexes: []comment{
					{subID: 1, comment: "Index on id column"},
				},
			},
			expected: `;
COMMENT ON INDEX test_table@idx_id IS 'Index on id column'`,
		},
		{
			name: "all comments",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
				Columns: []sqlbase.ColumnDescriptor{
					{ID: 1, Name: "id"},
					{ID: 2, Name: "name"},
				},
				Indexes: []sqlbase.IndexDescriptor{
					{ID: 1, Name: "idx_name"},
				},
			},
			comments: &tableComments{
				comment: strPtr("Main table"),
				columns: []comment{
					{subID: 1, comment: "Primary key"},
				},
				indexes: []comment{
					{subID: 1, comment: "Name index"},
				},
			},
			expected: `;
COMMENT ON TABLE test_table IS 'Main table';
COMMENT ON COLUMN test_table.id IS 'Primary key';
COMMENT ON INDEX test_table@idx_name IS 'Name index'`,
		},
		{
			name: "invalid column id",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
			},
			comments: &tableComments{
				columns: []comment{
					{subID: 999, comment: "Invalid column"},
				},
			},
			expectErr: true,
		},
		{
			name: "invalid index id",
			tableDesc: &sqlbase.TableDescriptor{
				Name: "test_table",
			},
			comments: &tableComments{
				indexes: []comment{
					{subID: 999, comment: "Invalid index"},
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := showComments(tt.tableDesc, tt.comments, &buf)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, buf.String())
			}
		})
	}
}

// Helper function
func strPtr(s string) *string {
	return &s
}

// TestShowCreatePartitioning tests the ShowCreatePartitioning function
func TestShowCreatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name      string
		tableDesc *sqlbase.TableDescriptor
		idxDesc   *sqlbase.IndexDescriptor
		partDesc  *sqlbase.PartitioningDescriptor
		indent    int
		colOffset int
		expected  string
		expectErr bool
	}{
		{
			name:      "no partitioning",
			tableDesc: &sqlbase.TableDescriptor{},
			idxDesc:   &sqlbase.IndexDescriptor{},
			partDesc:  &sqlbase.PartitioningDescriptor{NumColumns: 0},
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			a := &sqlbase.DatumAlloc{}
			err := ShowCreatePartitioning(a, tt.tableDesc, tt.idxDesc, tt.partDesc, &buf, tt.indent, tt.colOffset)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, buf.String())
			}
		})
	}
}

// TestReParseCreateTrigger tests the reParseCreateTrigger function
func TestReParseCreateTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name        string
		desc        *sqlbase.TriggerDescriptor
		expectError bool
		errorMsg    string
	}{
		{
			name: "invalid sql",
			desc: &sqlbase.TriggerDescriptor{
				TriggerBody: "INVALID SQL STATEMENT",
			},
			expectError: true,
			errorMsg:    "syntax error",
		},
		{
			name: "not a create trigger statement",
			desc: &sqlbase.TriggerDescriptor{
				TriggerBody: "SELECT * FROM test_table",
			},
			expectError: true,
			errorMsg:    "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := reParseCreateTrigger(tt.desc)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}
}

// TestShowCreateDisplayOptions tests the default values of ShowCreateDisplayOptions struct
func TestShowCreateDisplayOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// 测试默认的显示选项
	defaultOpts := ShowCreateDisplayOptions{
		FKDisplayMode:  IncludeFkClausesInCreate,
		IgnoreComments: false,
	}

	require.Equal(t, IncludeFkClausesInCreate, defaultOpts.FKDisplayMode)
	require.False(t, defaultOpts.IgnoreComments)

	// 测试 OmitFKClausesFromCreate 模式
	omitOpts := ShowCreateDisplayOptions{
		FKDisplayMode:  OmitFKClausesFromCreate,
		IgnoreComments: true,
	}

	require.Equal(t, OmitFKClausesFromCreate, omitOpts.FKDisplayMode)
	require.True(t, omitOpts.IgnoreComments)

	// 测试 OmitMissingFKClausesFromCreate 模式
	omitMissingOpts := ShowCreateDisplayOptions{
		FKDisplayMode:  OmitMissingFKClausesFromCreate,
		IgnoreComments: false,
	}

	require.Equal(t, OmitMissingFKClausesFromCreate, omitMissingOpts.FKDisplayMode)
	require.False(t, omitMissingOpts.IgnoreComments)
}
