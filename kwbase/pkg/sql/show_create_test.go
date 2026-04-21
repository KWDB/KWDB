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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestFormatQuoteNames tests the formatQuoteNames function
func TestFormatQuoteNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		names    []string
		expected string
	}{
		{
			name:     "single name",
			names:    []string{"col1"},
			expected: "col1",
		},
		{
			name:     "multiple names",
			names:    []string{"col1", "col2", "col3"},
			expected: "col1, col2, col3",
		},
		{
			name:     "empty names",
			names:    []string{},
			expected: "",
		},
		{
			name:     "names requiring quotes",
			names:    []string{"column name"},
			expected: `"column name"`,
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

// TestShowCreateInstanceTableExtended tests the ShowCreateInstanceTable function
func TestShowCreateInstanceTableExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name          string
		sTable        tree.Name
		cTable        string
		tagName       []string
		tagValue      []string
		sde           bool
		typ           []types.T
		shouldContain []string
	}{
		{
			name:     "basic instance table creation",
			sTable:   "super_table",
			cTable:   "child_table",
			tagName:  []string{"tag1", "tag2"},
			tagValue: []string{"'val1'", "123"},
			sde:      false,
			typ:      []types.T{*types.String, *types.Int},
			shouldContain: []string{
				"CREATE TABLE child_table",
				"USING super_table",
				"tag1",
				"tag2",
			},
		},
		{
			name:     "instance table with SDE",
			sTable:   "template_table",
			cTable:   "instance_table",
			tagName:  []string{"id"},
			tagValue: []string{"1"},
			sde:      true,
			typ:      []types.T{*types.Int},
			shouldContain: []string{
				"CREATE TABLE instance_table",
				"USING template_table",
				"DICT ENCODING",
			},
		},
		{
			name:     "empty tag value",
			sTable:   "super_table",
			cTable:   "child_table",
			tagName:  []string{"tag1"},
			tagValue: []string{""},
			sde:      false,
			typ:      []types.T{*types.String},
			shouldContain: []string{
				"CREATE TABLE child_table",
				"USING super_table",
			},
		},
		{
			name:     "bytes type tag value",
			sTable:   "super_table",
			cTable:   "child_table",
			tagName:  []string{"data"},
			tagValue: []string{"'hello'"},
			sde:      false,
			typ:      []types.T{*types.Bytes},
			shouldContain: []string{
				"CREATE TABLE child_table",
				"USING super_table",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShowCreateInstanceTable(tt.sTable, tt.cTable, tt.tagName, tt.tagValue, tt.sde, tt.typ)
			for _, expected := range tt.shouldContain {
				require.Contains(t, result, expected)
			}
		})
	}
}

// TestReParseCreateTrigger tests the reParseCreateTrigger function
func TestReParseCreateTrigger(t *testing.T) {
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
		{
			name: "valid create trigger",
			desc: &sqlbase.TriggerDescriptor{
				TriggerBody: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW INSERT INTO log_table VALUES (1)",
			},
			expectError: false,
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
