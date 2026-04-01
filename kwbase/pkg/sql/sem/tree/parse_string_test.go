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

package tree_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseAndRequireString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer ctx.Stop(context.Background())

	tests := []struct {
		name        string
		typ         *types.T
		str         string
		expected    string
		expectError bool
	}{
		{
			name:     "array",
			typ:      types.MakeArray(types.Int),
			str:      `{1,2,3}`,
			expected: `ARRAY[1,2,3]`,
		},
		{
			name:     "bit",
			typ:      types.MakeBit(3),
			str:      `101`,
			expected: `B'101'`,
		},
		{
			name:     "bool",
			typ:      types.Bool,
			str:      `true`,
			expected: `true`,
		},
		{
			name:     "bytes",
			typ:      types.Bytes,
			str:      `hello`,
			expected: `'\x68656c6c6f'`,
		},
		{
			name:     "date",
			typ:      types.Date,
			str:      `2023-01-01`,
			expected: `'2023-01-01'`,
		},
		{
			name:     "decimal",
			typ:      types.Decimal,
			str:      `123.45`,
			expected: `123.45`,
		},
		{
			name:     "float",
			typ:      types.Float,
			str:      `3.14`,
			expected: `3.14`,
		},
		{
			name:     "inet",
			typ:      types.INet,
			str:      `192.168.1.1`,
			expected: `'192.168.1.1'`,
		},
		{
			name:     "int",
			typ:      types.Int,
			str:      `42`,
			expected: `42`,
		},
		{
			name:     "interval",
			typ:      types.Interval,
			str:      `1h`,
			expected: `'01:00:00'`,
		},
		{
			name:     "json",
			typ:      types.Jsonb,
			str:      `{"a": 1}`,
			expected: `'{"a": 1}'`,
		},
		{
			name:     "oid",
			typ:      types.Oid,
			str:      `123`,
			expected: `123`,
		},
		{
			name:     "string",
			typ:      types.String,
			str:      `hello`,
			expected: `'hello'`,
		},
		{
			name:     "string with width limit",
			typ:      types.MakeString(2),
			str:      `hello`,
			expected: `'he'`,
		},
		{
			name:     "time",
			typ:      types.Time,
			str:      `12:34:56`,
			expected: `'12:34:56'`,
		},
		{
			name:     "timestamp",
			typ:      types.Timestamp,
			str:      `2023-01-01 12:00:00`,
			expected: `'2023-01-01 12:00:00+00:00'`,
		},
		{
			name:     "timestamptz",
			typ:      types.TimestampTZ,
			str:      `2023-01-01 12:00:00+00:00`,
			expected: `'2023-01-01 12:00:00+00:00'`,
		},
		{
			name:     "uuid",
			typ:      types.Uuid,
			str:      `123e4567-e89b-12d3-a456-426655440000`,
			expected: `'123e4567-e89b-12d3-a456-426655440000'`,
		},
		{
			name:        "unsupported collated string",
			typ:         types.MakeCollatedString(types.String, "en"),
			str:         `hello`,
			expectError: true,
		},
		{
			name:        "unknown type",
			typ:         types.Any,
			str:         `hello`,
			expectError: true,
		},
		{
			name:        "parse error",
			typ:         types.Int,
			str:         `hello`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			datum, err := tree.ParseAndRequireString(tt.typ, tt.str, ctx)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, datum)
				require.Equal(t, tt.expected, datum.String())
			}
		})
	}
}

func TestTSParseAndRequireString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer ctx.Stop(context.Background())

	tests := []struct {
		name        string
		typ         *types.T
		str         string
		expected    string
		expectError bool
		errMsg      string
	}{
		{
			name:        "valid string",
			typ:         types.String,
			str:         "hello",
			expectError: true,
			errMsg:      "too long for type",
		},
		{
			name:     "valid varchar",
			typ:      types.MakeVarChar(10, 0),
			str:      "hello",
			expected: `'hello'`,
		},
		{
			name:        "varchar truncation",
			typ:         types.MakeVarChar(2, 0),
			str:         "hello",
			expectError: true,
			errMsg:      "too long for type",
		},
		{
			name:     "valid nvarchar",
			typ:      types.MakeNVarChar(10),
			str:      "hello",
			expected: `'hello'`,
		},
		{
			name:        "nvarchar truncation",
			typ:         types.MakeNVarChar(2),
			str:         "hello",
			expectError: true,
			errMsg:      "too long for type",
		},
		{
			name:        "bytes truncation",
			typ:         types.MakeVarBytes(2, 0),
			str:         "bytes",
			expectError: true,
			errMsg:      "too long for type",
		},
		{
			name:     "valid int",
			typ:      types.Int,
			str:      "42",
			expected: `42`,
		},
		{
			name:        "invalid int format",
			typ:         types.Int,
			str:         "abc",
			expectError: true,
			errMsg:      "could not parse",
		},
		{
			name:     "valid timestamp",
			typ:      types.Timestamp,
			str:      "2023-01-01 12:00:00",
			expected: `1672574400000`, // Unix micro
		},
		{
			name:     "valid timestamp missing timezone",
			typ:      types.TimestampTZ,
			str:      "2023-01-01 12:00:00",
			expected: `1672574400000`,
		},
		{
			name:        "unsupported type for TS",
			typ:         types.Any,
			str:         "any",
			expectError: true,
			errMsg:      "could not resolve",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			datum, err := tree.TSParseAndRequireString("test_col", tt.typ, tt.str, ctx)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, datum)
				require.Equal(t, tt.expected, datum.String())
			}
		})
	}
}
