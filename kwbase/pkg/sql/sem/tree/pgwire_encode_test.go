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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func mustParseDJSONForPgwire(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDJSON(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func createCollatedString(t *testing.T, contents, locale string) *tree.DCollatedString {
	env := tree.CollationEnvironment{}
	d, err := tree.NewDCollatedString(contents, locale, &env)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPgwireFormatTuple(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		datums   []tree.Datum
		expected string
	}{
		{
			name: "ints and strings",
			datums: []tree.Datum{
				tree.NewDInt(1),
				tree.NewDString("hello"),
				tree.NewDString(`str with "quotes" and \backslash`),
			},
			expected: `(1,hello,"str with ""quotes"" and \\backslash")`,
		},
		{
			name: "nulls",
			datums: []tree.Datum{
				tree.DNull,
				tree.NewDInt(2),
			},
			expected: `(,2)`,
		},
		{
			name: "bytes",
			datums: []tree.Datum{
				tree.NewDBytes(`bytedata`),
			},
			expected: `("\\x6279746564617461")`, // FmtNode output for bytes is hex encoded
		},
		{
			name: "json",
			datums: []tree.Datum{
				mustParseDJSONForPgwire(t, `{"a": 1}`),
			},
			expected: `("{""a"": 1}")`, // The JSON contains quotes, which get doubled in tuple
		},
		{
			name: "collated string",
			datums: []tree.Datum{
				createCollatedString(t, "hello", "en"),
			},
			expected: `(hello)`,
		},
		{
			name: "special characters needing quotes",
			datums: []tree.Datum{
				tree.NewDString("str with space"),
				tree.NewDString("str,with,comma"),
				tree.NewDString("str\nwith\nnewline"),
			},
			expected: `("str with space","str,with,comma","str
with
newline")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := tree.NewDTuple(types.MakeTuple(nil), tt.datums...)

			// We format with FmtPgwireText to trigger the pgwireFormat path
			res := tree.AsStringWithFlags(tuple, tree.FmtPgwireText)
			require.Equal(t, tt.expected, res)
		})
	}
}

func TestPgwireFormatArray(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		paramTyp *types.T
		datums   []tree.Datum
		expected string
	}{
		{
			name:     "ints and strings",
			paramTyp: types.String, // The DArray itself holds string type usually if mixed but let's test specific
			datums: []tree.Datum{
				tree.NewDString("hello"),
				tree.NewDString(`str with "quotes" and \backslash`),
			},
			expected: `{hello,"str with \"quotes\" and \\backslash"}`,
		},
		{
			name:     "nulls",
			paramTyp: types.Int,
			datums: []tree.Datum{
				tree.DNull,
				tree.NewDInt(2),
			},
			expected: `{NULL,2}`,
		},
		{
			name:     "bytes",
			paramTyp: types.Bytes,
			datums: []tree.Datum{
				tree.NewDBytes(`bytedata`),
			},
			expected: `{"\\x6279746564617461"}`, // pgwire byte array output
		},
		{
			name:     "collated string",
			paramTyp: types.MakeCollatedString(types.String, "en"),
			datums: []tree.Datum{
				createCollatedString(t, "hello", "en"),
			},
			expected: `{hello}`,
		},
		{
			name:     "special characters needing quotes",
			paramTyp: types.String,
			datums: []tree.Datum{
				tree.NewDString("str with space"),
				tree.NewDString("str,with,comma"),
				tree.NewDString("str\nwith\nnewline"),
				tree.NewDString("NULL"),
				tree.NewDString("null"),
			},
			expected: `{"str with space","str,with,comma","str
with
newline","NULL","null"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tree.NewDArray(tt.paramTyp)
			for _, d := range tt.datums {
				require.NoError(t, arr.Append(d))
			}

			res := tree.AsStringWithFlags(arr, tree.FmtPgwireText)
			require.Equal(t, tt.expected, res)
		})
	}
}

func TestPgwireFormatInt2Vector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// DArray has customOid unexported, but we can construct it via NewDIntVectorFromDArray
	arr := tree.NewDArray(types.Int)
	require.NoError(t, arr.Append(tree.NewDInt(1)))
	require.NoError(t, arr.Append(tree.NewDInt(2)))

	d := tree.NewDIntVectorFromDArray(arr)

	res := tree.AsStringWithFlags(d, tree.FmtPgwireText)
	require.Equal(t, "1 2", res)
}
