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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExportFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *Export
		expected string
	}{
		{
			name: "export query",
			node: &Export{
				Query: &Select{
					Select: &SelectClause{
						Exprs: SelectExprs{
							{Expr: &UnresolvedName{NumParts: 1, Parts: NameParts{"a"}}},
						},
						From: From{
							Tables: TableExprs{
								&AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
							},
						},
					},
				},
				FileFormat: "CSV",
				File:       NewStrVal("s3://bucket/path"),
			},
			expected: `EXPORT INTO CSV 's3://bucket/path' FROM SELECT a FROM db.public.tbl`,
		},
		{
			name: "export database",
			node: &Export{
				Database:   "mydb",
				FileFormat: "CSV",
				File:       NewStrVal("s3://bucket/path"),
			},
			expected: `EXPORT INTO CSV 's3://bucket/path' FROM DATABASE mydb`,
		},
		{
			name: "export database with options",
			node: &Export{
				Database:   "mydb",
				FileFormat: "CSV",
				File:       NewStrVal("s3://bucket/path"),
				Options: KVOptions{
					{Key: "delimiter", Value: NewStrVal(",")},
				},
			},
			expected: `EXPORT INTO CSV 's3://bucket/path' FROM DATABASE mydb WITH delimiter = ','`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}
