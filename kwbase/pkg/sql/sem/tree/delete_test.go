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

func TestDeleteFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *Delete
		expected string
	}{
		{
			name: "basic delete",
			node: &Delete{
				Table: &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
			},
			expected: `DELETE FROM db.public.tbl`,
		},
		{
			name: "delete with where",
			node: &Delete{
				Table: &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
				Where: &Where{
					Type: AstWhere,
					Expr: &ComparisonExpr{
						Operator: EQ,
						Left:     &UnresolvedName{NumParts: 1, Parts: NameParts{"a"}},
						Right:    NewNumVal(nil, "1", false),
					},
				},
			},
			expected: `DELETE FROM db.public.tbl WHERE a = 1`,
		},
		{
			name: "delete with order by and limit",
			node: &Delete{
				Table: &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
				OrderBy: OrderBy{
					&Order{
						Expr:      &UnresolvedName{NumParts: 1, Parts: NameParts{"a"}},
						Direction: Descending,
					},
				},
				Limit: &Limit{Count: NewNumVal(nil, "10", false)},
			},
			expected: `DELETE FROM db.public.tbl ORDER BY a DESC LIMIT 10`,
		},
		{
			name: "delete with returning",
			node: &Delete{
				Table: &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
				Returning: &ReturningExprs{
					{Expr: &UnresolvedName{NumParts: 1, Parts: NameParts{"a"}}},
					{Expr: &UnresolvedName{NumParts: 1, Parts: NameParts{"b"}}},
				},
			},
			expected: `DELETE FROM db.public.tbl RETURNING a, b`,
		},
		{
			name: "delete with CTE",
			node: &Delete{
				With: &With{
					CTEList: []*CTE{
						{
							Name: AliasClause{Alias: "cte1"},
							Stmt: &Select{
								Select: &SelectClause{
									Exprs: SelectExprs{
										{Expr: NewNumVal(nil, "1", false)},
									},
								},
							},
						},
					},
				},
				Table: &AliasedTableExpr{Expr: func() *TableName { n := MakeTableName("db", "tbl"); return &n }()},
			},
			expected: `WITH cte1 AS (SELECT 1) DELETE FROM db.public.tbl`,
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
