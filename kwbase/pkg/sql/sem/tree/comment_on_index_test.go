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

func TestCommentOnIndexFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	indexName := TableIndexName{
		Table: TableName{tblName: tblName{TableName: "tbl"}},
		Index: "idx",
	}

	comment := "This is an index comment"
	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		node     *CommentOnIndex
		expected string
	}{
		{
			node: &CommentOnIndex{
				Index:   indexName,
				Comment: &comment,
			},
			expected: `COMMENT ON INDEX tbl@idx IS 'This is an index comment'`},
		{
			node: &CommentOnIndex{
				Index:   indexName,
				Comment: nil,
			},
			expected: `COMMENT ON INDEX tbl@idx IS NULL`},
		{node: &CommentOnIndex{
			Index: TableIndexName{
				Table: MakeTableNameFromPrefix(
					TableNamePrefix{SchemaName: "public", ExplicitSchema: true},
					"test_table",
				),
				Index: "test_index",
			},
			Comment: &comment,
		},
			expected: `COMMENT ON INDEX public.test_table@test_index IS 'This is an index comment'`},
		{node: &CommentOnIndex{
			Index:   indexName,
			Comment: strPtr("Comment with 'single' quotes"),
		},
			expected: `COMMENT ON INDEX tbl@idx IS 'Comment with ''single'' quotes'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
