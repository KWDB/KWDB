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

func TestCommentOnTableFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tblName, err := NewUnresolvedObjectName(1, [3]string{"tbl"}, NoAnnotation)
	require.NoError(t, err)

	comment := "This is a table comment"
	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		node     *CommentOnTable
		expected string
	}{
		{
			node: &CommentOnTable{
				Table:   tblName,
				Comment: &comment,
			},
			expected: `COMMENT ON TABLE tbl IS 'This is a table comment'`},
		{
			node: &CommentOnTable{
				Table:   tblName,
				Comment: nil,
			},
			expected: `COMMENT ON TABLE tbl IS NULL`},
		{
			node: &CommentOnTable{
				Table: func() *UnresolvedObjectName {
					n, _ := NewUnresolvedObjectName(2, [3]string{"test_table", "public"}, NoAnnotation)
					return n
				}(),
				Comment: &comment,
			},
			expected: `COMMENT ON TABLE public.test_table IS 'This is a table comment'`},
		{
			node: &CommentOnTable{
				Table: func() *UnresolvedObjectName {
					n, _ := NewUnresolvedObjectName(3, [3]string{"test_table", "public", "db"}, NoAnnotation)
					return n
				}(),
				Comment: &comment,
			},
			expected: `COMMENT ON TABLE db.public.test_table IS 'This is a table comment'`},
		{node: &CommentOnTable{
			Table:   tblName,
			Comment: strPtr("Comment with 'single' quotes"),
		},
			expected: `COMMENT ON TABLE tbl IS 'Comment with ''single'' quotes'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
