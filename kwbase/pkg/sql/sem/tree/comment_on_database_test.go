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

func TestCommentOnDatabaseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dbName := Name("db1")
	comment := "This is a database comment"
	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		node     *CommentOnDatabase
		expected string
	}{
		{
			node: &CommentOnDatabase{
				Name:    dbName,
				Comment: &comment,
			},
			expected: `COMMENT ON DATABASE db1 IS 'This is a database comment'`},
		{
			node: &CommentOnDatabase{
				Name:    dbName,
				Comment: nil,
			},
			expected: `COMMENT ON DATABASE db1 IS NULL`},
		{
			node: &CommentOnDatabase{
				Name:    Name("test_db"),
				Comment: &comment,
			},
			expected: `COMMENT ON DATABASE test_db IS 'This is a database comment'`},
		{node: &CommentOnDatabase{
			Name:    dbName,
			Comment: strPtr("Comment with 'single' quotes"),
		},
			expected: `COMMENT ON DATABASE db1 IS 'Comment with ''single'' quotes'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
