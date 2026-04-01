// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

func TestCommentOnColumnFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	colItem := &ColumnItem{
		ColumnName: Name("col1"),
	}

	comment := "This is a column comment"
	nullComment := "NULL"

	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		node     *CommentOnColumn
		expected string
	}{
		{
			node: &CommentOnColumn{
				ColumnItem: colItem,
				Comment:    &comment,
			},
			expected: `COMMENT ON COLUMN col1 IS 'This is a column comment'`},
		{
			node: &CommentOnColumn{
				ColumnItem: colItem,
				Comment:    nil,
			},
			expected: `COMMENT ON COLUMN col1 IS NULL`},
		{node: &CommentOnColumn{
			ColumnItem: &ColumnItem{
				TableName:  &UnresolvedObjectName{NumParts: 1, Parts: [3]string{"tbl"}},
				ColumnName: Name("col1"),
			},
			Comment: &comment,
		},
			expected: `COMMENT ON COLUMN tbl.col1 IS 'This is a column comment'`},
		{node: &CommentOnColumn{
			ColumnItem: &ColumnItem{
				TableName:  &UnresolvedObjectName{NumParts: 2, Parts: [3]string{"tbl", "db"}},
				ColumnName: Name("col1"),
			},
			Comment: &comment,
		},
			expected: `COMMENT ON COLUMN db.tbl.col1 IS 'This is a column comment'`},
		{
			node: &CommentOnColumn{
				ColumnItem: colItem,
				Comment:    &nullComment,
			},
			expected: `COMMENT ON COLUMN col1 IS 'NULL'`},
		{node: &CommentOnColumn{
			ColumnItem: colItem,
			Comment:    strPtr("Comment with 'single' quotes"),
		},
			expected: `COMMENT ON COLUMN col1 IS 'Comment with ''single'' quotes'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
