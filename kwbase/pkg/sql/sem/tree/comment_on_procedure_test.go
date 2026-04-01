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

func TestCommentOnProcedureFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	procName := TableName{tblName: tblName{TableName: "proc"}}
	comment := "This is a procedure comment"
	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		node     *CommentOnProcedure
		expected string
	}{
		{
			node: &CommentOnProcedure{
				Name:    procName,
				Comment: &comment,
			},
			expected: `COMMENT ON PROCEDURE proc IS 'This is a procedure comment'`},
		{
			node: &CommentOnProcedure{
				Name:    procName,
				Comment: nil,
			},
			expected: `COMMENT ON PROCEDURE proc IS NULL`},
		{ node: &CommentOnProcedure{
			Name: MakeTableNameFromPrefix(
				TableNamePrefix{SchemaName: "public", ExplicitSchema: true},
				"test_proc",
			),
			Comment: &comment,
		},
		expected: `COMMENT ON PROCEDURE public.test_proc IS 'This is a procedure comment'`},
		{ node: &CommentOnProcedure{
			Name:    procName,
			Comment: strPtr("Comment with 'single' quotes"),
		},
		expected: `COMMENT ON PROCEDURE proc IS 'Comment with ''single'' quotes'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
