// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//
//	http://license.coscl.org.cn/MulanPSL2
//
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

func TestAlterIndexFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *AlterIndex
		expected string
	}{
		{
			node: &AlterIndex{
				IfExists: false,
				Index:    TableIndexName{Table: TableName{tblName: tblName{TableName: "tbl"}}, Index: "idx"},
				Cmds:     AlterIndexCmds{},
			},
			expected: "ALTER INDEX tbl@idx",
		},
		{
			node: &AlterIndex{
				IfExists: true,
				Index:    TableIndexName{Table: TableName{tblName: tblName{TableName: "tbl"}}, Index: "idx"},
				Cmds:     AlterIndexCmds{},
			},
			expected: "ALTER INDEX IF EXISTS tbl@idx",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}

func TestAlterIndexCmdsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *AlterIndexCmds
		expected string
	}{
		{
			node:     &AlterIndexCmds{},
			expected: "",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
