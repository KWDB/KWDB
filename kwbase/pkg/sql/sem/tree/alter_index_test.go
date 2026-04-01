// Copyright 2017 The Cockroach Authors.
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
