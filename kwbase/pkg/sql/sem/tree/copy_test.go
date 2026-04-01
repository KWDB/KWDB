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

func TestCopyFromFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CopyFrom
		expected string
	}{
		{
			name: "basic copy",
			node: &CopyFrom{
				Table: MakeTableName("test_db", "test_table"),
				Stdin: false,
			},
			expected: "COPY test_db.public.test_table FROM ",
		},
		{
			name: "copy with stdin",
			node: &CopyFrom{
				Table: MakeTableName("test_db", "test_table"),
				Stdin: true,
			},
			expected: "COPY test_db.public.test_table FROM STDIN",
		},
		{
			name: "copy with columns",
			node: &CopyFrom{
				Table:   MakeTableName("test_db", "test_table"),
				Columns: NameList{"col1", "col2"},
				Stdin:   true,
			},
			expected: "COPY test_db.public.test_table (col1, col2) FROM STDIN",
		},
		// For copy with options, the options format is tested in other tests,
		// so we just provide a basic mock or verify string format directly.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}
