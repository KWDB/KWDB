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

func TestAlterTSDatabaseFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *AlterTSDatabase
		expected string
	}{
		{
			node: &AlterTSDatabase{
				Database: Name("tsdb1"),
			},
			expected: "ALTER TS DATABASE SET",
		},
		{
			node: &AlterTSDatabase{
				Database: Name("tsdb1"),
				LifeTime: &TimeInput{Value: 30, Unit: "d"},
			},
			expected: "ALTER TS DATABASE SET LIFETIME = 30d",
		},
		{
			node: &AlterTSDatabase{
				Database:          Name("tsdb1"),
				PartitionInterval: &TimeInput{Value: 7, Unit: "d"},
			},
			expected: "ALTER TS DATABASE SET PARTITION INTERVAL = 7d",
		},
		{
			node: &AlterTSDatabase{
				Database:          Name("tsdb1"),
				LifeTime:          &TimeInput{Value: 30, Unit: "d"},
				PartitionInterval: &TimeInput{Value: 7, Unit: "d"},
			},
			expected: "ALTER TS DATABASE SET LIFETIME = 30d PARTITION INTERVAL = 7d",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
