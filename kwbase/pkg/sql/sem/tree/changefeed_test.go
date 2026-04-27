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

func TestCreateChangefeedFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tbl1 := &TableName{tblName: tblName{TableName: "tbl1"}}
	tbl2 := &TableName{tblName: tblName{TableName: "tbl2"}}

	testCases := []struct {
		node     *CreateChangefeed
		expected string
	}{
		{
			node: &CreateChangefeed{
				Targets: TargetList{Tables: TablePatterns{tbl1}},
				SinkURI: NewDString("kafka://localhost:9092"),
				Options: nil,
			},
			expected: `CREATE CHANGEFEED FOR TABLE tbl1 INTO 'kafka://localhost:9092'`},
		{
			node: &CreateChangefeed{
				Targets: TargetList{Databases: NameList{"db1"}},
				SinkURI: NewDString("kafka://localhost:9092"),
				Options: nil,
			},
			expected: `CREATE CHANGEFEED FOR DATABASE db1 INTO 'kafka://localhost:9092'`},
		{
			node: &CreateChangefeed{
				Targets: TargetList{Tables: TablePatterns{tbl1}},
				SinkURI: nil,
				Options: nil,
			},
			expected: `EXPERIMENTAL CHANGEFEED FOR TABLE tbl1`},
		{
			node: &CreateChangefeed{
				Targets: TargetList{Tables: TablePatterns{tbl1}},
				SinkURI: NewDString("kafka://localhost:9092"),
				Options: KVOptions{{Key: "updated", Value: nil}},
			},
			expected: `CREATE CHANGEFEED FOR TABLE tbl1 INTO 'kafka://localhost:9092' WITH updated`},
		{
			node: &CreateChangefeed{
				Targets: TargetList{
					Tables: TablePatterns{tbl1, tbl2},
				},
				SinkURI: NewDString("kafka://localhost:9092"),
				Options: nil,
			},
			expected: `CREATE CHANGEFEED FOR TABLE tbl1, tbl2 INTO 'kafka://localhost:9092'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
