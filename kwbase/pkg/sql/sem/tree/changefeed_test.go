// Copyright 2018 The Cockroach Authors.
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
