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

func TestAlterSequenceFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	name1, err := NewUnresolvedObjectName(1, [3]string{"seq1"}, NoAnnotation)
	require.NoError(t, err)
	name2, err := NewUnresolvedObjectName(1, [3]string{"seq1"}, NoAnnotation)
	require.NoError(t, err)
	name3, err := NewUnresolvedObjectName(2, [3]string{"seq1", "db1", ""}, NoAnnotation)
	require.NoError(t, err)

	testCases := []struct {
		node     *AlterSequence
		expected string
	}{
		{
			node: &AlterSequence{
				IfExists: false,
				Name:     name1,
				Options:  SequenceOptions{},
			},
			expected: "ALTER SEQUENCE seq1",
		},
		{
			node: &AlterSequence{
				IfExists: true,
				Name:     name2,
				Options:  SequenceOptions{},
			},
			expected: "ALTER SEQUENCE IF EXISTS seq1",
		},
		{
			node: &AlterSequence{
				IfExists: false,
				Name:     name3,
				Options:  SequenceOptions{},
			},
			expected: "ALTER SEQUENCE db1.seq1",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
