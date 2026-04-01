// Copyright 2015 The Cockroach Authors.
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
