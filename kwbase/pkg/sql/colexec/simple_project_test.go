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

package colexec

import (
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSimpleProjectOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples     tuples
		expected   tuples
		colsToKeep []uint32
	}{
		{
			colsToKeep: []uint32{0, 2},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 3},
				{1, 3},
			},
		},
		{
			colsToKeep: []uint32{0, 1},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 2},
				{1, 2},
			},
		},
		{
			colsToKeep: []uint32{2, 1},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{3, 2},
				{3, 2},
			},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], len(tc.tuples[0]), tc.colsToKeep), nil
		})
	}

	// Empty projection. The all nulls injection test case will also return
	// nothing.
	runTestsWithoutAllNullsInjection(t, []tuples{{{1, 2, 3}, {1, 2, 3}}}, nil /* typs */, tuples{{}, {}}, orderedVerifier,
		func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], 3 /* numInputCols */, nil), nil
		})

	t.Run("RedundantProjectionIsNotPlanned", func(t *testing.T) {
		typs := []coltypes.T{coltypes.Int64, coltypes.Int64}
		input := newFiniteBatchSource(testAllocator.NewMemBatch(typs), 1 /* usableCount */)
		projectOp := NewSimpleProjectOp(input, len(typs), []uint32{0, 1})
		require.IsType(t, input, projectOp)
	})
}

// TestSimpleProjectOpWithUnorderedSynchronizer sets up the following
// structure:
//
//	input 1 --
//	          | --> unordered synchronizer --> simpleProjectOp --> constInt64Op
//	input 2 --
//
// and makes sure that the output is as expected. The idea is to test
// simpleProjectOp in case when it receives multiple "different internally"
// batches. See #45686 for detailed discussion.
func TestSimpleProjectOpWithUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	inputTypes := []coltypes.T{coltypes.Bytes, coltypes.Float64}
	constVal := int64(42)
	var wg sync.WaitGroup
	inputTuples := []tuples{
		{{"a", 1.0}, {"aa", 10.0}},
		{{"b", 2.0}, {"bb", 20.0}},
	}
	expected := tuples{
		{"a", constVal},
		{"aa", constVal},
		{"b", constVal},
		{"bb", constVal},
	}
	runTestsWithoutAllNullsInjection(t, inputTuples, [][]coltypes.T{inputTypes, inputTypes}, expected,
		unorderedVerifier, func(inputs []Operator) (Operator, error) {
			var input Operator
			input = NewParallelUnorderedSynchronizer(inputs, inputTypes, &wg)
			input = NewSimpleProjectOp(input, len(inputTypes), []uint32{0})
			return NewConstOp(testAllocator, input, coltypes.Int64, constVal, 1)
		})
	wg.Wait()
}
