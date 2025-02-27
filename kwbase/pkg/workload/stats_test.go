// Copyright 2019 The Cockroach Authors.
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

package workload

import (
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDistinctCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	test := func(rowCount, maxDistinctCount uint64) {
		n, count := float64(maxDistinctCount), float64(0)
		var expected uint64
		// This calculation should produce the same result as the calculation
		// in DistinctCount, but it's easier to see how this is correct (it's also
		// much less efficient). For each row, we select a new value. The
		// probability that it hasn't been seen before is (n-count)/n, where count
		// is the total number of values seen so far, and n is the number of
		// possible values. This probability is also equivalent to the expected
		// value of the increase in distinct values seen so far, so we calculate
		// the expected total number of distinct values by summing this probability
		// over all rows.
		for i := uint64(0); i < rowCount && expected < maxDistinctCount; i++ {
			count += (n - count) / n
			expected = uint64(int64(math.Round(count)))
		}

		actual := DistinctCount(rowCount, maxDistinctCount)
		if expected != actual {
			t.Fatalf("For row count %d and max distinct count %d, expected distinct"+
				" count %d but found %d", rowCount, maxDistinctCount, expected, actual)
		}
	}

	for _, rowCount := range []uint64{0, 1, 10, 100, 1000} {
		for _, maxDistinctCount := range []uint64{1, 10, 100, 1000} {
			test(rowCount, maxDistinctCount)
		}
	}
}
