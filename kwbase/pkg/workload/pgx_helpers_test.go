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

package workload

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDistribute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, total := range []int{0, 1, 2, 5, 10, 17, 25} {
		for _, num := range []int{1, 2, 3, 4, 5, 8, 13, 15} {
			d := distribute(total, num)
			// Verify the sum is correct and that the variance is no more than 1.
			min, max, sum := d[0], d[0], d[0]
			for i := 1; i < len(d); i++ {
				sum += d[i]
				if min > d[i] {
					min = d[i]
				}
				if max < d[i] {
					max = d[i]
				}
			}
			if sum != total {
				t.Errorf("%d / %d: incorrect sum %d", total, num, sum)
			}
			if max > min+1 {
				t.Errorf("%d / %d: min value %d, max value %d", total, num, min, max)
			}
		}
	}
}
