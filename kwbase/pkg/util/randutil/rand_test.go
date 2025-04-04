// Copyright 2014 The Cockroach Authors.
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

package randutil_test

import (
	"testing"

	_ "gitee.com/kwbasedb/kwbase/pkg/util/log" // for flags
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestPseudoRand(t *testing.T) {
	numbers := make(map[int]bool)
	// Make two random number generators and pull two numbers from each.
	rand1, _ := randutil.NewPseudoRand()
	rand2, _ := randutil.NewPseudoRand()
	numbers[rand1.Int()] = true
	numbers[rand1.Int()] = true
	numbers[rand2.Int()] = true
	numbers[rand2.Int()] = true
	// All four numbers should be distinct; no seed state is shared.
	if len(numbers) != 4 {
		t.Errorf("expected 4 unique numbers; got %d", len(numbers))
	}
}

func TestRandIntInRange(t *testing.T) {
	rand, _ := randutil.NewPseudoRand()
	for i := 0; i < 100; i++ {
		x := randutil.RandIntInRange(rand, 20, 40)
		if x < 20 || x >= 40 {
			t.Errorf("got result out of range: %d", x)
		}
	}
}

func TestRandBytes(t *testing.T) {
	rand, _ := randutil.NewPseudoRand()
	for i := 0; i < 100; i++ {
		x := randutil.RandBytes(rand, i)
		if len(x) != i {
			t.Errorf("got array with unexpected length: %d (expected %d)", len(x), i)
		}
	}
}
