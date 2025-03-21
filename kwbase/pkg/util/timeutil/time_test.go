// Copyright 2017 The Cockroach Authors.
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

package timeutil

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnixMicros(t *testing.T) {
	testCases := []struct {
		us      int64
		utcTime time.Time
	}{
		{-1, time.Date(1969, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		{0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 1000, time.UTC)},
		{4242424242424242, time.Date(2104, 6, 9, 3, 10, 42, 424242000, time.UTC)},
		{math.MaxInt64, time.Date(294247, 1, 10, 4, 0, 54, 775807000, time.UTC)},
		{-62135596800000000, time.Time{}},
	}
	for i, testCase := range testCases {
		if e, a := testCase.utcTime, FromUnixMicros(testCase.us).UTC(); e != a {
			t.Errorf("%d:FromUnixMicro: expected %v, but got %v", i, e, a)
		}

		if e, a := testCase.us, ToUnixMicros(testCase.utcTime); e != a {
			t.Errorf("%d:ToUnixMicro: expected %v, but got %v", i, e, a)
		}
	}

	for i := 0; i < 32; i++ {
		us := rand.Int63()
		if e, a := us, ToUnixMicros(FromUnixMicros(us)); e != a {
			t.Errorf("%d did not roundtrip; got back %d", e, a)
		}
	}
}

func TestUnixMicrosRounding(t *testing.T) {
	testCases := []struct {
		us      int64
		utcTime time.Time
	}{
		{0, time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC)},
		{0, time.Date(1970, 1, 1, 0, 0, 0, 499, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 500, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 999, time.UTC)},
	}
	for i, testCase := range testCases {
		if e, a := testCase.us, ToUnixMicros(testCase.utcTime); e != a {
			t.Errorf("%d:ToUnixMicro: expected %v, but got %v", i, e, a)
		}
	}
}

func TestReplaceLibPQTimePrefix(t *testing.T) {
	assert.Equal(t, "1970-02-02 11:00", ReplaceLibPQTimePrefix("1970-02-02 11:00"))
	assert.Equal(t, "1970-01-01 11:00", ReplaceLibPQTimePrefix("0000-01-01 11:00"))
}
