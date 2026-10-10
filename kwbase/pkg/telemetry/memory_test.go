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

package telemetry

import (
	"math"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestGetMaxTimespan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		r                   Resolution
		opts                QueryMemoryOptions
		expectedTimespan    int64
		expectedErrorString string
	}{
		// Simplest case: One series, room for exactly one hour of query (need two
		// slabs of memory budget, as queried time span may stagger across two
		// slabs)
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             2 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        1,
				InterpolationLimitNanos: 0,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room for to make query.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             sizeOfTimeSeriesData + sizeOfSample*360,
				EstimatedSources:        1,
				InterpolationLimitNanos: 0,
			},
			0,
			"insufficient",
		},
		// Not enough room because of multiple sources.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             2 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        2,
				InterpolationLimitNanos: 0,
			},
			0,
			"insufficient",
		},
		// 6 sources, room for 1 hour.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             12 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 0,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 6 sources, room for 2 hours.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 0,
			},
			(2 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room due to interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             12 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 1,
			},
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 1,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Insufficient room for interpolation buffer (due to straddling)
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: int64(float64(Resolution10s.SlabDuration()) * 0.75),
			},
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             24 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: int64(float64(Resolution10s.SlabDuration()) * 0.75),
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 1ns test resolution.
		{
			resolution1ns,
			QueryMemoryOptions{
				BudgetBytes:             3 * (sizeOfTimeSeriesData + sizeOfSample*10),
				EstimatedSources:        1,
				InterpolationLimitNanos: 1,
			},
			10,
			"",
		},
		// Overflow.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             math.MaxInt64,
				EstimatedSources:        1,
				InterpolationLimitNanos: math.MaxInt64,
			},
			math.MaxInt64,
			"",
		},
	} {
		t.Run("", func(t *testing.T) {
			mem := QueryMemoryContext{
				QueryMemoryOptions: tc.opts,
			}
			actual, err := mem.GetMaxTimespan(tc.r)
			if !testutils.IsError(err, tc.expectedErrorString) {
				t.Fatalf("got error %s, wanted error matching %s", err, tc.expectedErrorString)
			}
			if tc.expectedErrorString == "" {
				return
			}
			if a, e := actual, tc.expectedTimespan; a != e {
				t.Fatalf("got max timespan %d, wanted %d", a, e)
			}
		})
	}
}
