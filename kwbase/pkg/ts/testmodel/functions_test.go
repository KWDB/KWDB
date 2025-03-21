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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package testmodel

import "testing"

func TestAggregates(t *testing.T) {
	tests := []struct {
		data DataSeries
		// Expected values
		sum      float64
		min      float64
		max      float64
		first    float64
		last     float64
		avg      float64
		variance float64
	}{
		// Single value.
		{
			data: DataSeries{
				dp(1, 23432),
			},
			sum:      23432,
			min:      23432,
			max:      23432,
			first:    23432,
			last:     23432,
			avg:      23432,
			variance: 0,
		},
		// Value is constant.
		{
			data: DataSeries{
				dp(1, 2),
				dp(2, 2),
				dp(3, 2),
				dp(4, 2),
				dp(5, 2),
			},
			sum:      10,
			min:      2,
			max:      2,
			first:    2,
			last:     2,
			avg:      2,
			variance: 0,
		},
		// Value is constant zero.
		{
			data: DataSeries{
				dp(1, 0),
				dp(2, 0),
				dp(3, 0),
				dp(4, 0),
				dp(5, 0),
			},
			sum:      0,
			min:      0,
			max:      0,
			first:    0,
			last:     0,
			avg:      0,
			variance: 0,
		},
		// Value not constant, compute variance.
		{
			data: DataSeries{
				dp(1, 2),
				dp(2, 4),
				dp(3, 6),
			},
			sum:      12,
			min:      2,
			max:      6,
			first:    2,
			last:     6,
			avg:      4,
			variance: 2.6666666666666665,
		},
		// Negative values, variance still positive.
		{
			data: DataSeries{
				dp(1, -3),
				dp(2, -9),
				dp(3, -6),
			},
			sum:      -18,
			min:      -9,
			max:      -3,
			first:    -3,
			last:     -6,
			avg:      -6,
			variance: 6,
		},
		// Some fairly random numbers, variance and average computed from an online
		// calculator. Gives good confidence that the functions are actually doing
		// the correct thing.
		{
			data: DataSeries{
				dp(1, 60),
				dp(2, 3),
				dp(3, 352),
				dp(4, 7),
				dp(5, 143),
			},
			sum:      565,
			min:      3,
			max:      352,
			first:    60,
			last:     143,
			avg:      113,
			variance: 16833.2,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if actual := AggregateSum(tt.data); actual != tt.sum {
				t.Errorf("AggregateSum() = %v, expected %v", actual, tt.sum)
			}
			if actual := AggregateMin(tt.data); actual != tt.min {
				t.Errorf("AggregateMin() = %v, expected %v", actual, tt.min)
			}
			if actual := AggregateMax(tt.data); actual != tt.max {
				t.Errorf("AggregateMax() = %v, expected %v", actual, tt.max)
			}
			if actual := AggregateFirst(tt.data); actual != tt.first {
				t.Errorf("AggregateFirst() = %v, expected %v", actual, tt.first)
			}
			if actual := AggregateLast(tt.data); actual != tt.last {
				t.Errorf("AggregateLast() = %v, expected %v", actual, tt.last)
			}
			if actual := AggregateAverage(tt.data); actual != tt.avg {
				t.Errorf("AggregateAverage() = %v, expected %v", actual, tt.avg)
			}
			if actual := AggregateVariance(tt.data); actual != tt.variance {
				t.Errorf("AggregateVariance() = %v, expected %v", actual, tt.variance)
			}
		})
	}
}
