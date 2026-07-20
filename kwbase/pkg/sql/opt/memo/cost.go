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

package memo

import "math"

// Cost is the best-effort approximation of the actual cost of executing a
// particular operator tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost float64

// MaxCost is the maximum possible estimated cost. It's used to suppress memo
// group members during testing, by setting their cost so high that any other
// member will have a lower cost.
var MaxCost = Cost(math.Inf(+1))

// ulpToleranceForCostComparison is the number of units-of-least-precision
// allowed when comparing two costs. Two costs within this ULP distance are
// considered equal. This tolerance accommodates floating-point variance
// introduced by adding identical sub-costs in different orders. Using ULPs
// rather than an absolute epsilon ensures the error budget scales with the
// magnitude of the values.
const ulpToleranceForCostComparison = 1000

// Less returns true if this cost is significantly lower than the given cost.
// Costs within ulpToleranceForCostComparison ULPs are treated as equal to avoid
// spurious plan changes due to floating-point reordering.
func (c Cost) Less(other Cost) bool {
	ourBits := math.Float64bits(float64(c))
	otherBits := math.Float64bits(float64(other))
	return ourBits+ulpToleranceForCostComparison <= otherBits
}

// Sub subtracts the other cost from this cost and returns the result.
func (c Cost) Sub(other Cost) Cost {
	return c - other
}
