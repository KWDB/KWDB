// Copyright 2020 The Cockroach Authors.
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

package norm

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// MakeSegmentedOrdering returns an ordering choice which satisfies both
// limitOrdering and the ordering required by a window function. Returns nil if
// no such ordering exists. See OrderingChoice.PrefixIntersection for more
// details.
func (c *CustomFuncs) MakeSegmentedOrdering(
	input memo.RelExpr,
	prefix opt.ColSet,
	ordering physical.OrderingChoice,
	limitOrdering physical.OrderingChoice,
) *physical.OrderingChoice {

	// The columns in the closure of the prefix may be included in it. It's
	// beneficial to do so for a given column iff that column appears in the
	// limit's ordering.
	cl := input.Relational().FuncDeps.ComputeClosure(prefix)
	cl.IntersectionWith(limitOrdering.ColSet())
	cl.UnionWith(prefix)
	prefix = cl

	oc, ok := limitOrdering.PrefixIntersection(prefix, ordering.Columns)
	if !ok {
		return nil
	}
	return &oc
}

// AllArePrefixSafe returns whether every window function in the list satisfies
// the "prefix-safe" property.
//
// Being prefix-safe means that the computation of a window function on a given
// row does not depend on any of the rows that come after it. It's also
// precisely the property that lets us push limit operators below window
// functions:
//
//	(Limit (Window $input) n) = (Window (Limit $input n))
//
// Note that the frame affects whether a given window function is prefix-safe or not.
// rank() is prefix-safe under any frame, but avg():
//   - is not prefix-safe under RANGE BETWEEN UNBOUNDED PRECEDING TO CURRENT ROW
//     (the default), because we might cut off mid-peer group. If we can
//     guarantee that the ordering is over a key, then this becomes safe.
//   - is not prefix-safe under ROWS BETWEEN UNBOUNDED PRECEDING TO UNBOUNDED
//     FOLLOWING, because it needs to look at the entire partition.
//   - is prefix-safe under ROWS BETWEEN UNBOUNDED PRECEDING TO CURRENT ROW,
//     because it only needs to look at the rows up to any given row.
//
// (We don't currently handle this case).
//
// This function is best-effort. It's OK to report a function not as
// prefix-safe, even if it is.
func (c *CustomFuncs) AllArePrefixSafe(fns memo.WindowsExpr) bool {
	for i := range fns {
		if !c.isPrefixSafe(&fns[i]) {
			return false
		}
	}
	return true
}

// isPrefixSafe returns whether or not the given window function satisfies the
// "prefix-safe" property. See the comment above AllArePrefixSafe for more
// details.
func (c *CustomFuncs) isPrefixSafe(fn *memo.WindowsItem) bool {
	switch fn.Function.Op() {
	case opt.RankOp, opt.RowNumberOp, opt.DenseRankOp:
		return true
	}
	// TODO(justin): Add other cases. I think aggregates are valid here if the
	// upper bound is CURRENT ROW, and either:
	// * the mode is ROWS, or
	// * the mode is RANGE and the ordering is over a key.
	return false
}

// RemoveWindowPartitionCols returns a new window private struct with the given
// columns removed from the window partition column set.
func (c *CustomFuncs) RemoveWindowPartitionCols(
	private *memo.WindowPrivate, cols opt.ColSet,
) *memo.WindowPrivate {
	p := *private
	p.Partition = p.Partition.Difference(cols)
	return &p
}

// WindowPartition returns the set of columns that the window function uses to
// partition.
func (c *CustomFuncs) WindowPartition(priv *memo.WindowPrivate) opt.ColSet {
	return priv.Partition
}

// WindowOrdering returns the ordering used by the window function.
func (c *CustomFuncs) WindowOrdering(private *memo.WindowPrivate) physical.OrderingChoice {
	return private.Ordering
}

// ExtractDeterminedConditions returns a new list of filters containing only
// those expressions from the given list which are bound by columns which
// are functionally determined by the given columns.
func (c *CustomFuncs) ExtractDeterminedConditions(
	filters memo.FiltersExpr, cols opt.ColSet, input memo.RelExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if c.IsDeterminedBy(&filters[i], cols, input) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// ExtractUndeterminedConditions is the opposite of
// ExtractDeterminedConditions.
func (c *CustomFuncs) ExtractUndeterminedConditions(
	filters memo.FiltersExpr, cols opt.ColSet, input memo.RelExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		if !c.IsDeterminedBy(&filters[i], cols, input) {
			newFilters = append(newFilters, filters[i])
		}
	}
	return newFilters
}

// OrderingSucceeded returns true if an OrderingChoice is not nil.
func (c *CustomFuncs) OrderingSucceeded(result *physical.OrderingChoice) bool {
	return result != nil
}

// DerefOrderingChoice returns an OrderingChoice from a pointer.
func (c *CustomFuncs) DerefOrderingChoice(result *physical.OrderingChoice) physical.OrderingChoice {
	return *result
}

// HasRangeFrameWithOffset returns true if w contains a WindowsItem Frame that
// has a mode of RANGE and has a specific offset, such as OffsetPreceding or
// OffsetFollowing.
func (c *CustomFuncs) HasRangeFrameWithOffset(w memo.WindowsExpr) bool {
	for i := range w {
		if w[i].Frame.Mode == tree.RANGE && w[i].Frame.HasOffset() {
			return true
		}
	}
	return false
}
