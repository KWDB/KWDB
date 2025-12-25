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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// CanMapOnSetOp determines whether the filter can be mapped to either
// side of a set operator.
func (c *CustomFuncs) CanMapOnSetOp(src *memo.FiltersItem) bool {
	filterProps := src.ScalarProps()
	for i, ok := filterProps.OuterCols.Next(0); ok; i, ok = filterProps.OuterCols.Next(i + 1) {
		colType := c.f.Metadata().ColumnMeta(i).Type
		if sqlbase.DatumTypeHasCompositeKeyEncoding(colType) {
			return false
		}
	}
	return !filterProps.HasCorrelatedSubquery
}

// MapSetOpFilterLeft maps the filter onto the left expression by replacing
// the out columns of the filter with the appropriate corresponding columns on
// the left side of the operator.
// Useful for pushing filters to relations the set operation is composed of.
func (c *CustomFuncs) MapSetOpFilterLeft(
	filter *memo.FiltersItem, set *memo.SetPrivate,
) opt.ScalarExpr {
	return c.mapSetOpFilter(filter, set.OutCols, set.LeftCols)
}

// MapSetOpFilterRight maps the filter onto the right expression by replacing
// the out columns of the filter with the appropriate corresponding columns on
// the right side of the operator.
// Useful for pushing filters to relations the set operation is composed of.
func (c *CustomFuncs) MapSetOpFilterRight(
	filter *memo.FiltersItem, set *memo.SetPrivate,
) opt.ScalarExpr {
	return c.mapSetOpFilter(filter, set.OutCols, set.RightCols)
}

// mapSetOpFilter maps filter expressions to dst by replacing occurrences of
// columns in src with corresponding columns in dst (the two lists must be of
// equal length).
//
// For each column in src that is not an outer column, SetMap replaces it with
// the corresponding column in dst.
//
// For example, consider this query:
//
//	SELECT * FROM (SELECT x FROM a UNION SELECT y FROM b) WHERE x < 5
//
// If mapSetOpFilter is called on the left subtree of the Union, the filter
// x < 5 propagates to that side after mapping the column IDs appropriately.
// WLOG, If setMap is called on the right subtree, the filter x < 5 will be
// mapped similarly to y < 5 on the right side.
func (c *CustomFuncs) mapSetOpFilter(
	filter *memo.FiltersItem, src opt.ColList, dst opt.ColList,
) opt.ScalarExpr {
	// Map each column in src to one column in dst to map the
	// filters appropriately.
	var colMap util.FastIntMap
	for colIndex, outColID := range src {
		colMap.Set(int(outColID), int(dst[colIndex]))
	}

	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced and then replace them appropriately.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.VariableExpr:
			dstCol, ok := colMap.Get(int(t.Col))
			if !ok {
				// It is not part of the output cols so no replacement required.
				return nd
			}
			return c.f.ConstructVariable(opt.ColumnID(dstCol))
		}
		return c.f.Replace(nd, replace)
	}

	return replace(filter.Condition).(opt.ScalarExpr)
}

// GroupingAndConstCols returns the grouping columns and ConstAgg columns (for
// which the input and output column IDs match). A filter on these columns can
// be pushed through a GroupBy.
func (c *CustomFuncs) GroupingAndConstCols(
	grouping *memo.GroupingPrivate, aggs memo.AggregationsExpr,
) opt.ColSet {
	result := grouping.GroupingCols.Copy()

	// Add any ConstAgg columns.
	for i := range aggs {
		item := &aggs[i]
		if constAgg, ok := item.Agg.(*memo.ConstAggExpr); ok {
			// Verify that the input and output column IDs match.
			inputVar, isVarExpr := constAgg.Input.(*memo.VariableExpr)
			if !isVarExpr {
				continue
			}
			if item.Col == inputVar.Col {
				result.Add(item.Col)
			}
		}
	}
	return result
}

// CanConsolidateFilters returns true if there are at least two different
// filter conditions that contain the same variable, where the conditions
// have tight constraints and contain a single variable. For example,
// CanConsolidateFilters returns true with filters {x > 5, x < 10}, but false
// with {x > 5, y < 10} and {x > 5, x = y}.
func (c *CustomFuncs) CanConsolidateFilters(filters memo.FiltersExpr) bool {
	var seen opt.ColSet
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok {
			if seen.Contains(col) {
				return true
			}
			seen.Add(col)
		}
	}
	return false
}

// canConsolidateFilter determines whether a filter condition can be
// consolidated. Filters can be consolidated if they have tight constraints
// and contain a single variable. Examples of such filters include x < 5 and
// x IS NULL. If the filter can be consolidated, canConsolidateFilter returns
// the column ID of the variable and ok=true. Otherwise, canConsolidateFilter
// returns ok=false.
func (c *CustomFuncs) canConsolidateFilter(filter *memo.FiltersItem) (col opt.ColumnID, ok bool) {
	if !filter.ScalarProps().TightConstraints {
		return 0, false
	}

	outerCols := c.OuterCols(filter)
	if outerCols.Len() != 1 {
		return 0, false
	}

	col, _ = outerCols.Next(0)
	return col, true
}

// ConsolidateFilters consolidates filter conditions that contain the same
// variable, where the conditions have tight constraints and contain a single
// variable. The consolidated filters are combined with a tree of nested
// And operations, and wrapped with a Range expression.
//
// See the ConsolidateSelectFilters rule for more details about why this is
// necessary.
func (c *CustomFuncs) ConsolidateFilters(filters memo.FiltersExpr) memo.FiltersExpr {
	// First find the columns that have filter conditions that can be
	// consolidated.
	var seen, seenTwice opt.ColSet
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok {
			if seen.Contains(col) {
				seenTwice.Add(col)
			} else {
				seen.Add(col)
			}
		}
	}

	newFilters := make(memo.FiltersExpr, seenTwice.Len(), len(filters)-seenTwice.Len())

	// newFilters contains an empty item for each of the new Range expressions
	// that will be created below. Fill in rangeMap to track which column
	// corresponds to each item.
	var rangeMap util.FastIntMap
	i := 0
	for col, ok := seenTwice.Next(0); ok; col, ok = seenTwice.Next(col + 1) {
		rangeMap.Set(int(col), i)
		i++
	}

	// Iterate through each existing filter condition, and either consolidate it
	// into one of the new Range expressions or add it unchanged to the new
	// filters.
	for i := range filters {
		if col, ok := c.canConsolidateFilter(&filters[i]); ok && seenTwice.Contains(col) {
			// This is one of the filter conditions that can be consolidated into a
			// Range.
			cond := filters[i].Condition
			switch t := cond.(type) {
			case *memo.RangeExpr:
				// If it is already a range expression, unwrap it.
				cond = t.And
			}
			rangeIdx, _ := rangeMap.Get(int(col))
			rangeItem := &newFilters[rangeIdx]
			if rangeItem.Condition == nil {
				// This is the first condition.
				rangeItem.Condition = cond
			} else {
				// Build a left-deep tree of ANDs sorted by ID.
				rangeItem.Condition = c.mergeSortedAnds(rangeItem.Condition, cond)
			}
		} else {
			newFilters = append(newFilters, filters[i])
		}
	}

	// Construct each of the new Range operators now that we have built the
	// conjunctions.
	for i, n := 0, seenTwice.Len(); i < n; i++ {
		newFilters[i] = c.f.ConstructFiltersItem(c.f.ConstructRange(newFilters[i].Condition))
	}

	return newFilters
}

// mergeSortedAnds merges two left-deep trees of nested AndExprs sorted by ID.
// Returns a single sorted, left-deep tree of nested AndExprs, with any
// duplicate expressions eliminated.
func (c *CustomFuncs) mergeSortedAnds(left, right opt.ScalarExpr) opt.ScalarExpr {
	if right == nil {
		return left
	}
	if left == nil {
		return right
	}

	// Since both trees are left-deep, perform a merge-sort from right to left.
	nextLeft := left
	nextRight := right
	var remainingLeft, remainingRight opt.ScalarExpr
	if and, ok := left.(*memo.AndExpr); ok {
		remainingLeft = and.Left
		nextLeft = and.Right
	}
	if and, ok := right.(*memo.AndExpr); ok {
		remainingRight = and.Left
		nextRight = and.Right
	}

	if nextLeft.ID() == nextRight.ID() {
		// Eliminate duplicates.
		return c.mergeSortedAnds(left, remainingRight)
	}
	if nextLeft.ID() < nextRight.ID() {
		return c.f.ConstructAnd(c.mergeSortedAnds(left, remainingRight), nextRight)
	}
	return c.f.ConstructAnd(c.mergeSortedAnds(remainingLeft, right), nextLeft)
}

// AreFiltersSorted determines whether the expressions in a FiltersExpr are
// ordered by their expression IDs.
func (c *CustomFuncs) AreFiltersSorted(f memo.FiltersExpr) bool {
	for i, n := 0, f.ChildCount(); i < n-1; i++ {
		if f.Child(i).Child(0).(opt.ScalarExpr).ID() > f.Child(i+1).Child(0).(opt.ScalarExpr).ID() {
			return false
		}
	}
	return true
}

// SortFilters sorts a filter list by the IDs of the expressions. This has the
// effect of canonicalizing FiltersExprs which may have the same filters, but
// in a different order.
func (c *CustomFuncs) SortFilters(f memo.FiltersExpr) memo.FiltersExpr {
	result := make(memo.FiltersExpr, len(f))
	for i, n := 0, f.ChildCount(); i < n; i++ {
		fi := f.Child(i).(*memo.FiltersItem)
		result[i] = *fi
	}
	result.Sort()
	return result
}

// SimplifyFilters removes True operands from a FiltersExpr, and normalizes any
// False or Null condition to a single False condition. Null values map to False
// because FiltersExpr are only used by Select and Join, both of which treat a
// Null filter conjunct exactly as if it were false.
//
// SimplifyFilters also "flattens" any And operator child by merging its
// conditions into a new FiltersExpr list. If, after simplification, no operands
// remain, then SimplifyFilters returns an empty FiltersExpr.
//
// This method assumes that the NormalizeNestedAnds rule has already run and
// ensured a left deep And tree. If not (maybe because it's a testing scenario),
// then this rule may rematch, but it should still make forward progress).
func (c *CustomFuncs) SimplifyFilters(filters memo.FiltersExpr) memo.FiltersExpr {
	// Start by counting the number of conjuncts that will be flattened so that
	// the capacity of the FiltersExpr list can be determined.
	cnt := 0
	for _, item := range filters {
		cnt++
		condition := item.Condition
		for condition.Op() == opt.AndOp {
			cnt++
			condition = condition.(*memo.AndExpr).Left
		}
	}

	// Construct new filter list.
	newFilters := make(memo.FiltersExpr, 0, cnt)
	for _, item := range filters {
		var ok bool
		if newFilters, ok = c.addConjuncts(item.Condition, newFilters); !ok {
			return memo.FiltersExpr{c.f.ConstructFiltersItem(memo.FalseSingleton)}
		}
	}

	return newFilters
}

// addConjuncts recursively walks a scalar expression as long as it continues to
// find nested And operators. It adds any conjuncts (ignoring True operators) to
// the given FiltersExpr and returns true. If it finds a False or Null operator,
// it propagates a false return value all the up the call stack, and
// SimplifyFilters maps that to a FiltersExpr that is always false.
func (c *CustomFuncs) addConjuncts(
	scalar opt.ScalarExpr, filters memo.FiltersExpr,
) (_ memo.FiltersExpr, ok bool) {
	switch t := scalar.(type) {
	case *memo.AndExpr:
		var ok bool
		if filters, ok = c.addConjuncts(t.Left, filters); !ok {
			return nil, false
		}
		return c.addConjuncts(t.Right, filters)

	case *memo.FalseExpr, *memo.NullExpr:
		// Filters expression evaluates to False if any operand is False or Null.
		return nil, false

	case *memo.TrueExpr:
		// Filters operator skips True operands.

	default:
		filters = append(filters, c.f.ConstructFiltersItem(t))
	}
	return filters, true
}
