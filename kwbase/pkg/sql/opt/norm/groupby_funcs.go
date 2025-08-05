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

package norm

import (
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RemoveGroupingCols returns a new grouping private struct with the given
// columns removed from the grouping column set.
func (c *CustomFuncs) RemoveGroupingCols(
	private *memo.GroupingPrivate, cols opt.ColSet,
) *memo.GroupingPrivate {
	p := *private
	p.GroupingCols = private.GroupingCols.Difference(cols)
	return &p
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.AggregationsExpr, aggOp opt.Operator, cols opt.ColSet,
) memo.AggregationsExpr {
	outAggs := make(memo.AggregationsExpr, len(aggs)+cols.Len())
	copy(outAggs, aggs)
	c.makeAggCols(aggOp, cols, outAggs[len(aggs):])
	return outAggs
}

// AppendAggCols2 constructs a new Aggregations operator containing the
// aggregate functions from an existing Aggregations operator plus an
// additional set of aggregate functions, one for each column in the given set.
// The new functions are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols2(
	aggs memo.AggregationsExpr,
	aggOp opt.Operator,
	cols opt.ColSet,
	aggOp2 opt.Operator,
	cols2 opt.ColSet,
) memo.AggregationsExpr {
	colsLen := cols.Len()
	outAggs := make(memo.AggregationsExpr, len(aggs)+colsLen+cols2.Len())
	copy(outAggs, aggs)

	offset := len(aggs)
	c.makeAggCols(aggOp, cols, outAggs[offset:])
	offset += colsLen
	c.makeAggCols(aggOp2, cols2, outAggs[offset:])

	return outAggs
}

// makeAggCols is a helper method that constructs a new aggregate function of
// the given operator type for each column in the given set. The resulting
// aggregates are written into outElems and outColList. As an example, for
// columns (1,2) and operator ConstAggOp, makeAggCols will set the following:
//
//	outElems[0] = (ConstAggOp (Variable 1))
//	outElems[1] = (ConstAggOp (Variable 2))
//
//	outColList[0] = 1
//	outColList[1] = 2
func (c *CustomFuncs) makeAggCols(
	aggOp opt.Operator, cols opt.ColSet, outAggs memo.AggregationsExpr,
) {
	// Append aggregate functions wrapping a Variable reference to each column.
	i := 0
	for id, ok := cols.Next(0); ok; id, ok = cols.Next(id + 1) {
		varExpr := c.f.ConstructVariable(id)

		var outAgg opt.ScalarExpr
		switch aggOp {
		case opt.ConstAggOp:
			outAgg = c.f.ConstructConstAgg(varExpr)

		case opt.AnyNotNullAggOp:
			outAgg = c.f.ConstructAnyNotNullAgg(varExpr)

		case opt.FirstAggOp:
			outAgg = c.f.ConstructFirstAgg(varExpr)

		default:
			panic(errors.AssertionFailedf("unrecognized aggregate operator type: %v", log.Safe(aggOp)))
		}

		outAggs[i] = c.f.ConstructAggregationsItem(outAgg, id)
		i++
	}
}

// CanRemoveAggDistinctForKeys returns true if the given aggregate function
// where its input column, together with the grouping columns, form a key. In
// this case, the wrapper AggDistinct can be removed.
func (c *CustomFuncs) CanRemoveAggDistinctForKeys(
	input memo.RelExpr, private *memo.GroupingPrivate, agg opt.ScalarExpr,
) bool {
	if agg.ChildCount() == 0 {
		return false
	}
	inputFDs := &input.Relational().FuncDeps
	variable := agg.Child(0).(*memo.VariableExpr)
	cols := c.AddColToSet(private.GroupingCols, variable.Col)
	return inputFDs.ColsAreStrictKey(cols)
}

// ReplaceAggregationsItem returns a new list that is a copy of the given list,
// except that the given search item has been replaced by the given replace
// item. If the list contains the search item multiple times, then only the
// first instance is replaced. If the list does not contain the item, then the
// method panics.
func (c *CustomFuncs) ReplaceAggregationsItem(
	aggs memo.AggregationsExpr, search *memo.AggregationsItem, replace opt.ScalarExpr,
) memo.AggregationsExpr {
	newAggs := make([]memo.AggregationsItem, len(aggs))
	for i := range aggs {
		if search == &aggs[i] {
			copy(newAggs, aggs[:i])
			newAggs[i] = c.f.ConstructAggregationsItem(replace, search.Col)
			copy(newAggs[i+1:], aggs[i+1:])
			return newAggs
		}
	}
	panic(errors.AssertionFailedf("item to replace is not in the list: %v", search))
}

// HasNoGroupingCols returns true if the GroupingCols in the private are empty.
func (c *CustomFuncs) HasNoGroupingCols(private *memo.GroupingPrivate) bool {
	return private.GroupingCols.Empty()
}

// GroupingInputOrdering returns the Ordering in the private.
func (c *CustomFuncs) GroupingInputOrdering(private *memo.GroupingPrivate) physical.OrderingChoice {
	return private.Ordering
}

// ConstructProjectionFromDistinctOn converts a DistinctOn to a projection; this
// is correct when input groupings have at most one row (i.e. the input is
// already distinct). Note that DistinctOn can only have aggregations of type
// FirstAgg or ConstAgg.
func (c *CustomFuncs) ConstructProjectionFromDistinctOn(
	input memo.RelExpr, groupingCols opt.ColSet, aggs memo.AggregationsExpr,
) memo.RelExpr {
	// Always pass through grouping columns.
	passthrough := groupingCols.Copy()

	var projections memo.ProjectionsExpr
	for i := range aggs {
		varExpr := memo.ExtractAggFirstVar(aggs[i].Agg)
		inputCol := varExpr.Col
		outputCol := aggs[i].Col
		if inputCol == outputCol {
			passthrough.Add(inputCol)
		} else {
			projections = append(projections, c.f.ConstructProjectionsItem(varExpr, aggs[i].Col))
		}
	}
	return c.f.ConstructProject(input, projections, passthrough)
}

// DuplicateUpsertErrText returns the error text used when duplicate input rows
// to the Upsert operator are detected.
func (c *CustomFuncs) DuplicateUpsertErrText() string {
	return sqlbase.DuplicateUpsertErrText
}

// AreValuesDistinct returns true if a constant Values operator input contains
// only rows that are already distinct with respect to the given grouping
// columns. The Values operator can be wrapped by Select, Project, and/or
// LeftJoin operators.
//
// If nullsAreDistinct is true, then NULL values are treated as not equal to one
// another, and therefore rows containing a NULL value in any grouping column
// are always distinct.
func (c *CustomFuncs) AreValuesDistinct(
	input memo.RelExpr, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	switch t := input.(type) {
	case *memo.ValuesExpr:
		return c.areRowsDistinct(t.Rows, t.Cols, groupingCols, nullsAreDistinct)

	case *memo.SelectExpr:
		return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)

	case *memo.ProjectExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}

	case *memo.LeftJoinExpr:
		// Pass through call to left input if grouping on its columns. Also,
		// ensure that the left join does not cause duplication of left rows.
		leftCols := t.Left.Relational().OutputCols
		rightCols := t.Right.Relational().OutputCols
		if !groupingCols.SubsetOf(leftCols) {
			break
		}

		// If any set of key columns (lax or strict) from the right input are
		// equality joined to columns in the left input, then the left join will
		// never cause duplication of left rows.
		var eqCols opt.ColSet
		for i := range t.On {
			condition := t.On[i].Condition
			ok, _, rightColID := memo.ExtractJoinEquality(leftCols, rightCols, condition)
			if ok {
				eqCols.Add(rightColID)
			}
		}
		if !t.Right.Relational().FuncDeps.ColsAreLaxKey(eqCols) {
			// Not joining on a right input key.
			break
		}

		return c.AreValuesDistinct(t.Left, groupingCols, nullsAreDistinct)

	case *memo.UpsertDistinctOnExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}
	}
	return false
}

// areRowsDistinct returns true if the given rows are unique on the given
// grouping columns. If nullsAreDistinct is true, then NULL values are treated
// as unique, and therefore a row containing a NULL value in any grouping column
// is always distinct from every other row.
func (c *CustomFuncs) areRowsDistinct(
	rows memo.ScalarListExpr, cols opt.ColList, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	var seen map[string]bool
	var encoded []byte
	for _, scalar := range rows {
		row := scalar.(*memo.TupleExpr)

		// Reset scratch bytes.
		encoded = encoded[:0]

		forceDistinct := false
		for i, colID := range cols {
			if !groupingCols.Contains(colID) {
				// This is not a grouping column, so ignore.
				continue
			}

			// Try to extract constant value from column. Call IsConstValueOp first,
			// since this code doesn't handle the tuples and arrays that
			// ExtractConstDatum can return.
			col := row.Elems[i]
			if !opt.IsConstValueOp(col) {
				// At least one grouping column in at least one row is not constant,
				// so can't determine whether the rows are distinct.
				return false
			}
			datum := memo.ExtractConstDatum(col)

			// If this is an UpsertDistinctOn operator, then treat NULL values as
			// always distinct.
			if nullsAreDistinct && datum == tree.DNull {
				forceDistinct = true
				break
			}

			// Encode the datum using the key encoding format. The encodings for
			// multiple column datums are simply appended to one another.
			var err error
			encoded, err = sqlbase.EncodeTableKey(encoded, datum, encoding.Ascending)
			if err != nil {
				// Assume rows are not distinct if an encoding error occurs.
				return false
			}
		}

		if seen == nil {
			seen = make(map[string]bool, len(rows))
		}

		// Determine whether key has already been seen.
		key := string(encoded)
		if _, ok := seen[key]; ok && !forceDistinct {
			// Found duplicate.
			return false
		}

		// Add the key to the seen map.
		seen[key] = true
	}

	return true
}

// HasOnlyTagColumn check has only tag column from aggs and grouping colset
func (c *CustomFuncs) HasOnlyTagColumn(
	tsScan *memo.TSScanPrivate, aggs memo.AggregationsExpr, private *memo.GroupingPrivate,
) bool {
	if tsScan.HintType.OnlyTag() {
		return false
	}

	for _, agg := range aggs {
		switch agg.Agg.(type) {
		case *memo.CountRowsExpr:
			return false
		}
	}

	meta := c.mem.Metadata()

	allTag := true
	tsScan.Cols.ForEach(func(col opt.ColumnID) {
		if !meta.ColumnMeta(col).IsTag() {
			allTag = false
		}
	})

	if !allTag {
		return false
	}

	// check primary tag
	primaryTagCount := 0
	tableMeta := meta.TableMeta(tsScan.Table)
	private.GroupingCols.ForEach(func(col opt.ColumnID) {
		if meta.ColumnMeta(col).IsPrimaryTag() {
			primaryTagCount++
		}
	})

	return allTag && primaryTagCount == tableMeta.PrimaryTagCount
}

func onlyTagPrivateTSScan(tsScan *memo.TSScanExpr) *memo.TSScanPrivate {
	return &memo.TSScanPrivate{
		Table:            tsScan.Table,
		Cols:             tsScan.Cols,
		AccessMode:       tsScan.AccessMode,
		ScanAggs:         tsScan.ScanAggs,
		TagFilter:        tsScan.TagFilter,
		PrimaryTagFilter: tsScan.PrimaryTagFilter,
		PrimaryTagValues: tsScan.PrimaryTagValues,
		HintType:         keys.TagOnlyHint,
	}
}

// ChangeTSTableScanType change ts table scan mode
func (c *CustomFuncs) ChangeTSTableScanType(tsScan *memo.TSScanPrivate) memo.RelExpr {
	return c.f.ConstructTSScan(&memo.TSScanPrivate{
		Table:            tsScan.Table,
		Cols:             tsScan.Cols,
		AccessMode:       tsScan.AccessMode,
		ScanAggs:         tsScan.ScanAggs,
		TagFilter:        tsScan.TagFilter,
		PrimaryTagFilter: tsScan.PrimaryTagFilter,
		PrimaryTagValues: tsScan.PrimaryTagValues,
		HintType:         keys.TagOnlyHint,
	})
}

// ChangeTSTableScanTypeForSelect change ts table scan mode
func (c *CustomFuncs) ChangeTSTableScanTypeForSelect(selectExpr memo.RelExpr) memo.RelExpr {
	expr, ok := selectExpr.(*memo.SelectExpr)
	if !ok {
		return selectExpr
	}

	tsScan, ok1 := expr.Input.(*memo.TSScanExpr)
	if !ok1 {
		return selectExpr
	}

	newScan := c.f.ConstructTSScan(onlyTagPrivateTSScan(tsScan))

	return c.f.ConstructSelect(newScan, expr.Filters)
}

// ChangeTSTableScanTypeForProject change ts table scan mode
func (c *CustomFuncs) ChangeTSTableScanTypeForProject(projectExpr memo.RelExpr) memo.RelExpr {
	expr, ok := projectExpr.(*memo.ProjectExpr)
	if !ok {
		return projectExpr
	}

	switch t := expr.Input.(type) {
	case *memo.TSScanExpr:
		newScan := c.f.ConstructTSScan(onlyTagPrivateTSScan(t))
		return c.f.ConstructProject(newScan, expr.Projections, expr.Passthrough)
	case *memo.SelectExpr:
		tsScan, ok1 := t.Input.(*memo.TSScanExpr)
		if !ok1 {
			return projectExpr
		}

		newScan := c.f.ConstructTSScan(onlyTagPrivateTSScan(tsScan))

		newSelect := c.f.ConstructSelect(newScan, t.Filters)

		return c.f.ConstructProject(newSelect, expr.Projections, expr.Passthrough)
	}
	return projectExpr
}

// CheckForLastRowOpt checks ts table scan for last row optimize
func (c *CustomFuncs) CheckForLastRowOpt(
	tsScan *memo.TSScanPrivate, aggs memo.AggregationsExpr, private *memo.GroupingPrivate,
) bool {
	if tsScan.HintType == keys.LastRowOptHint || tsScan.PrimaryTagFilter != nil || tsScan.TagFilter != nil ||
		!private.GroupingCols.Empty() {
		return false
	}

	for _, agg := range aggs {
		switch agg.Agg.(type) {
		case *memo.LastRowExpr:
		default:
			return false
		}
	}

	return true
}

// DealForLastRowOpt flags tsScanExpr for last row optimize
func (c *CustomFuncs) DealForLastRowOpt(tsScan *memo.TSScanPrivate) memo.RelExpr {
	return c.f.ConstructTSScan(&memo.TSScanPrivate{
		Table:            tsScan.Table,
		Cols:             tsScan.Cols,
		AccessMode:       tsScan.AccessMode,
		ScanAggs:         tsScan.ScanAggs,
		TagFilter:        tsScan.TagFilter,
		PrimaryTagFilter: tsScan.PrimaryTagFilter,
		PrimaryTagValues: tsScan.PrimaryTagValues,
		HintType:         keys.LastRowOptHint,
	})
}
