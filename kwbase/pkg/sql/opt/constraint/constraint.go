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

package constraint

import (
	"math"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Constraint specifies the possible set of values that one or more columns
// will have in the result set. If this is a single column constraint, then
// that column's value will always be part of one of the spans in this
// constraint. If this is a multi-column constraint, then the combination of
// column values will always be part of one of the spans.
//
// Constrained columns are specified as an ordered list, and span key values
// correspond to those columns by position. Constraints are inferred from
// scalar filter conditions, since these restrict the set of possible results.
// Constraints over different combinations of columns can be combined together
// in a constraint set, which is a conjunction of constraints. See the
// Set struct comment for more details.
//
// A few examples:
//   - a constraint on @1 > 1: a single span             /@1: (/1 - ]
//   - a constraint on @1 = 1 AND @2 >= 1: a single span /@1/@2: [/1/1 - /1]
//   - a constraint on @1 < 5 OR @1 > 10: multiple spans /@1: [ - /5) (10 - ]
type Constraint struct {
	Columns Columns

	// Spans contains the spans in this constraint. The spans are always ordered
	// and non-overlapping.
	Spans Spans

	// Filters represents the Filter condition corresponding to Constraint
	Filters FiltersExpr
}

// FiltersExpr represents the FiltersItem array
type FiltersExpr []FiltersItem

// FiltersItem stores auxiliary information related to a predicate, which is used to construct the predicate
// attribute in Workloadinfo
type FiltersItem struct {
	Condition opt.ScalarExpr
	Stats     opt.PredicateStats
	ColumnID  opt.ColumnID
}

// Init initializes the constraint to the columns in the key context and with
// the given spans.
func (c *Constraint) Init(keyCtx *KeyContext, spans *Spans) {
	c.validateSpansOrdered(keyCtx, spans)
	c.Columns = keyCtx.Columns
	c.Spans = *spans
	c.Spans.makeImmutable()
}

// InitSingleSpan initializes the constraint to the columns in the key context
// and with one span.
func (c *Constraint) InitSingleSpan(keyCtx *KeyContext, span *Span) {
	c.Columns = keyCtx.Columns
	c.Spans.InitSingleSpan(span)
}

// IsContradiction returns true if there are no spans in the constraint.
func (c *Constraint) IsContradiction() bool {
	return c.Spans.Count() == 0
}

// IsUnconstrained returns true if the constraint contains an unconstrained
// span.
func (c *Constraint) IsUnconstrained() bool {
	return c.Spans.Count() == 1 && c.Spans.Get(0).IsUnconstrained()
}

// UnionWith merges the spans of the given constraint into this constraint.  The
// columns of both constraints must be the same. Constrained columns in the
// merged constraint can have values that are part of either of the input
// constraints.
func (c *Constraint) UnionWith(evalCtx *tree.EvalContext, other *Constraint) {
	c.assertColumnsMatch(other)
	if c.IsUnconstrained() || other.IsContradiction() {
		return
	}

	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	result := mergeSpansForUnion(keyCtx, c.Spans, other.Spans)

	c.Spans = result
	c.Spans.makeImmutable()
}

// IntersectWith intersects the spans of this constraint with those in the
// given constraint and updates this constraint with any overlapping spans. The
// columns of both constraints must be the same. If there are no overlapping
// spans, then the intersection is empty, and tryIntersectWith returns false.
// If a constraint set has even one empty constraint, then the entire set
// should be marked as empty and all constraints removed.
func (c *Constraint) IntersectWith(evalCtx *tree.EvalContext, other *Constraint) {
	c.assertColumnsMatch(other)
	if c.IsContradiction() || other.IsUnconstrained() {
		return
	}

	// Use variation on merge sort, because both sets of spans are ordered and
	// non-overlapping.
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	result := intersectSpansMerge(keyCtx, c.Spans, other.Spans)

	c.Spans = result
	c.Spans.makeImmutable()
}

func (c Constraint) String() string {
	var b strings.Builder
	b.WriteString(c.Columns.String())
	b.WriteString(": ")
	b.WriteString(c.describeConstraintKind())
	return b.String()
}

// ContainsSpan returns true if the constraint contains the given span (or a
// span that contains it). Uses binary search over ordered spans.
func (c *Constraint) ContainsSpan(evalCtx *tree.EvalContext, sp *Span) bool {
	keyCtx := MakeKeyContext(&c.Columns, evalCtx)
	return c.binarySearchContains(&keyCtx, sp)
}

// Combine refines the receiver constraint using constraints on a suffix of the
// same list of columns. For example:
//
//	c:      /a/b: [/1 - /2] [/4 - /4]
//	other:  /b: [/5 - /5]
//	result: /a/b: [/1/5 - /2/5] [/4/5 - /4/5]
func (c *Constraint) Combine(evalCtx *tree.EvalContext, other *Constraint) {
	if !other.Columns.IsStrictSuffixOf(&c.Columns) {
		// Note: we don't want to let the c and other pointers escape by passing
		// them directly to Sprintf.
		panic(errors.AssertionFailedf("%s not a suffix of %s", other.String(), c.String()))
	}
	if c.IsUnconstrained() || c.IsContradiction() || other.IsUnconstrained() {
		return
	}
	if other.IsContradiction() {
		c.setContradiction()
		return
	}
	offset := c.Columns.Count() - other.Columns.Count()

	keyCtx := KeyContext{Columns: c.Columns, EvalCtx: evalCtx}
	result, ok := c.combineSpansWithSuffix(keyCtx, other, offset)
	if ok {
		c.Spans = result
		c.Spans.makeImmutable()
	}
}

// ConsolidateSpans merges spans that have consecutive boundaries. For example:
//
//	[/1 - /2] [/3 - /4] becomes [/1 - /4].
func (c *Constraint) ConsolidateSpans(evalCtx *tree.EvalContext) {
	keyCtx := KeyContext{Columns: c.Columns, EvalCtx: evalCtx}
	result := c.consolidateConsecutiveSpans(&keyCtx)
	if result.Count() != 0 {
		c.Spans = result
		c.Spans.makeImmutable()
	}
}

// ExactPrefix returns the length of the longest column prefix which are
// constrained to a single value. For example:
//
//	/a/b/c: [/1/2/3 - /1/2/3]                    ->  ExactPrefix = 3
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]  ->  ExactPrefix = 2
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/3/8]  ->  ExactPrefix = 1
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3]  ->  ExactPrefix = 1
//	/a/b/c: [/1/2/3 - /1/2/3] [/3 - /4]          ->  ExactPrefix = 0
func (c *Constraint) ExactPrefix(evalCtx *tree.EvalContext) int {
	if c.IsContradiction() {
		return 0
	}

	for col := 0; ; col++ {
		if !c.allSpansHaveSameValueAtColumn(evalCtx, col) {
			return col
		}
	}
}

// ConstrainedColumns returns the number of columns which are constrained by
// the Constraint. For example:
//
//	/a/b/c: [/1/1 - /1] [/3 - /3]
//
// has 2 constrained columns. This may be less than the total number of columns
// in the constraint, especially if it represents an index constraint.
func (c *Constraint) ConstrainedColumns(evalCtx *tree.EvalContext) int {
	return c.maxKeyDepth()
}

// Prefix returns the length of the longest prefix of columns for which all the
// spans have the same start and end values. For example:
//
//	/a/b/c: [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4]
//
// has prefix 2.
//
// Note that Prefix returns a value that is greater than or equal to the value
// returned by ExactPrefix. For example:
//
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/3/8] -> ExactPrefix = 1, Prefix = 1
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/3/3 - /1/3/3] -> ExactPrefix = 1, Prefix = 3
func (c *Constraint) Prefix(evalCtx *tree.EvalContext) int {
	return c.findUniformSpanPrefix(evalCtx)
}

// ExtractConstCols returns a set of columns which are restricted to be
// constant by the constraint.
func (c *Constraint) ExtractConstCols(evalCtx *tree.EvalContext) opt.ColSet {
	var res opt.ColSet
	pre := c.ExactPrefix(evalCtx)
	for i := 0; i < pre; i++ {
		res.Add(c.Columns.Get(i).ID())
	}
	return res
}

// ExtractNotNullCols returns a set of columns that cannot be NULL when the
// constraint holds.
func (c *Constraint) ExtractNotNullCols(evalCtx *tree.EvalContext) opt.ColSet {
	if c.IsUnconstrained() || c.IsContradiction() {
		return opt.ColSet{}
	}

	var res opt.ColSet

	// If we have a span where the start and end key value diverge for a column,
	// none of the columns that follow can be not-null. For example:
	//   /1/2/3: [/1/2/3 - /1/4/1]
	// Because the span is not restricted to a single value on column 2, column 3
	// can take any value, like /1/3/NULL.
	//
	// Find the longest prefix of columns for which all the spans have the same
	// start and end values. For example:
	//   [/1/1/1 - /1/1/2] [/3/3/3 - /3/3/4]
	// has prefix 2. Only these columns and the first following column can be
	// known to be not-null.
	prefix := c.Prefix(evalCtx)
	c.addNonNullPrefixCols(&res, evalCtx, prefix)
	if prefix == c.Columns.Count() {
		return res
	}

	// Now look at the first column that follows the prefix.
	if c.isFirstColBeyondPrefixNonNull(evalCtx, prefix) {
		col := c.Columns.Get(prefix)
		res.Add(col.ID())
	}
	return res
}

// CalculateMaxResults returns a non-zero integer indicating the maximum number
// of results that can be read from indexCols by using c.Spans. The indexCols
// are assumed to form at least a weak key.
// If 0 is returned, the maximum number of results could not be deduced.
// We can calculate the maximum number of results when both of the following
// are satisfied:
//  1. The index columns form a weak key (assumption), and the spans do not
//     specify any nulls.
//  2. All spans cover all the columns of the index and have equal start and
//     end keys up to but not necessarily including the last column.
//
// TODO(asubiotto): The only reason to extract this is that both the heuristic
// planner and optimizer need this logic, due to the heuristic planner planning
// mutations. Once the optimizer plans mutations, this method can go away.
func (c *Constraint) CalculateMaxResults(
	evalCtx *tree.EvalContext, indexCols opt.ColSet, notNullCols opt.ColSet,
) uint64 {
	// Ensure that if we have nullable columns, we are only reading non-null
	// values, given that a unique index allows an arbitrary number of duplicate
	// entries if they have NULLs.
	if !c.allColsAreNonNull(indexCols, notNullCols, evalCtx) {
		return 0
	}

	numCols := c.Columns.Count()

	// Check if the longest prefix of columns for which all the spans have the
	// same start and end values covers all columns.
	prefix := c.Prefix(evalCtx)
	return c.computeDistinctValCount(evalCtx, numCols, prefix)
}

// TransformSpansToTsSpans convert spans in constraint to TsSpans.
func (c *Constraint) TransformSpansToTsSpans(precision int32) []execinfrapb.TsSpan {
	prec := getPrecisionFactor(precision)

	var tsSpans []execinfrapb.TsSpan
	var s execinfrapb.TsSpan

	first := c.Spans.firstSpan
	others := c.Spans.otherSpans

	s.FromTimeStamp, s.ToTimeStamp = convertSpanToTimestamp(first.start, bool(first.startBoundary), first.end, bool(first.endBoundary), prec)
	if s.FromTimeStamp != math.MinInt64 || s.ToTimeStamp != math.MaxInt64 {
		tsSpans = append(tsSpans, s)
	}

	for i := range others {
		s.FromTimeStamp, s.ToTimeStamp = convertSpanToTimestamp(others[i].start, bool(others[i].startBoundary), others[i].end, bool(others[i].endBoundary), prec)

		if s.FromTimeStamp != math.MinInt64 || s.ToTimeStamp != math.MaxInt64 {
			tsSpans = append(tsSpans, s)
		}
	}

	return tsSpans
}

// TransformSpansToOsnSpans convert spans in constraint to osnSpans.
func (c *Constraint) TransformSpansToOsnSpans() []execinfrapb.OsnSpan {
	var osnSpans []execinfrapb.OsnSpan
	var s execinfrapb.OsnSpan

	assign := func(start Key, startBoundary bool, end Key, endBoundary bool) (uint64, uint64) {
		var startNew uint64
		var endNew uint64
		if start.firstVal != nil {
			if t, ok := start.firstVal.(*tree.DInt); ok {
				startNew = uint64(*t)
			}
			if startBoundary {
				startNew++
			}
		}

		if end.firstVal != nil {
			if t, ok := end.firstVal.(*tree.DInt); ok {
				endNew = uint64(*t)
			}
			if endBoundary {
				endNew--
			}
		}
		if start.firstVal == nil {
			startNew = 0
		}
		if end.firstVal == nil {
			endNew = math.MaxUint64
		}
		return startNew, endNew
	}

	first := c.Spans.firstSpan
	others := c.Spans.otherSpans

	s.FromTimeStamp, s.ToTimeStamp = assign(first.start, bool(first.startBoundary), first.end, bool(first.endBoundary))
	if s.FromTimeStamp != 0 || s.ToTimeStamp != math.MaxUint64 {
		osnSpans = append(osnSpans, s)
	}

	for i := range others {
		s.FromTimeStamp, s.ToTimeStamp = assign(others[i].start, bool(others[i].startBoundary), others[i].end, bool(others[i].endBoundary))

		if s.FromTimeStamp != 0 || s.ToTimeStamp != math.MaxUint64 {
			osnSpans = append(osnSpans, s)
		}
	}

	return osnSpans
}

// assignPrecision converts the datum type time to an integer according to precision
//
// in parameter:
//
//	start            - the start value of the interval for filtering time
//	startBoundary		 - startBoundary indicates whether the span contains the start key value
//	end              - the end value of the interval for filtering time
//	endBoundary      - endBoundary indicates whether the span contains the end key value
//	precision        - precision represents time precision
//
// out parameter:
//
//	 startNew         - startNew represents the integer corresponding to the start time,
//											 and its units depend on precision
//	 endNew           - endNew represents the integer corresponding to the end time,
//	                    and its units depend on precision
// func assignPrecision(  //TODO: not sure if we should keep this version of assignPrecision
// 	start Key, startBoundary bool, end Key, endBoundary bool, precision int64,
// ) (startNew int64, endNew int64) {
// 	if start.firstVal != nil {
// 		if t, ok := start.firstVal.(*tree.DTimestampTZ); ok {
// 			nanosecond := t.Time.Nanosecond()
// 			second := t.Time.Unix()
// 			if second < math.MinInt64/precision {
// 				startNew = math.MinInt64
// 			} else {
// 				startNew = second*precision + int64(nanosecond)/(1e9/precision)
// 				if startBoundary {
// 					startNew++
// 				}
// 			}
// 		}
// 	}
// }

// assertColumnsMatch panics if the two constraints have different columns.
func (c *Constraint) assertColumnsMatch(other *Constraint) {
	if !c.Columns.Equals(&other.Columns) {
		panic(errors.AssertionFailedf("column mismatch"))
	}
}

// validateSpansOrdered panics if the spans are not ordered and non-overlapping.
func (c *Constraint) validateSpansOrdered(keyCtx *KeyContext, spans *Spans) {
	for i := 1; i < spans.Count(); i++ {
		if !spans.Get(i).StartsStrictlyAfter(keyCtx, spans.Get(i-1)) {
			panic(errors.AssertionFailedf("spans must be ordered and non-overlapping"))
		}
	}
}

// setContradiction clears all spans, making the constraint represent a
// contradictory condition (no possible values).
func (c *Constraint) setContradiction() {
	c.Spans = Spans{}
	c.Spans.makeImmutable()
}

// describeConstraintKind returns "unconstrained", "contradiction", or the
// span list string depending on the constraint's state.
func (c Constraint) describeConstraintKind() string {
	if c.IsUnconstrained() {
		return "unconstrained"
	} else if c.IsContradiction() {
		return "contradiction"
	}
	return c.Spans.String()
}

// binarySearchContains uses binary search over sorted spans to find one that
// contains the given span.
func (c *Constraint) binarySearchContains(keyCtx *KeyContext, sp *Span) bool {
	// Binary search to find an overlapping span.
	for l, r := 0, c.Spans.Count()-1; l <= r; {
		m := (l + r) / 2
		cSpan := c.Spans.Get(m)
		if sp.StartsAfter(keyCtx, cSpan) {
			l = m + 1
		} else if cSpan.StartsAfter(keyCtx, sp) {
			r = m - 1
		} else {
			// The spans must overlap. Check if sp is fully contained.
			return sp.CompareStarts(keyCtx, cSpan) >= 0 &&
				sp.CompareEnds(keyCtx, cSpan) <= 0
		}
	}
	return false
}

// allSpansHaveSameValueAtColumn checks whether all spans share the same start
// and end value at the given column index, and that value is consistent across
// all spans. Returns false when a span lacks that column's values or values differ.
func (c *Constraint) allSpansHaveSameValueAtColumn(evalCtx *tree.EvalContext, col int) bool {
	var val tree.Datum
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		if sp.start.Length() <= col || sp.end.Length() <= col {
			return false
		}
		startVal := sp.start.Value(col)
		if startVal.Compare(evalCtx, sp.end.Value(col)) != 0 {
			return false
		}
		if i == 0 {
			val = startVal
		} else if startVal.Compare(evalCtx, val) != 0 {
			return false
		}
	}
	return true
}

// maxKeyDepth returns the maximum key length across all spans. This represents
// the number of columns constrained by the Constraint.
func (c *Constraint) maxKeyDepth() int {
	count := 0
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		start := sp.StartKey()
		end := sp.EndKey()
		if sl := start.Length(); sl > count {
			count = sl
		}
		if el := end.Length(); el > count {
			count = el
		}
	}
	return count
}

// findUniformSpanPrefix returns the length of the longest prefix of columns
// for which all spans have the same start and end values.
func (c *Constraint) findUniformSpanPrefix(evalCtx *tree.EvalContext) int {
	prefix := 0
	for ; prefix < c.Columns.Count(); prefix++ {
		if !c.spansHaveUniformPrefixAt(evalCtx, prefix) {
			return prefix
		}
	}
	return prefix
}

// spansHaveUniformPrefixAt returns true if at column index prefix, every span
// has equal start and end values and both are present.
func (c *Constraint) spansHaveUniformPrefixAt(evalCtx *tree.EvalContext, prefix int) bool {
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		start := sp.StartKey()
		end := sp.EndKey()
		if start.Length() <= prefix || end.Length() <= prefix ||
			start.Value(prefix).Compare(evalCtx, end.Value(prefix)) != 0 {
			return false
		}
	}
	return true
}

// addNonNullPrefixCols adds columns to res for which all spans guarantee
// non-null values (no span has NULL as the start value for that column).
func (c *Constraint) addNonNullPrefixCols(res *opt.ColSet, evalCtx *tree.EvalContext, prefix int) {
	for i := 0; i < prefix; i++ {
		if !c.anySpanStartsWithNullAt(i) {
			res.Add(c.Columns.Get(i).ID())
		}
	}
}

// anySpanStartsWithNullAt returns true if any span has NULL as the start value
// at the given column index.
func (c *Constraint) anySpanStartsWithNullAt(col int) bool {
	for j := 0; j < c.Spans.Count(); j++ {
		start := c.Spans.Get(j).StartKey()
		if start.Value(col) == tree.DNull {
			return true
		}
	}
	return false
}

// isFirstColBeyondPrefixNonNull returns true if the first column beyond the
// prefix is guaranteed non-null across all spans.
func (c *Constraint) isFirstColBeyondPrefixNonNull(evalCtx *tree.EvalContext, prefix int) bool {
	col := c.Columns.Get(prefix)
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		var key Key
		var boundary SpanBoundary
		if !col.Descending() {
			key, boundary = sp.StartKey(), sp.StartBoundary()
		} else {
			key, boundary = sp.EndKey(), sp.EndBoundary()
		}
		// If the span is unbounded on the NULL side, or if it is of the form
		// [/NULL - /x], the column is nullable.
		if key.Length() <= prefix || (key.Value(prefix) == tree.DNull && boundary == IncludeBoundary) {
			return false
		}
	}
	return true
}

// allColsAreNonNull checks whether all index columns are known non-null given
// the constraint's not-null columns and a base set of not-null columns.
func (c *Constraint) allColsAreNonNull(
	indexCols, notNullCols opt.ColSet, evalCtx *tree.EvalContext,
) bool {
	return indexCols.SubsetOf(notNullCols.Union(c.ExtractNotNullCols(evalCtx)))
}

// computeDistinctValCount computes the maximum number of distinct results
// possible from this constraint based on the prefix length.
func (c *Constraint) computeDistinctValCount(
	evalCtx *tree.EvalContext, numCols, prefix int,
) uint64 {
	switch {
	case prefix < numCols-1:
		return 0
	case prefix == numCols-1:
		// If the prefix does not include the last column, calculate the number of
		// distinct values possible in the span. This is only supported for int
		// and date types.
		return c.sumDistinctValuesForLastCol(evalCtx, numCols)
	default:
		return uint64(c.Spans.Count())
	}
}

// sumDistinctValuesForLastCol sums the number of distinct integer or date
// values across all spans for the last column. Returns 0 if this cannot be
// computed.
func (c *Constraint) sumDistinctValuesForLastCol(evalCtx *tree.EvalContext, numCols int) uint64 {
	var distinctVals uint64
	colIdx := numCols - 1
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		start := sp.StartKey()
		end := sp.EndKey()

		// Ensure that the keys specify the last column.
		if start.Length() != numCols || end.Length() != numCols {
			return 0
		}

		// TODO(asubiotto): This logic is very similar to
		// updateDistinctCountsFromConstraint. It would be nice to extract this
		// logic somewhere.
		startIntVal, endIntVal, ok := extractIntRange(evalCtx, start.Value(colIdx), end.Value(colIdx))
		if !ok {
			return 0
		}

		if c.Columns.Get(colIdx).Ascending() {
			distinctVals += uint64(endIntVal - startIntVal)
		} else {
			distinctVals += uint64(startIntVal - endIntVal)
		}

		// Add one since both start and end boundaries should be inclusive
		// (due to Span.PreferInclusive).
		distinctVals++
	}
	return distinctVals
}

// combineSpansWithSuffix combines each span in the constraint with the suffix
// constraint's spans. Returns the resulting Spans and true if spans were
// modified.
func (c *Constraint) combineSpansWithSuffix(
	keyCtx KeyContext, other *Constraint, offset int,
) (Spans, bool) {
	var result Spans
	var resultInitialized bool

	for i := 0; i < c.Spans.Count(); i++ {
		sp := *c.Spans.Get(i)
		startLen, endLen := sp.start.Length(), sp.end.Length()

		// Special case: exact value on the column matching the suffix boundary.
		// This can break a single span into multiple refined spans.
		if startLen == endLen && startLen == offset &&
			sp.start.Compare(&keyCtx, sp.end, ExtendLow, ExtendLow) == 0 {

			if !resultInitialized {
				resultInitialized = true
				result.Alloc(c.Spans.Count() + other.Spans.Count())
				c.copyPrefixSpans(&result, i)
			}
			c.emitExactValueSpans(&result, &keyCtx, other, &sp)
			continue
		}

		// Try to extend start and end keys with the suffix constraint.
		modified := c.tryExtendStartKey(&sp, other, offset, startLen)
		modified = c.tryExtendEndKey(&sp, other, offset, endLen) || modified

		if modified {
			if !resultInitialized {
				resultInitialized = true
				result.Alloc(c.Spans.Count())
				c.copyPrefixSpans(&result, i)
			}
			// The span can become invalid (empty). For example:
			//   /1/2: [/1 - /1/2]
			//   /2: [/5 - /5]
			// This results in an invalid span [/1/5 - /1/2] which we must discard.
			if sp.start.Compare(&keyCtx, sp.end, sp.startExt(), sp.endExt()) < 0 {
				result.Append(&sp)
			}
		} else {
			if resultInitialized {
				result.Append(&sp)
			}
		}
	}
	return result, resultInitialized
}

// copyPrefixSpans copies spans before index i from the constraint's Spans into result.
func (c *Constraint) copyPrefixSpans(result *Spans, index int) {
	for j := 0; j < index; j++ {
		result.Append(c.Spans.Get(j))
	}
}

// emitExactValueSpans produces one span per suffix span when the prefix span
// has an exact (point) value at the suffix boundary.
func (c *Constraint) emitExactValueSpans(
	result *Spans, keyCtx *KeyContext, other *Constraint, sp *Span,
) {
	for j := 0; j < other.Spans.Count(); j++ {
		extSp := other.Spans.Get(j)
		var newSp Span
		newSp.Init(
			sp.start.Concat(extSp.start), extSp.startBoundary,
			sp.end.Concat(extSp.end), extSp.endBoundary,
		)
		result.Append(&newSp)
	}
}

// tryExtendStartKey attempts to extend the start key of the span with the
// first span's start from the suffix constraint. Returns true if modified.
func (c *Constraint) tryExtendStartKey(sp *Span, other *Constraint, offset, startLen int) bool {
	if startLen != offset || sp.startBoundary != IncludeBoundary {
		return false
	}
	// We can advance the starting boundary. Calculate constraints for the
	// column that follows. If we have multiple constraints, we can only use
	// the start of the first one to tighten the span.
	extSp := other.Spans.Get(0)
	if extSp.start.Length() > 0 {
		sp.start = sp.start.Concat(extSp.start)
		sp.startBoundary = extSp.startBoundary
		return true
	}
	return false
}

// tryExtendEndKey attempts to extend the end key of the span with the
// last span's end from the suffix constraint. Returns true if modified.
func (c *Constraint) tryExtendEndKey(sp *Span, other *Constraint, offset, endLen int) bool {
	if endLen != offset || sp.endBoundary != IncludeBoundary {
		return false
	}
	// End key case is symmetric with the start key case.
	extSp := other.Spans.Get(other.Spans.Count() - 1)
	if extSp.end.Length() > 0 {
		sp.end = sp.end.Concat(extSp.end)
		sp.endBoundary = extSp.endBoundary
		return true
	}
	return false
}

// consolidateConsecutiveSpans merges spans that have consecutive boundaries.
func (c *Constraint) consolidateConsecutiveSpans(keyCtx *KeyContext) Spans {
	var result Spans
	for i := 1; i < c.Spans.Count(); i++ {
		last := c.Spans.Get(i - 1)
		sp := c.Spans.Get(i)
		if c.spansHaveConsecutiveBoundaries(keyCtx, last, sp) {
			// We only initialize `result` if we need to change something.
			if result.Count() == 0 {
				result.Alloc(c.Spans.Count() - 1)
				for j := 0; j < i; j++ {
					result.Append(c.Spans.Get(j))
				}
			}
			// Extend the last result span's end to absorb sp.
			r := result.Get(result.Count() - 1)
			r.end = sp.end
			r.endBoundary = sp.endBoundary
		} else {
			if result.Count() != 0 {
				result.Append(sp)
			}
		}
	}
	return result
}

// spansHaveConsecutiveBoundaries returns true if two spans share consecutive
// boundaries (last.end is immediately followed by sp.start), allowing them to
// be merged.
func (c *Constraint) spansHaveConsecutiveBoundaries(keyCtx *KeyContext, last, sp *Span) bool {
	return last.endBoundary == IncludeBoundary &&
		sp.startBoundary == IncludeBoundary &&
		sp.start.IsNextKey(keyCtx, last.end)
}

var (
	// ConstScalarWhitelist if functionExpr is in the list of builtins, then it can convert constExpr
	ConstScalarWhitelist = map[string]struct{}{
		"client_encoding":              {},
		"version":                      {},
		"current_database":             {},
		"current_schema":               {},
		"current_user":                 {},
		"now":                          {},
		"current_timestamp":            {},
		"localtimestamp":               {},
		"statement_timestamp":          {},
		"cluster_logical_timestamp":    {},
		"clock_timestamp":              {},
		"timeofday":                    {},
		"transaction_timestamp":        {},
		"kwdb_internal.create_regtype": {},
		"get_bit":                      {},
		"set_bit":                      {},
		"quote_literal":                {},
		"quote_nullable":               {},
		"experimental_strftime":        {},
		"experimental_strptime":        {},
		"extract":                      {},
		"extract_duration":             {},
		"date_trunc":                   {},
		"timezone":                     {},
		"width_bucket":                 {},
	}
)

// -------- standalone functions --------

// mergeSpansForUnion merges two sets of spans (left and right) into a union,
// using a merge-sort approach. The result contains one span per contiguous
// merged region.
func mergeSpansForUnion(keyCtx KeyContext, leftSpans, rightSpans Spans) Spans {
	left := &leftSpans
	leftIndex := 0
	right := &rightSpans
	rightIndex := 0
	var result Spans
	result.Alloc(left.Count() + right.Count())

	for leftIndex < left.Count() || rightIndex < right.Count() {
		if rightIndex < right.Count() {
			if leftIndex >= left.Count() ||
				left.Get(leftIndex).Compare(&keyCtx, right.Get(rightIndex)) > 0 {
				// Swap the two sets, so that going forward the current left
				// span starts before the current right span.
				left, right = right, left
				leftIndex, rightIndex = rightIndex, leftIndex
			}
		}

		// Merge this span with any overlapping spans in left or right. Initially,
		// it can only overlap with spans in right, but after the merge we can
		// have new overlaps; hence why this is a loop and we check against both
		// left and right. For example:
		//   left : [/1 - /10] [/20 - /30] [/40 - /50]
		//   right: [/5 - /25] [/30 - /40]
		//                             span
		//   initial:                [/1 - /10]
		//   merge with [/5 - /25]:  [/1 - /25]
		//   merge with [/20 - /30]: [/1 - /30]
		//   merge with [/30 - /40]: [/1 - /40]
		//   merge with [/40 - /50]: [/1 - /50]
		mergeSpan := *left.Get(leftIndex)
		leftIndex++
		for {
			// Note that Span.TryUnionWith returns false for a different reason
			// than Constraint.tryUnionWith. Span.TryUnionWith returns false
			// when the spans are not contiguous, and therefore the union cannot
			// be represented as a valid Span. Constraint.tryUnionWith returns
			// false when the merged spans are unconstrained (cover entire key
			// range), and therefore the union cannot be represented as a valid
			// Constraint.
			var ok bool
			if leftIndex < left.Count() {
				if mergeSpan.TryUnionWith(&keyCtx, left.Get(leftIndex)) {
					leftIndex++
					ok = true
				}
			}
			if rightIndex < right.Count() {
				if mergeSpan.TryUnionWith(&keyCtx, right.Get(rightIndex)) {
					rightIndex++
					ok = true
				}
			}

			// If neither union succeeded, then it means either:
			//   1. The spans don't merge into a single contiguous span, and will
			//      need to be represented separately in this constraint.
			//   2. There are no more spans to merge.
			if !ok {
				break
			}
		}
		result.Append(&mergeSpan)
	}
	return result
}

// intersectSpansMerge computes the intersection of two sets of ordered spans
// using a merge-sort style approach.
func intersectSpansMerge(keyCtx KeyContext, leftSpans, rightSpans Spans) Spans {
	left := &leftSpans
	leftIndex := 0
	right := &rightSpans
	rightIndex := 0
	var result Spans
	result.Alloc(left.Count())

	for leftIndex < left.Count() && rightIndex < right.Count() {
		if left.Get(leftIndex).StartsAfter(&keyCtx, right.Get(rightIndex)) {
			rightIndex++
			continue
		}

		mergeSpan := *left.Get(leftIndex)
		if !mergeSpan.TryIntersectWith(&keyCtx, right.Get(rightIndex)) {
			leftIndex++
			continue
		}
		result.Append(&mergeSpan)

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.Get(leftIndex).CompareEnds(&keyCtx, right.Get(rightIndex))
		if cmp <= 0 {
			leftIndex++
		}
		if cmp >= 0 {
			rightIndex++
		}
	}
	return result
}

// getPrecisionFactor returns the multiplication factor for timestamp precision.
// Precision 3 -> 1e3 (millisecond), 6 -> 1e6 (microsecond), default -> 1e9 (nanosecond).
func getPrecisionFactor(precision int32) int64 {
	switch precision {
	case 3:
		return 1e3
	case 6:
		return 1e6
	default:
		return 1e9
	}
}

// convertSpanToTimestamp converts a span's start and end keys into timestamp
// integer representations using the given precision factor.
func convertSpanToTimestamp(
	start Key, startBoundary bool, end Key, endBoundary bool, prec int64,
) (int64, int64) {
	return assignPrecision(start, startBoundary, end, endBoundary, prec)
}

// extractIntRange extracts int64 range boundaries from datum values.
// Supports IntFamily and DateFamily. Returns the start/end int values and true
// on success, or false if the types are not supported.
func extractIntRange(evalCtx *tree.EvalContext, startVal, endVal tree.Datum) (int64, int64, bool) {
	var startIntVal, endIntVal int64
	if startVal.ResolvedType().Family() == types.IntFamily &&
		endVal.ResolvedType().Family() == types.IntFamily {
		startIntVal = int64(*startVal.(*tree.DInt))
		endIntVal = int64(*endVal.(*tree.DInt))
		return startIntVal, endIntVal, true
	} else if startVal.ResolvedType().Family() == types.DateFamily &&
		endVal.ResolvedType().Family() == types.DateFamily {
		startDate := startVal.(*tree.DDate)
		endDate := endVal.(*tree.DDate)
		if !startDate.IsFinite() || !endDate.IsFinite() {
			// One of the boundaries is not finite, so we can't determine the
			// distinct count for this column.
			return 0, 0, false
		}
		startIntVal = int64(startDate.PGEpochDays())
		endIntVal = int64(endDate.PGEpochDays())
		return startIntVal, endIntVal, true
	}
	return 0, 0, false
}

// assignPrecision converts the datum type time to an integer according to precision
//
// in parameter:
//
//	start            - the start value of the interval for filtering time
//	startBoundary		 - startBoundary indicates whether the span contains the start key value
//	end              - the end value of the interval for filtering time
//	endBoundary      - endBoundary indicates whether the span contains the end key value
//	precision        - precision represents time precision
//
// out parameter:
//
//	 startNew         - startNew represents the integer corresponding to the start time,
//											 and its units depend on precision
//	 endNew           - endNew represents the integer corresponding to the end time,
//	                    and its units depend on precision
func assignPrecision(
	start Key, startBoundary bool, end Key, endBoundary bool, precision int64,
) (startNew int64, endNew int64) {
	startNew = computeTimestampStart(start, startBoundary, precision)
	endNew = computeTimestampEnd(end, endBoundary, precision)
	return startNew, endNew
}

// computeTimestampStart converts a start key with a timestamp datum to an
// integer representation according to the given precision.
func computeTimestampStart(start Key, startBoundary bool, precision int64) int64 {
	if start.firstVal == nil {
		return math.MinInt64
	}
	t, ok := start.firstVal.(*tree.DTimestampTZ)
	if !ok {
		return math.MinInt64
	}
	nanosecond := t.Time.Nanosecond()
	second := t.Time.Unix()
	if second < math.MinInt64/precision {
		return math.MinInt64
	}
	startNew := second*precision + int64(nanosecond)/(1e9/precision)
	if startBoundary {
		startNew++
	}
	return startNew
}

// computeTimestampEnd converts an end key with a timestamp datum to an
// integer representation according to the given precision.
func computeTimestampEnd(end Key, endBoundary bool, precision int64) int64 {
	if end.firstVal == nil {
		return math.MaxInt64
	}
	t, ok := end.firstVal.(*tree.DTimestampTZ)
	if !ok {
		return math.MaxInt64
	}
	nanosecond := t.Time.Nanosecond()
	second := t.Time.Unix()
	if second > math.MaxInt64/precision {
		return math.MaxInt64
	}
	if second*precision <= (math.MaxInt64 - int64(nanosecond)/(1e9/precision)) {
		endNew := second*precision + int64(nanosecond)/(1e9/precision)
		if endBoundary {
			if nanosecond%int(1e9/precision) == 0 {
				endNew--
			}
		}
		return endNew
	}
	return math.MaxInt64
}
