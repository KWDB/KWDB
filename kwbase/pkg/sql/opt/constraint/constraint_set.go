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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// sentinelSets holds pre-allocated special constraint sets that are reused
// across the system to avoid allocation overhead.
var (
	// Unconstrained is an empty constraint set which does not impose any
	// constraints on any columns.
	Unconstrained = &Set{}

	// Contradiction is a special constraint set which indicates there are no
	// possible values for the expression; it will always yield the empty result
	// set.
	Contradiction = &Set{contradiction: true}
)

// Set is a conjunction of constraints that are inferred from scalar filter
// conditions. The constrained expression will always evaluate to a result set
// having values that conform to all of the constraints in the constraint set.
// Each constraint within the set is a disjunction of spans that together
// specify the domain of possible values which that constraint's column(s) can
// have. See the Constraint struct comment for more details.
//
// Constraint sets are useful for selecting indexes, pruning ranges, inferring
// non-null columns, and more. They serve as a "summary" of arbitrarily complex
// expressions, so that fast decisions can be made without analyzing the entire
// expression tree each time.
//
// A few examples:
//
//   - @1 >= 10
//     /@1: [/10 - ]
//
//   - @1 > 10 AND @2 = 5
//     /@1: [/11 - ]
//     /@2: [/5 - /5]
//
//   - (@1 = 10 AND @2 > 5) OR (@1 = 20 AND @2 > 0)
//     /@1: [/10 - /10] [/20 - /20]
//     /@2: [/1 - ]
//
//   - @1 > 10.5 AND @2 != 'foo'
//     /@1: (10.5 - ]
//     /@2: [ - 'foo') ('foo' - ]
type Set struct {
	// firstConstraint holds the first constraint in the set and otherConstraints
	// hold any constraints beyond the first. These are separated in order to
	// optimize for the common case of a set with a single constraint.
	firstConstraint  Constraint
	otherConstraints []Constraint

	// length is the number of constraints in the set.
	length int32

	// contradiction is true if this is the special Contradiction constraint set.
	contradiction bool
}

// SingleConstraint creates a Set with a single Constraint.
func SingleConstraint(c *Constraint) *Set {
	if c.IsContradiction() {
		return Contradiction
	}
	if c.IsUnconstrained() {
		return Unconstrained
	}
	return &Set{length: 1, firstConstraint: *c}
}

// SingleSpanConstraint creates a Set with a single constraint which
// has one span.
func SingleSpanConstraint(keyCtx *KeyContext, span *Span) *Set {
	if span.IsUnconstrained() {
		return Unconstrained
	}
	s := &Set{length: 1}
	s.firstConstraint.InitSingleSpan(keyCtx, span)
	return s
}

// Length returns the number of constraints in the set.
func (s *Set) Length() int {
	return int(s.length)
}

// Constraint returns the nth constraint in the set. Together with the Length
// method, Constraint allows iteration over the list of constraints (since
// there is no method to return a slice of constraints).
func (s *Set) Constraint(nth int) *Constraint {
	if nth == 0 && s.length != 0 {
		return &s.firstConstraint
	}
	return &s.otherConstraints[nth-1]
}

// IsUnconstrained returns true if the constraint set contains no constraints,
// which means column values can have any possible values.
func (s *Set) IsUnconstrained() bool {
	return s.length == 0 && !s.contradiction
}

// Intersect finds the overlap between this constraint set and the given set.
// Constraints that exist in either of the input sets will get merged into the
// combined set. Compatible constraints (that share same column list) are
// intersected with one another. Intersect returns the merged set.
func (s *Set) Intersect(evalCtx *tree.EvalContext, other *Set) *Set {
	// Intersection with the contradiction set is always the contradiction set.
	if s.isSentinelContradiction() || other.isSentinelContradiction() {
		return Contradiction
	}

	// Intersection with the unconstrained set is the identity op.
	if s.IsUnconstrained() {
		return other
	}
	if other.IsUnconstrained() {
		return s
	}

	// Create a new set to hold the merged sets.
	return s.mergeSets(evalCtx, other, mergeModeIntersect)
}

// Union creates a new set with constraints that allow any value that either of
// the input sets allowed. Compatible constraints (that share same column list)
// that exist in both sets are merged with one another. Note that the results
// may not be "tight", meaning that the new constraint set might allow
// additional combinations of values that neither of the input sets allowed. For
// example:
//
//	(x > 1 AND y > 10) OR (x < 5 AND y < 50)
//
// the union is unconstrained (and thus allows combinations like x,y = 10,0).
//
// Union returns the merged set.
func (s *Set) Union(evalCtx *tree.EvalContext, other *Set) *Set {
	// Union with the contradiction set is an identity operation.
	if s.isSentinelContradiction() {
		return other
	} else if other.isSentinelContradiction() {
		return s
	}

	// Union with the unconstrained set yields an unconstrained set.
	if s.IsUnconstrained() || other.IsUnconstrained() {
		return Unconstrained
	}

	// Create a new set to hold the merged sets.
	return s.mergeSets(evalCtx, other, mergeModeUnion)
}

// ExtractCols returns all columns involved in the constraints in this set.
func (s *Set) ExtractCols() opt.ColSet {
	if s.length == 0 {
		return opt.ColSet{}
	}
	return s.collectAllCols()
}

// ExtractNotNullCols returns a set of columns that cannot be NULL for the
// constraints in the set to hold.
func (s *Set) ExtractNotNullCols(evalCtx *tree.EvalContext) opt.ColSet {
	if s.isSentinel() {
		return opt.ColSet{}
	}
	return s.collectNotNullCols(evalCtx)
}

// ExtractConstCols returns a set of columns which can only have one value
// for the constraints in the set to hold.
func (s *Set) ExtractConstCols(evalCtx *tree.EvalContext) opt.ColSet {
	if s.isSentinel() {
		return opt.ColSet{}
	}
	return s.collectConstCols(evalCtx)
}

// allocConstraint allocates space for a new constraint in the set and returns
// a pointer to it. The first constraint is stored inline, and subsequent
// constraints are stored in the otherConstraints slice.
func (s *Set) allocConstraint(capacity int) *Constraint {
	s.length++

	// First constraint does not require heap allocation.
	if s.length == 1 {
		return &s.firstConstraint
	}

	// Second constraint allocates slice.
	if s.otherConstraints == nil {
		s.otherConstraints = make([]Constraint, 1, capacity)
		return &s.otherConstraints[0]
	}

	// Subsequent constraints extend slice.
	if cap(s.otherConstraints) < capacity {
		panic(errors.AssertionFailedf(
			"correct capacity should have been set when otherConstraints was allocated"))
	}

	// Remember that otherConstraints' length is one less than the set length.
	s.otherConstraints = s.otherConstraints[:s.length-1]
	return &s.otherConstraints[s.length-2]
}

// undoAllocConstraint rolls back the previous allocation performed by
// allocConstraint. The next call to allocConstraint will allocate the same
// slot as before.
func (s *Set) undoAllocConstraint() {
	s.length--
}

func (s *Set) String() string {
	if s.IsUnconstrained() {
		return "unconstrained"
	}
	if s.isSentinelContradiction() {
		return "contradiction"
	}

	var b strings.Builder
	for i := 0; i < s.Length(); i++ {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(s.Constraint(i).String())
	}
	return b.String()
}

// -------- internal helpers for Set --------

// mergeMode specifies whether to intersect or union constraints.
type mergeMode int

const (
	mergeModeIntersect mergeMode = iota
	mergeModeUnion
)

// isSentinelContradiction checks if this set is the singleton Contradiction
// sentinel (pointer identity). This is different from IsUnconstrained which
// compares by value.
func (s *Set) isSentinelContradiction() bool {
	return s == Contradiction
}

// isSentinel returns true if this is either the Unconstrained or Contradiction
// sentinel set.
func (s *Set) isSentinel() bool {
	return s == Unconstrained || s == Contradiction
}

// mergeSets performs either an intersection or a union of two constraint
// sets, depending on mode. It uses a merge-sort style approach since
// constraints within a set are ordered by column indexes.
func (s *Set) mergeSets(evalCtx *tree.EvalContext, other *Set, mode mergeMode) *Set {
	mergeSet := &Set{}

	index := 0
	length := s.Length()
	otherIndex := 0
	otherLength := other.Length()

	// For union, we stop when either side is exhausted (unmatched constraints
	// are dropped because union with unconstrained = unconstrained).
	// For intersect, we process remaining constraints from either side (a
	// missing constraint in the other set = unconstrained = identity).
	moreToProcess := func() bool {
		if mode == mergeModeUnion {
			return index < length && otherIndex < otherLength
		}
		return index < length || otherIndex < otherLength
	}

	for moreToProcess() {
		cmp := resolveConstraintCmp(s, other, index, length, otherIndex, otherLength)

		if cmp == 0 {
			// Constraints have same columns, so they're compatible and need to
			// be merged.
			merge := mergeSet.allocConstraint(length - index + otherLength - otherIndex)
			*merge = *s.Constraint(index)
			merge.applyMergeOp(evalCtx, other.Constraint(otherIndex), mode)
			if merge.isSentinelMergeResult(mode) {
				mergeSet.undoAllocConstraint()
				// For intersect, contradiction propagates immediately.
				if mode == mergeModeIntersect && merge.IsContradiction() {
					return Contradiction
				}
				// For union, unconstrained means the result is unconstrained overall.
				if mode == mergeModeUnion && merge.IsUnconstrained() {
					return Unconstrained
				}
			}

			index++
			otherIndex++
		} else if cmp < 0 {
			if mode == mergeModeIntersect {
				// Absence of other constraint = unconstrained, so just add it.
				merge := mergeSet.allocConstraint(length - index + otherLength - otherIndex)
				*merge = *s.Constraint(index)
			}
			index++
		} else {
			if mode == mergeModeIntersect {
				merge := mergeSet.allocConstraint(length - index + otherLength - otherIndex)
				*merge = *other.Constraint(otherIndex)
			}
			otherIndex++
		}
	}
	return mergeSet
}

// resolveConstraintCmp returns the comparison result for constraints at the
// current positions, handling the case where one side is exhausted.
func resolveConstraintCmp(s, other *Set, index, length, otherIndex, otherLength int) int {
	if index >= length {
		return 1
	}
	if otherIndex >= otherLength {
		return -1
	}
	return compareConstraintsByCols(s.Constraint(index), other.Constraint(otherIndex))
}

// applyMergeOp applies the appropriate merge operation (intersect/union)
// based on the merge mode.
func (c *Constraint) applyMergeOp(evalCtx *tree.EvalContext, other *Constraint, mode mergeMode) {
	if mode == mergeModeIntersect {
		c.IntersectWith(evalCtx, other)
	} else {
		c.UnionWith(evalCtx, other)
	}
}

// isSentinelMergeResult returns true if the merge produced a sentinel result
// (contradiction for intersect, unconstrained for union) that requires
// special handling.
func (c *Constraint) isSentinelMergeResult(mode mergeMode) bool {
	if mode == mergeModeIntersect {
		return c.IsContradiction()
	}
	return c.IsUnconstrained()
}

// collectAllCols collects all column IDs from all constraints.
func (s *Set) collectAllCols() opt.ColSet {
	res := s.firstConstraint.Columns.ColSet()
	for i := int32(1); i < s.length; i++ {
		res.UnionWith(s.otherConstraints[i-1].Columns.ColSet())
	}
	return res
}

// collectNotNullCols collects columns that cannot be NULL across all constraints.
func (s *Set) collectNotNullCols(evalCtx *tree.EvalContext) opt.ColSet {
	res := s.Constraint(0).ExtractNotNullCols(evalCtx)
	for i := 1; i < s.Length(); i++ {
		res.UnionWith(s.Constraint(i).ExtractNotNullCols(evalCtx))
	}
	return res
}

// collectConstCols collects columns that are restricted to single values.
func (s *Set) collectConstCols(evalCtx *tree.EvalContext) opt.ColSet {
	res := s.Constraint(0).ExtractConstCols(evalCtx)
	for i := 1; i < s.Length(); i++ {
		res.UnionWith(s.Constraint(i).ExtractConstCols(evalCtx))
	}
	return res
}

// compareConstraintsByCols orders constraints by the indexes of their columns,
// with column position determining significance in the sort key (most
// significant first).
func compareConstraintsByCols(left, right *Constraint) int {
	leftCount := left.Columns.Count()
	rightCount := right.Columns.Count()
	for i := 0; i < leftCount && i < rightCount; i++ {
		diff := int(left.Columns.Get(i) - right.Columns.Get(i))
		if diff != 0 {
			return diff
		}
	}
	return leftCount - rightCount
}
