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
	"bytes"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// SpanBoundary specifies whether a span endpoint is inclusive or exclusive of
// its start or end key. An inclusive boundary is represented as '[' and an
// exclusive boundary is represented as ')'. Examples:
//   [/0 - /1]  (inclusive, inclusive)
//   [/1 - /10) (inclusive, exclusive)
type SpanBoundary bool

const (
	// IncludeBoundary indicates that the boundary does include the respective
	// key.
	IncludeBoundary SpanBoundary = false

	// ExcludeBoundary indicates that the boundary does not include the
	// respective key.
	ExcludeBoundary SpanBoundary = true
)

// Span represents the range between two composite keys. The end keys of the
// range can be inclusive or exclusive. Each key value within the range is
// an N-tuple of datum values, one for each constrained column. Here are some
// examples:
//   @1 < 100                                          : [ - /100)
//   @1 >= 100                                         : [/100 - ]
//   @1 >= 1 AND @1 <= 10                              : [/1 - /10]
//   (@1 = 100 AND @2 > 10) OR (@1 > 100 AND @1 <= 101): (/100/10 - /101]
type Span struct {
	// Start is the starting boundary for the span.
	start Key

	// End is the ending boundary for the span.
	end Key

	// startBoundary indicates whether the span contains the start key value.
	startBoundary SpanBoundary

	// endBoundary indicates whether the span contains the the end key value.
	endBoundary SpanBoundary
}

// UnconstrainedSpan is the span without any boundaries.
var UnconstrainedSpan = Span{}

// IsUnconstrained is true if the span does not constrain the key range. Both
// the start and end boundaries are empty. This is the default state of a Span
// before Set is called. Unconstrained spans cannot be used in constraints,
// since the absence of a constraint is equivalent to an unconstrained span.
func (sp *Span) IsUnconstrained() bool {
	startUnconstrained := sp.start.IsEmpty() || (sp.start.IsNull() && sp.startBoundary == IncludeBoundary)
	endUnconstrained := sp.end.IsEmpty()

	return startUnconstrained && endUnconstrained
}

// HasSingleKey is true if the span contains exactly one key. This is true when
// the start key is the same as the end key, and both boundaries are inclusive.
func (sp *Span) HasSingleKey(evalCtx *tree.EvalContext) bool {
	l := sp.start.Length()
	if l == 0 || l != sp.end.Length() {
		return false
	}
	if sp.startBoundary != IncludeBoundary || sp.endBoundary != IncludeBoundary {
		return false
	}
	for i, n := 0, l; i < n; i++ {
		if sp.start.Value(i).Compare(evalCtx, sp.end.Value(i)) != 0 {
			return false
		}
	}
	return true
}

// StartKey returns the start key.
func (sp *Span) StartKey() Key {
	return sp.start
}

// StartBoundary returns whether the start key is included or excluded.
func (sp *Span) StartBoundary() SpanBoundary {
	return sp.startBoundary
}

// EndKey returns the end key.
func (sp *Span) EndKey() Key {
	return sp.end
}

// EndBoundary returns whether the end key is included or excluded.
func (sp *Span) EndBoundary() SpanBoundary {
	return sp.endBoundary
}

// Init sets the boundaries of this span to the given values. The following
// spans are not allowed:
//  1. Empty span (should never be used in a constraint); not verified.
//  2. Exclusive empty key boundary (use inclusive instead); causes panic.
func (sp *Span) Init(start Key, startBoundary SpanBoundary, end Key, endBoundary SpanBoundary) {
	if start.IsEmpty() && startBoundary == ExcludeBoundary {
		// Enforce one representation for empty boundary.
		panic(errors.AssertionFailedf("an empty start boundary must be inclusive"))
	}
	if end.IsEmpty() && endBoundary == ExcludeBoundary {
		// Enforce one representation for empty boundary.
		panic(errors.AssertionFailedf("an empty end boundary must be inclusive"))
	}

	sp.start = start
	sp.startBoundary = startBoundary
	sp.end = end
	sp.endBoundary = endBoundary
}

// Compare returns an integer indicating the ordering of the two spans. The
// result will be 0 if the spans are equal, -1 if this span is less than the
// given span, or 1 if this span is greater. Spans are first compared based on
// their start boundaries. If those are equal, then their end boundaries are
// compared. An inclusive start boundary is less than an exclusive start
// boundary, and an exclusive end boundary is less than an inclusive end
// boundary. Here are examples of how various spans are ordered, with
// equivalent extended keys shown as well (see Key.Compare comment):
//   [     - /2  )  =  /Low      - /2/Low
//   [     - /2/1)  =  /Low      - /2/1/Low
//   [     - /2/1]  =  /Low      - /2/1/High
//   [     - /2  ]  =  /Low      - /2/High
//   [     -     ]  =  /Low      - /High
//   [/1   - /2/1)  =  /1/Low    - /2/1/Low
//   [/1   - /2/1]  =  /1/Low    - /2/1/High
//   [/1   -     ]  =  /1/Low    - /High
//   [/1/1 - /2  )  =  /1/1/Low  - /2/Low
//   [/1/1 - /2  ]  =  /1/1/Low  - /2/High
//   [/1/1 -     ]  =  /1/1/Low  - /High
//   (/1/1 - /2  )  =  /1/1/High - /2/Low
//   (/1/1 - /2  ]  =  /1/1/High - /2/High
//   (/1/1 -     ]  =  /1/1/High - /High
//   (/1   - /2/1)  =  /1/High   - /2/1/Low
//   (/1   - /2/1]  =  /1/High   - /2/1/High
//   (/1   -     ]  =  /1/High   - /High
func (sp *Span) Compare(keyCtx *KeyContext, other *Span) int {
	// Span with lowest start boundary is less than the other.
	if cmp := sp.CompareStarts(keyCtx, other); cmp != 0 {
		return cmp
	}

	// Start boundary is same, so span with lowest end boundary is less than
	// the other.
	if cmp := sp.CompareEnds(keyCtx, other); cmp != 0 {
		return cmp
	}

	// End boundary is same as well, so spans are the same.
	return 0
}

// CompareStarts returns an integer indicating the ordering of the start
// boundaries of the two spans. The result will be 0 if the spans have the same
// start boundary, -1 if this span has a smaller start boundary than the given
// span, or 1 if this span has a bigger start boundary than the given span.
func (sp *Span) CompareStarts(keyCtx *KeyContext, other *Span) int {
	return sp.start.Compare(keyCtx, other.start, sp.startExt(), other.startExt())
}

// CompareEnds returns an integer indicating the ordering of the end boundaries
// of the two spans. The result will be 0 if the spans have the same end
// boundary, -1 if this span has a smaller end boundary than the given span, or
// 1 if this span has a bigger end boundary than the given span.
func (sp *Span) CompareEnds(keyCtx *KeyContext, other *Span) int {
	return sp.end.Compare(keyCtx, other.end, sp.endExt(), other.endExt())
}

// StartsAfter returns true if this span is greater than the given span and
// does not overlap it. In other words, this span's start boundary is greater
// or equal to the given span's end boundary.
func (sp *Span) StartsAfter(keyCtx *KeyContext, other *Span) bool {
	return sp.start.Compare(keyCtx, other.end, sp.startExt(), other.endExt()) >= 0
}

// StartsStrictlyAfter returns true if this span is greater than the given span and
// does not overlap or touch it. In other words, this span's start boundary is
// strictly greater than the given span's end boundary.
func (sp *Span) StartsStrictlyAfter(keyCtx *KeyContext, other *Span) bool {
	return sp.start.Compare(keyCtx, other.end, sp.startExt(), other.endExt()) > 0
}

// TryIntersectWith finds the overlap between this span and the given span.
// This span is updated to only cover the range that is common to both spans.
// If there is no overlap, then this span will not be updated, and
// TryIntersectWith will return false.
func (sp *Span) TryIntersectWith(keyCtx *KeyContext, other *Span) bool {
	cmpStarts := sp.CompareStarts(keyCtx, other)
	if cmpStarts > 0 {
		// If this span's start boundary is >= the other span's end boundary,
		// then intersection is empty.
		if sp.start.Compare(keyCtx, other.end, sp.startExt(), other.endExt()) >= 0 {
			return false
		}
	}

	cmpEnds := sp.CompareEnds(keyCtx, other)
	if cmpEnds < 0 {
		// If this span's end boundary is <= the other span's start boundary,
		// then intersection is empty.
		if sp.end.Compare(keyCtx, other.start, sp.endExt(), other.startExt()) <= 0 {
			return false
		}
	}

	// Only update now that it's known that intersection is not empty.
	if cmpStarts < 0 {
		sp.start = other.start
		sp.startBoundary = other.startBoundary
	}
	if cmpEnds > 0 {
		sp.end = other.end
		sp.endBoundary = other.endBoundary
	}
	return true
}

// TryUnionWith attempts to merge this span with the given span. If the merged
// spans cannot be expressed as a single span, then TryUnionWith will not
// update the span and TryUnionWith returns false. This could occur if the
// spans are disjoint, for example:
//   [/1 - /5] UNION [/10 - /15]
//
// Otherwise, this span is updated to the merged span range and TryUnionWith
// returns true. If the resulting span does not constrain the range [ - ], then
// its IsUnconstrained method returns true, and it cannot be used as part of a
// constraint in a constraint set.
func (sp *Span) TryUnionWith(keyCtx *KeyContext, other *Span) bool {
	// Determine the minimum start boundary.
	cmpStartKeys := sp.CompareStarts(keyCtx, other)

	var cmp int
	if cmpStartKeys < 0 {
		// This span is less, so see if there's any "space" after it and before
		// the start of the other span.
		cmp = sp.end.Compare(keyCtx, other.start, sp.endExt(), other.startExt())
	} else if cmpStartKeys > 0 {
		// This span is greater, so see if there's any "space" before it and
		// after the end of the other span.
		cmp = other.end.Compare(keyCtx, sp.start, other.endExt(), sp.startExt())
	}
	if cmp < 0 {
		// There's "space" between spans, so union of these spans can't be
		// expressed as a single span.
		return false
	}

	// Determine the maximum end boundary.
	cmpEndKeys := sp.CompareEnds(keyCtx, other)

	// Create the merged span.
	if cmpStartKeys > 0 {
		sp.start = other.start
		sp.startBoundary = other.startBoundary
	}
	if cmpEndKeys < 0 {
		sp.end = other.end
		sp.endBoundary = other.endBoundary
	}
	return true
}

// PreferInclusive tries to convert exclusive keys to inclusive keys. This is
// only possible if the relevant type supports Next/Prev.
//
// We prefer inclusive constraints because we can extend inclusive constraints
// with more constraints on columns that follow.
//
// Examples:
//  - for an integer column (/1 - /5)  =>  [/2 - /4].
//  - for a descending integer column (/5 - /1) => (/4 - /2).
//  - for a string column, we don't have Prev so
//      (/foo - /qux)  =>  [/foo\x00 - /qux).
//  - for a decimal column, we don't have either Next or Prev so we can't
//    change anything.
func (sp *Span) PreferInclusive(keyCtx *KeyContext) {
	if sp.startBoundary == ExcludeBoundary {
		if key, ok := sp.start.Next(keyCtx); ok {
			sp.start = key
			sp.startBoundary = IncludeBoundary
		}
	}
	if sp.endBoundary == ExcludeBoundary {
		if key, ok := sp.end.Prev(keyCtx); ok {
			sp.end = key
			sp.endBoundary = IncludeBoundary
		}
	}
}

// CutFront removes the first numCols columns in both keys.
func (sp *Span) CutFront(numCols int) {
	sp.start = sp.start.CutFront(numCols)
	sp.end = sp.end.CutFront(numCols)
}

func (sp *Span) startExt() KeyExtension {
	// Trivial cast of start boundary value:
	//   IncludeBoundary (false) = ExtendLow (false)
	//   ExcludeBoundary (true)  = ExtendHigh (true)
	return KeyExtension(sp.startBoundary)
}

func (sp *Span) endExt() KeyExtension {
	// Invert end boundary value:
	//   IncludeBoundary (false) = ExtendHigh (true)
	//   ExcludeBoundary (true)  = ExtendLow (false)
	return KeyExtension(!sp.endBoundary)
}

// String formats a Span. Inclusivity/exclusivity is shown using
// brackets/parens. Some examples:
//   [1 - 2]
//   (1/1 - 2)
//   [ - 5/6)
//   [1 - ]
//   [ - ]
func (sp Span) String() string {
	var buf bytes.Buffer
	if sp.startBoundary == IncludeBoundary {
		buf.WriteRune('[')
	} else {
		buf.WriteRune('(')
	}

	buf.WriteString(sp.start.String())
	buf.WriteString(" - ")
	buf.WriteString(sp.end.String())

	if sp.endBoundary == IncludeBoundary {
		buf.WriteRune(']')
	} else {
		buf.WriteRune(')')
	}

	return buf.String()
}
