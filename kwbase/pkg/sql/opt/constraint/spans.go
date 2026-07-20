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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// Spans is a collection of spans. There are no general requirements on the
// contents of the spans in the structure; the caller has to make sure they make
// sense in the respective context.
//
// The internal representation optimizes for the common single-span case by
// storing the first span inline; additional spans live in otherSpans.
type Spans struct {
	// firstSpan holds the first span and otherSpans hold any spans beyond the
	// first. These are separated in order to optimize for the common case of a
	// single-span constraint.
	firstSpan  Span
	otherSpans []Span
	numSpans   int32
	immutable  bool
}

// Alloc allocates enough space to support the given amount of spans without
// reallocation. Does nothing if the structure already contains spans.
func (s *Spans) Alloc(capacity int) {
	// We don't preallocate if the capacity is only 2: pre-allocating the slice to
	// size 1 is no better than allocating it on the first Append, but it's worse
	// if we end up not needing it.
	if capacity > minCapacityForAlloc && s.numSpans == 0 {
		s.otherSpans = make([]Span, 0, capacity-1)
	}
}

// minCapacityForAlloc is the threshold below which we skip pre-allocation
// of the otherSpans slice, because the overhead outweighs the benefit.
const minCapacityForAlloc = 2

// InitSingleSpan initializes the structure with a single span.
func (s *Spans) InitSingleSpan(sp *Span) {
	s.firstSpan = *sp
	s.otherSpans = nil
	s.numSpans = 1
	s.immutable = false
}

// Count returns the number of spans.
func (s *Spans) Count() int {
	return int(s.numSpans)
}

// Get returns the nth span.
func (s *Spans) Get(nth int) *Span {
	if nth == 0 && s.numSpans > 0 {
		return &s.firstSpan
	}
	return &s.otherSpans[nth-1]
}

// Append adds another span (at the end).
func (s *Spans) Append(sp *Span) {
	s.guardMutation()
	if s.numSpans == 0 {
		s.firstSpan = *sp
	} else {
		s.otherSpans = append(s.otherSpans, *sp)
	}
	s.numSpans++
}

// Truncate removes all but the first newLength spans.
func (s *Spans) Truncate(newLength int) {
	s.guardMutation()
	if int32(newLength) > s.numSpans {
		panic(errors.AssertionFailedf("can't truncate to longer length"))
	}
	if newLength == 0 {
		s.otherSpans = s.otherSpans[:0]
	} else {
		s.otherSpans = s.otherSpans[:newLength-1]
	}
	s.numSpans = int32(newLength)
}

func (s Spans) String() string {
	return s.buildString()
}

// makeImmutable causes panics in any future calls to methods that mutate either
// the Spans structure or any Span returned by Get.
func (s *Spans) makeImmutable() {
	s.immutable = true
}

// sortedAndMerged returns true if the collection of spans is strictly
// ordered and no spans overlap.
func (s *Spans) sortedAndMerged(keyCtx *KeyContext) bool {
	for i := 1; i < s.Count(); i++ {
		if !s.Get(i).StartsStrictlyAfter(keyCtx, s.Get(i-1)) {
			return false
		}
	}
	return true
}

// SortAndMerge sorts the spans and merges any overlapping spans.
func (s *Spans) SortAndMerge(keyCtx *KeyContext) {
	if s.sortedAndMerged(keyCtx) {
		return
	}
	sort.Sort(&spanSorter{keyCtx: *keyCtx, spans: s})

	// Merge overlapping spans. We maintain the last span and extend it with
	// whatever spans it overlaps with.
	newCount := s.mergeOverlappingSpans(keyCtx)
	s.Truncate(newCount)
}

// -------- internal helpers for Spans --------

// guardMutation panics if the Spans structure is marked immutable.
func (s *Spans) guardMutation() {
	if s.immutable {
		panic(errors.AssertionFailedf("mutation disallowed"))
	}
}

// buildString formats the Spans collection as a space-separated list of spans.
func (s Spans) buildString() string {
	var b strings.Builder
	for i := 0; i < s.Count(); i++ {
		if i > 0 {
			b.WriteRune(' ')
		}
		b.WriteString(s.Get(i).String())
	}
	return b.String()
}

// mergeOverlappingSpans merges adjacent or overlapping spans in a sorted
// Spans collection. It returns the new span count after merging.
// The caller is responsible for truncating the Spans to the returned count.
func (s *Spans) mergeOverlappingSpans(keyCtx *KeyContext) int {
	n := 0
	currentSpan := *s.Get(0)
	for i := 1; i < s.Count(); i++ {
		sp := s.Get(i)
		if sp.StartsStrictlyAfter(keyCtx, &currentSpan) {
			// No overlap. "Output" the current span and advance.
			*s.Get(n) = currentSpan
			n++
			currentSpan = *sp
		} else {
			// There is overlap; extend the current span to the right if necessary.
			if currentSpan.CompareEnds(keyCtx, sp) < 0 {
				currentSpan.end = sp.end
				currentSpan.endBoundary = sp.endBoundary
			}
		}
	}
	*s.Get(n) = currentSpan
	return n + 1
}

// spanSorter implements sort.Interface for sorting spans by their start
// boundaries using the provided KeyContext.
type spanSorter struct {
	keyCtx KeyContext
	spans  *Spans
}

var _ sort.Interface = &spanSorter{}

// Len is part of sort.Interface.
func (ss *spanSorter) Len() int {
	return ss.spans.Count()
}

// Less is part of sort.Interface.
func (ss *spanSorter) Less(i, j int) bool {
	return ss.spans.Get(i).Compare(&ss.keyCtx, ss.spans.Get(j)) < 0
}

// Swap is part of sort.Interface.
func (ss *spanSorter) Swap(i, j int) {
	si := ss.spans.Get(i)
	sj := ss.spans.Get(j)
	*si, *sj = *sj, *si
}
