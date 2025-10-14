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

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for count_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
)

// newCountRowAgg creates a COUNT(*) aggregate, which counts every row in the
// result unconditionally.
func newCountRowAgg(allocator *Allocator) *countAgg {
	allocator.AdjustMemoryUsage(int64(sizeOfCountAgg))
	return &countAgg{countRow: true}
}

// newCountAgg creates a COUNT(col) aggregate, which counts every row in the
// result where the value of col is not null.
func newCountAgg(allocator *Allocator) *countAgg {
	allocator.AdjustMemoryUsage(int64(sizeOfCountAgg))
	return &countAgg{countRow: false}
}

// countAgg supports both the COUNT(*) and COUNT(col) aggregates, which are
// distinguished by the countRow flag.
type countAgg struct {
	groups   []bool
	vec      []int64
	nulls    *coldata.Nulls
	curIdx   int
	curAgg   int64
	done     bool
	countRow bool
}

var _ aggregateFunc = &countAgg{}

const sizeOfCountAgg = unsafe.Sizeof(countAgg{})

func (a *countAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec.Int64()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *countAgg) Reset() {
	a.curIdx = -1
	a.curAgg = 0
	a.nulls.UnsetNulls()
	a.done = false
}

func (a *countAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *countAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *countAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		a.vec[a.curIdx] = a.curAgg
		a.curIdx++
		a.done = true
		return
	}

	sel := b.Selection()

	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	if !a.countRow && b.ColVec(int(inputIdxs[0])).MaybeHasNulls() {
		nulls := b.ColVec(int(inputIdxs[0])).Nulls()
		if sel != nil {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		} else {
			for i := range a.groups[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			for _, i := range sel[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
		} else {
			for i := range a.groups[:inputLen] {
				_ACCUMULATE_COUNT(a, nulls, i, false)
			}
		}
	}
}

// {{/*
// _ACCUMULATE_COUNT aggregates the value at index i into the count aggregate.
// _COL_WITH_NULLS indicates whether we have COUNT aggregate (i.e. not
// COUNT_ROWS) and there maybe NULLs.
func _ACCUMULATE_COUNT(a *countAgg, nulls *coldata.Nulls, i int, _COL_WITH_NULLS bool) { // */}}
	// {{define "accumulateCount" -}}

	if a.groups[i] {
		if a.curIdx != -1 {
			a.vec[a.curIdx] = a.curAgg
		}
		a.curIdx++
		a.curAgg = int64(0)
	}
	var y int64
	// {{if .ColWithNulls}}
	y = int64(0)
	if !nulls.NullAt(i) {
		y = 1
	}
	// {{else}}
	y = int64(1)
	// {{end}}
	a.curAgg += y
	// {{end}}

	// {{/*
} // */}}

func (a *countAgg) HandleEmptyInputScalar() {
	a.vec[0] = 0
}
