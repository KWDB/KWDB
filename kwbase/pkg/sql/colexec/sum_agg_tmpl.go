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
// +build execgen_template
//
// This file is the execgen template for sum_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _ASSIGN_ADD is the template addition function for assigning the first input
// to the result of the second input + the third input.
func _ASSIGN_ADD(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

func newSumAgg(allocator *Allocator, t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		allocator.AdjustMemoryUsage(int64(sizeOfSum_TYPEAgg))
		return &sum_TYPEAgg{}, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sum agg type %s", t)
	}
}

// {{range .}}

type sum_TYPEAgg struct {
	done bool

	groups  []bool
	scratch struct {
		curIdx int
		// curAgg holds the running total, so we can index into the slice once per
		// group, instead of on each iteration.
		curAgg _GOTYPE
		// vec points to the output vector we are updating.
		vec []_GOTYPE
		// nulls points to the output null vector that we are updating.
		nulls *coldata.Nulls
		// foundNonNullForCurrentGroup tracks if we have seen any non-null values
		// for the group that is currently being aggregated.
		foundNonNullForCurrentGroup bool
	}
}

var _ aggregateFunc = &sum_TYPEAgg{}

const sizeOfSum_TYPEAgg = unsafe.Sizeof(sum_TYPEAgg{})

func (a *sum_TYPEAgg) Init(groups []bool, v coldata.Vec) {
	a.groups = groups
	a.scratch.vec = v._TemplateType()
	a.scratch.nulls = v.Nulls()
	a.Reset()
}

func (a *sum_TYPEAgg) Reset() {
	a.scratch.curIdx = -1
	a.scratch.foundNonNullForCurrentGroup = false
	a.scratch.nulls.UnsetNulls()
	a.done = false
}

func (a *sum_TYPEAgg) CurrentOutputIndex() int {
	return a.scratch.curIdx
}

func (a *sum_TYPEAgg) SetOutputIndex(idx int) {
	if a.scratch.curIdx != -1 {
		a.scratch.curIdx = idx
		a.scratch.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *sum_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// The aggregation is finished. Flush the last value. If we haven't found
		// any non-nulls for this group so far, the output for this group should be
		// null.
		if !a.scratch.foundNonNullForCurrentGroup {
			a.scratch.nulls.SetNull(a.scratch.curIdx)
		} else {
			a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
		}
		a.scratch.curIdx++
		a.done = true
		return
	}
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TemplateType(), vec.Nulls()
	if nulls.MaybeHasNulls() {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, true)
			}
		}
	} else {
		if sel != nil {
			sel = sel[:inputLen]
			for _, i := range sel {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		} else {
			col = col[:inputLen]
			for i := range col {
				_ACCUMULATE_SUM(a, nulls, i, false)
			}
		}
	}
}

func (a *sum_TYPEAgg) HandleEmptyInputScalar() {
	a.scratch.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_SUM adds the value of the ith row to the output for the current
// group. If this is the first row of a new group, and no non-nulls have been
// found for the current group, then the output for the current group is set to
// null.
func _ACCUMULATE_SUM(a *sum_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}

	// {{define "accumulateSum"}}
	if a.groups[i] {
		// If we encounter a new group, and we haven't found any non-nulls for the
		// current group, the output for this group should be null. If
		// a.scratch.curIdx is negative, it means that this is the first group.
		if a.scratch.curIdx >= 0 {
			if !a.scratch.foundNonNullForCurrentGroup {
				a.scratch.nulls.SetNull(a.scratch.curIdx)
			} else {
				a.scratch.vec[a.scratch.curIdx] = a.scratch.curAgg
			}
		}
		a.scratch.curIdx++
		// {{with .Global}}
		a.scratch.curAgg = zero_TemplateTypeValue
		// {{end}}

		// {{/*
		// We only need to reset this flag if there are nulls. If there are no
		// nulls, this will be updated unconditionally below.
		// */}}
		// {{ if .HasNulls }}
		a.scratch.foundNonNullForCurrentGroup = false
		// {{ end }}
	}
	var isNull bool
	// {{ if .HasNulls }}
	isNull = nulls.NullAt(i)
	// {{ else }}
	isNull = false
	// {{ end }}
	if !isNull {
		_ASSIGN_ADD(a.scratch.curAgg, a.scratch.curAgg, col[i])
		a.scratch.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
