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
// This file is the execgen template for any_not_null_agg.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"
)

func newAnyNotNullAgg(allocator *Allocator, t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		allocator.AdjustMemoryUsage(int64(sizeOfAnyNotNull_TYPEAgg))
		return &anyNotNull_TYPEAgg{allocator: allocator}, nil
		// {{end}}
	default:
		return nil, errors.Errorf("unsupported any not null agg type %s", t)
	}
}

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _GOTYPESLICE is the template Go type slice variable for this operator. It
// will be replaced by the Go slice representation for each type in coltypes.T, for
// example []int64 for coltypes.Int64.
type _GOTYPESLICE interface{}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// */}}

// {{range .}}

// anyNotNull_TYPEAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
type anyNotNull_TYPEAgg struct {
	allocator                   *Allocator
	done                        bool
	groups                      []bool
	vec                         coldata.Vec
	col                         _GOTYPESLICE
	nulls                       *coldata.Nulls
	curIdx                      int
	curAgg                      _GOTYPE
	foundNonNullForCurrentGroup bool
}

var _ aggregateFunc = &anyNotNull_TYPEAgg{}

const sizeOfAnyNotNull_TYPEAgg = unsafe.Sizeof(anyNotNull_TYPEAgg{})

func (a *anyNotNull_TYPEAgg) Init(groups []bool, vec coldata.Vec) {
	a.groups = groups
	a.vec = vec
	a.col = vec._TemplateType()
	a.nulls = vec.Nulls()
	a.Reset()
}

func (a *anyNotNull_TYPEAgg) Reset() {
	a.curIdx = -1
	a.done = false
	a.foundNonNullForCurrentGroup = false
	a.nulls.UnsetNulls()
}

func (a *anyNotNull_TYPEAgg) CurrentOutputIndex() int {
	return a.curIdx
}

func (a *anyNotNull_TYPEAgg) SetOutputIndex(idx int) {
	if a.curIdx != -1 {
		a.curIdx = idx
		a.nulls.UnsetNullsAfter(idx + 1)
	}
}

func (a *anyNotNull_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32) {
	if a.done {
		return
	}
	inputLen := b.Length()
	if inputLen == 0 {
		// If we haven't found any non-nulls for this group so far, the output for
		// this group should be null.
		if !a.foundNonNullForCurrentGroup {
			a.nulls.SetNull(a.curIdx)
		} else {
			a.allocator.PerformOperation(
				[]coldata.Vec{a.vec},
				func() {
					execgen.SET(a.col, a.curIdx, a.curAgg)
				},
			)
		}
		a.curIdx++
		a.done = true
		return
	}
	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TemplateType(), vec.Nulls()
	// {{if eq .LTyp.String "Bytes"}}
	oldCurAggSize := len(a.curAgg)
	// {{end}}
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[:inputLen]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				} else {
					col = execgen.SLICE(col, 0, inputLen)
					for execgen.RANGE(i, col, 0, inputLen) {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				}
			}
		},
	)
	// {{if eq .LTyp.String "Bytes"}}
	a.allocator.AdjustMemoryUsage(int64(len(a.curAgg) - oldCurAggSize))
	// {{end}}
}

func (a *anyNotNull_TYPEAgg) HandleEmptyInputScalar() {
	a.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _FIND_ANY_NOT_NULL finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _FIND_ANY_NOT_NULL(a *anyNotNull_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "findAnyNotNull" -}}

	if a.groups[i] {
		// The `a.curIdx` check is necessary because for the first
		// group in the result set there is no "current group."
		if a.curIdx >= 0 {
			// If this is a new group, check if any non-nulls have been found for the
			// current group.
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				execgen.SET(a.col, a.curIdx, a.curAgg)
			}
		}
		a.curIdx++
		a.foundNonNullForCurrentGroup = false
	}
	var isNull bool
	// {{ if .HasNulls }}
	isNull = nulls.NullAt(i)
	// {{ else }}
	isNull = false
	// {{ end }}
	if !a.foundNonNullForCurrentGroup && !isNull {
		// If we haven't seen any non-nulls for the current group yet, and the
		// current value is non-null, then we can pick the current value to be the
		// output.
		val := execgen.UNSAFEGET(col, i)
		execgen.COPYVAL(a.curAgg, val)
		a.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
