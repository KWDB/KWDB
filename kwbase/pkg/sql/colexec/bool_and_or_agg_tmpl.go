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

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for bool_and_or_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the import
	// block. This was picked because it sorts after "pkg/sql/colexec/execerror" and
	// has no deps.
	_ "gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
)

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{range .}}

func newBool_OP_TYPEAgg(allocator *Allocator) aggregateFunc {
	allocator.AdjustMemoryUsage(int64(sizeOfBool_OP_TYPEAgg))
	return &bool_OP_TYPEAgg{}
}

type bool_OP_TYPEAgg struct {
	done       bool
	sawNonNull bool

	groups []bool
	vec    []bool

	nulls  *coldata.Nulls
	curIdx int
	curAgg bool
}

var _ aggregateFunc = &bool_OP_TYPEAgg{}

const sizeOfBool_OP_TYPEAgg = unsafe.Sizeof(bool_OP_TYPEAgg{})

func (b *bool_OP_TYPEAgg) Init(groups []bool, vec coldata.Vec) {
	b.groups = groups
	b.vec = vec.Bool()
	b.nulls = vec.Nulls()
	b.Reset()
}

func (b *bool_OP_TYPEAgg) Reset() {
	b.curIdx = -1
	b.nulls.UnsetNulls()
	b.done = false
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the _DEFAULT_VAL is true and for bool_or the _DEFAULT_VAL is false.
	b.curAgg = _DEFAULT_VAL
}

func (b *bool_OP_TYPEAgg) CurrentOutputIndex() int {
	return b.curIdx
}

func (b *bool_OP_TYPEAgg) SetOutputIndex(idx int) {
	if b.curIdx != -1 {
		b.curIdx = idx
		b.nulls.UnsetNullsAfter(idx)
	}
}

func (b *bool_OP_TYPEAgg) Compute(batch coldata.Batch, inputIdxs []uint32) {
	if b.done {
		return
	}
	inputLen := batch.Length()
	if inputLen == 0 {
		if !b.sawNonNull {
			b.nulls.SetNull(b.curIdx)
		} else {
			b.vec[b.curIdx] = b.curAgg
		}
		b.curIdx++
		b.done = true
		return
	}
	vec, sel := batch.ColVec(int(inputIdxs[0])), batch.Selection()
	col, nulls := vec.Bool(), vec.Nulls()
	if sel != nil {
		sel = sel[:inputLen]
		for _, i := range sel {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	} else {
		col = col[:inputLen]
		for i := range col {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	}
}

func (b *bool_OP_TYPEAgg) HandleEmptyInputScalar() {
	b.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(b *bool_OP_TYPEAgg, nulls *coldata.Nulls, i int) { // */}}
	// {{define "accumulateBoolean" -}}
	if b.groups[i] {
		if b.curIdx >= 0 {
			if !b.sawNonNull {
				b.nulls.SetNull(b.curIdx)
			} else {
				b.vec[b.curIdx] = b.curAgg
			}
		}
		b.curIdx++
		// {{with .Global}}
		b.curAgg = _DEFAULT_VAL
		// {{end}}
		b.sawNonNull = false
	}
	isNull := nulls.NullAt(i)
	if !isNull {
		// {{with .Global}}
		_ASSIGN_BOOL_OP(b.curAgg, b.curAgg, col[i])
		// {{end}}
		b.sawNonNull = true
	}

	// {{end}}

	// {{/*
} // */}}
