// Copyright 2019 The Cockroach Authors.
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
// This file is the execgen template for vec_comparators.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _COMPARE is the template equality function for assigning the first input
// to the result of comparing second and third inputs.
func _COMPARE(_, _, _ string) bool {
	execerror.VectorizedInternalPanic("")
}

// */}}

// vecComparator is a helper for the ordered synchronizer. It stores multiple
// column vectors of a single type and facilitates comparing values between
// them.
type vecComparator interface {
	// compare compares values from two vectors. vecIdx is the index of the vector
	// and valIdx is the index of the value in that vector to compare. Returns -1,
	// 0, or 1.
	compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 int) int

	// set sets the value of the vector at dstVecIdx at index dstValIdx to the value
	// at the vector at srcVecIdx at index srcValIdx.
	// NOTE: whenever set is used, the caller is responsible for updating the
	// memory accounts.
	set(srcVecIdx, dstVecIdx int, srcValIdx, dstValIdx int)

	// setVec updates the vector at idx.
	setVec(idx int, vec coldata.Vec)
}

// {{range .}}
type _TYPEVecComparator struct {
	vecs  []_GOTYPESLICE
	nulls []*coldata.Nulls
}

func (c *_TYPEVecComparator) compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 int) int {
	n1 := c.nulls[vecIdx1].MaybeHasNulls() && c.nulls[vecIdx1].NullAt(valIdx1)
	n2 := c.nulls[vecIdx2].MaybeHasNulls() && c.nulls[vecIdx2].NullAt(valIdx2)
	if n1 && n2 {
		return 0
	} else if n1 {
		return -1
	} else if n2 {
		return 1
	}
	left := execgen.UNSAFEGET(c.vecs[vecIdx1], valIdx1)
	right := execgen.UNSAFEGET(c.vecs[vecIdx2], valIdx2)
	var cmp int
	_COMPARE(cmp, left, right)
	return cmp
}

func (c *_TYPEVecComparator) setVec(idx int, vec coldata.Vec) {
	c.vecs[idx] = vec._TYPE()
	c.nulls[idx] = vec.Nulls()
}

func (c *_TYPEVecComparator) set(srcVecIdx, dstVecIdx int, srcIdx, dstIdx int) {
	if c.nulls[srcVecIdx].MaybeHasNulls() && c.nulls[srcVecIdx].NullAt(srcIdx) {
		c.nulls[dstVecIdx].SetNull(dstIdx)
	} else {
		c.nulls[dstVecIdx].UnsetNull(dstIdx)
		// {{ if eq .LTyp.String "Bytes" }}
		// Since flat Bytes cannot be set at arbitrary indices (data needs to be
		// moved around), we use CopySlice to accept the performance hit.
		// Specifically, this is a performance hit because we are overwriting the
		// variable number of bytes in `dstVecIdx`, so we will have to either shift
		// the bytes after that element left or right, depending on how long the
		// source bytes slice is. Refer to the CopySlice comment for an example.
		execgen.COPYSLICE(c.vecs[dstVecIdx], c.vecs[srcVecIdx], dstIdx, srcIdx, srcIdx+1)
		// {{ else }}
		v := execgen.UNSAFEGET(c.vecs[srcVecIdx], srcIdx)
		execgen.SET(c.vecs[dstVecIdx], dstIdx, v)
		// {{ end }}
	}
}

// {{end}}

func GetVecComparator(t coltypes.T, numVecs int) vecComparator {
	switch t {
	// {{range .}}
	case coltypes._TYPE:
		return &_TYPEVecComparator{
			vecs:  make([]_GOTYPESLICE, numVecs),
			nulls: make([]*coldata.Nulls, numVecs),
		}
		// {{end}}
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %v", t))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
