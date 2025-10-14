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
// This file is the execgen template for mergejoinbase.eg.go. It's formatted
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

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in coltypes.T, for example
// int64 for coltypes.Int64.
type _GOTYPE interface{}

// _ASSIGN_EQ is the template equality function for assigning the first input
// to the result of the the second input == the third input.
func _ASSIGN_EQ(_, _, _ interface{}) int {
	execerror.VectorizedInternalPanic("")
}

// */}}

// isBufferedGroupFinished checks to see whether or not the buffered group
// corresponding to input continues in batch.
func (o *mergeJoinBase) isBufferedGroupFinished(
	input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) bool {
	if batch.Length() == 0 {
		return true
	}
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	tupleToLookAtIdx := rowIdx
	sel := batch.Selection()
	if sel != nil {
		tupleToLookAtIdx = sel[rowIdx]
	}

	// Check all equality columns in the first row of batch to make sure we're in
	// the same group.
	for _, colIdx := range input.eqCols[:len(input.eqCols)] {
		colTyp := input.sourceTypes[colIdx]

		switch colTyp {
		// {{ range . }}
		case _TYPES_T:
			// We perform this null check on every equality column of the first
			// buffered tuple regardless of the join type since it is done only once
			// per batch. In some cases (like INNER JOIN, or LEFT OUTER JOIN with the
			// right side being an input) this check will always return false since
			// nulls couldn't be buffered up though.
			if bufferedGroup.firstTuple[colIdx].Nulls().NullAt(0) {
				return true
			}
			bufferedCol := bufferedGroup.firstTuple[colIdx]._TemplateType()
			prevVal := execgen.UNSAFEGET(bufferedCol, 0)
			var curVal _GOTYPE
			if batch.ColVec(int(colIdx)).MaybeHasNulls() && batch.ColVec(int(colIdx)).Nulls().NullAt(tupleToLookAtIdx) {
				return true
			}
			col := batch.ColVec(int(colIdx))._TemplateType()
			curVal = execgen.UNSAFEGET(col, tupleToLookAtIdx)
			var match bool
			_ASSIGN_EQ(match, prevVal, curVal)
			if !match {
				return true
			}
		// {{end}}
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled type %d", colTyp))
		}
	}
	return false
}
