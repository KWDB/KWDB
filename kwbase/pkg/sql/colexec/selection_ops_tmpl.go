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
// This file is the execgen template for selection_ops.eg.go. It's formatted in
// a special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"context"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"
)

// {{/*
// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// Dummy import to pull in "coltypes" package.
var _ = coltypes.Bool

// _ASSIGN_CMP is the template function for assigning the result of comparing
// the second input to the third input into the first input.
func _ASSIGN_CMP(_, _, _ interface{}) int {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{/*
func _SEL_CONST_LOOP(_HAS_NULLS bool) { // */}}
	// {{define "selConstLoop" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			var cmp bool
			arg := execgen.UNSAFEGET(col, i)
			_ASSIGN_CMP(cmp, arg, p.constArg)
			// {{if _HAS_NULLS}}
			isNull := nulls.NullAt(i)
			// {{else}}
			isNull := false
			// {{end}}
			if cmp && !isNull {
				sel[idx] = i
				idx++
			}
		}
	} else {
		batch.SetSelection(true)
		sel := batch.Selection()
		col = execgen.SLICE(col, 0, n)
		for execgen.RANGE(i, col, 0, n) {
			var cmp bool
			arg := execgen.UNSAFEGET(col, i)
			_ASSIGN_CMP(cmp, arg, p.constArg)
			// {{if _HAS_NULLS}}
			isNull := nulls.NullAt(i)
			// {{else}}
			isNull := false
			// {{end}}
			if cmp && !isNull {
				sel[idx] = i
				idx++
			}
		}
	}
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// {{/*
func _SEL_LOOP(_HAS_NULLS bool) { // */}}
	// {{define "selLoop" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			var cmp bool
			arg1 := execgen.UNSAFEGET(col1, i)
			arg2 := _R_UNSAFEGET(col2, i)
			_ASSIGN_CMP(cmp, arg1, arg2)
			// {{if _HAS_NULLS}}
			isNull := nulls.NullAt(i)
			// {{else}}
			isNull := false
			// {{end}}
			if cmp && !isNull {
				sel[idx] = i
				idx++
			}
		}
	} else {
		batch.SetSelection(true)
		sel := batch.Selection()
		// {{if not (eq .LTyp.String "Bytes")}}
		// {{/* Slice is a noop for Bytes type, so col1Len below might contain an
		// incorrect value. In order to keep bounds check elimination for all other
		// types, we simply omit this code snippet for Bytes. */}}
		col1 = execgen.SLICE(col1, 0, n)
		col1Len := execgen.LEN(col1)
		col2 = _R_SLICE(col2, 0, col1Len)
		// {{end}}
		for execgen.RANGE(i, col1, 0, n) {
			var cmp bool
			arg1 := execgen.UNSAFEGET(col1, i)
			arg2 := _R_UNSAFEGET(col2, i)
			_ASSIGN_CMP(cmp, arg1, arg2)
			// {{if _HAS_NULLS}}
			isNull := nulls.NullAt(i)
			// {{else}}
			isNull := false
			// {{end}}
			if cmp && !isNull {
				sel[idx] = i
				idx++
			}
		}
	}
	// {{end}}
	// {{end}}
	// {{/*
} // */}}

// selConstOpBase contains all of the fields for binary selections with a
// constant, except for the constant itself.
type selConstOpBase struct {
	OneInputNode
	colIdx         int
	decimalScratch decimalOverloadScratch
}

// selOpBase contains all of the fields for non-constant binary selections.
type selOpBase struct {
	OneInputNode
	col1Idx        int
	col2Idx        int
	decimalScratch decimalOverloadScratch
}

// {{define "selConstOp"}}
type _OP_CONST_NAME struct {
	selConstOpBase
	constArg _R_GO_TYPE
}

func (p *_OP_CONST_NAME) Next(ctx context.Context) coldata.Batch {
	// In order to inline the templated code of overloads, we need to have a
	// `decimalScratch` local variable of type `decimalOverloadScratch`.
	decimalScratch := p.decimalScratch
	// However, the scratch is not used in all of the selection operators, so
	// we add this to go around "unused" error.
	_ = decimalScratch
	for {
		batch := p.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec := batch.ColVec(p.colIdx)
		col := vec._L_TYP()
		var idx int
		n := batch.Length()
		if vec.MaybeHasNulls() {
			nulls := vec.Nulls()
			_SEL_CONST_LOOP(true)
		} else {
			_SEL_CONST_LOOP(false)
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (p *_OP_CONST_NAME) Init() {
	p.input.Init()
}

// {{end}}

// {{define "selOp"}}
type _OP_NAME struct {
	selOpBase
}

func (p *_OP_NAME) Next(ctx context.Context) coldata.Batch {
	// In order to inline the templated code of overloads, we need to have a
	// `decimalScratch` local variable of type `decimalOverloadScratch`.
	decimalScratch := p.decimalScratch
	// However, the scratch is not used in all of the selection operators, so
	// we add this to go around "unused" error.
	_ = decimalScratch
	for {
		batch := p.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec1 := batch.ColVec(p.col1Idx)
		vec2 := batch.ColVec(p.col2Idx)
		col1 := vec1._L_TYP()
		col2 := vec2._R_TYP()
		n := batch.Length()

		var idx int
		if vec1.MaybeHasNulls() || vec2.MaybeHasNulls() {
			nulls := vec1.Nulls().Or(vec2.Nulls())
			_SEL_LOOP(true)
		} else {
			_SEL_LOOP(false)
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (p *_OP_NAME) Init() {
	p.input.Init()
}

// {{end}}

// {{/*
// The outer range is a coltypes.T (the left type). The middle range is also a
// coltypes.T (the right type). The inner is the overloads associated with
// those two types.
// */}}
// {{range .}}
// {{range .}}
// {{range .}}
// {{template "selConstOp" .}}
// {{template "selOp" .}}
// {{end}}
// {{end}}
// {{end}}

// GetSelectionConstOperator returns the appropriate constant selection operator
// for the given left and right column types and comparison.
func GetSelectionConstOperator(
	leftColType *types.T,
	constColType *types.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
) (Operator, error) {
	c, err := typeconv.GetDatumToPhysicalFn(constColType)(constArg)
	if err != nil {
		return nil, err
	}
	selConstOpBase := selConstOpBase{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
	}
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	// {{range $lTyp, $rTypToOverloads := .}}
	case coltypes._L_TYP_VAR:
		switch rightType := typeconv.FromColumnType(constColType); rightType {
		// {{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes._R_TYP_VAR:
			switch cmpOp {
			// {{range $overloads}}
			case tree._NAME:
				return &_OP_CONST_NAME{selConstOpBase: selConstOpBase, constArg: c.(_R_GO_TYPE)}, nil
				// {{end}}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
			}
			// {{end}}
		default:
			return nil, errors.Errorf("unhandled right type: %s", rightType)
		}
		// {{end}}
	default:
		return nil, errors.Errorf("unhandled left type: %s", leftType)
	}
}

// GetSelectionOperator returns the appropriate two column selection operator
// for the given left and right column types and comparison.
func GetSelectionOperator(
	leftColType *types.T,
	rightColType *types.T,
	cmpOp tree.ComparisonOperator,
	input Operator,
	col1Idx int,
	col2Idx int,
) (Operator, error) {
	selOpBase := selOpBase{
		OneInputNode: NewOneInputNode(input),
		col1Idx:      col1Idx,
		col2Idx:      col2Idx,
	}
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	// {{range $lTyp, $rTypToOverloads := .}}
	case coltypes._L_TYP_VAR:
		switch rightType := typeconv.FromColumnType(rightColType); rightType {
		// {{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes._R_TYP_VAR:
			switch cmpOp {
			// {{range $overloads}}
			case tree._NAME:
				return &_OP_NAME{selOpBase: selOpBase}, nil
				// {{end }}
			default:
				return nil, errors.Errorf("unhandled comparison operator: %s", cmpOp)
			}
			// {{end}}
		default:
			return nil, errors.Errorf("unhandled right type: %s", rightType)
		}
		// {{end}}
	default:
		return nil, errors.Errorf("unhandled left type: %s", leftType)
	}
}
