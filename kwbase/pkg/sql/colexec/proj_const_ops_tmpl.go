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
// +build execgen_template
//
// This file is the execgen template for proj_const_{left,right}_ops.eg.go.
// It's formatted in a special way, so it's both valid Go and a valid
// text/template input. This permits editing this file with editor support.
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
var _ coltypes.T

// _ASSIGN is the template function for assigning the first input to the result
// of computation an operation on the second and the third inputs.
func _ASSIGN(_, _, _ interface{}) {
	execerror.VectorizedInternalPanic("")
}

// _RET_UNSAFEGET is the template function that will be replaced by
// "execgen.UNSAFEGET" which uses _RET_TYP.
func _RET_UNSAFEGET(_, _ interface{}) interface{} {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{define "projConstOp" }}

type _OP_CONST_NAME struct {
	projConstOpBase
	// {{ if _IS_CONST_LEFT }}
	constArg _L_GO_TYPE
	// {{ else }}
	constArg _R_GO_TYPE
	// {{ end }}
}

func (p _OP_CONST_NAME) Next(ctx context.Context) coldata.Batch {
	// In order to inline the templated code of overloads, we need to have a
	// `decimalScratch` local variable of type `decimalOverloadScratch`.
	decimalScratch := p.decimalScratch
	// However, the scratch is not used in all of the projection operators, so
	// we add this to go around "unused" error.
	_ = decimalScratch
	batch := p.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(p.colIdx)
	// {{if _IS_CONST_LEFT}}
	col := vec._R_TYP()
	// {{else}}
	col := vec._L_TYP()
	// {{end}}
	projVec := batch.ColVec(p.outputIdx)
	projCol := projVec._RET_TYP()
	if vec.Nulls().MaybeHasNulls() {
		_SET_PROJECTION(true)
	} else {
		_SET_PROJECTION(false)
	}
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

func (p _OP_CONST_NAME) Init() {
	p.input.Init()
}

// {{end}}

// {{/*
func _SET_PROJECTION(_HAS_NULLS bool) {
	// */}}
	// {{define "setProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	// {{if _HAS_NULLS}}
	colNulls := vec.Nulls()
	// {{end}}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	} else {
		col = execgen.SLICE(col, 0, n)
		_ = _RET_UNSAFEGET(projCol, n-1)
		for execgen.RANGE(i, col, 0, n) {
			_SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS)
		}
	}
	// {{if _HAS_NULLS}}
	colNullsCopy := colNulls.Copy()
	projVec.SetNulls(&colNullsCopy)
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
func _SET_SINGLE_TUPLE_PROJECTION(_HAS_NULLS bool) { // */}}
	// {{define "setSingleTupleProjection" -}}
	// {{$hasNulls := $.HasNulls}}
	// {{with $.Overload}}
	// {{if _HAS_NULLS}}
	if !colNulls.NullAt(i) {
		// We only want to perform the projection operation if the value is not null.
		// {{end}}
		arg := execgen.UNSAFEGET(col, i)
		// {{if _IS_CONST_LEFT}}
		_ASSIGN(projCol[i], p.constArg, arg)
		// {{else}}
		_ASSIGN(projCol[i], arg, p.constArg)
		// {{end}}
		// {{if _HAS_NULLS }}
	}
	// {{end}}
	// {{end}}
	// {{end}}
	// {{/*
}

// */}}

// {{/*
// The outer range is a coltypes.T (the left type). The middle range is also a
// coltypes.T (the right type). The inner is the overloads associated with
// those two types.
// */}}
// {{range .}}
// {{range .}}
// {{range .}}

// {{template "projConstOp" .}}

// {{end}}
// {{end}}
// {{end}}

// GetProjection_CONST_SIDEConstOperator returns the appropriate constant
// projection operator for the given left and right column types and operation.
func GetProjection_CONST_SIDEConstOperator(
	allocator *Allocator,
	leftColType *types.T,
	rightColType *types.T,
	outputPhysType coltypes.T,
	op tree.Operator,
	input Operator,
	colIdx int,
	constArg tree.Datum,
	outputIdx int,
) (Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, outputPhysType, outputIdx)
	projConstOpBase := projConstOpBase{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    outputIdx,
	}
	// {{if _IS_CONST_LEFT}}
	c, err := typeconv.GetDatumToPhysicalFn(leftColType)(constArg)
	// {{else}}
	c, err := typeconv.GetDatumToPhysicalFn(rightColType)(constArg)
	// {{end}}
	if err != nil {
		return nil, err
	}
	switch leftType := typeconv.FromColumnType(leftColType); leftType {
	// {{range $lTyp, $rTypToOverloads := .}}
	case coltypes._L_TYP_VAR:
		switch rightType := typeconv.FromColumnType(rightColType); rightType {
		// {{range $rTyp, $overloads := $rTypToOverloads}}
		case coltypes._R_TYP_VAR:
			switch op.(type) {
			case tree.BinaryOperator:
				switch op {
				// {{range $overloads}}
				// {{if .IsBinOp}}
				case tree._NAME:
					return &_OP_CONST_NAME{
						projConstOpBase: projConstOpBase,
						// {{if _IS_CONST_LEFT}}
						constArg: c.(_L_GO_TYPE),
						// {{else}}
						constArg: c.(_R_GO_TYPE),
						// {{end}}
					}, nil
				// {{end}}
				// {{end}}
				default:
					return nil, errors.Errorf("unhandled binary operator: %s", op)
				}
			case tree.ComparisonOperator:
				switch op {
				// {{range $overloads}}
				// {{if .IsCmpOp}}
				case tree._NAME:
					return &_OP_CONST_NAME{
						projConstOpBase: projConstOpBase,
						// {{if _IS_CONST_LEFT}}
						constArg: c.(_L_GO_TYPE),
						// {{else}}
						constArg: c.(_R_GO_TYPE),
						// {{end}}
					}, nil
				// {{end}}
				// {{end}}
				default:
					return nil, errors.Errorf("unhandled comparison operator: %s", op)
				}
			default:
				return nil, errors.New("unhandled operator type")
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
