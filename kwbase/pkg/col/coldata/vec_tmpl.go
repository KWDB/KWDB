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
// This file is the execgen template for vec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package coldata

import (
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
)

// {{/*

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// _GOTYPESLICE is a template Go type slice variable.
type _GOTYPESLICE interface{}

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// */}}

func (m *memColumn) Append(args SliceArgs) {
	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		// NOTE: it is unfortunate that we always append whole slice without paying
		// attention to whether the values are NULL. However, if we do start paying
		// attention, the performance suffers dramatically, so we choose to copy
		// over "actual" as well as "garbage" values.
		if args.Sel == nil {
			execgen.APPENDSLICE(toCol, fromCol, args.DestIdx, args.SrcStartIdx, args.SrcEndIdx)
		} else {
			sel := args.Sel[args.SrcStartIdx:args.SrcEndIdx]
			// {{if eq .LTyp.String "Bytes"}}
			// We need to truncate toCol before appending to it, so in case of Bytes,
			// we append an empty slice.
			execgen.APPENDSLICE(toCol, toCol, args.DestIdx, 0, 0)
			// We will be getting all values below to be appended, regardless of
			// whether the value is NULL. It is possible that Bytes' invariant of
			// non-decreasing offsets on the source is currently not maintained, so
			// we explicitly enforce it.
			maxIdx := 0
			for _, selIdx := range sel {
				if selIdx > maxIdx {
					maxIdx = selIdx
				}
			}
			fromCol.UpdateOffsetsToBeNonDecreasing(maxIdx + 1)
			// {{else}}
			toCol = execgen.SLICE(toCol, 0, args.DestIdx)
			// {{end}}
			for _, selIdx := range sel {
				val := execgen.UNSAFEGET(fromCol, selIdx)
				execgen.APPENDVAL(toCol, val)
			}
		}
		m.nulls.set(args)
		m.col = toCol
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

// {{/*
func _COPY_WITH_SEL(
	m *memColumn, args CopySliceArgs, fromCol, toCol _GOTYPESLICE, sel interface{}, _SEL_ON_DEST bool,
) { // */}}
	// {{define "copyWithSel" -}}
	if args.Src.MaybeHasNulls() {
		nulls := args.Src.Nulls()
		for i, selIdx := range sel[args.SrcStartIdx:args.SrcEndIdx] {
			if nulls.NullAt(selIdx) {
				// {{if .SelOnDest}}
				// Remove an unused warning in some cases.
				_ = i
				m.nulls.SetNull(selIdx)
				// {{else}}
				m.nulls.SetNull(i + args.DestIdx)
				// {{end}}
			} else {
				v := execgen.UNSAFEGET(fromCol, selIdx)
				// {{if .SelOnDest}}
				m.nulls.UnsetNull(selIdx)
				execgen.SET(toCol, selIdx, v)
				// {{else}}
				execgen.SET(toCol, i+args.DestIdx, v)
				// {{end}}
			}
		}
		return
	}
	// No Nulls.
	for i := range sel[args.SrcStartIdx:args.SrcEndIdx] {
		selIdx := sel[args.SrcStartIdx+i]
		v := execgen.UNSAFEGET(fromCol, selIdx)
		// {{if .SelOnDest}}
		execgen.SET(toCol, selIdx, v)
		// {{else}}
		execgen.SET(toCol, i+args.DestIdx, v)
		// {{end}}
	}
	// {{end}}
	// {{/*
}

// */}}

func (m *memColumn) Copy(args CopySliceArgs) {
	if !args.SelOnDest {
		// We're about to overwrite this entire range, so unset all the nulls.
		m.Nulls().UnsetNullRange(args.DestIdx, args.DestIdx+(args.SrcEndIdx-args.SrcStartIdx))
	}
	// } else {
	// SelOnDest indicates that we're applying the input selection vector as a lens
	// into the output vector as well. We'll set the non-nulls by hand below.
	// }

	switch args.ColType {
	// {{range .}}
	case _TYPES_T:
		fromCol := args.Src._TemplateType()
		toCol := m._TemplateType()
		if args.Sel != nil {
			sel := args.Sel
			if args.SelOnDest {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, true)
			} else {
				_COPY_WITH_SEL(m, args, sel, toCol, fromCol, false)
			}
			return
		}
		// No Sel.
		execgen.COPYSLICE(toCol, fromCol, args.DestIdx, args.SrcStartIdx, args.SrcEndIdx)
		m.nulls.set(args.SliceArgs)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %s", args.ColType))
	}
}

func (m *memColumn) Window(colType coltypes.T, start int, end int) Vec {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		col := m._TemplateType()
		return &memColumn{
			t:     colType,
			col:   execgen.WINDOW(col, start, end),
			nulls: m.nulls.Slice(start, end),
		}
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

// SetValueAt is an inefficient helper to set the value in a Vec when the type
// is unknown.
func SetValueAt(v Vec, elem interface{}, rowIdx int, colType coltypes.T) {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		target := v._TemplateType()
		newVal := elem.(_GOTYPE)
		execgen.SET(target, rowIdx, newVal)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}

// GetValueAt is an inefficient helper to get the value in a Vec when the type
// is unknown.
func GetValueAt(v Vec, rowIdx int, colType coltypes.T) interface{} {
	switch colType {
	// {{range .}}
	case _TYPES_T:
		target := v._TemplateType()
		return execgen.UNSAFEGET(target, rowIdx)
	// {{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", colType))
	}
}
