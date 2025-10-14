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
// This file is the execgen template for rowstovec.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execgen"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
)

// {{/*

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

const (
	_FAMILY = types.Family(0)
	_WIDTH  = int32(0)
)

type _GOTYPE interface{}

func _ROWS_TO_COL_VEC(
	rows sqlbase.EncDatumRows, vec coldata.Vec, columnIdx int, alloc *sqlbase.DatumAlloc,
) error { // */}}
	// {{define "rowsToColVec" -}}
	col := vec._TemplateType()
	datumToPhysicalFn := typeconv.GetDatumToPhysicalFn(columnType)
	for i := range rows {
		row := rows[i]
		if row[columnIdx].Datum == nil {
			if err = row[columnIdx].EnsureDecoded(columnType, alloc); err != nil {
				return
			}
		}
		datum := row[columnIdx].Datum
		if datum == tree.DNull {
			vec.Nulls().SetNull(i)
		} else {
			v, err := datumToPhysicalFn(datum)
			if err != nil {
				return
			}

			castV := v.(_GOTYPE)
			execgen.SET(col, i, castV)
		}
	}
	// {{end}}
	// {{/*
}

// */}}

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	allocator *Allocator,
	rows sqlbase.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	columnType *types.T,
	alloc *sqlbase.DatumAlloc,
) error {
	var err error
	allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			switch columnType.Family() {
			// {{range .}}
			case _FAMILY:
				// {{ if .Widths }}
				switch columnType.Width() {
				// {{range .Widths}}
				case _WIDTH:
					_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
				// {{end}}
				default:
					execerror.VectorizedInternalPanic(fmt.Sprintf("unsupported width %d for column type %s", columnType.Width(), columnType.String()))
				}
				// {{ else }}
				_ROWS_TO_COL_VEC(rows, vec, columnIdx, columnType, alloc)
				// {{end}}
			// {{end}}
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unsupported column type %s", columnType.String()))
			}
		},
	)
	return err
}
