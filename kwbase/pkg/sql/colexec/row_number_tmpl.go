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
// This file is the execgen template for row_number.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

// TODO(yuzefovich): add benchmarks.

// NewRowNumberOperator creates a new Operator that computes window function
// ROW_NUMBER. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
func NewRowNumberOperator(
	allocator *Allocator, input Operator, outputColIdx int, partitionColIdx int,
) Operator {
	input = newVectorTypeEnforcer(allocator, input, coltypes.Int64, outputColIdx)
	base := rowNumberBase{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
	}
	if partitionColIdx == -1 {
		return &rowNumberNoPartitionOp{base}
	}
	return &rowNumberWithPartitionOp{base}
}

// rowNumberBase extracts common fields and common initialization of two
// variations of row number operators. Note that it is not an operator itself
// and should not be used directly.
type rowNumberBase struct {
	OneInputNode
	allocator       *Allocator
	outputColIdx    int
	partitionColIdx int

	rowNumber int64
}

func (r *rowNumberBase) Init() {
	r.Input().Init()
}

// {{ range . }}

type _ROW_NUMBER_STRINGOp struct {
	rowNumberBase
}

var _ Operator = &_ROW_NUMBER_STRINGOp{}

func (r *_ROW_NUMBER_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	if batch.Length() == 0 {
		return coldata.ZeroBatch
	}

	// {{ if .HasPartition }}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{ end }}
	rowNumberCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	if sel != nil {
		for i := 0; i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[sel[i]] {
				r.rowNumber = 1
			}
			// {{ end }}
			r.rowNumber++
			rowNumberCol[sel[i]] = r.rowNumber
		}
	} else {
		for i := 0; i < batch.Length(); i++ {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rowNumber = 0
			}
			// {{ end }}
			r.rowNumber++
			rowNumberCol[i] = r.rowNumber
		}
	}
	return batch
}

// {{ end }}
