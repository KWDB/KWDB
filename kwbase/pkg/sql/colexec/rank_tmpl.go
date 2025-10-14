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
// This file is the execgen template for rank.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	// {{/*
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	// */}}
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): add benchmarks.

// NewRankOperator creates a new Operator that computes window functions RANK
// or DENSE_RANK (depending on the passed in windowFn).
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewRankOperator(
	allocator *Allocator,
	input Operator,
	windowFn execinfrapb.WindowerSpec_WindowFunc,
	orderingCols []execinfrapb.Ordering_Column,
	outputColIdx int,
	partitionColIdx int,
	peersColIdx int,
) (Operator, error) {
	if len(orderingCols) == 0 {
		return NewConstOp(allocator, input, coltypes.Int64, int64(1), outputColIdx)
	}
	input = newVectorTypeEnforcer(allocator, input, coltypes.Int64, outputColIdx)
	initFields := rankInitFields{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
		peersColIdx:     peersColIdx,
	}
	switch windowFn {
	case execinfrapb.WindowerSpec_RANK:
		if partitionColIdx != columnOmitted {
			return &rankWithPartitionOp{rankInitFields: initFields}, nil
		}
		return &rankNoPartitionOp{rankInitFields: initFields}, nil
	case execinfrapb.WindowerSpec_DENSE_RANK:
		if partitionColIdx != columnOmitted {
			return &denseRankWithPartitionOp{rankInitFields: initFields}, nil
		}
		return &denseRankNoPartitionOp{rankInitFields: initFields}, nil
	default:
		return nil, errors.Errorf("unsupported rank type %s", windowFn)
	}
}

// {{/*

// _UPDATE_RANK is the template function for updating the state of rank
// operators.
func _UPDATE_RANK() {
	execerror.VectorizedInternalPanic("")
}

// _UPDATE_RANK_INCREMENT is the template function for updating the state of
// rank operators.
func _UPDATE_RANK_INCREMENT() {
	execerror.VectorizedInternalPanic("")
}

// */}}

type rankInitFields struct {
	OneInputNode

	allocator       *Allocator
	outputColIdx    int
	partitionColIdx int
	peersColIdx     int
}

// {{range .}}

type _RANK_STRINGOp struct {
	rankInitFields

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen.
	rankIncrement int64
}

var _ Operator = &_RANK_STRINGOp{}

func (r *_RANK_STRINGOp) Init() {
	r.Input().Init()
	// All rank functions start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *_RANK_STRINGOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input().Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	// {{if .HasPartition}}
	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	// {{end}}
	peersCol := batch.ColVec(r.peersColIdx).Bool()
	rankCol := batch.ColVec(r.outputColIdx).Int64()
	sel := batch.Selection()
	// TODO(yuzefovich): template out sel vs non-sel cases.
	if sel != nil {
		for _, i := range sel[:n] {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if peersCol[i] {
				_UPDATE_RANK()
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	} else {
		for i := range rankCol[:n] {
			// {{ if .HasPartition }}
			if partitionCol[i] {
				r.rank = 1
				r.rankIncrement = 1
				rankCol[i] = 1
				continue
			}
			// {{end}}
			if peersCol[i] {
				_UPDATE_RANK()
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				_UPDATE_RANK_INCREMENT()
			}
		}
	}
	return batch
}

// {{end}}
