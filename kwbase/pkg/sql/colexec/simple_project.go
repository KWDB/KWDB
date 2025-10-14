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

package colexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// simpleProjectOp is an operator that implements "simple projection" - removal of
// columns that aren't needed by later operators.
type simpleProjectOp struct {
	OneInputNode
	NonExplainable
	closerHelper

	projection []uint32
	batches    map[coldata.Batch]*projectingBatch
	// numBatchesLoggingThreshold is the threshold on the number of items in
	// 'batches' map at which we will log a message when a new projectingBatch
	// is created. It is growing exponentially.
	numBatchesLoggingThreshold int
}

var _ closableOperator = &simpleProjectOp{}

// projectingBatch is a Batch that applies a simple projection to another,
// underlying batch, discarding all columns but the ones in its projection
// slice, in order.
type projectingBatch struct {
	coldata.Batch

	projection []uint32
	// colVecs is a lazily populated slice of coldata.Vecs to support returning
	// these in ColVecs().
	colVecs []coldata.Vec
}

func newProjectionBatch(projection []uint32) *projectingBatch {
	p := &projectingBatch{
		projection: make([]uint32, len(projection)),
	}
	// We make a copy of projection to be safe.
	copy(p.projection, projection)
	return p
}

func (b *projectingBatch) ColVec(i int) coldata.Vec {
	return b.Batch.ColVec(int(b.projection[i]))
}

func (b *projectingBatch) ColVecs() []coldata.Vec {
	if b.Batch == coldata.ZeroBatch {
		return nil
	}
	if b.colVecs == nil || len(b.colVecs) != len(b.projection) {
		b.colVecs = make([]coldata.Vec, len(b.projection))
	}
	for i := range b.colVecs {
		b.colVecs[i] = b.Batch.ColVec(int(b.projection[i]))
	}
	return b.colVecs
}

func (b *projectingBatch) Width() int {
	return len(b.projection)
}

func (b *projectingBatch) AppendCol(col coldata.Vec) {
	b.Batch.AppendCol(col)
	b.projection = append(b.projection, uint32(b.Batch.Width())-1)
}

func (b *projectingBatch) ReplaceCol(col coldata.Vec, idx int) {
	b.Batch.ReplaceCol(col, int(b.projection[idx]))
}

// NewSimpleProjectOp returns a new simpleProjectOp that applies a simple
// projection on the columns in its input batch, returning a new batch with
// only the columns in the projection slice, in order. In a degenerate case
// when input already outputs batches that satisfy the projection, a
// simpleProjectOp is not planned and input is returned.
func NewSimpleProjectOp(input Operator, numInputCols int, projection []uint32) Operator {
	if numInputCols == len(projection) {
		projectionIsRedundant := true
		for i := range projection {
			if projection[i] != uint32(i) {
				projectionIsRedundant = false
			}
		}
		if projectionIsRedundant {
			return input
		}
	}
	s := &simpleProjectOp{
		OneInputNode:               NewOneInputNode(input),
		projection:                 make([]uint32, len(projection)),
		batches:                    make(map[coldata.Batch]*projectingBatch),
		numBatchesLoggingThreshold: 128,
	}
	// We make a copy of projection to be safe.
	copy(s.projection, projection)
	return s
}

func (d *simpleProjectOp) Init() {
	d.input.Init()
}

func (d *simpleProjectOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	projBatch, found := d.batches[batch]
	if !found {
		projBatch = newProjectionBatch(d.projection)
		d.batches[batch] = projBatch
		if len(d.batches) == d.numBatchesLoggingThreshold {
			if log.V(1) {
				log.Infof(ctx, "simpleProjectOp: size of 'batches' map = %d", len(d.batches))
			}
			d.numBatchesLoggingThreshold = d.numBatchesLoggingThreshold * 2
		}
	}
	projBatch.Batch = batch
	return projBatch
}

// Close closes the simpleProjectOp's input.
// TODO(asubiotto): Remove this method. It only exists so that we can call Close
//
//	from some runTests subtests when not draining the input fully. The test
//	should pass in the testing.T object used so that the caller can decide to
//	explicitly close the input after checking the test.
func (d *simpleProjectOp) IdempotentClose(ctx context.Context) error {
	if !d.close() {
		return nil
	}
	if c, ok := d.input.(IdempotentCloser); ok {
		return c.IdempotentClose(ctx)
	}
	return nil
}
