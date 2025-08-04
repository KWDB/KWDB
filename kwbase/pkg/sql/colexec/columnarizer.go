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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// Columnarizer turns an execinfra.RowSource input into an Operator output, by
// reading the input in chunks of size coldata.BatchSize() and converting each
// chunk into a coldata.Batch column by column.
type Columnarizer struct {
	execinfra.ProcessorBase
	NonExplainable

	// mu is used to protect against concurrent DrainMeta and Next calls, which
	// are currently allowed.
	// TODO(asubiotto): Explore calling DrainMeta from the same goroutine as Next,
	//  which will simplify this model.
	mu syncutil.Mutex

	allocator  *Allocator
	input      execinfra.RowSource
	da         sqlbase.DatumAlloc
	initStatus OperatorInitStatus

	buffered        sqlbase.EncDatumRows
	batch           coldata.Batch
	accumulatedMeta []execinfrapb.ProducerMetadata
	ctx             context.Context
	typs            []coltypes.T
}

var _ Operator = &Columnarizer{}

// NewColumnarizer returns a new Columnarizer.
func NewColumnarizer(
	ctx context.Context,
	allocator *Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) (*Columnarizer, error) {
	var err error
	c := &Columnarizer{
		allocator: allocator,
		input:     input,
		ctx:       ctx,
	}
	if err = c.ProcessorBase.Init(
		nil,
		&execinfrapb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* output */
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{input}},
	); err != nil {
		return nil, err
	}
	c.typs, err = typeconv.FromColumnTypes(c.OutputTypes())

	return c, err
}

// Init is part of the Operator interface.
func (c *Columnarizer) Init() {
	// We don't want to call Start on the input to columnarizer and allocating
	// internal objects several times if Init method is called more than once, so
	// we have this check in place.
	if c.initStatus == OperatorNotInitialized {
		c.batch = c.allocator.NewMemBatch(c.typs)
		c.buffered = make(sqlbase.EncDatumRows, coldata.BatchSize())
		for i := range c.buffered {
			c.buffered[i] = make(sqlbase.EncDatumRow, len(c.typs))
		}
		c.accumulatedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)
		c.input.Start(c.ctx)
		c.initStatus = OperatorInitialized
	}
}

// Next is part of the Operator interface.
func (c *Columnarizer) Next(context.Context) coldata.Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batch.ResetInternalBatch()
	// Buffer up n rows.
	nRows := 0
	columnTypes := c.OutputTypes()
	for ; nRows < coldata.BatchSize(); nRows++ {
		row, meta := c.input.Next()
		if meta != nil {
			c.accumulatedMeta = append(c.accumulatedMeta, *meta)
			nRows--
			continue
		}
		if row == nil {
			break
		}
		// TODO(jordan): evaluate whether it's more efficient to skip the buffer
		// phase.
		copy(c.buffered[nRows], row)
	}

	// Write each column into the output batch.
	for idx, ct := range columnTypes {
		err := EncDatumRowsToColVec(c.allocator, c.buffered[:nRows], c.batch.ColVec(idx), idx, &ct, &c.da)
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
	}
	c.batch.SetLength(nRows)
	return c.batch
}

// Run is part of the execinfra.Processor interface.
//
// Columnarizers are not expected to be Run, so we prohibit calling this method
// on them.
func (c *Columnarizer) Run(context.Context) execinfra.RowStats {
	execerror.VectorizedInternalPanic("Columnarizer should not be Run")
	return execinfra.RowStats{}
}

// RunShortCircuit is part of the Processor interface.
func (c *Columnarizer) RunShortCircuit(context.Context, execinfra.Processor) error {
	return nil
}

var _ Operator = &Columnarizer{}
var _ execinfrapb.MetadataSource = &Columnarizer{}

// DrainMeta is part of the MetadataSource interface.
func (c *Columnarizer) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MoveToDraining(nil /* err */)
	for {
		meta := c.DrainHelper()
		if meta == nil {
			break
		}
		c.accumulatedMeta = append(c.accumulatedMeta, *meta)
	}
	return c.accumulatedMeta
}

// ChildCount is part of the Operator interface.
func (c *Columnarizer) ChildCount(verbose bool) int {
	if _, ok := c.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the Operator interface.
func (c *Columnarizer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := c.input.(execinfra.OpNode); ok {
			return n
		}
		execerror.VectorizedInternalPanic("input to Columnarizer is not an execinfra.OpNode")
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
