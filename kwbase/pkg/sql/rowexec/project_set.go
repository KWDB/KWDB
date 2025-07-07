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

package rowexec

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// projectSetProcessor is the physical processor implementation of
// projectSetNode.
type projectSetProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	spec  *execinfrapb.ProjectSetSpec

	// exprHelpers are the constant-folded, type checked expressions specified
	// in the ROWS FROM syntax. This can contain many kinds of expressions
	// (anything that is "function-like" including COALESCE, NULLIF) not just
	// SRFs.
	exprHelpers []*execinfra.ExprHelper

	// funcs contains a valid pointer to a SRF FuncExpr for every entry
	// in `exprHelpers` that is actually a SRF function application.
	// The size of the slice is the same as `exprHelpers` though.
	funcs []*tree.FuncExpr

	// inputRowReady is set when there was a row of input data available
	// from the source.
	inputRowReady bool

	// RowBuffer will contain the current row of results.
	rowBuffer sqlbase.EncDatumRow

	// gens contains the current "active" ValueGenerators for each entry
	// in `funcs`. They are initialized anew for every new row in the source.
	gens []tree.ValueGenerator

	// done indicates for each `Expr` whether the values produced by
	// either the SRF or the scalar expressions are fully consumed and
	// thus also whether NULLs should be emitted instead.
	done []bool

	// emitCount is used to track the number of rows that have been
	// emitted from Next().
	emitCount int64
}

var _ execinfra.Processor = &projectSetProcessor{}
var _ execinfra.RowSource = &projectSetProcessor{}
var _ execinfra.OpNode = &projectSetProcessor{}

const projectSetProcName = "projectSet"

func newProjectSetProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ProjectSetSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*projectSetProcessor, error) {
	outputTypes := append(input.OutputTypes(), spec.GeneratedColumns...)
	ps := &projectSetProcessor{
		input:       input,
		spec:        spec,
		exprHelpers: make([]*execinfra.ExprHelper, len(spec.Exprs)),
		funcs:       make([]*tree.FuncExpr, len(spec.Exprs)),
		rowBuffer:   make(sqlbase.EncDatumRow, len(outputTypes)),
		gens:        make([]tree.ValueGenerator, len(spec.Exprs)),
		done:        make([]bool, len(spec.Exprs)),
	}
	if err := ps.Init(
		ps,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{ps.input}},
	); err != nil {
		return nil, err
	}
	return ps, nil
}

// InitProcessorProcedure init processor in procedure
func (ps *projectSetProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if ps.EvalCtx.IsProcedure {
		if ps.FlowCtx != nil {
			ps.FlowCtx.Txn = txn
		}
		ps.Closed = false
		ps.State = execinfra.StateRunning
		ps.Out.SetRowIdx(0)
	}
}

// Start is part of the RowSource interface.
func (ps *projectSetProcessor) Start(ctx context.Context) context.Context {
	ps.input.Start(ctx)
	ctx = ps.StartInternal(ctx, projectSetProcName)

	// Initialize exprHelpers.
	for i, expr := range ps.spec.Exprs {
		var helper execinfra.ExprHelper
		err := helper.Init(expr, ps.input.OutputTypes(), ps.EvalCtx)
		if err != nil {
			ps.MoveToDraining(err)
			return ctx
		}
		if tFunc, ok := helper.Expr.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Expr is a set-generating function.
			ps.funcs[i] = tFunc
		}
		ps.exprHelpers[i] = &helper
	}
	return ctx
}

// nextInputRow returns the next row or metadata from ps.input. It also
// initializes the value generators for that row.
func (ps *projectSetProcessor) nextInputRow() (
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
	error,
) {
	row, meta := ps.input.Next()
	if row == nil {
		return nil, meta, nil
	}

	// Initialize a round of SRF generators or scalar values.
	for i := range ps.exprHelpers {
		if fn := ps.funcs[i]; fn != nil {
			// A set-generating function. Prepare its ValueGenerator.

			// Set ExprHelper.row so that we can use it as an IndexedVarContainer.
			ps.exprHelpers[i].Row = row

			ps.EvalCtx.IVarContainer = ps.exprHelpers[i]
			gen, err := fn.EvalArgsAndGetGenerator(ps.EvalCtx)
			if err != nil {
				return nil, nil, err
			}
			if gen == nil {
				gen = builtins.EmptyGenerator()
			}
			if err := gen.Start(ps.PbCtx(), ps.FlowCtx.Txn); err != nil {
				return nil, nil, err
			}
			ps.gens[i] = gen
		}
		ps.done[i] = false
	}

	return row, nil, nil
}

// nextGeneratorValues populates the row buffer with the next set of generated
// values. It returns true if any of the generators produce new values.
func (ps *projectSetProcessor) nextGeneratorValues() (newValAvail bool, err error) {
	colIdx := len(ps.input.OutputTypes())
	for i := range ps.exprHelpers {
		// Do we have a SRF?
		if gen := ps.gens[i]; gen != nil {
			// Yes. Is there still work to do for the current row?
			numCols := int(ps.spec.NumColsPerGen[i])
			if !ps.done[i] {
				// Yes; check whether this source still has some values available.
				hasVals, err := gen.Next(ps.PbCtx())
				if err != nil {
					return false, err
				}
				if hasVals {
					// This source has values, use them.
					for _, value := range gen.Values() {
						ps.rowBuffer[colIdx] = ps.toEncDatum(value, colIdx)
						colIdx++
					}
					newValAvail = true
				} else {
					ps.done[i] = true
					// No values left. Fill the buffer with NULLs for future results.
					for j := 0; j < numCols; j++ {
						ps.rowBuffer[colIdx] = ps.toEncDatum(tree.DNull, colIdx)
						colIdx++
					}
				}
			} else {
				// Already done. Increment colIdx.
				colIdx += numCols
			}
		} else {
			// A simple scalar result.
			// Do we still need to produce the scalar value? (first row)
			if !ps.done[i] {
				// Yes. Produce it once, then indicate it's "done".
				value, err := ps.exprHelpers[i].Eval(ps.rowBuffer)
				if err != nil {
					return false, err
				}
				ps.rowBuffer[colIdx] = ps.toEncDatum(value, colIdx)
				colIdx++
				newValAvail = true
				ps.done[i] = true
			} else {
				// Ensure that every row after the first returns a NULL value.
				ps.rowBuffer[colIdx] = ps.toEncDatum(tree.DNull, colIdx)
				colIdx++
			}
		}
	}
	return newValAvail, nil
}

// Next is part of the RowSource interface.
func (ps *projectSetProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	const cancelCheckCount = 10000

	for ps.State == execinfra.StateRunning {

		// Occasionally check for cancellation.
		ps.emitCount++
		if ps.emitCount%cancelCheckCount == 0 {
			if err := ps.PbCtx().Err(); err != nil {
				ps.MoveToDraining(err)
				return nil, ps.DrainHelper()
			}
		}

		// Start of a new row of input?
		if !ps.inputRowReady {
			// Read the row from the source.
			row, meta, err := ps.nextInputRow()
			if meta != nil {
				if meta.Err != nil {
					ps.MoveToDraining(nil /* err */)
				}
				return nil, meta
			}
			if err != nil {
				ps.MoveToDraining(err)
				return nil, ps.DrainHelper()
			}
			if row == nil {
				ps.MoveToDraining(nil /* err */)
				return nil, ps.DrainHelper()
			}

			// Keep the values for later.
			copy(ps.rowBuffer, row)
			ps.inputRowReady = true
		}

		// Try to find some data on the generator side.
		newValAvail, err := ps.nextGeneratorValues()
		if err != nil {
			ps.MoveToDraining(err)
			return nil, ps.DrainHelper()
		}
		if newValAvail {
			if outRow := ps.ProcessRowHelper(ps.rowBuffer); outRow != nil {
				return outRow, nil
			}
		} else {
			// The current batch of SRF values was exhausted. Advance
			// to the next input row.
			ps.inputRowReady = false
		}
	}
	return nil, ps.DrainHelper()
}

func (ps *projectSetProcessor) toEncDatum(d tree.Datum, colIdx int) sqlbase.EncDatum {
	generatedColIdx := colIdx - len(ps.input.OutputTypes())
	ctyp := &ps.spec.GeneratedColumns[generatedColIdx]
	return sqlbase.DatumToEncDatum(ctyp, d)
}

// ConsumerClosed is part of the RowSource interface.
func (ps *projectSetProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ps.InternalClose()
}

// ChildCount is part of the execinfra.OpNode interface.
func (ps *projectSetProcessor) ChildCount(verbose bool) int {
	if _, ok := ps.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ps *projectSetProcessor) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ps.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to projectSetProcessor is not an execinfra.OpNode")

	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
