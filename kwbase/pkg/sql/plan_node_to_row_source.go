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

package sql

import (
	"context"
	"strconv"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowflow"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type metadataForwarder interface {
	forwardMetadata(metadata *execinfrapb.ProducerMetadata)
}

type planNodeToRowSource struct {
	execinfra.ProcessorBase

	started bool

	fastPath    bool
	node        planNode
	params      runParams
	outputTypes []types.T

	firstNotWrapped planNode

	// run time state machine values
	row sqlbase.EncDatumRow

	// IsCallProcedure flags that child is call procedure node
	IsCallProcedure bool
	callProcedure   *callProcedureNode
}

func makePlanNodeToRowSource(
	source planNode, params runParams, fastPath bool,
) (*planNodeToRowSource, error) {
	nodeColumns := planColumns(source)

	types := make([]types.T, len(nodeColumns))
	for i := range nodeColumns {
		types[i] = *nodeColumns[i].Typ
	}
	row := make(sqlbase.EncDatumRow, len(nodeColumns))

	v, ok := source.(*callProcedureNode)
	ret := planNodeToRowSource{
		node:            source,
		params:          params,
		outputTypes:     types,
		row:             row,
		fastPath:        fastPath,
		IsCallProcedure: ok,
	}
	if ok {
		ret.callProcedure = v
	}

	return &ret, nil
}

var _ execinfra.LocalProcessor = &planNodeToRowSource{}

// InitWithOutput implements the LocalProcessor interface.
func (p *planNodeToRowSource) InitWithOutput(
	post *execinfrapb.PostProcessSpec, output execinfra.RowReceiver,
) error {
	return p.InitWithEvalCtx(
		p,
		post,
		p.outputTypes,
		nil, /* flowCtx */
		p.params.EvalContext(),
		0, /* processorID */
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	)
}

// SetInput implements the LocalProcessor interface.
// input is the first upstream RowSource. When we're done executing, we need to
// drain this row source of its metadata in case the planNode tree we're
// wrapping returned an error, since planNodes don't know how to drain trailing
// metadata.
func (p *planNodeToRowSource) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if p.firstNotWrapped == nil {
		// Short-circuit if we never set firstNotWrapped - indicating this planNode
		// tree had no DistSQL-plannable subtrees.
		return nil
	}
	p.AddInputToDrain(input)
	// Search the plan we're wrapping for firstNotWrapped, which is the planNode
	// that DistSQL planning resumed in. Replace that planNode with input,
	// wrapped as a planNode.
	return walkPlan(ctx, p.node, planObserver{
		replaceNode: func(ctx context.Context, nodeName string, plan planNode) (planNode, error) {
			if plan == p.firstNotWrapped {
				return makeRowSourceToPlanNode(input, p, planColumns(p.firstNotWrapped), p.firstNotWrapped), nil
			}
			return nil, nil
		},
	})
}

// InitProcessorProcedure init processor in procedure
func (p *planNodeToRowSource) InitProcessorProcedure(txn *kv.Txn) {
	if p.EvalCtx.IsProcedure {
		if p.FlowCtx != nil {
			p.FlowCtx.Txn = txn
		}
		p.started = false
		p.Closed = false
		p.State = execinfra.StateRunning
		p.Out.SetRowIdx(0)
	}
}

func (p *planNodeToRowSource) Start(ctx context.Context) context.Context {
	// We do not call p.StartInternal to avoid creating a span. Only the context
	// needs to be set.
	p.Ctx = ctx
	p.params.ctx = ctx
	if !p.started {
		p.started = true
		// This starts all of the nodes below this node.
		if err := startExec(p.params, p.node); err != nil {
			p.MoveToDraining(err)
			return ctx
		}
		if cts, ok := p.node.(*createMultiInstTableNode); ok {
			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(cts.res[0])),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(cts.res[1])),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(cts.res[2])),
				),
			}
			_, err := p.Out.EmitRow(ctx, res)
			if err != nil {
				panic(err)
			}
		}
	}
	return ctx
}

func (p *planNodeToRowSource) InternalClose() {
	if p.ProcessorBase.InternalClose() {
		p.started = true
	}
}

func (p *planNodeToRowSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State == execinfra.StateRunning && p.fastPath {
		var count int
		// If our node is a "fast path node", it means that we're set up to just
		// return a row count. So trigger the fast path and return the row count as
		// a row with a single column.
		fastPath, ok := p.node.(planNodeFastPath)

		if ok {
			var res bool
			if count, res = fastPath.FastPathResults(); res {
				if p.params.extendedEvalCtx.Tracing.Enabled() {
					log.VEvent(p.params.ctx, 2, "fast path completed")
				}
			} else {
				// Fall back to counting the rows.
				count = 0
				ok = false
			}
		}

		if !ok {
			// If we have no fast path to trigger, fall back to counting the rows
			// by Nexting our source until exhaustion.
			next, err := p.node.Next(p.params)
			if _, ok := p.node.(*selectIntoNode); ok && err != nil {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}
			for ; next; next, err = p.node.Next(p.params) {
				count++
			}
			if sel, ok := p.node.(*selectIntoNode); ok {
				count = len(sel.vars)
			}
			if err != nil {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}
		}
		p.MoveToDraining(nil /* err */)
		// Return the row count the only way we can: as a single-column row with
		// the count inside.
		return sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
	}

	for p.State == execinfra.StateRunning {
		var valid bool
		var err error
		if p.IsCallProcedure {
			// modify receive output type and rows
			cols := p.callProcedure.GetNextResultCols()
			var outputTypes []types.T
			for _, v := range cols {
				outputTypes = append(outputTypes, *v.Typ)
			}

			p.row = make(sqlbase.EncDatumRow, len(cols))
			p.outputTypes = outputTypes
			valid, err = p.node.Next(p.params)
			if !valid {
				// check next result
				has, err1 := p.callProcedure.HasNextResult(p.params.extendedEvalCtx.TxnImplicit)
				if has && err1 != nil {
					valid, err = p.node.Next(p.params)
				} else if err1 != nil {
					err = err1
				}
			}

			if err != nil || !valid {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}

		} else {
			valid, err = p.node.Next(p.params)
			if err != nil || !valid {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}
		}

		for i, datum := range p.node.Values() {
			if datum != nil {
				p.row[i] = sqlbase.DatumToEncDatum(&p.outputTypes[i], datum)
			}
		}
		// ProcessRow here is required to deal with projections, which won't be
		// pushed into the wrapped plan.
		if outRow := p.ProcessRowHelper(p.row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, p.DrainHelper()
}

func (p *planNodeToRowSource) ConsumerDone() {
	p.MoveToDraining(nil /* err */)
}

func (p *planNodeToRowSource) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	p.InternalClose()
}

// IsException implements the VectorizeAlwaysException interface.
func (p *planNodeToRowSource) IsException() bool {
	_, ok := p.node.(*setVarNode)
	return ok
}

// forwardMetadata will be called by any upstream rowSourceToPlanNode processors
// that need to forward metadata to the end of the flow. They can't pass
// metadata through local processors, so they instead add the metadata to our
// trailing metadata and expect us to forward it further.
func (p *planNodeToRowSource) forwardMetadata(metadata *execinfrapb.ProducerMetadata) {
	p.ProcessorBase.AppendTrailingMeta(*metadata)
}

// RunImp run planNodeToRowSource
func (p *planNodeToRowSource) RunImp(ctx context.Context, startTime time.Time) {
	if p.IsCallProcedure {
		p.RunProcedure(ctx, startTime)
	} else {
		execinfra.Run(ctx, p, p.Out.Output(), startTime)
	}
}

// ModifyDistsqlReceiver modify receiver output type/resultToStreamColMap/row
func (p *planNodeToRowSource) ModifyDistsqlReceiver(
	ctx context.Context, v *DistSQLReceiver, outputTypes []types.T,
) {
	cols := p.callProcedure.GetNextResultCols()
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			v.SetError(err)
			return
		}
	}

	if res, ok := v.resultWriter.(RestrictedCommandResult); ok {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}

	v.outputTypes = outputTypes
	v.resultToStreamColMap = make([]int, len(v.outputTypes))
	for i := range outputTypes {
		v.resultToStreamColMap[i] = i
	}
	v.row = nil
}

// EmitData emit data to dst
func EmitData(
	ctx context.Context,
	row sqlbase.EncDatumRow,
	meta *execinfrapb.ProducerMetadata,
	src execinfra.RowSource,
	dst execinfra.RowReceiver,
) bool {
	// Emit the row; stop if no more rows are needed.
	if row != nil || meta != nil {
		switch dst.Push(row, meta) {
		case execinfra.NeedMoreRows:
			dst.AddStats(0, row != nil)
			return true
		case execinfra.DrainRequested:
			execinfra.DrainAndForwardMetadata(ctx, src, dst)
		case execinfra.ConsumerClosed:
			src.ConsumerClosed()
		}
	}

	return false
}

// RunNext reads records from the source and outputs them to the receiver, properly
// draining the source of metadata and closing both the source and receiver.
//
// src needs to have been Start()ed before calling this.
// use NextResult to get procedure next result data, not use ProcessRowHelper
func RunNext(
	ctx context.Context, src *planNodeToRowSource, dst execinfra.RowReceiver, outputTypes []types.T,
) bool {
	for {
		row, meta := src.NextResult(outputTypes)
		// Emit the row; stop if no more rows are needed.
		if EmitData(ctx, row, meta, src, dst) {
			continue
		}
		return row != nil
	}
}

// CheckResultExist checks result exist
func (p *planNodeToRowSource) CheckResultExist() bool {
	return p.callProcedure.CheckResultExist()
}

// CheckTrailingMetaExist check whether trailingMeta exists.
func (p *planNodeToRowSource) CheckTrailingMetaExist() bool {
	return p.ProcessorBase.CheckTrailingMetaExist()
}

// NextResult get next procedure result row
func (p *planNodeToRowSource) NextResult(
	outputTypes []types.T,
) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	valid, err := p.callProcedure.Next(p.params)
	if err != nil || !valid {
		p.MoveToDraining(err)
		return nil, p.DrainHelper()
	}

	for i, datum := range p.node.Values() {
		if datum != nil {
			p.row[i] = sqlbase.DatumToEncDatum(&outputTypes[i], datum)
		}
	}

	return p.row, nil
}

// ModifyPlanNodeResultType modify plan node result type
func (p *planNodeToRowSource) ModifyPlanNodeResultType(ctx context.Context) []types.T {
	cols := p.callProcedure.GetNextResultCols()
	var outputTypes []types.T
	for _, v := range cols {
		outputTypes = append(outputTypes, *v.Typ)
	}

	// modify receiver to save result
	switch v := p.Out.Output().(type) {
	case *DistSQLReceiver:
		p.ModifyDistsqlReceiver(ctx, v, outputTypes)
	case *rowflow.CopyingRowReceiver:
		if v1, ok := v.RowReceiver.(*DistSQLReceiver); ok {
			p.ModifyDistsqlReceiver(ctx, v1, outputTypes)
		}
	}

	// reset state to running
	p.State = execinfra.StateRunning

	p.row = make(sqlbase.EncDatumRow, len(cols))
	return outputTypes
}

// RunProcedure reads records from the procedure and outputs them to the receiver, properly
// draining the source of metadata and closing both the source and receiver.
//
// src needs to have been Start()ed before calling this.
func (p *planNodeToRowSource) RunProcedure(ctx context.Context, startTime time.Time) {
	runDst := p.Out.Output()

	defer func() {
		if p1 := recover(); p1 != nil {
			err := errors.Errorf("%v", p1)
			if err.Error() == "" {
				// for test.
				panic(p1)
			}
			metaErr := &execinfrapb.ProducerMetadata{Err: err}
			runDst.Push(nil, metaErr)
			execinfra.HandleConsumerClosed(p, runDst, startTime, nil)
			return
		}
	}()

	isAddRow := false
	var err error
	count := 1
	if p.CheckResultExist() {
		var has bool
		for {
			// check cancel ctx error
			if err = ctx.Err(); err != nil {
				break
			}
			outputTypes := p.ModifyPlanNodeResultType(ctx)

			isAddRow = RunNext(ctx, p, runDst, outputTypes)

			p.State = execinfra.StateRunning
			has, err = p.callProcedure.HasNextResult(p.params.extendedEvalCtx.TxnImplicit)
			if !has || err != nil {
				break
			}

			// write complete
			typ, affected := p.callProcedure.GetResultTypeAndAffected()
			runDst.AddPGComplete(strconv.Itoa(count), typ, affected)
			count++
		}
	}

	p.callProcedure.endTransaction(p.params.extendedEvalCtx.TxnImplicit)

	if err != nil {
		runDst.AddPGComplete(strconv.Itoa(count), tree.Rows, 0)

		p.MoveToDraining(err)
		meta := p.DrainHelper()
		EmitData(ctx, nil, meta, p, runDst)
	} else if p.CheckTrailingMetaExist() {
		meta := p.DrainHelper()
		EmitData(ctx, nil, meta, p, runDst)
	}

	p.Out.Output().ProducerDone()
	p.Out.Output().AddStats(timeutil.Since(startTime), isAddRow)
}

// Run is part of the processor interface.
func (p *planNodeToRowSource) Run(ctx context.Context) execinfra.RowStats {
	if p.Out.Output() == nil {
		panic("processor output not initialized for emitting rows")
	}
	start := timeutil.Now()
	defer func(p *planNodeToRowSource, startTime time.Time) execinfra.RowStats {
		if p1 := recover(); p1 != nil {
			err := errors.Errorf("%v", p1)
			if err.Error() == "" {
				// for test.
				panic(p1)
			}
			metaErr := &execinfrapb.ProducerMetadata{Err: err}
			p.Out.Output().Push(nil, metaErr)
			execinfra.HandleConsumerClosed(p, p.Out.Output(), startTime, nil)
		}

		return p.Out.Output().GetStats()
	}(p, start)
	ctx = p.Start(ctx)
	p.RunImp(ctx, start)
	return p.Out.Output().GetStats()
}

// DealWithFlowCtx deal with FlowCtx in LocalPlan.
func (p *planNodeToRowSource) DealWithFlowCtx(ctx *execinfra.FlowCtx) {
	if p.Out.Output() == nil {
		panic("processor output not initialized for emitting rows")
	}
}
