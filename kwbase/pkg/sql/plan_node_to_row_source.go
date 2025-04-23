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

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
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

	return &planNodeToRowSource{
		node:        source,
		params:      params,
		outputTypes: types,
		row:         row,
		fastPath:    fastPath,
	}, nil
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
		valid, err := p.node.Next(p.params)
		if err != nil || !valid {
			p.MoveToDraining(err)
			return nil, p.DrainHelper()
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
