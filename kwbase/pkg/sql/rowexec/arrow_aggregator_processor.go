// Copyright 2024 The KWDB Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowexec

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// arrowAggPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowAggregator.Expr. Its JSON shape mirrors the struct
// in the sql package; the JSON bytes are the only contract between planner
// and executor.
type arrowAggPlan struct {
	GroupCols []int            `json:"group_cols"`
	Aggs      []arrowAggExprJS `json:"aggs"`
	// OutTypes is the aggregator's raw output column types, serialized via
	// serializeArrowOutType in the planner, in the executor's emission order.
	OutTypes []string `json:"out_types"`
}

// arrowAggExprJS is one aggregate expression. Input is -1 for COUNT(*). Inputs
// holds the (one or more) input column indices; for single-input aggregates it
// is derived from Input, while multi-input aggregates (e.g. FINAL_VARIANCE
// consuming [SQRDIFF, SUM, COUNT]) populate Inputs directly.
type arrowAggExprJS struct {
	Func   string `json:"func"`
	Input  int    `json:"input"`
	Inputs []int  `json:"inputs,omitempty"`
}

// arrowAggRuns counts how many times the arrow aggregator processor has run.
var arrowAggRuns int64

// ArrowAggRunCount returns the number of arrow aggregator processors that have
// run so far in this process.
func ArrowAggRunCount() int64 {
	return atomic.LoadInt64(&arrowAggRuns)
}

// arrowAggregatorProcessor is a real execution-stage processor that computes
// aggregations (sum/count/min/max/mean, with optional grouping) using the
// Arrow compute engine.
type arrowAggregatorProcessor struct {
	execinfra.ProcessorBase

	input      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowAggPlan
	outputRows sqlbase.EncDatumRows
	rowIdx     int
	// outputRec is the arrow.Record produced in compute, retained for
	// operator-to-operator buffering (§7.8). Released in ConsumerClosed.
	outputRec arrow.Record
}

var _ execinfra.RowSource = &arrowAggregatorProcessor{}

func newArrowAggregatorProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowAggPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	p := &arrowAggregatorProcessor{
		input: input,
		alloc: memory.NewGoAllocator(),
		da:    &sqlbase.DatumAlloc{},
		plan:  plan,
	}
	// The Arrow aggregator advertises its *raw* output types (matching the
	// colexec/rowexec aggregator, which uses its own outputTypes). The
	// post-process render (e.g. min(ts)::STRING) is applied downstream by
	// ProcessRowHelper, so the planner's post.OutputTypes (render result) must
	// not be used as the aggregator's declared output schema. When the planner
	// did not ship OutTypes (e.g. older plans) we fall back to post.OutputTypes.
	outTypes := post.OutputTypes
	if len(plan.OutTypes) > 0 {
		raw := make([]types.T, 0, len(plan.OutTypes))
		ok := true
		for _, s := range plan.OutTypes {
			t, good := deserializeArrowOutType(s)
			if !good {
				ok = false
				break
			}
			raw = append(raw, t)
		}
		if ok {
			outTypes = raw
		}
	}
	if len(outTypes) == 0 {
		outTypes = input.OutputTypes()
	}
	if err := p.Init(
		p, post, outTypes, flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{p.input}},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowAggregatorProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowAggregator")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowAggregatorProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		if p.rowIdx >= len(p.outputRows) {
			p.MoveToDraining(nil)
			break
		}
		row := p.outputRows[p.rowIdx]
		p.rowIdx++
		if outRow := p.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, p.DrainHelper()
}

// ConsumerClosed implements the RowSource interface.
func (p *arrowAggregatorProcessor) ConsumerClosed() {
	if p.outputRec != nil {
		p.outputRec.Release()
		p.outputRec = nil
	}
	p.ProcessorBase.InternalClose()
}

// ArrowOutput implements ArrowRecordEmitter, exposing the computed Record for
// operator-to-operator buffering. Ownership transfers to the caller (Retain is
// applied here; the caller must Release). Returns nil when no record was
// produced.
func (p *arrowAggregatorProcessor) ArrowOutput() arrow.Record {
	if p.outputRec == nil {
		return nil
	}
	p.outputRec.Retain()
	return p.outputRec
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowAggregatorProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.input.OutputTypes()
}

func (p *arrowAggregatorProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowAggRuns, 1)

	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}

	spec := buildArrowAggSpec(p.plan)
	agg := NewArrowAggregator(p.alloc, conv, spec)
	agg.Init(ctx)
	rec, done, err := agg.Next(ctx)
	if err != nil {
		return err
	}
	if done || rec == nil {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	// Hand the record to a downstream arrow operator via operator-to-operator
	// buffering (§7.8); released in ConsumerClosed.
	p.outputRec = rec
	p.outputRows, err = arrowRecordToEncDatumRows(p.Out.OutputTypes, rec)
	if err != nil {
		return err
	}
	// arrowRecordToEncDatumRows counts on the record still being alive for any
	// lazily-decoded values; release after building the rows.
	return nil
}

// deserializeArrowOutType reverses the planner-side serializeArrowOutType,
// recovering the aggregator's raw output type from its JSON-safe string.
func deserializeArrowOutType(s string) (types.T, bool) {
	if s == "" {
		return types.T{}, false
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return types.T{}, false
	}
	var t types.T
	if err := t.Unmarshal(b); err != nil {
		return types.T{}, false
	}
	return t, true
}

func buildArrowAggSpec(plan arrowAggPlan) ArrowAggSpec {
	groupCols := make([]string, len(plan.GroupCols))
	for i, c := range plan.GroupCols {
		groupCols[i] = fmt.Sprintf("col%d", c)
	}
	aggs := make([]ArrowAggExpr, len(plan.Aggs))
	for i, a := range plan.Aggs {
		inputs := make([]string, 0, len(a.Inputs)+1)
		if len(a.Inputs) > 0 {
			for _, c := range a.Inputs {
				if c >= 0 {
					inputs = append(inputs, fmt.Sprintf("col%d", c))
				}
			}
		} else if a.Input >= 0 {
			inputs = append(inputs, fmt.Sprintf("col%d", a.Input))
		}
		aggs[i] = ArrowAggExpr{Func: a.Func, Inputs: inputs}
	}
	return ArrowAggSpec{GroupCols: groupCols, Aggs: aggs}
}
