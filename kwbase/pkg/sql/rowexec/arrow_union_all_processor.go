// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package rowexec

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowUnionAllPlan is the JSON-serialized plan carried in
// ProcessorCoreUnion.ArrowUnionAll.Expr. A UNION ALL (or INTERSECT ALL /
// EXCEPT ALL) is a pure concatenation of its inputs, so the plan only needs the
// result column types; the number of inputs is determined by the stage's
// input streams.
type arrowUnionAllPlan struct {
	// Types are the KWDB result column types, used to rebuild the output Record.
	Types []types.T `json:"types"`
}

// arrowUnionAllProcessor concatenates all of its input streams into a single
// Arrow Record. It is the Arrow-side replacement for the row-based no-op
// processor that EnsureSingleStreamPerNode would otherwise insert to merge the
// union's input routers: instead of decoding every row into the row engine and
// re-encoding it downstream, the input Records (already in Arrow form when the
// upstream operators are Arrow-capable) are decoded once and rebuilt into a
// single concatenated Record for operator-to-operator delivery.
type arrowUnionAllProcessor struct {
	execinfra.ProcessorBase

	inputs     []execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowUnionAllPlan
	outputRec  arrow.Record
	rowSrc     execinfra.RowSource
}

var _ execinfra.RowSource = &arrowUnionAllProcessor{}
var _ ArrowRecordEmitter = &arrowUnionAllProcessor{}

func newArrowUnionAllProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	inputs []execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowUnionAllPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		return nil, errors.New("arrowUnionAll requires at least one input")
	}
	p := &arrowUnionAllProcessor{
		inputs: inputs,
		alloc:  memory.NewGoAllocator(),
		da:     &sqlbase.DatumAlloc{},
		plan:   plan,
	}
	inTypes := make([]types.T, len(plan.Types))
	for i := range plan.Types {
		inTypes[i] = plan.Types[i]
	}
	if err := p.Init(
		p, post, inTypes, flowCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: inputs},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowUnionAllProcessor) Start(ctx context.Context) context.Context {
	for _, in := range p.inputs {
		in.Start(ctx)
	}
	ctx = p.StartInternal(ctx, "arrowUnionAll")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowUnionAllProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		if p.rowSrc == nil {
			p.rowSrc = NewArrowToRowSource(p.outputRec, p.OutputTypes())
		}
		row, meta := p.rowSrc.Next()
		if meta != nil && meta.Err != nil {
			p.MoveToDraining(meta.Err)
			return nil, p.DrainHelper()
		}
		if row == nil {
			p.MoveToDraining(nil)
			return nil, p.DrainHelper()
		}
		return row, nil
	}
	return nil, p.DrainHelper()
}

// ConsumerClosed implements the RowSource interface.
func (p *arrowUnionAllProcessor) ConsumerClosed() {
	if p.outputRec != nil {
		p.outputRec.Release()
		p.outputRec = nil
	}
	p.ProcessorBase.InternalClose()
}

// ArrowOutput implements ArrowRecordEmitter, exposing the concatenated Record
// for operator-to-operator buffering. Ownership transfers to the caller
// (Retain is applied here; the caller must Release). Returns nil when no record
// was produced.
func (p *arrowUnionAllProcessor) ArrowOutput() arrow.Record {
	if p.outputRec == nil {
		return nil
	}
	p.outputRec.Retain()
	return p.outputRec
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowUnionAllProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.inputs[0].OutputTypes()
}

// compute pulls a single Record from each input (each Arrow-capable upstream
// emits exactly one Record), concatenates their rows, and rebuilds a single
// output Record. Upstream operators that emit Arrow Records hand them over
// without a row round-trip; row-based upstreams are adapted by unifiedInputFrom.
func (p *arrowUnionAllProcessor) compute(ctx context.Context) error {
	valTypes := p.plan.Types
	merged := make(sqlbase.EncDatumRows, 0)
	for _, in := range p.inputs {
		conv, err := unifiedInputFrom(p.alloc, in, p.da)
		if err != nil {
			return err
		}
		conv.Init(ctx)
		rec, done, err := conv.Next(ctx)
		if err != nil {
			return err
		}
		if done || rec == nil {
			continue
		}
		rows, err := arrowRecordToEncDatumRows(valTypes, rec)
		if err != nil {
			return err
		}
		merged = append(merged, rows...)
	}
	if len(merged) == 0 {
		p.outputRec = array.NewRecord(
			buildArrowSchema(typesToPtr(valTypes)), []arrow.Array{}, 0)
		return nil
	}
	cols, err := buildArrowColumns(p.alloc, typesToPtr(valTypes), merged, p.da)
	if err != nil {
		return err
	}
	p.outputRec = array.NewRecord(buildArrowSchema(typesToPtr(valTypes)), cols, int64(len(merged)))
	return nil
}

func typesToPtr(in []types.T) []*types.T {
	out := make([]*types.T, len(in))
	for i := range in {
		t := in[i]
		out[i] = &t
	}
	return out
}
