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
// See the License for the specific language governing to specific
// permissions and limitations under the License.

package rowexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/apd"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// arrowWindowerPlan mirrors the planner-side struct in pkg/sql/arrow_unification.go.
type arrowWindowerPlan struct {
	PartitionBy []int                `json:"partition_by"`
	Fns         []arrowWindowFnPlan `json:"fns"`
}

type arrowWindowFnPlan struct {
	Func      string `json:"func"`
	Input     int    `json:"input"`
	Ordering  []int  `json:"ordering"`
	OutputIdx int    `json:"output_idx"`
}

// arrowWindowerRuns counts how many times the arrow windower processor has run.
var arrowWindowerRuns int64

// ArrowWindowerRunCount returns the number of arrow windower processors run.
func ArrowWindowerRunCount() int64 {
	return atomic.LoadInt64(&arrowWindowerRuns)
}

// arrowWindowerProcessor computes the supported subset of window functions over
// Arrow records: no-frame (default RANGE frame) partition aggregate functions
// (sum/count/min/max/avg/bool_and/bool_or). Input must already be ordered by
// PARTITION BY then each window function's ORDER BY, exactly as the classic
// windower assumes. Each window function value is the running aggregate over
// the current peer group (rows with equal ORDER BY values within the partition).
type arrowWindowerProcessor struct {
	execinfra.ProcessorBase

	input      execinfra.RowSource
	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	plan       arrowWindowerPlan
	inTypes    []types.T
	outTypes   []types.T
	outputRows sqlbase.EncDatumRows
	rowIdx     int
}

var _ execinfra.RowSource = &arrowWindowerProcessor{}

func newArrowWindowerProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.Expression,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var plan arrowWindowerPlan
	if err := json.Unmarshal([]byte(spec.Expr), &plan); err != nil {
		return nil, err
	}
	inTypes := input.OutputTypes()
	for _, c := range plan.PartitionBy {
		if c >= len(inTypes) {
			return nil, fmt.Errorf("arrow windower: partition column %d out of range (ncols=%d)", c, len(inTypes))
		}
	}
	for _, fn := range plan.Fns {
		if fn.Input >= len(inTypes) {
			return nil, fmt.Errorf("arrow windower: input column %d out of range (ncols=%d)", fn.Input, len(inTypes))
		}
		for _, o := range fn.Ordering {
			if o >= len(inTypes) {
				return nil, fmt.Errorf("arrow windower: order column %d out of range (ncols=%d)", o, len(inTypes))
			}
		}
	}
	outTypes := make([]types.T, len(inTypes)+len(plan.Fns))
	copy(outTypes, inTypes)
	// The output column types are inferred from the aggregate.
	for _, fn := range plan.Fns {
		outTypes[fn.OutputIdx] = aggWindowOutputType(fn.Func, inTypes[fn.Input])
	}
	p := &arrowWindowerProcessor{
		input:    input,
		alloc:    memory.NewGoAllocator(),
		da:       &sqlbase.DatumAlloc{},
		plan:     plan,
		inTypes:  inTypes,
		outTypes: outTypes,
	}
	evalCtx := flowCtx.NewEvalCtx()
	if err := p.InitWithEvalCtx(
		p, post, outTypes, flowCtx, evalCtx, processorID, output, nil,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{p.input}},
	); err != nil {
		return nil, err
	}
	return p, nil
}

// Start implements the RowSource interface.
func (p *arrowWindowerProcessor) Start(ctx context.Context) context.Context {
	p.input.Start(ctx)
	ctx = p.StartInternal(ctx, "arrowWindower")
	if err := p.compute(ctx); err != nil {
		p.MoveToDraining(err)
	}
	return ctx
}

// Next implements the RowSource interface.
func (p *arrowWindowerProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for p.State == execinfra.StateRunning {
		if p.rowIdx >= len(p.outputRows) {
			p.MoveToDraining(nil)
			break
		}
		row := p.outputRows[p.rowIdx]
		p.rowIdx++
		return row, nil
	}
	return nil, p.DrainHelper()
}

// ConsumerClosed implements the RowSource interface.
func (p *arrowWindowerProcessor) ConsumerClosed() {
	p.ProcessorBase.InternalClose()
}

// InitProcessorProcedure implements the RowSource interface.
func (p *arrowWindowerProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if p.Out.OutputTypes != nil && len(p.Out.OutputTypes) > 0 {
		return
	}
	p.Out.OutputTypes = p.outTypes
}

// peerKey encodes the ORDER BY columns of a row so that equal peers share a key.
func (p *arrowWindowerProcessor) peerKey(row sqlbase.EncDatumRow, ordering []int) ([]byte, error) {
	return encodeCols(row, ordering, p.inTypes)
}

func (p *arrowWindowerProcessor) compute(ctx context.Context) error {
	atomic.AddInt64(&arrowWindowerRuns, 1)

	conv, err := unifiedInputFrom(p.alloc, p.input, p.da)
	if err != nil {
		return err
	}
	conv.Init(ctx)
	rec, done, err := conv.Next(ctx)
	if err != nil {
		return err
	}
	if done || rec == nil {
		p.outputRows = sqlbase.EncDatumRows{}
		return nil
	}
	rows, err := arrowRecordToEncDatumRows(p.inTypes, rec)
	if err != nil {
		return err
	}
	rec.Release()

	out := make(sqlbase.EncDatumRows, len(rows))
	for i, row := range rows {
		nr := make(sqlbase.EncDatumRow, len(p.outTypes))
		copy(nr, row)
		out[i] = nr
	}

	// Process partition by partition. Rows are already ordered by partition
	// columns, so equal partition keys are contiguous.
	start := 0
	for end := 1; end <= len(rows); end++ {
		if end == len(rows) || !samePartition(p, rows[start], rows[end]) {
			p.computePartition(out, rows[start:end], start)
			start = end
		}
	}
	p.outputRows = out
	return nil
}

// samePartition reports whether two rows share the same PARTITION BY key.
func samePartition(p *arrowWindowerProcessor, a, b sqlbase.EncDatumRow) bool {
	for _, c := range p.plan.PartitionBy {
		cmp, err := a.Compare(p.inTypes, p.da, sqlbase.ColumnOrdering{{ColIdx: c, Direction: encoding.Ascending}}, p.EvalCtx, b)
		if err != nil {
			panic(err)
		}
		if cmp != 0 {
			return false
		}
	}
	return true
}

// computePartition evaluates every window function over a single partition.
// Each function is accumulated over its peer groups (rows with equal ORDER BY
// values); the running aggregate is written back to every row in the peer.
// startIdx is the absolute index of part[0] within out.
func (p *arrowWindowerProcessor) computePartition(out, part sqlbase.EncDatumRows, startIdx int) {
	for _, fn := range p.plan.Fns {
		acc := &windowAccumulator{fn: fn.Func}
		peerStart := 0
		for i := 1; i <= len(part); i++ {
			atEnd := i == len(part)
			newPeer := atEnd
			if !atEnd {
				k1, err := p.peerKey(part[i-1], fn.Ordering)
				if err != nil {
					panic(err)
				}
				k2, err := p.peerKey(part[i], fn.Ordering)
				if err != nil {
					panic(err)
				}
				newPeer = !bytes.Equal(k1, k2)
			}
			// Consume the current row into the running aggregate.
			if err := acc.consume(p, part[i-1], fn.Input); err != nil {
				panic(err)
			}
			if newPeer {
				val, err := acc.finalize(p)
				if err != nil {
					panic(err)
				}
				// Write the running aggregate (accumulated up to and including
				// the current peer) to every row in the peer group. The
				// accumulator itself is NOT reset: it must keep accumulating
				// across peer groups within the same partition.
				for j := peerStart; j < i; j++ {
					out[startIdx+j][fn.OutputIdx] = val
				}
				peerStart = i
			}
		}
	}
}

// aggWindowOutputType returns the result type of a supported window aggregate.
func aggWindowOutputType(fn string, in types.T) types.T {
	switch fn {
	case "count":
		return *types.Int
	case "min", "max":
		return in
	case "sum":
		switch in.Family() {
		case types.IntFamily, types.FloatFamily, types.DecimalFamily:
			return in
		}
		return in
	case "avg":
		switch in.Family() {
		case types.IntFamily:
			return *types.Decimal
		}
		return in
	case "bool_and", "bool_or":
		return *types.Bool
	}
	return in
}

// windowAccumulator holds the running aggregate for one window function over a
// peer group.
type windowAccumulator struct {
	fn     string
	set    bool
	value  tree.Datum // current accumulated value
	count  int64      // for avg
}

func (a *windowAccumulator) reset() {
	a.set = false
	a.value = nil
	a.count = 0
}

func (a *windowAccumulator) consume(p *arrowWindowerProcessor, row sqlbase.EncDatumRow, input int) error {
	ed := row[input]
	if ed.IsNull() {
		// COUNT counts non-null; aggregates ignore nulls.
		return nil
	}
	if err := ed.EnsureDecoded(&p.inTypes[input], p.da); err != nil {
		return err
	}
	d := ed.Datum
	ctx := p.EvalCtx
	switch a.fn {
	case "count":
		a.count++
		a.set = true
		return nil
	case "min":
		if !a.set {
			a.value = d
			a.set = true
			return nil
		}
		if d.Compare(ctx, a.value) < 0 {
			a.value = d
		}
		return nil
	case "max":
		if !a.set {
			a.value = d
			a.set = true
			return nil
		}
		if d.Compare(ctx, a.value) > 0 {
			a.value = d
		}
		return nil
	case "bool_and":
		b := bool(*ed.Datum.(*tree.DBool))
		if !a.set {
			a.value = tree.MakeDBool(tree.DBool(b))
			a.set = true
			return nil
		}
		a.value = tree.MakeDBool(tree.DBool(bool(*a.value.(*tree.DBool)) && b))
		return nil
	case "bool_or":
		b := bool(*ed.Datum.(*tree.DBool))
		if !a.set {
			a.value = tree.MakeDBool(tree.DBool(b))
			a.set = true
			return nil
		}
		a.value = tree.MakeDBool(tree.DBool(bool(*a.value.(*tree.DBool)) || b))
		return nil
	case "sum", "avg":
		if !a.set {
			a.value = d
			a.set = true
			a.count = 1
			return nil
		}
		sum, err := addDatums(ctx, a.value, d)
		if err != nil {
			return err
		}
		a.value = sum
		a.count++
		return nil
	}
	return fmt.Errorf("arrow windower: unsupported aggregate %q", a.fn)
}

// addDatums adds two numeric datums of the same supported type (int/float/
// decimal), returning a datum of the same type.
func addDatums(ctx *tree.EvalContext, a, b tree.Datum) (tree.Datum, error) {
	switch av := a.(type) {
	case *tree.DInt:
		return tree.NewDInt(*av + *b.(*tree.DInt)), nil
	case *tree.DFloat:
		return tree.NewDFloat(*av + *b.(*tree.DFloat)), nil
	case *tree.DDecimal:
		var res apd.Decimal
		if _, err := tree.DecimalCtx.Add(&res, &av.Decimal, &b.(*tree.DDecimal).Decimal); err != nil {
			return nil, err
		}
		return &tree.DDecimal{Decimal: res}, nil
	}
	return nil, fmt.Errorf("arrow windower: unsupported sum type %s", a.ResolvedType())
}

func (a *windowAccumulator) finalize(p *arrowWindowerProcessor) (sqlbase.EncDatum, error) {
	if !a.set {
		// All-null group or empty peer -> NULL.
		return sqlbase.EncDatum{Datum: tree.DNull}, nil
	}
	var res tree.Datum = a.value
	if a.fn == "avg" {
		// Result type is decimal. Convert the running sum to a decimal and
		// divide by the non-null count.
		var sumDec apd.Decimal
		switch v := a.value.(type) {
		case *tree.DInt:
			sumDec.SetInt64(int64(*v))
		case *tree.DFloat:
			if _, err := sumDec.SetFloat64(float64(*v)); err != nil {
				return sqlbase.EncDatum{}, err
			}
		case *tree.DDecimal:
			sumDec = v.Decimal
		default:
			return sqlbase.EncDatum{}, fmt.Errorf("arrow windower: unsupported avg type %s", a.value.ResolvedType())
		}
		var countDec apd.Decimal
		countDec.SetInt64(a.count)
		var resDec apd.Decimal
		if _, err := tree.DecimalCtx.Quo(&resDec, &sumDec, &countDec); err != nil {
			return sqlbase.EncDatum{}, err
		}
		res = &tree.DDecimal{Decimal: resDec}
	} else if a.fn == "count" {
		res = tree.NewDInt(tree.DInt(a.count))
	}
	return sqlbase.EncDatum{Datum: res}, nil
}
