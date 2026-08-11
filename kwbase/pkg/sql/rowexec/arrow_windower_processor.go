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
	"sort"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/apd"
)

// arrowWindowerPlan mirrors the planner-side struct in pkg/sql/arrow_unification.go.
type arrowWindowerPlan struct {
	PartitionBy []int               `json:"partition_by"`
	Fns         []arrowWindowFnPlan `json:"fns"`
}

type arrowWindowFnPlan struct {
	Func      string                `json:"func"`
	Kind      string                `json:"kind"`
	Input     int                   `json:"input"`
	Ordering  []int                 `json:"ordering"`
	OutputIdx int                   `json:"output_idx"`
	Frame     *arrowWindowFramePlan `json:"frame,omitempty"`
}

type arrowWindowFramePlan struct {
	Mode  string `json:"mode"`
	Start string `json:"start"`
	End   string `json:"end"`
	// StartOffset / EndOffset carry the integer offset (in rows) for ROWS-mode
	// offset bounds (e.g. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING). They are
	// only meaningful when Start/End is "offset_preceding" / "offset_following".
	StartOffset int `json:"start_offset,omitempty"`
	EndOffset   int `json:"end_offset,omitempty"`
}

// isWholePartitionFrame reports whether the frame covers the entire partition
// (UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING), in which case every row in the
// partition gets the aggregate computed over all rows of the partition.
func (f *arrowWindowFramePlan) isWholePartitionFrame() bool {
	return f != nil && f.Start == "unbounded_preceding" && f.End == "unbounded_following"
}

// hasOffset reports whether either bound is a numeric offset (ROWS mode offset
// frame), which requires per-row window evaluation.
func (f *arrowWindowFramePlan) hasOffset() bool {
	if f == nil {
		return false
	}
	return f.Start == "offset_preceding" || f.End == "offset_following"
}

// frameBounds returns the inclusive [start, end] row indices for the window
// frame of row i within a partition of n rows. startEdge/endEdge distinguish
// whether a bound is inclusive in ROWS mode vs the peer-group semantics of
// RANGE mode.
func (f *arrowWindowFramePlan) frameBounds(i, n int) (int, int) {
	if f == nil {
		// Default RANGE frame: UNBOUNDED PRECEDING TO CURRENT ROW (running over
		// the current row's peer group, since RANGE treats equal ordering values
		// as a unit). Callers handle the peer-group expansion separately.
		return 0, i
	}
	start := 0
	switch {
	case f.Start == "unbounded_preceding":
		start = 0
	case f.Start == "current_row":
		start = i
	case f.Start == "offset_preceding":
		start = i - f.StartOffset
	}
	if start < 0 {
		start = 0
	}
	end := i
	switch {
	case f.End == "unbounded_following":
		end = n - 1
	case f.End == "current_row":
		end = i
	case f.End == "offset_following":
		end = i + f.EndOffset
	}
	if end > n-1 {
		end = n - 1
	}
	if end < start {
		end = start
	}
	return start, end
}

// offsetDatum returns d shifted by sign*off (off is a non-negative integer).
// It supports the ordering-column types that RANGE offset frames are defined
// over: integers, floats, decimals and timestamps. Returns nil for unsupported
// types, in which case the caller falls back to the peer-group semantics.
func offsetDatum(d tree.Datum, off, sign int) tree.Datum {
	switch v := d.(type) {
	case *tree.DInt:
		return tree.NewDInt(*v + tree.DInt(sign*off))
	case *tree.DFloat:
		return tree.NewDFloat(*v + tree.DFloat(sign*off))
	case *tree.DDecimal:
		r := &tree.DDecimal{}
		r.Decimal.Set(&v.Decimal)
		tree.ExactCtx.Add(&r.Decimal, &v.Decimal, apd.New(int64(sign*off), 0))
		return r
	case *tree.DTimestamp:
		return tree.MakeDTimestamp(v.Time.Add(time.Duration(sign*off)*time.Second), time.Microsecond)
	case *tree.DTimestampTZ:
		return tree.MakeDTimestampTZ(v.Time.Add(time.Duration(sign*off)*time.Second), time.Microsecond)
	}
	return nil
}

// rangeFrameBounds returns the inclusive [start, end] row indices for a RANGE
// offset frame of row i within a partition sorted by the order column. Unlike
// ROWS offset frames (which count physical rows), a RANGE offset frame includes
// every row whose ORDER BY value lies within [orderVal(i) - StartOffset,
// orderVal(i) + EndOffset]. Because the partition is sorted by the order column
// the frame is a single contiguous range around i, found by binary search over
// the order values.
func rangeFrameBounds(i, n int, orderVals []tree.Datum, evalCtx *tree.EvalContext, f *arrowWindowFramePlan) (int, int) {
	if n == 0 {
		return 0, -1
	}
	cur := orderVals[i]
	var low, high tree.Datum
	if f.Start == "offset_preceding" {
		low = offsetDatum(cur, f.StartOffset, -1)
	}
	if f.End == "offset_following" {
		high = offsetDatum(cur, f.EndOffset, +1)
	}
	start := 0
	end := n - 1
	if low != nil {
		// first index j with orderVals[j] >= low
		lo, hi := 0, n
		for lo < hi {
			mid := (lo + hi) / 2
			if orderVals[mid].Compare(evalCtx, low) >= 0 {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		start = lo
	}
	if high != nil {
		// last index j with orderVals[j] <= high
		lo, hi := 0, n-1
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if orderVals[mid].Compare(evalCtx, high) <= 0 {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		end = lo
	}
	if start > end {
		return i, i - 1 // empty frame
	}
	return start, end
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
	outputRec  arrow.Record
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
		if fn.Kind == "ranking" {
			if fn.Input != -1 {
				return nil, fmt.Errorf("arrow windower: ranking function %q must not reference an input column", fn.Func)
			}
		} else {
			if fn.Input >= len(inTypes) {
				return nil, fmt.Errorf("arrow windower: input column %d out of range (ncols=%d)", fn.Input, len(inTypes))
			}
		}
		for _, o := range fn.Ordering {
			if o >= len(inTypes) {
				return nil, fmt.Errorf("arrow windower: order column %d out of range (ncols=%d)", o, len(inTypes))
			}
		}
	}
	outTypes := make([]types.T, len(inTypes)+len(plan.Fns))
	copy(outTypes, inTypes)
	// The output column types are inferred from the function.
	for _, fn := range plan.Fns {
		if fn.Kind == "ranking" {
			outTypes[fn.OutputIdx] = *types.Int
		} else {
			outTypes[fn.OutputIdx] = aggWindowOutputType(fn.Func, inTypes[fn.Input])
		}
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
	// Build the output Arrow Record so a downstream colexec operator can consume
	// it directly via the Arrow->colexec zero-copy bridge (RecordToBatch),
	// skipping the row round-trip used by the classic Next() path.
	ptrTypes := make([]*types.T, len(p.outTypes))
	for i := range p.outTypes {
		t := p.outTypes[i]
		ptrTypes[i] = &t
	}
	// Only emit an Arrow Record when every output column can be materialized by
	// the Arrow engine; otherwise fall back to the row output (p.outputRows).
	if arrowTypesAllSupported(p.outTypes) {
		cols, err := buildArrowColumns(p.alloc, ptrTypes, p.outputRows, p.da)
		if err != nil {
			// Fall back to the row output instead of failing the whole flow.
			return nil
		}
		p.outputRec = array.NewRecord(buildArrowSchema(ptrTypes), cols, int64(len(p.outputRows)))
	}
	return nil
}

// ArrowOutput implements the ArrowRecordEmitter contract, exposing the computed
// output Record for a downstream Arrow or colexec operator to consume without a
// row round-trip.
func (p *arrowWindowerProcessor) ArrowOutput() arrow.Record {
	return p.outputRec
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
// part is the slice of input rows for this partition (ordered by PARTITION BY
// only). The function sorts the corresponding output rows in place by the
// window ORDER BY and then evaluates every window function over that sorted
// order.
func (p *arrowWindowerProcessor) computePartition(out, part sqlbase.EncDatumRows, startIdx int) {
	// The upstream stream is only guaranteed to be ordered by PARTITION BY,
	// not by the window function's ORDER BY. The classic windower sorts each
	// partition by the window ORDER BY before evaluation; we must do the same,
	// otherwise ranking values and running aggregates are computed over rows
	// in arbitrary (e.g. insertion) order. All functions in a window share the
	// same ORDER BY, so sorting by the first function's ordering suffices.
	// outPart is a sub-slice of out pointing at this partition's output rows;
	// because it shares the underlying array with out, every write to outPart[i]
	// lands at out[startIdx+i]. All downstream evaluation operates on outPart
	// (the sorted order), never on the unsorted input rows, so that ordering-
	// sensitive values such as RANGE offset frames (which binary-search the
	// order column) and ranking peer groups stay aligned with the output rows.
	outPart := out[startIdx : startIdx+len(part)]
	if len(p.plan.Fns) > 0 && len(p.plan.Fns[0].Ordering) > 0 {
		colOrder := make(sqlbase.ColumnOrdering, len(p.plan.Fns[0].Ordering))
		for i, c := range p.plan.Fns[0].Ordering {
			colOrder[i] = sqlbase.ColumnOrderInfo{ColIdx: c, Direction: encoding.Ascending}
		}
		sort.SliceStable(outPart, func(i, j int) bool {
			cmp, err := outPart[i].Compare(p.outTypes, p.da, colOrder, p.EvalCtx, outPart[j])
			if err != nil {
				panic(err)
			}
			return cmp < 0
		})
	}
	for _, fn := range p.plan.Fns {
		switch fn.Kind {
		case "ranking":
			p.computeRanking(outPart, fn)
		case "agg":
			p.computeAggregateWindow(outPart, fn)
		}
	}
}

// computeRanking evaluates a ranking window function (row_number/rank/
// dense_rank) over the partition. Ranking functions ignore the frame and
// operate over the whole partition, ordered by the function's ORDER BY.
// outPart is the partition's output-row slice, already sorted by the window
// ORDER BY (computePartition sorts it in place) and sharing the backing array
// with out, so writing outPart[i] updates the partition's i-th output row.
func (p *arrowWindowerProcessor) computeRanking(outPart sqlbase.EncDatumRows, fn arrowWindowFnPlan) {
	switch fn.Func {
	case "row_number":
		for i := range outPart {
			outPart[i][fn.OutputIdx] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i + 1))}
		}
	case "rank", "dense_rank":
		peerStart := 0
		denseGroupOrdinal := 1
		lastPeer := -1 // index of the first row of the last peer group
		for i := 1; i <= len(outPart); i++ {
			atEnd := i == len(outPart)
			newPeer := atEnd
			if !atEnd {
				k1, err := p.peerKey(outPart[i-1], fn.Ordering)
				if err != nil {
					panic(err)
				}
				k2, err := p.peerKey(outPart[i], fn.Ordering)
				if err != nil {
					panic(err)
				}
				newPeer = !bytes.Equal(k1, k2)
			}
			if newPeer {
				// rank = 1-based index of the first row of the peer group.
				// dense_rank = 1-based peer-group ordinal.
				rankVal := tree.NewDInt(tree.DInt(peerStart + 1))
				dense := tree.NewDInt(tree.DInt(denseGroupOrdinal))
				for j := peerStart; j < i; j++ {
					if fn.Func == "rank" {
						outPart[j][fn.OutputIdx] = sqlbase.EncDatum{Datum: rankVal}
					} else {
						outPart[j][fn.OutputIdx] = sqlbase.EncDatum{Datum: dense}
					}
				}
				lastPeer = peerStart
				peerStart = i
				denseGroupOrdinal++
			}
		}
		_ = lastPeer
	}
}

// computeAggregateWindow evaluates an aggregate window function over the
// partition. outPart is the partition's output-row slice, already sorted by the
// window ORDER BY (computePartition sorts it in place) and sharing the backing
// array with out, so outPart[i] is both the i-th input row of the partition and
// the slot where its aggregate result is written. Operating on outPart (rather
// than the unsorted input rows) is what makes ordering-sensitive frames — in
// particular RANGE offset frames, whose binary search assumes a sorted order
// column — correct.
//
//	- whole-partition frame: aggregate over all rows, written to every row.
//	- ROWS/RANGE offset frame: per-row sliding window.
//	- default / UNBOUNDED PRECEDING TO CURRENT ROW: running aggregate over peer
//	  groups.
func (p *arrowWindowerProcessor) computeAggregateWindow(outPart sqlbase.EncDatumRows, fn arrowWindowFnPlan) {
	whole := fn.Frame.isWholePartitionFrame()
	if whole {
		acc := &windowAccumulator{fn: fn.Func}
		for _, row := range outPart {
			if err := acc.consume(p, row, fn.Input); err != nil {
				panic(err)
			}
		}
		val, err := acc.finalize(p)
		if err != nil {
			panic(err)
		}
		for i := range outPart {
			outPart[i][fn.OutputIdx] = val
		}
		return
	}
	// ROWS/RANGE offset frame: each row gets the aggregate over its own [start,end]
	// window. outPart is sorted by the window ORDER BY, so the window indices map
	// directly onto it and RANGE's value-based binary search is valid.
	if fn.Frame != nil && !fn.Frame.isWholePartitionFrame() && fn.Frame.hasOffset() {
		// Per-row windows. ROWS offset frames count physical rows; RANGE offset
		// frames span every row whose ORDER BY value is within the offset window
		// of the current row's value (value-based, peer-group aware).
		var orderVals []tree.Datum
		if fn.Frame.Mode == "range" {
			// The RANGE offset is measured against the ordering column (the last
			// column in the ORDER BY list; PARTITION BY columns precede it).
			orderCol := fn.Ordering[len(fn.Ordering)-1]
			orderVals = make([]tree.Datum, len(outPart))
			for j := range outPart {
				if err := outPart[j][orderCol].EnsureDecoded(&p.inTypes[orderCol], p.da); err != nil {
					panic(err)
				}
				orderVals[j] = outPart[j][orderCol].Datum
			}
		}
		for i := range outPart {
			var s, e int
			if fn.Frame.Mode == "range" {
				s, e = rangeFrameBounds(i, len(outPart), orderVals, p.EvalCtx, fn.Frame)
			} else {
				s, e = fn.Frame.frameBounds(i, len(outPart))
			}
			acc := &windowAccumulator{fn: fn.Func}
			for j := s; j <= e; j++ {
				if err := acc.consume(p, outPart[j], fn.Input); err != nil {
					panic(err)
				}
			}
			val, err := acc.finalize(p)
			if err != nil {
				panic(err)
			}
			outPart[i][fn.OutputIdx] = val
		}
		return
	}
	// Default / ROWS UNBOUNDED PRECEDING TO CURRENT ROW (and RANGE running):
	// running aggregate accumulated over peer groups.
	acc := &windowAccumulator{fn: fn.Func}
	peerStart := 0
	for i := 1; i <= len(outPart); i++ {
		atEnd := i == len(outPart)
		newPeer := atEnd
		if !atEnd {
			k1, err := p.peerKey(outPart[i-1], fn.Ordering)
			if err != nil {
				panic(err)
			}
			k2, err := p.peerKey(outPart[i], fn.Ordering)
			if err != nil {
				panic(err)
			}
			newPeer = !bytes.Equal(k1, k2)
		}
		// Consume the current row into the running aggregate.
		if err := acc.consume(p, outPart[i-1], fn.Input); err != nil {
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
				outPart[j][fn.OutputIdx] = val
			}
			peerStart = i
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
	fn    string
	set   bool
	value tree.Datum // current accumulated value
	count int64      // for avg
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
