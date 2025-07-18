// Copyright 2016 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stringarena"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// distinct is the physical processor implementation of the DISTINCT relational operator.
type distinct struct {
	execinfra.ProcessorBase

	input            execinfra.RowSource
	types            []types.T
	haveLastGroupKey bool
	lastGroupKey     sqlbase.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	orderedCols      []uint32
	distinctCols     util.FastIntSet
	memAcc           mon.BoundAccount
	datumAlloc       sqlbase.DatumAlloc
	scratch          []byte
	nullsAreDistinct bool
	nullCount        uint32
	errorOnDup       string
}

// sortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type sortedDistinct struct {
	distinct
}

var _ execinfra.Processor = &distinct{}
var _ execinfra.RowSource = &distinct{}
var _ execinfra.OpNode = &distinct{}

const distinctProcName = "distinct"

var _ execinfra.Processor = &sortedDistinct{}
var _ execinfra.RowSource = &sortedDistinct{}
var _ execinfra.OpNode = &sortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// newDistinct instantiates a new Distinct processor.
func newDistinct(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.DistinctSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, errors.AssertionFailedf("0 distinct columns specified for distinct processor")
	}

	var distinctCols, orderedCols util.FastIntSet
	allSorted := true

	for _, col := range spec.OrderedColumns {
		orderedCols.Add(int(col))
	}
	for _, col := range spec.DistinctColumns {
		if !orderedCols.Contains(int(col)) {
			allSorted = false
		}
		distinctCols.Add(int(col))
	}
	if !orderedCols.SubsetOf(distinctCols) {
		return nil, errors.AssertionFailedf("ordered cols must be a subset of distinct cols")
	}

	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "distinct-mem")
	d := &distinct{
		input:            input,
		orderedCols:      spec.OrderedColumns,
		distinctCols:     distinctCols,
		memAcc:           memMonitor.MakeBoundAccount(),
		types:            input.OutputTypes(),
		nullsAreDistinct: spec.NullsAreDistinct,
		errorOnDup:       spec.ErrorOnDup,
	}

	var returnProcessor execinfra.RowSourcedProcessor = d
	if allSorted {
		// We can use the faster sortedDistinct processor.
		sd := &sortedDistinct{
			distinct: *d,
		}
		// Set d to the new distinct copy for further initialization.
		// TODO(asubiotto): We should have a distinctBase, rather than making a copy
		// of a distinct processor.
		d = &sd.distinct
		returnProcessor = sd
	}

	if err := d.Init(
		d, post, d.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{d.input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				d.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	d.lastGroupKey = d.Out.RowAlloc.AllocRow(len(d.types))
	d.haveLastGroupKey = false
	// If we set up the arena when d is created, the pointer to the memAcc
	// will be changed because the sortedDistinct case makes a copy of d.
	// So we have to set up the account here.
	d.arena = stringarena.Make(&d.memAcc)

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		d.input = newInputStatCollector(d.input)
		d.FinishTrace = d.outputStatsToTrace
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (d *distinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, distinctProcName)
}

// InitProcessorProcedure init processor in procedure
func (d *sortedDistinct) InitProcessorProcedure(txn *kv.Txn) {
	if d.EvalCtx.IsProcedure {
		if d.FlowCtx != nil {
			d.FlowCtx.Txn = txn
		}
		d.Closed = false
		d.State = execinfra.StateRunning
		d.Out.SetRowIdx(0)
	}
}

// Start is part of the RowSource interface.
func (d *sortedDistinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, sortedDistinctProcName)
}

func (d *distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
		return false, nil
	}
	for _, colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(
			&d.types[colIdx], &d.datumAlloc, d.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}

		// If null values are treated as distinct from one another, then a grouping
		// column with a NULL value means that the row should never match any other
		// row.
		if d.nullsAreDistinct && d.lastGroupKey[colIdx].IsNull() {
			return false, nil
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	var err error
	foundNull := false
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		appendTo, err = datum.Fingerprint(&d.types[i], &d.datumAlloc, appendTo)
		if err != nil {
			return nil, err
		}

		// If null values are treated as distinct from one another, then append
		// a unique identifier to the end of the encoding, so that the row will
		// always be in its own distinct group.
		if d.nullsAreDistinct && datum.IsNull() {
			foundNull = true
		}
	}

	if foundNull {
		appendTo = encoding.EncodeUint32Ascending(appendTo, d.nullCount)
		d.nullCount++
	}

	return appendTo, nil
}

func (d *distinct) close() {
	if d.InternalClose() {
		d.memAcc.Close(d.PbCtx())
		d.MemMonitor.Stop(d.PbCtx())
	}
}

// Next is part of the RowSource interface.
func (d *distinct) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			copy(d.lastGroupKey, row)
			d.haveLastGroupKey = true
			if err := d.arena.UnsafeReset(d.PbCtx()); err != nil {
				d.MoveToDraining(err)
				break
			}
			d.seen = make(map[string]struct{})
		}

		// Check whether row is distinct.
		if _, ok := d.seen[string(encoding)]; ok {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise an error.
				err = pgerror.Newf(pgcode.CardinalityViolation, d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}
		s, err := d.arena.AllocBytes(d.PbCtx(), encoding)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.seen[s] = struct{}{}

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// Next is part of the RowSource interface.
//
// sortedDistinct is simpler than distinct. All it has to do is keep track
// of the last row it saw, emitting if the new row is different.
func (d *sortedDistinct) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		if matched {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise an error.
				err = pgerror.Newf(pgcode.CardinalityViolation, d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}

		d.haveLastGroupKey = true
		copy(d.lastGroupKey, row)

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (d *distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

// InitProcessorProcedure init processor in procedure
func (d *distinct) InitProcessorProcedure(txn *kv.Txn) {
	if d.EvalCtx.IsProcedure {
		if d.FlowCtx != nil {
			d.FlowCtx.Txn = txn
		}
		d.Closed = false
		d.State = execinfra.StateRunning
		d.Out.SetRowIdx(0)
	}
}

var _ execinfrapb.DistSQLSpanStats = &DistinctStats{}

const distinctTagPrefix = "distinct."

// Stats implements the SpanStats interface.
func (ds *DistinctStats) Stats() map[string]string {
	inputStatsMap := ds.InputStats.Stats(distinctTagPrefix)
	inputStatsMap[distinctTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ds.MaxAllocatedMem)
	return inputStatsMap
}

// TsStats is stats of analyse in time series
func (ds *DistinctStats) TsStats() map[int32]map[string]string {
	return nil
}

// GetSpanStatsType check type of spanStats
func (ds *DistinctStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ds *DistinctStats) StatsForQueryPlan() []string {
	stats := ds.InputStats.StatsForQueryPlan("")

	if ds.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ds.MaxAllocatedMem)))
	}

	return stats
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (ds *DistinctStats) TsStatsForQueryPlan() map[int32][]string {
	return nil
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (d *distinct) outputStatsToTrace() {
	is, ok := getInputStats(d.FlowCtx, d.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(d.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp, &DistinctStats{InputStats: is, MaxAllocatedMem: d.MemMonitor.MaximumBytes()},
		)
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (d *distinct) ChildCount(verbose bool) int {
	if _, ok := d.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (d *distinct) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := d.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to distinct is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
