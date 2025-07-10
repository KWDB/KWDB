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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// inputStatCollector wraps an execinfra.RowSource and collects stats from it.
type inputStatCollector struct {
	execinfra.RowSource
	InputStats
}

var _ execinfra.RowSource = &inputStatCollector{}
var _ execinfra.OpNode = &inputStatCollector{}

// newInputStatCollector creates a new inputStatCollector that wraps the given
// input.
func newInputStatCollector(input execinfra.RowSource) *inputStatCollector {
	return &inputStatCollector{RowSource: input}
}

// ChildCount is part of the OpNode interface.
func (isc *inputStatCollector) ChildCount(verbose bool) int {
	if _, ok := isc.RowSource.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the OpNode interface.
func (isc *inputStatCollector) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return isc.RowSource.(execinfra.OpNode)
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// InitProcessorProcedure init processor in procedure
func (isc *inputStatCollector) InitProcessorProcedure(txn *kv.Txn) {
	isc.RowSource.InitProcessorProcedure(txn)
}

// Start implements the RowSource interface. It calls Start on the embedded
// RowSource and collects stats.
func (isc *inputStatCollector) Start(ctx context.Context) context.Context {
	start := timeutil.Now()
	ctx = isc.RowSource.Start(ctx)
	isc.StallTime += timeutil.Since(start)
	return ctx
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *inputStatCollector) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	start := timeutil.Now()
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	isc.StallTime += timeutil.Since(start)
	return row, meta
}

const (
	rowsReadTagSuffix  = "input.rows"
	stallTimeTagSuffix = "stalltime"
	// MaxMemoryTagSuffix is the tag suffix for the max memory used stat.
	MaxMemoryTagSuffix = "mem.max"
	// MaxDiskTagSuffix is the tag suffix for the max disk used stat.
	MaxDiskTagSuffix = "disk.max"
	// bytesReadTagSuffix is the tag suffix for the bytes read stat.
	bytesReadTagSuffix = "bytes.read"
)

// Stats is a utility method that returns a map of the InputStats` stats to
// output to a trace as tags. The given prefix is prefixed to the keys.
func (is InputStats) Stats(prefix string) map[string]string {
	return map[string]string{
		prefix + rowsReadTagSuffix:  fmt.Sprintf("%d", is.NumRows),
		prefix + stallTimeTagSuffix: is.RoundStallTime().String(),
	}
}

const (
	rowsReadQueryPlanSuffix  = "rows read"
	stallTimeQueryPlanSuffix = "stall time"
	// buildTimeQueryPlanSuffix only do the build time print for multiple model processing
	// when the switch is on and the server starts with single node mode.
	buildTimeQueryPlanSuffix = "build time"
	// MaxMemoryQueryPlanSuffix is the tag suffix for the max memory used.
	MaxMemoryQueryPlanSuffix = "max memory used"
	// MaxDiskQueryPlanSuffix is the tag suffix for the max disk used.
	MaxDiskQueryPlanSuffix = "max disk used"
	// bytesReadQueryPlanSuffix is the tag suffix for the bytes read.
	bytesReadQueryPlanSuffix = "bytes read"
	// outputRowsQueryPlanSuffix is rows of aggregation
	outputRowsQueryPlanSuffix = "output rows"
)

// StatsForQueryPlan is a utility method that returns a list of the InputStats'
// stats to output on a query plan. The given prefix is prefixed to each element
// in the returned list.
func (is InputStats) StatsForQueryPlan(prefix string) []string {
	// Only do the build print for multiple model processing
	// when the switch is on and the server starts with single node mode.
	if is.BuildTime != 0 {
		return []string{
			fmt.Sprintf("%s%s: %d", prefix, rowsReadQueryPlanSuffix, is.NumRows),
			fmt.Sprintf("%s%s: %v", prefix, stallTimeQueryPlanSuffix, is.RoundStallTime()),
			fmt.Sprintf("%s%s: %v", prefix, buildTimeQueryPlanSuffix, is.RoundBuildTime()),
		}
	}
	return []string{
		fmt.Sprintf("%s%s: %d", prefix, rowsReadQueryPlanSuffix, is.NumRows),
		fmt.Sprintf("%s%s: %v", prefix, stallTimeQueryPlanSuffix, is.RoundStallTime()),
	}
}

// RoundStallTime returns the InputStats' StallTime rounded to the nearest
// time.Millisecond.
func (is InputStats) RoundStallTime() time.Duration {
	return is.StallTime.Round(time.Microsecond)
}

// RoundBuildTime returns the InputStats' BuildTime rounded to the nearest
// time.Millisecond.
// RoundBuildTime only do the build time check for multiple model processing
// when the switch is on and the server starts with single node mode
func (is InputStats) RoundBuildTime() time.Duration {
	return is.BuildTime.Round(time.Microsecond)
}

// rowFetcherStatCollector is a wrapper on top of a row.Fetcher that collects stats.
//
// Only row.Fetcher methods that collect stats are overridden.
type rowFetcherStatCollector struct {
	*row.Fetcher
	// stats contains the collected stats.
	stats              InputStats
	startScanStallTime time.Duration
}

var _ rowFetcher = &rowFetcherStatCollector{}

// newRowFetcherStatCollector returns a new rowFetcherStatCollector.
func newRowFetcherStatCollector(f *row.Fetcher) *rowFetcherStatCollector {
	return &rowFetcherStatCollector{Fetcher: f}
}

// NextRow is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) NextRow(
	ctx context.Context,
) (sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error) {
	start := timeutil.Now()
	row, t, i, err := c.Fetcher.NextRow(ctx)
	if row != nil {
		c.stats.NumRows++
	}
	c.stats.StallTime += timeutil.Since(start)
	return row, t, i, err
}

// StartScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartScan(
	ctx context.Context,
	txn *kv.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// StartInconsistentScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartInconsistentScan(
	ctx context.Context,
	db *kv.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// getInputStats is a utility function to check whether the given input is
// collecting stats, returning true and the stats if so. If false is returned,
// the input is not collecting stats.
func getInputStats(flowCtx *execinfra.FlowCtx, input execinfra.RowSource) (InputStats, bool) {
	isc, ok := input.(*inputStatCollector)
	if !ok {
		return InputStats{}, false
	}
	return getStatsInner(flowCtx, isc.InputStats), true
}

func getStatsInner(flowCtx *execinfra.FlowCtx, stats InputStats) InputStats {
	if flowCtx.Cfg.TestingKnobs.DeterministicStats {
		stats.StallTime = 0
	}
	return stats
}

// getFetcherInputStats is a utility function to check whether the given input
// is collecting row fetcher stats, returning true and the stats if so. If
// false is returned, the input is not collecting row fetcher stats.
func getFetcherInputStats(flowCtx *execinfra.FlowCtx, f rowFetcher) (InputStats, bool) {
	rfsc, ok := f.(*rowFetcherStatCollector)
	if !ok {
		return InputStats{}, false
	}
	// Add row fetcher start scan stall time to Next() stall time.
	if !flowCtx.Cfg.TestingKnobs.DeterministicStats {
		rfsc.stats.StallTime += rfsc.startScanStallTime
	}
	return getStatsInner(flowCtx, rfsc.stats), true
}

// BuildResponseSpan build span of response with RowStats
func BuildResponseSpan(rowStats execinfra.RowStats) []string {
	responseSpan := make([]string, 2)
	if rowStats.StallTime > 0 {
		responseSpan[0] = fmt.Sprintf("%s: %d", rowsReadQueryPlanSuffix, rowStats.NumRows)
		responseSpan[1] = fmt.Sprintf("%s: %v", stallTimeQueryPlanSuffix, rowStats.StallTime.Round(time.Microsecond))
	}
	return responseSpan
}
