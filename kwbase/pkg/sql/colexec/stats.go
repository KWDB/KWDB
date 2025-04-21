// Copyright 2019 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/opentracing/opentracing-go"
)

// VectorizedStatsCollector collects VectorizedStats on Operators.
//
// If two Operators are connected (i.e. one is an input to another), the
// corresponding VectorizedStatsCollectors are also "connected" by sharing a
// StopWatch.
type VectorizedStatsCollector struct {
	Operator
	NonExplainable
	execpb.VectorizedStats

	// inputWatch is a single stop watch that is shared with all the input
	// Operators. If the Operator doesn't have any inputs (like colBatchScan),
	// it is not shared with anyone. It is used by the wrapped Operator to
	// measure its stall or execution time.
	inputWatch *timeutil.StopWatch
	// outputWatch is a stop watch that is shared with the Operator that the
	// wrapped Operator is feeding into. It must be started right before
	// returning a batch when Nexted. It is used by the "output" Operator.
	outputWatch *timeutil.StopWatch

	memMonitors  []*mon.BytesMonitor
	diskMonitors []*mon.BytesMonitor
}

var _ Operator = &VectorizedStatsCollector{}

// NewVectorizedStatsCollector creates a new VectorizedStatsCollector which
// wraps op that corresponds to a processor with ProcessorID id. isStall
// indicates whether stall or execution time is being measured. inputWatch must
// be non-nil.
func NewVectorizedStatsCollector(
	op Operator,
	id int32,
	isStall bool,
	inputWatch *timeutil.StopWatch,
	memMonitors []*mon.BytesMonitor,
	diskMonitors []*mon.BytesMonitor,
) *VectorizedStatsCollector {
	if inputWatch == nil {
		execerror.VectorizedInternalPanic("input watch for VectorizedStatsCollector is nil")
	}
	return &VectorizedStatsCollector{
		Operator:        op,
		VectorizedStats: execpb.VectorizedStats{ID: id, Stall: isStall},
		inputWatch:      inputWatch,
		memMonitors:     memMonitors,
		diskMonitors:    diskMonitors,
	}
}

// SetOutputWatch sets vsc.outputWatch to outputWatch. It is used to "connect"
// this VectorizedStatsCollector to the next one in the chain.
func (vsc *VectorizedStatsCollector) SetOutputWatch(outputWatch *timeutil.StopWatch) {
	vsc.outputWatch = outputWatch
}

// Next is part of Operator interface.
func (vsc *VectorizedStatsCollector) Next(ctx context.Context) coldata.Batch {
	if vsc.outputWatch != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. In order to avoid double counting the time
		// actually spent in the current "input" Operator, we're stopping the stop
		// watch of the other "output" Operator before doing any computations here.
		vsc.outputWatch.Stop()
	}

	var batch coldata.Batch
	if vsc.VectorizedStats.Stall {
		// We're measuring stall time, so there are no inputs into the wrapped
		// Operator, and we need to start the stop watch ourselves.
		vsc.inputWatch.Start()
	}
	batch = vsc.Operator.Next(ctx)
	if batch.Length() > 0 {
		vsc.NumBatches++
		vsc.NumTuples += int64(batch.Length())
	}
	vsc.inputWatch.Stop()
	if vsc.outputWatch != nil {
		// vsc.outputWatch is non-nil which means that this Operator is outputting
		// the batches into another one. To allow for measuring the execution time
		// of that other Operator, we're starting the stop watch right before
		// returning batch.
		vsc.outputWatch.Start()
	}
	return batch
}

// FinalizeStats records the time measured by the stop watch into the stats.
func (vsc *VectorizedStatsCollector) FinalizeStats() {
	vsc.Time = vsc.inputWatch.Elapsed()

	for _, memMon := range vsc.memMonitors {
		vsc.MaxAllocatedMem += memMon.MaximumBytes()
	}

	for _, diskMon := range vsc.diskMonitors {
		vsc.MaxAllocatedDisk += diskMon.MaximumBytes()
	}
}

// FinalizeTsStats records the time measured by the stop watch into the ts stats.
func (vsc *VectorizedStatsCollector) FinalizeTsStats(span opentracing.Span) rowexec.TsInputStats {
	if noop, ok := vsc.Operator.(*noopOperator); ok {
		if vecStats, ok1 := noop.input.(*VectorizedStatsCollector); ok1 {
			if tsReader, ok2 := vecStats.Operator.(*tsReaderOp); ok2 {
				var tsi rowexec.TsInputStats
				for _, stats := range tsReader.statsList {
					tsi.SetTsInputStats(stats)
				}
				return tsi
				// tracing.SetSpanStats(span, &vsc.VectorizedStats)
			}
		}
	}
	return rowexec.TsInputStats{}
}
