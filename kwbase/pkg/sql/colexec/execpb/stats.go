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

package execpb

import (
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

var _ tracing.SpanStats = &VectorizedStats{}
var _ execinfrapb.DistSQLSpanStats = &VectorizedStats{}

const (
	batchesOutputTagSuffix     = "output.batches"
	tuplesOutputTagSuffix      = "output.tuples"
	selectivityTagSuffix       = "selectivity"
	stallTimeTagSuffix         = "time.stall"
	executionTimeTagSuffix     = "time.execution"
	maxVecMemoryBytesTagSuffix = "mem.vectorized.max"
	maxVecDiskBytesTagSuffix   = "disk.vectorized.max"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeTagSuffix
	} else {
		timeSuffix = executionTimeTagSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	return map[string]string{
		batchesOutputTagSuffix:     fmt.Sprintf("%d", vs.NumBatches),
		tuplesOutputTagSuffix:      fmt.Sprintf("%d", vs.NumTuples),
		selectivityTagSuffix:       fmt.Sprintf("%.2f", selectivity),
		timeSuffix:                 fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
		maxVecMemoryBytesTagSuffix: fmt.Sprintf("%d", vs.MaxAllocatedMem),
		maxVecDiskBytesTagSuffix:   fmt.Sprintf("%d", vs.MaxAllocatedDisk),
	}
}

// TsStats is stats of analyse in time series
func (vs *VectorizedStats) TsStats() map[int32]map[string]string {
	return vs.TsInputStats.TsStats()
}

// GetSpanStatsType check type of spanStats
func (vs *VectorizedStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeVector + vs.TsInputStats.GetSpanStatsType()
}

const (
	batchesOutputQueryPlanSuffix     = "batches output"
	tuplesOutputQueryPlanSuffix      = "tuples output"
	selectivityQueryPlanSuffix       = "selectivity"
	stallTimeQueryPlanSuffix         = "stall time"
	executionTimeQueryPlanSuffix     = "execution time"
	maxVecMemoryBytesQueryPlanSuffix = "max vectorized memory allocated"
	maxVecDiskBytesQueryPlanSuffix   = "max vectorized disk allocated"
)

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeQueryPlanSuffix
	} else {
		timeSuffix = executionTimeQueryPlanSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	stats := []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesOutputQueryPlanSuffix, vs.NumTuples),
		fmt.Sprintf("%s: %.2f", selectivityQueryPlanSuffix, selectivity),
		fmt.Sprintf("%s: %v", timeSuffix, vs.Time.Round(time.Microsecond)),
	}

	if vs.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", maxVecMemoryBytesQueryPlanSuffix, humanizeutil.IBytes(vs.MaxAllocatedMem)))
	}

	if vs.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", maxVecDiskBytesQueryPlanSuffix, humanizeutil.IBytes(vs.MaxAllocatedDisk)))
	}
	return stats
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (vs *VectorizedStats) TsStatsForQueryPlan() map[int32][]string {
	return vs.TsInputStats.TsStatsForQueryPlan()
}
