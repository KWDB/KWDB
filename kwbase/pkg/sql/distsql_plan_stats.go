// Copyright 2017 The Cockroach Authors.
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
//
// This file implements functions for creating and managing the physical plan
// of database statistics collection. This file focuses on defining structures
// and methods for creating, planning statistical collection jobs
// that gather vital metadata for database optimization and query planning.
//
// Usage:
// - This component is primarily used within the database's query optimizer
//   to facilitate better query planning through accurate statistics.
// - It includes functionality for time series statistics collection, which is
//   essential for understanding database performance over time and for making
//   informed decisions on data storage and retrieval optimizations.
//
// Design:
// - The file implements several key data structures such as `TsSamplerConfig` and
//   `requestedStat`, which configure the details and parameters for statistics
//   collection jobs.
// - It includes comprehensive methods like `createStatsPlan` and `createTsStatsPlan`
//   that outline the step-by-step processes to configure and execute plans for
//   statistics collection, ensuring that all necessary data is captured efficiently
//   and effectively.
//
// Limitations:
// - The performance of the statistics collection can be significantly influenced
//   by the underlying hardware and database configuration, which may need
//   tuning to handle large-scale operations effectively.

package sql

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
)

type requestedStat struct {
	columns             []sqlbase.ColumnID
	histogram           bool
	histogramMaxBuckets int
	name                string
	columnsTypes        []sqlbase.ColumnType
	hasAllPTag          bool
	// virtual sketch is used to determine the row count of tag table
	virtualSketch bool
	// sortedHistogram supports sorted histogram collection for entities in tag table
	sortedHistogram sqlbase.SortedHistogramInfo
}

// TsSamplerConfig Configure parameters for TsSamplerConfig
type TsSamplerConfig struct {
	PlanCtx    *PlanningCtx
	TabDesc    *sqlbase.ImmutableTableDescriptor
	ColCfg     scanColumnsConfig
	ReqStats   []requestedStat
	SampleSize uint32
	histogram  []bool
	StatsCols  sqlbase.ResultColumns
	// Type of attribute of column
	StatsColsTypes []types.T
	ResultCols     sqlbase.ResultColumns
	// distinguish tag, primary tag and data column type
	ColumnTypes []uint32
}

const histogramSamples = 10000
const histogramBuckets = 200

// maxTimestampAge is the maximum allowed age of a scan timestamp during table
// stats collection, used when creating statistics AS OF SYSTEM TIME. The
// timestamp is advanced during long operations as needed. See TableReaderSpec.
//
// The lowest TTL we recommend is 10 minutes. This value must be be lower than
// that.
var maxTimestampAge = settings.RegisterDurationSetting(
	"sql.stats.max_timestamp_age",
	"maximum age of timestamp during table statistics collection",
	5*time.Minute,
)

func (dsp *DistSQLPlanner) createStatsPlan(
	planCtx *PlanningCtx,
	desc *sqlbase.ImmutableTableDescriptor,
	reqStats []requestedStat,
	job *jobs.Job,
) (PhysicalPlan, error) {
	if len(reqStats) == 0 {
		return PhysicalPlan{}, errors.New("no stats requested")
	}

	details := job.Details().(jobspb.CreateStatsDetails)

	// Calculate the set of columns we need to scan.
	var colCfg scanColumnsConfig
	var tableColSet util.FastIntSet
	for _, s := range reqStats {
		for _, c := range s.columns {
			if !tableColSet.Contains(int(c)) {
				tableColSet.Add(int(c))
				colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(c))
			}
		}
	}

	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	err := scan.initDescDefaults(nil /* planDependencies */, colCfg)
	if err != nil {
		return PhysicalPlan{}, err
	}
	sb := span.MakeBuilder(desc.TableDesc(), scan.index)
	scan.spans, err = sb.UnconstrainedSpans(scan.isDeleteSource)
	if err != nil {
		return PhysicalPlan{}, err
	}

	p, err := dsp.createTableReaders(planCtx, &scan, nil /* overrideResultColumns */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	if details.AsOf != nil {
		// If the read is historical, set the max timestamp age.
		val := maxTimestampAge.Get(&dsp.st.SV)
		for i := range p.Processors {
			spec := p.Processors[i].Spec.Core.TableReader
			spec.MaxTimestampAgeNanos = uint64(val)
		}
	}

	sketchSpecs := make([]execinfrapb.SketchSpec, len(reqStats))
	sampledColumnIDs := make([]sqlbase.ColumnID, scan.valNeededForCol.Len())
	for i, s := range reqStats {
		spec := execinfrapb.SketchSpec{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: uint32(s.histogramMaxBuckets),
			Columns:             make([]uint32, len(s.columns)),
			ColumnTypes:         make([]uint32, len(s.columns)),
			StatName:            s.name,
		}
		for i, colID := range s.columns {
			colIdx, ok := scan.colIdxMap[colID]
			if !ok {
				panic("necessary column not scanned")
			}
			streamColIdx := p.PlanToStreamColMap[colIdx]
			spec.Columns[i] = uint32(streamColIdx)
			spec.ColumnTypes[i] = uint32(sqlbase.ColumnType_TYPE_DATA)
			sampledColumnIDs[streamColIdx] = colID
		}

		sketchSpecs[i] = spec
	}

	// Set up the samplers.
	sampler := &execinfrapb.SamplerSpec{Sketches: sketchSpecs}
	for _, s := range reqStats {
		sampler.MaxFractionIdle = details.MaxFractionIdle
		if s.histogram {
			sampler.SampleSize = histogramSamples
		}
	}

	// The sampler outputs the original columns plus a rank column and four sketch columns.
	outTypes := make([]types.T, 0, len(p.ResultTypes)+5)
	outTypes = append(outTypes, p.ResultTypes...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, *types.Int)
	// A BYTES column with the sketch data.
	outTypes = append(outTypes, *types.Bytes)

	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Sampler: sampler},
		execinfrapb.PostProcessSpec{},
		outTypes,
		execinfrapb.Ordering{},
	)

	// Estimate the expected number of rows based on existing stats in the cache.
	tableStats, err := planCtx.planner.execCfg.TableStatsCache.GetTableStats(planCtx.ctx, desc.ID)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := stats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		// Convert to a signed integer first to make the linter happy.
		rowsExpected = uint64(int64(
			// The total expected number of rows is the same number that was measured
			// most recently, plus some overhead for possible insertions.
			float64(tableStats[0].RowCount) * (1 + overhead),
		))
	}

	var jobID int64
	if job.ID() != nil {
		jobID = *job.ID()
	}

	// Set up the final SampleAggregator stage.
	agg := &execinfrapb.SampleAggregatorSpec{
		Sketches:         sketchSpecs,
		SampleSize:       sampler.SampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.ID,
		JobID:            jobID,
		RowsExpected:     rowsExpected,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.nodeDesc.NodeID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].Node
	}
	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{SampleAggregator: agg},
		execinfrapb.PostProcessSpec{},
		[]types.T{},
	)

	return p, nil
}

//	createTsStatsPlan is used to creates a distributed SQL physical plan for collecting statistics of time series tables.
//	It configures and initializes necessary components for sampling, scanning, and aggregating data from the specified table.
//
//	     ts reader operator: Complete scanning of statistics column data
//	     ts sampler operator: Complete the collection of time series information, including sampling set, number of table rows, number of null values
//	                            and coded data used to calculate distinct-count values
//	     samplerAgg operator: Summarize the collected data, TODO: implement tsSamplerAgg in feature
//
// Parameters:
// - ctx: planCtx: Context for planning the query.
// - phs: desc: Descriptor of table.
// - reqStats: Statistics requested by the user.
//
// Returns:
// - PhysicalPlan: distributed execution plans are used to create statistics
// - An error if there is any issue in creating stats physical plan.
func (dsp *DistSQLPlanner) createTsStatsPlan(
	planCtx *PlanningCtx,
	desc *sqlbase.ImmutableTableDescriptor,
	reqStats []requestedStat,
	job *jobs.Job,
) (PhysicalPlan, error) {
	// Configure parameters for TsSamplerConfig
	tsSamplerCfg, err := initTsSamplerConfiguration(planCtx, desc, reqStats)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Get the desired column description
	colIdxMap, valNeededForCol, err := setupColumnConfiguration(&tsSamplerCfg)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Create the table readers; for this we initialize a dummy tsScanNode.
	tsScan := createTsScanNode(tsSamplerCfg)

	p, err := dsp.createTSReaders(planCtx, tsScan)
	if err != nil || len(p.Processors) == 0 {
		return PhysicalPlan{}, err
	}
	// Set output types for processors that need to push data to agents.
	setOutputTypes := func(p *PhysicalPlan) {
		for _, idx := range p.ResultRouters {
			if p.Processors[idx].ExecInTSEngine {
				p.Processors[idx].TSSpec.Post.OutputTypes = make([]types.Family, len(p.ResultTypes))
				for i, typ := range p.ResultTypes {
					p.Processors[idx].TSSpec.Post.OutputTypes[i] = typ.InternalType.Family
				}
			}
		}
	}
	// Add a layer of output types to the operator pushed down to the agent
	setOutputTypes(&p)

	tsSamplerSpec, err := initTsSamplerSpec(tsSamplerCfg, colIdxMap)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// The ts sampler outputs the original columns plus a rank column and four sketch columns.
	outTypes := make([]types.T, 0, len(tsSamplerCfg.StatsCols)+7)
	outTypes = append(outTypes, tsSamplerCfg.StatsColsTypes...)
	extraColumns := []types.T{*types.Int, *types.Int, *types.Int, *types.Int, *types.Bytes, *types.Int, *types.Int}
	outTypes = append(outTypes, extraColumns...)
	p.AddTSNoGroupingStage(
		execinfrapb.TSProcessorCoreUnion{Sampler: tsSamplerSpec},
		execinfrapb.TSPostProcessSpec{},
		outTypes,
		execinfrapb.Ordering{},
	)
	if len(p.Processors) == 0 {
		return PhysicalPlan{}, nil
	}
	// Set up the ts samplers.
	updateOutput(&tsSamplerCfg, &p)

	// Default parallel degree is 0, serial
	if err := dsp.addSynchronizerForTS(&p, 0); err != nil {
		return PhysicalPlan{}, err
	}
	// Add a layer of output types to the operator pushed down to the agent
	setOutputTypes(&p)

	for i, idx := range p.ResultRouters {
		if p.Processors[idx].ExecInTSEngine && p.Processors[idx].Node != dsp.nodeDesc.NodeID {
			p.AddNoopImplementation(
				execinfrapb.PostProcessSpec{}, idx, i, nil, &p.ResultTypes,
			)
		}
	}

	sketchAggSpecs := make([]execinfrapb.SketchSpec, len(reqStats))
	sampledColumnIDs := make([]sqlbase.ColumnID, valNeededForCol.Len())
	for i, s := range reqStats {
		spec := execinfrapb.SketchSpec{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: uint32(s.histogramMaxBuckets),
			Columns:             make([]uint32, len(s.columns)),
			ColumnTypes:         make([]uint32, len(s.columns)),
			StatName:            s.name,
			HasAllPTag:          s.hasAllPTag,
			VirtualSketch:       s.virtualSketch,
			SortedHistogramInfo: s.sortedHistogram,
		}
		for i, colID := range s.columns {
			colIdx, ok := colIdxMap[colID]
			if !ok {
				panic("necessary column not scanned")
			}
			streamColIdx := p.PlanToStreamColMap[colIdx]
			spec.Columns[i] = uint32(streamColIdx)
			spec.ColumnTypes[i] = tsSamplerCfg.ColumnTypes[colIdx]
			sampledColumnIDs[streamColIdx] = colID
		}

		sketchAggSpecs[i] = spec
	}

	// Estimate the expected number of rows based on existing stats in the cache.
	tableStats, err := planCtx.planner.execCfg.TableStatsCache.GetTableStats(planCtx.ctx, desc.ID)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := stats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		// Convert to a signed integer first to make the linter happy.
		rowsExpected = uint64(int64(
			// The total expected number of rows is the same number that was measured
			// most recently, plus some overhead for possible insertions.
			float64(tableStats[0].RowCount) * (1 + overhead),
		))
	}

	var jobID int64
	if job.ID() != nil {
		jobID = *job.ID()
	}

	details := job.Details().(jobspb.CreateStatsDetails)
	// Set up the final SampleAggregator stage.
	agg := &execinfrapb.SampleAggregatorSpec{
		Sketches:         sketchAggSpecs,
		SampleSize:       histogramSamples,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.ID,
		JobID:            jobID,
		RowsExpected:     rowsExpected,
		IsTsStats:        true,
		TimeZone:         details.TimeZone,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.nodeDesc.NodeID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].Node
	}
	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{SampleAggregator: agg},
		execinfrapb.PostProcessSpec{},
		[]types.T{},
	)
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	planCtx *PlanningCtx, job *jobs.Job,
) (PhysicalPlan, error) {
	details := job.Details().(jobspb.CreateStatsDetails)
	reqStats := make([]requestedStat, len(details.ColumnStats))
	histogramCollectionEnabled := stats.HistogramClusterMode.Get(&dsp.st.SV)
	for i := 0; i < len(reqStats); i++ {
		histogram := details.ColumnStats[i].HasHistogram && histogramCollectionEnabled
		columnTypes := make([]sqlbase.ColumnType, 0, len(details.ColumnStats[i].ColumnTypes))
		for _, v := range details.ColumnStats[i].ColumnTypes {
			columnTypes = append(columnTypes, sqlbase.ColumnType(v))
		}
		reqStats[i] = requestedStat{
			columns:             details.ColumnStats[i].ColumnIDs,
			histogram:           histogram,
			histogramMaxBuckets: histogramBuckets,
			name:                details.Name,
			columnsTypes:        columnTypes,
			hasAllPTag:          details.ColumnStats[i].HasAllPTag,
			virtualSketch:       details.ColumnStats[i].VirtualSketch,
			sortedHistogram:     details.ColumnStats[i].SortedHistogram,
		}
	}
	tableDesc := sqlbase.NewImmutableTableDescriptor(details.Table)

	if details.IsTsStats {
		if tableDesc.TsTable.HashNum == 0 {
			tableDesc.TsTable.HashNum = api.HashParamV2
		}
		return dsp.createTsStatsPlan(planCtx, tableDesc, reqStats, job)
	}
	return dsp.createStatsPlan(planCtx, tableDesc, reqStats, job)
}

func (dsp *DistSQLPlanner) planAndRunCreateStats(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	job *jobs.Job,
	resultRows *RowResultWriter,
) error {
	ctx = logtags.AddTag(ctx, "create-stats-distsql", nil)

	physPlan, err := dsp.createPlanForCreateStats(planCtx, job)
	if err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, &physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		evalCtx.ExecCfg.LeaseHolderCache,
		txn,
		func(ts hlc.Timestamp) {
			evalCtx.ExecCfg.Clock.Update(ts)
		},
		evalCtx.Tracing,
	)
	defer recv.Release()

	dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, nil /* finishedSetupFn */)()
	return resultRows.Err()
}
