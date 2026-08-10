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

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package physicalplan

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// arrowProjectionEnabledSetting, when set, routes arrow-computable projection
// expressions (e.g. `a+b`) through the Arrow compute engine instead of the
// scalar tree-evaluator. It is enabled by default; Arrow is the primary
// execution path with row-by-row evaluation as the fallback for expressions
// the Arrow engine does not cover.
var arrowProjectionEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_projection.enabled",
	"if set, arrow-computable projection expressions are evaluated using the Arrow compute engine",
	true,
)

func arrowProjectionEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowProjectionEnabledSetting.Get(&evalCtx.Settings.SV)
}

// ArrowFilterEnabled reports whether arrow-computable filters are routed
// through the Arrow compute engine.
func ArrowFilterEnabled(evalCtx *tree.EvalContext) bool {
	return arrowFilterEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// ArrowAggregatorEnabled reports whether aggregations are routed through the
// Arrow compute engine.
func ArrowAggregatorEnabled(evalCtx *tree.EvalContext) bool {
	return arrowAggregatorEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// ArrowScanEnabled reports whether the scan (table reader) side may feed the
// Arrow compute engine. It is the master gate for the Arrow path: when it is
// off, no Arrow operator can consume a scan, so the whole Arrow pipeline is
// bypassed and rows flow row-by-row as before.
func ArrowScanEnabled(evalCtx *tree.EvalContext) bool {
	return arrowScanEnabled(evalCtx)
}

// ArrowJoinEnabled reports whether equi-joins are routed through the Arrow
// compute engine.
func ArrowJoinEnabled(evalCtx *tree.EvalContext) bool {
	return arrowJoinEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// ArrowSorterEnabled reports whether sorts are routed through the Arrow compute
// engine.
func ArrowSorterEnabled(evalCtx *tree.EvalContext) bool {
	return arrowSorterEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// ArrowDistinctEnabled reports whether dedup (distinct) is routed through the
// Arrow compute engine.
func ArrowDistinctEnabled(evalCtx *tree.EvalContext) bool {
	return arrowDistinctEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// ArrowWindowerEnabled reports whether (the supported subset of) window
// functions are routed through the Arrow compute engine.
func ArrowWindowerEnabled(evalCtx *tree.EvalContext) bool {
	return arrowWindowerEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// arrowFilterEnabledSetting routes arrow-computable boolean filter expressions
// (e.g. `a > 1`, `a > b AND b < 5`) through the Arrow compute engine.
var arrowFilterEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_filter.enabled",
	"if set, arrow-computable filter expressions are evaluated using the Arrow compute engine",
	true,
)

// arrowAggregatorEnabledSetting routes sum/count/min/max/mean aggregations
// through the Arrow compute engine.
var arrowAggregatorEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_aggregator.enabled",
	"if set, aggregations are evaluated using the Arrow compute engine",
	true,
)

// arrowJoinEnabledSetting routes equi-joins through the Arrow compute engine.
var arrowJoinEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_join.enabled",
	"if set, equi-joins are evaluated using the Arrow compute engine",
	true,
)

// arrowScanEnabledSetting is the master gate for the Arrow path. When off, the
// scan side does not feed the Arrow engine, so no Arrow operator can consume a
// scan and the whole Arrow pipeline is bypassed (rows stay row-by-row).
var arrowScanEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_scan.enabled",
	"if set, table scans may feed the Arrow compute engine (master gate for the Arrow path)",
	true,
)

func arrowFilterEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowFilterEnabledSetting.Get(&evalCtx.Settings.SV)
}

func arrowAggregatorEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowAggregatorEnabledSetting.Get(&evalCtx.Settings.SV)
}

func arrowJoinEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowJoinEnabledSetting.Get(&evalCtx.Settings.SV)
}

func arrowScanEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowScanEnabledSetting.Get(&evalCtx.Settings.SV)
}

// ArrowTsScanEnabled reports whether a time-series scan may feed the Arrow
// compute engine (§6.7 / §6.8 审订: TS scan is only relationally coupled via its
// buffer format, so it can be an Arrow data source). It is an independent gate
// from ArrowScanEnabled (which governs relational KV scans).
func ArrowTsScanEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowTsScanEnabledSetting.Get(&evalCtx.Settings.SV)
}

// arrowTsScanEnabledSetting gates time-series scan -> Arrow (prototype, see
// rowexec.arrowTsReader). Off by default until the Arrow TS read path is wired
// into the planner and validated end-to-end.
var arrowTsScanEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_ts_scan.enabled",
	"if set, time-series scans may feed the Arrow compute engine (prototype: TS read emits Arrow Records)",
	false,
)

// arrowSorterEnabledSetting routes ORDER BY sorting through the Arrow compute
// engine.
var arrowSorterEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_sorter.enabled",
	"if set, sorts (ORDER BY) are evaluated using the Arrow compute engine",
	true,
)

// arrowDistinctEnabledSetting routes DISTINCT dedup through the Arrow compute
// engine.
var arrowDistinctEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_distinct.enabled",
	"if set, DISTINCT dedup is evaluated using the Arrow compute engine",
	true,
)

func arrowSorterEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowSorterEnabledSetting.Get(&evalCtx.Settings.SV)
}

func arrowDistinctEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowDistinctEnabledSetting.Get(&evalCtx.Settings.SV)
}

var arrowWindowerEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_windower.enabled",
	"if set, the supported subset of window functions (no-frame partition aggregates) is evaluated using the Arrow compute engine",
	true,
)

func arrowWindowerEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowWindowerEnabledSetting.Get(&evalCtx.Settings.SV)
}

var arrowUnionAllEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_union_all.enabled",
	"if set, UNION ALL set operations are merged using the Arrow compute engine (a zero-copy concatenation of the input records instead of the row-based no-op merge)",
	true,
)

func arrowUnionAllEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowUnionAllEnabledSetting.Get(&evalCtx.Settings.SV)
}

// ArrowUnionAllEnabled reports whether UNION ALL is merged through the Arrow
// compute engine.
func ArrowUnionAllEnabled(evalCtx *tree.EvalContext) bool {
	return arrowUnionAllEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

var arrowValuesEnabledSetting = settings.RegisterBoolSetting(
	"sql.arrow_values.enabled",
	"if set, the Values data source (pre-canned constant rows) is emitted as a single Arrow Record source instead of the classic row-based Values processor",
	false,
)

func arrowValuesEnabled(evalCtx *tree.EvalContext) bool {
	if evalCtx == nil || evalCtx.Settings == nil {
		return false
	}
	return arrowValuesEnabledSetting.Get(&evalCtx.Settings.SV)
}

// ArrowValuesEnabled reports whether Values is emitted through the Arrow compute
// engine. Defaults to false (conservative; flipped to true once the Arrow Values
// path is validated end-to-end), gated behind the master ArrowScan switch.
func ArrowValuesEnabled(evalCtx *tree.EvalContext) bool {
	return arrowValuesEnabled(evalCtx) && ArrowScanEnabled(evalCtx)
}

// Processor contains the information associated with a processor in a plan.
type Processor struct {
	// Node where the processor must be instantiated.
	Node roachpb.NodeID

	// Spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	Spec execinfrapb.ProcessorSpec

	LogicalSequenceID []uint64
}

// ProcessorIdx identifies a processor by its index in PhysicalPlan.Processors.
type ProcessorIdx int

// Stream connects the output router of one processor to an input synchronizer
// of another processor.
type Stream struct {
	// SourceProcessor index (within the same plan).
	SourceProcessor ProcessorIdx

	// SourceRouterSlot identifies the position of this stream among the streams
	// that originate from the same router. This is important when routing by hash
	// where the order of the streams in the OutputRouterSpec matters.
	SourceRouterSlot int

	// DestProcessor index (within the same plan).
	DestProcessor ProcessorIdx

	// DestInput identifies the input of DestProcessor (some processors have
	// multiple inputs).
	DestInput int
}

// PhysicalPlan represents a network of processors and streams along with
// information about the results output by this network. The results come from
// unconnected output routers of a subset of processors; all these routers
// output the same kind of data (same schema).
type PhysicalPlan struct {
	St *cluster.Settings
	// Processors in the plan.
	Processors []Processor

	// LocalProcessors contains all of the planNodeToRowSourceWrappers that were
	// installed in this physical plan to wrap any planNodes that couldn't be
	// properly translated into DistSQL processors. This will be empty if no
	// wrapping had to happen.
	LocalProcessors []execinfra.LocalProcessor

	// LocalProcessorIndexes contains pointers to all of the RowSourceIdx fields
	// of the  LocalPlanNodeSpecs that were created. This list is in the same
	// order as LocalProcessors, and is kept up-to-date so that LocalPlanNodeSpecs
	// always have the correct index into the LocalProcessors slice.
	LocalProcessorIndexes []*uint32

	// Streams accumulates the streams in the plan - both local (intra-node) and
	// remote (inter-node); when we have a final plan, the streams are used to
	// generate processor input and output specs (see PopulateEndpoints).
	Streams []Stream

	// ResultRouters identifies the output routers which output the results of the
	// plan. These are the routers to which we have to connect new streams in
	// order to extend the plan.
	//
	// The processors which have this routers are all part of the same "stage":
	// they have the same "schema" and PostProcessSpec.
	//
	// We assume all processors have a single output so we only need the processor
	// index.
	ResultRouters []ProcessorIdx

	// Synchronizer child process idx
	SynchronizerChildRouters []ProcessorIdx

	// TsTableReaderRouters tsTableReader process idx
	TsTableReaderRouters []ProcessorIdx

	// ResultTypes is the schema (column types) of the rows produced by the
	// ResultRouters.
	//
	// This is aliased with InputSyncSpec.ColumnTypes, so it must not be modified
	// in-place during planning.
	ResultTypes []types.T

	// MergeOrdering is the ordering guarantee for the result streams that must be
	// maintained when the streams eventually merge. The column indexes refer to
	// columns for the rows produced by ResultRouters.
	//
	// Empty when there is a single result router. The reason is that maintaining
	// an ordering sometimes requires to add columns to streams for the sole
	// reason of correctly merging the streams later (see AddProjection); we don't
	// want to pay this cost if we don't have multiple streams to merge.
	MergeOrdering execinfrapb.Ordering

	// Used internally for numbering stages.
	stageCounter int32

	// Used internally to avoid creating flow IDs for local flows. This boolean
	// specifies whether there is more than one node involved in a plan.
	remotePlan bool

	// MaxEstimatedRowCount tracks the maximum estimated row count that a table
	// reader in this plan will output. This information is used to decide
	// whether to use the vectorized execution engine.
	MaxEstimatedRowCount uint64
	// TotalEstimatedScannedRows is the sum of the row count estimate of all the
	// table readers in the plan.
	TotalEstimatedScannedRows uint64

	// the processors are all execute on ts engine
	AllProcessorsExecInTSEngine bool

	// Noop processor input number of gateway node from multiple node
	GateNoopInput int

	// TS processor type
	TsOperator execinfrapb.OperatorType

	// SQL use for trace display.
	SQL string

	// InlcudeApplyJoin whether has applyJoin, if InlcudeApplyJoin is true, we do not need to set inputsToDrain for tsInsertSelecter;
	// if the SQL contains apply-join and set inputsToDrain for ts insert select, it break down.
	InlcudeApplyJoin     bool
	UseQueryShortCircuit bool
	UseCompressType      int64
}

// IsRemotePlan is true when ts plan is dist.
func (p *PhysicalPlan) IsRemotePlan() bool {
	return p.remotePlan
}

// LimitInfo limit in time series select
type LimitInfo struct {
	Limit    uint32
	Offset   uint32
	HasLimit bool
}

// IsDistInTS is true when ts plan is dist.
func (p *PhysicalPlan) IsDistInTS() bool {
	if p.remotePlan {
		for _, v := range p.Processors {
			if v.ExecInTSEngine() {
				return true
			}
		}
	}
	return false
}

// NewStageID creates a stage identifier that can be used in processor specs.
func (p *PhysicalPlan) NewStageID() int32 {
	p.stageCounter++
	return p.stageCounter
}

// AddProcessor adds a processor to a PhysicalPlan and returns the index that
// can be used to refer to that processor.
func (p *PhysicalPlan) AddProcessor(proc Processor) ProcessorIdx {
	idx := ProcessorIdx(len(p.Processors))
	p.Processors = append(p.Processors, proc)
	return idx
}

// SetMergeOrdering sets p.MergeOrdering.
func (p *PhysicalPlan) SetMergeOrdering(o execinfrapb.Ordering) {
	if len(p.ResultRouters) > 1 {
		p.MergeOrdering = o
	} else {
		p.MergeOrdering = execinfrapb.Ordering{}
	}
}

// ProcessorCorePlacement indicates on which node a particular processor core
// needs to be planned.
type ProcessorCorePlacement struct {
	SQLInstanceID roachpb.NodeID
	Core          execinfrapb.ProcessorCoreUnion
	// EstimatedRowCount, if set to non-zero, is the optimizer's guess of how
	// many rows will be emitted from this processor.
	EstimatedRowCount uint64
}

// AddNoInputStage creates a stage of processors that don't have any input from
// the other stages (if such exist). nodes and cores must be a one-to-one
// mapping so that a particular processor core is planned on the appropriate
// node.
func (p *PhysicalPlan) AddNoInputStage(
	corePlacements []ProcessorCorePlacement,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	newOrdering execinfrapb.Ordering,
) {
	// Note that in order to find out whether we have a remote processor it is
	// not sufficient to have len(corePlacements) be greater than one - we might
	// plan multiple table readers on the gateway if the plan is local.
	//containsRemoteProcessor := false
	//for i := range corePlacements {
	//	if corePlacements[i].SQLInstanceID != p.GatewaySQLInstanceID {
	//		containsRemoteProcessor = true
	//		break
	//	}
	//}
	stageID := p.NewStageID()
	//stageID := p.NewStage(containsRemoteProcessor, false /* allowPartialDistribution */)
	p.ResultRouters = make([]ProcessorIdx, len(corePlacements))
	for i := range p.ResultRouters {
		proc := Processor{
			Node: corePlacements[i].SQLInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Core: corePlacements[i].Core,
				Post: post,
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID: stageID,
				//ResultTypes:       outputTypes,
				//EstimatedRowCount: corePlacements[i].EstimatedRowCount,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	p.SetMergeOrdering(newOrdering)
}

// AddNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddNoGroupingStage(
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	newOrdering execinfrapb.Ordering,
) {
	p.AddNoGroupingStageWithCoreFunc(
		func(_ int, _ *Processor) execinfrapb.ProcessorCoreUnion { return core },
		post,
		outputTypes,
		newOrdering,
	)
}

// AddNoGroupingStageForTSNoop adds a ts noop processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddNoGroupingStageForTSNoop(newOrdering *execinfrapb.Ordering) {
	post := execinfrapb.PostProcessSpec{}
	var tmp execinfrapb.Ordering
	if newOrdering != nil {
		tmp = *newOrdering
	} else {
		tmp = p.MergeOrdering
	}
	p.AddTSNoGroupingStageWithCoreFunc(
		func(_ int, _ *Processor) execinfrapb.ProcessorCoreUnion {
			return execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}
		},
		post,
		p.ResultTypes,
		tmp,
	)
}

// AddTSTableReader add timeseries table reader to get data from ae engine
func (p *PhysicalPlan) AddTSTableReader(outTypes []types.T) {
	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		execinfrapb.PostProcessSpec{OutputTypes: outTypes},
		p.ResultTypes, p.MergeOrdering,
	)
}

// AddNoop add noop
func (p *PhysicalPlan) AddNoop(
	post *execinfrapb.PostProcessSpec, newOrdering *execinfrapb.Ordering,
) {
	postTmp := execinfrapb.PostProcessSpec{OutputTypes: p.ResultTypes}
	if post != nil {
		postTmp = *post
	}
	var tmp execinfrapb.Ordering
	if newOrdering != nil {
		tmp = *newOrdering
	} else {
		tmp = p.MergeOrdering
	}

	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		postTmp,
		p.ResultTypes,
		tmp,
	)
}

// CheckAndAddNoopForAgent adds a noop processor for each result router, on the same node
func (p *PhysicalPlan) CheckAndAddNoopForAgent() {
	if len(p.ResultRouters) > 0 && p.Processors[0].ExecInTSEngine() {
		p.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			execinfrapb.PostProcessSpec{OutputTypes: p.ResultTypes},
			p.ResultTypes,
			p.MergeOrdering,
		)
	}
}

// AddTSNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddTSNoGroupingStage(
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	newOrdering execinfrapb.Ordering,
) {
	p.AddTSNoGroupingStageWithCoreFunc(
		func(_ int, _ *Processor) execinfrapb.ProcessorCoreUnion { return core },
		post,
		outputTypes,
		newOrdering,
	)
}

// AddTSNoGroupingStageWithCoreFunc is like AddNoGroupingStage, but creates a core
// spec based on the input processor's spec.
func (p *PhysicalPlan) AddTSNoGroupingStageWithCoreFunc(
	coreFunc func(int, *Processor) execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	newOrdering execinfrapb.Ordering,
) {
	post.OutputTypes = outputTypes
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			Node: prevProc.Node,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					Type:        execinfrapb.InputSyncSpec_UNORDERED,
					ColumnTypes: p.ResultTypes,
				}},
				Core: coreFunc(int(resultProc), prevProc),
				Post: post,
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				Engine: execinfrapb.ProcessorSpec_TimeSeries,
			},
		}

		pIdx := p.AddProcessor(proc)

		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			DestProcessor:    pIdx,
			SourceRouterSlot: 0,
			DestInput:        0,
		})

		p.ResultRouters[i] = pIdx
	}
	p.ResultTypes = outputTypes
	p.SetMergeOrdering(newOrdering)
}

func (p *PhysicalPlan) addNoopForGatewayNode(nodeID roachpb.NodeID) {
	p.AddSingleGroupStage(
		nodeID,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{
			InputNum:   uint32(p.GateNoopInput),
			TsOperator: p.TsOperator,
		}},
		execinfrapb.PostProcessSpec{OutputTypes: p.ResultTypes},
		p.ResultTypes,
	)
	if len(p.ResultRouters) != 1 {
		panic(fmt.Sprintf("%d results after single group stage", len(p.ResultRouters)))
	}
	// LogicalSequenceID is serial-number of porcessor, noop of gateway node only one, so value is 0
	p.Processors[p.ResultRouters[0]].LogicalSequenceID = []uint64{0}
}

// AddNoopToTsProcessors add noop processor to processor of time series
// forceMerge: whether forced forceMerge date to gateway node
func (p *PhysicalPlan) AddNoopToTsProcessors(nodeID roachpb.NodeID, local bool, forceMerge bool) {
	if p.ResultRouters == nil {
		return
	}
	childExecInTSEngineOld := p.ChildIsExecInTSEngine()
	// add noop-processor to processor of time series in other Node
	for i, idx := range p.ResultRouters {
		if p.Processors[idx].ExecInTSEngine() && p.Processors[idx].Node != nodeID {
			p.AddNoopImplementation(
				execinfrapb.PostProcessSpec{}, idx, i, &p.ResultRouters, &p.ResultTypes,
			)
		} else if p.Processors[idx].Node == nodeID && !local {
			p.AddNoopImplementation(
				execinfrapb.PostProcessSpec{}, idx, i, &p.ResultRouters, &p.ResultTypes,
			)
		}
	}

	// add noop-processor in gateway node for multi node
	if forceMerge || (childExecInTSEngineOld && local && len(p.ResultRouters) > 0) {
		p.addNoopForGatewayNode(nodeID)
	}
}

// AddNoopImplementation add noop process for ts engine
func (p *PhysicalPlan) AddNoopImplementation(
	post execinfrapb.PostProcessSpec,
	idx ProcessorIdx,
	i int,
	resultRouters *[]ProcessorIdx,
	types *[]types.T,
) {
	stageID := p.NewStageID()
	post.OutputTypes = *types
	proc := Processor{
		Node: p.Processors[idx].Node,
		Spec: execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				Type:        execinfrapb.InputSyncSpec_UNORDERED,
				ColumnTypes: *types,
			}},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			StageID: stageID,
		},
	}

	pIdx := p.AddProcessor(proc)

	p.Streams = append(p.Streams, Stream{
		SourceProcessor:  idx,
		DestProcessor:    pIdx,
		SourceRouterSlot: 0,
		DestInput:        0,
	})

	// if resultRouters is p variable, the resultRouters is nil
	if i == -1 {
		if resultRouters != nil {
			*resultRouters = append(*resultRouters, pIdx)
		} else {
			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
	} else {
		if resultRouters != nil {
			(*resultRouters)[i] = pIdx
		} else {
			p.ResultRouters[i] = pIdx
		}
	}
}

// AddTSNoopImplementation add noop process for ts engine
func (p *PhysicalPlan) AddTSNoopImplementation(
	post execinfrapb.PostProcessSpec,
	idx ProcessorIdx,
	i int,
	resultRouters *[]ProcessorIdx,
	types *[]types.T,
) {
	//stageID := p.NewStageID()
	proc := Processor{
		Node: p.Processors[idx].Node,
		Spec: execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				Type:        execinfrapb.InputSyncSpec_UNORDERED,
				ColumnTypes: *types,
			}},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			Engine: execinfrapb.ProcessorSpec_TimeSeries,
		},
	}

	pIdx := p.AddProcessor(proc)

	p.Streams = append(p.Streams, Stream{
		SourceProcessor:  idx,
		DestProcessor:    pIdx,
		SourceRouterSlot: 0,
		DestInput:        0,
	})

	// if resultRouters is p variable, the resultRouters is nil
	if i == -1 {
		if resultRouters != nil {
			*resultRouters = append(*resultRouters, pIdx)
		} else {
			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
	} else {
		if resultRouters != nil {
			(*resultRouters)[i] = pIdx
		} else {
			p.ResultRouters[i] = pIdx
		}
	}
}

// AddNoopForJoinToAgent ...
func (p *PhysicalPlan) AddNoopForJoinToAgent(
	nodes []roachpb.NodeID,
	leftRouters []ProcessorIdx,
	rightRouters []ProcessorIdx,
	thisNodeID roachpb.NodeID,
	leftTypes, rightTypes *[]types.T,
) {
	if len(nodes) == 1 {
		for k, idx := range leftRouters {
			if p.Processors[idx].ExecInTSEngine() && p.Processors[idx].Node != thisNodeID {
				p.AddNoopImplementation(
					execinfrapb.PostProcessSpec{}, idx, -1, nil, leftTypes,
				)
				leftRouters[k] = p.ResultRouters[k]
			}
		}
		for k, idx := range rightRouters {
			if p.Processors[idx].ExecInTSEngine() && p.Processors[idx].Node != thisNodeID {
				p.AddNoopImplementation(
					execinfrapb.PostProcessSpec{}, idx, -1, nil, rightTypes,
				)
				if len(rightRouters) == len(p.ResultRouters) {
					rightRouters[k] = p.ResultRouters[k]
				} else {
					rightRouters[k] = p.ResultRouters[len(leftRouters)+k]
				}
			}
		}
	} else {
		for i, idx := range leftRouters {
			if p.Processors[idx].ExecInTSEngine() {
				p.AddNoopImplementation(
					execinfrapb.PostProcessSpec{}, idx, i, &leftRouters, leftTypes,
				)
			}
		}
		for i, idx := range rightRouters {
			if p.Processors[idx].ExecInTSEngine() {
				p.AddNoopImplementation(
					execinfrapb.PostProcessSpec{}, idx, i, &rightRouters, rightTypes,
				)
			}
		}
	}
}

// AddNoGroupingStageWithCoreFunc is like AddNoGroupingStage, but creates a core
// spec based on the input processor's spec.
func (p *PhysicalPlan) AddNoGroupingStageWithCoreFunc(
	coreFunc func(int, *Processor) execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	newOrdering execinfrapb.Ordering,
) {
	post.OutputTypes = outputTypes
	stageID := p.NewStageID()
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			Node: prevProc.Node,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					Type:        execinfrapb.InputSyncSpec_UNORDERED,
					ColumnTypes: p.ResultTypes,
				}},
				Core: coreFunc(int(resultProc), prevProc),
				Post: post,
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)

		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			DestProcessor:    pIdx,
			SourceRouterSlot: 0,
			DestInput:        0,
		})

		p.ResultRouters[i] = pIdx
	}
	p.ResultTypes = outputTypes
	p.SetMergeOrdering(newOrdering)
}

// MergeResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
// forceSerialization determines whether the streams are forced to be serialized
// (i.e. whether we don't want any parallelism).
func (p *PhysicalPlan) MergeResultStreams(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering execinfrapb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
	forceSerialization bool,
) {
	proc := &p.Processors[destProcessor]
	// We want to use unordered synchronizer if the ordering is empty and
	// we're not being forced to serialize streams. Note that ordered
	// synchronizers support the case of an empty ordering - they will be
	// merging the result streams by fully consuming one stream at a time
	// before moving on to the next one.
	useUnorderedSync := len(ordering.Columns) == 0 && !forceSerialization
	if len(resultRouters) == 1 {
		// However, if we only have a single result router, then there is
		// nothing to merge, and we unconditionally will use the unordered
		// synchronizer since it is more efficient.
		useUnorderedSync = true
	}

	inputSpec := &proc.Spec.Input

	if useUnorderedSync {
		(*inputSpec)[destInput].Type = execinfrapb.InputSyncSpec_UNORDERED
	} else {
		(*inputSpec)[destInput].Type = execinfrapb.InputSyncSpec_ORDERED
		(*inputSpec)[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}

}

// MergeTSResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
// forceSerialization determines whether the streams are forced to be serialized
// (i.e. whether we don't want any parallelism).
func (p *PhysicalPlan) MergeTSResultStreams(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering execinfrapb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
	forceSerialization bool,
) {
	proc := &p.Processors[destProcessor]
	// We want to use unordered synchronizer if the ordering is empty and
	// we're not being forced to serialize streams. Note that ordered
	// synchronizers support the case of an empty ordering - they will be
	// merging the result streams by fully consuming one stream at a time
	// before moving on to the next one.
	useUnorderedSync := len(ordering.Columns) == 0 && !forceSerialization
	if len(resultRouters) == 1 {
		// However, if we only have a single result router, then there is
		// nothing to merge, and we unconditionally will use the unordered
		// synchronizer since it is more efficient.
		useUnorderedSync = true
	}
	if useUnorderedSync {
		proc.Spec.Input[destInput].Type = execinfrapb.InputSyncSpec_UNORDERED
	} else {
		proc.Spec.Input[destInput].Type = execinfrapb.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}
}

// AddSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node. The
// previous stage (ResultRouters) are all connected to this processor.
func (p *PhysicalPlan) AddSingleGroupStage(
	nodeID roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
) {
	proc := Processor{
		Node: nodeID,
		Spec: execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.ResultTypes,
			}},
			Core: core,
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			StageID: p.NewStageID(),
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false /* forceSerialization */)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.ResultTypes = outputTypes
	p.MergeOrdering = execinfrapb.Ordering{}
}

// AddTSSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node. The
// previous stage (ResultRouters) are all connected to this processor.
func (p *PhysicalPlan) AddTSSingleGroupStage(
	nodeID roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []types.T,
	final bool,
) {
	proc := Processor{
		Node: nodeID,
		Spec: execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.ResultTypes,
			}},
			Core: core,
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			Engine:           execinfrapb.ProcessorSpec_TimeSeries,
			FinalTsProcessor: final,
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeTSResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false /* forceSerialization */)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.ResultTypes = outputTypes
	p.MergeOrdering = execinfrapb.Ordering{}
}

// CheckLastStagePost checks that the processors of the last stage of the
// PhysicalPlan have identical post-processing, returning an error if not.
func (p *PhysicalPlan) CheckLastStagePost() error {
	if p.ChildIsExecInTSEngine() {
		var resultRouters []ProcessorIdx
		if p.ChildIsTSParallelProcessor() {
			resultRouters = p.SynchronizerChildRouters
		} else {
			resultRouters = p.ResultRouters
		}
		post := p.Processors[resultRouters[0]].Spec.Post

		// All processors of a stage should be identical in terms of post-processing;
		// verify this assumption.
		for i := 1; i < len(resultRouters); i++ {
			pi := &p.Processors[resultRouters[i]].Spec.Post
			if pi.Filter != post.Filter ||
				pi.Projection != post.Projection ||
				len(pi.OutputColumns) != len(post.OutputColumns) ||
				len(pi.RenderExprs) != len(post.RenderExprs) {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
			}
			for j, col := range pi.OutputColumns {
				if col != post.OutputColumns[j] {
					return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
				}
			}
			for j, col := range pi.RenderExprs {
				if col.String() != post.RenderExprs[j].String() {
					return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
				}
			}
		}

		return nil
	}
	post := p.Processors[p.ResultRouters[0]].Spec.Post

	// All processors of a stage should be identical in terms of post-processing;
	// verify this assumption.
	for i := 1; i < len(p.ResultRouters); i++ {
		pi := &p.Processors[p.ResultRouters[i]].Spec.Post
		if pi.Filter != post.Filter ||
			pi.Projection != post.Projection ||
			len(pi.OutputColumns) != len(post.OutputColumns) ||
			len(pi.RenderExprs) != len(post.RenderExprs) {
			return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
		}
		for j, col := range pi.OutputColumns {
			if col != post.OutputColumns[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
			}
		}
		for j, col := range pi.RenderExprs {
			if col != post.RenderExprs[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post.String(), pi)
			}
		}
	}

	return nil
}

// GetLastStagePost returns the PostProcessSpec for the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) GetLastStagePost() execinfrapb.PostProcessSpec {
	if err := p.CheckLastStagePost(); err != nil {
		panic(err)
	}
	return p.Processors[p.ResultRouters[0]].Spec.Post
}

// GetLastStageTSPost returns the TSPostProcessSpec for the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) GetLastStageTSPost() execinfrapb.PostProcessSpec {
	if p.ChildIsTSParallelProcessor() {
		return p.Processors[p.SynchronizerChildRouters[0]].Spec.Post
	}
	return p.Processors[p.ResultRouters[0]].Spec.Post
}

// SetLastStagePost changes the PostProcess spec of the processors in the last
// stage (ResultRouters).
// The caller must update the ordering via SetOrdering.
func (p *PhysicalPlan) SetLastStagePost(post execinfrapb.PostProcessSpec, outputTypes []types.T) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post = post
		if len(outputTypes) == 0 {
			p.Processors[pIdx].Spec.Post.OutputTypes = p.ResultTypes
		} else {
			p.Processors[pIdx].Spec.Post.OutputTypes = outputTypes
		}
	}
	p.ResultTypes = outputTypes
}

// SetLastStageTSPost changes the TSPostProcessSpec spec of the processors in the last
// stage (ResultRouters).
// The caller must update the ordering via SetOrdering.
func (p *PhysicalPlan) SetLastStageTSPost(post execinfrapb.PostProcessSpec, outputTypes []types.T) {
	// limit push down to synchronizer scan child spec
	if p.ChildIsTSParallelProcessor() {
		for _, pIdx := range p.SynchronizerChildRouters {
			p.Processors[pIdx].Spec.Post = post
		}
	} else {
		for _, pIdx := range p.ResultRouters {
			p.Processors[pIdx].Spec.Post = post
		}
	}

	p.ResultTypes = outputTypes
}

// ChildIsTSParallelProcessor check last is synchronizer
func (p *PhysicalPlan) ChildIsTSParallelProcessor() bool {
	if !p.ChildIsExecInTSEngine() {
		return false
	}
	return p.Processors[p.ResultRouters[0]].Spec.Core.TsSynchronizer != nil
}

// HasTSParallelProcessor check has synchronizer
func (p *PhysicalPlan) HasTSParallelProcessor() bool {
	for i := range p.Processors {
		if p.Processors[i].ExecInTSEngine() && p.Processors[i].Spec.Core.TsSynchronizer != nil {
			return true
		}
	}
	return false
}

// CheckLastIsNoop check last is noop
func (p *PhysicalPlan) CheckLastIsNoop() bool {
	return p.Processors[p.ResultRouters[0]].Spec.Core.Noop != nil
}

// AppendLastSequenceID modify the SequenceID structure of the processor to correspond with the logical operator
func (p *PhysicalPlan) AppendLastSequenceID(se uint64) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].LogicalSequenceID = append(p.Processors[pIdx].LogicalSequenceID, se)
	}
}

func isIdentityProjection(columns []uint32, numExistingCols int) bool {
	if len(columns) != numExistingCols {
		return false
	}
	for i, c := range columns {
		if c != uint32(i) {
			return false
		}
	}
	return true
}

// AddProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. The Ordering is updated;
// columns in the ordering are added to the projection as needed.
//
// The PostProcessSpec may not be updated if the resulting projection keeps all
// the columns in their original order.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
func (p *PhysicalPlan) AddProjection(columns []uint32) {
	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.ResultTypes)) {
		return
	}

	// Update the ordering.
	if len(p.MergeOrdering.Columns) > 0 {
		newOrdering := make([]execinfrapb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			// Look for the column in the new projection.
			found := -1
			for j, projCol := range columns {
				if projCol == c.ColIdx {
					found = j
				}
			}
			if found == -1 {
				// We have a column that is not in the projection but will be necessary
				// later when the streams are merged; add it.
				found = len(columns)
				columns = append(columns, c.ColIdx)
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	newResultTypes := make([]types.T, len(columns))
	for i, c := range columns {
		newResultTypes[i] = p.ResultTypes[c]
	}

	post := p.GetLastStagePost()

	if post.RenderExprs != nil {
		// Apply the projection to the existing rendering; in other words, keep
		// only the renders needed by the new output columns, and reorder them
		// accordingly.
		oldRenders := post.RenderExprs
		post.RenderExprs = make([]execinfrapb.Expression, len(columns))
		for i, c := range columns {
			post.RenderExprs[i] = oldRenders[c]
		}
	} else {
		// There is no existing rendering; we can use OutputColumns to set the
		// projection.
		if post.Projection {
			// We already had a projection: compose it with the new one.
			for i, c := range columns {
				columns[i] = post.OutputColumns[c]
			}
		}
		post.OutputColumns = columns
		post.Projection = true
	}

	p.SetLastStagePost(post, newResultTypes)
}

// AddTSProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. The Ordering is updated;
// columns in the ordering are added to the projection as needed.
//
// The PostProcessSpec may not be updated if the resulting projection keeps all
// the columns in their original order.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
func (p *PhysicalPlan) AddTSProjection(columns []uint32) {
	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.ResultTypes)) && len(columns) != 0 {
		return
	}

	// Update the ordering.
	if len(p.MergeOrdering.Columns) > 0 {
		newOrdering := make([]execinfrapb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			// Look for the column in the new projection.
			found := -1
			for j, projCol := range columns {
				if projCol == c.ColIdx {
					found = j
				}
			}
			if found == -1 {
				// We have a column that is not in the projection but will be necessary
				// later when the streams are merged; add it.
				found = len(columns)
				columns = append(columns, c.ColIdx)
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	newResultTypes := make([]types.T, len(columns))

	for i, c := range columns {
		newResultTypes[i] = p.ResultTypes[c]
	}

	post := p.GetLastStageTSPost()

	if post.RenderExprs != nil {
		// Apply the projection to the existing rendering; in other words, keep
		// only the renders needed by the new output columns, and reorder them
		// accordingly.
		oldRenders := post.RenderExprs
		post.RenderExprs = make([]execinfrapb.Expression, len(columns))
		for i, c := range columns {
			post.RenderExprs[i] = oldRenders[c]
		}
	}
	p.SetLastStageTSPost(post, newResultTypes)
}

// AddHashTSProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. The Ordering is updated;
// columns in the ordering are added to the projection as needed.
//
// The PostProcessSpec may not be updated if the resulting projection keeps all
// the columns in their original order.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
// for multiple model processing
func (p *PhysicalPlan) AddHashTSProjection(
	columns []uint32, useStatistic bool, isTag bool, resultCols sqlbase.ResultColumns, relCount int,
) {
	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.ResultTypes)) && len(columns) != 0 {
		return
	}

	// Update the ordering.
	if len(p.MergeOrdering.Columns) > 0 {
		newOrdering := make([]execinfrapb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			// Look for the column in the new projection.
			found := -1
			for j, projCol := range columns {
				if projCol == c.ColIdx {
					found = j
				}
			}
			if found == -1 {
				// We have a column that is not in the projection but will be necessary
				// later when the streams are merged; add it.
				found = len(columns)
				columns = append(columns, c.ColIdx)
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	if !useStatistic {
		newResultTypes := make([]types.T, len(columns))

		for i, c := range columns {
			if i < relCount {
				newResultTypes[i] = *resultCols[i].Typ
			} else {
				newResultTypes[i] = p.ResultTypes[c]
			}
		}

		post := p.GetLastStageTSPost()

		if post.RenderExprs != nil {
			// Apply the projection to the existing rendering; in other words, keep
			// only the renders needed by the new output columns, and reorder them
			// accordingly.
			oldRenders := post.RenderExprs
			post.RenderExprs = make([]execinfrapb.Expression, len(columns))
			for i, c := range columns {
				post.RenderExprs[i] = oldRenders[c]
			}
		}
		p.SetLastStageTSPost(post, newResultTypes)
	}
}

// exprColumn returns the column that is referenced by the expression, if the
// expression is just an IndexedVar.
//
// See MakeExpression for a description of indexVarMap.
func exprColumn(expr tree.TypedExpr, indexVarMap []int) (int, bool) {
	v, ok := expr.(*tree.IndexedVar)
	if !ok {
		return -1, false
	}
	return indexVarMap[v.Idx], true
}

// AddRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
//
// The Ordering is updated; columns in the ordering are added to the render
// expressions as necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddRendering(
	exprs []tree.TypedExpr, exprCtx ExprContext, indexVarMap []int, outTypes []types.T, pushTS bool,
) error {
	// Arrow projection acceleration: if enabled and all render expressions are
	// arrow-computable, evaluate the projection through the Arrow compute engine
	// via a dedicated processor stage.
	if enabled := arrowProjectionEnabled(exprCtx.EvalContext()); enabled {
		if p.canArrowRender(exprs, indexVarMap) && hasArrowComputeExpr(exprs) {
			return p.addArrowRendering(exprs, indexVarMap, outTypes)
		}
	}

	// First check if we need an Evaluator, or we are just shuffling values. We
	// also check if the rendering is a no-op ("identity").
	needRendering := false
	identity := len(exprs) == len(p.ResultTypes)

	for exprIdx, e := range exprs {
		varIdx, ok := exprColumn(e, indexVarMap)
		if !ok {
			needRendering = true
			break
		}
		identity = identity && (varIdx == exprIdx)
	}

	if !needRendering {
		if identity {
			// Nothing to do.
			return nil
		}
		// We don't need to do any rendering: the expressions effectively describe
		// just a projection.
		cols := make([]uint32, len(exprs))
		for i, e := range exprs {
			streamCol, _ := exprColumn(e, indexVarMap)
			if streamCol == -1 {
				panic(fmt.Sprintf("render %d refers to column not in source: %s", i, e))
			}
			cols[i] = uint32(streamCol)
		}
		p.AddProjection(cols)
		return nil
	}

	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 {
		post = execinfrapb.PostProcessSpec{}
		// The last stage contains render expressions. The new renders refer to
		// the output of these, so we need to add another "no-op" stage to which
		// to attach the new rendering.
		p.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	post.RenderExprs = make([]execinfrapb.Expression, len(exprs))
	local := !pushTS && len(p.ResultRouters) == 1
	for i, e := range exprs {
		var err error
		post.RenderExprs[i], err = MakeExpression(e, exprCtx, compositeMap, local, pushTS)
		if err != nil {
			return err
		}
	}

	if len(p.MergeOrdering.Columns) > 0 {
		outTypes = outTypes[:len(outTypes):len(outTypes)]
		newOrdering := make([]execinfrapb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			found := -1
			// Look for the column in the new projection.
			for exprIdx, e := range exprs {
				if varIdx, ok := exprColumn(e, indexVarMap); ok && varIdx == int(c.ColIdx) {
					found = exprIdx
					break
				}
			}
			if found == -1 {
				// We have a column that is not being rendered but will be necessary
				// later when the streams are merged; add it.

				// The new expression refers to column post.OutputColumns[c.ColIdx].
				internalColIdx := c.ColIdx
				if post.Projection {
					internalColIdx = post.OutputColumns[internalColIdx]
				}
				var newExpr execinfrapb.Expression
				var expressErr error
				newExpr, expressErr = MakeExpression(tree.NewTypedOrdinalReference(
					int(internalColIdx),
					&p.ResultTypes[c.ColIdx]),
					exprCtx, nil /* indexVarMap */, local, pushTS)
				if expressErr != nil {
					return expressErr
				}

				found = len(post.RenderExprs)
				post.RenderExprs = append(post.RenderExprs, newExpr)
				outTypes = append(outTypes, p.ResultTypes[c.ColIdx])
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	post.Projection = false
	post.OutputColumns = nil
	p.SetLastStagePost(post, outTypes)
	return nil
}

// AddTSRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
//
// The Ordering is updated; columns in the ordering are added to the render
// expressions as necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddTSRendering(
	exprs []tree.TypedExpr, exprCtx ExprContext, indexVarMap []int, outTypes []types.T,
) error {
	post := p.GetLastStageTSPost()
	if len(post.RenderExprs) > 0 {
		post = execinfrapb.PostProcessSpec{}
		// The last stage contains render expressions. The new renders refer to
		// the output of these, so we need to add another "no-op" stage to which
		// to attach the new rendering.
		p.AddTSNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	post.RenderExprs = make([]execinfrapb.Expression, len(exprs))
	for i, e := range exprs { // problem
		var err error
		renders, err := MakeTSExpression(e, exprCtx, compositeMap)
		if err != nil {
			return err
		}
		post.RenderExprs[i] = renders
	}

	if len(p.MergeOrdering.Columns) > 0 {
		outTypes = outTypes[:len(outTypes):len(outTypes)]
		newOrdering := make([]execinfrapb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			found := -1
			// Look for the column in the new projection.
			for exprIdx, e := range exprs {
				if varIdx, ok := exprColumn(e, indexVarMap); ok && varIdx == int(c.ColIdx) {
					found = exprIdx
					break
				}
			}
			if found == -1 {
				// We have a column that is not being rendered but will be necessary
				// later when the streams are merged; add it.

				// The new expression refers to column post.OutputColumns[c.ColIdx].
				internalColIdx := c.ColIdx
				newExpr, err := MakeTSExpression(tree.NewTypedOrdinalReference(
					int(internalColIdx),
					&p.ResultTypes[c.ColIdx]),
					exprCtx, nil /* indexVarMap */)
				if err != nil {
					return err
				}

				found = len(post.RenderExprs)
				post.RenderExprs = append(post.RenderExprs, newExpr)
				outTypes = append(outTypes, p.ResultTypes[c.ColIdx])
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}
	if len(exprs) == 0 {
		return nil
	}
	post.OutputTypes = outTypes
	p.SetLastStageTSPost(post, outTypes)
	return nil
}

// arrowProjectionPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowProjection.Expr. It mirrors the struct defined in the
// rowexec package (the executor), with identical JSON tags; the JSON bytes are
// the only contract between the planner and the executor.
type arrowProjectionPlan struct {
	Cols []arrowProjectionCol `json:"cols"`
}

type arrowArg struct {
	// Col is the index of the input (stream) column this argument reads from.
	// It is -1 when ConstInt/ConstFloat/ConstBool/ConstStr carry the value
	// instead (a literal).
	Col int `json:"col"`
	// Constant literal values; exactly one is set when Col == -1.
	ConstInt   *int64   `json:"cint,omitempty"`
	ConstFloat *float64 `json:"cfloat,omitempty"`
	ConstBool  *bool    `json:"cbool,omitempty"`
	ConstStr   *string  `json:"cstr,omitempty"`
	// Cast, when non-nil, marks a CAST applied to the operand. The value is the
	// target type tag produced by arrowCastTargetTag ("STRING"/"INT"/"FLOAT"/"DECIMAL").
	Cast *string `json:"cast,omitempty"`
	// CastScale carries the target scale for a DECIMAL cast target (ignored for
	// other targets). It is the resolved width of the DECIMAL type.
	CastScale *int32 `json:"cscale,omitempty"`
}

type arrowProjectionCol struct {
	Kind   string     `json:"kind"`   // "compute", "passthrough", or "case"
	Func   string     `json:"func"`   // for compute: add/sub/mul/div/negate/copy
	Inputs []arrowArg `json:"inputs"` // for compute: arguments (columns and/or constants)
	Input  int        `json:"input"`  // for passthrough: input column index
	// Branches are the WHEN/THEN pairs of a CASE/COALESCE projection. Only set
	// when Kind == "case". The executor evaluates each When as a boolean mask
	// and selects the first matching Then, falling back to Else.
	Branches []arrowCaseBranch `json:"branches,omitempty"`
	// Else is the fallback value of a CASE/COALESCE projection. Only set when
	// Kind == "case".
	Else *arrowProjectionCol `json:"else,omitempty"`
	// TZ marks that the timestamp operand of a datetime function is a
	// TIMESTAMPTZ, so the executor must honor the session time zone when
	// extracting/truncating. Only set when Kind == "datetime".
	TZ bool `json:"tz,omitempty"`
}

// arrowCaseBranch is a single WHEN/THEN pair of a CASE/COALESCE projection.
// When is a boolean-producing spec (comparison or isnull); Then is the value
// produced when When holds for a given row.
type arrowCaseBranch struct {
	When *arrowProjectionCol `json:"when"`
	Then *arrowProjectionCol `json:"then"`
}

// arrowOperandArg classifies a projection operand as either an input column
// reference or a supported constant literal, returning the arrowArg plus the
// operand's resolved type. ok is false for operands that cannot be accelerated
// (non-column, non-constant expressions, or unsupported types).
func (p *PhysicalPlan) arrowOperandArg(
	e tree.TypedExpr, indexVarMap []int,
) (arrowArg, *types.T, bool) {
	if colIdx, ok := exprColumn(e, indexVarMap); ok {
		if colIdx < 0 || colIdx >= len(p.ResultTypes) {
			return arrowArg{}, nil, false
		}
		return arrowArg{Col: colIdx}, &p.ResultTypes[colIdx], true
	}
	switch c := e.(type) {
	case *tree.DInt:
		v := int64(*c)
		return arrowArg{Col: -1, ConstInt: &v}, e.ResolvedType(), true
	case *tree.DFloat:
		v := float64(*c)
		return arrowArg{Col: -1, ConstFloat: &v}, e.ResolvedType(), true
	case *tree.DString:
		v := string(*c)
		return arrowArg{Col: -1, ConstStr: &v}, e.ResolvedType(), true
	case *tree.CastExpr:
		// Render-side CAST: recurse on the inner expression, then attach a cast
		// tag for the resolved target type. Only types understood by
		// arrowCastTargetTag are accelerated (numeric<->string so far); anything
		// else falls back to the row engine via ok==false.
		innerExpr, ok := c.Expr.(tree.TypedExpr)
		if !ok {
			return arrowArg{}, nil, false
		}
		inner, _, ok := p.arrowOperandArg(innerExpr, indexVarMap)
		if !ok {
			return arrowArg{}, nil, false
		}
		tag, ok := arrowCastTargetTag(c.ResolvedType())
		if !ok {
			return arrowArg{}, nil, false
		}
		inner.Cast = &tag
		if c.ResolvedType().Family() == types.DecimalFamily {
			scale := c.ResolvedType().Scale()
			inner.CastScale = &scale
		}
		return inner, c.ResolvedType(), true
	}
	return arrowArg{}, nil, false
}

// canArrowRender reports whether all render expressions can be evaluated by the
// Arrow compute engine, and whether the current plan state is simple enough to
// route through a dedicated arrow projection stage. It only returns true when
// the last stage's post is an identity (no projection, no render) and there is
// no merge ordering to preserve (both conditions hold for a simple
// `SELECT a+b FROM t`).
// hasArrowComputeExpr reports whether at least one render expression is a real
// Arrow compute operation (binary/unary, an Arrow-supported numeric function
// such as ABS/SQRT/FLOOR/CEIL/ROUND, or an Arrow-supported string function such
// as LOWER/UPPER/LENGTH/CONCAT/SUBSTRING). It is used to avoid routing a purely
// passthrough render (e.g. `SELECT a FROM t`) through the Arrow stage, which
// would add a processor for zero benefit.
func hasArrowComputeExpr(exprs []tree.TypedExpr) bool {
	for _, e := range exprs {
		switch ex := e.(type) {
		case *tree.BinaryExpr, *tree.UnaryExpr:
			return true
		case *tree.FuncExpr:
			if _, ok := arrowStringFuncName(arrowFuncName(ex)); ok {
				return true
			}
			if _, _, ok := arrowNumericFuncName(arrowFuncName(ex)); ok {
				return true
			}
			if arrowDatetimeFuncName(arrowFuncName(ex)) != "" {
				return true
			}
		case *tree.CaseExpr, *tree.CoalesceExpr:
			return true
		}
	}
	return false
}

func (p *PhysicalPlan) canArrowRender(exprs []tree.TypedExpr, indexVarMap []int) bool {
	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 {
		// The last stage already renders; adding another stage would double-render.
		return false
	}
	if len(p.MergeOrdering.Columns) > 0 {
		// Ordering handling would require adding passthrough columns and adjusting
		// the downstream merge, which is out of scope for this first integration.
		return false
	}
	for _, e := range exprs {
		switch ex := e.(type) {
		case *tree.BinaryExpr:
			switch ex.Operator {
			case tree.Plus, tree.Minus, tree.Mult, tree.Div:
			default:
				return false
			}
			la, lty, ok1 := p.arrowOperandArg(ex.Left.(tree.TypedExpr), indexVarMap)
			ra, rty, ok2 := p.arrowOperandArg(ex.Right.(tree.TypedExpr), indexVarMap)
			if !ok1 || !ok2 {
				return false
			}
			if la.Col < 0 && ra.Col < 0 {
				// Need at least one input column to reference.
				return false
			}
			if lty.Family() != rty.Family() {
				// Keep both operands the same type so the arrow kernel needs no
				// implicit promotion; the scalar path handles mixed types.
				return false
			}
			if !arrowSupportedComputeType(lty) || !arrowSupportedComputeType(ex.ResolvedType()) {
				return false
			}
		case *tree.UnaryExpr:
			if ex.Operator != tree.UnaryMinus {
				return false
			}
			a, ty, ok := p.arrowOperandArg(ex.Expr.(tree.TypedExpr), indexVarMap)
			if !ok || a.Col < 0 {
				return false
			}
			if !arrowSupportedComputeType(ty) || !arrowSupportedComputeType(ex.ResolvedType()) {
				return false
			}
	case *tree.FuncExpr:
	// Numeric scalar kernels (abs/sqrt/ln/sign/power) are evaluated by
	// the vendored arrow/compute module via compute.CallFunction. Gate
	// on operand count and numeric types below.
	rawName := arrowFuncName(ex)
		// Datetime functions (extract/date_trunc) take a constant field string
		// plus a Timestamp/TimestampTZ column and are evaluated by a dedicated
		// Go kernel in the Arrow projection executor (arrow/compute has no
		// datetime kernels in this vendored version).
		if arrowDatetimeFuncName(rawName) != "" {
			return p.canArrowDatetime(ex, indexVarMap)
		}
		if kernel, arity, ok := arrowNumericFuncName(rawName); ok {
				args, ok := p.arrowStringFuncArgs(ex.Exprs, indexVarMap)
				if !ok {
					return false
				}
				if arity >= 0 && len(args) != arity {
					return false
				}
				if !arrowNumericComputeType(ex.ResolvedType()) {
					return false
				}
				hasCol := false
				for _, a := range args {
					if a.col >= 0 {
						hasCol = true
					}
					if !arrowNumericComputeType(a.ty) {
						return false
					}
				}
				if !hasCol {
					// Need at least one input column to reference.
					return false
				}
				if len(args) > 1 {
					// Require same-family operands (arrow/compute kernels do not
					// implicitly promote, matching the binary-expr gate above).
					fam := args[0].ty.Family()
					for _, a := range args[1:] {
						if a.ty.Family() != fam {
							return false
						}
					}
				}
				_ = kernel
				return true
			}
			// String-function kernels (length/lower/upper/concat/substring) are
			// evaluated by a dedicated Go kernel inside the Arrow projection
			// executor (arrow/compute has no string kernels in this vendored
			// version). Gate on the operand shapes below.
			funcName, ok := arrowStringFuncName(rawName)
			if !ok {
				return false
			}
			args, ok := p.arrowStringFuncArgs(ex.Exprs, indexVarMap)
			if !ok {
				return false
			}
			hasCol := false
			for _, a := range args {
				if a.col >= 0 {
					hasCol = true
				}
			}
			if !hasCol {
				// Need at least one input column to reference.
				return false
			}
			switch funcName {
			case "length", "octet_length", "lower", "upper", "trim", "ltrim", "rtrim", "btrim":
				if len(args) != 1 && len(args) != 2 {
					return false
				}
				if args[0].ty.Family() != types.StringFamily || !arrowSupportedComputeType(args[0].ty) {
					return false
				}
				if len(args) == 2 {
					// The optional second argument is the trim characters set and
					// must also be a string.
					if args[1].ty.Family() != types.StringFamily {
						return false
					}
				}
				return true
			case "replace":
				if len(args) != 3 {
					return false
				}
				for _, a := range args {
					if a.ty.Family() != types.StringFamily {
						return false
					}
				}
				return true
			case "concat":
				if len(args) < 2 {
					return false
				}
				for _, a := range args {
					if a.ty.Family() != types.StringFamily {
						return false
					}
				}
				return true
			case "substring":
				if len(args) < 2 || len(args) > 3 {
					return false
				}
				if args[0].ty.Family() != types.StringFamily {
					return false
				}
				for _, a := range args[1:] {
					if a.ty.Family() != types.IntFamily {
						return false
					}
				}
				return true
			case "overlay":
				// overlay(str PLACING substr FROM start): 3 args, all but the last
				// are strings, the start position is an int.
				if len(args) != 3 {
					return false
				}
				if args[0].ty.Family() != types.StringFamily || args[1].ty.Family() != types.StringFamily {
					return false
				}
				if args[2].ty.Family() != types.IntFamily {
					return false
				}
				return true
			case "split_part":
				// split_part(str, sep, n): 3 args, str/sep are strings, n is an int.
				if len(args) != 3 {
					return false
				}
				if args[0].ty.Family() != types.StringFamily || args[1].ty.Family() != types.StringFamily {
					return false
				}
				if args[2].ty.Family() != types.IntFamily {
					return false
				}
				return true
			}
			return false
		case *tree.ComparisonExpr:
			// Boolean comparisons (used as CASE WHEN conditions) are evaluated by
			// the arrow/compute comparison kernels, which return a boolean array.
			if _, ok := arrowComparisonKernel(ex.Operator); !ok {
				return false
			}
			la, lty, ok1 := p.arrowOperandArg(ex.Left.(tree.TypedExpr), indexVarMap)
			ra, rty, ok2 := p.arrowOperandArg(ex.Right.(tree.TypedExpr), indexVarMap)
			if !ok1 || !ok2 {
				return false
			}
			if la.Col < 0 && ra.Col < 0 {
				return false
			}
			if lty.Family() != rty.Family() {
				return false
			}
			if !arrowSupportedComputeType(lty) {
				return false
			}
			return true
		case *tree.CastExpr:
			// Render-side CAST is accelerated only for target types understood by
			// arrowCastTargetTag, and only when the inner expression itself is
			// renderable. The actual cast tag is attached in arrowOperandArg.
			ce, _ := e.(*tree.CastExpr)
			if _, ok := arrowCastTargetTag(ce.ResolvedType()); !ok {
				return false
			}
			innerExpr, ok := ce.Expr.(tree.TypedExpr)
			if !ok {
				return false
			}
			return p.canArrowRender([]tree.TypedExpr{innerExpr}, indexVarMap)
		case *tree.CoalesceExpr:
			// COALESCE(a, b, ...) becomes a CASE where each branch's WHEN is
			// "a IS NOT NULL"; the result type is the COALESCE's resolved type.
			ce := e.(*tree.CoalesceExpr)
			all := make([]tree.TypedExpr, len(ce.Exprs)+1)
			for i, x := range ce.Exprs {
				all[i] = x.(tree.TypedExpr)
			}
			all[len(ce.Exprs)] = ce.Exprs[len(ce.Exprs)-1].(tree.TypedExpr) // duplicate last arg as ELSE
			return p.canArrowRenderCase(all, ce.ResolvedType(), indexVarMap)
		case *tree.CaseExpr:
			// CASE [operand] WHEN w THEN t ... ELSE e. The operand (if present)
			// and each WHEN/THEN/ELSE must be renderable, and every branch must
			// share the CASE's resolved result type (heterogeneous branches are
			// cast to the result type by the caller).
			ce := e.(*tree.CaseExpr)
			subs := make([]tree.TypedExpr, 0, 2*len(ce.Whens)+2)
			if ce.Expr != nil {
				subs = append(subs, ce.Expr.(tree.TypedExpr))
			}
			for _, w := range ce.Whens {
				subs = append(subs, w.Cond.(tree.TypedExpr), w.Val.(tree.TypedExpr))
			}
			if ce.Else != nil {
				subs = append(subs, ce.Else.(tree.TypedExpr))
			} else {
				subs = append(subs, ce.Whens[len(ce.Whens)-1].Val.(tree.TypedExpr))
			}
			return p.canArrowRenderCase(subs, ce.ResolvedType(), indexVarMap)
		default:
			// Constant literals are always renderable (materialized as scalars),
			// e.g. the THEN/ELSE values and WHEN keys of a CASE/COALESCE.
			if _, ok := e.(tree.Datum); ok {
				return true
			}
			// Plain column reference: allowed as a passthrough (identity copy).
			if _, ok := exprColumn(e, indexVarMap); !ok {
				return false
			}
			// Use the broader passthrough type set (which includes Decimal,
			// Timestamp, Uuid and Json) rather than the arithmetic compute set:
			// a copied-through column never enters an arrow/compute kernel, so
			// any type the executor can materialize into an Arrow array is fine.
			if !arrowSupportedPassthroughType(e.ResolvedType()) {
				return false
			}
		}
	}
	return true
}

// canArrowRenderCase checks whether a CASE/COALESCE expression can be rendered
// in the Arrow projection stage. It casts every sub-expression (branches and
// ELSE) to the CASE's resolved result type and verifies the casted expression
// is renderable; this keeps all branches the same type so the executor's CASE
// evaluator can build a single result array.
func (p *PhysicalPlan) canArrowRenderCase(subs []tree.TypedExpr, resultType *types.T, indexVarMap []int) bool {
	if _, ok := arrowCastTargetTag(resultType); !ok {
		// Only result types the arrow engine can materialize are supported
		// (decimal/timestamp/uuid/json would need a cast kernel we don't have).
		return false
	}
	for _, s := range subs {
		// If the sub-expression already matches the result type, use it as-is;
		// otherwise wrap it in a CAST to the result type.
		var e tree.TypedExpr = s
		if !s.ResolvedType().Equivalent(resultType) {
			ce, err := tree.NewTypedCastExpr(s, resultType)
			if err != nil {
				return false
			}
			e = ce
		}
		if !p.canArrowRender([]tree.TypedExpr{e}, indexVarMap) {
			return false
		}
	}
	return true
}

// arrowSupportedComputeType reports whether the Arrow compute kernels we have
// wired up can handle a value of the given type for arithmetic (add/sub/mul/
// div) and passthrough.
func arrowSupportedComputeType(t *types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily, types.BytesFamily:
		return true
	}
	return false
}

// arrowFuncName safely extracts the lower-cased SQL function name from a
// FuncExpr. During physical planning the function reference may still be an
// unresolved name (e.g. EXTRACT keeps its reference cell unpopulated until
// Normalize), so we handle both the resolved *FunctionDefinition and the
// *UnresolvedName forms instead of dereferencing FunctionReference blindly
// (which would panic on a nil interface).
func arrowFuncName(ex *tree.FuncExpr) string {
	switch ref := ex.Func.FunctionReference.(type) {
	case *tree.FunctionDefinition:
		return strings.ToLower(ref.Name)
	case *tree.UnresolvedName:
		return strings.ToLower(ref.Parts[0])
	}
	return ""
}

// arrowDatetimeFuncName normalizes a SQL datetime-function name to the internal
// projection Func name handled by the Arrow projection executor. Returns "" for
// anything outside the supported set (extract/date_trunc/now).
func arrowDatetimeFuncName(name string) string {
	switch strings.ToLower(name) {
	case "extract":
		return "extract"
	case "date_trunc":
		return "date_trunc"
	case "now", "current_timestamp", "transaction_timestamp":
		return "now"
	}
	return ""
}

// canArrowDatetime verifies that a datetime FuncExpr is shaped so the Arrow
// projection executor can evaluate it. extract/date_trunc take a constant field
// string plus a Timestamp/TimestampTZ column; now() takes no arguments and
// produces the current (statement) timestamp.
func (p *PhysicalPlan) canArrowDatetime(ex *tree.FuncExpr, indexVarMap []int) bool {
	if arrowDatetimeFuncName(arrowFuncName(ex)) == "now" {
		return len(ex.Exprs) == 0
	}
	if len(ex.Exprs) != 2 {
		return false
	}
	fieldArg, ok := ex.Exprs[0].(tree.TypedExpr)
	if !ok {
		return false
	}
	if _, ok := fieldArg.(*tree.DString); !ok {
		// The field must be a constant string literal.
		return false
	}
	tsArg, ok := ex.Exprs[1].(tree.TypedExpr)
	if !ok {
		return false
	}
	tsTy := tsArg.ResolvedType()
	if tsTy.Family() != types.TimestampFamily && tsTy.Family() != types.TimestampTZFamily {
		return false
	}
	return true
}

// arrowStringFuncName normalizes a SQL string-function name to the internal
// projection Func name handled by the Arrow projection executor. Returns ok=
// false for anything outside the supported set (length/lower/upper/concat/
// substring).
func arrowStringFuncName(name string) (string, bool) {
	switch strings.ToLower(name) {
	case "length":
		return "length", true
	case "octet_length":
		return "octet_length", true
	case "lower":
		return "lower", true
	case "upper":
		return "upper", true
	case "concat":
		return "concat", true
	case "substring", "substr":
		return "substring", true
	case "trim", "btrim", "ltrim", "rtrim":
		return strings.ToLower(name), true
	case "replace":
		return "replace", true
	case "overlay":
		return "overlay", true
	case "split_part":
		return "split_part", true
	}
	return "", false
}

// arrowNumericFuncName normalizes a SQL numeric scalar-function name to the
// arrow/compute kernel name used by the Arrow projection executor, together
// with the expected operand arity (-1 for "any", though all current kernels
// have fixed arity). These kernels are shipped by the vendored arrow/compute
// module, so no dedicated Go kernel is needed.
//
// Supported numeric kernels: abs/sqrt/ln/sign (1 arg), power (2 args), and the
// rounding kernels floor/ceil/trunc/round (1 arg). The executor routes them
// through compute.CallFunction with the kernel name, reusing the same
// binary-operator path that handles add/subtract/multiply/divide.
//
// Rounding semantics match KWDB's floatOverload1 builtins: floor/ceil/trunc
// take a float and return float64 (DFloat), and round uses banker's rounding
// (RoundToEven), which is exactly arrow's DefaultRoundOptions. Integer inputs
// are cast to float by the planner (as in the classic path) so arrow's
// int->float64 result is consistent.
func arrowNumericFuncName(name string) (kernel string, arity int, ok bool) {
	switch strings.ToLower(name) {
	case "abs":
		return "abs", 1, true
	case "sqrt":
		return "sqrt", 1, true
	case "ln":
		return "ln", 1, true
	case "sign":
		return "sign", 1, true
	case "power":
		return "power", 2, true
	case "floor":
		return "floor", 1, true
	case "ceil", "ceiling":
		return "ceil", 1, true
	case "trunc":
		return "trunc", 1, true
	case "round":
		return "round", 1, true
	}
	return "", 0, false
}

// arrowNumericComputeType reports whether the Arrow numeric kernels we route
// through compute.CallFunction can handle a value of the given type. We keep
// this to plain int/float (no decimal) to avoid widening the column-building
// surface; arrow/compute supports decimal for these kernels, but the planner
// gate intentionally stays conservative.
func arrowNumericComputeType(t *types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily:
		return true
	}
	return false
}

// arrowSupportedPassthroughType reports whether a bare column reference can be
// copied through the Arrow projection executor. It mirrors the type set that
// buildArrowColumns / appendEncDatum can materialize into an Arrow array
// (Int/Float/Bool/String/Bytes/Decimal/Timestamp/TimestampTZ/Uuid/Json), which
// is broader than arrowSupportedComputeType (the binary-operator compute set).
// This is the gate used for passthrough (Kind:"copy") columns so that, e.g., a
// DECIMAL or TIMESTAMP column alongside an arithmetic render can still take the
// Arrow path.
func arrowSupportedPassthroughType(t *types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily,
		types.StringFamily, types.BytesFamily, types.DecimalFamily,
		types.TimestampFamily, types.TimestampTZFamily,
		types.UuidFamily, types.JsonFamily,
		types.DateFamily, types.IntervalFamily:
		return true
	}
	return false
}

// arrowArgWithType pairs an operand's arrow arg with its SQL type; used only
// inside the planner gate to validate string-function operand shapes.
type arrowArgWithType struct {
	col int
	ty  *types.T
}

// arrowStringFuncArgs resolves the argument expressions of a string function
// to (col, type) pairs, returning ok=false if any operand is not arrow-eligible.
func (p *PhysicalPlan) arrowStringFuncArgs(exprs tree.Exprs, indexVarMap []int) ([]arrowArgWithType, bool) {
	out := make([]arrowArgWithType, 0, len(exprs))
	for _, e := range exprs {
		te, ok := e.(tree.TypedExpr)
		if !ok {
			return nil, false
		}
		a, t, ok := p.arrowOperandArg(te, indexVarMap)
		if !ok {
			return nil, false
		}
		out = append(out, arrowArgWithType{col: a.Col, ty: t})
	}
	return out, true
}

// arrowArgsFor resolves function arguments to plain arrowArgs for serialization
// into the projection plan.
func (p *PhysicalPlan) arrowArgsFor(exprs tree.Exprs, indexVarMap []int) ([]arrowArg, bool) {
	out := make([]arrowArg, 0, len(exprs))
	for _, e := range exprs {
		te, ok := e.(tree.TypedExpr)
		if !ok {
			return nil, false
		}
		a, _, ok := p.arrowOperandArg(te, indexVarMap)
		if !ok {
			return nil, false
		}
		out = append(out, a)
	}
	return out, true
}

// addArrowRendering builds a dedicated arrow projection stage that evaluates
// the render expressions via the Arrow compute engine. The previous (last)
// stage keeps emitting its full output (identity post); the new stage consumes
// those columns and produces exactly the rendered output columns.
func (p *PhysicalPlan) addArrowRendering(
	exprs []tree.TypedExpr, indexVarMap []int, outTypes []types.T,
) error {
	plan := arrowProjectionPlan{Cols: make([]arrowProjectionCol, 0, len(exprs))}
	outT := make([]types.T, 0, len(exprs))
	for _, e := range exprs {
		switch ex := e.(type) {
		case *tree.CoalesceExpr:
			col, ty, err := p.arrowCoalesceCol(ex, indexVarMap)
			if err != nil {
				return err
			}
			plan.Cols = append(plan.Cols, col)
			outT = append(outT, *ty)
		case *tree.CaseExpr:
			col, ty, err := p.arrowCaseCol(ex, indexVarMap)
			if err != nil {
				return err
			}
			plan.Cols = append(plan.Cols, col)
			outT = append(outT, *ty)
		default:
			col, ty, err := p.arrowProjectionColFor(e, indexVarMap)
			if err != nil {
				return err
			}
			plan.Cols = append(plan.Cols, col)
			outT = append(outT, *ty)
		}
	}
	planBytes, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	core := execinfrapb.ProcessorCoreUnion{
		ArrowProjection: &execinfrapb.Expression{Expr: string(planBytes)},
	}
	// The arrow stage consumes the previous stage's full output (identity post)
	// and produces exactly the rendered columns, so its own post is identity.
	p.AddNoGroupingStage(core, execinfrapb.PostProcessSpec{}, outT, p.MergeOrdering)
	return nil
}

// arrowProjectionColFor builds a single arrowProjectionCol (the JSON spec carried
// to the executor) for one render sub-expression. It is the per-expression core
// of addArrowRendering, also reused by the CASE/COALESCE builder.
func (p *PhysicalPlan) arrowProjectionColFor(e tree.TypedExpr, indexVarMap []int) (arrowProjectionCol, *types.T, error) {
	switch ex := e.(type) {
	case *tree.BinaryExpr:
		l, _, _ := p.arrowOperandArg(ex.Left.(tree.TypedExpr), indexVarMap)
		r, _, _ := p.arrowOperandArg(ex.Right.(tree.TypedExpr), indexVarMap)
		var fn string
		switch ex.Operator {
		case tree.Plus:
			fn = "add"
		case tree.Minus:
			fn = "subtract"
		case tree.Mult:
			fn = "multiply"
		case tree.Div:
			fn = "divide"
		}
		return arrowProjectionCol{
			Kind:   "compute",
			Func:   fn,
			Inputs: []arrowArg{l, r},
		}, ex.ResolvedType(), nil
	case *tree.UnaryExpr:
		a, _, _ := p.arrowOperandArg(ex.Expr.(tree.TypedExpr), indexVarMap)
		return arrowProjectionCol{
			Kind:   "compute",
			Func:   "negate",
			Inputs: []arrowArg{a},
		}, ex.ResolvedType(), nil
	case *tree.FuncExpr:
		rawName := arrowFuncName(ex)
		if kernel, _, ok := arrowNumericFuncName(rawName); ok {
			inputs, ok := p.arrowArgsFor(ex.Exprs, indexVarMap)
			if !ok {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported argument to %s", kernel)
			}
			return arrowProjectionCol{
				Kind:   "compute",
				Func:   kernel,
				Inputs: inputs,
			}, ex.ResolvedType(), nil
		}
		if arrowDatetimeFuncName(rawName) != "" {
			if arrowDatetimeFuncName(rawName) == "now" {
				// now() / current_timestamp / transaction_timestamp: no
				// arguments; emits the current (statement) timestamp as a
				// TimestampTZ column. Always timezone-aware.
				if len(ex.Exprs) != 0 {
					return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: now() expects 0 arguments")
				}
				return arrowProjectionCol{
					Kind: "datetime",
					Func: "now",
					TZ:   true,
				}, ex.ResolvedType(), nil
			}
			// extract(field FROM ts) / date_trunc(field, ts): the field is a
			// constant string (DString) passed as a scalar arg, the timestamp
			// operand is a column. Build a "func" column of Kind "datetime".
			if len(ex.Exprs) != 2 {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: %s expects 2 arguments", rawName)
			}
			fieldArg, ok := ex.Exprs[0].(tree.TypedExpr)
			if !ok {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: %s field must be a constant", rawName)
			}
			fieldConst, ok := fieldArg.(*tree.DString)
			if !ok {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: %s field must be a constant string", rawName)
			}
			field := string(*fieldConst)
			tsArg, _, ok := p.arrowOperandArg(ex.Exprs[1].(tree.TypedExpr), indexVarMap)
			if !ok {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported timestamp operand to %s", rawName)
			}
			tsTy := ex.Exprs[1].(tree.TypedExpr).ResolvedType()
			if tsTy.Family() != types.TimestampFamily && tsTy.Family() != types.TimestampTZFamily {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: %s expects a Timestamp operand", rawName)
			}
			fieldCopy := field
			return arrowProjectionCol{
				Kind: "datetime",
				Func: arrowDatetimeFuncName(rawName),
				Inputs: []arrowArg{
					{Col: -1, ConstStr: &fieldCopy},
					{Col: tsArg.Col},
				},
				TZ: tsTy.Family() == types.TimestampTZFamily,
			}, ex.ResolvedType(), nil
		}
		funcName, ok := arrowStringFuncName(rawName)
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported function %s", rawName)
		}
		inputs, ok := p.arrowArgsFor(ex.Exprs, indexVarMap)
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported argument to %s", funcName)
		}
		return arrowProjectionCol{
			Kind:   "compute",
			Func:   funcName,
			Inputs: inputs,
		}, ex.ResolvedType(), nil
	case *tree.ComparisonExpr:
		// Boolean comparison used as a CASE WHEN condition. Maps to an arrow/compute
		// comparison kernel that returns a boolean array.
		fn, ok := arrowComparisonKernel(ex.Operator)
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported comparison %v", ex.Operator)
		}
		l, _, _ := p.arrowOperandArg(ex.Left.(tree.TypedExpr), indexVarMap)
		r, _, _ := p.arrowOperandArg(ex.Right.(tree.TypedExpr), indexVarMap)
		return arrowProjectionCol{
			Kind:   "compute",
			Func:   fn,
			Inputs: []arrowArg{l, r},
		}, ex.ResolvedType(), nil
	case *tree.CastExpr:
		// Render-side CAST: the inner operand becomes a single arrowArg carrying
		// the cast target tag, fed to a "copy" compute (the executor applies the
		// cast while materializing the array).
		inner := ex.Expr.(tree.TypedExpr)
		tag, ok := arrowCastTargetTag(ex.ResolvedType())
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported cast to %s", ex.ResolvedType())
		}
		scale := int32(0)
		if s, ok := arrowCastScale(ex.ResolvedType()); ok {
			scale = s
		}
		in, _, ok := p.arrowOperandArg(inner, indexVarMap)
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported cast operand")
		}
		ct := tag
		in.Cast = &ct
		in.CastScale = &scale
		return arrowProjectionCol{
			Kind:   "compute",
			Func:   "copy",
			Inputs: []arrowArg{in},
		}, ex.ResolvedType(), nil
	default:
		// Plain column reference, or a constant literal materialized as a copy
		// of a scalar argument (e.g. the THEN/ELSE values and WHEN keys of a
		// CASE/COALESCE).
		if _, ok := e.(tree.Datum); ok {
			arg, _, ok := p.arrowOperandArg(e, indexVarMap)
			if !ok {
				return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported constant %v", e)
			}
			return arrowProjectionCol{
				Kind:   "compute",
				Func:   "copy",
				Inputs: []arrowArg{arg},
			}, e.ResolvedType(), nil
		}
		col, _ := exprColumn(e, indexVarMap)
		return arrowProjectionCol{
			Kind:  "passthrough",
			Input: col,
		}, e.ResolvedType(), nil
	}
}

// arrowCastScale returns the decimal scale for a cast target type, when the
// target is a decimal. Used to attach the scale to a render-side cast.
func arrowCastScale(t *types.T) (int32, bool) {
	if t.Family() == types.DecimalFamily {
		return t.Width(), true
	}
	return 0, false
}

// arrowCaseValueCol builds the THEN/ELSE value col for a CASE/COALESCE branch,
// casting the sub-expression to the CASE's result type when it differs so that
// all branches share one type (required by the executor CASE evaluator).
func (p *PhysicalPlan) arrowCaseValueCol(e tree.TypedExpr, resultType *types.T, indexVarMap []int) (arrowProjectionCol, error) {
	var inner tree.TypedExpr = e
	if !e.ResolvedType().Equivalent(resultType) {
		ce, err := tree.NewTypedCastExpr(e, resultType)
		if err != nil {
			return arrowProjectionCol{}, err
		}
		inner = ce
	}
	col, _, err := p.arrowProjectionColFor(inner, indexVarMap)
	if err != nil {
		return arrowProjectionCol{}, err
	}
	return col, nil
}

// arrowCaseCol builds the arrowProjectionCol for a CASE expression. Each WHEN
// becomes a boolean-producing spec (a comparison against the CASE operand for
// the "CASE op WHEN w" form, or the predicate directly for "CASE WHEN cond");
// each THEN becomes a value col cast to the result type.
func (p *PhysicalPlan) arrowCaseCol(ex *tree.CaseExpr, indexVarMap []int) (arrowProjectionCol, *types.T, error) {
	resultType := ex.ResolvedType()
	branches := make([]arrowCaseBranch, 0, len(ex.Whens))
	for _, w := range ex.Whens {
		when := ex.Expr // shared operand for "CASE op WHEN w"
		var whenCol arrowProjectionCol
		var err error
		if when != nil {
			// "CASE op WHEN w": WHEN is the equality op = w, evaluated as a
			// boolean-producing comparison spec.
			cmp := &tree.ComparisonExpr{
				Operator: tree.EQ,
				Left:     when.(tree.TypedExpr),
				Right:    w.Cond.(tree.TypedExpr),
			}
			whenCol, _, err = p.arrowComparisonCol(cmp, indexVarMap)
		} else {
			// "CASE WHEN cond": cond is already boolean.
			whenCol, _, err = p.arrowProjectionColFor(w.Cond.(tree.TypedExpr), indexVarMap)
		}
		if err != nil {
			return arrowProjectionCol{}, nil, err
		}
		thenCol, err := p.arrowCaseValueCol(w.Val.(tree.TypedExpr), resultType, indexVarMap)
		if err != nil {
			return arrowProjectionCol{}, nil, err
		}
		branches = append(branches, arrowCaseBranch{When: &whenCol, Then: &thenCol})
	}
	elseExpr := ex.Else
	if elseExpr == nil {
		// ELSE defaults to the last WHEN's value.
		elseExpr = ex.Whens[len(ex.Whens)-1].Val.(tree.TypedExpr)
	}
	elseCol, err := p.arrowCaseValueCol(elseExpr.(tree.TypedExpr), resultType, indexVarMap)
	if err != nil {
		return arrowProjectionCol{}, nil, err
	}
	return arrowProjectionCol{
		Kind:     "case",
		Branches: branches,
		Else:     &elseCol,
	}, resultType, nil
}

// arrowCoalesceCol builds the arrowProjectionCol for a COALESCE expression.
// COALESCE(a, b, c) is equivalent to CASE WHEN a IS NOT NULL THEN a
// WHEN b IS NOT NULL THEN b ELSE c; the WHEN condition is an "isnull" check on
// each argument.
func (p *PhysicalPlan) arrowCoalesceCol(ex *tree.CoalesceExpr, indexVarMap []int) (arrowProjectionCol, *types.T, error) {
	resultType := ex.ResolvedType()
	branches := make([]arrowCaseBranch, 0, len(ex.Exprs))
	for _, e := range ex.Exprs {
		te := e.(tree.TypedExpr)
		// WHEN te IS NOT NULL: the COALESCE WHEN mask is "is not null".
		whenCol := arrowProjectionCol{
			Kind:   "isnotnull",
			Inputs: []arrowArg{},
		}
		// Build the isnull arg: a passthrough column (or scalar) for te.
		arg, _, ok := p.arrowOperandArg(te, indexVarMap)
		if !ok {
			return arrowProjectionCol{}, nil, errors.Errorf("arrow coalesce: unsupported argument")
		}
		whenCol.Inputs = []arrowArg{arg}
		thenCol, err := p.arrowCaseValueCol(te, resultType, indexVarMap)
		if err != nil {
			return arrowProjectionCol{}, nil, err
		}
		branches = append(branches, arrowCaseBranch{When: &whenCol, Then: &thenCol})
	}
	// ELSE is the last argument (its value when all preceding are NULL).
	last := ex.Exprs[len(ex.Exprs)-1].(tree.TypedExpr)
	elseCol, err := p.arrowCaseValueCol(last, resultType, indexVarMap)
	if err != nil {
		return arrowProjectionCol{}, nil, err
	}
	return arrowProjectionCol{
		Kind:     "case",
		Branches: branches,
		Else:     &elseCol,
	}, resultType, nil
}

// arrowComparisonCol builds a boolean-producing arrowProjectionCol for a
// comparison expression (used as a CASE WHEN condition). Only equality and the
// other simple comparisons that map to arrow/compute kernels are supported; the
// executor evaluates them and returns a boolean mask.
func (p *PhysicalPlan) arrowComparisonCol(ex *tree.ComparisonExpr, indexVarMap []int) (arrowProjectionCol, *types.T, error) {
	fn, ok := arrowComparisonKernel(ex.Operator)
	if !ok {
		return arrowProjectionCol{}, nil, errors.Errorf("arrow projection: unsupported comparison %v in CASE WHEN", ex.Operator)
	}
	l, _, _ := p.arrowOperandArg(ex.Left.(tree.TypedExpr), indexVarMap)
	r, _, _ := p.arrowOperandArg(ex.Right.(tree.TypedExpr), indexVarMap)
	return arrowProjectionCol{
		Kind:   "compute",
		Func:   fn,
		Inputs: []arrowArg{l, r},
	}, types.Bool, nil
}

// arrowComparisonKernel maps a tree comparison operator to an arrow/compute
// kernel name that returns a boolean array.
func arrowComparisonKernel(op tree.ComparisonOperator) (string, bool) {
	switch op {
	case tree.EQ:
		return "equal", true
	case tree.NE:
		return "not_equal", true
	case tree.LT:
		return "less", true
	case tree.LE:
		return "less_equal", true
	case tree.GT:
		return "greater", true
	case tree.GE:
		return "greater_equal", true
	}
	return "", false
}

// arrowFilterPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowFilter.Expr. Its JSON shape mirrors the struct
// defined in the rowexec package; the JSON bytes are the only contract between
// the planner and the executor.
type arrowFilterPlan struct {
	Root arrowFilterNode `json:"root"`
}

type arrowFilterNode struct {
	Func     string               `json:"func"`
	Operands []arrowFilterOperand `json:"ops"`
}

type arrowFilterOperand struct {
	Leaf *arrowFilterLeaf `json:"leaf,omitempty"`
	Expr *arrowFilterNode `json:"expr,omitempty"`
}

type arrowFilterLeaf struct {
	Col        int      `json:"col"`
	ConstInt   *int64   `json:"cint,omitempty"`
	ConstFloat *float64 `json:"cfloat,omitempty"`
	ConstBool  *bool    `json:"cbool,omitempty"`
	ConstStr   *string  `json:"cstr,omitempty"`
	// ConstSetInt / ConstSetStr carry the member set of an IN / NOT IN predicate.
	// They serialize with the same JSON tags as the receiver-side leaf so the
	// arrow filter processor can rebuild the ConstSet.
	ConstSetInt []int64  `json:"csetint,omitempty"`
	ConstSetStr []string `json:"csetstr,omitempty"`
	// ConstSetFloat carries the member set of a decimal IN / NOT IN predicate.
	// Decimal columns are cast to FLOAT before the comparison (Arrow compute has
	// no DECIMAL is_in kernel), so the set is emitted as float64 (see buildArrowFilterNode).
	ConstSetFloat []float64 `json:"csetfloat,omitempty"`
	// ConstDecimal transports a decimal literal into a float-cast IN comparison.
	ConstDecimal *tree.DDecimal     `json:"cdec,omitempty"`
	Binary       *arrowFilterBinary `json:"bin,omitempty"`
	// Cast is a type conversion applied to Arg, supporting CAST(col AS ...) inside
	// arrow filter predicates (e.g. CAST(i AS STRING) LIKE '1%').
	Cast *arrowFilterCast `json:"cast,omitempty"`
	// Computed lifts a string function (substring/trim/concat/replace/...) on
	// columns into the Arrow filter path, so that "substring(col,1,3) = 'abc'"
	// evaluates entirely in the Arrow engine instead of falling back to a
	// row-by-row tree.Datum path. See arrow_arg.go (executor) for the
	// corresponding receiver type.
	Computed *arrowFilterComputed `json:"cmp,omitempty"`
	// Case, when non-nil, is a CASE/COALESCE expression producing the leaf value.
	// It reuses the projection CASE spec form (arrowProjectionCol) so all value
	// types and nested branches are supported; the WHEN conditions are
	// boolean-producing specs and the THEN/ELSE branches are value-producing specs.
	Case *arrowProjectionCol `json:"case,omitempty"`
}

// arrowFilterComputed wraps a projection function (its Func name plus a list of
// leaf arguments) as a filter leaf. The executor materializes it into an Arrow
// array via the existing arrowProjection string kernels, then uses that array
// as the operand of the surrounding comparison / LIKE predicate.
type arrowFilterComputed struct {
	Func string            `json:"func"`
	Args []arrowFilterLeaf `json:"args"`
}

// arrowFilterBinary is a nested arithmetic expression (add/sub/mul/div) that a
// leaf operand can expand into, so comparisons against computed columns (e.g.
// `a * 2 > b`) are fully evaluated by the Arrow compute engine.
type arrowFilterBinary struct {
	Func string            `json:"func"`
	Args []arrowFilterLeaf `json:"args"`
}

// arrowFilterCast is a type conversion leaf. Type encodes the target Arrow type
// as a compact tag ("STRING"/"INT"/"FLOAT"); Arg is the inner leaf operand.
type arrowFilterCast struct {
	Func string          `json:"func"`
	Type string          `json:"type"`
	Arg  arrowFilterLeaf `json:"arg"`
}

// arrowFilterLeafFromExpr classifies a filter operand expression as an
// arrowFilterLeaf: an input column, a constant literal, or a nested arithmetic
// expression (add/sub/mul/div) over such leaves. It returns the leaf, its
// resolved type, and whether it is arrow-computable.
func (p *PhysicalPlan) arrowFilterLeafFromExpr(
	e tree.TypedExpr, indexVarMap []int,
) (*arrowFilterLeaf, *types.T, bool) {
	if colIdx, ok := exprColumn(e, indexVarMap); ok {
		// The column type is carried by the IndexedVar itself, so we do not need
		// p.ResultTypes here. This also lets the same builder be used for a join
		// onExpr whose indices reference the (left++right) input space rather than
		// the join's output columns (§7.4).
		return &arrowFilterLeaf{Col: colIdx}, e.ResolvedType(), true
	}
	switch c := e.(type) {
	case *tree.DInt:
		v := int64(*c)
		return &arrowFilterLeaf{Col: -1, ConstInt: &v}, e.ResolvedType(), true
	case *tree.DFloat:
		v := float64(*c)
		return &arrowFilterLeaf{Col: -1, ConstFloat: &v}, e.ResolvedType(), true
	case *tree.DBool:
		v := bool(*c)
		return &arrowFilterLeaf{Col: -1, ConstBool: &v}, e.ResolvedType(), true
	case *tree.DString:
		s := string(*c)
		return &arrowFilterLeaf{Col: -1, ConstStr: &s}, e.ResolvedType(), true
	case *tree.DDecimal:
		v := *c
		return &arrowFilterLeaf{Col: -1, ConstDecimal: &v}, e.ResolvedType(), true
	case *tree.BinaryExpr:
		var fn string
		switch c.Operator {
		case tree.Plus:
			fn = "add"
		case tree.Minus:
			fn = "subtract"
		case tree.Mult:
			fn = "multiply"
		case tree.Div:
			fn = "divide"
		default:
			return nil, nil, false
		}
		l, lty, ok1 := p.arrowFilterLeafFromExpr(c.Left.(tree.TypedExpr), indexVarMap)
		r, rty, ok2 := p.arrowFilterLeafFromExpr(c.Right.(tree.TypedExpr), indexVarMap)
		if !ok1 || !ok2 {
			return nil, nil, false
		}
		if lty.Family() != rty.Family() {
			return nil, nil, false
		}
		return &arrowFilterLeaf{
			Binary: &arrowFilterBinary{Func: fn, Args: []arrowFilterLeaf{*l, *r}},
		}, lty, true
	case *tree.CastExpr:
		inner, _, ok := p.arrowFilterLeafFromExpr(c.Expr.(tree.TypedExpr), indexVarMap)
		if !ok {
			return nil, nil, false
		}
		// Only casts that depend on a column (not pure constants) are routed to
		// the Arrow engine, so the runtime cast always operates on an array.
		if inner.Col < 0 && inner.Binary == nil && inner.Cast == nil {
			return nil, nil, false
		}
		tag, ok := arrowCastTargetTag(c.ResolvedType())
		if !ok {
			return nil, nil, false
		}
		return &arrowFilterLeaf{
			Cast: &arrowFilterCast{Func: "cast", Type: tag, Arg: *inner},
		}, c.ResolvedType(), true
	case *tree.FuncExpr:
		// Lift supported string functions (substring/trim/concat/replace/...) on
		// columns into the Arrow filter path as a computed leaf, so that e.g.
		// "substring(col,1,3) = 'abc'" runs entirely in the Arrow engine instead
		// of falling back to a row-by-row tree.Datum evaluation.
		fn, ok := arrowStringFuncName(c.Func.String())
		if !ok {
			return nil, nil, false
		}
		args := make([]arrowFilterLeaf, len(c.Exprs))
		for i, a := range c.Exprs {
			al, _, ok := p.arrowFilterLeafFromExpr(a.(tree.TypedExpr), indexVarMap)
			if !ok {
				return nil, nil, false
			}
			args[i] = *al
		}
		return &arrowFilterLeaf{
			Computed: &arrowFilterComputed{Func: fn, Args: args},
		}, c.ResolvedType(), true
	case *tree.CaseExpr:
		// Route CASE/COALESCE in a filter predicate through the Arrow engine by
		// reusing the projection CASE spec. The result is a value leaf that the
		// surrounding comparison/IS-NULL predicate can consume. All branch values
		// are cast to the result type by arrowCaseCol, so the executor's CASE
		// evaluator handles every supported arrow value type uniformly.
		col, _, err := p.arrowCaseCol(c, indexVarMap)
		if err != nil {
			return nil, nil, false
		}
		cc := col
		return &arrowFilterLeaf{
			Case: &cc,
		}, c.ResolvedType(), true
	case *tree.CoalesceExpr:
		col, _, err := p.arrowCoalesceCol(c, indexVarMap)
		if err != nil {
			return nil, nil, false
		}
		cc := col
		return &arrowFilterLeaf{
			Case: &cc,
		}, c.ResolvedType(), true
	}
	return nil, nil, false
}

// arrowCastTargetTag maps a KWDB type to the compact tag used by the Arrow
// filter cast kernel. Only numeric/string targets are supported.
func arrowCastTargetTag(t *types.T) (string, bool) {
	if t == nil {
		return "", false
	}
	switch t.Family() {
	case types.StringFamily, types.BytesFamily:
		return "STRING", true
	case types.IntFamily:
		return "INT", true
	case types.FloatFamily:
		return "FLOAT", true
	case types.DecimalFamily:
		return "DECIMAL", true
	}
	return "", false
}

// arrowFilterStringCastable reports whether a type can be the left operand of a
// LIKE by being cast to string inside the Arrow kernel (matches castToString:
// int/float/bool/string -> string).
func arrowFilterStringCastable(t *types.T) bool {
	switch t.Family() {
	case types.StringFamily, types.IntFamily, types.FloatFamily, types.BoolFamily:
		return true
	}
	return false
}

// canArrowFilterExpr reports whether a boolean filter expression can be
// evaluated entirely by the Arrow compute engine (equality/comparison on
// columns/constants/computed leaves, combined with and/or/not).
func (p *PhysicalPlan) canArrowFilterExpr(e tree.TypedExpr, indexVarMap []int) bool {
	switch ex := e.(type) {
	case *tree.ComparisonExpr:
		// IS NULL / IS NOT NULL: KWDB lowers `x IS NULL` to a ComparisonExpr with
		// a DNull right operand (no dedicated Is/IsNot operator). Only the EQ/NE
		// forms with a column left operand and DNull right operand are routed to
		// the Arrow engine.
		if ex.Right == tree.DNull {
			switch ex.Operator {
			case tree.EQ:
				l, _, ok := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
				if !ok {
					return false
				}
				return l.Col >= 0 || l.Binary != nil || l.Cast != nil || l.Computed != nil
			case tree.NE:
				l, _, ok := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
				if !ok {
					return false
				}
				return l.Col >= 0 || l.Binary != nil || l.Cast != nil || l.Computed != nil
			}
			return false
		}
		switch ex.Operator {
		case tree.EQ, tree.LT, tree.GT, tree.LE, tree.GE, tree.NE:
			l, lty, ok1 := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
			r, rty, ok2 := p.arrowFilterLeafFromExpr(ex.Right.(tree.TypedExpr), indexVarMap)
			if !ok1 || !ok2 {
				return false
			}
			// Require at least one column or computed-column reference so that
			// trivially constant predicates are not accelerated.
			if l.Col < 0 && l.Binary == nil && l.Cast == nil && l.Computed == nil && r.Col < 0 && r.Binary == nil && r.Cast == nil && r.Computed == nil {
				return false
			}
			if lty.Family() != rty.Family() {
				return false
			}
			return arrowSupportedCompareType(lty)
		case tree.Like, tree.NotLike, tree.ILike, tree.NotILike:
			// LIKE requires a string-typed left operand (column or CAST to
			// string) and a constant string pattern. Arrow compute v17 has no
			// match_like kernel, so these are evaluated by a Go kernel; only the
			// constant-pattern form is routed to the Arrow engine.
			l, lty, ok1 := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
			r, rty, ok2 := p.arrowFilterLeafFromExpr(ex.Right.(tree.TypedExpr), indexVarMap)
			if !ok1 || !ok2 {
				return false
			}
			// A non-string left operand (int/float/bool) is cast to string by
			// buildArrowFilterNode so the Arrow kernel can stringify it before
			// matching; only genuinely un-castable types are rejected here.
			if !arrowFilterStringCastable(lty) {
				return false
			}
			if rty.Family() != types.StringFamily {
				return false
			}
			// Left must reference a column/computed column; pattern must be a constant.
			if l.Col < 0 && l.Binary == nil && l.Cast == nil && l.Computed == nil {
				return false
			}
			if r.ConstStr == nil {
				return false
			}
			return true
		case tree.In, tree.NotIn:
			// IN requires a column/computed-column left operand and a tuple of
			// constant values all of the same supported family (int or string,
			// the only types the arrow "in" kernel handles).
			l, lty, ok := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
			if !ok {
				return false
			}
			if l.Col < 0 && l.Binary == nil && l.Cast == nil && l.Computed == nil {
				return false
			}
			tup, ok := ex.Right.(*tree.DTuple)
			if !ok || len(tup.D) == 0 {
				return false
			}
			if !arrowSupportedCompareType(lty) {
				return false
			}
			switch lty.Family() {
			case types.IntFamily, types.StringFamily, types.DecimalFamily:
			default:
				return false
			}
			for _, e := range tup.D {
				rl, rty, ok := p.arrowFilterLeafFromExpr(e.(tree.TypedExpr), indexVarMap)
				if !ok {
					return false
				}
				if rl.Col >= 0 || rl.Binary != nil || rl.Cast != nil {
					// IN set must be constants.
					return false
				}
				if rty.Family() != lty.Family() {
					return false
				}
			}
			return true
		}
		return false
	case *tree.AndExpr:
		return p.canArrowFilterExpr(ex.Left.(tree.TypedExpr), indexVarMap) &&
			p.canArrowFilterExpr(ex.Right.(tree.TypedExpr), indexVarMap)
	case *tree.OrExpr:
		return p.canArrowFilterExpr(ex.Left.(tree.TypedExpr), indexVarMap) &&
			p.canArrowFilterExpr(ex.Right.(tree.TypedExpr), indexVarMap)
	case *tree.NotExpr:
		return p.canArrowFilterExpr(ex.Expr.(tree.TypedExpr), indexVarMap)
	}
	return false
}

// arrowSupportedCompareType reports whether the Arrow comparison kernels can
// handle the given type.
func arrowSupportedCompareType(t *types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily, types.BytesFamily, types.DecimalFamily:
		return true
	}
	return false
}

// ArrowTsScanSupported reports whether every column type of a time-series scan
// can be serialized into an Arrow Record (see rowexec.arrowTsReader, §6.7/§6.8
// 审订 in docs/arrow-unify-roadmap.md). The TS read path emits the same
// relational-format buffer (EncDatumRow) that relational scans do, and
// buildArrowColumns / arrowDataTypeForKWType already cover these types.
func ArrowTsScanSupported(typs []types.T) bool {
	for i := range typs {
		if !arrowSupportedCompareType(&typs[i]) {
			return false
		}
	}
	return true
}

// buildArrowFilterNode translates a typed filter expression into the JSON plan
// tree. The second return is false if the expression cannot be accelerated.
func (p *PhysicalPlan) buildArrowFilterNode(e tree.TypedExpr, indexVarMap []int) (arrowFilterNode, bool) {
	switch ex := e.(type) {
	case *tree.ComparisonExpr:
		if !p.canArrowFilterExpr(e, indexVarMap) {
			return arrowFilterNode{}, false
		}
		var fn string
		switch ex.Operator {
		case tree.EQ:
			// IS NULL: `x IS NULL` lowers to EQ with a DNull right operand.
			if ex.Right == tree.DNull {
				fn = "is_null"
			} else {
				fn = "equal"
			}
		case tree.LT:
			fn = "less"
		case tree.GT:
			fn = "greater"
		case tree.LE:
			fn = "less_equal"
		case tree.GE:
			fn = "greater_equal"
		case tree.NE:
			// IS NOT NULL: `x IS NOT NULL` lowers to NE with a DNull right operand.
			if ex.Right == tree.DNull {
				fn = "is_not_null"
			} else {
				fn = "not_equal"
			}
		case tree.Like:
			fn = "like"
		case tree.NotLike:
			fn = "not_like"
		case tree.ILike:
			fn = "ilike"
		case tree.NotILike:
			fn = "not_ilike"
		case tree.In:
			fn = "in"
		case tree.NotIn:
			fn = "not_in"
		}
		// IS NULL / IS NOT NULL take a single (column) operand; the right DNull
		// is dropped entirely.
		if fn == "is_null" || fn == "is_not_null" {
			l, _, ok := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
			if !ok {
				return arrowFilterNode{}, false
			}
			return arrowFilterNode{
				Func:     fn,
				Operands: []arrowFilterOperand{{Leaf: l}},
			}, true
		}
		l, lty, _ := p.arrowFilterLeafFromExpr(ex.Left.(tree.TypedExpr), indexVarMap)
		// LIKE-style predicates stringify a non-string left operand via a CAST to
		// STRING so the Arrow kernel can match it against the string pattern
		// (e.g. `i LIKE '1%'` on an integer column). Regular comparisons
		// (EQ/LT/GT/...) keep their native operand types and must never be cast;
		// casting them would compare a string against the (untyped) right operand
		// and break numeric/boolean filters like `a * 2 > b`.
		if (ex.Operator == tree.Like || ex.Operator == tree.NotLike ||
			ex.Operator == tree.ILike || ex.Operator == tree.NotILike) &&
			lty.Family() != types.StringFamily {
			if tag, ok := arrowCastTargetTag(types.String); ok {
				l = &arrowFilterLeaf{Cast: &arrowFilterCast{Func: "cast", Type: tag, Arg: *l}}
			} else {
				return arrowFilterNode{}, false
			}
		}
		var r *arrowFilterLeaf
		if ex.Operator == tree.In || ex.Operator == tree.NotIn {
			// The right operand is a tuple of constants; build a ConstSet leaf
			// from its elements (all same family, validated by canArrowFilterExpr).
			tup := ex.Right.(*tree.DTuple)
			set := &arrowFilterLeaf{Col: -1}
			switch lty.Family() {
			case types.IntFamily:
				ints := make([]int64, 0, len(tup.D))
				for _, e := range tup.D {
					rl, _, _ := p.arrowFilterLeafFromExpr(e.(tree.TypedExpr), indexVarMap)
					if rl.ConstInt == nil {
						return arrowFilterNode{}, false
					}
					ints = append(ints, *rl.ConstInt)
				}
				set.ConstSetInt = ints
			case types.StringFamily:
				strs := make([]string, 0, len(tup.D))
				for _, e := range tup.D {
					rl, _, _ := p.arrowFilterLeafFromExpr(e.(tree.TypedExpr), indexVarMap)
					if rl.ConstStr == nil {
						return arrowFilterNode{}, false
					}
					strs = append(strs, *rl.ConstStr)
				}
				set.ConstSetStr = strs
			case types.DecimalFamily:
				// Arrow compute has no DECIMAL is_in kernel; cast the decimal
				// column to FLOAT and compare against float constants (matching
				// the existing castToFloat64 path). For decimals within float64
				// precision this is exact; very large magnitudes may lose low
				// bits, consistent with the float cast path's precision contract.
				if tag, ok := arrowCastTargetTag(types.Float); ok {
					l = &arrowFilterLeaf{Cast: &arrowFilterCast{Func: "cast", Type: tag, Arg: *l}}
				} else {
					return arrowFilterNode{}, false
				}
				floats := make([]float64, 0, len(tup.D))
				for _, e := range tup.D {
					rl, _, _ := p.arrowFilterLeafFromExpr(e.(tree.TypedExpr), indexVarMap)
					if rl.ConstDecimal == nil {
						return arrowFilterNode{}, false
					}
					f, _ := rl.ConstDecimal.Float64()
					floats = append(floats, f)
				}
				set.ConstSetFloat = floats
			default:
				return arrowFilterNode{}, false
			}
			r = set
		} else {
			rb, _, _ := p.arrowFilterLeafFromExpr(ex.Right.(tree.TypedExpr), indexVarMap)
			r = rb
		}
		return arrowFilterNode{
			Func: fn,
			Operands: []arrowFilterOperand{
				{Leaf: l},
				{Leaf: r},
			},
		}, true
	case *tree.AndExpr:
		l, ok1 := p.buildArrowFilterNode(ex.Left.(tree.TypedExpr), indexVarMap)
		r, ok2 := p.buildArrowFilterNode(ex.Right.(tree.TypedExpr), indexVarMap)
		if !ok1 || !ok2 {
			return arrowFilterNode{}, false
		}
		return arrowFilterNode{Func: "and", Operands: []arrowFilterOperand{{Expr: &l}, {Expr: &r}}}, true
	case *tree.OrExpr:
		l, ok1 := p.buildArrowFilterNode(ex.Left.(tree.TypedExpr), indexVarMap)
		r, ok2 := p.buildArrowFilterNode(ex.Right.(tree.TypedExpr), indexVarMap)
		if !ok1 || !ok2 {
			return arrowFilterNode{}, false
		}
		return arrowFilterNode{Func: "or", Operands: []arrowFilterOperand{{Expr: &l}, {Expr: &r}}}, true
	case *tree.NotExpr:
		c, ok := p.buildArrowFilterNode(ex.Expr.(tree.TypedExpr), indexVarMap)
		if !ok {
			return arrowFilterNode{}, false
		}
		return arrowFilterNode{Func: "not", Operands: []arrowFilterOperand{{Expr: &c}}}, true
	}
	return arrowFilterNode{}, false
}

// addArrowFilter builds a dedicated arrow filter stage that evaluates the
// boolean expression via the Arrow compute engine and emits the matching input
// rows (all columns preserved).
func (p *PhysicalPlan) addArrowFilter(
	expr tree.TypedExpr, exprCtx ExprContext, indexVarMap []int,
) error {
	node, ok := p.buildArrowFilterNode(expr, indexVarMap)
	if !ok {
		return fmt.Errorf("arrow filter: expression not arrow-computable")
	}
	plan := arrowFilterPlan{Root: node}
	b, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	core := execinfrapb.ProcessorCoreUnion{
		ArrowFilter: &execinfrapb.Expression{Expr: string(b)},
	}
	p.AddNoGroupingStage(core, execinfrapb.PostProcessSpec{}, p.ResultTypes, p.MergeOrdering)
	return nil
}

// InterceptArrowFilterForScan, when the given scan filter is arrow-computable,
// strips the filter from the current last-stage post (the TableReader that
// carries it) and adds a dedicated ArrowFilter stage that outputs the same
// columns. It must be invoked before any projection is applied to the plan.
// The caller is responsible for gating on the cluster setting (it is expected
// to only call this when ArrowFilterEnabled is true). It returns true if it
// intercepted the filter.
func (p *PhysicalPlan) InterceptArrowFilterForScan(filter tree.TypedExpr, indexVarMap []int) bool {
	if filter == nil {
		return false
	}
	if !p.canArrowFilterExpr(filter, indexVarMap) {
		return false
	}
	node, ok := p.buildArrowFilterNode(filter, indexVarMap)
	if !ok {
		return false
	}
	b, err := json.Marshal(arrowFilterPlan{Root: node})
	if err != nil {
		return false
	}
	// Strip the filter from the current last-stage post (the TableReader) and
	// perform the filtering in a dedicated ArrowFilter stage instead.
	post := p.GetLastStagePost()
	post.Filter = execinfrapb.Expression{}
	p.SetLastStagePost(post, p.ResultTypes)
	core := execinfrapb.ProcessorCoreUnion{
		ArrowFilter: &execinfrapb.Expression{Expr: string(b)},
	}
	p.AddNoGroupingStage(core, execinfrapb.PostProcessSpec{}, p.ResultTypes, p.MergeOrdering)
	return true
}

// reverseProjection remaps expression variable indices to refer to internal
// columns (i.e. before post-processing) of a processor instead of output
// columns (i.e. after post-processing).
//
// Inputs:
//
//	indexVarMap is a mapping from columns that appear in an expression
//	            (planNode columns) to columns in the output stream of a
//	            processor.
//	outputColumns is the list of output columns in the processor's
//	              PostProcessSpec; it is effectively a mapping from the output
//	              schema to the internal schema of a processor.
//
// Result: a "composite map" that maps the planNode columns to the internal
//
//	columns of the processor.
//
// For efficiency, the indexVarMap and the resulting map are represented as
// slices, with missing elements having values -1.
//
// Used when adding expressions (filtering, rendering) to a processor's
// PostProcessSpec. For example:
//
//	TableReader // table columns A,B,C,D
//	Internal schema (before post-processing): A, B, C, D
//	OutputColumns:  [1 3]
//	Output schema (after post-processing): B, D
//
//	Expression "B < D" might be represented as:
//	  IndexedVar(4) < IndexedVar(1)
//	with associated indexVarMap:
//	  [-1 1 -1 -1 0]  // 1->1, 4->0
//	This is effectively equivalent to "IndexedVar(0) < IndexedVar(1)"; 0 means
//	the first output column (B), 1 means the second output column (D).
//
//	To get an index var map that refers to the internal schema:
//	  reverseProjection(
//	    [1 3],           // OutputColumns
//	    [-1 1 -1 -1 0],
//	  ) =
//	    [-1 3 -1 -1 1]   // 1->3, 4->1
//	This is effectively equivalent to "IndexedVar(1) < IndexedVar(3)"; 1
//	means the second internal column (B), 3 means the fourth internal column
//	(D).
func reverseProjection(outputColumns []uint32, indexVarMap []int) []int {
	if indexVarMap == nil {
		panic("no indexVarMap")
	}
	compositeMap := make([]int, len(indexVarMap))
	for i, col := range indexVarMap {
		if col == -1 {
			compositeMap[i] = -1
		} else {
			compositeMap[i] = int(outputColumns[col])
		}
	}
	return compositeMap
}

// AddFilterToPostSpec add filter to post spec
func (p *PhysicalPlan) AddFilterToPostSpec(filter *execinfrapb.Expression, execInTSEngine bool) {
	if execInTSEngine && p.ChildIsTSParallelProcessor() {
		for _, pIdx := range p.SynchronizerChildRouters {
			p.Processors[pIdx].Spec.Post.Filter = *filter
		}
	} else if execInTSEngine {
		for _, pIdx := range p.ResultRouters {
			p.Processors[pIdx].Spec.Post.Filter = *filter
		}
	} else {
		for _, pIdx := range p.ResultRouters {
			p.Processors[pIdx].Spec.Post.Filter = *filter
		}
	}
}

// AddRelationalFilter add relational filter
func (p *PhysicalPlan) AddRelationalFilter(
	expr tree.TypedExpr,
	exprCtx ExprContext,
	indexVarMap []int,
	post *execinfrapb.PostProcessSpec,
	addNoop bool,
	execInTSEngine bool,
) error {
	// Arrow filter acceleration: route arrow-computable boolean filter
	// expressions through the Arrow compute engine via a dedicated stage. We
	// only do this when the current post is a simple identity (no filter, no
	// render, no offset/limit) so we don't disturb downstream post-processing.
	if arrowFilterEnabled(exprCtx.EvalContext()) && post.Filter.Empty() &&
		len(post.RenderExprs) == 0 && post.Offset == 0 && post.Limit == 0 {
		if node, ok := p.buildArrowFilterNode(expr, indexVarMap); ok {
			plan := arrowFilterPlan{Root: node}
			b, err := json.Marshal(plan)
			if err != nil {
				return err
			}
			core := execinfrapb.ProcessorCoreUnion{
				ArrowFilter: &execinfrapb.Expression{Expr: string(b)},
			}
			p.AddNoGroupingStage(core, execinfrapb.PostProcessSpec{}, p.ResultTypes, p.MergeOrdering)
			return nil
		}
	}

	if addNoop {
		*post = execinfrapb.PostProcessSpec{OutputTypes: p.ResultTypes}
		p.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			*post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}
	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	filter, err := MakeExpression(expr, exprCtx, compositeMap, len(p.ResultRouters) == 1, execInTSEngine)
	if err != nil {
		return err
	}
	if !post.Filter.Empty() {
		// Either Expr or LocalExpr will be set (not both).
		if filter.Expr != "" {
			filter.Expr = fmt.Sprintf("(%s) AND (%s)", post.Filter.Expr, filter.Expr)
		} else if filter.LocalExpr != nil {
			filter.LocalExpr = tree.NewTypedAndExpr(
				post.Filter.LocalExpr,
				filter.LocalExpr,
			)
		}
	}
	p.AddFilterToPostSpec(&filter, false)
	return nil
}

// BuildArrowOnExprJSON builds the arrow filter JSON plan for a join onExpr
// (§7.4). It returns ("", false) when the expression is not arrow-computable, so
// the caller can fall back to the standard join engine. The resulting JSON shape
// mirrors arrowFilterPlan and is consumed by the arrow join executor as a
// post-filter stage applied after the equi-join.
func (p *PhysicalPlan) BuildArrowOnExprJSON(e tree.TypedExpr, indexVarMap []int) (string, bool) {
	if !p.canArrowFilterExpr(e, indexVarMap) {
		return "", false
	}
	node, ok := p.buildArrowFilterNode(e, indexVarMap)
	if !ok {
		return "", false
	}
	b, err := json.Marshal(arrowFilterPlan{Root: node})
	if err != nil {
		return "", false
	}
	return string(b), true
}

// AddFilter adds a filter on the output of a plan. The filter is added either
// as a post-processing step to the last stage or to a new "no-op" stage, as
// necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddFilter(
	expr tree.TypedExpr, exprCtx ExprContext, indexVarMap []int, filterCanExecInTSEngine bool,
) error {
	if expr == nil {
		return errors.Errorf("nil filter")
	}

	// child can not exec in ts engine
	if !p.ChildIsExecInTSEngine() {
		post := p.GetLastStagePost()
		addNoop := len(post.RenderExprs) > 0 || post.Offset != 0 || post.Limit != 0
		// The last stage contains render expressions or a limit. The filter refers
		// to the output as described by the existing spec, so we need to add
		// another "no-op" stage to which to attach the filter.
		//
		// In general, we might be able to canExecInTSEngine the filter "through" the rendering;
		// but the higher level planning code should figure this out when
		// propagating filters.
		return p.AddRelationalFilter(expr, exprCtx, indexVarMap, &post, addNoop, filterCanExecInTSEngine)
	}

	// child spec run in ts engine
	if !filterCanExecInTSEngine {
		post1 := execinfrapb.PostProcessSpec{}
		return p.AddRelationalFilter(expr, exprCtx, indexVarMap, &post1, true, filterCanExecInTSEngine)
	}

	// child is ts post
	post := p.Processors[p.ResultRouters[0]].Spec.Post
	childIsSort := p.Processors[p.ResultRouters[0]].Spec.Core.Sorter != nil
	// some cases need add ts noop spec, reference relationship case
	// case1: post of child plan have render
	// case2: post of child plan have offset
	// case3: post of child plan have limit
	// case4: child plan is sort (AE sort no support filter)
	if len(post.RenderExprs) > 0 || post.Offset != 0 || post.Limit != 0 || childIsSort {
		// The last stage contains render expressions or a limit. The filter refers
		// to the output as described by the existing spec, so we need to add
		// another "no-op" stage to which to attach the filter.
		//
		// In general, we might be able to exec in ts engine the filter "through" the rendering;
		// but the higher level planning code should figure this out when
		// propagating filters.
		post = execinfrapb.PostProcessSpec{}
		p.AddTSNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	filter, err := MakeTSExpression(expr, exprCtx, compositeMap)
	if err != nil {
		return err
	}
	if !post.Filter.Empty() {
		// Either Expr or LocalExpr will be set (not both).
		if filter.Expr != "" {
			filter.Expr = fmt.Sprintf("(%s) AND (%s)", post.Filter, filter.Expr)
		}
	}

	p.AddFilterToPostSpec(&filter, filterCanExecInTSEngine)
	return nil
}

// emptyPlan creates a plan with a single processor that generates no rows; the
// output stream has the given types.
func emptyPlan(types []types.T, node roachpb.NodeID) PhysicalPlan {
	s := execinfrapb.ValuesCoreSpec{
		Columns: make([]execinfrapb.DatumInfo, len(types)),
	}
	for i, t := range types {
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	return PhysicalPlan{
		Processors: []Processor{{
			Node: node,
			Spec: execinfrapb.ProcessorSpec{
				Core:   execinfrapb.ProcessorCoreUnion{Values: &s},
				Output: make([]execinfrapb.OutputRouterSpec, 1),
			},
		}},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   types,
	}
}

func (p *PhysicalPlan) checkLimitOffsetZero(offset uint64) bool {
	// We only have one processor producing results. Just update its PostProcessSpec.
	// SELECT FROM (SELECT OFFSET 10 LIMIT 1000) OFFSET 5 LIMIT 20 becomes
	// SELECT OFFSET 10+5 LIMIT min(1000, 20).
	var limitPost uint64
	if p.ChildIsExecInTSEngine() {
		post := p.GetLastStageTSPost()
		limitPost = uint64(post.Limit)
	} else {
		post := p.GetLastStagePost()
		limitPost = post.Limit
	}

	if offset != 0 {
		if limitPost > 0 && limitPost <= offset {
			return true
		}
	}

	return false
}

func dealWithOffset(offset uint64, count int64, post *execinfrapb.PostProcessSpec) {
	if offset != 0 {
		// If we're collapsing an offset into a stage that already has a limit,
		// we have to be careful, since offsets always are applied first, before
		// limits. So, if the last stage already has a limit, we subtract the
		// offset from that limit to preserve correctness.
		//
		// As an example, consider the requirement of applying an offset of 3 on
		// top of a limit of 10. In this case, we need to emit 7 result rows. But
		// just propagating the offset blindly would produce 10 result rows, an
		// incorrect result.
		post.Offset += offset
		if post.Limit > 0 {
			// Note that this can't fall below 0 - we would have already caught this
			// case above and returned an empty plan.
			post.Limit -= offset
		}
	}
	if count != math.MaxInt64 && (post.Limit == 0 || post.Limit > uint64(count)) {
		post.Limit = uint64(count)
	}
}

func (p *PhysicalPlan) handleZeroLimitOrOffSetEmpty(
	count *int64, limitZero *bool, node roachpb.NodeID,
) bool {
	if len(p.LocalProcessors) == 0 {
		*p = emptyPlan(p.ResultTypes, node)
		return true
	}
	*count = 1
	*limitZero = true
	return false
}

func (p *PhysicalPlan) handleSingleRouterLimitAndOffset(
	offset uint64, count int64, limitZero bool, exprCtx ExprContext, node roachpb.NodeID, push bool,
) error {
	// We only have one processor producing results. Just update its PostProcessSpec.
	// SELECT FROM (SELECT OFFSET 10 LIMIT 1000) OFFSET 5 LIMIT 20 becomes
	// SELECT OFFSET 10+5 LIMIT min(1000, 20).
	if p.checkLimitOffsetZero(offset) && p.handleZeroLimitOrOffSetEmpty(&count, &limitZero, node) {
		return nil
	}

	if p.ChildIsExecInTSEngine() {
		var post execinfrapb.PostProcessSpec
		if p.ChildIsTSParallelProcessor() {
			p.AddNoGroupingStageForTSNoop(nil)
		}
		post = p.GetLastStageTSPost()
		postTmp := execinfrapb.PostProcessSpec{Limit: uint64(post.Limit), Offset: uint64(post.Offset)}
		dealWithOffset(offset, count, &postTmp)
		post.Limit = postTmp.Limit
		post.Offset = postTmp.Offset
		p.SetLastStageTSPost(post, p.ResultTypes)
	} else {
		if p.ChildIsTSParallelProcessor() {
			// limit child is synchronizer , can not push down limit to it is child, parallel limit need twice limit
			p.AddTSTableReader(p.ResultTypes)
		}

		post := p.GetLastStagePost()
		dealWithOffset(offset, count, &post)
		p.SetLastStagePost(post, p.ResultTypes)
	}

	if limitZero {
		if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil, push); err != nil {
			return err
		}
	}
	return nil
}

// setLocalLimitForMultiRouter
// We have multiple processors producing results. We will add a single processor stage that limits.
// As an optimization, we also set a "local" limit on each processor producing results.
func (p *PhysicalPlan) setLocalLimitForMultiRouter(count int64, offset int64) uint64 {
	localLimit := uint64(count)
	if count != math.MaxInt64 {
		// If we have OFFSET 10 LIMIT 5, we may need as much as 15 rows from any
		// processor.
		localLimit = uint64(count + offset)
		if p.ChildIsExecInTSEngine() {
			if p.ChildIsTSParallelProcessor() {
				p.AddNoGroupingStageForTSNoop(nil)
			}
			post := p.GetLastStageTSPost()
			if post.Limit == 0 || post.Limit > localLimit {
				post.Limit = localLimit
				p.SetLastStageTSPost(post, p.ResultTypes)
			}
		} else {
			post := p.GetLastStagePost()
			if post.Limit == 0 || post.Limit > localLimit {
				post.Limit = localLimit
				p.SetLastStagePost(post, p.ResultTypes)
			}
		}
	}
	return localLimit
}

// AddLimit adds a limit and/or offset to the results of the current plan. If
// there are multiple result streams, they are joined into a single processor
// that is placed on the given node.
//
// For no limit, count should be MaxInt64.
func (p *PhysicalPlan) AddLimit(
	count int64, offset int64, exprCtx ExprContext, node roachpb.NodeID, push bool,
) error {
	if count < 0 {
		return errors.Errorf("negative limit")
	}
	if offset < 0 {
		return errors.Errorf("negative offset")
	}
	// limitZero is set to true if the limit is a legitimate LIMIT 0 requested by
	// the user. This needs to be tracked as a separate condition because DistSQL
	// uses count=0 to mean no limit, not a limit of 0. Normally, DistSQL will
	// short circuit 0-limit plans, but wrapped local planNodes sometimes need to
	// be fully-executed despite having 0 limit, so if we do in fact have a
	// limit-0 case when there's local planNodes around, we add an empty plan
	// instead of completely eliding the 0-limit plan.
	limitZero := false
	if count == 0 && p.handleZeroLimitOrOffSetEmpty(&count, &limitZero, node) {
		return nil
	}

	if len(p.ResultRouters) == 1 {
		return p.handleSingleRouterLimitAndOffset(uint64(offset), count, limitZero, exprCtx, node, push)
	}

	localLimit := p.setLocalLimitForMultiRouter(count, offset)

	// multi node need add noop for agent get data to kwbase, add local limit
	if p.ChildIsExecInTSEngine() {
		p.AddNoop(&execinfrapb.PostProcessSpec{Limit: localLimit, OutputTypes: p.ResultTypes}, nil)
	}

	post := execinfrapb.PostProcessSpec{
		Offset:      uint64(offset),
		OutputTypes: p.ResultTypes,
	}
	if count != math.MaxInt64 {
		post.Limit = uint64(count)
	}

	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		post,
		p.ResultTypes,
	)

	if limitZero {
		if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil, push); err != nil {
			return err
		}
	}
	return nil
}

// PopulateEndpoints processes p.Streams and adds the corresponding
// StreamEndpointSpecs to the processors' input and output specs. This should be
// used when the plan is completed and ready to be executed.
//
// The nodeAddresses map contains the address of all the nodes referenced in the
// plan.
func (p *PhysicalPlan) PopulateEndpoints(nodeAddresses map[roachpb.NodeID]string) {
	// Note: instead of using p.Streams, we could fill in the input/output specs
	// directly throughout the planning code, but this makes the rest of the code
	// a bit simpler.
	for sIdx, s := range p.Streams {
		p1 := &p.Processors[s.SourceProcessor]
		p2 := &p.Processors[s.DestProcessor]
		endpoint := execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(sIdx), DestProcessor: int32(s.DestProcessor)}
		if p1.Node == p2.Node {
			if p1.ExecInTSEngine() && !p2.ExecInTSEngine() {
				endpoint.Type = execinfrapb.StreamEndpointType_QUEUE
				p1.Spec.FinalTsProcessor = true
			} else {
				endpoint.Type = execinfrapb.StreamEndpointType_LOCAL
			}
		} else {
			endpoint.Type = execinfrapb.StreamEndpointType_REMOTE
		}
		p2.Spec.Input[s.DestInput].Streams = append(p2.Spec.Input[s.DestInput].Streams, endpoint)
		if endpoint.Type == execinfrapb.StreamEndpointType_REMOTE {
			if !p.remotePlan {
				p.remotePlan = true
			}
			endpoint.TargetNodeID = p2.Node
		}

		var router *execinfrapb.OutputRouterSpec
		router = &p1.Spec.Output[0]
		// We are about to put this stream on the len(router.Streams) position in
		// the router; verify this matches the sourceRouterSlot. We expect it to
		// because the streams should be in order; if that assumption changes we can
		// reorder them here according to sourceRouterSlot.
		if len(router.Streams) != s.SourceRouterSlot {
			panic(fmt.Sprintf(
				"sourceRouterSlot mismatch: %d, expected %d", len(router.Streams), s.SourceRouterSlot,
			))
		}
		router.Streams = append(router.Streams, endpoint)
	}
}

// GenerateFlowSpecs takes a plan (with populated endpoints) and generates the
// set of FlowSpecs (one per node involved in the plan).
//
// gateway is the current node's NodeID.
func (p *PhysicalPlan) GenerateFlowSpecs(
	gateway roachpb.NodeID, gossip *gossip.Gossip,
) (map[roachpb.NodeID]*execinfrapb.FlowSpec, error) {
	// Only generate a flow ID for a remote plan because it will need to be
	// referenced by remote nodes when connecting streams. This id generation is
	// skipped for performance reasons on local flows.
	flowID := execinfrapb.FlowID{}
	if p.remotePlan {
		flowID.UUID = uuid.MakeV4()
	}

	flows := make(map[roachpb.NodeID]*execinfrapb.FlowSpec, 1)

	maxNodeID := 0
	useAeGather := false
	for _, proc := range p.Processors {
		if maxNodeID < int(proc.Node) {
			maxNodeID = int(proc.Node)
		}
		flowSpec, ok := flows[proc.Node]
		if !ok {
			flowSpec = NewFlowSpec(flowID, gateway)
			flows[proc.Node] = flowSpec
		}
		if proc.ExecInTSEngine() && !useAeGather {
			for _, input := range proc.Spec.Input {
				for _, stream := range input.Streams {
					if stream.Type == execinfrapb.StreamEndpointType_REMOTE {
						useAeGather = true
					}
				}
			}
		}
		flowSpec.Processors = append(flowSpec.Processors, proc.Spec)
	}

	queryID := generateQueryID(uint16(gateway))

	//set brpcAddress for ae.
	brpcAddrs := make([]string, maxNodeID)
	for nodeID := range flows {
		if gossip != nil {
			node, err := gossip.GetNodeDescriptor(nodeID)
			if err != nil {
				log.Warning(context.Background(), err)
				return nil, errors.Errorf("unable to get descriptor for n%d", nodeID)
			}
			brpcAddrs[nodeID-1] = node.BrpcAddress.String()
		}
	}
	for nodeID, flow := range flows {
		flow.TsInfo.BrpcAddrs = brpcAddrs
		flow.TsInfo.UseAeGather = useAeGather
		flow.TsInfo.QueryID = queryID
		if nodeID == gateway {
			flow.TsInfo.UseQueryShortCircuit = p.UseQueryShortCircuit
			flow.TsInfo.UseCompressType = p.UseCompressType
		}
	}

	return flows, nil
}

const maxQueryID = 1<<47 - 1

// QueryIDForTS is query id for AE.
var QueryIDForTS = int64(0)

func generateQueryID(nodeID uint16) int64 {
	queryID := atomic.AddInt64(&QueryIDForTS, 1)

	// Reset when query ID exceeds maximum value
	if queryID > maxQueryID {
		queryID = (queryID & maxQueryID) + 1

		atomic.CompareAndSwapInt64(&QueryIDForTS, atomic.LoadInt64(&QueryIDForTS), queryID)
	}

	// QueryID occupies the upper 48 bits, while nodeID occupies the lower 16 bits.
	return (queryID << 16) | int64(nodeID)
}

// SetRowEstimates updates p according to the row estimates of left and right
// plans.
func (p *PhysicalPlan) SetRowEstimates(left, right *PhysicalPlan) {
	p.TotalEstimatedScannedRows = left.TotalEstimatedScannedRows + right.TotalEstimatedScannedRows
	p.MaxEstimatedRowCount = left.MaxEstimatedRowCount
	if right.MaxEstimatedRowCount > p.MaxEstimatedRowCount {
		p.MaxEstimatedRowCount = right.MaxEstimatedRowCount
	}
}

// MergePlans merges the processors and streams of two plan into a new plan.
// The result routers for each side are also returned (they point at processors
// in the merged plan).
func MergePlans(
	left, right *PhysicalPlan,
) (mergedPlan PhysicalPlan, leftRouters []ProcessorIdx, rightRouters []ProcessorIdx) {
	mergedPlan.Processors = append(left.Processors, right.Processors...)
	rightProcStart := ProcessorIdx(len(left.Processors))

	mergedPlan.Streams = append(left.Streams, right.Streams...)

	// Update the processor indices in the right streams.
	for i := len(left.Streams); i < len(mergedPlan.Streams); i++ {
		mergedPlan.Streams[i].SourceProcessor += rightProcStart
		mergedPlan.Streams[i].DestProcessor += rightProcStart
	}

	// Renumber the stages from the right plan.
	for i := rightProcStart; int(i) < len(mergedPlan.Processors); i++ {
		s := &mergedPlan.Processors[i].Spec
		if s.StageID != 0 {
			s.StageID += left.stageCounter
		}
	}
	mergedPlan.stageCounter = left.stageCounter + right.stageCounter

	mergedPlan.LocalProcessors = append(left.LocalProcessors, right.LocalProcessors...)
	mergedPlan.LocalProcessorIndexes = append(left.LocalProcessorIndexes, right.LocalProcessorIndexes...)
	// Update the local processor indices in the right streams.
	for i := len(left.LocalProcessorIndexes); i < len(mergedPlan.LocalProcessorIndexes); i++ {
		*mergedPlan.LocalProcessorIndexes[i] += uint32(len(left.LocalProcessorIndexes))
	}

	leftRouters = left.ResultRouters
	rightRouters = append([]ProcessorIdx(nil), right.ResultRouters...)
	// Update the processor indices in the right routers.
	for i := range rightRouters {
		rightRouters[i] += rightProcStart
	}

	mergedPlan.SetRowEstimates(left, right)

	return mergedPlan, leftRouters, rightRouters
}

// MergeResultTypes reconciles the ResultTypes between two plans. It enforces
// that each pair of ColumnTypes must either match or be null, in which case the
// non-null type is used. This logic is necessary for cases like
// SELECT NULL UNION SELECT 1.
func MergeResultTypes(left, right []types.T) ([]types.T, error) {
	if len(left) != len(right) {
		return nil, errors.Errorf("ResultTypes length mismatch: %d and %d", len(left), len(right))
	}
	merged := make([]types.T, len(left))
	for i := range left {
		leftType, rightType := &left[i], &right[i]
		if rightType.Family() == types.UnknownFamily {
			merged[i] = *leftType
		} else if leftType.Family() == types.UnknownFamily {
			merged[i] = *rightType
		} else if equivalentTypes(leftType, rightType) {
			merged[i] = *leftType
		} else {
			return nil, errors.Errorf(
				"conflicting ColumnTypes: %s and %s", leftType.DebugString(), rightType.DebugString())
		}
	}
	return merged, nil
}

// equivalentType checks whether a column type is equivalent to another for the
// purpose of UNION. Precision, Width, Oid, etc. do not affect the merging of
// values.
func equivalentTypes(c, other *types.T) bool {
	return c.Equivalent(other)
}

// AddJoinStage adds join processors at each of the specified nodes, and wires
// the left and right-side outputs to these processors.
func (p *PhysicalPlan) AddJoinStage(
	nodes []roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	leftEqCols, rightEqCols []uint32,
	leftTypes, rightTypes []types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	for _, n := range nodes {
		inputs := make([]execinfrapb.InputSyncSpec, 0, 2)
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: leftTypes})
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: rightTypes})

		proc := Processor{
			Node: n,
			Spec: execinfrapb.ProcessorSpec{
				Input:   inputs,
				Core:    core,
				Post:    post,
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		p.Processors = append(p.Processors, proc)
	}

	if len(nodes) > 1 {
		// Parallel hash or merge join: we distribute rows (by hash of
		// equality columns) to len(nodes) join processors.

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: leftEqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: rightEqCols,
			}
		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0, false /* forceSerialization */)
		// Connect right routers to the processor's second input if it has one.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1, false /* forceSerialization */)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// AddBLJoinStage adds join processors at each of the specified nodes, and wires
// the left and right-side outputs to these processors.
func (p *PhysicalPlan) AddBLJoinStage(
	nodes []roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	leftTypes, rightTypes []types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	if len(nodes) > 1 {
		for _, idx := range rightRouters {
			inputs := make([]execinfrapb.InputSyncSpec, 0, 2)
			inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: leftTypes})
			inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: rightTypes})
			proc := Processor{
				Node: p.Processors[idx].Node,
				Spec: execinfrapb.ProcessorSpec{
					Input:   inputs,
					Core:    core,
					Post:    post,
					Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					StageID: stageID,
				},
			}
			p.Processors = append(p.Processors, proc)

			// change SourceRouterSlot of streams, this is the second stream.
			sourceRouterSlot := 0
			for i := len(p.Streams) - 1; i >= 0; i-- {
				if p.Streams[i].SourceProcessor == idx {
					sourceRouterSlot = 1
					break
				}
			}
			// each ts plan connects to the local BLJ.
			p.Streams = append(p.Streams, Stream{
				SourceProcessor:  idx,
				SourceRouterSlot: sourceRouterSlot,
				DestProcessor:    ProcessorIdx(len(p.Processors) - 1),
				DestInput:        1,
			})
		}

		// Set up the left OutputRouters.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type: execinfrapb.OutputRouterSpec_MIRROR,
			}
		}

		// Connect the left routers to the BLJ. Each BLJ corresponds to a hash bucket.
		for bucket := 0; bucket < len(rightRouters); bucket++ {
			pIdx := pIdxStart + ProcessorIdx(bucket)

			// Connect left routers to the processor's first input. Currently the join
			// node doesn't care about the orderings of the left and right results.
			p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0, false /* forceSerialization */)

			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
	} else {
		p.ResultRouters = nil
		for _, n := range nodes {
			inputs := make([]execinfrapb.InputSyncSpec, 0, 2)
			inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: leftTypes})
			inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: rightTypes})
			proc := Processor{
				Node: n,
				Spec: execinfrapb.ProcessorSpec{
					Input:   inputs,
					Core:    core,
					Post:    post,
					Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					StageID: stageID,
				},
			}
			p.Processors = append(p.Processors, proc)
		}

		// Connect the left and right routers to the output joiners. Each joiner
		// corresponds to a hash bucket.
		for bucket := 0; bucket < len(nodes); bucket++ {
			pIdx := pIdxStart + ProcessorIdx(bucket)

			// Connect left routers to the processor's first input. Currently the join
			// node doesn't care about the orderings of the left and right results.
			p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0, false /* forceSerialization */)
			// Connect right routers to the processor's second input if it has one.
			p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1, false /* forceSerialization */)

			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
	}
}

// AddDistinctSetOpStage creates a distinct stage and a join stage to implement
// INTERSECT and EXCEPT plans.
//
// TODO(abhimadan): If there's a strong key on the left or right side, we
// can elide the distinct stage on that side.
func (p *PhysicalPlan) AddDistinctSetOpStage(
	nodes []roachpb.NodeID,
	joinCore execinfrapb.ProcessorCoreUnion,
	distinctCores []execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	eqCols []uint32,
	leftTypes, rightTypes []types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
) {
	const numSides = 2
	inputResultTypes := [numSides][]types.T{leftTypes, rightTypes}
	inputMergeOrderings := [numSides]execinfrapb.Ordering{leftMergeOrd, rightMergeOrd}
	inputResultRouters := [numSides][]ProcessorIdx{leftRouters, rightRouters}

	// Create distinct stages for the left and right sides, where left and right
	// sources are sent by hash to the node which will contain the join processor.
	// The distinct stage must be before the join stage for EXCEPT queries to
	// produce correct results (e.g., (VALUES (1),(1),(2)) EXCEPT (VALUES (1))
	// would return (1),(2) instead of (2) if there was no distinct processor
	// before the EXCEPT ALL join).
	distinctIdxStart := len(p.Processors)
	distinctProcs := make(map[roachpb.NodeID][]ProcessorIdx)

	for side, types := range inputResultTypes {
		distinctStageID := p.NewStageID()
		for _, n := range nodes {
			proc := Processor{
				Node: n,
				Spec: execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{
						{ColumnTypes: types},
					},
					Core:    distinctCores[side],
					Post:    execinfrapb.PostProcessSpec{},
					Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					StageID: distinctStageID,
				},
			}
			pIdx := p.AddProcessor(proc)
			distinctProcs[n] = append(distinctProcs[n], pIdx)
		}
	}

	if len(nodes) > 1 {
		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: eqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: eqCols,
			}
		}
	}

	// Connect the left and right streams to the distinct processors.
	for side, routers := range inputResultRouters {
		// Get the processor index offset for the current side.
		sideOffset := side * len(nodes)
		for bucket := 0; bucket < len(nodes); bucket++ {
			pIdx := ProcessorIdx(distinctIdxStart + sideOffset + bucket)
			p.MergeResultStreams(routers, bucket, inputMergeOrderings[side], pIdx, 0, false /* forceSerialization */)
		}
	}

	// Create a join stage, where the distinct processors on the same node are
	// connected to a join processor.
	joinStageID := p.NewStageID()
	p.ResultRouters = p.ResultRouters[:0]

	for _, n := range nodes {
		proc := Processor{
			Node: n,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:    joinCore,
				Post:    post,
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: joinStageID,
			},
		}
		pIdx := p.AddProcessor(proc)

		for side, distinctProc := range distinctProcs[n] {
			p.Streams = append(p.Streams, Stream{
				SourceProcessor:  distinctProc,
				SourceRouterSlot: 0,
				DestProcessor:    pIdx,
				DestInput:        side,
			})
		}

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// EnsureSingleStreamPerNode goes over the ResultRouters and merges any group of
// routers that are on the same node, using a no-op processor.
// forceSerialization determines whether the streams are forced to be serialized
// (i.e. whether we don't want any parallelism).
//
// TODO(radu): a no-op processor is not ideal if the next processor is on the
// same node. A fix for that is much more complicated, requiring remembering
// extra state in the PhysicalPlan.
func (p *PhysicalPlan) EnsureSingleStreamPerNode(
	forceSerialization bool, push bool, gatewayNodeID roachpb.NodeID,
) {
	// Fast path - check if we need to do anything.
	var nodes util.FastIntSet
	var foundDuplicates bool
	for _, pIdx := range p.ResultRouters {
		proc := &p.Processors[pIdx]
		if nodes.Contains(int(proc.Node)) {
			foundDuplicates = true
			break
		}
		nodes.Add(int(proc.Node))
	}
	if !foundDuplicates {
		return
	}
	streams := make([]ProcessorIdx, 0, 2)

	for i := 0; i < len(p.ResultRouters); i++ {
		pIdx := p.ResultRouters[i]
		node := p.Processors[p.ResultRouters[i]].Node
		streams = append(streams[:0], pIdx)
		// Find all streams on the same node.
		for j := i + 1; j < len(p.ResultRouters); {
			if p.Processors[p.ResultRouters[j]].Node == node {
				streams = append(streams, p.ResultRouters[j])
				// Remove the stream.
				copy(p.ResultRouters[j:], p.ResultRouters[j+1:])
				p.ResultRouters = p.ResultRouters[:len(p.ResultRouters)-1]
			} else {
				j++
			}
		}
		if len(streams) == 1 {
			// Nothing to do for this node.
			continue
		}

		if push {
			proc := Processor{
				Node: node,
				Spec: execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{
						// The other fields will be filled in by MergeResultStreams.
						ColumnTypes: p.ResultTypes,
					}},
					Core:   execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
					Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
					Engine: execinfrapb.ProcessorSpec_TimeSeries,
				},
			}
			mergedProcIdx := p.AddProcessor(proc)
			p.MergeResultStreams(streams, 0 /* sourceRouterSlot */, p.MergeOrdering, mergedProcIdx, 0 /* destInput */, forceSerialization)
			p.ResultRouters[i] = mergedProcIdx
		} else {
			// Merge the streams into a no-op processor.
			proc := Processor{
				Node: node,
				Spec: execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{
						// The other fields will be filled in by MergeResultStreams.
						ColumnTypes: p.ResultTypes,
					}},
					Core:   execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{OutputTypes: p.ResultTypes}},
					Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				},
			}
			mergedProcIdx := p.AddProcessor(proc)
			p.MergeResultStreams(streams, 0 /* sourceRouterSlot */, p.MergeOrdering, mergedProcIdx, 0 /* destInput */, forceSerialization)
			p.ResultRouters[i] = mergedProcIdx
		}

	}
}

// ChildIsExecInTSEngine get last can push ts engine
func (p *PhysicalPlan) ChildIsExecInTSEngine() bool {
	return p.Processors[p.ResultRouters[0]].ExecInTSEngine()
}

// SelfCanExecInTSEngine get can exec in ts engine
func (p *PhysicalPlan) SelfCanExecInTSEngine(selfProcessorCanExecInTSEngine bool) bool {
	// self can exec && child can exec
	return selfProcessorCanExecInTSEngine && p.ChildIsExecInTSEngine()
}

// ChildIsTSSortProcessor get last is ts sort
func (p *PhysicalPlan) ChildIsTSSortProcessor() bool {
	return p.Processors[p.ResultRouters[0]].ExecInTSEngine() && p.Processors[p.ResultRouters[0]].Spec.Core.Sorter != nil
}

// SetTSEngineReturnEncode set plan return encode
func (p *PhysicalPlan) SetTSEngineReturnEncode() {
	// check last is exec on ts engine , so all is exec on ts engine
	// all processors execute on ts engine, so return pg encode to client
	p.AllProcessorsExecInTSEngine = true
	for _, idx := range p.ResultRouters {
		if !p.Processors[idx].ExecInTSEngine() {
			p.AllProcessorsExecInTSEngine = false
			break
		}
	}
}

// AddTSOutputType add output types for ts processor
func (p *PhysicalPlan) AddTSOutputType(force bool) {
	for _, idx := range p.ResultRouters {
		if force || p.Processors[idx].ExecInTSEngine() {
			p.Processors[idx].Spec.Post.OutputTypes = p.ResultTypes
		}
	}
}

// extract the number after @; return 0 and false if not found.
func extractNumberAfterAt(s string) (int, bool) {
	re := regexp.MustCompile(`@(\d+)`)
	match := re.FindStringSubmatch(s)
	if len(match) < 2 {
		return 0, false
	}
	num, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return num, true
}

// replaceAtNumbers replaces the number after @ with a new number from the params array
// The number after @ is used as the index to get the new number
// Example: @1 will be replaced by @params[1]
func replaceAtNumbers(s string, params []uint32) (string, bool) {
	// Match pattern @ + digits
	re := regexp.MustCompile(`@(\d+)`)
	replaced := false

	result := re.ReplaceAllStringFunc(s, func(match string) string {
		replaced = true
		// Extract the number string after @
		numStr := match[1:]
		// Convert string to integer index
		index, err := strconv.Atoi(numStr)
		if err != nil {
			return match // return original if invalid
		}
		// Check if index is valid in params array
		if index >= 0 && index < len(params) {
			// Keep @, replace the number only
			return fmt.Sprintf("@%d", params[index-1])
		}
		// If index out of range, keep original
		return match
	})

	return result, replaced
}

func getTableOutPutInfoFromPost(
	scanPost *execinfrapb.PostProcessSpec, constValues []int64,
) []execinfrapb.TSStatisticReaderSpec_ParamInfo {
	scanOutput := make([]execinfrapb.TSStatisticReaderSpec_ParamInfo, 0)
	if len(scanPost.RenderExprs) != 0 {
		for i, v := range scanPost.RenderExprs {
			if v.String()[0] != '@' { // exprs other than columns
				constVal := constValues[i]
				if strings.Contains(strings.ToLower(v.String()), "function") {
					newFuncValue, _ := replaceAtNumbers(v.String(), scanPost.OutputColumns)
					scanOutput = append(scanOutput, execinfrapb.TSStatisticReaderSpec_ParamInfo{
						Typ:       execinfrapb.TSStatisticReaderSpec_ParamInfo_function,
						Value:     constVal,
						FuncValue: newFuncValue,
					})
				} else {

					scanOutput = append(scanOutput, execinfrapb.TSStatisticReaderSpec_ParamInfo{
						Typ:   execinfrapb.TSStatisticReaderSpec_ParamInfo_const,
						Value: constVal,
					})
				}
			} else {
				str := strings.Replace(scanPost.RenderExprs[i].String(), "@", "", -1)
				val, err := strconv.Atoi(str)
				if err != nil {
					val = 0
				}
				scanOutput = append(scanOutput, execinfrapb.TSStatisticReaderSpec_ParamInfo{
					Typ:   execinfrapb.TSStatisticReaderSpec_ParamInfo_colID,
					Value: int64(scanPost.OutputColumns[val-1]),
				})
			}
		}
	} else {
		scanOutput = make([]execinfrapb.TSStatisticReaderSpec_ParamInfo, len(scanPost.OutputColumns))
		for i, v := range scanPost.OutputColumns {
			scanOutput[i].Typ = execinfrapb.TSStatisticReaderSpec_ParamInfo_colID
			scanOutput[i].Value = int64(v)
		}
	}

	return scanOutput
}

func checkRepeat(
	mapAgg *map[execinfrapb.AggregatorSpec_Func]AggKey,
	params []uint32,
	constArguments []int64,
	typ execinfrapb.AggregatorSpec_Func,
) bool {
	if v, ok := (*mapAgg)[typ]; ok && len(v.Columns) == len(params) && len(v.Constants) == len(constArguments) {
		allSame := true
		for i, idx := range params {
			if idx != v.Columns[i] {
				allSame = false
				break
			}
		}
		for i, constArg := range constArguments {
			if constArg != v.Constants[i] {
				allSame = false
				break
			}
		}
		if allSame {
			return true
		}
	}
	(*mapAgg)[typ] = AggKey{Columns: params, Constants: constArguments}
	return false
}

// AggKey is used to represent column and constant parameters for each aggregate function
type AggKey struct {
	Columns   []uint32
	Constants []int64
}

// PushAggToStatisticReader push Agg to statistic reader
func (p *PhysicalPlan) PushAggToStatisticReader(
	exprCtx ExprContext,
	idx ProcessorIdx,
	aggSpecs *execinfrapb.AggregatorSpec,
	tsPostSpec *execinfrapb.PostProcessSpec,
	aggResTypes []types.T,
	constValues []int64,
	scalar bool,
) error {
	if p.Processors[idx].Spec.Core.TsStatisticReader != nil {
		tr := p.Processors[idx].Spec.Core.TsStatisticReader
		tr.Scalar = scalar
		scanPost := &p.Processors[idx].Spec.Post
		//get render col index and const value
		scanOutPut := getTableOutPutInfoFromPost(scanPost, constValues)

		scanCols := make([]execinfrapb.TSStatisticReaderSpec_Params, 0)
		scanAgg := make([]int32, 0)
		sumMap := make(map[uint32]int)
		countMap := make(map[uint32]int)
		// pre render column types
		inputColTypeArray := make([]*types.T, 0)
		colIndex := 0

		// renderAggColsMap records the correspondence between
		// the index of the original agg
		// and
		// the index of the columns output by the statistic reader.
		//
		// key: The index of the original agg
		//
		// val: The index of the columns output by the statistic reader.
		// 			This index is needed when building the projection layer,
		//      where avg has two members and other functions have one member.
		renderAggColsMap := make(map[int][]int)

		var addMap = func(key uint32, value int, destMap map[uint32]int) {
			if _, ok := destMap[key]; !ok {
				destMap[key] = value
			}
		}

		var addRenderAggColsMap = func(key int, value int, targetMap map[int][]int) {
			if _, ok := targetMap[key]; ok {
				targetMap[key] = append(targetMap[key], value)
			} else {
				targetMap[key] = []int{value}
			}
		}

		aggMap := make(map[execinfrapb.AggregatorSpec_Func]AggKey)
		addStatScan := func(
			params []uint32, constArguments []int64, typ execinfrapb.AggregatorSpec_Func, colType *types.T, originAggIdx int,
		) error {
			// prune repeat agg
			if checkRepeat(&aggMap, params, constArguments, typ) {
				if typ == execinfrapb.AggregatorSpec_SUM {
					addRenderAggColsMap(originAggIdx, sumMap[params[0]], renderAggColsMap)
				} else if typ == execinfrapb.AggregatorSpec_COUNT {
					addRenderAggColsMap(originAggIdx, countMap[params[0]], renderAggColsMap)
				}
				return nil
			}

			infos := make([]execinfrapb.TSStatisticReaderSpec_ParamInfo, len(params)+len(constArguments))
			for j, v := range params {
				if v >= uint32(len(scanOutPut)) {
					return pgerror.Newf(pgcode.Internal, "statistic table could not find col")
				}
				infos[j] = scanOutPut[v]
			}

			for j, v := range constArguments {
				infos[len(params)+j] = execinfrapb.TSStatisticReaderSpec_ParamInfo{
					Typ:   execinfrapb.TSStatisticReaderSpec_ParamInfo_const,
					Value: v,
				}
			}

			// count_rows or count(1), convert to count(ts)
			if (len(params) == 0 && typ == execinfrapb.AggregatorSpec_COUNT_ROWS) ||
				(typ == execinfrapb.AggregatorSpec_COUNT && infos[0].Typ == execinfrapb.TSStatisticReaderSpec_ParamInfo_const) {
				scanCols = append(scanCols, execinfrapb.TSStatisticReaderSpec_Params{
					Param: []execinfrapb.TSStatisticReaderSpec_ParamInfo{
						{
							Typ:   execinfrapb.TSStatisticReaderSpec_ParamInfo_colID,
							Value: 0,
						},
					}})
				scanAgg = append(scanAgg, int32(execinfrapb.AggregatorSpec_COUNT))
			} else {
				scanCols = append(scanCols, execinfrapb.TSStatisticReaderSpec_Params{Param: infos})
				scanAgg = append(scanAgg, int32(typ))
			}

			if colType != nil {
				inputColTypeArray = append(inputColTypeArray, colType)
			} else {
				if typ == execinfrapb.AggregatorSpec_SUM {
					inputColTypeArray = append(inputColTypeArray, types.Float)
				} else if typ == execinfrapb.AggregatorSpec_COUNT {
					inputColTypeArray = append(inputColTypeArray, types.Int4)
				}
			}

			if typ == execinfrapb.AggregatorSpec_SUM {
				addMap(params[0], colIndex, sumMap)
			} else if typ == execinfrapb.AggregatorSpec_COUNT {
				addMap(params[0], colIndex, countMap)
			}
			addRenderAggColsMap(originAggIdx, colIndex, renderAggColsMap)
			colIndex++
			return nil
		}

		for i, agg := range aggSpecs.Aggregations {
			if agg.Func == execinfrapb.AggregatorSpec_AVG {
				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, execinfrapb.AggregatorSpec_SUM, nil, i); err != nil {
					return err
				}

				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, execinfrapb.AggregatorSpec_COUNT, nil, i); err != nil {
					return err
				}
			} else {
				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, agg.Func, &aggResTypes[i], i); err != nil {
					return err
				}
			}
		}

		tr.AggTypes = scanAgg
		tr.ParamIdx = scanCols

		scanPost.RenderExprs = make([]execinfrapb.Expression, 0)

		addRender := func(val int) error {
			varIdxs := make([]int, 1)
			varIdxs[0] = val
			render := tree.NewOrdinalReference(0)
			expr, err2 := MakeTSExpression(render, exprCtx, varIdxs)
			if err2 != nil {
				return err2
			}
			scanPost.RenderExprs = append(scanPost.RenderExprs, expr)
			return nil
		}

		for i, agg := range aggSpecs.Aggregations {
			switch agg.Func {
			case execinfrapb.AggregatorSpec_AVG:
				varIdxs := make([]int, 2)
				v := renderAggColsMap[i][0]
				varIdxs[0] = v
				v1 := renderAggColsMap[i][1]
				varIdxs[1] = v1

				// create dev render for avg
				h := tree.MakeTypesOnlyIndexedVarHelper(inputColTypeArray)
				render, err2 := GetAvgRender(&h, varIdxs)
				if err2 != nil {
					return err2
				}
				expr, err2 := MakeTSExpression(render, exprCtx, nil)
				if err2 != nil {
					return err2
				}
				scanPost.RenderExprs = append(scanPost.RenderExprs, expr)
			default:
				v := renderAggColsMap[i][0]
				if err := addRender(v); err != nil {
					return err
				}
			}
		}

		// update reader post renders and outputTypes for local agg
		p.Processors[idx].Spec.Post.OutputTypes = tsPostSpec.OutputTypes
		p.Processors[idx].Spec.Post.OutputColumns = nil
		p.Processors[idx].Spec.Post.Projection = false
		p.ResultTypes = aggResTypes
	} else {
		return pgerror.New(pgcode.Internal, " statistic table could not find sum col")
	}

	return nil
}

// PushAggToTableReader push Agg to table reader
func (p *PhysicalPlan) PushAggToTableReader(
	idx ProcessorIdx,
	localAggsSpec *execinfrapb.AggregatorSpec,
	tsPost *execinfrapb.PostProcessSpec,
	pruneLocalAgg bool,
) {
	if p.Processors[idx].Spec.Core.TsTableReader != nil {
		r := p.Processors[idx].Spec.Core.TsTableReader
		r.Aggregator = localAggsSpec
		r.OrderedScan = pruneLocalAgg
		r.AggregatorPost = tsPost
		p.Processors[idx].Spec.Post.OutputTypes = tsPost.OutputTypes
	} else {
		str := fmt.Sprintf("PushAggToTableReader table reader does not exist, sub is %v", p.Processors[idx].Spec.Core)
		panic(str)
	}
}

// ExecInTSEngine returns true for execute in time series
func (p *Processor) ExecInTSEngine() bool {
	return p.Spec.ExecInTSEngine()
}

// GetRealRightRouters get real router when there has ts summary processor in the dist case.
// ex1:
//
//	node1			 node2     	      node3
//
// TSAggregator    TSAggregator     TSAggregator
//
//	|				|			    |
//
// TSSynchronizer  TSSynchronizer   TSSynchronizer
//
//	| 			    |				|
//
// TSAggregator─────────┘───────────────┘
// in this case, the rightRouters has only one, but we need to add BatchLookUpJoin for node1 and node2 and node3.
// ex2:
//
//		node1			 node2     	      node3
//				     TSAggregator     TSAggregator
//			 				|			    |
//	             TSSynchronizer   TSSynchronizer
//			  			    |				|
//
// TSAggregator─────────┘───────────────┘
// in this case, we add BatchLookUpJoin only for the nodes with ts data.
func (p *PhysicalPlan) GetRealRightRouters(
	nodes map[roachpb.NodeID]struct{}, rightRouters []ProcessorIdx, thisNodeID roachpb.NodeID,
) ([]ProcessorIdx, ProcessorIdx, bool) {
	if len(nodes) > 1 && len(rightRouters) == 1 {
		ps := []ProcessorIdx{}
		for _, v := range p.Streams {
			if v.DestProcessor == rightRouters[0] {
				if _, ok := nodes[p.Processors[v.SourceProcessor].Node]; ok {
					if p.Processors[v.SourceProcessor].Node != thisNodeID {
						ps = append(ps, v.SourceProcessor)
					}
				}
			}
		}
		ps = append(ps, rightRouters[0])
		return ps, rightRouters[0], true
	}
	return rightRouters, -1, false
}
