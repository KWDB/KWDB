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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
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

func getTableOutPutInfoFromPost(
	scanPost *execinfrapb.PostProcessSpec, constValues []int64,
) []execinfrapb.TSStatisticReaderSpec_ParamInfo {
	scanOutput := make([]execinfrapb.TSStatisticReaderSpec_ParamInfo, 0)
	if len(scanPost.RenderExprs) != 0 {
		for i, v := range scanPost.RenderExprs {
			if v.String()[0] != '@' { //const value
				constVal := constValues[i]
				scanOutput = append(scanOutput, execinfrapb.TSStatisticReaderSpec_ParamInfo{
					Typ:   execinfrapb.TSStatisticReaderSpec_ParamInfo_const,
					Value: constVal,
				})
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

		var addMap = func(key uint32, value int, destMap map[uint32]int) {
			if _, ok := destMap[key]; !ok {
				destMap[key] = value
			}
		}
		aggMap := make(map[execinfrapb.AggregatorSpec_Func]AggKey)
		addStatScan := func(
			params []uint32, constArguments []int64, typ execinfrapb.AggregatorSpec_Func, colType *types.T,
		) error {
			// prune repeat agg
			if checkRepeat(&aggMap, params, constArguments, typ) {
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
			colIndex++
			return nil
		}

		for i, agg := range aggSpecs.Aggregations {
			if agg.Func == execinfrapb.AggregatorSpec_AVG {
				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, execinfrapb.AggregatorSpec_SUM, nil); err != nil {
					return err
				}

				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, execinfrapb.AggregatorSpec_COUNT, nil); err != nil {
					return err
				}
			} else {
				if err := addStatScan(agg.ColIdx, agg.TimestampConstant, agg.Func, &aggResTypes[i]); err != nil {
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

		// posIdx records the real position of each agg functions,
		// can not use i because avg will be split into sum and count
		posIdx := 0
		for i, agg := range aggSpecs.Aggregations {
			switch agg.Func {
			case execinfrapb.AggregatorSpec_AVG:
				varIdxs := make([]int, 2)
				v, ok := sumMap[agg.ColIdx[0]]
				if !ok {
					return pgerror.New(pgcode.Internal, fmt.Sprintf("statistic table could not find sum col %d",
						agg.ColIdx[0]))
				}
				varIdxs[0] = v
				v1, ok1 := countMap[agg.ColIdx[0]]
				if !ok1 {
					return pgerror.New(pgcode.Internal, fmt.Sprintf("statistic table could not find count col %d",
						agg.ColIdx[0]))
				}
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
				posIdx += 2
			case execinfrapb.AggregatorSpec_SUM:
				v, ok := sumMap[agg.ColIdx[0]]
				if !ok {
					v = i
				}
				if err := addRender(v); err != nil {
					return err
				}
				posIdx++
			case execinfrapb.AggregatorSpec_COUNT:
				v, ok := countMap[agg.ColIdx[0]]
				if !ok {
					v = i
				}
				if err := addRender(v); err != nil {
					return err
				}
				posIdx++
			default:
				if err := addRender(posIdx); err != nil {
					return err
				}
				posIdx++
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
