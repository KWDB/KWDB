// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package physicalplan

import (
	"context"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func generatePipelineFlows(
	gateway roachpb.NodeID, p *PhysicalPlan, flowID execinfrapb.FlowID, gossip *gossip.Gossip,
) (map[roachpb.NodeID]*execinfrapb.FlowSpec, bool) {
	flows := make(map[roachpb.NodeID]*execinfrapb.FlowSpec, 1)

	// traversed
	// Find if there are redundant noop, and when haveRedundantNoop is true,
	// the remote noop will no longer be pushed down to AE.
	traversed := make(map[roachpb.NodeID]bool)
	haveRedundantNoop := false
	// store remote Redundant noop index.
	indexRedundantNoop := make(map[int]bool, 0)
	for i := len(p.Processors) - 1; i >= 0; i-- {
		pro := p.Processors[i]
		if pro.Node == gateway {
			continue
		}
		if _, exists := traversed[pro.Node]; exists {
			continue
		}
		traversed[pro.Node] = true
		if pro.Spec.Core.Noop != nil {
			if pro.Spec.Post.Limit == 0 && pro.Spec.Post.Filter.Empty() && len(pro.Spec.Post.RenderExprs) == 0 {
				haveRedundantNoop = true
				indexRedundantNoop[i] = true
			}
		}
	}

	maxNodeID := 0
	nextProcessorID := len(p.Processors)

	for i, proc := range p.Processors {
		flowSpec, ok := flows[proc.Node]
		if !ok {
			flowSpec = NewFlowSpec(flowID, gateway)
			flows[proc.Node] = flowSpec
			// update maximum node
			if maxNodeID < int(proc.Node) {
				maxNodeID = int(proc.Node)
			}
		}
		switch {
		case proc.ExecInTSEngine:
			flowSpec.TsProcessors = append(flowSpec.TsProcessors, proc.TSSpec)

		case haveRedundantNoop && indexRedundantNoop[i]:
			flowSpec.Processors = append(flowSpec.Processors, proc.Spec)

		default:
			tsSpec, ok := convertSpec(proc.Spec, p.Commandlimit)
			if !ok {
				return nil, false
			}
			flowSpec.TsProcessors = append(flowSpec.TsProcessors, tsSpec)
		}
	}

	gateFlow := flows[gateway]
	if haveRedundantNoop {
		optimizeRemoteNoops(gateway, flows)
	} else {
		nextProcessorID = addRemoteNoops(gateway, p, flows, nextProcessorID)
	}

	addNoop(gateFlow, p, len(p.Streams), true, nextProcessorID)
	nextProcessorID++

	// connect the gateway and remote.
	connectStreams(gateway, p, flows, gateFlow)

	// collect node's BRPCAddress.
	brpcAddrs, err := collectBRPCAddresses(flows, gossip, maxNodeID)
	if err != nil {
		return nil, false
	}

	queryID := generateQueryID(uint16(gateway))
	setTsInfo(flows, brpcAddrs, queryID, p, gateFlow)
	return flows, true
}

func setTsInfo(
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	brpcAddrs []string,
	queryID int64,
	p *PhysicalPlan,
	gateFlow *execinfrapb.FlowSpec,
) {
	// set TsInfo for every flow.
	for _, flow := range flows {
		flow.TsInfo.BrpcAddrs = brpcAddrs
		flow.TsInfo.UseAeGather = true
		flow.TsInfo.QueryID = queryID
	}
	if p.UseQueryShortCircuit == UseQueryShortCircuitForDistributedPre {
		// pipe model can exec query short circuit in gateway node.
		gateFlow.TsInfo.UseQueryShortCircuit = true
	}
}

func collectBRPCAddresses(
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec, gossip *gossip.Gossip, maxNodeID int,
) ([]string, error) {
	brpcAddrs := make([]string, maxNodeID)
	for nodeID := range flows {
		node, err := gossip.GetNodeDescriptor(nodeID)
		if err != nil {
			return nil, err
		}
		if int(nodeID) <= len(brpcAddrs) && nodeID > 0 {
			brpcAddrs[nodeID-1] = node.BrpcAddress.String()
		}
	}
	return brpcAddrs, nil
}

func connectStreams(
	gateway roachpb.NodeID,
	p *PhysicalPlan,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	gateFlow *execinfrapb.FlowSpec,
) {
	// Connect the NOOP of the gateway to the NOOP of other nodes.
	for nodeID, f := range flows {
		if nodeID == gateway {
			continue
		}
		s := execinfrapb.StreamEndpointSpec{
			Type:         execinfrapb.StreamEndpointType_REMOTE,
			StreamID:     execinfrapb.StreamID(len(p.Streams)),
			TargetNodeID: gateway,
		}
		p.Streams = append(p.Streams, Stream{})
		gateFlow.Processors[0].Input[0].Streams = append(gateFlow.Processors[0].Input[0].Streams, s)
		f.Processors[0].Output[0].Streams = append(f.Processors[0].Output[0].Streams, s)
	}
}

func addRemoteNoops(
	gateway roachpb.NodeID,
	p *PhysicalPlan,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	processID int,
) int {
	// remote node add relational noop.
	for nodeID, f := range flows {
		if nodeID == gateway {
			continue
		}
		// add noop for top flow.
		sid := len(p.Streams)
		addNoop(f, p, sid, false, processID)
		processID++
	}
	return processID
}

// optimizeRemoteNoops remove redundancy ae noop.
func optimizeRemoteNoops(gateway roachpb.NodeID, flows map[roachpb.NodeID]*execinfrapb.FlowSpec) {
	// remote node reuse relational noop
	// move the noop's outputStream to the top ae process's outputStream.
	for nodeID, f := range flows {
		if nodeID == gateway || len(f.Processors) == 0 || len(f.TsProcessors) == 0 {
			continue
		}
		s := f.Processors[0].Output[0].Streams[0]
		finalTsPro := f.TsProcessors[len(f.TsProcessors)-1]
		finalTsPro.Output[0].Type = execinfrapb.OutputRouterSpec_BY_GATHER
		finalTsPro.Output[0].Streams = append(finalTsPro.Output[0].Streams, s)
		// empty out stream.
		f.Processors[0].Output[0].Streams = f.Processors[0].Output[0].Streams[0:0]
	}
}

const maxQueryID = 1<<47 - 1

// QueryIDForTS is query id for AE.
var QueryIDForTS = int64(0)

func generateQueryID(nodeID uint16) int64 {
	queryID := atomic.AddInt64(&QueryIDForTS, 1)

	// Reset when query ID exceeds maximum value.
	if queryID > maxQueryID {
		queryID = (queryID & maxQueryID) + 1

		atomic.CompareAndSwapInt64(&QueryIDForTS, atomic.LoadInt64(&QueryIDForTS), queryID)
	}

	// QueryID occupies the upper 48 bits, while nodeID occupies the lower 16 bits.
	return (queryID << 16) | int64(nodeID)
}

// addNoop add a relational noop and add one outStream to top tsProcessor.
func addNoop(flowSpec *execinfrapb.FlowSpec, p *PhysicalPlan, sid int, isGateWay bool, pID int) {
	proc := execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{
			Type:        execinfrapb.InputSyncSpec_UNORDERED,
			ColumnTypes: p.ResultTypes,
			Streams: []execinfrapb.StreamEndpointSpec{
				{Type: execinfrapb.StreamEndpointType_QUEUE, StreamID: execinfrapb.StreamID(sid)}},
		}},
		Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		Post: execinfrapb.PostProcessSpec{},
		Output: []execinfrapb.OutputRouterSpec{{
			Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
		}},
		StageID:     p.NewStageID(),
		ProcessorID: int32(pID),
	}
	p.Streams = append(p.Streams, Stream{})
	flowSpec.Processors = append(flowSpec.Processors, proc)
	s := execinfrapb.StreamEndpointSpec{
		Type:     execinfrapb.StreamEndpointType_QUEUE,
		StreamID: execinfrapb.StreamID(sid),
	}
	tsStream := &flowSpec.TsProcessors[len(flowSpec.TsProcessors)-1].Output[0].Streams
	if isGateWay {
		// delete top tsProcessor's outStream when gateway.
		*tsStream = (*tsStream)[1:]
		// add SYNC_RESPONSE for gateway relational noop.
		proc.Output[0].Streams = append(proc.Output[0].Streams, execinfrapb.StreamEndpointSpec{
			Type: execinfrapb.StreamEndpointType_SYNC_RESPONSE,
		})
	} else {
		flowSpec.TsProcessors[len(flowSpec.TsProcessors)-1].Output[0].Type = execinfrapb.OutputRouterSpec_BY_GATHER
	}
	flowSpec.TsProcessors[len(flowSpec.TsProcessors)-1].FinalTsProcessor = true
	*tsStream = append(*tsStream, s)
}

// convertSpec convert ProcessorSpec to TSProcessorSpec.
func convertSpec(
	spec execinfrapb.ProcessorSpec, commandlimit uint32,
) (execinfrapb.TSProcessorSpec, bool) {
	if len(spec.Post.OutputTypes) == 0 {
		log.Warningf(context.Background(), "core:%v, outputType is nil", spec.Core)
		return execinfrapb.TSProcessorSpec{}, false
	}
	if spec.Core.Aggregator != nil {
		if spec.Core.Aggregator.GroupWindowId > 0 || spec.Core.Aggregator.HasTimeBucketGapFill {
			return execinfrapb.TSProcessorSpec{}, false
		}
		tsCore := execinfrapb.TSProcessorCoreUnion{Aggregator: spec.Core.Aggregator}
		return newTsSpec(spec, commandlimit, tsCore), true
	}
	if spec.Core.Noop != nil {
		tsCore := execinfrapb.TSProcessorCoreUnion{Noop: &execinfrapb.TSNoopSpec{}}
		return newTsSpec(spec, commandlimit, tsCore), true
	}
	if spec.Core.Sorter != nil {
		tsCore := execinfrapb.TSProcessorCoreUnion{Sorter: spec.Core.Sorter}
		return newTsSpec(spec, commandlimit, tsCore), true
	}
	if spec.Core.Distinct != nil {
		tsCore := execinfrapb.TSProcessorCoreUnion{Distinct: spec.Core.Distinct}
		return newTsSpec(spec, commandlimit, tsCore), true
	}
	if spec.Core.Windower != nil {
		tsCore := execinfrapb.TSProcessorCoreUnion{Window: spec.Core.Windower}
		return newTsSpec(spec, commandlimit, tsCore), true
	}
	return execinfrapb.TSProcessorSpec{}, false
}

func newTsSpec(
	spec execinfrapb.ProcessorSpec, commandlimit uint32, tsCore execinfrapb.TSProcessorCoreUnion,
) execinfrapb.TSProcessorSpec {
	tsPost := convertPost(spec.Post, spec.Post.OutputTypes, commandlimit)
	tsSpec := execinfrapb.TSProcessorSpec{
		Input:       spec.Input,
		Core:        tsCore,
		Post:        tsPost,
		Output:      spec.Output,
		ProcessorID: spec.ProcessorID,
	}
	return tsSpec
}

// convertPost convert PostProcessSpec to TSPostProcessSpec.
func convertPost(
	post execinfrapb.PostProcessSpec, typs []types.T, commandlimit uint32,
) execinfrapb.TSPostProcessSpec {
	if len(typs) == 0 && (len(post.RenderExprs) > 0 || len(post.OutputColumns) > 0) {
		panic("outputTypes is nil")
	}
	var Renders []string
	for _, v := range post.RenderExprsTs {
		Renders = append(Renders, v.String())
	}
	outputTypes := make([]types.Family, 0)
	for _, typ := range typs {
		outputTypes = append(outputTypes, typ.InternalType.Family)
	}
	tsPost := execinfrapb.TSPostProcessSpec{
		Limit:         uint32(post.Limit),
		Offset:        uint32(post.Offset),
		Filter:        post.FilterTs.String(),
		Renders:       Renders,
		OutputColumns: post.OutputColumns,
		OutputTypes:   outputTypes,
		Projection:    post.Projection,
		Commandlimit:  commandlimit,
	}
	return tsPost
}
