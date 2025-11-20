// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
)

// planAndRunCreateStream builds and runs the plan of stream.
func (dsp *DistSQLPlanner) planAndRunCreateStream(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	job *jobs.Job,
	finishedSetupFn func(),
) error {
	ctx = logtags.AddTag(ctx, "stream-physical-plan-builder", nil)
	streamDetails := job.Details().(jobspb.StreamDetails)

	parseJSON, err := json.ParseJSON(streamDetails.StreamMetadata.Parameters)
	if err != nil {
		return err
	}

	streamPara, err := sqlutil.UnmarshalStreamParameters(parseJSON)
	if err != nil {
		return err
	}

	stmt, err := parser.ParseOne(streamPara.StreamSink.SQL)
	if err != nil {
		return err
	}
	localPlanner := planCtx.planner
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.inStream = true

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(ctx)

	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	isLocal := err != nil || rec == cannotDistribute
	if len(localPlanner.curPlan.subqueryPlans) != 0 {
		return pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the stream filter")
	}
	evalCtxForStream := localPlanner.ExtendedEvalContext()
	evalCtxForStream.GroupWindow = &tree.GroupWindow{
		GroupWindowFunc: tree.GroupWindowUnknown,
	}
	evalCtxForStream.StartDistributeMode = false

	planCtxForStream := localPlanner.DistSQLPlanner().NewPlanningCtx(ctx, evalCtxForStream, nil)

	planCtxForStream.isLocal = isLocal
	planCtxForStream.isStream = true
	planCtxForStream.cdcCtx = &CDCContext{}
	planCtxForStream.planner = localPlanner
	planCtxForStream.stmtType = tree.Rows
	planCtxForStream.streamSpec = planCtx.streamSpec

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtxForStream, localPlanner.curPlan.plan)
	if err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, &physPlan)

	colTypes := sqlbase.ColTypeInfoFromColTypes(physPlan.ResultTypes)
	rowContainer := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), colTypes, streamInsertBatch)
	streamResultWriter := NewStreamResultWriter(
		ctx, streamDetails.StreamMetadata, &streamPara, physPlan.ResultTypes, streamDetails.TargetTableColTypes,
		rowContainer, evalCtx.ExecCfg, dsp.stopper,
	)
	defer func() {
		streamResultWriter.Close()
	}()

	recv := MakeDistSQLReceiver(
		ctx,
		streamResultWriter,
		stmt.AST.StatementType(),
		localPlanner.execCfg.RangeDescriptorCache,
		localPlanner.execCfg.LeaseHolderCache,
		txn,
		func(ts hlc.Timestamp) {
			localPlanner.execCfg.Clock.Update(ts)
		},
		localPlanner.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, finishedSetupFn)()

	return streamResultWriter.Err()
}

// createPlanForStream create a plan for stream.
func createPlanForStream(
	ctx context.Context, params runParams, query string, metadata *cdcpb.StreamMetadata, hasAgg bool,
) (PhysicalPlan, error) {
	var p PhysicalPlan
	// make a new local planner
	plan, cleanup := newInternalPlanner("pipe-filter-builder", params.p.txn, params.p.User(),
		&MemoryMetrics{}, params.p.execCfg)
	defer cleanup()

	dsp := plan.DistSQLPlanner()
	evalCtx := plan.ExtendedEvalContext()

	var noTxn *kv.Txn
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
	planCtx.planner = plan

	stmt, err := parser.ParseOne(query)
	if err != nil {
		return p, err
	}
	localPlanner := planCtx.planner
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.inStream = true

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(ctx)
	})
	if err != nil {
		return p, err
	}
	defer localPlanner.curPlan.close(ctx)
	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	isLocal := err != nil || rec == cannotDistribute
	if len(localPlanner.curPlan.subqueryPlans) != 0 {
		return p, pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the stream filter")
	}
	evalCtxForStream := localPlanner.ExtendedEvalContext()
	evalCtxForStream.StartDistributeMode = false

	planCtxForStream := localPlanner.DistSQLPlanner().NewPlanningCtx(ctx, evalCtxForStream, nil)
	planCtxForStream.isLocal = isLocal
	planCtxForStream.isStream = true
	planCtxForStream.cdcCtx = &CDCContext{}
	planCtxForStream.planner = localPlanner
	planCtxForStream.stmtType = tree.Rows
	planCtxForStream.streamSpec = &execinfrapb.StreamReaderSpec{
		Metadata: metadata,
		JobID:    0,
	}

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtxForStream, localPlanner.curPlan.plan)
	if err != nil {
		return p, err
	}

	if hasAgg {
		if len(physPlan.Processors) != 2 {
			return p, errors.Errorf("unsupported stream query: %q", query)
		}
	} else {
		if len(physPlan.Processors) != 1 {
			return p, errors.Errorf("unsupported stream query: %q", query)
		}
	}

	return physPlan, nil
}

// buildPhyPlanForStreamReaders createStreamReaders generates a plan consisting of stream reader processors,
// one for each node that has spans that we are reading.
// overridesResultColumns is optional.
func (p *PhysicalPlan) buildPhyPlanForStreamReaders(
	planCtx *PlanningCtx,
	n *tsScanNode,
	nodeID roachpb.NodeID,
	tsColMap map[sqlbase.ColumnID]tsColIndex,
	resCols []int,
	typs []types.T,
	descColumnIDs []sqlbase.ColumnID,
) error {
	err := p.initPhyPlanForStreamReader(planCtx, tsColMap, n.resultColumns, nodeID, resCols)
	if err != nil {
		return err
	}

	post, err1 := initPostSpecForStreamReader(planCtx, n, resCols)
	if err1 != nil {
		return err1
	}

	outCols, planToStreamColMap, outTypes := buildPostSpecForStreamReaders(n, typs, descColumnIDs)
	p.SetLastStageStreamPost(post, outTypes)

	p.AddTSProjection(outCols)
	p.PlanToStreamColMap = planToStreamColMap

	return nil
}

// initPhyPlanForStreamReader inits StreamReaderSpec
func (p *PhysicalPlan) initPhyPlanForStreamReader(
	planCtx *PlanningCtx,
	tsColMap map[sqlbase.ColumnID]tsColIndex,
	resultColumns sqlbase.ResultColumns,
	nodeID roachpb.NodeID,
	resCols []int,
) error {
	p.ResultRouters = make([]physicalplan.ProcessorIdx, 1)
	p.Processors = make([]physicalplan.Processor, 0, 1)

	outColIDs := make([]uint32, len(resCols))
	outColIndexes := make([]uint32, len(resCols))
	outTypes := make([]types.T, len(resCols))
	outColNames := make([]string, len(resCols))
	var primaryTagIDs []uint32
	var needNormalTags bool

	for idx, resColIndex := range resCols {
		resultCol := resultColumns[idx]
		outColIndexes[idx] = uint32(resColIndex)
		outTypes[idx] = *resultCol.Typ
		outColNames[idx] = resultCol.Name

		outColIDs[idx] = uint32(resultCol.PGAttributeNum)
		col := tsColMap[resultCol.PGAttributeNum]
		if col.colType == sqlbase.ColumnType_TYPE_PTAG {
			primaryTagIDs = append(primaryTagIDs, uint32(idx))
		}
		if !needNormalTags && col.colType == sqlbase.ColumnType_TYPE_TAG {
			needNormalTags = true
		}
	}

	cdcColumns := &cdcpb.CDCColumns{
		CDCColumnIDs:     outColIDs,
		CDCColumnIndexes: outColIndexes,
		CDCTypes:         outTypes,
		CDCColumnNames:   outColNames,
		NeedNormalTags:   &needNormalTags,
	}

	streamReader := execinfrapb.StreamReaderSpec{
		Metadata:   planCtx.streamSpec.Metadata,
		JobID:      planCtx.streamSpec.JobID,
		CDCColumns: cdcColumns,

		OrderingColumnIDs: primaryTagIDs,
		TargetColTypes:    planCtx.streamSpec.TargetColTypes,
	}

	proc := physicalplan.Processor{
		Node: nodeID,
		Spec: execinfrapb.ProcessorSpec{
			Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
		},
	}

	var reader = new(execinfrapb.StreamReaderSpec)
	*reader = streamReader
	proc.Spec.Core.StreamReader = reader

	pIdx := p.AddProcessor(proc)
	p.ResultRouters[0] = pIdx
	p.TsOperator = execinfrapb.OperatorType_TsSelect

	return nil
}

// initPostSpecForStreamReader inits TSPostProcessSpec for StreamReader
func initPostSpecForStreamReader(
	planCtx *PlanningCtx, n *tsScanNode, resCols []int,
) (execinfrapb.PostProcessSpec, error) {
	filter, err := physicalplan.MakeTSExpression(n.filter, planCtx, resCols)
	if err != nil {
		return execinfrapb.PostProcessSpec{}, err
	}

	post := execinfrapb.PostProcessSpec{
		Filter: filter,
	}

	return post, nil
}

// buildPostSpecForStreamReaders builds TSPostProcessSpec for StreamReader
func buildPostSpecForStreamReaders(
	n *tsScanNode, typs []types.T, descColumnIDs []sqlbase.ColumnID,
) ([]uint32, []int, []types.T) {
	var outCols []uint32
	var planToStreamColMap []int

	n.ScanSource.ForEach(func(i int) {
		outCols = append(outCols, uint32(i-1))
	})

	planToStreamColMap = getPlanToStreamColMapForReader(outCols, n.resultColumns, descColumnIDs)
	return outCols, planToStreamColMap, typs
}

// SetLastStageStreamPost changes the TSPostProcessSpec spec of the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) SetLastStageStreamPost(
	post execinfrapb.PostProcessSpec, outputTypes []types.T,
) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post = post
	}

	p.ResultTypes = outputTypes
}

// addStreamAggregators add aggregators for stream.
func (dsp *DistSQLPlanner) addStreamAggregators(
	planCtx *PlanningCtx, p *PhysicalPlan, n *groupNode,
) error {
	for _, function := range n.funcs {
		if function.funcName == optbuilder.Gapfill && n.reqOrdering == nil {
			return errors.Errorf("%s must use ordered", optbuilder.Gapfill)
		}
	}

	// get agg spec
	aggregations, aggregationsColumnTypes, canNotDist, err := getAggFuncAndType(p, planCtx, n.funcs, n.aggFuncs,
		p.PlanToStreamColMap, !(n.engine == tree.EngineTypeTimeseries) && len(p.ResultRouters) == 1)
	if err != nil {
		return err
	}

	// get agg col output type
	finalOutTypes, err1 := getFinalColumnType(p, aggregations, aggregationsColumnTypes)
	if err1 != nil {
		return err1
	}

	aggType := execinfrapb.AggregatorSpec_NON_SCALAR
	if n.isScalar {
		aggType = execinfrapb.AggregatorSpec_SCALAR
	}

	groupCols := getPhysicalGroupCols(p, n.groupCols)

	// We can have a local stage of distinct processors if all aggregation
	// functions are distinct.
	dsp.addDistinct(aggregations, p, n.plan)

	// Check if the previous stage is all on one node.
	prevStageNode := getPreStageNodeID(p)

	// We either have a local stage on each stream followed by a final stage, or
	// just a final stage. We only use a local stage if:
	//  - the previous stage is distributed on multiple nodes, and
	//  - all aggregation functions support it, and
	//  - no function is performing distinct aggregation.
	multiStage := checkIsMultiState(prevStageNode, aggregations)

	var aggsSpec execinfrapb.AggregatorSpec
	var finalAggsSpec execinfrapb.StreamAggregatorSpec
	var finalAggsPost execinfrapb.PostProcessSpec

	if !multiStage {
		aggsSpec = execinfrapb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     aggregations,
			GroupCols:        groupCols,
			OrderedGroupCols: groupCols,
		}

		finalAggsSpec = execinfrapb.StreamAggregatorSpec{
			Metadata: planCtx.streamSpec.Metadata,
			AggSpec:  &aggsSpec,
		}

	} else {
		return errors.Errorf("TwiceAggregators is not supported by stream")
	}

	// Set up the final stage.
	// Update p.PlanToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator
	// has been programmed to produce the same columns as the groupNode.
	p.PlanToStreamColMap = identityMap(p.PlanToStreamColMap, len(aggregations))

	// notNeedDist is a special identifier used for Interpolate aggregate functions.
	// Interpolate agg should not dist.
	notNeedDist := false
	if canNotDist {
		notNeedDist = true
		// The interpolate function should be computed at the gateway node.
		prevStageNode = 0
		aggsSpec.HasTimeBucketGapFill = true
		aggsSpec.TimeBucketGapFillColId = n.gapFillColID
	}
	aggsSpec.GroupWindowId = n.groupWindowID
	aggsSpec.Group_WindowTscolid = n.groupWindowTSColID

	if n.groupWindowID >= 0 {
		aggsSpec.GroupWindowId = n.groupWindowID
		aggsSpec.Group_WindowTscolid = n.groupWindowTSColID
		aggsSpec.Group_WindowId = append(aggsSpec.Group_WindowId, n.groupWindowExtend...)
	}
	if n.optType.WithSumInt() {
		// the flag is used to make the sum_int return 0.
		aggsSpec.ScalarGroupByWithSumInt = true
	}
	if len(aggsSpec.GroupCols) == 0 || len(p.ResultRouters) == 1 || notNeedDist {
		// TODO update this comments
		// No GROUP BY, or we have a single stream. Use a single final aggregator.
		// If the previous stage was all on a single node, put the final
		// aggregator there. Otherwise, bring the results back on this node.
		dsp.addStreamSingleGroupState(p, prevStageNode, finalAggsSpec, finalAggsPost, finalOutTypes)
	} else {
		return errors.Errorf("stream is not running in distributed mode")
	}

	return nil
}

// addSingleGroupState add single group state for stream aggregation
func (dsp *DistSQLPlanner) addStreamSingleGroupState(
	p *PhysicalPlan,
	prevStageNode roachpb.NodeID,
	finalAggsSpec execinfrapb.StreamAggregatorSpec,
	finalAggsPost execinfrapb.PostProcessSpec,
	finalOutTypes []types.T,
) {
	node := dsp.nodeDesc.NodeID
	if prevStageNode != 0 {
		node = prevStageNode
	}
	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{StreamAggregator: &finalAggsSpec},
		finalAggsPost,
		finalOutTypes,
	)
}
