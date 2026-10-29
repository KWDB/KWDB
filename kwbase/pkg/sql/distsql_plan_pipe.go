//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/logtags"
)

func (dsp *DistSQLPlanner) createPlanForPipe(planCtx *PlanningCtx, job *jobs.Job) PhysicalPlan {
	details := job.Details().(jobspb.PipeWatermarkDetails)

	var p PhysicalPlan
	stageID := p.NewStageID()
	p.ResultRouters = make([]physicalplan.ProcessorIdx, 1)
	p.Processors = make([]physicalplan.Processor, 0, 1)

	pipeSpec := &cdcpb.PipeWatermarkSpec{
		Metadata: *details.PipeMetadata,
		JobID:    *job.ID(),
	}
	proc := physicalplan.Processor{
		Node: planCtx.EvalContext().NodeID,
		Spec: execinfrapb.ProcessorSpec{
			Core:    execinfrapb.ProcessorCoreUnion{PipeWatermark: pipeSpec},
			Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
			StageID: stageID,
		},
	}
	pIdx := p.AddProcessor(proc)
	p.ResultRouters[0] = pIdx

	return p
}

func (dsp *DistSQLPlanner) planAndRunCreatePipe(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	job *jobs.Job,
	resultRows *RowResultWriter,
	resultsCh chan<- tree.Datums,
) error {
	ctx = logtags.AddTag(ctx, "create-watermark-resumer", nil)

	physPlan := dsp.createPlanForPipe(planCtx, job)

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

	finishedSetupFn := func() { resultsCh <- tree.Datums(nil) }

	dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, finishedSetupFn)()
	return resultRows.Err()
}
