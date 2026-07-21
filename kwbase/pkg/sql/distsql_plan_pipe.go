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
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
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

// PlanAndRunCreatePipe plans and executes the creation of a CDC pipe
func (dsp *DistSQLPlanner) PlanAndRunCreatePipe(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	p *GenericPlanner,
	job *jobs.Job,
	resultRows *RowResultWriter,
	resultsCh chan<- tree.Datums,
) error {
	var noTxn *kv.Txn
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
	planCtx.planner = p

	ctx = logtags.AddTag(ctx, "create-watermark-resumer", nil)

	physPlan := dsp.createPlanForPipe(planCtx, job)

	dsp.FinalizePlan(planCtx, &physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		evalCtx.ExecCfg.LeaseHolderCache,
		noTxn,
		func(ts hlc.Timestamp) {
			evalCtx.ExecCfg.Clock.Update(ts)
		},
		evalCtx.Tracing,
	)
	defer recv.Release()

	finishedSetupFn := func() { resultsCh <- tree.Datums(nil) }

	dsp.Run(planCtx, noTxn, &physPlan, recv, evalCtx, finishedSetupFn)()
	return resultRows.Err()
}

// MarshalPipeFilter extracts the filter expressions of metrics and tags from the physical plan
// and marshals them to bytes. They will be applied during the data capture phase.
func MarshalPipeFilter(
	params RunParams, metadata *cdcpb.PipeMetadata, pipeTableInfo *cdcpb.CDCTableInfo,
) error {
	// make a new local planner
	plan, cleanup := newInternalPlanner("pipe-filter-builder", params.PlannerTxn(), params.p.User(),
		&MemoryMetrics{}, params.p.execCfg)
	defer cleanup()

	// The column order in the filter must be consistent with that in the payload
	pipeQuery := fmt.Sprintf("SELECT * FROM %s.%s ",
		pipeTableInfo.Database,
		pipeTableInfo.Table)

	if metadata.Filter != "" {
		pipeQuery += " WHERE " + metadata.Filter
	}

	stmt, err := parser.ParseOne(pipeQuery)
	if err != nil {
		return err
	}
	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.SessionData().Database = params.p.CurrentDatabase()
	localPlanner.SessionData().SearchPath = params.p.CurrentSearchPath()

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.RunWithOptions(ResolveFlags{SkipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(params.Ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(params.Ctx)
	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	isLocal := err != nil || rec == cannotDistribute
	if len(localPlanner.curPlan.subqueryPlans) != 0 {
		return pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the pipe filter")
	}
	evalCtx := localPlanner.ExtendedEvalContext()
	planCtx := localPlanner.DistSQLPlanner().NewPlanningCtx(params.Ctx, evalCtx, params.PlannerTxn())
	planCtx.isLocal = isLocal
	planCtx.cdcCtx = &CDCContext{}
	planCtx.planner = localPlanner
	planCtx.stmtType = tree.Rows

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtx, localPlanner.curPlan.plan)
	if err != nil {
		return err
	}

	localPlanner.DistSQLPlanner().FinalizePlan(planCtx, &physPlan)

	if len(physPlan.Processors) == 1 {
		if physPlan.Processors[0].Spec.Core.Values != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported, "pipe filter %q is invalid", metadata.Filter)
		}
	}

	if planCtx.cdcCtx.metricsFilter.Expr != "" {
		metadata.MetricsFilter, err = protoutil.Marshal(&planCtx.cdcCtx.metricsFilter)
		if err != nil {
			return err
		}
	}

	for _, tf := range planCtx.cdcCtx.tagFilter {
		filler, err := protoutil.Marshal(&tf)
		if err != nil {
			return err
		}
		metadata.TagFilter = append(metadata.TagFilter, filler)
	}
	return nil
}

// CheckWhereExprForCDC checks if the filter is supported by pipe. Currently, only row-based simple
// Only supports immutable functions.
func CheckWhereExprForCDC(
	ctx context.Context,
	p *GenericPlanner,
	tn tree.TableName,
	table sqlbase.TableDescriptor,
	expr tree.Expr,
) (hasNormalTag bool, err error) {
	var hasNormalTag1, hasNormalTag2 bool
	switch exp := expr.(type) {
	case *tree.AndExpr:
		if hasNormalTag1, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.OrExpr:
		if hasNormalTag1, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.ComparisonExpr:
		if hasNormalTag1, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.BinaryExpr:
		if hasNormalTag1, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Right); err != nil {
			return hasNormalTag, err
		}
	case *tree.RangeCond:
		if hasNormalTag1, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Left); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag2, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.From); err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.To); err != nil {
			return hasNormalTag, err
		}
	case *tree.UnresolvedName:
		v, err := exp.NormalizeVarName()
		if err != nil {
			return hasNormalTag, err
		}
		if hasNormalTag, err = CheckWhereExprForCDC(ctx, p, tn, table, v); err != nil {
			return hasNormalTag, err
		}
	case *tree.ColumnItem:
		col, dropped, err := table.FindColumnByName(exp.ColumnName)
		if err != nil || dropped {
			return hasNormalTag, sqlbase.NewUndefinedColumnError(string(exp.ColumnName))
		}
		if col.IsTagCol() && !col.IsPrimaryTagCol() {
			hasNormalTag = true
		}
	case tree.Constant, tree.DNullExtern, *tree.DBool, *tree.Tuple:
	case *tree.ParenExpr:
		if hasNormalTag, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Expr); err != nil {
			return hasNormalTag, err
		}
	case *tree.FuncExpr:
		for i := range exp.Exprs {
			if hasNormalTag, err = CheckWhereExprForCDC(ctx, p, tn, table, exp.Exprs[i]); err != nil {
				return hasNormalTag, err
			}
		}
		f := p.optPlanningCtx.optimizer.Factory()
		catalog := p.optPlanningCtx.catalog
		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &catalog, f, p.stmt.AST)
		tExpr := bld.BuildFuncForPipe(exp, table, tn)
		if fExpr, ok := tExpr.(*tree.FuncExpr); ok {
			fn := fExpr.ResolvedOverload()
			if fn.AggregateFunc != nil || fn.WindowFunc != nil || strings.Contains(exp.Func.FunctionReference.FunctionName(), "time_bucket") {
				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
			}
			switch fn.Volatility {
			case tree.VolatilityImmutable, tree.VolatilityLeakProof:
			default:
				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
			}
		} else {
			return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
		}
	default:
		return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "expr %q is not supported by pipe filter", expr.String())
	}

	return hasNormalTag || hasNormalTag1 || hasNormalTag2, nil
}
