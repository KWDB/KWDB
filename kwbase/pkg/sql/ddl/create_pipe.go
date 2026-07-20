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

package ddl

import (
	"context"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/pipe"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	md "gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var pipeOptionExpectValues = map[string]sqlconst.KVStringOptValidate{
	sqlconst.OptEnable:        sqlconst.KVStringOptRequireValue,
	sqlconst.OptSink:          sqlconst.KVStringOptRequireValue,
	sqlconst.OptMessageFormat: sqlconst.KVStringOptRequireValue,
	sqlconst.OptIgnoreHistory: sqlconst.KVStringOptRequireValue,
	sqlconst.OptBufferSize:    sqlconst.KVStringOptRequireValue,
	sqlconst.OptCheckTag:      sqlconst.KVStringOptRequireValue,
	sqlconst.OptPublish:       sqlconst.KVStringOptRequireValue,
	sqlconst.OptLowWatermark:  sqlconst.KVStringOptRequireValue,
}

var _ sql.PlanNode = &createPipeNode{}

type createPipeNode struct {
	n             *tree.CreatePipe
	databaseID    uint64
	tableDescList []*MutableTableDescriptor
	pipeOpts      func() (map[string]string, error)

	run pipeComputeRun
}

// NewCreatePipeNode creates a new createPipeNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreatePipeNode(
	n *tree.CreatePipe,
	databaseID uint64,
	tableDescList []*MutableTableDescriptor,
	pipeOpts func() (map[string]string, error),
	run pipeComputeRun,
) *createPipeNode {
	return &createPipeNode{
		n:             n,
		databaseID:    databaseID,
		tableDescList: tableDescList,
		pipeOpts:      pipeOpts,
		run:           run,
	}
}

type pipeComputeRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

// CreatePipe creates a pipe node for exec.
func CreatePipe(ctx context.Context, p *GenericPlanner, n *tree.CreatePipe) (sql.PlanNode, error) {
	found, err := sql.CheckPipeByName(ctx, p, n.PipeName)
	if err != nil {
		return nil, err
	}
	if found {
		return nil, pgerror.Newf(pgcode.DuplicateObject, "pipe %q already exists", n.PipeName)
	}
	var databaseID uint64

	if n.Table.TableName != "" {
		// single table
		n.TableNames = append(n.TableNames, n.Table)
	} else if n.Database != "" {
		// single database
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on database")
		}

		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Database), true)
		if err != nil {
			return nil, err
		}
		if dbDesc.EngineType != tree.EngineTypeTimeseries {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts database")
		}
		databaseID = uint64(dbDesc.ID)
		schemas, err := p.GetSchemasForDatabase(ctx, p.Txn(), dbDesc.ID)
		if err != nil {
			return nil, err
		}

		// the names of all objects in the target database
		for _, schema := range schemas {
			toAppend, err := sql.GetObjectNames(
				ctx, p.Txn(), p, dbDesc, schema, true, /*explicitPrefix*/
			)
			if err != nil {
				return nil, err
			}
			n.TableNames = append(n.TableNames, toAppend...)
		}

	} else {
		// multi table
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on multi table")
		}
	}
	tableDescList := make([]*MutableTableDescriptor, len(n.TableNames))
	checkTableDuplicate := make(map[sqlbase.ID]struct{})

	for i := range n.TableNames {
		tableDescList[i], err = p.ResolveMutableTableDescriptor(
			ctx, &n.TableNames[i], true /*required*/, sql.ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDescList[i].IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "pipe is only used on ts table")
		}

		if _, ok := checkTableDuplicate[tableDescList[i].ID]; ok {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "duplicate table %s", tableDescList[i].Name)
		}
		checkTableDuplicate[tableDescList[i].ID] = struct{}{}

		if err = sql.CheckPipePrivilege(ctx, p, tableDescList[i], privilege.CREATE, nil); err != nil {
			return nil, err
		}
	}

	pipeOpts, err := p.TypeAsStringOpts(n.Options, pipeOptionExpectValues)
	if err != nil {
		return nil, err
	}

	return &createPipeNode{n: n, tableDescList: tableDescList, pipeOpts: pipeOpts, databaseID: databaseID}, nil
}

func (n *createPipeNode) StartExec(params RunParams) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	pipeOpts, err := n.pipeOpts()
	if err != nil {
		return err
	}
	options, lowWatermark, err := sql.MakePipeOptions(pipeOpts, nil)
	if err != nil {
		return err
	}

	pipeTableInfos, pipeTableIDs, err := sql.MakeCDCTableInfo(
		params.Ctx, params.GetPlanner(), n.tableDescList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := sql.CheckWhereExprForCDC(
			params.Ctx, params.GetPlanner(), n.n.Table, n.tableDescList[0].TableDescriptor, n.n.Where.Expr)
		if err != nil {
			return err
		}

		if whereNeedNormalTag {
			pipeTableInfos[0].NeedNormalTag = true
		}

		pipeTableInfos[0].Filter = n.n.Where.Expr.String()
		if err = sql.CheckWhereExprForHistory(params.Ctx, params.GetPlanner(), &pipeTableInfos[0]); err != nil {
			return err
		}
	}

	for i := range pipeTableInfos {
		pipeTableInfos[i].NeedNormalTag = pipeTableInfos[i].NeedNormalTag && options.CheckTag == sqlconst.OptOn
	}

	para := cdcpb.PipeParameters{
		Tables:      pipeTableInfos,
		TableIDs:    pipeTableIDs,
		PipeOptions: options,
	}

	parameters, err := cdcpb.MarshalPipeParameters(para)
	if err != nil {
		return err
	}

	metadata := md.PipeMetadata{
		Name:       n.n.PipeName,
		Parameters: parameters,
		DatabaseID: n.databaseID,
		CreateBy:   params.GetPlanner().User(),
		CreateAt:   tree.DTimestamp{Time: timeutil.Now()},
	}

	// set status of pipe metadata to 'Enable' if it is in primary cluster and option enable is 'on'.
	if options.Enable == sqlconst.OptOn {
		if err = sql.CheckCDCMax(params.Ctx, params.GetPlanner(), int64(len(pipeTableIDs))); err != nil {
			return err
		}

		// try to connect to the target sink to ensure it's available.
		if err = cdcpb.CheckSink(options.Sink, true); err != nil {
			return errors.Wrapf(err, "Kafka topic %q is not available", options.Sink)
		}

		metadata.Status = sqlconst.StatusEnable
	} else {
		metadata.Status = sqlconst.StatusDisable
	}

	jobID := 0

	if _, err = params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"write-pipe-metadata",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_pipes
(name,parameters,create_by,create_at,status,run_info,job_id,source_id,low_water_mark)
values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		metadata.Name, metadata.Parameters, metadata.CreateBy, metadata.CreateAt.Time,
		metadata.Status, "[]", jobID, metadata.DatabaseID, math.MinInt64,
	); err != nil {
		return err
	}

	pipeSchema, err := sql.LoadPipeByName(params.Ctx, params.GetPlanner(), n.n.PipeName)
	if err != nil {
		return err
	}

	for i := range para.TableIDs {
		if err = params.GetPlanner().AddCDCWatermark(params.Ctx, md.CDCWatermark{
			TableID:      para.TableIDs[i],
			TaskID:       pipeSchema.ID,
			TaskType:     sqlbase.CDCInstanceType_Pipe,
			InternalType: cdcpb.CDCInternalTypeUnknown,
			LowWatermark: lowWatermark,
			ClientID:     nil,
		}); err != nil {
			return err
		}
	}

	// launch pipe job if the enable parameter is 'on' in primary cluster.
	// the secondary cluster does not start sql.
	if options.Enable == sqlconst.OptOn && metadata.Status == sqlconst.StatusEnable {
		if len(pipeTableIDs) != 0 {
			for _, tableDesc := range n.tableDescList {
				if err := params.GetPlanner().AddCDCDescriptor(
					params.Ctx, tableDesc, sqlbase.CDCInstanceType_Pipe, pipeSchema.ID, []byte(pipeSchema.Name),
				); err != nil {
					return err
				}
			}
			jobRecord, err := pipe.BuildPipeJobRecord(
				params, n.n.PipeName, &options, pipeTableInfos, pipeTableIDs, pipeSchema.ID)
			if err != nil {
				return err
			}
			n.run.resultsCh = make(chan tree.Datums)
			n.run.errCh = make(chan error)
			startCh := make(chan tree.Datums)
			go func() {
				err := sql.StartPipeJob(params.Ctx, params.GetPlanner(), startCh, *jobRecord, pipeSchema)
				select {
				case <-params.Ctx.Done():
				case n.run.errCh <- err:
				}
				close(n.run.errCh)
				close(n.run.resultsCh)
			}()
		}
	}
	params.GetPlanner().SetAuditTarget(uint32(pipeSchema.ID), pipeSchema.Name.String(), nil)

	return err
}

func (n *createPipeNode) Next(params RunParams) (bool, error) {
	if n.run.resultsCh != nil {
		select {
		case <-params.Ctx.Done():
			return false, params.Ctx.Err()
		case err := <-n.run.errCh:
			return false, err
		case <-n.run.resultsCh:
			return true, nil
		}
	} else {
		return false, nil
	}
}

func (n *createPipeNode) Values() tree.Datums { return tree.Datums{} }

func (n *createPipeNode) Close(context.Context) {}

//// marshalPipeFilter extracts the filter expressions of metrics and tags from the physical plan
//// and marshals them to bytes. They will be applied during the data capture phase.
//func marshalPipeFilter(
//	params RunParams, metadata *cdcpb.PipeMetadata, pipeTableInfo *cdcpb.CDCTableInfo,
//) error {
//	// make a new local planner
//	plan, cleanup := sql.NewInternalPlanner("pipe-filter-builder", params.PlannerTxn(), params.GetPlanner().User(),
//		&sql.MemoryMetrics{}, params.ExecCfg())
//	defer cleanup()
//
//	// The column order in the filter must be consistent with that in the payload
//	pipeQuery := fmt.Sprintf("SELECT * FROM %s.%s ",
//		pipeTableInfo.Database,
//		pipeTableInfo.Table)
//
//	if metadata.Filter != "" {
//		pipeQuery += " WHERE " + metadata.Filter
//	}
//
//	stmt, err := parser.ParseOne(pipeQuery)
//	if err != nil {
//		return err
//	}
//	localPlanner := plan.(*GenericPlanner)
//	localPlanner.stmt = &Statement{Statement: stmt}
//	localPlanner.forceFilterInME = true
//	localPlanner.SessionData().Database = params.GetPlanner().CurrentDatabase()
//	localPlanner.SessionData().SearchPath = params.GetPlanner().CurrentSearchPath()
//
//	localPlanner.optPlanningCtx.init(localPlanner)
//
//	localPlanner.RunWithOptions(sql.ResolveFlags{SkipCache: true}, func() {
//		err = localPlanner.makeOptimizerPlan(params.Ctx)
//	})
//	if err != nil {
//		return err
//	}
//	defer localPlanner.curPlan.close(params.Ctx)
//	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
//	isLocal := err != nil || rec == cannotDistribute
//	if len(localPlanner.curPlan.subqueryPlans) != 0 {
//		return pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the pipe filter")
//	}
//	evalCtx := localPlanner.ExtendedEvalContext()
//	planCtx := localPlanner.DistSQLPlanner().NewPlanningCtx(params.Ctx, evalCtx, params.PlannerTxn())
//	planCtx.isLocal = isLocal
//	planCtx.cdcCtx = &sql.CDCContext{}
//	planCtx.planner = localPlanner
//	planCtx.stmtType = tree.Rows
//
//	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtx, localPlanner.curPlan.plan)
//	if err != nil {
//		return err
//	}
//
//	localPlanner.DistSQLPlanner().FinalizePlan(planCtx, &physPlan)
//
//	if len(physPlan.Processors) == 1 {
//		if physPlan.Processors[0].Spec.Core.Values != nil {
//			return pgerror.Newf(pgcode.FeatureNotSupported, "pipe filter %q is invalid", metadata.Filter)
//		}
//	}
//
//	if planCtx.cdcCtx.metricsFilter.Expr != "" {
//		metadata.MetricsFilter, err = protoutil.Marshal(&planCtx.cdcCtx.metricsFilter)
//		if err != nil {
//			return err
//		}
//	}
//
//	for _, tf := range planCtx.cdcCtx.tagFilter {
//		filler, err := protoutil.Marshal(&tf)
//		if err != nil {
//			return err
//		}
//		metadata.TagFilter = append(metadata.TagFilter, filler)
//	}
//	return nil
//}
//
//func buildPipeJobRecord(
//	params RunParams,
//	name tree.Name,
//	opt *cdcpb.PipeOptions,
//	pipeTableInfo []cdcpb.CDCTableInfo,
//	tableIDList []uint64,
//	pipeID uint64,
//) (*jobs.Record, error) {
//	metadata := &cdcpb.PipeMetadata{
//		ID:            pipeID,
//		Name:          string(name),
//		Sink:          opt.Sink,
//		MessageFormat: sqlconst.MessageFormatJSON,
//		IgnoreHistory: opt.IgnoreHistory == sqlconst.OptOn,
//		BufferSize:    uint64(opt.BufferSize * 1024 * 1024),
//		Publish:       opt.Publish,
//	}
//
//	metadata.TableList = make([]*cdcpb.CDCTable, len(pipeTableInfo))
//	for i := range pipeTableInfo {
//		metadata.TableList[i] = &cdcpb.CDCTable{
//			TableID:               tableIDList[i],
//			Database:              pipeTableInfo[i].Database,
//			Schema:                pipeTableInfo[i].Schema,
//			Table:                 pipeTableInfo[i].Table,
//			TsColumn:              pipeTableInfo[i].TsColumnName,
//			TsColumnPrecision:     pipeTableInfo[i].TsColumnPrecision,
//			OutputColumns:         pipeTableInfo[i].ColIDs,
//			OutputColumnTypes:     pipeTableInfo[i].ColTypes,
//			OutputColumnNames:     pipeTableInfo[i].ColNames,
//			NeedNormalTag:         pipeTableInfo[i].NeedNormalTag,
//			PrimaryTagColumnNames: pipeTableInfo[i].PrimaryTagCols,
//			NormalTagColumnNames:  pipeTableInfo[i].NormalTagCols,
//		}
//	}
//
//	if len(pipeTableInfo) == 1 {
//		metadata.Filter = pipeTableInfo[0].Filter
//		// extract and fill in the column ids, metrics and tag filter expressions.
//		if err := marshalPipeFilter(params, metadata, &pipeTableInfo[0]); err != nil {
//			return nil, err
//		}
//	}
//
//	return &jobs.Record{
//		Description: fmt.Sprintf(
//			"computes and persists global low-water mark for pipe %v, id %d", name, pipeID),
//		Statement: "",
//		Username:  params.GetPlanner().User(),
//		Details: jobspb.PipeWatermarkDetails{
//			PipeMetadata: metadata,
//		},
//		Progress: jobspb.PipeWatermarkProgress{},
//	}, nil
//}
//
//// checkWhereExprForCDC checks if the filter is supported by pipe. Currently, only row-based simple
//// Only supports immutable functions.
//func (p *GenericPlanner) checkWhereExprForCDC(
//	ctx context.Context, tn tree.TableName, table TableDescriptor, expr tree.Expr,
//) (hasNormalTag bool, err error) {
//	var hasNormalTag1, hasNormalTag2 bool
//	switch exp := expr.(type) {
//	case *tree.AndExpr:
//		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.OrExpr:
//		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.ComparisonExpr:
//		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.BinaryExpr:
//		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Right); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.RangeCond:
//		if hasNormalTag1, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Left); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag2, err = p.checkWhereExprForCDC(ctx, tn, table, exp.From); err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.To); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.UnresolvedName:
//		v, err := exp.NormalizeVarName()
//		if err != nil {
//			return hasNormalTag, err
//		}
//		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, v); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.ColumnItem:
//		col, dropped, err := table.FindColumnByName(exp.ColumnName)
//		if err != nil || dropped {
//			return hasNormalTag, sqlbase.NewUndefinedColumnError(string(exp.ColumnName))
//		}
//		if col.IsTagCol() && !col.IsPrimaryTagCol() {
//			hasNormalTag = true
//		}
//	case tree.Constant, tree.DNullExtern, *tree.DBool, *tree.Tuple:
//	case *tree.ParenExpr:
//		if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Expr); err != nil {
//			return hasNormalTag, err
//		}
//	case *tree.FuncExpr:
//		for i := range exp.Exprs {
//			if hasNormalTag, err = p.checkWhereExprForCDC(ctx, tn, table, exp.Exprs[i]); err != nil {
//				return hasNormalTag, err
//			}
//		}
//		f := p.optPlanningCtx.optimizer.Factory()
//		catalog := p.optPlanningCtx.catalog
//		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &catalog, f, p.stmt.AST)
//		tExpr := bld.BuildFuncForPipe(exp, table, tn)
//		if fExpr, ok := tExpr.(*tree.FuncExpr); ok {
//			fn := fExpr.ResolvedOverload()
//			if fn.AggregateFunc != nil || fn.WindowFunc != nil || strings.Contains(exp.Func.FunctionReference.FunctionName(), "time_bucket") {
//				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
//			}
//			switch fn.Volatility {
//			case tree.VolatilityImmutable, tree.VolatilityLeakProof:
//			default:
//				return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
//			}
//		} else {
//			return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "function %q is not supported by pipe filter", exp.Func.FunctionReference.FunctionName())
//		}
//	default:
//		return hasNormalTag, pgerror.Newf(pgcode.FeatureNotSupported, "expr %q is not supported by pipe filter", expr.String())
//	}
//
//	return hasNormalTag || hasNormalTag1 || hasNormalTag2, nil
//}
//
//func (p *GenericPlanner) checkWhereExprForHistory(ctx context.Context, table *cdcpb.CDCTableInfo) error {
//	const queryFormat = "SELECT * FROM %s.%s WHERE %s LIMIT 1"
//	query := fmt.Sprintf(
//		queryFormat,
//		table.Database,
//		table.Table,
//		table.Filter,
//	)
//
//	if _, err := p.ExecCfg().InternalExecutor.QueryRowEx(
//		ctx,
//		"check-where-expr",
//		p.Txn(),
//		InternalExecutorSessionDataOverride{User: security.RootUser},
//		query,
//	); err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func makePipeOptions(
//	pipeOpts map[string]string, originOpts *cdcpb.PipeOptions,
//) (cdcpb.PipeOptions, int64, error) {
//	var opts cdcpb.PipeOptions
//	watermark := int64(cdcpb.InvalidWatermark)
//
//	if value, ok := pipeOpts[optSink]; ok {
//		opts.Sink = value
//
//		if err := cdcpb.CheckSink(opts.Sink, false); err != nil {
//			return opts, watermark, err
//		}
//	} else {
//		if originOpts != nil {
//			opts.Sink = originOpts.Sink
//		} else {
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "option \"sink\" is required.")
//		}
//	}
//
//	if value, ok := pipeOpts[optEnable]; ok {
//		lowerValue := strings.ToLower(value)
//		switch lowerValue {
//		case sqlconst.OptOn, optOff:
//			opts.Enable = lowerValue
//		default:
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "enable parameter %q is invalid", value)
//		}
//	} else {
//		if originOpts != nil {
//			opts.Enable = originOpts.Enable
//		} else {
//			opts.Enable = sqlconst.OptOn
//		}
//	}
//
//	if value, ok := pipeOpts[optMessageFormat]; ok {
//		lowerValue := strings.ToLower(value)
//		switch lowerValue {
//		case sqlconst.MessageFormatJSON:
//			opts.MessageFormat = lowerValue
//		default:
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "invalid message_format: %s", value)
//		}
//	} else {
//		if originOpts != nil {
//			opts.MessageFormat = originOpts.MessageFormat
//		} else {
//			opts.MessageFormat = sqlconst.MessageFormatJSON
//		}
//	}
//
//	if value, ok := pipeOpts[sqlconst.OptIgnoreHistory]; ok {
//		lowerValue := strings.ToLower(value)
//		switch lowerValue {
//		case sqlconst.OptOn, optOff:
//			opts.IgnoreHistory = lowerValue
//		default:
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptIgnoreHistory, value)
//		}
//	} else {
//		if originOpts != nil {
//			opts.IgnoreHistory = originOpts.IgnoreHistory
//		} else {
//			opts.IgnoreHistory = sqlconst.OptOn
//		}
//	}
//
//	if value, ok := pipeOpts[optBufferSize]; ok {
//		num, err := strconv.Atoi(value)
//		if err != nil {
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optBufferSize, value)
//		}
//		if num < 0 || num > 1024 {
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", optBufferSize, value)
//		}
//		opts.BufferSize = num
//	} else {
//		if originOpts != nil {
//			opts.BufferSize = originOpts.BufferSize
//		} else {
//			opts.BufferSize = defaultBufferSize
//		}
//	}
//
//	if value, ok := pipeOpts[optCheckTag]; ok {
//		lowerValue := strings.ToLower(value)
//		switch lowerValue {
//		case sqlconst.OptOn, optOff:
//			opts.CheckTag = lowerValue
//		default:
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optCheckTag, value)
//		}
//	} else {
//		if originOpts != nil {
//			opts.CheckTag = originOpts.CheckTag
//		} else {
//			opts.CheckTag = sqlconst.OptOn
//		}
//	}
//
//	if value, ok := pipeOpts[optPublish]; ok {
//		lowerValues := strings.Split(strings.ToLower(value), ",")
//		keys := make(map[string]bool)
//		var list []string
//		containsAll := false
//		for _, lowerValue := range lowerValues {
//			if _, ok := keys[lowerValue]; ok {
//				continue
//			}
//
//			keys[lowerValue] = true
//			switch lowerValue {
//			case cdcpb.EventAll:
//				list = []string{cdcpb.EventAll}
//				containsAll = true
//			case cdcpb.EventInsert, cdcpb.EventDelete, cdcpb.EventUpdate, cdcpb.EventDDL:
//				list = append(list, lowerValue)
//			default:
//				return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optPublish, lowerValue)
//			}
//			if containsAll {
//				break
//			}
//		}
//		if len(list) > 0 {
//			opts.Publish = strings.Join(list, ",")
//		} else {
//			return opts, watermark, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optPublish, value)
//		}
//	} else {
//		if originOpts != nil {
//			opts.Publish = originOpts.Publish
//		} else {
//			opts.Publish = cdcpb.EventInsert
//		}
//	}
//
//	if value, ok := pipeOpts[optLowWatermark]; ok {
//		watermarkTime, err := tree.ParseDTimestamp(nil, value, time.Nanosecond)
//		if err != nil {
//			return opts, watermark, pgerror.Newf(
//				pgcode.InvalidParameterValue,
//				"%v, %s parameter %q is invalid, the format is like %s",
//				err,
//				optLowWatermark,
//				value,
//				optLowWatermarkFormat,
//			)
//		}
//
//		watermark = watermarkTime.UTC().UnixNano()
//		if watermark <= cdcpb.InvalidWatermark || watermark >= tree.TsMaxNanoTimestamp {
//			return opts, watermark, pgerror.Newf(
//				pgcode.InvalidParameterValue,
//				"%s parameter %q is invalid, must be from %v to %v",
//				optLowWatermark,
//				value,
//				timeutil.FromUnixNano(cdcpb.InvalidWatermark).UTC(),
//				timeutil.FromUnixNano(tree.TsMaxNanoTimestamp).UTC(),
//			)
//		}
//	}
//
//	return opts, watermark, nil
//}
//
//// makeCDCTableInfo constructs CDCTableInfo and extracts tableIds for Pipe and Publication.
//// It needs to check whether the specified columns in
//// CREATE PIPE FOR TABLE table_name(column_name[, ...]), ALTER PIPE SET TABLE table_name(column_name[, ...]),
//// CREATE PUB FOR TABLE table_name(column_name[, ...]), and ALTER PUB SET TABLE table_name(column_name[, ...])
//// are still existed. And in these cases, the parameter needCheckColumns is true.
//func (p *GenericPlanner) makeCDCTableInfo(
//	ctx context.Context,
//	tableDescList []*MutableTableDescriptor,
//	star bool,
//	cols tree.NameList,
//	needCheckColumns bool,
//) ([]cdcpb.CDCTableInfo, []uint64, error) {
//	var tables []cdcpb.CDCTableInfo
//	var tableIds []uint64
//
//	for i := range tableDescList {
//		var tableInfo cdcpb.CDCTableInfo
//		var columnTypes []string
//		var columnNames []string
//		var columnIDs []uint32
//		tableDesc := tableDescList[i]
//		if star {
//			for _, col := range tableDesc.Columns {
//				columnIDs = append(columnIDs, uint32(col.ID))
//				columnNames = append(columnNames, col.Name)
//				// get all column type
//				columnTypes = append(columnTypes, col.Type.SQLString())
//				if col.IsTagCol() && !col.IsPrimaryTagCol() {
//					tableInfo.NeedNormalTag = true
//				}
//			}
//		} else {
//
//			for _, colName := range cols {
//				col, dropping, err := tableDesc.FindColumnByName(colName)
//				if err != nil {
//					if needCheckColumns {
//						return nil, nil, err
//					}
//					// alter table drop column will trigger update columns automatically
//					continue
//				}
//				if dropping {
//					if needCheckColumns {
//						return nil, nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
//							"column %q being dropped, try again later", col.Name)
//					}
//					// alter table drop column will trigger update columns automatically
//					continue
//				}
//				columnIDs = append(columnIDs, uint32(col.ID))
//				columnNames = append(columnNames, string(colName))
//				// get all column type
//				columnTypes = append(columnTypes, col.Type.SQLString())
//				if col.IsTagCol() && !col.IsPrimaryTagCol() {
//					tableInfo.NeedNormalTag = true
//				}
//			}
//		}
//
//		dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.Txn(), tableDesc.ParentID)
//		if err != nil {
//			return nil, nil, err
//		}
//		tableInfo.Database = dbDesc.Name
//		// can not create other schema in ts database.
//		tableInfo.Schema = "public"
//		tableInfo.ID = uint64(tableDesc.ID)
//		tableInfo.Table = tableDesc.Name
//		tableInfo.IsStar = star
//		tableInfo.ColIDs = columnIDs
//		tableInfo.ColNames = columnNames
//		tableInfo.ColTypes = columnTypes
//		tableInfo.LowWatermark = 0
//		tableInfo.TsColumnName = tableDesc.Columns[0].Name
//		tableInfo.TsColumnPrecision = tableDesc.Columns[0].Type.Precision()
//		var primaryTags, normalTags []string
//		for _, col := range tableDesc.Columns {
//			if col.IsPrimaryTagCol() {
//				primaryTags = append(primaryTags, col.Name)
//				continue
//			}
//			if !col.IsPrimaryTagCol() && col.IsTagCol() {
//				normalTags = append(normalTags, col.Name)
//			}
//		}
//		tableInfo.PrimaryTagCols = primaryTags
//		tableInfo.NormalTagCols = normalTags
//		tables = append(tables, tableInfo)
//		tableIds = append(tableIds, uint64(tableDesc.ID))
//	}
//
//	return tables, tableIds, nil
//}
//
//type pipeWatermarkResumer struct {
//	job *jobs.Job
//}
//
//func (s *pipeWatermarkResumer) Resume(
//	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
//) error {
//	p := phs.(*planner)
//	pipeID := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.ID
//	pipeName := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.Name
//
//	dsp := p.DistSQLPlanner()
//	// Prepare the planning context.
//	evalCtx := p.ExtendedEvalContext()
//	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{})
//	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
//	defer func() {
//		if rows != nil {
//			rows.Close(ctx)
//		}
//	}()
//
//	var noTxn *kv.Txn
//	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
//	planCtx.planner = p
//
//	if err := dsp.planAndRunCreatePipe(
//		ctx, evalCtx, planCtx, noTxn, s.job, NewRowResultWriter(rows), resultsCh,
//	); err != nil {
//		// ctx maybe canceled
//		if strings.Contains(err.Error(), "stopped successfully") {
//			log.Infof(ctx, "successful to stop pipe %s.", pipeName)
//		} else {
//			log.Infof(ctx, "pipe %q failed with error %s", pipeName, err.Error())
//		}
//
//		updateErr := p.UpdatePipeRunHistory(context.Background(), s.job, err, pipeID)
//		if updateErr != nil {
//			return errors.Wrap(err, updateErr.Error())
//		}
//
//		return err
//	}
//
//	return nil
//}
//
//func (s *pipeWatermarkResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
//	return nil
//}
//
//var _ jobs.Resumer = &pipeWatermarkResumer{}
//
//func init() {
//	pipeWatermarkResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
//		return &pipeWatermarkResumer{job: job}
//	}
//	jobs.RegisterConstructor(jobspb.TypePipeWatermark, pipeWatermarkResumerFn)
//}
