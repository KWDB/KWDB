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

package ddl

import (
	"context"
	"go/constant"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	_ "gitee.com/kwbasedb/kwbase/pkg/stream" // for stream testing
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var streamOptionExpectValues = map[string]sqlconst.KVStringOptValidate{
	sqlutil.OptEnable:                 sqlconst.KVStringOptRequireValue,
	sqlutil.OptMaxDelay:               sqlconst.KVStringOptRequireValue,
	sqlutil.OptSyncTime:               sqlconst.KVStringOptRequireValue,
	sqlutil.OptProcessHistory:         sqlconst.KVStringOptRequireValue,
	sqlutil.OptIgnoreExpired:          sqlconst.KVStringOptRequireValue,
	sqlutil.OptIgnoreUpdate:           sqlconst.KVStringOptRequireValue,
	sqlutil.OptMaxRetries:             sqlconst.KVStringOptRequireValue,
	sqlutil.OptCheckpointInterval:     sqlconst.KVStringOptRequireValue,
	sqlutil.OptHeartbeatInterval:      sqlconst.KVStringOptRequireValue,
	sqlutil.OptRecalculateDelayRounds: sqlconst.KVStringOptRequireValue,
	sqlutil.OptBufferSize:             sqlconst.KVStringOptRequireValue,
	sqlutil.OptLowLatency:             sqlconst.KVStringOptRequireValue,
}

var _ sql.PlanNode = &createStreamNode{}

type createStreamNode struct {
	n               *tree.CreateStream
	targetTableDesc *MutableTableDescriptor
	streamOpts      func() (map[string]string, error)
	isExist         bool

	run streamComputeRun
}

// NewCreateStreamNode creates a new createStreamNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateStreamNode(
	n *tree.CreateStream,
	targetTableDesc *MutableTableDescriptor,
	streamOpts func() (map[string]string, error),
	isExist bool,
	run streamComputeRun,
) *createStreamNode {
	return &createStreamNode{
		n:               n,
		targetTableDesc: targetTableDesc,
		streamOpts:      streamOpts,
		isExist:         isExist,
		run:             run,
	}
}

// NewCreateStreamNode2 creates a new createStreamNode.
// nolint:unexportedreturn
func NewCreateStreamNode2(
	n *tree.CreateStream,
	targetTableDesc *MutableTableDescriptor,
	streamOpts func() (map[string]string, error),
	isExist bool,
) *createStreamNode {
	return &createStreamNode{
		n:               n,
		targetTableDesc: targetTableDesc,
		streamOpts:      streamOpts,
		isExist:         isExist,
	}
}

type streamComputeRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

// CreateStream creates a stream node for exec.
func CreateStream(
	ctx context.Context, p *GenericPlanner, n *tree.CreateStream,
) (sql.PlanNode, error) {
	found, err := sql.FindStreamByName(ctx, p, n.StreamName)
	if err != nil {
		return nil, err
	}
	if found {
		if n.IfNotExists {
			return &createStreamNode{n: n, isExist: true}, nil
		}
		return nil, pgerror.Newf(pgcode.DuplicateObject, "stream %q already exists", n.StreamName)
	}

	targetTableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, sql.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}

	// check the target table for INSERT privilege
	if err = sql.CheckStreamPrivilege(
		ctx, p, targetTableDesc, privilege.CREATE, privilege.INSERT,
		p.User(), n.StreamName.String(),
	); err != nil {
		return nil, err
	}

	streamOpts, err := p.TypeAsStringOpts(n.Options, streamOptionExpectValues)
	if err != nil {
		return nil, err
	}

	return &createStreamNode{n: n, targetTableDesc: targetTableDesc, streamOpts: streamOpts}, nil
}

func (n *createStreamNode) StartExec(params RunParams) (err error) {
	if n.isExist {
		return nil
	}

	targetTableInfo, targetTableID, err := sql.MakeStreamTableCommonInfo(
		params.Ctx, params.GetPlanner(), n.targetTableDesc)
	if err != nil {
		return err
	}

	var sourceTableDesc *MutableTableDescriptor
	if sourceTableDesc, err = sql.CheckStreamQuerySourceTable(params.Ctx, params.GetPlanner(), n.n.Query); err != nil {
		return err
	}
	sourceTableInfo, sourceTableID, err := sql.MakeStreamTableCommonInfo(
		params.Ctx, params.GetPlanner(), sourceTableDesc)
	if err != nil {
		return err
	}

	if sourceTableID == targetTableID {
		return errors.Newf(
			"cannot use the table \"%s.%s\" as both source and target table of stream",
			sourceTableInfo.Database, sourceTableInfo.Table,
		)
	}

	// check the source table for SELECT privilege.
	if err = sql.CheckStreamPrivilege(
		params.Ctx, params.GetPlanner(), sourceTableDesc, privilege.CREATE, privilege.SELECT,
		params.GetPlanner().User(), n.n.StreamName.String(),
	); err != nil {
		return err
	}

	// check the stream options.
	streamOpts, err := n.streamOpts()
	if err != nil {
		return err
	}
	options, err := sqlutil.MakeStreamOptions(streamOpts, nil)
	if err != nil {
		return err
	}

	originalQuery, err := makeOriginalQuery(n.n.Query, sourceTableInfo)
	if err != nil {
		return err
	}

	if err := sqlutil.CheckStreamOptions(options, targetTableInfo.IsTsTable); err != nil {
		return err
	}

	para := sqlutil.StreamParameters{
		SourceTableID: sourceTableID,
		SourceTable:   *sourceTableInfo,
		TargetTable:   *targetTableInfo,
		TargetTableID: targetTableID,
		Options:       *options,
		StreamSink:    *originalQuery,
	}

	// build plan of stream query, get result types.
	marshaledStreamParas, err := sqlutil.MarshalStreamParameters(para)
	if err != nil {
		return err
	}

	tempMetadata := &cdcpb.StreamMetadata{
		ID:         0,
		Name:       "FakeName",
		Parameters: marshaledStreamParas.String(),
	}
	physicalPlan, err := sql.CreatePlanForStream(params.Ctx, params, n.n.Query.String(), tempMetadata, originalQuery.HasAgg)
	if err != nil {
		return err
	}

	// check target table.
	targetColTypes, err := sql.CheckStreamTargetTableInfo(
		params.Ctx, params.GetPlanner(), n.targetTableDesc, &para, physicalPlan.ResultTypes, n.n.Query,
	)
	if err != nil {
		return err
	}

	// marshal stream parameters to JSON.
	marshaledStreamParas, err = sqlutil.MarshalStreamParameters(para)
	if err != nil {
		return err
	}

	metadata := metadata.StreamMetadata{
		Name:          n.n.StreamName,
		Parameters:    marshaledStreamParas,
		TargetTableID: targetTableID,
		CreateBy:      params.GetPlanner().User(),
		CreateAt:      tree.DTimestamp{Time: timeutil.Now()},
		LowWaterMark:  sqlutil.InvalidWaterMark,
		SourceTableID: sourceTableID,
	}

	if options.Enable == sqlutil.StreamOptOn {
		if err = sql.CheckStreamMax(params.Ctx, params.GetPlanner()); err != nil {
			return err
		}

		metadata.Status = sqlutil.StreamStatusEnable
	} else {
		metadata.Status = sqlutil.StreamStatusDisable
	}

	jobID := 0

	// save metadata to system table.
	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"insert-stream-metadata",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_streams
(name, parameters, create_by, create_at, status, run_info, job_id, target_table_id, source_table_id)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		metadata.Name, metadata.Parameters, metadata.CreateBy, metadata.CreateAt.Time,
		metadata.Status, "[]", jobID, metadata.TargetTableID, metadata.SourceTableID); err != nil {
		return err
	}

	streamSchema, err := sql.LoadStreamByName(params.Ctx, params.GetPlanner(), n.n.StreamName)
	if err != nil {
		return err
	}

	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"insert-stream-low-water-mark",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_cdc_watermark (table_id,task_id,task_type,internal_type,low_watermark)
VALUES ($1,$2,$3,$4,$5)`,
		metadata.SourceTableID,
		streamSchema.ID,
		sqlbase.CDCInstanceType_Stream,
		sqlconst.WaterMarkTypeRealtime,
		sqlutil.InvalidWaterMark,
	); err != nil {
		return err
	}

	// launch the stream job if the enable option is 'on'.
	// the secondary cluster does not start sql.
	if options.Enable == sqlutil.StreamOptOn && metadata.Status == sqlutil.StreamStatusEnable {
		jobRecord, err := sql.BuildStreamJobRecord(
			params, n.n.StreamName, streamSchema.ID, marshaledStreamParas.String(),
			originalQuery.SQL, sourceTableInfo, targetColTypes,
		)
		if err != nil {
			return err
		}

		n.run.resultsCh = make(chan tree.Datums)
		n.run.errCh = make(chan error)
		startCh := make(chan tree.Datums)

		go func() {
			err := sql.CreateAndStartStreamJob(params.Ctx, params.GetPlanner(), startCh, *jobRecord, streamSchema)
			select {
			case <-params.Ctx.Done():
			case n.run.errCh <- err:
			}
			close(n.run.errCh)
			close(n.run.resultsCh)
		}()

		log.Infof(params.Ctx, "create and start stream %s(%d)", n.n.StreamName, streamSchema.ID)
	}

	return nil
}

func (n *createStreamNode) Next(params RunParams) (bool, error) {
	if n.run.resultsCh != nil {
		var timeout timeutil.Timer
		defer timeout.Stop()
		timeout.Reset(time.Second * sqlutil.TimeoutFactor)

		select {
		case <-params.Ctx.Done():
			return false, params.Ctx.Err()
		case err := <-n.run.errCh:
			return false, err
		case <-n.run.resultsCh:
			return true, nil
		case <-timeout.C:
			return false, errors.New("timed out waiting for create stream job")
		}
	} else {
		return false, nil
	}
}

func (n *createStreamNode) Values() tree.Datums { return tree.Datums{} }

func (n *createStreamNode) Close(context.Context) {}

// makeOriginalQuery repairs the table names in the SQL to full paths(database.schema.table).
// It also adds 'where 1=1' when the WHERE clause is missing.
func makeOriginalQuery(
	query *tree.Select, sourceTableInfo *cdcpb.CDCTableInfo,
) (*sqlutil.StreamSink, error) {
	selectClause, ok := query.Select.(*tree.SelectClause)
	if !ok {
		return nil, errors.Errorf("invalid stream query: %s", query.String())
	}

	streamSink := &sqlutil.StreamSink{}

	if len(selectClause.GroupBy) != 0 {
		streamSink.HasAgg = true
		last := len(selectClause.GroupBy) - 1
		expr := selectClause.GroupBy[last]
		switch expr.(type) {
		case *tree.FuncExpr:
			funcExpr := expr.(*tree.FuncExpr)
			streamSink.Function = funcExpr.Func.FunctionName()
			switch streamSink.Function {
			case memo.CountWindow:
				streamSink.HasSlide = len(funcExpr.Exprs) == 2
			case memo.TimeWindow:
				streamSink.HasSlide = len(funcExpr.Exprs) == 3
			}
		}
	}

	// check and fix FROM clause
	if len(selectClause.From.Tables) != 1 {
		return nil, errors.Errorf("stream only supports single table query")
	}

	tableName, ok := sql.GetSourceTableName(query)
	if !ok {
		return nil, errors.Errorf("failed to extract table name from stream query")
	}

	tableName.ExplicitCatalog = true
	tableName.ExplicitSchema = true

	// WHERE clause doesn't exist, add `WHERE 1=1` for re-calculator cases.
	if selectClause.Where == nil {
		where := &tree.Where{
			Type: tree.AstWhere,
			Expr: &tree.ComparisonExpr{
				Left:  tree.NewNumVal(constant.MakeInt64(1), "1", false),
				Right: tree.NewNumVal(constant.MakeInt64(1), "1", false),
			},
		}
		selectClause.Where = where
	}

	sourceTableInfo.Filter = selectClause.Where.Expr.String()
	streamSink.SQL = selectClause.String()
	return streamSink, nil
}
