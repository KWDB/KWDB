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
	"go/constant"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	typeDropStreamTable = iota
	typeAddStreamTable
	typeAlterStreamTable
	typeOtherStreamTable
)

// TsStreamMaxActiveNumber indicates the max number of running stream
var TsStreamMaxActiveNumber = func() *settings.IntSetting {
	s := settings.RegisterPositiveIntSetting(
		"ts.stream.max_active_number",
		"the max number of running stream",
		10,
	)
	s.SetVisibility(settings.Public)
	return s
}()

var streamOptionExpectValues = map[string]KVStringOptValidate{
	sqlutil.OptEnable:                 KVStringOptRequireValue,
	sqlutil.OptMaxDelay:               KVStringOptRequireValue,
	sqlutil.OptSyncTime:               KVStringOptRequireValue,
	sqlutil.OptProcessHistory:         KVStringOptRequireValue,
	sqlutil.OptIgnoreExpired:          KVStringOptRequireValue,
	sqlutil.OptIgnoreUpdate:           KVStringOptRequireValue,
	sqlutil.OptMaxRetries:             KVStringOptRequireValue,
	sqlutil.OptCheckpointInterval:     KVStringOptRequireValue,
	sqlutil.OptHeartbeatInterval:      KVStringOptRequireValue,
	sqlutil.OptRecalculateDelayRounds: KVStringOptRequireValue,
	sqlutil.OptBufferSize:             KVStringOptRequireValue,
}

type createStreamNode struct {
	n               *tree.CreateStream
	targetTableDesc *sqlbase.MutableTableDescriptor
	streamOpts      func() (map[string]string, error)
	isExist         bool

	run streamComputeRun
}

type streamComputeRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

// StreamMetadata records a list of stream info to run stream
type StreamMetadata struct {
	id            uint64
	name          tree.Name
	createBy      string
	createAt      tree.DTimestamp
	status        string
	targetTableID uint64
	sourceTableID uint64
	lowWaterMark  int64
	jobID         int64
	parameters    json.JSON
	runInfo       json.JSON

	streamParameters sqlutil.StreamParameters
	runInfoList      []sqlutil.RunInfo
}

// Decode decode JSON to struct.
func (p *StreamMetadata) Decode() error {
	var err error
	p.streamParameters, err = sqlutil.UnmarshalStreamParameters(p.parameters)
	if err != nil {
		return err
	}

	p.runInfoList, err = sqlutil.UnmarshalStreamRunInfo(p.runInfo)
	if err != nil {
		return err
	}

	return nil
}

// CreateStream creates a stream node for exec.
func (p *planner) CreateStream(ctx context.Context, n *tree.CreateStream) (planNode, error) {
	found, err := p.findStreamByName(ctx, n.StreamName)
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
		ctx, &n.Table, true /*required*/, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}

	// check the target table for INSERT privilege
	if err = p.checkStreamPrivilege(
		ctx, targetTableDesc, privilege.CREATE, privilege.INSERT,
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

func (n *createStreamNode) startExec(params runParams) (err error) {
	if n.isExist {
		return nil
	}

	targetTableInfo, targetTableID, err := params.p.makeStreamTableCommonInfo(params.ctx, n.targetTableDesc)
	if err != nil {
		return err
	}

	var sourceTableDesc *MutableTableDescriptor
	if sourceTableDesc, err = params.p.checkStreamQuerySourceTable(params.ctx, n.n.Query); err != nil {
		return err
	}
	sourceTableInfo, sourceTableID, err := params.p.makeStreamTableCommonInfo(params.ctx, sourceTableDesc)

	if sourceTableID == targetTableID {
		return errors.Newf(
			"cannot use the table \"%s.%s\" as both source and target table of stream",
			sourceTableInfo.Database, sourceTableInfo.Table,
		)
	}

	// check the source table for SELECT privilege.
	if err = params.p.checkStreamPrivilege(
		params.ctx, sourceTableDesc, privilege.CREATE, privilege.SELECT,
		params.p.User(), n.n.StreamName.String(),
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

	query, hasAgg, err := fixOriginalQuery(n.n.Query, sourceTableInfo)
	if err != nil {
		return err
	}

	if err := sqlutil.CheckStreamOptions(options, targetTableInfo.IsTsTable); err != nil {
		return err
	}

	originalQuery := sqlutil.StreamSink{
		SQL:    query,
		HasAgg: hasAgg,
	}

	para := sqlutil.StreamParameters{
		SourceTableID: sourceTableID,
		SourceTable:   *sourceTableInfo,
		TargetTable:   *targetTableInfo,
		TargetTableID: targetTableID,
		Options:       *options,
		StreamSink:    originalQuery,
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
	physicalPlan, err := createPlanForStream(params.ctx, params, n.n.Query.String(), tempMetadata, hasAgg)
	if err != nil {
		return err
	}

	// check target table.
	targetColTypes, err := params.p.checkStreamTargetTableInfo(
		params.ctx, n.targetTableDesc, &para, physicalPlan.ResultTypes, n.n.Query,
	)
	if err != nil {
		return err
	}

	// marshal stream parameters to JSON.
	marshaledStreamParas, err = sqlutil.MarshalStreamParameters(para)
	if err != nil {
		return err
	}

	metadata := StreamMetadata{
		name:          n.n.StreamName,
		parameters:    marshaledStreamParas,
		targetTableID: targetTableID,
		createBy:      params.p.User(),
		createAt:      tree.DTimestamp{Time: timeutil.Now()},
		lowWaterMark:  sqlutil.InvalidWaterMark,
		sourceTableID: sourceTableID,
	}

	if options.Enable == sqlutil.StreamOptOn {
		if err = params.p.checkStreamMax(params.ctx); err != nil {
			return err
		}

		metadata.status = sqlutil.StreamStatusEnable
	} else {
		metadata.status = sqlutil.StreamStatusDisable
	}

	jobID := 0

	// save metadata to system table.
	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"insert-stream-metadata",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_streams
(name, parameters, create_by, create_at, status, run_info, job_id, target_table_id, source_table_id)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		metadata.name, metadata.parameters, metadata.createBy, metadata.createAt.Time,
		metadata.status, "[]", jobID, metadata.targetTableID, metadata.sourceTableID); err != nil {
		return err
	}

	streamSchema, err := params.p.loadStreamByName(params.ctx, n.n.StreamName)
	if err != nil {
		return err
	}

	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"insert-stream-low-water-mark",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_cdc_watermark (table_id,task_id,task_type,internal_type,low_watermark)
VALUES ($1,$2,$3,$4,$5)`,
		metadata.sourceTableID,
		streamSchema.id,
		cdcpb.TSCDCInstanceType_Stream,
		waterMarkTypeRealtime,
		sqlutil.InvalidWaterMark,
	); err != nil {
		return err
	}

	// launch the stream job if the enable option is 'on'.
	if options.Enable == sqlutil.StreamOptOn {
		jobRecord, err := buildStreamJobRecord(
			params, n.n.StreamName, streamSchema.id, marshaledStreamParas.String(),
			originalQuery.SQL, sourceTableInfo, targetColTypes,
		)
		if err != nil {
			return err
		}

		n.run.resultsCh = make(chan tree.Datums)
		n.run.errCh = make(chan error)
		startCh := make(chan tree.Datums)

		go func() {
			err := params.p.createAndStartStreamJob(params.ctx, startCh, *jobRecord, streamSchema)
			select {
			case <-params.ctx.Done():
			case n.run.errCh <- err:
			}
			close(n.run.errCh)
			close(n.run.resultsCh)
		}()
	}

	return nil
}

func (n *createStreamNode) Next(params runParams) (bool, error) {
	if n.run.resultsCh != nil {
		select {
		case <-params.ctx.Done():
			return false, params.ctx.Err()
		case err := <-n.run.errCh:
			return false, err
		case <-n.run.resultsCh:
			return true, nil
		}
	} else {
		return false, nil
	}
}

func (n *createStreamNode) Values() tree.Datums { return tree.Datums{} }

func (n *createStreamNode) Close(context.Context) {}

// fixOriginalQuery repairs the table names in the SQL to full paths(database.schema.table).
// It also adds 'where 1=1' when the WHERE clause is missing.
func fixOriginalQuery(
	query *tree.Select, sourceTableInfo *cdcpb.CDCTableInfo,
) (string, bool, error) {
	selectClause, ok := query.Select.(*tree.SelectClause)
	if !ok {
		return "", false, errors.Errorf("invalid stream query: %s", query.String())
	}

	hasAgg := false
	if len(selectClause.GroupBy) != 0 {
		hasAgg = true
	}

	// check and fix FROM clause
	if len(selectClause.From.Tables) != 1 {
		return "", hasAgg, errors.Errorf("stream only supports single table query")
	}

	tableName, ok := getSourceTableName(query)
	if !ok {
		return "", hasAgg, errors.Errorf("failed to extract table name from stream query")
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

	return selectClause.String(), hasAgg, nil
}
