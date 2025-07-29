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
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// streamResumer defines the Job of stream computing.
type streamResumer struct {
	job *jobs.Job
}

// Resume is called when the job starts.
func (s *streamResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	streamDetails := s.job.Details().(jobspb.StreamDetails)

	streamID := streamDetails.StreamMetadata.ID
	streamName := streamDetails.StreamMetadata.Name

	streamSpec := &execinfrapb.StreamReaderSpec{
		Metadata:       streamDetails.StreamMetadata,
		JobID:          *s.job.ID(),
		TargetColTypes: streamDetails.TargetTableColTypes,
	}

	originalPlan := phs.(*planner)

	var err error

	parameters, err := sqlutil.ParseStreamParameters(streamDetails.StreamMetadata.Parameters)
	if err != nil {
		return err
	}

	streamOpts, err := sqlutil.ParseStreamOpts(&parameters.Options)
	if err != nil {
		return err
	}

	opts := retry.Options{
		InitialBackoff: 5 * time.Second,
		Multiplier:     2,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     streamOpts.MaxRetries,
	}

	// the initial-startup of stream must send a message to resultsCh
	finishedSetupFn := func() { resultsCh <- tree.Datums(nil) }

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		err = nil

		if err = s.makeAndRunStreamPlan(ctx, originalPlan, streamSpec, finishedSetupFn); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "stopped successfully") {
				log.Infof(ctx, "stream %q is stopped by user: %s", streamName, msg)
				break
			} else {
				log.Errorf(ctx, "stream %q is existed with error: %s, retried times %d", streamName, msg, r.CurrentAttempts())

				// the retrying loop can use 'nil' finishedSetupFn to run the stream physical plan
				finishedSetupFn = nil
				continue
			}
		}
	}

	updateErr := originalPlan.updateStreamRunHistory(context.Background(), s.job, err, streamID)
	if updateErr != nil {
		return errors.Wrap(err, updateErr.Error())
	}

	return err
}

// makeAndRunStreamPlan make and run stream plan and implements the Job interface.
func (s *streamResumer) makeAndRunStreamPlan(
	ctx context.Context,
	originalPlan *planner,
	streamSpec *execinfrapb.StreamReaderSpec,
	finishedSetupFn func(),
) error {
	streamTxn := kv.NewTxn(ctx, originalPlan.execCfg.DB, originalPlan.execCfg.NodeID.Get())
	// make a new local planner
	p, cleanup := newInternalPlanner("stream-physical-plan-builder", streamTxn, security.RootUser,
		&MemoryMetrics{}, originalPlan.execCfg)
	defer cleanup()
	p.SessionData().Database = originalPlan.SessionData().Database
	p.SessionData().SearchPath = originalPlan.SessionData().SearchPath

	dsp := p.DistSQLPlanner()
	// Prepare the planning context.
	evalCtx := p.ExtendedEvalContext()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, streamTxn)
	planCtx.planner = p
	planCtx.streamSpec = streamSpec

	return dsp.planAndRunCreateStream(ctx, evalCtx, planCtx, streamTxn, s.job, finishedSetupFn)
}

//OnFailOrCancel implements the Job interface.
func (s *streamResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	return nil
}

var _ jobs.Resumer = &streamResumer{}

func init() {
	streamResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &streamResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeStream, streamResumerFn)
}

// createAndStartStreamJob starts stream job.
func (p *planner) createAndStartStreamJob(
	ctx context.Context, startCh chan tree.Datums, record jobs.Record, stream *StreamMetadata,
) error {
	job, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, startCh, record)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		return err
	case <-startCh:
	}

	return p.updateStreamRunInfo(ctx, job, err, stream)
}

// updateStreamRunHistory updates the running info of stream job.
func (p *planner) updateStreamRunInfo(
	ctx context.Context, job *jobs.Job, jobError error, stream *StreamMetadata,
) error {
	if stream == nil {
		return errors.Errorf("stream does not exist")
	}

	status, rInfo, err := constructStreamRunInfo(stream, job, jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"stream-update-job-info",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET status=$1, job_id=$2, run_info=$3 WHERE id=$4`,
		status,
		*job.ID(),
		rInfo,
		stream.id); err != nil {
		return err
	}

	return nil
}

// updateStreamRunHistory updates the historical running info of stream job.
func (p *planner) updateStreamRunHistory(
	ctx context.Context, job *jobs.Job, jobError error, streamID uint64,
) error {
	var err error
	stream, err := p.loadStreamByID(ctx, streamID)
	if err != nil {
		return err
	}

	if stream == nil {
		return errors.Errorf("stream with id %d does not exist", streamID)
	}

	status, rInfo, err := constructStreamRunInfo(stream, job, jobError)
	if err != nil {
		return err
	}

	if _, err = p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"stream-update-run-history",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET status=$1, run_info=$2 WHERE id=$3`,
		status,
		rInfo,
		streamID); err != nil {
		return err
	}

	return nil
}

// constructStreamRunInfo constructs running info of stream job.
func constructStreamRunInfo(
	stream *StreamMetadata, job *jobs.Job, jobError error,
) (string, json.JSON, error) {
	status := sqlutil.StreamStatusEnable
	index := -1
	for i, v := range stream.runInfoList {
		if v.JobID == *job.ID() {
			index = i
			break
		}
	}

	if index == -1 {
		stream.runInfoList = append(stream.runInfoList, sqlutil.RunInfo{
			JobID:     *job.ID(),
			StartTime: timeutil.Now().Format(time.RFC3339),
		})

		if len(stream.runInfoList) > sqlutil.StreamMaxRunInfo {
			stream.runInfoList = stream.runInfoList[1:]
		}

		index = len(stream.runInfoList) - 1
	}

	if jobError != nil {
		stream.runInfoList[index].EndTime = timeutil.Now().Format(time.RFC3339)

		if sqlutil.ShouldLogError(jobError) {
			stream.runInfoList[index].ErrorMessage = jobError.Error()
		}
		status = sqlutil.StreamStatusDisable
	}

	rInfo, err := sqlutil.MarshalStreamRunInfo(stream.runInfoList)
	if err != nil {
		return "", nil, err
	}

	return status, rInfo, nil
}

// buildStreamJobRecord builds the jobs.Record of stream.
// It also constructed filter expressions applicable to filtering at the SQL layer.
func buildStreamJobRecord(
	params runParams,
	name tree.Name,
	streamID uint64,
	parameters string,
	sql string,
	sourceTable *cdcpb.CDCTableInfo,
	targetTableColTypes []types.T,
) (*jobs.Record, error) {
	metadata := &cdcpb.StreamMetadata{
		ID:         streamID,
		Name:       string(name),
		Parameters: parameters,
	}

	// extract and fill in the column ids, metrics and tag filter expressions.
	if err := marshalStreamFilter(params, metadata, sourceTable); err != nil {
		return nil, err
	}

	nodeList, err := params.p.extendedEvalCtx.ExecCfg.CDCCoordinator.LiveNodeIDList(params.ctx)
	if err != nil {
		return nil, err
	}

	jobNodeList := make([]int32, len(nodeList))
	for i, nodeID := range nodeList {
		jobNodeList[i] = int32(nodeID)
	}

	return &jobs.Record{
		Description: "stream lifecycle management",
		Statement:   sql,
		Username:    params.p.User(),
		Details: jobspb.StreamDetails{
			StreamMetadata:      metadata,
			ActiveNodeList:      jobNodeList,
			TargetTableColTypes: targetTableColTypes,
		},
		Progress: jobspb.StreamProgress{},
	}, nil
}

// marshalStreamFilter extracts the filter expressions of metrics and tags from the physical plan
// and marshals them to bytes. They will be applied during the data capture phase
func marshalStreamFilter(
	params runParams, metadata *cdcpb.StreamMetadata, sourceTable *cdcpb.CDCTableInfo,
) error {
	// make a new local planner
	plan, cleanup := newInternalPlanner("stream-filter-builder", params.p.txn, params.p.User(),
		&MemoryMetrics{}, params.p.execCfg)
	defer cleanup()

	// The column order in the filter must be consistent with that in the payload
	streamQuery := fmt.Sprintf("SELECT * FROM %s.%s ",
		sourceTable.Database,
		sourceTable.Table)

	if sourceTable.Filter != "" {
		streamQuery += " WHERE " + sourceTable.Filter
	}

	streamSpec := &execinfrapb.StreamReaderSpec{
		Metadata: metadata,
	}

	stmt, err := parser.ParseOne(streamQuery)
	if err != nil {
		return err
	}
	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.SessionData().Database = params.p.CurrentDatabase()
	localPlanner.SessionData().SearchPath = params.p.CurrentSearchPath()

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(params.ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(params.ctx)

	evalCtx := localPlanner.ExtendedEvalContext()
	planCtx := localPlanner.DistSQLPlanner().NewPlanningCtx(params.ctx, evalCtx, params.p.txn)
	planCtx.isLocal = true
	planCtx.isStream = true
	planCtx.cdcCtx = &CDCContext{}
	planCtx.planner = localPlanner
	planCtx.stmtType = tree.Rows
	planCtx.streamSpec = streamSpec

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtx, localPlanner.curPlan.plan)
	if err != nil {
		return err
	}

	localPlanner.DistSQLPlanner().FinalizePlan(planCtx, &physPlan)

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
