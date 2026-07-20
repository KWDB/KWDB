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

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	_ "gitee.com/kwbasedb/kwbase/pkg/stream" // for stream testing
	"github.com/pkg/errors"
)

var _ sql.PlanNode = &alterStreamNode{}

type alterStreamNode struct {
	metadata.StreamMetadata
	n          *tree.AlterStream
	streamOpts func() (map[string]string, error)
	run        streamComputeRun
}

// NewAlterStreamNode creates a new alterStreamNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterStreamNode(
	streamMetadata metadata.StreamMetadata,
	n *tree.AlterStream,
	streamOpts func() (map[string]string, error),
	run streamComputeRun,
) *alterStreamNode {
	return &alterStreamNode{
		StreamMetadata: streamMetadata,
		n:              n,
		streamOpts:     streamOpts,
		run:            run,
	}
}

// NewAlterStreamNode2 creates a new alterStreamNode.
// nolint:unexportedreturn
func NewAlterStreamNode2(
	streamMetadata metadata.StreamMetadata, n *tree.AlterStream,
) *alterStreamNode {
	return &alterStreamNode{
		StreamMetadata: streamMetadata,
		n:              n,
	}
}

// AlterStream creates a alter stream node for exec.
func AlterStream(
	ctx context.Context, p *GenericPlanner, n *tree.AlterStream,
) (sql.PlanNode, error) {
	stream1, err := sql.LoadStreamByName(ctx, p, n.StreamName)
	if err != nil {
		return nil, err
	}
	if stream1 == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "stream %q does not exist", n.StreamName)
	}

	// check if the current user is the stream creator or has the system admin role
	if err = sql.CheckStreamPrivilege(
		ctx, p, nil, privilege.UPDATE, privilege.ALL,
		stream1.CreateBy, n.StreamName.String(),
	); err != nil {
		return nil, err
	}

	var alterNode alterStreamNode
	alterNode.n = n
	alterNode.StreamMetadata = *stream1

	if n.Options != nil {
		streamOpts, err := p.TypeAsStringOpts(n.Options, streamOptionExpectValues)
		if err != nil {
			return nil, err
		}
		alterNode.streamOpts = streamOpts
	}

	return &alterNode, nil
}

func (n *alterStreamNode) StartExec(params RunParams) (err error) {
	originalStatus := n.StreamMetadata.Status

	var job *jobs.Job
	var currentStatus jobs.Status
	if n.JobID != 0 {
		job, _ = params.ExecCfg().JobRegistry.LoadJobWithTxn(params.Ctx, n.JobID, params.PlannerTxn())
	}

	if job != nil {
		currentStatus, err = job.WithTxn(params.PlannerTxn()).CurrentStatus(params.Ctx)
		if err != nil {
			return err
		}
	}
	parameters, err := sqlutil.UnmarshalStreamParameters(n.Parameters)
	if err != nil {
		return err
	}

	if n.streamOpts != nil {
		// new options from ALTER STREAM command
		streamOpts, err := n.streamOpts()
		if err != nil {
			return err
		}

		originOpts := sqlutil.ConvertStreamOptsToMap(&parameters.Options)

		if originalStatus == sqlutil.StreamStatusEnable {
			if len(streamOpts) == 1 && streamOpts[sqlutil.OptEnable] == sqlutil.StreamOptOn {
				return nil
			}
		} else {
			if job != nil && !job.WithTxn(params.PlannerTxn()).CheckTerminalStatus(params.Ctx) {
				return errors.Errorf("stream %q is stopping, current status is %q.", n.Name, currentStatus)
			}

			if len(streamOpts) == 1 && streamOpts[sqlutil.OptEnable] == sqlutil.StreamOptOff {
				return nil
			}
		}

		opts, err := sqlutil.MakeStreamOptions(streamOpts, originOpts)
		if err != nil {
			return err
		}
		parameters.Options = *opts

		if err := sqlutil.CheckStreamOptions(opts, parameters.TargetTable.IsTsTable); err != nil {
			return err
		}
	}

	// ready to alter the stream status or parameters.
	var targetStatus string
	if parameters.Options.Enable == sqlutil.StreamOptOn {
		targetStatus = sqlutil.StreamStatusEnable
	} else {
		targetStatus = sqlutil.StreamStatusDisable
	}

	marshaledStreamParas, err := sqlutil.MarshalStreamParameters(parameters)
	if err != nil {
		return err
	}

	if _, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"write-stream-info",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET parameters=$1,status=$2 WHERE id=$3`,
		marshaledStreamParas, targetStatus, n.StreamMetadata.ID); err != nil {
		return err
	}

	// alter status 'on' to 'off'
	if parameters.Options.Enable == sqlutil.StreamOptOff {
		if job == nil {
			return nil
		}

		// Close the job by closing the CDC
		params.ExecCfg().CDCCoordinator.StopCDCByLocal(
			n.StreamMetadata.SourceTableID, n.StreamMetadata.ID, sqlbase.CDCInstanceType_Stream,
		)
		sql.WaitCDCStatusChanged(params.Ctx,
			params.ExecCfg().CDCCoordinator,
			parameters.SourceTableID,
			n.StreamMetadata.ID,
			sqlbase.CDCInstanceType_Stream,
			false)

		// stop the running stream job
		status, err := job.WithTxn(params.PlannerTxn()).CurrentStatus(params.Ctx)
		if err != nil {
			return err
		}

		// After CDC is closed, the job status is usually StatusFailed,
		// and if the job status is not StatusFailed, CancelRequested is used to close it,
		// which usually takes 30 seconds.
		switch status {
		case jobs.StatusFailed, jobs.StatusSucceeded, jobs.StatusCanceled:
			return nil
		case jobs.StatusRunning, jobs.StatusPending:
			return params.ExecCfg().JobRegistry.CancelRequested(params.Ctx, params.PlannerTxn(), *job.ID())
		default:
			return errors.Errorf("stream %q is stopping, current status is %s.", n.Name, currentStatus)
		}
	}

	targetColTypes, err := sql.ExtractTargetTableInfoForStream(params.Ctx, params.GetPlanner(), &parameters)
	if err != nil {
		return err
	}

	// alter status 'off' to 'on'
	if parameters.Options.Enable == sqlutil.StreamOptOn && targetStatus == sqlutil.StreamStatusEnable {
		if job != nil && !job.WithTxn(params.PlannerTxn()).CheckTerminalStatus(params.Ctx) {
			return errors.Errorf("stream %q is running, current status is %s.", n.Name, currentStatus)
		}

		if err = sql.CheckStreamMax(params.Ctx, params.GetPlanner()); err != nil {
			return err
		}

		jobRecord, err := sql.BuildStreamJobRecord(
			params, n.n.StreamName, n.ID, marshaledStreamParas.String(),
			n.StreamParameters.StreamSink.SQL, &parameters.SourceTable, targetColTypes,
		)
		if err != nil {
			return err
		}

		if _, err := params.ExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			"update-stream-info",
			params.PlannerTxn(),
			InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_streams SET status=$1 WHERE id=$2`,
			"Disable", n.StreamMetadata.ID); err != nil {
			return err
		}

		n.run.resultsCh = make(chan tree.Datums)
		n.run.errCh = make(chan error)
		startCh := make(chan tree.Datums)
		go func() {
			err := sql.CreateAndStartStreamJob(params.Ctx, params.GetPlanner(), startCh, *jobRecord, &n.StreamMetadata)
			select {
			case <-params.Ctx.Done():
			case n.run.errCh <- err:
			}
			close(n.run.errCh)
			close(n.run.resultsCh)
		}()
	}

	return nil
}

func (n *alterStreamNode) Next(params RunParams) (bool, error) {
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

func (*alterStreamNode) Values() tree.Datums { return tree.Datums{} }

func (*alterStreamNode) Close(context.Context) {}
