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
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

type alterStreamNode struct {
	StreamMetadata
	n          *tree.AlterStream
	streamOpts func() (map[string]string, error)
	run        streamComputeRun
}

// AlterStream creates a alter stream node for exec.
func (p *planner) AlterStream(ctx context.Context, n *tree.AlterStream) (planNode, error) {
	stream, err := p.loadStreamByName(ctx, n.StreamName)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "stream %q does not exist", n.StreamName)
	}

	// check if the current user is the stream creator or has the system admin role
	if err = p.checkStreamPrivilege(
		ctx, nil, privilege.UPDATE, privilege.ALL,
		stream.createBy, n.StreamName.String(),
	); err != nil {
		return nil, err
	}

	var alterNode alterStreamNode
	alterNode.n = n
	alterNode.StreamMetadata = *stream

	if n.Options != nil {
		streamOpts, err := p.TypeAsStringOpts(n.Options, streamOptionExpectValues)
		if err != nil {
			return nil, err
		}
		alterNode.streamOpts = streamOpts
	}

	return &alterNode, nil
}

func (n *alterStreamNode) startExec(params runParams) (err error) {
	originalStatus := n.StreamMetadata.status

	var job *jobs.Job
	var currentStatus jobs.Status
	if n.jobID != 0 {
		job, _ = params.p.execCfg.JobRegistry.LoadJob(params.ctx, n.jobID)
	}

	if job != nil {
		currentStatus, err = job.CurrentStatus(params.ctx)
		if err != nil {
			return err
		}
	}
	parameters, err := sqlutil.UnmarshalStreamParameters(n.parameters)
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
			if job != nil && !job.CheckTerminalStatus(params.ctx) {
				return errors.Errorf("stream %q is stopping, current status is %q.", n.name, currentStatus)
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
		params.ctx,
		"write-stream-info",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_streams SET parameters=$1,status=$2 WHERE id=$3`,
		marshaledStreamParas, targetStatus, n.StreamMetadata.id); err != nil {
		return err
	}

	// alter status 'on' to 'off'
	if parameters.Options.Enable == sqlutil.StreamOptOff {
		params.ExecCfg().CDCCoordinator.StopCDCByLocal(
			n.StreamMetadata.sourceTableID, n.StreamMetadata.id, cdcpb.TSCDCInstanceType_Stream,
		)
		if job == nil {
			return nil
		}
		if job.CheckTerminalStatus(params.ctx) {
			return nil
		}

		// stop the running stream job
		if !job.CheckTerminalStatus(params.ctx) {
			err = params.p.execCfg.JobRegistry.CancelRequested(params.ctx, params.p.txn, *job.ID())
			if err != nil {
				return err
			}
		} else {
			return errors.Errorf("stream %q is stopping, current status is %s.", n.name, currentStatus)
		}
	}

	targetColTypes, err := params.p.extractTargetTableInfoForStream(params.ctx, &parameters)
	if err != nil {
		return err
	}

	// alter status 'off' to 'on'
	if parameters.Options.Enable == sqlutil.StreamOptOn {
		if job != nil && !job.CheckTerminalStatus(params.ctx) {
			return errors.Errorf("stream %q is running, current status is %s.", n.name, currentStatus)
		}

		if err = params.p.checkStreamMax(params.ctx); err != nil {
			return err
		}

		jobRecord, err := buildStreamJobRecord(
			params, n.n.StreamName, n.id, marshaledStreamParas.String(),
			n.streamParameters.StreamSink.SQL, &parameters.SourceTable, targetColTypes,
		)
		if err != nil {
			return err
		}

		if _, err := params.ExecCfg().InternalExecutor.ExecEx(
			params.ctx,
			"update-stream-info",
			params.p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_streams SET status=$1 WHERE id=$2`,
			"Disable", n.StreamMetadata.id); err != nil {
			return err
		}

		n.run.resultsCh = make(chan tree.Datums)
		n.run.errCh = make(chan error)
		startCh := make(chan tree.Datums)
		go func() {
			err := params.p.createAndStartStreamJob(params.ctx, startCh, *jobRecord, &n.StreamMetadata)
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

func (n *alterStreamNode) Next(params runParams) (bool, error) {
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

func (*alterStreamNode) Values() tree.Datums { return tree.Datums{} }

func (*alterStreamNode) Close(context.Context) {}
