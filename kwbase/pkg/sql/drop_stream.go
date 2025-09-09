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
)

type dropStreamNode struct {
	streamID   uint64
	streamName tree.Name
	jobID      int64
	tableID    uint64
}

// DropStream creates a drop stream node for exec.
func (p *planner) DropStream(ctx context.Context, n *tree.DropStream) (planNode, error) {
	stream, err := p.loadStreamByName(ctx, n.StreamName)
	if err != nil {
		return nil, err
	}

	if stream == nil {
		if n.IfExists {
			return &dropStreamNode{streamName: "", jobID: 0}, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "stream %q does not exist", n.StreamName)
	}

	// check if the current user is the stream creator or has the system admin role
	if err = p.checkStreamPrivilege(
		ctx, nil, privilege.DROP, privilege.ALL,
		stream.createBy, n.StreamName.String(),
	); err != nil {
		return nil, err
	}

	return &dropStreamNode{streamID: stream.id, streamName: n.StreamName, jobID: stream.jobID, tableID: stream.sourceTableID}, nil
}

func (p *planner) canRemoveAllTableOwnedStreams(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) error {
	return p.checkTableUsedByStream(
		ctx, uint64(desc.ID), desc.Name, nil, behavior == tree.DropCascade)
}

func (p *planner) removeTableStreams(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"load-streams",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id,job_id,source_table_id FROM system.kwdb_streams WHERE source_table_id = $1 OR target_table_id = $1`,
		tableDesc.ID,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		streamID := uint64(tree.MustBeDInt(row[0]))
		jobID := int64(tree.MustBeDInt(row[1]))
		tableID := int64(tree.MustBeDInt(row[2]))

		if err = p.removeStream(ctx, jobID, streamID, uint64(tableID)); err != nil {
			return err
		}
	}

	return nil
}

func (p *planner) removeStream(
	ctx context.Context, jobID int64, streamID uint64, tableID uint64,
) error {
	if jobID != 0 {
		// Close the job by closing the CDC
		p.ExecCfg().CDCCoordinator.StopCDCByLocal(tableID, streamID, cdcpb.TSCDCInstanceType_Stream)
		waitCDCStatusChanged(ctx, p.ExecCfg().CDCCoordinator, tableID, streamID, cdcpb.TSCDCInstanceType_Stream, false)

		job, err := p.execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, p.txn)
		if err != nil {
			return err
		}

		// After CDC is closed, the job status is usually StatusFailed,
		// and if the job status is not StatusFailed, CancelRequested is used to close it,
		// which usually takes 30 seconds.
		if status, err := job.CurrentStatus(ctx); err == nil {
			if status == jobs.StatusRunning || status == jobs.StatusPending {
				_ = p.execCfg.JobRegistry.CancelRequested(ctx, p.txn, jobID)
			}
		}
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_streams WHERE id = $1",
		streamID,
	); err != nil {
		return err
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream-water-mark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_cdc_watermark WHERE table_id = $1 AND task_id = $2 AND task_type = $3",
		tableID,
		streamID,
		cdcpb.TSCDCInstanceType_Stream,
	); err != nil {
		return err
	}

	return nil
}

func (n *dropStreamNode) startExec(params runParams) error {
	if n.streamName == "" {
		return nil
	}

	return params.p.removeStream(params.ctx, n.jobID, n.streamID, n.tableID)
}

func (*dropStreamNode) Next(runParams) (bool, error) { return false, nil }
func (*dropStreamNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropStreamNode) Close(context.Context)        {}
