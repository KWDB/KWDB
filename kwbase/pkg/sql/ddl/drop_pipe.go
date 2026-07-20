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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

var _ sql.PlanNode = &dropPipeNode{}

type dropPipeNode struct {
	pipeID   uint64
	pipeName tree.Name
	meta     *metadata.PipeMetadata
}

// DropPipe creates a drop pipe node for exec.
func DropPipe(ctx context.Context, p *GenericPlanner, n *tree.DropPipe) (sql.PlanNode, error) {
	pipeInfo, err := sql.LoadPipeByName(ctx, p, n.PipeName)
	if err != nil {
		return nil, err
	}

	if pipeInfo == nil {
		if n.IfExists {
			return &dropPipeNode{pipeName: ""}, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "pipe %q does not exist", n.PipeName)
	}

	if err = sql.CheckPipePrivilege(ctx, p, nil, privilege.DROP, pipeInfo); err != nil {
		return nil, err
	}

	return &dropPipeNode{pipeID: pipeInfo.ID, pipeName: n.PipeName, meta: pipeInfo}, nil
}

//// canRemoveAllTableOwnedPipes checks whether the specified table is used by pipe.
//// In DROP DATABASE CASCADE, it returns an error if the specified table is used by more than one pipe.
//// In DROP DATABASE without CASCADE, it will return an error if the specified table is used
//// by	pipe, stream, subscription, and so on.
//func (p *GenericPlanner) canRemoveAllTableOwnedPipes(
//	ctx context.Context, desc *MutableTableDescriptor, behavior tree.DropBehavior,
//) error {
//	if !desc.IsTSTable() {
//		return nil
//	}
//
//	if behavior == tree.DropCascade {
//		if err := p.checkTableHasRelPipes(ctx, uint64(desc.ID)); err != nil {
//			return pgerror.Wrapf(err, pgcode.ObjectInUse, "relation %q is used by pipe", desc.Name)
//		}
//
//		return nil
//	}
//
//	if err := p.checkTableUsedByCDC(ctx, uint64(desc.ID), nil); err != nil {
//		return pgerror.Wrapf(err, pgcode.ObjectInUse, "relation %q is used by pipe", desc.Name)
//	}
//
//	return nil
//}
//
//// checkTableHasRelPipes checks whether the specified table is used by more than one pipe.
//// It returns an error if the specified table is used by more than one pipe.
//func (p *GenericPlanner) checkTableHasRelPipes(ctx context.Context, tableID uint64) error {
//	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
//		ctx,
//		"query-table-pipes",
//		p.Txn(),
//		InternalExecutorSessionDataOverride{User: security.RootUser},
//		`SELECT p.name,count(*) FROM system.kwdb_pipes p,system.kwdb_cdc_watermark c
//WHERE p.id=c.task_id
//AND p.id in (SELECT a.id FROM system.kwdb_pipes a,system.kwdb_cdc_watermark b WHERE a.id=b.task_id AND b.table_id = $1)
//GROUP BY p.name ORDER BY p.name`,
//		tableID,
//	)
//	if err != nil {
//		return err
//	}
//
//	if rows == nil {
//		return nil
//	}
//
//	for _, row := range rows {
//		if tree.MustBeDInt(row[1]) > 1 {
//			return errors.Errorf("the pipe %s has more than one relation table", tree.MustBeDString(row[0]))
//		}
//	}
//
//	return nil
//}
//
//func (p *GenericPlanner) removePipe(ctx context.Context, jobID int64, meta *PipeMetadata) error {
//	if err := p.removeCDCWatermarks(ctx, sqlbase.CDCInstanceType_Pipe, nil, &meta.id); err != nil {
//		return err
//	}
//
//	if _, err := p.ExecCfg().InternalExecutor.ExecEx(ctx, "delete-pipe-unpush", p.Txn(),
//		InternalExecutorSessionDataOverride{User: security.RootUser},
//		`DELETE FROM system.kwdb_unpush WHERE pusher_id=$1 AND type=$2`,
//		meta.id, sqlbase.CDCInstanceType_Pipe,
//	); err != nil {
//		log.Errorf(ctx, "pipe[%s] sends unsend failed. %v", meta.name, err)
//		return err
//	}
//
//	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
//		ctx,
//		"delete-pipe",
//		p.Txn(),
//		InternalExecutorSessionDataOverride{User: security.RootUser},
//		"DELETE FROM system.kwdb_pipes WHERE id = $1",
//		meta.id,
//	); err != nil {
//		return err
//	}
//
//	if jobID != 0 {
//		if len(meta.paraInfo.Tables) > 0 && p.ExecCfg().CDCCoordinator.HasTask(
//			sqlbase.CDCInstanceType_Pipe, meta.paraInfo.Tables[0].ID, uint64(jobID)) {
//			cdcID := uint64(jobID)
//			for _, item := range meta.paraInfo.Tables {
//				p.ExecCfg().CDCCoordinator.StopCDCByLocal(item.ID, cdcID, sqlbase.CDCInstanceType_Pipe)
//				WaitCDCStatusChanged(
//					ctx,
//					p.ExecCfg().CDCCoordinator,
//					item.ID,
//					cdcID,
//					sqlbase.CDCInstanceType_Pipe,
//					false,
//				)
//			}
//		} else {
//			job, err := p.execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, p.Txn())
//			if err != nil {
//				return err
//			}
//
//			if job.WithTxn(p.Txn()).CheckRunningStatus(ctx) {
//				if err = p.execCfg.JobRegistry.CancelRequested(ctx, p.Txn(), jobID); err != nil {
//					return err
//				}
//			}
//		}
//	}
//
//	return nil
//}

func (n *dropPipeNode) StartExec(params RunParams) error {
	if n.pipeName == "" {
		return nil
	}
	params.GetPlanner().SetAuditTarget(uint32(n.pipeID), n.pipeName.String(), nil)

	for _, table := range n.meta.ParaInfo.Tables {
		// remove it from CDC
		if err := params.GetPlanner().RemoveCDCDescriptorByTableID(
			params.Ctx, table.ID, sqlbase.CDCInstanceType_Pipe, n.pipeID,
		); err != nil {
			// table already be dropped.
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}

			return err
		}
	}

	if err := params.GetPlanner().RemoveCDCWatermarks(
		params.Ctx, sqlbase.CDCInstanceType_Pipe, nil, &n.meta.ID); err != nil {
		return err
	}

	err := sql.RemovePipe(params.Ctx, params.GetPlanner(), n.meta.JobID, n.meta)
	if err != nil {
		return err
	}

	return nil
}

func (*dropPipeNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropPipeNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropPipeNode) Close(context.Context)        {}
