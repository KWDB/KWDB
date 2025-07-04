// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type commentOnProcedureNode struct {
	n        *tree.CommentOnProcedure
	procDesc *sqlbase.ProcedureDescriptor
}

// CommentOnProcedure add comment on a procedure.
// Privileges: CREATE on procedure.
func (p *planner) CommentOnProcedure(
	ctx context.Context, n *tree.CommentOnProcedure,
) (planNode, error) {
	found, procDesc, err := ResolveProcedureObject(ctx, p, &n.Name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlbase.NewUndefinedProcedureError(&n.Name)
	}
	if err := p.CheckPrivilege(ctx, procDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnProcedureNode{n: n, procDesc: procDesc}, nil
}

func (n *commentOnProcedureNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-proc-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.UDRCommentType,
			n.procDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-proc-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.UDRCommentType,
			n.procDesc.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *commentOnProcedureNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnProcedureNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnProcedureNode) Close(context.Context)        {}
