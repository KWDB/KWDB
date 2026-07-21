// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/eventlog"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type commentOnTableNode struct {
	n         *tree.CommentOnTable
	tableDesc *ImmutableTableDescriptor
}

// CommentOnTable add comment on a table.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
func CommentOnTable(
	ctx context.Context, p *GenericPlanner, n *tree.CommentOnTable,
) (sql.PlanNode, error) {
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true, sql.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnTableNode{n: n, tableDesc: tableDesc}, nil
}

func (n *commentOnTableNode) StartExec(params RunParams) error {
	if n.n.Comment != nil {
		_, err := params.PlannerExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			"set-table-comment",
			params.GetTxn(),
			InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.TableCommentType,
			n.tableDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.PlannerExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			"delete-table-comment",
			params.GetTxn(),
			InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.TableCommentType,
			n.tableDesc.ID)
		if err != nil {
			return err
		}
	}

	return eventlog.MakeEventLogger(params.ExecCfg()).InsertEventRecord(
		params.Ctx,
		params.PlannerTxn(),
		eventlog.EventLogCommentOnTable,
		int32(n.tableDesc.ID),
		int32(params.ExtEvalContext().NodeID),
		struct {
			TableName string
			Statement string
			User      string
			Comment   *string
		}{
			params.GetPlanner().ResolvedName(n.n.Table).FQString(),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment,
		},
	)
}

func (n *commentOnTableNode) Next(RunParams) (bool, error) { return false, nil }
func (n *commentOnTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnTableNode) Close(context.Context)        {}
