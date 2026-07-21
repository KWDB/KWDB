// Copyright 2019 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type commentOnIndexNode struct {
	n         *tree.CommentOnIndex
	tableDesc *TableDescriptor
	indexDesc *IndexDescriptor
}

// CommentOnIndex adds a comment on an index.
// Privileges: CREATE on table.
func CommentOnIndex(
	ctx context.Context, p *GenericPlanner, n *tree.CommentOnIndex,
) (sql.PlanNode, error) {
	tableDesc, indexDesc, err := p.GetTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{n: n, tableDesc: tableDesc.TableDesc(), indexDesc: indexDesc}, nil
}

func (n *commentOnIndexNode) StartExec(params RunParams) error {
	if n.n.Comment != nil {
		err := upsertIndexComment(
			params.Ctx, params.GetPlanner(),
			n.tableDesc.ID,
			n.indexDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := sql.RemoveIndexComment(params.Ctx, params.GetPlanner(), n.tableDesc.ID, n.indexDesc.ID)
		if err != nil {
			return err
		}
	}
	return eventlog.MakeEventLogger(params.ExecCfg()).InsertEventRecord(
		params.Ctx,
		params.PlannerTxn(),
		eventlog.EventLogCommentOnIndex,
		int32(n.tableDesc.ID),
		int32(params.ExtEvalContext().NodeID),
		struct {
			TableName string
			IndexName string
			Statement string
			User      string
			Comment   *string
		}{
			n.tableDesc.Name,
			string(n.n.Index.Index),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment},
	)

}

func upsertIndexComment(
	ctx context.Context,
	p *GenericPlanner,
	tableID sqlbase.ID,
	indexID sqlbase.IndexID,
	comment string,
) error {
	_, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"set-index-comment",
		p.Txn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.IndexCommentType,
		tableID,
		indexID,
		comment)

	return err
}

func (n *commentOnIndexNode) Next(RunParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
