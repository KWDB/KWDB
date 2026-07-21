// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

var errEmptyIndexName = pgerror.New(pgcode.Syntax, "empty index name")

var _ sql.PlanNode = &renameIndexNode{}

type renameIndexNode struct {
	n         *tree.RenameIndex
	tableDesc *MutableTableDescriptor
	idx       *IndexDescriptor
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
func RenameIndex(
	ctx context.Context, p *GenericPlanner, n *tree.RenameIndex,
) (sql.PlanNode, error) {
	_, tableDesc, err := sql.ExpandMutableIndexName(ctx, p, n.Index, !n.IfExists /* requireTable */)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// IfExists specified and table did not exist -- noop.
		return sql.NewZeroNode(nil /* columns */), nil
	}

	idx, _, err := tableDesc.FindIndexByName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Noop.
			return sql.NewZeroNode(nil /* columns */), nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &renameIndexNode{n: n, idx: idx, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameIndexNode) ReadingOwnWrites() {}

func (n *renameIndexNode) StartExec(params RunParams) error {
	p := params.GetPlanner()
	ctx := params.Ctx
	tableDesc := n.tableDesc
	idx := n.idx

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID != idx.ID {
			continue
		}
		return dependentViewRenameError(ctx,
			p, "index", n.n.Index.Index.String(), tableDesc.ParentID, tableRef.ID)
	}

	if n.n.NewName == "" {
		return errEmptyIndexName
	}

	if n.n.Index.Index == n.n.NewName {
		// Noop.
		return nil
	}

	if _, _, err := tableDesc.FindIndexByName(string(n.n.NewName)); err == nil {
		return fmt.Errorf("index name %q already exists", string(n.n.NewName))
	}

	if err := tableDesc.RenameIndexDescriptor(idx, string(n.n.NewName)); err != nil {
		return err
	}

	if err := tableDesc.Validate(ctx, p.Txn()); err != nil {
		return err
	}

	if err := p.WriteSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
		return err
	}

	p.SetAuditTarget(uint32(idx.ID), n.n.NewName.String(), nil)
	return nil
}

func (n *renameIndexNode) Next(RunParams) (bool, error) { return false, nil }
func (n *renameIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameIndexNode) Close(context.Context)        {}
