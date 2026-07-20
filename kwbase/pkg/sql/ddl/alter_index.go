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

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &alterIndexNode{}
var _ sql.PlanNodeReadingOwnWrites = &alterIndexNode{}

type alterIndexNode struct {
	n         *tree.AlterIndex
	tableDesc *MutableTableDescriptor
	indexDesc *IndexDescriptor
}

// NewAlterIndexNode creates a new alterIndexNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterIndexNode(
	n *tree.AlterIndex, tableDesc *MutableTableDescriptor, indexDesc *IndexDescriptor,
) *alterIndexNode {
	return &alterIndexNode{
		n:         n,
		tableDesc: tableDesc,
		indexDesc: indexDesc,
	}
}

// AlterIndex applies a schema change on an index.
// Privileges: CREATE on table.
func AlterIndex(ctx context.Context, p *GenericPlanner, n *tree.AlterIndex) (sql.PlanNode, error) {
	tableDesc, indexDesc, err := p.GetTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}
	if tableDesc.IsTSTable() {
		return nil, sqlbase.TSUnsupportedError("alter index")
	}
	// As an artifact of finding the index by name, we get a pointer to a
	// different copy than the one in the tableDesc. To make it easier for the
	// code below, get a pointer to the index descriptor that's actually in
	// tableDesc.
	indexDesc, err = tableDesc.FindIndexByID(indexDesc.ID)
	if err != nil {
		return nil, err
	}
	return &alterIndexNode{n: n, tableDesc: tableDesc, indexDesc: indexDesc}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because ALTER INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterIndexNode) ReadingOwnWrites() {}

func (n *alterIndexNode) StartExec(params RunParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterIndexPartitionBy:
			return errors.AssertionFailedf(
				"unsupported alter index partition: %T, %+v", cmd, t.PartitionBy)
			//telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("index", "partition_by"))
			//partitioning, err := NewPartitioningDescriptor(
			//	params.Ctx,
			//	params.EvalContext(),
			//	n.tableDesc, n.indexDesc, t.PartitionBy)
			//if err != nil {
			//	return err
			//}
			//descriptorChanged = !proto.Equal(
			//	&n.indexDesc.Partitioning,
			//	&partitioning,
			//)
			//err = DeleteRemovedPartitionZoneConfigs(
			//	params.Ctx, params.PlannerTxn(),
			//	n.tableDesc.TableDesc(), n.indexDesc,
			//	&n.indexDesc.Partitioning, &partitioning,
			//	params.ExecCfg(),
			//)
			//if err != nil {
			//	return err
			//}
			//n.indexDesc.Partitioning = partitioning
		default:
			return errors.AssertionFailedf(
				"unsupported alter command: %T", cmd)
		}
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		// Nothing to be done
		return nil
	}
	mutationID := sqlbase.InvalidMutationID
	if addedMutations {
		mutationID = n.tableDesc.ClusterVersion.NextMutationID
	}
	if err := params.GetPlanner().WriteSchemaChange(
		params.Ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Record this index alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	params.GetPlanner().SetAuditTarget(uint32(n.indexDesc.ID), n.indexDesc.Name, nil)
	return nil
}

func (n *alterIndexNode) Next(params RunParams) (bool, error) { return false, nil }
func (n *alterIndexNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterIndexNode) Close(context.Context)               {}
