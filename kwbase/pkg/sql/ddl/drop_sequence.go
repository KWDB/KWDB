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

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

var _ sql.PlanNode = &dropSequenceNode{}

type dropSequenceNode struct {
	n  *tree.DropSequence
	td []sql.TableToDelete
}

// DropSequence handles DROP SEQUENCE statements
func DropSequence(
	ctx context.Context, p *GenericPlanner, n *tree.DropSequence,
) (sql.PlanNode, error) {
	td := make([]sql.TableToDelete, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.PrepareDropTable(ctx, tn, !n.IfExists, sql.ResolveRequireSequenceDesc, false)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and descriptor does not exist.
			continue
		}

		if depErr := sequenceDependencyError(ctx, p, droppedDesc); depErr != nil {
			return nil, depErr
		}

		td = append(td, sql.TableToDelete{Tn: tn, Desc: droppedDesc})
	}

	if len(td) == 0 {
		return sql.NewZeroNode(nil /* columns */), nil
	}

	return &dropSequenceNode{
		n:  n,
		td: td,
	}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because DROP SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropSequenceNode) ReadingOwnWrites() {}

func (n *dropSequenceNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("sequence"))

	ctx := params.Ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.Desc
		err := params.GetPlanner().DropSequenceImpl(
			ctx, droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()), n.n.DropBehavior,
		)
		if err != nil {
			return err
		}
		// Log a Drop Sequence event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		params.GetPlanner().SetAuditTarget(uint32(droppedDesc.GetID()), droppedDesc.GetName(), nil)
	}
	return nil
}

func (*dropSequenceNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropSequenceNode) Close(context.Context)        {}

// sequenceDependency error returns an error if the given sequence cannot be dropped because
// a table uses it in a DEFAULT expression on one of its columns, or nil if there is no
// such dependency.
func sequenceDependencyError(
	ctx context.Context, p *GenericPlanner, droppedDesc *MutableTableDescriptor,
) error {
	if len(droppedDesc.DependedOnBy) > 0 {
		return pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop sequence %s because other objects depend on it",
			droppedDesc.Name,
		)
	}
	return nil
}

func canRemoveAllTableOwnedSequences(
	ctx context.Context, p *GenericPlanner, desc *MutableTableDescriptor, behavior tree.DropBehavior,
) error {
	for _, col := range desc.Columns {
		err := canRemoveOwnedSequencesImpl(ctx, p, desc, &col, behavior, false /* isColumnDrop */)
		if err != nil {
			return err
		}
	}
	return nil
}

func canRemoveAllColumnOwnedSequences(
	ctx context.Context,
	p *GenericPlanner,
	desc *MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	behavior tree.DropBehavior,
) error {
	return canRemoveOwnedSequencesImpl(ctx, p, desc, col, behavior, true /* isColumnDrop */)
}

func canRemoveOwnedSequencesImpl(
	ctx context.Context,
	p *GenericPlanner,
	desc *MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	behavior tree.DropBehavior,
	isColumnDrop bool,
) error {
	for _, sequenceID := range col.OwnsSequenceIds {
		seqLookup, err := p.LookupTableByID(ctx, sequenceID)
		if err != nil {
			// Special case error swallowing for #50711 and #50781, which can cause a
			// column to own sequences that have been dropped/do not exist.
			if err.Error() == sqlerror.ErrTableDropped.Error() ||
				pgerror.GetPGCode(err) == pgcode.UndefinedTable {
				log.Eventf(ctx, "swallowing error ensuring owned sequences can be removed: %s", err.Error())
				continue
			}
			return err
		}
		seqDesc := seqLookup.Desc
		affectsNoColumns := len(seqDesc.DependedOnBy) == 0
		// It is okay if the sequence is depended on by columns that are being
		// dropped in the same transaction
		canBeSafelyRemoved := len(seqDesc.DependedOnBy) == 1 && seqDesc.DependedOnBy[0].ID == desc.ID
		// If only the column is being dropped, no other columns of the table can
		// depend on that sequence either
		if isColumnDrop {
			canBeSafelyRemoved = canBeSafelyRemoved && len(seqDesc.DependedOnBy[0].ColumnIDs) == 1 &&
				seqDesc.DependedOnBy[0].ColumnIDs[0] == col.ID
		}

		canRemove := affectsNoColumns || canBeSafelyRemoved

		// Once Drop Sequence Cascade actually respects the drop behavior, this
		// check should go away.
		if behavior == tree.DropCascade && !canRemove {
			return unimplemented.NewWithIssue(20965, "DROP SEQUENCE CASCADE is currently unimplemented")
		}
		// If Cascade is not enabled, and more than 1 columns depend on it, and the
		if behavior != tree.DropCascade && !canRemove {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop table %s because other objects depend on it",
				desc.Name,
			)
		}
	}
	return nil
}
