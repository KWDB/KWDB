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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// UDRTableName represents system.user_defined_routine
var UDRTableName = tree.NewTableName("system", "user_defined_routine")

// dropProcedureNode represents a drop procedure node.
type dropProcedureNode struct {
	n        *tree.DropProcedure
	procDesc *sqlbase.ProcedureDescriptor
}

// DropProcedure gets a dropProcedureNode to get function which should be deleted by name.
func (p *planner) DropProcedure(ctx context.Context, n *tree.DropProcedure) (planNode, error) {
	found, desc, err := ResolveProcedureObject(ctx, p, &n.Name)
	if err != nil {
		return nil, err
	}
	if !found {
		if n.IfExists {
			return newZeroNode(nil /* columns */), nil
		}
		return nil, sqlbase.NewUndefinedProcedureError(&n.Name)
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.DROP); err != nil {
		return nil, err
	}

	return &dropProcedureNode{
		n:        n,
		procDesc: desc,
	}, nil

}

func (d dropProcedureNode) startExec(params runParams) error {
	if err := params.p.dropProcedureImpl(params.ctx, *d.procDesc); err != nil {
		return err
	}
	return nil
}

// dropProcedureImpl does the work of dropping a procedure.
func (p *planner) dropProcedureImpl(
	ctx context.Context, procDesc sqlbase.ProcedureDescriptor,
) error {
	// Remove back-references from the tables/views this view depends on.
	for _, depID := range procDesc.DependsOn {
		dependencyDesc, err := p.Tables().getMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, procDesc.ID)
		// TODO (lucy): have more consistent/informative names for dependent jobs.
		if err := p.writeSchemaChange(
			ctx, dependencyDesc, sqlbase.InvalidMutationID, "removing references for procedure",
		); err != nil {
			return err
		}
	}
	stmt := fmt.Sprintf(
		`DELETE FROM %s WHERE db_id=%d and schema_id=%d and name='%s'`,
		UDRTableName,
		procDesc.DbID,
		procDesc.SchemaID,
		procDesc.Name,
	)

	_, err := p.execCfg.InternalExecutor.Exec(
		ctx,
		"drop procedure",
		p.txn,
		stmt,
	)
	if err != nil {
		return err
	}

	// remove procedure comment
	if err := p.removeProcedureComment(ctx, procDesc.ID); err != nil {
		return err
	}

	// remove procedure cache
	p.execCfg.ProcedureCache.Purge(uint32(procDesc.ID))
	return nil
}

func (d dropProcedureNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (d dropProcedureNode) Values() tree.Datums {
	return tree.Datums{}
}

func (d dropProcedureNode) Close(ctx context.Context) {}

func (p *planner) removeProcedureComment(ctx context.Context, procID sqlbase.ID) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-proc-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.UDRCommentType,
		procID)

	return err
}
