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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// dropProcedureNode represents a drop procedure node.
type dropTriggerNode struct {
	n         *tree.DropTrigger
	tableDesc *sqlbase.MutableTableDescriptor
}

// DropTrigger returns a dropTriggerNode to represent trigger which should be deleted by name.
func (p *planner) DropTrigger(ctx context.Context, n *tree.DropTrigger) (planNode, error) {
	// Ensure all nodes are the correct version.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionTrigger) {
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"not all nodes are at the correct version to use Triggers")
	}

	// resolve trigger bounded table
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	toDrop := tableDesc.GetTriggerByName(string(n.Name))
	if toDrop == nil {
		if n.IfExists {
			return newZeroNode(nil /* columns */), nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "trigger \"%s\" does not exist", string(n.Name))
	}

	return &dropTriggerNode{
		n:         n,
		tableDesc: tableDesc,
	}, nil

}

func (n dropTriggerNode) startExec(params runParams) error {
	trigIdx := -1
	found := false
	// find trigger which is being dropped
	for i := 0; i < len(n.tableDesc.Triggers); i++ {
		if n.tableDesc.Triggers[i].Name == string(n.n.Name) {
			trigIdx = i
			found = true
			continue
		}
		if found {
			n.tableDesc.Triggers[i].TriggerOrder--
		}
	}
	// remove it from triggers
	n.tableDesc.Triggers = append(n.tableDesc.Triggers[:trigIdx], n.tableDesc.Triggers[trigIdx+1:]...)
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, sqlbase.InvalidMutationID, "drop trigger",
	); err != nil {
		return err
	}
	return nil
}

func (n dropTriggerNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n dropTriggerNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n dropTriggerNode) Close(ctx context.Context) {}
