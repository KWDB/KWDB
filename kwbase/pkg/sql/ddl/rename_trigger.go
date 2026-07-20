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

package ddl

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlerror"
)

type renameTriggerNode struct {
	tab     *MutableTableDescriptor
	idx     int
	oldName string
	newName string
}

// RenameTrigger renames the trigger.
// Privileges: DROP on source table.
func RenameTrigger(
	ctx context.Context, p *GenericPlanner, n *tree.RenameTrigger,
) (sql.PlanNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, sqlerror.ErrEmptyTriggerName
	}
	if n.Table.TableName == "" {
		return nil, sqlerror.ErrNoTable
	}

	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, sql.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}
	var idx int
	found := false
	for i := range tableDesc.Triggers {
		if string(n.Name) == tableDesc.Triggers[i].Name {
			found = true
			idx = i
			break
		}
	}
	if !found {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "trigger \"%s\" on table \"%s\" does not exist", n.Name, n.Table.String())
	} else if n.Name == n.NewName {
		// Noop.
		return sql.NewZeroNode(nil /* columns */), nil
	}
	for i := range tableDesc.Triggers {
		if string(n.NewName) == tableDesc.Triggers[i].Name {
			return nil, pgerror.Newf(pgcode.InvalidObjectDefinition, "trigger \"%s\" already exists on table \"%s\"", n.NewName, n.Table.Table())
		}
	}

	return &renameTriggerNode{
		tab:     tableDesc,
		idx:     idx,
		oldName: string(n.Name),
		newName: string(n.NewName),
	}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
func (n *renameTriggerNode) ReadingOwnWrites() {}

func (n *renameTriggerNode) StartExec(params RunParams) error {
	n.tab.Triggers[n.idx].Name = n.newName
	triDesc := n.tab.Triggers[n.idx]

	// Reparse the ast of CreateTrigger
	ct, err := reParseCreateTrigger(&triDesc)
	if err != nil {
		return err
	}
	// set triggerName = newName
	ct.Name = tree.Name(n.newName)
	fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
	fmtCtx.FormatNode(ct)
	// rewrite TriggerBody
	n.tab.Triggers[n.idx].TriggerBody = fmtCtx.CloseAndGetString()

	err = sql.WriteTableDesc(params.Ctx, params.GetPlanner(), n.tab)
	if err != nil {
		return err
	}
	return nil
}

func (n *renameTriggerNode) Next(RunParams) (bool, error) { return false, nil }
func (n *renameTriggerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTriggerNode) Close(context.Context)        {}
