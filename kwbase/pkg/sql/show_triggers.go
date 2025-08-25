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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

var showTriggersColumns = sqlbase.ResultColumns{
	{Name: "trigger_name", Typ: types.String},
	{Name: "trigger_action_time", Typ: types.String},
	{Name: "trigger_event", Typ: types.String},
	{Name: "trigger_order", Typ: types.Int},
	{Name: "on_table", Typ: types.String},
	{Name: "enabled", Typ: types.Bool},
}

// ShowTriggers returns a SHOW TRIGGERS statement. The user must have any
// privilege on the table.
func (p *planner) ShowTriggers(ctx context.Context, n *tree.ShowTriggers) (planNode, error) {
	// Ensure all nodes are the correct version.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionTrigger) {
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"not all nodes are at the correct version to use Triggers")
	}
	tblName := n.Table.ToTableName()
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tblName, true, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}
	if err = p.CheckAnyPrivilege(ctx, tableDesc); err != nil {
		return nil, err
	}
	return &delayedNode{
		name:    fmt.Sprintf("SHOW TRIGGERS FROM %v", n.Table),
		columns: showTriggersColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			triggers := tableDesc.GetTriggers()
			v := p.newContainerValuesNode(showTriggersColumns, len(triggers))
			for _, trigger := range triggers {
				triggerName := tree.Name(trigger.Name)
				row := tree.Datums{
					tree.NewDString(triggerName.String()),
					tree.NewDString(trigger.ActionTime.String()),
					tree.NewDString(trigger.Event.String()),
					tree.NewDInt(tree.DInt(trigger.TriggerOrder)),
					tree.NewDString(tableDesc.Name),
					tree.MakeDBool(tree.DBool(trigger.Enabled)),
				}
				if _, err = v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
