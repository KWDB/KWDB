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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// ShowTriggersColumns is a template for show trigger results
var ShowTriggersColumns = sqlbase.ResultColumns{
	{Name: "trigger_name", Typ: types.String},
	{Name: "trigger_action_time", Typ: types.String},
	{Name: "trigger_event", Typ: types.String},
	{Name: "trigger_order", Typ: types.Int},
	{Name: "on_table", Typ: types.String},
	{Name: "enabled", Typ: types.Bool},
}

// dummy
type showTiggersNode struct {
	_ string
}

// ShowTriggers returns a SHOW TRIGGERS statement. The user must have any
// privilege on the table.
func ShowTriggers(
	ctx context.Context, p *GenericPlanner, n *tree.ShowTriggers,
) (sql.PlanNode, error) {
	// Ensure all nodes are the correct version.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionTrigger) {
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"not all nodes are at the correct version to use Triggers")
	}
	tblName := n.Table.ToTableName()
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tblName, true, sql.ResolveRequireTableDesc,
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
	return sql.NewDelayedNode(
			fmt.Sprintf("SHOW TRIGGERS FROM %v", n.Table),
			ShowTriggersColumns,
			func(ctx context.Context, p *GenericPlanner) (sql.PlanNode, error) {
				triggers := tableDesc.GetTriggers()
				v := p.NewContainerValuesNode(ShowTriggersColumns, len(triggers))
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
					if _, err = v.AddRowToValueNode(ctx, row); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
				return v, nil
			},
			nil),
		nil
}
