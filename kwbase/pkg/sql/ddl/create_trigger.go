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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

var _ sql.PlanNode = &createTriggerNode{}
var _ sql.PlanNodeReadingOwnWrites = &createTriggerNode{}

type createTriggerNode struct {
	n *tree.CreateTrigger
}

// NewCreateTriggerNode creates a new createTriggerNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateTriggerNode(n *tree.CreateTrigger) *createTriggerNode {
	return &createTriggerNode{
		n: n,
	}
}

func (n *createTriggerNode) StartExec(params RunParams) error {
	// Ensure all nodes are the correct version.
	if !params.ExecCfg().Settings.Version.IsActive(params.Ctx, clusterversion.VersionTrigger) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"not all nodes are at the correct version to use TRIGGER")
	}

	// resolve trigger bounded table
	tableDesc, err := params.GetPlanner().ResolveMutableTableDescriptor(
		params.Ctx, &n.n.TableName, true, sql.ResolveRequireTableDesc,
	)
	if err != nil {
		return err
	}

	// check table type
	if !tableDesc.IsPhysicalTable() || tableDesc.IsReplTable ||
		tableDesc.Temporary || tableDesc.IsTSTable() {
		return pgerror.Newf(pgcode.InvalidObjectDefinition, "object \"%s\" cannot be bound to a trigger", tableDesc.Name)
	}

	// check existed trigger with same name
	for _, trig := range tableDesc.Triggers {
		if string(n.n.Name) == trig.Name {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "trigger \"%s\" already exists on table %s", n.n.Name, n.n.TableName.Table())
		}
	}
	// create triggerDesc, add it to tableDesc and reorder all triggers if necessary
	trigDesc := makeTriggerDesc(n.n, tableDesc.NextTriggerID)
	if err := tableDesc.AddTriggerDesc(trigDesc, n.n.Order); err != nil {
		return err
	}
	tableDesc.NextTriggerID++

	if err := params.GetPlanner().WriteSchemaChange(
		params.Ctx, tableDesc, sqlbase.InvalidMutationID, "create trigger",
	); err != nil {
		return err
	}
	return nil
}

func (n *createTriggerNode) ReadingOwnWrites() {}

func (n *createTriggerNode) Next(params RunParams) (bool, error) {
	return false, nil
}

func (n *createTriggerNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *createTriggerNode) Close(ctx context.Context) {
}

func makeTriggerDesc(ct *tree.CreateTrigger, trigID sqlbase.ID) *sqlbase.TriggerDescriptor {
	return &sqlbase.TriggerDescriptor{
		ID:          trigID,
		Name:        string(ct.Name),
		ActionTime:  sqlbase.TriggerActionTime(ct.ActionTime),
		Event:       sqlbase.TriggerEvent(ct.Event),
		ForEachRow:  true,
		TriggerBody: ct.BodyStr,
		Enabled:     true,
	}
}
