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

package sql

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// createViewNode represents a CREATE VIEW statement.
var _ PlanNode = &createViewNode{}
var _ PlanNodeReadingOwnWrites = &createViewNode{}

type createViewNode struct {
	viewName *tree.TableName
	// viewQuery contains the view definition, with all table names fully
	// qualified.
	viewQuery   string
	ifNotExists bool
	temporary   bool
	dbDesc      *sqlbase.DatabaseDescriptor
	columns     sqlbase.ResultColumns

	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps     PlanDependencies
	materialized bool
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because CREATE VIEW performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createViewNode) ReadingOwnWrites() {}

func (n *createViewNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("view"))

	viewName := n.viewName.Table()
	isTemporary := n.temporary
	log.VEventf(params.Ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())

	// First check the backrefs and see if any of them are temporary.
	// If so, promote this view to temporary.
	backRefMutables := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor, len(n.planDeps))
	for id, updated := range n.planDeps {
		backRefMutable := params.GetPlanner().Tables().GetUncommittedTableByID(id).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		if n.materialized && (backRefMutable.Temporary || strings.Contains(backRefMutable.ViewQuery, "pg_temp_")) {
			if backRefMutable.IsView() {
				return pgerror.New(pgcode.FeatureNotSupported, "cannot create materialized view using temporary views")
			}
			return pgerror.New(pgcode.FeatureNotSupported, "cannot create materialized view using temporary tables")
		}
		if !isTemporary && backRefMutable.Temporary {
			// This notice is sent from pg, let's imitate.
			params.p.SendClientNotice(params.Ctx,
				pgerror.Noticef(`view "%s" will be a temporary view`, viewName),
			)
			isTemporary = true
		}
		backRefMutables[id] = backRefMutable
	}

	tKey, schemaID, err := GetTableCreateParams(params, n.dbDesc.ID, isTemporary, *n.viewName)
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.ifNotExists {
			return nil
		}
		return err
	}

	schemaName := tree.PublicSchemaName
	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempViewCounter)
		schemaName = tree.Name(params.p.TemporarySchemaName())
	}

	id, err := GenerateUniqueDescID(params.Ctx, params.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := n.dbDesc.GetPrivileges()

	time, err := params.CreationTimeForNewTableDescriptor()
	if err != nil {
		return err
	}

	desc, err := makeViewTableDesc(
		viewName,
		n.viewQuery,
		n.dbDesc.ID,
		schemaID,
		id,
		n.columns,
		time,
		privs,
		&params.p.semaCtx,
		isTemporary,
	)
	if err != nil {
		return err
	}

	if n.materialized {
		if !params.p.EvalContext().TxnImplicit {
			return pgerror.Newf(pgcode.InvalidTransactionState, "cannot create materialized view in an explicit transaction")
		}
		// Ensure all nodes are the correct version.
		if !params.ExecCfg().Settings.Version.IsActive(params.Ctx, clusterversion.VersionMaterializedViews) {
			return pgerror.New(pgcode.FeatureNotSupported,
				"all nodes are not at the correct version to use materialized views")
		}
		// If the view is materialized, set up some more state on the view descriptor.
		// In particular,
		// * mark the descriptor as a materialized view
		// * mark the state as adding and remember the AsOf time to perform
		//   the view query
		// * use AllocateIDs to give the view descriptor a primary key
		desc.IsMaterializedView = true
		desc.State = sqlbase.TableDescriptor_ADD
		desc.CreateAsOfTime = params.GetTxn().ReadTimestamp()
		if err := desc.AllocateIDs(); err != nil {
			return err
		}
	}

	// Collect all the tables/views this view depends on.
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}

	// TODO (lucy): I think this needs a NodeFormatter implementation. For now,
	// do some basic string formatting (not accurate in the general case).
	if err = params.GetPlanner().CreateDescriptorWithID(
		params.Ctx, tKey.Key(), id, &desc, params.EvalContext().Settings,
		fmt.Sprintf("CREATE VIEW %q AS %q", n.viewName.Table(), n.viewQuery),
	); err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for id, updated := range n.planDeps {
		backRefMutable := backRefMutables[id]
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := params.GetPlanner().WriteSchemaChange(
			params.Ctx, backRefMutable, sqlbase.InvalidMutationID, "updating view reference",
		); err != nil {
			return err
		}
	}

	if err := desc.Validate(params.Ctx, params.PlannerTxn()); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	_ = tree.MakeTableNameWithSchema(tree.Name(n.dbDesc.Name), schemaName, tree.Name(n.viewName.Table()))

	params.GetPlanner().SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	return nil
}

func (*createViewNode) Next(RunParams) (bool, error) { return false, nil }
func (*createViewNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createViewNode) Close(ctx context.Context)  {}
