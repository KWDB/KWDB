// Copyright 2020 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type createSchemaNode struct {
	n *tree.CreateSchema
}

// NewCreateSchemaNode creates a new createSchemaNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateSchemaNode(n *tree.CreateSchema) *createSchemaNode {
	return &createSchemaNode{
		n: n,
	}
}

func (n *createSchemaNode) StartExec(params RunParams) error {
	return createUserDefinedSchema(params.GetPlanner(), params, n.n)
}

func createUserDefinedSchema(p *GenericPlanner, params RunParams, n *tree.CreateSchema) error {
	// Users can't create a schema without being connected to a DB.
	if p.CurrentDatabase() == "" {
		return pgerror.New(pgcode.UndefinedDatabase,
			"cannot create schema without being connected to a database")
	}

	db, err := p.ResolveUncachedDatabaseByName(params.Ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return err
	}
	if db.EngineType == tree.EngineTypeTimeseries {
		return pgerror.Newf(pgcode.WrongObjectType, "cannot create schema in timeseries database \"%s\"", db.Name)
	}
	// Users cannot create schemas within the system database.
	if db.ID == keys.SystemDatabaseID {
		return pgerror.Newf(pgcode.InvalidObjectDefinition, "cannot create schema in the system database \"%s\"", db.Name)
	}

	if err := p.CheckPrivilege(params.Ctx, db, privilege.CREATE); err != nil {
		return err
	}

	// Ensure there aren't any name collisions.
	exists, err := sql.SchemaExists(params.Ctx, p, db.ID, string(n.Schema))
	if err != nil {
		return err
	}

	if exists {
		if n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", n.Schema)
	}
	// Check validity of the schema name.
	if err := sql.IsSchemaNameValid(string(n.Schema)); err != nil {
		return err
	}

	/*TODO: Version check
	  // Ensure that the cluster version is high enough to create the schema.
	  if !params.PlannerExecCfg().Settings.Version.IsActive(params.Ctx, clusterversion.VersionUserDefinedSchemas) {
	  	return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
	  		`creating schemas requires all nodes to be upgraded to %s`,
	  		clusterversion.VersionByKey(clusterversion.VersionUserDefinedSchemas))
	  }

	  // Check that creation of schemas is enabled.
	  if !p.EvalContext().SessionData.UserDefinedSchemasEnabled {
	  	return pgerror.Newf(pgcode.FeatureNotSupported,
	  		"session variable experimental_enable_user_defined_schemas is set to false, cannot create a schema")
	  }
	*/

	// Create the ID.
	id, err := sql.GenerateUniqueDescID(params.Ctx, p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Create the SchemaDescriptor.
	desc := &sqlbase.SchemaDescriptor{
		ParentID: db.ID,
		Name:     string(n.Schema),
		ID:       id,
		// Inherit the parent privileges.
		Privileges: db.GetPrivileges(),
	}

	// Finally create the schema on disk.
	if err = p.CreateDescriptorWithID(
		params.Ctx,
		sqlbase.NewSchemaKey(db.ID, string(n.Schema)).Key(),
		id,
		desc,
		params.ExecCfg().Settings,
		tree.AsStringWithFQNames(n, params.Ann()),
	); err != nil {
		return err
	}
	// Log Create Schema event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	params.GetPlanner().SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	return nil
}

func (*createSchemaNode) Next(RunParams) (bool, error) { return false, nil }
func (*createSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createSchemaNode) Close(ctx context.Context)  {}

// CreateSchema creates a schema. Currently only works in IF NOT EXISTS mode,
// for schemas that do in fact already exist.
func CreateSchema(
	ctx context.Context, p *GenericPlanner, n *tree.CreateSchema,
) (sql.PlanNode, error) {
	return &createSchemaNode{
		n: n,
	}, nil
}
