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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

type createProcedureNode struct {
	n        *tree.CreateProcedure
	dbDesc   *sqlbase.DatabaseDescriptor
	scID     sqlbase.ID
	planDeps planDependencies
}

func (n *createProcedureNode) startExec(params runParams) error {
	// Ensure all nodes are the correct version.
	if !params.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionUDR) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"all nodes are not at the correct version to use Stored Procedures")
	}
	// Check permissions on the schema.
	if err := params.p.canCreateOnSchema(params.ctx, n.n.Name.Schema(), n.dbDesc.ID, skipCheckPublicSchema); err != nil {
		return err
	}
	// generate procDesc
	privs := n.dbDesc.GetPrivileges()
	if n.dbDesc.ID == keys.SystemDatabaseID {
		privs = sqlbase.NewDefaultPrivilegeDescriptor()
	}
	desc, err := makeProcDesc(n.dbDesc.ID, n.scID, n.n, privs)
	if err != nil {
		return err
	}
	// assign unique id
	id, err := GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB)
	if err != nil {
		return err
	}
	desc.ID = id
	// fill in dependencies for procDesc
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}
	// update DependsOnBy for all dependencies
	backRefMutables := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor, len(n.planDeps))
	for id, updated := range n.planDeps {
		backRefMutable := params.p.Tables().getUncommittedTableByID(id).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		backRefMutables[id] = backRefMutable
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
		if err := params.p.writeSchemaChange(
			params.ctx, backRefMutable, sqlbase.InvalidMutationID, "updating procedure reference",
		); err != nil {
			return err
		}
	}

	// write system table
	descValue, err := protoutil.Marshal(&desc)
	if err != nil {
		return err
	}
	var ext []byte
	var rows []tree.Datums
	row := tree.Datums{
		tree.NewDString(string(n.n.Name.TableName)),
		tree.NewDInt(tree.DInt(n.dbDesc.ID)),
		tree.NewDInt(tree.DInt(n.scID)),
		tree.NewDBytes(tree.DBytes(descValue)),
		tree.NewDInt(tree.DInt(id)),
		tree.NewDInt(tree.DInt(sqlbase.Procedure)),
		tree.NewDString(params.p.User()),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.NewDInt(tree.DInt(1)),
		tree.DBoolTrue,
		tree.NewDBytes(tree.DBytes(ext)),
	}
	rows = append(rows, row)
	if err := WriteKWDBDesc(params.ctx, params.p.txn, sqlbase.UDRTable, rows, false); err != nil {
		return err
	}

	return nil
}

func (n *createProcedureNode) ReadingOwnWrites() {}

func (n *createProcedureNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *createProcedureNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *createProcedureNode) Close(ctx context.Context) {
}

// makeProcDesc constructs and returns a new procedureDescriptor
func makeProcDesc(
	dbID, schemaID sqlbase.ID, procedure *tree.CreateProcedure, privs *sqlbase.PrivilegeDescriptor,
) (sqlbase.ProcedureDescriptor, error) {
	var params []sqlbase.ProcParam
	for _, param := range procedure.Parameters {
		tmp := sqlbase.ProcParam{
			Name:      string(param.Name),
			Direction: sqlbase.ProcParam_InDirection,
			Type:      *param.Type,
		}
		params = append(params, tmp)
	}

	return sqlbase.ProcedureDescriptor{
		Name:       string(procedure.Name.TableName),
		DbID:       dbID,
		SchemaID:   schemaID,
		Parameters: params,
		ProcBody:   procedure.BodyStr,
		Language:   "SQL",
		// todo: tyh
		Privileges: privs,
	}, nil
}
