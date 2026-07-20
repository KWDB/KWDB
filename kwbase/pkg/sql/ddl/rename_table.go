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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &renameTableNode{}

type renameTableNode struct {
	n            *tree.RenameTable
	oldTn, newTn *tree.TableName
	tableDesc    *MutableTableDescriptor
}

// RenameTable renames the table, view or sequence.
// Privileges: DROP on source table/view/sequence, CREATE on destination database.
//
//	Notes: postgres requires the table owner.
//	       mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//	       on the new table (and does not copy privileges over).
func RenameTable(
	ctx context.Context, p *GenericPlanner, n *tree.RenameTable,
) (sql.PlanNode, error) {
	oldTn := n.Name.ToTableName()
	newTn := n.NewName.ToTableName()
	toRequire := sql.ResolveRequireTableOrViewDesc
	if n.IsView {
		toRequire = sql.ResolveRequireViewDesc
	} else if n.IsSequence {
		toRequire = sql.ResolveRequireSequenceDesc
	}

	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &oldTn, !n.IfExists, toRequire)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// Noop.
		return sql.NewZeroNode(nil /* columns */), nil
	}
	if err := sql.CheckViewMatchesMaterialized(*tableDesc, n.IsView, n.IsMaterialized); err != nil {
		return nil, err
	}

	if tableDesc.IsReplTable && tableDesc.ReplicateFrom != "" {
		return nil, errors.Errorf("Can not rename replicated table %s ", tableDesc.Name)
	}

	if tableDesc.TableType == tree.InstanceTable {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "can not rename instance table")
	}
	if tableDesc.IsTSTable() {
		if sqlbase.ContainsNonAlphaNumSymbol(newTn.String()) {
			return nil, sqlbase.NewTSNameInvalidError(newTn.String())
		}
		if len(newTn.String()) > sqlconst.MaxTSTableNameLength {
			return nil, sqlbase.NewTSNameOutOfLengthError("table", newTn.String(), sqlconst.MaxTSTableNameLength)
		}
		// check whether this table has been published or subscribed
		if err = checkTableRelatedPubsAndSubs(ctx, p, uint64(tableDesc.ID), tableDesc.Name, n.StatOp(), nil); err != nil {
			return nil, err
		}
	}
	if tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
		return nil, sqlbase.NewUndefinedRelationError(&oldTn)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	// Check if any views depend on this table/view. Because our views
	// are currently just stored as strings, they explicitly specify the name
	// of everything they depend on. Rather than trying to rewrite the view's
	// query with the new name, we simply disallow such renames for now.
	if len(tableDesc.DependedOnBy) > 0 {
		return nil, dependentViewRenameError(ctx,
			p, tableDesc.TypeName(), oldTn.String(), tableDesc.ParentID, tableDesc.DependedOnBy[0].ID)
	}

	return &renameTableNode{n: n, oldTn: &oldTn, newTn: &newTn, tableDesc: tableDesc}, nil
}

// ReadingOwnWrites implements the PlanNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameTableNode) ReadingOwnWrites() {}

func (n *renameTableNode) StartExec(params RunParams) error {
	p := params.GetPlanner()
	ctx := params.Ctx
	oldTn := n.oldTn
	newTn := n.newTn
	tableDesc := n.tableDesc
	//temporary table
	tempscname := tree.Name(p.TemporarySchemaName())
	if oldTn.TableNamePrefix.SchemaName == tempscname {
		//if user has explicit database and schema
		if newTn.TableNamePrefix.ExplicitCatalog {
			if newTn.TableNamePrefix.CatalogName != oldTn.TableNamePrefix.CatalogName {
				return pgerror.New(pgcode.FeatureNotSupported, "cannot move objects into or out of temporary schemas")
			}
		}
		if newTn.TableNamePrefix.ExplicitSchema {
			if newTn.TableNamePrefix.SchemaName != tempscname {
				return pgerror.New(pgcode.FeatureNotSupported, "cannot move objects into or out of temporary schemas")
			}
		}
		//not explicit database and schema
		newTn.TableNamePrefix.SchemaName = oldTn.TableNamePrefix.SchemaName
		newTn.TableNamePrefix.ExplicitSchema = true
		tableDesc.Temporary = true
		//if this is set to false, oldTn.TableNamePrefix.SchemaName will
		//be set to public after ResolveUncachedDatabase() function
		oldTn.TableNamePrefix.ExplicitSchema = true
	}
	//permanent table cannot convert to temp table
	if oldTn.TableNamePrefix.SchemaName != tempscname && newTn.TableNamePrefix.SchemaName == tempscname {
		return pgerror.New(pgcode.FeatureNotSupported, "cannot move objects into or out of temporary schemas")
	}

	prevObjPrefix, err := p.ResolveUncachedObjectPrefix(ctx, oldTn)
	if err != nil {
		return err
	}
	prevDbDesc := &prevObjPrefix.Database
	prevSchema := &prevObjPrefix.Schema

	// Check if target database exists.
	// We also look at uncached descriptors here.
	targetObjPrefix, err := p.ResolveUncachedObjectPrefix(ctx, newTn)
	if err != nil {
		return err
	}
	targetDbDesc := &targetObjPrefix.Database
	targetSchema := &targetObjPrefix.Schema
	if targetDbDesc.GetEngineType() == tree.EngineTypeTimeseries {
		if !tableDesc.IsTSTable() && !tableDesc.IsView() {
			return pgerror.Newf(pgcode.WrongObjectType, "can not change relational table %s to ts database %s", tableDesc.Name, targetDbDesc.Name)
		}
		if tableDesc.MaterializedView() {
			return pgerror.Newf(pgcode.FeatureNotSupported, "can not change materialized view %s to ts database %s", tableDesc.Name, targetDbDesc.Name)
		}
	} else {
		if tableDesc.IsTSTable() && !tableDesc.IsView() {
			return pgerror.Newf(pgcode.WrongObjectType, "can not change ts table %s to relational database %s", tableDesc.Name, targetDbDesc.Name)
		}
	}
	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return err
	}

	isNewSchemaTemp, _, err := sql.TemporarySchemaSessionID(newTn.Schema())
	if err != nil {
		return err
	}

	if newTn.ExplicitSchema && !isNewSchemaTemp && tableDesc.Temporary {
		return pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot convert a temporary table to a persistent table during renames",
		)
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Catalog() == newTn.Catalog() &&
		oldTn.Schema() == newTn.Schema() &&
		oldTn.Table() == newTn.Table() {
		// Noop.
		return nil
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.ID
	tableDesc.UnexposedParentSchemaID = targetSchema.ID
	newTbKey := sqlbase.MakeObjectNameKey(ctx, params.ExecCfg().Settings,
		targetDbDesc.GetID(), targetSchema.ID, newTn.Table()).Key()

	if err := tableDesc.Validate(ctx, p.Txn()); err != nil {
		return err
	}

	descID := tableDesc.GetID()
	for i := range tableDesc.GetTriggers() {
		ct, err := reParseCreateTrigger(&tableDesc.GetTriggers()[i])
		if err != nil {
			return err
		}
		ct.TableName = *newTn
		fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
		fmtCtx.FormatNode(ct)
		tableDesc.Triggers[i].TriggerBody = fmtCtx.CloseAndGetString()
	}

	renameDetails := sqlbase.TableDescriptor_NameInfo{
		ParentID:       prevDbDesc.ID,
		ParentSchemaID: prevSchema.ID,
		Name:           oldTn.Table(),
		ParentName:     oldTn.Catalog(),
		SchemaName:     oldTn.Schema(),
	}
	tableDesc.DrainingNames = append(tableDesc.DrainingNames, renameDetails)
	if err := p.WriteSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newTbKey, descID)
	}
	err = sql.WriteDescToBatchWrap(ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		p.EvalContext().Settings, b, descID, tableDesc.TableDesc())
	if err != nil {
		return err
	}

	var exists bool
	// if table is sequence, use LookupObjectID to check
	if tableDesc.IsSequence() {
		exists, _, err = sqlbase.LookupObjectID(
			params.Ctx, params.GetTxn(), targetDbDesc.ID, targetSchema.ID, newTn.Table(),
		)
	} else {
		exists, _, err = sqlbase.LookupPublicTableID(
			params.Ctx, p.ExecCfg().Settings, params.GetTxn(), targetDbDesc.ID, newTn.Table(),
		)
	}

	if err == nil && exists {
		return sqlbase.NewRelationAlreadyExistsError(newTn.Table())
	} else if err != nil {
		return err
	}

	b.CPut(newTbKey, descID, nil)
	if err := p.Txn().Run(ctx, b); err != nil {
		return err
	}
	if tableDesc.TableType == tree.TemplateTable {
		// get all instance table.
		cTbNameSpace, err := sqlbase.LookUpNameSpaceBySTbID(ctx, p.Txn(), descID, p.ExecCfg().InternalExecutor)
		if err != nil {
			return err
		}
		// change old name to new name.
		for i := range cTbNameSpace {
			cTbNameSpace[i].STableName = tableDesc.Name
		}
		// write table system.kwdb_ts_table.
		if err := sql.WriteInstTableMeta(ctx, p.Txn(), cTbNameSpace, true); err != nil {
			return err
		}
	}
	n.n.Name.SetAnnotation(&p.GetSemaCtx().Annotations, oldTn)
	n.n.NewName.SetAnnotation(&p.GetSemaCtx().Annotations, newTn)
	params.GetPlanner().SetAuditTarget(uint32(tableDesc.GetID()), tableDesc.GetName(), nil)
	stmt := tree.AsStringWithFQNames(n.n, params.Ann())
	if tableDesc.IsTSTable() {
		var pipeMetadatas []*metadata.PipeMetadata
		pipeMetadatas, err = sql.CheckTableRelatedPipe(params.Ctx, params.GetPlanner(), uint64(tableDesc.ID), uint64(tableDesc.ParentID))
		if err != nil {
			return err
		}

		if len(pipeMetadatas) > 0 {
			dbName := newTn.Catalog()
			schemaName := newTn.Schema()
			tableName := newTn.Table()
			if err = sql.SendDDLToPipe(
				params, dbName, schemaName, tableName, sqlconst.KafkaMsgKindAlterTable, stmt, pipeMetadatas, true,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *renameTableNode) Next(RunParams) (bool, error) { return false, nil }
func (n *renameTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameTableNode) Close(context.Context)        {}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func dependentViewRenameError(
	ctx context.Context, p *GenericPlanner, typeName, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.Txn(), viewID)
	if err != nil {
		if err.Error() == "descriptor not found" {
			// dependent maybe procedure
			found, proc, err1 := sql.GetProcedureNameByID(ctx, p.ExecCfg().InternalExecutor, p.Txn(), viewID)
			if err1 != nil {
				return err1
			}
			procName := proc.CatalogName + "." + proc.SchemaName + "." + proc.TableName
			if found {
				msg := fmt.Sprintf("cannot rename %s %q because procedure %q depends on it",
					typeName, objName, procName)
				hint := fmt.Sprintf("you can drop %s instead.", procName)
				return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
			}
		}
		return err
	}
	viewName := viewDesc.Name
	if viewDesc.ParentID != parentID {
		var err error
		viewName, err = sql.GetQualifiedTableName(ctx, p.Txn(), viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewID, err)
			msg := fmt.Sprintf("cannot rename %s %q because a view depends on it",
				typeName, objName)
			return sqlbase.NewDependentObjectError(msg)
		}
	}
	msg := fmt.Sprintf("cannot rename %s %q because view %q depends on it",
		typeName, objName, viewName)
	hint := fmt.Sprintf("you can drop %s instead.", viewName)
	return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
}
