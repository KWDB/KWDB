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

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var _ sql.PlanNode = &dropDatabaseNode{}

type dropDatabaseNode struct {
	n                  *tree.DropDatabase
	dbDesc             *DatabaseDescriptor
	td                 []sql.TableToDelete
	schemasToDelete    []*sqlbase.ResolvedSchema
	allObjectsToDelete []*MutableTableDescriptor
	delProcs           []sqlbase.ProcedureDescriptor
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//
//	Notes: postgres allows only the database owner to DROP a database.
//	       mysql requires the DROP privileges on the database.
//
// Parameters:
// - n: DropDatabase AST
// Returns:
// - DropDatabase PlanNode
func DropDatabase(
	ctx context.Context, p *GenericPlanner, n *tree.DropDatabase,
) (sql.PlanNode, error) {
	if n.Name == "" {
		return nil, sqlerror.ErrEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), !n.IfExists)
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return sql.NewZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	schemas, err := p.Tables().GetSchemasForDatabase(ctx, p.Txn(), dbDesc.ID)
	if err != nil {
		return nil, err
	}

	// the names of all objects in the target database
	var tableNames sql.TableNames
	procDescs := make([]sqlbase.ProcedureDescriptor, 0)
	schemasToDelete := make([]*sqlbase.ResolvedSchema, 0, len(schemas))
	for _, schema := range schemas {
		_, resSchema, err := p.ResolveUncachedSchemaDescriptor(ctx, dbDesc.ID, schema, true /* required */)
		if err != nil {
			return nil, err
		}
		// check DROP privilege per schema
		switch resSchema.Kind {
		case sqlbase.SchemaUserDefined:
			if err := p.CheckPrivilege(ctx, resSchema.Desc, privilege.DROP); err != nil {
				return nil, err
			}
		}

		schemasToDelete = append(schemasToDelete, &resSchema)
		toAppend, err := sql.GetObjectNames(
			ctx, p.Txn(), p, dbDesc, schema, true, /*explicitPrefix*/
		)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, toAppend...)
		procs, err1 := sql.GetAllProcDescByParentID(ctx, p.Txn(), dbDesc.ID, resSchema.ID)
		if err1 != nil {
			return nil, err1
		}
		procDescs = append(procDescs, procs...)
	}

	if len(tableNames) > 0 || len(procDescs) > 0 {
		switch n.DropBehavior {
		case tree.DropRestrict:
			return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q is not empty and RESTRICT was specified",
				tree.ErrNameStringP(&dbDesc.Name))
		case tree.DropDefault:
			// The default is CASCADE, however be cautious if CASCADE was
			// not specified explicitly.
			if p.SessionData().SafeUpdates {
				return nil, pgerror.DangerousStatementf(
					"DROP DATABASE on non-empty database without explicit CASCADE")
			}
		}
	}

	// checkRelatedPubsAndSubs checks whether the specified database to be dropped has been published or subscribed,
	// or tables in the specified database have been published or subscribed.
	if err = checkRelatedPubsAndSubs(ctx, p, dbDesc); err != nil {
		return nil, err
	}

	toDeletes := make([]sql.TableToDelete, 0, len(tableNames))
	for i, tableName := range tableNames {
		found, desc, err := p.LookupObject(
			ctx,
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{Required: true},
				RequireMutable:    true,
				IncludeOffline:    true,
			},
			tableName.Catalog(),
			tableName.Schema(),
			tableName.Table(),
		)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		tableDesc, ok := desc.(*MutableTableDescriptor)
		if !ok {
			return nil, errors.AssertionFailedf(
				"descriptor for %q is not MutableTableDescriptor",
				tableName.String(),
			)
		}
		if tableDesc.State == sqlbase.TableDescriptor_OFFLINE {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"cannot drop a database with OFFLINE tables, ensure %s is"+
					" dropped or made public before dropping database %s",
				tableName.String(), tree.AsString((*tree.Name)(&dbDesc.Name)))
		}
		if err := sql.PrepareDropWithTableDesc(ctx, p, tableDesc); err != nil {
			return nil, err
		}

		if err := sql.CanRemoveAllTableOwnedStreams(ctx, p, tableDesc, n.DropBehavior); err != nil {
			return nil, err
		}

		if tableDesc.IsTSTable() {
			if err := sql.CheckTableUsedByCDC(ctx, p, uint64(tableDesc.ID), nil); err != nil {
				return nil, err
			}
		}

		// Recursively check permissions on all dependent views, since some may
		// be in different databases.
		for _, ref := range tableDesc.DependedOnBy {
			if err := p.CanRemoveDependentView(ctx, tableDesc, ref, tree.DropCascade); err != nil {
				return nil, err
			}
		}
		toDeletes = append(toDeletes, sql.TableToDelete{&tableNames[i], tableDesc})
	}

	allObjectsToDelete, implicitDeleteMap, err := accumulateAllObjectsToDelete(ctx, p, toDeletes)
	if err != nil {
		return nil, err
	}

	return &dropDatabaseNode{
		n:                  n,
		dbDesc:             dbDesc,
		td:                 filterImplicitlyDeletedObjects(toDeletes, implicitDeleteMap),
		delProcs:           procDescs,
		schemasToDelete:    schemasToDelete,
		allObjectsToDelete: allObjectsToDelete}, nil
}

func (n *dropDatabaseNode) StartExec(params RunParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("database"))
	log.Infof(params.Ctx, "drop database %s start, type: %s, id: %d", n.dbDesc.Name, tree.EngineName(n.dbDesc.EngineType), n.dbDesc.ID)
	// if current database is being dropped, switch session database to defaultdb
	if string(n.n.Name) == params.GetPlanner().SessionData().Database && !params.GetPlanner().SessionData().SafeUpdates {
		params.GetPSessDataMutator().SetDatabase("defaultdb")
	}

	ctx := params.Ctx
	p := params.GetPlanner()
	//if n.dbDesc.EngineType == tree.EngineTypeTimeseries {
	//	// check all nodes are healthy, otherwise DDL is not allowed
	//	if _, err := api.GetAvailableNodeIDs(params.Ctx); err != nil {
	//		return err
	//	}
	//	return p.dropTSDatabase(ctx, *n)
	//}
	tbNameStrings := make([]string, 0, len(n.td))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.td))

	for _, delDesc := range n.allObjectsToDelete {
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: delDesc.Name,
			ID:   delDesc.ID,
		})
	}
	if err := sql.CreateDropDatabaseJob(ctx, p,
		n.dbDesc.ID, droppedTableDetails, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	for i := range n.delProcs {
		procDesc := n.delProcs[i]
		if err := dropProcedureImpl(params.Ctx, params.GetPlanner(), procDesc); err != nil {
			return err
		}
	}

	// When views, sequences, and tables are dropped, don't queue a separate job
	// for each of them, since the single DROP DATABASE job will cover them all.
	for _, toDel := range n.td {
		desc := toDel.Desc
		var cascadedObjects []string
		var err error
		if desc.IsView() {
			// TODO(knz): dependent dropped views should be qualified here.
			cascadedObjects, err = p.DropViewImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else if desc.IsSequence() {
			err = p.DropSequenceImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else {
			// TODO(knz): dependent dropped table names should be qualified here.
			cascadedObjects, err = sql.DropTableImpl(ctx, p, desc, false /* queueJob */, "", n.n.DropBehavior)
		}
		if err != nil {
			return err
		}
		tbNameStrings = append(tbNameStrings, cascadedObjects...)
		tbNameStrings = append(tbNameStrings, toDel.Tn.FQString())
	}

	descKey := sqlbase.MakeDescMetadataKey(n.dbDesc.ID)

	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
	}
	b.Del(descKey)

	for _, schemaToDelete := range n.schemasToDelete {
		if err := sql.DropSchemaImpl(ctx, p, b, n.dbDesc.ID, schemaToDelete); err != nil {
			return err
		}
	}

	err := sqlbase.RemoveDatabaseNamespaceEntry(
		ctx, p.Txn(), n.dbDesc.Name, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	// No job was created because no tables were dropped, so zone config can be
	// immediately removed.
	if len(n.allObjectsToDelete) == 0 {
		zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(n.dbDesc.ID))
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the zone config entry for this database.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	}

	p.Tables().AddUncommittedDatabase(n.dbDesc.Name, n.dbDesc.ID, sqlconst.DbDropped)

	if err := p.Txn().Run(ctx, b); err != nil {
		return err
	}
	if err := removeDbComment(ctx, p, n.dbDesc.ID); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	p.SetAuditTarget(uint32(n.dbDesc.ID), n.n.Name.String(), tbNameStrings)
	log.Infof(params.Ctx, "drop database %s finished, type: %s, id: %d", n.dbDesc.Name, tree.EngineName(n.dbDesc.EngineType), n.dbDesc.ID)
	return nil
}

func (*dropDatabaseNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() tree.Datums          { return tree.Datums{} }

// filterImplicitlyDeletedObjects takes a list of table descriptors and removes
// any descriptor that will be implicitly deleted.
func filterImplicitlyDeletedObjects(
	tables []sql.TableToDelete, implicitDeleteObjects map[sqlbase.ID]*MutableTableDescriptor,
) []sql.TableToDelete {
	filteredDeleteList := make([]sql.TableToDelete, 0, len(tables))
	for _, toDel := range tables {
		if _, found := implicitDeleteObjects[toDel.Desc.ID]; !found {
			filteredDeleteList = append(filteredDeleteList, toDel)
		}
	}
	return filteredDeleteList
}

// accumulateAllObjectsToDelete constructs a list of all the descriptors that
// will be deleted as a side effect of deleting the given objects. Additional
// objects may be deleted because of cascading views or sequence ownership. We
// also return a map of objects that will be "implicitly" deleted so we can
// filter on it later.
func accumulateAllObjectsToDelete(
	ctx context.Context, p *GenericPlanner, objects []sql.TableToDelete,
) ([]*MutableTableDescriptor, map[sqlbase.ID]*MutableTableDescriptor, error) {
	implicitDeleteObjects := make(map[sqlbase.ID]*MutableTableDescriptor)
	for _, toDel := range objects {
		err := accumulateCascadingViews(ctx, p, implicitDeleteObjects, toDel.Desc)
		if err != nil {
			return nil, nil, err
		}
		// Sequences owned by the table will also be implicitly deleted.
		if toDel.Desc.IsTable() {
			err := accumulateOwnedSequences(ctx, p, implicitDeleteObjects, toDel.Desc)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	allObjectsToDelete := make([]*MutableTableDescriptor, 0,
		len(objects)+len(implicitDeleteObjects))
	for _, desc := range implicitDeleteObjects {
		allObjectsToDelete = append(allObjectsToDelete, desc)
	}
	for _, toDel := range objects {
		if _, found := implicitDeleteObjects[toDel.Desc.ID]; !found {
			allObjectsToDelete = append(allObjectsToDelete, toDel.Desc)
		}
	}
	return allObjectsToDelete, implicitDeleteObjects, nil
}

// accumulateOwnedSequences finds all sequences that will be dropped as a result
// of the table referenced by desc being dropped, and adds them to the
// dependentObjects map.
func accumulateOwnedSequences(
	ctx context.Context,
	p *GenericPlanner,
	dependentObjects map[sqlbase.ID]*MutableTableDescriptor,
	desc *MutableTableDescriptor,
) error {
	for colID := range desc.GetColumns() {
		for _, seqID := range desc.GetColumns()[colID].OwnsSequenceIds {
			ownedSeqDesc, err := p.Tables().GetMutableTableVersionByID(ctx, seqID, p.Txn())
			if err != nil {
				// Special case error swallowing for #50711 and #50781, which can
				// cause columns to own sequences that have been dropped/do not
				// exist.
				if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
					log.Infof(ctx,
						"swallowing error for owned sequence that was not found %s", err.Error())
					continue
				}
				return err
			}
			dependentObjects[seqID] = ownedSeqDesc
		}
	}
	return nil
}

// accumulateCascadingViews finds all views that are to be deleted as part
// of a drop database cascade. This is important as KWDB allows cross-database
// references, which means this list can't be constructed by simply scanning
// the namespace table.
func accumulateCascadingViews(
	ctx context.Context,
	p *GenericPlanner,
	dependentObjects map[sqlbase.ID]*MutableTableDescriptor,
	desc *MutableTableDescriptor,
) error {
	for _, ref := range desc.DependedOnBy {
		found, _, err := sql.GetProcedureNameByID(ctx, p.ExecCfg().InternalExecutor, p.Txn(), ref.ID)
		if err != nil {
			return err
		}
		if found {
			continue
		}
		dependentDesc, err := p.Tables().GetMutableTableVersionByID(ctx, ref.ID, p.Txn())
		if err != nil {
			return err
		}
		if !dependentDesc.IsView() {
			continue
		}
		dependentObjects[ref.ID] = dependentDesc
		if err := accumulateCascadingViews(ctx, p, dependentObjects, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}

func removeDbComment(ctx context.Context, p *GenericPlanner, dbID sqlbase.ID) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-db-comment",
		p.Txn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}

//func (p *GenericPlanner) dropTSDatabase(ctx context.Context, n dropDatabaseNode) error {
//	idKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, n.dbDesc.Name)
//	// Set the dbID to InvalidID in system.namespace. This means that database is being dropped.
//	// In this state, it is forbidden to create tables or create database with the same name.
//	err := p.Txn().Put(ctx, idKey.Key(), sqlbase.InvalidID)
//	if err != nil {
//		return err
//	}
//
//	// this struct represents the information required by AE when deleting an object
//	var dropInfoForAE []sqlbase.DeleteMeMsg
//	// tableDescriptors to be deleted in this database
//	var tablesToDel []TableDescriptor
//	cascadedObjects := make([]string, 0, len(n.td))
//	for _, table := range n.td {
//		// change the table state to DROP
//		table.desc.TableDesc().State = sqlbase.TableDescriptor_DROP
//		if err := p.writeTableDesc(ctx, table.desc); err != nil {
//			return err
//		}
//		// build drop info required by AE
//		msg := sqlbase.DeleteMeMsg{
//			DatabaseName: n.dbDesc.Name,
//			TableID:      uint32(table.desc.ID),
//			TableName:    table.desc.Name,
//			TsVersion:    uint32(table.desc.TsTable.GetTsVersion()),
//		}
//		dropInfoForAE = append(dropInfoForAE, msg)
//		tablesToDel = append(tablesToDel, table.desc.TableDescriptor)
//		cascadedObjects = append(cascadedObjects, table.desc.Name)
//	}
//	// Create a Job to perform the second stage of ts DDL.
//	syncDetail := jobspb.SyncMetaCacheDetails{
//		Type:       dropKwdbTsDatabase,
//		Database:   *n.dbDesc,
//		DropMEInfo: dropInfoForAE,
//		DropDBInfo: tablesToDel,
//	}
//	jobID, err := p.CreateTSSchemaChangeJob(ctx, syncDetail, tree.AsStringWithFQNames(n.n, p.EvalContext().Annotations))
//	if err != nil {
//		return err
//	}
//
//	// Actively commit a transaction, and read/write system table operations
//	// need to be performed before this.
//	if err := p.Txn().Commit(ctx); err != nil {
//		return err
//	}
//	// After the transaction commits successfully, execute the Job and wait for it to complete.
//	if err = p.ExecCfg().JobRegistry.Run(
//		ctx,
//		p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
//		[]int64{jobID},
//	); err != nil {
//		return err
//	}
//	p.SetAuditTarget(uint32(n.dbDesc.ID), n.n.Name.String(), cascadedObjects)
//	log.Infof(ctx, "drop database %s finished, type: %s, id: %d", n.dbDesc.Name, tree.EngineName(n.dbDesc.EngineType), n.dbDesc.ID)
//	return nil
//}

// checkRelatedPubsAndSubs checks whether the specified database to be deleted has been published or subscribed,
// or tables in the specified database have been published or subscribed.
// Return error if the specified database has been published or subscribed,
// or tables in the specified database have been published or subscribed.
func checkRelatedPubsAndSubs(
	ctx context.Context, p *GenericPlanner, dbDesc *DatabaseDescriptor,
) error {
	// check whether the database to be deleted has been published,
	// or tables in the specified database have been published.
	pubDBMap, _, err := sql.FetchAllPublishedObjects(ctx, p, dbDesc)
	dbName := dbDesc.GetName()
	if err != nil {
		return err
	}
	if published, ok := pubDBMap[dbName]; ok {
		if published {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"database %q is published and cannot be deleted", tree.ErrNameStringP(&dbName))
		}
		return pgerror.Newf(pgcode.DependentObjectsStillExist,
			"tables in database %q are published and cannot be deleted", tree.ErrNameStringP(&dbName))
	}

	return nil
}
