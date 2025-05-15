// Copyright 2015 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type dropTableNode struct {
	n *tree.DropTable
	// td is a map from table descriptor to toDelete struct, indicating which
	// tables this operation should delete.
	td map[sqlbase.ID]toDelete
}

type toDelete struct {
	tn   *tree.TableName
	desc *sqlbase.MutableTableDescriptor
}

// DropTable drops a table.
// Privileges: DROP on table.
//
//	Notes: postgres allows only the table owner to DROP a table.
//	       mysql requires the DROP privilege on the table.
func (p *planner) DropTable(ctx context.Context, n *tree.DropTable) (planNode, error) {
	td := make(map[sqlbase.ID]toDelete, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, ResolveRequireTableDesc, n.DropBehavior == tree.DropCascade)
		if err != nil {
			//if strings.Contains(err.Error(), "")
			fmt.Println(err.Error())
			return nil, err
		}
		if droppedDesc == nil {
			continue
		}
		// drop multiple time-series tables at once is not supported
		if droppedDesc.IsTSTable() && len(n.Names) > 1 {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "drop multiple time-series tables at once is not supported")
		}

		// dropping timeseries table within explicit txn not supported
		if !p.extendedEvalCtx.TxnImplicit && droppedDesc.IsTSTable() {
			return nil, sqlbase.UnsupportedTSExplicitTxnError()
		}
		// check drop behavior
		if tree.TableType(droppedDesc.TableType) == tree.TemplateTable {
			if n.DropBehavior != tree.DropCascade {
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
					"can not drop template table %s without cascade", droppedDesc.Name)
			}
		}
		if droppedDesc.IsReplTable && droppedDesc.ReplicateFrom != "" {
			return nil, pgerror.Newf(pgcode.ObjectInUse, "can not drop replicated table %s", droppedDesc.Name)
		}
		td[droppedDesc.ID] = toDelete{tn, droppedDesc}
	}

	for _, toDel := range td {
		droppedDesc := toDel.desc
		if droppedDesc.IsTSTable() {
			if err := p.canDropInsTable(ctx, toDel.desc.ID); err != nil {
				return nil, err
			}
			continue
		}
		for i := range droppedDesc.InboundFKs {
			ref := &droppedDesc.InboundFKs[i]
			if _, ok := td[ref.OriginTableID]; !ok {
				if err := p.canRemoveFKBackreference(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		for _, idx := range droppedDesc.AllNonDropIndexes() {
			for _, ref := range idx.InterleavedBy {
				if _, ok := td[ref.Table]; !ok {
					if err := p.canRemoveInterleave(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, ref := range droppedDesc.DependedOnBy {
			if _, ok := td[ref.ID]; !ok {
				if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		if err := p.canRemoveAllTableOwnedSequences(ctx, droppedDesc, n.DropBehavior); err != nil {
			return nil, err
		}

	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropTableNode{
		n:  n,
		td: td,
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because DROP TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *dropTableNode) ReadingOwnWrites() {}

func (n *dropTableNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeDropCounter("table"))

	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}
		// drop time-series table
		//switch toDel.desc.TableType {
		//case tree.InstanceTable:
		//	return params.p.dropInstanceTable(ctx, toDel.tn.Catalog(), toDel.tn.Table(), toDel.desc)
		//case tree.TemplateTable, tree.TimeseriesTable:
		//	return params.p.dropTsTable(ctx, toDel.tn.Catalog(), toDel.desc)
		//}
		droppedViews, err := params.p.dropTableImpl(ctx, droppedDesc, true /* queueJob */, tree.AsStringWithFQNames(n.n, params.Ann()))
		if err != nil {
			params.p.SetAuditTarget(0, droppedDesc.GetName(), droppedViews)
			return err
		}
		params.p.SetAuditTarget(uint32(droppedDesc.ID), droppedDesc.GetName(), droppedViews)
	}

	return nil
}

func (*dropTableNode) Next(runParams) (bool, error) { return false, nil }
func (*dropTableNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropTableNode) Close(context.Context)        {}

// prepareDrop/dropTableImpl is used to drop a single table by
// name, which can result from a DROP TABLE, DROP VIEW, DROP SEQUENCE,
// or DROP DATABASE statement. This method returns the dropped table
// descriptor, to be used for the purpose of logging the event.  The table
// is not actually truncated or deleted synchronously. Instead, it is marked
// as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that,
// courtesy of up_version, the actual truncation and dropping will
// only happen once every node ACKs the version of the descriptor with
// the deleted bit set, meaning the lease manager will not hand out
// new leases for it and existing leases are released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) prepareDrop(
	ctx context.Context,
	name *tree.TableName,
	required bool,
	requiredType ResolveRequiredType,
	includeOffline bool,
) (*sqlbase.MutableTableDescriptor, error) {
	//tableDesc, err := p.ResolveMutableTableDescriptor(ctx, name, required, requiredType)
	//if err != nil {
	//	return nil, err
	//}
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required},
		RequireMutable:    true,
		IncludeOffline:    includeOffline,
	}
	desc, err := resolveExistingObjectImpl(ctx, p, name, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	if _, ok := desc.(*MutableTableDescriptor); !ok {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%s is not a valid relational object", name.TableName)
	}
	tableDesc := desc.(*MutableTableDescriptor)
	if tableDesc == nil {
		return nil, err
	}
	if err := p.prepareDropWithTableDesc(ctx, tableDesc); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

// prepareDropWithTableDesc behaves as prepareDrop, except it assumes the
// table descriptor is already fetched. This is useful for DropDatabase,
// as prepareDrop requires resolving a TableName when DropDatabase already
// has it resolved.
func (p *planner) prepareDropWithTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	return p.CheckPrivilege(ctx, tableDesc, privilege.DROP)
}

// canRemoveFKBackReference returns an error if the input backreference isn't
// allowed to be removed.
func (p *planner) canRemoveFKBackreference(
	ctx context.Context, from string, ref *sqlbase.ForeignKeyConstraint, behavior tree.DropBehavior,
) error {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
	if err != nil {
		return err
	}
	if behavior != tree.DropCascade {
		return fmt.Errorf("%q is referenced by foreign key from table %q", from, table.Name)
	}
	// Check to see whether we're allowed to edit the table that has a
	// foreign key constraint on the table that we're dropping right now.
	return p.CheckPrivilege(ctx, table, privilege.CREATE)
}

func (p *planner) canRemoveInterleave(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior tree.DropBehavior,
) error {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
	if err != nil {
		return err
	}
	// TODO(dan): It's possible to DROP a table that has a child interleave, but
	// some loose ends would have to be addressed. The zone would have to be
	// kept and deleted when the last table in it is removed. Also, the dropped
	// table's descriptor would have to be kept around in some Dropped but
	// non-public state for referential integrity of the `InterleaveDescriptor`
	// pointers.
	if behavior != tree.DropCascade {
		return unimplemented.NewWithIssuef(
			8036, "%q is interleaved by table %q", from, table.Name)
	}
	return p.CheckPrivilege(ctx, table, privilege.CREATE)
}

func (p *planner) removeInterleave(ctx context.Context, ref sqlbase.ForeignKeyReference) error {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
	if err != nil {
		return err
	}
	if table.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	idx, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}
	idx.Interleave.Ancestors = nil
	// No job description, since this is presumably part of some larger schema change.
	return p.writeSchemaChange(ctx, table, sqlbase.InvalidMutationID, "")
}

// dropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior.
func (p *planner) dropTableImpl(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, queueJob bool, jobDesc string,
) ([]string, error) {
	var droppedViews []string
	// Remove foreign key back references from tables that this table has foreign
	// keys to.
	for i := range tableDesc.OutboundFKs {
		ref := &tableDesc.OutboundFKs[i]
		if err := p.removeFKBackReference(ctx, tableDesc, ref); err != nil {
			return droppedViews, err
		}
	}
	tableDesc.OutboundFKs = nil

	// Remove foreign key forward references from tables that have foreign keys
	// to this table.
	for i := range tableDesc.InboundFKs {
		ref := &tableDesc.InboundFKs[i]
		if err := p.removeFKForBackReference(ctx, tableDesc, ref); err != nil {
			return droppedViews, err
		}
	}
	tableDesc.InboundFKs = nil

	// Remove interleave relationships.
	for _, idx := range tableDesc.AllNonDropIndexes() {
		if len(idx.Interleave.Ancestors) > 0 {
			if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
				return droppedViews, err
			}
		}
		for _, ref := range idx.InterleavedBy {
			if err := p.removeInterleave(ctx, ref); err != nil {
				return droppedViews, err
			}
		}
	}

	// Remove sequence dependencies.
	for i := range tableDesc.Columns {
		usesSequenceIds := tableDesc.Columns[i].UsesSequenceIds
		if err := p.removeSequenceDependencies(ctx, tableDesc, &tableDesc.Columns[i]); err != nil {
			return droppedViews, err
		}
		// drop sequence if table use it
		for _, sequenceID := range usesSequenceIds {
			seqDesc, err := p.Tables().getMutableTableVersionByID(ctx, sequenceID, p.txn)
			if err != nil {
				return droppedViews, err
			}
			if !seqDesc.SequenceOpts.IsSerial {
				continue
			}
			if seqDesc.Dropped() {
				continue
			}
			err = p.dropSequenceImpl(
				ctx, seqDesc, queueJob /* queueJob */, jobDesc, tree.DropCascade,
			)
			if err != nil {
				return droppedViews, err
			}
		}
	}

	// Drop sequences that the columns of the table own
	for _, col := range tableDesc.Columns {
		if err := p.dropSequencesOwnedByCol(ctx, &col, queueJob); err != nil {
			return droppedViews, err
		}
	}

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	for _, ref := range tableDesc.DependedOnBy {
		viewDesc, err := p.getViewDescForCascade(
			ctx, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, ref.ID, tree.DropCascade,
		)
		if err != nil {
			return droppedViews, err
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		cascadedViews, err := p.dropViewImpl(ctx, viewDesc, queueJob, "dropping dependent view", tree.DropCascade)
		if err != nil {
			return droppedViews, err
		}
		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, viewDesc.Name)
	}

	err := p.removeTableComment(ctx, tableDesc)
	if err != nil {
		return droppedViews, err
	}

	err = p.initiateDropTable(ctx, tableDesc, queueJob, jobDesc, true /* drain name */)
	return droppedViews, err
}

// drainName when set implies that the name needs to go through the draining
// names process. This parameter is always passed in as true except from
// TRUNCATE which directly deletes the old name to id map and doesn't need
// drain the old map.
func (p *planner) initiateDropTable(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	queueJob bool,
	jobDesc string,
	drainName bool,
) (err error) {
	if tableDesc.Dropped() && !tableDesc.IsTSTable() {
		return errors.Errorf("table %q is already being dropped", tableDesc.Name)
	}

	// If the table is not interleaved , use the delayed GC mechanism to
	// schedule usage of the more efficient ClearRange pathway. ClearRange will
	// only work if the entire hierarchy of interleaved tables are dropped at
	// once, as with ON DELETE CASCADE where the top-level "root" table is
	// dropped.
	//
	// TODO(bram): If interleaved and ON DELETE CASCADE, we will be able to use
	// this faster mechanism.
	if tableDesc.IsTable() && !tableDesc.IsInterleaved() {
		// Get the zone config applying to this table in order to
		// ensure there is a GC TTL.
		_, _, _, err := GetZoneConfigInTxn(
			ctx, p.txn, uint32(tableDesc.ID), &sqlbase.IndexDescriptor{}, "", false, /* getInheritedDefault */
		)
		if err != nil {
			return err
		}

		tableDesc.DropTime = timeutil.Now().UnixNano()
	}

	// Unsplit all manually split ranges in the table so they can be
	// automatically merged by the merge queue.
	//ranges, err := ScanMetaKVs(ctx, p.txn, tableDesc.TableSpan())
	//if err != nil {
	//	return err
	//}
	//for _, r := range ranges {
	//	var desc roachpb.RangeDescriptor
	//	if err := r.ValueProto(&desc); err != nil {
	//		return err
	//	}
	//	if (desc.GetStickyBit() != hlc.Timestamp{}) || desc.GetRangeType() == roachpb.TS_RANGE {
	//		_, keyTableID, _ := keys.DecodeTablePrefix(roachpb.Key(desc.StartKey))
	//		// TODO(replica): When dropping a relational table, avoid sending unSplit requests to the time-series range.
	//		// Modify the usage of default_replica later
	//		if uint64(tableDesc.ID) != keyTableID {
	//			continue
	//		}
	//		// Swallow "key is not the start of a range" errors because it would mean
	//		// that the sticky bit was removed and merged concurrently. DROP TABLE
	//		// should not fail because of this.
	//		if err := p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
	//			return err
	//		}
	//	}
	//}

	tableDesc.State = sqlbase.TableDescriptor_DROP
	if drainName {
		var schemaName string
		parentSchemaID := tableDesc.GetParentSchemaID()
		if tableDesc.Temporary {
			// The automatically created schema(pg_temp_*) of temporary tables
			// is recorded only in the namespace, and has no actual schemaDescriptor.
			// The tempSchemaName is obtained from the namespace using the ID.
			scName, err := getTempTableSchemaNameByID(ctx, p.Txn(), tableDesc.ParentID, parentSchemaID)
			if err != nil {
				return err
			}
			schemaName = scName
		} else {
			scDesc, err := getSchemaDescByID(ctx, p.txn, parentSchemaID)
			if err != nil {
				return err
			}
			if scDesc != nil {
				schemaName = scDesc.Name
			}
		}
		dbDesc, err := getDatabaseDescByID(ctx, p.txn, tableDesc.ParentID)
		if err != nil {
			return err
		}

		// drop ts table which is not a system table.
		if tableDesc.IsTSTable() && tableDesc.ID > keys.MaxReservedDescID {
			if tableDesc.TableType == tree.TemplateTable {
				allChild, err := sqlbase.GetAllInstanceByTmplTableID(ctx, p.txn, tableDesc.ID, true, p.ExecCfg().InternalExecutor)
				if err != nil {
					return err
				}
				for i := range allChild.InstTableIDs {
					// delete instance table
					if err := DropInstanceTable(ctx, p.txn, allChild.InstTableIDs[i], dbDesc.Name, allChild.InstTableNames[i]); err != nil {
						return err
					}
				}
			}
		}

		// Queue up name for draining.
		nameDetails := sqlbase.TableDescriptor_NameInfo{
			ParentID:       tableDesc.ParentID,
			ParentSchemaID: parentSchemaID,
			Name:           tableDesc.Name,
			ParentName:     dbDesc.Name,
			SchemaName:     schemaName,
		}
		tableDesc.DrainingNames = append(tableDesc.DrainingNames, nameDetails)
	}

	// Mark all jobs scheduled for schema changes as successful.
	jobIDs := make(map[int64]struct{})
	var id sqlbase.MutationID
	for _, m := range tableDesc.Mutations {
		if id != m.MutationID {
			id = m.MutationID
			jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc.TableDesc(), id)
			if err != nil {
				return err
			}
			jobIDs[jobID] = struct{}{}
		}
	}
	for jobID := range jobIDs {
		if err := p.ExecCfg().JobRegistry.Succeeded(ctx, p.txn, jobID); err != nil {
			return errors.Wrapf(err,
				"failed to mark job %d as as successful", errors.Safe(jobID))
		}
	}
	// Initiate an immediate schema change. When dropping a table
	// in a session, the data and the descriptor are not deleted.
	// Instead, that is taken care of asynchronously by the schema
	// change manager, which is notified via a system config gossip.
	// The schema change manager will properly schedule deletion of
	// the underlying data when the GC deadline expires.
	return p.writeDropTable(ctx, tableDesc, queueJob, jobDesc)
}

func (p *planner) removeFKForBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint,
) error {
	var originTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.OriginTableID {
		originTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving origin table ID %d: %v", ref.OriginTableID, err)
		}
		originTableDesc = lookup
	}
	if originTableDesc.Dropped() {
		// The origin table is being dropped. No need to modify it further.
		return nil
	}

	if err := removeFKForBackReferenceFromTable(originTableDesc, ref, tableDesc.TableDesc()); err != nil {
		return err
	}
	// No job description, since this is presumably part of some larger schema change.
	return p.writeSchemaChange(ctx, originTableDesc, sqlbase.InvalidMutationID, "")
}

// removeFKBackReferenceFromTable edits the supplied originTableDesc to
// remove the foreign key constraint that corresponds to the supplied
// backreference, which is a member of the supplied referencedTableDesc.
func removeFKForBackReferenceFromTable(
	originTableDesc *sqlbase.MutableTableDescriptor,
	backref *sqlbase.ForeignKeyConstraint,
	referencedTableDesc *sqlbase.TableDescriptor,
) error {
	matchIdx := -1
	for i, fk := range originTableDesc.OutboundFKs {
		if fk.ReferencedTableID == referencedTableDesc.ID && fk.Name == backref.Name {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key constraint "+
			"for backreference %v on table %q", backref, originTableDesc.Name)
	}
	// Delete our match.
	originTableDesc.OutboundFKs = append(
		originTableDesc.OutboundFKs[:matchIdx],
		originTableDesc.OutboundFKs[matchIdx+1:]...)
	return nil
}

// removeFKBackReference removes the FK back reference from the table that is
// referenced by the input constraint.
func (p *planner) removeFKBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint,
) error {
	var referencedTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ref.ReferencedTableID, err)
		}
		referencedTableDesc = lookup
	}
	if referencedTableDesc.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}

	if err := removeFKBackReferenceFromTable(referencedTableDesc, ref.Name, tableDesc.TableDesc()); err != nil {
		return err
	}
	// No job description, since this is presumably part of some larger schema change.
	return p.writeSchemaChange(ctx, referencedTableDesc, sqlbase.InvalidMutationID, "")
}

// removeFKBackReferenceFromTable edits the supplied referencedTableDesc to
// remove the foreign key backreference that corresponds to the supplied fk,
// which is a member of the supplied originTableDesc.
func removeFKBackReferenceFromTable(
	referencedTableDesc *sqlbase.MutableTableDescriptor,
	fkName string,
	originTableDesc *sqlbase.TableDescriptor,
) error {
	matchIdx := -1
	for i, backref := range referencedTableDesc.InboundFKs {
		if backref.OriginTableID == originTableDesc.ID && backref.Name == fkName {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key backreference "+
			"for constraint %q on table %q", fkName, originTableDesc.Name)
	}
	// Delete our match.
	referencedTableDesc.InboundFKs = append(referencedTableDesc.InboundFKs[:matchIdx], referencedTableDesc.InboundFKs[matchIdx+1:]...)
	return nil
}

func (p *planner) removeInterleaveBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	ancestor := idx.Interleave.Ancestors[len(idx.Interleave.Ancestors)-1]
	var t *sqlbase.MutableTableDescriptor
	if ancestor.TableID == tableDesc.ID {
		t = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, ancestor.TableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ancestor.TableID, err)
		}
		t = lookup
	}
	if t.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	targetIdx, err := t.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	foundAncestor := false
	for k, ref := range targetIdx.InterleavedBy {
		if ref.Table == tableDesc.ID && ref.Index == idx.ID {
			if foundAncestor {
				return errors.AssertionFailedf(
					"ancestor entry in %s for %s@%s found more than once", t.Name, tableDesc.Name, idx.Name)
			}
			targetIdx.InterleavedBy = append(targetIdx.InterleavedBy[:k], targetIdx.InterleavedBy[k+1:]...)
			foundAncestor = true
		}
	}
	if t != tableDesc {
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		return p.writeSchemaChange(
			ctx, t, sqlbase.InvalidMutationID, "removing reference for interleaved table",
		)
	}
	return nil
}

// removeMatchingReferences removes all refs from the provided slice that
// match the provided ID, returning the modified slice.
func removeMatchingReferences(
	refs []sqlbase.TableDescriptor_Reference, id sqlbase.ID,
) []sqlbase.TableDescriptor_Reference {
	updatedRefs := refs[:0]
	for _, ref := range refs {
		if ref.ID != id {
			updatedRefs = append(updatedRefs, ref)
		}
	}
	return updatedRefs
}

func (p *planner) removeTableComment(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-table-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.TableCommentType,
		tableDesc.ID)
	if err != nil {
		return err
	}

	_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2",
		keys.ColumnCommentType,
		tableDesc.ID)

	for _, indexDesc := range tableDesc.Indexes {
		err = p.removeIndexComment(
			ctx,
			tableDesc.ID,
			indexDesc.ID)
	}

	return err
}

//func (p *planner) dropTsTable(
//	ctx context.Context, dbName string, desc *sqlbase.MutableTableDescriptor,
//) error {
//	if _, err := api.GetAvailableNodeIDs(ctx); err != nil {
//		return err
//	}
//	log.Infof(ctx, "drop ts table %s 1st txn start, id: %d", desc.Name, desc.ID)
//	var err error
//	desc.TableDesc().State = sqlbase.TableDescriptor_DROP
//	if err = p.writeTableDesc(ctx, desc); err != nil {
//		return err
//	}
//	// Create a Job to perform the second stage of ts DDL.
//	syncDetail := jobspb.SyncMetaCacheDetails{
//		Type:       dropKwdbTsTable,
//		SNTable:    desc.TableDescriptor,
//		DropMEInfo: []sqlbase.DeleteMeMsg{{DatabaseName: dbName, TableName: desc.Name, TableID: uint32(desc.ID), TsVersion: uint32(desc.TsTable.GetTsVersion())}},
//	}
//	jobID, err := p.createTSSchemaChangeJob(ctx, syncDetail, p.stmt.SQL)
//	if err != nil {
//		return err
//	}
//
//	if err := MakeEventLogger(p.ExecCfg()).InsertEventRecord(
//		ctx,
//		p.txn,
//		EventLogDropTable,
//		int32(desc.ID),
//		int32(p.ExecCfg().NodeID.Get()),
//		struct {
//			TableName string
//			TableID   uint32
//			Statement string
//			User      string
//		}{
//			desc.Name,
//			uint32(desc.ID),
//			p.stmt.SQL,
//			p.User()},
//	); err != nil {
//		return err
//	}
//	// Actively commit a transaction, and read/write system table operations
//	// need to be performed before this.
//	if err = p.txn.Commit(ctx); err != nil {
//		return err
//	}
//
//	// After the transaction commits successfully, execute the Job and wait for it to complete.
//	if err = p.ExecCfg().JobRegistry.Run(
//		ctx,
//		p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
//		[]int64{jobID},
//	); err != nil {
//		return err
//	}
//	log.Infof(ctx, "drop ts table %s 1st txn finished, id: %d", desc.Name, desc.ID)
//	return nil
//}

// dropInstanceTable deletes instance table record from related system table.
//func (p *planner) dropInstanceTable(
//	ctx context.Context, dbName string, tableName string, temTable *sqlbase.MutableTableDescriptor,
//) error {
//	insTable, found, err := sqlbase.ResolveInstanceName(ctx, p.txn, dbName, tableName)
//	if err != nil {
//		return err
//	} else if !found {
//		return sqlbase.NewUndefinedTableError(tableName)
//	}
//
//	insTable.State = sqlbase.ChildDesc_DROP
//	if err := writeInstTableMeta(ctx, p.txn, []sqlbase.InstNameSpace{insTable}, true); err != nil {
//		return err
//	}
//
//	// Create a Job to perform the second stage of ts DDL.
//	syncDetail := jobspb.SyncMetaCacheDetails{
//		Type: dropKwdbInsTable,
//		DropMEInfo: []sqlbase.DeleteMeMsg{{
//			DatabaseName: insTable.DBName,
//			TableName:    insTable.InstName,
//			TableID:      uint32(insTable.InstTableID),
//			TemplateID:   uint32(insTable.TmplTableID),
//		}},
//	}
//	jobID, err := p.createTSSchemaChangeJob(ctx, syncDetail, p.stmt.SQL)
//	if err != nil {
//		return err
//	}
//
//	if err := MakeEventLogger(p.ExecCfg()).InsertEventRecord(
//		ctx,
//		p.txn,
//		EventLogDropTable,
//		int32(temTable.ID),
//		int32(p.ExecCfg().NodeID.Get()),
//		struct {
//			TableName  string
//			TableID    uint32
//			TemplateID uint32
//			Statement  string
//			User       string
//		}{
//			insTable.InstName,
//			uint32(insTable.InstTableID),
//			uint32(insTable.TmplTableID),
//			p.stmt.SQL,
//			p.User()},
//	); err != nil {
//		return err
//	}
//
//	// Actively commit a transaction, and read/write system table operations
//	// need to be performed before this.
//	if err := p.txn.Commit(ctx); err != nil {
//		return err
//	}
//
//	// After the transaction commits successfully, execute the Job and wait for it to complete.
//	if err = p.ExecCfg().JobRegistry.Run(
//		ctx,
//		p.ExecCfg().InternalExecutor,
//		[]int64{jobID},
//	); err != nil {
//		return err
//	}
//
//	return nil
//}
