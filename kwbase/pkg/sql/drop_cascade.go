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

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// dropCascadeState is used to drop schema cascade.
// Parameters:
// - schemasToDelete: all schema descriptor to be deleted.
// - objectNamesToDelete: all table names in all schemas.
// - td: all table descriptor to be deleted.
// - allTableObjectsToDelete: all descriptor and dependence on them to be deleted.(table/view and so on).
type dropCascadeState struct {
	schemasToDelete         []*sqlbase.ResolvedSchema
	objectNamesToDelete     []ObjectName
	td                      []toDelete
	allTableObjectsToDelete []*sqlbase.MutableTableDescriptor

	droppedNames []string
}

func newDropCascadeState() *dropCascadeState {
	return &dropCascadeState{
		// We ensure droppedNames is not nil when creating the dropCascadeState.
		// This makes it so that data in the event log is at least an empty list,
		// not NULL.
		droppedNames: []string{},
	}
}

// collectObjectsInSchema collects all objects' name in the given schema.
func (d *dropCascadeState) collectObjectsInSchema(
	ctx context.Context, p *planner, db *sqlbase.DatabaseDescriptor, schema *sqlbase.ResolvedSchema,
) error {
	names, err := GetObjectNames(ctx, p.txn, p, db, schema.Name, true /* explicitPrefix */)
	if err != nil {
		return err
	}
	for i := range names {
		d.objectNamesToDelete = append(d.objectNamesToDelete, names[i])
	}
	d.schemasToDelete = append(d.schemasToDelete, schema)
	return nil
}

// resolveCollectedObjects resolves all objects by name and returns its
// descriptor. If the object is not found and flags.required is true,
// an error is returned, otherwise a nil reference is returned.
func (d *dropCascadeState) resolveCollectedObjects(
	ctx context.Context, p *planner, db *sqlbase.DatabaseDescriptor,
) error {
	d.td = make([]toDelete, 0, len(d.objectNamesToDelete))
	// Resolve each of the collected names.
	for i := range d.objectNamesToDelete {
		objName := d.objectNamesToDelete[i]
		// First try looking up objName as a table.
		found, desc, err := p.LookupObject(
			ctx,
			tree.ObjectLookupFlags{
				// Note we set required to be false here in order to not error out
				// if we don't find the object,
				CommonLookupFlags: tree.CommonLookupFlags{Required: false},
				RequireMutable:    true,
				IncludeOffline:    true,
			},
			objName.Catalog(),
			objName.Schema(),
			objName.Table(),
		)
		if err != nil {
			return err
		}
		if found {
			tbDesc, ok := desc.(*sqlbase.MutableTableDescriptor)
			if !ok {
				return errors.AssertionFailedf(
					"descriptor for %q is not MutableTableDescriptor",
					objName.Table(),
				)
			}
			// if the table is offline, it is meaning that the table is being bulk-ingestion.
			// we cannot drop it now.
			if tbDesc.State == sqlbase.TableDescriptor_OFFLINE {
				dbName := db.GetName()
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"cannot drop a database with OFFLINE tables, ensure %s is"+
						" dropped or made public before dropping database %s",
					objName.FQString(), tree.AsString((*tree.Name)(&dbName)))
			}
			if err := p.prepareDropWithTableDesc(ctx, tbDesc); err != nil {
				return err
			}
			// Recursively check permissions on all dependent views, since some may
			// be in different databases.
			for _, ref := range tbDesc.DependedOnBy {
				if err := p.canRemoveDependentView(ctx, tbDesc, ref, tree.DropCascade); err != nil {
					return err
				}
			}
			d.td = append(d.td, toDelete{&objName, tbDesc})
		}
	}

	allObjectsToDelete, implicitDeleteMap, err := p.accumulateAllObjectsToDelete(ctx, d.td)
	if err != nil {
		return err
	}
	d.allTableObjectsToDelete = allObjectsToDelete
	d.td = filterImplicitlyDeletedObjects(d.td, implicitDeleteMap)
	return nil
}

// dropAllCollectedObjects drops all collected objects.
func (d *dropCascadeState) dropAllCollectedObjects(ctx context.Context, p *planner) error {
	// Delete all of the collected tables.
	for _, toDel := range d.td {
		desc := toDel.desc
		var cascadedObjects []string
		var err error
		if desc.IsView() {
			// TODO(knz): The names of dependent dropped views should be qualified here.
			cascadedObjects, err = p.dropViewImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else if desc.IsSequence() {
			err = p.dropSequenceImpl(ctx, desc, false /* queueJob */, "", tree.DropCascade)
		} else {
			// TODO(knz): The names of dependent dropped tables should be qualified here.
			cascadedObjects, err = p.dropTableImpl(ctx, desc, false /* droppingParent */, "", tree.DropCascade)
		}
		if err != nil {
			return err
		}
		d.droppedNames = append(d.droppedNames, cascadedObjects...)
		d.droppedNames = append(d.droppedNames, toDel.tn.FQString())
	}

	return nil
}

// getDroppedTableDetails puts the deleted table id and name into jobspb.DroppedTableDetails to
// create a schema change job.
func (d *dropCascadeState) getDroppedTableDetails() []jobspb.DroppedTableDetails {
	res := make([]jobspb.DroppedTableDetails, len(d.allTableObjectsToDelete))
	for i := range d.allTableObjectsToDelete {
		tbl := d.allTableObjectsToDelete[i]
		res[i] = jobspb.DroppedTableDetails{
			ID:   tbl.ID,
			Name: tbl.Name,
		}
	}
	return res
}
