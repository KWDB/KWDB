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
	"sort"
	"strings"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

//
// This file contains routines for low-level access to stored object
// descriptors, as well as accessors for the table cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var testDisableTableLeases bool

// TestDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestDisableTableLeases() func() {
	testDisableTableLeases = true
	return func() {
		testDisableTableLeases = false
	}
}

func (p *planner) getVirtualTabler() VirtualTabler {
	return p.extendedEvalCtx.VirtualSchemas
}

var errTableAdding = errors.New("table is being added")
var errTableDropped = errors.New("table is being dropped")

type inactiveTableError struct {
	error
}

// FilterTableState inspects the state of a given table and returns an error if
// the state is anything but PUBLIC. The error describes the state of the table.
func FilterTableState(tableDesc *sqlbase.TableDescriptor) error {
	switch tableDesc.State {
	case sqlbase.TableDescriptor_DROP:
		return inactiveTableError{errTableDropped}
	case sqlbase.TableDescriptor_OFFLINE:
		err := errors.Errorf("table %q is offline", tableDesc.Name)
		if tableDesc.OfflineReason != "" {
			err = errors.Errorf("table %q is offline: %s", tableDesc.Name, tableDesc.OfflineReason)
		}
		return inactiveTableError{err}
	case sqlbase.TableDescriptor_ADD:
		if tableDesc.IsTSTable() {
			return sqlbase.NewCreateTSTableError(tableDesc.Name)
		}
		return errTableAdding
	case sqlbase.TableDescriptor_ALTER:
		return nil
	case sqlbase.TableDescriptor_PUBLIC:
		return nil
	default:
		return errors.Errorf("table %s in unknown state: %s", tableDesc.Name, tableDesc.State.String())
	}
}

// An uncommitted database is a database that has been created/dropped
// within the current transaction using the TableCollection. A rename
// is a drop of the old name and creation of the new name.
type uncommittedDatabase struct {
	name    string
	id      sqlbase.ID
	dropped bool
}

// uncommittedSchema means uncommitted schema including name, id and dropped
type uncommittedSchema struct {
	name     string
	id       sqlbase.ID
	parentID sqlbase.ID
	dropped  bool
}

type uncommittedTable struct {
	*sqlbase.MutableTableDescriptor
	*sqlbase.ImmutableTableDescriptor
}

// TableCollection is a collection of tables held by a single session that
// serves SQL requests, or a background job using a table descriptor. The
// collection is cleared using releaseTables() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type TableCollection struct {
	// leaseMgr manages acquiring and releasing per-table leases.
	leaseMgr *LeaseManager
	// A collection of table descriptor valid for the timestamp.
	// They are released once the transaction using them is complete.
	// If the transaction gets pushed and the timestamp changes,
	// the tables are released.
	leasedTables []*sqlbase.ImmutableTableDescriptor

	// Tables modified by the uncommitted transaction affiliated
	// with this TableCollection. This allows a transaction to see
	// its own modifications while bypassing the table lease mechanism.
	// The table lease mechanism will have its own transaction to read
	// the table and will hang waiting for the uncommitted changes to
	// the table. These table descriptors are local to this
	// TableCollection and invisible to other transactions. A dropped
	// table is marked dropped.
	uncommittedTables []uncommittedTable

	// databaseCache is used as a cache for database names.
	// This field is nil when the field is initialized for an internalPlanner.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *databaseCache

	// schemaCache maps {databaseID, schemaName} -> (schemaID, if exists, otherwise nil).
	// TODO(sqlexec): replace with leasing system with custom schemas.
	// This is currently never cleared, because there should only be unique schemas
	// being added for each TableCollection as only temporary schemas can be
	// made, and you cannot read from other schema caches.
	schemaCache sync.Map

	// dbCacheSubscriber is used to block until the node's database cache has been
	// updated when releaseTables is called.
	dbCacheSubscriber dbCacheSubscriber

	// Same as uncommittedTables applying to databases modified within
	// an uncommitted transaction.
	uncommittedDatabases []uncommittedDatabase

	// Same as uncommittedTables applying to schemas modified within
	// an uncommitted transaction.
	uncommittedSchemas []uncommittedSchema

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	allDescriptors []sqlbase.DescriptorProto

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []*sqlbase.DatabaseDescriptor

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are purged at the same time as allDescriptors.
	allSchemasForDatabase map[sqlbase.ID]map[sqlbase.ID]string

	// settings are required to correctly resolve system.namespace accesses in
	// mixed version (19.2/20.1) clusters.
	// TODO(solon): This field could maybe be removed in 20.2.
	settings *cluster.Settings
}

type dbCacheSubscriber interface {
	// waitForCacheState takes a callback depending on the cache state and blocks
	// until the callback declares success. The callback is repeatedly called as
	// the cache is updated.
	waitForCacheState(cond func(*databaseCache) bool)
}

// getMutableTableDescriptor returns a mutable table descriptor.
//
// If flags.required is false, getMutableTableDescriptor() will gracefully
// return a nil descriptor and no error if the table does not exist.
func (tc *TableCollection) getMutableTableDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.MutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on table '%s'", tn)
	}

	refuseFurtherLookup, dbID, err := tc.getUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.databaseCache != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.databaseCache.getDatabaseID(ctx, tc.leaseMgr.db.Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// The following checks only work if the dbID is not invalid.
	if dbID != sqlbase.InvalidID {
		// Resolve the schema to the ID of the schema.
		foundSchema, schemaID, err := tc.resolveSchemaID(ctx, txn, dbID, tn.Schema())
		if err != nil || !foundSchema {
			return nil, err
		}

		if refuseFurtherLookup, table, err := tc.getUncommittedTable(
			dbID,
			schemaID,
			tn,
			flags.Required,
		); refuseFurtherLookup || err != nil {
			return nil, err
		} else if mut := table.MutableTableDescriptor; mut != nil {
			log.VEventf(ctx, 2, "found uncommitted table %d", mut.ID)
			return mut, nil
		}
	}

	phyAccessor := UncachedPhysicalAccessor{}
	obj, err := phyAccessor.GetObjectDesc(ctx, txn, tc.settings, tn, flags)
	if obj == nil {
		return nil, err
	}
	return obj.(*sqlbase.MutableTableDescriptor), err
}

// ResolveSchema attempts to lookup the schema from the schemaCache if it exists,
// otherwise falling back to a database lookup.
func (tc *TableCollection) ResolveSchema(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaName string,
) (bool, sqlbase.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, sqlbase.ResolvedSchema{ID: keys.PublicSchemaID, Kind: sqlbase.SchemaPublic, Name: schemaName}, nil
	}

	type schemaCacheKey struct {
		dbID       sqlbase.ID
		schemaName string
	}
	// TODO: schemaCache currently cannot guarantee consistency and is temporarily deprecated
	// key := schemaCacheKey{dbID: dbID, schemaName: schemaName}
	// First lookup the cache.
	// TODO (SQLSchema): This should look into the lease manager.
	//if val, ok := tc.schemaCache.Load(key); ok {
	//	return true, val.(sqlbase.ResolvedSchema), nil
	//}

	// Next, try lookup the result from KV, storing and returning the value.
	exists, resolved, err := (UncachedPhysicalAccessor{}).GetSchema(ctx, txn, dbID, schemaName)
	if err != nil || !exists {
		return exists, sqlbase.ResolvedSchema{}, err
	}

	//tc.schemaCache.Store(key, resolved)
	return exists, resolved, err
}

// resolveSchemaID attempts to lookup the schema from the schemaCache if it exists,
// otherwise falling back to a database lookup.
func (tc *TableCollection) resolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaName string,
) (bool, sqlbase.ID, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	type schemaCacheKey struct {
		dbID       sqlbase.ID
		schemaName string
	}

	// TODO: schemaCache currently cannot guarantee consistency and is temporarily deprecated
	//key := schemaCacheKey{dbID: dbID, schemaName: schemaName}
	// First lookup the cache.
	//if val, ok := tc.schemaCache.Load(key); ok {
	//	return true, val.(sqlbase.ID), nil
	//}

	// Next, try lookup the result from KV, storing and returning the value.
	exists, schemaID, err := resolveSchemaID(ctx, txn, dbID, schemaName)
	if err != nil || !exists {
		return exists, schemaID, err
	}
	//tc.schemaCache.Store(key, schemaID)
	return exists, schemaID, err
}

// GetTableVersion is for external calls
func (tc *TableCollection) GetTableVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	return tc.getTableVersion(ctx, txn, tn, flags)
}

// getTableVersion returns a table descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.releaseTables().
//
// If flags.required is false, getTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// It might also add a transaction deadline to the transaction that is
// enforced at the KV layer to ensure that the transaction doesn't violate
// the validity window of the table descriptor version returned.
func (tc *TableCollection) getTableVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on table '%s'", tn)
	}

	readTableFromStore := func() (*sqlbase.ImmutableTableDescriptor, error) {
		phyAccessor := UncachedPhysicalAccessor{}
		obj, err := phyAccessor.GetObjectDesc(ctx, txn, tc.settings, tn, flags)
		if obj == nil {
			return nil, err
		}
		return obj.(*sqlbase.ImmutableTableDescriptor), err
	}

	refuseFurtherLookup, dbID, err := tc.getUncommittedDatabaseID(tn.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.databaseCache != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.databaseCache.getDatabaseID(ctx, tc.leaseMgr.db.Txn, tn.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// If at this point we have an InvalidID, we should immediately try read from store.
	if dbID == sqlbase.InvalidID {
		return readTableFromStore()
	}

	// Resolve the schema to the ID of the schema.
	foundSchema, schemaID, err := tc.resolveSchemaID(ctx, txn, dbID, tn.Schema())
	if err != nil || !foundSchema {
		return nil, err
	}

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except the role-members-table.
	avoidCache := flags.AvoidCached || testDisableTableLeases ||
		(tn.Catalog() == sqlbase.SystemDB.Name && tn.TableName.String() != sqlbase.RoleMembersTable.Name)

	if refuseFurtherLookup, table, err := tc.getUncommittedTable(
		dbID,
		schemaID,
		tn,
		flags.Required,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := table.ImmutableTableDescriptor; immut != nil {
		// If not forcing to resolve using KV, tables being added aren't visible.
		if immut.Adding() && !avoidCache {
			err := errTableAdding
			if !flags.Required {
				err = nil
			}
			return nil, err
		}

		log.VEventf(ctx, 2, "found uncommitted table %d", immut.ID)
		return immut, nil
	}

	if avoidCache {
		return readTableFromStore()
	}

	// First, look to see if we already have the table.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	for _, table := range tc.leasedTables {
		if nameMatchesTable(&table.TableDescriptor, dbID, schemaID, tn.Table()) {
			log.VEventf(ctx, 2, "found table in table collection for table '%s'", tn)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	table, expiration, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, dbID, schemaID, tn.Table())
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if _, ok := err.(inactiveTableError); ok || err == sqlbase.ErrDescriptorNotFound {
			return readTableFromStore()
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad table for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", tn)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use ReadTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil
}

// getTableVersionByID is a by-ID variant of getTableVersion (i.e. uses same cache).
func (tc *TableCollection) getTableVersionByID(
	ctx context.Context, txn *kv.Txn, tableID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting table on table ID %d", tableID)

	if flags.AvoidCached || testDisableTableLeases {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return nil, err
		}
		if err := FilterTableState(table); err != nil {
			return nil, err
		}
		return sqlbase.NewImmutableTableDescriptor(*table), nil
	}

	for _, table := range tc.uncommittedTables {
		if immut := table.ImmutableTableDescriptor; immut.ID == tableID {
			log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
			if immut.Dropped() {
				return nil, sqlbase.NewUndefinedRelationError(
					tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("<id=%d>", tableID))),
				)
			}
			return immut, nil
		}
	}

	// First, look to see if we already have the table -- including those
	// via `getTableVersion`.
	for _, table := range tc.leasedTables {
		if table.ID == tableID {
			log.VEventf(ctx, 2, "found table %d in table cache", tableID)
			return table, nil
		}
	}

	readTimestamp := txn.ReadTimestamp()
	table, expiration, err := tc.leaseMgr.Acquire(ctx, readTimestamp, tableID)
	if err != nil {
		if err == sqlbase.ErrDescriptorNotFound {
			// Transform the descriptor error into an error that references the
			// table's ID.
			return nil, sqlbase.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad table for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedTables = append(tc.leasedTables, table)
	log.VEventf(ctx, 2, "added table '%s' to table collection", table.Name)

	// If the table we just acquired expires before the txn's deadline, reduce
	// the deadline. We use ReadTimestamp() that doesn't return the commit timestamp,
	// so we need to set a deadline on the transaction to prevent it from committing
	// beyond the table version expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return table, nil
}

// GetObjectDesc implements the SchemaAccessor interface.
func (tc *TableCollection) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	name *tree.TableName,
	found bool,
	user string,
	cfg *ExecutorConfig,
	isAudit bool,
) (bool, uint32, uint32, *[]sqlbase.ColumnDescriptor, uint32, error) {
	var flags tree.ObjectLookupFlags
	flags.Required = false
	table, err := tc.getTableVersion(ctx, txn, name, flags)
	if table == nil || err != nil {
		return false, 0, 0, nil, 0, nil
	}
	if isAudit {
		return false, 0, 0, nil, uint32(table.TableDesc().Version), nil
	}
	var pp planner
	pp.txn = txn
	pp.execCfg = cfg
	pp.curPlan = planTop{}
	var sd sessiondata.SessionData
	sd.User = user
	pp.avoidCachedDescriptors = true
	pp.extendedEvalCtx = extendedEvalContext{
		EvalContext: tree.EvalContext{
			SessionData:      &sd,
			InternalExecutor: cfg.InternalExecutor,
			Settings:         cfg.Settings,
		},
		ExecCfg:         cfg,
		schemaAccessors: newSchemaInterface(tc, cfg.VirtualSchemas),
	}
	err = pp.CheckPrivilege(ctx, table.TableDesc(), privilege.INSERT)
	if err != nil {
		return false, 0, 0, nil, 0, err
	}
	if (table.TableType == tree.TimeseriesTable) || (found && table.TableType == tree.TemplateTable) {
		cols := table.TableDescriptor.GetColumns()
		return true, uint32(table.TableDescriptor.ParentID), uint32(table.TableDescriptor.ID), &cols, 0, nil
	}
	return false, 0, 0, nil, 0, nil
}

// getMutableTableVersionByID is a variant of sqlbase.GetTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *TableCollection) getMutableTableVersionByID(
	ctx context.Context, tableID sqlbase.ID, txn *kv.Txn,
) (*sqlbase.MutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable table on table ID %d", tableID)

	if table := tc.getUncommittedTableByID(tableID).MutableTableDescriptor; table != nil {
		log.VEventf(ctx, 2, "found uncommitted table %d", tableID)
		return table, nil
	}
	return sqlbase.GetMutableTableDescFromID(ctx, txn, tableID)
}

// releaseTableLeases releases the leases for the tables with ids in
// the passed slice. Errors are logged but ignored.
func (tc *TableCollection) releaseTableLeases(ctx context.Context, tables []IDVersion) {
	// Sort the tables and leases to make it easy to find the leases to release.
	leasedTables := tc.leasedTables
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].id < tables[j].id
	})
	sort.Slice(leasedTables, func(i, j int) bool {
		return leasedTables[i].ID < leasedTables[j].ID
	})

	filteredLeases := leasedTables[:0] // will store the remaining leases
	tablesToConsider := tables
	shouldRelease := func(id sqlbase.ID) (found bool) {
		for len(tablesToConsider) > 0 && tablesToConsider[0].id < id {
			tablesToConsider = tablesToConsider[1:]
		}
		return len(tablesToConsider) > 0 && tablesToConsider[0].id == id
	}
	for _, l := range leasedTables {
		if !shouldRelease(l.ID) {
			filteredLeases = append(filteredLeases, l)
		} else if err := tc.leaseMgr.Release(l); err != nil {
			log.Warning(ctx, err)
		}
	}
	tc.leasedTables = filteredLeases
}

func (tc *TableCollection) releaseLeases(ctx context.Context) {
	if len(tc.leasedTables) > 0 {
		log.VEventf(ctx, 2, "releasing %d tables", len(tc.leasedTables))
		for _, table := range tc.leasedTables {
			if err := tc.leaseMgr.Release(table); err != nil {
				log.Warning(ctx, err)
			}
		}
		tc.leasedTables = tc.leasedTables[:0]
	}
}

// releaseTables releases all tables currently held by the TableCollection.
func (tc *TableCollection) releaseTables(ctx context.Context) {
	tc.releaseLeases(ctx)
	tc.uncommittedTables = nil
	tc.uncommittedDatabases = nil
	tc.uncommittedSchemas = nil
	tc.releaseAllDescriptors()
}

// ReleaseTSTables is used to externally call releaseTables
func (tc *TableCollection) ReleaseTSTables(ctx context.Context) {
	tc.releaseTables(ctx)
}

// Wait until the database cache has been updated to properly
// reflect all dropped databases, so that future commands on the
// same gateway node observe the dropped databases.
func (tc *TableCollection) waitForCacheToDropDatabases(ctx context.Context) {
	for _, uc := range tc.uncommittedDatabases {
		if !uc.dropped {
			continue
		}
		// Wait until the database cache has been updated to properly
		// reflect a dropped database, so that future commands on the
		// same gateway node observe the dropped database.
		tc.dbCacheSubscriber.waitForCacheState(
			func(dc *databaseCache) bool {
				// Resolve the database name from the database cache.
				dbID, err := dc.getCachedDatabaseID(uc.name)
				if err != nil || dbID == sqlbase.InvalidID {
					// dbID can still be 0 if required is false and
					// the database is not found. Swallowing error here
					// because it was felt there was no value in returning
					// it to a higher layer only to be swallow there. This
					// entire codepath is only called from one place so
					// it's better to swallow it here.
					return true
				}

				// If the database name still exists but it now references another
				// db with a more recent id, we're good - it means that the database
				// name has been reused.
				return dbID > uc.id
			})
	}
}

func (tc *TableCollection) hasUncommittedTables() bool {
	return len(tc.uncommittedTables) > 0
}

func (tc *TableCollection) addUncommittedTable(desc sqlbase.MutableTableDescriptor) error {
	if desc.Version != desc.ClusterVersion.Version+1 {
		return errors.Errorf(
			"descriptor version %d not incremented from cluster version %d",
			desc.Version, desc.ClusterVersion.Version)
	}
	tbl := uncommittedTable{
		MutableTableDescriptor:   &desc,
		ImmutableTableDescriptor: sqlbase.NewImmutableTableDescriptor(desc.TableDescriptor),
	}
	for i, table := range tc.uncommittedTables {
		if table.MutableTableDescriptor.ID == desc.ID {
			tc.uncommittedTables[i] = tbl
			return nil
		}
	}
	tc.uncommittedTables = append(tc.uncommittedTables, tbl)
	tc.releaseAllDescriptors()
	return nil
}

// returns all the idVersion pairs that have undergone a schema change.
// Returns nil for no schema changes. The version returned for each
// schema change is ClusterVersion - 1, because that's the one that will be
// used when checking for table descriptor two version invariance.
// Also returns strings representing the new <name, version> pairs
func (tc *TableCollection) getTablesWithNewVersion() []IDVersion {
	var tables []IDVersion
	for _, table := range tc.uncommittedTables {
		if mut := table.MutableTableDescriptor; !mut.IsNewTable() {
			tables = append(tables, NewIDVersionPrev(&mut.ClusterVersion))
		}
	}
	return tables
}

func (tc *TableCollection) getNewTables() (newTables []*ImmutableTableDescriptor) {
	for _, table := range tc.uncommittedTables {
		if mut := table.MutableTableDescriptor; mut.IsNewTable() {
			newTables = append(newTables, table.ImmutableTableDescriptor)
		}
	}
	return newTables
}

type dbAction bool

const (
	dbCreated dbAction = false
	dbDropped dbAction = true
)

func (tc *TableCollection) addUncommittedDatabase(name string, id sqlbase.ID, action dbAction) {
	db := uncommittedDatabase{name: name, id: id, dropped: action == dbDropped}
	tc.uncommittedDatabases = append(tc.uncommittedDatabases, db)
	tc.releaseAllDescriptors()
}

// getUncommittedDatabaseID returns a database ID for the requested tablename
// if the requested tablename is for a database modified within the transaction
// affiliated with the LeaseCollection.
func (tc *TableCollection) getUncommittedDatabaseID(
	requestedDbName string, required bool,
) (c bool, res sqlbase.ID, err error) {
	// Walk latest to earliest so that a DROP DATABASE followed by a
	// CREATE DATABASE with the same name will result in the CREATE DATABASE
	// being seen.
	for i := len(tc.uncommittedDatabases) - 1; i >= 0; i-- {
		db := tc.uncommittedDatabases[i]
		if requestedDbName == db.name {
			if db.dropped {
				if required {
					return true, sqlbase.InvalidID, sqlbase.NewUndefinedDatabaseError(requestedDbName)
				}
				return true, sqlbase.InvalidID, nil
			}
			return false, db.id, nil
		}
	}
	return false, sqlbase.InvalidID, nil
}

// addUncommittedSchema builds uncommitted Schema and add to uncommittedSchemas
func (tc *TableCollection) addUncommittedSchema(
	name string, id sqlbase.ID, parentID sqlbase.ID, action dbAction,
) {
	unSchema := uncommittedSchema{name: name, id: id, parentID: parentID, dropped: action == dbDropped}
	tc.uncommittedSchemas = append(tc.uncommittedSchemas, unSchema)
	tc.releaseAllDescriptors()
}

// getUncommittedTable returns a table for the requested tablename
// if the requested tablename is for a table modified within the transaction
// affiliated with the LeaseCollection.
//
// The first return value "refuseFurtherLookup" is true when there is
// a known deletion of that table, so it would be invalid to miss the
// cache and go to KV (where the descriptor prior to the DROP may
// still exist).
func (tc *TableCollection) getUncommittedTable(
	dbID sqlbase.ID, schemaID sqlbase.ID, tn *tree.TableName, required bool,
) (refuseFurtherLookup bool, table uncommittedTable, err error) {
	// Walk latest to earliest so that a DROP TABLE followed by a CREATE TABLE
	// with the same name will result in the CREATE TABLE being seen.
	for i := len(tc.uncommittedTables) - 1; i >= 0; i-- {
		table := tc.uncommittedTables[i]
		mutTbl := table.MutableTableDescriptor
		// If a table has gotten renamed we'd like to disallow using the old names.
		// The renames could have happened in another transaction but it's still okay
		// to disallow the use of the old name in this transaction because the other
		// transaction has already committed and this transaction is seeing the
		// effect of it.
		for _, drain := range mutTbl.DrainingNames {
			if drain.Name == string(tn.TableName) &&
				drain.ParentID == dbID &&
				drain.ParentSchemaID == schemaID {
				// Table name has gone away.
				if required {
					// If it's required here, say it doesn't exist.
					err = sqlbase.NewUndefinedRelationError(tn)
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedTable{}, err
			}
		}

		// Do we know about a table with this name?
		if nameMatchesTable(
			&mutTbl.TableDescriptor,
			dbID,
			schemaID,
			tn.Table(),
		) {
			// Right state?
			if err = FilterTableState(mutTbl.TableDesc()); err != nil && err != errTableAdding {
				if !required {
					// If it's not required here, we simply say we don't have it.
					err = nil
				}
				// The table collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedTable{}, err
			}

			// Got a table.
			return false, table, nil
		}
	}
	return false, uncommittedTable{}, nil
}

func (tc *TableCollection) getUncommittedTableByID(id sqlbase.ID) uncommittedTable {
	// Walk latest to earliest so that a DROP TABLE followed by a CREATE TABLE
	// with the same name will result in the CREATE TABLE being seen.
	for i := len(tc.uncommittedTables) - 1; i >= 0; i-- {
		table := tc.uncommittedTables[i]
		if table.MutableTableDescriptor.ID == id {
			return table
		}
	}
	return uncommittedTable{}
}

// getAllDescriptors returns all descriptors visible by the transaction,
// first checking the TableCollection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
func (tc *TableCollection) getAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]sqlbase.DescriptorProto, error) {
	if tc.allDescriptors == nil {
		descs, err := GetAllDescriptors(ctx, txn)
		if err != nil {
			return nil, err
		}
		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// getAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, first checking the TableCollection's cached descriptors for
// validity before scanning system.namespace and looking up the descriptors
// in the database cache, if necessary.
func (tc *TableCollection) getAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]*sqlbase.DatabaseDescriptor, error) {
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := GetAllDatabaseDescriptorIDs(ctx, txn)
		if err != nil {
			return nil, err
		}
		dbDescs, err := getDatabaseDescriptorsFromIDs(ctx, txn, dbDescIDs)
		if err != nil {
			return nil, err
		}
		tc.allDatabaseDescriptors = dbDescs
	}
	return tc.allDatabaseDescriptors, nil
}

func getSchemaDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, ids []sqlbase.ID,
) ([]*sqlbase.SchemaDescriptor, error) {
	results := make([]*sqlbase.SchemaDescriptor, 0, len(ids))
	for _, id := range ids {
		sc, err := getSchemaDescByID(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		results = append(results, sc)
	}
	return results, nil
}

// getDatabaseDesciptorsFromIDs returns the database descriptors from an input
// set of database IDs. It will return an error if any one of the IDs is not a
// database. It attempts to perform this operation in a single request,
// rather than making a round trip for each ID.
func getDatabaseDescriptorsFromIDs(
	ctx context.Context, txn *kv.Txn, ids []sqlbase.ID,
) ([]*sqlbase.DatabaseDescriptor, error) {
	b := txn.NewBatch()
	for _, id := range ids {
		key := sqlbase.MakeDescMetadataKey(id)
		b.Get(key)
	}
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}
	results := make([]*sqlbase.DatabaseDescriptor, 0, len(ids))
	for i := range b.Results {
		result := &b.Results[i]
		if result.Err != nil {
			return nil, result.Err
		}
		if len(result.Rows) != 1 {
			return nil, errors.AssertionFailedf(
				"expected one result for key %s but found %d",
				result.Keys[0],
				len(result.Rows),
			)
		}
		desc := &sqlbase.Descriptor{}
		if err := result.Rows[0].ValueProto(desc); err != nil {
			return nil, err
		}
		db := desc.GetDatabase()
		if db == nil {
			return nil, errors.AssertionFailedf(
				"%q is not a database",
				desc.String(),
			)
		}
		results = append(results, db)
	}
	return results, nil
}

// getSchemasForDatabase returns the schemas for a given database
// visible by the transaction. This uses the schema cache locally
// if possible, or else performs a scan on kv.
func (tc *TableCollection) getSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	if tc.allSchemasForDatabase == nil {
		tc.allSchemasForDatabase = make(map[sqlbase.ID]map[sqlbase.ID]string)
	}
	if _, ok := tc.allSchemasForDatabase[dbID]; !ok {
		var err error
		tc.allSchemasForDatabase[dbID], err = schema.GetForDatabase(ctx, txn, dbID)
		if err != nil {
			return nil, err
		}
	}
	return tc.allSchemasForDatabase[dbID], nil
}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by TableCollection.
func (tc *TableCollection) releaseAllDescriptors() {
	tc.allDescriptors = nil
	tc.allDatabaseDescriptors = nil
	tc.allSchemasForDatabase = nil
}

// Copy the modified schema to the table collection. Used when initializing
// an InternalExecutor.
func (tc *TableCollection) copyModifiedSchema(to *TableCollection) {
	if tc == nil {
		return
	}
	to.uncommittedTables = tc.uncommittedTables
	to.uncommittedDatabases = tc.uncommittedDatabases
	to.uncommittedSchemas = tc.uncommittedSchemas
	// Do not copy the leased descriptors because we do not want
	// the leased descriptors to be released by the "to" TableCollection.
	// The "to" TableCollection can re-lease the same descriptors.
}

// MaybeUpdateDeadline updates the deadline in a given transaction
// based on the leased descriptors in this collection. This update is
// only done when a deadline exists.
func (tc *TableCollection) MaybeUpdateDeadline(ctx context.Context, txn *kv.Txn) {
	leaseDeadline, haveDeadline := tc.Deadline()
	if haveDeadline {
		txn.UpdateDeadlineMaybe(ctx, leaseDeadline)
	}
	return
}

// Deadline returns the latest expiration from our leased
// descriptors which should be the transaction's deadline.
func (tc *TableCollection) Deadline() (deadline hlc.Timestamp, haveDeadline bool) {
	for _, l := range tc.leasedTables {
		tabState := tc.leaseMgr.findTableState(l.ID, false)
		if tabState != nil {
			versionState := tabState.findForTableVersion(l.Version)
			if versionState != nil {
				expiration := versionState.expiration
				if !haveDeadline || expiration.Less(deadline) {
					haveDeadline = true
					deadline = expiration
				}
			}
		}
	}
	return deadline, haveDeadline
}

type tableCollectionModifier interface {
	copyModifiedSchema(to *TableCollection)
}

// validatePrimaryKeys verifies that all tables modified in the transaction have
// an enabled primary key after potentially undergoing DROP PRIMARY KEY, which
// is required to be followed by ADD PRIMARY KEY.
func (tc *TableCollection) validatePrimaryKeys() error {
	modifiedTables := tc.getTablesWithNewVersion()
	for i := range modifiedTables {
		table := tc.getUncommittedTableByID(modifiedTables[i].id).MutableTableDescriptor
		if !table.HasPrimaryKey() {
			return errors.Errorf(
				"primary key of table %s dropped without subsequent addition of new primary key",
				table.Name,
			)
		}
	}
	return nil
}

// MigrationSchemaChangeRequiredContext flags a schema change as necessary to
// run even in a mixed-version 19.2/20.1 state where schema changes are normally
// banned, because the schema change is being run in a startup migration. It's
// the caller's responsibility to ensure that the schema change job is safe to
// run in a mixed-version state.
//
// TODO (lucy): Remove this in 20.2.
func MigrationSchemaChangeRequiredContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationSchemaChangeRequiredHint{}, migrationSchemaChangeRequiredHint{})
}

type migrationSchemaChangeRequiredHint struct{}

// errSchemaChangeDisallowedInMixedState signifies that an attempted schema
// change was disallowed from running in a mixed-version
var errSchemaChangeDisallowedInMixedState = errors.New("schema change cannot be initiated in this version until the version upgrade is finalized")

// createDropDatabaseJob queues a job for dropping a database.
func (p *planner) createDropDatabaseJob(
	ctx context.Context,
	databaseID sqlbase.ID,
	droppedDetails []jobspb.DroppedTableDetails,
	jobDesc string,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
		if ctx.Value(migrationSchemaChangeRequiredHint{}) == nil {
			return errSchemaChangeDisallowedInMixedState
		}
	}
	// TODO (lucy): This should probably be deleting the queued jobs for all the
	// tables being dropped, so that we don't have duplicate schema changers.
	descriptorIDs := make([]sqlbase.ID, 0, len(droppedDetails))
	for _, d := range droppedDetails {
		descriptorIDs = append(descriptorIDs, d.ID)
	}
	jobRecord := jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: descriptorIDs,
		Details: jobspb.SchemaChangeDetails{
			DroppedTables:     droppedDetails,
			DroppedDatabaseID: databaseID,
			FormatVersion:     jobspb.JobResumerFormatVersion,
		},
		Progress: jobspb.SchemaChangeProgress{},
	}
	_, err := p.extendedEvalCtx.QueueJob(jobRecord)
	return err
}

// createOrUpdateSchemaChangeJob queues a new job for the schema change if there
// is no existing schema change job for the table, or updates the existing job
// if there is one.
func (p *planner) createOrUpdateSchemaChangeJob(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	jobDesc string,
	mutationID sqlbase.MutationID,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
		if ctx.Value(migrationSchemaChangeRequiredHint{}) == nil {
			return errSchemaChangeDisallowedInMixedState
		}
	}
	var job *jobs.Job
	if cachedJob, ok := p.extendedEvalCtx.SchemaChangeJobCache[tableDesc.ID]; ok {
		job = cachedJob
	}

	if p.extendedEvalCtx.ExecCfg.TestingKnobs.RunAfterSCJobsCacheLookup != nil {
		p.extendedEvalCtx.ExecCfg.TestingKnobs.RunAfterSCJobsCacheLookup(job)
	}

	var spanList []jobspb.ResumeSpanList
	jobExists := job != nil
	if jobExists {
		spanList = job.Details().(jobspb.SchemaChangeDetails).ResumeSpanList
	}
	span := tableDesc.PrimaryIndexSpan()
	for i := len(tableDesc.ClusterVersion.Mutations) + len(spanList); i < len(tableDesc.Mutations); i++ {
		spanList = append(spanList,
			jobspb.ResumeSpanList{
				ResumeSpans: []roachpb.Span{span},
			},
		)
	}

	if !jobExists {
		// Queue a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				TableID:        tableDesc.ID,
				MutationID:     mutationID,
				ResumeSpanList: spanList,
				FormatVersion:  jobspb.JobResumerFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
		}
		newJob, err := p.extendedEvalCtx.QueueJob(jobRecord)
		if err != nil {
			return err
		}
		p.extendedEvalCtx.SchemaChangeJobCache[tableDesc.ID] = newJob
		// Only add a MutationJob if there's an associated mutation.
		// TODO (lucy): get rid of this when we get rid of MutationJobs.
		if mutationID != sqlbase.InvalidMutationID {
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: *newJob.ID()})
		}
		log.Infof(ctx, "queued new schema change job %d for table %d, mutation %d",
			*newJob.ID(), tableDesc.ID, mutationID)
	} else {
		// Update the existing job.
		oldDetails := job.Details().(jobspb.SchemaChangeDetails)
		newDetails := jobspb.SchemaChangeDetails{
			TableID:        tableDesc.ID,
			MutationID:     oldDetails.MutationID,
			ResumeSpanList: spanList,
			FormatVersion:  jobspb.JobResumerFormatVersion,
		}
		if oldDetails.MutationID != sqlbase.InvalidMutationID {
			// The previous queued schema change job was associated with a mutation,
			// which must have the same mutation ID as this schema change, so just
			// check for consistency.
			if mutationID != sqlbase.InvalidMutationID && mutationID != oldDetails.MutationID {
				return errors.AssertionFailedf(
					"attempted to update job for mutation %d, but job already exists with mutation %d",
					mutationID, oldDetails.MutationID)
			}
		} else {
			// The previous queued schema change job didn't have a mutation.
			if mutationID != sqlbase.InvalidMutationID {
				newDetails.MutationID = mutationID
				// Also add a MutationJob on the table descriptor.
				// TODO (lucy): get rid of this when we get rid of MutationJobs.
				tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID, JobID: *job.ID()})
			}
		}
		if err := job.WithTxn(p.txn).SetDetails(ctx, newDetails); err != nil {
			return err
		}
		if jobDesc != "" {
			if err := job.WithTxn(p.txn).SetDescription(
				ctx,
				func(ctx context.Context, description string) (string, error) {
					return strings.Join([]string{description, jobDesc}, ";"), nil
				},
			); err != nil {
				return err
			}
		}
		log.Infof(ctx, "job %d: updated with schema change for table %d, mutation %d",
			*job.ID(), tableDesc.ID, mutationID)
	}
	return nil
}

// writeSchemaChange effectively writes a table descriptor to the
// database within the current planner transaction, and queues up
// a schema changer for future processing.
// TODO (lucy): The way job descriptions are handled needs improvement.
// Currently, whenever we update a job, the provided job description string, if
// non-empty, is appended to the end of the existing description, regardless of
// whether the particular schema change written in this method call came from a
// separate statement in the same transaction, or from updating a dependent
// table descriptor during a schema change to another table, or from a step in a
// larger schema change to the same table.
func (p *planner) writeSchemaChange(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	mutationID sqlbase.MutationID,
	jobDesc string,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return errors.Errorf("no schema changes allowed on table %q as it is being dropped",
			tableDesc.Name)
	}
	if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, mutationID); err != nil {
		return err
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeSchemaChangeToBatch(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, b *kv.Batch,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return errors.Errorf("no schema changes allowed on table %q as it is being dropped",
			tableDesc.Name)
	}
	return p.writeTableDescToBatch(ctx, tableDesc, b)
}

func (p *planner) writeDropTable(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, queueJob bool, jobDesc string,
) error {
	if queueJob {
		if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeTableDescToBatch(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, b *kv.Batch,
) error {
	if tableDesc.IsVirtualTable() {
		return errors.AssertionFailedf("virtual descriptors cannot be stored, found: %v", tableDesc)
	}

	if tableDesc.IsNewTable() {
		if err := runSchemaChangesInTxn(
			ctx, p, tableDesc, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		); err != nil {
			return err
		}
	} else {
		// Only increment the table descriptor version once in this transaction.
		if err := tableDesc.MaybeIncrementVersion(ctx, p.txn, p.execCfg.Settings); err != nil {
			return err
		}
	}

	if err := tableDesc.ValidateTable(); err != nil {
		return errors.AssertionFailedf("table descriptor is not valid: %s\n%v", err, tableDesc)
	}

	if err := p.Tables().addUncommittedTable(*tableDesc); err != nil {
		return err
	}

	return writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, tableDesc.GetID(), tableDesc.TableDesc())
}
