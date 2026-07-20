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
	"bytes"
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
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

func (p *GenericPlanner) getVirtualTabler() VirtualTabler {
	return p.extendedEvalCtx.VirtualSchemas
}

type inactiveTableError struct {
	error
}

// TableToDelete represents a table that is pending deletion
type TableToDelete struct {
	Tn   *tree.TableName
	Desc *sqlbase.MutableTableDescriptor
}

// FilterTableState inspects the state of a given table and returns an error if
// the state is anything but PUBLIC. The error describes the state of the table.
func FilterTableState(tableDesc *sqlbase.TableDescriptor) error {
	switch tableDesc.State {
	case sqlbase.TableDescriptor_DROP:
		return inactiveTableError{sqlerror.ErrTableDropped}
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
		return sqlerror.ErrTableAdding
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
		foundSchema, schemaID, err := tc.ResolveSchemaID(ctx, txn, dbID, tn.Schema())
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

// ResolveSchemaID attempts to lookup the schema from the schemaCache if it exists,
// otherwise falling back to a database lookup.
func (tc *TableCollection) ResolveSchemaID(
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
	exists, schemaID, err := ResolveSchemaID(ctx, txn, dbID, schemaName)
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
		log.Infof(ctx, "GenericPlanner acquiring lease on table '%s'", tn)
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
	foundSchema, schemaID, err := tc.ResolveSchemaID(ctx, txn, dbID, tn.Schema())
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
			err := sqlerror.ErrTableAdding
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

// GetTableVersionByID is a by-ID variant of getTableVersion (i.e. uses same cache).
func (tc *TableCollection) GetTableVersionByID(
	ctx context.Context, txn *kv.Txn, tableID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "GenericPlanner getting table on table ID %d", tableID)

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
	var pp GenericPlanner
	pp.txn = txn
	pp.execCfg = cfg
	pp.curPlan = planTop{}
	var sd sessiondata.SessionData
	sd.User = user
	pp.AvoidCachedDescriptors = true
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
	if (table.IsTimeseriesTable() || table.IsSparseTable()) || (found && table.IsTemplateTable()) {
		cols := table.TableDescriptor.GetColumns()
		return true, uint32(table.TableDescriptor.ParentID), uint32(table.TableDescriptor.ID), &cols, 0, nil
	}
	return false, 0, 0, nil, 0, nil
}

// GetMutableTableVersionByID is a variant of sqlbase.GetTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *TableCollection) GetMutableTableVersionByID(
	ctx context.Context, tableID sqlbase.ID, txn *kv.Txn,
) (*sqlbase.MutableTableDescriptor, error) {
	log.VEventf(ctx, 2, "GenericPlanner getting mutable table on table ID %d", tableID)

	if table := tc.GetUncommittedTableByID(tableID).MutableTableDescriptor; table != nil {
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

// AddUncommittedTable adds an uncommitted table descriptor to the collection
func (tc *TableCollection) AddUncommittedTable(desc sqlbase.MutableTableDescriptor) error {
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

// AddUncommittedDatabase adds an uncommitted database descriptor to the collection
func (tc *TableCollection) AddUncommittedDatabase(
	name string, id sqlbase.ID, action sqlconst.DbAction,
) {
	db := uncommittedDatabase{name: name, id: id, dropped: action == sqlconst.DbDropped}
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

// AddUncommittedSchema builds uncommitted Schema and add to uncommittedSchemas
func (tc *TableCollection) AddUncommittedSchema(
	name string, id sqlbase.ID, parentID sqlbase.ID, action sqlconst.DbAction,
) {
	unSchema := uncommittedSchema{name: name, id: id, parentID: parentID, dropped: action == sqlconst.DbDropped}
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
			if err = FilterTableState(mutTbl.TableDesc()); err != nil && err != sqlerror.ErrTableAdding {
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

// GetUncommittedTableByID retrieves an uncommitted table descriptor by its ID
// nolint:unexportedreturn
func (tc *TableCollection) GetUncommittedTableByID(id sqlbase.ID) uncommittedTable {
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

// TcGetAllDescriptors returns all descriptors visible by the transaction,
// first checking the TableCollection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
func (tc *TableCollection) TcGetAllDescriptors(
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

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction. This uses the schema cache locally
// if possible, or else performs a scan on kv.
func (tc *TableCollection) GetSchemasForDatabase(
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
		table := tc.GetUncommittedTableByID(modifiedTables[i].id).MutableTableDescriptor
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

// CreateDropDatabaseJob queues a job for dropping a database.
func CreateDropDatabaseJob(
	ctx context.Context,
	p *GenericPlanner,
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
func (p *GenericPlanner) createOrUpdateSchemaChangeJob(
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

// WriteSchemaChange effectively writes a table descriptor to the
// database within the current GenericPlanner transaction, and queues up
// a schema changer for future processing.
// TODO (lucy): The way job descriptions are handled needs improvement.
// Currently, whenever we update a job, the provided job description string, if
// non-empty, is appended to the end of the existing description, regardless of
// whether the particular schema change written in this method call came from a
// separate statement in the same transaction, or from updating a dependent
// table descriptor during a schema change to another table, or from a step in a
// larger schema change to the same table.
func (p *GenericPlanner) WriteSchemaChange(
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

func (p *GenericPlanner) writeSchemaChangeToBatch(
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

func (p *GenericPlanner) writeDropTable(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, queueJob bool, jobDesc string,
) error {
	if queueJob {
		if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	return p.writeTableDesc(ctx, tableDesc)
}

// WriteTableDesc writes a table descriptor to the system tables
func WriteTableDesc(
	ctx context.Context, p *GenericPlanner, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, b); err != nil {
		return err
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *GenericPlanner) writeTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *GenericPlanner) writeTableDescToBatch(
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

	if err := p.Tables().AddUncommittedTable(*tableDesc); err != nil {
		return err
	}

	return writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, tableDesc.GetID(), tableDesc.TableDesc())

}

// PrepareDropTable and DropTableImpl is used to drop a single table by
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
func (p *GenericPlanner) PrepareDropTable(
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
	if err := PrepareDropWithTableDesc(ctx, p, tableDesc); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

// PrepareDropWithTableDesc behaves as PrepareDropTable, except it assumes the
// table descriptor is already fetched. This is useful for DropDatabase,
// as PrepareDropTable requires resolving a TableName when DropDatabase already
// has it resolved.
func PrepareDropWithTableDesc(
	ctx context.Context, p *GenericPlanner, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP)
	if err != nil {
		return err
	}
	return nil
}

// CanRemoveFKBackreference returns an error if the input backreference isn't allowed to be removed.
func CanRemoveFKBackreference(
	ctx context.Context,
	p *GenericPlanner,
	from string,
	ref *sqlbase.ForeignKeyConstraint,
	behavior tree.DropBehavior,
) error {
	table, err := p.Tables().GetMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
	if err != nil {
		return err
	}
	if behavior != tree.DropCascade {
		return fmt.Errorf("%q is referenced by foreign key from table %q", from, table.Name)
	}
	// Check to see whether we're allowed to edit the table that has a
	// foreign key constraint on the table that we're dropping right now.
	if err = p.CheckPrivilege(ctx, table, privilege.CREATE); err != nil {
		return err
	}
	return nil
}

// CanRemoveInterleave checks if an interleave relationship can be safely removed
func CanRemoveInterleave(
	ctx context.Context,
	p *GenericPlanner,
	from string,
	ref sqlbase.ForeignKeyReference,
	behavior tree.DropBehavior,
) error {
	table, err := p.Tables().GetMutableTableVersionByID(ctx, ref.Table, p.txn)
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
	if err = p.CheckPrivilege(ctx, table, privilege.CREATE); err != nil {
		return err
	}
	return nil
}

func (p *GenericPlanner) removeInterleave(
	ctx context.Context, ref sqlbase.ForeignKeyReference,
) error {
	table, err := p.Tables().GetMutableTableVersionByID(ctx, ref.Table, p.txn)
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
	return p.WriteSchemaChange(ctx, table, sqlbase.InvalidMutationID, "")
}

// DropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior.
func DropTableImpl(
	ctx context.Context,
	p *GenericPlanner,
	tableDesc *sqlbase.MutableTableDescriptor,
	queueJob bool,
	jobDesc string,
	behavior tree.DropBehavior,
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
		if err := p.RemoveSequenceDependencies(ctx, tableDesc, &tableDesc.Columns[i]); err != nil {
			return droppedViews, err
		}
		// drop sequence if table use it
		for _, sequenceID := range usesSequenceIds {
			seqDesc, err := p.Tables().GetMutableTableVersionByID(ctx, sequenceID, p.txn)
			if err != nil {
				return droppedViews, err
			}
			if !seqDesc.SequenceOpts.IsSerial {
				continue
			}
			if seqDesc.Dropped() {
				continue
			}
			err = p.DropSequenceImpl(
				ctx, seqDesc, queueJob /* queueJob */, jobDesc, behavior,
			)
			if err != nil {
				return droppedViews, err
			}
		}
	}

	// Drop sequences that the columns of the table own
	for _, col := range tableDesc.Columns {
		if err := DropSequencesOwnedByCol(ctx, p, &col, queueJob); err != nil {
			return droppedViews, err
		}
	}

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	for _, ref := range tableDesc.DependedOnBy {
		viewDesc, skipped, err := GetViewDescForCascade(ctx,
			p, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, ref.ID, behavior,
		)
		if err != nil {
			return droppedViews, err
		}
		if skipped {
			continue
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		cascadedViews, err := p.DropViewImpl(ctx, viewDesc, queueJob, "dropping dependent view", tree.DropCascade)
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

	if err := p.removeTableStreams(ctx, tableDesc); err != nil {
		return droppedViews, err
	}

	err = p.initiateDropTable(ctx, tableDesc, queueJob, jobDesc, true /* drain name */)
	return droppedViews, err
}

func (p *GenericPlanner) removeTableStreams(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	rows, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx,
		"load-streams",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id,job_id,source_table_id FROM system.kwdb_streams WHERE source_table_id = $1 OR target_table_id = $1`,
		tableDesc.ID,
	)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		streamID := uint64(tree.MustBeDInt(row[0]))
		jobID := int64(tree.MustBeDInt(row[1]))
		tableID := int64(tree.MustBeDInt(row[2]))

		if err = p.removeStream(ctx, jobID, streamID, uint64(tableID)); err != nil {
			return err
		}
	}

	return nil
}

// InitTableDescriptor returns a blank TableDescriptor.
func InitTableDescriptor(
	id, parentID, parentSchemaID sqlbase.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	temporary bool,
	tblType tree.TableType,
	creator string,
) sqlbase.MutableTableDescriptor {
	return *sqlbase.NewMutableCreatedTableDescriptor(sqlbase.TableDescriptor{
		ID:                      id,
		Name:                    name,
		ParentID:                parentID,
		UnexposedParentSchemaID: parentSchemaID,
		FormatVersion:           sqlbase.InterleavedFormatVersion,
		Version:                 1,
		ModificationTime:        creationTime,
		Privileges:              privileges,
		CreateAsOfTime:          creationTime,
		Temporary:               temporary,
		TableType:               tblType,
		Creator:                 creator,
		CreateTime:              creationTime,
	})
}

// buildTSTableDesc checks if object in create table is available and build time-series table descriptor
func buildTSTableDesc(
	desc *sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	n *tree.CreateTable,
	allTagDesc *[]*sqlbase.ColumnDescriptor,
	user string,
) error {
	allTagName := make(map[tree.Name]*sqlbase.ColumnDescriptor, len(n.Tags)+1)
	if len(n.StorageParams) > 0 {
		return sqlbase.TSUnsupportedError("storage params is not accepted for timeseries table")
	}
	if len(n.Defs) < 2 {
		return pgerror.New(pgcode.InvalidTableDefinition, "ts table must have at least 2 columns")
	}
	if len(n.Defs) > sqlconst.MaxTSDataColumns {
		return pgerror.Newf(pgcode.TooManyColumns, "table %s has too many columns,"+
			" each timeseries table can have maximum %d columns", n.Table.Table(), sqlconst.MaxTSDataColumns)
	}
	if desc.IsSparseTable() && len(n.Defs) > sqlconst.MaxSparseTSDataColumns {
		return pgerror.Newf(pgcode.TooManyColumns, "table %s has too many columns,"+
			" each sparse timeseries table can have maximum %d columns", n.Table.Table(), sqlconst.MaxSparseTSDataColumns)
	}
	desc.TsTable.TsVersion = 1
	desc.TsTable.NextTsVersion = desc.TsTable.TsVersion + 1

	if n.HashNum == 0 {
		desc.TsTable.HashNum = api.HashParamV2
	} else {
		desc.TsTable.HashNum = uint64(n.HashNum)
	}

	// The default primary tag for template tables is the instance table name
	if len(n.PrimaryTagList) == 0 {
		hiddenTag := sqlbase.ColumnDescriptor{
			Name:     "pTag",
			Type:     *types.MakeChar(63),
			Nullable: false,
			Hidden:   true,
			TsCol: sqlbase.TSCol{
				ColumnType:         sqlbase.ColumnType_TYPE_PTAG,
				StorageType:        sqlbase.DataType_CHAR,
				StorageLen:         63,
				VariableLengthType: sqlbase.VariableLengthType_ColStorageTypeTuple,
			},
		}
		*allTagDesc = append(*allTagDesc, &hiddenTag)
		allTagName[tree.Name(hiddenTag.Name)] = &hiddenTag
	}

	primaryTagName := make(map[tree.Name]struct{}, len(n.PrimaryTagList))
	for _, pt := range n.PrimaryTagList {
		primaryTagName[pt] = struct{}{}
	}
	var allColumnName []tree.Name
	for i := range n.Tags {
		columnType := sqlbase.ColumnType_TYPE_TAG
		if _, ok := allTagName[n.Tags[i].TagName]; ok {
			return pgerror.Newf(pgcode.DuplicateColumn, "duplicate tag name: %q", n.Tags[i].TagName)
		}
		if _, ok := primaryTagName[n.Tags[i].TagName]; ok {
			columnType = sqlbase.ColumnType_TYPE_PTAG
			if n.Tags[i].TagType.Width() == sqlconst.DefaultTypeWithLength && n.Tags[i].TagType.Oid() == oid.T_varchar {
				n.Tags[i].TagType = types.MakeVarChar(sqlconst.DefaultPrimaryTagVarcharWidth, n.Tags[i].TagType.TypeEngine())
			}
		}
		if len(string(n.Tags[i].TagName)) > sqlconst.MaxTagNameLength {
			return sqlbase.NewTSNameOutOfLengthError("tag", string(n.Tags[i].TagName), sqlconst.MaxTagNameLength)
		}
		if n.Tags[i].IsSerial {
			return pgerror.Newf(
				pgcode.FeatureNotSupported, "serial type for tag %s is not supported in timeseries table", n.Tags[i].TagName)
		}
		tagType, err := CheckTagType(n.Tags[i].TagName, n.Tags[i].TagType)
		if err != nil {
			return err
		}
		n.Tags[i].TagType = tagType
		// Building columnDesc for tags
		tagColumn, _, err := sqlbase.MakeTSColumnDefDescs(string(n.Tags[i].TagName), n.Tags[i].TagType, n.Tags[i].Nullable, columnType, nil, semaCtx, sqlbase.CompressInfo{})
		if err != nil {
			return err
		}
		*allTagDesc = append(*allTagDesc, tagColumn)
		allTagName[n.Tags[i].TagName] = tagColumn
		allColumnName = append(allColumnName, n.Tags[i].TagName)
	}
	for _, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			allColumnName = append(allColumnName, d.Name)
		}
	}
	for _, colName := range allColumnName {
		if colName == opt.HiddenOSNColumnName ||
			colName == opt.HiddenOperationColumnName ||
			colName == opt.HiddenEventColumnName {
			return pgerror.Newf(pgcode.InvalidName, "creating hidden %s column is not supported in the time series table", colName)
		}
	}
	// Check if the primary tag meets the requirements of the primary tag
	// 1. Cannot exceed four
	// 2. Floating point types and variable length types other than varchar are not supported
	// 3. The maximum length of varchar type is 128, with a default of 64
	// 4. The primary tag must be not null
	if len(n.PrimaryTagList) > sqlbase.MaxPrimaryTagNum {
		return pgerror.Newf(pgcode.ProgramLimitExceeded, "the max number of primary tags is %d", sqlbase.MaxPrimaryTagNum)
	}
	for _, pt := range n.PrimaryTagList {
		if tagColumn, ok := allTagName[pt]; ok {
			if err := checkPrimaryTag(*tagColumn); err != nil {
				return err
			}
		} else {
			return pgerror.Newf(pgcode.InvalidName, "primary tag %s is not a tag", string(pt))
		}
	}
	if n.DownSampling != nil {
		reten, err := checkRetentionForCreate(n.Defs, *n.DownSampling)
		if err != nil {
			return err
		}
		desc.TsTable.Resolution = reten.resolution
		desc.TsTable.KeepDuration = reten.keepDuration
		desc.TsTable.Sample = reten.samples
		desc.TsTable.Downsampling = reten.originRetention
		desc.TsTable.Lifetime = reten.lifetime
		desc.TsTable.DownsamplingCreator = user
	} else {
		desc.TsTable.Lifetime = sqlconst.InvalidLifetime
	}
	return nil
}

// checkColumnDef checks if object in column definition is available
func checkColumnDef(
	ctx context.Context,
	d *tree.ColumnTableDef,
	desc *sqlbase.MutableTableDescriptor,
	n *tree.CreateTable,
	st *cluster.Settings,
	sessionData *sessiondata.SessionData,
	columnDefaultExprs *[]tree.TypedExpr,
) error {
	if !desc.IsTSTable() {
		if d.ColumnEncode.EncodeAlgo != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported, "ENCODE only supported on ts table")
		}
		if d.ColumnCompress.CompressAlgo != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported, "COMPRESS only supported on ts table")
		}
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if !desc.IsVirtualTable() {
		switch d.Type.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"VECTOR column types are unsupported",
			)
		}
	}
	if supported, err := sqlutil.IsTypeSupportedInVersion(version, d.Type); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			d.Type.SQLString(),
		)
	}
	if d.PrimaryKey.Sharded {
		// This function can sometimes be called when `st` is nil,
		// and also before the version has been initialized. We only
		// allow hash sharded indexes to be created if we know for
		// certain that it supported by the cluster.
		if st == nil {
			return sqlerror.InvalidClusterForShardedIndexError
		}
		if version == (clusterversion.ClusterVersion{}) ||
			!version.IsActive(clusterversion.VersionHashShardedIndexes) {
			return sqlerror.InvalidClusterForShardedIndexError
		}

		if !sessionData.HashShardedIndexesEnabled {
			return sqlerror.HashShardedIndexesDisabledError
		}
		if n.PartitionBy != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		if n.Interleave != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		buckets, err := tree.EvalShardBucketCount(d.PrimaryKey.ShardBuckets)
		if err != nil {
			return err
		}
		shardCol, _, err := schema.MaybeCreateAndAddShardCol(int(buckets), desc,
			[]string{string(d.Name)}, true /* isNewTable */)
		if err != nil {
			return err
		}
		checkConstraint, err := schema.MakeShardCheckConstraintDef(desc, int(buckets), shardCol)
		if err != nil {
			return err
		}
		// Add the shard's check constraint to the list of TableDefs to treat it
		// like it's been "hoisted" like the explicitly added check constraints.
		// It'll then be added to this table's resulting table descriptor below in
		// the constraint pass.
		n.Defs = append(n.Defs, checkConstraint)
		*columnDefaultExprs = append(*columnDefaultExprs, nil)
	}
	return nil
}

// checkAndMakeTSColDesc checks if the first column type is timestamptz and make ts column descriptor
func checkAndMakeTSColDesc(
	d *tree.ColumnTableDef,
	semaCtx *tree.SemaContext,
	col **sqlbase.ColumnDescriptor,
	desc *sqlbase.MutableTableDescriptor,
	isFirstTSCol bool,
) (tree.TypedExpr, error) {
	var err error
	var expr tree.TypedExpr
	if isFirstTSCol {
		if d.Type.Family() != types.TimestampFamily && d.Type.Family() != types.TimestampTZFamily {
			return nil, pgerror.Newf(pgcode.DatatypeMismatch, "column %s: the 1st column's type in timeseries table must be TimestampTZ", d.Name)
		} else if d.Nullable.Nullability != tree.NotNull {
			return nil, pgerror.Newf(pgcode.NotNullViolation, "the 1st TimestampTZ column %s must be not null", string(d.Name))
		}
		if d.Type.InternalType.TimePrecisionIsSet {
			d.Type = types.MakeTimestampTZ(d.Type.Precision())
		} else {
			d.Type = types.MakeTimestampTZ(3)
		}
	} else {
		d.Type = sqlbase.UpdateTimeColPrecision(d.Type, true) // true for TS table
	}
	if err = schema.CheckTSColValidity(d); err != nil {
		return nil, err
	}
	nullable := d.Nullable.Nullability != tree.NotNull
	compressInfo := sqlbase.CompressInfo{
		EncodeAlgo:    d.ColumnEncode.EncodeAlgo,
		RelErr:        d.ColumnEncode.RelErr,
		AbsErr:        d.ColumnEncode.AbsErr,
		CompressAlgo:  d.ColumnCompress.CompressAlgo,
		CompressLevel: d.ColumnCompress.CompressLevel,
	}
	*col, expr, err = sqlbase.MakeTSColumnDefDescs(string(d.Name), d.Type, nullable, sqlbase.ColumnType_TYPE_DATA, d.DefaultExpr.Expr, semaCtx, compressInfo)
	if err != nil {
		return nil, err
	}
	if isFirstTSCol {
		// Add a unique constraint to the first column of the timeseries table to prevent a validate error
		tsPK := sqlbase.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(tsPK, true); err != nil {
			return nil, err
		}
	}
	return expr, nil
}

// buildIndexForDesc builds index descriptor for column descriptor
func buildIndexForDesc(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	d *tree.IndexTableDef,
	desc *sqlbase.MutableTableDescriptor,
	setupShardedIndexForNewTable func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
) error {
	idx := sqlbase.IndexDescriptor{
		Name:             string(d.Name),
		StoreColumnNames: d.Storing.ToStrings(),
		Version:          indexEncodingVersion,
	}
	if d.Inverted {
		idx.Type = sqlbase.IndexDescriptor_INVERTED
	}
	if d.Sharded != nil {
		if d.Interleave != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		if err := setupShardedIndexForNewTable(d, &idx); err != nil {
			return err
		}
	}
	if err := idx.FillColumns(d.Columns); err != nil {
		return err
	}
	if d.PartitionBy != nil {
		partitioning, err := NewPartitioningDescriptor(ctx, evalCtx, desc, &idx, d.PartitionBy)
		if err != nil {
			return err
		}
		idx.Partitioning = partitioning
	}

	if err := desc.AddIndex(idx, false); err != nil {
		return err
	}
	if d.Interleave != nil {
		return unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
	}
	return nil
}

// buildUniqueForDesc builds unique descriptor for column descriptor
func buildUniqueForDesc(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	d *tree.UniqueConstraintTableDef,
	desc *sqlbase.MutableTableDescriptor,
	n *tree.CreateTable,
	setupShardedIndexForNewTable func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
	primaryIndexColumnSet *map[string]struct{},
) error {
	idx := sqlbase.IndexDescriptor{
		Name:             string(d.Name),
		Unique:           true,
		StoreColumnNames: d.Storing.ToStrings(),
		Version:          indexEncodingVersion,
	}
	if d.Sharded != nil {
		if n.Interleave != nil && d.PrimaryKey {
			return pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		if err := setupShardedIndexForNewTable(&d.IndexTableDef, &idx); err != nil {
			return err
		}
	}
	if err := idx.FillColumns(d.Columns); err != nil {
		return err
	}
	if d.PartitionBy != nil {
		partitioning, err := NewPartitioningDescriptor(ctx, evalCtx, desc, &idx, d.PartitionBy)
		if err != nil {
			return err
		}
		idx.Partitioning = partitioning
	}
	if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
		return err
	}
	if d.PrimaryKey {
		if d.Interleave != nil {
			return unimplemented.NewWithIssue(
				45710,
				"interleave not supported in primary key constraint definition",
			)
		}
		*primaryIndexColumnSet = make(map[string]struct{})
		for _, c := range d.Columns {
			(*primaryIndexColumnSet)[string(c.Column)] = struct{}{}
		}
	}
	if d.Interleave != nil {
		return unimplemented.NewWithIssue(9148, "use CREATE INDEX to make interleaved indexes")
	}
	return nil
}

// buildFamilyForDesc builds column family descriptor for table descriptor
func buildFamilyForDesc(
	d *tree.FamilyTableDef,
	desc *sqlbase.MutableTableDescriptor,
	columnsInExplicitFamilies *map[string]bool,
) {
	fam := sqlbase.ColumnFamilyDescriptor{
		Name:        string(d.Name),
		ColumnNames: d.Columns.ToStrings(),
	}
	for _, c := range fam.ColumnNames {
		(*columnsInExplicitFamilies)[c] = true
	}
	desc.AddFamily(fam)
}

// addColToTblDesc resolves column table define to column desc and add column desc to table desc
func addColToTblDesc(
	semaCtx *tree.SemaContext,
	d *tree.ColumnTableDef,
	num int,
	desc *sqlbase.MutableTableDescriptor,
	columnDefaultExprs *[]tree.TypedExpr,
	indexEncodingVersion sqlbase.IndexDescriptorVersion,
) error {
	var err error
	var col *sqlbase.ColumnDescriptor
	var idx *sqlbase.IndexDescriptor
	var expr tree.TypedExpr
	if desc.IsTSTable() {
		isFirstTSCol := num == 0
		if expr, err = checkAndMakeTSColDesc(d, semaCtx, &col, desc, isFirstTSCol); err != nil {
			return err
		}
	} else {
		col, idx, expr, err = sqlbase.MakeColumnDefDescs(d, semaCtx, false)
		if err != nil {
			return err
		}
	}

	desc.AddColumn(col)
	if d.HasDefaultExpr() {
		// This resolution must be delayed until ColumnIDs have been populated.
		(*columnDefaultExprs)[num] = expr
	} else {
		(*columnDefaultExprs)[num] = nil
	}

	if idx != nil {
		idx.Version = indexEncodingVersion
		if err := desc.AddIndex(*idx, d.PrimaryKey.IsPrimaryKey); err != nil {
			return err
		}
	}

	if d.HasColumnFamily() {
		// Pass true for `create` and `ifNotExists` because when we're creating
		// a table, we always want to create the specified family if it doesn't
		// exist.
		err := desc.AddColumnToFamilyMaybeCreate(col.Name, string(d.Family.Name), true, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkColFamily checks if primary key column is in column family when cluster setting is nil
func checkColFamily(
	ctx context.Context, st *cluster.Settings, desc *sqlbase.MutableTableDescriptor,
) error {
	if version := st.Version.ActiveVersionOrEmpty(ctx); version != (clusterversion.ClusterVersion{}) &&
		!version.IsActive(clusterversion.VersionPrimaryKeyColumnsOutOfFamilyZero) {
		var colsInFamZero util.FastIntSet
		for _, colID := range desc.Families[0].ColumnIDs {
			colsInFamZero.Add(int(colID))
		}
		for _, colID := range desc.PrimaryIndex.ColumnIDs {
			if !colsInFamZero.Contains(int(colID)) {
				return errors.Errorf("primary key column %d is not in column family 0", colID)
			}
		}
	}
	return nil
}

// parseAndSerializeComputeCol parses compute expr of col and serialize result expr
func parseAndSerializeComputeCol(
	ctx context.Context, n *tree.CreateTable, desc *sqlbase.MutableTableDescriptor,
) error {
	// Now that we've constructed our columns, we pop into any of our computed
	// columns so that we can dequalify any column references.
	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		n.Table, sqlbase.ResultColumnsFromColDescs(desc.GetID(), desc.Columns),
	)

	for i := range desc.Columns {
		col := &desc.Columns[i]
		if col.IsComputed() {
			expr, err := parser.ParseExpr(*col.ComputeExpr)
			if err != nil {
				return err
			}

			expr, err = dequalifyColumnRefs(ctx, sourceInfo, expr)
			if err != nil {
				return err
			}
			serialized := tree.Serialize(expr)
			col.ComputeExpr = &serialized
		}
	}
	return nil
}

// MakeTableDesc creates a table descriptor from a CreateTable statement.
//
// txn and vt can be nil if the table to be created does not contain references
// to other tables (e.g. foreign keys or interleaving). This is useful at
// bootstrap when creating descriptors for virtual tables.
//
// parentID refers to the databaseID under which the descriptor is being
// created,and parentSchemaID refers to the schemaID of the schema under which
// the descriptor is being created.
//
// evalCtx can be nil if the table to be created has no default expression for
// any of the columns and no partitioning expression.
//
// semaCtx can be nil if the table to be created has no default expression on
// any of the columns and no check constraints.
//
// The caller must also ensure that the SchemaResolver is configured
// to bypass caching and enable visibility of just-added descriptors.
// This is used to resolve sequence and FK dependencies. Also see the
// comment at the start of the global scope resolveFK().
//
// If the table definition *may* use the SERIAL type, the caller is
// also responsible for processing serial types using
// processSerialInColumnDef() on every column definition, and creating
// the necessary sequences in KV before calling MakeTableDesc().
func MakeTableDesc(
	ctx context.Context,
	txn *kv.Txn,
	vt SchemaResolver,
	st *cluster.Settings,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	sessionData *sessiondata.SessionData,
	temporary bool,
) (sqlbase.MutableTableDescriptor, error) {
	// Used to delay establishing Column/Sequence dependency until ColumnIDs have
	// been populated.
	var err error
	columnDefaultExprs := make([]tree.TypedExpr, len(n.Defs))
	desc := InitTableDescriptor(
		id, parentID, parentSchemaID, n.Table.Table(), creationTime, privileges, temporary, n.TableType, sessionData.User,
	)
	var allTagDesc []*sqlbase.ColumnDescriptor
	if desc.IsTSTable() {
		if err = buildTSTableDesc(&desc, semaCtx, n, &allTagDesc, sessionData.User); err != nil {
			return desc, err
		}
	}

	if err = checkStorageParameters(semaCtx, n.StorageParams, storageParamExpectedTypes); err != nil {
		return desc, err
	}

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into new index descriptors.
	indexEncodingVersion := sqlbase.BaseIndexFormatVersion
	// We can't use st.Version.IsActive because this method is used during
	// server setup before the cluster version has been initialized.
	version := st.Version.ActiveVersionOrEmpty(ctx)
	if version != (clusterversion.ClusterVersion{}) &&
		version.IsActive(clusterversion.VersionSecondaryIndexColumnFamilies) {
		indexEncodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
	}

	for i, def := range n.Defs {
		if d, ok := def.(*tree.ColumnTableDef); ok {
			if err = checkColumnDef(ctx, d, &desc, n, st, sessionData, &columnDefaultExprs); err != nil {
				return desc, err
			}
			if err = addColToTblDesc(semaCtx, d, i, &desc, &columnDefaultExprs, indexEncodingVersion); err != nil {
				return desc, err
			}
		}
	}

	if n.IsTS() {
		generateTableFormatMetadata(&desc.TsTable, &desc.Columns)
		for _, tagColumn := range allTagDesc {
			desc.AddColumn(tagColumn)
		}
	}

	if err = parseAndSerializeComputeCol(ctx, n, &desc); err != nil {
		return desc, err
	}

	var primaryIndexColumnSet map[string]struct{}
	setupShardedIndexForNewTable := func(d *tree.IndexTableDef, idx *sqlbase.IndexDescriptor) error {
		if n.PartitionBy != nil {
			return pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		shardCol, newColumn, err := schema.SetupShardedIndex(
			ctx,
			st,
			sessionData.HashShardedIndexesEnabled,
			&d.Columns,
			d.Sharded.ShardBuckets,
			&desc,
			idx,
			true /* isNewTable */)
		if err != nil {
			return err
		}
		if newColumn {
			buckets, err := tree.EvalShardBucketCount(d.Sharded.ShardBuckets)
			if err != nil {
				return err
			}
			checkConstraint, err := schema.MakeShardCheckConstraintDef(&desc, int(buckets), shardCol)
			if err != nil {
				return err
			}
			n.Defs = append(n.Defs, checkConstraint)
			columnDefaultExprs = append(columnDefaultExprs, nil)
		}
		return nil
	}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// pass, handled above.
		case *tree.IndexTableDef:
			if desc.IsTSTable() {
				return desc, sqlbase.TSUnsupportedError("table def: index")
			}
			if err = buildIndexForDesc(ctx, st, evalCtx, d, &desc, setupShardedIndexForNewTable, indexEncodingVersion); err != nil {
				return desc, err
			}
		case *tree.UniqueConstraintTableDef:
			if desc.IsTSTable() {
				return desc, sqlbase.TSUnsupportedError("table def: unique")
			}
			if err = buildUniqueForDesc(ctx, st, evalCtx, d, &desc, n, setupShardedIndexForNewTable, indexEncodingVersion, &primaryIndexColumnSet); err != nil {
				return desc, err
			}
		case *tree.CheckConstraintTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("check constraint")
			}
		case *tree.ForeignKeyConstraintTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("referenced constraint")
			}
		case *tree.FamilyTableDef:
			if n.IsTS() {
				return desc, sqlbase.TSUnsupportedError("family")
			}
			// handled of relational table below.
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// If explicit primary keys are required, error out since a primary key was not supplied.
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() && evalCtx != nil &&
		evalCtx.SessionData != nil && evalCtx.SessionData.RequireExplicitPrimaryKeys {
		return desc, errors.Errorf(
			"no primary key specified for table %s (require_explicit_primary_keys = true)", desc.Name)
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[desc.Columns[i].Name]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	// Now that all columns are in place, add any explicit families (this is done
	// here, rather than in the constraint pass below since we want to pick up
	// explicit allocations before AllocateIDs adds implicit ones).
	columnsInExplicitFamilies := map[string]bool{}
	for _, def := range n.Defs {
		if d, ok := def.(*tree.FamilyTableDef); ok {
			buildFamilyForDesc(d, &desc, &columnsInExplicitFamilies)
		}
	}

	// Assign any implicitly added shard columns to the column family of the first column
	// in their corresponding set of index columns.
	for _, index := range desc.AllNonDropIndexes() {
		if index.IsSharded() && !columnsInExplicitFamilies[index.Sharded.Name] {
			// Ensure that the shard column wasn't explicitly assigned a column family
			// during table creation (this will happen when a create statement is
			// "roundtripped", for example).
			family := sqlbase.GetColumnFamilyForShard(&desc, index.Sharded.ColumnNames)
			if family != "" {
				if err := desc.AddColumnToFamilyMaybeCreate(index.Sharded.Name, family, false, false); err != nil {
					return desc, err
				}
			}
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return desc, err
	}

	// If any nodes are not at version VersionPrimaryKeyColumnsOutOfFamilyZero, then return an error
	// if a primary key column is not in column family 0.
	if st != nil {
		if err = checkColFamily(ctx, st, &desc); err != nil {
			return desc, err
		}
	}

	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		// Increment the counter if this index could be storing data across multiple column families.
		if len(idx.StoreColumnNames) > 1 && len(desc.Families) > 1 {
			telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
		}
	}

	if n.Interleave != nil {
		if err := AddInterleave(ctx, txn, vt, &desc, &desc.PrimaryIndex, n.Interleave); err != nil {
			return desc, err
		}
	}

	if n.PartitionBy != nil {
		partitioning, err := NewPartitioningDescriptor(
			ctx, evalCtx, &desc, &desc.PrimaryIndex, n.PartitionBy)
		if err != nil {
			return desc, err
		}
		desc.PrimaryIndex.Partitioning = partitioning
	}

	// Once all the IDs have been allocated, we can add the Sequence dependencies
	// as MaybeAddSequenceDependencies requires ColumnIDs to be correct.
	// Elements in n.Defs are not necessarily column definitions, so use a separate
	// counter to map ColumnDefs to columns.
	colIdx := 0
	for i := range n.Defs {
		if _, ok := n.Defs[i].(*tree.ColumnTableDef); ok {
			if expr := columnDefaultExprs[i]; expr != nil {
				changedSeqDescs, err := MaybeAddSequenceDependencies(ctx, vt, &desc, &desc.Columns[colIdx], expr, affected)
				if err != nil {
					return desc, err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					affected[changedSeqDesc.ID] = changedSeqDesc
				}
			}
			colIdx++
		}
	}

	// With all structural elements in place and IDs allocated, we can resolve the
	// constraints and qualifications.
	// FKs are resolved after the descriptor is otherwise complete and IDs have
	// been allocated since the FKs will reference those IDs. Resolution also
	// accumulates updates to other tables (adding backreferences) in the passed
	// map -- anything in that map should be saved when the table is created.
	//

	// We use a fkSelfResolver so that name resolution can find the newly created
	// table.
	fkResolver := &fkSelfResolver{
		SchemaResolver: vt,
		newTableDesc:   desc.TableDesc(),
		newTableName:   &n.Table,
	}

	generatedNames := map[string]struct{}{}
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			// Check after all ResolveFK calls.
		case *tree.IndexTableDef, *tree.UniqueConstraintTableDef, *tree.FamilyTableDef:
			// Pass, handled above.
		case *tree.CheckConstraintTableDef:
			ck, err := MakeCheckConstraint(ctx, &desc, d, generatedNames, semaCtx, n.Table)
			if err != nil {
				return desc, err
			}
			desc.Checks = append(desc.Checks, ck)
		case *tree.ForeignKeyConstraintTableDef:
			if err := ResolveFK(ctx, txn, fkResolver, &desc, d, affected, sqlconst.NewTable, tree.ValidationDefault, st); err != nil {
				return desc, err
			}
		default:
			return desc, errors.Errorf("unsupported table def: %T", def)
		}
	}

	// Now that we have all the other columns set up, we can validate
	// any computed columns.
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			if d.IsComputed() {
				if err := schema.ValidateComputedColumn(&desc, d, semaCtx); err != nil {
					return desc, err
				}
			}
		}
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err = desc.AllocateIDs()

	// Record the types of indexes that the table has.
	if err := desc.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		if idx.IsSharded() {
			telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
		}
		if idx.Type == sqlbase.IndexDescriptor_INVERTED {
			telemetry.Inc(sqltelemetry.InvertedIndexCounter)
		}
		return nil
	}); err != nil {
		return desc, err
	}

	return desc, err
}

func checkStorageParameters(
	semaCtx *tree.SemaContext,
	params tree.StorageParams,
	expectedTypes map[string]sqlconst.StorageParamType,
) error {
	for _, sp := range params {
		k := string(sp.Key)
		validate, ok := expectedTypes[k]
		if !ok {
			return errors.Errorf("invalid storage parameter %q", k)
		}
		if sp.Value == nil {
			return errors.Errorf("storage parameter %q requires a value", k)
		}
		var expectedType *types.T
		if validate == sqlconst.StorageParamBool {
			expectedType = types.Bool
		} else if validate == sqlconst.StorageParamInt {
			expectedType = types.Int
		} else if validate == sqlconst.StorageParamFloat {
			expectedType = types.Float
		} else {
			return unimplemented.NewWithIssuef(43299, "storage parameter %q", k)
		}

		_, err := tree.TypeCheckAndRequire(sp.Value, semaCtx, expectedType, k)
		if err != nil {
			return err
		}
	}
	return nil
}

// makeTableDesc creates a table descriptor from a CreateTable statement.
func makeTableDesc(
	params RunParams,
	n *tree.CreateTable,
	parentID, parentSchemaID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	affected map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	temporary bool,
) (ret sqlbase.MutableTableDescriptor, err error) {
	// Process any SERIAL columns to remove the SERIAL type,
	// as required by MakeTableDesc.
	createStmt := n
	ensureCopy := func() {
		if createStmt == n {
			newCreateStmt := *n
			n.Defs = append(tree.TableDefs(nil), n.Defs...)
			createStmt = &newCreateStmt
		}
	}
	for i, def := range n.Defs {
		d, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		// Do not include virtual tables in these statistics.
		if !sqlbase.IsVirtualTable(id) {
			IncTelemetryForNewColumn(d)
		}
		newDef, seqDbDesc, seqName, seqOpts, err := ProcessSerialInColumnDef(params.Ctx, params.GetPlanner(), d, &n.Table, n.IsTS())
		if err != nil {
			return ret, err
		}
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if seqName != nil {
			if err := DoCreateSequence(
				params,
				n.String(),
				seqDbDesc,
				parentSchemaID,
				seqName,
				temporary,
				seqOpts,
				"creating sequence",
				d.IsSerial,
			); err != nil {
				return ret, err
			}
		}
		if d != newDef {
			ensureCopy()
			n.Defs[i] = newDef
		}
	}

	// We need to run MakeTableDesc with caching disabled, because
	// it needs to pull in descriptors from FK depended-on tables
	// and interleaved parents using their current state in KV.
	// See the comment at the start of MakeTableDesc() and resolveFK().
	params.p.RunWithOptions(ResolveFlags{SkipCache: true}, func() {
		ret, err = MakeTableDesc(
			params.Ctx,
			params.PlannerTxn(),
			params.p,
			params.p.ExecCfg().Settings,
			n,
			parentID,
			parentSchemaID,
			id,
			creationTime,
			privileges,
			affected,
			&params.p.semaCtx,
			params.EvalContext(),
			params.SessionData(),
			temporary,
		)
	})
	return ret, err
}

func (p *GenericPlanner) removeStream(
	ctx context.Context, jobID int64, streamID uint64, tableID uint64,
) error {
	if jobID != 0 {
		// Close the job by closing the CDC
		p.ExecCfg().CDCCoordinator.StopCDCByLocal(tableID, streamID, sqlbase.CDCInstanceType_Stream)
		WaitCDCStatusChanged(ctx, p.ExecCfg().CDCCoordinator, tableID, streamID, sqlbase.CDCInstanceType_Stream, false)

		if err := p.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			job, _ := p.execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
			// After CDC is closed, the job status is usually StatusFailed,
			// and if the job status is not StatusFailed, CancelRequested is used to close it,
			// which usually takes 30 seconds.
			if job != nil {
				if status, err := job.WithTxn(txn).CurrentStatus(ctx); err == nil {
					if status == jobs.StatusRunning || status == jobs.StatusPending {
						_ = p.execCfg.JobRegistry.CancelRequested(ctx, txn, jobID)
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_streams WHERE id = $1",
		streamID,
	); err != nil {
		return err
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-stream-water-mark",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_cdc_watermark WHERE table_id = $1 AND task_id = $2 AND task_type = $3",
		tableID,
		streamID,
		sqlbase.CDCInstanceType_Stream,
	); err != nil {
		return err
	}

	return nil
}

// drainName when set implies that the name needs to go through the draining
// names process. This parameter is always passed in as true except from
// TRUNCATE which directly deletes the old name to id map and doesn't need
// drain the old map.
func (p *GenericPlanner) initiateDropTable(
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
	//ranges, err := sqlutil.ScanMetaKVs(ctx, p.txn, tableDesc.TableSpan())
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
		dbDesc, err := GetDatabaseDescByID(ctx, p.txn, tableDesc.ParentID)
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

func (p *GenericPlanner) removeFKForBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint,
) error {
	var originTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.OriginTableID {
		originTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().GetMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving origin table ID %d: %v", ref.OriginTableID, err)
		}
		originTableDesc = lookup
	}
	if originTableDesc.Dropped() {
		// The origin table is being dropped. No need to modify it further.
		return nil
	}

	if err := schema.RemoveFKForBackReferenceFromTable(originTableDesc, ref, tableDesc.TableDesc()); err != nil {
		return err
	}
	// No job description, since this is presumably part of some larger schema change.
	return p.WriteSchemaChange(ctx, originTableDesc, sqlbase.InvalidMutationID, "")
}

// removeFKBackReference removes the FK back reference from the table that is
// referenced by the input constraint.
func (p *GenericPlanner) removeFKBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint,
) error {
	var referencedTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().GetMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ref.ReferencedTableID, err)
		}
		referencedTableDesc = lookup
	}
	if referencedTableDesc.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}

	if err := schema.RemoveFKBackReferenceFromTable(referencedTableDesc, ref.Name, tableDesc.TableDesc()); err != nil {
		return err
	}
	// No job description, since this is presumably part of some larger schema change.
	return p.WriteSchemaChange(ctx, referencedTableDesc, sqlbase.InvalidMutationID, "")
}

// RemoveFKBackReferenceWrap is a wrapper
func RemoveFKBackReferenceWrap(
	ctx context.Context,
	p *GenericPlanner,
	tableDesc *sqlbase.MutableTableDescriptor,
	ref *sqlbase.ForeignKeyConstraint,
) error {
	return p.removeFKBackReference(ctx, tableDesc, ref)
}

func (p *GenericPlanner) removeInterleaveBackReference(
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
		lookup, err := p.Tables().GetMutableTableVersionByID(ctx, ancestor.TableID, p.txn)
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
		return p.WriteSchemaChange(
			ctx, t, sqlbase.InvalidMutationID, "removing reference for interleaved table",
		)
	}
	return nil
}

func (p *GenericPlanner) removeTableComment(
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
		err = RemoveIndexComment(ctx,
			p,
			tableDesc.ID,
			indexDesc.ID)
	}

	return err
}

// TryPurgeProcedureCache clears the cache and metadata of Procedure.
func (p *GenericPlanner) TryPurgeProcedureCache(
	ctx context.Context, procName tree.TableName, procID uint32,
) error {
	p.execCfg.ProcedureCache.Purge(procID)
	_, err := p.ExecCfg().InternalExecutor.Exec(
		ctx,
		"drop procedure",
		p.Txn(),
		fmt.Sprintf(
			`drop procedure %s`,
			procName.String(),
		),
	)
	return err
}

// GetProcedureNameByID obtains the procedure's name by ID.
func GetProcedureNameByID(
	ctx context.Context, ie *InternalExecutor, txn *kv.Txn, id sqlbase.ID,
) (bool, tree.TableName, error) {
	row, err := ie.QueryRow(
		ctx,
		"get procedure by ID",
		txn,
		fmt.Sprintf(
			`select db_id, schema_id, name FROM %s WHERE id=$1`,
			sqlconst.UDRTableName,
		),
		int64(id),
	)
	if err != nil {
		log.Infof(ctx, "trying to get procedure %d failed: %s", id, err)
		return false, tree.TableName{}, err
	}

	if row != nil {
		dbID := sqlbase.ID(tree.MustBeDInt(row[0]))
		scID := sqlbase.ID(tree.MustBeDInt(row[1]))
		name := tree.Name(tree.MustBeDString(row[2]))

		dbDesc, err := GetDatabaseDescByID(ctx, txn, dbID)
		if err != nil {
			return false, tree.TableName{}, err
		}
		scDesc, err := getSchemaDescByID(ctx, txn, scID)
		if err != nil {
			return false, tree.TableName{}, err
		}
		procName := tree.MakeTableName(tree.Name(dbDesc.Name), name)
		procName.SchemaName = tree.Name(scDesc.Name)
		return true, procName, nil
	}
	return false, tree.TableName{}, nil
}

// ShowForeignKeyConstraint returns a valid SQL representation of a FOREIGN KEY
// clause for a given index.
func ShowForeignKeyConstraint(
	buf *bytes.Buffer,
	dbPrefix string,
	originTable *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	lCtx *InternalLookupCtx,
) error {
	var refNames []string
	var originNames []string
	var fkTableName tree.TableName
	if lCtx != nil {
		fkTable, err := lCtx.GetTableByID(fk.ReferencedTableID)
		if err != nil {
			return err
		}
		fkDb, err := lCtx.GetDatabaseByID(fkTable.ParentID)
		if err != nil {
			return err
		}
		refNames, err = fkTable.NamesForColumnIDs(fk.ReferencedColumnIDs)
		if err != nil {
			return err
		}
		fkTableName = tree.MakeTableName(tree.Name(fkDb.Name), tree.Name(fkTable.Name))
		fkTableName.ExplicitSchema = fkDb.Name != dbPrefix
		originNames, err = originTable.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return err
		}
	} else {
		refNames = []string{"???"}
		originNames = []string{"???"}
		fkTableName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as ref]", fk.ReferencedTableID)))
		fkTableName.ExplicitSchema = false
	}
	buf.WriteString("FOREIGN KEY (")
	sqlutil.FormatQuoteNames(buf, originNames...)
	buf.WriteString(") REFERENCES ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&fkTableName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString("(")
	sqlutil.FormatQuoteNames(buf, refNames...)
	buf.WriteByte(')')
	// We omit MATCH SIMPLE because it is the default.
	if fk.Match != sqlbase.ForeignKeyReference_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(fk.Match.String())
	}
	if fk.OnDelete != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(fk.OnUpdate.String())
	}
	return nil
}

// SendDropTableStmtToPipe sends the drop table stmt to pipe.
func SendDropTableStmtToPipe(
	params RunParams, toDeletes map[sqlbase.ID]TableToDelete, ddlType string, stmt string,
) error {
	for _, toDel := range toDeletes {
		droppedDesc := toDel.Desc
		if droppedDesc == nil {
			continue
		}
		if !droppedDesc.IsTSTable() {
			continue
		}

		pipeMetadatas, err := CheckTableRelatedPipe(
			params.Ctx, params.GetPlanner(), uint64(droppedDesc.ID), uint64(droppedDesc.ParentID))
		if err != nil {
			return err
		}
		if len(pipeMetadatas) == 0 {
			continue
		}

		dbName := string(toDel.Tn.CatalogName)
		schemaName := string(toDel.Tn.SchemaName)
		tableName := string(toDel.Tn.TableName)
		if err = SendDDLToPipe(
			params, dbName, schemaName, tableName, ddlType, stmt, pipeMetadatas, true,
		); err != nil {
			return err
		}
	}

	return nil
}

// RemoveMatchingReferences removes all refs from the provided slice that
// match the provided ID, returning the modified slice.
func RemoveMatchingReferences(
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

// DoCreateSequence performs the creation of a sequence in KV. The
// context argument is a string to use in the event log.
func DoCreateSequence(
	params RunParams,
	context string,
	dbDesc *DatabaseDescriptor,
	schemaID sqlbase.ID,
	name *ObjectName,
	isTemporary bool,
	opts tree.SequenceOptions,
	jobDesc string,
	isSerial bool,
) error {
	id, err := GenerateUniqueDescID(params.Ctx, params.p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := dbDesc.GetPrivileges()

	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempSequenceCounter)
	}

	time, err := params.CreationTimeForNewTableDescriptor()
	if err != nil {
		return err
	}

	desc, err := MakeSequenceTableDesc(
		name.Table(),
		opts,
		dbDesc.ID,
		schemaID,
		id,
		time,
		privs,
		isTemporary,
		&params,
	)
	if err != nil {
		return err
	}
	desc.SequenceOpts.IsSerial = isSerial
	// makeSequenceTableDesc already validates the table. No call to
	// desc.ValidateTable() needed here.

	key := sqlbase.MakeObjectNameKey(
		params.Ctx,
		params.ExecCfg().Settings,
		dbDesc.ID,
		schemaID,
		name.Table(),
	).Key()
	if err = params.p.CreateDescriptorWithID(
		params.Ctx, key, id, &desc, params.EvalContext().Settings, jobDesc,
	); err != nil {
		return err
	}

	// Initialize the sequence value.
	seqValueKey := keys.MakeSequenceKey(uint32(id))
	b := &kv.Batch{}
	b.Inc(seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment)
	if err := params.p.txn.Run(params.Ctx, b); err != nil {
		return err
	}

	if err := desc.Validate(params.Ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create Sequence event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	params.p.SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	return nil
}

// MakeSequenceTableDesc creates a sequence descriptor.
func MakeSequenceTableDesc(
	sequenceName string,
	sequenceOptions tree.SequenceOptions,
	parentID sqlbase.ID,
	schemaID sqlbase.ID,
	id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	isTemporary bool,
	params *RunParams,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(
		id,
		parentID,
		schemaID,
		sequenceName,
		creationTime,
		privileges,
		isTemporary,
		tree.RelationalTable,
		"",
	)

	// Mimic a table with one column, "value".
	desc.Columns = []sqlbase.ColumnDescriptor{
		{
			ID:   1,
			Name: sqlconst.SequenceColumnName,
			Type: *types.Int,
		},
	}
	desc.PrimaryIndex = sqlbase.IndexDescriptor{
		ID:               keys.SequenceIndexID,
		Name:             sqlbase.PrimaryKeyIndexName,
		ColumnIDs:        []sqlbase.ColumnID{sqlbase.ColumnID(1)},
		ColumnNames:      []string{sqlconst.SequenceColumnName},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
	}
	desc.Families = []sqlbase.ColumnFamilyDescriptor{
		{
			ID:              keys.SequenceColumnFamilyID,
			ColumnIDs:       []sqlbase.ColumnID{1},
			ColumnNames:     []string{sqlconst.SequenceColumnName},
			Name:            "primary",
			DefaultColumnID: sqlconst.SequenceColumnID,
		},
	}

	// Fill in options, starting with defaults then overriding.
	opts := &sqlbase.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	err := AssignSequenceOptions(opts, sequenceOptions, true /* setDefaults */, params, id)
	if err != nil {
		return desc, err
	}
	desc.SequenceOpts = opts

	// A sequence doesn't have dependencies and thus can be made public
	// immediately.
	desc.State = sqlbase.TableDescriptor_PUBLIC

	return desc, desc.ValidateTable()
}

// RemoveColumnComment removes the comment associated with a column
func RemoveColumnComment(
	ctx context.Context,
	p *GenericPlanner,
	txn *kv.Txn,
	tableID sqlbase.ID,
	columnID sqlbase.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-column-comment",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ColumnCommentType,
		tableID,
		columnID)

	return err
}

// RemoveIndexComment removes the comment associated with an index
func RemoveIndexComment(
	ctx context.Context, p *GenericPlanner, tableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	_, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-index-comment",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.IndexCommentType,
		tableID,
		indexID)

	return err
}

// DropIndexByName drops an index from a table by its name
func DropIndexByName(
	ctx context.Context,
	p *GenericPlanner,
	tn *tree.TableName,
	idxName tree.UnrestrictedName,
	tableDesc *sqlbase.MutableTableDescriptor,
	ifExists bool,
	behavior tree.DropBehavior,
	constraintBehavior sqlconst.DropIndexConstraintBehavior,
	jobDesc string,
) error {
	idx, dropped, err := tableDesc.FindIndexByName(string(idxName))
	if err != nil {
		// Only index names of the form "table@idx" throw an error here if they
		// don't exist.
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return err
	}
	if dropped {
		return nil
	}

	// Check if requires CCL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(ctx, p.txn, uint32(tableDesc.ID), nil, "", false)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(idx.ID) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), tableDesc.TableDesc(), zone.Subzones, false /* newSubzones */)
			break
		}
	}

	// Remove all foreign key references and backreferences from the index.
	// TODO (lucy): This is incorrect for two reasons: The first is that FKs won't
	// be restored if the DROP INDEX is rolled back, and the second is that
	// validated constraints should be dropped in the schema changer in multiple
	// steps to avoid inconsistencies. We should be queuing a mutation to drop the
	// FK instead. The reason why the FK is removed here is to keep the index
	// state consistent with the removal of the reference on the other table
	// involved in the FK, in case of rollbacks (#38733).

	// TODO (rohany): switching all the checks from checking the legacy ID's to
	//  checking if the index has a prefix of the columns needed for the foreign
	//  key might result in some false positives for this index while it is in
	//  a mixed version cluster, but we have to remove all reads of the legacy
	//  explicit index fields.

	// Construct a list of all the remaining indexes, so that we can see if there
	// is another index that could replace the one we are deleting for a given
	// foreign key constraint.
	remainingIndexes := make([]*sqlbase.IndexDescriptor, 0, len(tableDesc.Indexes)+1)
	remainingIndexes = append(remainingIndexes, &tableDesc.PrimaryIndex)
	for i := range tableDesc.Indexes {
		index := &tableDesc.Indexes[i]
		if index.ID != idx.ID {
			remainingIndexes = append(remainingIndexes, index)
		}
	}

	// indexHasReplacementCandidate runs isValidIndex on each index in remainingIndexes and returns
	// true if at least one index satisfies isValidIndex.
	indexHasReplacementCandidate := func(isValidIndex func(*sqlbase.IndexDescriptor) bool) bool {
		foundReplacement := false
		for _, index := range remainingIndexes {
			if isValidIndex(index) {
				foundReplacement = true
				break
			}
		}
		return foundReplacement
	}
	// If we aren't at the cluster version where we have removed explicit foreign key IDs
	// from the foreign key descriptors, fall back to the existing drop index logic.
	// That means we pretend that we can never find replacements for any indexes.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionNoExplicitForeignKeyIndexIDs) {
		indexHasReplacementCandidate = func(func(*sqlbase.IndexDescriptor) bool) bool {
			return false
		}
	}

	// Check for foreign key mutations referencing this index.
	for _, m := range tableDesc.Mutations {
		if c := m.GetConstraint(); c != nil &&
			c.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
			// If the index being deleted could be used as a index for this outbound
			// foreign key mutation, then make sure that we have another index that
			// could be used for this mutation.
			idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs) &&
			!indexHasReplacementCandidate(func(idx *sqlbase.IndexDescriptor) bool {
				return idx.IsValidOriginIndex(c.ForeignKey.OriginColumnIDs)
			}) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"referencing constraint %q in the middle of being added, try again later", c.ForeignKey.Name)
		}
	}

	if err := p.MaybeUpgradeDependentOldForeignKeyVersionTables(ctx, tableDesc); err != nil {
		return err
	}

	// Index for updating the FK slices in place when removing FKs.
	sliceIdx := 0
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[sliceIdx] = tableDesc.OutboundFKs[i]
		sliceIdx++
		fk := &tableDesc.OutboundFKs[i]
		canReplace := func(idx *sqlbase.IndexDescriptor) bool {
			return idx.IsValidOriginIndex(fk.OriginColumnIDs)
		}
		// The index being deleted could be used as the origin index for this foreign key.
		if idx.IsValidOriginIndex(fk.OriginColumnIDs) && !indexHasReplacementCandidate(canReplace) {
			if behavior != tree.DropCascade && constraintBehavior != sqlconst.IgnoreIdxConstraint {
				return errors.Errorf("index %q is in use as a foreign key constraint", idx.Name)
			}
			sliceIdx--
			if err := p.removeFKBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	tableDesc.OutboundFKs = tableDesc.OutboundFKs[:sliceIdx]

	sliceIdx = 0
	for i := range tableDesc.InboundFKs {
		tableDesc.InboundFKs[sliceIdx] = tableDesc.InboundFKs[i]
		sliceIdx++
		fk := &tableDesc.InboundFKs[i]
		canReplace := func(idx *sqlbase.IndexDescriptor) bool {
			return idx.IsValidReferencedIndex(fk.ReferencedColumnIDs)
		}
		// The index being deleted could potentially be the referenced index for this fk.
		if idx.IsValidReferencedIndex(fk.ReferencedColumnIDs) &&
			// If we haven't found a replacement candidate for this foreign key, then
			// we need a cascade to delete this index.
			!indexHasReplacementCandidate(canReplace) {
			// If we found haven't found a replacement, then we check that the drop behavior is cascade.
			if err := CanRemoveFKBackreference(ctx, p, idx.Name, fk, behavior); err != nil {
				return err
			}
			sliceIdx--
			if err := p.removeFKForBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	tableDesc.InboundFKs = tableDesc.InboundFKs[:sliceIdx]

	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}
	for _, ref := range idx.InterleavedBy {
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if idx.Unique && behavior != tree.DropCascade && constraintBehavior != sqlconst.IgnoreIdxConstraint && !idx.CreatedExplicitly {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", idx.Name)
	}

	var droppedViews []string
	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := CanRemoveDependentViewGeneric(
				ctx, p, "index", idx.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, skipped, err := GetViewDescForCascade(
				ctx, p, "index", idx.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			if skipped {
				// In the table whose index is being removed, filter out all back-references
				// that refer to the view that's being removed.
				tableDesc.DependedOnBy = RemoveMatchingReferences(tableDesc.DependedOnBy, tableRef.ID)
				continue
			}
			viewJobDesc := fmt.Sprintf("removing view %q dependent on index %q which is being dropped",
				viewDesc.Name, idx.Name)
			cascadedViews, err := RemoveDependentView(ctx, p, tableDesc, viewDesc, viewJobDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the idx object we collected above. Make a copy.
	idxCopy := *idx
	idx = &idxCopy
	if !tableDesc.IsTSTable() {
		found := false
		for i, idxEntry := range tableDesc.Indexes {
			if idxEntry.ID == idx.ID {
				// Unsplit all manually split ranges in the index so they can be
				// automatically merged by the merge queue.
				span := tableDesc.IndexSpan(idxEntry.ID)
				ranges, err := sqlutil.ScanMetaKVs(ctx, p.txn, span)
				if err != nil {
					return err
				}
				for _, r := range ranges {
					var desc roachpb.RangeDescriptor
					if err := r.ValueProto(&desc); err != nil {
						return err
					}
					// We have to explicitly check that the range descriptor's start key
					// lies within the span of the index since ScanMetaKVs returns all
					// intersecting spans.
					if (desc.GetStickyBit() != hlc.Timestamp{}) && span.Key.Compare(desc.StartKey.AsRawKey()) <= 0 {
						// Swallow "key is not the start of a range" errors because it would
						// mean that the sticky bit was removed and merged concurrently. DROP
						// INDEX should not fail because of this.
						if err := p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
							return err
						}
					}
				}

				// the idx we picked up with FindIndexByID at the top may not
				// contain the same field any more due to other schema changes
				// intervening since the initial lookup. So we send the recent
				// copy idxEntry for drop instead.
				if err := tableDesc.AddIndexMutation(&idxEntry, sqlbase.DescriptorMutation_DROP); err != nil {
					return err
				}
				tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
		}

		if err := RemoveIndexComment(ctx, p, tableDesc.ID, idx.ID); err != nil {
			return err
		}
	} else {
		found := false
		for _, idxEntry := range tableDesc.Indexes {
			if idxEntry.ID == idx.ID {
				if err := tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_DROP); err != nil {
					return err
				}
				//tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
		}
	}

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return err
	}
	mutationID := tableDesc.ClusterVersion.NextMutationID

	if tableDesc.IsTSTable() {
		// creates and exec drop tag index job
		syncDetail := jobspb.SyncMetaCacheDetails{
			Type:                  DropTagIndex,
			SNTable:               tableDesc.TableDescriptor,
			CreateOrAlterTagIndex: *idx,
			MutationID:            mutationID,
		}
		jobID, err := CreateTSSchemaChangeJob(ctx, p, syncDetail, p.stmt.SQL, p.txn)
		if err != nil {
			return err
		}
		if mutationID != sqlbase.InvalidMutationID {
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: jobID})
		}
		if err = p.writeTableDesc(ctx, tableDesc); err != nil {
			return err
		}
		// Actively commit a transaction, and read/write system table operations
		// need to be performed before this.
		if err = p.txn.Commit(ctx); err != nil {
			return err
		}

		//After the transaction commits successfully, execute the Job and wait for it to complete.
		if err = p.ExecCfg().JobRegistry.Run(
			ctx,
			p.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
			[]int64{jobID},
		); err != nil {
			return err
		}
		log.Infof(ctx, "drop tag index %s 1st txn finished, id: %d", string(idxName), idx.ID)
	} else {
		if err := p.WriteSchemaChange(ctx, tableDesc, mutationID, jobDesc); err != nil {
			return err
		}
		p.SendClientNotice(ctx,
			errors.WithHint(
				pgerror.Noticef("the data for dropped indexes is reclaimed asynchronously"),
				"The reclamation delay can be customized in the zone configuration for the table."))
	}

	p.SetAuditTarget(uint32(idx.ID), idx.Name, droppedViews)
	return nil
}
