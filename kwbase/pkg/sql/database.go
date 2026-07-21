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
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

//
// This file contains routines for low-level access to stored database
// descriptors, as well as accessors for the database cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

// databaseCache holds a cache from database name to database ID. It is
// populated as database IDs are requested and a new cache is created whenever
// the system config changes. As such, no attempt is made to limit its size
// which is naturally limited by the number of database descriptors in the
// system the periodic reset whenever the system config is gossiped.
type databaseCache struct {
	// databases is really a map of string -> sqlbase.ID
	databases sync.Map

	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig *config.SystemConfig
}

func newDatabaseCache(cfg *config.SystemConfig) *databaseCache {
	return &databaseCache{
		systemConfig: cfg,
	}
}

func (dc *databaseCache) getID(name string) sqlbase.ID {
	val, ok := dc.databases.Load(name)
	if !ok {
		return sqlbase.InvalidID
	}
	return val.(sqlbase.ID)
}

func (dc *databaseCache) setID(name string, id sqlbase.ID) {
	dc.databases.Store(name, id)
}

// MakeDatabaseDesc creates a database descriptor from a CREATE DATABASE statement
func MakeDatabaseDesc(p *tree.CreateDatabase) sqlbase.DatabaseDescriptor {
	return sqlbase.DatabaseDescriptor{
		Name:       string(p.Name),
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}
}

// getDatabaseID resolves a database name into a database ID.
// Returns InvalidID on failure.
func getDatabaseID(
	ctx context.Context, txn *kv.Txn, name string, required bool,
) (sqlbase.ID, error) {
	if name == sqlbase.SystemDB.Name {
		return sqlbase.SystemDB.ID, nil
	}
	found, dbID, err := sqlbase.LookupDatabaseID(ctx, txn, name)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !found && required {
		return dbID, sqlbase.NewUndefinedDatabaseError(name)
	}
	return dbID, nil
}

// GetDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDescByID() instead.
func GetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	desc := &sqlbase.DatabaseDescriptor{}
	if err := getDescriptorByID(ctx, txn, id, desc); err != nil {
		return nil, err
	}
	return desc, nil
}

// getSchemaDescByID looks up the schema descriptor given its ID,
// returning nil if the descriptor is not found.
func getSchemaDescByID(
	ctx context.Context, txn *kv.Txn, schemaID sqlbase.ID,
) (*sqlbase.SchemaDescriptor, error) {
	if schemaID == keys.PublicSchemaID {
		return &sqlbase.SchemaDescriptor{Name: tree.PublicSchema, ID: keys.PublicSchemaID}, nil
	}
	desc := &sqlbase.SchemaDescriptor{}
	if err := getDescriptorByID(ctx, txn, schemaID, desc); err != nil {
		return nil, err
	}
	return desc, nil
}

// getTempTableSchemaNameByID looks up the schema name given its ID,
// returning an error if the schema is not found.
func getTempTableSchemaNameByID(
	ctx context.Context, txn *kv.Txn, dbID, schemaID sqlbase.ID,
) (string, error) {
	AllNameSpace, err := GetNameSpaceByParentID(ctx, txn, dbID, keys.RootNamespaceID)
	if err != nil {
		return "", err
	}
	for i := range AllNameSpace {
		n := AllNameSpace[i]
		if n.ID == uint64(schemaID) {
			return n.Name, nil
		}
	}
	IDFormat := fmt.Sprintf("(%d)", schemaID)
	return "", sqlbase.NewUndefinedSchemaError(IDFormat)
}

// MustGetDatabaseDescByID looks up the database descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	desc, err := GetDatabaseDescByID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return desc, nil
}

// getCachedDatabaseDesc looks up the database descriptor from the descriptor cache,
// given its name. Returns nil and no error if the name is not present in the
// cache.
func (dc *databaseCache) getCachedDatabaseDesc(name string) (*sqlbase.DatabaseDescriptor, error) {
	dbID, err := dc.getCachedDatabaseID(name)
	if dbID == sqlbase.InvalidID || err != nil {
		return nil, err
	}

	return dc.getCachedDatabaseDescByID(dbID)
}

// getCachedDatabaseDescByID looks up the database descriptor from the descriptor cache,
// given its ID.
func (dc *databaseCache) getCachedDatabaseDescByID(
	id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	if id == sqlbase.SystemDB.ID {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return &sysDB, nil
	}

	descKey := sqlbase.MakeDescMetadataKey(id)
	descVal := dc.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, nil
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	database := desc.GetDatabase()
	if database == nil {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "[%d] is not a database", id)
	}

	return database, database.Validate()
}

// getDatabaseDesc returns the database descriptor given its name
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *databaseCache) getDatabaseDesc(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *kv.Txn) error) error,
	name string,
	required bool,
) (*sqlbase.DatabaseDescriptor, error) {
	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	// The cache lookup may fail.
	desc, err := dc.getCachedDatabaseDesc(name)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if err := txnRunner(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the descriptor read as high-priority, thereby pushing any intents out
			// of its way. We don't want schema changes to prevent database lookup;
			// we'd rather force them to refresh. Also this prevents deadlocks in cases
			// where the name resolution is triggered by the transaction doing the
			// schema change itself.
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				return err
			}
			a := UncachedPhysicalAccessor{}
			desc, err = a.GetDatabaseDesc(ctx, txn, name,
				tree.DatabaseLookupFlags{Required: required})
			return err
		}); err != nil {
			return nil, err
		}
	}
	if desc != nil {
		dc.setID(name, desc.ID)
	}
	return desc, err
}

// getDatabaseDescByID returns the database descriptor given its ID
// if it exists in the cache, otherwise falls back to KV operations.
func (dc *databaseCache) getDatabaseDescByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID,
) (*sqlbase.DatabaseDescriptor, error) {
	desc, err := dc.getCachedDatabaseDescByID(id)
	if desc == nil || err != nil {
		if err != nil {
			log.VEventf(ctx, 3, "error getting database descriptor from cache: %s", err)
		}
		desc, err = MustGetDatabaseDescByID(ctx, txn, id)
	}
	return desc, err
}

// getDatabaseID returns the ID of a database given its name. It
// uses the descriptor cache if possible, otherwise falls back to KV
// operations.
func (dc *databaseCache) getDatabaseID(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *kv.Txn) error) error,
	name string,
	required bool,
) (sqlbase.ID, error) {
	dbID, err := dc.getCachedDatabaseID(name)
	if err != nil {
		return dbID, err
	}
	if dbID == sqlbase.InvalidID {
		if err := txnRunner(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Run the namespace read as high-priority, thereby pushing any intents out
			// of its way. We don't want schema changes to prevent database acquisitions;
			// we'd rather force them to refresh. Also this prevents deadlocks in cases
			// where the name resolution is triggered by the transaction doing the
			// schema change itself.
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				return err
			}
			var err error
			dbID, err = getDatabaseID(ctx, txn, name, required)
			return err
		}); err != nil {
			return sqlbase.InvalidID, err
		}
	}
	dc.setID(name, dbID)
	return dbID, nil
}

// getCachedDatabaseID returns the ID of a database given its name
// from the cache. This method never goes to the store to resolve
// the name to id mapping. Returns InvalidID if the name to id mapping or
// the database descriptor are not in the cache.
func (dc *databaseCache) getCachedDatabaseID(name string) (sqlbase.ID, error) {
	if id := dc.getID(name); id != sqlbase.InvalidID {
		return id, nil
	}

	if name == sqlbase.SystemDB.Name {
		return sqlbase.SystemDB.ID, nil
	}

	var nameKey sqlbase.DescriptorKey = sqlbase.NewDatabaseKey(name)
	nameVal := dc.systemConfig.GetValue(nameKey.Key())
	if nameVal == nil {
		// Try the deprecated system.namespace before returning InvalidID.
		// TODO(solon): This can be removed in 20.2.
		nameKey = sqlbase.NewDeprecatedDatabaseKey(name)
		nameVal = dc.systemConfig.GetValue(nameKey.Key())
		if nameVal == nil {
			return sqlbase.InvalidID, nil
		}
	}

	id, err := nameVal.GetInt()
	if sqlbase.ID(id) == sqlbase.InvalidID {
		return sqlbase.InvalidID, sqlbase.NewDropTSDBError(name)
	}
	return sqlbase.ID(id), err
}

// RenameDatabase handles the renaming of a database
func RenameDatabase(
	ctx context.Context, p *GenericPlanner, oldDesc *sqlbase.DatabaseDescriptor, newName string,
) error {
	oldName := oldDesc.Name
	oldDesc.SetName(newName)
	if err := oldDesc.Validate(); err != nil {
		return err
	}

	if exists, _, err := sqlbase.LookupDatabaseID(ctx, p.txn, newName); err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	newKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, newName).Key()

	descID := oldDesc.GetID()
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(oldDesc)

	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	err := sqlbase.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, oldName, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	p.Tables().AddUncommittedDatabase(oldName, descID, sqlconst.DbDropped)
	p.Tables().AddUncommittedDatabase(newName, descID, sqlconst.DbCreated)

	return p.txn.Run(ctx, b)
}
