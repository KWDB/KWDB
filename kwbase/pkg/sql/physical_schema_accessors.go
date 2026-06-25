// Copyright 2018 The Cockroach Authors.
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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// This file provides reference implementations of the schema accessor
// interface defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemas, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - UncachedPhysicalAccessor, for uncached db accessors
//
// - CachedPhysicalAccessor, which adds an object cache
//   - plugged on top another SchemaAccessor.
//   - uses a `*TableCollection` (table.go) as cache.
//

// UncachedPhysicalAccessor implements direct access to DB descriptors,
// without any kind of caching.
type UncachedPhysicalAccessor struct{}

var _ SchemaAccessor = UncachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	if name == sqlbase.SystemDB.Name {
		// We can't return a direct reference to SystemDB, because the
		// caller expects a private object that can be modified in-place.
		sysDB := sqlbase.MakeSystemDatabaseDesc()
		return &sysDB, nil
	}

	found, descID, err := sqlbase.LookupDatabaseID(ctx, txn, name)
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
			return nil, sqlbase.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}

	desc = &sqlbase.DatabaseDescriptor{}
	if err := getDescriptorByID(ctx, txn, descID, desc); err != nil {
		return nil, err
	}

	return desc, nil
}

// GetSchema implements the Accessor interface.
func (a UncachedPhysicalAccessor) GetSchema(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if scName == tree.PublicSchema {
		return true, sqlbase.ResolvedSchema{ID: keys.PublicSchemaID, Kind: sqlbase.SchemaPublic, Name: scName}, nil
	}

	// Lookup the schema ID.
	exists, schemaID, err := resolveSchemaID(ctx, txn, dbID, scName)
	if err != nil || !exists {
		return exists, sqlbase.ResolvedSchema{}, err
	}

	// The temporary schema doesn't have a descriptor, only a namespace entry.
	// Note that just performing this string check on the schema name is safe
	// because no user defined schemas can have the prefix "pg_".
	if strings.HasPrefix(scName, sessiondata.PgTempSchemaName) {
		return true, sqlbase.ResolvedSchema{ID: schemaID, Kind: sqlbase.SchemaTemporary, Name: scName}, nil
	}

	schema := &sqlbase.SchemaDescriptor{}
	// Get the descriptor from disk.
	if err := getDescriptorByID(ctx, txn, schemaID, schema); err != nil {
		return false, sqlbase.ResolvedSchema{}, err
	}

	return true, sqlbase.ResolvedSchema{
		ID:   schema.GetID(),
		Kind: sqlbase.SchemaUserDefined,
		Name: scName,
		Desc: schema,
	}, nil
}

// GetObjectNames implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (TableNames, error) {
	ok, schema, err := a.GetSchema(ctx, txn, dbDesc.ID, scName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.TableNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.Name)
	prefix := sqlbase.NewTableKey(dbDesc.ID, schema.ID, "").Key()
	sr, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	alreadySeen := make(map[string]bool)
	var tableNames tree.TableNames

	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(bytes.TrimPrefix(
			row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		alreadySeen[tableName] = true
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
	}

	// When constructing the list of entries under the `public` schema (and only
	// when constructing the list for the `public` schema), We scan both the
	// deprecated and new system.namespace table to get the complete list of
	// tables. Duplicate entries may be present in both the tables, so we filter
	// those out. If a duplicate entry is present, it doesn't matter which table
	// it is read from -- system.namespace entries are never modified, they are
	// only added/deleted. Entries are written to only one table, so duplicate
	// entries must have been copied over during migration. Thus, it doesn't
	// matter which table (newer/deprecated) the value is read from.
	//
	// It may seem counter-intuitive to read both tables if we have found data in
	// the newer version. The migration copied all entries from the deprecated
	// system.namespace and all new entries after the cluster version bump are added
	// to the new system.namespace. Why do we do this then?
	// This is to account the scenario where a table was created before
	// the cluster version was bumped, but after the older system.namespace was
	// copied into the newer system.namespace. Objects created in this window
	// will only be present in the older system.namespace. To account for this
	// scenario, we must do this filtering logic.
	// TODO(solon): This complexity can be removed in  20.2.
	if scName != tree.PublicSchema {
		return tableNames, nil
	}

	dprefix := sqlbase.NewDeprecatedTableKey(dbDesc.GetID(), "").Key()
	dsr, err := txn.Scan(ctx, dprefix, dprefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	for _, row := range dsr {
		// Decode using the deprecated key prefix.
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, dprefix), nil)
		if err != nil {
			return nil, err
		}
		if alreadySeen[tableName] {
			continue
		}
		tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.GetName()), tree.Name(scName), tree.Name(tableName))
		tn.ExplicitCatalog = flags.ExplicitPrefix
		tn.ExplicitSchema = flags.ExplicitPrefix
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	name *ObjectName,
	flags tree.ObjectLookupFlags,
) (ObjectDescriptor, error) {
	// Look up the database ID.
	dbID, err := getDatabaseID(ctx, txn, name.Catalog(), flags.Required)
	if err != nil || dbID == sqlbase.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	ok, schema, err := a.GetSchema(ctx, txn, dbID, name.Schema())
	if err != nil {
		return nil, err
	}
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(name))
		}
		return nil, nil
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := sqlbase.LookupSystemTableDescriptorID(ctx, settings, dbID, name.Table())
	if descID == sqlbase.InvalidID {
		var found bool
		found, descID, err = sqlbase.LookupObjectID(ctx, txn, dbID, schema.ID, name.Table())
		if err != nil {
			return nil, err
		}
		if !found {
			// KV name resolution failed.
			if flags.Required {
				return nil, sqlbase.NewUndefinedRelationError(name)
			}
			return nil, nil
		}
	}

	// Look up the table using the discovered database descriptor.
	desc := &sqlbase.TableDescriptor{}
	err = getDescriptorByID(ctx, txn, descID, desc)
	if err != nil {
		return nil, err
	}

	// We have a descriptor, allow it to be in the PUBLIC or ADD state. Possibly
	// OFFLINE if the relevant flag is set.
	acceptableStates := map[sqlbase.TableDescriptor_State]bool{
		sqlbase.TableDescriptor_ADD:     true,
		sqlbase.TableDescriptor_PUBLIC:  true,
		sqlbase.TableDescriptor_OFFLINE: flags.IncludeOffline,
	}
	if acceptableStates[desc.State] {
		// Immediately after a RENAME an old name still points to the
		// descriptor during the drain phase for the name. Do not
		// return a descriptor during draining.
		//
		// The second or condition ensures that clusters < 20.1 access the
		// system.namespace_deprecated table when selecting from system.namespace.
		// As this table can not be renamed by users, it is okay that the first
		// check fails.
		if desc.Name == name.Table() ||
			name.Table() == sqlbase.NamespaceTableName && name.Catalog() == sqlbase.SystemDB.Name {
			if flags.RequireMutable {
				return sqlbase.NewMutableExistingTableDescriptor(*desc), nil
			}
			return sqlbase.NewImmutableTableDescriptor(*desc), nil
		}
	}

	if desc.IsTSTable() {
		if err := desc.CheckTSTableStateValid(); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// CachedPhysicalAccessor adds a cache on top of any SchemaAccessor.
type CachedPhysicalAccessor struct {
	SchemaAccessor
	tc *TableCollection
}

var _ SchemaAccessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.AvoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.Required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.getDatabaseDescByID(ctx, txn, dbID)
			if desc == nil && flags.Required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.getDatabaseDesc(ctx, a.tc.leaseMgr.db.Txn, name, flags.Required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(ctx, txn, name, flags)
}

// GetSchema implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetSchema(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ResolvedSchema, error) {
	return a.tc.ResolveSchema(ctx, txn, dbID, scName)
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	name *ObjectName,
	flags tree.ObjectLookupFlags,
) (ObjectDescriptor, error) {
	if flags.RequireMutable {
		table, err := a.tc.getMutableTableDescriptor(ctx, txn, name, flags)
		if table == nil {
			// return nil interface.
			return nil, err
		}

		// In order not to affect the desc in the cache, when the table is a super table,
		// a new address needs to be created to transfer the desc, as it will be modified later.
		if table.IsTSTable() {
			if err = table.TableDescriptor.CheckTSTableStateValid(); err != nil {
				return nil, err
			}
			temp := *table
			return &temp, err
		}
		return table, err
	}
	table, err := a.tc.getTableVersion(ctx, txn, name, flags)
	if table == nil {
		// return nil interface.
		return nil, err
	}

	// In order not to affect the desc in the cache, when the table is a super table,
	// a new address needs to be created to transfer the desc, as it will be modified later.
	if table.IsTSTable() {
		if err = table.TableDescriptor.CheckTSTableStateValid(); err != nil {
			return nil, err
		}
		temp := *table
		return &temp, err
	}
	return table, err
}
