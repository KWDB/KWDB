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
	"context"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// optCatalog implements the cat.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// planner needs to be set via a call to init before calling other methods.
	planner *planner

	// cfg is the gossiped and cached system config. It may be nil if the node
	// does not yet have it available.
	cfg *config.SystemConfig

	// dataSources is a cache of table and view objects that's used to satisfy
	// repeated calls for the same data source.
	// Note that the data source object might still need to be recreated if
	// something outside of the descriptor has changed (e.g. table stats).
	dataSources map[*sqlbase.ImmutableTableDescriptor]cat.DataSource

	// tn is a temporary name used during resolution to avoid heap allocation.
	tn tree.TableName
}

var _ cat.Catalog = &optCatalog{}

// init initializes an optCatalog instance (which the caller can pre-allocate).
// The instance can be used across multiple queries, but reset() should be
// called for each query.
func (oc *optCatalog) init(planner *planner) {
	oc.planner = planner
	oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
}

// reset prepares the optCatalog to be used for a new query.
func (oc *optCatalog) reset() {
	// If we have accumulated too many tables in our map, throw everything away.
	// This deals with possible edge cases where we do a lot of DDL in a
	// long-lived session.
	if len(oc.dataSources) > 100 {
		oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
	}

	// Gossip can be nil in testing scenarios.
	if oc.planner.execCfg.Gossip != nil {
		oc.cfg = oc.planner.execCfg.Gossip.GetSystemConfig()
	}
}

// optDatabase is a wrapper around sqlbase.DatabaseDescriptor that implements the
// cat.Object and cat.Database interfaces.
type optDatabase struct {
	desc *sqlbase.DatabaseDescriptor
}

// ID is part of the cat.Object interface.
func (os *optDatabase) ID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optDatabase) PostgresDescriptorID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optDatabase) Equals(other cat.Object) bool {
	otherDatabase, ok := other.(*optDatabase)
	return ok && os.desc.ID == otherDatabase.desc.ID
}

// optSchema is a wrapper around sqlbase.DatabaseDescriptor that implements the
// cat.Object and cat.Schema interfaces.
type optSchema struct {
	planner  *planner
	database *sqlbase.DatabaseDescriptor
	schema   *sqlbase.ResolvedSchema

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	switch os.schema.Kind {
	case sqlbase.SchemaUserDefined, sqlbase.SchemaTemporary:
		// User defined schemas and the temporary schema have real ID's, so use
		// them here.
		return cat.StableID(os.schema.ID)
	default:
		// Virtual schemas and the public schema don't, so just fall back to the
		// parent database's ID.
		return cat.StableID(os.database.GetID())
	}
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSchema) PostgresDescriptorID() cat.StableID {
	return cat.StableID(os.database.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.database.ID == otherSchema.database.ID
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// GetDataSourceNames is part of the cat.Schema interface.
func (os *optSchema) GetDataSourceNames(ctx context.Context) ([]cat.DataSourceName, error) {
	return GetObjectNames(
		ctx, os.planner.Txn(),
		os.planner,
		os.database,
		os.name.Schema(),
		true, /* explicitPrefix */
	)
}

// GetDatabaseType is part of the cat.Schema interface.
func (os *optSchema) GetDatabaseType() tree.EngineType {
	return tree.EngineType(os.database.EngineType)
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	// ResolveTargetObject wraps ResolveTarget in order to raise "schema not
	// found" and "schema cannot be modified" errors. However, ResolveTargetObject
	// assumes that a data source object is being resolved, which is not the case
	// for ResolveSchema. Therefore, call ResolveTarget directly and produce a
	// more general error.
	oc.tn.TableName = ""
	oc.tn.TableNamePrefix = *name

	found, prefixI, err := oc.tn.ResolveTarget(
		ctx,
		oc.planner,
		oc.planner.CurrentDatabase(),
		oc.planner.CurrentSearchPath(),
	)
	if err != nil {
		return nil, cat.SchemaName{}, err
	}
	if !found {
		return nil, cat.SchemaName{}, pgerror.New(
			pgcode.InvalidSchemaName, "target database or schema does not exist",
		)
	}
	prefix := prefixI.(*sqlbase.ResolvedObjectPrefix)
	return &optSchema{
		planner:  oc.planner,
		database: &prefix.Database,
		schema:   &prefix.Schema,
		name:     oc.tn.TableNamePrefix,
	}, oc.tn.TableNamePrefix, nil
}

// ReleaseTables is part of the cat.Catalog interface.
func (oc *optCatalog) ReleaseTables(ctx context.Context) {
	oc.planner.Tables().releaseLeases(ctx)
}

// ResetTxn is part of the cat.Catalog interface.
func (oc *optCatalog) ResetTxn(ctx context.Context) {
	newTxn := kv.NewTxn(ctx, oc.planner.execCfg.DB, oc.planner.execCfg.NodeID.Get())
	*oc.planner.txn = *newTxn
}

func (oc *optCatalog) ResolveProcCatalog(
	ctx context.Context, t *tree.TableName, checkPri bool,
) (bool, *tree.CreateProcedure, error) {
	found, desc, err := ResolveProcedureObject(ctx, oc.planner, t)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return false, nil, nil
	}
	if checkPri {
		if err := oc.planner.CheckPrivilege(ctx, desc, privilege.EXECUTE); err != nil {
			return false, nil, err
		}
	}
	var params []*tree.ProcedureParameter
	for i := range desc.Parameters {
		param := &tree.ProcedureParameter{
			Name:      tree.Name(desc.Parameters[i].Name),
			Type:      &desc.Parameters[i].Type,
			Direction: tree.InDirection,
		}
		params = append(params, param)
	}
	var res = tree.CreateProcedure{
		Name:       *t,
		Parameters: params,
		BodyStr:    desc.ProcBody,
		ProcID:     int32(desc.ID),
		DBID:       int32(desc.DbID),
		SchemaID:   int32(desc.SchemaID),
	}
	return true, &res, nil
}

// ResolveProcedureObject  resolves Procedure Object
func ResolveProcedureObject(
	ctx context.Context, p SchemaResolver, t *ObjectName,
) (bool, *sqlbase.ProcedureDescriptor, error) {
	curDb := p.CurrentDatabase()
	if t.ExplicitSchema {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := p.CurrentSearchPath().MaybeResolveTemporarySchema(t.Schema())
		if err != nil {
			return false, nil, err
		}
		if t.ExplicitCatalog {
			// Already 3 parts: nothing to search. Delegate to the resolver.
			objMeta, err := GetProcedureMeta(ctx, p.Txn(), t.Catalog(), scName, t.Table())
			if objMeta != nil || err != nil {
				t.SchemaName = tree.Name(scName)
				return objMeta != nil, objMeta, err
			}
		} else {
			// Two parts: D.T.
			// Try to use the current database, and be satisfied if it's sufficient to find the object.
			//
			// Note: we test this even if curDb == "", because CockroachDB
			// supports querying virtual schemas even when the current
			// database is not set. For example, `select * from
			// pg_catalog.pg_tables` is meant to show all tables across all
			// databases when there is no current database set.
			if objMeta, err := GetProcedureMeta(ctx, p.Txn(), curDb, scName, t.Table()); objMeta != nil {
				if err == nil {
					t.CatalogName = tree.Name(curDb)
				}
				return objMeta != nil, objMeta, err
			}
			// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T instead.
			if objMeta, err := GetProcedureMeta(ctx, p.Txn(), t.Schema(), tree.PublicSchema, t.Table()); objMeta != nil || err != nil {
				if err == nil {
					t.CatalogName = t.SchemaName
					t.SchemaName = tree.PublicSchemaName
					t.ExplicitCatalog = true
				}
				return objMeta != nil, objMeta, err
			}
		}
		// Welp, really haven't found anything.
		return false, nil, nil
	}

	// This is a naked procedure name. Use the search path.
	iter := p.CurrentSearchPath().Iter()
	for next, ok := iter.Next(); ok; next, ok = iter.Next() {
		if objMeta, err := GetProcedureMeta(ctx, p.Txn(), curDb, next, t.Table()); objMeta != nil || err != nil {
			if err == nil {
				t.CatalogName = tree.Name(curDb)
				t.SchemaName = tree.Name(next)
			}
			return objMeta != nil, objMeta, err
		}
	}
	return false, nil, nil
}

// ResolveAndCheckProcPrivilege resolves procedure and check privilege of procedure
func ResolveAndCheckProcPrivilege(
	ctx context.Context, p *planner, t *ObjectName, pri privilege.Kind,
) error {
	found, desc, err := ResolveProcedureObject(ctx, p, t)
	if err != nil {
		return err
	}
	if !found {
		return sqlbase.NewUndefinedProcedureError(t)
	}
	if err := p.CheckPrivilege(ctx, desc, pri); err != nil {
		return err
	}
	return nil
}

// GetProcedureMeta gets Procedure Metadata
func GetProcedureMeta(
	ctx context.Context, txn *kv.Txn, dbName, scName, procName string,
) (*sqlbase.ProcedureDescriptor, error) {
	dbID, err := getDatabaseID(ctx, txn, dbName, true)
	if err != nil {
		return nil, err
	}
	found, scID, err := resolveSchemaID(ctx, txn, dbID, scName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	k := keys.MakeTablePrefix(uint32(sqlbase.UDRTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(sqlbase.UDRTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(dbID))
	k = encoding.EncodeUvarintAscending(k, uint64(scID))
	k = encoding.EncodeStringAscending(k, procName)

	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, k, sqlbase.UDRTable)
	if err != nil {
		return nil, err
	}
	var desc sqlbase.ProcedureDescriptor
	if rows != nil {
		routineType := int(tree.MustBeDInt(rows[0][5]))
		name := string(tree.MustBeDString(rows[0][2]))
		if routineType != int(sqlbase.Procedure) {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "%s is not a procedure", name)
		}
		val := tree.MustBeDBytes(rows[0][3])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", k)
		}
	} else {
		return nil, nil
	}
	return &desc, nil
}

// GetAllProcDescByParentID gets Procedure descriptor by dbID and schemaID
func GetAllProcDescByParentID(
	ctx context.Context, txn *kv.Txn, dbID, scID sqlbase.ID,
) ([]sqlbase.ProcedureDescriptor, error) {

	k := keys.MakeTablePrefix(uint32(sqlbase.UDRTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(sqlbase.UDRTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(dbID))
	k = encoding.EncodeUvarintAscending(k, uint64(scID))

	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, k, sqlbase.UDRTable)
	if err != nil {
		return nil, err
	}
	var descs []sqlbase.ProcedureDescriptor
	for i := range rows {
		routineType := int(tree.MustBeDInt(rows[i][5]))
		if routineType != int(sqlbase.Procedure) {
			continue
		}
		var desc sqlbase.ProcedureDescriptor
		val := tree.MustBeDBytes(rows[i][3])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", k)
		}
		descs = append(descs, desc)
	}
	return descs, nil
}

// GetAllProcDesc gets Procedure descriptor
func GetAllProcDesc(ctx context.Context, txn *kv.Txn) ([]sqlbase.ProcedureDescriptor, error) {

	k := keys.MakeTablePrefix(uint32(sqlbase.UDRTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(sqlbase.UDRTable.PrimaryIndex.ID))

	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, k, sqlbase.UDRTable)
	if err != nil {
		return nil, err
	}
	var descs []sqlbase.ProcedureDescriptor
	for i := range rows {
		routineType := int(tree.MustBeDInt(rows[i][5]))
		if routineType != int(sqlbase.Procedure) {
			continue
		}
		var desc sqlbase.ProcedureDescriptor
		val := tree.MustBeDBytes(rows[i][3])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", k)
		}
		descs = append(descs, desc)
	}
	return descs, nil
}

// UpdateProcedureMeta updates procedure descriptor with new descriptor
func UpdateProcedureMeta(
	ctx context.Context, txn *kv.Txn, procedure *sqlbase.ProcedureDescriptor,
) error {
	k := keys.MakeTablePrefix(uint32(sqlbase.UDRTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(sqlbase.UDRTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(procedure.DbID))
	k = encoding.EncodeUvarintAscending(k, uint64(procedure.SchemaID))
	k = encoding.EncodeStringAscending(k, procedure.Name)

	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, k, sqlbase.UDRTable)
	if err != nil {
		return err
	}
	var desc sqlbase.ProcedureDescriptor
	if rows != nil {
		routineType := int(tree.MustBeDInt(rows[0][5]))
		name := string(tree.MustBeDString(rows[0][2]))
		if routineType != int(sqlbase.Procedure) {
			return pgerror.Newf(pgcode.WrongObjectType, "%s is not a procedure", name)
		}
		val := tree.MustBeDBytes(rows[0][3])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", k)
		}
		descValue, err := protoutil.Marshal(procedure)
		if err != nil {
			return err
		}
		// update new descriptor
		rows[0][3] = tree.NewDBytes(tree.DBytes(descValue))
		// build correct rows
		rows[0][0], rows[0][1], rows[0][2] = rows[0][2], rows[0][0], rows[0][1]
		if err := WriteKWDBDesc(ctx, txn, sqlbase.UDRTable, rows, true); err != nil {
			return err
		}
	}
	return nil
}

// ResolveDatabase is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDatabase(
	ctx context.Context, flags cat.Flags, name string,
) (cat.Database, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}
	db, err := oc.planner.ResolveUncachedDatabaseByName(ctx, name, true)
	if err != nil {
		return nil, err
	}

	return &optDatabase{
		desc: db,
	}, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	oc.tn = *name
	desc, err := ResolveExistingObject(ctx, oc.planner, &oc.tn, tree.ObjectLookupFlagsWithRequired(), ResolveAnyDescType)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	ds, err := oc.dataSourceForDesc(ctx, flags, desc, &oc.tn)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	return ds, oc.tn, nil
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, dataSourceID cat.StableID,
) (_ cat.DataSource, isAdding bool, _ error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	tableLookup, err := oc.planner.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
			return nil, tableLookup.IsAdding, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
		}
		return nil, false, err
	}

	// The name is only used for virtual tables, which can't be looked up by ID.
	ds, err := oc.dataSourceForDesc(ctx, cat.Flags{}, tableLookup.Desc, &tree.TableName{})
	return ds, false, err
}

func getDescForCatalogObject(o cat.Object) (sqlbase.DescriptorProto, error) {
	switch t := o.(type) {
	case *optSchema:
		if t.schema.Kind == sqlbase.SchemaUserDefined {
			return t.schema.Desc, nil
		}
		return t.database, nil
	case *optTable:
		return t.desc, nil
	case *optVirtualTable:
		return t.desc, nil
	case *optView:
		return t.desc, nil
	case *optSequence:
		return t.desc, nil
	case *optDatabase:
		return t.desc, nil
	default:
		return nil, errors.AssertionFailedf("invalid object type: %T", o)
	}
}

func getDescForDataSource(o cat.DataSource) (*sqlbase.ImmutableTableDescriptor, error) {
	switch t := o.(type) {
	case *optTable:
		return t.desc, nil
	case *optVirtualTable:
		return t.desc, nil
	case *optView:
		return t.desc, nil
	case *optSequence:
		return t.desc, nil
	default:
		return nil, errors.AssertionFailedf("invalid object type: %T", o)
	}
}

// CheckPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	desc, err := getDescForCatalogObject(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckPrivilege(ctx, desc, priv)
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	desc, err := getDescForCatalogObject(o)
	if err != nil {
		return err
	}
	return oc.planner.CheckAnyPrivilege(ctx, desc)
}

// GetCurrentDatabase is part of the cat.Catalog interface.
func (oc *optCatalog) GetCurrentDatabase(ctx context.Context) string {
	return oc.planner.SessionData().Database
}

// HasAdminRole is part of the cat.Catalog interface.
func (oc *optCatalog) HasAdminRole(ctx context.Context) (bool, error) {
	return oc.planner.HasAdminRole(ctx)
}

// RequireAdminRole is part of the cat.Catalog interface.
func (oc *optCatalog) RequireAdminRole(ctx context.Context, action string) error {
	return oc.planner.RequireAdminRole(ctx, action)
}

// FullyQualifiedName is part of the cat.Catalog interface.
func (oc *optCatalog) FullyQualifiedName(
	ctx context.Context, ds cat.DataSource,
) (cat.DataSourceName, error) {
	return oc.fullyQualifiedNameWithTxn(ctx, ds, oc.planner.Txn())
}

func (oc *optCatalog) fullyQualifiedNameWithTxn(
	ctx context.Context, ds cat.DataSource, txn *kv.Txn,
) (cat.DataSourceName, error) {
	if vt, ok := ds.(*optVirtualTable); ok {
		// Virtual tables require special handling, because they can have multiple
		// effective instances that utilize the same descriptor.
		return vt.name, nil
	}

	desc, err := getDescForDataSource(ds)
	if err != nil {
		return cat.DataSourceName{}, err
	}

	dbID := desc.ParentID
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, dbID)
	if err != nil {
		return cat.DataSourceName{}, err
	}
	return tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(desc.Name)), nil
}

// dataSourceForDesc returns a data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForDesc(
	ctx context.Context,
	flags cat.Flags,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (cat.DataSource, error) {
	if desc.IsTable() || desc.MaterializedView() {
		// Tables require invalidation logic for cached wrappers.
		// Because they are backed by physical data, we treat materialized views
		// as tables for the purposes of planning.
		return oc.dataSourceForTable(ctx, flags, desc, name)
	}

	ds, ok := oc.dataSources[desc]
	if ok {
		return ds, nil
	}

	switch {
	case desc.IsView():
		ds = newOptView(desc)

	case desc.IsSequence():
		ds = newOptSequence(desc)

	default:
		return nil, errors.AssertionFailedf("unexpected table descriptor: %+v", desc)
	}

	oc.dataSources[desc] = ds
	return ds, nil
}

// dataSourceForTable returns a table data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForTable(
	ctx context.Context,
	flags cat.Flags,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (cat.DataSource, error) {
	if desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor, so we can't cache them (see the comment for
		// optVirtualTable.id for more information).
		return newOptVirtualTable(ctx, oc, desc, name)
	}

	// Even if we have a cached data source, we still have to cross-check that
	// statistics and the zone config haven't changed.
	var tableStats []*stats.TableStatistic
	if !flags.NoTableStats {
		var err error
		tableStats, err = oc.planner.execCfg.TableStatsCache.GetTableStats(context.TODO(), desc.ID)
		if err != nil {
			// Ignore any error. We still want to be able to run queries even if we lose
			// access to the statistics table.
			// TODO(radu): at least log the error.
			tableStats = nil
		}
	}

	zoneConfig, err := oc.getZoneConfig(desc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a data source wrapper for this descriptor,
	// and it was created with the same stats and zone config.
	if ds, ok := oc.dataSources[desc]; ok && !ds.(*optTable).isStale(tableStats, zoneConfig) {
		return ds, nil
	}

	ds, err := newOptTable(desc, tableStats, zoneConfig)
	if err != nil {
		return nil, err
	}
	oc.dataSources[desc] = ds
	return ds, nil
}

var emptyZoneConfig = &zonepb.ZoneConfig{}

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getZoneConfig(
	desc *sqlbase.ImmutableTableDescriptor,
) (*zonepb.ZoneConfig, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(uint32(desc.ID))
	if err != nil {
		return nil, err
	}
	if zone == nil {
		// This can happen with tests that override the hook.
		zone = emptyZoneConfig
	}
	return zone, err
}

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor
}

var _ cat.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor) *optView {
	return &optView{desc: desc}
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ov *optView) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// Equals is part of the cat.Object interface.
func (ov *optView) Equals(other cat.Object) bool {
	otherView, ok := other.(*optView)
	if !ok {
		return false
	}
	return ov.desc.ID == otherView.desc.ID && ov.desc.Version == otherView.desc.Version
}

// Name is part of the cat.View interface.
func (ov *optView) Name() tree.Name {
	return tree.Name(ov.desc.Name)
}

// IsSystemView is part of the cat.View interface.
func (ov *optView) IsSystemView() bool {
	return ov.desc.IsVirtualTable()
}

// Query is part of the cat.View interface.
func (ov *optView) Query() string {
	return ov.desc.ViewQuery
}

// ColumnNameCount is part of the cat.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.Columns)
}

// ColumnName is part of the cat.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return tree.Name(ov.desc.Columns[i].Name)
}

// optSequence is a wrapper around sqlbase.ImmutableTableDescriptor that
// implements the cat.Object and cat.DataSource interfaces.
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor) *optSequence {
	return &optSequence{desc: desc}
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (os *optSequence) PostgresDescriptorID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSequence) Equals(other cat.Object) bool {
	otherSeq, ok := other.(*optSequence)
	if !ok {
		return false
	}
	return os.desc.ID == otherSeq.desc.ID && os.desc.Version == otherSeq.desc.Version
}

// Name is part of the cat.Sequence interface.
func (os *optSequence) Name() tree.Name {
	return tree.Name(os.desc.Name)
}

// SequenceMarker is part of the cat.Sequence interface.
func (os *optSequence) SequenceMarker() {}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// indexes are the inlined wrappers for the table's primary and secondary
	// indexes.
	indexes []optIndex

	// rawStats stores the original table statistics slice. Used for a fast-path
	// check that the statistics haven't changed.
	rawStats []*stats.TableStatistic

	// stats are the inlined wrappers for table statistics.
	stats []optTableStat

	zone *zonepb.ZoneConfig

	// family is the inlined wrapper for the table's primary family. The primary
	// family is the first family explicitly specified by the user. If no families
	// were explicitly specified, then the primary family is synthesized.
	primaryFamily optFamily

	// families are the inlined wrappers for the table's non-primary families,
	// which are all the families specified by the user after the first. The
	// primary family is kept separate since the common case is that there's just
	// one family.
	families []optFamily

	outboundFKs []optForeignKeyConstraint
	inboundFKs  []optForeignKeyConstraint

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

// GetTableType return which type the table is.
func (ot *optTable) GetTableType() tree.TableType {
	return ot.desc.TableType
}

// GetTriggers returns the definition of trigger
func (ot *optTable) GetTriggers(event tree.TriggerEvent) []cat.TriggerMeta {
	res := make([]cat.TriggerMeta, 0)
	for _, trig := range ot.desc.Triggers {
		if trig.Event != sqlbase.TriggerEvent(event) {
			continue
		}
		res = append(res, cat.TriggerMeta{
			TriggerID:   tree.ID(trig.ID),
			TriggerName: trig.Name,
			ActionTime:  tree.TriggerActionTime(trig.ActionTime),
			Event:       tree.TriggerEvent(trig.Event),
			Body:        trig.TriggerBody,
		})
	}
	return res
}

// GetTSVersion return ts_version.
func (ot *optTable) GetTSVersion() uint32 {
	return uint32(ot.desc.TsTable.TsVersion)
}

// GetTSHashNum return ts_hash_num.
func (ot *optTable) GetTSHashNum() uint64 {
	if ot.desc.TsTable.HashNum == 0 {
		return api.HashParamV2
	}
	return ot.desc.TsTable.HashNum
}

// SetTableName set table name.
func (ot *optTable) SetTableName(name string) {
	ot.desc.Name = name
}

// GetTagMeta get name and type of all tags.
func (ot *optTable) GetTagMeta() []cat.TagMeta {
	tagMeta := make([]cat.TagMeta, 0)
	for _, col := range ot.desc.Columns {
		if col.IsTagCol() {
			tagMeta = append(tagMeta, cat.TagMeta{
				TagName: col.Name,
				TagType: col.Type,
			})
		}
	}
	return tagMeta
}

var _ cat.Table = &optTable{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor, stats []*stats.TableStatistic, tblZone *zonepb.ZoneConfig,
) (*optTable, error) {
	ot := &optTable{
		desc:     desc,
		rawStats: stats,
		zone:     tblZone,
	}

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

	// Build the indexes (add 1 to account for lack of primary index in
	// DeletableIndexes slice).
	ot.indexes = make([]optIndex, 1+len(ot.desc.DeletableIndexes()))

	for i := range ot.indexes {
		var idxDesc *sqlbase.IndexDescriptor
		if i == 0 {
			idxDesc = &desc.PrimaryIndex
		} else {
			idxDesc = &ot.desc.DeletableIndexes()[i-1]
		}

		// If there is a subzone that applies to the entire index, use that,
		// else use the table zone. Skip subzones that apply to partitions,
		// since they apply only to a subset of the index.
		idxZone := tblZone
		for j := range tblZone.Subzones {
			subzone := &tblZone.Subzones[j]
			if subzone.IndexID == uint32(idxDesc.ID) && subzone.PartitionName == "" {
				copyZone := subzone.Config
				copyZone.InheritFromParent(tblZone)
				idxZone = &copyZone
			}
		}
		ot.indexes[i].init(ot, i, idxDesc, idxZone)
	}

	for i := range ot.desc.OutboundFKs {
		fk := &ot.desc.OutboundFKs[i]
		ot.outboundFKs = append(ot.outboundFKs, optForeignKeyConstraint{
			name:              fk.Name,
			originTable:       ot.ID(),
			originColumns:     fk.OriginColumnIDs,
			referencedTable:   cat.StableID(fk.ReferencedTableID),
			referencedColumns: fk.ReferencedColumnIDs,
			validity:          fk.Validity,
			match:             fk.Match,
			deleteAction:      fk.OnDelete,
			updateAction:      fk.OnUpdate,
		})
	}
	for i := range ot.desc.InboundFKs {
		fk := &ot.desc.InboundFKs[i]
		ot.inboundFKs = append(ot.inboundFKs, optForeignKeyConstraint{
			name:              fk.Name,
			originTable:       cat.StableID(fk.OriginTableID),
			originColumns:     fk.OriginColumnIDs,
			referencedTable:   ot.ID(),
			referencedColumns: fk.ReferencedColumnIDs,
			validity:          fk.Validity,
			match:             fk.Match,
			deleteAction:      fk.OnDelete,
			updateAction:      fk.OnUpdate,
		})
	}

	ot.primaryFamily.init(ot, &desc.Families[0])
	ot.families = make([]optFamily, len(desc.Families)-1)
	for i := range ot.families {
		ot.families[i].init(ot, &desc.Families[i+1])
	}

	// Add stats last, now that other metadata is initialized.
	if stats != nil {
		ot.stats = make([]optTableStat, len(stats))
		n := 0
		for i := range stats {
			// We skip any stats that have columns that don't exist in the table anymore.
			if ok, err := ot.stats[n].init(ot, stats[i]); err != nil {
				return nil, err
			} else if ok {
				n++
			}
		}
		ot.stats = ot.stats[:n]
	}

	return ot, nil
}

// ID is part of the cat.Object interface.
func (ot *optTable) ID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ot *optTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// isStale checks if the optTable object needs to be refreshed because the stats
// or zone config have changed. False positives are ok.
func (ot *optTable) isStale(tableStats []*stats.TableStatistic, zone *zonepb.ZoneConfig) bool {
	// Fast check to verify that the statistics haven't changed: we check the
	// length and the address of the underlying array. This is not a perfect
	// check (in principle, the stats could have left the cache and then gotten
	// regenerated), but it works in the common case.
	if len(tableStats) != len(ot.rawStats) {
		return true
	}
	if len(tableStats) > 0 && &tableStats[0] != &ot.rawStats[0] {
		return true
	}
	if !zone.Equal(ot.zone) {
		return true
	}
	return false
}

// Equals is part of the cat.Object interface.
func (ot *optTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optTable)
	if !ok {
		return false
	}
	if ot == otherTable {
		// Fast path when it is the same object.
		return true
	}
	if ot.desc.ID != otherTable.desc.ID || ot.desc.Version != otherTable.desc.Version {
		return false
	}

	// Verify the stats are identical.
	if len(ot.stats) != len(otherTable.stats) {
		return false
	}
	for i := range ot.stats {
		if !ot.stats[i].equals(&otherTable.stats[i]) {
			return false
		}
	}

	// Verify that indexes are in same zones. For performance, skip deep equality
	// check if it's the same as the previous index (common case).
	var prevLeftZone, prevRightZone *zonepb.ZoneConfig
	for i := range ot.indexes {
		leftZone := ot.indexes[i].zone
		rightZone := otherTable.indexes[i].zone
		if leftZone == prevLeftZone && rightZone == prevRightZone {
			continue
		}
		if !leftZone.Equal(rightZone) {
			return false
		}
		prevLeftZone = leftZone
		prevRightZone = rightZone
	}

	return true
}

// Name is part of the cat.Table interface.
func (ot *optTable) Name() tree.Name {
	return tree.Name(ot.desc.Name)
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return false
}

// IsMaterializedView implements the cat.Table interface.
func (ot *optTable) IsMaterializedView() bool {
	return ot.desc.MaterializedView()
}

// IsInterleaved is part of the cat.Table interface.
func (ot *optTable) IsInterleaved() bool {
	return ot.desc.IsInterleaved()
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns())
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns())
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) cat.Column {
	return &ot.desc.DeletableColumns()[i]
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i cat.IndexOrdinal) cat.Index {
	return &ot.indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the cat.Table interface.
func (ot *optTable) Statistic(i int) cat.TableStatistic {
	return &ot.stats[i]
}

// CheckCount is part of the cat.Table interface.
func (ot *optTable) CheckCount() int {
	return len(ot.desc.ActiveChecks())
}

// Check is part of the cat.Table interface.
func (ot *optTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.ActiveChecks()[i]
	return cat.CheckConstraint{
		Constraint: check.Expr,
		Validated:  check.Validity == sqlbase.ConstraintValidity_Validated,
	}
}

// FamilyCount is part of the cat.Table interface.
func (ot *optTable) FamilyCount() int {
	return 1 + len(ot.families)
}

// Family is part of the cat.Table interface.
func (ot *optTable) Family(i int) cat.Family {
	if i == 0 {
		return &ot.primaryFamily
	}
	return &ot.families[i-1]
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) OutboundForeignKeyCount() int {
	return len(ot.outboundFKs)
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &ot.outboundFKs[i]
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (ot *optTable) InboundForeignKeyCount() int {
	return len(ot.inboundFKs)
}

// InboundForeignKey is part of the cat.Table interface.
func (ot *optTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	return &ot.inboundFKs[i]
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
	col, ok := ot.colMap[colID]
	if ok {
		return col, nil
	}
	return col, pgerror.Newf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

func (ot *optTable) GetParentID() tree.ID {
	return tree.ID(ot.desc.GetParentID())
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	desc *sqlbase.IndexDescriptor
	zone *zonepb.ZoneConfig

	// storedCols is the set of non-PK columns if this is the primary index,
	// otherwise it is desc.StoreColumnIDs.
	storedCols []sqlbase.ColumnID

	indexOrdinal  int
	numCols       int
	numKeyCols    int
	numLaxKeyCols int
}

var _ cat.Index = &optIndex{}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(
	tab *optTable, indexOrdinal int, desc *sqlbase.IndexDescriptor, zone *zonepb.ZoneConfig,
) {
	oi.tab = tab
	oi.desc = desc
	oi.zone = zone
	oi.indexOrdinal = indexOrdinal
	if desc == &tab.desc.PrimaryIndex {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]sqlbase.ColumnID, 0, tab.DeletableColumnCount()-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i, n := 0, tab.DeletableColumnCount(); i < n; i++ {
			id := tab.Column(i).ColID()
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, sqlbase.ColumnID(id))
			}
		}
		oi.numCols = tab.DeletableColumnCount()
	} else {
		oi.storedCols = desc.StoreColumnIDs
		oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)
	}

	if desc.Unique {
		notNull := true
		for _, id := range desc.ColumnIDs {
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.desc.DeletableColumns()[ord].Nullable {
				notNull = false
				break
			}
		}

		if notNull {
			// Unique index with no null columns: columns from index are sufficient
			// to form a key without needing extra primary key columns. There is no
			// separate lax key.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols
		} else {
			// Unique index with at least one nullable column: extra primary key
			// columns will be added to the row key when one of the unique index
			// columns has a NULL value.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols + len(desc.ExtraColumnIDs)
		}
	} else {
		// Non-unique index: extra primary key columns are always added to the row
		// key. There is no separate lax key.
		oi.numLaxKeyCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs)
		oi.numKeyCols = oi.numLaxKeyCols
	}
}

// ID is part of the cat.Index interface.
func (oi *optIndex) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Index interface.
func (oi *optIndex) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// IsUnique is part of the cat.Index interface.
func (oi *optIndex) IsUnique() bool {
	return oi.desc.Unique
}

// IsInverted is part of the cat.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.desc.Type == sqlbase.IndexDescriptor_INVERTED
}

// ColumnCount is part of the cat.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Zone is part of the cat.Index interface.
func (oi *optIndex) Zone() cat.Zone {
	return oi.zone
}

// Span is part of the cat.Index interface.
func (oi *optIndex) Span() roachpb.Span {
	desc := oi.tab.desc
	// Tables up to MaxSystemConfigDescID are grouped in a single system config
	// span.
	if desc.ID <= keys.MaxSystemConfigDescID {
		return keys.SystemConfigSpan
	}
	return desc.IndexSpan(oi.desc.ID)
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

// Ordinal is part of the cat.Index interface.
func (oi *optIndex) Ordinal() int {
	return oi.indexOrdinal
}

// PartitionByListPrefixes is part of the cat.Index interface.
func (oi *optIndex) PartitionByListPrefixes() []tree.Datums {
	list := oi.desc.Partitioning.List
	if len(list) == 0 {
		return nil
	}
	res := make([]tree.Datums, 0, len(list))
	var a sqlbase.DatumAlloc
	for i := range list {
		for _, valueEncBuf := range list[i].Values {
			t, _, err := sqlbase.DecodePartitionTuple(
				&a, &oi.tab.desc.TableDescriptor, oi.desc, &oi.desc.Partitioning,
				valueEncBuf, nil, /* prefixDatums */
			)
			if err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "while decoding partition tuple"))
			}
			// Ignore the DEFAULT case, where there is nothing to return.
			if len(t.Datums) > 0 {
				res = append(res, t.Datums)
			}
			// TODO(radu): split into multiple prefixes if Subpartition is also by list.
			// Note that this functionality should be kept in sync with the test catalog
			// implementation (test_catalog.go).
		}
	}
	return res
}

func (oi *optIndex) IndexColumnIDs() []uint32 {
	columnIDs := oi.desc.ColumnIDs
	uint32IDs := make([]uint32, len(columnIDs))
	for i, id := range columnIDs {
		uint32IDs[i] = uint32(id)
	}
	return uint32IDs
}

type optTableStat struct {
	stat           *stats.TableStatistic
	columnOrdinals []int
}

var _ cat.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool, _ error) {
	os.stat = stat
	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap[c]
		if !ok {
			// Column not in table (this is possible if the column was removed since
			// the statistic was calculated).
			return false, nil
		}
	}

	return true, nil
}

func (os *optTableStat) equals(other *optTableStat) bool {
	// Two table statistics are considered equal if they have been created at the
	// same time, on the same set of columns.
	if os.CreatedAt() != other.CreatedAt() || len(os.columnOrdinals) != len(other.columnOrdinals) {
		return false
	}
	for i, c := range os.columnOrdinals {
		if c != other.columnOrdinals[i] {
			return false
		}
	}
	return true
}

// CreatedAt is part of the cat.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.stat.CreatedAt
}

// ColumnCount is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the cat.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.stat.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.stat.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.stat.NullCount
}

// Histogram is part of the cat.TableStatistic interface.
func (os *optTableStat) Histogram() []cat.HistogramBucket {
	return os.stat.Histogram
}

// SortedHistogram is part of the cat.TableStatistic interface.
func (os *optTableStat) SortedHistogram() []cat.SortedHistogramBucket {
	return os.stat.SortedHistogram
}

// optFamily is a wrapper around sqlbase.ColumnFamilyDescriptor that keeps a
// reference to the table wrapper.
type optFamily struct {
	tab  *optTable
	desc *sqlbase.ColumnFamilyDescriptor
}

var _ cat.Family = &optFamily{}

// init can be used instead of newOptFamily when we have a pre-allocated
// instance (e.g. as part of a bigger struct).
func (oi *optFamily) init(tab *optTable, desc *sqlbase.ColumnFamilyDescriptor) {
	oi.tab = tab
	oi.desc = desc
}

// ID is part of the cat.Family interface.
func (oi *optFamily) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Family interface.
func (oi *optFamily) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// ColumnCount is part of the cat.Family interface.
func (oi *optFamily) ColumnCount() int {
	return len(oi.desc.ColumnIDs)
}

// Column is part of the cat.Family interface.
func (oi *optFamily) Column(i int) cat.FamilyColumn {
	ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
	return cat.FamilyColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Table is part of the cat.Family interface.
func (oi *optFamily) Table() cat.Table {
	return oi.tab
}

// optForeignKeyConstraint implements cat.ForeignKeyConstraint and represents a
// foreign key relationship. Both the origin and the referenced table store the
// same optForeignKeyConstraint (as an outbound and inbound reference,
// respectively).
type optForeignKeyConstraint struct {
	name string

	originTable   cat.StableID
	originColumns []sqlbase.ColumnID

	referencedTable   cat.StableID
	referencedColumns []sqlbase.ColumnID

	validity     sqlbase.ConstraintValidity
	match        sqlbase.ForeignKeyReference_Match
	deleteAction sqlbase.ForeignKeyReference_Action
	updateAction sqlbase.ForeignKeyReference_Action
}

var _ cat.ForeignKeyConstraint = &optForeignKeyConstraint{}

// Name is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) Name() string {
	return fk.name
}

// OriginTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) OriginTableID() cat.StableID {
	return fk.originTable
}

// ReferencedTableID is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ReferencedTableID() cat.StableID {
	return fk.referencedTable
}

// ColumnCount is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ColumnCount() int {
	return len(fk.originColumns)
}

// OriginColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) OriginColumnOrdinal(originTable cat.Table, i int) int {
	if originTable.ID() != fk.originTable {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to OriginColumnOrdinal (expected %d)",
			originTable.ID(), fk.originTable,
		))
	}

	tab := originTable.(*optTable)
	ord, _ := tab.lookupColumnOrdinal(fk.originColumns[i])
	return ord
}

// ReferencedColumnOrdinal is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) ReferencedColumnOrdinal(referencedTable cat.Table, i int) int {
	if referencedTable.ID() != fk.referencedTable {
		panic(errors.AssertionFailedf(
			"invalid table %d passed to ReferencedColumnOrdinal (expected %d)",
			referencedTable.ID(), fk.referencedTable,
		))
	}
	tab := referencedTable.(*optTable)
	ord, _ := tab.lookupColumnOrdinal(fk.referencedColumns[i])
	return ord
}

// Validated is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) Validated() bool {
	return fk.validity == sqlbase.ConstraintValidity_Validated
}

// MatchMethod is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) MatchMethod() tree.CompositeKeyMatchMethod {
	return sqlbase.ForeignKeyReferenceMatchValue[fk.match]
}

// DeleteReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) DeleteReferenceAction() tree.ReferenceAction {
	return sqlbase.ForeignKeyReferenceActionType[fk.deleteAction]
}

// UpdateReferenceAction is part of the cat.ForeignKeyConstraint interface.
func (fk *optForeignKeyConstraint) UpdateReferenceAction() tree.ReferenceAction {
	return sqlbase.ForeignKeyReferenceActionType[fk.updateAction]
}

// optVirtualTable is similar to optTable but is used with virtual tables.
type optVirtualTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// A virtual table can effectively have multiple instances, with different
	// contents. For example `db1.pg_catalog.pg_sequence` contains info about
	// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info about
	// sequences in db2.
	//
	// These instances should have different stable IDs. To achieve this, the
	// stable ID is the database ID concatenated with the descriptor ID.
	//
	// Note that some virtual tables have a special instance with empty catalog,
	// for example "".information_schema.tables contains info about tables in
	// all databases. We treat the empty catalog as having database ID 0.
	id cat.StableID

	// name is the fully qualified, fully resolved, fully normalized name of the
	// virtual table.
	name cat.DataSourceName

	// family is a synthesized primary family.
	family optVirtualFamily
}

func (ot *optVirtualTable) GetParentID() tree.ID {
	return tree.ID(ot.desc.GetParentID())
}

// GetTableType return which type the table is.
func (ot *optVirtualTable) GetTableType() tree.TableType {
	return ot.desc.TableType
}

// GetTriggers returns the definition of trigger
func (ot *optVirtualTable) GetTriggers(event tree.TriggerEvent) []cat.TriggerMeta {
	res := make([]cat.TriggerMeta, 0)
	for _, trig := range ot.desc.Triggers {
		if trig.Event != sqlbase.TriggerEvent(event) {
			continue
		}
		res = append(res, cat.TriggerMeta{
			TriggerID:   tree.ID(trig.ID),
			TriggerName: trig.Name,
			ActionTime:  tree.TriggerActionTime(trig.ActionTime),
			Event:       tree.TriggerEvent(trig.Event),
			Body:        trig.TriggerBody,
		})
	}
	return res
}

// GetTSVersion return ts_version.
func (ot *optVirtualTable) GetTSVersion() uint32 {
	return uint32(ot.desc.TsTable.TsVersion)
}

// GetTSHashNum return ts_hash_num.
func (ot *optVirtualTable) GetTSHashNum() uint64 {
	if ot.desc.TsTable.HashNum == 0 {
		return api.HashParamV2
	}
	return ot.desc.TsTable.HashNum
}

// SetTableName set table name.
func (ot *optVirtualTable) SetTableName(name string) {
	ot.desc.Name = name
}

// GetTagMeta get name and type of all tags.
func (ot *optVirtualTable) GetTagMeta() []cat.TagMeta {
	tagMeta := make([]cat.TagMeta, len(ot.desc.Columns))
	for _, v := range ot.desc.Columns {
		if v.IsTagCol() {
			tagMeta = append(tagMeta, cat.TagMeta{
				TagName: v.Name,
				TagType: v.Type,
			})
		}
	}
	return tagMeta
}

var _ cat.Table = &optVirtualTable{}

func newOptVirtualTable(
	ctx context.Context,
	oc *optCatalog,
	desc *sqlbase.ImmutableTableDescriptor,
	name *cat.DataSourceName,
) (*optVirtualTable, error) {
	// Calculate the stable ID (see the comment for optVirtualTable.id).
	id := cat.StableID(desc.ID)
	if name.Catalog() != "" {
		// TODO(radu): it's unfortunate that we have to lookup the schema again.
		_, prefixI, err := oc.planner.LookupSchema(ctx, name.Catalog(), name.Schema())
		if err != nil {
			return nil, err
		}
		if prefixI == nil {
			// The database was not found. This can happen e.g. when
			// accessing a virtual schema over a non-existent
			// database. This is a common scenario when the current db
			// in the session points to a database that was not created
			// yet.
			//
			// In that case we use an invalid database ID. We
			// distinguish this from the empty database case because the
			// virtual tables do not "contain" the same information in
			// both cases.
			id |= cat.StableID(math.MaxUint32) << 32
		} else {
			prefix := prefixI.(*sqlbase.ResolvedObjectPrefix)
			id |= cat.StableID(prefix.Database.ID) << 32
		}
	}

	ot := &optVirtualTable{
		desc: desc,
		id:   id,
		name: *name,
	}

	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	ot.family.init(ot)

	return ot, nil
}

// ID is part of the cat.Object interface.
func (ot *optVirtualTable) ID() cat.StableID {
	return ot.id
}

// PostgresDescriptorID is part of the cat.Object interface.
func (ot *optVirtualTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// Equals is part of the cat.Object interface.
func (ot *optVirtualTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optVirtualTable)
	if !ok {
		return false
	}
	if ot == otherTable {
		// Fast path when it is the same object.
		return true
	}
	if ot.id != otherTable.id || ot.desc.Version != otherTable.desc.Version {
		return false
	}
	// Check whether the database is renamed
	if ot.name.Catalog() != otherTable.name.Catalog() {
		return false
	}

	return true
}

// Name is part of the cat.Table interface.
func (ot *optVirtualTable) Name() tree.Name {
	return ot.name.TableName
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optVirtualTable) IsVirtualTable() bool {
	return true
}

// IsInterleaved is part of the cat.Table interface.
func (ot *optVirtualTable) IsInterleaved() bool {
	return ot.desc.IsInterleaved()
}

// IsMaterializedView implements the cat.Table interface.
func (ot *optVirtualTable) IsMaterializedView() bool {
	return ot.desc.MaterializedView()
}

// ColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns())
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optVirtualTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns())
}

// Column is part of the cat.Table interface.
func (ot *optVirtualTable) Column(i int) cat.Column {
	return &ot.desc.DeletableColumns()[i]
}

// IndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) IndexCount() int {
	return 0
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) WritableIndexCount() int {
	return 0
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optVirtualTable) DeletableIndexCount() int {
	return 0
}

// Index is part of the cat.Table interface.
func (ot *optVirtualTable) Index(i int) cat.Index {
	panic("no indexes")
}

// StatisticCount is part of the cat.Table interface.
func (ot *optVirtualTable) StatisticCount() int {
	return 0
}

// Statistic is part of the cat.Table interface.
func (ot *optVirtualTable) Statistic(i int) cat.TableStatistic {
	panic("no stats")
}

// CheckCount is part of the cat.Table interface.
func (ot *optVirtualTable) CheckCount() int {
	return len(ot.desc.ActiveChecks())
}

// Check is part of the cat.Table interface.
func (ot *optVirtualTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.ActiveChecks()[i]
	return cat.CheckConstraint{
		Constraint: check.Expr,
		Validated:  check.Validity == sqlbase.ConstraintValidity_Validated,
	}
}

// FamilyCount is part of the cat.Table interface.
func (ot *optVirtualTable) FamilyCount() int {
	return 1
}

// Family is part of the cat.Table interface.
func (ot *optVirtualTable) Family(i int) cat.Family {
	return &ot.family
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) OutboundForeignKeyCount() int {
	return 0
}

// OutboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic("no FKs")
}

// InboundForeignKeyCount is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKeyCount() int {
	return 0
}

// InboundForeignKey is part of the cat.Table interface.
func (ot *optVirtualTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	panic("no FKs")
}

// optVirtualFamily is a dummy implementation of cat.Family for the only family
// reported by a virtual table.
type optVirtualFamily struct {
	tab *optVirtualTable
}

var _ cat.Family = &optVirtualFamily{}

func (oi *optVirtualFamily) init(tab *optVirtualTable) {
	oi.tab = tab
}

// ID is part of the cat.Family interface.
func (oi *optVirtualFamily) ID() cat.StableID {
	return 0
}

// Name is part of the cat.Family interface.
func (oi *optVirtualFamily) Name() tree.Name {
	return "primary"
}

// ColumnCount is part of the cat.Family interface.
func (oi *optVirtualFamily) ColumnCount() int {
	return oi.tab.ColumnCount()
}

// Column is part of the cat.Family interface.
func (oi *optVirtualFamily) Column(i int) cat.FamilyColumn {
	return cat.FamilyColumn{Column: oi.tab.Column(i), Ordinal: i}
}

// Table is part of the cat.Family interface.
func (oi *optVirtualFamily) Table() cat.Table {
	return oi.tab
}
