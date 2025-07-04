// Copyright 2016 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

//
// This file contains routines for low-level access to stored
// descriptors.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var (
	errEmptyDatabaseName = pgerror.New(pgcode.Syntax, "empty database name")
	errNoDatabase        = pgerror.New(pgcode.InvalidName, "no database specified")
	errNoSchema          = pgerror.Newf(pgcode.InvalidName, "no schema specified")
	errNoTable           = pgerror.New(pgcode.InvalidName, "no table specified")
	errNoProcedure       = pgerror.New(pgcode.InvalidName, "no procedure specified")
	errNoMatch           = pgerror.New(pgcode.UndefinedObject, "no object matched")
)

// DefaultUserDBs is a set of the databases which are present in a new cluster.
var DefaultUserDBs = map[string]struct{}{
	sessiondata.DefaultDatabaseName: {},
	sessiondata.PgDatabaseName:      {},
}

// MaxDefaultDescriptorID is the maximum ID of a descriptor that exists in a
// new cluster.
var MaxDefaultDescriptorID = keys.MaxReservedDescID + sqlbase.ID(len(DefaultUserDBs))

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *kv.DB) (sqlbase.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := kv.IncrementValRetryable(ctx, db, keys.DescIDGenerator, 1)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	return sqlbase.ID(newVal - 1), nil
}

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, desc *sqlbase.DatabaseDescriptor, ifNotExists bool, jobDesc string,
) (bool, error) {
	shouldCreatePublicSchema := true
	dKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, desc.Name)
	// TODO(solon): This conditional can be removed in 20.2. Every database
	// is created with a public schema for cluster version >= 20.1, so we can remove
	// the `shouldCreatePublicSchema` logic as well.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) {
		shouldCreatePublicSchema = false
	}

	if exists, _, err := sqlbase.LookupDatabaseID(ctx, p.txn, desc.Name); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlbase.NewDatabaseAlreadyExistsError(desc.Name)
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}

	if err := p.createDescriptorWithID(ctx, dKey.Key(), id, desc, nil, jobDesc); err != nil {
		return true, err
	}

	// TODO(solon): This check should be removed and a public schema should
	// be created in every database in >= 20.2.
	if shouldCreatePublicSchema {
		// Every database must be initialized with the public schema.
		if err := p.CreateSchemaNamespaceEntry(ctx, sqlbase.NewPublicSchemaKey(id).Key(),
			keys.PublicSchemaID); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (p *planner) createDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id sqlbase.ID,
	descriptor sqlbase.DescriptorProto,
	st *cluster.Settings,
	jobDesc string,
) error {
	descriptor.SetID(id)
	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	//
	// TODO(pmattis): Need to handle if-not-exists here as well.
	//
	// TODO(pmattis): This is writing the namespace and descriptor table entries,
	// but not going through the normal INSERT logic and not performing a precise
	// mimicry. In particular, we're only writing a single key per table, while
	// perfect mimicry would involve writing a sentinel key for each row as well.

	b := &kv.Batch{}
	descID := descriptor.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
	}
	b.CPut(idKey, descID, nil)
	if err := WriteNewDescToBatch(ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), st, b, descID, descriptor); err != nil {
		return err
	}

	mutDesc, isTable := descriptor.(*sqlbase.MutableTableDescriptor)
	if isTable {
		if err := mutDesc.ValidateTable(); err != nil {
			return err
		}
		if err := p.Tables().addUncommittedTable(*mutDesc); err != nil {
			return err
		}
	}
	sc, isSchema := descriptor.(*sqlbase.SchemaDescriptor)
	if isSchema {
		p.Tables().addUncommittedSchema(sc.Name, sc.ID, sc.ParentID, dbCreated)
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if isTable && !mutDesc.IsTSTable() && mutDesc.Adding() {
		// Queue a schema change job to eventually make the table public.
		if err := p.createOrUpdateSchemaChangeJob(
			ctx,
			mutDesc,
			jobDesc,
			sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	return nil
}

// GetDescriptorID looks up the ID for plainKey.
// InvalidID is returned if the name cannot be resolved.
func GetDescriptorID(
	ctx context.Context, txn *kv.Txn, plainKey sqlbase.DescriptorKey,
) (sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !gr.Exists() {
		return sqlbase.InvalidID, nil
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// resolveSchemaID resolves a schema's ID based on db and name.
func resolveSchemaID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ID, error) {
	// Try to use the system name resolution bypass. Avoids a hotspot by explicitly
	// checking for public schema.
	if scName == tree.PublicSchema {
		return true, keys.PublicSchemaID, nil
	}

	sKey := sqlbase.NewSchemaKey(dbID, scName)
	schemaID, err := GetDescriptorID(ctx, txn, sKey)
	if err != nil || schemaID == sqlbase.InvalidID {
		return false, sqlbase.InvalidID, err
	}

	return true, schemaID, nil
}

// lookupDescriptorByID looks up the descriptor for `id` and returns it.
// It can be a table or database descriptor.
// Returns the descriptor (if found), a bool representing whether the
// descriptor was found and an error if any.
func lookupDescriptorByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID,
) (sqlbase.DescriptorProto, bool, error) {
	var desc sqlbase.DescriptorProto
	for _, lookupFn := range []func() (sqlbase.DescriptorProto, error){
		func() (sqlbase.DescriptorProto, error) {
			return sqlbase.GetTableDescFromID(ctx, txn, id)
		},
		func() (sqlbase.DescriptorProto, error) {
			return sqlbase.GetDatabaseDescFromID(ctx, txn, id)
		},
	} {
		var err error
		desc, err = lookupFn()
		if err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				continue
			}
			return nil, false, err
		}
		return desc, true, nil
	}
	return nil, false, nil
}

// getDescriptorByID looks up the descriptor for `id`, validates it,
// and unmarshals it into `descriptor`.
//
// In most cases you'll want to use wrappers: `getDatabaseDescByID` or
// `getTableDescByID`.
func getDescriptorByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID, descriptor sqlbase.DescriptorProto,
) error {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &sqlbase.Descriptor{}
	ts, err := txn.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return err
	}
	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.Table(ts)
		if table == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table", desc.String())
		}
		if err := table.MaybeFillInDescriptor(ctx, txn); err != nil {
			return err
		}

		if err := table.Validate(ctx, txn); err != nil {
			return err
		}
		*t = *table
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a database", desc.String())
		}

		if err := database.Validate(); err != nil {
			return err
		}
		*t = *database
	case *sqlbase.SchemaDescriptor:
		schema := desc.GetSchema()
		if schema == nil {
			return pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a schema", desc.String())
		}
		*t = *schema
	}
	return nil
}

// IsDefaultCreatedDescriptor returns whether or not a given descriptor ID is
// present at the time of starting a cluster.
func IsDefaultCreatedDescriptor(descID sqlbase.ID) bool {
	return descID <= MaxDefaultDescriptorID
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *kv.Txn) (int, error) {
	allDescs, err := GetAllDescriptors(ctx, txn)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, desc := range allDescs {
		if !IsDefaultCreatedDescriptor(desc.GetID()) {
			count++
		}
	}

	return count, nil
}

// GetAllDescriptors looks up and returns all available descriptors.
// Modifying this method requires modifying method timekvserver.getAllDescriptors
// at the same time.
func GetAllDescriptors(ctx context.Context, txn *kv.Txn) ([]sqlbase.DescriptorProto, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	descs := make([]sqlbase.DescriptorProto, 0, len(kvs))
	for _, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			table := desc.Table(kv.Value.Timestamp)
			if err := table.MaybeFillInDescriptor(ctx, txn); err != nil {
				return nil, err
			}
			descs = append(descs, table)
		case *sqlbase.Descriptor_Database:
			descs = append(descs, desc.GetDatabase())
		case *sqlbase.Descriptor_Schema:
			descs = append(descs, desc.GetSchema())
		default:
			return nil, errors.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(ctx context.Context, txn *kv.Txn) ([]sqlbase.ID, error) {
	log.Eventf(ctx, "fetching all database descriptor IDs")
	nameKey := sqlbase.NewDatabaseKey("" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}
	// See the comment in physical_schema_accessors.go,
	// func (a UncachedPhysicalAccessor) GetObjectNames. Same concept
	// applies here.
	// TODO(solon): This complexity can be removed in 20.2.
	nameKey = sqlbase.NewDeprecatedDatabaseKey("" /* name */).Key()
	dkvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}
	kvs = append(kvs, dkvs...)

	descIDs := make([]sqlbase.ID, 0, len(kvs))
	alreadySeen := make(map[sqlbase.ID]bool)
	for _, kv := range kvs {
		ID := sqlbase.ID(kv.ValueInt())
		if alreadySeen[ID] || ID == sqlbase.InvalidID {
			continue
		}
		alreadySeen[ID] = true
		descIDs = append(descIDs, ID)
	}
	return descIDs, nil
}

// writeDescToBatch adds a Put command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID. If kvTrace
// is enabled, it will log an event explaining the put that was performed.
func writeDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *kv.Batch,
	descID sqlbase.ID,
	desc sqlbase.DescriptorProto,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(desc)
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)
	return nil
}

// WriteNewDescToBatch adds a CPut command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID, asserting
// that there was no previous descriptor at that id present already. If kvTrace
// is enabled, it will log an event explaining the CPut that was performed.
func WriteNewDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *kv.Batch,
	tableID sqlbase.ID,
	desc sqlbase.DescriptorProto,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	descDesc := sqlbase.WrapDescriptor(desc)
	if kvTrace {
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(descKey, descDesc, nil)
	return nil
}
