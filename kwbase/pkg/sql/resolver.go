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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/schema"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// SchemaResolver abstracts the interfaces needed from the logical
// planner to perform name resolution below.
//
// We use an interface instead of passing *planner directly to make
// the resolution methods able to work even when we evolve the code to
// use a different plan builder.
// TODO(rytaft,andyk): study and reuse this.
type SchemaResolver interface {
	tree.TableNameExistingResolver
	tree.TableNameTargetResolver

	Txn() *kv.Txn
	LogicalSchemaAccessor() SchemaAccessor
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	CommonLookupFlags(required bool) tree.CommonLookupFlags
	ObjectLookupFlags(required bool, requireMutable bool) tree.ObjectLookupFlags
	LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error)
}

var _ SchemaResolver = &planner{}

var errNoPrimaryKey = errors.New("requested table does not have a primary key")

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn, dbName, p.CommonLookupFlags(required))
	})
	return res, err
}

// ResolveUncachedSchemaDescriptor looks up a schema from the store.
func (p *planner) ResolveUncachedSchemaDescriptor(
	ctx context.Context, dbID sqlbase.ID, name string, required bool,
) (found bool, schema sqlbase.ResolvedSchema, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		found, schema, err = p.LogicalSchemaAccessor().GetSchema(ctx, p.txn, dbID, name)
	})
	return found, schema, err
}

// GetObjectNames retrieves the names of all objects in the target database/
// schema. If explicitPrefix is set, the returned table names will have an
// explicit schema and catalog name.
func GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	dbDesc *DatabaseDescriptor,
	scName string,
	explicitPrefix bool,
) (res TableNames, err error) {
	return sc.LogicalSchemaAccessor().GetObjectNames(ctx, txn, dbDesc, scName,
		tree.DatabaseListFlags{
			CommonLookupFlags: sc.CommonLookupFlags(true /* required */),
			ExplicitPrefix:    explicitPrefix,
		})
}

// ResolveExistingObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	lookupFlags tree.ObjectLookupFlags,
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}

	return desc.(*ImmutableTableDescriptor), nil
}

// ResolveMutableExistingObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (res *MutableTableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required},
		RequireMutable:    true,
	}
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	if tblDesc, ok := desc.(*MutableTableDescriptor); ok {
		return tblDesc, nil
	}

	return nil, pgerror.Newf(pgcode.WrongObjectType, "%s is not a valid relational object", tn.TableName)
}

func resolveExistingObjectImpl(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	lookupFlags tree.ObjectLookupFlags,
	requiredType ResolveRequiredType,
) (res tree.NameResolutionResult, err error) {
	found, descI, err := tn.ResolveExisting(ctx, sc, lookupFlags, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if lookupFlags.Required {
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}

	obj := descI.(ObjectDescriptor)

	goodType := true
	switch requiredType {
	case ResolveRequireTableDesc:
		goodType = obj.TableDesc().IsTable()
	case ResolveRequireViewDesc:
		goodType = obj.TableDesc().IsView()
	case ResolveRequireTableOrViewDesc:
		goodType = obj.TableDesc().IsTable() || obj.TableDesc().IsView()
	case ResolveRequireSequenceDesc:
		goodType = obj.TableDesc().IsSequence()
	case ResolveRequireTSTableDesc:
		goodType = obj.TableDesc().IsTSTable()
	}
	if !goodType {
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
	}

	// If the table does not have a primary key, return an error
	// that the requested descriptor is invalid for use.
	if !lookupFlags.AllowWithoutPrimaryKey &&
		obj.TableDesc().IsTable() &&
		!obj.TableDesc().HasPrimaryKey() {
		return nil, errNoPrimaryKey
	}

	if lookupFlags.RequireMutable {
		return descI.(*MutableTableDescriptor), nil
	}

	return descI.(*ImmutableTableDescriptor), nil
}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
//
//	p.runWithOptions(resolveFlags{skipCache: true}, func() {
//	   someVar, err = ResolveExistingObject(ctx, p, ...)
//	})
//
// if err != nil { ... }
// use(someVar)
func (p *planner) runWithOptions(flags resolveFlags, fn func()) {
	if flags.skipCache {
		defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
		p.avoidCachedDescriptors = true
	}
	fn()
}

type resolveFlags struct {
	skipCache bool
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
) (table *MutableTableDescriptor, err error) {
	return ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
}

// ResolveImmutableTableDescriptor looks up an existing Immutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
func (p *planner) ResolveImmutableTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: required},
	}
	return ResolveExistingObject(ctx, p, tn, lookupFlags, requiredType)
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
		table, err = ResolveExistingObject(ctx, p, tn, lookupFlags, requiredType)
	})
	return table, err
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives.
//
// The object name is modified in-place with the result of the name
// resolution.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName,
) (res *sqlbase.ResolvedObjectPrefix, err error) {
	found, scMeta, err := tn.ResolveTarget(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if !tn.ExplicitSchema && !tn.ExplicitCatalog {
			return nil, pgerror.New(pgcode.InvalidName, "no database specified")
		}
		err = pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(tn))
		err = errors.WithHint(err, "verify that the current database and search_path are valid and/or the target database exists")
		return nil, err
	}
	scInfo := scMeta.(*sqlbase.ResolvedObjectPrefix)
	if IsVirtualSchemaName(tn.Schema()) || scInfo.Schema.Kind == sqlbase.SchemaVirtual {
		return nil, pgerror.Newf(pgcode.InvalidName,
			"schema cannot be modified: %q", tree.ErrString(&tn.TableNamePrefix))
	}
	return scInfo, nil
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, tn *ObjectName,
) (res *UncachedDatabaseDescriptor, err error) {
	var prefix *sqlbase.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, err = ResolveTargetObject(ctx, p, tn)
	})
	if err != nil {
		return nil, err
	}
	return &prefix.Database, err
}

func (p *planner) ResolveUncachedObjectPrefix(
	ctx context.Context, tn *ObjectName,
) (res *sqlbase.ResolvedObjectPrefix, err error) {
	var prefix *sqlbase.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, err = ResolveTargetObject(ctx, p, tn)
	})
	if err != nil {
		return nil, err
	}
	return prefix, err
}

// ResolveRequiredType can be passed to the ResolveExistingObject function to
// require the returned descriptor to be of a specific type.
type ResolveRequiredType int

// ResolveRequiredType options have descriptive names.
const (
	ResolveAnyDescType ResolveRequiredType = iota
	ResolveRequireTableDesc
	ResolveRequireViewDesc
	ResolveRequireTableOrViewDesc
	ResolveRequireSequenceDesc
	ResolveRequireTSTableDesc
)

var requiredTypeNames = [...]string{
	ResolveRequireTableDesc:       "table",
	ResolveRequireViewDesc:        "view",
	ResolveRequireTableOrViewDesc: "table or view",
	ResolveRequireSequenceDesc:    "sequence",
	ResolveRequireTSTableDesc:     "ts table",
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(ctx, p.txn, dbName, p.CommonLookupFlags(false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	var resolvedSchema sqlbase.ResolvedSchema
	found, resolvedSchema, err = sc.GetSchema(ctx, p.txn, dbDesc.ID, scName)
	if err != nil {
		return false, nil, err
	}

	return found, &sqlbase.ResolvedObjectPrefix{
		Database: *dbDesc,
		Schema:   resolvedSchema,
	}, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	p.tableName = tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(tbName))
	lookupFlags.CommonLookupFlags = p.CommonLookupFlags(false /* required */)
	objDesc, err := sc.GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings, &p.tableName, lookupFlags)
	return objDesc != nil, objDesc, err
}

// LookupSubObject implements the tree.TableNameExistingResolver interface.
// LookupSubObject lookup table name of super table, and then obtain the desc
// super table through its name, returned it as the desc of the sub table.
func (p *planner) LookupSubObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	curDB := p.CurrentDatabase()
	curSC := p.CurrentSearchPath().GetSchema()
	if dbName == "" && scName == "" {
		// case: table
		dbName, scName = curDB, curSC
	} else if dbName != "" && scName != "" {
		// case: db.sc.table
		// do nothing
	} else {
		// case: db.table or sc.table
		if scName == tree.PublicSchema {
			dbName = curDB
		} else {
			dbName = scName
			scName = tree.PublicSchema
		}
	}
	var ctbNamespace *sqlbase.InstNameSpace

	// look for instance table
	instNamespace := sqlbase.InstNameSpace{}
	instNamespace, found, err = sqlbase.ResolveInstanceName(ctx, p.txn, dbName, tbName)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return false, nil, nil
	}
	ctbNamespace = &instNamespace
	if ctbNamespace.State != sqlbase.ChildDesc_PUBLIC {
		if ctbNamespace.State == sqlbase.ChildDesc_ADD {
			return false, nil, sqlbase.NewCreateTSTableError(tbName)
		}
		if ctbNamespace.State == sqlbase.ChildDesc_DROP {
			return false, nil, sqlbase.NewDropTSTableError(tbName)
		}
		if ctbNamespace.State == sqlbase.ChildDesc_ALTER {
			return false, nil, sqlbase.NewAlterTSTableError(tbName)
		}
	}
	// try to use cache when resolving the table desc of the instance table
	sc := CachedPhysicalAccessor{tc: p.Tables()}
	sTableName := tree.MakeTableName(tree.Name(dbName), tree.Name(ctbNamespace.STableName))
	meta, err := sc.GetObjectDesc(ctx, p.txn, p.extendedEvalCtx.Settings, &sTableName, lookupFlags)
	if err != nil {
		return false, nil, err
	}
	if meta != nil {
		// copy meta and change the table type to instance table
		ctbMeta := *meta.TableDesc()
		ctbMeta.TableType = tree.InstanceTable
		ctbMeta.Name = tbName
		if lookupFlags.RequireMutable {
			objMeta = sqlbase.NewMutableExistingTableDescriptor(ctbMeta)
		} else {
			objMeta = sqlbase.NewImmutableTableDescriptor(ctbMeta)
		}
		return meta != nil, objMeta, nil
	}
	return false, nil, err
}

func (p *planner) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) ObjectLookupFlags(required, requireMutable bool) tree.ObjectLookupFlags {
	return tree.ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(required),
		RequireMutable:    requireMutable,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.ResolveUncachedDatabaseByName(ctx, string(database), true /*required*/)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if targets.Schemas != nil {
		if len(targets.Schemas) == 0 {
			return nil, errNoSchema
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Schemas))
		// Resolve the current database.
		db, err := p.ResolveUncachedDatabaseByName(ctx, p.CurrentDatabase(), true /* required */)
		if err != nil {
			return nil, err
		}
		for _, sc := range targets.Schemas {
			found, resSchema, err := p.ResolveUncachedSchemaDescriptor(ctx, db.ID, string(sc), true /* required */)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, sqlbase.NewUndefinedSchemaError(string(sc))
			}
			switch resSchema.Kind {
			case sqlbase.SchemaUserDefined:
				descs = append(descs, resSchema.Desc)
			default:
				return nil, pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot change privileges on schema %q", resSchema.Name)
			}
		}
		return descs, nil
	}

	if targets.Tables != nil {
		if len(targets.Tables) == 0 {
			return nil, errNoTable
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Tables))
		for _, tableTarget := range targets.Tables {
			tableGlob, err := tableTarget.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			tableNames, err := expandTableGlob(ctx, p, tableGlob)
			if err != nil {
				return nil, err
			}
			for i := range tableNames {
				descriptor, err := ResolveMutableExistingObject(ctx, p, &tableNames[i], true, ResolveAnyDescType)
				if err != nil {
					return nil, err
				}
				if descriptor.TableType == tree.InstanceTable {
					return nil, pgerror.Newf(pgcode.FeatureNotSupported, "unsupported feature: grant or revoke on %s", descriptor.TypeName())
				}
				descs = append(descs, descriptor)
			}
		}
		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if len(targets.Procedures) == 0 {
		return nil, errNoProcedure
	}
	descs := make([]sqlbase.DescriptorProto, 0, len(targets.Procedures))
	for _, procedure := range targets.Procedures {
		found, desc, err := ResolveProcedureObject(ctx, p, &procedure)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, sqlbase.NewUndefinedProcedureError(&procedure)
		}
		descs = append(descs, desc)

	}
	if len(descs) == 0 {
		return nil, errNoMatch
	}
	return descs, nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc *sqlbase.TableDescriptor,
) (string, error) {
	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, desc.ParentID)
	if err != nil {
		return "", err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := schema.ResolveNameByID(ctx, p.txn, desc.ParentID, schemaID)
	if err != nil {
		return "", err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.Name),
		tree.Name(schemaName),
		tree.Name(desc.Name),
	)
	return tbName.String(), nil
}

// findTableContainingIndex returns the descriptor of a table
// containing the index of the given name.
// This is used by expandMutableIndexName().
//
// An error is returned if the index name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingIndex(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags tree.CommonLookupFlags,
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sa.GetObjectNames(ctx, txn, dbDesc, scName,
		tree.DatabaseListFlags{CommonLookupFlags: lookupFlags, ExplicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDesc, err := ResolveMutableExistingObject(ctx, sc, tn, false /*required*/, ResolveAnyDescType)
		if err != nil {
			return nil, nil, err
		}
		// skip non-table but not skip materialized views,as of materialized view maybe contain index
		if tableDesc == nil || !(tableDesc.IsTable() || tableDesc.MaterializedView()) {
			continue
		}

		_, dropped, err := tableDesc.FindIndexByName(string(idxName))
		if err != nil || dropped {
			// err is nil if the index does not exist on the table.
			continue
		}
		if result != nil {
			return nil, nil, pgerror.Newf(pgcode.AmbiguousParameter,
				"index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.Required {
		return nil, nil, pgerror.Newf(pgcode.UndefinedObject,
			"index %q does not exist", idxName)
	}
	return result, desc, nil
}

// expandMutableIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p.txn, p, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context,
	txn *kv.Txn,
	sc SchemaResolver,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		desc, err = ResolveMutableExistingObject(ctx, sc, tn, requireTable, ResolveRequireTableOrViewDesc)
		if err != nil {
			return nil, nil, err
		}
		if desc != nil && desc.IsView() && !desc.MaterializedView() {
			return nil, nil, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table or materialized view", tn.Table())
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableIndexName(), index.Table.Table() is empty.
	// Once the table name is resolved for the index below, index.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := tn.TableNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if requireTable {
			err = pgerror.Newf(pgcode.UndefinedObject,
				"schema or database was not found while searching index: %q",
				tree.ErrString(&index.Index))
			err = errors.WithHint(err, "check the current database and search_path are valid")
			return nil, nil, err
		}
		return nil, nil, nil
	}

	lookupFlags := sc.CommonLookupFlags(requireTable)
	var foundTn *tree.TableName
	foundTn, desc, err = findTableContainingIndex(ctx, txn, sc, tn.Catalog(), tn.Schema(), index.Index, lookupFlags)
	if err != nil {
		return nil, nil, err
	}

	if foundTn != nil {
		// Memoize the table name that was found. tn is a reference to the table name
		// stored in index.Table.
		*tn = *foundTn
	}
	return tn, desc, nil
}

// getTableAndIndex returns the table and index descriptors for a
// TableIndexName.
//
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context, tableWithIndex *tree.TableIndexName, privilege privilege.Kind,
) (*MutableTableDescriptor, *sqlbase.IndexDescriptor, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	idx, _, err := cat.ResolveTableIndex(
		ctx, &catalog, cat.Flags{AvoidDescriptorCaches: true}, tableWithIndex,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := catalog.CheckPrivilege(ctx, idx.Table(), privilege); err != nil {
		return nil, nil, err
	}
	optIdx := idx.(*optIndex)
	return sqlbase.NewMutableExistingTableDescriptor(optIdx.tab.desc.TableDescriptor), optIdx.desc, nil
}

// expandTableGlob expands pattern into a list of tables represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, p *planner, pattern tree.TablePattern,
) (tree.TableNames, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	return cat.ExpandDataSourceGlob(ctx, &catalog, cat.Flags{}, pattern)
}

// fkSelfResolver is a SchemaResolver that inserts itself between a
// user of name resolution and another SchemaResolver, and will answer
// lookups of the new table being created. This is needed in the case
// of CREATE TABLE with a foreign key self-reference: the target of
// the FK definition is a table that does not exist yet.
type fkSelfResolver struct {
	SchemaResolver
	newTableName *tree.TableName
	newTableDesc *sqlbase.TableDescriptor
}

var _ SchemaResolver = &fkSelfResolver{}

// LookupSubObject looks up sub object with name.
func (r *fkSelfResolver) LookupSubObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	return false, nil, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if dbName == r.newTableName.Catalog() &&
		scName == r.newTableName.Schema() &&
		tbName == r.newTableName.Table() {
		table := r.newTableDesc
		if lookupFlags.RequireMutable {
			return true, sqlbase.NewMutableExistingTableDescriptor(*table), nil
		}
		return true, sqlbase.NewImmutableTableDescriptor(*table), nil
	}
	lookupFlags.IncludeOffline = false
	return r.SchemaResolver.LookupObject(ctx, lookupFlags, dbName, scName, tbName)
}

// internalLookupCtx can be used in contexts where all descriptors
// have been recently read, to accelerate the lookup of
// inter-descriptor relationships.
//
// This is used mainly in the generators for virtual tables,
// aliased as tableLookupFn below.
//
// It only reveals physical descriptors (not virtual descriptors).
type internalLookupCtx struct {
	dbNames map[sqlbase.ID]string
	dbIDs   []sqlbase.ID
	dbDescs map[sqlbase.ID]*DatabaseDescriptor
	tbDescs map[sqlbase.ID]*TableDescriptor
	tbIDs   []sqlbase.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

func newInternalLookupCtx(
	descs []sqlbase.DescriptorProto, prefix *DatabaseDescriptor,
) *internalLookupCtx {
	wrappedDescs := make([]sqlbase.Descriptor, len(descs))
	for i, desc := range descs {
		wrappedDescs[i] = *sqlbase.WrapDescriptor(desc)
	}
	return newInternalLookupCtxFromDescriptors(wrappedDescs, prefix)
}

func newInternalLookupCtxFromDescriptors(
	descs []sqlbase.Descriptor, prefix *DatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[sqlbase.ID]string)
	dbDescs := make(map[sqlbase.ID]*DatabaseDescriptor)
	tbDescs := make(map[sqlbase.ID]*TableDescriptor)
	var tbIDs, dbIDs []sqlbase.ID
	// Record database descriptors for name lookups.
	for _, desc := range descs {
		if database := desc.GetDatabase(); database != nil {
			dbNames[database.ID] = database.Name
			dbDescs[database.ID] = database
			if prefix == nil || prefix.ID == database.ID {
				dbIDs = append(dbIDs, database.ID)
			}
		} else if table := desc.Table(hlc.Timestamp{}); table != nil {
			tbDescs[table.ID] = table
			if prefix == nil || prefix.ID == table.ParentID {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, table.ID)
			}
		}
	}
	return &internalLookupCtx{
		dbNames: dbNames,
		dbDescs: dbDescs,
		tbDescs: tbDescs,
		tbIDs:   tbIDs,
		dbIDs:   dbIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id sqlbase.ID) (*DatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id sqlbase.ID) (*TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getParentName(table *TableDescriptor) string {
	parentName := l.dbNames[table.GetParentID()]
	if parentName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this virtual table before the dropped table descriptors are
		// effectively deleted.
		parentName = fmt.Sprintf("[%d]", table.GetParentID())
	}
	return parentName
}

// getParentAsTableName returns a TreeTable object of the parent table for a
// given table ID. Used to get the parent table of a table with interleaved
// indexes.
func (l *internalLookupCtx) getParentAsTableName(
	parentTableID sqlbase.ID, dbPrefix string,
) (tree.TableName, error) {
	var parentName tree.TableName
	parentTable, err := l.getTableByID(parentTableID)
	if err != nil {
		return tree.TableName{}, err
	}
	parentDbDesc, err := l.getDatabaseByID(parentTable.ParentID)
	if err != nil {
		return tree.TableName{}, err
	}
	parentName = tree.MakeTableName(tree.Name(parentDbDesc.Name), tree.Name(parentTable.Name))
	parentName.ExplicitSchema = parentDbDesc.Name != dbPrefix
	return parentName, nil
}

// getTableAsTableName returns a TableName object fot a given TableDescriptor.
func (l *internalLookupCtx) getTableAsTableName(
	table *sqlbase.TableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.ParentID)
	if err != nil {
		return tree.TableName{}, err
	}
	tableName = tree.MakeTableName(tree.Name(tableDbDesc.Name), tree.Name(table.Name))
	tableName.ExplicitSchema = tableDbDesc.Name != dbPrefix
	return tableName, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	tn := name.ToTableName()
	table, err := ResolveMutableExistingObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return table, nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (*MutableTableDescriptor, error) {
	tn := name.ToTableName()
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required},
		RequireMutable:         true,
		AllowWithoutPrimaryKey: true,
	}
	desc, err := resolveExistingObjectImpl(ctx, p, &tn, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*MutableTableDescriptor), nil
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = p.ResolveExistingObjectEx(ctx, name, required, requiredType)
	})
	return table, err
}

// See ResolveExistingObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType ResolveRequiredType,
) (res *ImmutableTableDescriptor, err error) {
	tn := name.ToTableName()
	lookupFlags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: required}}
	desc, err := resolveExistingObjectImpl(ctx, p, &tn, lookupFlags, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) *tree.TableName {
	return u.Resolved(&p.semaCtx.Annotations)
}
