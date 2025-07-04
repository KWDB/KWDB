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

// Package cat contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.
package cat

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// StableID permanently and uniquely identifies a catalog object (table, view,
// index, column, etc.) within its scope:
//
//	data source StableID: unique within database
//	index StableID: unique within table
//	column StableID: unique within table
//
// If a new catalog object is created, it will always be assigned a new StableID
// that has never, and will never, be reused by a different object in the same
// scope. This uniqueness guarantee is true even if the new object has the same
// name and schema as an old (and possibly dropped) object. The StableID will
// never change as long as the object exists.
//
// Note that while two instances of the same catalog object will always have the
// same StableID, they can have different schema if the schema has changed over
// time. See the Version type comments for more details.
//
// For most sqlbase objects, the StableID is the 32-bit descriptor ID. However,
// this is not always the case. For example, the StableID for virtual tables
// prepends the database ID, since the same descriptor ID is reused across
// databases.
type StableID uint64

// SchemaName is an alias for tree.TableNamePrefix, since it consists of the
// catalog + schema name.
type SchemaName = tree.TableNamePrefix

// Flags allows controlling aspects of some Catalog operations.
type Flags struct {
	// AvoidDescriptorCaches avoids using any cached descriptors (for tables,
	// views, schemas, etc). This is useful in cases where we are running a
	// statement like SHOW and we don't want to get table leases or otherwise
	// pollute the caches.
	AvoidDescriptorCaches bool

	// NoTableStats doesn't retrieve table statistics. This should be used in all
	// cases where we don't need them (like SHOW variants), to avoid polluting the
	// stats cache.
	NoTableStats bool
}

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
//
// NOTE: Catalog implementations need not be thread-safe. However, the objects
// returned by the Resolve methods (schemas and data sources) *must* be
// immutable after construction, and therefore also thread-safe.
type Catalog interface {
	// ResolveSchema locates a schema with the given name and returns it along
	// with the resolved SchemaName (which has all components filled in).
	// If the SchemaName is empty, returns the current database/schema (if one is
	// set; otherwise returns an error).
	//
	// The resolved SchemaName is the same with the resulting Schema.Name() except
	// that it has the ExplicitCatalog/ExplicitSchema flags set to correspond to
	// the input name. Its use is mainly for cosmetic purposes.
	//
	// If no such schema exists, then ResolveSchema returns an error.
	//
	// NOTE: The returned schema must be immutable after construction, and so can
	// be safely copied or used across goroutines.
	ResolveSchema(ctx context.Context, flags Flags, name *SchemaName) (Schema, SchemaName, error)

	// ResolveDatabase locates a database with the given name and returns it
	// along with the resolved Database.
	ResolveDatabase(ctx context.Context, flags Flags, name string) (Database, error)

	ResolveProcCatalog(ctx context.Context, name *tree.TableName, checkPri bool) (bool, *tree.CreateProcedure, error)
	// ResolveDataSource locates a data source with the given name and returns it
	// along with the resolved DataSourceName.
	//
	// The resolved DataSourceName is the same with the resulting
	// DataSource.Name() except that it has the ExplicitCatalog/ExplicitSchema
	// flags set to correspond to the input name. Its use is mainly for cosmetic
	// purposes. For example: the input name might be "t". The fully qualified
	// DataSource.Name() would be "currentdb.public.t"; the returned
	// DataSourceName would have the same fields but would still be formatted as
	// "t".
	//
	// If no such data source exists, then ResolveDataSource returns an error.
	//
	// NOTE: The returned data source must be immutable after construction, and
	// so can be safely copied or used across goroutines.
	ResolveDataSource(
		ctx context.Context, flags Flags, name *DataSourceName,
	) (DataSource, DataSourceName, error)

	// ResolveDataSourceByID is similar to ResolveDataSource, except that it
	// locates a data source by its StableID. See the comment for StableID for
	// more details.
	//
	// If the table is in the process of being added, returns an
	// "undefined relation" error but also returns isAdding=true.
	//
	// NOTE: The returned data source must be immutable after construction, and
	// so can be safely copied or used across goroutines.
	ResolveDataSourceByID(
		ctx context.Context, flags Flags, id StableID,
	) (_ DataSource, isAdding bool, _ error)

	// CheckPrivilege verifies that the current user has the given privilege on
	// the given catalog object. If not, then CheckPrivilege returns an error.
	CheckPrivilege(ctx context.Context, o Object, priv privilege.Kind) error

	// CheckAnyPrivilege verifies that the current user has any privilege on
	// the given catalog object. If not, then CheckAnyPrivilege returns an error.
	CheckAnyPrivilege(ctx context.Context, o Object) error

	// HasAdminRole checks that the current user has admin privileges. If yes,
	// returns true. Returns an error if query on the `system.users` table failed
	HasAdminRole(ctx context.Context) (bool, error)

	// RequireAdminRole checks that the current user has admin privileges. If not,
	// returns an error.
	RequireAdminRole(ctx context.Context, action string) error

	// FullyQualifiedName retrieves the fully qualified name of a data source.
	// Note that:
	//  - this call may involve a database operation so it shouldn't be used in
	//    performance sensitive paths;
	//  - the fully qualified name of a data source object can change without the
	//    object itself changing (e.g. when a database is renamed).
	FullyQualifiedName(ctx context.Context, ds DataSource) (DataSourceName, error)

	// GetCurrentDatabase return name of current database.
	GetCurrentDatabase(ctx context.Context) string

	// ReleaseTables releases all table leases for the current session.
	ReleaseTables(ctx context.Context)

	// ResetTxn resets the txn of the current session.
	ResetTxn(ctx context.Context)
}
