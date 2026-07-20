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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SchemaExists checks whether a schema with the given name exists
func SchemaExists(
	ctx context.Context, p *GenericPlanner, parentID sqlbase.ID, schema string,
) (bool, error) {
	// Check statically known schemas.
	if schema == tree.PublicSchema {
		return true, nil
	}
	for _, s := range virtualSchemas {
		if s.name == schema {
			return true, nil
		}
	}
	// Now lookup in the namespace for other schemas.
	exists, _, err := sqlbase.LookupObjectID(ctx, p.txn, parentID, keys.RootNamespaceID, schema)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// IsSchemaNameValid returns whether the input name is valid for a user defined
// schema.
func IsSchemaNameValid(name string) error {
	// Schemas starting with "pg_" are not allowed.
	if strings.HasPrefix(name, sessiondata.PgSchemaPrefix) {
		err := pgerror.Newf(pgcode.ReservedName, "unacceptable schema name %q", name)
		err = errors.WithDetail(err, `The prefix "pg_" is reserved for system schemas.`)
		return err
	}
	return nil
}

// IsVirtualSchemaName returns whether the input name is a virtual schema.
func IsVirtualSchemaName(schemaName string) bool {
	if _, ok := VirtualSchemaNames[schemaName]; ok {
		return true
	}
	return false
}

// VirtualSchemaNames is a set of all virtual schema names.
var VirtualSchemaNames = map[string]struct{}{
	sessiondata.PgCatalogName: {},
	informationSchemaName:     {},
	kwdbInternalName:          {},
}

// DropSchemaImpl performs the logic of dropping a user defined schema. It does
// not create a job to perform the final cleanup of the schema.
func DropSchemaImpl(
	ctx context.Context,
	p *GenericPlanner,
	b *kv.Batch,
	dbID sqlbase.ID,
	rsSchema *sqlbase.ResolvedSchema,
) error {
	scDesc := rsSchema.Desc
	if scDesc != nil {
		// Delete schema desc from system.descriptor.
		descKey := sqlbase.MakeDescMetadataKey(scDesc.ID)
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "Del %s", descKey)
		}
		b.Del(descKey)
	}
	// Delete the schema name from system.namespace.
	if err := sqlbase.RemoveSchemaNamespaceEntry(
		ctx,
		p.Txn(),
		dbID,
		rsSchema.Name,
	); err != nil {
		return err
	}
	p.Tables().AddUncommittedSchema(rsSchema.Name, rsSchema.ID, dbID, sqlconst.DbDropped)
	return nil
}
