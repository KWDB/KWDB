// Copyright 2019 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// LookupNamespaceID implements tree.PrivilegedAccessor.
// TODO(sqlexec): make this work for any arbitrary schema.
// This currently only works for public schemas and databases.
func (p *GenericPlanner) LookupNamespaceID(
	ctx context.Context, parentID int64, name string,
) (tree.DInt, bool, error) {
	var r tree.Datums
	for _, t := range []struct {
		tableName   string
		extraClause string
	}{
		{fmt.Sprintf("[%d AS namespace]", keys.NamespaceTableID), `AND "parentSchemaID" IN (0, 29)`},
		{fmt.Sprintf("[%d AS namespace]", keys.DeprecatedNamespaceTableID), ""},
	} {
		query := fmt.Sprintf(
			`SELECT id FROM %s WHERE "parentID" = $1 AND name = $2 %s`,
			t.tableName,
			t.extraClause,
		)
		var err error
		r, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
			ctx,
			"kwdb-internal-get-descriptor-id",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			query,
			parentID,
			name,
		)
		if err != nil {
			return 0, false, err
		}
		if r != nil {
			break
		}
	}
	if r == nil {
		return 0, false, nil
	}
	id := tree.MustBeDInt(r[0])
	if err := p.checkDescriptorPermissions(ctx, sqlbase.ID(id)); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// LookupZoneConfigByNamespaceID implements tree.PrivilegedAccessor.
func (p *GenericPlanner) LookupZoneConfigByNamespaceID(
	ctx context.Context, id int64,
) (tree.DBytes, bool, error) {
	if err := p.checkDescriptorPermissions(ctx, sqlbase.ID(id)); err != nil {
		return "", false, err
	}

	const query = `SELECT config FROM system.zones WHERE id = $1`
	r, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"kwdb-internal-get-zone",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query,
		id,
	)
	if err != nil {
		return "", false, err
	}
	if r == nil {
		return "", false, nil
	}
	return tree.MustBeDBytes(r[0]), true, nil
}

// checkDescriptorPermissions returns nil if the executing user has permissions
// to check the permissions of a descriptor given its ID, or the id given
// is not a descriptor of a table or database.
func (p *GenericPlanner) checkDescriptorPermissions(ctx context.Context, id sqlbase.ID) error {
	desc, found, err := lookupDescriptorByID(ctx, p.txn, id)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return pgerror.New(pgcode.InsufficientPrivilege, "insufficient privilege")
	}
	return nil
}
