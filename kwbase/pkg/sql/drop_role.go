// Copyright 2017 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/pkg/errors"
)

// DropRoleNode deletes entries from the system.users table.
// This is called from DROP USER and DROP ROLE.
type DropRoleNode struct {
	ifExists bool
	isRole   bool
	names    func() ([]string, error)

	numUsersDeleted int
}

// DropRole represents a DROP ROLE statement.
// Privileges: CREATEROLE privilege.
func (p *planner) DropRole(ctx context.Context, n *tree.DropRole) (planNode, error) {
	return p.DropRoleNode(ctx, n.Names, n.IfExists, n.IsRole, "DROP ROLE")
}

// DropRoleNode creates a "drop user" plan node. This can be called from DROP USER or DROP ROLE.
func (p *planner) DropRoleNode(
	ctx context.Context, namesE tree.Exprs, ifExists bool, isRole bool, opName string,
) (*DropRoleNode, error) {
	if err := p.HasRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	names, err := p.TypeAsStringArray(namesE, opName)
	if err != nil {
		return nil, err
	}

	return &DropRoleNode{
		ifExists: ifExists,
		isRole:   isRole,
		names:    names,
	}, nil
}

func (n *DropRoleNode) startExec(params runParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMDropCounter(sqltelemetry.Role)
		opName = "drop-role"
	} else {
		sqltelemetry.IncIAMDropCounter(sqltelemetry.User)
		opName = "drop-user"
	}

	names, err := n.names()
	if err != nil {
		return err
	}

	userNames := make(map[string]struct{})
	for _, name := range names {
		normalizedUsername, err := NormalizeAndValidateUsername(name)
		if err != nil {
			return err
		}
		userNames[normalizedUsername] = struct{}{}
	}

	f := tree.NewFmtCtx(tree.FmtSimple)
	defer f.Close()

	// Now check whether the user still has permission on any object in the database.

	// First check all the databases.
	if err := forEachDatabaseDesc(params.ctx, params.p, nil /*nil prefix = all databases*/, true, /* requiresPrivileges */
		func(db *sqlbase.DatabaseDescriptor) error {
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					f.FormatNameP(&db.Name)
					break
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Then check all the tables.
	//
	// We need something like forEachTableAll here, however we can't use
	// the predefined forEachTableAll() function because we need to look
	// at all _visible_ descriptors, not just those on which the current
	// user has permission.
	descs, err := params.p.Tables().getAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(descs, nil /*prefix - we want all descriptors */)
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		if !isTableVisible(table, true /*allowAdding*/) {
			continue
		}
		for _, u := range table.GetPrivileges().Users {
			if _, ok := userNames[u.User]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				parentName := lCtx.getParentName(table)
				tn := tree.MakeTableName(tree.Name(parentName), tree.Name(table.Name))
				f.FormatNode(&tn)
				break
			}
		}
	}

	// Then check all the procedures.
	procs, err := GetAllProcDesc(params.ctx, params.p.txn)
	if err != nil {
		return err
	}
	for _, proc := range procs {
		for _, u := range proc.Privileges.Users {
			if _, ok := userNames[u.User]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				f.FormatNameP(&proc.Name)
				break
			}
		}
	}

	// Was there any object depending on that user?
	if f.Len() > 0 {
		fnl := tree.NewFmtCtx(tree.FmtSimple)
		defer fnl.Close()
		for i, name := range names {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name)
		}
		return pgerror.Newf(pgcode.Grouping,
			"cannot drop role%s/user%s %s: grants still exist on %s",
			util.Pluralize(int64(len(names))), util.Pluralize(int64(len(names))),
			fnl.String(), f.String(),
		)
	}

	// All safe - do the work.
	var numRoleMembershipsDeleted int

	for normalizedUsername := range userNames {
		// Specifically reject special users and roles. Some (root, admin) would fail with
		// "privileges still exist" first.
		if normalizedUsername == sqlbase.AdminRole || normalizedUsername == sqlbase.PublicRole {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special role %s", normalizedUsername)
		}
		if normalizedUsername == security.RootUser {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special user %s", normalizedUsername)
		}

		numUsersDeleted, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			`DELETE FROM system.users WHERE username=$1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if numUsersDeleted == 0 && !n.ifExists {
			return errors.Errorf("role/user %s does not exist", normalizedUsername)
		}
		n.numUsersDeleted += numUsersDeleted

		// Drop all role memberships involving the user/role.
		numRoleMembershipsDeleted, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			fmt.Sprintf(
				`DELETE FROM %s WHERE username=$1`,
				RoleOptionsTableName,
			),
			normalizedUsername,
		)
		if err != nil {
			return err
		}
		params.p.SetAuditTarget(0, normalizedUsername, nil)
	}

	if numRoleMembershipsDeleted > 0 {
		// Some role memberships have been deleted, bump role_members table version to
		// force a refresh of role membership.
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	return nil
}

// FastPathResults implements the planNodeFastPath interface.
func (n *DropRoleNode) FastPathResults() (int, bool) { return n.numUsersDeleted, true }

// Next implements the planNode interface.
func (*DropRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*DropRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*DropRoleNode) Close(context.Context) {}
