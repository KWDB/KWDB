// Copyright 2020 The Cockroach Authors.
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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// GrantRoleNode creates entries in the system.role_members table.
// This is called from GRANT <ROLE>
var _ PlanNode = &GrantRoleNode{}

// GrantRoleNode represents a GRANT ROLE statement execution node
type GrantRoleNode struct {
	roles       tree.NameList
	members     tree.NameList
	adminOption bool

	run grantRoleRun
}

type grantRoleRun struct {
	rowsAffected int
}

// GrantRole represents a GRANT ROLE statement.
func (p *GenericPlanner) GrantRole(ctx context.Context, n *tree.GrantRole) (PlanNode, error) {
	return p.GrantRoleNode(ctx, n)
}

// GrantRoleNode is the real impl for GrantRole
func (p *GenericPlanner) GrantRoleNode(
	ctx context.Context, n *tree.GrantRole,
) (*GrantRoleNode, error) {
	sqltelemetry.IncIAMGrantCounter(n.AdminOption)

	ctx, span := tracing.ChildSpan(ctx, n.StatementTag(), p.GetNodeIDNumber())
	defer tracing.FinishSpan(span)

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// Check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	for _, r := range n.Roles {
		// If the user is an admin, don't check if the user is allowed to add/drop
		// roles in the role. However, if the role being modified is the admin role, then
		// make sure the user is an admin with the admin option.
		if hasAdminRole && string(r) != sqlbase.AdminRole {
			continue
		}
		if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
			errMsg := "%s is not a superuser or role admin for role %s"
			if string(r) == sqlbase.AdminRole {
				errMsg = "%s is not a role admin for role %s"
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, errMsg, p.User(), r)
		}
	}

	// Check that roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all roles.
	// This is wasteful when we have a LOT of roles compared to the number of roles being operated on.
	roles, err := p.GetAllRoles(ctx)
	if err != nil {
		return nil, err
	}

	// NOTE: membership manipulation involving the "public" pseudo-role fails with
	// "role public does not exist". This matches postgres behavior.

	for _, r := range n.Roles {
		if _, ok := roles[string(r)]; !ok {
			for name := range roleoption.ByName {
				if uppercase := strings.ToUpper(string(r)); uppercase == name {
					return nil, errors.WithHintf(
						pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r),
						"%s is a role option, try using ALTER ROLE to change a role's options.", uppercase)
				}
			}
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r)
		}
	}

	for _, m := range n.Members {
		if _, ok := roles[string(m)]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", m)
		}
	}

	// Given an acyclic directed membership graph, adding a new edge (grant.Member ∈ grant.Role)
	// means checking whether we have an expanded relationship (grant.Role ∈ ... ∈ grant.Member)
	// For each grant.Role, we lookup all the roles it is a member of.
	// After adding a given edge (grant.Member ∈ grant.Role), we add the edge to the list as well.
	allRoleMemberships := make(map[string]map[string]bool)
	for _, rawR := range n.Roles {
		r := string(rawR)
		allRoles, err := p.MemberOfWithAdminOption(ctx, r)
		if err != nil {
			return nil, err
		}
		allRoleMemberships[r] = allRoles
	}

	// Since we perform no queries here, check all role/member pairs for cycles.
	// Only if there are no errors do we proceed to write them.
	for _, rawR := range n.Roles {
		r := string(rawR)
		for _, rawM := range n.Members {
			m := string(rawM)
			if r == m {
				// self-cycle.
				return nil, pgerror.Newf(pgcode.InvalidGrantOperation, "%s cannot be a member of itself", m)
			}
			// Check if grant.Role ∈ ... ∈ grant.Member
			if memberOf, ok := allRoleMemberships[r]; ok {
				if _, ok = memberOf[m]; ok {
					return nil, pgerror.Newf(pgcode.InvalidGrantOperation,
						"making %s a member of %s would create a cycle", m, r)
				}
			}
			// Add the new membership. We don't care about the actual bool value.
			if _, ok := allRoleMemberships[m]; !ok {
				allRoleMemberships[m] = make(map[string]bool)
			}
			allRoleMemberships[m][r] = false
		}
	}

	return &GrantRoleNode{
		roles:       n.Roles,
		members:     n.Members,
		adminOption: n.AdminOption,
	}, nil
}

// StartExec begins execution of the node
func (n *GrantRoleNode) StartExec(params RunParams) error {
	opName := "grant-role"
	// Add memberships. Existing memberships are allowed.
	// If admin option is false, we do not remove it from existing memberships.
	memberStmt := `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, $3) ON CONFLICT ("role", "member")`
	if n.adminOption {
		// admin option: true, set "isAdmin" even if the membership exists.
		memberStmt += ` DO UPDATE SET "isAdmin" = true`
	} else {
		// admin option: false, do not clear it from existing memberships.
		memberStmt += ` DO NOTHING`
	}

	var rowsAffected int

	for _, r := range n.roles {
		for _, m := range n.members {
			affected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.Ctx,
				opName,
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				memberStmt,
				r, m, n.adminOption,
			)
			if err != nil {
				return err
			}

			rowsAffected += affected
		}
		params.GetPlanner().SetAuditTarget(0, string(r), nil)
	}

	// We need to bump the table version to trigger a refresh if anything changed.
	if rowsAffected > 0 {
		if err := params.GetPlanner().BumpTableVersion(params.Ctx, sqlconst.RoleMembersTableName); err != nil {
			return err
		}
	}

	n.run.rowsAffected += rowsAffected

	return nil
}

// Next implements the PlanNode interface.
func (*GrantRoleNode) Next(RunParams) (bool, error) { return false, nil }

// Values implements the PlanNode interface.
func (*GrantRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the PlanNode interface.
func (*GrantRoleNode) Close(context.Context) {}
