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
	"math/bits"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MembershipCache is a shared cache for role membership information.
type MembershipCache struct {
	syncutil.Mutex
	tableVersion sqlbase.DescriptorVersion
	// userCache is a mapping from username to userRoleMembership.
	userCache map[string]userRoleMembership
}

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[string]bool

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
	) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error

	// HasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	HasAdminRole(ctx context.Context) (bool, error)

	// RequireAdminRole is a wrapper on top of HasAdminRole.
	// It errors if HasAdminRole errors or if the user isn't a super-user.
	// Includes the named action in the error message.
	RequireAdminRole(ctx context.Context, action string) error

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)

	// LogAudit log audit information
	LogAudit(ctx context.Context, txn *kv.Txn, rows int, err error, startTime time.Time)

	//SetAuditTargetAndType sets audit information
	SetAuditTargetAndType(id uint32, name string, cascade []string, targetType target.AuditObjectType)
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use CheckPrivilege without a txn")
	}

	// Test whether the object is being audited, and if so, record an
	// audit event. We place this check here to increase the likelihood
	// it will not be forgotten if features are added that access
	// descriptors (since every use of descriptors presumably need a
	// permission check).
	p.maybeAudit(descriptor, privilege)

	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.CheckPrivilege(user, privilege) {
		return nil
	}

	// Check if the 'public' pseudo-role has privileges.
	if privs.CheckPrivilege(sqlbase.PublicRole, privilege) {
		return nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if privs.CheckPrivilege(role, privilege) {
			return nil
		}
	}

	typeName := getDescTypeName(descriptor)
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, privilege, typeName, descriptor.GetName())
}

// CheckPrivilegeBitmap check if desc has privileges in privilege map
func CheckPrivilegeBitmap(
	ctx context.Context, p *planner, desc sqlbase.DescriptorProto, PrivilegeBitmap uint32,
) error {
	for privs := PrivilegeBitmap; privs != 0; {
		priv := privilege.Kind(bits.TrailingZeros32(privs))
		if priv != 0 {
			if err := p.CheckPrivilege(ctx, desc, priv); err != nil {
				return err
			}
		}
		privs &= ^(1 << priv)
	}
	return nil
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use CheckAnyPrivilege without a txn")
	}

	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.AnyPrivilege(user) {
		return nil
	}

	// Check if 'public' has privileges.
	if privs.AnyPrivilege(sqlbase.PublicRole) {
		return nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if privs.AnyPrivilege(role) {
			return nil
		}
	}

	typeName := getDescTypeName(descriptor)
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s has no privileges on %s %s",
		p.SessionData().User, typeName, descriptor.GetName())
}

// HasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) HasAdminRole(ctx context.Context) (bool, error) {
	user := p.SessionData().User
	if user == "" {
		return false, errors.AssertionFailedf("empty user")
	}
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return false, errors.AssertionFailedf("cannot use HasAdminRole without a txn")
	}

	// Check if user is 'root' or 'node'.
	// TODO(knz): planner HasAdminRole has no business authorizing
	// the "node" principal - node should not be issuing SQL queries.
	if user == security.RootUser || user == security.NodeUser {
		return true, nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}

	// Check is 'user' is a member of role 'admin'.
	if _, ok := memberOf[sqlbase.AdminRole]; ok {
		return true, nil
	}

	return false, nil
}

// RequireAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) RequireAdminRole(ctx context.Context, action string) error {
	ok, err := p.HasAdminRole(ctx)

	if err != nil {
		return err
	}
	if !ok {
		//raise error if user is not a super-user
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to %s", action)
	}
	return nil
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
// Requires a valid transaction to be open.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	if p.txn == nil || !p.txn.IsOpen() {
		return nil, errors.AssertionFailedf("cannot use MemberOfWithAdminoption without a txn")
	}

	roleMembersCache := p.execCfg.RoleMemberCache

	// Lookup table version.
	objDesc, err := p.PhysicalSchemaAccessor().GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings, &roleMembersTableName,
		p.ObjectLookupFlags(true /*required*/, false /*requireMutable*/))
	if err != nil {
		return nil, err
	}
	tableDesc := objDesc.TableDesc()
	tableVersion := tableDesc.Version
	log.VEventf(ctx, 3, "user=%s, table version=%v, cache table version=%v",
		member, tableVersion, roleMembersCache.tableVersion)
	// We loop in case the table version changes while we're looking up memberships.
	for {
		// Check version and maybe clear cache while holding the mutex.
		// We release the lock here instead of using defer as we need to keep
		// going and re-lock if adding the looked-up entry.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Update version and drop the map.
			roleMembersCache.tableVersion = tableVersion
			roleMembersCache.userCache = make(map[string]userRoleMembership)
		}

		userMapping, ok := roleMembersCache.userCache[member]
		roleMembersCache.Unlock()

		if ok {
			// Found: return.
			log.VEventf(ctx, 3, "user=%s, find user in cache, role map=%v",
				member, userMapping)
			return userMapping, nil
		}

		// Lookup memberships outside the lock.
		memberships, err := p.resolveMemberOfWithAdminOption(ctx, member)
		if err != nil {
			return nil, err
		}

		// Update membership.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Table version has changed while we were looking, unlock and start over.
			tableVersion = roleMembersCache.tableVersion
			roleMembersCache.Unlock()
			continue
		}

		// Table version remains the same: update map, unlock, return.
		roleMembersCache.userCache[member] = memberships
		roleMembersCache.Unlock()
		return memberships, nil
	}
}

// resolveMemberOfWithAdminOption performs the actual recursive role membership lookup.
// TODO(mberhault): this is the naive way and performs a full lookup for each user,
// we could save detailed memberships (as opposed to fully expanded) and reuse them
// across users. We may then want to lookup more than just this user.
func (p *planner) resolveMemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := p.ExecCfg().InternalExecutor.Query(
			ctx, "expand-roles", nil /* txn */, lookupRolesStmt, m,
		)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	return ret, nil
}

// HasRoleOption converts the roleoption to it's SQL column name and
// checks if the user belongs to a role where the roleprivilege has value true.
// Only works on checking the "positive version" of the privilege.
// Requires a valid transaction to be open.
// Example: CREATEROLE instead of NOCREATEROLE.
func (p *planner) HasRoleOption(ctx context.Context, roleOption roleoption.Option) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use HasRoleOption without a txn")
	}

	user := p.SessionData().User
	if user == security.RootUser || user == security.NodeUser {
		return nil
	}

	normalizedName, err := NormalizeAndValidateUsername(user)
	if err != nil {
		return err
	}

	// Create list of roles for sql WHERE IN clause.
	memberOf, err := p.MemberOfWithAdminOption(ctx, normalizedName)
	if err != nil {
		return err
	}

	var roles = tree.NewDArray(types.String)
	err = roles.Append(tree.NewDString(normalizedName))
	if err != nil {
		return err
	}
	for role := range memberOf {
		err := roles.Append(tree.NewDString(role))
		if err != nil {
			return err
		}
	}

	hasRolePrivilege, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx, "has-role-option", p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			`SELECT 1 from %s WHERE option = '%s' AND username = ANY($1) LIMIT 1`,
			RoleOptionsTableName,
			roleOption.String()),
		roles)

	if err != nil {
		return err
	}

	if len(hasRolePrivilege) != 0 {
		return nil
	}

	// User is not a member of a role that has CREATEROLE privilege.
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege", user, roleOption.String())
}

// ConnAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level connection audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const ConnAuditingClusterSettingName = "server.auth_log.sql_connections.enabled"

// AuthAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level authentication audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const AuthAuditingClusterSettingName = "server.auth_log.sql_sessions.enabled"

// shouldCheckPublicSchema indicates whether canCreateOnSchema should check
// CREATE privileges for the public schema.
type shouldCheckPublicSchema bool

const (
	checkPublicSchema     shouldCheckPublicSchema = true
	skipCheckPublicSchema shouldCheckPublicSchema = false
)

// canCreateOnSchema returns whether a user has permission to create new objects
// on the specified schema. For `public` schemas, it checks if the user has
// CREATE privileges on the specified dbID. Note that skipCheckPublicSchema may
// be passed to skip this check, since some callers check this separately.
//
// Privileges on temporary schemas are not validated. This is the caller's
// responsibility.
func (p *planner) canCreateOnSchema(
	ctx context.Context, scName string, dbID sqlbase.ID, checkPublicSchema shouldCheckPublicSchema,
) error {
	_, resolvedSchema, err := p.ResolveUncachedSchemaDescriptor(ctx, dbID, scName, true /* required */)
	if err != nil {
		return err
	}

	switch resolvedSchema.Kind {
	case sqlbase.SchemaPublic:
		// The public schema is valid to create in if the parent database is.
		if !checkPublicSchema {
			// The caller wishes to skip this check.
			return nil
		}
		dbDesc, err := getDatabaseDescByID(ctx, p.Txn(), dbID)
		if err != nil {
			return err
		}
		return p.CheckPrivilege(ctx, dbDesc, privilege.CREATE)
	case sqlbase.SchemaTemporary:
		// Callers must check whether temporary schemas are valid to create in.
		return nil
	case sqlbase.SchemaVirtual:
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot CREATE on schema %s", resolvedSchema.Name)
	case sqlbase.SchemaUserDefined:
		return p.CheckPrivilege(ctx, resolvedSchema.Desc, privilege.CREATE)
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", resolvedSchema.Kind))
	}
}

// canDropInsTable returns whether a user has permission to drop the instance table.
func (p *planner) canDropInsTable(ctx context.Context, templateTableID sqlbase.ID) error {
	// get template tableDesc by ID
	tplTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, templateTableID)
	if err != nil {
		return err
	}
	// Check permissions.
	if err := p.CheckPrivilege(ctx, tplTable, privilege.DROP); err != nil {
		return err
	}

	return nil
}

// getDescTypeName returns type name, or "template table", if an instance table.
func getDescTypeName(descriptor sqlbase.DescriptorProto) string {
	switch desc := descriptor.(type) {
	case *ImmutableTableDescriptor:
		if desc.TableType == tree.InstanceTable {
			return "template table"
		}
	case *MutableTableDescriptor:
		if desc.TableType == tree.InstanceTable {
			return "template table"
		}
	case *TableDescriptor:
		if desc.TableType == tree.InstanceTable {
			return "template table"
		}
	}
	return descriptor.TypeName()
}
