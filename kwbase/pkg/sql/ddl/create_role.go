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

package ddl

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// CreateRoleNode creates entries in the system.users table.
// This is called from CREATE USER and CREATE ROLE.
var _ sql.PlanNode = &CreateRoleNode{}

// CreateRoleNode represents a CREATE ROLE/USER statement execution node
type CreateRoleNode struct {
	ifNotExists bool
	isRole      bool
	roleOptions roleoption.List
	userNameInfo
}

// CreateRole represents a CREATE ROLE statement.
// Privileges: INSERT on system.users.
//
//	notes: postgres allows the creation of users with an empty password. We do
//	       as well, but disallow password authentication for these users.
func CreateRole(ctx context.Context, p *GenericPlanner, n *tree.CreateRole) (sql.PlanNode, error) {
	return createRoleNode(ctx, p, n.Name, n.IfNotExists, n.IsRole,
		"CREATE ROLE", n.KVOptions)
}

// CreateRoleNode creates a "create user" plan node.
// This can be called from CREATE USER or CREATE ROLE.
func createRoleNode(
	ctx context.Context,
	p *GenericPlanner,
	nameE tree.Expr,
	ifNotExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*CreateRoleNode, error) {
	if err := p.HasRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	roleOptions, err := kvOptions.ToRoleOptions(p.TypeAsStringOrNull, opName)

	// Using CREATE ROLE syntax enables NOLOGIN by default.
	if isRole && !roleOptions.Contains(roleoption.LOGIN) &&
		!roleOptions.Contains(roleoption.NOLOGIN) {
		roleOptions = append(roleOptions,
			roleoption.RoleOption{Option: roleoption.NOLOGIN, HasValue: false})
	}

	if err != nil {
		return nil, err
	}

	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	ua, err := getUserAuthInfo(p, nameE, opName)
	if err != nil {
		return nil, err
	}

	return &CreateRoleNode{
		userNameInfo: ua,
		ifNotExists:  ifNotExists,
		isRole:       isRole,
		roleOptions:  roleOptions,
	}, nil
}

// StartExec begins execution of the node
func (n *CreateRoleNode) StartExec(params RunParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.Role)
		opName = "create-role"
	} else {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.User)
		opName = "create-user"
	}

	normalizedUsername, err := n.userNameInfo.resolveUsername()
	if err != nil {
		return err
	}

	var hashedPassword []byte
	if n.roleOptions.Contains(roleoption.PASSWORD) {
		hashedPassword, err = n.roleOptions.GetHashedPassword()
		if err != nil {
			return err
		}

		if len(hashedPassword) > 0 && params.ExecCfg().RPCContext.Insecure {
			// We disallow setting a non-empty password in insecure mode
			// because insecure means an observer may have MITM'ed the change
			// and learned the password.
			//
			// It's valid to clear the password (WITH PASSWORD NULL) however
			// since that forces cert auth when moving back to secure mode,
			// and certs can't be MITM'ed over the insecure SQL connection.
			return pgerror.New(pgcode.InvalidPassword,
				"setting or updating a password is not supported in insecure mode")
		}
	}

	// Reject the "public" role. It does not have an entry in the users table but is reserved.
	if normalizedUsername == sqlbase.PublicRole {
		return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", sqlbase.PublicRole)
	}

	// Check if the user/role exists.
	row, err := params.ExecCfg().InternalExecutor.QueryRowEx(
		params.Ctx,
		opName,
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(`select "isRole" from %s where username = $1`, sqlconst.UserTableName),
		normalizedUsername,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up user")
	}
	if row != nil {
		if n.ifNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject,
			"a role/user named %s already exists", normalizedUsername)
	}

	// TODO(richardjcai): move hashedPassword column to system.role_options.
	rowsAffected, err := params.ExecCfg().InternalExecutor.Exec(
		params.Ctx,
		opName,
		params.PlannerTxn(),
		fmt.Sprintf("insert into %s values ($1, $2, $3)", sqlconst.UserTableName),
		normalizedUsername,
		hashedPassword,
		n.isRole,
	)

	if err != nil {
		return err
	} else if rowsAffected != 1 {
		return errors.AssertionFailedf("%d rows affected by user creation; expected exactly one row affected",
			rowsAffected,
		)
	}

	// Get a map of statements to execute for role options and their values.
	stmts, err := n.roleOptions.GetSQLStmts(sqltelemetry.CreateRole)
	if err != nil {
		return err
	}

	for stmt, value := range stmts {
		qargs := []interface{}{normalizedUsername}

		if value != nil {
			isNull, val, err := value()
			if err != nil {
				return err
			}
			if isNull {
				// If the value of the role option is NULL, ensure that nil is passed
				// into the statement placeholder, since val is string type "NULL"
				// will not be interpreted as NULL by the InternalExecutor.
				qargs = append(qargs, nil)
			} else {
				qargs = append(qargs, val)
			}
		}

		_, err = params.ExecCfg().InternalExecutor.ExecEx(
			params.Ctx,
			opName,
			params.PlannerTxn(),
			InternalExecutorSessionDataOverride{User: security.RootUser},
			stmt,
			qargs...,
		)
		if err != nil {
			return err
		}
	}

	params.GetPlanner().SetAuditTarget(0, normalizedUsername, nil)
	return nil
}

// Next implements the PlanNode interface.
func (*CreateRoleNode) Next(RunParams) (bool, error) { return false, nil }

// Values implements the PlanNode interface.
func (*CreateRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the PlanNode interface.
func (*CreateRoleNode) Close(context.Context) {}

var errNoUserNameSpecified = errors.New("no username specified")

type userNameInfo struct {
	name func() (string, error)
}

func getUserAuthInfo(p *GenericPlanner, nameE tree.Expr, ctx string) (userNameInfo, error) {
	name, err := sql.TypeAsString(p, nameE, ctx)
	if err != nil {
		return userNameInfo{}, err
	}

	return userNameInfo{name: name}, nil
}

// resolveUsername returns the actual user name.
func (ua *userNameInfo) resolveUsername() (string, error) {
	name, err := ua.name()
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errNoUserNameSpecified
	}
	normalizedUsername, err := sqlutil.NormalizeAndValidateUsername(name)
	if err != nil {
		return "", err
	}

	return normalizedUsername, nil
}
