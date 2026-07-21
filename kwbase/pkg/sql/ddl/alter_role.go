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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// AlterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	userNameInfo
	ifExists    bool
	isRole      bool
	roleOptions roleoption.List
}

// NewAlterRoleNode creates a new alterRoleNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterRoleNode(
	userNameInfo userNameInfo, ifExists bool, isRole bool, roleOptions roleoption.List,
) *alterRoleNode {
	return &alterRoleNode{
		userNameInfo: userNameInfo,
		ifExists:     ifExists,
		isRole:       isRole,
		roleOptions:  roleOptions,
	}
}

// AlterRole represents a ALTER ROLE statement.
// Privileges: CREATEROLE privilege.
func AlterRole(ctx context.Context, p *GenericPlanner, n *tree.AlterRole) (sql.PlanNode, error) {
	return AlterRoleNodeImp(ctx, p, n.Name, n.IfExists, n.IsRole, "ALTER ROLE", n.KVOptions)
}

// AlterRoleNodeImp implements the ALTER ROLE node execution logic
// nolint:unexportedreturn
func AlterRoleNodeImp(
	ctx context.Context,
	p *GenericPlanner,
	nameE tree.Expr,
	ifExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*alterRoleNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.HasRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	roleOptions, err := kvOptions.ToRoleOptions(p.TypeAsStringOrNull, opName)
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

	return &alterRoleNode{
		userNameInfo: ua,
		ifExists:     ifExists,
		isRole:       isRole,
		roleOptions:  roleOptions,
	}, nil
}

func (n *alterRoleNode) StartExec(params RunParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.Role)
		opName = "alter-role"
	} else {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.User)
		opName = "alter-user"
	}
	name, err := n.name()
	if err != nil {
		return err
	}
	if name == "" {
		return errNoUserNameSpecified
	}
	if name == "admin" {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot edit admin role")
	}
	normalizedUsername, err := sqlutil.NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	// Check if role exists.
	row, err := params.ExecCfg().InternalExecutor.QueryRowEx(
		params.Ctx,
		opName,
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("SELECT * FROM %s WHERE username = $1", sqlconst.UserTableName),
		normalizedUsername,
	)
	if err != nil {
		return err
	}
	if row == nil {
		if n.ifExists {
			return nil
		}
		return errors.Newf("role/user %s does not exist", normalizedUsername)
	}

	if n.roleOptions.Contains(roleoption.PASSWORD) {
		hashedPassword, err := n.roleOptions.GetHashedPassword()
		if err != nil {
			return err
		}

		// TODO(knz): Remove in 20.2.
		if normalizedUsername == security.RootUser && len(hashedPassword) > 0 &&
			!params.EvalContext().Settings.Version.IsActive(params.Ctx, clusterversion.VersionRootPassword) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				`setting a root password requires all nodes to be upgraded to %s`,
				clusterversion.VersionByKey(clusterversion.VersionRootPassword),
			)
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

		// Updating PASSWORD is a special case since PASSWORD lives in system.users
		// while the rest of the role options lives in system.role_options.
		_, err = params.ExecCfg().InternalExecutor.Exec(
			params.Ctx,
			opName,
			params.PlannerTxn(),
			`UPDATE system.users SET "hashedPassword" = $2 WHERE username = $1`,
			normalizedUsername,
			hashedPassword,
		)
		if err != nil {
			return err
		}
	}

	// Get a map of statements to execute for role options and their values.
	stmts, err := n.roleOptions.GetSQLStmts(sqltelemetry.AlterRole)
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

		_, err := params.ExecCfg().InternalExecutor.ExecEx(
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

func (*alterRoleNode) Next(RunParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}
