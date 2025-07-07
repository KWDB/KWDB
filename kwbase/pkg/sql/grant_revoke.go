// Copyright 2015 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/pkg/errors"
)

// Grant adds privileges to users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//
//	Notes: postgres requires the object owner.
//	       mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(ctx context.Context, n *tree.Grant) (planNode, error) {
	if n.Targets.Databases != nil {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnDatabase)
	} else if n.Targets.Schemas != nil {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnSchema)
	} else if n.Targets.Tables != nil {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnTable)
	} else {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnProcedure)
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		isGrant:      true,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Grant(grantee, n.Privileges)
		},
	}, nil
}

// Revoke removes privileges from users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//
//	Notes: postgres requires the object owner.
//	       mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(ctx context.Context, n *tree.Revoke) (planNode, error) {
	if n.Targets.Databases != nil {
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnDatabase)
	} else if n.Targets.Schemas != nil {
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnSchema)
	} else if n.Targets.Tables != nil {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnTable)
	} else {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnProcedure)
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Revoke(grantee, n.Privileges)
		},
	}, nil
}

type changePrivilegesNode struct {
	targets         tree.TargetList
	grantees        tree.NameList
	desiredprivs    privilege.List
	isGrant         bool
	changePrivilege func(*sqlbase.PrivilegeDescriptor, string)
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changePrivilegesNode) ReadingOwnWrites() {}

func (n *changePrivilegesNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	// Check whether grantees exists
	users, err := p.GetAllRoles(ctx)
	if err != nil {
		return err
	}

	// We're allowed to grant/revoke privileges to/from the "public" role even though
	// it does not exist: add it to the list of all users and roles.
	users[sqlbase.PublicRole] = true // isRole

	for _, grantee := range n.grantees {
		if _, ok := users[string(grantee)]; !ok {
			return errors.Errorf("user or role %s does not exist", &grantee)
		}
	}

	var descriptors []sqlbase.DescriptorProto
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptors, err = getDescriptorsFromTargetList(ctx, p, n.targets)
	})
	if err != nil {
		return err
	}

	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	b := p.txn.NewBatch()
	var targetID []uint64
	var targets string
	for _, descriptor := range descriptors {
		if err := p.CheckPrivilege(ctx, descriptor, privilege.GRANT); err != nil {
			return err
		}

		// Only allow granting/revoking privileges that the requesting
		// user themselves have on the descriptor.
		for _, priv := range n.desiredprivs {
			if err := p.CheckPrivilege(ctx, descriptor, priv); err != nil {
				return err
			}
		}

		privileges := descriptor.GetPrivileges()
		for _, grantee := range n.grantees {
			n.changePrivilege(privileges, string(grantee))
		}

		// Validate privilege descriptors directly as the db/table level Validate
		// may fix up the descriptor.
		if err := privileges.Validate(descriptor.GetID()); err != nil {
			return err
		}
		targetID = append(targetID, uint64(descriptor.GetID()))
		targets = targets + " " + descriptor.GetName()

		switch d := descriptor.(type) {
		case *sqlbase.DatabaseDescriptor:
			if err := d.Validate(); err != nil {
				return err
			}
			if err := writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, descriptor.GetID(), descriptor); err != nil {
				return err
			}

		case *sqlbase.MutableTableDescriptor:
			// TODO (lucy): This should probably have a single consolidated job like
			// DROP DATABASE.
			// TODO (lucy): Have more consistent/informative names for dependent jobs.
			if err := p.createOrUpdateSchemaChangeJob(
				ctx, d, "updating privileges", sqlbase.InvalidMutationID,
			); err != nil {
				return err
			}
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(ctx, d, b); err != nil {
					return err
				}
			}

		case *sqlbase.SchemaDescriptor:
			if err := d.Validate(); err != nil {
				return err
			}
			if err := writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, descriptor.GetID(), descriptor); err != nil {
				return err
			}

		case *sqlbase.ProcedureDescriptor:
			if err := d.Validate(); err != nil {
				return err
			}
			if err := UpdateProcedureMeta(ctx, p.txn, d); err != nil {
				return err
			}
		}
		params.p.SetAuditTarget(uint32(descriptor.GetID()), descriptor.GetName(), nil)
	}

	// Now update the descriptors transactionally.
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	return nil
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}
