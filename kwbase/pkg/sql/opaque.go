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
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// DDLHandler is a function that handles a DDL statement and returns a PlanNode.
// It receives a *GenericPlanner and the AST node.
type DDLHandler func(ctx context.Context, p *GenericPlanner, n tree.Statement) (PlanNode, error)

var ddlHandlersMu syncutil.RWMutex
var ddlHandlers = map[reflect.Type]DDLHandler{}

// RegisterDDLHandler registers a DDL handler for a given statement type.
// This is called from init() functions in packages that provide DDL implementations.
func RegisterDDLHandler(t reflect.Type, handler DDLHandler) {
	ddlHandlersMu.Lock()
	defer ddlHandlersMu.Unlock()
	ddlHandlers[t] = handler
}

// getDDLHandler returns the registered DDL handler for the given statement type.
func getDDLHandler(t reflect.Type) (DDLHandler, bool) {
	ddlHandlersMu.RLock()
	defer ddlHandlersMu.RUnlock()
	h, ok := ddlHandlers[t]
	return h, ok
}

var planNodeNamesMu syncutil.RWMutex
var planNodeNamesRegistry = map[reflect.Type]string{}

// RegisterPlanNodeName registers a human-readable name for a PlanNode type.
// This is called from init() functions in packages that provide PlanNode implementations
// (such as the ddl package) to avoid circular imports with the sql package.
func RegisterPlanNodeName(typ reflect.Type, name string) {
	planNodeNamesMu.Lock()
	defer planNodeNamesMu.Unlock()
	planNodeNamesRegistry[typ] = name
}

// lookupPlanNodeName returns the registered name for a given PlanNode type,
// or false if no name was registered.
func lookupPlanNodeName(t reflect.Type) (string, bool) {
	planNodeNamesMu.RLock()
	defer planNodeNamesMu.RUnlock()
	name, ok := planNodeNamesRegistry[t]
	return name, ok
}

type opaqueMetadata struct {
	info string
	plan PlanNode
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata() {}
func (o *opaqueMetadata) String() string            { return o.info }

func buildOpaque(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
	p := evalCtx.Planner.(*GenericPlanner)

	// Opaque statements handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require(stmt.StatementTag(), tree.RejectSubqueries)

	var plan PlanNode
	var err error
	switch n := stmt.(type) {
	// most of DDL statement handling functions has been moved to ddlHandlers
	// we rely on this logic: reflect.TypeOf(stmt) --> statement Name --> ddlHandler
	// for all the centralized DDL handling
	case *tree.AlterAudit, *tree.AlterTSDatabase, *tree.AlterIndex, *tree.AlterPipe, *tree.AlterRole,
		*tree.AlterPub, *tree.AlterSequence, *tree.AlterSchedule, *tree.AlterStream, *tree.AlterTable,
		// Comment stmts
		*tree.CommentOnColumn, *tree.CommentOnDatabase, *tree.CommentOnIndex,
		*tree.CommentOnProcedure, *tree.CommentOnTable,
		// Create stmts. No *tree.CreateTable
		*tree.CreateAudit, *tree.CreateDatabase, *tree.CreateFunction,
		*tree.CreateIndex, *tree.CreateSchedule,
		*tree.CreateSchema, *tree.CreateRole, *tree.CreatePipe, *tree.CreatePublication, *tree.CreateSequence,
		*tree.CreateStats, *tree.CreateStream,
		// Drop stmts
		*tree.DropAudit, *tree.DropDatabase, *tree.DropFunction, *tree.DropIndex,
		*tree.DropRole, *tree.DropPipe, *tree.DropProcedure,
		*tree.DropPublication, *tree.DropSequence, *tree.DropSchedule, *tree.DropSchema, *tree.DropStream,
		*tree.DropTable, *tree.DropTrigger, *tree.DropView,
		// Rename stmts
		*tree.RenameColumn, *tree.RenameDatabase, *tree.RenameIndex, *tree.RenameTable, *tree.RenameTrigger,
		*tree.ShowHistogram, *tree.ShowSortHistogram, *tree.ShowTableStats, *tree.ShowTriggers:
		handler, ok := getDDLHandler(reflect.TypeOf(stmt))
		if !ok {
			panic(fmt.Sprintf("DDL statemnt to stmtName mapping failed: %s", stmt.String()))
			return nil, nil, errors.Errorf("DDL statemnt to stmtName mapping failed: %s", stmt.String())
		}
		plan, err = handler(ctx, p, n)
	case *tree.PauseSchedule:
		plan, err = p.PauseSchedule(ctx, n)
	case *tree.ResumeSchedule:
		plan, err = p.ResumeSchedule(ctx, n)
	case *tree.ShowDistribution:
		plan, err = p.ShowDistribution(ctx, n)
	case *tree.Deallocate:
		plan, err = p.Deallocate(ctx, n)
	case *tree.Discard:
		plan, err = p.Discard(ctx, n)
	case *tree.Grant:
		plan, err = p.Grant(ctx, n)
	case *tree.GrantRole:
		plan, err = p.GrantRole(ctx, n)
	case *tree.ImportPortal:
		plan, err = p.CreateImportPortal(ctx, n)
	case *tree.RebalanceTsData:
		plan, err = p.RebalanceTsDataNode(ctx, n)
	case *tree.RefreshMaterializedView:
		plan, err = p.RefreshMaterializedView(ctx, n)
	case *tree.Revoke:
		plan, err = p.Revoke(ctx, n)
	case *tree.RevokeRole:
		plan, err = p.RevokeRole(ctx, n)
	case *tree.Scatter:
		plan, err = p.Scatter(ctx, n)
	case *tree.Scrub:
		plan, err = p.Scrub(ctx, n)
	case *tree.SetClusterSetting:
		plan, err = p.SetClusterSetting(ctx, n)
	case *tree.SetZoneConfig:
		plan, err = p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		plan, err = p.SetVar(ctx, n)
	case *tree.SetTransaction:
		plan, err = p.SetTransaction(n)
	case *tree.SetSessionAuthorizationDefault:
		plan, err = p.SetSessionAuthorizationDefault()
	case *tree.SetSessionCharacteristics:
		plan, err = p.SetSessionCharacteristics(n)
	case *tree.ShowClusterSetting:
		plan, err = p.ShowClusterSetting(ctx, n)
	case *tree.ShowTraceForSession:
		plan, err = p.ShowTrace(ctx, n)
	case *tree.ShowZoneConfig:
		plan, err = p.ShowZoneConfig(ctx, n)
	case *tree.ShowFingerprints:
		plan, err = ShowFingerprints(ctx, p, n)
	case *tree.Truncate:
		plan, err = p.Truncate(ctx, n)
	case *tree.Vacuum:
		plan, err = p.Vacuum(ctx, n)
	case *tree.Backup:
		plan, err = p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, nil, errors.Errorf("The BACKUP can only be used in the enterprise version")
		}
	case *tree.Restore:
		plan, err = p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, nil, errors.Errorf("The RESTORE can only be used in the enterprise version")
		}
	case tree.CCLOnlyStatement:
		plan, err = p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, nil, pgerror.Newf(pgcode.CCLRequired,
				"a CCL binary is required to use this statement type: %T", stmt)
		}
	default:
		return nil, nil, errors.AssertionFailedf("unknown opaque statement %T", stmt)
	}
	if err != nil {
		return nil, nil, err
	}
	res := &opaqueMetadata{
		info: stmt.StatementTag(),
		plan: plan,
	}
	return res, planColumns(plan), nil
}

func init() {
	for _, stmt := range []tree.Statement{
		&tree.AlterTSDatabase{},
		&tree.AlterIndex{},
		&tree.AlterStream{},
		&tree.AlterPipe{},
		&tree.AlterPub{},
		&tree.AlterTable{},
		&tree.AlterSequence{},
		&tree.AlterRole{},
		&tree.AlterAudit{},
		&tree.AlterSchedule{},
		&tree.CommentOnColumn{},
		&tree.CommentOnDatabase{},
		&tree.CommentOnProcedure{},
		&tree.CommentOnIndex{},
		&tree.CommentOnTable{},
		&tree.CreateDatabase{},
		&tree.CreateFunction{},
		&tree.CreateSchedule{},
		&tree.CreateIndex{},
		&tree.CreatePipe{},
		&tree.CreatePublication{},
		&tree.CreateSchema{},
		&tree.CreateSequence{},
		&tree.CreateStats{},
		&tree.CreateStream{},
		&tree.CreateRole{},
		&tree.CreateAudit{},
		&tree.Deallocate{},
		&tree.Discard{},
		&tree.DropDatabase{},
		&tree.DropIndex{},
		&tree.DropPipe{},
		&tree.DropPublication{},
		&tree.DropSchema{},
		&tree.DropSchedule{},
		&tree.DropStream{},
		&tree.DropTable{},
		&tree.DropView{},
		&tree.DropRole{},
		&tree.DropSequence{},
		&tree.DropFunction{},
		&tree.DropProcedure{},
		&tree.DropTrigger{},
		&tree.ShowTriggers{},
		&tree.ShowDistribution{},
		&tree.DropAudit{},
		&tree.ImportPortal{},
		&tree.Grant{},
		&tree.GrantRole{},
		&tree.RefreshMaterializedView{},
		&tree.RenameColumn{},
		&tree.RenameDatabase{},
		&tree.RenameTrigger{},
		&tree.RenameIndex{},
		&tree.RenameTable{},
		&tree.ResumeSchedule{},
		&tree.Revoke{},
		&tree.RevokeRole{},
		&tree.PauseSchedule{},
		&tree.Scatter{},
		&tree.Scrub{},
		&tree.SetClusterSetting{},
		&tree.SetZoneConfig{},
		&tree.SetVar{},
		&tree.SetTransaction{},
		&tree.SetSessionAuthorizationDefault{},
		&tree.SetSessionCharacteristics{},
		&tree.ShowClusterSetting{},
		&tree.ShowHistogram{},
		&tree.ShowSortHistogram{},
		&tree.ShowTableStats{},
		&tree.ShowTraceForSession{},
		&tree.ShowZoneConfig{},
		&tree.ShowFingerprints{},
		&tree.RebalanceTsData{},
		&tree.ReplicationControl{},
		&tree.ReplicateSetRole{},
		&tree.ReplicateSetSecondary{},
		&tree.Truncate{},
		&tree.Vacuum{},

		// CCL statements (without Export which has an optimizer operator).
		&tree.Backup{},
		&tree.ShowBackup{},
		&tree.Restore{},
		&tree.CreateChangefeed{},
		&tree.Import{},
	} {
		typ := optbuilder.OpaqueReadOnly
		if tree.CanModifySchema(stmt) {
			typ = optbuilder.OpaqueDDL
		} else if tree.CanWriteData(stmt) {
			typ = optbuilder.OpaqueMutation
		}
		optbuilder.RegisterOpaque(reflect.TypeOf(stmt), typ, buildOpaque)
	}
}
