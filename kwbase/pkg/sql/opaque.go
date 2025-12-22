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
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type opaqueMetadata struct {
	info string
	plan planNode
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata() {}
func (o *opaqueMetadata) String() string            { return o.info }

func buildOpaque(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
	p := evalCtx.Planner.(*planner)

	// Opaque statements handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require(stmt.StatementTag(), tree.RejectSubqueries)

	var plan planNode
	var err error
	switch n := stmt.(type) {
	case *tree.AlterTSDatabase:
		plan, err = p.AlterTSDatabase(ctx, n)
	case *tree.AlterIndex:
		plan, err = p.AlterIndex(ctx, n)
	case *tree.AlterStream:
		plan, err = p.AlterStream(ctx, n)
	case *tree.AlterTable:
		plan, err = p.AlterTable(ctx, n)
	case *tree.AlterRole:
		plan, err = p.AlterRole(ctx, n)
	case *tree.AlterSequence:
		plan, err = p.AlterSequence(ctx, n)
	case *tree.AlterAudit:
		plan, err = p.AlterAudit(ctx, n)
	case *tree.CommentOnColumn:
		plan, err = p.CommentOnColumn(ctx, n)
	case *tree.CommentOnDatabase:
		plan, err = p.CommentOnDatabase(ctx, n)
	case *tree.CommentOnProcedure:
		plan, err = p.CommentOnProcedure(ctx, n)
	case *tree.CommentOnIndex:
		plan, err = p.CommentOnIndex(ctx, n)
	case *tree.CommentOnTable:
		plan, err = p.CommentOnTable(ctx, n)
	case *tree.CreateDatabase:
		plan, err = p.CreateDatabase(ctx, n)
	case *tree.CreateFunction:
		plan, err = p.CreateFunction(ctx, n)
	case *tree.CreateSchedule:
		plan, err = p.CreateSchedule(ctx, n)
	case *tree.AlterSchedule:
		plan, err = p.AlterSchedule(ctx, n)
	case *tree.PauseSchedule:
		plan, err = p.PauseSchedule(ctx, n)
	case *tree.ResumeSchedule:
		plan, err = p.ResumeSchedule(ctx, n)
	case *tree.DropSchedule:
		plan, err = p.DropSchedule(ctx, n)
	case *tree.CreateIndex:
		plan, err = p.CreateIndex(ctx, n)
	case *tree.CreateSchema:
		plan, err = p.CreateSchema(ctx, n)
	case *tree.CreateRole:
		plan, err = p.CreateRole(ctx, n)
	case *tree.CreateSequence:
		plan, err = p.CreateSequence(ctx, n)
	case *tree.CreateStats:
		plan, err = p.CreateStatistics(ctx, n)
	case *tree.CreateStream:
		plan, err = p.CreateStream(ctx, n)
	case *tree.CreateAudit:
		plan, err = p.CreateAudit(ctx, n)
	case *tree.Deallocate:
		plan, err = p.Deallocate(ctx, n)
	case *tree.Discard:
		plan, err = p.Discard(ctx, n)
	case *tree.DropDatabase:
		plan, err = p.DropDatabase(ctx, n)
	case *tree.DropIndex:
		plan, err = p.DropIndex(ctx, n)
	case *tree.DropRole:
		plan, err = p.DropRole(ctx, n)
	case *tree.DropSchema:
		plan, err = p.DropSchema(ctx, n)
	case *tree.DropStream:
		plan, err = p.DropStream(ctx, n)
	case *tree.DropTable:
		plan, err = p.DropTable(ctx, n)
	case *tree.DropFunction:
		plan, err = p.DropFunction(ctx, n)
	case *tree.DropProcedure:
		plan, err = p.DropProcedure(ctx, n)
	case *tree.DropTrigger:
		plan, err = p.DropTrigger(ctx, n)
	case *tree.ShowTriggers:
		plan, err = p.ShowTriggers(ctx, n)
	case *tree.ShowDistribution:
		plan, err = p.ShowDistribution(ctx, n)
	case *tree.DropView:
		plan, err = p.DropView(ctx, n)
	case *tree.DropSequence:
		plan, err = p.DropSequence(ctx, n)
	case *tree.DropAudit:
		plan, err = p.DropAudit(ctx, n)
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
	case *tree.RenameColumn:
		plan, err = p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		plan, err = p.RenameDatabase(ctx, n)
	case *tree.RenameTrigger:
		plan, err = p.RenameTrigger(ctx, n)
	case *tree.RenameIndex:
		plan, err = p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		plan, err = p.RenameTable(ctx, n)
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
	case *tree.ShowHistogram:
		plan, err = p.ShowHistogram(ctx, n)
	case *tree.ShowSortHistogram:
		plan, err = p.ShowSortHistogram(ctx, n)
	case *tree.ShowTableStats:
		plan, err = p.ShowTableStats(ctx, n)
	case *tree.ShowTraceForSession:
		plan, err = p.ShowTrace(ctx, n)
	case *tree.ShowZoneConfig:
		plan, err = p.ShowZoneConfig(ctx, n)
	case *tree.ShowFingerprints:
		plan, err = p.ShowFingerprints(ctx, n)
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
