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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/setting"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func TestMaybeLogStatementInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typ := executorTypeExec
	typ.logLabel()
	typ.vLevel()

	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, _, kvdb := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	p := makeTestPlanner()

	setting.AuditEnabled.Override(&p.execCfg.Settings.SV, true)
	p.execCfg.DB = kvdb
	p.execCfg.AuditServer = s.AuditServer()
	p.curPlan.stmt = &Statement{Statement: parser.Statement{AST: &tree.ProcSet{Name: "e1", Value: tree.MakeDBool(true)}}}
	p.maybeLogStatementInternal(ctx, typ, 3, 10, pgerror.New("test", "test"), timeutil.Now())

	factory := TestProcedureSenderFactory(
		func(_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return ba.CreateReply(), nil
		})

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	txn := kv.NewTxn(ctx, db, 1)
	p.LogAudit(ctx, txn, 10, pgerror.New("test", "test"), timeutil.Now())
}

func TestSetAuditEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	p.curPlan.stmt = &Statement{Statement: parser.Statement{AST: &tree.ProcSet{Name: "e1", Value: tree.MakeDBool(true)}}}
	p.SetAuditEvent()
}

func TestSetAuditLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	p.SetAuditLevel(target.StmtLevel)
}
