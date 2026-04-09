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

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/procedure"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func testProcedurePlanFn(
	expr memo.RelExpr, pr *physical.Required, src []*exec.LocalVariable,
) (exec.Plan, error) {
	return expr, nil
}

type testProcedureSenderFactory kv.SenderFunc

var _ kv.TxnSenderFactory = testProcedureSenderFactory(nil)

func (f testProcedureSenderFactory) RootTransactionalSender(
	txn *roachpb.Transaction, _ roachpb.UserPriority,
) kv.TxnSender {
	return kv.NewMockTransactionalSender(
		func(
			ctx context.Context, _ *roachpb.Transaction, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		},
		txn)
}

func (f testProcedureSenderFactory) LeafTransactionalSender(
	tis *roachpb.LeafTxnInputState,
) kv.TxnSender {
	return kv.NewMockTransactionalSender(
		func(
			ctx context.Context, _ *roachpb.Transaction, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		},
		&tis.Txn)
}

func (f testProcedureSenderFactory) NonTransactionalSender() kv.Sender {
	return nil
}

func TestCallProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makeTestPlanner()
	node := callProcedureNode{
		procName: "test",
		fn:       testProcedurePlanFn,
		params: runParams{
			ctx:             context.TODO(),
			extendedEvalCtx: p.extendedEvalCtx.copy(),
			p:               p,
		},
	}

	// Mock out DistSender's sender function to check the read consistency for
	// outgoing BatchRequests and return an empty reply.
	factory := testProcedureSenderFactory(
		func(_ context.Context, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return ba.CreateReply(), nil
		})

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	p.txn = kv.NewTxn(context.TODO(), db, 1)
	node.execCtx.Init()
	node.execCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
	node.endTransaction(true)
}

// Instruction represents a well-typed expression.
type testErrInstruction struct {
}

// Execute starts execute instruction
func (t *testErrInstruction) Execute(
	params procedure.RunParam, rCtx *procedure.SpExecContext,
) error {
	return pgerror.New("111", "test")
}

// Close will close all resource
func (t *testErrInstruction) Close() {}

func TestCallProcedureStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}
	test := callProcedureNode{
		procName: "test",
		fn:       testProcedurePlanFn,
		params:   runParam,
		ins:      &testErrInstruction{},
	}
	require.Error(t, test.startExec(runParam))

	runParam.p.extendedEvalCtx.TxnImplicit = false
	require.Error(t, test.startExec(runParam))
}

func TestHasNextResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}
	test := callProcedureNode{
		procName: "test",
		fn:       testProcedurePlanFn,
		params:   runParam,
		ins:      &testErrInstruction{},
	}
	_, err := test.HasNextResult(true)
	require.Error(t, err)

	test.err = pgerror.New("111", "test")
	_, err = test.HasNextResult(true)
	require.Error(t, err)
}

func TestProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	//execCfg := s.ExecutorConfig().(ExecutorConfig)
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, `
		CREATE DATABASE procedure_db;
		USE procedure_db;
	`)

	datadriven.RunTest(t, "testdata/procedure", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec":
			r.Exec(t, d.Input)
			return ""

		default:
			t.Fatalf("unsupported command %s", d.Cmd)
			return ""
		}
	})
}
