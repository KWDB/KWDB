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

package procedure

import (
	"context"
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	opttestutils "gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

// MockTxn implements transaction interface for testing
type MockTxn struct {
	isOpen bool
}

func (m *MockTxn) IsOpen() bool {
	return m.isOpen
}

func (m *MockTxn) Step(ctx context.Context, forceCommit bool) error {
	return nil
}

func (m *MockTxn) CommitOrCleanup(ctx context.Context) error {
	m.isOpen = false
	return nil
}

func (m *MockTxn) Rollback(ctx context.Context) error {
	m.isOpen = false
	return nil
}

func TestHandlerHelper_ExecHandler(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create handler with action
	action := &MockInstruction{}
	handler := &HandlerHelper{
		Action: action,
	}

	// Test with ModeContinue
	err := handler.ExecHandler(runParam, rCtx, tree.ModeContinue)
	require.NoError(t, err)
	require.True(t, action.executeCalled)

	// Test with ModeExit
	action.executeCalled = false
	err = handler.ExecHandler(runParam, rCtx, tree.ModeExit)
	require.NoError(t, err)
	require.True(t, action.executeCalled)
}

func TestDealWithHandleByTypeAndMode(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create handler
	handler := &HandlerHelper{
		Typ: tree.NOTFOUND,
		Action: &MockInstruction{},
	}
	rCtx.SetHandler(tree.ModeContinue, handler)

	// Test with matching type
	flag, err := dealWithHandleByTypeAndMode(runParam, rCtx, tree.NOTFOUND, tree.ModeContinue)
	require.NoError(t, err)
	require.True(t, flag)

	// Test with non-matching type
	flag, err = dealWithHandleByTypeAndMode(runParam, rCtx, tree.SQLEXCEPTION, tree.ModeContinue)
	require.NoError(t, err)
	require.False(t, flag)
}

func TestDealWithHandleByType(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create handlers
	continueHandler := &HandlerHelper{
		Typ: tree.NOTFOUND,
		Action: &MockInstruction{},
	}
	exitHandler := &HandlerHelper{
		Typ: tree.NOTFOUND,
		Action: &MockInstruction{},
	}
	rCtx.SetHandler(tree.ModeContinue, continueHandler)
	rCtx.SetHandler(tree.ModeExit, exitHandler)

	// Test with matching type
	continueIsExec, exitIsExec, err := dealWithHandleByType(runParam, rCtx, tree.NOTFOUND)
	require.NoError(t, err)
	require.True(t, continueIsExec)
	require.True(t, exitIsExec)
}

func TestGetNewContainer(t *testing.T) {
	// Create monitor
	// These monitors are started and stopped by subtests.
	st := cluster.MakeTestingClusterSettings()
	bytesMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	// Create result columns
	cols := sqlbase.ResultColumns{
		{Typ: types.Int},
		{Typ: types.String},
	}

	// Test getNewContainer
	container := getNewContainer(&bytesMonitor, cols)
	require.NotNil(t, container)
	container.Close(context.Background())
}

func testProcedurePlanFn(
	expr memo.RelExpr, pr *physical.Required, src []*exec.LocalVariable,
) (exec.Plan, error) {
	return expr, nil
}

func testGetResultCallBKFn(p Plan) (Plan, sqlbase.ResultColumns) {
	var resultCols sqlbase.ResultColumns
	resultCols = append(resultCols, sqlbase.ResultColumn{
		Name:           "t",
		Typ:            types.Int,
		Hidden:         false,
		TableID:        0,
		PGAttributeNum: 1,
		TypeModifier:   types.Int.TypeModifier(),
	})
	return p, resultCols
}

func testRunPlanCallBKFn(
	r RunParam, p Plan, rc *rowcontainer.RowContainer, s tree.StatementType,
) (int, error) {
	rc.AddRow(r.GetCtx(), tree.Datums{tree.NewDInt(1)})
	return 1, nil
}

func TestStmtIns_Execute(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	monitor := mon.MakeMonitor(
		"test-monitor",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer monitor.Stop(ctx)

	runParam := &MockRunParam{
		ctx: ctx,
		evalCtx: &tree.EvalContext{
			Mon: &monitor,
		},
	}
	runParam.NewTxn()

	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	// Revoke access to the underlying table. The user should retain indirect
	// access via the view.
	// catalog.Table(tree.NewTableName("t", "abc")).Revoked = true

	// Initialize context with starting values.
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.Database = "t"

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a, b FROM abc WHERE c='foo'")
	o.Memo().Metadata().AddSchema(catalog.Schema())

	rCtx := &SpExecContext{
		Fn:          testProcedurePlanFn,
		GetResultFn: testGetResultCallBKFn,
		RunPlanFn:   testRunPlanCallBKFn,
	}
	rCtx.Init()

	// Create StmtIns with createDefault=true
	stmtIns := &StmtIns{
		stmt:          "select * from t",
		rootExpr:      o.Memo().RootExpr().(memo.RelExpr),
		pr:            o.Memo().RootProps(),
		createDefault: true,
		typ:           tree.Rows,
	}

	// Test execute with createDefault=true
	err = stmtIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.False(t, stmtIns.createDefault)

	// TODO: Test with createDefault=false (requires more complex setup)
	stmtIns.createDefault = false
	err = stmtIns.Execute(runParam, rCtx)
	stmtIns.Close()
	rCtx.Close(ctx)
	require.NoError(t, err)
}

func TestStmtIns_Close(t *testing.T) {
	stmtIns := &StmtIns{}
	stmtIns.Close()
	// Just ensure no panic
}

func TestTransactionIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
		evalCtx: &tree.EvalContext{
			TxnImplicit: true,
		},
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test START transaction
	transactionIns := &TransactionIns{
		TxnType: tree.ProcedureTransactionStart,
	}

	err := transactionIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.Equal(t, tree.ProcedureTransactionStart, rCtx.GetProcedureTxn())

	// Test COMMIT transaction
	transactionIns.TxnType = tree.ProcedureTransactionCommit
	err = transactionIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.Equal(t, tree.ProcedureTransactionDefault, rCtx.GetProcedureTxn())

	// Test ROLLBACK transaction
	transactionIns.TxnType = tree.ProcedureTransactionStart
	err = transactionIns.Execute(runParam, rCtx)
	require.NoError(t, err)

	transactionIns.TxnType = tree.ProcedureTransactionRollBack
	err = transactionIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.Equal(t, tree.ProcedureTransactionDefault, rCtx.GetProcedureTxn())

	// Test invalid transaction type
	transactionIns.TxnType = tree.ProcedureTxnState(999)
	err = transactionIns.Execute(runParam, rCtx)
	require.Error(t, err)
}

func TestTransactionIns_Close(t *testing.T) {
	transactionIns := &TransactionIns{}
	transactionIns.Close()
	// Just ensure no panic
}

func TestIntoIns_Execute(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	monitor := mon.MakeMonitor(
		"test-monitor",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	runParam := &MockRunParam{
		ctx: ctx,
		evalCtx: &tree.EvalContext{
			Mon: &monitor,
		},
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create IntoIns
	intoIns := &IntoIns{}

	// TODO: Test Execute (requires more complex setup)
	_ = intoIns.Execute(runParam, rCtx)
	// require.NoError(t, err)
}

func TestIntoIns_Close(t *testing.T) {
	intoIns := &IntoIns{}
	intoIns.Close()
	// Just ensure no panic
}

func TestCheckTransactionEnd(t *testing.T) {
	runParam := &MockRunParam{
		ctx: context.TODO(),
		evalCtx: &tree.EvalContext{
			TxnImplicit: true,
		},
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test with active transaction
	rCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
	err := checkTransactionEnd(runParam, rCtx)
	require.NoError(t, err)
}

// func TestDealImplicitTxn(t *testing.T) {
// 	ctx := context.Background()
// 	runParam := &MockRunParam{
// 		ctx: ctx,
// 	}

// 	rCtx := &SpExecContext{}
// 	rCtx.Init()

// 	// Test with implicit transaction flag false
// 	implicitTxnFlag := false
// 	err := dealImplicitTxn(runParam, rCtx, &implicitTxnFlag, true)
// 	require.NoError(t, err)

// 	// Test with implicit transaction flag true
// 	implicitTxnFlag = true
// 	rCtx.SetProcedureTxn(tree.ProcedureTransactionStart)
// 	err = dealImplicitTxn(runParam, rCtx, &implicitTxnFlag, true)
// 	require.NoError(t, err)
// 	require.False(t, implicitTxnFlag)
// 	require.Equal(t, tree.ProcedureTransactionDefault, rCtx.GetProcedureTxn())
// }