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
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
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

// MockCursorExec implements CursorExec interface for testing
type MockCursorExec struct {
	nextRowCalled     int
	nextRowReturnRow  sqlbase.EncDatumRow
	nextRowReturnMeta *execinfrapb.ProducerMetadata
	runClearUpCalled  bool
	datumAlloc        *sqlbase.DatumAlloc
}

func (m *MockCursorExec) RunClearUp() {
	m.runClearUpCalled = true
}

func (m *MockCursorExec) NextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	m.nextRowCalled++
	return m.nextRowReturnRow, m.nextRowReturnMeta
}

func (m *MockCursorExec) GetDatumAlloc() *sqlbase.DatumAlloc {
	if m.datumAlloc == nil {
		m.datumAlloc = &sqlbase.DatumAlloc{}
	}
	return m.datumAlloc
}

func testStartPlanFn(
	r RunParam, h CursorExec, rc *rowcontainer.RowContainer, s tree.StatementType, p Plan,
) error {
	return nil
}

func TestCursor_Start(t *testing.T) {
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
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Create a cursor
	cursor := &Cursor{
		isOpen:     false,
		execHelper: &MockCursorExec{},
	}

	rCtx := &SpExecContext{
		Fn:          testProcedurePlanFn,
		GetResultFn: testGetResultCallBKFn,
		RunPlanFn:   testRunPlanCallBKFn,
	}
	rCtx.Init()

	// Set up mock functions
	rCtx.StartPlanFn = testStartPlanFn

	err := cursor.Start(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, cursor.isOpen)
}

func TestCursor_Next(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	// Create a cursor with mock exec helper
	mockExec := &MockCursorExec{}
	cursor := &Cursor{
		isOpen:     true,
		execHelper: mockExec,
		resultCols: sqlbase.ResultColumns{
			{Typ: types.Int},
		},
	}

	// Test with no more data
	result, err := cursor.Next(runParam)
	require.NoError(t, err)
	require.Empty(t, result)

	// Test with data
	datumAlloc := &sqlbase.DatumAlloc{}
	encdatum := sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))
	mockExec.nextRowReturnRow = sqlbase.EncDatumRow{encdatum}
	mockExec.datumAlloc = datumAlloc

	result, err = cursor.Next(runParam)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestCursor_Close(t *testing.T) {
	mockExec := &MockCursorExec{}
	cursor := &Cursor{
		isOpen:     true,
		execHelper: mockExec,
	}

	cursor.Close()
	require.False(t, cursor.isOpen)
	require.True(t, mockExec.runClearUpCalled)
}

func TestCursor_Clear(t *testing.T) {
	mockExec := &MockCursorExec{}
	cursor := &Cursor{
		isOpen:     true,
		execHelper: mockExec,
	}

	cursor.Clear()
	require.False(t, cursor.isOpen)
	require.True(t, mockExec.runClearUpCalled)
}

func TestSetCursorIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

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

	var cursorStruct MockCursorExec
	cursor := NewCursor("SELECT a, b FROM abc WHERE c='foo'", o.Memo().RootExpr().(memo.RelExpr), tree.Rows, o.Memo().RootProps(), &cursorStruct)
	//ins := NewSetCursorIns("testCursor", *cur)
	setCursorIns := &SetCursorIns{
		name: "testCursor",
		cur:  *cursor,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	err = setCursorIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.NotNil(t, rCtx.FindCursor("testCursor"))

	// Test with invalid cursor type
	setCursorIns.cur.typ = tree.RowsAffected
	err = setCursorIns.Execute(runParam, rCtx)
	require.Error(t, err)
}

func TestSetCursorIns_Close(t *testing.T) {
	setCursorIns := &SetCursorIns{}
	setCursorIns.Close()
	// Just ensure no panic
}

func TestOpenCursorIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	rCtx := &SpExecContext{
		Fn:          testProcedurePlanFn,
		GetResultFn: testGetResultCallBKFn,
		RunPlanFn:   testRunPlanCallBKFn,
	}
	rCtx.Init()

	// Create and add a cursor
	cursor := &Cursor{
		isOpen:     false,
		execHelper: &MockCursorExec{},
	}
	rCtx.AddCursor("testCursor", &CursorHelper{CursorPtr: cursor})

	// Set up mock function
	rCtx.StartPlanFn = testStartPlanFn

	// Test opening the cursor
	openCursorIns := &OpenCursorIns{
		name: "testCursor",
	}

	err := openCursorIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, cursor.isOpen)

	// Test opening an already open cursor
	err = openCursorIns.Execute(runParam, rCtx)
	require.Error(t, err)
}

func TestOpenCursorIns_Close(t *testing.T) {
	openCursorIns := &OpenCursorIns{}
	openCursorIns.Close()
	// Just ensure no panic
}

func TestFetchCursorIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx:     ctx,
		evalCtx: &tree.EvalContext{},
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create and add a cursor
	datumAlloc := &sqlbase.DatumAlloc{}
	encdatum := sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(42))

	mockExec := &MockCursorExec{
		nextRowReturnRow: sqlbase.EncDatumRow{encdatum},
		datumAlloc:        datumAlloc,
	}

	cursor := &Cursor{
		isOpen:     true,
		execHelper: mockExec,
		resultCols: sqlbase.ResultColumns{
			{Typ: types.Int},
		},
	}
	rCtx.AddCursor("testCursor", &CursorHelper{CursorPtr: cursor})

	// Add a variable to store the result
	var1 := &exec.LocalVariable{Name: "var1", Typ: *types.Int}
	rCtx.AddVariable(var1)

	// Test fetching from cursor
	fetchCursorIns := &FetchCursorIns{
		name:      "testCursor",
		dstVarIdx: []int{0},
	}

	err := fetchCursorIns.Execute(runParam, rCtx)
	require.NoError(t, err)
}

func TestFetchCursorIns_Close(t *testing.T) {
	fetchCursorIns := &FetchCursorIns{}
	fetchCursorIns.Close()
	// Just ensure no panic
}

func TestCloseCursorIns_Execute(t *testing.T) {
	runParam := &MockRunParam{}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create and add a cursor
	mockExec := &MockCursorExec{}
	cursor := &Cursor{
		isOpen:     true,
		execHelper: mockExec,
	}
	rCtx.AddCursor("testCursor", &CursorHelper{CursorPtr: cursor})

	// Test closing the cursor
	closeCursorIns := &CloseCursorIns{
		name: "testCursor",
	}

	err := closeCursorIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.False(t, cursor.isOpen)

	// Test closing an already closed cursor
	err = closeCursorIns.Execute(runParam, rCtx)
	require.Error(t, err)
}

func TestCloseCursorIns_Close(t *testing.T) {
	closeCursorIns := &CloseCursorIns{}
	closeCursorIns.Close()
	// Just ensure no panic
}