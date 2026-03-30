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
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestResult_AddQueryResult(t *testing.T) {
	result := &Result{}

	// Create a QueryResult
	queryResult := &QueryResult{
		ResultCols: sqlbase.ResultColumns{
			{Typ: types.Int},
		},
		Result: &rowcontainer.RowContainer{},
	}

	// Add the QueryResult
	result.AddQueryResult(queryResult)
	require.Len(t, result.res, 1)
	require.Equal(t, queryResult, result.res[0])
}

func TestResult_HasNextResult(t *testing.T) {
	result := &Result{}

	// Add some QueryResults
	queryResult1 := &QueryResult{StmtType: tree.Rows}
	queryResult2 := &QueryResult{StmtType: tree.Rows}
	result.AddQueryResult(queryResult1)
	result.AddQueryResult(queryResult2)

	// Test HasNextResult
	require.True(t, result.HasNextResult()) // Should find second result
	require.False(t, result.HasNextResult()) // Should return false after all results
}

func TestResult_CheckResultExist(t *testing.T) {
	result := &Result{}

	// Test with empty result
	require.False(t, result.CheckResultExist())

	// Add a QueryResult
	result.AddQueryResult(&QueryResult{})
	require.True(t, result.CheckResultExist())
}

func TestResult_GetNextResultCols(t *testing.T) {
	result := &Result{}

	// Add a QueryResult with columns
	expectedCols := sqlbase.ResultColumns{
		{Typ: types.Int},
		{Typ: types.String},
	}
	queryResult := &QueryResult{
		ResultCols: expectedCols,
	}
	result.AddQueryResult(queryResult)

	// Get the result columns
	cols := result.GetNextResultCols()
	require.Equal(t, expectedCols, cols)
}

func TestResult_GetResultTypeAndAffected(t *testing.T) {
	result := &Result{}

	// Add a QueryResult
	expectedType := tree.Rows
	expectedAffected := 5
	queryResult := &QueryResult{
		StmtType:     expectedType,
		RowsAffected: expectedAffected,
	}
	result.AddQueryResult(queryResult)

	var stmtType tree.StatementType
	var affected int
	// Get the result type and affected rows
	stmtType, affected = result.GetResultTypeAndAffected()
	require.Equal(t, expectedType, stmtType)
	require.Equal(t, expectedAffected, affected)
}

func TestResult_Next(t *testing.T) {
	result := &Result{}

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
	ctx := context.TODO()
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer monitor.Stop(ctx)
	
	// Add a QueryResult with a row container
	// datumAlloc := &sqlbase.DatumAlloc{}
	rowContainer := rowcontainer.NewRowContainer(monitor.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(sqlbase.ResultColumns{{Typ: types.Int}}), 1)
	rowContainer.AddRow(ctx, tree.Datums{tree.NewDInt(42)})
	defer rowContainer.Close(ctx)

	queryResult := &QueryResult{
		Result: rowContainer,
	}
	result.AddQueryResult(queryResult)

	// Test Next()
	hasNext, err := result.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Test Next() when no more rows
	hasNext, err = result.Next()
	require.NoError(t, err)
	require.False(t, hasNext)
}

func TestResult_Values(t *testing.T) {
	result := &Result{}

	// Add a QueryResult with a row container
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
	ctx := context.TODO()
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer monitor.Stop(ctx)

	rowContainer := rowcontainer.NewRowContainer(monitor.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(sqlbase.ResultColumns{{Typ: types.Int}}), 1)
	expectedRow := tree.Datums{tree.NewDInt(42)}
	rowContainer.AddRow(ctx, expectedRow)
	defer rowContainer.Close(ctx)

	queryResult := &QueryResult{
		Result: rowContainer,
	}
	result.AddQueryResult(queryResult)

	// Test Values()
	result.Next()
	values := result.Values()
	require.Equal(t, expectedRow, values)
}

func TestResult_Close(t *testing.T) {
	result := &Result{}

	// Add a QueryResult with a row container
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
	ctx := context.TODO()
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer monitor.Stop(ctx)

	rowContainer := rowcontainer.NewRowContainer(monitor.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(sqlbase.ResultColumns{{Typ: types.Int}}), 1)
	defer rowContainer.Close(ctx)

	queryResult := &QueryResult{
		Result: rowContainer,
	}
	result.AddQueryResult(queryResult)

	// Test Close()
	result.Close(context.Background())
	// Just ensure no panic
}

func TestReturnHelper_AddReturnLabel(t *testing.T) {
	returnHelper := &ReturnHelper{}

	// Add a return label
	returnHelper.AddReturnLabel("testLabel")
	require.True(t, returnHelper.retFlag)
	require.Equal(t, "testLabel", returnHelper.labelName)
}

func TestReturnHelper_AddReturnHandler(t *testing.T) {
	returnHelper := &ReturnHelper{}

	// Add a return handler
	handler := &HandlerHelper{}
	returnHelper.AddReturnHandler(handler)
	require.Equal(t, handler, returnHelper.handler)
}

func TestReturnHelper_RemoveReturn(t *testing.T) {
	returnHelper := &ReturnHelper{}

	// Add a return label and then remove it
	returnHelper.AddReturnLabel("testLabel")
	require.True(t, returnHelper.retFlag)

	returnHelper.RemoveReturn()
	require.False(t, returnHelper.retFlag)
	require.Empty(t, returnHelper.labelName)
}

func TestReturnHelper_NeedReturn(t *testing.T) {
	returnHelper := &ReturnHelper{}

	// Test when no return is needed
	require.False(t, returnHelper.NeedReturn())

	// Test when return is needed
	returnHelper.AddReturnLabel("testLabel")
	require.True(t, returnHelper.NeedReturn())
}

func TestHandlerHelper_checkFlag(t *testing.T) {
	handlerHelper := &HandlerHelper{
		Typ: tree.NOTFOUND | tree.SQLEXCEPTION,
	}

	// Test checking for NOT FOUND flag
	require.True(t, handlerHelper.checkFlag(tree.NOTFOUND))

	// Test checking for SQL EXCEPTION flag
	require.True(t, handlerHelper.checkFlag(tree.SQLEXCEPTION))
}