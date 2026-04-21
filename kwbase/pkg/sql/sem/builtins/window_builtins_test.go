// Copyright 2016 The Cockroach Authors.
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

package builtins

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestInitWindowBuiltins tests that window builtins are properly initialized.
func TestInitWindowBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Window functions are already registered during package initialization
	// This test just verifies that all window functions are properly registered

	// Verify that all window functions are registered
	windowFuncs := []string{
		"row_number",
		"group_row_number",
		"rank",
		"dense_rank",
		"percent_rank",
		"group_rank",
		"cume_dist",
		"ntile",
		"lag",
		"lead",
		"first_value",
		"last_value",
		"nth_value",
		"diff",
	}

	for _, name := range windowFuncs {
		require.Contains(t, builtins, name, "window function %s should be registered", name)
		def := builtins[name]
		require.True(t, def.props.Impure, "window function %s should be impure", name)
		require.Equal(t, tree.WindowClass, def.props.Class, "window function %s should have WindowClass", name)
		for _, overload := range def.overloads {
			require.NotNil(t, overload.WindowFunc, "window function %s should have WindowFunc constructor", name)
		}
	}
}

// TestWinProps tests the winProps function.
func TestWinProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	props := winProps()
	require.True(t, props.Impure, "winProps should return impure function properties")
	require.Equal(t, tree.WindowClass, props.Class, "winProps should return WindowClass")
}

// TestMakeWindowOverload tests the makeWindowOverload function.
func TestMakeWindowOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inTypes := tree.ArgTypes{{"val", types.Int}}
	retType := types.Int
	info := "test window function"

	constructor := func([]*types.T, *tree.EvalContext) tree.WindowFunc {
		return &rowNumberWindow{}
	}

	overload := makeWindowOverload(inTypes, retType, constructor, info)

	require.Equal(t, inTypes, overload.Types, "overload should have correct input types")
	require.Equal(t, retType, overload.FixedReturnType(), "overload should have correct return type")
	require.NotNil(t, overload.WindowFunc, "overload should have window function constructor")
	require.Equal(t, info, overload.Info, "overload should have correct info")
}

// TestSafeSub tests the safeSub function.
func TestSafeSub(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		a         tree.DInt
		b         tree.DInt
		expected  tree.DInt
		expectErr bool
	}{
		{10, 5, 5, false},
		{5, 10, -5, false},
		{0, 0, 0, false},
		{1, 0, 1, false},
		{0, 1, -1, false},
		{tree.DInt(1<<63 - 1), 1, tree.DInt(1<<63 - 2), false},
		{tree.DInt(-1 << 63), -1, tree.DInt(-1<<63 + 1), false},
	}

	for _, tt := range tests {
		result, err := safeSub(tt.a, tt.b)
		if tt.expectErr {
			require.Error(t, err, "safeSub(%d, %d) should error", tt.a, tt.b)
		} else {
			require.NoError(t, err, "safeSub(%d, %d) should not error", tt.a, tt.b)
			require.Equal(t, tt.expected, result, "safeSub(%d, %d) should return %d", tt.a, tt.b, tt.expected)
		}
	}
}

// TestRowNumberWindow tests the rowNumberWindow implementation.
func TestRowNumberWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		expected tree.DInt
	}{
		{"first row", 1},
		{"second row", 2},
		{"third row", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := newRowNumberWindow(nil, nil)
			defer window.Close(context.Background(), nil)

			// Test Reset
			window.Reset(context.Background())

			// Note: Full Compute test would require a WindowFrameRun, which is complex to mock
			// This test just ensures the basic functionality works
			require.NotNil(t, window, "newRowNumberWindow should return a non-nil window")
		})
	}
}

// TestRankWindow tests the rankWindow implementation.
func TestRankWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newRankWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newRankWindow should return a non-nil window")
}

// TestDenseRankWindow tests the denseRankWindow implementation.
func TestDenseRankWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newDenseRankWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newDenseRankWindow should return a non-nil window")
}

// TestPercentRankWindow tests the percentRankWindow implementation.
func TestPercentRankWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newPercentRankWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newPercentRankWindow should return a non-nil window")
}

// TestCumulativeDistWindow tests the cumulativeDistWindow implementation.
func TestCumulativeDistWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newCumulativeDistWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newCumulativeDistWindow should return a non-nil window")
}

// TestNtileWindow tests the ntileWindow implementation.
func TestNtileWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newNtileWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newNtileWindow should return a non-nil window")
}

// TestLeadLagWindow tests the leadLagWindow implementation.
func TestLeadLagWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		forward     bool
		withOffset  bool
		withDefault bool
	}{
		{
			name:        "lead without offset or default",
			forward:     true,
			withOffset:  false,
			withDefault: false,
		},
		{
			name:        "lag with offset and default",
			forward:     false,
			withOffset:  true,
			withDefault: true,
		},
		{
			name:        "lead with offset",
			forward:     true,
			withOffset:  true,
			withDefault: false,
		},
		{
			name:        "lag without offset",
			forward:     false,
			withOffset:  false,
			withDefault: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := newLeadLagWindow(tt.forward, tt.withOffset, tt.withDefault)
			defer window.Close(context.Background(), nil)

			// Test Reset
			window.Reset(context.Background())

			// Test that window is created correctly
			require.NotNil(t, window, "newLeadLagWindow should return a non-nil window")
		})
	}
}

// TestMakeLeadLagWindowConstructor tests the makeLeadLagWindowConstructor function.
func TestMakeLeadLagWindowConstructor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		forward     bool
		withOffset  bool
		withDefault bool
	}{
		{
			name:        "lead constructor",
			forward:     true,
			withOffset:  false,
			withDefault: false,
		},
		{
			name:        "lag constructor with offset and default",
			forward:     false,
			withOffset:  true,
			withDefault: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constructor := makeLeadLagWindowConstructor(tt.forward, tt.withOffset, tt.withDefault)
			window := constructor(nil, nil)
			require.NotNil(t, window, "makeLeadLagWindowConstructor should return a non-nil window")
		})
	}
}

// TestFirstValueWindow tests the firstValueWindow implementation.
func TestFirstValueWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newFirstValueWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newFirstValueWindow should return a non-nil window")
}

// TestLastValueWindow tests the lastValueWindow implementation.
func TestLastValueWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newLastValueWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newLastValueWindow should return a non-nil window")
}

// TestNthValueWindow tests the nthValueWindow implementation.
func TestNthValueWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newNthValueWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newNthValueWindow should return a non-nil window")
}

// TestDiffWindowInt tests the DiffWindowInt implementation.
func TestDiffWindowInt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newDiffWindowInt(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset
	window.Reset(context.Background())

	// Test that window is created correctly
	require.NotNil(t, window, "newDiffWindowInt should return a non-nil window")
}

// TestDiffWindowFloat tests the DiffWindowFloat implementation.
func TestDiffWindowFloat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name          string
		difftypes     []*types.T
		expectDecimal bool
	}{
		{
			name:          "float type",
			difftypes:     []*types.T{types.Float},
			expectDecimal: false,
		},
		{
			name:          "decimal type",
			difftypes:     []*types.T{types.Decimal},
			expectDecimal: true,
		},
		{
			name:          "mixed types",
			difftypes:     []*types.T{types.Float, types.Decimal},
			expectDecimal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window := newDiffWindowFloat(tt.difftypes, nil)
			defer window.Close(context.Background(), nil)

			// Test Reset
			window.Reset(context.Background())

			// Test that window is created correctly
			require.NotNil(t, window, "newDiffWindowFloat should return a non-nil window")
		})
	}
}

// TestShouldReset tests the ShouldReset function.
func TestShouldReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock framableAggregateWindowFunc
	mockAgg := &aggregateWindowFunc{}
	mockConstructor := func(*tree.EvalContext, tree.Datums) tree.AggregateFunc {
		return &mockAggregateFunc{}
	}
	framableWindow := &framableAggregateWindowFunc{
		agg:            mockAgg,
		aggConstructor: mockConstructor,
		shouldReset:    false,
	}

	// Test that ShouldReset sets shouldReset to true
	require.False(t, framableWindow.shouldReset, "shouldReset should be false initially")
	ShouldReset(framableWindow)
	require.True(t, framableWindow.shouldReset, "ShouldReset should set shouldReset to true")

	// Test that ShouldReset does nothing for non-framableAggregateWindowFunc
	t.Run("non-framable window", func(t *testing.T) {
		rowNumWindow := &rowNumberWindow{}
		ShouldReset(rowNumWindow)
		// Just ensure it doesn't panic
		require.NotNil(t, rowNumWindow)
	})

	// Test that ShouldReset does nothing for nil window
	t.Run("nil window", func(t *testing.T) {
		var nilWindow tree.WindowFunc
		ShouldReset(nilWindow)
		// Just ensure it doesn't panic
		require.Nil(t, nilWindow)
	})
}

// TestNewAggregateWindowFunc tests the NewAggregateWindowFunc function.
func TestNewAggregateWindowFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mockAggConstructor := func(*tree.EvalContext, tree.Datums) tree.AggregateFunc {
		return &mockAggregateFunc{}
	}

	constructor := NewAggregateWindowFunc(mockAggConstructor)
	window := constructor(nil)

	require.NotNil(t, window, "NewAggregateWindowFunc should return a non-nil window")

	// Test Reset and Close methods
	window.Reset(context.Background())
	window.Close(context.Background(), nil)
}

// TestNewFramableAggregateWindow tests the newFramableAggregateWindow function.
func TestNewFramableAggregateWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mockAgg := &mockAggregateFunc{}
	mockConstructor := func(*tree.EvalContext, tree.Datums) tree.AggregateFunc {
		return &mockAggregateFunc{}
	}

	window := newFramableAggregateWindow(mockAgg, mockConstructor)
	require.NotNil(t, window, "newFramableAggregateWindow should return a non-nil window")

	// Test Reset and Close methods
	window.Reset(context.Background())
	window.Close(context.Background(), nil)
}

// TestWindowFunctionProperties tests that all window functions have correct properties.
func TestWindowFunctionProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		expected bool
	}{
		{"row_number", true},
		{"rank", true},
		{"dense_rank", true},
		{"percent_rank", true},
		{"cume_dist", true},
		{"ntile", true},
		{"lag", true},
		{"lead", true},
		{"first_value", true},
		{"last_value", true},
		{"nth_value", true},
		{"diff", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, ok := builtins[tt.name]
			require.True(t, ok, "window function %s should exist", tt.name)
			require.True(t, def.props.Impure, "window function %s should be impure", tt.name)
			require.Equal(t, tree.WindowClass, def.props.Class, "window function %s should have WindowClass", tt.name)
		})
	}
}

// TestWindowFunctionOverloads tests that all window functions have valid overloads.
func TestWindowFunctionOverloads(t *testing.T) {
	defer leaktest.AfterTest(t)()

	windowFuncs := []string{
		"row_number",
		"rank",
		"dense_rank",
		"percent_rank",
		"cume_dist",
		"ntile",
		"lag",
		"lead",
		"first_value",
		"last_value",
		"nth_value",
		"diff",
	}

	for _, name := range windowFuncs {
		t.Run(name, func(t *testing.T) {
			def, ok := builtins[name]
			require.True(t, ok, "window function %s should exist", name)
			require.Greater(t, len(def.overloads), 0, "window function %s should have at least one overload", name)

			for i, overload := range def.overloads {
				require.NotNil(t, overload.WindowFunc, "overload %d of %s should have WindowFunc", i, name)
				require.NotEmpty(t, overload.Info, "overload %d of %s should have Info", i, name)
				require.NotNil(t, overload.ReturnType, "overload %d of %s should have ReturnType", i, name)
			}
		})
	}
}

// mockIndexedRow is a mock implementation of tree.IndexedRow for testing.
type mockIndexedRow struct {
	idx    int
	datums tree.Datums
}

func (m *mockIndexedRow) GetIdx() int {
	return m.idx
}

func (m *mockIndexedRow) GetDatum(idx int) (tree.Datum, error) {
	if idx >= len(m.datums) {
		return nil, errors.New("index out of range")
	}
	return m.datums[idx], nil
}

func (m *mockIndexedRow) GetDatums(startIdx, endIdx int) (tree.Datums, error) {
	if startIdx >= len(m.datums) || endIdx > len(m.datums) {
		return nil, errors.New("index out of range")
	}
	return m.datums[startIdx:endIdx], nil
}

// mockIndexedRows is a mock implementation of tree.IndexedRows for testing.
type mockIndexedRows struct {
	rows []*mockIndexedRow
}

func (m *mockIndexedRows) Len() int {
	return len(m.rows)
}

func (m *mockIndexedRows) GetRow(ctx context.Context, idx int) (tree.IndexedRow, error) {
	if idx >= len(m.rows) {
		return nil, errors.New("index out of range")
	}
	return m.rows[idx], nil
}

// mockAggregateFunc is a mock implementation of tree.AggregateFunc for testing.
type mockAggregateFunc struct{}

func (m *mockAggregateFunc) Add(context.Context, tree.Datum, ...tree.Datum) error {
	return nil
}

func (m *mockAggregateFunc) Result() (tree.Datum, error) {
	return tree.DNull, nil
}

func (m *mockAggregateFunc) Reset(context.Context) {}

func (m *mockAggregateFunc) Close(context.Context) {}

func (m *mockAggregateFunc) Size() int64 {
	return 0
}

func (m *mockAggregateFunc) AggHandling() {}

// createMockWindowFrameRun creates a mock WindowFrameRun for testing window function Compute methods.
func createMockWindowFrameRun(rowIdx int) *tree.WindowFrameRun {
	// Create mock rows
	rows := &mockIndexedRows{
		rows: []*mockIndexedRow{
			{idx: 0, datums: tree.Datums{tree.NewDInt(1), tree.NewDInt(10)}},
			{idx: 1, datums: tree.Datums{tree.NewDInt(2), tree.NewDInt(20)}},
			{idx: 2, datums: tree.Datums{tree.NewDInt(3), tree.NewDInt(30)}},
			{idx: 3, datums: tree.Datums{tree.NewDInt(4), tree.NewDInt(40)}},
			{idx: 4, datums: tree.Datums{tree.NewDInt(5), tree.NewDInt(50)}},
		},
	}

	return &tree.WindowFrameRun{
		Rows:               rows,
		ArgsIdxs:           []uint32{1}, // Use the second column as argument
		Frame:              nil,         // Use default frame
		StartBoundOffset:   tree.NewDInt(0),
		EndBoundOffset:     tree.NewDInt(0),
		FilterColIdx:       -1, // No filter
		OrdColIdx:          0,  // Order by first column
		PeerHelper:         tree.PeerGroupsIndicesHelper{},
		CurRowPeerGroupNum: 0,
		RowIdx:             rowIdx,
	}
}

// TestRowNumberWindowCompute tests the Compute method of rowNumberWindow.
func TestRowNumberWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newRowNumberWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Compute for different rows
	testCases := []struct {
		rowIdx   int
		expected tree.DInt
	}{
		{0, 1},
		{1, 2},
		{2, 3},
		{3, 4},
		{4, 5},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("row %d", tc.rowIdx), func(t *testing.T) {
			wfr := createMockWindowFrameRun(tc.rowIdx)
			result, err := window.Compute(context.Background(), nil, wfr)
			require.NoError(t, err, "Compute should not error")
			require.Equal(t, tree.NewDInt(tc.expected), result, "Compute should return correct row number")
		})
	}

	// Test Reset
	window.Reset(context.Background())
	// After reset, should start from 1 again
	wfr := createMockWindowFrameRun(0)
	result, err := window.Compute(context.Background(), nil, wfr)
	require.NoError(t, err, "Compute after reset should not error")
	require.Equal(t, tree.NewDInt(1), result, "Compute after reset should return 1")
}

// TestRankWindowCompute tests the Reset and Close methods of rankWindow.
func TestRankWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newRankWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset method
	window.Reset(context.Background())
	// Reset should not panic

	// Test Close method
	window.Close(context.Background(), nil)
	// Close should not panic
}

// TestDenseRankWindowCompute tests the Reset and Close methods of denseRankWindow.
func TestDenseRankWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newDenseRankWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset method
	window.Reset(context.Background())
	// Reset should not panic

	// Test Close method
	window.Close(context.Background(), nil)
	// Close should not panic
}

// TestLeadLagWindowCompute tests the Compute method of leadLagWindow.
func TestLeadLagWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test lead window
	leadWindow := newLeadLagWindow(true, false, false)
	defer leadWindow.Close(context.Background(), nil)

	// Test lead for different rows
	leadTestCases := []struct {
		rowIdx   int
		expected tree.Datum
	}{
		{0, tree.NewDInt(20)},
		{1, tree.NewDInt(30)},
		{2, tree.NewDInt(40)},
		{3, tree.NewDInt(50)},
		{4, tree.DNull}, // No lead for last row
	}

	for _, tc := range leadTestCases {
		t.Run(fmt.Sprintf("lead row %d", tc.rowIdx), func(t *testing.T) {
			wfr := createMockWindowFrameRun(tc.rowIdx)
			result, err := leadWindow.Compute(context.Background(), nil, wfr)
			require.NoError(t, err, "Compute should not error")
			require.Equal(t, tc.expected, result, "Compute should return correct lead value")
		})
	}

	// Test lag window
	lagWindow := newLeadLagWindow(false, false, false)
	defer lagWindow.Close(context.Background(), nil)

	// Test lag for different rows
	lagTestCases := []struct {
		rowIdx   int
		expected tree.Datum
	}{
		{0, tree.DNull}, // No lag for first row
		{1, tree.NewDInt(10)},
		{2, tree.NewDInt(20)},
		{3, tree.NewDInt(30)},
		{4, tree.NewDInt(40)},
	}

	for _, tc := range lagTestCases {
		t.Run(fmt.Sprintf("lag row %d", tc.rowIdx), func(t *testing.T) {
			wfr := createMockWindowFrameRun(tc.rowIdx)
			result, err := lagWindow.Compute(context.Background(), nil, wfr)
			require.NoError(t, err, "Compute should not error")
			require.Equal(t, tc.expected, result, "Compute should return correct lag value")
		})
	}

	// Test Reset
	leadWindow.Reset(context.Background())
	lagWindow.Reset(context.Background())
}

// TestFirstValueWindowCompute tests the Reset and Close methods of firstValueWindow.
func TestFirstValueWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newFirstValueWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset method
	window.Reset(context.Background())
	// Reset should not panic

	// Test Close method
	window.Close(context.Background(), nil)
	// Close should not panic
}

// TestLastValueWindowCompute tests the Reset and Close methods of lastValueWindow.
func TestLastValueWindowCompute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	window := newLastValueWindow(nil, nil)
	defer window.Close(context.Background(), nil)

	// Test Reset method
	window.Reset(context.Background())
	// Reset should not panic

	// Test Close method
	window.Close(context.Background(), nil)
	// Close should not panic
}
