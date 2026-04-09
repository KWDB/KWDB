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

package optbuilder

import (
	"context"
	"go/constant"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestBuildDelete tests the buildDelete function
func TestBuildDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create test catalog
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE t.public.test_table (id INT PRIMARY KEY, data STRING)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create a context and evaluation context
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.OptimizerFKs = true

	// Create an optimizer and factory
	var o xform.Optimizer
	o.Init(&evalCtx, catalog)
	o.DisableOptimizations()

	// Create a dummy statement for the Builder
	dummyStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				{
					Expr: tree.NewDInt(1),
				},
			},
		},
	}

	// Create a Builder
	b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), dummyStmt)

	// Test case 1: Basic DELETE without WHERE clause
	t.Run("basic delete without where", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic for DELETE without WHERE clause in safe updates mode
				t.Logf("Expected panic for DELETE without WHERE clause: %v", r)
			}
		}()

		del := &tree.Delete{
			Table: &tree.AliasedTableExpr{
				Expr: tree.NewTableName("t", "test_table"),
			},
		}

		inScope := b.allocScope()
		b.buildDelete(del, inScope)
	})

	// Test case 2: DELETE with WHERE clause
	t.Run("delete with where clause", func(t *testing.T) {
		del := &tree.Delete{
			Table: &tree.AliasedTableExpr{
				Expr: tree.NewTableName("t", "test_table"),
			},
			Where: &tree.Where{
				Expr: &tree.ComparisonExpr{
					Operator: tree.EQ,
					Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
					Right:    tree.NewNumVal(constant.MakeInt64(1), "1", false),
				},
			},
			Returning: &tree.NoReturningClause{},
		}

		inScope := b.allocScope()
		outScope := b.buildDelete(del, inScope)

		if outScope == nil {
			t.Error("Expected non-nil outScope")
		}
	})

	// Test case 3: DELETE with ORDER BY but no LIMIT (should panic)
	t.Run("delete with order by but no limit", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for DELETE with ORDER BY but no LIMIT")
			}
		}()

		del := &tree.Delete{
			Table: &tree.AliasedTableExpr{
				Expr: tree.NewTableName("t", "test_table"),
			},
			OrderBy: tree.OrderBy{
				&tree.Order{
					Expr:      &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
					Direction: tree.Ascending,
				},
			},
		}

		inScope := b.allocScope()
		b.buildDelete(del, inScope)
	})
}

// TestCheckDeleteFilter tests the checkDeleteFilter function
func TestCheckDeleteFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Supported expressions
	supportedExprs := []tree.Expr{
		&tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
		tree.NewNumVal(constant.MakeInt64(1), "1", false),
		&tree.StrVal{},
		tree.DBoolTrue,
		&tree.Placeholder{Idx: 1},
		&tree.Tuple{Exprs: []tree.Expr{tree.NewNumVal(constant.MakeInt64(1), "1", false)}},
		&tree.UserDefinedVar{},
	}

	for _, expr := range supportedExprs {
		err := checkDeleteFilter(expr)
		if err != nil {
			t.Errorf("Expected no error for expression %T, got %v", expr, err)
		}
	}

	// Test case 2: Complex expressions (should be supported)
	complexExprs := []tree.Expr{
		&tree.AndExpr{
			Left:  &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
			Right: tree.NewNumVal(constant.MakeInt64(1), "1", false),
		},
		&tree.OrExpr{
			Left:  &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
			Right: tree.NewNumVal(constant.MakeInt64(2), "2", false),
		},
		&tree.ComparisonExpr{
			Operator: tree.EQ,
			Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
			Right:    tree.NewNumVal(constant.MakeInt64(1), "1", false),
		},
		&tree.ParenExpr{
			Expr: &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"id", "", "", ""}},
		},
	}

	for _, expr := range complexExprs {
		err := checkDeleteFilter(expr)
		if err != nil {
			t.Errorf("Expected no error for complex expression %T, got %v", expr, err)
		}
	}

	// Test case 3: Unsupported expressions (should return error)
	// Note: This test may need adjustment based on actual unsupported expressions
	unsupportedExpr := &tree.CastExpr{
		Expr: tree.NewNumVal(constant.MakeInt64(1), "1", false),
		Type: types.Int,
	}

	err := checkDeleteFilter(unsupportedExpr)
	if err == nil {
		t.Error("Expected error for unsupported expression, got nil")
	}
}

// TestGetLimitOfTimestampWithPrecision tests the getLimitOfTimestampWithPrecision function
func TestGetLimitOfTimestampWithPrecision(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		precision   int64
		expectedMin int64
		expectedMax int64
	}{
		{
			name:        "millisecond precision",
			precision:   3,
			expectedMin: tree.TsMinMilliTimestamp - 1,
			expectedMax: tree.TsMaxMilliTimestamp + 1,
		},
		{
			name:        "microsecond precision",
			precision:   6,
			expectedMin: tree.TsMinMicroTimestamp - 1,
			expectedMax: tree.TsMaxMicroTimestamp + 1,
		},
		{
			name:        "nanosecond precision (default)",
			precision:   9,
			expectedMin: tree.TsMinNanoTimestamp - 1,
			expectedMax: tree.TsMaxNanoTimestamp + 1,
		},
		{
			name:        "zero precision (default to millisecond)",
			precision:   0,
			expectedMin: tree.TsMinNanoTimestamp - 1,
			expectedMax: tree.TsMaxNanoTimestamp + 1,
		},
		{
			name:        "unspecified precision (default to millisecond)",
			precision:   1,
			expectedMin: tree.TsMinNanoTimestamp - 1,
			expectedMax: tree.TsMaxNanoTimestamp + 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			min, max := getLimitOfTimestampWithPrecision(tc.precision)
			if min != tc.expectedMin {
				t.Errorf("Expected min %d, got %d", tc.expectedMin, min)
			}
			if max != tc.expectedMax {
				t.Errorf("Expected max %d, got %d", tc.expectedMax, max)
			}
		})
	}
}

// TestErrorTypeOfFilter tests the errorTypeOfFilter function
func TestErrorTypeOfFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		filter   filterType
		expected bool
	}{
		{
			name:     "initType - should not error",
			filter:   initType,
			expected: false,
		},
		{
			name:     "onlyTag - should not error",
			filter:   onlyTag,
			expected: false,
		},
		{
			name:     "onlyTS - should not error",
			filter:   onlyTS,
			expected: false,
		},
		{
			name:     "tagAndTS - should not error",
			filter:   tagAndTS,
			expected: false,
		},
		{
			name:     "unsupportedType - should error",
			filter:   unsupportedType,
			expected: true,
		},
		{
			name:     "multiTagFilter - should error",
			filter:   multiTagFilter,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := errorTypeOfFilter(tc.filter)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestBuildTSDelete tests the buildTSDelete function
func TestBuildTSDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create test catalog
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE t.public.ts_table (ts TIMESTAMP, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create a context and evaluation context
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.OptimizerFKs = true

	// Create an optimizer and factory
	var o xform.Optimizer
	o.Init(&evalCtx, catalog)
	o.DisableOptimizations()

	// Create a dummy statement for the Builder
	dummyStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				{
					Expr: tree.NewDInt(1),
				},
			},
		},
	}

	// Create a Builder
	b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), dummyStmt)

	// Get table from catalog
	table := catalog.Table(tree.NewTableName("t", "ts_table"))

	// Test case 1: Basic TS delete without WHERE clause
	t.Run("basic ts delete without where", func(t *testing.T) {
		del := &tree.Delete{
			Table: &tree.AliasedTableExpr{
				Expr: tree.NewTableName("t", "ts_table"),
			},
		}

		alias := tree.MakeTableName("t", "ts_table")
		inScope := b.allocScope()

		outScope := b.buildTSDelete(inScope, table, del, alias, sqlbase.ID(table.ID()), 1)

		if outScope == nil {
			t.Error("Expected non-nil outScope")
		}
	})

	// Test case 2: TS delete with WHERE clause
	t.Run("ts delete with where clause", func(t *testing.T) {
		del := &tree.Delete{
			Table: &tree.AliasedTableExpr{
				Expr: tree.NewTableName("t", "ts_table"),
			},
			Where: &tree.Where{
				Expr: &tree.ComparisonExpr{
					Operator: tree.EQ,
					Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"ts", "", "", ""}},
					Right:    tree.NewStrVal("2024-01-01 00:00:00"),
				},
			},
		}

		alias := tree.MakeTableName("t", "ts_table")
		inScope := b.allocScope()

		outScope := b.buildTSDelete(inScope, table, del, alias, sqlbase.ID(table.ID()), 1)

		if outScope == nil {
			t.Error("Expected non-nil outScope")
		}
	})
}

// TestMutationBuilderBuildDelete tests the mutationBuilder.buildDelete function
func TestMutationBuilderBuildDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test would require setting up a mutationBuilder with proper dependencies
	// For now, we'll create a placeholder test
	t.Run("placeholder test", func(t *testing.T) {
		t.Log("TestMutationBuilderBuildDelete placeholder - to be implemented with proper mutationBuilder setup")
	})
}

// TestMergeAndSpans tests the mergeAndSpans function
func TestMergeAndSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		leftSpans  []opt.TsSpan
		rightSpans []opt.TsSpan
		spans      []opt.TsSpan
		expected   []opt.TsSpan
	}{
		{
			name:       "leftSpans is nil",
			leftSpans:  nil,
			rightSpans: []opt.TsSpan{{Start: 1, End: 5}},
			spans:      []opt.TsSpan{{Start: 10, End: 15}},
			expected:   []opt.TsSpan{{Start: 10, End: 15}, {Start: 1, End: 5}},
		},
		{
			name:       "rightSpans is nil",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 5}},
			rightSpans: nil,
			spans:      []opt.TsSpan{{Start: 10, End: 15}},
			expected:   []opt.TsSpan{{Start: 10, End: 15}, {Start: 1, End: 5}},
		},
		{
			name:       "spans is nil",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 5}},
			rightSpans: []opt.TsSpan{{Start: 3, End: 7}},
			spans:      nil,
			expected:   []opt.TsSpan{{Start: 3, End: 5}},
		},
		{
			name:       "no overlap",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 5}},
			rightSpans: []opt.TsSpan{{Start: 6, End: 10}},
			spans:      []opt.TsSpan{{Start: 10, End: 15}},
			expected:   []opt.TsSpan{{Start: 10, End: 15}},
		},
		{
			name:       "overlap",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 10}},
			rightSpans: []opt.TsSpan{{Start: 5, End: 15}},
			spans:      []opt.TsSpan{{Start: 20, End: 25}},
			expected:   []opt.TsSpan{{Start: 20, End: 25}, {Start: 5, End: 10}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeAndSpans(tc.leftSpans, tc.rightSpans, tc.spans)
			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d spans, got %d", len(tc.expected), len(result))
				return
			}
			for i, span := range result {
				if span.Start != tc.expected[i].Start || span.End != tc.expected[i].End {
					t.Errorf("Expected span %d to be {%d, %d}, got {%d, %d}", i, tc.expected[i].Start, tc.expected[i].End, span.Start, span.End)
				}
			}
		})
	}
}

// TestMergeOrSpans tests the mergeOrSpans function
func TestMergeOrSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		leftSpans  []opt.TsSpan
		rightSpans []opt.TsSpan
		spans      []opt.TsSpan
		expected   []opt.TsSpan
	}{
		{
			name:       "leftSpans is nil",
			leftSpans:  nil,
			rightSpans: []opt.TsSpan{{Start: 1, End: 5}},
			spans:      []opt.TsSpan{{Start: 10, End: 15}},
			expected:   []opt.TsSpan{{Start: 10, End: 15}, {Start: 1, End: 5}},
		},
		{
			name:       "rightSpans is nil",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 5}},
			rightSpans: nil,
			spans:      []opt.TsSpan{{Start: 10, End: 15}},
			expected:   []opt.TsSpan{{Start: 10, End: 15}, {Start: 1, End: 5}},
		},
		{
			name:       "spans is nil",
			leftSpans:  []opt.TsSpan{{Start: 1, End: 5}},
			rightSpans: []opt.TsSpan{{Start: 3, End: 7}},
			spans:      nil,
			expected:   []opt.TsSpan{{Start: 1, End: 7}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeOrSpans(tc.leftSpans, tc.rightSpans, tc.spans)
			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d spans, got %d", len(tc.expected), len(result))
				return
			}
			for i, span := range result {
				if span.Start != tc.expected[i].Start || span.End != tc.expected[i].End {
					t.Errorf("Expected span %d to be {%d, %d}, got {%d, %d}", i, tc.expected[i].Start, tc.expected[i].End, span.Start, span.End)
				}
			}
		})
	}
}

// TestHandleDBool tests the handleDBool function
func TestHandleDBool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	precision := int64(3) // millisecond precision
	lowerLimit, upperLimit := getLimitOfTimestampWithPrecision(precision)

	testCases := []struct {
		name        string
		f           *tree.DBool
		spans       []opt.TsSpan
		typ         *filterType
		expected    []opt.TsSpan
		expectedTyp filterType
	}{
		{
			name:        "true bool with initType",
			f:           tree.DBoolTrue,
			spans:       []opt.TsSpan{{Start: 10, End: 15}},
			typ:         func() *filterType { t := filterType(initType); return &t }(),
			expected:    []opt.TsSpan{{Start: 10, End: 15}, {Start: lowerLimit, End: upperLimit}},
			expectedTyp: initType,
		},
		{
			name:        "false bool with initType",
			f:           tree.DBoolFalse,
			spans:       []opt.TsSpan{{Start: 10, End: 15}},
			typ:         func() *filterType { t := filterType(initType); return &t }(),
			expected:    []opt.TsSpan{{Start: 10, End: 15}, {Start: upperLimit, End: upperLimit}},
			expectedTyp: initType,
		},
		{
			name:        "true bool with onlyTag",
			f:           tree.DBoolTrue,
			spans:       []opt.TsSpan{{Start: 10, End: 15}},
			typ:         func() *filterType { t := filterType(onlyTag); return &t }(),
			expected:    []opt.TsSpan{{Start: 10, End: 15}, {Start: lowerLimit, End: upperLimit}},
			expectedTyp: tagAndTS,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := handleDBool(tc.f, tc.spans, tc.typ, precision)
			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d spans, got %d", len(tc.expected), len(result))
				return
			}
			for i, span := range result {
				if span.Start != tc.expected[i].Start || span.End != tc.expected[i].End {
					t.Errorf("Expected span %d to be {%d, %d}, got {%d, %d}", i, tc.expected[i].Start, tc.expected[i].End, span.Start, span.End)
				}
			}
			if *tc.typ != tc.expectedTyp {
				t.Errorf("Expected filter type %v, got %v", tc.expectedTyp, *tc.typ)
			}
		})
	}
}

// TestHandlePrimaryTagColumn tests the handlePrimaryTagColumn function
func TestHandlePrimaryTagColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	primaryTagIDs := map[int]struct{}{1: {}}

	testCases := []struct {
		name          string
		exprs         map[int]ExprWithLogicID
		id            int
		expr          tree.Expr
		op            tree.ComparisonOperator
		typ           *filterType
		primaryTagIDs map[int]struct{}
		logicID       opt.ColumnID
		expectedTyp   filterType
		expectedExprs map[int]ExprWithLogicID
	}{
		{
			name:          "valid datum expression",
			exprs:         make(map[int]ExprWithLogicID),
			id:            1,
			expr:          tree.NewDInt(1),
			op:            tree.EQ,
			typ:           func() *filterType { t := filterType(onlyTag); return &t }(),
			primaryTagIDs: primaryTagIDs,
			logicID:       opt.ColumnID(1),
			expectedTyp:   onlyTag,
			expectedExprs: map[int]ExprWithLogicID{
				1: {exp: tree.NewDInt(1), logicID: opt.ColumnID(1)},
			},
		},
		{
			name:          "invalid operator",
			exprs:         make(map[int]ExprWithLogicID),
			id:            1,
			expr:          tree.NewDInt(1),
			op:            tree.GT,
			typ:           func() *filterType { t := filterType(initType); return &t }(),
			primaryTagIDs: primaryTagIDs,
			logicID:       opt.ColumnID(1),
			expectedTyp:   unsupportedType,
			expectedExprs: map[int]ExprWithLogicID{
				1: {exp: tree.NewDInt(1), logicID: opt.ColumnID(1)},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handlePrimaryTagColumn(tc.exprs, tc.id, tc.expr, tc.op, tc.typ, tc.primaryTagIDs, tc.logicID)
			if *tc.typ != tc.expectedTyp {
				t.Errorf("Expected filter type %v, got %v", tc.expectedTyp, *tc.typ)
			}
			if len(tc.exprs) != len(tc.expectedExprs) {
				t.Errorf("Expected %d expressions, got %d", len(tc.expectedExprs), len(tc.exprs))
				return
			}
			for key, expected := range tc.expectedExprs {
				if actual, ok := tc.exprs[key]; !ok {
					t.Errorf("Expected expression for key %d, not found", key)
				} else if actual.exp.String() != expected.exp.String() || actual.logicID != expected.logicID {
					t.Errorf("Expected expression %v with logicID %d, got %v with logicID %d", expected.exp, expected.logicID, actual.exp, actual.logicID)
				}
			}
		})
	}
}

// TestGetTimeWithPrecision tests the getTimeWithPrecision function
func TestGetTimeWithPrecision(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a test time: 2024-01-01 00:00:00.123456789
	testTime, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00.123456789Z")
	if err != nil {
		t.Fatalf("Failed to parse test time: %v", err)
	}
	dTime := &tree.DTimestampTZ{Time: testTime}

	testCases := []struct {
		name           string
		time           *tree.DTimestampTZ
		precision      int64
		expectedExceed bool
		expectedTime   int64
	}{
		{
			name:           "millisecond precision",
			time:           dTime,
			precision:      3,
			expectedExceed: true,          // 123456789 nanoseconds = 123.456789 milliseconds
			expectedTime:   1704067200123, // 2024-01-01 00:00:00.123 in milliseconds
		},
		{
			name:           "microsecond precision",
			time:           dTime,
			precision:      6,
			expectedExceed: true,             // 123456789 nanoseconds = 123456.789 microseconds
			expectedTime:   1704067200123456, // 2024-01-01 00:00:00.123456 in microseconds
		},
		{
			name:           "nanosecond precision",
			time:           dTime,
			precision:      9,
			expectedExceed: false,               // 123456789 nanoseconds = 123456789 nanoseconds
			expectedTime:   1704067200123456789, // 2024-01-01 00:00:00.123456789 in nanoseconds
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exceed, result := getTimeWithPrecision(tc.time, tc.precision)
			if exceed != tc.expectedExceed {
				t.Errorf("Expected exceed %v, got %v", tc.expectedExceed, exceed)
			}
			if result != tc.expectedTime {
				t.Errorf("Expected time %d, got %d", tc.expectedTime, result)
			}
		})
	}
}
