// Copyright 2018 The Cockroach Authors.
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

package memo_test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	opttestutils "gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/opttester"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMemo(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/memo", flags)
}

func TestFormat(t *testing.T) {
	runDataDrivenTest(t, "testdata/format", memo.ExprFmtShowAll)
}

func TestLogicalProps(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideQualifications | memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/logprops/", flags)
}

func TestStats(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars
	runDataDrivenTest(t, "testdata/stats/", flags)
}

func TestStatsQuality(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideScalars
	runDataDrivenTest(t, "testdata/stats_quality/", flags)
}

func TestMemoInit(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE $1=10")

	o.Init(&evalCtx, catalog)
	if !o.Memo().IsEmpty() {
		t.Fatal("memo should be empty")
	}
	if o.Memo().MemoryEstimate() != 0 {
		t.Fatal("memory estimate should be 0")
	}
	if o.Memo().RootExpr() != nil {
		t.Fatal("root expression should be nil")
	}
	if o.Memo().RootProps() != nil {
		t.Fatal("root props should be nil")
	}
}

func TestMemoIsStale(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}

	// Revoke access to the underlying table. The user should retain indirect
	// access via the view.
	catalog.Table(tree.NewTableName("t", "abc")).Revoked = true

	// Initialize context with starting values.
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.Database = "t"

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a, b+1 FROM abcview WHERE c='foo'")
	o.Memo().Metadata().AddSchema(catalog.Schema())

	ctx := context.Background()
	stale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if !isStale {
			t.Errorf("memo should be stale")
		}

		// If we did not initialize the Memo's copy of a SessionData setting, the
		// tests as written still pass if the default value is 0. To detect this, we
		// create a new memo with the changed setting and verify it's not stale.
		var o2 xform.Optimizer
		opttestutils.BuildQuery(t, &o2, catalog, &evalCtx, "SELECT a, b+1 FROM abcview WHERE c='foo'")

		if isStale, err := o2.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale := func() {
		t.Helper()
		if isStale, err := o.Memo().IsStale(ctx, &evalCtx, catalog); err != nil {
			t.Fatal(err)
		} else if isStale {
			t.Errorf("memo should not be stale")
		}
	}

	notStale()

	// Stale location.
	evalCtx.SessionData.DataConversion.Location = time.FixedZone("PST", -8*60*60)
	stale()
	evalCtx.SessionData.DataConversion.Location = time.UTC
	notStale()

	// Stale bytes encode format.
	evalCtx.SessionData.DataConversion.BytesEncodeFormat = sessiondata.BytesEncodeBase64
	stale()
	evalCtx.SessionData.DataConversion.BytesEncodeFormat = sessiondata.BytesEncodeHex
	notStale()

	// Stale extra float digits.
	evalCtx.SessionData.DataConversion.ExtraFloatDigits = 2
	stale()
	evalCtx.SessionData.DataConversion.ExtraFloatDigits = 0
	notStale()

	// Stale reorder joins limit.
	evalCtx.SessionData.ReorderJoinsLimit = 4
	stale()
	evalCtx.SessionData.ReorderJoinsLimit = 0
	notStale()

	// Stale zig zag join enable.
	evalCtx.SessionData.ZigzagJoinEnabled = true
	stale()
	evalCtx.SessionData.ZigzagJoinEnabled = false
	notStale()

	// Stale optimizer FK planning enable.
	evalCtx.SessionData.OptimizerFKs = true
	stale()
	evalCtx.SessionData.OptimizerFKs = false
	notStale()

	// Stale safe updates.
	evalCtx.SessionData.SafeUpdates = true
	stale()
	evalCtx.SessionData.SafeUpdates = false
	notStale()

	// Stale data sources and schema. Create new catalog so that data sources are
	// recreated and can be modified independently.
	catalog = testcat.New()
	_, err = catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT, c STRING, INDEX (c))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE VIEW abcview AS SELECT a, b, c FROM abc")
	if err != nil {
		t.Fatal(err)
	}

	// User no longer has access to view.
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = true
	_, err = o.Memo().IsStale(ctx, &evalCtx, catalog)
	if exp := "user does not have privilege"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %+v", exp, err)
	}
	catalog.View(tree.NewTableName("t", "abcview")).Revoked = false
	notStale()

	// Table ID changes.
	catalog.Table(tree.NewTableName("t", "abc")).TabID = 1
	stale()
	catalog.Table(tree.NewTableName("t", "abc")).TabID = 53
	notStale()

	// Table Version changes.
	catalog.Table(tree.NewTableName("t", "abc")).TabVersion = 1
	stale()
	catalog.Table(tree.NewTableName("t", "abc")).TabVersion = 0
	notStale()
}

// TestStatsAvailable tests that the statisticsBuilder correctly identifies
// for each expression whether statistics were available on the base table.
// This test is here (instead of statistics_builder_test.go) to avoid import
// cycles.
func TestStatsAvailable(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL(
		"CREATE TABLE t (a INT, b INT)",
	); err != nil {
		t.Fatal(err)
	}

	var o xform.Optimizer

	testNotAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if e.Relational().Stats.Available {
				t.Fatal("stats should not be available")
			}
		})
	}

	// Stats should not be available for any expression.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testNotAvailable(o.Memo().RootExpr().(memo.RelExpr))

	if _, err := catalog.ExecuteDDL(
		`ALTER TABLE t INJECT STATISTICS '[
		{
			"columns": ["a"],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		},
		{
			"columns": ["b"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 1000,
			"distinct_count": 500
		}
	]'`); err != nil {
		t.Fatal(err)
	}

	testAvailable := func(expr memo.RelExpr) {
		traverseExpr(expr, func(e memo.RelExpr) {
			if !e.Relational().Stats.Available {
				t.Fatal("stats should be available")
			}
		})
	}

	// Stats should be available for all expressions.
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM t WHERE a=1")
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT sum(a), b FROM t GROUP BY b")
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))

	opttestutils.BuildQuery(t, &o, catalog, &evalCtx,
		"SELECT * FROM t AS t1, t AS t2 WHERE t1.a = t2.a AND t1.b = 5",
	)
	testAvailable(o.Memo().RootExpr().(memo.RelExpr))
}

// traverseExpr is a helper function to recursively traverse a relational
// expression and apply a function to the root as well as each relational
// child.
func traverseExpr(expr memo.RelExpr, f func(memo.RelExpr)) {
	f(expr)
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if child, ok := expr.Child(i).(memo.RelExpr); ok {
			traverseExpr(child, f)
		}
	}
}

func TestTSInfo(t *testing.T) {
	flags := memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps | memo.ExprFmtHideQualifications |
		memo.ExprFmtHideStats
	runDataDrivenTest(t, "testdata/tsinfo", flags)
}

// runDataDrivenTest runs data-driven testcases of the form
//
//	<command>
//	<SQL statement>
//	----
//	<expected results>
//
// See OptTester.Handle for supported commands.
func runDataDrivenTest(t *testing.T, path string, fmtFlags memo.ExprFmtFlags) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}

func TestMultiModelResetReasonString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		reason   memo.MultiModelResetReason
		expected string
	}{
		{memo.UnsupportedAggFuncOrExpr, "unsupported aggregation function or expression"},
		{memo.UnsupportedDataType, "unsupported data type"},
		{memo.JoinBetweenTimeSeriesTables, "join between time-series tables"},
		{memo.UnsupportedCrossJoin, "cross join is not supported in multi-model"},
		{memo.LeftJoinColsPositionMismatch, "mismatch in left join columns' positions with relationalInfo"},
		{memo.UnsupportedCastOnTSColumn, "cast on time series column is not supported in multi-model"},
		{memo.JoinColsTypeOrLengthMismatch, "mismatch in join columns' type or length"},
		{memo.UnsupportedOperation, "unsupported operation in multi-model context"},
		{memo.JoinOnTSMetricsColumn, "join on time-series metrics column"},
		{memo.UnsupportedSemiJoin, "semi join is not supported in multi-model"},
		{memo.UnsupportedOuterJoin, "outer join is not supported in multi-model"},
		{memo.UnsupportedPlanMode, "the access plan cannot be optimized in multi-model"},
		{memo.MultiModelResetReason(999), "unknown"},
	}

	for _, tc := range testCases {
		result := tc.reason.String()
		if result != tc.expected {
			t.Errorf("MultiModelResetReason(%d).String() = %q, want %q", tc.reason, result, tc.expected)
		}
	}
}

func TestMultimodelHelperGetTableIndexFromGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	helper := &memo.MultimodelHelper{}
	helper.TableGroup = [][]opt.TableID{
		{1, 2, 3},
		{4, 5},
		{6},
	}

	testCases := []struct {
		id       opt.TableID
		expected int
	}{
		{1, 0},
		{2, -1},
		{3, -1},
		{4, 1},
		{5, -1},
		{6, 2},
		{99, -1},
	}

	for _, tc := range testCases {
		result := helper.GetTableIndexFromGroup(tc.id)
		if result != tc.expected {
			t.Errorf("GetTableIndexFromGroup(%d) = %d, want %d", tc.id, result, tc.expected)
		}
	}
}

func TestMultimodelHelperGetTSTableIndexFromGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	helper := &memo.MultimodelHelper{}
	helper.TableGroup = [][]opt.TableID{
		{1, 2, 3},
		{4, 5},
		{6},
	}

	testCases := []struct {
		id       opt.TableID
		expected int
	}{
		{1, 0},
		{2, 0},
		{3, 0},
		{4, 1},
		{5, 1},
		{6, 2},
		{99, -1},
	}

	for _, tc := range testCases {
		result := helper.GetTSTableIndexFromGroup(tc.id)
		if result != tc.expected {
			t.Errorf("GetTSTableIndexFromGroup(%d) = %d, want %d", tc.id, result, tc.expected)
		}
	}
}

func TestMemoInitCheckHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	whiteList := &sqlbase.WhiteListMap{Map: make(map[uint32]sqlbase.FilterInfo)}
	m.InitCheckHelper(whiteList)
	if m.GetWhiteList() != whiteList {
		t.Error("GetWhiteList did not return the expected white list")
	}

	ctx := context.Background()
	m.InitCheckHelper(ctx)

	m.InitCheckHelper(keys.NoGroupHint)
}

func TestMemoSetWhiteList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	whiteList := &sqlbase.WhiteListMap{Map: make(map[uint32]sqlbase.FilterInfo)}
	m.SetWhiteList(whiteList)
	if m.GetWhiteList() != whiteList {
		t.Error("SetWhiteList did not set the white list correctly")
	}
}

func TestMemoForcePushGroupToTSEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	result := m.ForcePushGroupToTSEngine()
	if !result {
		t.Error("ForcePushGroupToTSEngine should return true by default")
	}
}

func TestMemoNotifyOnNewGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	m.NotifyOnNewGroup(func(e opt.Expr) {
	})
}

func TestMemoResetCost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc")

	relExpr := o.Memo().RootExpr().(memo.RelExpr)
	originalCost := relExpr.Cost()
	o.Memo().ResetCost(relExpr, memo.Cost(100.0))
	newCost := relExpr.Cost()

	if newCost != memo.Cost(100.0) {
		t.Errorf("ResetCost failed: got %v, want 100.0", newCost)
	}
	if originalCost == newCost {
		t.Error("Cost should have changed")
	}
}

func TestMemoSetFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	flag := opt.IncludeTSTable
	if m.CheckFlag(flag) {
		t.Error("Flag should not be set initially")
	}

	m.SetFlag(flag)
	if !m.CheckFlag(flag) {
		t.Error("Flag should be set after SetFlag")
	}

	m.ClearFlag(flag)
	if m.CheckFlag(flag) {
		t.Error("Flag should be cleared after ClearFlag")
	}
}

func TestMemoAddColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	col := opt.ColumnID(1)
	alias := "test_col"
	typ := memo.ExprType(memo.ExprTypCol)
	pos := memo.ExprPos(memo.ExprPosSelect)
	hash := uint32(12345)
	isTimeBucket := false

	m.AddColumn(col, alias, typ, pos, hash, isTimeBucket)

	helper := m.GetPushHelperAddress()
	if helper == nil {
		t.Fatal("GetPushHelperAddress returned nil")
	}

	info, ok := (*helper)[col]
	if !ok {
		t.Fatal("Column was not added to the helper")
	}

	if info.Alias != alias {
		t.Errorf("Alias mismatch: got %q, want %q", info.Alias, alias)
	}
	if info.Type != typ {
		t.Errorf("Type mismatch: got %d, want %d", info.Type, typ)
	}
	if info.Pos != pos {
		t.Errorf("Pos mismatch: got %d, want %d", info.Pos, pos)
	}
	if info.Hash != hash {
		t.Errorf("Hash mismatch: got %d, want %d", info.Hash, hash)
	}
	if info.IsTimeBucket != isTimeBucket {
		t.Errorf("IsTimeBucket mismatch: got %v, want %v", info.IsTimeBucket, isTimeBucket)
	}
}

func TestMemoCheckExecInTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	col := opt.ColumnID(1)
	m.AddColumn(col, "test_col", memo.ExprTypCol, memo.ExprPosSelect, 12345, false)

	result := m.CheckExecInTS(col, memo.ExprPosSelect)
	if !result {
		t.Error("CheckExecInTS should return true for ExprTypCol")
	}

	constCol := opt.ColumnID(2)
	m.AddColumn(constCol, "const_col", memo.ExprTypConst, memo.ExprPosSelect, 12346, false)
	result = m.CheckExecInTS(constCol, memo.ExprPosSelect)
	if !result {
		t.Error("CheckExecInTS should return true for ExprTypConst")
	}

	nonExistentCol := opt.ColumnID(999)
	result = m.CheckExecInTS(nonExistentCol, memo.ExprPosSelect)
	if result {
		t.Error("CheckExecInTS should return false for non-existent column")
	}
}

func TestMemoDetach(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc")

	o.Memo().Detach()

	if o.Memo().RootExpr() == nil {
		t.Error("RootExpr should not be nil after Detach")
	}
}

func TestMemoHasPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE a = $1")

	hasPlaceholders := o.Memo().HasPlaceholders()
	if !hasPlaceholders {
		t.Error("HasPlaceholders should return true for query with placeholder")
	}
}

func TestMemoHasPlaceholdersNoPlaceholder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE abc (a INT PRIMARY KEY, b INT)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT * FROM abc WHERE a = 1")

	hasPlaceholders := o.Memo().HasPlaceholders()
	if hasPlaceholders {
		t.Error("HasPlaceholders should return false for query without placeholder")
	}
}

func TestMemoSortFilters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	// nil
	{
		selExpr := &memo.SelectExpr{
			Filters: memo.FiltersExpr{},
		}
		relProps := &props.Relational{}

		m.SortFilters(selExpr, relProps)
	}

	// tsscanexpr
	{
		m.Metadata().AddColumn("col1", types.Int)
		m.Metadata().AddColumn("col2", types.Int)
		m.Metadata().AddColumn("col3", types.Int)

		cat := testcat.New()
		if _, err := cat.ExecuteDDL("CREATE TABLE t1 (k timestamp not null, e1 INT, e2 int, e3 int, ptag int not null)"); err != nil {
			t.Fatal(err)
		}

		var f norm.Factory
		f.Init(&evalCtx, cat)

		tn := tree.NewTableName("t", "t1")
		a := f.Metadata().AddTable(cat.Table(tn), tn)
		ax := a.ColumnID(0)

		eq := f.ConstructEq(f.ConstructVariable(ax), f.ConstructConst(tree.NewDInt(1), types.Int))
		filter1 := f.ConstructLt(f.ConstructVariable(a.ColumnID(1)), f.ConstructConst(tree.NewDInt(10), types.Int))
		filter2 := f.ConstructNe(f.ConstructVariable(a.ColumnID(2)), f.ConstructConst(tree.NewDInt(5), types.Int))

		var cols opt.ColSet
		cols.Add(1)
		cols.Add(2)
		cols.Add(3)
		scan := f.ConstructTSScan(&memo.TSScanPrivate{Table: a, Cols: cols})

		relProps := &props.Relational{Stats: props.Statistics{
			Available: true, RowCount: 10000, PTagCount: 100,
		}}

		{
			filters := memo.FiltersExpr{
				f.ConstructFiltersItem(eq),
				f.ConstructFiltersItem(filter1),
				f.ConstructFiltersItem(filter2),
			}
			sel := f.ConstructSelect(scan, filters)
			m.SortFilters(sel.(*memo.SelectExpr), relProps)
		}

		{
			filters := memo.FiltersExpr{
				f.ConstructFiltersItem(f.ConstructAnd(eq, filter1)),
				f.ConstructFiltersItem(f.ConstructOr(filter1, filter2)),
			}
			sel := f.ConstructSelect(scan, filters)
			m.SortFilters(sel.(*memo.SelectExpr), relProps)
		}
	}
}

func TestMemoSetAllFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	flags := opt.IncludeTSTable | opt.HasAutoLimit
	m.SetAllFlag(flags)
	if m.GetAllFlag() != flags {
		t.Errorf("SetAllFlag failed: got %d, want %d", m.GetAllFlag(), flags)
	}
}

func TestMemoSetScalarRoot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	var scalar opt.ScalarExpr
	m.SetScalarRoot(scalar)
	if m.RootExpr() != scalar {
		t.Error("SetScalarRoot failed to set root expression")
	}
}

func TestMemoReplaceProcedureParam(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)
	semaCtx := tree.MakeSemaContext()

	var params []sqlbase.ProcParam
	tmp := sqlbase.ProcParam{
		Name:      string("p1"),
		Direction: sqlbase.ProcParam_InDirection,
		Type:      *types.Int,
	}
	params = append(params, tmp)

	var body memo.ArrayCommand
	body.Bodys = append(body.Bodys, &memo.DeclareVarCommand{
		Col: memo.DeclareCol{
			Name: "p1",
			Typ:  types.Int,
			Idx:  1,
			Mode: tree.DeclareValue,
		},
	})

	private := &memo.CallProcedurePrivate{
		ProcName: string("CP1"),
		ProcCall: "CALL CP1(1)",
		ProcComm: &memo.BlockCommand{Body: body},
	}

	callExpr := m.MemoizeCallProcedure(private)
	m.SetRoot(callExpr, &physical.Required{})

	constParams := tree.Exprs{tree.NewDInt(1)}

	if err := m.ReplaceProcedureParam(&semaCtx, constParams, params); err != nil {
		t.Error("ReplaceProcedureParam failed")
	}
}

func TestMemoCalculateDop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)
	m.CalculateDop(10000, 100, uint32(1024))
	m.GetTsDop()
	m.SetTsDop(2)
	m.CalculateDop(10000, 100, uint32(1024))

	m.SetTsDop(0)
	m.CalculateDop(sqlbase.HighDataThreshold, 100, uint32(1024))

	m.SetTsDop(0)
	m.CalculateDop(sqlbase.LowDataThreshold, 100, uint32(1024))
}

func TestMemoCheckDataLength(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typ := *types.Int

	ret := memo.CheckDataLength(&typ)
	require.Equal(t, ret, true)

	typ.InternalType.TypeEngine = types.RELATIONAL.Mask()
	ret = memo.CheckDataLength(&typ)
	require.Equal(t, ret, false)

	typ.InternalType.TypeEngine = types.TIMESERIES.Mask()
	ret = memo.CheckDataLength(&typ)
	require.Equal(t, ret, true)

	typ1 := types.MakeBytes(10, types.TIMESERIES.Mask())
	ret = memo.CheckDataLength(typ1)
	require.Equal(t, ret, true)

	typ2 := types.MakeChar(10)
	typ2.InternalType.TypeEngine = types.TIMESERIES.Mask()
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, true)

	typ2 = types.MakeChar(sqlbase.TSMaxFixedLen/4 + 1)
	typ2.InternalType.TypeEngine = types.TIMESERIES.Mask()
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, false)

	typ2 = types.MakeVarBytes(16383, types.TIMESERIES.Mask())
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, true)

	typ2 = types.MakeVarBytes(sqlbase.TSMaxVariableLen+1, types.TIMESERIES.Mask())
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, false)

	typ2 = types.MakeVarChar(16383, types.TIMESERIES.Mask())
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, true)

	typ2 = types.MakeVarChar(sqlbase.TSMaxVariableLen+1, types.TIMESERIES.Mask())
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, false)

	typ2 = types.MakeNVarChar(sqlbase.TSMaxVariableLen/4 + 1)
	typ2.InternalType.TypeEngine = types.TIMESERIES.Mask()
	ret = memo.CheckDataLength(typ2)
	require.Equal(t, ret, false)
}

func TestMemoProcedureCacheIsStale(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE t1 (a timestamp not null, b INT)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	m.ProcedureCacheIsStale(evalCtx.Ctx(), &evalCtx, catalog)

	evalCtx.SessionData.ReorderJoinsLimit = 10
	m.ProcedureCacheIsStale(evalCtx.Ctx(), &evalCtx, catalog)
}

func TestMemoGetPhyColIDByMetaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE t1 (a timestamp not null, b INT) ATTRIBUTES (tag1 int not null, tag2 int) PRIMARY TAGS(tag1)")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, catalog, &evalCtx, "SELECT a,b FROM t1")
	o.Memo().CheckExpr(o.Memo().RootExpr())
	var ColID uint32
	// since a filter can only operate on a single column, we only get first column.
	ColID, err = o.Memo().GetPhyColIDByMetaID(opt.ColumnID(1))
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, ColID, uint32(1))

	id := o.Memo().Metadata().AddColumn("col1", types.Int)

	ColID, err = o.Memo().GetPhyColIDByMetaID(id)
	require.Error(t, err)
}

func TestMemoGetTagIndexKeyAndFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	tableName := tree.MakeTableName("tsdb", "t1")
	col1 := testcat.Column{
		Ordinal: 0, Name: "kt", Type: types.Timestamp, Nullable: false, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_TIMESTAMP, ColumnType: sqlbase.ColumnType_TYPE_DATA}}
	col2 := testcat.Column{
		Ordinal: 1, Name: "e1", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_DATA}}
	col3 := testcat.Column{
		Ordinal: 2, Name: "tag1", Type: types.Int, Nullable: false, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_PTAG}}
	col4 := testcat.Column{
		Ordinal: 3, Name: "tag2", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_TAG}}
	col5 := testcat.Column{
		Ordinal: 4, Name: "tag3", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_TAG}}
	testTable := &testcat.Table{
		TableType: tree.TimeseriesTable,
		TabID:     79,
		TabName:   tableName,
		Columns:   []*testcat.Column{&col1, &col2, &col3, &col4, &col5},
		Catalog:   catalog,
	}
	idx1 := &testcat.Index{
		IdxName: "primary", Unique: true, IdxTable: testTable, IdxOrdinal: 0, Columns: []cat.IndexColumn{{Column: &col1, Ordinal: 0}}, KeyCount: 3}
	idx2 := &testcat.Index{IdxName: "idx1", IdxTable: testTable, IdxOrdinal: 3, Columns: []cat.IndexColumn{
		{Column: &col4, Ordinal: 3},
		{Column: &col1, Ordinal: 0},
	}}
	testTable.Indexes = append(testTable.Indexes, idx1)
	testTable.Indexes = append(testTable.Indexes, idx2)
	// one index
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(&evalCtx, catalog)
	tableID := f.Memo().Metadata().AddTable(testTable, &tableName)
	private := &memo.TSScanPrivate{Table: tableID}
	ax1 := tableID.ColumnID(3)
	{
		tagFilters := make(memo.FiltersExpr, 0)
		tagFilter := f.ConstructEq(f.ConstructVariable(ax1), f.ConstructConst(tree.NewDInt(100), types.Int))
		tagFilters = append(tagFilters, f.ConstructFiltersItem(tagFilter))
		memo.GetTagIndexKeyAndFilter(private, &tagFilters, f.Memo(), 1)
		memo.GetTagIndexKeyAndFilter(private, &tagFilters, f.Memo(), 2)

		tagFilters = make(memo.FiltersExpr, 0)
		agFilter1 := f.ConstructEq(f.ConstructVariable(ax1), f.ConstructConst(tree.NewDInt(10), types.Int))
		tagFilter2 := f.ConstructOr(tagFilter, agFilter1)
		tagFilters = append(tagFilters, f.ConstructFiltersItem(tagFilter2))
		memo.GetTagIndexKeyAndFilter(private, &tagFilters, f.Memo(), 1)
	}

	{
		tagFilters := make(memo.FiltersExpr, 0)
		colID1 := tableID.ColumnID(2)
		agFilter1 := f.ConstructEq(f.ConstructVariable(colID1), f.ConstructConst(tree.NewDInt(100), types.Int))
		tagFilter2 := f.ConstructEq(f.ConstructVariable(colID1), f.ConstructConst(tree.NewDInt(10), types.Int))
		tagFilter3 := f.ConstructOr(agFilter1, tagFilter2)
		tagFilters = append(tagFilters, f.ConstructFiltersItem(tagFilter3))
		memo.GetTagIndexKeyAndFilter(private, &tagFilters, f.Memo(), 1)
	}

	{
		ptagMap := make(memo.TagColMap, 0)
		ptagMap[3] = struct{}{}
		tagMap := make(memo.TagColMap, 0)
		tagMap[4] = struct{}{}
		tagMap[5] = struct{}{}
		tempTagValues := make(map[uint32][]string)
		colID1 := tableID.ColumnID(2)
		agFilter1 := f.ConstructIn(f.ConstructVariable(colID1), f.ConstructTuple(memo.ScalarListExpr{
			f.ConstructConstVal(tree.NewDInt(10), types.Int),
			f.ConstructConstVal(tree.NewDInt(20), types.Int),
		}, types.Int))
		memo.GetTagValues(agFilter1, ptagMap, tagMap, &tempTagValues, 1, 2)
	}
}

func TestMemoGetTagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	tableName := tree.MakeTableName("tsdb", "t1")
	col1 := testcat.Column{
		Ordinal: 0, Name: "kt", Type: types.Timestamp, Nullable: false, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_TIMESTAMP, ColumnType: sqlbase.ColumnType_TYPE_DATA}}
	col2 := testcat.Column{
		Ordinal: 1, Name: "e1", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_DATA}}
	col3 := testcat.Column{
		Ordinal: 2, Name: "tag1", Type: types.Bool, Nullable: false, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_BOOL, ColumnType: sqlbase.ColumnType_TYPE_PTAG}}
	col4 := testcat.Column{
		Ordinal: 3, Name: "tag2", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_TAG}}
	col5 := testcat.Column{
		Ordinal: 4, Name: "tag3", Type: types.Int, Nullable: true, TsCol: sqlbase.TSCol{SqlType: sqlbase.DataType_INT, ColumnType: sqlbase.ColumnType_TYPE_TAG}}
	testTable := &testcat.Table{
		TableType: tree.TimeseriesTable,
		TabID:     79,
		TabName:   tableName,
		Columns:   []*testcat.Column{&col1, &col2, &col3, &col4, &col5},
		Catalog:   catalog,
	}
	idx1 := &testcat.Index{
		IdxName: "primary", Unique: true, IdxTable: testTable, IdxOrdinal: 0, Columns: []cat.IndexColumn{{Column: &col1, Ordinal: 0}}, KeyCount: 3}
	idx2 := &testcat.Index{IdxName: "idx1", IdxTable: testTable, IdxOrdinal: 3, Columns: []cat.IndexColumn{
		{Column: &col4, Ordinal: 3},
		{Column: &col1, Ordinal: 0},
	}}
	testTable.Indexes = append(testTable.Indexes, idx1)
	testTable.Indexes = append(testTable.Indexes, idx2)
	// one index
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(&evalCtx, catalog)
	tableID := f.Memo().Metadata().AddTable(testTable, &tableName)
	colID1 := tableID.ColumnID(2)

	agFilter1 := f.ConstructEq(f.ConstructVariable(colID1), f.ConstructTrue())
	ptagMap := make(memo.TagColMap, 0)
	ptagMap[3] = struct{}{}
	tagMap := make(memo.TagColMap, 0)
	tagMap[4] = struct{}{}
	tagMap[5] = struct{}{}
	tempTagValues := make(map[uint32][]string)
	memo.GetTagValues(agFilter1, ptagMap, tagMap, &tempTagValues, 1, 2)

	agFilter2 := f.ConstructEq(f.ConstructVariable(colID1), f.ConstructFalse())
	memo.GetTagValues(agFilter2, ptagMap, tagMap, &tempTagValues, 1, 2)
}
