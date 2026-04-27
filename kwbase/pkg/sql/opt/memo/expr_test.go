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

package memo_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/exprgen"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	opttestutils "gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/opttester"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	_ "gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// TestExprIsNeverNull runs data-driven testcases of the form
//
//	<command> [<args>]...
//	<SQL statement or expression>
//	----
//	<expected results>
//
// See OptTester.Handle for supported commands. In addition to those, we
// support:
//
//   - scalar-is-not-nullable [args]
//
//     Builds a scalar expression using the input and performs a best-effort
//     check to see if the scalar expression is nullable. It outputs this
//     result as a boolean.
//
//     The supported args (in addition to the ones supported by OptTester):
//
//   - vars=(type1,type2,...)
//
//     Adding a !null suffix on a var type is used to mark that var as
//     non-nullable.
func TestExprIsNeverNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/expr", func(t *testing.T, path string) {
		catalog := testcat.New()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var varTypes []*types.T
			var err error

			tester := opttester.New(catalog, d.Input)
			switch d.Cmd {
			case "scalar-is-not-nullable":
				var notNullCols opt.ColSet
				for _, arg := range d.CmdArgs {
					key, vals := arg.Key, arg.Vals
					switch key {
					case "vars":
						for i := 0; i < len(vals); i++ {
							if strings.HasSuffix(strings.ToLower(vals[i]), "!null") {
								vals[i] = strings.TrimSuffix(strings.ToLower(vals[i]), "!null")
								notNullCols.Add(opt.ColumnID(i + 1))
							}
						}
						varTypes, err = exprgen.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

					default:
						if err := tester.Flags.Set(arg); err != nil {
							d.Fatalf(t, "%s", err)
						}
					}
				}

				expr, err := parser.ParseExpr(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				ctx := context.Background()
				semaCtx := tree.MakeSemaContext()
				evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

				var o xform.Optimizer
				o.Init(&evalCtx, catalog)
				for i, typ := range varTypes {
					o.Memo().Metadata().AddColumn(fmt.Sprintf("@%d", i+1), typ)
				}
				b := optbuilder.NewScalar(ctx, &semaCtx, &evalCtx, o.Factory())
				err = b.Build(expr)
				if err != nil {
					return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
				}
				return fmt.Sprintf("%t\n", memo.ExprIsNeverNull(o.Memo().RootExpr().(opt.ScalarExpr), notNullCols))

			default:
				return tester.RunCommand(t, d)
			}
		})
	})
}

func TestExprString(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	test := memo.TagIndexInfo{
		UnionType: memo.Intersection,
		IndexID:   []uint32{uint32(0), uint32(1)},
		TagIndexValues: []map[uint32][]string{
			{0: []string{"10"}, 1: []string{"20"}},
		},
	}

	test.String()
}

// testExpr save expr all info
type testExpr struct {
}

// IsTargetExpr checks if it's target expr to handle
func (p *testExpr) IsTargetExpr(self opt.Expr) bool {

	return false
}

// NeedToHandleChild checks if children expr need to be handled
func (p *testExpr) NeedToHandleChild() bool {
	return true
}

// HandleChildExpr deals with all child expr
func (p *testExpr) HandleChildExpr(parent opt.Expr, child opt.Expr) bool {
	return true
}

func TestExprRebuildExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var m memo.Memo
	m.Init(&evalCtx)

	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (k timestamp not null, a INT, b int, e3 int, ptag int not null)"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.ExecuteDDL("CREATE TABLE t2 (k timestamp not null, a INT, b int, e3 int, ptag int not null)"); err != nil {
		t.Fatal(err)
	}

	var f norm.Factory
	f.Init(&evalCtx, cat)

	tn := tree.NewTableName("t", "t1")
	a := f.Metadata().AddTable(cat.Table(tn), tn)
	ax := a.ColumnID(0)

	eq := f.ConstructEq(f.ConstructVariable(ax), f.ConstructConst(tree.NewDInt(1), types.Int))
	memo.RebuildExpr(eq)
	filter1 := f.ConstructLt(f.ConstructVariable(a.ColumnID(1)), f.ConstructConst(tree.NewDInt(10), types.Int))
	memo.RebuildExpr(filter1)
	filter2 := f.ConstructNe(f.ConstructVariable(a.ColumnID(2)), f.ConstructConst(tree.NewDInt(5), types.Int))
	memo.RebuildExpr(filter2)

	var cols opt.ColSet
	cols.Add(1)
	cols.Add(2)
	cols.Add(3)
	scanExpr := f.ConstructScan(&memo.ScanPrivate{Table: a, Cols: cols})
	memo.RebuildExpr(scanExpr)
	selectExpr := f.ConstructSelect(scanExpr, memo.FiltersExpr{f.ConstructFiltersItem(eq)})
	memo.RebuildExpr(selectExpr)

	var o xform.Optimizer
	opttestutils.BuildQuery(t, &o, cat, &evalCtx, "SELECT a,b FROM t1 where a > 10 and b > (select max(b) from t2 where a < 10)")

	exprMap := make(map[interface{}]struct{}, 0)
	memo.RebuildExprs(o.Memo().RootExpr(), exprMap)
}

func TestExprFill(t *testing.T) {
	rang := tree.FillRange{BeforeRange: 0, AfterRange: 10}
	var f memo.TSFill
	f = &memo.RangeFill{FillType: tree.ExactFill, FillRange: &rang}
	f.String()
	f.ConvertFill()
	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDInt(1), Typ: types.Int}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDInt(math.MaxInt16 + 1), Typ: types.Int}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDInt(math.MaxInt32 + 1), Typ: types.Int}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDString("1"), Typ: types.String}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDFloat(1.1), Typ: types.Float}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.NewDBytes("2"), Typ: types.Bytes}, Typ: types.Float4}}
	f.String()
	f.ConvertFill()

	f = &memo.ConstFill{ConstExpr: &memo.CastExpr{Input: &memo.ConstExpr{Value: tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), Typ: types.Timestamp}, Typ: types.Int}}
	f.String()
	f.ConvertFill()
}

func TestExprJoinFlagsString(t *testing.T) {
	var tmp memo.JoinFlags
	tmp = memo.AllowHashJoinStoreLeft
	tmp.String()
}

func TestExprWindowFrameString(t *testing.T) {
	w := memo.WindowFrame{tree.RANGE, tree.UnboundedPreceding, tree.CurrentRow, tree.NoExclusion}
	w.String()
	w.Mode = tree.ROWS
	w.String()
	w.Mode = tree.GROUPS
	w.String()
}

type testExprHelper struct {
	f          *norm.Factory
	filter     opt.ScalarExpr
	filter1    opt.ScalarExpr
	scanExpr   memo.RelExpr
	selectExpr memo.RelExpr
	cat        *testcat.Catalog
	subExpr    memo.RelExpr
}

func testRule(ruleName opt.RuleName) bool {
	return false
}

func (h *testExprHelper) initTest() error {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int, e bytes, f json, h bool, j char(100), ts timestamptz)"); err != nil {
		return err
	}
	var f norm.Factory
	f.Init(&evalCtx, cat)

	f.NotifyOnMatchedRule(testRule)

	tn := tree.NewTableName("t", "t1")
	a := f.Metadata().AddTable(cat.Table(tn), tn)
	ax := a.ColumnID(0)
	ax1 := a.ColumnID(1)
	eq := f.ConstructEq(f.ConstructVariable(ax), f.ConstructConst(tree.NewDInt(1), types.Int))
	eq1 := f.ConstructIsNot(f.ConstructVariable(ax1), f.ConstructNull(types.Int))

	var cols opt.ColSet
	cols.Add(a.ColumnID(0))
	cols.Add(a.ColumnID(1))
	cols.Add(a.ColumnID(2))
	scanExpr := f.ConstructScan(&memo.ScanPrivate{Table: a, Cols: cols})
	memo.RebuildExpr(scanExpr)
	selectExpr := f.ConstructSelect(scanExpr, memo.FiltersExpr{f.ConstructFiltersItem(eq)})
	memo.RebuildExpr(selectExpr)

	var cols1 opt.ColSet
	cols1.Add(a.ColumnID(0))
	scanExpr1 := f.ConstructScan(&memo.ScanPrivate{Table: a, Cols: cols1})

	h.f = &f
	h.filter = eq
	h.filter1 = eq1
	h.scanExpr = scanExpr
	h.subExpr = scanExpr1
	h.selectExpr = selectExpr
	h.cat = cat
	return nil
}

func testExprInterfaceFn(e memo.RelExpr, child []opt.Expr) {
	var test testExpr
	e.Walk(&test)
	e.Private()
	e.String()

	for i := 0; i < e.ChildCount(); i++ {
		if child != nil && child[i] != nil {
			e.(opt.MutableExpr).SetChild(i, child[i])
		}
	}

	e.Memo()
	e.GetAddSynchronizer()
	e.SetAddSynchronizer()
	e.ResetAddSynchronizer()
	e.ClearRequiredPhysical()
	e.IsTSEngine()
	e.SetEngineTS()
	e.Rebuild()
	e.SimpleString(int(memo.ExprFmtHideQualifications))
}

func testScalarExprInterfaceFn(e opt.ScalarExpr, child []opt.Expr) {
	var test testExpr
	e.Walk(&test)
	e.Private()
	e.String()

	for i := 0; i < e.ChildCount(); i++ {
		if child != nil && child[i] != nil {
			e.(opt.MutableExpr).SetChild(i, child[i])
		}
	}

	e.IsTSEngine()
	e.SetEngineTS()

	if !opt.IsListOp(e) {
		e.ID()
	}

	e.DataType()
	e.CheckConstDeductionEnabled()
	e.SetConstDeductionEnabled(true)
	e.Rebuild()
	e.SimpleString(int(memo.ExprFmtHideQualifications))
}

func TestExprRecursiveCte(t *testing.T) {
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}

	bindingProps := &props.Relational{}
	bindingProps.OutputCols = helper.scanExpr.Relational().OutputCols
	bindingProps.Cardinality = props.AnyCardinality.AtLeast(props.OneCardinality)
	bindingProps.Stats.RowCount = 100

	expr1 := helper.f.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	})

	private := memo.RecursiveCTEPrivate{
		Name:          string("test"),
		WithID:        1,
		InitialCols:   opt.ColList{0, 1, 2},
		RecursiveCols: opt.ColList{0, 1, 2},
		OutCols:       opt.ColList{0, 1, 2},
	}
	w := helper.f.ConstructRecursiveCTE(expr1, helper.scanExpr, helper.selectExpr, &private)

	id := w.(*memo.RecursiveCTEExpr).WithBindingID()
	require.Equal(t, id, opt.WithID(1))

	testExprInterfaceFn(w, []opt.Expr{helper.selectExpr, nil, nil})
}

func TestExprWithScan(t *testing.T) {
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}

	helper.f.Memo().Metadata().AddWithBinding(0, helper.scanExpr)
	w := helper.f.ConstructWithScan(&memo.WithScanPrivate{With: 0, Name: "test"})
	testExprInterfaceFn(w, nil)
	testExprInterfaceFn(helper.scanExpr, nil)

	w1 := helper.f.ConstructVirtualScan(&memo.VirtualScanPrivate{
		Table: helper.scanExpr.(*memo.ScanExpr).Table,
		Cols:  helper.scanExpr.(*memo.ScanExpr).Cols,
	})
	testExprInterfaceFn(w1, nil)
	w1.SimpleString(int(memo.ExprFmtHideQualifications))
	testExprInterfaceFn(helper.selectExpr, nil)
	w = helper.f.ConstructWith(helper.scanExpr, helper.selectExpr, &memo.WithPrivate{OriginalExpr: &tree.Select{}})
	testExprInterfaceFn(w, nil)
}

func TestExprSort(t *testing.T) {
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}

	w := memo.SortExpr{Input: helper.selectExpr}
	w.InputOrdering.Optional = opt.ColSet{}
	w.InputOrdering.Columns = make([]physical.OrderingColumnChoice, 1)
	{
		w.InputOrdering.Columns[0].Group.Add(1)
		w.InputOrdering.Columns[0].Descending = true
	}
	testExprInterfaceFn(&w, []opt.Expr{helper.scanExpr})
}

func TestExprMax1Row(t *testing.T) {
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}

	w := helper.f.ConstructMax1Row(helper.scanExpr, "max one row test error")
	testExprInterfaceFn(w, []opt.Expr{helper.scanExpr})
	tester := opttester.New(helper.cat, "select * from t1")
	if _, err := tester.Optimize(); err != nil {
		t.Fatal(err)
	}
	w = helper.f.ConstructOrdinality(helper.scanExpr, &memo.OrdinalityPrivate{
		Ordering: physical.OrderingChoice{
			Columns: []physical.OrderingColumnChoice{{Group: opt.MakeColSet(1)}},
		},
		ColID: opt.ColumnID(1),
	})
	testExprInterfaceFn(w, []opt.Expr{helper.scanExpr})

	bindingProps := &props.Relational{}
	bindingProps.OutputCols = helper.scanExpr.(*memo.ScanExpr).Cols
	bindingProps.Cardinality = props.AnyCardinality.AtLeast(props.OneCardinality)
	bindingProps.Stats.RowCount = 10000
	w = helper.f.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	})
	testExprInterfaceFn(w, []opt.Expr{helper.scanExpr})
}

func TestExprInnerJoinExpr(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int)"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.ExecuteDDL("CREATE TABLE t2 (a int primary key , b INT, c int, foreign key (a) references t.t1(a))"); err != nil {
		t.Fatal(err)
	}

	tester := opttester.New(cat, "select * from t1 join t2 on t1.a=t2.a")
	if rootExpr, err := tester.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		e, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(e, nil)
		e.SimpleString(int(memo.ExprFmtHideQualifications))
		e.ProvidedPhysical()
		e.ClearRequiredPhysical()
	}

	tester1 := opttester.New(cat, "select * from t1 join t2 on t1.b=t2.a")
	if rootExpr, err := tester1.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		e, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(e, nil)
		e.SimpleString(int(memo.ExprFmtHideQualifications))
		e.ProvidedPhysical()
		e.ClearRequiredPhysical()
	}
}

func TestExprGroupByExpr(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int)"); err != nil {
		t.Fatal(err)
	}
	testSql := []string{
		"select sum(a) from t1",
		"select sum(a) from t1 group by b",
		"select distinct b from t1",
		"select a from t1 limit 10",
		"select a from t1 limit 10 offset 2",
		"select row_number() over (partition by b order by a) from t1",
		"with tt1 as (select row_number() over (partition by b order by a) as a1, b as a2 from t1) select * from tt1 where a2 in (1, 2)",
	}
	for _, sql := range testSql {
		tester := opttester.New(cat, sql)
		if rootExpr, err := tester.Optimize(); err != nil {
			t.Fatal(err)
		} else {
			insertExpr, _ := rootExpr.(memo.RelExpr)
			testExprInterfaceFn(insertExpr, nil)
			insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
			insertExpr.ProvidedPhysical()
		}
	}
	{
		if _, err := cat.ExecuteDDL("CREATE TABLE t2 (a INT primary key, b int, c int, d int)"); err != nil {
			t.Fatal(err)
		}
		setOpString := []string{"union", "union all", "intersect", "intersect all", "except", "except all"}
		for _, opStr := range setOpString {
			sql := "select b, c from t1 " + opStr + " select c, d from t2"
			tester := opttester.New(cat, sql)
			if rootExpr, err := tester.Optimize(); err != nil {
				t.Fatal(err)
			} else {
				insertExpr, _ := rootExpr.(memo.RelExpr)
				testExprInterfaceFn(insertExpr, nil)
				insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
				insertExpr.ProvidedPhysical()
			}
		}
	}
}

func TestExprInsertExpr(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int)"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.ExecuteDDL("CREATE TABLE t2 (a int primary key , b INT, c int, foreign key (a) references t.t1(a))"); err != nil {
		t.Fatal(err)
	}

	tester := opttester.New(cat, "insert into t1(a, b) values(1,2)")
	if rootExpr, err := tester.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		insertExpr, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(insertExpr, nil)
		insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
		insertExpr.ProvidedPhysical()
	}
}

func TestExprUpdateExpr(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int)"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.ExecuteDDL("CREATE TABLE t2 (a int primary key , b INT, c int, foreign key (a) references t.t1(a))"); err != nil {
		t.Fatal(err)
	}

	tester := opttester.New(cat, "update t1 set b = 20 where a = 10")
	if rootExpr, err := tester.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		insertExpr, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(insertExpr, nil)
		insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
		insertExpr.ProvidedPhysical()
	}
}

func TestExprUpsertExpr(t *testing.T) {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE t1 (a INT primary key, b int, c int, d int)"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.ExecuteDDL("CREATE TABLE t2 (a int primary key , b INT, c int, foreign key (a) references t.t1(a))"); err != nil {
		t.Fatal(err)
	}

	tester := opttester.New(cat, "upsert into t1 (a,b) values (20 , 10)")
	if rootExpr, err := tester.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		insertExpr, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(insertExpr, nil)
		insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
		insertExpr.ProvidedPhysical()
	}

	tester1 := opttester.New(cat, "delete from t1 where a = 10")
	if rootExpr, err := tester1.Optimize(); err != nil {
		t.Fatal(err)
	} else {
		insertExpr, _ := rootExpr.(memo.RelExpr)
		testExprInterfaceFn(insertExpr, nil)
		insertExpr.SimpleString(int(memo.ExprFmtHideQualifications))
		insertExpr.ProvidedPhysical()
	}
}

func TestExprScalarExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}
	e := helper.f.ConstructAny(helper.scanExpr, helper.filter, &memo.SubqueryPrivate{})
	testScalarExprInterfaceFn(e, []opt.Expr{helper.selectExpr, helper.filter1})

	e = helper.f.ConstructSubquery(helper.subExpr, &memo.SubqueryPrivate{})
	testScalarExprInterfaceFn(e, []opt.Expr{helper.selectExpr, helper.scanExpr})

	e = helper.f.ConstructExists(helper.subExpr, &memo.SubqueryPrivate{})
	testScalarExprInterfaceFn(e, []opt.Expr{helper.selectExpr, helper.scanExpr})

	valExpr := helper.f.ConstructVariable(opt.ColumnID(1))
	constExpr := helper.f.ConstructConst(tree.NewDInt(10), types.Int)
	constExpr1 := helper.f.ConstructConst(tree.NewDInt(100), types.Int)
	valExpr = helper.f.ConstructVariable(opt.ColumnID(1))
	testScalarExprInterfaceFn(valExpr, nil)

	e = helper.f.ConstructConst(tree.NewDInt(1), types.Int)
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructUserDefinedVar("test", types.Int)
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructNull(types.Int)
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructTrue()
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructFalse()
	testScalarExprInterfaceFn(e, nil)

	testScalarExprInterfaceFn(constExpr, nil)

	intlistExpr := memo.ScalarListExpr{constExpr, constExpr1}
	tupleExpr := helper.f.ConstructTuple(intlistExpr, types.Int)
	testScalarExprInterfaceFn(tupleExpr, []opt.Expr{&intlistExpr})

	plusExpr := helper.f.ConstructPlus(valExpr, constExpr)
	testScalarExprInterfaceFn(plusExpr, []opt.Expr{constExpr, valExpr})
	item := helper.f.ConstructProjectionsItem(plusExpr, 1)
	testScalarExprInterfaceFn(&item, []opt.Expr{plusExpr})
	e = &memo.ProjectionsExpr{item}
	testScalarExprInterfaceFn(e, []opt.Expr{&item})

	sumExpr := helper.f.ConstructSum(valExpr)
	testScalarExprInterfaceFn(sumExpr, []opt.Expr{valExpr})

	item1 := helper.f.ConstructAggregationsItem(sumExpr, opt.ColumnID(3))
	testScalarExprInterfaceFn(&item1, []opt.Expr{valExpr})
	aggExpr := &memo.AggregationsExpr{item1}
	testScalarExprInterfaceFn(aggExpr, []opt.Expr{&item1})

	item2 := helper.f.ConstructFiltersItem(helper.filter)
	testScalarExprInterfaceFn(&item2, []opt.Expr{helper.filter})
	e = &memo.FiltersExpr{item2}
	testScalarExprInterfaceFn(e, []opt.Expr{&item2})

	item3 := helper.f.ConstructZipItem(valExpr, opt.ColList{opt.ColumnID(1)})
	testScalarExprInterfaceFn(&item3, []opt.Expr{helper.filter})
	e = &memo.ZipExpr{item3}
	testScalarExprInterfaceFn(e, []opt.Expr{&item3})

	e = helper.f.ConstructAssignment(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	andExpr := helper.f.ConstructAnd(helper.filter, helper.filter1)
	testScalarExprInterfaceFn(andExpr, []opt.Expr{helper.filter, helper.filter1})

	e = helper.f.ConstructOr(helper.filter, helper.filter1)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.filter, helper.filter1})

	e = helper.f.ConstructRange(andExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{andExpr})

	e = helper.f.ConstructNot(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	testScalarExprInterfaceFn(helper.filter, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructLt(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructGt(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructLe(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructGe(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructNe(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructIn(valExpr, &intlistExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, &intlistExpr})

	e = helper.f.ConstructNotIn(valExpr, &intlistExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, &intlistExpr})

	constStrExpr := helper.f.ConstructConst(tree.NewDString("test"), types.String)
	e = helper.f.ConstructLike(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructNotLike(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructILike(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructNotILike(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructSimilarTo(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructNotSimilarTo(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructRegMatch(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructNotRegMatch(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructRegIMatch(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructNotRegIMatch(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructIs(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructIsNot(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructContains(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructJsonExists(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructJsonAllExists(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})
	e = helper.f.ConstructJsonSomeExists(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructOverlaps(valExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructAnyScalar(valExpr, constStrExpr, opt.NeOp)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	e = helper.f.ConstructBitand(helper.f.ConstructVariable(opt.ColumnID(1)), constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constStrExpr})

	minusExpr := helper.f.ConstructMinus(valExpr, constExpr)
	testScalarExprInterfaceFn(minusExpr, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructMult(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructDiv(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructFloorDiv(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructMod(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructPow(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructConcat(constStrExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{constStrExpr, constStrExpr})

	e = helper.f.ConstructLShift(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructFetchVal(helper.f.ConstructVariable(opt.ColumnID(6)), constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.f.ConstructVariable(opt.ColumnID(6)), constExpr})

	e = helper.f.ConstructFetchText(helper.f.ConstructVariable(opt.ColumnID(6)), constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.f.ConstructVariable(opt.ColumnID(6)), constExpr})

	strlistExpr := memo.ScalarListExpr{constStrExpr, constStrExpr}
	e = helper.f.ConstructFetchValPath(helper.f.ConstructVariable(opt.ColumnID(6)), helper.f.ConstructArray(strlistExpr, types.MakeArray(types.String)))
	testScalarExprInterfaceFn(e, []opt.Expr{helper.f.ConstructVariable(opt.ColumnID(6)), constStrExpr})

	e = helper.f.ConstructFetchTextPath(helper.f.ConstructVariable(opt.ColumnID(6)), helper.f.ConstructArray(strlistExpr, types.MakeArray(types.String)))
	testScalarExprInterfaceFn(e, []opt.Expr{helper.f.ConstructVariable(opt.ColumnID(6)), constStrExpr})

	e = helper.f.ConstructUnaryMinus(minusExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{minusExpr})

	e = helper.f.ConstructUnaryComplement(minusExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{minusExpr})

	e = helper.f.ConstructCast(valExpr, types.Float)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructIfErr(helper.filter, intlistExpr, strlistExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.filter, &intlistExpr, &strlistExpr})

	e = helper.f.ConstructCase(helper.filter, intlistExpr, valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.filter, &intlistExpr, valExpr})

	e = helper.f.ConstructWhen(helper.filter, valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{helper.filter, valExpr})

	e = helper.f.ConstructArray(intlistExpr, types.Int4Array)
	testScalarExprInterfaceFn(e, []opt.Expr{&intlistExpr})

	e = helper.f.ConstructIndirection(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructArrayFlatten(helper.scanExpr, &memo.SubqueryPrivate{RequestedCol: opt.ColumnID(1)})
	testScalarExprInterfaceFn(e, []opt.Expr{helper.scanExpr})

	e = helper.f.ConstructFunction(memo.ScalarListExpr{valExpr}, &memo.FunctionPrivate{Name: "abs", Typ: types.Int})
	testScalarExprInterfaceFn(e, []opt.Expr{&memo.ScalarListExpr{valExpr}})

	e = helper.f.ConstructCollate(valExpr, "Collate test")
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructCoalesce(intlistExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{&intlistExpr})

	tupleType := &types.T{InternalType: types.InternalType{Family: types.TupleFamily, Oid: oid.T_record, TupleContents: []types.T{*types.Int}}}
	tupleConst := helper.f.ConstructConst(
		tree.NewDTuple(tupleType, tree.NewDInt(1), tree.NewDInt(10)),
		tupleType,
	)
	e = helper.f.ConstructColumnAccess(tupleConst, memo.TupleOrdinal(0))
	testScalarExprInterfaceFn(e, []opt.Expr{tupleConst})

	e = helper.f.ConstructUnsupportedExpr(tree.NewDInt(1))
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructArrayAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructAvg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructBitAndAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructBitOrAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructBoolAnd(helper.f.ConstructVariable(opt.ColumnID(7)))
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructBoolOr(helper.f.ConstructVariable(opt.ColumnID(7)))
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructConcatAgg(helper.f.ConstructVariable(opt.ColumnID(8)))
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructCorr(valExpr, valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, valExpr})

	e = helper.f.ConstructCount(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructCountRows()
	testScalarExprInterfaceFn(e, nil)

	e = helper.f.ConstructMax(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructMin(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructMinExtend(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructMaxExtend(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructSumInt(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	ts := helper.f.ConstructConst(tree.MakeDTimestampTZ(time.Date(2018, 10, 6, 11, 49, 30, 123, time.UTC), 0), types.TimestampTZ)
	e = helper.f.ConstructFirst(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	e = helper.f.ConstructFirstTimeStamp(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	e = helper.f.ConstructFirstRow(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	e = helper.f.ConstructFirstRowTimeStamp(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	e = helper.f.ConstructLast(valExpr, ts, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts, ts})

	e = helper.f.ConstructLastTimeStamp(valExpr, ts, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts, ts})

	e = helper.f.ConstructLastRow(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	e = helper.f.ConstructLastRowTimeStamp(valExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, ts})

	v, err := tree.ParseDDecimal("10000.0000001")
	if err != nil {
		t.Fatal(err)
	}
	decimalExpr := helper.f.ConstructConst(v, types.Decimal)
	e = helper.f.ConstructMatching(valExpr, constExpr, decimalExpr, decimalExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr, decimalExpr, decimalExpr, constExpr})

	e = helper.f.ConstructSqrDiff(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructVariance(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructVarPop(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructQuantile(valExpr, decimalExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, decimalExpr})

	e = helper.f.ConstructStdDev(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructNorm(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	tsExpr := helper.f.ConstructVariable(opt.ColumnID(9))
	e = helper.f.ConstructTimeBucketGapfill(tsExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{tsExpr, ts})

	e = helper.f.ConstructImputation(tsExpr, ts)
	testScalarExprInterfaceFn(e, []opt.Expr{tsExpr, ts})

	intervalExpr := helper.f.ConstructConst(tree.NewDInterval(duration.MakeDuration(1000000000, 0, 0), types.IntervalTypeMetadata{}), types.Interval)
	e = helper.f.ConstructElapsed(tsExpr, intervalExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{tsExpr, intervalExpr})

	e = helper.f.ConstructTwa(tsExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{tsExpr, constExpr})

	e = helper.f.ConstructXorAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructJsonAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructJsonbAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructStringAgg(constStrExpr, constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{constStrExpr, constStrExpr})

	e = helper.f.ConstructConstAgg(constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{constStrExpr})

	e = helper.f.ConstructConstNotNullAgg(constStrExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{constStrExpr})

	e = helper.f.ConstructAnyNotNullAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructFirstAgg(valExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr})

	e = helper.f.ConstructAggDistinct(sumExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{sumExpr})

	e = helper.f.ConstructAggFilter(sumExpr, helper.filter)
	testScalarExprInterfaceFn(e, []opt.Expr{sumExpr, helper.filter})

	e = helper.f.ConstructWindowFromOffset(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})

	e = helper.f.ConstructWindowToOffset(valExpr, constExpr)
	testScalarExprInterfaceFn(e, []opt.Expr{valExpr, constExpr})
}

func TestExprValuesExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		t.Fatal(err)
	}

	w := helper.f.ConstructValues(
		memo.ScalarListExpr{helper.f.ConstructConst(tree.NewDInt(1), types.Int)},
		&memo.ValuesPrivate{},
	)
	testExprInterfaceFn(w, []opt.Expr{&memo.ScalarListExpr{helper.f.ConstructConst(tree.NewDInt(2), types.Int)}})
}

type testJoinHelper struct {
	H       *testExprHelper
	Right   memo.RelExpr
	On      memo.FiltersExpr
	Replace []opt.Expr
}

func constructJoinInfo() (*testJoinHelper, error) {
	var helper testExprHelper
	if err := helper.initTest(); err != nil {
		return nil, err
	}
	f := helper.f

	if _, err := helper.cat.ExecuteDDL("CREATE TABLE t2 (a INT primary key, b int, c int, d int)"); err != nil {
		return nil, err
	}
	tn := tree.NewTableName("t", "t2")
	t2 := helper.f.Metadata().AddTable(helper.cat.Table(tn), tn)
	t2bColID := t2.ColumnID(1)

	if _, err := helper.cat.ExecuteDDL("CREATE TABLE ts1 (a timestamp not null, b int, c int) tags(tag1 int not null, tag2 int) primary tags(tag1)"); err != nil {
		return nil, err
	}

	ts1Name := tree.NewTableName("t", "ts1")
	ts1 := helper.f.Metadata().AddTable(helper.cat.Table(ts1Name), ts1Name)
	ts1bColID := ts1.ColumnID(1)
	f.ConstructEq(f.ConstructVariable(ts1bColID), f.ConstructConst(tree.NewDInt(1), types.Int))
	var cols opt.ColSet
	cols.Add(ts1.ColumnID(1))
	cols.Add(ts1.ColumnID(2))
	cols.Add(ts1.ColumnID(3))
	tsscan := f.ConstructTSScan(&memo.TSScanPrivate{Table: ts1, Cols: cols})
	on := f.ConstructEq(f.ConstructVariable(opt.ColumnID(1)), f.ConstructVariable(ts1.ColumnID(1)))

	scanT2 := f.ConstructScan(&memo.ScanPrivate{Table: t2, Cols: cols})
	on1 := f.ConstructEq(f.ConstructVariable(t2bColID), f.ConstructVariable(ts1.ColumnID(1)))

	return &testJoinHelper{H: &helper, Right: tsscan, On: memo.FiltersExpr{f.ConstructFiltersItem(on)},
		Replace: []opt.Expr{scanT2, tsscan, &memo.FiltersExpr{f.ConstructFiltersItem(on1)}}}, nil
}

func TestExprJoinExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if j, err := constructJoinInfo(); err != nil {
		t.Fatal(err)
	} else {
		for _, op := range opt.JoinOperators {
			var w memo.RelExpr
			if opt.BatchLookUpJoinOp == op {
				w = j.H.f.ConstructBatchLookUpJoin(
					j.H.scanExpr, j.Right, j.On, &memo.JoinPrivate{},
				)
			} else {
				w = j.H.f.ConstructJoin(op, j.H.scanExpr, j.Right, j.On, &memo.JoinPrivate{})
			}
			testExprInterfaceFn(w, j.Replace)
		}

		w1 := j.H.f.ConstructIndexJoin(j.H.scanExpr,
			&memo.IndexJoinPrivate{Table: j.H.scanExpr.(*memo.ScanExpr).Table, Cols: j.H.scanExpr.(*memo.ScanExpr).Cols})
		testExprInterfaceFn(w1, j.Replace)

		{
			w2 := j.H.f.ConstructLookupJoin(j.H.scanExpr, j.On,
				&memo.LookupJoinPrivate{
					Table:    j.H.scanExpr.(*memo.ScanExpr).Table,
					Cols:     j.H.scanExpr.(*memo.ScanExpr).Cols,
					JoinType: opt.InnerJoinOp,
				})
			testExprInterfaceFn(w2, []opt.Expr{j.Replace[0], j.Replace[2]})
		}

		{
			w2 := j.H.f.ConstructZigzagJoin(j.On,
				&memo.ZigzagJoinPrivate{
					LeftTable:   j.H.scanExpr.(*memo.ScanExpr).Table,
					RightTable:  j.Replace[0].(*memo.ScanExpr).Table,
					LeftEqCols:  opt.ColList{j.H.scanExpr.(*memo.ScanExpr).Table.ColumnID(0)},
					RightEqCols: opt.ColList{j.Replace[0].(*memo.ScanExpr).Table.ColumnID(0)},
					LeftIndex:   0,
					RightIndex:  0,
					Cols:        j.H.scanExpr.(*memo.ScanExpr).Cols,
				})
			testExprInterfaceFn(w2, []opt.Expr{j.Replace[2]})
		}
	}
}

//func TestExprDistinctOnExpr(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	var helper testExprHelper
//	if err := helper.initTest(); err != nil {
//		t.Fatal(err)
//	} else {
//		w := helper.f.ConstructDistinctOn(helper.scanExpr,
//			j.H.scanExpr, j.Right, j.On, &memo.JoinPrivate{},
//		)
//		testExprInterfaceFn(w, j.Replace)
//	}
//}
