// Copyright 2024 The KWDB Authors.
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
// See the License for the specific language governing to specific
// permissions and limitations under the License.

package arrowpilot

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// TestArrowUnifyWindow verifies that the supported subset of window functions
// (no-frame partition aggregates) is routed through the Arrow windower and
// produces results identical to the standard engine.
func TestArrowUnifyWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	settings := []string{
		"sql.arrow_aggregator.enabled",
		"sql.arrow_sorter.enabled",
		"sql.arrow_distinct.enabled",
		"sql.arrow_filter.enabled",
		"sql.arrow_projection.enabled",
		"sql.arrow_windower.enabled",
	}
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	for _, setting := range settings {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer func() {
		for _, setting := range settings {
			_, _ = db.Exec("SET CLUSTER SETTING " + setting + " = false")
		}
	}()

	execStmt(t, db, "CREATE TABLE sales (region STRING, rep STRING, amount INT)")
	// region a: amounts 10,20,30 ; region b: 5,15
	execStmt(t, db, "INSERT INTO sales VALUES ('a','x',10),('a','y',20),('a','z',30),('b','p',5),('b','q',15)")

	intRows := func(q string) [][]int64 { return queryIntRows(t, db, q) }

	// SUM OVER (PARTITION BY region ORDER BY amount) -> running total per region.
	// Assert numerically (region strings are not int-comparable, order by amount
	// within each region).
	assertIntRows(t, intRows(
		"SELECT amount, SUM(amount) OVER (PARTITION BY region ORDER BY amount) AS r FROM sales ORDER BY region, amount"),
		[][]int64{
			{5, 5},
			{15, 20},
			{10, 10},
			{20, 30},
			{30, 60},
		})

	// COUNT OVER (PARTITION BY region) -> total rows per region (no order).
	assertIntRows(t, intRows(
		"SELECT amount, COUNT(*) OVER (PARTITION BY region) AS c FROM sales ORDER BY region, amount"),
		[][]int64{
			{5, 2},
			{15, 2},
			{10, 3},
			{20, 3},
			{30, 3},
		})

	// AVG OVER (PARTITION BY region ORDER BY amount) -> running avg per region.
	assertIntRows(t, intRows(
		"SELECT amount, AVG(amount) OVER (PARTITION BY region ORDER BY amount) AS a FROM sales ORDER BY region, amount"),
		[][]int64{
			{5, 5},
			{15, 10},  // (5+15)/2 = 10
			{10, 10},  // 10
			{20, 15},  // (10+20)/2 = 15
			{30, 20},  // (10+20+30)/3 = 20
		})

	if !waitForArrowRuns(t, rowexec.ArrowWindowerRunCount, 1) {
		t.Fatalf("arrow windower processor was not used for window functions (runcount=%d)", rowexec.ArrowWindowerRunCount())
	}
}

// TestArrowUnifyWindowFallback verifies that window functions outside the
// supported Arrow subset (ranking functions like row_number, and explicit
// frames like ROWS) safely fall back to the classic windower while still
// producing correct results.
func TestArrowUnifyWindowFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	settings := []string{
		"sql.arrow_aggregator.enabled",
		"sql.arrow_sorter.enabled",
		"sql.arrow_distinct.enabled",
		"sql.arrow_filter.enabled",
		"sql.arrow_projection.enabled",
		"sql.arrow_windower.enabled",
	}
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	for _, setting := range settings {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer func() {
		for _, setting := range settings {
			_, _ = db.Exec("SET CLUSTER SETTING " + setting + " = false")
		}
	}()

	execStmt(t, db, "CREATE TABLE t2 (g STRING, v INT)")
	execStmt(t, db, "INSERT INTO t2 VALUES ('a',10),('a',30),('a',20),('b',5),('b',15)")

	intRows := func(q string) [][]int64 { return queryIntRows(t, db, q) }

	// ROW_NUMBER is a ranking function -> falls back, but must be correct.
	assertIntRows(t, intRows(
		"SELECT v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM t2 ORDER BY g, v"),
		[][]int64{
			{5, 1},
			{15, 2},
			{10, 1},
			{20, 2},
			{30, 3},
		})

	// Explicit ROWS frame -> falls back, must be correct (running sum of
	// current row only, since frame is ROWS BETWEEN CURRENT ROW AND CURRENT ROW).
	assertIntRows(t, intRows(
		"SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS s FROM t2 ORDER BY g, v"),
		[][]int64{
			{5, 5},
			{15, 15},
			{10, 10},
			{20, 20},
			{30, 30},
		})
}

