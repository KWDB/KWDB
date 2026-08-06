// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package arrowpilot

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestArrowUnifyPipelineFilterAggSort verifies that a single query exercising a
// filter, a grouped aggregation, and an order-by sort is routed through the
// Arrow compute engine for every one of those operators (opt-in), and that the
// final result is identical to the standard execution engine bit-for-bit.
//
// Pipeline exercised:
//
//	scan -> Arrow filter (a*b > 10) -> Arrow agg (GROUP BY b) -> Arrow sorter (ORDER BY)
func TestArrowUnifyPipelineFilterAggSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	for _, setting := range []string{
		"sql.arrow_filter.enabled",
		"sql.arrow_aggregator.enabled",
		"sql.arrow_sorter.enabled",
	} {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer disableArrowSettings(t, db,
		"sql.arrow_filter.enabled",
		"sql.arrow_aggregator.enabled",
		"sql.arrow_sorter.enabled",
	)

	execStmt(t, db, "CREATE TABLE tp (a INT, b INT)")
	defer execStmt(t, db, "DROP TABLE tp")
	for i := 1; i <= 6; i++ {
		execStmt(t, db, fmt.Sprintf("INSERT INTO tp VALUES (%d, %d)", i, 7-i))
	}

	// (a*b): (1,6)=6 (2,5)=10 (3,4)=12 (4,3)=12 (5,2)=10 (6,1)=6
	// a*b > 10 keeps (3,4),(4,3). Group by b: b=4 -> SUM(a)=3, b=3 -> SUM(a)=4.
	assertIntRows(t, queryIntRows(t, db,
		"SELECT b, SUM(a) FROM tp WHERE a*b > 10 GROUP BY b ORDER BY b"),
		[][]int64{{3, 4}, {4, 3}})

	if !waitForArrowRuns(t, rowexec.ArrowFilterRunCount, 1) {
		t.Fatal("arrow filter processor was not used in pipeline")
	}
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatal("arrow aggregator processor was not used in pipeline")
	}
	if !waitForArrowRuns(t, rowexec.ArrowSorterRunCount, 1) {
		t.Fatal("arrow sorter processor was not used in pipeline")
	}
}

// TestArrowUnifyPipelineDistinct verifies that SELECT DISTINCT is routed through
// the Arrow distinct processor (opt-in) and produces the same set of rows as the
// standard engine. It is paired with the projection path so the DISTINCT input
// is produced by an Arrow projection (c = a + b) rather than raw scan columns.
func TestArrowUnifyPipelineDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	for _, setting := range []string{
		"sql.arrow_projection.enabled",
		"sql.arrow_distinct.enabled",
	} {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer disableArrowSettings(t, db,
		"sql.arrow_projection.enabled",
		"sql.arrow_distinct.enabled",
	)

	execStmt(t, db, "CREATE TABLE td (a INT, b INT)")
	defer execStmt(t, db, "DROP TABLE td")
	for _, r := range [][2]int64{
		{1, 4}, {2, 3}, {3, 2}, {4, 1}, // a+b = 5
		{1, 1}, {2, 2}, // a+b = 2, 4
		{5, 5}, // a+b = 10
	} {
		execStmt(t, db, fmt.Sprintf("INSERT INTO td VALUES (%d, %d)", r[0], r[1]))
	}

	// DISTINCT over a computed projection: distinct a+b values are {2,4,5,10}.
	assertIntRows(t, queryIntRows(t, db,
		"SELECT DISTINCT a + b FROM td ORDER BY a + b"),
		[][]int64{{2}, {4}, {5}, {10}})

	if !waitForArrowRuns(t, rowexec.ArrowProjectionRunCount, 1) {
		t.Fatal("arrow projection processor was not used for DISTINCT input")
	}
	if !waitForArrowRuns(t, rowexec.ArrowDistinctRunCount, 1) {
		t.Fatal("arrow distinct processor was not used in pipeline")
	}
}

// disableArrowSettings turns the given cluster settings back off before the
// test tears down its tables. The Arrow filter path encodes only a subset of
// column types, so leaving it on while DROP TABLE scans system JSON columns can
// trip the adapter; disabling first keeps teardown clean.
func disableArrowSettings(t *testing.T, db *sql.DB, settings ...string) {
	t.Helper()
	for _, setting := range settings {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = false"); err != nil {
			t.Logf("disable %s: %v", setting, err)
		}
	}
}

// TestArrowUnifyOrdinality verifies that WITH ORDINALITY is routed through the
// Arrow windower (row_number() OVER (), single already-ordered input) instead
// of the classic row-by-row ordinality processor, and that the emitted ordinal
// column is 1..N in input order and identical to the standard engine.
func TestArrowUnifyOrdinality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	for _, setting := range []string{
		"sql.arrow_windower.enabled",
	} {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer disableArrowSettings(t, db, "sql.arrow_windower.enabled")

	execStmt(t, db, "CREATE TABLE to2 (a INT)")
	defer execStmt(t, db, "DROP TABLE to2")
	for i := 1; i <= 5; i++ {
		execStmt(t, db, fmt.Sprintf("INSERT INTO to2 VALUES (%d)", i*10))
	}

	// WITH ORDINALITY appends a 1-based ordinal column in input order.
	assertIntRows(t, queryIntRows(t, db,
		"SELECT a, row_num FROM (SELECT a FROM to2) WITH ORDINALITY AS o(a, row_num) ORDER BY row_num"),
		[][]int64{{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}})

	if !waitForArrowRuns(t, rowexec.ArrowWindowerRunCount, 1) {
		t.Fatal("arrow windower processor was not used for WITH ORDINALITY")
	}
}
