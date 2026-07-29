// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package arrowpilot

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

// TestMain registers the test server factory so this package can spin up a
// real (in-process) KWDB server for end-to-end verification.
func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

// TestArrowProjectionE2EWithRealSQL drives the unified execution path with the
// output of a real SQL query executed against a real (in-process) KWDB server.
//
// End-to-end check:
//  1. a real SQL query (CREATE/INSERT/SELECT) produces real rows, with real
//     column types and encodings managed by the actual SQL engine;
//  2. those rows flow through RowToArrowConverter -> ArrowProjection (Arrow
//     compute "add") exactly as they would inside the unified engine;
//  3. the projected result is compared, row by row, against the result of the
//     same projection computed by the real SQL engine (SELECT a+b FROM t).
//
// This wires the unified path to the *data* produced by a real query. Routing a
// query's own projection operator through arrowProjection (so the execution
// engine itself uses the unified path) is the next integration step.
func TestArrowProjectionE2EWithRealSQL(t *testing.T) {
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("CREATE TABLE t (a INT, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)"); err != nil {
		t.Fatal(err)
	}

	// Standard answer, computed by the real SQL engine.
	ansRows, err := db.Query("SELECT a + b AS c FROM t ORDER BY a")
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, 0, 6)
	for ansRows.Next() {
		var v int64
		if err := ansRows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		want = append(want, strconv.FormatInt(v, 10))
	}
	ansRows.Close()

	// Real rows produced by a real SQL query, fed into the unified path.
	srcRows, err := db.Query("SELECT a, b FROM t ORDER BY a")
	if err != nil {
		t.Fatal(err)
	}
	intTyp := types.Int
	mk := func(v tree.Datum) sqlbase.EncDatum { return sqlbase.DatumToEncDatum(intTyp, v) }
	encRows := make(sqlbase.EncDatumRows, 0, 6)
	for srcRows.Next() {
		var a, b int64
		if err := srcRows.Scan(&a, &b); err != nil {
			t.Fatal(err)
		}
		encRows = append(encRows, sqlbase.EncDatumRow{
			mk(tree.NewDInt(tree.DInt(a))),
			mk(tree.NewDInt(tree.DInt(b))),
		})
	}
	srcRows.Close()

	alloc := memory.NewGoAllocator()
	conv := rowexec.NewRowToArrowConverter(alloc, []*types.T{intTyp, intTyp}, encRows)
	conv.Init(ctx)
	rec, done, err := conv.Next(ctx)
	if err != nil || done || rec == nil {
		t.Fatalf("rowToArrow: done=%v err=%v rec=%v", done, err, rec)
	}
	src := rowexec.NewArrowRecordSource(alloc, rec)
	proj := rowexec.NewArrowProjection(alloc, src, []rowexec.ArrowProjectionSpec{
		{OutputName: "c", Func: "add", Args: []rowexec.ArrowArg{{ColName: "col0"}, {ColName: "col1"}}},
	})
	proj.Init(ctx)
	vals, err := rowexec.ArrowProjectionResultInt64(proj, ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if len(vals) != len(want) {
		t.Fatalf("row count mismatch: got %d want %d", len(vals), len(want))
	}
	for i := range want {
		got := strconv.FormatInt(vals[i], 10)
		if got != want[i] {
			t.Fatalf("row %d: unified path got %s, SQL engine got %s", i, got, want[i])
		}
	}
}

// TestArrowProjectionPlannerIntegration verifies that a real SQL query
// (SELECT a+b FROM t) actually executes its projection through the Arrow
// compute engine once the sql.arrow_projection.enabled setting is turned on.
//
// This is the end-to-end proof that the unified path is wired into the planner's
// real projection execution: the query's own render operator is replaced by a
// dedicated arrowProjectionProcessor stage, rather than being driven only by
// test data as in TestArrowProjectionE2EWithRealSQL.
func TestArrowProjectionPlannerIntegration(t *testing.T) {
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("CREATE TABLE t (a INT, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)"); err != nil {
		t.Fatal(err)
	}

	// Turn on the arrow projection acceleration. The cluster setting is
	// eventually-consistent, so retry the query until the planner picks it up.
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = true"); err != nil {
		t.Fatal(err)
	}

	before := rowexec.ArrowProjectionRunCount()
	var got []int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := db.Query("SELECT a + b FROM t")
		if err != nil {
			t.Fatal(err)
		}
		got = got[:0]
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			got = append(got, int(v))
		}
		rows.Close()
		if rowexec.ArrowProjectionRunCount() > before {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	after := rowexec.ArrowProjectionRunCount()
	if after <= before {
		t.Fatalf("arrow projection processor was not used by the query (runs before=%d after=%d)", before, after)
	}

	sort.Ints(got)
	want := []int{11, 22, 33, 44, 55, 66}
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: arrow path got %d, want %d", i, got[i], want[i])
		}
	}
}

// TestArrowProjectionPlannerComputeExprs verifies that the planner routes a
// variety of arrow-computable render expressions through the arrowProjection
// processor: multi-column output, constant operands, and unary minus. Each case
// asserts both that the arrow path was used and that the produced values are
// correct.
func TestArrowProjectionPlannerComputeExprs(t *testing.T) {
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("CREATE TABLE t (a INT, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = true"); err != nil {
		t.Fatal(err)
	}

	// Each case: query + expected rows (in ascending order of the first output
	// column, since no ORDER BY is used so the row order isn't guaranteed, and
	// ORDER BY would risk introducing a merge ordering that disables the path).
	cases := []struct {
		q    string
		want [][]int64
	}{
		{"SELECT a + b FROM t", [][]int64{{11}, {22}, {33}, {44}, {55}, {66}}},
		{"SELECT a + b, a * 2 FROM t", [][]int64{{11, 2}, {22, 4}, {33, 6}, {44, 8}, {55, 10}, {66, 12}}},
		{"SELECT -a FROM t", [][]int64{{-1}, {-2}, {-3}, {-4}, {-5}, {-6}}},
		{"SELECT a + 1 FROM t", [][]int64{{2}, {3}, {4}, {5}, {6}, {7}}},
		{"SELECT a + b, a FROM t", [][]int64{{11, 1}, {22, 2}, {33, 3}, {44, 4}, {55, 5}, {66, 6}}},
	}

	for _, c := range cases {
		base := rowexec.ArrowProjectionRunCount()
		var got [][]int64
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			rows, err := db.Query(c.q)
			if err != nil {
				t.Fatalf("query %q: %v", c.q, err)
			}
			got = scanIntRows(t, rows)
			rows.Close()
			if rowexec.ArrowProjectionRunCount() > base {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if rowexec.ArrowProjectionRunCount() <= base {
			t.Fatalf("arrow projection processor was not used by query %q", c.q)
		}
		assertIntRows(t, got, c.want)
	}
}

// scanIntRows reads all columns of a result set as int64, returning one slice
// per row.
func scanIntRows(t *testing.T, rows *sql.Rows) [][]int64 {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]int64
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		holders := make([]interface{}, len(cols))
		for i := range ptrs {
			ptrs[i] = &holders[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]int64, len(cols))
		for i := range holders {
			v, ok := holders[i].(int64)
			if !ok {
				t.Fatalf("expected int64 column, got %T", holders[i])
			}
			row[i] = v
		}
		out = append(out, row)
	}
	return out
}

// assertIntRows sorts the got rows by their first column and compares them,
// cell by cell, against want.
func assertIntRows(t *testing.T, got, want [][]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d want %d", len(got), len(want))
	}
	sort.Slice(got, func(i, j int) bool { return got[i][0] < got[j][0] })
	sort.Slice(want, func(i, j int) bool { return want[i][0] < want[j][0] })
	for r := range want {
		if len(got[r]) != len(want[r]) {
			t.Fatalf("row %d column count mismatch: got %d want %d", r, len(got[r]), len(want[r]))
		}
		for c := range want[r] {
			if got[r][c] != want[r][c] {
				t.Fatalf("row %d col %d: got %d want %d", r, c, got[r][c], want[r][c])
			}
		}
	}
}

// queryIntRows runs q and reads every column as int64. NULL columns become the
// sentinel -9999 so left-outer-join results can be compared.
func queryIntRows(t *testing.T, db *sql.DB, q string) [][]int64 {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]int64
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		vals := make([]sql.NullInt64, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		row := make([]int64, len(cols))
		for i := range vals {
			if !vals[i].Valid {
				row[i] = -9999
			} else {
				row[i] = vals[i].Int64
			}
		}
		out = append(out, row)
	}
	return out
}

// queryFloatRows runs q and reads every column as float64. NULL columns become
// the sentinel -9999. It is used for AVG assertions (SQL AVG over integers
// returns DECIMAL, so the query is cast to FLOAT for comparison).
func queryFloatRows(t *testing.T, db *sql.DB, q string) [][]float64 {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]float64
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		vals := make([]sql.NullFloat64, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		row := make([]float64, len(cols))
		for i := range vals {
			if !vals[i].Valid {
				row[i] = -9999
			} else {
				row[i] = vals[i].Float64
			}
		}
		out = append(out, row)
	}
	return out
}

func assertFloatRows(t *testing.T, got, want [][]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d want %d", len(got), len(want))
	}
	sort.Slice(got, func(i, j int) bool { return got[i][0] < got[j][0] })
	sort.Slice(want, func(i, j int) bool { return want[i][0] < want[j][0] })
	for r := range want {
		if len(got[r]) != len(want[r]) {
			t.Fatalf("row %d column count mismatch: got %d want %d", r, len(got[r]), len(want[r]))
		}
		for c := range want[r] {
			if math.Abs(got[r][c]-want[r][c]) > 1e-9 {
				t.Fatalf("row %d col %d: got %v want %v", r, c, got[r][c], want[r][c])
			}
		}
	}
}

func execStmt(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// waitForArrowRuns polls get() until it reports at least min runs, confirming
// the opt-in Arrow processor was actually used by a query.
func waitForArrowRuns(t *testing.T, get func() int64, min int64) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if get() >= min {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return get() >= min
}

// TestArrowUnifyFilterAggJoin verifies that filter, aggregation and join
// operators are routed through the Arrow compute engine (opt-in) for real
// SQL queries, and that their results match the standard execution.
func TestArrowUnifyFilterAggJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	for _, setting := range []string{
		"sql.arrow_filter.enabled",
		"sql.arrow_aggregator.enabled",
		"sql.arrow_join.enabled",
	} {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}

	execStmt(t, db, "CREATE TABLE t (a INT, b INT)")
	execStmt(t, db, "CREATE TABLE j1 (k INT, v INT)")
	execStmt(t, db, "CREATE TABLE j2 (k INT, w INT)")
	tData := [][2]int64{{1, 6}, {2, 5}, {3, 4}, {4, 3}, {5, 2}, {6, 1}}
	for _, r := range tData {
		execStmt(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", r[0], r[1]))
	}
	execStmt(t, db, "INSERT INTO j1 VALUES (1, 10), (2, 20), (3, 30)")
	execStmt(t, db, "INSERT INTO j2 VALUES (1, 100), (2, 200), (4, 400)")

	runFilter := func(q string, want [][]int64) {
		assertIntRows(t, queryIntRows(t, db, q), want)
	}

	// --- FILTER ---
	// These filters reference computed expressions so they are applied as
	// PostProcess filters (not scan constraints) and reach the Arrow path.
	runFilter("SELECT a FROM t WHERE a * 2 > b", [][]int64{{3}, {4}, {5}, {6}})
	runFilter("SELECT a FROM t WHERE a + b > a * 2", [][]int64{{1}, {2}, {3}})
	runFilter("SELECT a FROM t WHERE a * 2 > b AND a < 5", [][]int64{{3}, {4}})
	if !waitForArrowRuns(t, rowexec.ArrowFilterRunCount, 1) {
		t.Fatal("arrow filter processor was not used")
	}

	// --- AGGREGATION ---
	runFilter("SELECT SUM(a), COUNT(*), MIN(b), MAX(b) FROM t", [][]int64{{21, 6, 1, 6}})
	runFilter("SELECT b, SUM(a) FROM t GROUP BY b", [][]int64{
		{1, 6}, {2, 5}, {3, 4}, {4, 3}, {5, 2}, {6, 1},
	})
	runFilter("SELECT b, COUNT(*) FROM t GROUP BY b", [][]int64{
		{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1},
	})
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatal("arrow aggregator processor was not used")
	}

	// NULL handling (verifies the pure-Arrow kernel skips nulls like SQL):
	//   - SUM/COUNT(col) ignore NULL, COUNT(*) counts every row,
	//   - a group whose only value is NULL aggregates to NULL (-9999 sentinel).
	if _, err := db.Exec("INSERT INTO t VALUES (NULL, 7)"); err != nil {
		t.Fatal(err)
	}
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatal("arrow aggregator processor was not used")
	}
	runFilter("SELECT SUM(a), COUNT(a), COUNT(*), MIN(b), MAX(b) FROM t", [][]int64{{21, 6, 7, 1, 7}})
	runFilter("SELECT b, SUM(a) FROM t WHERE b = 7 GROUP BY b", [][]int64{{7, -9999}})

	// --- AVG (decimal accumulator) ---
	// SQL AVG over integers returns DECIMAL, so the Arrow mean kernel must emit a
	// decimal value (it accumulates the sum as a high-precision apd.Decimal and
	// divides with tree.DecimalCtx, exactly like the standard engine). We cast to
	// FLOAT only for comparison. The extra (7, 8) row makes the global average of
	// non-null a (1..7) exactly 4.
	if _, err := db.Exec("INSERT INTO t VALUES (7, 8)"); err != nil {
		t.Fatal(err)
	}
	before := rowexec.ArrowAggRunCount()
	runFilterF := func(q string, want [][]float64) {
		assertFloatRows(t, queryFloatRows(t, db, q), want)
	}
	runFilterF("SELECT CAST(AVG(a) AS FLOAT) FROM t", [][]float64{{4}})
	runFilterF("SELECT CAST(AVG(a) AS FLOAT) FROM t WHERE a IN (1, 2)", [][]float64{{1.5}})
	runFilterF("SELECT b, CAST(AVG(a) AS FLOAT) FROM t WHERE b <= 3 GROUP BY b", [][]float64{
		{1, 6}, {2, 5}, {3, 4},
	})
	if rowexec.ArrowAggRunCount() <= before {
		t.Fatal("arrow aggregator processor was not used for AVG")
	}

	// --- JOIN (inner + left outer) ---
	runFilter("SELECT j1.v, j2.w FROM j1 JOIN j2 ON j1.k = j2.k", [][]int64{
		{10, 100}, {20, 200},
	})
	runFilter("SELECT j1.v, j2.w FROM j1 LEFT JOIN j2 ON j1.k = j2.k", [][]int64{
		{10, 100}, {20, 200}, {30, -9999},
	})
	runFilter("SELECT j1.v, j2.w FROM j1 RIGHT JOIN j2 ON j1.k = j2.k", [][]int64{
		{10, 100}, {20, 200}, {-9999, 400},
	})
	runFilter("SELECT j1.v, j2.w FROM j1 FULL OUTER JOIN j2 ON j1.k = j2.k", [][]int64{
		{10, 100}, {20, 200}, {30, -9999}, {-9999, 400},
	})
	if !waitForArrowRuns(t, rowexec.ArrowJoinRunCount, 1) {
		t.Fatal("arrow join processor was not used")
	}
}

// TestArrowUnifyJoinNonEqui verifies that an inner join with a non-equi onExpr
// (column-vs-const, column-vs-column, AND-combined, and a fully-excluding
// condition) is routed through the Arrow join processor and evaluated as a
// post-filter stage (§7.4), matching the standard engine result bit-for-bit.
func TestArrowUnifyJoinNonEqui(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = true"); err != nil {
		t.Fatalf("set arrow_join enabled: %v", err)
	}

	execStmt(t, db, "CREATE TABLE jl (k INT, v INT)")
	execStmt(t, db, "CREATE TABLE jr (k INT, w INT)")
	defer func() {
		// Only sql.arrow_join.enabled is on in this test, so the JsonFamily
		// system-table pitfall from the global arrow_filter setting does not
		// apply; disable before DROP anyway for safety.
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = false")
		execStmt(t, db, "DROP TABLE jl")
		execStmt(t, db, "DROP TABLE jr")
	}()

	execStmt(t, db, "INSERT INTO jl VALUES (1, 10), (2, 20), (3, 30)")
	execStmt(t, db, "INSERT INTO jr VALUES (1, 100), (2, 200), (4, 400)")

	runJoin := func(q string, want [][]int64) {
		assertIntRows(t, queryIntRows(t, db, q), want)
	}

	// column-vs-const non-equi onExpr (only (2,20) matches jl.v > 15).
	runJoin("SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k AND jl.v > 15", [][]int64{{20, 200}})
	// column-vs-column non-equi onExpr (jl.v < jr.w for every matched pair).
	runJoin("SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k AND jl.v < jr.w", [][]int64{
		{10, 100}, {20, 200},
	})
	// AND-combined non-equi onExpr.
	runJoin("SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k AND jl.v > 15 AND jr.w < 300", [][]int64{{20, 200}})
	// non-equi onExpr that excludes every matched pair.
	runJoin("SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k AND jl.v > 100", [][]int64{})

	// The non-equi onExpr must have been evaluated by the arrow join post-filter
	// stage (§7.4); otherwise the query would have used the standard joiner.
	if rowexec.ArrowJoinOnFilterRunCount() == 0 {
		t.Fatal("arrow join non-equi onExpr post-filter stage was not used")
	}
}

// TestArrowUnifyDecimalAgg verifies that SUM/MIN/MAX/AVG over DECIMAL columns
// (including a DECIMAL grouping key) are evaluated by the pure-Arrow aggregate
// engine and match the standard reference engine bit-for-bit.
func TestArrowUnifyDecimalAgg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = true"); err != nil {
		t.Fatalf("set aggregator enabled: %v", err)
	}

	execStmt(t, db, "CREATE TABLE td (a INT, d DECIMAL(12,2))")
	defer execStmt(t, db, "DROP TABLE td")
	for _, r := range [][2]interface{}{
		{1, 1.00}, {2, 2.00}, {3, 3.00}, {4, 4.00},
		{5, 5.00}, {6, 6.00}, {7, 7.00}, {8, 8.00}, {9, nil},
	} {
		if r[1] == nil {
			execStmt(t, db, fmt.Sprintf("INSERT INTO td VALUES (%d, NULL)", r[0]))
		} else {
			execStmt(t, db, fmt.Sprintf("INSERT INTO td VALUES (%d, %v)", r[0], r[1]))
		}
	}

	assertDec := func(q string, want [][]float64) {
		assertFloatRows(t, queryFloatRows(t, db, q), want)
	}

	// Global aggregates: NULL is skipped exactly like the standard engine.
	assertDec("SELECT CAST(SUM(d) AS FLOAT), CAST(MIN(d) AS FLOAT), CAST(MAX(d) AS FLOAT), CAST(AVG(d) AS FLOAT) FROM td",
		[][]float64{{36, 1, 8, 4.5}})

	// Filtered by an integer column; decimal aggregate over the surviving rows.
	assertDec("SELECT CAST(SUM(d) AS FLOAT), CAST(AVG(d) AS FLOAT) FROM td WHERE a <= 4",
		[][]float64{{10, 2.5}})

	// Grouped by an integer key with a decimal aggregate.
	assertDec("SELECT a % 2, CAST(SUM(d) AS FLOAT) FROM td WHERE a <= 8 GROUP BY a % 2",
		[][]float64{{0, 20}, {1, 16}})

	// Grouped by a DECIMAL key (exercises the decimal group-key path). The NULL
	// row is filtered out on the integer column a <= 8 so every group is exact.
	assertDec("SELECT CAST(d AS FLOAT), CAST(SUM(d) AS FLOAT) FROM td WHERE a <= 8 GROUP BY d ORDER BY d",
		[][]float64{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}})

	before := rowexec.ArrowAggRunCount()
	// Re-run one decimal aggregate to confirm the Arrow aggregator is used.
	assertDec("SELECT CAST(AVG(d) AS FLOAT) FROM td", [][]float64{{4.5}})
	if rowexec.ArrowAggRunCount() <= before {
		t.Fatal("arrow aggregator processor was not used for decimal aggregates")
	}
	_ = ctx
}

// TestArrowUnifyTimestampAgg verifies that MIN/MAX/COUNT over TIMESTAMP columns
// (including a TIMESTAMP grouping key) are evaluated by the pure-Arrow aggregate
// engine and match the standard reference engine. Timestamps are rendered via a
// downstream CAST to STRING (mirroring how the decimal e2e compares SUM/AVG); we
// deliberately avoid fusing a comparison such as (MIN(ts) = ...) into the
// aggregator's post-process, which breaks flow setup.
func TestArrowUnifyTimestampAgg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = true"); err != nil {
		t.Fatalf("set aggregator enabled: %v", err)
	}

	execStmt(t, db, "CREATE TABLE tt (a INT, ts TIMESTAMP)")
	defer execStmt(t, db, "DROP TABLE tt")
	for _, r := range [][2]interface{}{
		{1, "2021-01-01 00:00:00"},
		{2, "2021-01-02 00:00:00"},
		{3, "2021-01-03 00:00:00"},
		{4, "2021-01-01 00:00:00"}, // duplicate ts to exercise the timestamp group key
		{5, nil},
	} {
		if r[1] == nil {
			execStmt(t, db, fmt.Sprintf("INSERT INTO tt VALUES (%d, NULL)", r[0]))
		} else {
			execStmt(t, db, fmt.Sprintf("INSERT INTO tt VALUES (%d, '%s')", r[0], r[1]))
		}
	}

	// Global MIN/MAX over the timestamp column, rendered as STRING for stable
	// comparison (the engine normalizes timestamps to UTC, so +00:00 appears).
	assertStr(t, db, "SELECT MIN(ts)::STRING, MAX(ts)::STRING FROM tt",
		[][]string{{"2021-01-01 00:00:00+00:00", "2021-01-03 00:00:00+00:00"}})
	assertIntRows(t, queryIntRows(t, db, "SELECT COUNT(ts) FROM tt"), [][]int64{{4}})

	// Grouped by the timestamp key; the duplicate 2021-01-01 yields count 2.
	assertStr(t, db, "SELECT ts::STRING FROM tt WHERE a <= 4 GROUP BY ts ORDER BY ts",
		[][]string{
			{"2021-01-01 00:00:00+00:00"},
			{"2021-01-02 00:00:00+00:00"},
			{"2021-01-03 00:00:00+00:00"},
		})
	assertIntRows(t, queryIntRows(t, db, "SELECT COUNT(*) FROM tt WHERE a <= 4 GROUP BY ts ORDER BY ts"),
		[][]int64{{2}, {1}, {1}})

	// Per-group MIN/MAX (ident pass-through) equal the group key.
	assertStr(t, db, "SELECT MIN(ts)::STRING FROM tt WHERE a <= 4 GROUP BY ts ORDER BY ts",
		[][]string{
			{"2021-01-01 00:00:00+00:00"},
			{"2021-01-02 00:00:00+00:00"},
			{"2021-01-03 00:00:00+00:00"},
		})

	before := rowexec.ArrowAggRunCount()
	// Re-run one timestamp aggregate to confirm the Arrow aggregator is used.
	assertStr(t, db, "SELECT MIN(ts)::STRING FROM tt", [][]string{{"2021-01-01 00:00:00+00:00"}})
	if rowexec.ArrowAggRunCount() <= before {
		t.Fatal("arrow aggregator processor was not used for timestamp aggregates")
	}
	_ = ctx
}

// TestArrowUnifyFilterFuncs verifies that the opt-in Arrow filter engine
// supports string predicates beyond plain comparisons: SQL LIKE / NOT LIKE /
// ILIKE (evaluated by a Go kernel since Arrow compute v17 has no match_like),
// and CAST(col AS ...) type conversion inside the predicate (routed through a
// Go cast kernel, e.g. CAST(i AS STRING) LIKE '1%').
func TestArrowUnifyFilterFuncs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = true"); err != nil {
		t.Fatalf("set filter enabled: %v", err)
	}

	execStmt(t, db, "CREATE TABLE ff (a INT, name STRING)")
	defer func() {
		// Disable arrow unification before DROP: the internal row-count scan
		// issued by DROP TABLE touches system tables with JSON columns that the
		// Arrow adapter does not yet encode.
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = false")
		execStmt(t, db, "DROP TABLE ff")
	}()
	for _, r := range [][2]interface{}{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
		{4, "dave"},
		{5, "amy"},
	} {
		execStmt(t, db, fmt.Sprintf("INSERT INTO ff VALUES (%d, '%s')", r[0], r[1]))
	}

	// LIKE prefix match.
	assertStr(t, db, "SELECT name FROM ff WHERE name LIKE 'a%' ORDER BY a",
		[][]string{{"alice"}, {"amy"}})
	// NOT LIKE.
	assertStr(t, db, "SELECT name FROM ff WHERE name NOT LIKE 'a%' ORDER BY a",
		[][]string{{"bob"}, {"carol"}, {"dave"}})
	// ILIKE case-insensitive prefix match.
	assertStr(t, db, "SELECT name FROM ff WHERE name ILIKE 'A%' ORDER BY a",
		[][]string{{"alice"}, {"amy"}})
	// CAST(int AS STRING) then LIKE.
	assertStr(t, db, "SELECT name FROM ff WHERE CAST(a AS STRING) LIKE '1%' ORDER BY a",
		[][]string{{"alice"}})
	// CAST(int AS STRING) then string equality.
	assertStr(t, db, "SELECT name FROM ff WHERE CAST(a AS STRING) = '2' ORDER BY a",
		[][]string{{"bob"}})

	before := rowexec.ArrowFilterRunCount()
	// Re-run one LIKE filter to confirm the Arrow filter engine is used.
	assertStr(t, db, "SELECT name FROM ff WHERE name LIKE 'a%' ORDER BY a",
		[][]string{{"alice"}, {"amy"}})
	if rowexec.ArrowFilterRunCount() <= before {
		t.Fatal("arrow filter processor was not used for LIKE/CAST predicates")
	}
	_ = ctx
}

func queryStringRows(t *testing.T, db *sql.DB, q string) [][]string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := [][]string{}
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		vals := make([]sql.NullString, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		row := make([]string, len(cols))
		for i := range vals {
			if !vals[i].Valid {
				row[i] = "<null>"
			} else {
				row[i] = vals[i].String
			}
		}
		out = append(out, row)
	}
	return out
}

func assertStringRows(t *testing.T, got, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d want %d\n got=%v", len(got), len(want), got)
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("col count mismatch row %d: got %d want %d\n got=%v want=%v", i, len(got[i]), len(want[i]), got[i], want[i])
		}
		for j := range got[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %q want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}

func assertStr(t *testing.T, db *sql.DB, q string, want [][]string) {
	t.Helper()
	assertStringRows(t, queryStringRows(t, db, q), want)
}
