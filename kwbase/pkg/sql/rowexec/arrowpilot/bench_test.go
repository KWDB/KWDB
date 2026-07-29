// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the details.

package arrowpilot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// benchExec runs a DDL/DML statement, failing the benchmark on error.
func benchExec(b testing.TB, db *sql.DB, stmt string) {
	if _, err := db.Exec(stmt); err != nil {
		b.Fatalf("exec %q: %v", stmt, err)
	}
}

// consumeQuery runs q and fully drains the result set, discarding the values.
// It is used inside benchmark loops so the measured cost covers query
// execution plus (de)serialization of the full result, exactly as an
// application would read it. This makes the Native vs Arrow comparison fair:
// both paths pay the same wire/scan cost for the same rows.
func consumeQuery(b *testing.B, db *sql.DB, q string) {
	rows, err := db.Query(q)
	if err != nil {
		b.Fatalf("query %q: %v", q, err)
	}
	cols, err := rows.Columns()
	if err != nil {
		b.Fatal(err)
	}
	ptrs := make([]interface{}, len(cols))
	holders := make([]interface{}, len(cols))
	for i := range ptrs {
		ptrs[i] = &holders[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			b.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	rows.Close()
}

// loadIntPairs inserts n (a, b) rows into table t where b is a pseudo-random
// but deterministic function of a so GROUP BY b yields many groups. Rows are
// batched to keep benchmark setup cheap.
func loadIntPairs(b testing.TB, db *sql.DB, n int) {
	const batch = 500
	for start := 1; start <= n; start += batch {
		end := start + batch - 1
		if end > n {
			end = n
		}
		vals := make([]string, 0, end-start+1)
		for i := start; i <= end; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d)", i, (i*7)%1000))
		}
		benchExec(b, db, "INSERT INTO t VALUES "+strings.Join(vals, ", "))
	}
}

// enableArrowAndWarmup flips the opt-in selectors and then runs a warmup query,
// polling the Arrow run-count probe until it confirms the unified path is in
// use. Cluster settings are eventually consistent, so this mirrors the e2e
// tests: a query is only timed as "Arrow" once the planner actually routes it
// through the Arrow processor (proven by the monotonic run-count probe).
func enableArrowAndWarmup(b *testing.B, db *sql.DB, settings []string, probe func() int64, warmup string) {
	for _, s := range settings {
		if _, err := db.Exec("SET CLUSTER SETTING " + s + " = true"); err != nil {
			b.Fatalf("enable %s: %v", s, err)
		}
	}
	base := probe()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		consumeQuery(b, db, warmup)
		if probe() > base {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	b.Fatalf("arrow path not used for warmup %q (probe still %d)", warmup, probe())
}

// benchArrowVsNative spins up a real in-process KWDB server, runs setup (which
// must create the table(s) and load data), then times the same query twice:
//   - "Original": the standard operator (all arrow selectors off, the default
//     execution path) — i.e. the pre-refactor engine;
//   - "Arrow":    the refactored opt-in Arrow unified operator, confirmed via
//     the run-count probe.
//
// Both sub-benchmarks are reported side by side (ns/op) and an extra
// "Original_per_Arrow_x" metric is emitted giving the explicit 原算子 vs
// 重构算子 ratio: >1 means the refactored Arrow operator is faster (§7.5/§7.6/§7.7).
func benchArrowVsNative(b *testing.B, setup func(db *sql.DB), warmup string, settings []string, probe func() int64, query string) {
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	b.StopTimer()
	setup(db)
	b.StartTimer()

	var origNs, arrowNs float64

	b.Run("Original", func(b *testing.B) {
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			consumeQuery(b, db, query)
		}
		origNs = float64(time.Since(start).Nanoseconds()) / float64(b.N)
	})

	b.StopTimer()
	enableArrowAndWarmup(b, db, settings, probe, warmup)
	b.StartTimer()
	b.Run("Arrow", func(b *testing.B) {
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			consumeQuery(b, db, query)
		}
		arrowNs = float64(time.Since(start).Nanoseconds()) / float64(b.N)
	})

	if arrowNs > 0 {
		ratio := origNs / arrowNs
		// Emit the explicit 原算子 vs 重构算子 ratio. Printed as a standalone
		// line (and via b.ReportMetric on the parent result) so it is always
		// visible even though the parent benchmark runs no loop of its own.
		fmt.Printf("REFACTOR_COMPARE %s Original_ns/op=%.0f Arrow_ns/op=%.0f Original_per_Arrow_x=%.3f (%.2fx %s)\n",
			b.Name(), origNs, arrowNs, ratio, ratio,
			map[bool]string{true: "Arrow faster", false: "Arrow slower"}[ratio >= 1])
		b.ReportMetric(ratio, "Original_per_Arrow_x")
	}
}

// BenchmarkArrowFilter compares the refactored Arrow filter operator (重构算子)
// against the original standard operator (原算子) for a scan with a
// computed-expression predicate (forced into a post-filter so it reaches the
// Arrow processor, §7.3).
func BenchmarkArrowFilter(b *testing.B) {
	benchArrowVsNative(b,
		func(db *sql.DB) {
			benchExec(b, db, "CREATE TABLE t (a INT, b INT)")
			loadIntPairs(b, db, 10000)
		},
		"SELECT a FROM t WHERE a * 2 > b",
		[]string{"sql.arrow_filter.enabled"},
		rowexec.ArrowFilterRunCount,
		"SELECT a FROM t WHERE a * 2 > b",
	)
}

// BenchmarkArrowAggregator compares the refactored Arrow aggregator (重构算子)
// against the original standard operator (原算子) for a global (scalar, no
// GROUP BY) SUM/COUNT/MIN/MAX query. This is the aggregate shape that currently
// routes through the Arrow engine; GROUP BY distributed aggregation still falls
// back to the standard engine (see §7.5 / §7.6 in the handoff doc).
func BenchmarkArrowAggregator(b *testing.B) {
	benchArrowVsNative(b,
		func(db *sql.DB) {
			benchExec(b, db, "CREATE TABLE t (a INT, b INT)")
			loadIntPairs(b, db, 10000)
		},
		"SELECT SUM(a), COUNT(*), MIN(b), MAX(b) FROM t",
		[]string{"sql.arrow_aggregator.enabled"},
		rowexec.ArrowAggRunCount,
		"SELECT SUM(a), COUNT(*), MIN(b), MAX(b) FROM t",
	)
}

// BenchmarkArrowJoin compares the refactored Arrow hash-join (重构算子) against
// the original standard operator (原算子) for an inner equi-join.
func BenchmarkArrowJoin(b *testing.B) {
	benchArrowVsNative(b,
		func(db *sql.DB) {
			benchExec(b, db, "CREATE TABLE jl (k INT, v INT)")
			benchExec(b, db, "CREATE TABLE jr (k INT, w INT)")
			loadJoin(b, db, 10000)
		},
		"SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k",
		[]string{"sql.arrow_join.enabled"},
		rowexec.ArrowJoinRunCount,
		"SELECT jl.v, jr.w FROM jl JOIN jr ON jl.k = jr.k",
	)
}

// BenchmarkArrowProjection compares the refactored Arrow projection operator
// (重构算子) against the original standard operator (原算子) for a render with an
// arithmetic expression (a + b).
func BenchmarkArrowProjectionQuery(b *testing.B) {
	benchArrowVsNative(b,
		func(db *sql.DB) {
			benchExec(b, db, "CREATE TABLE t (a INT, b INT)")
			loadIntPairs(b, db, 10000)
		},
		"SELECT a + b FROM t",
		[]string{"sql.arrow_projection.enabled"},
		rowexec.ArrowProjectionRunCount,
		"SELECT a + b FROM t",
	)
}

// loadJoin inserts n rows into jl(k,v) and jr(k,w) where keys overlap so the
// inner equi-join (on k) produces a non-trivial number of matched rows.
func loadJoin(b testing.TB, db *sql.DB, n int) {
	const batch = 500
	for start := 1; start <= n; start += batch {
		end := start + batch - 1
		if end > n {
			end = n
		}
		lv := make([]string, 0, end-start+1)
		lw := make([]string, 0, end-start+1)
		for i := start; i <= end; i++ {
			lv = append(lv, fmt.Sprintf("(%d, %d)", i, i))
			// right side shares keys 1..n but with sparser, offset values so the
			// join is not a trivial 1:1 copy.
			lw = append(lw, fmt.Sprintf("(%d, %d)", i, i*3))
		}
		benchExec(b, db, "INSERT INTO jl VALUES "+strings.Join(lv, ", "))
		benchExec(b, db, "INSERT INTO jr VALUES "+strings.Join(lw, ", "))
	}
}
