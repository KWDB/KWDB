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
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowpilot

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyDefaultValuesPipeline verifies that, relying solely on the
// default-on Arrow gates, a VALUES source is emitted as a single Arrow Record
// and flows through an Arrow filter -> aggregator -> sorter pipeline, producing
// results identical to the classic row-based path. This guards the
// sql.arrow_values.enabled default=true flip (see docs/arrow-unify-roadmap.md).
//
// VALUES is a pure constant-row source with no KV or engine dependency, so this
// test runs without the C++ time-series engine.
func TestArrowUnifyDefaultValuesPipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// VALUES -> WHERE filter -> GROUP BY aggregate -> ORDER BY sorter.
	const query = `SELECT s, COUNT(*) FROM (VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'a')) AS t(i,s) WHERE i > 1 GROUP BY s ORDER BY s`

	// i>1 keeps (2,a),(3,b),(4,b),(5,a): a appears twice, b appears twice.
	want := map[string]int{"a": 2, "b": 2}

	read := func() map[string]int {
		t.Helper()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		out := make(map[string]int)
		for rows.Next() {
			var s string
			var c int
			if err := rows.Scan(&s, &c); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out[s] = c
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		return out
	}

	// Classic baseline: all Arrow gates off.
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = false"); err != nil {
		t.Fatal(err)
	}
	classic := read()

	// Arrow path: rely on the default-on gates (arrow_values / filter / agg /
	// sorter all default true). Re-enable the master gate for the baseline.
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = true"); err != nil {
		t.Fatal(err)
	}
	arrow := read()

	// Compare the full result maps (keys + values) so a missing or extra
	// GROUP BY group is caught, not just a wrong count on a known key.
	if len(classic) != len(want) {
		t.Fatalf("classic group count mismatch: got %v want %v", classic, want)
	}
	for k, v := range want {
		if classic[k] != v {
			t.Fatalf("classic s=%s mismatch: got %d want %d (full=%v)", k, classic[k], v, classic)
		}
	}
	if len(arrow) != len(want) {
		t.Fatalf("arrow group count mismatch: got %v want %v", arrow, want)
	}
	for k, v := range want {
		if arrow[k] != v {
			t.Fatalf("arrow s=%s mismatch: got %d want %d (full=%v)", k, arrow[k], v, arrow)
		}
	}

	// Confirm the pipeline actually executed on Arrow operators, not the
	// row-based fallback. Operators run asynchronously, so poll the run
	// counters (mirrors waitForArrowRuns usage elsewhere in this package).
	if !waitForArrowRuns(t, rowexec.ArrowFilterRunCount, 1) {
		t.Fatalf("expected the Arrow filter to be used, but run count stayed 0")
	}
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatalf("expected the Arrow aggregator to be used, but run count stayed 0")
	}
	if !waitForArrowRuns(t, rowexec.ArrowSorterRunCount, 1) {
		t.Fatalf("expected the Arrow sorter to be used, but run count stayed 0")
	}
}

// TestArrowUnifyDefaultSubquery verifies that, under the default-on Arrow gates,
// an IN/uncorrelated subquery built over a VALUES source produces results
// identical to the classic row-based path.
func TestArrowUnifyDefaultSubquery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	const query = `SELECT i FROM (VALUES (1),(2),(3),(4),(5)) AS t(i) WHERE i IN (SELECT j FROM (VALUES (2),(4),(6)) AS u(j)) ORDER BY i`

	want := []int{2, 4}

	read := func() []int {
		t.Helper()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var out []int
		for rows.Next() {
			var i int
			if err := rows.Scan(&i); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, i)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		return out
	}

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = false"); err != nil {
		t.Fatal(err)
	}
	classic := read()

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = true"); err != nil {
		t.Fatal(err)
	}
	arrow := read()

	if len(arrow) != len(want) || len(classic) != len(want) {
		t.Fatalf("row count mismatch: arrow=%v classic=%v want=%v", arrow, classic, want)
	}
	for i := range want {
		if arrow[i] != want[i] {
			t.Fatalf("arrow row %d mismatch: got %d want %d (full=%v)", i, arrow[i], want[i], arrow)
		}
		if classic[i] != want[i] {
			t.Fatalf("classic row %d mismatch: got %d want %d (full=%v)", i, classic[i], want[i], classic)
		}
	}
}
