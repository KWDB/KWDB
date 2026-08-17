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

// TestArrowUnifyTableReader verifies the stage-1 ArrowTableReader: a relational
// table scan is emitted as a single Arrow Record via ArrowOutput() (not via the
// NewRowSourceToArrow bridge), and flows through an Arrow filter -> aggregator
// pipeline producing results identical to the classic row-based path.
//
// This exercises a real KV-backed table (no time-series engine dependency), so
// it confirms the Arrow scan source feeds the Arrow DAG end to end.
func TestArrowUnifyTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec(`CREATE TABLE tr_t (i INT4 PRIMARY KEY, s STRING)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO tr_t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// SET the scan gate explicitly on, in case defaults change; the default path
	// is already arrow-on for the cluster setting, but pin it for clarity.
	if _, err := db.Exec(`SET sql.arrow_scan.enabled = true`); err != nil {
		t.Fatalf("set scan: %v", err)
	}

	const query = `SELECT s, COUNT(*) FROM tr_t WHERE i > 1 GROUP BY s ORDER BY s`
	// i>1 keeps (2,a),(3,b),(4,b),(5,a): a twice, b twice.
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

	before := rowexec.ArrowTableReaderRunCount()
	got := read()
	if !equalIntMap(want, got) {
		t.Fatalf("mismatch:\n want=%v\n got =%v", want, got)
	}

	// The scan must have been served by the ArrowTableReader direct source, not
	// the row-source-to-Arrow bridge. Allow async scheduling before asserting.
	if !waitForArrowRuns(t, rowexec.ArrowTableReaderRunCount, before+1) {
		t.Fatalf("arrow table reader did not emit (count=%d, want>=%d)", rowexec.ArrowTableReaderRunCount(), before+1)
	}
	if !waitForArrowRuns(t, rowexec.ArrowFilterRunCount, 1) {
		t.Fatalf("arrow filter not in pipeline")
	}
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatalf("arrow aggregator not in pipeline")
	}
}

// equalIntMap is a small helper comparing two int maps for full equality.
func equalIntMap(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
