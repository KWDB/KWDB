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
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyFilterBytes verifies a BYTES column flows through the Arrow
// filter path: a comparison predicate (b = <const>) plus the row-selection
// step. Previously the selection used arrow/compute FilterBinary, which
// panicked on the canonical Binary span layout; now Binary columns are
// gathered via the Go path. The query is a pure constant VALUES source, so it
// runs without the C++ time-series engine.
func TestArrowUnifyFilterBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	const query = "SELECT b FROM (" +
		"VALUES (CAST('aaa' AS BYTES)), (CAST('bbb' AS BYTES)), (CAST('aaa' AS BYTES)), (CAST('ccc' AS BYTES))" +
		") AS t(b) WHERE b = CAST('aaa' AS BYTES) ORDER BY b"

	read := func() []string {
		t.Helper()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var b []byte
			if err := rows.Scan(&b); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, string(b))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		return out
	}

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = false"); err != nil {
		t.Fatalf("disable arrow scan: %v", err)
	}
	classic := read()

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = true"); err != nil {
		t.Fatalf("enable arrow scan: %v", err)
	}
	arrow := read()

	// KWDB returns BYTES over the wire as a PostgreSQL hex-escape string
	// (e.g. "\x616161" for the three bytes 0x61,0x61,0x61), so the scanned
	// value is the literal escape text. The correctness check is that the
	// Arrow path reproduces exactly what the classic path returns.
	if len(classic) != 2 || len(arrow) != 2 {
		t.Fatalf("row count mismatch: classic=%v arrow=%v", classic, arrow)
	}
	for i := range classic {
		if classic[i] != arrow[i] {
			t.Fatalf("arrow/classic mismatch at %d: classic=%v arrow=%v", i, classic, arrow)
		}
		if want := `\x616161`; classic[i] != want {
			t.Fatalf("unexpected filtered value at %d: got %q want %q", i, classic[i], want)
		}
	}

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = false"); err != nil {
		t.Fatalf("restore arrow scan: %v", err)
	}
}
