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
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyValues verifies that the Values data source (pre-canned constant
// rows from a VALUES clause) is emitted as a single Arrow Record via the Arrow
// Values core when sql.arrow_values.enabled is on, and that the result matches
// the classic row-based Values path.
//
// Values is a pure constant-row source with no KV or engine dependency, so this
// test runs without the C++ time-series engine — it is safe to run in any test
// environment (see docs/arrow-unify-roadmap.md §阶段4 "Values 算子 Arrow 化").
func TestArrowUnifyValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Note: no ORDER BY — the test verifies the Arrow Values source emits the
	// correct rows; ordering is the下游 Sorter's responsibility (a separately
	// validated operator) and is irrelevant to the Values emit correctness.
	const query = "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')) AS t(i,s)"

	want := map[string]string{
		"1": "a", "2": "b", "3": "c", "4": "d", "5": "e",
	}
	read := func() map[string]string {
		t.Helper()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		out := make(map[string]string)
		for rows.Next() {
			var i int
			var s string
			if err := rows.Scan(&i, &s); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out[fmt.Sprintf("%d", i)] = s
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		return out
	}

	// Baseline: classic row-based Values (gate off).
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_values.enabled = false"); err != nil {
		t.Fatalf("disable arrow values: %v", err)
	}
	classic := read()

	// Arrow path: gate on. The master arrow_scan gate must also be on
	// (ArrowValuesEnabled = arrow_values.enabled && arrow_scan.enabled).
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = true"); err != nil {
		t.Fatalf("enable arrow scan: %v", err)
	}
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_values.enabled = true"); err != nil {
		t.Fatalf("enable arrow values: %v", err)
	}
	arrow := read()

	// Both paths must produce the same result set as the expected constant rows.
	for k, v := range want {
		if classic[k] != v {
			t.Fatalf("classic row i=%s mismatch: got %q want %q (full=%v)", k, classic[k], v, classic)
		}
		if arrow[k] != v {
			t.Fatalf("arrow row i=%s mismatch: got %q want %q (full=%v)", k, arrow[k], v, arrow)
		}
	}
	if len(arrow) != len(want) || len(classic) != len(want) {
		t.Fatalf("row count mismatch: arrow=%d classic=%d want=%d", len(arrow), len(classic), len(want))
	}

	// Restore default.
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_values.enabled = false"); err != nil {
		t.Fatalf("restore arrow values: %v", err)
	}
}
