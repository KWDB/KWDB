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

// TestArrowUnifyFilterCase verifies CASE/COALESCE predicates route through the
// Arrow filter path (reusing the projection CASE evaluator) and agree with the
// classic path. Pure constant VALUES source, no KV/engine dependency.
func TestArrowUnifyFilterCase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	const query = "SELECT a FROM (" +
		"VALUES (1),(2),(3),(4)" +
		") AS t(a) WHERE CASE WHEN a = 1 THEN true ELSE a = 2 END ORDER BY a"

	const coalesceQ = "SELECT a FROM (" +
		"VALUES (1),(2),(3)" +
		") AS t(a) WHERE COALESCE(a, 0) = 2 ORDER BY a"

	read := func(q string) []int64 {
		t.Helper()
		rows, err := db.Query(q)
		if err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, v)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		return out
	}

	eq := func(a, b []int64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	for _, q := range []string{query, coalesceQ} {
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = false"); err != nil {
			t.Fatalf("disable arrow scan: %v", err)
		}
		classic := read(q)
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_scan.enabled = true"); err != nil {
			t.Fatalf("enable arrow scan: %v", err)
		}
		arrow := read(q)
		if !eq(classic, arrow) {
			t.Fatalf("arrow/classic mismatch for %q: classic=%v arrow=%v", q, classic, arrow)
		}
	}
}
