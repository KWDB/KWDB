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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND. Either express or implied.
// See the License for the specific language governing to specific
// permissions and limitations under the License.

package arrowpilot

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// TestArrowUnifyBoolAgg verifies that BOOL_AND / BOOL_OR are routed through the
// Arrow compute engine (reusing the min/max kernel over a boolean column) and
// produce results identical to the standard execution engine, including the
// three-valued-logic NULL semantics.
func TestArrowUnifyBoolAgg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	settings := []string{
		"sql.arrow_aggregator.enabled",
		"sql.arrow_sorter.enabled",
		"sql.arrow_distinct.enabled",
		"sql.arrow_filter.enabled",
		"sql.arrow_projection.enabled",
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

	execStmt(t, db, "CREATE TABLE bools (g INT, b BOOL)")

	// g=1: mix of true/false/null -> BOOL_AND=false, BOOL_OR=true.
	// g=2: all true               -> BOOL_AND=true,  BOOL_OR=true.
	// g=3: only NULL              -> BOOL_AND=NULL,  BOOL_OR=NULL.
	// g=4: all false              -> BOOL_AND=false, BOOL_OR=false.
	execStmt(t, db, "INSERT INTO bools VALUES (1, true), (1, false), (1, NULL)")
	execStmt(t, db, "INSERT INTO bools VALUES (2, true), (2, true)")
	execStmt(t, db, "INSERT INTO bools VALUES (3, NULL), (3, NULL)")
	execStmt(t, db, "INSERT INTO bools VALUES (4, false), (4, false)")

	boolByGroup := func(fn string) map[int64]*bool {
		rows, err := db.Query(fmt.Sprintf("SELECT g, %s(b) FROM bools GROUP BY g ORDER BY g", fn))
		if err != nil {
			t.Fatalf("query %s: %v", fn, err)
		}
		defer rows.Close()
		out := make(map[int64]*bool)
		for rows.Next() {
			var g int64
			var b sql.NullBool
			if err := rows.Scan(&g, &b); err != nil {
				t.Fatalf("scan %s: %v", fn, err)
			}
			if !b.Valid {
				out[g] = nil
			} else {
				v := b.Bool
				out[g] = &v
			}
		}
		return out
	}

	assertBool := func(fn string, got map[int64]*bool, want map[int64]*bool) {
		for g, w := range want {
			gv, ok := got[g]
			if !ok {
				t.Fatalf("%s: missing group %d", fn, g)
			}
			if (w == nil) != (gv == nil) {
				t.Fatalf("%s(g=%d): NULL mismatch want=%v got=%v", fn, g, w, gv)
			}
			if w != nil && *w != *gv {
				t.Fatalf("%s(g=%d): want=%v got=%v", fn, g, *w, *gv)
			}
		}
	}

	// --- BOOL_AND ---
	andWant := map[int64]*bool{
		1: boolPtr(false),
		2: boolPtr(true),
		3: nil, // all NULL -> NULL
		4: boolPtr(false),
	}
	assertBool("bool_and", boolByGroup("bool_and"), andWant)
	if !waitForArrowRuns(t, rowexec.ArrowAggRunCount, 1) {
		t.Fatal("arrow aggregator processor was not used for bool_and")
	}

	// --- BOOL_OR ---
	orWant := map[int64]*bool{
		1: boolPtr(true),
		2: boolPtr(true),
		3: nil, // all NULL -> NULL
		4: boolPtr(false),
	}
	assertBool("bool_or", boolByGroup("bool_or"), orWant)

	// --- global (no GROUP BY) ---
	var globalAnd sql.NullBool
	if err := db.QueryRow("SELECT bool_and(b) FROM bools").Scan(&globalAnd); err != nil {
		t.Fatal(err)
	}
	if !globalAnd.Valid || globalAnd.Bool != false {
		t.Fatalf("global bool_and: want false got %v", globalAnd)
	}
	var globalOr sql.NullBool
	if err := db.QueryRow("SELECT bool_or(b) FROM bools").Scan(&globalOr); err != nil {
		t.Fatal(err)
	}
	if !globalOr.Valid || globalOr.Bool != true {
		t.Fatalf("global bool_or: want true got %v", globalOr)
	}
}

func boolPtr(b bool) *bool { return &b }
