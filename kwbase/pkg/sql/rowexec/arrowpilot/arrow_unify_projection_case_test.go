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
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyProjectionCase verifies that CASE and COALESCE projections are
// evaluated through the Arrow projection path. The executor builds a boolean
// mask per WHEN branch and selects the matching THEN value (or ELSE), falling
// back to a row-by-row path only when the expression is unsupported.
// See docs/arrow-unify-roadmap.md §2.6.
func TestArrowUnifyProjectionCase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = true"); err != nil {
		t.Fatalf("set projection enabled: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = false")
		execStmt(t, db, "DROP TABLE IF EXISTS cc")
	}()

	execStmt(t, db, "CREATE TABLE cc (a INT, b INT, c STRING, d INT)")
	for _, r := range [][4]interface{}{
		{1, 10, "x", nil},
		{2, 20, "y", nil},
		{3, 30, "z", 99},
		{4, 40, "w", 7},
	} {
		dv := "NULL"
		if r[3] != nil {
			dv = fmt.Sprintf("%d", r[3])
		}
		execStmt(t, db, fmt.Sprintf("INSERT INTO cc VALUES (%d, %d, '%s', %s)",
			r[0], r[1], r[2], dv))
	}

	// CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END
	assertStr(t, db,
		"SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM cc ORDER BY a",
		[][]string{{"one"}, {"two"}, {"other"}, {"other"}})

	// CASE WHEN a > 2 THEN a*100 ELSE a END (boolean WHEN form)
	assertStr(t, db,
		"SELECT CASE WHEN a > 2 THEN a*100 ELSE a END FROM cc ORDER BY a",
		[][]string{{"1"}, {"2"}, {"300"}, {"400"}})

	// COALESCE(d, -1): d is NULL for a=1,2 -> -1; otherwise the value.
	assertStr(t, db,
		"SELECT COALESCE(d, -1) FROM cc ORDER BY a",
		[][]string{{"-1"}, {"-1"}, {"99"}, {"7"}})

	// COALESCE(d, a, 0): a=1,2 take d(NULL)->a; a=3,4 take d.
	assertStr(t, db,
		"SELECT COALESCE(d, a, 0) FROM cc ORDER BY a",
		[][]string{{"1"}, {"2"}, {"99"}, {"7"}})

	// COALESCE(c, 'none'): c is never NULL here, so the value is returned.
	assertStr(t, db,
		"SELECT COALESCE(c, 'none') FROM cc ORDER BY a",
		[][]string{{"x"}, {"y"}, {"z"}, {"w"}})

	// Confirm the Arrow projection engine actually ran for a CASE render.
	before := rowexec.ArrowProjectionRunCount()
	assertStr(t, db,
		"SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM cc ORDER BY a",
		[][]string{{"one"}, {"other"}, {"other"}, {"other"}})
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for a CASE render")
	}

	// Confirm the Arrow projection engine actually ran for a COALESCE render.
	before = rowexec.ArrowProjectionRunCount()
	assertStr(t, db,
		"SELECT COALESCE(d, -1) FROM cc ORDER BY a",
		[][]string{{"-1"}, {"-1"}, {"99"}, {"7"}})
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for a COALESCE render")
	}
}

// TestArrowUnifyFilterDecimalIn verifies that IN / NOT IN predicates over a
// decimal column are routed through the Arrow filter path. The planner casts
// the decimal column to FLOAT and emits the set as float64 constants; the
// executor's is_in kernel matches against a float64 set.
// See docs/arrow-unify-roadmap.md §2.4.
func TestArrowUnifyFilterDecimalIn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = true"); err != nil {
		t.Fatalf("set filter enabled: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = false")
		execStmt(t, db, "DROP TABLE IF EXISTS di")
	}()

	execStmt(t, db, "CREATE TABLE di (id INT, v DECIMAL(10,2))")
	for _, r := range [][2]interface{}{
		{1, "1.00"},
		{2, "2.00"},
		{3, "3.00"},
		{4, "4.00"},
	} {
		execStmt(t, db, fmt.Sprintf("INSERT INTO di VALUES (%d, %s)", r[0], r[1]))
	}

	// v IN (1.00, 3.00) -> id 1, 3
	assertStr(t, db,
		"SELECT id FROM di WHERE v IN (1.00, 3.00) ORDER BY id",
		[][]string{{"1"}, {"3"}})
	// v NOT IN (2.00) -> id 1, 3, 4
	assertStr(t, db,
		"SELECT id FROM di WHERE v NOT IN (2.00) ORDER BY id",
		[][]string{{"1"}, {"3"}, {"4"}})
	// mixed order: v IN (4.00, 1.00) -> id 1, 4
	assertStr(t, db,
		"SELECT id FROM di WHERE v IN (4.00, 1.00) ORDER BY id",
		[][]string{{"1"}, {"4"}})

	// Confirm the Arrow filter engine actually ran for the decimal IN predicate.
	before := rowexec.ArrowFilterRunCount()
	assertStr(t, db,
		"SELECT id FROM di WHERE v IN (1.00, 3.00) ORDER BY id",
		[][]string{{"1"}, {"3"}})
	if rowexec.ArrowFilterRunCount() <= before {
		t.Fatal("arrow filter processor was not used for a decimal IN filter predicate")
	}
}
