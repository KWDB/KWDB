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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package arrowpilot

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// TestArrowUnifySortDistinct verifies that ORDER BY (sorter) and DISTINCT
// (distinct) are routed through the Arrow compute engine (§4.2 / §4.3) and
// produce results bit-for-bit identical to the standard execution engine.
func TestArrowUnifySortDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	for _, setting := range []string{
		"sql.arrow_sorter.enabled",
		"sql.arrow_distinct.enabled",
	} {
		if _, err := db.Exec("SET CLUSTER SETTING " + setting + " = true"); err != nil {
			t.Fatalf("set %s: %v", setting, err)
		}
	}
	defer func() {
		for _, setting := range []string{
			"sql.arrow_sorter.enabled",
			"sql.arrow_distinct.enabled",
		} {
			_, _ = db.Exec("SET CLUSTER SETTING " + setting + " = false")
		}
	}()

	execStmt(t, db, "CREATE TABLE sd (a INT, b INT)")
	// Intentionally out-of-order insertion plus a NULL to exercise ordering and
	// distinct semantics. Use -9999 as a sentinel for NULL at insert time.
	sdData := [][2]int64{{3, 1}, {1, 2}, {2, 2}, {1, 1}, {3, 1}, {2, 3}, {-9999, 2}}
	for _, r := range sdData {
		av := "NULL"
		if r[0] != -9999 {
			av = fmt.Sprintf("%d", r[0])
		}
		execStmt(t, db, fmt.Sprintf("INSERT INTO sd VALUES (%s, %d)", av, r[1]))
	}

	intRows := func(q string) [][]int64 {
		return queryIntRows(t, db, q)
	}

	// --- SORT: ascending single column (NULL sorts first, matching rowexec) ---
	assertIntRows(t, intRows("SELECT a FROM sd ORDER BY a"), [][]int64{
		{-9999}, {1}, {1}, {2}, {2}, {3}, {3},
	})
	if !waitForArrowRuns(t, rowexec.ArrowSorterRunCount, 1) {
		t.Fatal("arrow sorter processor was not used for ORDER BY")
	}

	// --- SORT: descending single column ---
	assertIntRows(t, intRows("SELECT a FROM sd ORDER BY a DESC"), [][]int64{
		{3}, {3}, {2}, {2}, {1}, {1}, {-9999},
	})

	// --- SORT: multi-column (b asc, a desc) ---
	// Note: the source has (a=3,b=1) inserted twice, so the b=1 group yields
	// two [1,3] rows then [1,1].
	assertIntRows(t, intRows("SELECT b, a FROM sd ORDER BY b, a DESC"), [][]int64{
		{1, 3}, {1, 3}, {1, 1},
		{2, 2}, {2, 1}, {2, -9999},
		{3, 2},
	})

	// --- DISTINCT: single column (NULL collapses to one row) ---
	assertIntRows(t, intRows("SELECT DISTINCT a FROM sd ORDER BY a"), [][]int64{
		{-9999}, {1}, {2}, {3},
	})
	if !waitForArrowRuns(t, rowexec.ArrowDistinctRunCount, 1) {
		t.Fatal("arrow distinct processor was not used for DISTINCT")
	}

	// --- DISTINCT: multi-column (a, b) ---
	assertIntRows(t, intRows("SELECT DISTINCT a, b FROM sd ORDER BY a, b"), [][]int64{
		{-9999, 2},
		{1, 1}, {1, 2},
		{2, 2}, {2, 3},
		{3, 1},
	})
}
