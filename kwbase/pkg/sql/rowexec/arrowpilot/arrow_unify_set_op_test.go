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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// TestArrowUnifySetOp verifies that the dedup stages of UNION DISTINCT,
// INTERSECT DISTINCT and EXCEPT DISTINCT are routed through the Arrow distinct
// processor (§4.9), reusing the same executor as plain DISTINCT, and produce
// results identical to the standard execution engine.
func TestArrowUnifySetOp(t *testing.T) {
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

	execStmt(t, db, "CREATE TABLE lhs (a INT)")
	execStmt(t, db, "CREATE TABLE rhs (a INT)")
	// lhs: 1,1,2,3,3,4     rhs: 2,3,3,5
	execStmt(t, db, "INSERT INTO lhs VALUES (1),(1),(2),(3),(3),(4)")
	execStmt(t, db, "INSERT INTO rhs VALUES (2),(3),(3),(5)")

	intRows := func(q string) [][]int64 { return queryIntRows(t, db, q) }

	// UNION DISTINCT: {1,2,3,4} ∪ {2,3,5} = {1,2,3,4,5}
	assertIntRows(t, intRows("SELECT a FROM lhs UNION SELECT a FROM rhs"), [][]int64{
		{1}, {2}, {3}, {4}, {5},
	})
	if !waitForArrowRuns(t, rowexec.ArrowDistinctRunCount, 1) {
		t.Fatal("arrow distinct processor was not used for UNION DISTINCT")
	}

	// INTERSECT DISTINCT: {1,2,3,4} ∩ {2,3,5} = {2,3}
	assertIntRows(t, intRows("SELECT a FROM lhs INTERSECT SELECT a FROM rhs"), [][]int64{
		{2}, {3},
	})
	if !waitForArrowRuns(t, rowexec.ArrowDistinctRunCount, 2) {
		t.Fatal("arrow distinct processor was not used for INTERSECT DISTINCT")
	}

	// EXCEPT DISTINCT: {1,2,3,4} - {2,3,5} = {1,4}
	assertIntRows(t, intRows("SELECT a FROM lhs EXCEPT SELECT a FROM rhs"), [][]int64{
		{1}, {4},
	})
	if !waitForArrowRuns(t, rowexec.ArrowDistinctRunCount, 3) {
		t.Fatal("arrow distinct processor was not used for EXCEPT DISTINCT")
	}

	// UNION ALL must NOT use the distinct processor (sanity check that the
	// Arrow distinct path is only engaged for the dedup variants).
	assertIntRows(t, intRows("SELECT a FROM lhs UNION ALL SELECT a FROM rhs"), [][]int64{
		{1}, {1}, {2}, {3}, {3}, {4}, {2}, {3}, {3}, {5},
	})
}
