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

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

// TestArrowUnifyMergeJoin verifies that a merge-join (planned as a mergeJoiner
// because the equality columns are the primary-key-ordered inputs) is routed
// through the Arrow compute engine when sql.arrow_join.enabled is on, for every
// supported join type and for an inner join with a non-equi onExpr (which the
// Arrow join evaluates as a post-filter stage). The results must match the
// standard engine bit-for-bit (§4.11).
//
// The join key is declared as the PRIMARY KEY on both sides so the planner
// delivers already-sorted inputs and selects a merge-joiner; with arrow-join
// enabled the planner swaps the mergeJoiner core for an ArrowJoin core, which
// is order-independent but semantically identical.
func TestArrowUnifyMergeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = true"); err != nil {
		t.Fatalf("set arrow_join enabled: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = false")
	}()

	execStmt(t, db, "CREATE TABLE ml (k INT PRIMARY KEY, v INT)")
	execStmt(t, db, "CREATE TABLE mr (k INT PRIMARY KEY, w INT)")
	defer func() {
		execStmt(t, db, "DROP TABLE ml")
		execStmt(t, db, "DROP TABLE mr")
	}()

	execStmt(t, db, "INSERT INTO ml VALUES (1, 10), (2, 20), (3, 30), (5, 50)")
	execStmt(t, db, "INSERT INTO mr VALUES (1, 100), (2, 200), (4, 400), (5, 500)")

	// Capture the reference result with arrow-join disabled (standard merge-join).
	refRows := func(q string) [][]int64 {
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = false"); err != nil {
			t.Fatalf("disable arrow_join: %v", err)
		}
		rows := queryIntRows(t, db, q)
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_join.enabled = true"); err != nil {
			t.Fatalf("enable arrow_join: %v", err)
		}
		return rows
	}

	runJoin := func(q string, want [][]int64) {
		before := rowexec.ArrowJoinRunCount()
		got := queryIntRows(t, db, q)
		require.Equal(t, want, got, "query: %s", q)
		if rowexec.ArrowJoinRunCount() == before {
			t.Fatalf("arrow join was not used for query: %s", q)
		}
	}

	// Inner join: (1,10,100), (2,20,200), (5,50,500).
	innerWant := [][]int64{{10, 100}, {20, 200}, {50, 500}}
	runJoin("SELECT ml.v, mr.w FROM ml JOIN mr ON ml.k = mr.k ORDER BY ml.v", innerWant)
	runJoin("SELECT ml.v, mr.w FROM ml INNER JOIN mr ON ml.k = mr.k ORDER BY ml.v", innerWant)

	// Left join: unmatched left key 3 -> (30, NULL); NULL mapped to 0 by
	// queryIntRows. (1,10,100), (2,20,200), (3,30,0), (5,50,500).
	leftWant := [][]int64{{10, 100}, {20, 200}, {30, 0}, {50, 500}}
	runJoin("SELECT ml.v, mr.w FROM ml LEFT JOIN mr ON ml.k = mr.k ORDER BY ml.v", leftWant)

	// Right join: unmatched right key 4 -> (0, 400). (1,10,100),(2,20,200),(0,400),(5,50,500).
	rightWant := [][]int64{{0, 400}, {10, 100}, {20, 200}, {50, 500}}
	runJoin("SELECT ml.v, mr.w FROM ml RIGHT JOIN mr ON ml.k = mr.k ORDER BY ml.v", rightWant)

	// Full outer join: both unmatched sides preserved.
	fullWant := [][]int64{{0, 400}, {10, 100}, {20, 200}, {30, 0}, {50, 500}}
	runJoin("SELECT ml.v, mr.w FROM ml FULL OUTER JOIN mr ON ml.k = mr.k ORDER BY ml.v", fullWant)

	// Inner join with a non-equi onExpr post-filter (only ml.v > 15 matches:
	// (2,20,200), (5,50,500)).
	runJoin("SELECT ml.v, mr.w FROM ml JOIN mr ON ml.k = mr.k AND ml.v > 15 ORDER BY ml.v", [][]int64{{20, 200}, {50, 500}})

	// Cross-check one query's result against the standard engine reference.
	require.Equal(t, refRows("SELECT ml.v, mr.w FROM ml FULL OUTER JOIN mr ON ml.k = mr.k ORDER BY ml.v"),
		queryIntRows(t, db, "SELECT ml.v, mr.w FROM ml FULL OUTER JOIN mr ON ml.k = mr.k ORDER BY ml.v"))
}
