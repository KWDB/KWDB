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

// TestArrowUnifyVariance verifies that VARIANCE/STDDEV are routed through the
// Arrow compute engine (both the local SQRDIFF stage and the FINAL_VARIANCE /
// FINAL_STDDEV merge stage) when sql.arrow_join is enabled, and that the
// results match the standard engine bit-for-bit (§4.12). The Arrow sqrdiff /
// final_variance kernels implement Welford's online algorithm and the
// parallel-variance merge exactly like colexec, so the numeric results are
// identical for float, int and decimal inputs, both grouped and ungrouped.
func TestArrowUnifyVariance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = true"); err != nil {
		t.Fatalf("enable arrow_aggregator: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = false")
	}()

	execStmt(t, db, "CREATE TABLE v1 (g INT, x INT)")
	defer func() { execStmt(t, db, "DROP TABLE v1") }()
	execStmt(t, db, "INSERT INTO v1 VALUES (1,10),(1,20),(1,30),(2,5),(2,15),(2,25)")
	execStmt(t, db, "CREATE TABLE v2 (y FLOAT)")
	defer func() { execStmt(t, db, "DROP TABLE v2") }()
	execStmt(t, db, "INSERT INTO v2 VALUES (1.0),(2.0),(3.0),(4.0),(5.0)")

	// Reference results computed by the standard (non-arrow) engine.
	refFloat := func(q string) [][]float64 {
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = false"); err != nil {
			t.Fatalf("disable arrow_join: %v", err)
		}
		rows := queryFloatRows(t, db, q)
		if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_aggregator.enabled = true"); err != nil {
			t.Fatalf("enable arrow_join: %v", err)
		}
		return rows
	}

	runFloat := func(q string, want [][]float64) {
		before := rowexec.ArrowAggRunCount()
		got := queryFloatRows(t, db, q)
		assertFloatRows(t, got, want)
		if rowexec.ArrowAggRunCount() == before {
			t.Fatalf("arrow aggregate was not used for query: %s", q)
		}
	}

	// Ungrouped sample variance / stddev on floats.
	runFloat("SELECT VARIANCE(y) FROM v2", refFloat("SELECT VARIANCE(y) FROM v2"))
	runFloat("SELECT STDDEV(y) FROM v2", refFloat("SELECT STDDEV(y) FROM v2"))

	// Ungrouped on ints (widened to decimal inside the engine).
	runFloat("SELECT VARIANCE(x) FROM v1", refFloat("SELECT VARIANCE(x) FROM v1"))
	runFloat("SELECT STDDEV(x) FROM v1", refFloat("SELECT STDDEV(x) FROM v1"))

	// Grouped variance / stddev.
	runFloat("SELECT g, VARIANCE(x) FROM v1 GROUP BY g ORDER BY g",
		refFloat("SELECT g, VARIANCE(x) FROM v1 GROUP BY g ORDER BY g"))
	runFloat("SELECT g, STDDEV(x) FROM v1 GROUP BY g ORDER BY g",
		refFloat("SELECT g, STDDEV(x) FROM v1 GROUP BY g ORDER BY g"))

	// Cross-check one grouped query against the reference explicitly.
	require.Equal(t,
		refFloat("SELECT g, STDDEV(x) FROM v1 GROUP BY g ORDER BY g"),
		queryFloatRows(t, db, "SELECT g, STDDEV(x) FROM v1 GROUP BY g ORDER BY g"))
}
