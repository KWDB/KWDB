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

// TestArrowUnifyProjectionRounding verifies that the rounding scalar functions
// floor/ceil/trunc/round are evaluated through the Arrow compute engine
// (arrow/compute kernels) and that their results match the classic engine.
//
// Semantics: KWDB defines these as floatOverload1, i.e. they take a float and
// return a float (DFloat). Integer inputs are cast to float by the planner,
// which is exactly what the Arrow kernels do (int -> float64). round uses
// banker's rounding (math.RoundToEven), matching arrow's DefaultRoundOptions.
// See docs/arrow-unify-roadmap.md §2.5.
func TestArrowUnifyProjectionRounding(t *testing.T) {
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
		execStmt(t, db, "DROP TABLE IF EXISTS rn")
	}()

	execStmt(t, db, "CREATE TABLE rn (f FLOAT)")
	// Mix of positive/negative/tie values that exercise each rounding mode.
	vals := []string{
		"3.2", "-3.2", "3.7", "-3.7", "3.5", "-3.5", "4.5", "-4.5", "0.0", "2.0", "-2.0",
	}
	for _, v := range vals {
		execStmt(t, db, fmt.Sprintf("INSERT INTO rn VALUES (%s)", v))
	}

	assertFloat := func(q string, want [][]float64) {
		t.Helper()
		assertFloatRows(t, queryFloatRows(t, db, q), want)
	}

	// Values sorted ascending by f:
	//   -4.5, -3.7, -3.5, -3.2, -2.0, 0.0, 2.0, 3.2, 3.5, 3.7, 4.5
	// floor (round down to the next integer <= x):
	//   -5,  -4,   -4,   -4,   -2,   0,   2,   3,   3,   3,   4
	assertFloat("SELECT floor(f) FROM rn ORDER BY f",
		[][]float64{{-5.0}, {-4.0}, {-4.0}, {-4.0}, {-2.0}, {0.0}, {2.0}, {3.0}, {3.0}, {3.0}, {4.0}})
	// ceil (round up to the next integer >= x):
	//   -4,  -3,   -3,   -3,   -2,   0,   2,   4,   4,   4,   5
	assertFloat("SELECT ceil(f) FROM rn ORDER BY f",
		[][]float64{{-4.0}, {-3.0}, {-3.0}, {-3.0}, {-2.0}, {0.0}, {2.0}, {4.0}, {4.0}, {4.0}, {5.0}})
	// ceiling is an alias of ceil
	assertFloat("SELECT ceiling(f) FROM rn ORDER BY f",
		[][]float64{{-4.0}, {-3.0}, {-3.0}, {-3.0}, {-2.0}, {0.0}, {2.0}, {4.0}, {4.0}, {4.0}, {5.0}})
	// trunc (towards zero):
	//   -4,  -3,   -3,   -3,   -2,   0,   2,   3,   3,   3,   4
	assertFloat("SELECT trunc(f) FROM rn ORDER BY f",
		[][]float64{{-4.0}, {-3.0}, {-3.0}, {-3.0}, {-2.0}, {0.0}, {2.0}, {3.0}, {3.0}, {3.0}, {4.0}})
	// round (banker's rounding: .5 ties to nearest even):
	//   -4,  -4,   -4,   -3,   -2,   0,   2,   3,   4,   4,   4
	assertFloat("SELECT round(f) FROM rn ORDER BY f",
		[][]float64{{-4.0}, {-4.0}, {-4.0}, {-3.0}, {-2.0}, {0.0}, {2.0}, {3.0}, {4.0}, {4.0}, {4.0}})

	// Integer input: floor has an INT4 overload (arrow returns float64); the
	// other rounding funcs only take FLOAT in KWDB, so they are exercised with
	// an explicit float cast which the planner inserts identically to the classic
	// path. The Arrow kernel returns float64 consistently in all cases.
	execStmt(t, db, "CREATE TABLE rn_i (i INT)")
	execStmt(t, db, "INSERT INTO rn_i VALUES (7), (-7), (3), (-3)")
	assertFloat("SELECT floor(i) FROM rn_i ORDER BY i",
		[][]float64{{-7.0}, {-3.0}, {3.0}, {7.0}})
	assertFloat("SELECT ceil(CAST(i AS FLOAT)) FROM rn_i ORDER BY i",
		[][]float64{{-7.0}, {-3.0}, {3.0}, {7.0}})
	assertFloat("SELECT trunc(CAST(i AS FLOAT)) FROM rn_i ORDER BY i",
		[][]float64{{-7.0}, {-3.0}, {3.0}, {7.0}})
	assertFloat("SELECT round(CAST(i AS FLOAT)) FROM rn_i ORDER BY i",
		[][]float64{{-7.0}, {-3.0}, {3.0}, {7.0}})

	// Confirm the Arrow projection engine actually ran for a rounding render.
	before := rowexec.ArrowProjectionRunCount()
	assertFloat("SELECT ceil(f) FROM rn ORDER BY f",
		[][]float64{{-4.0}, {-3.0}, {-3.0}, {-3.0}, {-2.0}, {0.0}, {2.0}, {4.0}, {4.0}, {4.0}, {5.0}})
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for a rounding render")
	}
}
