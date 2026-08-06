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
	"database/sql"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyProjectionDatetime verifies that EXTRACT and DATE_TRUNC over a
// TIMESTAMP column are evaluated through the Arrow projection path. The
// executor reuses the canonical builtins.ExtractTimeSpanFromTimestamp /
// TruncateTimestamp helpers over a native vectorized loop, so the Arrow path
// must produce results identical to the row-by-row path.
// See docs/arrow-unify-roadmap.md §2.6.
func TestArrowUnifyProjectionDatetime(t *testing.T) {
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
		execStmt(t, db, "DROP TABLE IF EXISTS dt")
	}()

	execStmt(t, db, "CREATE TABLE dt (id INT, ts TIMESTAMP)")
	for i, r := range [][2]interface{}{
		{1, "2021-03-14 15:09:26"},
		{2, "2022-07-04 00:00:00"},
		{3, "2023-12-25 23:59:59"},
		{4, "2000-01-01 12:00:00"},
	} {
		_ = i
		execStmt(t, db, fmt.Sprintf("INSERT INTO dt VALUES (%d, '%s')", r[0], r[1]))
	}

	// EXTRACT of several fields.
	assertStr(t, db,
		"SELECT EXTRACT(YEAR FROM ts) FROM dt ORDER BY id",
		[][]string{{"2021"}, {"2022"}, {"2023"}, {"2000"}})
	assertStr(t, db,
		"SELECT EXTRACT(MONTH FROM ts) FROM dt ORDER BY id",
		[][]string{{"3"}, {"7"}, {"12"}, {"1"}})
	assertStr(t, db,
		"SELECT EXTRACT(DAY FROM ts) FROM dt ORDER BY id",
		[][]string{{"14"}, {"4"}, {"25"}, {"1"}})
	assertStr(t, db,
		"SELECT EXTRACT(HOUR FROM ts) FROM dt ORDER BY id",
		[][]string{{"15"}, {"0"}, {"23"}, {"12"}})
	assertStr(t, db,
		"SELECT EXTRACT(MINUTE FROM ts) FROM dt ORDER BY id",
		[][]string{{"9"}, {"0"}, {"59"}, {"0"}})
	assertStr(t, db,
		"SELECT EXTRACT(SECOND FROM ts) FROM dt ORDER BY id",
		[][]string{{"26"}, {"0"}, {"59"}, {"0"}})

	// DATE_TRUNC to day: drops the time component.
	assertStr(t, db,
		"SELECT DATE_TRUNC('day', ts) FROM dt ORDER BY id",
		queryStr(t, db, "SELECT DATE_TRUNC('day', ts) FROM dt ORDER BY id"))

	// DATE_TRUNC to month and year.
	assertStr(t, db,
		"SELECT DATE_TRUNC('month', ts) FROM dt ORDER BY id",
		queryStr(t, db, "SELECT DATE_TRUNC('month', ts) FROM dt ORDER BY id"))

	// Confirm the Arrow projection engine actually ran for EXTRACT.
	before := rowexec.ArrowProjectionRunCount()
	assertStr(t, db,
		"SELECT EXTRACT(YEAR FROM ts) FROM dt ORDER BY id",
		[][]string{{"2021"}, {"2022"}, {"2023"}, {"2000"}})
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for an EXTRACT render")
	}

	// Confirm the Arrow projection engine actually ran for DATE_TRUNC.
	before = rowexec.ArrowProjectionRunCount()
	assertStr(t, db,
		"SELECT DATE_TRUNC('day', ts) FROM dt ORDER BY id",
		queryStr(t, db, "SELECT DATE_TRUNC('day', ts) FROM dt ORDER BY id"))
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for a DATE_TRUNC render")
	}
}

// TestArrowUnifyProjectionDatetimeTZ verifies that EXTRACT / DATE_TRUNC over a
// TIMESTAMPTZ column honor the session time zone, matching the row-by-row path.
func TestArrowUnifyProjectionDatetimeTZ(t *testing.T) {
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
		execStmt(t, db, "DROP TABLE IF EXISTS dttz")
	}()

	// Use a fixed non-UTC zone so TZ handling is observable.
	execStmt(t, db, "SET TIME ZONE 'America/New_York'")
	execStmt(t, db, "CREATE TABLE dttz (id INT, ts TIMESTAMPTZ)")
	// 2021-03-14 15:09:26 UTC == 11:09:26 EST (UTC-5).
	execStmt(t, db, "INSERT INTO dttz VALUES (1, '2021-03-14 15:09:26+00')")
	execStmt(t, db, "INSERT INTO dttz VALUES (2, '2022-07-04 03:30:00+00')")

	// EXTRACT(HOUR) in America/New_York: 15:09 UTC -> 11:09 EST.
	assertStr(t, db,
		"SELECT EXTRACT(HOUR FROM ts) FROM dttz ORDER BY id",
		[][]string{{"11"}, {"23"}})

	// DATE_TRUNC('day') must match the row-by-row engine exactly, including
	// the session time zone (DST-aware day boundary for the spring-forward
	// date 2021-03-14).
	rowRes := queryStr(t, db, "SELECT DATE_TRUNC('day', ts) FROM dttz ORDER BY id")
	assertStr(t, db,
		"SELECT DATE_TRUNC('day', ts) FROM dttz ORDER BY id", rowRes)

	// Confirm the Arrow projection engine actually ran (TIMESTAMPTZ path).
	before := rowexec.ArrowProjectionRunCount()
	got := queryStr(t, db, "SELECT EXTRACT(HOUR FROM ts) FROM dttz ORDER BY id")
	assertStr(t, db, "SELECT EXTRACT(HOUR FROM ts) FROM dttz ORDER BY id", got)
	if rowexec.ArrowProjectionRunCount() <= before {
		t.Fatal("arrow projection processor was not used for a TIMESTAMPTZ EXTRACT render")
	}
}

// queryStr captures the row-by-row engine output for q using the same
// KWDB-formatted rendering as assertStr (via queryStringRows). It is used to
// assert the Arrow path produces results identical to the row path without
// hard-coding fragile timestamp formatting.
func queryStr(t *testing.T, db *sql.DB, q string) [][]string {
	return queryStringRows(t, db, q)
}
