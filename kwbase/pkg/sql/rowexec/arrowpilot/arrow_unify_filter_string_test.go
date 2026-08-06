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

// TestArrowUnifyFilterStringFuncs verifies that string scalar functions used as
// the operand of a filter predicate (comparison / LIKE) are routed through the
// Arrow filter path as a "computed" leaf instead of falling back to the
// row-by-row tree.Datum path. The computed leaf is materialized by the existing
// arrowProjection string kernels (substring/upper/trim/length), then consumed
// by the surrounding predicate. See docs/arrow-unify-roadmap.md §6.5.
func TestArrowUnifyFilterStringFuncs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = true"); err != nil {
		t.Fatalf("set filter enabled: %v", err)
	}
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = true"); err != nil {
		t.Fatalf("set projection enabled: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_filter.enabled = false")
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = false")
		execStmt(t, db, "DROP TABLE IF EXISTS ffs")
	}()

	execStmt(t, db, "CREATE TABLE ffs (a INT, name STRING)")
	for _, r := range [][2]interface{}{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
		{4, "dave"},
		{5, "amy"},
	} {
		execStmt(t, db, fmt.Sprintf("INSERT INTO ffs VALUES (%d, '%s')", r[0], r[1]))
	}

	// substring(name,1,3) = 'ali'  -> alice
	assertStr(t, db, "SELECT name FROM ffs WHERE substring(name, 1, 3) = 'ali' ORDER BY a",
		[][]string{{"alice"}})
	// upper(name) = 'BOB'  -> bob
	assertStr(t, db, "SELECT name FROM ffs WHERE upper(name) = 'BOB' ORDER BY a",
		[][]string{{"bob"}})
	// trim(name) ties; ensure the computed leaf path handles identity trim.
	assertStr(t, db, "SELECT name FROM ffs WHERE trim(name) = name ORDER BY a",
		[][]string{{"alice"}, {"bob"}, {"carol"}, {"dave"}, {"amy"}})
	// substring(name,1,3) LIKE 'ali%'  -> alice (computed left + LIKE)
	assertStr(t, db, "SELECT name FROM ffs WHERE substring(name, 1, 3) LIKE 'ali%' ORDER BY a",
		[][]string{{"alice"}})
	// length(name) = 5  -> alice(5)/carol(5)  [bob=3, dave=4, amy=3]
	assertStr(t, db, "SELECT name FROM ffs WHERE length(name) = 5 ORDER BY a",
		[][]string{{"alice"}, {"carol"}})

	// Confirm the Arrow filter engine actually ran for the substring predicate.
	before := rowexec.ArrowFilterRunCount()
	assertStr(t, db, "SELECT name FROM ffs WHERE substring(name, 1, 3) = 'ali' ORDER BY a",
		[][]string{{"alice"}})
	if rowexec.ArrowFilterRunCount() <= before {
		t.Fatal("arrow filter processor was not used for a substring() filter predicate")
	}

	// Computed left operand in IN / NOT IN: the string function result is
	// materialized into an Arrow string array, then matched against a constant
	// set. substring(name,1,3) IN ('ali','car') -> alice/substring='ali',
	// carol/substring='car'. Trim the trailing whitespace preserving rows.
	assertStr(t, db,
		"SELECT name FROM ffs WHERE substring(name, 1, 3) IN ('ali', 'car') ORDER BY a",
		[][]string{{"alice"}, {"carol"}})
	assertStr(t, db,
		"SELECT name FROM ffs WHERE upper(name) NOT IN ('ALICE', 'BOB') ORDER BY a",
		[][]string{{"carol"}, {"dave"}, {"amy"}})

	// Confirm the Arrow filter engine actually ran for the computed-IN predicate.
	before = rowexec.ArrowFilterRunCount()
	assertStr(t, db,
		"SELECT name FROM ffs WHERE substring(name, 1, 3) IN ('ali', 'car') ORDER BY a",
		[][]string{{"alice"}, {"carol"}})
	if rowexec.ArrowFilterRunCount() <= before {
		t.Fatal("arrow filter processor was not used for a substring() IN filter predicate")
	}
}
