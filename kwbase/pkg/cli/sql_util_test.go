// Copyright 2015 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestConnRecover(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cliTestParams{t: t}
	c := newCLITest(p)
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	// Sanity check to establish baseline.
	rows, err := conn.Query(`SELECT 1`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Query detects a connection close.
	defer simulateServerRestart(&c, p, conn)()

	// When the server restarts, the next Query() attempt may encounter a
	// TCP reset error before the SQL driver realizes there is a problem
	// and starts delivering ErrBadConn. We don't know the timing of
	// this however.
	testutils.SucceedsSoon(t, func() error {
		if sqlRows, err := conn.Query(`SELECT 1`, nil); err != driver.ErrBadConn {
			return fmt.Errorf("expected ErrBadConn, got %v", err)
		} else if err == nil {
			if closeErr := sqlRows.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		return nil
	})

	// Check that Query recovers from a connection close by re-connecting.
	rows, err = conn.Query(`SELECT 1`, nil)
	if err != nil {
		t.Fatalf("conn.Query(): expected no error after reconnect, got %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Exec detects a connection close.
	defer simulateServerRestart(&c, p, conn)()

	// Ditto from Query().
	testutils.SucceedsSoon(t, func() error {
		if err := conn.Exec(`SELECT 1`, nil); err != driver.ErrBadConn {
			return fmt.Errorf("expected ErrBadConn, got %v", err)
		}
		return nil
	})

	// Check that Exec recovers from a connection close by re-connecting.
	if err := conn.Exec(`SELECT 1`, nil); err != nil {
		t.Fatalf("conn.Exec(): expected no error after reconnect, got %v", err)
	}
}

// simulateServerRestart restarts the test server and reconfigures the connection
// to use the new test server's port number. This is necessary because the port
// number is selected randomly.
func simulateServerRestart(c *cliTest, p cliTestParams, conn *sqlConn) func() {
	c.restartServer(p)
	url2, cleanup2 := sqlutils.PGUrl(c.t, c.ServingSQLAddr(), c.t.Name(), url.User(security.RootUser))
	conn.url = url2.String()
	return cleanup2
}

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	setCLIDefaultsForTests()

	var b bytes.Buffer

	// Non-query statement.
	if err := runQueryAndFormatResults(conn, &b, makeQuery(`SET DATABASE=system`)); err != nil {
		t.Fatal(err)
	}

	expected := `
SET
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Use system database for sample query/output as they are fairly fixed.
	cols, rows, err := runQuery(conn, makeQuery(`SHOW COLUMNS FROM system.namespace`), false)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{
		"column_name",
		"data_type",
		"is_nullable",
		"column_default",
		"generation_expression",
		"indices",
		"is_hidden",
		"is_tag",
	}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	expectedRows := [][]string{
		{`parentID`, `INT8`, `false`, `NULL`, ``, `{primary}`, `false`, `false`},
		{`name`, `STRING`, `false`, `NULL`, ``, `{primary}`, `false`, `false`},
		{`id`, `INT8`, `true`, `NULL`, ``, `{}`, `false`, `false`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW COLUMNS FROM system.namespace`)); err != nil {
		t.Fatal(err)
	}

	expected = `
  column_name | data_type | is_nullable | column_default | generation_expression |  indices  | is_hidden | is_tag
--------------+-----------+-------------+----------------+-----------------------+-----------+-----------+---------
  parentID    | INT8      |    false    | NULL           |                       | {primary} |   false   | false
  name        | STRING    |    false    | NULL           |                       | {primary} |   false   | false
  id          | INT8      |    true     | NULL           |                       | {}        |   false   | false
(3 rows)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT * FROM system.namespace WHERE name=$1`, "descriptor")); err != nil {
		t.Fatal(err)
	}

	expected = `
  parentID | parentSchemaID |    name    | id
-----------+----------------+------------+-----
         1 |             29 | descriptor |  3
(1 row)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test multiple results.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT 1 AS "1"; SELECT 2 AS "2", 3 AS "3"; SELECT 'hello' AS "'hello'"`)); err != nil {
		t.Fatal(err)
	}

	expected = `
  1
-----
  1
(1 row)
  2 | 3
----+----
  2 | 3
(1 row)
  'hello'
-----------
  hello
(1 row)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}

func TestTransactionRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cliTestParams{t: t}
	c := newCLITest(p)
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	var tries int
	err := conn.ExecTxn(func(conn *sqlConn) error {
		tries++
		if tries > 2 {
			return nil
		}

		// Prevent automatic server-side retries.
		rows, err := conn.Query(`SELECT now()`, nil)
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}

		// Force a client-side retry.
		rows, err = conn.Query(`SELECT kwdb_internal.force_retry('1h')`, nil)
		if err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	if tries <= 2 {
		t.Fatalf("expected transaction to require at least two tries, but it only required %d", tries)
	}
}
