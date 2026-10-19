// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package mysqlwire_test

import (
	"context"
	gosql "database/sql"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/go-sql-driver/mysql"
)

// These tests cover responses that carry more than one result set, and the
// degenerate result set with no columns. Both are framing problems: when the
// server gets them wrong the client does not see an error, it reads the
// leftover packets as the reply to its next command. So every test finishes by
// running an unrelated query on the same connection and checking its answer.

// resultSet is one result set as the client decoded it.
type resultSet struct {
	cols []string
	rows [][]string
}

// readAllResultSets drains rows, following NextResultSet the way a MySQL
// client iterates a multi-result response.
func readAllResultSets(t *testing.T, rows *gosql.Rows) []resultSet {
	t.Helper()
	var sets []resultSet
	for {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		set := resultSet{cols: cols}
		for rows.Next() {
			vals := make([]gosql.NullString, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			row := make([]string, len(cols))
			for i, v := range vals {
				if v.Valid {
					row[i] = v.String
				} else {
					row[i] = "NULL"
				}
			}
			set.rows = append(set.rows, row)
		}
		sets = append(sets, set)
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sets
}

// assertConnInSync runs a query whose answer cannot be confused with anything
// the previous command produced. A connection left holding packets from an
// earlier response returns those instead, or fails to decode them.
func assertConnInSync(t *testing.T, conn *gosql.Conn) {
	t.Helper()
	var got string
	if err := conn.QueryRowContext(context.Background(), "SELECT 'in-sync'").Scan(&got); err != nil {
		t.Fatalf("follow-up query on the same connection failed: %v", err)
	}
	if got != "in-sync" {
		t.Fatalf("follow-up query returned %q: the connection is reading a stale response", got)
	}
}

// openConn opens a single connection to dbName. multiStatements negotiates
// CLIENT_MULTI_STATEMENTS, which is what `mysql`, JDBC's allowMultiQueries and
// PHP's multi_query all do.
//
// The read timeout matters: a mis-framed response can leave the client waiting
// for packets that never come, and without it the test hangs instead of
// failing.
//
// The returned func closes the connection. Callers defer it rather than this
// registering t.Cleanup, which would run after leaktest has already checked
// for the driver's goroutines.
func openConn(t *testing.T, addr, dbName string, multiStatements bool) (*gosql.Conn, func()) {
	return openConnWithTimeout(t, addr, dbName, multiStatements, 20*time.Second)
}

func openConnWithTimeout(
	t *testing.T, addr, dbName string, multiStatements bool, readTimeout time.Duration,
) (*gosql.Conn, func()) {
	t.Helper()
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.User = "root"
	cfg.DBName = dbName
	cfg.MultiStatements = multiStatements
	cfg.ReadTimeout = readTimeout
	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return conn, func() {
		_ = conn.Close()
		_ = db.Close()
	}
}

func TestMySQLMultiStatementResultSets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	conn, closeConn := openConn(t, s.ServingMySQLAddr(), "e2e", true /* multiStatements */)
	defer closeConn()
	ctx := context.Background()

	t.Run("two queries", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, "SELECT 1 AS a; SELECT 2 AS b, 3 AS c")
		if err != nil {
			t.Fatal(err)
		}
		got := readAllResultSets(t, rows)
		want := []resultSet{
			{cols: []string{"a"}, rows: [][]string{{"1"}}},
			{cols: []string{"b", "c"}, rows: [][]string{{"2", "3"}}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		assertConnInSync(t, conn)
	})

	t.Run("statement then query", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS ms (x INT)"); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.QueryContext(ctx, "INSERT INTO ms VALUES (1); SELECT x FROM ms")
		if err != nil {
			t.Fatal(err)
		}
		got := readAllResultSets(t, rows)
		// The INSERT answers with an OK packet flagged as followed by more
		// results; go-sql-driver steps over such results on its own, so the
		// SELECT's set is the only one the caller sees.
		want := []resultSet{{cols: []string{"x"}, rows: [][]string{{"1"}}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		assertConnInSync(t, conn)
	})

	t.Run("error stops the batch", func(t *testing.T) {
		// MySQL reports the first failing statement and runs nothing after it:
		// the response is the first result set, flagged as followed by more,
		// then an ERR packet.
		//
		// This checks only that the error reaches the client. The vendored
		// go-sql-driver (1.4) does not clear the more-results flag when it reads
		// an ERR packet, so it keeps waiting for a result that never comes and
		// the connection is unusable afterwards; that is the driver's bug, and
		// the mysql CLI handles the same response correctly.
		// qa/run_mysql_protocol_test.sh checks the connection stays usable.
		// A connection of its own with a short read timeout, so that the
		// driver's wait for the missing result ends quickly.
		errConn, closeErrConn := openConnWithTimeout(
			t, s.ServingMySQLAddr(), "e2e", true /* multiStatements */, 2*time.Second)
		defer closeErrConn()
		rows, err := errConn.QueryContext(ctx, "SELECT 1; SELECT nosuchcol FROM ms; SELECT 3")
		if err == nil {
			for rows.Next() {
			}
			for rows.NextResultSet() {
				for rows.Next() {
				}
			}
			err = rows.Err()
			_ = rows.Close()
		}
		if err == nil {
			t.Fatal("expected the second statement's error")
		}
	})
}

func TestMySQLCallMultipleResultSets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE ta (a INT)")
	mustExec(t, db, "INSERT INTO ta VALUES (1)")
	mustExec(t, db, "CREATE TABLE tb (x INT, y STRING)")
	mustExec(t, db, "INSERT INTO tb VALUES (7, 'q')")
	// The second result set is wider than the first: before the fix its rows
	// were squeezed under the first set's single column, and 'q' was lost.
	mustExec(t, db, "CREATE PROCEDURE two_sets() BEGIN SELECT a FROM ta; SELECT x, y FROM tb; END")

	conn, closeConn := openConn(t, s.ServingMySQLAddr(), "e2e", false /* multiStatements */)
	defer closeConn()

	rows, err := conn.QueryContext(context.Background(), "CALL two_sets()")
	if err != nil {
		t.Fatal(err)
	}
	got := readAllResultSets(t, rows)
	want := []resultSet{
		{cols: []string{"a"}, rows: [][]string{{"1"}}},
		{cols: []string{"x", "y"}, rows: [][]string{{"7", "q"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	assertConnInSync(t, conn)
}

func TestMySQLZeroColumnResultSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE nocols ()")
	mustExec(t, db, "INSERT INTO nocols DEFAULT VALUES")

	conn, closeConn := openConn(t, s.ServingMySQLAddr(), "e2e", false /* multiStatements */)
	defer closeConn()

	// MySQL has no way to express a result set without columns: a column
	// count of zero is the first byte of an OK packet. The query must succeed
	// and leave the connection usable.
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM nocols")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	assertConnInSync(t, conn)
}
