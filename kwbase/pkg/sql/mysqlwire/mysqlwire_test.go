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
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/go-sql-driver/mysql"
)

// These tests drive a real MySQL client against a real node. That matters for
// this package specifically: the binary protocol's failure mode is a value
// written at the wrong width, which shifts everything after it without either
// side reporting an error. Only a client that parses the result the way MySQL
// specifies can tell a correct result set from a plausible-looking wrong one.
//
// go-sql-driver issues COM_STMT_PREPARE/EXECUTE/CLOSE for any query carrying
// arguments unless interpolateParams is set, which it is not here, so every
// parameterised call below exercises the prepared-statement path.

// startMySQLServer starts a test node with the MySQL listener enabled and
// returns a connection to it.
func startMySQLServer(t *testing.T) (serverutils.TestServerInterface, *gosql.DB) {
	t.Helper()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		// Port 0 lets the kernel pick, so concurrent test binaries do not
		// collide; ServingMySQLAddr reports what it settled on.
		MySQLAddr: util.TestAddr.String(),
	})

	addr := s.ServingMySQLAddr()
	if addr == "" {
		t.Fatal("the MySQL listener did not report a bound address")
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.User = "root"
	cfg.ParseTime = true
	// Keep the driver on the server-side prepared statement path, which is what
	// these tests exist to cover.
	cfg.InterpolateParams = false

	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	// One connection keeps prepared-statement handles on a single session, so
	// a failure points at the protocol rather than at pool behavior.
	db.SetMaxOpenConns(1)
	return s, db
}

// startMySQLServerWithStore is like startMySQLServer but backs the node with
// an on-disk store instead of the default in-memory one. The TS engine is
// only created when the store has a path and core dumps are not forbidden
// (see server.go's setTse), so any test that writes to or reads from a TS
// table needs this instead.
func startMySQLServerWithStore(t *testing.T) (serverutils.TestServerInterface, *gosql.DB) {
	t.Helper()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	t.Cleanup(dirCleanupFn)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		// Port 0 lets the kernel pick, so concurrent test binaries do not
		// collide; ServingMySQLAddr reports what it settled on.
		MySQLAddr:  util.TestAddr.String(),
		StoreSpecs: []base.StoreSpec{{Path: baseDir}},
		// The TS engine is only created when core dumps are not forbidden (see
		// server.go's setTse), which CatchCoreDump enables.
		CatchCoreDump: true,
	})

	addr := s.ServingMySQLAddr()
	if addr == "" {
		t.Fatal("the MySQL listener did not report a bound address")
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.User = "root"
	cfg.ParseTime = true
	// Keep the driver on the server-side prepared statement path, which is what
	// these tests exist to cover.
	cfg.InterpolateParams = false

	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	// One connection keeps prepared-statement handles on a single session, so
	// a failure points at the protocol rather than at pool behavior.
	db.SetMaxOpenConns(1)
	return s, db
}

func mustExec(t *testing.T, db *gosql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// TestMySQLTextProtocol covers the unparameterised path, which reaches the
// server as COM_QUERY and comes back in the text protocol.
func TestMySQLTextProtocol(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	var got int
	if err := db.QueryRow("SELECT 1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 returned %d", got)
	}
}

// TestMySQLPreparedStatementRoundTrip is the core check: every supported type
// travels out as a binary parameter and back as a binary result value.
func TestMySQLPreparedStatementRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, `CREATE TABLE t (
		id INT PRIMARY KEY,
		name STRING,
		amount DECIMAL(10,2),
		ratio FLOAT,
		flag BOOL,
		payload BYTES,
		created TIMESTAMP
	)`)

	ts := time.Date(2026, 8, 19, 12, 34, 56, 0, time.UTC)
	mustExec(t, db,
		"INSERT INTO t (id, name, amount, ratio, flag, payload, created) VALUES ($1,$2,$3,$4,$5,$6,$7)",
		1, "hello", "123.45", 2.5, true, []byte{0x00, 0xff, 0x10}, ts)

	var (
		id      int
		name    string
		amount  string
		ratio   float64
		flag    bool
		payload []byte
		created time.Time
	)
	err := db.QueryRow(
		"SELECT id, name, amount, ratio, flag, payload, created FROM t WHERE id = $1", 1,
	).Scan(&id, &name, &amount, &ratio, &flag, &payload, &created)
	if err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Errorf("id = %d, want 1", id)
	}
	if name != "hello" {
		t.Errorf("name = %q, want hello", name)
	}
	if amount != "123.45" {
		t.Errorf("amount = %q, want 123.45", amount)
	}
	if ratio != 2.5 {
		t.Errorf("ratio = %v, want 2.5", ratio)
	}
	if !flag {
		t.Error("flag = false, want true")
	}
	// Raw bytes must survive unaltered; a client that re-read them as text in
	// the connection's charset would mangle the 0xff.
	if got := fmt.Sprintf("%x", payload); got != "00ff10" {
		t.Errorf("payload = %s, want 00ff10", got)
	}
	if got := created.UTC().Format("2006-01-02 15:04:05"); got != "2026-08-19 12:34:56" {
		t.Errorf("created = %s, want 2026-08-19 12:34:56", got)
	}
}

// TestMySQLPreparedNulls covers both NULL bitmaps: the one a client sends with
// its arguments, and the one the server sends ahead of a result row. A row of
// all-NULL values reads correctly only if the server applies the two-bit
// offset MySQL reserves at the head of a result row's bitmap.
func TestMySQLPreparedNulls(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, `CREATE TABLE t (
		id INT PRIMARY KEY,
		name STRING,
		amount DECIMAL(10,2),
		ratio FLOAT,
		flag BOOL,
		payload BYTES,
		created TIMESTAMP
	)`)

	mustExec(t, db,
		"INSERT INTO t (id, name, amount, ratio, flag, payload, created) VALUES ($1,$2,$3,$4,$5,$6,$7)",
		2, nil, nil, nil, nil, nil, nil)

	var (
		id      int
		name    gosql.NullString
		amount  gosql.NullString
		ratio   gosql.NullFloat64
		flag    gosql.NullBool
		payload []byte
		created gosql.NullTime
	)
	err := db.QueryRow(
		"SELECT id, name, amount, ratio, flag, payload, created FROM t WHERE id = $1", 2,
	).Scan(&id, &name, &amount, &ratio, &flag, &payload, &created)
	if err != nil {
		t.Fatal(err)
	}

	if id != 2 {
		t.Errorf("id = %d, want 2", id)
	}
	for _, tc := range []struct {
		name  string
		isNil bool
	}{
		{"name", !name.Valid},
		{"amount", !amount.Valid},
		{"ratio", !ratio.Valid},
		{"flag", !flag.Valid},
		{"payload", payload == nil},
		{"created", !created.Valid},
	} {
		if !tc.isNil {
			t.Errorf("column %s came back non-NULL", tc.name)
		}
	}
}

// TestMySQLStatementReuse covers preparing once and executing repeatedly, then
// closing. Reuse is the reason clients prepare at all, and it exercises the
// path where a client sets the new-params-bound flag to 0 to reuse the
// parameter types it sent last time.
func TestMySQLStatementReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name STRING)")
	mustExec(t, db, "INSERT INTO t VALUES ($1, $2)", 1, "one")
	mustExec(t, db, "INSERT INTO t VALUES ($1, $2)", 2, "two")

	stmt, err := db.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for _, tc := range []struct {
		id   int
		want string
	}{{1, "one"}, {2, "two"}, {1, "one"}} {
		var got string
		if err := stmt.QueryRow(tc.id).Scan(&got); err != nil {
			t.Fatalf("executing for id %d: %v", tc.id, err)
		}
		if got != tc.want {
			t.Errorf("id %d returned %q, want %q", tc.id, got, tc.want)
		}
	}
}

// TestMySQLPreparedMultiRow covers a result set of more than one binary row,
// where a width mistake in one row desynchronises every row after it.
func TestMySQLPreparedMultiRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name STRING)")
	for i := 1; i <= 5; i++ {
		mustExec(t, db, "INSERT INTO t VALUES ($1, $2)", i, fmt.Sprintf("row%d", i))
	}

	rows, err := db.Query("SELECT id, name FROM t WHERE id >= $1 ORDER BY id", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		n++
		if id != n {
			t.Errorf("row %d has id %d", n, id)
		}
		if want := fmt.Sprintf("row%d", n); name != want {
			t.Errorf("row %d has name %q, want %q", n, name, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("read %d rows, want 5", n)
	}
}

// TestMySQLPreparedZeroRows checks that a prepared SELECT matching nothing
// still sends its column definitions. MySQL repeats the metadata on every
// execute, so answering with a bare OK packet would leave the client unable to
// describe the empty result and reporting "no such column" on field access.
func TestMySQLPreparedZeroRows(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name STRING)")

	rows, err := db.Query("SELECT id, name FROM t WHERE id = $1", 99999)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Errorf("column count = %d, want 2", len(cols))
	}
	if rows.Next() {
		t.Error("a query matching nothing returned a row")
	}
}

// TestMySQLErrorCodes checks the numeric codes reaching a real client. These
// are what frameworks branch on to decide whether to retry, rewrite an insert
// as an update, or surface the failure.
func TestMySQLErrorCodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")
	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name STRING)")
	mustExec(t, db, "INSERT INTO t VALUES ($1, $2)", 1, "one")

	for _, tc := range []struct {
		name     string
		run      func() error
		wantCode uint16
	}{
		{
			name:     "duplicate key",
			run:      func() error { _, err := db.Exec("INSERT INTO t VALUES ($1, $2)", 1, "dup"); return err },
			wantCode: 1062, // ER_DUP_ENTRY
		},
		{
			name:     "unknown table",
			run:      func() error { _, err := db.Query("SELECT * FROM no_such_table"); return err },
			wantCode: 1146, // ER_NO_SUCH_TABLE
		},
		{
			name:     "unknown column",
			run:      func() error { _, err := db.Query("SELECT no_such_column FROM t"); return err },
			wantCode: 1054, // ER_BAD_FIELD_ERROR
		},
		{
			name:     "syntax error",
			run:      func() error { _, err := db.Exec("THIS IS NOT SQL"); return err },
			wantCode: 1064, // ER_PARSE_ERROR
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil {
				t.Fatal("expected an error")
			}
			myErr, ok := err.(*mysql.MySQLError)
			if !ok {
				t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
			}
			if myErr.Number != tc.wantCode {
				t.Errorf("code = %d (%s), want %d", myErr.Number, myErr.Message, tc.wantCode)
			}
			// None of the cases above attach a hint upstream; catch a hint
			// leaking onto one of them so this test keeps meaning something
			// beyond the numeric code, which .Number alone does not cover.
			if strings.Contains(myErr.Message, "HINT:") {
				t.Errorf("a hint-free error grew a hint: %s", myErr.Message)
			}
		})
	}
}

// TestMySQLUseDatabase covers COM_INIT_DB, which clients send both for USE and
// for the database named in the handshake.
func TestMySQLUseDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE DATABASE e2e")
	mustExec(t, db, "USE e2e")

	var name string
	if err := db.QueryRow("SELECT database()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "e2e" {
		t.Errorf("current database = %q, want e2e", name)
	}
}

// The tests above run insecure, where authentication is short-circuited to
// "trust" and no credentials are exchanged. The ones below run secure, which
// is the only mode that exercises the path from CREATE USER through the digest
// columns to a client's challenge response.

// startSecureMySQLServer starts a node that verifies credentials, and returns
// the SQL connection used to administer it.
func startSecureMySQLServer(t *testing.T) (serverutils.TestServerInterface, *gosql.DB) {
	t.Helper()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure:  false,
		MySQLAddr: util.TestAddr.String(),
	})
	if s.ServingMySQLAddr() == "" {
		t.Fatal("the MySQL listener did not report a bound address")
	}
	return s, sqlDB
}

// mysqlConnectAs attempts one MySQL connection with the given credentials and
// returns the error the server replied with, if any.
func mysqlConnectAs(t *testing.T, s serverutils.TestServerInterface, user, password string) error {
	t.Helper()
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = s.ServingMySQLAddr()
	cfg.User = user
	cfg.Passwd = password
	// caching_sha2_password is what the server nominates and what MySQL 8
	// clients use. Its fast path is a challenge-response that needs no
	// transport security; a mismatch falls through to full authentication,
	// which the driver performs over RSA.
	cfg.AllowCleartextPasswords = false

	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var n int
	return db.QueryRow("SELECT 1").Scan(&n)
}

// TestMySQLPasswordAuthentication covers the chain that no other test reaches:
// CREATE USER writes the MySQL digests alongside the bcrypt hash, and the
// listener verifies a client's challenge response against them. Every layer of
// this is covered in isolation elsewhere, but a break anywhere between them —
// a digest not written, a column read by the wrong name, a role path that
// skips the digests — leaves every unit test green and every client locked
// out.
func TestMySQLPasswordAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}

	if err := mysqlConnectAs(t, s, "mysqluser", "hunter2"); err != nil {
		t.Fatalf("the correct password was rejected: %v", err)
	}
}

// TestMySQLPasswordRejection checks that a wrong password is refused, and that
// the refusal is indistinguishable from the one an unknown account gets.
// Telling the two apart would let an unauthenticated caller enumerate accounts
// by observing which name produces a different error.
func TestMySQLPasswordRejection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}

	wrongPassword := mysqlConnectAs(t, s, "mysqluser", "not-the-password")
	unknownUser := mysqlConnectAs(t, s, "nosuchuser", "not-the-password")

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"wrong password", wrongPassword},
		{"unknown user", unknownUser},
	} {
		if tc.err == nil {
			t.Fatalf("%s: the connection was accepted", tc.name)
		}
		myErr, ok := tc.err.(*mysql.MySQLError)
		if !ok {
			t.Fatalf("%s: error is %T, not a *mysql.MySQLError: %v", tc.name, tc.err, tc.err)
		}
		// ER_ACCESS_DENIED_ERROR, which is what a MySQL server reports.
		if myErr.Number != 1045 {
			t.Errorf("%s: code = %d (%s), want 1045", tc.name, myErr.Number, myErr.Message)
		}
	}

	wrongErr, ok1 := wrongPassword.(*mysql.MySQLError)
	unknownErr, ok2 := unknownUser.(*mysql.MySQLError)
	if !ok1 || !ok2 {
		return
	}
	// The messages name the account, so they differ in that one word; what
	// must not differ is anything that reveals whether the account exists.
	normalise := func(m string) string {
		return strings.ReplaceAll(strings.ReplaceAll(m, "mysqluser", "USER"), "nosuchuser", "USER")
	}
	if normalise(wrongErr.Message) != normalise(unknownErr.Message) {
		t.Errorf("a wrong password and an unknown account are distinguishable:\n  %s\n  %s",
			wrongErr.Message, unknownErr.Message)
	}
}

// TestMySQLPasswordChange covers ALTER USER, which recomputes the digests
// through a different code path than CREATE USER. Testing only creation would
// leave an account whose password was changed authenticating against its old
// digests, or against none at all.
func TestMySQLPasswordChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'first'"); err != nil {
		t.Fatal(err)
	}
	if err := mysqlConnectAs(t, s, "mysqluser", "first"); err != nil {
		t.Fatalf("the initial password was rejected: %v", err)
	}

	if _, err := sqlDB.Exec("ALTER USER mysqluser WITH PASSWORD 'second'"); err != nil {
		t.Fatal(err)
	}
	if err := mysqlConnectAs(t, s, "mysqluser", "second"); err != nil {
		t.Fatalf("the new password was rejected: %v", err)
	}
	if err := mysqlConnectAs(t, s, "mysqluser", "first"); err == nil {
		t.Error("the old password still authenticates after the change")
	}
}

// TestMySQLPasswordlessAccount checks that an account created without a
// password cannot be reached through the MySQL listener. The digests are NULL
// for such an account, and the full-authentication fallback compares against a
// bcrypt hash that is absent; both must refuse rather than treat "nothing
// recorded" as "anything matches".
func TestMySQLPasswordlessAccount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER nopassword"); err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []string{"", "guess"} {
		if err := mysqlConnectAs(t, s, "nopassword", attempt); err == nil {
			t.Errorf("an account with no password accepted %q", attempt)
		}
	}
}

// The MySQL driver follows whichever plugin the server nominates in its
// greeting, and the server always nominates caching_sha2_password. Reaching
// the other two plugins end to end therefore needs a client that asks for one
// by name, which is what the minimal handshake below does. It also exercises
// the auth-switch exchange: a client requesting a plugin the greeting did not
// nominate sends an empty response and waits to be switched, and verifying
// that empty response instead of switching would reject every correct
// password.

// decodeGreeting parses the server's initial handshake far enough to recover
// the connection id, which a client reports as its thread id, the scramble,
// which is split across two fields for compatibility with pre-4.1 servers,
// and the status flags, which are the first thing a client learns about the
// server's escaping rules. The results come back in the order the wire
// carries them.
func decodeGreeting(
	t *testing.T, payload []byte,
) (connID uint32, scramble []byte, plugin string, statusFlags uint16) {
	t.Helper()
	r := mysqlwirebase.NewReader(payload)
	if _, err := r.Byte(); err != nil { // protocol version
		t.Fatal(err)
	}
	if _, err := r.NulString(); err != nil { // server version
		t.Fatal(err)
	}
	var err error
	if connID, err = r.Uint32(); err != nil {
		t.Fatal(err)
	}
	part1, err := r.Bytes(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Skip(1); err != nil { // filler
		t.Fatal(err)
	}
	capLow, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Skip(1); err != nil { // charset
		t.Fatal(err)
	}
	if statusFlags, err = r.Uint16(); err != nil {
		t.Fatal(err)
	}
	capHigh, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	capabilities := uint32(capLow) | uint32(capHigh)<<16
	if _, err := r.Byte(); err != nil { // auth plugin data length
		t.Fatal(err)
	}
	if err := r.Skip(10); err != nil { // reserved
		t.Fatal(err)
	}
	part2, err := r.Bytes(mysqlwirebase.ScrambleLen - 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Skip(1); err != nil { // NUL terminating part 2
		t.Fatal(err)
	}
	if capabilities&mysqlwirebase.ClientPluginAuth != 0 {
		if plugin, err = r.NulString(); err != nil {
			t.Fatal(err)
		}
	}
	return connID, append(part1, part2...), plugin, statusFlags
}

// authenticateWithPlugin performs a handshake that explicitly requests the
// given plugin, and reports the error the server ended it with, if any.
func authenticateWithPlugin(
	t *testing.T, s serverutils.TestServerInterface, user, password, plugin string,
) error {
	t.Helper()

	netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer netConn.Close()
	if err := netConn.SetDeadline(timeutil.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	pr := mysqlwirebase.NewPacketReader(bufio.NewReader(netConn), 1<<20)
	pw := mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn))

	greeting, err := pr.ReadPacket()
	if err != nil {
		t.Fatalf("reading the greeting: %v", err)
	}
	_, scramble, greetingPlugin, _ := decodeGreeting(t, greeting)

	// A client asking for the plugin the greeting nominated can answer at
	// once; one asking for a different plugin has no scramble it knows belongs
	// to that plugin, so it sends nothing and waits to be switched.
	var authResponse []byte
	var awaitingKey bool
	if plugin == greetingPlugin {
		authResponse, awaitingKey = pluginResponse(t, plugin, password, scramble)
	}

	resp := &mysqlwirebase.HandshakeResponse{
		Capabilities: mysqlwirebase.ClientProtocol41 |
			mysqlwirebase.ClientPluginAuth |
			mysqlwirebase.ClientSecureConnection,
		MaxPacketSize: 1 << 24,
		Charset:       byte(mysqlwirebase.CollationUTF8MB4GeneralCI),
		User:          user,
		AuthResponse:  authResponse,
		AuthPlugin:    plugin,
	}
	pw.SetSequence(pr.Sequence())
	if err := pw.WritePacket(resp.Encode()); err != nil {
		t.Fatal(err)
	}
	if err := pw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Drive the exchange until the server settles on OK or ERR.
	for {
		pr.SetSequence(pw.Sequence())
		payload, err := pr.ReadPacket()
		if err != nil {
			t.Fatalf("reading the server's reply: %v", err)
		}
		pw.SetSequence(pr.Sequence())

		switch payload[0] {
		case mysqlwirebase.OKPacketHeader:
			return nil

		case mysqlwirebase.ErrPacketHeader:
			return decodeErrPacket(t, payload)

		case mysqlwirebase.AuthSwitchRequestHeader:
			// AuthSwitchRequest: plugin name, then the scramble to answer
			// against.
			r := mysqlwirebase.NewReader(payload[1:])
			newPlugin, err := r.NulString()
			if err != nil {
				t.Fatal(err)
			}
			newScramble, err := r.Bytes(mysqlwirebase.ScrambleLen)
			if err != nil {
				t.Fatal(err)
			}
			scramble = newScramble
			var response []byte
			response, awaitingKey = pluginResponse(t, newPlugin, password, scramble)
			if err := pw.WritePacket(response); err != nil {
				t.Fatal(err)
			}
			if err := pw.Flush(); err != nil {
				t.Fatal(err)
			}

		case mysqlwirebase.AuthMoreDataHeader:
			if len(payload) < 2 {
				t.Fatalf("truncated AuthMoreData: % x", payload)
			}
			if awaitingKey {
				// The server answered a key request with its public key. The
				// ciphertext is bound to the scramble, so a captured exchange
				// cannot be replayed against another connection.
				pub, err := mysqlwirebase.DecodePublicKeyPEM(payload[1:])
				if err != nil {
					t.Fatalf("decoding the public key: %v", err)
				}
				ciphertext, err := mysqlwirebase.EncryptPasswordRSA(password, scramble, pub)
				if err != nil {
					t.Fatal(err)
				}
				if err := pw.WritePacket(ciphertext); err != nil {
					t.Fatal(err)
				}
				if err := pw.Flush(); err != nil {
					t.Fatal(err)
				}
				awaitingKey = false
				continue
			}
			switch payload[1] {
			case mysqlwirebase.CachingSHA2FastAuthSuccess:
				// The scramble matched; the OK packet follows separately.
				continue

			case mysqlwirebase.CachingSHA2PerformFullAuth:
				// The fast path did not match, so the password itself has to
				// reach the server. Without TLS that means encrypting it under
				// the server's RSA key, which the client asks for first.
				if err := pw.WritePacket(
					[]byte{mysqlwirebase.CachingSHA2RequestPublicKey}); err != nil {
					t.Fatal(err)
				}
				if err := pw.Flush(); err != nil {
					t.Fatal(err)
				}
				awaitingKey = true

			default:
				t.Fatalf("unexpected AuthMoreData: % x", payload)
			}

		default:
			t.Fatalf("unexpected packet header 0x%02x", payload[0])
		}
	}
}

// pluginResponse computes the client's answer for one plugin, and reports
// whether the server is expected to reply with its public key.
func pluginResponse(
	t *testing.T, plugin, password string, scramble []byte,
) (response []byte, awaitsPublicKey bool) {
	t.Helper()
	switch plugin {
	case mysqlwirebase.AuthNativePassword:
		return mysqlwirebase.NativePasswordResponse(password, scramble), false
	case mysqlwirebase.AuthCachingSHA2Password:
		return mysqlwirebase.CachingSHA2Response(password, scramble), false
	case mysqlwirebase.AuthSHA256Password:
		// sha256_password has no challenge-response fast path: the password
		// itself must reach the server every time. Off TLS that means asking
		// for the public key first, which is what this single byte does.
		return []byte{mysqlwirebase.SHA256RequestPublicKey}, true
	default:
		t.Fatalf("no client implementation for plugin %q", plugin)
		return nil, false
	}
}

// decodeErrPacket turns an ERR packet into a *mysql.MySQLError so that the
// assertions match those made against the driver elsewhere in this file.
func decodeErrPacket(t *testing.T, payload []byte) error {
	t.Helper()
	r := mysqlwirebase.NewReader(payload[1:])
	code, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if marker, err := r.Byte(); err == nil && marker == '#' {
		if err := r.Skip(5); err != nil { // SQLSTATE
			t.Fatal(err)
		}
	}
	return &mysql.MySQLError{Number: code, Message: string(r.Remaining())}
}

// TestMySQLAuthPlugins covers the plugins the driver cannot be made to request.
// mysql_native_password verifies against a different digest column than
// caching_sha2_password, so an account reachable through one says nothing
// about the other, and the auth-switch exchange that gets a client onto a
// non-nominated plugin has no other end-to-end coverage.
func TestMySQLAuthPlugins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}

	for _, plugin := range []string{
		mysqlwirebase.AuthNativePassword,
		mysqlwirebase.AuthCachingSHA2Password,
		mysqlwirebase.AuthSHA256Password,
	} {
		t.Run(plugin, func(t *testing.T) {
			if err := authenticateWithPlugin(t, s, "mysqluser", "hunter2", plugin); err != nil {
				t.Errorf("the correct password was rejected: %v", err)
			}
			err := authenticateWithPlugin(t, s, "mysqluser", "wrong", plugin)
			if err == nil {
				t.Fatal("a wrong password was accepted")
			}
			myErr, ok := err.(*mysql.MySQLError)
			if !ok {
				t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
			}
			if myErr.Number != 1045 {
				t.Errorf("code = %d (%s), want 1045", myErr.Number, myErr.Message)
			}
		})
	}
}

// TLS is negotiated before the handshake response: a client that wants it
// sends a truncated SSLRequest, upgrades the socket, and then restarts the
// handshake inside the session with the packet sequence continuing across the
// upgrade. Everything above connects in the clear, so this is the only
// coverage of that path.

// mysqlTLSConfigName registers the test CA once and returns the name to put in
// a DSN. go-sql-driver looks TLS configurations up by name rather than taking
// one inline.
// mysqlTLSClientConfig builds the client side of the test PKI. Verifying
// against the CA rather than skipping verification means a server that
// presented no certificate, or one signed by something else, fails the caller
// rather than silently downgrading it.
func mysqlTLSClientConfig(t *testing.T) *tls.Config {
	t.Helper()
	caPEM, err := securitytest.EmbeddedAssets.ReadFile(
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("the embedded CA certificate could not be parsed")
	}
	return &tls.Config{
		RootCAs: pool,
		// The test certificates are issued for localhost, while the listener is
		// reached at 127.0.0.1, so the name has to be given explicitly.
		ServerName: "localhost",
	}
}

func mysqlTLSConfigName(t *testing.T) string {
	t.Helper()
	const name = "kwdb-test-ca"
	if err := mysql.RegisterTLSConfig(name, mysqlTLSClientConfig(t)); err != nil {
		t.Fatal(err)
	}
	return name
}

// coalesceFirstWrite prepends prefix to the connection's first write so both
// reach the peer in one segment. crypto/tls writes the ClientHello from the
// goroutine that drives the handshake, so no synchronization is needed here.
type coalesceFirstWrite struct {
	net.Conn
	prefix []byte
	sent   bool
}

func (c *coalesceFirstWrite) Write(p []byte) (int, error) {
	if c.sent {
		return c.Conn.Write(p)
	}
	c.sent = true
	joined := make([]byte, 0, len(c.prefix)+len(p))
	joined = append(append(joined, c.prefix...), p...)
	if _, err := c.Conn.Write(joined); err != nil {
		return 0, err
	}
	return len(p), nil
}

// TestMySQLTLSClientHelloArrivesWithSSLRequest pins the case where a client
// sends its SSLRequest and the TLS ClientHello that follows without waiting in
// between, so both reach the server in a single read. The server reads the
// 32-byte SSLRequest through a buffered reader, which takes whatever else the
// socket has ready; if the TLS handshake is then run on the bare connection
// those bytes are unreachable and both ends wait for each other forever. The
// deadline below turns that into a reported failure rather than a hung binary.
func TestMySQLTLSClientHelloArrivesWithSSLRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer netConn.Close()
	if err := netConn.SetDeadline(timeutil.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	pr := mysqlwirebase.NewPacketReader(bufio.NewReader(netConn), 1<<20)
	if _, err := pr.ReadPacket(); err != nil {
		t.Fatalf("reading the greeting: %v", err)
	}

	// Frame the SSLRequest by hand: it has to travel with the ClientHello
	// rather than be written on its own, which is what the wrapper below does.
	payload := mysqlwirebase.EncodeSSLRequest(
		mysqlwirebase.ClientProtocol41|mysqlwirebase.ClientSSL,
		1<<24, byte(mysqlwirebase.CollationUTF8MB4GeneralCI),
	)
	framed := []byte{
		byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16),
		pr.Sequence(),
	}
	framed = append(framed, payload...)

	tlsConn := tls.Client(
		&coalesceFirstWrite{Conn: netConn, prefix: framed},
		mysqlTLSClientConfig(t),
	)
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("the TLS handshake did not complete: %v", err)
	}
}

// TestMySQLTLSConnection checks that a client can upgrade the connection and
// then authenticate and query inside the TLS session.
func TestMySQLTLSConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = s.ServingMySQLAddr()
	cfg.User = "mysqluser"
	cfg.Passwd = "hunter2"
	cfg.TLSConfig = mysqlTLSConfigName(t)

	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var got int
	if err := db.QueryRow("SELECT 1").Scan(&got); err != nil {
		t.Fatalf("querying over TLS: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 returned %d", got)
	}

	// The prepared-statement path has to keep working across the upgrade: the
	// packet reader and writer are rebuilt around the TLS connection, and
	// their sequence numbers have to carry over.
	var two int
	if err := db.QueryRow("SELECT $1::INT", 2).Scan(&two); err != nil {
		t.Fatalf("prepared statement over TLS: %v", err)
	}
	if two != 2 {
		t.Errorf("prepared SELECT returned %d, want 2", two)
	}
}

// TestMySQLTLSRejectsWrongPassword checks that upgrading the transport does
// not bypass credential checking. Over TLS the cleartext plugins hand the
// password over directly, so this is a different verification path than the
// challenge-response used in the clear.
func TestMySQLTLSRejectsWrongPassword(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB := startSecureMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = s.ServingMySQLAddr()
	cfg.User = "mysqluser"
	cfg.Passwd = "wrong"
	cfg.TLSConfig = mysqlTLSConfigName(t)

	db, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var n int
	err = db.QueryRow("SELECT 1").Scan(&n)
	if err == nil {
		t.Fatal("a wrong password was accepted over TLS")
	}
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	}
	if myErr.Number != 1045 {
		t.Errorf("code = %d (%s), want 1045", myErr.Number, myErr.Message)
	}
}

// TestMySQLTLSRefusedWhenInsecure checks the refusal a client gets when it asks
// for TLS from a node that has none. The greeting does not advertise
// CLIENT_SSL in that case, but a client can still send an SSLRequest, and the
// server has to answer with an error rather than attempting an upgrade it
// cannot perform.
func TestMySQLTLSRefusedWhenInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer netConn.Close()
	if err := netConn.SetDeadline(timeutil.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	pr := mysqlwirebase.NewPacketReader(bufio.NewReader(netConn), 1<<20)
	pw := mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn))

	greeting, err := pr.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, plugin, _ := decodeGreeting(t, greeting); plugin == "" {
		t.Fatal("the greeting named no authentication plugin")
	}

	pw.SetSequence(pr.Sequence())
	if err := pw.WritePacket(mysqlwirebase.EncodeSSLRequest(
		mysqlwirebase.ClientProtocol41|mysqlwirebase.ClientSSL,
		1<<24, byte(mysqlwirebase.CollationUTF8MB4GeneralCI),
	)); err != nil {
		t.Fatal(err)
	}
	if err := pw.Flush(); err != nil {
		t.Fatal(err)
	}

	pr.SetSequence(pw.Sequence())
	payload, err := pr.ReadPacket()
	if err != nil {
		t.Fatalf("reading the refusal: %v", err)
	}
	if payload[0] != mysqlwirebase.ErrPacketHeader {
		t.Fatalf("packet header = 0x%02x, want an ERR packet", payload[0])
	}
	if err := decodeErrPacket(t, payload); err == nil {
		t.Fatal("the ERR packet carried no message")
	} else if !strings.Contains(err.Error(), "TLS") {
		t.Errorf("the refusal does not mention TLS: %v", err)
	}
}

// TestMySQLTimeSeriesRead covers reading a time-series table over the MySQL
// protocol. The TS engine can emit pre-encoded PostgreSQL wire bytes through a
// short circuit; that path is unreadable by a MySQL client, so the planner must
// not choose it for this connection. Without that guard every query below comes
// back with column metadata and zero rows, and no error.
func TestMySQLTimeSeriesRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServerWithStore(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "CREATE TS DATABASE tse2e")
	mustExec(t, db, `CREATE TABLE tse2e.sensor (
		k_timestamp TIMESTAMPTZ NOT NULL,
		temp        FLOAT
	) TAGS (device_id INT NOT NULL) PRIMARY TAGS(device_id)`)
	mustExec(t, db, "INSERT INTO tse2e.sensor VALUES ('2026-09-03 08:00:00', 21.5, 1)")
	mustExec(t, db, "INSERT INTO tse2e.sensor VALUES ('2026-09-03 08:00:10', 22.1, 1)")
	mustExec(t, db, "INSERT INTO tse2e.sensor VALUES ('2026-09-03 08:00:20', 23.7, 1)")

	var n int
	if err := db.QueryRow("SELECT count(*) FROM tse2e.sensor").Scan(&n); err != nil {
		t.Fatalf("count over the MySQL protocol: %v", err)
	}
	if n != 3 {
		t.Errorf("count(*) returned %d, want 3", n)
	}

	rows, err := db.Query("SELECT temp FROM tse2e.sensor ORDER BY k_timestamp")
	if err != nil {
		t.Fatalf("scan over the MySQL protocol: %v", err)
	}
	defer rows.Close()
	var got []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []float64{21.5, 22.1, 23.7}
	if len(got) != len(want) {
		t.Fatalf("scan returned %d rows (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %v, want %v", i, got[i], want[i])
		}
	}

	// last() is a TS-engine builtin, so it exercises the same execution path as
	// the scans above rather than falling back to relational planning.
	var last float64
	if err := db.QueryRow("SELECT last(temp) FROM tse2e.sensor").Scan(&last); err != nil {
		t.Fatalf("last() over the MySQL protocol: %v", err)
	}
	if last != 23.7 {
		t.Errorf("last(temp) = %v, want 23.7", last)
	}
}

// TestMySQLSetNamesCollate covers the statement MySQL connector libraries send
// during the connection phase. Rejecting it does not degrade behavior, it
// stops the connection from being established at all.
func TestMySQLSetNamesCollate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	if _, err := db.Exec("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'"); err != nil {
		t.Fatalf("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci': %v", err)
	}
	// The rewrite has to produce a statement that is not just parseable but
	// semantically correct: confirm the charset it carries actually reached
	// client_encoding rather than being dropped along with COLLATE.
	var encoding string
	if err := db.QueryRow("SHOW client_encoding").Scan(&encoding); err != nil {
		t.Fatal(err)
	}
	if encoding != "utf8" {
		t.Errorf("client_encoding = %q, want %q", encoding, "utf8")
	}

	for _, q := range []string{
		"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
		"SET NAMES 'utf8mb4'",
		"SET NAMES utf8mb4",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}

	// The session must still be usable, i.e. the charset actually took effect
	// rather than the statement being swallowed.
	var got string
	if err := db.QueryRow("SELECT '中文'").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "中文" {
		t.Errorf("round-tripped %q, want %q", got, "中文")
	}

	// A COLLATE that is not part of SET NAMES must still reach the SQL layer.
	if _, err := db.Exec("SET NAMES utf8mb4 COLLATE"); err == nil {
		// stripSetNamesCollate leaves this statement alone (setnames_test.go
		// pins the dangling-COLLATE case), so the parser has to reject it and
		// a nil here means the driver matched some other response to it. Probe
		// the connection: if the responses have slipped by one, this sentinel
		// comes back carrying the rejection that belonged to the statement
		// above rather than its own row.
		var sentinel string
		probeErr := db.QueryRow("SELECT 'sentinel-42'").Scan(&sentinel)
		t.Errorf("a malformed COLLATE clause should still be rejected; "+
			"a follow-up SELECT returned %q with err %v", sentinel, probeErr)
	}

	// SET NAMES latin1: an unrecognized charset must still error, and the
	// error must not carry the errors library's automatic "file an issue"
	// hint. vars.go's client_encoding Set routes an unrecognized value
	// through unimplemented.NewWithIssueDetailf, which is the same call that
	// populates the issuelink.UnimplementedErrorHint entry in
	// boilerplateHints (conn.go) — this is the end-to-end check that the
	// entry actually reaches a live connection, not only an isolated table
	// test.
	if _, err := db.Exec("SET NAMES latin1"); err == nil {
		t.Error("SET NAMES latin1 should be rejected: KWDB only emits UTF-8")
	} else if strings.Contains(err.Error(), "We appreciate your feedback") {
		t.Errorf("the errors-library boilerplate hint leaked to the client: %v", err)
	}
}

// TestMySQLSetCharacterSetResultsNull covers the other statement Connector/J
// sends during the connection phase, right after it has been told
// character_set_results is utf8mb4. In MySQL the assignment means "do not
// transcode the result set on the server"; KWDB emits UTF-8 and never
// transcodes, so there is nothing to switch off and the request is honored by
// doing nothing. Rejecting it, which is what KWDB does today, stops the
// connection from being established at all.
func TestMySQLSetCharacterSetResultsNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	for _, q := range []string{
		"SET character_set_results = NULL",
		"SET SESSION character_set_results = NULL",
		"set character_set_results=null;",
		"SET character_set_results TO NULL",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}

	// A no-op, not a reset. The variable has to still report what the session
	// had, because the client asked for a conversion to stop happening, not for
	// the value to change.
	var charset string
	if err := db.QueryRow("SHOW character_set_results").Scan(&charset); err != nil {
		t.Fatal(err)
	}
	if charset != "utf8mb4" {
		t.Errorf("character_set_results = %q after SET ... = NULL, want %q",
			charset, "utf8mb4")
	}

	// The session must still be usable and still return UTF-8, i.e. the
	// statement was absorbed rather than turning something off.
	var got string
	if err := db.QueryRow("SELECT '中文'").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "中文" {
		t.Errorf("round-tripped %q, want %q", got, "中文")
	}

	// The fail-open boundary. NULLIF is a different word, so this statement
	// reaches the SQL layer, which evaluates it and rejects the resulting 'a'.
	// A rewrite that matched on the prefix NULL would have made it succeed and
	// left the variable silently unset.
	_, err := db.Exec("SET character_set_results = NULLIF('a','b')")
	if err == nil {
		t.Fatal("SET character_set_results = NULLIF('a','b') should still be rejected")
	}
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	}
	if myErr.Number != 1105 { // ER_UNKNOWN_ERROR
		t.Errorf("code = %d (%s), want 1105", myErr.Number, myErr.Message)
	}
	// The message has to still quote the value the SQL layer evaluated, which
	// is the evidence the statement arrived as the client wrote it.
	if !strings.Contains(myErr.Message, `"a"`) {
		t.Errorf("message %q does not name the evaluated value", myErr.Message)
	}

	// Only the one variable named is answered. Accepting NULL for the rest
	// would be a change to what SET means, made in the wrong layer.
	if _, err := db.Exec("SET character_set_client = NULL"); err == nil {
		t.Error("SET character_set_client = NULL should still be rejected")
	}
}

// TestMySQLAutocommitRejectionIsActionable pins the rejection and its hint.
// PyMySQL and mysqlclient send SET AUTOCOMMIT = false during the connection
// phase, so this error is the first thing many users will see; it has to say
// what to do about it. The rejection itself is deliberate: KWDB commits every
// statement outside an explicit transaction and will not pretend otherwise.
func TestMySQLAutocommitRejectionIsActionable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	if _, err := db.Exec("SET autocommit = 1"); err != nil {
		t.Errorf("autocommit=1 must be accepted: %v", err)
	}

	_, err := db.Exec("SET autocommit = 0")
	if err == nil {
		t.Fatal("autocommit=0 must be rejected rather than silently ignored")
	}
	msg := err.Error()
	for _, want := range []string{"autocommit", "explicit transaction", "autocommit=True"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the rejection should mention %q; got: %s", want, msg)
		}
	}
	if strings.Contains(msg, "CockroachDB") {
		t.Errorf("the rejection leaks the upstream product name: %s", msg)
	}
	if strings.Contains(msg, "no effect") {
		t.Errorf("autocommit is honored, not merely tolerated; got: %s", msg)
	}
}

// TestMySQLAssertionFailureHintIsNotLeaked pins that the boilerplate hint the
// errors library attaches automatically to assertion failures (a
// "You have encountered an unexpected error. ... We appreciate your
// feedback." block addressed to a KWDB developer, not to the client whose
// query just failed) never reaches a MySQL client, even though bufferErr
// does forward a hand-written hint like the autocommit one.
//
// kwdb_internal.force_assertion_error is the established way to trigger a
// genuine errors.AssertionFailedf over a live connection for exactly this
// kind of test: pkg/server/updates_test.go already uses it the same way, so
// this is not a contrived error, just a real one driven over the MySQL wire
// instead of pgwire.
func TestMySQLAssertionFailureHintIsNotLeaked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	_, err := db.Exec(`SELECT kwdb_internal.force_assertion_error('boom')`)
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, boilerplate := range []string{
		"We appreciate your feedback",
		"encountered an unexpected error",
	} {
		if strings.Contains(msg, boilerplate) {
			t.Errorf("assertion-failure boilerplate leaked to the client: %s", msg)
		}
	}
}

// TestMySQLGreetingDeclaresBackslashHandling reads the status flags off the
// greeting itself. The driver-level test below only proves the OK packet that
// ends the connection phase carries the rule; a client that takes its state
// from the greeting instead has to be told the same thing there, or the same
// server describes its own lexer two ways.
func TestMySQLGreetingDeclaresBackslashHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer netConn.Close()
	if err := netConn.SetDeadline(timeutil.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	pr := mysqlwirebase.NewPacketReader(bufio.NewReader(netConn), 1<<20)
	payload, err := pr.ReadPacket()
	if err != nil {
		t.Fatalf("reading the greeting: %v", err)
	}
	_, _, _, status := decodeGreeting(t, payload)
	if status&mysqlwirebase.ServerStatusNoBackslashEscapes == 0 {
		t.Errorf("greeting status flags = %#04x, want ServerStatusNoBackslashEscapes (%#04x) set",
			status, mysqlwirebase.ServerStatusNoBackslashEscapes)
	}
}

// readGreetingConnID dials the MySQL listener, reads the greeting the server
// speaks first, and reports the connection id it announced. Nothing
// authenticates: the id is assigned before the server knows anything about the
// client, so the greeting alone settles what was handed out.
func readGreetingConnID(t *testing.T, netConn net.Conn) uint32 {
	t.Helper()
	if err := netConn.SetDeadline(timeutil.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	pr := mysqlwirebase.NewPacketReader(bufio.NewReader(netConn), 1<<20)
	payload, err := pr.ReadPacket()
	if err != nil {
		t.Fatalf("reading the greeting: %v", err)
	}
	connID, _, _, _ := decodeGreeting(t, payload)
	return connID
}

// TestMySQLConnectionIDIsUniquePerConnection pins the greeting's connection id
// to being an identifier rather than a count. A client hands this number back
// as the target of KILL and expects CONNECTION_ID() to agree with it, so two
// sessions sharing one number, or one number naming different sessions over
// time, is a wrong answer that no error ever reports.
func TestMySQLConnectionIDIsUniquePerConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	// Serial connections are the case a gauge of live connections gets wrong
	// most quietly: each one is the only connection open while it runs, so the
	// gauge reads 1 every time and every session in a row is called 1.
	for want := uint32(1); want <= 3; want++ {
		func() {
			netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
			if err != nil {
				t.Fatal(err)
			}
			defer netConn.Close()
			if got := readGreetingConnID(t, netConn); got != want {
				t.Errorf("serial connection %d: greeting connection id = %d, want %d",
					want, got, want)
			}
		}()
	}

	// Connections that are all open at once must differ from each other. The
	// ids are read while every connection is still live, because a number that
	// only becomes free again on disconnect is not what makes it unique.
	const concurrent = 4
	seen := make(map[uint32]int, concurrent)
	for i := 0; i < concurrent; i++ {
		netConn, err := net.Dial("tcp", s.ServingMySQLAddr())
		if err != nil {
			t.Fatal(err)
		}
		defer netConn.Close()
		got := readGreetingConnID(t, netConn)
		if prev, dup := seen[got]; dup {
			t.Errorf("connections %d and %d were both given connection id %d",
				prev, i, got)
		}
		seen[got] = i
	}
}

// TestMySQLBackslashRoundTrip is what the status flag is for. A client that
// interpolates arguments into the statement text picks its escaping rule from
// what the server declared, so an undeclared rule is silent data corruption
// rather than an error: an embedded quote makes the statement fail with a
// lexical error, and a backslash is stored doubled.
//
// InterpolateParams is deliberately the opposite of every other test in this
// file. With server-side prepares the argument never reaches the wire as text,
// which is exactly why this hole stayed hidden.
func TestMySQLBackslashRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = s.ServingMySQLAddr()
	cfg.User = "root"
	cfg.InterpolateParams = true
	interpDB, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer interpDB.Close()
	interpDB.SetMaxOpenConns(1)

	mustExec(t, interpDB, "CREATE DATABASE e2e")
	mustExec(t, interpDB, "USE e2e")
	mustExec(t, interpDB, "CREATE TABLE escapes (id INT PRIMARY KEY, v STRING)")

	values := []string{
		// A quote has to be doubled rather than backslash-escaped, or the
		// literal is left unterminated and the statement fails outright.
		`O'Brien`,
		// A backslash has to be passed through rather than doubled, or the
		// stored value gains a character nobody sent.
		`a\b`,
		`c:\tmp\new`,
		`mixed \ and ' together`,
	}
	for i, v := range values {
		// The driver interpolates only when the number of `?` matches the
		// number of arguments, so these cannot use the $1 form the rest of the
		// file uses: with $1 it would fall back to a server-side prepare and
		// quietly stop testing anything.
		mustExec(t, interpDB, "INSERT INTO escapes (id, v) VALUES (?, ?)", i, v)
	}

	rows, err := interpDB.Query("SELECT id, v FROM escapes ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(values) {
		t.Fatalf("read back %d row(s), want %d: %q", len(got), len(values), got)
	}
	for i := range values {
		if got[i] != values[i] {
			t.Errorf("row %d round-tripped %q (%d bytes), want %q (%d bytes)",
				i, got[i], len(got[i]), values[i], len(values[i]))
		}
	}
}

// driverProbeQueryE2E is the statement Connector/J 8.4.0 sends immediately
// after the handshake, taken verbatim from NativeSession.loadServerVariables.
// It is duplicated from sysvars_test.go on purpose: that copy pins what the
// rewrite produces, this one pins that the product still runs on a live node.
const driverProbeQueryE2E = `/*mysql-connector-j-8.4.0 (Revision: 1c3f5c149e0bfe31c7fbeb24e2d260cd890972c4) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

// driverProbeColumns are the labels the driver indexes the result by. It reads
// the row into a map keyed by these, so a label that does not match means the
// value is simply absent and the driver falls back to a default without saying
// so.
var driverProbeColumns = []string{
	"auto_increment_increment", "character_set_client", "character_set_connection",
	"character_set_results", "character_set_server", "collation_server",
	"collation_connection", "init_connect", "interactive_timeout", "license",
	"lower_case_table_names", "max_allowed_packet", "net_write_timeout",
	"performance_schema", "sql_mode", "system_time_zone", "time_zone",
	"transaction_isolation", "wait_timeout",
}

// TestMySQLDriverProbeQuery runs the driver's connection-phase query end to
// end. Nothing about it degrades gracefully: it is one statement, so a single
// column that errors takes the other eighteen with it and the connection never
// comes up. The value assertions are the ones where handing back KWDB's own
// answer would be read wrongly rather than rejected.
func TestMySQLDriverProbeQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	got, cols := queryProbeRow(t, db, driverProbeQueryE2E)

	if len(cols) != len(driverProbeColumns) {
		t.Fatalf("got %d columns %q, want %d", len(cols), cols, len(driverProbeColumns))
	}
	for i, want := range driverProbeColumns {
		if cols[i] != want {
			t.Errorf("column %d is labeled %q, want %q", i, cols[i], want)
		}
	}

	for name, want := range map[string]string{
		"character_set_client":     "utf8mb4",
		"character_set_connection": "utf8mb4",
		"character_set_results":    "utf8mb4",
		"character_set_server":     "utf8mb4",
		"collation_server":         "utf8mb4_general_ci",
		"collation_connection":     "utf8mb4_general_ci",
		"interactive_timeout":      "28800",
		"net_write_timeout":        "60",
		"wait_timeout":             "28800",
		"time_zone":                "UTC",
		// The driver looks this up in a case-sensitive map keyed with hyphens;
		// KWDB's own `serializable` matches no entry there.
		"transaction_isolation": "SERIALIZABLE",
		// Integer.parseInt, so a value carrying a unit suffix would be read as
		// -1 and leave the client capping statements at 64KB.
		"max_allowed_packet": "67108864",
		// Tells the driver to fold identifiers before comparing metadata, which
		// is what KWDB does to unquoted names.
		"lower_case_table_names": "1",
	} {
		if v := got[name]; !v.Valid || v.String != want {
			t.Errorf("%s = %v, want %q", name, nullStr(v), want)
		}
	}

	// A backslash inside a literal is a backslash here, and `"` quotes an
	// identifier. A driver not told both interpolates its parameters wrongly
	// and corrupts data rather than failing.
	sqlMode := got["sql_mode"]
	if !sqlMode.Valid {
		t.Errorf("sql_mode is NULL")
	} else {
		for _, mode := range []string{"ANSI_QUOTES", "NO_BACKSLASH_ESCAPES"} {
			if !strings.Contains(sqlMode.String, mode) {
				t.Errorf("sql_mode = %q, want it to declare %s", sqlMode.String, mode)
			}
		}
	}

	// NULL is the honest answer for a MySQL setting KWDB has no equivalent of:
	// it says this server holds no value under that name. Reporting a made-up
	// one would have the driver act on a facility that is not there.
	for _, name := range []string{
		"auto_increment_increment", "init_connect", "license",
		"performance_schema", "system_time_zone",
	} {
		if v := got[name]; v.Valid {
			t.Errorf("%s = %q, want NULL: KWDB has no such setting", name, v.String)
		}
	}
}

// TestMySQLSystemVariablesAreAnswerable sends every name the rewrite has a
// covering row for, plus a sample of the fall-through path, in one statement.
//
// The table tests can only show what the rewrite produces; they cannot show
// that what it produced stands up in the SQL layer. A misspelled variable name
// or a function that does not exist shows up here and nowhere else. One
// statement rather than many is deliberate: that is how the driver asks, and it
// is the shape in which one bad expression fails all the others.
func TestMySQLSystemVariablesAreAnswerable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	// Every key of mysqlSystemVarExpr, which TestSysVarCoverageIsPinned holds
	// this list in step with, followed by fall-through samples: one variable
	// KWDB really has and one it has never heard of.
	covered := []string{
		"@@autocommit", "@@character_set_server", "@@lower_case_table_names",
		"@@sql_mode", "@@transaction_isolation", "@@transaction_read_only",
		"@@tx_isolation", "@@tx_read_only", "@@version",
		"@@version_comment", "@@zz_no_such_variable",
	}
	got, cols := queryProbeRow(t, db, "SELECT "+strings.Join(covered, ", "))

	for i, name := range covered {
		// The synthesized label has to be the expression as the client wrote
		// it, or a client indexing its result by label finds nothing.
		if cols[i] != name {
			t.Errorf("column %d is labeled %q, want %q", i, cols[i], name)
		}
	}

	for name, want := range map[string]string{
		"@@autocommit":             "1",
		"@@character_set_server":   "utf8mb4",
		"@@lower_case_table_names": "1",
		"@@transaction_isolation":  "SERIALIZABLE",
		"@@transaction_read_only":  "0",
		"@@tx_isolation":           "SERIALIZABLE",
		"@@tx_read_only":           "0",
		// Must be the string the handshake greeting carries: one server, one
		// version story. sysvars.go builds it from that same constant.
		"@@version":         "8.0.11-KWDB",
		"@@version_comment": "KaiwuDB",
	} {
		if v := got[name]; !v.Valid || v.String != want {
			t.Errorf("%s = %v, want %q", name, nullStr(v), want)
		}
	}
	if v := got["@@sql_mode"]; !v.Valid || !strings.Contains(v.String, "ANSI_QUOTES") {
		t.Errorf("@@sql_mode = %v, want it to declare ANSI_QUOTES", nullStr(v))
	}
	// The whole point of the two-argument current_setting: an unknown name
	// yields NULL instead of failing the statement, so a variable renamed
	// anywhere in KWDB degrades one column rather than refusing the connection.
	if v := got["@@zz_no_such_variable"]; v.Valid {
		t.Errorf("@@zz_no_such_variable = %q, want NULL", v.String)
	}
}

// TestMySQLSystemVariableRewriteLeavesLiteralsAlone is the live-node half of
// the fail-open contract: the rewrite must not reach text that merely looks
// like a variable reference, and the forms it refuses must still be refused.
func TestMySQLSystemVariableRewriteLeavesLiteralsAlone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	for _, tc := range []struct{ query, want string }{
		{"SELECT '@@version_comment'", "@@version_comment"},
		{"SELECT '中文'", "中文"},
		// # is the bitwise XOR operator, not a comment: 5 XOR 3 is 6. A rewrite
		// that treated it as one would eat the right operand.
		{"SELECT 5 # 3", "6"},
	} {
		var got string
		if err := db.QueryRow(tc.query).Scan(&got); err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s returned %q, want %q", tc.query, got, tc.want)
		}
	}

	// Refused on purpose. MySQL's global and session scopes are two value
	// spaces and KWDB has one, so answering a global read with the session
	// value would be a plausible wrong answer.
	_, err := db.Query("SELECT @@global.time_zone")
	if err == nil {
		t.Fatal("SELECT @@global.time_zone should still be rejected")
	}
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	}
	if myErr.Number != 1064 { // ER_PARSE_ERROR
		t.Errorf("code = %d (%s), want 1064", myErr.Number, myErr.Message)
	}
	// The parser's own hint names the statement it was reading and is accurate,
	// so it stays. What must not appear is the errors library's boilerplate or
	// the upstream project's name, neither of which tells a MySQL client
	// anything it can act on.
	for _, leak := range []string{"CockroachDB", "We appreciate your feedback"} {
		if strings.Contains(myErr.Message, leak) {
			t.Errorf("error message leaked %q: %s", leak, myErr.Message)
		}
	}
}

// TestMySQLSystemVariablePreparedPath pins that COM_STMT_PREPARE and COM_QUERY
// end up at the same statement. They reach the rewrite from two different
// call sites, and a client that prepares its session setup rather than issuing
// it directly -- which connection pools do -- would otherwise be served by
// whichever of the two was not kept up to date.
func TestMySQLSystemVariablePreparedPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	// startMySQLServer leaves interpolateParams off, so Prepare really does
	// issue COM_STMT_PREPARE rather than rewriting on the client.
	for _, query := range []string{
		driverProbeQueryE2E,
		"SELECT @@sql_mode, @@transaction_isolation AS iso",
	} {
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatalf("preparing %.60q: %v", query, err)
		}
		rows, err := stmt.Query()
		if err != nil {
			t.Fatalf("running the prepared %.60q: %v", query, err)
		}
		prepared, preparedCols := scanProbeRow(t, rows)
		direct, directCols := queryProbeRow(t, db, query)
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}

		if strings.Join(preparedCols, ",") != strings.Join(directCols, ",") {
			t.Errorf("%.60q: prepared columns %q, direct %q", query, preparedCols, directCols)
		}
		for name, want := range direct {
			if got := prepared[name]; got != want {
				t.Errorf("%.60q: %s = %v when prepared, %v when queried directly",
					query, name, nullStr(got), nullStr(want))
			}
		}
	}
}

// greetingSniffer wraps a connection to the MySQL listener and keeps the first
// packet the server writes, which is the greeting. The driver does not expose
// the connection id it was handed, and an id read off any other socket would
// say nothing about the session running the query, so both numbers have to come
// from the one connection.
type greetingSniffer struct {
	net.Conn
	mu struct {
		syncutil.Mutex
		// buf accumulates until the first packet is whole. A TCP read is not
		// obliged to deliver exactly one packet.
		buf []byte
		// payload is the greeting once buf holds all of it.
		payload []byte
	}
}

func (g *greetingSniffer) Read(p []byte) (int, error) {
	n, err := g.Conn.Read(p)
	if n <= 0 {
		return n, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mu.payload != nil {
		return n, err
	}
	g.mu.buf = append(g.mu.buf, p[:n]...)
	if len(g.mu.buf) < 4 {
		return n, err
	}
	// A packet is a three-byte little-endian payload length, a sequence
	// number, then the payload.
	size := int(g.mu.buf[0]) | int(g.mu.buf[1])<<8 | int(g.mu.buf[2])<<16
	if len(g.mu.buf) >= 4+size {
		g.mu.payload = g.mu.buf[4 : 4+size]
	}
	return n, err
}

// greeting returns the greeting payload, or nil if the packet never arrived
// whole.
func (g *greetingSniffer) greeting() []byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.payload
}

// TestMySQLConnectionIDMatchesGreeting pins that CONNECTION_ID() answers with
// the number this same connection was given in its greeting. A client keeps
// that number as its thread id and hands it back -- to KILL, to a monitoring
// query, to its own logs -- so a server that answers with a second number is
// giving a wrong answer that no error ever reports.
func TestMySQLConnectionIDMatchesGreeting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	// The driver reaches the listener through a dial function of ours. It is
	// the only place the greeting can be read without also re-implementing
	// authentication and the text protocol on a bare socket.
	sniffed := make(chan *greetingSniffer, 1)
	netName := "kwdb-greeting-sniffer-" + t.Name()
	mysql.RegisterDial(netName, func(addr string) (net.Conn, error) {
		netConn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		g := &greetingSniffer{Conn: netConn}
		select {
		case sniffed <- g:
		default: // Only the first connection is examined.
		}
		return g, nil
	})

	cfg := mysql.NewConfig()
	cfg.Net = netName
	cfg.Addr = s.ServingMySQLAddr()
	cfg.User = "root"
	sniffDB, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer sniffDB.Close()
	// One connection, so the greeting read below belongs to the session that
	// answered the query.
	sniffDB.SetMaxOpenConns(1)

	row, cols := queryProbeRow(t, sniffDB, "SELECT CONNECTION_ID()")
	if len(cols) != 1 || cols[0] != "CONNECTION_ID()" {
		t.Errorf("columns = %q, want exactly [CONNECTION_ID()]", cols)
	}
	answered, err := strconv.ParseUint(row["CONNECTION_ID()"].String, 10, 32)
	if err != nil {
		t.Fatalf("CONNECTION_ID() returned %s: %v", nullStr(row["CONNECTION_ID()"]), err)
	}

	var g *greetingSniffer
	select {
	case g = <-sniffed:
	default:
		t.Fatal("the driver never went through the test's dial function")
	}
	payload := g.greeting()
	if payload == nil {
		t.Fatal("the greeting packet was never seen whole")
	}
	announced, _, _, _ := decodeGreeting(t, payload)
	if uint32(answered) != announced {
		t.Errorf("CONNECTION_ID() = %d, but this connection's greeting announced %d",
			answered, announced)
	}
}

// TestMySQLUserFunction covers USER(), which nothing outside this package could
// answer: USER is a reserved word in KWDB's grammar, so the statement fails at
// the opening paren before any builtin lookup happens.
//
// The answer has to follow the account the session authenticated as rather than
// being a constant, which is what the second half checks on a server that
// really verifies a password.
func TestMySQLUserFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	got, cols := queryProbeRow(t, db, "SELECT USER()")
	// MySQL labels the column with the call as written. Without that a client
	// reading its result set by label finds nothing.
	if len(cols) != 1 || cols[0] != "USER()" {
		t.Errorf("columns = %q, want exactly [USER()]", cols)
	}
	if v := got["USER()"]; !v.Valid || v.String != "root" {
		t.Errorf("USER() = %s, want %q", nullStr(v), "root")
	}

	// A literal that spells the call is not a call: the rewrite recognizes a
	// prefix and cannot reach inside a string.
	var literal string
	if err := db.QueryRow(`SELECT 'USER()'`).Scan(&literal); err != nil {
		t.Fatal(err)
	}
	if literal != "USER()" {
		t.Errorf("SELECT 'USER()' returned %q, want it untouched", literal)
	}

	// The table of names is closed, so a function KWDB does not have is still
	// reported as unknown rather than being absorbed by the rewrite.
	_, err := db.Query("SELECT myuser()")
	if err == nil {
		t.Fatal("SELECT myuser() should still be rejected")
	}
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	}
	if myErr.Number != 1305 { // ER_SP_DOES_NOT_EXIST
		t.Errorf("code = %d (%s), want 1305", myErr.Number, myErr.Message)
	}

	// Only a server that checks passwords can show the answer follows the
	// account rather than being whatever the first test connected as.
	sec, secSQL := startSecureMySQLServer(t)
	defer sec.Stopper().Stop(context.Background())
	defer secSQL.Close()
	if _, err := secSQL.Exec("CREATE USER mysqluser WITH PASSWORD 'hunter2'"); err != nil {
		t.Fatal(err)
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = sec.ServingMySQLAddr()
	cfg.User = "mysqluser"
	cfg.Passwd = "hunter2"
	userDB, err := gosql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer userDB.Close()
	userDB.SetMaxOpenConns(1)

	got, _ = queryProbeRow(t, userDB, "SELECT USER()")
	if v := got["USER()"]; !v.Valid || v.String != "mysqluser" {
		t.Errorf("USER() on a session authenticated as mysqluser = %s, want %q",
			nullStr(v), "mysqluser")
	}
}

// queryProbeRow runs a single-row query and returns its values keyed by column
// label, plus the labels in order. Keying by label is what the driver does, so
// a mislabeled column shows up as a missing value rather than a wrong one.
func queryProbeRow(
	t *testing.T, db *gosql.DB, query string,
) (map[string]gosql.NullString, []string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%.60q: %v", query, err)
	}
	return scanProbeRow(t, rows)
}

// scanProbeRow reads the one row rows must carry and closes it.
func scanProbeRow(t *testing.T, rows *gosql.Rows) (map[string]gosql.NullString, []string) {
	t.Helper()
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("no row returned")
	}
	values := make([]gosql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Error("more than one row returned")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]gosql.NullString, len(cols))
	for i, c := range cols {
		out[c] = values[i]
	}
	return out, cols
}

// nullStr renders a nullable value for an error message, so that a NULL and
// the four-letter string "NULL" cannot be confused in test output.
func nullStr(v gosql.NullString) string {
	if !v.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%q", v.String)
}

// TestMySQLSetSessionTransactionIsolation covers the one statement this package
// rewrites that already worked, so both halves have to be pinned: that the
// MySQL spelling now sets the session default, and that every neighboring
// transaction mode still means exactly what it meant before.
//
// SET SESSION TRANSACTION ISOLATION LEVEL is what a connection pool sends from
// Connection.setTransactionIsolation(), outside any transaction. KWDB read it
// as a statement about the current transaction and answered "there is no
// transaction in progress" -- an error about something the client never
// mentioned, on the first statement of the pool's startup sequence.
func TestMySQLSetSessionTransactionIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	// (a) The statement itself, where a pool sends it.
	if _, err := db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		t.Fatalf("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED: %v", err)
	}

	// (b) and (c): the session default really moved, and reads back the same in
	// both spellings. A server that accepted the statement and left the level
	// alone would pass (a) and be worse than the error it replaced.
	var iso string
	if err := db.QueryRow("SHOW transaction_isolation").Scan(&iso); err != nil {
		t.Fatal(err)
	}
	if iso != "read committed" {
		t.Errorf("SHOW transaction_isolation = %q, want %q", iso, "read committed")
	}
	if err := db.QueryRow("SELECT @@session.transaction_isolation").Scan(&iso); err != nil {
		t.Fatal(err)
	}
	if iso != "READ-COMMITTED" {
		t.Errorf("@@session.transaction_isolation = %q, want %q", iso, "READ-COMMITTED")
	}

	// (d) A session default that did not reach the next explicit transaction
	// would make the whole statement decorative: what a pool sets a connection
	// to is meant to govern the work done on it.
	func() {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
		var inTxn string
		if err := tx.QueryRow("SHOW transaction_isolation").Scan(&inTxn); err != nil {
			t.Fatal(err)
		}
		if inTxn != "read committed" {
			t.Errorf("inside the transaction: transaction_isolation = %q, want %q",
				inTxn, "read committed")
		}
	}()

	// (e) READ UNCOMMITTED is a level KWDB does not have. The parser names the
	// word it cannot take, which tells the client what is wrong; mapping it to
	// READ COMMITTED would hand back weaker isolation than was asked for and
	// report success.
	_, err := db.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	if err == nil {
		t.Fatal("READ UNCOMMITTED should still be refused")
	}
	myErr, ok := err.(*mysql.MySQLError)
	if !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	}
	if !strings.Contains(strings.ToLower(myErr.Message), "uncommitted") {
		t.Errorf("message %q does not name the level it refused", myErr.Message)
	}

	// (f) Without SESSION the statement is about the current transaction, and
	// KWDB's reading of it is untouched: a client that wants transaction-scoped
	// isolation still has a way to say so.
	_, err = db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	if err == nil {
		t.Fatal("SET TRANSACTION outside a transaction should still be refused")
	}
	if myErr, ok := err.(*mysql.MySQLError); !ok {
		t.Fatalf("error is %T, not a *mysql.MySQLError: %v", err, err)
	} else if !strings.Contains(myErr.Message, "no transaction in progress") {
		t.Errorf("message %q, want the transaction-scoped rejection", myErr.Message)
	}

	// (g) Guardrail. PRIORITY is a mode SET SESSION CHARACTERISTICS AS
	// TRANSACTION rejects with 0A000, taking the transaction down with it, so a
	// rewrite that read one keyword too far would turn this working statement
	// into an aborted transaction.
	func() {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := tx.Exec("SET SESSION TRANSACTION PRIORITY HIGH"); err != nil {
			t.Fatalf("SET SESSION TRANSACTION PRIORITY HIGH: %v", err)
		}
		var prio string
		if err := tx.QueryRow("SHOW TRANSACTION PRIORITY").Scan(&prio); err != nil {
			t.Fatal(err)
		}
		if prio != "high" {
			t.Errorf("SHOW TRANSACTION PRIORITY = %q, want %q", prio, "high")
		}
	}()

	// (h) Guardrail, and the dangerous one: the target statement never reads
	// AS OF SYSTEM TIME at all. A rewrite reaching this statement would report
	// success and turn a historical read into a current one, with nothing in
	// the result to say so.
	func() {
		before := timeutil.Now()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := tx.Exec("SET SESSION TRANSACTION AS OF SYSTEM TIME '-3600s'"); err != nil {
			t.Fatalf("SET SESSION TRANSACTION AS OF SYSTEM TIME: %v", err)
		}
		var ts time.Time
		if err := tx.QueryRow("SELECT now()").Scan(&ts); err != nil {
			t.Fatal(err)
		}
		if !ts.Before(before.Add(-30 * time.Minute)) {
			t.Errorf("now() inside the historical transaction = %s, want it around an hour before %s",
				ts, before)
		}
	}()
}

// TestMySQLSetSessionTransactionReadOnly covers Connection.setReadOnly(), which
// a pool calls right after setting the isolation level.
//
// It is answered with the session variable rather than with SET SESSION
// CHARACTERISTICS AS TRANSACTION READ ONLY, which would parse: that statement
// carries no ISOLATION clause, and the branch handling it resets the session
// isolation level to SERIALIZABLE. This test runs the pool's order -- level,
// then read-only -- so that substitution would show up here as the level
// silently changing underneath.
func TestMySQLSetSessionTransactionReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db := startMySQLServer(t)
	defer s.Stopper().Stop(context.Background())
	defer db.Close()

	mustExec(t, db, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	mustExec(t, db, "SET SESSION TRANSACTION READ ONLY")

	var ro string
	if err := db.QueryRow("SHOW transaction_read_only").Scan(&ro); err != nil {
		t.Fatal(err)
	}
	if ro != "on" {
		t.Errorf("SHOW transaction_read_only = %q after READ ONLY, want %q", ro, "on")
	}
	var iso string
	if err := db.QueryRow("SHOW transaction_isolation").Scan(&iso); err != nil {
		t.Fatal(err)
	}
	if iso != "read committed" {
		t.Errorf("transaction_isolation = %q after READ ONLY, want it left at %q",
			iso, "read committed")
	}

	mustExec(t, db, "SET SESSION TRANSACTION READ WRITE")
	if err := db.QueryRow("SHOW transaction_read_only").Scan(&ro); err != nil {
		t.Fatal(err)
	}
	if ro != "off" {
		t.Errorf("SHOW transaction_read_only = %q after READ WRITE, want %q", ro, "off")
	}
}
