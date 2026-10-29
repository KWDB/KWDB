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

package mysqlwire

import (
	"bufio"
	"context"
	"io/ioutil"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/lib/pq/oid"
)

// TestMySQLErrorCode covers the translation applications actually branch on.
// An ORM decides whether to retry a transaction, rewrite an insert as an
// update, or surface the failure by reading the numeric code, so collapsing
// distinct failures onto one code is a behavior change, not a cosmetic one.
func TestMySQLErrorCode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name      string
		pgCode    string
		wantCode  uint16
		wantState string
	}{
		// These three share SQLSTATE class 23 but mean very different things.
		{"unique violation", pgcode.UniqueViolation, mysqlwirebase.ErrDupEntry, "23505"},
		{"not null violation", pgcode.NotNullViolation, mysqlwirebase.ErrBadNull, "23502"},
		{"foreign key violation", pgcode.ForeignKeyViolation, mysqlwirebase.ErrNoReferencedRow, "23503"},

		// Class 42 mixes schema mistakes with privilege failures.
		{"undefined table", pgcode.UndefinedTable, mysqlwirebase.ErrNoSuchTable, "42P01"},
		{"undefined column", pgcode.UndefinedColumn, mysqlwirebase.ErrBadField, "42703"},
		{"insufficient privilege", pgcode.InsufficientPrivilege, mysqlwirebase.ErrSpecificAccessDenied, "42501"},
		{"syntax error falls back to class 42", pgcode.Syntax, mysqlwirebase.ErrParse, "42601"},

		// Retryable conflicts must reach the client as a code it retries on.
		{"serialization failure", pgcode.SerializationFailure, mysqlwirebase.ErrLockDeadlock, "40001"},
		{"deadlock", pgcode.DeadlockDetected, mysqlwirebase.ErrLockDeadlock, "40P01"},

		{"division by zero", pgcode.DivisionByZero, mysqlwirebase.ErrDivisionByZero, "22012"},
		{"undefined database", pgcode.UndefinedDatabase, mysqlwirebase.ErrBadDB, "3D000"},
		{"too many connections", pgcode.TooManyConnections, mysqlwirebase.ErrConCountError, "53300"},
		{"disk full maps to out of resources", pgcode.DiskFull, mysqlwirebase.ErrOutOfResources, "53100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pgerror.New(tc.pgCode, "boom")
			code, state := mysqlErrorCode(err)
			if code != tc.wantCode {
				t.Errorf("code = %d, want %d", code, tc.wantCode)
			}
			if state != tc.wantState {
				t.Errorf("SQLSTATE = %s, want %s", state, tc.wantState)
			}
		})
	}
}

// TestMySQLErrorCodeFallbacks checks the two ends of the mapping: an error
// carrying no usable PostgreSQL code, and one whose class has no specific
// MySQL counterpart. Both must still produce a well-formed ERR packet rather
// than an empty SQLSTATE, which clients parse positionally.
func TestMySQLErrorCodeFallbacks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	code, state := mysqlErrorCode(errNoPGCode{})
	if code != mysqlwirebase.ErrUnknownError {
		t.Errorf("code = %d, want %d", code, mysqlwirebase.ErrUnknownError)
	}
	if len(state) != 5 {
		t.Errorf("SQLSTATE = %q, want 5 characters", state)
	}

	// A class with no entry in the table keeps its SQLSTATE and reports the
	// generic code.
	code, state = mysqlErrorCode(pgerror.New("22P05", "unsupported"))
	if code != mysqlwirebase.ErrUnknownError {
		t.Errorf("code = %d, want the generic code %d", code, mysqlwirebase.ErrUnknownError)
	}
	if state != "22P05" {
		t.Errorf("SQLSTATE = %q, want it preserved as 22P05", state)
	}
}

type errNoPGCode struct{}

func (errNoPGCode) Error() string { return "no code" }

// testClient is the client half of an in-memory connection. It keeps the
// packet counter coherent across both directions the way a real client does:
// within one command a single sequence is advanced by whichever side is
// sending, so a reply continues the numbering the client started rather than
// restarting it.
type testClient struct {
	conn net.Conn
	pw   *mysqlwirebase.PacketWriter
	pr   *mysqlwirebase.PacketReader
}

func newTestClient(c net.Conn) *testClient {
	return &testClient{
		conn: c,
		pw:   mysqlwirebase.NewPacketWriter(bufio.NewWriter(c)),
		pr:   mysqlwirebase.NewPacketReader(bufio.NewReader(c), 1<<20),
	}
}

// send writes one command packet, starting a fresh sequence as a client does.
func (tc *testClient) send(t *testing.T, cmd mysqlwirebase.Command, body []byte) {
	t.Helper()
	tc.pw.ResetSequence()
	payload := append([]byte{byte(cmd)}, body...)
	if err := tc.pw.WritePacket(payload); err != nil {
		t.Fatal(err)
	}
	if err := tc.pw.Flush(); err != nil {
		t.Fatal(err)
	}
}

// recv reads one reply packet. The pipe is unbuffered, so the server's write
// is already blocked waiting for this read by the time it is called.
func (tc *testClient) recv(t *testing.T) []byte {
	t.Helper()
	tc.pr.SetSequence(tc.pw.Sequence())
	payload, err := tc.pr.ReadPacket()
	if err != nil {
		t.Fatalf("reading reply: %v", err)
	}
	return payload
}

// newTestConn builds a conn over one end of an in-memory pipe. Nothing behind
// it consumes the StmtBuf, so the tests below drive serveCommands and then
// inspect what it queued.
func newTestConn(t *testing.T, capabilities uint32) (*conn, *testClient) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	st := cluster.MakeTestingClusterSettings()
	metrics := makeServerMetrics(time.Second)
	c := newConn(serverSide, sql.SessionArgs{}, capabilities, &metrics, &st.SV)
	return c, newTestClient(clientSide)
}

// drainCommands runs the reader loop while exchange drives the client side,
// then returns everything the loop queued for the connExecutor.
func drainCommands(t *testing.T, c *conn, client *testClient, exchange func()) []sql.Command {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.serveCommands(context.Background(), func() bool { return false })
	}()

	exchange()
	// COM_QUIT ends the loop cleanly, which is how a client normally leaves.
	client.send(t, mysqlwirebase.ComQuit, nil)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("serveCommands did not return")
	}

	// Drain exactly what is buffered. CurCmd blocks when the buffer runs dry,
	// and Close would discard what is already in it, so the count has to come
	// from Len rather than from reading until an error.
	var cmds []sql.Command
	for i, n := 0, c.stmtBuf.Len(); i < n; i++ {
		cmd, _, err := c.stmtBuf.CurCmd()
		if err != nil {
			t.Fatalf("reading queued command %d of %d: %v", i, n, err)
		}
		cmds = append(cmds, cmd)
		c.stmtBuf.AdvanceOne()
	}
	return cmds
}

// TestConnQueryCommand checks that COM_QUERY reaches the SQL layer as an
// ExecStmt followed by a Sync. Without the Sync the connExecutor never runs
// what was queued and the client waits forever for a reply that was computed
// but never flushed.
func TestConnQueryCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	cmds := drainCommands(t, c, client, func() {
		client.send(t, mysqlwirebase.ComQuery, []byte("SELECT 1"))
	})

	if len(cmds) != 2 {
		t.Fatalf("queued %d command(s), want 2: %v", len(cmds), cmds)
	}
	execStmt, ok := cmds[0].(sql.ExecStmt)
	if !ok {
		t.Fatalf("first command is %T, want sql.ExecStmt", cmds[0])
	}
	if execStmt.AST == nil {
		t.Error("ExecStmt carries no AST")
	}
	if _, ok := cmds[1].(sql.Sync); !ok {
		t.Errorf("second command is %T, want sql.Sync", cmds[1])
	}
}

// TestConnStmtPrepareCommands checks the command triple a COM_STMT_PREPARE
// expands into. The Describe is what eventually writes the response, so its
// absence would leave the client waiting.
func TestConnStmtPrepareCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	cmds := drainCommands(t, c, client, func() {
		client.send(t, mysqlwirebase.ComStmtPrepare, []byte("SELECT $1::INT"))
	})

	if len(cmds) != 3 {
		t.Fatalf("queued %d command(s), want 3: %v", len(cmds), cmds)
	}
	prep, ok := cmds[0].(sql.PrepareStmt)
	if !ok {
		t.Fatalf("first command is %T, want sql.PrepareStmt", cmds[0])
	}
	desc, ok := cmds[1].(sql.DescribeStmt)
	if !ok {
		t.Fatalf("second command is %T, want sql.DescribeStmt", cmds[1])
	}
	if prep.Name == "" {
		t.Error("the prepared statement was queued without a name")
	}
	// The Describe has to name the statement the Prepare just created, or it
	// describes something else entirely.
	if desc.Name != prep.Name {
		t.Errorf("describe names %q, prepare named %q", desc.Name, prep.Name)
	}
	if _, ok := cmds[2].(sql.Sync); !ok {
		t.Errorf("third command is %T, want sql.Sync", cmds[2])
	}

	// The statement is reserved but not yet reachable: its metadata arrives
	// with the Describe.
	if reg := c.stmts.peekPending(); reg == nil {
		t.Error("no statement was left pending for the describe")
	}
}

// TestConnStmtPrepareRejectsMultipleStatements checks that a multi-statement
// prepare is refused. The response has room for exactly one statement's
// metadata, so accepting the batch would leave the client unable to tell which
// statement it holds a handle for.
func TestConnStmtPrepareRejectsMultipleStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	var reply []byte
	cmds := drainCommands(t, c, client, func() {
		client.send(t, mysqlwirebase.ComStmtPrepare, []byte("SELECT 1; SELECT 2"))
		reply = client.recv(t)
	})

	if len(cmds) != 0 {
		t.Errorf("a rejected prepare queued %d command(s): %v", len(cmds), cmds)
	}
	assertErrPacket(t, reply, mysqlwirebase.ErrParse)
}

// TestConnUnknownCommand checks that an unimplemented command is answered with
// an ERR packet rather than silently ignored: a client that receives nothing
// blocks until its read timeout.
func TestConnUnknownCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	var reply []byte
	cmds := drainCommands(t, c, client, func() {
		client.send(t, mysqlwirebase.Command(0x7f), nil)
		reply = client.recv(t)
	})

	if len(cmds) != 0 {
		t.Errorf("an unknown command queued %d command(s): %v", len(cmds), cmds)
	}
	assertErrPacket(t, reply, mysqlwirebase.ErrUnknownCommand)
}

// TestConnStmtExecuteUnknownHandle checks the response to a handle the server
// never issued.
func TestConnStmtExecuteUnknownHandle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	var reply []byte
	cmds := drainCommands(t, c, client, func() {
		// Statement id 999, flags 0, iteration count 1.
		client.send(t, mysqlwirebase.ComStmtExecute,
			[]byte{0xe7, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00})
		reply = client.recv(t)
	})

	if len(cmds) != 0 {
		t.Errorf("an unknown handle queued %d command(s): %v", len(cmds), cmds)
	}
	assertErrPacket(t, reply, mysqlwirebase.ErrUnknownStmtHandler)
}

// TestConnPingSequenceNumber checks the packet sequence number of a reply.
//
// Within one command a single counter is advanced by whichever side is
// sending, so the server's reply continues the client's numbering rather than
// restarting it. Getting this wrong makes strict clients report "commands out
// of sync" and drop the connection, while lenient ones carry on — which is
// exactly the kind of bug that survives manual testing.
func TestConnPingSequenceNumber(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()

	var reply []byte
	drainCommands(t, c, client, func() {
		client.send(t, mysqlwirebase.ComPing, nil)
		reply = client.recv(t)
	})

	if len(reply) == 0 || reply[0] != mysqlwirebase.OKPacketHeader {
		t.Fatalf("reply header = % x, want an OK packet", reply)
	}
	// The client sent sequence 0, so the reply must carry sequence 1. recv
	// primes the reader with the writer's counter and ReadPacket rejects a
	// mismatch, so a wrong sequence already failed the test above; what is
	// checked here is that the counter advanced past the reply, which pins
	// down which packet was accepted rather than merely that one arrived.
	if got := client.pr.Sequence(); got != 2 {
		t.Errorf("reader sequence after the reply = %d, want 2 (reply carried 1)", got)
	}
}

// assertErrPacket checks that the payload is an ERR packet carrying the given
// code, and that its SQLSTATE is the fixed width clients parse positionally.
func assertErrPacket(t *testing.T, payload []byte, wantCode uint16) {
	t.Helper()
	if payload == nil {
		t.Fatal("no packet was written to the client")
	}
	if payload[0] != mysqlwirebase.ErrPacketHeader {
		t.Fatalf("packet header = 0x%02x, want 0x%02x (ERR)",
			payload[0], mysqlwirebase.ErrPacketHeader)
	}
	r := mysqlwirebase.NewReader(payload[1:])
	code, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if code != wantCode {
		t.Errorf("error code = %d, want %d", code, wantCode)
	}
	marker, err := r.Byte()
	if err != nil {
		t.Fatal(err)
	}
	if marker != '#' {
		t.Fatalf("SQLSTATE marker = %q, want #", marker)
	}
	state, err := r.Bytes(5)
	if err != nil {
		t.Fatalf("SQLSTATE is not 5 characters: %v", err)
	}
	if len(state) != 5 {
		t.Errorf("SQLSTATE = %q, want 5 characters", state)
	}
}

// TestStatusFlagsDeclareBackslashHandling pins the lexical rule the server
// reports. KWDB keeps standard_conforming_strings on (SET ... = off is
// rejected), so a backslash inside a string literal is a backslash. A client
// that is not told this escapes with backslashes: an embedded quote becomes \'
// and leaves the literal unterminated, and a lone backslash is doubled and
// stored as two.
func TestStatusFlagsDeclareBackslashHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()
	defer c.conn.Close()

	// The transaction state must not change the answer: the escaping rule is a
	// property of the lexer, not of the session.
	for _, tc := range []struct {
		name string
		txn  sql.TransactionStatusIndicator
	}{
		{"outside a transaction", sql.IdleTxnBlock},
		{"inside a transaction", sql.InTxnBlock},
		{"inside a failed transaction", sql.InFailedTxnBlock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.statusFlags(tc.txn, false /* moreResults */)
			if got&mysqlwirebase.ServerStatusNoBackslashEscapes == 0 {
				t.Errorf("statusFlags = %#04x, want ServerStatusNoBackslashEscapes (%#04x) set",
					got, mysqlwirebase.ServerStatusNoBackslashEscapes)
			}
			if got&mysqlwirebase.ServerStatusAutocommit == 0 {
				t.Errorf("statusFlags = %#04x, want ServerStatusAutocommit (%#04x) still set",
					got, mysqlwirebase.ServerStatusAutocommit)
			}
		})
	}
}

// TestIntermediateEOFPacketsDeclareBackslashHandling covers the status-flag
// writes that do not go through statusFlags: the EOF packets closing the
// column-definition block of a result set and the two definition blocks of a
// COM_STMT_PREPARE response. Only a client that did not negotiate
// CLIENT_DEPRECATE_EOF is sent them, so a modern driver can see the right rule
// while the `mysql` CLI is told the opposite one by the same connection.
func TestIntermediateEOFPacketsDeclareBackslashHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()
	defer c.conn.Close()

	cols := sqlbase.ResultColumns{{Name: "x", Typ: types.Int}}
	if err := c.bufferResultSetHeader(cols); err != nil {
		t.Fatal(err)
	}
	c.bufferStmtPrepareOK(1 /* stmtID */, []oid.Oid{oid.T_int8}, cols)

	var eofs int
	for i, p := range c.writerState.packets {
		if len(p) == 0 || p[0] != mysqlwirebase.EOFPacketHeader {
			continue
		}
		eofs++
		// An EOF payload under CLIENT_PROTOCOL_41 is header, warnings, status.
		if len(p) < 5 {
			t.Fatalf("packet %d is a truncated EOF: % x", i, p)
		}
		status := uint16(p[3]) | uint16(p[4])<<8
		if status&mysqlwirebase.ServerStatusNoBackslashEscapes == 0 {
			t.Errorf("packet %d: EOF status = %#04x, want ServerStatusNoBackslashEscapes (%#04x) set",
				i, status, mysqlwirebase.ServerStatusNoBackslashEscapes)
		}
	}
	if eofs != 3 {
		t.Errorf("buffered %d EOF packet(s), want 3: the set of writes this test covers has changed", eofs)
	}
}

// TestStatusFlagWritesGoThroughBaseStatusFlags is a regression guard rather
// than a behavior test. The flags are written from six places and three of
// them bypass statusFlags, so the failure mode is not "the bit is wrong" but
// "the next write forgets it", after which one connection declares two
// different lexers depending on which packet the client happens to read.
// Scanning the source keeps baseStatusFlags the single source of truth without
// relying on the next author knowing all six sites.
func TestStatusFlagWritesGoThroughBaseStatusFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// go test runs with the package directory as the working directory.
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("found no source files to scan")
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := ioutil.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(trimmed, "ServerStatusAutocommit") {
				continue
			}
			// The declaration of baseStatusFlags is the one place allowed to
			// name it.
			if strings.Contains(trimmed, "baseStatusFlags") {
				continue
			}
			t.Errorf("%s:%d names ServerStatusAutocommit outside baseStatusFlags: %s\n\t"+
				"every status-flag write must start from baseStatusFlags, or this packet "+
				"claims a different escaping rule than the rest of the connection",
				file, i+1, trimmed)
		}
	}
}

// TestBufferErrRecordsTelemetry checks that an error sent to a MySQL client is
// counted the way pgwire counts it. kwdb_internal.feature_usage reads these
// counters, so an error that reached a client over MySQL but was never
// recorded simply does not exist there.
func TestBufferErrRecordsTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c, client := newTestConn(t, mysqlwirebase.ClientProtocol41)
	defer client.conn.Close()
	defer c.conn.Close()

	const counter = "errorcodes." + pgcode.Syntax
	before := telemetry.GetRawFeatureCounts()[counter]
	c.bufferErr(context.Background(), pgerror.New(pgcode.Syntax, "syntax error"))
	if after := telemetry.GetRawFeatureCounts()[counter]; after != before+1 {
		t.Fatalf("%s went from %d to %d, want one more", counter, before, after)
	}
}
