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
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/stdstrings"
	"github.com/lib/pq/oid"
)

// conn serves one MySQL client connection.
//
// The structure mirrors pgwire.conn deliberately, because the contract with
// the SQL layer is the same: a reader goroutine translates client packets into
// commands on a StmtBuf, a separate goroutine runs the connExecutor draining
// that buffer, and results come back through the sql.ClientComm methods this
// type implements (in command_result.go). Keeping the two connection types
// shaped alike means a change to the connExecutor contract shows up in both
// places rather than silently diverging.
//
// What differs is everything about the wire: MySQL frames packets with a
// length and a sequence number, the server speaks first, and result sets carry
// their own metadata rather than a separate row-description message.
type conn struct {
	conn net.Conn

	sessionArgs sql.SessionArgs
	metrics     *ServerMetrics

	pr *mysqlwirebase.PacketReader
	pw *mysqlwirebase.PacketWriter

	// parser avoids allocating one per statement.
	parser parser.Parser

	// stmtBuf holds commands queued for the connExecutor.
	stmtBuf sql.StmtBuf
	// lastPushed mirrors the position StmtBuf assigned to the most recent
	// command, which StmtBuf does not expose. Only the reader goroutine pushes,
	// through push, so predicting positions from it is exact.
	lastPushed sql.CmdPos

	// batch bounds the statements of the COM_QUERY being executed, as command
	// positions [first, last]. A result inside the batch but before its last
	// statement is followed by another result, which the client can only know
	// from SERVER_MORE_RESULTS_EXISTS. Keyed by position rather than counted
	// down, so that the connExecutor closing a retried statement's result again
	// cannot desynchronise it. Written by the reader goroutine, read by the
	// executor's.
	batch struct {
		first, last int64
	}
	// lastTxnStatus is the transaction state the most recent result closed
	// with. A CALL ends its inner result sets before the statement itself
	// closes, when the connExecutor has not supplied a state yet.
	lastTxnStatus int32

	// res is the single reusable result, as in pgwire: the connExecutor
	// produces results strictly in order.
	res commandResult

	// err records a network error. Once set, the connection is unusable.
	err atomic.Value

	// capabilities is the negotiated capability set. It decides how packets are
	// encoded, so it is fixed once the handshake completes.
	capabilities uint32

	// currentDB tracks the database COM_INIT_DB selected, so that result-set
	// metadata can name it.
	currentDB string

	// connID is the id this connection announced in its greeting. Anything
	// this connection later says about its own identity has to come from here,
	// or the server tells one client two different stories about it.
	connID uint32

	// stmts holds the statements prepared on this connection.
	stmts stmtRegistry

	// intSizer reports the session's default_int_size. The parser fixes the
	// width of SERIAL and other unqualified integers when it parses, so this
	// has to be consulted per statement, as pgwire does; nil until serveConn
	// has set up the session.
	intSizer interface{ GetUnqualifiedIntSize() *types.T }

	// sv carries the cluster settings the parser consults.
	sv *settings.Values

	writerState struct {
		// mu serializes every mutation of the buffered output. The results of a
		// statement are produced on the connExecutor's goroutine while the
		// goroutine that reads commands answers PING, RESET and the like from
		// its own, and a client sends its next command as soon as the previous
		// response reaches the socket -- which can be before the executor has
		// finished resetting this state after a flush. pgwire needs no lock
		// because only its executor goroutine ever writes; this protocol has
		// commands that carry no SQL work, so the invariant is enforced here
		// instead of assumed.
		mu syncutil.Mutex

		fi flushInfo
		// packets holds complete payloads awaiting framing. Buffering payloads
		// rather than framed bytes keeps sequence numbering out of the retry
		// path: a rewind drops whole packets and restores the counter, which
		// would be far harder to get right on a byte buffer.
		packets [][]byte
		// bytes tracks the payload size buffered, for the flush threshold.
		bytes int
	}
}

// flushInfo records what has reached the client, so that results buffered for
// a statement the connExecutor decides to retry can be discarded.
type flushInfo struct {
	conn *conn
	// lastFlushed is the highest command whose results have gone to the network.
	lastFlushed sql.CmdPos
	// cmdStarts maps a command to the index in writerState.packets at which its
	// results begin.
	cmdStarts map[sql.CmdPos]int
	// cmdSeq maps a command to the packet sequence number in effect when its
	// results began. Rewinding the buffer must rewind this too, or the client
	// sees a sequence gap and drops the connection.
	cmdSeq map[sql.CmdPos]uint8
}

// registerCmd notes where a command's results start, the first time one is
// buffered.
func (fi *flushInfo) registerCmd(pos sql.CmdPos) {
	fi.conn.writerState.mu.Lock()
	defer fi.conn.writerState.mu.Unlock()
	if _, ok := fi.cmdStarts[pos]; ok {
		return
	}
	fi.cmdStarts[pos] = len(fi.conn.writerState.packets)
	fi.cmdSeq[pos] = fi.conn.pw.Sequence()
}

func newConn(
	netConn net.Conn,
	sArgs sql.SessionArgs,
	capabilities uint32,
	metrics *ServerMetrics,
	sv *settings.Values,
) *conn {
	// The reader is provisional: ServeConn replaces pr and pw with the pair the
	// connection phase already used, so that bytes the handshake's reader has
	// buffered are not stranded behind a second reader on the same socket.
	rd := bufio.NewReader(netConn)
	c := &conn{
		conn:         netConn,
		sessionArgs:  sArgs,
		metrics:      metrics,
		pr:           mysqlwirebase.NewPacketReader(rd, maxPacketPayload),
		pw:           mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn)),
		capabilities: capabilities,
		sv:           sv,
	}
	c.stmtBuf.Init()
	c.lastPushed = -1 // StmtBuf numbers the first command 0
	atomic.StoreInt64(&c.batch.first, -1)
	atomic.StoreInt64(&c.batch.last, -1)
	atomic.StoreInt32(&c.lastTxnStatus, int32(sql.IdleTxnBlock))
	c.stmts.init()
	c.res.released = true
	c.writerState.fi.conn = c
	c.writerState.fi.lastFlushed = -1
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)
	c.writerState.fi.cmdSeq = make(map[sql.CmdPos]uint8)
	return c
}

// maxPacketPayload bounds a reassembled logical packet. MySQL's own default
// max_allowed_packet is 64 MiB, and a client that needs more than that is
// better off told so than allowed to drive unbounded server allocation.
const maxPacketPayload = 64 << 20

func (c *conn) setErr(err error) { c.err.Store(&err) }

// GetErr returns the network error that terminated this connection, if any.
func (c *conn) GetErr() error {
	err := c.err.Load()
	if err != nil {
		return *err.(*error)
	}
	return nil
}

// baseStatusFlags is where every packet carrying status flags starts, so that
// the rules the server claims about itself are stated in exactly one place.
// Six call sites write these flags and only three of them go through
// statusFlags, so a second literal here would let one connection describe its
// own lexer two different ways depending on which packet a client reads.
//
// ServerStatusNoBackslashEscapes is a statement of fact rather than a
// compatibility concession: KWDB pins standard_conforming_strings on, so a
// backslash inside a string literal is a backslash. A client that is not told
// this escapes an embedded quote as \' and leaves the literal unterminated,
// and doubles a lone backslash into a value nobody sent.
const baseStatusFlags = mysqlwirebase.ServerStatusAutocommit |
	mysqlwirebase.ServerStatusNoBackslashEscapes

// statusFlags reports the session state MySQL clients track between commands.
// The `mysql` CLI uses the in-transaction bit to decide whether Ctrl-C should
// roll back, and drivers use it to detect that an implicit transaction is open.
// moreResults tells the client that another result follows this one in the
// same response; without it the client reads that result as the reply to its
// next command.
func (c *conn) statusFlags(t sql.TransactionStatusIndicator, moreResults bool) uint16 {
	flags := baseStatusFlags
	if t == sql.InTxnBlock || t == sql.InFailedTxnBlock {
		flags |= mysqlwirebase.ServerStatusInTrans
	}
	if moreResults {
		flags |= mysqlwirebase.ServerMoreResultsExists
	}
	return flags
}

// unqualifiedIntSize is the type an unqualified INT parses to in this session.
func (c *conn) unqualifiedIntSize() *types.T {
	if c.intSizer == nil {
		return types.Int
	}
	return c.intSizer.GetUnqualifiedIntSize()
}

// push queues a command for the connExecutor. Every command must go through
// here, or lastPushed stops matching the positions StmtBuf assigns.
func (c *conn) push(ctx context.Context, cmd sql.Command) error {
	if err := c.stmtBuf.Push(ctx, cmd); err != nil {
		return err
	}
	c.lastPushed++
	return nil
}

// setBatch records the positions of the statements about to be pushed for one
// COM_QUERY. It must run before the first of them is pushed: the executor may
// close that statement's result before the reader pushes the rest.
func (c *conn) setBatch(n int) {
	atomic.StoreInt64(&c.batch.last, int64(c.lastPushed)+int64(n))
	atomic.StoreInt64(&c.batch.first, int64(c.lastPushed)+1)
}

// moreResultsAfter reports whether the result at pos is followed by another
// result of the same COM_QUERY.
func (c *conn) moreResultsAfter(pos sql.CmdPos) bool {
	first, last := atomic.LoadInt64(&c.batch.first), atomic.LoadInt64(&c.batch.last)
	return int64(pos) >= first && int64(pos) < last
}

// bufferPacket queues one payload for the client.
func (c *conn) bufferPacket(payload []byte) {
	c.writerState.mu.Lock()
	defer c.writerState.mu.Unlock()
	c.writerState.packets = append(c.writerState.packets, payload)
	c.writerState.bytes += len(payload)
}

// bufferResultSetHeader writes the column count and the column definitions
// that open a result set.
func (c *conn) bufferResultSetHeader(cols sqlbase.ResultColumns) error {
	c.bufferPacket(mysqlwirebase.EncodeColumnCount(len(cols)))
	for i := range cols {
		// The table name is best-effort: KWDB's result columns carry a source
		// table id rather than a name, and expressions have no source at all.
		// Clients display it but do not depend on it.
		def := columnDefinition(cols[i], c.currentDB, "")
		c.bufferPacket(def.Encode())
	}
	// Clients that did not negotiate CLIENT_DEPRECATE_EOF expect an EOF packet
	// between the metadata and the rows.
	if c.capabilities&mysqlwirebase.ClientDeprecateEOF == 0 {
		c.bufferPacket(mysqlwirebase.EncodeEOF(0, baseStatusFlags, c.capabilities))
	}
	return nil
}

// bufferRow writes one result row in the text protocol.
func (c *conn) bufferRow(
	ctx context.Context, row tree.Datums, conv sessiondata.DataConversionConfig,
) error {
	values := make([][]byte, len(row))
	for i := range row {
		v, err := encodeDatum(row[i], conv)
		if err != nil {
			return err
		}
		values[i] = v
	}
	c.bufferPacket(mysqlwirebase.EncodeTextRow(values))
	return nil
}

// bufferBinaryRow writes one result row in the binary protocol, which is what
// a result set produced by a prepared statement carries.
//
// mysqlTypes must be the types reported in the column definitions: the client
// reads each value at the width its declared type implies.
func (c *conn) bufferBinaryRow(
	ctx context.Context, row tree.Datums, mysqlTypes []byte, conv sessiondata.DataConversionConfig,
) error {
	values := make([][]byte, len(row))
	for i := range row {
		if i >= len(mysqlTypes) {
			return errors.AssertionFailedf(
				"mysqlwire: row has %d values but only %d column types", len(row), len(mysqlTypes))
		}
		v, err := encodeDatumBinary(row[i], mysqlTypes[i], conv)
		if err != nil {
			return err
		}
		values[i] = v
	}
	c.bufferPacket(mysqlwirebase.EncodeBinaryRow(values))
	return nil
}

// bufferStmtPrepareOK writes the whole COM_STMT_PREPARE response: the header
// carrying the handle and the two counts, then the parameter definitions, then
// the column definitions, each block terminated by an EOF packet unless the
// client negotiated CLIENT_DEPRECATE_EOF.
func (c *conn) bufferStmtPrepareOK(
	stmtID uint32, paramTypes []oid.Oid, columns sqlbase.ResultColumns,
) {
	c.bufferPacket(mysqlwirebase.EncodeStmtPrepareOK(
		stmtID, uint16(len(columns)), uint16(len(paramTypes)), 0 /* warnings */))

	deprecateEOF := c.capabilities&mysqlwirebase.ClientDeprecateEOF != 0
	if len(paramTypes) > 0 {
		for _, o := range paramTypes {
			c.bufferPacket(paramColumnDefinition(o).Encode())
		}
		if !deprecateEOF {
			c.bufferPacket(mysqlwirebase.EncodeEOF(0, baseStatusFlags, c.capabilities))
		}
	}
	if len(columns) > 0 {
		for i := range columns {
			c.bufferPacket(columnDefinition(columns[i], c.currentDB, "").Encode())
		}
		if !deprecateEOF {
			c.bufferPacket(mysqlwirebase.EncodeEOF(0, baseStatusFlags, c.capabilities))
		}
	}
}

// bufferResultSetTerminator closes a result set, with whichever packet the
// client negotiated.
func (c *conn) bufferResultSetTerminator(t sql.TransactionStatusIndicator, moreResults bool) {
	if c.capabilities&mysqlwirebase.ClientDeprecateEOF != 0 {
		c.bufferPacket(mysqlwirebase.EncodeOK(
			&mysqlwirebase.OK{StatusFlags: c.statusFlags(t, moreResults)},
			c.capabilities, true /* eofSubstitute */))
		return
	}
	c.bufferPacket(mysqlwirebase.EncodeEOF(0, c.statusFlags(t, moreResults), c.capabilities))
}

// bufferOK writes the OK packet that completes a statement returning no rows.
func (c *conn) bufferOK(affectedRows uint64, t sql.TransactionStatusIndicator, moreResults bool) {
	c.bufferPacket(mysqlwirebase.EncodeOK(&mysqlwirebase.OK{
		AffectedRows: affectedRows,
		StatusFlags:  c.statusFlags(t, moreResults),
	}, c.capabilities, false /* eofSubstitute */))
}

// boilerplateHints are the hints the errors library attaches automatically to
// assertion failures (errors.AssertionFailedf, via package assert), to
// unimplemented-feature errors (unimplemented.New* in this codebase, via
// package issuelink) when no real issue URL is set — which is always, for
// unimplemented.New*: see unimplementedInternal in
// pkg/util/errorutil/unimplemented, which deliberately leaves IssueURL empty
// "to avoid potential legal/abuse risks" — and to any other
// errors.WithIssueLink call that leaves IssueURL empty (package issuelink's
// withIssueLink type; no call site in this codebase does that today, but
// nothing stops one from being added later). All three are addressed to a
// KWDB developer ("please file an issue"), not to the client whose query
// just failed, and the error code already conveys "internal error" /
// "unimplemented" on its own, so none of them carries anything a MySQL
// client can act on.
//
// These are filtered out by exact identity, not by length. A length bound
// alone cannot separate them from a genuinely useful hint: the
// assertion-failure hint is 251 bytes — shorter, not longer, than this
// package's own autocommit-rejection hint (vars.go's
// makeCompatBoolVarWithHint), which is 272 bytes. Because 251 < 272, no
// length bound can work at all: any threshold loose enough to admit the
// useful 272-byte hint necessarily also admits the useless 251-byte one,
// which is the Critical finding bufferErr was reviewed for in the first
// place. (The unimplemented-feature hint is the one that runs ~274 bytes; it
// is filtered by the same identity check, not because its length happens to
// be close to the autocommit hint's.) The map keys are built from the same
// exported constants and helper package (stdstrings.IssueReferral) the
// errors library composes these hints from, rather than copied literal
// text, so a wording change upstream keeps this filter in sync instead of
// silently going stale.
//
// A hint carrying a real issue link (errors.WithIssueLink with a non-empty
// IssueURL, used directly by a few call sites such as conn_executor.go's
// schema-change-after-commit error) does not match any entry here: its
// ErrorHint() is just "See: <url>", never the bare referral text, so it is
// short, error-specific, and passes through unfiltered.
// assertionErrorHint repeats the hint text errors.AssertionFailedf attaches.
// The constant itself lives in github.com/cockroachdb/errors/assert, which
// this repository's lint forbids importing directly, and the top-level errors
// package re-exports UnimplementedErrorHint but not this one.
const assertionErrorHint = `You have encountered an unexpected error.`

var boilerplateHints = map[string]struct{}{
	assertionErrorHint + stdstrings.IssueReferral:            {},
	errors.UnimplementedErrorHint + stdstrings.IssueReferral: {},
	// withIssueLink with an empty IssueURL (package issuelink) emits the bare
	// stdstrings.IssueReferral with no prefix sentence — the gap the "no call
	// site does this today" note above leaves open. Closing it here costs
	// nothing and avoids a silent leak if one is added.
	stdstrings.IssueReferral: {},
}

// maxHintBytes bounds the hint text appended to a MySQL ERR packet message,
// once boilerplateHints above has already dropped the two hints that would
// otherwise dominate this budget. 320 comfortably fits this package's own
// autocommit-rejection hint (272 bytes unfolded) with room to grow, while
// keeping a typical short base message plus " HINT: " plus the hint well
// under the conventional 512-byte MYSQL_ERRMSG_SIZE. The bound applies to
// the hint only: KWDB's own error messages are not length-bounded, so a long
// message paired with a near-maximum hint can still exceed 512 bytes overall
// — a pre-existing property of bufferErr, not one this introduces.
const maxHintBytes = 320

// bufferErr writes an ERR packet for a failed statement.
//
// Unlike the Postgres wire protocol, MySQL's ERR packet carries a single
// message string with no separate hint/detail fields, so a hint attached via
// errors.WithHint would otherwise never reach a MySQL client: it lives only in
// err's metadata, and err.Error() does not include it. Appending it here is
// what makes hints (for example on the autocommit rejection) actually
// actionable for MySQL clients rather than pgwire-only. Hints are not always
// hand-written by a caller: the errors library attaches them automatically to
// assertion failures and unimplemented features. Those two are dropped by
// identity via boilerplateHints below before anything is flattened or
// bounded, because they carry nothing a MySQL client can act on (see that
// var's comment for why length alone cannot tell them apart from a
// genuinely useful hint). Everything that remains is flattened to one line
// and bounded by maxHintBytes.
//
// Error details (errors.WithDetail) are deliberately NOT flattened here, only
// hints. Some details, notably compatErrMsg in vars.go ("... has no effect in
// CockroachDB."), name the upstream product and are not meant to reach a
// MySQL client. If a future change adds errors.FlattenDetails alongside this,
// it needs the same review compatErrMsg's callers would have gotten.
func (c *conn) bufferErr(ctx context.Context, err error) {
	// Counted as pgwire counts it, so kwdb_internal.feature_usage sees errors
	// whichever interface the client used; this also reports internal errors.
	sqltelemetry.RecordError(ctx, err, c.sv)

	code, state := mysqlErrorCode(err)
	msg := err.Error()

	// errors.GetAllHints (rather than the pre-joined errors.FlattenHints) is
	// used here so boilerplateHints can be matched against each hint
	// individually, by exact text, instead of string-matching inside an
	// already-flattened blob.
	var kept []string
	for _, h := range errors.GetAllHints(err) {
		if _, isBoilerplate := boilerplateHints[h]; isBoilerplate {
			continue
		}
		kept = append(kept, h)
	}
	if len(kept) > 0 {
		// Join multiple surviving hints the same way errors.FlattenHints
		// joins its full list, so a multi-hint error still reads the same
		// once the boilerplate is gone and the line below collapses this to
		// one line.
		hint := strings.Join(kept, "\n--\n")
		// An ERR packet carries one flat line: collapse any internal newlines
		// (multi-line hints are common even among survivors, e.g.
		// type_check.go's one-signature-per-line "candidates are:" hint) and
		// bound the length so a single hint cannot dominate or blow out the
		// packet. The hint is appended last, so without a bound a long one
		// would push the actionable part of the message out of whatever a
		// client-side buffer keeps.
		hint = strings.Join(strings.Fields(hint), " ")
		if len(hint) > maxHintBytes {
			// Cut on a rune boundary so truncation cannot split a multi-byte
			// UTF-8 sequence and emit invalid bytes into the packet.
			cut := maxHintBytes
			for cut > 0 && !utf8.RuneStart(hint[cut]) {
				cut--
			}
			hint = hint[:cut] + "..."
		}
		msg += " HINT: " + hint
	}
	c.bufferPacket(mysqlwirebase.EncodeErr(code, state, msg, c.capabilities))
}

// mysqlErrorCode translates a KWDB error into the code and SQLSTATE a MySQL
// client expects.
//
// KWDB errors carry PostgreSQL codes. The SQLSTATE is largely shared between
// the two systems, so it passes through; the numeric code has no equivalent
// and is derived from the SQLSTATE class. Applications branch on both, so
// getting the common classes right matters more than exhaustive coverage.
func mysqlErrorCode(err error) (uint16, string) {
	state := pgerror.GetPGCode(err)
	if len(state) != 5 {
		return mysqlwirebase.ErrUnknownError, mysqlwirebase.SQLStateGeneralError
	}

	// An exact match is tried first. Several of these live in classes whose
	// fallback below maps to something else entirely: a unique violation and a
	// NOT NULL violation share class 23, and a missing table and a privilege
	// failure share class 42, but no client treats them alike.
	if code, ok := pgCodeToMySQL[state]; ok {
		return code, state
	}

	switch state[:2] {
	case "42": // syntax error or access rule violation
		return mysqlwirebase.ErrParse, state
	case "28": // invalid authorization specification
		return mysqlwirebase.ErrAccessDenied, state
	case "3D": // invalid catalog name
		return mysqlwirebase.ErrBadDB, state
	case "0A": // feature not supported
		return mysqlwirebase.ErrNotSupportedYet, state
	case "08": // connection exception
		return mysqlwirebase.ErrAborting, state
	case "40": // transaction rollback
		// Everything in this class is a conflict the client should retry, which
		// is exactly what MySQL's deadlock code asks of it.
		return mysqlwirebase.ErrLockDeadlock, state
	case "53": // insufficient resources
		return mysqlwirebase.ErrOutOfResources, state
	default:
		return mysqlwirebase.ErrUnknownError, state
	}
}

// pgCodeToMySQL maps the PostgreSQL error codes KWDB reports onto the MySQL
// codes clients act on.
//
// Only codes with a genuine MySQL counterpart are listed. Mapping a KWDB error
// onto an unrelated MySQL code would be worse than the generic code: a client
// that retried on a mis-mapped deadlock would loop on an error that will never
// clear.
var pgCodeToMySQL = map[string]uint16{
	pgcode.UniqueViolation:            mysqlwirebase.ErrDupEntry,
	pgcode.NotNullViolation:           mysqlwirebase.ErrBadNull,
	pgcode.ForeignKeyViolation:        mysqlwirebase.ErrNoReferencedRow,
	pgcode.DependentObjectsStillExist: mysqlwirebase.ErrRowIsReferenced,

	pgcode.UndefinedTable:    mysqlwirebase.ErrNoSuchTable,
	pgcode.UndefinedColumn:   mysqlwirebase.ErrBadField,
	pgcode.UndefinedFunction: mysqlwirebase.ErrSPDoesNotExist,
	pgcode.UndefinedDatabase: mysqlwirebase.ErrBadDB,
	pgcode.DuplicateColumn:   mysqlwirebase.ErrDupFieldName,
	pgcode.DuplicateDatabase: mysqlwirebase.ErrDBCreateExists,
	pgcode.DuplicateRelation: mysqlwirebase.ErrTableExists,

	pgcode.StringDataRightTruncation: mysqlwirebase.ErrDataTooLong,
	pgcode.NumericValueOutOfRange:    mysqlwirebase.ErrWarnDataOutOfRange,
	pgcode.InvalidTextRepresentation: mysqlwirebase.ErrTruncatedWrongValue,
	pgcode.DivisionByZero:            mysqlwirebase.ErrDivisionByZero,

	pgcode.SerializationFailure: mysqlwirebase.ErrLockDeadlock,
	pgcode.DeadlockDetected:     mysqlwirebase.ErrLockDeadlock,
	pgcode.LockNotAvailable:     mysqlwirebase.ErrLockWaitTimeout,
	pgcode.QueryCanceled:        mysqlwirebase.ErrQueryInterrupted,

	pgcode.InsufficientPrivilege: mysqlwirebase.ErrSpecificAccessDenied,
	pgcode.TooManyConnections:    mysqlwirebase.ErrConCountError,
	pgcode.DiskFull:              mysqlwirebase.ErrOutOfResources,
	pgcode.OutOfMemory:           mysqlwirebase.ErrOutOfResources,

	pgcode.InvalidSQLStatementName: mysqlwirebase.ErrUnknownStmtName,
}

// Flush is part of the sql.ClientComm interface. It frames every buffered
// payload and writes it to the network.
func (c *conn) Flush(pos sql.CmdPos) error {
	c.writerState.mu.Lock()
	defer c.writerState.mu.Unlock()
	return c.flushLocked(pos)
}

// flushLocked is Flush with writerState.mu already held.
func (c *conn) flushLocked(pos sql.CmdPos) error {
	if err := c.GetErr(); err != nil {
		return err
	}

	for _, p := range c.writerState.packets {
		if err := c.pw.WritePacket(p); err != nil {
			c.setErr(err)
			return err
		}
	}
	if err := c.pw.Flush(); err != nil {
		c.setErr(err)
		return err
	}

	c.writerState.packets = c.writerState.packets[:0]
	c.writerState.bytes = 0
	c.writerState.fi.lastFlushed = pos
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)
	c.writerState.fi.cmdSeq = make(map[sql.CmdPos]uint8)
	return nil
}

// maybeFlush flushes once the buffered payload exceeds the session's result
// buffer size.
func (c *conn) maybeFlush(pos sql.CmdPos) (bool, error) {
	c.writerState.mu.Lock()
	over := int64(c.writerState.bytes) > c.sessionArgs.ConnResultsBufferSize
	c.writerState.mu.Unlock()
	if !over {
		return false, nil
	}
	return true, c.Flush(pos)
}

// truncateBufferedResults discards packets buffered past idx, restoring the
// sequence number so that what does reach the client stays contiguous.
func (c *conn) truncateBufferedResults(idx int) bool {
	c.writerState.mu.Lock()
	defer c.writerState.mu.Unlock()
	return c.truncateBufferedResultsLocked(idx)
}

// truncateBufferedResultsLocked is truncateBufferedResults with the lock held.
func (c *conn) truncateBufferedResultsLocked(idx int) bool {
	if idx > len(c.writerState.packets) {
		return false
	}
	for _, p := range c.writerState.packets[idx:] {
		c.writerState.bytes -= len(p)
	}
	c.writerState.packets = c.writerState.packets[:idx]
	return true
}

// LockCommunication is part of the sql.ClientComm interface.
//
// As in pgwire, results are written synchronously as they are produced, so
// there is nothing to lock: while the connExecutor holds the lock it is not
// producing results.
func (c *conn) LockCommunication() sql.ClientLock {
	return &clientConnLock{flushInfo: &c.writerState.fi}
}

// clientConnLock lets the SQL layer discover what has already reached the
// client and discard what has not.
type clientConnLock struct {
	*flushInfo
}

var _ sql.ClientLock = &clientConnLock{}

// Close is part of the sql.ClientLock interface.
func (cl *clientConnLock) Close() {}

// ClientPos is part of the sql.ClientLock interface.
func (cl *clientConnLock) ClientPos() sql.CmdPos { return cl.lastFlushed }

// RTrim is part of the sql.ClientLock interface.
func (cl *clientConnLock) RTrim(ctx context.Context, pos sql.CmdPos) {
	if pos <= cl.lastFlushed {
		panic(fmt.Sprintf("mysqlwire: asked to trim to pos %d, at or below the last flush %d",
			pos, cl.lastFlushed))
	}
	c := cl.conn
	c.writerState.mu.Lock()
	defer c.writerState.mu.Unlock()
	idx, ok := cl.cmdStarts[pos]
	if !ok {
		// No results for pos have been buffered yet.
		idx = len(c.writerState.packets)
	}
	if seq, ok := cl.cmdSeq[pos]; ok {
		c.pw.SetSequence(seq)
	}
	c.truncateBufferedResultsLocked(idx)

	for p := range cl.cmdStarts {
		if p >= pos {
			delete(cl.cmdStarts, p)
			delete(cl.cmdSeq, p)
		}
	}
}

// serveCommands is the reader loop: it turns client packets into commands on
// the StmtBuf until the client disconnects or the connection fails.
//
// Sequence numbering restarts at zero for every command, which is why the loop
// owns the reset rather than the individual handlers.
func (c *conn) serveCommands(ctx context.Context, draining func() bool) error {
	for {
		// Only the reader's counter is reset here. The writer's is set below,
		// once the command has actually been read: results are produced on the
		// connExecutor's goroutine, so a reset at this point would race with
		// the previous command's response still being written and renumber it
		// mid-flight.
		c.pr.ResetSequence()

		payload, err := c.pr.ReadPacket()
		if err != nil {
			// A closed connection is the normal way a client goes away, not an
			// error worth surfacing.
			return nil
		}
		// The reply continues the client's numbering rather than restarting:
		// within one command a single counter is advanced by whichever side is
		// sending. Replying with the sequence the client used makes strict
		// clients report "commands out of sync" and drop the connection, while
		// lenient ones carry on, so this is easy to get wrong and hard to
		// notice.
		c.pw.SetSequence(c.pr.Sequence())

		if len(payload) == 0 {
			continue
		}
		timeReceived := timeutil.Now()

		if draining() {
			c.bufferPacket(mysqlwirebase.EncodeErr(
				mysqlwirebase.ErrServerShutdown, mysqlwirebase.SQLStateConnectionFailure,
				"Server shutdown in progress", c.capabilities))
			_ = c.Flush(0)
			return nil
		}

		cmd := mysqlwirebase.Command(payload[0])
		body := payload[1:]

		switch cmd {
		case mysqlwirebase.ComQuit:
			// The client is closing the connection; no reply is expected.
			return nil

		case mysqlwirebase.ComPing:
			c.bufferOK(0, sql.IdleTxnBlock, false /* moreResults */)
			if err := c.Flush(0); err != nil {
				return err
			}

		case mysqlwirebase.ComInitDB:
			if err := c.handleInitDB(ctx, string(body), timeReceived); err != nil {
				return err
			}

		case mysqlwirebase.ComQuery:
			if err := c.handleQuery(ctx, string(body), timeReceived); err != nil {
				return err
			}

		case mysqlwirebase.ComResetConnection:
			// KWDB has no cheaper reset than the session already provides
			// between statements, so acknowledging is honest: the client's
			// expectation is that temporary state is gone, and the session
			// carries none across this point.
			c.bufferOK(0, sql.IdleTxnBlock, false /* moreResults */)
			if err := c.Flush(0); err != nil {
				return err
			}

		case mysqlwirebase.ComFieldList:
			// Deprecated in MySQL 5.7 and removed in 8.0; clients fall back to
			// querying information_schema when the server refuses it.
			c.bufferPacket(mysqlwirebase.EncodeErr(
				mysqlwirebase.ErrNotSupportedYet, mysqlwirebase.SQLStateFeatureDisabled,
				"COM_FIELD_LIST is not supported; query information_schema instead",
				c.capabilities))
			if err := c.Flush(0); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtPrepare:
			if err := c.handleStmtPrepare(ctx, string(body)); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtExecute:
			if err := c.handleStmtExecute(ctx, body, timeReceived); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtSendLongData:
			// No reply, by protocol.
			if err := c.handleStmtSendLongData(ctx, body); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtClose:
			// No reply, by protocol.
			if err := c.handleStmtClose(ctx, body); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtReset:
			if err := c.handleStmtReset(ctx, body); err != nil {
				return err
			}

		case mysqlwirebase.ComStmtFetch:
			// Fetching implies a cursor, which handleStmtExecute refuses to open;
			// a client reaching here has ignored that refusal.
			c.bufferPacket(mysqlwirebase.EncodeErr(
				mysqlwirebase.ErrNotSupportedYet, mysqlwirebase.SQLStateFeatureDisabled,
				"COM_STMT_FETCH is not supported; cursors are not implemented",
				c.capabilities))
			if err := c.Flush(0); err != nil {
				return err
			}

		default:
			log.VEventf(ctx, 2, "mysqlwire: unsupported command %s (0x%02x)", cmd, byte(cmd))
			c.bufferPacket(mysqlwirebase.EncodeErr(
				mysqlwirebase.ErrUnknownCommand, mysqlwirebase.SQLStateGeneralError,
				fmt.Sprintf("Unknown command %s", cmd), c.capabilities))
			if err := c.Flush(0); err != nil {
				return err
			}
		}
	}
}

// dialect assembles the per-connection facts a rewrite may have to answer with.
// Handing the rewrites a value rather than the connection itself is what keeps
// each of them a pure function a table test can drive without a socket.
func (c *conn) dialect() dialectContext {
	return dialectContext{connID: c.connID}
}

// sendErrorThroughExecutor hands err to the connExecutor rather than writing it
// from the goroutine that reads commands.
//
// writerState has no lock: every packet is meant to be buffered and flushed by
// the executor's goroutine, which is what makes that safe, and pgwire relies on
// the same invariant. Writing an error here instead would break it. The client
// sends its next command as soon as the previous response reaches the socket,
// which can be before the executor goroutine has finished resetting
// writerState after its flush, so the two would race over the packet slice --
// dropping the error packet (the client then reads the previous response and
// sees no error at all) or replaying the previous terminator (the client then
// reads the previous statement's error for this one). Both were observed as
// rare failures under a loaded full-suite run.

// handleQuery parses a COM_QUERY payload and queues its statements.
//
// MySQL's COM_QUERY is the direct analog of the PostgreSQL simple query
// protocol: one packet may carry several statements, and each produces its own
// result set.
func (c *conn) handleQuery(ctx context.Context, query string, timeReceived time.Time) error {
	// MySQL spellings KWDB's grammar, inherited from PostgreSQL, does not
	// accept -- SET NAMES with a COLLATE clause, `@@variable`, USER() -- are
	// translated here rather than in the grammar, so that the MySQL reading
	// stays on the MySQL port. Clients send several of them on every connect.
	// See rewriteMySQLDialect.
	query = rewriteMySQLDialect(query, c.dialect())

	startParse := timeutil.Now()
	stmts, err := c.parser.ParseWithInt(query, c.unqualifiedIntSize(), c.sv)
	endParse := timeutil.Now()
	if err != nil {
		return c.sendErrorThroughExecutor(ctx, err)
	}

	if len(stmts) > 1 && c.capabilities&mysqlwirebase.ClientMultiStatements == 0 {
		// The client did not ask for multi-statement support, so accepting a
		// batch would send it result sets it has no way to iterate. The code
		// is spelled as a pgcode because bufferErr maps it to MySQL's 1064 the
		// same way it maps every other error the SQL layer raises.
		return c.sendErrorThroughExecutor(ctx, pgerror.New(pgcode.Syntax,
			"multiple statements require CLIENT_MULTI_STATEMENTS"))
	}

	c.setBatch(len(stmts))
	for _, stmt := range stmts {
		if err := c.push(ctx, sql.ExecStmt{
			Statement:    stmt,
			TimeReceived: timeReceived,
			ParseStart:   startParse,
			ParseEnd:     endParse,
		}); err != nil {
			return err
		}
	}
	// Sync makes the connExecutor run what was just queued and flush the
	// results; without it the statements would sit in the buffer.
	return c.push(ctx, sql.Sync{})
}

func (c *conn) sendErrorThroughExecutor(ctx context.Context, err error) error {
	if pushErr := c.push(ctx, sql.SendError{Err: err}); pushErr != nil {
		return pushErr
	}
	return c.push(ctx, sql.Sync{})
}

// handleInitDB implements COM_INIT_DB, which MySQL clients send for `USE db`
// and for the database named in the handshake.
//
// It is expressed as a SET statement so that the change goes through the same
// validation and session bookkeeping as any other database switch, rather than
// updating session state behind the connExecutor's back.
func (c *conn) handleInitDB(ctx context.Context, db string, timeReceived time.Time) error {
	c.currentDB = db
	stmt := fmt.Sprintf("SET database = %s", tree.NameString(db))
	return c.handleQuery(ctx, stmt, timeReceived)
}
