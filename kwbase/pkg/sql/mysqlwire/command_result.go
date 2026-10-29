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
	"context"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// completionMsgType records what a result must emit when it closes.
//
// The connExecutor drives results through a uniform interface and does not
// know which protocol is underneath; this is where a MySQL connection decides
// that "a statement finished" means an OK packet, an EOF packet, or nothing at
// all.
type completionMsgType int

const (
	// commandComplete is a statement that ran to completion. It closes with
	// either a result-set terminator or an OK packet, depending on whether the
	// statement returned rows.
	commandComplete completionMsgType = iota
	// flushOnClose is a result that emits no packet of its own but does put
	// whatever is buffered on the wire.
	//
	// This is what the connExecutor's Sync carries. PostgreSQL answers a Sync
	// with ReadyForQuery, and pgwire flushes as a side effect of writing it;
	// MySQL has no such packet, but the flush is not optional: outside the
	// skipBatch path the connExecutor never flushes on its own, so a Sync that
	// does nothing leaves every result sitting in the buffer and the client
	// waiting forever for a reply that was already computed.
	flushOnClose
	// noCompletionMsg is a result whose completion the MySQL protocol does not
	// acknowledge. The extended-protocol machinery of PostgreSQL has several
	// such steps (Bind, Describe, Close); MySQL has no equivalent packets, so
	// these results exist only to carry an error if one occurs.
	noCompletionMsg
)

// resultFlags packs the per-statement switches of a commandResult into one
// field. Each switch is an independent bit.
type resultFlags uint8

const (
	// flagWroteColumns records whether the column definitions have gone into
	// the write buffer. A statement that returns rows must emit them exactly
	// once, before the first row.
	flagWroteColumns resultFlags = 1 << iota
	// flagBufferingDisabled forces every row to be flushed as it arrives, for
	// statements that stream results indefinitely.
	flagBufferingDisabled
	// flagBinary selects the binary protocol for rows, which is what a result
	// set produced by a prepared statement carries.
	flagBinary
)

// has reports whether every bit in f is set.
func (fl resultFlags) has(f resultFlags) bool { return fl&f == f }

// commandResult is the MySQL-side implementation of sql.CommandResult. The
// connExecutor writes query output into it, and it turns that output into
// MySQL packets buffered on the connection.
//
// One instance is live at a time per connection, mirroring pgwire: the
// connExecutor produces results strictly in order, so reusing a single struct
// avoids an allocation per statement.
type commandResult struct {
	conn *conn
	conv sessiondata.DataConversionConfig
	pos  sql.CmdPos

	err error
	// errExpected asserts that a result created for an error path actually
	// receives one before it closes.
	errExpected bool

	typ completionMsgType

	stmtType     tree.StatementType
	descOpt      sql.RowDescOpt
	rowsAffected int

	// columns is retained between SetColumns and Close so that the result-set
	// terminator can be emitted with the right shape, and so that AddRow knows
	// how many values to expect.
	columns sqlbase.ResultColumns

	// flags holds the per-statement switches; see resultFlags.
	flags resultFlags

	// binaryTypes caches the MySQL type of each column, which binary rows are
	// encoded against. It is filled on the first row.
	binaryTypes []byte

	// prepare, when non-nil, marks this result as the Describe half of a
	// COM_STMT_PREPARE. Closing it writes the prepare response.
	prepare *preparedStmt
	// preparing, when non-nil, marks this result as the Prepare half of a
	// COM_STMT_PREPARE. It exists so that a failed preparation can release the
	// handle it reserved.
	preparing *preparedStmt
	// inferredTypes holds the placeholder types a Describe reported.
	inferredTypes []oid.Oid

	released bool
}

var _ sql.CommandResult = &commandResult{}

// SetColumns is part of the sql.RestrictedCommandResult interface.
//
// The column definitions are not written here. MySQL requires the column count
// to precede them, and a statement can still fail between planning and its
// first row; emitting the header lazily lets such a failure be reported as an
// ERR packet instead of a truncated result set.
// A binary result set always carries its column definitions, even though the
// SQL layer passes DontNeedRowDesc for a portal: MySQL repeats the metadata on
// every COM_STMT_EXECUTE, where PostgreSQL sends it once at Describe time.
func (r *commandResult) SetColumns(ctx context.Context, cols sqlbase.ResultColumns) {
	r.assertNotReleased()
	if r.descOpt == sql.NeedRowDesc || r.flags.has(flagBinary) {
		r.columns = cols
	}
}

// AddRow is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) error {
	r.assertNotReleased()
	if r.err != nil {
		// Once an error is set the client will receive an ERR packet, so any
		// further rows would be bytes the client never reads as rows.
		return r.err
	}
	if r.zeroColumns() {
		// See zeroColumns: the statement is answered with an OK packet at
		// Close, and the rows themselves have nothing to carry.
		r.rowsAffected++
		return nil
	}
	if !r.flags.has(flagWroteColumns) {
		if err := r.conn.bufferResultSetHeader(r.columns); err != nil {
			return err
		}
		r.flags |= flagWroteColumns
	}
	r.rowsAffected++
	if r.flags.has(flagBinary) {
		if r.binaryTypes == nil {
			r.binaryTypes = make([]byte, len(r.columns))
			for i := range r.columns {
				r.binaryTypes[i], _, _, _, _ = mysqlType(r.columns[i].Typ)
			}
		}
		if err := r.conn.bufferBinaryRow(ctx, row, r.binaryTypes, r.conv); err != nil {
			return err
		}
	} else if err := r.conn.bufferRow(ctx, row, r.conv); err != nil {
		return err
	}
	if r.flags.has(flagBufferingDisabled) {
		return r.conn.Flush(r.pos)
	}
	_, err := r.conn.maybeFlush(r.pos)
	return err
}

// Close is part of the sql.CommandResultClose interface. It writes whatever
// packet terminates this result.
func (r *commandResult) Close(ctx context.Context, t sql.TransactionStatusIndicator) {
	r.assertNotReleased()
	defer r.release()

	if r.errExpected && r.err == nil {
		panic("mysqlwire: result closed without the expected error")
	}

	atomic.StoreInt32(&r.conn.lastTxnStatus, int32(t))
	if r.err != nil {
		if r.preparing != nil {
			// Preparation failed, so the SQL layer skips the rest of this batch,
			// including the Describe that would have completed the statement.
			// Releasing the reservation here keeps the queue aligned; leaving it
			// would hand this statement's slot to the next prepare's Describe.
			r.conn.stmts.takePending()
		}
		r.conn.bufferErr(ctx, r.err)
		return
	}
	if r.prepare != nil {
		r.closePrepare()
		return
	}
	switch r.typ {
	case flushOnClose:
		// The error is recorded on the connection.
		_ = r.conn.Flush(r.pos)
		return
	case noCompletionMsg:
		return
	}

	r.endResult(ctx, t, r.stmtType, r.rowsAffected, r.conn.moreResultsAfter(r.pos))
}

// zeroColumns reports whether this result is a row-returning statement whose
// rows have no columns, e.g. SELECT * FROM a table created as CREATE TABLE t ().
// MySQL cannot express that: a result set opens with its column count, and a
// count of zero is the first byte of an OK packet, so the client parses the
// "result set" as an OK and the rest of the response as the reply to its next
// command. Such a statement is answered with an OK packet instead.
func (r *commandResult) zeroColumns() bool {
	return len(r.columns) == 0 && (r.descOpt == sql.NeedRowDesc || r.flags.has(flagBinary))
}

// endResult writes the packet that ends one result: the terminator of a result
// set, or an OK packet for a statement that returns no rows. moreResults marks
// it as followed by another result in the same response.
func (r *commandResult) endResult(
	ctx context.Context,
	t sql.TransactionStatusIndicator,
	stmtType tree.StatementType,
	rowsAffected int,
	moreResults bool,
) {
	// A statement that produced a row description terminates its result set;
	// everything else reports an affected-row count in an OK packet.
	if r.flags.has(flagWroteColumns) {
		r.conn.bufferResultSetTerminator(t, moreResults)
		return
	}
	if stmtType == tree.Rows && !r.zeroColumns() &&
		(r.descOpt == sql.NeedRowDesc || r.flags.has(flagBinary)) {
		// A row-returning statement that matched nothing still owes the client
		// the column metadata; clients use it to shape an empty result rather
		// than reporting "no such column" on every field access.
		if err := r.conn.bufferResultSetHeader(r.columns); err != nil {
			r.conn.bufferErr(ctx, err)
			return
		}
		r.conn.bufferResultSetTerminator(t, moreResults)
		return
	}
	r.conn.bufferOK(uint64(rowsAffected), t, moreResults)
}

// Discard is part of the sql.CommandResultClose interface.
func (r *commandResult) Discard() {
	r.assertNotReleased()
	r.release()
}

// Err is part of the sql.CommandResultErrBase interface.
func (r *commandResult) Err() error {
	r.assertNotReleased()
	return r.err
}

// SetError is part of the sql.CommandResultErrBase interface.
func (r *commandResult) SetError(err error) {
	r.assertNotReleased()
	r.err = err
}

// IncrementRowsAffected is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) IncrementRowsAffected(n int) {
	r.assertNotReleased()
	r.rowsAffected += n
}

// RowsAffected is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) RowsAffected() int {
	r.assertNotReleased()
	return r.rowsAffected
}

// ResetStmtType is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.assertNotReleased()
	r.stmtType = stmt.StatementType()
}

// DisableBuffering is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) DisableBuffering() {
	r.assertNotReleased()
	r.flags |= flagBufferingDisabled
}

// BufferedResultsLen is part of the sql.RestrictedCommandResult interface.
//
// The value is a count of buffered packets rather than of bytes. The SQL layer
// treats it as an opaque mark: it reads one here and hands the same value back
// to TruncateBufferedResults, so the unit only has to agree with itself.
func (r *commandResult) BufferedResultsLen() int {
	return len(r.conn.writerState.packets)
}

// TruncateBufferedResults is part of the sql.RestrictedCommandResult
// interface. It supports automatic transaction retries: results buffered but
// not yet flushed can be discarded and regenerated.
func (r *commandResult) TruncateBufferedResults(idx int) bool {
	return r.conn.truncateBufferedResults(idx)
}

// AppendParamStatusUpdate is part of the sql.RestrictedCommandResult
// interface.
//
// PostgreSQL pushes session variable changes to the client with ParameterStatus
// messages. MySQL has no unsolicited equivalent — clients discover session
// state by querying it — so these updates are dropped.
func (r *commandResult) AppendParamStatusUpdate(param string, val string) {}

// AppendNotice is part of the sql.RestrictedCommandResult interface.
//
// MySQL reports warnings as a count in the OK packet, which the client then
// retrieves with SHOW WARNINGS. Since KWDB does not retain notices for later
// retrieval, surfacing only the count would promise a SHOW WARNINGS result we
// cannot produce, so notices are dropped rather than counted.
func (r *commandResult) AppendNotice(noticeErr error) {}

// WarningForExport is part of the sql.RestrictedCommandResult interface. It
// exists for the EXPORT statement's progress reporting, which has no MySQL
// protocol equivalent.
func (r *commandResult) WarningForExport(warning error) {}

// SetClient is part of the sql.RestrictedCommandResult interface. Client
// encoding is negotiated in the MySQL handshake, not per statement.
func (r *commandResult) SetClient(s string) {}

// IsCommandResult is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) IsCommandResult() bool { return true }

// SupportsPGWireResults is part of the sql.RestrictedCommandResult interface.
// A MySQL client cannot read PostgreSQL wire bytes, so the planner must not
// choose the TS engine's short circuit for this connection.
func (r *commandResult) SupportsPGWireResults() bool { return false }

// IsLimit is part of the sql.RestrictedCommandResult interface. Portals with
// row limits belong to the PostgreSQL extended protocol; MySQL's text protocol
// has no equivalent.
func (r *commandResult) IsLimit() bool { return false }

// StmtType is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) StmtType() tree.StatementType { return r.stmtType }

// AddPGResult is part of the sql.RestrictedCommandResult interface. It exists
// so that pre-encoded PostgreSQL wire bytes can be forwarded verbatim, which a
// MySQL connection must never do.
//
// The planner asks SupportsPGWireResults before choosing the path that produces
// these bytes, so nothing should call this. Returning an error rather than
// dropping the bytes means a future regression in that guard surfaces as a
// failed statement instead of an empty result set. It is an error rather than a
// panic because this runs on a live client connection: a client must not be
// able to take the node down.
func (r *commandResult) AddPGResult(ctx context.Context, res []byte) error {
	return errors.AssertionFailedf(
		"mysqlwire: received %d bytes of pre-encoded PostgreSQL wire data; "+
			"the query short circuit must not be planned for a MySQL connection", len(res))
}

// AddPGComplete is part of the sql.RestrictedCommandResult interface. On the
// TS engine's pgwire short circuit it is inapplicable for the same reason as
// AddPGResult; unlike AddPGResult it has no error return, so it can only
// ignore the call there.
//
// It is also called independently, from plan_node_to_row_source.go, to mark
// the boundary between the result sets of a CALL <procedure>() statement.
// There it ends the current result set with SERVER_MORE_RESULTS_EXISTS and
// resets the result so that the next set, whose columns arrive through
// SetColumns, opens with its own column definitions. Without the flag the
// client cannot tell where one set ends: rows of a later set were read under
// the first set's columns (dropping any extra ones), or failed to decode.
func (r *commandResult) AddPGComplete(cmd string, typ tree.StatementType, rowsAffected int) {
	r.assertNotReleased()
	if r.err != nil || r.typ != commandComplete || r.prepare != nil {
		return
	}
	// The statement has not closed yet, so the connExecutor has not supplied
	// a transaction state; the one the previous result closed with is what
	// the client last saw and is still in effect inside the procedure.
	t := sql.TransactionStatusIndicator(atomic.LoadInt32(&r.conn.lastTxnStatus))
	// endResult only needs a context to report an encoding error, and this
	// interface method does not receive one.
	r.endResult(context.Background(), t, typ, rowsAffected, true /* moreResults */)
	r.flags &^= flagWroteColumns
	r.binaryTypes = nil
	r.rowsAffected = 0
}

// SetInferredTypes is part of the sql.DescribeResult interface. It reports the
// placeholder types the SQL layer inferred, which a COM_STMT_PREPARE response
// passes on so that the client knows how to encode its arguments.
func (r *commandResult) SetInferredTypes(types []oid.Oid) {
	r.assertNotReleased()
	r.inferredTypes = types
}

// SetInferredTypesEx is part of the sql.DescribeResult interface. It is the
// variant the direct time-series insert path takes -- conn_executor_prepare.go
// calls it instead of SetInferredTypes when the prepared statement is a
// payload insert -- carrying tag placement and column storage widths alongside
// the same placeholder types, so that a PostgreSQL client can assemble the
// payload itself.
//
// MySQL COM_STMT_PREPARE has no field for any of that: its response reports
// parameter and column counts and their types, and nothing about tags or
// storage widths. The types are recorded exactly as SetInferredTypes does,
// because the client still needs them to encode its arguments; the three
// extension arguments are dropped. A MySQL client therefore loses only the
// payload short-cut and inserts through the ordinary path, which is the same
// thing it does for every other statement.
func (r *commandResult) SetInferredTypesEx(types []oid.Oid, _ int16, _ []uint16, _ []uint32) {
	r.SetInferredTypes(types)
}

// SetNoDataRowDescription is part of the sql.DescribeResult interface. A
// statement returning no rows reports zero columns in its prepare response,
// which is the state this result already carries.
func (r *commandResult) SetNoDataRowDescription() {
	r.assertNotReleased()
	r.columns = nil
}

// SetPrepStmtOutput is part of the sql.DescribeResult interface. It reports the
// result shape of a prepared statement.
func (r *commandResult) SetPrepStmtOutput(ctx context.Context, cols sqlbase.ResultColumns) {
	r.assertNotReleased()
	r.columns = cols
}

// closePrepare writes a COM_STMT_PREPARE response and publishes the statement's
// handle.
//
// The metadata is recorded on the statement before the response goes out,
// because the response is what unblocks the client to send COM_STMT_EXECUTE,
// and executing needs the parameter types this Describe just produced.
func (r *commandResult) closePrepare() {
	ps := r.prepare
	r.conn.stmts.setMetadata(ps, r.inferredTypes, r.columns)
	r.conn.stmts.commit(ps)
	r.conn.bufferStmtPrepareOK(ps.id, r.inferredTypes, r.columns)
}

// SetPortalOutput is part of the sql.DescribeResult interface.
func (r *commandResult) SetPortalOutput(
	ctx context.Context, cols sqlbase.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
}

func (r *commandResult) release() { *r = commandResult{released: true} }

func (r *commandResult) assertNotReleased() {
	if r.released {
		panic("mysqlwire: command result used after release")
	}
}

// newCommandResult prepares the connection's result for one statement.
func (c *conn) newCommandResult(
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	stmt tree.Statement,
	conv sessiondata.DataConversionConfig,
) sql.CommandResult {
	r := &c.res
	*r = commandResult{
		conn:     c,
		conv:     conv,
		pos:      pos,
		typ:      commandComplete,
		stmtType: stmt.StatementType(),
		descOpt:  descOpt,
	}
	return r
}

// newMiscResult prepares a result for a command that has no MySQL completion
// packet of its own.
func (c *conn) newMiscResult(pos sql.CmdPos, typ completionMsgType) *commandResult {
	r := &c.res
	*r = commandResult{
		conn: c,
		pos:  pos,
		typ:  typ,
	}
	return r
}

// CreateStatementResult is part of the sql.ClientComm interface.
func (c *conn) CreateStatementResult(
	stmt tree.Statement,
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
	limit int,
	portalName string,
	implicitTxn bool,
) sql.CommandResult {
	r := c.newCommandResult(descOpt, pos, stmt, conv)
	// The format code reaches here untouched from the BindStmt this package
	// pushed, so a binary request can only have come from COM_STMT_EXECUTE.
	if len(formatCodes) > 0 && formatCodes[0] == pgwirebase.FormatBinary {
		c.res.flags |= flagBinary
	}
	return r
}

// CreateSyncResult is part of the sql.ClientComm interface.
//
// Closing this result is what puts a statement's output on the wire; see
// flushOnClose.
func (c *conn) CreateSyncResult(pos sql.CmdPos) sql.SyncResult {
	return c.newMiscResult(pos, flushOnClose)
}

// CreateFlushResult is part of the sql.ClientComm interface.
func (c *conn) CreateFlushResult(pos sql.CmdPos) sql.FlushResult {
	return c.newMiscResult(pos, flushOnClose)
}

// CreateDrainResult is part of the sql.ClientComm interface.
func (c *conn) CreateDrainResult(pos sql.CmdPos) sql.DrainResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateBindResult is part of the sql.ClientComm interface.
func (c *conn) CreateBindResult(pos sql.CmdPos) sql.BindResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreatePrepareResult is part of the sql.ClientComm interface.
//
// Success writes nothing: the response to COM_STMT_PREPARE is written by the
// Describe that follows. The statement is attached only so that a failure can
// release the handle it reserved.
func (c *conn) CreatePrepareResult(pos sql.CmdPos) sql.ParseResult {
	r := c.newMiscResult(pos, noCompletionMsg)
	r.preparing = c.stmts.peekPending()
	return r
}

// CreateDescribeResult is part of the sql.ClientComm interface.
//
// The only Describe this package pushes is the one that completes a
// COM_STMT_PREPARE, so the result is bound to the statement awaiting its
// metadata.
func (c *conn) CreateDescribeResult(pos sql.CmdPos) sql.DescribeResult {
	r := c.newMiscResult(pos, noCompletionMsg)
	r.prepare = c.stmts.takePending()
	return r
}

// CreateDeleteResult is part of the sql.ClientComm interface.
func (c *conn) CreateDeleteResult(pos sql.CmdPos) sql.DeleteResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateEmptyQueryResult is part of the sql.ClientComm interface.
//
// An empty query is not an error in either protocol; MySQL acknowledges it
// with a plain OK packet, which the result's Close emits.
func (c *conn) CreateEmptyQueryResult(pos sql.CmdPos) sql.EmptyQueryResult {
	return c.newMiscResult(pos, commandComplete)
}

// CreateErrorResult is part of the sql.ClientComm interface.
func (c *conn) CreateErrorResult(pos sql.CmdPos) sql.ErrorResult {
	r := c.newMiscResult(pos, noCompletionMsg)
	r.errExpected = true
	return r
}

// CreateCopyInResult is part of the sql.ClientComm interface.
//
// COPY has no MySQL counterpart; LOAD DATA INFILE is a different protocol
// exchange that KWDB does not implement. The result exists so that a COPY
// statement fails cleanly rather than leaving the connection wedged.
func (c *conn) CreateCopyInResult(pos sql.CmdPos) sql.CopyInResult {
	return c.newMiscResult(pos, noCompletionMsg)
}
