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
	"encoding/hex"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
)

// MySQL's prepared statements map onto the same connExecutor commands that
// drive PostgreSQL's extended query protocol — PrepareStmt, DescribeStmt,
// BindStmt, ExecPortal — because the two protocols are asking for the same
// thing in different packets. What differs is that MySQL wants the parameter
// and result metadata returned once, at prepare time, in a single response,
// where PostgreSQL lets a client describe whenever it likes.
//
// Arguments arrive in MySQL's binary encoding but are handed to the SQL layer
// as text. The executor decodes arguments with pgwirebase.DecodeOidDatum
// against the type it inferred for each placeholder, and its text decoding
// accepts exactly the spellings the binary reader produces. Encoding into
// PostgreSQL's binary format instead would mean translating MySQL binary to
// PostgreSQL binary for every type, with no gain: the values are small and the
// conversion cost is dwarfed by planning.

// preparedStmt is one statement prepared by COM_STMT_PREPARE.
type preparedStmt struct {
	// id is the handle the client uses to refer to this statement.
	id uint32
	// name identifies the statement in the connExecutor's namespace.
	name string
	// sql is retained for error messages.
	sql string

	// paramTypes are the placeholder types the SQL layer inferred. They decide
	// how a binary argument is rendered for the executor, and what type is
	// reported in the parameter definitions.
	paramTypes []oid.Oid
	// columns describe the result. Empty for a statement returning no rows.
	columns sqlbase.ResultColumns

	// boundTypes remembers the parameter types from the last COM_STMT_EXECUTE.
	// A client may set the new-params-bound flag to 0 to say "same types as
	// last time", and without this the value block could not be parsed.
	boundTypes []mysqlwirebase.ParamType

	// longData accumulates COM_STMT_SEND_LONG_DATA payloads, indexed by
	// parameter. A nil entry means the argument was not streamed and will
	// appear in the COM_STMT_EXECUTE value block instead.
	longData [][]byte
}

// stmtRegistry holds a connection's prepared statements.
//
// It is guarded because the two goroutines serving a connection both touch it:
// the reader creates and looks up statements, while the connExecutor's goroutine
// fills in the metadata that a Describe produced. The client cannot execute a
// statement before it has seen the prepare response, so the two never contend
// for the same statement in practice, but the handoff still crosses goroutines
// and needs to be published safely.
type stmtRegistry struct {
	syncutil.Mutex
	nextID uint32
	stmts  map[uint32]*preparedStmt
	// pending holds the statements whose Describe has not yet run, oldest
	// first. A statement becomes visible in stmts only once its describe
	// completes, so that one whose preparation failed leaves no handle behind.
	//
	// This is a queue rather than a single slot because nothing stops a client
	// from sending a second COM_STMT_PREPARE before reading the first response.
	// Real clients wait, but with one slot a client that did not would silently
	// receive the second statement's metadata under the first one's handle.
	pending []*preparedStmt
}

func (reg *stmtRegistry) init() {
	reg.stmts = make(map[uint32]*preparedStmt)
}

// begin allocates a handle and records the statement as pending.
func (reg *stmtRegistry) begin(query string) *preparedStmt {
	reg.Lock()
	defer reg.Unlock()
	reg.nextID++
	ps := &preparedStmt{
		id:   reg.nextID,
		name: fmt.Sprintf("mysql_stmt_%d", reg.nextID),
		sql:  query,
	}
	reg.pending = append(reg.pending, ps)
	return ps
}

// commit publishes a statement, making its handle usable.
func (reg *stmtRegistry) commit(ps *preparedStmt) {
	reg.Lock()
	defer reg.Unlock()
	ps.longData = make([][]byte, len(ps.paramTypes))
	reg.stmts[ps.id] = ps
}

// peekPending returns the oldest statement awaiting its Describe without
// removing it.
func (reg *stmtRegistry) peekPending() *preparedStmt {
	reg.Lock()
	defer reg.Unlock()
	if len(reg.pending) == 0 {
		return nil
	}
	return reg.pending[0]
}

// takePending hands over the oldest statement awaiting its Describe, if any.
func (reg *stmtRegistry) takePending() *preparedStmt {
	reg.Lock()
	defer reg.Unlock()
	if len(reg.pending) == 0 {
		return nil
	}
	ps := reg.pending[0]
	reg.pending = reg.pending[1:]
	return ps
}

func (reg *stmtRegistry) get(id uint32) (*preparedStmt, bool) {
	reg.Lock()
	defer reg.Unlock()
	ps, ok := reg.stmts[id]
	return ps, ok
}

func (reg *stmtRegistry) remove(id uint32) (*preparedStmt, bool) {
	reg.Lock()
	defer reg.Unlock()
	ps, ok := reg.stmts[id]
	if ok {
		delete(reg.stmts, id)
	}
	return ps, ok
}

// setMetadata records what a Describe reported. Called from the connExecutor's
// goroutine.
func (reg *stmtRegistry) setMetadata(
	ps *preparedStmt, paramTypes []oid.Oid, columns sqlbase.ResultColumns,
) {
	reg.Lock()
	defer reg.Unlock()
	ps.paramTypes = paramTypes
	ps.columns = columns
}

// setBoundTypes records the parameter types of a COM_STMT_EXECUTE and clears
// the long-data buffers it consumed.
func (reg *stmtRegistry) setBoundTypes(ps *preparedStmt, bound []mysqlwirebase.ParamType) {
	reg.Lock()
	defer reg.Unlock()
	ps.boundTypes = bound
	// Streamed arguments apply to exactly one execution; MySQL discards them
	// once consumed, and a client that streams again before the next execute
	// expects to start from empty.
	ps.longData = make([][]byte, len(ps.paramTypes))
}

// clearLongData discards streamed arguments without touching the bound types.
func (reg *stmtRegistry) clearLongData(ps *preparedStmt) {
	reg.Lock()
	defer reg.Unlock()
	ps.longData = make([][]byte, len(ps.paramTypes))
}

// appendLongData accumulates a COM_STMT_SEND_LONG_DATA chunk.
func (reg *stmtRegistry) appendLongData(ps *preparedStmt, param int, chunk []byte) bool {
	reg.Lock()
	defer reg.Unlock()
	if param < 0 || param >= len(ps.longData) {
		return false
	}
	ps.longData[param] = append(ps.longData[param], chunk...)
	return true
}

// snapshot returns the fields a COM_STMT_EXECUTE needs, copied under the lock.
func (reg *stmtRegistry) snapshot(
	ps *preparedStmt,
) (paramTypes []oid.Oid, boundTypes []mysqlwirebase.ParamType, longData [][]byte) {
	reg.Lock()
	defer reg.Unlock()
	return ps.paramTypes, ps.boundTypes, ps.longData
}

// handleStmtPrepare implements COM_STMT_PREPARE.
//
// The response cannot be written here: the parameter and column metadata it
// must carry is only known once the SQL layer has planned the statement. The
// reader queues a Prepare and a Describe, and the Describe's result writes the
// whole response when it closes. See commandResult.closePrepare.
func (c *conn) handleStmtPrepare(ctx context.Context, query string) error {
	// MySQL numbers placeholders by position and writes them all as `?`; KWDB's
	// parser only knows the `$N` form. Rewriting here rather than in the
	// grammar keeps `?` reading as the JSON existence operator everywhere else.
	// See rewritePlaceholders.
	query, _ = rewritePlaceholders(query)
	// Connection pools may prepare their session setup statements rather than
	// issuing them directly, so the same dialect rewrites apply here. Going
	// through the one entry point is what keeps this path and COM_QUERY from
	// drifting apart.
	query = rewriteMySQLDialect(query, c.dialect())

	startParse := timeutil.Now()
	stmts, err := c.parser.ParseWithInt(query, c.unqualifiedIntSize(), c.sv)
	endParse := timeutil.Now()
	if err != nil {
		c.bufferErr(ctx, err)
		return c.Flush(0)
	}
	if len(stmts) != 1 {
		// MySQL prepares exactly one statement per call, and the response has
		// room for exactly one statement's metadata.
		c.bufferPacket(mysqlwirebase.EncodeErr(
			mysqlwirebase.ErrParse, mysqlwirebase.SQLStateSyntaxError,
			"COM_STMT_PREPARE accepts a single statement", c.capabilities))
		return c.Flush(0)
	}

	ps := c.stmts.begin(query)
	if err := c.push(ctx, sql.PrepareStmt{
		Name:       ps.name,
		Statement:  stmts[0],
		ParseStart: startParse,
		ParseEnd:   endParse,
	}); err != nil {
		return err
	}
	if err := c.push(ctx, sql.DescribeStmt{
		Name: ps.name,
		Type: pgwirebase.PrepareStatement,
	}); err != nil {
		return err
	}
	return c.push(ctx, sql.Sync{})
}

// cursorTypeMask covers the cursor bits of a COM_STMT_EXECUTE flags byte:
// READ_ONLY, FOR_UPDATE and SCROLLABLE. Bits outside the mask are ignored
// rather than rejected, because MySQL has added flags here over time and a
// newer client setting one is not asking for behavior this server lacks.
const cursorTypeMask byte = 0x01 | 0x02 | 0x04

// handleStmtExecute implements COM_STMT_EXECUTE.
func (c *conn) handleStmtExecute(ctx context.Context, body []byte, timeReceived time.Time) error {
	r := mysqlwirebase.NewReader(body)

	stmtID, err := r.Uint32()
	if err != nil {
		return c.stmtProtocolErr(ctx, "reading statement id: %v", err)
	}
	ps, ok := c.stmts.get(stmtID)
	if !ok {
		return c.stmtUnknownHandle(ctx, stmtID)
	}

	flags, err := r.Byte()
	if err != nil {
		return c.stmtProtocolErr(ctx, "reading flags: %v", err)
	}
	if flags&cursorTypeMask != 0 {
		// Serving a cursor means holding a portal open across commands and
		// resuming it on COM_STMT_FETCH. KWDB's portals can do that, but nothing
		// here drives them that way yet, and silently returning the whole result
		// set would break a client that asked for a cursor precisely because the
		// result does not fit in memory.
		c.bufferPacket(mysqlwirebase.EncodeErr(
			mysqlwirebase.ErrNotSupportedYet, mysqlwirebase.SQLStateFeatureDisabled,
			"cursors are not supported; execute with CURSOR_TYPE_NO_CURSOR",
			c.capabilities))
		return c.Flush(0)
	}
	if _, err := r.Uint32(); err != nil { // iteration count, always 1
		return c.stmtProtocolErr(ctx, "reading iteration count: %v", err)
	}

	paramOids, boundTypes, longData := c.stmts.snapshot(ps)
	numParams := len(paramOids)

	var params []mysqlwirebase.Param
	if numParams > 0 {
		nullBitmap, err := r.Bytes(mysqlwirebase.NullBitmapLen(numParams, 0 /* offset */))
		if err != nil {
			return c.stmtProtocolErr(ctx, "reading NULL bitmap: %v", err)
		}
		newParamsBound, err := r.Byte()
		if err != nil {
			return c.stmtProtocolErr(ctx, "reading new-params-bound flag: %v", err)
		}
		if newParamsBound != 0 {
			if boundTypes, err = mysqlwirebase.ReadParamTypes(r, numParams); err != nil {
				return c.stmtProtocolErr(ctx, "%v", err)
			}
		} else if len(boundTypes) != numParams {
			// The client said "reuse the previous types" without ever having
			// sent any, so the value block cannot be parsed.
			return c.stmtProtocolErr(ctx,
				"new-params-bound is 0 but no parameter types were sent previously")
		}
		if params, err = mysqlwirebase.ReadParams(r, boundTypes, nullBitmap, longData); err != nil {
			return c.stmtProtocolErr(ctx, "%v", err)
		}
		c.stmts.setBoundTypes(ps, boundTypes)
	}

	args := make([][]byte, numParams)
	for i, p := range params {
		args[i] = renderParam(p, paramOids[i])
	}

	if err := c.push(ctx, sql.BindStmt{
		PreparedStatementName: ps.name,
		// The unnamed portal may be overwritten freely, which is what repeated
		// execution of the same statement needs; a named portal would have to be
		// deleted between executions.
		PortalName: "",
		Args:       args,
		// Arguments were rendered into MySQL's text spelling above.
		ArgFormatCodes: []pgwirebase.FormatCode{pgwirebase.FormatText},
		// Results must come back in the binary protocol. Within mysqlwire this
		// code selects MySQL's binary encoding, not PostgreSQL's; it reaches the
		// result untouched by the SQL layer, which only carries it.
		OutFormats: []pgwirebase.FormatCode{pgwirebase.FormatBinary},
	}); err != nil {
		return err
	}
	if err := c.push(ctx, sql.ExecPortal{
		Name:         "",
		TimeReceived: timeReceived,
	}); err != nil {
		return err
	}
	return c.push(ctx, sql.Sync{})
}

// handleStmtClose implements COM_STMT_CLOSE, which expects no response.
func (c *conn) handleStmtClose(ctx context.Context, body []byte) error {
	r := mysqlwirebase.NewReader(body)
	stmtID, err := r.Uint32()
	if err != nil {
		// A malformed close has no reply to carry the complaint, and the client
		// is not waiting for one.
		return nil
	}
	ps, ok := c.stmts.remove(stmtID)
	if !ok {
		return nil
	}
	// Release the SQL layer's copy too; otherwise a long-lived connection that
	// prepares repeatedly accumulates plans for statements the client has
	// forgotten.
	if err := c.push(ctx, sql.DeletePreparedStmt{
		Name: ps.name,
		Type: pgwirebase.PrepareStatement,
	}); err != nil {
		return err
	}
	return c.push(ctx, sql.Sync{})
}

// handleStmtReset implements COM_STMT_RESET, which discards streamed arguments
// and acknowledges with an OK packet.
func (c *conn) handleStmtReset(ctx context.Context, body []byte) error {
	r := mysqlwirebase.NewReader(body)
	stmtID, err := r.Uint32()
	if err != nil {
		return c.stmtProtocolErr(ctx, "reading statement id: %v", err)
	}
	ps, ok := c.stmts.get(stmtID)
	if !ok {
		return c.stmtUnknownHandle(ctx, stmtID)
	}
	c.stmts.clearLongData(ps)
	c.bufferOK(0, sql.IdleTxnBlock, false /* moreResults */)
	return c.Flush(0)
}

// handleStmtSendLongData implements COM_STMT_SEND_LONG_DATA, which streams one
// argument in chunks ahead of COM_STMT_EXECUTE and expects no response.
func (c *conn) handleStmtSendLongData(ctx context.Context, body []byte) error {
	r := mysqlwirebase.NewReader(body)
	stmtID, err := r.Uint32()
	if err != nil {
		return nil
	}
	paramID, err := r.Uint16()
	if err != nil {
		return nil
	}
	ps, ok := c.stmts.get(stmtID)
	if !ok {
		return nil
	}
	// An out-of-range parameter is dropped rather than reported: the command has
	// no reply packet, and the COM_STMT_EXECUTE that follows will fail with a
	// message the client can act on.
	if !c.stmts.appendLongData(ps, int(paramID), r.Remaining()) {
		log.VEventf(ctx, 2, "mysqlwire: long data for out-of-range parameter %d of statement %d",
			paramID, stmtID)
	}
	return nil
}

// stmtUnknownHandle reports a statement id the client never received.
func (c *conn) stmtUnknownHandle(ctx context.Context, stmtID uint32) error {
	log.VEventf(ctx, 2, "mysqlwire: unknown prepared statement handle %d", stmtID)
	c.bufferPacket(mysqlwirebase.EncodeErr(
		mysqlwirebase.ErrUnknownStmtHandler, mysqlwirebase.SQLStateGeneralError,
		fmt.Sprintf("Unknown prepared statement handler (%d) given to mysqld_stmt_execute", stmtID),
		c.capabilities))
	return c.Flush(0)
}

// stmtProtocolErr reports a COM_STMT_* packet this server could not parse.
func (c *conn) stmtProtocolErr(ctx context.Context, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	log.VEventf(ctx, 2, "mysqlwire: malformed prepared-statement packet: %s", msg)
	c.bufferPacket(mysqlwirebase.EncodeErr(
		mysqlwirebase.ErrMalformedPacket, mysqlwirebase.SQLStateGeneralError,
		"Malformed communication packet: "+msg, c.capabilities))
	return c.Flush(0)
}

// renderParam converts a decoded binary argument into the text the executor's
// argument decoding expects for the placeholder's inferred type.
func renderParam(p mysqlwirebase.Param, typ oid.Oid) []byte {
	if p.Null {
		// A nil argument is how the SQL layer spells NULL.
		return nil
	}
	if typ == oid.T_bytea {
		// KWDB decodes a bytea argument from text, where raw bytes would be read
		// as an escape sequence. The hex form round-trips any byte string.
		out := make([]byte, 0, 2+hex.EncodedLen(len(p.Value)))
		out = append(out, '\\', 'x')
		return append(out, []byte(hex.EncodeToString(p.Value))...)
	}
	return p.Value
}

// paramColumnDefinition describes a placeholder in a COM_STMT_PREPARE
// response.
//
// MySQL names every parameter "?" and reports a type. Clients use the type to
// decide how to encode the argument they will send, so reporting the type the
// SQL layer inferred is what makes a driver send, say, an integer rather than
// a string for an integer placeholder.
func paramColumnDefinition(o oid.Oid) *mysqlwirebase.ColumnDefinition {
	typ := mysqlwirebase.TypeVarString
	length := uint32(65535)
	decimals := byte(decimalsNotApplicable)
	charset := mysqlwirebase.CollationUTF8MB4GeneralCI
	var flags uint16
	if t, ok := types.OidToType[o]; ok {
		typ, length, decimals, flags, charset = mysqlType(t)
	}
	return &mysqlwirebase.ColumnDefinition{
		Name:         "?",
		OrgName:      "?",
		CharacterSet: charset,
		ColumnLength: length,
		Type:         typ,
		Flags:        flags,
		Decimals:     decimals,
	}
}
