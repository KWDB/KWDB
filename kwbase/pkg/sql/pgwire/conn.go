// Copyright 2018 The Cockroach Authors.
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

package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/setting"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/lib/pq/oid"
)

// conn implements a pgwire network connection (version 3 of the protocol,
// implemented by Postgres v7.4 and later). conn.serve() reads protocol
// messages, transforms them into commands that it pushes onto a StmtBuf (where
// they'll be picked up and executed by the connExecutor).
// The connExecutor produces results for the commands, which are delivered to
// the client through the sql.ClientComm interface, implemented by this conn
// (code is in command_result.go).
type conn struct {
	conn net.Conn

	sessionArgs sql.SessionArgs

	metrics *ServerMetrics

	// rd is a buffered reader consuming conn. All reads from conn go through
	// this.
	rd bufio.Reader

	// parser is used to avoid allocating a parser each time.
	parser parser.Parser

	// stmtBuf is populated with commands queued for execution by this conn.
	stmtBuf sql.StmtBuf

	// res is used to avoid allocations in the conn's ClientComm implementation.
	res commandResult

	// err is an error, accessed atomically. It represents any error encountered
	// while accessing the underlying network connection. This can read via
	// GetErr() by anybody. If it is found to be != nil, the conn is no longer to
	// be used.
	err atomic.Value

	// writerState groups together all aspects of the write-side state of the
	// connection.
	writerState struct {
		fi flushInfo
		// buf contains command results (rows, etc.) until they're flushed to the
		// network connection.
		buf    bytes.Buffer
		tagBuf [64]byte
	}

	readBuf    pgwirebase.ReadBuffer
	msgBuilder writeBuffer

	sv *settings.Values

	// testingLogEnabled is used in unit tests in this package to
	// force-enable auth logging without dancing around the
	// asynchronicity of cluster settings.
	testingLogEnabled bool
}

// serveConn creates a conn that will serve the netConn. It returns once the
// network connection is closed.
//
// Internally, a connExecutor will be created to execute commands. Commands read
// from the network are buffered in a stmtBuf which is consumed by the
// connExecutor. The connExecutor produces results which are buffered and
// sometimes synchronously flushed to the network.
//
// The reader goroutine (this one) outlives the connExecutor's goroutine (the
// "processor goroutine").
// However, they can both signal each other to stop. Here's how the different
// cases work:
// 1) The reader receives a ClientMsgTerminate protocol packet: the reader
// closes the stmtBuf and also cancels the command processing context. These
// actions will prompt the command processor to finish.
// 2) The reader gets a read error from the network connection: like above, the
// reader closes the command processor.
// 3) The reader's context is canceled (happens when the server is draining but
// the connection was busy and hasn't quit yet): the reader notices the canceled
// context and, like above, closes the processor.
// 4) The processor encouters an error. This error can come from various fatal
// conditions encoutered internally by the processor, or from a network
// communication error encountered while flushing results to the network.
// The processor will cancel the reader's context and terminate.
// Note that query processing errors are different; they don't cause the
// termination of the connection.
//
// Draining notes:
//
// The reader notices that the server is draining by polling the draining()
// closure passed to serveConn. At that point, the reader delegates the
// responsibility of closing the connection to the statement processor: it will
// push a DrainRequest to the stmtBuf which signal the processor to quit ASAP.
// The processor will quit immediately upon seeing that command if it's not
// currently in a transaction. If it is in a transaction, it will wait until the
// first time a Sync command is processed outside of a transaction - the logic
// being that we want to stop when we're both outside transactions and outside
// batches.
func (s *Server) serveConn(
	ctx context.Context,
	netConn net.Conn,
	sArgs sql.SessionArgs,
	reserved mon.BoundAccount,
	authOpt authOptions,
) {
	sArgs.RemoteAddr = netConn.RemoteAddr()

	if log.V(2) {
		log.Infof(ctx, "new connection with options: %+v", sArgs)
	}
	c := newConn(netConn, sArgs, &s.metrics, &s.execCfg.Settings.SV)
	c.testingLogEnabled = atomic.LoadInt32(&s.testingLogEnabled) > 0
	// log user successfully login
	auditLoginInfo := server.MakeAuditInfo(timeutil.Now(), c.sessionArgs.User, c.sessionArgs.Roles, target.Login,
		target.ObjectConn, 0, c.sessionArgs.RemoteAddr, nil)
	if s.SQLServer.GetExecutorConfig() != nil {
		nodeInfo := s.SQLServer.GetExecutorConfig().NodeInfo
		hostIP, hostPort, _ := net.SplitHostPort(s.SQLServer.GetExecutorConfig().Addr)
		auditLoginInfo.SetReporter(nodeInfo.ClusterID(), nodeInfo.NodeID.Get(), hostIP, hostPort, "", 0)
	}
	if err := s.SQLServer.GetExecutorConfig().AuditServer.LogAudit(ctx, nil, &auditLoginInfo); err != nil {
		log.Errorf(ctx, "log audit info with err: %s", err)
	}
	// Do the reading of commands from the network.
	c.serveImpl(ctx, s.IsDraining, s.SQLServer, reserved, authOpt, s.stopper, s.cfg.Addr, s.cfg.AdvertiseAddr)
	auditLogoutInfo := server.MakeAuditInfo(timeutil.Now(), c.sessionArgs.User, c.sessionArgs.Roles, target.Logout,
		target.ObjectConn, 0, c.sessionArgs.RemoteAddr, nil)
	if err := s.SQLServer.GetExecutorConfig().AuditServer.LogAudit(ctx, nil, &auditLogoutInfo); err != nil {
		log.Errorf(ctx, "log audit info with err: %s", err)
	}
}

func newConn(
	netConn net.Conn, sArgs sql.SessionArgs, metrics *ServerMetrics, sv *settings.Values,
) *conn {
	c := &conn{
		conn:        netConn,
		sessionArgs: sArgs,
		metrics:     metrics,
		rd:          *bufio.NewReader(netConn),
		sv:          sv,
	}
	c.stmtBuf.Init()
	c.res.released = true
	c.writerState.fi.buf = &c.writerState.buf
	c.writerState.fi.lastFlushed = -1
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)
	c.msgBuilder.init(metrics.BytesOutCount)

	return c
}

func (c *conn) setErr(err error) {
	c.err.Store(err)
}

func (c *conn) GetErr() error {
	err := c.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

func (c *conn) authLogEnabled() bool {
	return c.testingLogEnabled || logSessionAuth.Get(c.sv)
}

// getRolesOfUser performs the actual recursive role membership lookup.
// we could save detailed memberships (as opposed to fully expanded) and reuse them
// across users. We may then want to lookup more than just this user.
func (c *conn) getRolesOfUser(
	ctx context.Context, ie *sql.InternalExecutor, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := ie.Query(ctx, "expand-roles", nil, lookupRolesStmt, m)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	return ret, nil
}

// serveImpl continuously reads from the network connection and pushes execution
// instructions into a sql.StmtBuf, from where they'll be processed by a command
// "processor" goroutine (a connExecutor).
// The method returns when the pgwire termination message is received, when
// network communication fails, when the server is draining or when ctx is
// canceled (which also happens when draining (but not from the get-go), and
// when the processor encounters a fatal error).
//
// serveImpl always closes the network connection before returning.
//
// sqlServer is used to create the command processor. As a special facility for
// tests, sqlServer can be nil, in which case the command processor and the
// write-side of the connection will not be created.
func (c *conn) serveImpl(
	ctx context.Context,
	draining func() bool,
	sqlServer *sql.Server,
	reserved mon.BoundAccount,
	authOpt authOptions,
	stopper *stop.Stopper,
	addr string,
	advertiseAddr string,
) {
	defer func() { _ = c.conn.Close() }()

	ctx = logtags.AddTag(ctx, "user", c.sessionArgs.User)

	inTestWithoutSQL := sqlServer == nil
	var authLogger *log.SecondaryLogger
	if !inTestWithoutSQL {
		authLogger = sqlServer.GetExecutorConfig().AuthLogger
		sessionStart := timeutil.Now()
		defer func() {
			if c.authLogEnabled() {
				authLogger.Logf(ctx, "session terminated; duration: %s", timeutil.Now().Sub(sessionStart))
			}
		}()
	}

	// NOTE: We're going to write a few messages to the connection in this method,
	// for the handshake. After that, all writes are done async, in the
	// startWriter() goroutine.

	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn() // This calms the linter that wants these callbacks to always be called.

	var sentDrainSignal bool
	// The net.Conn is switched to a conn that exits if the ctx is canceled.
	c.conn = newReadTimeoutConn(c.conn, func() error {
		// If the context was canceled, it's time to stop reading. Either a
		// higher-level server or the command processor have canceled us.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// If the server is draining, we'll let the processor know by pushing a
		// DrainRequest. This will make the processor quit whenever it finds a good
		// time.
		if !sentDrainSignal && draining() {
			_ /* err */ = c.stmtBuf.Push(ctx, sql.DrainRequest{})
			sentDrainSignal = true
		}
		return nil
	})
	c.rd = *bufio.NewReader(c.conn)

	// the authPipe below logs authentication messages iff its auth
	// logger is non-nil. We define this here.
	var sessionAuthLogger *log.SecondaryLogger
	if !inTestWithoutSQL && c.authLogEnabled() {
		sessionAuthLogger = authLogger
	}

	// We'll build an authPipe to communicate with the authentication process.
	authPipe := newAuthPipe(c, sessionAuthLogger)
	var authenticator authenticatorIO = authPipe

	// procCh is the channel on which we'll receive the termination signal from
	// the command processor.
	var procCh <-chan error

	//var connHandler sql.ConnectionHandler
	if sqlServer != nil {
		// Spawn the command processing goroutine, which also handles connection
		// authentication). It will notify us when it's done through procCh, and
		// we'll also interact with the authentication process through ac.
		var ac AuthConn = authPipe
		procCh = c.processCommandsAsync(ctx, authOpt, ac, sqlServer, reserved, cancelConn, addr, advertiseAddr)
	} else {
		// sqlServer == nil means we are in a local test. In this case
		// we only need the minimum to make pgx happy.
		var err error
		for param, value := range testingStatusReportParams {
			if err := c.sendParamStatus(param, value); err != nil {
				break
			}
		}
		if err != nil {
			reserved.Close(ctx)
			return
		}
		var ac AuthConn = authPipe
		// Simulate auth succeeding.
		ac.AuthOK(fixedIntSizer{size: types.Int})
		dummyCh := make(chan error)
		close(dummyCh)
		procCh = dummyCh
		// An initial readyForQuery message is part of the handshake.
		c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
		c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			reserved.Close(ctx)
			return
		}
	}

	var terminateSeen bool

	// We need an intSizer, which we're ultimately going to get from the
	// authenticator once authentication succeeds (because it will actually be a
	// ConnectionHandler). Until then, we unfortunately still need some intSizer
	// because we technically might enqueue parsed statements in the statement
	// buffer even before authentication succeeds (because we need this go routine
	// to keep reading from the network connection while authentication is in
	// progress in order to react to the connection closing).
	var intSizer unqualifiedIntSizer = fixedIntSizer{size: types.Int}
	var authDone bool

	// update active connections metric when connection is closed.
	defer func() {
		if authDone {
			c.metrics.SucConns.Dec(1)
		}
	}()

Loop:
	for {
		var typ pgwirebase.ClientMessageType
		var n int
		var err error
		typ, n, err = c.readBuf.ReadTypedMsg(&c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			break Loop
		}
		timeReceived := timeutil.Now()
		var ClientEncoding string
		sessionData := intSizer.GetTsSessionData()
		if sessionData == nil {
			ClientEncoding = "UTF8"
		} else if sessionData.ClientEncoding == "" {
			ClientEncoding = "UTF8"
		} else {
			ClientEncoding = sessionData.ClientEncoding
		}

		// In order to make the session effective for both writing in and writing out,
		// it is necessary to write the session into the conn
		// c.sessionArgs.SessionDefaults.Set("client_encoding", ClientEncoding)
		log.VEventf(ctx, 2, "pgwire: processing %s", typ)

		if !authDone {
			if typ == pgwirebase.ClientMsgPassword {
				var pwd []byte
				if pwd, err = c.readBuf.GetBytes(n - 4); err != nil {
					break Loop
				}
				// Pass the data to the authenticator. This hopefully causes it to finish
				// authentication in the background and give us an intSizer when we loop
				// around.
				if err = authenticator.sendPwdData(pwd); err != nil {
					break Loop
				}
				continue
			}
			// Wait for the auth result.
			intSizer, err = authenticator.authResult()
			if err != nil {
				// The error has already been sent to the client.
				break Loop
			} else {
				authDone = true
				c.metrics.SucConns.Inc(1)
			}
		}

		// TODO(jordan): there's one big missing piece of implementation here.
		// In Postgres, if an error is encountered during extended protocol mode,
		// the protocol skips all messages until a Sync is received to "regain
		// protocol synchronization". We don't do this. If this becomes a problem,
		// we should copy their behavior.

		switch typ {
		case pgwirebase.ClientMsgPassword:
			c.ClientEncode(ctx, ClientEncoding)
			// This messages are only acceptable during the auth phase, handled above.
			err = pgwirebase.NewProtocolViolationErrorf("unexpected authentication data")
			_ /* err */ = writeErr(
				ctx, &sqlServer.GetExecutorConfig().Settings.SV, err,
				&c.msgBuilder, &c.writerState.buf)
			break Loop
		case pgwirebase.ClientMsgSimpleQuery:
			c.ClientEncode(ctx, ClientEncoding)
			if err = c.handleSimpleQuery(
				ctx, &c.readBuf, timeReceived, intSizer.GetUnqualifiedIntSize(), intSizer, sqlServer,
			); err != nil {
				break
			}
			err = c.stmtBuf.Push(ctx, sql.Sync{})

		case pgwirebase.ClientMsgExecute:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.handleExecute(ctx, &c.readBuf, timeReceived)
		case pgwirebase.ClientMsgParse:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.handleParse(ctx, &c.readBuf, intSizer.GetUnqualifiedIntSize(), intSizer, sqlServer)

		case pgwirebase.ClientMsgDescribe:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.handleDescribe(ctx, &c.readBuf)

		case pgwirebase.ClientMsgBind:
			err = c.handleBind(ctx, &c.readBuf, ClientEncoding)

		case pgwirebase.ClientMsgClose:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.handleClose(ctx, &c.readBuf)

		case pgwirebase.ClientMsgTerminate:
			c.ClientEncode(ctx, ClientEncoding)
			terminateSeen = true
			break Loop

		case pgwirebase.ClientMsgSync:
			c.ClientEncode(ctx, ClientEncoding)
			// We're starting a batch here. If the client continues using the extended
			// protocol and encounters an error, everything until the next sync
			// message has to be skipped. See:
			// https://www.postgresql.org/docs/current/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

			err = c.stmtBuf.Push(ctx, sql.Sync{})

		case pgwirebase.ClientMsgFlush:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.handleFlush(ctx)

		case pgwirebase.ClientMsgCopyData, pgwirebase.ClientMsgCopyDone, pgwirebase.ClientMsgCopyFail:
			// We're supposed to ignore these messages, per the protocol spec. This
			// state will happen when an error occurs on the server-side during a copy
			// operation: the server will send an error and a ready message back to
			// the client, and must then ignore further copy messages. See:
			// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
			c.ClientEncode(ctx, ClientEncoding)

		default:
			c.ClientEncode(ctx, ClientEncoding)
			err = c.stmtBuf.Push(
				ctx,
				sql.SendError{Err: pgwirebase.NewUnrecognizedMsgTypeErr(typ)})
		}
		if err != nil {
			break Loop
		}
	}

	// We're done reading data from the client, so make the communication
	// goroutine stop. Depending on what that goroutine is currently doing (or
	// blocked on), we cancel and close all the possible channels to make sure we
	// tickle it in the right way.

	// Signal command processing to stop. It might be the case that the processor
	// canceled our context and that's how we got here; in that case, this will
	// be a no-op.
	c.stmtBuf.Close()

	// Cancel the processor's context.
	cancelConn()
	// In case the authenticator is blocked on waiting for data from the client,
	// tell it that there's no more data coming. This is a no-op if authentication
	// was completed already.
	authenticator.noMorePwdData()

	// Wait for the processor goroutine to finish, if it hasn't already. We're
	// ignoring the error we get from it, as we have no use for it. It might be a
	// connection error, or a context cancelation error case this goroutine is the
	// one that triggered the execution to stop.
	<-procCh

	if terminateSeen {
		return
	}
	// If we're draining, let the client know by piling on an AdminShutdownError
	// and flushing the buffer.
	if draining() {
		// TODO(andrei): I think sending this extra error to the client if we also
		// sent another error for the last query (like a context canceled) is a bad
		// idead; see #22630. I think we should find a way to return the
		// AdminShutdown error as the only result of the query.
		_ /* err */ = writeErr(ctx, &sqlServer.GetExecutorConfig().Settings.SV,
			newAdminShutdownErr(ErrDrainingExistingConn), &c.msgBuilder, &c.writerState.buf)
		_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
	}
}

// ClientEncode support GBK,GB18030 TO UTF8 client encoding
func (c *conn) ClientEncode(ctx context.Context, ClientEncoding string) {
	var err error
	c.readBuf.Msg, err = ClientDecoding(ClientEncoding, c.readBuf.Msg)
	if err != nil && log.V(2) {
		log.Infof(ctx, "ClientDecoding failed %v", err)
	}
	return
}

// unqualifiedIntSizer is used by a conn to get the SQL session's current int size
// setting.
//
// It's a restriction on the ConnectionHandler type.
type unqualifiedIntSizer interface {
	// GetUnqualifiedIntSize returns the size that the parser should consider for an
	// unqualified INT.
	GetUnqualifiedIntSize() *types.T

	GetTsinsertdirect() bool

	GetTsSessionData() *sessiondata.SessionData

	GetPreparedStatement(PreparedStatementName string) (*sql.PreparedStatement, bool)

	GetTsSupportBatch() bool

	TsinsertaddActiveQuery(curStmt *sql.Statement) func()
}

type fixedIntSizer struct {
	size *types.T
}

func (f fixedIntSizer) GetUnqualifiedIntSize() *types.T {
	return f.size
}

func (f fixedIntSizer) GetTsinsertdirect() bool {
	return false
}

func (f fixedIntSizer) GetTsSupportBatch() bool {
	return false
}

func (f fixedIntSizer) GetTsSessionData() *sessiondata.SessionData {
	return nil
}

func (f fixedIntSizer) GetPreparedStatement(
	PreparedStatementName string,
) (*sql.PreparedStatement, bool) {
	return nil, false
}

func (f fixedIntSizer) TsinsertaddActiveQuery(curStmt *sql.Statement) func() {
	return func() {}
}

// processCommandsAsync spawns a goroutine that authenticates the connection and
// then processes commands from c.stmtBuf.
//
// It returns a channel that will be signaled when this goroutine is done.
// Whatever error is returned on that channel has already been written to the
// client connection, if applicable.
//
// If authentication fails, this goroutine finishes and, as always, cancelConn
// is called.
//
// Args:
// ac: An interface used by the authentication process to receive password data
//
//	and to ultimately declare the authentication successful.
//
// reserved: Reserved memory. This method takes ownership.
// cancelConn: A function to be called when this goroutine exits. Its goal is to
//
//	cancel the connection's context, thus stopping the connection's goroutine.
//	The returned channel is also closed before this goroutine dies, but the
//	connection's goroutine is not expected to be reading from that channel
//	(instead, it's expected to always be monitoring the network connection).
func (c *conn) processCommandsAsync(
	ctx context.Context,
	authOpt authOptions,
	ac AuthConn,
	sqlServer *sql.Server,
	reserved mon.BoundAccount,
	cancelConn func(),
	addr string,
	advertiseAddr string,
) <-chan error {
	// reservedOwned is true while we own reserved, false when we pass ownership
	// away.
	reservedOwned := true
	retCh := make(chan error, 1)
	go func() {
		var retErr error
		var connHandler sql.ConnectionHandler
		var authOK bool
		var connCloseAuthHandler func()
		defer func() {
			// Release resources, if we still own them.
			if reservedOwned {
				reserved.Close(ctx)
			}
			// Notify the connection's goroutine that we're terminating. The
			// connection might know already, as it might have triggered this
			// goroutine's finish, but it also might be us that we're triggering the
			// connection's death. This context cancelation serves to interrupt a
			// network read on the connection's goroutine.
			cancelConn()

			pgwireKnobs := sqlServer.GetExecutorConfig().PGWireTestingKnobs
			if pgwireKnobs != nil && pgwireKnobs.CatchPanics {
				if r := recover(); r != nil {
					// Catch the panic and return it to the client as an error.
					if err, ok := r.(error); ok {
						// Mask the cause but keep the details.
						retErr = errors.Handled(err)
					} else {
						retErr = errors.Newf("%+v", r)
					}
					retErr = pgerror.WithCandidateCode(retErr, pgcode.CrashShutdown)
					// Add a prefix. This also adds a stack trace.
					retErr = errors.Wrap(retErr, "caught fatal error")
					_ = writeErr(
						ctx, &sqlServer.GetExecutorConfig().Settings.SV, retErr,
						&c.msgBuilder, &c.writerState.buf)
					_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
					c.stmtBuf.Close()
					// Send a ready for query to make sure the client can react.
					// TODO(andrei, jordan): Why are we sending this exactly?
					c.bufferReadyForQuery('I')
				}
			}
			if !authOK {
				ac.AuthFail(retErr)
			}
			if connCloseAuthHandler != nil {
				connCloseAuthHandler()
			}
			// Inform the connection goroutine of success or failure.
			retCh <- retErr
		}()

		// check user sql connection
		if retErr = c.checkConnectionLimit(ctx, authOpt.ie, c.sessionArgs.User, sqlServer.GetExecutorConfig()); retErr != nil {
			return
		}

		// Authenticate the connection.
		if connCloseAuthHandler, retErr = c.handleAuthentication(
			ctx, ac, authOpt, sqlServer.GetExecutorConfig(),
		); retErr != nil {
			// Auth failed or some other error.
			return
		}

		sendError := func(err error) error {
			_ /* err */ = writeErr(ctx, &sqlServer.GetExecutorConfig().Settings.SV, err, &c.msgBuilder, c.conn)
			return err
		}

		var err error

		// Inform the client of the default session settings.
		connHandler, err = c.sendInitialConnData(ctx, sqlServer)
		if err != nil {
			retErr = sendError(err)
			return
		}

		// Signal the connection was established to the authenticator.
		ac.AuthOK(connHandler)
		// Mark the authentication as succeeded in case a panic
		// is thrown below and we need to report to the client
		// using the defer above.
		authOK = true

		// Now actually process commands.
		reservedOwned = false // We're about to pass ownership away.
		retErr = sqlServer.ServeConn(ctx, connHandler, reserved, cancelConn)
	}()
	return retCh
}

func (c *conn) sendParamStatus(param, value string) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterStatus)
	c.msgBuilder.writeTerminatedString(param)
	c.msgBuilder.writeTerminatedString(value)
	return c.msgBuilder.finishMsg(c.conn)
}

func (c *conn) bufferParamStatus(param, value string) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterStatus)
	c.msgBuilder.writeTerminatedString(param)
	c.msgBuilder.writeTerminatedString(value)
	return c.msgBuilder.finishMsg(&c.writerState.buf)
}

func (c *conn) bufferNotice(ctx context.Context, noticeErr error) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoticeResponse)
	c.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	c.msgBuilder.writeTerminatedString("NOTICE")
	return writeErrFields(ctx, c.sv, noticeErr, &c.msgBuilder, &c.writerState.buf)
}

func (c *conn) bufferWarning(ctx context.Context, warning error) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoticeResponse)
	c.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	c.msgBuilder.writeTerminatedString("WARNING")
	return writeErrFields(ctx, c.sv, warning, &c.msgBuilder, &c.writerState.buf)
}

func (c *conn) sendInitialConnData(
	ctx context.Context, sqlServer *sql.Server,
) (sql.ConnectionHandler, error) {
	connHandler, err := sqlServer.SetupConn(
		ctx, c.sessionArgs, &c.stmtBuf, c, c.metrics.SQLMemMetrics)
	if err != nil {
		_ /* err */ = writeErr(
			ctx, &sqlServer.GetExecutorConfig().Settings.SV, err, &c.msgBuilder, c.conn)
		return sql.ConnectionHandler{}, err
	}

	// Send the initial "status parameters" to the client.  This
	// overlaps partially with session variables. The client wants to
	// see the values that result from the combination of server-side
	// defaults with client-provided values.
	// For details see: https://www.postgresql.org/docs/10/static/libpq-status.html
	for _, param := range statusReportParams {
		param := param
		value := connHandler.GetParamStatus(ctx, param)
		if err := c.sendParamStatus(param, value); err != nil {
			return sql.ConnectionHandler{}, err
		}
	}
	// The two following status parameters have no equivalent session
	// variable.
	if err := c.sendParamStatus("session_authorization", c.sessionArgs.User); err != nil {
		return sql.ConnectionHandler{}, err
	}

	// TODO(knz): this should retrieve the admin status during
	// authentication using the roles table, instead of using a
	// simple/naive username match.
	isSuperUser := c.sessionArgs.User == security.RootUser
	superUserVal := "off"
	if isSuperUser {
		superUserVal = "on"
	}
	if err := c.sendParamStatus("is_superuser", superUserVal); err != nil {
		return sql.ConnectionHandler{}, err
	}

	// An initial readyForQuery message is part of the handshake.
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
	if err := c.msgBuilder.finishMsg(c.conn); err != nil {
		return sql.ConnectionHandler{}, err
	}
	return connHandler, nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleSimpleQuery(
	ctx context.Context,
	buf *pgwirebase.ReadBuffer,
	timeReceived time.Time,
	unqualifiedIntSize *types.T,
	unqis unqualifiedIntSizer,
	server *sql.Server,
) error {
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	tracing.AnnotateTrace()

	c.parser.IsShortcircuit = unqis.GetTsinsertdirect()
	var dit sql.DirectInsertTable
	var di sql.DirectInsert
	di.DirectTimes[sql.HandleSimpleStart] = timeutil.Now()
	c.parser.Dudgetstable = func(dbName *string, tableName string) bool {
		if *dbName == "public" || *dbName == "" {
			*dbName = unqis.GetTsSessionData().Database
		}

		if *dbName == "defaultdb" {
			return false
		}
		var err error
		var isTsTable bool
		isTsTable, dit, err = server.GetCFG().InternalExecutor.IsTsTable(ctx, *dbName, tableName, c.sessionArgs.User)
		return err == nil && isTsTable
	}

	startParse := timeutil.Now()
	stmts, err := c.parser.ParseWithInt(query, unqualifiedIntSize, c.sv)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	if len(stmts) == 0 {
		return c.stmtBuf.Push(
			ctx, sql.ExecStmt{
				Statement:    parser.Statement{},
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			})
	}

	if c.parser.GetIsTsTable() {
		if len(stmts) == 1 {
			di.DirectTimes[sql.OtherStart] = timeutil.Now()
			di.DirectTimes[sql.ParseStart], di.DirectTimes[sql.ParseEnd] = startParse, endParse
			// Only a single INSERT statement is processed
			ins, ok := stmts[0].AST.(*tree.Insert)
			if !ok {
				goto normalExec
			}

			// Check whether the inserted value and the number of columns match.
			if matchCnt := sql.NumofInsertDirect(ins, &dit.ColsDesc, stmts, &di); matchCnt != di.RowNum*di.ColNum || c.parser.Customize {
				c.parser.IsShortcircuit = false
				if stmts, err = c.parser.ParseWithInt(query, unqualifiedIntSize, c.sv); err != nil {
					return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
				}
				goto normalExec
			}

			cfg := server.GetCFG()
			// Set batch error flag
			stmts[0].Insertdirectstmt.IgnoreBatcherror = unqis.GetTsSupportBatch() && di.RowNum > 1

			// Execute in a transaction
			if err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				curStmt := sql.Statement{Statement: stmts[0]}
				unregisterFn := unqis.TsinsertaddActiveQuery(&curStmt)
				defer func() {
					unregisterFn()
				}()

				// Set execution context
				evalCtx := getEvalContext(ctx, txn, server)
				// Get table collection and version
				tables := sql.GetTableCollection(cfg, cfg.InternalExecutor)

				if ins.IsNoSchema {
					ins.Columns = ins.NoSchemaColumns.GetNameList()
					add, err := maybeAddNonExistsColumn(ctx, cfg.InternalExecutor, &dit, ins)
					if err != nil {
						return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
					}
					if add {
						for r := retry.Start(retryOpt); r.Next(); {
							allGet := true
							clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
							evalCtx.Txn.SetFixedTimestamp(ctx, clock.Now())
							table, err := tables.GetTableVersion(ctx, txn, dit.Tname, tree.ObjectLookupFlags{})
							for idx := range ins.NoSchemaColumns {
								colDef := ins.NoSchemaColumns[idx]
								_, err = sql.GetTSColumnByName(colDef.Name, table.TableDesc().Columns)
								if err == nil {
									continue
								} else {
									allGet = false
									break
								}
							}
							if allGet {
								break
							} else {
								tables.ReleaseTSTables(ctx)
							}
						}
					}
				}

				defer tables.ReleaseTSTables(ctx)
				table, err := tables.GetTableVersion(ctx, txn, dit.Tname, tree.ObjectLookupFlags{})
				if err != nil || table == nil {
					return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
				}

				evalCtx.StartSinglenode = (server.GetCFG().StartMode == sql.StartSingleNode)
				dit.DbID, dit.TabID = uint32(table.TableDescriptor.ParentID), uint32(table.TableDescriptor.ID)
				if table.TsTable.HashNum == 0 {
					table.TsTable.HashNum = api.HashParamV2
				}
				dit.HashNum = table.TsTable.HashNum
				dit.ColsDesc = table.TableDesc().Columns
				// Get column information
				if err = sql.GetColsInfo(ctx, evalCtx, &dit.ColsDesc, ins, &di, &stmts[0], cfg.TsIDGen); err != nil {
					return err
				}

				di.PArgs.TSVersion = uint32(table.TsTable.TsVersion)

				// Set time zone context
				var ptCtx tree.ParseTimeContext
				if con, ok := unqis.(sql.ConnectionHandler); ok {
					ptCtx = tree.NewParseTimeContext(con.GetTsZone())
				}

				r := c.allocCommandResult()
				*r = commandResult{conn: c, typ: commandComplete}

				err = sql.GetPayloadMapForMuiltNode(
					ctx, ptCtx, dit, &di, stmts, evalCtx, table, cfg)

				if err != nil {
					return err
				}

				// Processing audit logs
				if err = handleInsertDirectAuditLog(ctx, server, startParse, stmts, unqis, dit.TabID, query); err != nil {
					return err
				}
				di.DirectTimes[sql.OtherEnd] = timeutil.Now()

				stmts[0].Insertdirectstmt.InsertValues = nil
				stmts[0].Insertdirectstmt.ValuesType = nil

				// Send insert_direct information
				if err = Send(ctx, unqis, evalCtx, r, stmts, di, c, timeReceived, startParse, endParse, curStmt); err != nil {
					return err
				}
				if r.Err() != nil {
					return r.err
				}
				return nil
			}); err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}

			return nil
		}
		// Non-single INSERT statement, reparse
		c.parser.IsShortcircuit = false
		if stmts, err = c.parser.ParseWithInt(query, unqualifiedIntSize, c.sv); err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}

	}

normalExec:

	for i := range stmts {
		// The CopyFrom statement is special. We need to detect it so we can hand
		// control of the connection, through the stmtBuf, to a copyMachine, and
		// block this network routine until control is passed back.
		if cp, ok := stmts[i].AST.(*tree.CopyFrom); ok {
			if len(stmts) != 1 {
				// NOTE(andrei): I don't know if Postgres supports receiving a COPY
				// together with other statements in the "simple" protocol, but I'd
				// rather not worry about it since execution of COPY is special - it
				// takes control over the connection.
				return c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"COPY together with other statements in a query string is not supported"),
					})
			}
			copyDone := sync.WaitGroup{}
			copyDone.Add(1)
			if err := c.stmtBuf.Push(ctx, sql.CopyIn{Conn: c, Stmt: cp, CopyDone: &copyDone}); err != nil {
				return err
			}
			copyDone.Wait()
			return nil
		}

		if err := c.stmtBuf.Push(
			ctx,
			sql.ExecStmt{
				Statement:    stmts[i],
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			}); err != nil {
			return err
		}
	}
	return nil
}

var retryOpt = retry.Options{
	InitialBackoff: 20 * time.Millisecond,
	MaxBackoff:     1 * time.Second,
	Multiplier:     2,
	MaxRetries:     60, // about 5 minutes
}

// maybeAddNonExistsColumn can automatically add columns that do not exist.
func maybeAddNonExistsColumn(
	ctx context.Context, ie *sql.InternalExecutor, dit *sql.DirectInsertTable, ins *tree.Insert,
) (bool, error) {
	addStmts, err := constructAutoAddStmts(dit.ColsDesc, ins, *dit.Tname, dit.TableType)
	if err != nil {
		return false, err
	}

	for i := range addStmts {
		// Retry while execution returns error.
		for r := retry.Start(retryOpt); r.Next(); {
			_, err := ie.Query(ctx, "auto add column", nil, addStmts[i])
			if err != nil {
				if optbuilder.IsInsertNoSchemaRetryableError(err) {
					log.Warningf(ctx, "auto alter add failed: %s, err: %s\n", addStmts[i], err.Error())
				} else if strings.Contains(err.Error(), "schema version") {
					return false, err
				} else {
					break
				}
			} else {
				log.Infof(ctx, "auto alter add success: %s\n", addStmts[i])
				break
			}
		}
	}
	return len(addStmts) != 0, nil
}

func createColumnMap(cols []sqlbase.ColumnDescriptor) map[string]*sqlbase.ColumnDescriptor {
	columnMap := make(map[string]*sqlbase.ColumnDescriptor, len(cols))
	for i := range cols {
		columnMap[cols[i].Name] = &cols[i]
	}
	return columnMap
}

const alterStmt = `ALTER TABLE %s ADD %s %s %s`
const alterColumnTypeStmt = `ALTER TABLE %s ALTER COLUMN %s TYPE %s`
const alterTagTypeStmt = `ALTER TABLE %s ALTER TAG %s TYPE %s`

// constructAutoAddStmts checks whether columns are exist and generate alter table stmt if necessary
func constructAutoAddStmts(
	cols []sqlbase.ColumnDescriptor,
	ins *tree.Insert,
	tbName tree.TableName,
	tableType tree.TableType,
) ([]string, error) {
	var addStmts []string
	columnMap := createColumnMap(cols)
	inColName := make(map[tree.Name]bool, len(ins.NoSchemaColumns))

	isInstanceOrTemplateTable := tableType == tree.InstanceTable || tableType == tree.TemplateTable
	// constructs the map of the metadata column to the input value based on the user-specified column name
	for idx := range ins.NoSchemaColumns {
		colDef := &ins.NoSchemaColumns[idx]
		typ := `COLUMN`
		if colDef.IsTag {
			typ = `TAG`
		}
		targetCol, exists := columnMap[string(colDef.Name)]
		if !exists {
			if _, ok := inColName[colDef.Name]; !ok {
				inColName[colDef.Name] = true
			} else {
				return []string{}, pgerror.Newf(pgcode.DuplicateColumn, "multiple assignments to the same column \"%s\"", string(colDef.Name))
			}
			// If the column does not exist, generate a ALTER ... ADD ... statement.
			addStmts = append(addStmts, fmt.Sprintf(alterStmt, tbName.String(), typ, colDef.Name.String(), colDef.StrType))
		} else {
			if targetCol.IsTagCol() != colDef.IsTag {
				return []string{}, pgerror.Newf(pgcode.DuplicateColumn, "duplicate %s name: %q", typ, colDef.Name)
			}
			if len(colDef.TypeLen) > 0 {
				num, err := strconv.Atoi(colDef.TypeLen)
				if err != nil {
					return []string{}, err
				}
				if targetCol.Type.Width() < int32(num) && targetCol.Type.Family() != types.TimestampTZFamily {
					if colDef.IsTag {
						addStmts = append(addStmts, fmt.Sprintf(alterTagTypeStmt, tbName.String(), colDef.Name.String(), colDef.StrType+"("+colDef.TypeLen+")"))
					} else {
						addStmts = append(addStmts, fmt.Sprintf(alterColumnTypeStmt, tbName.String(), colDef.Name.String(), colDef.StrType+"("+colDef.TypeLen+")"))
					}
				}
			}
		}

		// instance table does not support specifying tag column
		if targetCol != nil && targetCol.IsTagCol() && isInstanceOrTemplateTable {
			return []string{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot insert tag column: \"%s\" for INSTANCE table", targetCol.Name)
		}
	}
	return addStmts, nil
}

func handleInsertDirectAuditLog(
	ctx context.Context,
	server *sql.Server,
	startParse time.Time,
	stmts parser.Statements,
	unqis unqualifiedIntSizer,
	tabID uint32,
	query string,
) error {
	isAuditLog := false
	if server != nil {
		isAuditLog = setting.AuditEnabled.Get(&server.GetCFG().Settings.SV) && setting.AuditLogEnabled.Get(&server.GetCFG().Settings.SV)
	}
	if isAuditLog {
		version1, err := server.GetCFG().InternalExecutor.GetAudiLogVesion(ctx)
		if err != nil {
			return err
		}
		err = shortInsertLogAudit(ctx, 0, nil, startParse, server, &stmts[0], unqis.GetTsSessionData(), tabID, version1, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func getEvalContext(ctx context.Context, txn *kv.Txn, server *sql.Server) tree.EvalContext {
	var sd sessiondata.SessionData
	EvalContext := tree.EvalContext{
		Context:          ctx,
		Txn:              txn,
		SessionData:      &sd,
		InternalExecutor: server.GetCFG().InternalExecutor,
		Settings:         server.GetCFG().Settings,
		NodeID:           server.GetCFG().NodeID.Get(),
	}
	return EvalContext
}

// Send insert_direct information
func Send(
	ctx context.Context,
	unqis unqualifiedIntSizer,
	EvalContext tree.EvalContext,
	r *commandResult,
	stmts parser.Statements,
	di sql.DirectInsert,
	c *conn,
	timeReceived, startParse, endParse time.Time,
	curStmt sql.Statement,
) error {
	con, ok := unqis.(sql.ConnectionHandler)
	if !ok {
		return nil
	}

	insertStmt := &stmts[0].Insertdirectstmt

	failedRows := insertStmt.BatchFailed
	rowNum := di.RowNum

	if rowNum == failedRows || rowNum == 0 {
		return errors.Errorf("All BatchInsert Error.Check logs under the user data directory for more information.")
	}

	insertStmt.InsertFast = true
	insertStmt.RowsAffected = int64(rowNum - failedRows)

	var err error
	if insertStmt.UseDeepRule, insertStmt.DedupRule, insertStmt.DedupRows, err =
		con.SendDTI(EvalContext.Context, &EvalContext, r, di, stmts, curStmt); err != nil {
		return err
	}

	return c.stmtBuf.Push(ctx, sql.ExecStmt{
		Statement:    stmts[0],
		TimeReceived: timeReceived,
		ParseStart:   startParse,
		ParseEnd:     endParse,
	})
}

func shortInsertLogAudit(
	ctx context.Context,
	rows int,
	err error,
	startTime time.Time,
	svr *sql.Server,
	stmt *parser.Statement,
	sessionData *sessiondata.SessionData,
	tableID uint32,
	version uint32,
	query string,
) error {
	var auditTxn *kv.Txn
	if svr != nil {
		auditTxn = svr.GetCFG().DB.NewTxn(ctx, "audit txt")
		if svr.GetCFG().AuditServer.GetHandler().GetTableVersion() != version {
			svr.GetCFG().AuditServer.GetHandler().SetTableVersion(version)
		}
	}

	// Now log!

	if sessionData != nil && stmt != nil {
		auditInfo := server.MakeAuditInfo(startTime, sessionData.User, sessionData.Roles,
			target.OperationType(stmt.AST.StatOp()), target.ObjectTable, 1, sessionData.RemoteAddr, err)
		auditInfo.SetTarget(tableID, "", nil)
		auditInfo.SetCommand(query)
		if svr != nil {
			if err := svr.GetCFG().AuditServer.LogAudit(ctx, auditTxn, &auditInfo); err != nil {
				return err
			}
		}

	}
	return nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleParse(
	ctx context.Context,
	buf *pgwirebase.ReadBuffer,
	nakedIntSize *types.T,
	unqis unqualifiedIntSizer,
	server *sql.Server,
) error {
	name, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	// The client may provide type information for (some of) the placeholders.
	numQArgTypes, err := buf.GetUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := buf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		inTypeHints[i] = oid.Oid(typ)
	}

	c.parser.IsShortcircuit = unqis.GetTsinsertdirect()
	var dit sql.DirectInsertTable
	c.parser.Dudgetstable = func(dbName *string, tableName string) bool {
		user := c.sessionArgs.User
		if *dbName == "public" || *dbName == "" {
			*dbName = unqis.GetTsSessionData().Database
		}
		var isTsTable bool
		var insertErr error
		isTsTable, dit, insertErr = server.GetCFG().InternalExecutor.IsTsTable(ctx, *dbName, tableName, user)
		if insertErr != nil {
			return false
		}
		return isTsTable
	}
	c.parser.SetPrepareMode(true)
	startParse := timeutil.Now()
Reparse:
	stmts, err := c.parser.ParseWithInt(query, nakedIntSize, c.sv)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	if len(stmts) > 1 {
		err := pgerror.WrongNumberOfPreparedStatements(len(stmts))
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var stmt parser.Statement
	if len(stmts) == 1 {
		stmt = stmts[0]
	}
	// len(stmts) == 0 results in a nil (empty) statement.
	stmt.Insertdirectstmt.InsertFast = c.parser.GetIsTsTable()
	if stmt.Insertdirectstmt.InsertFast && (stmt.NumPlaceholders == 0 || len(stmt.Insertdirectstmt.InsertValues) > 0) {
		c.parser.IsShortcircuit = false
		goto Reparse
	}
	if len(inTypeHints) > stmt.NumPlaceholders {
		err := pgwirebase.NewProtocolViolationErrorf(
			"received too many type hints: %d vs %d placeholders in query",
			len(inTypeHints), stmt.NumPlaceholders,
		)
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	var sqlTypeHints tree.PlaceholderTypes
	if len(inTypeHints) > 0 {
		// Prepare the mapping of SQL placeholder names to types. Pre-populate it with
		// the type hints received from the client, if any.
		sqlTypeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
		for i, t := range inTypeHints {
			if t == 0 {
				continue
			}
			v, ok := types.OidToType[t]
			if !ok {
				err := pgwirebase.NewProtocolViolationErrorf("unknown oid type: %v", t)
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			sqlTypeHints[i] = v
		}
	}

	endParse := timeutil.Now()

	if _, ok := stmt.AST.(*tree.CopyFrom); ok {
		// We don't support COPY in extended protocol because it'd be complicated:
		// it wouldn't be the preparing, but the execution that would need to
		// execute the copyMachine.
		// Also note that COPY FROM in extended mode seems to be quite broken in
		// Postgres too:
		// https://www.postgresql.org/message-id/flat/CAMsr%2BYGvp2wRx9pPSxaKFdaObxX8DzWse%2BOkWk2xpXSvT0rq-g%40mail.gmail.com#CAMsr+YGvp2wRx9pPSxaKFdaObxX8DzWse+OkWk2xpXSvT0rq-g@mail.gmail.com
		return c.stmtBuf.Push(ctx, sql.SendError{Err: fmt.Errorf("CopyFrom not supported in extended protocol mode")})
	}

	var insclosnum int
	if stmt.Insertdirectstmt.InsertFast {
		if v, ok := stmt.AST.(*tree.Insert); ok {
			dit.Desc = make(tree.NameList, len(v.Columns))
			if v.Columns != nil {
				copy(dit.Desc, v.Columns)
			}
			if len(dit.Desc) == 0 {
				insclosnum = len(dit.ColsDesc)
			} else {
				insclosnum = len(dit.Desc)
			}
		}
	}

	return c.stmtBuf.Push(
		ctx,
		sql.PrepareStmt{
			Name:         name,
			Statement:    stmt,
			TypeHints:    sqlTypeHints,
			RawTypeHints: inTypeHints,
			ParseStart:   startParse,
			ParseEnd:     endParse,
			PrepareInsertDirect: sql.PrepareInsertDirect{
				Dit:        dit,
				Inscolsnum: insclosnum,
			},
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleDescribe(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DescribeStmt{
			Name: name,
			Type: typ,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleClose(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DeletePreparedStmt{
			Name: name,
			Type: typ,
		})
}

// If no format codes are provided then all arguments/result-columns use
// the default format, text.
var formatCodesAllText = []pgwirebase.FormatCode{pgwirebase.FormatText}

// handleBind queues instructions for creating a portal from a prepared
// statement.
// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleBind(
	ctx context.Context, buf *pgwirebase.ReadBuffer, ClientEncoding string,
) error {
	portalName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	statementName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var qArgFormatCodes []pgwirebase.FormatCode
	switch numQArgFormatCodes {
	case 0:
		// No format codes means all arguments are passed as text.
		qArgFormatCodes = formatCodesAllText
	case 1:
		// `1` means read one code and apply it to every argument.
		ch, err := buf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		code := pgwirebase.FormatCode(ch)
		if code == pgwirebase.FormatText {
			qArgFormatCodes = formatCodesAllText
		} else {
			qArgFormatCodes = []pgwirebase.FormatCode{code}
		}
	default:
		qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgFormatCodes)
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			ch, err := buf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			qArgFormatCodes[i] = pgwirebase.FormatCode(ch)
		}
	}

	numValues, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	qargs := make([][]byte, numValues)
	for i := 0; i < int(numValues); i++ {
		plen, err := buf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[i] = nil
			continue
		}
		b, err := buf.GetBytes(int(plen))
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		b, err = ClientDecoding(ClientEncoding, b)
		if err != nil && log.V(2) {
			log.Infof(ctx, "ClientDecoding failed %v", err)
		}
		qargs[i] = b
	}

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var columnFormatCodes []pgwirebase.FormatCode
	switch numColumnFormatCodes {
	case 0:
		// All columns will use the text format.
		columnFormatCodes = formatCodesAllText
	case 1:
		// All columns will use the one specified format.
		ch, err := buf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		code := pgwirebase.FormatCode(ch)
		if code == pgwirebase.FormatText {
			columnFormatCodes = formatCodesAllText
		} else {
			columnFormatCodes = []pgwirebase.FormatCode{code}
		}
	default:
		columnFormatCodes = make([]pgwirebase.FormatCode, numColumnFormatCodes)
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			ch, err := buf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			columnFormatCodes[i] = pgwirebase.FormatCode(ch)
		}
	}
	return c.stmtBuf.Push(
		ctx,
		sql.BindStmt{
			PreparedStatementName: statementName,
			PortalName:            portalName,
			Args:                  qargs,
			ArgFormatCodes:        qArgFormatCodes,
			OutFormats:            columnFormatCodes,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleExecute(
	ctx context.Context, buf *pgwirebase.ReadBuffer, timeReceived time.Time,
) error {
	portalName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	limit, err := buf.GetUint32()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(ctx, sql.ExecPortal{
		Name:         portalName,
		TimeReceived: timeReceived,
		Limit:        int(limit),
	})
}

func (c *conn) handleFlush(ctx context.Context) error {
	return c.stmtBuf.Push(ctx, sql.Flush{})
}

// BeginCopyIn is part of the pgwirebase.Conn interface.
func (c *conn) BeginCopyIn(ctx context.Context, columns []sqlbase.ResultColumn) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyInResponse)
	c.msgBuilder.writeByte(byte(pgwirebase.FormatText))
	c.msgBuilder.putInt16(int16(len(columns)))
	for range columns {
		c.msgBuilder.putInt16(int16(pgwirebase.FormatText))
	}
	return c.msgBuilder.finishMsg(c.conn)
}

// SendCommandComplete is part of the pgwirebase.Conn interface.
func (c *conn) SendCommandComplete(tag []byte) error {
	c.bufferCommandComplete(tag)
	return nil
}

// Rd is part of the pgwirebase.Conn interface.
func (c *conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader{conn: c}
}

// flushInfo encapsulates information about what results have been flushed to
// the network.
type flushInfo struct {
	// buf is a reference to writerState.buf.
	buf *bytes.Buffer
	// lastFlushed indicates the highest command for which results have been
	// flushed. The command may have further results in the buffer that haven't
	// been flushed.
	lastFlushed sql.CmdPos
	// map from CmdPos to the index of the buffer where the results for the
	// respective result begins.
	cmdStarts map[sql.CmdPos]int
}

// registerCmd updates cmdStarts when the first result for a new command is
// received.
func (fi *flushInfo) registerCmd(pos sql.CmdPos) {
	if _, ok := fi.cmdStarts[pos]; ok {
		return
	}
	fi.cmdStarts[pos] = fi.buf.Len()
}

func cookTag(tagStr string, buf []byte, stmtType tree.StatementType, rowsAffected int) []byte {
	if tagStr == "INSERT" {
		// From the postgres docs (49.5. Message Formats):
		// `INSERT oid rows`... oid is the object ID of the inserted row if
		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
		tagStr = "INSERT 0"
	}
	tag := append(buf, tagStr...)

	switch stmtType {
	case tree.RowsAffected:
		tag = append(tag, ' ')
		tag = strconv.AppendInt(tag, int64(rowsAffected), 10)

	case tree.Rows:
		tag = append(tag, ' ')
		tag = strconv.AppendUint(tag, uint64(rowsAffected), 10)

	case tree.Ack, tree.DDL:
		if tagStr == "SELECT" {
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(rowsAffected), 10)
		}

	case tree.CopyIn:
		// Nothing to do. The CommandComplete message has been sent elsewhere.
		panic(fmt.Sprintf("CopyIn statements should have been handled elsewhere " +
			"and not produce results"))
	default:
		panic(fmt.Sprintf("unexpected result type %v", stmtType))
	}

	return tag
}

// bufferRow serializes a row and adds it to the buffer.
//
// formatCodes describes the desired encoding for each column. It can be nil, in
// which case all columns are encoded using the text encoding. Otherwise, it
// needs to contain an entry for every column.
func (c *conn) bufferRow(
	ctx context.Context,
	row tree.Datums,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
	oids []oid.Oid,
) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
	c.msgBuilder.putInt16(int16(len(row)))
	count := len(formatCodes)
	for i, col := range row {
		fmtCode := pgwirebase.FormatText
		if formatCodes != nil {
			if i < count {
				fmtCode = formatCodes[i]
			} else {
				// default format code
				if count == 1 {
					fmtCode = formatCodes[0]
				} else {
					c.msgBuilder.setError(errors.Errorf("format code count %d less than result count %d", count, len(row)))
				}
			}
		}
		switch fmtCode {
		case pgwirebase.FormatText:
			c.msgBuilder.writeTextDatumWithOid(ctx, col, conv, oids[i])
		case pgwirebase.FormatBinary:
			c.msgBuilder.writeBinaryDatum(ctx, col, conv.Location, oids[i])
		default:
			c.msgBuilder.setError(errors.Errorf("unsupported format code %s", fmtCode))
		}
	}

	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferReadyForQuery(txnStatus byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(txnStatus)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferParseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferBindComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgBindComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferCloseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCloseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferCommandComplete(tag []byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCommandComplete)
	c.msgBuilder.write(tag)
	c.msgBuilder.nullTerminate()
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferPortalSuspended() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgPortalSuspended)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferErr(ctx context.Context, err error) {
	if err := writeErr(ctx, c.sv,
		err, &c.msgBuilder, &c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferEmptyQueryResponse() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgEmptyQuery)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func writeErr(
	ctx context.Context, sv *settings.Values, err error, msgBuilder *writeBuffer, w io.Writer,
) error {
	// Record telemetry for the error.
	sqltelemetry.RecordError(ctx, err, sv)
	msgBuilder.initMsg(pgwirebase.ServerMsgErrorResponse)
	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	msgBuilder.writeTerminatedString("ERROR")
	return writeErrFields(ctx, sv, err, msgBuilder, w)
}

func writeErrFields(
	ctx context.Context, sv *settings.Values, err error, msgBuilder *writeBuffer, w io.Writer,
) error {
	// Now send the error to the client.
	pgErr := pgerror.Flatten(err)

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSQLState)
	msgBuilder.writeTerminatedString(pgErr.Code)

	if pgErr.Detail != "" {
		msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFileldDetail)
		msgBuilder.writeTerminatedString(pgErr.Detail)
	}

	if pgErr.Hint != "" {
		msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFileldHint)
		msgBuilder.writeTerminatedString(pgErr.Hint)
	}

	if pgErr.Source != nil {
		errCtx := pgErr.Source
		if errCtx.File != "" {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFile)
			msgBuilder.writeTerminatedString(errCtx.File)
		}

		if errCtx.Line > 0 {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcLine)
			msgBuilder.writeTerminatedString(strconv.Itoa(int(errCtx.Line)))
		}

		if errCtx.Function != "" {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFunction)
			msgBuilder.writeTerminatedString(errCtx.Function)
		}
	}

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
	msgBuilder.writeTerminatedString(pgErr.Message)

	msgBuilder.nullTerminate()
	return msgBuilder.finishMsg(w)
}

func (c *conn) bufferParamDesc(types []oid.Oid) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterDescription)
	c.msgBuilder.putInt16(int16(len(types)))
	for _, t := range types {
		c.msgBuilder.putInt32(int32(t))
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferNoDataMsg() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoData)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

// writeRowDescription writes a row description to the given writer.
//
// formatCodes specifies the format for each column. It can be nil, in which
// case all columns will use FormatText.
//
// If an error is returned, it has also been saved on c.err.
func (c *conn) writeRowDescription(
	ctx context.Context,
	columns []sqlbase.ResultColumn,
	formatCodes []pgwirebase.FormatCode,
	w io.Writer,
) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgRowDescription)
	c.msgBuilder.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(ctx, "pgwire: writing column %s of type: %s", column.Name, column.Typ)
		}
		clientEncoding := c.res.conv.ClientEncoding
		tmpbuf, err := ClientEncoding(clientEncoding, []byte(column.Name))
		if err != nil {
			c.setErr(err)
			return err
		}
		c.msgBuilder.writeTerminatedString(string(tmpbuf))
		typ := pgTypeForParserType(column.Typ)
		c.msgBuilder.putInt32(int32(column.TableID))        // Table OID (optional).
		c.msgBuilder.putInt16(int16(column.PGAttributeNum)) // Column attribute ID (optional).
		c.msgBuilder.putInt32(int32(typ.oid))
		c.msgBuilder.putInt16(int16(typ.size))
		c.msgBuilder.putInt32(column.GetTypeModifier()) // Type modifier
		if formatCodes == nil {
			c.msgBuilder.putInt16(int16(pgwirebase.FormatText))
		} else {
			c.msgBuilder.putInt16(int16(formatCodes[i]))
		}
	}
	if err := c.msgBuilder.finishMsg(w); err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// Flush is part of the ClientComm interface.
//
// In case conn.err is set, this is a no-op - the previous err is returned.
func (c *conn) Flush(pos sql.CmdPos) error {
	// Check that there were no previous network errors. If there were, we'd
	// probably also fail the write below, but this check is here to make
	// absolutely sure that we don't send some results after we previously had
	// failed to send others.
	if err := c.GetErr(); err != nil {
		return err
	}

	c.writerState.fi.lastFlushed = pos
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)

	_ /* n */, err := c.writerState.buf.WriteTo(c.conn)
	if err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// maybeFlush flushes the buffer to the network connection if it exceeded
// sessionArgs.ConnResultsBufferSize.
func (c *conn) maybeFlush(pos sql.CmdPos) (bool, error) {
	if int64(c.writerState.buf.Len()) <= c.sessionArgs.ConnResultsBufferSize {
		return false, nil
	}
	return true, c.Flush(pos)
}

// LockCommunication is part of the ClientComm interface.
//
// The current implementation of conn writes results to the network
// synchronously, as they are produced (modulo buffering). Therefore, there's
// nothing to "lock" - communication is naturally blocked as the command
// processor won't write any more results.
func (c *conn) LockCommunication() sql.ClientLock {
	return &clientConnLock{flushInfo: &c.writerState.fi}
}

// clientConnLock is the connection's implementation of sql.ClientLock. It lets
// the sql module lock the flushing of results and find out what has already
// been flushed.
type clientConnLock struct {
	*flushInfo
}

var _ sql.ClientLock = &clientConnLock{}

// Close is part of the sql.ClientLock interface.
func (cl *clientConnLock) Close() {
	// Nothing to do. See LockCommunication note.
}

// ClientPos is part of the sql.ClientLock interface.
func (cl *clientConnLock) ClientPos() sql.CmdPos {
	return cl.lastFlushed
}

// RTrim is part of the sql.ClientLock interface.
func (cl *clientConnLock) RTrim(ctx context.Context, pos sql.CmdPos) {
	if pos <= cl.lastFlushed {
		panic(fmt.Sprintf("asked to trim to pos: %d, below the last flush: %d", pos, cl.lastFlushed))
	}
	idx, ok := cl.cmdStarts[pos]
	if !ok {
		// If we don't have a start index for pos yet, it must be that no results
		// for it yet have been produced yet.
		idx = cl.buf.Len()
	}
	// Remove everything from the buffer after idx.
	cl.buf.Truncate(idx)
	// Update cmdStarts: delete commands that were trimmed.
	for p := range cl.cmdStarts {
		if p >= pos {
			delete(cl.cmdStarts, p)
		}
	}
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
	return c.newCommandResult(descOpt, pos, stmt, formatCodes, conv, limit, portalName, implicitTxn)
}

// CreateSyncResult is part of the sql.ClientComm interface.
func (c *conn) CreateSyncResult(pos sql.CmdPos) sql.SyncResult {
	return c.newMiscResult(pos, readyForQuery)
}

// CreateFlushResult is part of the sql.ClientComm interface.
func (c *conn) CreateFlushResult(pos sql.CmdPos) sql.FlushResult {
	return c.newMiscResult(pos, flush)
}

// CreateDrainResult is part of the sql.ClientComm interface.
func (c *conn) CreateDrainResult(pos sql.CmdPos) sql.DrainResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateBindResult is part of the sql.ClientComm interface.
func (c *conn) CreateBindResult(pos sql.CmdPos) sql.BindResult {
	return c.newMiscResult(pos, bindComplete)
}

// CreatePrepareResult is part of the sql.ClientComm interface.
func (c *conn) CreatePrepareResult(pos sql.CmdPos) sql.ParseResult {
	return c.newMiscResult(pos, parseComplete)
}

// CreateDescribeResult is part of the sql.ClientComm interface.
func (c *conn) CreateDescribeResult(pos sql.CmdPos) sql.DescribeResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateEmptyQueryResult is part of the sql.ClientComm interface.
func (c *conn) CreateEmptyQueryResult(pos sql.CmdPos) sql.EmptyQueryResult {
	return c.newMiscResult(pos, emptyQueryResponse)
}

// CreateDeleteResult is part of the sql.ClientComm interface.
func (c *conn) CreateDeleteResult(pos sql.CmdPos) sql.DeleteResult {
	return c.newMiscResult(pos, closeComplete)
}

// CreateErrorResult is part of the sql.ClientComm interface.
func (c *conn) CreateErrorResult(pos sql.CmdPos) sql.ErrorResult {
	res := c.newMiscResult(pos, noCompletionMsg)
	res.errExpected = true
	return res
}

// CreateCopyInResult is part of the sql.ClientComm interface.
func (c *conn) CreateCopyInResult(pos sql.CmdPos) sql.CopyInResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// pgwireReader is an io.Reader that wraps a conn, maintaining its metrics as
// it is consumed.
type pgwireReader struct {
	conn *conn
}

// pgwireReader implements the pgwirebase.BufferedReader interface.
var _ pgwirebase.BufferedReader = &pgwireReader{}

// Read is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) Read(p []byte) (int, error) {
	n, err := r.conn.rd.Read(p)
	r.conn.metrics.BytesInCount.Inc(int64(n))
	return n, err
}

// ReadString is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadString(delim byte) (string, error) {
	s, err := r.conn.rd.ReadString(delim)
	r.conn.metrics.BytesInCount.Inc(int64(len(s)))
	return s, err
}

// ReadByte is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadByte() (byte, error) {
	b, err := r.conn.rd.ReadByte()
	if err == nil {
		r.conn.metrics.BytesInCount.Inc(1)
	}
	return b, err
}

// statusReportParams is a list of session variables that are also
// reported as server run-time parameters in the pgwire connection
// initialization.
//
// The standard PostgreSQL status vars are listed here:
// https://www.postgresql.org/docs/10/static/libpq-status.html
var statusReportParams = []string{
	"server_version",
	"server_encoding",
	"client_encoding",
	"application_name",
	// Note: is_superuser and session_authorization are handled
	// specially in serveImpl().
	"DateStyle",
	"IntervalStyle",
	"TimeZone",
	"integer_datetimes",
	"standard_conforming_strings",
	"kwdb_version", // CockroachDB extension.
}

// testingStatusReportParams is the minimum set of status parameters
// needed to make pgx tests in the local package happy.
var testingStatusReportParams = map[string]string{
	"client_encoding":             "UTF8",
	"standard_conforming_strings": "on",
}

// readTimeoutConn overloads net.Conn.Read by periodically calling
// checkExitConds() and aborting the read if an error is returned.
type readTimeoutConn struct {
	net.Conn
	// checkExitConds is called periodically by Read(). If it returns an error,
	// the Read() returns that error. Future calls to Read() are allowed, in which
	// case checkExitConds() will be called again.
	checkExitConds func() error
}

func newReadTimeoutConn(c net.Conn, checkExitConds func() error) net.Conn {
	// net.Pipe does not support setting deadlines. See
	// https://github.com/golang/go/blob/go1.7.4/src/net/pipe.go#L57-L67
	//
	// TODO(andrei): starting with Go 1.10, pipes are supposed to support
	// timeouts, so this should go away when we upgrade the compiler.
	if c.LocalAddr().Network() == "pipe" {
		return c
	}
	return &readTimeoutConn{
		Conn:           c,
		checkExitConds: checkExitConds,
	}
}

func (c *readTimeoutConn) Read(b []byte) (int, error) {
	// readTimeout is the amount of time ReadTimeoutConn should wait on a
	// read before checking for exit conditions. The tradeoff is between the
	// time it takes to react to session context cancellation and the overhead
	// of waking up and checking for exit conditions.
	const readTimeout = 1 * time.Second

	// Remove the read deadline when returning from this function to avoid
	// unexpected behavior.
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	for {
		if err := c.checkExitConds(); err != nil {
			return 0, err
		}
		if err := c.SetReadDeadline(timeutil.Now().Add(readTimeout)); err != nil {
			return 0, err
		}
		n, err := c.Conn.Read(b)
		// Continue if the error is due to timing out.
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		}
		return n, err
	}
}

// SQLConnectionMaxLimit controls the max sql connections.
var SQLConnectionMaxLimit = settings.RegisterValidatedIntSetting(
	"server.sql_connections.max_limit",
	"the maximum of connections from clients to servers per node",
	200,
	func(v int64) error {
		if v < 4 || v > 50000 {
			return errors.Errorf("server.sql_connections.max_limit should be set to a value between 4 and 50000")
		}
		return nil
	},
)

// ErrUserConnectionLimited is the error of ordinary user connection failure.
const ErrUserConnectionLimited = "remaining connection slots are reserved for superuser connections"

// ErrConnectionLimited is the error of superuser connection failure.
const ErrConnectionLimited = "too many clients already"

// checkConnectionLimit check sql connections from kwdb_internal.cluster_sessions
func (c *conn) checkConnectionLimit(
	ctx context.Context, ie *sql.InternalExecutor, username string, execCfg *sql.ExecutorConfig,
) error {
	sendError := func(err error) error {
		_ /* err */ = writeErr(ctx, &execCfg.Settings.SV, err, &c.msgBuilder, c.conn)
		return err
	}

	maxLimit := SQLConnectionMaxLimit.Get(c.sv)
	errMessage := ErrConnectionLimited
	isRoot := username == security.RootUser
	// reserve three connections for root user, with the same behavior as PostgreSQL.
	if !isRoot {
		maxLimit -= 3
		errMessage = ErrUserConnectionLimited
	}

	getConns := `SELECT count(*) FROM kwdb_internal.node_sessions WHERE client_address != '<admin>'`
	connCounts, err := ie.Query(ctx, "get-sql-connections", nil, getConns)
	if err != nil {
		return err
	}

	counts := int64(tree.MustBeDInt(connCounts[0][0]))
	if counts+1 > maxLimit {
		return sendError(errors.Errorf(errMessage))
	}

	return nil
}
