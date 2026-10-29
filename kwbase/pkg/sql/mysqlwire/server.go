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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"io"
	"net"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// serverVersion is what KWDB reports in the handshake.
//
// The 8.0 prefix is load-bearing rather than cosmetic: MySQL clients and
// connectors gate features on a version comparison, and a string that does not
// parse as a MySQL version makes several of them refuse the connection outright
// or disable capabilities they would otherwise negotiate. The KWDB suffix keeps
// the reported identity honest for anyone reading it.
const serverVersion = "8.0.11-KWDB"

// baseSQLMemoryBudget is the memory pre-allocated for each connection, and
// mirrors pgwire's value so that a MySQL connection costs a node the same as a
// PostgreSQL one.
//
// pgwire reads the same quantity from KWBASE_BASE_SQL_MEMORY_BUDGET. This does
// not, because envutil panics when one variable is registered twice, and
// duplicating the registration would make the process refuse to start. An
// operator who tunes that variable therefore changes the pgwire budget only;
// the MySQL listener keeps the default.
var baseSQLMemoryBudget = int64(2.1 * float64(mon.DefaultPoolAllocationSize))

const connReservationBatchSize = 5

var noteworthyMySQLMemoryUsageBytes = envutil.EnvOrDefaultInt64(
	"KWBASE_NOTEWORTHY_MYSQL_CONN_MEMORY_USAGE", 2*1024*1024)

// connResultsBufferSize caps how much result data is buffered before being
// flushed to a MySQL client. It shares pgwire's cluster setting so that both
// protocols behave the same under the same configuration.
var connResultsBufferSize = settings.RegisterPublicByteSizeSetting(
	"sql.mysql.conn.results_buffer_size",
	"default size of the buffer that accumulates results for a statement or a batch "+
		"of statements before they are sent to a MySQL client",
	16<<10,
)

// ServerMetrics tracks the MySQL listener, separately from the PostgreSQL one
// so that operators can see which protocol their load actually arrives on.
type ServerMetrics struct {
	BytesInCount   *metric.Counter
	BytesOutCount  *metric.Counter
	Conns          *metric.Gauge
	NewConns       *metric.Counter
	ConnMemMetrics sql.MemoryMetrics
}

var (
	metaMySQLConns = metric.Metadata{
		Name:        "sql.mysql.conns",
		Help:        "Number of active MySQL protocol connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMySQLNewConns = metric.Metadata{
		Name:        "sql.mysql.new_conns",
		Help:        "Counter of the number of MySQL protocol connections created",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaMySQLBytesIn = metric.Metadata{
		Name:        "sql.mysql.bytesin",
		Help:        "Number of bytes received from MySQL protocol clients",
		Measurement: "SQL Client Traffic",
		Unit:        metric.Unit_BYTES,
	}
	metaMySQLBytesOut = metric.Metadata{
		Name:        "sql.mysql.bytesout",
		Help:        "Number of bytes sent to MySQL protocol clients",
		Measurement: "SQL Client Traffic",
		Unit:        metric.Unit_BYTES,
	}
)

func makeServerMetrics(histogramWindow time.Duration) ServerMetrics {
	return ServerMetrics{
		BytesInCount:   metric.NewCounter(metaMySQLBytesIn),
		BytesOutCount:  metric.NewCounter(metaMySQLBytesOut),
		Conns:          metric.NewGauge(metaMySQLConns),
		NewConns:       metric.NewCounter(metaMySQLNewConns),
		ConnMemMetrics: sql.MakeMemMetrics("mysql-conns", histogramWindow),
	}
}

// Server serves MySQL protocol connections against a KWDB SQL server.
//
// It listens on its own port rather than sharing the PostgreSQL one. That is
// forced by the protocols, not chosen: pgwire's client speaks first, MySQL's
// server speaks first, so no amount of sniffing can tell them apart before one
// side commits.
type Server struct {
	cfg       *base.Config
	execCfg   *sql.ExecutorConfig
	SQLServer *sql.Server
	stopper   *stop.Stopper

	metrics     ServerMetrics
	connMonitor mon.BytesMonitor

	// rsaKey lets the cleartext plugins run without TLS. It is generated once
	// per node at startup: MySQL's own behavior is to generate a key pair for
	// exactly this purpose, and a client that pins the key across restarts is
	// already outside what the protocol promises.
	rsaKey *rsa.PrivateKey

	mu struct {
		syncutil.Mutex
		// draining is set while the node is shutting down, so that new
		// connections are refused with a MySQL-shaped error rather than a
		// dropped TCP connection.
		draining bool
		// connCancelMap lets a drain interrupt connections that are idle in the
		// middle of a command.
		connCancelMap cancelChanMap
	}

	// connCount is read without the mutex by IsDraining callers on the hot path.
	connCount int32

	// nextConnID hands out the id a connection announces in its greeting. It is
	// deliberately not connCount: that one is a gauge of live connections, so
	// it repeats (connections opened one after another are all the only one
	// open, hence all 1) and collides (a connection opened after another
	// closed is given the number of one still running). An identifier may do
	// neither. Wrapping past 2^32 does repeat a number, as it does in MySQL,
	// whose field is this width.
	nextConnID uint32
}

type cancelChanMap map[chan struct{}]context.CancelFunc

// MakeServer builds a MySQL protocol server. It mirrors pgwire.MakeServer so
// that the two listeners are configured the same way by the node startup code.
func MakeServer(
	ambientCtx log.AmbientContext,
	cfg *base.Config,
	st *cluster.Settings,
	executorConfig *sql.ExecutorConfig,
	sqlServer *sql.Server,
	stopper *stop.Stopper,
	parentMemoryMonitor *mon.BytesMonitor,
	histogramWindow time.Duration,
) (*Server, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, errors.Wrap(err, "generating the RSA key for MySQL password exchange")
	}

	s := &Server{
		cfg:       cfg,
		execCfg:   executorConfig,
		SQLServer: sqlServer,
		stopper:   stopper,
		metrics:   makeServerMetrics(histogramWindow),
		rsaKey:    key,
	}
	s.mu.connCancelMap = make(cancelChanMap)
	s.connMonitor = mon.MakeMonitor("mysql-conn",
		mon.MemoryResource,
		s.metrics.ConnMemMetrics.CurBytesCount,
		s.metrics.ConnMemMetrics.MaxBytesHist,
		int64(connReservationBatchSize)*baseSQLMemoryBudget,
		noteworthyMySQLMemoryUsageBytes, st)
	s.connMonitor.Start(context.Background(), parentMemoryMonitor, mon.BoundAccount{})
	return s, nil
}

// Metrics exposes the listener's metrics for registration by the node.
func (s *Server) Metrics() (res []interface{}) {
	return []interface{}{&s.metrics}
}

// IsDraining reports whether the server is shutting down.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// SetDraining puts the server into or out of the draining state.
func (s *Server) SetDraining(drain bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.draining = drain
}

// ServeConn serves one connection: it drives the handshake, authenticates the
// client, and then runs the command loop until the client disconnects.
func (s *Server) ServeConn(ctx context.Context, netConn net.Conn) error {
	atomic.AddInt32(&s.connCount, 1)
	s.metrics.Conns.Inc(1)
	s.metrics.NewConns.Inc(1)
	defer func() {
		atomic.AddInt32(&s.connCount, -1)
		s.metrics.Conns.Dec(1)
		_ = netConn.Close()
	}()

	if s.IsDraining() {
		return writeStandaloneErr(netConn, mysqlwirebase.NewError(
			mysqlwirebase.ErrServerShutdown, mysqlwirebase.SQLStateConnectionFailure,
			"Server shutdown in progress"))
	}

	handshake, err := s.handshake(ctx, netConn)
	if err != nil {
		// handshake has already reported the failure to the client; no session
		// exists yet, so there is nothing else to tear down.
		log.VEventf(ctx, 1, "mysqlwire: handshake failed: %v", err)
		return err
	}

	// A MySQL client names its database in the handshake rather than with a
	// separate command, so it has to become a session default here. Setting it
	// only on the conn would leave the session pointing at the cluster default
	// and every unqualified table name unresolvable, while the client believed
	// its database had been selected.
	defaults := sql.SessionDefaults{SessionDefaultsMp: make(map[string]string)}
	if handshake.database != "" {
		defaults.SessionDefaultsMp["database"] = handshake.database
	}
	sArgs := sql.SessionArgs{
		User:                  handshake.user,
		RemoteAddr:            handshake.conn.RemoteAddr(),
		SessionDefaults:       defaults,
		ConnResultsBufferSize: connResultsBufferSize.Get(&s.execCfg.Settings.SV),
	}

	reserved := s.connMonitor.MakeBoundAccount()
	if err := reserved.Grow(ctx, baseSQLMemoryBudget); err != nil {
		return err
	}

	c := newConn(handshake.conn, sArgs, handshake.capabilities, &s.metrics, &s.execCfg.Settings.SV)
	c.pr, c.pw = handshake.pr, handshake.pw
	c.currentDB = handshake.database
	c.connID = handshake.connID

	return s.serveConn(ctx, c, reserved)
}

// serveConn runs the connExecutor alongside the reader loop, exactly as pgwire
// does: the reader turns packets into commands, and a second goroutine drains
// them.
func (s *Server) serveConn(ctx context.Context, c *conn, reserved mon.BoundAccount) error {
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	connHandler, err := s.SQLServer.SetupConn(
		ctx, c.sessionArgs, &c.stmtBuf, c, s.metrics.ConnMemMetrics)
	if err != nil {
		c.bufferErr(ctx, err)
		_ = c.Flush(0)
		reserved.Close(ctx)
		return err
	}

	// Set before the reader loop starts: every statement it parses needs the
	// session's int size.
	c.intSizer = connHandler

	procCh := make(chan error, 1)
	go func() {
		procCh <- s.SQLServer.ServeConn(ctx, connHandler, reserved, cancelConn)
	}()

	readErr := c.serveCommands(ctx, s.IsDraining)

	// Closing the buffer tells the connExecutor there is nothing more coming,
	// which is how it learns to shut down when the client hangs up.
	c.stmtBuf.Close()
	cancelConn()
	<-procCh

	return readErr
}

// handshakeResult carries what the connection phase established.
type handshakeResult struct {
	conn         net.Conn
	pr           *mysqlwirebase.PacketReader
	pw           *mysqlwirebase.PacketWriter
	user         string
	database     string
	capabilities uint32
	// connID is the id already announced in the greeting. It travels with the
	// rest of the connection phase's results so that later answers about this
	// connection come from the number the client was actually given.
	connID uint32
}

// handshake performs the MySQL connection phase and, on failure, reports the
// reason to the client.
//
// Reporting has to happen here rather than in the caller because both pieces
// of state it needs are local: the packet sequence number, which depends on
// how far the conversation got, and which connection is actually live, which
// changes when the client upgrades to TLS. Writing to the pre-upgrade
// connection after a TLS handshake injects plaintext into the TLS stream, and
// the client reports a broken connection rather than the access denial that
// actually happened.
func (s *Server) handshake(ctx context.Context, netConn net.Conn) (*handshakeResult, error) {
	res, pw, caps, err := s.handshakeImpl(ctx, netConn)
	if err != nil && pw != nil {
		code, state := mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError
		if myErr, ok := err.(*mysqlwirebase.Error); ok {
			code, state = myErr.Code, myErr.SQLState
		}
		if writeErr := pw.WritePacket(mysqlwirebase.EncodeErr(
			code, state, err.Error(), caps)); writeErr == nil {
			_ = pw.Flush()
		}
	}
	return res, err
}

// bufferedConn serves reads from r while everything else stays on Conn. It
// hands whatever a buffered reader has already pulled off the socket to the
// next reader instead of leaving it unreachable behind the bare connection.
type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// handshakeImpl runs the connection phase, returning the writer and negotiated
// capabilities alongside any error so that handshake can report it.
func (s *Server) handshakeImpl(
	ctx context.Context, netConn net.Conn,
) (*handshakeResult, *mysqlwirebase.PacketWriter, uint32, error) {
	rd := bufio.NewReader(netConn)
	pr := mysqlwirebase.NewPacketReader(rd, maxPacketPayload)
	pw := mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn))

	scramble, err := mysqlwirebase.GenerateScramble()
	if err != nil {
		return nil, nil, 0, err
	}

	// caps is what an error packet is encoded with. It starts as the server's
	// advertised set and narrows to the negotiated set once the client replies.
	caps := uint32(mysqlwirebase.ClientProtocol41)

	serverCaps := mysqlwirebase.ServerCapabilities
	tlsConfig, err := s.tlsConfig()
	if err != nil {
		return nil, nil, 0, err
	}
	if tlsConfig != nil {
		serverCaps |= mysqlwirebase.ClientSSL
	}

	// The id is allocated here rather than after authentication because the
	// greeting is where it is announced, so a connection that never
	// authenticates still consumes one.
	connID := atomic.AddUint32(&s.nextConnID, 1)

	greeting := &mysqlwirebase.Handshake{
		ServerVersion: serverVersion,
		// The connection id is what a client reports as its thread id and what
		// KILL would target. KWDB has no MySQL-style thread registry, so this
		// is unique per connection but not resolvable back to a session.
		ConnectionID: connID,
		Scramble:     scramble,
		Capabilities: serverCaps,
		Charset:      byte(mysqlwirebase.CollationUTF8MB4GeneralCI),
		StatusFlags:  baseStatusFlags,
		AuthPlugin:   mysqlwirebase.AuthCachingSHA2Password,
	}
	if err := pw.WritePacket(greeting.Encode()); err != nil {
		return nil, pw, caps, err
	}
	if err := pw.Flush(); err != nil {
		return nil, pw, caps, err
	}
	// The connection phase numbers packets with one counter shared by both
	// sides, so the client's reply continues from the greeting rather than
	// restarting at zero.
	pr.SetSequence(pw.Sequence())

	payload, err := pr.ReadPacket()
	if err != nil {
		return nil, pw, caps, err
	}
	pw.SetSequence(pr.Sequence())

	clientCaps, err := mysqlwirebase.PeekCapabilities(payload)
	if err != nil {
		return nil, pw, caps, err
	}

	tlsActive := false
	if mysqlwirebase.IsSSLRequest(payload, clientCaps) {
		if tlsConfig == nil {
			return nil, pw, caps, mysqlwirebase.NewError(
				mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError,
				"the server is not configured to accept TLS connections")
		}
		// rd may already hold bytes belonging to the TLS handshake. MySQL has
		// no server turn between the SSLRequest and the ClientHello -- unlike
		// PostgreSQL, whose client waits for a one-byte answer before starting
		// TLS -- so a client writes both back to back and one read can return
		// them together. Handing crypto/tls the bare socket there strands the
		// buffered ClientHello: the server waits for a message it has already
		// received while the client waits for the server, and nothing on the
		// wire times either side out.
		tlsConn := tls.Server(&bufferedConn{Conn: netConn, r: rd}, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return nil, pw, caps, err
		}
		netConn = tlsConn
		tlsActive = true

		// The handshake response arrives inside the TLS session, but its
		// sequence numbers continue from the SSLRequest that preceded it.
		seqR, seqW := pr.Sequence(), pw.Sequence()
		rd = bufio.NewReader(netConn)
		pr = mysqlwirebase.NewPacketReader(rd, maxPacketPayload)
		pw = mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn))
		pr.SetSequence(seqR)
		pw.SetSequence(seqW)

		if payload, err = pr.ReadPacket(); err != nil {
			return nil, pw, caps, err
		}
		pw.SetSequence(pr.Sequence())
	}

	resp, err := mysqlwirebase.DecodeHandshakeResponse(payload, serverCaps)
	if err != nil {
		return nil, pw, caps, err
	}
	caps = resp.Capabilities

	auth := &authenticator{
		conn:           netConn,
		pr:             pr,
		pw:             pw,
		scramble:       scramble,
		tlsConfig:      tlsConfig,
		rsaKey:         s.rsaKey,
		tlsActive:      tlsActive,
		insecure:       s.cfg.Insecure,
		greetingPlugin: greeting.AuthPlugin,
	}
	result, err := auth.authenticate(ctx, resp.User, resp.AuthPlugin, resp.AuthResponse, s.lookupUser)
	if err != nil {
		return nil, pw, caps, err
	}
	log.VEventf(ctx, 1, "mysqlwire: %q authenticated with %s (tls=%t)",
		resp.User, result.plugin, result.tlsActive)

	// The OK packet completes the connection phase and moves the connection
	// into the command phase.
	if err := pw.WritePacket(mysqlwirebase.EncodeOK(
		&mysqlwirebase.OK{StatusFlags: baseStatusFlags},
		resp.Capabilities, false /* eofSubstitute */)); err != nil {
		return nil, pw, caps, err
	}
	if err := pw.Flush(); err != nil {
		return nil, pw, caps, err
	}

	return &handshakeResult{
		conn:         netConn,
		pr:           pr,
		pw:           pw,
		user:         resp.User,
		database:     resp.Database,
		capabilities: resp.Capabilities,
		connID:       connID,
	}, pw, caps, nil
}

// tlsConfig returns the node's TLS configuration, or nil when the server runs
// insecure.
func (s *Server) tlsConfig() (*tls.Config, error) {
	if s.cfg.Insecure {
		return nil, nil
	}
	return s.cfg.GetServerTLSConfig()
}

// writeStandaloneErr sends an ERR packet on a connection that has not yet
// exchanged anything, which is the case only when a node refuses a connection
// because it is draining.
func writeStandaloneErr(netConn net.Conn, err error) error {
	code, state := mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError
	if myErr, ok := err.(*mysqlwirebase.Error); ok {
		code, state = myErr.Code, myErr.SQLState
	}
	// Nothing has been sent yet, so this is packet zero of the conversation.
	pw := mysqlwirebase.NewPacketWriter(bufio.NewWriter(netConn))
	if writeErr := pw.WritePacket(mysqlwirebase.EncodeErr(
		code, state, err.Error(), mysqlwirebase.ClientProtocol41)); writeErr != nil {
		return err
	}
	_ = pw.Flush()
	return err
}
