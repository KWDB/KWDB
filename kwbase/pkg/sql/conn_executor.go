// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/fsm"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/net/trace"
)

// noteworthyMemoryUsageBytes is the minimum size tracked by a
// transaction or session monitor before the monitor starts explicitly
// logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("KWBASE_NOTEWORTHY_SESSION_MEMORY_USAGE", 1024*1024)

// A connExecutor is in charge of executing queries received on a given client
// connection. The connExecutor implements a state machine (dictated by the
// Postgres/pgwire session semantics). The state machine is supposed to run
// asynchronously wrt the client connection: it receives input statements
// through a stmtBuf and produces results through a clientComm interface. The
// connExecutor maintains a cursor over the statementBuffer and executes
// statements / produces results for one statement at a time. The cursor points
// at all times to the statement that the connExecutor is currently executing.
// Results for statements before the cursor have already been produced (but not
// necessarily delivered to the client). Statements after the cursor are queued
// for future execution. Keeping already executed statements in the buffer is
// useful in case of automatic retries (in which case statements from the
// retried transaction have to be executed again); the connExecutor is in charge
// of removing old statements that are no longer needed for retries from the
// (head of the) buffer. Separately, the implementer of the clientComm interface
// (e.g. the pgwire module) is in charge of keeping track of what results have
// been delivered to the client and what results haven't (yet).
//
// The connExecutor has two main responsibilities: to dispatch queries to the
// execution engine(s) and relay their results to the clientComm, and to
// implement the state machine maintaining the various aspects of a connection's
// state. The state machine implementation is further divided into two aspects:
// maintaining the transaction status of the connection (outside of a txn,
// inside a txn, in an aborted txn, in a txn awaiting client restart, etc.) and
// maintaining the cursor position (i.e. correctly jumping to whatever the
// "next" statement to execute is in various situations).
//
// The cursor normally advances one statement at a time, but it can also skip
// some statements (remaining statements in a query string are skipped once an
// error is encountered) and it can sometimes be rewound when performing
// automatic retries. Rewinding can only be done if results for the rewound
// statements have not actually been delivered to the client; see below.
//
//                                                   +---------------------+
//                                                   |connExecutor         |
//                                                   |                     |
//                                                   +->execution+--------------+
//                                                   ||  +                 |    |
//                                                   ||  |fsm.Event        |    |
//                                                   ||  |                 |    |
//                                                   ||  v                 |    |
//                                                   ||  fsm.Machine(TxnStateTransitions)
//                                                   ||  +  +--------+     |    |
//      +--------------------+                       ||  |  |txnState|     |    |
//      |stmtBuf             |                       ||  |  +--------+     |    |
//      |                    | statements are read   ||  |                 |    |
//      | +-+-+ +-+-+ +-+-+  +------------------------+  |                 |    |
//      | | | | | | | | | |  |                       |   |   +-------------+    |
//  +---> +-+-+ +++-+ +-+-+  |                       |   |   |session data |    |
//  |   |        ^           |                       |   |   +-------------+    |
//  |   |        |   +-----------------------------------+                 |    |
//  |   |        +   v       | cursor is advanced    |  advanceInfo        |    |
//  |   |       cursor       |                       |                     |    |
//  |   +--------------------+                       +---------------------+    |
//  |                                                                           |
//  |                                                                           |
//  +-------------+                                                             |
//                +--------+                                                    |
//                | parser |                                                    |
//                +--------+                                                    |
//                |                                                             |
//                |                                                             |
//                |                                   +----------------+        |
//        +-------+------+                            |execution engine<--------+
//        | pgwire conn  |               +------------+(local/DistSQL) |
//        |              |               |            +----------------+
//        |   +----------+               |
//        |   |clientComm<---------------+
//        |   +----------+           results are produced
//        |              |
//        +-------^------+
//                |
//                |
//        +-------+------+
//        | SQL client   |
//        +--------------+
//
// The connExecutor is disconnected from client communication (i.e. generally
// network communication - i.e. pgwire.conn); the module doing client
// communication is responsible for pushing statements into the buffer and for
// providing an implementation of the clientConn interface (and thus sending
// results to the client). The connExecutor does not control when
// results are delivered to the client, but still it does have some influence
// over that; this is because of the fact that the possibility of doing
// automatic retries goes away the moment results for the transaction in
// question are delivered to the client. The communication module has full
// freedom in sending results whenever it sees fit; however the connExecutor
// influences communication in the following ways:
//
// a) When deciding whether an automatic retry can be performed for a
// transaction, the connExecutor needs to:
//
//   1) query the communication status to check that no results for the txn have
//   been delivered to the client and, if this check passes:
//   2) lock the communication so that no further results are delivered to the
//   client, and, eventually:
//   3) rewind the clientComm to a certain position corresponding to the start
//   of the transaction, thereby discarding all the results that had been
//   accumulated for the previous attempt to run the transaction in question.
//
// These steps are all orchestrated through clientComm.lockCommunication() and
// rewindCapability{}.
//
// b) The connExecutor sometimes ask the clientComm to deliver everything
// (most commonly in response to a Sync command).
//
// As of Feb 2018, the pgwire.conn delivers results synchronously to the client
// when its internal buffer overflows. In principle, delivery of result could be
// done asynchronously wrt the processing of commands (e.g. we could have a
// timing policy in addition to the buffer size). The first implementation of
// that showed a performance impact of involving a channel communication in the
// Sync processing path.
//
//
// Implementation notes:
//
// --- Error handling ---
//
// The key to understanding how the connExecutor handles errors is understanding
// the fact that there's two distinct categories of errors to speak of. There
// are "query execution errors" and there are the rest. Most things fall in the
// former category: invalid queries, queries that fail constraints at runtime,
// data unavailability errors, retriable errors (i.e. serializability
// violations) "internal errors" (e.g. connection problems in the cluster). This
// category of errors doesn't represent dramatic events as far as the connExecutor
// is concerned: they produce "results" for the query to be passed to the client
// just like more successful queries do and they produce Events for the
// state machine just like the successful queries (the events in question
// are generally event{non}RetriableErr and they generally cause the
// state machine to move to the Aborted state, but the connExecutor doesn't
// concern itself with this). The way the connExecutor reacts to these errors is
// the same as how it reacts to a successful query completing: it moves the
// cursor over the incoming statements as instructed by the state machine and
// continues running statements.
//
// And then there's other errors that don't have anything to do with a
// particular query, but with the connExecutor itself. In other languages, these
// would perhaps be modeled as Exceptions: we want them to unwind the stack
// significantly. These errors cause the connExecutor.run() to break out of its
// loop and return an error. Example of such errors include errors in
// communication with the client (e.g. the network connection is broken) or the
// connection's context being canceled.
//
// All of connExecutor's methods only return errors for the 2nd category. Query
// execution errors are written to a CommandResult. Low-level methods don't
// operate on a CommandResult directly; instead they operate on a wrapper
// (resultWithStoredErr), which provides access to the query error for purposes
// of building the correct state machine event.
//
// --- Context management ---
//
// At the highest level, there's connExecutor.run() that takes a context. That
// context is supposed to represent "the connection's context": its lifetime is
// the client connection's lifetime and it is assigned to
// connEx.ctxHolder.connCtx. Below that, every SQL transaction has its own
// derived context because that's the level at which we trace operations. The
// lifetime of SQL transactions is determined by the txnState: the state machine
// decides when transactions start and end in txnState.performStateTransition().
// When we're inside a SQL transaction, most operations are considered to happen
// in the context of that txn. When there's no SQL transaction (i.e.
// stateNoTxn), everything happens in the connection's context.
//
// High-level code in connExecutor is agnostic of whether it currently is inside
// a txn or not. To deal with both cases, such methods don't explicitly take a
// context; instead they use connEx.Ctx(), which returns the appropriate ctx
// based on the current state.
// Lower-level code (everything from connEx.execStmt() and below which runs in
// between state transitions) knows what state its running in, and so the usual
// pattern of explicitly taking a context as an argument is used.

// Server is the top level singleton for handling SQL connections. It creates
// connExecutors to server every incoming connection.
type Server struct {
	_ util.NoCopy

	cfg *ExecutorConfig

	// sqlStats tracks per-application statistics for all applications on each
	// node. Newly collected statistics flow into sqlStats.
	sqlStats sqlStats
	// reportedStats is a pool of stats that is held for reporting, and is
	// cleared on a lower interval than sqlStats. Stats from sqlStats flow
	// into reported stats when sqlStats is cleared.
	reportedStats sqlStats

	reCache *tree.RegexpCache

	// pool is the parent monitor for all session monitors except "internal" ones.
	pool *mon.BytesMonitor

	// Metrics is used to account normal queries.
	Metrics Metrics

	// InternalMetrics is used to account internal queries.
	InternalMetrics Metrics

	// dbCache is a cache for database descriptors, maintained through Gossip
	// updates.
	dbCache *databaseCacheHolder
}

// Metrics collects timeseries data about SQL activity.
type Metrics struct {
	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics EngineMetrics

	// StartedStatementCounters contains metrics for statements initiated by
	// users. These metrics count user-initiated operations, regardless of
	// success (in particular, TxnCommitCount is the number of COMMIT statements
	// attempted, not the number of transactions that successfully commit).
	StartedStatementCounters StatementCounters

	// ExecutedStatementCounters contains metrics for successfully executed
	// statements.
	ExecutedStatementCounters StatementCounters
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	systemCfg := config.NewSystemConfig(cfg.DefaultZoneConfig)
	return &Server{
		cfg:             cfg,
		Metrics:         makeMetrics(false /*internal*/),
		InternalMetrics: makeMetrics(true /*internal*/),
		// dbCache will be updated on Start().
		dbCache:       newDatabaseCacheHolder(newDatabaseCache(systemCfg)),
		pool:          pool,
		sqlStats:      sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
		reportedStats: sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
		reCache:       tree.NewRegexpCache(512),
	}
}

func makeMetrics(internal bool) Metrics {
	return Metrics{
		EngineMetrics: EngineMetrics{
			DistSQLSelectCount:    metric.NewCounter(getMetricMeta(MetaDistSQLSelect, internal)),
			SQLOptFallbackCount:   metric.NewCounter(getMetricMeta(MetaSQLOptFallback, internal)),
			SQLOptPlanCacheHits:   metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheHits, internal)),
			SQLOptPlanCacheMisses: metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheMisses, internal)),

			// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
			DistSQLExecLatency: metric.NewLatency(getMetricMeta(MetaDistSQLExecLatency, internal),
				6*metricsSampleInterval),
			SQLExecLatency: metric.NewLatency(getMetricMeta(MetaSQLExecLatency, internal),
				6*metricsSampleInterval),
			DistSQLServiceLatency: metric.NewLatency(getMetricMeta(MetaDistSQLServiceLatency, internal),
				6*metricsSampleInterval),
			SQLServiceLatency: metric.NewLatency(getMetricMeta(MetaSQLServiceLatency, internal),
				6*metricsSampleInterval),
			SQLTxnLatency: metric.NewLatency(getMetricMeta(MetaSQLTxnLatency, internal),
				6*metricsSampleInterval),

			TxnAbortCount: metric.NewCounter(getMetricMeta(MetaTxnAbort, internal)),
			FailureCount:  metric.NewCounter(getMetricMeta(MetaFailure, internal)),
		},
		StartedStatementCounters:  makeStartedStatementCounters(internal),
		ExecutedStatementCounters: makeExecutedStatementCounters(internal),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg := s.cfg.Gossip.GetSystemConfig()
				s.dbCache.updateSystemConfig(sysCfg)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	// Start a loop to clear SQL stats at the max reset interval. This is
	// to ensure that we always have some worker clearing SQL stats to avoid
	// continually allocating space for the SQL stats. Additionally, spawn
	// a loop to clear the reported stats at the same large interval just
	// in case the telemetry worker fails.
	s.PeriodicallyClearSQLStats(ctx, stopper, MaxSQLStatReset, &s.sqlStats, s.ResetSQLStats)
	s.PeriodicallyClearSQLStats(ctx, stopper, MaxSQLStatReset, &s.reportedStats, s.ResetReportedStats)
	// Start a second loop to clear SQL stats at the requested interval.
	s.PeriodicallyClearSQLStats(ctx, stopper, SQLStatReset, &s.sqlStats, s.ResetSQLStats)
}

// ResetSQLStats resets the executor's collected sql statistics.
func (s *Server) ResetSQLStats(ctx context.Context) {
	// Dump the SQL stats into the reported stats before clearing the SQL stats.
	s.sqlStats.resetAndMaybeDumpStats(ctx, &s.reportedStats)
}

// ResetReportedStats resets the executor's collected reported stats.
func (s *Server) ResetReportedStats(ctx context.Context) {
	s.reportedStats.resetAndMaybeDumpStats(ctx, nil /* target */)
}

// GetScrubbedStmtStats returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (s *Server) GetScrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	return s.sqlStats.getScrubbedStmtStats(s.cfg.VirtualSchemas)
}

// Avoid lint errors.
var _ = (*Server).GetScrubbedStmtStats

// GetUnscrubbedStmtStats returns the same thing as GetScrubbedStmtStats, except
// identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	return s.sqlStats.getUnscrubbedStmtStats(s.cfg.VirtualSchemas)
}

// GetCFG returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (s *Server) GetCFG() *ExecutorConfig {
	return s.cfg
}

// GetScrubbedReportingStats does the same thing as GetScrubbedStmtStats but
// returns statistics from the reported stats pool.
func (s *Server) GetScrubbedReportingStats() []roachpb.CollectedStatementStatistics {
	return s.reportedStats.getScrubbedStmtStats(s.cfg.VirtualSchemas)
}

// GetUnscrubbedReportingStats does the same thing as GetUnscrubbedStmtStats but
// returns statistics from the reported stats pool.
func (s *Server) GetUnscrubbedReportingStats() []roachpb.CollectedStatementStatistics {
	return s.reportedStats.getUnscrubbedStmtStats(s.cfg.VirtualSchemas)
}

// GetStmtStatsLastReset returns the time at which the statement statistics were
// last cleared.
func (s *Server) GetStmtStatsLastReset() time.Time {
	return s.sqlStats.getLastReset()
}

// GetExecutorConfig returns this server's executor config.
func (s *Server) GetExecutorConfig() *ExecutorConfig {
	return s.cfg
}

// SetupConn creates a connExecutor for the client connection.
//
// When this method returns there are no resources allocated yet that
// need to be close()d.
//
// Args:
// args: The initial session parameters. They are validated by SetupConn
//
//	and an error is returned if this validation fails.
//
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
//
//	produce results for the client.
//
// memMetrics: The metrics that statements executed on this connection will
//
//	contribute to.
func (s *Server) SetupConn(
	ctx context.Context,
	args SessionArgs,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
) (ConnectionHandler, error) {
	sd := s.newSessionData(args)
	uuid := s.cfg.ClusterID()
	buf := make([]byte, 36)
	uuid.StringBytes(buf)
	sd.ClusterID = buf[:]
	if s.cfg.AdminURL != nil {
		sd.ClientIP = s.cfg.AdminURL().Host
	} else {
		sd.ClientIP = "unknown"
	}
	sdMut := s.makeSessionDataMutator(sd, args.SessionDefaults)
	ex, err := s.newConnExecutor(
		ctx, sd, &sdMut, stmtBuf, clientComm, memMetrics, &s.Metrics, resetSessionDataToDefaults)
	return ConnectionHandler{ex}, err
}

// ConnectionHandler is the interface between the result of SetupConn
// and the ServeConn below. It encapsulates the connExecutor and hides
// it away from other packages.
type ConnectionHandler struct {
	ex *connExecutor
}

// GetClientEncoding is the func that get client_encoding session
func (h ConnectionHandler) GetClientEncoding() string {
	if h.ex == nil {
		return "UTF8"
	}
	if h.ex.dataMutator == nil {
		return "UTF8"
	}
	if h.ex.dataMutator.data == nil {
		return "UTF8"
	}
	if h.ex.dataMutator.data.ClientEncoding == "" {
		return "UTF8"
	}
	return h.ex.dataMutator.data.ClientEncoding
}

// SendDTI send directTsInsert
func (h ConnectionHandler) SendDTI(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	r CommandResult,
	di DirectInsert,
	stmts parser.Statements,
) (useDeepRule bool, dedupRule int64, dedupRows int64, err error) {
	h.ex.metrics.StartedStatementCounters.QueryCount.Inc()
	h.ex.metrics.StartedStatementCounters.InsertCount.Inc()
	if !evalCtx.StartSinglenode {
		h.ex.planner.ExtendedEvalContext().StartDistributeMode = true
	}
	starttime := timeutil.Now()
	ctx, sp := tracing.EnsureChildSpan(
		ctx, h.ex.server.cfg.AmbientCtx.Tracer,
		// We print the type of command, not the String() which includes long
		// statements.
		"insert stmt")
	defer sp.Finish()
	useDeepRule, dedupRule, dedupRows = h.ex.SendDirectTsInsert(ctx, evalCtx, r, di.PayloadNodeMap)
	if err = r.Err(); err != nil {
		return
	}
	endtime := timeutil.Now()
	var tempStmt Statement
	var flags planFlags
	tempStmt.AST = stmts[0].AST
	user := h.ex.sessionData.User
	database := h.ex.sessionData.Database
	h.ex.statsCollector.recordStatement(
		&tempStmt, nil,
		flags.IsSet(planFlagDistributed), flags.IsSet(planFlagImplicitTxn),
		0, di.RowNum, nil,
		0, 0, endtime.Sub(starttime).Seconds(), 0, 0, 0, 0,
		user, database,
	)
	h.ex.metrics.ExecutedStatementCounters.QueryCount.Inc()
	h.ex.metrics.ExecutedStatementCounters.InsertCount.Inc()
	return
}

// GetTsinsertdirect return TsInsertShortcircuit
func (h ConnectionHandler) GetTsinsertdirect() bool {
	return h.ex.sessionData.TsInsertShortcircuit
}

// GetTsSessionData implements get session
func (h ConnectionHandler) GetTsSessionData() *sessiondata.SessionData {
	return h.ex.sessionData
}

// GetTsSupportBatch get TsSupportBatch status
func (h ConnectionHandler) GetTsSupportBatch() bool {
	return h.ex.sessionData.TsSupportBatch
}

// GetTsZone constructs a ParseTimeContext that returns the given values.
func (h ConnectionHandler) GetTsZone() time.Time {
	return h.ex.state.sqlTimestamp.In(h.ex.sessionData.DataConversion.Location)
}

// GetPreparedStatement get PreparedStatement
func (h ConnectionHandler) GetPreparedStatement(
	PreparedStatementName string,
) (*PreparedStatement, bool) {
	ps, ok := h.ex.extraTxnState.prepStmtsNamespace.prepStmts[PreparedStatementName]
	return ps, ok
}

// GetUnqualifiedIntSize implements pgwire.sessionDataProvider and returns
// the type that INT should be parsed as.
func (h ConnectionHandler) GetUnqualifiedIntSize() *types.T {
	var size int
	if h.ex != nil {
		// The executor will be nil in certain testing situations where
		// no server is actually present.
		size = h.ex.sessionData.DefaultIntSize
	}
	switch size {
	case 4, 32:
		return types.Int4
	default:
		return types.Int
	}
}

// GetParamStatus retrieves the configured value of the session
// variable identified by varName. This is used for the initial
// message sent to a client during a session set-up.
func (h ConnectionHandler) GetParamStatus(ctx context.Context, varName string) string {
	name := strings.ToLower(varName)
	v, ok := varGen[name]
	if !ok {
		log.Fatalf(ctx, "programming error: status param %q must be defined session var", varName)
		return ""
	}
	hasDefault, defVal := getSessionVarDefaultString(name, v, h.ex.dataMutator)
	if !hasDefault {
		log.Fatalf(ctx, "programming error: status param %q must have a default value", varName)
		return ""
	}
	return defVal
}

// GetDBName is the method to obtain Database
func (h ConnectionHandler) GetDBName() string {
	if h.ex.sessionData != nil {
		return h.ex.sessionData.Database
	}
	return ""
}

// ServeConn serves a client connection by reading commands from the stmtBuf
// embedded in the ConnHandler.
//
// If not nil, reserved represents memory reserved for the connection. The
// connExecutor takes ownership of this memory.
func (s *Server) ServeConn(
	ctx context.Context, h ConnectionHandler, reserved mon.BoundAccount, cancel context.CancelFunc,
) error {
	defer func() {
		r := recover()
		h.ex.closeWrapper(ctx, r)
	}()
	return h.ex.run(ctx, s.pool, reserved, cancel)
}

// newSessionData a SessionData that can be passed to newConnExecutor.
func (s *Server) newSessionData(args SessionArgs) *sessiondata.SessionData {
	sd := &sessiondata.SessionData{
		User:              args.User,
		Roles:             args.Roles,
		RemoteAddr:        args.RemoteAddr,
		ResultsBufferSize: args.ConnResultsBufferSize,
	}
	s.populateMinimalSessionData(sd)
	return sd
}

func (s *Server) makeSessionDataMutator(
	sd *sessiondata.SessionData, defaults SessionDefaults,
) sessionDataMutator {
	return sessionDataMutator{
		data:               sd,
		defaults:           defaults,
		settings:           s.cfg.Settings,
		paramStatusUpdater: &noopParamStatusUpdater{},
	}
}

// populateMinimalSessionData populates sd with some minimal values needed for
// not crashing. Fields of sd that are already set are not overwritten.
func (s *Server) populateMinimalSessionData(sd *sessiondata.SessionData) {
	if sd.SequenceState == nil {
		sd.SequenceState = sessiondata.NewSequenceState()
	}
	if sd.DataConversion == (sessiondata.DataConversionConfig{}) {
		sd.DataConversion = sessiondata.DataConversionConfig{
			Location: time.UTC,
		}
	}
	if len(sd.SearchPath.GetPathArray()) == 0 {
		sd.SearchPath = sqlbase.DefaultSearchPath
	}
	if sd.UserDefinedVars == nil {
		sd.UserDefinedVars = make(map[string]interface{})
	}
}

type sdResetOption bool

const (
	resetSessionDataToDefaults     sdResetOption = true
	dontResetSessionDataToDefaults               = false
)

// newConnExecutor creates a new connExecutor.
//
// resetOpt controls whether sd is to be reset to the default values.
// TODO(andrei): resetOpt is a hack needed by the InternalExecutor, which
// doesn't want this resetting. Figure out a better API where the responsibility
// of assigning default values is either entirely inside or outside of this
// ctor.
func (s *Server) newConnExecutor(
	ctx context.Context,
	sd *sessiondata.SessionData,
	sdMutator *sessionDataMutator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	resetOpt sdResetOption,
) (*connExecutor, error) {
	// Create the various monitors.
	// The session monitors are started in activate().
	sessionRootMon := mon.MakeMonitor(
		"session root",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		-1 /* increment */, math.MaxInt64, s.cfg.Settings,
	)
	sessionMon := mon.MakeMonitor(
		"session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)
	// The txn monitor is started in txnState.resetForNewSQLTxn().
	txnMon := mon.MakeMonitor(
		"txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)

	ex := &connExecutor{
		server:      s,
		metrics:     srvMetrics,
		stmtBuf:     stmtBuf,
		clientComm:  clientComm,
		mon:         &sessionRootMon,
		sessionMon:  &sessionMon,
		sessionData: sd,
		dataMutator: sdMutator,
		state: txnState{
			mon:     &txnMon,
			connCtx: ctx,
		},
		transitionCtx: transitionCtx{
			db:     s.cfg.DB,
			nodeID: s.cfg.NodeID.Get(),
			clock:  s.cfg.Clock,
			// Future transaction's monitors will inherits from sessionRootMon.
			connMon:  &sessionRootMon,
			tracer:   s.cfg.AmbientCtx.Tracer,
			settings: s.cfg.Settings,
		},
		memMetrics: memMetrics,
		planner:    planner{execCfg: s.cfg},

		// ctxHolder will be reset at the start of run(). We only define
		// it here so that an early call to close() doesn't panic.
		ctxHolder:                 ctxHolder{connCtx: ctx},
		executorType:              executorTypeExec,
		hasCreatedTemporarySchema: false,
		stmtDiagnosticsRecorder:   s.cfg.StmtDiagnosticsRecorder,
	}

	ex.state.txnAbortCount = ex.metrics.EngineMetrics.TxnAbortCount

	sdMutator.setCurTxnReadOnly = func(val bool) {
		ex.state.readOnly = val
	}
	sdMutator.onTempSchemaCreation = func() {
		ex.hasCreatedTemporarySchema = true
	}
	sdMutator.RegisterOnSessionDataChange("application_name", func(newName string) {
		ex.appStats = ex.server.sqlStats.getStatsForApplication(newName)
		ex.applicationName.Store(newName)
	})
	// Initialize the session data from provided defaults. We need to do this early
	// because other initializations below use the configured values.
	if resetOpt == resetSessionDataToDefaults {
		if err := resetSessionVars(ctx, sdMutator); err != nil {
			log.Errorf(ctx, "error setting up client session: %v", err)
			return nil, err
		}
	} else {
		// We have set the ex.sessionData without using the dataMutator.
		// So we need to update the application name manually.
		ex.applicationName.Store(ex.sessionData.ApplicationName)
		// When the connEx is serving an internal executor, it can inherit
		// the application name from an outer session. This happens
		// e.g. during ::regproc casts and built-in functions that use SQL internally.
		// In that case, we do not want to record statistics against
		// the outer application name directly; instead we want
		// to use a separate bucket. However we will still
		// want to have separate buckets for different applications so that
		// we can measure their respective "pressure" on internal queries.
		// Hence the choice here to add the delegate prefix
		// to the current app name.
		var appStatsBucketName string
		if !strings.HasPrefix(ex.sessionData.ApplicationName, sqlbase.InternalAppNamePrefix) {
			appStatsBucketName = sqlbase.DelegatedAppNamePrefix + ex.sessionData.ApplicationName
		} else {
			// If this is already an "internal app", don't put more prefix.
			appStatsBucketName = ex.sessionData.ApplicationName
		}
		ex.appStats = s.sqlStats.getStatsForApplication(appStatsBucketName)
	}

	ex.phaseTimes[sessionInit] = timeutil.Now()
	ex.extraTxnState.prepStmtsNamespace = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceMemAcc = ex.sessionMon.MakeBoundAccount()
	ex.extraTxnState.tables = TableCollection{
		leaseMgr:          s.cfg.LeaseManager,
		databaseCache:     s.dbCache.getDatabaseCache(),
		dbCacheSubscriber: s.dbCache,
		settings:          s.cfg.Settings,
	}
	ex.extraTxnState.txnRewindPos = -1
	ex.extraTxnState.schemaChangeJobsCache = make(map[sqlbase.ID]*jobs.Job)
	ex.mu.ActiveQueries = make(map[ClusterWideID]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.sessionTracing.ex = ex
	ex.transitionCtx.sessionTracing = &ex.sessionTracing
	ex.statsCollector = ex.newStatsCollector()
	ex.initPlanner(ctx, &ex.planner)

	return ex, nil
}

// newConnExecutorWithTxn creates a connExecutor that will execute statements
// under a higher-level txn. This connExecutor runs with a different state
// machine, much reduced from the regular one. It cannot initiate or end
// transactions (so, no BEGIN, COMMIT, ROLLBACK, no auto-commit, no automatic
// retries).
//
// If there is no error, this function also activate()s the returned
// executor, so the caller does not need to run the
// activation. However this means that run() or close() must be called
// to release resources.
func (s *Server) newConnExecutorWithTxn(
	ctx context.Context,
	sd *sessiondata.SessionData,
	sdMutator *sessionDataMutator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	parentMon *mon.BytesMonitor,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	txn *kv.Txn,
	tcModifier tableCollectionModifier,
	resetOpt sdResetOption,
) (*connExecutor, error) {
	ex, err := s.newConnExecutor(
		ctx, sd, sdMutator, stmtBuf, clientComm, memMetrics, srvMetrics, resetOpt)
	if err != nil {
		return nil, err
	}

	// The new transaction stuff below requires active monitors and traces, so
	// we need to activate the executor now.
	ex.activate(ctx, parentMon, mon.BoundAccount{})

	// Perform some surgery on the executor - replace its state machine and
	// initialize the state.
	ex.machine = fsm.MakeMachine(
		BoundTxnStateTransitions,
		stateOpen{ImplicitTxn: fsm.False},
		&ex.state,
	)
	ex.state.resetForNewSQLTxn(
		ctx,
		explicitTxn,
		txn.ReadTimestamp().GoTime(),
		nil, /* historicalTimestamp */
		txn.UserPriority(),
		tree.ReadWrite,
		txn,
		ex.transitionCtx,
		tree.SerializableIsolation)

	// Modify the TableCollection to match the parent executor's TableCollection.
	// This allows the InternalExecutor to see schema changes made by the
	// parent executor.
	if tcModifier != nil {
		tcModifier.copyModifiedSchema(&ex.extraTxnState.tables)
	}
	return ex, nil
}

// SQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics should be reset.
var SQLStatReset = settings.RegisterPublicNonNegativeDurationSettingWithMaximum(
	"diagnostics.sql_stat_reset.interval",
	"interval controlling how often SQL statement statistics should "+
		"be reset (should be less than diagnostics.forced_sql_stat_reset.interval). It has a max value of 24H.",
	time.Hour,
	time.Hour*24,
)

// MaxSQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics must be flushed within.
var MaxSQLStatReset = settings.RegisterPublicNonNegativeDurationSettingWithMaximum(
	"diagnostics.forced_sql_stat_reset.interval",
	"interval after which SQL statement statistics are refreshed even "+
		"if not collected (should be more than diagnostics.sql_stat_reset.interval). It has a max value of 24H.",
	time.Hour*2, // 2 x diagnostics.sql_stat_reset.interval
	time.Hour*24,
)

// clusterTransactionIsolation is transaction level setting of cluster ,which including UNSPECIFIED, SERIALIZABLE and READ COMMITTED
var clusterTransactionIsolation = settings.RegisterEnumSetting(
	"sql.txn.cluster_transaction_isolation",
	"default cluster transactions isolation",
	"SERIALIZABLE",
	map[int64]string{
		int64(tree.SerializableIsolation):  "SERIALIZABLE",
		int64(tree.RepeatedReadIsolation):  "REPEATABLE READ",
		int64(tree.ReadCommittedIsolation): "READ COMMITTED",
	},
)

// txnIsolationLevelToKV return isolation level through given level, default level and cluster level,
// which priority from high to low. We will choose s lower isolation level is the higher level is
// UnspecifiedIsolation, and the level is SerializableIsolation if all the isolation are UnspecifiedIsolation.
func (ex *connExecutor) txnIsolationLevelToKV(level tree.IsolationLevel) tree.IsolationLevel {
	defaultLevel := tree.IsolationLevel(ex.sessionData.DefaultTxnIsolationLevel)
	if level == tree.UnspecifiedIsolation {
		if defaultLevel != tree.UnspecifiedIsolation {
			return defaultLevel
		}
		return tree.SerializableIsolation
	}
	return level
}

// PeriodicallyClearSQLStats spawns a loop to reset stats based on the setting
// of a given duration settings variable. We take in a function to actually do
// the resetting, as some stats have extra work that needs to be performed
// during the reset. For example, the SQL stats need to dump into the parent
// stats before clearing data fully.
func (s *Server) PeriodicallyClearSQLStats(
	ctx context.Context,
	stopper *stop.Stopper,
	setting *settings.DurationSetting,
	stats *sqlStats,
	reset func(ctx context.Context),
) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		var timer timeutil.Timer
		for {
			s.sqlStats.Lock()
			last := stats.lastReset
			s.sqlStats.Unlock()

			next := last.Add(setting.Get(&s.cfg.Settings.SV))
			wait := next.Sub(timeutil.Now())
			if wait < 0 {
				reset(ctx)
			} else {
				timer.Reset(wait)
				select {
				case <-stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Read = true
				}
			}
		}
	})
}

type closeType int

const (
	normalClose closeType = iota
	panicClose
	// externalTxnClose means that the connExecutor has been used within a
	// higher-level txn (through the InternalExecutor).
	externalTxnClose
)

func (ex *connExecutor) closeWrapper(ctx context.Context, recovered interface{}) {
	if recovered != nil {
		// A warning header guaranteed to go to stderr. This is unanonymized.
		var cutStmt string
		var stmt string
		if ex.curStmt != nil {
			stmt = ex.curStmt.String()
			cutStmt = stmt
		}
		if len(cutStmt) > panicLogOutputCutoffChars {
			cutStmt = cutStmt[:panicLogOutputCutoffChars] + " [...]"
		}

		log.Shout(ctx, log.Severity_ERROR,
			fmt.Sprintf("a SQL panic has occurred while executing %q: %s", cutStmt, recovered))

		ex.close(ctx, panicClose)

		safeErr := AnonymizeStatementsForReporting("executing", stmt, recovered)

		log.ReportPanic(ctx, &ex.server.cfg.Settings.SV, safeErr, 1 /* depth */)

		// Propagate the (sanitized) panic further.
		// NOTE(andrei): It used to be that we sanitized the panic and then a higher
		// layer was in charge of doing the log.ReportPanic() call. Now that the
		// call is above, it's unclear whether we should propagate the original
		// panic or safeErr. I'm propagating safeErr to be on the safe side.
		panic(safeErr)
	}
	// Closing is not cancelable.
	closeCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	ex.close(closeCtx, normalClose)
}

func (ex *connExecutor) close(ctx context.Context, closeType closeType) {
	ex.sessionEventf(ctx, "finishing connExecutor")

	txnEv := noEvent
	if _, noTxn := ex.machine.CurState().(stateNoTxn); !noTxn {
		txnEv = txnRollback
	}

	if closeType == normalClose {
		// We'll cleanup the SQL txn by creating a non-retriable (commit:true) event.
		// This event is guaranteed to be accepted in every state.
		ev := eventNonRetriableErr{IsCommit: fsm.True}
		payload := eventNonRetriableErrPayload{err: pgerror.Newf(pgcode.AdminShutdown,
			"connExecutor closing")}
		if err := ex.machine.ApplyWithPayload(ctx, ev, payload); err != nil {
			log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
		}
	} else if closeType == externalTxnClose {
		ex.state.finishExternalTxn()
	}

	if err := ex.resetExtraTxnState(ctx, ex.server.dbCache, txnEv); err != nil {
		log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
	}

	if ex.hasCreatedTemporarySchema && !ex.server.cfg.TestingKnobs.DisableTempObjectsCleanupOnSessionExit {
		ie := MakeInternalExecutor(ctx, ex.server, MemoryMetrics{}, ex.server.cfg.Settings)
		err := cleanupSessionTempObjects(
			ctx,
			ex.server.cfg.Settings,
			ex.server.cfg.DB,
			&ie,
			ex.sessionID,
		)
		if err != nil {
			log.Errorf(
				ctx,
				"error deleting temporary objects at session close, "+
					"the temp tables deletion job will retry periodically: %s",
				err,
			)
		}
	}

	if closeType != panicClose {
		// Close all statements and prepared portals.
		ex.extraTxnState.prepStmtsNamespace.resetTo(
			ctx, prepStmtNamespace{}, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
			ctx, prepStmtNamespace{}, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
		)
		ex.extraTxnState.prepStmtsNamespaceMemAcc.Close(ctx)
	}

	if ex.sessionTracing.Enabled() {
		if err := ex.sessionTracing.StopTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	if ex.eventLog != nil {
		ex.eventLog.Finish()
		ex.eventLog = nil
	}

	// Stop idle timer if the connExecutor is closed to ensure cancel session
	// is not called.
	ex.mu.IdleInSessionTimeout.Stop()

	if closeType != panicClose {
		ex.state.mon.Stop(ctx)
		ex.sessionMon.Stop(ctx)
		ex.mon.Stop(ctx)
	} else {
		ex.state.mon.EmergencyStop(ctx)
		ex.sessionMon.EmergencyStop(ctx)
		ex.mon.EmergencyStop(ctx)
	}
}

type connExecutor struct {
	_ util.NoCopy

	// The server to which this connExecutor is attached. The reference is used
	// for getting access to configuration settings.
	// Note: do not use server.Metrics directly. Use metrics below instead.
	server *Server

	// The metrics to which the statement metrics should be accounted.
	// This is different whether the executor is for regular client
	// queries or for "internal" queries.
	metrics *Metrics

	// mon tracks memory usage for SQL activity within this session. It
	// is not directly used, but rather indirectly used via sessionMon
	// and state.mon. sessionMon tracks session-bound objects like prepared
	// statements and result sets.
	//
	// The reason why state.mon and mon are split is to enable
	// separate reporting of statistics per transaction and per
	// session. This is because the "interesting" behavior w.r.t memory
	// is typically caused by transactions, not sessions. The reason why
	// sessionMon and mon are split is to enable separate reporting of
	// statistics for result sets (which escape transactions).
	mon        *mon.BytesMonitor
	sessionMon *mon.BytesMonitor
	// memMetrics contains the metrics that statements executed on this connection
	// will contribute to.
	memMetrics MemoryMetrics

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm ClientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	state          txnState
	transitionCtx  transitionCtx
	sessionTracing SessionTracing

	// eventLog for SQL statements and other important session events. Will be set
	// if traceSessionEventLogEnabled; it is used by ex.sessionEventf()
	eventLog trace.EventLog

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried.
	extraTxnState struct {
		// tables collects descriptors used by the current transaction.
		tables TableCollection

		// jobs accumulates jobs staged for execution inside the transaction.
		// Staging happens when executing statements that are implemented with a
		// job. The jobs are staged via the function QueueJob in
		// pkg/sql/planner.go. The staged jobs are executed once the transaction
		// that staged them commits.
		jobs jobsCollection

		// schemaChangeJobsCache is a map of descriptor IDs to Jobs.
		// Used in createOrUpdateSchemaChangeJob so we can check if a job has been
		// queued up for the given ID.
		schemaChangeJobsCache map[sqlbase.ID]*jobs.Job

		// autoRetryCounter keeps track of the which iteration of a transaction
		// auto-retry we're currently in. It's 0 whenever the transaction state is not
		// stateOpen.
		autoRetryCounter int

		// numDDL keeps track of how many DDL statements have been
		// executed so far.
		numDDL int

		// txnRewindPos is the position within stmtBuf to which we'll rewind when
		// performing automatic retries. This is more or less the position where the
		// current transaction started.
		// This field is only defined while in stateOpen.
		//
		// Set via setTxnRewindPos().
		txnRewindPos CmdPos

		// prepStmtNamespace contains the prepared statements and portals that the
		// session currently has access to.
		// Portals are bound to a transaction and they're all destroyed once the
		// transaction finishes.
		// Prepared statements are not transactional and so it's a bit weird that
		// they're part of extraTxnState, but it's convenient to put them here
		// because they need the same kind of "snapshoting" as the portals (see
		// prepStmtsNamespaceAtTxnRewindPos).
		prepStmtsNamespace prepStmtNamespace

		// prepStmtsNamespaceAtTxnRewindPos is a snapshot of the prep stmts/portals
		// (ex.prepStmtsNamespace) before processing the command at position
		// txnRewindPos.
		// Here's the deal: prepared statements are not transactional, but they do
		// need to interact properly with automatic retries (i.e. rewinding the
		// command buffer). When doing a rewind, we need to be able to restore the
		// prep stmts as they were. We do this by taking a snapshot every time
		// txnRewindPos is advanced. Prepared statements are shared between the two
		// collections, but these collections are periodically reconciled.
		prepStmtsNamespaceAtTxnRewindPos prepStmtNamespace

		// prepStmtsNamespaceMemAcc is the memory account that is shared
		// between prepStmtsNamespace and prepStmtsNamespaceAtTxnRewindPos. It
		// tracks the memory usage of portals and should be closed upon
		// connExecutor's closure.
		prepStmtsNamespaceMemAcc mon.BoundAccount

		// onTxnFinish (if non-nil) will be called when txn is finished (either
		// committed or aborted). It is set when txn is started but can remain
		// unset when txn is executed within another higher-level txn.
		onTxnFinish func(txnEvent)

		// savepoints maintains the stack of savepoints currently open.
		savepoints savepointStack
		// savepointsAtTxnRewindPos is a snapshot of the savepoints stack before
		// processing the command at position txnRewindPos. When rewinding, we're
		// going to restore this snapshot.
		savepointsAtTxnRewindPos savepointStack
	}

	// sessionData contains the user-configurable connection variables.
	sessionData *sessiondata.SessionData
	// dataMutator is nil for session-bound internal executors; we shouldn't issue
	// statements that manipulate session state to an internal executor.
	dataMutator *sessionDataMutator
	// appStats tracks per-application SQL usage statistics. It is maintained to
	// represent statistrics for the application currently identified by
	// sessiondata.ApplicationName.
	appStats *appStats
	// applicationName is the same as sessionData.ApplicationName. It's copied
	// here as an atomic so that it can be read concurrently by serialize().
	applicationName atomic.Value

	// ctxHolder contains the connection's context in which all command executed
	// on the connection are running. This generally should not be used directly,
	// but through the Ctx() method; if we're inside a transaction, Ctx() is going
	// to return a derived context. See the Context Management comments at the top
	// of the file.
	ctxHolder ctxHolder

	// onCancelSession is called when the SessionRegistry is cancels this session.
	// For pgwire connections, this is hooked up to canceling the connection's
	// context.
	// If nil, canceling this session will be a no-op.
	onCancelSession context.CancelFunc

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner
	// phaseTimes tracks session- and transaction-level phase times. It is
	// copied-by-value when resetting statsCollector before executing each
	// statement.
	phaseTimes phaseTimes

	// statsCollector is used to collect statistics about SQL statements and
	// transactions.
	statsCollector *sqlStatsCollector

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[ClusterWideID]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement

		// IdleInSessionTimeout is returned by the AfterFunc call that cancels the
		// session if the idle time exceeds the idle_in_session_timeout.
		IdleInSessionTimeout timeutil.Timeout
	}

	// curStmt is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmt tree.Statement

	sessionID ClusterWideID

	// activated determines whether activate() was called already.
	// When this is set, close() must be called to release resources.
	activated bool

	// draining is set if we've received a DrainRequest. Once this is set, we're
	// going to find a suitable time to close the connection.
	draining bool

	// executorType is set to whether this executor is an ordinary executor which
	// responds to user queries or an internal one.
	executorType executorType

	// hasCreatedTemporarySchema is set if the executor has created a
	// temporary schema, which requires special cleanup on close.
	hasCreatedTemporarySchema bool

	// stmtDiagnosticsRecorder is used to track which queries need to have
	// information collected.
	stmtDiagnosticsRecorder StmtDiagnosticsRecorder
}

// ctxHolder contains a connection's context and, while session tracing is
// enabled, a derived context with a recording span. The connExecutor should use
// the latter while session tracing is active, or the former otherwise; that's
// what the ctx() method returns.
type ctxHolder struct {
	connCtx           context.Context
	sessionTracingCtx context.Context
}

func (ch *ctxHolder) ctx() context.Context {
	if ch.sessionTracingCtx != nil {
		return ch.sessionTracingCtx
	}
	return ch.connCtx
}

func (ch *ctxHolder) hijack(sessionTracingCtx context.Context) {
	if ch.sessionTracingCtx != nil {
		panic("hijack already in effect")
	}
	ch.sessionTracingCtx = sessionTracingCtx
}

func (ch *ctxHolder) unhijack() {
	if ch.sessionTracingCtx == nil {
		panic("hijack not in effect")
	}
	ch.sessionTracingCtx = nil
}

type prepStmtNamespace struct {
	// prepStmts contains the prepared statements currently available on the
	// session.
	prepStmts map[string]*PreparedStatement
	// portals contains the portals currently available on the session.
	portals map[string]PreparedPortal
}

func (ns prepStmtNamespace) String() string {
	var sb strings.Builder
	sb.WriteString("Prep stmts: ")
	for name := range ns.prepStmts {
		sb.WriteString(name + " ")
	}
	sb.WriteString("Portals: ")
	for name := range ns.portals {
		sb.WriteString(name + " ")
	}
	return sb.String()
}

// resetTo resets a namespace to equate another one (`to`). All the receiver's
// references are release and all the to's references are duplicated.
//
// An empty `to` can be passed in to deallocate everything.
func (ns *prepStmtNamespace) resetTo(
	ctx context.Context, to prepStmtNamespace, prepStmtsNamespaceMemAcc *mon.BoundAccount,
) {
	for name, p := range ns.prepStmts {
		p.decRef(ctx)
		delete(ns.prepStmts, name)
	}
	for name, p := range ns.portals {
		p.decRef(ctx, prepStmtsNamespaceMemAcc, name)
		delete(ns.portals, name)
	}

	for name, ps := range to.prepStmts {
		ps.incRef(ctx)
		ns.prepStmts[name] = ps
	}
	for name, p := range to.portals {
		p.incRef(ctx)
		ns.portals[name] = p
	}
}

// resetExtraTxnState resets the fields of ex.extraTxnState when a transaction
// commits, rolls back or restarts.
func (ex *connExecutor) resetExtraTxnState(
	ctx context.Context, dbCacheHolder *databaseCacheHolder, ev txnEvent,
) error {
	ex.extraTxnState.jobs = nil

	for k := range ex.extraTxnState.schemaChangeJobsCache {
		delete(ex.extraTxnState.schemaChangeJobsCache, k)
	}

	ex.extraTxnState.tables.releaseTables(ctx)

	ex.extraTxnState.tables.databaseCache = dbCacheHolder.getDatabaseCache()

	// Close all portals.
	for name, p := range ex.extraTxnState.prepStmtsNamespace.portals {
		p.decRef(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
		delete(ex.extraTxnState.prepStmtsNamespace.portals, name)
	}

	switch ev {
	case txnCommit, txnRollback:
		ex.extraTxnState.savepoints.clear()
		// After txn is finished, we need to call onTxnFinish (if it's non-nil).
		if ex.extraTxnState.onTxnFinish != nil {
			ex.extraTxnState.onTxnFinish(ev)
			ex.extraTxnState.onTxnFinish = nil
		}
	}
	// NOTE: on txnRestart we don't need to muck with the savepoints stack. It's either a
	// a ROLLBACK TO SAVEPOINT that generated the event, and that statement deals with the
	// savepoints, or it's a rewind which also deals with them.

	return nil
}

// Ctx returns the transaction's ctx, if we're inside a transaction, or the
// session's context otherwise.
func (ex *connExecutor) Ctx() context.Context {
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		return ex.ctxHolder.ctx()
	}
	// stateInternalError is used by the InternalExecutor.
	if _, ok := ex.machine.CurState().(stateInternalError); ok {
		return ex.ctxHolder.ctx()
	}
	return ex.state.Ctx
}

// activate engages the use of resources that must be cleaned up
// afterwards. after activate() completes, the close() method must be
// called.
//
// Args:
// parentMon: The root monitor.
// reserved: Memory reserved for the connection. The connExecutor takes
//
//	ownership of this memory.
func (ex *connExecutor) activate(
	ctx context.Context, parentMon *mon.BytesMonitor, reserved mon.BoundAccount,
) {
	// Note: we pass `reserved` to sessionRootMon where it causes it to act as a
	// buffer. This is not done for sessionMon nor state.mon: these monitors don't
	// start with any buffer, so they'll need to ask their "parent" for memory as
	// soon as the first allocation. This is acceptable because the session is
	// single threaded, and the point of buffering is just to avoid contention.
	ex.mon.Start(ctx, parentMon, reserved)
	ex.sessionMon.Start(ctx, ex.mon, mon.BoundAccount{})

	// Enable the trace if configured.
	if traceSessionEventLogEnabled.Get(&ex.server.cfg.Settings.SV) {
		remoteStr := "<admin>"
		if ex.sessionData.RemoteAddr != nil {
			remoteStr = ex.sessionData.RemoteAddr.String()
		}
		ex.eventLog = trace.NewEventLog(
			fmt.Sprintf("sql session [%s]", ex.sessionData.User), remoteStr)
	}

	ex.activated = true
}

// run implements the run loop for a connExecutor. Commands are read one by one
// from the input buffer; they are executed and the resulting state transitions
// are performed.
//
// run returns when either the stmtBuf is closed by someone else or when an
// error is propagated from query execution. Note that query errors are not
// propagated as errors to this layer; only things that are supposed to
// terminate the session are (e.g. client communication errors and ctx
// cancelations).
// run() is expected to react on ctx cancelation, but the caller needs to also
// close the stmtBuf at the same time as canceling the ctx. If cancelation
// happens in the middle of a query execution, that's expected to interrupt the
// execution and generate an error. run() is then supposed to return because the
// buffer is closed and no further commands can be read.
//
// When this returns, ex.close() needs to be called and  the connection to the
// client needs to be terminated. If it returns with an error, that error may
// represent a communication error (in which case the connection might already
// also have an error from the reading side), or some other unexpected failure.
// Returned errors have not been communicated to the client: it's up to the
// caller to do that if it wants.
//
// If not nil, reserved represents Memory reserved for the connection. The
// connExecutor takes ownership of this memory.
//
// onCancel, if not nil, will be called when the SessionRegistry cancels the
// session. TODO(andrei): This is hooked up to canceling the pgwire connection's
// context (of which ctx is also a child). It seems uncouth for the connExecutor
// to cancel a higher-level task. A better design would probably be for pgwire
// to own the SessionRegistry, instead of it being owned by the sql.Server -
// then pgwire would directly cancel its own tasks; the sessions also more
// naturally belong there. There is a problem, however, as query cancelation (as
// opposed to session cancelation) is done through the SessionRegistry and that
// does belong with the connExecutor. Introducing a query registry, separate
// from the session registry, might be too costly - the way query cancelation
// works is that every session is asked to cancel a given query until the right
// one is found. That seems like a good performance trade-off.
func (ex *connExecutor) run(
	ctx context.Context,
	parentMon *mon.BytesMonitor,
	reserved mon.BoundAccount,
	onCancel context.CancelFunc,
) error {
	if !ex.activated {
		ex.activate(ctx, parentMon, reserved)
	}
	ex.ctxHolder.connCtx = ctx
	ex.onCancelSession = onCancel

	ex.sessionID = ex.generateID()
	ex.server.cfg.SessionRegistry.register(ex.sessionID, ex)
	ex.planner.extendedEvalCtx.setSessionID(ex.sessionID)
	defer ex.server.cfg.SessionRegistry.deregister(ex.sessionID)

	for {
		ex.curStmt = nil
		if err := ctx.Err(); err != nil {
			return err
		}

		var err error
		if err = ex.execCmd(ex.Ctx()); err != nil {
			if err == io.EOF || err == errDrainingComplete {
				return nil
			}
			return err
		}
	}
}

// errDrainingComplete is returned by execCmd when the connExecutor previously got
// a DrainRequest and the time is ripe to finish this session (i.e. we're no
// longer in a transaction).
var errDrainingComplete = fmt.Errorf("draining done. this is a good time to finish this session")

// execCmd reads the current command from the stmtBuf and executes it. The
// transaction state is modified accordingly, and the stmtBuf is advanced or
// rewinded accordingly.
//
// Returns an error if communication of results to the client has failed and the
// session should be terminated. Returns io.EOF if the stmtBuf has been closed.
// Returns drainingComplete if the session should finish because draining is
// complete (i.e. we received a DrainRequest - possibly previously - and the
// connection is found to be idle).
func (ex *connExecutor) execCmd(ctx context.Context) error {
	cmd, pos, err := ex.stmtBuf.CurCmd()
	if err != nil {
		return err // err could be io.EOF
	}

	ctx, sp := tracing.EnsureChildSpan(
		ctx, ex.server.cfg.AmbientCtx.Tracer,
		// We print the type of command, not the String() which includes long
		// statements.
		cmd.command())
	defer sp.Finish()

	if log.ExpensiveLogEnabled(ctx, 2) || ex.eventLog != nil {
		ex.sessionEventf(ctx, "[%s pos:%d] executing %s",
			ex.machine.CurState(), pos, cmd)
	}

	var ev fsm.Event
	var payload fsm.EventPayload
	var res ResultBase

	switch tcmd := cmd.(type) {
	case ExecStmt:
		if tcmd.AST == nil {
			res = ex.clientComm.CreateEmptyQueryResult(pos)
			break
		}
		ex.curStmt = tcmd.AST

		stmtRes := ex.clientComm.CreateStatementResult(
			tcmd.AST,
			NeedRowDesc,
			pos,
			nil, /* formatCodes */
			ex.sessionData.DataConversion,
			0,  /* limit */
			"", /* portalName */
			ex.implicitTxn(),
		)

		curStmt := Statement{Statement: tcmd.Statement}
		res = stmtRes

		ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
		ex.phaseTimes[sessionStartParse] = tcmd.ParseStart
		ex.phaseTimes[sessionEndParse] = tcmd.ParseEnd

		if !curStmt.Insertdirectstmt.InsertFast {
			stmtCtx := withStatement(ctx, ex.curStmt)
			ev, payload, err = ex.execStmt(stmtCtx, curStmt, stmtRes, nil /* pinfo */)
			if payload != nil {
				if p, ok := payload.(*eventRetriableErrPayload); ok {
					log.Warningf(ctx, "retry error=%v\n ; curStmt=%s\n", p.err, curStmt.SQL)
				}
			}
		} else {
			ex.sendDedupClientNoticeToRes(ctx, stmtRes, curStmt.Insertdirectstmt.UseDeepRule, curStmt.Insertdirectstmt.DedupRule, curStmt.Insertdirectstmt.DedupRows)
			err = curStmt.Insertdirectstmt.ErrorInfo
			if curStmt.Insertdirectstmt.IgnoreBatcherror && curStmt.Insertdirectstmt.BatchFailed+curStmt.Insertdirectstmt.BatchFailedColumn != 0 {
				ex.sendBatchErrorToRes(stmtRes, curStmt.Insertdirectstmt.BatchFailed+curStmt.Insertdirectstmt.BatchFailedColumn)
			}
			stmtRes.IncrementRowsAffected(int(curStmt.Insertdirectstmt.RowsAffected - curStmt.Insertdirectstmt.DedupRows))
		}
		if err != nil {
			return err
		}
	case ExecPortal:
		// ExecPortal is handled like ExecStmt, except that the placeholder info
		// is taken from the portal.

		portalName := tcmd.Name
		portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]
		if !ok {
			err := pgerror.Newf(pgcode.InvalidCursorName, "unknown portal %q", portalName)
			ev = eventNonRetriableErr{IsCommit: fsm.False}
			payload = eventNonRetriableErrPayload{err: err}
			res = ex.clientComm.CreateErrorResult(pos)
			break
		}
		if portal.Stmt.AST == nil {
			res = ex.clientComm.CreateEmptyQueryResult(pos)
			break
		}

		if log.ExpensiveLogEnabled(ctx, 2) {
			log.VEventf(ctx, 2, "portal resolved to: %s", portal.Stmt.AST.String())
		}
		ex.curStmt = portal.Stmt.AST

		pinfo := &tree.PlaceholderInfo{
			PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
				TypeHints: portal.Stmt.TypeHints,
				Types:     portal.Stmt.Types,
			},
			Values: portal.Qargs,
		}

		ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
		// When parsing has been done earlier, via a separate parse
		// message, it is not any more part of the statistics collected
		// for this execution. In that case, we simply report that
		// parsing took no time.
		ex.phaseTimes[sessionStartParse] = time.Time{}
		ex.phaseTimes[sessionEndParse] = time.Time{}

		stmtRes := ex.clientComm.CreateStatementResult(
			portal.Stmt.AST,
			// The client is using the extended protocol, so no row description is
			// needed.
			DontNeedRowDesc,
			pos, portal.OutFormats,
			ex.sessionData.DataConversion,
			tcmd.Limit,
			portalName,
			ex.implicitTxn(),
		)
		ctx, sp := tracing.EnsureChildSpan(
			ctx, ex.server.cfg.AmbientCtx.Tracer,
			// We print the type of command, not the String() which includes long
			// statements.
			cmd.command())
		defer sp.Finish()

		if portal.Stmt.Insertdirectstmt.InsertFast {
			if !portal.Stmt.PrepareInsertDirect.EvalContext.StartSinglenode {
				ex.planner.ExtendedEvalContext().StartDistributeMode = true
			}
			starttime := timeutil.Now()
			ex.metrics.StartedStatementCounters.QueryCount.Inc()
			ex.metrics.StartedStatementCounters.InsertCount.Inc()
			ex.SendDirectTsInsert(ctx, &portal.Stmt.PrepareInsertDirect.EvalContext, portal.Stmt.PrepareInsertDirect.stmtRes, portal.Stmt.PrepareInsertDirect.payloadNodeMap)
			if err = portal.Stmt.Insertdirectstmt.ErrorInfo; err != nil {
				return err
			}
			if err = portal.Stmt.PrepareInsertDirect.stmtRes.Err(); err != nil {
				return err
			}
			stmtRes.IncrementRowsAffected(int(portal.Stmt.Insertdirectstmt.RowsAffected))

			var tempStmt Statement
			var flags planFlags
			tempStmt.AST = portal.Stmt.AST
			user := ex.sessionData.User
			database := ex.sessionData.Database
			ex.statsCollector.recordStatement(
				&tempStmt, nil,
				flags.IsSet(planFlagDistributed), flags.IsSet(planFlagImplicitTxn),
				0, int(portal.Stmt.Insertdirectstmt.RowsAffected), nil,
				0, 0, timeutil.Since(starttime).Seconds(), 0, 0, 0, 0,
				user, database,
			)
			ex.metrics.ExecutedStatementCounters.QueryCount.Inc()
			ex.metrics.ExecutedStatementCounters.InsertCount.Inc()
		}

		if stmtRes.IsLimit() && stmtRes.StmtType() != tree.Ack {
			//ex.server.cfg.DistSQLPlanner.distSQLSrv.CommandLimit = tcmd.Limit
			ex.sessionData.CommandLimit = int64(tcmd.Limit)
		}
		var isBinary bool
		if portal.OutFormats != nil {
			for _, out := range portal.OutFormats {
				if out == pgwirebase.FormatBinary {
					isBinary = true
					break
				}
			}
		}
		ex.sessionData.OutFormats = isBinary
		res = stmtRes
		if portal.Stmt.Statement.Insertdirectstmt.InsertFast {
			break
		}
		ev, payload, err = ex.execPortal(ctx, portal, portalName, stmtRes, pinfo)
		if err != nil {
			return err
		}
	case PrepareStmt:
		ex.curStmt = tcmd.AST
		res = ex.clientComm.CreatePrepareResult(pos)
		stmtCtx := withStatement(ctx, ex.curStmt)
		ev, payload = ex.execPrepare(stmtCtx, tcmd)
	case DescribeStmt:
		descRes := ex.clientComm.CreateDescribeResult(pos)
		res = descRes
		ev, payload = ex.execDescribe(ctx, tcmd, descRes)
	case BindStmt:
		res = ex.clientComm.CreateBindResult(pos)
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[tcmd.PreparedStatementName]
		if ok && ps.Statement.Insertdirectstmt.InsertFast {
			ev, payload = ex.execPreparedirectBind(ctx, tcmd)
		} else {
			ev, payload = ex.execBind(ctx, tcmd)
		}
	case DeletePreparedStmt:
		res = ex.clientComm.CreateDeleteResult(pos)
		ev, payload = ex.execDelPrepStmt(ctx, tcmd)
	case SendError:
		res = ex.clientComm.CreateErrorResult(pos)
		ev = eventNonRetriableErr{IsCommit: fsm.False}
		payload = eventNonRetriableErrPayload{err: tcmd.Err}
	case Sync:
		// Note that the Sync result will flush results to the network connection.
		res = ex.clientComm.CreateSyncResult(pos)
		if ex.draining {
			// If we're draining, check whether this is a good time to finish the
			// connection. If we're not inside a transaction, we stop processing
			// now. If we are inside a transaction, we'll check again the next time
			// a Sync is processed.
			if ex.idleConn() {
				// If we're about to close the connection, close res in order to flush
				// now, as we won't have an opportunity to do it later.
				res.Close(ctx, stateToTxnStatusIndicator(ex.machine.CurState()))
				return errDrainingComplete
			}
		}
	case CopyIn:
		res = ex.clientComm.CreateCopyInResult(pos)
		var err error
		ev, payload, err = ex.execCopyIn(ctx, tcmd)
		if err != nil {
			return err
		}
	case DrainRequest:
		// We received a drain request. We terminate immediately if we're not in a
		// transaction. If we are in a transaction, we'll finish as soon as a Sync
		// command (i.e. the end of a batch) is processed outside of a
		// transaction.
		ex.draining = true
		res = ex.clientComm.CreateDrainResult(pos)
		if ex.idleConn() {
			return errDrainingComplete
		}
	case Flush:
		// Closing the res will flush the connection's buffer.
		res = ex.clientComm.CreateFlushResult(pos)
	default:
		panic(fmt.Sprintf("unsupported command type: %T", cmd))
	}

	var advInfo advanceInfo

	// If an event was generated, feed it to the state machine.
	if ev != nil {
		var err error
		advInfo, err = ex.txnStateTransitionsApplyWrapper(ev, payload, res, pos)
		if err != nil {
			return err
		}
	} else {
		// If no event was generated synthesize an advance code.
		advInfo = advanceInfo{
			code: advanceOne,
		}
	}

	// Decide if we need to close the result or not. We don't need to do it if
	// we're staying in place or rewinding - the statement will be executed
	// again.
	if advInfo.code != stayInPlace && advInfo.code != rewind {
		// Close the result. In case of an execution error, the result might have
		// its error set already or it might not.
		resErr := res.Err()

		pe, ok := payload.(payloadWithError)
		if ok {
			ex.sessionEventf(ctx, "execution error: %s", pe.errorCause())
			if resErr == nil {
				res.SetError(pe.errorCause())
			}
		}
		res.Close(ctx, stateToTxnStatusIndicator(ex.machine.CurState()))
	} else {
		res.Discard()
	}

	// Move the cursor according to what the state transition told us to do.
	switch advInfo.code {
	case advanceOne:
		ex.stmtBuf.AdvanceOne()
	case skipBatch:
		// We'll flush whatever results we have to the network. The last one must
		// be an error. This flush may seem unnecessary, as we generally only
		// flush when the client requests it through a Sync or a Flush but without
		// it the Node.js driver isn't happy. That driver likes to send "flush"
		// command and only sends Syncs once it received some data. But we ignore
		// flush commands (just like we ignore any other commands) when skipping
		// to the next batch.
		if err := ex.clientComm.Flush(pos); err != nil {
			return err
		}
		if err := ex.stmtBuf.seekToNextBatch(); err != nil {
			return err
		}
	case rewind:
		ex.rewindPrepStmtNamespace(ctx)
		ex.extraTxnState.savepoints = ex.extraTxnState.savepointsAtTxnRewindPos
		advInfo.rewCap.rewindAndUnlock(ctx)
	case stayInPlace:
		// Nothing to do. The same statement will be executed again.
	default:
		panic(fmt.Sprintf("unexpected advance code: %s", advInfo.code))
	}

	if err := ex.updateTxnRewindPosMaybe(ctx, cmd, pos, advInfo); err != nil {
		return err
	}

	if rewindCapability, canRewind := ex.getRewindTxnCapability(); !canRewind {
		// Trim statements that cannot be retried to reclaim memory.
		ex.stmtBuf.ltrim(ctx, pos)
	} else {
		rewindCapability.close()
	}

	if ex.server.cfg.TestingKnobs.AfterExecCmd != nil {
		ex.server.cfg.TestingKnobs.AfterExecCmd(ctx, cmd, ex.stmtBuf)
	}

	return nil
}

func (ex *connExecutor) idleConn() bool {
	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		return true
	case stateInternalError:
		return true
	default:
		return false
	}
}

// updateTxnRewindPosMaybe checks whether the ex.extraTxnState.txnRewindPos
// should be advanced, based on the advInfo produced by running cmd at position
// pos.
func (ex *connExecutor) updateTxnRewindPosMaybe(
	ctx context.Context, cmd Command, pos CmdPos, advInfo advanceInfo,
) error {
	// txnRewindPos is only maintained while in stateOpen.
	if _, ok := ex.machine.CurState().(stateOpen); !ok {
		return nil
	}
	if advInfo.txnEvent == txnRestart {
		txn := ex.planner.Txn()
		if err := txn.PrepareForRetry(ctx, nil); err != nil {
			return err
		}
	}
	if advInfo.txnEvent == txnStart || advInfo.txnEvent == txnRestart {
		var nextPos CmdPos
		switch advInfo.code {
		case stayInPlace:
			nextPos = pos
		case advanceOne:
			// Future rewinds will refer to the next position; the statement that
			// started the transaction (i.e. BEGIN) will not be itself be executed
			// again.
			nextPos = pos + 1
		case rewind:
			if advInfo.rewCap.rewindPos != ex.extraTxnState.txnRewindPos {
				return errors.AssertionFailedf(
					"unexpected rewind position: %d when txn start is: %d",
					errors.Safe(advInfo.rewCap.rewindPos),
					errors.Safe(ex.extraTxnState.txnRewindPos))
			}
			// txnRewindPos stays unchanged.
			return nil
		default:
			return errors.AssertionFailedf(
				"unexpected advance code when starting a txn: %s",
				errors.Safe(advInfo.code))
		}
		ex.setTxnRewindPos(ctx, nextPos)
	} else {
		// See if we can advance the rewind point even if this is not the point
		// where the transaction started. We can do that after running a special
		// statement (e.g. SET TRANSACTION or SAVEPOINT) or after most commands that
		// don't execute statements.
		// The idea is that, for example, we don't want the following sequence to
		// disable retries for what comes after the sequence:
		// 1: PrepareStmt BEGIN
		// 2: BindStmt
		// 3: ExecutePortal
		// 4: Sync

		// Note that the current command cannot influence the rewind point if
		// if the rewind point is not current set to the command's position
		// (i.e. we don't do anything if txnRewindPos != pos).

		if advInfo.code != advanceOne {
			panic(fmt.Sprintf("unexpected advanceCode: %s", advInfo.code))
		}

		var canAdvance bool
		_, inOpen := ex.machine.CurState().(stateOpen)
		if inOpen && (ex.extraTxnState.txnRewindPos == pos) {
			switch tcmd := cmd.(type) {
			case ExecStmt:
				canAdvance = ex.stmtDoesntNeedRetry(tcmd.AST)
			case ExecPortal:
				portal := ex.extraTxnState.prepStmtsNamespace.portals[tcmd.Name]
				canAdvance = ex.stmtDoesntNeedRetry(portal.Stmt.AST)
			case PrepareStmt:
				canAdvance = true
			case DescribeStmt:
				canAdvance = true
			case BindStmt:
				canAdvance = true
			case DeletePreparedStmt:
				canAdvance = true
			case SendError:
				canAdvance = true
			case Sync:
				canAdvance = true
			case CopyIn:
				// Can't advance.
			case DrainRequest:
				canAdvance = true
			case Flush:
				canAdvance = true
			default:
				panic(fmt.Sprintf("unsupported cmd: %T", cmd))
			}
			if canAdvance {
				ex.setTxnRewindPos(ctx, pos+1)
			}
		}
	}
	return nil
}

// setTxnRewindPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnRewindPos(ctx context.Context, pos CmdPos) {
	if pos <= ex.extraTxnState.txnRewindPos {
		panic(fmt.Sprintf("can only move the  txnRewindPos forward. "+
			"Was: %d; new value: %d", ex.extraTxnState.txnRewindPos, pos))
	}
	ex.extraTxnState.txnRewindPos = pos
	ex.stmtBuf.ltrim(ctx, pos)
	ex.commitPrepStmtNamespace(ctx)
	ex.extraTxnState.savepointsAtTxnRewindPos = ex.extraTxnState.savepoints.clone()
}

// stmtDoesntNeedRetry returns true if the given statement does not need to be
// retried when performing automatic retries. This means that the results of the
// statement do not change with retries.
func (ex *connExecutor) stmtDoesntNeedRetry(stmt tree.Statement) bool {
	wrap := Statement{Statement: parser.Statement{AST: stmt}}
	return isSavepoint(wrap) || isSetTransaction(wrap)
}

func stateToTxnStatusIndicator(s fsm.State) TransactionStatusIndicator {
	switch s.(type) {
	case stateOpen:
		return InTxnBlock
	case stateAborted:
		return InFailedTxnBlock
	case stateNoTxn:
		return IdleTxnBlock
	case stateCommitWait:
		return InTxnBlock
	case stateInternalError:
		return InTxnBlock
	default:
		panic(fmt.Sprintf("unknown state: %T", s))
	}
}

// We handle the CopyFrom statement by creating a copyMachine and handing it
// control over the connection until the copying is done. The contract is that,
// when this is called, the pgwire.conn is not reading from the network
// connection any more until this returns. The copyMachine will to the reading
// and writing up to the CommandComplete message.
func (ex *connExecutor) execCopyIn(
	ctx context.Context, cmd CopyIn,
) (fsm.Event, fsm.EventPayload, error) {

	// When we're done, unblock the network connection.
	defer cmd.CopyDone.Done()

	state := ex.machine.CurState()
	_, isNoTxn := state.(stateNoTxn)
	_, isOpen := state.(stateOpen)
	if !isNoTxn && !isOpen {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */)}
		return ev, payload, nil
	}

	// If we're in an explicit txn, then the copying will be done within that
	// txn. Otherwise, we tell the copyMachine to manage its own transactions
	// and give it a closure to reset the accumulated extraTxnState.
	var txnOpt copyTxnOpt
	if isOpen {
		txnOpt = copyTxnOpt{
			txn:           ex.state.mu.txn,
			txnTimestamp:  ex.state.sqlTimestamp,
			stmtTimestamp: ex.server.cfg.Clock.PhysicalTime(),
		}
	} else {
		txnOpt = copyTxnOpt{
			resetExtraTxnState: func(ctx context.Context) error {
				return ex.resetExtraTxnState(ctx, ex.server.dbCache, noEvent)
			},
		}
	}

	var monToStop *mon.BytesMonitor
	defer func() {
		if monToStop != nil {
			monToStop.Stop(ctx)
		}
	}()
	if isNoTxn {
		// HACK: We're reaching inside ex.state and starting the monitor. Normally
		// that's driven by the state machine, but we're bypassing the state machine
		// here.
		ex.state.mon.Start(ctx, ex.sessionMon, mon.BoundAccount{} /* reserved */)
		monToStop = ex.state.mon
	}
	txnOpt.resetPlanner = func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time) {
		// HACK: We're reaching inside ex.state and changing sqlTimestamp by hand.
		// It is used by resetPlanner. Normally sqlTimestamp is updated by the
		// state machine, but the copyMachine manages its own transactions without
		// going through the state machine.
		ex.state.sqlTimestamp = txnTS
		ex.statsCollector = ex.newStatsCollector()
		ex.statsCollector.reset(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
		ex.initPlanner(ctx, p)
		ex.resetPlanner(ctx, p, txn, stmtTS)
	}
	var cm copyMachineInterface
	var err error
	if table := cmd.Stmt.Table; table.Table() == fileUploadTable && table.Schema() == kwdbInternalName {
		cm, err = newFileUploadMachine(ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg)
	} else {
		cm, err = newCopyMachine(
			ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg,
			// execInsertPlan
			func(ctx context.Context, p *planner, res RestrictedCommandResult) error {
				_, _, err := ex.execWithDistSQLEngine(ctx, p, tree.RowsAffected, res, false /* distribute */, nil /* progressAtomic */, "")
				return err
			},
		)
	}
	if err != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
	}
	if err := cm.run(ctx); err != nil {
		// TODO(andrei): We don't have a retriable error story for the copy machine.
		// When running outside of a txn, the copyMachine should probably do retries
		// internally. When not, it's unclear what we should do. For now, we abort
		// the txn (if any).
		// We also don't have a story for distinguishing communication errors (which
		// should terminate the connection) from query errors. For now, we treat all
		// errors as query errors.
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
	}
	return nil, nil, nil
}

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementType() != tree.Rows
}

// generateID generates a unique ID based on the node's ID and its current HLC
// timestamp. These IDs are either scoped at the query level or at the session
// level.
func (ex *connExecutor) generateID() ClusterWideID {
	return GenerateClusterWideID(ex.server.cfg.Clock.Now(), ex.server.cfg.NodeID.Get())
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnRewindPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) {
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespace, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnRewindPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) {
	ex.extraTxnState.prepStmtsNamespace.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos, &ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
}

// getRewindTxnCapability checks whether rewinding to the position previously
// set through setTxnRewindPos() is possible and, if it is, returns a
// rewindCapability bound to that position. The returned bool is true if the
// rewind is possible. If it is, client communication is blocked until the
// rewindCapability is exercised.
func (ex *connExecutor) getRewindTxnCapability() (rewindCapability, bool) {
	cl := ex.clientComm.LockCommunication()

	// If we already delivered results at or past the start position, we can't
	// rewind.
	if cl.ClientPos() >= ex.extraTxnState.txnRewindPos {
		cl.Close()
		return rewindCapability{}, false
	}
	return rewindCapability{
		cl:        cl,
		buf:       ex.stmtBuf,
		rewindPos: ex.extraTxnState.txnRewindPos,
	}, true
}

// isCommit returns true if stmt is a "COMMIT" statement.
func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}

func errIsRetriable(err error) bool {
	err = errors.UnwrapAll(err)
	_, retriable := err.(*roachpb.TransactionRetryWithProtoRefreshError)
	return retriable
}

// makeErrEvent takes an error and returns either an eventRetriableErr or an
// eventNonRetriableErr, depending on the error type.
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventPayload) {
	retriable := errIsRetriable(err)
	if retriable {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(isCommit(stmt)),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		payload := eventRetriableErrPayload{
			err:    err,
			rewCap: rc,
		}
		return ev, payload
	}
	ev := eventNonRetriableErr{
		IsCommit: fsm.FromBool(isCommit(stmt)),
	}
	payload := eventNonRetriableErrPayload{err: err}
	return ev, payload
}

// setTransactionModes implements the txnModesSetter interface.
func (ex *connExecutor) setTransactionModes(
	modes tree.TransactionModes, asOfTs hlc.Timestamp,
) error {
	// This method cheats and manipulates ex.state directly, not through an event.
	// The alternative would be to create a special event, but it's unclear how
	// that'd work given that this method is called while executing a statement.

	// Transform the transaction options into the types needed by the state
	// machine.
	if modes.UserPriority != tree.UnspecifiedUserPriority {
		pri, err := priorityToProto(modes.UserPriority)
		if err != nil {
			return err
		}
		if err := ex.state.setPriority(pri); err != nil {
			return err
		}
	}
	// set isolation level of txn
	if modes.Isolation != tree.UnspecifiedIsolation {
		level := ex.txnIsolationLevelToKV(modes.Isolation)
		if err := ex.state.setIsolationLevel(enginepb.Level(level)); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.ActiveSQLTransaction)
		}
	}
	rwMode := modes.ReadWriteMode
	if modes.AsOf.Expr != nil && (asOfTs == hlc.Timestamp{}) {
		return errors.AssertionFailedf("expected an evaluated AS OF timestamp")
	}
	if (asOfTs != hlc.Timestamp{}) {
		ex.state.setHistoricalTimestamp(ex.Ctx(), asOfTs)
		ex.state.sqlTimestamp = asOfTs.GoTime()
		if rwMode == tree.UnspecifiedReadWriteMode {
			rwMode = tree.ReadOnly
		}
	}
	if modes.Name != "" {
		if err := ex.state.setDebugName(modes.Name); err != nil {
			return err
		}
	}
	return ex.state.setReadOnlyMode(rwMode)
}

func priorityToProto(mode tree.UserPriority) (roachpb.UserPriority, error) {
	var pri roachpb.UserPriority
	switch mode {
	case tree.UnspecifiedUserPriority:
		pri = roachpb.NormalUserPriority
	case tree.Low:
		pri = roachpb.MinUserPriority
	case tree.Normal:
		pri = roachpb.NormalUserPriority
	case tree.High:
		pri = roachpb.MaxUserPriority
	default:
		return roachpb.UserPriority(0), errors.AssertionFailedf("unknown user priority: %s", errors.Safe(mode))
	}
	return pri, nil
}

func (ex *connExecutor) readWriteModeWithSessionDefault(
	mode tree.ReadWriteMode,
) tree.ReadWriteMode {
	if mode == tree.UnspecifiedReadWriteMode {
		if ex.sessionData.DefaultReadOnly {
			return tree.ReadOnly
		}
		return tree.ReadWrite
	}
	return mode
}

// initEvalCtx initializes the fields of an extendedEvalContext that stay the
// same across multiple statements. resetEvalCtx must also be called before each
// statement, to reinitialize other fields.
func (ex *connExecutor) initEvalCtx(ctx context.Context, evalCtx *extendedEvalContext, p *planner) {
	scInterface := newSchemaInterface(&ex.extraTxnState.tables, ex.server.cfg.VirtualSchemas)

	ie := MakeInternalExecutor(
		ctx,
		ex.server,
		ex.memMetrics,
		ex.server.cfg.Settings,
	)
	ie.SetSessionData(ex.sessionData)

	*evalCtx = extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:            p,
			Sequence:           p,
			SessionData:        ex.sessionData,
			SessionAccessor:    p,
			PrivilegedAccessor: p,
			ClientNoticeSender: p,
			Settings:           ex.server.cfg.Settings,
			TestingKnobs:       ex.server.cfg.EvalContextTestingKnobs,
			ClusterID:          ex.server.cfg.ClusterID(),
			ClusterName:        ex.server.cfg.RPCContext.ClusterName(),
			NodeID:             ex.server.cfg.NodeID.Get(),
			Locality:           ex.server.cfg.Locality,
			ReCache:            ex.server.reCache,
			InternalExecutor:   &ie,
			DB:                 ex.server.cfg.DB,
		},
		SessionMutator:       ex.dataMutator,
		VirtualSchemas:       ex.server.cfg.VirtualSchemas,
		Tracing:              &ex.sessionTracing,
		StatusServer:         ex.server.cfg.StatusServer,
		MemMetrics:           &ex.memMetrics,
		Tables:               &ex.extraTxnState.tables,
		ExecCfg:              ex.server.cfg,
		DistSQLPlanner:       ex.server.cfg.DistSQLPlanner,
		TxnModesSetter:       ex,
		Jobs:                 &ex.extraTxnState.jobs,
		SchemaChangeJobCache: ex.extraTxnState.schemaChangeJobsCache,
		schemaAccessors:      scInterface,
		sqlStatsCollector:    ex.statsCollector,
	}
}

// resetEvalCtx initializes the fields of evalCtx that can change
// during a session (i.e. the fields not set by initEvalCtx).
//
// stmtTS is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
func (ex *connExecutor) resetEvalCtx(evalCtx *extendedEvalContext, txn *kv.Txn, stmtTS time.Time) {
	evalCtx.TxnState = ex.getTransactionState()
	evalCtx.TxnReadOnly = ex.state.readOnly
	evalCtx.TxnImplicit = ex.implicitTxn()
	evalCtx.StmtTimestamp = stmtTS
	evalCtx.TxnTimestamp = ex.state.sqlTimestamp
	evalCtx.Placeholders = nil
	evalCtx.Annotations = nil
	evalCtx.IVarContainer = nil
	evalCtx.Context = ex.Ctx()
	evalCtx.Txn = txn
	evalCtx.Mon = ex.state.mon
	evalCtx.PrepareOnly = false
	evalCtx.SkipNormalize = false
	evalCtx.GroupWindow = &tree.GroupWindow{
		GroupWindowFunc: tree.GroupWindowUnknown,
	}
}

// getTransactionState retrieves a text representation of the given state.
func (ex *connExecutor) getTransactionState() string {
	state := ex.machine.CurState()
	if ex.implicitTxn() {
		// If the statement reading the state is in an implicit transaction, then we
		// want to tell NoTxn to the client.
		state = stateNoTxn{}
	}
	return state.(fmt.Stringer).String()
}

func (ex *connExecutor) implicitTxn() bool {
	state := ex.machine.CurState()
	os, ok := state.(stateOpen)
	return ok && os.ImplicitTxn.Get()
}

// initPlanner initializes a planner so it can can be used for planning a
// query in the context of this session.
func (ex *connExecutor) initPlanner(ctx context.Context, p *planner) {
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	ex.initEvalCtx(ctx, &p.extendedEvalCtx, p)

	p.sessionDataMutator = ex.dataMutator
	p.noticeSender = nil
	p.preparedStatements = ex.getPrepStmtsAccessor()

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)
}

func (ex *connExecutor) resetPlanner(
	ctx context.Context, p *planner, txn *kv.Txn, stmtTS time.Time,
) {
	p.txn = txn
	p.stmt = nil

	p.cancelChecker.Reset(ctx)

	p.semaCtx = tree.MakeSemaContext()
	p.semaCtx.Location = &ex.sessionData.DataConversion.Location
	p.semaCtx.SearchPath = ex.sessionData.SearchPath
	p.semaCtx.UserDefinedVars = ex.sessionData.UserDefinedVars
	p.semaCtx.AsOfTimestamp = nil
	p.semaCtx.Annotations = nil

	ex.resetEvalCtx(&p.extendedEvalCtx, txn, stmtTS)

	p.autoCommit = false
	p.isPreparing = false
	p.avoidCachedDescriptors = false
	p.discardRows = false
	p.collectBundle = false
}

// txnStateTransitionsApplyWrapper is a wrapper on top of Machine built with the
// TxnStateTransitions above. Its point is to detect when we go in and out of
// transactions and update some state.
//
// Any returned error indicates an unrecoverable error for the session;
// execution on this connection should be interrupted.
func (ex *connExecutor) txnStateTransitionsApplyWrapper(
	ev fsm.Event, payload fsm.EventPayload, res ResultBase, pos CmdPos,
) (advanceInfo, error) {
	var implicitTxn bool

	if os, ok := ex.machine.CurState().(stateOpen); ok {
		implicitTxn = os.ImplicitTxn.Get()
	}

	err := ex.machine.ApplyWithPayload(withStatement(ex.Ctx(), ex.curStmt), ev, payload)
	if err != nil {
		if _, ok := err.(fsm.TransitionNotFoundError); ok {
			panic(err)
		}
		return advanceInfo{}, err
	}

	advInfo := ex.state.consumeAdvanceInfo()

	if advInfo.code == rewind {
		ex.extraTxnState.autoRetryCounter++
	}

	// Handle transaction events which cause updates to txnState.
	switch advInfo.txnEvent {
	case noEvent:

	case txnStart:
		ex.extraTxnState.autoRetryCounter = 0
		ex.extraTxnState.onTxnFinish = ex.recordTransactionStart()
	case txnCommit:
		if res.Err() != nil {
			err := errorutil.UnexpectedWithIssueErrorf(
				26687,
				"programming error: non-error event "+
					advInfo.txnEvent.String()+ //the event is included like this so that it doesn't get sanitized
					" generated even though res.Err() has been set to: %s",
				res.Err())
			log.Error(ex.Ctx(), err)
			errorutil.SendReport(ex.Ctx(), &ex.server.cfg.Settings.SV, err)
			return advanceInfo{}, err
		}

		handleErr := func(err error) {
			if implicitTxn {
				// The schema change/job failed but it was also the only
				// operation in the transaction. In this case, the transaction's
				// error is the schema change error.
				// TODO (lucy): I'm not sure the above is true. What about DROP TABLE
				// with multiple tables?
				res.SetError(err)
			} else {
				// The schema change/job failed but everything else in the
				// transaction was actually committed successfully already. At
				// this point, it is too late to cancel the transaction. In
				// effect, we have violated the "A" of ACID.
				//
				// This situation is sufficiently serious that we cannot let the
				// error that caused the schema change to fail flow back to the
				// client as-is. We replace it by a custom code dedicated to
				// this situation. Replacement occurs because this error code is
				// a "serious error" and the code computation logic will give it
				// a higher priority.
				//
				// We also print out the original error code as prefix of the
				// error message, in case it was a serious error.
				newErr := pgerror.Wrapf(err,
					pgcode.TransactionCommittedWithSchemaChangeFailure,
					"transaction committed but schema change aborted with error: (%s)",
					pgerror.GetPGCode(err))
				newErr = errors.WithHint(newErr,
					"Some of the non-DDL statements may have committed successfully, "+
						"but some of the DDL statement(s) failed.\nManual inspection may be "+
						"required to determine the actual state of the database.")
				newErr = errors.WithIssueLink(newErr,
					errors.IssueLink{IssueURL: "https://gitee.com/kwbasedb/kwbase/issues/42061"})
				res.SetError(newErr)
			}
		}
		ex.notifyStatsRefresherOfNewTables(ex.Ctx())

		if err := ex.server.cfg.JobRegistry.Run(
			ex.ctxHolder.connCtx,
			ex.server.cfg.InternalExecutor,
			ex.extraTxnState.jobs); err != nil {
			handleErr(err)
		}

		// Wait for the cache to reflect the dropped databases if any.
		ex.extraTxnState.tables.waitForCacheToDropDatabases(ex.Ctx())

		fallthrough
	case txnRestart, txnRollback:
		if err := ex.resetExtraTxnState(ex.Ctx(), ex.server.dbCache, advInfo.txnEvent); err != nil {
			return advanceInfo{}, err
		}
	default:
		return advanceInfo{}, errors.AssertionFailedf(
			"unexpected event: %v", errors.Safe(advInfo.txnEvent))
	}

	return advInfo, nil
}

// initStatementResult initializes res according to a query.
//
// cols represents the columns of the result rows. Should be nil if
// stmt.AST.StatementType() != tree.Rows.
//
// If an error is returned, it is to be considered a query execution error.
func (ex *connExecutor) initStatementResult(
	ctx context.Context,
	res RestrictedCommandResult,
	stmtType tree.StatementType,
	cols sqlbase.ResultColumns,
) error {
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	if stmtType == tree.Rows {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}
	return nil
}

// newStatsCollector returns a sqlStatsCollector that will record stats in the
// session's stats containers.
func (ex *connExecutor) newStatsCollector() *sqlStatsCollector {
	return newSQLStatsCollector(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
}

// cancelQuery is part of the registrySession interface.
func (ex *connExecutor) cancelQuery(queryID ClusterWideID) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if queryMeta, exists := ex.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
}

// cancelSession is part of the registrySession interface.
func (ex *connExecutor) cancelSession() {
	if ex.onCancelSession == nil {
		return
	}
	// TODO(abhimadan): figure out how to send a nice error message to the client.
	ex.onCancelSession()
}

// user is part of the registrySession interface.
func (ex *connExecutor) user() string {
	return ex.sessionData.User
}

// serialize is part of the registrySession interface.
func (ex *connExecutor) serialize() serverpb.Session {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	ex.state.mu.RLock()
	defer ex.state.mu.RUnlock()

	var kvTxnID *uuid.UUID
	var activeTxnInfo *serverpb.TxnInfo
	txn := ex.state.mu.txn
	if txn != nil {
		id := txn.ID()
		kvTxnID = &id
		activeTxnInfo = &serverpb.TxnInfo{
			ID:             id,
			Start:          ex.state.mu.txnStart,
			TxnDescription: txn.String(),
		}
	}

	activeQueries := make([]serverpb.ActiveQuery, 0, len(ex.mu.ActiveQueries))
	truncateSQL := func(sql string) string {
		if len(sql) > MaxSQLBytes {
			sql = sql[:MaxSQLBytes-utf8.RuneLen('…')]
			// Ensure the resulting string is valid utf8.
			for {
				if r, _ := utf8.DecodeLastRuneInString(sql); r != utf8.RuneError {
					break
				}
				sql = sql[:len(sql)-1]
			}
			sql += "…"
		}
		return sql
	}

	for id, query := range ex.mu.ActiveQueries {
		if query.hidden {
			continue
		}
		sql := truncateSQL(query.getStatement())
		progress := math.Float64frombits(atomic.LoadUint64(&query.progressAtomic))
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			TxnID:         query.txnID,
			ID:            id.String(),
			Start:         query.start.UTC(),
			Sql:           sql,
			IsDistributed: query.isDistributed,
			Phase:         (serverpb.ActiveQuery_Phase)(query.phase),
			Progress:      float32(progress),
			ExecProgress:  int32(query.execProgress),
		})
	}
	lastActiveQuery := ""
	if ex.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(ex.mu.LastActiveQuery.String())
	}

	remoteStr := "<admin>"
	if ex.sessionData.RemoteAddr != nil {
		remoteStr = ex.sessionData.RemoteAddr.String()
	}

	return serverpb.Session{
		Username:        ex.sessionData.User,
		ClientAddress:   remoteStr,
		ApplicationName: ex.applicationName.Load().(string),
		Start:           ex.phaseTimes[sessionInit].UTC(),
		ActiveQueries:   activeQueries,
		ActiveTxn:       activeTxnInfo,
		KvTxnID:         kvTxnID,
		LastActiveQuery: lastActiveQuery,
		ID:              ex.sessionID.GetBytes(),
		AllocBytes:      ex.mon.AllocBytes(),
		MaxAllocBytes:   ex.mon.MaximumBytes(),
		ConnectionId:    "",
	}
}

func (ex *connExecutor) getPrepStmtsAccessor() preparedStatementsAccessor {
	return connExPrepStmtsAccessor{
		ex: ex,
	}
}

// sessionEventf logs a message to the session event log (if any).
func (ex *connExecutor) sessionEventf(ctx context.Context, format string, args ...interface{}) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventfDepth(ctx, 1 /* depth */, 2 /* level */, format, args...)
	}
	if ex.eventLog != nil {
		ex.eventLog.Printf(format, args...)
	}
}

// notifyStatsRefresherOfNewTables is called on txn commit to inform
// the stats refresher that new tables exist and should have their stats
// collected now.
func (ex *connExecutor) notifyStatsRefresherOfNewTables(ctx context.Context) {
	for _, desc := range ex.extraTxnState.tables.getNewTables() {
		// The CREATE STATISTICS run for an async CTAS query is initiated by the
		// SchemaChanger, so we don't do it here.
		if desc.IsTable() && !desc.IsAs() {
			// Initiate a run of CREATE STATISTICS. We use a large number
			// for rowsAffected because we want to make sure that stats always get
			// created/refreshed here.
			if desc.TableType == tree.TimeseriesTable {
				ex.planner.execCfg.StatsRefresher.
					NotifyTsMutation(desc.ID, math.MaxInt32 /* rowsAffected */, math.MaxInt32 /* entitiesAffected */, math.MaxInt32 /* unorderedAffected */)
			} else if desc.TableType == tree.RelationalTable {
				ex.planner.execCfg.StatsRefresher.
					NotifyMutation(desc.ID, math.MaxInt32 /* rowsAffected */)
			}
		}
	}
}

// StatementCounters groups metrics for counting different types of
// statements.
type StatementCounters struct {
	// QueryCount includes all statements and it is therefore the sum of
	// all the below metrics.
	QueryCount telemetry.CounterWithMetric

	// Basic CRUD statements.
	SelectCount telemetry.CounterWithMetric
	UpdateCount telemetry.CounterWithMetric
	InsertCount telemetry.CounterWithMetric
	DeleteCount telemetry.CounterWithMetric

	// Transaction operations.
	TxnBeginCount    telemetry.CounterWithMetric
	TxnCommitCount   telemetry.CounterWithMetric
	TxnRollbackCount telemetry.CounterWithMetric

	// Savepoint operations. SavepointCount is for real SQL savepoints;
	// the RestartSavepoint variants are for the
	// kwbase-specific client-side retry protocol.
	SavepointCount                  telemetry.CounterWithMetric
	ReleaseSavepointCount           telemetry.CounterWithMetric
	RollbackToSavepointCount        telemetry.CounterWithMetric
	RestartSavepointCount           telemetry.CounterWithMetric
	ReleaseRestartSavepointCount    telemetry.CounterWithMetric
	RollbackToRestartSavepointCount telemetry.CounterWithMetric

	// DdlCount counts all statements whose StatementType is DDL.
	DdlCount telemetry.CounterWithMetric

	// MiscCount counts all statements not covered by a more specific stat above.
	MiscCount telemetry.CounterWithMetric
}

func makeStartedStatementCounters(internal bool) StatementCounters {
	return StatementCounters{
		TxnBeginCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnBeginStarted, internal)),
		TxnCommitCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitStarted, internal)),
		TxnRollbackCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackStarted, internal)),
		RestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRestartSavepointStarted, internal)),
		ReleaseRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseRestartSavepointStarted, internal)),
		RollbackToRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToRestartSavepointStarted, internal)),
		SavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSavepointStarted, internal)),
		ReleaseSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseSavepointStarted, internal)),
		RollbackToSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToSavepointStarted, internal)),
		SelectCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSelectStarted, internal)),
		UpdateCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaUpdateStarted, internal)),
		InsertCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaInsertStarted, internal)),
		DeleteCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDeleteStarted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlStarted, internal)),
		MiscCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaMiscStarted, internal)),
		QueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaQueryStarted, internal)),
	}
}

func makeExecutedStatementCounters(internal bool) StatementCounters {
	return StatementCounters{
		TxnBeginCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnBeginExecuted, internal)),
		TxnCommitCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnCommitExecuted, internal)),
		TxnRollbackCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaTxnRollbackExecuted, internal)),
		RestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRestartSavepointExecuted, internal)),
		ReleaseRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseRestartSavepointExecuted, internal)),
		RollbackToRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToRestartSavepointExecuted, internal)),
		SavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSavepointExecuted, internal)),
		ReleaseSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseSavepointExecuted, internal)),
		RollbackToSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToSavepointExecuted, internal)),
		SelectCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaSelectExecuted, internal)),
		UpdateCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaUpdateExecuted, internal)),
		InsertCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaInsertExecuted, internal)),
		DeleteCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDeleteExecuted, internal)),
		DdlCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaDdlExecuted, internal)),
		MiscCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaMiscExecuted, internal)),
		QueryCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaQueryExecuted, internal)),
	}
}

func (sc *StatementCounters) incrementCount(ex *connExecutor, stmt tree.Statement) {
	sc.QueryCount.Inc()
	switch t := stmt.(type) {
	case *tree.BeginTransaction:
		sc.TxnBeginCount.Inc()
	case *tree.Select:
		sc.SelectCount.Inc()
	case *tree.Update:
		sc.UpdateCount.Inc()
	case *tree.Insert:
		sc.InsertCount.Inc()
	case *tree.Delete:
		sc.DeleteCount.Inc()
	case *tree.CommitTransaction:
		sc.TxnCommitCount.Inc()
	case *tree.RollbackTransaction:
		// The CommitWait state means that the transaction has already committed
		// after a specially handled `RELEASE SAVEPOINT kwbase_restart` command.
		if ex.getTransactionState() == CommitWaitStateStr {
			sc.TxnCommitCount.Inc()
		} else {
			sc.TxnRollbackCount.Inc()
		}
	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(t.Name) {
			sc.RestartSavepointCount.Inc()
		} else {
			sc.SavepointCount.Inc()
		}
	case *tree.ReleaseSavepoint:
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.ReleaseRestartSavepointCount.Inc()
		} else {
			sc.ReleaseSavepointCount.Inc()
		}
	case *tree.RollbackToSavepoint:
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.RollbackToRestartSavepointCount.Inc()
		} else {
			sc.RollbackToSavepointCount.Inc()
		}
	default:
		if tree.CanModifySchema(stmt) {
			sc.DdlCount.Inc()
		} else {
			sc.MiscCount.Inc()
		}
	}
}

// connExPrepStmtsAccessor is an implementation of preparedStatementsAccessor
// that gives access to a connExecutor's prepared statements.
type connExPrepStmtsAccessor struct {
	ex *connExecutor
}

var _ preparedStatementsAccessor = connExPrepStmtsAccessor{}

// List is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) List() map[string]*PreparedStatement {
	// Return a copy of the data, to prevent modification of the map.
	stmts := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts
	ret := make(map[string]*PreparedStatement, len(stmts))
	for key, stmt := range stmts {
		ret[key] = stmt
	}
	return ret
}

// Get is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Get(name string) (*PreparedStatement, bool) {
	s, ok := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	return s, ok
}

// Delete is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Delete(ctx context.Context, name string) bool {
	_, ok := ps.Get(name)
	if !ok {
		return false
	}
	ps.ex.deletePreparedStmt(ctx, name)
	return true
}

// DeleteAll is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) DeleteAll(ctx context.Context) {
	ps.ex.extraTxnState.prepStmtsNamespace.resetTo(
		ctx, prepStmtNamespace{}, &ps.ex.extraTxnState.prepStmtsNamespaceMemAcc,
	)
}

// contextStatementKey is an empty type for the handle associated with the
// statement value (see context.Value).
type contextStatementKey struct{}

// withStatement adds a SQL statement to the provided context. The statement
// will then be included in crash reports which use that context.
func withStatement(ctx context.Context, stmt tree.Statement) context.Context {
	return context.WithValue(ctx, contextStatementKey{}, stmt)
}

// statementFromCtx returns the statement value from a context, or nil if unset.
func statementFromCtx(ctx context.Context) tree.Statement {
	stmt := ctx.Value(contextStatementKey{})
	if stmt == nil {
		return nil
	}
	return stmt.(tree.Statement)
}

func init() {
	// Register a function to include the anonymized statement in crash reports.
	log.RegisterTagFn("statement", func(ctx context.Context) string {
		stmt := statementFromCtx(ctx)
		if stmt == nil {
			return ""
		}
		// Anonymize the statement for reporting.
		return anonymizeStmtAndConstants(stmt)
	})
}

func (ex *connExecutor) SendDirectTsInsert(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	res CommandResult,
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
) (useDeepRule bool, dedupRule int64, dedupRows int64) {
	tsIns := tsInsertNodePool.Get().(*tsInsertNode)
	for _, payloadVals := range payloadNodeMap {
		tsIns.nodeIDs = append(tsIns.nodeIDs, payloadVals.NodeID)
		tsIns.allNodePayloadInfos = append(tsIns.allNodePayloadInfos, payloadVals.PerNodePayloads)
	}
	cfg := ex.server.cfg
	ex.planner.txn = evalCtx.Txn
	ex.planner.execCfg = cfg

	recv := MakeDistSQLReceiver(
		ctx, res, tree.RowsAffected,
		cfg.RangeDescriptorCache, cfg.LeaseHolderCache,
		ex.planner.txn,
		func(ts hlc.Timestamp) {
			cfg.Clock.Update(ts)
		},
		&ex.sessionTracing,
	)
	defer recv.Release()

	exEvalCtx := ex.planner.ExtendedEvalContext()
	exEvalCtx.Tracing = &SessionTracing{}
	var planCtx *PlanningCtx
	distribute := false
	distribute = shouldDistributePlan(
		ctx, ex.sessionData.DistSQLMode, cfg.DistSQLPlanner, tsIns)
	if distribute {
		planCtx = cfg.DistSQLPlanner.NewPlanningCtx(ctx, exEvalCtx, ex.planner.txn)
	} else {
		planCtx = cfg.DistSQLPlanner.newLocalPlanningCtx(ctx, exEvalCtx)
	}
	planCtx.isLocal = !distribute
	planCtx.planner = &ex.planner
	planCtx.stmtType = recv.stmtType

	cleanup := cfg.DistSQLPlanner.PlanAndRun(
		ctx, exEvalCtx, planCtx, ex.planner.txn, tsIns, recv, ex.planner.GetStmt(),
	)
	ex.sendDedupClientNotice(ctx, &ex.planner, recv)
	useDeepRule = recv.useDeepRule
	dedupRule = recv.dedupRule
	dedupRows = recv.dedupRows
	defer cleanup()
	return
}
