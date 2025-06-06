// Copyright 2016 The Cockroach Authors.
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
	"math"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var _ sqlutil.InternalExecutor = &InternalExecutor{}

// InternalExecutor can be used internally by code modules to execute SQL
// statements without needing to open a SQL connection.
//
// InternalExecutor can execute one statement at a time. As of 03/2018, it
// doesn't offer a session interface for maintaining session state or for
// running explicit SQL transactions. However, it supports running SQL
// statements inside a higher-lever (KV) txn and inheriting session variables
// from another session.
//
// Methods not otherwise specified are safe for concurrent execution.
type InternalExecutor struct {
	s *Server

	// mon is the monitor used by all queries executed through the
	// InternalExecutor.
	mon *mon.BytesMonitor

	// memMetrics is the memory metrics that queries executed through the
	// InternalExecutor will contribute to.
	memMetrics MemoryMetrics

	// sessionData, if not nil, represents the session variables used by
	// statements executed on this internalExecutor. Note that queries executed by
	// the executor will run on copies of this data.
	sessionData *sessiondata.SessionData

	// The internal executor uses its own TableCollection. A TableCollection
	// is a schema cache for each transaction and contains data like the schema
	// modified by a transaction. Occasionally an internal executor is called
	// within the context of a transaction that has modified the schema, the
	// internal executor should see the modified schema. This interface allows
	// the internal executor to modify its TableCollection to match the
	// TableCollection of the parent executor.
	tcModifier tableCollectionModifier
}

// MakeInternalExecutor creates an InternalExecutor.
func MakeInternalExecutor(
	ctx context.Context, s *Server, memMetrics MemoryMetrics, settings *cluster.Settings,
) InternalExecutor {
	monitor := mon.MakeUnlimitedMonitor(
		ctx,
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		math.MaxInt64, /* noteworthy */
		settings,
	)
	return InternalExecutor{
		s:          s,
		mon:        &monitor,
		memMetrics: memMetrics,
	}
}

// SetSessionData binds the session variables that will be used by queries
// performed through this executor from now on.
//
// SetSessionData cannot be called concurently with query execution.
func (ie *InternalExecutor) SetSessionData(sessionData *sessiondata.SessionData) {
	ie.s.populateMinimalSessionData(sessionData)
	ie.sessionData = sessionData
}

// GetServer return InternalExecutor Server
func (ie InternalExecutor) GetServer() *Server {
	return ie.s
}

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// returns a StmtBuf into which commands can be pushed and a WaitGroup that will
// be signaled when connEx.run() returns.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// sd will constitute the executor's session state.
func (ie *InternalExecutor) initConnEx(
	ctx context.Context,
	txn *kv.Txn,
	sd *sessiondata.SessionData,
	sdMut sessionDataMutator,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup, error) {
	clientComm := &internalClientComm{
		sync: syncCallback,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
	}

	stmtBuf := NewStmtBuf()
	var ex *connExecutor
	var err error
	if txn == nil {
		ex, err = ie.s.newConnExecutor(
			ctx,
			sd, &sdMut,
			stmtBuf,
			clientComm,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			dontResetSessionDataToDefaults,
		)
	} else {
		ex, err = ie.s.newConnExecutorWithTxn(
			ctx,
			sd, &sdMut,
			stmtBuf,
			clientComm,
			ie.mon,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			txn,
			ie.tcModifier,
			dontResetSessionDataToDefaults,
		)
	}
	if err != nil {
		return nil, nil, err
	}
	ex.executorType = executorTypeInternal
	ex.planner.extendedEvalCtx.IsInternalSQL = true
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := ex.run(ctx, ie.mon, mon.BoundAccount{} /*reserved*/, nil /* cancel */); err != nil {
			sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
			errCallback(err)
		}
		closeMode := normalClose
		if txn != nil {
			closeMode = externalTxnClose
		}
		ex.close(ctx, closeMode)
		wg.Done()
	}()
	return stmtBuf, &wg, nil
}

// Query executes the supplied SQL statement and returns the resulting rows.
// If no user has been previously set through SetSessionData, the statement is
// executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Query is deprecated because it may transparently execute a query as root. Use
// QueryEx instead.
func (ie *InternalExecutor) Query(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	return ie.QueryEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryEx is like Query, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	datums, _, err := ie.queryInternal(ctx, opName, txn, session, stmt, qargs...)
	return datums, err
}

// QueryWithCols is like QueryEx, but it also returns the computed ResultColumns
// of the input query.
func (ie *InternalExecutor) QueryWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	return ie.queryInternal(ctx, opName, txn, session, stmt, qargs...)
}

func (ie *InternalExecutor) queryInternal(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, opName, txn, sessionDataOverride, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
//
// QueryRow is deprecated (like Query). Use QueryRowEx() instead.
func (ie *InternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	return ie.QueryRowEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryRowEx is like QueryRow, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	rows, err := ie.QueryEx(ctx, opName, txn, session, stmt, qargs...)
	if err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		return rows[0], nil
	default:
		return nil, &tree.MultipleResultsError{SQL: stmt}
	}
}

// Exec executes the supplied SQL statement and returns the number of rows
// affected (not like the results; see Query()). If no user has been previously
// set through SetSessionData, the statement is executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Exec is deprecated because it may transparently execute a query as root. Use
// ExecEx instead.
func (ie *InternalExecutor) Exec(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (int, error) {
	return ie.ExecEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// GetTableCollection return TableCollection
func GetTableCollection(cfg *ExecutorConfig, ie *InternalExecutor) *TableCollection {
	return &TableCollection{
		leaseMgr:          cfg.LeaseManager,
		settings:          cfg.Settings,
		databaseCache:     ie.s.dbCache.getDatabaseCache(),
		dbCacheSubscriber: ie.s.dbCache,
	}
}

// IsTsTable determine the type of table, it need ctx, db ,tbname
func (ie *InternalExecutor) IsTsTable(
	ctx context.Context, dbName string, tbName string, user string,
) (bool, DirectInsertTable, error) {
	var (
		dit       DirectInsertTable
		cfg       = ie.s.cfg
		isTsTable bool
	)

	err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tables := &TableCollection{
			leaseMgr:          cfg.LeaseManager,
			settings:          cfg.Settings,
			databaseCache:     ie.s.dbCache.getDatabaseCache(),
			dbCacheSubscriber: ie.s.dbCache,
		}
		defer tables.releaseTables(ctx)

		dit.Tname = tree.NewTableName(tree.Name(dbName), tree.Name(tbName))

		flags := tree.ObjectLookupFlags{}
		table, err := tables.getTableVersion(ctx, txn, dit.Tname, flags)
		if table == nil || err != nil {
			return errors.Errorf("can not find tables")
		}

		if table.TableType != tree.TimeseriesTable && table.TableType != tree.TemplateTable {
			return errors.Errorf("%s is not a ts table", tbName)
		}

		sd := &sessiondata.SessionData{User: user}
		pp := &planner{
			txn:     txn,
			execCfg: cfg,
			curPlan: planTop{},
			extendedEvalCtx: extendedEvalContext{
				EvalContext: tree.EvalContext{
					SessionData:      sd,
					InternalExecutor: cfg.InternalExecutor,
					Settings:         cfg.Settings,
				},
				ExecCfg:         cfg,
				schemaAccessors: newSchemaInterface(tables, cfg.VirtualSchemas),
			},
			avoidCachedDescriptors: true,
		}

		if err := pp.CheckPrivilege(ctx, table.TableDesc(), privilege.INSERT); err != nil {
			return err
		}

		tableDesc := table.TableDesc()
		dit.DbID = uint32(tableDesc.ParentID)
		dit.TabID = uint32(tableDesc.ID)
		if tableDesc.TsTable.HashNum == 0 {
			tableDesc.TsTable.HashNum = api.HashParamV2
		}
		dit.HashNum = tableDesc.TsTable.HashNum
		dit.ColsDesc = tableDesc.Columns
		dit.TableType = table.GetTableType()
		isTsTable = true
		return nil
	})

	return isTsTable, dit, err
}

// GetAudiLogVesion determine the version
func (ie *InternalExecutor) GetAudiLogVesion(ctx context.Context) (uint32, error) {
	var tables *TableCollection
	var version uint32
	tables = &TableCollection{
		leaseMgr:          ie.s.cfg.LeaseManager,
		settings:          ie.s.cfg.Settings,
		databaseCache:     ie.s.dbCache.getDatabaseCache(),
		dbCacheSubscriber: ie.s.dbCache,
	}

	defer tables.releaseTables(ctx)
	err := ie.s.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var auditTableName = tree.MakeTableName("system", "audits")
		_, _, _, _, version1, err := tables.GetObjectDesc(ctx, txn, &auditTableName, false, "", nil, true)
		if err != nil {
			return err
		}
		version = version1
		return nil
	})
	return version, err
}

// ExecEx is like Exec, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) ExecEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx, opName, txn, session, stmt, qargs...)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

// GetClusterID returns ClusterID.
func (ie *InternalExecutor) GetClusterID() (string, error) {
	clusterID, err := ie.s.cfg.Gossip.GetClusterID()
	if err != nil {
		return "", err
	}
	return clusterID.String(), nil
}

type result struct {
	rows         []tree.Datums
	rowsAffected int
	cols         sqlbase.ResultColumns
	err          error
}

// applyOverrides overrides the respective fields from sd for all the fields set on o.
func applyOverrides(o sqlbase.InternalExecutorSessionDataOverride, sd *sessiondata.SessionData) {
	if o.User != "" {
		sd.User = o.User
	}
	if o.Database != "" {
		sd.Database = o.Database
	}
	if o.ApplicationName != "" {
		sd.ApplicationName = o.ApplicationName
	}
	if o.SearchPath != nil {
		sd.SearchPath = *o.SearchPath
	}
}

func (ie *InternalExecutor) maybeRootSessionDataOverride(
	opName string,
) sqlbase.InternalExecutorSessionDataOverride {
	if ie.sessionData == nil {
		return sqlbase.InternalExecutorSessionDataOverride{
			User:            security.RootUser,
			ApplicationName: sqlbase.InternalAppNamePrefix + "-" + opName,
		}
	}
	o := sqlbase.InternalExecutorSessionDataOverride{}
	if ie.sessionData.User == "" {
		o.User = security.RootUser
	}
	if ie.sessionData.ApplicationName == "" {
		o.ApplicationName = sqlbase.InternalAppNamePrefix + "-" + opName
	}
	return o
}

// execInternal executes a statement.
//
// sessionDataOverride can be used to control select fields in the executor's
// session data. It overrides what has been previously set through
// SetSessionData(), if anything.
func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (retRes result, retErr error) {
	ctx = logtags.AddTag(ctx, "intExec", opName)

	var sd *sessiondata.SessionData
	if ie.sessionData != nil {
		// TODO(andrei): Properly clone (deep copy) ie.sessionData.
		sdCopy := *ie.sessionData
		sd = &sdCopy
	} else {
		sd = ie.s.newSessionData(SessionArgs{})
	}
	applyOverrides(sessionDataOverride, sd)
	if sd.User == "" {
		return result{}, errors.AssertionFailedf("no user specified for internal query")
	}
	if sd.ApplicationName == "" {
		sd.ApplicationName = sqlbase.InternalAppNamePrefix + "-" + opName
	}
	sdMutator := ie.s.makeSessionDataMutator(sd, SessionDefaults{} /* defaults */)

	defer func() {
		// We wrap errors with the opName, but not if they're retriable - in that
		// case we need to leave the error intact so that it can be retried at a
		// higher level.
		if retErr != nil && !errIsRetriable(retErr) {
			retErr = errors.Wrapf(retErr, opName)
		}
		if retRes.err != nil && !errIsRetriable(retRes.err) {
			retRes.err = errors.Wrapf(retRes.err, opName)
		}
	}()

	ctx, sp := tracing.EnsureChildSpan(ctx, ie.s.cfg.AmbientCtx.Tracer, opName)
	defer sp.Finish()

	timeReceived := timeutil.Now()
	parseStart := timeReceived
	parsed, err := parser.ParseOne(stmt)
	if err != nil {
		return result{}, err
	}
	parseEnd := timeutil.Now()

	// resPos will be set to the position of the command that represents the
	// statement we care about before that command is sent for execution.
	var resPos CmdPos

	resCh := make(chan result)
	var resultsReceived bool
	syncCallback := func(results []resWithPos) {
		resultsReceived = true
		for _, res := range results {
			if res.pos == resPos {
				resCh <- result{rows: res.rows, rowsAffected: res.RowsAffected(), cols: res.cols, err: res.Err()}
				return
			}
			if res.err != nil {
				// If we encounter an error, there's no point in looking further; the
				// rest of the commands in the batch have been skipped.
				resCh <- result{err: res.Err()}
				return
			}
		}
		resCh <- result{err: errors.AssertionFailedf("missing result for pos: %d and no previous error", resPos)}
	}
	errCallback := func(err error) {
		if resultsReceived {
			return
		}
		resCh <- result{err: err}
	}

	stmtBuf, wg, err := ie.initConnEx(ctx, txn, sd, sdMutator, syncCallback, errCallback)
	if err != nil {
		return result{}, err
	}

	// Transforms the args to datums. The datum types will be passed as type hints
	// to the PrepareStmt command.
	datums := golangFillQueryArguments(qargs...)
	typeHints := make(tree.PlaceholderTypes, len(datums))
	for i, d := range datums {
		// Arg numbers start from 1.
		typeHints[tree.PlaceholderIdx(i)] = d.ResolvedType()
	}
	if len(qargs) == 0 {
		resPos = 0
		if err := stmtBuf.Push(
			ctx,
			ExecStmt{
				Statement:    parsed,
				TimeReceived: timeReceived,
				ParseStart:   parseStart,
				ParseEnd:     parseEnd,
			}); err != nil {
			return result{}, err
		}
	} else {
		resPos = 2
		if err := stmtBuf.Push(
			ctx,
			PrepareStmt{
				Statement:  parsed,
				ParseStart: parseStart,
				ParseEnd:   parseEnd,
				TypeHints:  typeHints,
			},
		); err != nil {
			return result{}, err
		}

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: datums}); err != nil {
			return result{}, err
		}

		if err := stmtBuf.Push(ctx, ExecPortal{TimeReceived: timeReceived}); err != nil {
			return result{}, err
		}
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return result{}, err
	}

	res := <-resCh
	stmtBuf.Close()
	wg.Wait()
	return res, nil
}

// internalClientComm is an implementation of ClientComm used by the
// InternalExecutor. Result rows are buffered in memory.
type internalClientComm struct {
	// results will contain the results of the commands executed by an
	// InternalExecutor.
	results []resWithPos

	lastDelivered CmdPos

	// sync, if set, is called whenever a Sync is executed. It returns all the
	// results since the previous Sync.
	sync func([]resWithPos)
}

type resWithPos struct {
	*bufferedCommandResult
	pos CmdPos
}

// CreateStatementResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateStatementResult(
	_ tree.Statement,
	_ RowDescOpt,
	pos CmdPos,
	_ []pgwirebase.FormatCode,
	_ sessiondata.DataConversionConfig,
	_ int,
	_ string,
	_ bool,
) CommandResult {
	return icc.createRes(pos, nil /* onClose */)
}

// createRes creates a result. onClose, if not nil, is called when the result is
// closed.
func (icc *internalClientComm) createRes(pos CmdPos, onClose func(error)) *bufferedCommandResult {
	res := &bufferedCommandResult{
		closeCallback: func(res *bufferedCommandResult, typ resCloseType, err error) {
			if typ == discarded {
				return
			}
			icc.results = append(icc.results, resWithPos{bufferedCommandResult: res, pos: pos})
			if onClose != nil {
				onClose(err)
			}
		},
	}
	return res
}

// CreatePrepareResult is part of the ClientComm interface.
func (icc *internalClientComm) CreatePrepareResult(pos CmdPos) ParseResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateBindResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateBindResult(pos CmdPos) BindResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateSyncResult is part of the ClientComm interface.
//
// The returned SyncResult will call the sync callback when its closed.
func (icc *internalClientComm) CreateSyncResult(pos CmdPos) SyncResult {
	return icc.createRes(pos, func(err error) {
		results := make([]resWithPos, len(icc.results))
		copy(results, icc.results)
		icc.results = icc.results[:0]
		icc.sync(results)
		icc.lastDelivered = pos
	} /* onClose */)
}

// LockCommunication is part of the ClientComm interface.
func (icc *internalClientComm) LockCommunication() ClientLock {
	return &noopClientLock{
		clientComm: icc,
	}
}

// Flush is part of the ClientComm interface.
func (icc *internalClientComm) Flush(pos CmdPos) error {
	return nil
}

// CreateDescribeResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDescribeResult(pos CmdPos) DescribeResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateDeleteResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDeleteResult(pos CmdPos) DeleteResult {
	panic("unimplemented")
}

// CreateFlushResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateFlushResult(pos CmdPos) FlushResult {
	panic("unimplemented")
}

// CreateErrorResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateErrorResult(pos CmdPos) ErrorResult {
	panic("unimplemented")
}

// CreateEmptyQueryResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateEmptyQueryResult(pos CmdPos) EmptyQueryResult {
	panic("unimplemented")
}

// CreateCopyInResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateCopyInResult(pos CmdPos) CopyInResult {
	panic("unimplemented")
}

// CreateDrainResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDrainResult(pos CmdPos) DrainResult {
	panic("unimplemented")
}

// noopClientLock is an implementation of ClientLock that says that no results
// have been communicated to the client.
type noopClientLock struct {
	clientComm *internalClientComm
}

// Close is part of the ClientLock interface.
func (ncl *noopClientLock) Close() {}

// ClientPos is part of the ClientLock interface.
func (ncl *noopClientLock) ClientPos() CmdPos {
	return ncl.clientComm.lastDelivered
}

// RTrim is part of the ClientLock interface.
func (ncl *noopClientLock) RTrim(_ context.Context, pos CmdPos) {
	var i int
	var r resWithPos
	for i, r = range ncl.clientComm.results {
		if r.pos >= pos {
			break
		}
	}
	ncl.clientComm.results = ncl.clientComm.results[:i]
}
