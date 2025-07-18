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

package sql

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/infos"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/fsm"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// ShowJobType is the type of job
type ShowJobType int32

const (
	// ShowJobRESTART for task type
	ShowJobRESTART ShowJobType = 0
	// ShowJobIMPORT for task type
	ShowJobIMPORT ShowJobType = 1
	// ShowJobEXPORT for task type
	ShowJobEXPORT ShowJobType = 2
)

// ShowJobStatus is the status of job
type ShowJobStatus int32

const (
	// ShowJobSuccess job status success
	ShowJobSuccess ShowJobStatus = 0
	// ShowJobFail for job status failed
	ShowJobFail ShowJobStatus = 1
)

// ShowJobsForImportAndExportforInsert is the feature which insert task to system.jobs for export and import.
func ShowJobsForImportAndExportforInsert(
	ctx context.Context,
	db *kv.DB,
	internalExecutor *InternalExecutor,
	jobid int64,
	t ShowJobType,
	desc string,
	statement string,
	usr string,
) error {
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var deTailsTemp jobspb.Details
		var progressTemp jobspb.ProgressDetails
		switch t {
		case ShowJobIMPORT:
			deTailsTemp = jobspb.ImportDetails{}
			progressTemp = jobspb.ImportProgress{}
		case ShowJobEXPORT:
			deTailsTemp = jobspb.ExportDetails{}
			progressTemp = jobspb.ExportProgress{}
		default:
			deTailsTemp = jobspb.ImportDetails{}
			progressTemp = jobspb.ImportProgress{}
		}
		var payload = jobspb.Payload{
			Description:   desc,
			Statement:     statement,
			Username:      usr,
			StartedMicros: timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime()),
			DescriptorIDs: []sqlbase.ID{},
			Details:       jobspb.WrapPayloadDetails(deTailsTemp),
			Noncancelable: false,
		}
		var progress = jobspb.Progress{
			Details:       jobspb.WrapProgressDetails(progressTemp),
			RunningStatus: string(jobs.StatusRunning),
		}
		progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
		payloadBytes, err := protoutil.Marshal(&payload)
		if err != nil {
			return err
		}
		progressBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			return err
		}
		if err == nil {
			const stmt = "INSERT INTO system.jobs (id, status, payload, progress) VALUES ($1, $2, $3, $4)"
			_, err = internalExecutor.Exec(ctx, "job-insert", txn, stmt, jobid, jobs.StatusRunning, payloadBytes, progressBytes)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ShowJobsUpdate is the feature which update task to system.jobs.
func ShowJobsUpdate(
	ctx context.Context,
	internalExecutor *InternalExecutor,
	txn *kv.Txn,
	jobid int64,
	jobstatus int32,
	finishTime int64,
	errorTime int64,
	errorInfo string,
) error {
	var payload *jobspb.Payload
	var progress *jobspb.Progress
	const selectStmt = "SELECT payload, progress FROM system.jobs WHERE id = $1"
	row, err := internalExecutor.QueryRowEx(
		ctx, "log-job", txn, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		selectStmt, jobid)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.Errorf("no such job %d found", jobid)
	}
	if payload, err = jobs.UnmarshalPayload(row[0]); err != nil {
		return err
	}
	if progress, err = jobs.UnmarshalProgress(row[1]); err != nil {
		return err
	}
	payload.FinishedMicros = finishTime
	//progress.RunningStatus = "succeeded"
	var status string
	if jobstatus == 0 {
		status = "succeeded"
		progress.RunningStatus = string(jobs.StatusSucceeded)
		payload.FinishedMicros = finishTime
	} else if jobstatus == 1 {
		status = "failed"
		payload.ErrorMicros = errorTime
		payload.Error = errorInfo
		progress.RunningStatus = string(jobs.StatusFailed)
	}
	payloadBytes, err := protoutil.Marshal(payload)
	if err != nil {
		return err
	}
	progressBytes, err := protoutil.Marshal(progress)
	if err != nil {
		return err
	}

	if err == nil {
		const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2, progress = $3 WHERE id = $4"
		_, err = internalExecutor.Exec(ctx, "job-update", txn, updateStmt, status, payloadBytes, progressBytes, jobid)
	}
	if err != nil {
		return err
	}
	return nil
}

// execStmt executes one statement by dispatching according to the current
// state. Returns an Event to be passed to the state machine, or nil if no
// transition is needed. If nil is returned, then the cursor is supposed to
// advance to the next statement.
//
// If an error is returned, the session is supposed to be considered done. Query
// execution errors are not returned explicitly and they're also not
// communicated to the client. Instead they're incorporated in the returned
// event (the returned payload will implement payloadWithError). It is the
// caller's responsibility to deliver execution errors to the client.
//
// Args:
// stmt: The statement to execute.
// res: Used to produce query results.
// pinfo: The values to use for the statement's placeholders. If nil is passed,
//
//	then the statement cannot have any placeholder.
func (ex *connExecutor) execStmt(
	ctx context.Context, stmt Statement, res RestrictedCommandResult, pinfo *tree.PlaceholderInfo,
) (fsm.Event, fsm.EventPayload, error) {
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", stmt, ex.machine.CurState())
	}

	// Stop the session idle timeout when a new statement is executed.
	ex.mu.IdleInSessionTimeout.Stop()
	res.SetClient()

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err := ex.runObserverStatement(ctx, stmt, res)
		// Note that regardless of res.Err(), these observer statements don't
		// generate error events; transactions are always allowed to continue.
		return nil, nil, err
	}

	queryID := ex.generateID()
	stmt.queryID = queryID

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error

	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		ev, payload = ex.execStmtInNoTxnState(ctx, stmt)
	case stateOpen:
		if ex.server.cfg.Settings.IsCPUProfiling() {
			labels := pprof.Labels(
				"stmt.tag", stmt.AST.StatementTag(),
				"stmt.anonymized", stmt.AnonymizedStr,
			)
			pprof.Do(ctx, labels, func(ctx context.Context) {
				ev, payload, err = ex.execStmtInOpenState(ctx, stmt, res, pinfo)
			})
		} else {
			// build SelectInto statement when subquery as value in setting user defined variables
			if astSetVar, ok := stmt.AST.(*tree.SetVar); ok && len(astSetVar.Name) > 0 && astSetVar.Name[0] == '@' && len(astSetVar.Values) == 1 {
				if sub, ok := astSetVar.Values[0].(*tree.Subquery); ok {
					sel := tree.Select{Select: sub.Select}
					selInto := tree.SelectInto{
						Targets:      tree.SelectIntoTargets{tree.SelectIntoTarget{Udv: tree.UserDefinedVar{VarName: astSetVar.Name}}},
						SelectClause: &sel,
					}
					stmt.AST = &selInto
				}
			}
			ev, payload, err = ex.execStmtInOpenState(ctx, stmt, res, pinfo)
		}
		switch ev.(type) {
		case eventNonRetriableErr:
			ex.recordFailure()
		}
	case stateAborted:
		ev, payload = ex.execStmtInAbortedState(ctx, stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	if ex.sessionData.IdleInSessionTimeout > 0 {
		// Cancel the session if the idle time exceeds the idle in session timeout.
		timer := time.AfterFunc(ex.sessionData.IdleInSessionTimeout, ex.cancelSession)
		timeout := timeutil.Timeout{}
		timeout.Set(timer)
		ex.mu.IdleInSessionTimeout = timeout
	}

	return ev, payload, err
}

func (ex *connExecutor) recordFailure() {
	ex.metrics.EngineMetrics.FailureCount.Inc(1)
}

// execPortal executes a prepared statement. It is a "wrapper" around execStmt
// method that is performing additional work to track portal's state.
func (ex *connExecutor) execPortal(
	ctx context.Context,
	portal PreparedPortal,
	portalName string,
	stmtRes CommandResult,
	pinfo *tree.PlaceholderInfo,
) (ev fsm.Event, payload fsm.EventPayload, err error) {
	curStmt := Statement{
		Statement:     portal.Stmt.Statement,
		Prepared:      portal.Stmt,
		ExpectedTypes: portal.Stmt.Columns,
		AnonymizedStr: portal.Stmt.AnonymizedStr,
	}
	stmtCtx := withStatement(ctx, ex.curStmt)
	switch ex.machine.CurState().(type) {
	case stateOpen:
		// We're about to execute the statement in an open state which
		// could trigger the dispatch to the execution engine. However, it
		// is possible that we're trying to execute an already exhausted
		// portal - in such a scenario we should return no rows, but the
		// execution engine is not aware of that and would run the
		// statement as if it was running it for the first time. In order
		// to prevent such behavior, we check whether the portal has been
		// exhausted and execute the statement only if it hasn't. If it has
		// been exhausted, then we do not dispatch the query for execution,
		// but connExecutor will still perform necessary state transitions
		// which will emit CommandComplete messages and alike (in a sense,
		// by not calling execStmt we "execute" the portal in such a way
		// that it returns 0 rows).
		// Note that here we deviate from Postgres which returns an error
		// when attempting to execute an exhausted portal which has a
		// StatementType() different from "Rows".
		if !portal.exhausted {
			ev, payload, err = ex.execStmt(stmtCtx, curStmt, stmtRes, pinfo)
			// Portal suspension is supported via a "side" state machine
			// (see pgwire.limitedCommandResult for details), so when
			// execStmt returns, we know for sure that the portal has been
			// executed to completion, thus, it is exhausted.
			// Note that the portal is considered exhausted regardless of
			// the fact whether an error occurred or not - if it did, we
			// still don't want to re-execute the portal from scratch.
			// The current statement may have just closed and deleted the portal,
			// so only exhaust it if it still exists.
			if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
				ex.exhaustPortal(portalName)
			}
		}
	default:
		ev, payload, err = ex.execStmt(stmtCtx, curStmt, stmtRes, pinfo)
	}
	return
}

// execStmtInOpenState executes one statement in the context of the session's
// current transaction.
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// directly and delegates everything else to the execution engines.
// Results and query execution errors are written to res.
//
// This method also handles "auto commit" - committing of implicit transactions.
//
// If an error is returned, the connection is supposed to be consider done.
// Query execution errors are not returned explicitly; they're incorporated in
// the returned Event.
//
// The returned event can be nil if no state transition is required.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context, stmt Statement, res RestrictedCommandResult, pinfo *tree.PlaceholderInfo,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	// Update the deadline on the transaction based on the collections.
	//ex.extraTxnState.tables.MaybeUpdateDeadline(ctx, ex.state.mu.txn)

	ex.incrementStartedStmtCounter(stmt)
	defer func() {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(stmt)
		}
	}()
	os := ex.machine.CurState().(stateOpen)

	var timeoutTicker *time.Timer
	queryTimedOut := false
	doneAfterFunc := make(chan struct{}, 1)

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancelation function here.
	unregisterFn := ex.addActiveQuery(ctx, stmt.queryID, stmt, ex.state.cancel)

	// queryDone is a cleanup function dealing with unregistering a query.
	// It also deals with overwriting res.Error to a more user-friendly message in
	// case of query cancelation. res can be nil to opt out of this.
	queryDone := func(ctx context.Context, res RestrictedCommandResult) {
		if timeoutTicker != nil {
			if !timeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-doneAfterFunc
			}
		}
		unregisterFn()

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if res != nil && ctx.Err() != nil && res.Err() != nil {
			if queryTimedOut {
				res.SetError(sqlbase.QueryTimeoutError)
			} else {
				res.SetError(sqlbase.QueryCanceledError)
			}
		}
	}
	// Generally we want to unregister after the auto-commit below. However, in
	// case we'll execute the statement through the parallel execution queue,
	// we'll pass the responsibility for unregistering to the queue.
	defer func() {
		if queryDone != nil {
			queryDone(ctx, res)
		}
	}()

	// set isolation of txn
	if ex.planner.txn != nil && ex.planner.txn.IsoLevel() == enginepb.Unspecified {
		level := ex.txnIsolationLevelToKV(tree.UnspecifiedIsolation)
		if err := ex.state.setIsolationLevel(enginepb.Level(level)); err != nil {
			return nil, nil, err
		}
	}

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.reset(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutator.paramStatusUpdater = res
	p.noticeSender = res

	var shouldCollectDiagnostics bool
	var finishCollectionDiagnostics StmtDiagnosticsTraceFinishFunc

	if explainBundle, ok := stmt.AST.(*tree.ExplainAnalyzeDebug); ok {
		telemetry.Inc(sqltelemetry.ExplainAnalyzeDebugUseCounter)
		// Always collect diagnostics for EXPLAIN ANALYZE (DEBUG).
		shouldCollectDiagnostics = true
		// Strip off the explain node to execute the inner statement.
		stmt.AST = explainBundle.Statement
		// TODO(radu): should we trim the "EXPLAIN ANALYZE (DEBUG)" part from
		// stmt.SQL?

		// Clear any ExpectedTypes we set if we prepared this statement (they
		// reflect the column types of the EXPLAIN itself and not those of the inner
		// statement).
		stmt.ExpectedTypes = nil

		// EXPLAIN ANALYZE (DEBUG) does not return the rows for the given query;
		// instead it returns some text which includes a URL.
		// TODO(radu): maybe capture some of the rows and include them in the
		// bundle.
		p.discardRows = true
	} else {
		shouldCollectDiagnostics, finishCollectionDiagnostics = ex.stmtDiagnosticsRecorder.ShouldCollectDiagnostics(ctx, stmt.AST)
		if shouldCollectDiagnostics {
			telemetry.Inc(sqltelemetry.StatementDiagnosticsCollectedCounter)
		}
	}

	if shouldCollectDiagnostics {
		p.collectBundle = true
		tr := ex.server.cfg.AmbientCtx.Tracer
		origCtx := ctx
		var sp opentracing.Span
		ctx, sp = tracing.StartSnowballTrace(ctx, tr, "traced statement")
		// TODO(radu): consider removing this if/when #46164 is addressed.
		p.extendedEvalCtx.Context = ctx
		defer func() {
			// Record the statement information that we've collected.
			// Note that in case of implicit transactions, the trace contains the auto-commit too.
			sp.Finish()
			trace := tracing.GetRecording(sp)
			ie := p.extendedEvalCtx.InternalExecutor.(*InternalExecutor)
			placeholders := p.extendedEvalCtx.Placeholders
			if finishCollectionDiagnostics != nil {
				bundle, collectionErr := buildStatementBundle(
					origCtx, ex.server.cfg.DB, ie, &p.curPlan, trace, placeholders,
				)
				finishCollectionDiagnostics(origCtx, bundle.trace, bundle.zip, collectionErr)
			} else {
				// Handle EXPLAIN ANALYZE (DEBUG).
				// If there was a communication error, no point in setting any results.
				if retErr == nil {
					retErr = setExplainBundleResult(
						origCtx, res, stmt.AST, trace, &p.curPlan, placeholders, ie, ex.server.cfg,
					)
				}
			}
		}()
	}

	if ex.sessionData.StmtTimeout > 0 {
		timeoutTicker = time.AfterFunc(
			ex.sessionData.StmtTimeout-timeutil.Since(ex.phaseTimes[sessionQueryReceived]),
			func() {
				ex.cancelQuery(stmt.queryID)
				queryTimedOut = true
				doneAfterFunc <- struct{}{}
			})
	}

	defer func() {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, stmt.String(), execErr)
		}

		// Do the auto-commit, if necessary.
		if retEv != nil || retErr != nil {
			return
		}
		if os.ImplicitTxn.Get() {
			retEv, retPayload = ex.handleAutoCommit(ctx, stmt.AST)
			return
		}
	}()

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		return ev, payload, nil
	}

	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// BEGIN is always an error when in the Open state. It's legitimate only in
		// the NoTxn state.
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx)
		return ev, payload, nil

	case *tree.Savepoint:
		return ex.execSavepointInOpenState(ctx, s, res)

	case *tree.ReleaseSavepoint:
		ev, payload := ex.execRelease(ctx, s, res)
		return ev, payload, nil

	case *tree.RollbackToSavepoint:
		ev, payload := ex.execRollbackToSavepointInOpenState(ctx, s, res)
		return ev, payload, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.Newf(
				pgcode.DuplicatePreparedStatement,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		var typeHints tree.PlaceholderTypes
		if len(s.Types) > 0 {
			if len(s.Types) > stmt.NumPlaceholders {
				err := pgerror.Newf(pgcode.Syntax, "too many types provided")
				return makeErrEvent(err)
			}
			typeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
			copy(typeHints, s.Types)
		}

		// get and parse prepared statement when user defined variable as sql
		if s.Udv != nil && s.Statement == nil {
			varName := strings.ToLower(s.Udv.String())
			varStr := ex.sessionData.UserDefinedVars[varName]
			if stmtStr, ok := varStr.(*tree.DString); ok {
				resStmt, err := parser.ParseOne(string(*stmtStr))
				if err != nil {
					err := pgerror.Newf(pgcode.Syntax, "invalid syntax of query")
					return makeErrEvent(err)
				}
				s.Statement = resStmt.AST
			} else {
				err := pgerror.Newf(pgcode.Syntax, "invalid syntax of query")
				return makeErrEvent(err)
			}
		}
		if _, err := ex.addPreparedStmt(
			ctx, name,
			Statement{
				Statement: parser.Statement{
					// We need the SQL string just for the part that comes after
					// "PREPARE ... AS",
					// TODO(radu): it would be nice if the parser would figure out this
					// string and store it in tree.Prepare.
					SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
					AST:             s.Statement,
					NumPlaceholders: stmt.NumPlaceholders,
					NumAnnotations:  stmt.NumAnnotations,
				},
			},
			typeHints,
			PreparedStatementOriginSQL,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil

	case *tree.Execute:
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution below.
		name := s.Name.String()
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.Newf(
				pgcode.InvalidSQLStatementName,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ps, name, s.Params, ex.sessionData.SearchPath, ex.planner.semaCtx.Location)
		if err != nil {
			return makeErrEvent(err)
		}

		stmt.Statement = ps.Statement
		stmt.Prepared = ps
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.AST)

		if s.DiscardRows {
			p.discardRows = true
		}
	}

	p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens.

	if os.ImplicitTxn.Get() {
		asOfTs, err := p.isAsOf(stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if asOfTs != nil {
			p.semaCtx.AsOfTimestamp = asOfTs
			p.extendedEvalCtx.SetTxnTimestamp(asOfTs.GoTime())
			ex.state.setHistoricalTimestamp(ctx, *asOfTs)
		}
	} else {
		// If we're in an explicit txn, we allow AOST but only if it matches with
		// the transaction's timestamp. This is useful for running AOST statements
		// using the InternalExecutor inside an external transaction; one might want
		// to do that to force p.avoidCachedDescriptors to be set below.
		ts, err := p.isAsOf(stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			if readTs := ex.state.getReadTimestamp(); *ts != readTs {
				err = pgerror.Newf(pgcode.Syntax,
					"inconsistent AS OF SYSTEM TIME timestamp; expected: %s", readTs)
				err = errors.WithHint(err, "try SET TRANSACTION AS OF SYSTEM TIME")
				return makeErrEvent(err)
			}
			p.semaCtx.AsOfTimestamp = ts
		}
	}

	// The first order of business is to ensure proper sequencing
	// semantics.  As per PostgreSQL's dialect specs, the "read" part of
	// statements always see the data as per a snapshot of the database
	// taken the instant the statement begins to run. In particular a
	// mutation does not see its own writes. If a query contains
	// multiple mutations using CTEs (WITH) or a read part following a
	// mutation, all still operate on the same read snapshot.
	//
	// (To communicate data between CTEs and a main query, the result
	// set / RETURNING can be used instead. However this is not relevant
	// here.)

	// We first ensure stepping mode is enabled.
	//
	// This ought to be done just once when a txn gets initialized;
	// unfortunately, there are too many places where the txn object
	// is re-configured, re-set etc without using NewTxnWithSteppingEnabled().
	//
	// Manually hunting them down and calling ConfigureStepping() each
	// time would be error prone (and increase the chance that a future
	// change would forget to add the call).
	//
	// TODO(andrei): really the code should be rearchitected to ensure
	// that all uses of SQL execution initialize the client.Txn using a
	// single/common function. That would be where the stepping mode
	// gets enabled once for all SQL statements executed "underneath".
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode) }()

	// Then we create a sequencing point.
	//
	// This is not the only place where a sequencing point is
	// placed. There are also sequencing point after every stage of
	// constraint checks and cascading actions at the _end_ of a
	// statement's execution.
	//
	// TODO(knz): At the time of this writing CockroachDB performs
	// cascading actions and the corresponding FK existence checks
	// interleaved with mutations. This is incorrect; the correct
	// behavior, as described in issue
	// https://gitee.com/kwbasedb/kwbase/issues/33475, is to
	// execute cascading actions no earlier than after all the "main
	// effects" of the current statement (including all its CTEs) have
	// completed. There should be a sequence point between the end of
	// the main execution and the start of the cascading actions, as
	// well as in-between very stage of cascading actions.
	// This TODO can be removed when the cascading code is reorganized
	// accordingly and the missing call to Step() is introduced.
	if err := ex.state.mu.txn.Step(ctx); err != nil {
		return makeErrEvent(err)
	}

	if err := p.semaCtx.Placeholders.Assign(pinfo, stmt.NumPlaceholders); err != nil {
		return makeErrEvent(err)
	}
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	ex.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)
	p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
	var jobID int64
	if _, ok := stmt.AST.(*tree.Export); ok {
		jobID = ex.server.cfg.JobRegistry.MakeJobID()
		err := ShowJobsForImportAndExportforInsert(ctx, ex.server.cfg.DB, ex.server.cfg.InternalExecutor, jobID, ShowJobEXPORT, "export", stmt.SQL, ex.planner.User())
		if err != nil {
			return nil, nil, err
		}
	}
	if ex.executorType != executorTypeInternal && ex.state.mu.txn.IsoLevel() == enginepb.ReadCommitted && !ex.implicitTxn() {
		if err := ex.dispatchReadCommittedStmtToExecutionEngine(ctx, p, res); err != nil {
			return nil, nil, err
		}
	} else {
		if err := ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
			if _, ok := stmt.AST.(*tree.Export); ok {
				err := ex.server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return ShowJobsUpdate(ctx, ex.server.cfg.InternalExecutor, txn, jobID, int32(ShowJobFail), timeutil.ToUnixMicros(ex.server.cfg.Clock.Now().GoTime()), timeutil.ToUnixMicros(ex.server.cfg.Clock.Now().GoTime()), err.Error())
				})
				if err != nil {
					return nil, nil, err
				}
			}
			return nil, nil, err
		}
	}

	if _, ok := stmt.AST.(*tree.Export); ok {
		var errorTime int64
		var errorInfo string
		jobStatus := int32(ShowJobSuccess)
		if res.Err() != nil {
			errorTime = timeutil.ToUnixMicros(ex.server.cfg.Clock.Now().GoTime())
			errorInfo = res.Err().Error()
			jobStatus = int32(ShowJobFail)
		}
		err := ex.server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return ShowJobsUpdate(ctx, ex.server.cfg.InternalExecutor, txn, jobID, jobStatus, timeutil.ToUnixMicros(ex.server.cfg.Clock.Now().GoTime()), errorTime, errorInfo)
		})
		if err != nil {
			return nil, nil, err
		}
	}
	if p.ShortCircuit {
		// reset the flag after exec, p.ShortCircuit is true when stable without child tables.
		p.ShortCircuit = false
	}
	if err := res.Err(); err != nil {
		return makeErrEvent(err)
	}

	txn := ex.state.mu.txn

	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetriableErr{
				IsCommit:     fsm.FromBool(isCommit(stmt.AST)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			txn.ManualRestart(ctx, ex.server.cfg.Clock.Now())
			payload := eventRetriableErrPayload{
				err: roachpb.NewTransactionRetryWithProtoRefreshError(
					"serializable transaction timestamp pushed (detected by connExecutor)",
					txn.ID(),
					// No updated transaction required; we've already manually updated our
					// client.Txn.
					roachpb.Transaction{},
				),
				rewCap: rc,
			}
			return ev, payload, nil
		}
	}
	// No event was generated.
	return nil, nil, nil
}

// checkTableTwoVersionInvariant checks whether any new table schema being
// modified written at a version V has only valid leases at version = V - 1.
// A transaction retry error is returned whenever the invariant is violated.
// Before returning the retry error the current transaction is
// rolled-back and the function waits until there are only outstanding
// leases on the current version. This affords the retry to succeed in the
// event that there are no other schema changes simultaneously contending with
// this txn.
//
// checkTableTwoVersionInvariant blocks until it's legal for the modified
// table descriptors (if any) to be committed.
// Reminder: a descriptor version v can only be written at a timestamp
// that's not covered by a lease on version v-2. So, if the current
// txn wants to write some updated descriptors, it needs
// to wait until all incompatible leases are revoked or expire. If
// incompatible leases exist, we'll block waiting for these leases to
// go away. Then, the transaction is restarted by generating a retriable error.
// Note that we're relying on the fact that the number of conflicting
// leases will only go down over time: no new conflicting leases can be
// created as of the time of this call because v-2 can't be leased once
// v-1 exists.
//
// If this method succeeds it is the caller's responsibility to release the
// executor's table leases after the txn commits so that schema changes can
// proceed.
func (ex *connExecutor) checkTableTwoVersionInvariant(ctx context.Context) error {
	tables := ex.extraTxnState.tables.getTablesWithNewVersion()
	if tables == nil {
		return nil
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		panic("transaction has already committed")
	}

	// We potentially hold leases for tables which we've modified which
	// we need to drop. Say we're updating tables at version V. All leases
	// for version V-2 need to be dropped immediately, otherwise the check
	// below that nobody holds leases for version V-2 will fail. Worse yet,
	// the code below loops waiting for nobody to hold leases on V-2. We also
	// may hold leases for version V-1 of modified tables that are good to drop
	// but not as vital for correctness. It's good to drop them because as soon
	// as this transaction commits jobs may start and will need to wait until
	// the lease expires. It is safe because V-1 must remain valid until this
	// transaction commits; if we commit then nobody else could have written
	// a new V beneath us because we've already laid down an intent.
	//
	// All this being said, we must retain our leases on tables which we have
	// not modified to ensure that our writes to those other tables in this
	// transaction remain valid.
	ex.extraTxnState.tables.releaseTableLeases(ctx, tables)

	// We know that so long as there are no leases on the updated tables as of
	// the current provisional commit timestamp for this transaction then if this
	// transaction ends up committing then there won't have been any created
	// in the meantime.
	count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, txn.ProvisionalCommitTimestamp())
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

	// Restart the transaction so that it is able to replay itself at a newer timestamp
	// with the hope that the next time around there will be leases only at the current
	// version.
	retryErr := txn.PrepareRetryableError(ctx,
		fmt.Sprintf(
			`cannot publish new versions for tables: %v, old versions still in use`,
			tables))
	// We cleanup the transaction and create a new transaction after
	// waiting for the invariant to be satisfied because the wait time
	// might be extensive and intents can block out leases being created
	// on a descriptor.
	//
	// TODO(vivek): Change this to restart a txn while fixing #20526 . All the
	// table descriptor intents can be laid down here after the invariant
	// has been checked.
	userPriority := txn.UserPriority()
	// We cleanup the transaction and create a new transaction wait time
	// might be extensive and so we'd better get rid of all the intents.
	txn.CleanupOnError(ctx, retryErr)
	// Release the rest of our leases on unmodified tables so we don't hold up
	// schema changes there and potentially create a deadlock.
	ex.extraTxnState.tables.releaseLeases(ctx)

	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := ex.server.cfg.Clock.Now()
		count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, now)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation != nil {
			ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation()
		}
	}

	// Create a new transaction to retry with a higher timestamp than the
	// timestamps used in the retry loop above.
	ex.state.mu.txn = kv.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db, ex.transitionCtx.nodeID)
	if err := ex.state.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	return retryErr
}

// commitSQLTransaction executes a commit after the execution of a
// stmt, which can be any statement when executing a statement with an
// implicit transaction, or a COMMIT statement when using an explicit
// transaction.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	err := ex.commitSQLTransactionInternal(ctx, stmt)
	if err != nil {
		return ex.makeErrEvent(err, stmt)
	}
	return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
}

func (ex *connExecutor) commitSQLTransactionInternal(
	ctx context.Context, stmt tree.Statement,
) error {
	if err := ex.extraTxnState.tables.validatePrimaryKeys(); err != nil {
		return err
	}

	if err := ex.checkTableTwoVersionInvariant(ctx); err != nil {
		return err
	}
	// This is the last step before committing a transaction.
	// This is a perfect time to refresh the deadline prior to committing.
	ex.extraTxnState.tables.MaybeUpdateDeadline(ctx, ex.state.mu.txn)

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return err
	}

	// Now that we've committed, if we modified any table we need to make sure
	// to release the leases for them so that the schema change can proceed and
	// we don't block the client.
	if tables := ex.extraTxnState.tables.getTablesWithNewVersion(); tables != nil {
		ex.extraTxnState.tables.releaseLeases(ctx)
	}
	return nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned; it is
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, planner *planner, res RestrictedCommandResult,
) error {
	stmt := planner.stmt

	if exp, ok := stmt.AST.(*tree.Export); ok {
		var auditFlag = true
		startTime := timeutil.Now()
		var id uint32
		tableInfo := &infos.TableInfo{
			Statement: stmt.String(),
			User:      planner.User(),
		}

		auditLog := func() {
			// dispatchToExecutionEngine is called recursively, affecting planner.curPlan.auditInfo,
			// so it is called in defer
			if auditFlag == true {
				planner.SetAuditTarget(id, tableInfo.TableName, nil)
				planner.LogAudit(ctx, planner.Txn(), 0, res.Err(), startTime)
			}
		}
		defer auditLog()
		expOpts, err := checkBeforeExport(ctx, planner, res, ex, exp)
		if err != nil {
			return nil
		}
		if exp.Settings {
			if exp.FileFormat != "SQL" {
				res.SetError(errors.Errorf("unsupported file format:%s while export cluster settings", exp.FileFormat))
				return nil
			}
			// Read the system.settings and Write into clustersetting.sql
			if err := getClusterSettingSQL(ctx, planner, exp.File.(*tree.StrVal).RawString(), res); err != nil {
				res.SetError(err)
				return nil
			}
			return nil
		}
		if exp.Users {
			if exp.FileFormat != "SQL" {
				res.SetError(errors.Errorf("unsupported file format:%s while export users", exp.FileFormat))
				return nil
			}
			// Read the system.users and Write into users.sql
			if err := getUserSQL(ctx, planner, exp.File.(*tree.StrVal).RawString(), res); err != nil {
				res.SetError(err)
				return nil
			}
			return nil
		}
		// export database
		if exp.Database != "" && exp.Query == nil {
			// database audit in exportRelationalAndTsDatabase function,so set auditFlag is false here
			auditFlag = false
			// dispatch's err should use res.SetError to return, these had been set in the func, so return nil
			_ = exportRelationalAndTsDatabase(ctx, planner, res, ex, exp, expOpts)
			return nil
		}

		// export ts table
		if tableName, onlyOneTable := getOnlyOneTableName(exp); tableName != nil {
			tsTable, err := planner.ResolveUncachedTableDescriptor(ctx, tableName, true, ResolveRequireTableDesc)
			if err != nil {
				res.SetError(err)
				return nil
			}
			tableInfo.TableName = tableName.TableName.String()
			id = uint32(tsTable.GetID())
			// write timeseries table create statement
			if tsTable.IsTSTable() {
				exp.IsTS = true
				// export more than one table can't export metadata
				// export from select either.
				if !expOpts.onlyData && exp.Database == "" && onlyOneTable && getTableSelect(stmt.AST) {
					if err = writeTimeSeriesMeta(ctx, planner, exp.File.(*tree.StrVal).RawString(), tsTable, res, expOpts.withComment, expOpts.withPrivileges); err != nil {
						res.SetError(err)
						return nil
					}
				}
			}
		}
	}

	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	ex.statsCollector.phaseTimes[plannerStartLogicalPlan] = timeutil.Now()

	// Prepare the plan. Note, the error is processed below. Everything
	// between here and there needs to happen even if there's an error.
	err := ex.makeExecPlan(ctx, planner)
	// We'll be closing the plan manually below after execution; this
	// defer is a catch-all in case some other return path is taken.
	defer planner.curPlan.close(ctx)

	if planner.autoCommit {
		planner.curPlan.flags.Set(planFlagImplicitTxn)
	}

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if planner.curPlan.avoidBuffering || ex.sessionData.AvoidBuffering {
		res.DisableBuffering()
	}

	defer func() {
		planner.maybeLogStatement(
			ctx,
			ex.executorType,
			ex.extraTxnState.autoRetryCounter,
			res.RowsAffected(),
			res.Err(),
			ex.statsCollector.phaseTimes[sessionQueryReceived],
		)
	}()

	ex.statsCollector.phaseTimes[plannerEndLogicalPlan] = timeutil.Now()
	ex.sessionTracing.TracePlanEnd(ctx, err)

	// Finally, process the planning error from above.
	if err != nil {
		res.SetError(err)
		return nil
	}

	var cols sqlbase.ResultColumns
	var stmtType tree.StatementType
	// Procedure has different statement type, getting compiled statement type when use procedure cache.
	if planner.curPlan.mem.ProcCacheHelper.ExecViaProcedureCache {
		stmtType = planner.curPlan.mem.ProcCacheHelper.ProcedureStmtType
	} else {
		stmtType = stmt.AST.StatementType()
	}
	if stmtType == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}
	if err := ex.initStatementResult(ctx, res, stmtType, cols); err != nil {
		res.SetError(err)
		return nil
	}

	ex.sessionTracing.TracePlanCheckStart(ctx)
	distributePlan := false
	distributePlan = shouldDistributePlan(
		ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner.curPlan.plan)
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan)

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String())
	}

	ex.statsCollector.phaseTimes[plannerStartExecStmt] = timeutil.Now()

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = distributePlan
	progAtomic := &queryMeta.progressAtomic
	ex.mu.Unlock()

	// We need to set the "exec done" flag early because
	// curPlan.close(), which will need to observe it, may be closed
	// during execution (PlanAndRun).
	//
	// TODO(knz): This is a mis-design. Andrei says "it's OK if
	// execution closes the plan" but it transfers responsibility to
	// run any "finalizers" on the plan (including plan sampling for
	// stats) to the execution engine. That's a lot of responsibility
	// to transfer! It would be better if this responsibility remained
	// around here.
	planner.curPlan.flags.Set(planFlagExecDone)

	if distributePlan {
		planner.curPlan.flags.Set(planFlagDistributed)
	} else {
		planner.curPlan.flags.Set(planFlagDistSQLLocal)
	}

	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	bytesRead, rowsRead, err := ex.execWithDistSQLEngine(ctx, planner, stmtType, res, distributePlan, progAtomic, stmt.SQL)
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	ex.statsCollector.phaseTimes[plannerEndExecStmt] = timeutil.Now()

	// Record the statement summary. This also closes the plan if the
	// plan has not been closed earlier.
	ex.recordStatementSummary(
		ctx, planner,
		ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err(), bytesRead, rowsRead,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return err
}

// makeExecPlan creates an execution plan and populates planner.curPlan, using
// either the optimizer or the heuristic planner.
func (ex *connExecutor) makeExecPlan(ctx context.Context, planner *planner) error {
	planner.curPlan.init(planner.stmt, ex.appStats)
	if planner.collectBundle {
		planner.curPlan.instrumentation.savePlanString = true
	}

	// if this is an implicit DDL, upgrade txn's iso Level to Serializable
	if tree.CanModifySchema(planner.stmt.AST) && ex.state.mu.txn.IsoLevel() == enginepb.ReadCommitted {
		if ex.implicitTxn() {
			if err := ex.state.setIsolationLevel(enginepb.Serializable); err != nil {
				return err
			}
			log.VEventf(ctx, 2, "transaction isolation level upgrade to serializable when implicit transaction level is read committed")
		} else {
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"can not use multi-statement transactions involving a schema change under weak isolation levels")
		}
	}

	if err := planner.makeOptimizerPlan(ctx); err != nil {
		// Capture stmt hint error. If it is an embedded hint, return err directly
		if strings.Contains(err.Error(), `Stmt Hint Err:`) && planner.EvalContext().HintStmtEmbed {
			return err
		}
		if externalErr := canFallbackFromExternal(err, planner.EvalContext().HintID); externalErr != "" {
			externalErr = "Failed:" + externalErr
			setHintStatus := `
UPDATE
defaultdb.public.query_hint_table
set
Hint_Status = $1
WHERE
Hint_id=$2
`
			a := planner.getHintID()
			_, err = planner.execCfg.InternalExecutor.Query(
				ctx, "set-hintStatus", nil /* txn */, setHintStatus, externalErr, a)
			if err != nil {
				return err
			}

			// no hint optimize
			planner.EvalContext().HintReoptimize = true
			if err = planner.makeOptimizerPlan(ctx); err == nil {

				// TODO(knz): Remove this accounting if/when savepoint rollbacks
				// support rolling back over DDL.
				if planner.curPlan.flags.IsSet(planFlagIsDDL) {
					ex.extraTxnState.numDDL++
				}
				planner.EvalContext().HintReoptimize = false
				return nil
			}
			planner.EvalContext().HintReoptimize = false
		}

		log.VEventf(ctx, 1, "optimizer plan failed: %v", err)
		return err
	}

	//
	readOnly := sqlbase.ReadOnly.Get(planner.ExecCfg().SV())
	if !strings.HasPrefix(planner.SessionData().ApplicationName, sqlbase.InternalAppNamePrefix) && (readOnly || sqlbase.ReadOnlyInternal) {
		o := planObserver{
			enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
				return true, nil
			},
			leaveNode: func(_ string, n planNode) (err error) {
				err = checkPlanNodeType(n)
				return err
			},
		}
		err := walkPlan(ctx, planner.curPlan.plan, o)
		if err != nil {
			return err
		}
	}

	// time-series table transactions do not require retries
	if planner.TsInScopeFlag {
		planner.txn.NotReplyForTS = true
	}

	hintID := planner.getHintID()
	if hintID > 0 {
		hintState := "Applied"
		setHintStatus := `
UPDATE
defaultdb.public.query_hint_table
set
Hint_Status = $1
WHERE
Hint_id=$2
`
		_, err := planner.execCfg.InternalExecutor.Query(
			ctx, "set-hintStatus", nil /* txn */, setHintStatus, hintState, hintID)
		if err != nil {
			return err
		}

	}

	// TODO(knz): Remove this accounting if/when savepoint rollbacks
	// support rolling back over DDL.
	if planner.curPlan.flags.IsSet(planFlagIsDDL) {
		ex.extraTxnState.numDDL++
	}

	return nil
}

// execWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
// stmt is use for displaying sql in trace.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context,
	planner *planner,
	stmtType tree.StatementType,
	res RestrictedCommandResult,
	distribute bool,
	progressAtomic *uint64,
	stmt string,
) (bytesRead, rowsRead int64, _ error) {
	recv := MakeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			ex.server.cfg.Clock.Update(ts)
		},
		&ex.sessionTracing,
	)
	recv.progressAtomic = progressAtomic
	defer recv.Release()

	evalCtx := planner.ExtendedEvalContext()
	var planCtx *PlanningCtx
	if distribute {
		planCtx = ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		planCtx = ex.server.cfg.DistSQLPlanner.newLocalPlanningCtx(ctx, evalCtx)
	}
	planCtx.isLocal = !distribute
	planCtx.planner = planner
	planCtx.stmtType = recv.stmtType
	planCtx.tsTableReaderID = 0
	if planner.collectBundle {
		planCtx.saveDiagram = func(diagram execinfrapb.FlowDiagram) {
			planner.curPlan.distSQLDiagrams = append(planner.curPlan.distSQLDiagrams, diagram)
		}
	}

	var evalCtxFactory func() *extendedEvalContext
	if len(planner.curPlan.subqueryPlans) != 0 || len(planner.curPlan.postqueryPlans) != 0 {
		// The factory reuses the same object because the contexts are not used
		// concurrently.
		var factoryEvalCtx extendedEvalContext
		ex.initEvalCtx(ctx, &factoryEvalCtx, planner)
		evalCtxFactory = func() *extendedEvalContext {
			ex.resetEvalCtx(&factoryEvalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
			factoryEvalCtx.Placeholders = &planner.semaCtx.Placeholders
			factoryEvalCtx.Annotations = &planner.semaCtx.Annotations
			// Query diagnostics can change the Context; make sure we are using the
			// same one.
			// TODO(radu): consider removing this if/when #46164 is addressed.
			factoryEvalCtx.Context = evalCtx.Context
			return &factoryEvalCtx
		}
	}

	// need to return empty when stable without ctables.
	if planner.ShortCircuit {
		return recv.bytesRead, recv.rowsRead, recv.commErr
	}
	if len(planner.curPlan.subqueryPlans) != 0 {
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, distribute,
		) {
			return recv.bytesRead, recv.rowsRead, recv.commErr
		}
	}
	recv.discardRows = planner.discardRows
	// We pass in whether or not we wanted to distribute this plan, which tells
	// the planner whether or not to plan remote table readers.
	cleanup := ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.plan, recv, stmt,
	)
	ex.sendDedupClientNotice(ctx, planner, recv)
	//collect the TotalMaxMem and TotalMaxDisk
	// Note that we're not cleaning up right away because postqueries might
	// need to have access to the main query tree.
	defer cleanup()
	if recv.commErr != nil || res.Err() != nil {
		return recv.bytesRead, recv.rowsRead, recv.commErr
	}
	// relational export will print meta.sql's info here, time series will print succeed here
	// we should deal err above first
	if exp, ok := planner.stmt.AST.(*tree.Export); ok {
		exportNode, ok := planner.curPlan.plan.(*exportNode)
		if !ok {
			return 0, 0, errors.Errorf("planner.curPlan.plan is not exportNode.")
		}
		// only export time series table should print succeed here, export database had printed above
		if exp.IsTS && exp.Database == "" {
			row := tree.Datums{
				tree.NewDString("succeed"),
			}
			res.SetColumns(ctx, sqlbase.ExportTsColumns)
			if err := res.AddRow(ctx, row); err != nil {
				return 0, 0, err
			}
		}
		if !exportNode.expOpts.onlyData && !exp.IsTS {
			part := "meta"
			filename := strings.Replace(exportFilePatternSQL, exportFilePatternPart, part, -1)
			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(1)),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(planner.execCfg.NodeID.Get())),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(1)),
				),
			}
			recv.Push(res, nil)
		}
	}
	if len(planner.curPlan.postqueryPlans) != 0 {
		ex.server.cfg.DistSQLPlanner.PlanAndRunPostqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.postqueryPlans, recv, distribute,
		)
	}
	return recv.bytesRead, recv.rowsRead, recv.commErr
}

// sendDedupClientNotice sends a notice out-of-band to the client.
func (ex *connExecutor) sendDedupClientNotice(
	ctx context.Context, planner *planner, recv *DistSQLReceiver,
) {
	if recv.useDeepRule {
		var errMsg error
		var errString string
		switch recv.dedupRule {
		case int64(execinfrapb.DedupRule_TsReject):
			errString = "rejected"
			errMsg = errors.Newf(
				" %d rows %s\nDetails: %d rows rejected due to duplicate data. Check logs under the user data directory for more information.",
				recv.dedupRows, errString, recv.dedupRows)
			planner.SendClientNotice(ctx, errMsg)
		case int64(execinfrapb.DedupRule_TsDiscard):
			errString = "discarded"
			errMsg = errors.Newf(
				" %d rows %s\nDetails: %d rows discarded due to duplicate data. Check logs under the user data directory for more information.",
				recv.dedupRows, errString, recv.dedupRows)
			planner.SendClientNotice(ctx, errMsg)
		default:
			log.Errorf(ctx, "the dedupRule %d not support to send msg to client, do nothing.\n", recv.dedupRule)
			// only have TsReject and TsDiscard
		}
	}
}

// SendDetupClientNoticeToRes is used to insert_direct send warning messages
func (ex *connExecutor) sendDedupClientNoticeToRes(
	ctx context.Context, r CommandResult, useDeepRule bool, dedupRule int64, dedupRows int64,
) {
	if useDeepRule {
		var errMsg error
		var errString string
		switch dedupRule {
		case int64(execinfrapb.DedupRule_TsReject):
			errString = "rejected"
			errMsg = errors.Newf(
				" %d rows %s\nDetails: %d rows rejected due to duplicate data. Check logs under the user data directory for more information.",
				dedupRows, errString, dedupRows)
			r.AppendNotice(errMsg)
			//planner.SendClientNotice(ctx, errMsg)
		case int64(execinfrapb.DedupRule_TsDiscard):
			errString = "discarded"
			errMsg = errors.Newf(
				" %d rows %s\nDetails: %d rows discarded due to duplicate data. Check logs under the user data directory for more information.",
				dedupRows, errString, dedupRows)
			r.AppendNotice(errMsg)
			//planner.SendClientNotice(ctx, errMsg)
		default:
			log.Errorf(ctx, "the dedupRule %d not support to send msg to client, do nothing.\n", dedupRule)
			// only have TsReject and TsDiscard
		}
	}
}

func (ex *connExecutor) sendBatchErrorToRes(r CommandResult, Rows int) {
	errString := "rejected"
	errMsg := errors.Newf(
		" %d rows %s\nDetails: %d rows rejected due to err. Check logs under the user data directory for more information.",
		Rows, errString, Rows)
	r.AppendNotice(errMsg)
}

// beginTransactionTimestampsAndReadMode computes the timestamps and
// ReadWriteMode to be used for the associated transaction state based on the
// values of the statement's Modes. Note that this method may reset the
// connExecutor's planner in order to compute the timestamp for the AsOf clause
// if it exists. The timestamps correspond to the timestamps passed to
// makeEventTxnStartPayload; txnSQLTimestamp propagates to become the
// TxnTimestamp while historicalTimestamp populated with a non-nil value only
// if the BeginTransaction statement has a non-nil AsOf clause expression. A
// non-nil historicalTimestamp implies a ReadOnly rwMode.
func (ex *connExecutor) beginTransactionTimestampsAndReadMode(
	ctx context.Context, s *tree.BeginTransaction,
) (
	rwMode tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	err error,
) {
	now := ex.server.cfg.Clock.Now()
	if s.Modes.AsOf.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode)
		return rwMode, now.GoTime(), nil, nil
	}
	ex.statsCollector.reset(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
	p := &ex.planner
	ex.resetPlanner(ctx, p, nil /* txn */, now.GoTime())
	ts, err := p.EvalAsOfTimestamp(s.Modes.AsOf)
	if err != nil {
		return 0, time.Time{}, nil, err
	}
	// NB: This check should never return an error because the parser should
	// disallow the creation of a TransactionModes struct which both has an
	// AOST clause and is ReadWrite but performing a check decouples this code
	// from that and hopefully adds clarity that the returning of ReadOnly with
	// a historical timestamp is intended.
	if s.Modes.ReadWriteMode == tree.ReadWrite {
		return 0, time.Time{}, nil, tree.ErrAsOfSpecifiedWithReadWrite
	}
	return tree.ReadOnly, ts.GoTime(), &ts, nil
}

// execStmtInNoTxnState "executes" a statement when no transaction is in scope.
// For anything but BEGIN, this method doesn't actually execute the statement;
// it just returns an Event that will generate a transaction. The statement will
// then be executed again, but this time in the Open state (implicit txn).
//
// Note that eventTxnStart, which is generally returned by this method, causes
// the state to change and previous results to be flushed, but for implicit txns
// the cursor is not advanced. This means that the statement will run again in
// stateOpen, at each point its results will also be flushed.
func (ex *connExecutor) execStmtInNoTxnState(
	ctx context.Context, stmt Statement,
) (_ fsm.Event, payload fsm.EventPayload) {
	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		ex.incrementStartedStmtCounter(stmt)
		defer func() {
			if !payloadHasError(payload) {
				ex.incrementExecutedStmtCounter(stmt)
			}
		}()
		pri, err := priorityToProto(s.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		mode, sqlTs, historicalTs, err := ex.beginTransactionTimestampsAndReadMode(ctx, s)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				pri, mode, sqlTs,
				historicalTs,
				ex.transitionCtx,
				ex.txnIsolationLevelToKV(s.Modes.Isolation),
			)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
	default:
		mode := tree.ReadWrite
		if ex.sessionData.DefaultReadOnly {
			mode = tree.ReadOnly
		}
		// NB: Implicit transactions are created without a historical timestamp even
		// though the statement might contain an AOST clause. In these cases the
		// clause is evaluated and applied execStmtInOpenState.
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				roachpb.NormalUserPriority,
				mode,
				ex.server.cfg.Clock.PhysicalTime(),
				nil, /* historicalTimestamp */
				ex.transitionCtx,
				ex.txnIsolationLevelToKV(tree.UnspecifiedIsolation))
	}
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
//   - COMMIT / ROLLBACK: aborts the current transaction.
//   - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//     allowing it to be retried.
func (ex *connExecutor) execStmtInAbortedState(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) (_ fsm.Event, payload fsm.EventPayload) {
	ex.incrementStartedStmtCounter(stmt)
	defer func() {
		if !payloadHasError(payload) {
			ex.incrementExecutedStmtCounter(stmt)
		}
	}()

	reject := func() (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}

	switch s := stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if _, ok := s.(*tree.CommitTransaction); ok {
			// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
			res.ResetStmtType((*tree.RollbackTransaction)(nil))
		}
		return ex.rollbackSQLTransaction(ctx)

	case *tree.RollbackToSavepoint:
		return ex.execRollbackToSavepointInAbortedState(ctx, s)

	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(s.Name) {
			// We allow SAVEPOINT kwbase_restart as an alternative to ROLLBACK TO
			// SAVEPOINT kwbase_restart in the Aborted state. This is needed
			// because any client driver (that we know of) which links subtransaction
			// `ROLLBACK/RELEASE` to an object's lifetime will fail to `ROLLBACK` on a
			// failed `RELEASE`. Instead, we now can use the creation of another
			// subtransaction object (which will issue another `SAVEPOINT` statement)
			// to indicate retry intent. Specifically, this change was prompted by
			// subtransaction handling in `libpqxx` (C++ driver) and `rust-postgres`
			// (Rust driver).
			res.ResetStmtType((*tree.RollbackToSavepoint)(nil))
			return ex.execRollbackToSavepointInAbortedState(
				ctx, &tree.RollbackToSavepoint{Savepoint: s.Name})
		}
		return reject()

	default:
		return reject()
	}
}

// execStmtInCommitWaitState executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res RestrictedCommandResult,
) (ev fsm.Event, payload fsm.EventPayload) {
	ex.incrementStartedStmtCounter(stmt)
	defer func() {
		if !payloadHasError(payload) {
			ex.incrementExecutedStmtCounter(stmt)
		}
	}()
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
	default:
		ev = eventNonRetriableErr{IsCommit: fsm.False}
		payload = eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionCommittedError(),
		}
		return ev, payload
	}
}

// runObserverStatement executes the given observer statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runObserverStatement(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) error {
	switch sqlStmt := stmt.AST.(type) {
	case *tree.ShowTransactionStatus:
		return ex.runShowTransactionState(ctx, res)
	case *tree.ShowSavepointStatus:
		return ex.runShowSavepointState(ctx, res)
	case *tree.ShowSyntax:
		return ex.runShowSyntax(ctx, sqlStmt.Statement, res)
	case *tree.SetTracing:
		ex.runSetTracing(ctx, sqlStmt, res)
		return nil
	default:
		res.SetError(errors.AssertionFailedf("unrecognized observer statement type %T", stmt.AST))
		return nil
	}
}

// runShowSyntax executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ShowSyntaxColumns)
	var commErr error
	parser.RunShowSyntax(ctx, stmt,
		func(ctx context.Context, field, msg string) {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
		},
		func(ctx context.Context, err error) {
			sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
		},
	)
	return commErr
}

// runShowTransactionState executes a SHOW TRANSACTION STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	state := fmt.Sprintf("%s", ex.machine.CurState())
	return res.AddRow(ctx, tree.Datums{tree.NewDString(state)})
}

func (ex *connExecutor) runSetTracing(
	ctx context.Context, n *tree.SetTracing, res RestrictedCommandResult,
) {
	if len(n.Values) == 0 {
		res.SetError(errors.AssertionFailedf("set tracing missing argument"))
		return
	}

	modes := make([]string, len(n.Values))
	for i, v := range n.Values {
		v = unresolvedNameToStrVal(v)
		var strMode string
		switch val := v.(type) {
		case *tree.StrVal:
			strMode = val.RawString()
		case *tree.DBool:
			if *val {
				strMode = "on"
			} else {
				strMode = "off"
			}
		default:
			res.SetError(pgerror.New(pgcode.Syntax,
				"expected string or boolean for set tracing argument"))
			return
		}
		modes[i] = strMode
	}

	if err := ex.enableTracing(modes); err != nil {
		res.SetError(err)
	}
}

func (ex *connExecutor) enableTracing(modes []string) error {
	traceKV := false
	recordingType := tracing.SnowballRecording
	enableMode := true
	showResults := false

	for _, s := range modes {
		switch strings.ToLower(s) {
		case "results":
			showResults = true
		case "on":
			enableMode = true
		case "off":
			enableMode = false
		case "kv":
			traceKV = true
		case "local":
			recordingType = tracing.SingleNodeRecording
		case "cluster":
			recordingType = tracing.SnowballRecording
		default:
			return pgerror.Newf(pgcode.Syntax,
				"set tracing: unknown mode %q", s)
		}
	}
	if !enableMode {
		return ex.sessionTracing.StopTracing()
	}
	return ex.sessionTracing.StartTracing(recordingType, traceKV, showResults)
}

// addActiveQuery adds a running query to the list of running queries.
//
// It returns a cleanup function that needs to be run when the query is no
// longer executing. NOTE(andrei): As of Feb 2018, "executing" does not imply
// that the results have been delivered to the client.
func (ex *connExecutor) addActiveQuery(
	ctx context.Context, queryID ClusterWideID, stmt Statement, cancelFun context.CancelFunc,
) func() {
	_, hidden := stmt.AST.(tree.HiddenFromShowQueries)
	qm := &queryMeta{
		txnID:         ex.state.mu.txn.ID(),
		start:         ex.phaseTimes[sessionQueryReceived],
		rawStmt:       stmt.SQL,
		phase:         preparing,
		isDistributed: false,
		ctxCancel:     cancelFun,
		hidden:        hidden,
	}
	ex.mu.Lock()
	ex.mu.ActiveQueries[queryID] = qm
	ex.mu.Unlock()
	return func() {
		ex.mu.Lock()
		_, ok := ex.mu.ActiveQueries[queryID]
		if !ok {
			ex.mu.Unlock()
			panic(fmt.Sprintf("query %d missing from ActiveQueries", queryID))
		}
		delete(ex.mu.ActiveQueries, queryID)
		ex.mu.LastActiveQuery = stmt.AST

		ex.mu.Unlock()
	}
}

// handleAutoCommit commits the KV transaction if it hasn't been committed
// already.
//
// It's possible that the statement constituting the implicit txn has already
// committed it (in case it tried to run as a 1PC). This method detects that
// case.
// NOTE(andrei): It bothers me some that we're peeking at txn to figure out
// whether we committed or not, where SQL could already know that - individual
// statements could report this back through the Event.
//
// Args:
// stmt: The statement that we just ran.
func (ex *connExecutor) handleAutoCommit(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		log.Event(ctx, "statement execution committed the txn")
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	}

	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		if err := knob(ctx, stmt.String()); err != nil {
			return ex.makeErrEvent(err, stmt)
		}
	}

	ev, payload := ex.commitSQLTransaction(ctx, stmt)
	var err error
	if perr, ok := payload.(payloadWithError); ok {
		err = perr.errorCause()
	}
	log.VEventf(ctx, 2, "AutoCommit. err: %v", err)
	return ev, payload
}

// incrementStartedStmtCounter increments the appropriate started
// statement counter for stmt's type.
func (ex *connExecutor) incrementStartedStmtCounter(stmt Statement) {
	ex.metrics.StartedStatementCounters.incrementCount(ex, stmt.AST)
}

// incrementExecutedStmtCounter increments the appropriate executed
// statement counter for stmt's type.
func (ex *connExecutor) incrementExecutedStmtCounter(stmt Statement) {
	ex.metrics.ExecutedStatementCounters.incrementCount(ex, stmt.AST)
}

// payloadHasError returns true if the passed payload implements
// payloadWithError.
func payloadHasError(payload fsm.EventPayload) bool {
	_, hasErr := payload.(payloadWithError)
	return hasErr
}

// recordTransactionStart records the start of the transaction and returns a
// closure to be called once the transaction finishes.
func (ex *connExecutor) recordTransactionStart() func(txnEvent) {
	ex.state.mu.RLock()
	txnStart := ex.state.mu.txnStart
	ex.state.mu.RUnlock()
	implicit := ex.implicitTxn()
	return func(ev txnEvent) { ex.recordTransaction(ev, implicit, txnStart) }
}

func (ex *connExecutor) recordTransaction(ev txnEvent, implicit bool, txnStart time.Time) {
	txnEnd := timeutil.Now()
	txnTime := txnEnd.Sub(txnStart)
	ex.metrics.EngineMetrics.SQLTxnLatency.RecordValue(txnTime.Nanoseconds())
	ex.statsCollector.recordTransaction(
		txnTime.Seconds(),
		ev,
		implicit,
	)
}

func canFallbackFromExternal(err error, isExternalHint int64) string {
	if strings.Contains(err.Error(), `hint info`) && isExternalHint > 0 {
		return "ERR HINT INFO"
	}
	if strings.Contains(err.Error(), `could not produce a query plan conforming to the`) && isExternalHint > 0 {
		return "AJMHJDG"
	}

	pgerr, ok := err.(*pgerror.Error)
	if !ok {
		return ""
	}
	switch pgerr.Code {
	case pgcode.ExternalBindLookupRight, pgcode.ExternalBindUnknowType,
		pgcode.ExternalBindNoTable, pgcode.ExternalBindRealOrder,
		pgcode.ExternalBindScanIndex, pgcode.ExternalBindJoinIndex,
		pgcode.ExternalApplyMergeJoin, pgcode.ExternalApplyLookupJoin,
		pgcode.ExternalApplyHashJoinDisallow, pgcode.ExternalApplyMergeJoinDisallow,
		pgcode.ExternalApplyLookupJoinDisallow,
		pgcode.ExternalApplyTableScanUse, pgcode.ExternalApplyTableScanForce,
		pgcode.ExternalApplyTableScanIgnore,
		pgcode.ExternalApplyIndexScanUse, pgcode.ExternalApplyIndexScanForce,
		pgcode.ExternalApplyIndexScanIgnore,
		pgcode.ExternalApplyIndexOnlyUse, pgcode.ExternalApplyIndexOnlyForce,
		pgcode.ExternalApplyIndexOnlyIgnore,
		pgcode.ExternalApplyCardinalityScan, pgcode.ExternalApplyCardinalityJoin:
		return pgerr.Code
	default:
		return ""
	}
}

// SendRes IncrementRowsAffected
func (h ConnectionHandler) SendRes(
	ctx context.Context, insertRows int, AST tree.Statement, err error,
) error {
	stmtRes := h.ex.clientComm.CreateStatementResult(
		AST,
		NeedRowDesc,
		1,
		nil, /* formatCodes */
		h.ex.sessionData.DataConversion,
		0,  /* limit */
		"", /* portalName */
		h.ex.implicitTxn(),
	)
	if err != nil {
		stmtRes.SetError(err)
	} else {
		stmtRes.IncrementRowsAffected(insertRows) //todo
	}
	stmtRes.Close(ctx, IdleTxnBlock)

	return nil
}

func checkPlanNodeType(n planNode) error {
	switch n.(type) {
	case *valuesNode, *delayedNode, *scanBufferNode, *setVarNode,
		*scanNode, *tsScanNode, *groupNode, *joinNode, *projectSetNode,
		*limitNode, *explainDistSQLNode, *explainPlanNode, *explainVecNode, *sortNode, *unionNode,
		*distinctNode, *renderNode, *applyJoinNode, *indexJoinNode, *lookupJoinNode, *zigzagJoinNode,
		*filterNode, *windowNode, *zeroNode, *showTraceNode, *showTraceReplicaNode, *showFingerprintsNode, *unaryNode:
		return nil
	case *setClusterSettingNode:
		if n.(*setClusterSettingNode).name != sqlbase.ClusterSettingReadOnly {
			return errors.Errorf("cannot SET CLUSTER SETTING in a read-only transaction except \"%s\";", sqlbase.ClusterSettingReadOnly)
		}
		return nil
	default:
		return errors.Errorf("cannot execute CREATE,INSERT,UPDATE,UPSERT,DELETE,DROP,ALTER,TRUNCATE,RENAME in a read-only transaction")
	}
}

// Each statement in an explicit READ COMMITTED transaction has a SAVEPOINT.
// This allows for TransactionRetry errors to be retried automatically. We don't
// do this for implicit transactions because the conn_executor state machine
// already has retry logic for implicit transactions. To avoid having to
// implement the retry logic in the state machine, we use the KV savepoint API
// directly.
// To address errors caused by write-write conflicts under the RC (Read Committed)
// isolation level: previously, in scenarios where a write-write conflict occurred
// because the read timestamp of Transaction 1 was earlier than the commit timestamp
// of data written by Transaction 2, Transaction 1 would still write an intent upon
// encountering this error. During a subsequent retry, an error would occur when
// Transaction 1 encountered its own previously written intent while reading.
// When the retry timestamp was adjusted in the interceptor, incorrect results
// were produced because the transaction did not re-read the latest data. To resolve this,
// the approach was changed to statement-level retries. Under the RC isolation level,
// if the timestamp does not match, the process directly returns an error without writing
// subsequent intents. Additionally, a new method, dispatchReadCommittedStmtToExecutionEngine,
// was introduced to enable statement-level retries specifically for the RC isolation level.
func (ex *connExecutor) dispatchReadCommittedStmtToExecutionEngine(
	ctx context.Context, p *planner, res RestrictedCommandResult,
) error {
	readCommittedSavePointToken, err := ex.state.mu.txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}
	maxRetries := 5
	for attemptNum := 0; ; attemptNum++ {
		bufferPos := res.BufferedResultsLen()
		if err = ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
			return err
		}
		maybeRetriableErr := res.Err()
		if maybeRetriableErr == nil {
			// If there was no error, then we must release the savepoint and break.
			if err := ex.state.mu.txn.ReleaseSavepoint(ctx, readCommittedSavePointToken); err != nil {
				return err
			}
			break
		}

		// If the error does not allow for a partial retry, then stop. The error
		// is already set on res.Err() and will be returned to the client.
		var txnRetryErr *roachpb.TransactionRetryWithProtoRefreshError
		// todo: txnRetryErr.TxnMustRestartFromBeginning()
		if !errors.As(maybeRetriableErr, &txnRetryErr) || txnRetryErr.PrevTxnAborted() {
			break
		}
		// If we reached the maximum number of retries, then we must stop.
		if attemptNum == maxRetries {
			res.SetError(errors.Wrapf(
				maybeRetriableErr,
				"read committed retry limit exceeded; set by max_retries_for_read_committed=%d",
				maxRetries,
			))
			break
		}
		// In order to retry the statement, we need to clear any results and
		// errors that were buffered, rollback to the savepoint, then prepare the
		// kv.txn for the partial txn retry.
		if ableToClear := res.TruncateBufferedResults(bufferPos); !ableToClear {
			// If the buffer exceeded the maximum size, then it might have been
			// flushed and sent back to the client already. In that case, we can't
			// retry the statement.
			res.SetError(errors.Wrapf(
				maybeRetriableErr,
				"cannot automatically retry since some results were already sent to the client",
			))
			break
		}
		if knob := ex.server.cfg.TestingKnobs.OnReadCommittedStmtRetry; knob != nil {
			knob(txnRetryErr)
		}
		res.SetError(nil)
		if err := ex.state.mu.txn.RollbackToSavepoint(ctx, readCommittedSavePointToken); err != nil {
			return err
		}
		if err := ex.state.mu.txn.PrepareForPartialRetry(ctx); err != nil {
			return err
		}
		// todo: ex.state.mu.txn.Step(ctx, false /* allowReadTimestampStep */);
		if err := ex.state.mu.txn.Step(ctx); err != nil {
			return err
		}
		ex.state.mu.autoRetryCounter++
		ex.state.mu.autoRetryReason = txnRetryErr
	}
	return nil
}
