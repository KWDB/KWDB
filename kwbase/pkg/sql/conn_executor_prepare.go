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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/fsm"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func (ex *connExecutor) execPrepare(
	ctx context.Context, parseCmd PrepareStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return ex.makeErrEvent(err, parseCmd.AST)
	}

	// The anonymous statement can be overwritten.
	if parseCmd.Name != "" {
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[parseCmd.Name]; ok {
			err := pgerror.Newf(
				pgcode.DuplicatePreparedStatement,
				"prepared statement %q already exists", parseCmd.Name,
			)
			return retErr(err)
		}
	} else {
		// Deallocate the unnamed statement, if it exists.
		ex.deletePreparedStmt(ctx, "")
	}

	ps, err := ex.addPreparedStmt(
		ctx,
		parseCmd.Name,
		Statement{Statement: parseCmd.Statement},
		parseCmd.TypeHints,
		PreparedStatementOriginWire,
	)
	if err != nil {
		return retErr(err)
	}
	// Convert the inferred SQL types back to an array of pgwire Oids.
	if len(ps.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"more than %d arguments to prepared statement: %d",
				pgwirebase.MaxPreparedStatementArgs, len(ps.TypeHints)))
	}

	var types tree.PlaceholderTypes
	// prepare direct skip memo without types
	if ps.Insertdirectstmt.InsertFast {
		ps.PrepareInsertDirect = parseCmd.PrepareInsertDirect
		types = ps.TypeHints

		if int64(ps.PrepareMetadata.Statement.NumPlaceholders)/
			ps.PrepareMetadata.Insertdirectstmt.RowsAffected != int64(ps.PrepareInsertDirect.Inscolsnum) {
			ex.deletePreparedStmt(ctx, parseCmd.Name)
			return retErr(pgerror.Newf(
				pgcode.Syntax,
				"insert (row %d) has more expressions than target columns, %d expressions for %d targets",
				parseCmd.Insertdirectstmt.RowsAffected, ps.PrepareMetadata.Statement.NumPlaceholders, ps.PrepareInsertDirect.Inscolsnum))
		}
	} else {
		types = ps.Types
	}
	inferredTypes := make([]oid.Oid, len(types))
	copy(inferredTypes, parseCmd.RawTypeHints)

	var inscolsnum int
	var colName string
	var dit DirectInsertTable
	colDescMap := make(map[string]oid.Oid)
	if ps.Insertdirectstmt.InsertFast {
		for _, colDesc := range parseCmd.PrepareInsertDirect.Dit.ColsDesc {
			colDescMap[colDesc.Name] = colDesc.Type.InternalType.Oid
		}
		inscolsnum = parseCmd.PrepareInsertDirect.Inscolsnum
		dit = parseCmd.PrepareInsertDirect.Dit
	}

	for i := range types {
		if !ps.Insertdirectstmt.InsertFast {
			// OID to Datum is not a 1-1 mapping (for example, int4 and int8
			// both map to TypeInt), so we need to maintain the types sent by
			// the client.
			if inferredTypes[i] == 0 {
				t, _ := ps.ValueType(tree.PlaceholderIdx(i))
				inferredTypes[i] = t.Oid()
			}
			continue
		}
		if ps.TypeHints[i] == nil {
			if len(dit.Desc) == 0 {
				colName = dit.ColsDesc[i%inscolsnum].Name
			} else {
				colName = string(dit.Desc[i%inscolsnum])
			}
			if Oid, ok := colDescMap[colName]; ok {
				inferredTypes[i] = Oid
			}
		}
	}
	// Remember the inferred placeholder types so they can be reported on
	// Describe.
	ps.InferredTypes = inferredTypes
	return nil, nil
}

// addPreparedStmt creates a new PreparedStatement with the provided name using
// the given query. The new prepared statement is added to the connExecutor and
// also returned. It is illegal to call this when a statement with that name
// already exists (even for anonymous prepared statements).
//
// placeholderHints are used to assist in inferring placeholder types.
func (ex *connExecutor) addPreparedStmt(
	ctx context.Context,
	name string,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
	origin PreparedStatementOrigin,
) (*PreparedStatement, error) {
	if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
		panic(fmt.Sprintf("prepared statement already exists: %q", name))
	}

	// Prepare the query. This completes the typing of placeholders.
	prepared, err := ex.prepare(ctx, stmt, placeholderHints, origin)
	if err != nil {
		return nil, err
	}

	if err := prepared.memAcc.Grow(ctx, int64(len(name))); err != nil {
		return nil, err
	}
	ex.extraTxnState.prepStmtsNamespace.prepStmts[name] = prepared
	return prepared, nil
}

// prepare prepares the given statement.
//
// placeholderHints may contain partial type information for placeholders.
// prepare will populate the missing types. It can be nil.
//
// The PreparedStatement is returned (or nil if there are no results). The
// returned PreparedStatement needs to be close()d once its no longer in use.
func (ex *connExecutor) prepare(
	ctx context.Context,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
	origin PreparedStatementOrigin,
) (*PreparedStatement, error) {
	if placeholderHints == nil {
		placeholderHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
	}

	prepared := &PreparedStatement{
		PrepareMetadata: sqlbase.PrepareMetadata{
			PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
				TypeHints: placeholderHints,
			},
		},
		memAcc:   ex.sessionMon.MakeBoundAccount(),
		refCount: 1,

		createdAt: timeutil.Now(),
		origin:    origin,
	}
	// NB: if we start caching the plan, we'll want to keep around the memory
	// account used for the plan, rather than clearing it.
	defer prepared.memAcc.Clear(ctx)

	if stmt.AST == nil {
		return prepared, nil
	}
	prepared.Statement = stmt.Statement

	// Point to the prepared state, which can be further populated during query
	// preparation.
	stmt.Prepared = prepared

	if err := tree.ProcessPlaceholderAnnotations(stmt.AST, placeholderHints); err != nil {
		return nil, err
	}

	// Preparing needs a transaction because it needs to retrieve db/table
	// descriptors for type checking. If we already have an open transaction for
	// this planner, use it. Using the user's transaction here is critical for
	// proper deadlock detection. At the time of writing, it is the case that any
	// data read on behalf of this transaction is not cached for use in other
	// transactions. It's critical that this fact remain true but nothing really
	// enforces it. If we create a new transaction (newTxn is true), we'll need to
	// finish it before we return.

	var flags planFlags
	prepare := func(ctx context.Context, txn *kv.Txn) (err error) {
		ex.statsCollector.reset(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
		p := &ex.planner
		ex.resetPlanner(ctx, p, txn, ex.server.cfg.Clock.PhysicalTime() /* stmtTS */)
		p.stmt = &stmt
		p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
		if !stmt.Insertdirectstmt.InsertFast {
			flags, err = ex.populatePrepared(ctx, txn, placeholderHints, p)
		}
		return err
	}

	if txn := ex.state.mu.txn; txn != nil && txn.IsOpen() {
		// Use the existing transaction.
		if err := prepare(ctx, txn); err != nil {
			return nil, err
		}
	} else {
		// When executing a prepare statement through JDBC, an explicit transaction is initiated.
		// After this transaction ends, it cannot follow the normal TxnStateTransitions state machine
		// process to release the table lease. Therefore, the table lease should be actively released
		// after the transaction ends.
		defer ex.extraTxnState.tables.releaseTables(ctx)

		// Use a new transaction. This will handle retriable errors here rather
		// than bubbling them up to the connExecutor state machine.
		if err := ex.server.cfg.DB.Txn(ctx, prepare); err != nil {
			return nil, err
		}
	}

	// Account for the memory used by this prepared statement.
	if err := prepared.memAcc.Grow(ctx, prepared.MemoryEstimate()); err != nil {
		return nil, err
	}
	ex.updateOptCounters(flags)
	return prepared, nil
}

// populatePrepared analyzes and type-checks the query and populates
// stmt.Prepared.
func (ex *connExecutor) populatePrepared(
	ctx context.Context, txn *kv.Txn, placeholderHints tree.PlaceholderTypes, p *planner,
) (planFlags, error) {
	if before := ex.server.cfg.TestingKnobs.BeforePrepare; before != nil {
		if err := before(ctx, ex.planner.stmt.String(), txn); err != nil {
			return 0, err
		}
	}
	stmt := p.stmt
	if err := p.semaCtx.Placeholders.Init(stmt.NumPlaceholders, placeholderHints); err != nil {
		return 0, err
	}
	p.extendedEvalCtx.PrepareOnly = true

	protoTS, err := p.isAsOf(stmt.AST)
	if err != nil {
		return 0, err
	}
	if protoTS != nil {
		p.semaCtx.AsOfTimestamp = protoTS
		txn.SetFixedTimestamp(ctx, *protoTS)
	}

	// PREPARE has a limited subset of statements it can be run with. Postgres
	// only allows SELECT, INSERT, UPDATE, DELETE and VALUES statements to be
	// prepared.
	// See: https://www.postgresql.org/docs/current/static/sql-prepare.html
	// However, we allow a large number of additional statements.
	// As of right now, the optimizer only works on SELECT statements and will
	// fallback for all others, so this should be safe for the foreseeable
	// future.
	flags, err := p.prepareUsingOptimizer(ctx)
	if err != nil {
		log.VEventf(ctx, 1, "optimizer prepare failed: %v", err)
		return 0, err
	}
	log.VEvent(ctx, 2, "optimizer prepare succeeded")
	// stmt.Prepared fields have been populated.
	return flags, nil
}

func (ex *connExecutor) execBind(
	ctx context.Context, bindCmd BindStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	portalName := bindCmd.PortalName
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.Newf(
				pgcode.DuplicateCursor, "portal %q already exists", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
	}

	ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(pgerror.Newf(
			pgcode.InvalidSQLStatementName,
			"unknown prepared statement %q", bindCmd.PreparedStatementName))
	}

	numQArgs := uint16(len(ps.InferredTypes))
	var qargs tree.QueryArguments
	if ps.PrepareInsertDirect.Dit.ColsDesc == nil {
		// Decode the arguments, except for internal queries for which we just verify
		// that the arguments match what's expected.
		qargs = make(tree.QueryArguments, numQArgs)
	}
	if bindCmd.internalArgs != nil {
		if len(bindCmd.internalArgs) != int(numQArgs) {
			return retErr(
				pgwirebase.NewProtocolViolationErrorf(
					"expected %d arguments, got %d", numQArgs, len(bindCmd.internalArgs)))
		}
		for i, datum := range bindCmd.internalArgs {
			t := ps.InferredTypes[i]
			if oid := datum.ResolvedType().Oid(); datum != tree.DNull && oid != t {
				return retErr(
					pgwirebase.NewProtocolViolationErrorf(
						"for argument %d expected OID %d, got %d", i, t, oid))
			}
			qargs[i] = datum
		}
	} else {
		qArgFormatCodes := bindCmd.ArgFormatCodes

		// If there is only one format code, then that format code is used to decode all the
		// arguments. But if the number of format codes provided does not match the number of
		// arguments AND it's not a single format code then we cannot infer what format to use to
		// decode all of the arguments.
		if len(qArgFormatCodes) != 1 && len(qArgFormatCodes) != int(numQArgs) {
			return retErr(pgwirebase.NewProtocolViolationErrorf(
				"wrong number of format codes specified: %d for %d arguments",
				len(qArgFormatCodes), numQArgs))
		}

		// If a single format code is provided and there is more than one argument to be decoded,
		// then expand qArgFormatCodes to the number of arguments provided.
		// If the number of format codes matches the number of arguments then nothing needs to be
		// done.
		if len(qArgFormatCodes) == 1 && numQArgs > 1 {
			fmtCode := qArgFormatCodes[0]
			qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgs)
			for i := range qArgFormatCodes {
				qArgFormatCodes[i] = fmtCode
			}
		}

		if len(bindCmd.Args) != int(numQArgs) {
			return retErr(
				pgwirebase.NewProtocolViolationErrorf(
					"expected %d arguments, got %d", numQArgs, len(bindCmd.Args)))
		}

		ptCtx := tree.NewParseTimeContext(ex.state.sqlTimestamp.In(ex.sessionData.DataConversion.Location))
		if ps.PrepareInsertDirect.Dit.ColsDesc == nil {
			for i, arg := range bindCmd.Args {
				k := tree.PlaceholderIdx(i)
				t := ps.InferredTypes[i]
				if arg == nil {
					// nil indicates a NULL argument value.
					qargs[k] = tree.DNull
				} else {
					d, err := pgwirebase.DecodeOidDatum(ptCtx, t, qArgFormatCodes[i], arg)
					if err != nil {
						return retErr(pgerror.Wrapf(err, pgcode.ProtocolViolation,
							"error in argument for %s", k))
					}
					qargs[k] = d
				}
			}
		}
	}

	numCols := len(ps.Columns)
	if (len(bindCmd.OutFormats) > 1) && (len(bindCmd.OutFormats) != numCols) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"expected 1 or %d for number of format codes, got %d",
			numCols, len(bindCmd.OutFormats)))
	}

	columnFormatCodes := bindCmd.OutFormats
	if len(bindCmd.OutFormats) == 1 && numCols > 1 {
		// Apply the format code to every column.
		columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
		for i := 0; i < numCols; i++ {
			columnFormatCodes[i] = bindCmd.OutFormats[0]
		}
	}

	// Create the new PreparedPortal.
	if err := ex.addPortal(ctx, portalName, ps, qargs, columnFormatCodes); err != nil {
		return retErr(err)
	}

	if log.V(2) {
		log.Infof(ctx, "portal: %q for %q, args %q, formats %q",
			portalName, ps.Statement, qargs, columnFormatCodes)
	}

	return nil, nil
}

// Prepare insert_direct bind process, splice payload here to send
func (ex *connExecutor) execPreparedirectBind(
	ctx context.Context, bindCmd BindStmt,
) (fsm.Event, fsm.EventPayload) {
	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	portalName := bindCmd.PortalName
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.Newf(
				pgcode.DuplicateCursor, "portal %q already exists", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
	}

	ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(pgerror.Newf(
			pgcode.InvalidSQLStatementName,
			"unknown prepared statement %q", bindCmd.PreparedStatementName))
	}

	numQArgs := uint16(len(ps.InferredTypes))
	if bindCmd.internalArgs != nil {
		if len(bindCmd.internalArgs) != int(numQArgs) {
			return retErr(
				pgwirebase.NewProtocolViolationErrorf(
					"expected %d arguments, got %d", numQArgs, len(bindCmd.internalArgs)))
		}
	} else {
		qArgFormatCodes := bindCmd.ArgFormatCodes
		if len(qArgFormatCodes) != 1 && len(qArgFormatCodes) != int(numQArgs) {
			return retErr(pgwirebase.NewProtocolViolationErrorf(
				"wrong number of format codes specified: %d for %d arguments",
				len(qArgFormatCodes), numQArgs))
		}

		if len(qArgFormatCodes) == 1 && numQArgs > 1 {
			fmtCode := qArgFormatCodes[0]
			qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgs)
			for i := range qArgFormatCodes {
				qArgFormatCodes[i] = fmtCode
			}
			bindCmd.ArgFormatCodes = qArgFormatCodes
		}

		if len(bindCmd.Args) != int(numQArgs) {
			return retErr(
				pgwirebase.NewProtocolViolationErrorf(
					"expected %d arguments, got %d", numQArgs, len(bindCmd.Args)))
		}
	}

	if ins, ok := ps.PrepareMetadata.Statement.AST.(*tree.Insert); ok {
		var di DirectInsert
		copy(ins.Columns, ps.PrepareInsertDirect.Dit.Desc)

		ie := ex.server.GetCFG().InternalExecutor
		cfg := ie.GetServer().GetCFG()

		err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			tables := GetTableCollection(cfg, ie)
			defer tables.ReleaseTSTables(ctx)

			flags := tree.ObjectLookupFlags{}
			table, err := tables.GetTableVersion(ctx, txn, ps.PrepareInsertDirect.Dit.Tname, flags)
			if table == nil || err != nil {
				return err
			}
			var sd sessiondata.SessionData
			EvalContext := tree.EvalContext{
				Context:          ctx,
				Txn:              txn,
				SessionData:      &sd,
				InternalExecutor: ex.server.GetCFG().InternalExecutor,
				Settings:         ex.server.GetCFG().Settings,
			}
			EvalContext.StartSinglenode = (ex.server.GetCFG().StartMode == StartSingleNode)
			if err := GetColsInfo(ctx, EvalContext, &ps.PrepareInsertDirect.Dit.ColsDesc, ins, &di, &ps.PrepareMetadata.Statement); err != nil {
				return err
			}
			di.RowNum, di.ColNum = len(bindCmd.Args), len(di.IDMap)
			di.PArgs.TSVersion = uint32(table.TsTable.TsVersion)
			ptCtx := tree.NewParseTimeContext(ex.state.sqlTimestamp.In(ex.sessionData.DataConversion.Location))
			// Calculate rowTimestamps and save the value of the timestamp column
			inputValues, rowTimestamps, err := TsprepareTypeCheck(ptCtx, bindCmd.Args, ps.InferredTypes, bindCmd.ArgFormatCodes, &ps.PrepareInsertDirect.Dit.ColsDesc, di)
			if err != nil {
				return err
			}
			if !EvalContext.StartSinglenode {
				//start mode
				di.PayloadNodeMap = make(map[int]*sqlbase.PayloadForDistTSInsert, 1)
				if err = BuildRowBytesForPrepareTsInsert(ptCtx, bindCmd.Args, ps.PrepareInsertDirect.Dit, &di, EvalContext, table, cfg.NodeInfo.NodeID.Get(), rowTimestamps); err != nil {
					return err
				}

			} else {
				// start-single-node mode
				priTagValMap := BuildPreparepriTagValMap(bindCmd.Args, di)
				di.PayloadNodeMap = make(map[int]*sqlbase.PayloadForDistTSInsert, 1)
				for _, priTagRowIdx := range priTagValMap {
					if err = BuildPreparePayload(&EvalContext, inputValues, priTagRowIdx,
						&di, ps.PrepareInsertDirect.Dit, bindCmd.Args); err != nil {
						return err
					}
				}
			}

			numCols := len(ps.Columns)
			columnFormatCodes := bindCmd.OutFormats
			if len(bindCmd.OutFormats) == 1 && numCols > 1 {
				// Apply the format code to every column.
				columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
				for i := 0; i < numCols; i++ {
					columnFormatCodes[i] = bindCmd.OutFormats[0]
				}
			}

			stmtRes := ex.clientComm.CreateStatementResult(
				ps.AST,
				// The client is using the extended protocol, so no row description is
				// needed.
				DontNeedRowDesc,
				0, columnFormatCodes,
				ex.sessionData.DataConversion,
				0,
				portalName,
				ex.implicitTxn(),
			)

			ps.PrepareInsertDirect.payloadNodeMap = di.PayloadNodeMap
			ps.PrepareInsertDirect.stmtRes = stmtRes
			ps.PrepareInsertDirect.EvalContext = EvalContext
			ps.PrepareInsertDirect.EvalContext.Txn = nil
			return nil
		})
		if err != nil {
			return retErr(err)
		}
	}

	var qargs tree.QueryArguments
	columnFormatCodes := bindCmd.OutFormats
	// Create the new PreparedPortal.
	if err := ex.addPortal(ctx, portalName, ps, qargs, columnFormatCodes); err != nil {
		return retErr(err)
	}

	if log.V(2) {
		log.Infof(ctx, "portal: %q for %q, args %q, formats %q",
			portalName, ps.Statement, qargs, columnFormatCodes)
	}

	return nil, nil
}

// addPortal creates a new PreparedPortal on the connExecutor.
//
// It is illegal to call this when a portal with that name already exists (even
// for anonymous portals).
func (ex *connExecutor) addPortal(
	ctx context.Context,
	portalName string,
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) error {
	if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
		panic(fmt.Sprintf("portal already exists: %q", portalName))
	}

	portal, err := ex.makePreparedPortal(ctx, portalName, stmt, qargs, outFormats)
	if err != nil {
		return err
	}

	ex.extraTxnState.prepStmtsNamespace.portals[portalName] = portal
	return nil
}

// exhaustPortal marks a portal with the provided name as "exhausted" and
// panics if there is no portal with such name.
func (ex *connExecutor) exhaustPortal(portalName string) {
	portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]
	if !ok {
		panic(errors.AssertionFailedf("portal %s doesn't exist", portalName))
	}
	portal.exhausted = true
	ex.extraTxnState.prepStmtsNamespace.portals[portalName] = portal
}

func (ex *connExecutor) deletePreparedStmt(ctx context.Context, name string) {
	ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	if !ok {
		return
	}
	ps.decRef(ctx)
	delete(ex.extraTxnState.prepStmtsNamespace.prepStmts, name)
}

func (ex *connExecutor) deletePortal(ctx context.Context, name string) {
	portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[name]
	if !ok {
		return
	}
	portal.decRef(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
	delete(ex.extraTxnState.prepStmtsNamespace.portals, name)
}

func (ex *connExecutor) execDelPrepStmt(
	ctx context.Context, delCmd DeletePreparedStmt,
) (fsm.Event, fsm.EventPayload) {
	switch delCmd.Type {
	case pgwirebase.PrepareStatement:
		_, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[delCmd.Name]
		if !ok {
			// The spec says "It is not an error to issue Close against a nonexistent
			// statement or portal name". See
			// https://www.postgresql.org/docs/current/static/protocol-flow.html.
			break
		}

		ex.deletePreparedStmt(ctx, delCmd.Name)
	case pgwirebase.PreparePortal:
		_, ok := ex.extraTxnState.prepStmtsNamespace.portals[delCmd.Name]
		if !ok {
			break
		}
		ex.deletePortal(ctx, delCmd.Name)
	default:
		panic(fmt.Sprintf("unknown del type: %s", delCmd.Type))
	}
	return nil, nil
}

func (ex *connExecutor) execDescribe(
	ctx context.Context, descCmd DescribeStmt, res DescribeResult,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	switch descCmd.Type {
	case pgwirebase.PrepareStatement:
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[descCmd.Name]
		if !ok {
			return retErr(pgerror.Newf(
				pgcode.InvalidSQLStatementName,
				"unknown prepared statement %q", descCmd.Name))
		}

		res.SetInferredTypes(ps.InferredTypes)

		if stmtHasNoData(ps.AST) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPrepStmtOutput(ctx, ps.Columns)
		}
	case pgwirebase.PreparePortal:
		portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[descCmd.Name]
		if !ok {
			return retErr(pgerror.Newf(
				pgcode.InvalidCursorName, "unknown portal %q", descCmd.Name))
		}

		if stmtHasNoData(portal.Stmt.AST) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPortalOutput(ctx, portal.Stmt.Columns, portal.OutFormats)
		}
	default:
		return retErr(errors.AssertionFailedf(
			"unknown describe type: %s", errors.Safe(descCmd.Type)))
	}
	return nil, nil
}
