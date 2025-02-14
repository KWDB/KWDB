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
//
//
//
// This file contains the definition of a connection's state machine, expressed
// through the util/fsm library. Also see txn_state.go, which contains the
// txnState structure used as the ExtendedState mutated by all the Actions.

package sql

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/fsm"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// Constants for the String() representation of the session states. Shared with
// the CLI code which needs to recognize them.
const (
	NoTxnStateStr         = "NoTxn"
	OpenStateStr          = "Open"
	AbortedStateStr       = "Aborted"
	CommitWaitStateStr    = "CommitWait"
	InternalErrorStateStr = "InternalError"
)

/// States.

type stateNoTxn struct{}

var _ fsm.State = &stateNoTxn{}

// GetImplicitTxn get ImplicitTxn
func (stateNoTxn) GetImplicitTxn() bool {
	return false
}

func (stateNoTxn) String() string {
	return NoTxnStateStr
}

type stateOpen struct {
	ImplicitTxn fsm.Bool
}

var _ fsm.State = &stateOpen{}

// GetImplicitTxn get ImplicitTxn
func (o stateOpen) GetImplicitTxn() bool {
	return o.ImplicitTxn.Get()
}

func (stateOpen) String() string {
	return OpenStateStr
}

// stateAborted is entered on errors (retriable and non-retriable). A ROLLBACK
// TO SAVEPOINT can move the transaction back to stateOpen.
type stateAborted struct{}

var _ fsm.State = &stateAborted{}

// GetImplicitTxn get ImplicitTxn
func (stateAborted) GetImplicitTxn() bool {
	return false
}

func (stateAborted) String() string {
	return AbortedStateStr
}

type stateCommitWait struct{}

var _ fsm.State = &stateCommitWait{}

// GetImplicitTxn get ImplicitTxn
func (stateCommitWait) GetImplicitTxn() bool {
	return false
}

func (stateCommitWait) String() string {
	return CommitWaitStateStr
}

// stateInternalError is used by the InternalExecutor when running statements in
// a higher-level transaction. The fsm is in this state after encountering an
// execution error when running in an external transaction: the "SQL
// transaction" is finished, however the higher-level transaction is not rolled
// back.
type stateInternalError struct{}

var _ fsm.State = &stateInternalError{}

// GetImplicitTxn get ImplicitTxn
func (stateInternalError) GetImplicitTxn() bool {
	return false
}

func (stateInternalError) String() string {
	return InternalErrorStateStr
}

func (stateNoTxn) State()         {}
func (stateOpen) State()          {}
func (stateAborted) State()       {}
func (stateCommitWait) State()    {}
func (stateInternalError) State() {}

/// Events.

type eventTxnStart struct {
	ImplicitTxn fsm.Bool
}
type eventTxnStartPayload struct {
	tranCtx transitionCtx

	pri roachpb.UserPriority
	// txnSQLTimestamp is the timestamp that statements executed in the
	// transaction that is started by this event will report for now(),
	// current_timestamp(), transaction_timestamp().
	txnSQLTimestamp     time.Time
	readOnly            tree.ReadWriteMode
	historicalTimestamp *hlc.Timestamp
	isoLevel            tree.IsolationLevel
}

// makeEventTxnStartPayload creates an eventTxnStartPayload.
func makeEventTxnStartPayload(
	pri roachpb.UserPriority,
	readOnly tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	tranCtx transitionCtx,
	isoLevel tree.IsolationLevel,
) eventTxnStartPayload {
	return eventTxnStartPayload{
		pri:                 pri,
		readOnly:            readOnly,
		txnSQLTimestamp:     txnSQLTimestamp,
		historicalTimestamp: historicalTimestamp,
		tranCtx:             tranCtx,
		isoLevel:            isoLevel,
	}
}

type eventTxnFinish struct{}

// eventTxnFinishPayload represents the payload for eventTxnFinish.
type eventTxnFinishPayload struct {
	// commit is set if the transaction committed, false if it was aborted.
	commit bool
}

// eventSavepointRollback is generated when we want to move from Aborted to Open
// through a ROLLBACK TO SAVEPOINT <not kwbase_restart>. Note that it is not
// generated when such a savepoint is rolled back to from the Open state. In
// that case no event is necessary.
type eventSavepointRollback struct{}

type eventNonRetriableErr struct {
	IsCommit fsm.Bool
}

// eventNonRetriableErrPayload represents the payload for eventNonRetriableErr.
type eventNonRetriableErrPayload struct {
	// err is the error that caused the event.
	err error
}

// errorCause implements the payloadWithError interface.
func (p eventNonRetriableErrPayload) errorCause() error {
	return p.err
}

// eventNonRetriableErrorPayload implements payloadWithError.
var _ payloadWithError = eventNonRetriableErrPayload{}

type eventRetriableErr struct {
	CanAutoRetry fsm.Bool
	IsCommit     fsm.Bool
}

// eventRetriableErrPayload represents the payload for eventRetriableErr.
type eventRetriableErrPayload struct {
	// err is the error that caused the event
	err error
	// rewCap must be set if CanAutoRetry is set on the event. It will be passed
	// back to the connExecutor to perform the rewind.
	rewCap rewindCapability
}

// errorCause implements the payloadWithError interface.
func (p eventRetriableErrPayload) errorCause() error {
	return p.err
}

// eventRetriableErrPayload implements payloadWithError.
var _ payloadWithError = eventRetriableErrPayload{}

// eventTxnRestart is generated by a rollback to a savepoint placed at the
// beginning of the transaction (commonly SAVEPOINT kwbase_restart).
type eventTxnRestart struct{}

// eventTxnReleased is generated after a successful RELEASE SAVEPOINT
// kwbase_restart. It moves the state to CommitWait. The event is not
// generated by releasing regular savepoints.
type eventTxnReleased struct{}

// payloadWithError is a common interface for the payloads that wrap an error.
type payloadWithError interface {
	errorCause() error
}

func (eventTxnStart) Event()          {}
func (eventTxnFinish) Event()         {}
func (eventSavepointRollback) Event() {}
func (eventNonRetriableErr) Event()   {}
func (eventRetriableErr) Event()      {}
func (eventTxnRestart) Event()        {}
func (eventTxnReleased) Event()       {}

// TxnStateTransitions describe the transitions used by a connExecutor's
// fsm.Machine. Args.Extended is a txnState, which is muted by the Actions.
//
// This state machine accepts the eventNonRetriableErr{IsCommit: fsm.True} in all
// states. This contract is in place to support the cleanup of connExecutor ->
// this event can always be sent when the connExecutor is tearing down.
//
// NOTE: The Args.Ctx passed to the actions is the connExecutor's context. While
// we are inside a SQL txn, the txn's ctx should be used for operations (i.e
// txnState.Ctx, which is a child ctx). This is so because transitions that move
// in and out of transactions need to have access to both contexts.
//
//go:generate ../util/fsm/gen/reports.sh TxnStateTransitions stateNoTxn
var TxnStateTransitions = fsm.Compile(fsm.Pattern{
	// NoTxn
	//
	// Note that we don't handle any errors in this state. The connExecutor is
	// supposed to send an eventTxnStart before any other statement that may
	// generate an error.
	stateNoTxn{}: {
		eventTxnStart{fsm.Var("implicitTxn")}: {
			Description: "BEGIN, or before a statement running as an implicit txn",
			Next:        stateOpen{ImplicitTxn: fsm.Var("implicitTxn")},
			Action:      noTxnToOpen,
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it produces a skipBatch advance
			// code.
			Description: "anything but BEGIN or extended protocol command error",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},

	/// Open
	stateOpen{ImplicitTxn: fsm.Any}: {
		eventTxnFinish{}: {
			Description: "COMMIT/ROLLBACK, or after a statement running as an implicit txn",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				// Note that the KV txn has been committed or rolled back by the
				// statement execution by this point.
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		// Handle the error on COMMIT cases: we move to NoTxn as per Postgres error
		// semantics.
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.True}: {
			Description: "Retriable err on COMMIT",
			Next:        stateNoTxn{},
			Action:      cleanupAndFinishOnError,
		},
		eventNonRetriableErr{IsCommit: fsm.True}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
	},
	stateOpen{ImplicitTxn: fsm.Var("implicitTxn")}: {
		// This is the case where we auto-retry.
		eventRetriableErr{CanAutoRetry: fsm.True, IsCommit: fsm.Any}: {
			// We leave the transaction in Open. In particular, we don't move to
			// RestartWait, as there'd be nothing to move us back from RestartWait to
			// Open.
			// Note: Preparing the KV txn for restart has already happened by this
			// point.
			Description: "Retriable err; will auto-retry",
			Next:        stateOpen{ImplicitTxn: fsm.Var("implicitTxn")},
			Action: func(args fsm.Args) error {
				// The caller will call rewCap.rewindAndUnlock().
				args.Extended.(*txnState).setAdvanceInfo(
					rewind,
					args.Payload.(eventRetriableErrPayload).rewCap,
					txnRestart)
				return nil
			},
		},
	},
	// Handle the errors in implicit txns. They move us to NoTxn.
	stateOpen{ImplicitTxn: fsm.True}: {
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next:   stateNoTxn{},
			Action: cleanupAndFinishOnError,
		},
	},
	// Handle the errors in explicit txns. They move us to Aborted.
	stateOpen{ImplicitTxn: fsm.False}: {
		eventNonRetriableErr{IsCommit: fsm.False}: {
			Next: stateAborted{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT kwbase. There's not much to do other than generating a
		// txnRestart output event.
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT kwbase_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				err := func() error {
					ts.mu.Lock()
					defer ts.mu.Unlock()
					if err := ts.mu.txn.PrepareForRetry(ts.Ctx, nil); err != nil {
						return err
					}
					return nil
				}()
				if err != nil {
					return err
				}
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
		eventRetriableErr{CanAutoRetry: fsm.False, IsCommit: fsm.False}: {
			Next: stateAborted{},
			Action: func(args fsm.Args) error {
				// Note: Preparing the KV txn for restart has already happened by this
				// point.
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
		eventTxnReleased{}: {
			Description: "RELEASE SAVEPOINT kwbase_restart",
			Next:        stateCommitWait{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnCommit)
				return nil
			},
		},
	},

	/// Aborted
	//
	// Note that we don't handle any error events here. Any statement but a
	// ROLLBACK (TO SAVEPOINT) is expected to not be passed to the state machine.
	stateAborted{}: {
		eventTxnFinish{}: {
			Description: "ROLLBACK",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.txnAbortCount.Inc(1)
				// Note that the KV txn has been rolled back by now by statement
				// execution.
				return ts.finishTxn(args.Payload.(eventTxnFinishPayload))
			},
		},
		// Any statement.
		eventNonRetriableErr{IsCommit: fsm.False}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "any other statement",
			Next:        stateAborted{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
		// ConnExecutor closing.
		eventNonRetriableErr{IsCommit: fsm.True}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "ConnExecutor closing",
			Next:        stateAborted{},
			Action:      cleanupAndFinishOnError,
		},
		// ROLLBACK TO SAVEPOINT <not kwbase_restart> success.
		eventSavepointRollback{}: {
			Description: "ROLLBACK TO SAVEPOINT (not kwbase_restart) success",
			Next:        stateOpen{ImplicitTxn: fsm.False},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, noEvent)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT <not kwbase_restart> failed because the txn needs to restart.
		eventRetriableErr{CanAutoRetry: fsm.Any, IsCommit: fsm.Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			Description: "ROLLBACK TO SAVEPOINT (not kwbase_restart) failed because txn needs restart",
			Next:        stateAborted{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
		// ROLLBACK TO SAVEPOINT kwbase_restart.
		eventTxnRestart{}: {
			Description: "ROLLBACK TO SAVEPOINT kwbase_restart",
			Next:        stateOpen{ImplicitTxn: fsm.False},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(advanceOne, noRewind, txnRestart)
				return nil
			},
		},
	},

	stateCommitWait{}: {
		eventTxnFinish{}: {
			Description: "COMMIT",
			Next:        stateNoTxn{},
			Action: func(args fsm.Args) error {
				return args.Extended.(*txnState).finishTxn(
					args.Payload.(eventTxnFinishPayload),
				)
			},
		},
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			// This event doesn't change state, but it returns a skipBatch code.
			//
			// Note that we don't expect any errors from error on COMMIT in this
			// state.
			Description: "any other statement",
			Next:        stateCommitWait{},
			Action: func(args fsm.Args) error {
				args.Extended.(*txnState).setAdvanceInfo(skipBatch, noRewind, noEvent)
				return nil
			},
		},
	},
})

// noTxnToOpen implements the side effects of starting a txn. It also calls
// setAdvanceInfo().
func noTxnToOpen(args fsm.Args) error {
	connCtx := args.Ctx
	ev := args.Event.(eventTxnStart)
	payload := args.Payload.(eventTxnStartPayload)
	ts := args.Extended.(*txnState)

	txnTyp := explicitTxn
	advCode := advanceOne
	if ev.ImplicitTxn.Get() {
		txnTyp = implicitTxn
		// For an implicit txn, we want the statement that produced the event to be
		// executed again (this time in state Open).
		advCode = stayInPlace
	}

	ts.resetForNewSQLTxn(
		connCtx,
		txnTyp,
		payload.txnSQLTimestamp,
		payload.historicalTimestamp,
		payload.pri,
		payload.readOnly,
		nil, /* txn */
		payload.tranCtx,
		payload.isoLevel,
	)
	ts.setAdvanceInfo(advCode, noRewind, txnStart)
	return nil
}

// finishTxn finishes the transaction. It also calls setAdvanceInfo().
func (ts *txnState) finishTxn(payload eventTxnFinishPayload) error {
	ts.finishSQLTxn()

	var ev txnEvent
	if payload.commit {
		ev = txnCommit
	} else {
		ev = txnRollback
	}
	ts.setAdvanceInfo(advanceOne, noRewind, ev)
	return nil
}

// cleanupAndFinishOnError rolls back the KV txn and finishes the SQL txn.
func cleanupAndFinishOnError(args fsm.Args) error {
	ts := args.Extended.(*txnState)
	ts.mu.txn.CleanupOnError(ts.Ctx, args.Payload.(payloadWithError).errorCause())
	ts.finishSQLTxn()
	ts.setAdvanceInfo(skipBatch, noRewind, txnRollback)
	return nil
}

// BoundTxnStateTransitions is the state machine used by the InternalExecutor
// when running SQL inside a higher-level txn. It's a very limited state
// machine: it doesn't allow starting or finishing txns, auto-retries, etc.
var BoundTxnStateTransitions = fsm.Compile(fsm.Pattern{
	stateOpen{ImplicitTxn: fsm.False}: {
		// We accept eventNonRetriableErr with both IsCommit={True, fsm.False}, even
		// though this state machine does not support COMMIT statements because
		// connExecutor.close() sends an eventNonRetriableErr{IsCommit: fsm.True} event.
		eventNonRetriableErr{IsCommit: fsm.Any}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.finishSQLTxn()
				ts.setAdvanceInfo(skipBatch, noRewind, txnRollback)
				return nil
			},
		},
		eventRetriableErr{CanAutoRetry: fsm.Any, IsCommit: fsm.False}: {
			Next: stateInternalError{},
			Action: func(args fsm.Args) error {
				ts := args.Extended.(*txnState)
				ts.finishSQLTxn()
				ts.setAdvanceInfo(skipBatch, noRewind, txnRollback)
				return nil
			},
		},
	},
})
