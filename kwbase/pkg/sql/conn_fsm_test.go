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

package sql

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/fsm"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestConnFSMStatesAndEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// States
	var s fsm.State
	s = stateNoTxn{}
	require.False(t, stateNoTxn{}.GetImplicitTxn())
	require.Equal(t, NoTxnStateStr, stateNoTxn{}.String())
	s.State()

	s = stateOpen{ImplicitTxn: fsm.True}
	require.True(t, stateOpen{ImplicitTxn: fsm.True}.GetImplicitTxn())
	require.False(t, stateOpen{ImplicitTxn: fsm.False}.GetImplicitTxn())
	require.Equal(t, OpenStateStr, stateOpen{}.String())
	s.State()

	s = stateAborted{}
	require.False(t, stateAborted{}.GetImplicitTxn())
	require.Equal(t, AbortedStateStr, stateAborted{}.String())
	s.State()

	s = stateCommitWait{}
	require.False(t, stateCommitWait{}.GetImplicitTxn())
	require.Equal(t, CommitWaitStateStr, stateCommitWait{}.String())
	s.State()

	s = stateInternalError{}
	require.False(t, stateInternalError{}.GetImplicitTxn())
	require.Equal(t, InternalErrorStateStr, stateInternalError{}.String())
	s.State()

	// Events
	var ev fsm.Event
	ev = eventTxnStart{}
	ev.Event()
	ev = eventTxnFinish{}
	ev.Event()
	ev = eventSavepointRollback{}
	ev.Event()
	ev = eventNonRetriableErr{}
	ev.Event()
	ev = eventRetriableErr{}
	ev.Event()
	ev = eventTxnRestart{}
	ev.Event()
	ev = eventTxnReleased{}
	ev.Event()

	// Payloads
	ts := timeutil.Now()
	ht := hlc.Timestamp{}
	payload := makeEventTxnStartPayload(roachpb.NormalUserPriority, tree.ReadWrite, ts, &ht, transitionCtx{}, tree.SerializableIsolation)
	require.Equal(t, roachpb.NormalUserPriority, payload.pri)
	require.Equal(t, tree.ReadWrite, payload.readOnly)
	require.Equal(t, ts, payload.txnSQLTimestamp)
	require.Equal(t, &ht, payload.historicalTimestamp)
	require.Equal(t, tree.SerializableIsolation, payload.isoLevel)

	err1 := fmt.Errorf("err1")
	err2 := fmt.Errorf("err2")
	require.Equal(t, err1, eventNonRetriableErrPayload{err: err1}.errorCause())
	require.Equal(t, err2, eventRetriableErrPayload{err: err2}.errorCause())
}
