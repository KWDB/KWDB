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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTxnStateFinishExternalTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)
	ts.finishExternalTxn()
	require.Nil(t, ts.sp)
	require.Nil(t, ts.Ctx)
	require.Nil(t, ts.mu.txn)
}

func TestTxnStateGetReadTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	require.NotNil(t, ts.mu.txn)
	readTS := ts.getReadTimestamp()
	require.NotZero(t, readTS)
}

func TestTxnStateSetReadOnlyMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		mode      tree.ReadWriteMode
		expected  bool
		withHist  bool
		expectErr bool
	}{
		{
			name:      "unspecified mode",
			mode:      tree.UnspecifiedReadWriteMode,
			expected:  false,
			expectErr: false,
		},
		{
			name:      "read only",
			mode:      tree.ReadOnly,
			expected:  true,
			expectErr: false,
		},
		{
			name:      "read write",
			mode:      tree.ReadWrite,
			expected:  false,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCon := makeTestContext()
			_, ts := testCon.createOpenState(explicitTxn)

			if tt.withHist {
				ts.isHistorical = true
			}

			err := ts.setReadOnlyMode(tt.mode)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, ts.readOnly)
			}
		})
	}
}

func TestTxnStateSetReadOnlyModeWithHistorical(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)
	ts.isHistorical = true

	err := ts.setReadOnlyMode(tree.ReadWrite)
	require.Error(t, err)
	require.Equal(t, tree.ErrAsOfSpecifiedWithReadWrite, err)
}

func TestTxnStateSetDebugName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		debugName tree.Name
		expectSet bool
	}{
		{
			name:      "empty name",
			debugName: "",
			expectSet: false,
		},
		{
			name:      "valid name",
			debugName: "test_procedure",
			expectSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCon := makeTestContext()
			_, ts := testCon.createOpenState(explicitTxn)

			err := ts.setDebugName(tt.debugName)
			require.NoError(t, err)

			if tt.expectSet {
				require.Equal(t, tt.debugName, ts.debugName)
			} else {
				require.Equal(t, tree.Name(""), ts.debugName)
			}
		})
	}
}

func TestTxnStateSetAdvanceInfoEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	ts.adv = advanceInfo{}
	ts.setAdvanceInfo(advanceOne, noRewind, noEvent)
	require.NotEqual(t, advanceUnknown, ts.adv.code)

	ts.consumeAdvanceInfo()
	require.Equal(t, advanceUnknown, ts.adv.code)
}

func TestTxnStateConsumeAdvanceInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	expectedAdv := advanceInfo{
		code:     advanceOne,
		txnEvent: txnCommit,
		rewCap:   noRewind,
	}
	ts.adv = expectedAdv

	adv := ts.consumeAdvanceInfo()
	require.Equal(t, expectedAdv, adv)
	require.Equal(t, advanceUnknown, ts.adv.code)
}

func TestTxnStateSetAdvanceInfoPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	ts.adv.code = advanceOne
	require.Panics(t, func() {
		ts.setAdvanceInfo(advanceOne, noRewind, noEvent)
	})
}

func TestTxnStateSetAdvanceInfoRewindPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	rewCap := rewindCapability{rewindPos: CmdPos(123)}
	require.Panics(t, func() {
		ts.setAdvanceInfo(advanceOne, rewCap, noEvent)
	})
}

func TestTxnStateSetReadOnlyModeInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	invalidMode := tree.ReadWriteMode(999)
	err := ts.setReadOnlyMode(invalidMode)
	require.Error(t, err)
	require.True(t, errors.IsAssertionFailure(err))
}

func TestTxnStateSetHistoricalTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	historicalTS := testCon.clock.Now()
	ts.setHistoricalTimestamp(ts.Ctx, historicalTS)
	require.True(t, ts.isHistorical)
}

func TestTxnStateSetIsolationLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCon := makeTestContext()
	_, ts := testCon.createOpenState(explicitTxn)

	levels := []enginepb.Level{
		enginepb.Serializable,
		enginepb.ReadCommitted,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			err := ts.setIsolationLevel(level)
			require.NoError(t, err)

			ts.mu.RLock()
			require.Equal(t, level, ts.mu.isolationLevel)
			ts.mu.RUnlock()
		})
	}
}
