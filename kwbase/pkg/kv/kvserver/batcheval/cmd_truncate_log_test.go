// Copyright 2019 The Cockroach Authors.
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

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func putTruncatedState(
	t *testing.T,
	eng relational.Engine,
	rangeID roachpb.RangeID,
	truncState roachpb.RaftTruncatedState,
	legacy bool,
) {
	key := keys.RaftTruncatedStateKey(rangeID)
	if legacy {
		key = keys.RaftTruncatedStateLegacyKey(rangeID)
	}
	if err := relational.MVCCPutProto(
		context.Background(), eng, nil, key,
		hlc.Timestamp{}, nil /* txn */, &truncState,
	); err != nil {
		t.Fatal(err)
	}
}

func readTruncStates(
	t *testing.T, eng relational.Engine, rangeID roachpb.RangeID,
) (legacy roachpb.RaftTruncatedState, unreplicated roachpb.RaftTruncatedState) {
	t.Helper()
	legacyFound, err := relational.MVCCGetProto(
		context.Background(), eng, keys.RaftTruncatedStateLegacyKey(rangeID),
		hlc.Timestamp{}, &legacy, relational.MVCCGetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if legacyFound != (legacy != roachpb.RaftTruncatedState{}) {
		t.Fatalf("legacy key found=%t but state is %+v", legacyFound, legacy)
	}

	unreplicatedFound, err := relational.MVCCGetProto(
		context.Background(), eng, keys.RaftTruncatedStateKey(rangeID),
		hlc.Timestamp{}, &unreplicated, relational.MVCCGetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if unreplicatedFound != (unreplicated != roachpb.RaftTruncatedState{}) {
		t.Fatalf("unreplicated key found=%t but state is %+v", unreplicatedFound, unreplicated)
	}
	return
}

const (
	expectationNeither = iota
	expectationLegacy
	expectationUnreplicated
)

type unreplicatedTruncStateTest struct {
	startsWithLegacy bool
	exp              int // see consts above
}

func TestTruncateLogUnreplicatedTruncatedState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Check out the old clusterversion.VersionUnreplicatedRaftTruncatedState
	// for information on what's being tested. The cluster version is gone, but
	// the migration is done range by range and so it still exists.

	const (
		startsLegacy       = true
		startsUnreplicated = false
	)

	testCases := []unreplicatedTruncStateTest{
		// This is the case where we've already migrated.
		{startsUnreplicated, expectationUnreplicated},
		// This is the case in which the migration is triggered. As a result,
		// we see neither of the keys written. The new key will be written
		// atomically as a side effect (outside of the scope of this test).
		{startsLegacy, expectationNeither},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			runUnreplicatedTruncatedState(t, tc)
		})
	}
}

func runUnreplicatedTruncatedState(t *testing.T, tc unreplicatedTruncStateTest) {
	ctx := context.Background()

	const (
		rangeID    = 12
		term       = 10
		firstIndex = 100
	)

	evalCtx := &MockEvalCtx{
		Desc:       &roachpb.RangeDescriptor{RangeID: rangeID},
		Term:       term,
		FirstIndex: firstIndex,
	}

	eng := relational.NewDefaultInMem()
	defer eng.Close()

	truncState := roachpb.RaftTruncatedState{
		Index: firstIndex + 1,
		Term:  term,
	}

	// Put down the TruncatedState specified by the test case.
	putTruncatedState(t, eng, rangeID, truncState, tc.startsWithLegacy)

	// Send a truncation request.
	req := roachpb.TruncateLogRequest{
		RangeID: rangeID,
		Index:   firstIndex + 7,
	}
	cArgs := CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Args:    &req,
	}
	resp := &roachpb.TruncateLogResponse{}
	res, err := TruncateLog(ctx, eng, cArgs, resp)
	if err != nil {
		t.Fatal(err)
	}

	expTruncState := roachpb.RaftTruncatedState{
		Index: req.Index - 1,
		Term:  term,
	}

	legacy, unreplicated := readTruncStates(t, eng, rangeID)

	switch tc.exp {
	case expectationLegacy:
		assert.Equal(t, expTruncState, legacy)
		assert.Zero(t, unreplicated)
	case expectationUnreplicated:
		// The unreplicated key that we see should be the initial truncated
		// state (it's only updated below Raft).
		assert.Equal(t, truncState, unreplicated)
		assert.Zero(t, legacy)
	case expectationNeither:
		assert.Zero(t, unreplicated)
		assert.Zero(t, legacy)
	default:
		t.Fatalf("unknown expectation %d", tc.exp)
	}

	assert.NotNil(t, res.Replicated.State)
	assert.NotNil(t, res.Replicated.State.TruncatedState)
	assert.Equal(t, expTruncState, *res.Replicated.State.TruncatedState)
}
