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

package stateloader

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"go.etcd.io/etcd/raft/raftpb"
)

func TestSynthesizeHardState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	eng := relational.NewDefaultInMem()
	stopper.AddCloser(eng)

	tHS := raftpb.HardState{Term: 2, Vote: 3, Commit: 4}

	testCases := []struct {
		TruncTerm, RaftAppliedIndex uint64
		OldHS                       *raftpb.HardState
		NewHS                       raftpb.HardState
		Err                         string
	}{
		{OldHS: nil, TruncTerm: 42, RaftAppliedIndex: 24, NewHS: raftpb.HardState{Term: 42, Vote: 0, Commit: 24}},
		// Can't wind back the committed index of the new HardState.
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit - 1, Err: "can't decrease HardState.Commit"},
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit, NewHS: tHS},
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit + 1, NewHS: raftpb.HardState{Term: tHS.Term, Vote: 3, Commit: tHS.Commit + 1}},
		// Higher Term is picked up, but vote isn't carried over when the term
		// changes.
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit, TruncTerm: 11, NewHS: raftpb.HardState{Term: 11, Vote: 0, Commit: tHS.Commit}},
	}

	for i, test := range testCases {
		func() {
			batch := eng.NewBatch()
			defer batch.Close()
			rsl := Make(roachpb.RangeID(1))

			if test.OldHS != nil {
				if err := rsl.SetHardState(context.Background(), batch, *test.OldHS); err != nil {
					t.Fatal(err)
				}
			}

			oldHS, err := rsl.LoadHardState(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}

			err = rsl.SynthesizeHardState(
				context.Background(), batch, oldHS, roachpb.RaftTruncatedState{Term: test.TruncTerm}, test.RaftAppliedIndex,
			)
			if !testutils.IsError(err, test.Err) {
				t.Fatalf("%d: expected %q got %v", i, test.Err, err)
			} else if err != nil {
				// No further checking if we expected an error and got it.
				return
			}

			hs, err := rsl.LoadHardState(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(hs, test.NewHS) {
				t.Fatalf("%d: expected %+v, got %+v", i, &test.NewHS, &hs)
			}
		}()
	}
}
