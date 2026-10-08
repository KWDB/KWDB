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

package batcheval

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/abortspan"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestDeclareKeysResolveIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const id = "f90b99de-6bd2-48a3-873c-12fdb9867a3c"
	txnMeta := enginepb.TxnMeta{}
	{
		var err error
		txnMeta.ID, err = uuid.FromString(id)
		if err != nil {
			t.Fatal(err)
		}
	}
	abortSpanKey := fmt.Sprintf(`write local: /Local/RangeID/99/r/AbortSpan/"%s"`, id)
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("a"),
	}
	tests := []struct {
		status      roachpb.TransactionStatus
		poison      bool
		expDeclares bool
	}{
		{
			status:      roachpb.ABORTED,
			poison:      true,
			expDeclares: true,
		},
		{
			status:      roachpb.ABORTED,
			poison:      false,
			expDeclares: true,
		},
		{
			status:      roachpb.COMMITTED,
			poison:      true,
			expDeclares: false,
		},
		{
			status:      roachpb.COMMITTED,
			poison:      false,
			expDeclares: false,
		},
	}
	ctx := context.Background()
	engine := relational.NewDefaultInMem()
	defer engine.Close()
	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		for _, test := range tests {
			t.Run("", func(t *testing.T) {
				ri := roachpb.ResolveIntentRequest{
					IntentTxn: txnMeta,
					Status:    test.status,
					Poison:    test.poison,
				}
				ri.Key = roachpb.Key("b")
				rir := roachpb.ResolveIntentRangeRequest{
					IntentTxn: ri.IntentTxn,
					Status:    ri.Status,
					Poison:    ri.Poison,
				}
				rir.Key = ri.Key
				rir.EndKey = roachpb.Key("c")

				as := abortspan.New(desc.RangeID)

				var latchSpans, lockSpans spanset.SpanSet
				batch := engine.NewBatch()
				batch = spanset.NewBatch(batch, &latchSpans)
				defer batch.Close()

				var h roachpb.Header
				h.RangeID = desc.RangeID

				cArgs := CommandArgs{Header: h}
				cArgs.EvalCtx = (&MockEvalCtx{AbortSpan: as}).EvalContext()

				if !ranged {
					cArgs.Args = &ri
					declareKeysResolveIntent(&desc, h, &ri, &latchSpans, &lockSpans)
					if _, err := ResolveIntent(ctx, batch, cArgs, &roachpb.ResolveIntentResponse{}); err != nil {
						t.Fatal(err)
					}
				} else {
					cArgs.Args = &rir
					declareKeysResolveIntentRange(&desc, h, &rir, &latchSpans, &lockSpans)
					if _, err := ResolveIntentRange(ctx, batch, cArgs, &roachpb.ResolveIntentRangeResponse{}); err != nil {
						t.Fatal(err)
					}
				}

				if s := latchSpans.String(); strings.Contains(s, abortSpanKey) != test.expDeclares {
					t.Errorf("expected AbortSpan declared: %t, but got spans\n%s", test.expDeclares, s)
				}
				if !lockSpans.Empty() {
					t.Errorf("expected no lock spans declared, but got spans\n%s", lockSpans.String())
				}
			})
		}
	})
}

// TestResolveIntentAfterPartialRollback checks that the ResolveIntent
// and ResolveIntentRange properly propagate their IgnoredSeqNums
// parameter to the MVCC layer and only commit writes at non-ignored
// seqnums.
func TestResolveIntentAfterPartialRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	endKey := roachpb.Key("z")
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(k),
		EndKey:   roachpb.RKey(endKey),
	}

	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		db := relational.NewDefaultInMem()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		var v roachpb.Value
		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 0
		if err := relational.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 1
		if err := relational.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true, relational.NormalCommitType); err != nil {
			t.Fatal(err)
		}

		// Partially revert the 2nd store above.
		ignoredSeqNums := []enginepb.IgnoredSeqNumRange{{Start: 1, End: 1}}

		h := roachpb.Header{
			RangeID:   desc.RangeID,
			Timestamp: ts,
		}

		var spans spanset.SpanSet
		rbatch := db.NewBatch()
		rbatch = spanset.NewBatch(rbatch, &spans)
		defer rbatch.Close()

		if !ranged {
			// Resolve a point intent.
			ri := roachpb.ResolveIntentRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			ri.Key = k

			declareKeysResolveIntent(&desc, h, &ri, &spans, nil)

			if _, err := ResolveIntent(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{}).EvalContext(),
					Args:    &ri,
				},
				&roachpb.ResolveIntentResponse{},
			); err != nil {
				t.Fatal(err)
			}
		} else {
			// Resolve an intent range.
			rir := roachpb.ResolveIntentRangeRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			rir.Key = k
			rir.EndKey = endKey

			declareKeysResolveIntentRange(&desc, h, &rir, &spans, nil)

			h.MaxSpanRequestKeys = 10
			if _, err := ResolveIntentRange(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{}).EvalContext(),
					Args:    &rir,
				},
				&roachpb.ResolveIntentRangeResponse{},
			); err != nil {
				t.Fatal(err)
			}
		}

		if err := rbatch.Commit(true, relational.NormalCommitType); err != nil {
			t.Fatal(err)
		}

		batch = db.NewBatch()
		defer batch.Close()

		// The second write has been rolled back; verify that the remaining
		// value is from the first write.
		res, i, err := relational.MVCCGet(ctx, batch, k, ts2, relational.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if i != nil {
			t.Errorf("%s: found intent, expected none: %+v", k, i)
		}
		if res == nil {
			t.Errorf("%s: no value found, expected one", k)
		} else {
			s, err := res.GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, "a", string(s), "at key %s", k)
		}
	})
}
