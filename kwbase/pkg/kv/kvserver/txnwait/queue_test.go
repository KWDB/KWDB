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

package txnwait

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	min := enginepb.MinTxnPriority
	max := enginepb.MaxTxnPriority
	mid := enginepb.TxnPriority(1)
	testCases := []struct {
		force      bool
		typ        roachpb.PushTxnType
		pusherPri  enginepb.TxnPriority
		pusheePri  enginepb.TxnPriority
		shouldPush bool
	}{
		{false, roachpb.PUSH_ABORT, min, min, false},
		{false, roachpb.PUSH_ABORT, min, mid, false},
		{false, roachpb.PUSH_ABORT, min, max, false},
		{false, roachpb.PUSH_ABORT, mid, min, true},
		{false, roachpb.PUSH_ABORT, mid, mid, false},
		{false, roachpb.PUSH_ABORT, mid, max, false},
		{false, roachpb.PUSH_ABORT, max, min, true},
		{false, roachpb.PUSH_ABORT, max, mid, true},
		{false, roachpb.PUSH_ABORT, max, max, false},
		{false, roachpb.PUSH_TIMESTAMP, min, min, false},
		{false, roachpb.PUSH_TIMESTAMP, min, mid, false},
		{false, roachpb.PUSH_TIMESTAMP, min, max, false},
		{false, roachpb.PUSH_TIMESTAMP, mid, min, true},
		{false, roachpb.PUSH_TIMESTAMP, mid, mid, false},
		{false, roachpb.PUSH_TIMESTAMP, mid, max, false},
		{false, roachpb.PUSH_TIMESTAMP, max, min, true},
		{false, roachpb.PUSH_TIMESTAMP, max, mid, true},
		{false, roachpb.PUSH_TIMESTAMP, max, max, false},
		{false, roachpb.PUSH_TOUCH, min, min, true},
		{false, roachpb.PUSH_TOUCH, min, mid, true},
		{false, roachpb.PUSH_TOUCH, min, max, true},
		{false, roachpb.PUSH_TOUCH, mid, min, true},
		{false, roachpb.PUSH_TOUCH, mid, mid, true},
		{false, roachpb.PUSH_TOUCH, mid, max, true},
		{false, roachpb.PUSH_TOUCH, max, min, true},
		{false, roachpb.PUSH_TOUCH, max, mid, true},
		{false, roachpb.PUSH_TOUCH, max, max, true},
		// Force pushes always push immediately.
		{true, roachpb.PUSH_ABORT, min, min, true},
		{true, roachpb.PUSH_ABORT, min, mid, true},
		{true, roachpb.PUSH_ABORT, min, max, true},
		{true, roachpb.PUSH_ABORT, mid, min, true},
		{true, roachpb.PUSH_ABORT, mid, mid, true},
		{true, roachpb.PUSH_ABORT, mid, max, true},
		{true, roachpb.PUSH_ABORT, max, min, true},
		{true, roachpb.PUSH_ABORT, max, mid, true},
		{true, roachpb.PUSH_ABORT, max, max, true},
		{true, roachpb.PUSH_TIMESTAMP, min, min, true},
		{true, roachpb.PUSH_TIMESTAMP, min, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, min, max, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, min, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, mid, max, true},
		{true, roachpb.PUSH_TIMESTAMP, max, min, true},
		{true, roachpb.PUSH_TIMESTAMP, max, mid, true},
		{true, roachpb.PUSH_TIMESTAMP, max, max, true},
		{true, roachpb.PUSH_TOUCH, min, min, true},
		{true, roachpb.PUSH_TOUCH, min, mid, true},
		{true, roachpb.PUSH_TOUCH, min, max, true},
		{true, roachpb.PUSH_TOUCH, mid, min, true},
		{true, roachpb.PUSH_TOUCH, mid, mid, true},
		{true, roachpb.PUSH_TOUCH, mid, max, true},
		{true, roachpb.PUSH_TOUCH, max, min, true},
		{true, roachpb.PUSH_TOUCH, max, mid, true},
		{true, roachpb.PUSH_TOUCH, max, max, true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				Force:    test.force,
				PushType: test.typ,
				PusherTxn: roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						Priority: test.pusherPri,
					},
				},
				PusheeTxn: enginepb.TxnMeta{
					Priority: test.pusheePri,
				},
			}
			if shouldPush := ShouldPushImmediately(&req); shouldPush != test.shouldPush {
				t.Errorf("expected %t; got %t", test.shouldPush, shouldPush)
			}
		})
	}
}

func makeTS(w int64, l int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: w, Logical: l}
}

func TestIsPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ          roachpb.PushTxnType
		pushTo       hlc.Timestamp
		txnStatus    roachpb.TransactionStatus
		txnTimestamp hlc.Timestamp
		isPushed     bool
	}{
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.STAGING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 1), true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.STAGING, makeTS(10, 2), true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				PushType: test.typ,
				PushTo:   test.pushTo,
			}
			txn := roachpb.Transaction{
				Status: test.txnStatus,
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: test.txnTimestamp,
				},
			}
			if isPushed := isPushed(&req, &txn); isPushed != test.isPushed {
				t.Errorf("expected %t; got %t", test.isPushed, isPushed)
			}
		})
	}
}

func makeConfig(s kv.SenderFunc) Config {
	var cfg Config
	cfg.RangeDesc = &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin, EndKey: roachpb.RKeyMax,
	}
	manual := hlc.NewManualClock(123)
	cfg.Clock = hlc.NewClock(manual.UnixNano, time.Nanosecond)
	cfg.Stopper = stop.NewStopper()
	cfg.Metrics = NewMetrics(time.Minute)
	if s != nil {
		factory := kv.NonTransactionalFactoryFunc(s)
		cfg.DB = kv.NewDB(testutils.MakeAmbientCtx(), factory, cfg.Clock)
	}
	return cfg
}

// TestMaybeWaitForQueryWithContextCancellation adds a new waiting query to the
// queue and cancels its context. It then verifies that the query was cleaned
// up. Regression test against #28849, before which the waiting query would
// leak.
func TestMaybeWaitForQueryWithContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := makeConfig(nil)
	defer cfg.Stopper.Stop(context.Background())
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	ctx, cancel := context.WithCancel(context.Background())
	waitingRes := make(chan *roachpb.Error)
	go func() {
		req := &roachpb.QueryTxnRequest{WaitForUpdate: true}
		waitingRes <- q.MaybeWaitForQuery(ctx, req)
	}()

	cancel()
	if pErr := <-waitingRes; !testutils.IsPError(pErr, "context canceled") {
		t.Errorf("unexpected error %v", pErr)
	}
	if len(q.mu.queries) != 0 {
		t.Errorf("expected no waiting queries, found %v", q.mu.queries)
	}

	metrics := cfg.Metrics
	allMetricsAreZero := metrics.PusheeWaiting.Value() == 0 &&
		metrics.PusherWaiting.Value() == 0 &&
		metrics.QueryWaiting.Value() == 0 &&
		metrics.PusherSlow.Value() == 0

	if !allMetricsAreZero {
		t.Errorf("expected all metric gauges to be zero, got some that aren't")
	}
}

// TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn tests that if any
// QueryTxn on a pushee txn returns an aborted transaction status, all
// pushees of that transaction are informed of the aborted status and
// released.
func TestPushersReleasedAfterAnyQueryTxnFindsAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var mockSender kv.SenderFunc
	cfg := makeConfig(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return mockSender(ctx, ba)
	})
	defer cfg.Stopper.Stop(context.Background())
	q := NewQueue(cfg)
	q.Enable(1 /* leaseSeq */)

	// Set an extremely high transaction liveness threshold so that the pushee
	// is only queried once per pusher.
	defer TestingOverrideTxnLivenessThreshold(time.Hour)()

	// Enqueue pushee transaction in the queue.
	txn := roachpb.MakeTransaction("test", nil, 0, cfg.Clock.Now(), 0)
	q.EnqueueTxn(&txn)

	const numPushees = 3
	var queryTxnCount int32
	mockSender = func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		resp := br.Responses[0].GetInner().(*roachpb.QueryTxnResponse)
		resp.QueriedTxn = txn
		if atomic.AddInt32(&queryTxnCount, 1) == numPushees {
			// Only the last pusher's query observes an ABORTED transaction. As
			// mentioned in the corresponding comment in MaybeWaitForPush, this
			// isn't expected without an associated update to the pushee's
			// transaction record. However, it is possible if the pushee hasn't
			// written a transaction record yet and the timestamp cache loses
			// resolution due to memory pressure. While rare, we need to handle
			// this case correctly.
			resp.QueriedTxn.Status = roachpb.ABORTED
		}
		return br, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < numPushees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			req := roachpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, PushType: roachpb.PUSH_ABORT}
			res, err := q.MaybeWaitForPush(ctx, &req)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, roachpb.ABORTED, res.PusheeTxn.Status)
		}()
	}
	wg.Wait()
}
