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

package kvcoord_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// Test that a transaction gets cleaned up when the heartbeat loop finds out
// that it has already been aborted (by a 3rd party). That is, we don't wait for
// the client to find out before the intents are removed.
// This relies on the TxnCoordSender's heartbeat loop to notice the changed
// transaction status and do an async abort.
// After the heartbeat loop finds out about the abort, subsequent requests sent
// through the TxnCoordSender return TransactionAbortedErrors. On those errors,
// the contract is that the client.Txn creates a new transaction internally and
// switches the TxnCoordSender instance. The expectation is that the old
// transaction has been cleaned up by that point.
func TestHeartbeatFindsOutAboutAbortedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var cleanupSeen int64
	key := roachpb.Key("a")
	key2 := roachpb.Key("b")
	s, _, origDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingProposalFilter: func(args storagebase.ProposalFilterArgs) *roachpb.Error {
					// We'll eventually expect to see an EndTxn(commit=false)
					// with the right intents.
					if args.Req.IsSingleEndTxnRequest() {
						et := args.Req.Requests[0].GetInner().(*roachpb.EndTxnRequest)
						if !et.Commit && et.Key.Equal(key) &&
							reflect.DeepEqual(et.LockSpans, []roachpb.Span{{Key: key}, {Key: key2}}) {
							atomic.StoreInt64(&cleanupSeen, 1)
						}
					}
					return nil
				},
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	push := func(ctx context.Context, key roachpb.Key) error {
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := kv.NewTxn(ctx, origDB, 0 /* gatewayNodeID */)
		// We need to explicitly set a high priority for the push to happen.
		if err := conflictTxn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		// Push through a Put, as opposed to a Get, so that the pushee gets aborted.
		if err := conflictTxn.Put(ctx, key, "pusher was here"); err != nil {
			return err
		}
		return conflictTxn.CommitOrCleanup(ctx)
	}

	// Make a db with a short heartbeat interval.
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			// Short heartbeat interval.
			HeartbeatInterval: time.Millisecond,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		s.DistSenderI().(*kvcoord.DistSender),
	)
	db := kv.NewDB(ambient, tsf, s.Clock())
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	if err := txn.Put(ctx, key, "val"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(ctx, key2, "val"); err != nil {
		t.Fatal(err)
	}

	if err := push(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Now wait until the heartbeat loop notices that the transaction is aborted.
	testutils.SucceedsSoon(t, func() error {
		if txn.Sender().(*kvcoord.TxnCoordSender).IsTracking() {
			return fmt.Errorf("txn heartbeat loop running")
		}
		return nil
	})

	// Check that an EndTxn(commit=false) with the right intents has been sent.
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&cleanupSeen) == 0 {
			return fmt.Errorf("no cleanup sent yet")
		}
		return nil
	})

	// Check that further sends through the aborted txn are rejected. The
	// TxnCoordSender is supposed to synthesize a TransactionAbortedError.
	if err := txn.CommitOrCleanup(ctx); !testutils.IsError(
		err, "TransactionRetryWithProtoRefreshError: TransactionAbortedError",
	) {
		t.Fatalf("expected aborted error, got: %s", err)
	}
}

// Test that, when a transaction restarts, we don't get a second heartbeat loop
// for it. This bug happened in the past.
//
// The test traces the restarting transaction and looks in it to see how many
// times a heartbeat loop was started.
func TestNoDuplicateHeartbeatLoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	key := roachpb.Key("a")

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("test", tracing.Recordable)
	tracing.StartRecording(sp, tracing.SingleNodeRecording)
	txnCtx := opentracing.ContextWithSpan(context.Background(), sp)

	push := func(ctx context.Context, key roachpb.Key) error {
		return db.Put(ctx, key, "push")
	}

	var attempts int
	err := db.Txn(txnCtx, func(ctx context.Context, txn *kv.Txn) error {
		attempts++
		if attempts == 1 {
			if err := push(context.Background() /* keep the contexts separate */, key); err != nil {
				return err
			}
		}
		if _, err := txn.Get(ctx, key); err != nil {
			return err
		}
		return txn.Put(ctx, key, "val")
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got: %d", attempts)
	}
	sp.Finish()
	recording := tracing.GetRecording(sp)
	var foundHeartbeatLoop bool
	for _, sp := range recording {
		if strings.Contains(sp.Operation, "heartbeat loop") {
			if foundHeartbeatLoop {
				t.Fatal("second heartbeat loop found")
			}
			foundHeartbeatLoop = true
		}
	}
	if !foundHeartbeatLoop {
		t.Fatal("no heartbeat loop found. Test rotted?")
	}
}
