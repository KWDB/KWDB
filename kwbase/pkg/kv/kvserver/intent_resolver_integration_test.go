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

package kvserver

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func beginTransaction(
	t *testing.T, store *Store, pri roachpb.UserPriority, key roachpb.Key, putKey bool,
) *roachpb.Transaction {
	txn := newTransaction("test", key, pri, store.Clock())
	if !putKey {
		return txn
	}

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	br, pErr := store.TestSender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	return br.Txn
}

// TestContendedIntentWithDependencyCycle verifies that a queue of
// writers on a contended key will still notice a dependency cycle.
// In this case, txn3 writes "a", then txn1 writes "b" and "a", then
// txn2 writes "b", then txn3 writes "b". The deadlock is broken by
// an aborted transaction.
//
// Additional non-transactional reads on the same contended key are
// inserted to verify they do not interfere with writing transactions
// always pushing to ensure the dependency cycle can be detected.
//
// This test is something of an integration test which exercises the
// IntentResolver as well as the concurrency Manager's lockTable and
// txnWaitQueue.
func TestContendedIntentWithDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}

	// Create the three transactions; at this point, none of them have
	// conflicts. Txn1 has written "b", Txn3 has written "a".
	txn1 := beginTransaction(t, store, -3, keyB, true /* putKey */)
	txn2 := beginTransaction(t, store, -2, keyB, false /* putKey */)
	txn3 := beginTransaction(t, store, -1, keyA, true /* putKey */)

	// Send txn1 put, followed by an end transaction.
	txnCh1 := make(chan error, 1)
	go func() {
		put := putArgs(keyA, []byte("value"))
		assignSeqNumsForReqs(txn1, &put)
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &put); pErr != nil {
			txnCh1 <- pErr.GoError()
			return
		}
		et, _ := endTxnArgs(txn1, true)
		et.LockSpans = []roachpb.Span{spanA, spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn1, &et)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &et)
		txnCh1 <- pErr.GoError()
	}()

	// Send a non-transactional read to keyB. This adds an early waiter
	// to the intent resolver on keyB which txn2 must skip in order to
	// properly register itself as a dependency by pushing txn1.
	readCh1 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh1 <- pErr.GoError()
	}()

	// Send txn2 put, followed by an end transaction.
	txnCh2 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn2, &put)
		repl, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &put)
		if pErr != nil {
			txnCh2 <- pErr.GoError()
			return
		}
		txn2Copy := *repl.Header().Txn
		txn2 = &txn2Copy
		et, _ := endTxnArgs(txn2, true)
		et.LockSpans = []roachpb.Span{spanB}
		et.CanCommitAtHigherTimestamp = true
		assignSeqNumsForReqs(txn2, &et)
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &et)
		txnCh2 <- pErr.GoError()
	}()

	// Send another non-transactional read to keyB to add a waiter in
	// between txn2 and txn3. Txn3 must wait on txn2, instead of getting
	// queued behind this reader, in order to establish the dependency cycle.
	readCh2 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh2 <- pErr.GoError()
	}()

	// Send txn3. Pause for 10ms to make it more likely that we have a
	// dependency cycle of length 3, although we don't mind testing
	// either way.
	time.Sleep(10 * time.Millisecond)
	txnCh3 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn3, &put)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &put)
		txnCh3 <- pErr.GoError()
	}()

	// The third transaction will always be aborted.
	err := <-txnCh3
	if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
		t.Fatalf("expected transaction aborted error; got %T", err)
	}
	if err := <-txnCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-txnCh2; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh2; err != nil {
		t.Fatal(err)
	}
}
