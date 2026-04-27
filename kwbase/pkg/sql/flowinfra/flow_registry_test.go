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

package flowinfra

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// lookupFlow returns the registered flow with the given ID. If no such flow is
// registered, waits until it gets registered - up to the given timeout. If the
// timeout elapses and the flow is not registered, the bool return value will be
// false.
func lookupFlow(fr *FlowRegistry, fid execinfrapb.FlowID, timeout time.Duration) Flow {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.getEntryLocked(fid)
	if entry.flow != nil {
		return entry.flow
	}
	entry = fr.waitForFlowLocked(context.TODO(), fid, timeout)
	if entry == nil {
		return nil
	}
	return entry.flow
}

// lookupStreamInfo returns a stream entry from a FlowRegistry. If either the
// flow or the streams are missing, an error is returned.
//
// A copy of the registry's InboundStreamInfo is returned so it can be accessed
// without locking.
func lookupStreamInfo(
	fr *FlowRegistry, fid execinfrapb.FlowID, sid execinfrapb.StreamID,
) (InboundStreamInfo, error) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.getEntryLocked(fid)
	if entry.flow == nil {
		return InboundStreamInfo{}, errors.Errorf("missing flow entry: %s", fid)
	}
	si, ok := entry.inboundStreams[sid]
	if !ok {
		return InboundStreamInfo{}, errors.Errorf("missing stream entry: %d", sid)
	}
	return *si, nil
}

func TestFlowRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := NewFlowRegistry(roachpb.NodeID(0))

	id1 := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	f1 := &FlowBase{}

	id2 := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	f2 := &FlowBase{}

	id3 := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	f3 := &FlowBase{}

	id4 := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	f4 := &FlowBase{}

	// A basic duration; needs to be significantly larger than possible delays
	// in scheduling goroutines.
	jiffy := 10 * time.Millisecond

	// -- Lookup, register, lookup, unregister, lookup. --

	if f := lookupFlow(reg, id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	const flowStreamTimeout = 10 * time.Second

	ctx := context.Background()
	if err := reg.RegisterFlow(
		ctx, id1, f1, nil /* inboundStreams */, flowStreamTimeout,
	); err != nil {
		t.Fatal(err)
	}

	if f := lookupFlow(reg, id1, 0); f != f1 {
		t.Error("couldn't lookup previously registered flow")
	}

	reg.UnregisterFlow(id1)

	if f := lookupFlow(reg, id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	// -- Lookup with timeout, register in the meantime. --

	go func() {
		time.Sleep(jiffy)
		if err := reg.RegisterFlow(
			ctx, id1, f1, nil /* inboundStreams */, flowStreamTimeout,
		); err != nil {
			t.Error(err)
		}
	}()

	if f := lookupFlow(reg, id1, 10*jiffy); f != f1 {
		t.Error("couldn't lookup registered flow (with wait)")
	}

	if f := lookupFlow(reg, id1, 0); f != f1 {
		t.Error("couldn't lookup registered flow")
	}

	// -- Multiple lookups before register. --

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		if f := lookupFlow(reg, id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	go func() {
		if f := lookupFlow(reg, id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	time.Sleep(jiffy)
	if err := reg.RegisterFlow(
		ctx, id2, f2, nil /* inboundStreams */, flowStreamTimeout,
	); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// -- Multiple lookups, with the first one failing. --

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	wg2.Add(1)
	go func() {
		if f := lookupFlow(reg, id3, jiffy); f != nil {
			t.Error("expected lookup to fail")
		}
		wg1.Done()
	}()

	go func() {
		if f := lookupFlow(reg, id3, 10*jiffy); f != f3 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg2.Done()
	}()

	wg1.Wait()
	if err := reg.RegisterFlow(
		ctx, id3, f3, nil /* inboundStreams */, flowStreamTimeout,
	); err != nil {
		t.Fatal(err)
	}
	wg2.Wait()

	// -- Lookup with huge timeout, register in the meantime. --

	go func() {
		time.Sleep(jiffy)
		if err := reg.RegisterFlow(
			ctx, id4, f4, nil /* inboundStreams */, flowStreamTimeout,
		); err != nil {
			t.Error(err)
		}
	}()

	// This should return in a jiffy.
	if f := lookupFlow(reg, id4, time.Hour); f != f4 {
		t.Error("couldn't lookup registered flow (with wait)")
	}
}

// Test that, if inbound streams are not connected within the timeout, errors
// are propagated to their consumers and future attempts to connect them fail.
func TestStreamConnectionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := NewFlowRegistry(roachpb.NodeID(0))

	jiffy := time.Nanosecond

	// Register a flow with a very low timeout. After it times out, we'll attempt
	// to connect a stream, but it'll be too late.
	id1 := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	f1 := &FlowBase{}
	replicationID1 := execinfrapb.StreamID(1)
	consumer := &distsqlutils.RowBuffer{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	inboundStreams := map[execinfrapb.StreamID]*InboundStreamInfo{
		replicationID1: {receiver: RowInboundStreamHandler{consumer}, waitGroup: wg},
	}
	if err := reg.RegisterFlow(
		context.TODO(), id1, f1, inboundStreams, jiffy,
	); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		si, err := lookupStreamInfo(reg, id1, replicationID1)
		if err != nil {
			t.Fatal(err)
		}
		if !si.canceled {
			return errors.Errorf("not timed out yet")
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		if !consumer.ProducerClosed() {
			return errors.New("expected consumer to have been closed when the flow timed out")
		}
		return nil
	})

	// Create a dummy server stream to pass to ConnectInboundStream.
	serverStream, _ /* clientStream */, cleanup, err := createDummyStream()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	_, _, _, err = reg.ConnectInboundStream(context.TODO(), id1, replicationID1, serverStream, jiffy)
	if !testutils.IsError(err, "came too late") {
		t.Fatalf("expected %q, got: %v", "came too late", err)
	}

	// Unregister the flow. Subsequent attempts to connect a stream should result
	// in a different error than before.
	reg.UnregisterFlow(id1)
	_, _, _, err = reg.ConnectInboundStream(context.TODO(), id1, replicationID1, serverStream, jiffy)
	if !testutils.IsError(err, "not found") {
		t.Fatalf("expected %q, got: %v", "not found", err)
	}
}

// Test that the FlowRegistry send the correct handshake messages:
// - if an inbound stream arrives to the registry before the consumer is
// scheduled, then a Handshake message informing that the consumer is not yet
// connected is sent;
// - once the consumer connects, another Handshake message is sent.
func TestHandshake(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reg := NewFlowRegistry(roachpb.NodeID(0))

	tests := []struct {
		name                   string
		consumerConnectedEarly bool
	}{
		{
			name:                   "consumer early",
			consumerConnectedEarly: true,
		},
		{
			name:                   "consumer late",
			consumerConnectedEarly: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}
			replicationID := execinfrapb.StreamID(1)

			serverStream, clientStream, cleanup, err := createDummyStream()
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()

			connectProducer := func() {
				// Simulate a producer connecting to the server. This should be called
				// async because the consumer is not yet there and ConnectInboundStream
				// is blocking.
				if _, _, _, err := reg.ConnectInboundStream(
					context.TODO(), flowID, replicationID, serverStream, time.Hour,
				); err != nil {
					t.Error(err)
				}
			}
			connectConsumer := func() {
				f1 := &FlowBase{}
				consumer := &distsqlutils.RowBuffer{}
				wg := &sync.WaitGroup{}
				wg.Add(1)
				inboundStreams := map[execinfrapb.StreamID]*InboundStreamInfo{
					replicationID: {receiver: RowInboundStreamHandler{consumer}, waitGroup: wg},
				}
				if err := reg.RegisterFlow(
					context.TODO(), flowID, f1, inboundStreams, time.Hour, /* timeout */
				); err != nil {
					t.Fatal(err)
				}
			}

			// If the consumer is supposed to be connected early, then we connect the
			// consumer and then we connect the producer. Otherwise, we connect the
			// producer and expect a first handshake and only then we connect the
			// consumer.
			if tc.consumerConnectedEarly {
				connectConsumer()
				go connectProducer()
			} else {
				go connectProducer()

				// Expect the client (the producer) to receive a Handshake saying that the
				// consumer is not connected yet.
				consumerSignal, err := clientStream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if consumerSignal.Handshake == nil {
					t.Fatalf("expected handshake, got: %+v", consumerSignal)
				}
				if consumerSignal.Handshake.ConsumerScheduled {
					t.Fatal("expected !ConsumerScheduled")
				}

				connectConsumer()
			}

			// Now expect another Handshake message telling the producer that the consumer
			// has connected.
			consumerSignal, err := clientStream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if consumerSignal.Handshake == nil {
				t.Fatalf("expected handshake, got: %+v", consumerSignal)
			}
			if !consumerSignal.Handshake.ConsumerScheduled {
				t.Fatal("expected ConsumerScheduled")
			}
		})
	}
}

// TestFlowRegistryDrain verifies a FlowRegistry's draining behavior. See
// subtests for more details.
func TestFlowRegistryDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	reg := NewFlowRegistry(roachpb.NodeID(0))

	flow := &FlowBase{}
	id := execinfrapb.FlowID{UUID: uuid.MakeV4()}
	registerFlow := func(t *testing.T, id execinfrapb.FlowID) {
		t.Helper()
		if err := reg.RegisterFlow(
			ctx, id, flow, nil /* inboundStreams */, 0, /* timeout */
		); err != nil {
			t.Fatal(err)
		}
	}

	// WaitForFlow verifies that Drain waits for a flow to finish within the
	// timeout.
	t.Run("WaitForFlow", func(t *testing.T) {
		registerFlow(t, id)
		drainDone := make(chan struct{})
		go func() {
			reg.Drain(math.MaxInt64 /* flowDrainWait */, 0 /* minFlowDrainWait */, nil /* reporter */)
			drainDone <- struct{}{}
		}()
		// Be relatively sure that the FlowRegistry is draining.
		time.Sleep(time.Microsecond)
		reg.UnregisterFlow(id)
		<-drainDone
		reg.Undrain()
	})

	// DrainTimeout verifies that Drain returns once the timeout expires.
	t.Run("DrainTimeout", func(t *testing.T) {
		registerFlow(t, id)
		reg.Drain(0 /* flowDrainWait */, 0 /* minFlowDrainWait */, nil /* reporter */)
		reg.UnregisterFlow(id)
		reg.Undrain()
	})

	// AcceptNewFlow verifies that a FlowRegistry continues accepting flows
	// while draining.
	t.Run("AcceptNewFlow", func(t *testing.T) {
		registerFlow(t, id)
		drainDone := make(chan struct{})
		go func() {
			reg.Drain(math.MaxInt64 /* flowDrainWait */, 0 /* minFlowDrainWait */, nil /* reporter */)
			drainDone <- struct{}{}
		}()
		// Be relatively sure that the FlowRegistry is draining.
		time.Sleep(time.Microsecond)
		newFlowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}
		registerFlow(t, newFlowID)
		reg.UnregisterFlow(id)
		select {
		case <-drainDone:
			t.Fatal("finished draining before unregistering new flow")
		default:
		}
		reg.UnregisterFlow(newFlowID)
		<-drainDone
		// The registry should not accept new flows once it has finished draining.
		if err := reg.RegisterFlow(
			ctx, id, flow, nil /* inboundStreams */, 0, /* timeout */
		); !testutils.IsError(err, "draining") {
			t.Fatalf("unexpected error: %v", err)
		}
		reg.Undrain()
	})

	// MinFlowWait verifies that the FlowRegistry waits a minimum amount of time
	// for incoming flows to be registered.
	t.Run("MinFlowWait", func(t *testing.T) {
		// Case in which draining is initiated with zero running flows.
		drainDone := make(chan struct{})
		// Register a flow right before the FlowRegistry waits for
		// minFlowDrainWait. Use an errChan because draining is performed from
		// another goroutine and cannot call t.Fatal.
		errChan := make(chan error)
		reg.testingRunBeforeDrainSleep = func() {
			if err := reg.RegisterFlow(
				ctx, id, flow, nil /* inboundStreams */, 0, /* timeout */
			); err != nil {
				errChan <- err
			}
			errChan <- nil
		}
		defer func() { reg.testingRunBeforeDrainSleep = nil }()
		go func() {
			reg.Drain(math.MaxInt64 /* flowDrainWait */, 0 /* minFlowDrainWait */, nil /* reporter */)
			drainDone <- struct{}{}
		}()
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
		reg.UnregisterFlow(id)
		<-drainDone
		reg.Undrain()

		// Case in which a running flow finishes before the minimum wait time. We
		// attempt to register another flow after the completion of the first flow
		// to simulate an incoming flow that is registered during the minimum wait
		// time. However, it is possible to unregister the first flow after the
		// minimum wait time has passed, in which case we simply verify that the
		// FlowRegistry drain process has lasted at least the required wait time.
		registerFlow(t, id)
		reg.testingRunBeforeDrainSleep = func() {
			if err := reg.RegisterFlow(
				ctx, id, flow, nil /* inboundStreams */, 0, /* timeout */
			); err != nil {
				errChan <- err
			}
			errChan <- nil
		}
		minFlowDrainWait := 10 * time.Millisecond
		start := timeutil.Now()
		go func() {
			reg.Drain(math.MaxInt64 /* flowDrainWait */, minFlowDrainWait, nil /* reporter */)
			drainDone <- struct{}{}
		}()
		// Be relatively sure that the FlowRegistry is draining.
		time.Sleep(time.Microsecond)
		reg.UnregisterFlow(id)
		select {
		case <-drainDone:
			if timeutil.Since(start) < minFlowDrainWait {
				t.Fatal("flow registry did not wait at least minFlowDrainWait")
			}
			return
		case err := <-errChan:
			if err != nil {
				t.Fatal(err)
			}
		}
		reg.UnregisterFlow(id)
		<-drainDone

		reg.Undrain()
	})
}

// TestInboundStreamTimeoutIsRetryable verifies that a failure from an inbound
// stream to connect in a timeout is considered retryable by
// pgerror.IsSQLRetryableError.
// TODO(asubiotto): This error should also be considered retryable by clients.
func TestInboundStreamTimeoutIsRetryable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fr := NewFlowRegistry(0)
	wg := sync.WaitGroup{}
	rc := &execinfra.RowChannel{}
	rc.InitWithBufSizeAndNumSenders(sqlbase.OneIntCol, 1 /* chanBufSize */, 1 /* numSenders */)
	inboundStreams := map[execinfrapb.StreamID]*InboundStreamInfo{
		0: {
			receiver:  RowInboundStreamHandler{rc},
			waitGroup: &wg,
		},
	}
	wg.Add(1)
	if err := fr.RegisterFlow(
		context.Background(), execinfrapb.FlowID{}, &FlowBase{}, inboundStreams, 0, /* timeout */
	); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, meta := rc.Next(); meta == nil {
		t.Fatal("expected error but got no meta")
	} else if !pgerror.IsSQLRetryableError(meta.Err) {
		t.Fatalf("unexpected error: %v", meta.Err)
	}
}

// TestTimeoutPushDoesntBlockRegister verifies that in the case of a timeout
// error, we are still able to register flows while Pushing the error (#34041).
func TestTimeoutPushDoesntBlockRegister(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	fr := NewFlowRegistry(0)
	// pushChan is used to be able to tell when a Push on the RowBuffer has
	// occurred.
	pushChan := make(chan *execinfrapb.ProducerMetadata)
	rc := distsqlutils.NewRowBuffer(
		sqlbase.OneIntCol,
		nil, /* rows */
		distsqlutils.RowBufferArgs{
			OnPush: func(_ sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata) {
				pushChan <- meta
				<-pushChan
			},
		},
	)

	wg := sync.WaitGroup{}
	wg.Add(1)
	inboundStreams := map[execinfrapb.StreamID]*InboundStreamInfo{
		0: {
			receiver:  RowInboundStreamHandler{rc},
			waitGroup: &wg,
		},
	}

	// RegisterFlow with an immediate timeout.
	if err := fr.RegisterFlow(
		ctx, execinfrapb.FlowID{}, &FlowBase{}, inboundStreams, 0, /* timeout */
	); err != nil {
		t.Fatal(err)
	}

	// Ensure RegisterFlow performs a Push.
	meta := <-pushChan
	if !testutils.IsError(meta.Err, errNoInboundStreamConnection.Error()) {
		t.Fatalf("unexpected err %v, expected %s", meta.Err, errNoInboundStreamConnection)
	}

	// Attempt to register a flow. Note that this flow has no inbound streams, so
	// Pushing to the RowBuffer is unexpected.
	if err := fr.RegisterFlow(
		ctx, execinfrapb.FlowID{UUID: uuid.MakeV4()}, &FlowBase{}, nil /* inboundStreams */, time.Hour, /* timeout */
	); err != nil {
		t.Fatal(err)
	}

	// Unblock the first RegisterFlow.
	close(pushChan)
}

// TestFlowCancelPartiallyBlocked tests that cancellation messages can propagate
// into a flow even if one of the inbound streams are blocked (#35859).
func TestFlowCancelPartiallyBlocked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	fr := NewFlowRegistry(0)
	left := &execinfra.RowChannel{}
	left.InitWithBufSizeAndNumSenders(nil /* types */, 1, 1)
	right := &execinfra.RowChannel{}
	right.InitWithBufSizeAndNumSenders(nil /* types */, 1, 1)

	wgLeft := sync.WaitGroup{}
	wgLeft.Add(1)
	wgRight := sync.WaitGroup{}
	wgRight.Add(1)
	inboundStreams := map[execinfrapb.StreamID]*InboundStreamInfo{
		0: {
			receiver:  RowInboundStreamHandler{left},
			waitGroup: &wgLeft,
		},
		1: {
			receiver:  RowInboundStreamHandler{right},
			waitGroup: &wgRight,
		},
	}

	// Fill up the left, so pushes to it block.
	left.Push(nil, &execinfrapb.ProducerMetadata{})

	// RegisterFlow with an immediate timeout.
	flow := &FlowBase{
		FlowCtx: execinfra.FlowCtx{
			ID: execinfrapb.FlowID{UUID: uuid.FastMakeV4()},
		},
		inboundStreams: inboundStreams,
		flowRegistry:   fr,
	}
	if err := fr.RegisterFlow(
		ctx, flow.ID, flow, inboundStreams, 10*time.Second, /* timeout */
	); err != nil {
		t.Fatal(err)
	}

	flow.cancel()

	// Reading from the right shouldn't block and should immediately return a
	// flow canceled error.

	_, meta := right.Next()
	if meta.Err != sqlbase.QueryCanceledError {
		t.Fatal("expected query canceled, found", meta.Err)
	}

	// Read from the left to unblock the canceler, assert that the next
	// message is the query canceled message as well.

	_, _ = left.Next()
	_, meta = left.Next()
	if meta.Err != sqlbase.QueryCanceledError {
		t.Fatal("expected query canceled, found", meta.Err)
	}
}

// mockRowReceiver is a mock implementation of execinfra.RowReceiver for testing.
type mockRowReceiver struct {
	rows   []sqlbase.EncDatumRow
	meta   []*execinfrapb.ProducerMetadata
	done   bool
	closed bool
	typs   []types.T
}

func (m *mockRowReceiver) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if row != nil {
		m.rows = append(m.rows, row)
	}
	if meta != nil {
		m.meta = append(m.meta, meta)
	}
	return execinfra.NeedMoreRows
}

func (m *mockRowReceiver) PushPGResult(ctx context.Context, res []byte) error {
	return nil
}

func (m *mockRowReceiver) AddPGComplete(
	complete string, stmtType tree.StatementType, rowsAffected int,
) {
}

func (m *mockRowReceiver) Types() []types.T {
	return m.typs
}

func (m *mockRowReceiver) ProducerDone() {
	m.done = true
}

func (m *mockRowReceiver) GetCols() int {
	return len(m.typs)
}

func (m *mockRowReceiver) AddStats(time time.Duration, isAddRows bool) {
}

func (m *mockRowReceiver) GetStats() execinfra.RowStats {
	return execinfra.RowStats{}
}

func (m *mockRowReceiver) ConsumerDone() {
	m.closed = true
}

func (m *mockRowReceiver) ConsumerClosed() bool {
	return m.closed
}

// mockQueue is a mock implementation of Queue for testing.
type mockQueue struct{}

func (m *mockQueue) Push(ctx context.Context, msg interface{}) error {
	return nil
}

func (m *mockQueue) Pop(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (m *mockQueue) Close() error {
	return nil
}

func TestMessageQueueNewMessageQueueInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	receiver := &mockRowReceiver{}
	typs := []types.T{
		*types.Int,
	}

	mq := NewMessageQueueInfo(nil, receiver, typs)
	if mq == nil {
		t.Error("expected NewMessageQueueInfo() to return a non-nil MessageQueue")
	}
	if len(mq.typs) != len(typs) {
		t.Error("expected MessageQueue.typs to be set correctly")
	}
}

func TestMessageQueueAddRowLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	receiver := &mockRowReceiver{}
	typs := []types.T{
		*types.Int,
	}

	mq := NewMessageQueueInfo(nil, receiver, typs)

	// Initialize the mutex and condition variable
	mq.mu.Lock()
	mq.mu.cond = sync.NewCond(&mq.mu.Mutex)

	// Create a row
	row := sqlbase.EncDatumRow{}

	// Test adding a row
	err := mq.addRowLocked(ctx, row)
	if err != nil {
		t.Errorf("expected addRowLocked() to succeed, got: %v", err)
	}
	if mq.mu.rowBufLen != 1 {
		t.Errorf("expected rowBufLen to be 1, got: %d", mq.mu.rowBufLen)
	}

	mq.mu.Unlock()
}

func TestMessageQueueAddMetadataLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	receiver := &mockRowReceiver{}
	typs := []types.T{
		*types.Int,
	}

	mq := NewMessageQueueInfo(nil, receiver, typs)

	// Initialize the mutex and condition variable
	mq.mu.Lock()
	mq.mu.cond = sync.NewCond(&mq.mu.Mutex)

	// Create metadata
	meta := &execinfrapb.ProducerMetadata{}

	// Test adding metadata
	mq.addMetadataLocked(meta)
	if len(mq.mu.metadataBuf) != 1 {
		t.Errorf("expected metadataBuf length to be 1, got: %d", len(mq.mu.metadataBuf))
	}

	mq.mu.Unlock()
}

func TestIsFlowRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with a retryable error
	retryableErr := &flowRetryableError{cause: nil}
	if !IsFlowRetryableError(retryableErr) {
		t.Error("expected IsFlowRetryableError() to return true for flowRetryableError")
	}

	// Test with a non-retryable error
	nonRetryableErr := fmt.Errorf("non-retryable error")
	if IsFlowRetryableError(nonRetryableErr) {
		t.Error("expected IsFlowRetryableError() to return false for non-retryable error")
	}
}

func TestNewInboundStreamInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	receiver := RowInboundStreamHandler{&mockRowReceiver{}}
	var wg sync.WaitGroup

	isi := NewInboundStreamInfo(receiver, &wg)
	if isi == nil {
		t.Error("expected NewInboundStreamInfo() to return a non-nil InboundStreamInfo")
	}
	if isi.receiver != receiver {
		t.Error("expected InboundStreamInfo.receiver to be set correctly")
	}
	if isi.waitGroup != &wg {
		t.Error("expected InboundStreamInfo.waitGroup to be set correctly")
	}
	if isi.connected {
		t.Error("expected InboundStreamInfo.connected to be false initially")
	}
	if isi.canceled {
		t.Error("expected InboundStreamInfo.canceled to be false initially")
	}
	if isi.finished {
		t.Error("expected InboundStreamInfo.finished to be false initially")
	}
}
