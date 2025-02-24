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

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/caller"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

func TestTransportMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	clients := []batchClient{
		{replica: rd1},
		{replica: rd2},
		{replica: rd3},
	}
	gt := grpcTransport{orderedClients: clients}

	verifyOrder := func(replicas []roachpb.ReplicaDescriptor) {
		file, line, _ := caller.Lookup(1)
		for i, bc := range gt.orderedClients {
			if bc.replica != replicas[i] {
				t.Fatalf("%s:%d: expected order %+v; got mismatch at index %d: %+v",
					file, line, replicas, i, bc.replica)
			}
		}
	}

	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Now replica 3.
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})

	// Advance the client index and move replica 3 back to front.
	gt.clientIndex++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.clientIndex != 0 {
		t.Fatalf("expected client index 0; got %d", gt.clientIndex)
	}

	// Advance the client index again and verify replica 3 can
	// be moved to front for a second retry.
	gt.clientIndex++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.clientIndex != 0 {
		t.Fatalf("expected client index 0; got %d", gt.clientIndex)
	}

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and move rd1 front; should be no change.
	gt.clientIndex++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and and move rd1 to front. Should move
	// client index back for a retry.
	gt.clientIndex++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})
	if gt.clientIndex != 1 {
		t.Fatalf("expected client index 1; got %d", gt.clientIndex)
	}

	// Advance client index once more; verify second retry.
	gt.clientIndex++
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})
	if gt.clientIndex != 1 {
		t.Fatalf("expected client index 1; got %d", gt.clientIndex)
	}
}

// TestSpanImport tests that the gRPC transport ingests trace information that
// came from gRPC responses (through the "snowball tracing" mechanism).
func TestSpanImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	metrics := makeDistSenderMetrics()
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}
	server := mockInternalClient{}
	// Let's spice things up and simulate an error from the server.
	expectedErr := "my expected error"
	server.pErr = roachpb.NewErrorf(expectedErr)

	recCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
	defer cancel()

	server.tr = opentracing.SpanFromContext(recCtx).Tracer().(*tracing.Tracer)

	br, err := gt.sendBatch(recCtx, roachpb.NodeID(1), &server, roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !testutils.IsPError(br.Error, expectedErr) {
		t.Fatalf("expected err: %s, got: %q", expectedErr, br.Error)
	}
	expectedMsg := "mockInternalClient processing batch"
	if tracing.FindMsgInRecording(getRec(), expectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", expectedMsg)
	}
}

// mockInternalClient is an implementation of roachpb.InternalClient.
// It simulates aspects of how the Node normally handles tracing in gRPC calls.
type mockInternalClient struct {
	tr   *tracing.Tracer
	pErr *roachpb.Error
}

var _ roachpb.InternalClient = &mockInternalClient{}

// Batch is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) Batch(
	ctx context.Context, in *roachpb.BatchRequest, opts ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	sp := m.tr.StartRootSpan("mock", nil /* logTags */, tracing.RecordableSpan)
	defer sp.Finish()
	tracing.StartRecording(sp, tracing.SnowballRecording)
	ctx = opentracing.ContextWithSpan(ctx, sp)

	log.Eventf(ctx, "mockInternalClient processing batch")
	br := &roachpb.BatchResponse{}
	br.Error = m.pErr
	if rec := tracing.GetRecording(sp); rec != nil {
		br.CollectedSpans = append(br.CollectedSpans, rec...)
	}
	return br, nil
}

// RangeFeed is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeFeed(
	ctx context.Context, in *roachpb.RangeFeedRequest, opts ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported RangeFeed call")
}
