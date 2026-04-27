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

package flowinfra

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"google.golang.org/grpc/metadata"
)

// mockDistSQLFlowStreamServer is a mock implementation of execinfrapb.DistSQL_FlowStreamServer for testing.
type mockDistSQLFlowStreamServer struct {
	sentSignals []*execinfrapb.ConsumerSignal
	ctx         context.Context
}

func (m *mockDistSQLFlowStreamServer) Send(signal *execinfrapb.ConsumerSignal) error {
	m.sentSignals = append(m.sentSignals, signal)
	return nil
}

func (m *mockDistSQLFlowStreamServer) Recv() (*execinfrapb.ProducerMessage, error) {
	return nil, nil
}

func (m *mockDistSQLFlowStreamServer) RecvMsg(msg interface{}) error {
	return nil
}

func (m *mockDistSQLFlowStreamServer) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockDistSQLFlowStreamServer) SendHeader(header metadata.MD) error {
	return nil
}

func (m *mockDistSQLFlowStreamServer) SetHeader(header metadata.MD) error {
	return nil
}

func (m *mockDistSQLFlowStreamServer) SetTrailer(trailer metadata.MD) {
}

func (m *mockDistSQLFlowStreamServer) Context() context.Context {
	return m.ctx
}

func TestSendDrainSignalToStreamProducer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	mockStream := &mockDistSQLFlowStreamServer{}

	err := sendDrainSignalToStreamProducer(ctx, mockStream)
	if err != nil {
		t.Errorf("expected sendDrainSignalToStreamProducer() to succeed, got: %v", err)
	}

	if len(mockStream.sentSignals) != 1 {
		t.Errorf("expected 1 signal to be sent, got: %d", len(mockStream.sentSignals))
	}

	if mockStream.sentSignals[0].DrainRequest == nil {
		t.Error("expected DrainRequest to be set")
	}
}
