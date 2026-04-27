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

package execinfrapb

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// TestCallbackMetadataSourceDrainMeta tests the DrainMeta method of CallbackMetadataSource
func TestCallbackMetadataSourceDrainMeta(t *testing.T) {
	// Create a callback function
	called := false
	callback := func(ctx context.Context) []ProducerMetadata {
		called = true
		return []ProducerMetadata{}
	}

	// Create CallbackMetadataSource
	source := CallbackMetadataSource{
		DrainMetaCb: callback,
	}

	// Test DrainMeta
	ctx := context.Background()
	meta := source.DrainMeta(ctx)
	if !called {
		t.Error("DrainMetaCb was not called")
	}
	if len(meta) != 0 {
		t.Errorf("DrainMeta returned %d metadata items, expected 0", len(meta))
	}
}

// TestNewMockDistSQLServer tests the newMockDistSQLServer function
func TestNewMockDistSQLServer(t *testing.T) {
	// Test newMockDistSQLServer
	mock := newMockDistSQLServer()
	if mock == nil {
		t.Error("newMockDistSQLServer returned nil")
	}
	if mock.InboundStreams == nil {
		t.Error("InboundStreams channel is nil")
	}
	if mock.RunSyncFlowCalls == nil {
		t.Error("RunSyncFlowCalls channel is nil")
	}
}

// TestMockDistSQLServerSetupFlow tests the SetupFlow method of MockDistSQLServer
func TestMockDistSQLServerSetupFlow(t *testing.T) {
	// Create MockDistSQLServer
	mock := newMockDistSQLServer()

	// Test SetupFlow
	ctx := context.Background()
	req := &SetupFlowRequest{}
	resp, err := mock.SetupFlow(ctx, req)
	if err != nil {
		t.Errorf("SetupFlow() failed: %v", err)
	}
	if resp != nil {
		t.Error("SetupFlow() returned non-nil response")
	}
}

// TestMockDialer tests the MockDialer struct
func TestMockDialer(t *testing.T) {
	// Create MockDialer
	dialer := &MockDialer{}

	// Test Close (should panic since conn is nil)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Close() did not panic when conn is nil")
		}
	}()
	dialer.Close()
}

// TestStartMockDistSQLServer tests the StartMockDistSQLServer function
func TestStartMockDistSQLServer(t *testing.T) {
	// Create stopper
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Create clock
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Test StartMockDistSQLServer
	_, mock, addr, err := StartMockDistSQLServer(clock, stopper, 1)
	if err != nil {
		t.Errorf("StartMockDistSQLServer() failed: %v", err)
	}
	if mock == nil {
		t.Error("StartMockDistSQLServer returned nil mock")
	}
	if addr == nil {
		t.Error("StartMockDistSQLServer returned nil addr")
	}
}
