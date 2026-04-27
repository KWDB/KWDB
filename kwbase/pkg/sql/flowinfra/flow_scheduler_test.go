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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// mockFlow is a mock implementation of Flow for testing.
type mockFlow struct {
	id              execinfrapb.FlowID
	flowSpecMemSize int64
	started         bool
	cleanedUp       bool
}

func (m *mockFlow) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt FuseOpt,
) (context.Context, error) {
	return ctx, nil
}

func (m *mockFlow) SetTxn(txn *kv.Txn) {
}

func (m *mockFlow) SetTS(ts bool) {
}

func (m *mockFlow) SetQueryShortCircuit(useQueryShortCircuit bool) {
}

func (m *mockFlow) SetVectorized(vectorized bool) {
}

func (m *mockFlow) Start(ctx context.Context, doneFn func()) error {
	m.started = true
	if doneFn != nil {
		doneFn()
	}
	return nil
}

func (m *mockFlow) Run(ctx context.Context, doneFn func()) error {
	m.started = true
	if doneFn != nil {
		doneFn()
	}
	return nil
}

func (m *mockFlow) StartProcessor(ctx context.Context, doneFn func()) error {
	return nil
}

func (m *mockFlow) NextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, nil
}

func (m *mockFlow) Wait() {
}

func (m *mockFlow) IsLocal() bool {
	return true
}

func (m *mockFlow) IsVectorized() bool {
	return false
}

func (m *mockFlow) IsTimeSeries() bool {
	return false
}

func (m *mockFlow) GetFlowCtx() *execinfra.FlowCtx {
	return &execinfra.FlowCtx{}
}

func (m *mockFlow) AddStartable(startable Startable) {
}

func (m *mockFlow) GetID() execinfrapb.FlowID {
	return m.id
}

func (m *mockFlow) Cleanup(ctx context.Context) {
	m.cleanedUp = true
}

func (m *mockFlow) ConcurrentExecution() bool {
	return false
}

func (m *mockFlow) SetFlowSpecMemSize(memSize int64) {
	m.flowSpecMemSize = memSize
}

func (m *mockFlow) GetFlowSpecMemSize() int64 {
	return m.flowSpecMemSize
}

func (m *mockFlow) GetStats() execinfra.RowStats {
	return execinfra.RowStats{}
}

func TestNewFlowScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ambient := log.AmbientContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	settings := cluster.MakeClusterSettings()
	var metrics *execinfra.DistSQLMetrics
	var acc *mon.BoundAccount

	fs := NewFlowScheduler(ambient, stopper, settings, metrics, acc)
	if fs == nil {
		t.Error("expected NewFlowScheduler() to return a non-nil FlowScheduler")
	}
	if fs.stopper != stopper {
		t.Error("expected FlowScheduler.stopper to be set correctly")
	}
	if fs.metrics != metrics {
		t.Error("expected FlowScheduler.metrics to be set correctly")
	}
}

func TestFlowSchedulerCanRunFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ambient := log.AmbientContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	settings := cluster.MakeClusterSettings()
	var metrics *execinfra.DistSQLMetrics
	var acc *mon.BoundAccount

	fs := NewFlowScheduler(ambient, stopper, settings, metrics, acc)

	// Test canRunFlow with no running flows
	fs.mu.Lock()
	canRun := fs.canRunFlow(&mockFlow{})
	fs.mu.Unlock()

	if !canRun {
		t.Error("expected canRunFlow() to return true with no running flows")
	}

	// Test canRunFlow with max running flows reached
	fs.mu.Lock()
	fs.mu.numRunning = fs.mu.maxRunningFlows
	canRun = fs.canRunFlow(&mockFlow{})
	fs.mu.Unlock()

	if canRun {
		t.Error("expected canRunFlow() to return false when max running flows reached")
	}
}
