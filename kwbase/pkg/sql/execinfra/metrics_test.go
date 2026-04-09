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

package execinfra

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

func TestMakeDistSQLMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Verify all metrics are initialized
	if metrics.QueriesActive == nil {
		t.Error("QueriesActive should not be nil")
	}
	if metrics.QueriesTotal == nil {
		t.Error("QueriesTotal should not be nil")
	}
	if metrics.FlowsActive == nil {
		t.Error("FlowsActive should not be nil")
	}
	if metrics.FlowsTotal == nil {
		t.Error("FlowsTotal should not be nil")
	}
	if metrics.FlowsQueued == nil {
		t.Error("FlowsQueued should not be nil")
	}
	if metrics.QueueWaitHist == nil {
		t.Error("QueueWaitHist should not be nil")
	}
	if metrics.MaxBytesHist == nil {
		t.Error("MaxBytesHist should not be nil")
	}
	if metrics.CurBytesCount == nil {
		t.Error("CurBytesCount should not be nil")
	}
	if metrics.VecOpenFDs == nil {
		t.Error("VecOpenFDs should not be nil")
	}
	if metrics.CurDiskBytesCount == nil {
		t.Error("CurDiskBytesCount should not be nil")
	}
	if metrics.MaxDiskBytesHist == nil {
		t.Error("MaxDiskBytesHist should not be nil")
	}

	// Verify metric names
	if metrics.QueriesActive.Name != "sql.distsql.queries.active" {
		t.Errorf("Expected QueriesActive name 'sql.distsql.queries.active', got %s", metrics.QueriesActive.Name)
	}
	if metrics.QueriesTotal.Name != "sql.distsql.queries.total" {
		t.Errorf("Expected QueriesTotal name 'sql.distsql.queries.total', got %s", metrics.QueriesTotal.Name)
	}
	if metrics.FlowsActive.Name != "sql.distsql.flows.active" {
		t.Errorf("Expected FlowsActive name 'sql.distsql.flows.active', got %s", metrics.FlowsActive.Name)
	}
	if metrics.FlowsTotal.Name != "sql.distsql.flows.total" {
		t.Errorf("Expected FlowsTotal name 'sql.distsql.flows.total', got %s", metrics.FlowsTotal.Name)
	}
	if metrics.FlowsQueued.Name != "sql.distsql.flows.queued" {
		t.Errorf("Expected FlowsQueued name 'sql.distsql.flows.queued', got %s", metrics.FlowsQueued.Name)
	}
}

func TestDistSQLMetricsQueryStartStop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Initial state
	if metrics.QueriesActive.Value() != 0 {
		t.Errorf("Expected initial QueriesActive 0, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 0 {
		t.Errorf("Expected initial QueriesTotal 0, got %d", metrics.QueriesTotal.Count())
	}

	// Start first query
	metrics.QueryStart()
	if metrics.QueriesActive.Value() != 1 {
		t.Errorf("Expected QueriesActive 1 after first start, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 1 {
		t.Errorf("Expected QueriesTotal 1 after first start, got %d", metrics.QueriesTotal.Count())
	}

	// Start second query
	metrics.QueryStart()
	if metrics.QueriesActive.Value() != 2 {
		t.Errorf("Expected QueriesActive 2 after second start, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 2 {
		t.Errorf("Expected QueriesTotal 2 after second start, got %d", metrics.QueriesTotal.Count())
	}

	// Stop first query
	metrics.QueryStop()
	if metrics.QueriesActive.Value() != 1 {
		t.Errorf("Expected QueriesActive 1 after first stop, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 2 {
		t.Errorf("Expected QueriesTotal 2 after first stop, got %d", metrics.QueriesTotal.Count())
	}

	// Stop second query
	metrics.QueryStop()
	if metrics.QueriesActive.Value() != 0 {
		t.Errorf("Expected QueriesActive 0 after second stop, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 2 {
		t.Errorf("Expected QueriesTotal 2 after second stop, got %d", metrics.QueriesTotal.Count())
	}
}

func TestDistSQLMetricsFlowStartStop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Initial state
	if metrics.FlowsActive.Value() != 0 {
		t.Errorf("Expected initial FlowsActive 0, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 0 {
		t.Errorf("Expected initial FlowsTotal 0, got %d", metrics.FlowsTotal.Count())
	}

	// Start first flow
	metrics.FlowStart()
	if metrics.FlowsActive.Value() != 1 {
		t.Errorf("Expected FlowsActive 1 after first start, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 1 {
		t.Errorf("Expected FlowsTotal 1 after first start, got %d", metrics.FlowsTotal.Count())
	}

	// Start second flow
	metrics.FlowStart()
	if metrics.FlowsActive.Value() != 2 {
		t.Errorf("Expected FlowsActive 2 after second start, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 2 {
		t.Errorf("Expected FlowsTotal 2 after second start, got %d", metrics.FlowsTotal.Count())
	}

	// Stop first flow
	metrics.FlowStop()
	if metrics.FlowsActive.Value() != 1 {
		t.Errorf("Expected FlowsActive 1 after first stop, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 2 {
		t.Errorf("Expected FlowsTotal 2 after first stop, got %d", metrics.FlowsTotal.Count())
	}

	// Stop second flow
	metrics.FlowStop()
	if metrics.FlowsActive.Value() != 0 {
		t.Errorf("Expected FlowsActive 0 after second stop, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 2 {
		t.Errorf("Expected FlowsTotal 2 after second stop, got %d", metrics.FlowsTotal.Count())
	}
}

func TestDistSQLMetricsMultipleQueriesAndFlows(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Simulate multiple concurrent queries and flows
	for i := 0; i < 5; i++ {
		metrics.QueryStart()
		metrics.FlowStart()
	}

	if metrics.QueriesActive.Value() != 5 {
		t.Errorf("Expected QueriesActive 5, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 5 {
		t.Errorf("Expected QueriesTotal 5, got %d", metrics.QueriesTotal.Count())
	}
	if metrics.FlowsActive.Value() != 5 {
		t.Errorf("Expected FlowsActive 5, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 5 {
		t.Errorf("Expected FlowsTotal 5, got %d", metrics.FlowsTotal.Count())
	}

	// Stop some queries and flows
	for i := 0; i < 3; i++ {
		metrics.QueryStop()
		metrics.FlowStop()
	}

	if metrics.QueriesActive.Value() != 2 {
		t.Errorf("Expected QueriesActive 2, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 5 {
		t.Errorf("Expected QueriesTotal 5, got %d", metrics.QueriesTotal.Count())
	}
	if metrics.FlowsActive.Value() != 2 {
		t.Errorf("Expected FlowsActive 2, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 5 {
		t.Errorf("Expected FlowsTotal 5, got %d", metrics.FlowsTotal.Count())
	}

	// Start more queries and flows
	for i := 0; i < 3; i++ {
		metrics.QueryStart()
		metrics.FlowStart()
	}

	if metrics.QueriesActive.Value() != 5 {
		t.Errorf("Expected QueriesActive 5, got %d", metrics.QueriesActive.Value())
	}
	if metrics.QueriesTotal.Count() != 8 {
		t.Errorf("Expected QueriesTotal 8, got %d", metrics.QueriesTotal.Count())
	}
	if metrics.FlowsActive.Value() != 5 {
		t.Errorf("Expected FlowsActive 5, got %d", metrics.FlowsActive.Value())
	}
	if metrics.FlowsTotal.Count() != 8 {
		t.Errorf("Expected FlowsTotal 8, got %d", metrics.FlowsTotal.Count())
	}
}

func TestDistSQLMetricsMetricStruct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify DistSQLMetrics implements metric.Struct interface
	var _ metric.Struct = DistSQLMetrics{}

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Verify MetricStruct method exists and doesn't panic
	metrics.MetricStruct()
}

func TestDistSQLMetricsHistograms(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Test QueueWaitHist
	metrics.QueueWaitHist.RecordValue(1000000) // 1ms in nanoseconds
	metrics.QueueWaitHist.RecordValue(5000000) // 5ms in nanoseconds

	// Test MaxBytesHist
	metrics.MaxBytesHist.RecordValue(1024)
	metrics.MaxBytesHist.RecordValue(2048)

	// Test MaxDiskBytesHist
	metrics.MaxDiskBytesHist.RecordValue(4096)
	metrics.MaxDiskBytesHist.RecordValue(8192)

	// Verify histograms are working (just check they don't panic)
	// Note: We can't easily verify the exact values in histograms
	// without exposing internal state, but we can verify they were created
	if metrics.QueueWaitHist.Name != "sql.distsql.flows.queue_wait" {
		t.Errorf("Expected QueueWaitHist name 'sql.distsql.flows.queue_wait', got %s", metrics.QueueWaitHist.Name)
	}
	if metrics.MaxBytesHist.Name != "sql.mem.distsql.max" {
		t.Errorf("Expected MaxBytesHist name 'sql.mem.distsql.max', got %s", metrics.MaxBytesHist.Name)
	}
	if metrics.MaxDiskBytesHist.Name != "sql.disk.distsql.max" {
		t.Errorf("Expected MaxDiskBytesHist name 'sql.disk.distsql.max', got %s", metrics.MaxDiskBytesHist.Name)
	}
}

func TestDistSQLMetricsGauges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	histogramWindow := 30 * time.Second
	metrics := MakeDistSQLMetrics(histogramWindow)

	// Test FlowsQueued gauge
	metrics.FlowsQueued.Inc(5)
	if metrics.FlowsQueued.Value() != 5 {
		t.Errorf("Expected FlowsQueued 5, got %d", metrics.FlowsQueued.Value())
	}

	metrics.FlowsQueued.Dec(3)
	if metrics.FlowsQueued.Value() != 2 {
		t.Errorf("Expected FlowsQueued 2, got %d", metrics.FlowsQueued.Value())
	}

	// Test CurBytesCount gauge
	metrics.CurBytesCount.Inc(1024)
	if metrics.CurBytesCount.Value() != 1024 {
		t.Errorf("Expected CurBytesCount 1024, got %d", metrics.CurBytesCount.Value())
	}

	// Test VecOpenFDs gauge
	metrics.VecOpenFDs.Inc(10)
	if metrics.VecOpenFDs.Value() != 10 {
		t.Errorf("Expected VecOpenFDs 10, got %d", metrics.VecOpenFDs.Value())
	}

	// Test CurDiskBytesCount gauge
	metrics.CurDiskBytesCount.Inc(4096)
	if metrics.CurDiskBytesCount.Value() != 4096 {
		t.Errorf("Expected CurDiskBytesCount 4096, got %d", metrics.CurDiskBytesCount.Value())
	}
}
