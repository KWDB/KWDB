// Copyright 2015 The Cockroach Authors.
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

package status

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/status/statuspb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/ts/tspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/kr/pretty"
)

// byTimeAndName is a slice of tspb.TimeSeriesData.
type byTimeAndName []tspb.TimeSeriesData

// implement sort.Interface for byTimeAndName
func (a byTimeAndName) Len() int      { return len(a) }
func (a byTimeAndName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeAndName) Less(i, j int) bool {
	if a[i].Name != a[j].Name {
		return a[i].Name < a[j].Name
	}
	if a[i].Datapoints[0].TimestampNanos != a[j].Datapoints[0].TimestampNanos {
		return a[i].Datapoints[0].TimestampNanos < a[j].Datapoints[0].TimestampNanos
	}
	return a[i].Source < a[j].Source
}

var _ sort.Interface = byTimeAndName{}

// byStoreID is a slice of roachpb.StoreID.
type byStoreID []roachpb.StoreID

// implement sort.Interface for byStoreID
func (a byStoreID) Len() int      { return len(a) }
func (a byStoreID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byStoreID) Less(i, j int) bool {
	return a[i] < a[j]
}

var _ sort.Interface = byStoreID{}

// byStoreDescID is a slice of storage.StoreStatus
type byStoreDescID []statuspb.StoreStatus

// implement sort.Interface for byStoreDescID.
func (a byStoreDescID) Len() int      { return len(a) }
func (a byStoreDescID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byStoreDescID) Less(i, j int) bool {
	return a[i].Desc.StoreID < a[j].Desc.StoreID
}

var _ sort.Interface = byStoreDescID{}

// fakeStore implements only the methods of store needed by MetricsRecorder to
// interact with stores.
type fakeStore struct {
	storeID  roachpb.StoreID
	desc     roachpb.StoreDescriptor
	registry *metric.Registry
}

func (fs fakeStore) StoreID() roachpb.StoreID {
	return fs.storeID
}

func (fs fakeStore) Descriptor(_ bool) (*roachpb.StoreDescriptor, error) {
	return &fs.desc, nil
}

func (fs fakeStore) Registry() *metric.Registry {
	return fs.registry
}

// TestMetricsRecorder verifies that the metrics recorder properly formats the
// statistics from various registries, both for Time Series and for Status
// Summaries.
func TestMetricsRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// ========================================
	// Construct a series of fake descriptors for use in test.
	// ========================================
	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(1),
	}
	storeDesc1 := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(1),
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 50,
			Used:      50,
		},
	}
	storeDesc2 := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(2),
		Capacity: roachpb.StoreCapacity{
			Capacity:  200,
			Available: 75,
			Used:      125,
		},
	}

	// ========================================
	// Create registries and add them to the recorder (two node-level, two
	// store-level).
	// ========================================
	reg1 := metric.NewRegistry()
	store1 := fakeStore{
		storeID:  roachpb.StoreID(1),
		desc:     storeDesc1,
		registry: metric.NewRegistry(),
	}
	store2 := fakeStore{
		storeID:  roachpb.StoreID(2),
		desc:     storeDesc2,
		registry: metric.NewRegistry(),
	}
	manual := hlc.NewManualClock(100)
	st := cluster.MakeTestingClusterSettings()
	recorder := NewMetricsRecorder(hlc.NewClock(manual.UnixNano, time.Nanosecond), nil, nil, nil, st)
	recorder.AddStore(store1)
	recorder.AddStore(store2)
	recorder.AddNode(reg1, nodeDesc, 50, "foo:26257", "foo:26258", "foo:5432")

	// Ensure the metric system's view of time does not advance during this test
	// as the test expects time to not advance too far which would age the actual
	// data (e.g. in histogram's) unexpectedly.
	defer metric.TestingSetNow(func() time.Time {
		return timeutil.Unix(0, manual.UnixNano())
	})()

	// ========================================
	// Generate Metrics Data & Expected Results
	// ========================================

	// Flatten the four registries into an array for ease of use.
	regList := []struct {
		reg    *metric.Registry
		prefix string
		source int64
		isNode bool
	}{
		{
			reg:    reg1,
			prefix: "one.",
			source: 1,
			isNode: true,
		},
		{
			reg:    reg1,
			prefix: "two.",
			source: 1,
			isNode: true,
		},
		{
			reg:    store1.registry,
			prefix: "",
			source: int64(store1.storeID),
			isNode: false,
		},
		{
			reg:    store2.registry,
			prefix: "",
			source: int64(store2.storeID),
			isNode: false,
		},
	}

	// Every registry will have a copy of the following metrics.
	metricNames := []struct {
		name string
		typ  string
		val  int64
	}{
		{"testGauge", "gauge", 20},
		{"testGaugeFloat64", "floatgauge", 20},
		{"testCounter", "counter", 5},
		{"testHistogram", "histogram", 10},
		{"testLatency", "latency", 10},

		// Stats needed for store summaries.
		{"ranges", "counter", 1},
		{"replicas.leaders", "gauge", 1},
		{"replicas.leaseholders", "gauge", 1},
		{"ranges", "gauge", 1},
		{"ranges.unavailable", "gauge", 1},
		{"ranges.underreplicated", "gauge", 1},
	}

	// Add the metrics to each registry and set their values. At the same time,
	// generate expected time series results and status summary metric values.
	var expected []tspb.TimeSeriesData
	expectedNodeSummaryMetrics := make(map[string]float64)
	expectedStoreSummaryMetrics := make(map[string]float64)

	// addExpected generates expected data for a single metric data point.
	addExpected := func(prefix, name string, source, time, val int64, isNode bool) {
		// Generate time series data.
		tsPrefix := "cr.node."
		if !isNode {
			tsPrefix = "cr.store."
		}
		expect := tspb.TimeSeriesData{
			Name:   tsPrefix + prefix + name,
			Source: strconv.FormatInt(source, 10),
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: time,
					Value:          float64(val),
				},
			},
		}
		expected = append(expected, expect)

		// Generate status summary data.
		if isNode {
			expectedNodeSummaryMetrics[prefix+name] = float64(val)
		} else {
			// This can overwrite the previous value, but this is expected as
			// all stores in our tests have identical values; when comparing
			// status summaries, the same map is used as expected data for all
			// stores.
			expectedStoreSummaryMetrics[prefix+name] = float64(val)
		}
	}

	// Add metric for node ID.
	g := metric.NewGauge(metric.Metadata{Name: "node-id"})
	g.Update(int64(nodeDesc.NodeID))
	addExpected("", "node-id", 1, 100, g.Value(), true)

	for _, reg := range regList {
		for _, data := range metricNames {
			switch data.typ {
			case "gauge":
				g := metric.NewGauge(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(g)
				g.Update(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "floatgauge":
				g := metric.NewGaugeFloat64(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(g)
				g.Update(float64(data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "counter":
				c := metric.NewCounter(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(c)
				c.Inc((data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "histogram":
				h := metric.NewHistogram(metric.Metadata{Name: reg.prefix + data.name}, time.Second, 1000, 2)
				reg.reg.AddMetric(h)
				h.RecordValue(data.val)
				for _, q := range recordHistogramQuantiles {
					addExpected(reg.prefix, data.name+q.suffix, reg.source, 100, data.val, reg.isNode)
				}
			case "latency":
				l := metric.NewLatency(metric.Metadata{Name: reg.prefix + data.name}, time.Hour)
				reg.reg.AddMetric(l)
				l.RecordValue(data.val)
				// Latency is simply three histograms (at different resolution
				// time scales).
				for _, q := range recordHistogramQuantiles {
					addExpected(reg.prefix, data.name+q.suffix, reg.source, 100, data.val, reg.isNode)
				}
			default:
				t.Fatalf("unexpected: %+v", data)
			}
		}
	}

	// ========================================
	// Verify time series data
	// ========================================
	actual := recorder.GetTimeSeriesData()

	// Actual comparison is simple: sort the resulting arrays by time and name,
	// and use reflect.DeepEqual.
	sort.Sort(byTimeAndName(actual))
	sort.Sort(byTimeAndName(expected))
	if a, e := actual, expected; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not yield expected time series collection; diff:\n %v", pretty.Diff(e, a))
	}

	totalMemory, err := GetTotalMemory(context.Background())
	if err != nil {
		t.Error("couldn't get total memory", err)
	}

	// ========================================
	// Verify node summary generation
	// ========================================
	expectedNodeSummary := &statuspb.NodeStatus{
		Desc:      nodeDesc,
		BuildInfo: build.GetInfo(),
		StartedAt: 50,
		UpdatedAt: 100,
		Metrics:   expectedNodeSummaryMetrics,
		StoreStatuses: []statuspb.StoreStatus{
			{
				Desc:    storeDesc1,
				Metrics: expectedStoreSummaryMetrics,
			},
			{
				Desc:    storeDesc2,
				Metrics: expectedStoreSummaryMetrics,
			},
		},
		TotalSystemMemory: totalMemory,
		NumCpus:           int32(runtime.NumCPU()),
	}

	// Make sure there is at least one environment variable that will be
	// reported.
	if err := os.Setenv("GOGC", "100"); err != nil {
		t.Fatal(err)
	}

	nodeSummary := recorder.GenerateNodeStatus(context.Background())
	if nodeSummary == nil {
		t.Fatalf("recorder did not return nodeSummary")
	}
	if len(nodeSummary.Args) == 0 {
		t.Fatalf("expected args to be present")
	}
	if len(nodeSummary.Env) == 0 {
		t.Fatalf("expected env to be present")
	}
	nodeSummary.Args = nil
	nodeSummary.Env = nil
	nodeSummary.Activity = nil
	nodeSummary.Latencies = nil

	sort.Sort(byStoreDescID(nodeSummary.StoreStatuses))
	if a, e := nodeSummary, expectedNodeSummary; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not produce expected NodeSummary; diff:\n %s", pretty.Diff(e, a))
	}

	// Make sure that all methods other than GenerateNodeStatus can operate in
	// parallel with each other (i.e. even if recorder.mu is RLocked).
	recorder.mu.RLock()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			if _, err := recorder.MarshalJSON(); err != nil {
				t.Error(err)
			}
			_ = recorder.PrintAsText(ioutil.Discard)
			_ = recorder.GetTimeSeriesData()
			wg.Done()
		}()
	}
	wg.Wait()
	recorder.mu.RUnlock()
}
