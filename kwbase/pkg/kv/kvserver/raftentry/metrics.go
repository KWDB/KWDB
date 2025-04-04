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

package raftentry

import "gitee.com/kwbasedb/kwbase/pkg/util/metric"

var (
	metaEntryCacheSize = metric.Metadata{
		Name:        "raft.entrycache.size",
		Help:        "Number of Raft entries in the Raft entry cache",
		Measurement: "Entry Count",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheBytes = metric.Metadata{
		Name:        "raft.entrycache.bytes",
		Help:        "Aggregate size of all Raft entries in the Raft entry cache",
		Measurement: "Entry Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaEntryCacheAccesses = metric.Metadata{
		Name:        "raft.entrycache.accesses",
		Help:        "Number of cache lookups in the Raft entry cache",
		Measurement: "Accesses",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheHits = metric.Metadata{
		Name:        "raft.entrycache.hits",
		Help:        "Number of successful cache lookups in the Raft entry cache",
		Measurement: "Hits",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics is the set of metrics for the raft entry cache.
type Metrics struct {
	// NB: the values in the gauges are updated asynchronously and may hold stale
	// values in the face of concurrent updates.
	Size     *metric.Gauge
	Bytes    *metric.Gauge
	Accesses *metric.Counter
	Hits     *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Size:     metric.NewGauge(metaEntryCacheSize),
		Bytes:    metric.NewGauge(metaEntryCacheBytes),
		Accesses: metric.NewCounter(metaEntryCacheAccesses),
		Hits:     metric.NewCounter(metaEntryCacheHits),
	}
}
