// Copyright 2019 The Cockroach Authors.
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

package rangefeed

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil/singleflight"
)

var (
	metaRangeFeedCatchupScanNanos = metric.Metadata{
		Name:        "kv.rangefeed.catchup_scan_nanos",
		Help:        "Time spent in RangeFeed catchup scan",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// Metrics are for production monitoring of RangeFeeds.
type Metrics struct {
	RangeFeedCatchupScanNanos *metric.Counter

	RangeFeedSlowClosedTimestampLogN  log.EveryN
	RangeFeedSlowClosedTimestampNudge singleflight.Group
	// RangeFeedSlowClosedTimestampNudgeSem bounds the amount of work that can be
	// spun up on behalf of the RangeFeed nudger. We don't expect to hit this
	// limit, but it's here to limit the effect on stability in case something
	// unexpected happens.
	RangeFeedSlowClosedTimestampNudgeSem chan struct{}
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// NewMetrics makes the metrics for RangeFeeds monitoring.
func NewMetrics() *Metrics {
	return &Metrics{
		RangeFeedCatchupScanNanos:            metric.NewCounter(metaRangeFeedCatchupScanNanos),
		RangeFeedSlowClosedTimestampLogN:     log.Every(5 * time.Second),
		RangeFeedSlowClosedTimestampNudgeSem: make(chan struct{}, 1024),
	}
}
