// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

const metricsSampleInterval = 10 * time.Second

func getMetricMeta(meta metric.Metadata, internal bool) metric.Metadata {
	if internal {
	}

	return meta
}

// MLPredictMetrics contains pointers to the metrics of ML prediction.
type MLPredictMetrics struct {
	OnlinePredictTotalCount   *metric.Counter
	OnlinePredictSuccessCount *metric.Counter
	OnlinePredictResponseTime *metric.Histogram
}

// MetricStruct implements the metrics.Struct interface.
func (MLPredictMetrics) MetricStruct() {}

var _ metric.Struct = MLPredictMetrics{}

// MakeMLPredictMetrics make ml predict metrics.
func MakeMLPredictMetrics() *MLPredictMetrics {
	prefix := "ml.predict"

	// ML predict metrics counter names.
	MetaOnlinePredictTotalCount := metric.Metadata{
		Name:        prefix + ".total.count",
		Help:        "Number of predictions",
		Measurement: "Predictions",
		Unit:        metric.Unit_COUNT,
	}
	MetaOnlinePredictSuccessCount := metric.Metadata{
		Name:        prefix + ".success.count",
		Help:        "Number of success predictions",
		Measurement: "Predictions",
		Unit:        metric.Unit_COUNT,
	}
	MetaOnlinePredictResponseTime := metric.Metadata{
		Name:        prefix + ".response.latency",
		Help:        "Number of avg response latency",
		Measurement: "Time",
		Unit:        metric.Unit_NANOSECONDS,
	}

	return &MLPredictMetrics{
		OnlinePredictTotalCount:   metric.NewCounter(MetaOnlinePredictTotalCount),
		OnlinePredictSuccessCount: metric.NewCounter(MetaOnlinePredictSuccessCount),
		OnlinePredictResponseTime: metric.NewLatency(getMetricMeta(MetaOnlinePredictResponseTime, false),
			metricsSampleInterval),
	}
}

// Enabled used to turn on/off ml component.
var Enabled = settings.RegisterPublicBoolSetting(
	"ml.enabled",
	"set to true will initialize ml related tables and user",
	false,
)
