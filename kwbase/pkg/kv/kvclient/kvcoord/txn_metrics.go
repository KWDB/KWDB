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

package kvcoord

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts          *metric.Counter
	Commits         *metric.Counter
	Commits1PC      *metric.Counter // Commits which finished in a single phase
	ParallelCommits *metric.Counter // Commits which entered the STAGING state

	RefreshSuccess                *metric.Counter
	RefreshFail                   *metric.Counter
	RefreshFailWithCondensedSpans *metric.Counter
	RefreshMemoryLimitExceeded    *metric.Counter

	Durations *metric.Histogram

	TxnsWithCondensedIntents      *metric.Counter
	TxnsWithCondensedIntentsGauge *metric.Gauge

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld           telemetry.CounterWithMetric
	RestartsWriteTooOldMulti      telemetry.CounterWithMetric
	RestartsSerializable          telemetry.CounterWithMetric
	RestartsAsyncWriteFailure     telemetry.CounterWithMetric
	RestartsReadWithinUncertainty telemetry.CounterWithMetric
	RestartsTxnAborted            telemetry.CounterWithMetric
	RestartsTxnPush               telemetry.CounterWithMetric
	RestartsUnknown               telemetry.CounterWithMetric
}

var (
	metaAbortsRates = metric.Metadata{
		Name:        "txn.aborts",
		Help:        "Number of aborted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommitsRates = metric.Metadata{
		Name:        "txn.commits",
		Help:        "Number of committed KV transactions (including 1PC)",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommits1PCRates = metric.Metadata{
		Name:        "txn.commits1PC",
		Help:        "Number of KV transaction on-phase commit attempts",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaParallelCommitsRates = metric.Metadata{
		Name:        "txn.parallelcommits",
		Help:        "Number of KV transaction parallel commit attempts",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshSuccess = metric.Metadata{
		Name:        "txn.refresh.success",
		Help:        "Number of successful refreshes",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshFail = metric.Metadata{
		Name:        "txn.refresh.fail",
		Help:        "Number of failed refreshes",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshFailWithCondensedSpans = metric.Metadata{
		Name: "txn.refresh.fail_with_condensed_spans",
		Help: "Number of failed refreshes for transactions whose read " +
			"tracking lost fidelity because of condensing. Such a failure " +
			"could be a false conflict. Failures counted here are also counted " +
			"in txn.refresh.fail, and the respective transactions are also counted in " +
			"txn.refresh.memory_limit_exceeded.",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshMemoryLimitExceeded = metric.Metadata{
		Name: "txn.refresh.memory_limit_exceeded",
		Help: "Number of transaction which exceed the refresh span bytes limit, causing " +
			"their read spans to be condensed",
		Measurement: "Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaDurationsHistograms = metric.Metadata{
		Name:        "txn.durations",
		Help:        "KV transaction durations",
		Measurement: "KV Txn Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaTxnsWithCondensedIntentSpans = metric.Metadata{
		Name: "txn.condensed_intent_spans",
		Help: "KV transactions that have exceeded their intent tracking " +
			"memory budget (kv.transaction.max_intents_bytes). See also " +
			"txn.condensed_intent_spans_gauge for a gauge of such transactions currently running.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaTxnsWithCondensedIntentSpansGauge = metric.Metadata{
		Name: "txn.condensed_intent_spans_gauge",
		Help: "KV transactions currently running that have exceeded their intent tracking " +
			"memory budget (kv.transaction.max_intents_bytes). See also txn.condensed_intent_spans " +
			"for a perpetual counter/rate.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsHistogram = metric.Metadata{
		Name:        "txn.restarts",
		Help:        "Number of restarted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// There are two ways we can get "write too old" restarts. In both cases, a
	// WriteTooOldError is generated in the MVCC layer. This is intercepted on
	// the way out by the Store, which performs a single retry at a pushed
	// timestamp. If the retry succeeds, the immediate operation succeeds but
	// the WriteTooOld flag is set on the Transaction, which causes EndTxn to
	// return a/ TransactionRetryError with RETRY_WRITE_TOO_OLD. These are
	// captured as txn.restarts.writetooold.
	//
	// If the Store's retried operation generates a second WriteTooOldError
	// (indicating a conflict with a third transaction with a higher timestamp
	// than the one that caused the first WriteTooOldError), the store doesn't
	// retry again, and the WriteTooOldError will be returned up the stack to be
	// retried at this level. These are captured as
	// txn.restarts.writetoooldmulti. This path is inefficient, and if it turns
	// out to be common we may want to do something about it.
	metaRestartsWriteTooOld = metric.Metadata{
		Name:        "txn.restarts.writetooold",
		Help:        "Number of restarts due to a concurrent writer committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsWriteTooOldMulti = metric.Metadata{
		Name:        "txn.restarts.writetoooldmulti",
		Help:        "Number of restarts due to multiple concurrent writers committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsSerializable = metric.Metadata{
		Name:        "txn.restarts.serializable",
		Help:        "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsAsyncWriteFailure = metric.Metadata{
		Name:        "txn.restarts.asyncwritefailure",
		Help:        "Number of restarts due to async consensus writes that failed to leave intents",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsReadWithinUncertainty = metric.Metadata{
		Name:        "txn.restarts.readwithinuncertainty",
		Help:        "Number of restarts due to reading a new value within the uncertainty interval",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsTxnAborted = metric.Metadata{
		Name:        "txn.restarts.txnaborted",
		Help:        "Number of restarts due to an abort by a concurrent transaction (usually due to deadlock)",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// TransactionPushErrors at this level are unusual. They are
	// normally handled at the Store level with the txnwait and
	// contention queues. However, they can reach this level and be
	// retried in tests that disable the store-level retries, and
	// there may be edge cases that allow them to reach this point in
	// production.
	metaRestartsTxnPush = metric.Metadata{
		Name:        "txn.restarts.txnpush",
		Help:        "Number of restarts due to a transaction push failure",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsUnknown = metric.Metadata{
		Name:        "txn.restarts.unknown",
		Help:        "Number of restarts due to a unknown reasons",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                        metric.NewCounter(metaAbortsRates),
		Commits:                       metric.NewCounter(metaCommitsRates),
		Commits1PC:                    metric.NewCounter(metaCommits1PCRates),
		ParallelCommits:               metric.NewCounter(metaParallelCommitsRates),
		RefreshFail:                   metric.NewCounter(metaRefreshFail),
		RefreshFailWithCondensedSpans: metric.NewCounter(metaRefreshFailWithCondensedSpans),
		RefreshSuccess:                metric.NewCounter(metaRefreshSuccess),
		RefreshMemoryLimitExceeded:    metric.NewCounter(metaRefreshMemoryLimitExceeded),
		Durations:                     metric.NewLatency(metaDurationsHistograms, histogramWindow),
		TxnsWithCondensedIntents:      metric.NewCounter(metaTxnsWithCondensedIntentSpans),
		TxnsWithCondensedIntentsGauge: metric.NewGauge(metaTxnsWithCondensedIntentSpansGauge),
		Restarts:                      metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:           telemetry.NewCounterWithMetric(metaRestartsWriteTooOld),
		RestartsWriteTooOldMulti:      telemetry.NewCounterWithMetric(metaRestartsWriteTooOldMulti),
		RestartsSerializable:          telemetry.NewCounterWithMetric(metaRestartsSerializable),
		RestartsAsyncWriteFailure:     telemetry.NewCounterWithMetric(metaRestartsAsyncWriteFailure),
		RestartsReadWithinUncertainty: telemetry.NewCounterWithMetric(metaRestartsReadWithinUncertainty),
		RestartsTxnAborted:            telemetry.NewCounterWithMetric(metaRestartsTxnAborted),
		RestartsTxnPush:               telemetry.NewCounterWithMetric(metaRestartsTxnPush),
		RestartsUnknown:               telemetry.NewCounterWithMetric(metaRestartsUnknown),
	}
}
