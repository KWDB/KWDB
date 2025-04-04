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

package kvserver

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/grpcutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

var consistencyCheckInterval = settings.RegisterNonNegativeDurationSetting(
	"server.consistency_check.interval",
	"the time between range consistency checks; set to 0 to disable consistency checking."+
		" Note that intervals that are too short can negatively impact performance.",
	24*time.Hour,
)

var consistencyCheckRate = settings.RegisterPublicValidatedByteSizeSetting(
	"server.consistency_check.max_rate",
	"the rate limit (bytes/sec) to use for consistency checks; used in "+
		"conjunction with server.consistency_check.interval to control the "+
		"frequency of consistency checks. Note that setting this too high can "+
		"negatively impact performance.",
	8<<20, // 8MB
	validatePositive,
)

var testingAggressiveConsistencyChecks = envutil.EnvOrDefaultBool("KWBASE_CONSISTENCY_AGGRESSIVE", false)

type consistencyQueue struct {
	*baseQueue
	interval       func() time.Duration
	replicaCountFn func() int
}

// newConsistencyQueue returns a new instance of consistencyQueue.
func newConsistencyQueue(store *Store, gossip *gossip.Gossip) *consistencyQueue {
	q := &consistencyQueue{
		interval: func() time.Duration {
			return consistencyCheckInterval.Get(&store.ClusterSettings().SV)
		},
		replicaCountFn: store.ReplicaCount,
	}
	q.baseQueue = newBaseQueue(
		"consistencyChecker", q, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.ConsistencyQueueSuccesses,
			failures:             store.metrics.ConsistencyQueueFailures,
			pending:              store.metrics.ConsistencyQueuePending,
			processingNanos:      store.metrics.ConsistencyQueueProcessingNanos,
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(consistencyCheckRate),
		},
	)
	return q
}

func (q *consistencyQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ *config.SystemConfig,
) (bool, float64) {
	interval := q.interval()
	if interval <= 0 {
		return false, 0
	}

	shouldQ, priority := true, float64(0)
	if !repl.store.cfg.TestingKnobs.DisableLastProcessedCheck {
		lpTS, err := repl.getQueueLastProcessed(ctx, q.name)
		if err != nil {
			return false, 0
		}
		if shouldQ, priority = shouldQueueAgain(now, lpTS, interval); !shouldQ {
			return false, 0
		}
	}
	// Check if all replicas are live. Some tests run without a NodeLiveness configured.
	if repl.store.cfg.NodeLiveness != nil {
		for _, rep := range repl.Desc().Replicas().All() {
			if live, err := repl.store.cfg.NodeLiveness.IsLive(rep.NodeID); err != nil {
				log.VErrEventf(ctx, 3, "node %d liveness failed: %s", rep.NodeID, err)
				return false, 0
			} else if !live {
				return false, 0
			}
		}
	}
	return true, priority
}

// process() is called on every range for which this node is a lease holder.
func (q *consistencyQueue) process(
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) error {
	if q.interval() <= 0 {
		return nil
	}

	// Call setQueueLastProcessed because the consistency checker targets a much
	// longer cycle time than other queues. That it ignores errors is likely a
	// historical accident that should be revisited.
	if err := repl.setQueueLastProcessed(ctx, q.name, repl.store.Clock().Now()); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}

	req := roachpb.CheckConsistencyRequest{
		// Tell CheckConsistency that the caller is the queue. This triggers
		// code to handle inconsistencies by recomputing with a diff and
		// instructing the nodes in the minority to terminate with a fatal
		// error. It also triggers a stats readjustment if there is no
		// inconsistency but the persisted stats are found to disagree with
		// those reflected in the data. All of this really ought to be lifted
		// into the queue in the future.
		Mode: roachpb.ChecksumMode_CHECK_VIA_QUEUE,
	}
	resp, pErr := repl.CheckConsistency(ctx, req)
	if pErr != nil {
		var shouldQuiesce bool
		select {
		case <-repl.store.Stopper().ShouldQuiesce():
			shouldQuiesce = true
		default:
		}

		if !shouldQuiesce || !grpcutil.IsClosedConnection(pErr.GoError()) {
			// Suppress noisy errors about closed GRPC connections when the
			// server is quiescing.
			log.Error(ctx, pErr.GoError())
			return pErr.GoError()
		}
		return nil
	}
	if fn := repl.store.cfg.TestingKnobs.ConsistencyTestingKnobs.ConsistencyQueueResultHook; fn != nil {
		fn(resp)
	}
	return nil
}

func (q *consistencyQueue) timer(duration time.Duration) time.Duration {
	// An interval between replicas to space consistency checks out over
	// the check interval.
	replicaCount := q.replicaCountFn()
	if replicaCount == 0 {
		return 0
	}
	replInterval := q.interval() / time.Duration(replicaCount)
	if replInterval < duration {
		return 0
	}
	return replInterval - duration
}

// purgatoryChan returns nil.
func (*consistencyQueue) purgatoryChan() <-chan time.Time {
	return nil
}
