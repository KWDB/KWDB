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

	"gitee.com/kwbasedb/kwbase/pkg/util/causer"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

const (
	raftSnapshotQueueName = "raftsnapshot"
	replicateQueueName    = "replicate"

	// maxQueueTimeoutBackoffShift caps queue process timeout at base * 2^3.
	maxQueueTimeoutBackoffShift = 3
)

func queueUsesTimeoutBackoff(queueName string) bool {
	return queueName == raftSnapshotQueueName || queueName == replicateQueueName
}

func isQueueTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	found := false
	causer.Visit(err, func(err error) bool {
		if _, ok := err.(*contextutil.TimeoutError); ok {
			found = true
			return true
		}
		if err == context.DeadlineExceeded {
			found = true
			return true
		}
		return false
	})
	return found
}

func applyQueueTimeoutBackoff(base time.Duration, failures uint32) time.Duration {
	if failures > maxQueueTimeoutBackoffShift {
		failures = maxQueueTimeoutBackoffShift
	}
	return base << failures
}

func (r *Replica) queueTimeoutFailuresLocked(queueName string) *uint32 {
	switch queueName {
	case raftSnapshotQueueName:
		return &r.mu.snapshotQueueTimeoutFailures
	case replicateQueueName:
		return &r.mu.replicateQueueTimeoutFailures
	default:
		return nil
	}
}

// processTimeoutWithQueueBackoff returns the process timeout for a queue,
// applying exponential backoff after consecutive timeouts (2^failures, max 2^3).
func (r *Replica) processTimeoutWithQueueBackoff(
	ctx context.Context, queueName string, baseTimeout time.Duration,
) time.Duration {
	if !queueUsesTimeoutBackoff(queueName) {
		return baseTimeout
	}
	r.mu.RLock()
	failuresPtr := r.queueTimeoutFailuresLocked(queueName)
	var failures uint32
	if failuresPtr != nil {
		failures = *failuresPtr
	}
	r.mu.RUnlock()
	timeout := applyQueueTimeoutBackoff(baseTimeout, failures)
	if failures > 0 {
		log.VEventf(ctx, 2,
			"r%d %s queue timeout backoff: base=%s failures=%d timeout=%s",
			r.RangeID, queueName, baseTimeout, failures, timeout)
	}
	return timeout
}

func (r *Replica) recordQueueProcessResult(queueName string, err error) {
	if !queueUsesTimeoutBackoff(queueName) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	failuresPtr := r.queueTimeoutFailuresLocked(queueName)
	if failuresPtr == nil {
		return
	}
	if err == nil {
		*failuresPtr = 0
		return
	}
	if !isQueueTimeoutError(err) {
		return
	}
	if *failuresPtr < maxQueueTimeoutBackoffShift {
		*failuresPtr++
	}
}
