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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestApplyQueueTimeoutBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	base := 5 * time.Minute
	for _, tc := range []struct {
		name     string
		failures uint32
		want     time.Duration
	}{
		{"no failures", 0, base},
		{"first timeout doubles", 1, 10 * time.Minute},
		{"second timeout quadruples", 2, 20 * time.Minute},
		{"third timeout octuples", 3, 40 * time.Minute},
		{"failures capped at shift 3", 4, 40 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, applyQueueTimeoutBackoff(base, tc.failures))
		})
	}
}

func TestQueueUsesTimeoutBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.True(t, queueUsesTimeoutBackoff(raftSnapshotQueueName))
	require.True(t, queueUsesTimeoutBackoff(replicateQueueName))
	require.False(t, queueUsesTimeoutBackoff("merge"))
}

func TestIsQueueTimeoutError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.False(t, isQueueTimeoutError(nil))
	require.True(t, isQueueTimeoutError(context.DeadlineExceeded))
	require.True(t, isQueueTimeoutError(errors.Wrap(context.DeadlineExceeded, "wrapped")))
	require.False(t, isQueueTimeoutError(errors.New("other error")))
}

func TestRecordQueueProcessResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeoutErr := context.DeadlineExceeded

	for _, tc := range []struct {
		name        string
		queueName   string
		initialSnap uint32
		initialRepl uint32
		err         error
		wantSnap    uint32
		wantRepl    uint32
	}{
		{
			name:        "success resets snapshot failures",
			queueName:   raftSnapshotQueueName,
			initialSnap: 2,
			err:         nil,
			wantSnap:    0,
		},
		{
			name:        "timeout increments snapshot failures",
			queueName:   raftSnapshotQueueName,
			initialSnap: 0,
			err:         timeoutErr,
			wantSnap:    1,
		},
		{
			name:        "non-timeout error leaves failures unchanged",
			queueName:   raftSnapshotQueueName,
			initialSnap: 1,
			err:         errors.New("snapshot failed"),
			wantSnap:    1,
		},
		{
			name:        "failures capped at max shift",
			queueName:   raftSnapshotQueueName,
			initialSnap: maxQueueTimeoutBackoffShift,
			err:         timeoutErr,
			wantSnap:    maxQueueTimeoutBackoffShift,
		},
		{
			name:        "replicate queue uses separate counter",
			queueName:   replicateQueueName,
			initialSnap: 2,
			initialRepl: 0,
			err:         timeoutErr,
			wantSnap:    2,
			wantRepl:    1,
		},
		{
			name:        "unknown queue is noop",
			queueName:   "merge",
			initialSnap: 1,
			err:         timeoutErr,
			wantSnap:    1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r Replica
			r.mu.snapshotQueueTimeoutFailures = tc.initialSnap
			r.mu.replicateQueueTimeoutFailures = tc.initialRepl

			r.recordQueueProcessResult(tc.queueName, tc.err)

			require.Equal(t, tc.wantSnap, r.mu.snapshotQueueTimeoutFailures)
			require.Equal(t, tc.wantRepl, r.mu.replicateQueueTimeoutFailures)
		})
	}
}

func TestProcessTimeoutWithQueueBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	base := 5 * time.Minute

	for _, tc := range []struct {
		name      string
		queueName string
		failures  uint32
		want      time.Duration
	}{
		{
			name:      "non-backoff queue returns base timeout",
			queueName: "merge",
			failures:  2,
			want:      base,
		},
		{
			name:      "snapshot queue applies exponential backoff",
			queueName: raftSnapshotQueueName,
			failures:  2,
			want:      20 * time.Minute,
		},
		{
			name:      "replicate queue applies exponential backoff",
			queueName: replicateQueueName,
			failures:  1,
			want:      10 * time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r Replica
			switch tc.queueName {
			case raftSnapshotQueueName:
				r.mu.snapshotQueueTimeoutFailures = tc.failures
			case replicateQueueName:
				r.mu.replicateQueueTimeoutFailures = tc.failures
			}

			got := r.processTimeoutWithQueueBackoff(ctx, tc.queueName, base)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMakeRateLimitedTimeoutFuncWithQueueBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	queueGuaranteedProcessingTimeBudget.Override(&st.SV, time.Minute)
	recoverySnapshotRate.Override(&st.SV, 1<<30)

	var r Replica
	r.mu.snapshotQueueTimeoutFailures = 2
	repl := mvccStatsReplicaInQueue{size: 1 << 20}

	baseTimeout := makeRateLimitedTimeoutFunc(recoverySnapshotRate)(context.Background(), st, repl)
	backoffTimeout := r.processTimeoutWithQueueBackoff(
		context.Background(), raftSnapshotQueueName, baseTimeout,
	)
	require.Equal(t, 4*baseTimeout, backoffTimeout)
}
