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

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

const (
	// forceTsCheckpointSizeThreshold triggers a TS checkpoint when a range raft
	// log exceeds this size but cannot be truncated yet (tsFlushedIndex lag).
	forceTsCheckpointSizeThreshold = 5 << 30 // 5 GiB
	// forceTsCheckpointMinInterval is the minimum time between force checkpoint
	// attempts on this store (dedupes concurrent raft log queue workers).
	forceTsCheckpointMinInterval = 15 * time.Second
)

type tsForceCheckpointGate struct {
	syncutil.Mutex
	inFlight    bool
	lastAttempt time.Time
}

// maybeForceTsCheckpoint asynchronously runs a node-wide TS checkpoint when raft
// log combine WAL is enabled and a range's raft log is large but not truncatable.
func (s *Store) maybeForceTsCheckpoint(
	ctx context.Context, rangeID roachpb.RangeID, logSize int64,
) {
	if logSize < forceTsCheckpointSizeThreshold || s.TsEngine == nil || s.stopper == nil {
		return
	}
	if !tse.TsRaftLogCombineWAL.Get(&s.ClusterSettings().SV) {
		return
	}

	s.tsForceCheckpointGate.Lock()
	if s.tsForceCheckpointGate.inFlight {
		s.tsForceCheckpointGate.Unlock()
		log.VEventf(ctx, 2, "skip force ts checkpoint for r%d: already in flight", rangeID)
		return
	}
	if since := timeutil.Since(s.tsForceCheckpointGate.lastAttempt); since < forceTsCheckpointMinInterval {
		s.tsForceCheckpointGate.Unlock()
		log.VEventf(ctx, 2, "skip force ts checkpoint for r%d: min interval (%s remaining)",
			rangeID, forceTsCheckpointMinInterval-since)
		return
	}
	s.tsForceCheckpointGate.inFlight = true
	s.tsForceCheckpointGate.lastAttempt = timeutil.Now()
	s.tsForceCheckpointGate.Unlock()

	if err := s.stopper.RunAsyncTask(ctx, "force-ts-checkpoint", func(ctx context.Context) {
		defer func() {
			s.tsForceCheckpointGate.Lock()
			s.tsForceCheckpointGate.inFlight = false
			s.tsForceCheckpointGate.Unlock()
		}()
		if err := s.TsEngine.Checkpoint(); err != nil {
			log.Warningf(ctx, "force ts checkpoint failed, triggered by r%d: %v", rangeID, err)
			return
		}
		log.Infof(ctx, "force ts checkpoint completed, triggered by r%d, logSize=%s",
			rangeID, humanizeutil.IBytes(logSize))
	}); err != nil {
		s.tsForceCheckpointGate.Lock()
		s.tsForceCheckpointGate.inFlight = false
		s.tsForceCheckpointGate.Unlock()
		log.Warningf(ctx, "failed to start force ts checkpoint for r%d: %v", rangeID, err)
	}
}
