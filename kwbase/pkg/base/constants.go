// Copyright 2016 The Cockroach Authors.
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

package base

import "time"

const (
	// DefaultMaxClockOffset is the default maximum acceptable clock offset value.
	// On Azure, clock offsets between 250ms and 500ms are common. On AWS and GCE,
	// clock offsets generally stay below 250ms. See comments on Config.MaxOffset
	// for more on this setting.
	DefaultMaxClockOffset = 500 * time.Millisecond

	// DefaultTxnHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 5x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultTxnHeartbeatInterval = 1 * time.Second

	// SlowRequestThreshold is the amount of time to wait before considering a
	// request to be "slow".
	SlowRequestThreshold = 60 * time.Second

	// ChunkRaftCommandThresholdBytes is the threshold in bytes at which
	// to chunk or otherwise limit commands being sent to Raft.
	ChunkRaftCommandThresholdBytes = 256 * 1000

	// HeapProfileDir is the directory name where the heap profiler stores profiles
	// when there is a potential OOM situation.
	HeapProfileDir = "heap_profiler"

	// GoroutineDumpDir is the directory name where the goroutine dumper
	// stores dump when one of the dump heuristics is triggered.
	GoroutineDumpDir = "goroutine_dump"

	// MinRangeMaxBytes is the minimum value for range max bytes.
	MinRangeMaxBytes = 5 << 20 // 5 MB
)

const (
	// StartCmdName start name
	StartCmdName = "start"
	// StartSingleReplicaCmdName start single replica name
	StartSingleReplicaCmdName = "start-single-replica"
	// StartSingleNodeCmdName start single node name
	StartSingleNodeCmdName = "start-single-node"
)
