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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestReplicaGCShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(t time.Duration) hlc.Timestamp {
		return hlc.Timestamp{WallTime: t.Nanoseconds()}
	}

	base := 2 * (ReplicaGCQueueSuspectTimeout + ReplicaGCQueueInactivityThreshold)
	var (
		z       = ts(0)
		bTS     = ts(base)
		cTS     = ts(base + ReplicaGCQueueSuspectTimeout)
		cTSnext = ts(base + ReplicaGCQueueSuspectTimeout + 1)
		iTSprev = ts(base + ReplicaGCQueueInactivityThreshold - 1)
		iTS     = ts(base + ReplicaGCQueueInactivityThreshold)
	)

	for i, test := range []struct {
		now, lastCheck, lastActivity hlc.Timestamp
		isCandidate                  bool

		shouldQ  bool
		priority float64
	}{
		// Test outcomes when range is in candidate state.

		// All timestamps current: candidacy plays no role.
		{now: z, lastCheck: z, lastActivity: z, isCandidate: true, shouldQ: false, priority: 0},
		// Threshold: no action taken.
		{now: cTS, lastCheck: z, lastActivity: bTS, isCandidate: true, shouldQ: false, priority: 0},
		// Queue with priority.
		{now: cTSnext, lastCheck: z, lastActivity: bTS, isCandidate: true, shouldQ: true, priority: 1},
		// Last processed recently: candidate still gets processed eagerly.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: true, shouldQ: true, priority: 1},
		// Last processed recently: non-candidate stays put.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: false, priority: 0},
		// Still no effect until iTS reached.
		{now: iTSprev, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: false, priority: 0},
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: true, shouldQ: true, priority: 1},
		// Verify again that candidacy increases priority.
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: true, priority: 0},
	} {
		if sq, pr := replicaGCShouldQueueImpl(
			test.now, test.lastCheck, test.lastActivity, test.isCandidate,
		); sq != test.shouldQ || pr != test.priority {
			t.Errorf("%d: %+v: got (%t,%f)", i, test, sq, pr)
		}
	}
}
