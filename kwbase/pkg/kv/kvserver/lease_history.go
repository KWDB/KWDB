// Copyright 2017 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// leaseHistoryMaxEntries controls if replica lease histories are enabled and
// how much memory they take up when enabled.
var leaseHistoryMaxEntries = envutil.EnvOrDefaultInt("KWBASE_LEASE_HISTORY", 5)

type leaseHistory struct {
	syncutil.Mutex
	index   int
	history []roachpb.Lease // A circular buffer with index.
}

func newLeaseHistory() *leaseHistory {
	lh := &leaseHistory{
		history: make([]roachpb.Lease, 0, leaseHistoryMaxEntries),
	}
	return lh
}

func (lh *leaseHistory) add(lease roachpb.Lease) {
	lh.Lock()
	defer lh.Unlock()

	// Not through the first pass through the buffer.
	if lh.index == len(lh.history) {
		lh.history = append(lh.history, lease)
	} else {
		lh.history[lh.index] = lease
	}
	lh.index++
	if lh.index >= leaseHistoryMaxEntries {
		lh.index = 0
	}
}

func (lh *leaseHistory) get() []roachpb.Lease {
	lh.Lock()
	defer lh.Unlock()
	if len(lh.history) == 0 {
		return nil
	}
	if len(lh.history) < leaseHistoryMaxEntries || lh.index == 0 {
		result := make([]roachpb.Lease, len(lh.history))
		copy(result, lh.history)
		return lh.history
	}
	first := lh.history[lh.index:]
	second := lh.history[:lh.index]
	result := make([]roachpb.Lease, len(first)+len(second))
	copy(result, first)
	copy(result[len(first):], second)
	return result
}
