// Copyright 2018 The Cockroach Authors.
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

package ctpb

import (
	"fmt"
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
)

// Epoch is an int64 with its own type to avoid mix-ups in positional arguments.
type Epoch int64

// LAI is an int64 denoting a lease applied index with its own type to avoid
// mix-ups in positional arguments.
type LAI int64

// String formats Entry for human consumption as well as testing (by avoiding
// randomness in the output caused by map iteraton order).
func (e Entry) String() string {
	rangeIDs := make([]roachpb.RangeID, 0, len(e.MLAI))
	for k := range e.MLAI {
		rangeIDs = append(rangeIDs, k)
	}

	sort.Slice(rangeIDs, func(i, j int) bool {
		a, b := rangeIDs[i], rangeIDs[j]
		if a == b {
			return e.MLAI[a] < e.MLAI[b]
		}
		return a < b
	})
	sl := make([]string, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		sl = append(sl, fmt.Sprintf("r%d: %d", rangeID, e.MLAI[rangeID]))
	}
	if len(sl) == 0 {
		sl = []string{"(empty)"}
	}
	return fmt.Sprintf("CT: %s @ Epoch %d\nFull: %t\nMLAI: %s\n", e.ClosedTimestamp, e.Epoch, e.Full, strings.Join(sl, ", "))
}

func (r Reaction) String() string {
	return fmt.Sprintf("Refresh: %v", r.Requested)
}
