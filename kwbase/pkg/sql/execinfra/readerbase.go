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

package execinfra

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// We ignore any limits that are higher than this value to avoid integer
// overflows. See limitHint for how this bound is used.
const readerOverflowProtection = 1000000000000000 /* 10^15 */

// LimitHint returns the limit hint to set for a KVFetcher based on
// the spec's limit hint and the PostProcessSpec.
func LimitHint(specLimitHint int64, post *execinfrapb.PostProcessSpec) (limitHint int64) {
	// We prioritize the post process's limit since ProcOutputHelper
	// will tell us to stop once we emit enough rows.
	if post.Limit != 0 && post.Limit <= readerOverflowProtection {
		limitHint = int64(post.Limit)
	} else if specLimitHint != 0 && specLimitHint <= readerOverflowProtection {
		// If it turns out that limiHint rows are sufficient for our consumer, we
		// want to avoid asking for another batch. Currently, the only way for us to
		// "stop" is if we block on sending rows and the consumer sets
		// ConsumerDone() on the RowChannel while we block. So we want to block
		// *after* sending all the rows in the limit hint; to do this, we request
		// rowChannelBufSize + 1 more rows:
		//  - rowChannelBufSize rows guarantee that we will fill the row channel
		//    even after limitHint rows are consumed
		//  - the extra row gives us chance to call Push again after we unblock,
		//    which will notice that ConsumerDone() was called.
		//
		// This flimsy mechanism is only useful in the (optimistic) case that the
		// processor that only needs this many rows is our direct, local consumer.
		// If we have a chain of processors and RowChannels, or remote streams, this
		// reasoning goes out the door.
		//
		// TODO(radu, andrei): work on a real mechanism for limits.
		limitHint = specLimitHint + RowChannelBufSize + 1
	}

	if !post.Filter.Empty() {
		// We have a filter so we will likely need to read more rows.
		limitHint *= 2
	}

	return limitHint
}

// MisplannedRanges filters out the misplanned ranges and their RangeInfo for a
// given node.
func MisplannedRanges(
	ctx context.Context, rangeInfos []roachpb.RangeInfo, nodeID roachpb.NodeID,
) (misplannedRanges []roachpb.RangeInfo) {
	for _, ri := range rangeInfos {
		if ri.Lease.Replica.NodeID != nodeID {
			misplannedRanges = append(misplannedRanges, ri)
		}
	}

	if len(misplannedRanges) != 0 {
		var msg string
		if len(misplannedRanges) < 3 {
			msg = fmt.Sprintf("%+v", misplannedRanges[0].Desc)
		} else {
			msg = fmt.Sprintf("%+v...", misplannedRanges[:3])
		}
		log.VEventf(ctx, 2, "tableReader pushing metadata about misplanned ranges: %s",
			msg)
	}

	return misplannedRanges
}
