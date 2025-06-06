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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"github.com/google/btree"
)

// ReplicaPlaceholder represents a "lock" of a part of the keyspace on a given
// *Store for the application of a (preemptive or Raft) snapshot. Placeholders
// are kept synchronously in two places in (*Store).mu, namely the
// replicaPlaceholders and replicaByKey maps, and exist only while the Raft
// scheduler tries to apply raft.Ready containing a snapshot to some Replica.
//
// To see why placeholders are necessary, consider the case in which two
// snapshots arrive at a Store, one for r1 and bounds [a,c) and the other for r2
// and [b,c), and assume that the keyspace [a,c) is not associated to any
// Replica on the receiving Store. This situation can occur because even though
// "logically" the keyspace always shards cleanly into replicas, incoming
// snapshots don't always originate from a mutually consistent version of this
// sharding. For example, a range Q might split, creating a range R, but some
// Store might be receiving a snapshot of Q before the split as well as a
// replica of R (which postdates the split). Similar examples are possible with
// merges as well as with arbitrarily complicated combinations of multiple
// merges and splits.
//
// Without placeholders, the following interleaving of two concurrent Raft
// scheduler goroutines g1 and g2 is possible for the above example:
//
// - g1: new raft.Ready for r1 wants to apply snapshot
// - g1: check for conflicts with existing replicas: none found; [a,c) is empty
// - g2: new raft.Ready for r2 wants to apply snapshot
// - g2: check for conflicts with existing replicas: none found; [b,c) is empty
// - g2: apply snapshot: writes replica for r2 to [b,c)
// - g2: done
// - g1: apply snapshot: writes replica for r1 to [a,c)
// - boom: we now have two replicas on this store that overlap
//
// Placeholders avoid this problem because they provide a serialization point
// between g1 and g2: When g1 checks for conflicts, it also checks for an
// existing placeholder (inserting its own atomically when none found), so that
// g2 would later fail the overlap check on g1's placeholder.
//
// Placeholders are removed by the goroutine that inserted them at the end of
// the respective Raft cycle, so they usually live only for as long as it takes
// to write the snapshot to disk. See (*Store).processRaftSnapshotRequest for
// details.
type ReplicaPlaceholder struct {
	rangeDesc roachpb.RangeDescriptor
}

var _ KeyRange = &ReplicaPlaceholder{}

// Desc returns the range Placeholder's descriptor.
func (r *ReplicaPlaceholder) Desc() *roachpb.RangeDescriptor {
	return &r.rangeDesc
}

func (r *ReplicaPlaceholder) startKey() roachpb.RKey {
	return r.Desc().StartKey
}

// Less implements the btree.Item interface.
func (r *ReplicaPlaceholder) Less(i btree.Item) bool {
	return r.Desc().StartKey.Less(i.(rangeKeyItem).startKey())
}

func (r *ReplicaPlaceholder) String() string {
	return fmt.Sprintf("range=%d [%s-%s) (placeholder)",
		r.Desc().RangeID, r.rangeDesc.StartKey, r.rangeDesc.EndKey)
}
