// Copyright 2015 The Cockroach Authors.
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

package kvcoord

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func staticSize(size int64) func() int64 {
	return func() int64 {
		return size
	}
}

func TestLeaseHolderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	cacheSize := (1 << 4) * defaultShards
	lc := NewLeaseHolderCache(staticSize(int64(cacheSize)))
	if repStoreID, ok := lc.Lookup(ctx, 12); ok {
		t.Errorf("lookup of missing key returned: %d", repStoreID)
	}
	rangeID := roachpb.RangeID(5)
	replicaStoreID := roachpb.StoreID(1)
	lc.Update(ctx, rangeID, replicaStoreID)
	if repStoreID, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected StoreID %d", replicaStoreID)
	} else if repStoreID != replicaStoreID {
		t.Errorf("expected StoreID %d, got %d", replicaStoreID, repStoreID)
	}
	newReplicaStoreID := roachpb.StoreID(7)
	lc.Update(ctx, rangeID, newReplicaStoreID)
	if repStoreID, ok := lc.Lookup(ctx, rangeID); !ok {
		t.Fatalf("expected StoreID %d", replicaStoreID)
	} else if repStoreID != newReplicaStoreID {
		t.Errorf("expected StoreID %d, got %d", newReplicaStoreID, repStoreID)
	}

	lc.Update(ctx, rangeID, roachpb.StoreID(0))
	if repStoreID, ok := lc.Lookup(ctx, rangeID); ok {
		t.Errorf("lookup of evicted key returned: %d", repStoreID)
	}

	for i := 10; i < 10+cacheSize+2; i++ {
		lc.Update(ctx, roachpb.RangeID(i), replicaStoreID)
	}
	_, ok11 := lc.Lookup(ctx, 11)
	_, ok12 := lc.Lookup(ctx, 12)
	if ok11 || !ok12 {
		t.Fatalf("unexpected policy used in cache : %v, %v", ok11, ok12)
	}
}

func BenchmarkLeaseHolderCacheParallel(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.TODO()
	cacheSize := (1 << 4) * defaultShards
	lc := NewLeaseHolderCache(staticSize(int64(cacheSize)))
	numRanges := 2 * len(lc.shards)
	for i := 1; i <= numRanges; i++ {
		rangeID := roachpb.RangeID(i)
		lc.Update(ctx, rangeID, roachpb.StoreID(i))
	}
	b.RunParallel(func(pb *testing.PB) {
		var n int
		for pb.Next() {
			rangeID := roachpb.RangeID(n%numRanges + 1)
			n++
			if _, ok := lc.Lookup(ctx, rangeID); !ok {
				b.Fatalf("r%d: should be found in the cache", rangeID)
			}
		}
	})
}
