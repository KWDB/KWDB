// Copyright 2015 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
package kvserver

// TODO(kang): The background pre-distribution function is disabled.
//             Turn off unit testing and turn it on again when the
//             function is enabled.
//import (
//	"context"
//	"fmt"
//	//"fmt"
//	//"math"
//	//"os"
//	"reflect"
//	"sort"
//	"strconv"
//	//"strconv"
//	//"sync"
//	"testing"
//	"time"
//
//	"github.com/gogo/protobuf/proto"
//	//"gitee.com/kwbasedb/kwbase/pkg/base"
//	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
//	"gitee.com/kwbasedb/kwbase/pkg/gossip"
//	"gitee.com/kwbasedb/kwbase/pkg/keys"
//	//"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/constraint"
//	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
//	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
//	//"gitee.com/kwbasedb/kwbase/pkg/rpc"
//	//"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
//	//"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
//	//"gitee.com/kwbasedb/kwbase/pkg/testutils"
//	"gitee.com/kwbasedb/kwbase/pkg/testutils/gossiputil"
//	//"gitee.com/kwbasedb/kwbase/pkg/util"
//	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
//	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
//	"gitee.com/kwbasedb/kwbase/pkg/util/log"
//	//"gitee.com/kwbasedb/kwbase/pkg/util/metric"
//	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
//	//"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
//	//"github.com/olekukonko/tablewriter"
//	//"github.com/pkg/errors"
//	"github.com/stretchr/testify/require"
//)
//
//const firstTsRangeID = roachpb.RangeID(1)
//
//var TsRange = roachpb.TS_RANGE
//
//var simpleTsZoneConfig = zonepb.ZoneConfig{
//	NumReplicas: proto.Int32(1),
//	Constraints: []zonepb.Constraints{
//		{
//			Constraints: []zonepb.Constraint{
//				{Value: "a", Type: zonepb.Constraint_REQUIRED},
//				{Value: "ssd", Type: zonepb.Constraint_REQUIRED},
//			},
//		},
//	},
//}
//
//var threeReplicaTsZoneConfigLeasePreferences = zonepb.ZoneConfig{
//	NumReplicas: proto.Int32(3),
//	LeasePreferences: []zonepb.LeasePreference{
//		{
//			Constraints: []zonepb.Constraint{
//				{
//					Type: zonepb.Constraint_REQUIRED,
//				},
//			},
//		},
//	},
//}
//
//var emptyConstraintsTsZoneConfig = zonepb.ZoneConfig{
//	NumReplicas: proto.Int32(1),
//}
//
//var multiDCTsConfig = zonepb.ZoneConfig{
//	NumReplicas: proto.Int32(2),
//	Constraints: []zonepb.Constraints{
//		{Constraints: []zonepb.Constraint{{Value: "ssd", Type: zonepb.Constraint_REQUIRED}}},
//	},
//}
//
//var singleTsStore = []*roachpb.StoreDescriptor{
//	{
//		StoreID: 1,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 1,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//}
//
//var tsStore = []*roachpb.StoreDescriptor{
//	{
//		StoreID: 1,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 1,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 2,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 2,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 3,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 3,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//}
//
//var sameDCTsStores = []*roachpb.StoreDescriptor{
//	{
//		StoreID: 1,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 1,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 2,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 2,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 3,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 3,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 4,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 4,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 5,
//		Attrs:   roachpb.Attributes{Attrs: []string{"mem"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 5,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//}
//
//var multiDCTsStores = []*roachpb.StoreDescriptor{
//	{
//		StoreID: 1,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 1,
//			Attrs:  roachpb.Attributes{Attrs: []string{"a"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//	{
//		StoreID: 2,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 2,
//			Attrs:  roachpb.Attributes{Attrs: []string{"b"}},
//		},
//		Capacity: roachpb.StoreCapacity{
//			Capacity:     200,
//			Available:    100,
//			LogicalBytes: 100,
//		},
//	},
//}
//
//var multiDiversityDCTsStores = []*roachpb.StoreDescriptor{
//	{
//		StoreID: 1,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 1,
//			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "a"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 2,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 2,
//			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "a"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 3,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 3,
//			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "b"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 4,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 4,
//			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "b"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 5,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 5,
//			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "c"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 6,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 6,
//			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "c"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 7,
//		Attrs:   roachpb.Attributes{Attrs: []string{"ssd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 7,
//			Attrs:  roachpb.Attributes{Attrs: []string{"odd"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "d"},
//				},
//			},
//		},
//	},
//	{
//		StoreID: 8,
//		Attrs:   roachpb.Attributes{Attrs: []string{"hdd"}},
//		Node: roachpb.NodeDescriptor{
//			NodeID: 8,
//			Attrs:  roachpb.Attributes{Attrs: []string{"even"}},
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{
//					{Key: "datacenter", Value: "d"},
//				},
//			},
//		},
//	},
//}
//
//var preDistributionRangeDesc = roachpb.RangeDescriptor{
//	RangeID:   firstTsRangeID,
//	RangeType: &TsRange,
//	PreDistLeaseHolder: roachpb.ReplicaDescriptor{
//		NodeID:  1,
//		StoreID: 1,
//	},
//	PreDist: []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  1,
//			StoreID: 1,
//		},
//		{
//			NodeID:  2,
//			StoreID: 2,
//		},
//		{
//			NodeID:  3,
//			StoreID: 3,
//		},
//	},
//}
//
//func tsReplicas(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
//	res := make([]roachpb.ReplicaDescriptor, len(storeIDs))
//	for i, storeID := range storeIDs {
//		res[i].NodeID = roachpb.NodeID(storeID)
//		res[i].StoreID = storeID
//		res[i].ReplicaID = roachpb.ReplicaID(i + 1)
//	}
//	return res
//}
//
//// createTestAllocator creates a stopper, gossip, store pool and allocator for
//// use in tests. Stopper must be stopped by the caller.
//func createTestAllocatorTs(
//	numNodes int, deterministic bool,
//) (*stop.Stopper, *gossip.Gossip, *StorePool, Allocator, *hlc.ManualClock) {
//	stopper, g, manual, storePool, _ := createTestStorePool(
//		TestTimeUntilStoreDeadOff, deterministic,
//		func() int { return numNodes },
//		storagepb.NodeLivenessStatus_LIVE)
//	a := MakeAllocator(storePool, func(string) (time.Duration, bool) {
//		return 0, true
//	})
//	return stopper, g, storePool, a, manual
//}
//
//// mockStorePool sets up a collection of a alive and dead stores in the store
//// pool for testing purposes.
//func mockTsStorePool(
//	storePool *StorePool,
//	aliveStoreIDs []roachpb.StoreID,
//	unavailableStoreIDs []roachpb.StoreID,
//	deadStoreIDs []roachpb.StoreID,
//	decommissioningStoreIDs []roachpb.StoreID,
//	decommissionedStoreIDs []roachpb.StoreID,
//) {
//	storePool.detailsMu.Lock()
//	defer storePool.detailsMu.Unlock()
//
//	liveNodeSet := map[roachpb.NodeID]storagepb.NodeLivenessStatus{}
//	storePool.detailsMu.storeDetails = map[roachpb.StoreID]*storeDetail{}
//	for _, storeID := range aliveStoreIDs {
//		liveNodeSet[roachpb.NodeID(storeID)] = storagepb.NodeLivenessStatus_LIVE
//		detail := storePool.getStoreDetailLocked(storeID)
//		detail.desc = &roachpb.StoreDescriptor{
//			StoreID: storeID,
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
//		}
//	}
//	for _, storeID := range unavailableStoreIDs {
//		liveNodeSet[roachpb.NodeID(storeID)] = storagepb.NodeLivenessStatus_UNAVAILABLE
//		detail := storePool.getStoreDetailLocked(storeID)
//		detail.desc = &roachpb.StoreDescriptor{
//			StoreID: storeID,
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
//		}
//	}
//	for _, storeID := range deadStoreIDs {
//		liveNodeSet[roachpb.NodeID(storeID)] = storagepb.NodeLivenessStatus_DEAD
//		detail := storePool.getStoreDetailLocked(storeID)
//		detail.desc = &roachpb.StoreDescriptor{
//			StoreID: storeID,
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
//		}
//	}
//	for _, storeID := range decommissioningStoreIDs {
//		liveNodeSet[roachpb.NodeID(storeID)] = storagepb.NodeLivenessStatus_DECOMMISSIONING
//		detail := storePool.getStoreDetailLocked(storeID)
//		detail.desc = &roachpb.StoreDescriptor{
//			StoreID: storeID,
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
//		}
//	}
//	for _, storeID := range decommissionedStoreIDs {
//		liveNodeSet[roachpb.NodeID(storeID)] = storagepb.NodeLivenessStatus_DECOMMISSIONED
//		detail := storePool.getStoreDetailLocked(storeID)
//		detail.desc = &roachpb.StoreDescriptor{
//			StoreID: storeID,
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
//		}
//	}
//
//	// Set the node liveness function using the set we constructed.
//	storePool.nodeLivenessFn =
//		func(nodeID roachpb.NodeID, now time.Time, threshold time.Duration) storagepb.NodeLivenessStatus {
//			if status, ok := liveNodeSet[nodeID]; ok {
//				return status
//			}
//			return storagepb.NodeLivenessStatus_UNAVAILABLE
//		}
//}
//
//var preDistributionSixReplicaRangeDesc = roachpb.RangeDescriptor{
//	RangeID:   firstTsRangeID,
//	RangeType: &TsRange,
//	PreDistLeaseHolder: roachpb.ReplicaDescriptor{
//		NodeID:  1,
//		StoreID: 1,
//	},
//	PreDist: []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  1,
//			StoreID: 1,
//		},
//		{
//			NodeID:  2,
//			StoreID: 2,
//		},
//		{
//			NodeID:  3,
//			StoreID: 3,
//		},
//		{
//			NodeID:  4,
//			StoreID: 5,
//		},
//		{},
//	},
//}
//
//func preDistributionRangeDescWithNumReplica(numReplica int) roachpb.RangeDescriptor {
//	var preDistributionNumReplicaRangeDesc = roachpb.RangeDescriptor{
//		RangeID:   firstTsRangeID,
//		RangeType: &TsRange,
//	}
//	var replicas []roachpb.ReplicaDescriptor
//	for i := 1; i <= numReplica; i++ {
//		replicas = append(replicas, roachpb.ReplicaDescriptor{
//			NodeID:  roachpb.NodeID(i),
//			StoreID: roachpb.StoreID(i),
//		})
//		if i == 1 {
//			preDistributionNumReplicaRangeDesc.SetPreDistLeaseHolder(replicas[0])
//		}
//	}
//	preDistributionNumReplicaRangeDesc.SetPreDistReplicas(roachpb.MakeReplicaDescriptors(replicas))
//	return preDistributionNumReplicaRangeDesc
//}
//
//func makeTsDescriptor(storeList []roachpb.StoreID) roachpb.RangeDescriptor {
//	desc := roachpb.RangeDescriptor{
//		RangeType: &TsRange,
//		EndKey:    roachpb.RKey(keys.SystemPrefix),
//	}
//
//	desc.InternalReplicas = make([]roachpb.ReplicaDescriptor, len(storeList))
//
//	for i, node := range storeList {
//		desc.InternalReplicas[i] = roachpb.ReplicaDescriptor{
//			StoreID:   node,
//			NodeID:    roachpb.NodeID(node),
//			ReplicaID: roachpb.ReplicaID(node),
//		}
//	}
//
//	return desc
//}
//
//// AllocateTsTarget
//func TestAllocatorTsSimpleRetrievalWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(1, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	gossiputil.NewStoreGossiper(g).GossipStores(singleTsStore, t)
//	result, _, err := a.AllocateTsTarget(
//		context.Background(),
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&simpleTsZoneConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	if result.Node.NodeID != 1 || result.StoreID != 1 {
//		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
//	}
//}
//
//// AllocateTsTarget
//func TestAllocatorTsSimpleRetrievalWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(1, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	gossiputil.NewStoreGossiper(g).GossipStores(singleTsStore, t)
//	internalReplicas := []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  11,
//			StoreID: 11,
//		},
//	}
//	preDistributionRangeDesc.InternalReplicas = internalReplicas
//	//1 Test whether the pre-distributed leaseHolder Node has a replica
//	result, _, err := a.AllocateTsTarget(
//		context.Background(),
//		&preDistributionRangeDesc,
//		&emptyConstraintsTsZoneConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	if result.Node.NodeID != 1 || result.StoreID != 1 {
//		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
//	}
//
//	gossiputil.NewStoreGossiper(g).GossipStores(tsStore, t)
//	internalReplicas = []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  1,
//			StoreID: 1,
//		},
//	}
//	preDistributionRangeDesc.InternalReplicas = internalReplicas
//	//2 Test whether the pre-distributed Node has a copy
//	result, _, err = a.AllocateTsTarget(
//		context.Background(),
//		&preDistributionRangeDesc,
//		&emptyConstraintsTsZoneConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	if result.Node.NodeID != 2 || result.StoreID != 2 {
//		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
//	}
//
//	internalReplicas = []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  1,
//			StoreID: 1,
//		},
//		{
//			NodeID:  2,
//			StoreID: 2,
//		},
//		{
//			NodeID:  3,
//			StoreID: 3,
//		},
//	}
//	preDistributionRangeDesc.InternalReplicas = internalReplicas
//	//3 Test that all pre-distributed Nodes have replicas
//	result, _, err = a.AllocateTsTarget(
//		context.Background(),
//		&preDistributionRangeDesc,
//		&emptyConstraintsTsZoneConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//
//	if result != nil {
//		t.Errorf("expected nil: %+v", result)
//	}
//}
//
//// AllocateTsTarget
//func TestAllocatorTsNoAvailableDisksWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, _, _, a, _ := createTestAllocatorTs(1, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	result, _, err := a.AllocateTsTarget(
//		context.Background(),
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&simpleTsZoneConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//	if result != nil {
//		t.Errorf("expected nil result: %+v", result)
//	}
//	if err == nil {
//		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
//	}
//}
//
//// AllocateTsTarget
//func TestAllocatorTsTwoDatacentersWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(1, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	gossiputil.NewStoreGossiper(g).GossipStores(multiDCTsStores, t)
//	ctx := context.Background()
//	result1, _, err := a.AllocateTsTarget(
//		ctx,
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&multiDCTsConfig,
//		[]roachpb.ReplicaDescriptor{},
//	)
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	result2, _, err := a.AllocateTsTarget(
//		ctx,
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&multiDCTsConfig,
//		[]roachpb.ReplicaDescriptor{{
//			NodeID:  result1.Node.NodeID,
//			StoreID: result1.StoreID,
//		}},
//	)
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	ids := []int{int(result1.Node.NodeID), int(result2.Node.NodeID)}
//	sort.Ints(ids)
//	if expected := []int{1, 2}; !reflect.DeepEqual(ids, expected) {
//		t.Errorf("Expected nodes %+v: %+v vs %+v", expected, result1.Node, result2.Node)
//	}
//	// Verify that no result is forthcoming if we already have a replica.
//	result3, _, err := a.AllocateTsTarget(
//		ctx,
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&multiDCTsConfig,
//		[]roachpb.ReplicaDescriptor{
//			{
//				NodeID:  result1.Node.NodeID,
//				StoreID: result1.StoreID,
//			},
//			{
//				NodeID:  result2.Node.NodeID,
//				StoreID: result2.StoreID,
//			},
//		},
//	)
//	if err == nil {
//		t.Errorf("expected error on allocation without available stores: %+v", result3)
//	}
//}
//
//// AllocateTsTarget
//func TestAllocatorTsExistingReplicaWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(1, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	gossiputil.NewStoreGossiper(g).GossipStores(sameDCTsStores, t)
//	result, _, err := a.AllocateTsTarget(
//		context.Background(),
//		&roachpb.RangeDescriptor{RangeID: firstTsRangeID},
//		&zonepb.ZoneConfig{
//			NumReplicas: proto.Int32(0),
//			Constraints: []zonepb.Constraints{
//				{
//					Constraints: []zonepb.Constraint{
//						{Value: "a", Type: zonepb.Constraint_REQUIRED},
//						{Value: "hdd", Type: zonepb.Constraint_REQUIRED},
//					},
//				},
//			},
//		},
//		[]roachpb.ReplicaDescriptor{
//			{
//				NodeID:  2,
//				StoreID: 2,
//			},
//		},
//	)
//	if err != nil {
//		t.Fatalf("Unable to perform allocation: %+v", err)
//	}
//	if !(result.StoreID == 3 || result.StoreID == 4) {
//		t.Errorf("expected result to have store ID 3 or 4: %+v", result)
//	}
//}
//
//// RebalanceTsTarget
//func TestAllocatorTsRebalanceWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stores := []*roachpb.StoreDescriptor{
//		{
//			StoreID: 1,
//			Node:    roachpb.NodeDescriptor{NodeID: 1},
//		},
//		{
//			StoreID: 2,
//			Node:    roachpb.NodeDescriptor{NodeID: 2},
//		},
//		{
//			StoreID: 3,
//			Node:    roachpb.NodeDescriptor{NodeID: 3},
//		},
//		{
//			StoreID: 4,
//			Node:    roachpb.NodeDescriptor{NodeID: 4},
//		},
//		{
//			StoreID: 5,
//			Node:    roachpb.NodeDescriptor{NodeID: 5},
//		},
//	}
//
//	stopper, g, _, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//
//	gossiputil.NewStoreGossiper(g).GossipStores(stores, t)
//	ctx := context.Background()
//
//	repl := &Replica{
//		RangeID: firstTsRangeID,
//		store: &Store{
//			nodeDesc: &roachpb.NodeDescriptor{
//				NodeID: 3,
//			},
//		},
//	}
//	for i := 3; i <= 5; i++ {
//		replicas := []roachpb.ReplicaDescriptor{{
//			NodeID:  roachpb.NodeID(i),
//			StoreID: roachpb.StoreID(i),
//		}}
//		preDistributionRangeDesc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
//	}
//	repl.mu.state.Desc = &preDistributionRangeDesc
//
//	// pre distribution: {node: 1, node: 2, node: 3}
//	// current distribution: {node: 3, node: 4, node: 5}
//	// add replica: node 1
//	// remove replica: node 5
//	var rangeUsageInfo RangeUsageInfo
//	target, removeTarget, _, ok := a.RebalanceTsTarget(
//		ctx,
//		zonepb.EmptyCompleteZoneConfig(),
//		repl,
//		[]roachpb.ReplicaDescriptor{{NodeID: 3, StoreID: 3}, {NodeID: 4, StoreID: 4}, {NodeID: 5, StoreID: 5}},
//		rangeUsageInfo,
//		storeFilterThrottled,
//	)
//
//	if !ok {
//		t.Fatalf("unable to get rebalance store")
//	}
//
//	if !(target.NodeID == 1 && (removeTarget.NodeID == 5 || removeTarget.NodeID == 4)) {
//		t.Errorf("expected store 1-add or 4/5-remove; got %d-add, %d-remove", target.NodeID, removeTarget.NodeID)
//	}
//
//	// pre distribution: {node: 1, node: 2, node: 3}
//	// current distribution: {node: 1, node: 3, node: 4}
//	// add replica: node 2
//	// remove replica: node 4
//	replicas := []roachpb.ReplicaDescriptor{
//		{
//			NodeID:  roachpb.NodeID(1),
//			StoreID: roachpb.StoreID(1),
//		},
//		{
//			NodeID:  roachpb.NodeID(3),
//			StoreID: roachpb.StoreID(3),
//		},
//		{
//			NodeID:  roachpb.NodeID(4),
//			StoreID: roachpb.StoreID(4),
//		},
//	}
//	preDistributionRangeDesc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
//	repl.mu.state.Desc = &preDistributionRangeDesc
//
//	var needRemoveID = 5
//	if removeTarget.NodeID == 5 {
//		needRemoveID = 4
//	}
//
//	target, removeTarget, _, _ = a.RebalanceTsTarget(
//		ctx,
//		zonepb.EmptyCompleteZoneConfig(),
//		repl,
//		[]roachpb.ReplicaDescriptor{
//			{NodeID: 1, StoreID: 1},
//			{NodeID: 3, StoreID: 3},
//			{NodeID: roachpb.NodeID(needRemoveID), StoreID: roachpb.StoreID(needRemoveID)}},
//		rangeUsageInfo,
//		storeFilterThrottled,
//	)
//
//	if !ok {
//		t.Fatalf("unable to get rebalance store")
//	}
//
//	if !(target.NodeID == 2 && (removeTarget.NodeID == roachpb.NodeID(needRemoveID))) {
//		t.Errorf("expected store 2-add or %d-remove; got %d-add, %d-remove", target.NodeID, needRemoveID, removeTarget.NodeID)
//	}
//
//	// pre distribution: {node: 1, node: 2, node: 3}
//	// current distribution: {node: 1, node: 2, node: 3}
//	replicas = []roachpb.ReplicaDescriptor{{
//		NodeID:  roachpb.NodeID(1),
//		StoreID: roachpb.StoreID(1),
//	},
//		{
//			NodeID:  roachpb.NodeID(2),
//			StoreID: roachpb.StoreID(2),
//		},
//		{
//			NodeID:  roachpb.NodeID(3),
//			StoreID: roachpb.StoreID(3),
//		}}
//	preDistributionRangeDesc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
//	repl.mu.state.Desc = &preDistributionRangeDesc
//
//	target, removeTarget, _, _ = a.RebalanceTsTarget(
//		ctx,
//		zonepb.EmptyCompleteZoneConfig(),
//		repl,
//		[]roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1}, {NodeID: 2, StoreID: 2}, {NodeID: 3, StoreID: 3}},
//		rangeUsageInfo,
//		storeFilterThrottled,
//	)
//	zero := roachpb.ReplicationTarget{}
//	if target != zero {
//		t.Fatalf("should not obtain a rebalanced store")
//	}
//}
//
//// TransferLeaseTargetForTsRange
//func TestAllocatorTsTransferLeaseTargetWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	stopper, g, _, a, _ := createTestAllocatorTs(10, true /* deterministic */)
//	defer stopper.Stop(context.Background())
//
//	var stores []*roachpb.StoreDescriptor
//	for i := 1; i <= 5; i++ {
//		stores = append(stores, &roachpb.StoreDescriptor{
//			StoreID: roachpb.StoreID(i),
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
//			//Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
//		})
//	}
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(stores, t)
//
//	testCases := []struct {
//		existing    []roachpb.ReplicaDescriptor
//		leaseholder roachpb.NodeID
//		check       bool
//		expected    roachpb.NodeID
//	}{
//		// The leaseholder is transferred from a non-predistributed
//	    	replica to a non-predistributed replica, and the current
//	    	leaseholder can be used as a candidate
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(4), StoreID: roachpb.StoreID(4)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//				{NodeID: roachpb.NodeID(6), StoreID: roachpb.StoreID(6)},
//			},
//			leaseholder: 5,
//			check:       true,
//			expected:    0,
//		},
//		// The leaseholder is transferred from a non-predistributed
//	     	replica to a non-predistributed replica, and the current
//	     	leaseholder is not a candidate
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(4), StoreID: roachpb.StoreID(4)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//				{NodeID: roachpb.NodeID(6), StoreID: roachpb.StoreID(6)},
//			},
//			leaseholder: 5,
//			check:       false,
//			expected:    4,
//		},
//		// The leaseholder is on the pre-distributed leaseHolder replica
//	    	and does not perform any operation
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 1,
//			check:       true,
//			expected:    0,
//		},
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 1,
//			check:       false,
//			expected:    0,
//		},
//		// The leaseholder does not exist yet, so the leaseholder is
//	    	transferred to the pre-distributed leaseHolder replica
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 0,
//			check:       true,
//			expected:    1,
//		},
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 0,
//			check:       false,
//			expected:    1,
//		},
//		// The leaseholder is transferred from the non-predistributed
//	    	replica to the predistributed leaseholder replica
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//			},
//			leaseholder: 5,
//			check:       true,
//			expected:    1,
//		},
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//			},
//			leaseholder: 5,
//			check:       false,
//			expected:    1,
//		},
//		// The leaseholder is transferred from the non-predistributed
//	    	replica to the predistributed non-leaseHolder replica
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(4), StoreID: roachpb.StoreID(4)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//			},
//			leaseholder: 5,
//			check:       true,
//			expected:    2,
//		},
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(4), StoreID: roachpb.StoreID(4)},
//				{NodeID: roachpb.NodeID(5), StoreID: roachpb.StoreID(5)},
//			},
//			leaseholder: 5,
//			check:       false,
//			expected:    2,
//		},
//		// The leaseholder is transferred from the pre-distributed
//	    	non-leaseHolder replica to the pre-distributed leaseHolder
//	    	replica
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 2,
//			check:       true,
//			expected:    1,
//		},
//		{
//			existing: []roachpb.ReplicaDescriptor{
//				{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)},
//				{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)},
//				{NodeID: roachpb.NodeID(3), StoreID: roachpb.StoreID(3)},
//			},
//			leaseholder: 2,
//			check:       false,
//			expected:    1,
//		},
//	}
//
//	for _, c := range testCases {
//		t.Run("", func(t *testing.T) {
//			preDistributionRangeDesc.SetReplicas(roachpb.MakeReplicaDescriptors(c.existing))
//
//			target := a.TransferLeaseTargetForTsRange(
//				context.Background(),
//				zonepb.EmptyCompleteZoneConfig(),
//				&preDistributionRangeDesc,
//				//c.existing,
//				c.leaseholder,
//				0,
//				nil,     /* replicaStats */
//				c.check, /* checkTransferLeaseSource */
//				true,    /* checkCandidateFullness */
//				false,   /* alwaysAllowDecisionWithoutStats */
//			)
//
//			if c.expected != target.NodeID {
//				t.Fatalf("expected %d, but found %d", c.expected, target.StoreID)
//			}
//		})
//	}
//}
//
//// RebalanceTargetForTsRange
//func TestAllocatorTsRebalanceDifferentLocalitySizesWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//
//	// Set up 8 stores -- 2 in each of the first 2 localities, and 4 in the third.
//	// Because of the desire for diversity, the nodes in the small localities end
//	// up being fuller than the nodes in the large locality. In the past this has
//	// caused an over-eagerness to rebalance to nodes in the large locality, and
//	// not enough willingness to rebalance within the small localities. This test
//	// verifies that we compare fairly amongst stores that will givve the store
//	// an optimal diversity score, not considering the fullness of those that
//	// will make for worse diversity.
//	stores := []*roachpb.StoreDescriptor{
//		{
//			StoreID: 1,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(1),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(50, 50),
//		},
//		{
//			StoreID: 2,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(2),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "1"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(40, 60),
//		},
//		{
//			StoreID: 3,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(3),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(50, 50),
//		},
//		{
//			StoreID: 4,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(4),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "2"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(40, 60),
//		},
//		{
//			StoreID: 5,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(5),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(90, 10),
//		},
//		{
//			StoreID: 6,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(6),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(80, 20),
//		},
//		{
//			StoreID: 7,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(7),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(80, 20),
//		},
//		{
//			StoreID: 8,
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(8),
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{{Key: "locale", Value: "3"}},
//				},
//			},
//			Capacity: testStoreCapacitySetup(80, 20),
//		},
//	}
//
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(stores, t)
//
//	testCases := []struct {
//		existing []roachpb.ReplicaDescriptor
//		expected roachpb.StoreID // 0 if no rebalance is expected
//	}{
//		{tsReplicas(1, 3, 5), 0},
//		{tsReplicas(2, 3, 5), 1},
//		{tsReplicas(1, 4, 5), 3},
//		{tsReplicas(1, 3, 6), 5},
//		{tsReplicas(1, 5, 6), 3},
//		{tsReplicas(2, 5, 6), 3},
//		{tsReplicas(3, 5, 6), 1},
//		{tsReplicas(4, 5, 6), 1},
//	}
//
//	repl := &Replica{
//		RangeID: firstTsRangeID,
//		store: &Store{
//			nodeDesc: &roachpb.NodeDescriptor{
//				NodeID: 3,
//			},
//		},
//	}
//
//	for i, tc := range testCases {
//		var rangeUsageInfo RangeUsageInfo
//		result, _, details, ok := a.RebalanceTargetForTsRange(
//			ctx,
//			&threeReplicaTsZoneConfigLeasePreferences,
//			repl,
//			firstTsRangeID,
//			tc.existing,
//			rangeUsageInfo,
//		)
//		var resultID roachpb.StoreID
//		if ok {
//			resultID = result.StoreID
//		}
//		if resultID != tc.expected {
//			t.Errorf("%d: 1-RebalanceTarget(%v) expected s%d; got %v: %s", i, tc.existing, tc.expected, result, details)
//		}
//	}
//
//	// Add a couple less full nodes in a fourth locality, then run a few more tests:
//	stores = append(stores, &roachpb.StoreDescriptor{
//		StoreID: 9,
//		Node: roachpb.NodeDescriptor{
//			NodeID: roachpb.NodeID(9),
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
//			},
//		},
//		Capacity: testStoreCapacitySetup(70, 30),
//	})
//	stores = append(stores, &roachpb.StoreDescriptor{
//		StoreID: 10,
//		Node: roachpb.NodeDescriptor{
//			NodeID: roachpb.NodeID(10),
//			Locality: roachpb.Locality{
//				Tiers: []roachpb.Tier{{Key: "locale", Value: "4"}},
//			},
//		},
//		Capacity: testStoreCapacitySetup(60, 40),
//	})
//
//	sg.GossipStores(stores, t)
//
//	testCases2 := []struct {
//		existing []roachpb.ReplicaDescriptor
//		expected []roachpb.StoreID
//	}{
//		{tsReplicas(1, 3, 5), []roachpb.StoreID{9}},
//		{tsReplicas(2, 3, 5), []roachpb.StoreID{9}},
//		{tsReplicas(1, 4, 5), []roachpb.StoreID{9}},
//		{tsReplicas(1, 3, 6), []roachpb.StoreID{9}},
//		{tsReplicas(1, 5, 6), []roachpb.StoreID{9}},
//		{tsReplicas(2, 5, 6), []roachpb.StoreID{9}},
//		{tsReplicas(3, 5, 6), []roachpb.StoreID{9}},
//		{tsReplicas(4, 5, 6), []roachpb.StoreID{9}},
//		{tsReplicas(5, 6, 7), []roachpb.StoreID{9}},
//		{tsReplicas(1, 5, 9), nil},
//		{tsReplicas(3, 5, 9), nil},
//		{tsReplicas(1, 3, 9), []roachpb.StoreID{5, 6, 7, 8}},
//		{tsReplicas(1, 3, 10), []roachpb.StoreID{5, 6, 7, 8}},
//		// This last case is a bit more interesting - the difference in range count
//		// between s10 an s9 is significant enough to motivate a rebalance if they
//		// were the only two valid options, but they're both considered underful
//		// relative to the other equally valid placement options (s3 and s4), so
//		// the system doesn't consider it helpful to rebalance between them. It'd
//		// prefer to move replicas onto both s9 and s10 from other stores.
//		{tsReplicas(1, 5, 10), nil},
//	}
//
//	for i, tc := range testCases2 {
//		log.Infof(ctx, "case #%d", i)
//		var rangeUsageInfo RangeUsageInfo
//		result, _, details, ok := a.RebalanceTargetForTsRange(
//			ctx,
//			&threeReplicaTsZoneConfigLeasePreferences,
//			repl,
//			firstTsRangeID,
//			tc.existing,
//			rangeUsageInfo,
//		)
//		var gotExpected bool
//		if !ok {
//			gotExpected = (tc.expected == nil)
//		} else {
//			for _, expectedStoreID := range tc.expected {
//				if result.StoreID == expectedStoreID {
//					gotExpected = true
//					break
//				}
//			}
//		}
//		if !gotExpected {
//			t.Errorf("%d: 2-RebalanceTarget(%v) expected store in %v; got %v: %s",
//				i, tc.existing, tc.expected, result, details)
//		}
//	}
//}
//
//// ShouldTransferLeaseForTsRange
//func TestAllocatorTsShouldTransferLeaseWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	stopper, g, _, a, _ := createTestAllocatorTs(10, true /* deterministic */)
//	defer stopper.Stop(context.Background())
//
//	// 4 stores where the lease count for each store is equal to 10x the store ID.
//	var stores []*roachpb.StoreDescriptor
//	for i := 1; i <= 4; i++ {
//		stores = append(stores, &roachpb.StoreDescriptor{
//			StoreID:  roachpb.StoreID(i),
//			Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
//			Capacity: roachpb.StoreCapacity{LeaseCount: int32(10 * i)},
//		})
//	}
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(stores, t)
//
//	testCases := []struct {
//		leaseholder roachpb.StoreID
//		existing    []roachpb.ReplicaDescriptor
//		expected    bool
//	}{
//		{leaseholder: 1, existing: nil, expected: false},
//		{leaseholder: 2, existing: nil, expected: false},
//		{leaseholder: 3, existing: nil, expected: false},
//		{leaseholder: 4, existing: nil, expected: false},
//		{leaseholder: 3, existing: tsReplicas(1), expected: true},
//		{leaseholder: 3, existing: tsReplicas(1, 2), expected: true},
//		{leaseholder: 3, existing: tsReplicas(2), expected: false},
//		{leaseholder: 3, existing: tsReplicas(3), expected: false},
//		{leaseholder: 3, existing: tsReplicas(4), expected: false},
//		{leaseholder: 4, existing: tsReplicas(1), expected: true},
//		{leaseholder: 4, existing: tsReplicas(2), expected: true},
//		{leaseholder: 4, existing: tsReplicas(3), expected: true},
//		{leaseholder: 4, existing: tsReplicas(1, 2, 3), expected: true},
//	}
//	for _, c := range testCases {
//		t.Run("", func(t *testing.T) {
//			leaseholderReplica := roachpb.ReplicaDescriptor{
//				NodeID:  roachpb.NodeID(c.leaseholder),
//				StoreID: c.leaseholder,
//			}
//			result := a.ShouldTransferLeaseForTsRange(
//				context.Background(),
//				&threeReplicaTsZoneConfigLeasePreferences,
//				c.existing,
//				leaseholderReplica,
//				&roachpb.RangeDescriptor{
//					RangeID: firstTsRangeID,
//				},
//				nil, /* replicaStats */
//			)
//			if c.expected != result {
//				t.Fatalf("expected %v, but found %v", c.expected, result)
//			}
//		})
//	}
//}
//
//// ShouldTransferLeaseForTsRange
//func TestAllocatorTsShouldTransferLeaseWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	stopper, g, _, a, _ := createTestAllocatorTs(10, true /* deterministic */)
//	defer stopper.Stop(context.Background())
//
//	var stores []*roachpb.StoreDescriptor
//	for i := 1; i <= 4; i++ {
//		stores = append(stores, &roachpb.StoreDescriptor{
//			StoreID: roachpb.StoreID(i),
//			Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i)},
//		})
//	}
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(stores, t)
//
//	testCases := []struct {
//		leaseholder roachpb.StoreID
//		existing    []roachpb.ReplicaDescriptor
//		expected    bool
//	}{
//		{leaseholder: 1, existing: nil, expected: false},
//		{leaseholder: 2, existing: nil, expected: false},
//		{leaseholder: 3, existing: nil, expected: false},
//		{leaseholder: 4, existing: nil, expected: false},
//		{leaseholder: 3, existing: tsReplicas(1), expected: true},
//		{leaseholder: 3, existing: tsReplicas(1, 2), expected: true},
//		{leaseholder: 3, existing: tsReplicas(2), expected: false},
//		{leaseholder: 3, existing: tsReplicas(3), expected: false},
//		{leaseholder: 3, existing: tsReplicas(4), expected: false},
//		{leaseholder: 4, existing: tsReplicas(1), expected: true},
//		{leaseholder: 4, existing: tsReplicas(2), expected: true},
//		{leaseholder: 4, existing: tsReplicas(3), expected: true},
//		{leaseholder: 4, existing: tsReplicas(1, 2, 3), expected: true},
//	}
//	for _, c := range testCases {
//		t.Run("", func(t *testing.T) {
//			leaseholderReplica := roachpb.ReplicaDescriptor{
//				NodeID:  roachpb.NodeID(c.leaseholder),
//				StoreID: c.leaseholder,
//			}
//			result := a.ShouldTransferLeaseForTsRange(
//				context.Background(),
//				zonepb.EmptyCompleteZoneConfig(),
//				c.existing,
//				leaseholderReplica,
//				&preDistributionRangeDesc,
//				nil, /* replicaStats */
//			)
//			if c.expected != result {
//				t.Fatalf("expected %v, but found %v", c.expected, result)
//			}
//		})
//	}
//}
//
//// ShouldTransferLeaseForTsRange
//func TestAllocatorTsLeasePreferencesWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	stopper, g, _, a, _ := createTestAllocatorTs(10, true /* deterministic */)
//	defer stopper.Stop(context.Background())
//
//	// 4 stores with distinct localities, store attributes, and node attributes
//	// where the lease count for each store is equal to 100x the store ID.
//	var stores []*roachpb.StoreDescriptor
//	for i := 1; i <= 4; i++ {
//		stores = append(stores, &roachpb.StoreDescriptor{
//			StoreID: roachpb.StoreID(i),
//			Attrs:   roachpb.Attributes{Attrs: []string{fmt.Sprintf("s%d", i)}},
//			Node: roachpb.NodeDescriptor{
//				NodeID: roachpb.NodeID(i),
//				Attrs:  roachpb.Attributes{Attrs: []string{fmt.Sprintf("n%d", i)}},
//				Locality: roachpb.Locality{
//					Tiers: []roachpb.Tier{
//						{Key: "dc", Value: strconv.Itoa(i)},
//					},
//				},
//			},
//			Capacity: roachpb.StoreCapacity{LeaseCount: int32(100 * i)},
//		})
//	}
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(stores, t)
//
//	preferDC1 := []zonepb.LeasePreference{
//		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "1", Type: zonepb.Constraint_REQUIRED}}},
//	}
//	preferDC4Then3Then2 := []zonepb.LeasePreference{
//		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "4", Type: zonepb.Constraint_REQUIRED}}},
//		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "3", Type: zonepb.Constraint_REQUIRED}}},
//		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "2", Type: zonepb.Constraint_REQUIRED}}},
//	}
//	preferN2ThenS3 := []zonepb.LeasePreference{
//		{Constraints: []zonepb.Constraint{{Value: "n2", Type: zonepb.Constraint_REQUIRED}}},
//		{Constraints: []zonepb.Constraint{{Value: "s3", Type: zonepb.Constraint_REQUIRED}}},
//	}
//	preferNotS1ThenNotN2 := []zonepb.LeasePreference{
//		{Constraints: []zonepb.Constraint{{Value: "s1", Type: zonepb.Constraint_PROHIBITED}}},
//		{Constraints: []zonepb.Constraint{{Value: "n2", Type: zonepb.Constraint_PROHIBITED}}},
//	}
//	preferNotS1AndNotN2 := []zonepb.LeasePreference{
//		{
//			Constraints: []zonepb.Constraint{
//				{Value: "s1", Type: zonepb.Constraint_PROHIBITED},
//				{Value: "n2", Type: zonepb.Constraint_PROHIBITED},
//			},
//		},
//	}
//	preferMatchesNothing := []zonepb.LeasePreference{
//		{Constraints: []zonepb.Constraint{{Key: "dc", Value: "5", Type: zonepb.Constraint_REQUIRED}}},
//		{Constraints: []zonepb.Constraint{{Value: "n6", Type: zonepb.Constraint_REQUIRED}}},
//	}
//
//	testCases := []struct {
//		leaseholder        roachpb.StoreID
//		existing           []roachpb.ReplicaDescriptor
//		preferences        []zonepb.LeasePreference
//		expectedCheckTrue  roachpb.StoreID /* checkTransferLeaseSource = true */
//		expectedCheckFalse roachpb.StoreID /* checkTransferLeaseSource = false */
//	}{
//		{1, nil, preferDC1, 0, 0},
//		{1, tsReplicas(1, 2, 3, 4), preferDC1, 0, 2},
//		{1, tsReplicas(2, 3, 4), preferDC1, 0, 2},
//		{2, tsReplicas(1, 2, 3, 4), preferDC1, 1, 1},
//		{2, tsReplicas(2, 3, 4), preferDC1, 0, 3},
//		{4, tsReplicas(2, 3, 4), preferDC1, 2, 2},
//		{1, nil, preferDC4Then3Then2, 0, 0},
//		{1, tsReplicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
//		{1, tsReplicas(1, 2, 3), preferDC4Then3Then2, 3, 3},
//		{1, tsReplicas(1, 2), preferDC4Then3Then2, 2, 2},
//		{3, tsReplicas(1, 2, 3, 4), preferDC4Then3Then2, 4, 4},
//		{3, tsReplicas(1, 2, 3), preferDC4Then3Then2, 0, 2},
//		{3, tsReplicas(1, 3), preferDC4Then3Then2, 0, 1},
//		{4, tsReplicas(1, 2, 3, 4), preferDC4Then3Then2, 0, 3},
//		{4, tsReplicas(1, 2, 4), preferDC4Then3Then2, 0, 2},
//		{4, tsReplicas(1, 4), preferDC4Then3Then2, 0, 1},
//		{1, tsReplicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
//		{1, tsReplicas(1, 3, 4), preferN2ThenS3, 3, 3},
//		{1, tsReplicas(1, 4), preferN2ThenS3, 0, 4},
//		{2, tsReplicas(1, 2, 3, 4), preferN2ThenS3, 0, 3},
//		{2, tsReplicas(1, 2, 4), preferN2ThenS3, 0, 1},
//		{3, tsReplicas(1, 2, 3, 4), preferN2ThenS3, 2, 2},
//		{3, tsReplicas(1, 3, 4), preferN2ThenS3, 0, 1},
//		{4, tsReplicas(1, 4), preferN2ThenS3, 1, 1},
//		{1, tsReplicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
//		{1, tsReplicas(1, 3, 4), preferNotS1ThenNotN2, 3, 3},
//		{1, tsReplicas(1, 2), preferNotS1ThenNotN2, 2, 2},
//		{1, tsReplicas(1), preferNotS1ThenNotN2, 0, 0},
//		{2, tsReplicas(1, 2, 3, 4), preferNotS1ThenNotN2, 0, 3},
//		{2, tsReplicas(2, 3, 4), preferNotS1ThenNotN2, 0, 3},
//		{2, tsReplicas(1, 2, 3), preferNotS1ThenNotN2, 0, 3},
//		{2, tsReplicas(1, 2, 4), preferNotS1ThenNotN2, 0, 4},
//		{4, tsReplicas(1, 2, 3, 4), preferNotS1ThenNotN2, 2, 2},
//		{4, tsReplicas(1, 4), preferNotS1ThenNotN2, 0, 1},
//		{1, tsReplicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
//		{1, tsReplicas(1, 2), preferNotS1AndNotN2, 0, 2},
//		{2, tsReplicas(1, 2, 3, 4), preferNotS1AndNotN2, 3, 3},
//		{2, tsReplicas(2, 3, 4), preferNotS1AndNotN2, 3, 3},
//		{2, tsReplicas(1, 2, 3), preferNotS1AndNotN2, 3, 3},
//		{2, tsReplicas(1, 2, 4), preferNotS1AndNotN2, 4, 4},
//		{3, tsReplicas(1, 3), preferNotS1AndNotN2, 0, 1},
//		{4, tsReplicas(1, 4), preferNotS1AndNotN2, 0, 1},
//		{1, tsReplicas(1, 2, 3, 4), preferMatchesNothing, 0, 2},
//		{2, tsReplicas(1, 2, 3, 4), preferMatchesNothing, 0, 1},
//		{3, tsReplicas(1, 3, 4), preferMatchesNothing, 1, 1},
//		{4, tsReplicas(1, 3, 4), preferMatchesNothing, 1, 1},
//		{4, tsReplicas(2, 3, 4), preferMatchesNothing, 2, 2},
//	}
//
//	for _, c := range testCases {
//		t.Run("", func(t *testing.T) {
//			zone := &zonepb.ZoneConfig{NumReplicas: proto.Int32(0), LeasePreferences: c.preferences}
//			leaseholderReplica := roachpb.ReplicaDescriptor{
//				NodeID:  roachpb.NodeID(c.leaseholder),
//				StoreID: c.leaseholder,
//			}
//			result := a.ShouldTransferLeaseForTsRange(
//				context.Background(),
//				zone,
//				c.existing,
//				leaseholderReplica,
//				&preDistributionRangeDesc,
//				nil, /* replicaStats */
//			)
//			expectTransfer := c.expectedCheckTrue != 0
//			if expectTransfer != result {
//				t.Errorf("expected %v, but found %v", expectTransfer, result)
//			}
//			target := a.TransferLeaseTarget(
//				context.Background(),
//				zone,
//				c.existing,
//				c.leaseholder,
//				0,
//				nil,   /* replicaStats */
//				true,  /* checkTransferLeaseSource */
//				true,  /* checkCandidateFullness */
//				false, /* alwaysAllowDecisionWithoutStats */
//			)
//			if c.expectedCheckTrue != target.StoreID {
//				t.Errorf("expected s%d for check=true, but found %v", c.expectedCheckTrue, target)
//			}
//			target = a.TransferLeaseTarget(
//				context.Background(),
//				zone,
//				c.existing,
//				c.leaseholder,
//				0,
//				nil,   /* replicaStats */
//				false, /* checkTransferLeaseSource */
//				true,  /* checkCandidateFullness */
//				false, /* alwaysAllowDecisionWithoutStats */
//			)
//			if c.expectedCheckFalse != target.StoreID {
//				t.Errorf("expected s%d for check=false, but found %v", c.expectedCheckFalse, target)
//			}
//		})
//	}
//}
//
//// RemoveTargetForPreDist
//func TestAllocatorTsRemoveTargetWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	stopper, g, _, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	defer stopper.Stop(context.Background())
//	sg := gossiputil.NewStoreGossiper(g)
//	sg.GossipStores(multiDiversityDCTsStores, t)
//
//	// Given a set of existing replicas for a range, pick out the ones that should
//	// be removed purely on the basis of pre distribution.
//	testCases := []struct {
//		existing []roachpb.StoreID
//		expected []roachpb.StoreID
//	}{
//		{
//			[]roachpb.StoreID{1, 2, 3, 5},
//			[]roachpb.StoreID{5},
//		},
//	}
//	for _, c := range testCases {
//		existingRepls := make([]roachpb.ReplicaDescriptor, len(c.existing))
//		for i, storeID := range c.existing {
//			existingRepls[i] = roachpb.ReplicaDescriptor{
//				NodeID:  roachpb.NodeID(storeID),
//				StoreID: storeID,
//			}
//		}
//		targetRepl, details, err := a.RemoveTargetForPreDist(
//			context.Background(),
//			existingRepls,
//			&preDistributionRangeDesc,
//		)
//		if err != nil {
//			t.Fatal(err)
//		}
//		var found bool
//		for _, storeID := range c.expected {
//			if targetRepl.StoreID == storeID {
//				found = true
//				break
//			}
//		}
//		if !found {
//			t.Errorf("expected RemoveTarget(%v) in %v, but got %d; details: %s", c.existing, c.expected, targetRepl.StoreID, details)
//		}
//	}
//}
//
//// ComputeAction
//func TestAllocatorTsComputeActionWithZoneConfig(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	// Each test case should describe a repair situation which has a lower
//	// priority than the previous test case.
//	testCases := []struct {
//		zone           zonepb.ZoneConfig
//		desc           roachpb.RangeDescriptor
//		expectedAction AllocatorAction
//	}{
//		// Need three replicas, have three, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Need five replicas, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(5),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Need three replicas, have two.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need five replicas, have four, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(5),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need five replicas, have four.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(5),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need three replicas, have four, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need five replicas, have six, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(5),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   5,
//						NodeID:    5,
//						ReplicaID: 5,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need three replicas, have five, one is on a dead store.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need three replicas, have four.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction: AllocatorRemove,
//		},
//		// Need three replicas, have five.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   5,
//						NodeID:    5,
//						ReplicaID: 5,
//					},
//				},
//			},
//			expectedAction: AllocatorRemove,
//		},
//		// Need three replicas, two are on dead stores. Should
//		// be a noop because there aren't enough live replicas for
//		// a quorum.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   7,
//						NodeID:    7,
//						ReplicaID: 7,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//		// Need three replicas, have three, none of the replicas in the store pool.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   10,
//						NodeID:    10,
//						ReplicaID: 10,
//					},
//					{
//						StoreID:   20,
//						NodeID:    20,
//						ReplicaID: 20,
//					},
//					{
//						StoreID:   30,
//						NodeID:    30,
//						ReplicaID: 30,
//					},
//				},
//			},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//		// Need three replicas, have three.
//		{
//			zone: zonepb.ZoneConfig{
//				NumReplicas:   proto.Int32(3),
//				Constraints:   []zonepb.Constraints{{Constraints: []zonepb.Constraint{{Value: "us-east", Type: zonepb.Constraint_DEPRECATED_POSITIVE}}}},
//				RangeMinBytes: proto.Int64(0),
//				RangeMaxBytes: proto.Int64(64000),
//			},
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//				},
//			},
//			expectedAction: AllocatorConsiderRebalance,
//		},
//	}
//
//	stopper, _, sp, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//
//	// Set up eight stores. Stores six and seven are marked as dead. Replica eight
//	// is dead.
//	mockTsStorePool(sp,
//		[]roachpb.StoreID{1, 2, 3, 4, 5, 8}, // aliveStoreIDs
//		nil,
//		[]roachpb.StoreID{6, 7}, // deadStoreIDs
//		nil,
//		nil,
//	)
//
//	lastPriority := float64(999999999)
//	for i, tcase := range testCases {
//		action, priority := a.ComputeAction(ctx, &tcase.zone, &tcase.desc)
//		if tcase.expectedAction != action {
//			t.Errorf("Test case %d expected action %q, got action %q",
//				i, allocatorActionNames[tcase.expectedAction], allocatorActionNames[action])
//			continue
//		}
//		if tcase.expectedAction != AllocatorNoop && priority > lastPriority {
//			t.Errorf("Test cases should have descending priority. Case %d had priority %f, previous case had priority %f", i, priority, lastPriority)
//		}
//		lastPriority = priority
//	}
//}
//
//// ComputeAction
//func TestAllocatorTsComputeActionWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	// Each test case should describe a repair situation which has a lower
//	// priority than the previous test case.
//	testCases := []struct {
//		need           int
//		desc           roachpb.RangeDescriptor
//		expectedAction AllocatorAction
//	}{
//		// Need three replicas, have three, one is on a dead store.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Need five replicas, one is on a dead store.
//		{
//			need: 5,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Need three replicas, have two.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need five replicas, have four, one is on a dead store.
//		{
//			need: 5,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need five replicas, have four.
//		{
//			need: 5,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction: AllocatorAdd,
//		},
//		// Need three replicas, have four, one is on a dead store.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need five replicas, have six, one is on a dead store.
//		{
//			need: 5,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   5,
//						NodeID:    5,
//						ReplicaID: 5,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need three replicas, have five, one is on a dead store.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Need three replicas, have four.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction: AllocatorRemove,
//		},
//		// Need three replicas, have five.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//					{
//						StoreID:   5,
//						NodeID:    5,
//						ReplicaID: 5,
//					},
//				},
//			},
//			expectedAction: AllocatorRemove,
//		},
//		// Need three replicas, two are on dead stores. Should
//		// be a noop because there aren't enough live replicas for
//		// a quorum.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   7,
//						NodeID:    7,
//						ReplicaID: 7,
//					},
//					{
//						StoreID:   6,
//						NodeID:    6,
//						ReplicaID: 6,
//					},
//				},
//			},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//		// Need three replicas, have three, none of the replicas in the store pool.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   10,
//						NodeID:    10,
//						ReplicaID: 10,
//					},
//					{
//						StoreID:   20,
//						NodeID:    20,
//						ReplicaID: 20,
//					},
//					{
//						StoreID:   30,
//						NodeID:    30,
//						ReplicaID: 30,
//					},
//				},
//			},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//		// Need three replicas, have three.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType:          &TsRange,
//				PreDistLeaseHolder: preDistributionRangeDesc.PreDistLeaseHolder,
//				PreDist:            preDistributionRangeDesc.PreDist,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//				},
//			},
//			expectedAction: AllocatorConsiderRebalance,
//		},
//	}
//
//	stopper, _, sp, a, _ := createTestAllocatorTs(10, false)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//
//	// Set up eight stores. Stores six and seven are marked as dead. Replica eight is dead.
//	mockTsStorePool(sp,
//		[]roachpb.StoreID{1, 2, 3, 4, 5, 8}, // aliveStoreIDs
//		nil,
//		[]roachpb.StoreID{6, 7}, // deadStoreIDs
//		nil,
//		nil,
//	)
//
//	lastPriority := float64(999999999)
//	for i, tcase := range testCases {
//		tcase.desc.PreDist = preDistributionRangeDescWithNumReplica(tcase.need).PreDist
//		action, priority := a.ComputeAction(ctx, zonepb.EmptyCompleteZoneConfig(), &tcase.desc)
//		if tcase.expectedAction != action {
//			t.Errorf("Test case %d expected action %q, got action %q",
//				i, allocatorActionNames[tcase.expectedAction], allocatorActionNames[action])
//			continue
//		}
//		if tcase.expectedAction != AllocatorNoop && priority > lastPriority {
//			t.Errorf("Test cases should have descending priority. Case %d had priority %f, "+
//				"previous case had priority %f", i, priority, lastPriority)
//		}
//		lastPriority = priority
//	}
//}
//
//// ComputeAction
//func TestAllocatorTsComputeActionRemoveDeadWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	threeReplDesc := roachpb.RangeDescriptor{
//		RangeType: &TsRange,
//		InternalReplicas: []roachpb.ReplicaDescriptor{
//			{
//				StoreID:   1,
//				NodeID:    1,
//				ReplicaID: 1,
//			},
//			{
//				StoreID:   2,
//				NodeID:    2,
//				ReplicaID: 2,
//			},
//			{
//				StoreID:   3,
//				NodeID:    3,
//				ReplicaID: 3,
//			},
//		},
//	}
//	fourReplDesc := threeReplDesc
//	fourReplDesc.InternalReplicas = append(fourReplDesc.InternalReplicas, roachpb.ReplicaDescriptor{
//		StoreID:   4,
//		NodeID:    4,
//		ReplicaID: 4,
//	})
//
//	// Each test case should describe a repair situation which has a lower
//	// priority than the previous test case.
//	testCases := []struct {
//		need           int
//		desc           roachpb.RangeDescriptor
//		live           []roachpb.StoreID
//		dead           []roachpb.StoreID
//		expectedAction AllocatorAction
//	}{
//		// Needs three replicas, one is dead, and there's no replacement. Since
//		// there's no replacement we can't do anything, but an action is still
//		// emitted.
//		{
//			need:           3,
//			desc:           threeReplDesc,
//			live:           []roachpb.StoreID{1, 2},
//			dead:           []roachpb.StoreID{3},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Needs three replicas, one is dead, but there is a replacement.
//		{
//			need:           3,
//			desc:           threeReplDesc,
//			live:           []roachpb.StoreID{1, 2, 4},
//			dead:           []roachpb.StoreID{3},
//			expectedAction: AllocatorReplaceDead,
//		},
//		// Needs three replicas, two are dead (i.e. the range lacks a quorum).
//		{
//			need:           3,
//			desc:           threeReplDesc,
//			live:           []roachpb.StoreID{1, 4},
//			dead:           []roachpb.StoreID{2, 3},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//		// Needs three replicas, has four, one is dead.
//		{
//			need:           3,
//			desc:           fourReplDesc,
//			live:           []roachpb.StoreID{1, 2, 4},
//			dead:           []roachpb.StoreID{3},
//			expectedAction: AllocatorRemoveDead,
//		},
//		// Needs three replicas, has four, two are dead (i.e. the range lacks a quorum).
//		{
//			need:           3,
//			desc:           fourReplDesc,
//			live:           []roachpb.StoreID{1, 4},
//			dead:           []roachpb.StoreID{2, 3},
//			expectedAction: AllocatorRangeUnavailable,
//		},
//	}
//
//	stopper, _, sp, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//
//	for i, tcase := range testCases {
//		mockTsStorePool(sp, tcase.live, nil, tcase.dead, nil, nil)
//		tcase.desc.PreDist = preDistributionRangeDescWithNumReplica(tcase.need).PreDist
//		action, _ := a.ComputeAction(ctx, zonepb.EmptyCompleteZoneConfig(), &tcase.desc)
//		if tcase.expectedAction != action {
//			t.Errorf("Test case %d expected action %d, got action %d", i, tcase.expectedAction, action)
//		}
//	}
//}
//
//// ComputeAction
//func TestAllocatorTsComputeActionDecommissionWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	testCases := []struct {
//		need            int
//		desc            roachpb.RangeDescriptor
//		expectedAction  AllocatorAction
//		live            []roachpb.StoreID
//		dead            []roachpb.StoreID
//		decommissioning []roachpb.StoreID
//		decommissioned  []roachpb.StoreID
//	}{
//		// Has three replicas, but one is in decommissioning status. We can't
//		// replace it (nor add a new replica) since there isn't a live target,
//		// but that's still the action being emitted.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//				},
//			},
//			expectedAction:  AllocatorReplaceDecommissioning,
//			live:            []roachpb.StoreID{1, 2},
//			dead:            nil,
//			decommissioning: []roachpb.StoreID{3},
//		},
//		// Has three replicas, one is in decommissioning status, and one is on a
//		// dead node. Replacing the dead replica is more important.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//				},
//			},
//			expectedAction:  AllocatorReplaceDead,
//			live:            []roachpb.StoreID{1},
//			dead:            []roachpb.StoreID{2},
//			decommissioning: []roachpb.StoreID{3},
//		},
//		// Needs three replicas, has four, where one is decommissioning and one is
//		// dead.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction:  AllocatorRemoveDead,
//			live:            []roachpb.StoreID{1, 4},
//			dead:            []roachpb.StoreID{2},
//			decommissioning: []roachpb.StoreID{3},
//		},
//		// Needs three replicas, has four, where one is decommissioning and one is
//		// decommissioned.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction:  AllocatorRemoveDead,
//			live:            []roachpb.StoreID{1, 4},
//			dead:            nil,
//			decommissioning: []roachpb.StoreID{3},
//			decommissioned:  []roachpb.StoreID{2},
//		},
//		// Needs three replicas, has three, all decommissioning
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//				},
//			},
//			expectedAction:  AllocatorReplaceDecommissioning,
//			live:            nil,
//			dead:            nil,
//			decommissioning: []roachpb.StoreID{1, 2, 3},
//		},
//		// Needs 3. Has 1 live, 3 decommissioning.
//		{
//			need: 3,
//			desc: roachpb.RangeDescriptor{
//				RangeType: &TsRange,
//				InternalReplicas: []roachpb.ReplicaDescriptor{
//					{
//						StoreID:   1,
//						NodeID:    1,
//						ReplicaID: 1,
//					},
//					{
//						StoreID:   2,
//						NodeID:    2,
//						ReplicaID: 2,
//					},
//					{
//						StoreID:   3,
//						NodeID:    3,
//						ReplicaID: 3,
//					},
//					{
//						StoreID:   4,
//						NodeID:    4,
//						ReplicaID: 4,
//					},
//				},
//			},
//			expectedAction:  AllocatorRemoveDecommissioning,
//			live:            []roachpb.StoreID{4},
//			dead:            nil,
//			decommissioning: []roachpb.StoreID{1, 2, 3},
//		},
//	}
//
//	stopper, _, sp, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//
//	for i, tcase := range testCases {
//		mockTsStorePool(sp, tcase.live, nil, tcase.dead, tcase.decommissioning, tcase.decommissioned)
//		tcase.desc.PreDist = preDistributionRangeDescWithNumReplica(tcase.need).PreDist
//		action, _ := a.ComputeAction(ctx, zonepb.EmptyCompleteZoneConfig(), &tcase.desc)
//		if tcase.expectedAction != action {
//			t.Errorf("Test case %d expected action %s, got action %s", i, tcase.expectedAction, action)
//			continue
//		}
//	}
//}
//
//// ComputeAction
//func TestAllocatorTsRemoveLearnerWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	need := 3
//	learnerType := roachpb.LEARNER
//	rangeWithLearnerDesc := roachpb.RangeDescriptor{
//		RangeType: &TsRange,
//		InternalReplicas: []roachpb.ReplicaDescriptor{
//			{
//				StoreID:   1,
//				NodeID:    1,
//				ReplicaID: 1,
//			},
//			{
//				StoreID:   2,
//				NodeID:    2,
//				ReplicaID: 2,
//				Type:      &learnerType,
//			},
//		},
//	}
//
//	// Removing a learner is prioritized over adding a new replica to an under
//	// replicated range.
//	stopper, _, sp, a, _ := createTestAllocatorTs(10, false /* deterministic */)
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//	live, dead := []roachpb.StoreID{1, 2}, []roachpb.StoreID{3}
//	mockTsStorePool(sp, live, nil, dead, nil, nil)
//	rangeWithLearnerDesc.PreDist = preDistributionRangeDescWithNumReplica(need).PreDist
//	action, _ := a.ComputeAction(ctx, zonepb.EmptyCompleteZoneConfig(), &rangeWithLearnerDesc)
//	require.Equal(t, AllocatorRemoveLearner, action)
//}
//
//// ComputeAction
//func TestAllocatorTsComputeActionDynamicNumReplicas(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	t.Skip()
//
//	// In this test, the configured zone config has a replication factor of five
//	// set. We are checking that the effective replication factor is rounded down
//	// to the number of stores which are not decommissioned or decommissioning.
//	testCases := []struct {
//		storeList           []roachpb.StoreID
//		expectedNumReplicas int
//		expectedAction      AllocatorAction
//		live                []roachpb.StoreID
//		unavailable         []roachpb.StoreID
//		dead                []roachpb.StoreID
//		decommissioning     []roachpb.StoreID
//	}{
//		{
//			// Four known stores, three of them are decommissioning, so effective
//			// replication factor would be 1 if we hadn't decided that we'll never
//			// drop past 3, so 3 it is.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorRemoveDecommissioning,
//			live:                []roachpb.StoreID{4},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     []roachpb.StoreID{1, 2, 3},
//		},
//		{
//			// Ditto.
//			storeList:           []roachpb.StoreID{1, 2, 3},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorReplaceDecommissioning,
//			live:                []roachpb.StoreID{4, 5},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     []roachpb.StoreID{1, 2, 3},
//		},
//		{
//			// Four live stores and one dead one, so the effective replication
//			// factor would be even (four), in which case we drop down one more
//			// to three. Then the right thing becomes removing the dead replica
//			// from the range at hand, rather than trying to replace it.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorRemoveDead,
//			live:                []roachpb.StoreID{1, 2, 3, 5},
//			unavailable:         nil,
//			dead:                []roachpb.StoreID{4},
//			decommissioning:     nil,
//		},
//		{
//			// Two replicas, one on a dead store, but we have four live nodes
//			// in the system which amounts to an effective replication factor
//			// of three (avoiding the even number). Adding a replica is more
//			// important than replacing the dead one.
//			storeList:           []roachpb.StoreID{1, 4},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorAdd,
//			live:                []roachpb.StoreID{1, 2, 3, 5},
//			unavailable:         nil,
//			dead:                []roachpb.StoreID{4},
//			decommissioning:     nil,
//		},
//		{
//			// Similar to above, but nothing to do.
//			storeList:           []roachpb.StoreID{1, 2, 3},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorConsiderRebalance,
//			live:                []roachpb.StoreID{1, 2, 3, 4},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Effective replication factor can't dip below three (unless the
//			// zone config explicitly asks for that, which it does not), so three
//			// it is and we are under-replicaed.
//			storeList:           []roachpb.StoreID{1, 2},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorAdd,
//			live:                []roachpb.StoreID{1, 2},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Three and happy.
//			storeList:           []roachpb.StoreID{1, 2, 3},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorConsiderRebalance,
//			live:                []roachpb.StoreID{1, 2, 3},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Three again, on account of avoiding the even four.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorRemove,
//			live:                []roachpb.StoreID{1, 2, 3, 4},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// The usual case in which there are enough nodes to accommodate the
//			// zone config.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 5,
//			expectedAction:      AllocatorConsiderRebalance,
//			live:                []roachpb.StoreID{1, 2, 3, 4, 5},
//			unavailable:         nil,
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// No dead or decommissioning node and enough nodes around, so
//			// sticking with the zone config.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 5,
//			expectedAction:      AllocatorConsiderRebalance,
//			live:                []roachpb.StoreID{1, 2, 3, 4},
//			unavailable:         []roachpb.StoreID{5},
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Ditto.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 5,
//			expectedAction:      AllocatorConsiderRebalance,
//			live:                []roachpb.StoreID{1, 2, 3},
//			unavailable:         []roachpb.StoreID{4, 5},
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Ditto, but we've lost quorum.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 5,
//			expectedAction:      AllocatorRangeUnavailable,
//			live:                []roachpb.StoreID{1, 2},
//			unavailable:         []roachpb.StoreID{3, 4, 5},
//			dead:                nil,
//			decommissioning:     nil,
//		},
//		{
//			// Ditto (dead nodes don't reduce NumReplicas, only decommissioning
//			// or decommissioned do, and both correspond to the 'decommissioning'
//			// slice in these tests).
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 5,
//			expectedAction:      AllocatorReplaceDead,
//			live:                []roachpb.StoreID{1, 2, 3},
//			unavailable:         []roachpb.StoreID{4},
//			dead:                []roachpb.StoreID{5},
//			decommissioning:     nil,
//		},
//		{
//			// Avoiding four, so getting three, and since there is no dead store
//			// the most important thing is removing a decommissioning replica.
//			storeList:           []roachpb.StoreID{1, 2, 3, 4, 5},
//			expectedNumReplicas: 3,
//			expectedAction:      AllocatorRemoveDecommissioning,
//			live:                []roachpb.StoreID{1, 2, 3},
//			unavailable:         []roachpb.StoreID{4},
//			dead:                nil,
//			decommissioning:     []roachpb.StoreID{5},
//		},
//	}
//
//	var numNodes int
//	stopper, _, _, sp, _ := createTestStorePool(
//		TestTimeUntilStoreDeadOff, false,
//		func() int { return numNodes },
//		storagepb.NodeLivenessStatus_LIVE)
//	a := MakeAllocator(sp, func(string) (time.Duration, bool) {
//		return 0, true
//	})
//
//	ctx := context.Background()
//	defer stopper.Stop(ctx)
//	need := 5
//	for _, prefixKey := range []roachpb.RKey{
//		roachpb.RKey(keys.NodeLivenessPrefix),
//		roachpb.RKey(keys.SystemPrefix),
//	} {
//		for _, c := range testCases {
//			t.Run(prefixKey.String(), func(t *testing.T) {
//				numNodes = len(c.storeList) - len(c.decommissioning)
//				mockTsStorePool(sp, c.live, c.unavailable, c.dead,
//					c.decommissioning, []roachpb.StoreID{})
//				desc := makeTsDescriptor(c.storeList)
//				desc.EndKey = prefixKey
//
//				clusterNodes := a.storePool.ClusterNodeCount()
//				desc.PreDist = preDistributionRangeDescWithNumReplica(need).PreDist
//				effectiveNumReplicas := GetNeededReplicasWithPreDist(len(desc.PreDist), clusterNodes)
//				require.Equal(t, c.expectedNumReplicas, effectiveNumReplicas, "clusterNodes=%d", clusterNodes)
//
//				action, _ := a.ComputeAction(ctx, zonepb.EmptyCompleteZoneConfig(), &desc)
//				require.Equal(t, c.expectedAction.String(), action.String())
//			})
//		}
//	}
//}
//
//// GetNeededReplicasWithPreDist
//func TestAllocatorTsGetNeededReplicasWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	testCases := []struct {
//		preDistRepls int
//		availNodes   int
//		expected     int
//	}{
//		// If preDistRepls <= 3, GetNeededReplicas should always return preDistRepls.
//		{1, 0, 1},
//		{1, 1, 1},
//		{2, 0, 2},
//		{2, 1, 2},
//		{2, 2, 2},
//		{3, 0, 3},
//		{3, 1, 3},
//		{3, 3, 3},
//		// Things get more involved when preDistRepls > 3.
//		{4, 1, 3},
//		{4, 2, 3},
//		{4, 3, 3},
//		{4, 4, 4},
//		{5, 1, 3},
//		{5, 2, 3},
//		{5, 3, 3},
//		{5, 4, 3},
//		{5, 5, 5},
//		{6, 1, 3},
//		{6, 2, 3},
//		{6, 3, 3},
//		{6, 4, 3},
//		{6, 5, 5},
//		{6, 6, 6},
//		{7, 1, 3},
//		{7, 2, 3},
//		{7, 3, 3},
//		{7, 4, 3},
//		{7, 5, 5},
//		{7, 6, 5},
//		{7, 7, 7},
//	}
//
//	for _, tc := range testCases {
//		if e, a := tc.expected, GetNeededReplicasWithPreDist(tc.preDistRepls, tc.availNodes); e != a {
//			t.Errorf(
//				"GetNeededReplicasWithPreDist(preDistRepls=%d, availNodes=%d) got %d; want %d",
//				tc.preDistRepls, tc.availNodes, a, e)
//		}
//	}
//}
//
//// allocatorError
//func TestAllocatorErrorWithPreDist(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//
//	preDist := []roachpb.ReplicaDescriptor{
//		{StoreID: 1, NodeID: 1},
//		{StoreID: 2, NodeID: 2},
//		{StoreID: 3, NodeID: 3},
//	}
//
//	testCases := []struct {
//		ae       allocatorError
//		expected string
//	}{
//		{allocatorError{preDist: nil, existingReplicas: 1, aliveStores: 1},
//			"0 of 1 live stores are able to take a new replica for the range (1 already has a replica); likely not enough nodes in cluster"},
//		{allocatorError{preDist: nil, existingReplicas: 1, aliveStores: 2, throttledStores: 1},
//			"0 of 2 live stores are able to take a new replica for the range (1 throttled, 1 already has a replica)"},
//		{allocatorError{preDist: preDist, existingReplicas: 1, aliveStores: 1},
//			`0 of 1 live stores are able to take a new replica for the range (1 already has a replica); must match preDist [{NodeId:1,StoreId:1}{NodeId:2,StoreId:2}{NodeId:3,StoreId:3}]`},
//		{allocatorError{preDist: preDist, existingReplicas: 1, aliveStores: 2},
//			`0 of 2 live stores are able to take a new replica for the range (1 already has a replica); must match preDist [{NodeId:1,StoreId:1}{NodeId:2,StoreId:2}{NodeId:3,StoreId:3}]`},
//		{allocatorError{preDist: preDist, existingReplicas: 1, aliveStores: 1},
//			`0 of 1 live stores are able to take a new replica for the range (1 already has a replica); must match preDist [{NodeId:1,StoreId:1}{NodeId:2,StoreId:2}{NodeId:3,StoreId:3}]`},
//		{allocatorError{preDist: preDist, existingReplicas: 1, aliveStores: 2},
//			`0 of 2 live stores are able to take a new replica for the range (1 already has a replica); must match preDist [{NodeId:1,StoreId:1}{NodeId:2,StoreId:2}{NodeId:3,StoreId:3}]`},
//		{allocatorError{preDist: preDist, existingReplicas: 1, aliveStores: 2, throttledStores: 1},
//			`0 of 2 live stores are able to take a new replica for the range (1 throttled, 1 already has a replica); must match preDist [{NodeId:1,StoreId:1}{NodeId:2,StoreId:2}{NodeId:3,StoreId:3}]`},
//	}
//
//	for i, testCase := range testCases {
//		if actual := testCase.ae.Error(); testCase.expected != actual {
//			t.Errorf("%d: actual error message \"%s\" does not match expected \"%s\"", i, actual, testCase.expected)
//		}
//	}
//}
