// Copyright 2019 The Cockroach Authors.
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

package replicaoracle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// TestRandomOracle defeats TestUnused for RandomChoice.
func TestRandomOracle(t *testing.T) {
	_ = NewOracleFactory(RandomChoice, Config{})
}

// Test that the binPackingOracle is consistent in its choices: once a range has
// been assigned to one node, that choice is reused.
func TestBinPackingOracleIsConsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := roachpb.RangeDescriptor{RangeID: 99}
	queryState := MakeQueryState()
	expRepl := kvcoord.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{
			NodeID: 99, StoreID: 99, ReplicaID: 99}}
	queryState.AssignedRanges[rng.RangeID] = expRepl
	of := NewOracleFactory(BinPackingChoice, Config{
		LeaseHolderCache: kvcoord.NewLeaseHolderCache(func() int64 { return 1 }),
	})
	// For our purposes, an uninitialized binPackingOracle will do.
	bp := of.Oracle(nil)
	repl, err := bp.ChoosePreferredReplica(context.TODO(), rng, queryState)
	if err != nil {
		t.Fatal(err)
	}
	if repl != expRepl {
		t.Fatalf("expected replica %+v, got: %+v", expRepl, repl)
	}
}

func TestClosest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	g, _ := makeGossip(t, stopper)
	nd, _ := g.GetNodeDescriptor(1)
	of := NewOracleFactory(ClosestChoice, Config{
		Gossip:   g,
		NodeDesc: *nd,
	})
	of.(*closestOracle).latencyFunc = func(s string) (time.Duration, bool) {
		if strings.HasSuffix(s, "2") {
			return time.Nanosecond, true
		}
		return time.Millisecond, true
	}
	o := of.Oracle(nil)
	info, err := o.ChoosePreferredReplica(context.TODO(), roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}, QueryState{})
	if err != nil {
		t.Fatalf("Failed to choose closest replica: %v", err)
	}
	if info.NodeID != 2 {
		t.Fatalf("Failed to choose node 2, got %v", info.NodeID)
	}
}

func makeGossip(t *testing.T, stopper *stop.Stopper) (*gossip.Gossip, *hlc.Clock) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	server := rpc.NewServer(rpcContext)

	const nodeID = 1
	g := gossip.NewTest(nodeID, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	if err := g.SetNodeDescriptor(newNodeDesc(nodeID)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	for i := roachpb.NodeID(2); i <= 3; i++ {
		err := g.AddInfoProto(gossip.MakeNodeIDKey(i), newNodeDesc(i), gossip.NodeDescriptorTTL)
		if err != nil {
			t.Fatal(err)
		}
	}
	return g, clock
}

func newNodeDesc(nodeID roachpb.NodeID) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", fmt.Sprintf("invalid.invalid:%d", nodeID)),
	}
}

// TestReplicaSliceOrErr tests the replicaSliceOrErr function
func TestReplicaSliceOrErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Test case 1: Gossip has node information
	g, _ := makeGossip(t, stopper)
	desc := roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
		},
	}
	replicas, err := replicaSliceOrErr(desc, g)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got: %d", len(replicas))
	}

	// Test case 2: Gossip has no node information (should return error)
	// Create a new gossip instance with only node 1
	g2 := gossip.NewTest(1, rpc.NewInsecureTestingContext(hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper),
		rpc.NewServer(rpc.NewInsecureTestingContext(hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper)),
		stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	if err := g2.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	if err := g2.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	desc2 := roachpb.RangeDescriptor{
		RangeID: 123,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 99, StoreID: 99}, // Node 99 not in gossip
		},
	}
	_, err = replicaSliceOrErr(desc2, g2)
	if err == nil {
		t.Fatal("expected error for unknown node, got nil")
	}
	if !strings.Contains(err.Error(), "node info not available in gossip") {
		t.Fatalf("expected error message to contain 'node info not available in gossip', got: %s", err.Error())
	}
}

// TestRandomOracleChoosePreferredReplica tests the randomOracle's ChoosePreferredReplica method
func TestRandomOracleChoosePreferredReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, _ := makeGossip(t, stopper)
	of := NewOracleFactory(RandomChoice, Config{Gossip: g})
	o := of.Oracle(nil)

	desc := roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}

	// Test that it returns a replica without error
	repl, err := o.ChoosePreferredReplica(context.TODO(), desc, QueryState{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if repl.NodeID == 0 {
		t.Fatal("expected non-zero node ID")
	}

	// Test that it returns different replicas (with high probability)
	replicaCounts := make(map[roachpb.NodeID]int)
	iterations := 100
	for i := 0; i < iterations; i++ {
		repl, err := o.ChoosePreferredReplica(context.TODO(), desc, QueryState{})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		replicaCounts[repl.NodeID]++
	}

	// Check that we got at least two different replicas
	if len(replicaCounts) < 2 {
		t.Fatalf("expected at least 2 different replicas, got: %v", replicaCounts)
	}
}

// TestBinPackingOracleChoosePreferredReplica tests the binPackingOracle's ChoosePreferredReplica method
func TestBinPackingOracleChoosePreferredReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, _ := makeGossip(t, stopper)
	nd, _ := g.GetNodeDescriptor(1)

	// Test case 1: Use cached lease holder
	of := NewOracleFactory(BinPackingChoice, Config{
		Gossip:           g,
		NodeDesc:         *nd,
		LeaseHolderCache: kvcoord.NewLeaseHolderCache(func() int64 { return 1 }),
	})
	o := of.Oracle(nil)

	// Add a lease holder to the cache
	bp := o.(*binPackingOracle)
	bp.leaseHolderCache.Update(context.TODO(), 123, 1) // Range 123 has lease on store 1

	desc := roachpb.RangeDescriptor{
		RangeID: 123,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
		},
	}

	repl, err := o.ChoosePreferredReplica(context.TODO(), desc, QueryState{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if repl.NodeID != 1 {
		t.Fatalf("expected node 1 (cached lease holder), got: %d", repl.NodeID)
	}

	// Test case 2: Use least loaded node
	queryState := MakeQueryState()
	queryState.RangesPerNode[2] = maxPreferredRangesPerLeaseHolder // Node 2 is full
	queryState.RangesPerNode[3] = 5                                // Node 3 has some load
	queryState.RangesPerNode[1] = 0

	desc2 := roachpb.RangeDescriptor{
		RangeID: 456, // New range not in cache
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}

	repl2, err := o.ChoosePreferredReplica(context.TODO(), desc2, queryState)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Check that the selected node is either 1 or 3 (both have lower load than 2)
	if repl2.NodeID != 1 && repl2.NodeID != 3 {
		t.Fatalf("expected node 1 or 3 (least loaded), got: %d", repl2.NodeID)
	}
	// Verify the selected node has lower load than node 2
	if queryState.RangesPerNode[repl2.NodeID] >= queryState.RangesPerNode[2] {
		t.Fatalf("selected node %d has load %d, which is not less than node 2's load %d",
			repl2.NodeID, queryState.RangesPerNode[repl2.NodeID], queryState.RangesPerNode[2])
	}

	// Test case 3: Use node with existing assignments but not full
	queryState2 := MakeQueryState()
	queryState2.RangesPerNode[1] = 5 // Node 1 has some load but not full

	repl3, err := o.ChoosePreferredReplica(context.TODO(), desc2, queryState2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if repl3.NodeID != 1 {
		t.Fatalf("expected node 1 (has existing assignments), got: %d", repl3.NodeID)
	}
}

// TestLatencyFunc tests the latencyFunc function
func TestLatencyFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Test case 1: RPC context is not nil
	rpcCtx := rpc.NewInsecureTestingContext(hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper)
	lf := latencyFunc(rpcCtx)
	if lf == nil {
		t.Fatal("expected non-nil latency function for non-nil RPC context")
	}

	// Test case 2: RPC context is nil
	lf2 := latencyFunc(nil)
	if lf2 != nil {
		t.Fatal("expected nil latency function for nil RPC context")
	}
}

// TestRegisterPolicyAndNewOracleFactory tests the RegisterPolicy and NewOracleFactory functions
func TestRegisterPolicyAndNewOracleFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test registering a new policy
	customPolicy := RegisterPolicy(func(cfg Config) OracleFactory {
		return &randomOracle{gossip: cfg.Gossip}
	})

	// Test creating an OracleFactory with the new policy
	cfg := Config{}
	of := NewOracleFactory(customPolicy, cfg)
	if of == nil {
		t.Fatal("expected non-nil OracleFactory")
	}

	// Test creating an OracleFactory with an invalid policy (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid policy")
		}
	}()
	_ = NewOracleFactory(Policy(255), cfg)
}
