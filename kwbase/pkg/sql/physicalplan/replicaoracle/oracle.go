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

// Package replicaoracle provides functionality for physicalplan to choose a
// replica for a range.
package replicaoracle

import (
	"context"
	"math"
	"math/rand"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// Policy determines how an Oracle should select a replica.
type Policy byte

var (
	// RandomChoice chooses lease replicas randomly.
	RandomChoice = RegisterPolicy(newRandomOracleFactory)
	// BinPackingChoice bin-packs the choices.
	BinPackingChoice = RegisterPolicy(newBinPackingOracleFactory)
	// ClosestChoice chooses the node closest to the current node.
	ClosestChoice = RegisterPolicy(newClosestOracleFactory)
)

// Config is used to construct an OracleFactory.
type Config struct {
	NodeDesc         roachpb.NodeDescriptor
	Settings         *cluster.Settings
	Gossip           *gossip.Gossip
	RPCContext       *rpc.Context
	LeaseHolderCache *kvcoord.LeaseHolderCache
}

// Oracle is used to choose the lease holder for ranges. This
// interface was extracted so we can experiment with different choosing
// policies.
// Note that choices that start out random can act as self-fulfilling prophecies
// - if there's no active lease, the node that will be asked to execute part of
// the query (the chosen node) will acquire a new lease.
type Oracle interface {
	// ChoosePreferredReplica returns a choice for one range. Implementors are
	// free to use the queryState param, which has info about the number of	ranges
	// already handled by each node for the current SQL query. The state is not
	// updated with the result of this method; the caller is in charge of  that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	ChoosePreferredReplica(
		context.Context, roachpb.RangeDescriptor, QueryState,
	) (kvcoord.ReplicaInfo, error)
}

// OracleFactory creates an oracle for a Txn.
type OracleFactory interface {
	Oracle(*kv.Txn) Oracle
}

// OracleFactoryFunc creates an OracleFactory from a Config.
type OracleFactoryFunc func(Config) OracleFactory

// NewOracleFactory creates an oracle with the given policy.
func NewOracleFactory(policy Policy, cfg Config) OracleFactory {
	ff, ok := oracleFactoryFuncs[policy]
	if !ok {
		panic(errors.Errorf("unknown Policy %v", policy))
	}
	return ff(cfg)
}

// RegisterPolicy creates a new policy given a function which constructs an
// OracleFactory. RegisterPolicy is intended to be called only during init and
// is not safe for concurrent use.
func RegisterPolicy(f OracleFactoryFunc) Policy {
	if len(oracleFactoryFuncs) == 255 {
		panic("Can only register 255 Policy instances")
	}
	r := Policy(len(oracleFactoryFuncs))
	oracleFactoryFuncs[r] = f
	return r
}

var oracleFactoryFuncs = map[Policy]OracleFactoryFunc{}

// QueryState encapsulates the history of assignments of ranges to nodes
// done by an oracle on behalf of one particular query.
type QueryState struct {
	RangesPerNode  map[roachpb.NodeID]int
	AssignedRanges map[roachpb.RangeID]kvcoord.ReplicaInfo
}

// MakeQueryState creates an initialized QueryState.
func MakeQueryState() QueryState {
	return QueryState{
		RangesPerNode:  make(map[roachpb.NodeID]int),
		AssignedRanges: make(map[roachpb.RangeID]kvcoord.ReplicaInfo),
	}
}

// randomOracle is a Oracle that chooses the lease holder randomly
// among the replicas in a range descriptor.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ OracleFactory = &randomOracle{}

func newRandomOracleFactory(cfg Config) OracleFactory {
	return &randomOracle{gossip: cfg.Gossip}
}

func (o *randomOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *randomOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, _ QueryState,
) (kvcoord.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kvcoord.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

type closestOracle struct {
	gossip      *gossip.Gossip
	latencyFunc kvcoord.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newClosestOracleFactory(cfg Config) OracleFactory {
	return &closestOracle{
		latencyFunc: latencyFunc(cfg.RPCContext),
		gossip:      cfg.Gossip,
		nodeDesc:    cfg.NodeDesc,
	}
}

func (o *closestOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *closestOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kvcoord.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kvcoord.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)
	return replicas[0], nil
}

// maxPreferredRangesPerLeaseHolder applies to the binPackingOracle.
// When choosing lease holders, we try to choose the same node for all the
// ranges applicable, until we hit this limit. The rationale is that maybe a
// bunch of those ranges don't have an active lease, so our choice is going to
// be self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// binPackingOracle coalesces choices together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	leaseHolderCache                 *kvcoord.LeaseHolderCache
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	latencyFunc                      kvcoord.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newBinPackingOracleFactory(cfg Config) OracleFactory {
	return &binPackingOracle{
		maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
		gossip:                           cfg.Gossip,
		nodeDesc:                         cfg.NodeDesc,
		leaseHolderCache:                 cfg.LeaseHolderCache,
		latencyFunc:                      latencyFunc(cfg.RPCContext),
	}
}

var _ OracleFactory = &binPackingOracle{}

func (o *binPackingOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *binPackingOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kvcoord.ReplicaInfo, error) {
	// Attempt to find a cached lease holder and use it if found.
	// If an error occurs, ignore it and proceed to choose a replica below.
	if storeID, ok := o.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
		var repl kvcoord.ReplicaInfo
		repl.ReplicaDescriptor = roachpb.ReplicaDescriptor{StoreID: storeID}
		// Fill in the node descriptor.
		nodeID, err := o.gossip.GetNodeIDForStoreID(storeID)
		if err != nil {
			log.VEventf(ctx, 2, "failed to lookup store %d: %s", storeID, err)
		} else if nd, err := o.gossip.GetNodeDescriptor(nodeID); err != nil {
			log.VEventf(ctx, 2, "failed to resolve node %d: %s", nodeID, err)
		} else {
			repl.ReplicaDescriptor.NodeID = nodeID
			repl.NodeDesc = nd
			return repl, nil
		}
	}

	// If we've assigned the range before, return that assignment.
	if repl, ok := queryState.AssignedRanges[desc.RangeID]; ok {
		return repl, nil
	}

	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kvcoord.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)

	// Use helper to select a replica according to bin-packing rules.
	return o.selectReplicaFromList(replicas, queryState), nil
}

// selectReplicaFromList encapsulates the logic to pick a replica according
// to bin-packing preferences: prefer already-assigned but not-full replicas,
// otherwise pick the least-loaded replica.
func (o *binPackingOracle) selectReplicaFromList(
	replicas kvcoord.ReplicaSlice, queryState QueryState,
) kvcoord.ReplicaInfo {
	minLoad := int(math.MaxInt32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := queryState.RangesPerNode[repl.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repl
		}
		if assignedRanges < minLoad {
			leastLoadedIdx = i
			minLoad = assignedRanges
		}
	}
	return replicas[leastLoadedIdx]
}

// replicaSliceOrErr returns a ReplicaSlice for the given range descriptor.
// ReplicaSlices are restricted to replicas on nodes for which a NodeDescriptor
// is available in gossip. If no nodes are available, a RangeUnavailableError is
// returned.
func replicaSliceOrErr(
	desc roachpb.RangeDescriptor, gsp *gossip.Gossip,
) (kvcoord.ReplicaSlice, error) {
	// Learner replicas won't serve reads/writes, so send only to the `Voters`
	// replicas. This is just an optimization to save a network hop, everything
	// would still work if we had `All` here.
	voterReplicas := desc.Replicas().Voters()
	replicas := kvcoord.NewReplicaSlice(gsp, voterReplicas)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range voterReplicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return kvcoord.ReplicaSlice{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, errors.Errorf("node info not available in gossip"), nodeIDs...)
	}
	return replicas, nil
}

// latencyFunc returns a kv.LatencyFunc for use with
// Replicas.OptimizeReplicaOrder.
func latencyFunc(rpcCtx *rpc.Context) kvcoord.LatencyFunc {
	if rpcCtx != nil {
		return rpcCtx.RemoteClocks.Latency
	}
	return nil
}
