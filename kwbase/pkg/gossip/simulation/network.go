// Copyright 2014 The Cockroach Authors.
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

package simulation

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/resolver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"google.golang.org/grpc"
)

// Node represents a node used in a Network. It includes information
// about the node's gossip instance, network address, and underlying
// server.
type Node struct {
	Gossip    *gossip.Gossip
	Server    *grpc.Server
	Listener  net.Listener
	Registry  *metric.Registry
	Resolvers []resolver.Resolver
}

// Addr returns the address of the connected listener.
func (n *Node) Addr() net.Addr {
	return n.Listener.Addr()
}

// Network provides access to a test gossip network of nodes.
type Network struct {
	Nodes           []*Node
	Stopper         *stop.Stopper
	RPCContext      *rpc.Context
	nodeIDAllocator roachpb.NodeID // provides unique node IDs
	tlsConfig       *tls.Config
	started         bool
}

// NewNetwork creates nodeCount gossip nodes.
func NewNetwork(
	stopper *stop.Stopper, nodeCount int, createResolvers bool, defaultZoneConfig *zonepb.ZoneConfig,
) *Network {
	log.Infof(context.TODO(), "simulating gossip network with %d nodes", nodeCount)

	n := &Network{
		Nodes:   []*Node{},
		Stopper: stopper,
	}
	n.RPCContext = rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		n.Stopper,
		cluster.MakeTestingClusterSettings(),
	)
	var err error
	n.tlsConfig, err = n.RPCContext.GetServerTLSConfig()
	if err != nil {
		log.Fatal(context.TODO(), err)
	}

	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	n.RPCContext.ClusterID.Set(context.TODO(), uuid.MakeV4())

	for i := 0; i < nodeCount; i++ {
		node, err := n.CreateNode(defaultZoneConfig)
		if err != nil {
			log.Fatal(context.TODO(), err)
		}
		// Build a resolver for each instance or we'll get data races.
		if createResolvers {
			r, err := resolver.NewResolverFromAddress(n.Nodes[0].Addr())
			if err != nil {
				log.Fatalf(context.TODO(), "bad gossip address %s: %s", n.Nodes[0].Addr(), err)
			}
			node.Resolvers = []resolver.Resolver{r}
		}
	}
	return n
}

// CreateNode creates a simulation node and starts an RPC server for it.
func (n *Network) CreateNode(defaultZoneConfig *zonepb.ZoneConfig) (*Node, error) {
	server := rpc.NewServer(n.RPCContext)
	ln, err := net.Listen(util.IsolatedTestAddr.Network(), util.IsolatedTestAddr.String())
	if err != nil {
		return nil, err
	}
	node := &Node{Server: server, Listener: ln, Registry: metric.NewRegistry()}
	node.Gossip = gossip.NewTest(0, n.RPCContext, server, n.Stopper, node.Registry, defaultZoneConfig)
	n.Stopper.RunWorker(context.TODO(), func(context.Context) {
		<-n.Stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		<-n.Stopper.ShouldStop()
		server.Stop()
		node.Gossip.EnableSimulationCycler(false)
	})
	n.Nodes = append(n.Nodes, node)
	return node, nil
}

// StartNode initializes a gossip instance for the simulation node and
// starts it.
func (n *Network) StartNode(node *Node) error {
	node.Gossip.Start(node.Addr(), node.Resolvers)
	node.Gossip.EnableSimulationCycler(true)
	n.nodeIDAllocator++
	node.Gossip.NodeID.Set(context.TODO(), n.nodeIDAllocator)
	if err := node.Gossip.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  node.Gossip.NodeID.Get(),
		Address: util.MakeUnresolvedAddr(node.Addr().Network(), node.Addr().String()),
	}); err != nil {
		return err
	}
	if err := node.Gossip.AddInfo(node.Addr().String(),
		encoding.EncodeUint64Ascending(nil, 0), time.Hour); err != nil {
		return err
	}
	n.Stopper.RunWorker(context.TODO(), func(context.Context) {
		netutil.FatalIfUnexpected(node.Server.Serve(node.Listener))
	})
	return nil
}

// GetNodeFromID returns the simulation node associated with
// provided node ID, or nil if there is no such node.
func (n *Network) GetNodeFromID(nodeID roachpb.NodeID) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Gossip.NodeID.Get() == nodeID {
			return node, true
		}
	}
	return nil, false
}

// SimulateNetwork runs until the simCallback returns false.
//
// At each cycle, every node gossips a key equal to its address (unique)
// with the cycle as the value. The received cycle value can be used
// to determine the aging of information between any two nodes in the
// network.
//
// At each cycle of the simulation, node 0 gossips the sentinel.
//
// The simulation callback receives the cycle and the network as arguments.
func (n *Network) SimulateNetwork(simCallback func(cycle int, network *Network) bool) {
	n.Start()
	nodes := n.Nodes
	for cycle := 1; ; cycle++ {
		// Node 0 gossips sentinel & cluster ID every cycle.
		if err := nodes[0].Gossip.AddInfo(
			gossip.KeySentinel,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)),
			time.Hour,
		); err != nil {
			log.Fatal(context.TODO(), err)
		}
		if err := nodes[0].Gossip.AddInfo(
			gossip.KeyClusterID,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)),
			0*time.Second,
		); err != nil {
			log.Fatal(context.TODO(), err)
		}
		// Every node gossips every cycle.
		for _, node := range nodes {
			if err := node.Gossip.AddInfo(
				node.Addr().String(),
				encoding.EncodeUint64Ascending(nil, uint64(cycle)),
				time.Hour,
			); err != nil {
				log.Fatal(context.TODO(), err)
			}
			node.Gossip.SimulationCycle()
		}
		// If the simCallback returns false, we're done with the
		// simulation; exit the loop. This condition is tested here
		// instead of in the for statement in order to guarantee
		// we run at least one iteration of this loop in order to
		// gossip the cluster ID and sentinel.
		if !simCallback(cycle, n) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Infof(context.TODO(), "gossip network simulation: total infos sent=%d, received=%d", n.infosSent(), n.infosReceived())
}

// Start starts all gossip nodes.
// TODO(spencer): make all methods in Network return errors instead of
// fatal logging.
func (n *Network) Start() {
	if n.started {
		return
	}
	n.started = true
	for _, node := range n.Nodes {
		if err := n.StartNode(node); err != nil {
			log.Fatal(context.TODO(), err)
		}
	}
}

// RunUntilFullyConnected blocks until the gossip network has received
// gossip from every other node in the network. It returns the gossip
// cycle at which the network became fully connected.
func (n *Network) RunUntilFullyConnected() int {
	var connectedAtCycle int
	n.SimulateNetwork(func(cycle int, network *Network) bool {
		if network.IsNetworkConnected() {
			connectedAtCycle = cycle
			return false
		}
		return true
	})
	return connectedAtCycle
}

// IsNetworkConnected returns true if the network is fully connected
// with no partitions (i.e. every node knows every other node's
// network address).
func (n *Network) IsNetworkConnected() bool {
	for _, leftNode := range n.Nodes {
		for _, rightNode := range n.Nodes {
			if _, err := leftNode.Gossip.GetInfo(gossip.MakeNodeIDKey(rightNode.Gossip.NodeID.Get())); err != nil {
				return false
			}
		}
	}
	return true
}

// infosSent returns the total count of infos sent from all nodes in
// the network.
func (n *Network) infosSent() int {
	var count int64
	for _, node := range n.Nodes {
		count += node.Gossip.GetNodeMetrics().InfosSent.Counter.Count()
	}
	return int(count)
}

// infosReceived returns the total count of infos received from all
// nodes in the network.
func (n *Network) infosReceived() int {
	var count int64
	for _, node := range n.Nodes {
		count += node.Gossip.GetNodeMetrics().InfosReceived.Counter.Count()
	}
	return int(count)
}
