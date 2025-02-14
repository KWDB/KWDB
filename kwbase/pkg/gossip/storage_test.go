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

package gossip_test

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/resolver"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/simulation"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/pkg/errors"
)

type testStorage struct {
	syncutil.Mutex
	read, write bool
	info        gossip.BootstrapInfo
}

func (ts *testStorage) isRead() bool {
	ts.Lock()
	defer ts.Unlock()
	return ts.read
}

func (ts *testStorage) isWrite() bool {
	ts.Lock()
	defer ts.Unlock()
	return ts.write
}

func (ts *testStorage) Info() gossip.BootstrapInfo {
	ts.Lock()
	defer ts.Unlock()
	return ts.info
}

func (ts *testStorage) Len() int {
	ts.Lock()
	defer ts.Unlock()
	return len(ts.info.Addresses)
}

func (ts *testStorage) ReadBootstrapInfo(info *gossip.BootstrapInfo) error {
	ts.Lock()
	defer ts.Unlock()
	ts.read = true
	*info = *protoutil.Clone(&ts.info).(*gossip.BootstrapInfo)
	return nil
}

func (ts *testStorage) WriteBootstrapInfo(info *gossip.BootstrapInfo) error {
	ts.Lock()
	defer ts.Unlock()
	ts.write = true
	ts.info = *protoutil.Clone(info).(*gossip.BootstrapInfo)
	return nil
}

type unresolvedAddrSlice []util.UnresolvedAddr

func (s unresolvedAddrSlice) Len() int {
	return len(s)
}
func (s unresolvedAddrSlice) Less(i, j int) bool {
	networkCmp := strings.Compare(s[i].Network(), s[j].Network())
	return networkCmp < 0 || networkCmp == 0 && strings.Compare(s[i].String(), s[j].String()) < 0
}
func (s unresolvedAddrSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// TestGossipStorage verifies that a gossip node can join the cluster
// using the bootstrap hosts in a gossip.Storage object.
func TestGossipStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	defaultZoneConfig := zonepb.DefaultZoneConfigRef()
	network := simulation.NewNetwork(stopper, 3, true, defaultZoneConfig)

	// Set storage for each of the nodes.
	addresses := make(unresolvedAddrSlice, len(network.Nodes))
	stores := make([]testStorage, len(network.Nodes))
	for i, n := range network.Nodes {
		addresses[i] = util.MakeUnresolvedAddr(n.Addr().Network(), n.Addr().String())
		if err := n.Gossip.SetStorage(&stores[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the gossip network to connect.
	network.RunUntilFullyConnected()

	// Wait long enough for storage to get the expected number of addresses.
	testutils.SucceedsSoon(t, func() error {
		for i := range stores {
			p := &stores[i]

			if expected, actual := len(network.Nodes)-1 /* -1 is ourself */, p.Len(); expected != actual {
				return errors.Errorf("expected %v, got %v (info: %#v)", expected, actual, p.Info().Addresses)
			}
		}
		return nil
	})

	for i := range stores {
		p := &stores[i]

		if !p.isRead() {
			t.Errorf("%d: expected read from storage", i)
		}
		if !p.isWrite() {
			t.Errorf("%d: expected write from storage", i)
		}

		p.Lock()
		gotAddresses := unresolvedAddrSlice(p.info.Addresses)
		sort.Sort(gotAddresses)
		var expectedAddresses unresolvedAddrSlice
		for j, addr := range addresses {
			if i != j { // skip node's own address
				expectedAddresses = append(expectedAddresses, addr)
			}
		}
		sort.Sort(expectedAddresses)

		// Verify all gossip addresses are written to each persistent store.
		if !reflect.DeepEqual(gotAddresses, expectedAddresses) {
			t.Errorf("%d: expected addresses: %s, got: %s", i, expectedAddresses, gotAddresses)
		}
		p.Unlock()
	}

	// Create an unaffiliated gossip node with only itself as a resolver,
	// leaving it no way to reach the gossip network.
	node, err := network.CreateNode(defaultZoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	node.Gossip.SetBootstrapInterval(1 * time.Millisecond)

	r, err := resolver.NewResolverFromAddress(node.Addr())
	if err != nil {
		t.Fatal(err)
	}
	node.Resolvers = []resolver.Resolver{r}
	if err := network.StartNode(node); err != nil {
		t.Fatal(err)
	}

	// Wait for a bit to ensure no connection.
	select {
	case <-time.After(10 * time.Millisecond):
		// expected outcome...
	case <-node.Gossip.Connected:
		t.Fatal("unexpectedly connected to gossip")
	}

	// Give the new node storage with info established from a node
	// in the established network.
	var ts2 testStorage
	if err := stores[0].ReadBootstrapInfo(&ts2.info); err != nil {
		t.Fatal(err)
	}
	if err := node.Gossip.SetStorage(&ts2); err != nil {
		t.Fatal(err)
	}

	network.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		if cycle > 1000 {
			t.Fatal("failed to connect to gossip")
		}
		select {
		case <-node.Gossip.Connected:
			return false
		default:
			return true
		}
	})

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := len(network.Nodes)-1 /* -1 is ourself */, ts2.Len(); expected != actual {
			return errors.Errorf("expected %v, got %v (info: %#v)", expected, actual, ts2.Info().Addresses)
		}
		return nil
	})
}

// TestGossipStorageCleanup verifies that bad resolvers are purged
// from the bootstrap info after gossip has successfully connected.
func TestGossipStorageCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	const numNodes = 3
	network := simulation.NewNetwork(stopper, numNodes, false, zonepb.DefaultZoneConfigRef())

	const notReachableAddr = "localhost:0"
	const invalidAddr = "10.0.0.1000:3333333"
	// Set storage for each of the nodes.
	addresses := make(unresolvedAddrSlice, len(network.Nodes))
	stores := make([]testStorage, len(network.Nodes))
	for i, n := range network.Nodes {
		addresses[i] = util.MakeUnresolvedAddr(n.Addr().Network(), n.Addr().String())
		// Pre-add an invalid address to each gossip storage.
		if err := stores[i].WriteBootstrapInfo(&gossip.BootstrapInfo{
			Addresses: []util.UnresolvedAddr{
				util.MakeUnresolvedAddr("tcp", network.Nodes[(i+1)%numNodes].Addr().String()), // node i+1 address
				util.MakeUnresolvedAddr("tcp", notReachableAddr),                              // unreachable address
				util.MakeUnresolvedAddr("tcp", invalidAddr),                                   // invalid address
			},
		}); err != nil {
			t.Fatal(err)
		}
		if err := n.Gossip.SetStorage(&stores[i]); err != nil {
			t.Fatal(err)
		}
		n.Gossip.SetStallInterval(1 * time.Millisecond)
		n.Gossip.SetBootstrapInterval(1 * time.Millisecond)
	}

	// Wait for the gossip network to connect.
	network.RunUntilFullyConnected()

	// Let the gossip network continue running in the background without the
	// simulation cycler preventing it from operating.
	for _, node := range network.Nodes {
		node.Gossip.EnableSimulationCycler(false)
	}

	// Wait long enough for storage to get the expected number of
	// addresses and no pending cleanups.
	testutils.SucceedsSoon(t, func() error {
		for i := range stores {
			p := &stores[i]
			if expected, actual := len(network.Nodes)-1 /* -1 is ourself */, p.Len(); expected != actual {
				return errors.Errorf("expected %v, got %v (info: %#v)", expected, actual, p.Info().Addresses)
			}
			for _, addr := range p.Info().Addresses {
				if addr.String() == invalidAddr {
					return errors.Errorf("n%d still needs bootstrap cleanup", i)
				}
			}
		}
		return nil
	})
}
