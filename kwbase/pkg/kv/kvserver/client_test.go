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

/*
	Package storage_test provides a means of testing store

functionality which depends on a fully-functional KV client. This
cannot be done within the storage package because of circular
dependencies.

By convention, tests in package storage_test have names of the form
client_*.go.
*/
package kvserver_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/resolver"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/rpc/nodedialer"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cenkalti/backoff"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
)

// createTestStore creates a test store using an in-memory
// engine.
func createTestStore(t testing.TB, stopper *stop.Stopper) (*kvserver.Store, *hlc.ManualClock) {
	manual := hlc.NewManualClock(123)
	cfg := kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	store := createTestStoreWithOpts(t, testStoreOpts{cfg: &cfg}, stopper)
	return store, manual
}

// DEPRECATED. Use createTestStoreWithOpts().
func createTestStoreWithConfig(
	t testing.TB, stopper *stop.Stopper, storeCfg kvserver.StoreConfig,
) *kvserver.Store {
	store := createTestStoreWithOpts(t,
		testStoreOpts{
			cfg: &storeCfg,
		},
		stopper,
	)
	return store
}

// testStoreOpts affords control over aspects of store creation.
type testStoreOpts struct {
	// dontBootstrap, if set, means that the engine will not be bootstrapped.
	dontBootstrap bool
	// dontCreateSystemRanges is relevant only if dontBootstrap is not set.
	// If set, the store will have a single range. If not set, the store will have
	// all the system ranges that are generally created for a cluster at boostrap.
	dontCreateSystemRanges bool

	cfg *kvserver.StoreConfig
	eng storage.Engine
}

// createTestStoreWithOpts creates a test store using the given engine and clock.
// TestStoreConfig() can be used for creating a config suitable for most
// tests.
func createTestStoreWithOpts(
	t testing.TB, opts testStoreOpts, stopper *stop.Stopper,
) *kvserver.Store {
	var storeCfg kvserver.StoreConfig
	if opts.cfg == nil {
		manual := hlc.NewManualClock(123)
		storeCfg = kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	} else {
		storeCfg = *opts.cfg
	}
	eng := opts.eng
	if eng == nil {
		eng = storage.NewDefaultInMem()
		stopper.AddCloser(eng)
	}

	tracer := storeCfg.Settings.Tracer
	ac := log.AmbientContext{Tracer: tracer}
	storeCfg.AmbientCtx = ac

	rpcContext := rpc.NewContext(
		ac, &base.Config{Insecure: true}, storeCfg.Clock, stopper, storeCfg.Settings)
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rpcContext.ClusterID.Set(context.TODO(), uuid.MakeV4())
	nodeDesc := &roachpb.NodeDescriptor{
		NodeID:  1,
		Address: util.MakeUnresolvedAddr("tcp", "invalid.invalid:26257"),
	}
	server := rpc.NewServer(rpcContext) // never started
	storeCfg.Gossip = gossip.NewTest(
		nodeDesc.NodeID, rpcContext, server, stopper, metric.NewRegistry(), storeCfg.DefaultZoneConfig,
	)
	storeCfg.ScanMaxIdleTime = 1 * time.Second
	stores := kvserver.NewStores(
		ac, storeCfg.Clock,
		clusterversion.TestingBinaryVersion, clusterversion.TestingBinaryMinSupportedVersion)

	if err := storeCfg.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatal(err)
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSender := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx: ac,
		Clock:      storeCfg.Clock,
		Settings:   storeCfg.Settings,
		RPCContext: rpcContext,
		TestingKnobs: kvcoord.ClientTestingKnobs{
			TransportFactory: kvcoord.SenderTransportFactory(tracer, stores),
		},
		RPCRetryOptions: &retryOpts,
	}, storeCfg.Gossip)

	tcsFactory := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ac,
			Settings:   storeCfg.Settings,
			Clock:      storeCfg.Clock,
			Stopper:    stopper,
		},
		distSender,
	)
	storeCfg.DB = kv.NewDB(ac, tcsFactory, storeCfg.Clock)
	storeCfg.StorePool = kvserver.NewTestStorePool(storeCfg)
	storeCfg.Transport = kvserver.NewDummyRaftTransport(storeCfg.Settings)
	// TODO(bdarnell): arrange to have the transport closed.
	ctx := context.Background()
	if !opts.dontBootstrap {
		if err := kvserver.InitEngine(
			ctx, eng, roachpb.StoreIdent{NodeID: 1, StoreID: 1}, clusterversion.TestingClusterVersion,
		); err != nil {
			t.Fatal(err)
		}
	}
	store := kvserver.NewStore(ctx, storeCfg, eng, nodeDesc)
	if !opts.dontBootstrap {
		var kvs []roachpb.KeyValue
		var splits []roachpb.RKey
		bootstrapVersion := clusterversion.TestingClusterVersion
		kvs, tableSplits := sqlbase.MakeMetadataSchema(storeCfg.DefaultZoneConfig, storeCfg.DefaultSystemZoneConfig).GetInitialValues(bootstrapVersion)
		if !opts.dontCreateSystemRanges {
			splits = config.StaticSplits()
			splits = append(splits, tableSplits...)
			sort.Slice(splits, func(i, j int) bool {
				return splits[i].Less(splits[j])
			})
		}
		err := kvserver.WriteInitialClusterData(
			ctx,
			eng,
			kvs, /* initialValues */
			clusterversion.TestingBinaryVersion,
			1 /* numStores */, splits, storeCfg.Clock.PhysicalNow())
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(ctx, stopper, nil); err != nil {
		t.Fatal(err)
	}
	stores.AddStore(store)

	// Connect to gossip and gossip the store's capacity.
	<-store.Gossip().Connected
	if err := store.GossipStore(ctx, false /* useCached */); err != nil {
		t.Fatal(err)
	}
	// Wait for the store's single range to have quorum before proceeding.
	repl := store.LookupReplica(roachpb.RKeyMin)

	// Send a request through the range to make sure everything is warmed up
	// and works.
	// NB: it's unclear if this code is necessary.
	var ba roachpb.BatchRequest
	get := roachpb.GetRequest{}
	get.Key = keys.LocalMax
	ba.Header.Replica = repl.Desc().Replicas().Voters()[0]
	ba.Header.RangeID = repl.RangeID
	ba.Add(&get)
	_, pErr := store.Send(ctx, ba)
	require.NoError(t, pErr.GoError())

	// Wait for the system config to be available in gossip. All sorts of things
	// might not work properly while the system config is not available.
	testutils.SucceedsSoon(t, func() error {
		if cfg := store.Gossip().GetSystemConfig(); cfg == nil {
			return errors.Errorf("system config not available in gossip yet")
		}
		return nil
	})

	// Make all the initial ranges part of replication queue purgatory. This is
	// similar to what a real cluster does after bootstrap - we want the initial
	// ranges to up-replicate as soon as other nodes join.
	if err := store.ForceReplicationScanAndProcess(); err != nil {
		t.Fatal(err)
	}

	return store
}

type multiTestContext struct {
	t           testing.TB
	storeConfig *kvserver.StoreConfig
	manualClock *hlc.ManualClock
	rpcContext  *rpc.Context
	// rpcTestingKnobs are optional configuration for the rpcContext.
	rpcTestingKnobs rpc.ContextTestingKnobs

	// By default, a multiTestContext starts with a bunch of system ranges, just
	// like a regular Server after bootstrap. If startWithSingleRange is set,
	// we'll start with a single range spanning all the key space. The split
	// queue, if not disabled, might then create other range system ranges.
	startWithSingleRange bool

	nodeIDtoAddrMu struct {
		*syncutil.RWMutex
		nodeIDtoAddr map[roachpb.NodeID]net.Addr
	}

	nodeDialer *nodedialer.Dialer
	transport  *kvserver.RaftTransport

	// The per-store clocks slice normally contains aliases of
	// multiTestContext.clock, but it may be populated before Start() to
	// use distinct clocks per store.
	clocks      []*hlc.Clock
	engines     []storage.Engine
	grpcServers []*grpc.Server
	distSenders []*kvcoord.DistSender
	dbs         []*kv.DB
	gossips     []*gossip.Gossip
	storePools  []*kvserver.StorePool
	// We use multiple stoppers so we can restart different parts of the
	// test individually. transportStopper is for 'transport', and the
	// 'stoppers' slice corresponds to the 'stores'.
	transportStopper *stop.Stopper
	engineStoppers   []*stop.Stopper

	// The fields below may mutate at runtime so the pointers they contain are
	// protected by 'mu'.
	mu             *syncutil.RWMutex
	senders        []*kvserver.Stores
	stores         []*kvserver.Store
	stoppers       []*stop.Stopper
	idents         []roachpb.StoreIdent
	nodeLivenesses []*kvserver.NodeLiveness
}

func (m *multiTestContext) getNodeIDAddress(nodeID roachpb.NodeID) (net.Addr, error) {
	m.nodeIDtoAddrMu.RLock()
	addr, ok := m.nodeIDtoAddrMu.nodeIDtoAddr[nodeID]
	m.nodeIDtoAddrMu.RUnlock()
	if ok {
		return addr, nil
	}
	return nil, errors.Errorf("unknown peer %d", nodeID)
}

func (m *multiTestContext) Start(t testing.TB, numStores int) {
	{
		// Only the fields we nil out below can be injected into m as it
		// starts up, so fail early if anything else was set (as we'd likely
		// override it and the test wouldn't get what it wanted).
		mCopy := *m
		mCopy.storeConfig = nil
		mCopy.clocks = nil
		mCopy.engines = nil
		mCopy.engineStoppers = nil
		mCopy.startWithSingleRange = false
		mCopy.rpcTestingKnobs = rpc.ContextTestingKnobs{}
		var empty multiTestContext
		if !reflect.DeepEqual(empty, mCopy) {
			t.Fatalf("illegal fields set in multiTestContext:\n%s", pretty.Diff(empty, mCopy))
		}
	}

	m.t = t

	m.nodeIDtoAddrMu.RWMutex = &syncutil.RWMutex{}
	m.mu = &syncutil.RWMutex{}
	m.stores = make([]*kvserver.Store, numStores)
	m.storePools = make([]*kvserver.StorePool, numStores)
	m.distSenders = make([]*kvcoord.DistSender, numStores)
	m.dbs = make([]*kv.DB, numStores)
	m.stoppers = make([]*stop.Stopper, numStores)
	m.senders = make([]*kvserver.Stores, numStores)
	m.idents = make([]roachpb.StoreIdent, numStores)
	m.grpcServers = make([]*grpc.Server, numStores)
	m.gossips = make([]*gossip.Gossip, numStores)
	m.nodeLivenesses = make([]*kvserver.NodeLiveness, numStores)

	if m.storeConfig != nil && m.storeConfig.Clock != nil {
		require.Nil(t, m.manualClock, "can't use manual clock; storeConfig.Clock is set")
		require.Empty(t, m.clocks, "can't populate .clocks; storeConfig.Clock is set")
		m.clocks = []*hlc.Clock{m.storeConfig.Clock}
	} else if len(m.clocks) == 0 {
		if m.manualClock == nil {
			m.manualClock = hlc.NewManualClock(123)
		}
		m.clocks = []*hlc.Clock{hlc.NewClock(m.manualClock.UnixNano, time.Nanosecond)}
	}

	if m.storeConfig != nil {
		// Either they're equal, or the left is initially nil (see the golf
		// above).
		m.storeConfig.Clock = m.clocks[0]
	}

	if m.transportStopper == nil {
		m.transportStopper = stop.NewStopper()
	}
	st := cluster.MakeTestingClusterSettings()
	if m.rpcContext == nil {
		m.rpcContext = rpc.NewContextWithTestingKnobs(log.AmbientContext{Tracer: st.Tracer}, &base.Config{Insecure: true}, m.clock(),
			m.transportStopper, st, m.rpcTestingKnobs)
		// Ensure that tests using this test context and restart/shut down
		// their servers do not inadvertently start talking to servers from
		// unrelated concurrent tests.
		m.rpcContext.ClusterID.Set(context.TODO(), uuid.MakeV4())
		// We are sharing the same RPC context for all simulated nodes, so we can't enforce
		// some of the RPC check validation.
		m.rpcContext.TestingAllowNamedRPCToAnonymousServer = true

		// Create a breaker which never trips and never backs off to avoid
		// introducing timing-based flakes.
		m.rpcContext.BreakerFactory = func() *circuit.Breaker {
			return circuit.NewBreakerWithOptions(&circuit.Options{
				BackOff: &backoff.ZeroBackOff{},
			})
		}
	}
	m.nodeDialer = nodedialer.New(m.rpcContext, m.getNodeIDAddress)
	m.transport = kvserver.NewRaftTransport(
		log.AmbientContext{Tracer: st.Tracer}, st,
		m.nodeDialer, nil, m.transportStopper,
	)

	for idx := 0; idx < numStores; idx++ {
		m.addStore(idx)
	}

	// Wait for gossip to startup.
	testutils.SucceedsSoon(t, func() error {
		for i, g := range m.gossips {
			if cfg := g.GetSystemConfig(); cfg == nil {
				return errors.Errorf("system config not available at index %d", i)
			}
		}
		return nil
	})
}

func (m *multiTestContext) clock() *hlc.Clock {
	return m.clocks[0]
}

func (m *multiTestContext) Stop() {
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.t.Errorf("mtc.Stop() panicked: %+v", r)
			}
		}()
		m.mu.RLock()

		// Quiesce everyone in parallel (before the transport stopper) to avoid
		// deadlocks.
		var wg sync.WaitGroup
		wg.Add(len(m.stoppers))
		for _, s := range m.stoppers {
			go func(s *stop.Stopper) {
				defer wg.Done()
				// Some Stoppers may be nil if stopStore has been called
				// without restartStore.
				if s != nil {
					// TODO(tschottdorf): seems like it *should* be possible to
					// call .Stop() directly, but then stressing essentially
					// any test (TestRaftAfterRemove is a good example) results
					// in deadlocks where a task can't finish because of
					// getting stuck in addWriteCommand.
					s.Quiesce(context.TODO())
				}
			}(s)
		}
		m.mu.RUnlock()
		wg.Wait()

		m.mu.RLock()
		defer m.mu.RUnlock()
		for _, stopper := range m.stoppers {
			if stopper != nil {
				stopper.Stop(context.TODO())
			}
		}
		m.transportStopper.Stop(context.TODO())

		for _, s := range m.engineStoppers {
			s.Stop(context.TODO())
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		// If we've already failed, just attach another failure to the
		// test, since a timeout during shutdown after a failure is
		// probably not interesting, and will prevent the display of any
		// pending t.Error. If we're timing out but the test was otherwise
		// a success, panic so we see stack traces from other goroutines.
		if m.t.Failed() {
			m.t.Error("timed out during shutdown")
		} else {
			panic("timed out during shutdown")
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.stores {
		if s != nil {
			s.AssertInvariants()
		}
	}
}

// gossipStores forces each store to gossip its store descriptor and then
// blocks until all nodes have received these updated descriptors.
func (m *multiTestContext) gossipStores() {
	timestamps := make(map[string]int64)
	for i := 0; i < len(m.stores); i++ {
		<-m.gossips[i].Connected
		if err := m.stores[i].GossipStore(context.Background(), false /* useCached */); err != nil {
			m.t.Fatal(err)
		}
		infoStatus := m.gossips[i].GetInfoStatus()
		storeKey := gossip.MakeStoreKey(m.stores[i].Ident.StoreID)
		timestamps[storeKey] = infoStatus.Infos[storeKey].OrigStamp
	}
	// Wait until all stores know about each other.
	testutils.SucceedsSoon(m.t, func() error {
		for i := 0; i < len(m.stores); i++ {
			nodeID := m.stores[i].Ident.NodeID
			infoStatus := m.gossips[i].GetInfoStatus()
			for storeKey, timestamp := range timestamps {
				info, ok := infoStatus.Infos[storeKey]
				if !ok {
					return errors.Errorf("node %d does not have a storeDesc for %s yet", nodeID, storeKey)
				}
				if info.OrigStamp < timestamp {
					return errors.Errorf("node %d's storeDesc for %s is not up to date", nodeID, storeKey)
				}
			}
		}
		return nil
	})
}

// initGossipNetwork gossips all store descriptors and waits until all
// storePools have received those descriptors.
func (m *multiTestContext) initGossipNetwork() {
	m.gossipStores()
	testutils.SucceedsSoon(m.t, func() error {
		for i := 0; i < len(m.stores); i++ {
			if _, alive, _ := m.storePools[i].GetStoreList(roachpb.RangeID(0)); alive != len(m.stores) {
				return errors.Errorf("node %d's store pool only has %d alive stores, expected %d",
					m.stores[i].Ident.NodeID, alive, len(m.stores))
			}
		}
		return nil
	})
	log.Info(context.Background(), "gossip network initialized")
}

type multiTestContextKVTransport struct {
	mtc      *multiTestContext
	idx      int
	replicas kvcoord.ReplicaSlice
	mu       struct {
		syncutil.Mutex
		pending map[roachpb.ReplicaID]struct{}
	}
}

func (m *multiTestContext) kvTransportFactory(
	_ kvcoord.SendOptions, _ *nodedialer.Dialer, replicas kvcoord.ReplicaSlice,
) (kvcoord.Transport, error) {
	t := &multiTestContextKVTransport{
		mtc:      m,
		replicas: replicas,
	}
	t.mu.pending = map[roachpb.ReplicaID]struct{}{}
	return t, nil
}

func (t *multiTestContextKVTransport) String() string {
	return fmt.Sprintf("%T: replicas=%v, idx=%d", t, t.replicas, t.idx)
}

func (t *multiTestContextKVTransport) IsExhausted() bool {
	return t.idx == len(t.replicas)
}

func (t *multiTestContextKVTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "send context is canceled")
	}
	rep := t.replicas[t.idx]
	t.idx++
	t.setPending(rep.ReplicaID, true)

	// Node IDs are assigned in the order the nodes are created by
	// the multi test context, so we can derive the index for stoppers
	// and senders by subtracting 1 from the node ID.
	nodeIndex := int(rep.NodeID) - 1
	if log.V(1) {
		log.Infof(ctx, "SendNext nodeIndex=%d", nodeIndex)
	}

	// This method crosses store boundaries: it is possible that the
	// destination store is stopped while the source is still running.
	// Run the send in a Task on the destination store to simulate what
	// would happen with real RPCs.
	t.mtc.mu.RLock()
	s := t.mtc.stoppers[nodeIndex]
	sender := t.mtc.senders[nodeIndex]
	t.mtc.mu.RUnlock()

	if s == nil {
		t.setPending(rep.ReplicaID, false)
		return nil, roachpb.NewSendError("store is stopped")
	}

	// Clone txn of ba args for sending.
	ba.Replica = rep.ReplicaDescriptor
	if txn := ba.Txn; txn != nil {
		ba.Txn = ba.Txn.Clone()
	}
	var br *roachpb.BatchResponse
	var pErr *roachpb.Error
	if err := s.RunTask(ctx, "mtc send", func(ctx context.Context) {
		br, pErr = sender.Send(ctx, ba)
	}); err != nil {
		pErr = roachpb.NewError(err)
	}
	if br == nil {
		br = &roachpb.BatchResponse{}
	}
	if br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(sender, br))
	}
	br.Error = pErr

	// On certain errors, we must expire leases to ensure that the
	// next attempt has a chance of succeeding.
	switch tErr := pErr.GetDetail().(type) {
	case *roachpb.NotLeaseHolderError:
		if leaseHolder := tErr.LeaseHolder; leaseHolder != nil {
			t.mtc.mu.RLock()
			leaseHolderStore := t.mtc.stores[leaseHolder.NodeID-1]
			t.mtc.mu.RUnlock()
			if leaseHolderStore == nil {
				// The lease holder is known but down, so expire its lease.
				if t.mtc.manualClock != nil {
					t.mtc.advanceClock(ctx)
				}
			}
		} else {
			// stores has the range, is *not* the lease holder, but the
			// lease holder is not known; this can happen if the lease
			// holder is removed from the group. Move the manual clock
			// forward in an attempt to expire the lease.
			if t.mtc.manualClock != nil {
				t.mtc.advanceClock(ctx)
			}
		}
	}
	t.setPending(rep.ReplicaID, false)
	return br, nil
}

func (t *multiTestContextKVTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
	panic("unimplemented")
}

func (t *multiTestContextKVTransport) NextReplica() roachpb.ReplicaDescriptor {
	if t.IsExhausted() {
		return roachpb.ReplicaDescriptor{}
	}
	return t.replicas[t.idx].ReplicaDescriptor
}

func (t *multiTestContextKVTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.mu.pending[replica.ReplicaID]; ok {
		return
	}
	for i := range t.replicas {
		if t.replicas[i].ReplicaDescriptor == replica {
			if i < t.idx {
				t.idx--
			}
			// Swap the client representing this replica to the front.
			t.replicas[i], t.replicas[t.idx] = t.replicas[t.idx], t.replicas[i]
			return
		}
	}
}

func (t *multiTestContextKVTransport) setPending(repID roachpb.ReplicaID, pending bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pending {
		t.mu.pending[repID] = struct{}{}
	} else {
		delete(t.mu.pending, repID)
	}
}

// rangeDescByAge implements sort.Interface for RangeDescriptor, sorting by the
// age of the RangeDescriptor. This is intended to find the most recent version
// of the same RangeDescriptor, when multiple versions of it are available.
type rangeDescByAge []*roachpb.RangeDescriptor

func (rd rangeDescByAge) Len() int      { return len(rd) }
func (rd rangeDescByAge) Swap(i, j int) { rd[i], rd[j] = rd[j], rd[i] }
func (rd rangeDescByAge) Less(i, j int) bool {
	// "Less" means "older" according to this sort.
	// A RangeDescriptor version with a higher NextReplicaID is always more recent.
	if rd[i].NextReplicaID != rd[j].NextReplicaID {
		return rd[i].NextReplicaID < rd[j].NextReplicaID
	}
	// If two RangeDescriptor versions have the same NextReplicaID, then the one
	// with the fewest replicas is the newest.
	return len(rd[i].InternalReplicas) > len(rd[j].InternalReplicas)
}

// FirstRange implements the RangeDescriptorDB interface. It returns the range
// descriptor which contains roachpb.KeyMin.
//
// DistSender's implementation of FirstRange() does not work correctly because
// the gossip network used by multiTestContext is only partially operational.
func (m *multiTestContext) FirstRange() (*roachpb.RangeDescriptor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var descs []*roachpb.RangeDescriptor
	for _, str := range m.senders {
		// Node liveness heartbeats start quickly, sometimes before the first
		// range would be available here and before we've added all ranges.
		if str == nil {
			continue
		}
		// Find every version of the RangeDescriptor for the first range by
		// querying all stores; it may not be present on all stores, but the
		// current version is guaranteed to be present on one of them as long
		// as all stores are alive.
		if err := str.VisitStores(func(s *kvserver.Store) error {
			firstRng := s.LookupReplica(roachpb.RKeyMin)
			if firstRng != nil {
				descs = append(descs, firstRng.Desc())
			}
			return nil
		}); err != nil {
			m.t.Fatalf("no error should be possible from this invocation of VisitStores, but found %s", err)
		}
	}
	if len(descs) == 0 {
		return nil, errors.New("first Range is not present on any live store in the multiTestContext")
	}
	// Sort the RangeDescriptor versions by age and return the most recent
	// version.
	sort.Sort(rangeDescByAge(descs))
	return descs[len(descs)-1], nil
}

func (m *multiTestContext) makeStoreConfig(i int) kvserver.StoreConfig {
	var cfg kvserver.StoreConfig
	if m.storeConfig != nil {
		cfg = *m.storeConfig
		cfg.Clock = m.clocks[i]
	} else {
		cfg = kvserver.TestStoreConfig(m.clocks[i])
		m.storeConfig = &cfg
	}
	cfg.NodeDialer = m.nodeDialer
	cfg.Transport = m.transport
	cfg.Gossip = m.gossips[i]
	cfg.TestingKnobs.DisableMergeQueue = true
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.ReplicateQueueAcceptsUnsplit = true
	cfg.TestingKnobs.DisableSafeReplicationChanges = true
	return cfg
}

var _ kvcoord.RangeDescriptorDB = mtcRangeDescriptorDB{}

type mtcRangeDescriptorDB struct {
	*multiTestContext
	ds **kvcoord.DistSender
}

func (mrdb mtcRangeDescriptorDB) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	return (*mrdb.ds).RangeLookup(ctx, key, useReverseScan)
}

func (m *multiTestContext) populateDB(idx int, st *cluster.Settings, stopper *stop.Stopper) {
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	ambient := m.storeConfig.AmbientCtx
	m.distSenders[idx] = kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx: ambient,
		Clock:      m.clocks[idx],
		RPCContext: m.rpcContext,
		RangeDescriptorDB: mtcRangeDescriptorDB{
			multiTestContext: m,
			ds:               &m.distSenders[idx],
		},
		Settings: st,
		TestingKnobs: kvcoord.ClientTestingKnobs{
			TransportFactory: m.kvTransportFactory,
		},
		RPCRetryOptions: &retryOpts,
	}, m.gossips[idx])
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Settings:   m.storeConfig.Settings,
			Clock:      m.clocks[idx],
			Stopper:    stopper,
		},
		m.distSenders[idx],
	)
	m.dbs[idx] = kv.NewDB(ambient, tcsFactory, m.clocks[idx])
}

func (m *multiTestContext) populateStorePool(
	idx int, cfg kvserver.StoreConfig, nodeLiveness *kvserver.NodeLiveness,
) {
	m.storePools[idx] = kvserver.NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		m.gossips[idx],
		m.clocks[idx],
		nodeLiveness.GetNodeCount,
		kvserver.MakeStorePoolNodeLivenessFunc(nodeLiveness),
		/* deterministic */ false,
	)
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore(idx int) {
	var clock *hlc.Clock
	if len(m.clocks) > idx {
		clock = m.clocks[idx]
	} else {
		clock = m.storeConfig.Clock
		m.clocks = append(m.clocks, clock)
	}
	var eng storage.Engine
	var needBootstrap bool
	if len(m.engines) > idx {
		eng = m.engines[idx]
		_, err := kvserver.ReadStoreIdent(context.Background(), eng)
		if _, notBootstrapped := err.(*kvserver.NotBootstrappedError); notBootstrapped {
			needBootstrap = true
		} else if err != nil {
			m.t.Fatal(err)
		}
	} else {
		engineStopper := stop.NewStopper()
		m.engineStoppers = append(m.engineStoppers, engineStopper)
		eng = storage.NewDefaultInMem()
		engineStopper.AddCloser(eng)
		m.engines = append(m.engines, eng)
		needBootstrap = true
	}
	grpcServer := rpc.NewServer(m.rpcContext)
	m.grpcServers[idx] = grpcServer
	kvserver.RegisterMultiRaftServer(grpcServer, m.transport)

	stopper := stop.NewStopper()

	// Give this store the first store as a resolver. We don't provide all of the
	// previous stores as resolvers as doing so can cause delays in bringing the
	// gossip network up.
	resolvers := func() []resolver.Resolver {
		m.nodeIDtoAddrMu.Lock()
		defer m.nodeIDtoAddrMu.Unlock()
		addr := m.nodeIDtoAddrMu.nodeIDtoAddr[1]
		if addr == nil {
			return nil
		}
		r, err := resolver.NewResolverFromAddress(addr)
		if err != nil {
			m.t.Fatal(err)
		}
		return []resolver.Resolver{r}
	}()
	m.gossips[idx] = gossip.NewTest(
		roachpb.NodeID(idx+1),
		m.rpcContext,
		grpcServer,
		m.transportStopper,
		metric.NewRegistry(),
		zonepb.DefaultZoneConfigRef(),
	)

	nodeID := roachpb.NodeID(idx + 1)
	cfg := m.makeStoreConfig(idx)
	ambient := log.AmbientContext{Tracer: cfg.Settings.Tracer}
	m.populateDB(idx, cfg.Settings, stopper)
	nlActive, nlRenewal := cfg.NodeLivenessDurations()
	m.nodeLivenesses[idx] = kvserver.NewNodeLiveness(
		ambient, m.clocks[idx], m.dbs[idx], m.engines, m.gossips[idx],
		nlActive, nlRenewal, cfg.Settings, metric.TestSampleInterval,
	)
	m.populateStorePool(idx, cfg, m.nodeLivenesses[idx])
	cfg.DB = m.dbs[idx]
	cfg.NodeLiveness = m.nodeLivenesses[idx]
	cfg.StorePool = m.storePools[idx]

	ctx := context.Background()
	if needBootstrap {
		if err := kvserver.InitEngine(ctx, eng, roachpb.StoreIdent{
			NodeID:  roachpb.NodeID(idx + 1),
			StoreID: roachpb.StoreID(idx + 1),
		}, clusterversion.TestingClusterVersion); err != nil {
			m.t.Fatal(err)
		}
	}
	if needBootstrap && idx == 0 {
		// Bootstrap the initial range on the first engine.
		var splits []roachpb.RKey
		bootstrapVersion := clusterversion.TestingClusterVersion
		kvs, tableSplits := sqlbase.MakeMetadataSchema(cfg.DefaultZoneConfig, cfg.DefaultSystemZoneConfig).GetInitialValues(bootstrapVersion)
		if !m.startWithSingleRange {
			splits = config.StaticSplits()
			splits = append(splits, tableSplits...)
			sort.Slice(splits, func(i, j int) bool {
				return splits[i].Less(splits[j])
			})
		}
		err := kvserver.WriteInitialClusterData(
			ctx,
			eng,
			kvs, /* initialValues */
			clusterversion.TestingBinaryVersion,
			len(m.engines), splits, cfg.Clock.PhysicalNow())
		if err != nil {
			m.t.Fatal(err)
		}
	}
	store := kvserver.NewStore(ctx, cfg, eng, &roachpb.NodeDescriptor{NodeID: nodeID})
	if err := store.Start(ctx, stopper, nil); err != nil {
		m.t.Fatal(err)
	}

	sender := kvserver.NewStores(ambient, clock,
		clusterversion.TestingBinaryVersion, clusterversion.TestingBinaryMinSupportedVersion,
	)
	sender.AddStore(store)
	perReplicaServer := kvserver.MakeServer(&roachpb.NodeDescriptor{NodeID: nodeID}, sender)
	kvserver.RegisterPerReplicaServer(grpcServer, perReplicaServer)

	ln, err := netutil.ListenAndServeGRPC(m.transportStopper, grpcServer, util.TestAddr)
	if err != nil {
		m.t.Fatal(err)
	}
	m.nodeIDtoAddrMu.Lock()
	if m.nodeIDtoAddrMu.nodeIDtoAddr == nil {
		m.nodeIDtoAddrMu.nodeIDtoAddr = make(map[roachpb.NodeID]net.Addr)
	}
	_, ok := m.nodeIDtoAddrMu.nodeIDtoAddr[nodeID]
	if !ok {
		m.nodeIDtoAddrMu.nodeIDtoAddr[nodeID] = ln.Addr()
	}
	m.nodeIDtoAddrMu.Unlock()
	if ok {
		m.t.Fatalf("node %d already listening", nodeID)
	}

	// Add newly created objects to the multiTestContext's collections.
	// (these must be populated before the store is started so that
	// FirstRange() can find the sender)
	m.mu.Lock()
	m.stores[idx] = store
	m.stoppers[idx] = stopper
	m.senders[idx] = sender
	// Save the store identities for later so we can use them in
	// replication operations even while the store is stopped.
	m.idents[idx] = *store.Ident
	m.mu.Unlock()

	// NB: On Mac OS X, we sporadically see excessively long dialing times (~15s)
	// which cause various trickle down badness in tests. To avoid every test
	// having to worry about such conditions we pre-warm the connection
	// cache. See #8440 for an example of the headaches the long dial times
	// cause.
	if _, err := m.rpcContext.GRPCDialNode(ln.Addr().String(), nodeID,
		rpc.DefaultClass).Connect(ctx); err != nil {
		m.t.Fatal(err)
	}

	m.gossips[idx].Start(ln.Addr(), resolvers)

	if err := m.gossipNodeDesc(m.gossips[idx], nodeID); err != nil {
		m.t.Fatal(err)
	}

	ran := struct {
		sync.Once
		ch chan struct{}
	}{
		ch: make(chan struct{}),
	}
	m.nodeLivenesses[idx].StartHeartbeat(ctx, stopper, func(ctx context.Context) {
		now := clock.Now()
		if err := store.WriteLastUpTimestamp(ctx, now); err != nil {
			log.Warning(ctx, err)
		}
		ran.Do(func() {
			close(ran.ch)
		})
	})

	store.WaitForInit()

	// Wait until we see the first heartbeat by waiting for the callback (which
	// fires *after* the node becomes live).
	<-ran.ch
}

func (m *multiTestContext) nodeDesc(nodeID roachpb.NodeID) *roachpb.NodeDescriptor {
	addr := m.nodeIDtoAddrMu.nodeIDtoAddr[nodeID]
	return &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
	}
}

// gossipNodeDesc adds the node descriptor to the gossip network.
// Mostly makes sure that we don't see a warning per request.
func (m *multiTestContext) gossipNodeDesc(g *gossip.Gossip, nodeID roachpb.NodeID) error {
	return g.SetNodeDescriptor(m.nodeDesc(nodeID))
}

// StopStore stops a store but leaves the engine intact.
// All stopped stores must be restarted before multiTestContext.Stop is called.
func (m *multiTestContext) stopStore(i int) {
	// Attempting to acquire a write lock here could lead to a situation in
	// which an outstanding Raft proposal would never return due to address
	// resolution calling back into the `multiTestContext` and attempting to
	// acquire a read lock while this write lock is block on another read lock
	// held by `SendNext` which in turn is waiting on that Raft proposal:
	//
	// SendNext[hold RLock] -> Raft[want RLock]
	//             ʌ               /
	//              \             v
	//             stopStore[want Lock]
	//
	// Instead, we only acquire a read lock to fetch the stopper, and are
	// careful not to hold any locks while stopping the stopper.
	m.mu.RLock()
	stopper := m.stoppers[i]
	m.mu.RUnlock()

	stopper.Stop(context.TODO())

	m.mu.Lock()
	m.stoppers[i] = nil
	// Break the transport breakers for this node so that messages sent between
	// a store stopping and that store restarting will never remain in-flight in
	// the transport and end up reaching the store. This has been the cause of
	// flakiness in the past.
	m.transport.GetCircuitBreaker(m.idents[i].NodeID, rpc.DefaultClass).Break()
	m.transport.GetCircuitBreaker(m.idents[i].NodeID, rpc.SystemClass).Break()
	m.senders[i].RemoveStore(m.stores[i])
	m.stores[i] = nil
	m.mu.Unlock()
}

// restartStore restarts a store previously stopped with StopStore. It does not
// wait for the store to successfully perform a heartbeat before returning. This
// is important for tests where a restarted store may not be able to heartbeat
// immediately.
func (m *multiTestContext) restartStoreWithoutHeartbeat(i int) {
	m.mu.Lock()
	stopper := stop.NewStopper()
	m.stoppers[i] = stopper
	cfg := m.makeStoreConfig(i)
	m.populateDB(i, m.storeConfig.Settings, stopper)
	nlActive, nlRenewal := cfg.NodeLivenessDurations()
	m.nodeLivenesses[i] = kvserver.NewNodeLiveness(
		log.AmbientContext{Tracer: m.storeConfig.Settings.Tracer}, m.clocks[i], m.dbs[i], m.engines,
		m.gossips[i], nlActive, nlRenewal, cfg.Settings, metric.TestSampleInterval,
	)
	m.populateStorePool(i, cfg, m.nodeLivenesses[i])
	cfg.DB = m.dbs[i]
	cfg.NodeLiveness = m.nodeLivenesses[i]
	cfg.StorePool = m.storePools[i]
	ctx := context.Background()
	store := kvserver.NewStore(ctx, cfg, m.engines[i], &roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i + 1)})
	m.stores[i] = store

	// Need to start the store before adding it so that the store ID is initialized.
	if err := store.Start(ctx, stopper, nil); err != nil {
		m.t.Fatal(err)
	}
	m.senders[i].AddStore(store)
	m.transport.GetCircuitBreaker(m.idents[i].NodeID, rpc.DefaultClass).Reset()
	m.transport.GetCircuitBreaker(m.idents[i].NodeID, rpc.SystemClass).Reset()
	m.mu.Unlock()
	cfg.NodeLiveness.StartHeartbeat(ctx, stopper, func(ctx context.Context) {
		now := m.clocks[i].Now()
		if err := store.WriteLastUpTimestamp(ctx, now); err != nil {
			log.Warning(ctx, err)
		}
	})
}

// restartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) restartStore(i int) {
	m.restartStoreWithoutHeartbeat(i)

	// Wait until we see the first heartbeat.
	liveness := m.nodeLivenesses[i]
	testutils.SucceedsSoon(m.t, func() error {
		if live, err := liveness.IsLive(roachpb.NodeID(i + 1)); err != nil || !live {
			return errors.New("node not live")
		}
		return nil
	})
}

func (m *multiTestContext) Store(i int) *kvserver.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stores[i]
}

// findStartKeyLocked returns the start key of the given range.
func (m *multiTestContext) findStartKeyLocked(rangeID roachpb.RangeID) roachpb.RKey {
	// We can use the first store that returns results because the start
	// key never changes.
	for _, s := range m.stores {
		rep, err := s.GetReplica(rangeID)
		if err == nil && rep.IsInitialized() {
			return rep.Desc().StartKey
		}
	}
	m.t.Fatalf("couldn't find range %s on any store", rangeID)
	return nil // unreached, but the compiler can't tell.
}

// findMemberStoreLocked finds a non-stopped Store which is a member
// of the given range.
func (m *multiTestContext) findMemberStoreLocked(desc roachpb.RangeDescriptor) *kvserver.Store {
	for _, s := range m.stores {
		if s == nil {
			// Store is stopped.
			continue
		}
		for _, r := range desc.InternalReplicas {
			if s.StoreID() == r.StoreID {
				return s
			}
		}
	}
	m.t.Fatalf("couldn't find a live member of %s", &desc)
	return nil // unreached, but the compiler can't tell.
}

// restart stops and restarts all stores but leaves the engines intact,
// so the stores should contain the same persistent storage as before.
func (m *multiTestContext) restart() {
	for i := range m.stores {
		m.stopStore(i)
	}
	for i := range m.stores {
		m.restartStore(i)
	}
}

// changeReplicas performs a ChangeReplicas operation, retrying until the
// destination store has been addded or removed. Returns the range's
// NextReplicaID, which is the ID of the newly-added replica if this is an add.
func (m *multiTestContext) changeReplicas(
	startKey roachpb.RKey, dest int, changeType roachpb.ReplicaChangeType,
) (roachpb.ReplicaID, error) {
	ctx := context.Background()

	var alreadyDoneErr string
	switch changeType {
	case roachpb.ADD_REPLICA:
		alreadyDoneErr = "unable to add replica .* which is already present"
	case roachpb.REMOVE_REPLICA:
		alreadyDoneErr = "unable to remove replica .* which is not present"
	}

	retryOpts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	}
	var desc roachpb.RangeDescriptor
	for r := retry.Start(retryOpts); r.Next(); {

		// Perform a consistent read to get the updated range descriptor (as
		// opposed to just going to one of the stores), to make sure we have
		// the effects of any previous ChangeReplicas call. By the time
		// ChangeReplicas returns the raft leader is guaranteed to have the
		// updated version, but followers are not.
		if err := m.dbs[0].GetProto(ctx, keys.RangeDescriptorKey(startKey), &desc); err != nil {
			return 0, err
		}

		_, err := m.dbs[0].AdminChangeReplicas(
			ctx, startKey.AsRawKey(),
			desc,
			roachpb.MakeReplicationChanges(
				changeType,
				roachpb.ReplicationTarget{
					NodeID:  m.idents[dest].NodeID,
					StoreID: m.idents[dest].StoreID,
				}),
		)

		if err == nil || testutils.IsError(err, alreadyDoneErr) {
			break
		}

		if _, ok := errors.Cause(err).(*roachpb.AmbiguousResultError); ok {
			// Try again after an AmbiguousResultError. If the operation
			// succeeded, then the next attempt will return alreadyDoneErr;
			// if it failed then the next attempt should succeed.
			continue
		}

		// We can't use storage.IsSnapshotError() because the original error object
		// is lost. We could make a this into a roachpb.Error but it seems overkill
		// for this one usage.
		if testutils.IsError(err, "snapshot failed: .*|descriptor changed") {
			log.Info(ctx, err)
			continue
		}
		return 0, err
	}

	return desc.NextReplicaID, nil
}

// replicateRange replicates the given range onto the given stores.
func (m *multiTestContext) replicateRange(rangeID roachpb.RangeID, dests ...int) {
	m.t.Helper()
	if err := m.replicateRangeNonFatal(rangeID, dests...); err != nil {
		m.t.Fatal(err)
	}
}

// replicateRangeNonFatal replicates the given range onto the given stores.
func (m *multiTestContext) replicateRangeNonFatal(rangeID roachpb.RangeID, dests ...int) error {
	m.mu.RLock()
	startKey := m.findStartKeyLocked(rangeID)
	m.mu.RUnlock()

	expectedReplicaIDs := make([]roachpb.ReplicaID, len(dests))
	for i, dest := range dests {
		var err error
		expectedReplicaIDs[i], err = m.changeReplicas(startKey, dest, roachpb.ADD_REPLICA)
		if err != nil {
			return err
		}
	}

	// Wait for the replication to complete on all destination nodes.
	return retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
		for i, dest := range dests {
			repl, err := m.stores[dest].GetReplica(rangeID)
			if err != nil {
				return err
			}
			repDesc, err := repl.GetReplicaDescriptor()
			if err != nil {
				return err
			}
			if e := expectedReplicaIDs[i]; repDesc.ReplicaID != e {
				return errors.Errorf("expected replica %s to have ID %d", repl, e)
			}
			if t := repDesc.GetType(); t != roachpb.VOTER_FULL {
				return errors.Errorf("expected replica %s to be a voter was %s", repl, t)
			}
			if !repl.Desc().ContainsKey(startKey) {
				return errors.Errorf("expected replica %s to contain %s", repl, startKey)
			}
		}
		return nil
	})
}

// unreplicateRange removes a replica of the range from the dest store.
func (m *multiTestContext) unreplicateRange(rangeID roachpb.RangeID, dest int) {
	m.t.Helper()
	if err := m.unreplicateRangeNonFatal(rangeID, dest); err != nil {
		m.t.Fatal(err)
	}
}

// unreplicateRangeNonFatal removes a replica of the range from the dest store.
// Returns an error rather than calling m.t.Fatal upon error.
func (m *multiTestContext) unreplicateRangeNonFatal(rangeID roachpb.RangeID, dest int) error {
	m.mu.RLock()
	startKey := m.findStartKeyLocked(rangeID)
	m.mu.RUnlock()

	_, err := m.changeReplicas(startKey, dest, roachpb.REMOVE_REPLICA)
	return err
}

// waitForUnreplicated waits until no replica exists for the specified range
// on the dest store.
func (m *multiTestContext) waitForUnreplicated(rangeID roachpb.RangeID, dest int) error {
	// Wait for the unreplications to complete on destination node.
	return retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
		_, err := m.stores[dest].GetReplica(rangeID)
		switch err.(type) {
		case nil:
			return fmt.Errorf("replica still exists on dest %d", dest)
		case *roachpb.RangeNotFoundError:
			return nil
		default:
			return err
		}
	})
}

// readIntFromEngines reads the current integer value at the given key
// from all configured engines, filling in zeros when the value is not
// found. Returns a slice of the same length as mtc.engines.
func (m *multiTestContext) readIntFromEngines(key roachpb.Key) []int64 {
	results := make([]int64, len(m.engines))
	for i, eng := range m.engines {
		val, _, err := storage.MVCCGet(context.Background(), eng, key, m.clocks[i].Now(),
			storage.MVCCGetOptions{})
		if err != nil {
			log.VEventf(context.TODO(), 1, "engine %d: error reading from key %s: %s", i, key, err)
		} else if val == nil {
			log.VEventf(context.TODO(), 1, "engine %d: missing key %s", i, key)
		} else {
			results[i], err = val.GetInt()
			if err != nil {
				log.Errorf(context.TODO(), "engine %d: error decoding %s from key %s: %+v", i, val, key, err)
			}
		}
	}
	return results
}

// waitForValuesT is like waitForValues but allows the caller to provide a
// testing.T which may differ from m.t.
func (m *multiTestContext) waitForValuesT(t testing.TB, key roachpb.Key, expected []int64) {
	t.Helper()
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://gitee.com/kwbasedb/kwbase/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}
	testutils.SucceedsSoon(t, func() error {
		actual := m.readIntFromEngines(key)
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}

// waitForValues waits up to the given duration for the integer values
// at the given key to match the expected slice (across all engines).
// Fails the test if they do not match.
func (m *multiTestContext) waitForValues(key roachpb.Key, expected []int64) {
	m.t.Helper()
	m.waitForValuesT(m.t, key, expected)
}

// transferLease transfers the lease for the given range from the source
// replica to the target replica. Assumes that the caller knows who the
// current leaseholder is.
func (m *multiTestContext) transferLease(
	ctx context.Context, rangeID roachpb.RangeID, source int, dest int,
) {
	if err := m.transferLeaseNonFatal(ctx, rangeID, source, dest); err != nil {
		m.t.Fatal(err)
	}
}

// transferLease transfers the lease for the given range from the source
// replica to the target replica. Assumes that the caller knows who the
// current leaseholder is.
// Returns an error rather than calling m.t.Fatal upon error.
func (m *multiTestContext) transferLeaseNonFatal(
	ctx context.Context, rangeID roachpb.RangeID, source int, dest int,
) error {
	live := m.stores[dest] != nil && !m.stores[dest].IsDraining()
	if !live {
		return errors.Errorf("can't transfer lease to down or draining node at index %d", dest)
	}

	// Heartbeat the liveness record of the destination node to make sure that the
	// lease we're about to transfer can be used afterwards. Otherwise, the
	// liveness record might be expired and the node is considered down, making
	// this transfer irrelevant. In particular, this can happen if the clock was
	// advanced recently, so all the liveness records (including the destination)
	// are expired. In that case, the simple fact that the transfer succeeded
	// doesn't mean that the destination now has a usable lease.
	if err := m.heartbeatLiveness(ctx, dest); err != nil {
		return err
	}

	sourceRepl, err := m.stores[source].GetReplica(rangeID)
	if err != nil {
		return err
	}
	if err := sourceRepl.AdminTransferLease(ctx, m.idents[dest].StoreID); err != nil {
		return err
	}

	return nil
}

func (m *multiTestContext) heartbeatLiveness(ctx context.Context, store int) error {
	m.mu.RLock()
	nl := m.nodeLivenesses[store]
	m.mu.RUnlock()
	l, err := nl.Self()
	if err != nil {
		return err
	}

	for r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 5}); r.Next(); {
		if err = nl.Heartbeat(ctx, l); err != kvserver.ErrEpochIncremented {
			break
		}
	}
	return err
}

// advanceClock advances the mtc's manual clock such that all
// expiration-based leases become expired. The liveness records of all the nodes
// will also become expired on the new clock value (and this will cause all the
// epoch-based leases to be considered expired until the liveness record is
// heartbeated).
//
// This method asserts that all the stores share the manual clock. Otherwise,
// the desired effect would be ambiguous.
func (m *multiTestContext) advanceClock(ctx context.Context) {
	for i, clock := range m.clocks {
		if clock != m.clock() {
			log.Fatalf(ctx, "clock at index %d is different from the shared clock", i)
		}
	}
	m.manualClock.Increment(m.storeConfig.LeaseExpiration())
	log.Infof(ctx, "test clock advanced to: %s", m.clock().Now())
}

// getRaftLeader returns the replica that is the current raft leader for the
// specified rangeID.
func (m *multiTestContext) getRaftLeader(rangeID roachpb.RangeID) *kvserver.Replica {
	m.t.Helper()
	var raftLeaderRepl *kvserver.Replica
	testutils.SucceedsSoon(m.t, func() error {
		m.mu.RLock()
		defer m.mu.RUnlock()
		var latestTerm uint64
		for _, store := range m.stores {
			raftStatus := store.RaftStatus(rangeID)
			if raftStatus == nil {
				// Replica does not exist on this store or there is no raft
				// status yet.
				continue
			}
			if raftStatus.Term > latestTerm || (raftLeaderRepl == nil && raftStatus.Term == latestTerm) {
				// If we find any newer term, it means any previous election is
				// invalid.
				raftLeaderRepl = nil
				latestTerm = raftStatus.Term
				if raftStatus.RaftState == raft.StateLeader {
					var err error
					raftLeaderRepl, err = store.GetReplica(rangeID)
					if err != nil {
						return err
					}
				}
			}
		}
		if latestTerm == 0 || raftLeaderRepl == nil {
			return errors.Errorf("could not find a raft leader for range %s", rangeID)
		}
		return nil
	})
	return raftLeaderRepl
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key roachpb.Key) *roachpb.GetRequest {
	return &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key roachpb.Key, value []byte) *roachpb.PutRequest {
	return &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

// cPutArgs returns a ConditionPutRequest to the default replica
// for the specified key and value, with the given expected value.
func cPutArgs(key roachpb.Key, value, expValue []byte) *roachpb.ConditionalPutRequest {
	var ev *roachpb.Value
	if expValue != nil {
		value := roachpb.MakeValueFromBytes(expValue)
		ev = &value
	}

	return &roachpb.ConditionalPutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpValue: ev,
	}
}

// incrementArgs returns an IncrementRequest addressed to the default replica
// for the specified key.
func incrementArgs(key roachpb.Key, inc int64) *roachpb.IncrementRequest {
	return &roachpb.IncrementRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func truncateLogArgs(index uint64, rangeID roachpb.RangeID) *roachpb.TruncateLogRequest {
	return &roachpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (*roachpb.HeartbeatTxnRequest, roachpb.Header) {
	return &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, roachpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) *roachpb.PushTxnRequest {
	return &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: pushee.Key,
		},
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func TestSortRangeDescByAge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var replicaDescs []roachpb.ReplicaDescriptor
	var rangeDescs []*roachpb.RangeDescriptor

	expectedReplicas := 0
	nextReplID := 1

	// Cut a new range version with the current replica set.
	newRangeVersion := func(marker string) {
		currentRepls := append([]roachpb.ReplicaDescriptor(nil), replicaDescs...)
		rangeDescs = append(rangeDescs, &roachpb.RangeDescriptor{
			RangeID:          roachpb.RangeID(1),
			InternalReplicas: currentRepls,
			NextReplicaID:    roachpb.ReplicaID(nextReplID),
			EndKey:           roachpb.RKey(marker),
		})
	}

	// function to add a replica.
	addReplica := func(marker string) {
		replicaDescs = append(replicaDescs, roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(nextReplID),
			StoreID:   roachpb.StoreID(nextReplID),
			ReplicaID: roachpb.ReplicaID(nextReplID),
		})
		nextReplID++
		newRangeVersion(marker)
		expectedReplicas++
	}

	// function to remove a replica.
	removeReplica := func(marker string) {
		remove := rand.Intn(len(replicaDescs))
		replicaDescs = append(replicaDescs[:remove], replicaDescs[remove+1:]...)
		newRangeVersion(marker)
		expectedReplicas--
	}

	for i := 0; i < 10; i++ {
		addReplica(fmt.Sprint("added", i))
	}
	for i := 0; i < 3; i++ {
		removeReplica(fmt.Sprint("removed", i))
	}
	addReplica("final-add")

	// randomize array
	sortedRangeDescs := make([]*roachpb.RangeDescriptor, len(rangeDescs))
	for i, r := range rand.Perm(len(rangeDescs)) {
		sortedRangeDescs[i] = rangeDescs[r]
	}
	// Sort array by age.
	sort.Sort(rangeDescByAge(sortedRangeDescs))
	// Make sure both arrays are equal.
	if !reflect.DeepEqual(sortedRangeDescs, rangeDescs) {
		t.Fatalf("RangeDescriptor sort by age was not correct. Diff: %s", pretty.Diff(sortedRangeDescs, rangeDescs))
	}
}

func verifyRangeStats(
	reader storage.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats,
) error {
	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), reader)
	if err != nil {
		return err
	}
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount = 0, 0
	if ms != expMS {
		return errors.Errorf("expected and actual stats differ:\n%s", pretty.Diff(expMS, ms))
	}
	return nil
}

func verifyRecomputedStats(
	reader storage.Reader, d *roachpb.RangeDescriptor, expMS enginepb.MVCCStats, nowNanos int64,
) error {
	if ms, err := rditer.ComputeStatsForRange(d, reader, nowNanos); err != nil {
		return err
	} else if expMS != ms {
		return fmt.Errorf("expected range's stats to agree with recomputation: got\n%+v\nrecomputed\n%+v", expMS, ms)
	}
	return nil
}

func waitForTombstone(
	t *testing.T, reader storage.Reader, rangeID roachpb.RangeID,
) (tombstone roachpb.RangeTombstone) {
	testutils.SucceedsSoon(t, func() error {
		tombstoneKey := keys.RangeTombstoneKey(rangeID)
		ok, err := storage.MVCCGetProto(
			context.TODO(), reader, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		)
		if err != nil {
			t.Fatalf("failed to read tombstone: %v", err)
		}
		if !ok {
			return fmt.Errorf("tombstone not found for range %d", rangeID)
		}
		return nil
	})
	return tombstone
}
