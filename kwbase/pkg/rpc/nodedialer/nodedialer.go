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

package nodedialer

import (
	"context"
	"fmt"
	"net"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/util/grpcutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// No more than one failure to connect to a given node will be logged in the given interval.
const logPerNodeFailInterval = time.Minute

type wrappedBreaker struct {
	*circuit.Breaker
	log.EveryN
}

// An AddressResolver translates NodeIDs into addresses.
type AddressResolver func(roachpb.NodeID) (net.Addr, error)

// A Dialer wraps an *rpc.Context for dialing based on node IDs. For each node,
// it maintains a circuit breaker that prevents rapid connection attempts and
// provides hints to the callers on whether to log the outcome of the operation.
type Dialer struct {
	rpcContext *rpc.Context
	resolver   AddressResolver

	breakers [rpc.NumConnectionClasses]syncutil.IntMap // map[roachpb.NodeID]*wrappedBreaker
}

// New initializes a Dialer.
func New(rpcContext *rpc.Context, resolver AddressResolver) *Dialer {
	return &Dialer{
		rpcContext: rpcContext,
		resolver:   resolver,
	}
}

// Stopper returns this node dialer's Stopper.
// TODO(bdarnell): This is a bit of a hack for kv/transport_race.go
func (n *Dialer) Stopper() *stop.Stopper {
	return n.rpcContext.Stopper
}

// Silence lint warning because this method is only used in race builds.
var _ = (*Dialer).Stopper

// Dial returns a grpc connection to the given node. It logs whenever the
// node first becomes unreachable or reachable.
func (n *Dialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	// Don't trip the breaker if we're already canceled.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	breaker := n.getBreaker(nodeID, class)
	addr, err := n.resolver(nodeID)
	if err != nil {
		err = errors.Wrapf(err, "failed to resolve n%d", nodeID)
		breaker.Fail(err)
		return nil, err
	}
	return n.dial(ctx, nodeID, addr, breaker, class)
}

// DialNoBreaker ignores the breaker if there is an error dialing. This function
// should only be used when there is good reason to believe that the node is reachable.
func (n *Dialer) DialNoBreaker(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return nil, err
	}
	return n.dial(ctx, nodeID, addr, nil /* breaker */, class)
}

// DialInternalClient is a specialization of DialClass for callers that
// want a roachpb.InternalClient. This supports an optimization to bypass the
// network for the local node. Returns a context.Context which should be used
// when making RPC calls on the returned server. (This context is annotated to
// mark this request as in-process and bypass ctx.Peer checks).
func (n *Dialer) DialInternalClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (context.Context, roachpb.InternalClient, error) {
	if n == nil || n.resolver == nil {
		return nil, nil, errors.New("no node dialer configured")
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return nil, nil, err
	}
	if localClient := n.rpcContext.GetLocalInternalClientForAddr(addr.String(), nodeID); localClient != nil {
		log.VEvent(ctx, 2, "sending request to local client")

		// Create a new context from the existing one with the "local request" field set.
		// This tells the handler that this is an in-process request, bypassing ctx.Peer checks.
		localCtx := grpcutil.NewLocalRequestContext(ctx)

		return localCtx, localClient, nil
	}
	log.VEventf(ctx, 2, "sending request to %s", addr)
	conn, err := n.dial(ctx, nodeID, addr, n.getBreaker(nodeID, class), class)
	if err != nil {
		return nil, nil, err
	}
	return ctx, roachpb.NewInternalClient(conn), err
}

// dial performs the dialing of the remote connection. If breaker is nil,
// then perform this logic without using any breaker functionality.
func (n *Dialer) dial(
	ctx context.Context,
	nodeID roachpb.NodeID,
	addr net.Addr,
	breaker *wrappedBreaker,
	class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	// Don't trip the breaker if we're already canceled.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if breaker != nil && !breaker.Ready() {
		err = errors.Wrapf(circuit.ErrBreakerOpen, "unable to dial n%d", nodeID)
		return nil, err
	}
	defer func() {
		// Enforce a minimum interval between warnings for failed connections.
		if err != nil && ctx.Err() == nil && breaker != nil && breaker.ShouldLog() {
			log.Infof(ctx, "unable to connect to n%d: %s", nodeID, err)
		}
	}()
	conn, err := n.rpcContext.GRPCDialNode(addr.String(), nodeID, class).Connect(ctx)
	if err != nil {
		// If we were canceled during the dial, don't trip the breaker.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		err = errors.Wrapf(err, "failed to connect to n%d at %v", nodeID, addr)
		if breaker != nil {
			breaker.Fail(err)
		}
		return nil, err
	}
	// Check to see if the connection is in the transient failure state. This can
	// happen if the connection already existed, but a recent heartbeat has
	// failed and we haven't yet torn down the connection.
	err = grpcutil.ConnectionReady(conn)
	if err := grpcutil.ConnectionReady(conn); err != nil {
		err = errors.Wrapf(err, "failed to check for ready connection to n%d at %v", nodeID, addr)
		if breaker != nil {
			breaker.Fail(err)
		}
		return nil, err
	}

	// TODO(bdarnell): Reconcile the different health checks and circuit breaker
	// behavior in this file. Note that this different behavior causes problems
	// for higher-levels in the system. For example, DistSQL checks for
	// ConnHealth when scheduling processors, but can then see attempts to send
	// RPCs fail when dial fails due to an open breaker. Reset the breaker here
	// as a stop-gap before the reconciliation occurs.
	if breaker != nil {
		breaker.Success()
	}
	return conn, nil
}

// ConnHealth returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat.
// Returns circuit.ErrBreakerOpen if the breaker is tripped, otherwise
// ErrNoConnection if no connection to the node currently exists.
func (n *Dialer) ConnHealth(nodeID roachpb.NodeID, class rpc.ConnectionClass) error {
	if n == nil || n.resolver == nil {
		return errors.New("no node dialer configured")
	}
	// NB: Don't call Ready(). The breaker protocol would require us to follow
	// that up with a dial, which we won't do as this is called in hot paths.
	if n.getBreaker(nodeID, class).Tripped() {
		return circuit.ErrBreakerOpen
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return err
	}
	return n.rpcContext.ConnHealth(addr.String(), nodeID, class)
}

// ConnHealthTryDial returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat. If no
// healthy connection is found, it will attempt to dial the node.
//
// This exists for components that do not themselves actively maintain RPC
// connections to remote nodes, e.g. DistSQL. However, it can cause significant
// latency if the remote node is unresponsive (e.g. if the server/VM is shut
// down), and should be avoided in latency-sensitive code paths. Preferably,
// this should be replaced by some other mechanism to maintain RPC connections.
// See also: https://github.com/cockroachdb/cockroach/issues/70111
func (n *Dialer) ConnHealthTryDial(nodeID roachpb.NodeID, class rpc.ConnectionClass) error {
	err := n.ConnHealth(nodeID, class)
	if err == nil || !n.getBreaker(nodeID, class).Ready() {
		return err
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return err
	}
	return n.rpcContext.GRPCDialNode(addr.String(), nodeID, class).Health()
}

// GetCircuitBreaker retrieves the circuit breaker for connections to the
// given node. The breaker should not be mutated as this affects all connections
// dialing to that node through this NodeDialer.
func (n *Dialer) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) *circuit.Breaker {
	return n.getBreaker(nodeID, class).Breaker
}

func (n *Dialer) getBreaker(nodeID roachpb.NodeID, class rpc.ConnectionClass) *wrappedBreaker {
	breakers := &n.breakers[class]
	value, ok := breakers.Load(int64(nodeID))
	if !ok {
		name := fmt.Sprintf("rpc %v [n%d]", n.rpcContext.Config.Addr, nodeID)
		breaker := &wrappedBreaker{Breaker: n.rpcContext.NewBreaker(name), EveryN: log.Every(logPerNodeFailInterval)}
		value, _ = breakers.LoadOrStore(int64(nodeID), unsafe.Pointer(breaker))
	}
	return (*wrappedBreaker)(value)
}

type dialerAdapter Dialer

func (da *dialerAdapter) Ready(nodeID roachpb.NodeID) bool {
	return (*Dialer)(da).GetCircuitBreaker(nodeID, rpc.DefaultClass).Ready()
}

func (da *dialerAdapter) Dial(
	ctx context.Context, nodeID roachpb.NodeID,
) (closedts.BackwardsCompatibleClosedTimestampClient, error) {
	conn, err := (*Dialer)(da).Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, err
	}
	return closedTimestampClientAdapter{c: ctpb.NewClosedTimestampClient(conn)}, nil
}

// closedTimestampClientAdapter adapts a ClosedTimestampClient to a
// BackwardsCompatibleClosedTimestampClient.
type closedTimestampClientAdapter struct {
	c ctpb.ClosedTimestampClient
}

func (c closedTimestampClientAdapter) Get(
	ctx context.Context, opts ...grpc.CallOption,
) (ctpb.Client, error) {
	return c.c.Get(ctx, opts...)
}

func (c closedTimestampClientAdapter) Get192(
	ctx context.Context, opts ...grpc.CallOption,
) (ctpb.Client, error) {
	return c.c.(ctpb.ClosedTimestampClient192).Get192(ctx, opts...)
}

var _ closedts.BackwardsCompatibleClosedTimestampClient = closedTimestampClientAdapter{}

var _ closedts.Dialer = (*Dialer)(nil).CTDialer()

// CTDialer wraps the NodeDialer into a closedts.Dialer.
func (n *Dialer) CTDialer() closedts.Dialer {
	return (*dialerAdapter)(n)
}
