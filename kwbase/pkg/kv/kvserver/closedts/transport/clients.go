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

package transport

import (
	"context"
	"strings"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
)

// Config holds the information necessary to create a client registry.
type Config struct {
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	NodeID   roachpb.NodeID
	Dialer   closedts.Dialer
	Sink     closedts.Notifyee
}

// Clients manages clients receiving closed timestamp updates from
// peer nodes, along with facilities to request information about certain
// ranges. Received updates are relayed to a provided Notifyee.
type Clients struct {
	cfg Config

	// TODO(tschottdorf): remove unused clients. Perhaps expiring them after,
	// say, 24h is enough? There is no interruption when doing so; the only
	// price is that a full update is sent, but that is pretty cheap too.
	clients syncutil.IntMap
}

var _ closedts.ClientRegistry = (*Clients)(nil)

// NewClients sets up a client registry.
func NewClients(cfg Config) *Clients {
	return &Clients{cfg: cfg}
}

type client struct {
	mu struct {
		syncutil.Mutex
		requested map[roachpb.RangeID]struct{} // never nil
	}
}

// Request is called when serving a follower read has failed due to missing or
// insufficient information. By calling this method, the caller gives the
// instruction to connect to the given node (if it hasn't already) and ask it to
// send (or re-send) up-to-date information about the specified range. Having
// done so, the information should soon thereafter be available to the Sink and
// from there, further follower read attempts. Does not block.
func (pr *Clients) Request(nodeID roachpb.NodeID, rangeID roachpb.RangeID) {
	if nodeID == pr.cfg.NodeID {
		return
	}
	if cl := pr.getOrCreateClient(nodeID); cl != nil {
		cl.mu.Lock()
		cl.mu.requested[rangeID] = struct{}{}
		cl.mu.Unlock()
	}
}

// EnsureClient makes sure that updates from the given nodes are pulled in, if
// they aren't already. This call does not block (and is cheap).
func (pr *Clients) EnsureClient(nodeID roachpb.NodeID) {
	if nodeID == pr.cfg.NodeID {
		return
	}
	pr.getOrCreateClient(nodeID)
}

func (pr *Clients) getOrCreateClient(nodeID roachpb.NodeID) *client {
	// Fast path to check for existing client without an allocation.
	p, found := pr.clients.Load(int64(nodeID))
	cl := (*client)(p)
	if found {
		return cl
	}
	if !pr.cfg.Dialer.Ready(nodeID) {
		return nil
	}

	if nodeID == pr.cfg.NodeID {
		panic("must not create client to local node")
	}

	// Slow path: create the client. Another inserter might race us to it.

	// This allocates, so only do it when necessary.
	ctx := logtags.AddTag(context.Background(), "ct-client", "")

	cl = &client{}
	cl.mu.requested = map[roachpb.RangeID]struct{}{}

	if firstClient, loaded := pr.clients.LoadOrStore(int64(nodeID), unsafe.Pointer(cl)); loaded {
		return (*client)(firstClient)
	}

	// If our client made it into the map, start it. The point in inserting
	// before starting is to be able to collect RangeIDs immediately while never
	// blocking callers.
	pr.cfg.Stopper.RunWorker(ctx, func(ctx context.Context) {
		defer pr.clients.Delete(int64(nodeID))
		rawClient, err := pr.cfg.Dialer.Dial(ctx, nodeID)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "error opening closed timestamp stream to n%d: %+v", nodeID, err)
			}
			return
		}
		processUpdates := func(ctx context.Context, fallback192 bool) error {
			var c ctpb.Client
			var err error
			if !fallback192 {
				c, err = rawClient.Get(ctx)
			} else {
				c, err = rawClient.Get192(ctx)
			}
			if err != nil {
				return err
			}
			defer func() {
				_ = c.CloseSend()
			}()

			ctx = c.Context()

			ch := pr.cfg.Sink.Notify(nodeID)
			defer close(ch)

			reaction := &ctpb.Reaction{}
			for {
				if err := c.Send(reaction); err != nil {
					log.VEventf(ctx, 2, "closed timestamp connection to node %d lost: %s", nodeID, err)
					return err
				}
				entry, err := c.Recv()
				if err != nil {
					log.VEventf(ctx, 2, "closed timestamp connection to node %d lost: %s (%T)", nodeID, err, err)
					return err
				}

				select {
				case ch <- *entry:
				case <-ctx.Done():
					return nil
				case <-pr.cfg.Stopper.ShouldQuiesce():
					return nil
				}

				var requested map[roachpb.RangeID]struct{}
				cl.mu.Lock()
				requested, cl.mu.requested = cl.mu.requested, map[roachpb.RangeID]struct{}{}
				cl.mu.Unlock()

				slice := make([]roachpb.RangeID, 0, len(requested))
				for rangeID := range requested {
					slice = append(slice, rangeID)
				}
				reaction = &ctpb.Reaction{
					Requested: slice,
				}
			}
		}

		err = processUpdates(ctx, false)
		// We've changed the name of an RPC service in 20.1, so when we try to
		// connect to 19.2 nodes they don't know what we're talking about. Fallback
		// to the old name.
		if err != nil && strings.Contains(err.Error(), "unknown service") {
			log.VEventf(ctx, 1, "falling back to 19.2-style closed ts conn for node: %d", nodeID)
			err = processUpdates(ctx, true)
		}
		if err != nil {
			log.VEventf(ctx, 2, "closed timestamp connection to node %d lost: %s", nodeID, err)
		}
	})

	return cl
}
