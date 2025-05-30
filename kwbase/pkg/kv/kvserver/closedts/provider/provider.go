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

package provider

import (
	"context"
	"math"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// Config holds the information necessary to create a Provider.
type Config struct {
	// NodeID is the ID of the node on which the Provider is housed.
	NodeID   roachpb.NodeID
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Storage  closedts.Storage
	Clock    closedts.LiveClockFn
	Close    closedts.CloseFn
}

type subscriber struct {
	ch    chan<- ctpb.Entry
	queue []ctpb.Entry
}

// Provider implements closedts.Provider. It orchestrates the flow of closed
// timestamps and lets callers check whether they can serve reads.
type Provider struct {
	cfg *Config

	mu struct {
		syncutil.RWMutex
		*sync.Cond // on RWMutex.RLocker()
		// The current subscribers. The goroutine associated to each
		// subscriber uses the RLock to mutate its slot. Thus, when
		// accessing this slice for any other reason, the write lock
		// needs to be acquired.
		subscribers []*subscriber
		draining    bool // tell subscribers to terminate
	}

	everyClockLog log.EveryN
}

var _ closedts.Provider = (*Provider)(nil)

// NewProvider initializes a Provider, that has yet to be started.
func NewProvider(cfg *Config) *Provider {
	p := &Provider{
		cfg:           cfg,
		everyClockLog: log.Every(time.Minute),
	}
	p.mu.Cond = sync.NewCond(p.mu.RLocker())
	return p
}

// Start implements closedts.Provider.
//
// TODO(tschottdorf): the closer functionality could be extracted into its own
// component, which would make the interfaces a little cleaner. Decide whether
// it's worth it during testing.
func (p *Provider) Start() {
	p.cfg.Stopper.RunWorker(logtags.AddTag(context.Background(), "ct-closer", nil), p.runCloser)
}

func (p *Provider) drain() {
	p.mu.Lock()
	p.mu.draining = true
	p.mu.Unlock()
	for {
		p.mu.Broadcast()
		p.mu.Lock()
		done := true
		for _, sub := range p.mu.subscribers {
			done = done && sub == nil
		}
		p.mu.Unlock()

		if done {
			return
		}
	}
}

func (p *Provider) runCloser(ctx context.Context) {
	// The loop below signals the subscribers, so when it exits it needs to do
	// extra work to help the subscribers terminate.
	defer p.drain()

	if p.cfg.NodeID == 0 {
		// This Provider is likely misconfigured.
		panic("can't use NodeID zero")
	}
	ch := p.Notify(p.cfg.NodeID)
	defer close(ch)

	confCh := make(chan struct{}, 1)
	confChanged := func() {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.TargetDuration.SetOnChange(&p.cfg.Settings.SV, confChanged)
	// Track whether we've ever been live to avoid logging warnings about not
	// being live during node startup.
	var everBeenLive bool
	var t timeutil.Timer
	defer t.Stop()
	for {
		closeFraction := closedts.CloseFraction.Get(&p.cfg.Settings.SV)
		targetDuration := float64(closedts.TargetDuration.Get(&p.cfg.Settings.SV))
		t.Reset(time.Duration(closeFraction * targetDuration))

		select {
		case <-p.cfg.Stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
		case <-confCh:
			// Loop around to use the updated timer.
			continue
		}

		next, liveAtEpoch, err := p.cfg.Clock(p.cfg.NodeID)
		next.WallTime -= int64(targetDuration)
		if err != nil {
			if everBeenLive && p.everyClockLog.ShouldLog() {
				log.Warningf(ctx, "unable to move closed timestamp forward: %+v", err)
			}
			// Broadcast even if nothing new was queued, so that the subscribers
			// loop to check their client's context.
			p.mu.Broadcast()
		} else {
			everBeenLive = true
			// Close may fail if the data being closed does not correspond to the
			// current liveAtEpoch.
			closed, m, ok := p.cfg.Close(next, liveAtEpoch)
			if !ok {
				if log.V(1) {
					log.Infof(ctx, "failed to close %v due to liveness epoch mismatch at %v",
						next, liveAtEpoch)
				}
				continue
			}
			if log.V(1) {
				log.Infof(ctx, "closed ts=%s with %+v, next closed timestamp should be %s",
					closed, m, next)
			}
			entry := ctpb.Entry{
				Epoch:           liveAtEpoch,
				ClosedTimestamp: closed,
				MLAI:            m,
			}

			// Simulate a subscription to the local node, so that the new information
			// is added to the storage (and thus becomes available to future subscribers
			// as well, not only to existing ones). The other end of the chan will Broadcast().
			//
			// TODO(tschottdorf): the transport should ignore connection requests from
			// the node to itself. Those connections would pointlessly loop this around
			// once more.
			ch <- entry
		}
	}
}

// Notify implements closedts.Notifyee. It passes the incoming stream of Entries
// to the local Storage.
func (p *Provider) Notify(nodeID roachpb.NodeID) chan<- ctpb.Entry {
	ch := make(chan ctpb.Entry)

	p.cfg.Stopper.RunWorker(context.Background(), func(ctx context.Context) {
		handle := func(entry ctpb.Entry) {
			p.cfg.Storage.Add(nodeID, entry)
		}
		// Special-case data about the origin node, which folks can subscribe to.
		// This is easily generalized to also allow subscriptions for data that
		// originated on other nodes, but this doesn't seem necessary right now.
		if nodeID == p.cfg.NodeID {
			handle = func(entry ctpb.Entry) {
				// Add to the Storage first.
				p.cfg.Storage.Add(nodeID, entry)
				// Notify existing subscribers.
				p.mu.Lock()
				for _, sub := range p.mu.subscribers {
					if sub == nil {
						continue
					}
					sub.queue = append(sub.queue, entry)
				}
				p.mu.Unlock()
				// Wake up all clients.
				p.mu.Broadcast()
			}
		}
		for entry := range ch {
			handle(entry)
		}
	})

	return ch
}

// Subscribe implements closedts.Producer. It produces a stream of Entries
// pertaining to the local Node.
//
// TODO(tschottdorf): consider not forcing the caller to launch the goroutine.
func (p *Provider) Subscribe(ctx context.Context, ch chan<- ctpb.Entry) {
	var i int
	sub := &subscriber{ch, nil}
	p.mu.Lock()
	for i = 0; i < len(p.mu.subscribers); i++ {
		if p.mu.subscribers[i] == nil {
			p.mu.subscribers[i] = sub
			break
		}
	}
	if i == len(p.mu.subscribers) {
		p.mu.subscribers = append(p.mu.subscribers, sub)
	}
	draining := p.mu.draining
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.mu.subscribers[i] = nil
		p.mu.Unlock()
		close(ch)
	}()

	if draining {
		return
	}

	if log.V(1) {
		log.Infof(ctx, "new subscriber (slot %d) connected", i)
	}

	// The subscription is already active, so any storage snapshot from now on is
	// going to fully catch up the subscriber without a gap.
	{
		var entries []ctpb.Entry

		p.cfg.Storage.VisitAscending(p.cfg.NodeID, func(entry ctpb.Entry) (done bool) {
			// Don't block in this method.
			entries = append(entries, entry)
			return false // not done
		})

		for _, entry := range entries {
			select {
			case ch <- entry:
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	}

	for {
		p.mu.RLock()
		var done bool
		for len(p.mu.subscribers[i].queue) == 0 {
			if ctx.Err() != nil || p.mu.draining {
				done = true
				break
			}
			p.mu.Wait()
		}
		var queue []ctpb.Entry
		// When only readers are around (as they are now), we can actually
		// mutate our slot because that's all the others do as well.
		queue, p.mu.subscribers[i].queue = p.mu.subscribers[i].queue, nil
		p.mu.RUnlock()

		if done {
			return
		}

		shouldLog := log.V(1)
		var n int
		minMLAI := ctpb.LAI(math.MaxInt64)
		var minRangeID, maxRangeID roachpb.RangeID
		var maxMLAI ctpb.LAI

		for _, entry := range queue {
			if shouldLog {
				n += len(entry.MLAI)
				for rangeID, mlai := range entry.MLAI {
					if mlai < minMLAI {
						minMLAI = mlai
						minRangeID = rangeID
					}
					if mlai > maxMLAI {
						maxMLAI = mlai
						maxRangeID = rangeID
					}
				}
			}

			select {
			case ch <- entry:
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
		if shouldLog {
			log.Infof(ctx, "sent %d closed timestamp entries to client %d (%d range updates total, min/max mlai: %d@r%d / %d@r%d)", len(queue), i, n, minMLAI, minRangeID, maxMLAI, maxRangeID)
		}
	}
}

// MaxClosed implements closedts.Provider.
func (p *Provider) MaxClosed(
	nodeID roachpb.NodeID, rangeID roachpb.RangeID, epoch ctpb.Epoch, lai ctpb.LAI,
) hlc.Timestamp {
	var maxTS hlc.Timestamp
	p.cfg.Storage.VisitDescending(nodeID, func(entry ctpb.Entry) (done bool) {
		if mlai, found := entry.MLAI[rangeID]; found {
			if entry.Epoch == epoch && mlai <= lai {
				maxTS = entry.ClosedTimestamp
				return true
			}
		}
		return false
	})

	return maxTS
}
