// Copyright 2020 The Cockroach Authors.
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

// Package gcjobnotifier provides a mechanism to share a SystemConfigDeltaFilter
// among all gc jobs.
//
// It exists in a separate package to avoid import cycles between sql and gcjob.
package gcjobnotifier

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

type systemConfigProvider interface {
	GetSystemConfig() *config.SystemConfig
	RegisterSystemConfigChannel() <-chan struct{}
}

// Notifier is used to maybeNotify GC jobs of their need to potentially
// update gc ttls. It exists to be a single copy of the system config rather
// than one per GC job.
type Notifier struct {
	provider systemConfigProvider
	prefix   roachpb.Key
	stopper  *stop.Stopper
	settings *cluster.Settings
	mu       struct {
		syncutil.Mutex
		started, stopped bool
		deltaFilter      *gossip.SystemConfigDeltaFilter
		notifyees        map[chan struct{}]struct{}
	}
}

// New constructs a new Notifier.
func New(
	settings *cluster.Settings, provider systemConfigProvider, stopper *stop.Stopper,
) *Notifier {
	n := &Notifier{
		provider: provider,
		prefix:   keys.MakeTablePrefix(uint32(keys.ZonesTableID)),
		stopper:  stopper,
		settings: settings,
	}
	n.mu.notifyees = make(map[chan struct{}]struct{})
	return n
}

func noopFunc() {}

// AddNotifyee should be called prior to the first reading of the system config.
//
// TODO(lucy,ajwerner): Currently we're calling refreshTables on every zone
// config update to any table. We should really be only updating a cached
// TTL whenever we get an update on one of the tables/indexes (or the db)
// that this job is responsible for, and computing the earliest deadline
// from our set of cached TTL values. To do this we'd need to know the full
// set of zone object IDs which may be relevant for a given table. In general
// that should be fine as a the relevant IDs should be stable for the life of
// anything being deleted. If we did this, we'd associate a set of keys with
// each notifyee.
func (n *Notifier) AddNotifyee(ctx context.Context) (onChange <-chan struct{}, cleanup func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.mu.started {
		log.ReportOrPanic(ctx, &n.settings.SV,
			"adding a notifyee to a Notifier before starting")
	}
	if n.mu.stopped {
		return nil, noopFunc
	}
	if n.mu.deltaFilter == nil {
		zoneCfgFilter := gossip.MakeSystemConfigDeltaFilter(n.prefix)
		n.mu.deltaFilter = &zoneCfgFilter
		// The system config can be nil until the first gossip update. In that
		// case, leave the filter empty so the first config is treated as an update.
		if cfg := n.provider.GetSystemConfig(); cfg != nil {
			n.mu.deltaFilter.ForModified(cfg, func(kv roachpb.KeyValue) {})
		}
	}
	c := make(chan struct{}, 1)
	n.mu.notifyees[c] = struct{}{}
	return c, func() { n.cleanup(c) }
}

func (n *Notifier) cleanup(c chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.mu.notifyees, c)
	if len(n.mu.notifyees) == 0 {
		n.mu.deltaFilter = nil
	}
}

func (n *Notifier) markStopped() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mu.stopped = true
}

func (n *Notifier) markStarted() (alreadyStarted bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	alreadyStarted = n.mu.started
	n.mu.started = true
	return alreadyStarted
}

// Start starts the notifier. It must be started before calling AddNotifyee.
// Start must not be called more than once.
func (n *Notifier) Start(ctx context.Context) {
	if alreadyStarted := n.markStarted(); alreadyStarted {
		log.ReportOrPanic(ctx, &n.settings.SV, "started Notifier more than once")
		return
	}
	if err := n.stopper.RunAsyncTask(ctx, "gcjob.Notifier", n.run); err != nil {
		n.markStopped()
	}
}

func (n *Notifier) run(_ context.Context) {
	defer n.markStopped()
	gossipUpdateCh := n.provider.RegisterSystemConfigChannel()
	for {
		select {
		case <-n.stopper.ShouldQuiesce():
			return
		case <-gossipUpdateCh:
			n.maybeNotify()
		}
	}
}

func (n *Notifier) maybeNotify() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Nobody is listening.
	if len(n.mu.notifyees) == 0 {
		return
	}

	cfg := n.provider.GetSystemConfig()
	if cfg == nil {
		return
	}
	zoneConfigUpdated := false
	n.mu.deltaFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
		zoneConfigUpdated = true
	})

	// Nothing we care about was updated.
	if !zoneConfigUpdated {
		return
	}

	for c := range n.mu.notifyees {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
