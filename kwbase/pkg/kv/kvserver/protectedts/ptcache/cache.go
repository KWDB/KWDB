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

package ptcache

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil/singleflight"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// Cache implements protectedts.Cache.
type Cache struct {
	db       *kv.DB
	storage  protectedts.Storage
	stopper  *stop.Stopper
	settings *cluster.Settings
	sf       singleflight.Group
	mu       struct {
		syncutil.RWMutex

		started bool

		// Updated in doUpdate().
		lastUpdate hlc.Timestamp
		state      ptpb.State

		// Updated in doUpdate but mutable. The records in the map are not mutated
		// and should not be by any client.
		recordsByID map[uuid.UUID]*ptpb.Record

		// TODO(ajwerner): add a more efficient lookup structure such as an
		// interval.Tree for Iterate.
	}
}

// Config configures a Cache.
type Config struct {
	DB       *kv.DB
	Storage  protectedts.Storage
	Settings *cluster.Settings
}

// New returns a new cache.
func New(config Config) *Cache {
	c := &Cache{
		db:       config.DB,
		storage:  config.Storage,
		settings: config.Settings,
	}
	c.mu.recordsByID = make(map[uuid.UUID]*ptpb.Record)
	return c
}

var _ protectedts.Cache = (*Cache)(nil)

// Iterate is part of the protectedts.Cache interface.
func (c *Cache) Iterate(
	_ context.Context, from, to roachpb.Key, it protectedts.Iterator,
) (asOf hlc.Timestamp) {
	c.mu.RLock()
	state, lastUpdate := c.mu.state, c.mu.lastUpdate
	c.mu.RUnlock()

	sp := roachpb.Span{
		Key:    from,
		EndKey: to,
	}
	for i := range state.Records {
		r := &state.Records[i]
		if !overlaps(r, sp) {
			continue
		}
		if wantMore := it(r); !wantMore {
			break
		}
	}
	return lastUpdate
}

// QueryRecord is part of the protectedts.Cache interface.
func (c *Cache) QueryRecord(_ context.Context, id uuid.UUID) (exists bool, asOf hlc.Timestamp) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists = c.mu.recordsByID[id]
	return exists, c.mu.lastUpdate
}

// refreshKey is used for the singleflight.
const refreshKey = ""

// Refresh is part of the protectedts.Cache interface.
func (c *Cache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	for !c.upToDate(asOf) {
		ch, _ := c.sf.DoChan(refreshKey, c.doSingleFlightUpdate)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-ch:
			if res.Err != nil {
				return res.Err
			}
		}
	}
	return nil
}

// Start starts the periodic fetching of the Cache. A Cache must not be used
// until after it has been started. An error will be returned if it has
// already been started.
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.started {
		return errors.New("cannot start a Cache more than once")
	}
	c.mu.started = true
	c.stopper = stopper
	return c.stopper.RunAsyncTask(ctx, "periodically-refresh-protectedts-cache",
		c.periodicallyRefreshProtectedtsCache)
}

func (c *Cache) periodicallyRefreshProtectedtsCache(ctx context.Context) {
	settingChanged := make(chan struct{}, 1)
	protectedts.PollInterval.SetOnChange(&c.settings.SV, func() {
		select {
		case settingChanged <- struct{}{}:
		default:
		}
	})
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(0) // Read immediately upon startup
	var lastReset time.Time
	var doneCh <-chan singleflight.Result
	// TODO(ajwerner): consider resetting the timer when the state is updated
	// due to a call to Refresh.
	for {
		select {
		case <-timer.C:
			// Let's not reset the timer until we get our response.
			timer.Read = true
			doneCh, _ = c.sf.DoChan(refreshKey, c.doSingleFlightUpdate)
		case <-settingChanged:
			if timer.Read { // we're currently fetching
				continue
			}
			interval := protectedts.PollInterval.Get(&c.settings.SV)
			// NB: It's okay if nextUpdate is a negative duration; timer.Reset will
			// treat a negative duration as zero and send a notification immediately.
			nextUpdate := interval - timeutil.Since(lastReset)
			timer.Reset(nextUpdate)
			lastReset = timeutil.Now()
		case res := <-doneCh:
			if res.Err != nil {
				if ctx.Err() == nil {
					log.Errorf(ctx, "failed to refresh protected timestamps: %v", res.Err)
				}
			}
			timer.Reset(protectedts.PollInterval.Get(&c.settings.SV))
			lastReset = timeutil.Now()
		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

func (c *Cache) doSingleFlightUpdate() (interface{}, error) {
	// TODO(ajwerner): add log tags to the context.
	ctx, cancel := c.stopper.WithCancelOnQuiesce(context.Background())
	defer cancel()
	return nil, c.stopper.RunTaskWithErr(ctx,
		"refresh-protectedts-cache", c.doUpdate)
}

func (c *Cache) getMetadata() ptpb.Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.state.Metadata
}

func (c *Cache) doUpdate(ctx context.Context) error {
	// NB: doUpdate is only ever called underneath c.singleFlight and thus is
	// never called concurrently. Due to the lack of concurrency there are no
	// concerns about races as this is the only method which writes to the Cache's
	// state.
	prev := c.getMetadata()
	var (
		versionChanged bool
		state          ptpb.State
		ts             hlc.Timestamp
	)
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		// NB: because this is a read-only transaction, the commit will be a no-op;
		// returning nil here means the transaction will commit and will never need
		// to change its read timestamp.
		defer func() {
			if err == nil {
				ts = txn.ReadTimestamp()
			}
		}()
		md, err := c.storage.GetMetadata(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "failed to fetch protectedts metadata")
		}
		if versionChanged = md.Version != prev.Version; !versionChanged {
			return nil
		}
		if state, err = c.storage.GetState(ctx, txn); err != nil {
			return errors.Wrap(err, "failed to fetch protectedts state")
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.lastUpdate = ts
	if versionChanged {
		c.mu.state = state
		for id := range c.mu.recordsByID {
			delete(c.mu.recordsByID, id)
		}
		for i := range state.Records {
			r := &state.Records[i]
			c.mu.recordsByID[r.ID] = r
		}
	}
	return nil
}

// upToDate returns true if the lastUpdate for the cache is at least asOf.
func (c *Cache) upToDate(asOf hlc.Timestamp) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return asOf.LessEq(c.mu.lastUpdate)
}

func overlaps(r *ptpb.Record, sp roachpb.Span) bool {
	for i := range r.Spans {
		if r.Spans[i].Overlaps(sp) {
			return true
		}
	}
	return false
}
