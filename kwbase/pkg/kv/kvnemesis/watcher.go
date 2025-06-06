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

package kvnemesis

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/span"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ClosedTimestampTargetInterval allows for setting the closed timestamp target
// interval.
type ClosedTimestampTargetInterval interface {
	Set(context.Context, time.Duration) error
	ResetToDefault(context.Context) error
}

// Watcher slurps all changes that happen to some span of kvs using RangeFeed.
type Watcher struct {
	ct ClosedTimestampTargetInterval
	mu struct {
		syncutil.Mutex
		kvs             *Engine
		frontier        *span.Frontier
		frontierWaiters map[hlc.Timestamp][]chan error
	}
	cancel func()
	g      ctxgroup.Group
}

// Watch starts a new Watcher over the given span of kvs. See Watcher.
func Watch(
	ctx context.Context, dbs []*kv.DB, ct ClosedTimestampTargetInterval, dataSpan roachpb.Span,
) (*Watcher, error) {
	if len(dbs) < 1 {
		return nil, errors.New(`at least one db must be given`)
	}
	firstDB := dbs[0]

	w := &Watcher{
		ct: ct,
	}
	var err error
	if w.mu.kvs, err = MakeEngine(); err != nil {
		return nil, err
	}
	w.mu.frontier = span.MakeFrontier(dataSpan)
	w.mu.frontierWaiters = make(map[hlc.Timestamp][]chan error)
	ctx, w.cancel = context.WithCancel(ctx)
	w.g = ctxgroup.WithContext(ctx)

	dss := make([]*kvcoord.DistSender, len(dbs))
	for i := range dbs {
		sender := dbs[i].NonTransactionalSender()
		dss[i] = sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
	}

	startTs := firstDB.Clock().Now()
	eventC := make(chan *roachpb.RangeFeedEvent, 128)
	w.g.GoCtx(func(ctx context.Context) error {
		ts := startTs
		for i := 0; ; i = (i + 1) % len(dbs) {
			w.mu.Lock()
			ts.Forward(w.mu.frontier.Frontier())
			w.mu.Unlock()

			ds := dss[i]
			err := ds.RangeFeed(ctx, dataSpan, ts, true /* withDiff */, eventC)
			if isRetryableRangeFeedErr(err) {
				log.Infof(ctx, "got retryable RangeFeed error: %+v", err)
				continue
			}
			return err
		}
	})
	w.g.GoCtx(func(ctx context.Context) error {
		return w.processEvents(ctx, eventC)
	})

	// Make sure the RangeFeed has started up, else we might lose some events.
	if err := w.WaitForFrontier(ctx, startTs); err != nil {
		_ = w.Finish()
		return nil, err
	}

	return w, nil
}

func isRetryableRangeFeedErr(err error) bool {
	switch {
	case errors.Is(err, context.Canceled):
		return false
	default:
		return true
	}
}

// Finish tears down the Watcher and returns all the kvs it has ingested. It may
// be called multiple times, though not concurrently.
func (w *Watcher) Finish() *Engine {
	if w.cancel == nil {
		// Finish was already called.
		return w.mu.kvs
	}
	w.cancel()
	w.cancel = nil
	// Only WaitForFrontier cares about errors.
	_ = w.g.Wait()
	return w.mu.kvs
}

// WaitForFrontier blocks until all kv changes <= the given timestamp are
// guaranteed to have been ingested.
func (w *Watcher) WaitForFrontier(ctx context.Context, ts hlc.Timestamp) (retErr error) {
	log.Infof(ctx, `watcher waiting for %s`, ts)
	if err := w.ct.Set(ctx, 1*time.Millisecond); err != nil {
		return err
	}
	defer func() {
		if err := w.ct.ResetToDefault(ctx); err != nil {
			retErr = errors.WithSecondaryError(retErr, err)
		}
	}()
	resultCh := make(chan error, 1)
	w.mu.Lock()
	w.mu.frontierWaiters[ts] = append(w.mu.frontierWaiters[ts], resultCh)
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-resultCh:
		return err
	}
}

func (w *Watcher) processEvents(ctx context.Context, eventC chan *roachpb.RangeFeedEvent) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-eventC:
			switch e := event.GetValue().(type) {
			case *roachpb.RangeFeedError:
				return e.Error.GoError()
			case *roachpb.RangeFeedValue:
				log.Infof(ctx, `rangefeed Put %s %s -> %s (prev %s)`,
					e.Key, e.Value.Timestamp, e.Value.PrettyPrint(), e.PrevValue.PrettyPrint())
				w.mu.Lock()
				// TODO(dan): If the exact key+ts is put into kvs more than once, the
				// Engine will keep the last. This matches our txn semantics (if a key
				// is written in a transaction more than once, only the last is kept)
				// but it means that we'll won't catch it if we violate those semantics.
				// Consider first doing a Get and somehow failing if this exact key+ts
				// has previously been put with a different value.
				w.mu.kvs.Put(storage.MVCCKey{Key: e.Key, Timestamp: e.Value.Timestamp}, e.Value.RawBytes)
				prevTs := e.Value.Timestamp.Prev()
				prevValue := w.mu.kvs.Get(e.Key, prevTs)

				// RangeFeed doesn't send the timestamps of the previous values back
				// because changefeeds don't need them. It would likely be easy to
				// implement, but would add unnecessary allocations in changefeeds,
				// which don't need them. This means we'd want to make it an option in
				// the request, which seems silly to do for only this test.
				prevValue.Timestamp = hlc.Timestamp{}
				prevValueMismatch := !prevValue.Equal(e.PrevValue)
				var engineContents string
				if prevValueMismatch {
					engineContents = w.mu.kvs.DebugPrint("  ")
				}
				w.mu.Unlock()

				if prevValueMismatch {
					log.Infof(ctx, "rangefeed mismatch\n%s", engineContents)
					//panic(errors.Errorf(
					//	`expected (%s, %s) previous value %s got: %s`, e.Key, prevTs, prevValue, e.PrevValue))
				}
			case *roachpb.RangeFeedCheckpoint:
				w.mu.Lock()
				if w.mu.frontier.Forward(e.Span, e.ResolvedTS) {
					frontier := w.mu.frontier.Frontier()
					log.Infof(ctx, `watcher reached frontier %s lagging by %s`,
						frontier, timeutil.Now().Sub(frontier.GoTime()))
					for ts, chs := range w.mu.frontierWaiters {
						if frontier.Less(ts) {
							continue
						}
						log.Infof(ctx, `watcher notifying %s`, ts)
						delete(w.mu.frontierWaiters, ts)
						for _, ch := range chs {
							ch <- nil
						}
					}
				}
				w.mu.Unlock()
			}
		}
	}
}
