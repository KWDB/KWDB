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

package provider_test

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/provider"
	providertestutils "gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/provider/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestProviderSubscribeNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	// We'll only unleash the closer loop when the test is basically done, and
	// once we do that we want it to run aggressively.
	// Testing that the closer loop works as advertised is left to another test.
	closedts.TargetDuration.Override(&st.SV, time.Millisecond)
	closedts.CloseFraction.Override(&st.SV, 1.0)

	storage := &providertestutils.TestStorage{}
	unblockClockCh := make(chan struct{})
	cfg := &provider.Config{
		NodeID:   2, // note that we're not using 1, just for kicks
		Settings: st,
		Stopper:  stopper,
		Storage:  storage,
		Clock: func(roachpb.NodeID) (hlc.Timestamp, ctpb.Epoch, error) {
			select {
			case <-stopper.ShouldQuiesce():
			case <-unblockClockCh:
			}
			return hlc.Timestamp{}, ctpb.Epoch(1), errors.New("injected clock error")
		},
		Close: func(next hlc.Timestamp, expCurEpoch ctpb.Epoch) (hlc.Timestamp, map[roachpb.RangeID]ctpb.LAI, bool) {
			panic("should never be called")
		},
	}

	p := provider.NewProvider(cfg)
	p.Start()

	// We won't touch n1 in this test, so this entry should never pop up.
	unseenEntry := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 456},
		Epoch:           17,
	}
	cfg.Storage.Add(1, unseenEntry)

	entryAt := func(i int) ctpb.Entry {
		return ctpb.Entry{
			ClosedTimestamp: hlc.Timestamp{WallTime: int64(i) * 1e9},
			Epoch:           ctpb.Epoch(i),
			MLAI: map[roachpb.RangeID]ctpb.LAI{
				roachpb.RangeID(i): ctpb.LAI(10 * i),
			},
		}
	}

	const numEntries = 10 // must be even
	var entries []ctpb.Entry
	for i := 0; i < numEntries; i++ {
		entries = append(entries, entryAt(i))
	}

	var readerSeq int32 // atomically
	reader := func() error {
		i := atomic.AddInt32(&readerSeq, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx = logtags.AddTag(ctx, "reader", int(i))

		log.Infof(ctx, "starting")
		defer log.Infof(ctx, "done")

		ch := make(chan ctpb.Entry)
		stopper.RunWorker(ctx, func(ctx context.Context) {
			p.Subscribe(ctx, ch)
		})

		var sl []ctpb.Entry // for debug purposes only
		// Read entries off the subscription. We check two invariants:
		// 1. we see each Entry (identified via its Epoch) at least twice
		//    (morally exactly twice, but the Provider gives a weaker guarantee)
		// 2. An Entry can only be observed after the previous Entry has been seen
		//    at least once. That is, to see Epoch X, we need to have seen Epoch X-1.
		//
		// These could be sharpened somewhat, but only at a distinct loss of clarity
		// in this part of the test.
		//
		// Examples, writing only the Epoch (which uniquely identifies the Entry in this test):
		// OK:
		// - 1 2 3 1 2 3
		//   First writer sends everything before second writer.
		// - 1 1 2 3 1 2 3
		//   First writer sends everything before second, but first 1 gets duplicated by Provider.
		// - 1 2 3 1 2 3 1 2 3
		//   Same as last, but whole thing gets duplicated.
		// - 1 2 3 2 3 1 2 3
		//   Only 2 3 gets duplicated.
		// Not OK:
		// - 1 1 2 3 3
		//   Two seen only once.
		// - 1 3 2 1 2 3
		//   Three observed before two.
		m := map[ctpb.Epoch]int{-1: 2} // pretend we've seen Epoch -1 twice, streamlines code below
		expM := map[ctpb.Epoch]int{-1: 2}
		for _, entry := range entries {
			expM[entry.Epoch] = 2
		}
		for {
			select {
			case <-time.After(10 * time.Second):
				return errors.Errorf("nothing emitted after %v", sl)
			case entry, ok := <-ch: // implies runtime.Gosched
				if !ok {
					if ctx.Err() != nil {
						// Expected, we must've canceled the context below earlier, which means the
						// checks were successful.
						return nil
					}
					return errors.New("sender closed channel before reader canceled their context")
				}
				sl = append(sl, entry)
				log.Infof(ctx, "got %d entries now,latest: %+v", len(sl), entry)
				diagErr := errors.Errorf("saw: %v", sl)
				prevEpo := entry.Epoch - 1
				if m[prevEpo] < 1 {
					return errors.Wrapf(
						diagErr,
						"entry for epoch %d received before a matching entry for immediately preceding epoch %d",
						entry.Epoch, prevEpo,
					)
				}
				m[entry.Epoch]++
				if m[entry.Epoch] > 2 {
					m[entry.Epoch] = 2
				}

				if reflect.DeepEqual(expM, m) && ctx.Err() == nil {
					log.Info(ctx, "canceling subscription")
					cancel()
					// As a little gotcha, we need to work around the implementation a tiny bit.
					// The provider uses a sync.Cond to notify clients and it is likely waiting
					// for new activity for our subscription. Thus, it's not going to notice
					// that this client is going away; it would notice if the Provider's closer
					// did its job (we've blocked it so far) because that periodically wakes
					// up all clients, rain or shine. So we unblock it now; the Clock is set up
					// to return errors, so as a nice little benefit we verify that even in that
					// case the subscription does get woken up.
					close(unblockClockCh)
				}
			}
		}
	}

	// Add some entries via Notify, and race them with various subscriptions. Note
	// that in reality, we have only a single notification going on for the local node
	// (run by a Provider goroutine). But the data that comes in from other nodes uses
	// the same mechanism, and it's nice to get coverage for it. In particular, during
	// reconnections, you could imagine two notification streams for the same NodeID to
	// be active in parallel.
	var g errgroup.Group
	for i := range []struct{}{{}, {}} { // twice
		i := i // goroutine copy
		g.Go(func() error {
			ctx := logtags.AddTag(context.Background(), "writer", i)
			log.Info(ctx, "starting")
			defer log.Info(ctx, "done")
			nCh := p.Notify(roachpb.NodeID(2))
			defer close(nCh)
			for _, entry := range entries {
				nCh <- entry // implies runtime.Gosched
				log.Infof(ctx, "wrote %s", entry)
			}
			return nil
		})
	}

	for i := 0; i < 1; i++ { // HACK
		g.Go(reader)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		snap := storage.Snapshot()
		require.Equal(t, 2, len(snap))    // definitely should have records about two nodes
		require.Equal(t, 1, len(snap[1])) // one persisted entry for n1
		// Morally this is true immediately, but consider that the goroutine consuming
		// from the writer threads above may have read the entry but not put it into
		// the Storage yet. The reader threads would usually remove this race, but
		// they can be satisfied early by a duplicate that is emitted during the
		// switchover from storage to subscription.
		if exp, act := 2*numEntries, len(snap[2]); exp < act {
			t.Fatalf("got %d entries in storage, expected no more than %d", act, exp)
		} else if exp > act {
			return errors.Errorf("storage has %d entries, need %d", exp, act)
		}
		return nil
	})
}

// TestProviderSubscribeConcurrent prevents regression of a bug that improperly
// handled concurrent subscriptions.
func TestProviderSubscribeConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(&st.SV, time.Millisecond)
	closedts.CloseFraction.Override(&st.SV, 1.0)

	stopper := stop.NewStopper()
	storage := &providertestutils.TestStorage{}

	var ts int64 // atomic
	cfg := &provider.Config{
		NodeID:   1,
		Settings: st,
		Stopper:  stopper,
		Storage:  storage,
		Clock: func(roachpb.NodeID) (hlc.Timestamp, ctpb.Epoch, error) {
			return hlc.Timestamp{}, 1, nil
		},
		Close: func(next hlc.Timestamp, expCurEpoch ctpb.Epoch) (hlc.Timestamp, map[roachpb.RangeID]ctpb.LAI, bool) {
			return hlc.Timestamp{
					WallTime: atomic.AddInt64(&ts, 1),
				}, map[roachpb.RangeID]ctpb.LAI{
					1: ctpb.LAI(atomic.LoadInt64(&ts)),
				}, true
		},
	}

	p := provider.NewProvider(cfg)
	p.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	cancel = func() {}
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ch := make(chan ctpb.Entry, 3)
			p.Subscribe(ctx, ch)
			// Read from channel until stopper stops Provider (and in turn Provider
			// closes channel).
			for range ch {
			}
		}()
	}
	stopper.Stop(context.Background())
	wg.Wait()
}
