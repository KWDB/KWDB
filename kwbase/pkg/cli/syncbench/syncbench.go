// Copyright 2017 The Cockroach Authors.
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

package syncbench

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
)

var numOps uint64
var numBytes uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type worker struct {
	db      storage.Engine
	latency struct {
		syncutil.Mutex
		*hdrhistogram.WindowedHistogram
	}
	logOnly bool
}

func newWorker(db storage.Engine) *worker {
	w := &worker{db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

func (w *worker) run(wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := context.Background()
	rand := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var buf []byte

	randBlock := func(min, max int) []byte {
		data := make([]byte, rand.Intn(max-min)+min)
		for i := range data {
			data[i] = byte(rand.Int() & 0xff)
		}
		return data
	}

	for {
		start := timeutil.Now()
		b := w.db.NewBatch()
		if w.logOnly {
			block := randBlock(300, 400)
			if err := b.LogData(block); err != nil {
				log.Fatal(ctx, err)
			}
		} else {
			for j := 0; j < 5; j++ {
				block := randBlock(60, 80)
				key := encoding.EncodeUint32Ascending(buf, rand.Uint32())
				if err := b.Put(storage.MakeMVCCMetadataKey(key), block); err != nil {
					log.Fatal(ctx, err)
				}
				buf = key[:0]
			}
		}
		bytes := uint64(b.Len())
		if err := b.Commit(true, storage.NormalCommitType); err != nil {
			log.Fatal(ctx, err)
		}
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		elapsed := clampLatency(timeutil.Since(start), minLatency, maxLatency)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(ctx, err)
		}
		w.latency.Unlock()
	}
}

// Options holds parameters for the test.
type Options struct {
	Dir         string
	Concurrency int
	Duration    time.Duration
	LogOnly     bool
}

// Run a test of writing synchronously to the RocksDB WAL.
//
// TODO(tschottdorf): this should receive a RocksDB instance so that the caller
// in cli can use OpenEngine (which in turn allows to use encryption, etc).
func Run(opts Options) error {
	// Check if the directory exists.
	_, err := os.Stat(opts.Dir)
	if err == nil {
		return errors.Errorf("error: supplied path '%s' must not exist", opts.Dir)
	}

	defer func() {
		_ = os.RemoveAll(opts.Dir)
	}()

	fmt.Printf("writing to %s\n", opts.Dir)

	db, err := storage.NewDefaultEngine(
		0,
		base.StorageConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      opts.Dir,
		})
	if err != nil {
		return err
	}

	workers := make([]*worker, opts.Concurrency)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newWorker(db)
		workers[i].logOnly = opts.LogOnly
		go workers[i].run(&wg)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		done <- sysutil.Signal(0)
	}()

	if opts.Duration > 0 {
		go func() {
			time.Sleep(opts.Duration)
			done <- sysutil.Signal(0)
		}()
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			return nil
		}
	}
}
