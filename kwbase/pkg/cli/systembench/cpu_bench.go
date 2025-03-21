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

package systembench

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// CPUBenchmarkType represents a CPU Benchmark.
type CPUBenchmarkType int

const (
	// CPUPrimeTest identifies a prime factoring CPU test.
	CPUPrimeTest CPUBenchmarkType = iota
)

// CPUOptions holds parameters for the test.
type CPUOptions struct {
	Concurrency int
	Duration    time.Duration

	Type CPUBenchmarkType
}

// workerCPUPrimes holds a latency histogram.
type workerCPUPrimes struct {
	latency *namedHistogram
}

// workerCPUPrimes implements the worker interface.
func (w *workerCPUPrimes) run(ctx context.Context) error {
	dividend := 1
	// This is to make sure we're measuring the divisions
	// vs the performance of locking.
	batchSize := 1000
	for {
		start := timeutil.Now()
		count := uint64(0)
		for i := 0; i < batchSize; i++ {
			limit := math.Sqrt(float64(dividend))
			divisor := 1
			for float64(divisor) <= limit {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				remainder := dividend % divisor
				count++

				if remainder == 0 {
					dividend++
					break
				}
				divisor++
			}
		}
		elapsed := timeutil.Since(start)
		atomic.AddUint64(&numOps, count)
		w.latency.Record(elapsed)
	}
}

// workerCPUPrimes implements the worker interface.
func (w *workerCPUPrimes) getLatencyHistogram() *namedHistogram {
	return w.latency
}

// newWorkerCPUPrimes creates a worker that will verify prime numbers by doing standard
// division of the number by all numbers between 2 and the square root of the number.
// If any number gives a remainder of 0, the next number is calculated.
func newWorkerCPUPrimes(
	ctx context.Context, cpuOptions *CPUOptions, registry *histogramRegistry,
) worker {
	registry.Register("ops")
	w := &workerCPUPrimes{}
	w.latency = registry.Register("ops")
	return w
}

// RunCPU runs cpu benchmarks specified by cpuOptions.
func RunCPU(cpuOptions CPUOptions) error {
	ctx := context.Background()
	reg := newHistogramRegistry()

	workers := make([]worker, cpuOptions.Concurrency)
	var workerCreator func(ctx context.Context, cpuOptions *CPUOptions, registry *histogramRegistry) worker

	switch cpuOptions.Type {
	case CPUPrimeTest:
		workerCreator = newWorkerCPUPrimes
	default:
		return errors.Errorf("Please specify a valid subtest.")
	}

	for i := range workers {
		workers[i] = workerCreator(ctx, &cpuOptions, reg)
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64

	return runTest(ctx, test{
		init: func(g *errgroup.Group) {
			for i := range workers {
				g.Go(func() error {
					return workers[i].run(ctx)
				})
			}
		},

		tick: func(elapsed time.Duration, i int) {
			now := timeutil.Now()
			ops := atomic.LoadUint64(&numOps)
			elapsedSinceLastTick := now.Sub(lastNow)
			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%8s %10s %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
					humanize.Comma(int64(float64(ops-lastOps)/elapsedSinceLastTick.Seconds())),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
			})
			lastNow = now
			lastOps = ops
		},

		done: func(elapsed time.Duration) {
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%8s %13s %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(startElapsed.Seconds())*time.Second,
					humanize.Comma(int64(atomic.LoadUint64(&numOps))),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})
		},
	}, cpuOptions.Duration)
}
