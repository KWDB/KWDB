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
	"math/rand"
	"sync/atomic"
	"time"

	systembench "gitee.com/kwbasedb/kwbase/pkg/cli/systembench/systembenchpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ClientOptions holds parameters for client part of
// the network test.
type ClientOptions struct {
	Concurrency int
	Duration    time.Duration
	LatencyMode bool

	Addresses []string
}

// RunClient runs the client workload for the network benchmark.
func RunClient(clientOptions ClientOptions) error {
	ctx := context.TODO()
	clients := make([]systembench.PingerClient, len(clientOptions.Addresses))
	reg := newHistogramRegistry()

	var packetSize int
	if clientOptions.LatencyMode {
		packetSize = 56
	} else {
		packetSize = 128 << 10 // 128KB
	}

	for i := 0; i < len(clientOptions.Addresses); i++ {
		conn, err := grpc.Dial(clientOptions.Addresses[i],
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithInitialWindowSize(65535),
			grpc.WithInitialConnWindowSize(65535),
		)
		if err != nil {
			return err
		}
		clients[i] = systembench.NewPingerClient(conn)
	}
	lastNow := time.Second * 0
	var lastOps uint64
	var lastBytes uint64

	return runTest(ctx, test{
		init: func(g *errgroup.Group) {
			for i := range clients {
				name := clientOptions.Addresses[i]
				namedHist := reg.Register(name)
				g.Go(func() error {
					return grpcClientWorker(ctx, clients[i], namedHist, packetSize)
				})
			}
		},

		tick: func(elapsedTotal time.Duration, i int) {
			elapsed := elapsedTotal - lastNow
			ops := atomic.LoadUint64(&numOps)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed__________________address____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%8s %24s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(elapsedTotal.Seconds())*time.Second,
					tick.Name,
					float64(ops-lastOps)/elapsed.Seconds(),
					float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
			})
			lastNow = elapsedTotal
			lastOps = ops
			lastBytes = bytes
		},

		done: func(elapsed time.Duration) {
			const totalHeader = "\n_elapsed__________________address____ops(total)__mb(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%8s %24s %13d %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(elapsed.Seconds())*time.Second,
					tick.Name,
					atomic.LoadUint64(&numOps),
					float64(atomic.LoadUint64(&numBytes)/(1024.0*1024.0)),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})
		},
	}, clientOptions.Duration)
}

func grpcClientWorker(
	ctx context.Context, c systembench.PingerClient, latency *namedHistogram, payloadSize int,
) error {
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	for {
		start := timeutil.Now()
		resp, err := c.Ping(ctx, &systembench.PingRequest{Payload: payload})

		if err != nil {
			return err
		}

		elapsed := timeutil.Since(start)
		latency.Record(elapsed)
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, uint64(len(payload)+len(resp.Payload)))
	}
}
