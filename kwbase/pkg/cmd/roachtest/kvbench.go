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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/search"
	"gitee.com/kwbasedb/kwbase/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
	"github.com/codahale/hdrhistogram"
)

// kvBenchKeyDistribution represents the distribution of keys generated by `workload`.
type kvBenchKeyDistribution int

const (
	sequential kvBenchKeyDistribution = iota
	random
	zipfian
)

type kvBenchSpec struct {
	Nodes int
	CPUs  int
	// Number of shards that the primary key `k` is sharded with. We manually pre-split
	// the `kv` table to create exactly as many splits as there are shards. This should,
	// in theory, split a sequential write load evenly across all the shards. Note that a
	// hash-sharded index combined with our load-based splitting mechanism should achieve
	// something similar to this, but that would take too long for it to be feasibly
	// relied upon for the purposes of this benchmark.
	NumShards       int
	KeyDistribution kvBenchKeyDistribution
	SecondaryIndex  bool

	EstimatedMaxThroughput int
	LatencyThresholdMs     float64
}

func registerKVBenchSpec(r *testRegistry, b kvBenchSpec) {
	nameParts := []string{
		"kv0bench",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
	}
	if b.NumShards > 0 {
		nameParts = append(nameParts, fmt.Sprintf("shards=%d", b.NumShards))
	}
	opts := []createOption{cpu(b.CPUs)}
	switch b.KeyDistribution {
	case sequential:
		nameParts = append(nameParts, "sequential")
	case random:
		nameParts = append(nameParts, "random")
	case zipfian:
		nameParts = append(nameParts, "zipfian")
	default:
		panic("unexpected")
	}

	if b.SecondaryIndex {
		nameParts = append(nameParts, "2nd_idx")
	}

	name := strings.Join(nameParts, "/")
	nodes := makeClusterSpec(b.Nodes+1, opts...)
	r.Add(testSpec{
		Name: name,
		// These tests don't have pass/fail conditions so we don't want to run them
		// nightly. Currently they're only good for printing the results of a search
		// for --max-rate.
		// TODO(andrei): output something to roachperf and start running them
		// nightly.
		Tags:    []string{"manual"},
		Owner:   OwnerKV,
		Cluster: nodes,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runKVBench(ctx, t, c, b)
		},
	})
}

func registerKVBench(r *testRegistry) {
	specs := []kvBenchSpec{
		{
			Nodes:                  5,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 30000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              10,
		},
		{
			Nodes:                  5,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 20000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              0,
		},
		{
			Nodes:                  10,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 60000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              20,
		},
		{
			Nodes:                  10,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 20000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              0,
		},
		{
			Nodes:                  20,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 100000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              80,
		},
		{
			Nodes:                  20,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 20000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         false,
			NumShards:              0,
		},
		{
			Nodes:                  5,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 10000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         true,
			NumShards:              10,
		},
		{
			Nodes:                  5,
			CPUs:                   8,
			KeyDistribution:        sequential,
			EstimatedMaxThroughput: 5000,
			LatencyThresholdMs:     10.0,
			SecondaryIndex:         true,
			NumShards:              0,
		},
	}

	for _, b := range specs {
		registerKVBenchSpec(r, b)
	}
}

func makeKVLoadGroup(c *cluster, numRoachNodes, numLoadNodes int) loadGroup {
	return loadGroup{
		roachNodes: c.Range(1, numRoachNodes),
		loadNodes:  c.Range(numRoachNodes+1, numRoachNodes+numLoadNodes),
	}
}

// KVBench is a benchmarking tool that runs the `kv` workload against CockroachDB based on
// various configuration settings (see `kvBenchSpec`). The tool searches for the maximum
// throughput that can be sustained while maintaining an average latency below a certain
// threshold (as described in the configuration).
//
// This tool was primarily written with the objective of demonstrating the write
// performance characteristics of using hash sharded indexes, for sequential workloads
// which would've otherwise created a single-range hotspot.
func runKVBench(ctx context.Context, t *test, c *cluster, b kvBenchSpec) {
	loadGroup := makeKVLoadGroup(c, b.Nodes, 1)
	roachNodes := loadGroup.roachNodes
	loadNodes := loadGroup.loadNodes

	if err := c.PutE(ctx, t.l, kwbase, "./kwbase", roachNodes); err != nil {
		t.Fatal(err)
	}
	if err := c.PutE(ctx, t.l, workload, "./workload", loadNodes); err != nil {
		t.Fatal(err)
	}

	const restartWait = 15 * time.Second
	// TODO(aayush): I do not have a good reasoning for why I chose this precision value.
	precision := int(math.Max(1.0, float64(b.EstimatedMaxThroughput/50)))
	initStepSize := 2 * precision
	// Search between 100 and 10000000 for the max throughput that can be maintained while
	// sustaining an avg latency of less than `LatencyThresholdMs`.
	// TODO(aayush): `avg` here is just an arbitrary statistic, should I make the
	// concerned statistic a config option?
	resultsDir, err := ioutil.TempDir("", "roachtest-kvbench")
	if err != nil {
		t.Fatal(errors.Wrapf(err, `failed to create temp results dir`))
	}
	s := search.NewLineSearcher(100 /* min */, 10000000 /* max */, b.EstimatedMaxThroughput, initStepSize, precision)
	searchPredicate := func(maxrate int) (bool, error) {
		m := newMonitor(ctx, c, roachNodes)
		// Restart
		m.ExpectDeaths(int32(len(roachNodes)))
		// Wipe cluster before starting a new run because factors like load-based
		// splitting can significantly change the underlying layout of the table and
		// affect benchmark results.
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, t, roachNodes)
		time.Sleep(restartWait)

		// We currently only support one loadGroup.
		resultChan := make(chan *kvBenchResult, 1)
		m.Go(func(ctx context.Context) error {
			db := c.Conn(ctx, 1)

			var initCmd strings.Builder

			fmt.Fprintf(&initCmd, `./workload init kv --num-shards=%d`,
				b.NumShards)
			if b.SecondaryIndex {
				initCmd.WriteString(` --secondary-index`)
			}
			fmt.Fprintf(&initCmd, ` {pgurl%s}`, roachNodes)
			if err := c.RunE(ctx, loadNodes, initCmd.String()); err != nil {
				return err
			}

			var splitCmd strings.Builder
			if b.NumShards > 0 {
				splitCmd.WriteString(`USE kv; ALTER TABLE kv SPLIT AT VALUES `)

				for i := 0; i < b.NumShards; i++ {
					if i != 0 {
						splitCmd.WriteString(`,`)
					}
					fmt.Fprintf(&splitCmd, `(%d)`, i)
				}
				splitCmd.WriteString(`;`)
			}
			if _, err := db.Exec(splitCmd.String()); err != nil {
				t.l.Printf(splitCmd.String())
				return err
			}

			workloadCmd := strings.Builder{}
			clusterHistPath := fmt.Sprintf("%s/kvbench/maxrate=%d/stats.json",
				perfArtifactsDir, maxrate)

			// The number of workers running on the loadGen node must be high enough to
			// fully saturate the loadGen node since the free variable here is the value
			// of the `--max-rate` flag passed to workload and not the level of
			// concurrency.
			//
			// To understand why this is needed, note that if this concurrency value were
			// too low, we would be searching indefinitely since we would never hit
			// throughput values that are greater than or equal to `maxrate`, and this
			// would lead to the `searchPredicate` always returning true as long as the
			// latency at this throughput ceiling was below the configured threshold.
			const loadConcurrency = 192
			const ramp = time.Second * 300
			const duration = time.Second * 300

			fmt.Fprintf(&workloadCmd,
				`./workload run kv --ramp=%fs --duration=%fs {pgurl%s} --read-percent=0`+
					` --concurrency=%d --histograms=%s --max-rate=%d --num-shards=%d`,
				ramp.Seconds(), duration.Seconds(), roachNodes,
				b.CPUs*loadConcurrency, clusterHistPath, maxrate, b.NumShards)
			switch b.KeyDistribution {
			case sequential:
				workloadCmd.WriteString(` --sequential`)
			case zipfian:
				workloadCmd.WriteString(` --zipfian`)
			case random:
				workloadCmd.WriteString(` --random`)
			default:
				panic(`unexpected`)
			}

			err := c.RunE(ctx, loadNodes, workloadCmd.String())
			if err != nil {
				return errors.Wrapf(err, `error running workload`)
			}

			localHistPath := filepath.Join(resultsDir, fmt.Sprintf(`kvbench-%d-stats.json`, maxrate))
			if err := c.Get(ctx, t.l, clusterHistPath, localHistPath, loadNodes); err != nil {
				t.Fatal(err)
			}

			snapshots, err := histogram.DecodeSnapshots(localHistPath)
			if err != nil {
				return errors.Wrapf(err, `failed to decode histogram snapshots`)
			}

			res := newResultFromSnapshots(maxrate, snapshots)
			resultChan <- res
			return nil
		})

		if err := m.WaitE(); err != nil {
			return false, err
		}
		close(resultChan)
		res := <-resultChan

		var color ttycolor.Code
		var msg string
		pass := res.latency() <= b.LatencyThresholdMs
		if pass {
			color = ttycolor.Green
			msg = "PASS"
		} else {
			color = ttycolor.Red
			msg = "FAIL"
		}
		ttycolor.Stdout(color)
		t.l.Printf(`--- SEARCH ITER %s: kv workload avg latency: %0.1fms (threshold: %0.1fms), avg throughput: %d`,
			msg, res.latency(), b.LatencyThresholdMs, res.throughput())
		ttycolor.Stdout(ttycolor.Reset)
		return pass, nil
	}
	if res, err := s.Search(searchPredicate); err != nil {
		t.Fatal(err)
	} else {
		ttycolor.Stdout(ttycolor.Green)
		t.l.Printf("-------\nMAX THROUGHPUT = %d\n--------\n\n", res)
		ttycolor.Stdout(ttycolor.Reset)
	}
}

type kvBenchResult struct {
	Cumulative map[string]*hdrhistogram.Histogram
	Elapsed    time.Duration
}

// TODO(aayush): The result related logic below is similar to `workload/tpcc/result.go`,
// so this could definitely be cleaner and better abstracted.
func newResultFromSnapshots(
	maxrate int, snapshots map[string][]histogram.SnapshotTick,
) *kvBenchResult {
	var start, end time.Time
	ret := make(map[string]*hdrhistogram.Histogram, len(snapshots))
	for n, snaps := range snapshots {
		var cur *hdrhistogram.Histogram
		for _, s := range snaps {
			h := hdrhistogram.Import(s.Hist)
			if cur == nil {
				cur = h
			} else {
				cur.Merge(h)
			}
			if start.IsZero() || s.Now.Before(start) {
				start = s.Now
			}
			if sEnd := s.Now.Add(s.Elapsed); end.IsZero() || sEnd.After(end) {
				end = sEnd
			}
		}
		ret[n] = cur
	}
	return &kvBenchResult{
		Cumulative: ret,
		Elapsed:    end.Sub(start),
	}
}

func (r kvBenchResult) latency() float64 {
	return time.Duration(r.Cumulative[`write`].Mean()).Seconds() * 1000
}

func (r kvBenchResult) throughput() int {
	// Currently the `kv` workload does not track histograms purely for throughput. We can
	// compute the average throughput here but not much more than that.
	return int(float64(r.Cumulative[`write`].TotalCount()) / r.Elapsed.Seconds())
}
