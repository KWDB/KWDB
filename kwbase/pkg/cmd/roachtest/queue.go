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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func registerQueue(r *testRegistry) {
	// One node runs the workload generator, all other nodes host CockroachDB.
	const numNodes = 2
	r.Add(testSpec{
		Skip:    "https://gitee.com/kwbasedb/kwbase/issues/17229",
		Name:    fmt.Sprintf("queue/nodes=%d", numNodes-1),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runQueue(ctx, t, c)
		},
	})
}

func runQueue(ctx context.Context, t *test, c *cluster) {
	dbNodeCount := c.spec.NodeCount - 1
	workloadNode := c.spec.NodeCount

	// Distribute programs to the correct nodes and start CockroachDB.
	c.Put(ctx, kwbase, "./kwbase", c.Range(1, dbNodeCount))
	c.Put(ctx, workload, "./workload", c.Node(workloadNode))
	c.Start(ctx, t, c.Range(1, dbNodeCount))

	runQueueWorkload := func(duration time.Duration, initTables bool) {
		m := newMonitor(ctx, c, c.Range(1, dbNodeCount))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal("", " --concurrency="+fmt.Sprint(dbNodeCount*64))
			duration := fmt.Sprintf(" --duration=%s", duration.String())
			batch := " --batch 100"
			init := ""
			if initTables {
				init = " --init"
			}
			cmd := fmt.Sprintf(
				"./workload run queue --histograms="+perfArtifactsDir+"/stats.json"+
					init+
					concurrency+
					duration+
					batch+
					" {pgurl:1-%d}",
				dbNodeCount,
			)
			c.Run(ctx, c.Node(workloadNode), cmd)
			return nil
		})
		m.Wait()
	}

	// getQueueScanTime samples the time to run a statement that scans the queue
	// table.
	getQueueScanTime := func() time.Duration {
		db := c.Conn(ctx, 1)
		sampleCount := 5
		samples := make([]time.Duration, sampleCount)
		for i := 0; i < sampleCount; i++ {
			startTime := timeutil.Now()
			var queueCount int
			row := db.QueryRow("SELECT count(*) FROM queue.queue WHERE ts < 1000")
			if err := row.Scan(&queueCount); err != nil {
				t.Fatalf("error running delete statement on queue: %s", err)
			}
			endTime := timeutil.Now()
			samples[i] = endTime.Sub(startTime)
		}
		var sum time.Duration
		for _, sample := range samples {
			sum += sample
		}
		return sum / time.Duration(sampleCount)
	}

	// Run an initial short workload to populate the queue table and get a baseline
	// performance for the queue scan time.
	t.Status("running initial workload")
	runQueueWorkload(10*time.Second, true)
	scanTimeBefore := getQueueScanTime()

	// Set TTL on table queue.queue to 0, so that rows are deleted immediately
	db := c.Conn(ctx, 1)
	_, err := db.ExecContext(ctx, `ALTER TABLE queue.queue CONFIGURE ZONE USING gc.ttlseconds = 30`)
	if err != nil && strings.Contains(err.Error(), "syntax error") {
		// Pre-2.1 was EXPERIMENTAL.
		// TODO(knz): Remove this in 2.2.
		_, err = db.ExecContext(ctx, `ALTER TABLE queue.queue EXPERIMENTAL CONFIGURE ZONE 'gc: {ttlseconds: 30}'`)
	}
	if err != nil {
		t.Fatalf("error setting zone config TTL: %s", err)
	}
	// Truncate table to avoid duplicate key constraints.
	if _, err := db.Exec("DELETE FROM queue.queue"); err != nil {
		t.Fatalf("error deleting rows after initial insertion: %s", err)
	}

	t.Status("running primary workload")
	runQueueWorkload(10*time.Minute, false)

	// Sanity Check: ensure that the queue has actually been deleting rows. There
	// may be some entries left over from the end of the workflow, but the number
	// should not exceed the computed maxRows.

	row := db.QueryRow("SELECT count(*) FROM queue.queue")
	var queueCount int
	if err := row.Scan(&queueCount); err != nil {
		t.Fatalf("error selecting queueCount from queue: %s", err)
	}
	maxRows := 100
	if local {
		maxRows *= dbNodeCount * 64
	}
	if queueCount > maxRows {
		t.Fatalf("resulting table had %d entries, expected %d or fewer", queueCount, maxRows)
	}

	// Sample the scan time after the primary workload. We expect this to be
	// similar to the baseline time; if time needed has increased by a factor
	// of five or more, we consider the test to have failed.
	scanTimeAfter := getQueueScanTime()
	fmt.Printf("scan time before load: %s, scan time after: %s", scanTimeBefore, scanTimeAfter)
	fmt.Printf("scan time increase: %f (%f/%f)", float64(scanTimeAfter)/float64(scanTimeBefore), float64(scanTimeAfter), float64(scanTimeBefore))
	if scanTimeAfter > scanTimeBefore*30 {
		t.Fatalf(
			"scan time increased by factor of %f after queue workload",
			float64(scanTimeAfter)/float64(scanTimeBefore),
		)
	}
}
