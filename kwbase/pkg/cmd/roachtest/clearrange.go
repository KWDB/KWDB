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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
)

func registerClearRange(r *testRegistry) {
	for _, checks := range []bool{true, false} {
		checks := checks
		r.Add(testSpec{
			Name:  fmt.Sprintf(`clearrange/checks=%t`, checks),
			Owner: OwnerStorage,
			// 5h for import, 90 for the test. The import should take closer
			// to <3:30h but it varies.
			Timeout:    5*time.Hour + 90*time.Minute,
			MinVersion: "v19.1.0",
			Cluster:    makeClusterSpec(10, cpu(16)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClearRange(ctx, t, c, checks)
			},
		})
	}
}

func runClearRange(ctx context.Context, t *test, c *cluster, aggressiveChecks bool) {
	c.Put(ctx, kwbase, "./kwbase")

	t.Status("restoring fixture")
	c.Start(ctx, t)

	// NB: on a 10 node cluster, this should take well below 3h.
	tBegin := timeutil.Now()
	c.Run(ctx, c.Node(1), "./kwbase", "workload", "fixtures", "import", "bank",
		"--payload-bytes=10240", "--ranges=10", "--rows=65104166", "--seed=4", "--db=bigbank")
	c.l.Printf("import took %.2fs", timeutil.Since(tBegin).Seconds())
	c.Stop(ctx)
	t.Status()

	if aggressiveChecks {
		// Run with an env var that runs a synchronous consistency check after each rebalance and merge.
		// This slows down merges, so it might hide some races.
		//
		// NB: the below invocation was found to actually make it to the server at the time of writing.
		c.Start(ctx, t, startArgs(
			"--env", "KWBASE_CONSISTENCY_AGGRESSIVE=true KWBASE_ENFORCE_CONSISTENT_STATS=true",
		))
	} else {
		c.Start(ctx, t)
	}

	// Also restore a much smaller table. We'll use it to run queries against
	// the cluster after having dropped the large table above, verifying that
	// the  cluster still works.
	t.Status(`restoring tiny table`)
	defer t.WorkerStatus()

	if t.buildVersion.AtLeast(version.MustParse("v19.2.0")) {
		conn := c.Conn(ctx, 1)
		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.bulk_io_write.concurrent_addsstable_requests = $1`, c.spec.NodeCount); err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}

	// Use a 120s connect timeout to work around the fact that the server will
	// declare itself ready before it's actually 100% ready. See:
	// https://gitee.com/kwbasedb/kwbase/issues/34897#issuecomment-465089057
	c.Run(ctx, c.Node(1), `KWBASE_CONNECT_TIMEOUT=120 ./kwbase sql --insecure -e "DROP DATABASE IF EXISTS tinybank"`)
	c.Run(ctx, c.Node(1), "./kwbase", "workload", "fixtures", "import", "bank", "--db=tinybank",
		"--payload-bytes=100", "--ranges=10", "--rows=800", "--seed=1")

	t.Status()

	// Set up a convenience function that we can call to learn the number of
	// ranges for the bigbank.bank table (even after it's been dropped).
	numBankRanges := func() func() int {
		conn := c.Conn(ctx, 1)
		defer conn.Close()

		var startHex string
		if err := conn.QueryRow(
			`SELECT to_hex(start_key) FROM kwdb_internal.ranges_no_leases WHERE database_name = 'bigbank' AND table_name = 'bank' ORDER BY start_key ASC LIMIT 1`,
		).Scan(&startHex); err != nil {
			t.Fatal(err)
		}
		return func() int {
			conn := c.Conn(ctx, 1)
			defer conn.Close()
			var n int
			if err := conn.QueryRow(
				`SELECT count(*) FROM kwdb_internal.ranges_no_leases WHERE substr(to_hex(start_key), 1, length($1::string)) = $1`, startHex,
			).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
	}()

	m := newMonitor(ctx, c)
	m.Go(func(ctx context.Context) error {
		conn := c.Conn(ctx, 1)
		defer conn.Close()

		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`); err != nil {
			return err
		}

		// Merge as fast as possible to put maximum stress on the system.
		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_interval = '0s'`); err != nil {
			return err
		}

		t.WorkerStatus("dropping table")
		defer t.WorkerStatus()

		// Set a low TTL so that the ClearRange-based cleanup mechanism can kick in earlier.
		// This could also be done after dropping the table.
		if _, err := conn.ExecContext(ctx, `ALTER TABLE bigbank.bank CONFIGURE ZONE USING gc.ttlseconds = 30`); err != nil {
			return err
		}

		t.WorkerStatus("computing number of ranges")
		initialBankRanges := numBankRanges()

		t.WorkerStatus("dropping bank table")
		if _, err := conn.ExecContext(ctx, `DROP TABLE bigbank.bank`); err != nil {
			return err
		}

		// Spend some time reading data with a timeout to make sure the
		// DROP above didn't brick the cluster. At the time of writing,
		// clearing all of the table data takes ~6min, so we want to run
		// for at least a multiple of that duration.
		const minDuration = 45 * time.Minute
		deadline := timeutil.Now().Add(minDuration)
		curBankRanges := numBankRanges()
		t.WorkerStatus("waiting for ~", curBankRanges, " merges to complete (and for at least ", minDuration, " to pass)")
		for timeutil.Now().Before(deadline) || curBankRanges > 1 {
			after := time.After(5 * time.Minute)
			curBankRanges = numBankRanges() // this call takes minutes, unfortunately
			t.WorkerProgress(1 - float64(curBankRanges)/float64(initialBankRanges))

			var count int
			// NB: context cancellation in QueryRowContext does not work as expected.
			// See #25435.
			if _, err := conn.ExecContext(ctx, `SET statement_timeout = '5s'`); err != nil {
				return err
			}
			// If we can't aggregate over 80kb in 5s, the database is far from usable.
			if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM tinybank.bank`).Scan(&count); err != nil {
				return err
			}

			t.WorkerStatus("waiting for ~", curBankRanges, " merges to complete (and for at least ", timeutil.Now().Sub(deadline), " to pass)")
			select {
			case <-after:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// TODO(tschottdorf): verify that disk space usage drops below to <some small amount>, but that
		// may not actually happen (see https://gitee.com/kwbasedb/kwbase/issues/29290).
		return nil
	})
	m.Wait()
}
