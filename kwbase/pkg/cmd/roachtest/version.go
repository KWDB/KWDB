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
	"runtime"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/binfetcher"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
)

// TODO(tbg): remove this test. Use the harness in versionupgrade.go
// to make a much better one, much more easily.
func registerVersion(r *testRegistry) {
	runVersion := func(ctx context.Context, t *test, c *cluster, binaryVersion string) {
		nodes := c.spec.NodeCount - 1
		goos := ifLocal(runtime.GOOS, "linux")

		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "kwbase",
			Version: "v" + binaryVersion,
			GOOS:    goos,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, workload, "./workload", c.Node(nodes+1))

		c.Put(ctx, b, "./kwbase", c.Range(1, nodes))
		// Force disable encryption.
		// TODO(mberhault): allow it once version >= 2.1.
		c.Start(ctx, t, c.Range(1, nodes), startArgsDontEncrypt)

		stageDuration := 10 * time.Minute
		buffer := 10 * time.Minute
		if local {
			t.l.Printf("local mode: speeding up test\n")
			stageDuration = 10 * time.Second
			buffer = time.Minute
		}

		loadDuration := " --duration=" + (time.Duration(3*nodes+2)*stageDuration + buffer).String()

		var deprecatedWorkloadsStr string
		if !t.buildVersion.AtLeast(version.MustParse("v20.2.0")) {
			deprecatedWorkloadsStr += " --deprecated-fk-indexes"
		}

		workloads := []string{
			"./workload run tpcc --tolerate-errors --wait=false --drop --init --warehouses=1 " + deprecatedWorkloadsStr + loadDuration + " {pgurl:1-%d}",
			"./workload run kv --tolerate-errors --init" + loadDuration + " {pgurl:1-%d}",
		}

		m := newMonitor(ctx, c, c.Range(1, nodes))
		for _, cmd := range workloads {
			cmd := cmd // loop-local copy
			m.Go(func(ctx context.Context) error {
				cmd = fmt.Sprintf(cmd, nodes)
				return c.RunE(ctx, c.Node(nodes+1), cmd)
			})
		}

		m.Go(func(ctx context.Context) error {
			l, err := t.l.ChildLogger("upgrader")
			if err != nil {
				return err
			}
			// NB: the number of calls to `sleep` needs to be reflected in `loadDuration`.
			sleepAndCheck := func() error {
				t.WorkerStatus("sleeping")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(stageDuration):
				}
				// Make sure everyone is still running.
				for i := 1; i <= nodes; i++ {
					t.WorkerStatus("checking ", i)
					db := c.Conn(ctx, i)
					defer db.Close()
					rows, err := db.Query(`SHOW DATABASES`)
					if err != nil {
						return err
					}
					if err := rows.Close(); err != nil {
						return err
					}
					// Regression test for #37425. We can't run this in 2.1 because
					// 19.1 changed downstream-of-raft semantics for consistency
					// checks but unfortunately our versioning story for these
					// checks had been broken for a long time. See:
					//
					// https://gitee.com/kwbasedb/kwbase/issues/37737#issuecomment-496026918
					if !strings.HasPrefix(binaryVersion, "2.") {
						if err := c.CheckReplicaDivergenceOnDB(ctx, db); err != nil {
							return errors.Wrapf(err, "node %d", i)
						}
					}
				}
				return nil
			}

			db := c.Conn(ctx, 1)
			defer db.Close()
			// See analogous comment in the upgrade/mixedWith roachtest.
			db.SetMaxIdleConns(0)

			// First let the load generators run in the cluster at `version`.
			if err := sleepAndCheck(); err != nil {
				return err
			}

			stop := func(node int) error {
				m.ExpectDeath()
				l.Printf("stopping node %d\n", node)
				return c.StopCockroachGracefullyOnNode(ctx, node)
			}

			var oldVersion string
			if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&oldVersion); err != nil {
				return err
			}
			l.Printf("cluster version is %s\n", oldVersion)

			// Now perform a rolling restart into the new binary.
			for i := 1; i < nodes; i++ {
				t.WorkerStatus("upgrading ", i)
				l.Printf("upgrading %d\n", i)
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, kwbase, "./kwbase", c.Node(i))
				c.Start(ctx, t, c.Node(i), startArgsDontEncrypt)
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			l.Printf("stopping last node\n")
			// Stop the last node.
			if err := stop(nodes); err != nil {
				return err
			}

			// Set cluster.preserve_downgrade_option to be the old cluster version to
			// prevent upgrade.
			l.Printf("preventing automatic upgrade\n")
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf("SET CLUSTER SETTING cluster.preserve_downgrade_option = '%s';", oldVersion),
			); err != nil {
				return err
			}

			// Do upgrade for the last node.
			l.Printf("upgrading last node\n")
			c.Put(ctx, kwbase, "./kwbase", c.Node(nodes))
			c.Start(ctx, t, c.Node(nodes), startArgsDontEncrypt)
			if err := sleepAndCheck(); err != nil {
				return err
			}

			// Changed our mind, let's roll that back.
			for i := 1; i <= nodes; i++ {
				l.Printf("downgrading node %d\n", i)
				t.WorkerStatus("downgrading", i)
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, b, "./kwbase", c.Node(i))
				c.Start(ctx, t, c.Node(i), startArgsDontEncrypt)
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			// OK, let's go forward again.
			for i := 1; i <= nodes; i++ {
				l.Printf("upgrading node %d (again)\n", i)
				t.WorkerStatus("upgrading", i, "(again)")
				if err := stop(i); err != nil {
					return err
				}
				c.Put(ctx, kwbase, "./kwbase", c.Node(i))
				c.Start(ctx, t, c.Node(i), startArgsDontEncrypt)
				if err := sleepAndCheck(); err != nil {
					return err
				}
			}

			// Reset cluster.preserve_downgrade_option to allow auto upgrade.
			l.Printf("reenabling auto-upgrade\n")
			if _, err := db.ExecContext(ctx,
				"RESET CLUSTER SETTING cluster.preserve_downgrade_option;",
			); err != nil {
				return err
			}

			return sleepAndCheck()
		})
		m.Wait()
	}

	for _, n := range []int{3, 5} {
		r.Add(testSpec{
			Name:       fmt.Sprintf("version/mixed/nodes=%d", n),
			Owner:      OwnerKV,
			MinVersion: "v2.1.0",
			Cluster:    makeClusterSpec(n + 1),
			Run: func(ctx context.Context, t *test, c *cluster) {
				pred, err := PredecessorVersion(r.buildVersion)
				if err != nil {
					t.Fatal(err)
				}
				runVersion(ctx, t, c, pred)
			},
		})
	}
}
