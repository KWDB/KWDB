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
)

func registerRoachmart(r *testRegistry) {
	runRoachmart := func(ctx context.Context, t *test, c *cluster, partition bool) {
		c.Put(ctx, kwbase, "./kwbase")
		c.Put(ctx, workload, "./workload")
		c.Start(ctx, t)

		// TODO(benesch): avoid hardcoding this list.
		nodes := []struct {
			i    int
			zone string
		}{
			{1, "us-central1-b"},
			{4, "us-west1-b"},
			{7, "europe-west2-b"},
		}

		roachmartRun := func(ctx context.Context, i int, args ...string) {
			args = append(args,
				"--local-zone="+nodes[i].zone,
				"--local-percent=90",
				"--users=10",
				"--orders=100",
				fmt.Sprintf("--partition=%v", partition))

			if err := c.RunE(ctx, c.Node(nodes[i].i), args...); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("initializing workload")
		roachmartRun(ctx, 0, "./workload", "init", "roachmart")

		duration := " --duration=" + ifLocal("10s", "10m")

		t.Status("running workload")
		m := newMonitor(ctx, c)
		for i := range nodes {
			i := i
			m.Go(func(ctx context.Context) error {
				roachmartRun(ctx, i, "./workload", "run", "roachmart", duration)
				return nil
			})
		}

		m.Wait()
	}

	for _, v := range []bool{true, false} {
		v := v
		r.Add(testSpec{
			Name:    fmt.Sprintf("roachmart/partition=%v", v),
			Owner:   OwnerPartitioning,
			Cluster: makeClusterSpec(9, geo(), zones("us-central1-b,us-west1-b,europe-west2-b")),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runRoachmart(ctx, t, c, v)
			},
		})
	}
}
