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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func registerSchemaChangeInvertedIndex(r *testRegistry) {
	r.Add(testSpec{
		Name:    "schemachange/invertedindex",
		Owner:   OwnerSQLSchema,
		Cluster: makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSchemaChangeInvertedIndex(ctx, t, c)
		},
	})
}

// runInvertedIndex tests the correctness and performance of building an
// inverted index on randomly generated JSON data (from the JSON workload).
func runSchemaChangeInvertedIndex(ctx context.Context, t *test, c *cluster) {
	kwdbNodes := c.Range(1, c.spec.NodeCount-1)
	workloadNode := c.Node(c.spec.NodeCount)

	c.Put(ctx, kwbase, "./kwbase", kwdbNodes)
	c.Put(ctx, workload, "./workload", workloadNode)
	c.Start(ctx, t, kwdbNodes)

	cmdInit := fmt.Sprintf("./workload init json {pgurl:1}")
	c.Run(ctx, workloadNode, cmdInit)

	// On a 4-node GCE cluster with the standard configuration, this generates ~10 million rows
	initialDataDuration := time.Minute * 20
	indexDuration := time.Hour
	if c.isLocal() {
		initialDataDuration = time.Minute
		indexDuration = time.Minute
	}

	// First generate random JSON data using the JSON workload.
	// TODO (lucy): Using a pre-generated test fixture would be much faster
	m := newMonitor(ctx, c, kwdbNodes)

	cmdWrite := fmt.Sprintf(
		"./workload run json --read-percent=0 --duration %s {pgurl:1-%d} --batch 1000 --sequential",
		initialDataDuration.String(), c.spec.NodeCount-1,
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, workloadNode, cmdWrite)

		db := c.Conn(ctx, 1)
		defer db.Close()

		var count int
		if err := db.QueryRow(`SELECT count(*) FROM json.j`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		t.l.Printf("finished writing %d rows to table", count)

		return nil
	})

	m.Wait()

	// Run the workload (with both reads and writes), and create the index at the same time.
	m = newMonitor(ctx, c, kwdbNodes)

	cmdWriteAndRead := fmt.Sprintf(
		"./workload run json --read-percent=50 --duration %s {pgurl:1-%d} --sequential",
		indexDuration.String(), c.spec.NodeCount-1,
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, workloadNode, cmdWriteAndRead)
		return nil
	})

	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		t.l.Printf("creating index")
		start := timeutil.Now()
		if _, err := db.Exec(`CREATE INVERTED INDEX ON json.j (v)`); err != nil {
			return err
		}
		t.l.Printf("index was created, took %v", timeutil.Since(start))

		return nil
	})

	m.Wait()
}
