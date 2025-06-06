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
	"reflect"
	"strings"
	"time"
)

func runCLINodeStatus(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, kwbase, "./kwbase")
	c.Start(ctx, t, c.Range(1, 3))

	db := c.Conn(ctx, 1)
	defer db.Close()

	waitForFullReplication(t, db)

	lastWords := func(s string) []string {
		var result []string
		s = elideInsecureDeprecationNotice(s)
		lines := strings.Split(s, "\n")
		for _, line := range lines {
			words := strings.Fields(line)
			if n := len(words); n > 0 {
				result = append(result, words[n-2]+" "+words[n-1])
			}
		}
		return result
	}

	nodeStatus := func() (raw string, _ []string) {
		out, err := c.RunWithBuffer(ctx, t.l, c.Node(1),
			"./kwbase node status --insecure -p {pgport:1}")
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		raw = string(out)
		return raw, lastWords(string(out))
	}

	{
		expected := []string{
			"is_available is_live",
			"true true",
			"true true",
			"true true",
		}
		raw, actual := nodeStatus()
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\nfrom:\n%s", expected, actual, raw)
		}
	}

	waitUntil := func(expected []string) {
		var raw string
		var actual []string
		// Node liveness takes ~9s to time out. Give the test double that time.
		for i := 0; i < 20; i++ {
			raw, actual = nodeStatus()
			if reflect.DeepEqual(expected, actual) {
				break
			}
			t.l.Printf("not done: %s vs %s\n", expected, actual)
			time.Sleep(time.Second)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s from:\n%s", expected, actual, raw)
		}
	}

	// Kill node 2 and wait for it to be marked as !is_available and !is_live.
	c.Stop(ctx, c.Node(2))
	waitUntil([]string{
		"is_available is_live",
		"true true",
		"false false",
		"true true",
	})

	// Kill node 3 and wait for all of the nodes to be marked as
	// !is_available. Node 1 is not available because the liveness check can no
	// longer write to the liveness range due to lack of quorum. This test is
	// verifying that "node status" still returns info in this situation since
	// it only accesses gossip info.
	c.Stop(ctx, c.Node(3))
	waitUntil([]string{
		"is_available is_live",
		"false true",
		"false false",
		"false false",
	})

	// Stop the cluster and restart only 2 of the nodes. Verify that three nodes
	// show up in the node status output.
	c.Stop(ctx, c.Range(1, 3))
	c.Start(ctx, t, c.Range(1, 2))

	// Wait for the cluster to come back up.
	waitForFullReplication(t, db)

	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"false false",
	})

	// Start node again to satisfy roachtest.
	c.Start(ctx, t, c.Node(3))
}
