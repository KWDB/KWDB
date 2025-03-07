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

	_ "github.com/lib/pq"
)

func runClockJump(ctx context.Context, t *test, c *cluster, tc clockJumpTestCase) {
	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when injecting offset
	if c.spec.NodeCount != 1 {
		t.Fatalf("Expected num nodes to be 1, got: %d", c.spec.NodeCount)
	}

	t.Status("deploying offset injector")
	offsetInjector := newOffsetInjector(c)
	if err := offsetInjector.deploy(ctx); err != nil {
		t.Fatal(err)
	}

	if err := c.RunE(ctx, c.Node(1), "test -x ./kwbase"); err != nil {
		c.Put(ctx, kwbase, "./kwbase", c.All())
	}
	c.Wipe(ctx)
	c.Start(ctx, t)

	db := c.Conn(ctx, c.spec.NodeCount)
	defer db.Close()
	if _, err := db.Exec(
		fmt.Sprintf(
			`SET CLUSTER SETTING server.clock.forward_jump_check_enabled= %v`,
			tc.jumpCheckEnabled)); err != nil {
		t.Fatal(err)
	}

	// Wait for Cockroach to process the above cluster setting
	time.Sleep(10 * time.Second)

	if !isAlive(db, c.l) {
		t.Fatal("Node unexpectedly crashed")
	}

	t.Status("injecting offset")
	// If we expect the node to crash, make sure it's restarted after the
	// test is done to pacify the dead node detector. Do this after the
	// clock offset is reset or the node will crash again.
	var aliveAfterOffset bool
	defer func() {
		offsetInjector.recover(ctx, c.spec.NodeCount)
		// Resetting the clock is a jump in the opposite direction which
		// can cause a crash even if the original jump didn't. Wait a few
		// seconds before checking whether the node is alive and
		// restarting it if not.
		time.Sleep(3 * time.Second)
		if !isAlive(db, c.l) {
			c.Start(ctx, t, c.Node(1))
		}
	}()
	defer offsetInjector.recover(ctx, c.spec.NodeCount)
	offsetInjector.offset(ctx, c.spec.NodeCount, tc.offset)

	// Wait a few seconds to let it crash if it's going to crash.
	time.Sleep(3 * time.Second)

	t.Status("validating health")
	aliveAfterOffset = isAlive(db, c.l)
	if aliveAfterOffset != tc.aliveAfterOffset {
		t.Fatalf("Expected node health %v, got %v", tc.aliveAfterOffset, aliveAfterOffset)
	}
}

type clockJumpTestCase struct {
	name             string
	jumpCheckEnabled bool
	offset           time.Duration
	aliveAfterOffset bool
}

func registerClockJumpTests(r *testRegistry) {
	testCases := []clockJumpTestCase{
		{
			name:             "large_forward_enabled",
			offset:           500 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterOffset: false,
		},
		{
			name: "small_forward_enabled",
			// NB: The offset here needs to be small enough such that this jump plus
			// the forward jump check interval (125ms) is less than the tolerated
			// forward clock jump (250ms).
			offset:           100 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterOffset: true,
		},
		{
			name:             "large_backward_enabled",
			offset:           -500 * time.Millisecond,
			jumpCheckEnabled: true,
			aliveAfterOffset: true,
		},
		{
			name:             "large_forward_disabled",
			offset:           500 * time.Millisecond,
			jumpCheckEnabled: false,
			aliveAfterOffset: true,
		},
		{
			name:             "large_backward_disabled",
			offset:           -500 * time.Millisecond,
			jumpCheckEnabled: false,
			aliveAfterOffset: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		spec := testSpec{
			Name:  "clock/jump/" + tc.name,
			Owner: OwnerKV,
			// These tests muck with NTP, therefore we don't want the cluster reused
			// by others.
			Cluster: makeClusterSpec(1, reuseTagged("offset-injector")),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockJump(ctx, t, c, tc)
			},
		}
		r.Add(spec)
	}
}
