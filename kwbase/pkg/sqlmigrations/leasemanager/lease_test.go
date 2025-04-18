// Copyright 2016 The Cockroach Authors.
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

package leasemanager_test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sqlmigrations/leasemanager"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

const (
	clientID1 = "1"
	clientID2 = "2"
)

var (
	leaseKey = roachpb.Key("/SystemVersion/lease")
)

func TestAcquireAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); !testutils.IsError(err, "unexpected value") {
		t.Fatal(err)
	}

	l, err = lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestReacquireLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	if _, err := lm.AcquireLease(ctx, leaseKey); err != nil {
		t.Fatal(err)
	}

	// We allow re-acquiring the same lease as long as the client ID is
	// the same to allow a client to reacquire its own leases rather than
	// having to wait them out if it crashes and restarts.
	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestExtendLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}

	manual.Increment(int64(time.Second))
	timeRemainingBefore := lm.TimeRemaining(l)
	if err := lm.ExtendLease(ctx, l); err != nil {
		t.Fatal(err)
	}
	timeRemainingAfter := lm.TimeRemaining(l)
	if !(timeRemainingAfter > timeRemainingBefore) {
		t.Errorf("expected time remaining after renewal (%s) to be greater than before renewal (%s)",
			timeRemainingAfter, timeRemainingBefore)
	}

	manual.Increment(int64(leasemanager.DefaultLeaseDuration) + 1)
	if tr := lm.TimeRemaining(l); tr >= 0 {
		t.Errorf("expected negative time remaining on lease, got %s", tr)
	}
	if err := lm.ExtendLease(ctx, l); !testutils.IsError(err, "can't extend lease that expired") {
		t.Fatalf("didn't get expected error when renewing lease %+v: %v", l, err)
	}

	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestLeasesMultipleClients(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual1 := hlc.NewManualClock(123)
	clock1 := hlc.NewClock(manual1.UnixNano, time.Nanosecond)
	manual2 := hlc.NewManualClock(123)
	clock2 := hlc.NewClock(manual2.UnixNano, time.Nanosecond)
	lm1 := leasemanager.New(db, clock1, leasemanager.Options{ClientID: clientID1})
	lm2 := leasemanager.New(db, clock2, leasemanager.Options{ClientID: clientID2})

	l1, err := lm1.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm2.AcquireLease(ctx, leaseKey)
	if !testutils.IsError(err, "is not available until") {
		t.Fatalf("didn't get expected error trying to acquire already held lease: %v", err)
	}
	if _, ok := err.(*leasemanager.LeaseNotAvailableError); !ok {
		t.Fatalf("expected LeaseNotAvailableError, got %v", err)
	}

	// Ensure a lease can be "stolen" after it's expired.
	manual2.Increment(int64(leasemanager.DefaultLeaseDuration) + 1)
	l2, err := lm2.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}

	// lm1's clock indicates that its lease should still be valid, but it doesn't
	// own it anymore.
	manual1.Increment(int64(leasemanager.DefaultLeaseDuration) / 2)
	if err := lm1.ExtendLease(ctx, l1); !testutils.IsError(err, "out of sync with DB state") {
		t.Fatalf("didn't get expected error trying to extend expired lease: %v", err)
	}
	if err := lm1.ReleaseLease(ctx, l1); !testutils.IsError(err, "unexpected value") {
		t.Fatalf("didn't get expected error trying to release stolen lease: %v", err)
	}

	if err := lm2.ReleaseLease(ctx, l2); err != nil {
		t.Fatal(err)
	}
}
