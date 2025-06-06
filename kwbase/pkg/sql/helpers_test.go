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

package sql

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// A unique id for a particular table descriptor version.
type tableVersionID struct {
	id      sqlbase.ID
	version sqlbase.DescriptorVersion
}

// LeaseRemovalTracker can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. LeaseManager
// operations).
// To use it, its LeaseRemovedNotification method must be hooked up to
// LeaseStoreTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, get a tracker object through TrackRemoval() before calling
// LeaseManager.Release(), and then call WaitForRemoval() on the tracker to
// block for the removal from the store.
//
// All methods are thread-safe.
type LeaseRemovalTracker struct {
	mu syncutil.Mutex
	// map from a lease whose release we're waiting for to a tracker for that
	// lease.
	tracking map[tableVersionID]RemovalTracker
}

type RemovalTracker struct {
	removed chan struct{}
	// Pointer to a shared err. *err is written when removed is closed.
	err *error
}

// NewLeaseRemovalTracker creates a LeaseRemovalTracker.
func NewLeaseRemovalTracker() *LeaseRemovalTracker {
	return &LeaseRemovalTracker{
		tracking: make(map[tableVersionID]RemovalTracker),
	}
}

// TrackRemoval starts monitoring lease removals for a particular lease.
// This should be called before triggering the operation that (asynchronously)
// removes the lease.
func (w *LeaseRemovalTracker) TrackRemoval(table *sqlbase.ImmutableTableDescriptor) RemovalTracker {
	id := tableVersionID{
		id:      table.ID,
		version: table.Version,
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[id]; ok {
		return tracker
	}
	tracker := RemovalTracker{removed: make(chan struct{}), err: new(error)}
	w.tracking[id] = tracker
	return tracker
}

// WaitForRemoval blocks until the lease is removed from the store.
func (t RemovalTracker) WaitForRemoval() error {
	<-t.removed
	return *t.err
}

// LeaseRemovedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// LeaseStoreTestingKnobs.LeaseReleasedEvent.
func (w *LeaseRemovalTracker) LeaseRemovedNotification(
	id sqlbase.ID, version sqlbase.DescriptorVersion, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()

	idx := tableVersionID{
		id:      id,
		version: version,
	}

	if tracker, ok := w.tracking[idx]; ok {
		*tracker.err = err
		close(tracker.removed)
		delete(w.tracking, idx)
	}
}

func (m *LeaseManager) ExpireLeases(clock *hlc.Clock) {
	past := clock.Now().GoTime().Add(-time.Millisecond)

	m.tableNames.mu.Lock()
	for _, table := range m.tableNames.tables {
		table.expiration = hlc.Timestamp{WallTime: past.UnixNano()}
	}
	m.tableNames.mu.Unlock()
}

// AcquireAndAssertMinVersion acquires a read lease for the specified table ID.
// The lease is grabbed on the latest version if >= specified version.
// It returns a table descriptor and an expiration time valid for the timestamp.
func (m *LeaseManager) AcquireAndAssertMinVersion(
	ctx context.Context,
	timestamp hlc.Timestamp,
	tableID sqlbase.ID,
	minVersion sqlbase.DescriptorVersion,
) (*sqlbase.ImmutableTableDescriptor, hlc.Timestamp, error) {
	t := m.findTableState(tableID, true)
	if err := ensureVersion(ctx, tableID, minVersion, m); err != nil {
		return nil, hlc.Timestamp{}, err
	}
	table, _, err := t.findForTimestamp(ctx, timestamp)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return &table.ImmutableTableDescriptor, table.expiration, nil
}
