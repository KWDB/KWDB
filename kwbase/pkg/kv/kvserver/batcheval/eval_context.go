// Copyright 2017 The Cockroach Authors.
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

package batcheval

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/engine/tse"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/abortspan"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/limit"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"golang.org/x/time/rate"
)

// Limiters is the collection of per-store limits used during cmd evaluation.
type Limiters struct {
	BulkIOWriteRate              *rate.Limiter
	ConcurrentImportRequests     limit.ConcurrentRequestLimiter
	ConcurrentExportRequests     limit.ConcurrentRequestLimiter
	ConcurrentAddSSTableRequests limit.ConcurrentRequestLimiter
	// concurrentRangefeedIters is a semaphore used to limit the number of
	// rangefeeds in the "catch-up" state across the store. The "catch-up" state
	// is a temporary state at the beginning of a rangefeed which is expensive
	// because it uses an engine iterator.
	ConcurrentRangefeedIters limit.ConcurrentRequestLimiter
}

// EvalContext is the interface through which command evaluation accesses the
// underlying state.
type EvalContext interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	EvalKnobs() storagebase.BatchEvalTestingKnobs

	Engine() storage.Engine
	TsEngine() *tse.TsEngine
	Clock() *hlc.Clock
	DB() *kv.DB
	AbortSpan() *abortspan.AbortSpan
	GetConcurrencyManager() concurrency.Manager
	GetLimiters() *Limiters

	NodeID() roachpb.NodeID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	GetNodeLocality() roachpb.Locality

	IsFirstRange() bool
	GetFirstIndex() (uint64, error)
	GetTerm(uint64) (uint64, error)
	GetLeaseAppliedIndex() uint64

	Desc() *roachpb.RangeDescriptor
	ContainsKey(key roachpb.Key) bool

	// CanCreateTxnRecord determines whether a transaction record can be created
	// for the provided transaction information. See Replica.CanCreateTxnRecord
	// for details about its arguments, return values, and preconditions.
	CanCreateTxnRecord(
		txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
	) (ok bool, minCommitTS hlc.Timestamp, reason roachpb.TransactionAbortedReason)

	// GetMVCCStats returns a snapshot of the MVCC stats for the range.
	// If called from a command that declares a read/write span on the
	// entire range, the stats will be consistent with the data that is
	// visible to the batch. Otherwise, it may return inconsistent
	// results due to concurrent writes.
	GetMVCCStats() enginepb.MVCCStats

	// GetSplitQPS returns the queries/s request rate for this range.
	//
	// NOTE: This should not be used when the load based splitting cluster
	// setting is disabled.
	GetSplitQPS() float64

	GetGCThreshold() hlc.Timestamp
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, roachpb.Lease)

	GetExternalStorage(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error)
	GetExternalStorageFromURI(ctx context.Context, uri string) (cloud.ExternalStorage, error)
}

// MockEvalCtx is a dummy implementation of EvalContext for testing purposes.
// For technical reasons, the interface is implemented by a wrapper .EvalContext().
type MockEvalCtx struct {
	ClusterSettings  *cluster.Settings
	Desc             *roachpb.RangeDescriptor
	StoreID          roachpb.StoreID
	Clock            *hlc.Clock
	Stats            enginepb.MVCCStats
	QPS              float64
	AbortSpan        *abortspan.AbortSpan
	GCThreshold      hlc.Timestamp
	Term, FirstIndex uint64
	CanCreateTxn     func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason)
	Lease            roachpb.Lease
}

// EvalContext returns the MockEvalCtx as an EvalContext. It will reflect future
// modifications to the underlying MockEvalContext.
func (m *MockEvalCtx) EvalContext() EvalContext {
	return &mockEvalCtxImpl{m}
}

type mockEvalCtxImpl struct {
	// Hide the fields of MockEvalCtx which have names that conflict with some
	// of the interface methods.
	*MockEvalCtx
}

func (m *mockEvalCtxImpl) String() string {
	return "mock"
}
func (m *mockEvalCtxImpl) ClusterSettings() *cluster.Settings {
	return m.MockEvalCtx.ClusterSettings
}
func (m *mockEvalCtxImpl) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	return storagebase.BatchEvalTestingKnobs{}
}
func (m *mockEvalCtxImpl) Engine() storage.Engine {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) TsEngine() *tse.TsEngine {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) Clock() *hlc.Clock {
	return m.MockEvalCtx.Clock
}
func (m *mockEvalCtxImpl) DB() *kv.DB {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetLimiters() *Limiters {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) AbortSpan() *abortspan.AbortSpan {
	return m.MockEvalCtx.AbortSpan
}
func (m *mockEvalCtxImpl) GetConcurrencyManager() concurrency.Manager {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) NodeID() roachpb.NodeID {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetNodeLocality() roachpb.Locality {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) StoreID() roachpb.StoreID {
	return m.MockEvalCtx.StoreID
}
func (m *mockEvalCtxImpl) GetRangeID() roachpb.RangeID {
	return m.MockEvalCtx.Desc.RangeID
}
func (m *mockEvalCtxImpl) IsFirstRange() bool {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetFirstIndex() (uint64, error) {
	return m.FirstIndex, nil
}
func (m *mockEvalCtxImpl) GetTerm(uint64) (uint64, error) {
	return m.Term, nil
}
func (m *mockEvalCtxImpl) GetLeaseAppliedIndex() uint64 {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) Desc() *roachpb.RangeDescriptor {
	return m.MockEvalCtx.Desc
}
func (m *mockEvalCtxImpl) ContainsKey(key roachpb.Key) bool {
	return false
}
func (m *mockEvalCtxImpl) GetMVCCStats() enginepb.MVCCStats {
	return m.Stats
}
func (m *mockEvalCtxImpl) GetSplitQPS() float64 {
	return m.QPS
}
func (m *mockEvalCtxImpl) CanCreateTxnRecord(
	uuid.UUID, []byte, hlc.Timestamp,
) (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
	return m.CanCreateTxn()
}
func (m *mockEvalCtxImpl) GetGCThreshold() hlc.Timestamp {
	return m.GCThreshold
}
func (m *mockEvalCtxImpl) GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error) {
	panic("unimplemented")
}
func (m *mockEvalCtxImpl) GetLease() (roachpb.Lease, roachpb.Lease) {
	return m.Lease, roachpb.Lease{}
}

func (m *mockEvalCtxImpl) GetExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}

func (m *mockEvalCtxImpl) GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}
