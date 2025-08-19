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

package kv

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MockTransactionalSender allows a function to be used as a TxnSender.
type MockTransactionalSender struct {
	senderFunc func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error)
	txn roachpb.Transaction
}

// SetIsoLevel is part of the TxnSender interface.
func (m *MockTransactionalSender) SetIsoLevel(isoLevel enginepb.Level) error {
	m.txn.IsoLevel = isoLevel
	return nil
}

// IsoLevel is part of the TxnSender interface.
func (m *MockTransactionalSender) IsoLevel() enginepb.Level {
	return m.txn.IsoLevel
}

// GetRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) GetRetryableErr(
	ctx context.Context,
) *roachpb.TransactionRetryWithProtoRefreshError {
	return nil
}

// ClearRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) ClearRetryableErr(ctx context.Context) error {
	return nil
}

// NewMockTransactionalSender creates a MockTransactionalSender.
// The passed in txn is cloned.
func NewMockTransactionalSender(
	f func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error),
	txn *roachpb.Transaction,
) *MockTransactionalSender {
	return &MockTransactionalSender{senderFunc: f, txn: *txn}
}

// Send is part of the TxnSender interface.
func (m *MockTransactionalSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return m.senderFunc(ctx, &m.txn, ba)
}

// GetLeafTxnInputState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnInputState(
	context.Context, TxnStatusOpt,
) (roachpb.LeafTxnInputState, error) {
	panic("unimplemented")
}

// GetLeafTxnFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnFinalState(
	context.Context, TxnStatusOpt,
) (roachpb.LeafTxnFinalState, error) {
	panic("unimplemented")
}

// UpdateRootWithLeafFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateRootWithLeafFinalState(
	context.Context, *roachpb.LeafTxnFinalState,
) {
	panic("unimplemented")
}

// AnchorOnSystemConfigRange is part of the TxnSender interface.
func (m *MockTransactionalSender) AnchorOnSystemConfigRange() error {
	return errors.New("unimplemented")
}

// TxnStatus is part of the TxnSender interface.
func (m *MockTransactionalSender) TxnStatus() roachpb.TransactionStatus {
	return m.txn.Status
}

// SetUserPriority is part of the TxnSender interface.
func (m *MockTransactionalSender) SetUserPriority(pri roachpb.UserPriority) error {
	m.txn.Priority = roachpb.MakePriority(pri)
	return nil
}

// SetDebugName is part of the TxnSender interface.
func (m *MockTransactionalSender) SetDebugName(name string) {
	m.txn.Name = name
}

// SetUnconsistency is part of the TxnSender interface.
func (m *MockTransactionalSender) SetUnconsistency(unconsistency bool) {
	m.txn.UnConsistency = unconsistency
}

// String is part of the TxnSender interface.
func (m *MockTransactionalSender) String() string {
	return m.txn.String()
}

// ReadTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ReadTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// ProvisionalCommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ProvisionalCommitTimestamp() hlc.Timestamp {
	return m.txn.WriteTimestamp
}

// CommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestamp() (hlc.Timestamp, error) {
	return m.txn.ReadTimestamp, nil
}

// CommitTimestampFixed is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestampFixed() bool {
	panic("unimplemented")
}

// SetFixedTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) SetFixedTimestamp(_ context.Context, ts hlc.Timestamp) {
	m.txn.WriteTimestamp = ts
	m.txn.ReadTimestamp = ts
	m.txn.MaxTimestamp = ts
	m.txn.CommitTimestampFixed = true

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	m.txn.MinTimestamp.Backward(ts)

	// For backwards compatibility with 19.2, set the DeprecatedOrigTimestamp too (although
	// not really needed by this Mock sender).
	m.txn.DeprecatedOrigTimestamp = ts
}

// ManualRestart is part of the TxnSender interface.
func (m *MockTransactionalSender) ManualRestart(
	ctx context.Context, pri roachpb.UserPriority, ts hlc.Timestamp,
) {
	if !(m.txn.IsoLevel == enginepb.ReadCommitted) {
		m.txn.Restart(pri, 0 /* upgradePriority */, ts)
	} else {
		m.txn.BumpReadTimestamp(ts)
	}
}

// IsSerializablePushAndRefreshNotPossible is part of the TxnSender interface.
func (m *MockTransactionalSender) IsSerializablePushAndRefreshNotPossible() bool {
	return false
}

// CreateSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) CreateSavepoint(context.Context) (SavepointToken, error) {
	panic("unimplemented")
}

// RollbackToSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) RollbackToSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// ReleaseSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) ReleaseSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// Epoch is part of the TxnSender interface.
func (m *MockTransactionalSender) Epoch() enginepb.TxnEpoch { panic("unimplemented") }

// TestingCloneTxn is part of the TxnSender interface.
func (m *MockTransactionalSender) TestingCloneTxn() *roachpb.Transaction {
	return m.txn.Clone()
}

// Active is part of the TxnSender interface.
func (m *MockTransactionalSender) Active() bool {
	panic("unimplemented")
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.Error {
	panic("unimplemented")
}

// DisablePipelining is part of the client.TxnSender interface.
func (m *MockTransactionalSender) DisablePipelining() error { return nil }

// PrepareRetryableError is part of the client.TxnSender interface.
func (m *MockTransactionalSender) PrepareRetryableError(ctx context.Context, msg string) error {
	return roachpb.NewTransactionRetryWithProtoRefreshError(msg, m.txn.ID, *m.txn.Clone())
}

// Step is part of the TxnSender interface.
func (m *MockTransactionalSender) Step(_ context.Context, _ bool) error {
	// At least one test (e.g sql/TestPortalsDestroyedOnTxnFinish) requires
	// the ability to run simple statements that do not access storage,
	// and that requires a non-panicky Step().
	return nil
}

// ConfigureStepping is part of the TxnSender interface.
func (m *MockTransactionalSender) ConfigureStepping(context.Context, SteppingMode) SteppingMode {
	// See Step() above.
	return SteppingDisabled
}

// GetSteppingMode is part of the TxnSender interface.
func (m *MockTransactionalSender) GetSteppingMode(context.Context) SteppingMode {
	return SteppingDisabled
}

// MockTxnSenderFactory is a TxnSenderFactory producing MockTxnSenders.
type MockTxnSenderFactory struct {
	senderFunc func(context.Context, *roachpb.Transaction, roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error)
}

var _ TxnSenderFactory = MockTxnSenderFactory{}

// MakeMockTxnSenderFactory creates a MockTxnSenderFactory from a sender
// function that receives the transaction in addition to the request. The
// function is responsible for putting the txn inside the batch, if needed.
func MakeMockTxnSenderFactory(
	senderFunc func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error),
) MockTxnSenderFactory {
	return MockTxnSenderFactory{
		senderFunc: senderFunc,
	}
}

// RootTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) RootTransactionalSender(
	txn *roachpb.Transaction, _ roachpb.UserPriority,
) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, txn)
}

// LeafTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) LeafTransactionalSender(tis *roachpb.LeafTxnInputState) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, &tis.Txn)
}

// NonTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) NonTransactionalSender() Sender {
	return nil
}
