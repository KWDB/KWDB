// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// mockInternalExecutor is a mock implementation of sqlutil.InternalExecutor
// for testing purposes.
type mockInternalExecutor struct {
	queryResults [][]tree.Datums
	err          error
}

func (m *mockInternalExecutor) Exec(
	ctx context.Context, opName string, txn *kv.Txn, statement string, params ...interface{},
) (int, error) {
	return 0, m.err
}

func (m *mockInternalExecutor) ExecEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	o sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	return 0, m.err
}

func (m *mockInternalExecutor) Query(
	ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
) ([]tree.Datums, error) {
	if len(m.queryResults) > 0 {
		return m.queryResults[0], m.err
	}
	return nil, m.err
}

func (m *mockInternalExecutor) QueryEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	if len(m.queryResults) > 0 {
		return m.queryResults[0], m.err
	}
	return nil, m.err
}

func (m *mockInternalExecutor) QueryWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	o sqlbase.InternalExecutorSessionDataOverride,
	statement string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	if len(m.queryResults) > 0 {
		return m.queryResults[0], nil, m.err
	}
	return nil, nil, m.err
}

func (m *mockInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	if len(m.queryResults) > 0 && len(m.queryResults[0]) > 0 {
		return m.queryResults[0][0], m.err
	}
	return nil, m.err
}

func (m *mockInternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	if len(m.queryResults) > 0 && len(m.queryResults[0]) > 0 {
		return m.queryResults[0][0], m.err
	}
	return nil, m.err
}

func TestUDFCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	defer stopper.Stop(ctx)

	// Create mock dependencies
	mockG := gossip.NewTest(1, rpcContext, rpc.NewServer(rpcContext), stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	mockDB := &kv.DB{}
	mockExecutor := &mockInternalExecutor{}

	// Create UDF cache
	cacheSize := 10
	udfCache := NewUDFCache(cacheSize, mockG, mockDB, mockExecutor)

	// Test case 1: GetUDF with non-existent UDF (should return error)
	udfName := "test_udf"
	mockExecutor.err = nil
	mockExecutor.queryResults = [][]tree.Datums{} // Empty result set

	_, err := udfCache.GetUDF(ctx, udfName)
	if err == nil {
		t.Error("GetUDF should return error for non-existent UDF")
	}

	// Test case 2: DeleteUDF for non-existent UDF (should not panic)
	udfCache.DeleteUDF(ctx, "non_existent_udf")

	// Test case 3: Gossip functions
	err = GossipUdfAdded(mockG, "gossip_test_udf")
	if err != nil {
		t.Errorf("GossipUdfAdded should not return error, got %v", err)
	}

	err = GossipUdfDeleted(mockG, "gossip_test_udf")
	if err != nil {
		t.Errorf("GossipUdfDeleted should not return error, got %v", err)
	}

	// Test case 4: LoadUDF
	err = udfCache.LoadUDF(ctx, stopper)
	if err != nil {
		t.Errorf("LoadUDF should not return error, got %v", err)
	}

	// Test case 5: RefreshUDF
	udfCache.RefreshUDF(ctx, "refresh_test_udf")
}

func TestUDFEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a UDFEntry
	entry := &UDFEntry{
		mustWait: false,
	}

	// Test Fn field
	if entry.Fn != nil {
		t.Error("UDFEntry.Fn should be nil initially")
	}

	// Test err field
	if entry.err != nil {
		t.Error("UDFEntry.err should be nil initially")
	}

	// Test refreshing and mustRefreshAgain fields
	if entry.refreshing {
		t.Error("UDFEntry.refreshing should be false initially")
	}

	if entry.mustRefreshAgain {
		t.Error("UDFEntry.mustRefreshAgain should be false initially")
	}

	// Test mustWait field
	if entry.mustWait {
		t.Error("UDFEntry.mustWait should be false as set")
	}
}

func TestUDFCacheLookupUdfLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	defer stopper.Stop(ctx)

	// Create mock dependencies
	mockG := gossip.NewTest(1, rpcContext, rpc.NewServer(rpcContext), stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	mockDB := &kv.DB{}
	mockExecutor := &mockInternalExecutor{}

	// Create UDF cache
	cacheSize := 10
	udfCache := NewUDFCache(cacheSize, mockG, mockDB, mockExecutor)

	// Add a UDF entry to the cache
	udfName := "test_udf"
	entry := &UDFEntry{
		mustWait: false,
		Fn:       &tree.FunctionDefinition{Name: udfName},
	}

	udfCache.mu.Lock()
	udfCache.mu.cache.Add(udfName, entry)

	// Test lookupUdfLocked
	found, e := udfCache.lookupUdfLocked(ctx, udfName)
	if !found {
		t.Error("lookupUdfLocked should find the UDF entry")
	}

	if e != entry {
		t.Error("lookupUdfLocked should return the correct UDF entry")
	}

	udfCache.mu.Unlock()

	// Test lookupUdfLocked for non-existent UDF
	udfCache.mu.Lock()
	found, e = udfCache.lookupUdfLocked(ctx, "non_existent_udf")
	if found {
		t.Error("lookupUdfLocked should not find non-existent UDF")
	}

	if e != nil {
		t.Error("lookupUdfLocked should return nil for non-existent UDF")
	}
	udfCache.mu.Unlock()
}
