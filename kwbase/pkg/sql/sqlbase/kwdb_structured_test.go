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

package sqlbase_test

import (
	"context"
	"encoding/binary"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/stretchr/testify/assert"
)

func TestDecodeHashPointFromPayload(t *testing.T) {
	// Create a payload with known range group ID
	payload := make([]byte, 18) // At least 16+2 bytes to accommodate RangeGroupIDOffset and RangeGroupIDSize
	binary.LittleEndian.PutUint16(payload[sqlbase.RangeGroupIDOffset:], 12345)

	result := sqlbase.DecodeHashPointFromPayload(payload)
	expected := []api.HashPoint{api.HashPoint(12345)}

	assert.Equal(t, expected, result)
}

func TestDecodeOsnIDFromPayload(t *testing.T) {
	// Create a payload with known osn ID
	payload := make([]byte, 16) // At least 16 bytes to accommodate OsnIDSize
	binary.LittleEndian.PutUint64(payload[sqlbase.OsnIDOffset:], 9876543210)

	result := sqlbase.DecodeOsnIDFromPayload(payload)
	expected := uint64(9876543210)

	assert.Equal(t, expected, result)
}

func TestGetKWDBMetadataRow(t *testing.T) {
	ctx := context.Background()

	// Test with nil transaction
	_, err := sqlbase.GetKWDBMetadataRow(ctx, nil, roachpb.Key("test"), sqlbase.TableDescriptor{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must provide a non-nil transaction")
}

func TestGetKWDBMetadataRows(t *testing.T) {
	ctx := context.Background()

	// Test with nil transaction
	_, err := sqlbase.GetKWDBMetadataRows(ctx, nil, roachpb.Key("test"), sqlbase.TableDescriptor{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must provide a non-nil transaction")
}

func TestGetKWDBMetadataRowWithUnConsistency(t *testing.T) {
	ctx := context.Background()

	// Test with nil transaction
	_, err := sqlbase.GetKWDBMetadataRowWithUnConsistency(ctx, nil, roachpb.Key("test"), sqlbase.TableDescriptor{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must provide a non-nil transaction")
}

func TestGetKWDBMetadataRowsWithUnConsistency(t *testing.T) {
	ctx := context.Background()

	// Test with nil transaction
	_, err := sqlbase.GetKWDBMetadataRowsWithUnConsistency(ctx, nil, roachpb.Key("test"), sqlbase.TableDescriptor{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must provide a non-nil transaction")
}

func TestGetMetadataRowsFromKeyValue(t *testing.T) {
	// Create a mock table descriptor
	tableDesc := sqlbase.TableDescriptor{
		ID:   1,
		Name: "test_table",
		Indexes: []sqlbase.IndexDescriptor{
			{
				ID:          1,
				Name:        "primary_index",
				ColumnIDs:   []sqlbase.ColumnID{1},
				ColumnNames: []string{"id"},
			},
		},
		Columns: []sqlbase.ColumnDescriptor{
			{
				ID:   1,
				Name: "id",
			},
		},
	}

	// Create a key with proper encoding
	k := keys.MakeTablePrefix(uint32(tableDesc.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(tableDesc.Indexes[0].ID+sqlbase.IntZero)) // Add IntZero offset

	// Create a mock KeyValue
	keyValue := &kv.KeyValue{
		Key:   k,
		Value: &roachpb.Value{},
	}

	// Test with a non-existent index
	_, err := sqlbase.GetMetadataRowsFromKeyValue(context.Background(), keyValue, &tableDesc)
	assert.NotNil(t, err)
}

func TestNeedConvert(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"valid string", false},
		{"", false},
		{"\x00", true},      // Null byte
		{"\x01", true},      // Control character
		{"\x1F", true},      // Control character
		{"\x7F", true},      // Delete character
		{"\x20", false},     // Space character (not in the control range)
		{"\xC2\xA0", false}, // Valid UTF-8 (non-breaking space)
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sqlbase.NeedConvert(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDatumToString(t *testing.T) {
	tests := []struct {
		input    tree.Datum
		expected string
	}{
		{tree.NewDString("test_string"), "test_string"},
		{tree.NewDBytes(tree.DBytes("test_bytes")), "test_bytes"},
		{tree.DNull, "NULL"}, // Assuming DNull converts to "NULL"
		{tree.NewDInt(42), "42"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := sqlbase.DatumToString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInstNameSpace(t *testing.T) {
	// Test the InstNameSpace struct
	instNS := sqlbase.InstNameSpace{
		InstName:    "test_instance",
		InstTableID: 123,
		TmplTableID: 456,
		DBName:      "test_db",
	}

	assert.Equal(t, "test_instance", instNS.InstName)
	assert.Equal(t, sqlbase.ID(123), instNS.InstTableID)
	assert.Equal(t, sqlbase.ID(456), instNS.TmplTableID)
	assert.Equal(t, "test_db", instNS.DBName)
}

func TestAllInstTableInfo(t *testing.T) {
	// Test the AllInstTableInfo struct
	info := sqlbase.AllInstTableInfo{
		InstTableIDs:   []sqlbase.ID{1, 2, 3},
		InstTableNames: []string{"table1", "table2", "table3"},
	}

	assert.Equal(t, 3, len(info.InstTableIDs))
	assert.Equal(t, 3, len(info.InstTableNames))
	assert.Equal(t, sqlbase.ID(1), info.InstTableIDs[0])
	assert.Equal(t, "table1", info.InstTableNames[0])
}

// Mock transaction for testing
type mockTxn struct {
	getError               error
	scanError              error
	getUnConsistencyError  error
	scanUnConsistencyError error
}

func (m *mockTxn) Get(ctx context.Context, key roachpb.Key) (kv.KeyValue, error) {
	return kv.KeyValue{}, m.getError
}

func (m *mockTxn) Scan(ctx context.Context, begin, end roachpb.Key, max int64) ([]kv.KeyValue, error) {
	return nil, m.scanError
}

func (m *mockTxn) GetWithUnConsistency(ctx context.Context, key roachpb.Key) (kv.KeyValue, error) {
	return kv.KeyValue{}, m.getUnConsistencyError
}

func (m *mockTxn) ScanWithUnReadConsistency(ctx context.Context, begin, end roachpb.Key, max int64) ([]kv.KeyValue, error) {
	return nil, m.scanUnConsistencyError
}

// Implement other required methods for kv.Txn interface
func (m *mockTxn) AddCommitTrigger(f func())        {}
func (m *mockTxn) AddOnFinish(func(error))          {}
func (m *mockTxn) Commit(ctx context.Context) error { return nil }
func (m *mockTxn) IsFinalized() bool                { return false }
func (m *mockTxn) Isolation() interface{}           { return nil }
func (m *mockTxn) Status() interface{}              { return nil }
func (m *mockTxn) Type() interface{}                { return nil }
func (m *mockTxn) UpdateStateOnRemoteRetryableErr(ctx context.Context, retryErr interface{}) error {
	return nil
}
func (m *mockTxn) OrigTimestamp() interface{}                            { return nil }
func (m *mockTxn) CommitTimestamp() (interface{}, error)                 { return nil, nil }
func (m *mockTxn) CommitTimestampFixed() bool                            { return false }
func (m *mockTxn) SetFixedTimestamp(ctx context.Context, ts interface{}) {}
func (m *mockTxn) UserPriority() interface{}                             { return nil }
func (m *mockTxn) InternalAddRequest(req roachpb.Request)                {}
func (m *mockTxn) Epoch() uint32                                         { return 0 }
func (m *mockTxn) Serialize() ([]byte, error)                            { return nil, nil }
func (m *mockTxn) SetSystemConfigTrigger()                               {}
func (m *mockTxn) SetAsyncError(err error)                               {}
func (m *mockTxn) GetTxnCoordMeta(ctx context.Context) interface{} {
	return nil
}
func (m *mockTxn) GetStrippedTxnCoordMeta() interface{}          { return nil }
func (m *mockTxn) WithPriority(priority interface{})             {}
func (m *mockTxn) DisablePipelining() error                      { return nil }
func (m *mockTxn) IsSerializablePushAndRefreshNotPossible() bool { return false }
func (m *mockTxn) GenerateForcedRetryableError(ctx context.Context, msg string) error {
	return nil
}
func (m *mockTxn) MaybeWatchForConcurrentRefreshSpans(ctx context.Context) error { return nil }
func (m *mockTxn) Refresh(ctx context.Context, refreshAtDelta int32) error       { return nil }
func (m *mockTxn) TypeForKey(key roachpb.Key) interface{}                        { return nil }
