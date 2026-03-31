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
	"bytes"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// Helper function to create a test table descriptor
func createKWDBTestTableDescriptor() sqlbase.TableDescriptor {
	return sqlbase.TableDescriptor{
		ID:           100,
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1},
		Families:     []sqlbase.ColumnFamilyDescriptor{{ID: 0}, {ID: 1}, {ID: 2}},
		NextFamilyID: 3,
		Indexes:      []sqlbase.IndexDescriptor{},
	}
}

// Helper function to create a test table descriptor with columns
func createKWDBTestTableDescriptorWithColumns() sqlbase.TableDescriptor {
	return sqlbase.TableDescriptor{
		ID:           100,
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, ColumnIDs: []sqlbase.ColumnID{1, 2}},
		Families:     []sqlbase.ColumnFamilyDescriptor{{ID: 0}, {ID: 1}, {ID: 2}},
		NextFamilyID: 3,
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Type: *types.Int, Name: "col1"},
			{ID: 2, Type: *types.String, Name: "col2"},
		},
		Indexes: []sqlbase.IndexDescriptor{},
	}
}

func TestMakeKWDBMetadataKeyInt(t *testing.T) {
	tableDesc := createKWDBTestTableDescriptor()

	// Test with empty primary values
	key, err := sqlbase.MakeKWDBMetadataKeyInt(tableDesc, []uint64{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedStart := keys.MakeTablePrefix(uint32(tableDesc.ID))
	leading := key[0:3]
	if bytes.Compare(leading, expectedStart) == 0 {
		t.Errorf("Expected key to start with table prefix, got %v", key)
	}

	// Test with single primary value
	key, err = sqlbase.MakeKWDBMetadataKeyInt(tableDesc, []uint64{42})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedKey := keys.MakeTablePrefix(uint32(tableDesc.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDesc.PrimaryIndex.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(42))

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}

	// Test with multiple primary values
	key, err = sqlbase.MakeKWDBMetadataKeyInt(tableDesc, []uint64{42, 24})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedKey = keys.MakeTablePrefix(uint32(tableDesc.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDesc.PrimaryIndex.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(42))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(24))

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}

	// Test with primary values matching column count (should add family key)
	tableDescWithCols := createKWDBTestTableDescriptorWithColumns()
	key, err = sqlbase.MakeKWDBMetadataKeyInt(tableDescWithCols, []uint64{42, 24})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedKey = keys.MakeTablePrefix(uint32(tableDescWithCols.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDescWithCols.PrimaryIndex.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(42))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(24))
	expectedKey = keys.MakeFamilyKey(expectedKey, uint32(tableDescWithCols.NextFamilyID-1))

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}
}

func TestMakeKWDBMetadataKeyString(t *testing.T) {
	tableDesc := createKWDBTestTableDescriptor()

	// Test with empty primary values
	key := sqlbase.MakeKWDBMetadataKeyString(tableDesc, []string{})
	expectedStart := keys.MakeTablePrefix(uint32(tableDesc.ID))
	leading := key[0:3]
	if bytes.Compare(leading, expectedStart) == 0 {
		t.Errorf("Expected key to start with table prefix, got %v", key)
	}

	// Test with single primary value
	key = sqlbase.MakeKWDBMetadataKeyString(tableDesc, []string{"test"})
	expectedKey := keys.MakeTablePrefix(uint32(tableDesc.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDesc.PrimaryIndex.ID))
	expectedKey = encoding.EncodeStringAscending(expectedKey, "test")

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}

	// Test with multiple primary values
	key = sqlbase.MakeKWDBMetadataKeyString(tableDesc, []string{"test", "value"})
	expectedKey = keys.MakeTablePrefix(uint32(tableDesc.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDesc.PrimaryIndex.ID))
	expectedKey = encoding.EncodeStringAscending(expectedKey, "test")
	expectedKey = encoding.EncodeStringAscending(expectedKey, "value")

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}

	// Test with primary values matching column count (should add family key)
	tableDescWithCols := createKWDBTestTableDescriptorWithColumns()
	key = sqlbase.MakeKWDBMetadataKeyString(tableDescWithCols, []string{"test", "value"})
	expectedKey = keys.MakeTablePrefix(uint32(tableDescWithCols.ID))
	expectedKey = encoding.EncodeUvarintAscending(expectedKey, uint64(tableDescWithCols.PrimaryIndex.ID))
	expectedKey = encoding.EncodeStringAscending(expectedKey, "test")
	expectedKey = encoding.EncodeStringAscending(expectedKey, "value")
	expectedKey = keys.MakeFamilyKey(expectedKey, uint32(tableDescWithCols.Families[1].ID))
	expectedKey = keys.MakeFamilyKey(expectedKey, uint32(tableDescWithCols.NextFamilyID-1))

	if !roachpb.Key(key).Equal(expectedKey) {
		t.Errorf("Expected key %v, got %v", expectedKey, key)
	}
}

func TestDecodeKWDBTableKey(t *testing.T) {
	tableDesc := createKWDBTestTableDescriptorWithColumns()
	index := &tableDesc.PrimaryIndex

	// Create a key with integer values
	key := keys.MakeTablePrefix(uint32(tableDesc.ID))
	key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	key = encoding.EncodeUvarintAscending(key, uint64(42))
	key = encoding.EncodeStringAscending(key, "test")

	result, err := sqlbase.DecodeKWDBTableKey(key, index, tableDesc)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 decoded values, got %d", len(result))
	}

	if dInt, ok := result[0].(*tree.DInt); !ok || int(*dInt) != 42 {
		t.Errorf("Expected first value to be DInt(42), got %v", result[0])
	}

	if dStr, ok := result[1].(*tree.DString); !ok || string(*dStr) != "test" {
		t.Errorf("Expected second value to be DString('test'), got %v", result[1])
	}

	// Test with wrong index ID
	wrongKey := keys.MakeTablePrefix(uint32(tableDesc.ID))
	wrongKey = encoding.EncodeUvarintAscending(wrongKey, uint64(999)) // Wrong index ID

	_, err = sqlbase.DecodeKWDBTableKey(wrongKey, index, tableDesc)
	if err == nil {
		t.Error("Expected error for wrong index ID, got none")
	}
}

func TestMakeDropKWDBMetadataKeyInt(t *testing.T) {
	tableDesc := createKWDBTestTableDescriptor()

	// Test with empty values
	dropKeys, err := sqlbase.MakeDropKWDBMetadataKeyInt(tableDesc, []uint64{}, []tree.Datum{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(dropKeys) != len(tableDesc.Families) {
		t.Errorf("Expected %d keys (number of families), got %d", len(tableDesc.Families), len(dropKeys))
	}

	// Test with primary values
	dropKeys, err = sqlbase.MakeDropKWDBMetadataKeyInt(tableDesc, []uint64{42}, []tree.Datum{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Check that we have keys for each family
	expectedPK := keys.MakeTablePrefix(uint32(tableDesc.ID))
	expectedPK = encoding.EncodeUvarintAscending(expectedPK, uint64(tableDesc.PrimaryIndex.ID))
	expectedPK = encoding.EncodeUvarintAscending(expectedPK, uint64(42))

	for i, fam := range tableDesc.Families {
		expectedFamKey := keys.MakeFamilyKey(expectedPK, uint32(fam.ID))
		if !dropKeys[i].Equal(expectedFamKey) {
			t.Errorf("Expected family key %v at index %d, got %v", expectedFamKey, i, dropKeys[i])
		}
	}
}
