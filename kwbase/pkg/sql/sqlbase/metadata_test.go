// Copyright 2023 The KWDB Authors.
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

package sqlbase_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

func TestWrapDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test wrapping a TableDescriptor
	tableDesc := &sqlbase.MutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   123,
			Name: "test_table",
		},
	}
	wrappedTable := sqlbase.WrapDescriptor(tableDesc)
	if wrappedTable.GetTable() == nil {
		t.Error("Expected Table field to be set for TableDescriptor")
	}
	if wrappedTable.GetTable().ID != 123 {
		t.Errorf("Expected ID 123, got %d", wrappedTable.GetTable().ID)
	}

	// Test wrapping a DatabaseDescriptor
	dbDesc := &sqlbase.DatabaseDescriptor{
		ID:   456,
		Name: "test_db",
	}
	wrappedDB := sqlbase.WrapDescriptor(dbDesc)
	if wrappedDB.GetDatabase() == nil {
		t.Error("Expected Database field to be set for DatabaseDescriptor")
	}
	if wrappedDB.GetDatabase().ID != 456 {
		t.Errorf("Expected ID 456, got %d", wrappedDB.GetDatabase().ID)
	}

	// Test wrapping a SchemaDescriptor
	schemaDesc := &sqlbase.SchemaDescriptor{
		ID:   789,
		Name: "test_schema",
	}
	wrappedSchema := sqlbase.WrapDescriptor(schemaDesc)
	if wrappedSchema.GetSchema() == nil {
		t.Error("Expected Schema field to be set for SchemaDescriptor")
	}
	if wrappedSchema.GetSchema().ID != 789 {
		t.Errorf("Expected ID 789, got %d", wrappedSchema.GetSchema().ID)
	}
}

func TestMetadataSchemaBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	// Create a new metadata schema
	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Test SystemDescriptorCount
	count := ms.SystemDescriptorCount()
	if count <= 0 {
		t.Errorf("Expected positive descriptor count, got %d", count)
	}

	// Test DescriptorIDs
	ids := ms.DescriptorIDs()
	if len(ids) != count {
		t.Errorf("Expected %d IDs, got %d", count, len(ids))
	}

	// Verify IDs are sorted
	if !sort.IsSorted(ids) {
		t.Error("Expected sorted IDs")
	}
}

func TestMetadataSchemaAddDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Create a test table descriptor
	tableDesc := &sqlbase.TableDescriptor{
		ID:         70, // Reserved ID
		Name:       "test_table",
		ParentID:   keys.RootNamespaceID,
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}

	// Add the descriptor
	ms.AddDescriptor(keys.RootNamespaceID, tableDesc)

	// Verify it was added
	if ms.SystemDescriptorCount() == 0 {
		t.Error("Expected descriptor to be added")
	}

	// Try to add a descriptor with ID > MaxReservedDescID (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when adding descriptor with ID > MaxReservedDescID")
		}
	}()

	tableDescBad := &sqlbase.TableDescriptor{
		ID:         keys.MaxReservedDescID + 1,
		Name:       "bad_table",
		ParentID:   keys.RootNamespaceID,
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}
	ms.AddDescriptor(keys.RootNamespaceID, tableDescBad)
}

func TestMetadataSchemaAddSplitIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Add some split IDs
	testIDs := []uint32{100, 200, 300}
	ms.AddSplitIDs(testIDs...)

	// Get initial values to check if split IDs are included
	cv := clusterversion.TestingClusterVersion
	_, splits := ms.GetInitialValues(cv)

	// Verify that splits contain our IDs
	expectedSplits := make([]roachpb.RKey, len(testIDs))
	for i, id := range testIDs {
		expectedSplits[i] = roachpb.RKey(keys.MakeTablePrefix(id))
	}

	// Sort both slices for comparison
	sort.Slice(expectedSplits, func(i, j int) bool {
		return expectedSplits[i].Less(expectedSplits[j])
	})
	sort.Slice(splits, func(i, j int) bool {
		return splits[i].Less(splits[j])
	})

	if len(splits) < len(expectedSplits) {
		t.Errorf("Expected at least %d splits, got %d", len(expectedSplits), len(splits))
	}

	// Check if our expected splits are in the actual splits
	for _, expectedSplit := range expectedSplits {
		found := false
		for _, actualSplit := range splits {
			if expectedSplit.Equal(actualSplit) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected split %s not found in actual splits", expectedSplit)
		}
	}
}

func TestGetInitialValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Get initial values
	cv := clusterversion.TestingClusterVersion
	kvs, splits := ms.GetInitialValues(cv)

	// Verify we have some key-value pairs
	if len(kvs) == 0 {
		t.Error("Expected some key-value pairs")
	}

	// Verify we have the DescIDGenerator key
	foundIDGen := false
	for _, kv := range kvs {
		if kv.Key.Equal(keys.DescIDGenerator) {
			foundIDGen = true
			if val, err := kv.Value.GetInt(); err != nil || val != int64(keys.MinUserDescID) {
				t.Errorf("Expected DescIDGenerator to have value %d, got %d", keys.MinUserDescID, val)
			}
			break
		}
	}
	if !foundIDGen {
		t.Error("Expected to find DescIDGenerator key")
	}

	// Verify splits are sorted
	sortedSplits := make([]roachpb.RKey, len(splits))
	copy(sortedSplits, splits)
	sort.Slice(sortedSplits, func(i, j int) bool {
		return sortedSplits[i].Less(sortedSplits[j])
	})
	if !reflect.DeepEqual(splits, sortedSplits) {
		t.Error("Expected splits to be sorted")
	}
}

func TestLookupSystemTableDescriptorID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Test with non-system DB ID
	id := sqlbase.LookupSystemTableDescriptorID(ctx, st, 999, "some_table")
	if id != sqlbase.InvalidID {
		t.Errorf("Expected InvalidID for non-system DB, got %d", id)
	}

	// Test with system DB ID but unknown table
	id = sqlbase.LookupSystemTableDescriptorID(ctx, st, sqlbase.SystemDB.ID, "unknown_table")
	if id != sqlbase.InvalidID {
		t.Errorf("Expected InvalidID for unknown table, got %d", id)
	}

	// Test with a known system table (like namespace table)
	id = sqlbase.LookupSystemTableDescriptorID(ctx, st, sqlbase.SystemDB.ID, sqlbase.NamespaceTableName)
	if id == sqlbase.InvalidID {
		t.Errorf("Expected valid ID for namespace table, got InvalidID")
	}
	if id != keys.NamespaceTableID {
		t.Errorf("Expected namespace table ID %d, got %d", keys.NamespaceTableID, id)
	}
}

func TestWhiteListMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Initialize WhiteListMap
	wlm := &sqlbase.WhiteListMap{
		Map: make(map[uint32]sqlbase.FilterInfo),
	}

	// Add a test entry
	key := uint32(123)
	filterInfo := sqlbase.FilterInfo{
		Pos: 0x05, // Binary: 00000101 - positions 0 and 2 are set
		Typ: 0x02, // Binary: 0000010 - type 1 is set
	}
	wlm.Map[key] = filterInfo

	// Test CanPushByPos
	// Position 0 is set, so CanPushByPos should return false (can't push)
	if wlm.CanPushByPos(key, 0) {
		t.Error("Expected CanPushByPos to return false for position 0")
	}
	// Position 1 is not set, so CanPushByPos should return true (can push)
	if !wlm.CanPushByPos(key, 1) {
		t.Error("Expected CanPushByPos to return true for position 1")
	}
	// Position 2 is set, so CanPushByPos should return false (can't push)
	if wlm.CanPushByPos(key, 2) {
		t.Error("Expected CanPushByPos to return false for position 2")
	}

	// Test CheckWhiteListParam
	// Position 0 is set, so CheckWhiteListParam should return true
	if !wlm.CheckWhiteListParam(key, 0) {
		t.Error("Expected CheckWhiteListParam to return true for position 0")
	}
	// Position 1 is not set, so CheckWhiteListParam should return false
	if wlm.CheckWhiteListParam(key, 1) {
		t.Error("Expected CheckWhiteListParam to return false for position 1")
	}
	// Position 2 is set, so CheckWhiteListParam should return true
	if !wlm.CheckWhiteListParam(key, 2) {
		t.Error("Expected CheckWhiteListParam to return true for position 2")
	}

	// Test CheckWhiteListAll
	// Position 0 is set AND type 1 is set, so CheckWhiteListAll should return true
	if !wlm.CheckWhiteListAll(key, 0, 1) {
		t.Error("Expected CheckWhiteListAll to return true for position 0 and type 1")
	}
	// Position 0 is set BUT type 0 is not set, so CheckWhiteListAll should return false
	if wlm.CheckWhiteListAll(key, 0, 0) {
		t.Error("Expected CheckWhiteListAll to return false for position 0 and type 0")
	}
	// Position 1 is not set, so CheckWhiteListAll should return false regardless of type
	if wlm.CheckWhiteListAll(key, 1, 1) {
		t.Error("Expected CheckWhiteListAll to return false for position 1 and type 1")
	}

	// Test with unknown key
	unknownKey := uint32(999)
	if wlm.CanPushByPos(unknownKey, 0) != true {
		t.Error("Expected CanPushByPos to return true for unknown key")
	}
	if wlm.CheckWhiteListParam(unknownKey, 0) != false {
		t.Error("Expected CheckWhiteListParam to return false for unknown key")
	}
	if wlm.CheckWhiteListAll(unknownKey, 0, 0) != false {
		t.Error("Expected CheckWhiteListAll to return false for unknown key")
	}
}

// Helper function to test the bit logic
func getBitIsTrue(value uint32, index uint32) bool {
	return (value>>index)&0x01 == 1
}

func TestGetBitIsTrue(t *testing.T) {
	// Test various bit positions
	tests := []struct {
		value    uint32
		index    uint32
		expected bool
	}{
		{0x01, 0, true},        // Bit 0 of 0x01 is set
		{0x01, 1, false},       // Bit 1 of 0x01 is not set
		{0x02, 1, true},        // Bit 1 of 0x02 is set
		{0x04, 2, true},        // Bit 2 of 0x04 is set
		{0xFF, 7, true},        // Bit 7 of 0xFF is set
		{0xFF, 8, false},       // Bit 8 of 0xFF is not set
		{0x80000000, 31, true}, // Bit 31 of 0x800000 is set
	}

	for i, test := range tests {
		result := getBitIsTrue(test.value, test.index)
		if result != test.expected {
			t.Errorf("Test %d: getBitIsTrue(%d, %d) = %v, expected %v",
				i, test.value, test.index, result, test.expected)
		}
	}
}

// Test DescriptorKey interface implementation
func TestDescriptorKeyInterface(t *testing.T) {
	// Test databaseKey
	dbKey := sqlbase.NewDatabaseKey("test_db")
	if dbKey.Name() != "test_db" {
		t.Errorf("Expected name 'test_db', got '%s'", dbKey.Name())
	}
	if dbKey.Key() == nil {
		t.Error("Expected non-nil key")
	}

	// Test tableKey
	tableKey := sqlbase.NewTableKey(sqlbase.ID(1), sqlbase.ID(2), "test_table")
	if tableKey.Name() != "test_table" {
		t.Errorf("Expected name 'test_table', got '%s'", tableKey.Name())
	}
	if tableKey.Key() == nil {
		t.Error("Expected non-nil key")
	}

	// Test publicSchemaKey
	schemaKey := sqlbase.NewPublicSchemaKey(sqlbase.ID(1))
	if schemaKey.Name() != "public" {
		t.Errorf("Expected name '%s', got '%s'", "public", schemaKey.Name())
	}
	if schemaKey.Key() == nil {
		t.Error("Expected non-nil key")
	}
}

// Test DescriptorProto interface implementation
func TestDescriptorProtoInterface(t *testing.T) {
	// Test TableDescriptor
	tableDesc := &sqlbase.TableDescriptor{
		ID:         123,
		Name:       "test_table",
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}

	if tableDesc.GetPrivileges() == nil {
		t.Error("Expected non-nil privileges for TableDescriptor")
	}
	if tableDesc.GetID() != 123 {
		t.Errorf("Expected ID 123, got %d", tableDesc.GetID())
	}
	tableDesc.SetID(456)
	if tableDesc.GetID() != 456 {
		t.Errorf("Expected ID 456 after setting, got %d", tableDesc.GetID())
	}
	if tableDesc.TypeName() != "relation" {
		t.Errorf("Expected type name 'Table', got '%s'", tableDesc.TypeName())
	}
	if tableDesc.GetName() != "test_table" {
		t.Errorf("Expected name 'test_table', got '%s'", tableDesc.GetName())
	}
	tableDesc.SetName("new_name")
	if tableDesc.GetName() != "new_name" {
		t.Errorf("Expected name 'new_name' after setting, got '%s'", tableDesc.GetName())
	}

	// Test DatabaseDescriptor
	dbDesc := &sqlbase.DatabaseDescriptor{
		ID:         789,
		Name:       "test_db",
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}

	if dbDesc.GetPrivileges() == nil {
		t.Error("Expected non-nil privileges for DatabaseDescriptor")
	}
	if dbDesc.GetID() != 789 {
		t.Errorf("Expected ID 789, got %d", dbDesc.GetID())
	}
	dbDesc.SetID(999)
	if dbDesc.GetID() != 999 {
		t.Errorf("Expected ID 999 after setting, got %d", dbDesc.GetID())
	}
	if dbDesc.TypeName() != "database" {
		t.Errorf("Expected type name 'Database', got '%s'", dbDesc.TypeName())
	}
	if dbDesc.GetName() != "test_db" {
		t.Errorf("Expected name 'test_db', got '%s'", dbDesc.GetName())
	}
	dbDesc.SetName("new_db_name")
	if dbDesc.GetName() != "new_db_name" {
		t.Errorf("Expected name 'new_db_name' after setting, got '%s'", dbDesc.GetName())
	}
}

// Test protobuf marshaling/unmarshaling for key structures
func TestProtobufSerialization(t *testing.T) {
	// Test TableDescriptor serialization
	origDesc := &sqlbase.TableDescriptor{
		ID:         123,
		Name:       "test_table",
		ParentID:   456,
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}

	// Marshal
	data, err := protoutil.Marshal(origDesc)
	if err != nil {
		t.Fatalf("Failed to marshal TableDescriptor: %v", err)
	}

	// Unmarshal
	newDesc := &sqlbase.TableDescriptor{}
	err = protoutil.Unmarshal(data, newDesc)
	if err != nil {
		t.Fatalf("Failed to unmarshal TableDescriptor: %v", err)
	}

	// Compare
	if origDesc.GetID() != newDesc.GetID() {
		t.Errorf("ID mismatch: expected %d, got %d", origDesc.GetID(), newDesc.GetID())
	}
	if origDesc.GetName() != newDesc.GetName() {
		t.Errorf("Name mismatch: expected %s, got %s", origDesc.GetName(), newDesc.GetName())
	}
	if origDesc.ParentID != newDesc.ParentID {
		t.Errorf("ParentID mismatch: expected %d, got %d", origDesc.ParentID, newDesc.ParentID)
	}
}

func TestSystemDatabaseInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Get initial values
	cv := clusterversion.TestingClusterVersion
	kvs, _ := ms.GetInitialValues(cv)

	// Find the system database descriptor
	var systemDBDesc *sqlbase.DatabaseDescriptor
	for _, kv := range kvs {
		if !kv.Key.Equal(keys.DescIDGenerator) { // Skip the ID generator key
			var desc sqlbase.Descriptor
			if err := kv.Value.GetProto(&desc); err == nil {
				if db := desc.GetDatabase(); db != nil && db.Name == sqlbase.SystemDB.Name {
					systemDBDesc = db
					break
				}
			}
		}
	}

	if systemDBDesc == nil {
		t.Fatal("System database descriptor not found in initial values")
	}

	if systemDBDesc.ID != sqlbase.SystemDB.ID {
		t.Errorf("Expected system DB ID %d, got %d", sqlbase.SystemDB.ID, systemDBDesc.ID)
	}
	if systemDBDesc.Name != sqlbase.SystemDB.Name {
		t.Errorf("Expected system DB name %s, got %s", sqlbase.SystemDB.Name, systemDBDesc.Name)
	}
}

func TestBootstrapVersionHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSystemZoneConfig := zonepb.DefaultSystemZoneConfig()

	ms := sqlbase.MakeMetadataSchema(&defaultZoneConfig, &defaultSystemZoneConfig)

	// Test with older version (without namespace table with schemas)
	oldCV := clusterversion.ClusterVersion{
		Version: roachpb.Version{Major: 19, Minor: 1}, // Use a lower version
	}

	// Test with newer version (with namespace table with schemas)
	newCV := clusterversion.ClusterVersion{
		Version: roachpb.Version{Major: 20, Minor: 1}, // Use a higher version
	}

	// Get initial values for both versions
	oldKVs, _ := ms.GetInitialValues(oldCV)
	newKVs, _ := ms.GetInitialValues(newCV)

	// Both should have some values
	if len(oldKVs) == 0 {
		t.Error("Expected some key-value pairs with old version")
	}
	if len(newKVs) == 0 {
		t.Error("Expected some key-value pairs with new version")
	}
}
