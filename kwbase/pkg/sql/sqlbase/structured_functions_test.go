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

package sqlbase

import (
	"context"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func TestColumnTypeIsInvertedIndexable2(t *testing.T) {
	tests := []struct {
		name     string
		typ      *types.T
		expected bool
	}{
		{
			name:     "nil type",
			typ:      types.Float,
			expected: false,
		},
		{
			name:     "int type",
			typ:      types.Int,
			expected: false, // Ints are not inverted indexable
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ColumnTypeIsInvertedIndexable(tt.typ)
			if result != tt.expected {
				t.Errorf("ColumnTypeIsInvertedIndexable(%v) = %v, want %v", tt.typ, result, tt.expected)
			}
		})
	}
}

func TestIndexDescriptor_ContainsColumnID2(t *testing.T) {
	tests := []struct {
		name     string
		index    IndexDescriptor
		colID    ColumnID
		expected bool
	}{
		{
			name:     "empty index",
			index:    IndexDescriptor{},
			colID:    1,
			expected: false,
		},
		{
			name: "column in index",
			index: IndexDescriptor{
				ColumnIDs: []ColumnID{1, 2, 3},
			},
			colID:    2,
			expected: true,
		},
		{
			name: "column not in index",
			index: IndexDescriptor{
				ColumnIDs: []ColumnID{1, 2, 3},
			},
			colID:    4,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.index.ContainsColumnID(tt.colID)
			if result != tt.expected {
				t.Errorf("IndexDescriptor.ContainsColumnID(%d) = %v, want %v", tt.colID, result, tt.expected)
			}
		})
	}
}

func TestGeneratedFamilyName2(t *testing.T) {
	tests := []struct {
		name        string
		familyID    FamilyID
		columnNames []string
		expected    string
	}{
		{
			name:        "single column",
			familyID:    1,
			columnNames: []string{"id"},
			expected:    "fam_1_id",
		},
		{
			name:        "multiple columns",
			familyID:    2,
			columnNames: []string{"name", "age"},
			expected:    "fam_2_name_age",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GeneratedFamilyName(tt.familyID, tt.columnNames...)
			if result != tt.expected {
				t.Errorf("GeneratedFamilyName(%d, %v) = %q, want %q", tt.familyID, tt.columnNames, result, tt.expected)
			}
		})
	}
}

func TestHasCompositeKeyEncoding2(t *testing.T) {
	tests := []struct {
		name     string
		family   types.Family
		expected bool
	}{
		{
			name:     "int family",
			family:   types.IntFamily,
			expected: false,
		},
		{
			name:     "string family",
			family:   types.StringFamily,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasCompositeKeyEncoding(tt.family)
			if result != tt.expected {
				t.Errorf("HasCompositeKeyEncoding(%v) = %v, want %v", tt.family, result, tt.expected)
			}
		})
	}
}

func TestDatumTypeHasCompositeKeyEncoding2(t *testing.T) {
	tests := []struct {
		name     string
		typ      *types.T
		expected bool
	}{
		{
			name:     "float type",
			typ:      types.Float,
			expected: true,
		},
		{
			name:     "int type",
			typ:      types.Int,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DatumTypeHasCompositeKeyEncoding(tt.typ)
			if result != tt.expected {
				t.Errorf("DatumTypeHasCompositeKeyEncoding(%v) = %v, want %v", tt.typ, result, tt.expected)
			}
		})
	}
}

func TestMustBeValueEncoded2(t *testing.T) {
	tests := []struct {
		name     string
		family   types.Family
		expected bool
	}{
		{
			name:     "int family",
			family:   types.IntFamily,
			expected: false,
		},
		{
			name:     "string family",
			family:   types.StringFamily,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MustBeValueEncoded(tt.family)
			if result != tt.expected {
				t.Errorf("MustBeValueEncoded(%v) = %v, want %v", tt.family, result, tt.expected)
			}
		})
	}
}

func TestIndexDescriptor_HasOldStoredColumns(t *testing.T) {
	tests := []struct {
		name     string
		index    IndexDescriptor
		expected bool
	}{
		{
			name:     "no stored columns",
			index:    IndexDescriptor{},
			expected: false,
		},
		{
			name: "with stored columns",
			index: IndexDescriptor{
				ColumnIDs:        []ColumnID{1, 2, 3},
				StoreColumnIDs:   []ColumnID{4, 5},
				StoreColumnNames: []string{"name", "age", "address"},
				ExtraColumnIDs:   []ColumnID{6, 7},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.index.HasOldStoredColumns()
			if result != tt.expected {
				t.Errorf("IndexDescriptor.HasOldStoredColumns() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestByID_Swap(t *testing.T) {
	// Create a byID slice with test data
	cols := byID{
		{id: 3},
		{id: 1},
		{id: 2},
	}

	// Swap elements at indices 0 and 1
	cols.Swap(0, 1)

	// Verify the swap was successful
	expected := byID{
		{id: 1},
		{id: 3},
		{id: 2},
	}

	for i := range cols {
		if cols[i].id != expected[i].id {
			t.Errorf("cols[%d].id = %d, want %d", i, cols[i].id, expected[i].id)
		}
	}
}

func TestByID_Less(t *testing.T) {
	// Create a byID slice with test data
	cols := byID{
		{id: 3},
		{id: 1},
		{id: 2},
	}

	// Test Less function
	if !cols.Less(1, 0) { // 1 < 3 should be true
		t.Errorf("cols.Less(1, 0) = false, want true")
	}

	if !cols.Less(1, 2) { // 1 < 2 should be true
		t.Errorf("cols.Less(1, 2) = false, want true")
	}

	if cols.Less(0, 1) { // 3 < 1 should be false
		t.Errorf("cols.Less(0, 1) = true, want false")
	}

	if cols.Less(2, 1) { // 2 < 1 should be false
		t.Errorf("cols.Less(2, 1) = true, want false")
	}
}

func TestByID_Sort(t *testing.T) {
	// Create an unsorted byID slice
	cols := byID{
		{id: 3},
		{id: 1},
		{id: 2},
	}

	// Sort the slice
	sort.Sort(cols)

	// Verify the slice is sorted by id
	expected := []ColumnID{1, 2, 3}
	for i, expectedID := range expected {
		if cols[i].id != expectedID {
			t.Errorf("cols[%d].id = %d, want %d", i, cols[i].id, expectedID)
		}
	}
}

func TestGetDatabaseDescFromID(t *testing.T) {
	ctx := context.Background()

	t.Run("database not found", func(t *testing.T) {
		mockGetter := &mockProtoGetter{
			protos: make(map[string]protoutil.Message),
		}
		_, err := GetDatabaseDescFromID(ctx, mockGetter, 1)
		if err == nil {
			t.Error("expected error for missing database descriptor")
		}
	})

	t.Run("database found", func(t *testing.T) {
		dbDesc := &DatabaseDescriptor{
			Name: "testdb",
			ID:   100,
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Database{Database: dbDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(100)): wrappedDesc,
			},
		}
		result, err := GetDatabaseDescFromID(ctx, mockGetter, 100)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result == nil || result.Name != "testdb" {
			t.Errorf("expected database name 'testdb', got %v", result)
		}
	})

	t.Run("wrong descriptor type", func(t *testing.T) {
		tableDesc := &TableDescriptor{
			Name:             "testtable",
			ID:               101,
			ModificationTime: hlc.Timestamp{WallTime: timeutil.Now().Unix()},
			Privileges:       NewDefaultPrivilegeDescriptor(),
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Table{Table: tableDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(101)): wrappedDesc,
			},
		}
		_, err := GetDatabaseDescFromID(ctx, mockGetter, 101)
		if err == nil {
			t.Error("expected error for wrong descriptor type")
		}
	})
}

func TestGetTableDescFromID(t *testing.T) {
	ctx := context.Background()

	t.Run("table not found", func(t *testing.T) {
		mockGetter := &mockProtoGetter{
			protos: make(map[string]protoutil.Message),
		}
		_, err := GetTableDescFromID(ctx, mockGetter, 1)
		if err == nil {
			t.Error("expected error for missing table descriptor")
		}
	})

	t.Run("table found", func(t *testing.T) {
		tableDesc := &TableDescriptor{
			Name:             "testtable",
			ID:               200,
			ModificationTime: hlc.Timestamp{WallTime: timeutil.Now().Unix()},
			Privileges:       NewDefaultPrivilegeDescriptor(),
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Table{Table: tableDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(200)): wrappedDesc,
			},
		}
		result, err := GetTableDescFromID(ctx, mockGetter, 200)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result == nil || result.Name != "testtable" {
			t.Errorf("expected table name 'testtable', got %v", result)
		}
	})

	t.Run("wrong descriptor type", func(t *testing.T) {
		dbDesc := &DatabaseDescriptor{
			Name: "testdb",
			ID:   201,
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Database{Database: dbDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(201)): wrappedDesc,
			},
		}
		_, err := GetTableDescFromID(ctx, mockGetter, 201)
		if err == nil {
			t.Error("expected error for wrong descriptor type")
		}
	})
}

func TestGetMutableTableDescFromID(t *testing.T) {
	ctx := context.Background()

	t.Run("table not found", func(t *testing.T) {
		mockGetter := &mockProtoGetter{
			protos: make(map[string]protoutil.Message),
		}
		_, err := GetMutableTableDescFromID(ctx, mockGetter, 1)
		if err == nil {
			t.Error("expected error for missing table descriptor")
		}
	})

	t.Run("table found", func(t *testing.T) {
		tableDesc := &TableDescriptor{
			Name:             "mutabletable",
			ID:               300,
			ModificationTime: hlc.Timestamp{WallTime: timeutil.Now().Unix()},
			Privileges:       NewDefaultPrivilegeDescriptor(),
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Table{Table: tableDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(300)): wrappedDesc,
			},
		}
		result, err := GetMutableTableDescFromID(ctx, mockGetter, 300)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result == nil || result.Name != "mutabletable" {
			t.Errorf("expected table name 'mutabletable', got %v", result)
		}
	})
}

func TestGetColumnFamilyForShard(t *testing.T) {
	t.Run("column found in family", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			ID: 400,
			Families: []ColumnFamilyDescriptor{
				{
					ID:          1,
					Name:        "family1",
					ColumnNames: []string{"col1", "col2"},
				},
				{
					ID:          2,
					Name:        "family2",
					ColumnNames: []string{"col3"},
				},
			},
		})
		result := GetColumnFamilyForShard(desc, []string{"col1"})
		if result != "family1" {
			t.Errorf("expected 'family1', got %q", result)
		}
	})

	t.Run("column not found", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			ID: 401,
			Families: []ColumnFamilyDescriptor{
				{
					ID:          1,
					Name:        "family1",
					ColumnNames: []string{"col1"},
				},
			},
		})
		result := GetColumnFamilyForShard(desc, []string{"nonexistent"})
		if result != "" {
			t.Errorf("expected empty string, got %q", result)
		}
	})

	t.Run("empty index columns", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			ID: 402,
			Families: []ColumnFamilyDescriptor{
				{
					ID:          1,
					Name:        "family1",
					ColumnNames: []string{"col1"},
				},
			},
			Columns: []ColumnDescriptor{
				{Name: "a"},
				{Name: "b"},
				{Name: "c"},
			},
			PrimaryIndex: makeIndexDescriptor("c", []string{"a", "b"}),
			Indexes: []IndexDescriptor{
				makeIndexDescriptor("d", []string{"b", "a"}),
				makeIndexDescriptor("e", []string{"b"}),
				func() IndexDescriptor {
					idx := makeIndexDescriptor("f", []string{"c"})
					idx.EncodingType = PrimaryIndexEncoding
					return idx
				}(),
			},
			Privileges:    NewDefaultPrivilegeDescriptor(),
			FormatVersion: FamilyFormatVersion,
		})
		result := GetColumnFamilyForShard(desc, []string{"a"})
		if result != "" {
			t.Errorf("expected empty string for empty index columns, got %q", result)
		}
	})
}

func TestGetTableDescFromIDWithFKsChanged(t *testing.T) {
	ctx := context.Background()

	t.Run("table not found", func(t *testing.T) {
		mockGetter := &mockProtoGetter{
			protos: make(map[string]protoutil.Message),
		}
		_, _, err := GetTableDescFromIDWithFKsChanged(ctx, mockGetter, 1)
		if err == nil {
			t.Error("expected error for missing table descriptor")
		}
	})

	t.Run("table found without FK changes", func(t *testing.T) {
		tableDesc := &TableDescriptor{
			Name:             "fk table",
			ID:               500,
			ModificationTime: hlc.Timestamp{WallTime: timeutil.Now().Unix()},
			Privileges:       NewDefaultPrivilegeDescriptor(),
		}
		wrappedDesc := &Descriptor{
			Union: &Descriptor_Table{Table: tableDesc},
		}
		mockGetter := &mockProtoGetter{
			protos: map[string]protoutil.Message{
				string(MakeDescMetadataKey(500)): wrappedDesc,
			},
		}
		result, changed, err := GetTableDescFromIDWithFKsChanged(ctx, mockGetter, 500)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result == nil || result.Name != "fk table" {
			t.Errorf("expected table name 'fk table', got %v", result)
		}
		if changed {
			t.Error("expected changed to be false")
		}
	})
}

func TestNewMutableCreatedTableDescriptor(t *testing.T) {
	tbl := TableDescriptor{
		ID:   100,
		Name: "test",
	}
	result := NewMutableCreatedTableDescriptor(tbl)

	if result.ID != 100 {
		t.Errorf("expected ID 100, got %d", result.ID)
	}
	if result.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Name)
	}
}

func TestNewMutableExistingTableDescriptor(t *testing.T) {
	tbl := TableDescriptor{
		ID:   200,
		Name: "existing",
	}
	result := NewMutableExistingTableDescriptor(tbl)

	if result.ID != 200 {
		t.Errorf("expected ID 200, got %d", result.ID)
	}
	if result.Name != "existing" {
		t.Errorf("expected name 'existing', got %q", result.Name)
	}
}

func TestNewImmutableTableDescriptor(t *testing.T) {
	tbl := TableDescriptor{
		ID:   300,
		Name: "immutable",
	}
	result := NewImmutableTableDescriptor(tbl)

	if result.ID != 300 {
		t.Errorf("expected ID 300, got %d", result.ID)
	}
	if result.Name != "immutable" {
		t.Errorf("expected name 'immutable', got %q", result.Name)
	}
}

func TestNewSchemaKey(t *testing.T) {
	parentID := ID(100)
	key := NewSchemaKey(parentID, "testschema")

	if key.parentID != parentID {
		t.Errorf("expected parentID %d, got %d", parentID, key.parentID)
	}
	if key.name != "testschema" {
		t.Errorf("expected name 'testschema', got %q", key.name)
	}
}

func TestNewPublicSchemaKey(t *testing.T) {
	parentID := ID(100)
	key := NewPublicSchemaKey(parentID)

	if key.parentID != parentID {
		t.Errorf("expected parentID %d, got %d", parentID, key.parentID)
	}
	if key.name != "public" {
		t.Errorf("expected name 'public', got %q", key.name)
	}
}

func TestNewDeprecatedTableKey(t *testing.T) {
	parentID := ID(100)
	key := NewDeprecatedTableKey(parentID, "deprecated")

	if key.parentID != parentID {
		t.Errorf("expected parentID %d, got %d", parentID, key.parentID)
	}
	if key.name != "deprecated" {
		t.Errorf("expected name 'deprecated', got %q", key.name)
	}
}

func TestNewDeprecatedDatabaseKey(t *testing.T) {
	key := NewDeprecatedDatabaseKey("deprecated_db")

	if key.name != "deprecated_db" {
		t.Errorf("expected name 'deprecated_db', got %q", key.name)
	}
}

type mockProtoGetter struct {
	protos map[string]protoutil.Message
	getErr error
}

func (m *mockProtoGetter) GetProtoTs(
	ctx context.Context, key interface{}, msg protoutil.Message,
) (hlc.Timestamp, error) {
	if m.getErr != nil {
		return hlc.Timestamp{}, m.getErr
	}
	if keyStr, ok := key.(roachpb.Key); ok {
		if proto, found := m.protos[string(keyStr)]; found {
			bytes, err := protoutil.Marshal(proto)
			if err != nil {
				return hlc.Timestamp{}, err
			}
			if err := protoutil.Unmarshal(bytes, msg); err != nil {
				return hlc.Timestamp{}, err
			}
		}
	}
	return hlc.Timestamp{}, nil
}

var _ protoGetter = &mockProtoGetter{}

func TestColumnsSelectors(t *testing.T) {
	cols := []ColumnDescriptor{
		{Name: "id", ID: 1},
		{Name: "name", ID: 2},
		{Name: "age", ID: 3},
	}

	result := ColumnsSelectors(cols)

	if len(result) != len(cols) {
		t.Errorf("expected %d selectors, got %d", len(cols), len(result))
	}

	for i := range cols {
		if result[i].Expr == nil {
			t.Errorf("selector %d Expr is nil", i)
		}
	}
}

func TestNewDatabaseKey(t *testing.T) {
	key := NewDatabaseKey("testdb")
	if key.Name() != "testdb" {
		t.Errorf("expected name 'testdb', got %q", key.Name())
	}

	expectedKey := MakeNameMetadataKey(keys.RootNamespaceID, keys.RootNamespaceID, "testdb")
	if !key.Key().Equal(expectedKey) {
		t.Errorf("expected key %v, got %v", expectedKey, key.Key())
	}
}

func TestNewPublicTableKey(t *testing.T) {
	parentID := ID(100)
	key := NewPublicTableKey(parentID, "testtable")

	if key.parentID != parentID {
		t.Errorf("expected parentID %d, got %d", parentID, key.parentID)
	}
	if key.parentSchemaID != keys.PublicSchemaID {
		t.Errorf("expected parentSchemaID %d, got %d", keys.PublicSchemaID, key.parentSchemaID)
	}
	if key.name != "testtable" {
		t.Errorf("expected name 'testtable', got %q", key.name)
	}
}

func TestNewTableKey(t *testing.T) {
	parentID := ID(100)
	parentSchemaID := ID(200)
	key := NewTableKey(parentID, parentSchemaID, "testtable")

	if key.parentID != parentID {
		t.Errorf("expected parentID %d, got %d", parentID, key.parentID)
	}
	if key.parentSchemaID != parentSchemaID {
		t.Errorf("expected parentSchemaID %d, got %d", parentSchemaID, key.parentSchemaID)
	}
	if key.name != "testtable" {
		t.Errorf("expected name 'testtable', got %q", key.name)
	}
}

func TestTableDescriptorsLen(t *testing.T) {
	tds := TableDescriptors{
		{ID: 1, Name: "table1"},
		{ID: 2, Name: "table2"},
		{ID: 3, Name: "table3"},
	}

	expected := 3
	actual := tds.Len()
	if actual != expected {
		t.Errorf("TableDescriptors.Len() = %d, want %d", actual, expected)
	}

	// Test with empty slice
	emptyTds := TableDescriptors{}
	expected = 0
	actual = emptyTds.Len()
	if actual != expected {
		t.Errorf("Empty TableDescriptors.Len() = %d, want %d", actual, expected)
	}
}

func TestTableDescriptorsLess(t *testing.T) {
	tds := TableDescriptors{
		{ID: 1, Name: "table1"},
		{ID: 2, Name: "table2"},
	}

	// Test i < j (should return true)
	if !tds.Less(0, 1) {
		t.Errorf("TableDescriptors.Less(0, 1) = false, want true")
	}

	// Test i > j (should return false)
	if tds.Less(1, 0) {
		t.Errorf("TableDescriptors.Less(1, 0) = true, want false")
	}

	// Test i == j (should return false)
	if tds.Less(0, 0) {
		t.Errorf("TableDescriptors.Less(0, 0) = true, want false")
	}
}

func TestTableDescriptorsSwap(t *testing.T) {
	tds := TableDescriptors{
		{ID: 1, Name: "table1"},
		{ID: 2, Name: "table2"},
	}

	originalFirst := tds[0]
	originalSecond := tds[1]

	tds.Swap(0, 1)

	// Check that positions were swapped
	if tds[0].ID != originalSecond.ID || tds[1].ID != originalFirst.ID {
		t.Errorf("TableDescriptors.Swap did not correctly swap elements")
	}
	if tds[0].Name != originalSecond.Name || tds[1].Name != originalFirst.Name {
		t.Errorf("TableDescriptors.Swap did not correctly swap elements")
	}
}

func TestColumnIDsLen(t *testing.T) {
	cids := ColumnIDs{1, 2, 3, 4, 5}

	expected := 5
	actual := cids.Len()
	if actual != expected {
		t.Errorf("ColumnIDs.Len() = %d, want %d", actual, expected)
	}

	// Test with empty slice
	emptyCids := ColumnIDs{}
	expected = 0
	actual = emptyCids.Len()
	if actual != expected {
		t.Errorf("Empty ColumnIDs.Len() = %d, want %d", actual, expected)
	}
}

func TestColumnIDsLess(t *testing.T) {
	cids := ColumnIDs{10, 20, 30}

	// Test i < j (should return true)
	if !cids.Less(0, 1) {
		t.Errorf("ColumnIDs.Less(0, 1) = false, want true")
	}

	// Test i > j (should return false)
	if cids.Less(1, 0) {
		t.Errorf("ColumnIDs.Less(1, 0) = true, want false")
	}

	// Test i == j (should return false)
	if cids.Less(1, 1) {
		t.Errorf("ColumnIDs.Less(1, 1) = true, want false")
	}
}

func TestColumnIDsSwap(t *testing.T) {
	cids := ColumnIDs{100, 200}

	originalFirst := cids[0]
	originalSecond := cids[1]

	cids.Swap(0, 1)

	// Check that positions were swapped
	if cids[0] != originalSecond || cids[1] != originalFirst {
		t.Errorf("ColumnIDs.Swap did not correctly swap elements")
	}
}

func TestTableDescriptorsSort(t *testing.T) {
	tds := TableDescriptors{
		{ID: 5, Name: "table5"},
		{ID: 1, Name: "table1"},
		{ID: 3, Name: "table3"},
		{ID: 2, Name: "table2"},
	}

	// Sort the table descriptors
	sort.Sort(tds)

	// Verify they are sorted by ID
	expectedOrder := []ID{1, 2, 3, 5}
	for i, expectedID := range expectedOrder {
		if tds[i].ID != expectedID {
			t.Errorf("After sorting, tds[%d].ID = %d, want %d", i, tds[i].ID, expectedID)
		}
	}
}

func TestColumnIDsSort(t *testing.T) {
	cids := ColumnIDs{50, 10, 30, 20, 40}

	// Sort the column IDs
	sort.Sort(cids)

	// Verify they are sorted
	expectedOrder := ColumnIDs{10, 20, 30, 40, 50}
	for i, expectedID := range expectedOrder {
		if cids[i] != expectedID {
			t.Errorf("After sorting, cids[%d] = %d, want %d", i, cids[i], expectedID)
		}
	}
}

func TestTableDescriptor_TypeName(t *testing.T) {
	tests := []struct {
		name      string
		tableType tree.TableType
		expected  string
	}{
		{
			name:      "timeseries table",
			tableType: tree.TimeseriesTable,
			expected:  "timeseries table",
		},
		{
			name:      "template table",
			tableType: tree.TemplateTable,
			expected:  "template table",
		},
		{
			name:      "instance table",
			tableType: tree.InstanceTable,
			expected:  "instance table",
		},
		{
			name:      "normal table",
			tableType: tree.RelationalTable,
			expected:  "relation",
		},

		{
			name:      "unknown table type",
			tableType: 999, // Some unknown table type
			expected:  "relation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := &TableDescriptor{
				TableType: tt.tableType,
			}
			result := desc.TypeName()
			if result != tt.expected {
				t.Errorf("TableDescriptor.TypeName() for TableType %v = %q, want %q", tt.tableType, result, tt.expected)
			}
		})
	}
}

func TestTableDescriptor_SetName(t *testing.T) {
	t.Run("set table name", func(t *testing.T) {
		desc := &TableDescriptor{
			Name: "old_name",
		}

		// Set new name
		newName := "new_table_name"
		desc.SetName(newName)

		// Verify the name was updated
		if desc.Name != newName {
			t.Errorf("TableDescriptor.SetName() failed: expected name %q, got %q", newName, desc.Name)
		}
	})

	t.Run("set empty table name", func(t *testing.T) {
		desc := &TableDescriptor{
			Name: "old_name",
		}

		// Set empty name
		emptyName := ""
		desc.SetName(emptyName)

		// Verify the name was updated
		if desc.Name != emptyName {
			t.Errorf("TableDescriptor.SetName() failed: expected name %q, got %q", emptyName, desc.Name)
		}
	})
}

func TestSchemaDescriptor_SetID(t *testing.T) {
	t.Run("set schema ID", func(t *testing.T) {
		desc := &SchemaDescriptor{
			ID: 123,
		}

		// Set new ID
		newID := ID(456)
		desc.SetID(newID)

		// Verify the ID was updated
		if desc.ID != newID {
			t.Errorf("SchemaDescriptor.SetID() failed: expected ID %d, got %d", newID, desc.ID)
		}
	})

	t.Run("set zero schema ID", func(t *testing.T) {
		desc := &SchemaDescriptor{
			ID: 123,
		}

		// Set zero ID
		zeroID := ID(0)
		desc.SetID(zeroID)

		// Verify the ID was updated
		if desc.ID != zeroID {
			t.Errorf("SchemaDescriptor.SetID() failed: expected ID %d, got %d", zeroID, desc.ID)
		}
	})
}

func TestSchemaDescriptor_TypeName(t *testing.T) {
	t.Run("get schema type name", func(t *testing.T) {
		desc := &SchemaDescriptor{}

		// Get type name
		result := desc.TypeName()

		// Verify the type name is correct
		expected := "schema"
		if result != expected {
			t.Errorf("SchemaDescriptor.TypeName() failed: expected %q, got %q", expected, result)
		}
	})
}

func TestSchemaDescriptor_SetName(t *testing.T) {
	t.Run("set schema name", func(t *testing.T) {
		desc := &SchemaDescriptor{
			Name: "old_name",
		}

		// Set new name
		newName := "new_schema_name"
		desc.SetName(newName)

		// Verify the name was updated
		if desc.Name != newName {
			t.Errorf("SchemaDescriptor.SetName() failed: expected name %q, got %q", newName, desc.Name)
		}
	})

	t.Run("set empty schema name", func(t *testing.T) {
		desc := &SchemaDescriptor{
			Name: "old_name",
		}

		// Set empty name
		emptyName := ""
		desc.SetName(emptyName)

		// Verify the name was updated
		if desc.Name != emptyName {
			t.Errorf("SchemaDescriptor.SetName() failed: expected name %q, got %q", emptyName, desc.Name)
		}
	})
}

func TestTableDescriptor_GetTriggerByName(t *testing.T) {
	t.Run("trigger found", func(t *testing.T) {
		// Create a table descriptor with multiple triggers
		desc := &TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:       "trigger1",
					Event:      TriggerEvent_INSERT,
					ActionTime: TriggerActionTime_BEFORE,
				},
				{
					Name:       "trigger2",
					Event:      TriggerEvent_UPDATE,
					ActionTime: TriggerActionTime_AFTER,
				},
				{
					Name:       "trigger3",
					Event:      TriggerEvent_DELETE,
					ActionTime: TriggerActionTime_BEFORE,
				},
			},
		}

		// Test finding each trigger by name
		trigger1 := desc.GetTriggerByName("trigger1")
		if trigger1 == nil {
			t.Error("expected to find trigger1, but got nil")
		} else if trigger1.Name != "trigger1" {
			t.Errorf("expected trigger name 'trigger1', got '%s'", trigger1.Name)
		}

		trigger2 := desc.GetTriggerByName("trigger2")
		if trigger2 == nil {
			t.Error("expected to find trigger2, but got nil")
		} else if trigger2.Name != "trigger2" {
			t.Errorf("expected trigger name 'trigger2', got '%s'", trigger2.Name)
		}

		trigger3 := desc.GetTriggerByName("trigger3")
		if trigger3 == nil {
			t.Error("expected to find trigger3, but got nil")
		} else if trigger3.Name != "trigger3" {
			t.Errorf("expected trigger name 'trigger3', got '%s'", trigger3.Name)
		}
	})

	t.Run("trigger not found", func(t *testing.T) {
		// Create a table descriptor with triggers
		desc := &TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:       "trigger1",
					Event:      TriggerEvent_INSERT,
					ActionTime: TriggerActionTime_BEFORE,
				},
				{
					Name:       "trigger2",
					Event:      TriggerEvent_UPDATE,
					ActionTime: TriggerActionTime_AFTER,
				},
			},
		}

		// Test finding a non-existent trigger
		result := desc.GetTriggerByName("nonexistent_trigger")
		if result != nil {
			t.Errorf("expected nil for non-existent trigger, got %v", result)
		}
	})

	t.Run("empty triggers list", func(t *testing.T) {
		// Create a table descriptor with empty triggers list
		desc := &TableDescriptor{
			Triggers: []TriggerDescriptor{},
		}

		// Test finding any trigger
		result := desc.GetTriggerByName("any_trigger")
		if result != nil {
			t.Errorf("expected nil for empty triggers list, got %v", result)
		}
	})

	t.Run("case sensitivity", func(t *testing.T) {
		// Create a table descriptor with a trigger
		desc := &TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:       "Trigger1", // CamelCase name
					Event:      TriggerEvent_INSERT,
					ActionTime: TriggerActionTime_BEFORE,
				},
			},
		}

		// Test finding with exact case
		exactMatch := desc.GetTriggerByName("Trigger1")
		if exactMatch == nil {
			t.Error("expected to find 'Trigger1' with exact case, but got nil")
		}

		// Test finding with different case
		caseMismatch := desc.GetTriggerByName("trigger1")
		if caseMismatch != nil {
			t.Errorf("expected nil for case mismatch, got %v", caseMismatch)
		}
	})
}

func TestTableDescriptor_FindIndexesWithPartition(t *testing.T) {
	t.Run("find indexes with existing partition", func(t *testing.T) {
		// Create a table descriptor with partitioned indexes
		desc := &TableDescriptor{
			Indexes: []IndexDescriptor{
				{
					ID:   1,
					Name: "idx1",
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "partition1", Values: [][]byte{{}}}},
					},
				},
				{
					ID:   2,
					Name: "idx2",
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "partition2", Values: [][]byte{{}}}},
					},
				},
				{
					ID:   3,
					Name: "idx3",
					// No partitioning
				},
			},
		}

		// Test finding indexes with "partition1"
		result := desc.FindIndexesWithPartition("partition1")
		if len(result) != 1 {
			t.Errorf("expected 2 indexes with partition 'partition1', got %d", len(result))
		}

		// Check that the correct indexes are returned
		foundIdx1 := false
		foundIdx2 := false
		for _, idx := range result {
			if idx.Name == "idx1" {
				foundIdx1 = true
			} else if idx.Name == "idx2" {
				foundIdx2 = true
			}
		}
		if !foundIdx1 {
			t.Error("expected to find index 'idx1' with partition 'partition1'")
		}
		if foundIdx2 {
			t.Error("Not expected to find index 'idx2' with partition 'partition1'")
		}

		// Test finding indexes with "partition2"
		result = desc.FindIndexesWithPartition("partition2")
		if len(result) != 1 {
			t.Errorf("expected 1 index with partition 'partition2', got %d", len(result))
		}
		if len(result) > 0 && result[0].Name != "idx2" {
			t.Errorf("expected index 'idx2' with partition 'partition2', got %s", result[0].Name)
		}

		// Test finding indexes with "partition3"
		result = desc.FindIndexesWithPartition("partition3")
		if len(result) != 0 {
			t.Errorf("expected 0 index with partition 'partition3', got %d", len(result))
		}
		if len(result) > 0 && result[0].Name != "idx2" {
			t.Errorf("expected index 'idx2' with partition 'partition3', got %s", result[0].Name)
		}
	})

	t.Run("find indexes with non-existent partition", func(t *testing.T) {
		// Create a table descriptor with partitioned indexes
		desc := &TableDescriptor{
			Indexes: []IndexDescriptor{
				{
					ID:   1,
					Name: "idx1",
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "partition1", Values: [][]byte{{}}}},
					},
				},
			},
		}

		// Test finding indexes with non-existent partition
		result := desc.FindIndexesWithPartition("non_existent_partition")
		if len(result) != 0 {
			t.Errorf("expected 0 indexes with non-existent partition, got %d", len(result))
		}
	})

	t.Run("find indexes with no partitions", func(t *testing.T) {
		// Create a table descriptor with no partitioned indexes
		desc := &TableDescriptor{
			Indexes: []IndexDescriptor{
				{
					ID:   1,
					Name: "idx1",
					// No partitioning
				},
				{
					ID:   2,
					Name: "idx2",
					// No partitioning
				},
			},
		}

		// Test finding indexes with any partition
		result := desc.FindIndexesWithPartition("any_partition")
		if len(result) != 0 {
			t.Errorf("expected 0 indexes with no partitions, got %d", len(result))
		}
	})

	t.Run("find indexes with empty table", func(t *testing.T) {
		// Create an empty table descriptor
		desc := &TableDescriptor{}

		// Test finding indexes with any partition
		result := desc.FindIndexesWithPartition("any_partition")
		if len(result) != 0 {
			t.Errorf("expected 0 indexes in empty table, got %d", len(result))
		}
	})
}

func TestMutableTableDescriptor_AddTriggerDesc(t *testing.T) {
	t.Run("add first trigger with no order", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{})
		trigDesc := &TriggerDescriptor{
			Name:       "trigger1",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_BEFORE,
		}

		err := desc.AddTriggerDesc(trigDesc, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Triggers) != 1 {
			t.Errorf("expected 1 trigger, got %d", len(desc.Triggers))
		}

		if desc.Triggers[0].Name != "trigger1" {
			t.Errorf("expected trigger name 'trigger1', got '%s'", desc.Triggers[0].Name)
		}

		if desc.Triggers[0].TriggerOrder != 1 {
			t.Errorf("expected trigger order 1, got %d", desc.Triggers[0].TriggerOrder)
		}
	})

	t.Run("add BEFORE trigger after existing BEFORE triggers", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
				{
					Name:         "trigger2",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 2,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger3",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_BEFORE,
		}

		err := desc.AddTriggerDesc(trigDesc, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Triggers) != 3 {
			t.Errorf("expected 3 triggers, got %d", len(desc.Triggers))
		}

		if desc.Triggers[2].Name != "trigger3" {
			t.Errorf("expected trigger name 'trigger3', got '%s'", desc.Triggers[2].Name)
		}

		if desc.Triggers[2].TriggerOrder != 3 {
			t.Errorf("expected trigger order 3, got %d", desc.Triggers[2].TriggerOrder)
		}
	})

	t.Run("add BEFORE trigger before AFTER triggers", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
				{
					Name:         "trigger2",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_AFTER,
					TriggerOrder: 2,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger3",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_BEFORE,
		}

		err := desc.AddTriggerDesc(trigDesc, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Triggers) != 3 {
			t.Errorf("expected 3 triggers, got %d", len(desc.Triggers))
		}

		if desc.Triggers[1].Name != "trigger3" {
			t.Errorf("expected trigger name 'trigger3' at position 1, got '%s'", desc.Triggers[1].Name)
		}

		if desc.Triggers[1].TriggerOrder != 2 {
			t.Errorf("expected trigger order 2 for trigger3, got %d", desc.Triggers[1].TriggerOrder)
		}

		if desc.Triggers[2].TriggerOrder != 3 {
			t.Errorf("expected trigger order 3 for trigger2, got %d", desc.Triggers[2].TriggerOrder)
		}
	})

	t.Run("add AFTER trigger", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
				{
					Name:         "trigger2",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 2,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger3",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_AFTER,
		}

		err := desc.AddTriggerDesc(trigDesc, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Triggers) != 3 {
			t.Errorf("expected 3 triggers, got %d", len(desc.Triggers))
		}

		if desc.Triggers[2].Name != "trigger3" {
			t.Errorf("expected trigger name 'trigger3', got '%s'", desc.Triggers[2].Name)
		}

		if desc.Triggers[2].TriggerOrder != 3 {
			t.Errorf("expected trigger order 3, got %d", desc.Triggers[2].TriggerOrder)
		}
	})

	t.Run("add trigger with unknown action time", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{})
		trigDesc := &TriggerDescriptor{
			Name:       "trigger1",
			Event:      TriggerEvent_INSERT,
			ActionTime: 999, // Unknown action time
		}
		order := &tree.TriggerOrder{
			OrderType:    tree.TriggerOrderTypeFollow,
			OtherTrigger: "non_existent_trigger",
		}

		err := desc.AddTriggerDesc(trigDesc, order)
		if err == nil {
			t.Error("expected error for unknown action time, but got nil")
		}
	})

	t.Run("add trigger with follow order", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
				{
					Name:         "trigger2",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 2,
				},
				{
					Name:         "trigger3",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 3,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger4",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_BEFORE,
		}

		order := &tree.TriggerOrder{
			OrderType:    tree.TriggerOrderTypeFollow,
			OtherTrigger: "trigger2",
		}

		err := desc.AddTriggerDesc(trigDesc, order)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Triggers) != 4 {
			t.Errorf("expected 4 triggers, got %d", len(desc.Triggers))
		}

		// Check that trigger4 is after trigger2
		trigger2Index := -1
		trigger4Index := -1
		for i, trig := range desc.Triggers {
			if trig.Name == "trigger2" {
				trigger2Index = i
			} else if trig.Name == "trigger4" {
				trigger4Index = i
			}
		}

		if trigger2Index == -1 {
			t.Error("expected to find trigger2")
		}

		if trigger4Index == -1 {
			t.Error("expected to find trigger4")
		}

		if trigger4Index <= trigger2Index {
			t.Errorf("expected trigger4 to be after trigger2, but trigger2 is at index %d and trigger4 is at index %d", trigger2Index, trigger4Index)
		}
	})

	t.Run("add trigger with follow order to non-existent trigger", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger2",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_BEFORE,
		}

		order := &tree.TriggerOrder{
			OrderType:    tree.TriggerOrderTypeFollow,
			OtherTrigger: "non_existent_trigger",
		}

		err := desc.AddTriggerDesc(trigDesc, order)
		if err == nil {
			t.Error("expected error for non-existent trigger, but got nil")
		}
	})

	t.Run("add trigger with follow order to trigger with different action time", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger2",
			Event:      TriggerEvent_INSERT,
			ActionTime: TriggerActionTime_AFTER, // Different action time
		}

		order := &tree.TriggerOrder{
			OrderType:    tree.TriggerOrderTypeFollow,
			OtherTrigger: "trigger1",
		}

		err := desc.AddTriggerDesc(trigDesc, order)
		if err == nil {
			t.Error("expected error for different action time, but got nil")
		}
	})

	t.Run("add trigger with follow order to trigger with different event", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Triggers: []TriggerDescriptor{
				{
					Name:         "trigger1",
					Event:        TriggerEvent_INSERT,
					ActionTime:   TriggerActionTime_BEFORE,
					TriggerOrder: 1,
				},
			},
		})

		trigDesc := &TriggerDescriptor{
			Name:       "trigger2",
			Event:      TriggerEvent_UPDATE, // Different event
			ActionTime: TriggerActionTime_BEFORE,
		}

		order := &tree.TriggerOrder{
			OrderType:    tree.TriggerOrderTypeFollow,
			OtherTrigger: "trigger1",
		}

		err := desc.AddTriggerDesc(trigDesc, order)
		if err == nil {
			t.Error("expected error for different event, but got nil")
		}
	})
}

func TestMutableTableDescriptor_AddIndex(t *testing.T) {
	t.Run("add primary index", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
		})

		idx := IndexDescriptor{
			Name:        "", // Empty name, should be set to PrimaryKeyIndexName
			Type:        IndexDescriptor_FORWARD,
			ColumnNames: []string{"id"},
			ColumnIDs:   []ColumnID{1},
		}

		err := desc.AddIndex(idx, true)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if desc.PrimaryIndex.Name != PrimaryKeyIndexName {
			t.Errorf("expected primary index name %q, got %q", PrimaryKeyIndexName, desc.PrimaryIndex.Name)
		}

		if len(desc.Indexes) != 0 {
			t.Errorf("expected 0 secondary indexes, got %d", len(desc.Indexes))
		}
	})

	t.Run("add primary index with custom name", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
		})

		customName := "custom_pk"
		idx := IndexDescriptor{
			Name:        customName,
			Type:        IndexDescriptor_FORWARD,
			ColumnNames: []string{"id"},
			ColumnIDs:   []ColumnID{1},
		}

		err := desc.AddIndex(idx, true)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if desc.PrimaryIndex.Name != customName {
			t.Errorf("expected primary index name %q, got %q", customName, desc.PrimaryIndex.Name)
		}
	})

	t.Run("add multiple primary indexes", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
			PrimaryIndex: IndexDescriptor{
				Name:        PrimaryKeyIndexName,
				Type:        IndexDescriptor_FORWARD,
				ColumnNames: []string{"id"},
				ColumnIDs:   []ColumnID{1},
			},
		})

		idx := IndexDescriptor{
			Name:        "another_pk",
			Type:        IndexDescriptor_FORWARD,
			ColumnNames: []string{"name"},
			ColumnIDs:   []ColumnID{2},
		}

		err := desc.AddIndex(idx, true)
		if err == nil {
			t.Error("expected error for multiple primary keys, but got nil")
		}
	})

	t.Run("add secondary index", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
			PrimaryIndex: IndexDescriptor{
				Name:        PrimaryKeyIndexName,
				Type:        IndexDescriptor_FORWARD,
				ColumnNames: []string{"id"},
				ColumnIDs:   []ColumnID{1},
			},
		})

		idx := IndexDescriptor{
			Name:        "idx_name",
			Type:        IndexDescriptor_FORWARD,
			ColumnNames: []string{"name"},
			ColumnIDs:   []ColumnID{2},
		}

		err := desc.AddIndex(idx, false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Indexes) != 1 {
			t.Errorf("expected 1 secondary index, got %d", len(desc.Indexes))
		}

		if desc.Indexes[0].Name != "idx_name" {
			t.Errorf("expected index name 'idx_name', got %q", desc.Indexes[0].Name)
		}
	})

	t.Run("add inverted index", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "data", Type: *types.StringArray},
			},
			PrimaryIndex: IndexDescriptor{
				Name:        PrimaryKeyIndexName,
				Type:        IndexDescriptor_FORWARD,
				ColumnNames: []string{"id"},
				ColumnIDs:   []ColumnID{1},
			},
		})

		idx := IndexDescriptor{
			Name:        "idx_data",
			Type:        IndexDescriptor_INVERTED,
			ColumnNames: []string{"data"},
			ColumnIDs:   []ColumnID{2},
		}

		err := desc.AddIndex(idx, false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Indexes) != 1 {
			t.Errorf("expected 1 inverted index, got %d", len(desc.Indexes))
		}

		if desc.Indexes[0].Name != "idx_data" {
			t.Errorf("expected index name 'idx_data', got %q", desc.Indexes[0].Name)
		}

		if desc.Indexes[0].Type != IndexDescriptor_INVERTED {
			t.Errorf("expected index type %v, got %v", IndexDescriptor_INVERTED, desc.Indexes[0].Type)
		}
	})
}

func TestMutableTableDescriptor_AddColumnToFamilyMaybeCreate(t *testing.T) {
	t.Run("add column to existing family", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		err := desc.AddColumnToFamilyMaybeCreate("name", "family1", false, false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Families[0].ColumnNames) != 2 {
			t.Errorf("expected 2 columns in family1, got %d", len(desc.Families[0].ColumnNames))
		}

		if desc.Families[0].ColumnNames[1] != "name" {
			t.Errorf("expected column 'name' in family1, got %q", desc.Families[0].ColumnNames[1])
		}
	})

	t.Run("add column to non-existent family without create", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		err := desc.AddColumnToFamilyMaybeCreate("name", "family2", false, false)
		if err == nil {
			t.Error("expected error for non-existent family, but got nil")
		}
	})

	t.Run("add column to non-existent family with create", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		err := desc.AddColumnToFamilyMaybeCreate("name", "family2", true, false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Families) != 2 {
			t.Errorf("expected 2 families, got %d", len(desc.Families))
		}

		if desc.Families[1].Name != "family2" {
			t.Errorf("expected family name 'family2', got %q", desc.Families[1].Name)
		}

		if len(desc.Families[1].ColumnNames) != 1 {
			t.Errorf("expected 1 column in family2, got %d", len(desc.Families[1].ColumnNames))
		}

		if desc.Families[1].ColumnNames[0] != "name" {
			t.Errorf("expected column 'name' in family2, got %q", desc.Families[1].ColumnNames[0])
		}
	})

	t.Run("add column to existing family with create and ifNotExists false", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		err := desc.AddColumnToFamilyMaybeCreate("name", "family1", true, false)
		if err == nil {
			t.Error("expected error for existing family with create and ifNotExists false, but got nil")
		}
	})

	t.Run("add column to existing family with create and ifNotExists true", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		err := desc.AddColumnToFamilyMaybeCreate("name", "family1", true, true)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(desc.Families[0].ColumnNames) != 2 {
			t.Errorf("expected 2 columns in family1, got %d", len(desc.Families[0].ColumnNames))
		}

		if desc.Families[0].ColumnNames[1] != "name" {
			t.Errorf("expected column 'name' in family1, got %q", desc.Families[0].ColumnNames[1])
		}
	})
}

func TestMutableTableDescriptor_RemoveColumnFromFamily(t *testing.T) {
	t.Run("remove column from family", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id", "name", "age"},
					ColumnIDs:   []ColumnID{1, 2, 3},
				},
			},
		})

		desc.RemoveColumnFromFamily(2) // Remove column with ID 2 (name)

		if len(desc.Families[0].ColumnNames) != 2 {
			t.Errorf("expected 2 columns in family1, got %d", len(desc.Families[0].ColumnNames))
		}

		if len(desc.Families[0].ColumnIDs) != 2 {
			t.Errorf("expected 2 column IDs in family1, got %d", len(desc.Families[0].ColumnIDs))
		}

		if desc.Families[0].ColumnNames[0] != "id" || desc.Families[0].ColumnNames[1] != "age" {
			t.Errorf("expected columns ['id', 'age'], got %v", desc.Families[0].ColumnNames)
		}

		if desc.Families[0].ColumnIDs[0] != 1 || desc.Families[0].ColumnIDs[1] != 3 {
			t.Errorf("expected column IDs [1, 3], got %v", desc.Families[0].ColumnIDs)
		}
	})

	t.Run("remove column from non-existent family", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		// This should not panic
		desc.RemoveColumnFromFamily(999) // Remove non-existent column

		if len(desc.Families) != 1 {
			t.Errorf("expected 1 family, got %d", len(desc.Families))
		}

		if len(desc.Families[0].ColumnNames) != 1 {
			t.Errorf("expected 1 column in family1, got %d", len(desc.Families[0].ColumnNames))
		}
	})

	t.Run("remove all columns from non-zero family", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
				{
					ID:          1,
					Name:        "family2",
					ColumnNames: []string{"name"},
					ColumnIDs:   []ColumnID{2},
				},
			},
		})

		desc.RemoveColumnFromFamily(2) // Remove the only column from family2

		if len(desc.Families) != 1 {
			t.Errorf("expected 1 family, got %d", len(desc.Families))
		}

		if desc.Families[0].Name != "family1" {
			t.Errorf("expected family name 'family1', got %q", desc.Families[0].Name)
		}
	})

	t.Run("remove all columns from family 0", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family0",
					ColumnNames: []string{"id"},
					ColumnIDs:   []ColumnID{1},
				},
			},
		})

		desc.RemoveColumnFromFamily(1) // Remove the only column from family0

		if len(desc.Families) != 1 {
			t.Errorf("expected 1 family (family0 should not be removed), got %d", len(desc.Families))
		}

		if desc.Families[0].ID != 0 {
			t.Errorf("expected family ID 0, got %d", desc.Families[0].ID)
		}

		if len(desc.Families[0].ColumnNames) != 0 {
			t.Errorf("expected 0 columns in family0, got %d", len(desc.Families[0].ColumnNames))
		}
	})
}

func TestMutableTableDescriptor_RenameColumnDescriptor(t *testing.T) {
	t.Run("rename column in table and families", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "old_name"},
				{ID: 3, Name: "age"},
			},
			Families: []ColumnFamilyDescriptor{
				{
					ID:          0,
					Name:        "family1",
					ColumnNames: []string{"id", "old_name"},
					ColumnIDs:   []ColumnID{1, 2},
				},
			},
		})

		column := &desc.Columns[1] // Column with old_name
		newName := "new_name"

		desc.RenameColumnDescriptor(column, newName)

		// Check that the column name was updated in the columns list
		if desc.Columns[1].Name != newName {
			t.Errorf("expected column name %q, got %q", newName, desc.Columns[1].Name)
		}

		// Check that the column name was updated in the family
		if desc.Families[0].ColumnNames[1] != newName {
			t.Errorf("expected column name %q in family1, got %q", newName, desc.Families[0].ColumnNames[1])
		}
	})

	t.Run("rename column in indexes", func(t *testing.T) {
		desc := NewMutableExistingTableDescriptor(TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "old_name"},
			},
			PrimaryIndex: IndexDescriptor{
				Name:        PrimaryKeyIndexName,
				Type:        IndexDescriptor_FORWARD,
				ColumnNames: []string{"id"},
				ColumnIDs:   []ColumnID{1},
			},
			Indexes: []IndexDescriptor{
				{
					Name:             "idx_old_name",
					Type:             IndexDescriptor_FORWARD,
					ColumnNames:      []string{"old_name"},
					ColumnIDs:        []ColumnID{2},
					StoreColumnNames: []string{"id"},
					StoreColumnIDs:   []ColumnID{1},
				},
			},
		})

		column := &desc.Columns[1] // Column with old_name
		newName := "new_name"

		desc.RenameColumnDescriptor(column, newName)

		// Check that the column name was updated in the secondary index
		if desc.Indexes[0].ColumnNames[0] != newName {
			t.Errorf("expected column name %q in index, got %q", newName, desc.Indexes[0].ColumnNames[0])
		}
	})
}

func TestTableDescriptor_FindActiveColumnsByNames(t *testing.T) {
	t.Run("find active columns by names", func(t *testing.T) {
		desc := &TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
				{ID: 3, Name: "age"},
			},
		}

		names := tree.NameList{"id", "name", "age"}
		cols, err := desc.FindActiveColumnsByNames(names)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(cols) != 3 {
			t.Errorf("expected 3 columns, got %d", len(cols))
		}

		if cols[0].Name != "id" || cols[1].Name != "name" || cols[2].Name != "age" {
			t.Errorf("expected columns ['id', 'name', 'age'], got %v", cols)
		}
	})

	t.Run("find active columns with non-existent column", func(t *testing.T) {
		desc := &TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
		}

		names := tree.NameList{"id", "non_existent", "name"}
		cols, err := desc.FindActiveColumnsByNames(names)
		if err == nil {
			t.Error("expected error for non-existent column, but got nil")
		}

		if cols != nil {
			t.Errorf("expected nil columns for error case, got %v", cols)
		}
	})

	t.Run("find active columns with empty names list", func(t *testing.T) {
		desc := &TableDescriptor{
			Name: "test_table",
			Columns: []ColumnDescriptor{
				{ID: 1, Name: "id"},
				{ID: 2, Name: "name"},
			},
		}

		names := tree.NameList{}
		cols, err := desc.FindActiveColumnsByNames(names)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(cols) != 0 {
			t.Errorf("expected 0 columns for empty names list, got %d", len(cols))
		}
	})
}
