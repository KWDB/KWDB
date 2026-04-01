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
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
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
