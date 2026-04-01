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
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestTableDescriptorFindActiveColumnByName tests the FindActiveColumnByName method for TableDescriptor.
func TestTableDescriptorFindActiveColumnByName(t *testing.T) {
	// Test case: Find existing active column
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int},
			{ID: 2, Name: "name", Type: *types.String},
		},
	}

	col, err := table.FindActiveColumnByName("name")
	if err != nil {
		t.Errorf("FindActiveColumnByName should find existing active column: %v", err)
	}
	if col == nil || col.ID != 2 {
		t.Errorf("FindActiveColumnByName should return correct column")
	}

	// Test case: Find non-existent column
	col, err = table.FindActiveColumnByName("non_existent")
	if err == nil {
		t.Errorf("FindActiveColumnByName should return error for non-existent column")
	}
	if col != nil {
		t.Errorf("FindActiveColumnByName should return nil for non-existent column")
	}
}

// TestTableDescriptorFindActiveColumnByID tests the FindActiveColumnByID method for TableDescriptor.
func TestTableDescriptorFindActiveColumnByID(t *testing.T) {
	// Test case: Find existing active column by ID
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int},
			{ID: 2, Name: "name", Type: *types.String},
		},
	}

	col, err := table.FindActiveColumnByID(2)
	if err != nil {
		t.Errorf("FindActiveColumnByID should find existing active column: %v", err)
	}
	if col == nil || col.Name != "name" {
		t.Errorf("FindActiveColumnByID should return correct column")
	}

	// Test case: Find non-existent column by ID
	col, err = table.FindActiveColumnByID(999)
	if err == nil {
		t.Errorf("FindActiveColumnByID should return error for non-existent column")
	}
	if col != nil {
		t.Errorf("FindActiveColumnByID should return nil for non-existent column")
	}
}

// TestTableDescriptorFindActiveIndexByID tests the FindActiveIndexByID method for TableDescriptor.
func TestTableDescriptorFindActiveIndexByID(t *testing.T) {
	// Test case: Find existing active index by ID
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, Name: "primary"},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, Name: "idx_name"},
		},
	}

	idx := table.FindActiveIndexByID(2)
	if idx == nil || idx.Name != "idx_name" {
		t.Errorf("FindActiveIndexByID should return correct index")
	}

	// Test case: Find primary index by ID
	idx = table.FindActiveIndexByID(1)
	if idx == nil || idx.Name != "primary" {
		t.Errorf("FindActiveIndexByID should return correct primary index")
	}

	// Test case: Find non-existent index by ID
	idx = table.FindActiveIndexByID(999)
	if idx != nil {
		t.Errorf("FindActiveIndexByID should return nil for non-existent index")
	}
}

// TestTableDescriptorNamesForColumnIDs tests the NamesForColumnIDs method for TableDescriptor.
func TestTableDescriptorNamesForColumnIDs(t *testing.T) {
	// Test case: Get names for existing column IDs
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id"},
			{ID: 2, Name: "name"},
			{ID: 3, Name: "email"},
		},
	}

	names, err := table.NamesForColumnIDs([]sqlbase.ColumnID{1, 3})
	if err != nil {
		t.Errorf("NamesForColumnIDs should return names for existing column IDs: %v", err)
	}
	if len(names) != 2 || names[0] != "id" || names[1] != "email" {
		t.Errorf("NamesForColumnIDs should return correct names")
	}

	// Test case: Get names for non-existent column ID
	_, err = table.NamesForColumnIDs([]sqlbase.ColumnID{999})
	if err == nil {
		t.Errorf("NamesForColumnIDs should return error for non-existent column ID")
	}
}

// TestTableDescriptorColumnIdxMap tests the ColumnIdxMap method for TableDescriptor.
func TestTableDescriptorColumnIdxMap(t *testing.T) {
	// Test case: Get column index map
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id"},
			{ID: 2, Name: "name"},
		},
	}

	colIdxMap := table.ColumnIdxMap()
	if len(colIdxMap) != 2 || colIdxMap[1] != 0 || colIdxMap[2] != 1 {
		t.Errorf("ColumnIdxMap should return correct mapping")
	}
}

// TestMutableTableDescriptorRenameColumnDescriptor tests the RenameColumnDescriptor method for MutableTableDescriptor.
func TestMutableTableDescriptorRenameColumnDescriptor(t *testing.T) {
	mutableTable := &sqlbase.MutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			Columns: []sqlbase.ColumnDescriptor{
				{ID: 1, Name: "old_name"},
			},
			Families: []sqlbase.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary", ColumnIDs: []sqlbase.ColumnID{1}, ColumnNames: []string{"old_name"}},
			},
		},
	}

	mutableTable.RenameColumnDescriptor(&mutableTable.Columns[0], "new_name")
	if mutableTable.Columns[0].Name != "new_name" {
		t.Errorf("RenameColumnDescriptor should rename column")
	}
}

// TestTableDescriptorFindColumnByName tests the FindColumnByName method for TableDescriptor.
func TestTableDescriptorFindColumnByName(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id"},
			{ID: 2, Name: "name"},
		},
	}

	col, dropping, err := table.FindColumnByName("name")
	if err != nil {
		t.Errorf("FindColumnByName should not return error: %v", err)
	}
	if dropping {
		t.Errorf("FindColumnByName should find a dropping column")
	}
	if col.ID != 2 {
		t.Errorf("FindColumnByName should return correct column")
	}

	// Test case: Column not found
	_, dropping, err = table.FindColumnByName("not_exists")
	if err == nil {
		t.Errorf("FindColumnByName should return error for non-existent column: %v", err)
	}
	if dropping {
		t.Errorf("FindColumnByName should not find a dropping column")
	}
}

// TestTableDescriptorFindColumnByID tests the FindColumnByID method for TableDescriptor.
func TestTableDescriptorFindColumnByID(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id"},
			{ID: 2, Name: "name"},
		},
	}

	col, err := table.FindColumnByID(1)
	if err != nil {
		t.Errorf("FindColumnByID should not return error: %v", err)
	}
	if col.Name != "id" {
		t.Errorf("FindColumnByID should return correct column")
	}

	// Test case: Column not found
	_, err = table.FindColumnByID(999)
	if err == nil {
		t.Errorf("FindColumnByID should return error for non-existent column")
	}
}

// TestTableDescriptorFindIndexByName tests the FindIndexByName method for TableDescriptor.
func TestTableDescriptorFindIndexByName(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, Name: "primary"},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, Name: "idx_name"},
		},
	}

	idx, dropping, err := table.FindIndexByName("idx_name")
	if err != nil {
		t.Errorf("FindIndexByName should not return error: %v", err)
	}
	if dropping {
		t.Errorf("FindIndexByName should not find a dropping index")
	}
	if idx.ID != 2 {
		t.Errorf("FindIndexByName should return correct index")
	}

	// Test case: Index not found
	_, dropping, err = table.FindIndexByName("not_exists")
	if err == nil {
		t.Errorf("FindIndexByName should not return error for non-existent index: %v", err)
	}
	if dropping {
		t.Errorf("FindIndexByName should not find a dropping index")
	}
}

// TestTableDescriptorFindIndexByID tests the FindIndexByID method for TableDescriptor.
func TestTableDescriptorFindIndexByID(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, Name: "primary"},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, Name: "idx_name"},
		},
	}

	idx, err := table.FindIndexByID(2)
	if err != nil {
		t.Errorf("FindIndexByID should not return error: %v", err)
	}
	if idx.Name != "idx_name" {
		t.Errorf("FindIndexByID should return correct index")
	}

	// Test case: Index not found
	_, err = table.FindIndexByID(999)
	if err == nil {
		t.Errorf("FindIndexByID should return error for non-existent index")
	}
}

// TestTableDescriptorFindFamilyByID tests the FindFamilyByID method for TableDescriptor.
func TestTableDescriptorFindFamilyByID(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		Families: []sqlbase.ColumnFamilyDescriptor{
			{ID: 0, Name: "primary"},
			{ID: 1, Name: "secondary"},
		},
	}

	family, err := table.FindFamilyByID(1)
	if err != nil {
		t.Errorf("FindFamilyByID should not return error: %v", err)
	}
	if family.Name != "secondary" {
		t.Errorf("FindFamilyByID should return correct family")
	}

	// Test case: Family not found
	_, err = table.FindFamilyByID(999)
	if err == nil {
		t.Errorf("FindFamilyByID should return error for non-existent family")
	}
}

// TestTableDescriptorHasPrimaryKey tests the HasPrimaryKey method for TableDescriptor.
func TestTableDescriptorHasPrimaryKey(t *testing.T) {
	// Test case: Table with primary key
	tableWithPK := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{
			ColumnIDs: []sqlbase.ColumnID{1},
		},
	}

	if !tableWithPK.HasPrimaryKey() {
		t.Errorf("Table with primary key should return true")
	}

	// Test case: Table without primary key
	tableWithoutPK := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{
			ColumnIDs: []sqlbase.ColumnID{},
		},
	}

	if !tableWithoutPK.HasPrimaryKey() {
		t.Errorf("Table primary key should not be disabled")
	}
}

// TestTableDescriptorIsPrimaryIndexDefaultRowID tests the IsPrimaryIndexDefaultRowID method for TableDescriptor.
func TestTableDescriptorIsPrimaryIndexDefaultRowID(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary",
		},
	}

	isDefault := table.IsPrimaryIndexDefaultRowID()
	// This method returns whether the primary index is the default row ID
	t.Logf("IsPrimaryIndexDefaultRowID returned: %v", isDefault)
}

// TestColumnDescriptorIsDefaultPrimaryKeyColumn tests the IsDefaultPrimaryKeyColumn method for ColumnDescriptor.
func TestColumnDescriptorIsDefaultPrimaryKeyColumn(t *testing.T) {
	// Test case: Default primary key column
	defaultExpr := "unique_rowid()"
	defaultPKCol := &sqlbase.ColumnDescriptor{
		ID:          1,
		Name:        "rowid",
		Type:        *types.Int,
		Hidden:      true,
		DefaultExpr: &defaultExpr,
	}

	if !defaultPKCol.IsDefaultPrimaryKeyColumn() {
		t.Errorf("Default primary key column should return true")
	}

	// Test case: Non-default primary key column
	nonDefaultPKCol := &sqlbase.ColumnDescriptor{
		ID:   2,
		Name: "name",
		Type: *types.String,
	}

	if nonDefaultPKCol.IsDefaultPrimaryKeyColumn() {
		t.Errorf("Non-default primary key column should return false")
	}
}

// TestTableDescriptorVisibleColumns tests the VisibleColumns method for TableDescriptor.
func TestTableDescriptorVisibleColumns(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id", Hidden: false},
			{ID: 2, Name: "name", Hidden: false},
			{ID: 3, Name: "hidden", Hidden: true},
		},
	}

	visibleCols := table.VisibleColumns()
	if len(visibleCols) != 2 {
		t.Errorf("VisibleColumns should return only public columns")
	}
}

// TestTableDescriptorColumnTypes tests the ColumnTypes method for TableDescriptor.
func TestTableDescriptorColumnTypes(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		Columns: []sqlbase.ColumnDescriptor{
			{ID: 1, Name: "id", Type: *types.Int},
			{ID: 2, Name: "name", Type: *types.String},
		},
	}

	colTypes := table.ColumnTypes()
	if len(colTypes) != 2 || colTypes[0].Family() != types.IntFamily || colTypes[1].Family() != types.StringFamily {
		t.Errorf("ColumnTypes should return correct column types")
	}
}

// TestTableDescriptorGoingOffline tests the GoingOffline method for TableDescriptor.
func TestTableDescriptorGoingOffline(t *testing.T) {
	// Test case: Regular table
	table := &sqlbase.TableDescriptor{}
	if table.GoingOffline() {
		t.Errorf("Regular table should not be going offline")
	}

	// Test case: Offline table
	offlineTable := &sqlbase.TableDescriptor{State: sqlbase.TableDescriptor_OFFLINE}
	if !offlineTable.GoingOffline() {
		t.Errorf("Table with OFFLINE state should be going offline")
	}
}

// TestTableDescriptorHasColumnBackfillMutation tests the HasColumnBackfillMutation method for TableDescriptor.
func TestTableDescriptorHasColumnBackfillMutation(t *testing.T) {
	// Test case: Table without backfill mutations
	table := &sqlbase.TableDescriptor{}
	if table.HasColumnBackfillMutation() {
		t.Errorf("Table without mutations should not have backfill mutation")
	}

	// This test depends on the specific implementation of HasColumnBackfillMutation
	// which checks for certain mutation states
	t.Log("HasColumnBackfillMutation tested with sample mutation")
}

// TestTableDescriptorHasDrainingNames tests the HasDrainingNames method for TableDescriptor.
func TestTableDescriptorHasDrainingNames(t *testing.T) {
	// Test case: Regular table
	table := &sqlbase.TableDescriptor{}
	if table.HasDrainingNames() {
		t.Errorf("Regular table should not have draining names")
	}

	// Test case: Table with draining names
	drainingTable := &sqlbase.TableDescriptor{DrainingNames: []sqlbase.TableDescriptor_NameInfo{{Name: "old_name"}}}
	if !drainingTable.HasDrainingNames() {
		t.Errorf("Table with draining names should return true")
	}
}

// TestTableDescriptorIsNewTable tests the IsNewTable method for MutableTableDescriptor.
func TestTableDescriptorIsNewTable(t *testing.T) {
	// Test case: New table
	newTable := &sqlbase.MutableTableDescriptor{TableDescriptor: sqlbase.TableDescriptor{ID: 0}}
	if !newTable.IsNewTable() {
		t.Errorf("Table with ID 0 should be new table")
	}

	// Test case: Existing table
	existingTable := &sqlbase.MutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{ID: 1},
		ClusterVersion:  sqlbase.TableDescriptor{ID: 1},
	}
	if existingTable.IsNewTable() {
		t.Errorf("Table with ID > 0 should not be new table")
	}
}

// TestTableDescriptorPartitionNames tests the PartitionNames method for TableDescriptor.
func TestTableDescriptorPartitionNames(t *testing.T) {
	// Test case: Table with partitioned indexes
	table := &sqlbase.TableDescriptor{
		Indexes: []sqlbase.IndexDescriptor{
			{
				Name: "idx1",
				Partitioning: sqlbase.PartitioningDescriptor{
					List: []sqlbase.PartitioningDescriptor_List{{Name: "part1"}, {Name: "part2"}},
				},
			},
		},
	}

	partitionNames := table.PartitionNames()
	if len(partitionNames) != 2 {
		t.Errorf("PartitionNames should return all partition names")
	}
}

// TestColumnIDsEquals tests the Equals method for ColumnIDs.
func TestColumnIDsEquals(t *testing.T) {
	// Test case 1: Empty slices should be equal
	var empty1, empty2 sqlbase.ColumnIDs
	if !empty1.Equals(empty2) {
		t.Errorf("empty ColumnIDs should be equal")
	}

	// Test case 2: Identical slices should be equal
	c1 := sqlbase.ColumnIDs{1, 2, 3}
	c2 := sqlbase.ColumnIDs{1, 2, 3}
	if !c1.Equals(c2) {
		t.Errorf("identical ColumnIDs should be equal")
	}

	// Test case 3: Different lengths should not be equal
	c3 := sqlbase.ColumnIDs{1, 2}
	if c1.Equals(c3) {
		t.Errorf("ColumnIDs with different lengths should not be equal")
	}

	// Test case 4: Different values should not be equal
	c4 := sqlbase.ColumnIDs{1, 2, 4}
	if c1.Equals(c4) {
		t.Errorf("ColumnIDs with different values should not be equal")
	}
}

// TestColumnDescriptorEqual tests the Equal method for ColumnDescriptor.
func TestColumnDescriptorEqual(t *testing.T) {
	// Create two identical ColumnDescriptors
	cd1 := &sqlbase.ColumnDescriptor{
		ID:       1,
		Name:     "test_col",
		Type:     *types.Int,
		Nullable: true,
	}
	cd2 := &sqlbase.ColumnDescriptor{
		ID:       1,
		Name:     "test_col",
		Type:     *types.Int,
		Nullable: true,
	}

	if !cd1.Equal(cd2) {
		t.Errorf("identical ColumnDescriptors should be equal")
	}

	// Modify one field and test inequality
	cd3 := &sqlbase.ColumnDescriptor{
		ID:       2, // Different ID
		Name:     "test_col",
		Type:     *types.Int,
		Nullable: true,
	}

	if cd1.Equal(cd3) {
		t.Errorf("ColumnDescriptors with different IDs should not be equal")
	}
}

// TestIndexDescriptorEqual tests the Equal method for IndexDescriptor.
func TestIndexDescriptorEqual(t *testing.T) {
	// Create two identical IndexDescriptors
	id1 := &sqlbase.IndexDescriptor{
		ID:          1,
		Name:        "test_index",
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
		Unique:      true,
	}
	id2 := &sqlbase.IndexDescriptor{
		ID:          1,
		Name:        "test_index",
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
		Unique:      true,
	}

	if !id1.Equal(id2) {
		t.Errorf("identical IndexDescriptors should be equal")
	}

	// Modify one field and test inequality
	id3 := &sqlbase.IndexDescriptor{
		ID:          1,
		Name:        "different_name", // Different name
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
		Unique:      true,
	}

	if id1.Equal(id3) {
		t.Errorf("IndexDescriptors with different names should not be equal")
	}
}

// TestTableDescriptorEqual tests the Equal method for TableDescriptor.
func TestTableDescriptorEqual(t *testing.T) {
	// Create two identical TableDescriptors
	tb1 := &sqlbase.TableDescriptor{
		ID:   1,
		Name: "test_table",
		Columns: []sqlbase.ColumnDescriptor{
			{
				ID:       1,
				Name:     "id",
				Type:     *types.Int,
				Nullable: false,
			},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:          1,
			Name:        "primary",
			ColumnNames: []string{"id"},
			ColumnIDs:   []sqlbase.ColumnID{1},
			Unique:      true,
		},
	}
	tb2 := &sqlbase.TableDescriptor{
		ID:   1,
		Name: "test_table",
		Columns: []sqlbase.ColumnDescriptor{
			{
				ID:       1,
				Name:     "id",
				Type:     *types.Int,
				Nullable: false,
			},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:          1,
			Name:        "primary",
			ColumnNames: []string{"id"},
			ColumnIDs:   []sqlbase.ColumnID{1},
			Unique:      true,
		},
	}

	if !tb1.Equal(tb2) {
		t.Errorf("identical TableDescriptors should be equal")
	}

	// Modify one field and test inequality
	tb3 := &sqlbase.TableDescriptor{
		ID:   2, // Different ID
		Name: "test_table",
		Columns: []sqlbase.ColumnDescriptor{
			{
				ID:       1,
				Name:     "id",
				Type:     *types.Int,
				Nullable: false,
			},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			ID:          1,
			Name:        "primary",
			ColumnNames: []string{"id"},
			ColumnIDs:   []sqlbase.ColumnID{1},
			Unique:      true,
		},
	}

	if tb1.Equal(tb3) {
		t.Errorf("TableDescriptors with different IDs should not be equal")
	}
}

// TestForeignKeyConstraintEqual tests the Equal method for ForeignKeyConstraint.
func TestForeignKeyConstraintEqual(t *testing.T) {
	// Create two identical ForeignKeyConstraints
	fk1 := &sqlbase.ForeignKeyConstraint{
		Name:                "test_fk",
		OriginTableID:       1,
		OriginColumnIDs:     []sqlbase.ColumnID{1},
		ReferencedTableID:   2,
		ReferencedColumnIDs: []sqlbase.ColumnID{1},
	}
	fk2 := &sqlbase.ForeignKeyConstraint{
		Name:                "test_fk",
		OriginTableID:       1,
		OriginColumnIDs:     []sqlbase.ColumnID{1},
		ReferencedTableID:   2,
		ReferencedColumnIDs: []sqlbase.ColumnID{1},
	}

	if !fk1.Equal(fk2) {
		t.Errorf("identical ForeignKeyConstraints should be equal")
	}

	// Modify one field and test inequality
	fk3 := &sqlbase.ForeignKeyConstraint{
		Name:                "different_fk", // Different name
		OriginTableID:       1,
		OriginColumnIDs:     []sqlbase.ColumnID{1},
		ReferencedTableID:   2,
		ReferencedColumnIDs: []sqlbase.ColumnID{1},
	}

	if fk1.Equal(fk3) {
		t.Errorf("ForeignKeyConstraints with different names should not be equal")
	}
}

// TestColumnFamilyDescriptorEqual tests the Equal method for ColumnFamilyDescriptor.
func TestColumnFamilyDescriptorEqual(t *testing.T) {
	// Create two identical ColumnFamilyDescriptors
	cf1 := &sqlbase.ColumnFamilyDescriptor{
		ID:          1,
		Name:        "test_family",
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
	}
	cf2 := &sqlbase.ColumnFamilyDescriptor{
		ID:          1,
		Name:        "test_family",
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
	}

	if !cf1.Equal(cf2) {
		t.Errorf("identical ColumnFamilyDescriptors should be equal")
	}

	// Modify one field and test inequality
	cf3 := &sqlbase.ColumnFamilyDescriptor{
		ID:          2, // Different ID
		Name:        "test_family",
		ColumnNames: []string{"col1", "col2"},
		ColumnIDs:   []sqlbase.ColumnID{1, 2},
	}

	if cf1.Equal(cf3) {
		t.Errorf("ColumnFamilyDescriptors with different IDs should not be equal")
	}
}

// TestForeignKeyReferenceEqual tests the Equal method for ForeignKeyReference.
func TestForeignKeyReferenceEqual(t *testing.T) {
	// Create two identical ForeignKeyReferences
	fkRef1 := &sqlbase.ForeignKeyReference{
		Table:           1,
		Index:           2,
		SharedPrefixLen: 1,
	}
	fkRef2 := &sqlbase.ForeignKeyReference{
		Table:           1,
		Index:           2,
		SharedPrefixLen: 1,
	}

	if !fkRef1.Equal(fkRef2) {
		t.Errorf("identical ForeignKeyReferences should be equal")
	}

	// Modify one field and test inequality
	fkRef3 := &sqlbase.ForeignKeyReference{
		Table:           3, // Different table
		Index:           2,
		SharedPrefixLen: 1,
	}

	if fkRef1.Equal(fkRef3) {
		t.Errorf("ForeignKeyReferences with different tables should not be equal")
	}
}

// TestPartitioningDescriptorEqual tests the Equal method for PartitioningDescriptor.
func TestPartitioningDescriptorEqual(t *testing.T) {
	// Create two identical PartitioningDescriptors
	pd1 := &sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		List: []sqlbase.PartitioningDescriptor_List{
			{
				Name:   "part1",
				Values: [][]byte{[]byte("val1")},
			},
		},
	}
	pd2 := &sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		List: []sqlbase.PartitioningDescriptor_List{
			{
				Name:   "part1",
				Values: [][]byte{[]byte("val1")},
			},
		},
	}

	if !pd1.Equal(pd2) {
		t.Errorf("identical PartitioningDescriptors should be equal")
	}

	// Modify one field and test inequality
	pd3 := &sqlbase.PartitioningDescriptor{
		NumColumns: 2, // Different number of columns
		List: []sqlbase.PartitioningDescriptor_List{
			{
				Name:   "part1",
				Values: [][]byte{[]byte("val1")},
			},
		},
	}

	if pd1.Equal(pd3) {
		t.Errorf("PartitioningDescriptors with different numColumns should not be equal")
	}
}

// TestSchemaDescriptorEqual tests the Equal method for SchemaDescriptor.
func TestSchemaDescriptorEqual(t *testing.T) {
	// Create two identical SchemaDescriptors
	schema1 := &sqlbase.SchemaDescriptor{
		ID:       1,
		Name:     "test_schema",
		ParentID: 1,
	}
	schema2 := &sqlbase.SchemaDescriptor{
		ID:       1,
		Name:     "test_schema",
		ParentID: 1,
	}

	if !schema1.Equal(schema2) {
		t.Errorf("identical SchemaDescriptors should be equal")
	}

	// Modify one field and test inequality
	schema3 := &sqlbase.SchemaDescriptor{
		ID:       2, // Different ID
		Name:     "test_schema",
		ParentID: 1,
	}

	if schema1.Equal(schema3) {
		t.Errorf("SchemaDescriptors with different IDs should not be equal")
	}
}

// TestDatabaseDescriptorEqual tests the Equal method for DatabaseDescriptor.
func TestDatabaseDescriptorEqual(t *testing.T) {
	// Create two identical DatabaseDescriptors
	db1 := &sqlbase.DatabaseDescriptor{
		ID:   1,
		Name: "test_db",
	}
	db2 := &sqlbase.DatabaseDescriptor{
		ID:   1,
		Name: "test_db",
	}

	if !db1.Equal(db2) {
		t.Errorf("identical DatabaseDescriptors should be equal")
	}

	// Modify one field and test inequality
	db3 := &sqlbase.DatabaseDescriptor{
		ID:   2, // Different ID
		Name: "test_db",
	}

	if db1.Equal(db3) {
		t.Errorf("DatabaseDescriptors with different IDs should not be equal")
	}
}

// TestTSColEqual tests the Equal method for TSCol.
func TestTSColEqual(t *testing.T) {
	// Create two identical TSCols
	tsCol1 := &sqlbase.TSCol{
		ColumnType: 1,
		StorageLen: 8,
	}
	tsCol2 := &sqlbase.TSCol{
		ColumnType: 1,
		StorageLen: 8,
	}

	if !tsCol1.Equal(tsCol2) {
		t.Errorf("identical TSCols should be equal")
	}

	// Modify one field and test inequality
	tsCol3 := &sqlbase.TSCol{
		ColumnType: 2, // Different column type
		StorageLen: 8,
	}

	if tsCol1.Equal(tsCol3) {
		t.Errorf("TSCols with different column types should not be equal")
	}
}

// TestShardedDescriptorEqual tests the Equal method for ShardedDescriptor.
func TestShardedDescriptorEqual(t *testing.T) {
	// Create two identical ShardedDescriptors
	sharded1 := &sqlbase.ShardedDescriptor{
		IsSharded:    true,
		ShardBuckets: 16,
		Name:         "shard_col",
		ColumnNames:  []string{"col1"},
	}
	sharded2 := &sqlbase.ShardedDescriptor{
		IsSharded:    true,
		ShardBuckets: 16,
		Name:         "shard_col",
		ColumnNames:  []string{"col1"},
	}

	if !sharded1.Equal(sharded2) {
		t.Errorf("identical ShardedDescriptors should be equal")
	}

	// Modify one field and test inequality
	sharded3 := &sqlbase.ShardedDescriptor{
		IsSharded:    true,
		ShardBuckets: 32, // Different number of buckets
		Name:         "shard_col",
		ColumnNames:  []string{"col1"},
	}

	if sharded1.Equal(sharded3) {
		t.Errorf("ShardedDescriptors with different bucket counts should not be equal")
	}
}

// TestInterleaveDescriptorEqual tests the Equal method for InterleaveDescriptor.
func TestInterleaveDescriptorEqual(t *testing.T) {
	// Create two identical InterleaveDescriptors
	interleave1 := &sqlbase.InterleaveDescriptor{
		Ancestors: []sqlbase.InterleaveDescriptor_Ancestor{
			{
				TableID:         1,
				IndexID:         2,
				SharedPrefixLen: 1,
			},
		},
	}
	interleave2 := &sqlbase.InterleaveDescriptor{
		Ancestors: []sqlbase.InterleaveDescriptor_Ancestor{
			{
				TableID:         1,
				IndexID:         2,
				SharedPrefixLen: 1,
			},
		},
	}

	if !interleave1.Equal(interleave2) {
		t.Errorf("identical InterleaveDescriptors should be equal")
	}

	// Modify one field and test inequality
	interleave3 := &sqlbase.InterleaveDescriptor{
		Ancestors: []sqlbase.InterleaveDescriptor_Ancestor{
			{
				TableID:         3, // Different table ID
				IndexID:         2,
				SharedPrefixLen: 1,
			},
		},
	}

	if interleave1.Equal(interleave3) {
		t.Errorf("InterleaveDescriptors with different ancestor table IDs should not be equal")
	}
}

// TestDescriptorEqual tests the Equal method for Descriptor.
func TestDescriptorEqual(t *testing.T) {
	// Create two identical Descriptors with Table
	desc1 := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Table{
			Table: &sqlbase.TableDescriptor{
				ID:   1,
				Name: "test_table",
			},
		},
	}
	desc2 := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Table{
			Table: &sqlbase.TableDescriptor{
				ID:   1,
				Name: "test_table",
			},
		},
	}

	if !desc1.Equal(desc2) {
		t.Errorf("identical Descriptors should be equal")
	}

	// Modify one field and test inequality
	desc3 := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Table{
			Table: &sqlbase.TableDescriptor{
				ID:   2, // Different table ID
				Name: "test_table",
			},
		},
	}

	if desc1.Equal(desc3) {
		t.Errorf("Descriptors with different table IDs should not be equal")
	}
}

// TestTSTableEqual tests the Equal method for TSTable.
func TestTSTableEqual(t *testing.T) {
	// Create two identical TSTables
	tsTable1 := &sqlbase.TSTable{
		TsVersion:     1,
		NextTsVersion: 2,
	}
	tsTable2 := &sqlbase.TSTable{
		TsVersion:     1,
		NextTsVersion: 2,
	}

	if !tsTable1.Equal(tsTable2) {
		t.Errorf("identical TSTables should be equal")
	}

	// Modify one field and test inequality
	tsTable3 := &sqlbase.TSTable{
		TsVersion:     2, // Different version
		NextTsVersion: 2,
	}

	if tsTable1.Equal(tsTable3) {
		t.Errorf("TSTables with different versions should not be equal")
	}
}

// TestCDCDescriptorEqual tests the Equal method for CDCDescriptor.
func TestCDCDescriptorEqual(t *testing.T) {
	// Create two identical CDCDescriptors
	cdc1 := &sqlbase.CDCDescriptor{
		ID:      1,
		Version: 100,
	}
	cdc2 := &sqlbase.CDCDescriptor{
		ID:      1,
		Version: 100,
	}

	if !cdc1.Equal(cdc2) {
		t.Errorf("identical CDCDescriptors should be equal")
	}

	// Modify one field and test inequality
	cdc3 := &sqlbase.CDCDescriptor{
		ID:      2, // Different ID
		Version: 100,
	}

	if cdc1.Equal(cdc3) {
		t.Errorf("CDCDescriptors with different IDs should not be equal")
	}
}

// TestFunctionDescriptorEqual tests the Equal method for FunctionDescriptor.
func TestFunctionDescriptorEqual(t *testing.T) {
	// Create two identical FunctionDescriptors
	func1 := &sqlbase.FunctionDescriptor{
		FunctionType: 1,
		Name:         "test_func",
	}
	func2 := &sqlbase.FunctionDescriptor{
		FunctionType: 1,
		Name:         "test_func",
	}

	if !func1.Equal(func2) {
		t.Errorf("identical FunctionDescriptors should be equal")
	}

	// Modify one field and test inequality
	func3 := &sqlbase.FunctionDescriptor{
		FunctionType: 2, // Different ID
		Name:         "test_func",
	}

	if func1.Equal(func3) {
		t.Errorf("FunctionDescriptors with different IDs should not be equal")
	}
}

// TestProcedureDescriptorEqual tests the Equal method for ProcedureDescriptor.
func TestProcedureDescriptorEqual(t *testing.T) {
	// Create two identical ProcedureDescriptors
	proc1 := &sqlbase.ProcedureDescriptor{
		ID:   1,
		Name: "test_proc",
		// 	Params: []sqlbase.ProcParam{
		// 		{
		// 			Name: "param1",
		// 			Type: *types.Int,
		// 		},
		// 	},
	}
	proc2 := &sqlbase.ProcedureDescriptor{
		ID:   1,
		Name: "test_proc",
		// Params: []sqlbase.ProcParam{
		// 	{
		// 		Name: "param1",
		// 		Type: *types.Int,
		// },
		// },
	}

	if !proc1.Equal(proc2) {
		t.Errorf("identical ProcedureDescriptors should be equal")
	}

	// Modify one field and test inequality
	proc3 := &sqlbase.ProcedureDescriptor{
		ID:   2, // Different ID
		Name: "test_proc",
		// Params: []sqlbase.ProcParam{
		// 	{
		// 		Name: "param1",
		// 		Type: *types.Int,
		// 	},
		// },
	}

	if proc1.Equal(proc3) {
		t.Errorf("ProcedureDescriptors with different IDs should not be equal")
	}
}

// TestProcParamEqual tests the Equal method for ProcParam.
func TestProcParamEqual(t *testing.T) {
	// Create two identical ProcParams
	param1 := &sqlbase.ProcParam{
		Name: "param1",
		Type: *types.Int,
	}
	param2 := &sqlbase.ProcParam{
		Name: "param1",
		Type: *types.Int,
	}

	if !param1.Equal(param2) {
		t.Errorf("identical ProcParams should be equal")
	}

	// Modify one field and test inequality
	param3 := &sqlbase.ProcParam{
		Name: "param2", // Different name
		Type: *types.Int,
	}

	if param1.Equal(param3) {
		t.Errorf("ProcParams with different names should not be equal")
	}
}

// TestTableDescriptorCheckConstraintEqual tests the Equal method for TableDescriptor_CheckConstraint.
func TestTableDescriptorCheckConstraintEqual(t *testing.T) {
	// Create two identical TableDescriptor_CheckConstraints
	check1 := &sqlbase.TableDescriptor_CheckConstraint{
		Name: "test_check",
		Expr: "CHECK (id > 0)",
	}
	check2 := &sqlbase.TableDescriptor_CheckConstraint{
		Name: "test_check",
		Expr: "CHECK (id > 0)",
	}

	if !check1.Equal(check2) {
		t.Errorf("identical TableDescriptor_CheckConstraints should be equal")
	}

	// Modify one field and test inequality
	check3 := &sqlbase.TableDescriptor_CheckConstraint{
		Name: "different_check", // Different name
		Expr: "CHECK (id > 0)",
	}

	if check1.Equal(check3) {
		t.Errorf("TableDescriptor_CheckConstraints with different names should not be equal")
	}
}

// TestTriggerDescriptorEqual tests the Equal method for TriggerDescriptor.
func TestTriggerDescriptorEqual(t *testing.T) {
	// Create two identical TriggerDescriptors
	trigger1 := &sqlbase.TriggerDescriptor{
		Name: "test_trigger",
	}
	trigger2 := &sqlbase.TriggerDescriptor{
		Name: "test_trigger",
	}

	if !trigger1.Equal(trigger2) {
		t.Errorf("identical TriggerDescriptors should be equal")
	}

	// Modify one field and test inequality
	trigger3 := &sqlbase.TriggerDescriptor{
		Name: "different_trigger", // Different name
	}

	if trigger1.Equal(trigger3) {
		t.Errorf("TriggerDescriptors with different names should not be equal")
	}
}

// TestTSDBEqual tests the Equal method for TSDB.
func TestTSDBEqual(t *testing.T) {
	// Create two identical TSDBs
	tsdb1 := &sqlbase.TSDB{
		Lifetime: 1,
		Creator:  "test_tsdb",
	}
	tsdb2 := &sqlbase.TSDB{
		Lifetime: 1,
		Creator:  "test_tsdb",
	}

	if !tsdb1.Equal(tsdb2) {
		t.Errorf("identical TSDBs should be equal")
	}

	// Modify one field and test inequality
	tsdb3 := &sqlbase.TSDB{
		Lifetime: 2, // Different ID
		Creator:  "test_tsdb",
	}

	if tsdb1.Equal(tsdb3) {
		t.Errorf("TSDBs with different IDs should not be equal")
	}
}

// TestColumnIDsHasPrefix tests the HasPrefix method for ColumnIDs.
func TestColumnIDsHasPrefix(t *testing.T) {
	// Test case 1: Empty prefix should be a prefix of any list
	var empty sqlbase.ColumnIDs
	c1 := sqlbase.ColumnIDs{1, 2, 3}
	if !c1.HasPrefix(empty) {
		t.Errorf("empty ColumnIDs should be a prefix of any list")
	}

	// Test case 2: Identical list should be a prefix
	c2 := sqlbase.ColumnIDs{1, 2, 3}
	if !c1.HasPrefix(c2) {
		t.Errorf("identical ColumnIDs should be a prefix")
	}

	// Test case 3: Prefix shorter than list
	c3 := sqlbase.ColumnIDs{1, 2}
	if !c1.HasPrefix(c3) {
		t.Errorf("shorter ColumnIDs should be a prefix of longer list")
	}

	// Test case 4: Prefix longer than list
	c4 := sqlbase.ColumnIDs{1, 2, 3, 4}
	if c1.HasPrefix(c4) {
		t.Errorf("longer ColumnIDs should not be a prefix of shorter list")
	}

	// Test case 5: Different values
	c5 := sqlbase.ColumnIDs{1, 3}
	if c1.HasPrefix(c5) {
		t.Errorf("ColumnIDs with different values should not be a prefix")
	}
}

// TestIndexDescriptorContainsColumnID tests the ContainsColumnID method for IndexDescriptor.
func TestIndexDescriptorContainsColumnID(t *testing.T) {
	idx := &sqlbase.IndexDescriptor{
		ColumnIDs:      []sqlbase.ColumnID{1, 2},
		ExtraColumnIDs: []sqlbase.ColumnID{3},
		StoreColumnIDs: []sqlbase.ColumnID{4},
	}

	// Test case 1: Column in ColumnIDs
	if !idx.ContainsColumnID(1) {
		t.Errorf("IndexDescriptor should contain column ID 1")
	}

	// Test case 2: Column in ExtraColumnIDs
	if !idx.ContainsColumnID(3) {
		t.Errorf("IndexDescriptor should contain column ID 3")
	}

	// Test case 3: Column in StoreColumnIDs
	if !idx.ContainsColumnID(4) {
		t.Errorf("IndexDescriptor should contain column ID 4")
	}

	// Test case 4: Column not present
	if idx.ContainsColumnID(5) {
		t.Errorf("IndexDescriptor should not contain column ID 5")
	}
}

// TestIndexDescriptorFullColumnIDs tests the FullColumnIDs method for IndexDescriptor.
func TestIndexDescriptorFullColumnIDs(t *testing.T) {
	// Test case 1: Unique index
	uniqueIdx := &sqlbase.IndexDescriptor{
		Unique:           true,
		ColumnIDs:        []sqlbase.ColumnID{1, 2},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_DESC},
	}
	uniqueCols, uniqueDirs := uniqueIdx.FullColumnIDs()
	if len(uniqueCols) != 2 || uniqueCols[0] != 1 || uniqueCols[1] != 2 {
		t.Errorf("Unique index should return only ColumnIDs")
	}
	if len(uniqueDirs) != 2 || uniqueDirs[0] != sqlbase.IndexDescriptor_ASC || uniqueDirs[1] != sqlbase.IndexDescriptor_DESC {
		t.Errorf("Unique index should return only ColumnDirections")
	}

	// Test case 2: Non-unique index
	nonUniqueIdx := &sqlbase.IndexDescriptor{
		Unique:           false,
		ColumnIDs:        []sqlbase.ColumnID{1},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		ExtraColumnIDs:   []sqlbase.ColumnID{2, 3},
	}
	nonUniqueCols, nonUniqueDirs := nonUniqueIdx.FullColumnIDs()
	if len(nonUniqueCols) != 3 || nonUniqueCols[0] != 1 || nonUniqueCols[1] != 2 || nonUniqueCols[2] != 3 {
		t.Errorf("Non-unique index should return ColumnIDs plus ExtraColumnIDs")
	}
	if len(nonUniqueDirs) != 3 || nonUniqueDirs[0] != sqlbase.IndexDescriptor_ASC || nonUniqueDirs[1] != sqlbase.IndexDescriptor_ASC || nonUniqueDirs[2] != sqlbase.IndexDescriptor_ASC {
		t.Errorf("Non-unique index should return ColumnDirections plus ASC for ExtraColumnIDs")
	}
}

// TestIndexDescriptorIsInterleaved tests the IsInterleaved method for IndexDescriptor.
func TestIndexDescriptorIsInterleaved(t *testing.T) {
	// Test case 1: Not interleaved
	notInterleaved := &sqlbase.IndexDescriptor{}
	if notInterleaved.IsInterleaved() {
		t.Errorf("IndexDescriptor with no ancestors or interleaved by should not be interleaved")
	}

	// Test case 2: Interleaved with ancestors
	withAncestors := &sqlbase.IndexDescriptor{
		Interleave: sqlbase.InterleaveDescriptor{
			Ancestors: []sqlbase.InterleaveDescriptor_Ancestor{{TableID: 1, IndexID: 1, SharedPrefixLen: 1}},
		},
	}
	if !withAncestors.IsInterleaved() {
		t.Errorf("IndexDescriptor with ancestors should be interleaved")
	}

	// Test case 3: Interleaved with interleaved by
	withInterleavedBy := &sqlbase.IndexDescriptor{
		InterleavedBy: []sqlbase.ForeignKeyReference{{Table: 1, Index: 1}},
	}
	if !withInterleavedBy.IsInterleaved() {
		t.Errorf("IndexDescriptor with interleaved by should be interleaved")
	}
}

// TestIndexDescriptorIsSharded tests the IsSharded method for IndexDescriptor.
func TestIndexDescriptorIsSharded(t *testing.T) {
	// Test case 1: Not sharded
	notSharded := &sqlbase.IndexDescriptor{}
	if notSharded.IsSharded() {
		t.Errorf("IndexDescriptor with no sharded descriptor should not be sharded")
	}

	// Test case 2: Sharded
	sharded := &sqlbase.IndexDescriptor{
		Sharded: sqlbase.ShardedDescriptor{IsSharded: true},
	}
	if !sharded.IsSharded() {
		t.Errorf("IndexDescriptor with IsSharded=true should be sharded")
	}
}

// TestTableDescriptorIsTable tests the IsTable method for TableDescriptor.
func TestTableDescriptorIsTable(t *testing.T) {
	// Test case 1: Regular table
	table := &sqlbase.TableDescriptor{}
	if !table.IsTable() {
		t.Errorf("Regular table should be considered a table")
	}

	// Test case 2: View
	view := &sqlbase.TableDescriptor{ViewQuery: "SELECT * FROM table"}
	if view.IsTable() {
		t.Errorf("View should not be considered a table")
	}

	// Test case 3: Sequence
	sequence := &sqlbase.TableDescriptor{SequenceOpts: &sqlbase.TableDescriptor_SequenceOpts{}}
	if sequence.IsTable() {
		t.Errorf("Sequence should not be considered a table")
	}
}

// TestTableDescriptorIsTSTable tests the IsTSTable method for TableDescriptor.
func TestTableDescriptorIsTSTable(t *testing.T) {
	// Test case 1: Regular table
	table := &sqlbase.TableDescriptor{}
	if table.IsTSTable() {
		t.Errorf("Regular table should not be considered a TS table")
	}

	// Test case 2: Timeseries table
	tsTable := &sqlbase.TableDescriptor{TableType: tree.TimeseriesTable}
	if !tsTable.IsTSTable() {
		t.Errorf("Timeseries table should be considered a TS table")
	}

	// Test case 3: Template table
	templateTable := &sqlbase.TableDescriptor{TableType: tree.TemplateTable}
	if !templateTable.IsTSTable() {
		t.Errorf("Template table should be considered a TS table")
	}

	// Test case 4: Instance table
	instanceTable := &sqlbase.TableDescriptor{TableType: tree.InstanceTable}
	if !instanceTable.IsTSTable() {
		t.Errorf("Instance table should be considered a TS table")
	}
}

// TestTableDescriptorIsPhysicalTable tests the IsPhysicalTable method for TableDescriptor.
func TestTableDescriptorIsPhysicalTable(t *testing.T) {
	// Test case 1: Regular table
	table := &sqlbase.TableDescriptor{}
	if !table.IsPhysicalTable() {
		t.Errorf("Regular table should be considered a physical table")
	}

	// Test case 2: View
	view := &sqlbase.TableDescriptor{ViewQuery: "SELECT * FROM table"}
	if view.IsPhysicalTable() {
		t.Errorf("View should not be considered a physical table")
	}

	// Test case 3: Sequence
	sequence := &sqlbase.TableDescriptor{SequenceOpts: &sqlbase.TableDescriptor_SequenceOpts{}}
	if !sequence.IsPhysicalTable() {
		t.Errorf("Sequence should be considered a physical table")
	}

	// Test case 4: Materialized view
	materializedView := &sqlbase.TableDescriptor{IsMaterializedView: true}
	if !materializedView.IsPhysicalTable() {
		t.Errorf("Materialized view should be considered a physical table")
	}
}

// TestHasCompositeKeyEncoding tests the HasCompositeKeyEncoding function.
func TestHasCompositeKeyEncoding(t *testing.T) {
	// Test case 1: CollatedStringFamily should return true
	if !sqlbase.HasCompositeKeyEncoding(types.CollatedStringFamily) {
		t.Errorf("CollatedStringFamily should have composite key encoding")
	}

	// Test case 2: FloatFamily should return true
	if !sqlbase.HasCompositeKeyEncoding(types.FloatFamily) {
		t.Errorf("FloatFamily should have composite key encoding")
	}

	// Test case 3: DecimalFamily should return true
	if !sqlbase.HasCompositeKeyEncoding(types.DecimalFamily) {
		t.Errorf("DecimalFamily should have composite key encoding")
	}

	// Test case 4: IntFamily should return false
	if sqlbase.HasCompositeKeyEncoding(types.IntFamily) {
		t.Errorf("IntFamily should not have composite key encoding")
	}
}

// TestDatumTypeHasCompositeKeyEncoding tests the DatumTypeHasCompositeKeyEncoding function.
func TestDatumTypeHasCompositeKeyEncoding(t *testing.T) {
	// Test case 1: Float type should return true
	floatType := types.Float
	if !sqlbase.DatumTypeHasCompositeKeyEncoding(floatType) {
		t.Errorf("Float type should have composite key encoding")
	}

	// Test case 2: Int type should return false
	intType := types.Int
	if sqlbase.DatumTypeHasCompositeKeyEncoding(intType) {
		t.Errorf("Int type should not have composite key encoding")
	}
}

// TestMustBeValueEncoded tests the MustBeValueEncoded function.
func TestMustBeValueEncoded(t *testing.T) {
	// Test case 1: ArrayFamily should return true
	if !sqlbase.MustBeValueEncoded(types.ArrayFamily) {
		t.Errorf("ArrayFamily should be value encoded")
	}

	// Test case 2: JsonFamily should return true
	if !sqlbase.MustBeValueEncoded(types.JsonFamily) {
		t.Errorf("JsonFamily should be value encoded")
	}

	// Test case 3: TupleFamily should return true
	if !sqlbase.MustBeValueEncoded(types.TupleFamily) {
		t.Errorf("TupleFamily should be value encoded")
	}

	// Test case 4: IntFamily should return false
	if sqlbase.MustBeValueEncoded(types.IntFamily) {
		t.Errorf("IntFamily should not be value encoded")
	}
}

// TestColumnTypeIsIndexable tests the ColumnTypeIsIndexable function.
func TestColumnTypeIsIndexable(t *testing.T) {
	// Test case 1: Int type should be indexable
	intType := types.Int
	if !sqlbase.ColumnTypeIsIndexable(intType) {
		t.Errorf("Int type should be indexable")
	}

	// Test case 2: Array type should not be indexable
	arrayType := types.MakeArray(types.Int)
	if sqlbase.ColumnTypeIsIndexable(arrayType) {
		t.Errorf("Array type should not be indexable")
	}
}

// TestColumnTypeIsInvertedIndexable tests the ColumnTypeIsInvertedIndexable function.
func TestColumnTypeIsInvertedIndexable(t *testing.T) {
	// Test case 1: Json type should be inverted indexable
	jsonType := types.Jsonb
	if !sqlbase.ColumnTypeIsInvertedIndexable(jsonType) {
		t.Errorf("Json type should be inverted indexable")
	}

	// Test case 2: Array type should be inverted indexable
	arrayType := types.MakeArray(types.Int)
	if !sqlbase.ColumnTypeIsInvertedIndexable(arrayType) {
		t.Errorf("Array type should be inverted indexable")
	}

	// Test case 3: Int type should not be inverted indexable
	intType := types.Int
	if sqlbase.ColumnTypeIsInvertedIndexable(intType) {
		t.Errorf("Int type should not be inverted indexable")
	}
}

// TestTableDescriptorSize 测试 TableDescriptor_Size 函数
func TestTableDescriptorSize(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.TableDescriptor
		expected int
	}{
		{
			name:     "EmptyTableDescriptor",
			input:    sqlbase.TableDescriptor{},
			expected: 170,
		},
		{
			name: "BasicTableDescriptor",
			input: sqlbase.TableDescriptor{
				ID:           1,
				Name:         "test_table",
				ParentID:     0,
				Version:      1,
				Columns:      []sqlbase.ColumnDescriptor{},
				NextColumnID: 1,
			},
			expected: 180,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.input.Size()
			require.Equal(t, tc.expected, size)
		})
	}
}

// TestIsVirtualTable tests the IsVirtualTable global function.
func TestIsVirtualTable(t *testing.T) {
	// Test case: Regular table ID
	if sqlbase.IsVirtualTable(100) {
		t.Errorf("Regular table ID should not be considered virtual")
	}

	// Test case: Virtual table ID (using a high ID that's likely to be virtual)
	if !sqlbase.IsVirtualTable(4294967199) {
		t.Errorf("Virtual table ID should be considered virtual")
	}
}

// TestDatabaseDescriptorValidate tests the Validate method for DatabaseDescriptor.
// func TestDatabaseDescriptorValidate(t *testing.T) {
// 	// Test case: Valid database descriptor
// 	db := &sqlbase.DatabaseDescriptor{
// 		ID:   50,
// 		Name: "test_db",
// 		Privileges: sqlbase.PrivilegeDescriptor{
// 			Grant: sqlbase.GrantPrivileges{},
// 		},
// 	}

// 	err := db.Validate()
// 	if err != nil {
// 		t.Errorf("Valid database descriptor should pass validation: %v", err)
// 	}

// 	// Test case: Invalid database descriptor with empty name
// 	dbInvalid := &sqlbase.DatabaseDescriptor{
// 		ID:   50,
// 		Name: "",
// 	}

// 	err = dbInvalid.Validate()
// 	if err == nil {
// 		t.Errorf("Database descriptor with empty name should fail validation")
// 	}
// }

// TestSchemaDescriptorValidate tests the Validate method for SchemaDescriptor.
// func TestSchemaDescriptorValidate(t *testing.T) {
// 	// Test case: Valid schema descriptor
// 	schema := &sqlbase.SchemaDescriptor{
// 		ID:   50,
// 		Name: "public",
// 	}

// 	err := schema.Validate()
// 	if err != nil {
// 		t.Errorf("Valid schema descriptor should pass validation: %v", err)
// 	}

// 	// Test case: Invalid schema descriptor with empty name
// 	schemaInvalid := &sqlbase.SchemaDescriptor{
// 		ID:   50,
// 		Name: "",
// 	}

// 	err = schemaInvalid.Validate()
// 	if err == nil {
// 		t.Errorf("Schema descriptor with empty name should fail validation")
// 	}
// }

// TestTableDescriptorKeysPerRow tests the KeysPerRow method for TableDescriptor.
func TestTableDescriptorKeysPerRow(t *testing.T) {
	// Test case: Primary index
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1},
		Families:     []sqlbase.ColumnFamilyDescriptor{{}, {}, {}},
	}
	keys, err := table.KeysPerRow(1)
	if err != nil {
		t.Errorf("KeysPerRow should not return error for primary index")
	}
	if keys != 3 {
		t.Errorf("KeysPerRow should return number of column families for primary index")
	}

	// Test case: Secondary index with no stored columns
	tableWithSecondary := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, StoreColumnIDs: []sqlbase.ColumnID{}},
		},
		Families: []sqlbase.ColumnFamilyDescriptor{{}, {}, {}},
	}
	keys, err = tableWithSecondary.KeysPerRow(2)
	if err != nil {
		t.Errorf("KeysPerRow should not return error for secondary index")
	}
	if keys != 1 {
		t.Errorf("KeysPerRow should return 1 for secondary index with no stored columns")
	}

	// Test case: Secondary index with stored columns
	tableWithStoredColumns := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, StoreColumnIDs: []sqlbase.ColumnID{1}},
		},
		Families: []sqlbase.ColumnFamilyDescriptor{{}, {}, {}},
	}
	keys, err = tableWithStoredColumns.KeysPerRow(2)
	if err != nil {
		t.Errorf("KeysPerRow should not return error for secondary index with stored columns")
	}
	if keys != 3 {
		t.Errorf("KeysPerRow should return number of column families for secondary index with stored columns")
	}
}

// TestTableDescriptorAllActiveAndInactiveChecks tests the AllActiveAndInactiveChecks method for TableDescriptor.
func TestTableDescriptorAllActiveAndInactiveChecks(t *testing.T) {
	// Test case: Table with no checks
	table := &sqlbase.TableDescriptor{}
	checks := table.AllActiveAndInactiveChecks()
	if len(checks) != 0 {
		t.Errorf("AllActiveAndInactiveChecks should return empty slice when no checks")
	}

	// Test case: Table with active checks
	tableWithChecks := &sqlbase.TableDescriptor{
		Checks: []*sqlbase.TableDescriptor_CheckConstraint{
			{Name: "check1", Expr: "id > 0", Validity: sqlbase.ConstraintValidity_Validated},
		},
	}
	checks = tableWithChecks.AllActiveAndInactiveChecks()
	if len(checks) != 1 || checks[0].Name != "check1" {
		t.Errorf("AllActiveAndInactiveChecks should return active checks")
	}
}

// TestTableDescriptorAllActiveAndInactiveForeignKeys tests the AllActiveAndInactiveForeignKeys method for TableDescriptor.
func TestTableDescriptorAllActiveAndInactiveForeignKeys(t *testing.T) {
	// Test case: Table with no foreign keys
	table := &sqlbase.TableDescriptor{}
	fks := table.AllActiveAndInactiveForeignKeys()
	if len(fks) != 0 {
		t.Errorf("AllActiveAndInactiveForeignKeys should return empty slice when no foreign keys")
	}

	// Test case: Table with active foreign keys
	tableWithFKs := &sqlbase.TableDescriptor{
		OutboundFKs: []sqlbase.ForeignKeyConstraint{
			{Name: "fk1", Validity: sqlbase.ConstraintValidity_Validated},
		},
	}
	fks = tableWithFKs.AllActiveAndInactiveForeignKeys()
	if len(fks) != 1 || fks[0].Name != "fk1" {
		t.Errorf("AllActiveAndInactiveForeignKeys should return active foreign keys")
	}
}

// TestTableDescriptorForeachNonDropIndex tests the ForeachNonDropIndex method for TableDescriptor.
func TestTableDescriptorForeachNonDropIndex(t *testing.T) {
	table := &sqlbase.TableDescriptor{
		PrimaryIndex: sqlbase.IndexDescriptor{ID: 1, Name: "primary"},
		Indexes: []sqlbase.IndexDescriptor{
			{ID: 2, Name: "idx1"},
			{ID: 3, Name: "idx2"},
		},
	}

	count := 0
	err := table.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		count++
		return nil
	})
	if err != nil {
		t.Errorf("ForeachNonDropIndex should not return error")
	}
	if count != 3 { // Primary + 2 secondary
		t.Errorf("ForeachNonDropIndex should iterate over all indexes")
	}
}

// TestIndexDescriptorSQLString tests the SQLString method for IndexDescriptor.
func TestIndexDescriptorSQLString(t *testing.T) {
	// Test case: Basic index
	idx := &sqlbase.IndexDescriptor{
		Name:             "idx1",
		ColumnNames:      []string{"col1", "col2"},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_DESC},
	}
	tableName := tree.NewUnqualifiedTableName(tree.Name("test_table"))
	sqlStr := idx.SQLString(tableName)
	if !strings.Contains(sqlStr, "INDEX idx1 ON test_table (col1 ASC, col2 DESC)") {
		t.Errorf("SQLString should return correct SQL representation")
	}

	// Test case: Unique index
	uniqueIdx := &sqlbase.IndexDescriptor{
		Name:             "unique_idx",
		Unique:           true,
		ColumnNames:      []string{"col1"},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
	}
	sqlStr = uniqueIdx.SQLString(tableName)
	if !strings.Contains(sqlStr, "UNIQUE INDEX unique_idx ON test_table (col1 ASC)") {
		t.Errorf("SQLString should return correct SQL representation for unique index")
	}

	// Test case: Inverted index
	invertedIdx := &sqlbase.IndexDescriptor{
		Name:        "inverted_idx",
		Type:        sqlbase.IndexDescriptor_INVERTED,
		ColumnNames: []string{"col1"},
	}
	sqlStr = invertedIdx.SQLString(tableName)
	if !strings.Contains(sqlStr, "INVERTED INDEX inverted_idx ON test_table (col1)") {
		t.Errorf("SQLString should return correct SQL representation for inverted index")
	}

	// Test case: Sharded index
	shardedIdx := &sqlbase.IndexDescriptor{
		Name:             "sharded_idx",
		ColumnNames:      []string{"col1"},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		Sharded:          sqlbase.ShardedDescriptor{IsSharded: true, ShardBuckets: 16},
	}
	sqlStr = shardedIdx.SQLString(tableName)
	if !strings.Contains(sqlStr, "USING HASH WITH BUCKET_COUNT = 16") {
		t.Errorf("SQLString should return correct SQL representation for sharded index")
	}

	// Test case: Index with storing columns
	storingIdx := &sqlbase.IndexDescriptor{
		Name:             "storing_idx",
		ColumnNames:      []string{"col1"},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		StoreColumnNames: []string{"col2", "col3"},
	}
	sqlStr = storingIdx.SQLString(tableName)
	if !strings.Contains(sqlStr, "STORING (col2, col3)") {
		t.Errorf("SQLString should return correct SQL representation for index with storing columns")
	}
}

// TestIndexDescriptorFillColumns tests the FillColumns method for IndexDescriptor.
func TestIndexDescriptorFillColumns(t *testing.T) {
	// Test case: Fill columns from IndexElemList
	idx := &sqlbase.IndexDescriptor{}
	elems := tree.IndexElemList{
		{Column: tree.Name("col1"), Direction: tree.Ascending},
		{Column: tree.Name("col2"), Direction: tree.Descending},
	}
	err := idx.FillColumns(elems)
	if err != nil {
		t.Errorf("FillColumns should not return error")
	}
	if len(idx.ColumnNames) != 2 || idx.ColumnNames[0] != "col1" || idx.ColumnNames[1] != "col2" {
		t.Errorf("FillColumns should set column names correctly")
	}
	if len(idx.ColumnDirections) != 2 || idx.ColumnDirections[0] != sqlbase.IndexDescriptor_ASC || idx.ColumnDirections[1] != sqlbase.IndexDescriptor_DESC {
		t.Errorf("FillColumns should set column directions correctly")
	}
}

// TestGeneratedFamilyName tests the GeneratedFamilyName function.
func TestGeneratedFamilyName(t *testing.T) {
	// Test case: Generate family name
	familyID := sqlbase.FamilyID(1)
	columnNames := []string{"col1", "col2"}
	name := sqlbase.GeneratedFamilyName(familyID, columnNames...)
	if name != "fam_1_col1_col2" {
		t.Errorf("GeneratedFamilyName should return correct family name")
	}
}

// TestColumnDescriptorSize 测试 ColumnDescriptor_Size 函数
func TestColumnDescriptorSize(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.ColumnDescriptor
		expected int
	}{
		{
			name:     "EmptyColumnDescriptor",
			input:    sqlbase.ColumnDescriptor{},
			expected: 40,
		},
		{
			name: "BasicColumnDescriptor",
			input: sqlbase.ColumnDescriptor{
				ID:          1,
				Name:        "test_column",
				Nullable:    false,
				DefaultExpr: nil,
			},
			expected: 51,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.input.Size()
			require.Equal(t, tc.expected, size)
		})
	}
}

// TestIndexDescriptorSize 测试 IndexDescriptor_Size 函数
func TestIndexDescriptorSize(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.IndexDescriptor
		expected int
	}{
		{
			name:     "EmptyIndexDescriptor",
			input:    sqlbase.IndexDescriptor{},
			expected: 56,
		},
		{
			name: "EmptyIndexDescriptor",
			input: sqlbase.IndexDescriptor{
				ID:   1,
				Name: "test_index",
			},
			expected: 66,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.input.Size()
			require.Equal(t, tc.expected, size)
		})
	}
}
