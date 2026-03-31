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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

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

// TestTableDescriptorMarshal 测试 TableDescriptor_Marshal 函数
func TestTableDescriptorMarshal(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.TableDescriptor
		expected []byte
	}{
		{
			name:     "EmptyTableDescriptorMarshal",
			input:    sqlbase.TableDescriptor{},
			expected: []byte{0xa, 0x0, 0x18, 0x0, 0x20, 0x0, 0x28, 0x0, 0x3a, 0x0, 0x48, 0x0, 0x52, 0x38, 0xa, 0x0, 0x10, 0x0, 0x18, 0x0, 0x4a, 0x10, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0, 0x40, 0x0, 0x5a, 0x0, 0x7a, 0x4, 0x8, 0x0, 0x28, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xa2, 0x1, 0x6, 0x8, 0x0, 0x12, 0x0, 0x18, 0x0, 0xa8, 0x1, 0x0, 0x60, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x98, 0x1, 0x0, 0xb8, 0x1, 0x0, 0xc2, 0x1, 0x0, 0xe8, 0x1, 0x0, 0xf2, 0x1, 0x4, 0x8, 0x0, 0x12, 0x0, 0xf8, 0x1, 0x0, 0x80, 0x2, 0x0, 0x92, 0x2, 0x0, 0x9a, 0x2, 0x0, 0xb2, 0x2, 0x0, 0xb8, 0x2, 0x0, 0xc0, 0x2, 0x0, 0xca, 0x2, 0x0, 0xd0, 0x2, 0x0, 0xd8, 0x2, 0x0, 0xe2, 0x2, 0x19, 0x8, 0x0, 0x10, 0x0, 0x20, 0x0, 0x28, 0x0, 0x5a, 0x0, 0x60, 0x0, 0x68, 0x0, 0x78, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xea, 0x2, 0x0, 0xf2, 0x2, 0x0, 0xf8, 0x2, 0x0, 0x80, 0x3, 0x0, 0x98, 0x3, 0x0},
		},
		{
			name: "BasicTableDescriptorMarshal",
			input: sqlbase.TableDescriptor{
				ID:           1,
				Name:         "test_table",
				ParentID:     0,
				Version:      1,
				Columns:      []sqlbase.ColumnDescriptor{},
				NextColumnID: 1,
			},
			expected: []byte{0xa, 0xa, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x1, 0x20, 0x0, 0x28, 0x1, 0x3a, 0x0, 0x48, 0x1, 0x52, 0x38, 0xa, 0x0, 0x10, 0x0, 0x18, 0x0, 0x4a, 0x10, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0, 0x40, 0x0, 0x5a, 0x0, 0x7a, 0x4, 0x8, 0x0, 0x28, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xa2, 0x1, 0x6, 0x8, 0x0, 0x12, 0x0, 0x18, 0x0, 0xa8, 0x1, 0x0, 0x60, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x98, 0x1, 0x0, 0xb8, 0x1, 0x0, 0xc2, 0x1, 0x0, 0xe8, 0x1, 0x0, 0xf2, 0x1, 0x4, 0x8, 0x0, 0x12, 0x0, 0xf8, 0x1, 0x0, 0x80, 0x2, 0x0, 0x92, 0x2, 0x0, 0x9a, 0x2, 0x0, 0xb2, 0x2, 0x0, 0xb8, 0x2, 0x0, 0xc0, 0x2, 0x0, 0xca, 0x2, 0x0, 0xd0, 0x2, 0x0, 0xd8, 0x2, 0x0, 0xe2, 0x2, 0x19, 0x8, 0x0, 0x10, 0x0, 0x20, 0x0, 0x28, 0x0, 0x5a, 0x0, 0x60, 0x0, 0x68, 0x0, 0x78, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xea, 0x2, 0x0, 0xf2, 0x2, 0x0, 0xf8, 0x2, 0x0, 0x80, 0x3, 0x0, 0x98, 0x3, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.input.Marshal()
			require.NoError(t, err)
			require.Equal(t, tc.expected, data)
		})
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

// TestColumnDescriptorMarshal 测试 ColumnDescriptor_Marshal 函数
func TestColumnDescriptorMarshal(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.ColumnDescriptor
		expected []byte
	}{
		{
			name:     "EmptyColumnDescriptorMarshal",
			input:    sqlbase.ColumnDescriptor{},
			expected: []byte{0xa, 0x0, 0x10, 0x0, 0x1a, 0xe, 0x8, 0x0, 0x10, 0x0, 0x18, 0x0, 0x30, 0x0, 0x50, 0x0, 0x60, 0x0, 0x70, 0x0, 0x20, 0x0, 0x30, 0x0, 0x6a, 0xe, 0x8, 0x0, 0x10, 0x0, 0x18, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0},
		},
		{
			name: "BasicColumnDescriptorMarshal",
			input: sqlbase.ColumnDescriptor{
				ID:          1,
				Name:        "test_column",
				Nullable:    false,
				DefaultExpr: nil,
			},
			expected: []byte{0xa, 0xb, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x10, 0x1, 0x1a, 0xe, 0x8, 0x0, 0x10, 0x0, 0x18, 0x0, 0x30, 0x0, 0x50, 0x0, 0x60, 0x0, 0x70, 0x0, 0x20, 0x0, 0x30, 0x0, 0x6a, 0xe, 0x8, 0x0, 0x10, 0x0, 0x18, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.input.Marshal()
			require.NoError(t, err)
			require.Equal(t, tc.expected, data)
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

// TestIndexDescriptorMarshal 测试 IndexDescriptor_Marshal 函数
func TestIndexDescriptorMarshal(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.IndexDescriptor
		expected []byte
	}{
		{
			name:     "EmptyIndexDescriptorMarshal",
			input:    sqlbase.IndexDescriptor{},
			expected: []byte{0xa, 0x0, 0x10, 0x0, 0x18, 0x0, 0x4a, 0x10, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0, 0x40, 0x0, 0x5a, 0x0, 0x7a, 0x4, 0x8, 0x0, 0x28, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xa2, 0x1, 0x6, 0x8, 0x0, 0x12, 0x0, 0x18, 0x0, 0xa8, 0x1, 0x0},
		},
		{
			name: "BasicIndexDescriptorMarshal",
			input: sqlbase.IndexDescriptor{
				ID:   1,
				Name: "test_index",
			},
			expected: []byte{0xa, 0xa, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x10, 0x1, 0x18, 0x0, 0x4a, 0x10, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x20, 0x0, 0x28, 0x0, 0x30, 0x0, 0x38, 0x0, 0x40, 0x0, 0x5a, 0x0, 0x7a, 0x4, 0x8, 0x0, 0x28, 0x0, 0x80, 0x1, 0x0, 0x88, 0x1, 0x0, 0x90, 0x1, 0x0, 0x98, 0x1, 0x0, 0xa2, 0x1, 0x6, 0x8, 0x0, 0x12, 0x0, 0x18, 0x0, 0xa8, 0x1, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.input.Marshal()
			require.NoError(t, err)
			require.Equal(t, tc.expected, data)
		})
	}
}

// TestDatabaseDescriptorSize 测试 DatabaseDescriptor_Size 函数
func TestDatabaseDescriptorSize(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.DatabaseDescriptor
		expected int
	}{
		{
			name:     "EmptyDatabaseDescriptor",
			input:    sqlbase.DatabaseDescriptor{},
			expected: 20,
		},
		{
			name: "BasicDatabaseDescriptor",
			input: sqlbase.DatabaseDescriptor{
				ID:   1,
				Name: "test_database",
			},
			expected: 33,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.input.Size()
			require.Equal(t, tc.expected, size)
		})
	}
}

// TestDatabaseDescriptorMarshal 测试 DatabaseDescriptor_Marshal 函数
func TestDatabaseDescriptorMarshal(t *testing.T) {
	testCases := []struct {
		name     string
		input    sqlbase.DatabaseDescriptor
		expected []byte
	}{
		{
			name:     "EmptyDatabaseDescriptorMarshal",
			input:    sqlbase.DatabaseDescriptor{},
			expected: []byte{0xa, 0x0, 0x10, 0x0, 0x22, 0x0, 0x28, 0x0, 0x32, 0xa, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x22, 0x0, 0x28, 0x0},
		},
		{
			name: "BasicDatabaseDescriptorMarshal",
			input: sqlbase.DatabaseDescriptor{
				ID:   1,
				Name: "test_database",
			},
			expected: []byte{0xa, 0xd, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x10, 0x1, 0x22, 0x0, 0x28, 0x0, 0x32, 0xa, 0x8, 0x0, 0x10, 0x0, 0x1a, 0x0, 0x22, 0x0, 0x28, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.input.Marshal()
			require.NoError(t, err)
			require.Equal(t, tc.expected, data)
		})
	}
}
