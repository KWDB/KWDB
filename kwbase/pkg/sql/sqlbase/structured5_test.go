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
)

func TestKWDBHAInfoRAI2(t *testing.T) {
	obj1 := &sqlbase.KWDBHAInfo{}
	obj2 := &sqlbase.KWDBHAInfo{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestKWDBNodeInfoRAI2(t *testing.T) {
	obj1 := &sqlbase.KWDBNodeInfo{}
	obj2 := &sqlbase.KWDBNodeInfo{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestUserPrivilegesRAI2(t *testing.T) {
	obj1 := &sqlbase.UserPrivileges{}
	obj2 := &sqlbase.UserPrivileges{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPrivilegeDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.PrivilegeDescriptor{}
	obj2 := &sqlbase.PrivilegeDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestForeignKeyReferenceRAI2(t *testing.T) {
	obj1 := &sqlbase.ForeignKeyReference{}
	obj2 := &sqlbase.ForeignKeyReference{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestForeignKeyConstraintRAI2(t *testing.T) {
	obj1 := &sqlbase.ForeignKeyConstraint{}
	obj2 := &sqlbase.ForeignKeyConstraint{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestColumnDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.ColumnDescriptor{}
	obj2 := &sqlbase.ColumnDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTSColRAI2(t *testing.T) {
	obj1 := &sqlbase.TSCol{}
	obj2 := &sqlbase.TSCol{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestColumnFamilyDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.ColumnFamilyDescriptor{}
	obj2 := &sqlbase.ColumnFamilyDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestInterleaveDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.InterleaveDescriptor{}
	obj2 := &sqlbase.InterleaveDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestInterleaveDescriptor_AncestorRAI2(t *testing.T) {
	obj1 := &sqlbase.InterleaveDescriptor_Ancestor{}
	obj2 := &sqlbase.InterleaveDescriptor_Ancestor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestShardedDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.ShardedDescriptor{}
	obj2 := &sqlbase.ShardedDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPartitioningDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor{}
	obj2 := &sqlbase.PartitioningDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPartitioningDescriptor_ListRAI2(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_List{}
	obj2 := &sqlbase.PartitioningDescriptor_List{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPartitioningDescriptor_RangeRAI2(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_Range{}
	obj2 := &sqlbase.PartitioningDescriptor_Range{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPartitioningDescriptor_HashPointRAI2(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_HashPoint{}
	obj2 := &sqlbase.PartitioningDescriptor_HashPoint{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestIndexDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.IndexDescriptor{}
	obj2 := &sqlbase.IndexDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestConstraintToUpdateRAI2(t *testing.T) {
	obj1 := &sqlbase.ConstraintToUpdate{}
	obj2 := &sqlbase.ConstraintToUpdate{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestPrimaryKeySwapRAI2(t *testing.T) {
	obj1 := &sqlbase.PrimaryKeySwap{}
	obj2 := &sqlbase.PrimaryKeySwap{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestMaterializedViewRefreshRAI2(t *testing.T) {
	obj1 := &sqlbase.MaterializedViewRefresh{}
	obj2 := &sqlbase.MaterializedViewRefresh{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutationRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation{}
	obj2 := &sqlbase.DescriptorMutation{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutation_ColumnRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation_Column{}
	obj2 := &sqlbase.DescriptorMutation_Column{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutation_IndexRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation_Index{}
	obj2 := &sqlbase.DescriptorMutation_Index{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutation_ConstraintRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation_Constraint{}
	obj2 := &sqlbase.DescriptorMutation_Constraint{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutation_PrimaryKeySwapRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation_PrimaryKeySwap{}
	obj2 := &sqlbase.DescriptorMutation_PrimaryKeySwap{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorMutation_MaterializedViewRefreshRAI2(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation_MaterializedViewRefresh{}
	obj2 := &sqlbase.DescriptorMutation_MaterializedViewRefresh{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestFunctionDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.FunctionDescriptor{}
	obj2 := &sqlbase.FunctionDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestProcedureDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.ProcedureDescriptor{}
	obj2 := &sqlbase.ProcedureDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestProcParamRAI2(t *testing.T) {
	obj1 := &sqlbase.ProcParam{}
	obj2 := &sqlbase.ProcParam{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor{}
	obj2 := &sqlbase.TableDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_SchemaChangeLeaseRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SchemaChangeLease{}
	obj2 := &sqlbase.TableDescriptor_SchemaChangeLease{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_CheckConstraintRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_CheckConstraint{}
	obj2 := &sqlbase.TableDescriptor_CheckConstraint{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_NameInfoRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_NameInfo{}
	obj2 := &sqlbase.TableDescriptor_NameInfo{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_ReferenceRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_Reference{}
	obj2 := &sqlbase.TableDescriptor_Reference{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_MutationJobRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_MutationJob{}
	obj2 := &sqlbase.TableDescriptor_MutationJob{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_SequenceOptsRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SequenceOpts{}
	obj2 := &sqlbase.TableDescriptor_SequenceOpts{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_SequenceOpts_SequenceOwnerRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}
	obj2 := &sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_ReplacementRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_Replacement{}
	obj2 := &sqlbase.TableDescriptor_Replacement{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTableDescriptor_GCDescriptorMutationRAI2(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_GCDescriptorMutation{}
	obj2 := &sqlbase.TableDescriptor_GCDescriptorMutation{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTriggerDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.TriggerDescriptor{}
	obj2 := &sqlbase.TriggerDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestCDCDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.CDCDescriptor{}
	obj2 := &sqlbase.CDCDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestKWDBTSColumnRAI2(t *testing.T) {
	obj1 := &sqlbase.KWDBTSColumn{}
	obj2 := &sqlbase.KWDBTSColumn{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTSTableRAI2(t *testing.T) {
	obj1 := &sqlbase.TSTable{}
	obj2 := &sqlbase.TSTable{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestSchemaDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.SchemaDescriptor{}
	obj2 := &sqlbase.SchemaDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestSchemaDescriptor_NameInfoRAI2(t *testing.T) {
	obj1 := &sqlbase.SchemaDescriptor_NameInfo{}
	obj2 := &sqlbase.SchemaDescriptor_NameInfo{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDatabaseDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.DatabaseDescriptor{}
	obj2 := &sqlbase.DatabaseDescriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestTSDBRAI2(t *testing.T) {
	obj1 := &sqlbase.TSDB{}
	obj2 := &sqlbase.TSDB{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptorRAI2(t *testing.T) {
	obj1 := &sqlbase.Descriptor{}
	obj2 := &sqlbase.Descriptor{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptor_TableRAI2(t *testing.T) {
	obj1 := &sqlbase.Descriptor_Table{}
	obj2 := &sqlbase.Descriptor_Table{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptor_DatabaseRAI2(t *testing.T) {
	obj1 := &sqlbase.Descriptor_Database{}
	obj2 := &sqlbase.Descriptor_Database{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}

func TestDescriptor_SchemaRAI2(t *testing.T) {
	obj1 := &sqlbase.Descriptor_Schema{}
	obj2 := &sqlbase.Descriptor_Schema{}
	_ = obj1.Equal(obj2)
	_ = obj1.Equal(nil)
}
