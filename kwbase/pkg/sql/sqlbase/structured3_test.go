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

func TestDeleteMeMsgRAI(t *testing.T) {
	obj1 := &sqlbase.DeleteMeMsg{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.DeleteMeMsg{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBHAInfoRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBHAInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBHAInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPreRelationRAI(t *testing.T) {
	obj1 := &sqlbase.PreRelation{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PreRelation{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCreateCTableRAI(t *testing.T) {
	obj1 := &sqlbase.CreateCTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.CreateCTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBCTableRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBCTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBCTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBTsTableRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBTsTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBTsTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBKTSColumnRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBKTSColumn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBKTSColumn{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestNTagIndexInfoRAI(t *testing.T) {
	obj1 := &sqlbase.NTagIndexInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.NTagIndexInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCreateTsTableRAI(t *testing.T) {
	obj1 := &sqlbase.CreateTsTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.CreateTsTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestBlockInfoRAI(t *testing.T) {
	obj1 := &sqlbase.BlockInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.BlockInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestBlocksDistributionRAI(t *testing.T) {
	obj1 := &sqlbase.BlocksDistribution{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.BlocksDistribution{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestUpdateReplicationMetaDataRAI(t *testing.T) {
	obj1 := &sqlbase.UpdateReplicationMetaData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.UpdateReplicationMetaData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBNodeInfoRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBNodeInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBNodeInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBReplicationMetaDataRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBReplicationMetaData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBReplicationMetaData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBReplicationSyncRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBReplicationSync{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBReplicationSync{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBReplicationAgentMetaDataRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBReplicationAgentMetaData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBReplicationAgentMetaData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBReplicationProgressRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBReplicationProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBReplicationProgress{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBReplicationProgressSetRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBReplicationProgressSet{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBReplicationProgressSet{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationServiceCallerFuncInputsRAI(t *testing.T) {
	obj1 := &sqlbase.ReplicationServiceCallerFuncInputs{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ReplicationServiceCallerFuncInputs{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWhiteListRAI(t *testing.T) {
	obj1 := &sqlbase.WhiteList{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.WhiteList{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSInsertSelectRAI(t *testing.T) {
	obj1 := &sqlbase.TSInsertSelect{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TSInsertSelect{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestUserPrivilegesRAI(t *testing.T) {
	obj1 := &sqlbase.UserPrivileges{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.UserPrivileges{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPrivilegeDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.PrivilegeDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PrivilegeDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestForeignKeyReferenceRAI(t *testing.T) {
	obj1 := &sqlbase.ForeignKeyReference{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ForeignKeyReference{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestForeignKeyConstraintRAI(t *testing.T) {
	obj1 := &sqlbase.ForeignKeyConstraint{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ForeignKeyConstraint{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestColumnDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.ColumnDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ColumnDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSColRAI(t *testing.T) {
	obj1 := &sqlbase.TSCol{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TSCol{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestColumnFamilyDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.ColumnFamilyDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ColumnFamilyDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestInterleaveDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.InterleaveDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.InterleaveDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestInterleaveDescriptor_AncestorRAI(t *testing.T) {
	obj1 := &sqlbase.InterleaveDescriptor_Ancestor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.InterleaveDescriptor_Ancestor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestShardedDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.ShardedDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ShardedDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPartitioningDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PartitioningDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPartitioningDescriptor_ListRAI(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_List{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PartitioningDescriptor_List{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPartitioningDescriptor_RangeRAI(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_Range{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PartitioningDescriptor_Range{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPartitioningDescriptor_HashPointRAI(t *testing.T) {
	obj1 := &sqlbase.PartitioningDescriptor_HashPoint{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PartitioningDescriptor_HashPoint{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestIndexDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.IndexDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.IndexDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestConstraintToUpdateRAI(t *testing.T) {
	obj1 := &sqlbase.ConstraintToUpdate{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ConstraintToUpdate{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPrimaryKeySwapRAI(t *testing.T) {
	obj1 := &sqlbase.PrimaryKeySwap{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.PrimaryKeySwap{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestMaterializedViewRefreshRAI(t *testing.T) {
	obj1 := &sqlbase.MaterializedViewRefresh{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.MaterializedViewRefresh{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDescriptorMutationRAI(t *testing.T) {
	obj1 := &sqlbase.DescriptorMutation{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.DescriptorMutation{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestFunctionDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.FunctionDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.FunctionDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProcedureDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.ProcedureDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ProcedureDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProcParamRAI(t *testing.T) {
	obj1 := &sqlbase.ProcParam{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ProcParam{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_SchemaChangeLeaseRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SchemaChangeLease{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_SchemaChangeLease{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_CheckConstraintRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_CheckConstraint{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_CheckConstraint{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_NameInfoRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_NameInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_NameInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_ReferenceRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_Reference{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_Reference{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_MutationJobRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_MutationJob{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_MutationJob{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_SequenceOptsRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SequenceOpts{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_SequenceOpts{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_SequenceOpts_SequenceOwnerRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_ReplacementRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_Replacement{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_Replacement{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescriptor_GCDescriptorMutationRAI(t *testing.T) {
	obj1 := &sqlbase.TableDescriptor_GCDescriptorMutation{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TableDescriptor_GCDescriptorMutation{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTriggerDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.TriggerDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TriggerDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCDCDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.CDCDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.CDCDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBTSColumnRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBTSColumn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBTSColumn{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSTableRAI(t *testing.T) {
	obj1 := &sqlbase.TSTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TSTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSchemaDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.SchemaDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.SchemaDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSchemaDescriptor_NameInfoRAI(t *testing.T) {
	obj1 := &sqlbase.SchemaDescriptor_NameInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.SchemaDescriptor_NameInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDatabaseDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.DatabaseDescriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	// obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.DatabaseDescriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSDBRAI(t *testing.T) {
	obj1 := &sqlbase.TSDB{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.TSDB{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDescriptorRAI(t *testing.T) {
	obj1 := &sqlbase.Descriptor{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.Descriptor{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestImportTableRAI(t *testing.T) {
	obj1 := &sqlbase.ImportTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ImportTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBTagValueRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBTagValue{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBTagValue{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestChildDescRAI(t *testing.T) {
	obj1 := &sqlbase.ChildDesc{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.ChildDesc{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestKWDBScheOptionRAI(t *testing.T) {
	obj1 := &sqlbase.KWDBScheOption{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.KWDBScheOption{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSortedHistogramInfoRAI(t *testing.T) {
	obj1 := &sqlbase.SortedHistogramInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &sqlbase.SortedHistogramInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}
