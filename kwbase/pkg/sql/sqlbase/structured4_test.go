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


func TestDatumEncodingRAI(t *testing.T) {
    obj1 := sqlbase.DatumEncoding(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestJoinTypeRAI(t *testing.T) {
    obj1 := sqlbase.JoinType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestScanLockingStrengthRAI(t *testing.T) {
    obj1 := sqlbase.ScanLockingStrength(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestScanLockingWaitPolicyRAI(t *testing.T) {
    obj1 := sqlbase.ScanLockingWaitPolicy(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestReplicationTypeRAI(t *testing.T) {
    obj1 := sqlbase.ReplicationType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestAgentTypeRAI(t *testing.T) {
    obj1 := sqlbase.AgentType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestDataTypeRAI(t *testing.T) {
    obj1 := sqlbase.DataType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestVariableLengthTypeRAI(t *testing.T) {
    obj1 := sqlbase.VariableLengthType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestKWDBReplicationStatusRAI(t *testing.T) {
    obj1 := sqlbase.KWDBReplicationStatus(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestKWDBConnectionStatusRAI(t *testing.T) {
    obj1 := sqlbase.KWDBConnectionStatus(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestKWDBReplicationLevelRAI(t *testing.T) {
    obj1 := sqlbase.KWDBReplicationLevel(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestColumnTypeRAI(t *testing.T) {
    obj1 := sqlbase.ColumnType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestKWDBHAStatusRAI(t *testing.T) {
    obj1 := sqlbase.KWDBHAStatus(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestConstraintValidityRAI(t *testing.T) {
    obj1 := sqlbase.ConstraintValidity(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTriggerActionTimeRAI(t *testing.T) {
    obj1 := sqlbase.TriggerActionTime(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTriggerEventRAI(t *testing.T) {
    obj1 := sqlbase.TriggerEvent(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestCDCInstanceTypeRAI(t *testing.T) {
    obj1 := sqlbase.CDCInstanceType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestForeignKeyReference_ActionRAI(t *testing.T) {
    obj1 := sqlbase.ForeignKeyReference_Action(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestForeignKeyReference_MatchRAI(t *testing.T) {
    obj1 := sqlbase.ForeignKeyReference_Match(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestIndexDescriptor_DirectionRAI(t *testing.T) {
    obj1 := sqlbase.IndexDescriptor_Direction(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestIndexDescriptor_TypeRAI(t *testing.T) {
    obj1 := sqlbase.IndexDescriptor_Type(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestConstraintToUpdate_ConstraintTypeRAI(t *testing.T) {
    obj1 := sqlbase.ConstraintToUpdate_ConstraintType(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestDescriptorMutation_StateRAI(t *testing.T) {
    obj1 := sqlbase.DescriptorMutation_State(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestDescriptorMutation_DirectionRAI(t *testing.T) {
    obj1 := sqlbase.DescriptorMutation_Direction(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestProcParam_ParamDirectionRAI(t *testing.T) {
    obj1 := sqlbase.ProcParam_ParamDirection(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTableDescriptor_StateRAI(t *testing.T) {
    obj1 := sqlbase.TableDescriptor_State(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTableDescriptor_AuditModeRAI(t *testing.T) {
    obj1 := sqlbase.TableDescriptor_AuditMode(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTableDescriptor_LockSateRAI(t *testing.T) {
    obj1 := sqlbase.TableDescriptor_LockSate(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestSchemaDescriptor_StateRAI(t *testing.T) {
    obj1 := sqlbase.SchemaDescriptor_State(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestChildDesc_StateRAI(t *testing.T) {
    obj1 := sqlbase.ChildDesc_State(0)
    _ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

