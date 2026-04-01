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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// Test ReplicationType enum values
func TestReplicationType(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.ReplicationType
		expected string
	}{
		{"T_SYNC_TO_E", sqlbase.ReplicationType_T_SYNC_TO_E, "T_SYNC_TO_E"},
		{"E_SYNC_TO_E", sqlbase.ReplicationType_E_SYNC_TO_E, "E_SYNC_TO_E"},
		{"E_SYNC_TO_C", sqlbase.ReplicationType_E_SYNC_TO_C, "E_SYNC_TO_C"},
		{"C_SYNC_TO_C", sqlbase.ReplicationType_C_SYNC_TO_C, "C_SYNC_TO_C"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test AgentType enum values
func TestAgentType(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.AgentType
		expected string
	}{
		{"METADATA", sqlbase.AgentType_METADATA, "METADATA"},
		{"RDBMS", sqlbase.AgentType_RDBMS, "RDBMS"},
		{"TS", sqlbase.AgentType_TS, "TS"},
		{"ML", sqlbase.AgentType_ML, "ML"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test DataType enum values
func TestDataType(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.DataType
		expected string
	}{
		{"TIMESTAMP", sqlbase.DataType_TIMESTAMP, "TIMESTAMP"},
		{"SMALLINT", sqlbase.DataType_SMALLINT, "SMALLINT"},
		{"INT", sqlbase.DataType_INT, "INT"},
		{"BIGINT", sqlbase.DataType_BIGINT, "BIGINT"},
		{"FLOAT", sqlbase.DataType_FLOAT, "FLOAT"},
		{"DOUBLE", sqlbase.DataType_DOUBLE, "DOUBLE"},
		{"BOOL", sqlbase.DataType_BOOL, "BOOL"},
		{"CHAR", sqlbase.DataType_CHAR, "CHAR"},
		{"BYTES", sqlbase.DataType_BYTES, "BYTES"},
		{"NCHAR", sqlbase.DataType_NCHAR, "NCHAR"},
		{"VARCHAR", sqlbase.DataType_VARCHAR, "VARCHAR"},
		{"NVARCHAR", sqlbase.DataType_NVARCHAR, "NVARCHAR"},
		{"VARBYTES", sqlbase.DataType_VARBYTES, "VARBYTES"},
		{"SDECHAR", sqlbase.DataType_SDECHAR, "SDECHAR"},
		{"SDEVARCHAR", sqlbase.DataType_SDEVARCHAR, "SDEVARCHAR"},
		{"NULLVAL", sqlbase.DataType_NULLVAL, "NULLVAL"},
		{"UNKNOWN", sqlbase.DataType_UNKNOWN, "UNKNOWN"},
		{"DECIMAL", sqlbase.DataType_DECIMAL, "DECIMAL"},
		{"TIMESTAMPTZ", sqlbase.DataType_TIMESTAMPTZ, "TIMESTAMPTZ"},
		{"DATE", sqlbase.DataType_DATE, "DATE"},
		{"TIMESTAMP_MICRO", sqlbase.DataType_TIMESTAMP_MICRO, "TIMESTAMP_MICRO"},
		{"TIMESTAMP_NANO", sqlbase.DataType_TIMESTAMP_NANO, "TIMESTAMP_NANO"},
		{"TIMESTAMPTZ_MICRO", sqlbase.DataType_TIMESTAMPTZ_MICRO, "TIMESTAMPTZ_MICRO"},
		{"TIMESTAMPTZ_NANO", sqlbase.DataType_TIMESTAMPTZ_NANO, "TIMESTAMPTZ_NANO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test VariableLengthType enum values
func TestVariableLengthType(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.VariableLengthType
		expected string
	}{
		{"ColStorageTypeTuple", sqlbase.VariableLengthType_ColStorageTypeTuple, "ColStorageTypeTuple"},
		{"ColStorageTypeTPageEnd", sqlbase.VariableLengthType_ColStorageTypeTPageEnd, "ColStorageTypeTPageEnd"},
		{"ColStorageTypeTIndependentPage", sqlbase.VariableLengthType_ColStorageTypeTIndependentPage, "ColStorageTypeTIndependentPage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test KWDBReplicationStatus enum values
func TestKWDBReplicationStatus(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.KWDBReplicationStatus
		expected string
	}{
		{"UNINITIALIZED", sqlbase.KWDBReplicationStatus_UNINITIALIZED, "UNINITIALIZED"},
		{"RUNNING", sqlbase.KWDBReplicationStatus_RUNNING, "RUNNING"},
		{"STOPPED", sqlbase.KWDBReplicationStatus_STOPPED, "STOPPED"},
		{"STARTING", sqlbase.KWDBReplicationStatus_STARTING, "STARTING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test KWDBConnectionStatus enum values
func TestKWDBConnectionStatus(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.KWDBConnectionStatus
		expected string
	}{
		{"Dead", sqlbase.KWDBConnectionStatus_Dead, "Dead"},
		{"HEALTHY", sqlbase.KWDBConnectionStatus_HEALTHY, "HEALTHY"},
		{"UNHEALTHY", sqlbase.KWDBConnectionStatus_UNHEALTHY, "UNHEALTHY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test KWDBReplicationLevel enum values
func TestKWDBReplicationLevel(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.KWDBReplicationLevel
		expected string
	}{
		{"DATABASEREPL", sqlbase.KWDBReplicationLevel_DATABASEREPL, "DATABASEREPL"},
		{"TABLEREPL", sqlbase.KWDBReplicationLevel_TABLEREPL, "TABLEREPL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test ColumnType enum values
func TestColumnType(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.ColumnType
		expected string
	}{
		{"TYPE_DATA", sqlbase.ColumnType_TYPE_DATA, "TYPE_DATA"},
		{"TYPE_TAG", sqlbase.ColumnType_TYPE_TAG, "TYPE_TAG"},
		{"TYPE_PTAG", sqlbase.ColumnType_TYPE_PTAG, "TYPE_PTAG"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test KWDBHAStatus enum values
func TestKWDBHAStatus(t *testing.T) {
	tests := []struct {
		name     string
		enum     sqlbase.KWDBHAStatus
		expected string
	}{
		{"HASTOPPED", sqlbase.KWDBHAStatus_HASTOPPED, "HASTOPPED"},
		{"HARUNNING", sqlbase.KWDBHAStatus_HARUNNING, "HARUNNING"},
		{"HAPAUSED", sqlbase.KWDBHAStatus_HAPAUSED, "HAPAUSED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enum.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.enum.String())
			}

			// Test Enum() method
			enumPtr := tt.enum.Enum()
			if *enumPtr != tt.enum {
				t.Errorf("Enum() returned %v, expected %v", *enumPtr, tt.enum)
			}
		})
	}
}

// Test DeleteMeMsg message
func TestDeleteMeMsg(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
		TableID:      123,
		TemplateID:   456,
		TableName:    "test_table",
		StartTs:      1000,
		EndTs:        200,
		CompressTs:   1500,
		TsVersion:    1,
		IsTSTable:    true,
		IndexID:      789,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal DeleteMeMsg: %v", err)
	}

	unmarshaled := &sqlbase.DeleteMeMsg{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal DeleteMeMsg: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestDeleteMeMsgString(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
		TableID:      123,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestDeleteMeMsgProtoMessage(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{}
	msg.ProtoMessage()
}

func TestDeleteMeMsgDescriptor(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{}
	_, _ = msg.Descriptor()
}

func TestDeleteMeMsgMarshalTo(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
		TableID:      123,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestDeleteMeMsgXXXMerge(t *testing.T) {
	src := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
		TableID:      123,
	}
	dst := &sqlbase.DeleteMeMsg{}
	dst.XXX_Merge(src)
	if dst.DatabaseName != src.DatabaseName || dst.TableID != src.TableID {
		t.Errorf("XXX_Merge failed")
	}
}

func TestDeleteMeMsgSize(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestDeleteMeMsgReset(t *testing.T) {
	msg := &sqlbase.DeleteMeMsg{
		DatabaseName: "test_db",
	}
	msg.Reset()
	if msg.DatabaseName != "" || msg.TableID != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestKWDBHAInfoString(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{
		ClusterId:        "cluster1",
		Role:             1,
		HaStatus:         sqlbase.KWDBHAStatus_HARUNNING,
		ConnectionStatus: sqlbase.KWDBConnectionStatus_HEALTHY,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBHAInfoProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{}
	msg.ProtoMessage()
}

func TestKWDBHAInfoDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{}
	_, _ = msg.Descriptor()
}

func TestKWDBHAInfoMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{
		ClusterId: "cluster1",
		Role:      1,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBHAInfoXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBHAInfo{
		ClusterId: "cluster1",
		Role:      1,
	}
	dst := &sqlbase.KWDBHAInfo{}
	dst.XXX_Merge(src)
	if dst.ClusterId != src.ClusterId || dst.Role != src.Role {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBHAInfoSize(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{
		ClusterId: "cluster1",
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBHAInfoReset(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{
		ClusterId: "cluster1",
	}
	msg.Reset()
	if msg.ClusterId != "" {
		t.Error("Reset() should zero all fields")
	}
}

func TestPreRelationString(t *testing.T) {
	msg := &sqlbase.PreRelation{
		TableId: 123,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestPreRelationProtoMessage(t *testing.T) {
	msg := &sqlbase.PreRelation{}
	msg.ProtoMessage()
}

func TestPreRelationDescriptor(t *testing.T) {
	msg := &sqlbase.PreRelation{}
	_, _ = msg.Descriptor()
}

func TestPreRelationMarshalTo(t *testing.T) {
	msg := &sqlbase.PreRelation{
		TableId: 123,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestPreRelationXXXMerge(t *testing.T) {
	src := &sqlbase.PreRelation{
		TableId: 123,
	}
	dst := &sqlbase.PreRelation{}
	dst.XXX_Merge(src)
	if dst.TableId != src.TableId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestPreRelationSize(t *testing.T) {
	msg := &sqlbase.PreRelation{
		TableId: 1,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestPreRelationReset(t *testing.T) {
	msg := &sqlbase.PreRelation{
		TableId: 1,
	}
	msg.Reset()
	if msg.TableId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestCreateCTableString(t *testing.T) {
	msg := &sqlbase.CreateCTable{
		CTable: sqlbase.KWDBCTable{
			Id:   123,
			Name: "test_table",
		},
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestCreateCTableProtoMessage(t *testing.T) {
	msg := &sqlbase.CreateCTable{}
	msg.ProtoMessage()
}

func TestCreateCTableDescriptor(t *testing.T) {
	msg := &sqlbase.CreateCTable{}
	_, _ = msg.Descriptor()
}

func TestCreateCTableMarshalTo(t *testing.T) {
	msg := &sqlbase.CreateCTable{
		CTable: sqlbase.KWDBCTable{
			Id:   123,
			Name: "test_table",
		},
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestCreateCTableXXXMerge(t *testing.T) {
	src := &sqlbase.CreateCTable{
		CTable: sqlbase.KWDBCTable{
			Id:   123,
			Name: "test_table",
		},
	}
	dst := &sqlbase.CreateCTable{}
	dst.XXX_Merge(src)
	if dst.CTable.Id != src.CTable.Id {
		t.Errorf("XXX_Merge failed")
	}
}

func TestCreateCTableSize(t *testing.T) {
	msg := &sqlbase.CreateCTable{
		StableId: 4,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestCreateCTableReset(t *testing.T) {
	msg := &sqlbase.CreateCTable{
		StableId: 1,
	}
	msg.Reset()
	if msg.StableId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestKWDBCTableString(t *testing.T) {
	msg := &sqlbase.KWDBCTable{
		Id:   123,
		Name: "test_table",
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBCTableProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBCTable{}
	msg.ProtoMessage()
}

func TestKWDBCTableDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBCTable{}
	_, _ = msg.Descriptor()
}

func TestKWDBCTableMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBCTable{
		Id:   123,
		Name: "test_table",
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBCTableXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBCTable{
		Id:   123,
		Name: "test_table",
	}
	dst := &sqlbase.KWDBCTable{}
	dst.XXX_Merge(src)
	if dst.Id != src.Id || dst.Name != src.Name {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBCTableSize(t *testing.T) {
	msg := &sqlbase.KWDBCTable{
		Id: 1,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBCTableReset(t *testing.T) {
	msg := &sqlbase.KWDBCTable{
		Id: 1,
	}
	msg.Reset()
	if msg.Id != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBHAInfo message
func TestKWDBHAInfo(t *testing.T) {
	msg := &sqlbase.KWDBHAInfo{
		ClusterId:        "cluster1",
		Role:             1,
		HaStatus:         sqlbase.KWDBHAStatus_HARUNNING,
		ConnectionStatus: sqlbase.KWDBConnectionStatus_HEALTHY,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBHAInfo: %v", err)
	}

	unmarshaled := &sqlbase.KWDBHAInfo{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBHAInfo: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	if !reflect.DeepEqual(unmarshaled, msg) {
		t.Errorf("Equal failed: messages should be equal")
	}

	// Test with different values
	msg2 := &sqlbase.KWDBHAInfo{
		ClusterId:        "cluster2",
		Role:             2,
		HaStatus:         sqlbase.KWDBHAStatus_HASTOPPED,
		ConnectionStatus: sqlbase.KWDBConnectionStatus_UNHEALTHY,
	}

	if reflect.DeepEqual(msg, msg2) {
		t.Errorf("Equal failed: messages should not be equal")
	}
}

// Test KWDBNodeInfo message
func TestKWDBNodeInfo(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId:           "cluster1",
		InternalIp:          "192.168.1.1",
		InternalPort:        5432,
		AgentInternalPort:   []uint32{5555, 666},
		StorageInternalPort: 3306,
		NodeExternalIp:      "203.0.113.1",
		NodeExternalPort:    5433,
		AgentExternalPort:   []uint32{5556, 6667},
		StorageExternalPort: 3307,
		NodeName:            "node1",
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBNodeInfo: %v", err)
	}

	unmarshaled := &sqlbase.KWDBNodeInfo{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBNodeInfo: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	if !reflect.DeepEqual(unmarshaled, msg) {
		t.Errorf("Equal failed: messages should be equal")
	}
}
func TestKWDBNodeInfoString(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBNodeInfoProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{}
	msg.ProtoMessage()
}

func TestKWDBNodeInfoDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{}
	_, _ = msg.Descriptor()
}

func TestKWDBNodeInfoMarshal(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestKWDBNodeInfoMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBNodeInfoXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	dst := &sqlbase.KWDBNodeInfo{}
	dst.XXX_Merge(src)
	if dst.ClusterId != src.ClusterId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBNodeInfoSize(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBNodeInfoReset(t *testing.T) {
	msg := &sqlbase.KWDBNodeInfo{
		ClusterId: "cluster1",
	}
	msg.Reset()
	if msg.ClusterId != "" {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBCTable message
func TestKWDBCTable(t *testing.T) {
	msg := &sqlbase.KWDBCTable{
		Id:                  123,
		Name:                "test_table",
		Lifetime:            86400,
		KeepDuration:        []uint64{3600, 7200},
		Resolution:          []uint64{1000, 2000},
		Sample:              []string{"sample1", "sample2"},
		DownsamplingCreator: "creator",
		DownsamplingCounter: 100,
		Payloads:            [][]byte{[]byte("payload1"), []byte("payload2")},
		PrimaryKeys:         [][]byte{[]byte("key1"), []byte("key2")},
		NodeIDs:             []int32{1, 2, 3},
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBCTable: %v", err)
	}

	unmarshaled := &sqlbase.KWDBCTable{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBCTable: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

// Test KWDBTsTable message
func TestKWDBTsTable(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{
		TsTableId:         12345,
		DatabaseId:        1,
		LifeTime:          86400,
		ActiveTime:        3600,
		KColumnsId:        []uint32{1, 2, 3},
		RowSize:           1024,
		BitmapOffset:      512,
		TableName:         "test_ts_table",
		Sde:               true,
		PartitionInterval: 3600,
		TsVersion:         1,
		HashNum:           4,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBTsTable: %v", err)
	}

	unmarshaled := &sqlbase.KWDBTsTable{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBTsTable: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

// Test KWDBKTSColumn message
func TestKWDBKTSColumn(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{
		ColumnId:           1,
		Name:               "test_column",
		Nullable:           true,
		StorageType:        sqlbase.DataType_VARCHAR,
		StorageLen:         255,
		ColOffset:          0,
		VariableLengthType: sqlbase.VariableLengthType_ColStorageTypeTuple,
		DefaultValue:       "default",
		ColType:            sqlbase.ColumnType_TYPE_DATA,
		Dropped:            false,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBKTSColumn: %v", err)
	}

	unmarshaled := &sqlbase.KWDBKTSColumn{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBKTSColumn: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestKWDBTsTableString(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{
		TsTableId: 12345,
		TableName: "test_ts_table",
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBTsTableProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{}
	msg.ProtoMessage()
}

func TestKWDBTsTableDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{}
	_, _ = msg.Descriptor()
}

func TestKWDBTsTableMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{
		TsTableId: 12345,
		TableName: "test_ts_table",
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBTsTableXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBTsTable{
		TsTableId: 12345,
		TableName: "test_ts_table",
	}
	dst := &sqlbase.KWDBTsTable{}
	dst.XXX_Merge(src)
	if dst.TsTableId != src.TsTableId || dst.TableName != src.TableName {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBTsTableSize(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{
		TsTableId: 12345,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBTsTableReset(t *testing.T) {
	msg := &sqlbase.KWDBTsTable{
		TsTableId: 12345,
	}
	msg.Reset()
	if msg.TsTableId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestKWDBKTSColumnString(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{
		ColumnId: 1,
		Name:     "test_column",
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBKTSColumnProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{}
	msg.ProtoMessage()
}

func TestKWDBKTSColumnDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{}
	_, _ = msg.Descriptor()
}

func TestKWDBKTSColumnXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBKTSColumn{
		ColumnId: 1,
		Name:     "test_column",
	}
	dst := &sqlbase.KWDBKTSColumn{}
	dst.XXX_Merge(src)
	if dst.ColumnId != src.ColumnId || dst.Name != src.Name {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBKTSColumnSize(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{
		ColumnId: 1,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBKTSColumnReset(t *testing.T) {
	msg := &sqlbase.KWDBKTSColumn{
		ColumnId: 1,
	}
	msg.Reset()
	if msg.ColumnId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestNTagIndexInfoString(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestNTagIndexInfoProtoMessage(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{}
	msg.ProtoMessage()
}

func TestNTagIndexInfoDescriptor(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{}
	_, _ = msg.Descriptor()
}

func TestNTagIndexInfoMarshalTo(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestNTagIndexInfoXXXMerge(t *testing.T) {
	src := &sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}
	dst := &sqlbase.NTagIndexInfo{}
	dst.XXX_Merge(src)
	if dst.IndexId != src.IndexId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestNTagIndexInfoSize(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestNTagIndexInfoReset(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
	}
	msg.Reset()
	if msg.IndexId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestCreateTsTableString(t *testing.T) {
	msg := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId: 12345,
			TableName: "test_ts_table",
		},
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestCreateTsTableProtoMessage(t *testing.T) {
	msg := &sqlbase.CreateTsTable{}
	msg.ProtoMessage()
}

func TestCreateTsTableDescriptor(t *testing.T) {
	msg := &sqlbase.CreateTsTable{}
	_, _ = msg.Descriptor()
}

func TestCreateTsTableMarshalTo(t *testing.T) {
	msg := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId: 12345,
			TableName: "test_ts_table",
		},
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestCreateTsTableXXXMerge(t *testing.T) {
	src := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId: 12345,
			TableName: "test_ts_table",
		},
	}
	dst := &sqlbase.CreateTsTable{}
	dst.XXX_Merge(src)
	if dst.TsTable.TsTableId != src.TsTable.TsTableId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestCreateTsTableSize(t *testing.T) {
	msg := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId: 12345,
		},
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestCreateTsTableReset(t *testing.T) {
	msg := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId: 12345,
		},
	}
	msg.Reset()
	if msg.TsTable.TsTableId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test NTagIndexInfo message
func TestNTagIndexInfo(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal NTagIndexInfo: %v", err)
	}

	unmarshaled := &sqlbase.NTagIndexInfo{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal NTagIndexInfo: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

// Test CreateTsTable message
func TestCreateTsTable(t *testing.T) {
	kColumn := sqlbase.KWDBKTSColumn{
		ColumnId:           1,
		Name:               "test_column",
		Nullable:           true,
		StorageType:        sqlbase.DataType_VARCHAR,
		StorageLen:         255,
		ColOffset:          0,
		VariableLengthType: sqlbase.VariableLengthType_ColStorageTypeTuple,
		DefaultValue:       "default",
		ColType:            sqlbase.ColumnType_TYPE_DATA,
		Dropped:            false,
	}

	indexInfo := sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}

	msg := &sqlbase.CreateTsTable{
		TsTable: sqlbase.KWDBTsTable{
			TsTableId:         12345,
			DatabaseId:        1,
			LifeTime:          86400,
			ActiveTime:        3600,
			KColumnsId:        []uint32{1, 2, 3},
			RowSize:           1024,
			BitmapOffset:      512,
			TableName:         "test_ts_table",
			Sde:               true,
			PartitionInterval: 3600,
			TsVersion:         1,
			HashNum:           4,
		},
		KColumn:   []sqlbase.KWDBKTSColumn{kColumn},
		OldField:  nil,
		IndexInfo: []sqlbase.NTagIndexInfo{indexInfo},
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal CreateTsTable: %v", err)
	}

	unmarshaled := &sqlbase.CreateTsTable{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal CreateTsTable: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

// Test BlockInfo message
func TestBlockInfo(t *testing.T) {
	falseVal := float32(0.5)
	msg := &sqlbase.BlockInfo{
		Level:            "entity_segment",
		BlocksNum:        10,
		BlocksSize:       102400,
		AvgSize:          10240.0,
		CompressionRatio: &falseVal,
		LastSegLevel0:    5,
		LastSegLevel1:    3,
		LastSegLevel2:    2,
		OriginalSize:     204800,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal BlockInfo: %v", err)
	}

	unmarshaled := &sqlbase.BlockInfo{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal BlockInfo: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestBlockInfoString(t *testing.T) {
	msg := &sqlbase.BlockInfo{
		Level:      "entity_segment",
		BlocksNum:  10,
		BlocksSize: 102400,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestBlockInfoProtoMessage(t *testing.T) {
	msg := &sqlbase.BlockInfo{}
	msg.ProtoMessage()
}

func TestBlockInfoDescriptor(t *testing.T) {
	msg := &sqlbase.BlockInfo{}
	_, _ = msg.Descriptor()
}

func TestBlockInfoMarshalTo(t *testing.T) {
	msg := &sqlbase.BlockInfo{
		Level:      "entity_segment",
		BlocksNum:  10,
		BlocksSize: 102400,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestBlockInfoXXXMerge(t *testing.T) {
	src := &sqlbase.BlockInfo{
		Level:      "entity_segment",
		BlocksNum:  10,
		BlocksSize: 102400,
	}
	dst := &sqlbase.BlockInfo{}
	dst.XXX_Merge(src)
	if dst.Level != src.Level || dst.BlocksNum != src.BlocksNum {
		t.Errorf("XXX_Merge failed")
	}
}

func TestBlockInfoSize(t *testing.T) {
	msg := &sqlbase.BlockInfo{
		Level:     "entity_segment",
		BlocksNum: 10,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestBlockInfoReset(t *testing.T) {
	msg := &sqlbase.BlockInfo{
		Level:     "entity_segment",
		BlocksNum: 10,
	}
	msg.Reset()
	if msg.Level != "" || msg.BlocksNum != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test BlocksDistribution message
func TestBlocksDistribution(t *testing.T) {
	blockInfo := sqlbase.BlockInfo{
		Level:         "entity_segment",
		BlocksNum:     10,
		BlocksSize:    102400,
		AvgSize:       10240.0,
		LastSegLevel0: 5,
		LastSegLevel1: 3,
		LastSegLevel2: 2,
		OriginalSize:  204800,
	}

	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{blockInfo},
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal BlocksDistribution: %v", err)
	}

	unmarshaled := &sqlbase.BlocksDistribution{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal BlocksDistribution: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestBlocksDistributionString(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{{Level: "etity_segment"}},
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestBlocksDistributionProtoMessage(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{}
	msg.ProtoMessage()
}

func TestBlocksDistributionDescriptor(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{}
	_, _ = msg.Descriptor()
}

func TestBlocksDistributionMarshal(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{{Level: "etity_segment"}},
	}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestBlocksDistributionMarshalTo(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{{Level: "etity_segment"}},
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestBlocksDistributionXXXMerge(t *testing.T) {
	src := &sqlbase.BlocksDistribution{}
	dst := &sqlbase.BlocksDistribution{}
	dst.XXX_Merge(src)
}

func TestBlocksDistributionSize(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{{Level: "etity_segment"}},
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestBlocksDistributionReset(t *testing.T) {
	msg := &sqlbase.BlocksDistribution{
		BlockInfo: []sqlbase.BlockInfo{{Level: "test"}},
	}
	msg.Reset()
	if len(msg.BlockInfo) != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBReplicationMetaData message
func TestKWDBReplicationMetaData(t *testing.T) {
	sync := &sqlbase.KWDBReplicationSync{
		SyncId:                  1,
		SourceClusterId:         "source_cluster",
		SourceIp4:               "192.168.1.1",
		SourceConnectionId:      100,
		SourceToken:             1000,
		DestinationClusterId:    "dest_cluster",
		DestinationConnectionId: 200,
		DestinationIp4:          "192.168.1.2",
		DestinationToken:        2000,
		TotalAgentMeta:          5,
		IsReplicated:            1,
	}

	msg := &sqlbase.KWDBReplicationMetaData{
		DescId:          123,
		TotalNumOfSyncs: 1,
		Syncs:           []*sqlbase.KWDBReplicationSync{sync},
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationMetaData: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationMetaData{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationMetaData: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestKWDBReplicationMetaDataString(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{
		DescId: 123,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

// Test UpdateReplicationMetaData message
func TestUpdateReplicationMetaData(t *testing.T) {
	sync := &sqlbase.KWDBReplicationSync{
		SyncId:               1,
		SourceClusterId:      "source_cluster",
		SourceIp4:            "192.168.1.1",
		SourceConnectionId:   100,
		SourceToken:          1000,
		DestinationClusterId: "dest_cluster",
		DestinationIp4:       "192.168.1.2",
		DestinationToken:     2000,
	}

	metaData := &sqlbase.KWDBReplicationMetaData{
		DescId:          123,
		TotalNumOfSyncs: 1,
		Syncs:           []*sqlbase.KWDBReplicationSync{sync},
	}

	msg := &sqlbase.UpdateReplicationMetaData{
		KwdbRepMdt: *metaData,
	}

	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal UpdateReplicationMetaData: %v", err)
	}

	unmarshaled := &sqlbase.UpdateReplicationMetaData{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal UpdateReplicationMetaData: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestUpdateReplicationMetaDataString(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestUpdateReplicationMetaDataProtoMessage(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{}
	msg.ProtoMessage()
}

func TestUpdateReplicationMetaDataDescriptor(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{}
	_, _ = msg.Descriptor()
}

func TestUpdateReplicationMetaDataMarshal(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestUpdateReplicationMetaDataMarshalTo(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestUpdateReplicationMetaDataXXXMerge(t *testing.T) {
	src := &sqlbase.UpdateReplicationMetaData{
		KwdbRepMdt: sqlbase.KWDBReplicationMetaData{
			DescId: 123,
		},
	}
	dst := &sqlbase.UpdateReplicationMetaData{}
	dst.XXX_Merge(src)
	if dst.KwdbRepMdt.DescId != src.KwdbRepMdt.DescId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestUpdateReplicationMetaDataSize(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{
		KwdbRepMdt: sqlbase.KWDBReplicationMetaData{
			DescId: 123,
		},
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestUpdateReplicationMetaDataReset(t *testing.T) {
	msg := &sqlbase.UpdateReplicationMetaData{
		KwdbRepMdt: sqlbase.KWDBReplicationMetaData{
			DescId: 123,
		},
	}
	msg.Reset()
	if msg.KwdbRepMdt.DescId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBReplicationSync message
func TestKWDBReplicationSync(t *testing.T) {
	agentMeta := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType:         sqlbase.AgentType_METADATA,
		SourcePort:        5555,
		DesitionationPort: 6666,
		Frequency:         60,
		MaxRecords:        1000,
		Status:            sqlbase.KWDBReplicationStatus_RUNNING,
		ReplicaDescName:   "replica1",
	}

	msg := &sqlbase.KWDBReplicationSync{
		SyncId:                  1,
		SourceClusterId:         "source_cluster",
		SourceIp4:               "192.168.1.1",
		SourceConnectionId:      100,
		SourceToken:             1000,
		DestinationClusterId:    "dest_cluster",
		DestinationConnectionId: 200,
		DestinationIp4:          "192.168.1.2",
		DestinationToken:        2000,
		TotalAgentMeta:          1,
		AgentMetaData:           []*sqlbase.KWDBReplicationAgentMetaData{agentMeta},
		IsReplicated:            1,
	}

	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationSync: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationSync{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationSync: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	msg2 := &sqlbase.KWDBReplicationSync{
		SyncId: 2,
	}
	if reflect.DeepEqual(msg, msg2) {
		t.Errorf("Equal failed: messages should not be equal")
	}
}

func TestKWDBReplicationSyncString(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBReplicationSyncProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{}
	msg.ProtoMessage()
}

func TestKWDBReplicationSyncDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{}
	_, _ = msg.Descriptor()
}

func TestKWDBReplicationSyncMarshal(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestKWDBReplicationSyncMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBReplicationSyncXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	dst := &sqlbase.KWDBReplicationSync{}
	dst.XXX_Merge(src)
	if dst.SyncId != src.SyncId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBReplicationSyncSize(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBReplicationSyncReset(t *testing.T) {
	msg := &sqlbase.KWDBReplicationSync{
		SyncId: 1,
	}
	msg.Reset()
	if msg.SyncId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

func TestKWDBReplicationMetaDataProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{}
	msg.ProtoMessage()
}

func TestKWDBReplicationMetaDataDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{}
	_, _ = msg.Descriptor()
}

func TestKWDBReplicationMetaDataMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{
		DescId: 123,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBReplicationMetaDataXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBReplicationMetaData{
		DescId: 123,
	}
	dst := &sqlbase.KWDBReplicationMetaData{}
	dst.XXX_Merge(src)
	if dst.DescId != src.DescId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBReplicationMetaDataSize(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{
		DescId: 123,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBReplicationMetaDataReset(t *testing.T) {
	msg := &sqlbase.KWDBReplicationMetaData{
		DescId: 123,
	}
	msg.Reset()
	if msg.DescId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBReplicationAgentMetaData message
func TestKWDBReplicationAgentMetaData(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType:         sqlbase.AgentType_TS,
		SourcePort:        5432,
		DesitionationPort: 5433,
		Frequency:         10,
		MaxRecords:        1000,
		Status:            sqlbase.KWDBReplicationStatus_RUNNING,
		ReplicaDescName:   "replica1",
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationAgentMetaData: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationAgentMetaData{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationAgentMetaData: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestKWDBReplicationAgentMetaDataString(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBReplicationAgentMetaDataProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{}
	msg.ProtoMessage()
}

func TestKWDBReplicationAgentMetaDataDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{}
	_, _ = msg.Descriptor()
}

func TestKWDBReplicationAgentMetaDataMarshal(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestKWDBReplicationAgentMetaDataMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBReplicationAgentMetaDataXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	dst := &sqlbase.KWDBReplicationAgentMetaData{}
	dst.XXX_Merge(src)
	if dst.AgentType != src.AgentType {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBReplicationAgentMetaDataSize(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBReplicationAgentMetaDataReset(t *testing.T) {
	msg := &sqlbase.KWDBReplicationAgentMetaData{
		AgentType: sqlbase.AgentType_TS,
	}
	msg.Reset()
	if msg.AgentType != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBReplicationProgress message
func TestKWDBReplicationProgress(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{
		Txn:       100,
		Per:       50,
		Min:       10,
		Total:     200,
		Completed: 100,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationProgress: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationProgress{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationProgress: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	msg2 := &sqlbase.KWDBReplicationProgress{Txn: 200}
	if reflect.DeepEqual(msg, msg2) {
		t.Errorf("Equal failed: messages should not be equal")
	}
}

func TestKWDBReplicationProgressString(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{Txn: 100}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBReplicationProgressProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{}
	msg.ProtoMessage()
}

func TestKWDBReplicationProgressDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{}
	_, _ = msg.Descriptor()
}

func TestKWDBReplicationProgressMarshal(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{Txn: 100}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestKWDBReplicationProgressMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{Txn: 100}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBReplicationProgressXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBReplicationProgress{Txn: 100}
	dst := &sqlbase.KWDBReplicationProgress{}
	dst.XXX_Merge(src)
	if dst.Txn != src.Txn {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBReplicationProgressSize(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{Txn: 100}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBReplicationProgressReset(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgress{Txn: 100}
	msg.Reset()
	if msg.Txn != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test KWDBReplicationProgressSet message
func TestKWDBReplicationProgressSet(t *testing.T) {
	progress := &sqlbase.KWDBReplicationProgress{
		Txn:       100,
		Per:       50,
		Min:       10,
		Total:     200,
		Completed: 100,
	}

	msg := &sqlbase.KWDBReplicationProgressSet{
		PortalId:    1,
		Source:      "source1",
		Destination: "dest1",
		AgentType:   sqlbase.AgentType_TS,
		Progress:    progress,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationProgressSet: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationProgressSet{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationProgressSet: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestKWDBReplicationProgressSetString(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestKWDBReplicationProgressSetProtoMessage(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{}
	msg.ProtoMessage()
}

func TestKWDBReplicationProgressSetDescriptor(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{}
	_, _ = msg.Descriptor()
}

func TestKWDBReplicationProgressSetMarshal(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestKWDBReplicationProgressSetMarshalTo(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestKWDBReplicationProgressSetXXXMerge(t *testing.T) {
	src := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	dst := &sqlbase.KWDBReplicationProgressSet{}
	dst.XXX_Merge(src)
	if dst.PortalId != src.PortalId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestKWDBReplicationProgressSetSize(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestKWDBReplicationProgressSetReset(t *testing.T) {
	msg := &sqlbase.KWDBReplicationProgressSet{PortalId: 1}
	msg.Reset()
	if msg.PortalId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test ReplicationServiceCallerFuncInputs message
func TestReplicationServiceCallerFuncInputs(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{
		AgentType:       sqlbase.AgentType_TS,
		ReplicationType: sqlbase.ReplicationType_E_SYNC_TO_E,
		PortalId:        1,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal ReplicationServiceCallerFuncInputs: %v", err)
	}

	unmarshaled := &sqlbase.ReplicationServiceCallerFuncInputs{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal ReplicationServiceCallerFuncInputs: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestReplicationServiceCallerFuncInputsString(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestReplicationServiceCallerFuncInputsProtoMessage(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{}
	msg.ProtoMessage()
}

func TestReplicationServiceCallerFuncInputsDescriptor(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{}
	_, _ = msg.Descriptor()
}

func TestReplicationServiceCallerFuncInputsMarshal(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestReplicationServiceCallerFuncInputsMarshalTo(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestReplicationServiceCallerFuncInputsXXXMerge(t *testing.T) {
	src := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	dst := &sqlbase.ReplicationServiceCallerFuncInputs{}
	dst.XXX_Merge(src)
	if dst.PortalId != src.PortalId {
		t.Errorf("XXX_Merge failed")
	}
}

func TestReplicationServiceCallerFuncInputsSize(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestReplicationServiceCallerFuncInputsReset(t *testing.T) {
	msg := &sqlbase.ReplicationServiceCallerFuncInputs{PortalId: 1}
	msg.Reset()
	if msg.PortalId != 0 {
		t.Error("Reset() should zero all fields")
	}
}

// Test WhiteList message
func TestWhiteList(t *testing.T) {
	msg := &sqlbase.WhiteList{
		Name:     "test_func",
		ArgNum:   2,
		ArgType:  []uint32{1, 2},
		Position: 1,
		Enabled:  true,
		ArgOpt:   0,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal WhiteList: %v", err)
	}

	unmarshaled := &sqlbase.WhiteList{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal WhiteList: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestWhiteListString(t *testing.T) {
	msg := &sqlbase.WhiteList{Name: "test"}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestWhiteListProtoMessage(t *testing.T) {
	msg := &sqlbase.WhiteList{}
	msg.ProtoMessage()
}

func TestWhiteListDescriptor(t *testing.T) {
	msg := &sqlbase.WhiteList{}
	_, _ = msg.Descriptor()
}

func TestWhiteListMarshal(t *testing.T) {
	msg := &sqlbase.WhiteList{Name: "test"}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestWhiteListMarshalTo(t *testing.T) {
	msg := &sqlbase.WhiteList{Name: "test"}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestWhiteListXXXMerge(t *testing.T) {
	src := &sqlbase.WhiteList{Name: "test"}
	dst := &sqlbase.WhiteList{}
	dst.XXX_Merge(src)
	if dst.Name != src.Name {
		t.Errorf("XXX_Merge failed")
	}
}

func TestWhiteListSize(t *testing.T) {
	msg := &sqlbase.WhiteList{Name: "test"}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestWhiteListReset(t *testing.T) {
	msg := &sqlbase.WhiteList{Name: "test"}
	msg.Reset()
	if msg.Name != "" {
		t.Error("Reset() should zero all fields")
	}
}

// Test TSInsertSelect message
func TestTSInsertSelect(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{
		Sql:     "SELECT * FROM table",
		TableId: 123,
		DbId:    456,
	}

	// Test marshaling and unmarshaling
	data, err := protoutil.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal TSInsertSelect: %v", err)
	}

	unmarshaled := &sqlbase.TSInsertSelect{}
	err = protoutil.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal TSInsertSelect: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

func TestTSInsertSelectString(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	str := msg.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestTSInsertSelectProtoMessage(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{}
	msg.ProtoMessage()
}

func TestTSInsertSelectDescriptor(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{}
	_, _ = msg.Descriptor()
}

func TestTSInsertSelectMarshal(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal() should return non-empty data")
	}
}

func TestTSInsertSelectMarshalTo(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	buf := make([]byte, 256)
	n, err := msg.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() failed: %v", err)
	}
	if n == 0 {
		t.Error("MarshalTo() should return non-zero size")
	}
}

func TestTSInsertSelectXXXMerge(t *testing.T) {
	src := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	dst := &sqlbase.TSInsertSelect{}
	dst.XXX_Merge(src)
	if dst.Sql != src.Sql {
		t.Errorf("XXX_Merge failed")
	}
}

func TestTSInsertSelectSize(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	size := msg.Size()
	if size == 0 {
		t.Error("Size() should return non-zero size")
	}
}

func TestTSInsertSelectReset(t *testing.T) {
	msg := &sqlbase.TSInsertSelect{Sql: "SELECT 1"}
	msg.Reset()
	if msg.Sql != "" {
		t.Error("Reset() should zero all fields")
	}
}
