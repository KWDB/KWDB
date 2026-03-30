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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/gogo/protobuf/proto"
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal DeleteMeMsg: %v", err)
	}

	unmarshaled := &sqlbase.DeleteMeMsg{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal DeleteMeMsg: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBHAInfo: %v", err)
	}

	unmarshaled := &sqlbase.KWDBHAInfo{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBHAInfo: %v", err)
	}

	if !msg.Equal(unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	if !unmarshaled.Equal(msg) {
		t.Errorf("Equal failed: messages should be equal")
	}

	// Test with different values
	msg2 := &sqlbase.KWDBHAInfo{
		ClusterId:        "cluster2",
		Role:             2,
		HaStatus:         sqlbase.KWDBHAStatus_HASTOPPED,
		ConnectionStatus: sqlbase.KWDBConnectionStatus_UNHEALTHY,
	}

	if msg.Equal(msg2) {
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBNodeInfo: %v", err)
	}

	unmarshaled := &sqlbase.KWDBNodeInfo{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBNodeInfo: %v", err)
	}

	if !msg.Equal(unmarshaled) {
		t.Errorf("Equal failed: messages should be equal")
	}

	if !unmarshaled.Equal(msg) {
		t.Errorf("Equal failed: messages should be equal")
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBCTable: %v", err)
	}

	unmarshaled := &sqlbase.KWDBCTable{}
	err = proto.Unmarshal(data, unmarshaled)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBTsTable: %v", err)
	}

	unmarshaled := &sqlbase.KWDBTsTable{}
	err = proto.Unmarshal(data, unmarshaled)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBKTSColumn: %v", err)
	}

	unmarshaled := &sqlbase.KWDBKTSColumn{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBKTSColumn: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}

// Test NTagIndexInfo message
func TestNTagIndexInfo(t *testing.T) {
	msg := &sqlbase.NTagIndexInfo{
		IndexId: 1,
		ColIds:  []uint32{1, 2, 3},
	}

	// Test marshaling and unmarshaling
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal NTagIndexInfo: %v", err)
	}

	unmarshaled := &sqlbase.NTagIndexInfo{}
	err = proto.Unmarshal(data, unmarshaled)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal CreateTsTable: %v", err)
	}

	unmarshaled := &sqlbase.CreateTsTable{}
	err = proto.Unmarshal(data, unmarshaled)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal BlockInfo: %v", err)
	}

	unmarshaled := &sqlbase.BlockInfo{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal BlockInfo: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal BlocksDistribution: %v", err)
	}

	unmarshaled := &sqlbase.BlocksDistribution{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal BlocksDistribution: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationMetaData: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationMetaData{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationMetaData: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationAgentMetaData: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationAgentMetaData{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationAgentMetaData: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationProgress: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationProgress{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationProgress: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal KWDBReplicationProgressSet: %v", err)
	}

	unmarshaled := &sqlbase.KWDBReplicationProgressSet{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal KWDBReplicationProgressSet: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal ReplicationServiceCallerFuncInputs: %v", err)
	}

	unmarshaled := &sqlbase.ReplicationServiceCallerFuncInputs{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal ReplicationServiceCallerFuncInputs: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal WhiteList: %v", err)
	}

	unmarshaled := &sqlbase.WhiteList{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal WhiteList: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
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
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal TSInsertSelect: %v", err)
	}

	unmarshaled := &sqlbase.TSInsertSelect{}
	err = proto.Unmarshal(data, unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal TSInsertSelect: %v", err)
	}

	if !reflect.DeepEqual(msg, unmarshaled) {
		t.Errorf("Marshal/Unmarshal failed: got %v, want %v", unmarshaled, msg)
	}
}
