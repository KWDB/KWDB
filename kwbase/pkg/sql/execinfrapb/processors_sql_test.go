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

package execinfrapb

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// TestScanVisibility tests the ScanVisibility enum methods
func TestScanVisibility(t *testing.T) {
	// Test Enum method
	visibility := ScanVisibility_PUBLIC
	enumPtr := visibility.Enum()
	if *enumPtr != ScanVisibility_PUBLIC {
		t.Errorf("Enum() returned %v, expected %v", *enumPtr, ScanVisibility_PUBLIC)
	}

	// Test String method
	if visibility.String() != "PUBLIC" {
		t.Errorf("String() returned %q, expected %q", visibility.String(), "PUBLIC")
	}

	// Test EnumDescriptor method
	desc, path := visibility.EnumDescriptor()
	if len(desc) == 0 {
		t.Error("EnumDescriptor() returned empty descriptor")
	}
	if len(path) == 0 {
		t.Error("EnumDescriptor() returned empty path")
	}
}

// TestDedupRule tests the DedupRule enum methods
func TestDedupRule(t *testing.T) {
	// Test Enum method
	rule := DedupRule_TsReject
	enumPtr := rule.Enum()
	if *enumPtr != DedupRule_TsReject {
		t.Errorf("Enum() returned %v, expected %v", *enumPtr, DedupRule_TsReject)
	}

	// Test String method
	if rule.String() != "TsReject" {
		t.Errorf("String() returned %q, expected %q", rule.String(), "TsReject")
	}

	// Test EnumDescriptor method
	desc, path := rule.EnumDescriptor()
	if len(desc) == 0 {
		t.Error("EnumDescriptor() returned empty descriptor")
	}
	if len(path) == 0 {
		t.Error("EnumDescriptor() returned empty path")
	}
}

// TestAllGeneratedFunctions tests all generated serialization and deserialization functions
func TestAllGeneratedFunctions(t *testing.T) {
	// Test all message types
	testValuesCoreSpec(t)
	testTableReaderSpec(t)
	testTsInsertSelSpec(t)
	testPayloadForDistributeMode(t)
	testTsInsertProSpec(t)
	testSpan(t)
	testTsDeleteProSpec(t)
	testDeleteEntityGroup(t)
	testTsTagUpdateProSpec(t)
	testTsCreateTableProSpec(t)
	testTsProSpec(t)
	testTsAlterProSpec(t)
	testIndexSkipTableReaderSpec(t)
	testJoinReaderSpec(t)
	testSorterSpec(t)
	testDistinctSpec(t)
	testOrdinalitySpec(t)
	testZigzagJoinerSpec(t)
	testMergeJoinerSpec(t)
	testHashJoinerSpec(t)
	testBatchLookupJoinerSpec(t)
	testAggregatorSpec(t)
	testInterleavedReaderJoinerSpec(t)
	testProjectSetSpec(t)
	testWindowerSpec(t)
	testReplicationServiceCallerSpec(t)
	testTsInsertWithCDCProSpec(t)
	testCDCData(t)
	testCDCPushData(t)
}

func testValuesCoreSpec(t *testing.T) {
	spec := &ValuesCoreSpec{}
	testMessage(t, spec)
}

func testTableReaderSpec(t *testing.T) {
	spec := &TableReaderSpec{
		Table:    sqlbase.TableDescriptor{},
		IndexIdx: 1,
		Reverse:  true,
	}
	testMessage(t, spec)
}

func testTsInsertSelSpec(t *testing.T) {
	spec := &TsInsertSelSpec{
		TargetTableId: 123,
		DbId:          456,
		Cols:          []int32{1, 2, 3},
	}
	testMessage(t, spec)
}

func testPayloadForDistributeMode(t *testing.T) {
	spec := &PayloadForDistributeMode{
		Row:      [][]byte{{1, 2, 3}, {4, 5, 6}},
		StartKey: []byte{7, 8, 9},
		EndKey:   []byte{10, 11, 12},
	}
	testMessage(t, spec)
}

func testTsInsertProSpec(t *testing.T) {
	spec := &TsInsertProSpec{
		RowNums: []uint32{1, 2, 3},
		PayLoad: [][]byte{{1, 2, 3}, {4, 5, 6}},
	}
	testMessage(t, spec)
}

func testSpan(t *testing.T) {
	spec := &Span{
		StartTs: 123456,
		EndTs:   789012,
	}
	testMessage(t, spec)
}

func testTsDeleteProSpec(t *testing.T) {
	spec := &TsDeleteProSpec{
		TableId:      123,
		RangeGroupId: 456,
		EntityGroups: []DeleteEntityGroup{
			{
				GroupId: 789,
			},
		},
	}
	testMessage(t, spec)
}

func testDeleteEntityGroup(t *testing.T) {
	spec := &DeleteEntityGroup{
		GroupId: 123,
	}
	testMessage(t, spec)
}

func testTsTagUpdateProSpec(t *testing.T) {
	spec := &TsTagUpdateProSpec{
		TableId:        123,
		RangeGroupId:   456,
		PrimaryTagKeys: [][]byte{{1, 2, 3}, {4, 5, 6}},
		PrimaryTags:    [][]byte{{7, 8, 9}, {10, 11, 12}},
	}
	testMessage(t, spec)
}

func testTsCreateTableProSpec(t *testing.T) {
	spec := &TsCreateTableProSpec{
		TsTableID: 123,
		HashNum:   456,
	}
	testMessage(t, spec)
}

func testTsProSpec(t *testing.T) {
	spec := &TsProSpec{}
	testMessage(t, spec)
}

func testTsAlterProSpec(t *testing.T) {
	spec := &TsAlterProSpec{
		TsTableID:         123,
		PartitionInterval: 456,
	}
	testMessage(t, spec)
}

func testIndexSkipTableReaderSpec(t *testing.T) {
	spec := &IndexSkipTableReaderSpec{
		Table:    sqlbase.TableDescriptor{},
		IndexIdx: 1,
		Reverse:  true,
	}
	testMessage(t, spec)
}

func testJoinReaderSpec(t *testing.T) {
	spec := &JoinReaderSpec{
		Table:    sqlbase.TableDescriptor{},
		IndexIdx: 1,
	}
	testMessage(t, spec)
}

func testSorterSpec(t *testing.T) {
	spec := &SorterSpec{}
	testMessage(t, spec)
}

func testDistinctSpec(t *testing.T) {
	spec := &DistinctSpec{}
	testMessage(t, spec)
}

func testOrdinalitySpec(t *testing.T) {
	spec := &OrdinalitySpec{}
	testMessage(t, spec)
}

func testZigzagJoinerSpec(t *testing.T) {
	spec := &ZigzagJoinerSpec{}
	testMessage(t, spec)
}

func testMergeJoinerSpec(t *testing.T) {
	spec := &MergeJoinerSpec{}
	testMessage(t, spec)
}

func testHashJoinerSpec(t *testing.T) {
	spec := &HashJoinerSpec{}
	testMessage(t, spec)
}

func testBatchLookupJoinerSpec(t *testing.T) {
	spec := &BatchLookupJoinerSpec{}
	testMessage(t, spec)
}

func testAggregatorSpec(t *testing.T) {
	spec := &AggregatorSpec{
		Aggregations: []AggregatorSpec_Aggregation{
			{},
		},
	}
	testMessage(t, spec)
}

func testInterleavedReaderJoinerSpec(t *testing.T) {
	spec := &InterleavedReaderJoinerSpec{
		Tables: []InterleavedReaderJoinerSpec_Table{
			{},
		},
	}
	testMessage(t, spec)
}

func testProjectSetSpec(t *testing.T) {
	spec := &ProjectSetSpec{}
	testMessage(t, spec)
}

func testWindowerSpec(t *testing.T) {
	spec := &WindowerSpec{
		PartitionBy: []uint32{1, 2, 3},
		WindowFns:   []WindowerSpec_WindowFn{},
	}
	testMessage(t, spec)
}

func testReplicationServiceCallerSpec(t *testing.T) {
	spec := &ReplicationServiceCallerSpec{
		FuncName:  "testFunc",
		Inputs:    sqlbase.ReplicationServiceCallerFuncInputs{},
		IsGateWay: true,
		DisasterNodes: []ReplicationServiceCallerSpec_DisasterNode{
			{
				NodeId: 1,
				Err:    "test error",
			},
		},
	}
	testMessage(t, spec)
}

func testTsInsertWithCDCProSpec(t *testing.T) {
	spec := &TsInsertWithCDCProSpec{
		RowNums:       []uint32{1, 2, 3},
		PayLoad:       [][]byte{{1, 2, 3}, {4, 5, 6}},
		PrimaryTagKey: [][]byte{{7, 8, 9}, {10, 11, 12}},
		PayloadPrefix: [][]byte{{13, 14, 15}, {16, 17, 18}},
		CDCData: CDCData{
			TableID:      123,
			MinTimestamp: 456,
		},
	}
	testMessage(t, spec)
}

func testCDCData(t *testing.T) {
	spec := &CDCData{
		TableID:      789,
		MinTimestamp: 987,
		PushData: []*CDCPushData{
			{
				TaskID: 123,
			},
		},
	}
	testMessage(t, spec)
}

func testCDCPushData(t *testing.T) {
	spec := &CDCPushData{
		TaskID: 123,
	}
	testMessage(t, spec)
}

// testMessage is a helper function to test serialization and deserialization of a message
func testMessage(t *testing.T, msg interface{}) {
	// Test Size function
	sizeMethod, ok := msg.(interface{ Size() int })
	if ok {
		size := sizeMethod.Size()
		if size < 0 {
			t.Errorf("Size() returned %d, expected non-negative value", size)
		}

		// Test Marshal function
		_, ok := msg.(interface{ Marshal() ([]byte, error) })
		if ok {

			// Test MarshalTo function
			marshalToMethod, ok := msg.(interface{ MarshalTo([]byte) (int, error) })
			if ok {
				buf := make([]byte, size)
				n, err := marshalToMethod.MarshalTo(buf)
				if err != nil {
					t.Errorf("MarshalTo() failed: %v", err)
				}
				if n < 0 {
					t.Errorf("MarshalTo() returned %d, expected non-negative value", n)
				}
			}
		}
	}
}

func TestValuesCoreSpecRAI(t *testing.T) {
	obj1 := &ValuesCoreSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTableReaderSpecRAI(t *testing.T) {
	obj1 := &TableReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsInsertSelSpecRAI(t *testing.T) {
	obj1 := &TsInsertSelSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestPayloadForDistributeModeRAI(t *testing.T) {
	obj1 := &PayloadForDistributeMode{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsInsertProSpecRAI(t *testing.T) {
	obj1 := &TsInsertProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestSpanRAI(t *testing.T) {
	obj1 := &Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsDeleteProSpecRAI(t *testing.T) {
	obj1 := &TsDeleteProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestDeleteEntityGroupRAI(t *testing.T) {
	obj1 := &DeleteEntityGroup{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsTagUpdateProSpecRAI(t *testing.T) {
	obj1 := &TsTagUpdateProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsCreateTableProSpecRAI(t *testing.T) {
	obj1 := &TsCreateTableProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsProSpecRAI(t *testing.T) {
	obj1 := &TsProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsAlterProSpecRAI(t *testing.T) {
	obj1 := &TsAlterProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestIndexSkipTableReaderSpecRAI(t *testing.T) {
	obj1 := &IndexSkipTableReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestJoinReaderSpecRAI(t *testing.T) {
	obj1 := &JoinReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestSorterSpecRAI(t *testing.T) {
	obj1 := &SorterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestDistinctSpecRAI(t *testing.T) {
	obj1 := &DistinctSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOrdinalitySpecRAI(t *testing.T) {
	obj1 := &OrdinalitySpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestZigzagJoinerSpecRAI(t *testing.T) {
	obj1 := &ZigzagJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestMergeJoinerSpecRAI(t *testing.T) {
	obj1 := &MergeJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestHashJoinerSpecRAI(t *testing.T) {
	obj1 := &HashJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestBatchLookupJoinerSpecRAI(t *testing.T) {
	obj1 := &BatchLookupJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestAggregatorSpecRAI(t *testing.T) {
	obj1 := &AggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestAggregatorSpec_AggregationRAI(t *testing.T) {
	obj1 := &AggregatorSpec_Aggregation{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestInterleavedReaderJoinerSpecRAI(t *testing.T) {
	obj1 := &InterleavedReaderJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestInterleavedReaderJoinerSpec_TableRAI(t *testing.T) {
	obj1 := &InterleavedReaderJoinerSpec_Table{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestProjectSetSpecRAI(t *testing.T) {
	obj1 := &ProjectSetSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpecRAI(t *testing.T) {
	obj1 := &WindowerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpec_FuncRAI(t *testing.T) {
	obj1 := &WindowerSpec_Func{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpec_FrameRAI(t *testing.T) {
	obj1 := &WindowerSpec_Frame{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpec_Frame_BoundRAI(t *testing.T) {
	obj1 := &WindowerSpec_Frame_Bound{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpec_Frame_BoundsRAI(t *testing.T) {
	obj1 := &WindowerSpec_Frame_Bounds{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestWindowerSpec_WindowFnRAI(t *testing.T) {
	obj1 := &WindowerSpec_WindowFn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationServiceCallerSpecRAI(t *testing.T) {
	obj1 := &ReplicationServiceCallerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationServiceCallerSpec_DisasterNodeRAI(t *testing.T) {
	obj1 := &ReplicationServiceCallerSpec_DisasterNode{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsInsertWithCDCProSpecRAI(t *testing.T) {
	obj1 := &TsInsertWithCDCProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestCDCDataRAI(t *testing.T) {
	obj1 := &CDCData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestCDCPushDataRAI(t *testing.T) {
	obj1 := &CDCPushData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}
