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

package sql

import (
	"reflect"
	"testing"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

type cdcPayloadSeed struct {
	payload    []byte
	rowNum     uint32
	primaryTag []byte
	rowBytes   []byte
	startKey   []byte
	endKey     []byte
	valueSize  int64
}

// forceSetUnexportedField sets an unexported struct field in tests.
// This keeps the test independent from the hidden concrete type of
// allNodePayloadInfos.
func forceSetUnexportedField(target interface{}, fieldName string, value reflect.Value) {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		panic("target must be pointer to struct")
	}

	f := v.Elem().FieldByName(fieldName)
	if !f.IsValid() {
		panic("field not found: " + fieldName)
	}

	toSet := value
	if !toSet.Type().AssignableTo(f.Type()) {
		if toSet.Type().ConvertibleTo(f.Type()) {
			toSet = toSet.Convert(f.Type())
		} else {
			panic("value is not assignable to field " + fieldName)
		}
	}

	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(toSet)
}

func setNumericField(f reflect.Value, v int64) {
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		f.SetUint(uint64(v))
	default:
		panic("unsupported numeric field kind: " + f.Kind().String())
	}
}

func setBytesLikeField(f reflect.Value, b []byte) {
	src := reflect.ValueOf(b)
	if src.Type().AssignableTo(f.Type()) {
		f.Set(src)
		return
	}
	if src.Type().ConvertibleTo(f.Type()) {
		f.Set(src.Convert(f.Type()))
		return
	}

	// Support [][]byte (or aliases of it) by wrapping one []byte into a
	// single-element outer slice.
	if f.Kind() == reflect.Slice &&
		f.Type().Elem().Kind() == reflect.Slice &&
		f.Type().Elem().Elem().Kind() == reflect.Uint8 {
		outer := reflect.MakeSlice(f.Type(), 1, 1)
		elem := reflect.ValueOf(b)
		if elem.Type().AssignableTo(f.Type().Elem()) {
			outer.Index(0).Set(elem)
			f.Set(outer)
			return
		}
		if elem.Type().ConvertibleTo(f.Type().Elem()) {
			outer.Index(0).Set(elem.Convert(f.Type().Elem()))
			f.Set(outer)
			return
		}
	}

	panic("cannot assign []byte to field type " + f.Type().String())
}

func setStringOrNumericField(f reflect.Value, s string, n int64) {
	switch f.Kind() {
	case reflect.String:
		f.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		setNumericField(f, n)
	default:
		panic("unsupported TaskType/TaskID field kind: " + f.Kind().String())
	}
}

func makeTestCDCData(t *testing.T) *sqlbase.CDCData {
	t.Helper()

	cdc := &sqlbase.CDCData{}
	v := reflect.ValueOf(cdc).Elem()

	tableIDField := v.FieldByName("TableID")
	if !tableIDField.IsValid() {
		t.Fatal("sqlbase.CDCData.TableID not found")
	}
	setNumericField(tableIDField, 1001)

	minTsField := v.FieldByName("MinTimestamp")
	if !minTsField.IsValid() {
		t.Fatal("sqlbase.CDCData.MinTimestamp not found")
	}
	setNumericField(minTsField, 123456789)

	pushDataField := v.FieldByName("PushData")
	if !pushDataField.IsValid() {
		t.Fatal("sqlbase.CDCData.PushData not found")
	}

	pushSlice := reflect.MakeSlice(pushDataField.Type(), 2, 2)
	elemType := pushDataField.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	baseType := elemType
	if isPtr {
		baseType = elemType.Elem()
	}

	for i := 0; i < 2; i++ {
		item := reflect.New(baseType).Elem()

		taskIDField := item.FieldByName("TaskID")
		if !taskIDField.IsValid() {
			t.Fatal("CDC push data TaskID not found")
		}
		setStringOrNumericField(taskIDField, "", int64(i+1))

		taskTypeField := item.FieldByName("TaskType")
		if !taskTypeField.IsValid() {
			t.Fatal("CDC push data TaskType not found")
		}
		setStringOrNumericField(taskTypeField, "type", int64(10+i))

		rowsField := item.FieldByName("Rows")
		if !rowsField.IsValid() {
			t.Fatal("CDC push data Rows not found")
		}

		switch {
		case rowsField.Kind() == reflect.Slice && rowsField.Type().Elem().Kind() == reflect.Uint8:
			setBytesLikeField(rowsField, []byte{byte('r'), byte('0' + i)})
		case rowsField.Kind() == reflect.Slice &&
			rowsField.Type().Elem().Kind() == reflect.Slice &&
			rowsField.Type().Elem().Elem().Kind() == reflect.Uint8:
			outer := reflect.MakeSlice(rowsField.Type(), 1, 1)
			elem := reflect.ValueOf([]byte{byte('r'), byte('0' + i)})
			if elem.Type().AssignableTo(rowsField.Type().Elem()) {
				outer.Index(0).Set(elem)
			} else if elem.Type().ConvertibleTo(rowsField.Type().Elem()) {
				outer.Index(0).Set(elem.Convert(rowsField.Type().Elem()))
			} else {
				t.Fatalf("cannot assign [][]byte-style value to Rows field type %s", rowsField.Type())
			}
			rowsField.Set(outer)
		default:
			t.Fatalf("unsupported CDC push data Rows field type: %s", rowsField.Type())
		}

		if isPtr {
			pushSlice.Index(i).Set(item.Addr())
		} else {
			pushSlice.Index(i).Set(item)
		}
	}

	pushDataField.Set(pushSlice)
	return cdc
}

func makeTsInsertWithCDCNodeForTest(
	t *testing.T, nodeIDs []roachpb.NodeID, perNode [][]cdcPayloadSeed,
) *tsInsertWithCDCNode {
	t.Helper()

	n := &tsInsertWithCDCNode{
		CDCData: makeTestCDCData(t),
	}

	// nodeIDs is unexported in the real node type.
	forceSetUnexportedField(n, "nodeIDs", reflect.ValueOf(nodeIDs))

	// allNodePayloadInfos is a hidden concrete nested slice type, so create it
	// reflectively from the real field type.
	field := reflect.ValueOf(n).Elem().FieldByName("allNodePayloadInfos")
	if !field.IsValid() {
		t.Fatal("tsInsertWithCDCNode.allNodePayloadInfos not found")
	}

	outer := reflect.MakeSlice(field.Type(), len(perNode), len(perNode))
	innerType := field.Type().Elem()
	elemType := innerType.Elem()

	elemIsPtr := elemType.Kind() == reflect.Ptr
	baseElemType := elemType
	if elemIsPtr {
		baseElemType = elemType.Elem()
	}

	for i, seeds := range perNode {
		inner := reflect.MakeSlice(innerType, len(seeds), len(seeds))
		for j, seed := range seeds {
			elem := reflect.New(baseElemType).Elem()

			payloadField := elem.FieldByName("Payload")
			if !payloadField.IsValid() {
				t.Fatal("payload info field Payload not found")
			}
			setBytesLikeField(payloadField, seed.payload)

			rowNumField := elem.FieldByName("RowNum")
			if !rowNumField.IsValid() {
				t.Fatal("payload info field RowNum not found")
			}
			setNumericField(rowNumField, int64(seed.rowNum))

			primaryTagField := elem.FieldByName("PrimaryTagKey")
			if !primaryTagField.IsValid() {
				t.Fatal("payload info field PrimaryTagKey not found")
			}
			setBytesLikeField(primaryTagField, seed.primaryTag)

			rowBytesField := elem.FieldByName("RowBytes")
			if rowBytesField.IsValid() {
				setBytesLikeField(rowBytesField, seed.rowBytes)
			}

			startKeyField := elem.FieldByName("StartKey")
			if startKeyField.IsValid() {
				setBytesLikeField(startKeyField, seed.startKey)
			}

			endKeyField := elem.FieldByName("EndKey")
			if endKeyField.IsValid() {
				setBytesLikeField(endKeyField, seed.endKey)
			}

			valueSizeField := elem.FieldByName("ValueSize")
			if valueSizeField.IsValid() {
				setNumericField(valueSizeField, seed.valueSize)
			}

			if elemIsPtr {
				inner.Index(j).Set(elem.Addr())
			} else {
				inner.Index(j).Set(elem)
			}
		}
		outer.Index(i).Set(inner)
	}

	forceSetUnexportedField(n, "allNodePayloadInfos", outer)
	return n
}

func compareCDCDataWithSource(t *testing.T, src *sqlbase.CDCData, got execinfrapb.CDCData) {
	t.Helper()

	srcV := reflect.ValueOf(src).Elem()
	gotV := reflect.ValueOf(got)

	if !reflect.DeepEqual(srcV.FieldByName("TableID").Interface(), gotV.FieldByName("TableID").Interface()) {
		t.Fatalf("TableID mismatch: src=%v got=%v",
			srcV.FieldByName("TableID").Interface(), gotV.FieldByName("TableID").Interface())
	}
	if !reflect.DeepEqual(srcV.FieldByName("MinTimestamp").Interface(), gotV.FieldByName("MinTimestamp").Interface()) {
		t.Fatalf("MinTimestamp mismatch: src=%v got=%v",
			srcV.FieldByName("MinTimestamp").Interface(), gotV.FieldByName("MinTimestamp").Interface())
	}

	srcPush := srcV.FieldByName("PushData")
	gotPush := gotV.FieldByName("PushData")
	if srcPush.Len() != gotPush.Len() {
		t.Fatalf("PushData length mismatch: src=%d got=%d", srcPush.Len(), gotPush.Len())
	}

	for i := 0; i < srcPush.Len(); i++ {
		srcItem := srcPush.Index(i)
		if srcItem.Kind() == reflect.Ptr {
			srcItem = srcItem.Elem()
		}
		gotItem := gotPush.Index(i)
		if gotItem.Kind() == reflect.Ptr {
			gotItem = gotItem.Elem()
		}

		if !reflect.DeepEqual(srcItem.FieldByName("TaskID").Interface(), gotItem.FieldByName("TaskID").Interface()) {
			t.Fatalf("TaskID mismatch at %d", i)
		}
		if !reflect.DeepEqual(srcItem.FieldByName("TaskType").Interface(), gotItem.FieldByName("TaskType").Interface()) {
			t.Fatalf("TaskType mismatch at %d", i)
		}
		if !reflect.DeepEqual(srcItem.FieldByName("Rows").Interface(), gotItem.FieldByName("Data").Interface()) {
			t.Fatalf("Rows/Data mismatch at %d", i)
		}
	}
}

func TestBuildCDCDataProto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src := makeTestCDCData(t)
	got := buildCDCDataProto(src)

	compareCDCDataWithSource(t, src, got)
}

func TestCreateTsInsertWithCDCNodeForSingleMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodeIDs := []roachpb.NodeID{1}
	perNode := [][]cdcPayloadSeed{
		{
			{
				payload:    []byte("payload-1"),
				rowNum:     3,
				primaryTag: []byte("ptag-1"),
				rowBytes:   []byte("row-1"),
				startKey:   []byte("start-1"),
				endKey:     []byte("end-1"),
				valueSize:  11,
			},
			{
				payload:    []byte("payload-2"),
				rowNum:     5,
				primaryTag: []byte("ptag-2"),
				rowBytes:   []byte("row-2"),
				startKey:   []byte("start-2"),
				endKey:     []byte("end-2"),
				valueSize:  22,
			},
		},
	}

	n := makeTsInsertWithCDCNodeForTest(t, nodeIDs, perNode)
	plan, err := createTsInsertWithCDCNodeForSingleMode(n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(plan.Processors) != 1 {
		t.Fatalf("expected 1 processor, got %d", len(plan.Processors))
	}
	if len(plan.ResultRouters) != 1 {
		t.Fatalf("expected 1 result router, got %d", len(plan.ResultRouters))
	}
	if plan.GateNoopInput != 1 {
		t.Fatalf("expected GateNoopInput=1, got %d", plan.GateNoopInput)
	}
	if plan.TsOperator != execinfrapb.OperatorType_TsInsert {
		t.Fatalf("expected TsOperator=%v, got %v", execinfrapb.OperatorType_TsInsert, plan.TsOperator)
	}

	spec := plan.Processors[0].Spec.Core.TsInsertWithCDC
	if spec == nil {
		t.Fatal("TsInsertWithCDC spec is nil")
	}

	if !reflect.DeepEqual(spec.PayLoad, [][]byte{[]byte("payload-1"), []byte("payload-2")}) {
		t.Fatalf("unexpected PayLoad: %v", spec.PayLoad)
	}
	if !reflect.DeepEqual(spec.RowNums, []uint32{3, 5}) {
		t.Fatalf("unexpected RowNums: %v", spec.RowNums)
	}
	if !reflect.DeepEqual(spec.PrimaryTagKey, [][]byte{[]byte("ptag-1"), []byte("ptag-2")}) {
		t.Fatalf("unexpected PrimaryTagKey: %v", spec.PrimaryTagKey)
	}

	compareCDCDataWithSource(t, n.CDCData, spec.CDCData)
}

func TestCreateTsTsInsertWithCDCNodeForDistributeMode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodeIDs := []roachpb.NodeID{1, 2}
	perNode := [][]cdcPayloadSeed{
		{
			{
				payload:    []byte("prefix-1"),
				rowNum:     7,
				primaryTag: []byte("ptag-a"),
				rowBytes:   []byte("row-a"),
				startKey:   []byte("start-a"),
				endKey:     []byte("end-a"),
				valueSize:  33,
			},
		},
		{
			{
				payload:    []byte("prefix-2"),
				rowNum:     9,
				primaryTag: []byte("ptag-b"),
				rowBytes:   []byte("row-b"),
				startKey:   []byte("start-b"),
				endKey:     []byte("end-b"),
				valueSize:  44,
			},
		},
	}

	n := makeTsInsertWithCDCNodeForTest(t, nodeIDs, perNode)
	plan, err := createTsTsInsertWithCDCNodeForDistributeMode(n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(plan.Processors) != 2 {
		t.Fatalf("expected 2 processors, got %d", len(plan.Processors))
	}
	if len(plan.ResultRouters) != 2 {
		t.Fatalf("expected 2 result routers, got %d", len(plan.ResultRouters))
	}
	if plan.GateNoopInput != 2 {
		t.Fatalf("expected GateNoopInput=2, got %d", plan.GateNoopInput)
	}
	if plan.TsOperator != execinfrapb.OperatorType_TsInsert {
		t.Fatalf("expected TsOperator=%v, got %v", execinfrapb.OperatorType_TsInsert, plan.TsOperator)
	}

	spec0 := plan.Processors[0].Spec.Core.TsInsertWithCDC
	if spec0 == nil {
		t.Fatal("processor 0 TsInsertWithCDC spec is nil")
	}
	if !reflect.DeepEqual(spec0.RowNums, []uint32{7}) {
		t.Fatalf("unexpected processor 0 RowNums: %v", spec0.RowNums)
	}
	if !reflect.DeepEqual(spec0.PrimaryTagKey, [][]byte{[]byte("ptag-a")}) {
		t.Fatalf("unexpected processor 0 PrimaryTagKey: %v", spec0.PrimaryTagKey)
	}
	if !reflect.DeepEqual(spec0.PayloadPrefix, [][]byte{[]byte("prefix-1")}) {
		t.Fatalf("unexpected processor 0 PayloadPrefix: %v", spec0.PayloadPrefix)
	}
	if len(spec0.AllPayload) != 1 {
		t.Fatalf("expected processor 0 AllPayload len=1, got %d", len(spec0.AllPayload))
	}
	if !reflect.DeepEqual(spec0.AllPayload[0].Row, [][]byte{[]byte("row-a")}) {
		t.Fatalf("unexpected processor 0 AllPayload[0].Row: %v", spec0.AllPayload[0].Row)
	}
	if !reflect.DeepEqual(spec0.AllPayload[0].StartKey, []byte("start-a")) {
		t.Fatalf("unexpected processor 0 AllPayload[0].StartKey: %v", spec0.AllPayload[0].StartKey)
	}
	if !reflect.DeepEqual(spec0.AllPayload[0].EndKey, []byte("end-a")) {
		t.Fatalf("unexpected processor 0 AllPayload[0].EndKey: %v", spec0.AllPayload[0].EndKey)
	}
	if spec0.AllPayload[0].ValueSize != 33 {
		t.Fatalf("unexpected processor 0 AllPayload[0].ValueSize: %v", spec0.AllPayload[0].ValueSize)
	}
	compareCDCDataWithSource(t, n.CDCData, spec0.CDCData)

	spec1 := plan.Processors[1].Spec.Core.TsInsertWithCDC
	if spec1 == nil {
		t.Fatal("processor 1 TsInsertWithCDC spec is nil")
	}
	if !reflect.DeepEqual(spec1.RowNums, []uint32{9}) {
		t.Fatalf("unexpected processor 1 RowNums: %v", spec1.RowNums)
	}
	if !reflect.DeepEqual(spec1.PrimaryTagKey, [][]byte{[]byte("ptag-b")}) {
		t.Fatalf("unexpected processor 1 PrimaryTagKey: %v", spec1.PrimaryTagKey)
	}
	if !reflect.DeepEqual(spec1.PayloadPrefix, [][]byte{[]byte("prefix-2")}) {
		t.Fatalf("unexpected processor 1 PayloadPrefix: %v", spec1.PayloadPrefix)
	}
	if len(spec1.AllPayload) != 1 {
		t.Fatalf("expected processor 1 AllPayload len=1, got %d", len(spec1.AllPayload))
	}
	if !reflect.DeepEqual(spec1.AllPayload[0].Row, [][]byte{[]byte("row-b")}) {
		t.Fatalf("unexpected processor 1 AllPayload[0].Row: %v", spec1.AllPayload[0].Row)
	}
	if !reflect.DeepEqual(spec1.AllPayload[0].StartKey, []byte("start-b")) {
		t.Fatalf("unexpected processor 1 AllPayload[0].StartKey: %v", spec1.AllPayload[0].StartKey)
	}
	if !reflect.DeepEqual(spec1.AllPayload[0].EndKey, []byte("end-b")) {
		t.Fatalf("unexpected processor 1 AllPayload[0].EndKey: %v", spec1.AllPayload[0].EndKey)
	}
	if spec1.AllPayload[0].ValueSize != 44 {
		t.Fatalf("unexpected processor 1 AllPayload[0].ValueSize: %v", spec1.AllPayload[0].ValueSize)
	}
	compareCDCDataWithSource(t, n.CDCData, spec1.CDCData)
}
