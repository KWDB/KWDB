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

package stats

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

func makeTestTableStatisticProto() *TableStatisticProto {
	return &TableStatisticProto{
		TableID:       sqlbase.ID(123),
		StatisticID:   456,
		Name:          "auto_stat",
		ColumnIDs:     []sqlbase.ColumnID{1, 2, 3},
		CreatedAt:     time.Date(2026, 3, 25, 10, 30, 0, 0, time.UTC),
		RowCount:      1000,
		DistinctCount: 100,
		NullCount:     10,
	}
}

func TestTableStatisticProto_MarshalUnmarshal_RoundTrip(t *testing.T) {
	orig := makeTestTableStatisticProto()

	data, err := protoutil.Marshal(orig)
	if err != nil {
		t.Fatalf("protoutil.Marshal() error: %v", err)
	}

	var decoded TableStatisticProto
	if err := protoutil.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("protoutil.Unmarshal() error: %v", err)
	}

	if orig.TableID != decoded.TableID {
		t.Fatalf("TableID mismatch: got %v want %v", decoded.TableID, orig.TableID)
	}
	if orig.StatisticID != decoded.StatisticID {
		t.Fatalf("StatisticID mismatch: got %v want %v", decoded.StatisticID, orig.StatisticID)
	}
	if orig.Name != decoded.Name {
		t.Fatalf("Name mismatch: got %q want %q", decoded.Name, orig.Name)
	}
	if !reflect.DeepEqual(orig.ColumnIDs, decoded.ColumnIDs) {
		t.Fatalf("ColumnIDs mismatch: got %v want %v", decoded.ColumnIDs, orig.ColumnIDs)
	}
	if !orig.CreatedAt.Equal(decoded.CreatedAt) {
		t.Fatalf("CreatedAt mismatch: got %v want %v", decoded.CreatedAt, orig.CreatedAt)
	}
	if orig.RowCount != decoded.RowCount {
		t.Fatalf("RowCount mismatch: got %v want %v", decoded.RowCount, orig.RowCount)
	}
	if orig.DistinctCount != decoded.DistinctCount {
		t.Fatalf("DistinctCount mismatch: got %v want %v", decoded.DistinctCount, orig.DistinctCount)
	}
	if orig.NullCount != decoded.NullCount {
		t.Fatalf("NullCount mismatch: got %v want %v", decoded.NullCount, orig.NullCount)
	}
}

func TestTableStatisticProto_SizeMatchesMarshalLength(t *testing.T) {
	m := makeTestTableStatisticProto()

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	if got, want := len(data), m.Size(); got != want {
		t.Fatalf("marshal length != Size(): got %d want %d", got, want)
	}
}

func TestTableStatisticProto_MarshalToMatchesMarshal(t *testing.T) {
	m := makeTestTableStatisticProto()

	data1, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo() error: %v", err)
	}
	data2 := buf[:n]

	if !bytes.Equal(data1, data2) {
		t.Fatalf("Marshal() and MarshalTo() differ:\nmarshal=%v\nmarshalTo=%v", data1, data2)
	}
}

func TestTableStatisticProto_Unmarshal_UnexpectedEOF(t *testing.T) {
	m := makeTestTableStatisticProto()

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if len(data) < 2 {
		t.Fatalf("encoded data too short for truncation test")
	}

	truncated := data[:len(data)-1]

	var decoded TableStatisticProto
	err = protoutil.Unmarshal(truncated, &decoded)
	if err == nil {
		t.Fatal("expected error for truncated input, got nil")
	}
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestTableStatisticProto_Unmarshal_WrongWireType(t *testing.T) {
	// field 1 (TableID) The correct wire type should be 0. Here, it is deliberately written as 2
	// tag = (1 << 3) | 2 = 0x0a
	data := []byte{
		0x0a, // wrong wire type for field 1
		0x01, // length
		0x01, // payload
	}

	var decoded TableStatisticProto
	err := protoutil.Unmarshal(data, &decoded)
	if err == nil {
		t.Fatal("expected wrong wire type error, got nil")
	}
}

func TestTableStatisticProto_Unmarshal_PackedColumnIDs(t *testing.T) {
	m := &TableStatisticProto{
		TableID:     sqlbase.ID(1),
		ColumnIDs:   []sqlbase.ColumnID{10, 20, 30},
		CreatedAt:   time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		RowCount:    1,
		NullCount:   0,
		Name:        "packed-cols",
		StatisticID: 99,
	}

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var decoded TableStatisticProto
	if err := protoutil.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if !reflect.DeepEqual(decoded.ColumnIDs, m.ColumnIDs) {
		t.Fatalf("ColumnIDs mismatch: got %v want %v", decoded.ColumnIDs, m.ColumnIDs)
	}
}

func TestSkipTableStatistic_ValidInput(t *testing.T) {
	m := makeTestTableStatisticProto()

	data, err := protoutil.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	n, err := skipTableStatistic(data)
	if err != nil {
		t.Fatalf("skipTableStatistic() error: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive skip length, got %d", n)
	}
}

func TestSkipTableStatistic_UnexpectedEOF(t *testing.T) {
	data := []byte{0x80}

	_, err := skipTableStatistic(data)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected %v, got %v", io.ErrUnexpectedEOF, err)
	}
}
