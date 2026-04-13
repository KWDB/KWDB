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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/lib/pq/oid"
)

// TestBigEndianToLittleEndian tests the bigEndianToLittleEndian function
func TestBigEndianToLittleEndian(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: 8-byte value
	bigEndian8 := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	expected8 := []byte{0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00}
	result8 := bigEndianToLittleEndian(bigEndian8)
	if len(result8) != len(expected8) {
		t.Fatalf("expected length %d, got %d", len(expected8), len(result8))
	}
	for i, b := range expected8 {
		if result8[i] != b {
			t.Fatalf("expected byte %d to be %x, got %x", i, b, result8[i])
		}
	}

	// Test case 2: 4-byte value
	bigEndian4 := []byte{0x00, 0x01, 0x02, 0x03}
	expected4 := []byte{0x03, 0x02, 0x01, 0x00}
	result4 := bigEndianToLittleEndian(bigEndian4)
	if len(result4) != len(expected4) {
		t.Fatalf("expected length %d, got %d", len(expected4), len(result4))
	}
	for i, b := range expected4 {
		if result4[i] != b {
			t.Fatalf("expected byte %d to be %x, got %x", i, b, result4[i])
		}
	}

	// Test case 3: Empty slice
	empty := []byte{}
	resultEmpty := bigEndianToLittleEndian(empty)
	if len(resultEmpty) != 0 {
		t.Fatalf("expected empty slice, got length %d", len(resultEmpty))
	}
}

// TestFastParseInt tests the fastParseInt function
func TestFastParseInt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Positive integer
	in1, ok1 := fastParseInt("12345")
	if !ok1 || in1 != 12345 {
		t.Fatalf("expected 12345, got %d (ok: %v)", in1, ok1)
	}

	// Test case 2: Negative integer
	in2, ok2 := fastParseInt("-12345")
	if !ok2 || in2 != -12345 {
		t.Fatalf("expected -12345, got %d (ok: %v)", in2, ok2)
	}

	// Test case 3: Zero
	in3, ok3 := fastParseInt("0")
	if !ok3 || in3 != 0 {
		t.Fatalf("expected 0, got %d (ok: %v)", in3, ok3)
	}

	// Test case 4: Invalid input
	_, ok4 := fastParseInt("abc")
	if ok4 {
		t.Fatalf("expected ok to be false for invalid input, got %v", ok4)
	}

	// Test case 5: Empty string
	_, ok5 := fastParseInt("")
	if ok5 {
		t.Fatalf("expected ok to be false for empty string, got %v", ok5)
	}

	// Test case 6: Just a sign
	_, ok6 := fastParseInt("-")
	if ok6 {
		t.Fatalf("expected ok to be false for just a sign, got %v", ok6)
	}
}

// TestParseIntCommon tests the parseIntCommon function
func TestParseIntCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	column := &sqlbase.ColumnDescriptor{
		Name: "test_col",
		Type: *types.Int,
	}

	// Test case 1: Valid integer
	in1, dat1, err1 := parseIntCommon(column, parser.NORMALTYPE, "12345")
	if err1 != nil {
		t.Fatalf("expected no error, got %v", err1)
	}
	if in1 != 12345 {
		t.Fatalf("expected 12345, got %d", in1)
	}
	if dat1 != nil {
		t.Fatalf("expected dat to be nil, got %v", dat1)
	}

	// Test case 2: True boolean
	in2, dat2, err2 := parseIntCommon(column, parser.NORMALTYPE, "true")
	if err2 != nil {
		t.Fatalf("expected no error, got %v", err2)
	}
	if in2 != 0 {
		t.Fatalf("expected in to be 0, got %d", in2)
	}
	if dat2 == nil {
		t.Fatalf("expected dat to be non-nil")
	}
	if *dat2.(*tree.DInt) != 1 {
		t.Fatalf("expected dat to be 1, got %v", *dat2.(*tree.DInt))
	}

	// Test case 3: False boolean
	in3, dat3, err3 := parseIntCommon(column, parser.NORMALTYPE, "false")
	if err3 != nil {
		t.Fatalf("expected no error, got %v", err3)
	}
	if in3 != 0 {
		t.Fatalf("expected in to be 0, got %d", in3)
	}
	if dat3 == nil {
		t.Fatalf("expected dat to be non-nil")
	}
	if *dat3.(*tree.DInt) != 0 {
		t.Fatalf("expected dat to be 0, got %v", *dat3.(*tree.DInt))
	}
}

// TestGetConverterByOid tests the GetConverterByOid function
func TestGetConverterByOid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: TimestampTZ
	conv1 := GetConverterByOid(oid.T_timestamptz)
	if conv1 == nil {
		t.Fatal("expected converter for timestamp with time zone, got nil")
	}

	// Test case 2: Integer
	conv2 := GetConverterByOid(oid.T_int8)
	if conv2 == nil {
		t.Fatal("expected converter for int8, got nil")
	}

	// Test case 3: String
	conv3 := GetConverterByOid(oid.T_varchar)
	if conv3 == nil {
		t.Fatal("expected converter for varchar, got nil")
	}

	// Test case 4: Unsupported type
	conv4 := GetConverterByOid(oid.T_unknown)
	if conv4 == nil {
		t.Fatal("expected converter for unknown type, got nil")
	}
}

// TestBuildColConverters tests the BuildColConverters function
func TestBuildColConverters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cols := []*sqlbase.ColumnDescriptor{
		{
			Name: "col1",
			Type: *types.Int,
		},
		{
			Name: "col2",
			Type: *types.String,
		},
		{
			Name: "col3",
			Type: *types.TimestampTZ,
		},
	}

	converters := BuildColConverters(cols)
	if len(converters) != len(cols) {
		t.Fatalf("expected %d converters, got %d", len(cols), len(converters))
	}

	for i, conv := range converters {
		if conv == nil {
			t.Fatalf("expected converter for column %d, got nil", i)
		}
	}
}

// TestComputeColumnSize tests the computeColumnSize function
func TestComputeColumnSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cols := []*sqlbase.ColumnDescriptor{
		{
			Name: "col1",
			Type: *types.Int,
			TsCol: sqlbase.TSCol{
				StorageLen: 8,
			},
		},
		{
			Name: "col2",
			Type: *types.String,
			TsCol: sqlbase.TSCol{
				StorageLen:         255,
				VariableLengthType: sqlbase.StorageTuple,
			},
		},
		{
			Name: "col3",
			Type: *types.Float,
			TsCol: sqlbase.TSCol{
				StorageLen: 8,
			},
		},
	}

	colSize, preAllocSize, err := computeColumnSize(&cols)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if colSize <= 0 {
		t.Fatalf("expected positive column size, got %d", colSize)
	}

	if preAllocSize < 0 {
		t.Fatalf("expected non-negative pre-allocation size, got %d", preAllocSize)
	}
}

// TestGetSingleDatum tests the GetSingleDatum function
func TestGetSingleDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ptCtx := tree.NewParseTimeContext(time.Now())
	// Test case 1: Integer
	intCol := sqlbase.ColumnDescriptor{
		Name: "int_col",
		Type: *types.Int,
	}
	datum1, err1 := GetSingleDatum(ptCtx, intCol, parser.NUMTYPE, "12345")
	if err1 != nil {
		t.Fatalf("expected no error for integer, got %v", err1)
	}
	if datum1 == nil {
		t.Fatal("expected datum for integer, got nil")
	}

	// Test case 2: String
	stringCol := sqlbase.ColumnDescriptor{
		Name: "string_col",
		Type: *types.String,
	}
	datum2, err2 := GetSingleDatum(ptCtx, stringCol, parser.STRINGTYPE, "test")
	if err2 != nil {
		t.Fatalf("expected no error for string, got %v", err2)
	}
	if datum2 == nil {
		t.Fatal("expected datum for string, got nil")
	}

	// Test case 3: Timestamp
	timestampCol := sqlbase.ColumnDescriptor{
		Name: "timestamp_col",
		Type: *types.TimestampTZ,
	}
	datum3, err3 := GetSingleDatum(ptCtx, timestampCol, parser.STRINGTYPE, "2023-01-01 00:00:00")
	if err3 != nil {
		t.Fatalf("expected no error for timestamp, got %v", err3)
	}
	if datum3 == nil {
		t.Fatal("expected datum for timestamp, got nil")
	}
}

// TestGetPrepareInputValues tests the getPrepareInputValues function
func TestGetPrepareInputValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ptCtx := tree.NewParseTimeContext(time.Now())
	bindCmd := &BindStmt{
		Args: [][]byte{
			[]byte("1"),
			[]byte("test"),
			[]byte("2"),
			[]byte("test2"),
		},
		ArgFormatCodes: []pgwirebase.FormatCode{pgwirebase.FormatText, pgwirebase.FormatText, pgwirebase.FormatText, pgwirebase.FormatText},
	}

	inferredTypes := []oid.Oid{oid.T_int8, oid.T_varchar, oid.T_int8, oid.T_varchar}
	di := &DirectInsert{
		RowNum: 4,
		ColNum: 2,
	}

	outputValues, err := getPrepareInputValues(ptCtx, bindCmd, inferredTypes, di)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(outputValues) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(outputValues))
	}

	for i, row := range outputValues {
		if len(row) != 2 {
			t.Fatalf("expected 2 columns in row %d, got %d", i, len(row))
		}
	}
}

// TestGetTSColumnByName tests the GetTSColumnByName function
func TestGetTSColumnByName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cols := []sqlbase.ColumnDescriptor{
		{
			ID:   1,
			Name: "col1",
			Type: *types.Int,
		},
		{
			ID:   2,
			Name: "col2",
			Type: *types.String,
		},
	}

	// Test case 1: Existing column
	col, err := GetTSColumnByName("col1", cols)
	if err != nil {
		t.Fatalf("expected no error for existing column, got %v", err)
	}
	if col == nil {
		t.Fatal("expected column for existing name, got nil")
	}
	if col.Name != "col1" {
		t.Fatalf("expected column name 'col1', got '%s'", col.Name)
	}

	// Test case 2: Non-existing column
	col2, err2 := GetTSColumnByName("col3", cols)
	if err2 == nil {
		t.Fatal("expected error for non-existing column, got nil")
	}
	if col2 != nil {
		t.Fatalf("expected nil for non-existing column, got %v", col2)
	}
}

// TestNumofInsertDirect tests the NumofInsertDirect function
func TestNumofInsertDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ins := &tree.Insert{
		Columns:    []tree.Name{"col1", "col2"},
		IsNoSchema: false,
	}

	colsDesc := []sqlbase.ColumnDescriptor{
		{
			ID:   1,
			Name: "col1",
			Type: *types.Int,
		},
		{
			ID:   2,
			Name: "col2",
			Type: *types.String,
		},
		{
			ID:   3,
			Name: "col3",
			Type: *types.Float,
		},
	}

	stmt := parser.Statement{
		Insertdirectstmt: parser.Insertdirectstmt{
			RowsAffected: 4,
			InsertValues: []string{"1", "test", "2", "test2"},
		},
	}

	stmts := parser.Statements{stmt}
	di := &DirectInsert{}

	result := NumofInsertDirect(ins, &colsDesc, stmts, di)
	if result != 4 {
		t.Fatalf("expected 4 insert values, got %d", result)
	}
	if di.ColNum != 2 {
		t.Fatalf("expected 2 columns, got %d", di.ColNum)
	}
	if di.RowNum != 4 {
		t.Fatalf("expected 4 rows, got %d", di.RowNum)
	}
}
