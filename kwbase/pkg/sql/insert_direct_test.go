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
	"context"
	"encoding/binary"
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
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

	ptCtx := tree.NewParseTimeContext(timeutil.Now())
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

	ptCtx := tree.NewParseTimeContext(timeutil.Now())
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

// TestAllConvertFunctions tests all conversion functions in insert_direct.go
func TestAllConvertFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ptCtx := tree.NewParseTimeContext(timeutil.Now())

	// Create test column descriptors for different types
	timestampCol := &sqlbase.ColumnDescriptor{
		ID:   1,
		Name: "timestamp_col",
		Type: *types.Timestamp,
	}

	// timestamptzCol := &sqlbase.ColumnDescriptor{
	// 	ID:   2,
	// 	Name: "timestamptz_col",
	// 	Type: *types.TimestampTZ,
	// }

	int4Col := &sqlbase.ColumnDescriptor{
		ID:   3,
		Name: "int4_col",
		Type: *types.Int4,
	}

	int2Col := &sqlbase.ColumnDescriptor{
		ID:   4,
		Name: "int2_col",
		Type: *types.Int2,
	}

	nstringCol := &sqlbase.ColumnDescriptor{
		ID:   5,
		Name: "nstring_col",
		Type: *types.NVarChar,
	}

	bytesCol := &sqlbase.ColumnDescriptor{
		ID:   6,
		Name: "bytes_col",
		Type: *types.VarBytes,
	}

	floatCol := &sqlbase.ColumnDescriptor{
		ID:   7,
		Name: "float_col",
		Type: *types.Float4,
	}

	boolCol := &sqlbase.ColumnDescriptor{
		ID:   8,
		Name: "bool_col",
		Type: *types.Bool,
	}

	geometryCol := &sqlbase.ColumnDescriptor{
		ID:   9,
		Name: "geometry_col",
		Type: *types.Geometry,
	}

	// Test convertTimestampString
	t.Run("convertTimestampString", func(t *testing.T) {
		// Test valid timestamp string
		datum, err := convertTimestampString(timestampCol, "2023-01-01 12:00:00")
		if err != nil {
			t.Fatalf("expected no error for valid timestamp, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid timestamp, got nil")
		}

		// Test invalid timestamp string
		_, err = convertTimestampString(timestampCol, "invalid-timestamp")
		if err == nil {
			t.Fatal("expected error for invalid timestamp, got nil")
		}
	})

	// Test convertTimestampNumeric
	t.Run("convertTimestampNumeric", func(t *testing.T) {
		// Test valid numeric timestamp
		datum, err := convertTimestampNumeric(timestampCol, "1672560000000", parser.NORMALTYPE)
		if err != nil {
			t.Fatalf("expected no error for valid numeric timestamp, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid numeric timestamp, got nil")
		}

		// Test "now" keyword
		datum, err = convertTimestampNumeric(timestampCol, "now", parser.NORMALTYPE)
		if err != nil {
			t.Fatalf("expected no error for 'now' keyword, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for 'now' keyword, got nil")
		}

		// Test invalid numeric timestamp
		_, err = convertTimestampNumeric(timestampCol, "invalid", parser.NORMALTYPE)
		if err == nil {
			t.Fatal("expected error for invalid numeric timestamp, got nil")
		}
	})

	// Test convertInt4
	t.Run("convertInt4", func(t *testing.T) {
		// Test valid int4 value
		datum, err := convertInt4(ptCtx, int4Col, parser.NUMTYPE, "123")
		if err != nil {
			t.Fatalf("expected no error for valid int4, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid int4, got nil")
		}

		// Test valid int4 value at upper limit
		datum, err = convertInt4(ptCtx, int4Col, parser.NUMTYPE, "2147483647")
		if err != nil {
			t.Fatalf("expected no error for int4 upper limit, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for int4 upper limit, got nil")
		}

		// Test valid int4 value at lower limit
		datum, err = convertInt4(ptCtx, int4Col, parser.NUMTYPE, "-2147483648")
		if err != nil {
			t.Fatalf("expected no error for int4 lower limit, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for int4 lower limit, got nil")
		}

		// Test out of range int4 value
		_, err = convertInt4(ptCtx, int4Col, parser.NUMTYPE, "2147483648")
		if err == nil {
			t.Fatal("expected error for out of range int4, got nil")
		}

		// Test string type (should error)
		_, err = convertInt4(ptCtx, int4Col, parser.STRINGTYPE, "123")
		if err == nil {
			t.Fatal("expected error for string type in int4 conversion, got nil")
		}
	})

	// Test convertInt2
	t.Run("convertInt2", func(t *testing.T) {
		// Test valid int2 value
		datum, err := convertInt2(ptCtx, int2Col, parser.NUMTYPE, "123")
		if err != nil {
			t.Fatalf("expected no error for valid int2, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid int2, got nil")
		}

		// Test valid int2 value at upper limit
		datum, err = convertInt2(ptCtx, int2Col, parser.NUMTYPE, "32767")
		if err != nil {
			t.Fatalf("expected no error for int2 upper limit, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for int2 upper limit, got nil")
		}

		// Test valid int2 value at lower limit
		datum, err = convertInt2(ptCtx, int2Col, parser.NUMTYPE, "-32768")
		if err != nil {
			t.Fatalf("expected no error for int2 lower limit, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for int2 lower limit, got nil")
		}

		// Test out of range int2 value
		_, err = convertInt2(ptCtx, int2Col, parser.NUMTYPE, "32768")
		if err == nil {
			t.Fatal("expected error for out of range int2, got nil")
		}

		// Test string type (should error)
		_, err = convertInt2(ptCtx, int2Col, parser.STRINGTYPE, "123")
		if err == nil {
			t.Fatal("expected error for string type in int2 conversion, got nil")
		}
	})

	// Test convertNString
	t.Run("convertNString", func(t *testing.T) {
		// Test valid nstring value
		datum, err := convertNString(ptCtx, nstringCol, parser.STRINGTYPE, "test")
		if err != nil {
			t.Fatalf("expected no error for valid nstring, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid nstring, got nil")
		}

		// Test numeric type (should error)
		_, err = convertNString(ptCtx, nstringCol, parser.NUMTYPE, "123")
		if err == nil {
			t.Fatal("expected error for numeric type in nstring conversion, got nil")
		}

		// Test normal type (should error)
		_, err = convertNString(ptCtx, nstringCol, parser.NORMALTYPE, "test")
		if err == nil {
			t.Fatal("expected error for normal type in nstring conversion, got nil")
		}
	})

	// Test convertBytes
	t.Run("convertBytes", func(t *testing.T) {
		// Test valid bytes value
		datum, err := convertBytes(ptCtx, bytesCol, parser.STRINGTYPE, "test")
		if err != nil {
			t.Fatalf("expected no error for valid bytes, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid bytes, got nil")
		}

		// Test numeric type (should error)
		_, err = convertBytes(ptCtx, bytesCol, parser.NUMTYPE, "123")
		if err == nil {
			t.Fatal("expected error for numeric type in bytes conversion, got nil")
		}

		// Test normal type (should error)
		_, err = convertBytes(ptCtx, bytesCol, parser.NORMALTYPE, "test")
		if err == nil {
			t.Fatal("expected error for normal type in bytes conversion, got nil")
		}
	})

	// Test convertFloat
	t.Run("convertFloat", func(t *testing.T) {
		// Test valid float value
		datum, err := convertFloat(ptCtx, floatCol, parser.NUMTYPE, "123.45")
		if err != nil {
			t.Fatalf("expected no error for valid float, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid float, got nil")
		}

		// Test valid integer as float
		datum, err = convertFloat(ptCtx, floatCol, parser.NUMTYPE, "123")
		if err != nil {
			t.Fatalf("expected no error for integer as float, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for integer as float, got nil")
		}

		// Test string type (should error)
		_, err = convertFloat(ptCtx, floatCol, parser.STRINGTYPE, "123.45")
		if err == nil {
			t.Fatal("expected error for string type in float conversion, got nil")
		}

		// Test invalid float value
		_, err = convertFloat(ptCtx, floatCol, parser.NUMTYPE, "invalid")
		if err == nil {
			t.Fatal("expected error for invalid float, got nil")
		}
	})

	// Test convertBool
	t.Run("convertBool", func(t *testing.T) {
		// Test valid boolean value "true"
		datum, err := convertBool(ptCtx, boolCol, parser.STRINGTYPE, "true")
		if err != nil {
			t.Fatalf("expected no error for 'true' boolean, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for 'true' boolean, got nil")
		}

		// Test valid boolean value "false"
		datum, err = convertBool(ptCtx, boolCol, parser.STRINGTYPE, "false")
		if err != nil {
			t.Fatalf("expected no error for 'false' boolean, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for 'false' boolean, got nil")
		}

		// Test valid boolean value "1"
		datum, err = convertBool(ptCtx, boolCol, parser.NUMTYPE, "1")
		if err != nil {
			t.Fatalf("expected no error for '1' boolean, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for '1' boolean, got nil")
		}

		// Test valid boolean value "0"
		datum, err = convertBool(ptCtx, boolCol, parser.NUMTYPE, "0")
		if err != nil {
			t.Fatalf("expected no error for '0' boolean, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for '0' boolean, got nil")
		}

		// Test normal type (should error)
		_, err = convertBool(ptCtx, boolCol, parser.NORMALTYPE, "true")
		if err == nil {
			t.Fatal("expected error for normal type in bool conversion, got nil")
		}

		// Test invalid boolean value
		_, err = convertBool(ptCtx, boolCol, parser.STRINGTYPE, "invalid")
		if err == nil {
			t.Fatal("expected error for invalid boolean, got nil")
		}
	})

	// Test convertGeometry
	t.Run("convertGeometry", func(t *testing.T) {
		// Test valid geometry value
		datum, err := convertGeometry(ptCtx, geometryCol, parser.STRINGTYPE, "POINT(1 1)")
		if err != nil {
			t.Fatalf("expected no error for valid geometry, got %v", err)
		}
		if datum == nil {
			t.Fatal("expected datum for valid geometry, got nil")
		}

		// Test normal type (should error)
		_, err = convertGeometry(ptCtx, geometryCol, parser.NORMALTYPE, "POINT(1 1)")
		if err == nil {
			t.Fatal("expected error for normal type in geometry conversion, got nil")
		}

		// Test numeric type (should error)
		_, err = convertGeometry(ptCtx, geometryCol, parser.NUMTYPE, "123")
		if err == nil {
			t.Fatal("expected error for numeric type in geometry conversion, got nil")
		}

		// Test invalid geometry value
		_, err = convertGeometry(ptCtx, geometryCol, parser.STRINGTYPE, "invalid")
		if err == nil {
			t.Fatal("expected error for invalid geometry, got nil")
		}
	})

	// Test convertInt2Value
	t.Run("convertInt2Value", func(t *testing.T) {
		// Test valid int2 value (using binary format, not string)
		// Create a byte slice representing a valid int2 value (e.g., 123 in binary)
		validInt2 := make([]byte, 2)
		binary.BigEndian.PutUint16(validInt2, 123)
		args := [][]byte{validInt2}
		ok, err := convertInt2Value(args, 0, int2Col)
		if err != nil {
			t.Fatalf("expected no error for valid int2 value, got %v", err)
		}
		if ok {
			t.Fatal("expected ok to be false for valid int2 value, got true")
		}

		// Test invalid int2 value (out of range)
		// Create a byte slice representing a value outside int16 range
		invalidInt2 := make([]byte, 2)
		binary.BigEndian.PutUint16(invalidInt2, 32768) // This is outside int16 range
		args = [][]byte{invalidInt2}
		ok, err = convertInt2Value(args, 0, int2Col)
		if err == nil {
			t.Fatal("expected error for invalid int2 value, got nil")
		}
		if !ok {
			t.Fatal("expected ok to be true for invalid int2 value, got false")
		}
	})
}

// TestAllFormatFunctions tests all format functions in insert_direct.go
func TestAllFormatFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ptCtx := tree.NewParseTimeContext(timeutil.Now())

	// Create test column descriptors for different types
	timestampCol := &sqlbase.ColumnDescriptor{
		ID:   1,
		Name: "timestamp_col",
		Type: *types.Timestamp,
	}

	timestamptzCol := &sqlbase.ColumnDescriptor{
		ID:   2,
		Name: "timestamptz_col",
		Type: *types.TimestampTZ,
	}

	int4Col := &sqlbase.ColumnDescriptor{
		ID:   3,
		Name: "int4_col",
		Type: *types.Int4,
	}

	int2Col := &sqlbase.ColumnDescriptor{
		ID:   4,
		Name: "int2_col",
		Type: *types.Int2,
	}

	float8Col := &sqlbase.ColumnDescriptor{
		ID:   5,
		Name: "float8_col",
		Type: *types.Float4,
	}

	float4Col := &sqlbase.ColumnDescriptor{
		ID:   6,
		Name: "float4_col",
		Type: *types.Float4,
	}

	boolCol := &sqlbase.ColumnDescriptor{
		ID:   7,
		Name: "bool_col",
		Type: *types.Bool,
	}

	stringCol := &sqlbase.ColumnDescriptor{
		ID:   8,
		Name: "string_col",
		Type: *types.String,
	}

	// Test timeFormatText
	t.Run("timeFormatText", func(t *testing.T) {
		// Test valid timestamp text
		args := [][]byte{[]byte("2023-01-01 12:00:00")}
		var rowTimestamps []int64
		err := timeFormatText(ptCtx, args, 0, oid.T_timestamp, timestampCol, true, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid timestamp text, got %v", err)
		}
		if len(args[0]) != 8 {
			t.Fatalf("expected 8 bytes for timestamp, got %d", len(args[0]))
		}
		if len(rowTimestamps) != 1 {
			t.Fatalf("expected 1 row timestamp, got %d", len(rowTimestamps))
		}

		// Test valid timestamptz text
		args = [][]byte{[]byte("2023-01-01 12:00:00")}
		rowTimestamps = []int64{}
		err = timeFormatText(ptCtx, args, 0, oid.T_timestamptz, timestamptzCol, true, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid timestamptz text, got %v", err)
		}
		if len(args[0]) != 8 {
			t.Fatalf("expected 8 bytes for timestamptz, got %d", len(args[0]))
		}
		if len(rowTimestamps) != 1 {
			t.Fatalf("expected 1 row timestamp, got %d", len(rowTimestamps))
		}

		// Test invalid timestamp text
		args = [][]byte{[]byte("invalid-timestamp")}
		rowTimestamps = []int64{}
		err = timeFormatText(ptCtx, args, 0, oid.T_timestamp, timestampCol, true, &rowTimestamps)
		if err == nil {
			t.Fatal("expected error for invalid timestamp text, got nil")
		}
	})

	// Test intFormatText
	t.Run("intFormatText", func(t *testing.T) {
		// Test valid int4 text
		args := [][]byte{[]byte("123")}
		err := intFormatText(args, 0, oid.T_int4, int4Col)
		if err != nil {
			t.Fatalf("expected no error for valid int4 text, got %v", err)
		}
		if len(args[0]) != 4 {
			t.Fatalf("expected 4 bytes for int4, got %d", len(args[0]))
		}

		// Test valid int2 text
		args = [][]byte{[]byte("123")}
		err = intFormatText(args, 0, oid.T_int2, int2Col)
		if err != nil {
			t.Fatalf("expected no error for valid int2 text, got %v", err)
		}
		if len(args[0]) != 2 {
			t.Fatalf("expected 2 bytes for int2, got %d", len(args[0]))
		}

		// Test out of range int2 text
		args = [][]byte{[]byte("32768")}
		err = intFormatText(args, 0, oid.T_int2, int2Col)
		if err == nil {
			t.Fatal("expected error for out of range int2 text, got nil")
		}
	})

	// Test floatFormatText
	t.Run("floatFormatText", func(t *testing.T) {
		// Test valid float8 text
		args := [][]byte{[]byte("123.45")}
		err := floatFormatText(args, 0, oid.T_float8, float8Col)
		if err != nil {
			t.Fatalf("expected no error for valid float8 text, got %v", err)
		}
		if len(args[0]) != 8 {
			t.Fatalf("expected 8 bytes for float8, got %d", len(args[0]))
		}

		// Test valid float4 text
		args = [][]byte{[]byte("123.45")}
		err = floatFormatText(args, 0, oid.T_float4, float4Col)
		if err != nil {
			t.Fatalf("expected no error for valid float4 text, got %v", err)
		}
		if len(args[0]) != 4 {
			t.Fatalf("expected 4 bytes for float4, got %d", len(args[0]))
		}

		// Test invalid float text
		args = [][]byte{[]byte("invalid")}
		err = floatFormatText(args, 0, oid.T_float8, float8Col)
		if err == nil {
			t.Fatal("expected error for invalid float text, got nil")
		}
	})

	// Test charFormatText
	t.Run("charFormatText", func(t *testing.T) {
		// Test valid string text
		args := [][]byte{[]byte("test")}
		var rowTimestamps []int64
		err := charFormatText(ptCtx, args, 0, stringCol, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid string text, got %v", err)
		}

		// Test valid boolean text
		args = [][]byte{[]byte("true")}
		err = charFormatText(ptCtx, args, 0, boolCol, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid boolean text, got %v", err)
		}
		if len(args[0]) != 1 {
			t.Fatalf("expected 1 byte for boolean, got %d", len(args[0]))
		}

		// Test valid timestamp text
		args = [][]byte{[]byte("2023-01-01 12:00:00")}
		rowTimestamps = []int64{}
		err = charFormatText(ptCtx, args, 0, timestampCol, true, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid timestamp text, got %v", err)
		}
		if len(args[0]) != 8 {
			t.Fatalf("expected 8 bytes for timestamp, got %d", len(args[0]))
		}
		if len(rowTimestamps) != 1 {
			t.Fatalf("expected 1 row timestamp, got %d", len(rowTimestamps))
		}
	})

	// Test boolFormatText
	t.Run("boolFormatText", func(t *testing.T) {
		// Test valid boolean text "true"
		args := [][]byte{[]byte("true")}
		err := boolFormatText(args, 0)
		if err != nil {
			t.Fatalf("expected no error for valid boolean text 'true', got %v", err)
		}
		if len(args[0]) != 1 {
			t.Fatalf("expected 1 byte for boolean, got %d", len(args[0]))
		}
		if args[0][0] != 1 {
			t.Fatalf("expected 1 for 'true', got %d", args[0][0])
		}

		// Test valid boolean text "false"
		args = [][]byte{[]byte("false")}
		err = boolFormatText(args, 0)
		if err != nil {
			t.Fatalf("expected no error for valid boolean text 'false', got %v", err)
		}
		if len(args[0]) != 1 {
			t.Fatalf("expected 1 byte for boolean, got %d", len(args[0]))
		}
		if args[0][0] != 0 {
			t.Fatalf("expected 0 for 'false', got %d", args[0][0])
		}

		// Test invalid boolean text
		args = [][]byte{[]byte("invalid")}
		err = boolFormatText(args, 0)
		if err == nil {
			t.Fatal("expected error for invalid boolean text, got nil")
		}
	})

	// Test intFormatBinary
	t.Run("intFormatBinary", func(t *testing.T) {
		// Test valid int4 binary
		validInt4 := make([]byte, 4)
		binary.BigEndian.PutUint32(validInt4, 123)
		args := [][]byte{validInt4}
		var rowTimestamps []int64
		err := intFormatBinary(args, 0, int4Col, oid.T_int4, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid int4 binary, got %v", err)
		}

		// Test valid int2 binary
		validInt2 := make([]byte, 2)
		binary.BigEndian.PutUint16(validInt2, 123)
		args = [][]byte{validInt2}
		err = intFormatBinary(args, 0, int2Col, oid.T_int2, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid int2 binary, got %v", err)
		}
	})

	// Test float8FormatBinary
	t.Run("float8FormatBinary", func(t *testing.T) {
		// Test valid float8 binary
		validFloat8 := make([]byte, 8)
		binary.BigEndian.PutUint64(validFloat8, math.Float64bits(123.45))
		args := [][]byte{validFloat8}
		err := float8FormatBinary(args, 0, float8Col)
		if err != nil {
			t.Fatalf("expected no error for valid float8 binary, got %v", err)
		}

		// Test valid float4 binary (converting to float4)
		validFloat4 := make([]byte, 8)
		binary.BigEndian.PutUint64(validFloat4, math.Float64bits(123.45))
		args = [][]byte{validFloat4}
		err = float8FormatBinary(args, 0, float4Col)
		if err != nil {
			t.Fatalf("expected no error for valid float4 binary, got %v", err)
		}
		if len(args[0]) != 4 {
			t.Fatalf("expected 4 bytes for float4, got %d", len(args[0]))
		}
	})

	// Test float4FormatBinary
	t.Run("float4FormatBinary", func(t *testing.T) {
		// Test valid float4 binary
		validFloat4 := make([]byte, 4)
		binary.BigEndian.PutUint32(validFloat4, math.Float32bits(123.45))
		args := [][]byte{validFloat4}
		err := float4FormatBinary(args, 0, float4Col)
		if err != nil {
			t.Fatalf("expected no error for valid float4 binary, got %v", err)
		}

		// Test valid float4 binary (converting to float8)
		validFloat4 = make([]byte, 4)
		binary.BigEndian.PutUint32(validFloat4, math.Float32bits(123.45))
		args = [][]byte{validFloat4}
		err = float4FormatBinary(args, 0, float8Col)
		if err != nil {
			t.Fatalf("expected no error for valid float4 binary, got %v", err)
		}
		if len(args[0]) != 4 {
			t.Fatalf("expected 4 bytes for float4, got %d", len(args[0]))
		}
	})

	// Test charFormatBinary
	t.Run("charFormatBinary", func(t *testing.T) {
		// Test valid string binary
		args := [][]byte{[]byte("test")}
		var rowTimestamps []int64
		err := charFormatBinary(args, 0, stringCol, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid string binary, got %v", err)
		}

		// Test valid boolean binary
		args = [][]byte{[]byte("true")}
		err = charFormatBinary(args, 0, boolCol, false, &rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for valid boolean binary, got %v", err)
		}
	})
}

// TestAllDirectInsertFunctions tests all direct insert functions in insert_direct.go
func TestAllDirectInsertFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ptCtx := tree.NewParseTimeContext(timeutil.Now())
	ctx := context.Background()

	// Create test column descriptors for different types
	timestampCol := &sqlbase.ColumnDescriptor{
		ID:   1,
		Name: "timestamp_col",
		Type: *types.Timestamp,
		TsCol: sqlbase.TSCol{
			StorageLen: 8,
		},
	}

	int4Col := &sqlbase.ColumnDescriptor{
		ID:   2,
		Name: "int4_col",
		Type: *types.Int4,
		TsCol: sqlbase.TSCol{
			StorageLen: 4,
		},
	}

	stringCol := &sqlbase.ColumnDescriptor{
		ID:   3,
		Name: "string_col",
		Type: *types.String,
		TsCol: sqlbase.TSCol{
			StorageLen:         255,
			VariableLengthType: sqlbase.StorageTuple,
		},
	}

	// Create test table descriptor
	tableDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 1,
			Columns: []sqlbase.ColumnDescriptor{
				*timestampCol,
				*int4Col,
				*stringCol,
			},
		},
	}

	// Test preAllocateDataRowBytes
	t.Run("preAllocateDataRowBytes", func(t *testing.T) {
		dataCols := []*sqlbase.ColumnDescriptor{int4Col, stringCol}
		rowBytes, dataOffset, varDataOffset, err := preAllocateDataRowBytes(2, &dataCols)
		if err != nil {
			t.Fatalf("expected no error for preAllocateDataRowBytes, got %v", err)
		}
		if len(rowBytes) != 2 {
			t.Fatalf("expected 2 row bytes, got %d", len(rowBytes))
		}
		if dataOffset <= 0 {
			t.Fatalf("expected positive data offset, got %d", dataOffset)
		}
		if varDataOffset <= dataOffset {
			t.Fatalf("expected var data offset > data offset, got varDataOffset=%d, dataOffset=%d", varDataOffset, dataOffset)
		}
	})

	// Test setDefaultValues
	t.Run("setDefaultValues", func(t *testing.T) {
		di := &DirectInsert{
			RowNum: 2,
			ColNum: 2,
		}

		temp := "123"
		// Create a column with default value
		colWithDefault := &sqlbase.ColumnDescriptor{
			ID:          4,
			Name:        "col_with_default",
			Type:        *types.Int4,
			DefaultExpr: &temp,
		}

		stmt := &parser.Statement{
			Insertdirectstmt: parser.Insertdirectstmt{
				InsertValues: []string{"1", "2", "3", "4"},
				ValuesType:   []parser.TokenType{parser.NUMTYPE, parser.NUMTYPE, parser.NUMTYPE, parser.NUMTYPE},
			},
		}

		err := setDefaultValues(di, 2, colWithDefault, stmt)
		if err != nil {
			t.Fatalf("expected no error for setDefaultValues, got %v", err)
		}
		if len(stmt.Insertdirectstmt.InsertValues) != 6 {
			t.Fatalf("expected 6 insert values, got %d", len(stmt.Insertdirectstmt.InsertValues))
		}
		if len(stmt.Insertdirectstmt.ValuesType) != 6 {
			t.Fatalf("expected 6 values types, got %d", len(stmt.Insertdirectstmt.ValuesType))
		}
	})

	// Test TsprepareTypeCheck
	t.Run("TsprepareTypeCheck", func(t *testing.T) {
		// Create test columns
		cols := []sqlbase.ColumnDescriptor{
			*timestampCol,
			*int4Col,
			*stringCol,
		}

		// Create test arguments
		args := [][]byte{
			[]byte("2023-01-01 12:00:00"),
			[]byte("123"),
			[]byte("test"),
			[]byte("2023-01-02 12:00:00"),
			[]byte("456"),
			[]byte("test2"),
		}

		// Create infer types
		inferTypes := []oid.Oid{
			oid.T_timestamp,
			oid.T_int4,
			oid.T_varchar,
			oid.T_timestamp,
			oid.T_int4,
			oid.T_varchar,
		}

		// Create format codes
		formatCodes := []pgwirebase.FormatCode{
			pgwirebase.FormatText,
			pgwirebase.FormatText,
			pgwirebase.FormatText,
			pgwirebase.FormatText,
			pgwirebase.FormatText,
			pgwirebase.FormatText,
		}

		// Create direct insert
		di := DirectInsert{
			RowNum: 6,
			ColNum: 3,
			IDMap:  []int{0, 1, 2},
			PosMap: []int{0, 1, 2},
		}

		_, _, err := TsprepareTypeCheck(ptCtx, args, inferTypes, formatCodes, &cols, di)
		if err != nil {
			t.Fatalf("expected no error for TsprepareTypeCheck, got %v", err)
		}
	})

	// Test GetColsInfo
	t.Run("GetColsInfo", func(t *testing.T) {
		// Create test insert statement
		ins := &tree.Insert{
			Columns: []tree.Name{"timestamp_col", "int4_col", "string_col"},
		}

		// Create direct insert
		di := &DirectInsert{}

		// Create test statement
		stmt := &parser.Statement{
			Insertdirectstmt: parser.Insertdirectstmt{
				InsertValues: []string{"2023-01-01 12:00:00", "123", "test"},
			},
		}

		// Create TS ID generator
		tsIDGen := &sqlbase.TSIDGenerator{}

		// Create eval context
		evalCtx := tree.EvalContext{}

		// Get columns info
		err := GetColsInfo(ctx, evalCtx, &tableDesc.Columns, ins, di, stmt, tsIDGen)
		if err != nil {
			t.Fatalf("expected no error for GetColsInfo, got %v", err)
		}
		if len(di.PrettyCols) == 0 {
			t.Fatal("expected pretty cols to be non-empty")
		}
	})

	// Test BuildRowBytesForPrepareTsInsert
	t.Run("BuildRowBytesForPrepareTsInsert", func(t *testing.T) {
		// Create test arguments
		args := [][]byte{
			[]byte("2023-01-01 12:00:00"),
			[]byte("123"),
			[]byte("test"),
		}

		// Create direct insert table
		dit := DirectInsertTable{
			TabID:   1,
			DbID:    1,
			HashNum: 16,
		}

		// Create TS ID generator
		tsIDGen := &sqlbase.TSIDGenerator{}

		// Create payload args
		pArgs, err := execbuilder.BuildPayloadArgs(1, tsIDGen, nil, nil, []*sqlbase.ColumnDescriptor{int4Col, stringCol})
		if err != nil {
			t.Fatalf("expected no error for BuildPayloadArgs, got %v", err)
		}

		// Create direct insert
		di := &DirectInsert{
			RowNum: 3,
			ColNum: 3,
			ColIndexs: map[int]int{
				1: 0,
				2: 1,
				3: 2,
			},
			PrettyCols:     []*sqlbase.ColumnDescriptor{timestampCol, int4Col, stringCol},
			Dcs:            []*sqlbase.ColumnDescriptor{int4Col, stringCol},
			PArgs:          pArgs,
			PayloadNodeMap: make(map[int]*sqlbase.PayloadForDistTSInsert),
			InputValues:    make([]tree.Datums, 1),
		}

		// Create eval context
		evalCtx := tree.EvalContext{}

		// Create row timestamps
		rowTimestamps := []int64{1672560000000}

		// Create executor config
		cfg := &ExecutorConfig{}

		// Build row bytes
		err = BuildRowBytesForPrepareTsInsert(ptCtx, args, dit, di, evalCtx, tableDesc, 1, rowTimestamps, cfg)
		if err != nil {
			t.Fatalf("expected no error for BuildRowBytesForPrepareTsInsert, got %v", err)
		}
	})

	value := []tree.Datum{tree.DNull, tree.DNull, tree.DNull}
	values := []tree.Datums{value}
	// Test getSingleRowBytes
	t.Run("getSingleRowBytes", func(t *testing.T) {
		// Create a properly initialized TsPayload
		tp := execbuilder.NewTsPayload()
		// Pre-allocate enough space for the payload
		initialPayload := make([]byte, 300)
		tp.SetPayload(initialPayload)

		// Create row process context
		rpCtx := &RowProcessContext{
			PtCtx: ptCtx,
			Di: &DirectInsert{
				RowNum: 1,
				ColNum: 3,
				ColIndexs: map[int]int{
					1: 0,
					2: 1,
					3: 2,
				},
			},
			Tp:            tp,
			RowBytes:      [][]byte{make([]byte, 300)},
			InsertValues:  []string{"1672560000000", "123", "test"},
			ValuesType:    []parser.TokenType{parser.NUMTYPE, parser.NUMTYPE, parser.STRINGTYPE},
			InputValues:   values,
			RowTimestamps: []int64{0},
			DataInfos: []columnExecInfo{
				{
					column:       int4Col,
					colIdx:       1,
					isData:       true,
					dataColIdx:   0,
					isLastData:   false,
					curColLength: 4,
					converter:    nil,
				},
				{
					column:       stringCol,
					colIdx:       2,
					isData:       true,
					dataColIdx:   1,
					isLastData:   true,
					curColLength: 255,
					converter:    nil,
				},
			},
			TagInfos: []columnExecInfo{
				{
					column:       timestampCol,
					colIdx:       0,
					isData:       false,
					isPrimaryTag: true,
					converter:    nil,
				},
			},
			TagKeyBuf:         make([]byte, 300),
			DataOffset:        8,               // execbuilder.DataLenSize
			IndependentOffset: 8 + 1 + 4 + 255, // data offset + bitmap + int4 + string
		}

		// Get single row bytes
		tagKey, err := getSingleRowBytes(rpCtx, 0)
		if err != nil {
			t.Fatalf("expected no error for getSingleRowBytes, got %v", err)
		}
		if tagKey == "" {
			t.Fatal("expected tag key to be non-empty")
		}
	})

	// Test GetRowBytesForTsInsert
	t.Run("GetRowBytesForTsInsert", func(t *testing.T) {
		// Create direct insert
		di := &DirectInsert{
			RowNum: 1,
			ColNum: 3,
			ColIndexs: map[int]int{
				1: 0,
				2: 1,
				3: 2,
			},
			PrettyCols: []*sqlbase.ColumnDescriptor{timestampCol, int4Col, stringCol},
			Dcs:        []*sqlbase.ColumnDescriptor{int4Col, stringCol},
		}

		// Create test statement
		stmt := parser.Statement{
			Insertdirectstmt: parser.Insertdirectstmt{
				InsertValues: []string{"1672560000000", "123", "test"},
				ValuesType:   []parser.TokenType{parser.NUMTYPE, parser.NUMTYPE, parser.STRINGTYPE},
			},
		}

		// Create statements
		stmts := parser.Statements{stmt}

		// Create row timestamps
		rowTimestamps := []int64{1672560000000}

		// Get row bytes
		inputValues, priTagValMap, rowBytes, err := GetRowBytesForTsInsert(ctx, ptCtx, di, stmts, rowTimestamps)
		if err != nil {
			t.Fatalf("expected no error for GetRowBytesForTsInsert, got %v", err)
		}
		if len(inputValues) != 1 {
			t.Fatalf("expected 1 input value, got %d", len(inputValues))
		}
		if len(priTagValMap) == 0 {
			t.Fatal("expected primary tag value map to be non-empty")
		}
		if len(rowBytes) != 1 {
			t.Fatalf("expected 1 row byte, got %d", len(rowBytes))
		}
	})

	// Test GetPayloadMapForMuiltNode
	t.Run("GetPayloadMapForMuiltNode", func(t *testing.T) {
		// Create direct insert table with non-zero HashNum
		dit := DirectInsertTable{
			TabID:   1,
			DbID:    1,
			HashNum: 16, // Use a non-zero value to avoid divide by zero
		}

		// Create TS ID generator
		tsIDGen := &sqlbase.TSIDGenerator{}

		// Create payload args
		pArgs, err := execbuilder.BuildPayloadArgs(1, tsIDGen, nil, nil, []*sqlbase.ColumnDescriptor{int4Col, stringCol})
		if err != nil {
			t.Fatalf("expected no error for BuildPayloadArgs, got %v", err)
		}

		// Create direct insert
		di := &DirectInsert{
			RowNum: 1,
			ColNum: 3,
			ColIndexs: map[int]int{
				1: 0,
				2: 1,
				3: 2,
			},
			PrettyCols:     []*sqlbase.ColumnDescriptor{timestampCol, int4Col, stringCol},
			Dcs:            []*sqlbase.ColumnDescriptor{int4Col, stringCol},
			PArgs:          pArgs,
			PayloadNodeMap: make(map[int]*sqlbase.PayloadForDistTSInsert),
			InputValues:    make([]tree.Datums, 1),
		}

		// Create test statement
		stmt := parser.Statement{
			Insertdirectstmt: parser.Insertdirectstmt{
				InsertValues: []string{"1672560000000", "123", "test"},
				ValuesType:   []parser.TokenType{parser.NUMTYPE, parser.NUMTYPE, parser.STRINGTYPE},
			},
		}

		// Create statements
		stmts := parser.Statements{stmt}

		// Create eval context with transaction
		st := cluster.MakeTestingClusterSettings()
		evalCtx := tree.MakeTestingEvalContext(st)

		// Create executor config
		cfg := &ExecutorConfig{}

		// Get payload map
		err = GetPayloadMapForMuiltNode(ctx, ptCtx, dit, di, stmts, evalCtx, tableDesc, cfg)
		if err != nil {
			t.Fatalf("expected no error for GetPayloadMapForMuiltNode, got %v", err)
		}
		if len(di.PayloadNodeMap) == 0 {
			t.Fatal("expected payload node map to be non-empty")
		}
	})
}
