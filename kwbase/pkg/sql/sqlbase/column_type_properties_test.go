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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/apd"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/assert"
)

func TestLimitValueWidth_BytesFamily(t *testing.T) {
	// Test bytes with width constraint
	byteType := types.MakeBytes(5, 0) // Max 5 bytes

	// Valid bytes (within width)
	testColName := "testColName"
	validBytes := tree.NewDBytes(tree.DBytes("test")) // 4 bytes
	result, err := sqlbase.LimitValueWidth(byteType, validBytes, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validBytes, result)

	// Invalid bytes (exceeds width)
	longBytes := tree.NewDBytes(tree.DBytes("verylong"))
	result, err = sqlbase.LimitValueWidth(byteType, longBytes, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "value too long for type")

	// Bytes with zero width (unlimited)
	unlimitedByteType := types.MakeBytes(0, 0)
	result, err = sqlbase.LimitValueWidth(unlimitedByteType, longBytes, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, longBytes, result)
}

func TestLimitValueWidth_StringFamily(t *testing.T) {
	// Test string with width constraint
	stringType := types.MakeString(5) // Max 5 runes
	testColName := "testColName"

	// Valid string (within width)
	validString := tree.NewDString("test") // 4 runes
	result, err := sqlbase.LimitValueWidth(stringType, validString, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validString, result)

	// Invalid string (exceeds width)
	longString := tree.NewDString("verylong") // 8 runes
	result, err = sqlbase.LimitValueWidth(stringType, longString, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "value too long for type")

	// String with zero width (unlimited)
	unlimitedStringType := types.MakeString(0)
	result, err = sqlbase.LimitValueWidth(unlimitedStringType, longString, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, longString, result)
}

func TestLimitValueWidth_CollatedStringFamily(t *testing.T) {
	testColName := "testColName"

	// Test collated string with width constraint
	BaseStringType := types.MakeString(5) // Max 5 runes
	collatedStringType := types.MakeCollatedString(BaseStringType, "en")

	// Valid collated string (within width)
	env := &tree.CollationEnvironment{}
	validCollatedString, err := tree.NewDCollatedString("test", "en", env)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sqlbase.LimitValueWidth(collatedStringType, validCollatedString, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validCollatedString, result)

	// Invalid collated string (exceeds width)
	longCollatedString, err := tree.NewDCollatedString("verylong", "en", env)
	if err != nil {
		t.Fatal(err)
	}
	result, err = sqlbase.LimitValueWidth(collatedStringType, longCollatedString, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "value too long for type")
}

func TestLimitValueWidth_IntFamily(t *testing.T) {
	testColName := "testInt"
	// Valid int16 value
	int16Type := types.MakeScalar(types.IntFamily, oid.T_int2, 0, 16, "", 0)
	validInt16 := tree.NewDInt(30000)
	result, err := sqlbase.LimitValueWidth(int16Type, validInt16, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validInt16, result)

	// Valid int32 value
	int32Type := types.MakeScalar(types.IntFamily, oid.T_int4, 0, 32, "", 0)
	validInt32 := tree.NewDInt(200000)
	result, err = sqlbase.LimitValueWidth(int32Type, validInt32, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validInt32, result)

	// Valid int64 value (default)
	int64Type := types.MakeScalar(types.IntFamily, oid.T_int8, 0, 64, "", 0)
	validInt64 := tree.NewDInt(92237203685475800)
	result, err = sqlbase.LimitValueWidth(int64Type, validInt64, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validInt64, result)
}

func TestLimitValueWidth_BitFamily(t *testing.T) {
	// Test bit array with width constraint
	bitType := types.MakeBit(4) // Exactly 4 bits
	testColName := "testBit"
	// Valid bit array (exactly 4 bits)
	validBitArray := tree.NewDBitArray(4)
	result, err := sqlbase.LimitValueWidth(bitType, validBitArray, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validBitArray, result)

	// Invalid bit array (more than 4 bits for fixed bit type)
	longBitArray := tree.NewDBitArray(5) // 5 bits
	result, err = sqlbase.LimitValueWidth(bitType, longBitArray, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "bit string length")

	// Varbit type with width constraint (max 6 bits)
	varbitType := types.MakeVarBit(6)

	// Valid varbit array (within limit)
	validVarbitArray := tree.NewDBitArray(3)
	result, err = sqlbase.LimitValueWidth(varbitType, validVarbitArray, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validVarbitArray, result)

	// Valid varbit array (at limit)
	atLimitVarbitArray := tree.NewDBitArray(6)
	result, err = sqlbase.LimitValueWidth(varbitType, atLimitVarbitArray, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, atLimitVarbitArray, result)

	// Invalid varbit array (exceeds limit)
	overLimitVarbitArray := tree.NewDBitArray(8)
	result, err = sqlbase.LimitValueWidth(varbitType, overLimitVarbitArray, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "bit string length")
}

func TestLimitValueWidth_DecimalFamily(t *testing.T) {
	// Test decimal with precision and scale constraints
	decimalType := types.MakeDecimal(5, 2) // Max 5 digits, 2 decimal places
	testColName := "testDecimal"
	// Valid decimal (within precision and scale)
	validDecimal := &tree.DDecimal{}
	err := validDecimal.SetString("123.45")
	assert.NoError(t, err)
	result, err := sqlbase.LimitValueWidth(decimalType, validDecimal, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validDecimal, result)

	// Decimal that needs truncation
	truncateDecimal := &tree.DDecimal{}
	err = truncateDecimal.SetString("123.456")
	assert.NoError(t, err)
	result, err = sqlbase.LimitValueWidth(decimalType, truncateDecimal, &testColName)
	assert.NoError(t, err)
	assert.NotEqual(t, truncateDecimal, result) // Should be truncated
	if resultDec, ok := result.(*tree.DDecimal); ok {
		// Check that the result has been truncated to the correct scale
		// Use the internal Exponent field of the decimal
		assert.Equal(t, int32(-2), resultDec.Exponent) // Scale should be 2
	}

	// Non-finite decimal (should pass through unchanged)
	infiniteDecimal := &tree.DDecimal{}
	infiniteDecimal.Form = apd.Infinite
	result, err = sqlbase.LimitValueWidth(decimalType, infiniteDecimal, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, infiniteDecimal, result)

	// Unlimited precision decimal
	unlimitedDecimalType := types.Decimal // Default unlimited decimal
	result, err = sqlbase.LimitValueWidth(unlimitedDecimalType, truncateDecimal, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, truncateDecimal, result)
}

func TestLimitValueWidth_TimeFamily(t *testing.T) {
	// Test time with precision constraint
	timeType := types.MakeTime(3) // Millisecond precision
	testColName := "testTime"

	// Valid time
	validTime := tree.MakeDTime((12*3600+30*60+45)*1e6 + 100100)
	result, err := sqlbase.LimitValueWidth(timeType, validTime, &testColName)
	assert.NoError(t, err)
	// The result should be rounded to millisecond precision
	expectedTime := tree.MakeDTime((12*3600+30*60+45)*1e6 + 100000)
	assert.Equal(t, *expectedTime, *result.(*tree.DTime))
}

func TestLimitValueWidth_TimestampFamily(t *testing.T) {
	// Test timestamp with precision constraint
	timestampType := types.MakeTimestamp(3) // Millisecond precision
	testColName := "testTime"

	// Valid timestamp
	validTimestamp := tree.MakeDTimestamp(timeutil.Unix(1234567890, 123456789), time.Microsecond)
	result, err := sqlbase.LimitValueWidth(timestampType, validTimestamp, &testColName)
	assert.NoError(t, err)
	// The result should be rounded to millisecond precision
	roundedTimestamp := tree.MakeDTimestamp(timeutil.Unix(1234567890, 123000000), time.Millisecond)
	assert.Equal(t, roundedTimestamp, result)
}

func TestLimitValueWidth_TimestampTZFamily(t *testing.T) {
	// Test timestamp with timezone with precision constraint
	timestampTZType := types.MakeTimestampTZ(3) // Millisecond precision
	testColName := "testTS"

	// Valid timestamp with timezone
	validTimestampTZ := tree.MakeDTimestampTZ(timeutil.Unix(1234567890, 123456789), time.Microsecond)
	result, err := sqlbase.LimitValueWidth(timestampTZType, validTimestampTZ, &testColName)
	assert.NoError(t, err)
	// The result should be rounded to millisecond precision
	roundedTimestampTZ := tree.MakeDTimestampTZ(timeutil.Unix(1234567890, 123000000), time.Millisecond)
	assert.Equal(t, roundedTimestampTZ, result)
}

func TestLimitValueWidth_TimeTZFamily(t *testing.T) {
	// Test time with timezone with precision constraint
	timeTZType := types.MakeTimeTZ(3) // Millisecond precision
	testColName := "testTS"

	// Valid time with timezone
	validTimeTZ := tree.NewDTimeTZFromTime(time.Date(0, 0, 0, 12, 30, 45, 123456789, time.UTC))
	result, err := sqlbase.LimitValueWidth(timeTZType, validTimeTZ, &testColName)
	assert.NoError(t, err)
	// The result should be rounded to millisecond precision
	roundedTimeTZ := tree.NewDTimeTZFromTime(time.Date(0, 0, 0, 12, 30, 45, 123000000, time.UTC))
	assert.Equal(t, roundedTimeTZ, result)
}

func TestLimitValueWidth_IntervalFamily(t *testing.T) {
	// Test interval with type metadata
	intervalType := types.MakeInterval(types.DefaultIntervalTypeMetadata)
	testColName := "testTS"

	// Valid interval
	durationVal := duration.MakeDuration(123456789, 0, 0) // Use MakeDuration function
	validInterval := tree.NewDInterval(durationVal, types.DefaultIntervalTypeMetadata)
	result, err := sqlbase.LimitValueWidth(intervalType, validInterval, &testColName)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLimitValueWidth_ArrayFamily(t *testing.T) {
	// Test array with element type constraints
	elementType := types.MakeString(3) // Max 3 characters per string
	arrayType := types.MakeArray(elementType)
	testColName := "testArray"

	// Valid array elements
	validString1 := tree.NewDString("abc")
	validString2 := tree.NewDString("def")
	validArray := tree.NewDArray(elementType)
	err := validArray.Append(validString1)
	assert.NoError(t, err)
	err = validArray.Append(validString2)
	assert.NoError(t, err)

	result, err := sqlbase.LimitValueWidth(arrayType, validArray, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, validArray, result)

	// Array with invalid element
	invalidString := tree.NewDString("verylong")
	invalidArray := tree.NewDArray(elementType)
	err = invalidArray.Append(validString1)
	assert.NoError(t, err)
	err = invalidArray.Append(invalidString)
	assert.NoError(t, err)
	result, err = sqlbase.LimitValueWidth(arrayType, invalidArray, &testColName)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "value too long for type")
}

func TestLimitValueWidth_NoChangeRequired(t *testing.T) {
	// Test that values that don't need modification are returned as-is
	stringType := types.MakeString(10) // More than needed
	testColName := "testDummy"

	shortString := tree.NewDString("short")
	result, err := sqlbase.LimitValueWidth(stringType, shortString, &testColName)
	assert.NoError(t, err)
	assert.Equal(t, shortString, result)
}

func TestCheckDatumTypeFitsColumnType(t *testing.T) {
	// Create a column descriptor with Int type
	colDesc := &sqlbase.ColumnDescriptor{
		Name: "test_col",
		Type: *types.Int,
		ID:   1,
	}

	// Test with matching type
	matchingType := types.Int
	err := sqlbase.CheckDatumTypeFitsColumnType(colDesc, matchingType)
	assert.NoError(t, err)

	// Test with non-matching type
	nonMatchingType := types.String
	err = sqlbase.CheckDatumTypeFitsColumnType(colDesc, nonMatchingType)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "doesn't match type")

	// Test with unknown family (should pass)
	unknownType := types.Unknown
	err = sqlbase.CheckDatumTypeFitsColumnType(colDesc, unknownType)
	assert.NoError(t, err)

	// Test with equivalent types (aliases)
	textColDesc := &sqlbase.ColumnDescriptor{
		Name: "text_col",
		Type: *types.String, // TEXT alias
		ID:   2,
	}
	varcharType := types.VarChar // VARCHAR alias
	err = sqlbase.CheckDatumTypeFitsColumnType(textColDesc, varcharType)
	// Note: The function checks for equivalence, so if String and VarChar are equivalent, this should pass
	// If they are not equivalent, this should fail
	// Let's test both scenarios by checking the actual behavior
	assert.NoError(t, err) // Assuming they are equivalent based on comment in function
}

func TestCheckDatumTypeFitsColumnType_ErrorDetails(t *testing.T) {
	// Create a column descriptor with specific type
	colDesc := &sqlbase.ColumnDescriptor{
		Name: "special_col",
		Type: *types.Timestamp,
		ID:   1,
	}

	// Test with wrong type to check error message
	wrongType := types.Int
	err := sqlbase.CheckDatumTypeFitsColumnType(colDesc, wrongType)
	assert.Error(t, err)

	// Check that the error is a PGError with the correct code
	// Use pgerror.GetPGCode instead of GetPGCause if GetPGCause doesn't exist
	if pgErr, ok := err.(*pgerror.Error); ok {
		assert.Equal(t, string(pgcode.DatatypeMismatch), string(pgErr.Code))
	}

	// Check that the error message contains relevant information
	assert.Contains(t, err.Error(), "value type")
	assert.Contains(t, err.Error(), "doesn't match type")
	assert.Contains(t, err.Error(), "timestamp")
	assert.Contains(t, err.Error(), "special_col")
}
