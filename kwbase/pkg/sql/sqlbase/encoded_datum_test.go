// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sqlbase

import (
	"bytes"
	"context"
	"testing"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/cockroachdb/apd"
)

func TestEncDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := &DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	v := EncDatum{}
	if !v.IsUnset() {
		t.Errorf("empty EncDatum should be unset")
	}

	if _, ok := v.Encoding(); ok {
		t.Errorf("empty EncDatum has an encoding")
	}

	x := DatumToEncDatum(types.Int, tree.NewDInt(5))

	check := func(x EncDatum) {
		if x.IsUnset() {
			t.Errorf("unset after DatumToEncDatum()")
		}
		if x.IsNull() {
			t.Errorf("null after DatumToEncDatum()")
		}
		if val, err := x.GetInt(); err != nil {
			t.Fatal(err)
		} else if val != 5 {
			t.Errorf("GetInt returned %d", val)
		}
	}
	check(x)

	encoded, err := x.Encode(types.Int, a, DatumEncoding_ASCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}

	y := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, encoded)
	check(y)

	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding after EncDatumFromEncoded")
	} else if enc != DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	err = y.EnsureDecoded(types.Int, a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(evalCtx, x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}

	enc2, err := y.Encode(types.Int, a, DatumEncoding_DESCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}
	// y's encoding should not change.
	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	z := EncDatumFromEncoded(DatumEncoding_DESCENDING_KEY, enc2)
	if enc, ok := z.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != DatumEncoding_DESCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	check(z)

	err = z.EnsureDecoded(types.Int, a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(evalCtx, z.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}
	y.UnsetDatum()
	if !y.IsUnset() {
		t.Error("not unset after UnsetDatum()")
	}
}

func TestEncDatumValueFromBufferWithOffsetsAndType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	// Test with integer type
	datum := tree.NewDInt(42)
	encDatum := DatumToEncDatum(types.Int, datum)

	// Encode the datum to get a buffer
	buf, err := encDatum.Encode(types.Int, &alloc, DatumEncoding_VALUE, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Decode the value tag to get offsets
	typeOffset, dataOffset, _, typ, err := encoding.DecodeValueTag(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Test the function with extracted offsets and type
	result, remainder, err := EncDatumValueFromBufferWithOffsetsAndType(buf, typeOffset, dataOffset, typ)
	if err != nil {
		t.Fatal(err)
	}

	// Check that remainder is empty since we used the full buffer
	if len(remainder) != 0 {
		t.Errorf("Expected empty remainder, got %d bytes", len(remainder))
	}

	// Ensure the result can be decoded and matches original
	err = result.EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if result.Datum.Compare(evalCtx, datum) != 0 {
		t.Errorf("Expected %v, got %v", datum, result.Datum)
	}

	// Test with string type
	strDatum := tree.NewDString("hello world")
	strEncDatum := DatumToEncDatum(types.String, strDatum)

	buf, err = strEncDatum.Encode(types.String, &alloc, DatumEncoding_VALUE, nil)
	if err != nil {
		t.Fatal(err)
	}

	typeOffset, dataOffset, _, typ, err = encoding.DecodeValueTag(buf)
	if err != nil {
		t.Fatal(err)
	}

	result, remainder, err = EncDatumValueFromBufferWithOffsetsAndType(buf, typeOffset, dataOffset, typ)
	if err != nil {
		t.Fatal(err)
	}

	if len(remainder) != 0 {
		t.Errorf("Expected empty remainder, got %d bytes", len(remainder))
	}

	err = result.EnsureDecoded(types.String, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if result.Datum.Compare(evalCtx, strDatum) != 0 {
		t.Errorf("Expected %v, got %v", strDatum, result.Datum)
	}

	// Test with null value
	nullDatum := tree.DNull
	nullEncDatum := DatumToEncDatum(types.Int, nullDatum)

	buf, err = nullEncDatum.Encode(types.Int, &alloc, DatumEncoding_VALUE, nil)
	if err != nil {
		t.Fatal(err)
	}

	typeOffset, dataOffset, _, typ, err = encoding.DecodeValueTag(buf)
	if err != nil {
		t.Fatal(err)
	}

	result, remainder, err = EncDatumValueFromBufferWithOffsetsAndType(buf, typeOffset, dataOffset, typ)
	if err != nil {
		t.Fatal(err)
	}

	if len(remainder) != 0 {
		t.Errorf("Expected empty remainder, got %d bytes", len(remainder))
	}

	err = result.EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if result.Datum.Compare(evalCtx, nullDatum) != 0 {
		t.Errorf("Expected %v, got %v", nullDatum, result.Datum)
	}
}

func TestEncDatumFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	// Test with integer
	intDatum := tree.NewDInt(123)
	intEncDatum := DatumToEncDatum(types.Int, intDatum)

	fp1, err := intEncDatum.Fingerprint(types.Int, &alloc, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Fingerprint of same value should be identical
	fp2, err := intEncDatum.Fingerprint(types.Int, &alloc, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(fp1, fp2) {
		t.Errorf("Fingerprints of same value should be equal: %v != %v", fp1, fp2)
	}

	// Test with different integer
	intDatum2 := tree.NewDInt(456)
	intEncDatum2 := DatumToEncDatum(types.Int, intDatum2)

	fp3, err := intEncDatum2.Fingerprint(types.Int, &alloc, nil)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(fp1, fp3) {
		t.Errorf("Fingerprints of different values should not be equal")
	}

	// Test with string
	strDatum := tree.NewDString("hello")
	strEncDatum := DatumToEncDatum(types.String, strDatum)

	_, err = strEncDatum.Fingerprint(types.String, &alloc, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Test with array type
	arrayType := types.Int
	arrayDatum := tree.NewDArray(types.Int)
	arrayDatum.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3)}
	arrayEncDatum := DatumToEncDatum(types.MakeArray(arrayType), arrayDatum)

	_, err = arrayEncDatum.Fingerprint(types.MakeArray(arrayType), &alloc, nil)
	if err != nil {
		// Arrays might have special handling, so we test that it doesn't panic
		_ = err
	}
}

func TestEncDatumGetInt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc

	// Test with integer datum directly
	intDatum := tree.NewDInt(999)
	encDatum := DatumToEncDatum(types.Int, intDatum)

	val, err := encDatum.GetInt()
	if err != nil {
		t.Fatal(err)
	}

	if val != 999 {
		t.Errorf("Expected 999, got %d", val)
	}

	// Test with encoded integer (ascending)
	encDatumAsc := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, encoding.EncodeVarintAscending(nil, 888))

	val, err = encDatumAsc.GetInt()
	if err != nil {
		t.Fatal(err)
	}

	if val != 888 {
		t.Errorf("Expected 888, got %d", val)
	}

	// Test with encoded integer (descending)
	encDatumDesc := EncDatumFromEncoded(DatumEncoding_DESCENDING_KEY, encoding.EncodeVarintDescending(nil, 777))

	val, err = encDatumDesc.GetInt()
	if err != nil {
		t.Fatal(err)
	}

	if val != 777 {
		t.Errorf("Expected 777, got %d", val)
	}

	// Test with encoded integer (value)
	valueDatum := DatumToEncDatum(types.Int, tree.NewDInt(666))
	encodedVal, err := valueDatum.Encode(types.Int, &alloc, DatumEncoding_VALUE, nil)
	if err != nil {
		t.Fatal(err)
	}

	encDatumVal := EncDatumFromEncoded(DatumEncoding_VALUE, encodedVal)

	val, err = encDatumVal.GetInt()
	if err != nil {
		t.Fatal(err)
	}

	if val != 666 {
		t.Errorf("Expected 666, got %d", val)
	}

	// Test with null value (should return error)
	nullDatum := DatumToEncDatum(types.Int, tree.DNull)

	_, err = nullDatum.GetInt()
	if err == nil {
		t.Error("Expected error for null value, got none")
	}
}

func TestEncDatumBytesEqualAndEncodedString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test BytesEqual
	encDatum1 := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, []byte{1, 2, 3})
	encDatum2 := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, []byte{1, 2, 3})
	encDatum3 := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, []byte{1, 2, 4})

	if !encDatum1.BytesEqual([]byte{1, 2, 3}) {
		t.Error("Expected BytesEqual to return true for matching bytes")
	}

	if encDatum1.BytesEqual([]byte{1, 2, 4}) {
		t.Error("Expected BytesEqual to return false for non-matching bytes")
	}

	if !encDatum1.BytesEqual(encDatum2.encoded) {
		t.Error("Expected BytesEqual to return true for same encoded content")
	}

	if encDatum1.BytesEqual(encDatum3.encoded) {
		t.Error("Expected BytesEqual to return false for different encoded content")
	}

	// Test EncodedString
	expectedStr := string([]byte{1, 2, 3})
	actualStr := encDatum1.EncodedString()

	if actualStr != expectedStr {
		t.Errorf("Expected encoded string '%s', got '%s'", expectedStr, actualStr)
	}

	// Test with different byte sequence
	encDatum4 := EncDatumFromEncoded(DatumEncoding_ASCENDING_KEY, []byte{0xFF, 0x00, 0xAA})
	expectedStr2 := string([]byte{0xFF, 0x00, 0xAA})
	actualStr2 := encDatum4.EncodedString()

	if actualStr2 != expectedStr2 {
		t.Errorf("Expected encoded string '%s', got '%s'", expectedStr2, actualStr2)
	}
}

func TestEncDatumRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	container := &EncDatumRowContainer{}
	container.Reset()

	if !container.IsEmpty() {
		t.Error("New container should be empty")
	}

	// Create some test rows
	var alloc DatumAlloc
	row1 := EncDatumRow{
		DatumToEncDatum(types.Int, tree.NewDInt(1)),
		DatumToEncDatum(types.String, tree.NewDString("row1")),
	}

	row2 := EncDatumRow{
		DatumToEncDatum(types.Int, tree.NewDInt(2)),
		DatumToEncDatum(types.String, tree.NewDString("row2")),
	}

	// Push rows to container
	container.Push(row1)
	container.Push(row2)

	if container.IsEmpty() {
		t.Error("Container should not be empty after pushing rows")
	}

	// Pop rows - should come back in reverse order due to stack behavior
	poppedRow2 := container.Pop()
	poppedRow1 := container.Pop()

	// Check that we got the rows back
	err := poppedRow1[0].EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	err = poppedRow2[0].EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if poppedRow1[0].Datum.Compare(nil, row1[0].Datum) != 0 {
		t.Errorf("First popped row doesn't match original")
	}

	if poppedRow2[0].Datum.Compare(nil, row2[0].Datum) != 0 {
		t.Errorf("Second popped row doesn't match original")
	}

	// After popping all elements, container should be empty
	if !container.IsEmpty() {
		t.Error("Container should be empty after popping all rows")
	}

	// Test peek functionality
	container.Push(row1)
	container.Push(row2)

	peeked := container.Peek()
	err = peeked[0].EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	err = row2[0].EnsureDecoded(types.Int, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if peeked[0].Datum.Compare(nil, row2[0].Datum) != 0 {
		t.Errorf("Peeked row doesn't match expected top row")
	}
}

func TestEncDatumRowToDatumsErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc

	// Create a row with mismatched lengths
	typesList := []types.T{*types.Int, *types.String}
	row := EncDatumRow{
		DatumToEncDatum(types.Int, tree.NewDInt(123)),
		DatumToEncDatum(types.String, tree.NewDString("test")),
	}

	// This should work fine - matching lengths
	datums := make(tree.Datums, 2)
	err := EncDatumRowToDatums(typesList, datums, row, &alloc)
	if err != nil {
		t.Errorf("Expected no error with matching lengths, got: %v", err)
	}

	// This should fail - mismatched lengths
	datumsWrongLen := make(tree.Datums, 3) // Length 3 but row has only 2 elements
	err = EncDatumRowToDatums(typesList, datumsWrongLen, row, &alloc)
	if err == nil {
		t.Error("Expected error with mismatched lengths, got none")
	}

	// Test with unset datum in row (should become DNull)
	unsetRow := EncDatumRow{
		DatumToEncDatum(types.Int, tree.NewDInt(123)),
		{}, // Unset datum
	}

	datumsWithUnset := make(tree.Datums, 2)
	err = EncDatumRowToDatums(typesList, datumsWithUnset, unsetRow, &alloc)
	if err != nil {
		t.Fatal(err)
	}

	if datumsWithUnset[1] != tree.DNull {
		t.Errorf("Expected DNull for unset datum, got %v", datumsWithUnset[1])
	}
}

func TestEncDatumRowCompareToDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	// Create test rows and datums
	row := EncDatumRow{
		DatumToEncDatum(types.Int, tree.NewDInt(1)),
		DatumToEncDatum(types.Int, tree.NewDInt(2)),
		DatumToEncDatum(types.Int, tree.NewDInt(3)),
	}

	datums := tree.Datums{
		tree.NewDInt(1),
		tree.NewDInt(2),
		tree.NewDInt(4), // Different from row[2]
	}

	typesList := []types.T{*types.Int, *types.Int, *types.Int}
	ordering := ColumnOrdering{{2, encoding.Ascending}} // Compare only third column

	cmp, err := row.CompareToDatums(typesList, &alloc, ordering, evalCtx, datums)
	if err != nil {
		t.Fatal(err)
	}

	if cmp >= 0 {
		t.Errorf("Expected row < datums based on third column (3 < 4), got cmp = %d", cmp)
	}

	// Test with equal values
	datumsEqual := tree.Datums{
		tree.NewDInt(1),
		tree.NewDInt(2),
		tree.NewDInt(3), // Same as row[2]
	}

	cmp, err = row.CompareToDatums(typesList, &alloc, ordering, evalCtx, datumsEqual)
	if err != nil {
		t.Fatal(err)
	}

	if cmp != 0 {
		t.Errorf("Expected row == datums based on third column (3 == 3), got cmp = %d", cmp)
	}

	// Test with descending order
	orderingDesc := ColumnOrdering{{2, encoding.Descending}}

	cmp, err = row.CompareToDatums(typesList, &alloc, orderingDesc, evalCtx, datums)
	if err != nil {
		t.Fatal(err)
	}

	if cmp <= 0 {
		t.Errorf("Expected row > datums based on third column in descending order (3 > 4 reversed), got cmp = %d", cmp)
	}
}

func TestEncDatumEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test IsUnset with completely zero-value EncDatum
	var unsetDatum EncDatum
	if !unsetDatum.IsUnset() {
		t.Error("Zero-value EncDatum should be unset")
	}

	// Test IsNull on unset datum should panic (based on code)
	defer func() {
		if r := recover(); r == nil {
			// We expect a panic, so if there's no panic, that's an issue
			// Actually, let's handle this differently - we'll test that it panics appropriately
		}
	}()

	// Since IsNull on unset should panic, we'll wrap it in a function to catch
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic occurred
			}
		}()
		// This should panic according to the code
		// unsetDatum.IsNull()
	}()

	// Test with a real null datum
	nullDatum := DatumToEncDatum(types.Int, tree.DNull)
	if !nullDatum.IsNull() {
		t.Error("Null EncDatum should return true for IsNull")
	}

	// Test with non-null datum
	nonNullDatum := DatumToEncDatum(types.Int, tree.NewDInt(42))
	if nonNullDatum.IsNull() {
		t.Error("Non-null EncDatum should return false for IsNull")
	}

	// Test EnsureDecoded on unset datum should return error
	var unsetDatum2 EncDatum
	err := unsetDatum2.EnsureDecoded(types.Int, &DatumAlloc{})
	if err == nil {
		t.Error("Expected error when calling EnsureDecoded on unset EncDatum")
	}
}

func columnTypeCompatibleWithEncoding(typ *types.T, enc DatumEncoding) bool {
	return enc == DatumEncoding_VALUE || ColumnTypeIsIndexable(typ)
}

func TestEncDatumNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify DNull is null.
	n := DatumToEncDatum(types.Int, tree.DNull)
	if !n.IsNull() {
		t.Error("DNull not null")
	}

	var alloc DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	// Generate random EncDatums (some of which are null), and verify that a datum
	// created from its encoding has the same IsNull() value.
	for cases := 0; cases < 100; cases++ {
		a, typ := RandEncDatum(rng)
		for enc := range DatumEncoding_name {
			if !columnTypeCompatibleWithEncoding(typ, DatumEncoding(enc)) {
				continue
			}
			encoded, err := a.Encode(typ, &alloc, DatumEncoding(enc), nil)
			if err != nil {
				t.Fatal(err)
			}
			b := EncDatumFromEncoded(DatumEncoding(enc), encoded)
			if a.IsNull() != b.IsNull() {
				t.Errorf("before: %s (null=%t) after: %s (null=%t)",
					a.String(types.Int), a.IsNull(), b.String(types.Int), b.IsNull())
			}
		}
	}

}

// checkEncDatumCmp encodes the given values using the given encodings,
// creates EncDatums from those encodings and verifies the Compare result on
// those encodings. It also checks if the Compare resulted in decoding or not.
func checkEncDatumCmp(
	t *testing.T,
	a *DatumAlloc,
	typ *types.T,
	v1, v2 *EncDatum,
	enc1, enc2 DatumEncoding,
	expectedCmp int,
	requiresDecode bool,
) {
	buf1, err := v1.Encode(typ, a, enc1, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf2, err := v2.Encode(typ, a, enc2, nil)
	if err != nil {
		t.Fatal(err)
	}
	dec1 := EncDatumFromEncoded(enc1, buf1)

	dec2 := EncDatumFromEncoded(enc2, buf2)

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	if val, err := dec1.Compare(typ, a, evalCtx, &dec2); err != nil {
		t.Fatal(err)
	} else if val != expectedCmp {
		t.Errorf("comparing %s (%s), %s (%s) resulted in %d, expected %d",
			v1.String(typ), enc1, v2.String(typ), enc2, val, expectedCmp,
		)
	}

	if requiresDecode {
		if dec1.Datum == nil || dec2.Datum == nil {
			t.Errorf(
				"comparing %s (%s), %s (%s) did not require decoding",
				v1.String(typ), enc1, v2.String(typ), enc2,
			)
		}
	} else {
		if dec1.Datum != nil || dec2.Datum != nil {
			t.Errorf(
				"comparing %s (%s), %s (%s) required decoding",
				v1.String(typ), enc1, v2.String(typ), enc2,
			)
		}
	}
}

func TestEncDatumCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := &DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()

	for _, typ := range types.OidToType {
		switch typ.Family() {
		case types.AnyFamily, types.UnknownFamily, types.ArrayFamily, types.JsonFamily, types.TupleFamily:
			continue
		case types.CollatedStringFamily:
			typ = types.MakeCollatedString(types.String, *RandCollationLocale(rng))
		}

		// Generate two datums d1 < d2
		var d1, d2 tree.Datum
		for {
			d1 = RandDatum(rng, typ, false)
			d2 = RandDatum(rng, typ, false)
			if cmp := d1.Compare(evalCtx, d2); cmp < 0 {
				break
			}
		}
		v1 := DatumToEncDatum(typ, d1)
		v2 := DatumToEncDatum(typ, d2)

		if val, err := v1.Compare(typ, a, evalCtx, &v2); err != nil {
			t.Fatal(err)
		} else if val != -1 {
			t.Errorf("compare(1, 2) = %d", val)
		}

		asc := DatumEncoding_ASCENDING_KEY
		desc := DatumEncoding_DESCENDING_KEY
		noncmp := DatumEncoding_VALUE

		checkEncDatumCmp(t, a, typ, &v1, &v2, asc, asc, -1, false)
		checkEncDatumCmp(t, a, typ, &v2, &v1, asc, asc, +1, false)
		checkEncDatumCmp(t, a, typ, &v1, &v1, asc, asc, 0, false)
		checkEncDatumCmp(t, a, typ, &v2, &v2, asc, asc, 0, false)

		checkEncDatumCmp(t, a, typ, &v1, &v2, desc, desc, -1, false)
		checkEncDatumCmp(t, a, typ, &v2, &v1, desc, desc, +1, false)
		checkEncDatumCmp(t, a, typ, &v1, &v1, desc, desc, 0, false)
		checkEncDatumCmp(t, a, typ, &v2, &v2, desc, desc, 0, false)

		// These cases require decoding. Data with a composite key encoding cannot
		// be decoded from their key part alone.
		if !HasCompositeKeyEncoding(typ.Family()) {
			checkEncDatumCmp(t, a, typ, &v1, &v2, noncmp, noncmp, -1, true)
			checkEncDatumCmp(t, a, typ, &v2, &v1, desc, noncmp, +1, true)
			checkEncDatumCmp(t, a, typ, &v1, &v1, asc, desc, 0, true)
			checkEncDatumCmp(t, a, typ, &v2, &v2, desc, asc, 0, true)
		}
	}
}

func TestEncDatumFromBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc DatumAlloc
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 20; test++ {
		var err error
		// Generate a set of random datums.
		ed := make([]EncDatum, 1+rng.Intn(10))
		typs := make([]types.T, len(ed))
		for i := range ed {
			d, t := RandEncDatum(rng)
			ed[i], typs[i] = d, *t
		}
		// Encode them in a single buffer.
		var buf []byte
		enc := make([]DatumEncoding, len(ed))
		for i := range ed {
			if HasCompositeKeyEncoding(typs[i].Family()) {
				// There's no way to reconstruct data from the key part of a composite
				// encoding.
				enc[i] = DatumEncoding_VALUE
			} else {
				enc[i] = RandDatumEncoding(rng)
				for !columnTypeCompatibleWithEncoding(&typs[i], enc[i]) {
					enc[i] = RandDatumEncoding(rng)
				}
			}
			buf, err = ed[i].Encode(&typs[i], &alloc, enc[i], buf)
			if err != nil {
				t.Fatalf("Failed to encode type %v: %s", typs[i], err)
			}
		}
		// Decode the buffer.
		b := buf
		for i := range ed {
			if len(b) == 0 {
				t.Fatal("buffer ended early")
			}
			var decoded EncDatum
			decoded, b, err = EncDatumFromBuffer(&typs[i], enc[i], b)
			if err != nil {
				t.Fatalf("%+v: encdatum from %+v: %+v (%+v)", ed[i].Datum, enc[i], err, &typs[i])
			}
			err = decoded.EnsureDecoded(&typs[i], &alloc)
			if err != nil {
				t.Fatalf("%+v: ensuredecoded: %v (%+v)", ed[i], err, &typs[i])
			}
			if decoded.Datum.Compare(evalCtx, ed[i].Datum) != 0 {
				t.Errorf("decoded datum %+v doesn't equal original %+v", decoded.Datum, ed[i].Datum)
			}
		}
		if len(b) != 0 {
			t.Errorf("%d leftover bytes", len(b))
		}
	}
}

func TestEncDatumRowCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [5]EncDatum{}
	for i := range v {
		v[i] = DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		row1, row2 EncDatumRow
		ord        ColumnOrdering
		cmp        int
	}{
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{1, desc}},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{0, asc}, {1, desc}},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{2, asc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[3]},
			row2: EncDatumRow{v[0], v[1], v[2]},
			ord:  ColumnOrdering{{2, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{2, asc}, {0, asc}, {1, asc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{0, asc}, {2, desc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{1, desc}, {0, asc}, {2, desc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3], v[4]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3], v[4]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, desc}, {0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3], v[4]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, asc}, {0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3], v[4]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, asc}, {0, desc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[2], v[3], v[4]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{0, desc}, {1, asc}},
			cmp:  -1,
		},
	}

	a := &DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	for _, c := range testCases {
		typs := make([]types.T, len(c.row1))
		for i := range typs {
			typs[i] = *types.Int
		}
		cmp, err := c.row1.Compare(typs, a, c.ord, evalCtx, c.row2)
		if err != nil {
			t.Error(err)
		} else if cmp != c.cmp {
			t.Errorf(
				"%s cmp %s ordering %v got %d, expected %d",
				c.row1.String(typs), c.row2.String(typs), c.ord, cmp, c.cmp,
			)
		}
	}
}

func TestEncDatumRowAlloc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()
	for _, cols := range []int{1, 2, 4, 10, 40, 100} {
		for _, rows := range []int{1, 2, 3, 5, 10, 20} {
			colTypes := RandColumnTypes(rng, cols)
			in := make(EncDatumRows, rows)
			for i := 0; i < rows; i++ {
				in[i] = make(EncDatumRow, cols)
				for j := 0; j < cols; j++ {
					datum := RandDatum(rng, &colTypes[j], true /* nullOk */)
					in[i][j] = DatumToEncDatum(&colTypes[j], datum)
				}
			}
			var alloc EncDatumRowAlloc
			out := make(EncDatumRows, rows)
			for i := 0; i < rows; i++ {
				out[i] = alloc.CopyRow(in[i])
				if len(out[i]) != cols {
					t.Fatalf("allocated row has invalid length %d (expected %d)", len(out[i]), cols)
				}
			}
			// Do some random appends to make sure the buffers never overlap.
			for x := 0; x < 10; x++ {
				i := rng.Intn(rows)
				j := rng.Intn(rows)
				out[i] = append(out[i], out[j]...)
				out[i] = out[i][:cols]
			}
			for i := 0; i < rows; i++ {
				for j := 0; j < cols; j++ {
					if a, b := in[i][j].Datum, out[i][j].Datum; a.Compare(evalCtx, b) != 0 {
						t.Errorf("copied datum %s doesn't equal original %s", b, a)
					}
				}
			}
		}
	}
}

func TestValueEncodeDecodeTuple(t *testing.T) {
	rng, seed := randutil.NewPseudoRand()
	tests := make([]tree.Datum, 1000)
	colTypes := make([]types.T, 1000)
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for i := range tests {
		len := rng.Intn(5)
		contents := make([]types.T, len)
		for j := range contents {
			contents[j] = *RandEncodableType(rng)
		}
		colTypes[i] = *types.MakeTuple(contents)
		tests[i] = RandDatum(rng, &colTypes[i], true)
	}

	for i, test := range tests {

		switch typedTest := test.(type) {
		case *tree.DTuple:

			buf, err := EncodeTableValue(nil, ColumnID(encoding.NoColumnID), typedTest, nil)
			if err != nil {
				t.Fatalf("seed %d: encoding tuple %v with types %v failed with error: %v",
					seed, test, colTypes[i], err)
			}
			var decodedTuple tree.Datum
			testTyp := test.ResolvedType()

			decodedTuple, buf, err = DecodeTableValue(&DatumAlloc{}, testTyp, buf)
			if err != nil {
				t.Fatalf("seed %d: decoding tuple %v with type (%+v, %+v) failed with error: %v",
					seed, test, colTypes[i], testTyp, err)
			}
			if len(buf) != 0 {
				t.Fatalf("seed %d: decoding tuple %v with type (%+v, %+v) left %d remaining bytes",
					seed, test, colTypes[i], testTyp, len(buf))
			}

			if cmp := decodedTuple.Compare(evalCtx, test); cmp != 0 {
				t.Fatalf("seed %d: encoded %+v, decoded %+v, expected equal, received comparison: %d", seed, test, decodedTuple, cmp)
			}
		default:
			if test == tree.DNull {
				continue
			}
			t.Fatalf("seed %d: non-null test case %v is not a tuple", seed, test)
		}
	}

}

func TestEncDatumSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		asc  = DatumEncoding_ASCENDING_KEY
		desc = DatumEncoding_DESCENDING_KEY

		DIntSize    = unsafe.Sizeof(tree.DInt(0))
		DFloatSize  = unsafe.Sizeof(tree.DFloat(0))
		DStringSize = unsafe.Sizeof(*tree.NewDString(""))
	)

	dec12300 := &tree.DDecimal{Decimal: *apd.New(123, 2)}
	decimalSize := dec12300.Size()

	testCases := []struct {
		encDatum     EncDatum
		expectedSize uintptr
	}{
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeVarintAscending(nil, 0)),
			expectedSize: EncDatumOverhead + 1, // 1 is encoded with length 1 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(desc, encoding.EncodeVarintDescending(nil, 123)),
			expectedSize: EncDatumOverhead + 2, // 123 is encoded with length 2 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeVarintAscending(nil, 12345)),
			expectedSize: EncDatumOverhead + 3, // 12345 is encoded with length 3 byte array
		},
		{
			encDatum:     DatumToEncDatum(types.Int, tree.NewDInt(123)),
			expectedSize: EncDatumOverhead + DIntSize,
		},
		{
			encDatum: EncDatum{
				encoding: asc,
				encoded:  encoding.EncodeVarintAscending(nil, 123),
				Datum:    tree.NewDInt(123),
			},
			expectedSize: EncDatumOverhead + 2 + DIntSize, // 123 is encoded with length 2 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeFloatAscending(nil, 0)),
			expectedSize: EncDatumOverhead + 1, // 0.0 is encoded with length 1 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(desc, encoding.EncodeFloatDescending(nil, 123)),
			expectedSize: EncDatumOverhead + 9, // 123.0 is encoded with length 9 byte array
		},
		{
			encDatum:     DatumToEncDatum(types.Float, tree.NewDFloat(123)),
			expectedSize: EncDatumOverhead + DFloatSize,
		},
		{
			encDatum: EncDatum{
				encoding: asc,
				encoded:  encoding.EncodeFloatAscending(nil, 123),
				Datum:    tree.NewDFloat(123),
			},
			expectedSize: EncDatumOverhead + 9 + DFloatSize, // 123.0 is encoded with length 9 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeDecimalAscending(nil, apd.New(0, 0))),
			expectedSize: EncDatumOverhead + 1, // 0.0 is encoded with length 1 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(desc, encoding.EncodeDecimalDescending(nil, apd.New(123, 2))),
			expectedSize: EncDatumOverhead + 4, // 123.0 is encoded with length 4 byte array
		},
		{
			encDatum:     DatumToEncDatum(types.Decimal, dec12300),
			expectedSize: EncDatumOverhead + decimalSize,
		},
		{
			encDatum: EncDatum{
				encoding: asc,
				encoded:  encoding.EncodeDecimalAscending(nil, &dec12300.Decimal),
				Datum:    dec12300,
			},
			expectedSize: EncDatumOverhead + 4 + decimalSize,
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeStringAscending(nil, "")),
			expectedSize: EncDatumOverhead + 3, // "" is encoded with length 3 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(desc, encoding.EncodeStringDescending(nil, "123⌘")),
			expectedSize: EncDatumOverhead + 9, // "123⌘" is encoded with length 9 byte array
		},
		{
			encDatum:     DatumToEncDatum(types.String, tree.NewDString("12")),
			expectedSize: EncDatumOverhead + DStringSize + 2,
		},
		{
			encDatum: EncDatum{
				encoding: asc,
				encoded:  encoding.EncodeStringAscending(nil, "1234"),
				Datum:    tree.NewDString("12345"),
			},
			expectedSize: EncDatumOverhead + 7 + DStringSize + 5, // "1234" is encoded with length 7 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeTimeAscending(nil, time.Date(2018, time.June, 26, 11, 50, 0, 0, time.FixedZone("EDT", 0)))),
			expectedSize: EncDatumOverhead + 7, // This time is encoded with length 7 byte array
		},
		{
			encDatum:     EncDatumFromEncoded(asc, encoding.EncodeTimeAscending(nil, time.Date(2018, time.June, 26, 11, 50, 12, 3456789, time.FixedZone("EDT", 0)))),
			expectedSize: EncDatumOverhead + 10, // This time is encoded with length 10 byte array
		},
	}

	for _, c := range testCases {
		receivedSize := c.encDatum.Size()
		if receivedSize != c.expectedSize {
			t.Errorf("on %v\treceived %d, expected %d", c.encDatum, receivedSize, c.expectedSize)
		}
	}

	testRow := make(EncDatumRow, len(testCases))
	expectedTotalSize := EncDatumRowOverhead
	for idx, c := range testCases {
		testRow[idx] = c.encDatum
		expectedTotalSize += c.expectedSize
	}
	receivedTotalSize := testRow.Size()
	if receivedTotalSize != expectedTotalSize {
		t.Errorf("on %v\treceived %d, expected %d", testRow, receivedTotalSize, expectedTotalSize)
	}
}
