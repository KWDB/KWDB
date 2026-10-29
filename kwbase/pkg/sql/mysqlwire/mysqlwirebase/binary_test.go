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

package mysqlwirebase

import (
	"bytes"
	"testing"
)

// TestNullBitmap covers the offset that binary result rows apply. A client
// that reads column i's null flag from bit i rather than bit i+2 sees a
// plausible but wrong result set rather than a protocol error, so the offset
// is worth pinning down.
func TestNullBitmap(t *testing.T) {
	for _, tc := range []struct {
		n, offset, wantLen int
	}{
		{n: 0, offset: 2, wantLen: 1},
		{n: 1, offset: 2, wantLen: 1},
		{n: 6, offset: 2, wantLen: 1},
		{n: 7, offset: 2, wantLen: 2},
		{n: 8, offset: 0, wantLen: 1},
		{n: 9, offset: 0, wantLen: 2},
	} {
		if got := NullBitmapLen(tc.n, tc.offset); got != tc.wantLen {
			t.Errorf("NullBitmapLen(%d, %d) = %d, want %d", tc.n, tc.offset, got, tc.wantLen)
		}
	}

	// Setting one field must not disturb its neighbors.
	const n, offset = 20, 2
	for target := 0; target < n; target++ {
		bitmap := make([]byte, NullBitmapLen(n, offset))
		NullBitmapSet(bitmap, target, offset)
		for i := 0; i < n; i++ {
			if got, want := NullBitmapGet(bitmap, i, offset), i == target; got != want {
				t.Fatalf("after setting %d, field %d reads %v", target, i, got)
			}
		}
	}
}

// TestBinaryRowRoundTrip checks that the fixed-width encodings and the NULL
// bitmap agree with the reader. A width mismatch does not corrupt one value,
// it desynchronises every value after it.
func TestBinaryRowRoundTrip(t *testing.T) {
	types := []byte{
		TypeTiny, TypeShort, TypeLong, TypeLongLong,
		TypeFloat, TypeDouble, TypeVarString, TypeLongLong,
	}

	w := NewWriter(0)
	if err := WriteBinaryInt(w, TypeTiny, -8); err != nil {
		t.Fatal(err)
	}
	tiny := w.Bytes()

	w = NewWriter(0)
	if err := WriteBinaryInt(w, TypeShort, -300); err != nil {
		t.Fatal(err)
	}
	short := w.Bytes()

	w = NewWriter(0)
	if err := WriteBinaryInt(w, TypeLong, -70000); err != nil {
		t.Fatal(err)
	}
	long := w.Bytes()

	w = NewWriter(0)
	if err := WriteBinaryInt(w, TypeLongLong, -5000000000); err != nil {
		t.Fatal(err)
	}
	longlong := w.Bytes()

	w = NewWriter(0)
	if err := WriteBinaryFloat(w, TypeFloat, 0.5); err != nil {
		t.Fatal(err)
	}
	flt := w.Bytes()

	w = NewWriter(0)
	if err := WriteBinaryFloat(w, TypeDouble, -1.25); err != nil {
		t.Fatal(err)
	}
	dbl := w.Bytes()

	w = NewWriter(0)
	w.LenEncBytes([]byte("hello"))
	str := w.Bytes()

	values := [][]byte{tiny, short, long, longlong, flt, dbl, str, nil /* NULL */}

	got, err := DecodeBinaryRow(EncodeBinaryRow(values), types)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-8", "-300", "-70000", "-5000000000", "0.5", "-1.25", "hello", ""}
	for i := range want {
		if i == len(want)-1 {
			if got[i] != nil {
				t.Errorf("column %d: NULL decoded as %q", i, got[i])
			}
			continue
		}
		if string(got[i]) != want[i] {
			t.Errorf("column %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestBinaryIntWidths checks that each integer type occupies exactly the
// width MySQL declares, since the client reads by declared width.
func TestBinaryIntWidths(t *testing.T) {
	for _, tc := range []struct {
		typ      byte
		wantSize int
	}{
		{TypeTiny, 1},
		{TypeShort, 2},
		{TypeYear, 2},
		{TypeLong, 4},
		{TypeInt24, 4},
		{TypeLongLong, 8},
	} {
		w := NewWriter(0)
		if err := WriteBinaryInt(w, tc.typ, 1); err != nil {
			t.Fatalf("type 0x%02x: %v", tc.typ, err)
		}
		if got := len(w.Bytes()); got != tc.wantSize {
			t.Errorf("type 0x%02x encoded in %d bytes, want %d", tc.typ, got, tc.wantSize)
		}
	}
	if err := WriteBinaryInt(NewWriter(0), TypeVarString, 1); err == nil {
		t.Error("expected an error for a non-integer type")
	}
	if err := WriteBinaryFloat(NewWriter(0), TypeLongLong, 1); err == nil {
		t.Error("expected an error for a non-float type")
	}
}

// TestBinaryUnsigned checks that the unsigned flag is honored. Without it a
// large unsigned value is reported as a negative number, which is a wrong
// answer rather than a failure.
func TestBinaryUnsigned(t *testing.T) {
	w := NewWriter(0)
	if err := WriteBinaryInt(w, TypeLongLong, -1); err != nil {
		t.Fatal(err)
	}
	raw := w.Bytes()

	signed, _, err := ReadBinaryValue(NewReader(raw), TypeLongLong, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(signed) != "-1" {
		t.Errorf("signed read gave %q, want -1", signed)
	}

	unsigned, _, err := ReadBinaryValue(NewReader(raw), TypeLongLong, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(unsigned) != "18446744073709551615" {
		t.Errorf("unsigned read gave %q, want 18446744073709551615", unsigned)
	}
}

// TestBinaryDateTimeRoundTrip covers the variable-length datetime encoding,
// where the leading length byte tells the client how many components follow.
func TestBinaryDateTimeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		year, month, day, hour, minute, second, micro int
		typ                                           byte
		want                                          string
	}{
		{name: "date only", year: 2026, month: 8, day: 19, typ: TypeDate, want: "2026-08-19"},
		{
			name: "with time", year: 2026, month: 8, day: 19, hour: 12, minute: 34, second: 56,
			typ: TypeDatetime, want: "2026-08-19 12:34:56",
		},
		{
			name: "with micros", year: 2026, month: 8, day: 19, hour: 12, minute: 34, second: 56,
			micro: 789000, typ: TypeTimestamp, want: "2026-08-19 12:34:56.789000",
		},
		{name: "zero", typ: TypeDate, want: "0000-00-00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWriter(0)
			WriteBinaryDateTime(w, tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second, tc.micro)
			got, _, err := ReadBinaryValue(NewReader(w.Bytes()), tc.typ, false)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBinaryTimeRoundTrip covers MySQL's TIME, which is a signed duration
// rather than a clock reading and folds whole days into the hour field.
func TestBinaryTimeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		negative             bool
		days                 uint32
		hour, minute, second int
		micro                int
		want                 string
	}{
		{name: "plain", hour: 12, minute: 34, second: 56, want: "12:34:56"},
		{name: "micros", hour: 1, minute: 2, second: 3, micro: 400000, want: "01:02:03.400000"},
		{name: "days fold into hours", days: 2, hour: 3, minute: 0, second: 0, want: "51:00:00"},
		{name: "negative", negative: true, hour: 5, minute: 6, second: 7, want: "-05:06:07"},
		{name: "zero", want: "00:00:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWriter(0)
			WriteBinaryTime(w, tc.negative, tc.days, tc.hour, tc.minute, tc.second, tc.micro)
			got, _, err := ReadBinaryValue(NewReader(w.Bytes()), TypeTime, false)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReadParams covers the three ways a COM_STMT_EXECUTE argument can arrive:
// inline in the value block, as a NULL flagged in the bitmap, or streamed
// ahead of time by COM_STMT_SEND_LONG_DATA.
func TestReadParams(t *testing.T) {
	types := []ParamType{
		{Type: TypeLongLong},
		{Type: TypeVarString},
		{Type: TypeLong},
		{Type: TypeBlob},
	}

	w := NewWriter(0)
	if err := WriteBinaryInt(w, TypeLongLong, 42); err != nil {
		t.Fatal(err)
	}
	w.LenEncBytes([]byte("text"))
	// The third parameter is NULL and the fourth was streamed, so neither
	// contributes bytes here.
	body := w.Bytes()

	nullBitmap := make([]byte, NullBitmapLen(len(types), 0))
	NullBitmapSet(nullBitmap, 2, 0)

	longData := make([][]byte, len(types))
	longData[3] = []byte{0x00, 0xff, 0x10}

	params, err := ReadParams(NewReader(body), types, nullBitmap, longData)
	if err != nil {
		t.Fatal(err)
	}
	if len(params) != 4 {
		t.Fatalf("got %d params, want 4", len(params))
	}
	if string(params[0].Value) != "42" {
		t.Errorf("param 0 = %q, want 42", params[0].Value)
	}
	if string(params[1].Value) != "text" || params[1].Binary {
		t.Errorf("param 1 = %q (binary=%v), want text (binary=false)", params[1].Value, params[1].Binary)
	}
	if !params[2].Null {
		t.Errorf("param 2 = %q, want NULL", params[2].Value)
	}
	if !bytes.Equal(params[3].Value, longData[3]) {
		t.Errorf("param 3 = %x, want %x", params[3].Value, longData[3])
	}
	if !params[3].Binary {
		t.Error("param 3 should be flagged as binary; a blob is not text")
	}
}

// TestReadParamTypes checks that the unsigned flag is read from the second
// byte of each type pair.
func TestReadParamTypes(t *testing.T) {
	w := NewWriter(0)
	w.Byte(TypeLongLong)
	w.Byte(0x80) // unsigned
	w.Byte(TypeVarString)
	w.Byte(0x00)

	types, err := ReadParamTypes(NewReader(w.Bytes()), 2)
	if err != nil {
		t.Fatal(err)
	}
	if types[0].Type != TypeLongLong || !types[0].Unsigned {
		t.Errorf("param 0 = %+v, want LongLong unsigned", types[0])
	}
	if types[1].Type != TypeVarString || types[1].Unsigned {
		t.Errorf("param 1 = %+v, want VarString signed", types[1])
	}

	if _, err := ReadParamTypes(NewReader([]byte{TypeLong}), 1); err == nil {
		t.Error("expected an error for a truncated type block")
	}
}

// TestStmtPrepareOK pins the layout of the COM_STMT_PREPARE response header.
// A client reads the handle from it and cannot execute anything if the field
// order is wrong.
func TestStmtPrepareOK(t *testing.T) {
	got := EncodeStmtPrepareOK(7, 3 /* columns */, 2 /* params */, 0 /* warnings */)
	want := []byte{
		0x00,                   // status
		0x07, 0x00, 0x00, 0x00, // statement id
		0x03, 0x00, // column count
		0x02, 0x00, // parameter count
		0x00,       // reserved
		0x00, 0x00, // warning count
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got % x, want % x", got, want)
	}
}

// TestBinaryValueTruncation checks that a short packet is reported rather than
// read past. Every fixed-width read is a place a malicious or buggy client
// could otherwise walk the reader off the end of the buffer.
func TestBinaryValueTruncation(t *testing.T) {
	for _, typ := range []byte{
		TypeTiny, TypeShort, TypeLong, TypeLongLong, TypeFloat, TypeDouble,
		TypeDate, TypeTime, TypeVarString,
	} {
		if _, _, err := ReadBinaryValue(NewReader(nil), typ, false); err == nil {
			t.Errorf("type 0x%02x: expected an error on an empty payload", typ)
		}
	}
	// A datetime whose length byte announces more than the payload holds.
	if _, _, err := ReadBinaryValue(NewReader([]byte{11, 0xe0, 0x07}), TypeDatetime, false); err == nil {
		t.Error("expected an error for a truncated datetime")
	}
	// A length byte MySQL never emits.
	if _, _, err := ReadBinaryValue(NewReader([]byte{5}), TypeDatetime, false); err == nil {
		t.Error("expected an error for an invalid datetime length")
	}
	if _, _, err := ReadBinaryValue(NewReader([]byte{9}), TypeTime, false); err == nil {
		t.Error("expected an error for an invalid time length")
	}
}
