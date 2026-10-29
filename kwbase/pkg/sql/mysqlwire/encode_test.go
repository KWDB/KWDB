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

package mysqlwire

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
)

func intArray(t *testing.T, vals ...int) *tree.DArray {
	t.Helper()
	a := tree.NewDArray(types.Int)
	for _, v := range vals {
		if err := a.Append(tree.NewDInt(tree.DInt(v))); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

// stringArray builds a STRING[]; a nil element is SQL NULL.
func stringArray(t *testing.T, vals ...interface{}) *tree.DArray {
	t.Helper()
	a := tree.NewDArray(types.String)
	for _, v := range vals {
		d := tree.Datum(tree.DNull)
		if v != nil {
			d = tree.NewDString(v.(string))
		}
		if err := a.Append(d); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func testConv() sessiondata.DataConversionConfig {
	return sessiondata.DataConversionConfig{Location: time.UTC}
}

func mustDate(t *testing.T, s string) *tree.DDate {
	t.Helper()
	tm, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatal(err)
	}
	d, err := tree.NewDDateFromTime(tm)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustDecimal(t *testing.T, s string) *tree.DDecimal {
	t.Helper()
	d, err := tree.ParseDDecimal(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestBinaryEncodingMatchesColumnType is the guard against the failure mode
// that makes the binary protocol dangerous: a value written at a width other
// than the one the column definition declared.
//
// The client reads each value at the declared width, so a mismatch does not
// corrupt one value, it shifts every value after it. Nothing reports an error
// on either side — the client simply reads garbage that happens to parse. This
// test closes the loop for every type mysqlType can report: it takes the type
// mysqlType assigns to a column, encodes a datum against it, and requires that
// reading it back with the same declared type consumes the whole encoding and
// yields the expected value.
func TestBinaryEncodingMatchesColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	conv := testConv()
	for _, tc := range []struct {
		name  string
		typ   *types.T
		datum tree.Datum
		want  string
	}{
		{"bool true", types.Bool, tree.MakeDBool(true), "1"},
		{"bool false", types.Bool, tree.MakeDBool(false), "0"},
		{"int2", types.Int2, tree.NewDInt(-300), "-300"},
		{"int4", types.Int4, tree.NewDInt(-70000), "-70000"},
		{"int8", types.Int, tree.NewDInt(-5000000000), "-5000000000"},
		{"float4", types.Float4, tree.NewDFloat(0.5), "0.5"},
		{"float8", types.Float, tree.NewDFloat(-1.25), "-1.25"},
		{"decimal", types.Decimal, mustDecimal(t, "123.45"), "123.45"},
		{"string", types.String, tree.NewDString("hello"), "hello"},
		{"bytes", types.Bytes, tree.NewDBytes("\x00\xff\x10"), "\x00\xff\x10"},
		{"date", types.Date, mustDate(t, "2026-08-19"), "2026-08-19"},
		{
			"time", types.Time,
			tree.MakeDTime(timeofday.New(12, 34, 56, 0)),
			"12:34:56",
		},
		{
			"timestamp", types.Timestamp,
			tree.MakeDTimestamp(time.Date(2026, 8, 19, 12, 34, 56, 0, time.UTC), time.Microsecond),
			"2026-08-19 12:34:56",
		},
		{
			// MySQL's binary TIME has no field for a zone, so a TIMETZ column is
			// reported as a string and keeps its offset.
			"timetz", types.TimeTZ,
			tree.NewDTimeTZFromLocation(timeofday.New(11, 0, 0, 0), time.FixedZone("", -5*3600)),
			"11:00:00-05:00",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			colType, _, _, _, _ := mysqlType(tc.typ)

			encoded, err := encodeDatumBinary(tc.datum, colType, conv)
			if err != nil {
				t.Fatalf("encoding as type 0x%02x: %v", colType, err)
			}

			r := mysqlwirebase.NewReader(encoded)
			got, _, err := mysqlwirebase.ReadBinaryValue(r, colType, false /* unsigned */)
			if err != nil {
				t.Fatalf("decoding as type 0x%02x: %v", colType, err)
			}
			// A reader left with bytes to spare means the writer emitted more
			// than the declared type describes, which is exactly the
			// desynchronisation this test exists to catch.
			if r.Len() != 0 {
				t.Errorf("type 0x%02x: %d byte(s) left unread after decoding; "+
					"the encoded width does not match the declared column type",
					colType, r.Len())
			}
			if string(got) != tc.want {
				t.Errorf("round-tripped %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEncodeDatumBinaryNull checks that SQL NULL produces no bytes at all. In
// the binary protocol a NULL is carried solely by the row's NULL bitmap, so
// emitting even one placeholder byte would shift the rest of the row.
func TestEncodeDatumBinaryNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, typ := range []*types.T{types.Int, types.String, types.Timestamp, types.Bytes} {
		colType, _, _, _, _ := mysqlType(typ)
		got, err := encodeDatumBinary(tree.DNull, colType, testConv())
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("type 0x%02x: NULL encoded as % x, want no bytes", colType, got)
		}
	}
}

// TestEncodeDatumText pins the text-protocol spellings that clients parse back
// into native types. These are MySQL's own formats, not KWDB's: a timestamp
// rendered the PostgreSQL way, or a boolean rendered as "t", would reach the
// client as a string it cannot convert.
func TestEncodeDatumText(t *testing.T) {
	defer leaktest.AfterTest(t)()

	conv := testConv()
	for _, tc := range []struct {
		name  string
		datum tree.Datum
		want  string
	}{
		{"bool true is 1 not t", tree.MakeDBool(true), "1"},
		{"bool false is 0 not f", tree.MakeDBool(false), "0"},
		{"int", tree.NewDInt(42), "42"},
		{"string", tree.NewDString("hi"), "hi"},
		{"bytes are raw", tree.NewDBytes("\x00\xff"), "\x00\xff"},
		{"date", mustDate(t, "2026-08-19"), "2026-08-19"},
		{
			"timestamp uses a space, not T",
			tree.MakeDTimestamp(time.Date(2026, 8, 19, 12, 34, 56, 0, time.UTC), time.Microsecond),
			"2026-08-19 12:34:56",
		},
		{"time", tree.MakeDTime(timeofday.New(1, 2, 3, 0)), "01:02:03"},
		{
			// MySQL allows six fractional digits and clients reject more; see
			// mysqlTimestampFormat.
			"timestamp stops at microseconds",
			tree.MakeDTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC), time.Nanosecond),
			"2024-01-01 00:00:00.123456",
		},
		{
			"timetz keeps its offset",
			tree.NewDTimeTZFromLocation(timeofday.New(11, 0, 0, 0), time.FixedZone("", -5*3600)),
			"11:00:00-05:00",
		},
		{"int array in PostgreSQL text form", intArray(t, 1, 2), "{1,2}"},
		{
			// The empty string, the string 'NULL' and SQL NULL must stay
			// distinguishable, and non-ASCII text must not be escaped.
			"string array keeps every element distinguishable",
			stringArray(t, "", "NULL", nil, "fünf"),
			`{"","NULL",NULL,fünf}`,
		},
		{
			"tuple in PostgreSQL text form",
			tree.NewDTuple(
				types.MakeTuple([]types.T{*types.Int, *types.String, *types.String}),
				tree.NewDInt(1), tree.NewDString("hello"), tree.DNull),
			"(1,hello,)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeDatum(tc.datum, conv)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}

	got, err := encodeDatum(tree.DNull, conv)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("NULL encoded as %q, want nil", got)
	}
}

// TestMySQLTypeFlags checks the column metadata clients branch on: the NUM flag
// drives numeric coercion and right-alignment, and BLOB-like columns must carry
// the binary collation so that clients do not reinterpret bytes as text in the
// connection's character set.
func TestMySQLTypeFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name        string
		typ         *types.T
		wantNum     bool
		wantCharset uint16
	}{
		{"int is numeric", types.Int, true, mysqlwirebase.CollationUTF8MB4GeneralCI},
		{"float is numeric", types.Float, true, mysqlwirebase.CollationUTF8MB4GeneralCI},
		{"decimal is numeric", types.Decimal, true, mysqlwirebase.CollationUTF8MB4GeneralCI},
		{"bool is numeric", types.Bool, true, mysqlwirebase.CollationUTF8MB4GeneralCI},
		{"string is not numeric", types.String, false, mysqlwirebase.CollationUTF8MB4GeneralCI},
		{"bytes use the binary collation", types.Bytes, false, mysqlwirebase.CollationBinary},
		{"json uses the binary collation", types.Jsonb, false, mysqlwirebase.CollationBinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, flags, charset := mysqlType(tc.typ)
			if gotNum := flags&mysqlwirebase.ColumnFlagNum != 0; gotNum != tc.wantNum {
				t.Errorf("NUM flag = %v, want %v", gotNum, tc.wantNum)
			}
			if charset != tc.wantCharset {
				t.Errorf("charset = %d, want %d", charset, tc.wantCharset)
			}
		})
	}
}
