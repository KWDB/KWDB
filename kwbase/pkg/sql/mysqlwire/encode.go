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
	"strconv"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
)

// MySQL's text protocol renders every value as a string; these are the formats
// MySQL itself uses, which is what clients parse when they convert a column
// back into a native type.
//
// Timestamps stop at microseconds even though KWDB keeps nanoseconds, so the
// digits past the sixth are lost on this interface. That is deliberate: MySQL
// allows at most six fractional digits, and clients rely on it -- go-sql-driver
// with parseTime rejects a seventh as "invalid time string", and KWDB's
// timestamps default to nanosecond precision, so now() alone would break every
// such client. The binary protocol has no field for anything finer either.
const (
	mysqlDateFormat      = "2006-01-02"
	mysqlTimestampFormat = "2006-01-02 15:04:05.999999"
	mysqlTimeFormat      = "15:04:05.999999"
	// mysqlTimeTZFormat is the PostgreSQL interface's TIMETZ spelling. MySQL has
	// no zone-aware time, so there is no MySQL spelling to match; mysqlType
	// reports these columns as strings, and dropping the offset would turn
	// different times into equal-looking ones.
	mysqlTimeTZFormat = "15:04:05.999999-07:00"
)

// encodeDatum renders a KWDB datum as the bytes a MySQL client expects for the
// column type mysqlType reported. A nil return means SQL NULL.
//
// Several KWDB types have no MySQL counterpart and are reported as strings by
// mysqlType; for those, this renders KWDB's own textual form, which is the
// same text the PostgreSQL interface would produce. The two interfaces
// therefore agree on the value even where they disagree on the type.
func encodeDatum(d tree.Datum, conv sessiondata.DataConversionConfig) ([]byte, error) {
	if d == tree.DNull {
		return nil, nil
	}

	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBool:
		// MySQL renders booleans as the TINYINT values clients coerce back to
		// true and false; "t"/"f" as PostgreSQL spells them would not survive
		// that coercion.
		if bool(*v) {
			return []byte("1"), nil
		}
		return []byte("0"), nil

	case *tree.DInt:
		return strconv.AppendInt(nil, int64(*v), 10), nil

	case *tree.DFloat:
		return strconv.AppendFloat(nil, float64(*v), 'g', conv.GetFloatPrec(), 64), nil

	case *tree.DDecimal:
		return []byte(v.Decimal.String()), nil

	case *tree.DString:
		return []byte(*v), nil

	case *tree.DCollatedString:
		return []byte(v.Contents), nil

	case *tree.DBytes:
		// The column is reported with the binary collation, so the raw bytes go
		// on the wire unescaped. Applying KWDB's hex or escape encoding here
		// would hand the client a rendering of the bytes rather than the bytes.
		return []byte(*v), nil

	case *tree.DUuid:
		return []byte(v.UUID.String()), nil

	case *tree.DIPAddr:
		return []byte(v.IPAddr.String()), nil

	case *tree.DDate:
		t, err := v.ToTime()
		if err != nil {
			// Infinite dates have no MySQL representation; their KWDB spelling
			// is more useful to a reader than an error that kills the query.
			return []byte(v.String()), nil
		}
		return []byte(t.Format(mysqlDateFormat)), nil

	case *tree.DTime:
		return []byte(timeofday.TimeOfDay(*v).ToTime().Format(mysqlTimeFormat)), nil

	case *tree.DTimeTZ:
		return []byte(v.TimeTZ.ToTime().Format(mysqlTimeTZFormat)), nil

	case *tree.DTimestamp:
		return []byte(v.Time.Format(mysqlTimestampFormat)), nil

	case *tree.DTimestampTZ:
		// Render in the session's zone, matching what the PostgreSQL interface
		// does, so both interfaces show a user the same wall-clock time.
		loc := conv.Location
		if loc == nil {
			loc = time.UTC
		}
		return []byte(v.Time.In(loc).Format(mysqlTimestampFormat)), nil

	case *tree.DJSON:
		return []byte(v.JSON.String()), nil

	case *tree.DBitArray, *tree.DInterval, *tree.DTuple, *tree.DArray:
		// No MySQL type corresponds to these; mysqlType reports them as
		// strings, and they carry the text the PostgreSQL interface sends.
		// FmtBareStrings, used before, renders SQL syntax instead: ARRAY[...]
		// that no client parses, with the empty string dropped, 'NULL' made
		// indistinguishable from NULL, and non-ASCII text escaped.
		return []byte(tree.AsStringWithFlags(v, tree.FmtPgwireText)), nil

	case *tree.DOid:
		// As the PostgreSQL interface sends it: the bare number.
		return []byte(tree.AsStringWithFlags(v, tree.FmtSimple)), nil

	default:
		return nil, errors.AssertionFailedf("mysqlwire: unsupported datum type %T", d)
	}
}

// encodeDatumBinary renders a KWDB datum in the binary protocol, which is what
// a result set produced by a prepared statement carries. A nil return means
// SQL NULL.
//
// colType selects the encoding, not the datum: it must be the type mysqlType
// reported for this column, because the client reads each value at the width
// the column definition declared. A mismatch here does not produce one wrong
// value, it desynchronises every value after it. Every type mysqlType can
// report must therefore be handled, and anything without a fixed-width binary
// form falls through to a length-encoded string carrying exactly the bytes the
// text protocol would have sent.
func encodeDatumBinary(
	d tree.Datum, colType byte, conv sessiondata.DataConversionConfig,
) ([]byte, error) {
	if d == tree.DNull {
		return nil, nil
	}

	w := mysqlwirebase.NewWriter(16)
	unwrapped := tree.UnwrapDatum(nil, d)

	switch colType {
	case mysqlwirebase.TypeTiny, mysqlwirebase.TypeShort, mysqlwirebase.TypeLong,
		mysqlwirebase.TypeInt24, mysqlwirebase.TypeLongLong, mysqlwirebase.TypeYear:
		var v int64
		switch t := unwrapped.(type) {
		case *tree.DInt:
			v = int64(*t)
		case *tree.DBool:
			// BOOL is TINYINT(1) in MySQL; clients coerce 0 and 1 back to a
			// boolean.
			if bool(*t) {
				v = 1
			}
		default:
			return nil, errors.AssertionFailedf(
				"mysqlwire: datum %T cannot be encoded as MySQL integer type 0x%02x", d, colType)
		}
		if err := mysqlwirebase.WriteBinaryInt(w, colType, v); err != nil {
			return nil, err
		}
		return w.Bytes(), nil

	case mysqlwirebase.TypeFloat, mysqlwirebase.TypeDouble:
		f, ok := unwrapped.(*tree.DFloat)
		if !ok {
			return nil, errors.AssertionFailedf(
				"mysqlwire: datum %T cannot be encoded as MySQL float type 0x%02x", d, colType)
		}
		if err := mysqlwirebase.WriteBinaryFloat(w, colType, float64(*f)); err != nil {
			return nil, err
		}
		return w.Bytes(), nil

	case mysqlwirebase.TypeDate:
		v, ok := unwrapped.(*tree.DDate)
		if !ok {
			return nil, errors.AssertionFailedf(
				"mysqlwire: datum %T cannot be encoded as MySQL DATE", d)
		}
		t, err := v.ToTime()
		if err != nil {
			// Infinite dates have no MySQL representation. The zero date is the
			// closest the format can carry; the text path is equally lossy here,
			// sending a spelling no MySQL client can parse as a date.
			w.Byte(0)
			return w.Bytes(), nil
		}
		mysqlwirebase.WriteBinaryDateTime(w, t.Year(), int(t.Month()), t.Day(), 0, 0, 0, 0)
		return w.Bytes(), nil

	case mysqlwirebase.TypeTimestamp, mysqlwirebase.TypeDatetime:
		var t time.Time
		switch v := unwrapped.(type) {
		case *tree.DTimestamp:
			t = v.Time
		case *tree.DTimestampTZ:
			// Render in the session's zone, matching the text path, so both
			// interfaces show a user the same wall-clock time.
			loc := conv.Location
			if loc == nil {
				loc = time.UTC
			}
			t = v.Time.In(loc)
		default:
			return nil, errors.AssertionFailedf(
				"mysqlwire: datum %T cannot be encoded as MySQL TIMESTAMP", d)
		}
		mysqlwirebase.WriteBinaryDateTime(w,
			t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000)
		return w.Bytes(), nil

	case mysqlwirebase.TypeTime:
		var tod timeofday.TimeOfDay
		switch v := unwrapped.(type) {
		case *tree.DTime:
			tod = timeofday.TimeOfDay(*v)
		default:
			// TIMETZ is reported as a string (see mysqlType) and never gets
			// here: this type has no field for the offset.
			return nil, errors.AssertionFailedf(
				"mysqlwire: datum %T cannot be encoded as MySQL TIME", d)
		}
		t := tod.ToTime()
		mysqlwirebase.WriteBinaryTime(w, false /* negative */, 0, /* days */
			t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000)
		return w.Bytes(), nil

	default:
		// DECIMAL, the string and blob families, BIT, JSON, and every KWDB type
		// mysqlType reports as a string. MySQL transmits all of these as
		// length-encoded strings in the binary protocol too, with the same bytes
		// the text protocol uses.
		text, err := encodeDatum(d, conv)
		if err != nil {
			return nil, err
		}
		w.LenEncBytes(text)
		return w.Bytes(), nil
	}
}
