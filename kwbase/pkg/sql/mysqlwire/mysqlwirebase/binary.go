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
	"fmt"
	"math"
	"strconv"
)

// The binary protocol is what MySQL uses for prepared statements: parameters
// travel from client to server and rows travel back encoded by type rather
// than as text. The two directions share one value encoding, so the readers
// and writers here serve both COM_STMT_EXECUTE parameters and binary result
// rows.
//
// Values are rendered to and parsed from MySQL's text-protocol spelling at
// this boundary. That keeps the SQL layer working in one representation: the
// rest of mysqlwire already converts between KWDB datums and MySQL text, and
// duplicating that conversion for the binary case would be two chances to
// disagree about, say, how a timestamp is spelled.

// NullBitmapLen returns the size of the NULL bitmap covering n fields.
//
// offset is the number of leading bits the bitmap reserves before the first
// field. Result rows reserve 2 (a quirk of the format); parameter blocks
// reserve none.
func NullBitmapLen(n, offset int) int {
	return (n + offset + 7) / 8
}

// NullBitmapGet reports whether field i is NULL.
func NullBitmapGet(bitmap []byte, i, offset int) bool {
	pos := i + offset
	idx := pos / 8
	if idx >= len(bitmap) {
		return false
	}
	return bitmap[idx]&(1<<uint(pos%8)) != 0
}

// NullBitmapSet marks field i as NULL.
func NullBitmapSet(bitmap []byte, i, offset int) {
	pos := i + offset
	idx := pos / 8
	if idx >= len(bitmap) {
		return
	}
	bitmap[idx] |= 1 << uint(pos%8)
}

// binaryRowNullOffset is the number of bits a result row's NULL bitmap
// reserves before its first column. MySQL defines it as 2; the bits carry no
// meaning but a client that ignores the offset reads every column's null flag
// from the wrong bit.
const binaryRowNullOffset = 2

// EncodeBinaryRow assembles one row of a binary-protocol result set.
//
// values holds each column already encoded in its binary form, as produced by
// the WriteBinary* helpers for the type reported in the column definition. A
// nil entry encodes SQL NULL, in which case no bytes are emitted for that
// column: the NULL bitmap alone carries it.
func EncodeBinaryRow(values [][]byte) []byte {
	bitmapLen := NullBitmapLen(len(values), binaryRowNullOffset)
	n := 1 + bitmapLen
	for _, v := range values {
		n += len(v)
	}

	w := NewWriter(n)
	w.Byte(OKPacketHeader) // binary rows are introduced by 0x00

	bitmap := make([]byte, bitmapLen)
	for i, v := range values {
		if v == nil {
			NullBitmapSet(bitmap, i, binaryRowNullOffset)
		}
	}
	w.Raw(bitmap)

	for _, v := range values {
		if v != nil {
			w.Raw(v)
		}
	}
	return w.Bytes()
}

// DecodeBinaryRow parses a binary-protocol row whose columns have the given
// MySQL types, returning each value in its text-protocol spelling with nil for
// SQL NULL. Only the test client needs this.
func DecodeBinaryRow(payload []byte, types []byte) ([][]byte, error) {
	r := NewReader(payload)
	header, err := r.Byte()
	if err != nil {
		return nil, fmt.Errorf("reading row header: %w", err)
	}
	if header != OKPacketHeader {
		return nil, fmt.Errorf("expected binary row header 0x00, got 0x%02x", header)
	}

	bitmap, err := r.Bytes(NullBitmapLen(len(types), binaryRowNullOffset))
	if err != nil {
		return nil, fmt.Errorf("reading NULL bitmap: %w", err)
	}

	out := make([][]byte, len(types))
	for i, typ := range types {
		if NullBitmapGet(bitmap, i, binaryRowNullOffset) {
			continue
		}
		v, _, err := ReadBinaryValue(r, typ, false /* unsigned */)
		if err != nil {
			return nil, fmt.Errorf("reading column %d: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}

// WriteBinaryInt appends an integer in the width the given MySQL type
// declares. The width must match what the column definition reported, because
// the client sizes its read off the declared type rather than off the data.
func WriteBinaryInt(w *Writer, typ byte, v int64) error {
	switch typ {
	case TypeTiny:
		w.Byte(byte(v))
	case TypeShort, TypeYear:
		w.Uint16(uint16(v))
	case TypeLong, TypeInt24:
		w.Uint32(uint32(v))
	case TypeLongLong:
		w.Uint64(uint64(v))
	default:
		return fmt.Errorf("type 0x%02x is not an integer type", typ)
	}
	return nil
}

// WriteBinaryFloat appends a floating-point value in the width the given MySQL
// type declares.
func WriteBinaryFloat(w *Writer, typ byte, v float64) error {
	switch typ {
	case TypeFloat:
		w.Uint32(math.Float32bits(float32(v)))
	case TypeDouble:
		w.Uint64(math.Float64bits(v))
	default:
		return fmt.Errorf("type 0x%02x is not a floating-point type", typ)
	}
	return nil
}

// WriteBinaryDateTime appends a date or timestamp.
//
// The trailing components are omitted when they are zero, which is what MySQL
// itself does; the leading length byte tells the client how much to read.
func WriteBinaryDateTime(w *Writer, year int, month, day, hour, minute, second int, micros int) {
	switch {
	case year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && micros == 0:
		w.Byte(0)
	case micros != 0:
		w.Byte(11)
		w.Uint16(uint16(year))
		w.Byte(byte(month))
		w.Byte(byte(day))
		w.Byte(byte(hour))
		w.Byte(byte(minute))
		w.Byte(byte(second))
		w.Uint32(uint32(micros))
	case hour != 0 || minute != 0 || second != 0:
		w.Byte(7)
		w.Uint16(uint16(year))
		w.Byte(byte(month))
		w.Byte(byte(day))
		w.Byte(byte(hour))
		w.Byte(byte(minute))
		w.Byte(byte(second))
	default:
		w.Byte(4)
		w.Uint16(uint16(year))
		w.Byte(byte(month))
		w.Byte(byte(day))
	}
}

// WriteBinaryTime appends a TIME value, which MySQL expresses as a signed
// day-and-clock offset rather than a point in time.
func WriteBinaryTime(w *Writer, negative bool, days uint32, hour, minute, second int, micros int) {
	if days == 0 && hour == 0 && minute == 0 && second == 0 && micros == 0 {
		w.Byte(0)
		return
	}
	neg := byte(0)
	if negative {
		neg = 1
	}
	if micros != 0 {
		w.Byte(12)
	} else {
		w.Byte(8)
	}
	w.Byte(neg)
	w.Uint32(days)
	w.Byte(byte(hour))
	w.Byte(byte(minute))
	w.Byte(byte(second))
	if micros != 0 {
		w.Uint32(uint32(micros))
	}
}

// ReadBinaryValue consumes one binary-protocol value of the given MySQL type.
//
// The value comes back in its text-protocol spelling, so that callers can hand
// it to the same parsing the text protocol already uses. The second return
// value reports a binary string: bytes that carry no character encoding and
// must not be reinterpreted as text.
func ReadBinaryValue(
	r *Reader, typ byte, unsigned bool,
) (val []byte, binaryString bool, err error) {
	switch typ {
	case TypeNull:
		return nil, false, nil

	case TypeTiny:
		b, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		if unsigned {
			return strconv.AppendUint(nil, uint64(b), 10), false, nil
		}
		return strconv.AppendInt(nil, int64(int8(b)), 10), false, nil

	case TypeShort, TypeYear:
		v, err := r.Uint16()
		if err != nil {
			return nil, false, err
		}
		if unsigned {
			return strconv.AppendUint(nil, uint64(v), 10), false, nil
		}
		return strconv.AppendInt(nil, int64(int16(v)), 10), false, nil

	case TypeLong, TypeInt24:
		v, err := r.Uint32()
		if err != nil {
			return nil, false, err
		}
		if unsigned {
			return strconv.AppendUint(nil, uint64(v), 10), false, nil
		}
		return strconv.AppendInt(nil, int64(int32(v)), 10), false, nil

	case TypeLongLong:
		v, err := r.Uint64()
		if err != nil {
			return nil, false, err
		}
		if unsigned {
			return strconv.AppendUint(nil, v, 10), false, nil
		}
		return strconv.AppendInt(nil, int64(v), 10), false, nil

	case TypeFloat:
		v, err := r.Uint32()
		if err != nil {
			return nil, false, err
		}
		// 32 bits of precision: rendering at float64 width would append digits
		// the client never sent.
		return strconv.AppendFloat(nil, float64(math.Float32frombits(v)), 'g', -1, 32), false, nil

	case TypeDouble:
		v, err := r.Uint64()
		if err != nil {
			return nil, false, err
		}
		return strconv.AppendFloat(nil, math.Float64frombits(v), 'g', -1, 64), false, nil

	case TypeDate, TypeNewDate, TypeDatetime, TypeTimestamp:
		return readBinaryDateTime(r)

	case TypeTime:
		return readBinaryTime(r)

	default:
		// Everything else travels as a length-encoded string: DECIMAL and
		// NEWDECIMAL (MySQL never sends these as binary floats, to preserve
		// exactness), the string and blob families, BIT, ENUM, SET, JSON and
		// GEOMETRY.
		b, err := r.LenEncString()
		if err != nil {
			return nil, false, err
		}
		return b, isBinaryStringType(typ), nil
	}
}

// isBinaryStringType reports whether a length-encoded value of this type is
// opaque bytes rather than text in the connection's character set.
func isBinaryStringType(typ byte) bool {
	switch typ {
	case TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeBit, TypeGeometry:
		return true
	default:
		return false
	}
}

// readBinaryDateTime parses a DATE, DATETIME or TIMESTAMP into the text form
// MySQL would have sent for the same value.
func readBinaryDateTime(r *Reader) ([]byte, bool, error) {
	n, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	var year, month, day, hour, minute, second, micros int
	switch n {
	case 0:
		// MySQL's "zero date". It has no valid calendar reading, and rendering
		// it as MySQL spells it lets the SQL layer reject it with a message
		// about the value rather than about a truncated packet.
		return []byte("0000-00-00"), false, nil
	case 4, 7, 11:
		y, err := r.Uint16()
		if err != nil {
			return nil, false, err
		}
		mo, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		d, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		year, month, day = int(y), int(mo), int(d)
	default:
		return nil, false, fmt.Errorf("invalid datetime length %d", n)
	}

	if n >= 7 {
		h, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		mi, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		s, err := r.Byte()
		if err != nil {
			return nil, false, err
		}
		hour, minute, second = int(h), int(mi), int(s)
	}
	if n == 11 {
		us, err := r.Uint32()
		if err != nil {
			return nil, false, err
		}
		micros = int(us)
	}

	if n == 4 {
		return []byte(fmt.Sprintf("%04d-%02d-%02d", year, month, day)), false, nil
	}
	if micros != 0 {
		return []byte(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
			year, month, day, hour, minute, second, micros)), false, nil
	}
	return []byte(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		year, month, day, hour, minute, second)), false, nil
}

// readBinaryTime parses a TIME into the text form MySQL would have sent.
//
// MySQL's TIME is a signed duration, not a clock reading: it carries a day
// count and can exceed 24 hours or go negative. The days are folded into the
// hour field, which is how MySQL renders it.
func readBinaryTime(r *Reader) ([]byte, bool, error) {
	n, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	if n == 0 {
		return []byte("00:00:00"), false, nil
	}
	if n != 8 && n != 12 {
		return nil, false, fmt.Errorf("invalid time length %d", n)
	}

	neg, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	days, err := r.Uint32()
	if err != nil {
		return nil, false, err
	}
	hour, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	minute, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	second, err := r.Byte()
	if err != nil {
		return nil, false, err
	}
	micros := uint32(0)
	if n == 12 {
		if micros, err = r.Uint32(); err != nil {
			return nil, false, err
		}
	}

	sign := ""
	if neg != 0 {
		sign = "-"
	}
	totalHours := uint64(days)*24 + uint64(hour)
	if micros != 0 {
		return []byte(fmt.Sprintf("%s%02d:%02d:%02d.%06d",
			sign, totalHours, minute, second, micros)), false, nil
	}
	return []byte(fmt.Sprintf("%s%02d:%02d:%02d", sign, totalHours, minute, second)), false, nil
}

// Param is one decoded COM_STMT_EXECUTE parameter.
type Param struct {
	// Null is true for a SQL NULL argument; Value is then nil.
	Null bool
	// Binary marks a value that is opaque bytes rather than text.
	Binary bool
	// Value is the argument in MySQL's text-protocol spelling, or the raw bytes
	// when Binary is set.
	Value []byte
}

// ParamType is the type a client declared for one parameter.
type ParamType struct {
	Type     byte
	Unsigned bool
}

// paramUnsignedFlag is the high bit of the second type byte in a
// COM_STMT_EXECUTE parameter type block.
const paramUnsignedFlag = 0x80

// ReadParamTypes consumes the parameter type block of a COM_STMT_EXECUTE
// packet: two bytes per parameter, the second carrying the unsigned flag.
func ReadParamTypes(r *Reader, numParams int) ([]ParamType, error) {
	types := make([]ParamType, numParams)
	for i := 0; i < numParams; i++ {
		t, err := r.Byte()
		if err != nil {
			return nil, fmt.Errorf("reading type of parameter %d: %w", i, err)
		}
		flags, err := r.Byte()
		if err != nil {
			return nil, fmt.Errorf("reading flags of parameter %d: %w", i, err)
		}
		types[i] = ParamType{Type: t, Unsigned: flags&paramUnsignedFlag != 0}
	}
	return types, nil
}

// ReadParams consumes the parameter values of a COM_STMT_EXECUTE packet.
//
// nullBitmap marks arguments the client sent as NULL; those consume no bytes
// in the value block. longData supplies arguments already accumulated by
// COM_STMT_SEND_LONG_DATA, which likewise consume no bytes here: a nil entry
// means the argument was not streamed.
func ReadParams(
	r *Reader, types []ParamType, nullBitmap []byte, longData [][]byte,
) ([]Param, error) {
	params := make([]Param, len(types))
	for i, pt := range types {
		if i < len(longData) && longData[i] != nil {
			// A streamed argument is always a string or blob; its type byte is
			// whatever the client declared, which for LONG DATA is a blob type.
			params[i] = Param{Value: longData[i], Binary: isBinaryStringType(pt.Type)}
			continue
		}
		if NullBitmapGet(nullBitmap, i, 0 /* offset */) || pt.Type == TypeNull {
			params[i] = Param{Null: true}
			continue
		}
		v, isBin, err := ReadBinaryValue(r, pt.Type, pt.Unsigned)
		if err != nil {
			return nil, fmt.Errorf("reading parameter %d: %w", i, err)
		}
		params[i] = Param{Value: v, Binary: isBin}
	}
	return params, nil
}

// EncodeStmtPrepareOK serializes the first packet of a COM_STMT_PREPARE
// response. The parameter and column definitions follow it as separate
// packets. See "COM_STMT_PREPARE_OK".
func EncodeStmtPrepareOK(stmtID uint32, numColumns, numParams uint16, warnings uint16) []byte {
	w := NewWriter(12)
	w.Byte(OKPacketHeader)
	w.Uint32(stmtID)
	w.Uint16(numColumns)
	w.Uint16(numParams)
	w.Byte(0) // reserved filler
	w.Uint16(warnings)
	return w.Bytes()
}
