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
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// columnDefinition describes a KWDB result column in the terms a MySQL client
// understands.
//
// The mapping is lossy in one direction that matters: KWDB has types MySQL
// does not (INET, UUID, arrays, intervals, tuples). Those are reported as
// variable-length strings, which is what the text protocol transmits anyway,
// so a client sees the same rendering it would get from KWDB's PostgreSQL
// interface. What it loses is the ability to introspect the type; a driver
// asking "is this column an integer" gets "no, it is a string", which is
// accurate as far as MySQL's type system can express it.
//
// schema and table name the column's source, and appear in the metadata some
// clients display. They are best-effort: expressions have no source table.
func columnDefinition(
	col sqlbase.ResultColumn, schema, table string,
) *mysqlwirebase.ColumnDefinition {
	typ, length, decimals, flags, charset := mysqlType(col.Typ)
	return &mysqlwirebase.ColumnDefinition{
		Schema:       schema,
		Table:        table,
		OrgTable:     table,
		Name:         col.Name,
		OrgName:      col.Name,
		CharacterSet: charset,
		ColumnLength: length,
		Type:         typ,
		Flags:        flags,
		Decimals:     decimals,
	}
}

// decimalsNotApplicable is the value MySQL uses in the decimals field for
// types that have no fractional part. 31 (rather than 0) is what MySQL sends
// for string and blob columns, and some clients use it to tell "no scale" from
// "scale zero".
const decimalsNotApplicable = 31

// mysqlType maps a KWDB type onto the column type, display length, scale,
// flags, and collation a MySQL client expects.
//
// The lengths are display widths, which clients use to size output columns;
// they do not constrain what the server may send. The values below are what
// MySQL reports for the corresponding native type, so that `mysql` renders
// KWDB results with the same column widths it would render MySQL's.
func mysqlType(t *types.T) (typ byte, length uint32, decimals byte, flags uint16, charset uint16) {
	// Strings are the default: anything without a closer MySQL equivalent is
	// rendered as text, which the text protocol transmits faithfully.
	typ = mysqlwirebase.TypeVarString
	length = 65535
	decimals = decimalsNotApplicable
	charset = mysqlwirebase.CollationUTF8MB4GeneralCI

	switch t.Family() {
	case types.BoolFamily:
		// MySQL has no boolean type; BOOL is an alias for TINYINT(1), and
		// clients recognize the width-1 TINY column as a boolean.
		typ, length, decimals = mysqlwirebase.TypeTiny, 1, 0

	case types.IntFamily:
		decimals = 0
		switch t.Width() {
		case 16:
			typ, length = mysqlwirebase.TypeShort, 6
		case 32:
			typ, length = mysqlwirebase.TypeLong, 11
		default:
			typ, length = mysqlwirebase.TypeLongLong, 20
		}

	case types.FloatFamily:
		decimals = decimalsNotApplicable
		if t.Width() == 32 {
			typ, length = mysqlwirebase.TypeFloat, 12
		} else {
			typ, length = mysqlwirebase.TypeDouble, 22
		}

	case types.DecimalFamily:
		typ = mysqlwirebase.TypeNewDecimal
		// Precision 0 means unrestricted in KWDB, which MySQL cannot express;
		// report its maximum instead of a zero width that clients render as an
		// empty column.
		if p := t.Precision(); p > 0 {
			length = uint32(p) + 2 // sign and decimal point
			decimals = byte(t.Scale())
		} else {
			length, decimals = 65, 30
		}

	case types.DateFamily:
		typ, length, decimals = mysqlwirebase.TypeDate, 10, 0

	case types.TimeFamily:
		// TIMETZ is deliberately not here and falls to the string default.
		// MySQL has no zone-aware time type, and a TIME column cannot carry
		// the offset: the binary protocol has no field for it, and clients
		// parsing the text as TIME reject it.
		typ, length, decimals = mysqlwirebase.TypeTime, 17, 6

	case types.TimestampFamily, types.TimestampTZFamily:
		typ, length, decimals = mysqlwirebase.TypeTimestamp, 26, 6

	case types.BytesFamily:
		// The binary collation stops clients from re-interpreting the bytes as
		// text in whatever charset the connection negotiated.
		typ, length, charset = mysqlwirebase.TypeBlob, 65535, mysqlwirebase.CollationBinary
		flags |= mysqlwirebase.ColumnFlagBinary | mysqlwirebase.ColumnFlagBlob

	case types.JsonFamily:
		typ, length, charset = mysqlwirebase.TypeJSON, 4294967295, mysqlwirebase.CollationBinary
		flags |= mysqlwirebase.ColumnFlagBinary | mysqlwirebase.ColumnFlagBlob

	case types.BitFamily:
		typ, length, decimals = mysqlwirebase.TypeBit, uint32(t.Width()), 0
		flags |= mysqlwirebase.ColumnFlagUnsigned

	case types.StringFamily, types.CollatedStringFamily:
		typ = mysqlwirebase.TypeVarString
		if w := t.Width(); w > 0 {
			length = uint32(w)
		}
	}

	if numericFamily(t.Family()) {
		flags |= mysqlwirebase.ColumnFlagNum
	}
	return typ, length, decimals, flags, charset
}

// numericFamily reports whether clients should treat the column as a number.
// The NUM flag drives right-alignment in the `mysql` CLI and numeric coercion
// in some drivers.
func numericFamily(f types.Family) bool {
	switch f {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily, types.BoolFamily:
		return true
	default:
		return false
	}
}
