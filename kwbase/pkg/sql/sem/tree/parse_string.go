// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"strconv"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ParseAndRequireString parses s as type t for simple types. Arrays and collated
// strings are not handled.
func ParseAndRequireString(t *types.T, s string, ctx ParseTimeContext) (Datum, error) {
	switch t.Family() {
	case types.ArrayFamily:
		return ParseDArrayFromString(ctx, s, t.ArrayContents())
	case types.BitFamily:
		return ParseDBitArray(s)
	case types.BoolFamily:
		return ParseDBool(s)
	case types.BytesFamily:
		return ParseDByte(s)
	case types.DateFamily:
		return ParseDDate(ctx, s)
	case types.DecimalFamily:
		return ParseDDecimal(s)
	case types.FloatFamily:
		return ParseDFloat(s)
	case types.INetFamily:
		return ParseDIPAddrFromINetString(s)
	case types.IntFamily:
		return ParseDInt(s)
	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		return ParseDIntervalWithTypeMetadata(s, itm)
	case types.JsonFamily:
		return ParseDJSON(s)
	case types.OidFamily:
		i, err := ParseDInt(s)
		return NewDOid(*i), err
	case types.StringFamily:
		// If the string type specifies a limit we truncate to that limit:
		//   'hello'::CHAR(2) -> 'he'
		// This is true of all the string type variants.
		if t.Width() > 0 {
			s = util.TruncateString(s, int(t.Width()))
		}
		return NewDString(s), nil
	case types.CollatedStringFamily:
		// If the string type specifies a limit we truncate to that limit:
		//   'hello'::CHAR(2) -> 'he'
		// This is true of all the string type variants.
		if t.Oid() == types.T_citext {
			if t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			d, err := NewDCIText(s, &CollationEnvironment{})
			return d, err
		}
		return nil, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	case types.TimeFamily:
		return ParseDTime(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimeTZFamily:
		return ParseDTimeTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampFamily:
		if t.InternalType.Precision == 0 && !t.InternalType.TimePrecisionIsSet {
			return ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(9))
		}
		return ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampTZFamily:
		if t.InternalType.Precision == 0 && !t.InternalType.TimePrecisionIsSet {
			return ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(9))
		}
		return ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.UuidFamily:
		return ParseDUuidFromString(s)
	default:
		return nil, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	}
}

// TSParseAndRequireString parses s as ts type typ for simple types.
// It's same like ParseAndRequireString, but only used by import.
func TSParseAndRequireString(
	colName string, typ *types.T, s string, ctx ParseTimeContext,
) (Datum, error) {
	// Typing a string literal constant into some value type.
	switch typ.Family() {
	case types.StringFamily:
		switch typ.Oid() {
		case oid.T_text, oid.T_varchar, oid.T_bpchar, types.T_geometry:
			// check length, string(n)/char(n)/varchar(n) is calculated length by byte
			if len(s) > int(typ.Width()) {
				return DNull, pgerror.Newf(pgcode.StringDataRightTruncation,
					"value '%s' too long for type %s (column %s)", s, typ.SQLString(), colName)
			}
		case types.T_nchar, types.T_nvarchar:
			// check length, nchar(n)/nvarchar(n) is calculated length by rune
			if utf8.RuneCountInString(s) > int(typ.Width()) {
				return DNull, pgerror.Newf(pgcode.StringDataRightTruncation,
					"value '%s' too long for type %s (column %s)", s, typ.SQLString(), colName)
			}
		default:
			return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
		}
		return NewDString(s), nil

	case types.BytesFamily:
		v, err := ParseDByte(s)
		if err != nil {
			return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
		}
		// check length, bytes(n)/varbytes(n) is calculated length by byte
		if len(string(*v)) > int(typ.Width()) {
			return DNull, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value '%s' too long for type %s (column %s)", s, typ.SQLString(), colName)
		}
		return v, nil

	case types.TimestampFamily:
		t, err := ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(typ.Precision()))
		if err != nil {
			i, err1 := strconv.ParseInt(s, 0, 64)
			if err1 != nil {
				return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
			}
			if i < TsMinTimestamp || i > TsMaxTimestamp {
				return DNull, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"integer \"%d\" out of range for type %s (column %s)", i, typ.SQLString(), colName)
			}
			return NewDInt(DInt(i)), nil
		}
		dVal := t.UnixMilli()
		// check the maximum and minimum value of the timestamp
		if dVal < TsMinTimestamp || dVal > TsMaxTimestamp {
			i, err1 := strconv.ParseInt(s, 0, 64)
			if err1 != nil {
				return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
			}
			if i < TsMinTimestamp || i > TsMaxTimestamp {
				return DNull, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"integer \"%d\" out of range for type %s (column %s)", i, typ.SQLString(), colName)
			}
			return NewDInt(DInt(i)), nil
		}
		return NewDInt(DInt(dVal)), nil
	case types.TimestampTZFamily:
		t, err := ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(typ.Precision()))
		if err != nil {
			if err != nil {
				i, err1 := strconv.ParseInt(s, 0, 64)
				if err1 != nil {
					return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
				}
				if i < TsMinTimestamp || i > TsMaxTimestamp {
					return DNull, pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer \"%d\" out of range for type %s (column %s)", i, typ.SQLString(), colName)
				}
				return NewDInt(DInt(i)), nil
			}
		}
		dVal := t.UnixMilli()
		// check the maximum and minimum value of the timestamp
		if dVal < TsMinTimestamp || dVal > TsMaxTimestamp {
			i, err1 := strconv.ParseInt(s, 0, 64)
			if err1 != nil {
				return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
			}
			if i < TsMinTimestamp || i > TsMaxTimestamp {
				return DNull, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"integer \"%d\" out of range for type %s (column %s)", i, typ.SQLString(), colName)
			}
			return NewDInt(DInt(i)), nil
		}
		return NewDInt(DInt(dVal)), nil
	case types.IntFamily:
		i, err := strconv.ParseInt(s, 0, 64)
		if err != nil {
			return DNull, makeParseError(s, types.Int, err)
		}
		// Width is defined in bits.
		width := uint(typ.Width() - 1)

		// We're performing bounds checks inline with Go's implementation of min and max int in Math.go.
		shifted := i >> width
		if (i >= 0 && shifted > 0) || (i < 0 && shifted < -1) {
			return DNull, pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%s\" out of range for type %s (column %s)", s, typ.SQLString(), colName)
		}
		return NewDInt(DInt(i)), nil
	case types.FloatFamily:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return DNull, makeParseError(s, types.Float, err)
		}
		return NewDFloat(DFloat(f)), nil
	case types.BoolFamily:
		return ParseDBool(s)

	default:
		return DNull, NewDatatypeMismatchError(colName, s, typ.SQLString())
	}
}
