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

package colencoding

import (
	"strconv"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the idx'th position of the input exec.Vec.
// See the analog in sqlbase/column_type_encoding.go.
func DecodeTableValueToCol(
	vec coldata.Vec, idx int, typ encoding.Type, dataOffset int, valTyp *types.T, b []byte,
) ([]byte, error) {
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vec.Nulls().SetNull(idx)
		return b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valTyp.Family() != types.BoolFamily {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatumToCol(vec, idx, typ, valTyp, b)
}

// decodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place). It writes the result
// to the idx'th position of the input exec.Vec.
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
// See the analog in sqlbase/column_type_encoding.go.
func decodeUntaggedDatumToCol(
	vec coldata.Vec, idx int, typ encoding.Type, t *types.T, buf []byte,
) ([]byte, error) {
	var err error
	switch t.Family() {
	case types.BoolFamily:
		var b bool
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		buf, b, err = encoding.DecodeBoolValue(buf)
		vec.Bool()[idx] = b
	case types.BytesFamily, types.StringFamily:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vec.Bytes().Set(idx, data)
	case types.DateFamily, types.OidFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		vec.Int64()[idx] = i
	case types.DecimalFamily:
		if typ == encoding.Int {
			b, i, err := encoding.DecodeUntaggedIntValue(buf)
			if err != nil {
				return b, err
			}
			vec.Decimal()[idx].SetInt64(i)
			return b, nil
		} else if typ == encoding.Float {
			b, data, err := encoding.DecodeUntaggedFloatValue(buf)
			if err != nil {
				return b, err
			}
			_, _, err = (vec.Decimal()[idx]).SetString(strconv.FormatFloat(data, 'f', -1, 64))
			if err != nil {
				return b, err
			}
			return b, nil
		}
		buf, err = encoding.DecodeIntoUntaggedDecimalValue(&vec.Decimal()[idx], buf)
	case types.FloatFamily:
		var f float64
		buf, f, err = encoding.DecodeUntaggedFloatValue(buf)
		vec.Float64()[idx] = f
	case types.IntFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		switch t.Width() {
		case 8:
			vec.Int16()[idx] = int16(i)
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vec.Int64()[idx] = i
		}
	case types.UuidFamily:
		var data uuid.UUID
		buf, data, err = encoding.DecodeUntaggedUUIDValue(buf)
		// TODO(yuzefovich): we could peek inside the encoding package to skip a
		// couple of conversions.
		if err == nil {
			vec.Bytes().Set(idx, data.GetBytes())
		}
	case types.TimestampFamily, types.TimestampTZFamily:
		var t time.Time
		buf, t, err = encoding.DecodeUntaggedTimeValue(buf)
		vec.Timestamp()[idx] = t
	case types.IntervalFamily:
		var d duration.Duration
		buf, d, err = encoding.DecodeUntaggedDurationValue(buf)
		vec.Interval()[idx] = d
	default:
		return buf, errors.AssertionFailedf(
			"couldn't decode type: %s", log.Safe(t))
	}
	return buf, err
}
