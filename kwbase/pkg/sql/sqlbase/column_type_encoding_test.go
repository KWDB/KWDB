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

package sqlbase

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
	"gitee.com/kwbasedb/kwbase/pkg/util/timetz"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"gitee.com/kwbasedb/kwbase/pkg/util/uint128"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/apd"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
)

func genColumnType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		columnType := RandColumnType(genParams.Rng)
		return gopter.NewGenResult(columnType, gopter.NoShrinker)
	}
}

func genDatum() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(RandDatum(genParams.Rng, RandColumnType(genParams.Rng),
			false), gopter.NoShrinker)
	}
}

func genDatumWithType(columnType interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		datum := RandDatum(genParams.Rng, columnType.(*types.T), false)
		return gopter.NewGenResult(datum, gopter.NoShrinker)
	}
}

func genEncodingDirection() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(
			encoding.Direction((genParams.Rng.Int()%int(encoding.Descending))+1),
			gopter.NoShrinker)
	}
}

func hasKeyEncoding(typ *types.T) bool {
	// Only some types are round-trip key encodable.
	switch typ.Family() {
	case types.JsonFamily, types.ArrayFamily, types.CollatedStringFamily, types.TupleFamily, types.DecimalFamily:
		return false
	}
	return true
}

func TestEncodeTableValue(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	var scratch []byte
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum) string {
			b, err := EncodeTableValue(nil, 0, d, scratch)
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := DecodeTableValue(a, d.ResolvedType(), b)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if newD.Compare(ctx, d) != 0 {
				return "unequal"
			}
			return ""
		},
		genDatum(),
	))
	properties.TestingRun(t)
}

func TestDecodeTableKeyTypes(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	t.Run("Bool", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{tree.DBoolTrue, tree.DBoolFalse} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Bool, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Int", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDInt(0), tree.NewDInt(1), tree.NewDInt(-1),
				tree.NewDInt(math.MaxInt64), tree.NewDInt(math.MinInt64),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Int, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Float", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDFloat(0), tree.NewDFloat(1.5), tree.NewDFloat(-3.14),
				tree.NewDFloat(math.MaxFloat64), tree.NewDFloat(-math.MaxFloat64),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Float, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("String", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDString(""), tree.NewDString("hello"), tree.NewDString("世界"),
				tree.NewDString(strings.Repeat("a", 100)),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.String, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDBytes(tree.DBytes("")),
				tree.NewDBytes(tree.DBytes("abcde")),
				tree.NewDBytes(tree.DBytes("hello\x00world")),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Bytes, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Date", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(0)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(18429)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(-730119)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(2921939)
					return tree.NewDDate(d)
				}(),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Date, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Time", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.MakeDTime(timeofday.FromInt(0)),
				tree.MakeDTime(timeofday.FromInt(8639999999999)),
				tree.MakeDTime(timeofday.FromInt(1234567890)),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Time, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("TimeTZ", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now().Add(-24 * time.Second))),
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now())),
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now().Add(24 * time.Second))),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.TimeTZ, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.MakeDTimestamp(timeutil.Now().Add(-24*time.Second), time.Second),
				tree.MakeDTimestamp(timeutil.Now(), time.Second),
				tree.MakeDTimestamp(timeutil.Now().Add(24*time.Second), time.Second),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Timestamp, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("TimestampTZ", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.MakeDTimestampTZ(timeutil.Now().Add(-24*time.Second), time.Second),
				tree.MakeDTimestampTZ(timeutil.Now(), time.Second),
				tree.MakeDTimestampTZ(timeutil.Now().Add(24*time.Second), time.Second),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.TimestampTZ, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Interval", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDInterval(duration.MakeDuration(2000000000, 0, 0), types.IntervalTypeMetadata{}),
				tree.NewDInterval(duration.MakeDuration(1000000000, 0, 0), types.IntervalTypeMetadata{}),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Interval, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("UUID", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDUuid(tree.DUuid{uuid.FromUint128(uint128.Uint128{0, 0})}),
				tree.NewDUuid(tree.DUuid{uuid.FromUint128(uint128.Uint128{2220, 3333})}),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Uuid, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("BitArray", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			v1, _ := tree.NewDBitArrayFromInt(0b10101010, 8)
			v2, _ := tree.NewDBitArrayFromInt(0b10101010, 10)
			v3, _ := tree.NewDBitArrayFromInt(0b10101010, 12)
			for _, v := range []tree.Datum{
				v1, v2, v3,
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.VarBit, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Name", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			for _, v := range []tree.Datum{
				tree.NewDName(""), tree.NewDName("test_name"), tree.NewDName("123"),
			} {
				b, err := EncodeTableKey(nil, v, dir)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableKey(a, types.Name, b, dir)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Null", func(t *testing.T) {
		for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
			b := encoding.EncodeNullAscending(nil)
			if dir == encoding.Descending {
				b = encoding.EncodeNullDescending(nil)
			}
			result, remaining, err := DecodeTableKey(a, types.Int, b, dir)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(remaining) != 0 {
				t.Fatalf("leftover bytes: %v", remaining)
			}
			if result != tree.DNull {
				t.Fatalf("expected DNull, got %v", result)
			}
		}
	})
}

func TestEncodeTableValueTypes(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	t.Run("Bool", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{tree.DBoolTrue, tree.DBoolFalse} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Bool, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Int", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDInt(0), tree.NewDInt(1), tree.NewDInt(-1),
				tree.NewDInt(math.MaxInt64), tree.NewDInt(math.MinInt64),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Int, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Float", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDFloat(0), tree.NewDFloat(1.5), tree.NewDFloat(-3.14),
				tree.NewDFloat(math.MaxFloat64), tree.NewDFloat(-math.MaxFloat64),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Float, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Decimal", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				&tree.DDecimal{Decimal: *apd.New(0, 0)},
				&tree.DDecimal{Decimal: *apd.New(123, 2)},
				&tree.DDecimal{Decimal: *apd.New(-456, 3)},
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Decimal, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("String", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDString(""), tree.NewDString("hello"), tree.NewDString("世界"),
				tree.NewDString(strings.Repeat("a", 100)),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.String, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDBytes(tree.DBytes("")),
				tree.NewDBytes(tree.DBytes("abcde")),
				tree.NewDBytes(tree.DBytes("hello\x00world")),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Bytes, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Date", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(0)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(18429)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(-730119)
					return tree.NewDDate(d)
				}(),
				func() *tree.DDate {
					d, _ := pgdate.MakeDateFromPGEpoch(2921939)
					return tree.NewDDate(d)
				}(),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Date, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Time", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.MakeDTime(timeofday.FromInt(0)),
				tree.MakeDTime(timeofday.FromInt(8639999999999)),
				tree.MakeDTime(timeofday.FromInt(1234567890)),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Time, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("TimeTZ", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now().Add(-24 * time.Second))),
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now())),
				tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(timeutil.Now().Add(24 * time.Second))),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.TimeTZ, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.MakeDTimestamp(timeutil.Now().Add(-24*time.Second), time.Second),
				tree.MakeDTimestamp(timeutil.Now(), time.Second),
				tree.MakeDTimestamp(timeutil.Now().Add(24*time.Second), time.Second),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Timestamp, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("TimestampTZ", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.MakeDTimestampTZ(timeutil.Now().Add(-24*time.Second), time.Second),
				tree.MakeDTimestampTZ(timeutil.Now(), time.Second),
				tree.MakeDTimestampTZ(timeutil.Now().Add(24*time.Second), time.Second),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.TimestampTZ, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Interval", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDInterval(duration.MakeDuration(2000000000, 0, 0), types.IntervalTypeMetadata{}),
				tree.NewDInterval(duration.MakeDuration(1000000000, 0, 0), types.IntervalTypeMetadata{}),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Interval, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("UUID", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			for _, v := range []tree.Datum{
				tree.NewDUuid(tree.DUuid{uuid.FromUint128(uint128.Uint128{0, 0})}),
				tree.NewDUuid(tree.DUuid{uuid.FromUint128(uint128.Uint128{2220, 3333})}),
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.Uuid, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("BitArray", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			v1, _ := tree.NewDBitArrayFromInt(0b10101010, 8)
			v2, _ := tree.NewDBitArrayFromInt(0b10101010, 10)
			v3, _ := tree.NewDBitArrayFromInt(0b10101010, 12)
			for _, v := range []tree.Datum{
				v1, v2, v3,
			} {
				b, err := EncodeTableValue(nil, colID, v, nil)
				if err != nil {
					t.Fatalf("encode error: %v", err)
				}
				result, remaining, err := DecodeTableValue(a, types.VarBit, b)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if len(remaining) != 0 {
					t.Fatalf("leftover bytes: %v", remaining)
				}
				if result.Compare(ctx, v) != 0 {
					t.Fatalf("value mismatch: got %v, want %v", result, v)
				}
			}
		}
	})

	t.Run("Null", func(t *testing.T) {
		for _, colID := range []ColumnID{0, 1} {
			b, err := EncodeTableValue(nil, colID, tree.DNull, nil)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}
			result, remaining, err := DecodeTableValue(a, types.Int, b)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if len(remaining) != 0 {
				t.Fatalf("leftover bytes: %v", remaining)
			}
			if result != tree.DNull {
				t.Fatalf("expected DNull, got %v", result)
			}
		}
	})
}

func TestEncodeTableKey(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum, dir encoding.Direction) string {
			b, err := EncodeTableKey(nil, d, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := DecodeTableKey(a, d.ResolvedType(), b, dir)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if newD.Compare(ctx, d) != 0 {
				return "unequal"
			}
			return ""
		},
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(genDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))
	properties.Property("order-preserving", prop.ForAll(
		func(datums []tree.Datum, dir encoding.Direction) string {
			d1 := datums[0]
			d2 := datums[1]
			b1, err := EncodeTableKey(nil, d1, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			b2, err := EncodeTableKey(nil, d2, dir)
			if err != nil {
				return "error: " + err.Error()
			}

			expectedCmp := d1.Compare(ctx, d2)
			cmp := bytes.Compare(b1, b2)

			if expectedCmp == 0 {
				if cmp != 0 {
					return fmt.Sprintf("equal inputs produced inequal outputs: \n%v\n%v", b1, b2)
				}
				// If the inputs are equal and so are the outputs, no more checking to do.
				return ""
			}

			cmpsMatch := expectedCmp == cmp
			dirIsAscending := dir == encoding.Ascending

			if cmpsMatch != dirIsAscending {
				return fmt.Sprintf("non-order preserving encoding: \n%v\n%v", b1, b2)
			}
			return ""
		},
		// For each column type, generate two datums of that type.
		genColumnType().
			SuchThat(hasKeyEncoding).
			FlatMap(
				func(t interface{}) gopter.Gen {
					colTyp := t.(*types.T)
					return gopter.CombineGens(
						genDatumWithType(colTyp),
						genDatumWithType(colTyp))
				}, reflect.TypeOf([]interface{}{})).
			Map(func(datums []interface{}) []tree.Datum {
				ret := make([]tree.Datum, len(datums))
				for i, d := range datums {
					ret[i] = d.(tree.Datum)
				}
				return ret
			}),
		genEncodingDirection(),
	))
	properties.TestingRun(t)
}

func TestSkipTableKey(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	properties.Property("correctness", prop.ForAll(
		func(d tree.Datum, dir encoding.Direction) string {
			b, err := EncodeTableKey(nil, d, dir)
			if err != nil {
				return "error: " + err.Error()
			}
			res, err := SkipTableKey(b)
			if err != nil {
				return "error: " + err.Error()
			}
			if len(res) != 0 {
				fmt.Println(res, len(res), d.ResolvedType(), d.ResolvedType().Family())
				return "expected 0 bytes remaining"
			}
			return ""
		},
		genColumnType().
			SuchThat(hasKeyEncoding).FlatMap(genDatumWithType, reflect.TypeOf((*tree.Datum)(nil)).Elem()),
		genEncodingDirection(),
	))
	properties.TestingRun(t)
}

func TestMarshalColumnValueRoundtrip(t *testing.T) {
	a := &DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)

	properties.Property("roundtrip",
		prop.ForAll(
			func(typ *types.T) string {
				d, ok := genDatumWithType(typ).Sample()
				if !ok {
					return "error generating datum"
				}
				datum := d.(tree.Datum)
				desc := ColumnDescriptor{
					Type: *typ,
				}
				value, err := MarshalColumnValue(&desc, datum)
				if err != nil {
					return "error marshaling: " + err.Error()
				}
				outDatum, err := UnmarshalColumnValue(a, typ, value)
				if err != nil {
					return "error unmarshaling: " + err.Error()
				}
				if datum.Compare(ctx, outDatum) != 0 {
					return fmt.Sprintf("datum didn't roundtrip.\ninput: %v\noutput: %v", datum, outDatum)
				}
				return ""
			},
			genColumnType(),
		),
	)
	properties.TestingRun(t)
}
