// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"math"
	"strconv"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// TestParseDatumStringAs tests that datums are roundtrippable between
// printing with FmtExport and ParseDatumStringAs, but with random datums.
// This test lives in sqlbase to avoid dependency cycles when trying to move
// RandDatumWithNullChance into tree.
func TestRandParseDatumStringAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := append([]*types.T{
		types.MakeTimestamp(0),
		types.MakeTimestamp(3),
		types.MakeTimestamp(6),
		types.MakeTimestampTZ(0),
		types.MakeTimestampTZ(3),
		types.MakeTimestampTZ(6),
		types.MakeTime(0),
		types.MakeTime(3),
		types.MakeTime(6),
		types.MakeTimeTZ(0),
		types.MakeTimeTZ(3),
		types.MakeTimeTZ(6),
		types.MakeCollatedString(types.String, "en"),
		types.MakeCollatedString(types.String, "de"),
		types.MakeCollatedString(types.CIText, ""),
	},
		types.Scalar...)
	for _, ty := range types.Scalar {
		if ty != types.Jsonb {
			tests = append(tests, types.MakeArray(ty))
		}
	}
	evalCtx := tree.NewTestingEvalContext(nil)
	rng, _ := randutil.NewPseudoRand()
	for _, typ := range tests {
		const testsForTyp = 100
		t.Run(typ.String(), func(t *testing.T) {
			for i := 0; i < testsForTyp; i++ {
				datum := RandDatumWithNullChance(rng, typ, 0)

				// Because of how RandDatumWithNullChanceWorks, we might
				// get an interesting datum for a time related type that
				// doesn't have the precision that we requested. In these
				// cases, manually correct the type ourselves.
				switch d := datum.(type) {
				case *tree.DTimestampTZ:
					datum = d.Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
				case *tree.DTimestamp:
					datum = d.Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
				case *tree.DTime:
					datum = d.Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
				case *tree.DTimeTZ:
					datum = d.Round(tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()))
				}

				ds := tree.AsStringWithFlags(datum, tree.FmtExport)
				parsed, err := ParseDatumStringAs(typ, ds, evalCtx)
				if err != nil {
					t.Fatal(ds, err)
				}
				if parsed.Compare(evalCtx, datum) != 0 {
					t.Fatal(ds, "expected", datum, "found", parsed)
				}
			}
		})
	}
}

// TestParseDatumStringAs tests that datums are roundtrippable between
// printing with FmtExport and ParseDatumStringAs.
func TestParseDatumStringAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := map[*types.T][]string{
		types.Bool: {
			"true",
			"false",
		},
		types.Bytes: {
			`\x`,
			`\x00`,
			`\xff`,
			`\xffff`,
			fmt.Sprintf(`\x%x`, "abc"),
		},
		types.Date: {
			"2001-01-01",
		},
		types.Decimal: {
			"0.0",
			"1.0",
			"-1.0",
			strconv.FormatFloat(math.MaxFloat64, 'G', -1, 64),
			strconv.FormatFloat(math.SmallestNonzeroFloat64, 'G', -1, 64),
			strconv.FormatFloat(-math.MaxFloat64, 'G', -1, 64),
			strconv.FormatFloat(-math.SmallestNonzeroFloat64, 'G', -1, 64),
			"1E+1000",
			"1E-1000",
			"Infinity",
			"-Infinity",
			"NaN",
		},
		types.IntArray: {
			"ARRAY[]",
			"ARRAY[1, 2]",
		},
		types.StringArray: {
			`ARRAY[NULL, 'NULL']`,
			`ARRAY['hello', 'there']`,
			`ARRAY['hel,lo']`,
		},
		types.Float: {
			"0.0",
			"-0.0",
			"1.0",
			"-1.0",
			strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64),
			strconv.FormatFloat(math.SmallestNonzeroFloat64, 'g', -1, 64),
			strconv.FormatFloat(-math.MaxFloat64, 'g', -1, 64),
			strconv.FormatFloat(-math.SmallestNonzeroFloat64, 'g', -1, 64),
			"+Inf",
			"-Inf",
			"NaN",
		},
		types.INet: {
			"127.0.0.1",
		},
		types.Int: {
			"1",
			"0",
			"-1",
			strconv.Itoa(math.MaxInt64),
			strconv.Itoa(math.MinInt64),
		},
		types.Interval: {
			"01:00:00",
			"-00:01:00",
			"2 years 3 mons",
		},
		types.MakeInterval(types.IntervalTypeMetadata{}): {
			"01:02:03",
			"02:03:04",
			"-00:01:00",
			"2 years 3 mons",
		},
		types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}): {
			"01:02:03",
			"02:03:04.123",
		},
		types.MakeInterval(types.IntervalTypeMetadata{Precision: 6, PrecisionIsSet: true}): {
			"01:02:03",
			"02:03:04.123456",
		},
		types.Jsonb: {
			"{}",
			"[]",
			"null",
			"1",
			"1.0",
			`""`,
			`"abc"`,
			`"ab\u0000c"`,
			`"ab\u0001c"`,
			`"ab⚣ cd"`,
		},
		types.String: {
			"",
			"abc",
			"abc\x00",
			"ab⚣ cd",
		},
		types.Timestamp: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.MakeTimestamp(0): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04+00:00",
		},
		types.MakeTimestamp(3): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123+00:00",
		},
		types.MakeTimestamp(6): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.TimestampTZ: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.MakeTimestampTZ(0): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04+00:00",
		},
		types.MakeTimestampTZ(3): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123+00:00",
		},
		types.MakeTimestampTZ(6): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.Time: {
			"01:02:03",
			"02:03:04.123456",
		},
		types.MakeTime(0): {
			"01:02:03",
			"02:03:04",
		},
		types.MakeTime(3): {
			"01:02:03",
			"02:03:04.123",
		},
		types.MakeTime(6): {
			"01:02:03",
			"02:03:04.123456",
		},
		types.TimeTZ: {
			"01:02:03+00:00:00",
			"01:02:03+11:00:00",
			"01:02:03+11:00:00",
			"01:02:03-11:00:00",
			"02:03:04.123456+11:00:00",
		},
		types.MakeTimeTZ(0): {
			"01:02:03+03:30:00",
		},
		types.MakeTimeTZ(3): {
			"01:02:03+03:30:00",
			"02:03:04.123+03:30:00",
		},
		types.MakeTimeTZ(6): {
			"01:02:03+03:30:00",
			"02:03:04.123456+03:30:00",
		},
		types.Uuid: {
			uuid.MakeV4().String(),
		},
	}
	evalCtx := tree.NewTestingEvalContext(nil)
	for typ, exprs := range tests {
		t.Run(typ.String(), func(t *testing.T) {
			for _, s := range exprs {
				t.Run(fmt.Sprintf("%q", s), func(t *testing.T) {
					d, err := ParseDatumStringAs(typ, s, evalCtx)
					if err != nil {
						t.Fatal(err)
					}
					if d.ResolvedType().Family() != typ.Family() {
						t.Fatalf("unexpected type: %s", d.ResolvedType())
					}
					ds := tree.AsStringWithFlags(d, tree.FmtExport)
					parsed, err := ParseDatumStringAs(typ, ds, evalCtx)
					if err != nil {
						t.Fatal(err)
					}
					if parsed.Compare(evalCtx, d) != 0 {
						t.Fatal("expected", d, "found", parsed)
					}
				})
			}
		})
	}
}
