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

package schemachange

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// TestColumnConversions rolls-up a lot of test plumbing to prevent
// top-level namespace pollution.
func TestColumnConversions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnType := func(typStr string) *types.T {
		t, err := parser.ParseType(typStr)
		if err != nil {
			panic(err)
		}
		return t
	}

	// columnConversionInfo is where we document conversions that
	// don't require a fully-generalized conversion path or where there are
	// restrictions on conversions that seem non-obvious at first glance.
	columnConversionInfo := map[string]map[string]ColumnConversionKind{
		"BYTES": {
			"BYTES": ColumnConversionTrivial,

			"STRING":     ColumnConversionValidate,
			"STRING(20)": ColumnConversionValidate,
		},

		"DECIMAL(6)": {
			"DECIMAL(8)": ColumnConversionTrivial,
		},

		"FLOAT4": {
			"FLOAT4": ColumnConversionTrivial,
			"FLOAT8": ColumnConversionTrivial,
		},

		"INET": {
			// This doesn't have an "obvious" conversion to bytes since it's
			// encoded as a netmask length, followed by the actual address bytes.
			"BYTES": ColumnConversionImpossible,
		},

		"INT8": {
			"INT8": ColumnConversionTrivial,
			"INT4": ColumnConversionValidate,
			"BIT":  ColumnConversionGeneral,
		},
		"INT4": {
			"INT2": ColumnConversionValidate,
			"INT8": ColumnConversionTrivial,
		},

		"VARBIT": {
			"INT":    ColumnConversionGeneral,
			"STRING": ColumnConversionGeneral,
			"BYTES":  ColumnConversionImpossible,
			"BIT(4)": ColumnConversionValidate,
		},
		"BIT(4)": {
			"BIT(2)": ColumnConversionValidate,
			"BIT(8)": ColumnConversionTrivial,
		},

		"STRING": {
			"BIT":   ColumnConversionGeneral,
			"BYTES": ColumnConversionTrivial,
		},
		"STRING(5)": {
			"BYTES": ColumnConversionTrivial,
		},

		"TIME": {
			"TIME":    ColumnConversionTrivial,
			"TIME(5)": ColumnConversionGeneral,
			"TIME(6)": ColumnConversionTrivial,
		},
		"TIMETZ": {
			"TIMETZ":    ColumnConversionTrivial,
			"TIMETZ(5)": ColumnConversionGeneral,
			"TIMETZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMP": {
			"TIMESTAMP(5)":   ColumnConversionGeneral,
			"TIMESTAMP(6)":   ColumnConversionTrivial,
			"TIMESTAMPTZ(5)": ColumnConversionGeneral,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMP(0)": {
			"TIMESTAMP(3)":   ColumnConversionTrivial,
			"TIMESTAMP(6)":   ColumnConversionTrivial,
			"TIMESTAMPTZ(3)": ColumnConversionTrivial,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMP(3)": {
			"TIMESTAMP(0)": ColumnConversionGeneral,
			"TIMESTAMP(1)": ColumnConversionGeneral,
			"TIMESTAMP(3)": ColumnConversionTrivial,
			"TIMESTAMP(6)": ColumnConversionTrivial,

			"TIMESTAMPTZ(0)": ColumnConversionGeneral,
			"TIMESTAMPTZ(1)": ColumnConversionGeneral,
			"TIMESTAMPTZ(3)": ColumnConversionTrivial,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMPTZ": {
			"TIMESTAMP(5)":   ColumnConversionGeneral,
			"TIMESTAMP(6)":   ColumnConversionTrivial,
			"TIMESTAMPTZ(5)": ColumnConversionGeneral,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMPTZ(3)": {
			"TIMESTAMP(0)": ColumnConversionGeneral,
			"TIMESTAMP(1)": ColumnConversionGeneral,
			"TIMESTAMP(3)": ColumnConversionTrivial,
			"TIMESTAMP(6)": ColumnConversionTrivial,

			"TIMESTAMPTZ(0)": ColumnConversionGeneral,
			"TIMESTAMPTZ(1)": ColumnConversionGeneral,
			"TIMESTAMPTZ(3)": ColumnConversionTrivial,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},
		"TIMESTAMPTZ(0)": {
			"TIMESTAMP(3)":   ColumnConversionTrivial,
			"TIMESTAMP(6)":   ColumnConversionTrivial,
			"TIMESTAMPTZ(3)": ColumnConversionTrivial,
			"TIMESTAMPTZ(6)": ColumnConversionTrivial,
		},

		"UUID": {
			"BYTES": ColumnConversionGeneral,
		},
	}

	// Verify that columnConversionInfo does what it says.
	t.Run("columnConversionInfo sanity", func(t *testing.T) {
		for from, mid := range columnConversionInfo {
			for to, expected := range mid {
				actual, err := ClassifyConversion(columnType(from), columnType(to))

				// Verify that we only return cannot-coerce errors.
				if err != nil {
					if code := pgerror.GetPGCode(err); code != pgcode.CannotCoerce {
						t.Errorf("unexpected error code returned: %s", code)
					}
				}

				if actual != expected {
					t.Errorf("%v -> %v returned %s, expecting %s", from, to, actual, expected)
				} else {
					t.Log(from, to, actual)
				}
			}
		}
	})

	t.Run("column conversion checks", func(t *testing.T) {
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(db)

		for from, mid := range columnConversionInfo {
			for to, kind := range mid {
				// TODO(bob): Generalize in the next pass.
				if kind != ColumnConversionTrivial {
					continue
				}

				fromTyp := columnType(from)
				toTyp := columnType(to)
				t.Run(fmt.Sprint(fromTyp.Family(), "->", toTyp.Family()), func(t *testing.T) {
					sqlDB.Exec(t, "CREATE DATABASE d")
					defer sqlDB.Exec(t, "DROP DATABASE d")

					sqlDB.Exec(t, fmt.Sprintf(
						"CREATE TABLE d.t (i int8 primary key, a %s)", columnType(from).SQLString()))

					// We're just going to use an ugly, two-dimensional switch
					// structure here to establish some values that we want to
					// test from and various values we expect to be able to
					// convert to.
					var insert []interface{}
					var expect []interface{}

					switch fromTyp.Family() {
					case types.BytesFamily:
						insert = []interface{}{[]uint8{}, []uint8("data")}
						switch toTyp.Family() {
						case types.BytesFamily:
							expect = insert
						}

					case types.BitFamily:
						switch fromTyp.Width() {
						case 4:
							insert = []interface{}{[]uint8("0110")}
						case 0:
							insert = []interface{}{[]uint8("110"), []uint8("000110")}
						}
						switch toTyp.Family() {
						case types.BitFamily:
							expect = insert
						}

					case types.DecimalFamily:
						insert = []interface{}{"-112358", "112358"}
						switch toTyp.Family() {
						case types.DecimalFamily:
							// We're going to see decimals returned as strings
							expect = []interface{}{[]uint8("-112358"), []uint8("112358")}
						}

					case types.FloatFamily:
						insert = []interface{}{-1.2, 0.0, 1.2}
						switch toTyp.Family() {
						case types.FloatFamily:
							expect = insert
						}

					case types.IntFamily:
						insert = []interface{}{int64(-1), int64(0), int64(1)}
						switch fromTyp.Width() {
						case 0, 64:
							insert = append(insert, int64(math.MinInt64), int64(math.MaxInt64))
						case 32:
							insert = append(insert, int64(math.MinInt32), int64(math.MaxInt32))
						case 16:
							insert = append(insert, int64(math.MinInt16), int64(math.MaxInt16))
						}
						switch toTyp.Family() {
						case types.IntFamily:
							expect = insert
						}

					case types.StringFamily:
						insert = []interface{}{"", "text", "✈"}
						switch toTyp.Family() {
						case types.StringFamily:
							expect = []interface{}{"", "text"}
						case types.BytesFamily:
							expect = []interface{}{[]uint8{}, []uint8("text"), []uint8{0xE2, 0x9C, 0x88}}
						}

					case types.TimeFamily,
						types.TimestampFamily,
						types.TimestampTZFamily,
						types.TimeTZFamily:

						const timeOnly = "15:04:05"
						const timeOnlyWithZone = "15:04:05 -0700"
						const noZone = "2006-01-02 15:04:05"
						const withZone = "2006-01-02 15:04:05 -0700"

						var fromFmt string
						switch fromTyp.Family() {
						case types.TimeFamily:
							fromFmt = timeOnly
						case types.TimestampFamily:
							fromFmt = noZone
						case types.TimestampTZFamily:
							fromFmt = withZone
						case types.TimeTZFamily:
							fromFmt = timeOnlyWithZone
						}

						// Always use a non-UTC zone for this test
						const tz = "America/New_York"
						sqlDB.Exec(t, fmt.Sprintf("SET SESSION TIME ZONE '%s'", tz))

						// Use a fixed time so we don't experience weirdness when
						// testing TIME values if the local time would roll over
						// a day boundary when moving between Eastern and UTC.
						// We can re-use the format strings as test data.
						now := fromFmt
						insert = []interface{}{now}

						switch toTyp.Family() {
						case
							types.TimeFamily,
							types.TimestampFamily,
							types.TimestampTZFamily,
							types.TimeTZFamily:
							// We're going to re-parse the text as though we're in UTC
							// so that we can drop the TZ info.
							if parsed, err := time.ParseInLocation(fromFmt, now, time.UTC); err == nil {
								expect = []interface{}{parsed}
							} else {
								t.Fatal(err)
							}
						}

					case types.UuidFamily:
						u := uuid.MakeV4()
						insert = []interface{}{u}
						switch toTyp.Family() {
						case types.BytesFamily:
							expect = []interface{}{u.GetBytes()}
						}

					default:
						t.Fatalf("don't know how to create initial value for %s", fromTyp.Family())
					}
					if expect == nil {
						t.Fatalf("expect variable not initialized for %s -> %s", fromTyp.Family(), toTyp.Family())
					}

					// Insert the test data.
					if tx, err := db.Begin(); err != nil {
						t.Fatal(err)
					} else {
						for i, v := range insert {
							sqlDB.Exec(t, "INSERT INTO d.t (i, a) VALUES ($1, $2)", i, v)
						}

						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
					}

					findColumn := func(colType *types.T) bool {
						var a, expr string
						lookFor := fmt.Sprintf("a %s NULL,", colType.SQLString())
						sqlDB.QueryRow(t, "SHOW CREATE d.t").Scan(&a, &expr)
						t.Log(lookFor, expr)
						return strings.Contains(expr, lookFor)
					}

					// Sanity-check that our findColumn is working.
					if !findColumn(fromTyp) {
						t.Fatal("could not find source column")
					}

					// Execute the schema change.
					sqlDB.Exec(t, fmt.Sprintf(
						"ALTER TABLE d.t ALTER COLUMN a SET DATA TYPE %s", columnType(to).SQLString()))

					// Verify that the column descriptor was updated.
					if !findColumn(toTyp) {
						t.Fatal("could not find target column")
					}

					// Verify data conversions.
					rows := sqlDB.Query(t, "SELECT a FROM d.t")
					defer rows.Close()

					count := 0
					for rows.Next() {
						var actual interface{}
						if err := rows.Scan(&actual); err != nil {
							t.Fatal(err)
						}
						expected := expect[count]

						if actualBytes, ok := actual.([]uint8); ok {
							if len(actualBytes) >= 2 && bytes.Equal(actualBytes[:2], []byte("\\x")) {
								actualBytes, err := hex.DecodeString(string(actualBytes[2:]))
								if err != nil {
									t.Fatal(err)
								}
								actual = actualBytes
							}
						}

						var success bool
						switch e := expected.(type) {
						case time.Time:
							success = e.Equal(actual.(time.Time))
						default:
							success = reflect.DeepEqual(e, actual)
						}

						if !success {
							t.Errorf("unexpected value found at row %d: %v (%s) vs %v (%s)",
								count, expected, reflect.TypeOf(expected), actual, reflect.TypeOf(actual))
						}
						count++
					}
					if count != len(expect) {
						t.Errorf("expecting %d rows, found %d", len(expect), count)
					}
				})
			}
		}
	})
}
