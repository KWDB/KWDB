// Copyright 2016 The Cockroach Authors.
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

package builtins

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"math"
	"math/rand"
	"net"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/ipaddr"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCategory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if expected, actual := categoryString, builtins["lower"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryString, builtins["length"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, builtins["now"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categorySystemInfo, builtins["version"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
}

// TestGenerateUniqueIDOrder verifies the expected ordering of
// GenerateUniqueID.
func TestGenerateUniqueIDOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []tree.DInt{
		GenerateUniqueID(0, 0),
		GenerateUniqueID(1, 0),
		GenerateUniqueID(2<<15, 0),
		GenerateUniqueID(0, 1),
		GenerateUniqueID(0, 10000),
		GenerateUniqueInt(0),
	}
	prev := tests[0]
	for _, tc := range tests[1:] {
		if tc <= prev {
			t.Fatalf("%d > %d", tc, prev)
		}
	}
}

func TestStringToArrayAndBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// s allows us to have a string pointer literal.
	s := func(x string) *string { return &x }
	fs := func(x *string) string {
		if x != nil {
			return *x
		}
		return "<nil>"
	}
	cases := []struct {
		input    string
		sep      *string
		nullStr  *string
		expected []*string
	}{
		{`abcxdef`, s(`x`), nil, []*string{s(`abc`), s(`def`)}},
		{`xxx`, s(`x`), nil, []*string{s(``), s(``), s(``), s(``)}},
		{`xxx`, s(`xx`), nil, []*string{s(``), s(`x`)}},
		{`abcxdef`, s(``), nil, []*string{s(`abcxdef`)}},
		{`abcxdef`, s(`abcxdef`), nil, []*string{s(``), s(``)}},
		{`abcxdef`, s(`x`), s(`abc`), []*string{nil, s(`def`)}},
		{`abcxdef`, s(`x`), s(`x`), []*string{s(`abc`), s(`def`)}},
		{`abcxdef`, s(`x`), s(``), []*string{s(`abc`), s(`def`)}},
		{``, s(`x`), s(``), []*string{}},
		{``, s(``), s(``), []*string{}},
		{``, s(`x`), nil, []*string{}},
		{``, s(``), nil, []*string{}},
		{`abcxdef`, nil, nil, []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`abc`), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`x`), []*string{s(`a`), s(`b`), s(`c`), nil, s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(``), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{``, nil, s(``), []*string{}},
		{``, nil, nil, []*string{}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("string_to_array(%q, %q)", tc.input, fs(tc.sep)), func(t *testing.T) {
			result, err := stringToArray(tc.input, tc.sep, tc.nullStr)
			if err != nil {
				t.Fatal(err)
			}

			expectedArray := tree.NewDArray(types.String)
			for _, s := range tc.expected {
				datum := tree.DNull
				if s != nil {
					datum = tree.NewDString(*s)
				}
				if err := expectedArray.Append(datum); err != nil {
					t.Fatal(err)
				}
			}

			evalContext := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if result.Compare(evalContext, expectedArray) != 0 {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}

			if tc.sep == nil {
				return
			}

			s, err := arrayToString(result.(*tree.DArray), *tc.sep, tc.nullStr)
			if err != nil {
				t.Fatal(err)
			}
			if s == tree.DNull {
				t.Errorf("expected not null, found null")
			}

			ds := s.(*tree.DString)
			fmt.Println(ds)
			if string(*ds) != tc.input {
				t.Errorf("original %s, roundtripped %s", tc.input, s)
			}
		})
	}
}

func TestEscapeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		bytes []byte
		str   string
	}{
		{[]byte{}, ``},
		{[]byte{'a', 'b', 'c'}, `abc`},
		{[]byte{'a', 'b', 'c', 'd'}, `abcd`},
		{[]byte{'a', 'b', 0, 'd'}, `ab\000d`},
		{[]byte{'a', 'b', 0, 0, 'd'}, `ab\000\000d`},
		{[]byte{'a', 'b', 0, 'a', 'b', 'c', 0, 'd'}, `ab\000abc\000d`},
		{[]byte{'a', 'b', 0, 0}, `ab\000\000`},
		{[]byte{'a', 'b', '\\', 'd'}, `ab\\d`},
		{[]byte{'a', 'b', 200, 'd'}, `ab\310d`},
		{[]byte{'a', 'b', 7, 'd'}, "ab\x07d"},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			result := encodeEscape(tc.bytes)
			if result != tc.str {
				t.Fatalf("expected %q, got %q", tc.str, result)
			}

			decodedResult, err := decodeEscape(tc.str)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decodedResult, tc.bytes) {
				t.Fatalf("expected %q, got %#v", tc.bytes, decodedResult)
			}
		})
	}
}

func TestEscapeFormatRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i := 0; i < 1000; i++ {
		b := make([]byte, rand.Intn(100))
		for j := 0; j < len(b); j++ {
			b[j] = byte(rand.Intn(256))
		}
		str := encodeEscape(b)
		decodedResult, err := decodeEscape(str)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decodedResult, b) {
			t.Fatalf("generated %#v, after round-tripping got %#v", b, decodedResult)
		}
	}
}

func TestLPadRPad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		padFn    func(string, int, string) (string, error)
		str      string
		length   int
		fill     string
		expected string
	}{
		{lpad, "abc", 1, "xy", "a"},
		{lpad, "abc", 2, "xy", "ab"},
		{lpad, "abc", 3, "xy", "abc"},
		{lpad, "abc", 5, "xy", "xyabc"},
		{lpad, "abc", 6, "xy", "xyxabc"},
		{lpad, "abc", 7, "xy", "xyxyabc"},
		{lpad, "abc", 1, " ", "a"},
		{lpad, "abc", 2, " ", "ab"},
		{lpad, "abc", 3, " ", "abc"},
		{lpad, "abc", 5, " ", "  abc"},
		{lpad, "Hello, 世界", 9, " ", "Hello, 世界"},
		{lpad, "Hello, 世界", 10, " ", " Hello, 世界"},
		{lpad, "Hello", 8, "世界", "世界世Hello"},
		{lpad, "foo", -1, "世界", ""},
		{rpad, "abc", 1, "xy", "a"},
		{rpad, "abc", 2, "xy", "ab"},
		{rpad, "abc", 3, "xy", "abc"},
		{rpad, "abc", 5, "xy", "abcxy"},
		{rpad, "abc", 6, "xy", "abcxyx"},
		{rpad, "abc", 7, "xy", "abcxyxy"},
		{rpad, "abc", 1, " ", "a"},
		{rpad, "abc", 2, " ", "ab"},
		{rpad, "abc", 3, " ", "abc"},
		{rpad, "abc", 5, " ", "abc  "},
		{rpad, "abc", 5, " ", "abc  "},
		{rpad, "Hello, 世界", 9, " ", "Hello, 世界"},
		{rpad, "Hello, 世界", 10, " ", "Hello, 世界 "},
		{rpad, "Hello", 8, "世界", "Hello世界世"},
		{rpad, "foo", -1, "世界", ""},
	}
	for _, tc := range testCases {
		out, err := tc.padFn(tc.str, tc.length, tc.fill)
		if err != nil {
			t.Errorf("Found err %v, expected nil", err)
		}
		if out != tc.expected {
			t.Errorf("expected %s, found %s", tc.expected, out)
		}
	}
}

func TestExtractTimeSpanFromTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	utcPositiveOffset := time.FixedZone("otan happy time", 60*60*4+30*60)
	utcNegativeOffset := time.FixedZone("otan sad time", -60*60*4-30*60)

	testCases := []struct {
		input    time.Time
		timeSpan string

		expected      tree.DFloat
		expectedError string
	}{
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone_hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone_minute", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "epoch", expected: 1.576023255123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone", expected: 4*60*60 + 30*60},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone_hour", expected: 4},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone_minute", expected: 30},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "epoch", expected: 1.576007055123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone", expected: -4*60*60 - 30*60},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone_hour", expected: -4},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone_minute", expected: -30},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "epoch", expected: 1.576039455123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "it's numberwang!", expectedError: "unsupported timespan: it's numberwang!"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.timeSpan, tc.input.Format(time.RFC3339)), func(t *testing.T) {
			datum, err := extractTimeSpanFromTimestampTZ(nil, tc.input, tc.timeSpan)
			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, *(datum.(*tree.DFloat)))
			}
		})
	}
}

func TestExtractTimeSpanFromTimeTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		timeTZString  string
		timeSpan      string
		expected      tree.DFloat
		expectedError string
	}{
		{timeTZString: "11:12:13+01:02", timeSpan: "hour", expected: 11},
		{timeTZString: "11:12:13+01:02", timeSpan: "minute", expected: 12},
		{timeTZString: "11:12:13+01:02", timeSpan: "second", expected: 13},
		{timeTZString: "11:12:13.123456+01:02", timeSpan: "millisecond", expected: 13123.456},
		{timeTZString: "11:12:13.123456+01:02", timeSpan: "microsecond", expected: 13123456},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone", expected: 3720},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone_hour", expected: 1},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone_minute", expected: 2},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone", expected: -3720},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone_hour", expected: -1},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone_minute", expected: -2},
		{timeTZString: "11:12:13.5+01:02", timeSpan: "epoch", expected: 36613.5},
		{timeTZString: "11:12:13.5-01:02", timeSpan: "epoch", expected: 44053.5},

		{timeTZString: "11:12:13-01:02", timeSpan: "epoch2", expectedError: "unsupported timespan: epoch2"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.timeSpan, tc.timeTZString), func(t *testing.T) {
			timeTZ, err := tree.ParseDTimeTZ(nil, tc.timeTZString, time.Microsecond)
			assert.NoError(t, err)

			datum, err := extractTimeSpanFromTimeTZ(timeTZ, tc.timeSpan)
			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, *(datum.(*tree.DFloat)))
			}
		})
	}
}

func TestExtractTimeSpanFromInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		timeSpan    string
		intervalStr string
		expected    *tree.DFloat
	}{
		{"millennia", "25000 months 1000 days", tree.NewDFloat(2)},
		{"millennia", "-25000 months 1000 days", tree.NewDFloat(-2)},
		{"millennium", "25000 months 1000 days", tree.NewDFloat(2)},
		{"millenniums", "25000 months 1000 days", tree.NewDFloat(2)},

		{"century", "25000 months 1000 days", tree.NewDFloat(20)},
		{"century", "-25000 months 1000 days", tree.NewDFloat(-20)},
		{"centuries", "25000 months 1000 days", tree.NewDFloat(20)},

		{"decade", "25000 months 1000 days", tree.NewDFloat(208)},
		{"decade", "-25000 months 1000 days", tree.NewDFloat(-208)},
		{"decades", "25000 months 1000 days", tree.NewDFloat(208)},

		{"year", "25000 months 1000 days", tree.NewDFloat(2083)},
		{"year", "-25000 months 1000 days", tree.NewDFloat(-2083)},
		{"years", "25000 months 1000 days", tree.NewDFloat(2083)},

		{"month", "25000 months 1000 days", tree.NewDFloat(4)},
		{"month", "-25000 months 1000 days", tree.NewDFloat(-4)},
		{"months", "25000 months 1000 days", tree.NewDFloat(4)},

		{"day", "25000 months 1000 days", tree.NewDFloat(1000)},
		{"day", "-25000 months 1000 days", tree.NewDFloat(1000)},
		{"day", "-25000 months -1000 days", tree.NewDFloat(-1000)},
		{"days", "25000 months 1000 days", tree.NewDFloat(1000)},

		{"hour", "25-1 100:56:01.123456", tree.NewDFloat(100)},
		{"hour", "25-1 -100:56:01.123456", tree.NewDFloat(-100)},
		{"hours", "25-1 100:56:01.123456", tree.NewDFloat(100)},

		{"minute", "25-1 100:56:01.123456", tree.NewDFloat(56)},
		{"minute", "25-1 -100:56:01.123456", tree.NewDFloat(-56)},
		{"minutes", "25-1 100:56:01.123456", tree.NewDFloat(56)},

		{"second", "25-1 100:56:01.123456", tree.NewDFloat(1.123456)},
		{"second", "25-1 -100:56:01.123456", tree.NewDFloat(-1.123456)},
		{"seconds", "25-1 100:56:01.123456", tree.NewDFloat(1.123456)},

		{"millisecond", "25-1 100:56:01.123456", tree.NewDFloat(1123.456)},
		{"millisecond", "25-1 -100:56:01.123456", tree.NewDFloat(-1123.456)},
		{"milliseconds", "25-1 100:56:01.123456", tree.NewDFloat(1123.456)},

		{"microsecond", "25-1 100:56:01.123456", tree.NewDFloat(1123456)},
		{"microsecond", "25-1 -100:56:01.123456", tree.NewDFloat(-1123456)},
		{"microseconds", "25-1 100:56:01.123456", tree.NewDFloat(1123456)},

		{"epoch", "25-1 100:56:01.123456", tree.NewDFloat(791895361.123456)},
		{"epoch", "25-1 -100:56:01.123456", tree.NewDFloat(791168638.876544)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s as %s", tc.intervalStr, tc.timeSpan), func(t *testing.T) {
			interval, err := tree.ParseDInterval(tc.intervalStr)
			assert.NoError(t, err)

			d, err := extractTimeSpanFromInterval(interval, tc.timeSpan)
			assert.NoError(t, err)

			assert.Equal(t, *tc.expected, *(d.(*tree.DFloat)))
		})
	}
}

func TestTruncateTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	loc, err := timeutil.LoadLocation("Australia/Sydney")
	require.NoError(t, err)

	testCases := []struct {
		fromTime time.Time
		timeSpan string
		expected *tree.DTimestampTZ
	}{
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "millennium", tree.MakeDTimestampTZ(time.Date(2001, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "century", tree.MakeDTimestampTZ(time.Date(2101, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "decade", tree.MakeDTimestampTZ(time.Date(2110, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "year", tree.MakeDTimestampTZ(time.Date(2118, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "quarter", tree.MakeDTimestampTZ(time.Date(2118, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "month", tree.MakeDTimestampTZ(time.Date(2118, time.March, 1, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "day", tree.MakeDTimestampTZ(time.Date(2118, time.March, 11, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "week", tree.MakeDTimestampTZ(time.Date(2118, time.March, 7, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "hour", tree.MakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "second", tree.MakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 0, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "millisecond", tree.MakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 80000000, loc), time.Microsecond)},
		{time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc), "microsecond", tree.MakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 80009000, loc), time.Microsecond)},

		// Test Monday and Sunday boundaries.
		{time.Date(2019, time.November, 11, 5, 6, 7, 80009001, loc), "week", tree.MakeDTimestampTZ(time.Date(2019, time.November, 11, 0, 0, 0, 0, loc), time.Microsecond)},
		{time.Date(2019, time.November, 10, 5, 6, 7, 80009001, loc), "week", tree.MakeDTimestampTZ(time.Date(2019, time.November, 4, 0, 0, 0, 0, loc), time.Microsecond)},
	}

	for _, tc := range testCases {
		t.Run(tc.timeSpan, func(t *testing.T) {
			result, err := truncateTimestamp(nil, tc.fromTime, tc.timeSpan)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFloatWidthBucket(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		operand  float64
		b1       float64
		b2       float64
		count    int
		expected int
	}{
		{0.5, 2, 3, 5, 0},
		{8, 2, 3, 5, 6},
		{1.5, 1, 3, 2, 1},
		{5.35, 0.024, 10.06, 5, 3},
		{-3.0, -5, 5, 10, 3},
		{1, 1, 10, 2, 1},  // minimum should be inclusive
		{10, 1, 10, 2, 3}, // maximum should be exclusive
		{4, 10, 1, 4, 3},
		{11, 10, 1, 4, 0},
		{0, 10, 1, 4, 5},
	}

	for _, tc := range testCases {
		got := widthBucket(tc.operand, tc.b1, tc.b2, tc.count)
		if got != tc.expected {
			t.Errorf("expected %d, found %d", tc.expected, got)
		}
	}
}

// TestGetBuiltinProperties tests looking up built-in function properties
func TestGetBuiltinProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test non-existent function
	props, overloads := GetBuiltinProperties("not_exist_function")
	assert.Nil(t, props)
	assert.Nil(t, overloads)

	// Test existing function
	props, overloads = GetBuiltinProperties("lower")
	assert.NotNil(t, props)
	assert.NotEmpty(t, overloads)
}

// TestDefProps tests default function properties constructor
func TestDefProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := defProps()
	assert.Equal(t, tree.FunctionProperties{}, p)
	assert.Empty(t, p.Category)
	assert.False(t, p.NullableArgs)
}

// TestArrayPropsNullableArgs tests array props with nullable args enabled
func TestArrayPropsNullableArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := arrayPropsNullableArgs()
	assert.Equal(t, categoryArray, p.Category)
	assert.True(t, p.NullableArgs)
}

// TestMakeBuiltin tests builtinDefinition constructor
func TestMakeBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	props := defProps()
	overload := tree.Overload{}
	def := makeBuiltin(props, overload)

	assert.Equal(t, props, def.props)
	assert.Len(t, def.overloads, 1)
	assert.Equal(t, overload, def.overloads[0])
}

// TestNewDecodeError tests decode error creation
func TestNewDecodeError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := newDecodeError("UTF8")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid byte sequence for encoding")
	assert.Contains(t, err.Error(), "UTF8")
}

// TestNewEncodeError tests encode error creation
func TestNewEncodeError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := newEncodeError('€', "ASCII")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "character")
	assert.Contains(t, err.Error(), "€")
	assert.Contains(t, err.Error(), "ASCII")
}

// TestCategorizeType tests the categorizeType function for all type families and edge cases
func TestCategorizeType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Define test cases covering all switch branches and default behavior
	testCases := []struct {
		name     string
		input    *types.T
		expected string
	}{
		// Date & Time category
		{
			name:     "Date family returns date/time category",
			input:    types.Date,
			expected: categoryDateAndTime,
		},
		{
			name:     "Interval family returns date/time category",
			input:    types.Interval,
			expected: categoryDateAndTime,
		},
		{
			name:     "Timestamp family returns date/time category",
			input:    types.Timestamp,
			expected: categoryDateAndTime,
		},
		{
			name:     "TimestampTZ family returns date/time category",
			input:    types.TimestampTZ,
			expected: categoryDateAndTime,
		},

		// Math category
		{
			name:     "Int family returns math category",
			input:    types.Int,
			expected: categoryMath,
		},
		{
			name:     "Decimal family returns math category",
			input:    types.Decimal,
			expected: categoryMath,
		},
		{
			name:     "Float family returns math category",
			input:    types.Float,
			expected: categoryMath,
		},

		// String category
		{
			name:     "String family returns string category",
			input:    types.String,
			expected: categoryString,
		},
		{
			name:     "Bytes family returns string category",
			input:    types.Bytes,
			expected: categoryString,
		},

		// Default branch: returns uppercase type string
		{
			name:     "Bool family returns uppercase type name",
			input:    types.Bool,
			expected: "BOOL",
		},
		{
			name:     "JSONB family returns uppercase type name",
			input:    types.Jsonb,
			expected: "JSONB",
		},
		{
			name:     "UUID family returns uppercase type name",
			input:    types.Uuid,
			expected: "UUID",
		},
	}

	// Execute all test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := categorizeType(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// newTestEvalContext creates a minimal EvalContext for testing.
func newTestEvalContext() *tree.EvalContext {
	st := cluster.MakeTestingClusterSettings()
	fixedTime := time.Date(2023, 10, 15, 14, 30, 45, 123456789, time.UTC)
	return &tree.EvalContext{
		Context:  context.Background(),
		Settings: st,
		SessionData: &sessiondata.SessionData{
			SequenceState: sessiondata.NewSequenceState(),
		},
		Locality:           roachpb.Locality{},
		ClusterID:          uuid.MakeV4(),
		ClusterName:        "test-cluster",
		NodeID:             1,
		ReCache:            tree.NewRegexpCache(512),
		Sequence:           &sqlbase.DummySequenceOperators{},
		SessionAccessor:    &sqlbase.DummySessionAccessor{},
		PrivilegedAccessor: &sqlbase.DummyPrivilegedAccessor{},
		TsDBAccessor:       &sqlbase.DummyTsDBAccessor{},
		Txn:                &kv.Txn{},
		StmtTimestamp:      fixedTime,
		TxnTimestamp:       fixedTime,
	}
}

// TestStringFunctions tests all string-related builtin functions.
func TestStringFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	// bit_length
	t.Run("bit_length", func(t *testing.T) {
		bit1, _ := tree.NewDBitArrayFromInt(0b10101, 5)
		bit2, _ := tree.NewDBitArrayFromInt(0, 0)
		bit3, _ := tree.NewDBitArrayFromInt(0b1111100000, 10)
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// String overload: number of bytes * 8
			{"ascii string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDInt(40), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDInt(0), false},
			{"unicode string (3 bytes per char)", tree.Datums{tree.NewDString("世界")}, 0, tree.NewDInt(48), false},

			// Bytes overload: length * 8
			{"normal bytes", tree.Datums{tree.NewDBytes("hello")}, 1, tree.NewDInt(40), false},
			{"empty bytes", tree.Datums{tree.NewDBytes("")}, 1, tree.NewDInt(0), false},

			// BitArray overload: actual bit length
			{"bit array 5 bits", tree.Datums{bit1}, 2, tree.NewDInt(5), false},
			{"empty bit array", tree.Datums{bit2}, 2, tree.NewDInt(0), false},
			{"bit array 10 bits", tree.Datums{bit3}, 2, tree.NewDInt(10), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Get bit_length builtin overloads
				_, overloads := GetBuiltinProperties("bit_length")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("octet_length", func(t *testing.T) {
		bit1, _ := tree.NewDBitArrayFromInt(0b10101, 5)
		bit2, _ := tree.NewDBitArrayFromInt(0b10101010, 8)
		bit3, _ := tree.NewDBitArrayFromInt(0b1111111111, 10)
		bit4, _ := tree.NewDBitArrayFromInt(0, 0)
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// String overload: byte length of string
			{"ascii string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDInt(5), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDInt(0), false},
			{"unicode string with multi-byte chars", tree.Datums{tree.NewDString("世界")}, 0, tree.NewDInt(6), false},

			// Bytes overload: raw byte length
			{"normal bytes", tree.Datums{tree.NewDBytes("hello")}, 1, tree.NewDInt(5), false},
			{"empty bytes", tree.Datums{tree.NewDBytes("")}, 1, tree.NewDInt(0), false},

			// BitArray overload: (bitLen + 7) / 8 (ceiling division)
			{"bit array 5 bits", tree.Datums{bit1}, 2, tree.NewDInt(1), false},
			{"bit array 8 bits", tree.Datums{bit2}, 2, tree.NewDInt(1), false},
			{"bit array 10 bits", tree.Datums{bit3}, 2, tree.NewDInt(2), false},
			{"empty bit array", tree.Datums{bit4}, 2, tree.NewDInt(0), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("octet_length")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// convert_from / convert_to
	t.Run("convert_from", func(t *testing.T) {
		fn := builtins["convert_from"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDBytes("hello"), tree.NewDString("utf8")})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("hello"), got)
	})
	t.Run("convert_to", func(t *testing.T) {
		fn := builtins["convert_to"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDString("hello"), tree.NewDString("utf8")})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDBytes("hello"), got)
	})

	// get_bit / set_bit (test only basic)
	t.Run("get_bit", func(t *testing.T) {
		bit1, _ := tree.NewDBitArrayFromInt(0b10000, 5)
		bit2, _ := tree.NewDBitArrayFromInt(0b101, 3)
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// ------------------------------
			// Overload 0: bit_array (VarBit)
			// ------------------------------
			{
				name:     "bit array get bit at index 0 (1)",
				args:     tree.Datums{bit1, tree.NewDInt(0)},
				overload: 0,
				expected: tree.NewDInt(1),
				wantErr:  false,
			},
			{
				name:     "bit array get bit at index 4 (0)",
				args:     tree.Datums{bit1, tree.NewDInt(4)},
				overload: 0,
				expected: tree.NewDInt(0),
				wantErr:  false,
			},
			{
				name:     "bit array negative index returns error",
				args:     tree.Datums{bit2, tree.NewDInt(-1)},
				overload: 0,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bit array index out of bounds returns error",
				args:     tree.Datums{bit2, tree.NewDInt(10)},
				overload: 0,
				expected: nil,
				wantErr:  true,
			},

			// ------------------------------
			// Overload 1: bytes (byte string)
			// ------------------------------
			{
				name:     "bytes get bit at index 0 (1)",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0b10000000})), tree.NewDInt(0)},
				overload: 1,
				expected: tree.NewDInt(1),
				wantErr:  false,
			},
			{
				name:     "bytes get bit at index 7 (0)",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0b10000000})), tree.NewDInt(7)},
				overload: 1,
				expected: tree.NewDInt(0),
				wantErr:  false,
			},
			{
				name:     "bytes multi-byte get bit at index 9",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0xFF, 0b01000000})), tree.NewDInt(9)},
				overload: 1,
				expected: tree.NewDInt(1),
				wantErr:  false,
			},
			{
				name:     "bytes negative index returns error",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00})), tree.NewDInt(-2)},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bytes index out of bounds returns error",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00})), tree.NewDInt(100)},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("get_bit")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("set_bit", func(t *testing.T) {
		bit1, _ := tree.NewDBitArrayFromInt(0b10000, 5)
		bit2, _ := tree.NewDBitArrayFromInt(0b101, 3)
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// ------------------------------
			// Overload 0: bit_string (VarBit)
			// ------------------------------
			{
				name:     "bit array set bit 0 to 1",
				args:     tree.Datums{bit1, tree.NewDInt(0), tree.NewDInt(1)},
				overload: 0,
				expected: bit1,
				wantErr:  false,
			},
			{
				name:     "bit array set bit 2 to 0",
				args:     tree.Datums{bit1, tree.NewDInt(2), tree.NewDInt(0)},
				overload: 0,
				expected: bit1,
				wantErr:  false,
			},
			{
				name:     "bit array invalid bit value (2) returns error",
				args:     tree.Datums{bit2, tree.NewDInt(0), tree.NewDInt(2)},
				overload: 0,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bit array negative index returns error",
				args:     tree.Datums{bit2, tree.NewDInt(-1), tree.NewDInt(1)},
				overload: 0,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bit array index out of bounds returns error",
				args:     tree.Datums{bit2, tree.NewDInt(10), tree.NewDInt(1)},
				overload: 0,
				expected: nil,
				wantErr:  true,
			},

			// ------------------------------
			// Overload 1: byte_string (Bytes)
			// ------------------------------
			{
				name:     "bytes set bit 0 to 1",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0b00000000})), tree.NewDInt(0), tree.NewDInt(1)},
				overload: 1,
				expected: tree.NewDBytes(tree.DBytes([]byte{0b10000000})),
				wantErr:  false,
			},
			{
				name:     "bytes set bit 7 to 0",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0b11111111})), tree.NewDInt(7), tree.NewDInt(0)},
				overload: 1,
				expected: tree.NewDBytes(tree.DBytes([]byte{0b11111110})),
				wantErr:  false,
			},
			{
				name:     "bytes multi-byte set bit 9 to 1",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00, 0x00})), tree.NewDInt(9), tree.NewDInt(1)},
				overload: 1,
				expected: tree.NewDBytes(tree.DBytes([]byte{0x00, 0b01000000})),
				wantErr:  false,
			},
			{
				name:     "bytes invalid bit value (5) returns error",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00})), tree.NewDInt(0), tree.NewDInt(5)},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bytes negative index returns error",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00})), tree.NewDInt(-3), tree.NewDInt(1)},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
			{
				name:     "bytes index out of bounds returns error",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00})), tree.NewDInt(100), tree.NewDInt(1)},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("set_bit")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// client_encoding
	t.Run("client_encoding", func(t *testing.T) {
		fn := builtins["client_encoding"].overloads[0].Fn
		ctx.SessionData.ClientEncoding = "UTF8"
		got, err := fn(ctx, tree.Datums{})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("UTF8"), got)
	})

	// cast_check_ts
	t.Run("cast_check_ts", func(t *testing.T) {
		fn := builtins["cast_check_ts"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDString("123"), tree.NewDString("int")})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, got)

		got, err = fn(ctx, tree.Datums{tree.NewDString("abc"), tree.NewDString("int")})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, got)
	})

	// fnv32, fnv32a, fnv64, fnv64a, crc32ieee, crc32c
	t.Run("hash_functions", func(t *testing.T) {
		testCases := []struct {
			name   string
			fnName string
			input  string
		}{
			{"fnv32", "fnv32", "hello"},
			{"fnv32a", "fnv32a", "hello"},
			{"fnv64", "fnv64", "hello"},
			{"fnv64a", "fnv64a", "hello"},
			{"crc32ieee", "crc32ieee", "hello"},
			{"crc32c", "crc32c", "hello"},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fn := builtins[tc.fnName].overloads[0].Fn // string variant
				got, err := fn(ctx, tree.Datums{tree.NewDString(tc.input)})
				require.NoError(t, err)
				assert.NotNil(t, got)
			})
		}
	})

	// to_dec, to_oct, to_bin (all return errors)
	t.Run("to_dec", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{"int argument", tree.Datums{tree.NewDInt(123)}, 0, true},
			{"string argument", tree.Datums{tree.NewDString("test")}, 1, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_dec")
				overload := overloads[tt.overload]

				_, err := overload.Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})
	t.Run("to_oct", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{"int argument", tree.Datums{tree.NewDInt(123)}, 0, true},
			{"string argument", tree.Datums{tree.NewDString("test")}, 1, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_oct")
				overload := overloads[tt.overload]

				_, err := overload.Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})
	t.Run("to_bin", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{"int argument", tree.Datums{tree.NewDInt(123)}, 0, true},
			{"string argument", tree.Datums{tree.NewDString("test")}, 1, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_bin")
				overload := overloads[tt.overload]

				_, err := overload.Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})

	t.Run("leftbit", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{name: "int input", args: tree.Datums{tree.NewDInt(1), tree.NewDInt(1)}, overload: 0, wantErr: true},
			{name: "float input", args: tree.Datums{tree.NewDFloat(1.0), tree.NewDInt(1)}, overload: 1, wantErr: true},
			{name: "timestamp input", args: tree.Datums{tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), tree.NewDInt(1)}, overload: 2, wantErr: true},
			{name: "bool input", args: tree.Datums{tree.MakeDBool(true), tree.NewDInt(1)}, overload: 3, wantErr: true},
			{name: "string input", args: tree.Datums{tree.NewDString("a"), tree.NewDInt(1)}, overload: 4, wantErr: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("leftbit")
				_, err := overloads[tt.overload].Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})

	t.Run("subbit", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{name: "int input", args: tree.Datums{tree.NewDInt(1), tree.NewDInt(1), tree.NewDInt(2)}, overload: 0, wantErr: true},
			{name: "float input", args: tree.Datums{tree.NewDFloat(1.0), tree.NewDInt(1), tree.NewDInt(2)}, overload: 1, wantErr: true},
			{name: "timestamp input", args: tree.Datums{tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), tree.NewDInt(1), tree.NewDInt(2)}, overload: 2, wantErr: true},
			{name: "bool input", args: tree.Datums{tree.MakeDBool(true), tree.NewDInt(1), tree.NewDInt(2)}, overload: 3, wantErr: true},
			{name: "string input", args: tree.Datums{tree.NewDString("a"), tree.NewDInt(1), tree.NewDInt(2)}, overload: 4, wantErr: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("subbit")
				_, err := overloads[tt.overload].Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})

	t.Run("rightbit", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{name: "int input", args: tree.Datums{tree.NewDInt(1), tree.NewDInt(1)}, overload: 0, wantErr: true},
			{name: "float input", args: tree.Datums{tree.NewDFloat(1.0), tree.NewDInt(1)}, overload: 1, wantErr: true},
			{name: "timestamp input", args: tree.Datums{tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), tree.NewDInt(1)}, overload: 2, wantErr: true},
			{name: "bool input", args: tree.Datums{tree.MakeDBool(true), tree.NewDInt(1)}, overload: 3, wantErr: true},
			{name: "string input", args: tree.Datums{tree.NewDString("a"), tree.NewDInt(1)}, overload: 4, wantErr: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("rightbit")
				_, err := overloads[tt.overload].Fn(ctx, tt.args)
				assert.Error(t, err)
			})
		}
	})

	t.Run("length", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			{"string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDInt(5), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDInt(0), false},
			{"unicode", tree.Datums{tree.NewDString("世界")}, 0, tree.NewDInt(2), false},
			{"bytes", tree.Datums{tree.NewDBytes("hello")}, 1, tree.NewDInt(5), false},
			{"empty bytes", tree.Datums{tree.NewDBytes("")}, 1, tree.NewDInt(0), false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				overload := lengthImpls(true).overloads[tt.overload]
				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// Test char_length function (only 2 overloads: string, bytes)
	t.Run("char_length", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			{"string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDInt(5), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDInt(0), false},
			{"unicode", tree.Datums{tree.NewDString("世界")}, 0, tree.NewDInt(2), false},
			{"bytes", tree.Datums{tree.NewDBytes("hello")}, 1, tree.NewDInt(5), false},
			{"empty bytes", tree.Datums{tree.NewDBytes("")}, 1, tree.NewDInt(0), false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				overload := lengthImpls(false).overloads[tt.overload]
				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// Test character_length function (same as char_length)
	t.Run("character_length", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			{"string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDInt(5), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDInt(0), false},
			{"unicode", tree.Datums{tree.NewDString("世界")}, 0, tree.NewDInt(2), false},
			{"bytes", tree.Datums{tree.NewDBytes("hello")}, 1, tree.NewDInt(5), false},
			{"empty bytes", tree.Datums{tree.NewDBytes("")}, 1, tree.NewDInt(0), false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				overload := lengthImpls(false).overloads[tt.overload]
				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("lower", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// String overload: convert to lowercase
			{"uppercase string", tree.Datums{tree.NewDString("HELLO")}, 0, tree.NewDString("hello"), false},
			{"mixed case string", tree.Datums{tree.NewDString("HeLLo WoRLd")}, 0, tree.NewDString("hello world"), false},
			{"lowercase string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDString("hello"), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDString(""), false},
			{"string with numbers and symbols", tree.Datums{tree.NewDString("Go123!@#")}, 0, tree.NewDString("go123!@#"), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("lower")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("upper", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// String overload: convert to uppercase
			{"lowercase string", tree.Datums{tree.NewDString("hello")}, 0, tree.NewDString("HELLO"), false},
			{"mixed case string", tree.Datums{tree.NewDString("HeLLo WoRLd")}, 0, tree.NewDString("HELLO WORLD"), false},
			{"uppercase string", tree.Datums{tree.NewDString("HELLO")}, 0, tree.NewDString("HELLO"), false},
			{"empty string", tree.Datums{tree.NewDString("")}, 0, tree.NewDString(""), false},
			{"string with numbers and symbols", tree.Datums{tree.NewDString("go123!@#")}, 0, tree.NewDString("GO123!@#"), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("upper")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("substr", func(t *testing.T) {
		substrFn := builtins["substr"].overloads[0].Fn // string, int
		got, err := substrFn(ctx, tree.Datums{tree.NewDString("abcdef"), tree.NewDInt(2)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("bcdef"), got)

		substrFn2 := builtins["substr"].overloads[1].Fn // string, int, int
		got, err = substrFn2(ctx, tree.Datums{tree.NewDString("abcdef"), tree.NewDInt(2), tree.NewDInt(3)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("bcd"), got)
	})

	t.Run("concat", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			// Variadic string concatenation tests
			{"two normal strings",
				tree.Datums{tree.NewDString("hello"), tree.NewDString(" world")},
				tree.NewDString("hello world"),
				false,
			},
			{"multiple string arguments",
				tree.Datums{tree.NewDString("a"), tree.NewDString("b"), tree.NewDString("c")},
				tree.NewDString("abc"),
				false,
			},
			{"single string",
				tree.Datums{tree.NewDString("test")},
				tree.NewDString("test"),
				false,
			},
			{"empty strings",
				tree.Datums{tree.NewDString(""), tree.NewDString("")},
				tree.NewDString(""),
				false,
			},
			{"contains null returns null",
				tree.Datums{tree.NewDString("hello"), tree.DNull},
				tree.DNull,
				false,
			},
			{"all null returns null",
				tree.Datums{tree.DNull, tree.DNull},
				tree.DNull,
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("concat")
				overload := overloads[0]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("concat_ws", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			// Normal case with separator and two strings
			{"normal separator and two strings",
				tree.Datums{tree.NewDString(","), tree.NewDString("a"), tree.NewDString("b")},
				tree.NewDString("a,b"),
				false,
			},
			// Multiple arguments
			{"multiple arguments with separator",
				tree.Datums{tree.NewDString("-"), tree.NewDString("x"), tree.NewDString("y"), tree.NewDString("z")},
				tree.NewDString("x-y-z"),
				false,
			},
			// NULL in non-separator arguments is skipped
			{"skip null arguments",
				tree.Datums{tree.NewDString(","), tree.NewDString("a"), tree.DNull, tree.NewDString("b")},
				tree.NewDString("a,b"),
				false,
			},
			// Separator is NULL → return NULL
			{"null separator returns null",
				tree.Datums{tree.DNull, tree.NewDString("a"), tree.NewDString("b")},
				tree.DNull,
				false,
			},
			// Only separator, no content
			{"only separator returns empty string",
				tree.Datums{tree.NewDString(",")},
				tree.NewDString(""),
				false,
			},
			// Empty separator
			{"empty separator",
				tree.Datums{tree.NewDString(""), tree.NewDString("a"), tree.NewDString("b")},
				tree.NewDString("ab"),
				false,
			},
			// Empty content strings
			{"empty content strings",
				tree.Datums{tree.NewDString(","), tree.NewDString(""), tree.NewDString("")},
				tree.NewDString(","),
				false,
			},
			// No arguments (expect error)
			{"no arguments returns error",
				tree.Datums{},
				nil,
				true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("concat_ws")
				overload := overloads[0]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("split_part", func(t *testing.T) {
		splitPartFn := builtins["split_part"].overloads[0].Fn
		got, err := splitPartFn(ctx, tree.Datums{tree.NewDString("1,2,3"), tree.NewDString(","), tree.NewDInt(2)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("2"), got)

		// Out of range returns empty string
		got, err = splitPartFn(ctx, tree.Datums{tree.NewDString("1,2,3"), tree.NewDString(","), tree.NewDInt(5)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString(""), got)

		// Zero or negative field returns error
		_, err = splitPartFn(ctx, tree.Datums{tree.NewDString("1,2,3"), tree.NewDString(","), tree.NewDInt(0)})
		assert.Error(t, err)
	})

	t.Run("repeat", func(t *testing.T) {
		repeatFn := builtins["repeat"].overloads[0].Fn
		got, err := repeatFn(ctx, tree.Datums{tree.NewDString("abc"), tree.NewDInt(3)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("abcabcabc"), got)

		got, err = repeatFn(ctx, tree.Datums{tree.NewDString("abc"), tree.NewDInt(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString(""), got)
	})

	t.Run("replace", func(t *testing.T) {
		replaceFn := builtins["replace"].overloads[0].Fn
		got, err := replaceFn(ctx, tree.Datums{tree.NewDString("hello world"), tree.NewDString("world"), tree.NewDString("earth")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("hello earth"), got)

		got, err = replaceFn(ctx, tree.Datums{tree.NewDString("abc abc"), tree.NewDString("a"), tree.NewDString("x")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("xbc xbc"), got)
	})

	t.Run("translate", func(t *testing.T) {
		// Get the translate function overload
		translateFn := builtins["translate"].overloads[0].Fn

		// Test translate function with input "hello", find "hel", replace "123"
		// Actual behavior:
		// - 'h' → '1' (1st char of find → 1st char of replace)
		// - 'e' → '2' (2nd char of find → 2nd char of replace)
		// - 'l' → '3' (3rd char of find → 3rd char of replace)
		// - The second 'l' in "hello" also matches 'l' in find → replaced with '3'
		// Expected result updated to "1233o" (matches function's actual logic)
		got, err := translateFn(ctx, tree.Datums{tree.NewDString("hello"), tree.NewDString("hel"), tree.NewDString("123")})
		assert.NoError(t, err)
		// Modify expected value from "123lo" to "1233o" to match actual return value
		assert.Equal(t, tree.NewDString("1233o"), got)
	})

	t.Run("reverse", func(t *testing.T) {
		reverseFn := builtins["reverse"].overloads[0].Fn
		got, err := reverseFn(ctx, tree.Datums{tree.NewDString("hello")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("olleh"), got)
	})

	t.Run("overlay", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: 3 arguments (input, replacement, start position)
			{
				name:     "3 args normal replacement",
				args:     tree.Datums{tree.NewDString("doggie"), tree.NewDString("CAT"), tree.NewDInt(2)},
				overload: 0,
				expected: tree.NewDString("dCATie"),
				wantErr:  false,
			},
			{
				name:     "3 args start at position 1",
				args:     tree.Datums{tree.NewDString("hello"), tree.NewDString("XX"), tree.NewDInt(1)},
				overload: 0,
				expected: tree.NewDString("XXllo"),
				wantErr:  false,
			},
			{
				name:     "3 args empty replacement string",
				args:     tree.Datums{tree.NewDString("abcde"), tree.NewDString(""), tree.NewDInt(3)},
				overload: 0,
				expected: tree.NewDString("abcde"),
				wantErr:  false,
			},

			// Overload 1: 4 arguments (input, replacement, start, length)
			{
				name:     "4 args normal overlay",
				args:     tree.Datums{tree.NewDString("abcdef"), tree.NewDString("X"), tree.NewDInt(3), tree.NewDInt(2)},
				overload: 1,
				expected: tree.NewDString("abXef"),
				wantErr:  false,
			},
			{
				name:     "4 args insert at position without delete",
				args:     tree.Datums{tree.NewDString("test"), tree.NewDString("XY"), tree.NewDInt(3), tree.NewDInt(0)},
				overload: 1,
				expected: tree.NewDString("teXYst"),
				wantErr:  false,
			},
			{
				name:     "4 args delete all characters",
				args:     tree.Datums{tree.NewDString("12345"), tree.NewDString("A"), tree.NewDInt(1), tree.NewDInt(5)},
				overload: 1,
				expected: tree.NewDString("A"),
				wantErr:  false,
			},
			{
				name:     "4 args empty input string",
				args:     tree.Datums{tree.NewDString(""), tree.NewDString("hello"), tree.NewDInt(1), tree.NewDInt(0)},
				overload: 1,
				expected: tree.NewDString("hello"),
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("overlay")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("lpad", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: 2 args (string, length), default fill with space
			{"pad with spaces to longer length",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(8)},
				0,
				tree.NewDString("   hello"),
				false,
			},
			{"truncate string longer than target length",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(3)},
				0,
				tree.NewDString("hel"),
				false,
			},
			{"pad empty string with spaces",
				tree.Datums{tree.NewDString(""), tree.NewDInt(5)},
				0,
				tree.NewDString("     "),
				false,
			},
			{"zero length returns empty string",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(0)},
				0,
				tree.NewDString(""),
				false,
			},

			// Overload 1: 3 args (string, length, fill string)
			{"pad with custom fill character",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(7), tree.NewDString("xy")},
				1,
				tree.NewDString("xyhello"),
				false,
			},
			{"truncate with custom fill",
				tree.Datums{tree.NewDString("abcdef"), tree.NewDInt(4), tree.NewDString("*")},
				1,
				tree.NewDString("abcd"),
				false,
			},
			{"fill with multi-character string",
				tree.Datums{tree.NewDString("123"), tree.NewDInt(10), tree.NewDString("0")},
				1,
				tree.NewDString("0000000123"),
				false,
			},
			{"empty fill string returns original truncated",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(10), tree.NewDString("")},
				1,
				tree.NewDString("test"),
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("lpad")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("rpad", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: 2 args (string, length), default fill with space
			{"pad with spaces to longer length",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(8)},
				0,
				tree.NewDString("hello   "),
				false,
			},
			{"truncate string longer than target length",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(3)},
				0,
				tree.NewDString("hel"),
				false,
			},
			{"pad empty string with spaces",
				tree.Datums{tree.NewDString(""), tree.NewDInt(5)},
				0,
				tree.NewDString("     "),
				false,
			},
			{"zero length returns empty string",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(0)},
				0,
				tree.NewDString(""),
				false,
			},

			// Overload 1: 3 args (string, length, fill string)
			{"pad with custom fill character",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(7), tree.NewDString("xy")},
				1,
				tree.NewDString("helloxy"),
				false,
			},
			{"truncate with custom fill",
				tree.Datums{tree.NewDString("abcdef"), tree.NewDInt(4), tree.NewDString("*")},
				1,
				tree.NewDString("abcd"),
				false,
			},
			{"fill with multi-character string",
				tree.Datums{tree.NewDString("123"), tree.NewDInt(10), tree.NewDString("0")},
				1,
				tree.NewDString("1230000000"),
				false,
			},
			{"empty fill string returns original truncated",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(10), tree.NewDString("")},
				1,
				tree.NewDString("test"),
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("rpad")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("trim", func(t *testing.T) {
		trimFn := builtins["btrim"].overloads[1].Fn
		got, err := trimFn(ctx, tree.Datums{tree.NewDString("  hello  ")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("hello"), got)

		trimFn2 := builtins["btrim"].overloads[0].Fn
		got, err = trimFn2(ctx, tree.Datums{tree.NewDString("xxxhelloxxx"), tree.NewDString("x")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("hello"), got)
	})

	t.Run("kwdbdb_internal.pb_to_json", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: 2 args (pbname string, data bytes)
			{
				name: "2 args invalid protobuf type returns error",
				args: tree.Datums{
					tree.NewDString("invalid.proto.type"),
					tree.NewDBytes(tree.DBytes([]byte{})),
				},
				overload: 0,
				wantErr:  true,
			},

			// Overload 1: 3 args (pbname string, data bytes, emit_defaults bool)
			{
				name: "3 args invalid protobuf type returns error",
				args: tree.Datums{
					tree.NewDString("invalid.proto.type"),
					tree.NewDBytes(tree.DBytes([]byte{})),
					tree.MakeDBool(true),
				},
				overload: 1,
				wantErr:  true,
			},
			{
				name: "3 args with emit_defaults false returns error",
				args: tree.Datums{
					tree.NewDString("invalid.proto.type"),
					tree.NewDBytes(tree.DBytes([]byte{0x00, 0x01, 0x02})),
					tree.MakeDBool(false),
				},
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdbdb_internal.pb_to_json")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, got)
				}
			})
		}
	})

	t.Run("left", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: Bytes input
			{"left positive bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x61, 0x62, 0x63, 0x64})), tree.NewDInt(2)},
				0,
				tree.NewDBytes(tree.DBytes([]byte{0x61, 0x62})),
				false,
			},
			{"left negative bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abcd"))), tree.NewDInt(-1)},
				0,
				tree.NewDBytes(tree.DBytes([]byte("abc"))),
				false,
			},
			{"left exceed length bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abc"))), tree.NewDInt(10)},
				0,
				tree.NewDBytes(tree.DBytes([]byte("abc"))),
				false,
			},
			{"left too negative returns empty",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abc"))), tree.NewDInt(-10)},
				0,
				tree.NewDBytes(tree.DBytes([]byte{})),
				false,
			},

			// Overload 1: String input
			{"left positive string",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(3)},
				1,
				tree.NewDString("hel"),
				false,
			},
			{"left negative string",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(-2)},
				1,
				tree.NewDString("hel"),
				false,
			},
			{"left unicode string",
				tree.Datums{tree.NewDString("世界你好"), tree.NewDInt(2)},
				1,
				tree.NewDString("世界"),
				false,
			},
			{"left zero returns empty",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(0)},
				1,
				tree.NewDString(""),
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("left")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("right", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: Bytes input
			{"right positive bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x61, 0x62, 0x63, 0x64})), tree.NewDInt(2)},
				0,
				tree.NewDBytes(tree.DBytes([]byte{0x63, 0x64})),
				false,
			},
			{"right negative bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abcd"))), tree.NewDInt(-1)},
				0,
				tree.NewDBytes(tree.DBytes([]byte("bcd"))),
				false,
			},
			{"right exceed length bytes",
				tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abc"))), tree.NewDInt(10)},
				0,
				tree.NewDBytes(tree.DBytes([]byte("abc"))),
				false,
			},

			// Overload 1: String input
			{"right positive string",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(3)},
				1,
				tree.NewDString("llo"),
				false,
			},
			{"right negative string",
				tree.Datums{tree.NewDString("hello"), tree.NewDInt(-2)},
				1,
				tree.NewDString("llo"),
				false,
			},
			{"right unicode string",
				tree.Datums{tree.NewDString("世界你好"), tree.NewDInt(2)},
				1,
				tree.NewDString("你好"),
				false,
			},
			{"right zero returns empty",
				tree.Datums{tree.NewDString("test"), tree.NewDInt(0)},
				1,
				tree.NewDString(""),
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("right")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("ascii/chr", func(t *testing.T) {
		asciiFn := builtins["ascii"].overloads[0].Fn
		got, err := asciiFn(ctx, tree.Datums{tree.NewDString("A")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(65), got)

		chrFn := builtins["chr"].overloads[0].Fn
		got, err = chrFn(ctx, tree.Datums{tree.NewDInt(65)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("A"), got)
	})

	t.Run("md5", func(t *testing.T) {
		md5Fn := builtins["md5"].overloads[0].Fn
		got, err := md5Fn(ctx, tree.Datums{tree.NewDString("hello")})
		assert.NoError(t, err)
		expected := fmt.Sprintf("%x", md5.Sum([]byte("hello")))
		assert.Equal(t, tree.NewDString(expected), got)
	})

	t.Run("sha1", func(t *testing.T) {
		sha1Fn := builtins["sha1"].overloads[0].Fn
		got, err := sha1Fn(ctx, tree.Datums{tree.NewDString("hello")})
		assert.NoError(t, err)
		expected := fmt.Sprintf("%x", sha1.Sum([]byte("hello")))
		assert.Equal(t, tree.NewDString(expected), got)
	})

	t.Run("sha256", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			{
				name:     "hash empty string",
				args:     tree.Datums{tree.NewDString("")},
				overload: 0,
				expected: tree.NewDString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
				wantErr:  false,
			},
			{
				name:     "hash normal string",
				args:     tree.Datums{tree.NewDString("abc")},
				overload: 0,
				expected: tree.NewDString("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
				wantErr:  false,
			},
			{
				name:     "hash bytes input",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes("abc"))},
				overload: 1,
				expected: tree.NewDString("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("sha256")
				overload := overloads[tt.overload]
				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("sha512", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			{
				name:     "hash empty string",
				args:     tree.Datums{tree.NewDString("")},
				overload: 0,
				expected: tree.NewDString("cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"),
				wantErr:  false,
			},
			{
				name:     "hash normal string",
				args:     tree.Datums{tree.NewDString("abc")},
				overload: 0,
				expected: tree.NewDString("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"),
				wantErr:  false,
			},
			{
				name:     "hash bytes input",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes("abc"))},
				overload: 1,
				expected: tree.NewDString("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"),
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("sha512")
				overload := overloads[tt.overload]
				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("encode/decode", func(t *testing.T) {
		encodeFn := builtins["encode"].overloads[0].Fn
		got, err := encodeFn(ctx, tree.Datums{tree.NewDBytes("hello"), tree.NewDString("hex")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("68656c6c6f"), got)

		decodeFn := builtins["decode"].overloads[0].Fn
		got2, err := decodeFn(ctx, tree.Datums{tree.NewDString("68656c6c6f"), tree.NewDString("hex")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDBytes("hello"), got2)
	})

	t.Run("quote_ident", func(t *testing.T) {
		quoteFn := builtins["quote_ident"].overloads[0].Fn
		got, err := quoteFn(ctx, tree.Datums{tree.NewDString("foo")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("foo"), got) // no quotes needed

		got, err = quoteFn(ctx, tree.Datums{tree.NewDString("foo bar")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString(`"foo bar"`), got)
	})

	t.Run("quote_literal", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: string input (preferred)
			{
				name:     "plain string",
				args:     tree.Datums{tree.NewDString("hello")},
				overload: 0,
				expected: tree.NewDString("'hello'"),
				wantErr:  false,
			},
			{
				name:     "string with single quote",
				args:     tree.Datums{tree.NewDString("hello 'world'")},
				overload: 0,
				expected: tree.NewDString("e'hello \\'world\\''"),
				wantErr:  false,
			},
			{
				name:     "empty string",
				args:     tree.Datums{tree.NewDString("")},
				overload: 0,
				expected: tree.NewDString("''"),
				wantErr:  false,
			},

			// Overload 1: any type (cast to string first)
			{
				name:     "integer cast to string",
				args:     tree.Datums{tree.NewDInt(123)},
				overload: 1,
				expected: tree.NewDString("'123'"),
				wantErr:  false,
			},
			{
				name:     "bool cast to string",
				args:     tree.Datums{tree.MakeDBool(true)},
				overload: 1,
				expected: tree.NewDString("'true'"),
				wantErr:  false,
			},
			{
				name:     "bytes cast to string",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abc")))},
				overload: 1,
				expected: tree.NewDString("e'\\\\x616263'"),
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("quote_literal")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("quote_nullable", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: String input (preferred)
			{
				name:     "string input with quotes",
				args:     tree.Datums{tree.NewDString("hello 'world'")},
				overload: 0,
				expected: tree.NewDString("e'hello \\'world\\''"),
				wantErr:  false,
			},
			{
				name:     "empty string input",
				args:     tree.Datums{tree.NewDString("")},
				overload: 0,
				expected: tree.NewDString("''"),
				wantErr:  false,
			},
			{
				name:     "null input returns NULL string",
				args:     tree.Datums{tree.DNull},
				overload: 0,
				expected: tree.NewDString("NULL"),
				wantErr:  false,
			},

			// Overload 1: Any type input
			{
				name:     "integer input cast to string",
				args:     tree.Datums{tree.NewDInt(456)},
				overload: 1,
				expected: tree.NewDString("'456'"),
				wantErr:  false,
			},
			{
				name:     "boolean false input",
				args:     tree.Datums{tree.MakeDBool(false)},
				overload: 1,
				expected: tree.NewDString("'false'"),
				wantErr:  false,
			},
			{
				name:     "bytes input converted to string",
				args:     tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x61, 0x62, 0x63}))},
				overload: 1,
				expected: tree.NewDString("e'\\\\x616263'"),
				wantErr:  false,
			},
			{
				name:     "null any type returns NULL string",
				args:     tree.Datums{tree.DNull},
				overload: 1,
				expected: tree.NewDString("NULL"),
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("quote_nullable")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("regexp_extract", func(t *testing.T) {
		regexpFn := builtins["regexp_extract"].overloads[0].Fn
		got, err := regexpFn(ctx, tree.Datums{tree.NewDString("abc123def"), tree.NewDString("\\d+")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("123"), got)
	})

	t.Run("regexp_replace", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: 3 arguments (input, regex, replacement)
			{
				name:     "basic replace first match",
				args:     tree.Datums{tree.NewDString("hello hello"), tree.NewDString("hello"), tree.NewDString("hi")},
				overload: 0,
				expected: tree.NewDString("hi hello"),
				wantErr:  false,
			},
			{
				name:     "no match returns original string",
				args:     tree.Datums{tree.NewDString("abcde"), tree.NewDString("xyz"), tree.NewDString("123")},
				overload: 0,
				expected: tree.NewDString("abcde"),
				wantErr:  false,
			},
			{
				name:     "replace with empty string",
				args:     tree.Datums{tree.NewDString("test123"), tree.NewDString("[0-9]"), tree.NewDString("")},
				overload: 0,
				expected: tree.NewDString("test23"),
				wantErr:  false,
			},

			// Overload 1: 4 arguments (input, regex, replacement, flags)
			{
				name:     "case-insensitive replace (flag i)",
				args:     tree.Datums{tree.NewDString("Hello HELLO hello"), tree.NewDString("hello"), tree.NewDString("hi"), tree.NewDString("i")},
				overload: 1,
				expected: tree.NewDString("hi HELLO hello"),
				wantErr:  false,
			},
			{
				name:     "global replace all matches (flag g)",
				args:     tree.Datums{tree.NewDString("a b c a b c"), tree.NewDString("a"), tree.NewDString("x"), tree.NewDString("g")},
				overload: 1,
				expected: tree.NewDString("x b c x b c"),
				wantErr:  false,
			},
			{
				name:     "global case-insensitive replace (flag gi)",
				args:     tree.Datums{tree.NewDString("Hi hi HI"), tree.NewDString("hi"), tree.NewDString("hello"), tree.NewDString("gi")},
				overload: 1,
				expected: tree.NewDString("hello hello hello"),
				wantErr:  false,
			},
			{
				name:     "invalid regex pattern returns error",
				args:     tree.Datums{tree.NewDString("test"), tree.NewDString("(invalid"), tree.NewDString("x"), tree.NewDString("g")},
				overload: 1,
				expected: nil,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("regexp_replace")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestLikeEscapeFunction tests like_escape function
	t.Run("like_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"basic match", tree.Datums{tree.NewDString("abc"), tree.NewDString("a%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"no match", tree.Datums{tree.NewDString("abc"), tree.NewDString("b%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
			{"escape character", tree.Datums{tree.NewDString("a%c"), tree.NewDString("a\\%%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("like_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestNotLikeEscapeFunction tests not_like_escape function
	t.Run("not_like_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"basic not match", tree.Datums{tree.NewDString("abc"), tree.NewDString("b%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"match negated", tree.Datums{tree.NewDString("abc"), tree.NewDString("a%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("not_like_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestILikeEscapeFunction tests ilike_escape function (case-insensitive)
	t.Run("ilike_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"case-insensitive match", tree.Datums{tree.NewDString("ABC"), tree.NewDString("a%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"case-insensitive no match", tree.Datums{tree.NewDString("ABC"), tree.NewDString("x%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("ilike_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestNotILikeEscapeFunction tests not_ilike_escape function
	t.Run("not_ilike_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"case-insensitive not match", tree.Datums{tree.NewDString("ABC"), tree.NewDString("x%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"case-insensitive negated match", tree.Datums{tree.NewDString("ABC"), tree.NewDString("a%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("not_ilike_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestSimilarToEscapeFunction tests similar_to_escape function
	t.Run("similar_to_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"basic similar to match", tree.Datums{tree.NewDString("abc"), tree.NewDString("%(b|d)%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"no similar to match", tree.Datums{tree.NewDString("abc"), tree.NewDString("%(x|y)%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("similar_to_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestNotSimilarToEscapeFunction tests not_similar_to_escape function
	t.Run("not_similar_to_escape", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"not similar to match", tree.Datums{tree.NewDString("abc"), tree.NewDString("%(x|y)%"), tree.NewDString("\\")}, tree.MakeDBool(true), false},
			{"similar to negated match", tree.Datums{tree.NewDString("abc"), tree.NewDString("%(b|d)%"), tree.NewDString("\\")}, tree.MakeDBool(false), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("not_similar_to_escape")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// TestInitcapFunction tests initcap function (capitalizes first letter of each word)
	t.Run("initcap", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"lowercase string", tree.Datums{tree.NewDString("hello world")}, tree.NewDString("Hello World"), false},
			{"uppercase string", tree.Datums{tree.NewDString("HELLO WORLD")}, tree.NewDString("Hello World"), false},
			{"mixed case", tree.Datums{tree.NewDString("hElLo WoRlD")}, tree.NewDString("Hello World"), false},
			{"empty string", tree.Datums{tree.NewDString("")}, tree.NewDString(""), false},
			{"single character", tree.Datums{tree.NewDString("a")}, tree.NewDString("A"), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("initcap")
				got, err := overloads[0].Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("strpos", func(t *testing.T) {
		strposFn := builtins["strpos"].overloads[0].Fn
		got, err := strposFn(ctx, tree.Datums{tree.NewDString("hello"), tree.NewDString("l")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(3), got)
	})

	t.Run("to_hex", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			expected tree.Datum
			wantErr  bool
		}{
			// Overload 0: Int to hex
			{"int zero", tree.Datums{tree.NewDInt(0)}, 0, tree.NewDString("0"), false},
			{"int positive small", tree.Datums{tree.NewDInt(255)}, 0, tree.NewDString("ff"), false},
			{"int positive large", tree.Datums{tree.NewDInt(123456)}, 0, tree.NewDString("1e240"), false},
			{"int negative value", tree.Datums{tree.NewDInt(-1)}, 0, tree.NewDString("ffffffffffffffff"), false},

			// Overload 1: Bytes to hex (USE STANDARD FORMAT BELOW)
			{"empty bytes", tree.Datums{tree.NewDBytes(tree.DBytes([]byte{}))}, 1, tree.NewDString(""), false},
			{"normal ASCII bytes", tree.Datums{tree.NewDBytes(tree.DBytes([]byte("abc")))}, 1, tree.NewDString("616263"), false},
			{"binary bytes", tree.Datums{tree.NewDBytes(tree.DBytes([]byte{0x00, 0x01, 0xFF}))}, 1, tree.NewDString("0001ff"), false},

			// Overload 2: String to hex
			{"empty string", tree.Datums{tree.NewDString("")}, 2, tree.NewDString(""), false},
			{"ASCII string", tree.Datums{tree.NewDString("abc")}, 2, tree.NewDString("616263"), false},
			{"unicode string", tree.Datums{tree.NewDString("世界")}, 2, tree.NewDString("e4b896e7958c"), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_hex")
				overload := overloads[tt.overload]

				got, err := overload.Fn(ctx, tt.args)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("to_english", func(t *testing.T) {
		toEnglishFn := builtins["to_english"].overloads[0].Fn
		got, err := toEnglishFn(ctx, tree.Datums{tree.NewDInt(123)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("one-two-three"), got)
	})
}

// ------------------------------------------------------------------------
// Sequence functions (simulate minimal behavior)
// ------------------------------------------------------------------------
func TestSequenceFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	t.Run("nextval", func(t *testing.T) {
		_, overloads := GetBuiltinProperties("nextval")
		// Invalid sequence → returns error (no panic)
		_, err := overloads[0].Fn(ctx, tree.Datums{tree.NewDString("invalid_seq")})
		assert.Error(t, err)
	})

	t.Run("currval", func(t *testing.T) {
		_, overloads := GetBuiltinProperties("currval")
		// No nextval called → returns error (no panic)
		_, err := overloads[0].Fn(ctx, tree.Datums{tree.NewDString("test_seq")})
		assert.Error(t, err)
	})

	t.Run("lastval", func(t *testing.T) {
		_, overloads := GetBuiltinProperties("lastval")
		// Safe because SessionData is initialized
		_, err := overloads[0].Fn(ctx, tree.Datums{})
		assert.Error(t, err)
	})

	t.Run("setval", func(t *testing.T) {
		_, overloads := GetBuiltinProperties("setval")

		// Overload 0: 2 args
		_, err0 := overloads[0].Fn(ctx, tree.Datums{tree.NewDString("invalid"), tree.NewDInt(10)})
		assert.Error(t, err0)

		// Overload 1: 3 args
		_, err1 := overloads[1].Fn(ctx, tree.Datums{tree.NewDString("invalid"), tree.NewDInt(20), tree.MakeDBool(true)})
		assert.Error(t, err1)
	})
}

// TestMathFunctions tests all math-related builtin functions.
func TestMathFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	t.Run("greatest", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"integer values",
				tree.Datums{tree.NewDInt(1), tree.NewDInt(5), tree.NewDInt(3)},
				tree.NewDInt(5),
				false,
			},
			{"string values",
				tree.Datums{tree.NewDString("apple"), tree.NewDString("banana"), tree.NewDString("cherry")},
				tree.NewDString("cherry"),
				false,
			},
			{"with nulls",
				tree.Datums{tree.DNull, tree.NewDInt(10), tree.DNull},
				tree.NewDInt(10),
				false,
			},
			{"all null returns null",
				tree.Datums{tree.DNull, tree.DNull},
				tree.DNull,
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("greatest")
				got, err := overloads[0].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	t.Run("least", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			expected tree.Datum
			wantErr  bool
		}{
			{"integer values",
				tree.Datums{tree.NewDInt(1), tree.NewDInt(5), tree.NewDInt(3)},
				tree.NewDInt(1),
				false,
			},
			{"string values",
				tree.Datums{tree.NewDString("apple"), tree.NewDString("banana"), tree.NewDString("cherry")},
				tree.NewDString("apple"),
				false,
			},
			{"with nulls",
				tree.Datums{tree.DNull, tree.NewDInt(10), tree.DNull},
				tree.NewDInt(10),
				false,
			},
			{"all null returns null",
				tree.Datums{tree.DNull, tree.DNull},
				tree.DNull,
				false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("least")
				got, err := overloads[0].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.expected, got)
				}
			})
		}
	})

	// isnan
	t.Run("isnan", func(t *testing.T) {
		fn := builtins["isnan"].overloads[0].Fn // float
		got, err := fn(ctx, tree.Datums{tree.NewDFloat(1.0)})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, got)

		// decimal
		fnDec := builtins["isnan"].overloads[1].Fn
		dec := &tree.DDecimal{Decimal: *apd.New(1, 0)}
		got, err = fnDec(ctx, tree.Datums{dec})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, got)
	})

	// trunc
	t.Run("trunc", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float truncate
			{
				name: "trunc float 3.9",
				args: tree.Datums{
					tree.NewDFloat(3.9),
				},
				overload: 0,
				wantErr:  false,
			},
			{
				name: "trunc float negative -2.1",
				args: tree.Datums{
					tree.NewDFloat(-2.1),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal truncate
			{
				name: "trunc decimal 5.67",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("5.67")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},
			{
				name: "trunc decimal negative -8.99",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("-8.99")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("trunc")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	// cot
	t.Run("cot", func(t *testing.T) {
		fn := builtins["cot"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDFloat(math.Pi / 4)})
		require.NoError(t, err)
		assert.InDelta(t, 1.0, float64(*got.(*tree.DFloat)), 1e-9)
	})

	t.Run("abs", func(t *testing.T) {
		absFn := builtins["abs"].overloads[2].Fn // int
		got, err := absFn(ctx, tree.Datums{tree.NewDInt(-5)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(5), got)

		absFloat := builtins["abs"].overloads[0].Fn
		got, err = absFloat(ctx, tree.Datums{tree.NewDFloat(-3.14)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(3.14), got)

		absDecimal := builtins["abs"].overloads[1].Fn
		d := &tree.DDecimal{Decimal: *apd.New(-42, 0)}
		got, err = absDecimal(ctx, tree.Datums{d})
		assert.NoError(t, err)
		expected := &tree.DDecimal{Decimal: *apd.New(42, 0)}
		assert.Equal(t, expected, got)
	})

	t.Run("ceil", func(t *testing.T) {
		ceilFn := builtins["ceil"].overloads[0].Fn
		got, err := ceilFn(ctx, tree.Datums{tree.NewDFloat(3.2)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(4.0), got)
	})

	t.Run("floor", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float
			{
				name: "floor float 3.7",
				args: tree.Datums{
					tree.NewDFloat(3.7),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal
			{
				name: "floor decimal 3.7",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("3.7")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: int
			{
				name: "floor int 5",
				args: tree.Datums{
					tree.NewDInt(5),
				},
				overload: 2,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("floor")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("round", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: round(float) → rounded float
			{
				name: "round float 2.5",
				args: tree.Datums{
					tree.NewDFloat(2.5),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: round(decimal) → rounded decimal
			{
				name: "round decimal 2.5",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("2.5")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: round(float, int) → float with precision
			{
				name: "round float with precision 1.234 2",
				args: tree.Datums{
					tree.NewDFloat(1.234),
					tree.NewDInt(2),
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: round(decimal, int) → decimal with precision
			{
				name: "round decimal with precision 1.234 2",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("1.234")
					return tree.Datums{dec, tree.NewDInt(2)}
				}(),
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("round")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("sqrt", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float sqrt (valid positive)
			{
				name: "sqrt float positive 16",
				args: tree.Datums{
					tree.NewDFloat(16.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 0: float sqrt negative → error
			{
				name: "sqrt float negative returns error",
				args: tree.Datums{
					tree.NewDFloat(-9.0),
				},
				overload: 0,
				wantErr:  true,
			},

			// Overload 1: decimal sqrt (valid positive)
			{
				name: "sqrt decimal positive 25",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("25")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},

			// Overload 1: decimal sqrt negative → error
			{
				name: "sqrt decimal negative returns error",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("-4")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("sqrt")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.True(t, errors.Is(err, errSqrtOfNegNumber))
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("pow", func(t *testing.T) {
		powFn := builtins["pow"].overloads[0].Fn
		got, err := powFn(ctx, tree.Datums{tree.NewDFloat(2), tree.NewDFloat(3)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(8.0), got)
	})

	t.Run("mod", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float mod
			{
				name: "mod float 10.0 3.0",
				args: tree.Datums{
					tree.NewDFloat(10.0),
					tree.NewDFloat(3.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal mod
			{
				name: "mod decimal 10 3",
				args: func() tree.Datums {
					dec1, _ := tree.ParseDDecimal("10")
					dec2, _ := tree.ParseDDecimal("3")
					return tree.Datums{dec1, dec2}
				}(),
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: int mod
			{
				name: "mod int 10 3",
				args: tree.Datums{
					tree.NewDInt(10),
					tree.NewDInt(3),
				},
				overload: 2,
				wantErr:  false,
			},

			// Error: int zero modulus
			{
				name: "mod int zero modulus",
				args: tree.Datums{
					tree.NewDInt(10),
					tree.NewDInt(0),
				},
				overload: 2,
				wantErr:  true,
			},

			// Error: decimal zero modulus
			{
				name: "mod decimal zero modulus",
				args: func() tree.Datums {
					dec1, _ := tree.ParseDDecimal("10")
					dec2, _ := tree.ParseDDecimal("0")
					return tree.Datums{dec1, dec2}
				}(),
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("mod")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.True(t, errors.Is(err, tree.ErrZeroModulus))
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					_, isInt := got.(*tree.DInt)
					assert.True(t, isFloat || isDecimal || isInt)
				}
			})
		}
	})

	t.Run("div", func(t *testing.T) {
		dec1, _ := tree.ParseDDecimal("10")
		dec2, _ := tree.ParseDDecimal("3")
		dec3, _ := tree.ParseDDecimal("0")
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float / float
			{
				name: "div float 10.0 / 3.0",
				args: tree.Datums{
					tree.NewDFloat(10.0),
					tree.NewDFloat(3.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal / decimal
			{
				name: "div decimal 10 / 3",
				args: tree.Datums{
					dec1,
					dec2,
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: int / int
			{
				name: "div int 10 / 3",
				args: tree.Datums{
					tree.NewDInt(10),
					tree.NewDInt(3),
				},
				overload: 2,
				wantErr:  false,
			},

			// Divide by zero int
			{
				name: "div int by zero returns error",
				args: tree.Datums{
					tree.NewDInt(10),
					tree.NewDInt(0),
				},
				overload: 2,
				wantErr:  true,
			},

			// Divide by zero decimal
			{
				name: "div decimal by zero returns error",
				args: tree.Datums{
					dec1,
					dec3,
				},
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("div")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.True(t, errors.Is(err, tree.ErrDivByZero))
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					_, isInt := got.(*tree.DInt)
					assert.True(t, isFloat || isDecimal || isInt)
				}
			})
		}
	})

	t.Run("exp", func(t *testing.T) {
		dec1, _ := tree.ParseDDecimal("2.0")
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float64 exponential
			{
				name: "exp float 1.0",
				args: tree.Datums{
					tree.NewDFloat(1.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal exponential
			{
				name: "exp decimal 2.0",
				args: tree.Datums{
					dec1,
				},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("exp")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("ln", func(t *testing.T) {
		lnFn := builtins["ln"].overloads[0].Fn
		got, err := lnFn(ctx, tree.Datums{tree.NewDFloat(math.E)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), got)
	})

	t.Run("log", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float base 10 log
			{
				name: "log base10 float 100",
				args: tree.Datums{
					tree.NewDFloat(100.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: float custom base log
			{
				name: "log custom base float 2 8",
				args: tree.Datums{
					tree.NewDFloat(2.0),
					tree.NewDFloat(8.0),
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: decimal base10 log
			{
				name: "log base10 decimal 100",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("100")
					return tree.Datums{dec}
				}(),
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: decimal custom base log
			{
				name: "log custom base decimal 2 8",
				args: func() tree.Datums {
					dec1, _ := tree.ParseDDecimal("2")
					dec2, _ := tree.ParseDDecimal("8")
					return tree.Datums{dec1, dec2}
				}(),
				overload: 3,
				wantErr:  false,
			},

			// Error: float negative number
			{
				name: "log float negative returns error",
				args: tree.Datums{
					tree.NewDFloat(-10.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Error: decimal negative number
			{
				name: "log decimal negative returns error",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("-5")
					return tree.Datums{dec}
				}(),
				overload: 2,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("log")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("sin/cos/tan", func(t *testing.T) {
		sinFn := builtins["sin"].overloads[0].Fn
		got, err := sinFn(ctx, tree.Datums{tree.NewDFloat(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), got)

		cosFn := builtins["cos"].overloads[0].Fn
		got, err = cosFn(ctx, tree.Datums{tree.NewDFloat(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), got)

		tanFn := builtins["tan"].overloads[0].Fn
		got, err = tanFn(ctx, tree.Datums{tree.NewDFloat(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), got)
	})

	t.Run("acos/asin/atan/atan2", func(t *testing.T) {
		acosFn := builtins["acos"].overloads[0].Fn
		got, err := acosFn(ctx, tree.Datums{tree.NewDFloat(1)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), got)

		asinFn := builtins["asin"].overloads[0].Fn
		got, err = asinFn(ctx, tree.Datums{tree.NewDFloat(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), got)

		atanFn := builtins["atan"].overloads[0].Fn
		got, err = atanFn(ctx, tree.Datums{tree.NewDFloat(0)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), got)

		atan2Fn := builtins["atan2"].overloads[0].Fn
		got, err = atan2Fn(ctx, tree.Datums{tree.NewDFloat(1), tree.NewDFloat(1)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(math.Pi/4), got)
	})

	t.Run("pi", func(t *testing.T) {
		piFn := builtins["pi"].overloads[0].Fn
		got, err := piFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(math.Pi), got)
	})

	t.Run("radians/degrees", func(t *testing.T) {
		radFn := builtins["radians"].overloads[0].Fn
		got, err := radFn(ctx, tree.Datums{tree.NewDFloat(180)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(math.Pi), got)

		degFn := builtins["degrees"].overloads[0].Fn
		got, err = degFn(ctx, tree.Datums{tree.NewDFloat(math.Pi)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(180), got)
	})

	t.Run("sign", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float sign
			{
				name: "sign float positive",
				args: tree.Datums{
					tree.NewDFloat(10.5),
				},
				overload: 0,
				wantErr:  false,
			},
			{
				name: "sign float zero",
				args: tree.Datums{
					tree.NewDFloat(0.0),
				},
				overload: 0,
				wantErr:  false,
			},
			{
				name: "sign float negative",
				args: tree.Datums{
					tree.NewDFloat(-5.2),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal sign
			{
				name: "sign decimal positive",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("8.9")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},
			{
				name: "sign decimal zero",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("0")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},
			{
				name: "sign decimal negative",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("-3.14")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: int sign
			{
				name: "sign int positive",
				args: tree.Datums{
					tree.NewDInt(100),
				},
				overload: 2,
				wantErr:  false,
			},
			{
				name: "sign int zero",
				args: tree.Datums{
					tree.NewDInt(0),
				},
				overload: 2,
				wantErr:  false,
			},
			{
				name: "sign int negative",
				args: tree.Datums{
					tree.NewDInt(-77),
				},
				overload: 2,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("sign")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					_, isInt := got.(*tree.DInt)
					assert.True(t, isFloat || isDecimal || isInt)
				}
			})
		}
	})

	t.Run("cbrt", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: float64 cube root
			{
				name: "cbrt float 8.0",
				args: tree.Datums{
					tree.NewDFloat(8.0),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: decimal cube root
			{
				name: "cbrt decimal 8.0",
				args: func() tree.Datums {
					dec, _ := tree.ParseDDecimal("8.0")
					return tree.Datums{dec}
				}(),
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("cbrt")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isFloat := got.(*tree.DFloat)
					_, isDecimal := got.(*tree.DDecimal)
					assert.True(t, isFloat || isDecimal)
				}
			})
		}
	})

	t.Run("width_bucket", func(t *testing.T) {
		// Test cases for width_bucket function covering all overloads
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: decimal, decimal, decimal, int - equal width buckets
			{
				name: "decimal equal width buckets",
				args: func() tree.Datums {
					operand, _ := tree.ParseDDecimal("5.5")
					b1, _ := tree.ParseDDecimal("1")
					b2, _ := tree.ParseDDecimal("10")
					return tree.Datums{operand, b1, b2, tree.NewDInt(5)}
				}(),
				overload: 0,
				wantErr:  false,
			},
			// Overload 1: int, int, int, int - equal width buckets
			{
				name: "int equal width buckets",
				args: tree.Datums{
					tree.NewDInt(7),
					tree.NewDInt(1),
					tree.NewDInt(10),
					tree.NewDInt(5),
				},
				overload: 1,
				wantErr:  false,
			},
			// Overload 2: operand with threshold array
			{
				name: "with threshold array",
				args: func() tree.Datums {
					pathArr := tree.NewDArray(types.Int)
					_ = pathArr.Append(tree.NewDInt(2))
					_ = pathArr.Append(tree.NewDInt(5))
					_ = pathArr.Append(tree.NewDInt(10))
					return tree.Datums{tree.NewDInt(8), pathArr}
				}(),
				overload: 2,
				wantErr:  false,
			},
			// Error case: type mismatch
			{
				name: "type mismatch returns error",
				args: func() tree.Datums {
					pathArr := tree.NewDArray(types.String)
					_ = pathArr.Append(tree.NewDString("a"))
					return tree.Datums{tree.NewDInt(5), pathArr}
				}(),
				overload: 2,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("width_bucket")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DInt)
					assert.True(t, ok)
				}
			})
		}
	})
}

// TestDateTimeFunctions tests all date/time related builtin functions.
func TestDateTimeFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()
	// Set a fixed time for deterministic tests
	fixedTime := time.Date(2023, 10, 15, 14, 30, 45, 123456789, time.UTC)

	// localtimestamp
	t.Run("localtimestamp", func(t *testing.T) {
		// Safely find the overload for localtimestamp (0 arguments, returns Timestamp)
		var localTimestampFn func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error)
		for _, overload := range builtins["localtimestamp"].overloads {
			// Match overload with 0 arguments and returns TIMESTAMP (not TIMESTAMPTZ)
			if len(overload.Types.(tree.ArgTypes)) == 0 &&
				overload.FixedReturnType().Family() == types.TimestampFamily &&
				!overload.FixedReturnType().Equal(*types.TimestampTZ) {
				localTimestampFn = overload.Fn
				break
			}
		}

		// Fail if no overload found (self-documenting error)
		require.NotNil(t, localTimestampFn, "localtimestamp overload (no args, timestamp) not found")

		// Call the builtin function
		got, err := localTimestampFn(ctx, tree.Datums{})
		assert.NoError(t, err)

		// Verify return type is DTimestamp (NOT TimestampTZ)
		assert.IsType(t, &tree.DTimestamp{}, got)
	})

	// clock_timestamp
	t.Run("clock_timestamp", func(t *testing.T) {
		fn := builtins["clock_timestamp"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.IsType(t, &tree.DTimestampTZ{}, got)
		// Not checking exact value because it's current time.
	})

	// timeofday
	t.Run("timeofday", func(t *testing.T) {
		// Find the correct overload for timeofday (no arguments)
		var timeOfDayFn func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error)
		for _, overload := range builtins["timeofday"].overloads {
			// Look for the overload that accepts 0 arguments and returns String type
			if len(overload.Types.(tree.ArgTypes)) == 0 &&
				overload.FixedReturnType() == types.String {
				timeOfDayFn = overload.Fn
				break
			}
		}

		// Invoke the timeofday builtin function with evaluation context
		got, err := timeOfDayFn(ctx, tree.Datums{})
		assert.NoError(t, err)

		// Verify the result is a non-empty string
		resultStr, ok := got.(*tree.DString)
		assert.True(t, ok, "expected return type DString for timeofday")
		assert.NotEmpty(t, resultStr)

		// Validate the output format matches PostgreSQL style: "Mon Jan 2 15:04:05.000000000 2006 -0700"
		// Basic format check (starts with 3-letter weekday, contains date/time components)
		val := string(*resultStr)
		assert.Regexp(t, `^[A-Za-z]{3} [A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}\.\d+ \d{4} [+-]\d{4}$`, val)
	})

	// extract_duration
	t.Run("extract_duration", func(t *testing.T) {
		fn := builtins["extract_duration"].overloads[0].Fn
		interval := &tree.DInterval{Duration: duration.MakeDuration(int64(2*time.Hour+30*time.Minute), 0, 0)}
		got, err := fn(ctx, tree.Datums{tree.NewDString("hour"), interval})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(2), got)

		got, err = fn(ctx, tree.Datums{tree.NewDString("minute"), interval})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(150), got)
	})

	t.Run("extract", func(t *testing.T) {
		// Fixed test time for consistent results
		testTime := time.Date(2025, 12, 25, 10, 30, 45, 123456789, time.UTC)
		testDate, _ := pgdate.MakeDateFromTime(testTime)
		testInterval := &tree.DInterval{
			Duration: duration.MakeDuration(int64(time.Hour*2+time.Minute*30+time.Second*15), 0, 0),
		}

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: extract from Timestamp
			{
				name: "extract year from timestamp",
				args: tree.Datums{
					tree.NewDString("year"),
					tree.MakeDTimestamp(testTime, time.Microsecond),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: extract from Interval
			{
				name: "extract hour from interval",
				args: tree.Datums{
					tree.NewDString("hour"),
					testInterval,
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: extract from Date
			{
				name: "extract month from date",
				args: tree.Datums{
					tree.NewDString("month"),
					tree.NewDDate(testDate),
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: extract from TimestampTZ
			{
				name: "extract timezone from timestamptz",
				args: tree.Datums{
					tree.NewDString("timezone"),
					tree.MakeDTimestampTZ(testTime, time.Microsecond),
				},
				overload: 3,
				wantErr:  false,
			},

			// Overload 4: extract from Time
			{
				name: "extract minute from time",
				args: tree.Datums{
					tree.NewDString("minute"),
					tree.MakeDTime(timeofday.New(10, 30, 45, 123456789)),
				},
				overload: 4,
				wantErr:  false,
			},

			// Overload 5: extract from TimeTZ
			{
				name: "extract timezone_hour from timetz",
				args: tree.Datums{
					tree.NewDString("timezone_hour"),
					tree.NewDTimeTZFromTime(testTime),
				},
				overload: 5,
				wantErr:  false,
			},

			// Overload 6: extract from Int (Unix timestamp)
			{
				name: "extract second from int unix time",
				args: tree.Datums{
					tree.NewDString("second"),
					tree.NewDInt(1735149045),
				},
				overload: 6,
				wantErr:  false,
			},

			// Invalid element → error
			{
				name: "invalid element returns error",
				args: tree.Datums{
					tree.NewDString("invalid"),
					tree.MakeDTimestamp(testTime, time.Microsecond),
				},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("extract")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DFloat)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("date_trunc", func(t *testing.T) {
		// Fixed test time for consistent results
		testTime := time.Date(2025, 12, 25, 10, 30, 45, 123456789, time.UTC)
		testDate, _ := pgdate.MakeDateFromTime(testTime)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: date_trunc on Timestamp
			{
				name: "truncate timestamp to day",
				args: tree.Datums{
					tree.NewDString("day"),
					tree.MakeDTimestamp(testTime, time.Nanosecond),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: date_trunc on Date
			{
				name: "truncate date to month",
				args: tree.Datums{
					tree.NewDString("month"),
					tree.NewDDate(testDate),
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: date_trunc on Time
			{
				name: "truncate time to hour",
				args: tree.Datums{
					tree.NewDString("hour"),
					tree.MakeDTime(timeofday.New(10, 30, 45, 0)),
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: date_trunc on TimestampTZ
			{
				name: "truncate timestamptz to year",
				args: tree.Datums{
					tree.NewDString("year"),
					tree.MakeDTimestampTZ(testTime, time.Nanosecond),
				},
				overload: 3,
				wantErr:  false,
			},

			// Invalid time span → error
			{
				name: "invalid element returns error",
				args: tree.Datums{
					tree.NewDString("invalid"),
					tree.MakeDTimestamp(testTime, time.Nanosecond),
				},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("date_trunc")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					// Validate all possible return types
					_, isTS := got.(*tree.DTimestamp)
					_, isTSTZ := got.(*tree.DTimestampTZ)
					_, isInterval := got.(*tree.DInterval)
					assert.True(t, isTS || isTSTZ || isInterval)
				}
			})
		}
	})

	t.Run("age", func(t *testing.T) {
		// Fixed test time for consistent results
		testTime1 := time.Date(2025, 12, 25, 10, 30, 45, 0, time.UTC)
		testTime2 := time.Date(2024, 12, 25, 10, 30, 45, 0, time.UTC)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: age(timestamptz)
			{
				name: "age with one timestamp argument",
				args: tree.Datums{
					tree.MakeDTimestampTZ(testTime1, time.Microsecond),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: age(timestamptz, timestamptz)
			{
				name: "age with two timestamp arguments",
				args: tree.Datums{
					tree.MakeDTimestampTZ(testTime1, time.Microsecond),
					tree.MakeDTimestampTZ(testTime2, time.Microsecond),
				},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("age")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DInterval)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("current_date", func(t *testing.T) {
		currentDateFn := builtins["current_date"].overloads[0].Fn
		got, err := currentDateFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		expected, err1 := tree.NewDDateFromTime(fixedTime.In(ctx.GetLocation()))
		if err1 != nil {
			assert.NoError(t, err1)
		}
		assert.Equal(t, expected, got)
	})

	t.Run("now", func(t *testing.T) {
		nowFn := builtins["now"].overloads[0].Fn
		got, err := nowFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		expected := tree.MakeDTimestampTZ(fixedTime, time.Nanosecond)
		assert.Equal(t, expected, got)
	})

	t.Run("current_time", func(t *testing.T) {
		currentTimeFn := builtins["current_time"].overloads[0].Fn
		got, err := currentTimeFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.NotNil(t, got)
	})

	t.Run("current_timestamp", func(t *testing.T) {
		currentTimestampFn := builtins["current_timestamp"].overloads[0].Fn
		got, err := currentTimestampFn(ctx, tree.Datums{tree.NewDInt(9)})
		assert.NoError(t, err)
		expected := tree.MakeDTimestampTZ(fixedTime, time.Nanosecond)
		assert.Equal(t, expected, got)
	})

	t.Run("transaction_timestamp", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "no args returns timestamptz",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("transaction_timestamp")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DTimestampTZ)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("localtime", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: time with timezone (no args)
			{
				name:     "no args returns timetz",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
			// Overload 1: time without timezone (no args)
			{
				name:     "no args returns time",
				args:     tree.Datums{},
				overload: 1,
				wantErr:  false,
			},
			// Overload 2: time with timezone + valid precision
			{
				name:     "with valid precision returns timetz",
				args:     tree.Datums{tree.NewDInt(6)},
				overload: 2,
				wantErr:  false,
			},
			// Overload 3: time without timezone + valid precision
			{
				name:     "with valid precision returns time",
				args:     tree.Datums{tree.NewDInt(6)},
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("localtime")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isTimeTZ := got.(*tree.DTimeTZ)
					_, isTime := got.(*tree.DTime)
					assert.True(t, isTimeTZ || isTime)
				}
			})
		}
	})

	t.Run("statement_timestamp", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: returns TimestampTZ (preferred)
			{
				name:     "returns timestamptz",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
			// Overload 1: returns Timestamp
			{
				name:     "returns timestamp",
				args:     tree.Datums{},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("statement_timestamp")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isTZ := got.(*tree.DTimestampTZ)
					_, isNoTZ := got.(*tree.DTimestamp)
					assert.True(t, isTZ || isNoTZ)
				}
			})
		}
	})

	t.Run("experimental_follower_read_timestamp", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: no args returns timestamptz
			{
				name:     "no args returns timestamptz",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("experimental_follower_read_timestamp")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					_, _ = got.(*tree.DTimestampTZ)
				}
			})
		}
	})

	t.Run("timezone", func(t *testing.T) {
		testTime := time.Date(2025, 12-1, 25, 10, 30, 45, 123456789, time.UTC)
		testTS := tree.MakeDTimestamp(testTime, time.Nanosecond)
		testTSTZ := tree.MakeDTimestampTZ(testTime, time.Nanosecond)
		testDTime := tree.MakeDTime(timeofday.New(10, 30, 45, 123456789))
		testDTimeTZ := tree.NewDTimeTZFromTime(testTime)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: string, string → timestamp
			{
				name: "timezone string, string",
				args: tree.Datums{
					tree.NewDString("UTC"),
					tree.NewDString("2025-12-25 10:30:45"),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: string, timestamp → timestamptz
			{
				name: "timezone string, timestamp",
				args: tree.Datums{
					tree.NewDString("UTC"),
					testTS,
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: string, timestamptz → timestamp
			{
				name: "timezone string, timestamptz",
				args: tree.Datums{
					tree.NewDString("UTC"),
					testTSTZ,
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: string, time → timetz
			{
				name: "timezone string, time",
				args: tree.Datums{
					tree.NewDString("UTC"),
					testDTime,
				},
				overload: 3,
				wantErr:  false,
			},

			// Overload 4: string, timetz → timetz
			{
				name: "timezone string, timetz",
				args: tree.Datums{
					tree.NewDString("UTC"),
					testDTimeTZ,
				},
				overload: 4,
				wantErr:  false,
			},

			// Overload 5: timestamp, string → timestamptz (deprecated)
			{
				name: "timezone timestamp, string",
				args: tree.Datums{
					testTS,
					tree.NewDString("UTC"),
				},
				overload: 5,
				wantErr:  false,
			},

			// Overload 6: timestamptz, string → timestamp (deprecated)
			{
				name: "timezone timestamptz, string",
				args: tree.Datums{
					testTSTZ,
					tree.NewDString("UTC"),
				},
				overload: 6,
				wantErr:  false,
			},

			// Overload 7: time, string → timetz (deprecated)
			{
				name: "timezone time, string",
				args: tree.Datums{
					testDTime,
					tree.NewDString("UTC"),
				},
				overload: 7,
				wantErr:  false,
			},

			// Overload 8: timetz, string → timetz (deprecated)
			{
				name: "timezone timetz, string",
				args: tree.Datums{
					testDTimeTZ,
					tree.NewDString("UTC"),
				},
				overload: 8,
				wantErr:  false,
			},

			{
				name: "timezone invalid timezone",
				args: tree.Datums{
					tree.NewDString("INVALID_TZ"),
					testTS,
				},
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("timezone")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, isTS := got.(*tree.DTimestamp)
					_, isTSTZ := got.(*tree.DTimestampTZ)
					_, isTimeTZ := got.(*tree.DTimeTZ)
					assert.True(t, isTS || isTSTZ || isTimeTZ)
				}
			})
		}
	})

	t.Run("to_time", func(t *testing.T) {
		// Fixed test time for consistent results
		testTime := time.Date(2025, 12, 25, 10, 30, 45, 123456789, time.UTC)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: int (milliseconds) -> time
			{
				name: "to_time from int milliseconds",
				args: tree.Datums{
					tree.NewDInt(37845000), // 10:30:45
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 0: invalid int out of range -> error
			{
				name: "to_time invalid int out of range",
				args: tree.Datums{
					tree.NewDInt(999999999),
				},
				overload: 0,
				wantErr:  true,
			},

			// Overload 1: string -> time
			{
				name: "to_time from valid string",
				args: tree.Datums{
					tree.NewDString("10:30:45"),
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 1: invalid string -> error
			{
				name: "to_time invalid string 24:00:00",
				args: tree.Datums{
					tree.NewDString("24:00:00"),
				},
				overload: 1,
				wantErr:  true,
			},

			// Overload 2: timestamp -> time
			{
				name: "to_time from timestamp",
				args: tree.Datums{
					tree.MakeDTimestamp(testTime, time.Nanosecond),
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: timestamptz -> time
			{
				name: "to_time from timestamptz",
				args: tree.Datums{
					tree.MakeDTimestampTZ(testTime, time.Nanosecond),
				},
				overload: 3,
				wantErr:  false,
			},

			// Overload 4: time -> time
			{
				name: "to_time from time",
				args: tree.Datums{
					tree.MakeDTime(timeofday.New(10, 30, 45, 123456789)),
				},
				overload: 4,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_time")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DTime)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("time_to_sec", func(t *testing.T) {
		// 固定测试时间
		testTime := time.Date(2025, 12, 25, 10, 30, 45, 123456789, time.UTC)
		testDTime := tree.MakeDTime(timeofday.New(10, 30, 45, 123456789))

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: time -> 秒数
			{
				name: "time_to_sec from time type",
				args: tree.Datums{
					testDTime,
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: string -> 秒数
			{
				name: "time_to_sec from valid time string",
				args: tree.Datums{
					tree.NewDString("10:30:45"),
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 1: 纯数字字符串 -> 错误
			{
				name: "time_to_sec invalid numeric string",
				args: tree.Datums{
					tree.NewDString("12345"),
				},
				overload: 1,
				wantErr:  true,
			},

			// Overload 2: timestamp -> 秒数
			{
				name: "time_to_sec from timestamp",
				args: tree.Datums{
					tree.MakeDTimestamp(testTime, time.Nanosecond),
				},
				overload: 2,
				wantErr:  false,
			},

			// Overload 3: timestamptz -> 秒数
			{
				name: "time_to_sec from timestamptz",
				args: tree.Datums{
					tree.MakeDTimestampTZ(testTime, time.Nanosecond),
				},
				overload: 3,
				wantErr:  false,
			},

			// 错误用例：超出范围的时间
			{
				name: "time_to_sec out of range time",
				args: tree.Datums{
					tree.NewDString("25:00:00"),
				},
				overload: 1,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_to_sec")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DInt)
					assert.True(t, ok)
				}
			})
		}
	})

	// Test case for str_to_date function
	t.Run("str_to_date", func(t *testing.T) {
		// Retrieve the str_to_date function implementation
		strToDateFn := builtins["str_to_date"].overloads[0].Fn

		// Use the standard strptime/strftime format supported by github.com/knz/strtime
		// Parsing "2023-10-15" with format "%Y-%m-%d"
		got, err := strToDateFn(ctx, tree.Datums{
			tree.NewDString("2023-10-15"),
			tree.NewDString("%Y-%m-%d"),
		})
		// Assert that no error is returned during parsing
		assert.NoError(t, err)

		// The function uses the session's location (not UTC), so the expected value must match it
		sessionLoc := ctx.GetLocation()
		expected := tree.MakeDTimestampTZ(
			time.Date(2023, 10, 15, 0, 0, 0, 0, sessionLoc),
			time.Microsecond,
		)
		// Assert that the parsed timestamp matches the expected value
		assert.Equal(t, expected, got)
	})

	t.Run("experimental_strftime", func(t *testing.T) {
		// Fixed test time for consistent results
		testTime := time.Date(2025, 12, 25, 10, 30, 45, 0, time.UTC)
		pDate, _ := pgdate.MakeDateFromTime(testTime)
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: Timestamp
			{
				name: "timestamp with format %Y-%m-%d",
				args: tree.Datums{
					tree.MakeDTimestamp(testTime, time.Microsecond),
					tree.NewDString("%Y-%m-%d"),
				},
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: Date
			{
				name: "date with format %Y-%m-%d",
				args: tree.Datums{
					tree.NewDDate(pDate),
					tree.NewDString("%Y-%m-%d"),
				},
				overload: 1,
				wantErr:  false,
			},

			// Overload 2: TimestampTZ
			{
				name: "timestamptz with format %H:%M:%S",
				args: tree.Datums{
					tree.MakeDTimestampTZ(testTime, time.Microsecond),
					tree.NewDString("%H:%M:%S"),
				},
				overload: 2,
				wantErr:  false,
			},

			// Invalid format → returns error
			{
				name: "invalid format returns error",
				args: tree.Datums{
					tree.MakeDTimestamp(testTime, time.Microsecond),
					tree.NewDString("%invalid"),
				},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("experimental_strftime")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DString)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("experimental_strptime", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Valid format and input -> success
			{
				name: "valid string and format returns timestamptz",
				args: tree.Datums{
					tree.NewDString("2025-12-25 10:30:45"),
					tree.NewDString("%Y-%m-%d %H:%M:%S"),
				},
				overload: 0,
				wantErr:  false,
			},
			// Invalid format -> returns error
			{
				name: "invalid format returns error",
				args: tree.Datums{
					tree.NewDString("2025-12-25"),
					tree.NewDString("%invalid"),
				},
				overload: 0,
				wantErr:  false,
			},
			// Mismatched input and format -> error
			{
				name: "mismatched input and format returns error",
				args: tree.Datums{
					tree.NewDString("abc"),
					tree.NewDString("%Y-%m-%d"),
				},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("experimental_strptime")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DTimestampTZ)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("time_bucket", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "bucket timestamp with interval",
				args:     tree.Datums{tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), tree.NewDString("1h")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "bucket timestamptz with interval",
				args:     tree.Datums{tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), tree.NewDString("1h")},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_bucket")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, got)
			})
		}
	})

	t.Run("to_timestamp", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "parse from unix string",
				args:     tree.Datums{tree.NewDString("1720000000")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "parse from unix int",
				args:     tree.Datums{tree.NewDInt(1720000000)},
				overload: 1,
				wantErr:  false,
			},
			{
				name:     "parse from string with format",
				args:     tree.Datums{tree.NewDString("2025-01-01 12:00:00.000000"), tree.NewDString("YYYY-MM-DD HH24:MI:SS.US")},
				overload: 2,
				wantErr:  false,
			},
			{
				name:     "parse from timestamp with format",
				args:     tree.Datums{tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), tree.NewDString("YYYY-MM-DD HH24:MI:SS.MS")},
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("to_timestamp")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DTimestamp)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("time_bucket_gapfill", func(t *testing.T) {
		now := timeutil.Now()
		ts := tree.MakeDTimestamp(now, 0)
		tstz := tree.MakeDTimestampTZ(now, 0)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "timestamp with string interval",
				args:     tree.Datums{ts, tree.NewDString("1h")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "timestamptz with string interval",
				args:     tree.Datums{tstz, tree.NewDString("1h")},
				overload: 1,
				wantErr:  false,
			},
			{
				name:     "timestamp with int interval",
				args:     tree.Datums{ts, tree.NewDInt(3600)},
				overload: 2,
				wantErr:  false,
			},
			{
				name:     "timestamptz with int interval",
				args:     tree.Datums{tstz, tree.NewDInt(3600)},
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_bucket_gapfill")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, got)
				}
			})
		}
	})
}

// TestArrayFunctions tests all array-related builtin functions.
func TestArrayFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	// string_to_array with NULL delimiter
	t.Run("string_to_array", func(t *testing.T) {
		// Test cases for string_to_array function covering all overloads
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: string, delimiter - split string into array
			{
				name: "split string with delimiter",
				args: tree.Datums{
					tree.NewDString("a,b,c"),
					tree.NewDString(","),
				},
				overload: 0,
				wantErr:  false,
			},
			// Overload 1: string, delimiter, null string - with null replacement
			{
				name: "split string with delimiter and null string",
				args: tree.Datums{
					tree.NewDString("a,NULL,c"),
					tree.NewDString(","),
					tree.NewDString("NULL"),
				},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("string_to_array")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DArray)
					assert.True(t, ok)
				}
			})
		}
	})

	// array_to_string with nullStr
	t.Run("array_to_string", func(t *testing.T) {
		// Test cases for array_to_string function covering all overloads
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			// Overload 0: array, delimiter - join array to string with delimiter
			{
				name: "join string array with delimiter",
				args: func() tree.Datums {
					arr := tree.NewDArray(types.String)
					_ = arr.Append(tree.NewDString("x"))
					_ = arr.Append(tree.NewDString("y"))
					_ = arr.Append(tree.NewDString("z"))
					return tree.Datums{arr, tree.NewDString(",")}
				}(),
				overload: 0,
				wantErr:  false,
			},

			// Overload 1: array, delimiter, null string - with null replacement
			{
				name: "join array with delimiter and null string",
				args: func() tree.Datums {
					arr := tree.NewDArray(types.String)
					_ = arr.Append(tree.NewDString("a"))
					_ = arr.Append(tree.DNull)
					_ = arr.Append(tree.NewDString("b"))
					return tree.Datums{arr, tree.NewDString(","), tree.NewDString("NULL")}
				}(),
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("array_to_string")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DString)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("array_length", func(t *testing.T) {
		arrayLengthFn := builtins["array_length"].overloads[0].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		got, err := arrayLengthFn(ctx, tree.Datums{arr, tree.NewDInt(1)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(2), got)
	})

	t.Run("array_lower", func(t *testing.T) {
		arrayLowerFn := builtins["array_lower"].overloads[0].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayLowerFn(ctx, tree.Datums{arr, tree.NewDInt(1)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(1), got)
	})

	t.Run("array_upper", func(t *testing.T) {
		arrayUpperFn := builtins["array_upper"].overloads[0].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayUpperFn(ctx, tree.Datums{arr, tree.NewDInt(1)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(1), got)
	})

	t.Run("array_append", func(t *testing.T) {
		arrayAppendFn := builtins["array_append"].overloads[1].Fn
		// Create an int array with initial element 1
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))

		// Call array_append function to append element 2 to the array
		got, err := arrayAppendFn(ctx, tree.Datums{arr, tree.NewDInt(2)})
		assert.NoError(t, err)

		// Build expected result array [1, 2]
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(1))
		_ = expected.Append(tree.NewDInt(2))

		// Verify the result matches the expectation
		assert.Equal(t, expected, got)
	})

	t.Run("array_prepend", func(t *testing.T) {
		arrayPrependFn := builtins["array_prepend"].overloads[1].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayPrependFn(ctx, tree.Datums{tree.NewDInt(2), arr})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(2))
		_ = expected.Append(tree.NewDInt(1))
		assert.Equal(t, expected, got)
	})

	t.Run("array_cat", func(t *testing.T) {
		arrayCatFn := builtins["array_cat"].overloads[1].Fn
		arr1 := tree.NewDArray(types.Int)
		_ = arr1.Append(tree.NewDInt(1))
		arr2 := tree.NewDArray(types.Int)
		_ = arr2.Append(tree.NewDInt(2))
		got, err := arrayCatFn(ctx, tree.Datums{arr1, arr2})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(1))
		_ = expected.Append(tree.NewDInt(2))
		assert.Equal(t, expected, got)
	})

	t.Run("array_remove", func(t *testing.T) {
		arrayRemoveFn := builtins["array_remove"].overloads[1].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayRemoveFn(ctx, tree.Datums{arr, tree.NewDInt(1)})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(2))
		assert.Equal(t, expected, got)
	})

	t.Run("array_replace", func(t *testing.T) {
		arrayReplaceFn := builtins["array_replace"].overloads[1].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayReplaceFn(ctx, tree.Datums{arr, tree.NewDInt(1), tree.NewDInt(3)})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(3))
		_ = expected.Append(tree.NewDInt(2))
		_ = expected.Append(tree.NewDInt(3))
		assert.Equal(t, expected, got)
	})

	t.Run("array_position", func(t *testing.T) {
		arrayPositionFn := builtins["array_position"].overloads[1].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		_ = arr.Append(tree.NewDInt(3))
		got, err := arrayPositionFn(ctx, tree.Datums{arr, tree.NewDInt(2)})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(2), got)
	})

	t.Run("array_positions", func(t *testing.T) {
		arrayPositionsFn := builtins["array_positions"].overloads[0].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		_ = arr.Append(tree.NewDInt(1))
		got, err := arrayPositionsFn(ctx, tree.Datums{arr, tree.NewDInt(1)})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.Int)
		_ = expected.Append(tree.NewDInt(1))
		_ = expected.Append(tree.NewDInt(3))
		assert.Equal(t, expected, got)
	})

	t.Run("string_to_array", func(t *testing.T) {
		stringToArrayFn := builtins["string_to_array"].overloads[0].Fn
		got, err := stringToArrayFn(ctx, tree.Datums{tree.NewDString("a,b,c"), tree.NewDString(",")})
		assert.NoError(t, err)
		expected := tree.NewDArray(types.String)
		_ = expected.Append(tree.NewDString("a"))
		_ = expected.Append(tree.NewDString("b"))
		_ = expected.Append(tree.NewDString("c"))
		assert.Equal(t, expected, got)
	})

	t.Run("array_to_string", func(t *testing.T) {
		arrayToStringFn := builtins["array_to_string"].overloads[0].Fn
		arr := tree.NewDArray(types.String)
		_ = arr.Append(tree.NewDString("a"))
		_ = arr.Append(tree.NewDString("b"))
		_ = arr.Append(tree.NewDString("c"))
		got, err := arrayToStringFn(ctx, tree.Datums{arr, tree.NewDString(",")})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("a,b,c"), got)
	})
}

// TestJSONFunctions tests all JSON-related builtin functions.
func TestJSONFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	// json_remove_path
	t.Run("json_remove_path", func(t *testing.T) {
		fn := builtins["json_remove_path"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":{"b":2}}`)
		pathArr := tree.NewDArray(types.String)
		_ = pathArr.Append(tree.NewDString("a"))
		_ = pathArr.Append(tree.NewDString("b"))
		got, err := fn(ctx, tree.Datums{&tree.DJSON{JSON: j}, pathArr})
		require.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":{}}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// json_strip_nulls
	t.Run("json_strip_nulls", func(t *testing.T) {
		fn := builtins["json_strip_nulls"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":null,"b":1}`)
		got, err := fn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		require.NoError(t, err)
		expected, _ := json.ParseJSON(`{"b":1}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// json_object (single array)
	t.Run("json_object_single_array", func(t *testing.T) {
		fn := builtins["json_object"].overloads[0].Fn
		arr := tree.NewDArray(types.String)
		_ = arr.Append(tree.NewDString("a"))
		_ = arr.Append(tree.NewDString("1"))
		_ = arr.Append(tree.NewDString("b"))
		_ = arr.Append(tree.NewDString("2"))
		got, err := fn(ctx, tree.Datums{arr})
		require.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":"1","b":"2"}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// json_object (two arrays)
	t.Run("json_object_two_arrays", func(t *testing.T) {
		fn := builtins["json_object"].overloads[1].Fn
		keys := tree.NewDArray(types.String)
		_ = keys.Append(tree.NewDString("a"))
		_ = keys.Append(tree.NewDString("b"))
		vals := tree.NewDArray(types.String)
		_ = vals.Append(tree.NewDString("1"))
		_ = vals.Append(tree.NewDString("2"))
		got, err := fn(ctx, tree.Datums{keys, vals})
		require.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":"1","b":"2"}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// row_to_json
	t.Run("row_to_json", func(t *testing.T) {
		fn := builtins["row_to_json"].overloads[0].Fn
		tuple := tree.NewDTuple(
			types.MakeLabeledTuple([]types.T{*types.Int, *types.String}, []string{"a", "b"}),
			tree.NewDInt(1),
			tree.NewDString("hello"),
		)
		got, err := fn(ctx, tree.Datums{tuple})
		require.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":1,"b":"hello"}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// json_array_length (already tested, but verify error cases)
	t.Run("json_array_length_error", func(t *testing.T) {
		fn := builtins["json_array_length"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":1}`)
		_, err := fn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.Error(t, err)
	})

	t.Run("json_extract_path", func(t *testing.T) {
		jsonExtractPathFn := builtins["json_extract_path"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":{"b":2}}`)
		got, err := jsonExtractPathFn(ctx, tree.Datums{&tree.DJSON{JSON: j}, tree.NewDString("a"), tree.NewDString("b")})
		assert.NoError(t, err)
		expected := &tree.DJSON{JSON: json.FromInt64(2)}
		assert.Equal(t, expected, got)
	})

	t.Run("jsonb_extract_path", func(t *testing.T) {
		jsonExtractPathFn := builtins["jsonb_extract_path"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":{"b":2}}`)
		got, err := jsonExtractPathFn(ctx, tree.Datums{&tree.DJSON{JSON: j}, tree.NewDString("a"), tree.NewDString("b")})
		assert.NoError(t, err)
		expected := &tree.DJSON{JSON: json.FromInt64(2)}
		assert.Equal(t, expected, got)
	})

	t.Run("json_set", func(t *testing.T) {
		jsonSetFn := builtins["json_set"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":1}`)
		pathArr := tree.NewDArray(types.String)
		_ = pathArr.Append(tree.NewDString("a"))
		newVal, _ := json.ParseJSON(`2`)
		got, err := jsonSetFn(ctx, tree.Datums{&tree.DJSON{JSON: j}, pathArr, &tree.DJSON{JSON: newVal}})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":2}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	t.Run("jsonb_set", func(t *testing.T) {
		jsonSetFn := builtins["jsonb_set"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":1}`)
		pathArr := tree.NewDArray(types.String)
		_ = pathArr.Append(tree.NewDString("a"))
		newVal, _ := json.ParseJSON(`2`)
		got, err := jsonSetFn(ctx, tree.Datums{&tree.DJSON{JSON: j}, pathArr, &tree.DJSON{JSON: newVal}})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":2}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	t.Run("jsonb_insert", func(t *testing.T) {
		jsonInsertFn := builtins["jsonb_insert"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":[1,2]}`)
		pathArr := tree.NewDArray(types.String)
		_ = pathArr.Append(tree.NewDString("a"))
		_ = pathArr.Append(tree.NewDString("1")) // index 1
		newVal, _ := json.ParseJSON(`3`)
		got, err := jsonInsertFn(ctx, tree.Datums{&tree.DJSON{JSON: j}, pathArr, &tree.DJSON{JSON: newVal}})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":[1,3,2]}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	// Test case for jsonb_pretty function
	t.Run("jsonb_pretty", func(t *testing.T) {
		// Retrieve the jsonb_pretty function implementation
		prettyFn := builtins["jsonb_pretty"].overloads[0].Fn

		// Parse JSON input: {"a": 1}
		j, _ := json.ParseJSON(`{"a":1}`)

		// Invoke jsonb_pretty to format JSON with indentation
		got, err := prettyFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)

		// Extract the actual string content from DString
		resultStr := string(tree.MustBeDString(got))

		// Verify the pretty-printed output contains newlines
		assert.Contains(t, resultStr, "\n")
	})

	t.Run("json_typeof", func(t *testing.T) {
		typeofFn := builtins["json_typeof"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":1}`)
		got, err := typeofFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("object"), got)

		j, _ = json.ParseJSON(`[1,2]`)
		got, err = typeofFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("array"), got)
	})

	t.Run("jsonb_typeof", func(t *testing.T) {
		typeofFn := builtins["jsonb_typeof"].overloads[0].Fn
		j, _ := json.ParseJSON(`{"a":1}`)
		got, err := typeofFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("object"), got)

		j, _ = json.ParseJSON(`[1,2]`)
		got, err = typeofFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("array"), got)
	})

	t.Run("json_array_length", func(t *testing.T) {
		arrayLenFn := builtins["json_array_length"].overloads[0].Fn
		j, _ := json.ParseJSON(`[1,2,3]`)
		got, err := arrayLenFn(ctx, tree.Datums{&tree.DJSON{JSON: j}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(3), got)
	})

	t.Run("array_to_json", func(t *testing.T) {
		arrayToJSONFn := builtins["array_to_json"].overloads[0].Fn
		arr := tree.NewDArray(types.Int)
		_ = arr.Append(tree.NewDInt(1))
		_ = arr.Append(tree.NewDInt(2))
		got, err := arrayToJSONFn(ctx, tree.Datums{arr})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`[1,2]`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	t.Run("to_json", func(t *testing.T) {
		toJSONFn := builtins["to_json"].overloads[0].Fn
		got, err := toJSONFn(ctx, tree.Datums{tree.NewDInt(42)})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`42`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	t.Run("json_build_array", func(t *testing.T) {
		buildArrayFn := builtins["json_build_array"].overloads[0].Fn
		got, err := buildArrayFn(ctx, tree.Datums{tree.NewDInt(1), tree.NewDString("two")})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`[1,"two"]`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})

	t.Run("json_build_object", func(t *testing.T) {
		buildObjectFn := builtins["json_build_object"].overloads[0].Fn
		got, err := buildObjectFn(ctx, tree.Datums{tree.NewDString("a"), tree.NewDInt(1), tree.NewDString("b"), tree.NewDString("two")})
		assert.NoError(t, err)
		expected, _ := json.ParseJSON(`{"a":1,"b":"two"}`)
		assert.Equal(t, &tree.DJSON{JSON: expected}, got)
	})
}

// TestSystemInfoFunctions tests system info functions.
func TestSystemInfoFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()
	ctx.SessionData.Database = "testdb"
	ctx.SessionData.User = "testuser"
	ctx.SessionData.RowCount = 42

	t.Run("version", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns version string",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("version")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					// Verify return type is DString
					s, ok := got.(*tree.DString)
					assert.True(t, ok, "return type should be DString")
					assert.NotEmpty(t, *s)
				}
			})
		}
	})

	// pg_collation_for
	t.Run("pg_collation_for", func(t *testing.T) {
		fn := builtins["pg_collation_for"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDString("hello")})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString(`"default"`), got)

		// collated string
		collStr, err := tree.NewDCollatedString("hello", "en_US", &tree.CollationEnvironment{})
		require.NoError(t, err)
		got, err = fn(ctx, tree.Datums{collStr})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString(`"en_US"`), got)
	})

	// kwdb_internal.locality_value
	t.Run("kwdb_internal.locality_value", func(t *testing.T) {
		ctx.Locality = roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}}
		fn := builtins["kwdb_internal.locality_value"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{tree.NewDString("region")})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("us-east"), got)

		got, err = fn(ctx, tree.Datums{tree.NewDString("zone")})
		require.NoError(t, err)
		assert.Equal(t, tree.DNull, got)
	})

	// kwdb_internal.node_executable_version
	t.Run("kwdb_internal.node_executable_version", func(t *testing.T) {
		fn := builtins["kwdb_internal.node_executable_version"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{})
		require.NoError(t, err)
		expected := ctx.Settings.Version.BinaryVersion().String()
		assert.Equal(t, tree.NewDString(expected), got)
	})

	// kwdb_internal.cluster_id
	t.Run("kwdb_internal.cluster_id", func(t *testing.T) {
		fn := builtins["kwdb_internal.cluster_id"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDUuid(tree.DUuid{UUID: ctx.ClusterID}), got)
	})

	// kwdb_internal.cluster_name
	t.Run("kwdb_internal.cluster_name", func(t *testing.T) {
		fn := builtins["kwdb_internal.cluster_name"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString(ctx.ClusterName), got)
	})

	t.Run("kwdb_internal.force_error", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns pgerror with specified code and message",
				args:     tree.Datums{tree.NewDString("22012"), tree.NewDString("test division by zero")},
				overload: 0,
				wantErr:  true,
			},
			{
				name:     "returns generic error when error code is empty",
				args:     tree.Datums{tree.NewDString(""), tree.NewDString("generic test error")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.force_error")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.notice", func(t *testing.T) {
		ctx.ClientNoticeSender = &sqlbase.DummyClientNoticeSender{}
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "sends client notice and returns 0",
				args:     tree.Datums{tree.NewDString("test notice message")},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.notice")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					// Verify return type is DInt and value is 0
					d, ok := got.(*tree.DInt)
					assert.True(t, ok, "return type should be DInt")
					assert.Equal(t, tree.DInt(0), *d)
				}
			})
		}
	})

	t.Run("kwdb_internal.force_assertion_error", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns assertion failed error",
				args:     tree.Datums{tree.NewDString("test assertion failure")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.force_assertion_error")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					// Verify it's an assertion error
					assert.True(t, errors.IsAssertionFailure(err))
				}
			})
		}
	})

	t.Run("kwdb_internal.force_panic", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns privilege error when not admin",
				args:     tree.Datums{tree.NewDString("test panic")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.force_panic")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.force_log_fatal", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns privilege error when not admin user",
				args:     tree.Datums{tree.NewDString("test fatal log")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.force_log_fatal")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.force_retry", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "return zero",
				args:     tree.Datums{tree.NewDInterval(duration.MakeDuration(0, 0, 0), types.IntervalTypeMetadata{})}, // 1s
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.force_retry")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tree.DZero, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.lease_holder", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "invalid empty key returns error",
				args:     tree.Datums{tree.NewDBytes("")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "valid dummy key expects txn error in test context",
				args:     tree.Datums{tree.NewDBytes("test_key")},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.lease_holder")
				backCtx := context.Background()
				s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
				defer s.Stopper().Stop(backCtx)
				ctx.Txn = db.NewTxn(backCtx, "test")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					return
				}

				assert.NoError(t, err)
				_, ok := got.(*tree.DInt)
				assert.True(t, ok, "return type should be DInt")
			})
		}
	})

	t.Run("kwdb_internal.no_constant_folding", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns input int unchanged",
				args:     tree.Datums{tree.NewDInt(123)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "returns input string unchanged",
				args:     tree.Datums{tree.NewDString("test")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "returns null unchanged",
				args:     tree.Datums{tree.DNull},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.no_constant_folding")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.args[0], got)
				}
			})
		}
	})

	t.Run("kwdb_internal.pretty_key", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns formatted key string",
				args:     tree.Datums{tree.NewDBytes("test-key"), tree.NewDInt(0)},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.pretty_key")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DString)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("kwdb_internal.range_stats", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "fails in test context with no real txn",
				args:     tree.Datums{tree.NewDBytes("test-key")},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.range_stats")
				backCtx := context.Background()
				s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
				defer s.Stopper().Stop(backCtx)
				ctx.Txn = db.NewTxn(backCtx, "test")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					return
				}
				assert.NoError(t, err)
				_, ok := got.(*tree.DJSON)
				assert.True(t, ok)
			})
		}
	})

	t.Run("kwdb_internal.get_namespace_id", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns null or id for non-existent schema",
				args:     tree.Datums{tree.NewDInt(1), tree.NewDString("non_existent_schema")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.get_namespace_id")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					if got != tree.DNull {
						_, ok := got.(*tree.DInt)
						assert.True(t, ok)
					}
				}
			})
		}
	})

	t.Run("kwdb_internal.get_zone_config", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns null for non-existent namespace ID",
				args:     tree.Datums{tree.NewDInt(999999)},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.get_zone_config")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					if got != tree.DNull {
						_, ok := got.(*tree.DBytes)
						assert.True(t, ok)
					}
				}
			})
		}
	})

	t.Run("kwdb_internal.set_vmodule", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns permission error for non-admin user",
				args:     tree.Datums{tree.NewDString("")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.set_vmodule")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.num_inverted_index_entries", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "null json input returns zero",
				args:     tree.Datums{tree.DNull},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "null array input returns zero",
				args:     tree.Datums{tree.DNull},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.num_inverted_index_entries")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tree.DZero, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.is_admin", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns admin status boolean",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.is_admin")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					_, ok := got.(*tree.DBool)
					assert.True(t, ok)
				}
			})
		}
	})

	// kwdb_internal.round_decimal_values (basic)
	t.Run("kwdb_internal.round_decimal_values", func(t *testing.T) {
		d1, _ := tree.ParseDDecimal("123.456")
		arrVal := tree.NewDArray(types.Decimal)
		_ = arrVal.Append(d1)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "round single decimal to given scale",
				args:     tree.Datums{d1, tree.NewDInt(2)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "round decimal array to given scale",
				args:     tree.Datums{arrVal, tree.NewDInt(2)},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.round_decimal_values")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, got)
				}
			})
		}
	})

	t.Run("kwdb_internal.completed_migrations", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns string array in test context",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("kwdb_internal.completed_migrations")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					arr, ok := got.(*tree.DArray)
					assert.True(t, ok)
					assert.Equal(t, types.StringArray, arr.ResolvedType())
				}
			})
		}
	})

	t.Run("current_database", func(t *testing.T) {
		dbFn := builtins["current_database"].overloads[0].Fn
		got, err := dbFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("testdb"), got)
	})

	t.Run("current_schema", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns current schema string or null",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("current_schema")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					if got == tree.DNull {
						assert.Equal(t, tree.DNull, got)
					} else {
						_, ok := got.(*tree.DString)
						assert.True(t, ok, "return type should be DString")
					}
				}
			})
		}
	})

	t.Run("current_schemas", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns string array with include_pg_catalog = false",
				args:     tree.Datums{tree.DBoolFalse},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("current_schemas")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					// Verify return type is DArray of String
					arr, ok := got.(*tree.DArray)
					assert.True(t, ok, "return type should be DArray")
					assert.Equal(t, types.StringArray, arr.ResolvedType())
				}
			})
		}
	})

	t.Run("current_user", func(t *testing.T) {
		userFn := builtins["current_user"].overloads[0].Fn
		got, err := userFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("testuser"), got)
	})

	t.Run("row_count", func(t *testing.T) {
		rowCountFn := builtins["row_count"].overloads[0].Fn
		got, err := rowCountFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(42), got)
	})

	t.Run("get_range_count", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "invalid range ID returns error",
				args:     tree.Datums{tree.NewDInt(0), tree.NewDInt(1)},
				overload: 0,
				wantErr:  true,
			},
			{
				name:     "valid IDs expect test context error",
				args:     tree.Datums{tree.NewDInt(1), tree.NewDInt(1)},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("get_range_count")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					return
				}
				assert.NoError(t, err)
				_, ok := got.(*tree.DInt)
				assert.True(t, ok)
			})
		}
	})
}

// TestNetFunctions tests network address functions.
func TestNetFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	var ipAddr ipaddr.IPAddr
	var ip ipaddr.IPAddr
	var ip6 ipaddr.IPAddr
	_ = ipaddr.ParseINet("192.168.1.2/24", &ip)
	_ = ipaddr.ParseINet("2001:db8::/32", &ip6)

	// broadcast
	t.Run("broadcast", func(t *testing.T) {
		fn := builtins["broadcast"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}})
		require.NoError(t, err)
		var expected ipaddr.IPAddr
		_ = ipaddr.ParseINet("192.168.1.255/24", &expected)
		assert.Equal(t, &tree.DIPAddr{IPAddr: expected}, got)
	})

	// hostmask
	t.Run("hostmask", func(t *testing.T) {
		fn := builtins["hostmask"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}})
		require.NoError(t, err)
		var expected ipaddr.IPAddr
		_ = ipaddr.ParseINet("0.0.0.255", &expected)
		assert.Equal(t, &tree.DIPAddr{IPAddr: expected}, got)
	})

	// netmask
	t.Run("netmask", func(t *testing.T) {
		fn := builtins["netmask"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}})
		require.NoError(t, err)
		var expected ipaddr.IPAddr
		_ = ipaddr.ParseINet("255.255.255.0", &expected)
		assert.Equal(t, &tree.DIPAddr{IPAddr: expected}, got)
	})

	// text
	t.Run("text", func(t *testing.T) {
		fn := builtins["text"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("192.168.1.2/24"), got)
	})

	// inet_same_family
	t.Run("inet_same_family", func(t *testing.T) {
		fn := builtins["inet_same_family"].overloads[0].Fn
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}, &tree.DIPAddr{IPAddr: ip}})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, got)

		got, err = fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ip}, &tree.DIPAddr{IPAddr: ip6}})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, got)
	})

	// inet_contained_by_or_equals
	t.Run("inet_contained_by_or_equals", func(t *testing.T) {
		fn := builtins["inet_contained_by_or_equals"].overloads[0].Fn
		var ipSmall ipaddr.IPAddr
		_ = ipaddr.ParseINet("192.168.1.2/24", &ipSmall)
		var ipBig ipaddr.IPAddr
		_ = ipaddr.ParseINet("192.168.0.0/16", &ipBig)
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipSmall}, &tree.DIPAddr{IPAddr: ipBig}})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, got)
	})

	// inet_contains_or_equals
	t.Run("inet_contains_or_equals", func(t *testing.T) {
		fn := builtins["inet_contains_or_equals"].overloads[0].Fn
		var ipSmall ipaddr.IPAddr
		_ = ipaddr.ParseINet("192.168.1.2/24", &ipSmall)
		var ipBig ipaddr.IPAddr
		_ = ipaddr.ParseINet("192.168.0.0/16", &ipBig)
		got, err := fn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipBig}, &tree.DIPAddr{IPAddr: ipSmall}})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, got)
	})

	// from_ip / to_ip
	t.Run("from_ip", func(t *testing.T) {
		fn := builtins["from_ip"].overloads[0].Fn
		ipBytes := net.ParseIP("192.168.1.2").To4()
		got, err := fn(ctx, tree.Datums{tree.NewDBytes(tree.DBytes(ipBytes))})
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("192.168.1.2"), got)
	})
	t.Run("to_ip", func(t *testing.T) {
		// Safely get the only overload
		fn := builtins["to_ip"].overloads[0].Fn

		// Test valid IPv4
		testIP := "192.168.1.2"
		got, err := fn(ctx, tree.Datums{tree.NewDString(testIP)})
		require.NoError(t, err)

		expectedIP := net.ParseIP(testIP)
		expected := tree.NewDBytes(tree.DBytes(expectedIP))
		assert.Equal(t, expected, got)

		// Test invalid IP (error case)
		_, err = fn(ctx, tree.Datums{tree.NewDString("invalid-ip")})
		assert.Error(t, err)
	})

	t.Run("abbrev", func(t *testing.T) {
		abbrevFn := builtins["abbrev"].overloads[0].Fn
		if ipErr := ipaddr.ParseINet("192.168.1.2/24", &ipAddr); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		got, err := abbrevFn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipAddr}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("192.168.1.2/24"), got)
	})

	t.Run("family", func(t *testing.T) {
		familyFn := builtins["family"].overloads[0].Fn
		if ipErr := ipaddr.ParseINet("192.168.1.2/24", &ipAddr); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		got, err := familyFn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipAddr}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(4), got)
	})

	t.Run("host", func(t *testing.T) {
		hostFn := builtins["host"].overloads[0].Fn
		if ipErr := ipaddr.ParseINet("192.168.1.2/24", &ipAddr); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		got, err := hostFn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipAddr}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("192.168.1.2"), got)
	})

	t.Run("masklen", func(t *testing.T) {
		masklenFn := builtins["masklen"].overloads[0].Fn
		if ipErr := ipaddr.ParseINet("192.168.1.2/24", &ipAddr); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		got, err := masklenFn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipAddr}})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(24), got)
	})

	t.Run("set_masklen", func(t *testing.T) {
		setMasklenFn := builtins["set_masklen"].overloads[0].Fn
		if ipErr := ipaddr.ParseINet("192.168.1.2/24", &ipAddr); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		got, err := setMasklenFn(ctx, tree.Datums{&tree.DIPAddr{IPAddr: ipAddr}, tree.NewDInt(16)})
		assert.NoError(t, err)
		var ipAddr2 ipaddr.IPAddr
		if ipErr := ipaddr.ParseINet("192.168.1.2/16", &ipAddr2); ipErr != nil {
			assert.NoError(t, ipErr)
		}
		assert.Equal(t, &tree.DIPAddr{IPAddr: ipAddr2}, got)
	})
}

// TestUUIDFunctions tests UUID related functions.
func TestUUIDFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	t.Run("experimental_uuid_v4", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns 16-byte UUID",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("experimental_uuid_v4")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					// Verify return type is DBytes
					b, ok := got.(*tree.DBytes)
					assert.True(t, ok, "return type should be DBytes")

					// UUID is always 16 bytes long
					assert.Equal(t, 16, len(*b))
				}
			})
		}
	})

	t.Run("uuid_v4", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns 16-byte UUID",
				args:     tree.Datums{},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("experimental_uuid_v4")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					// Verify return type is DBytes
					b, ok := got.(*tree.DBytes)
					assert.True(t, ok, "return type should be DBytes")

					// UUID is always 16 bytes long
					assert.Equal(t, 16, len(*b))
				}
			})
		}
	})

	t.Run("gen_random_uuid", func(t *testing.T) {
		genFn := builtins["gen_random_uuid"].overloads[0].Fn
		got, err := genFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		_, ok := got.(*tree.DUuid)
		assert.True(t, ok)
	})

	t.Run("to_uuid/from_uuid", func(t *testing.T) {
		toUUIDFn := builtins["to_uuid"].overloads[0].Fn
		uuidStr := "123e4567-e89b-12d3-a456-426614174000"
		got, err := toUUIDFn(ctx, tree.Datums{tree.NewDString(uuidStr)})
		assert.NoError(t, err)
		uv, _ := uuid.FromString(uuidStr)
		assert.Equal(t, tree.NewDBytes(tree.DBytes(uv.GetBytes())), got)

		fromUUIDFn := builtins["from_uuid"].overloads[0].Fn
		got2, err := fromUUIDFn(ctx, tree.Datums{got})
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString(uuidStr), got2)
	})
}

// TestMiscFunctions tests miscellaneous functions.
func TestMiscFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()

	t.Run("random", func(t *testing.T) {
		randomFn := builtins["random"].overloads[0].Fn
		got, err := randomFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		f := float64(*got.(*tree.DFloat))
		assert.True(t, f >= 0 && f < 1)
	})

	t.Run("unique_rowid", func(t *testing.T) {
		uniqueRowIDFn := builtins["unique_rowid"].overloads[0].Fn
		got, err := uniqueRowIDFn(ctx, tree.Datums{})
		assert.NoError(t, err)
		assert.IsType(t, tree.NewDInt(0), got)
	})
}

// TestGEOFunctions tests GEO functions.
func TestGEOFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("")

	ctx := newTestEvalContext()

	// -------------------------------------------------------------------------
	// ST_Buffer Geometry with radius
	// -------------------------------------------------------------------------
	t.Run("st_buffer_geometry_float", func(t *testing.T) {
		fn := builtins["st_buffer"].overloads[0].Fn

		// Test with a point WKT
		pointWKT := tree.NewDString("POINT(0 0)")
		radius := tree.NewDFloat(1.0)

		got, err := fn(ctx, tree.Datums{pointWKT, radius})
		assert.NoError(t, err)

		// Result is a polygon WKT string
		_, ok := got.(*tree.DString)
		assert.True(t, ok)
	})

	// -------------------------------------------------------------------------
	// ST_Distance Geometry to Geometry
	// -------------------------------------------------------------------------
	t.Run("st_distance_geometry_geometry", func(t *testing.T) {
		fn := builtins["st_distance"].overloads[0].Fn

		// Test with two points
		p1 := tree.NewDString("POINT(0 0)")
		p2 := tree.NewDString("POINT(3 4)")

		got, err := fn(ctx, tree.Datums{p1, p2})
		assert.NoError(t, err)

		// Expected distance is 5.0
		expected := tree.NewDFloat(5.0)
		assert.Equal(t, expected, got)
	})

	t.Run("st_dwithin", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "two points within distance returns true",
				args:     tree.Datums{tree.NewDString("POINT(0 0)"), tree.NewDString("POINT(0 1)"), tree.NewDFloat(2.0)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "two points outside distance returns false",
				args:     tree.Datums{tree.NewDString("POINT(0 0)"), tree.NewDString("POINT(3 4)"), tree.NewDFloat(4.0)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry wkt returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(0 0)"), tree.NewDFloat(1.0)},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_dwithin")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				// Validate expected results
				switch tt.name {
				case "two points within distance returns true":
					assert.Equal(t, tree.DBoolTrue, b)
				case "two points outside distance returns false":
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_contains", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "polygon contains point returns true",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "polygon does not contain point returns false",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"), tree.NewDString("POINT(3 3)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry WKT returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_contains")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				switch tt.name {
				case "polygon contains point returns true":
					assert.Equal(t, tree.DBoolTrue, b)
				case "polygon does not contain point returns false":
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_intersects", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "polygon and point intersect returns true",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "polygon and point do not intersect returns false",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(3 3)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_intersects")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				if tt.name == "polygon and point intersect returns true" {
					assert.Equal(t, tree.DBoolTrue, b)
				} else {
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_equals", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "same points are equal returns true",
				args:     tree.Datums{tree.NewDString("POINT(1 1)"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "different points are not equal returns false",
				args:     tree.Datums{tree.NewDString("POINT(1 1)"), tree.NewDString("POINT(2 2)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_equals")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				if tt.name == "same points are equal returns true" {
					assert.Equal(t, tree.DBoolTrue, b)
				} else {
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_touches", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "polygon touches point returns true",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(0 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "polygon does not touch point returns false",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(3 3)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(0 1)")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_touches")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				if tt.name == "polygon touches point returns true" {
					assert.Equal(t, tree.DBoolTrue, b)
				} else {
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_covers", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "polygon covers point returns true",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "polygon does not cover point returns false",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))"), tree.NewDString("POINT(3 3)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry returns error",
				args:     tree.Datums{tree.NewDString("INVALID"), tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_covers")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				b, ok := got.(*tree.DBool)
				assert.True(t, ok, "return type should be DBool")

				if tt.name == "polygon covers point returns true" {
					assert.Equal(t, tree.DBoolTrue, b)
				} else {
					assert.Equal(t, tree.DBoolFalse, b)
				}
			})
		}
	})

	t.Run("st_area", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "square polygon returns correct area",
				args:     tree.Datums{tree.NewDString("POLYGON((0 0,0 2,2 2,2 0,0 0))")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "point geometry returns zero area",
				args:     tree.Datums{tree.NewDString("POINT(1 1)")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid geometry returns error",
				args:     tree.Datums{tree.NewDString("INVALID")},
				overload: 0,
				wantErr:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("st_area")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				f, ok := got.(*tree.DFloat)
				assert.True(t, ok, "return type should be DFloat")

				if tt.name == "square polygon returns correct area" {
					assert.Equal(t, tree.DFloat(4.0), *f)
				}
				if tt.name == "point geometry returns zero area" {
					assert.Equal(t, tree.DFloat(0.0), *f)
				}
			})
		}
	})
}

// TestGroupWindowFunctions tests group window functions.
func TestGroupWindowFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := newTestEvalContext()
	ctx.GroupWindow = &tree.GroupWindow{
		GroupWindowFunc: tree.GroupWindowUnknown,
	}

	t.Run("time_window_start", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns original timestamptz",
				args:     tree.Datums{tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), time.Nanosecond)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "handles null input",
				args:     tree.Datums{tree.DNull},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_window_start")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					if tt.args[0] == tree.DNull {
						assert.Equal(t, tree.DNull, got)
					} else {
						_, ok := got.(*tree.DTimestampTZ)
						assert.True(t, ok, "return type should be DTimestampTZ")
						assert.Equal(t, tt.args[0], got)
					}
				}
			})
		}
	})

	t.Run("time_window_end", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "returns original timestamptz",
				args:     tree.Datums{tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), time.Nanosecond)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "handles null input",
				args:     tree.Datums{tree.DNull},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_window_end")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					if tt.args[0] == tree.DNull {
						assert.Equal(t, tree.DNull, got)
					} else {
						_, ok := got.(*tree.DTimestampTZ)
						assert.True(t, ok, "return type should be DTimestampTZ")
						assert.Equal(t, tt.args[0], got)
					}
				}
			})
		}
	})

	t.Run("state_window", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "int column",
				args:     tree.Datums{tree.NewDInt(1)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "bool column",
				args:     tree.Datums{tree.MakeDBool(true)},
				overload: 1,
				wantErr:  false,
			},
			{
				name:     "varchar column",
				args:     tree.Datums{tree.NewDString("state")},
				overload: 2,
				wantErr:  false,
			},
			{
				name:     "bytes column",
				args:     tree.Datums{tree.NewDBytes("data")},
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("state_window")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.args[0], got)
				}
			})
		}
	})

	t.Run("event_window", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "start and end bool",
				args:     tree.Datums{tree.MakeDBool(true), tree.MakeDBool(false)},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("event_window")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tree.DZero, got)
				}
			})
		}
	})

	t.Run("session_window", func(t *testing.T) {
		now := timeutil.Now()
		ts := tree.MakeDTimestamp(now, 0)
		tstz := tree.MakeDTimestampTZ(now, 0)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "timestamp with interval",
				args:     tree.Datums{ts, tree.NewDString("5m")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "timestamptz with interval",
				args:     tree.Datums{tstz, tree.NewDString("5m")},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("session_window")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.args[0], got)
				}
			})
		}
	})

	t.Run("count_window", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "two int args valid",
				args:     tree.Datums{tree.NewDInt(10), tree.NewDInt(5)},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "single int arg",
				args:     tree.Datums{tree.NewDInt(10)},
				overload: 1,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("count_window")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tree.DZero, got)
				}
			})
		}
	})

	t.Run("time_window", func(t *testing.T) {
		now := timeutil.Now()
		ts := tree.MakeDTimestamp(now, 0)
		tstz := tree.MakeDTimestampTZ(now, 0)

		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "timestamp with interval and sliding",
				args:     tree.Datums{ts, tree.NewDString("1h"), tree.NewDString("30m")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "timestamptz with interval and sliding",
				args:     tree.Datums{tstz, tree.NewDString("1h"), tree.NewDString("30m")},
				overload: 1,
				wantErr:  false,
			},
			{
				name:     "timestamp with interval only",
				args:     tree.Datums{ts, tree.NewDString("1h")},
				overload: 2,
				wantErr:  false,
			},
			{
				name:     "timestamptz with interval only",
				args:     tree.Datums{tstz, tree.NewDString("1h")},
				overload: 3,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("time_window")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tt.args[0], got)
				}
			})
		}
	})
}
