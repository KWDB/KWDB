// Copyright 2019 The Cockroach Authors.
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

package pgdate

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	for _, tc := range []struct {
		s      string
		err    string
		pgdays int32
	}{
		{
			s:      "2000-01-01",
			pgdays: 0,
		},
		{
			s:      "1999-12-31",
			pgdays: -1,
		},
		{
			s:      "2000-01-02",
			pgdays: 1,
		},
		{
			s:      "0001-01-01",
			pgdays: -730119,
		},
		{
			s:      "0001-12-31 BC",
			pgdays: -730120,
		},
		{
			s:      "0002-01-01 BC",
			pgdays: -730850,
		},
		{
			s:      "5874897-12-31",
			pgdays: highDays,
		},
		{
			s:      "4714-11-24 BC",
			pgdays: lowDays,
		},
		{
			s:   "4714-11-23 BC",
			err: "date is out of range",
		},
		{
			s:   "5874898-01-01",
			err: "date is out of range",
		},
		{
			s:   "0000-01-01 AD",
			err: "only positive years are permitted in AD/BC notation",
		},
	} {
		t.Run(tc.s, func(t *testing.T) {
			d, err := ParseDate(time.Time{}, ParseModeYMD, tc.s)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v, expected %v", err, tc.err)
				}
				return
			}
			pg := d.PGEpochDays()
			if pg != tc.pgdays {
				t.Fatalf("%d != %d", pg, tc.pgdays)
			}
			s := d.String()
			if s != tc.s {
				t.Fatalf("%s != %s", s, tc.s)
			}
		})
	}
}

func TestMakeCompatibleDateFromDisk(t *testing.T) {
	for _, tc := range []struct {
		in, out int64
	}{
		{0, 0},
		{1, 1},
		{-1, -1},
		{math.MaxInt64, math.MaxInt64},
		{math.MinInt64, math.MinInt64},
		{math.MaxInt32, math.MaxInt64},
		{math.MinInt32, math.MinInt64},
	} {
		t.Run(fmt.Sprint(tc.in), func(t *testing.T) {
			date := MakeCompatibleDateFromDisk(tc.in)
			orig := date.UnixEpochDaysWithOrig()
			if orig != tc.in {
				t.Fatalf("%d != %d", orig, tc.in)
			}
			days := date.UnixEpochDays()
			if days != tc.out {
				t.Fatalf("%d != %d", days, tc.out)
			}
		})
	}
}

func TestMakeDateFromTime(t *testing.T) {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pgEpochWithHourOffset := time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)
	// These dates are negative, which makes rounding a little different.
	dayBeforeUnixEpoch := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
	dayBeforeUnixEpochWithHourOffset := time.Date(1969, 12, 31, 1, 0, 0, 0, time.UTC)
	twoDaysBeforeUnixEpoch := time.Date(1969, 12, 30, 0, 0, 0, 0, time.UTC)
	twoDaysBeforeUnixEpochWithHourOffset := time.Date(1969, 12, 30, 1, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		in  time.Time
		out string
	}{
		{pgEpoch.In(time.FixedZone("secsPerDay", secondsPerDay)), "2000-01-02"},
		{pgEpoch.In(time.FixedZone("secsPerDay-1", secondsPerDay-1)), "2000-01-01"},
		{pgEpoch.In(time.FixedZone("1", 1)), "2000-01-01"},
		{pgEpoch, "2000-01-01"},
		{pgEpoch.In(time.FixedZone("-1", -1)), "1999-12-31"},
		{pgEpoch.In(time.FixedZone("-secsPerDay", -secondsPerDay)), "1999-12-31"},
		{pgEpochWithHourOffset, "2000-01-01"},

		{dayBeforeUnixEpoch, "1969-12-31"},
		{dayBeforeUnixEpochWithHourOffset, "1969-12-31"},
		{twoDaysBeforeUnixEpoch, "1969-12-30"},
		{twoDaysBeforeUnixEpochWithHourOffset, "1969-12-30"},
	} {
		t.Run(tc.in.Format(time.RFC3339), func(t *testing.T) {
			d, err := MakeDateFromTime(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			exp := tc.in.Format("2006-01-02")
			// Sanity check our tests.
			if exp != tc.out {
				t.Fatalf("got %s, expected %s", exp, tc.out)
			}
			s := d.String()
			if exp != s {
				t.Fatalf("got %s, expected %s", s, exp)
			}
		})
	}
}
