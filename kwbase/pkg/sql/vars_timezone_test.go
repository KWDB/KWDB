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

package sql

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func TestSessionDataTimeZoneFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		loc      *time.Location
		expected string
	}{
		{
			name:     "UTC",
			loc:      time.UTC,
			expected: "UTC",
		},
		{
			name:     "fixed offset +08:00",
			loc:      timeutil.FixedOffsetTimeZoneToLocation(8*3600, "+08:00"),
			expected: "+08:00",
		},
		{
			name:     "fixed offset -05:00",
			loc:      timeutil.FixedOffsetTimeZoneToLocation(-5*3600, "-05:00"),
			expected: "-05:00",
		},
		{
			name:     "fixed offset UTC",
			loc:      timeutil.FixedOffsetTimeZoneToLocation(0, "UTC"),
			expected: "UTC",
		},
		{
			name:     "fixed offset +05:30",
			loc:      timeutil.FixedOffsetTimeZoneToLocation(5*3600+30*60, "+05:30"),
			expected: "+05:30",
		},
		{
			name:     "fixed offset -08:30",
			loc:      timeutil.FixedOffsetTimeZoneToLocation(-8*3600-30*60, "-08:30"),
			expected: "-08:30",
		},
		{
			name:     "local timezone",
			loc:      time.Local,
			expected: time.Local.String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sessionDataTimeZoneFormat(tt.loc)
			if result != tt.expected {
				t.Errorf("sessionDataTimeZoneFormat(%v) = %q, expected %q", tt.loc, result, tt.expected)
			}
		})
	}
}

func TestServerTimezoneFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string (local timezone)",
			input:    "",
			expected: "UTC", // default when system timezone is UTC
		},
		{
			name:     "numeric offset 8",
			input:    "8",
			expected: "+08:00",
		},
		{
			name:     "numeric offset -5",
			input:    "-5",
			expected: "-05:00",
		},
		{
			name:     "numeric offset 0",
			input:    "0",
			expected: "+00:00",
		},
		{
			name:     "numeric offset with fraction 5.5",
			input:    "5.5",
			expected: "+05:30",
		},
		{
			name:     "numeric offset -3.25",
			input:    "-3.25",
			expected: "-03:15",
		},
		{
			name:     "ISO8601 format +08:00",
			input:    "+08:00",
			expected: "+08:00",
		},
		{
			name:     "ISO8601 format -05:00",
			input:    "-05:00",
			expected: "-05:00",
		},
		{
			name:     "named timezone Asia/Shanghai",
			input:    "Asia/Shanghai",
			expected: "Asia/Shanghai",
		},
		{
			name:     "named timezone America/New_York",
			input:    "America/New_York",
			expected: "America/New_York",
		},
		{
			name:     "UTC",
			input:    "UTC",
			expected: "UTC",
		},
		{
			name:     "invalid timezone returns original",
			input:    "Invalid/Timezone",
			expected: "Invalid/Timezone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := serverTimezoneFormat(tt.input)
			if result != tt.expected {
				t.Errorf("serverTimezoneFormat(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}
