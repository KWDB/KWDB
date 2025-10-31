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
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
)

func TestTimeBuiltinIntOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeBuiltin, exists := builtins["to_time"]
	if !exists {
		t.Fatal("to_time builtin not found")
	}

	var intOverload tree.Overload
	for _, ov := range timeBuiltin.overloads {
		if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Int) {
			intOverload = ov
			break
		}
	}

	if intOverload.Fn == nil {
		t.Fatal("to_time(INT) overload not found")
	}

	testCases := []struct {
		milliseconds int64
		expectedHour int
		expectedMin  int
		expectedSec  int
		description  string
	}{
		{1000, 0, 0, 1, "1000ms -> 00:00:01"},
		{3600000, 1, 0, 0, "3600000ms -> 01:00:00"},
		{3661000, 1, 1, 1, "3661000ms -> 01:01:01"},
		{0, 0, 0, 0, "0ms -> 00:00:00"},
		{1234567, 0, 20, 34, "1234567ms -> 00:20:34"},
		{86399000, 23, 59, 59, "86399000ms -> 23:59:59"},
		{3723000, 1, 2, 3, "3723000ms -> 01:02:03"},
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			args := tree.Datums{tree.NewDInt(tree.DInt(tc.milliseconds))}

			result, err := intOverload.Fn(evalCtx, args)
			if err != nil {
				t.Fatalf("to_time(%d) failed: %v", tc.milliseconds, err)
			}

			if result.ResolvedType().Family() != types.TimeFamily {
				t.Fatalf("expected TIME result, got %s", result.ResolvedType())
			}

			timeResult := result.(*tree.DTime)
			timeOfDay := timeofday.TimeOfDay(*timeResult)
			hour, min, sec := timeOfDay.Hour(), timeOfDay.Minute(), timeOfDay.Second()

			if hour != tc.expectedHour {
				t.Errorf("hour mismatch: expected %d, got %d", tc.expectedHour, hour)
			}
			if min != tc.expectedMin {
				t.Errorf("minute mismatch: expected %d, got %d", tc.expectedMin, min)
			}
			if sec != tc.expectedSec {
				t.Errorf("second mismatch: expected %d, got %d", tc.expectedSec, sec)
			}

			t.Logf("✓ to_time(%d) = %02d:%02d:%02d", tc.milliseconds, hour, min, sec)
		})
	}
}

func TestTimeToSecBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeToSecBuiltin, exists := builtins["time_to_sec"]
	if !exists {
		t.Fatal("time_to_sec builtin not found")
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	t.Run("TIME_to_INT", func(t *testing.T) {
		var timeOverload tree.Overload
		for _, ov := range timeToSecBuiltin.overloads {
			if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Time) {
				timeOverload = ov
				break
			}
		}

		if timeOverload.Fn == nil {
			t.Fatal("time_to_sec(TIME) overload not found")
		}

		timeInput := tree.MakeDTime(timeofday.New(1, 2, 3, 0))
		args := tree.Datums{timeInput}

		result, err := timeOverload.Fn(evalCtx, args)
		if err != nil {
			t.Fatalf("time_to_sec(%s) failed: %v", timeInput, err)
		}

		if result.ResolvedType().Family() != types.IntFamily {
			t.Fatalf("expected INT result, got %s", result.ResolvedType())
		}

		expectedSeconds := int64(1*3600 + 2*60 + 3) // 1:02:03 = 3723 seconds
		actualSeconds := int64(tree.MustBeDInt(result))

		if actualSeconds != expectedSeconds {
			t.Errorf("expected %d seconds, got %d", expectedSeconds, actualSeconds)
		}

		t.Logf("✓ time_to_sec(%s) = %d seconds", timeInput, actualSeconds)
	})

	t.Run("TIMESTAMP_to_INT", func(t *testing.T) {
		var timestampOverload tree.Overload
		for _, ov := range timeToSecBuiltin.overloads {
			if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Timestamp) {
				timestampOverload = ov
				break
			}
		}

		if timestampOverload.Fn == nil {
			t.Fatal("time_to_sec(TIMESTAMP) overload not found")
		}

		timestampInput := tree.MakeDTimestamp(time.Date(2023, 1, 1, 1, 2, 3, 0, time.UTC), time.Nanosecond)
		args := tree.Datums{timestampInput}

		result, err := timestampOverload.Fn(evalCtx, args)
		if err != nil {
			t.Fatalf("time_to_sec(%s) failed: %v", timestampInput, err)
		}

		if result.ResolvedType().Family() != types.IntFamily {
			t.Fatalf("expected INT result, got %s", result.ResolvedType())
		}

		expectedSeconds := int64(1*3600 + 2*60 + 3) // 1:02:03 = 3723 seconds
		actualSeconds := int64(tree.MustBeDInt(result))

		if actualSeconds != expectedSeconds {
			t.Errorf("expected %d seconds, got %d", expectedSeconds, actualSeconds)
		}

		t.Logf("✓ time_to_sec(%s) = %d seconds", timestampInput, actualSeconds)
	})
}

func TestStrToDateBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	strToDateBuiltin, exists := builtins["str_to_date"]
	if !exists {
		t.Fatal("str_to_date builtin not found")
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	testCases := []struct {
		dateStr     string
		formatStr   string
		expected    string
		description string
		expectError bool
	}{
		{
			dateStr:     "2023-12-25 14:30:45",
			formatStr:   "%Y-%m-%d %H:%M:%S",
			expected:    "2023-12-25 14:30:45",
			description: "Standard datetime format",
			expectError: false,
		},
		{
			dateStr:     "25/12/2023",
			formatStr:   "%d/%m/%Y",
			expected:    "2023-12-25 00:00:00",
			description: "Date only format",
			expectError: false,
		},
		{
			dateStr:     "20231225",
			formatStr:   "%Y%m%d",
			expected:    "2023-12-25 00:00:00",
			description: "Compact date format",
			expectError: false,
		},
		{
			dateStr:     "14:30:45",
			formatStr:   "%H:%M:%S",
			expected:    "1899-12-31 14:30:45",
			description: "Time only format",
			expectError: false,
		},
		{
			dateStr:     "10/25/23",
			formatStr:   "%D",
			expected:    "2023-10-25 00:00:00",
			description: "MM/DD/YY format",
			expectError: false,
		},
		{
			dateStr:     "2023-10-25",
			formatStr:   "%F",
			expected:    "2023-10-25 00:00:00",
			description: "ISO date format",
			expectError: false,
		},
		{
			dateStr:     "2023 286",
			formatStr:   "%Y %j",
			expected:    "2023-10-13 00:00:00",
			description: "Year and day of year format",
			expectError: false,
		},
		{
			dateStr:     "2023 10 4",
			formatStr:   "%Y %U %u",
			expected:    "2023-03-09 00:00:00",
			description: "Year, week number and weekday format",
			expectError: false,
		},
		{
			dateStr:     "invalid-date",
			formatStr:   "%Y-%m-%d",
			expected:    "NULL",
			description: "Invalid date string (implementation returns zero time)",
			expectError: false,
		},
		{
			dateStr:     "2023-13-01",
			formatStr:   "%Y-%m-%d",
			expected:    "NULL",
			description: "Invalid month (implementation returns zero time)",
			expectError: false,
		},
		{
			dateStr:     "2023-12-32",
			formatStr:   "%Y-%m-%d",
			expected:    "NULL",
			description: "Invalid day (implementation returns zero time)",
			expectError: false,
		},
		{
			dateStr:     "25:61:45",
			formatStr:   "%H:%M:%S",
			expected:    "NULL",
			description: "Invalid time (implementation returns zero time)",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			args := tree.Datums{
				tree.NewDString(tc.dateStr),
				tree.NewDString(tc.formatStr),
			}

			result, err := strToDateBuiltin.overloads[0].Fn(evalCtx, args)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error for invalid input, but got none")
				}
				if result != tree.DNull {
					t.Errorf("expected DNull for invalid input, but got %v", result)
				}
				t.Logf("✓ str_to_date('%s', '%s') = NULL (expected error)", tc.dateStr, tc.formatStr)
				return
			}

			if err != nil {
				t.Fatalf("str_to_date('%s', '%s') failed: %v", tc.dateStr, tc.formatStr, err)
			}

			if tc.expected == "NULL" {
				if result != tree.DNull {
					t.Errorf("expected NULL, got %v", result)
				}
				return
			}

			if result.ResolvedType().Family() != types.TimestampTZFamily {
				t.Fatalf("expected TIMESTAMPTZ result, got %s", result.ResolvedType())
			}

			timestampResult := result.(*tree.DTimestampTZ)
			actualTime := timestampResult.Time

			actualStr := actualTime.Format("2006-01-02 15:04:05")

			if actualStr != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actualStr)
			}

			t.Logf("✓ str_to_date('%s', '%s') = %s", tc.dateStr, tc.formatStr, actualStr)
		})
	}
}

func TestStrToDateNullHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	strToDateBuiltin, exists := builtins["str_to_date"]
	if !exists {
		t.Fatal("str_to_date builtin not found")
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	testCases := []struct {
		dateStr     tree.Datum
		formatStr   tree.Datum
		description string
	}{
		{
			dateStr:     tree.DNull,
			formatStr:   tree.NewDString("%Y-%m-%d"),
			description: "Null date string",
		},
		{
			dateStr:     tree.NewDString("2023-12-25"),
			formatStr:   tree.DNull,
			description: "Null format string",
		},
		{
			dateStr:     tree.DNull,
			formatStr:   tree.DNull,
			description: "Both arguments null",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			args := tree.Datums{tc.dateStr, tc.formatStr}

			result, err := strToDateBuiltin.overloads[0].Fn(evalCtx, args)
			if err != nil {
				t.Fatalf("str_to_date failed: %v", err)
			}

			if result != tree.DNull {
				t.Errorf("expected DNull for null input, but got %v", result)
			}

			t.Logf("✓ str_to_date(null, null) = NULL")
		})
	}
}
