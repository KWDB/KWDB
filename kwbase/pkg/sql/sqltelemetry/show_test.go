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

package sqltelemetry_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

func TestShowTelemetryTypeString(t *testing.T) {
	tests := []struct {
		showType sqltelemetry.ShowTelemetryType
		expected string
	}{
		{sqltelemetry.Ranges, "ranges"},
		{sqltelemetry.Partitions, "partitions"},
		{sqltelemetry.Locality, "locality"},
		{sqltelemetry.Create, "create"},
		{sqltelemetry.RangeForRow, "rangeforrow"},
		{sqltelemetry.Queries, "queries"},
		{sqltelemetry.Indexes, "indexes"},
		{sqltelemetry.Constraints, "constraints"},
		{sqltelemetry.Jobs, "jobs"},
		{sqltelemetry.Roles, "roles"},
		{sqltelemetry.Schedules, "schedules"},
	}

	for _, test := range tests {
		result := test.showType.String()
		if result != test.expected {
			t.Errorf("ShowTelemetryType(%d).String() = %s; expected %s", test.showType, result, test.expected)
		}
	}
}

func TestIncrementShowCounter(t *testing.T) {
	// Test that we can call IncrementShowCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncrementShowCounter should not panic, but it did: %v", r)
		}
	}()

	// Test with each show type
	showTypes := []sqltelemetry.ShowTelemetryType{
		sqltelemetry.Ranges,
		sqltelemetry.Partitions,
		sqltelemetry.Locality,
		sqltelemetry.Create,
		sqltelemetry.RangeForRow,
		sqltelemetry.Queries,
		sqltelemetry.Indexes,
		sqltelemetry.Constraints,
		sqltelemetry.Jobs,
		sqltelemetry.Roles,
		sqltelemetry.Schedules,
	}

	for _, showType := range showTypes {
		sqltelemetry.IncrementShowCounter(showType)
	}
}
