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

func TestCancelRequestCounter(t *testing.T) {
	counter := sqltelemetry.CancelRequestCounter
	if counter == nil {
		t.Error("CancelRequestCounter should not be nil")
	}
}

func TestUnimplementedClientStatusParameterCounter(t *testing.T) {
	parameters := []string{"application_name", "search_path", "timezone", "datestyle"}

	for _, param := range parameters {
		counter := sqltelemetry.UnimplementedClientStatusParameterCounter(param)
		if counter == nil {
			t.Errorf("UnimplementedClientStatusParameterCounter with parameter '%s' should not return nil", param)
		}
	}

	// Test with empty string
	counter := sqltelemetry.UnimplementedClientStatusParameterCounter("")
	if counter == nil {
		t.Error("UnimplementedClientStatusParameterCounter with empty string should not return nil")
	}
}

func TestBinaryDecimalInfinityCounter(t *testing.T) {
	counter := sqltelemetry.BinaryDecimalInfinityCounter
	if counter == nil {
		t.Error("BinaryDecimalInfinityCounter should not be nil")
	}
}

func TestUncategorizedErrorCounter(t *testing.T) {
	counter := sqltelemetry.UncategorizedErrorCounter
	if counter == nil {
		t.Error("UncategorizedErrorCounter should not be nil")
	}
}

func TestInterleavedPortalRequestCounter(t *testing.T) {
	counter := sqltelemetry.InterleavedPortalRequestCounter
	if counter == nil {
		t.Error("InterleavedPortalRequestCounter should not be nil")
	}
}

func TestPortalWithLimitRequestCounter(t *testing.T) {
	counter := sqltelemetry.PortalWithLimitRequestCounter
	if counter == nil {
		t.Error("PortalWithLimitRequestCounter should not be nil")
	}
}
