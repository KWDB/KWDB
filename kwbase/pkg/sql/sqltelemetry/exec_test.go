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

func TestDistSQLExecCounter(t *testing.T) {
	counter := sqltelemetry.DistSQLExecCounter
	if counter == nil {
		t.Error("DistSQLExecCounter should not be nil")
	}
}

func TestVecExecCounter(t *testing.T) {
	counter := sqltelemetry.VecExecCounter
	if counter == nil {
		t.Error("VecExecCounter should not be nil")
	}
}

func TestVecModeCounter(t *testing.T) {
	counter := sqltelemetry.VecModeCounter("auto")
	if counter == nil {
		t.Error("VecModeCounter should not return nil")
	}

	// Test with different modes
	modes := []string{"auto", "on", "off", "experimental_on"}
	for _, mode := range modes {
		counter := sqltelemetry.VecModeCounter(mode)
		if counter == nil {
			t.Errorf("VecModeCounter with mode '%s' should not return nil", mode)
		}
	}
}
