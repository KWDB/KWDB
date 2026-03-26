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

func TestDefaultIntSize4Counter(t *testing.T) {
	counter := sqltelemetry.DefaultIntSize4Counter
	if counter == nil {
		t.Error("DefaultIntSize4Counter should not be nil")
	}
}

func TestForceSavepointRestartCounter(t *testing.T) {
	counter := sqltelemetry.ForceSavepointRestartCounter
	if counter == nil {
		t.Error("ForceSavepointRestartCounter should not be nil")
	}
}

func TestUnimplementedSessionVarValueCounter(t *testing.T) {
	varNames := []string{"var1", "var2", "var3"}
	values := []string{"val1", "val2", "val3"}

	for _, varName := range varNames {
		for _, val := range values {
			counter := sqltelemetry.UnimplementedSessionVarValueCounter(varName, val)
			if counter == nil {
				t.Errorf("UnimplementedSessionVarValueCounter with varName '%s' and val '%s' should not return nil", varName, val)
			}
		}
	}

	// Test with empty strings
	counter := sqltelemetry.UnimplementedSessionVarValueCounter("", "")
	if counter == nil {
		t.Error("UnimplementedSessionVarValueCounter with empty strings should not return nil")
	}
}
