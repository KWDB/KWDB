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

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestCursorExecHelper tests the CursorExecHelper methods
func TestCursorExecHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a CursorExecHelper
	c := &CursorExecHelper{}

	// Test RunClearUp (should not panic)
	t.Run("RunClearUp", func(t *testing.T) {
		c.RunClearUp()
		// No error expected
	})
}

// TestGetPlanResultColumn tests the GetPlanResultColumn function
func TestGetPlanResultColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a simple planTop
	plan := &planTop{
		plan: &zeroNode{},
	}

	// Test GetPlanResultColumn
	resultPlan, _ := GetPlanResultColumn(plan)

	// Verify the returned plan is the same as the input
	if resultPlan != plan {
		t.Error("GetPlanResultColumn should return the same plan")
	}
}
