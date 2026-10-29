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

package mysqlwire

import (
	"context"
	"testing"
)

// TestAddPGResultRejectsPGBytes pins the backstop. After the planner learned to
// ask SupportsPGWireResults, nothing should reach this method; if something
// does, the guard is broken and the caller must hear about it rather than have
// its rows dropped.
func TestAddPGResultRejectsPGBytes(t *testing.T) {
	r := &commandResult{}
	err := r.AddPGResult(context.Background(), []byte("D\x00\x00\x00\x0b"))
	if err == nil {
		t.Fatal("AddPGResult returned nil; pre-encoded pg bytes must be rejected, not dropped")
	}
}

// TestSupportsPGWireResultsIsFalse guards the value the planner reads. Flipping
// it to true would silently restore the empty-result-set bug.
func TestSupportsPGWireResultsIsFalse(t *testing.T) {
	r := &commandResult{}
	if r.SupportsPGWireResults() {
		t.Error("a MySQL connection cannot forward PostgreSQL wire bytes")
	}
}
