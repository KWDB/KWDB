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
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestZeroNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create test columns
	columns := sqlbase.ResultColumns{
		{Name: "col1", Typ: types.Int},
		{Name: "col2", Typ: types.String},
	}

	// Test newZeroNode
	zeroNode := newZeroNode(columns)
	if zeroNode == nil {
		t.Error("newZeroNode should return a non-nil pointer")
	}

	// Verify columns are set correctly
	if len(zeroNode.columns) != len(columns) {
		t.Errorf("expected %d columns, got %d", len(columns), len(zeroNode.columns))
	}

	for i, col := range zeroNode.columns {
		if col.Name != columns[i].Name {
			t.Errorf("column %d name mismatch: expected %s, got %s", i, columns[i].Name, col.Name)
		}
	}

	// Test startExec
	err := zeroNode.startExec(runParams{})
	if err != nil {
		t.Errorf("startExec should return nil, got %v", err)
	}

	// Test Next
	next, err := zeroNode.Next(runParams{})
	if err != nil {
		t.Errorf("Next should return nil error, got %v", err)
	}
	if next {
		t.Error("Next should return false")
	}

	// Test Values
	values := zeroNode.Values()
	if values != nil {
		t.Error("Values should return nil")
	}

	// Test Close
	// Close should not panic
	zeroNode.Close(context.Background())
}

//func TestZeroNodeEmptyColumns(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	// Test with empty columns
//	zeroNode := newZeroNode(nil)
//	if zeroNode == nil {
//		t.Error("newZeroNode should return a non-nil pointer even with nil columns")
//	}
//
//	if zeroNode.columns != nil {
//		t.Error("columns should be nil when nil is passed to newZeroNode")
//	}
//
//	// Test methods with empty columns
//	err := zeroNode.startExec(runParams{})
//	if err != nil {
//		t.Errorf("startExec should return nil, got %v", err)
//	}
//
//	next, err := zeroNode.Next(runParams{})
//	if err != nil {
//		t.Errorf("Next should return nil error, got %v", err)
//	}
//	if next {
//		t.Error("Next should return false")
//	}
//
//	values := zeroNode.Values()
//	if values != nil {
//		t.Error("Values should return nil")
//	}
//
//	zeroNode.Close(context.Background())
//}
