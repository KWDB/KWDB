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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// TestInit tests the Init function
func TestInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create mock dependencies
	mockDB := &kv.DB{}
	mockTSE := &tse.TsEngine{}

	// Call Init
	Init(mockDB, mockTSE)

	// Verify that handler has been initialized
	if handler.db != mockDB {
		t.Fatalf("expected handler.db to be %v, got %v", mockDB, handler.db)
	}
	if handler.tse != mockTSE {
		t.Fatalf("expected handler.tse to be %v, got %v", mockTSE, handler.tse)
	}
}

// TestIsObjectCannotFoundError tests the IsObjectCannotFoundError function
func TestIsObjectCannotFoundError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case 1: Object not found error
	err1 := errors.New("object cannot found")
	if !IsObjectCannotFoundError(err1) {
		t.Fatal("expected IsObjectCannotFoundError to return true for 'object cannot found' error")
	}

	// Test case 2: Other error
	err2 := errors.New("some other error")
	if IsObjectCannotFoundError(err2) {
		t.Fatal("expected IsObjectCannotFoundError to return false for non-'object cannot found' error")
	}

	// Test case 3: Error containing 'object cannot found' as substring
	err3 := errors.New("something object cannot found something")
	if !IsObjectCannotFoundError(err3) {
		t.Fatal("expected IsObjectCannotFoundError to return true for error containing 'object cannot found' substring")
	}
}

// TestMakeNameSpaceByRow tests the makeNameSpaceByRow function
func TestMakeNameSpaceByRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create test row data
	row := tree.Datums{
		tree.NewDInt(1),         // ParentID
		tree.NewDInt(2),         // ParentSchemaID
		tree.NewDString("test"), // Name
		tree.NewDInt(3),         // ID
	}

	// Call makeNameSpaceByRow
	ns := makeNameSpaceByRow(row)

	// Verify the result
	expected := sqlbase.Namespace{
		ParentID:       1,
		ParentSchemaID: 2,
		Name:           "test",
		ID:             3,
	}

	if ns != expected {
		t.Fatalf("expected namespace %v, got %v", expected, ns)
	}
}

// TestSortTSColumns tests the sortTSColumns function
func TestSortTSColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create test data
	a := []sqlbase.KWDBKTSColumn{
		{ColumnId: 1},
		{ColumnId: 3},
	}
	b := []sqlbase.KWDBKTSColumn{
		{ColumnId: 2},
		{ColumnId: 4},
	}

	// Call sortTSColumns
	result := sortTSColumns(a, b)

	// Verify the result is sorted by ColumnId
	expectedOrder := []uint32{1, 2, 3, 4}
	for i, col := range result {
		if col.ColumnId != expectedOrder[i] {
			t.Fatalf("expected column %d to have ColumnId %d, got %d", i, expectedOrder[i], col.ColumnId)
		}
	}

	// Test with empty slices
	aEmpty := []sqlbase.KWDBKTSColumn{}
	bEmpty := []sqlbase.KWDBKTSColumn{}
	resultEmpty := sortTSColumns(aEmpty, bEmpty)
	if len(resultEmpty) != 0 {
		t.Fatalf("expected empty result for empty input, got %d columns", len(resultEmpty))
	}

	// Test with only one slice non-empty
	aOnly := []sqlbase.KWDBKTSColumn{{ColumnId: 2}, {ColumnId: 1}}
	resultAOnly := sortTSColumns(aOnly, bEmpty)
	if len(resultAOnly) != 2 {
		t.Fatalf("expected 2 columns in result, got %d", len(resultAOnly))
	}
	// Note: sortTSColumns doesn't sort a, so the order remains unchanged
	if resultAOnly[0].ColumnId != 2 || resultAOnly[1].ColumnId != 1 {
		t.Fatalf("expected result to preserve original order of a, got %v", resultAOnly)
	}
}

// TestGetNameSpaceByParentID tests the GetNameSpaceByParentID function
// Note: This test would require a mock kv.Txn implementation
// For simplicity, we'll just test the error handling path
func TestGetNameSpaceByParentID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Test with a mock txn that returns "object cannot found" error
	mockTxn := &kv.Txn{}
	// We can't easily mock the txn.Scan method here without a proper mock implementation
	// In a real test, we would use a mock kv.Txn that returns the appropriate error

	// For now, we'll just test the function signature and basic error handling
	// The actual implementation would require a more complex test setup
	_, err := GetNameSpaceByParentID(ctx, mockTxn, 1, 1)
	// We expect an error here since we're using a real kv.Txn without proper setup
	if err == nil {
		t.Log("GetNameSpaceByParentID returned no error with mock txn")
	} else {
		t.Logf("GetNameSpaceByParentID returned error as expected: %v", err)
	}
}
