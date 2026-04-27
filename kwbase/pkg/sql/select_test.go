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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// mockRows is a mock implementation of planNode for testing selectIntoNode
// mockRows 是一个模拟的 planNode 实现，用于测试 selectIntoNode
type mockRows struct {
	values [][]tree.Datum
	index  int
}

func (m *mockRows) startExec(params runParams) error {
	return nil
}

func (m *mockRows) Next(params runParams) (bool, error) {
	if m.index >= len(m.values) {
		return false, nil
	}
	m.index++
	return true, nil
}

func (m *mockRows) Values() tree.Datums {
	if m.index-1 < 0 || m.index-1 >= len(m.values) {
		return nil
	}
	return m.values[m.index-1]
}

func (m *mockRows) Close(ctx context.Context) {
}

// TestSelectIntoNode tests the selectIntoNode functionality
func TestSelectIntoNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test case 1: Normal case with one row and matching columns
	t.Run("Normal case with one row", func(t *testing.T) {
		// Create mock rows with one row and two columns
		mock := &mockRows{
			values: [][]tree.Datum{
				{tree.NewDInt(1), tree.NewDString("test")},
			},
			index: 0,
		}

		// Create selectIntoNode with two variables
		n := &selectIntoNode{
			rows: mock,
			vars: []string{"var1", "var2"},
			end:  false,
		}

		// Create a planner with sessionDataMutator
		p := &planner{
			sessionDataMutator: &sessionDataMutator{
				data: &sessiondata.SessionData{},
			},
		}

		// Create runParams
		runParams := runParams{
			p: p,
		}

		// Test Next
		ok, err := n.Next(runParams)
		if err != nil {
			t.Errorf("Next should not return error, got %v", err)
		}
		if !ok {
			t.Error("Next should return true for one row")
		}

		// Test Next again (should return false)
		ok, err = n.Next(runParams)
		if err != nil {
			t.Errorf("Next should not return error, got %v", err)
		}
		if ok {
			t.Error("Next should return false for second call")
		}
	})

	// Test case 2: Zero rows
	t.Run("Zero rows", func(t *testing.T) {
		// Create mock rows with zero rows
		mock := &mockRows{
			values: [][]tree.Datum{},
			index:  0,
		}

		// Create selectIntoNode
		n := &selectIntoNode{
			rows: mock,
			vars: []string{"var1"},
			end:  false,
		}

		// Create a planner
		p := &planner{}

		// Create runParams
		runParams := runParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(runParams)
		if err == nil {
			t.Error("Next should return error for zero rows")
		}
	})

	// Test case 3: More than one row
	t.Run("More than one row", func(t *testing.T) {
		// Create mock rows with two rows
		mock := &mockRows{
			values: [][]tree.Datum{
				{tree.NewDInt(1)},
				{tree.NewDInt(2)},
			},
			index: 0,
		}

		// Create selectIntoNode
		n := &selectIntoNode{
			rows: mock,
			vars: []string{"var1"},
			end:  false,
		}

		// Create a planner
		p := &planner{}

		// Create runParams
		runParams := runParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(runParams)
		if err == nil {
			t.Error("Next should return error for more than one row")
		}
	})

	// Test case 4: Column count mismatch
	t.Run("Column count mismatch", func(t *testing.T) {
		// Create mock rows with one row and one column
		mock := &mockRows{
			values: [][]tree.Datum{
				{tree.NewDInt(1)},
			},
			index: 0,
		}

		// Create selectIntoNode with two variables
		n := &selectIntoNode{
			rows: mock,
			vars: []string{"var1", "var2"},
			end:  false,
		}

		// Create a planner
		p := &planner{}

		// Create runParams
		runParams := runParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(runParams)
		if err == nil {
			t.Error("Next should return error for column count mismatch")
		}
	})

	// Test case 5: Test startExec
	t.Run("startExec", func(t *testing.T) {
		// Create selectIntoNode
		n := &selectIntoNode{}

		// Create runParams
		runParams := runParams{}

		// Test startExec
		err := n.startExec(runParams)
		if err != nil {
			t.Errorf("startExec should not return error, got %v", err)
		}
	})

	// Test case 6: Test Values
	t.Run("Values", func(t *testing.T) {
		// Create selectIntoNode
		n := &selectIntoNode{}

		// Test Values
		values := n.Values()
		if values != nil {
			t.Error("Values should return nil")
		}
	})

	// Test case 7: Test Close
	t.Run("Close", func(t *testing.T) {
		// Create mock rows
		mock := &mockRows{}

		// Create selectIntoNode
		n := &selectIntoNode{
			rows: mock,
		}

		// Test Close
		n.Close(context.Background())
		// No error expected
	})
}
