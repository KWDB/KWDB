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

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// mockRows is a mock implementation of PlanNode for testing selectIntoNode
// mockRows 是一个模拟的 PlanNode 实现，用于测试 selectIntoNode
type mockRows struct {
	values [][]tree.Datum
	index  int
}

func (m *mockRows) StartExec(params RunParams) error {
	return nil
}

func (m *mockRows) Next(params RunParams) (bool, error) {
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

		// Create a GenericPlanner with sessionDataMutator
		p := &GenericPlanner{
			sessionDataMutator: &sessionDataMutator{
				data: &sessiondata.SessionData{},
			},
		}

		// Create RunParams
		RunParams := RunParams{
			p: p,
		}

		// Test Next
		ok, err := n.Next(RunParams)
		if err != nil {
			t.Errorf("Next should not return error, got %v", err)
		}
		if !ok {
			t.Error("Next should return true for one row")
		}

		// Test Next again (should return false)
		ok, err = n.Next(RunParams)
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

		// Create a GenericPlanner
		p := &GenericPlanner{}

		// Create RunParams
		RunParams := RunParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(RunParams)
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

		// Create a GenericPlanner
		p := &GenericPlanner{}

		// Create RunParams
		RunParams := RunParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(RunParams)
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

		// Create a GenericPlanner
		p := &GenericPlanner{}

		// Create RunParams
		RunParams := RunParams{
			p: p,
		}

		// Test Next (should return error)
		_, err := n.Next(RunParams)
		if err == nil {
			t.Error("Next should return error for column count mismatch")
		}
	})

	// Test case 5: Test StartExec
	t.Run("StartExec", func(t *testing.T) {
		// Create selectIntoNode
		n := &selectIntoNode{}

		// Create RunParams
		RunParams := RunParams{}

		// Test StartExec
		err := n.StartExec(RunParams)
		if err != nil {
			t.Errorf("StartExec should not return error, got %v", err)
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

// TestResolveNames tests the resolveNames method
func TestResolveNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a GenericPlanner
	// Start a test server
	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db")

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a GenericPlanner with admin privileges
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser, // Root user has admin privileges
		&MemoryMetrics{},
		&execCfg,
	)
	GenericPlanner := p.(*GenericPlanner)
	defer cleanup()

	// Create a simple expression
	expr := tree.NewDInt(1)

	// Test resolveNames with nil source and ivarHelper
	resolvedExpr, hasColumns, err := GenericPlanner.resolveNames(expr, nil, tree.IndexedVarHelper{})
	if err != nil {
		t.Errorf("resolveNames should not return error, got %v", err)
	}
	if resolvedExpr == nil {
		t.Error("resolveNames should return non-nil expression")
	}
	if hasColumns {
		t.Error("resolveNames should return false for hasColumns for simple expression")
	}

	// Test resolveNames with nil expr
	resolvedExpr, hasColumns, err = GenericPlanner.resolveNames(nil, nil, tree.IndexedVarHelper{})
	if err != nil {
		t.Errorf("resolveNames should not return error for nil expr, got %v", err)
	}
	if resolvedExpr != nil {
		t.Error("resolveNames should return nil for nil expr")
	}
	if hasColumns {
		t.Error("resolveNames should return false for hasColumns for nil expr")
	}
}
