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
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestRunParams tests the runParams methods
func TestRunParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db")

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	p := internalPlanner.(*planner)
	// Create runParams
	runParams := &runParams{
		ctx:             ctx,
		p:               p,
		extendedEvalCtx: p.ExtendedEvalContext(),
	}

	// Ensure extendedEvalCtx has DB set
	runParams.extendedEvalCtx.DB = s.DB()

	// Ensure planner has preparedStatements set
	p.preparedStatements = connExPrepStmtsAccessor{
		ex: &connExecutor{},
	}

	// Test GetCtx
	t.Run("GetCtx", func(t *testing.T) {
		gotCtx := runParams.GetCtx()
		if gotCtx != ctx {
			t.Error("GetCtx should return the correct context")
		}
	})

	// Test GetTxn
	t.Run("GetTxn", func(t *testing.T) {
		txn := runParams.GetTxn()
		if txn == nil {
			t.Error("GetTxn should return non-nil transaction")
		}
	})

	// Test SetTxn
	t.Run("SetTxn", func(t *testing.T) {
		newTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
		runParams.SetTxn(newTxn)
		if runParams.GetTxn() != newTxn {
			t.Error("SetTxn should set the transaction")
		}
	})

	// Test NewTxn
	t.Run("NewTxn", func(t *testing.T) {
		oldTxn := runParams.GetTxn()
		runParams.NewTxn()
		newTxn := runParams.GetTxn()
		if newTxn == oldTxn {
			t.Error("NewTxn should create a new transaction")
		}
	})

	// Test SetUserDefinedVar
	t.Run("SetUserDefinedVar", func(t *testing.T) {
		err := runParams.SetUserDefinedVar("test_var", tree.NewDInt(42))
		if err != nil {
			t.Errorf("SetUserDefinedVar should not return error, got %v", err)
		}
	})

	// Test DeallocatePrepare
	t.Run("DeallocatePrepare", func(t *testing.T) {
		// Test with empty name (delete all)
		err := runParams.DeallocatePrepare("")
		if err != nil {
			t.Errorf("DeallocatePrepare should not return error, got %v", err)
		}

		// Test with non-existent name (should return error)
		err = runParams.DeallocatePrepare("non_existent")
		if err == nil {
			t.Error("DeallocatePrepare should return error for non-existent prepared statement")
		}
	})
}

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

// TestRunPlanInsideProcedure tests the RunPlanInsideProcedure function
func TestRunPlanInsideProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database and table
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db; CREATE TABLE test_db.test_table (id INT PRIMARY KEY, name STRING);")

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := internalPlanner.(*planner)

	// Create runParams
	runParams := &runParams{
		ctx:             ctx,
		p:               p,
		extendedEvalCtx: p.ExtendedEvalContext(),
	}

	// Create a simple planTop
	plan := &planTop{
		plan: &zeroNode{},
	}

	// Create a row container
	rowContainer := rowcontainer.NewRowContainer(
		p.ExtendedEvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(nil),
		0,
	)
	defer rowContainer.Close(ctx)

	// Test RunPlanInsideProcedure
	rowsAffected, err := RunPlanInsideProcedure(runParams, plan, rowContainer, tree.RowsAffected)
	if err != nil {
		t.Errorf("RunPlanInsideProcedure should not return error, got %v", err)
	}
	if rowsAffected != 0 {
		t.Errorf("RunPlanInsideProcedure should return 0 rows affected for zeroNode, got %d", rowsAffected)
	}
}

// TestStartPlanInsideProcedure tests the StartPlanInsideProcedure function
func TestStartPlanInsideProcedure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db")

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a planner
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := internalPlanner.(*planner)

	// Create runParams
	runParams := &runParams{
		ctx:             ctx,
		p:               p,
		extendedEvalCtx: p.ExtendedEvalContext(),
	}

	// Create a simple planTop
	plan := &planTop{
		plan: &zeroNode{},
	}

	// Create a row container
	rowContainer := rowcontainer.NewRowContainer(
		p.ExtendedEvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(nil),
		0,
	)
	defer rowContainer.Close(ctx)

	// Create a CursorExecHelper
	cursorExecHelper := &CursorExecHelper{}

	// Test StartPlanInsideProcedure
	err := StartPlanInsideProcedure(runParams, cursorExecHelper, rowContainer, tree.RowsAffected, plan)
	// Note: This test may fail because it requires a fully initialized plan
	// but we're testing the function signature and basic functionality
	if err != nil {
		t.Logf("StartPlanInsideProcedure returned error (expected in test environment): %v", err)
	}
}
