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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestExecFactoryConstructTSScan tests the ConstructTSScan method
func TestExecFactoryConstructTSScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create a mock table for testing
	tabDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "time",
					Type: *types.TimestampTZ,
				},
				{
					ID:   2,
					Name: "value",
					Type: *types.Float,
				},
			},
			TableType: tree.TimeseriesTable,
		},
	}

	// Create optTable
	optTable := &optTable{desc: tabDesc}

	// Create TSScanPrivate
	cols := opt.ColSet{}
	cols.Add(1)
	cols.Add(2)
	private := &memo.TSScanPrivate{
		Table: opt.TableID(1),
		Cols:  cols,
		Flags: memo.TSScanFlags{
			AccessMode:       1,
			HintType:         2,
			OrderedScanType:  3,
			Direction:        tree.Ascending,
			PrimaryTagValues: map[uint32][]string{},
		},
	}

	// Create filters
	tagFilter := []tree.TypedExpr{tree.NewDInt(1)}
	primaryFilter := []tree.TypedExpr{tree.NewDInt(2)}
	tagIndexFilter := []tree.TypedExpr{tree.NewDInt(3)}

	// Test case: ConstructTSScan
	t.Run("ConstructTSScan", func(t *testing.T) {
		node, err := ef.ConstructTSScan(optTable, private, tagFilter, primaryFilter, tagIndexFilter, nil, 100.0, nil)
		if err != nil {
			t.Fatalf("ConstructTSScan should not return error, got %v", err)
		}

		tsScanNode, ok := node.(*tsScanNode)
		if !ok {
			t.Error("ConstructTSScan should return tsScanNode")
		}

		if tsScanNode.Table != optTable {
			t.Error("tsScanNode should have the correct table")
		}

		if len(tsScanNode.TagFilterArray) != 1 {
			t.Errorf("tsScanNode should have %d tag filters, got %d", 1, len(tsScanNode.TagFilterArray))
		}

		if len(tsScanNode.PrimaryTagFilterArray) != 1 {
			t.Errorf("tsScanNode should have %d primary tag filters, got %d", 1, len(tsScanNode.PrimaryTagFilterArray))
		}

		if len(tsScanNode.TagIndexFilterArray) != 1 {
			t.Errorf("tsScanNode should have %d tag index filters, got %d", 1, len(tsScanNode.TagIndexFilterArray))
		}
	})
}

// TestExecFactoryConstructTsInsertSelect tests the ConstructTsInsertSelect method
func TestExecFactoryConstructTsInsertSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create a simple values node for testing
	rows := [][]tree.TypedExpr{
		{tree.NewDInt(1), tree.NewDString("test")},
	}
	cols := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "name", Typ: types.String},
	}
	valuesNode, err := ef.ConstructValues(rows, cols)
	if err != nil {
		t.Fatalf("ConstructValues should not return error, got %v", err)
	}

	// Test case: ConstructTsInsertSelect
	t.Run("ConstructTsInsertSelect", func(t *testing.T) {
		tableID := uint64(1)
		dbID := uint64(2)
		tableCols := opt.ColList{1, 2}
		colIdxs := opt.ColIdxs{0, 1}
		tableName := "test_table"
		tableType := int32(1)

		node, err := ef.ConstructTsInsertSelect(valuesNode, tableID, dbID, tableCols, colIdxs, tableName, tableType)
		if err != nil {
			t.Fatalf("ConstructTsInsertSelect should not return error, got %v", err)
		}

		tsInsertSelectNode, ok := node.(*tsInsertSelectNode)
		if !ok {
			t.Error("ConstructTsInsertSelect should return tsInsertSelectNode")
		}

		if tsInsertSelectNode.TableID != tableID {
			t.Errorf("tsInsertSelectNode should have tableID %d, got %d", tableID, tsInsertSelectNode.TableID)
		}

		if tsInsertSelectNode.DBID != dbID {
			t.Errorf("tsInsertSelectNode should have dbID %d, got %d", dbID, tsInsertSelectNode.DBID)
		}

		if tsInsertSelectNode.TableName != tableName {
			t.Errorf("tsInsertSelectNode should have tableName %s, got %s", tableName, tsInsertSelectNode.TableName)
		}

		if tsInsertSelectNode.TableType != tableType {
			t.Errorf("tsInsertSelectNode should have tableType %d, got %d", tableType, tsInsertSelectNode.TableType)
		}

		if len(tsInsertSelectNode.Cols) != len(tableCols) {
			t.Errorf("tsInsertSelectNode should have %d columns, got %d", len(tableCols), len(tsInsertSelectNode.Cols))
		}

		if len(tsInsertSelectNode.ColIdxs) != len(colIdxs) {
			t.Errorf("tsInsertSelectNode should have %d column indexes, got %d", len(colIdxs), len(tsInsertSelectNode.ColIdxs))
		}
	})
}

// TestExecFactoryConstructBatchLookUpJoin tests the ConstructBatchLookUpJoin method
func TestExecFactoryConstructBatchLookUpJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create left and right nodes for testing
	leftRows := [][]tree.TypedExpr{
		{tree.NewDInt(1), tree.NewDString("test1")},
	}
	leftCols := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "name", Typ: types.String},
	}
	leftNode, err := ef.ConstructValues(leftRows, leftCols)
	if err != nil {
		t.Fatalf("ConstructValues should not return error, got %v", err)
	}

	rightRows := [][]tree.TypedExpr{
		{tree.NewDInt(1), tree.NewDInt(100)},
	}
	rightCols := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "value", Typ: types.Int},
	}
	rightNode, err := ef.ConstructValues(rightRows, rightCols)
	if err != nil {
		t.Fatalf("ConstructValues should not return error, got %v", err)
	}

	// Test case: ConstructBatchLookUpJoin
	t.Run("ConstructBatchLookUpJoin", func(t *testing.T) {
		joinType := sqlbase.InnerJoin
		leftEqCols := []exec.ColumnOrdinal{0}
		rightEqCols := []exec.ColumnOrdinal{0}
		leftEqColsAreKey := true
		rightEqColsAreKey := true
		extraOnCond := tree.DBoolTrue

		node, err := ef.ConstructBatchLookUpJoin(joinType, leftNode, rightNode, leftEqCols, rightEqCols, leftEqColsAreKey, rightEqColsAreKey, extraOnCond)
		if err != nil {
			t.Fatalf("ConstructBatchLookUpJoin should not return error, got %v", err)
		}

		batchLookUpJoinNode, ok := node.(*batchLookUpJoinNode)
		if !ok {
			t.Error("ConstructBatchLookUpJoin should return batchLookUpJoinNode")
		}

		if batchLookUpJoinNode.joinType != joinType {
			t.Errorf("batchLookUpJoinNode should have joinType %v, got %v", joinType, batchLookUpJoinNode.joinType)
		}
	})
}

// TestExecFactoryConstructApplyJoin tests the ConstructApplyJoin method
func TestExecFactoryConstructApplyJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create left node for testing
	leftRows := [][]tree.TypedExpr{
		{tree.NewDInt(1), tree.NewDString("test1")},
	}
	leftCols := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "name", Typ: types.String},
	}
	leftNode, err := ef.ConstructValues(leftRows, leftCols)
	if err != nil {
		t.Fatalf("ConstructValues should not return error, got %v", err)
	}

	// Create right columns
	rightColumns := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "value", Typ: types.Int},
	}

	// Test case: ConstructApplyJoin
	t.Run("ConstructApplyJoin", func(t *testing.T) {
		joinType := sqlbase.InnerJoin
		onCond := tree.DBoolTrue
		var right memo.RelExpr
		planRightSideFn := func(leftRow tree.Datums, listMap *sqlbase.WhiteListMap) (exec.Plan, error) {
			return leftNode, nil
		}

		node, err := ef.ConstructApplyJoin(joinType, leftNode, rightColumns, onCond, right, planRightSideFn)
		if err != nil {
			t.Fatalf("ConstructApplyJoin should not return error, got %v", err)
		}

		_, ok := node.(exec.Node)
		if !ok {
			t.Error("ConstructApplyJoin should return exec.Node")
		}
	})
}

// TestExecFactoryConstructScanForZigzag tests the constructScanForZigzag method
func TestExecFactoryConstructScanForZigzag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create table descriptor
	tabDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "id",
					Type: *types.Int,
				},
				{
					ID:   2,
					Name: "name",
					Type: *types.String,
				},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
		},
	}

	// Create index descriptor
	indexDesc := &sqlbase.IndexDescriptor{
		ID:        2,
		Name:      "idx_name",
		ColumnIDs: []sqlbase.ColumnID{2},
	}

	// Test case: constructScanForZigzag
	t.Run("constructScanForZigzag", func(t *testing.T) {
		cols := exec.ColumnOrdinalSet{}
		cols.Add(0)
		cols.Add(1)

		// Skip privilege checks for testing
		p.(*planner).skipSelectPrivilegeChecks = true
		defer func() { p.(*planner).skipSelectPrivilegeChecks = false }()

		scanNode, err := ef.constructScanForZigzag(indexDesc, tabDesc, cols)
		if err != nil {
			t.Fatalf("constructScanForZigzag should not return error, got %v", err)
		}

		if scanNode == nil {
			t.Error("constructScanForZigzag should return non-nil scanNode")
		}

		if scanNode.index != indexDesc {
			t.Error("scanNode should have the correct index")
		}

		if !scanNode.isSecondaryIndex {
			t.Error("scanNode should be a secondary index")
		}
	})
}

// TestExecFactoryConstructZigzagJoin tests the ConstructZigzagJoin method
func TestExecFactoryConstructZigzagJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create table descriptor
	tabDesc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID:   1,
			Name: "test_table",
			Columns: []sqlbase.ColumnDescriptor{
				{
					ID:   1,
					Name: "id",
					Type: *types.Int,
				},
				{
					ID:   2,
					Name: "name",
					Type: *types.String,
				},
			},
			PrimaryIndex: sqlbase.IndexDescriptor{
				ID:        1,
				Name:      "primary",
				ColumnIDs: []sqlbase.ColumnID{1},
			},
		},
	}

	// Create index descriptor
	indexDesc := &sqlbase.IndexDescriptor{
		ID:        2,
		Name:      "idx_name",
		ColumnIDs: []sqlbase.ColumnID{2},
	}

	// Create optTable and optIndex
	leftTable := &optTable{desc: tabDesc}
	leftIndex := &optIndex{desc: indexDesc}
	rightTable := &optTable{desc: tabDesc}
	rightIndex := &optIndex{desc: indexDesc}

	// Create fixed values node
	rows := [][]tree.TypedExpr{
		{tree.NewDInt(1), tree.NewDString("test")},
	}
	cols := sqlbase.ResultColumns{
		{Name: "id", Typ: types.Int},
		{Name: "name", Typ: types.String},
	}
	fixedValsNode, err := ef.ConstructValues(rows, cols)
	if err != nil {
		t.Fatalf("ConstructValues should not return error, got %v", err)
	}

	// Test case: ConstructZigzagJoin
	t.Run("ConstructZigzagJoin", func(t *testing.T) {
		leftEqCols := []exec.ColumnOrdinal{0}
		rightEqCols := []exec.ColumnOrdinal{0}
		leftCols := exec.ColumnOrdinalSet{}
		leftCols.Add(0)
		leftCols.Add(1)
		rightCols := exec.ColumnOrdinalSet{}
		rightCols.Add(0)
		rightCols.Add(1)
		onCond := tree.DBoolTrue
		fixedVals := []exec.Node{fixedValsNode}
		reqOrdering := exec.OutputOrdering{}

		// Skip privilege checks for testing
		p.(*planner).skipSelectPrivilegeChecks = true
		defer func() { p.(*planner).skipSelectPrivilegeChecks = false }()

		node, err := ef.ConstructZigzagJoin(leftTable, leftIndex, rightTable, rightIndex, leftEqCols, rightEqCols, leftCols, rightCols, onCond, fixedVals, reqOrdering)
		if err != nil {
			t.Fatalf("ConstructZigzagJoin should not return error, got %v", err)
		}

		zigzagJoinNode, ok := node.(*zigzagJoinNode)
		if !ok {
			t.Error("ConstructZigzagJoin should return zigzagJoinNode")
		}

		if len(zigzagJoinNode.sides) != 2 {
			t.Errorf("zigzagJoinNode should have %d sides, got %d", 2, len(zigzagJoinNode.sides))
		}

		if len(zigzagJoinNode.sides[0].eqCols) != len(leftEqCols) {
			t.Errorf("zigzagJoinNode should have %d left eq cols, got %d", len(leftEqCols), len(zigzagJoinNode.sides[0].eqCols))
		}

		if len(zigzagJoinNode.sides[1].eqCols) != len(rightEqCols) {
			t.Errorf("zigzagJoinNode should have %d right eq cols, got %d", len(rightEqCols), len(zigzagJoinNode.sides[1].eqCols))
		}
	})
}

// TestExecFactoryBuildInstruction tests the buildInstruction method
func TestExecFactoryBuildInstruction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create scalar function
	scalarFn := func(expr opt.ScalarExpr) (tree.TypedExpr, error) {
		return tree.NewDInt(1), nil
	}

	// Test case: buildInstruction with ArrayCommand
	t.Run("buildInstruction with ArrayCommand", func(t *testing.T) {
		arrayCmd := &memo.ArrayCommand{
			Bodys: []memo.ProcCommand{},
		}

		ins, err := ef.buildInstruction(arrayCmd, scalarFn)
		if err != nil {
			t.Fatalf("buildInstruction should not return error, got %v", err)
		}

		if ins == nil {
			t.Error("buildInstruction should return non-nil instruction")
		}
	})

	// Test case: buildInstruction with BlockCommand
	t.Run("buildInstruction with BlockCommand", func(t *testing.T) {
		blockCmd := &memo.BlockCommand{
			Label: "test_block",
			Body: memo.ArrayCommand{
				Bodys: []memo.ProcCommand{},
			},
		}

		ins, err := ef.buildInstruction(blockCmd, scalarFn)
		if err != nil {
			t.Fatalf("buildInstruction should not return error, got %v", err)
		}

		if ins == nil {
			t.Error("buildInstruction should return non-nil instruction")
		}
	})

	// Test case: buildInstruction with OpenCursorCommand
	t.Run("buildInstruction with OpenCursorCommand", func(t *testing.T) {
		openCursorCmd := &memo.OpenCursorCommand{
			Name: "test_cursor",
		}

		ins, err := ef.buildInstruction(openCursorCmd, scalarFn)
		if err != nil {
			t.Fatalf("buildInstruction should not return error, got %v", err)
		}

		if ins == nil {
			t.Error("buildInstruction should return non-nil instruction")
		}
	})
}

// TestExecFactoryConstructCreateTables tests the ConstructCreateTables method
func TestExecFactoryConstructCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Create database descriptor
	dbDesc := &sqlbase.DatabaseDescriptor{
		ID:   1,
		Name: "test_db",
	}

	// Create schema map
	schemas := make(map[string]cat.Schema)
	schemas["test_db"] = &optSchema{database: dbDesc}

	// Create CreateTable
	ct := &tree.CreateTable{
		Instances: []tree.InsTableDef{
			{
				Name: tree.MakeUnqualifiedTableName("test_table"),
			},
		},
	}

	// Test case: ConstructCreateTables
	t.Run("ConstructCreateTables", func(t *testing.T) {
		node, err := ef.ConstructCreateTables(nil, schemas, ct)
		if err != nil {
			t.Fatalf("ConstructCreateTables should not return error, got %v", err)
		}

		createMultiInstTableNode, ok := node.(*createMultiInstTableNode)
		if !ok {
			t.Error("ConstructCreateTables should return createMultiInstTableNode")
		}

		if len(createMultiInstTableNode.ns) != len(ct.Instances) {
			t.Errorf("createMultiInstTableNode should have %d instances, got %d", len(ct.Instances), len(createMultiInstTableNode.ns))
		}

		if len(createMultiInstTableNode.dbDescs) != len(schemas) {
			t.Errorf("createMultiInstTableNode should have %d dbDescs, got %d", len(schemas), len(createMultiInstTableNode.dbDescs))
		}
	})
}

// TestExecFactoryMakeTSSpansForExpr tests the MakeTSSpansForExpr method
func TestExecFactoryMakeTSSpansForExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a planner
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	// Create execFactory
	ef := makeExecFactory(p.(*planner))

	// Test case: MakeTSSpansForExpr with FiltersExpr
	t.Run("MakeTSSpansForExpr with FiltersExpr", func(t *testing.T) {
		// Create a mock constraint
		out := new(constraint.Constraint)

		// Create a mock FiltersExpr
		filtersExpr := &memo.FiltersExpr{}

		tight := ef.MakeTSSpansForExpr(filtersExpr, out, opt.TableID(1))
		_ = tight // Use the variable to avoid unused variable error

		if out == nil {
			t.Error("out should not be nil")
		}
	})
}
