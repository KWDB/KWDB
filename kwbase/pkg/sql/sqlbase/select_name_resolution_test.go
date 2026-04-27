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

package sqlbase_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a test DataSourceInfo for name resolution
func createTestDataSourceInfoForNameResolution() *sqlbase.DataSourceInfo {
	return &sqlbase.DataSourceInfo{
		SourceColumns: []sqlbase.ResultColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "value"},
		},
		SourceAlias: tree.MakeTableNameWithSchema("test_db", "public", "test_table"),
	}
}

// testIndexedVarContainer implements tree.IndexedVarContainer for testing
type testIndexedVarContainer struct {
	vars []tree.Datum
}

func (c *testIndexedVarContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	return c.vars[idx], nil
}

func (c *testIndexedVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return types.Int
}

func (c *testIndexedVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// Helper function to create a test IndexedVarHelper
func createTestIndexedVarHelper() tree.IndexedVarHelper {
	container := &testIndexedVarContainer{
		vars: []tree.Datum{tree.NewDInt(0), tree.NewDString("name"), tree.NewDString("value")},
	}
	return tree.MakeIndexedVarHelper(container, 3)
}

func TestResolveNames_VisitPre(t *testing.T) {
	sourceInfo := createTestDataSourceInfoForNameResolution()
	ivarHelper := createTestIndexedVarHelper()
	searchPath := sessiondata.SearchPath{}

	t.Run("ColumnItem resolution", func(t *testing.T) {
		colItem := &tree.ColumnItem{
			ColumnName: tree.Name("id"),
		}

		result, foundDependentVars, err := sqlbase.ResolveNames(colItem, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)

		ivar, ok := result.(*tree.IndexedVar)
		assert.True(t, ok)
		assert.Equal(t, 0, ivar.Idx)
	})

	t.Run("IndexedVar in expression", func(t *testing.T) {
		ivar := &tree.IndexedVar{Idx: 1}

		result, foundDependentVars, err := sqlbase.ResolveNames(ivar, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)
		_, ok := result.(*tree.IndexedVar)
		assert.True(t, ok)
	})
}

func TestResolveNames(t *testing.T) {
	sourceInfo := createTestDataSourceInfoForNameResolution()
	ivarHelper := createTestIndexedVarHelper()
	searchPath := sessiondata.SearchPath{}

	t.Run("Resolve simple column reference", func(t *testing.T) {
		colItem := &tree.ColumnItem{
			ColumnName: tree.Name("name"),
		}

		result, foundDependentVars, err := sqlbase.ResolveNames(colItem, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)

		ivar, ok := result.(*tree.IndexedVar)
		assert.True(t, ok)
		assert.Equal(t, 1, ivar.Idx)
	})

	t.Run("Resolve function with star", func(t *testing.T) {
		funcExpr := &tree.FuncExpr{
			Func: tree.WrapFunction("count"),
			Exprs: []tree.Expr{
				tree.UnqualifiedStar{},
			},
		}

		result, foundDependentVars, err := sqlbase.ResolveNames(funcExpr, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		// count function is of agg, and foundDependentVars should be true
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)

		_, ok := result.(*tree.FuncExpr)
		assert.True(t, ok)
	})

	t.Run("Resolve complex expression", func(t *testing.T) {
		binaryExpr := &tree.BinaryExpr{
			Operator: tree.Plus,
			Left: &tree.ColumnItem{
				ColumnName: tree.Name("id"),
			},
			Right: tree.NewDInt(1),
		}

		result, foundDependentVars, err := sqlbase.ResolveNames(binaryExpr, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)

		_, ok := result.(*tree.BinaryExpr)
		assert.True(t, ok)
	})

	t.Run("Resolve non-existent column", func(t *testing.T) {
		colItem := &tree.ColumnItem{
			ColumnName: tree.Name("non_existent"),
		}

		result, foundDependentVars, err := sqlbase.ResolveNames(colItem, sourceInfo, ivarHelper, searchPath)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
		assert.False(t, foundDependentVars)
		assert.NotNil(t, result)
	})
}

func TestResolveNamesUsingVisitor(t *testing.T) {
	sourceInfo := createTestDataSourceInfoForNameResolution()
	ivarHelper := createTestIndexedVarHelper()
	searchPath := sessiondata.SearchPath{}

	t.Run("Using custom visitor", func(t *testing.T) {
		colItem := &tree.ColumnItem{
			ColumnName: tree.Name("value"),
		}

		var v sqlbase.NameResolutionVisitor

		result, foundDependentVars, err := sqlbase.ResolveNamesUsingVisitor(&v, colItem, sourceInfo, ivarHelper, searchPath)

		assert.NoError(t, err)
		assert.True(t, foundDependentVars)
		assert.NotNil(t, result)

		ivar, ok := result.(*tree.IndexedVar)
		assert.True(t, ok)
		assert.Equal(t, 2, ivar.Idx)
	})
}
