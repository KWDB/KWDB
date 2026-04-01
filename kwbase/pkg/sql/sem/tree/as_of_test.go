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

package tree_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAsOfFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		expr     string
		expected string
	}{
		{`AS OF SYSTEM TIME '2023-01-01 12:00:00'`, `AS OF SYSTEM TIME '2023-01-01 12:00:00'`},
		{`AS OF SYSTEM TIME '-10s'`, `AS OF SYSTEM TIME '-10s'`},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			stmt, err := parser.ParseOne("SELECT * FROM t " + tt.expr)
			require.NoError(t, err)

			sel, ok := stmt.AST.(*tree.Select)
			require.True(t, ok)

			sc, ok := sel.Select.(*tree.SelectClause)
			require.True(t, ok)

			require.NotNil(t, sc.From.AsOf.Expr)

			formatted := tree.AsStringWithFlags(&sc.From.AsOf, tree.FmtSimple)
			require.Equal(t, tt.expected, formatted)
		})
	}
}

func TestEvalAsOfTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	semaCtx := tree.MakeSemaContext()

	tests := []struct {
		name        string
		expr        tree.Expr
		expectError bool
	}{
		{
			name:        "valid timestamp string",
			expr:        tree.NewDString("2023-01-01 12:00:00"),
			expectError: false,
		},
		{
			name:        "valid interval string",
			expr:        tree.NewDString("-10s"),
			expectError: false,
		},
		{
			name:        "valid expression type",
			expr:        tree.NewDInt(123),
			expectError: false,
		},
		{
			name:        "non-constant expression",
			expr:        tree.NewUnresolvedName("non_const_col"),
			expectError: true,
		},
		{
			name:        "invalid timestamp format",
			expr:        tree.NewDString("invalid_timestamp"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asOf := tree.AsOfClause{Expr: tt.expr}
			// When passing a raw int expression like NewDInt, TypeCheck will not implicitly
			// fail to cast it to types.String in this test context unless we use actual
			// non-string literal parsing. Let's just verify EvalAsOfTimestamp errors.

			// For testing invalid type, we can use a boolean literal which doesn't implicitly cast to string.
			if tt.name == "invalid expression type" {
				asOf.Expr = tree.MakeDBool(true)
			}

			ts, err := tree.EvalAsOfTimestamp(asOf, &semaCtx, evalCtx)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ts)
			}
		})
	}

	// Test FollowerReadTimestampFunction
	t.Run("follower_read_timestamp", func(t *testing.T) {
		// Mock a FuncExpr
		expr, err := parser.ParseExpr("experimental_follower_read_timestamp()")
		require.NoError(t, err)

		asOf := tree.AsOfClause{Expr: expr}
		_, err = tree.EvalAsOfTimestamp(asOf, &semaCtx, evalCtx)
		// It will fail because experimental_follower_read_timestamp is a CCL only feature
		// but the code path checking for validity will still be covered.
		require.Error(t, err)
		require.Contains(t, err.Error(), "only available in ccl distribution")
	})

	// Test Invalid FuncExpr
	t.Run("invalid_function", func(t *testing.T) {
		expr, err := parser.ParseExpr("now()")
		require.NoError(t, err)

		asOf := tree.AsOfClause{Expr: expr}
		_, err = tree.EvalAsOfTimestamp(asOf, &semaCtx, evalCtx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "only constant expressions")
	})
}
