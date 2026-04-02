// Copyright 2017 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package transform

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"github.com/stretchr/testify/require"
)

func TestExprTransformAggregateInExprNilExpr(t *testing.T) {
	tc := &ExprTransformContext{}
	searchPath := sessiondata.MakeSearchPath([]string{"pg_catalog"})

	result := tc.AggregateInExpr(nil, searchPath)
	require.False(t, result)
}

func TestExprTransformIsAggregateVisitorInterface(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	require.Implements(t, (*tree.Visitor)(nil), v)
}

func TestExprTransformNormalizeExprSkipNormalize(t *testing.T) {
	tc := &ExprTransformContext{}
	evalCtx := &tree.EvalContext{SkipNormalize: true}
	expr := tree.NewDInt(1)

	result, err := tc.NormalizeExpr(evalCtx, expr)
	require.NoError(t, err)
	require.Equal(t, expr, result)
}

func TestExprTransformNormalizeExprNormal(t *testing.T) {
	tc := &ExprTransformContext{}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	expr := tree.NewDInt(1)

	result, err := tc.NormalizeExpr(&evalCtx, expr)
	require.NoError(t, err)
	require.Equal(t, expr, result)
}

func TestExprTransformAggregateInExpr(t *testing.T) {
	tc := &ExprTransformContext{}
	searchPath := sessiondata.MakeSearchPath([]string{"pg_catalog"})

	// Test 1: no aggregate
	expr1 := tree.NewDInt(1)
	require.False(t, tc.AggregateInExpr(expr1, searchPath))

	// Test 2: with aggregate
	expr2 := &tree.FuncExpr{
		Func: tree.WrapFunction("sum"),
	}
	require.True(t, tc.AggregateInExpr(expr2, searchPath))
}
