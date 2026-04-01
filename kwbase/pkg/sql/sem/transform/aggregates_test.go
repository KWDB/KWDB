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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"github.com/stretchr/testify/require"
)

func init() {
	if tree.FunDefs == nil {
		tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	}
	tree.FunDefs["sum"] = &tree.FunctionDefinition{
		Name: "sum",
		FunctionProperties: tree.FunctionProperties{
			Class: tree.AggregateClass,
		},
	}
	tree.FunDefs["lower"] = &tree.FunctionDefinition{
		Name: "lower",
		FunctionProperties: tree.FunctionProperties{
			Class: tree.NormalClass,
		},
	}
}

func TestIsAggregateVisitorInterface(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	require.Implements(t, (*tree.Visitor)(nil), v)
}

func TestIsAggregateVisitorVisitPost(t *testing.T) {
	v := &IsAggregateVisitor{}
	expr := tree.NewDInt(1)
	result := v.VisitPost(expr)
	require.Equal(t, expr, result)
}

func TestIsAggregateVisitorVisitPreSubquery(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	subq := &tree.Subquery{
		Select: &tree.SelectClause{},
	}
	recurse, newExpr := v.VisitPre(subq)
	require.False(t, recurse)
	require.Equal(t, subq, newExpr)
	require.False(t, v.Aggregated)
}

func TestIsAggregateVisitorVisitPreOtherExpr(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	expr := tree.NewDInt(1)
	recurse, newExpr := v.VisitPre(expr)
	require.True(t, recurse)
	require.Equal(t, expr, newExpr)
	require.False(t, v.Aggregated)
}

func TestIsAggregateVisitorAggregateFuncResolveError(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	funcExpr := &tree.FuncExpr{
		Func: tree.ResolvableFunctionReference{
			FunctionReference: &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{"nonexistent_function_xyz"},
			},
		},
	}
	recurse, newExpr := v.VisitPre(funcExpr)
	require.False(t, recurse)
	require.Equal(t, funcExpr, newExpr)
	require.False(t, v.Aggregated)
}

func TestIsAggregateVisitorVisitPreWindowFunc(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	funcExpr := &tree.FuncExpr{
		Func:      tree.WrapFunction("sum"),
		WindowDef: &tree.WindowDef{},
	}
	recurse, newExpr := v.VisitPre(funcExpr)
	require.True(t, recurse)
	require.Equal(t, funcExpr, newExpr)
	require.False(t, v.Aggregated)
}

func TestIsAggregateVisitorVisitPreAggregateFunc(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	funcExpr := &tree.FuncExpr{
		Func: tree.WrapFunction("sum"),
	}
	recurse, newExpr := v.VisitPre(funcExpr)
	require.False(t, recurse)
	require.Equal(t, funcExpr, newExpr)
	require.True(t, v.Aggregated)
}

func TestIsAggregateVisitorVisitPreNormalFunc(t *testing.T) {
	v := &IsAggregateVisitor{
		Aggregated: false,
		searchPath: sessiondata.MakeSearchPath([]string{"pg_catalog"}),
	}
	funcExpr := &tree.FuncExpr{
		Func: tree.WrapFunction("lower"),
	}
	recurse, newExpr := v.VisitPre(funcExpr)
	require.True(t, recurse)
	require.Equal(t, funcExpr, newExpr)
	require.False(t, v.Aggregated)
}
