// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGetRenderColName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sp := sessiondata.SearchPath{}

	testCases := []struct {
		target   SelectExpr
		expected string
		hasError bool
	}{
		{
			target: SelectExpr{
				Expr: &UnresolvedName{Parts: NameParts{"col1"}},
				As:   "alias",
			},
			expected: "alias",
			hasError: false,
		},
		{
			target: SelectExpr{
				Expr: &UnresolvedName{Parts: NameParts{"col1"}},
			},
			expected: "col1",
			hasError: false,
		},
		{
			target: SelectExpr{
				Expr: &ColumnItem{ColumnName: Name("col1")},
			},
			expected: "col1",
			hasError: false,
		},
		{
			target: SelectExpr{
				Expr: NewDInt(123),
			},
			expected: "?column?",
			hasError: false,
		},
	}

	for i, tc := range testCases {
		result, err := GetRenderColName(sp, tc.target)
		if tc.hasError {
			require.Error(t, err, "test case %d", i)
		} else {
			require.NoError(t, err, "test case %d", i)
			require.Equal(t, tc.expected, result, "test case %d", i)
		}
	}
}

func TestComputeColNameInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sp := sessiondata.SearchPath{}

	testCases := []struct {
		target   Expr
		expected string
		strength int
		hasError bool
	}{
		{
			target:   &UnresolvedName{Parts: NameParts{"col1"}},
			expected: "col1",
			strength: 2,
			hasError: false,
		},
		{
			target:   &UnresolvedName{Star: true},
			expected: "",
			strength: 0,
			hasError: false,
		},
		{
			target:   &ColumnItem{ColumnName: Name("col1")},
			expected: "col1",
			strength: 2,
			hasError: false,
		},
		{
			target:   &IndirectionExpr{Expr: &ColumnItem{ColumnName: Name("col1")}},
			expected: "col1",
			strength: 2,
			hasError: false,
		},
		{
			target:   &NullIfExpr{},
			expected: "nullif",
			strength: 2,
			hasError: false,
		},
		{
			target:   &IfExpr{},
			expected: "if",
			strength: 2,
			hasError: false,
		},
		{
			target:   &ParenExpr{Expr: &ColumnItem{ColumnName: Name("col1")}},
			expected: "col1",
			strength: 2,
			hasError: false,
		},
		{
			target:   &ArrayFlatten{},
			expected: "array",
			strength: 2,
			hasError: false,
		},
		{
			target:   &Subquery{Exists: true},
			expected: "exists",
			strength: 2,
			hasError: false,
		},
		{
			target:   &CaseExpr{},
			expected: "case",
			strength: 1,
			hasError: false,
		},
		{
			target:   &Array{},
			expected: "array",
			strength: 2,
			hasError: false,
		},
		{
			target:   &Tuple{Row: true},
			expected: "row",
			strength: 2,
			hasError: false,
		},
		{
			target:   &CoalesceExpr{},
			expected: "coalesce",
			strength: 2,
			hasError: false,
		},
		{
			target:   &IfErrExpr{},
			expected: "iserror",
			strength: 2,
			hasError: false,
		},
		{
			target:   &ColumnAccessExpr{ColName: "col1"},
			expected: "col1",
			strength: 2,
			hasError: false,
		},
		{
			target:   &UserDefinedVar{VarName: "var1"},
			expected: "var1",
			strength: 2,
			hasError: false,
		},
		{
			target:   MakeDBool(true),
			expected: "bool",
			strength: 1,
			hasError: false,
		},
		{
			target:   dNull{},
			expected: "null",
			strength: 1,
			hasError: false,
		},
	}

	for i, tc := range testCases {
		strength, result, err := ComputeColNameInternal(sp, tc.target)
		if tc.hasError {
			require.Error(t, err, "test case %d", i)
		} else {
			require.NoError(t, err, "test case %d", i)
			require.Equal(t, tc.strength, strength, "test case %d", i)
			require.Equal(t, tc.expected, result, "test case %d", i)
		}
	}
}

func TestComputeColNameInternalSubquery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sp := sessiondata.SearchPath{}

	testCases := []struct {
		selectStmt SelectStatement
		expected   string
		strength   int
		hasError   bool
	}{
		{
			selectStmt: &ValuesClause{Rows: []Exprs{{NewDInt(1)}}},
			expected:   "column1",
			strength:   2,
			hasError:   false,
		},
		{
			selectStmt: &ValuesClause{Rows: []Exprs{{NewDInt(1), NewDInt(2)}}},
			expected:   "",
			strength:   0,
			hasError:   false,
		},
		{
			selectStmt: &SelectClause{Exprs: []SelectExpr{{Expr: &ColumnItem{ColumnName: Name("col1")}}}},
			expected:   "col1",
			strength:   2,
			hasError:   false,
		},
		{
			selectStmt: &SelectClause{Exprs: []SelectExpr{{Expr: &ColumnItem{ColumnName: Name("col1")}, As: "alias"}}},
			expected:   "alias",
			strength:   2,
			hasError:   false,
		},
		{
			selectStmt: &SelectClause{Exprs: []SelectExpr{{Expr: &ColumnItem{ColumnName: Name("col1")}}, {Expr: &ColumnItem{ColumnName: Name("col2")}}}},
			expected:   "",
			strength:   0,
			hasError:   false,
		},
		{
			selectStmt: &ParenSelect{Select: &Select{Select: &SelectClause{Exprs: []SelectExpr{{Expr: &ColumnItem{ColumnName: Name("col1")}}}}}},
			expected:   "col1",
			strength:   2,
			hasError:   false,
		},
	}

	for i, tc := range testCases {
		strength, result, err := computeColNameInternalSubquery(sp, tc.selectStmt)
		if tc.hasError {
			require.Error(t, err, "test case %d", i)
		} else {
			require.NoError(t, err, "test case %d", i)
			require.Equal(t, tc.strength, strength, "test case %d", i)
			require.Equal(t, tc.expected, result, "test case %d", i)
		}
	}
}

func TestComputeCastName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		typ      *types.T
		expected string
	}{
		{
			typ:      types.Int,
			expected: "int8",
		},
		{
			typ:      types.String,
			expected: "text",
		},
		{
			typ:      types.Bool,
			expected: "bool",
		},
		{
			typ:      types.MakeArray(types.Int),
			expected: "int8",
		},
		{
			typ:      types.MakeArray(types.String),
			expected: "text",
		},
	}

	for i, tc := range testCases {
		result := computeCastName(tc.typ)
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
