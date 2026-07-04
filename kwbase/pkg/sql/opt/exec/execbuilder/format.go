// Copyright 2019 The Cockroach Authors.
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

package execbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// installFormatInterceptor registers the scalar formatting interceptor
// that converts scalar expressions into SQL text for EXPLAIN output.
func init() {
	memo.ScalarFmtInterceptor = formatScalarForExplain
}

// formatScalarForExplain is the registered interceptor for ExprFmtHideScalars.
// It attempts to execbuild scalar expressions and render them as SQL text,
// falling back to the default format when building is not possible.
func formatScalarForExplain(f *memo.ExprFmtCtx, scalar opt.ScalarExpr) string {
	if !isPurelyScalarExpression(scalar) {
		return ""
	}

	// Allow the filters node to pass through; formatting is per-filter.
	if scalar.Op() == opt.FiltersOp {
		return ""
	}

	// Build the scalar expression and format it as SQL text.
	formatted, err := buildAndFormatScalar(f.Memo, scalar, f.ColumnString)
	if err != nil {
		return ""
	}
	return formatted
}

// isPurelyScalarExpression recursively verifies that an expression tree
// contains only scalar (non-relational) operators.
func isPurelyScalarExpression(expr opt.Expr) bool {
	if !opt.IsScalarOp(expr) {
		return false
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if !isPurelyScalarExpression(expr.Child(i)) {
			return false
		}
	}
	return true
}

// buildAndFormatScalar execbuilds a scalar expression and formats it as SQL text,
// using the provided columnNameLookup function to resolve column references.
func buildAndFormatScalar(
	mem *memo.Memo,
	scalar opt.ScalarExpr,
	columnNameLookup func(opt.ColumnID) string,
) (string, error) {
	bld := New(nil /* factory */, mem, nil /* catalog */, scalar, nil /* evalCtx */)

	ivh := tree.MakeIndexedVarHelper(nil /* container */, mem.Metadata().NumColumns())
	expr, err := bld.BuildScalar(&ivh)
	if err != nil {
		return "", err
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
		ctx.WriteString(columnNameLookup(opt.ColumnID(idx + 1)))
	})
	expr.Format(fmtCtx)
	return fmtCtx.String(), nil
}
