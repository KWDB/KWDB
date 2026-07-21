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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

// aggregateVisitorCheckResult represents the outcome of checking a function
// expression for aggregate characteristics during the visitor walk.
type aggregateVisitorCheckResult int

const (
	// aggregateCheckContinue indicates the walk should continue recursing into
	// child expressions.
	aggregateCheckContinue aggregateVisitorCheckResult = iota
	// aggregateCheckStop indicates the walk should stop at this node.
	aggregateCheckStop
)

// IsAggregateVisitor checks if walked expressions contain aggregate functions.
type IsAggregateVisitor struct {
	Aggregated bool
	// searchPath is used to search for unqualified function names.
	searchPath sessiondata.SearchPath
}

// compile-time interface compliance check
var _ tree.Visitor = &IsAggregateVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *IsAggregateVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	checkResult := v.evaluateExpressionForAggregates(expr)
	return checkResult == aggregateCheckContinue, expr
}

// VisitPost satisfies the Visitor interface.
func (*IsAggregateVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// evaluateExpressionForAggregates examines the given expression to determine
// whether the visitor should continue recursing or stop. It classifies
// function expressions and subqueries and updates the Aggregated flag when an
// aggregate function is detected.
func (v *IsAggregateVisitor) evaluateExpressionForAggregates(
	expr tree.Expr,
) aggregateVisitorCheckResult {
	switch typedExpr := expr.(type) {
	case *tree.FuncExpr:
		return v.handleFunctionExpression(typedExpr)
	case *tree.Subquery:
		return aggregateCheckStop
	default:
		return aggregateCheckContinue
	}
}

// handleFunctionExpression processes a function expression, determining whether
// it is an aggregate, a window function application, or a regular function.
func (v *IsAggregateVisitor) handleFunctionExpression(
	funcExpr *tree.FuncExpr,
) aggregateVisitorCheckResult {
	if funcExpr.IsWindowFunctionApplication() {
		// A window function application of an aggregate builtin is not an
		// aggregate function, but it can contain aggregate functions.
		return aggregateCheckContinue
	}

	funcDef, err := funcExpr.Func.Resolve(v.searchPath)
	if err != nil {
		return aggregateCheckStop
	}

	if funcDef.Class == tree.AggregateClass {
		v.Aggregated = true
		return aggregateCheckStop
	}

	return aggregateCheckContinue
}
