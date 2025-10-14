// Copyright 2020 The Cockroach Authors.
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

package norm

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
)

// ConcatLeftDeepAnds concatenates any left-deep And expressions in the right
// expression with any left-deep And expressions in the left expression. The
// result is a combined left-deep And expression. Note that NormalizeNestedAnds
// has already guaranteed that both inputs will already be left-deep.
func (c *CustomFuncs) ConcatLeftDeepAnds(left, right opt.ScalarExpr) opt.ScalarExpr {
	if and, ok := right.(*memo.AndExpr); ok {
		return c.f.ConstructAnd(c.ConcatLeftDeepAnds(left, and.Left), and.Right)
	}
	return c.f.ConstructAnd(left, right)
}

// NegateComparison negates a comparison op like:
//
//	a.x = 5
//
// to:
//
//	a.x <> 5
func (c *CustomFuncs) NegateComparison(
	cmp opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	negate := opt.NegateOpMap[cmp]
	return c.f.DynamicConstruct(negate, left, right).(opt.ScalarExpr)
}

// FindRedundantConjunct takes the left and right operands of an Or operator as
// input. It examines each conjunct from the left expression and determines
// whether it appears as a conjunct in the right expression. If so, it returns
// the matching conjunct. Otherwise, it returns nil. For example:
//
//	A OR A                               =>  A
//	B OR A                               =>  nil
//	A OR (A AND B)                       =>  A
//	(A AND B) OR (A AND C)               =>  A
//	(A AND B AND C) OR (A AND (D OR E))  =>  A
//
// Once a redundant conjunct has been found, it is extracted via a call to the
// ExtractRedundantConjunct function. Redundant conjuncts are extracted from
// multiple nested Or operators by repeated application of these functions.
func (c *CustomFuncs) FindRedundantConjunct(left, right opt.ScalarExpr) opt.ScalarExpr {
	// Recurse over each conjunct from the left expression and determine whether
	// it's redundant.
	for {
		// Assume a left-deep And expression tree normalized by NormalizeNestedAnds.
		if and, ok := left.(*memo.AndExpr); ok {
			if c.isConjunct(and.Right, right) {
				return and.Right
			}
			left = and.Left
		} else {
			if c.isConjunct(left, right) {
				return left
			}
			return nil
		}
	}
}

// isConjunct returns true if the candidate expression is a conjunct within the
// given conjunction. The conjunction is assumed to be left-deep (normalized by
// the NormalizeNestedAnds rule).
func (c *CustomFuncs) isConjunct(candidate, conjunction opt.ScalarExpr) bool {
	for {
		if and, ok := conjunction.(*memo.AndExpr); ok {
			if and.Right == candidate {
				return true
			}
			conjunction = and.Left
		} else {
			return conjunction == candidate
		}
	}
}

// ExtractRedundantConjunct extracts a redundant conjunct from an Or expression,
// and returns an And of the conjunct with the remaining Or expression (a
// logically equivalent expression). For example:
//
//	(A AND B) OR (A AND C)  =>  A AND (B OR C)
//
// If extracting the conjunct from one of the OR conditions would result in an
// empty condition, the conjunct itself is returned (a logically equivalent
// expression). For example:
//
//	A OR (A AND B)  =>  A
//
// These transformations are useful for finding a conjunct that can be pushed
// down in the query tree. For example, if the redundant conjunct A is fully
// bound by one side of a join, it can be pushed through the join, even if B and
// C cannot.
func (c *CustomFuncs) ExtractRedundantConjunct(
	conjunct, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	if conjunct == left || conjunct == right {
		return conjunct
	}

	return c.f.ConstructAnd(
		conjunct,
		c.f.ConstructOr(
			c.extractConjunct(conjunct, left.(*memo.AndExpr)),
			c.extractConjunct(conjunct, right.(*memo.AndExpr)),
		),
	)
}

// extractConjunct traverses the And subtree looking for the given conjunct,
// which must be present. Once it's located, it's removed from the tree, and
// the remaining expression is returned.
func (c *CustomFuncs) extractConjunct(conjunct opt.ScalarExpr, and *memo.AndExpr) opt.ScalarExpr {
	if and.Right == conjunct {
		return and.Left
	}
	if and.Left == conjunct {
		return and.Right
	}
	return c.f.ConstructAnd(c.extractConjunct(conjunct, and.Left.(*memo.AndExpr)), and.Right)
}
