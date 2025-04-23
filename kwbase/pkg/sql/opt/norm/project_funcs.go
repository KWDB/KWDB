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
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// CanMergeProjections returns true if the outer Projections operator never
// references any of the inner Projections columns. If true, then the outer does
// not depend on the inner, and the two can be merged into a single set.
func (c *CustomFuncs) CanMergeProjections(outer, inner memo.ProjectionsExpr) bool {
	innerCols := c.ProjectionCols(inner)
	for i := range outer {
		if outer[i].ScalarProps().OuterCols.Intersects(innerCols) {
			return false
		}
	}
	return true
}

// MergeProjections concatenates the synthesized columns from the outer
// Projections operator, and the synthesized columns from the inner Projections
// operator that are passed through by the outer. Note that the outer
// synthesized columns must never contain references to the inner synthesized
// columns; this can be verified by first calling CanMergeProjections.
func (c *CustomFuncs) MergeProjections(
	outer, inner memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.ProjectionsExpr {
	// No need to recompute properties on the new projections, since they should
	// still be valid.
	newProjections := make(memo.ProjectionsExpr, len(outer), len(outer)+len(inner))
	copy(newProjections, outer)
	for i := range inner {
		item := &inner[i]
		if passthrough.Contains(item.Col) {
			newProjections = append(newProjections, *item)
		}
	}
	return newProjections
}

// MergeProjectWithValues merges a Project operator with its input Values
// operator. This is only possible in certain circumstances, which are described
// in the MergeProjectWithValues rule comment.
//
// Values columns that are part of the Project passthrough columns are retained
// in the final Values operator, and Project synthesized columns are added to
// it. Any unreferenced Values columns are discarded. For example:
//
//	SELECT column1, 3 FROM (VALUES (1, 2))
//	=>
//	(VALUES (1, 3))
func (c *CustomFuncs) MergeProjectWithValues(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, input memo.RelExpr,
) memo.RelExpr {
	newExprs := make(memo.ScalarListExpr, 0, len(projections)+passthrough.Len())
	newTypes := make([]types.T, 0, len(newExprs))
	newCols := make(opt.ColList, 0, len(newExprs))

	values := input.(*memo.ValuesExpr)
	tuple := values.Rows[0].(*memo.TupleExpr)
	for i, colID := range values.Cols {
		if passthrough.Contains(colID) {
			newExprs = append(newExprs, tuple.Elems[i])
			newTypes = append(newTypes, *tuple.Elems[i].DataType())
			newCols = append(newCols, colID)
		}
	}

	for i := range projections {
		item := &projections[i]
		newExprs = append(newExprs, item.Element)
		newTypes = append(newTypes, *item.Element.DataType())
		newCols = append(newCols, item.Col)
	}

	tupleTyp := types.MakeTuple(newTypes)
	rows := memo.ScalarListExpr{c.f.ConstructTuple(newExprs, tupleTyp)}
	return c.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: newCols,
		ID:   values.ID,
	})
}
