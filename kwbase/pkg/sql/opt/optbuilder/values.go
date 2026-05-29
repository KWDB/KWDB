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

package optbuilder

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// buildValuesClause builds a set of memo groups that represent the given values
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildValuesClause(
	values *tree.ValuesClause, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	numRows := len(values.Rows)
	numCols := 0
	if numRows > 0 {
		numCols = len(values.Rows[0])
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	b.semaCtx.Properties.Require(exprTypeValues.String(), tree.RejectSpecial)
	inScope.context = exprTypeValues

	// Typing a VALUES clause is not trivial; consider:
	//   VALUES (NULL), (1)
	// We want to type the entire column as INT. For this, we must find the first
	// expression that resolves to a definite type. Moreover, we want the NULL to
	// be typed correctly; so once we figure out the type, we must go back and
	// add casts as necessary. We do this column by column and store the groups in
	// a linearized matrix.

	// Bulk allocate ScalarListExpr slices: we need a matrix of size numRows by
	// numCols and one slice of length numRows for the tuples.
	elems := make(memo.ScalarListExpr, numRows*numCols+numRows)
	tuples, elems := elems[:numRows], elems[numRows:]

	colTypes := make([]types.T, numCols)
	for colIdx := range colTypes {
		desired := types.Any
		if colIdx < len(desiredTypes) {
			desired = desiredTypes[colIdx]
		}
		colTypes[colIdx] = *types.Unknown

		elemPos := colIdx
		for _, tuple := range values.Rows {
			if numCols != len(tuple) {
				reportValuesLenError(numCols, len(tuple))
			}

			expr := inScope.walkExprTree(tuple[colIdx])
			texpr, err := tree.TypeCheck(expr, b.semaCtx, desired)
			if err != nil {
				panic(err)
			}

			// If the target column type is known, allow assignment casts between CITEXT
			// and the string family here.
			if tree.IsCITextStringMixed(texpr.ResolvedType(), desired) {
				texpr, err = tree.NewTypedCastExpr(texpr, desired)
				if err != nil {
					panic(err)
				}
			}
			if typ := texpr.ResolvedType(); typ.Family() != types.UnknownFamily {
				if colTypes[colIdx].Family() == types.UnknownFamily {
					colTypes[colIdx] = *typ
				} else if !typ.Equivalent(&colTypes[colIdx]) {
					panic(pgerror.Newf(pgcode.DatatypeMismatch,
						"VALUES types %s and %s cannot be matched", typ, &colTypes[colIdx]))
				}
			}
			elems[elemPos] = b.buildScalar(texpr, inScope, nil, nil, nil)
			if name, ok := memo.CheckGroupWindowExist(elems[elemPos]); ok {
				panic(pgerror.Newf(pgcode.Syntax, "%s(): group window function can be only used in single time series table query.", name))
			}
			elemPos += numCols
		}

		// If we still don't have a type for the column, set it to the desired type.
		if colTypes[colIdx].Family() == types.UnknownFamily && desired.Family() != types.AnyFamily {
			colTypes[colIdx] = *desired
		}

		// Add casts to NULL values if necessary.
		if colTypes[colIdx].Family() != types.UnknownFamily {
			elemPos := colIdx
			for range values.Rows {
				if elems[elemPos].DataType().Family() == types.UnknownFamily {
					elems[elemPos] = b.factory.ConstructCast(elems[elemPos], &colTypes[colIdx])
				}
				elemPos += numCols
			}
		}
	}

	// Build the tuples.
	tupleTyp := types.MakeTuple(colTypes)
	for rowIdx := range tuples {
		tuples[rowIdx] = b.factory.ConstructTuple(elems[:numCols], tupleTyp)
		elems = elems[numCols:]
	}

	outScope = inScope.push()
	for colIdx := 0; colIdx < numCols; colIdx++ {
		// The column names for VALUES are column1, column2, etc.
		alias := fmt.Sprintf("column%d", colIdx+1)
		b.synthesizeColumn(outScope, alias, &colTypes[colIdx], nil, nil /* scalar */)
	}

	colList := colsToColList(outScope.cols)
	outScope.expr = b.factory.ConstructValues(tuples, &memo.ValuesPrivate{
		Cols: colList,
		ID:   b.factory.Metadata().NextUniqueID(),
	})
	return outScope
}

func reportValuesLenError(expected, actual int) {
	panic(pgerror.Newf(
		pgcode.Syntax,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		expected, actual))
}
