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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// buildUnionClause builds a set of memo groups that represent the given union
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUnionClause(
	clause *tree.UnionClause, desiredTypes []*types.T, inScope *scope,
) (outScope *scope) {
	leftScope := b.buildStmt(clause.Left, desiredTypes, inScope)
	// Try to propagate types left-to-right, if we didn't already have desired
	// types.
	if len(desiredTypes) == 0 {
		desiredTypes = leftScope.makeColumnTypes()
	}
	rightScope := b.buildStmt(clause.Right, desiredTypes, inScope)
	if b.factory.Memo().CheckFlag(opt.GroupWindowUseOrderScan) {
		panic(pgerror.Newf(pgcode.Syntax, "group window function cannot be used in UNION."))
	}
	return b.buildSetOp(clause.Type, clause.All, inScope, leftScope, rightScope)
}

func (b *Builder) buildSetOp(
	typ tree.UnionType, all bool, inScope, leftScope, rightScope *scope,
) (outScope *scope) {
	// Remove any hidden columns, as they are not included in the Union.
	leftScope.removeHiddenCols()
	rightScope.removeHiddenCols()

	outScope = inScope.push()
	b.mergeTableType(leftScope, rightScope, outScope)
	// propagateTypesLeft/propagateTypesRight indicate whether we need to wrap
	// the left/right side in a projection to cast some of the columns to the
	// correct type.
	// For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. We need to
	// wrap the left side in a project operation with a Cast expression so the
	// output column will have the correct type.
	propagateTypesLeft, propagateTypesRight := b.checkTypesMatch(
		leftScope, rightScope,
		true, /* tolerateUnknownLeft */
		true, /* tolerateUnknownRight */
		typ.String(),
	)

	if propagateTypesLeft {
		leftScope = b.propagateTypes(leftScope /* dst */, rightScope /* src */)
	}
	if propagateTypesRight {
		rightScope = b.propagateTypes(rightScope /* dst */, leftScope /* src */)
	}

	// For UNION, we have to synthesize new output columns (because they contain
	// values from both the left and right relations). This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	if typ == tree.UnionOp {
		outScope.cols = make([]scopeColumn, 0, len(leftScope.cols))
		for i := range leftScope.cols {
			c := &leftScope.cols[i]
			b.synthesizeColumn(outScope, string(c.name), c.typ, nil, nil /* scalar */)
		}
	} else {
		outScope.appendColumnsFromScope(leftScope)
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	newCols := colsToColList(outScope.cols)

	left := leftScope.expr.(memo.RelExpr)
	right := rightScope.expr.(memo.RelExpr)
	private := memo.SetPrivate{LeftCols: leftCols, RightCols: rightCols, OutCols: newCols}

	if all {
		switch typ {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnionAll(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersectAll(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExceptAll(left, right, &private)
		}
	} else {
		switch typ {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnion(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersect(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExcept(left, right, &private)
		}
	}

	return outScope
}

// checkTypesMatch is used when the columns must match between two scopes (e.g.
// for a UNION). Throws an error if the scopes don't have the same number of
// columns, or when column types don't match 1-1, except:
//   - if tolerateUnknownLeft is set and the left column has Unknown type while
//     the right has a known type (in this case it returns propagateToLeft=true).
//   - if tolerateUnknownRight is set and the right column has Unknown type while
//     the right has a known type (in this case it returns propagateToRight=true).
//
// clauseTag is used only in error messages.
//
// TODO(dan): This currently checks whether the types are exactly the same,
// but Postgres is more lenient:
// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
func (b *Builder) checkTypesMatch(
	leftScope, rightScope *scope,
	tolerateUnknownLeft bool,
	tolerateUnknownRight bool,
	clauseTag string,
) (propagateToLeft, propagateToRight bool) {
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(pgerror.Newf(
			pgcode.Syntax,
			"each %s query must have the same number of columns: %d vs %d",
			clauseTag, len(leftScope.cols), len(rightScope.cols),
		))
	}

	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]

		if l.typ.Equivalent(r.typ) {
			continue
		}

		// Note that Unknown types are equivalent so at this point at most one of
		// the types can be Unknown.
		if l.typ.Family() == types.UnknownFamily && tolerateUnknownLeft {
			propagateToLeft = true
			continue
		}
		if r.typ.Family() == types.UnknownFamily && tolerateUnknownRight {
			propagateToRight = true
			continue
		}

		panic(pgerror.Newf(
			pgcode.DatatypeMismatch,
			"%v types %s and %s cannot be matched", clauseTag, l.typ, r.typ,
		))
	}
	return propagateToLeft, propagateToRight
}

// propagateTypes propagates the types of the source columns to the destination
// columns by wrapping the destination in a Project operation. The Project
// operation passes through columns that already have the correct type, and
// creates cast expressions for those that don't.
func (b *Builder) propagateTypes(dst, src *scope) *scope {
	expr := dst.expr.(memo.RelExpr)
	dstCols := dst.cols

	dst = dst.push()
	dst.cols = make([]scopeColumn, 0, len(dstCols))

	for i := 0; i < len(dstCols); i++ {
		dstType := dstCols[i].typ
		srcType := src.cols[i].typ
		if dstType.Family() == types.UnknownFamily && srcType.Family() != types.UnknownFamily {
			// Create a new column which casts the old column to the correct type.
			castExpr := b.factory.ConstructCast(b.factory.ConstructVariable(dstCols[i].id), srcType)
			b.synthesizeColumn(dst, string(dstCols[i].name), srcType, nil /* expr */, castExpr)
		} else {
			// The column is already the correct type, so add it as a passthrough
			// column.
			dst.appendColumn(&dstCols[i])
		}
	}
	dst.expr = b.constructProject(expr, dst.cols)
	return dst
}
