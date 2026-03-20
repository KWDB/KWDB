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

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (b *Builder) processWiths(
	with *tree.With, inScope *scope, buildStmt func(inScope *scope) *scope,
) *scope {
	inScope = b.buildCTEs(with, inScope)
	prevAtRoot := inScope.atRoot
	inScope.atRoot = false
	outScope := buildStmt(inScope)
	// if table are stable or tstable, return error
	if with != nil && (outScope.TableType.HasStable() || outScope.TableType.HasGtable()) {
		panic(pgerror.New(pgcode.Warning, "with...as... is not supported in timeseries table"))
	}
	inScope.atRoot = prevAtRoot
	return outScope
}

func (b *Builder) buildCTE(
	cte *tree.CTE, inScope *scope, isRecursive bool,
) (memo.RelExpr, physical.Presentation) {
	if !isRecursive {
		// check fill clause
		if sel, ok := cte.Stmt.(*tree.Select); ok {
			if selClause, ok := sel.Select.(*tree.SelectClause); ok && selClause.Fill.FillType > 0 {
				panic(pgerror.New(pgcode.Syntax, "FILL clause is not supported in WITH...AS..."))
			}
		}
		cteScope := b.buildStmt(cte.Stmt, nil /* desiredTypes */, inScope)
		cteScope.removeHiddenCols()
		b.dropOrderingAndExtraCols(cteScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	// WITH RECURSIVE queries are always of the form:
	//
	//   WITH RECURSIVE name(cols) AS (
	//     initial_query
	//     UNION ALL
	//     recursive_query
	//   )
	//
	// Recursive CTE evaluation (paraphrased from postgres docs):
	//  1. Evaluate the initial query; emit the results and also save them in
	//     a "working" table.
	//  2. So long as the working table is not empty:
	//     * evaluate the recursive query, substituting the current contents of
	//       the working table for the recursive self-reference.
	//     * emit all resulting rows, and save them as the next iteration's
	//       working table.
	//
	// Note however, that a non-recursive CTE can be used even when RECURSIVE is
	// specified (particularly useful when there are multiple CTEs defined).
	// Handling this while having decent error messages is tricky.

	// Generate an id for the recursive CTE reference. This is the id through
	// which the recursive expression refers to the current working table
	// (via WithScan).
	withID := b.factory.Memo().NextWithID()

	// cteScope allows recursive references to this CTE.
	cteScope := inScope.push()
	cteSrc := &cteSource{
		id:   withID,
		name: cte.Name,
	}
	cteScope.ctes = map[string]*cteSource{cte.Name.Alias.String(): cteSrc}

	initial, recursive, isUnionAll, ok := b.splitRecursiveCTE(cte.Stmt)
	// We don't currently support the UNION form (only UNION ALL).
	if !ok || !isUnionAll {
		// Build this as a non-recursive CTE, but throw a proper error message if it
		// does have a recursive reference.
		cteSrc.onRef = func() {
			if !ok {
				panic(pgerror.Newf(
					pgcode.Syntax,
					"recursive query %q does not have the form non-recursive-term UNION ALL recursive-term",
					cte.Name.Alias,
				))
			} else {
				panic(unimplementedWithIssueDetailf(
					46642, "",
					"recursive query %q uses UNION which is not implemented (only UNION ALL is supported)",
					cte.Name.Alias,
				))
			}
		}
		return b.buildCTE(cte, cteScope, false /* recursive */)
	}

	// Set up an error if the initial part has a recursive reference.
	cteSrc.onRef = func() {
		panic(pgerror.Newf(
			pgcode.Syntax,
			"recursive reference to query %q must not appear within its non-recursive term",
			cte.Name.Alias,
		))
	}
	// If the initial statement contains CTEs, we don't want the Withs hoisted
	// above the recursive CTE.
	b.pushWithFrame()
	initialScope := b.buildStmt(initial, nil /* desiredTypes */, cteScope)
	b.popWithFrame(initialScope)

	initialScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(initialScope)

	// The properties of the binding are tricky: the recursive expression is
	// invoked repeatedly and these must hold each time. We can't use the initial
	// expression's properties directly, as those only hold the first time the
	// recursive query is executed. We can't really say too much about what the
	// working table contains, except that it has at least one row (the recursive
	// query is never invoked with an empty working table).
	bindingProps := &props.Relational{}
	bindingProps.OutputCols = initialScope.colSet()
	bindingProps.Cardinality = props.AnyCardinality.AtLeast(props.OneCardinality)
	// We don't really know the input row count, except for the first time we run
	// the recursive query. We don't have anything better though.
	bindingProps.Stats.RowCount = initialScope.expr.Relational().Stats.RowCount
	// Row count must be greater than 0 or the stats code will throw an error.
	// Set it to 1 to match the cardinality.
	if bindingProps.Stats.RowCount < 1 {
		bindingProps.Stats.RowCount = 1
	}
	cteSrc.expr = b.factory.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	})
	b.factory.Metadata().AddWithBinding(withID, cteSrc.expr)

	cteSrc.cols = b.getCTECols(initialScope, cte.Name)

	outScope := inScope.push()

	initialTypes := initialScope.makeColumnTypes()

	// Synthesize new output columns (because they contain values from both the
	// initial and the recursive relations). These columns will also be used to
	// refer to the working table (from the recursive query); we can't use the
	// initial columns directly because they might contain duplicate IDs (e.g.
	// consider initial query SELECT 0, 0).
	for i, c := range cteSrc.cols {
		newCol := b.synthesizeColumn(outScope, c.Alias, initialTypes[i], nil /* expr */, nil /* scalar */)
		cteSrc.cols[i].ID = newCol.id
	}

	// We want to check if the recursive query is actually recursive. This is for
	// annoying cases like `SELECT 1 UNION ALL SELECT 2`.
	numRefs := 0
	cteSrc.onRef = func() {
		numRefs++
	}

	// If the recursive statement contains CTEs, we don't want the Withs hoisted
	// above the recursive CTE.
	b.pushWithFrame()
	recursiveScope := b.buildStmt(recursive, initialTypes /* desiredTypes */, cteScope)
	b.popWithFrame(recursiveScope)

	if numRefs == 0 {
		// Build this as a non-recursive CTE.
		cteScope := b.buildSetOp(tree.UnionOp, false /* all */, inScope, initialScope, recursiveScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	if numRefs != 1 {
		// We disallow multiple recursive references for consistency with postgres.
		panic(pgerror.Newf(
			pgcode.Syntax,
			"recursive reference to query %q must not appear more than once",
			cte.Name.Alias,
		))
	}

	recursiveScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(recursiveScope)

	// We allow propagation of types from the initial query to the recursive
	// query.
	_, propagateToRight := b.checkTypesMatch(initialScope, recursiveScope,
		false, /* tolerateUnknownLeft */
		true,  /* tolerateUnknownRight */
		"UNION",
	)
	if propagateToRight {
		recursiveScope = b.propagateTypes(recursiveScope /* dst */, initialScope /* src */)
	}

	private := memo.RecursiveCTEPrivate{
		Name:          string(cte.Name.Alias),
		WithID:        withID,
		InitialCols:   colsToColList(initialScope.cols),
		RecursiveCols: colsToColList(recursiveScope.cols),
		OutCols:       colsToColList(outScope.cols),
	}

	expr := b.factory.ConstructRecursiveCTE(cteSrc.expr, initialScope.expr, recursiveScope.expr, &private)
	return expr, cteSrc.cols
}

// getCTECols returns a presentation for the scope, renaming the columns to
// those provided in the AliasClause (if any). Throws an error if there is a
// mismatch in the number of columns.
func (b *Builder) getCTECols(cteScope *scope, name tree.AliasClause) physical.Presentation {
	presentation := cteScope.makePresentation()

	if len(presentation) == 0 {
		err := pgerror.Newf(
			pgcode.FeatureNotSupported,
			"WITH clause %q does not return any columns",
			tree.ErrString(&name),
		)
		panic(errors.WithHint(err, "missing RETURNING clause?"))
	}

	if name.Cols == nil {
		return presentation
	}

	if len(presentation) != len(name.Cols) {
		panic(pgerror.Newf(
			pgcode.InvalidColumnReference,
			"source %q has %d columns available but %d columns specified",
			name.Alias, len(presentation), len(name.Cols),
		))
	}
	for i := range presentation {
		presentation[i].Alias = string(name.Cols[i])
	}
	return presentation
}

// splitRecursiveCTE splits a CTE statement of the form
//
//	initial_query UNION ALL recursive_query
//
// into the initial and recursive parts. If the statement is not of this form,
// returns ok=false.
func (b *Builder) splitRecursiveCTE(
	stmt tree.Statement,
) (initial, recursive *tree.Select, isUnionAll bool, ok bool) {
	sel, ok := stmt.(*tree.Select)
	// The form above doesn't allow for "outer" WITH, ORDER BY, or LIMIT
	// clauses.
	if !ok || sel.With != nil || sel.OrderBy != nil || sel.Limit != nil {
		return nil, nil, false, false
	}
	union, ok := sel.Select.(*tree.UnionClause)
	if !ok || union.Type != tree.UnionOp {
		return nil, nil, false, false
	}
	return union.Left, union.Right, union.All, true
}
