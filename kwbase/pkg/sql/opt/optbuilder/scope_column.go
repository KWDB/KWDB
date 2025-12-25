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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// scopeColumn holds per-column information that is scoped to a particular
// relational expression. Note that scopeColumn implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a scopeColumn.
type scopeColumn struct {
	// name is the current name of this column. It is usually the same as
	// the original name, unless this column was renamed with an AS expression.
	name  tree.Name
	table tree.TableName
	typ   *types.T

	// id is an identifier for this column, which is unique across all the
	// columns in the query.
	id opt.ColumnID

	// hidden is true if the column is not selected by a '*' wildcard operator.
	// The column must be explicitly referenced by name, or otherwise is not
	// included.
	hidden bool

	// mutation is true if the column is in the process of being dropped or added
	// to the table. It should not be visible to variable references.
	mutation bool

	// descending indicates whether this column is sorted in descending order.
	// This field is only used for ordering columns.
	descending bool

	// scalar is the scalar expression associated with this column. If it is nil,
	// then the column is a passthrough from an inner scope or a table column.
	scalar opt.ScalarExpr

	// expr is the AST expression that this column refers to, if any.
	// expr is nil if the column does not refer to an expression.
	expr tree.TypedExpr

	// exprStr contains a stringified representation of expr, or the original
	// column name if expr is nil. It is populated lazily inside getExprStr().
	exprStr string

	// procProps saves all procedure property
	procProperty *tree.ProcedureValueProperty
}

// ProcUsed returns cols used by procedure
func (s *scopeColumn) ProcUsed() bool {
	return s.procProperty != nil
}

// IsDeclared returns cols from declare
func (s *scopeColumn) IsDeclared() bool {
	return s.procProperty != nil && s.procProperty.IsDeclared()
}

// RealIdx returns value real local index
func (s *scopeColumn) RealIdx() int {
	if s.procProperty == nil {
		return -1
	}
	return s.procProperty.RealIdx()
}

// IsParam returns cols from param
func (s *scopeColumn) IsParam() bool {
	return s.procProperty != nil && s.procProperty.IsParam()
}

// CopyProcedureProperty returns new property
func (s *scopeColumn) CopyProcedureProperty() *tree.ProcedureValueProperty {
	if s.procProperty == nil {
		return nil
	}
	return s.procProperty.Copy()
}

// clearName sets the empty table and column name. This is used to make the
// column anonymous so that it cannot be referenced, but will still be
// projected.
func (s *scopeColumn) clearName() {
	s.name = ""
	s.table = tree.TableName{}
}

// GetName gets name
func (s *scopeColumn) GetName() tree.Name {
	return s.name
}

// GetType gets type
func (s *scopeColumn) GetType() *types.T {
	return s.typ
}

// getExpr returns the the expression that this column refers to, or the column
// itself if the column does not refer to an expression.
func (s *scopeColumn) getExpr() tree.TypedExpr {
	if s.expr == nil {
		return s
	}
	return s.expr
}

// getExprStr gets a stringified representation of the expression that this
// column refers to.
func (s *scopeColumn) getExprStr() string {
	if s.exprStr == "" {
		s.exprStr = symbolicExprStr(s.getExpr())
	}
	return s.exprStr
}

var _ tree.Expr = &scopeColumn{}
var _ tree.TypedExpr = &scopeColumn{}
var _ tree.VariableExpr = &scopeColumn{}

func (s *scopeColumn) String() string {
	return tree.AsString(s)
}

// Format implements the NodeFormatter interface.
func (s *scopeColumn) Format(ctx *tree.FmtCtx) {
	// FmtCheckEquivalence is used by getExprStr when comparing expressions for
	// equivalence. If that flag is present, then use the unique column id to
	// differentiate this column from other columns.
	if ctx.HasFlags(tree.FmtCheckEquivalence) {
		// Use double @ to distinguish from Cockroach column ordinals.
		ctx.Printf("@@%d", s.id)
		return
	}

	if ctx.HasFlags(tree.FmtShowTableAliases) && s.table.TableName != "" {
		if s.table.ExplicitSchema && s.table.SchemaName != "" {
			if s.table.ExplicitCatalog && s.table.CatalogName != "" {
				ctx.FormatNode(&s.table.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&s.table.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&s.table.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&s.name)
}

// Walk is part of the tree.Expr interface.
func (s *scopeColumn) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck is part of the tree.Expr interface.
func (s *scopeColumn) TypeCheck(_ *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	return s, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (s *scopeColumn) ResolvedType() *types.T {
	return s.typ
}

// Eval is part of the tree.TypedExpr interface.
func (s *scopeColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	colName := string(s.name)
	if len(s.exprStr) > 0 {
		colName = s.exprStr
	} else if s.expr != nil {
		colName = s.expr.String()
	}
	panic(errors.AssertionFailedf("scopeColumn %v must be replaced before evaluation ", colName))
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (s *scopeColumn) Variable() {}

// ReplaceProcCond replaces the scopeColumn in the procStmt condition with IndexedVar
type ReplaceProcCond struct{}

// Replace replaces the scopeColumn in the procStmt condition with IndexedVar
func (d *ReplaceProcCond) Replace(expr tree.Expr) tree.TypedExpr {
	if v, ok := (expr).(*scopeColumn); ok && v.IsDeclared() {
		indexVal := tree.NewTypedOrdinalReference(v.RealIdx(), v.typ)
		indexVal.ProcProperty = v.CopyProcedureProperty()
		return indexVal
	}
	return expr.Walk(d).(tree.TypedExpr)
}

// VisitPre for ReplaceProcCond
func (d *ReplaceProcCond) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v, ok := (expr).(*scopeColumn); ok && v.IsDeclared() {
		indexVal := tree.NewTypedOrdinalReference(v.RealIdx(), v.typ)
		indexVal.ProcProperty = v.CopyProcedureProperty()
		return false, indexVal
	}
	return true, expr
}

// VisitPost for ReplaceProcCond
func (d *ReplaceProcCond) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	return expr
}
