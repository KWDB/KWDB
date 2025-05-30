// Copyright 2016 The Cockroach Authors.
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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// VarName occurs inside scalar expressions.
//
// Immediately after parsing, the following types can occur:
//
//   - UnqualifiedStar: a naked star as argument to a function, e.g. count(*),
//     or at the top level of a SELECT clause.
//     See also uses of StarExpr() and StarSelectExpr() in the grammar.
//
// - UnresolvedName: other names of the form `a.b....e` or `a.b...e.*`.
//
// Consumers of variable names do not like UnresolvedNames and instead
// expect either AllColumnsSelector or ColumnItem. Use
// NormalizeVarName() for this.
//
// After a ColumnItem is available, it should be further resolved, for this
// the Resolve() method should be used; see name_resolution.go.
type VarName interface {
	TypedExpr

	// NormalizeVarName() guarantees to return a variable name
	// that is not an UnresolvedName. This converts the UnresolvedName
	// to an AllColumnsSelector or ColumnItem as necessary.
	NormalizeVarName() (VarName, error)
}

var _ VarName = &UnresolvedName{}
var _ VarName = UnqualifiedStar{}
var _ VarName = &AllColumnsSelector{}
var _ VarName = &TupleStar{}
var _ VarName = &ColumnItem{}

// UnqualifiedStar corresponds to a standalone '*' in a scalar
// expression.
type UnqualifiedStar struct{}

// Format implements the NodeFormatter interface.
func (UnqualifiedStar) Format(ctx *FmtCtx) { ctx.WriteByte('*') }
func (u UnqualifiedStar) String() string   { return AsString(u) }

// NormalizeVarName implements the VarName interface.
func (u UnqualifiedStar) NormalizeVarName() (VarName, error) { return u, nil }

var singletonStarName VarName = UnqualifiedStar{}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() VarName { return singletonStarName }

// ResolvedType implements the TypedExpr interface.
func (UnqualifiedStar) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("unqualified stars ought to be replaced before this point"))
}

// Variable implements the VariableExpr interface.
func (UnqualifiedStar) Variable() {}

// UnresolvedName is defined in name_part.go. It also implements the
// VarName interface, and thus TypedExpr too.

// ResolvedType implements the TypedExpr interface.
func (*UnresolvedName) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("unresolved names ought to be replaced before this point"))
}

// Variable implements the VariableExpr interface.  Although, the
// UnresolvedName ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (*UnresolvedName) Variable() {}

// NormalizeVarName implements the VarName interface.
func (n *UnresolvedName) NormalizeVarName() (VarName, error) {
	return classifyColumnItem(n)
}

// AllColumnsSelector corresponds to a selection of all
// columns in a table when used in a SELECT clause.
// (e.g. `table.*`).
type AllColumnsSelector struct {
	// TableName corresponds to the table prefix, before the star.
	TableName *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (a *AllColumnsSelector) Format(ctx *FmtCtx) {
	ctx.FormatNode(a.TableName)
	ctx.WriteString(".*")
}
func (a *AllColumnsSelector) String() string { return AsString(a) }

// NormalizeVarName implements the VarName interface.
func (a *AllColumnsSelector) NormalizeVarName() (VarName, error) { return a, nil }

// Variable implements the VariableExpr interface.  Although, the
// AllColumnsSelector ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (a *AllColumnsSelector) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (*AllColumnsSelector) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("all-columns selectors ought to be replaced before this point"))
}

// ColumnItem corresponds to the name of a column in an expression.
type ColumnItem struct {
	// TableName holds the table prefix, if the name refers to a column. It is
	// optional.
	//
	// This uses UnresolvedObjectName because we need to preserve the
	// information about which parts were initially specified in the SQL
	// text. ColumnItems are intermediate data structures anyway, that
	// still need to undergo name resolution.
	TableName *UnresolvedObjectName
	// ColumnName names the designated column.
	ColumnName Name
}

// Format implements the NodeFormatter interface.
func (c *ColumnItem) Format(ctx *FmtCtx) {
	if c.TableName != nil {
		c.TableName.Format(ctx)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.ColumnName)
}
func (c *ColumnItem) String() string { return AsString(c) }

// NormalizeVarName implements the VarName interface.
func (c *ColumnItem) NormalizeVarName() (VarName, error) { return c, nil }

// Column retrieves the unqualified column name.
func (c *ColumnItem) Column() string {
	return string(c.ColumnName)
}

// Variable implements the VariableExpr interface.
//
// Note that in common uses, ColumnItem ought to be replaced to an
// IndexedVar prior to evaluation.
func (c *ColumnItem) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (c *ColumnItem) ResolvedType() *types.T {
	if presetTypesForTesting == nil {
		return nil
	}
	return presetTypesForTesting[c.String()]
}

// NewColumnItem constructs a column item from an already valid
// TableName. This can be used for e.g. pretty-printing.
func NewColumnItem(tn *TableName, colName Name) *ColumnItem {
	c := MakeColumnItem(tn, colName)
	return &c
}

// MakeColumnItem constructs a column item from an already valid
// TableName. This can be used for e.g. pretty-printing.
func MakeColumnItem(tn *TableName, colName Name) ColumnItem {
	c := ColumnItem{ColumnName: colName}
	if tn.Table() != "" {
		numParts := 1
		if tn.ExplicitCatalog {
			numParts = 3
		} else if tn.ExplicitSchema {
			numParts = 2
		}

		c.TableName = &UnresolvedObjectName{
			NumParts: numParts,
			Parts:    [3]string{tn.Table(), tn.Schema(), tn.Catalog()},
		}
	}
	return c
}

// UserDefinedVar corresponds to the name of a column in an expression.
type UserDefinedVar struct {
	VarName string

	typeAnnotation
}

func (c *UserDefinedVar) String() string { return c.VarName }

// Format implements the NodeFormatter interface.
func (c *UserDefinedVar) Format(ctx *FmtCtx) {
	ctx.WriteString(c.VarName)
}

// Walk implements the Expr interface.
func (c *UserDefinedVar) Walk(_ Visitor) Expr {
	return c
}

// TypeCheck implements the Expr interface.  This function has a valid
// implementation only for testing within this package. During query
// execution, ColumnItems are replaced to IndexedVars prior to type
// checking.
func (c *UserDefinedVar) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	if ctx == nil || ctx.UserDefinedVars == nil {
		return nil, pgerror.New(pgcode.Warning, "user defined var type check error")
	}
	varName := strings.ToLower(c.VarName)
	if val, ok := ctx.UserDefinedVars[varName]; ok {
		c.typ = val.(Datum).ResolvedType()
	} else {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "%s is not defined", c.VarName)
	}
	return c, nil
}

// NewUserDefinedVarExpr returns a new UserDefinedVar that is well-typed.
func NewUserDefinedVarExpr(varName string, typ *types.T) *UserDefinedVar {
	node := &UserDefinedVar{}
	node.VarName = varName
	node.typ = typ
	return node
}
