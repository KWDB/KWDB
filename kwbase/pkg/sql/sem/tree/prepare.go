// Copyright 2016 The Cockroach Authors.
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

package tree

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// Prepare represents a PREPARE statement.
type Prepare struct {
	Name      Name
	Types     []*types.T
	Statement Statement
	Udv       *UserDefinedVar

	// NumPlaceholders indicates the number of arguments to the statement (which
	// are referenced through placeholders). This corresponds to the highest
	// argument position (i.e. the x in "$x") that appears in the query.
	//
	// Note: where there are "gaps" in the placeholder positions, this number is
	// based on the highest position encountered. For example, for `SELECT $3`,
	// NumPlaceholders is 3. These cases are malformed and will result in a
	// type-check error.
	NumPlaceholders int

	// NumAnnotations indicates the number of annotations in the tree. It is equal
	// to the maximum annotation index.
	NumAnnotations AnnotationIdx
}

// Format implements the NodeFormatter interface.
func (node *Prepare) Format(ctx *FmtCtx) {
	ctx.WriteString("PREPARE ")
	ctx.FormatNode(&node.Name)
	if len(node.Types) > 0 {
		ctx.WriteString(" (")
		for i, t := range node.Types {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(t.SQLString())
		}
		ctx.WriteRune(')')
	}
	ctx.WriteString(" AS ")
	if node.Udv != nil {
		ctx.WriteString(node.Udv.String())
	} else {
		ctx.FormatNode(node.Statement)
	}
}

// CannedOptPlan is used as the AST for a PREPARE .. AS OPT PLAN statement.
// This is a testing facility that allows execution (and benchmarking) of
// specific plans. See exprgen package for more information on the syntax.
type CannedOptPlan struct {
	Plan string
}

// Format implements the NodeFormatter interface.
func (node *CannedOptPlan) Format(ctx *FmtCtx) {
	// This node can only be used as the AST for a Prepare statement of the form:
	//   PREPARE name AS OPT PLAN '...').
	ctx.WriteString("OPT PLAN ")
	ctx.WriteString(lex.EscapeSQLString(node.Plan))
}

// Execute represents an EXECUTE statement.
type Execute struct {
	Name   Name
	Params Exprs
	// DiscardRows is set when we want to throw away all the rows rather than
	// returning for client (used for testing and benchmarking).
	DiscardRows bool
}

// Format implements the NodeFormatter interface.
func (node *Execute) Format(ctx *FmtCtx) {
	ctx.WriteString("EXECUTE ")
	ctx.FormatNode(&node.Name)
	if len(node.Params) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Params)
		ctx.WriteByte(')')
	}
	if node.DiscardRows {
		ctx.WriteString(" DISCARD ROWS")
	}
}

// Deallocate represents a DEALLOCATE statement.
type Deallocate struct {
	Name Name // empty for ALL
}

// Format implements the NodeFormatter interface.
func (node *Deallocate) Format(ctx *FmtCtx) {
	ctx.WriteString("DEALLOCATE ")
	if node.Name == "" {
		ctx.WriteString("ALL")
	} else {
		ctx.FormatNode(&node.Name)
	}
}
