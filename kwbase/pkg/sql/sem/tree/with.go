// Copyright 2017 The Cockroach Authors.
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

// With represents a WITH statement.
type With struct {
	Recursive bool
	CTEList   []*CTE
}

// CTE represents a common table expression inside of a WITH clause.
type CTE struct {
	Name AliasClause
	Stmt Statement
}

// Format implements the NodeFormatter interface.
func (node *With) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	node.writeWithKeywordPrefix(ctx)
	node.writeRecursiveModifier(ctx)
	node.writeCTEList(ctx)
	ctx.WriteByte(' ')
}

// writeWithKeywordPrefix emits the WITH keyword.
func (node *With) writeWithKeywordPrefix(ctx *FmtCtx) {
	ctx.WriteString("WITH ")
}

// writeRecursiveModifier emits the RECURSIVE keyword when the CTE is recursive.
func (node *With) writeRecursiveModifier(ctx *FmtCtx) {
	if node.Recursive {
		ctx.WriteString("RECURSIVE ")
	}
}

// writeCTEList iterates over the CTE definitions and formats each one as
// "name AS (statement)", separated by commas.
func (node *With) writeCTEList(ctx *FmtCtx) {
	for i, cte := range node.CTEList {
		node.writeCTESeparator(ctx, i)
		node.formatSingleCTE(ctx, cte)
	}
}

// writeCTESeparator writes a comma before every CTE except the first.
func (node *With) writeCTESeparator(ctx *FmtCtx, index int) {
	if index != 0 {
		ctx.WriteString(", ")
	}
}

// formatSingleCTE outputs a single CTE in the form "name AS (statement)".
func (node *With) formatSingleCTE(ctx *FmtCtx, cte *CTE) {
	ctx.FormatNode(&cte.Name)
	ctx.WriteString(" AS (")
	ctx.FormatNode(cte.Stmt)
	ctx.WriteString(")")
}
