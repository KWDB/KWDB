// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package tree

// Delete represents a DELETE statement.
type Delete struct {
	With      *With
	Table     TableExpr
	Where     *Where
	OrderBy   OrderBy
	Limit     *Limit
	Returning ReturningClause
}

// Format implements the NodeFormatter interface.
func (node *Delete) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	node.writeDeleteFromClause(ctx)
	node.maybeWriteWhereClause(ctx)
	node.maybeWriteOrderByClause(ctx)
	node.maybeWriteLimitClause(ctx)
	node.maybeWriteReturningClause(ctx)
}

// writeDeleteFromClause outputs the DELETE FROM keyword followed by the table.
func (node *Delete) writeDeleteFromClause(ctx *FmtCtx) {
	ctx.WriteString("DELETE FROM ")
	ctx.FormatNode(node.Table)
}

// maybeWriteWhereClause emits the WHERE clause when present.
func (node *Delete) maybeWriteWhereClause(ctx *FmtCtx) {
	if node.Where != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Where)
	}
}

// maybeWriteOrderByClause emits the ORDER BY clause when present.
func (node *Delete) maybeWriteOrderByClause(ctx *FmtCtx) {
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.OrderBy)
	}
}

// maybeWriteLimitClause emits the LIMIT clause when present.
func (node *Delete) maybeWriteLimitClause(ctx *FmtCtx) {
	if node.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Limit)
	}
}

// maybeWriteReturningClause emits the RETURNING clause when present.
func (node *Delete) maybeWriteReturningClause(ctx *FmtCtx) {
	if HasReturningClause(node.Returning) {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Returning)
	}
}
