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

// This code was derived from https://github.com/youtube/vitess.

// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.

package tree

// Insert represents an INSERT statement.
type Insert struct {
	With            *With
	Table           TableExpr
	Columns         NameList
	Rows            *Select
	OnConflict      *OnConflict
	Returning       ReturningClause
	NoSchemaColumns NoSchemaNameList
	IsNoSchema      bool
}

// Format implements the NodeFormatter interface.
func (node *Insert) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	if node.OnConflict.IsUpsertAlias() {
		ctx.WriteString("UPSERT")
	} else {
		ctx.WriteString("INSERT")
		if node.IsNoSchema {
			ctx.WriteString(" WITHOUT SCHEMA ")
		}
	}
	ctx.WriteString(" INTO ")
	ctx.FormatNode(node.Table)
	if node.Columns != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&node.Columns)
		ctx.WriteByte(')')
	} else if node.IsNoSchema {
		ctx.WriteByte('(')
		ctx.FormatNode(&node.NoSchemaColumns)
		ctx.WriteByte(')')
	}
	if node.DefaultValues() {
		ctx.WriteString(" DEFAULT VALUES")
	} else {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Rows)
	}
	if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
		ctx.WriteString(" ON CONFLICT")
		if len(node.OnConflict.Columns) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.OnConflict.Columns)
			ctx.WriteString(")")
		}
		if node.OnConflict.DoNothing {
			ctx.WriteString(" DO NOTHING")
		} else {
			ctx.WriteString(" DO UPDATE SET ")
			ctx.FormatNode(&node.OnConflict.Exprs)
			if node.OnConflict.Where != nil {
				ctx.WriteByte(' ')
				ctx.FormatNode(node.OnConflict.Where)
			}
		}
	}
	if HasReturningClause(node.Returning) {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Returning)
	}
}

// DefaultValues returns true iff only default values are being inserted.
func (node *Insert) DefaultValues() bool {
	return node.Rows == nil || node.Rows.Select == nil
}

// OnConflict represents an `ON CONFLICT (columns) DO UPDATE SET exprs WHERE
// where` clause.
//
// The zero value for OnConflict is used to signal the UPSERT short form, which
// uses the primary key for as the conflict index and the values being inserted
// for Exprs.
type OnConflict struct {
	Columns   NameList
	Exprs     UpdateExprs
	Where     *Where
	DoNothing bool
}

// IsUpsertAlias returns true if the UPSERT syntactic sugar was used.
func (oc *OnConflict) IsUpsertAlias() bool {
	return oc != nil && oc.Columns == nil && oc.Exprs == nil && oc.Where == nil && !oc.DoNothing
}
