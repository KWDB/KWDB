// Copyright 2012, Google Inc. All rights reserved.
// Copyright 2015 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.

// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
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

// SetVar represents a SET or RESET statement.
type SetVar struct {
	Name          string
	Values        Exprs
	IsUserDefined bool
}

// Format implements the NodeFormatter interface.
func (node *SetVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SET ")
	if node.Name == "" {
		ctx.WriteString("ROW (")
		ctx.FormatNode(&node.Values)
		ctx.WriteString(")")
	} else {
		ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
			// Session var names never contain PII and should be distinguished
			// for feature tracking purposes.
			ctx.FormatNameP(&node.Name)
		})

		ctx.WriteString(" = ")
		ctx.FormatNode(&node.Values)
	}
}

// SetClusterSetting represents a SET CLUSTER SETTING statement.
type SetClusterSetting struct {
	Name  string
	Value Expr
}

// Format implements the NodeFormatter interface.
func (node *SetClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("SET CLUSTER SETTING ")
	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
		ctx.FormatNameP(&node.Name)
	})

	ctx.WriteString(" = ")
	ctx.FormatNode(node.Value)
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *SetTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("SET TRANSACTION")
	node.Modes.Format(ctx)
}

// SetSessionAuthorizationDefault represents a SET SESSION AUTHORIZATION DEFAULT
// statement. This can be extended (and renamed) if we ever support names in the
// last position.
type SetSessionAuthorizationDefault struct{}

// Format implements the NodeFormatter interface.
func (node *SetSessionAuthorizationDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("SET SESSION AUTHORIZATION DEFAULT")
}

// SetSessionCharacteristics represents a SET SESSION CHARACTERISTICS AS TRANSACTION statement.
type SetSessionCharacteristics struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *SetSessionCharacteristics) Format(ctx *FmtCtx) {
	ctx.WriteString("SET SESSION CHARACTERISTICS AS TRANSACTION")
	node.Modes.Format(ctx)
}

// SetTracing represents a SET TRACING statement.
type SetTracing struct {
	Values Exprs
}

// Format implements the NodeFormatter interface.
func (node *SetTracing) Format(ctx *FmtCtx) {
	ctx.WriteString("SET TRACING = ")
	ctx.FormatNode(&node.Values)
}

// SelectInto represents a SELECT INTO statement.
type SelectInto struct {
	Names  UserDefinedVars
	Values *Select
}

// UserDefinedVars corresponds to an array of user defined variables in an expression.
type UserDefinedVars []UserDefinedVar

// Format implements the NodeFormatter interface.
func (node *SelectInto) Format(ctx *FmtCtx) {
	ctx.WriteString("SELECT ")
	if selClause, ok := node.Values.Select.(*SelectClause); ok {
		if selClause.Distinct {
			if selClause.DistinctOn != nil {
				ctx.FormatNode(&selClause.DistinctOn)
				ctx.WriteByte(' ')
			} else {
				ctx.WriteString("DISTINCT ")
			}
		}
		ctx.FormatNode(&selClause.Exprs)
		ctx.WriteString(" INTO")
		for i := range node.Names {
			ctx.WriteString(" ")
			ctx.FormatNode(&node.Names[i])
		}
		if len(selClause.From.Tables) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&selClause.From)
		}
		if selClause.Where != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(selClause.Where)
		}
		if len(selClause.GroupBy) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&selClause.GroupBy)
		}
		if selClause.Having != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(selClause.Having)
		}
		if len(selClause.Window) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&selClause.Window)
		}
	}
	if len(node.Values.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.Values.OrderBy)
	}
	if node.Values.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Values.Limit)
	}
}
