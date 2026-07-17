// Copyright 2012, Google Inc. All rights reserved.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

// DropBehavior represents options for dropping schema elements.
type DropBehavior int

// DropBehavior values.
const (
	DropDefault DropBehavior = iota
	DropRestrict
	DropCascade
)

var dropBehaviorName = [...]string{
	DropDefault:  "",
	DropRestrict: "RESTRICT",
	DropCascade:  "CASCADE",
}

func (d DropBehavior) String() string {
	return dropBehaviorName[d]
}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// dropDatabaseKeyword is the SQL keyword for DROP DATABASE.
const dropDatabaseKeyword = "DROP DATABASE "

// Format implements the NodeFormatter interface.
func (node *DropDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString(dropDatabaseKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Name)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	IndexList    TableIndexNames
	IfExists     bool
	DropBehavior DropBehavior
	Concurrently bool
}

// dropIndexKeyword is the SQL keyword for DROP INDEX.
const dropIndexKeyword = "DROP INDEX "

// Format implements the NodeFormatter interface.
func (node *DropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString(dropIndexKeyword)
	formatConcurrentlyClause(ctx, node.Concurrently)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.IndexList)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropPipe represents a DROP PIPE statement.
type DropPipe struct {
	PipeName Name
	IfExists bool
}

var _ Statement = &DropPipe{}

// Format implements the NodeFormatter interface.
func (node *DropPipe) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP PIPE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	node.PipeName.Format(ctx)
}

// DropPublication represents a DROP PUBLICATION statement.
type DropPublication struct {
	PubName  Name
	IfExists bool
}

var _ Statement = &DropPublication{}

// Format implements the NodeFormatter interface.
func (node *DropPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP PUBLICATION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	node.PubName.Format(ctx)
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// dropTableKeyword is the SQL keyword for DROP TABLE.
const dropTableKeyword = "DROP TABLE "

// Format implements the NodeFormatter interface.
func (node *DropTable) Format(ctx *FmtCtx) {
	ctx.WriteString(dropTableKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Names)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Names          TableNames
	IfExists       bool
	DropBehavior   DropBehavior
	IsMaterialized bool
}

// Format implements the NodeFormatter interface.
func (node *DropView) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP ")
	if node.IsMaterialized {
		ctx.WriteString("MATERIALIZED ")
	}
	ctx.WriteString("VIEW ")
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Names)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropSequence represents a DROP SEQUENCE statement.
type DropSequence struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// dropSequenceKeyword is the SQL keyword for DROP SEQUENCE.
const dropSequenceKeyword = "DROP SEQUENCE "

// Format implements the NodeFormatter interface.
func (node *DropSequence) Format(ctx *FmtCtx) {
	ctx.WriteString(dropSequenceKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Names)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropRole represents a DROP ROLE statement
type DropRole struct {
	Names    Exprs
	IsRole   bool
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropRole) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP")
	node.writeRoleOrUserKeyword(ctx)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Names)
}

// writeRoleOrUserKeyword appends either " ROLE " or " USER " based on IsRole.
func (node *DropRole) writeRoleOrUserKeyword(ctx *FmtCtx) {
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
}

// DropSchema represents a DROP SCHEMA command.
type DropSchema struct {
	Names        []string
	IfExists     bool
	DropBehavior DropBehavior
}

// dropSchemaKeyword is the SQL keyword for DROP SCHEMA.
const dropSchemaKeyword = "DROP SCHEMA "

// Format implements the NodeFormatter interface.
func (node *DropSchema) Format(ctx *FmtCtx) {
	ctx.WriteString(dropSchemaKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	formatCommaSeparatedNames(ctx, node.Names)
	formatDropBehaviorClause(ctx, node.DropBehavior)
}

// DropFunction represents a DROP function command.
type DropFunction struct {
	Names    []string
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP FUNCTION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	for i := range node.Names {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&node.Names[i])
	}
}

// DropProcedure represents a DROP Procedure command.
type DropProcedure struct {
	Name     TableName
	IfExists bool
}

// dropProcedureKeyword is the SQL keyword for DROP PROCEDURE.
const dropProcedureKeyword = "DROP PROCEDURE "

// Format implements the NodeFormatter interface.
func (node *DropProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString(dropProcedureKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Name)
}

// DropTrigger represents a DROP TRIGGER command.
type DropTrigger struct {
	Name     Name
	IfExists bool
	Table    TableName
}

// dropTriggerKeyword is the SQL keyword for DROP TRIGGER.
const dropTriggerKeyword = "DROP TRIGGER "

// Format implements the NodeFormatter interface.
func (node *DropTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString(dropTriggerKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Table)
}

// DropAudit represents a DROP AUDIT statement.
type DropAudit struct {
	Names    NameList
	IfExists bool
}

// dropAuditKeyword is the SQL keyword for DROP AUDIT.
const dropAuditKeyword = "DROP AUDIT "

// Format implements the NodeFormatter interface.
func (node *DropAudit) Format(ctx *FmtCtx) {
	ctx.WriteString(dropAuditKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	ctx.FormatNode(&node.Names)
}

// DropStream represents a DROP PIPE statement.
type DropStream struct {
	StreamName Name
	IfExists   bool
}

var _ Statement = &DropStream{}

// dropStreamKeyword is the SQL keyword for DROP STREAM.
const dropStreamKeyword = "DROP STREAM "

// Format implements the NodeFormatter interface.
func (node *DropStream) Format(ctx *FmtCtx) {
	ctx.WriteString(dropStreamKeyword)
	formatIfExistsClause(ctx, node.IfExists)
	node.StreamName.Format(ctx)
}

// dropFunctionKeyword is the SQL keyword for DROP FUNCTION.
const dropFunctionKeyword = "DROP FUNCTION "

// formatIfExistsClause writes "IF EXISTS " when the flag is true.
func formatIfExistsClause(ctx *FmtCtx, ifExists bool) {
	if ifExists {
		ctx.WriteString("IF EXISTS ")
	}
}

// formatConcurrentlyClause writes "CONCURRENTLY " when the flag is true.
func formatConcurrentlyClause(ctx *FmtCtx, concurrently bool) {
	if concurrently {
		ctx.WriteString("CONCURRENTLY ")
	}
}

// formatDropBehaviorClause appends the drop behavior keyword when it is not
// the default.
func formatDropBehaviorClause(ctx *FmtCtx, behavior DropBehavior) {
	if behavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(behavior.String())
	}
}

// formatCommaSeparatedNames outputs a list of string names separated by commas.
func formatCommaSeparatedNames(ctx *FmtCtx, names []string) {
	for idx := range names {
		if idx > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&names[idx])
	}
}
