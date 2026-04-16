// Copyright 2015 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	IfExists bool
	Table    *UnresolvedObjectName
	Cmds     AlterTableCmds
}

// Format implements the NodeFormatter interface.
func (node *AlterTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Table)
	ctx.FormatNode(&node.Cmds)
}

// AlterTableCmds represents a list of table alterations.
type AlterTableCmds []AlterTableCmd

// Format implements the NodeFormatter interface.
func (node *AlterTableCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(",")
		}
		ctx.FormatNode(n)
	}
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
	NodeFormatter
	// TelemetryCounter returns the telemetry counter to increment
	// when this command is used.
	TelemetryCounter() telemetry.Counter
	// Placeholder function to ensure that only desired types
	// (AlterTable*) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()          {}
func (*AlterTableAddConstraint) alterTableCmd()      {}
func (*AlterTableAlterColumnType) alterTableCmd()    {}
func (*AlterTableAlterTagType) alterTableCmd()       {}
func (*AlterTableAlterPrimaryKey) alterTableCmd()    {}
func (*AlterTableDropColumn) alterTableCmd()         {}
func (*AlterTableDropConstraint) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()        {}
func (*AlterTableDropStored) alterTableCmd()         {}
func (*AlterTableSetNotNull) alterTableCmd()         {}
func (*AlterTableRenameColumn) alterTableCmd()       {}
func (*AlterTableRenameConstraint) alterTableCmd()   {}
func (*AlterTableRenameTable) alterTableCmd()        {}
func (*AlterTableSetAudit) alterTableCmd()           {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionBy) alterTableCmd()        {}
func (*AlterTableInjectStats) alterTableCmd()        {}
func (*AlterTableAddTag) alterTableCmd()             {}
func (*AlterTableDropTag) alterTableCmd()            {}
func (*AlterTableRenameTag) alterTableCmd()          {}
func (*AlterTableSetTag) alterTableCmd()             {}
func (*AlterTableSetRetentions) alterTableCmd()      {}
func (*AlterTableSetActivetime) alterTableCmd()      {}
func (*AlterPartitionInterval) alterTableCmd()       {}

var _ AlterTableCmd = &AlterTableAddColumn{}
var _ AlterTableCmd = &AlterTableAddConstraint{}
var _ AlterTableCmd = &AlterTableAlterColumnType{}
var _ AlterTableCmd = &AlterTableAlterTagType{}
var _ AlterTableCmd = &AlterTableDropColumn{}
var _ AlterTableCmd = &AlterTableDropConstraint{}
var _ AlterTableCmd = &AlterTableDropNotNull{}
var _ AlterTableCmd = &AlterTableDropStored{}
var _ AlterTableCmd = &AlterTableSetNotNull{}
var _ AlterTableCmd = &AlterTableRenameColumn{}
var _ AlterTableCmd = &AlterTableRenameConstraint{}
var _ AlterTableCmd = &AlterTableRenameTable{}
var _ AlterTableCmd = &AlterTableSetAudit{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionBy{}
var _ AlterTableCmd = &AlterTableInjectStats{}
var _ AlterTableCmd = &AlterTableAddTag{}
var _ AlterTableCmd = &AlterTableDropTag{}
var _ AlterTableCmd = &AlterTableRenameTag{}
var _ AlterTableCmd = &AlterTableSetTag{}
var _ AlterTableCmd = &AlterTableSetRetentions{}
var _ AlterTableCmd = &AlterTableSetActivetime{}
var _ AlterTableCmd = &AlterPartitionInterval{}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() Name
}

// AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	IfNotExists bool
	ColumnDef   *ColumnTableDef
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAddColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD COLUMN ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.ColumnDef)
}

// HoistAddColumnConstraints converts column constraints in ADD COLUMN commands,
// stored in node.Cmds, into top-level commands to add those constraints.
// Currently, this only applies to checks. For example, the ADD COLUMN in
//
//	ALTER TABLE t ADD COLUMN a INT CHECK (a < 1)
//
// is transformed into two commands, as in
//
//	ALTER TABLE t ADD COLUMN a INT, ADD CONSTRAINT check_a CHECK (a < 1)
//
// (with an auto-generated name).
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following statement is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//	ALTER TABLE t ADD COLUMN a INT CHECK (a < b)
func (node *AlterTable) HoistAddColumnConstraints() {
	var normalizedCmds AlterTableCmds

	for _, cmd := range node.Cmds {
		normalizedCmds = append(normalizedCmds, cmd)

		if t, ok := cmd.(*AlterTableAddColumn); ok {
			d := t.ColumnDef
			for _, checkExpr := range d.CheckExprs {
				normalizedCmds = append(normalizedCmds,
					&AlterTableAddConstraint{
						ConstraintDef: &CheckConstraintTableDef{
							Expr: checkExpr.Expr,
							Name: checkExpr.ConstraintName,
						},
						ValidationBehavior: ValidationDefault,
					},
				)
			}
			d.CheckExprs = nil
		}
	}
	node.Cmds = normalizedCmds
}

// ValidationBehavior specifies whether or not a constraint is validated.
type ValidationBehavior int

const (
	// ValidationDefault is the default validation behavior (immediate).
	ValidationDefault ValidationBehavior = iota
	// ValidationSkip skips validation of any existing data.
	ValidationSkip
)

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef      ConstraintTableDef
	ValidationBehavior ValidationBehavior
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAddConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_constraint")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD ")
	ctx.FormatNode(node.ConstraintDef)
	if node.ValidationBehavior == ValidationSkip {
		ctx.WriteString(" NOT VALID")
	}
}

// AlterTableAlterColumnType represents an ALTER TABLE ALTER COLUMN TYPE command.
type AlterTableAlterColumnType struct {
	Collation     string
	Column        Name
	ToType        *types.T
	Using         Expr
	EncodeAlgo    *string
	CompressAlgo  *string
	CompressLevel *string
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAlterColumnType) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "alter_column_type")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterColumnType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET DATA")
	if node.ToType != nil {
		ctx.WriteString(" TYPE ")
		ctx.WriteString(node.ToType.SQLString())
		if len(node.Collation) > 0 {
			ctx.WriteString(" COLLATE ")
			ctx.WriteString(node.Collation)
		}
		if node.Using != nil {
			ctx.WriteString(" USING ")
			ctx.FormatNode(node.Using)
		}
	}
	if node.EncodeAlgo != nil {
		ctx.WriteString(" ENCODE ")
		ctx.WriteString(*node.EncodeAlgo)
	}
	if node.CompressAlgo != nil {
		ctx.WriteString(" COMPRESS ")
		ctx.WriteString(*node.CompressAlgo)
	}
	if node.CompressLevel != nil {
		ctx.WriteString(" LEVEL ")
		ctx.WriteString(*node.CompressLevel)
	}
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableAlterColumnType) GetColumn() Name {
	return node.Column
}

// AlterTableAlterTagType represents an ALTER TABLE ALTER TAG TYPE command.
type AlterTableAlterTagType struct {
	Tag    Name
	ToType *types.T
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAlterTagType) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "alter_tag_type")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterTagType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER TAG ")
	ctx.FormatNode(&node.Tag)
	ctx.WriteString(" SET DATA TYPE ")
	ctx.WriteString(node.ToType.SQLString())
}

// AlterTableAlterPrimaryKey represents an ALTER TABLE ALTER PRIMARY KEY command.
type AlterTableAlterPrimaryKey struct {
	Columns    IndexElemList
	Interleave *InterleaveDef
	Sharded    *ShardedIndexDef
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAlterPrimaryKey) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "alter_primary_key")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterPrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER PRIMARY KEY USING COLUMNS (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteString(")")
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	IfExists     bool
	Column       Name
	DropBehavior DropBehavior
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_column")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP COLUMN ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Column)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   Name
	DropBehavior DropBehavior
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_constraint")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP CONSTRAINT ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Constraint)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableValidateConstraint represents a VALIDATE CONSTRAINT command.
type AlterTableValidateConstraint struct {
	Constraint Name
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableValidateConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "validate_constraint")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableValidateConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" VALIDATE CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
}

// AlterTableRenameTable represents an ALTE RTABLE RENAME TO command.
type AlterTableRenameTable struct {
	NewName TableName
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameTable) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_table")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameTable) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameColumn represents an ALTER TABLE RENAME [COLUMN] command.
type AlterTableRenameColumn struct {
	Column  Name
	NewName Name
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_column")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameConstraint represents an ALTER TABLE RENAME CONSTRAINT command.
type AlterTableRenameConstraint struct {
	Constraint Name
	NewName    Name
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_constraint")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	Column  Name
	Default Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() Name {
	return node.Column
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetDefault) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_default")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	if node.Default == nil {
		ctx.WriteString(" DROP DEFAULT")
	} else {
		ctx.WriteString(" SET DEFAULT ")
		ctx.FormatNode(node.Default)
	}
}

// AlterTableSetNotNull represents an ALTER COLUMN SET NOT NULL
// command.
type AlterTableSetNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetNotNull) GetColumn() Name {
	return node.Column
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetNotNull) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_not_null")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET NOT NULL")
}

// AlterTableDropNotNull represents an ALTER COLUMN DROP NOT NULL
// command.
type AlterTableDropNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() Name {
	return node.Column
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropNotNull) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_not_null")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP NOT NULL")
}

// AlterTableDropStored represents an ALTER COLUMN DROP STORED command
// to remove the computed-ness from a column.
type AlterTableDropStored struct {
	Column Name
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableDropStored) GetColumn() Name {
	return node.Column
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropStored) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_stored")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropStored) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP STORED")
}

// AlterTablePartitionBy represents an ALTER TABLE PARTITION BY
// command.
type AlterTablePartitionBy struct {
	*PartitionBy
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTablePartitionBy) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "partition_by")
}

// Format implements the NodeFormatter interface.
func (node *AlterTablePartitionBy) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionBy)
}

// AuditMode represents a table audit mode
type AuditMode int

const (
	// AuditModeDisable is the default mode - no audit.
	AuditModeDisable AuditMode = iota
	// AuditModeReadWrite enables audit on read or write statements.
	AuditModeReadWrite
)

var auditModeName = [...]string{
	AuditModeDisable:   "OFF",
	AuditModeReadWrite: "READ WRITE",
}

func (m AuditMode) String() string {
	return auditModeName[m]
}

// TelemetryName returns a friendly string for use in telemetry that represents
// the AuditMode.
func (m AuditMode) TelemetryName() string {
	return strings.ReplaceAll(strings.ToLower(m.String()), " ", "_")
}

// AlterTableSetAudit represents an ALTER TABLE AUDIT SET statement.
type AlterTableSetAudit struct {
	Mode AuditMode
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetAudit) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_audit")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetAudit) Format(ctx *FmtCtx) {
	ctx.WriteString(" EXPERIMENTAL_AUDIT SET ")
	ctx.WriteString(node.Mode.String())
}

// AlterTableInjectStats represents an ALTER TABLE INJECT STATISTICS statement.
type AlterTableInjectStats struct {
	Stats Expr
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableInjectStats) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "inject_stats")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableInjectStats) Format(ctx *FmtCtx) {
	ctx.WriteString(" INJECT STATISTICS ")
	ctx.FormatNode(node.Stats)
}

// AlterTableAddTag represents an ALTER TABLE ADD TAG statement.
type AlterTableAddTag struct {
	Tag Tag
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddTag) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD TAG ")
	ctx.FormatNode(&node.Tag)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAddTag) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_tag")
}

// AlterTableDropTag represents an ALTER TABLE DROP TAG statement.
type AlterTableDropTag struct {
	TagName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropTag) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP TAG ")
	ctx.FormatNode(&node.TagName)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropTag) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_tag")
}

// AlterTableRenameTag represents an ALTER TABLE RENAME TAG statement.
type AlterTableRenameTag struct {
	OldName Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameTag) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TAG ")
	ctx.FormatNode(&node.OldName)
	ctx.WriteString(" ")
	ctx.FormatNode(&node.NewName)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameTag) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_tag")
}

// AlterTableSetTag represents an ALTER TABLE SET TAG statement.
type AlterTableSetTag struct {
	Expr *UpdateExpr
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetTag) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET TAG ")
	ctx.FormatNode(node.Expr)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetTag) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_tag")
}

// AlterTableSetRetentions represents an ALTER TABLE SET RETENTIONS statement.
type AlterTableSetRetentions struct {
	TimeInput TimeInput
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetRetentions) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET RETENTIONS = ")
	ctx.FormatNode(&node.TimeInput)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetRetentions) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_retentions")
}

// AlterTableSetActivetime represents an ALTER TABLE SET ACTIVETIME statement.
type AlterTableSetActivetime struct {
	TimeInput TimeInput
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetActivetime) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET ACTIVETIME = ")
	ctx.FormatNode(&node.TimeInput)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetActivetime) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_activetime")
}

// AlterPartitionInterval represents an ALTER TABLE SET PARTITION INTERVAL statement.
type AlterPartitionInterval struct {
	TimeInput TimeInput
}

// Format implements the NodeFormatter interface.
func (node *AlterPartitionInterval) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET PARTITION INTERVAL = ")
	ctx.FormatNode(&node.TimeInput)
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterPartitionInterval) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_pratition_interval")
}
