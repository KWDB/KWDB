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

import (
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
)

// EngineType specifies type of engine.
type EngineType int

const (
	// EngineTypeRelational represents a relational database.
	EngineTypeRelational EngineType = 0
	// EngineTypeTimeseries represents a timeseries database.
	EngineTypeTimeseries EngineType = 1
)

// EngineName converts EngineType to string of engine type
func EngineName(e EngineType) string {
	var engine string
	switch e {
	case EngineTypeRelational:
		return "RELATIONAL"
	case EngineTypeTimeseries:
		return "TIMESERIES"
	}
	return engine
}

// TSDatabase represents information about the timing database.
type TSDatabase struct {
	// DownSampling rules when create ts database including retentions, keep duration and method
	DownSampling      *DownSampling
	PartitionInterval *TimeInput
}

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	Name        Name
	Template    string
	Encoding    string
	Collate     string
	CType       string
	EngineType  EngineType
	TSDatabase  TSDatabase
	Comment     string
}

// Format implements the NodeFormatter interface.
func (node *CreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.EngineType == EngineTypeTimeseries {
		ctx.WriteString("TS ")
	}
	ctx.WriteString("DATABASE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	if node.Template != "" {
		ctx.WriteString(" TEMPLATE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Template, ctx.flags.EncodeFlags())
	}
	if node.Encoding != "" {
		ctx.WriteString(" ENCODING = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Encoding, ctx.flags.EncodeFlags())
	}
	if node.Collate != "" {
		ctx.WriteString(" LC_COLLATE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Collate, ctx.flags.EncodeFlags())
	}
	if node.CType != "" {
		ctx.WriteString(" LC_CTYPE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.CType, ctx.flags.EncodeFlags())
	}
	if node.EngineType == EngineTypeTimeseries {
		if node.TSDatabase.DownSampling != nil {
			ctx.FormatNode(node.TSDatabase.DownSampling)
		}
		if node.TSDatabase.PartitionInterval != nil {
			ctx.WriteString(" PARTITION INTERVAL ")
			ctx.Printf("%d", node.TSDatabase.PartitionInterval.Value)
			ctx.WriteString(node.TSDatabase.PartitionInterval.Unit)
		}
	}
	if node.Comment != "" {
		ctx.WriteString(" COMMENT = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Comment, ctx.flags.EncodeFlags())
	}
}

// AlterSchedule struct
type AlterSchedule struct {
	ScheduleName    Name
	Recurrence      Expr
	ScheduleOptions KVOptions
	IfExists        bool
}

var _ Statement = &AlterSchedule{}

// Format implements the NodeFormatter interface.
func (node *AlterSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SCHEDULE")
	if node.IfExists {
		ctx.WriteString(" IF EXISTS")
	}
	if node.ScheduleName != "" {
		ctx.WriteString(" ")
		node.ScheduleName.Format(ctx)
	}

	ctx.WriteString(" RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		node.Recurrence.Format(ctx)
	}

	if node.ScheduleOptions != nil {
		ctx.WriteString(" WITH EXPERIMENTAL SCHEDULE OPTIONS ")
		node.ScheduleOptions.Format(ctx)
	}
}

// CreateSchedule represents scheduled SQL job.
type CreateSchedule struct {
	SQL             string
	ScheduleName    Expr
	Recurrence      Expr
	ScheduleOptions KVOptions
	IfNotExists     bool
}

var _ Statement = &CreateSchedule{}

// Format implements the NodeFormatter interface.
func (node *CreateSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEDULE")
	if node.IfNotExists {
		ctx.WriteString(" IF NOT EXISTS")
	}
	if node.ScheduleName != nil {
		ctx.WriteString(" ")
		node.ScheduleName.Format(ctx)
	}

	ctx.WriteString(" FOR SQL")
	ctx.WriteString(node.SQL)

	ctx.WriteString(" RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		node.Recurrence.Format(ctx)
	}

	if node.ScheduleOptions != nil {
		ctx.WriteString(" WITH EXPERIMENTAL SCHEDULE OPTIONS ")
		node.ScheduleOptions.Format(ctx)
	}
}

// CreateStream represents a CREATE STREAM statement.
type CreateStream struct {
	StreamName  Name
	Table       TableName
	Options     KVOptions
	Query       *Select
	IfNotExists bool
}

var _ Statement = &CreateStream{}

// Format implements the NodeFormatter interface.
func (node *CreateStream) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE STREAM ")
	if node.IfNotExists {
		ctx.WriteString(" IF NOT EXISTS")
	}
	node.StreamName.Format(ctx)
	ctx.WriteString(" INTO ")
	ctx.FormatNode(&node.Table)
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS")
		ctx.FormatNode(&node.Options)
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.Query)
}

// AlterStream represents an ALTER STREAM statement.
type AlterStream struct {
	StreamName Name
	Options    KVOptions
}

var _ Statement = &AlterStream{}

// Format implements the NodeFormatter interface.
func (node *AlterStream) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER STREAM ")
	node.StreamName.Format(ctx)
	if node.Options != nil {
		ctx.WriteString(" SET OPTIONS ")
		ctx.FormatNode(&node.Options)
	}
}

// IndexElem represents a column with a direction in a CREATE INDEX statement.
type IndexElem struct {
	Column     Name
	Direction  Direction
	NullsOrder NullsOrder
}

// Format implements the NodeFormatter interface.
func (node *IndexElem) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Column)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
	if node.NullsOrder != DefaultNullsOrder {
		ctx.WriteByte(' ')
		ctx.WriteString(node.NullsOrder.String())
	}
}

// IndexElemList is list of IndexElem.
type IndexElemList []IndexElem

// Format pretty-prints the contained names separated by commas.
// Format implements the NodeFormatter interface.
func (l *IndexElemList) Format(ctx *FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*l)[i])
	}
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        Name
	Table       TableName
	Unique      bool
	Inverted    bool
	IfNotExists bool
	Columns     IndexElemList
	Sharded     *ShardedIndexDef
	// Extra columns to be stored together with the indexed ones as an optimization
	// for improved reading performance.
	Storing      NameList
	Interleave   *InterleaveDef
	PartitionBy  *PartitionBy
	Concurrently bool
}

// Format implements the NodeFormatter interface.
func (node *CreateIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Unique {
		ctx.WriteString("UNIQUE ")
	}
	if node.Inverted && !ctx.HasFlags(FmtPGIndexDef) {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if node.Concurrently {
		ctx.WriteString("CONCURRENTLY ")
	}
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("ON ")
	ctx.FormatNode(&node.Table)
	if ctx.HasFlags(FmtPGIndexDef) {
		ctx.WriteString(" USING")
		if node.Inverted {
			ctx.WriteString(" gin")
		} else {
			ctx.WriteString(" btree")
		}
	}
	ctx.WriteString(" (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if len(node.Storing) > 0 {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// TableDef represents a column, index or constraint definition within a CREATE
// TABLE statement.
type TableDef interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()

	// SetName replaces the name of the definition in-place. Used in the parser.
	SetName(name Name)
}

func (*ColumnTableDef) tableDef()               {}
func (*IndexTableDef) tableDef()                {}
func (*FamilyTableDef) tableDef()               {}
func (*ForeignKeyConstraintTableDef) tableDef() {}
func (*CheckConstraintTableDef) tableDef()      {}

// TableDefs represents a list of table definitions.
type TableDefs []TableDef

// Format implements the NodeFormatter interface.
func (node *TableDefs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// Nullability represents either NULL, NOT NULL or an unspecified value (silent
// NULL).
type Nullability int

// The values for NullType.
const (
	NotNull Nullability = iota
	Null
	SilentNull
)

// ColumnTableDef represents a column definition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name     Name
	Type     *types.T
	IsSerial bool
	Nullable struct {
		Nullability    Nullability
		ConstraintName Name
	}
	PrimaryKey struct {
		IsPrimaryKey bool
		Sharded      bool
		ShardBuckets Expr
	}
	Unique               bool
	UniqueConstraintName Name
	DefaultExpr          struct {
		Expr           Expr
		ConstraintName Name
	}
	CheckExprs []ColumnTableDefCheckExpr
	References struct {
		Table          *TableName
		Col            Name
		ConstraintName Name
		Actions        ReferenceActions
		Match          CompositeKeyMatchMethod
	}
	Computed struct {
		Computed bool
		Expr     Expr
	}
	Family struct {
		Name        Name
		Create      bool
		IfNotExists bool
	}
	Comment string
}

// ColumnTableDefCheckExpr represents a check constraint on a column definition
// within a CREATE TABLE statement.
type ColumnTableDefCheckExpr struct {
	Expr           Expr
	ConstraintName Name
}

func processCollationOnType(name Name, typ *types.T, c ColumnCollation) (*types.T, error) {
	switch typ.Family() {
	case types.StringFamily:
		return types.MakeCollatedString(typ, string(c)), nil
	case types.CollatedStringFamily:
		return nil, pgerror.Newf(pgcode.Syntax,
			"multiple COLLATE declarations for column %q", name)
	case types.ArrayFamily:
		elemTyp, err := processCollationOnType(name, typ.ArrayContents(), c)
		if err != nil {
			return nil, err
		}
		return types.MakeArray(elemTyp), nil
	default:
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"COLLATE declaration for non-string-typed column %q", name)
	}
}

// NewColumnTableDef constructs a column definition for a CreateTable statement.
func NewColumnTableDef(
	name Name, typ *types.T, isSerial bool, qualifications []NamedColumnQualification,
) (*ColumnTableDef, error) {
	d := &ColumnTableDef{
		Name:     name,
		Type:     typ,
		IsSerial: isSerial,
	}
	d.Nullable.Nullability = SilentNull
	for _, c := range qualifications {
		switch t := c.Qualification.(type) {
		case ColumnCollation:
			locale := string(t)
			_, err := language.Parse(locale)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax, "invalid locale %s", locale)
			}
			d.Type, err = processCollationOnType(name, d.Type, t)
			if err != nil {
				return nil, err
			}
		case *ColumnDefault:
			if d.HasDefaultExpr() {
				return nil, pgerror.Newf(pgcode.Syntax,
					"multiple default values specified for column %q", name)
			}
			d.DefaultExpr.Expr = t.Expr
			d.DefaultExpr.ConstraintName = c.Name
		case NotNullConstraint:
			if d.Nullable.Nullability == Null {
				return nil, pgerror.Newf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = NotNull
			d.Nullable.ConstraintName = c.Name
		case NullConstraint:
			if d.Nullable.Nullability == NotNull {
				return nil, pgerror.Newf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = Null
			d.Nullable.ConstraintName = c.Name
		case PrimaryKeyConstraint:
			d.PrimaryKey.IsPrimaryKey = true
			d.UniqueConstraintName = c.Name
		case ShardedPrimaryKeyConstraint:
			d.PrimaryKey.IsPrimaryKey = true
			constraint := c.Qualification.(ShardedPrimaryKeyConstraint)
			d.PrimaryKey.Sharded = true
			d.PrimaryKey.ShardBuckets = constraint.ShardBuckets
			d.UniqueConstraintName = c.Name
		case UniqueConstraint:
			d.Unique = true
			d.UniqueConstraintName = c.Name
		case *ColumnCheckConstraint:
			d.CheckExprs = append(d.CheckExprs, ColumnTableDefCheckExpr{
				Expr:           t.Expr,
				ConstraintName: c.Name,
			})
		case *ColumnFKConstraint:
			if d.HasFKConstraint() {
				return nil, pgerror.Newf(pgcode.InvalidTableDefinition,
					"multiple foreign key constraints specified for column %q", name)
			}
			d.References.Table = &t.Table
			d.References.Col = t.Col
			d.References.ConstraintName = c.Name
			d.References.Actions = t.Actions
			d.References.Match = t.Match
		case *ColumnComputedDef:
			d.Computed.Computed = true
			d.Computed.Expr = t.Expr
		case *ColumnFamilyConstraint:
			if d.HasColumnFamily() {
				return nil, pgerror.Newf(pgcode.InvalidTableDefinition,
					"multiple column families specified for column %q", name)
			}
			d.Family.Name = t.Family
			d.Family.Create = t.Create
			d.Family.IfNotExists = t.IfNotExists
		case ColumnComment:
			if d.Comment != "" {
				return nil, pgerror.Newf(pgcode.Syntax,
					"multiple comments specified for column %q", name)
			}
			d.Comment = string(t)
		default:
			return nil, errors.AssertionFailedf("unexpected column qualification: %T", c)
		}
	}
	return d, nil
}

// SetName implements the TableDef interface.
func (node *ColumnTableDef) SetName(name Name) {
	node.Name = name
}

// HasDefaultExpr returns if the ColumnTableDef has a default expression.
func (node *ColumnTableDef) HasDefaultExpr() bool {
	return node.DefaultExpr.Expr != nil
}

// HasFKConstraint returns if the ColumnTableDef has a foreign key constraint.
func (node *ColumnTableDef) HasFKConstraint() bool {
	return node.References.Table != nil
}

// IsComputed returns if the ColumnTableDef is a computed column.
func (node *ColumnTableDef) IsComputed() bool {
	return node.Computed.Computed
}

// HasColumnFamily returns if the ColumnTableDef has a column family.
func (node *ColumnTableDef) HasColumnFamily() bool {
	return node.Family.Name != "" || node.Family.Create
}

// Format implements the NodeFormatter interface.
func (node *ColumnTableDef) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Name)

	// ColumnTableDef node type will not be specified if it represents a CREATE
	// TABLE ... AS query.
	if node.Type != nil {
		ctx.WriteByte(' ')
		ctx.WriteString(node.columnTypeString())
	}

	if node.Nullable.Nullability != SilentNull && node.Nullable.ConstraintName != "" {
		ctx.WriteString(" CONSTRAINT ")
		ctx.FormatNode(&node.Nullable.ConstraintName)
	}
	switch node.Nullable.Nullability {
	case Null:
		ctx.WriteString(" NULL")
	case NotNull:
		ctx.WriteString(" NOT NULL")
	}
	if node.PrimaryKey.IsPrimaryKey || node.Unique {
		if node.UniqueConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.UniqueConstraintName)
		}
		if node.PrimaryKey.IsPrimaryKey {
			ctx.WriteString(" PRIMARY KEY")
			if node.PrimaryKey.Sharded {
				ctx.WriteString(" USING HASH WITH BUCKET_COUNT=")
				ctx.FormatNode(node.PrimaryKey.ShardBuckets)
			}
		} else if node.Unique {
			ctx.WriteString(" UNIQUE")
		}
	}
	if node.HasDefaultExpr() {
		if node.DefaultExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.DefaultExpr.ConstraintName)
		}
		ctx.WriteString(" DEFAULT ")
		ctx.FormatNode(node.DefaultExpr.Expr)
	}
	for _, checkExpr := range node.CheckExprs {
		if checkExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&checkExpr.ConstraintName)
		}
		ctx.WriteString(" CHECK (")
		ctx.FormatNode(checkExpr.Expr)
		ctx.WriteByte(')')
	}
	if node.HasFKConstraint() {
		if node.References.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.References.ConstraintName)
		}
		ctx.WriteString(" REFERENCES ")
		ctx.FormatNode(node.References.Table)
		if node.References.Col != "" {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.References.Col)
			ctx.WriteByte(')')
		}
		if node.References.Match != MatchSimple {
			ctx.WriteByte(' ')
			ctx.WriteString(node.References.Match.String())
		}
		ctx.FormatNode(&node.References.Actions)
	}
	if node.IsComputed() {
		ctx.WriteString(" AS (")
		ctx.FormatNode(node.Computed.Expr)
		ctx.WriteString(") STORED")
	}
	if node.HasColumnFamily() {
		if node.Family.Create {
			ctx.WriteString(" CREATE")
			if node.Family.IfNotExists {
				ctx.WriteString(" IF NOT EXISTS")
			}
		}
		ctx.WriteString(" FAMILY")
		if len(node.Family.Name) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.Family.Name)
		}
	}
	if node.Comment != "" {
		ctx.WriteString(" COMMENT = ")
		ctx.WriteString(node.Comment)
	}
}

func (node *ColumnTableDef) columnTypeString() string {
	if node.IsSerial {
		// Map INT types to SERIAL keyword.
		switch node.Type.Width() {
		case 16:
			return "SERIAL2"
		case 32:
			return "SERIAL4"
		}
		return "SERIAL8"
	}
	return node.Type.SQLString()
}

// String implements the fmt.Stringer interface.
func (node *ColumnTableDef) String() string { return AsString(node) }

// NamedColumnQualification wraps a NamedColumnQualification with a name.
type NamedColumnQualification struct {
	Name          Name
	Qualification ColumnQualification
}

// ColumnQualification represents a constraint on a column.
type ColumnQualification interface {
	columnQualification()
}

func (ColumnCollation) columnQualification()             {}
func (*ColumnDefault) columnQualification()              {}
func (NotNullConstraint) columnQualification()           {}
func (NullConstraint) columnQualification()              {}
func (PrimaryKeyConstraint) columnQualification()        {}
func (ShardedPrimaryKeyConstraint) columnQualification() {}
func (UniqueConstraint) columnQualification()            {}
func (*ColumnCheckConstraint) columnQualification()      {}
func (*ColumnComputedDef) columnQualification()          {}
func (*ColumnFKConstraint) columnQualification()         {}
func (*ColumnFamilyConstraint) columnQualification()     {}
func (ColumnComment) columnQualification()               {}

// ColumnCollation represents a COLLATE clause for a column.
type ColumnCollation string

// ColumnDefault represents a DEFAULT clause for a column.
type ColumnDefault struct {
	Expr Expr
}

// ColumnComment represents a Comment clause for a column.
type ColumnComment string

// NotNullConstraint represents NOT NULL on a column.
type NotNullConstraint struct{}

// NullConstraint represents NULL on a column.
type NullConstraint struct{}

// PrimaryKeyConstraint represents PRIMARY KEY on a column.
type PrimaryKeyConstraint struct{}

// ShardedPrimaryKeyConstraint represents `PRIMARY KEY .. USING HASH..`
// on a column.
type ShardedPrimaryKeyConstraint struct {
	Sharded      bool
	ShardBuckets Expr
}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// ColumnCheckConstraint represents either a check on a column.
type ColumnCheckConstraint struct {
	Expr Expr
}

// ColumnFKConstraint represents a FK-constaint on a column.
type ColumnFKConstraint struct {
	Table   TableName
	Col     Name // empty-string means use PK
	Actions ReferenceActions
	Match   CompositeKeyMatchMethod
}

// ColumnComputedDef represents the description of a computed column.
type ColumnComputedDef struct {
	Expr Expr
}

// ColumnFamilyConstraint represents FAMILY on a column.
type ColumnFamilyConstraint struct {
	Family      Name
	Create      bool
	IfNotExists bool
}

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name        Name
	Columns     IndexElemList
	Sharded     *ShardedIndexDef
	Storing     NameList
	Interleave  *InterleaveDef
	Inverted    bool
	PartitionBy *PartitionBy
}

// SetName implements the TableDef interface.
func (node *IndexTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *IndexTableDef) Format(ctx *FmtCtx) {
	if node.Inverted {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ConstraintTableDef represents a constraint definition within a CREATE TABLE
// statement.
type ConstraintTableDef interface {
	TableDef
	// Placeholder function to ensure that only desired types
	// (*ConstraintTableDef) conform to the ConstraintTableDef interface.
	constraintTableDef()
}

func (*UniqueConstraintTableDef) constraintTableDef()     {}
func (*ForeignKeyConstraintTableDef) constraintTableDef() {}
func (*CheckConstraintTableDef) constraintTableDef()      {}

// UniqueConstraintTableDef represents a unique constraint within a CREATE
// TABLE statement.
type UniqueConstraintTableDef struct {
	IndexTableDef
	PrimaryKey bool
}

// Format implements the NodeFormatter interface.
func (node *UniqueConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	if node.PrimaryKey {
		ctx.WriteString("PRIMARY KEY ")
	} else {
		ctx.WriteString("UNIQUE ")
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ReferenceAction is the method used to maintain referential integrity through
// foreign keys.
type ReferenceAction int

// The values for ReferenceAction.
const (
	NoAction ReferenceAction = iota
	Restrict
	SetNull
	SetDefault
	Cascade
)

var referenceActionName = [...]string{
	NoAction:   "NO ACTION",
	Restrict:   "RESTRICT",
	SetNull:    "SET NULL",
	SetDefault: "SET DEFAULT",
	Cascade:    "CASCADE",
}

func (ra ReferenceAction) String() string {
	return referenceActionName[ra]
}

// ReferenceActions contains the actions specified to maintain referential
// integrity through foreign keys for different operations.
type ReferenceActions struct {
	Delete ReferenceAction
	Update ReferenceAction
}

// Format implements the NodeFormatter interface.
func (node *ReferenceActions) Format(ctx *FmtCtx) {
	if node.Delete != NoAction {
		ctx.WriteString(" ON DELETE ")
		ctx.WriteString(node.Delete.String())
	}
	if node.Update != NoAction {
		ctx.WriteString(" ON UPDATE ")
		ctx.WriteString(node.Update.String())
	}
}

// CompositeKeyMatchMethod is the algorithm use when matching composite keys.
// See https://gitee.com/kwbasedb/kwbase/issues/20305 or
// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
// different composite foreign key matching methods.
type CompositeKeyMatchMethod int

// The values for CompositeKeyMatchMethod.
const (
	MatchSimple CompositeKeyMatchMethod = iota
	MatchFull
	MatchPartial // Note: PARTIAL not actually supported at this point.
)

var compositeKeyMatchMethodName = [...]string{
	MatchSimple:  "MATCH SIMPLE",
	MatchFull:    "MATCH FULL",
	MatchPartial: "MATCH PARTIAL",
}

func (c CompositeKeyMatchMethod) String() string {
	return compositeKeyMatchMethodName[c]
}

// ForeignKeyConstraintTableDef represents a FOREIGN KEY constraint in the AST.
type ForeignKeyConstraintTableDef struct {
	Name     Name
	Table    TableName
	FromCols NameList
	ToCols   NameList
	Actions  ReferenceActions
	Match    CompositeKeyMatchMethod
}

// Format implements the NodeFormatter interface.
func (node *ForeignKeyConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("FOREIGN KEY (")
	ctx.FormatNode(&node.FromCols)
	ctx.WriteString(") REFERENCES ")
	ctx.FormatNode(&node.Table)

	if len(node.ToCols) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&node.ToCols)
		ctx.WriteByte(')')
	}

	if node.Match != MatchSimple {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Match.String())
	}

	ctx.FormatNode(&node.Actions)
}

// SetName implements the TableDef interface.
func (node *ForeignKeyConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// CheckConstraintTableDef represents a check constraint within a CREATE
// TABLE statement.
type CheckConstraintTableDef struct {
	Name   Name
	Expr   Expr
	Hidden bool
}

// SetName implements the TableDef interface.
func (node *CheckConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *CheckConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("CHECK (")
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// FamilyTableDef represents a family definition within a CREATE TABLE
// statement.
type FamilyTableDef struct {
	Name    Name
	Columns NameList
}

// SetName implements the TableDef interface.
func (node *FamilyTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *FamilyTableDef) Format(ctx *FmtCtx) {
	ctx.WriteString("FAMILY ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
}

// ShardedIndexDef represents a hash sharded secondary index definition within a CREATE
// TABLE or CREATE INDEX statement.
type ShardedIndexDef struct {
	ShardBuckets Expr
}

// EvalShardBucketCount evaluates and checks the integer argument to a `USING HASH WITH
// BUCKET_COUNT` index creation query.
func EvalShardBucketCount(shardBuckets Expr) (int32, error) {
	const invalidBucketCountMsg = `BUCKET_COUNT must be a strictly positive integer value`
	cst, ok := shardBuckets.(*NumVal)
	if !ok {
		return 0, pgerror.New(pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	buckets, err := cst.AsInt32()
	if err != nil || buckets <= 0 {
		if err != nil {
			return 0, pgerror.Wrap(err, pgcode.InvalidParameterValue, invalidBucketCountMsg)
		}
		return 0, pgerror.New(pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	return buckets, nil
}

// Format implements the NodeFormatter interface.
func (node *ShardedIndexDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" USING HASH WITH BUCKET_COUNT = ")
	ctx.FormatNode(node.ShardBuckets)
}

// InterleaveDef represents an interleave definition within a CREATE TABLE
// or CREATE INDEX statement.
type InterleaveDef struct {
	Parent       TableName
	Fields       NameList
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *InterleaveDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" INTERLEAVE IN PARENT ")
	ctx.FormatNode(&node.Parent)
	ctx.WriteString(" (")
	for i := range node.Fields {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Fields[i])
	}
	ctx.WriteString(")")
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// PartitionByType is an enum of each type of partitioning (LIST/RANGE).
type PartitionByType string

const (
	// PartitionByList indicates a PARTITION BY LIST clause.
	PartitionByList PartitionByType = "LIST"
	// PartitionByRange indicates a PARTITION BY LIST clause.
	PartitionByRange PartitionByType = "RANGE"
)

// PartitionBy represents an PARTITION BY definition within a CREATE/ALTER
// TABLE/INDEX statement.
type PartitionBy struct {
	Fields NameList
	// Exactly one of List or Range is required to be non-empty.
	List      []ListPartition
	Range     []RangePartition
	HashPoint []HashPointPartition
	IsHash    bool
}

// Format implements the NodeFormatter interface.
func (node *PartitionBy) Format(ctx *FmtCtx) {
	if node == nil {
		ctx.WriteString(` PARTITION BY NOTHING`)
		return
	}
	if len(node.List) > 0 {
		if node.IsHash {
			ctx.WriteString(` PARTITION BY HASH (`)
		} else {
			ctx.WriteString(` PARTITION BY LIST (`)
		}
	} else if len(node.Range) > 0 {
		ctx.WriteString(` PARTITION BY RANGE (`)
	}
	ctx.FormatNode(&node.Fields)
	ctx.WriteString(`) (`)
	for i := range node.List {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.List[i])
	}
	for i := range node.Range {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Range[i])
	}
	ctx.WriteString(`)`)
}

// ListPartition represents a PARTITION definition within a PARTITION BY LIST.
type ListPartition struct {
	Name         UnrestrictedName
	Exprs        Exprs
	Subpartition *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *ListPartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES IN (`)
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
}

// RangePartition represents a PARTITION definition within a PARTITION BY RANGE.
type RangePartition struct {
	Name         UnrestrictedName
	From         Exprs
	To           Exprs
	Subpartition *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *RangePartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES FROM (`)
	ctx.FormatNode(&node.From)
	ctx.WriteString(`) TO (`)
	ctx.FormatNode(&node.To)
	ctx.WriteByte(')')
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
}

// HashPointPartition represents a PARTITION definition within a PARTITION BY RANGE.
type HashPointPartition struct {
	Name       UnrestrictedName
	HashPoints []int32
	From       int32
	To         int32
}

// Format implements the NodeFormatter interface.
func (node *HashPointPartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(fmt.Sprintf("HashPoint VALUES  %+v", node.HashPoints))
}

// StorageParam is a key-value parameter for table storage.
type StorageParam struct {
	Key   Name
	Value Expr
}

// StorageParams is a list of StorageParams.
type StorageParams []StorageParam

// Format implements the NodeFormatter interface.
func (o *StorageParams) Format(ctx *FmtCtx) {
	for i := range *o {
		n := &(*o)[i]
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&n.Key)
		if n.Value != nil {
			ctx.WriteString(` = `)
			ctx.FormatNode(n.Value)
		}
	}
}

// CreateTableOnCommitSetting represents the CREATE TABLE ... ON COMMIT <action>
// parameters.
type CreateTableOnCommitSetting uint32

const (
	// CreateTableOnCommitUnset indicates that ON COMMIT was unset.
	CreateTableOnCommitUnset CreateTableOnCommitSetting = iota
	// CreateTableOnCommitPreserveRows indicates that ON COMMIT PRESERVE ROWS was set.
	CreateTableOnCommitPreserveRows
)

// TableType is an enum of table type
type TableType uint32

const (
	// RelationalTable represents relational table
	RelationalTable TableType = iota
	// TimeseriesTable represents time series table
	TimeseriesTable
	// TemplateTable represents template table
	TemplateTable
	// InstanceTable represents instance table
	InstanceTable
)

// TableTypeName converts TableType to string of table type
func TableTypeName(t TableType) string {
	var tableType string
	switch t {
	case RelationalTable:
		tableType = "RELATIONAL_TABLE"
	case TimeseriesTable:
		tableType = "TIMESERIES_TABLE"
	case TemplateTable:
		tableType = "TEMPLATE_TABLE"
	case InstanceTable:
		tableType = "INSTANCE_TABLE"
	}
	return tableType
}

// Tags is an array of Tag
type Tags []Tag

// Tag is a struct of tag attribute including name, type and value
type Tag struct {
	TagName  Name
	TagType  *types.T
	TagVal   Expr
	Nullable bool
	ColID    int
	IsSerial bool
	Comment  string
}

// Format implements the NodeFormatter interface.
func (node *Tags) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&n)
	}
}

// Format implements the NodeFormatter interface.
func (node *Tag) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.TagName)

	if node.TagType != nil {
		ctx.WriteByte(' ')
		ctx.WriteString(node.TagType.SQLString())
	}

	if node.TagVal != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.TagVal)
	}
	if node.Comment != "" {
		ctx.WriteString(" COMMENT = ")
		ctx.WriteString(node.Comment)
	}

}

// InsTableDef represents one instance table definition
type InsTableDef struct {
	Name        TableName
	UsingSource TableName
	Tags        Tags
}

// InsTableDefs represents multiple instance table's definitions.
type InsTableDefs []InsTableDef

// Format implements the NodeFormatter interface.
func (ctds InsTableDefs) Format(ctx *FmtCtx) {
	for _, ctd := range ctds {
		ctx.FormatNode(&ctd.Name)
		ctx.WriteString(" USING ")
		ctx.FormatNode(&ctd.UsingSource)

		var tagNameList NameList
		var tagValList Exprs
		for _, tag := range ctd.Tags {
			if len(tag.TagName) != 0 {
				tagNameList = append(tagNameList, tag.TagName)
			}
			tagValList = append(tagValList, tag.TagVal)
		}
		if len(tagNameList) != 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&tagNameList)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" ATTRIBUTES")
		ctx.WriteString(" (")
		ctx.FormatNode(&tagValList)
		ctx.WriteByte(')')
		ctx.WriteString(" ")
	}
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists   bool
	Table         TableName
	Interleave    *InterleaveDef
	PartitionBy   *PartitionBy
	Temporary     bool
	StorageParams StorageParams
	OnCommit      CreateTableOnCommitSetting
	// In CREATE...AS queries, Defs represents a list of ColumnTableDefs, one for
	// each column, and a ConstraintTableDef for each constraint on a subset of
	// these columns.
	Defs       TableDefs
	AsSource   *Select
	ActiveTime *TimeInput
	TableType  TableType
	Tags       Tags
	// In InstanceTable, UsingSource represents this table belong to which SuperTable
	UsingSource TableName
	Instances   InsTableDefs

	// DownSampling rules when create table including retentions, keep duration and method
	// only support lifetime for now, e.g. retentions 1 day
	DownSampling *DownSampling
	// dict encoding
	Sde               bool
	PrimaryTagList    NameList
	PartitionInterval *TimeInput
	Comment           string
	HashNum           int64
}

// Retention including Resolution and KeepDuration
type Retention struct {
	Resolution   TimeInput
	KeepDuration TimeInput
}

// MemInput has MemCapacity input data in create portal statement
type MemInput struct {
	MemCapacity int64
	Value       int64
	Unit        string
}

// Method store agg and column name for downsampling
type Method struct {
	Agg string
	Col string
}

// MethodArray is an array of Method
type MethodArray []Method

// RetentionList is an array of Retention
type RetentionList []Retention

// DownSampling means down sampling rules including retentions, keep duration and method
type DownSampling struct {
	KeepDurationOrLifetime TimeInput
	// Retentions is an array of Retention in down sampling
	Retentions RetentionList
	// Method is an array of aggregation rule(first, last, avg, max, min)
	Methods MethodArray
}

// As returns true if this table represents a CREATE TABLE ... AS statement,
// false otherwise.
func (node *CreateTable) As() bool {
	return node.AsSource != nil
}

// IsTS returns true if this table represents a CREATE TS TABLE stmt,
// false otherwise.
func (node *CreateTable) IsTS() bool {
	if node.TableType == 1 || node.TableType == 2 || node.TableType == 3 {
		return true
	}
	return false
}

// AsHasUserSpecifiedPrimaryKey returns true if a CREATE TABLE ... AS statement
// has a PRIMARY KEY constraint specified.
func (node *CreateTable) AsHasUserSpecifiedPrimaryKey() bool {
	if node.As() {
		for _, def := range node.Defs {
			if d, ok := def.(*ColumnTableDef); !ok {
				return false
			} else if d.PrimaryKey.IsPrimaryKey {
				return true
			}
		}
	}
	return false
}

// Format implements the NodeFormatter interface.
func (node *CreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	ctx.WriteString("TABLE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	if len(node.Instances) == 0 {
		ctx.FormatNode(&node.Table)
		node.FormatBody(ctx)
	} else {
		node.Instances.Format(ctx)
	}
}

// FormatBody formats the "body" of the create table definition - everything
// but the CREATE TABLE tableName part.
func (node *CreateTable) FormatBody(ctx *FmtCtx) {
	if node.As() {
		if len(node.Defs) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.Defs)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" AS ")
		ctx.FormatNode(node.AsSource)
	} else if node.TableType == TemplateTable || node.TableType == TimeseriesTable {
		ctx.WriteString(" (")
		for i, def := range node.Defs {
			d, ok := def.(*ColumnTableDef)
			if ok {
				if i > 0 && d.Name != "" {
					ctx.WriteString(", ")
				}
				ctx.WriteString(string(d.Name))
				ctx.WriteByte(' ')
				ctx.WriteString(d.Type.SQLString())
				if d.Nullable.Nullability == NotNull {
					ctx.WriteString(" NOT NULL")
				}
			}
		}
		ctx.WriteByte(')')
		ctx.WriteString(" TAGS (")
		ctx.FormatTags(node.Tags)
		ctx.WriteByte(')')
		if node.TableType == TimeseriesTable {
			ctx.WriteString(" PRIMARY TAGS(")
			for i, priTag := range node.PrimaryTagList {
				ctx.WriteString(string(priTag))
				if i < len(node.PrimaryTagList)-1 {
					ctx.WriteString(", ")
				}
			}
			ctx.WriteByte(')')
		}
		if node.ActiveTime != nil {
			ctx.WriteString(" ACTIVETIME ")
			ctx.Printf("%d", node.ActiveTime.Value)
			ctx.WriteString(node.ActiveTime.Unit)
		}
		if node.HashNum != 0 {
			ctx.WriteString(" WITH HASH (")
			ctx.Printf("%d", node.HashNum)
			ctx.WriteString(strconv.FormatInt(node.HashNum, 10))
			ctx.WriteByte(')')
		}
		if node.DownSampling != nil {
			ctx.FormatNode(node.DownSampling)
		}
		if node.PartitionInterval != nil {
			ctx.WriteString(" PARTITION INTERVAL ")
			ctx.Printf("%d", node.PartitionInterval.Value)
			ctx.WriteString(node.PartitionInterval.Unit)
		}
	} else if node.TableType == InstanceTable {
		var tagNameList NameList
		var tagValList Exprs
		ctx.WriteString(" USING ")
		ctx.FormatNode(&node.UsingSource)
		for _, tag := range node.Tags {
			if len(tag.TagName) != 0 {
				tagNameList = append(tagNameList, tag.TagName)
			}
			tagValList = append(tagValList, tag.TagVal)
		}
		if len(tagNameList) != 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&tagNameList)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" ATTRIBUTES")
		ctx.WriteString(" (")
		ctx.FormatNode(&tagValList)
		ctx.WriteByte(')')
	} else {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Defs)
		ctx.WriteByte(')')
		if node.Interleave != nil {
			ctx.FormatNode(node.Interleave)
		}
		if node.PartitionBy != nil {
			ctx.FormatNode(node.PartitionBy)
		}
		// No storage parameters are implemented, so we never list the storage
		// parameters in the output format.
	}
	if node.Comment != "" {
		ctx.WriteString(" COMMENT = ")
		ctx.WriteString(node.Comment)
	}
}

// Format implements the NodeFormatter interface.
func (node *DownSampling) Format(ctx *FmtCtx) {
	ctx.WriteString(" RETENTIONS ")
	ctx.Printf("%d", node.KeepDurationOrLifetime.Value)
	ctx.WriteString(node.KeepDurationOrLifetime.Unit)
	if node.Retentions != nil {
		ctx.WriteByte(',')
		for i, retention := range node.Retentions {
			ctx.Printf(" %d%s : %d%s", retention.KeepDuration.Value, retention.KeepDuration.Unit, retention.Resolution.Value, retention.Resolution.Unit)
			if i < len(node.Retentions)-1 {
				ctx.WriteString(", ")
			}
		}
		ctx.WriteString(" SAMPLE ")
		for i := range node.Methods {
			if node.Methods[i].Col == "" {
				ctx.Printf("%s ", node.Methods[i].Agg)
			} else {
				ctx.Printf("%s(%s) ", node.Methods[i].Agg, node.Methods[i].Col)
			}
			if i < len(node.Methods)-1 {
				ctx.WriteString(", ")
			}
		}
	}
}

// HoistConstraints finds column check and foreign key constraints defined
// inline with their columns and makes them table-level constraints, stored in
// n.Defs. For example, the foreign key constraint in
//
//	CREATE TABLE foo (a INT REFERENCES bar(a))
//
// gets pulled into a top-level constraint like:
//
//	CREATE TABLE foo (a INT, FOREIGN KEY (a) REFERENCES bar(a))
//
// Similarly, the CHECK constraint in
//
//	CREATE TABLE foo (a INT CHECK (a < 1), b INT)
//
// gets pulled into a top-level constraint like:
//
//	CREATE TABLE foo (a INT, b INT, CHECK (a < 1))
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following table definition is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//	CREATE TABLE foo (a INT CHECK (a < b), b INT)
//
// Unique constraints are not hoisted.
func (node *CreateTable) HoistConstraints() {
	for _, d := range node.Defs {
		if col, ok := d.(*ColumnTableDef); ok {
			for _, checkExpr := range col.CheckExprs {
				node.Defs = append(node.Defs,
					&CheckConstraintTableDef{
						Expr: checkExpr.Expr,
						Name: checkExpr.ConstraintName,
					},
				)
			}
			col.CheckExprs = nil
			if col.HasFKConstraint() {
				var targetCol NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				node.Defs = append(node.Defs, &ForeignKeyConstraintTableDef{
					Table:    *col.References.Table,
					FromCols: NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
					Actions:  col.References.Actions,
					Match:    col.References.Match,
				})
				col.References.Table = nil
			}
		}
	}
}

// CreateSchema represents a CREATE SCHEMA statement.
type CreateSchema struct {
	IfNotExists bool
	Schema      Name
}

// Format implements the NodeFormatter interface.
func (node *CreateSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEMA ")

	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}

	ctx.WriteString(string(node.Schema))
}

// CreateSequence represents a CREATE SEQUENCE statement.
type CreateSequence struct {
	IfNotExists bool
	Name        TableName
	Temporary   bool
	Options     SequenceOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")

	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}

	ctx.WriteString("SEQUENCE ")

	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.FormatNode(&node.Options)
}

// SequenceOptions represents a list of sequence options.
type SequenceOptions []SequenceOption

// Format implements the NodeFormatter interface.
func (node *SequenceOptions) Format(ctx *FmtCtx) {
	for i := range *node {
		option := &(*node)[i]
		ctx.WriteByte(' ')
		switch option.Name {
		case SeqOptCycle, SeqOptNoCycle:
			ctx.WriteString(option.Name)
		case SeqOptCache:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			ctx.Printf("%d", *option.IntVal)
		case SeqOptMaxValue, SeqOptMinValue:
			if option.IntVal == nil {
				ctx.WriteString("NO ")
				ctx.WriteString(option.Name)
			} else {
				ctx.WriteString(option.Name)
				ctx.WriteByte(' ')
				ctx.Printf("%d", *option.IntVal)
			}
		case SeqOptStart:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("WITH ")
			}
			ctx.Printf("%d", *option.IntVal)
		case SeqOptIncrement:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("BY ")
			}
			ctx.Printf("%d", *option.IntVal)
		case SeqOptVirtual:
			ctx.WriteString(option.Name)
		case SeqOptOwnedBy:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			switch option.ColumnItemVal {
			case nil:
				ctx.WriteString("NONE")
			default:
				ctx.FormatNode(option.ColumnItemVal)
			}
		default:
			panic(errors.AssertionFailedf("unexpected SequenceOption: %v", option))
		}
	}
}

// SequenceOption represents an option on a CREATE SEQUENCE statement.
type SequenceOption struct {
	Name string

	IntVal *int64

	OptionalWord bool

	ColumnItemVal *ColumnItem
}

// Names of options on CREATE SEQUENCE.
const (
	SeqOptAs        = "AS"
	SeqOptCycle     = "CYCLE"
	SeqOptNoCycle   = "NO CYCLE"
	SeqOptOwnedBy   = "OWNED BY"
	SeqOptCache     = "CACHE"
	SeqOptIncrement = "INCREMENT"
	SeqOptMinValue  = "MINVALUE"
	SeqOptMaxValue  = "MAXVALUE"
	SeqOptStart     = "START"
	SeqOptVirtual   = "VIRTUAL"

	// Avoid unused warning for constants.
	_ = SeqOptAs
)

// ToRoleOptions converts KVOptions to a roleoption.List using
// typeAsString to convert exprs to strings.
func (o KVOptions) ToRoleOptions(
	typeAsStringOrNull func(e Expr, op string) (func() (bool, string, error), error), op string,
) (roleoption.List, error) {
	roleOptions := make(roleoption.List, len(o))

	for i, ro := range o {
		option, err := roleoption.ToOption(ro.Key.String())
		if err != nil {
			return nil, err
		}

		if ro.Value != nil {
			if ro.Value == DNull {
				roleOptions[i] = roleoption.RoleOption{
					Option: option, HasValue: true, Value: func() (bool, string, error) {
						return true, "", nil
					},
				}
			} else {
				strFn, err := typeAsStringOrNull(ro.Value, op)
				if err != nil {
					return nil, err
				}

				if err != nil {
					return nil, err
				}
				roleOptions[i] = roleoption.RoleOption{
					Option: option, Value: strFn, HasValue: true,
				}
			}
		} else {
			roleOptions[i] = roleoption.RoleOption{
				Option: option, HasValue: false,
			}
		}
	}

	return roleOptions, nil
}

func (o *KVOptions) formatAsRoleOptions(ctx *FmtCtx) {
	for _, option := range *o {
		ctx.WriteString(" ")
		ctx.WriteString(
			// "_" replaces space (" ") in YACC for handling tree.Name formatting.
			strings.ReplaceAll(
				strings.ToUpper(option.Key.String()), "_", " "),
		)

		// Password is a special case.
		if strings.ToUpper(option.Key.String()) == "PASSWORD" {
			ctx.WriteString(" ")
			if ctx.flags.HasFlags(FmtShowPasswords) {
				ctx.FormatNode(option.Value)
			} else {
				ctx.WriteString("'*****'")
			}
		} else if option.Value == DNull {
			ctx.WriteString(" ")
			ctx.FormatNode(option.Value)
		} else if option.Value != nil {
			ctx.WriteString(" ")
			ctx.FormatNode(option.Value)
		}
	}
}

// CreateFunction represents a CREATE FUNCTION statement.
type CreateFunction struct {
	FunctionName Name
	Arguments    FuncArgDefs
	ReturnType   *types.T
	FuncBody     string
}

// Format implements the NodeFormatter interface.
func (node *CreateFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	ctx.WriteString("FUNCTION ")
	ctx.WriteString(string(node.FunctionName))
	ctx.WriteString(" (")
	for i, arg := range node.Arguments {
		ctx.WriteString(string(arg.ArgName))
		ctx.WriteString(" ")
		ctx.WriteString(arg.ArgType.SQLString())
		if i < len(node.Arguments)-1 {
			ctx.WriteString(", ")
		}
	}
	ctx.WriteString(") ")
	ctx.WriteString("RETURNS ")
	ctx.WriteString(node.ReturnType.SQLString())
	ctx.WriteString(" LUA")
	ctx.WriteString(" BEGIN ")
	ctx.WriteString("'")
	ctx.WriteString(node.FuncBody)
	ctx.WriteString("'")
	ctx.WriteString(" END")
}

// FuncArgDefs is used for represent udf arguments
type FuncArgDefs []FuncArgDef

// FuncArgDef represents a argument definition within a CREATE FUNCTION statement.
type FuncArgDef struct {
	ArgName Name
	ArgType *types.T
}

// CreateRole represents a CREATE ROLE statement.
type CreateRole struct {
	Name        Expr
	IfNotExists bool
	IsRole      bool
	KVOptions   KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateRole) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.Name)

	if len(node.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		node.KVOptions.formatAsRoleOptions(ctx)
	}
}

// AlterRole represents an ALTER ROLE statement.
type AlterRole struct {
	Name      Expr
	IfExists  bool
	IsRole    bool
	KVOptions KVOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterRole) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)

	if len(node.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		node.KVOptions.formatAsRoleOptions(ctx)
	}
}

// CreateView represents a CREATE VIEW statement.
type CreateView struct {
	Name        TableName
	ColumnNames NameList
	AsSource    *Select
	IfNotExists bool
	Temporary   bool
	// Materialized stands if create a materialized view.
	Materialized bool
}

// Format implements the NodeFormatter interface.
func (node *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")

	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	if node.Materialized {
		ctx.WriteString("MATERIALIZED ")
	}
	ctx.WriteString("VIEW ")

	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)

	if len(node.ColumnNames) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&node.ColumnNames)
		ctx.WriteByte(')')
	}

	ctx.WriteString(" AS ")
	ctx.FormatNode(node.AsSource)
}

// RefreshMaterializedView represents a REFRESH MATERIALIZED VIEW statement.
type RefreshMaterializedView struct {
	Name *UnresolvedObjectName
}

var _ Statement = &RefreshMaterializedView{}

// Format implements the NodeFormatter interface.
func (node *RefreshMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("REFRESH MATERIALIZED VIEW ")
	ctx.FormatNode(node.Name)
}

// CreateStats represents a CREATE STATISTICS statement.
type CreateStats struct {
	Name        Name
	ColumnIDs   ColumnIDList
	ColumnNames NameList
	Table       TableExpr
	Options     CreateStatsOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateStats) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE STATISTICS ")
	ctx.FormatNode(&node.Name)

	if len(node.ColumnNames) > 0 {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&node.ColumnNames)
	}

	ctx.WriteString(" FROM ")
	ctx.FormatNode(node.Table)

	if !node.Options.Empty() {
		ctx.WriteString(" WITH OPTIONS ")
		ctx.FormatNode(&node.Options)
	}
}

// CreateStatsOptions contains options for CREATE STATISTICS.
type CreateStatsOptions struct {
	// Throttling enables throttling and indicates the fraction of time we are
	// idling (between 0 and 1).
	Throttling float64

	// AsOf performs a historical read at the given timestamp.
	// Note that the timestamp will be moved up during the operation if it gets
	// too old (in order to avoid problems with TTL expiration).
	AsOf AsOfClause

	// SortedHistogram enables sorting histogram collection for entities in time series tables
	SortedHistogram bool
}

// Empty returns true if no options were provided.
func (o *CreateStatsOptions) Empty() bool {
	return o.Throttling == 0 && o.AsOf.Expr == nil
}

// Format implements the NodeFormatter interface.
func (o *CreateStatsOptions) Format(ctx *FmtCtx) {
	sep := ""
	if o.Throttling != 0 {
		fmt.Fprintf(ctx, "THROTTLING %g", o.Throttling)
		sep = " "
	}
	if o.AsOf.Expr != nil {
		ctx.WriteString(sep)
		ctx.FormatNode(&o.AsOf)
		sep = " "
	}
}

// CombineWith combines two options, erroring out if the two options contain
// incompatible settings.
func (o *CreateStatsOptions) CombineWith(other *CreateStatsOptions) error {
	if other.Throttling != 0 {
		if o.Throttling != 0 {
			return errors.New("THROTTLING specified multiple times")
		}
		o.Throttling = other.Throttling
	}
	if other.AsOf.Expr != nil {
		if o.AsOf.Expr != nil {
			return errors.New("AS OF specified multiple times")
		}
		o.AsOf = other.AsOf
	}
	return nil
}

// CreateAudit represents a CREATE AUDIT statement.
type CreateAudit struct {
	IfNotExists bool
	Name        Name
	Target      AuditTarget
	Operations  NameList
	Operators   NameList
	Condition   Expr
	Whenever    string
	Action      Expr
	Level       Expr
}

// AuditTarget contains type and target name of audit.
type AuditTarget struct {
	Type string
	Name TableName
}

// Format implements the NodeFormatter interface.
func (t *AuditTarget) Format(ctx *FmtCtx) {
	if t.Type != "" {
		ctx.WriteString(t.Type)
	}
	if t.Name.TableName != "" {
		ctx.FormatNode(&t.Name)
	}
}

// Format implements the NodeFormatter interface.
func (n *CreateAudit) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE AUDIT ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&n.Target)
	ctx.WriteString(" FOR ")
	ctx.FormatNode(&n.Operations)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&n.Operators)
	if n.Condition != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(n.Condition)
	}
	if n.Whenever != "" {
		ctx.WriteString(" WHENEVER ")
		ctx.FormatName(n.Whenever)
	}
	if n.Action != nil {
		ctx.WriteString(" ACTION ")
		ctx.FormatNode(n.Action)
	}
	if n.Level != nil {
		ctx.WriteString(" LEVEL ")
		ctx.FormatNode(n.Level)
	}
}

// TimeInput is used for definition of retention/partition interval
type TimeInput struct {
	Value int64
	Unit  string
}

// Format implements the NodeFormatter interface.
func (node *TimeInput) Format(ctx *FmtCtx) {
	ctx.WriteString(strconv.FormatInt(node.Value, 10))
	ctx.WriteString(node.Unit)
}
