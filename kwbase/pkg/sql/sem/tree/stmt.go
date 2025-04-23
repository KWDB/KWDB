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
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.

package tree

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
)

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// StatementType is the enumerated type for Statement return styles on
// the wire.
type StatementType int

//go:generate stringer -type=StatementType
const (
	// Ack indicates that the statement does not have a meaningful
	// return. Examples include SET, BEGIN, COMMIT.
	Ack StatementType = iota
	// DDL indicates that the statement mutates the database schema.
	//
	// Note: this is the type indicated back to the client; it is not a
	// sufficient test for schema mutation for planning purposes. There
	// are schema-modifying statements (e.g. CREATE TABLE AS) which
	// report RowsAffected to the client, not DDL.
	// Use CanModifySchema() below instead.
	DDL
	// RowsAffected indicates that the statement returns the count of
	// affected rows.
	RowsAffected
	// Rows indicates that the statement returns the affected rows after
	// the statement was applied.
	Rows
	// CopyIn indicates a COPY FROM statement.
	CopyIn
	// Unknown indicates that the statement does not have a known
	// return style at the time of parsing. This is not first in the
	// enumeration because it is more convenient to have Ack as a zero
	// value, and because the use of Unknown should be an explicit choice.
	// The primary example of this statement type is EXECUTE, where the
	// statement type depends on the statement type of the prepared statement
	// being executed.
	Unknown
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType() StatementType
	// StatementTag is a short string identifying the type of statement
	// (usually a single verb). This is different than the Stringer output,
	// which is the actual statement (including args).
	// TODO(dt): Currently tags are always pg-compatible in the future it
	// might make sense to pass a tag format specifier.
	StatementTag() string

	// StatOp represent operation of statement, used in audit.
	StatOp() string

	// StatTargetType represent the target type of statement, used in audit.
	StatTargetType() string
}

// canModifySchema is to be implemented by statements that can modify
// the database schema but may have StatementType() != DDL.
// See CanModifySchema() below.
type canModifySchema interface {
	modifiesSchema() bool
}

// CanModifySchema returns true if the statement can modify
// the database schema.
func CanModifySchema(stmt Statement) bool {
	if stmt.StatementType() == DDL {
		return true
	}
	scm, ok := stmt.(canModifySchema)
	return ok && scm.modifiesSchema()
}

// CanWriteData returns true if the statement can modify data.
func CanWriteData(stmt Statement) bool {
	switch stmt.(type) {
	// Normal write operations.
	case *Insert, *Delete, *Update, *Truncate:
		return true
	// Import operations.
	case *CopyFrom, *Import, *Restore:
		return true
	// CockroachDB extensions.
	case *Split, *Unsplit, *Relocate, *Scatter, *SelectInto:
		return true
	}
	return false
}

// IsStmtParallelized determines if a given statement's execution should be
// parallelized. This means that its results should be mocked out, and that
// it should be run asynchronously and in parallel with other statements that
// are independent.
func IsStmtParallelized(stmt Statement) bool {
	parallelizedRetClause := func(ret ReturningClause) bool {
		_, ok := ret.(*ReturningNothing)
		return ok
	}
	switch s := stmt.(type) {
	case *Delete:
		return parallelizedRetClause(s.Returning)
	case *Insert:
		return parallelizedRetClause(s.Returning)
	case *Update:
		return parallelizedRetClause(s.Returning)
	}
	return false
}

// HiddenFromShowQueries is a pseudo-interface to be implemented
// by statements that should not show up in SHOW QUERIES (and are hence
// not cancellable using CANCEL QUERIES either). Usually implemented by
// statements that spawn jobs.
type HiddenFromShowQueries interface {
	hiddenFromShowQueries()
}

// ObserverStatement is a marker interface for statements which are allowed to
// run regardless of the current transaction state: statements other than
// rollback are generally rejected if the session is in a failed transaction
// state, but it's convenient to allow some statements (e.g. "show syntax; set
// tracing").
// Such statements are not expected to modify the database, the transaction or
// session state (other than special cases such as enabling/disabling tracing).
//
// These statements short-circuit the regular execution - they don't get planned
// (there are no corresponding planNodes). The connExecutor recognizes them and
// handles them.
type ObserverStatement interface {
	observerStatement()
}

// CCLOnlyStatement is used to make CCL only functions
type CCLOnlyStatement interface {
	cclOnlyStatement()
}

// StatementType implements the Statement interface.
func (*AlterTSDatabase) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterTSDatabase) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTSDatabase) StatementTag() string { return "ALTER TS DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*AlterTSDatabase) StatTargetType() string { return "TS DATABASE" }

// StatementType implements the Statement interface.
func (*AlterIndex) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterIndex) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterIndex) StatementTag() string { return "ALTER INDEX" }

// StatTargetType implements the StatTargetType interface.
func (*AlterIndex) StatTargetType() string { return "INDEX" }

func (*AlterIndex) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterTable) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTable) StatementTag() string { return "ALTER TABLE" }

// StatTargetType implements the StatTargetType interface.
func (*AlterTable) StatTargetType() string { return "TABLE" }

func (*AlterTable) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterSequence) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterSequence) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterSequence) StatementTag() string { return "ALTER SEQUENCE" }

// StatTargetType implements the StatTargetType interface.
func (*AlterSequence) StatTargetType() string { return "SEQUENCE" }

// StatementType implements the Statement interface.
func (*AlterRole) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*AlterRole) StatOp() string { return string(target.Alter) }

// StatementTag returns a short string identifying the type of statement.
func (n *AlterRole) StatementTag() string {
	if n.IsRole {
		return "ALTER ROLE"
	}
	return "ALTER USER"
}

// StatTargetType implements the StatTargetType interface.
func (n *AlterRole) StatTargetType() string {
	if n.IsRole {
		return "ROLE"
	}
	return "USER"
}

func (*AlterRole) cclOnlyStatement() {}

func (*AlterRole) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterAudit) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterAudit) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterAudit) StatementTag() string { return "ALTER AUDIT" }

// StatTargetType implements the StatTargetType interface.
func (*AlterAudit) StatTargetType() string { return "AUDIT" }

// StatementType implements the Statement interface.
func (*Backup) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Backup) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Backup) StatementTag() string { return "BACKUP" }

// StatTargetType implements the StatTargetType interface.
func (*Backup) StatTargetType() string { return "BACKUP" }

func (*Backup) cclOnlyStatement() {}

func (*Backup) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*BeginTransaction) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*BeginTransaction) StatementTag() string { return "BEGIN" }

// StatTargetType implements the StatTargetType interface.
func (*BeginTransaction) StatTargetType() string { return "TRANSACTION" }

// StatementType implements the Statement interface.
func (*ControlJobs) StatementType() StatementType { return RowsAffected }

// StatOp implements the StatOp interface.
func (c *ControlJobs) StatOp() string {
	if c.Command == PauseJob {
		return "PAUSE"
	} else if c.Command == CancelJob {
		return "CANCEL"
	}
	return "RESUME"
}

// StatementTag returns a short string identifying the type of statement.
func (c *ControlJobs) StatementTag() string {
	return fmt.Sprintf("%s JOBS", JobCommandToStatement[c.Command])
}

// StatTargetType implements the StatTargetType interface.
func (*ControlJobs) StatTargetType() string { return "JOB" }

// StatementType implements the Statement interface.
func (*CancelQueries) StatementType() StatementType { return RowsAffected }

// StatementTag returns a short string identifying the type of statement.
func (*CancelQueries) StatementTag() string { return "CANCEL QUERIES" }

// StatOp implements the StatOp interface.
func (*CancelQueries) StatOp() string { return "CANCEL" }

// StatTargetType implements the StatTargetType interface.
func (*CancelQueries) StatTargetType() string { return "QUERY" }

// StatementType implements the Statement interface.
func (*CancelSessions) StatementType() StatementType { return RowsAffected }

// StatOp implements the StatOp interface.
func (*CancelSessions) StatOp() string { return "CANCEL" }

// StatementTag returns a short string identifying the type of statement.
func (*CancelSessions) StatementTag() string { return "CANCEL SESSIONS" }

// StatTargetType implements the StatTargetType interface.
func (*CancelSessions) StatTargetType() string { return "SESSION" }

// StatementType implements the Statement interface.
func (*CannedOptPlan) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*CannedOptPlan) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CannedOptPlan) StatementTag() string { return "PREPARE AS OPT PLAN" }

// StatTargetType implements the StatTargetType interface.
func (*CannedOptPlan) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*CommentOnColumn) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CommentOnColumn) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnColumn) StatementTag() string { return "COMMENT ON COLUMN" }

// StatTargetType implements the StatTargetType interface.
func (*CommentOnColumn) StatTargetType() string { return "COLUMN" }

// StatementType implements the Statement interface.
func (*CommentOnDatabase) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CommentOnDatabase) StatOp() string { return "COMMENT" }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnDatabase) StatementTag() string { return "COMMENT ON DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*CommentOnDatabase) StatTargetType() string { return "DATABASE" }

// StatementType implements the Statement interface.
func (*CommentOnIndex) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CommentOnIndex) StatOp() string { return "COMMENT" }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnIndex) StatementTag() string { return "COMMENT ON INDEX" }

// StatTargetType implements the StatTargetType interface.
func (*CommentOnIndex) StatTargetType() string { return "INDEX" }

// StatementType implements the Statement interface.
func (*CommentOnTable) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CommentOnTable) StatOp() string { return "COMMENT" }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnTable) StatementTag() string { return "COMMENT ON TABLE" }

// StatTargetType implements the StatTargetType interface.
func (*CommentOnTable) StatTargetType() string { return "TABLE" }

// StatementType implements the Statement interface.
func (*Compress) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Compress) StatOp() string { return "COMPRESS" }

// StatementTag returns a short string identifying the type of statement.
func (n *Compress) StatementTag() string { return "COMPRESS" }

// StatTargetType implements the StatTargetType interface.
func (n *Compress) StatTargetType() string {
	if n.Typ != CompressTypeTable {
		return "DATABASE"
	}
	return "TABLE"
}

// StatementType implements the Statement interface.
func (*CommitTransaction) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*CommitTransaction) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CommitTransaction) StatementTag() string { return "COMMIT" }

// StatTargetType implements the StatTargetType interface.
func (*CommitTransaction) StatTargetType() string { return "TRANSACTION" }

// StatementType implements the Statement interface.
func (*CopyFrom) StatementType() StatementType { return CopyIn }

// StatOp implements the StatOp interface.
func (*CopyFrom) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CopyFrom) StatementTag() string { return "COPY" }

// StatTargetType implements the StatTargetType interface.
func (*CopyFrom) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*CreateAudit) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateAudit) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateAudit) StatementTag() string { return "CREATE AUDIT" }

// StatTargetType implements the StatTargetType interface.
func (*CreateAudit) StatTargetType() string { return "AUDIT" }

// StatementType implements the Statement interface.
func (*CreateChangefeed) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*CreateChangefeed) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateChangefeed) StatementTag() string {
	if n.SinkURI == nil {
		return "EXPERIMENTAL CHANGEFEED"
	}
	return "CREATE CHANGEFEED"
}

// StatTargetType implements the StatTargetType interface.
func (*CreateChangefeed) StatTargetType() string { return "CHANGEFEED" }

func (*CreateChangefeed) cclOnlyStatement() {}

// StatementType implements the Statement interface.
func (*CreateDatabase) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateDatabase) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateDatabase) StatementTag() string {
	if n.EngineType == EngineTypeTimeseries {
		return "CREATE TS DATABASE"
	}
	return "CREATE DATABASE"
}

// StatTargetType implements the StatTargetType interface.
func (*CreateDatabase) StatTargetType() string { return "DATABASE" }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateIndex) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateIndex) StatementTag() string { return "CREATE INDEX" }

// StatTargetType implements the StatTargetType interface.
func (*CreateIndex) StatTargetType() string { return "INDEX" }

// StatementType implements the Statement interface.
func (n *CreateSchema) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateSchema) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateSchema) StatementTag() string {
	return "CREATE SCHEMA"
}

// StatTargetType implements the StatTargetType interface.
func (*CreateSchema) StatTargetType() string { return "SCHEMA" }

// modifiesSchema implements the canModifySchema interface.
func (*CreateSchema) modifiesSchema() bool { return true }

// StatementType implements the Statement interface.
func (n *CreateTable) StatementType() StatementType {
	if len(n.Instances) == 0 {
		return DDL
	}
	return Rows
}

// StatOp implements the StatOp interface.
func (*CreateTable) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateTable) StatementTag() string {
	if n.As() {
		return "CREATE TABLE AS"
	}
	return "CREATE TABLE"
}

// StatTargetType implements the StatTargetType interface.
func (*CreateTable) StatTargetType() string { return "TABLE" }

// modifiesSchema implements the canModifySchema interface.
func (*CreateTable) modifiesSchema() bool { return true }

// StatementType implements the Statement interface.
func (*CreateRole) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*CreateRole) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateRole) StatementTag() string {
	if n.IsRole {
		return "CREATE ROLE"
	}
	return "CREATE USER"
}

// StatTargetType implements the StatTargetType interface.
func (n *CreateRole) StatTargetType() string {
	if n.IsRole {
		return "ROLE"
	}
	return "USER"
}

func (*CreateRole) cclOnlyStatement() {}

func (*CreateRole) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*CreateView) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateView) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateView) StatementTag() string { return "CREATE VIEW" }

// StatTargetType implements the StatTargetType interface.
func (*CreateView) StatTargetType() string { return "VIEW" }

// StatementType implements the Statement interface.
func (*CreateSequence) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateSequence) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSequence) StatementTag() string { return "CREATE SEQUENCE" }

// StatTargetType implements the StatTargetType interface.
func (*CreateSequence) StatTargetType() string { return "SEQUENCE" }

// StatementType implements the Statement interface.
func (*CreateFunction) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateFunction) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateFunction) StatementTag() string { return "CREATE FUNCTION" }

// StatTargetType implements the StatTargetType interface.
func (*CreateFunction) StatTargetType() string { return "FUNCTION" }

// StatementType implements the Statement interface.
func (*CreateSchedule) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateSchedule) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSchedule) StatementTag() string { return "CREATE SCHEDULE" }

// StatTargetType implements the StatTargetType interface.
func (*CreateSchedule) StatTargetType() string { return "SCHEDULE" }

// StatementType implements the Statement interface.
func (*PauseSchedule) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*PauseSchedule) StatOp() string { return "PAUSE" }

// StatementTag returns a short string identifying the type of statement.
func (*PauseSchedule) StatementTag() string { return "PAUSE SCHEDULE" }

// StatTargetType implements the StatTargetType interface.
func (*PauseSchedule) StatTargetType() string { return "SCHEDULE" }

// StatementType implements the Statement interface.
func (*ResumeSchedule) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*ResumeSchedule) StatOp() string { return "RESUME" }

// StatementTag returns a short string identifying the type of statement.
func (*ResumeSchedule) StatementTag() string { return "RESUME SCHEDULE" }

// StatTargetType implements the StatTargetType interface.
func (*ResumeSchedule) StatTargetType() string { return "SCHEDULE" }

// StatementType implements the Statement interface.
func (*AlterSchedule) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*AlterSchedule) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterSchedule) StatementTag() string { return "ALTER SCHEDULE" }

// StatTargetType implements the StatTargetType interface.
func (*AlterSchedule) StatTargetType() string { return "SCHEDULE" }

// StatementType implements the Statement interface.
func (*CreateStats) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*CreateStats) StatOp() string { return "CREATE" }

// StatementTag returns a short string identifying the type of statement.
func (*CreateStats) StatementTag() string { return "CREATE STATISTICS" }

// StatTargetType implements the StatTargetType interface.
func (*CreateStats) StatTargetType() string { return "STATISTICS" }

// StatementType implements the Statement interface.
func (*Deallocate) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Deallocate) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *Deallocate) StatementTag() string {
	// Postgres distinguishes the command tags for these two cases of Deallocate statements.
	if n.Name == "" {
		return "DEALLOCATE ALL"
	}
	return "DEALLOCATE"
}

// StatTargetType implements the StatTargetType interface.
func (*Deallocate) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Discard) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Discard) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Discard) StatementTag() string { return "DISCARD" }

// StatTargetType implements the StatTargetType interface.
func (*Discard) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (n *Delete) StatementType() StatementType { return n.Returning.statementType() }

// StatementTag returns a short string identifying the type of statement.
func (*Delete) StatementTag() string { return "DELETE" }

// StatOp implements the StatOp interface.
func (n *Delete) StatOp() string { return "DELETE" }

// StatTargetType implements the StatTargetType interface.
func (*Delete) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*DropDatabase) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropDatabase) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropDatabase) StatementTag() string { return "DROP DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*DropDatabase) StatTargetType() string { return "DATABASE" }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropIndex) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropIndex) StatementTag() string { return "DROP INDEX" }

// StatTargetType implements the StatTargetType interface.
func (*DropIndex) StatTargetType() string { return "INDEX" }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropTable) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropTable) StatementTag() string { return "DROP TABLE" }

// StatTargetType implements the StatTargetType interface.
func (*DropTable) StatTargetType() string { return "TABLE" }

// StatementType implements the Statement interface.
func (*DropView) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropView) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropView) StatementTag() string { return "DROP VIEW" }

// StatTargetType implements the StatTargetType interface.
func (*DropView) StatTargetType() string { return "VIEW" }

// StatementType implements the Statement interface.
func (*DropSequence) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropSequence) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropSequence) StatementTag() string { return "DROP SEQUENCE" }

// StatTargetType implements the StatTargetType interface.
func (*DropSequence) StatTargetType() string { return "SEQUENCE" }

// StatementType implements the Statement interface.
func (n *DropRole) StatementType() StatementType {
	if n.IsRole {
		return Ack
	}
	return RowsAffected
}

// StatOp implements the StatOp interface.
func (*DropRole) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (n *DropRole) StatementTag() string {
	if n.IsRole {
		return "DROP ROLE"
	}
	return "DROP USER"
}

// StatTargetType implements the StatTargetType interface.
func (n *DropRole) StatTargetType() string {
	if n.IsRole {
		return "ROLE"
	}
	return "USER"
}

func (*DropRole) cclOnlyStatement() {}

func (*DropRole) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*DropSchedule) StatementType() StatementType { return RowsAffected }

// StatementTag implements the Statement interface.
func (*DropSchedule) StatementTag() string { return "DROP SCHEDULE" }

// StatOp implements the StatOp interface.
func (*DropSchedule) StatOp() string { return "DROP" }

// StatTargetType implements the StatTargetType interface.
func (*DropSchedule) StatTargetType() string { return "SCHEDULE" }

// StatementType implements the Statement interface.
func (*DropSchema) StatementType() StatementType { return DDL }

// StatementTag implements the Statement interface.
func (*DropSchema) StatementTag() string { return "DROP SCHEMA" }

// StatOp implements the StatOp interface.
func (*DropSchema) StatOp() string { return "DROP" }

// StatTargetType implements the StatTargetType interface.
func (*DropSchema) StatTargetType() string { return "SCHEMA" }

// StatTargetType implements the StatTargetType interface.
func (*DropFunction) StatTargetType() string { return "FUNCTION" }

// StatementType implements the Statement interface.
func (*DropFunction) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropFunction) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropFunction) StatementTag() string { return "DROP FUNCTION" }

// StatementType implements the Statement interface.
func (*DropAudit) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*DropAudit) StatOp() string { return "DROP" }

// StatementTag returns a short string identifying the type of statement.
func (*DropAudit) StatementTag() string { return "DROP AUDIT" }

// StatTargetType implements the StatTargetType interface.
func (*DropAudit) StatTargetType() string { return "AUDIT" }

// StatementType implements the Statement interface.
func (*Execute) StatementType() StatementType { return Unknown }

// StatOp implements the StatOp interface.
func (*Execute) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Execute) StatementTag() string { return "EXECUTE" }

// StatTargetType implements the StatTargetType interface.
func (*Execute) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Explain) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Explain) StatOp() string { return "EXPLAIN" }

// StatementTag returns a short string identifying the type of statement.
func (*Explain) StatementTag() string { return "EXPLAIN" }

// StatTargetType implements the StatTargetType interface.
func (*Explain) StatTargetType() string { return "QUERY" }

// StatementType implements the Statement interface.
func (*ExplainAnalyzeDebug) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ExplainAnalyzeDebug) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ExplainAnalyzeDebug) StatementTag() string { return "EXPLAIN ANALYZE (DEBUG)" }

// StatTargetType implements the StatTargetType interface.
func (*ExplainAnalyzeDebug) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Export) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Export) StatOp() string { return "EXPORT" }

// StatTargetType implements the StatTargetType interface.
func (n *Export) StatTargetType() string {
	if n.Database != "" {
		return "DATABASE"
	}
	return "TABLE"
}

func (*Export) cclOnlyStatement() {}

// StatementTag returns a short string identifying the type of statement.
func (*Export) StatementTag() string { return "EXPORT" }

// StatementType implements the Statement interface.
func (*Grant) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*Grant) StatOp() string { return "GRANT" }

// StatementTag returns a short string identifying the type of statement.
func (*Grant) StatementTag() string { return "GRANT" }

// StatTargetType implements the StatTargetType interface.
func (*Grant) StatTargetType() string { return "PRIVILEGE" }

// StatementType implements the Statement interface.
func (*GrantRole) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*GrantRole) StatOp() string { return "GRANT" }

// StatementTag returns a short string identifying the type of statement.
func (*GrantRole) StatementTag() string { return "GRANT" }

// StatTargetType implements the StatTargetType interface.
func (*GrantRole) StatTargetType() string { return "ROLE" }

func (*GrantRole) cclOnlyStatement() {}

// StatementType implements the Statement interface.
func (n *Insert) StatementType() StatementType { return n.Returning.statementType() }

// StatOp implements the StatOp interface.
func (*Insert) StatOp() string { return "INSERT" }

// StatementTag returns a short string identifying the type of statement.
func (*Insert) StatementTag() string { return "INSERT" }

// StatTargetType implements the StatTargetType interface.
func (*Insert) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (n *Import) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Import) StatementTag() string { return "IMPORT" }

// StatOp implements the StatOp interface.
func (*Import) StatOp() string { return string(target.Import) }

// StatTargetType returns a short string identifying the type of statement.
func (*Import) StatTargetType() string { return "" }

func (*Import) cclOnlyStatement() {}

// StatementType implements the Statement interface.
func (*ImportPortal) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ImportPortal) StatementTag() string { return "IMPORT KWDB" }

// StatOp implements the StatOp interface.
func (*ImportPortal) StatOp() string { return "Import KWDB" }

// StatTargetType implements the StatTargetType interface.
func (*ImportPortal) StatTargetType() string { return "Import KWDB" }

// StatementType implements the Statement interface.
func (*ParenSelect) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ParenSelect) StatOp() string { return "SELECT" }

// StatementTag returns a short string identifying the type of statement.
func (*ParenSelect) StatementTag() string { return "SELECT" }

// StatTargetType implements the StatTargetType interface.
func (*ParenSelect) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Prepare) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Prepare) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Prepare) StatementTag() string { return "PREPARE" }

// StatTargetType implements the StatTargetType interface.
func (*Prepare) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*RebalanceTsData) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*RebalanceTsData) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*RebalanceTsData) StatementTag() string { return "REBALANCE" }

// StatTargetType implements the StatTargetType interface.
func (*RebalanceTsData) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*RefreshMaterializedView) StatementType() StatementType { return DDL }

// StatementTag implements the Statement interface.
func (*RefreshMaterializedView) StatementTag() string { return "REFRESH MATERIALIZED VIEW" }

// StatTargetType implements the Statement interface.
func (*RefreshMaterializedView) StatTargetType() string { return "MATERIALIZED VIEW" }

// StatOp implements the Statement interface.
func (n *RefreshMaterializedView) StatOp() string { return "REFRESH" }

// StatementType implements the Statement interface.
func (*ReleaseSavepoint) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*ReleaseSavepoint) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ReleaseSavepoint) StatementTag() string { return "RELEASE" }

// StatTargetType implements the StatTargetType interface.
func (*ReleaseSavepoint) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*RenameColumn) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*RenameColumn) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*RenameColumn) StatementTag() string { return "RENAME COLUMN" }

// StatTargetType implements the StatTargetType interface.
func (*RenameColumn) StatTargetType() string { return "COLUMN" }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*RenameDatabase) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*RenameDatabase) StatementTag() string { return "RENAME DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*RenameDatabase) StatTargetType() string { return "DATABASE" }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*RenameIndex) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*RenameIndex) StatementTag() string { return "RENAME INDEX" }

// StatTargetType implements the StatTargetType interface.
func (*RenameIndex) StatTargetType() string { return "INDEX" }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*RenameTable) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (n *RenameTable) StatementTag() string {
	if n.IsView {
		return "RENAME VIEW"
	} else if n.IsSequence {
		return "RENAME SEQUENCE"
	}
	return "RENAME TABLE"
}

// StatTargetType implements the StatTargetType interface.
func (n *RenameTable) StatTargetType() string {
	if n.IsView {
		return "VIEW"
	} else if n.IsSequence {
		return "SEQUENCE"
	}
	return "TABLE"
}

// StatementType implements the Statement interface.
func (*Relocate) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Relocate) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *Relocate) StatementTag() string {
	if n.RelocateLease {
		return "EXPERIMENTAL_RELOCATE LEASE"
	}
	return "EXPERIMENTAL_RELOCATE"
}

// StatTargetType implements the StatTargetType interface.
func (*Relocate) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ReplicationControl) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (n *ReplicationControl) StatementTag() string {
	return " REPLICATION  " + ReplicationCommandToStatement[n.Command]
}

// StatOp implements the StatOp interface.
func (n *ReplicationControl) StatOp() string { return ReplicationCommandToStatement[n.Command] }

// StatTargetType implements the StatTargetType interface.
func (*ReplicationControl) StatTargetType() string { return " REPLICATION " }

// StatementType implements the Statement interface.
func (*ReplicateSetSecondary) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*ReplicateSetSecondary) StatementTag() string { return "REPLICATE SET SECONDARY" }

// StatOp implements the StatOp interface.
func (*ReplicateSetSecondary) StatOp() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*ReplicateSetSecondary) StatTargetType() string { return "SECONDARY" }

// StatementType implements the Statement interface.
func (*ReplicateSetRole) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*ReplicateSetRole) StatementTag() string { return "REPLICATE SET ROLE" }

// StatOp implements the StatOp interface.
func (*ReplicateSetRole) StatOp() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*ReplicateSetRole) StatTargetType() string { return "ROLE" }

// StatementType implements the Statement interface.
func (*Restore) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Restore) StatOp() string { return string(target.Restore) }

// StatementTag returns a short string identifying the type of statement.
func (*Restore) StatementTag() string { return "RESTORE" }

func (*Restore) cclOnlyStatement() {}

// StatTargetType implements the StatTargetType interface.
func (*Restore) StatTargetType() string { return "" }

func (*Restore) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*Revoke) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*Revoke) StatOp() string { return "REVOKE" }

// StatementTag returns a short string identifying the type of statement.
func (*Revoke) StatementTag() string { return "REVOKE" }

// StatTargetType implements the StatTargetType interface.
func (*Revoke) StatTargetType() string { return "PRIVILEGE" }

// StatementType implements the Statement interface.
func (*RevokeRole) StatementType() StatementType { return DDL }

// StatOp implements the StatOp interface.
func (*RevokeRole) StatOp() string { return "REVOKE" }

// StatementTag returns a short string identifying the type of statement.
func (*RevokeRole) StatementTag() string { return "REVOKE" }

// StatTargetType implements the StatTargetType interface.
func (*RevokeRole) StatTargetType() string { return "ROLE" }

func (*RevokeRole) cclOnlyStatement() {}

// StatementType implements the Statement interface.
func (*RollbackToSavepoint) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*RollbackToSavepoint) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackToSavepoint) StatementTag() string { return "ROLLBACK" }

// StatTargetType implements the StatTargetType interface.
func (*RollbackToSavepoint) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*RollbackTransaction) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*RollbackTransaction) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackTransaction) StatementTag() string { return "ROLLBACK" }

// StatTargetType implements the StatTargetType interface.
func (*RollbackTransaction) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Savepoint) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Savepoint) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Savepoint) StatementTag() string { return "SAVEPOINT" }

// StatTargetType implements the StatTargetType interface.
func (*Savepoint) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Scatter) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Scatter) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*Scatter) StatementTag() string { return "SCATTER" }

// StatTargetType implements the StatTargetType interface.
func (n *Scatter) StatTargetType() string {
	if n.TableOrIndex.Index != "" {
		return "INDEX"
	}
	return "Table"
}

// StatementType implements the Statement interface.
func (*Scrub) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Scrub) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *Scrub) StatementTag() string { return "SCRUB" }

// StatTargetType implements the StatTargetType interface.
func (*Scrub) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Select) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Select) StatOp() string { return "SELECT" }

// StatementTag returns a short string identifying the type of statement.
func (*Select) StatementTag() string { return "SELECT" }

// StatTargetType implements the StatTargetType interface.
func (*Select) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SelectClause) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*SelectClause) StatOp() string { return "SELECT" }

// StatementTag returns a short string identifying the type of statement.
func (*SelectClause) StatementTag() string { return "SELECT" }

// StatTargetType implements the StatTargetType interface.
func (*SelectClause) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SetVar) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*SetVar) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetVar) StatementTag() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*SetVar) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SetClusterSetting) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (n *SetClusterSetting) StatOp() string {
	if _, ok := n.Value.(DefaultVal); ok {
		return "RESET"
	}
	return "SET"
}

// StatementTag returns a short string identifying the type of statement.
func (*SetClusterSetting) StatementTag() string { return "SET CLUSTER SETTING" }

// StatTargetType implements the StatTargetType interface.
func (*SetClusterSetting) StatTargetType() string { return "CLUSTERSETTINGS" }

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*SetTransaction) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetTransaction) StatementTag() string { return "SET TRANSACTION" }

// StatTargetType implements the StatTargetType interface.
func (*SetTransaction) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SetTracing) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*SetTracing) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetTracing) StatementTag() string { return "SET TRACING" }

// observerStatement implements the ObserverStatement interface.
func (*SetTracing) observerStatement() {}

// StatTargetType implements the StatTargetType interface.
func (*SetTracing) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SetZoneConfig) StatementType() StatementType { return RowsAffected }

// StatOp implements the StatOp interface.
func (*SetZoneConfig) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*SetZoneConfig) StatementTag() string { return "CONFIGURE ZONE" }

// StatTargetType implements the StatTargetType interface.
func (n *SetZoneConfig) StatTargetType() string {
	if n.ZoneSpecifier.Database != "" {
		return "DATABASE"
	} else if n.ZoneSpecifier.NamedZone != "" {
		return "RANGE"
	} else if n.ZoneSpecifier.TargetsIndex() {
		return "INDEX"
	}
	return "TABLE"
}

// StatementType implements the Statement interface.
func (*SetSessionAuthorizationDefault) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*SetSessionAuthorizationDefault) StatOp() string { return "SET" }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionAuthorizationDefault) StatementTag() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*SetSessionAuthorizationDefault) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SetSessionCharacteristics) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*SetSessionCharacteristics) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionCharacteristics) StatementTag() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*SetSessionCharacteristics) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowVar) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowVar) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowVar) StatementTag() string { return "SHOW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowVar) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowUdvVar) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowUdvVar) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowUdvVar) StatementTag() string { return "SHOW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowUdvVar) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowClusterSetting) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowClusterSetting) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSetting) StatementTag() string { return "SHOW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowClusterSetting) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowApplications) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowApplications) StatementTag() string { return "SHOW APPLICATIONS" }

// StatOp implements the StatOp interface.
func (*ShowApplications) StatOp() string { return "SHOW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowApplications) StatTargetType() string { return "APPLICATIONS" }

// StatementType implements the Statement interface.
func (*ShowTags) StatementType() StatementType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTagValues) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowTags) StatOp() string { return "" }

// StatOp implements the StatOp interface.
func (*ShowTagValues) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTags) StatementTag() string { return "SHOW TAGS" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTagValues) StatementTag() string { return "SHOW TAG VALUES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTags) StatTargetType() string { return "" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTagValues) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowRetentions) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowRetentions) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRetentions) StatementTag() string { return "SHOW RETENTIONS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowRetentions) StatTargetType() string { return "RETENTIONS" }

// StatementType implements the Statement interface.
func (*ShowClusterSettingList) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowClusterSettingList) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSettingList) StatementTag() string { return "SHOW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowClusterSettingList) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowColumns) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowColumns) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowColumns) StatementTag() string { return "SHOW COLUMNS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowColumns) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowCreate) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowCreate) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreate) StatementTag() string { return "SHOW CREATE" }

// StatTargetType implements the StatTargetType interface.
func (*ShowCreate) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowCreateDatabase) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowCreateDatabase) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateDatabase) StatementTag() string { return "SHOW CREATE DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*ShowCreateDatabase) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowBackup) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowBackup) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowBackup) StatementTag() string { return "SHOW BACKUP" }

// StatTargetType implements the StatTargetType interface.
func (*ShowBackup) StatTargetType() string { return "" }

func (*ShowBackup) cclOnlyStatement() {}

// StatementType implements the Statement interface.
func (*ShowDatabases) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowDatabases) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabases) StatementTag() string { return "SHOW DATABASES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowDatabases) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowFunction) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowFunction) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *ShowFunction) StatementTag() string {
	if n.ShowAllFunc {
		return "SHOW FUNCTIONS"
	}
	return "SHOW FUNCTION"
}

// StatTargetType implements the StatTargetType interface.
func (*ShowFunction) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSchedule) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSchedule) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *ShowSchedule) StatementTag() string {
	if n.ShowAllSche {
		return "SHOW FUNCTIONS"
	}
	return "SHOW FUNCTION"
}

// StatTargetType implements the StatTargetType interface.
func (*ShowSchedule) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowTraceForSession) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowTraceForSession) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTraceForSession) StatementTag() string { return "SHOW TRACE FOR SESSION" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTraceForSession) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowGrants) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowGrants) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowGrants) StatementTag() string { return "SHOW GRANTS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowGrants) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowDatabaseIndexes) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowDatabaseIndexes) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabaseIndexes) StatementTag() string { return "SHOW INDEXES FROM DATABASE" }

// StatTargetType implements the StatTargetType interface.
func (*ShowDatabaseIndexes) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowIndexes) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowIndexes) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowIndexes) StatementTag() string { return "SHOW INDEXES FROM TABLE" }

// StatTargetType implements the StatTargetType interface.
func (*ShowIndexes) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowPartitions) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowPartitions) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of the statement.
func (*ShowPartitions) StatementTag() string { return "SHOW PARTITIONS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowPartitions) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowQueries) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowQueries) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowQueries) StatementTag() string { return "SHOW QUERIES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowQueries) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowJobs) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowJobs) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowJobs) StatementTag() string { return "SHOW JOBS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowJobs) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowRoleGrants) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowRoleGrants) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoleGrants) StatementTag() string { return "SHOW GRANTS ON ROLE" }

// StatTargetType implements the StatTargetType interface.
func (*ShowRoleGrants) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSessions) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSessions) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSessions) StatementTag() string { return "SHOW SESSIONS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSessions) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowTableStats) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowTableStats) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTableStats) StatementTag() string { return "SHOW STATISTICS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTableStats) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowHistogram) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowHistogram) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowHistogram) StatementTag() string { return "SHOW HISTOGRAM" }

// StatTargetType implements the StatTargetType interface.
func (*ShowHistogram) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSortHistogram) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSortHistogram) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSortHistogram) StatementTag() string { return "SHOW SORT HISTOGRAM" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSortHistogram) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSyntax) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSyntax) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSyntax) StatementTag() string { return "SHOW SYNTAX" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSyntax) StatTargetType() string { return "" }

func (*ShowSyntax) observerStatement() {}

// StatementType implements the Statement interface.
func (*ShowTransactionStatus) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowTransactionStatus) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransactionStatus) StatementTag() string { return "SHOW TRANSACTION STATUS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTransactionStatus) StatTargetType() string { return "" }

func (*ShowTransactionStatus) observerStatement() {}

// StatementType implements the Statement interface.
func (*ShowSavepointStatus) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSavepointStatus) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSavepointStatus) StatementTag() string { return "SHOW SAVEPOINT STATUS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSavepointStatus) StatTargetType() string { return "" }

func (*ShowSavepointStatus) observerStatement() {}

// StatementType implements the Statement interface.
func (*ShowUsers) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowUsers) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowUsers) StatementTag() string { return "SHOW USERS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowUsers) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
//func (*ShowSysConfig) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
//func (*ShowSysConfig) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
//func (*ShowSysConfig) StatementTag() string { return "SHOW SYSTEM CONFIGS" }

// StatTargetType implements the StatTargetType interface.
//func (*ShowSysConfig) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
//func (*ShowSysConfigs) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
//func (*ShowSysConfigs) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
//func (*ShowSysConfigs) StatementTag() string { return "SHOW SYSTEM CONFIG" }

// StatTargetType implements the StatTargetType interface.
//func (*ShowSysConfigs) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowRoles) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowRoles) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoles) StatementTag() string { return "SHOW ROLES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowRoles) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowZoneConfig) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowZoneConfig) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowZoneConfig) StatementTag() string { return "SHOW ZONE CONFIGURATION" }

// StatTargetType implements the StatTargetType interface.
func (*ShowZoneConfig) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowRanges) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowRanges) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRanges) StatementTag() string { return "SHOW RANGES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowRanges) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowRangeForRow) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowRangeForRow) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRangeForRow) StatementTag() string { return "SHOW RANGE FOR ROW" }

// StatTargetType implements the StatTargetType interface.
func (*ShowRangeForRow) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowFingerprints) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowFingerprints) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFingerprints) StatementTag() string { return "SHOW EXPERIMENTAL_FINGERPRINTS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowFingerprints) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowConstraints) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowConstraints) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowConstraints) StatementTag() string { return "SHOW CONSTRAINTS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowConstraints) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowTables) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTables) StatementTag() string { return "SHOW TABLES" }

// StatTargetType implements the StatTargetType interface.
func (*ShowTables) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSchemas) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSchemas) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSchemas) StatementTag() string { return "SHOW SCHEMAS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSchemas) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowSequences) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowSequences) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSequences) StatementTag() string { return "SHOW SCHEMAS" }

// StatTargetType implements the StatTargetType interface.
func (*ShowSequences) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ShowAudits) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ShowAudits) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowAudits) StatementTag() string { return "SHOW AUDITS" }

// StatTargetType implements the Statement interface.
func (*ShowAudits) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Split) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Split) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*Split) StatementTag() string { return "SPLIT" }

// StatTargetType implements the StatTargetType interface.
func (*Split) StatTargetType() string { return "TABLE" }

// StatementType implements the Statement interface.
func (*Unsplit) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*Unsplit) StatOp() string { return "ALTER" }

// StatementTag returns a short string identifying the type of statement.
func (*Unsplit) StatementTag() string { return "UNSPLIT" }

// StatTargetType implements the StatTargetType interface.
func (*Unsplit) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*SelectInto) StatementType() StatementType { return RowsAffected }

// StatOp implements the StatOp interface.
func (*SelectInto) StatOp() string { return "SELECT" }

// StatementTag returns a short string identifying the type of statement.
func (*SelectInto) StatementTag() string { return "SET" }

// StatTargetType implements the StatTargetType interface.
func (*SelectInto) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*Truncate) StatementType() StatementType { return Ack }

// StatOp implements the StatOp interface.
func (*Truncate) StatOp() string { return "TRUNCATE" }

// StatementTag returns a short string identifying the type of statement.
func (*Truncate) StatementTag() string { return "TRUNCATE" }

// modifiesSchema implements the canModifySchema interface.
func (*Truncate) modifiesSchema() bool { return true }

// StatTargetType implements the StatTargetType interface.
func (*Truncate) StatTargetType() string { return "TABLE" }

// StatementType implements the Statement interface.
func (n *Update) StatementType() StatementType { return n.Returning.statementType() }

// StatOp implements the StatOp interface.
func (n *Update) StatOp() string { return "UPDATE" }

// StatementTag returns a short string identifying the type of statement.
func (*Update) StatementTag() string { return "UPDATE" }

// StatTargetType implements the StatTargetType interface.
func (*Update) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*UnionClause) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*UnionClause) StatOp() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*UnionClause) StatementTag() string { return "UNION" }

// StatTargetType implements the StatTargetType interface.
func (*UnionClause) StatTargetType() string { return "" }

// StatementType implements the Statement interface.
func (*ValuesClause) StatementType() StatementType { return Rows }

// StatOp implements the StatOp interface.
func (*ValuesClause) StatOp() string { return "" }

// StatTargetType implements the StatTargetType interface.
func (*ValuesClause) StatTargetType() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ValuesClause) StatementTag() string { return "VALUES" }

func (n *AlterTSDatabase) String() string                { return AsString(n) }
func (n *AlterIndex) String() string                     { return AsString(n) }
func (n *AlterTable) String() string                     { return AsString(n) }
func (n *AlterTableCmds) String() string                 { return AsString(n) }
func (n *AlterTableAddColumn) String() string            { return AsString(n) }
func (n *AlterTableAddConstraint) String() string        { return AsString(n) }
func (n *AlterTableAlterColumnType) String() string      { return AsString(n) }
func (n *AlterTableAlterTagType) String() string         { return AsString(n) }
func (n *AlterTableDropColumn) String() string           { return AsString(n) }
func (n *AlterTableDropConstraint) String() string       { return AsString(n) }
func (n *AlterTableDropNotNull) String() string          { return AsString(n) }
func (n *AlterTableDropStored) String() string           { return AsString(n) }
func (n *AlterTableSetDefault) String() string           { return AsString(n) }
func (n *AlterTableSetNotNull) String() string           { return AsString(n) }
func (n *AlterRole) String() string                      { return AsString(n) }
func (n *AlterSequence) String() string                  { return AsString(n) }
func (n *AlterAudit) String() string                     { return AsString(n) }
func (n *Backup) String() string                         { return AsString(n) }
func (n *BeginTransaction) String() string               { return AsString(n) }
func (c *ControlJobs) String() string                    { return AsString(c) }
func (n *CancelQueries) String() string                  { return AsString(n) }
func (n *CancelSessions) String() string                 { return AsString(n) }
func (n *CannedOptPlan) String() string                  { return AsString(n) }
func (n *CommentOnColumn) String() string                { return AsString(n) }
func (n *CommentOnDatabase) String() string              { return AsString(n) }
func (n *CommentOnIndex) String() string                 { return AsString(n) }
func (n *CommentOnTable) String() string                 { return AsString(n) }
func (n *CommitTransaction) String() string              { return AsString(n) }
func (n *Compress) String() string                       { return AsString(n) }
func (n *CopyFrom) String() string                       { return AsString(n) }
func (n *CreateAudit) String() string                    { return AsString(n) }
func (n *CreateChangefeed) String() string               { return AsString(n) }
func (n *CreateDatabase) String() string                 { return AsString(n) }
func (n *CreateIndex) String() string                    { return AsString(n) }
func (n *CreateRole) String() string                     { return AsString(n) }
func (n *CreateTable) String() string                    { return AsString(n) }
func (n *CreateSchema) String() string                   { return AsString(n) }
func (n *CreateSequence) String() string                 { return AsString(n) }
func (n *CreateFunction) String() string                 { return AsString(n) }
func (n *CreateStats) String() string                    { return AsString(n) }
func (n *CreateView) String() string                     { return AsString(n) }
func (n *Deallocate) String() string                     { return AsString(n) }
func (n *Delete) String() string                         { return AsString(n) }
func (n *DropDatabase) String() string                   { return AsString(n) }
func (n *DropIndex) String() string                      { return AsString(n) }
func (n *DropSchedule) String() string                   { return AsString(n) }
func (n *DropSchema) String() string                     { return AsString(n) }
func (n *DropTable) String() string                      { return AsString(n) }
func (n *DropView) String() string                       { return AsString(n) }
func (n *DropSequence) String() string                   { return AsString(n) }
func (n *DropRole) String() string                       { return AsString(n) }
func (n *CreateSchedule) String() string                 { return AsString(n) }
func (n *PauseSchedule) String() string                  { return AsString(n) }
func (n *ResumeSchedule) String() string                 { return AsString(n) }
func (n *AlterSchedule) String() string                  { return AsString(n) }
func (n *DropFunction) String() string                   { return AsString(n) }
func (n *DropAudit) String() string                      { return AsString(n) }
func (n *Execute) String() string                        { return AsString(n) }
func (n *Explain) String() string                        { return AsString(n) }
func (n *ExplainAnalyzeDebug) String() string            { return AsString(n) }
func (n *Export) String() string                         { return AsString(n) }
func (n *Grant) String() string                          { return AsString(n) }
func (n *GrantRole) String() string                      { return AsString(n) }
func (n *Insert) String() string                         { return AsString(n) }
func (n *Import) String() string                         { return AsString(n) }
func (n *ImportPortal) String() string                   { return AsString(n) }
func (n *ParenSelect) String() string                    { return AsString(n) }
func (n *Prepare) String() string                        { return AsString(n) }
func (n *RebalanceTsData) String() string                { return AsString(n) }
func (n *RefreshMaterializedView) String() string        { return AsString(n) }
func (n *ReleaseSavepoint) String() string               { return AsString(n) }
func (n *Relocate) String() string                       { return AsString(n) }
func (n *RenameColumn) String() string                   { return AsString(n) }
func (n *RenameDatabase) String() string                 { return AsString(n) }
func (n *RenameIndex) String() string                    { return AsString(n) }
func (n *RenameTable) String() string                    { return AsString(n) }
func (n *Restore) String() string                        { return AsString(n) }
func (n *Revoke) String() string                         { return AsString(n) }
func (n *RevokeRole) String() string                     { return AsString(n) }
func (n *RollbackToSavepoint) String() string            { return AsString(n) }
func (n *RollbackTransaction) String() string            { return AsString(n) }
func (n *Savepoint) String() string                      { return AsString(n) }
func (n *Scatter) String() string                        { return AsString(n) }
func (n *Scrub) String() string                          { return AsString(n) }
func (n *Select) String() string                         { return AsString(n) }
func (n *SelectClause) String() string                   { return AsString(n) }
func (n *SetClusterSetting) String() string              { return AsString(n) }
func (n *SetZoneConfig) String() string                  { return AsString(n) }
func (n *SetSessionAuthorizationDefault) String() string { return AsString(n) }
func (n *SetSessionCharacteristics) String() string      { return AsString(n) }
func (n *SetTransaction) String() string                 { return AsString(n) }
func (n *SetTracing) String() string                     { return AsString(n) }
func (n *ReplicationControl) String() string             { return AsString(n) }
func (n *ReplicateSetSecondary) String() string          { return AsString(n) }
func (n *ReplicateSetRole) String() string               { return AsString(n) }
func (n *SetVar) String() string                         { return AsString(n) }
func (n *ShowApplications) String() string               { return AsString(n) }
func (n *ShowTags) String() string                       { return AsString(n) }
func (n *ShowTagValues) String() string                  { return AsString(n) }
func (n *ShowBackup) String() string                     { return AsString(n) }
func (n *ShowClusterSetting) String() string             { return AsString(n) }
func (n *ShowClusterSettingList) String() string         { return AsString(n) }
func (n *ShowColumns) String() string                    { return AsString(n) }
func (n *ShowConstraints) String() string                { return AsString(n) }
func (n *ShowCreate) String() string                     { return AsString(n) }
func (n *ShowCreateDatabase) String() string             { return AsString(n) }
func (n *ShowDatabases) String() string                  { return AsString(n) }
func (n *ShowFunction) String() string                   { return AsString(n) }
func (n *ShowSchedule) String() string                   { return AsString(n) }
func (n *ShowDatabaseIndexes) String() string            { return AsString(n) }
func (n *ShowGrants) String() string                     { return AsString(n) }
func (n *ShowHistogram) String() string                  { return AsString(n) }
func (n *ShowSortHistogram) String() string              { return AsString(n) }
func (n *ShowIndexes) String() string                    { return AsString(n) }
func (n *ShowPartitions) String() string                 { return AsString(n) }
func (n *ShowJobs) String() string                       { return AsString(n) }
func (n *ShowQueries) String() string                    { return AsString(n) }
func (n *ShowRanges) String() string                     { return AsString(n) }
func (n *ShowRangeForRow) String() string                { return AsString(n) }
func (n *ShowRoleGrants) String() string                 { return AsString(n) }
func (n *ShowRoles) String() string                      { return AsString(n) }
func (n *ShowSavepointStatus) String() string            { return AsString(n) }
func (n *ShowSchemas) String() string                    { return AsString(n) }
func (n *ShowSequences) String() string                  { return AsString(n) }
func (n *ShowSessions) String() string                   { return AsString(n) }
func (n *ShowSyntax) String() string                     { return AsString(n) }
func (n *ShowTableStats) String() string                 { return AsString(n) }
func (n *ShowTables) String() string                     { return AsString(n) }
func (n *ShowRetentions) String() string                 { return AsString(n) }
func (n *ShowTraceForSession) String() string            { return AsString(n) }
func (n *ShowTransactionStatus) String() string          { return AsString(n) }
func (n *ShowUsers) String() string                      { return AsString(n) }
func (n *ShowVar) String() string                        { return AsString(n) }
func (n *ShowUdvVar) String() string                     { return AsString(n) }
func (n *ShowZoneConfig) String() string                 { return AsString(n) }
func (n *ShowFingerprints) String() string               { return AsString(n) }
func (n *ShowAudits) String() string                     { return AsString(n) }
func (n *Split) String() string                          { return AsString(n) }
func (n *Unsplit) String() string                        { return AsString(n) }
func (n *SelectInto) String() string                     { return AsString(n) }
func (n *Truncate) String() string                       { return AsString(n) }
func (n *UnionClause) String() string                    { return AsString(n) }
func (n *Update) String() string                         { return AsString(n) }
func (n *ValuesClause) String() string                   { return AsString(n) }
