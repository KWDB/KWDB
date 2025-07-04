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

import "gitee.com/kwbasedb/kwbase/pkg/sql/lex"

// ShowVar represents a SHOW statement.
type ShowVar struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	// Session var names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
		ctx.FormatNameP(&node.Name)
	})
}

// ShowUdvVar represents a SHOW UDV statement.
type ShowUdvVar struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowUdvVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	ctx.WriteString(node.Name)
}

// ShowClusterSetting represents a SHOW CLUSTER SETTING statement.
type ShowClusterSetting struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CLUSTER SETTING ")
	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
		ctx.FormatNameP(&node.Name)
	})
}

// ShowClusterSettingList represents a SHOW [ALL|PUBLIC] CLUSTER SETTINGS statement.
type ShowClusterSettingList struct {
	// All indicates whether to include non-public settings in the output.
	All bool
}

// Format implements the NodeFormatter interface.
func (node *ShowClusterSettingList) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	qual := "PUBLIC"
	if node.All {
		qual = "ALL"
	}
	ctx.WriteString(qual)
	ctx.WriteString(" CLUSTER SETTINGS")
}

// BackupDetails represents the type of details to display for a SHOW BACKUP
// statement.
type BackupDetails int

const (
	// BackupDefaultDetails identifies a bare SHOW BACKUP statement.
	BackupDefaultDetails BackupDetails = iota
	// BackupRangeDetails identifies a SHOW BACKUP RANGES statement.
	BackupRangeDetails
	// BackupFileDetails identifies a SHOW BACKUP FILES statement.
	BackupFileDetails
)

// ShowBackup represents a SHOW BACKUP statement.
type ShowBackup struct {
	Path                 Expr
	Details              BackupDetails
	ShouldIncludeSchemas bool
	Options              KVOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW BACKUP ")
	if node.Details == BackupRangeDetails {
		ctx.WriteString("RANGES ")
	} else if node.Details == BackupFileDetails {
		ctx.WriteString("FILES ")
	}
	if node.ShouldIncludeSchemas {
		ctx.WriteString("SCHEMAS ")
	}
	ctx.FormatNode(node.Path)
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table       *UnresolvedObjectName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowColumns) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW COLUMNS FROM ")
	ctx.FormatNode(node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct {
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowDatabases) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DATABASES")

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowFunction represents a SHOW FUNCTION statement.
type ShowFunction struct {
	ShowAllFunc bool
	FuncName    Name
}

// Format implements the NodeFormatter interface.
func (n *ShowFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.ShowAllFunc {
		ctx.WriteString("FUNCTIONS")
	} else {
		ctx.WriteString("FUNCTION ")
		ctx.WriteString(string(n.FuncName))
	}
}

// ScheduledJobExecutorType is a type identifying the names of
// the supported scheduled job executors.
type ScheduledJobExecutorType int

const (
	// InvalidExecutor is a placeholder for an invalid executor type.
	InvalidExecutor ScheduledJobExecutorType = iota
	// ScheduledRetentionExecutor is an executor responsible for the execution of the scheduled retention
	ScheduledRetentionExecutor
	// ScheduledCompressExecutor is an executor responsible for the execution of the scheduled compress
	ScheduledCompressExecutor
	// ScheduledExecSQLExecutor is an executor responsible for
	// the execution of the scheduled SQL.
	ScheduledExecSQLExecutor
	// ScheduledAutonomyExecutor is an executor responsible for the execution of the scheduled autonomy
	ScheduledAutonomyExecutor
	// ScheduledCountExecutor is an executor responsible for the execution of the scheduled count
	ScheduledCountExecutor
)

var scheduleExecutorInternalNames = map[ScheduledJobExecutorType]string{
	InvalidExecutor:            "unknown-executor",
	ScheduledRetentionExecutor: "scheduled-retention-executor",
	ScheduledCompressExecutor:  "scheduled-compress-executor",
	ScheduledExecSQLExecutor:   "scheduled-sql-executor",
	ScheduledAutonomyExecutor:  "scheduled-autonomy-executor",
	ScheduledCountExecutor:     "scheduled-count-executor",
}

// InternalName returns an internal executor name.
// This name can be used to filter matching schedules.
func (t ScheduledJobExecutorType) InternalName() string {
	return scheduleExecutorInternalNames[t]
}

// UserName returns a user friendly executor name.
func (t ScheduledJobExecutorType) UserName() string {
	switch t {
	case ScheduledRetentionExecutor:
		return "RETENTION"
	case ScheduledCompressExecutor:
		return "COMPRESS"
	case ScheduledExecSQLExecutor:
		return "SQL"
	case ScheduledAutonomyExecutor:
		return "AUTONOMY"
	case ScheduledCountExecutor:
		return "COUNT"
	}
	return "unsupported-executor"
}

// ScheduleState describes what kind of schedules to display
type ScheduleState int

const (
	// SpecifiedSchedules indicates that show schedules should
	// only show subset of schedules.
	SpecifiedSchedules ScheduleState = iota

	// ActiveSchedules indicates that show schedules should
	// only show those schedules that are currently active.
	ActiveSchedules

	// PausedSchedules indicates that show schedules should
	// only show those schedules that are currently paused.
	PausedSchedules
)

// Format implements the NodeFormatter interface.
func (s ScheduleState) Format(ctx *FmtCtx) {
	switch s {
	case ActiveSchedules:
		ctx.WriteString("RUNNING")
	case PausedSchedules:
		ctx.WriteString("PAUSED")
	default:
		// Nothing
	}
}

//
//// ShowSchedules represents a SHOW SCHEDULES statement.
//type ShowSchedules struct {
//	WhichSchedules ScheduleState
//	ExecutorType   ScheduledJobExecutorType
//	ScheduleName   Name
//}
//
//var _ Statement = &ShowSchedules{}
//
//// Format implements the NodeFormatter interface.
//func (n *ShowSchedules) Format(ctx *FmtCtx) {
//	if n.ScheduleName != "" {
//		ctx.Printf("SHOW SCHEDULE %s", string(n.ScheduleName))
//		return
//	}
//	ctx.Printf("SHOW")
//
//	if n.WhichSchedules != SpecifiedSchedules {
//		ctx.WriteString(" ")
//		n.WhichSchedules.Format(ctx)
//	}
//
//	ctx.Printf(" SCHEDULES")
//
//	if n.ExecutorType != InvalidExecutor {
//		ctx.Printf(" FOR %s", n.ExecutorType.UserName())
//	}
//}

// ShowSchedule represents a SHOW SCHEDULE statement.
type ShowSchedule struct {
	ShowAllSche  bool
	ScheduleName Name
}

// Format implements the NodeFormatter interface.
func (n *ShowSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.ShowAllSche {
		ctx.WriteString("SCHEDULES")
		return
	}
	ctx.WriteString("SCHEDULE ")
	ctx.WriteString(string(n.ScheduleName))
}

// PauseSchedule represents a SHOW SCHEDULE statement.
type PauseSchedule struct {
	ScheduleName Name
	IfExists     bool
}

// Format implements the NodeFormatter interface.
func (n *PauseSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString("PAUSE SCHEDULE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.WriteString(string(n.ScheduleName))
}

var _ Statement = &PauseSchedule{}

// ResumeSchedule represents a SHOW SCHEDULE statement.
type ResumeSchedule struct {
	ScheduleName Name
	IfExists     bool
}

// Format implements the NodeFormatter interface.
func (n *ResumeSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString("RESUME SCHEDULE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.WriteString(string(n.ScheduleName))
}

var _ Statement = &ResumeSchedule{}

// DropSchedule represents a SHOW SCHEDULE statement.
type DropSchedule struct {
	ScheduleName *Name
	Schedules    *Select
	IfExists     bool
}

// Format implements the NodeFormatter interface.
func (n *DropSchedule) Format(ctx *FmtCtx) {
	if n.ScheduleName != nil {
		ctx.WriteString("DROP SCHEDULE ")
		if n.IfExists {
			ctx.WriteString("IF EXISTS ")
		}
		ctx.WriteString(string(*n.ScheduleName))
	} else if n.Schedules != nil {
		ctx.WriteString("DROP SCHEDULES ")
		if n.IfExists {
			ctx.WriteString("IF EXISTS ")
		}
		n.Schedules.Format(ctx)
	}
}

var _ Statement = &DropSchedule{}

// ShowTraceType is an enum of SHOW TRACE variants.
type ShowTraceType string

// A list of the SHOW TRACE variants.
const (
	ShowTraceRaw     ShowTraceType = "TRACE"
	ShowTraceKV      ShowTraceType = "KV TRACE"
	ShowTraceReplica ShowTraceType = "EXPERIMENTAL_REPLICA TRACE"
)

// ShowTraceForSession represents a SHOW TRACE FOR SESSION statement.
type ShowTraceForSession struct {
	TraceType ShowTraceType
	Compact   bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTraceForSession) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Compact {
		ctx.WriteString("COMPACT ")
	}
	ctx.WriteString(string(node.TraceType))
	ctx.WriteString(" FOR SESSION")
}

// ShowIndexes represents a SHOW INDEX statement.
type ShowIndexes struct {
	Table       *UnresolvedObjectName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowIndexes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM ")
	ctx.FormatNode(node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowDatabaseIndexes represents a SHOW INDEXES FROM DATABASE statement.
type ShowDatabaseIndexes struct {
	Database    Name
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowDatabaseIndexes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM DATABASE ")
	ctx.FormatNode(&node.Database)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowQueries represents a SHOW QUERIES statement.
type ShowQueries struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.All {
		ctx.WriteString("ALL ")
	}
	if node.Cluster {
		ctx.WriteString("CLUSTER QUERIES")
	} else {
		ctx.WriteString("LOCAL QUERIES")
	}
}

// ShowJobs represents a SHOW JOBS statement
type ShowJobs struct {
	// If non-nil, a select statement that provides the job ids to be shown.
	Jobs *Select
	// If Automatic is true, show only automatically-generated jobs such
	// as automatic CREATE STATISTICS jobs. If Automatic is false, show
	// only non-automatically-generated jobs.
	Automatic bool

	// Whether to block and wait for completion of all running jobs to be displayed.
	Block bool
	Name  Name
}

// Format implements the NodeFormatter interface.
func (node *ShowJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Automatic {
		ctx.WriteString("AUTOMATIC ")
	}
	ctx.WriteString("JOBS")
	if node.Block {
		ctx.WriteString(" WHEN COMPLETE")
	}
	if node.Jobs != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.Jobs)
	}
}

// ShowSessions represents a SHOW SESSIONS statement
type ShowSessions struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.All {
		ctx.WriteString("ALL ")
	}
	if node.Cluster {
		ctx.WriteString("CLUSTER SESSIONS")
	} else {
		ctx.WriteString("LOCAL SESSIONS")
	}
}

// ShowApplications represents a SHOW APPLICATIONS statement
type ShowApplications struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowApplications) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW APPLICATIONS")
}

// ShowSchemas represents a SHOW SCHEMAS statement.
type ShowSchemas struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (node *ShowSchemas) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SCHEMAS")
	if node.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.Database)
	}
}

// ShowSequences represents a SHOW SEQUENCES statement.
type ShowSequences struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (node *ShowSequences) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SEQUENCES")
	if node.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.Database)
	}
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	TableNamePrefix
	WithComment bool
	IsTemplate  bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TABLES")
	if node.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.TableNamePrefix)
	}

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowProcedures represents a SHOW PROCEDURES statement.
type ShowProcedures struct {
	TableNamePrefix
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowProcedures) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW PROCEDURES")
	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowConstraints represents a SHOW CONSTRAINTS statement.
type ShowConstraints struct {
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowConstraints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CONSTRAINTS FROM ")
	ctx.FormatNode(node.Table)
}

// ShowGrants represents a SHOW GRANTS statement.
// TargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *TargetList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (node *ShowGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS")
	if node.Targets != nil {
		ctx.WriteString(" ON ")
		ctx.FormatNode(node.Targets)
	}
	if node.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&node.Grantees)
	}
}

// ShowRoleGrants represents a SHOW GRANTS ON ROLE statement.
type ShowRoleGrants struct {
	Roles    NameList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (node *ShowRoleGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS ON ROLE")
	if node.Roles != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.Roles)
	}
	if node.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&node.Grantees)
	}
}

// ShowCreate represents a SHOW CREATE statement.
type ShowCreate struct {
	Name *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreate) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")
	ctx.FormatNode(node.Name)
}

// ShowCreateDatabase represents a SHOW CREATE DATABASE statement.
type ShowCreateDatabase struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE DATABASE ")
	ctx.FormatNode(&node.Database)
}

// ShowCreateProcedure represents a SHOW CREATE PROCEDURE statement.
type ShowCreateProcedure struct {
	Name TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE PROCEDURE ")
	ctx.FormatNode(&node.Name)
}

// ShowSyntax represents a SHOW SYNTAX statement.
// This the most lightweight thing that can be done on a statement
// server-side: just report the statement that was entered without
// any processing. Meant for use for syntax checking on clients,
// when the client version might differ from the server.
type ShowSyntax struct {
	Statement string
}

// Format implements the NodeFormatter interface.
func (node *ShowSyntax) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SYNTAX ")
	ctx.WriteString(lex.EscapeSQLString(node.Statement))
}

// ShowTransactionStatus represents a SHOW TRANSACTION STATUS statement.
type ShowTransactionStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowTransactionStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRANSACTION STATUS")
}

// ShowSavepointStatus represents a SHOW SAVEPOINT STATUS statement.
type ShowSavepointStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowSavepointStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SAVEPOINT STATUS")
}

// ShowUsers represents a SHOW USERS statement.
type ShowUsers struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowUsers) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW USERS")
}

// ShowRoles represents a SHOW ROLES statement.
type ShowRoles struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowRoles) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ROLES")
}

// ShowRanges represents a SHOW RANGES statement.
type ShowRanges struct {
	TableOrIndex TableIndexName
	DatabaseName Name
}

// Format implements the NodeFormatter interface.
func (node *ShowRanges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RANGES FROM ")
	if node.DatabaseName != "" {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(&node.TableOrIndex)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableOrIndex)
	}
}

// ShowRangeForRow represents a SHOW RANGE FOR ROW statement.
type ShowRangeForRow struct {
	TableOrIndex TableIndexName
	Row          Exprs
}

// Format implements the NodeFormatter interface.
func (node *ShowRangeForRow) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RANGE FROM ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	ctx.WriteString(" FOR ROW (")
	ctx.FormatNode(&node.Row)
	ctx.WriteString(")")
}

// ShowFingerprints represents a SHOW EXPERIMENTAL_FINGERPRINTS statement.
type ShowFingerprints struct {
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowFingerprints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
	ctx.FormatNode(node.Table)
}

// ShowTableStats represents a SHOW STATISTICS FOR TABLE statement.
type ShowTableStats struct {
	Table     *UnresolvedObjectName
	UsingJSON bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTableStats) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW STATISTICS ")
	if node.UsingJSON {
		ctx.WriteString("USING JSON ")
	}
	ctx.WriteString("FOR TABLE ")
	ctx.FormatNode(node.Table)
}

// ShowHistogram represents a SHOW HISTOGRAM statement.
type ShowHistogram struct {
	HistogramID int64
}

// Format implements the NodeFormatter interface.
func (node *ShowHistogram) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW HISTOGRAM %d", node.HistogramID)
}

// ShowSortHistogram represents a SHOW SORT HISTOGRAM statement.
type ShowSortHistogram struct {
	Table       *UnresolvedObjectName
	HistogramID int64
}

// Format implements the NodeFormatter interface.
func (node *ShowSortHistogram) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW SORT HISTOGRAM %d", node.HistogramID)
}

// ShowPartitions represents a SHOW PARTITIONS statement.
type ShowPartitions struct {
	IsDB     bool
	Database Name

	IsIndex bool
	Index   TableIndexName

	IsTable bool
	Table   *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowPartitions) Format(ctx *FmtCtx) {
	if node.IsDB {
		ctx.Printf("SHOW PARTITIONS FROM DATABASE ")
		ctx.FormatNode(&node.Database)
	} else if node.IsIndex {
		ctx.Printf("SHOW PARTITIONS FROM INDEX ")
		ctx.FormatNode(&node.Index)
	} else {
		ctx.Printf("SHOW PARTITIONS FROM TABLE ")
		ctx.FormatNode(node.Table)
	}
}

// ShowAudits represents a SHOW AUDITS statement.
type ShowAudits struct {
	Target     AuditTarget
	Operations NameList
	Operators  NameList
	Enable     string
}

// Format implements the NodeFormatter interface.
func (n *ShowAudits) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW AUDITS")
	if n.Target.Type != "" {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&n.Target)
	}
	if n.Operations != nil {
		ctx.WriteString(" OF ")
		ctx.FormatNode(&n.Operations)
	}
	if n.Operators != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&n.Operators)
	}
	if n.Enable != "" {
		ctx.WriteString(" ")
		ctx.WriteString(n.Enable)
	}
}
