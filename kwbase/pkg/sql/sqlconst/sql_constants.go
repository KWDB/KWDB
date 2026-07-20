// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sqlconst

import (
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// Max length of fixed and indefinite length type.
const (
	MaxFixedLen                   = 1024
	MaxNCharLen                   = 254
	DefaultTypeWithLength         = 0
	DefaultFixedLen               = 1
	DefaultVariableLEN            = 254
	MaxPrimaryTagWidth            = 128
	DefaultPrimaryTagVarcharWidth = 64
	MaxTSDataColumns              = 4096  // the maximum number of data columns for ts table.
	MaxVariableTupleLen           = 255   // the max length of variable-length type in tuple mode
	MaxSparseTSDataColumns        = 20000 // the maximum number of data columns for ts table.
)

// MaxResolution means max MaxResolution is 24 hours.
const MaxResolution = 24 * 3600

// InvalidLifetime is used internally
const InvalidLifetime = MaxLifeTime + 1

// MaxLifeTime means max lifetime on table which is 1000 years.
const MaxLifeTime = 1000 * 365 * 24 * 3600

// DefaultPartitionInterval means default Partition Interval on table which is 10 day.
const DefaultPartitionInterval = 10 * 24 * 3600

// handles the operation this block
const (
	// OptFirstRun specifies the first run time for a scheduled job
	// states and behaviors of scheduled jobs
	OptFirstRun          = "first_run"
	OptOnExecFailure     = "on_execution_failure"
	OptOnPreviousRunning = "on_previous_running"
)

// handles the operation this block
const (
	// ScheduleRetention is the name of scheduled_table_retention
	ScheduleRetention = "scheduled_table_retention"
	// ScheduleAutonomy is the name of scheduled_table_autonomy
	ScheduleAutonomy = "scheduled_table_autonomy"
	// ScheduleVacuum is the name of scheduled_table_vacuum
	ScheduleVacuum = "scheduled_table_vacuum"
	// ScheduleMigrate is the name of scheduled_table_migrate
	ScheduleMigrate = "scheduled_table_migrate"
	// ScheduleCount is the name of scheduled_table_count
	ScheduleCount = "scheduled_table_count"

	// ScheduleRefreshLicense is the name of scheduled_refresh_license
	ScheduleRefreshLicense = "scheduled_refresh_license"
	// RefreshLicenseExecutorName is the name of refresh-license-executor
	RefreshLicenseExecutorName = "refresh-license-executor"
)

// TypeDropStreamTable is a stream operation type indicating a drop stream table.
// TypeAddStreamTable is a stream operation type indicating an add stream table.
// TypeAlterStreamTable is a stream operation type indicating an alter stream table.
// TypeOtherStreamTable is a stream operation type indicating other stream table actions.
const (
	TypeDropStreamTable = iota
	TypeAddStreamTable
	TypeAlterStreamTable
	TypeOtherStreamTable
)

// WaterMarkType defines the type of watermark (realtime or historical)
// WaterMarkTypeRealtime indicates a realtime watermark that tracks live data
// WaterMarkTypeHistorical waterMarkType is an enum of watermark types.
type WaterMarkType int32

// SplitWindowUpdateMaxRequest defines the maximum number of split window update requests
const (
	// waterMarkTypeRealtime is the type of realtime watermark.
	WaterMarkTypeRealtime WaterMarkType = 0
	// waterMarkTypeHistorical is the type of historical watermark.
	WaterMarkTypeHistorical WaterMarkType = 1
	// splitWindowUpdateMaxRequest is the maximum value of the split window in the queue.
	SplitWindowUpdateMaxRequest = 100
)

// OptEnable is the option key to enable a feature
const (
	// StreamInsertBatch defines the number of rows inserted per batch.
	StreamInsertBatch = 1000
)

// OptCheckTag controls whether normal tags are retrieved in CDC
const (
	// states and behaviors of scheduled jobs
	OptEnable        = "enable"
	OptSink          = "sink"
	OptMessageFormat = "message_format"
	OptIgnoreHistory = "ignore_history"
	OptBufferSize    = "buffer_size"
	// The parameter retrieve_tags to control whether to retrieve normal tags.
	// The default value is enabled. When the user-specified output columns and filter columns do not include normal tags,
	// tag retrieve will not be performed regardless of this parameter's value (i.e., retrieve_tags remains enabled).
	// When the output columns and filter columns contain normal tags, retrieve_tags determines whether CDC enables
	// tag retrieve.
	OptCheckTag           = "retrieve_tags"
	OptLowWatermark       = "low_watermark"
	OptLowWatermarkFormat = "2006-01-02 15:04:05"
	OptSubTimeout         = "sub_timeout"
	OptPublish            = "publish"
	DefaultSubTimeout     = 2
	DefaultPublish        = "insert"

	StatusEnable  = "Enable"
	StatusDisable = "Disable"

	OptOn  = "on"
	OptOff = "off"

	MessageFormatJSON = "json"
	CdcMaxRunInfo     = 5
	DefaultBufferSize = 1

	TypeDropCDCTable = iota
	TypeAddCDCTable
	TypeAlterCDCTable
	TypeOtherCDCTable
)

// TsStreamMaxActiveNumber indicates the max number of running stream
var TsStreamMaxActiveNumber = func() *settings.IntSetting {
	s := settings.RegisterPositiveIntSetting(
		"ts.stream.max_active_number",
		"the max number of running stream",
		10,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// UDRTableName represents system.user_defined_routine
var UDRTableName = tree.NewTableName("system", "user_defined_routine")

// UserTableName represents system.users.
var UserTableName = tree.NewTableName("system", "users")

// RoleOptionsTableName represents system.role_options.
var RoleOptionsTableName = tree.NewTableName("system", "role_options")

// RoleMembersTableName represents system.role_members.
var RoleMembersTableName = tree.NewTableName("system", "role_members")

// UserLoginTableName specifies the system table name for user login status
var UserLoginTableName = tree.NewTableName("system", "user_login_status")

// KVStringOptValidate indicates the requested validation of a TypeAsStringOpts
// option.
type KVStringOptValidate string

// KVStringOptValidate values
const (
	KVStringOptAny            KVStringOptValidate = `any`
	KVStringOptRequireNoValue KVStringOptValidate = `no-value`
	KVStringOptRequireValue   KVStringOptValidate = `value`
)

// StorageParamType indicates the required type of a storage parameter.
type StorageParamType int

// StorageParamType values
const (
	StorageParamBool StorageParamType = iota
	StorageParamInt
	StorageParamFloat
	StorageParamUnimplemented
)

// MaxTSTableNameLength represents the maximum length of timeseries table name.
const MaxTSTableNameLength = 128

// MaxTagNameLength represents the maximum length of tag name.
const MaxTagNameLength = 128

// ConnAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level connection audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const ConnAuditingClusterSettingName = "server.auth_log.sql_connections.enabled"

// AuthAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level authentication audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const AuthAuditingClusterSettingName = "server.auth_log.sql_sessions.enabled"

// ShouldCheckPublicSchema indicates whether CanCreateOnSchema should check
// CREATE privileges for the public schema.
type ShouldCheckPublicSchema bool

// handles the operation this block
const (
	CheckPublicSchema     ShouldCheckPublicSchema = true
	SkipCheckPublicSchema ShouldCheckPublicSchema = false
)

// user define functions use the default schemaID and databaseID
// for storage and parsing currently
const (
	UDFFunctionDBID     int = 0
	UDFFunctionSchemaID int = 0
)

// message kind to send to pipe about ddl
const (
	KafkaMsgKindCreateTable = "create_table" // create table
	// DropIndexConstraintBehavior specifies the behavior when dropping an index used as a constraint
	KafkaMsgKindAlterTable = "alter_table" // alter table
	KafkaMsgKindDropTable  = "drop_table"  // drop table
)

// handles the operation this block
const (
	SequenceColumnID   = 1
	SequenceColumnName = "value"
)

// DropIndexConstraintBehavior is used when dropping an index to signal whether
// it is okay to do so even if it is in use as a constraint (outbound FK or
// unique). This is a subset of what is implied by DropBehavior CASCADE, which
// implies dropping *all* dependencies. This is used e.g. when the element
// constrained is being dropped anyway.
type DropIndexConstraintBehavior bool

// CheckIdxConstraint indicates the index constraint should be checked.
// IgnoreIdxConstraint indicates the index constraint should be ignored.
const (
	CheckIdxConstraint  DropIndexConstraintBehavior = true
	IgnoreIdxConstraint DropIndexConstraintBehavior = false
)

// DbAction represents a database action type (create or drop).
type DbAction bool

// DbCreated indicates a database was created.
// DbDropped indicates a database was dropped.
const (
	DbCreated DbAction = false
	DbDropped DbAction = true
)

// FKTableState is the state of the referencing table resolveFK() is called on.
type FKTableState int

const (
	// NewTable represents a new table, where the FK constraint is specified in the
	// CREATE TABLE
	NewTable FKTableState = iota
	// EmptyTable represents an existing table that is empty
	EmptyTable
	// NonEmptyTable represents an existing non-empty table
	NonEmptyTable
)
