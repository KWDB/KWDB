// Copyright 2017 The Cockroach Authors.
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

package sqlbase

import (
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

// DefaultSearchPath is the search path used by virgin sessions.
var DefaultSearchPath = sessiondata.MakeSearchPath([]string{"public"})

// AdminRole is the default (and non-droppable) role with superuser privileges.
var AdminRole = "admin"

// PublicRole is the special "public" pseudo-role.
// All users are implicit members of "public". The role cannot be created,
// dropped, assigned to another role, and is generally not listed.
// It can be granted privileges, implicitly granting them to all users (current and future).
var PublicRole = "public"

const (
	// TimestampCol is the series timestamp
	TimestampCol = "k_timestamp"

	// ValidCol stands for VALID column name in TS.
	ValidCol = "valid"

	// FirstAgg is the function name of first agg function.
	FirstAgg = "first"

	// FirstTSAgg is the function name of firstts agg function.
	FirstTSAgg = "firstts"

	// FirstRowAgg is the function name of first row agg function.
	FirstRowAgg = "first_row"

	// FirstRowTSAgg is the function name of first row ts agg function.
	FirstRowTSAgg = "first_row_ts"

	// LastAgg is the function name of last agg function.
	LastAgg = "last"

	// LastRowAgg is the function name of last row agg function.
	LastRowAgg = "last_row"

	// LastTSAgg is the function name of lastts agg function.
	LastTSAgg = "lastts"

	// LastRowTSAgg is the function name of last row ts agg function.
	LastRowTSAgg = "last_row_ts"

	// MinAgg is the function name of min agg function.
	MinAgg = "min"

	// MaxAgg is the function name of max agg function.
	MaxAgg = "max"
)

// ReportableAppNamePrefix indicates that the application name can be
// reported in telemetry without scrubbing. (Note this only applies to
// the application name itself. Query data is still scrubbed as
// usual.)
const ReportableAppNamePrefix = "$ "

// InternalAppNamePrefix indicates that the application name identifies
// an internal task / query / job to CockroachDB. Different application
// names are used to classify queries in different categories.
const InternalAppNamePrefix = ReportableAppNamePrefix + "internal"

// DelegatedAppNamePrefix is added to a regular client application
// name for SQL queries that are ran internally on behalf of other SQL
// queries inside that application. This is not the same as
// RepotableAppNamePrefix; in particular the application name with
// DelegatedAppNamePrefix should be scrubbed in reporting.
const DelegatedAppNamePrefix = "$$ "

// TsTableReaderProcName name of stats span in time series
const TsTableReaderProcName = "relation timeseries table reader"

// UserDefinedFunctionType is user defined function type
type UserDefinedFunctionType int32

// User define function type
const (
	DefinedFunction    UserDefinedFunctionType = 1
	DefinedAggregation UserDefinedFunctionType = 2
)

// RoutineType is user defined routine type: procedure or function
type RoutineType int

// User Defined Routine type
const (
	Procedure RoutineType = iota + 1
	Function
)

// Oid for virtual database and table.
const (
	CrdbInternalID = math.MaxUint32 - iota
	CrdbInternalBackwardDependenciesTableID
	CrdbInternalBuildInfoTableID
	CrdbInternalBuiltinFunctionsTableID
	CrdbInternalClusterQueriesTableID
	CrdbInternalClusterTransactionsTableID
	CrdbInternalClusterSessionsTableID
	CrdbInternalClusterSettingsTableID
	CrdbInternalCreateStmtsTableID
	CrdbInternalFeatureUsageID
	CrdbInternalForwardDependenciesTableID
	CrdbInternalGossipNodesTableID
	CrdbInternalGossipAlertsTableID
	CrdbInternalGossipLivenessTableID
	CrdbInternalGossipNetworkTableID
	CrdbInternalIndexColumnsTableID
	CrdbInternalJobsTableID
	CrdbInternalKVNodeStatusTableID
	CrdbInternalKVStoreStatusTableID
	CrdbInternalLeasesTableID
	CrdbInternalLocalQueriesTableID
	CrdbInternalLocalTransactionsTableID
	CrdbInternalLocalSessionsTableID
	CrdbInternalLocalMetricsTableID
	CrdbInternalPartitionsTableID
	CrdbInternalPredefinedCommentsTableID
	CrdbInternalRangesNoLeasesTableID
	CrdbInternalRangesViewID
	CrdbInternalRuntimeInfoTableID
	CrdbInternalSchemaChangesTableID
	CrdbInternalSessionTraceTableID
	CrdbInternalSessionVariablesTableID
	CrdbInternalStmtStatsTableID
	CrdbInternalTableColumnsTableID
	CrdbInternalTableIndexesTableID
	CrdbInternalTablesTableID
	CrdbInternalTxnStatsTableID
	CrdbInternalZonesTableID
	CrdbInternalKWDBMemoryTableID
	CrdbInternalAuditPoliciesTableID
	CrdbInternalKWDBAttributeValueTableID
	CrdbInternalKWDBObjectCreateStatementID
	CrdbInternalKWDBObjectRetentionID
	CrdbInternalKWDBStreamsTableID
	CrdbInternalTSEInfoID
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
	InformationSchemaCheckConstraints
	InformationSchemaColumnPrivilegesID
	InformationSchemaColumnsTableID
	InformationSchemaConstraintColumnUsageTableID
	InformationSchemaEnabledRolesID
	InformationSchemaKeyColumnUsageTableID
	InformationSchemaParametersTableID
	InformationSchemaReferentialConstraintsTableID
	InformationSchemaRoleTableGrantsID
	InformationSchemaRoutineTableID
	InformationSchemaSchemataTableID
	InformationSchemaSchemataTablePrivilegesID
	InformationSchemaSequencesID
	InformationSchemaStatisticsTableID
	InformationSchemaTableConstraintTableID
	InformationSchemaTablePrivilegesID
	InformationSchemaTablesTableID
	InformationSchemaViewsTableID
	InformationSchemaUserPrivilegesID
	InformationSchemaProcedurePrivilegesID
	PgCatalogID
	PgCatalogAmTableID
	PgCatalogAttrDefTableID
	PgCatalogAttributeTableID
	PgCatalogAuthIDTableID
	PgCatalogAuthMembersTableID
	PgCatalogAvailableExtensionsTableID
	PgCatalogCastTableID
	PgCatalogClassTableID
	PgCatalogCollationTableID
	PgCatalogConstraintTableID
	PgCatalogConversionTableID
	PgCatalogDatabaseTableID
	PgCatalogDefaultACLTableID
	PgCatalogDependTableID
	PgCatalogDescriptionTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogEnumTableID
	PgCatalogExtensionTableID
	PgCatalogForeignDataWrapperTableID
	PgCatalogForeignServerTableID
	PgCatalogForeignTableTableID
	PgCatalogIndexTableID
	PgCatalogIndexesTableID
	PgCatalogInheritsTableID
	PgCatalogLanguageTableID
	PgCatalogLocksTableID
	PgCatalogMatViewsTableID
	PgCatalogNamespaceTableID
	PgCatalogOperatorTableID
	PgCatalogPreparedStatementsTableID
	PgCatalogPreparedXactsTableID
	PgCatalogProcTableID
	PgCatalogRangeTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogSecLabelsTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
	PgCatalogShdependTableID
	PgCatalogUserTableID
	PgCatalogUserMappingTableID
	PgCatalogTablesTableID
	PgCatalogTablespaceTableID
	PgCatalogTriggerTableID
	PgCatalogTypeTableID
	PgCatalogViewsTableID
	PgCatalogStatActivityTableID
	PgCatalogSecurityLabelTableID
	PgCatalogSharedSecurityLabelTableID
	PgCatalogStatAllIndexesTableID
	CrdbInternalKWDBSchedulesTableID
	CrdbInternalKWDBFunctionsTableID
	KwdbInternalKWDBProceduresTableID
	KwdbInternalKWDBTriggersTableID
	MinVirtualID = KwdbInternalKWDBTriggersTableID
)

const (
	// FirstTsDataColSize is first timestamp column in the data column
	// needs to reserve 16 bytes for LSN
	FirstTsDataColSize = 16
)

const (
	// LowDataThreshold is tht low data volume threshold
	// After testing,
	// simple query (e.g. select * from t1 order by c1 limit 1)
	// and aggregate query (e.g. select time_bucket(k_timestamp,'2s') as bucket, count(c1),sum(c1),max(c1),first(c1) from t1 group by bucket limit 1)
	// have the same efficiency when parallel is enabled and when parallel is not enabled with ts query
	LowDataThreshold = 1000
	// HighDataThreshold represents high data volume threshold
	HighDataThreshold = 100000
	// MaxDopForLowData represents the degree of parallelism in low data volumes
	MaxDopForLowData = 0
	// MaxDopForHighData represents the degree of parallelism in high data volumes
	MaxDopForHighData = 32
	// DefaultDop represents the degree of parallelism without parallel requests
	DefaultDop = 0
)
