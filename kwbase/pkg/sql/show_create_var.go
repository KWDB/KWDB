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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	ddlopts "gitee.com/kwbasedb/kwbase/pkg/sql/ddl_opts"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// NewCreateTableNode is a function variable that is set by the ddl package during init.
// It constructs a new createTableNode.
var NewCreateTableNode func(ct *tree.CreateTable, dbDesc *sqlbase.DatabaseDescriptor, sourcePlan PlanNode) PlanNode

// NewCreateTriggerNode is a function variable that is set by the ddl package during init.
// It constructs a new createTriggerNode.
var NewCreateTriggerNode func(ct *tree.CreateTrigger) PlanNode

// NewCreateProcedureNode is a function variable that is set by the ddl package during init.
// It constructs a new createProcedureNode.
var NewCreateProcedureNode func(cp *tree.CreateProcedure, dbDesc *sqlbase.DatabaseDescriptor, scID sqlbase.ID, planDeps PlanDependencies) PlanNode

// NewCreateMultiInstTableNode is a function variable that is set by the ddl package during init.
// It constructs a new createMultiInstTableNode.
var NewCreateMultiInstTableNode func(ns []*tree.CreateTable, dbDescs map[string]*sqlbase.DatabaseDescriptor, sourcePlan PlanNode) PlanNode

// NewCreateViewNode is a function variable that is set by the ddl package during init.
// It constructs a new createViewNode.
var NewCreateViewNode func(
	viewName *tree.TableName,
	ifNotExists bool,
	temporary bool,
	materialized bool,
	viewQuery string,
	dbDesc *sqlbase.DatabaseDescriptor,
	columns sqlbase.ResultColumns,
	planDeps PlanDependencies,
) PlanNode

// ShowCreateTable is a function variable that is set by the ddl package during init.
// It returns a valid SQL representation of the CREATE TABLE statement used to
// create the given table.
//
// This uses a function variable pattern to avoid a circular import between the
// sql package and the ddl package. The ddl package imports sql, so sql cannot
// import ddl back. Instead, ddl registers its implementation here during init.
var ShowCreateTable func(
	ctx context.Context,
	p *GenericPlanner,
	tn *tree.Name,
	dbPrefix string,
	desc *sqlbase.TableDescriptor,
	lCtx *InternalLookupCtx,
	displayOptions ddlopts.ShowCreateDisplayOptions,
) (string, error)

// EventLogSetClusterSettingType defines the event log type for cluster setting changes
// It is set by the eventlog package during init to avoid a circular import.
var EventLogSetClusterSettingType string

// InsertEventRecordFunc is a function variable for inserting event log records
// EventLogSetClusterSettingDetail is the detail struct for cluster setting change events.
// It is reused from the set_cluster_setting.go to avoid circular imports.
var InsertEventRecordFunc func(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	eventType string,
	targetID, reportingID int32,
	info interface{},
) error

// CheckPasswordSettingFunc checks constraints among cluster settings about password length.
// It is set by the ddl package during init to avoid a circular import.
var CheckPasswordSettingFunc func(p *GenericPlanner, name string, value int64) error
