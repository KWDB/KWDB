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

package ddl

import (
	"context"
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// GenericPlanner is a type alias for sql.GenericPlanner.
type GenericPlanner = sql.GenericPlanner

// PlanNode is sql.PlanNode
type PlanNode = sql.PlanNode

// RunParams is a type alias for sql.RunParams.
type RunParams = sql.RunParams

// MutableTableDescriptor is a type alias for sqlbase.MutableTableDescriptor.
type MutableTableDescriptor = sqlbase.MutableTableDescriptor

// InternalExecutorSessionDataOverride is a type alias for sqlbase.InternalExecutorSessionDataOverride.
type InternalExecutorSessionDataOverride = sqlbase.InternalExecutorSessionDataOverride

// IndexDescriptor is a type alias for sqlbase.IndexDescriptor.
type IndexDescriptor = sqlbase.IndexDescriptor

// DatabaseDescriptor is a type alias for sqlbase.DatabaseDescriptor.
type DatabaseDescriptor = sqlbase.DatabaseDescriptor

// TableDescriptor is a type alias for sqlbase.TableDescriptor.
type TableDescriptor = sqlbase.TableDescriptor

// ImmutableTableDescriptor is a type alias for sqlbase.ImmutableTableDescriptor.
type ImmutableTableDescriptor = sqlbase.ImmutableTableDescriptor

// TSSchemaChangerTestingKnobs is a nickname for TSSchemaChangerTestingKnobs
type TSSchemaChangerTestingKnobs = sql.TSSchemaChangerTestingKnobs

// var ddlStmtTypeToNameMap = map[reflect.Type]string{
// 	reflect.TypeOf(&tree.AlterAudit{}):         "alter_audit",
// 	reflect.TypeOf(&tree.AlterIndex{}):         "alter_index",
// 	reflect.TypeOf(&tree.AlterPipe{}):          "alter_pipe",
// 	reflect.TypeOf(&tree.AlterPub{}):           "alter_publication",
// 	reflect.TypeOf(&tree.AlterRole{}):          "alter_role",
// 	reflect.TypeOf(&tree.AlterSchedule{}):      "alter_schedule",
// 	reflect.TypeOf(&tree.AlterSequence{}):      "alter_sequence",
// 	reflect.TypeOf(&tree.AlterStream{}):        "alter_stream",
// 	reflect.TypeOf(&tree.AlterSub{}):           "alter_subscription",
// 	reflect.TypeOf(&tree.AlterTSDatabase{}):    "alter_ts_database",
// 	reflect.TypeOf(&tree.AlterTable{}):         "alter_table",
// 	reflect.TypeOf(&tree.CommentOnColumn{}):    "comment_on_column",
// 	reflect.TypeOf(&tree.CommentOnDatabase{}):  "comment_on_database",
// 	reflect.TypeOf(&tree.CommentOnIndex{}):     "comment_on_index",
// 	reflect.TypeOf(&tree.CommentOnProcedure{}): "comment_on_procedure",
// 	reflect.TypeOf(&tree.CommentOnTable{}):     "comment_on_table",
// 	reflect.TypeOf(&tree.CreateAudit{}):        "create_audit",
// 	reflect.TypeOf(&tree.CreateCompartment{}):  "create_compartment",
// 	reflect.TypeOf(&tree.CreateDatabase{}):     "create_database",
// 	reflect.TypeOf(&tree.CreateFunction{}):     "create_function",
// 	reflect.TypeOf(&tree.CreateIndex{}):        "create_index",
// 	reflect.TypeOf(&tree.CreateLabel{}):        "create_label",
// 	reflect.TypeOf(&tree.CreateLevel{}):        "create_level",
// 	reflect.TypeOf(&tree.CreateLink{}):         "create_dblink",
// 	reflect.TypeOf(&tree.CreatePipe{}):         "create_pipe",
// 	reflect.TypeOf(&tree.CreatePublication{}):  "create_publication",
// 	reflect.TypeOf(&tree.CreateRole{}):         "create_role",
// 	reflect.TypeOf(&tree.CreateSchedule{}):     "create_schedule",
// 	reflect.TypeOf(&tree.CreateSchema{}):       "create_schema",
// 	reflect.TypeOf(&tree.CreateSequence{}):     "create_sequence",
// 	reflect.TypeOf(&tree.CreateStats{}):        "create_statistics",
// 	reflect.TypeOf(&tree.CreateStream{}):       "create_stream",
// 	reflect.TypeOf(&tree.CreateSubscription{}): "create_subscription",
// 	// there is NO handling for create Trigger/Procedure in opaque.go
// 	// reflect.TypeOf(&tree.CreateTrigger{}):     "create_trigger",
// 	// reflect.TypeOf(&tree.CreateProcedure{}):   "create_procedure",
// 	reflect.TypeOf(&tree.DropAudit{}):         "drop_audit",
// 	reflect.TypeOf(&tree.DropCompartment{}):   "drop_compartment",
// 	reflect.TypeOf(&tree.DropDatabase{}):      "drop_database",
// 	reflect.TypeOf(&tree.DropFunction{}):      "drop_function",
// 	reflect.TypeOf(&tree.DropIndex{}):         "drop_index",
// 	reflect.TypeOf(&tree.DropLabel{}):         "drop_label",
// 	reflect.TypeOf(&tree.DropLevel{}):         "drop_level",
// 	reflect.TypeOf(&tree.DropLink{}):          "drop_dblink",
// 	reflect.TypeOf(&tree.DropPipe{}):          "drop_pipe",
// 	reflect.TypeOf(&tree.DropProcedure{}):     "drop_procedure",
// 	reflect.TypeOf(&tree.DropPublication{}):   "drop_publication",
// 	reflect.TypeOf(&tree.DropRole{}):          "drop_role",
// 	reflect.TypeOf(&tree.DropSchedule{}):      "drop_schedule",
// 	reflect.TypeOf(&tree.DropSchema{}):        "drop_schema",
// 	reflect.TypeOf(&tree.DropSequence{}):      "drop_sequence",
// 	reflect.TypeOf(&tree.DropStream{}):        "drop_stream",
// 	reflect.TypeOf(&tree.DropSubscription{}):  "drop_subscription",
// 	reflect.TypeOf(&tree.DropTable{}):         "drop_table",
// 	reflect.TypeOf(&tree.DropTrigger{}):       "drop_trigger",
// 	reflect.TypeOf(&tree.DropView{}):          "drop_view",
// 	reflect.TypeOf(&tree.RenameColumn{}):      "rename_column",
// 	reflect.TypeOf(&tree.RenameDatabase{}):    "rename_database",
// 	reflect.TypeOf(&tree.RenameIndex{}):       "rename_index",
// 	reflect.TypeOf(&tree.RenameTable{}):       "rename_table",
// 	reflect.TypeOf(&tree.RenameTrigger{}):     "rename_trigger",
// 	reflect.TypeOf(&tree.ShowHistogram{}):     "show_histogram",
// 	reflect.TypeOf(&tree.ShowSortHistogram{}): "show_sorthistogram",
// 	reflect.TypeOf(&tree.ShowTableStats{}):    "show_table_stats",
// 	reflect.TypeOf(&tree.ShowTriggers{}):      "show_triggers",
// }

var ddlHandleToNameMap = map[reflect.Type]string{
	reflect.TypeOf(&alterTSDatabaseNode{}):    "alter ts database",
	reflect.TypeOf(&alterIndexNode{}):         "alter index",
	reflect.TypeOf(&alterPipeNode{}):          "alter pipe",
	reflect.TypeOf(&alterPubNode{}):           "alter publication",
	reflect.TypeOf(&alterSequenceNode{}):      "alter sequence",
	reflect.TypeOf(&alterStreamNode{}):        "alter stream",
	reflect.TypeOf(&alterTableNode{}):         "alter table",
	reflect.TypeOf(&alterScheduleNode{}):      "alter schedule",
	reflect.TypeOf(&alterRoleNode{}):          "alter role",
	reflect.TypeOf(&alterAuditNode{}):         "alter audit",
	reflect.TypeOf(&commentOnColumnNode{}):    "comment on column",
	reflect.TypeOf(&commentOnDatabaseNode{}):  "comment on database",
	reflect.TypeOf(&commentOnProcedureNode{}): "comment on procedure",
	reflect.TypeOf(&commentOnIndexNode{}):     "comment on index",
	reflect.TypeOf(&commentOnTableNode{}):     "comment on table",
	reflect.TypeOf(&createDatabaseNode{}):     "create database",
	reflect.TypeOf(&createFunctionNode{}):     "create function",
	reflect.TypeOf(&createIndexNode{}):        "create index",
	reflect.TypeOf(&createPipeNode{}):         "create pipe",
	reflect.TypeOf(&createPubNode{}):          "create publication",
	reflect.TypeOf(&createSequenceNode{}):     "create sequence",
	reflect.TypeOf(&createSchemaNode{}):       "create schema",
	reflect.TypeOf(&createScheduleNode{}):     "create schedule",
	reflect.TypeOf(&createStatsNode{}):        "create statistics",
	reflect.TypeOf(&createProcedureNode{}):    "create procedure",
	reflect.TypeOf(&createTriggerNode{}):      "create trigger",
	reflect.TypeOf(&createStreamNode{}):       "create stream",
	reflect.TypeOf(&CreateRoleNode{}):         "create role",
	reflect.TypeOf(&createAuditNode{}):        "create audit",
	reflect.TypeOf(&dropDatabaseNode{}):       "drop database",
	reflect.TypeOf(&dropSchemaNode{}):         "drop schema",
	reflect.TypeOf(&dropIndexNode{}):          "drop index",
	reflect.TypeOf(&dropPipeNode{}):           "drop pipe",
	reflect.TypeOf(&dropPublicationNode{}):    "drop publication",
	reflect.TypeOf(&dropSequenceNode{}):       "drop sequence",
	reflect.TypeOf(&dropStreamNode{}):         "drop stream",
	reflect.TypeOf(&renameColumnNode{}):       "rename column",
	reflect.TypeOf(&renameDatabaseNode{}):     "rename database",
	reflect.TypeOf(&renameTriggerNode{}):      "rename trigger",
	reflect.TypeOf(&renameIndexNode{}):        "rename index",
	reflect.TypeOf(&renameTableNode{}):        "rename table",
	reflect.TypeOf(&dropTableNode{}):          "drop table",
	reflect.TypeOf(&DropRoleNode{}):           "drop role",
	reflect.TypeOf(&dropViewNode{}):           "drop view",
	reflect.TypeOf(&dropFunctionNode{}):       "drop function",
	reflect.TypeOf(&dropProcedureNode{}):      "drop procedure",
	reflect.TypeOf(&dropTriggerNode{}):        "drop trigger",
	reflect.TypeOf(&dropAuditNode{}):          "drop audit",
	reflect.TypeOf(&showHistogramNode{}):      "show histogram",
	reflect.TypeOf(&showTiggersNode{}):        "show triggers",
	reflect.TypeOf(&showTableStatsNode{}):     "show table stats",
	reflect.TypeOf(&showSortHistogramNode{}):  "show sorthistogram",
}

func init() {
	// Register ShowCreateTable into the sql package to break the circular import
	// between sql and ddl. The ddl package imports sql, so sql cannot import ddl
	// directly. Instead, sql declares a function variable and ddl sets it here.
	sql.ShowCreateTable = ShowCreateTable

	// Register node constructor functions to break circular imports.
	// These allow the sql package's opt_exec_factory.go to construct ddl node types
	// without directly referencing the ddl package.
	sql.NewCreateTriggerNode = func(ct *tree.CreateTrigger) sql.PlanNode {
		return &createTriggerNode{n: ct}
	}
	sql.NewCreateProcedureNode = func(cp *tree.CreateProcedure, dbDesc *DatabaseDescriptor, scID sqlbase.ID, planDeps sql.PlanDependencies) sql.PlanNode {
		return &createProcedureNode{n: cp, dbDesc: dbDesc, scID: scID, planDeps: planDeps}
	}

	// Register all DDL statement handlers.
	// Each handler wraps the existing DDL function/method to match sql.DDLHandler signature:
	//   func(ctx context.Context, p *GenericPlanner, n tree.Statement) (sql.PlanNode, error)
	//
	// There are 3 categories of function signatures in this package:
	// 1. Standalone functions like func(ctx, n) (sql.PlanNode, error)
	// 2. Methods on *GenericPlanner (or *GenericPlanner) like func(ctx, p, n) (sql.PlanNode, error)
	// 3. Functions that take *GenericPlanner as first parameter

	// === ALTER statements ===

	// AlterTSDatabase: func(p GenericPlanner, ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterTSDatabase{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterTSDatabase(ctx, gp, n.(*tree.AlterTSDatabase))
		})

	// AlterIndex: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterIndex{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterIndex(ctx, gp, n.(*tree.AlterIndex))
		})

	// AlterStream: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterStream{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterStream(ctx, gp, n.(*tree.AlterStream))
		})

	// === COMMENT ON statements (methods in ddl package) ===

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CommentOnColumn{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			// CommentOnColumn is defined in ddl on the local GenericPlanner type
			return CommentOnColumn(ctx, gp, n.(*tree.CommentOnColumn))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CommentOnDatabase{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CommentOnDatabase(ctx, gp, n.(*tree.CommentOnDatabase))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CommentOnProcedure{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CommentOnProcedure(ctx, gp, n.(*tree.CommentOnProcedure))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CommentOnIndex{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CommentOnIndex(ctx, gp, n.(*tree.CommentOnIndex))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CommentOnTable{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CommentOnTable(ctx, gp, n.(*tree.CommentOnTable))
		})

	// === CREATE statements ===

	// CreateDatabase: standalone func(ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateDatabase{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateDatabase(ctx, gp, n.(*tree.CreateDatabase))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateFunction{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateFunction(ctx, gp, n.(*tree.CreateFunction))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateSchedule{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateSchedule(ctx, gp, n.(*tree.CreateSchedule))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateIndex{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateIndex(ctx, gp, n.(*tree.CreateIndex))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreatePipe{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreatePipe(ctx, gp, n.(*tree.CreatePipe))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreatePublication{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreatePublication(ctx, gp, n.(*tree.CreatePublication))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateSchema{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateSchema(ctx, gp, n.(*tree.CreateSchema))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateRole{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateRole(ctx, gp, n.(*tree.CreateRole))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateSequence{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateSequence(ctx, gp, n.(*tree.CreateSequence))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateStats{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateStatistics(ctx, gp, n.(*tree.CreateStats))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateStream{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateStream(ctx, gp, n.(*tree.CreateStream))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.CreateAudit{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return CreateAudit(ctx, gp, n.(*tree.CreateAudit))
		})

	// === DROP statements ===

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropDatabase{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropDatabase(ctx, gp, n.(*tree.DropDatabase))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropIndex{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropIndex(ctx, gp, n.(*tree.DropIndex))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropPipe{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropPipe(ctx, gp, n.(*tree.DropPipe))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropPublication{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropPublication(ctx, gp, n.(*tree.DropPublication))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropSchema{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropSchema(ctx, gp, n.(*tree.DropSchema))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropSchedule{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropSchedule(ctx, gp, n.(*tree.DropSchedule))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropStream{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropStream(ctx, gp, n.(*tree.DropStream))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropTable{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropTable(ctx, gp, n.(*tree.DropTable))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropView{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropView(ctx, gp, n.(*tree.DropView))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropRole{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropRole(ctx, gp, n.(*tree.DropRole))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropSequence{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropSequence(ctx, gp, n.(*tree.DropSequence))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropFunction{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropFunction(ctx, gp, n.(*tree.DropFunction))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropProcedure{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropProcedure(ctx, gp, n.(*tree.DropProcedure))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropTrigger{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropTrigger(ctx, gp, n.(*tree.DropTrigger))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.DropAudit{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return DropAudit(ctx, gp, n.(*tree.DropAudit))
		})

	// === ALTER statements (methods) ===

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterPipe{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterPipe(ctx, gp, n.(*tree.AlterPipe))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterPub{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterPublication(ctx, gp, n.(*tree.AlterPub))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterTable{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterTable(ctx, gp, n.(*tree.AlterTable))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterRole{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterRole(ctx, gp, n.(*tree.AlterRole))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterSequence{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterSequence(ctx, gp, n.(*tree.AlterSequence))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterAudit{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterAudit(ctx, gp, n.(*tree.AlterAudit))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.AlterSchedule{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return AlterSchedule(ctx, gp, n.(*tree.AlterSchedule))
		})

	// === RENAME statements ===

	// RenameColumn: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.RenameColumn{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return RenameColumn(ctx, gp, n.(*tree.RenameColumn))
		})

	// RenameDatabase: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.RenameDatabase{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return RenameDatabase(ctx, gp, n.(*tree.RenameDatabase))
		})

	// RenameTrigger: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error) (method on *GenericPlanner)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.RenameTrigger{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return RenameTrigger(ctx, gp, n.(*tree.RenameTrigger))
		})

	// RenameIndex: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error) (method on *GenericPlanner)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.RenameIndex{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return RenameIndex(ctx, gp, n.(*tree.RenameIndex))
		})

	// RenameTable: func(p *GenericPlanner, ctx, n) (sql.PlanNode, error) (method on *GenericPlanner)
	sql.RegisterDDLHandler(reflect.TypeOf(&tree.RenameTable{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return RenameTable(ctx, gp, n.(*tree.RenameTable))
		})

	// === SHOW statements ===

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.ShowTriggers{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return ShowTriggers(ctx, gp, n.(*tree.ShowTriggers))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.ShowHistogram{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return ShowHistogram(ctx, gp, n.(*tree.ShowHistogram))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.ShowSortHistogram{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return ShowSortHistogram(ctx, gp, n.(*tree.ShowSortHistogram))
		})

	sql.RegisterDDLHandler(reflect.TypeOf(&tree.ShowTableStats{}),
		func(ctx context.Context, gp *GenericPlanner, n tree.Statement) (sql.PlanNode, error) {
			return ShowTableStats(ctx, gp, n.(*tree.ShowTableStats))
		})

	// Register PlanNode names for EXPLAIN output.
	// These are registered here (in the ddl package) to avoid circular imports
	// between the sql package (walk.go) and the ddl package.
	for k, v := range ddlHandleToNameMap {
		sql.RegisterPlanNodeName(k, v)
	}

}
