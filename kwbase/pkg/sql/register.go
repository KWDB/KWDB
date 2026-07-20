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
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

func init() {
	// Register node constructor functions.
	// These are set here because the types (createTableNode, createMultiInstTableNode,
	// createViewNode) are all defined within the sql package itself, so there are
	// no circular import issues. The function variables are declared in show_create_var.go
	// and used by opt_exec_factory.go to construct these node types.
	NewCreateTableNode = func(ct *tree.CreateTable, dbDesc *sqlbase.DatabaseDescriptor, sourcePlan PlanNode) PlanNode {
		return &createTableNode{n: ct, dbDesc: dbDesc, sourcePlan: sourcePlan}
	}
	NewCreateMultiInstTableNode = func(ns []*tree.CreateTable, dbDescs map[string]*sqlbase.DatabaseDescriptor, sourcePlan PlanNode) PlanNode {
		return &createMultiInstTableNode{ns: ns, dbDescs: dbDescs, sourcePlan: sourcePlan}
	}
	NewCreateViewNode = func(
		viewName *tree.TableName,
		ifNotExists bool,
		temporary bool,
		materialized bool,
		viewQuery string,
		dbDesc *sqlbase.DatabaseDescriptor,
		columns sqlbase.ResultColumns,
		planDeps PlanDependencies,
	) PlanNode {
		return &createViewNode{
			viewName:     viewName,
			ifNotExists:  ifNotExists,
			temporary:    temporary,
			materialized: materialized,
			viewQuery:    viewQuery,
			dbDesc:       dbDesc,
			columns:      columns,
			planDeps:     planDeps,
		}
	}

	// Register PlanNode names for EXPLAIN output.
	// These are registered here (in the sql package) because all these types
	// are defined within the sql package itself, so there are no circular import issues.

	// Nodes that define their own schema.
	RegisterPlanNodeName(reflect.TypeOf(&DelayedNode{}), "delayed")
	RegisterPlanNodeName(reflect.TypeOf(&groupNode{}), "group")
	RegisterPlanNodeName(reflect.TypeOf(&joinNode{}), "join")
	RegisterPlanNodeName(reflect.TypeOf(&ordinalityNode{}), "ordinality")
	RegisterPlanNodeName(reflect.TypeOf(&renderNode{}), "render")
	RegisterPlanNodeName(reflect.TypeOf(&scanNode{}), "scan")
	RegisterPlanNodeName(reflect.TypeOf(&tsScanNode{}), "ts scan")
	RegisterPlanNodeName(reflect.TypeOf(&synchronizerNode{}), "synchronizer")
	RegisterPlanNodeName(reflect.TypeOf(&unionNode{}), "union")
	RegisterPlanNodeName(reflect.TypeOf(&valuesNode{}), "values")
	RegisterPlanNodeName(reflect.TypeOf(&virtualTableNode{}), "virtual table")
	RegisterPlanNodeName(reflect.TypeOf(&explainPlanNode{}), "explain plan")
	RegisterPlanNodeName(reflect.TypeOf(&windowNode{}), "window")
	RegisterPlanNodeName(reflect.TypeOf(&showTraceNode{}), "show trace")
	RegisterPlanNodeName(reflect.TypeOf(&zeroNode{}), "zero")
	RegisterPlanNodeName(reflect.TypeOf(&deleteNode{}), "delete")
	RegisterPlanNodeName(reflect.TypeOf(&updateNode{}), "update")
	RegisterPlanNodeName(reflect.TypeOf(&insertNode{}), "insert")
	RegisterPlanNodeName(reflect.TypeOf(&insertFastPathNode{}), "insert fast path")
	RegisterPlanNodeName(reflect.TypeOf(&upsertNode{}), "upsert")
	RegisterPlanNodeName(reflect.TypeOf(&indexJoinNode{}), "index join")
	RegisterPlanNodeName(reflect.TypeOf(&projectSetNode{}), "project set")
	RegisterPlanNodeName(reflect.TypeOf(&applyJoinNode{}), "apply join")
	RegisterPlanNodeName(reflect.TypeOf(&lookupJoinNode{}), "lookup join")
	RegisterPlanNodeName(reflect.TypeOf(&batchLookUpJoinNode{}), "batch lookup join")
	RegisterPlanNodeName(reflect.TypeOf(&zigzagJoinNode{}), "zigzag join")
	RegisterPlanNodeName(reflect.TypeOf(&importPortalNode{}), "import portal")

	// Nodes with a fixed schema.
	RegisterPlanNodeName(reflect.TypeOf(&scrubNode{}), "scrub")
	RegisterPlanNodeName(reflect.TypeOf(&explainDistSQLNode{}), "explain distsql")
	RegisterPlanNodeName(reflect.TypeOf(&explainVecNode{}), "explain vec")
	RegisterPlanNodeName(reflect.TypeOf(&relocateNode{}), "relocate")
	RegisterPlanNodeName(reflect.TypeOf(&scatterNode{}), "scatter")
	RegisterPlanNodeName(reflect.TypeOf(&splitNode{}), "split")
	RegisterPlanNodeName(reflect.TypeOf(&unsplitNode{}), "unsplit")
	RegisterPlanNodeName(reflect.TypeOf(&unsplitAllNode{}), "unsplit all")
	RegisterPlanNodeName(reflect.TypeOf(&showTraceReplicaNode{}), "show trace replica")
	RegisterPlanNodeName(reflect.TypeOf(&sequenceSelectNode{}), "sequence select")
	RegisterPlanNodeName(reflect.TypeOf(&exportNode{}), "export")
	RegisterPlanNodeName(reflect.TypeOf(&hookFnNode{}), "hook fn")

	// Nodes that delegate to their source.
	RegisterPlanNodeName(reflect.TypeOf(&bufferNode{}), "buffer")
	RegisterPlanNodeName(reflect.TypeOf(&distinctNode{}), "distinct")
	RegisterPlanNodeName(reflect.TypeOf(&filterNode{}), "filter")
	RegisterPlanNodeName(reflect.TypeOf(&max1RowNode{}), "max 1 row")
	RegisterPlanNodeName(reflect.TypeOf(&limitNode{}), "limit")
	RegisterPlanNodeName(reflect.TypeOf(&spoolNode{}), "spool")
	RegisterPlanNodeName(reflect.TypeOf(&serializeNode{}), "serialize")
	RegisterPlanNodeName(reflect.TypeOf(&saveTableNode{}), "save table")
	RegisterPlanNodeName(reflect.TypeOf(&scanBufferNode{}), "scan buffer")
	RegisterPlanNodeName(reflect.TypeOf(&sortNode{}), "sort")
	RegisterPlanNodeName(reflect.TypeOf(&recursiveCTENode{}), "recursive cte")
	RegisterPlanNodeName(reflect.TypeOf(&rowSourceToPlanNode{}), "row source to plan")
	RegisterPlanNodeName(reflect.TypeOf(&tsInsertSelectNode{}), "ts insert select")
}
