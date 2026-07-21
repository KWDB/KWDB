// Copyright 2016 The Cockroach Authors.
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

package sql

import "gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"

// planDataSource contains the data source information for data
// produced by a PlanNode.
type planDataSource struct {
	// columns gives the result columns (always anonymous source).
	columns sqlbase.ResultColumns

	// plan which can be used to retrieve the data.
	plan PlanNode
}

// CheckTsScanNode return tsScanNode
func CheckTsScanNode(plan PlanNode) bool {
	switch n := plan.(type) {
	case *tsScanNode:
		return true
	case *filterNode:
		return CheckTsScanNode(n.source.plan)
	case *renderNode:
		return CheckTsScanNode(n.source.plan)
	case *indexJoinNode:
		return CheckTsScanNode(n.input)
	case *lookupJoinNode:
		return CheckTsScanNode(n.input)
	case *applyJoinNode:
		return CheckTsScanNode(n.input.plan)
	case *joinNode:
		return CheckTsScanNode(n.left.plan) || CheckTsScanNode(n.right.plan)
	case *limitNode:
		return CheckTsScanNode(n.plan)
	case *max1RowNode:
		return CheckTsScanNode(n.plan)
	case *distinctNode:
		return CheckTsScanNode(n.plan)
	case *sortNode:
		return CheckTsScanNode(n.plan)
	case *groupNode:
		return CheckTsScanNode(n.plan)
	case *windowNode:
		return CheckTsScanNode(n.plan)
	case *unionNode:
		return CheckTsScanNode(n.left) || CheckTsScanNode(n.right)
	case *splitNode:
		return CheckTsScanNode(n.rows)
	case *unsplitNode:
		return CheckTsScanNode(n.rows)
	case *relocateNode:
		return CheckTsScanNode(n.rows)
	case *insertNode:
		return CheckTsScanNode(n.source)
	case *upsertNode:
		return CheckTsScanNode(n.source)
	case *updateNode:
		return CheckTsScanNode(n.source)
	case *serializeNode:
		return CheckTsScanNode(n.source)
	case *rowCountNode:
		return CheckTsScanNode(n.source)
	case *createTableNode:
		return CheckTsScanNode(n.sourcePlan)
	case *DelayedNode:
		return CheckTsScanNode(n.plan)
	case *explainDistSQLNode:
		return CheckTsScanNode(n.plan)
	case *explainVecNode:
		return CheckTsScanNode(n.plan)
	case *ordinalityNode:
		return CheckTsScanNode(n.source)
	case *spoolNode:
		return CheckTsScanNode(n.source)
	case *saveTableNode:
		return CheckTsScanNode(n.source)
	case *showTraceReplicaNode:
		return CheckTsScanNode(n.plan)
	case *explainPlanNode:
		return CheckTsScanNode(n.plan)
	case *cancelQueriesNode:
		return CheckTsScanNode(n.rows)
	case *cancelSessionsNode:
		return CheckTsScanNode(n.rows)
	case *controlJobsNode:
		return CheckTsScanNode(n.rows)
	case *projectSetNode:
		return CheckTsScanNode(n.source)
	case *rowSourceToPlanNode:
		return CheckTsScanNode(n.originalPlanNode)
	case *errorIfRowsNode:
		return CheckTsScanNode(n.plan)
	case *bufferNode:
		return CheckTsScanNode(n.plan)
	case *recursiveCTENode:
		return CheckTsScanNode(n.initial)
	case *exportNode:
		return CheckTsScanNode(n.source)
	case *synchronizerNode:
		return CheckTsScanNode(n.plan)
	default:
		return false
	}
}
