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

package sql

import "gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"

var noColumns = make(sqlbase.ResultColumns, 0)

// planColumns returns the signature of rows logically computed
// by the given PlanNode.
// The signature consists of the list of columns with
// their name and type.
//
// The length of the returned slice is guaranteed to be equal to the
// length of the tuple returned by the PlanNode's Values() method
// during local execution.
//
// The returned slice is *not* mutable. To modify the result column
// set, implement a separate recursion (e.g. needed_columns.go) or use
// planMutableColumns defined below.
func planColumns(plan PlanNode) sqlbase.ResultColumns {
	return getPlanColumns(plan, false)
}

// planMutableColumns is similar to planColumns() but returns a
// ResultColumns slice that can be modified by the caller.
func planMutableColumns(plan PlanNode) sqlbase.ResultColumns {
	return getPlanColumns(plan, true)
}

// getPlanColumns implements the logic for the
// planColumns/planMutableColumns functions. The mut argument
// indicates whether the slice should be mutable (mut=true) or not.
func getPlanColumns(plan PlanNode, mut bool) sqlbase.ResultColumns {
	switch n := plan.(type) {

	// Nodes that define their own schema.
	case *DelayedNode:
		return n.columns
	case *groupNode:
		return n.columns
	case *joinNode:
		return n.columns
	case *ordinalityNode:
		return n.columns
	case *renderNode:
		return n.columns
	case *scanNode:
		return n.resultColumns
	case *tsScanNode:
		return n.resultColumns
	case *synchronizerNode:
		return n.columns
	case *unionNode:
		return n.columns
	case *valuesNode:
		return n.columns
	case *virtualTableNode:
		return n.columns
	case *explainPlanNode:
		return n.run.results.columns
	case *windowNode:
		return n.columns
	case *showTraceNode:
		return n.columns
	case *zeroNode:
		return n.columns
	case *deleteNode:
		return n.columns
	case *updateNode:
		return n.columns
	case *insertNode:
		return n.columns
	case *insertFastPathNode:
		return n.columns
	case *upsertNode:
		return n.columns
	case *indexJoinNode:
		return n.resultColumns
	case *projectSetNode:
		return n.columns
	case *applyJoinNode:
		return n.columns
	case *lookupJoinNode:
		return n.columns
	case *batchLookUpJoinNode:
		return n.columns
	case *zigzagJoinNode:
		return n.columns
	case *importPortalNode:
		return n.columns
	// Nodes with a fixed schema.
	case *scrubNode:
		return n.getColumns(mut, sqlbase.ScrubColumns)
	case *explainDistSQLNode:
		return n.getColumns(mut, sqlbase.ExplainDistSQLColumns)
	case *explainVecNode:
		return n.getColumns(mut, sqlbase.ExplainVecColumns)
	case *relocateNode:
		return n.getColumns(mut, sqlbase.AlterTableRelocateColumns)
	case *scatterNode:
		return n.getColumns(mut, sqlbase.AlterTableScatterColumns)
	case *showFingerprintsNode:
		return n.getColumns(mut, sqlbase.ShowFingerprintsColumns)
	case *splitNode:
		return n.getColumns(mut, sqlbase.AlterTableSplitColumns)
	case *unsplitNode:
		return n.getColumns(mut, sqlbase.AlterTableUnsplitColumns)
	case *unsplitAllNode:
		return n.getColumns(mut, sqlbase.AlterTableUnsplitColumns)
	case *showTraceReplicaNode:
		return n.getColumns(mut, sqlbase.ShowReplicaTraceColumns)
	case *sequenceSelectNode:
		return n.getColumns(mut, sqlbase.SequenceSelectColumns)
	case *exportNode:
		if n.isTS {
			return n.getColumns(mut, sqlbase.ExportTsColumns)
		}
		return n.getColumns(mut, sqlbase.ExportColumns)

	// The columns in the hookFnNode are returned by the hook function; we don't
	// know if they can be modified in place or not.
	case *hookFnNode:
		return n.getColumns(mut, n.header)

	// Nodes that have the same schema as their source or their
	// valueNode helper.
	case *bufferNode:
		return getPlanColumns(n.plan, mut)
	case *distinctNode:
		return getPlanColumns(n.plan, mut)
	case *filterNode:
		return getPlanColumns(n.source.plan, mut)
	case *max1RowNode:
		return getPlanColumns(n.plan, mut)
	case *limitNode:
		return getPlanColumns(n.plan, mut)
	case *spoolNode:
		return getPlanColumns(n.source, mut)
	case *serializeNode:
		return getPlanColumns(n.source, mut)
	case *saveTableNode:
		return getPlanColumns(n.source, mut)
	case *scanBufferNode:
		return getPlanColumns(n.buffer, mut)
	case *sortNode:
		return getPlanColumns(n.plan, mut)
	case *recursiveCTENode:
		return getPlanColumns(n.initial, mut)
	case *rowSourceToPlanNode:
		return n.planCols
	case *createMultiInstTableNode:
		return n.getColumns(mut, sqlbase.CreateChildTablesColumns)
	case *tsInsertSelectNode:
		return getPlanColumns(n.plan, mut)
	}

	// Every other node has no columns in their results.
	return noColumns
}

// OptColumnsSlot is a helper struct for nodes with a static signature
// (e.g. explainDistSQLNode). It allows instances to reuse a common
// (shared) ResultColumns slice as long as no read/write access is
// requested to the slice via planMutableColumns.
type OptColumnsSlot struct {
	columns sqlbase.ResultColumns
}

func (c *OptColumnsSlot) getColumns(mut bool, cols sqlbase.ResultColumns) sqlbase.ResultColumns {
	if c.columns != nil {
		return c.columns
	}
	if !mut {
		return cols
	}
	c.columns = make(sqlbase.ResultColumns, len(cols))
	copy(c.columns, cols)
	return c.columns
}
