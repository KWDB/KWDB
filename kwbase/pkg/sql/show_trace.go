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

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// showTraceNode is a planNode that processes session trace data.
type showTraceNode struct {
	columns sqlbase.ResultColumns
	compact bool

	// If set, the trace will also include "KV trace" messages - verbose messages
	// around the interaction of SQL with KV. Some of the messages are per-row.
	kvTracingEnabled bool

	run traceRun
}

// ShowTrace shows the current stored session trace, or the trace of a given
// query.
// Privileges: None.
func (p *planner) ShowTrace(ctx context.Context, n *tree.ShowTraceForSession) (planNode, error) {
	var node planNode = p.makeShowTraceNode(n.Compact, n.TraceType == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := sqlbase.GetTraceAgeColumnIdx(n.Compact)
	node = &sortNode{
		plan: node,
		ordering: sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
	}

	if n.TraceType == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{plan: node}
	}
	return node, nil
}

// makeShowTraceNode creates a new showTraceNode.
//
// Args:
// kvTracingEnabled: If set, the trace will also include "KV trace" messages -
//
//	verbose messages around the interaction of SQL with KV. Some of the
//	messages are per-row.
func (p *planner) makeShowTraceNode(compact bool, kvTracingEnabled bool) *showTraceNode {
	n := &showTraceNode{
		kvTracingEnabled: kvTracingEnabled,
		compact:          compact,
	}
	if compact {
		// We make a copy here because n.columns can be mutated to rename columns.
		n.columns = append(n.columns, sqlbase.ShowCompactTraceColumns...)
	} else {
		n.columns = append(n.columns, sqlbase.ShowTraceColumns...)
	}
	return n
}

// traceRun contains the run-time state of showTraceNode during local execution.
type traceRun struct {
	resultRows []tree.Datums
	curRow     int
}

func (n *showTraceNode) startExec(params runParams) error {
	// Get all the data upfront and process the traces. Subsequent
	// invocations of Next() will merely return the results.
	traceRows, err := params.extendedEvalCtx.Tracing.getSessionTrace()
	if err != nil {
		return err
	}
	n.processTraceRows(params.EvalContext(), traceRows)
	return nil
}

// Next implements the planNode interface
func (n *showTraceNode) Next(params runParams) (bool, error) {
	if n.run.curRow >= len(n.run.resultRows) {
		return false, nil
	}
	n.run.curRow++
	return true, nil
}

// processTraceRows populates n.resultRows.
// This code must be careful not to overwrite traceRows,
// because this is a shared slice which will be reused
// by subsequent SHOW TRACE FOR SESSION statements.
func (n *showTraceNode) processTraceRows(evalCtx *tree.EvalContext, traceRows []traceRow) {
	// Filter trace rows based on the message (in the SHOW KV TRACE case)
	if n.kvTracingEnabled {
		res := make([]traceRow, 0, len(traceRows))
		for _, r := range traceRows {
			msg := r[traceMsgCol].(*tree.DString)
			if kvMsgRegexp.MatchString(string(*msg)) {
				res = append(res, r)
			}
		}
		traceRows = res
	}
	if len(traceRows) == 0 {
		return
	}

	// Render the final rows.
	n.run.resultRows = make([]tree.Datums, len(traceRows))
	for i, r := range traceRows {
		ts := r[traceTimestampCol].(*tree.DTimestampTZ)
		loc := r[traceLocCol]
		tag := r[traceTagCol]
		msg := r[traceMsgCol]
		spanIdx := r[traceSpanIdxCol]
		op := r[traceOpCol]
		age := r[traceAgeCol]

		if !n.compact {
			n.run.resultRows[i] = tree.Datums{ts, age, msg, tag, loc, op, spanIdx}
		} else {
			msgStr := msg.(*tree.DString)
			if locStr := string(*loc.(*tree.DString)); locStr != "" {
				msgStr = tree.NewDString(fmt.Sprintf("%s %s", locStr, string(*msgStr)))
			}
			n.run.resultRows[i] = tree.Datums{age, msgStr, tag, op}
		}
	}
}

func (n *showTraceNode) Values() tree.Datums {
	return n.run.resultRows[n.run.curRow-1]
}

func (n *showTraceNode) Close(ctx context.Context) {
	n.run.resultRows = nil
}

// kvMsgRegexp is the message filter used for SHOW KV TRACE.
var kvMsgRegexp = regexp.MustCompile(
	strings.Join([]string{
		"^fetched: ",
		"^CPut ",
		"^Put ",
		"^InitPut ",
		"^DelRange ",
		"^ClearRange ",
		"^Del ",
		"^Get ",
		"^Scan ",
		"^FKScan ",
		"^CascadeScan ",
		"^querying next range at ",
		"^output row: ",
		"^rows affected: ",
		"^execution failed after ",
		"^r.*: sending batch ",
		"^cascading ",
		"^fast path completed",
	}, "|"),
)
