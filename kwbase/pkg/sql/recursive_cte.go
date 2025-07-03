// Copyright 2019 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// recursiveCTENode implements the logic for a recursive CTE:
//  1. Evaluate the initial query; emit the results and also save them in
//     a "working" table.
//  2. So long as the working table is not empty:
//     * evaluate the recursive query, substituting the current contents of
//     the working table for the recursive self-reference;
//     * emit all resulting rows, and save them as the next iteration's
//     working table.
//
// The recursive query tree is regenerated each time using a callback
// (implemented by the execbuilder).
type recursiveCTENode struct {
	initial planNode

	genIterationFn exec.RecursiveCTEIterationFn

	label string

	recursiveCTERun
}

type recursiveCTERun struct {
	// workingRows contains the rows produced by the current iteration (aka the
	// "working" table).
	workingRows *rowcontainer.RowContainer
	// nextRowIdx is the index inside workingRows of the next row to be returned
	// by the operator.
	nextRowIdx int

	initialDone bool
	done        bool
}

func (n *recursiveCTENode) startExec(params runParams) error {
	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)
	n.nextRowIdx = 0
	return nil
}

func (n *recursiveCTENode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	n.nextRowIdx++

	if !n.initialDone {
		ok, err := n.initial.Next(params)
		if err != nil {
			return false, err
		}
		if ok {
			if _, err = n.workingRows.AddRow(params.ctx, n.initial.Values()); err != nil {
				return false, err
			}
			return true, nil
		}
		n.initialDone = true
	}

	if n.done {
		return false, nil
	}

	if n.workingRows.Len() == 0 {
		// Last iteration returned no rows.
		n.done = true
		return false, nil
	}

	// There are more rows to return from the last iteration.
	if n.nextRowIdx <= n.workingRows.Len() {
		return true, nil
	}

	// Let's run another iteration.

	lastWorkingRows := n.workingRows
	defer lastWorkingRows.Close(params.ctx)

	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		plan:         n.initial,
		bufferedRows: lastWorkingRows,
		label:        n.label,
	}
	newPlan, err := n.genIterationFn(buf)
	if err != nil {
		return false, err
	}

	if err := runPlanInsidePlan(params, newPlan.(*planTop), n.workingRows); err != nil {
		return false, err
	}
	n.nextRowIdx = 1
	return n.workingRows.Len() > 0, nil
}

func (n *recursiveCTENode) Values() tree.Datums {
	return n.workingRows.At(n.nextRowIdx - 1)
}

func (n *recursiveCTENode) Close(ctx context.Context) {
	n.initial.Close(ctx)
	n.workingRows.Close(ctx)
}
