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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// DelayedNode wraps a PlanNode in cases where the PlanNode
// constructor must be delayed during query execution (as opposed to
// SQL prepare) for resource tracking purposes.
var _ PlanNode = &DelayedNode{}

// DelayedNode is a planNode whose construction must be delayed during query
// execution for resource tracking purposes.
type DelayedNode struct {
	name        string
	columns     sqlbase.ResultColumns
	constructor nodeConstructor
	plan        PlanNode
}

type nodeConstructor func(context.Context, *GenericPlanner) (PlanNode, error)

// Next performs one unit of work for the DelayedNode, delegating to the wrapped plan.
func (d *DelayedNode) Next(params RunParams) (bool, error) { return d.plan.Next(params) }

// Values returns the values at the current row, delegating to the wrapped plan.
func (d *DelayedNode) Values() tree.Datums { return d.plan.Values() }

// Close terminates the DelayedNode's execution and releases its resources.
func (d *DelayedNode) Close(ctx context.Context) {
	if d.plan != nil {
		d.plan.Close(ctx)
		d.plan = nil
	}
}

// StartExec constructs the wrapped PlanNode now that execution is underway.
func (d *DelayedNode) StartExec(params RunParams) error {
	if d.plan != nil {
		panic("wrapped plan should not yet exist")
	}

	plan, err := d.constructor(params.Ctx, params.p)
	if err != nil {
		return err
	}
	d.plan = plan

	// Recursively invoke StartExec on new plan. Normally, StartExec doesn't
	// recurse - calling children is handled by the PlanNode walker. The reason
	// this won't suffice here is that the child of this node doesn't exist
	// until after StartExec is invoked.
	return StartExec(params, plan)
}

// NewDelayedNode creates a new DelayedNode that wraps a plan whose construction
// is deferred until execution begins.
func NewDelayedNode(
	name string, columns sqlbase.ResultColumns, constructor nodeConstructor, plan PlanNode,
) *DelayedNode {
	return &DelayedNode{
		name:        name,
		columns:     columns,
		constructor: constructor,
		plan:        plan,
	}
}
