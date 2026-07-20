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

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// bufferNode consumes its input one row at a time, stores it in the buffer,
// and passes the row through. The buffered rows can be iterated over multiple
// times.
var _ PlanNode = &bufferNode{}

type bufferNode struct {
	plan PlanNode

	// TODO(yuzefovich): the buffer should probably be backed by disk. If so, the
	// comments about TempStorage suggest that it should be used by DistSQL
	// processors, but this node is local.
	bufferedRows       *rowcontainer.RowContainer
	passThruNextRowIdx int

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

func (n *bufferNode) StartExec(params RunParams) error {
	n.bufferedRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.plan, false /* mut */)),
		0, /* rowCapacity */
	)
	return nil
}

func (n *bufferNode) Next(params RunParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	ok, err := n.plan.Next(params)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if _, err = n.bufferedRows.AddRow(params.Ctx, n.plan.Values()); err != nil {
		return false, err
	}
	n.passThruNextRowIdx++
	return true, nil
}

func (n *bufferNode) Values() tree.Datums {
	return n.bufferedRows.At(n.passThruNextRowIdx - 1)
}

func (n *bufferNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	n.bufferedRows.Close(ctx)
}

// scanBufferNode behaves like an iterator into the bufferNode it is
// referencing. The bufferNode can be iterated over multiple times
// simultaneously, however, a new scanBufferNode is needed.
var _ PlanNode = &scanBufferNode{}

type scanBufferNode struct {
	buffer *bufferNode

	nextRowIdx int

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

func (n *scanBufferNode) StartExec(RunParams) error {
	return nil
}

func (n *scanBufferNode) Next(RunParams) (bool, error) {
	n.nextRowIdx++
	return n.nextRowIdx <= n.buffer.bufferedRows.Len(), nil
}

func (n *scanBufferNode) Values() tree.Datums {
	return n.buffer.bufferedRows.At(n.nextRowIdx - 1)
}

func (n *scanBufferNode) Close(context.Context) {
}
