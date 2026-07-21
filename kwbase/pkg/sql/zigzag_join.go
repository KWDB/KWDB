// Copyright 2018 The Cockroach Authors.
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

// zigzagJoinNode represents a zigzag join. A zigzag join uses multiple indexes
// at the same time that are all prefixed with fixed columns (columns
// constrained to constant values) followed by values that must remain
// equal across all sides (such as primary keys). The zigzag joiner takes
// advantage of the sorted order of those equality columns to efficiently
// find rows that satisfy the constant constraints as well as the equality
// constraints.
//
// For a more detailed description of zigzag joins, as well as when they can
// be planned, see the comment in rowexec/zigzagjoiner.go.
type zigzagJoinNode struct {
	// sides contains information about each individual "side" of a
	// zigzag join. Must contain 2 or more zigzagJoinSides.
	sides []zigzagJoinSide

	// columns are the produced columns, namely the columns in all
	// indexes in 'sides' - in the same order as sides.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	reqOrdering ReqOrdering
}

// zigzagJoinSide contains information about one "side" of the zigzag
// join. Note that the length of all eqCols in one zigzagJoinNode should
// be the same.
type zigzagJoinSide struct {
	// scan references a scan node containing index/table descriptor references
	// for this side of the join.
	scan *scanNode

	// eqCols is an int slice containing the equated columns for this side
	// of the zigzag join.
	eqCols []int

	// fixedVals contains fixed values for a prefix of this side's index columns.
	// Represented as a values node with one row/tuple, and just the columns
	// that are fixed.
	fixedVals *valuesNode
}

func (zj *zigzagJoinNode) StartExec(params RunParams) error {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Next is part of the PlanNode interface.
func (zj *zigzagJoinNode) Next(params RunParams) (bool, error) {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Values is part of the PlanNode interface.
func (zj *zigzagJoinNode) Values() tree.Datums {
	panic("zigzag joins cannot be executed outside of distsql")
}

// Close is part of the PlanNode interface.
func (zj *zigzagJoinNode) Close(ctx context.Context) {
}
