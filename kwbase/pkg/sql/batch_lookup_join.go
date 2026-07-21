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

type batchLookUpJoinNode struct {
	joinType sqlbase.JoinType

	// The data sources.
	left  planDataSource
	right planDataSource

	// pred represents the join predicate.
	pred *joinPredicate

	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns
}

func (p *GenericPlanner) makeBatchLookUpJoinNode(
	left planDataSource, right planDataSource, pred *joinPredicate,
) *batchLookUpJoinNode {
	blj := &batchLookUpJoinNode{
		left:     left,
		right:    right,
		joinType: pred.joinType,
		pred:     pred,
		columns:  pred.cols,
	}
	return blj
}

func (blj *batchLookUpJoinNode) StartExec(params RunParams) error {
	panic("batchLookUpJoinNode cannot be run in local mode")
}

// Next implements the PlanNode interface.
func (blj *batchLookUpJoinNode) Next(params RunParams) (res bool, err error) {
	panic("batchLookUpJoinNode cannot be run in local mode")
}

// Values implements the PlanNode interface.
func (blj *batchLookUpJoinNode) Values() tree.Datums {
	panic("batchLookUpJoinNode cannot be run in local mode")
}

// Close implements the PlanNode interface.
func (blj *batchLookUpJoinNode) Close(ctx context.Context) {
	blj.right.plan.Close(ctx)
	blj.left.plan.Close(ctx)
}
