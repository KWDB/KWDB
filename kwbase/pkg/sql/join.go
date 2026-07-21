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

// joinNode is a PlanNode whose rows are the result of an inner or
// left/right outer join.
var _ PlanNode = &joinNode{}

type joinNode struct {
	joinType sqlbase.JoinType

	// The data sources.
	left  planDataSource
	right planDataSource

	// pred represents the join predicate.
	pred *joinPredicate

	// mergeJoinOrdering is set if the left and right sides have similar ordering
	// on the equality columns (or a subset of them). The column indices refer to
	// equality columns: a ColIdx of i refers to left column
	// pred.leftEqualityIndices[i] and right column pred.rightEqualityIndices[i].
	mergeJoinOrdering sqlbase.ColumnOrdering

	reqOrdering ReqOrdering

	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns
}

func (p *GenericPlanner) makeJoinNode(
	left planDataSource, right planDataSource, pred *joinPredicate,
) *joinNode {
	n := &joinNode{
		left:     left,
		right:    right,
		joinType: pred.joinType,
		pred:     pred,
		columns:  pred.cols,
	}
	return n
}

func (n *joinNode) StartExec(params RunParams) error {
	panic("joinNode cannot be run in local mode")
}

// Next implements the PlanNode interface.
func (n *joinNode) Next(params RunParams) (res bool, err error) {
	panic("joinNode cannot be run in local mode")
}

// Values implements the PlanNode interface.
func (n *joinNode) Values() tree.Datums {
	panic("joinNode cannot be run in local mode")
}

// Close implements the PlanNode interface.
func (n *joinNode) Close(ctx context.Context) {
	n.right.plan.Close(ctx)
	n.left.plan.Close(ctx)
}

// interleavedNodes returns the ancestor on which an interleaved join is
// defined as well as the descendants of this ancestor which participate in
// the join. One of the left/right scan nodes is the ancestor and the other
// descendant. Nils are returned if there is no interleaved relationship.
// TODO(richardwu): For sibling joins, both left and right tables are
// "descendants" while the ancestor is some common ancestor. We will need to
// probably return descendants as a slice.
//
// An interleaved join has an equality on some columns of the interleave prefix.
// The "interleaved join ancestor" is the ancestor which contains all these
// join columns in its primary key.
// TODO(richardwu): For prefix/subset joins, this ancestor will be the furthest
// ancestor down the interleaved hierarchy which contains all the columns of
// the maximal join prefix (see maximalJoinPrefix in distsql_join.go).
func (n *joinNode) interleavedNodes() (ancestor *scanNode, descendant *scanNode) {
	leftScan, leftOk := n.left.plan.(*scanNode)
	rightScan, rightOk := n.right.plan.(*scanNode)

	if !leftOk || !rightOk {
		return nil, nil
	}

	leftAncestors := leftScan.index.Interleave.Ancestors
	rightAncestors := rightScan.index.Interleave.Ancestors

	// A join between an ancestor and descendant: the descendant of the two
	// tables must have have more interleaved ancestors than the other,
	// which makes the other node the potential interleaved ancestor.
	// TODO(richardwu): The general case where we can have a join
	// on a common ancestor's primary key requires traversing both
	// ancestor slices.
	if len(leftAncestors) > len(rightAncestors) {
		ancestor = rightScan
		descendant = leftScan
	} else {
		ancestor = leftScan
		descendant = rightScan
	}

	// We check the ancestors of the potential descendant to see if any of
	// them match the potential ancestor.
	for _, descAncestor := range descendant.index.Interleave.Ancestors {
		if descAncestor.TableID == ancestor.desc.ID && descAncestor.IndexID == ancestor.index.ID {
			// If the tables are indeed interleaved, then we return
			// the potentials as confirmed ancestor-descendant.
			return ancestor, descendant
		}
	}

	// We could not establish an ancestor-descendant relationship so we
	// return nils for both.
	return nil, nil
}
