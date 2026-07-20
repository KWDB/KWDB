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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// planToTree uses a stack to "parse" the planObserver's sequence of calls
// into a tree which can be easily serialized as JSON or Protobuf.
//
// e.g. for the plan
//
//	join [cond: t1.a = t2.b]
//	  scan [table: t1]
//	  scan [table: t2]
//
// the observer would call
//
//	enterNode join         // push onto stack
//	enterNode scan         // push onto stack
//	attr table: t1         // add attribute
//	leaveNode              // pop scan node; add it as a child of join node
//	enterNode scan         // push onto stack
//	attr table: t2         // add attribute
//	leaveNode              // pop scan node; add it as a child of join node
//	expr cond: t1.a = t2.b // add attribute
//	leaveNode              // keep root node on stack (base case because it's the root).
//
// and planToTree would return the join node.
func planToTree(ctx context.Context, top *planTop) *roachpb.ExplainTreePlanNode {
	var nodeStack planNodeStack
	observer := planObserver{
		// We set followRowSourceToPlanNode to true, to instruct the plan observer
		// to follow the edges from rowSourceToPlanNodes (indicating that the prior
		// node was not plannable by DistSQL) to the original planNodes that were
		// replaced by DistSQL nodes. This prevents the walk from ending at these
		// special replacement nodes.
		// TODO(jordan): this is pretty hacky. We should modify DistSQL physical
		//  planning to avoid mutating its input PlanNode tree instead.
		followRowSourceToPlanNode: true,
		enterNode: func(ctx context.Context, nodeName string, plan PlanNode) (bool, error) {
			nodeStack.push(&roachpb.ExplainTreePlanNode{
				Name: nodeName,
			})
			return true, nil
		},
		expr: func(_ observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr) {
			if expr == nil {
				return
			}
			stackTop := nodeStack.peek()
			stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   fieldName,
				Value: tree.AsStringWithFlags(expr, sampledLogicalPlanFmtFlags),
			})
		},
		spans: func(nodeName, fieldName string, index *sqlbase.IndexDescriptor, spans []roachpb.Span, hardLimitSet bool) {
			// TODO(jordan): it's expensive to serialize long span
			// strings. It's unfortunate that we're still calling
			// PrettySpans, just to check to see whether the output is - or
			// not. Unfortunately it's not so clear yet how to write a
			// shorter function. Suggestions welcome.
			spanss := sqlbase.PrettySpans(index, spans, 2)
			if spanss != "" {
				if spanss == "-" {
					spanss = getAttrForSpansAll(hardLimitSet)
				} else {
					// Spans contain literal values from the query and thus
					// cannot be spelled out in the collected plan.
					spanss = fmt.Sprintf("%d span%s", len(spans), util.Pluralize(int64(len(spans))))
				}
				stackTop := nodeStack.peek()
				stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
					Key:   fieldName,
					Value: spanss,
				})
			}
		},
		attr: func(nodeName, fieldName, attr string) {
			stackTop := nodeStack.peek()
			stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   fieldName,
				Value: attr,
			})
		},
		// for multiple model processing
		addWarningMessage: func(nodeName, fieldName, attr string) {
			stackTop := nodeStack.peek()
			stackTop.Name = nodeName
			stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   fieldName,
				Value: attr,
			})
		},
		leaveNode: func(nodeName string, plan PlanNode) error {
			if nodeStack.len() == 1 {
				return nil
			}
			poppedNode := nodeStack.pop()
			newStackTop := nodeStack.peek()
			newStackTop.Children = append(newStackTop.Children, poppedNode)
			return nil
		},
	}

	if err := observePlan(
		ctx, top.plan, top.subqueryPlans, top.postqueryPlans, observer, true /* returnError */, sampledLogicalPlanFmtFlags, nil,
	); err != nil {
		panic(fmt.Sprintf("error while walking plan to save it to statement stats: %s", err.Error()))
	}
	return nodeStack.peek()
}

type planNodeStack struct {
	stack []*roachpb.ExplainTreePlanNode
}

func (ns *planNodeStack) push(node *roachpb.ExplainTreePlanNode) {
	ns.stack = append(ns.stack, node)
}

func (ns *planNodeStack) pop() *roachpb.ExplainTreePlanNode {
	if len(ns.stack) == 0 {
		return nil
	}
	stackTop := ns.stack[len(ns.stack)-1]
	ns.stack = ns.stack[0 : len(ns.stack)-1]
	return stackTop
}

func (ns *planNodeStack) peek() *roachpb.ExplainTreePlanNode {
	if len(ns.stack) == 0 {
		return nil
	}
	return ns.stack[len(ns.stack)-1]
}

func (ns *planNodeStack) len() int {
	return len(ns.stack)
}
