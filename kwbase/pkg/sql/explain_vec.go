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
	"reflect"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colflow"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// explainVecNode is a planNode that wraps a plan and returns
// information related to running that plan with the vectorized engine.
type explainVecNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planNode

	stmtType tree.StatementType

	run struct {
		lines []string
		// The current row returned by the node.
		values tree.Datums
	}
	subqueryPlans []subquery
}

type flowWithNode struct {
	nodeID roachpb.NodeID
	flow   *execinfrapb.FlowSpec
}

func (n *explainVecNode) startExec(params runParams) error {
	n.run.values = make(tree.Datums, 1)
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	willDistributePlan, _ := willDistributePlan(distSQLPlanner, n.plan, params)
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := makeExplainVecPlanningCtx(distSQLPlanner, params, n.stmtType, n.subqueryPlans, willDistributePlan)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	plan, err := makePhysicalPlan(planCtx, distSQLPlanner, n.plan)
	if err != nil {
		if len(n.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, &plan)
	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	flowCtx := makeFlowCtx(planCtx, plan, params)
	flowCtx.Cfg.ClusterID = &distSQLPlanner.rpcCtx.ClusterID

	// We want to get the vectorized plan which would be executed with the
	// current 'vectorize' option. If 'vectorize' is set to 'off', then the
	// vectorized engine is disabled, and we will return an error in such case.
	// With all other options, we don't change the setting to the
	// most-inclusive option as we used to because the plan can be different
	// based on 'vectorize' setting.
	if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.VectorizeOff {
		return errors.New("vectorize is set to 'off'")
	}

	sortedFlows := make([]flowWithNode, 0, len(flows))
	for nodeID, flow := range flows {
		sortedFlows = append(sortedFlows, flowWithNode{nodeID: nodeID, flow: flow})
	}
	// Sort backward, since the first thing you add to a treeprinter will come last.
	sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].nodeID < sortedFlows[j].nodeID })
	tp := treeprinter.NewWithIndent(false /* leftPad */, true /* rightPad */, 0 /* edgeLength */)
	root := tp.Child("")
	verbose := n.options.Flags[tree.ExplainFlagVerbose]
	thisNodeID := distSQLPlanner.nodeDesc.NodeID
	for _, flow := range sortedFlows {
		node := root.Childf("Node %d", flow.nodeID)
		fuseOpt := flowinfra.FuseNormally
		if flow.nodeID == thisNodeID && !willDistributePlan {
			fuseOpt = flowinfra.FuseAggressively
		}
		opChains, err := colflow.SupportsVectorized(params.ctx, flowCtx, flow.flow.Processors, flow.flow.TsProcessors, fuseOpt, nil /* output */)
		if err != nil {
			return err
		}
		// It is possible that when iterating over execinfra.OpNodes we will hit
		// a panic (an input that doesn't implement OpNode interface), so we're
		// catching such errors.
		if err := execerror.CatchVectorizedRuntimeError(func() {
			for _, op := range opChains {
				formatOpChain(op, node, verbose)
			}
		}); err != nil {
			return err
		}
	}
	n.run.lines = tp.FormattedRows()
	return nil
}

func makeFlowCtx(planCtx *PlanningCtx, plan PhysicalPlan, params runParams) *execinfra.FlowCtx {
	flowCtx := &execinfra.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Cfg: &execinfra.ServerConfig{
			Settings:       params.p.execCfg.Settings,
			DiskMonitor:    &mon.BytesMonitor{},
			VecFDSemaphore: params.p.execCfg.DistSQLSrv.VecFDSemaphore,
		},
	}
	return flowCtx
}

func makeExplainVecPlanningCtx(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	stmtType tree.StatementType,
	subqueryPlans []subquery,
	willDistributePlan bool,
) *PlanningCtx {
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn)
	planCtx.isLocal = !willDistributePlan
	planCtx.ignoreClose = true
	planCtx.planner = params.p
	planCtx.stmtType = stmtType
	planCtx.planner.curPlan.subqueryPlans = subqueryPlans
	for i := range planCtx.planner.curPlan.subqueryPlans {
		p := &planCtx.planner.curPlan.subqueryPlans[i]
		// Fake subquery results - they're not important for our explain output.
		p.started = true
		p.result = tree.DNull
	}
	return planCtx
}

func shouldOutput(operator execinfra.OpNode, verbose bool) bool {
	_, nonExplainable := operator.(colexec.NonExplainable)
	return !nonExplainable || verbose
}

func formatOpChain(operator execinfra.OpNode, node treeprinter.Node, verbose bool) {
	seenOps := make(map[reflect.Value]struct{})
	if shouldOutput(operator, verbose) {
		doFormatOpChain(operator, node.Child(reflect.TypeOf(operator).String()), verbose, seenOps)
	} else {
		doFormatOpChain(operator, node, verbose, seenOps)
	}
}
func doFormatOpChain(
	operator execinfra.OpNode,
	node treeprinter.Node,
	verbose bool,
	seenOps map[reflect.Value]struct{},
) {
	for i := 0; i < operator.ChildCount(verbose); i++ {
		child := operator.Child(i, verbose)
		childOpValue := reflect.ValueOf(child)
		childOpName := reflect.TypeOf(child).String()
		if _, seenOp := seenOps[childOpValue]; seenOp {
			// We have already seen this operator, so in order to not repeat the full
			// chain again, we will simply print out this operator's name and will
			// not recurse into its children. Note that we print out the name
			// unequivocally.
			node.Child(childOpName)
			continue
		}
		seenOps[childOpValue] = struct{}{}
		if shouldOutput(child, verbose) {
			doFormatOpChain(child, node.Child(childOpName), verbose, seenOps)
		} else {
			doFormatOpChain(child, node, verbose, seenOps)
		}
	}
}

func (n *explainVecNode) Next(runParams) (bool, error) {
	if len(n.run.lines) == 0 {
		return false, nil
	}
	n.run.values[0] = tree.NewDString(n.run.lines[0])
	n.run.lines = n.run.lines[1:]
	return true, nil
}

func (n *explainVecNode) Values() tree.Datums { return n.run.values }
func (n *explainVecNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
