// Copyright 2015 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// runParams is a struct containing all parameters passed to planNode.Next() and
// startPlan.
type runParams struct {
	// context.Context for this method call.
	ctx context.Context

	// extendedEvalCtx groups fields useful for this execution.
	// Used during local execution and distsql physical planning.
	extendedEvalCtx *extendedEvalContext

	// planner associated with this execution. Only used during local
	// execution.
	p *planner
}

// EvalContext() gives convenient access to the runParam's EvalContext().
func (r *runParams) EvalContext() *tree.EvalContext {
	return &r.extendedEvalCtx.EvalContext
}

// SessionData gives convenient access to the runParam's SessionData.
func (r *runParams) SessionData() *sessiondata.SessionData {
	return r.extendedEvalCtx.SessionData
}

// ExecCfg gives convenient access to the runParam's ExecutorConfig.
func (r *runParams) ExecCfg() *ExecutorConfig {
	return r.extendedEvalCtx.ExecCfg
}

// Ann is a shortcut for the Annotations from the eval context.
func (r *runParams) Ann() *tree.Annotations {
	return r.extendedEvalCtx.EvalContext.Annotations
}

// createTimeForNewTableDescriptor consults the cluster version to determine
// whether the CommitTimestamp() needs to be observed when creating a new
// TableDescriptor. See TableDescriptor.ModificationTime.
//
// TODO(ajwerner): remove in 20.1.
func (r *runParams) creationTimeForNewTableDescriptor() (hlc.Timestamp, error) {
	// Before 19.2 we needed to observe the transaction CommitTimestamp to ensure
	// that CreateAsOfTime and ModificationTime reflected the timestamp at which the
	// creating transaction committed. Starting in 19.2 we use a zero-valued
	// CreateAsOfTime and ModificationTime when creating a table descriptor and then
	// upon reading use the MVCC timestamp to populate the values.
	var ts hlc.Timestamp
	if !r.ExecCfg().Settings.Version.IsActive(
		r.ctx, clusterversion.VersionTableDescModificationTimeFromMVCC,
	) {
		var err error
		ts, err = r.p.txn.CommitTimestamp()
		if err != nil {
			return ts, err
		}
	}
	return ts, nil
}

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - setLimitHint()                (limit_hint.go)
// - planColumns()                 (plan_columns.go)
type planNode interface {
	startExec(params runParams) error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	//
	// Available after startPlan(). It is illegal to call Next() after it returns
	// false. It is legal to call Next() even if the node implements
	// planNodeFastPath and the FastPathResults() method returns true.
	Next(params runParams) (bool, error)

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	//
	// Available after Next().
	Values() tree.Datums

	// Close terminates the planNode execution and releases its resources.
	// This method should be called if the node has been used in any way (any
	// methods on it have been called) after it was constructed. Note that this
	// doesn't imply that startExec() has been necessarily called.
	//
	// This method must not be called during execution - the planNode
	// tree must remain "live" and readable via walk() even after
	// execution completes.
	//
	// The node must not be used again after this method is called. Some nodes put
	// themselves back into memory pools on Close.
	Close(ctx context.Context)
}

// PlanNode is the exported name for planNode. Useful for CCL hooks.
type PlanNode = planNode

// planNodeFastPath is implemented by nodes that can perform all their
// work during startPlan(), possibly affecting even multiple rows. For
// example, DELETE can do this.
type planNodeFastPath interface {
	// FastPathResults returns the affected row count and true if the
	// node has no result set and has already executed when startPlan() completes.
	// Note that Next() must still be valid even if this method returns
	// true, although it may have nothing left to do.
	FastPathResults() (int, bool)
}

// planNodeReadingOwnWrites can be implemented by planNodes which do
// not use the standard SQL principle of reading at the snapshot
// established at the start of the transaction. It requests that
// the top-level (shared) `startExec` function disable stepping
// mode for the duration of the node's `startExec()` call.
//
// This done e.g. for most DDL statements that perform multiple KV
// operations on descriptors, expecting to read their own writes.
//
// Note that only `startExec()` runs with the modified stepping mode,
// not the `Next()` methods. This interface (and the idea of
// temporarily disabling stepping mode) is neither sensical nor
// applicable to planNodes whose execution is interleaved with
// that of others.
type planNodeReadingOwnWrites interface {
	// ReadingOwnWrites is a marker interface.
	ReadingOwnWrites()
}

var _ planNode = &alterTSDatabaseNode{}
var _ planNode = &alterIndexNode{}
var _ planNode = &alterSequenceNode{}
var _ planNode = &alterTableNode{}
var _ planNode = &alterScheduleNode{}
var _ planNode = &alterAuditNode{}
var _ planNode = &bufferNode{}
var _ planNode = &cancelQueriesNode{}
var _ planNode = &cancelSessionsNode{}
var _ planNode = &changePrivilegesNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createFunctionNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &createProcedureNode{}
var _ planNode = &createTableNode{}
var _ planNode = &CreateRoleNode{}
var _ planNode = &createViewNode{}
var _ planNode = &createScheduleNode{}
var _ planNode = &createAuditNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &deleteRangeNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropScheduleNode{}
var _ planNode = &dropSchemaNode{}
var _ planNode = &dropSequenceNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &DropRoleNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &dropFunctionNode{}
var _ planNode = &dropProcedureNode{}
var _ planNode = &dropAuditNode{}
var _ planNode = &errorIfRowsNode{}
var _ planNode = &explainDistSQLNode{}
var _ planNode = &explainPlanNode{}
var _ planNode = &explainVecNode{}
var _ planNode = &filterNode{}
var _ planNode = &GrantRoleNode{}
var _ planNode = &groupNode{}
var _ planNode = &hookFnNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &insertFastPathNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &max1RowNode{}
var _ planNode = &operateDataNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &projectSetNode{}
var _ planNode = &pauseScheduleNode{}
var _ planNode = &recursiveCTENode{}
var _ planNode = &refreshMaterializedViewNode{}
var _ planNode = &relocateNode{}
var _ planNode = &renameColumnNode{}
var _ planNode = &renameDatabaseNode{}
var _ planNode = &renameIndexNode{}
var _ planNode = &renameTableNode{}
var _ planNode = &resumeScheduleNode{}
var _ planNode = &renderNode{}
var _ planNode = &RevokeRoleNode{}
var _ planNode = &rowCountNode{}
var _ planNode = &scanBufferNode{}
var _ planNode = &scanNode{}
var _ planNode = &scatterNode{}
var _ planNode = &serializeNode{}
var _ planNode = &sequenceSelectNode{}
var _ planNode = &showFingerprintsNode{}
var _ planNode = &showTraceNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &selectIntoNode{}
var _ planNode = &tsDDLNode{}

// var _ planNode = &replicationControlNode{}
// var _ planNode = &replicateSetRoleNode{}
// var _ planNode = &replicateSetSecondaryNode{}
var _ planNode = &unsplitNode{}
var _ planNode = &unsplitAllNode{}
var _ planNode = &truncateNode{}
var _ planNode = &unaryNode{}
var _ planNode = &unionNode{}
var _ planNode = &updateNode{}
var _ planNode = &upsertNode{}
var _ planNode = &valuesNode{}
var _ planNode = &virtualTableNode{}
var _ planNode = &windowNode{}
var _ planNode = &zeroNode{}

var _ planNodeFastPath = &deleteRangeNode{}
var _ planNodeFastPath = &rowCountNode{}
var _ planNodeFastPath = &serializeNode{}
var _ planNodeFastPath = &setZoneConfigNode{}
var _ planNodeFastPath = &controlJobsNode{}
var _ planNodeFastPath = &dropScheduleNode{}

var _ planNodeReadingOwnWrites = &alterIndexNode{}
var _ planNodeReadingOwnWrites = &alterSequenceNode{}
var _ planNodeReadingOwnWrites = &alterTableNode{}
var _ planNodeReadingOwnWrites = &createIndexNode{}
var _ planNodeReadingOwnWrites = &createSequenceNode{}
var _ planNodeReadingOwnWrites = &createProcedureNode{}
var _ planNodeReadingOwnWrites = &createTableNode{}
var _ planNodeReadingOwnWrites = &createViewNode{}
var _ planNodeReadingOwnWrites = &changePrivilegesNode{}
var _ planNodeReadingOwnWrites = &dropSchemaNode{}
var _ planNodeReadingOwnWrites = &setZoneConfigNode{}

// planNodeRequireSpool serves as marker for nodes whose parent must
// ensure that the node is fully run to completion (and the results
// spooled) during the start phase. This is currently implemented by
// all mutation statements except for upsert.
type planNodeRequireSpool interface {
	requireSpool()
}

var _ planNodeRequireSpool = &serializeNode{}

// planNodeSpool serves as marker for nodes that can perform all their
// execution during the start phase. This is different from the "fast
// path" interface because a node that performs all its execution
// during the start phase might still have some result rows and thus
// not implement the fast path.
//
// This interface exists for the following optimization: nodes
// that require spooling but are the children of a spooled node
// do not require the introduction of an explicit spool.
type planNodeSpooled interface {
	spooled()
}

var _ planNodeSpooled = &spoolNode{}

// planTop is the struct that collects the properties
// of an entire plan.
// Note: some additional per-statement state is also stored in
// semaCtx (placeholders).
// TODO(jordan): investigate whether/how per-plan state like
// placeholder data can be concentrated in a single struct.
type planTop struct {
	// stmt is a reference to the current statement (AST and other metadata).
	stmt *Statement

	// plan is the top-level node of the logical plan.
	plan planNode

	// mem/catalog retains the memo and catalog that were used to create the
	// plan.
	mem     *memo.Memo
	catalog *optCatalog

	// deps, if non-nil, collects the table/view dependencies for this query.
	// Any planNode constructors that resolves a table name or reference in the query
	// to a descriptor must register this descriptor into planDeps.
	// This is (currently) used by CREATE VIEW.
	// TODO(knz): Remove this in favor of a better encapsulated mechanism.
	deps planDependencies

	// subqueryPlans contains all the sub-query plans.
	subqueryPlans []subquery

	//auditInfo is used to record audit information
	auditInfo *server.AuditInfo

	// postqueryPlans contains all the plans for subqueries that are to be
	// executed after the main query (for example, foreign key checks).
	postqueryPlans []postquery

	// flags is populated during planning and execution.
	flags planFlags

	// execErr retains the last execution error, if any.
	execErr error

	// avoidBuffering, when set, causes the execution to avoid buffering
	// results.
	avoidBuffering bool

	instrumentation planInstrumentation

	// If we are collecting query diagnostics, flow diagrams are saved here.
	distSQLDiagrams []execinfrapb.FlowDiagram

	planType tree.LogicPlanType
}

// physicalPlanTop is a utility wrapper around PhysicalPlan that allows for
// storing planNodes that "power" the processors in the physical plan.
type physicalPlanTop struct {
	// PhysicalPlan contains the physical plan that has not yet been finalized.
	*PhysicalPlan
	// planNodesToClose contains the planNodes that are a part of the physical
	// plan (via planNodeToRowSource wrapping). These planNodes need to be
	// closed explicitly since we don't have a planNode tree that performs the
	// closure.
	planNodesToClose []planNode
}

// postquery is a query tree that is executed after the main one. It can only
// return an error (for example, foreign key violation).
type postquery struct {
	plan planNode
}

// init resets planTop to point to a given statement; used at the start of the
// planning process.
func (p *planTop) init(stmt *Statement, appStats *appStats) {
	*p = planTop{stmt: stmt}
	p.instrumentation.init(appStats)
}

// close ensures that the plan's resources have been deallocated.
func (p *planTop) close(ctx context.Context) {
	if p.plan != nil {
		p.instrumentation.savePlanInfo(ctx, p)
		p.plan.Close(ctx)
		p.plan = nil
	}

	for i := range p.subqueryPlans {
		// Once a subquery plan has been evaluated, it already closes its
		// plan.
		if p.subqueryPlans[i].plan != nil {
			p.subqueryPlans[i].plan.Close(ctx)
			p.subqueryPlans[i].plan = nil
		}
	}

	for i := range p.postqueryPlans {
		if p.postqueryPlans[i].plan != nil {
			p.postqueryPlans[i].plan.Close(ctx)
			p.postqueryPlans[i].plan = nil
		}
	}
}

// planMaybePhysical is a utility struct representing a plan. It can currently
// use either planNode or DistSQL spec representation, but eventually will be
// replaced by the latter representation directly.
type planMaybePhysical struct {
	planNode planNode
	// physPlan (when non-nil) contains the physical plan that has not yet
	// been finalized.
	physPlan *physicalPlanTop
}

// startExec calls startExec() on each planNode using a depth-first, post-order
// traversal.  The subqueries, if any, are also started.
//
// If the planNode also implements the nodeReadingOwnWrites interface,
// the txn is temporarily reconfigured to use read-your-own-writes for
// the duration of the call to startExec. This is used e.g. by
// DDL statements.
//
// Reminder: walkPlan() ensures that subqueries and sub-plans are
// started before startExec() is called.
func startExec(params runParams, plan planNode) error {

	o := planObserver{
		enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
			switch p.(type) {
			case *explainPlanNode, *explainDistSQLNode, *explainVecNode:
				// Do not recurse: we're not starting the plan if we just show its structure with EXPLAIN.
				return false, nil
			case *showTraceNode:
				// showTrace needs to override the params struct, and does so in its startExec() method.
				return false, nil
			}
			return true, nil
		},
		leaveNode: func(_ string, n planNode) (err error) {
			if _, ok := n.(planNodeReadingOwnWrites); ok {
				prevMode := params.p.Txn().ConfigureStepping(params.ctx, kv.SteppingDisabled)
				defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()
			}
			return n.startExec(params)
		},
	}
	return walkPlan(params.ctx, plan, o)
}

func (p *planner) maybePlanHook(ctx context.Context, stmt tree.Statement) (planNode, error) {
	// TODO(dan): This iteration makes the plan dispatch no longer constant
	// time. We could fix that with a map of `reflect.Type` but including
	// reflection in such a primary codepath is unfortunate. Instead, the
	// upcoming IR work will provide unique numeric type tags, which will
	// elegantly solve this.
	for _, planHook := range planHooks {
		if fn, header, subplans, avoidBuffering, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if fn != nil {
			if avoidBuffering {
				p.curPlan.avoidBuffering = true
			}
			return &hookFnNode{f: fn, header: header, subplans: subplans}, nil
		}
	}
	for _, planHook := range wrappedPlanHooks {
		if node, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if node != nil {
			return node, err
		}
	}

	return nil, nil
}

// resetNewTxn Create a new Txn and replace the old Txn.
func (r *runParams) resetNewTxn() {
	newTxn := kv.NewTxn(r.ctx, r.extendedEvalCtx.DB, r.extendedEvalCtx.NodeID)
	*r.p.txn = *newTxn
}

// Mark transaction as operating on the system DB if the descriptor id
// is within the SystemConfig range.
func (p *planner) maybeSetSystemConfig(id sqlbase.ID) error {
	if !sqlbase.IsSystemConfigID(id) {
		return nil
	}
	// Mark transaction as operating on the system DB.
	return p.txn.SetSystemConfigTrigger()
}

// planFlags is used throughout the planning code to keep track of various
// events or decisions along the way.
type planFlags uint32

const (
	// planFlagOptCacheHit is set if a plan from the query plan cache was used (and
	// re-optimized).
	planFlagOptCacheHit = (1 << iota)

	// planFlagOptCacheMiss is set if we looked for a plan in the query plan cache but
	// did not find one.
	planFlagOptCacheMiss

	// planFlagDistributed is set if the plan is for the DistSQL engine, in
	// distributed mode.
	planFlagDistributed

	// planFlagDistSQLLocal is set if the plan is for the DistSQL engine,
	// but in local mode.
	planFlagDistSQLLocal

	// planFlagExecDone marks that execution has been completed.
	planFlagExecDone

	// planFlagImplicitTxn marks that the plan was run inside of an implicit
	// transaction.
	planFlagImplicitTxn

	// planFlagIsDDL marks that the plan contains DDL.
	planFlagIsDDL
)

func (pf planFlags) IsSet(flag planFlags) bool {
	return (pf & flag) != 0
}

func (pf *planFlags) Set(flag planFlags) {
	*pf |= flag
}

// planInstrumentation handles collection of plan information before the plan is
// closed.
type planInstrumentation struct {
	appStats          *appStats
	savedPlanForStats *roachpb.ExplainTreePlanNode

	// If savePlanString is set to true, an EXPLAIN (VERBOSE)-style plan string
	// will be saved in planString.
	savePlanString bool
	planString     string
}

func (pi *planInstrumentation) init(appStats *appStats) {
	pi.appStats = appStats
}

// savePlanInfo is called before the plan is closed.
func (pi *planInstrumentation) savePlanInfo(ctx context.Context, curPlan *planTop) {
	if !curPlan.flags.IsSet(planFlagExecDone) {
		return
	}
	if pi.appStats != nil && pi.appStats.shouldSaveLogicalPlanDescription(
		curPlan.stmt,
		curPlan.catalog.planner.SessionData().User,
		curPlan.catalog.planner.SessionData().Database,
		curPlan.flags.IsSet(planFlagDistributed),
		curPlan.flags.IsSet(planFlagImplicitTxn),
		curPlan.execErr,
	) {
		pi.savedPlanForStats = planToTree(ctx, curPlan)
	}

	if pi.savePlanString {
		pi.planString = planToString(ctx, curPlan.plan, curPlan.subqueryPlans, curPlan.postqueryPlans)
	}
}

// getHintID return the sql external hint id
func (p *planner) getHintID() int64 {
	if p.EvalContext().HintModelTiDB || p.EvalContext().HintStmtEmbed {
		return -1
	}
	return p.EvalContext().HintID
}
