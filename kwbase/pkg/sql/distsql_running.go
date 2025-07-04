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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/rpc/nodedialer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colflow"
	"gitee.com/kwbasedb/kwbase/pkg/sql/distsql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// To allow queries to send out flow RPCs in parallel, we use a pool of workers
// that can issue the RPCs on behalf of the running code. The pool is shared by
// multiple queries.
const numRunners = 16

const clientRejectedMsg string = "client rejected when attempting to run DistSQL plan"

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx        context.Context
	nodeDialer *nodedialer.Dialer
	flowReq    *execinfrapb.SetupFlowRequest
	nodeID     roachpb.NodeID
	resultChan chan<- runnerResult
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID roachpb.NodeID
	err    error
}

func (req runnerRequest) run() {
	res := runnerResult{nodeID: req.nodeID}

	conn, err := req.nodeDialer.Dial(req.ctx, req.nodeID, rpc.DefaultClass)
	if err != nil {
		res.err = err
	} else {
		client := execinfrapb.NewDistSQLClient(conn)
		// TODO(radu): do we want a timeout here?
		resp, err := client.SetupFlow(req.ctx, req.flowReq)
		if err != nil {
			res.err = err
		} else {
			res.err = resp.Error.ErrorDetail(req.ctx)
		}
	}
	req.resultChan <- res
}

func (dsp *DistSQLPlanner) initRunners() {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	dsp.runnerChan = make(chan runnerRequest)
	for i := 0; i < numRunners; i++ {
		dsp.stopper.RunWorker(context.TODO(), func(context.Context) {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldStop()
			for {
				select {
				case req := <-runnerChan:
					req.run()

				case <-stopChan:
					return
				}
			}
		})
	}
}

// find TsProcessors in flows
func findTSProcessors(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) bool {
	for _, v := range flows {
		if v.TsProcessors != nil || len(v.TsProcessors) > 0 {
			return true
		}
	}
	return false
}

// setupFlows sets up all the flows specified in flows using the provided state.
// It will first attempt to set up all remote flows using the dsp workers if
// available or sequentially if not, and then finally set up the gateway flow,
// whose output is the DistSQLReceiver provided. This flow is then returned to
// be run.
// shortCircuit is true when stable without child tables.
// plan is the PhysicalPlan.
func (dsp *DistSQLPlanner) setupFlows(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	leafInputState *roachpb.LeafTxnInputState,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsql.LocalState,
	vectorizeThresholdMet bool,
	plan *PhysicalPlan,
) (context.Context, flowinfra.Flow, error) {
	thisNodeID := dsp.nodeDesc.NodeID
	_, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 && !findTSProcessors(flows) {
		return nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	// trace display flow spec for query from the client.
	if evalCtx.Tracing.enabled && !evalCtx.IsInternalSQL && !evalCtx.IsDisplayed && !strings.Contains(evalCtx.SessionData.ApplicationName, "internal") {
		dsp.displayQueryFlowSpec(ctx, flows, int32(thisNodeID), plan.SQL, localState.IsLocal)
	}

	evalCtxProto := execinfrapb.MakeEvalContext(&evalCtx.EvalContext)
	setupReq := execinfrapb.SetupFlowRequest{
		LeafTxnInputState: leafInputState,
		Version:           execinfra.Version,
		EvalContext:       evalCtxProto,
		TraceKV:           evalCtx.Tracing.KVTracingEnabled(),
	}

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}
	if evalCtx.SessionData.VectorizeMode != sessiondata.VectorizeOff {
		if !vectorizeThresholdMet && (evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeAuto) {
			// Vectorization is not justified for this flow because the expected
			// amount of data is too small and the overhead of pre-allocating data
			// structures needed for the vectorized engine is expected to dominate
			// the execution time.
			setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
		} else {
			fuseOpt := flowinfra.FuseNormally
			if localState.IsLocal {
				fuseOpt = flowinfra.FuseAggressively
			}
			// Now we check to see whether or not to even try vectorizing the flow.
			// The goal here is to determine up front whether all of the flows can be
			// vectorized. If any of them can't, turn off the setting.
			// TODO(yuzefovich): this is a safe but quite inefficient way of setting
			// up vectorized flows since the flows will effectively be planned twice.
			var tmpClusterID *base.ClusterIDContainer
			if dsp.rpcCtx != nil {
				tmpClusterID = &dsp.rpcCtx.ClusterID
			} else {
				tmpClusterID = nil
			}
			for _, spec := range flows {
				if _, err := colflow.SupportsVectorized(
					ctx, &execinfra.FlowCtx{
						EvalCtx: &evalCtx.EvalContext,
						Cfg: &execinfra.ServerConfig{
							DiskMonitor:    &mon.BytesMonitor{},
							Settings:       dsp.st,
							ClusterID:      tmpClusterID,
							VecFDSemaphore: dsp.distSQLSrv.VecFDSemaphore,
						},
						NodeID: -1,
					}, spec.Processors, spec.TsProcessors, fuseOpt, recv,
				); err != nil {
					// Vectorization attempt failed with an error.
					returnVectorizationSetupError := false
					if evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeExperimentalAlways {
						returnVectorizationSetupError = true
						// If running with VectorizeExperimentalAlways, this check makes sure
						// that we can still run SET statements (mostly to set vectorize to
						// off) and the like.
						if len(spec.Processors) == 1 &&
							spec.Processors[0].Core.LocalPlanNode != nil {
							rsidx := spec.Processors[0].Core.LocalPlanNode.RowSourceIdx
							if rsidx != nil {
								lp := localState.LocalProcs[*rsidx]
								if z, ok := lp.(colflow.VectorizeAlwaysException); ok {
									if z.IsException() {
										returnVectorizationSetupError = false
									}
								}
							}
						}
					}
					log.VEventf(ctx, 1, "failed to vectorize: %s", err)
					if returnVectorizationSetupError {
						return nil, nil, err
					}
					// Vectorization is not supported for this flow, so we override the
					// setting.
					setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
					break
				}
			}
		}
	}
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := setupReq
		req.Flow = *flowSpec
		runReq := runnerRequest{
			ctx:        ctx,
			nodeDialer: dsp.nodeDialer,
			flowReq:    &req,
			nodeID:     nodeID,
			resultChan: resultChan,
		}
		defer physicalplan.ReleaseSetupFlowRequest(&req)

		// Send out a request to the workers; if no worker is available, run
		// directly.
		select {
		case dsp.runnerChan <- runReq:
		default:
			runReq.run()
		}
	}

	var firstErr error
	// Now wait for all the flows to be scheduled on remote nodes. Note that we
	// are not waiting for the flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		res := <-resultChan
		if firstErr == nil {
			firstErr = res.err
		}
		// TODO(radu): accumulate the flows that we failed to set up and move them
		// into the local flow.
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}

	// Set up the flow on this node.
	localReq := setupReq
	localReq.Flow = *flows[thisNodeID]
	defer physicalplan.ReleaseSetupFlowRequest(&localReq)
	ctx, flow, err := dsp.distSQLSrv.SetupLocalSyncFlow(ctx, evalCtx.Mon, &localReq, recv, localState, plan.AllProcessorsExecInTSEngine)
	if err != nil {
		return nil, nil, err
	}

	return ctx, flow, nil
}

// Run executes a physical plan. The plan should have been finalized using
// FinalizePlan.
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// Args:
// - txn is the transaction in which the plan will run. If nil, the different
// processors are expected to manage their own internal transactions.
// - evalCtx is the evaluation context in which the plan will run. It might be
// mutated.
// - finishedSetupFn, if non-nil, is called synchronously after all the
// processors have successfully started up.
//
// It returns a non-nil (although it can be a noop when an error is
// encountered) cleanup function that must be called in order to release the
// resources.
func (dsp *DistSQLPlanner) Run(
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) (cleanup func()) {
	ctx := planCtx.ctx
	var flow flowinfra.Flow
	flow, ctx = dsp.GetFlow(planCtx, txn, plan, recv, evalCtx, finishedSetupFn, true)
	if flow == nil {
		return func() {}
	}

	// TODO(radu): this should go through the flow scheduler.
	if err := flow.Run(ctx, func() {}); err != nil {
		recv.SetError(err)
	}

	return dsp.RunClearUp(ctx, planCtx, flow, recv)
}

// GetFlow get flow spec
func (dsp *DistSQLPlanner) GetFlow(
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
	supportVectorizedThreshold bool,
) (flowinfra.Flow, context.Context) {
	ctx := planCtx.ctx
	var (
		localState distsql.LocalState
	)

	leafInputState, err := getStateParam(planCtx, txn, plan, evalCtx, &localState)
	if err != nil {
		recv.SetError(err)
		return nil, ctx
	}

	if err := planCtx.sanityCheckAddresses(); err != nil {
		recv.SetError(err)
		return nil, ctx
	}

	flows := plan.GenerateFlowSpecs(dsp.nodeDesc.NodeID /* gateway */)
	if _, ok := flows[dsp.nodeDesc.NodeID]; !ok {
		recv.SetError(errors.Errorf("expected to find gateway flow"))
		return nil, ctx
	}

	if err := savePlanDiagram(planCtx, flows); err != nil {
		recv.SetError(err)
		return nil, ctx
	}

	printLogForPlanDiagram(planCtx, flows)

	log.VEvent(ctx, 1, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.ResultTypes
	recv.resultToStreamColMap = plan.PlanToStreamColMap

	vectorizedThresholdMet := !planCtx.hasBatchLookUpJoin && supportVectorizedThreshold &&
		plan.MaxEstimatedRowCount >= evalCtx.SessionData.VectorizeRowCountThreshold && !planCtx.useQueryShortCircuit

	if len(flows) == 1 {
		// We ended up planning everything locally, regardless of whether we
		// intended to distribute or not.
		localState.IsLocal = true
	}
	ctx, flow, err := dsp.setupFlows(ctx, evalCtx, leafInputState, flows, recv, localState, vectorizedThresholdMet, plan)
	if err != nil {
		recv.SetError(err)
		return flow, ctx
	}
	if flow != nil && planCtx.planner != nil && planCtx.planner.stmt != nil {
		flow.SetRunProcedure(planCtx.planner.stmt.AST.StatOp() == "CALL")
	}
	if plan.AllProcessorsExecInTSEngine {
		flow.SetCloses(plan.Closes)
	}

	if finishedSetupFn != nil {
		finishedSetupFn()
	}

	// Check that flows that were forced to be planned locally also have no concurrency.
	// This is important, since these flows are forced to use the RootTxn (since
	// they might have mutations), and the RootTxn does not permit concurrency.
	// For such flows, we were supposed to have fused everything.
	if txn != nil && planCtx.isLocal && flow.ConcurrentExecution() && !CheckTSEngine(&flows) {
		recv.SetError(errors.AssertionFailedf(
			"unexpected concurrency for a flow that was forced to be planned locally"))
		return flow, ctx
	}

	return flow, ctx
}

// getStateParam get state param
func getStateParam(
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	evalCtx *extendedEvalContext,
	localState *distsql.LocalState,
) (*roachpb.LeafTxnInputState, error) {
	var leafInputState *roachpb.LeafTxnInputState
	ctx := planCtx.ctx
	// NB: putting part of evalCtx in localState means it might be mutated down
	// the line.
	localState.EvalContext = &evalCtx.EvalContext
	localState.Txn = txn
	if planCtx.isLocal && plan.IsDistInTS() {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
		// If the plan is not local, we will have to set up leaf txns using the
		// txnCoordMeta.
		tis, err := txn.GetLeafTxnInputStateOrRejectClient(ctx)
		if err != nil {
			log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
			return leafInputState, err
		}
		leafInputState = &tis
	} else if planCtx.isLocal && !plan.IsDistInTS() {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
	} else if txn != nil {
		// If the plan is not local, we will have to set up leaf txns using the
		// txnCoordMeta.
		tis, err := txn.GetLeafTxnInputStateOrRejectClient(ctx)
		if err != nil {
			log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
			return leafInputState, err
		}
		leafInputState = &tis
	}

	return leafInputState, nil
}

// savePlanDiagram save plan diagram
func savePlanDiagram(planCtx *PlanningCtx, flows map[roachpb.NodeID]*execinfrapb.FlowSpec) error {
	if planCtx.saveDiagram != nil {
		// Local flows might not have the UUID field set. We need it to be set to
		// distinguish statistics for processors in subqueries vs the main query vs
		// postqueries.
		if len(flows) == 1 {
			for _, f := range flows {
				if f.FlowID == (execinfrapb.FlowID{}) {
					f.FlowID.UUID = uuid.MakeV4()
				}
			}
		}
		log.VEvent(planCtx.ctx, 1, "creating plan diagram")
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		diagram, err := execinfrapb.GeneratePlanDiagram(stmtStr, flows, planCtx.saveDiagramShowInputTypes)
		if err != nil {
			return err
		}
		planCtx.saveDiagram(diagram)
	}

	return nil
}

// printLogForPlanDiagram prints log for plan diagram
func printLogForPlanDiagram(planCtx *PlanningCtx, flows map[roachpb.NodeID]*execinfrapb.FlowSpec) {
	if logPlanDiagram {
		log.VEvent(planCtx.ctx, 1, "creating plan diagram for logging")
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		_, url, err := execinfrapb.GeneratePlanDiagramURL(stmtStr, flows, false /* showInputTypes */)
		if err != nil {
			log.Infof(planCtx.ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(planCtx.ctx, "Plan diagram URL:\n%s", url.Fragment)
		}
	}
}

// CheckTSEngine checks that whether exists ts engine flow
func CheckTSEngine(flows *map[roachpb.NodeID]*execinfrapb.FlowSpec) bool {
	for _, f := range *flows {
		if f.TsProcessors != nil {
			return true
		}
	}
	return false
}

// Start starts flow
func (dsp *DistSQLPlanner) Start(
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) (flowinfra.Flow, context.Context) {
	ctx := planCtx.ctx
	var flow flowinfra.Flow
	flow, ctx = dsp.GetFlow(planCtx, txn, plan, recv, evalCtx, finishedSetupFn, false)
	if flow == nil {
		return nil, ctx
	}

	if err := flow.StartProcessor(ctx, func() {}); err != nil {
		recv.SetError(err)
		flow.Wait()
		return nil, ctx
	}

	return flow, ctx
}

// NextRow gets next row data
func (dsp *DistSQLPlanner) NextRow(
	flow flowinfra.Flow,
) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return flow.NextRow()
}

// RunClearUp for clean up
func (dsp *DistSQLPlanner) RunClearUp(
	ctx context.Context, planCtx *PlanningCtx, flow flowinfra.Flow, recv *DistSQLReceiver,
) func() {
	dsp.RowStats = flow.GetStats()

	// TODO(yuzefovich): it feels like this closing should happen after
	// PlanAndRun. We should refactor this and get rid off ignoreClose field.
	if planCtx.planner != nil && !planCtx.ignoreClose {
		// planCtx can change before the cleanup function is executed, so we make
		// a copy of the planner and bind it to the function.
		curPlan := &planCtx.planner.curPlan
		return func() {
			// We need to close the planNode tree we translated into a DistSQL plan
			// before flow.Cleanup, which closes memory accounts that expect to be
			// emptied.
			curPlan.execErr = recv.resultWriter.Err()
			curPlan.close(ctx)
			flow.Cleanup(ctx)
		}
	}

	// ignoreClose is set to true meaning that someone else will handle the
	// closing of the current plan, so we simply clean up the flow.
	return func() {
		flow.Cleanup(ctx)
	}
}

// DistSQLReceiver is a RowReceiver that writes results to a rowResultWriter.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// DistSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type DistSQLReceiver struct {
	ctx context.Context

	// resultWriter is the interface which we send results to.
	resultWriter rowResultWriter

	stmtType tree.StatementType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []types.T

	// resultToStreamColMap maps result columns to columns in the rowexec results
	// stream.
	resultToStreamColMap []int

	// noColsRequired indicates that the caller is only interested in the
	// existence of a single row. Used by subqueries in EXISTS mode.
	noColsRequired bool

	// discardRows is set when we want to discard rows (for testing/benchmarks).
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// commErr keeps track of the error received from interacting with the
	// resultWriter. This represents a "communication error" and as such is unlike
	// query execution errors: when the DistSQLReceiver is used within a SQL
	// session, such errors mean that we have to bail on the session.
	// Query execution errors are reported to the resultWriter. For some client's
	// convenience, communication errors are also reported to the resultWriter.
	//
	// Once set, no more rows are accepted.
	commErr error

	row    tree.Datums
	status execinfra.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kvcoord.RangeDescriptorCache
	leaseCache *kvcoord.LeaseHolderCache
	tracing    *SessionTracing
	cleanup    func()

	// The transaction in which the flow producing data for this
	// receiver runs. The DistSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back LeafTxnFinalState objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *kv.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)

	// bytesRead and rowsRead track the corresponding metrics while executing the
	// statement.
	bytesRead int64
	rowsRead  int64

	expectedRowsRead int64
	progressAtomic   *uint64

	useDeepRule bool
	// Deduplication mode, 2 is reject mode, and 3 is discard mode.
	dedupRule  int64
	dedupRows  int64
	insertRows int64

	nodeErrForTsAlter map[int32]string

	// RowStats record stallTime and number of rows
	execinfra.RowStats
}

// rowResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow writes a result row.
	// Note that the caller owns the row slice and might reuse it.
	AddRow(ctx context.Context, row tree.Datums) error
	AddPGResult(ctx context.Context, res []byte) error
	// AddPGComplete adds a pg complete flag.
	AddPGComplete(cmd string, typ tree.StatementType, rowsAffected int)
	IncrementRowsAffected(n int)
	RowsAffected() int
	SetError(error)
	Err() error
}

type metadataResultWriter interface {
	AddMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata)
}

type metadataCallbackWriter struct {
	rowResultWriter
	fn func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error
}

func (w *metadataCallbackWriter) AddMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata) {
	if err := w.fn(ctx, meta); err != nil {
		w.SetError(err)
	}
}

// errOnlyResultWriter is a rowResultWriter that only supports receiving an
// error. All other functions that deal with producing results panic.
type errOnlyResultWriter struct {
	err error
}

var _ rowResultWriter = &errOnlyResultWriter{}

func (w *errOnlyResultWriter) SetError(err error) {
	w.err = err
}
func (w *errOnlyResultWriter) Err() error {
	return w.err
}

func (w *errOnlyResultWriter) AddPGResult(ctx context.Context, res []byte) error {
	panic("AddPGResult not supported by errOnlyResultWriter")
}

// AddPGComplete implements the rowResultWriter interface.
func (w *errOnlyResultWriter) AddPGComplete(_ string, _ tree.StatementType, _ int) {
	panic("AddPGComplete not supported by errOnlyResultWriter")
}

func (w *errOnlyResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	panic("AddRow not supported by errOnlyResultWriter")
}
func (w *errOnlyResultWriter) IncrementRowsAffected(n int) {
	panic("IncrementRowsAffected not supported by errOnlyResultWriter")
}

// RowsAffected returns either the number of times AddRow was called, or the
// sum of all n passed into IncrementRowsAffected.
func (w *errOnlyResultWriter) RowsAffected() int {
	panic("RowsAffected not supported by errOnlyResultWriter")
}

var _ execinfra.RowReceiver = &DistSQLReceiver{}

var receiverSyncPool = sync.Pool{
	New: func() interface{} {
		return &DistSQLReceiver{}
	},
}

// MakeDistSQLReceiver creates a DistSQLReceiver.
//
// ctx is the Context that the receiver will use throughout its
// lifetime. resultWriter is the container where the results will be
// stored. If only the row count is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func MakeDistSQLReceiver(
	ctx context.Context,
	resultWriter rowResultWriter,
	stmtType tree.StatementType,
	rangeCache *kvcoord.RangeDescriptorCache,
	leaseCache *kvcoord.LeaseHolderCache,
	txn *kv.Txn,
	updateClock func(observedTs hlc.Timestamp),
	tracing *SessionTracing,
) *DistSQLReceiver {
	consumeCtx, cleanup := tracing.TraceExecConsume(ctx)
	r := receiverSyncPool.Get().(*DistSQLReceiver)
	*r = DistSQLReceiver{
		ctx:          consumeCtx,
		cleanup:      cleanup,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
		tracing:      tracing,
	}
	return r
}

// Release releases this DistSQLReceiver back to the pool.
func (r *DistSQLReceiver) Release() {
	*r = DistSQLReceiver{}
	receiverSyncPool.Put(r)
}

// clone clones the receiver for running subqueries. Not all fields are cloned,
// only those required for running subqueries.
func (r *DistSQLReceiver) clone() *DistSQLReceiver {
	ret := receiverSyncPool.Get().(*DistSQLReceiver)
	*ret = DistSQLReceiver{
		ctx:         r.ctx,
		cleanup:     func() {},
		rangeCache:  r.rangeCache,
		leaseCache:  r.leaseCache,
		txn:         r.txn,
		updateClock: r.updateClock,
		stmtType:    tree.Rows,
		tracing:     r.tracing,
	}
	return ret
}

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
func (r *DistSQLReceiver) SetError(err error) {
	r.resultWriter.SetError(err)
}

// AddStats record stallTime and number of rows
func (r *DistSQLReceiver) AddStats(time time.Duration, isAddRows bool) {
	if isAddRows {
		r.NumRows++
	}
	r.StallTime += time
}

// GetStats get RowStats
func (r *DistSQLReceiver) GetStats() execinfra.RowStats {
	return r.RowStats
}

// GetCols return column size of time-series plan
func (r *DistSQLReceiver) GetCols() int {
	return len(r.resultToStreamColMap)
}

// PushPGResult is part of the RowReceiver interface.
func (r *DistSQLReceiver) PushPGResult(ctx context.Context, res []byte) error {
	err := r.resultWriter.AddPGResult(ctx, res)
	if err != nil {
		return err
	}
	return nil
}

// AddPGComplete is part of the RowReceiver interface.
func (r *DistSQLReceiver) AddPGComplete(cmd string, typ tree.StatementType, rowsAffected int) {
	r.resultWriter.AddPGComplete(cmd, typ, rowsAffected)
}

// Push is part of the RowReceiver interface.
func (r *DistSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if meta != nil {
		if meta.TsInsert != nil {
			if meta.TsInsert.InsertSuccess && meta.TsInsert.DedupRows > 0 &&
				(meta.TsInsert.DedupRule == int64(execinfrapb.DedupRule_TsReject) || meta.TsInsert.DedupRule == int64(execinfrapb.DedupRule_TsDiscard)) {
				r.useDeepRule = true
				r.dedupRule = meta.TsInsert.DedupRule
				r.insertRows = meta.TsInsert.InsertRows
				r.dedupRows = meta.TsInsert.DedupRows
				r.resultWriter.IncrementRowsAffected(int(meta.TsInsert.InsertRows))
				r.status = execinfra.ConsumerClosed
			} else {
				// If timing insertion, only increase the number of affected rows.
				r.resultWriter.IncrementRowsAffected(int(meta.TsInsert.NumRow))
				r.status = execinfra.ConsumerClosed
				if !meta.TsInsert.InsertSuccess {
					err := errors.Newf("insert failed, reason:%s", meta.TsInsert.InsertErr)
					r.resultWriter.SetError(err)
				}
			}
		}
		if meta.TsDelete != nil {
			r.resultWriter.IncrementRowsAffected(int(meta.TsDelete.DeleteRow))
			r.status = execinfra.ConsumerClosed
			if !meta.TsDelete.DeleteSuccess {
				err := errors.Newf("delete data from timeseries table failed, reason:%s", meta.TsDelete.DeleteErr)
				r.resultWriter.SetError(err)
			}
		}
		if meta.TsTagUpdate != nil {
			r.resultWriter.IncrementRowsAffected(int(meta.TsTagUpdate.UpdatedRow))
			r.status = execinfra.ConsumerClosed
			if !meta.TsTagUpdate.IsUpdateSuccess {
				err := errors.Newf("update tag of timeseries table failed, reason:%s", meta.TsTagUpdate.UpdateErr)
				r.resultWriter.SetError(err)
			}
		}
		if meta.TsCreate != nil {
			r.status = execinfra.ConsumerClosed
			if !meta.TsCreate.CreateSuccess {
				err := errors.Newf("create timeseries table failed, reason:%s", meta.TsCreate.CreateErr)
				r.resultWriter.SetError(err)
			}
		}
		if meta.TsPro != nil {
			r.status = execinfra.ConsumerClosed
			if !meta.TsPro.Success {
				err := errors.Newf(meta.TsPro.Err)
				r.resultWriter.SetError(err)
			}
		}
		if meta.TsAlterColumn != nil {
			r.status = execinfra.ConsumerClosed
			if !meta.TsAlterColumn.AlterSuccess {
				r.nodeErrForTsAlter = meta.TsAlterColumn.NodeIDMapErr
				err := errors.Newf("alter ts column failed, reason:%s", meta.TsAlterColumn.AlterErr)
				r.resultWriter.SetError(err)
			}
		}

		if meta.LeafTxnFinalState != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.LeafTxnFinalState.Txn.ID {
					if err := r.txn.UpdateRootWithLeafFinalState(r.ctx, meta.LeafTxnFinalState); err != nil {
						r.resultWriter.SetError(err)
					}
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf final state (%s); but have no root", meta.LeafTxnFinalState))
			}
		}
		if meta.Err != nil {
			// Check if the error we just received should take precedence over a
			// previous error (if any).
			if roachpb.ErrPriority(meta.Err) > roachpb.ErrPriority(r.resultWriter.Err()) {
				if r.txn != nil {
					if err, ok := errors.If(meta.Err, func(err error) (v interface{}, ok bool) {
						v, ok = err.(*roachpb.UnhandledRetryableError)
						return v, ok
					}); ok {
						retryErr := err.(*roachpb.UnhandledRetryableError)
						// Update the txn in response to remote errors. In the non-DistSQL
						// world, the TxnCoordSender handles "unhandled" retryable errors,
						// but this one is coming from a distributed SQL node, which has
						// left the handling up to the root transaction.
						meta.Err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
						// Update the clock with information from the error. On non-DistSQL
						// code paths, the DistSender does this.
						// TODO(andrei): We don't propagate clock signals on success cases
						// through DistSQL; we should. We also don't propagate them through
						// non-retryable errors; we also should.
						r.updateClock(retryErr.PErr.Now)
					}
				}
				r.resultWriter.SetError(meta.Err)
			}
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.resultWriter.Err() == nil {
				r.resultWriter.SetError(err)
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.resultWriter.SetError(
					errors.New("trying to ingest remote spans but there is no recording span set up"))
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
			}
		}
		if meta.Metrics != nil {
			r.bytesRead += meta.Metrics.BytesRead
			r.rowsRead += meta.Metrics.RowsRead
			if r.progressAtomic != nil && r.expectedRowsRead != 0 {
				progress := float64(r.rowsRead) / float64(r.expectedRowsRead)
				atomic.StoreUint64(r.progressAtomic, math.Float64bits(progress))
			}
			meta.Metrics.Release()
			meta.Release()
		}
		if metaWriter, ok := r.resultWriter.(metadataResultWriter); ok {
			metaWriter.AddMeta(r.ctx, meta)
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return execinfra.ConsumerClosed
	}
	if r.status != execinfra.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.IncrementRowsAffected(int(tree.MustBeDInt(row[0].Datum)))
		return r.status
	}

	if r.discardRows {
		// Discard rows.
		return r.status
	}

	// If no columns are needed by the output, the consumer is only looking for
	// whether a single row is pushed or not, so the contents do not matter, and
	// planNodeToRowSource is not set up to handle decoding the row.
	if r.noColsRequired {
		r.row = []tree.Datum{}
		r.status = execinfra.ConsumerClosed
	} else {
		if r.row == nil {
			r.row = make(tree.Datums, len(r.resultToStreamColMap))
		}
		for i, resIdx := range r.resultToStreamColMap {
			err := row[resIdx].EnsureDecoded(&r.outputTypes[resIdx], &r.alloc)
			if err != nil {
				r.resultWriter.SetError(err)
				r.status = execinfra.ConsumerClosed
				return r.status
			}
			r.row[i] = row[resIdx].Datum
		}
	}
	r.tracing.TraceExecRowsResult(r.ctx, r.row)
	// Note that AddRow accounts for the memory used by the Datums.
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		// ErrLimitedResultClosed is not a real error, it is a
		// signal to stop distsql and return success to the client.
		if !errors.Is(commErr, ErrLimitedResultClosed) {
			// Set the error on the resultWriter too, for the convenience of some of the
			// clients. If clients don't care to differentiate between communication
			// errors and query execution errors, they can simply inspect
			// resultWriter.Err(). Also, this function itself doesn't care about the
			// distinction and just uses resultWriter.Err() to see if we're still
			// accepting results.
			r.resultWriter.SetError(commErr)

			// We don't need to shut down the connection
			// if there's a portal-related error. This is
			// definitely a layering violation, but is part
			// of some accepted technical debt (see comments on
			// sql/pgwire.limitedCommandResult.moreResultsNeeded).
			// Instead of changing the signature of AddRow, we have
			// a sentinel error that is handled specially here.
			if !errors.Is(commErr, ErrLimitedResultNotSupported) {
				r.commErr = commErr
			}
			r.status = execinfra.ConsumerClosed
		} else {
			r.status = execinfra.DrainRequested
		}
	}
	return r.status
}

var (
	// ErrLimitedResultNotSupported is an error produced by pgwire
	// indicating an unsupported feature of row count limits was attempted.
	ErrLimitedResultNotSupported = unimplemented.NewWithIssue(40195, "multiple active portals not supported")
	// ErrLimitedResultClosed is a sentinel error produced by pgwire
	// indicating the portal should be closed without error.
	ErrLimitedResultClosed = errors.New("row count limit closed")
)

// ProducerDone is part of the RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
	r.cleanup()
}

// Types is part of the RowReceiver interface.
func (r *DistSQLReceiver) Types() []types.T {
	return r.outputTypes
}

// updateCaches takes information about some ranges that were mis-planned and
// updates the range descriptor and lease-holder caches accordingly.
//
// TODO(andrei): updating these caches is not perfect: we can clobber newer
// information that someone else has populated because there's no timing info
// anywhere. We also may fail to remove stale info from the LeaseHolderCache if
// the ids of the ranges that we get are different than the ids in that cache.
func (r *DistSQLReceiver) updateCaches(ctx context.Context, ranges []roachpb.RangeInfo) error {
	// Update the RangeDescriptorCache.
	rngDescs := make([]roachpb.RangeDescriptor, len(ranges))
	for i, ri := range ranges {
		rngDescs[i] = ri.Desc
	}
	if err := r.rangeCache.InsertRangeDescriptors(ctx, rngDescs...); err != nil {
		return err
	}

	// Update the LeaseHolderCache.
	for _, ri := range ranges {
		r.leaseCache.Update(ctx, ri.Desc.RangeID, ri.Lease.Replica.StoreID)
	}
	return nil
}

// PlanAndRunSubqueries returns false if an error was encountered and sets that
// error in the provided receiver.
func (dsp *DistSQLPlanner) PlanAndRunSubqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	for planIdx, subqueryPlan := range subqueryPlans {
		stmt := ""
		if planner.extendedEvalCtx.Tracing.enabled {
			// display sql of subquery in trace.
			fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
			subqueryPlan.subquery.Format(fmtCtx)
			stmt = "[Subquery]: " + fmtCtx.CloseAndGetString()
		}

		if err := dsp.planAndRunSubquery(
			ctx,
			planIdx,
			subqueryPlan,
			planner,
			evalCtxFactory(),
			subqueryPlans,
			recv,
			maybeDistribute,
			stmt,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}

	return true
}

func (dsp *DistSQLPlanner) planAndRunSubquery(
	ctx context.Context,
	planIdx int,
	subqueryPlan subquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
	stmt string,
) error {
	subqueryMonitor := mon.MakeMonitor(
		"subquery",
		mon.MemoryResource,
		dsp.distSQLSrv.Metrics.CurBytesCount,
		dsp.distSQLSrv.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		dsp.distSQLSrv.Settings,
	)
	subqueryMonitor.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
	defer subqueryMonitor.Stop(ctx)

	subqueryMemAccount := subqueryMonitor.MakeBoundAccount()
	defer subqueryMemAccount.Close(ctx)

	var subqueryPlanCtx *PlanningCtx
	var distributeSubquery bool
	if maybeDistribute {
		distributeSubquery = shouldDistributePlan(
			ctx, planner.SessionData().DistSQLMode, dsp, subqueryPlan.plan)
	}
	if distributeSubquery {
		subqueryPlanCtx = dsp.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		subqueryPlanCtx = dsp.newLocalPlanningCtx(ctx, evalCtx)
	}

	subqueryPlanCtx.isLocal = !distributeSubquery
	subqueryPlanCtx.planner = planner
	subqueryPlanCtx.stmtType = tree.Rows
	if planner.collectBundle {
		subqueryPlanCtx.saveDiagram = func(diagram execinfrapb.FlowDiagram) {
			planner.curPlan.distSQLDiagrams = append(planner.curPlan.distSQLDiagrams, diagram)
		}
	}
	// Don't close the top-level plan from subqueries - someone else will handle
	// that.
	subqueryPlanCtx.ignoreClose = true
	subqueryPhysPlan, err := dsp.createPlanForNode(subqueryPlanCtx, subqueryPlan.plan)
	if err != nil {
		return err
	}
	subqueryPlanCtx.runningSubquery = true
	dsp.FinalizePlan(subqueryPlanCtx, &subqueryPhysPlan)

	// TODO(arjun): #28264: We set up a row container, wrap it in a row
	// receiver, and use it and serialize the results of the subquery. The type
	// of the results stored in the container depends on the type of the subquery.
	subqueryRecv := recv.clone()
	var typ sqlbase.ColTypeInfo
	var rows *rowcontainer.RowContainer
	if subqueryPlan.execMode == rowexec.SubqueryExecModeExists {
		subqueryRecv.noColsRequired = true
		typ = sqlbase.ColTypeInfoFromColTypes([]types.T{})
	} else {
		// Apply the PlanToStreamColMap projection to the ResultTypes to get the
		// final set of output types for the subquery. The reason this is necessary
		// is that the output schema of a query sometimes contains columns necessary
		// to merge the streams, but that aren't required by the final output of the
		// query. These get projected out, so we need to similarly adjust the
		// expected result types of the subquery here.
		colTypes := make([]types.T, len(subqueryPhysPlan.PlanToStreamColMap))
		for i, resIdx := range subqueryPhysPlan.PlanToStreamColMap {
			colTypes[i] = subqueryPhysPlan.ResultTypes[resIdx]
		}
		typ = sqlbase.ColTypeInfoFromColTypes(colTypes)
	}
	rows = rowcontainer.NewRowContainer(subqueryMemAccount, typ, 0)
	defer rows.Close(ctx)

	subqueryRowReceiver := NewRowResultWriter(rows)
	subqueryRecv.resultWriter = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	subqueryPhysPlan.SQL = stmt
	dsp.Run(subqueryPlanCtx, planner.txn, &subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if subqueryRecv.commErr != nil {
		return subqueryRecv.commErr
	}
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.Len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case rowexec.SubqueryExecModeAllRows, rowexec.SubqueryExecModeAllRowsNormalized:
		var result tree.DTuple
		for rows.Len() > 0 {
			row := rows.At(0)
			rows.PopFirst()
			if row.Len() == 1 {
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				result.D = append(result.D, row[0])
			} else {
				result.D = append(result.D, &tree.DTuple{D: row})
			}
		}

		if subqueryPlan.execMode == rowexec.SubqueryExecModeAllRowsNormalized {
			result.Normalize(&evalCtx.EvalContext)
		}
		subqueryPlans[planIdx].result = &result
	case rowexec.SubqueryExecModeOneRow:
		switch rows.Len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			row := rows.At(0)
			switch row.Len() {
			case 1:
				subqueryPlans[planIdx].result = row[0]
			default:
				subqueryPlans[planIdx].result = &tree.DTuple{D: rows.At(0)}
			}
		default:
			return pgerror.New(pgcode.CardinalityViolation,
				"more than one row returned by a subquery used as an expression")
		}
	default:
		return fmt.Errorf("unexpected subqueryExecMode: %d", subqueryPlan.execMode)
	}
	return nil
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// It returns a non-nil (although it can be a noop when an error is
// encountered) cleanup function that must be called once the planTop AST is no
// longer needed and can be closed. Note that this function also cleans up the
// flow which is unfortunate but is caused by the sharing of memory monitors
// between planning and execution - cleaning up the flow wants to close the
// monitor, but it cannot do so because the AST needs to live longer and still
// uses the same monitor. That's why we end up in a situation that in order to
// clean up the flow, we need to close the AST first, but we can only do that
// after PlanAndRun returns.
// stmt is use for displaying sql in trace.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan planNode,
	recv *DistSQLReceiver,
	stmt string,
) (cleanup func()) {
	physPlan := dsp.GetPhysPlan(ctx, planCtx, plan, recv, stmt)
	if physPlan == nil {
		return func() {}
	}

	return dsp.Run(planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)
}

// GetPhysPlan gets physical plan
func (dsp *DistSQLPlanner) GetPhysPlan(
	ctx context.Context, planCtx *PlanningCtx, plan planNode, recv *DistSQLReceiver, stmt string,
) *PhysicalPlan {
	log.VEventf(ctx, 1, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)
	physPlan, err := dsp.createPlanForNode(planCtx, plan)
	if err != nil {
		recv.SetError(err)
		return nil
	}
	dsp.FinalizePlan(planCtx, &physPlan)
	recv.expectedRowsRead = int64(physPlan.TotalEstimatedScannedRows)
	physPlan.SQL = stmt
	return &physPlan
}

// PlanAndStart for get physical plan and start plan
func (dsp *DistSQLPlanner) PlanAndStart(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan planNode,
	recv *DistSQLReceiver,
	stmt string,
) (flowinfra.Flow, context.Context) {
	physPlan := dsp.GetPhysPlan(ctx, planCtx, plan, recv, stmt)
	if physPlan == nil {
		return nil, planCtx.ctx
	}
	return dsp.Start(planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)
}

// PlanAndRunPostqueries returns false if an error was encountered and sets
// that error in the provided receiver.
func (dsp *DistSQLPlanner) PlanAndRunPostqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	postqueryPlans []postquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	prevSteppingMode := planner.Txn().ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = planner.Txn().ConfigureStepping(ctx, prevSteppingMode) }()
	for _, postqueryPlan := range postqueryPlans {
		// We place a sequence point before every postquery, so
		// that each subsequent postquery can observe the writes
		// by the previous step.
		if err := planner.Txn().Step(ctx); err != nil {
			recv.SetError(err)
			return false
		}
		if err := dsp.planAndRunPostquery(
			ctx,
			postqueryPlan,
			planner,
			evalCtxFactory(),
			recv,
			maybeDistribute,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}

	return true
}

func (dsp *DistSQLPlanner) planAndRunPostquery(
	ctx context.Context,
	postqueryPlan postquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) error {
	postqueryMonitor := mon.MakeMonitor(
		"postquery",
		mon.MemoryResource,
		dsp.distSQLSrv.Metrics.CurBytesCount,
		dsp.distSQLSrv.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		dsp.distSQLSrv.Settings,
	)
	postqueryMonitor.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
	defer postqueryMonitor.Stop(ctx)

	postqueryMemAccount := postqueryMonitor.MakeBoundAccount()
	defer postqueryMemAccount.Close(ctx)

	var postqueryPlanCtx *PlanningCtx
	var distributePostquery bool
	if maybeDistribute {
		distributePostquery = shouldDistributePlan(
			ctx, planner.SessionData().DistSQLMode, dsp, postqueryPlan.plan)
	}
	if distributePostquery {
		postqueryPlanCtx = dsp.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		postqueryPlanCtx = dsp.newLocalPlanningCtx(ctx, evalCtx)
	}

	postqueryPlanCtx.isLocal = !distributePostquery
	postqueryPlanCtx.planner = planner
	postqueryPlanCtx.stmtType = tree.Rows
	postqueryPlanCtx.ignoreClose = true
	if planner.collectBundle {
		postqueryPlanCtx.saveDiagram = func(diagram execinfrapb.FlowDiagram) {
			planner.curPlan.distSQLDiagrams = append(planner.curPlan.distSQLDiagrams, diagram)
		}
	}

	postqueryPhysPlan, err := dsp.createPlanForNode(postqueryPlanCtx, postqueryPlan.plan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(postqueryPlanCtx, &postqueryPhysPlan)

	postqueryRecv := recv.clone()
	// TODO(yuzefovich): at the moment, errOnlyResultWriter is sufficient here,
	// but it may not be the case when we support cascades through the optimizer.
	postqueryRecv.resultWriter = &errOnlyResultWriter{}
	dsp.Run(postqueryPlanCtx, planner.txn, &postqueryPhysPlan, postqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if postqueryRecv.commErr != nil {
		return postqueryRecv.commErr
	}
	return postqueryRecv.resultWriter.Err()
}

// displayQueryFlowSpec construct display message and log it.
// flows are the flow of all nodes.
// thisNodeID is the id of gateway node.
func (dsp *DistSQLPlanner) displayQueryFlowSpec(
	ctx context.Context,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	thisNodeID int32,
	sql string,
	isLocal bool,
) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("display flow spec of sql: %s\n", sql))
	buf.WriteString(fmt.Sprintf("Node %v is gateway node\n", thisNodeID))
	buf.WriteString(fmt.Sprintf("IsLocal is %v\n", isLocal))
	isQuery := false

	// sort node id.
	idList := make([]roachpb.NodeID, 0, len(flows))
	for nodeID := range flows {
		idList = append(idList, nodeID)
	}
	sort.Slice(idList, func(i, j int) bool { return idList[i] < idList[j] })

	// display flow order by nodeID.
	for _, id := range idList {
		flow, _ := flows[id]
		buf.WriteString(fmt.Sprintf(">>Node: %v \n", id))
		res, ok := execinfrapb.DisplayFlowSpec(*flow)
		if ok {
			isQuery = true
		}
		buf.WriteString(res)
		buf.WriteString(fmt.Sprintf(">>>>>>>>>>>>>>>>>Node %v End<<<<<<<<<<<<<<<<<\n", id))
	}

	// only log query flow spec.
	if isQuery {
		log.VEventfDepth(ctx, 2, 1, buf.String())
	}
}
