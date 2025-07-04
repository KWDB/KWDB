// Copyright 2017 The Cockroach Authors.
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
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/logtags"
)

// RowResultWriter is a thin wrapper around a RowContainer.
type RowResultWriter struct {
	rowContainer *rowcontainer.RowContainer
	rowsAffected int
	err          error
}

var _ rowResultWriter = &RowResultWriter{}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(rowContainer *rowcontainer.RowContainer) *RowResultWriter {
	return &RowResultWriter{rowContainer: rowContainer}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) IncrementRowsAffected(n int) {
	b.rowsAffected += n
}

// RowsAffected returns either the number of times AddRow was called, or the
// sum of all n passed into IncrementRowsAffected.
func (b *RowResultWriter) RowsAffected() int {
	return b.rowsAffected
}

// AddPGResult implements the rowResultWriter interface.
func (b *RowResultWriter) AddPGResult(ctx context.Context, res []byte) error {
	return nil
}

// AddPGComplete implements the rowResultWriter interface.
func (b *RowResultWriter) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

// AddRow implements the rowResultWriter interface.
func (b *RowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	_, err := b.rowContainer.AddRow(ctx, row)
	return err
}

// SetError is part of the rowResultWriter interface.
func (b *RowResultWriter) SetError(err error) {
	b.err = err
}

// Err is part of the rowResultWriter interface.
func (b *RowResultWriter) Err() error {
	return b.err
}

// callbackResultWriter is a rowResultWriter that runs a callback function
// on AddRow.
type callbackResultWriter struct {
	fn           func(ctx context.Context, row tree.Datums) error
	rowsAffected int
	err          error
}

var _ rowResultWriter = &callbackResultWriter{}

// newCallbackResultWriter creates a new callbackResultWriter.
func newCallbackResultWriter(
	fn func(ctx context.Context, row tree.Datums) error,
) *callbackResultWriter {
	return &callbackResultWriter{fn: fn}
}

func (c *callbackResultWriter) IncrementRowsAffected(n int) {
	c.rowsAffected += n
}

// RowsAffected returns either the number of times AddRow was called, or the
// sum of all n passed into IncrementRowsAffected.
func (c *callbackResultWriter) RowsAffected() int {
	return c.rowsAffected
}

func (c *callbackResultWriter) AddPGResult(ctx context.Context, res []byte) error {
	return nil
}

// AddPGComplete implements the rowResultWriter interface.
func (c *callbackResultWriter) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

func (c *callbackResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return c.fn(ctx, row)
}

func (c *callbackResultWriter) SetError(err error) {
	c.err = err
}

func (c *callbackResultWriter) Err() error {
	return c.err
}

func (dsp *DistSQLPlanner) setupAllNodesPlanning(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []roachpb.NodeID, error) {
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* txn */)

	resp, err := execCfg.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, nil, err
	}
	// Because we're not going through the normal pathways, we have to set up
	// the nodeID -> nodeAddress map ourselves.
	for _, node := range resp.Nodes {
		if err := dsp.CheckNodeHealthAndVersion(planCtx, &node.Desc); err != nil {
			continue
		}
	}
	nodes := make([]roachpb.NodeID, 0, len(planCtx.NodeAddresses))
	for nodeID := range planCtx.NodeAddresses {
		nodes = append(nodes, nodeID)
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return planCtx, nodes, nil
}

/*
	 assaign one table by files input & make specs for the table by node alives
			Specs	spec[0]   spec[1]   spec[2]	...
			nodes	node[0]   node[1]   node[2]	...
			files   file[0]   file[1]   file[2]	...
					file[3]	...
					file[n-2] file[n-1] file[n]
		Spec[nodeID].URI[file_id] = files[file_id]
		  Spec[0].URI[0] = files[0] | Spec[1].URI[1] = files[1] | ...
		  Spec[0].URI[3] = files[3] | Spec[1].URI[4] = files[4] | ...
		Spec[nodeID].Results[file_id] -> job.TableResults[tableID].Results[file_id]
		  Spec[0].Results[0] -> job.TableResults[tableID].Results[0]
		  Spec[1].Results[1] -> job.TableResults[tableID].Results[1]
		  Spec[2].Results[2] -> job.TableResults[tableID].Results[2]
		  Spec[0].Results[3] -> job.TableResults[tableID].Results[3]
		  Spec[1].Results[4] -> job.TableResults[tableID].Results[4]
		  ...
*/
func makeImportReaderSpecs(
	job *jobs.Job,
	table sqlbase.ImportTable,
	from []string,
	format roachpb.IOFileFormat,
	nodes []roachpb.NodeID,
	walltime int64,
	writeWAL bool,
) []*execinfrapb.ReadImportDataSpec {
	// For each input file, assign it to a node.
	inputSpecs := make([]*execinfrapb.ReadImportDataSpec, 0, len(nodes))
	progress := job.Progress()
	importProgress := progress.GetImport()
	optimizedDispatch := len(nodes) == 1
	for i, input := range from {
		// Round robin assign CSV files to nodes. Files 0 through len(nodes)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < len(nodes) {
			spec := &execinfrapb.ReadImportDataSpec{
				Table:  table,
				Format: format,
				Progress: execinfrapb.JobProgress{
					JobID: *job.ID(), // all import specs use one jobid
					Slot:  int32(i),
				},
				WalltimeNanos:     walltime,
				Uri:               make(map[int32]string),
				ResumePos:         make(map[int32]int64),
				OptimizedDispatch: optimizedDispatch,
				WriteWAL:          writeWAL,
			}
			inputSpecs = append(inputSpecs, spec)
		}
		n := i % len(nodes)
		// specs[node_i].URI[fileIDX] = fromFiles[fileIDX]
		inputSpecs[n].Uri[int32(i)] = input
		// specs[node_i].Results[node_i] = progress.TableResultsPos[tableID].Results[fileIDX]
		if importProgress.TableResumePos[int32(table.Desc.ID)].ResumePos != nil {
			inputSpecs[n].ResumePos[int32(i)] = importProgress.TableResumePos[int32(table.Desc.ID)].ResumePos[int32(i)]
		}
	}
	return inputSpecs
}

func presplitTableBoundaries(
	ctx context.Context, cfg *ExecutorConfig, table sqlbase.ImportTable,
) error {
	expirationTime := cfg.DB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	for _, span := range table.Desc.AllIndexSpans() {
		if err := cfg.DB.AdminSplit(ctx, span.Key, span.Key, expirationTime); err != nil {
			return err
		}

		log.VEventf(ctx, 1, "scattering index range %s", span.Key)
		scatterReq := &roachpb.AdminScatterRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(span),
		}
		if _, pErr := kv.SendWrapped(ctx, cfg.DB.NonTransactionalSender(), scatterReq); pErr != nil {
			log.Errorf(ctx, "failed to scatter span %s: %s", span.Key, pErr)
		}
	}
	return nil
}

// DistIngest is used by IMPORT to run a DistSQL flow to ingest data by starting
// reader processes on many nodes that each read and ingest their assigned files
// and then send back a summary of what they ingested. The combined summary is
// returned.
func DistIngest(
	ctx context.Context,
	phs PlanHookState,
	table sqlbase.ImportTable,
	from []string,
	job *jobs.Job,
	tableNums int,
) (roachpb.BulkOpSummary, error) {
	// from is all files need to read
	ctx = logtags.AddTag(ctx, "import-distsql-ingest", nil)
	details := job.Details().(jobspb.ImportDetails)
	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()

	planCtx, nodes, err := dsp.setupAllNodesPlanning(ctx, evalCtx, phs.ExecCfg())
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	inputSpecs := makeImportReaderSpecs(job, table, from, details.Format, nodes, details.Walltime, details.WriteWAL)

	var p PhysicalPlan
	// Setup a one-stage plan with one proc per input spec.
	stageID := p.NewStageID()
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(inputSpecs))
	// assaign specs to node
	for i, rcs := range inputSpecs {
		proc := physicalplan.Processor{
			Node: nodes[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{ReadImport: rcs},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	// The direct-ingest readers will emit a binary encoded BulkOpSummary.
	p.PlanToStreamColMap = []int{0, 1}
	p.ResultTypes = []types.T{*types.Bytes, *types.Bytes}

	tableID := table.Desc.ID
	dsp.FinalizePlan(planCtx, &p)

	if err := job.FractionProgressed(ctx,
		func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			// make results for every table
			prog := details.(*jobspb.Progress_Import).Import
			prog.ReadProgress = make([]float32, len(from))
			if prog.TableResumePos == nil {
				prog.TableResumePos = make(map[int32]jobspb.ImportResumePos, tableNums)
			}
			// make table results for every file
			if _, ok := prog.TableResumePos[int32(tableID)]; !ok {
				prog.TableResumePos[int32(tableID)] = jobspb.ImportResumePos{ResumePos: make([]int64, len(from))}
			}
			return 0.0
		},
	); err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	fileProgress := make([]int64, len(from))
	fractionProgress := make([]uint32, len(from))

	updateJobProgress := func() error {
		return job.FractionProgressed(ctx,
			func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				var overall float32
				prog := details.(*jobspb.Progress_Import).Import
				for i := range fileProgress {
					// tablesResults[tableID].ResultsJobPos[fileIDX] = fileRunProces[fileIDX]
					prog.TableResumePos[int32(tableID)].ResumePos[i] = atomic.LoadInt64(&fileProgress[i])
				}
				for i := range fractionProgress {
					// prog.ReadProgress[fileIDX] = fileReadProcess[fileIDX]
					fileProgress := math.Float32frombits(atomic.LoadUint32(&fractionProgress[i]))
					prog.ReadProgress[i] = fileProgress
					overall += fileProgress
				}
				return overall / float32(len(from))
			},
		)
	}

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			for i, v := range meta.BulkProcessorProgress.ResumePos {
				atomic.StoreInt64(&fileProgress[i], v)
			}
			for i, v := range meta.BulkProcessorProgress.CompletedFraction {
				atomic.StoreUint32(&fractionProgress[i], math.Float32bits(v))
			}
			return updateJobProgress()
		}
		return nil
	}

	var res roachpb.BulkOpSummary
	// write file result to console or job table
	rowResultWriter := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts roachpb.BulkOpSummary
		// unmarshal results by every spec processors; total = node counts
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})

	if table.TableType == tree.RelationalTable {
		if err := presplitTableBoundaries(ctx, phs.ExecCfg(), table); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
	}
	// send to job table or user console
	recv := MakeDistSQLReceiver(
		ctx,
		&metadataCallbackWriter{rowResultWriter: rowResultWriter, fn: metaFn},
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(planCtx, nil, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
		return rowResultWriter.Err()
	})
	// wait for every dispatched file result
	if err := g.Wait(); err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	return res, nil
}
