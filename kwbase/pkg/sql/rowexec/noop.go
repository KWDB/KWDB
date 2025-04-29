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

package rowexec

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	execinfra.ProcessorBase
	input           execinfra.RowSource
	InputNum        uint32
	recordNum       uint32
	tsinsertRowNum  uint32
	tsExecResult    bool
	tsExecError     string
	hasFilter       bool
	hasOutputColumn bool

	DedupRows  int64
	DedupRule  int64
	InsertRows int64
	// delete rows
	DeleteRows int64

	// store every node's error.
	NodeIDMapErr map[int32]string
	firstErr     string

	TsOperatorType execinfrapb.OperatorType
	// emitCount is used to track the number of rows that have been
	// emitted from Next().
	emitCount int64
}

var _ execinfra.Processor = &noopProcessor{}
var _ execinfra.RowSource = &noopProcessor{}
var _ execinfra.OpNode = &noopProcessor{}

const noopProcName = "noop"

func newNoopProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	noopSpec *execinfrapb.NoopCoreSpec,
) (*noopProcessor, error) {

	n := &noopProcessor{input: input}
	if noopSpec != nil {
		n.InputNum = noopSpec.InputNum
		n.tsExecResult = true
		flowCtx.NoopInputNums = noopSpec.InputNum
		n.TsOperatorType = noopSpec.TsOperator
	}
	if post != nil {
		n.hasFilter = !post.Filter.Empty()
		n.hasOutputColumn = len(post.OutputColumns) > 0
	}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{n.input}},
	); err != nil {
		return nil, err
	}

	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		n.input = newInputStatCollector(n.input)
		n.FinishTrace = n.outputStatsToTrace
	}

	return n, nil
}

// Start is part of the RowSource interface.
func (n *noopProcessor) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, noopProcName)
}

// Next is part of the RowSource interface.
func (n *noopProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for n.State == execinfra.StateRunning {

		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata

		row, meta = n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			if n.handleMetaForTs(meta) {
				continue
			}

			return nil, meta
		}

		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
		const cancelCheckCount = 1024
		n.emitCount++
		if n.emitCount%cancelCheckCount == 0 {
			if err := n.PbCtx().Err(); err != nil {
				n.MoveToDraining(err)
				return nil, n.DrainHelper()
			}
		}
	}
	return nil, n.DrainHelper()
}

// handleMetaForTs handles Meta Information Returned by Timing Operators.
func (n *noopProcessor) handleMetaForTs(meta *execinfrapb.ProducerMetadata) (NeedMoreMetas bool) {
	switch n.TsOperatorType {
	case execinfrapb.OperatorType_TsInsert:
		if n.InputNum > 1 {
			if meta.TsInsert != nil {
				n.recordNum++
				n.tsinsertRowNum += meta.TsInsert.NumRow
				n.tsExecResult = n.tsExecResult && meta.TsInsert.InsertSuccess
				if !meta.TsInsert.InsertSuccess {
					n.tsExecError += meta.TsInsert.InsertErr
				} else {
					n.DedupRule = meta.TsInsert.DedupRule
					n.DedupRows += meta.TsInsert.DedupRows
					n.InsertRows += meta.TsInsert.InsertRows
				}
			}
			if n.recordNum < n.InputNum {
				return true
			}
			meta.TsInsert.NumRow = n.tsinsertRowNum
			meta.TsInsert.InsertSuccess = n.tsExecResult
			meta.TsInsert.InsertErr = n.tsExecError
			meta.TsInsert.InsertRows = n.InsertRows
			meta.TsInsert.DedupRule = n.DedupRule
			meta.TsInsert.DedupRows = n.DedupRows
		}
	case execinfrapb.OperatorType_TsDropTsTable, execinfrapb.OperatorType_TsDropDeleteEntities,
		execinfrapb.OperatorType_TsDeleteExpiredData, execinfrapb.OperatorType_TsCompressTsTable,
		execinfrapb.OperatorType_TsVacuum, execinfrapb.OperatorType_TsCount:
		// n.InputNum > 1: Collect multi node information.
		if n.InputNum > 1 {
			if meta.TsPro != nil {
				if meta.TsPro.Success {
					n.recordNum++
				} else {
					// Returns an error after any node fails.
					return false
				}
				if n.recordNum < n.InputNum {
					return true
				}
			}
		}
	case execinfrapb.OperatorType_TsDropColumn, execinfrapb.OperatorType_TsAddColumn, execinfrapb.OperatorType_TsAlterType,
		execinfrapb.OperatorType_TsCommit, execinfrapb.OperatorType_TsRollback:
		if n.InputNum > 1 {
			if meta.TsAlterColumn != nil {
				if !meta.TsAlterColumn.AlterSuccess {
					if n.NodeIDMapErr == nil {
						n.NodeIDMapErr = make(map[int32]string, 0)
						n.firstErr = meta.TsAlterColumn.AlterErr
					}
					n.NodeIDMapErr[meta.TsAlterColumn.NodeID] = meta.TsAlterColumn.NodeIDMapErr[meta.TsAlterColumn.NodeID]
				}
				n.recordNum++
				if n.recordNum < n.InputNum {
					return true
				}
				meta.TsAlterColumn.NodeIDMapErr = n.NodeIDMapErr
				meta.TsAlterColumn.AlterErr = n.firstErr
				if n.firstErr != "" {
					meta.TsAlterColumn.AlterSuccess = false
				}
			}
		}
	case execinfrapb.OperatorType_TsCreateTable:
		if n.InputNum > 1 {
			if meta.TsCreate != nil {
				if meta.TsCreate.CreateSuccess {
					n.recordNum++
				} else {
					return false
				}
				if n.recordNum < n.InputNum {
					return true
				}
			}
		}
	case execinfrapb.OperatorType_TsDeleteMultiEntitiesData:
		if n.InputNum > 1 {
			if meta.TsDelete != nil {
				if meta.TsDelete.DeleteSuccess {
					n.DeleteRows += int64(meta.TsDelete.DeleteRow)
					n.recordNum++
				} else {
					// Returns an error after any node fails.
					return false
				}
				if n.recordNum < n.InputNum {
					return true
				}
				meta.TsDelete.DeleteRow = uint64(n.DeleteRows)
			}
		}
	default:
	}

	return false
}

func (n *noopProcessor) IsShortCircuitForPgEncode() bool {
	// n.InputNum > 1: Multiple nodes refuse to use this optimization.
	return n.InputNum <= 1 && !n.hasFilter && !n.hasOutputColumn
}

// ConsumerClosed is part of the RowSource interface.
func (n *noopProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}

// ChildCount is part of the execinfra.OpNode interface.
func (n *noopProcessor) ChildCount(bool) int {
	if _, ok := n.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (n *noopProcessor) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := n.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to noop is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}

const noopTagPrefix = "noop."

// Stats implements the SpanStats interface.
func (n *NoopStats) Stats() map[string]string {
	return n.InputStats.Stats(noopTagPrefix)
}

// TsStats is stats of analyse in time series
func (n *NoopStats) TsStats() map[int32]map[string]string {
	return nil
}

// GetSpanStatsType check type of spanStats
func (n *NoopStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (n *NoopStats) StatsForQueryPlan() []string {
	return n.InputStats.StatsForQueryPlan("")
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (n *NoopStats) TsStatsForQueryPlan() map[int32][]string {
	return nil
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (n *noopProcessor) outputStatsToTrace() {
	is, ok := getInputStats(n.FlowCtx, n.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(n.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp, &OrdinalityStats{InputStats: is},
		)
	}
}
