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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// countAggregator is a simple processor that counts the number of rows it
// receives. It's a specialized aggregator that can be used for COUNT(*).
type countAggregator struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	count int
}

var _ execinfra.Processor = &countAggregator{}
var _ execinfra.RowSource = &countAggregator{}

const countRowsProcName = "count rows"

var outputTypes = []types.T{*types.Int}

func newCountAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ag.input = newInputStatCollector(input)
		ag.FinishTrace = ag.outputStatsToTrace
	}

	if err := ag.Init(
		ag,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ag.input},
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *countAggregator) Start(ctx context.Context) context.Context {
	ag.input.Start(ctx)
	return ag.StartInternal(ctx, countRowsProcName)
}

func (ag *countAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			ret := make(sqlbase.EncDatumRow, 1)
			ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
			rendered, _, err := ag.Out.ProcessRow(ag.PbCtx(), ret)
			// We're done as soon as we process our one output row, so we
			// transition into draining state. We will, however, return non-nil
			// error (if such occurs during rendering) separately below.
			ag.MoveToDraining(nil /* err */)
			if err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
			return rendered, nil
		}
		ag.count++
	}
	return nil, ag.DrainHelper()
}

func (ag *countAggregator) ConsumerDone() {
	ag.MoveToDraining(nil /* err */)
}

func (ag *countAggregator) ConsumerClosed() {
	ag.InternalClose()
}

// InitProcessorProcedure init processor in procedure
func (ag *countAggregator) InitProcessorProcedure(txn *kv.Txn) {
	if ag.EvalCtx.IsProcedure {
		if ag.FlowCtx != nil {
			ag.FlowCtx.Txn = txn
		}
		ag.Closed = false
		ag.State = execinfra.StateRunning
		ag.Out.SetRowIdx(0)
	}
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (ag *countAggregator) outputStatsToTrace() {
	is, ok := getInputStats(ag.FlowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp, &AggregatorStats{InputStats: is},
		)
	}
}
