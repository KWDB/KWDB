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

package rowexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// ordinalityProcessor is the processor of the WITH ORDINALITY operator, which
// adds an additional ordinal column to the result.
type ordinalityProcessor struct {
	execinfra.ProcessorBase

	input  execinfra.RowSource
	curCnt int64
}

var _ execinfra.Processor = &ordinalityProcessor{}
var _ execinfra.RowSource = &ordinalityProcessor{}

const ordinalityProcName = "ordinality"

func newOrdinalityProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.OrdinalitySpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	o := &ordinalityProcessor{input: input, curCnt: 1}

	colTypes := make([]types.T, len(input.OutputTypes())+1)
	copy(colTypes, input.OutputTypes())
	colTypes[len(colTypes)-1] = *types.Int
	if err := o.Init(
		o,
		post,
		colTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{o.input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				o.ConsumerClosed()
				return nil
			}},
	); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		o.input = newInputStatCollector(o.input)
		o.FinishTrace = o.outputStatsToTrace
	}

	return o, nil
}

// Start is part of the RowSource interface.
func (o *ordinalityProcessor) Start(ctx context.Context) context.Context {
	o.input.Start(ctx)
	return o.StartInternal(ctx, ordinalityProcName)
}

// Next is part of the RowSource interface.
func (o *ordinalityProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for o.State == execinfra.StateRunning {
		row, meta := o.input.Next()
		if meta != nil {
			if meta.Err != nil {
				o.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			o.MoveToDraining(nil /* err */)
			break
		}

		// The ordinality should increment even if the row gets filtered out.
		row = append(row, sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(o.curCnt))))
		o.curCnt++
		if outRow := o.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, o.DrainHelper()

}

// ConsumerClosed is part of the RowSource interface.
func (o *ordinalityProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	o.InternalClose()
}

const ordinalityTagPrefix = "ordinality."

// Stats implements the SpanStats interface.
func (os *OrdinalityStats) Stats() map[string]string {
	return os.InputStats.Stats(ordinalityTagPrefix)
}

// TsStats is stats of analyse in time series
func (os *OrdinalityStats) TsStats() map[int32]map[string]string {
	return nil
}

// GetSpanStatsType check type of spanStats
func (os *OrdinalityStats) GetSpanStatsType() int {
	return tracing.SpanStatsTypeDefault
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (os *OrdinalityStats) StatsForQueryPlan() []string {
	return os.InputStats.StatsForQueryPlan("")
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (os *OrdinalityStats) TsStatsForQueryPlan() map[int32][]string {
	return nil
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (o *ordinalityProcessor) outputStatsToTrace() {
	is, ok := getInputStats(o.FlowCtx, o.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(o.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp, &OrdinalityStats{InputStats: is},
		)
	}
}
