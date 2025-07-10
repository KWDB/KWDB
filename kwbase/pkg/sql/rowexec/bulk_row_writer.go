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
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// CTASPlanResultTypes is the result types for EXPORT plans.
var CTASPlanResultTypes = []types.T{
	*types.Bytes, // rows
}

type bulkRowWriter struct {
	execinfra.ProcessorBase
	flowCtx        *execinfra.FlowCtx
	processorID    int32
	batchIdxAtomic int64
	spec           execinfrapb.BulkRowWriterSpec
	input          execinfra.RowSource
	output         execinfra.RowReceiver
	summary        roachpb.BulkOpSummary
}

var _ execinfra.Processor = &bulkRowWriter{}
var _ execinfra.RowSource = &bulkRowWriter{}

func newBulkRowWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BulkRowWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &bulkRowWriter{
		flowCtx:        flowCtx,
		processorID:    processorID,
		batchIdxAtomic: 0,
		spec:           spec,
		input:          input,
		output:         output,
	}
	if err := c.Init(
		c, &execinfrapb.PostProcessSpec{}, CTASPlanResultTypes, flowCtx, processorID, output,
		nil /* memMonitor */, execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{input}},
	); err != nil {
		return nil, err
	}
	return c, nil
}

// Start is part of the RowSource interface.
func (sp *bulkRowWriter) Start(ctx context.Context) context.Context {
	sp.input.Start(ctx)
	ctx = sp.StartInternal(ctx, "bulkRowWriter")
	err := sp.work(ctx)
	sp.MoveToDraining(err)
	return ctx
}

// Next is part of the RowSource interface.
func (sp *bulkRowWriter) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// If there wasn't an error while processing, output the summary.
	if sp.ProcessorBase.State == execinfra.StateRunning {
		countsBytes, marshalErr := protoutil.Marshal(&sp.summary)
		sp.MoveToDraining(marshalErr)
		if marshalErr == nil {
			// Output the summary.
			return sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
			}, nil
		}
	}
	return nil, sp.DrainHelper()
}

func (sp *bulkRowWriter) work(ctx context.Context) error {
	kvCh := make(chan row.KVBatch, 10)
	var g ctxgroup.Group

	conv, err := row.NewDatumRowConverter(ctx, nil,
		&sp.spec.Table /* targetColNames */, sp.EvalCtx, kvCh)
	if err != nil {
		return err
	}
	if conv.EvalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	g = ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		return sp.ingestLoop(ctx, kvCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		return sp.convertLoop(ctx, kvCh, conv)
	})
	return g.Wait()
}

func (sp *bulkRowWriter) OutputTypes() []types.T {
	return CTASPlanResultTypes
}

func (sp *bulkRowWriter) ingestLoop(ctx context.Context, kvCh chan row.KVBatch) error {
	writeTS := sp.spec.Table.CreateAsOfTime
	const bufferSize int64 = 64 << 20
	adder, err := sp.flowCtx.Cfg.BulkAdder(
		ctx, sp.flowCtx.Cfg.DB, writeTS, storagebase.BulkAdderOptions{MinBufferSize: bufferSize, ShouldLogLogicalOp: true},
	)
	if err != nil {
		return err
	}
	defer adder.Close(ctx)

	// ingestKvs drains kvs from the channel until it closes, ingesting them using
	// the BulkAdder. It handles the required buffering/sorting/etc.
	ingestKvs := func() error {
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				if err := adder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					if _, ok := err.(storagebase.DuplicateKeyError); ok {
						return errors.WithStack(err)
					}
					return err
				}
			}
		}

		if err := adder.Flush(ctx); err != nil {
			if err, ok := err.(storagebase.DuplicateKeyError); ok {
				return errors.WithStack(err)
			}
			return err
		}
		return nil
	}

	// Drain the kvCh using the BulkAdder until it closes.
	if err := ingestKvs(); err != nil {
		return err
	}

	sp.summary = adder.GetSummary()
	return nil
}

func (sp *bulkRowWriter) convertLoop(
	ctx context.Context, kvCh chan row.KVBatch, conv *row.DatumRowConverter,
) error {
	defer close(kvCh)

	done := false
	alloc := &sqlbase.DatumAlloc{}
	typs := sp.input.OutputTypes()

	for {
		var rows int64
		for {
			row, meta := sp.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return meta.Err
				}
				sp.AppendTrailingMeta(*meta)
				continue
			}
			if row == nil {
				done = true
				break
			}
			rows++

			for i, ed := range row {
				if ed.IsNull() {
					conv.Datums[i] = tree.DNull
					continue
				}
				if err := ed.EnsureDecoded(&typs[i], alloc); err != nil {
					return err
				}
				conv.Datums[i] = ed.Datum
			}

			// `conv.Row` uses these as arguments to GenerateUniqueID to generate
			// hidden primary keys, when necessary. We want them to be ascending per
			// to reduce overlap in the resulting kvs and non-conflicting (because
			// of primary key uniqueness). The ids that come out of GenerateUniqueID
			// are sorted by (fileIndex, rowIndex) and unique as long as the two
			// inputs are a unique combo, so using the processor ID and a
			// monotonically increasing batch index should do what we want.
			if err := conv.Row(ctx, sp.processorID, sp.batchIdxAtomic); err != nil {
				return err
			}
			atomic.AddInt64(&sp.batchIdxAtomic, 1)
		}
		if rows < 1 {
			break
		}

		if err := conv.SendBatch(ctx); err != nil {
			return err
		}

		if done {
			break
		}
	}
	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (sp *bulkRowWriter) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	sp.InternalClose()
}

// InitProcessorProcedure init processor in procedure
func (sp *bulkRowWriter) InitProcessorProcedure(txn *kv.Txn) {
	if sp.EvalCtx.IsProcedure {
		if sp.FlowCtx != nil {
			sp.FlowCtx.Txn = txn
		}
		sp.Closed = false
		sp.State = execinfra.StateRunning
		sp.Out.SetRowIdx(0)
	}
}
