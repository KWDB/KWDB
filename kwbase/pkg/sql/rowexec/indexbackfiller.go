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

package rowexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/backfill"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	backfill.IndexBackfiller

	adder storagebase.BulkAdder

	desc *sqlbase.ImmutableTableDescriptor
}

func (ib *indexBackfiller) Push(ctx context.Context, res []byte) error {
	return nil
}

func (ib *indexBackfiller) IsShortCircuitForPgEncode() bool {
	return false
}

func (ib *indexBackfiller) NextPgWire() (val []byte, code int, err error) {
	return nil, 0, nil
}

func (ib *indexBackfiller) SupportPgWire() bool {
	return false
}

func (ib *indexBackfiller) Start(ctx context.Context) context.Context {
	return nil
}

func (ib *indexBackfiller) RunTS(ctx context.Context) {
	//TODO umimplement me
	panic("implement me")
}

var _ execinfra.Processor = &indexBackfiller{}
var _ chunkBackfiller = &indexBackfiller{}

var backfillerBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_size", "the initial size of the BulkAdder buffer handling index backfills", 32<<20,
)

var backfillerMaxBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_buffer_size", "the maximum size of the BulkAdder buffer handling index backfills", 512<<20,
)

var backfillerBufferIncrementSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_increment", "the size by which the BulkAdder attempts to grow its buffer before flushing", 32<<20,
)

var backillerSSTSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_sst_size", "target size for ingested files during backfills", 16<<20,
)

func newIndexBackfiller(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*indexBackfiller, error) {
	ib := &indexBackfiller{
		desc: sqlbase.NewImmutableTableDescriptor(spec.Table),
		backfiller: backfiller{
			name:        "Index",
			filter:      backfill.IndexMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	ib.backfiller.chunks = ib

	if err := ib.IndexBackfiller.Init(ib.desc); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) prepare(ctx context.Context) error {
	minBufferSize := backfillerBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	maxBufferSize := func() int64 { return backfillerMaxBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	sstSize := func() int64 { return backillerSSTSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	stepSize := backfillerBufferIncrementSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	opts := storagebase.BulkAdderOptions{
		SSTSize:            sstSize,
		MinBufferSize:      minBufferSize,
		MaxBufferSize:      maxBufferSize,
		StepBufferSize:     stepSize,
		SkipDuplicates:     ib.ContainsInvertedIndex(),
		ShouldLogLogicalOp: true,
	}
	if ib.desc.IsReplTable {
		opts.ShouldLogLogicalOp = true
	}
	adder, err := ib.flowCtx.Cfg.BulkAdder(ctx, ib.flowCtx.Cfg.DB, ib.spec.ReadAsOf, opts)
	if err != nil {
		return err
	}
	ib.adder = adder
	return nil
}

func (ib indexBackfiller) close(ctx context.Context) {
	ib.adder.Close(ctx)
}

func (ib *indexBackfiller) flush(ctx context.Context) error {
	return ib.wrapDupError(ctx, ib.adder.Flush(ctx))
}

func (ib *indexBackfiller) CurrentBufferFill() float32 {
	return ib.adder.CurrentBufferFill()
}

func (ib *indexBackfiller) wrapDupError(ctx context.Context, orig error) error {
	if orig == nil {
		return nil
	}
	typed, ok := orig.(storagebase.DuplicateKeyError)
	if !ok {
		return orig
	}

	desc, err := ib.desc.MakeFirstMutationPublic(sqlbase.IncludeConstraints)
	immutable := sqlbase.NewImmutableTableDescriptor(*desc.TableDesc())
	if err != nil {
		return err
	}
	v := &roachpb.Value{RawBytes: typed.Value}
	return row.NewUniquenessConstraintViolationError(ctx, immutable, typed.Key, v)
}

func (ib *indexBackfiller) runChunk(
	tctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.RunBeforeBackfillChunk != nil {
		if err := knobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, err
		}
	}
	if knobs.RunAfterBackfillChunk != nil {
		defer knobs.RunAfterBackfillChunk()
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "chunk", int32(ib.flowCtx.NodeID))
	defer tracing.FinishSpan(traceSpan)

	var key roachpb.Key

	start := timeutil.Now()
	var entries []sqlbase.IndexEntry
	if err := ib.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		entries, key, err = ib.BuildIndexEntriesChunk(ctx, txn, ib.desc, sp, chunkSize, false /*traceKV*/)
		return err
	}); err != nil {
		return nil, err
	}
	prepTime := timeutil.Now().Sub(start)

	start = timeutil.Now()
	for _, i := range entries {
		if err := ib.adder.Add(ctx, i.Key, i.Value.RawBytes); err != nil {
			return nil, ib.wrapDupError(ctx, err)
		}
	}
	if knobs.RunAfterBackfillChunk != nil {
		if err := ib.adder.Flush(ctx); err != nil {
			return nil, ib.wrapDupError(ctx, err)
		}
	}
	addTime := timeutil.Now().Sub(start)

	if log.V(3) {
		log.Infof(ctx, "index backfill stats: entries %d, prepare %+v, add-sst %+v",
			len(entries), prepTime, addTime)
	}
	return key, nil
}
