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

package rowexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/backfill"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	backfill.ColumnBackfiller

	desc        *sqlbase.ImmutableTableDescriptor
	otherTables []*sqlbase.ImmutableTableDescriptor
}

func (cb *columnBackfiller) Push(ctx context.Context, res []byte) error {
	return nil
}

func (cb *columnBackfiller) NextPgWire() (val []byte, code int, err error) {
	return nil, 0, nil
}

func (cb *columnBackfiller) SupportPgWire() bool {
	return false
}

func (cb *columnBackfiller) IsShortCircuitForPgEncode() bool {
	return false
}

func (cb *columnBackfiller) Start(ctx context.Context) context.Context {
	return nil
}

func (cb *columnBackfiller) RunTS(ctx context.Context) {
	//TODO unimplemented
	panic("unimplemented")
}

var _ execinfra.Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

func newColumnBackfiller(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*columnBackfiller, error) {
	otherTables := make([]*sqlbase.ImmutableTableDescriptor, len(spec.OtherTables))
	for i, tbl := range spec.OtherTables {
		otherTables[i] = sqlbase.NewImmutableTableDescriptor(tbl)
	}
	cb := &columnBackfiller{
		desc:        sqlbase.NewImmutableTableDescriptor(spec.Table),
		otherTables: otherTables,
		backfiller: backfiller{
			name:        "Column",
			filter:      backfill.ColumnMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	cb.backfiller.chunks = cb

	if err := cb.ColumnBackfiller.Init(cb.flowCtx.NewEvalCtx(), cb.desc); err != nil {
		return nil, err
	}

	return cb, nil
}

func (cb *columnBackfiller) close(ctx context.Context) {}
func (cb *columnBackfiller) prepare(ctx context.Context) error {
	return nil
}
func (cb *columnBackfiller) flush(ctx context.Context) error {
	return nil
}
func (cb *columnBackfiller) CurrentBufferFill() float32 {
	return 0
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	var key roachpb.Key
	err := cb.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk()
		}

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		key, err = cb.RunColumnBackfillChunk(
			ctx,
			txn,
			cb.desc,
			cb.otherTables,
			sp,
			chunkSize,
			true,  /*alsoCommit*/
			false, /*traceKV*/
		)
		return err
	})
	return key, err
}
