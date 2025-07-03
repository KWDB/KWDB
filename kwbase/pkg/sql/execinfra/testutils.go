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

package execinfra

import (
	"context"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

// StaticNodeID is the default Node ID to be used in tests.
const StaticNodeID = roachpb.NodeID(3)

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []types.T
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(types []types.T, rows sqlbase.EncDatumRows) *RepeatableRowSource {
	if types == nil {
		panic("types required")
	}
	return &RepeatableRowSource{rows: rows, types: types}
}

// OutputTypes is part of the RowSource interface.
func (r *RepeatableRowSource) OutputTypes() []types.T {
	return r.types
}

// Start is part of the RowSource interface.
func (r *RepeatableRowSource) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end.
	if r.nextRowIdx >= len(r.rows) {
		return nil, nil
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, nil
}

// Reset resets the RepeatableRowSource such that a subsequent call to Next()
// returns the first row.
func (r *RepeatableRowSource) Reset() {
	r.nextRowIdx = 0
}

// ConsumerDone is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerClosed() {}

// InitProcessorProcedure init processor in procedure
func (r *RepeatableRowSource) InitProcessorProcedure(txn *kv.Txn) {}

// NewTestMemMonitor creates and starts a new memory monitor to be used in
// tests.
// TODO(yuzefovich): consider reusing this in tree.MakeTestingEvalContext
// (currently it would create an import cycle, so this code will need to be
// moved).
func NewTestMemMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	memMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	memMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	return &memMonitor
}

// NewTestDiskMonitor creates and starts a new disk monitor to be used in
// tests.
func NewTestDiskMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	return &diskMonitor
}

// GenerateValuesSpec generates a ValuesCoreSpec that encodes the given rows.
// We pass the types as well because zero rows are allowed.
func GenerateValuesSpec(
	colTypes []types.T, rows sqlbase.EncDatumRows, rowsPerChunk int,
) (execinfrapb.ValuesCoreSpec, error) {
	var spec execinfrapb.ValuesCoreSpec
	spec.Columns = make([]execinfrapb.DatumInfo, len(colTypes))
	for i := range spec.Columns {
		spec.Columns[i].Type = colTypes[i]
		spec.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
	}

	var a sqlbase.DatumAlloc
	for i := 0; i < len(rows); {
		var buf []byte
		for end := i + rowsPerChunk; i < len(rows) && i < end; i++ {
			for j, info := range spec.Columns {
				var err error
				buf, err = rows[i][j].Encode(&colTypes[j], &a, info.Encoding, buf)
				if err != nil {
					return execinfrapb.ValuesCoreSpec{}, err
				}
			}
		}
		spec.RawBytes = append(spec.RawBytes, buf)
	}
	return spec, nil
}

// RowDisposer is a RowReceiver that discards any rows Push()ed.
type RowDisposer struct{}

// GetCols is part of the distsql.RowReceiver interface.
func (r *RowDisposer) GetCols() int {
	//TODO "unimplemented"
	panic("unimplemented")
}

// AddStats record stallTime and number of rows
func (r *RowDisposer) AddStats(time time.Duration, isAddRows bool) {}

// GetStats get RowStats
func (r *RowDisposer) GetStats() RowStats {
	return RowStats{}
}

var _ RowReceiver = &RowDisposer{}

// Push is part of the distsql.RowReceiver interface.
func (r *RowDisposer) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) ConsumerStatus {
	return NeedMoreRows
}

// PushPGResult is part of the RowReceiver interface.
func (r *RowDisposer) PushPGResult(ctx context.Context, res []byte) error {
	return nil
}

// AddPGComplete is part of the RowReceiver interface.
func (r *RowDisposer) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

// ProducerDone is part of the RowReceiver interface.
func (r *RowDisposer) ProducerDone() {}

// Types is part of the RowReceiver interface.
func (r *RowDisposer) Types() []types.T {
	return nil
}
