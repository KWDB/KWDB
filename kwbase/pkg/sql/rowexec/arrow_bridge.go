// Copyright 2024 The KWDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND. Either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowexec

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// ---------------------------------------------------------------------------
// arrow_bridge — §阶段3 (colexec 经 arrow_bridge 纳入统一 DAG)
//
// The Arrow compute engine and the legacy/colexec engines speak different data
// representations: Arrow operators consume and produce arrow.Record batches via
// the UnifiedProcessor / ArrowRecordEmitter interfaces, whereas colexec
// operators consume and produce EncDatum rows via the RowSource / Processor
// interfaces.
//
// These two adapters form the bridge between the two worlds so that a colexec
// stage can sit either upstream of (as a source feeding Arrow) or downstream of
// (as a sink consuming Arrow) an Arrow compute stage inside the same flow. The
// planner already decides per-operator whether to emit an Arrow core or a
// colexec/row core (see arrow_unification.go's arrowXxxCoreFor helpers); these
// adapters are what make the resulting mixed DAG actually runnable:
//
//   - NewRowSourceToArrow: any RowSource (incl. a colexec operator's row output)
//     is wrapped as a UnifiedProcessor that incrementsally builds a single native
//     arrow.Record via per-column Arrow builders, with no intermediate EncDatum
//     buffer. This is the colexec -> Arrow direction and is what unifiedInputFrom
//     wires in whenever the upstream of an Arrow operator is not itself an
//     ArrowRecordEmitter (e.g. a colexec operator). It also implements
//     ArrowRecordEmitter so a following Arrow operator can pull the record
//     operator-to-operator without a row round-trip.
//
//   - NewArrowToRowSource: an arrow.Record is wrapped as a RowSource so the output
//     of an Arrow operator can be consumed by a colexec operator (or any other
//     row-based stage). This is the Arrow -> colexec direction.
//
// Both reuse the already-validated arrowDataTypeForKWType / newArrowBuilder /
// appendEncDatum / arrowRecordToEncDatumRows paths; they do not change the
// per-operator execution model.
// ---------------------------------------------------------------------------

// buildArrowSchema returns the Arrow schema for a set of KWDB types, reusing the
// same family->arrow mapping used by the scan bridge. Unsupported types fall back
// to a string column so the batch can still be built; the operator layer rejects
// unsupported types explicitly.
func buildArrowSchema(typs []*types.T) *arrow.Schema {
	fields := make([]arrow.Field, len(typs))
	for i, t := range typs {
		dt, err := arrowDataTypeForKWType(t)
		if err != nil {
			dt = arrow.BinaryTypes.String
		}
		fields[i] = arrow.Field{Name: t.Name(), Type: dt, Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

// rowSourceToArrowBridge reads rows from an underlying RowSource and builds a
// single unified Arrow Record incrementally: each value is appended straight
// into its per-column Arrow builder as the row arrives, so the input is never
// materialized into an intermediate buffer. This is the colexec -> Arrow bridge
// used by unifiedInputFrom. It also implements ArrowRecordEmitter so a downstream
// Arrow operator can consume the built record operator-to-operator.
type rowSourceToArrowBridge struct {
	alloc memory.Allocator
	input execinfra.RowSource
	da    *sqlbase.DatumAlloc
	typs  []*types.T
	rec   arrow.Record
	sent  bool
}

// NewRowSourceToArrow wraps a legacy RowSource (e.g. the row output of a colexec
// operator) as a UnifiedProcessor that emits a single native arrow.Record built
// directly from the streamed rows. The resulting processor also satisfies
// ArrowRecordEmitter so it can hand its record to a downstream Arrow operator
// without a row round-trip.
func NewRowSourceToArrow(
	alloc memory.Allocator, input execinfra.RowSource, da *sqlbase.DatumAlloc,
) UnifiedProcessor {
	inTypes := input.OutputTypes()
	typs := make([]*types.T, len(inTypes))
	for i := range inTypes {
		t := inTypes[i]
		typs[i] = &t
	}
	return &rowSourceToArrowBridge{alloc: alloc, input: input, da: da, typs: typs}
}

// Init implements UnifiedProcessor.
func (s *rowSourceToArrowBridge) Init(ctx context.Context) {}

// Allocator implements UnifiedProcessor.
func (s *rowSourceToArrowBridge) Allocator() memory.Allocator { return s.alloc }

// Next implements UnifiedProcessor. It drains the input once, appending each row
// directly into per-column Arrow builders, then emits a single Record.
func (s *rowSourceToArrowBridge) Next(ctx context.Context) (arrow.Record, bool, error) {
	if s.sent {
		return nil, true, nil
	}
	s.sent = true
	rec, err := s.build()
	if err != nil {
		return nil, false, err
	}
	s.rec = rec
	return rec, false, nil
}

// ArrowOutput implements ArrowRecordEmitter, exposing the built Record so a
// downstream Arrow operator can consume it operator-to-operator without a row
// round-trip. It lazily builds on first access and shares s.rec with Next.
func (s *rowSourceToArrowBridge) ArrowOutput() arrow.Record {
	if s.rec == nil {
		if rec, err := s.build(); err == nil {
			s.rec = rec
			s.sent = true
		}
	}
	return s.rec
}

// build reads every input row exactly once and appends each value into the
// matching column builder, producing a single native Arrow Record.
func (s *rowSourceToArrowBridge) build() (arrow.Record, error) {
	nIn := len(s.typs)
	builders := make([]array.Builder, nIn)
	fields := make([]arrow.Field, nIn)
	release := func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}
	for i, t := range s.typs {
		dt, err := arrowDataTypeForKWType(t)
		if err != nil {
			release()
			return nil, err
		}
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: dt, Nullable: true}
		b, err := newArrowBuilder(s.alloc, t)
		if err != nil {
			release()
			return nil, err
		}
		builders[i] = b
	}
	n := 0
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if meta.Err != nil {
				release()
				return nil, meta.Err
			}
			continue
		}
		if row == nil {
			break
		}
		for i := range row {
			if err := appendEncDatum(builders[i], s.typs[i], &row[i], s.da); err != nil {
				release()
				return nil, err
			}
		}
		n++
	}
	cols := make([]arrow.Array, nIn)
	for i := range builders {
		cols[i] = builders[i].NewArray()
		builders[i].Release()
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(n)), nil
}

// arrowToRowSource adapts an arrow.Record to the RowSource interface so that the
// output of an Arrow compute operator can be consumed by a colexec operator or
// any other row-based stage. This is the Arrow -> colexec direction of the
// bridge.
type arrowToRowSource struct {
	rec  arrow.Record
	typs []types.T
	rows sqlbase.EncDatumRows
	pos  int
}

// NewArrowToRowSource wraps an arrow.Record as a RowSource. The record is
// converted lazily into EncDatum rows on demand; ownership of rec stays with the
// caller and must be released after the source is consumed.
func NewArrowToRowSource(rec arrow.Record, typs []types.T) execinfra.RowSource {
	return &arrowToRowSource{rec: rec, typs: typs}
}

// OutputTypes implements RowSource.
func (s *arrowToRowSource) OutputTypes() []types.T { return s.typs }

// Start implements RowSource.
func (s *arrowToRowSource) Start(ctx context.Context) context.Context { return ctx }

// InitProcessorProcedure implements RowSource.
func (s *arrowToRowSource) InitProcessorProcedure(_ *kv.Txn) {}

// Next implements RowSource, returning one row at a time from the converted
// record. When the record is fully consumed, both returns are empty.
func (s *arrowToRowSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if s.rows == nil {
		if s.rec == nil {
			return nil, nil
		}
		rows, err := arrowRecordToEncDatumRows(s.typs, s.rec)
		if err != nil {
			s.rec = nil
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		s.rows = rows
	}
	if s.pos >= len(s.rows) {
		s.rec = nil
		return nil, nil
	}
	row := s.rows[s.pos]
	s.pos++
	return row, nil
}

// ConsumerDone implements RowSource.
func (s *arrowToRowSource) ConsumerDone() {}

// ConsumerClosed implements RowSource.
func (s *arrowToRowSource) ConsumerClosed() {}

// ensure interfaces are satisfied.
var (
	_ UnifiedProcessor   = (*rowSourceToArrowBridge)(nil)
	_ ArrowRecordEmitter = (*rowSourceToArrowBridge)(nil)
	_ execinfra.RowSource = (*arrowToRowSource)(nil)
)
