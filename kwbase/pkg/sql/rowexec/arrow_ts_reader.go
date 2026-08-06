// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND.

package rowexec

import (
	"context"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowTsReader is a prototype Arrow data source for time-series (TS) scans.
//
// Design (see docs/arrow-unify-roadmap.md §6.7 / §6.8 审订): TS scan is NOT
// semantically coupled to the relational layer — the TS engine (tse C++ engine)
// only hands its data back in a crdb relational-format buffer (coldata.Vec /
// EncDatumRow). The colexec TsReaderOp is merely an adapter that wraps the tse
// FFI (SetupTsFlow / NextVectorizedTsFlow / CloseTsFlow) and copies the pulled
// coldata.Vec into a coldata.Batch. arrowTsReader reuses the identical tse FFI
// (via the embedded *TsTableReader) but emits an Arrow Record instead, proving
// that the TS read can be an Arrow data source without touching the tse engine.
//
// The tse FFI is a lower-level public read interface, NOT bound to colexec — it
// is shared by both TsTableReader (rowexec, row format) and TsReaderOp (colexec,
// vectorized format). arrowTsReader embeds *TsTableReader to reuse its mature
// SetupTsFlow / cancelTsFlow / cleanup plumbing, and overrides Next to pull the
// same EncDatumRows the row-format reader produces, then amortizes them into a
// single Arrow Record via buildArrowColumns (rowexec's internal facility, so no
// new import dependency on colexec is introduced).
//
// It implements both execinfra.RowSource (inherited from TsTableReader, so it
// can be received by unifiedInputFrom) and ArrowRecordEmitter (so the unified
// DAG consumes its Record operator-to-operator without a row round-trip).

type arrowTsReader struct {
	*TsTableReader

	// arrowMode selects how the reader is consumed.
	//   false: legacy row-push model. Next() delegates to the embedded
	//          TsTableReader and pushes rows into the RowReceiver, exactly like
	//          the relational TsTableReader. ArrowOutput() is disabled (returns
	//          nil) because the downstream receives the RowReceiver, not this
	//          struct, so the Arrow Record can never be picked up.
	//   true:  Arrow fast path. Next() produces no rows; the unified DAG pulls
	//          the accumulated Arrow Record via ArrowOutput(). Used when this
	//          reader is wired directly as an Arrow operator's upstream (future
	//          wiring; the current rowflow QUEUE path always uses arrowMode=false).
	arrowMode bool

	alloc    memory.Allocator
	outTyps  []*types.T
	da       *sqlbase.DatumAlloc
	outputRec arrow.Record
	emitted  bool
	done     bool
}

var _ execinfra.RowSource = &arrowTsReader{}
var _ ArrowRecordEmitter = &arrowTsReader{}

// NewArrowTsReader creates an arrowTsReader. It mirrors NewTsTableReader's FFI
// setup, additionally recording the memory allocator and output types used to
// build Arrow Records.
func NewArrowTsReader(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	typs []types.T,
	output execinfra.RowReceiver,
	sid execinfrapb.StreamID,
	tsProcessorSpecs []execinfrapb.ProcessorSpec,
	tsInfo execinfrapb.TsInfo,
	alloc memory.Allocator,
	arrowMode bool,
) (*arrowTsReader, error) {
	// In arrowMode the rows produced by the embedded reader must not be pushed
	// into a RowChannel (the downstream reads the Arrow Record via ArrowOutput
	// instead and would never drain the channel). Route them to a discardReceiver.
	ttrOutput := output
	if arrowMode {
		ttrOutput = discardReceiver{}
	}
	ttr, err := NewTsTableReader(ctx, flowCtx, processorID, typs, ttrOutput, sid, tsProcessorSpecs, tsInfo)
	if err != nil {
		return nil, err
	}
	outTyps := make([]*types.T, len(typs))
	for i := range typs {
		t := typs[i]
		outTyps[i] = &t
	}
	return &arrowTsReader{
		TsTableReader: ttr,
		arrowMode:     arrowMode,
		alloc:         alloc,
		outTyps:       outTyps,
		da:            &sqlbase.DatumAlloc{},
	}, nil
}

// discardReceiver is a RowReceiver that swallows every row pushed into it. It is
// used as the embedded TsTableReader's RowReceiver when arrowTsReader runs in
// arrowMode: the TS engine still produces rows through TsTableReader.Next(), but
// instead of being pushed into a RowChannel (which the downstream would never
// drain — the downstream reads the Arrow Record via ArrowOutput), they are
// discarded here. The rows are still returned by TsTableReader.Next() and picked
// up by pullRecord (Arrow path) or by the downstream's Next() (row path).
type discardReceiver struct{}

func (discardReceiver) Push(sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) execinfra.ConsumerStatus {
	return execinfra.NeedMoreRows
}
func (discardReceiver) PushPGResult(context.Context, []byte) error { return nil }
func (discardReceiver) AddPGComplete(string, tree.StatementType, int) {}
func (discardReceiver) Types() []types.T                             { return nil }
func (discardReceiver) ProducerDone()                               {}
func (discardReceiver) GetCols() int                                { return 0 }
func (discardReceiver) AddStats(time.Duration, bool)                {}
func (discardReceiver) GetStats() execinfra.RowStats                { return execinfra.RowStats{} }

// Next implements the RowSource interface.
//
//   - arrowMode == false (legacy row-push model): delegate to the embedded
//     TsTableReader so rows flow into the RowReceiver exactly as the relational
//     reader does. ArrowOutput is a no-op. This is what the current rowflow
//     QUEUE path relies on.
//   - arrowMode == true (Arrow fast path): the downstream consumes the reader in
//     one of two mutually-exclusive ways, never both:
//       * an Arrow operator calls unifiedInputFrom -> ArrowOutput(), which drains
//         the whole stream into a single Arrow Record (via pullRecord). Next() is
//         then never called.
//       * a legacy row operator calls Next() directly; we delegate to the embedded
//         reader so rows flow out normally (they were routed to the discardReceiver
//         at construction, so no double delivery happens). ArrowOutput() is then
//         never called.
//     In either case only one consumer path touches the embedded reader, so there
//     is no race for the TS engine's rows.
func (atr *arrowTsReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if !atr.arrowMode {
		return atr.TsTableReader.Next()
	}
	// Arrow fast path, row consumer: behave exactly like the relational reader.
	return atr.TsTableReader.Next()
}

// RunTS is part of the Processor interface. In arrowMode the Arrow Record is
// produced lazily by ArrowOutput() (driven by the downstream Arrow operator's
// unifiedInputFrom), so the generic Run loop must NOT also drain the reader —
// otherwise two goroutines would consume the TS engine concurrently. When an
// arrowTsReader is wired as an Arrow emitter it is intentionally not added to
// f.TsTableReaders (the flow never calls RunTS on it); this override is purely
// defensive against any future code path that does.
func (atr *arrowTsReader) RunTS(ctx context.Context) {
	if !atr.arrowMode {
		atr.TsTableReader.RunTS(ctx)
	}
}

// ConsumerClosed is part of the RowSource interface. In arrowMode the TS engine
// handle is released here as well (idempotent with pullRecord's DropHandle) so a
// row-mode downstream that closes the reader still releases tse resources.
func (atr *arrowTsReader) ConsumerClosed() {
	if atr.arrowMode {
		atr.DropHandle(atr.Ctx)
		return
	}
	atr.TsTableReader.ConsumerClosed()
}

// pullRecord drains the embedded TsTableReader into a single Arrow Record stored
// in outputRec. It is only called from ArrowOutput() (arrowMode), which is called
// exactly once by unifiedInputFrom, so it drains the entire stream. On exhaustion
// it releases the TS engine handle (DropHandle is idempotent).
func (atr *arrowTsReader) pullRecord() (bool, error) {
	if atr.done {
		return false, nil
	}
	if atr.outputRec != nil && !atr.emitted {
		return true, nil
	}
	var rows sqlbase.EncDatumRows
	for {
		row, meta := atr.TsTableReader.Next()
		if meta != nil {
			if meta.Err != nil {
				atr.DropHandle(atr.Ctx)
				return false, meta.Err
			}
			continue
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		atr.done = true
		atr.DropHandle(atr.Ctx)
		return false, nil
	}
	cols, err := buildArrowColumns(atr.alloc, atr.outTyps, rows, atr.da)
	if err != nil {
		return false, err
	}
	fields := make([]arrow.Field, len(atr.outTyps))
	for i, t := range atr.outTyps {
		dt, err := arrowDataTypeForKWType(t)
		if err != nil {
			return false, err
		}
		fields[i] = arrow.Field{Name: t.Name(), Type: dt}
	}
	atr.outputRec = array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(len(rows)))
	atr.emitted = false
	atr.done = true
	atr.DropHandle(atr.Ctx)
	return true, nil
}

// ArrowOutput implements ArrowRecordEmitter. unifiedInputFrom calls it before
// touching Next(), so we lazily pull the first Record here if it has not been
// built yet. Ownership transfers to the caller (Retain applied here; caller must
// Release). Returns nil after the last Record has been consumed.
func (atr *arrowTsReader) ArrowOutput() arrow.Record {
	if !atr.arrowMode {
		return nil
	}
	if atr.outputRec != nil && atr.emitted {
		return nil
	}
	if atr.outputRec == nil && !atr.done {
		if _, err := atr.pullRecord(); err != nil {
			atr.MoveToDraining(err)
			return nil
		}
	}
	if atr.outputRec == nil || atr.emitted {
		return nil
	}
	atr.outputRec.Retain()
	atr.emitted = true
	return atr.outputRec
}


