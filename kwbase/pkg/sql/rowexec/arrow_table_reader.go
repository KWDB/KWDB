// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND.

package rowexec

// arrowTableReader is a stage-1 Arrow data source wrapper around the relational
// tableReader. It does NOT re-implement KV decoding; it reuses the embedded
// *tableReader (which decodes KV into EncDatumRow via row.Fetcher) and, when
// Arrow is enabled for this scan, exposes the decoded rows as an Arrow Record
// via ArrowOutput(). Downstream Arrow operators consume it through
// unifiedInputFrom -> NewArrowRecordSource, skipping the secondary
// NewRowSourceToArrow re-batch that the non-Arrow path would otherwise incur.
//
// Design notes (see docs/arrow-unify-roadmap.md §7.6 / next step):
//   - This is the minimal, low-risk "ArrowTableReader": the tableReader stays a
//     row source; we only add an ArrowRecordEmitter surface on top of it.
//   - The flow schedules processors in two phases: all startables' Start()
//     (during which the downstream Arrow operator calls ArrowOutput() and drains
//     every row), THEN all processors' Run(). In arrowMode, Run() is therefore a
//     no-op — the data has already been pulled by ArrowOutput(). This mirrors
//     arrowTsReader's arrowMode safety model and avoids double delivery.
//   - Stage 2 (true direct KV->Arrow decode inside row.Fetcher) is deferred until
//     the mon.Allocator -> arrow memory.Allocator bridge (roadmap stage5) exists,
//     so Arrow builder memory is accounted under MemoryMonitor.
import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// arrowTableReaderRuns counts how many ArrowTableReader sources have emitted an
// Arrow Record (i.e. took the stage-1 direct Arrow scan path). Used by tests to
// confirm a relational scan was served by ArrowTableReader rather than the
// NewRowSourceToArrow bridge.
var arrowTableReaderRuns int64

// ArrowTableReaderRunCount returns the number of ArrowTableReader sources that
// have produced an Arrow Record.
func ArrowTableReaderRunCount() int64 {
	return atomic.LoadInt64(&arrowTableReaderRuns)
}

// arrowTableReader embeds *tableReader and adds an ArrowRecordEmitter surface.
type arrowTableReader struct {
	*tableReader

	// arrowMode, when true, means this reader emits an Arrow Record via
	// ArrowOutput() instead of pushing rows through the RowReceiver in Run().
	arrowMode bool

	// arrowAlloc is the Arrow memory allocator used to build the Record. It is
	// kept separate from tableReader.alloc (which is a DatumAlloc) to avoid the
	// ambiguous-selector collision from embedding.
	arrowAlloc memory.Allocator

	// outTyps is the post-processed output column types, used to build the
	// Arrow Schema. Derived from tableReader.Out.OutputTypes at construction.
	outTyps []*types.T

	// arrowRec is the single Arrow Record produced lazily by ArrowOutput().
	arrowRec arrow.Record

	// arrowEmitted is true once ArrowOutput() has handed the Record to a caller.
	arrowEmitted bool

	// started guards the lazy fetcher start. The flow schedules startables'
	// Start() (which drives the downstream Arrow operator's ArrowOutput()) BEFORE
	// this reader's Run(), so the embedded tableReader.Start (which calls
	// fetcher.StartScan) has not run yet when ArrowOutput() is first invoked.
	// ArrowOutput() therefore lazily starts the fetcher on first call; started
	// prevents a second StartScan when Run() later executes.
	started bool
}

var _ execinfra.Processor = &arrowTableReader{}
var _ execinfra.RowSource = &arrowTableReader{}
var _ ArrowRecordEmitter = &arrowTableReader{}

// newArrowTableReader wraps a freshly constructed *tableReader in an
// arrowTableReader operating in arrowMode. The wrapped tableReader is not
// pre-Started; ArrowOutput() lazily starts its fetcher on first pull.
func newArrowTableReader(tr *tableReader) *arrowTableReader {
	outTyps := tr.Out.OutputTypes
	ptrTyps := make([]*types.T, len(outTyps))
	for i := range outTyps {
		t := outTyps[i]
		ptrTyps[i] = &t
	}
	return &arrowTableReader{
		tableReader: tr,
		arrowMode:   true,
		arrowAlloc:  memory.NewGoAllocator(),
		outTyps:     ptrTyps,
	}
}

// startFetcher lazily starts the embedded tableReader's KV fetcher. It must run
// before the first Next() so ArrowOutput() can drain rows during the downstream
// operator's Start phase (which precedes this reader's Run()).
func (atr *arrowTableReader) startFetcher() {
	if atr.started {
		return
	}
	ctx := atr.tableReader.FlowCtx.Cfg.AmbientContext.AnnotateCtx(context.Background())
	atr.tableReader.Start(ctx)
	atr.started = true
}

// ArrowOutput implements ArrowRecordEmitter. unifiedInputFrom calls it (from the
// downstream Arrow operator's Start) before Run() is ever invoked, so we lazily
// drain every decoded row here and build a single Arrow Record. Ownership
// transfers to the caller (Retain applied); returns nil after the Record has
// been consumed once.
func (atr *arrowTableReader) ArrowOutput() arrow.Record {
	if !atr.arrowMode {
		return nil
	}
	if atr.arrowEmitted {
		return nil
	}
	atr.startFetcher()
	if atr.arrowRec == nil {
		rows := make([]sqlbase.EncDatumRow, 0, 1024)
		for {
			row, meta := atr.tableReader.Next()
			if meta != nil {
				if meta.Err != nil {
					atr.MoveToDraining(meta.Err)
					return nil
				}
				// Non-error metadata (e.g. tracing) is ignored for the Arrow
				// path; the row loop continues.
				continue
			}
			if row == nil {
				break
			}
			rowCopy := make(sqlbase.EncDatumRow, len(row))
			copy(rowCopy, row)
			rows = append(rows, rowCopy)
		}
		cols, err := buildArrowColumns(atr.arrowAlloc, atr.outTyps, rows, &atr.tableReader.alloc)
		if err != nil {
			atr.MoveToDraining(err)
			return nil
		}
		fields := make([]arrow.Field, len(atr.outTyps))
		for i, t := range atr.outTyps {
			dt, derr := arrowDataTypeForKWType(t)
			if derr != nil {
				atr.MoveToDraining(derr)
				return nil
			}
			fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: dt}
		}
		atr.arrowRec = array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(len(rows)))
		atomic.AddInt64(&arrowTableReaderRuns, 1)
	}
	atr.arrowRec.Retain()
	atr.arrowEmitted = true
	return atr.arrowRec
}

// Run implements execinfra.Processor. In arrowMode the rows have already been
// drained by ArrowOutput() during the downstream Start phase, so Run is a
// no-op (returns zero stats). Otherwise it delegates to the embedded
// tableReader's normal push path.
func (atr *arrowTableReader) Run(ctx context.Context) execinfra.RowStats {
	if atr.arrowMode {
		return execinfra.RowStats{}
	}
	return atr.tableReader.Run(ctx)
}
