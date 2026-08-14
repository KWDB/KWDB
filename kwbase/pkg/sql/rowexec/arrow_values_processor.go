// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND.

package rowexec

// arrowValuesProcessor is the Arrow counterpart of the classic valuesProcessor.
//
// Values is a pure pre-canned constant-row source: the planner inlines the rows
// as encoded bytes (ValuesCoreSpec.RawBytes) plus the column typing
// (ValuesCoreSpec.Columns). The relational valuesProcessor decodes those bytes
// row-at-a-time via StreamDecoder and pushes datum rows through PostProcess;
// arrowValuesProcessor instead amortizes the whole batch into a single Arrow
// Record at construction time and emits it operator-to-operator through
// ArrowOutput, exactly like every other Arrow source in the unified DAG.
//
// Because Values is a data source with no inputs and no compute, it has NO KV or
// engine dependency (unlike TS/relational scans). arrowValuesProcessor embeds
// *valuesProcessor to inherit the full RowSource/Processor interface surface and
// the StreamDecoder-based decoding, overriding only ArrowOutput (to hand back the
// pre-built Record) and Next (to emit no datum rows, since the operator is only
// ever consumed as an Arrow upstream).

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/pkg/errors"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

type arrowValuesProcessor struct {
	*valuesProcessor

	alloc      memory.Allocator
	da         *sqlbase.DatumAlloc
	outputRec  arrow.Record
	emitted    bool
	inputRows  sqlbase.EncDatumRows
	recordBuilt bool
}

var _ execinfra.RowSource = &arrowValuesProcessor{}
var _ ArrowRecordEmitter = &arrowValuesProcessor{}

// newArrowValuesProcessor constructs an arrowValuesProcessor from a
// ValuesCoreSpec. The entire constant batch is decoded once into EncDatumRows
// and converted into a single Arrow Record via buildArrowColumns.
func newArrowValuesProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ValuesCoreSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*arrowValuesProcessor, error) {
	av := &arrowValuesProcessor{
		alloc: memory.NewGoAllocator(),
		da:    &sqlbase.DatumAlloc{},
	}

	// Decode the pre-canned rows using the same StreamDecoder framing the
	// planner emits and the relational valuesProcessor consumes. Each element of
	// spec.RawBytes is one encoded EncDatumRow.
	rows := make(sqlbase.EncDatumRows, 0, spec.NumRows)
	sd := &flowinfra.StreamDecoder{}
	// Bogus header (typing + header) required before any data, per StreamDecoder.
	hdr := &execinfrapb.ProducerMessage{
		Typing: spec.Columns,
		Header: &execinfrapb.ProducerHeader{},
	}
	if err := sd.AddMessage(context.TODO(), hdr); err != nil {
		return nil, err
	}
	rowBuf := make(sqlbase.EncDatumRow, len(spec.Columns))
	for _, rb := range spec.RawBytes {
		dataMsg := &execinfrapb.ProducerMessage{}
		if len(spec.Columns) == 0 {
			// Zero-column case: the row count rides on NumEmptyRows instead of
			// RawBytes. Handled below after the loop.
			continue
		}
		dataMsg.Data.RawBytes = rb
		if err := sd.AddMessage(context.TODO(), dataMsg); err != nil {
			return nil, err
		}
		row, meta, err := sd.GetRow(rowBuf)
		if err != nil {
			return nil, err
		}
		if meta != nil && meta.Err != nil {
			return nil, meta.Err
		}
		if row == nil {
			continue
		}
		// StreamDecoder.GetRow reuses rowBuf's backing array across calls, so
		// every returned `row` aliases the same slice. Copy it so each stored
		// EncDatumRow owns its own backing array (otherwise all rows collapse
		// onto the last decoded value).
		rowCopy := make(sqlbase.EncDatumRow, len(row))
		copy(rowCopy, row)
		rows = append(rows, rowCopy)
	}
	if len(spec.Columns) == 0 {
		// Zero-column source: numRows encoded via NumEmptyRows on a single
		// message rather than RawBytes rows.
		emptyMsg := &execinfrapb.ProducerMessage{Data: execinfrapb.ProducerData{NumEmptyRows: int32(spec.NumRows)}}
		if err := sd.AddMessage(context.TODO(), emptyMsg); err != nil {
			return nil, err
		}
		for i := 0; i < int(spec.NumRows); i++ {
			rows = append(rows, sqlbase.EncDatumRow{})
		}
	}
	if len(rows) != int(spec.NumRows) {
		return nil, errors.Errorf(
			"arrow values: decoded %d rows, expected %d", len(rows), spec.NumRows)
	}

	// Apply the planner's post-process (filter + projection) to the constant
	// rows before materializing them into the Arrow Record. We reuse the embedded
	// classic valuesProcessor's ProcOutputHelper (initialized with `post`) to
	// apply it row-wise: rows dropped by a filter return ok==false and are
	// skipped; projected columns are selected. This is correct for any post,
	// including CASE/COALESCE filters that the Arrow filter kernel does not
	// natively support, because the row-wise post-process uses the full classic
	// expression evaluator.
	vp, err := newValuesProcessor(flowCtx, processorID, spec, post, output)
	if err != nil {
		return nil, err
	}
	av.valuesProcessor = vp
	// The constant rows are decoded above (rows). The planner post (filter +
	// projection) and the Arrow Record materialization are deferred to
	// ArrowOutput(): applying post requires the processor's context (PbCtx),
	// which is only available after Start(). This mirrors the classic
	// valuesProcessor.Next() path (which calls ProcessRowHelper after Start)
	// and keeps a single StartInternal (double-Starting would feed the embedded
	// StreamDecoder a second producer header, faulting the gRPC receiver with
	// "received multiple headers" during server-start migrations).
	av.inputRows = rows
	return av, nil
}

// buildArrowRecord applies the planner post to the pre-decoded constant rows
// and amortizes the result into a single Arrow Record. It is called lazily from
// ArrowOutput() once the processor has been Started (so PbCtx is available for
// the post-process expression evaluation).
func (av *arrowValuesProcessor) buildArrowRecord() error {
	if av.recordBuilt {
		return nil
	}
	processed := make([]sqlbase.EncDatumRow, 0, len(av.inputRows))
	for _, row := range av.inputRows {
		// ProcessRowHelper uses the processor's own context (PbCtx); this matches
		// the classic valuesProcessor.Next() and is required for correct
		// evaluation of CASE/COALESCE filters. A nil outRow means the row was
		// dropped by the post (filter or limit); skip it just like Next() does.
		or, ok, perr := av.Out.ProcessRow(av.PbCtx(), row)
		if perr != nil {
			return perr
		}
		if ok && or != nil {
			processed = append(processed, or)
		}
	}

	// The post-processed rows are expressed in terms of av.Out.OutputTypes
	// (the planner's post output typing), not the raw spec.Columns typing. Use
	// them both for column materialization and for the Record schema. Field
	// names follow the "col%d" convention used by every other Arrow source in
	// the unified DAG (arrow_adapter/arrow_bridge/arrow_join/...), which is the
	// name space the downstream Arrow operators resolve projections against.
	// The post-processed rows are expressed in terms of av.Out.OutputTypes
	// (the planner's post output typing), not the raw spec.Columns typing. Use
	// them both for column materialization and for the Record schema. Field
	// names follow the "col%d" convention used by every other Arrow source in
	// the unified DAG (arrow_adapter/arrow_bridge/arrow_join/...), which is the
	// name space the downstream Arrow operators resolve projections against.
	outTyps := av.Out.OutputTypes
	ptrTyps := make([]*types.T, len(outTyps))
	for i := range outTyps {
		ptrTyps[i] = &outTyps[i]
	}
	cols, err := buildArrowColumns(av.alloc, ptrTyps, processed, av.da)
	if err != nil {
		return err
	}
	fields := make([]arrow.Field, len(ptrTyps))
	for i := range ptrTyps {
		fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: cols[i].DataType(), Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	av.outputRec = array.NewRecord(schema, cols, int64(len(processed)))
	av.recordBuilt = true
	return nil
}

// Start initializes the processor. It performs the base processor
// initialization (StartInternal) and then materializes the Arrow Record from
// the pre-decoded constant rows. The Record must be built here — after
// StartInternal has set EvalCtx.Context (required for correct CASE/COALESCE
// filter evaluation) — rather than in the constructor (where EvalCtx.Context
// is still nil) or lazily in ArrowOutput (which may be pulled before Start by
// the unified DAG). It deliberately does NOT call the embedded
// valuesProcessor.Start(), which would feed the inherited StreamDecoder a
// producer header; calling it here too would feed the *same* embedded
// StreamDecoder a second header, which the downstream gRPC receiver rejects as
// "received multiple headers" during server-start migrations.
func (av *arrowValuesProcessor) Start(ctx context.Context) context.Context {
	ctx = av.StartInternal(ctx, "arrow-values")
	if err := av.buildArrowRecord(); err != nil {
		av.MoveToDraining(err)
	}
	return ctx
}

// ArrowOutput returns the single pre-built Record. It is delivered once; on
// subsequent calls nil signals end-of-stream to the unified DAG.
func (av *arrowValuesProcessor) ArrowOutput() arrow.Record {
	if av.emitted {
		return nil
	}
	av.emitted = true
	if av.outputRec == nil {
		return nil
	}
	av.outputRec.Retain()
	return av.outputRec
}

// Next never emits datum rows — arrowValuesProcessor is consumed exclusively as
// an Arrow upstream via ArrowOutput. Returning (nil, nil) signals an empty row
// stream, which is semantically harmless because no downstream reads it as a
// RowSource.
func (av *arrowValuesProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, nil
}

// ConsumerClosed releases the Arrow Record buffers back to the allocator.
func (av *arrowValuesProcessor) ConsumerClosed() {
	log.VEventf(av.Ctx, 1, "arrow values processor consumed and closed")
	if av.outputRec != nil {
		av.outputRec.Release()
		av.outputRec = nil
	}
}
