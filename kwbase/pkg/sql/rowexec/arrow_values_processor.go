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

	alloc     memory.Allocator
	outTyps   []*types.T
	da        *sqlbase.DatumAlloc
	outputRec arrow.Record
	emitted   bool
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

	// Resolve the output column types from the spec.
	av.outTyps = make([]*types.T, len(spec.Columns))
	for i := range spec.Columns {
		av.outTyps[i] = &spec.Columns[i].Type
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
		rows = append(rows, row)
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

	// Amortize the whole batch into a single Arrow Record.
	cols, err := buildArrowColumns(av.alloc, av.outTyps, rows, av.da)
	if err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(av.outTyps))
	for i, t := range av.outTyps {
		fields[i] = arrow.Field{Name: t.Name(), Type: cols[i].DataType(), Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	av.outputRec = array.NewRecord(schema, cols, int64(len(rows)))

	// Build the embedded classic valuesProcessor so the full RowSource/Processor
	// interface is satisfied; its Next path is overridden below so it never
	// emits datum rows (this operator is consumed only via ArrowOutput).
	vp, err := newValuesProcessor(flowCtx, processorID, spec, post, output)
	if err != nil {
		return nil, err
	}
	av.valuesProcessor = vp
	return av, nil
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
