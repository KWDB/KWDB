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
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// indexSkipTableReader is a processor that retrieves distinct rows from
// a table using the prefix of an index to skip reading some rows in the
// table. Specifically, given a prefix of an index to distinct over,
// the indexSkipTableReader returns all distinct rows where that prefix
// of the index is distinct. It uses the index to seek to distinct values
// of the prefix instead of doing a full table scan.
type indexSkipTableReader struct {
	execinfra.ProcessorBase

	spans roachpb.Spans

	// currentSpan maintains which span we are currently scanning.
	currentSpan int

	// keyPrefixLen holds the length of the prefix of the index
	// that we are performing a distinct over.
	keyPrefixLen int
	// indexLen holds the number of columns in the index that
	// is being considered.
	indexLen int

	reverse bool

	ignoreMisplannedRanges bool
	misplannedRanges       []roachpb.RangeInfo

	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc
}

const indexSkipTableReaderProcName = "index skip table reader"

var istrPool = sync.Pool{
	New: func() interface{} {
		return &indexSkipTableReader{}
	},
}

var _ execinfra.Processor = &indexSkipTableReader{}
var _ execinfra.RowSource = &indexSkipTableReader{}
var _ execinfrapb.MetadataSource = &indexSkipTableReader{}

func newIndexSkipTableReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.IndexSkipTableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*indexSkipTableReader, error) {
	if flowCtx.NodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	t := istrPool.Get().(*indexSkipTableReader)

	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	t.ignoreMisplannedRanges = flowCtx.Local
	t.reverse = spec.Reverse

	if err := t.Init(
		t,
		post,
		types,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain:        nil,
			TrailingMetaCallback: t.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	neededColumns := t.Out.NeededColumns()
	t.keyPrefixLen = neededColumns.Len()

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)

	immutDesc := sqlbase.NewImmutableTableDescriptor(spec.Table)
	index, isSecondaryIndex, err := immutDesc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	t.indexLen = len(index.ColumnIDs)

	cols := immutDesc.Columns
	if returnMutations {
		cols = immutDesc.ReadableColumns
	}

	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        columnIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  neededColumns,
	}

	if err := t.fetcher.Init(
		t.reverse,
		spec.LockingStrength,
		true,  /* returnRangeInfo */
		false, /* isCheck */
		&t.alloc,
		tableArgs,
	); err != nil {
		return nil, err
	}

	// Make a copy of the spans for this reader, as we will modify them.
	nSpans := len(spec.Spans)
	if cap(t.spans) >= nSpans {
		t.spans = t.spans[:nSpans]
	} else {
		t.spans = make(roachpb.Spans, nSpans)
	}

	// If we are scanning in reverse, then copy the spans in backwards.
	if t.reverse {
		for i, s := range spec.Spans {
			t.spans[len(spec.Spans)-i-1] = s.Span
		}
	} else {
		for i, s := range spec.Spans {
			t.spans[i] = s.Span
		}
	}

	return t, nil
}

// InitProcessorProcedure init processor in procedure
func (t *indexSkipTableReader) InitProcessorProcedure(txn *kv.Txn) {
	if t.EvalCtx.IsProcedure {
		if t.FlowCtx != nil {
			t.FlowCtx.Txn = txn
		}
		t.Closed = false
		t.State = execinfra.StateRunning
		t.Out.SetRowIdx(0)
	}
}

func (t *indexSkipTableReader) Start(ctx context.Context) context.Context {
	t.StartInternal(ctx, indexSkipTableReaderProcName)
	return ctx
}

func (t *indexSkipTableReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for t.State == execinfra.StateRunning {
		if t.currentSpan >= len(t.spans) {
			t.MoveToDraining(nil)
			return nil, t.DrainHelper()
		}

		// Start a scan to get the smallest value within this span.
		err := t.fetcher.StartScan(
			t.PbCtx(), t.FlowCtx.Txn, t.spans[t.currentSpan:t.currentSpan+1],
			true, 1 /* batch size limit */, t.FlowCtx.TraceKV,
		)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}

		// Range info resets once a scan begins, so we need to maintain
		// the range info we get after each scan.
		if !t.ignoreMisplannedRanges {
			ranges := execinfra.MisplannedRanges(t.PbCtx(), t.fetcher.GetRangesInfo(), t.FlowCtx.NodeID)
			for _, r := range ranges {
				t.misplannedRanges = roachpb.InsertRangeInfo(t.misplannedRanges, r)
			}
		}

		// This key *must not* be modified, as this will cause the fetcher
		// to begin acting incorrectly. This is because modifications
		// will corrupt the row internal to the fetcher.
		key, err := t.fetcher.PartialKey(t.keyPrefixLen)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}

		row, _, _, err := t.fetcher.NextRow(t.PbCtx())
		if err != nil {
			t.MoveToDraining(err)
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		if row == nil {
			// No more rows in this span, so move to the next one.
			t.currentSpan++
			continue
		}

		if !t.reverse {
			// We set the new key to be the largest key with the prefix that we have
			// so that we skip all values with the same prefix, and "skip" to the
			// next distinct value.
			t.spans[t.currentSpan].Key = key.PrefixEnd()
		} else {
			// In the case of reverse, this is much easier. The reverse fetcher
			// returns the key retrieved, in this case the first key smaller
			// than EndKey in the current span. Since EndKey is exclusive, we
			// just set the retrieved key as EndKey for the next scan.
			t.spans[t.currentSpan].EndKey = key
		}

		// If the changes we made turned our current span invalid, mark that
		// we should move on to the next span before returning the row.
		if !t.spans[t.currentSpan].Valid() {
			t.currentSpan++
		}

		if outRow := t.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, t.DrainHelper()
}

func (t *indexSkipTableReader) Release() {
	t.ProcessorBase.Reset()
	t.fetcher.Reset()
	*t = indexSkipTableReader{
		ProcessorBase:    t.ProcessorBase,
		fetcher:          t.fetcher,
		spans:            t.spans[:0],
		misplannedRanges: t.misplannedRanges[:0],
		currentSpan:      0,
	}
	istrPool.Put(t)
}

func (t *indexSkipTableReader) ConsumerClosed() {
	t.InternalClose()
}

func (t *indexSkipTableReader) generateTrailingMeta(
	ctx context.Context,
) []execinfrapb.ProducerMetadata {
	trailingMeta := t.generateMeta(ctx)
	t.InternalClose()
	return trailingMeta
}

func (t *indexSkipTableReader) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	if !t.ignoreMisplannedRanges {
		if len(t.misplannedRanges) != 0 {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: t.misplannedRanges})
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, t.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

func (t *indexSkipTableReader) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return t.generateMeta(ctx)
}
