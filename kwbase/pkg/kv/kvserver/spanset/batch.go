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

package spanset

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// Iterator wraps an engine.Iterator and ensures that it can
// only be used to access spans in a SpanSet.
type Iterator struct {
	i     relational.Iterator
	spans *SpanSet

	// spansOnly controls whether or not timestamps associated with the
	// spans are considered when ensuring access. If set to true,
	// only span boundaries are checked.
	spansOnly bool

	// Timestamp the access is taking place. If timestamp is zero, access is
	// considered non-MVCC. If spansOnly is set to true, ts is not consulted.
	ts hlc.Timestamp

	// Seeking to an invalid key puts the iterator in an error state.
	err error
	// Reaching an out-of-bounds key with Next/Prev invalidates the
	// iterator but does not set err.
	invalid bool
}

var _ relational.Iterator = &Iterator{}
var _ relational.MVCCIterator = &Iterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet. Timestamps associated with the spans
// in the spanset are not considered, only the span boundaries are checked.
func NewIterator(iter relational.Iterator, spans *SpanSet) *Iterator {
	return &Iterator{i: iter, spans: spans, spansOnly: true}
}

// NewIteratorAt constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet at the given timestamp.
func NewIteratorAt(iter relational.Iterator, spans *SpanSet, ts hlc.Timestamp) *Iterator {
	return &Iterator{i: iter, spans: spans, ts: ts}
}

// Close is part of the engine.Iterator interface.
func (i *Iterator) Close() {
	i.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (i *Iterator) Iterator() relational.Iterator {
	return i.i
}

// SeekGE is part of the engine.Iterator interface.
func (i *Iterator) SeekGE(key relational.MVCCKey) {
	if i.spansOnly {
		i.err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	} else {
		i.err = i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, i.ts)
	}
	if i.err == nil {
		i.invalid = false
	}
	i.i.SeekGE(key)
}

// SeekLT is part of the engine.Iterator interface.
func (i *Iterator) SeekLT(key relational.MVCCKey) {
	// CheckAllowed{At} supports the span representation of [,key), which
	// corresponds to the span [key.Prev(),).
	revSpan := roachpb.Span{EndKey: key.Key}
	if i.spansOnly {
		i.err = i.spans.CheckAllowed(SpanReadOnly, revSpan)
	} else {
		i.err = i.spans.CheckAllowedAt(SpanReadOnly, revSpan, i.ts)
	}
	if i.err == nil {
		i.invalid = false
	}
	i.i.SeekLT(key)
}

// Valid is part of the engine.Iterator interface.
func (i *Iterator) Valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	ok, err := i.i.Valid()
	if err != nil {
		return false, i.err
	}
	return ok && !i.invalid, nil
}

// Next is part of the engine.Iterator interface.
func (i *Iterator) Next() {
	i.i.Next()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// Prev is part of the engine.Iterator interface.
func (i *Iterator) Prev() {
	i.i.Prev()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// NextKey is part of the engine.Iterator interface.
func (i *Iterator) NextKey() {
	i.i.NextKey()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// Key is part of the engine.Iterator interface.
func (i *Iterator) Key() relational.MVCCKey {
	return i.i.Key()
}

// Value is part of the engine.Iterator interface.
func (i *Iterator) Value() []byte {
	return i.i.Value()
}

// ValueProto is part of the engine.Iterator interface.
func (i *Iterator) ValueProto(msg protoutil.Message) error {
	return i.i.ValueProto(msg)
}

// UnsafeKey is part of the engine.Iterator interface.
func (i *Iterator) UnsafeKey() relational.MVCCKey {
	return i.i.UnsafeKey()
}

// UnsafeValue is part of the engine.Iterator interface.
func (i *Iterator) UnsafeValue() []byte {
	return i.i.UnsafeValue()
}

// ComputeStats is part of the engine.Iterator interface.
func (i *Iterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return enginepb.MVCCStats{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, i.ts); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return i.i.ComputeStats(start, end, nowNanos)
}

// FindSplitKey is part of the engine.Iterator interface.
func (i *Iterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (relational.MVCCKey, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return relational.MVCCKey{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, i.ts); err != nil {
			return relational.MVCCKey{}, err
		}
	}
	return i.i.FindSplitKey(start, end, minSplitKey, targetSize)
}

// CheckForKeyCollisions is part of the engine.Iterator interface.
func (i *Iterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return i.i.CheckForKeyCollisions(sstData, start, end)
}

// SetUpperBound is part of the engine.Iterator interface.
func (i *Iterator) SetUpperBound(key roachpb.Key) {
	i.i.SetUpperBound(key)
}

// Stats is part of the engine.Iterator interface.
func (i *Iterator) Stats() relational.IteratorStats {
	return i.i.Stats()
}

// MVCCOpsSpecialized is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCOpsSpecialized() bool {
	if mvccIt, ok := i.i.(relational.MVCCIterator); ok {
		return mvccIt.MVCCOpsSpecialized()
	}
	return false
}

// MVCCGet is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts relational.MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key}); err != nil {
			return nil, nil, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key}, timestamp); err != nil {
			return nil, nil, err
		}
	}
	return i.i.(relational.MVCCIterator).MVCCGet(key, timestamp, opts)
}

// MVCCScan is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCScan(
	start, end roachpb.Key, timestamp hlc.Timestamp, opts relational.MVCCScanOptions,
) (relational.MVCCScanResult, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return relational.MVCCScanResult{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, timestamp); err != nil {
			return relational.MVCCScanResult{}, err
		}
	}
	return i.i.(relational.MVCCIterator).MVCCScan(start, end, timestamp, opts)
}

type spanSetReader struct {
	r     relational.Reader
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ relational.Reader = spanSetReader{}

func (s spanSetReader) Close() {
	s.r.Close()
}

func (s spanSetReader) Closed() bool {
	return s.r.Closed()
}

// ExportToSst is part of the engine.Reader interface.
func (s spanSetReader) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io relational.IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return s.r.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

func (s spanSetReader) Get(key relational.MVCCKey) ([]byte, error) {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
			return nil, err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return nil, err
		}
	}
	//lint:ignore SA1019 implementing deprecated interface function (Get) is OK
	return s.r.Get(key)
}

func (s spanSetReader) GetProto(
	key relational.MVCCKey, msg protoutil.Message,
) (bool, int64, int64, error) {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
			return false, 0, 0, err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return false, 0, 0, err
		}
	}
	//lint:ignore SA1019 implementing deprecated interface function (GetProto) is OK
	return s.r.GetProto(key, msg)
}

func (s spanSetReader) Iterate(
	start, end roachpb.Key, f func(relational.MVCCKeyValue) (bool, error),
) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, s.ts); err != nil {
			return err
		}
	}
	return s.r.Iterate(start, end, f)
}

func (s spanSetReader) NewIterator(opts relational.IterOptions) relational.Iterator {
	if s.spansOnly {
		return NewIterator(s.r.NewIterator(opts), s.spans)
	}
	return NewIteratorAt(s.r.NewIterator(opts), s.spans, s.ts)
}

// GetDBEngine recursively searches for the underlying rocksDB engine.
func GetDBEngine(reader relational.Reader, span roachpb.Span) relational.Reader {
	switch v := reader.(type) {
	case ReadWriter:
		return GetDBEngine(getSpanReader(v, span), span)
	case *spanSetBatch:
		return GetDBEngine(getSpanReader(v.ReadWriter, span), span)
	default:
		return reader
	}
}

// getSpanReader is a getter to access the engine.Reader field of the
// spansetReader.
func getSpanReader(r ReadWriter, span roachpb.Span) relational.Reader {
	if err := r.spanSetReader.spans.CheckAllowed(SpanReadOnly, span); err != nil {
		panic("Not in the span")
	}

	return r.spanSetReader.r
}

type spanSetWriter struct {
	w     relational.Writer
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ relational.Writer = spanSetWriter{}

func (s spanSetWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	// Assume that the constructor of the batch has bounded it correctly.
	return s.w.ApplyBatchRepr(repr, sync)
}

func (s spanSetWriter) Clear(key relational.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Clear(key)
}

func (s spanSetWriter) SingleClear(key relational.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.SingleClear(key)
}

func (s spanSetWriter) ClearRange(start, end relational.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.ClearRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter relational.Iterator, start, end roachpb.Key) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}, s.ts); err != nil {
			return err
		}
	}
	return s.w.ClearIterRange(iter, start, end)
}

func (s spanSetWriter) Merge(key relational.MVCCKey, value []byte) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Merge(key, value)
}

func (s spanSetWriter) Put(key relational.MVCCKey, value []byte) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Put(key, value)
}

func (s spanSetWriter) LogData(data []byte) error {
	return s.w.LogData(data)
}

func (s spanSetWriter) LogLogicalOp(
	op relational.MVCCLogicalOpType, details relational.MVCCLogicalOpDetails,
) {
	s.w.LogLogicalOp(op, details)
}

// ReadWriter is used outside of the spanset package internally, in ccl.
type ReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ relational.ReadWriter = ReadWriter{}

func makeSpanSetReadWriter(rw relational.ReadWriter, spans *SpanSet) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, spansOnly: true},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, spansOnly: true},
	}
}

func makeSpanSetReadWriterAt(
	rw relational.ReadWriter, spans *SpanSet, ts hlc.Timestamp,
) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, ts: ts},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, ts: ts},
	}
}

// NewReadWriter returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet.
func NewReadWriter(rw relational.ReadWriter, spans *SpanSet) relational.ReadWriter {
	return makeSpanSetReadWriter(rw, spans)
}

// NewReadWriterAt returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet at a given timestamp.
// If zero timestamp is provided, accesses are considered non-MVCC.
func NewReadWriterAt(
	rw relational.ReadWriter, spans *SpanSet, ts hlc.Timestamp,
) relational.ReadWriter {
	return makeSpanSetReadWriterAt(rw, spans, ts)
}

type spanSetBatch struct {
	ReadWriter
	b     relational.Batch
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ relational.Batch = spanSetBatch{}

func (s spanSetBatch) Commit(sync bool, commitType relational.BatchCommitType) error {
	return s.b.Commit(sync, relational.NormalCommitType)
}

func (s spanSetBatch) Distinct() relational.ReadWriter {
	if s.spansOnly {
		return NewReadWriter(s.b.Distinct(), s.spans)
	}
	return NewReadWriterAt(s.b.Distinct(), s.spans, s.ts)
}

func (s spanSetBatch) Empty() bool {
	return s.b.Empty()
}

func (s spanSetBatch) Len() int {
	return s.b.Len()
}

func (s spanSetBatch) Repr() []byte {
	return s.b.Repr()
}

// NewBatch returns an engine.Batch that asserts access of the underlying
// Batch against the given SpanSet. We only consider span boundaries, associated
// timestamps are not considered.
func NewBatch(b relational.Batch, spans *SpanSet) relational.Batch {
	return &spanSetBatch{
		ReadWriter: makeSpanSetReadWriter(b, spans),
		b:          b,
		spans:      spans,
		spansOnly:  true,
	}
}

// NewBatchAt returns an engine.Batch that asserts access of the underlying
// Batch against the given SpanSet at the given timestamp.
// If the zero timestamp is used, all accesses are considered non-MVCC.
func NewBatchAt(b relational.Batch, spans *SpanSet, ts hlc.Timestamp) relational.Batch {
	return &spanSetBatch{
		ReadWriter: makeSpanSetReadWriterAt(b, spans, ts),
		b:          b,
		spans:      spans,
		ts:         ts,
	}
}
