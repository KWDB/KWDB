// Copyright 2018 The Cockroach Authors.
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

package workload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	rowStartParam = `row-start`
	rowEndParam   = `row-end`
)

// WriteCSVRows writes the specified table rows as a csv. If sizeBytesLimit is >
// 0, it will be used as an approximate upper bound for how much to write. The
// next rowStart is returned (so last row written + 1).
func WriteCSVRows(
	ctx context.Context, w io.Writer, table Table, rowStart, rowEnd int, sizeBytesLimit int64,
) (rowBatchIdx int, err error) {
	cb := coldata.NewMemBatchWithSize(nil /* types */, 0 /* size */)
	var a bufalloc.ByteAllocator

	bytesWrittenW := &bytesWrittenWriter{w: w}
	csvW := csv.NewWriter(bytesWrittenW)
	var rowStrings []string
	for rowBatchIdx = rowStart; rowBatchIdx < rowEnd; rowBatchIdx++ {
		if sizeBytesLimit > 0 && bytesWrittenW.written > sizeBytesLimit {
			break
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		a = a[:0]
		table.InitialRows.FillBatch(rowBatchIdx, cb, &a)
		if numCols := cb.Width(); cap(rowStrings) < numCols {
			rowStrings = make([]string, numCols)
		} else {
			rowStrings = rowStrings[:numCols]
		}
		for rowIdx, numRows := 0, cb.Length(); rowIdx < numRows; rowIdx++ {
			for colIdx, col := range cb.ColVecs() {
				rowStrings[colIdx] = colDatumToCSVString(col, rowIdx)
			}
			if err := csvW.Write(rowStrings, nil); err != nil {
				return 0, err
			}
		}
	}
	csvW.Flush()
	return rowBatchIdx, csvW.Error()
}

type csvRowsReader struct {
	t                    Table
	batchStart, batchEnd int

	buf  bytes.Buffer
	csvW *csv.Writer

	batchIdx int
	cb       coldata.Batch
	a        bufalloc.ByteAllocator

	stringsBuf []string
}

func (r *csvRowsReader) Read(p []byte) (n int, err error) {
	if r.cb == nil {
		r.cb = coldata.NewMemBatchWithSize(nil /* types */, 0 /* size */)
	}

	for {
		if r.buf.Len() > 0 {
			return r.buf.Read(p)
		}
		r.buf.Reset()
		if r.batchIdx == r.batchEnd {
			return 0, io.EOF
		}
		r.a = r.a[:0]
		r.t.InitialRows.FillBatch(r.batchIdx, r.cb, &r.a)
		r.batchIdx++
		if numCols := r.cb.Width(); cap(r.stringsBuf) < numCols {
			r.stringsBuf = make([]string, numCols)
		} else {
			r.stringsBuf = r.stringsBuf[:numCols]
		}
		for rowIdx, numRows := 0, r.cb.Length(); rowIdx < numRows; rowIdx++ {
			for colIdx, col := range r.cb.ColVecs() {
				r.stringsBuf[colIdx] = colDatumToCSVString(col, rowIdx)
			}
			if err := r.csvW.Write(r.stringsBuf, nil); err != nil {
				return 0, err
			}
		}
		r.csvW.Flush()
	}
}

// NewCSVRowsReader returns an io.Reader that outputs the initial data of the
// given table as CSVs. If batchEnd is the zero-value it defaults to the end of
// the table.
func NewCSVRowsReader(t Table, batchStart, batchEnd int) io.Reader {
	if batchEnd == 0 {
		batchEnd = t.InitialRows.NumBatches
	}
	r := &csvRowsReader{t: t, batchStart: batchStart, batchEnd: batchEnd, batchIdx: batchStart}
	r.csvW = csv.NewWriter(&r.buf)
	return r
}

func colDatumToCSVString(col coldata.Vec, rowIdx int) string {
	if col.Nulls().NullAt(rowIdx) {
		return `NULL`
	}
	switch col.Type() {
	case coltypes.Bool:
		return strconv.FormatBool(col.Bool()[rowIdx])
	case coltypes.Int64:
		return strconv.FormatInt(col.Int64()[rowIdx], 10)
	case coltypes.Float64:
		return strconv.FormatFloat(col.Float64()[rowIdx], 'f', -1, 64)
	case coltypes.Bytes:
		// See the HACK comment in ColBatchToRows.
		bytes := col.Bytes().Get(rowIdx)
		return *(*string)(unsafe.Pointer(&bytes))
	}
	panic(fmt.Sprintf(`unhandled type %s`, col.Type().GoTypeName()))
}

// HandleCSV configures a Generator with url params and outputs the data for a
// single Table as a CSV (optionally limiting the rows via `row-start` and
// `row-end` params). It is intended for use in implementing a
// `net/http.Handler`.
func HandleCSV(w http.ResponseWriter, req *http.Request, prefix string, meta Meta) error {
	ctx := context.Background()
	if err := req.ParseForm(); err != nil {
		return err
	}

	gen := meta.New()
	if f, ok := gen.(Flagser); ok {
		var flags []string
		f.Flags().VisitAll(func(f *pflag.Flag) {
			if vals, ok := req.Form[f.Name]; ok {
				for _, val := range vals {
					flags = append(flags, fmt.Sprintf(`--%s=%s`, f.Name, val))
				}
			}
		})
		if err := f.Flags().Parse(flags); err != nil {
			return errors.Wrapf(err, `parsing parameters %s`, strings.Join(flags, ` `))
		}
	}

	tableName := strings.TrimPrefix(req.URL.Path, prefix)
	var table *Table
	for _, t := range gen.Tables() {
		if t.Name == tableName {
			table = &t
			break
		}
	}
	if table == nil {
		return errors.Errorf(`could not find table %s in generator %s`, tableName, meta.Name)
	}
	if table.InitialRows.FillBatch == nil {
		return errors.Errorf(`csv-server is not supported for workload %s`, meta.Name)
	}

	rowStart, rowEnd := 0, table.InitialRows.NumBatches
	if vals, ok := req.Form[rowStartParam]; ok && len(vals) > 0 {
		var err error
		rowStart, err = strconv.Atoi(vals[len(vals)-1])
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, rowStartParam)
		}
	}
	if vals, ok := req.Form[rowEndParam]; ok && len(vals) > 0 {
		var err error
		rowEnd, err = strconv.Atoi(vals[len(vals)-1])
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, rowEndParam)
		}
	}

	w.Header().Set(`Content-Type`, `text/csv`)
	_, err := WriteCSVRows(ctx, w, *table, rowStart, rowEnd, -1 /* sizeBytesLimit */)
	return err
}

type bytesWrittenWriter struct {
	w       io.Writer
	written int64
}

func (w *bytesWrittenWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.written += int64(n)
	return n, err
}

// CSVMux returns a mux over http handers for csv data in all tables in the
// given generators.
func CSVMux(metas []Meta) *http.ServeMux {
	mux := http.NewServeMux()
	for _, meta := range metas {
		meta := meta
		prefix := fmt.Sprintf(`/csv/%s/`, meta.Name)
		mux.HandleFunc(prefix, func(w http.ResponseWriter, req *http.Request) {
			if err := HandleCSV(w, req, prefix, meta); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})
	}
	return mux
}
