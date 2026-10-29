// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/storage/localexternal"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

var testEvalCtx = &tree.EvalContext{
	SessionData: &sessiondata.SessionData{
		DataConversion: sessiondata.DataConversionConfig{Location: time.UTC},
	},
	StmtTimestamp: timeutil.Unix(100000000, 0),
	Settings:      cluster.MakeTestingClusterSettings(),
}

// Value generator represents a value of some data at specified row/col.
type csvRow = int
type csvCol = int

type valueGenerator interface {
	getValue(r csvRow, c csvCol) string
}
type intGenerator struct{} // Returns row value
type strGenerator struct{} // Returns generated string

var _ valueGenerator = &intGenerator{}
var _ valueGenerator = &strGenerator{}

func (g *intGenerator) getValue(r csvRow, _ csvCol) string {
	return strconv.Itoa(r)
}

func (g *strGenerator) getValue(r csvRow, c csvCol) string {
	return fmt.Sprintf("data<r=%d;c=%d>", r, c)
}

// A breakpoint handler runs some breakpoint specific logic. A handler
// returns a boolean indicating if this breakpoint should remain active.
// An error result will cause csv generation to stop and the csv server
// to return that error.
type csvBpHandler = func() (bool, error)
type csvBreakpoint struct {
	row     csvRow
	enabled bool
	handler csvBpHandler
}

// csvGenerator generates csv data.
type csvGenerator struct {
	startRow    int
	numRows     int
	columns     []valueGenerator
	breakpoints map[int]*csvBreakpoint

	data      []string
	size      int
	rowPos    int // csvRow number we're emitting
	rowOffset int // Offset within the current row
}

const maxBreakpointPos = math.MaxInt32

var _ io.ReadCloser = &csvGenerator{}

func (csv *csvGenerator) Open() (io.ReadCloser, error) {
	csv.rowPos = 0
	csv.rowOffset = 0
	csv.maybeInitData()
	return csv, nil
}

// Note: we read one row at a time because reading as much as the
// buffer space allows might cause problems for breakpoints (e.g.
// a breakpoint may block until job progress is updated, but that
// update would never happen because we're still reading the data).
func (csv *csvGenerator) Read(p []byte) (n int, err error) {
	n = 0
	if n < cap(p) && csv.rowPos < len(csv.data) {
		if csv.rowOffset == 0 {
			if err = csv.handleBreakpoint(csv.rowPos); err != nil {
				return
			}
		}
		rowBytes := copy(p[n:], csv.data[csv.rowPos][csv.rowOffset:])
		csv.rowOffset += rowBytes
		n += rowBytes

		if rowBytes == len(csv.data[csv.rowPos]) {
			csv.rowOffset = 0
			csv.rowPos++
		}
	}

	if n == 0 {
		_ = csv.Close()
		err = io.EOF
	}

	return
}

func (csv *csvGenerator) maybeInitData() {
	if csv.data != nil {
		return
	}

	csv.data = make([]string, csv.numRows)
	csv.size = 0
	for i := 0; i < csv.numRows; i++ {
		csv.data[i] = ""
		for colIdx, gen := range csv.columns {
			if len(csv.data[i]) > 0 {
				csv.data[i] += ","
			}
			csv.data[i] += gen.getValue(i+csv.startRow, colIdx)
		}
		csv.data[i] += "\n"
		csv.size += len(csv.data[i])
	}
}

func (csv *csvGenerator) handleBreakpoint(idx int) (err error) {
	if bp, ok := csv.breakpoints[idx]; ok && bp.enabled {
		bp.enabled, err = bp.handler()
	}
	return
}

func (csv *csvGenerator) Close() error {
	// Execute breakpoint at the end of the stream.
	_ = csv.handleBreakpoint(maxBreakpointPos)
	csv.data = nil
	return nil
}

// Add a breakpoint to the generator.
func (csv *csvGenerator) addBreakpoint(r csvRow, handler csvBpHandler) *csvBreakpoint {
	csv.breakpoints[r] = &csvBreakpoint{
		row:     r,
		enabled: true,
		handler: handler,
	}
	return csv.breakpoints[r]
}

// Creates a new instance of csvGenerator, generating 'numRows', starting form
// the specified 'startRow'. The 'columns' list specifies data generators for
// each column. The optional 'breakpoints' specifies the list of csv breakpoints
// which allow caller to execute some code while generating csv data.
func newCsvGenerator(startRow int, numRows int, columns ...valueGenerator) *csvGenerator {
	return &csvGenerator{
		startRow:    startRow,
		numRows:     numRows,
		columns:     columns,
		breakpoints: make(map[int]*csvBreakpoint),
	}
}

// generatorExternalStorage is an external storage implementation
// that returns its data from the underlying generator.
type generatorExternalStorage struct {
	conf roachpb.ExternalStorage
	gen  *csvGenerator
}

var _ localexternal.ExternalStorage = &generatorExternalStorage{}

func (es *generatorExternalStorage) Conf() roachpb.ExternalStorage {
	return es.conf
}

func (es *generatorExternalStorage) ReadFile(
	ctx context.Context, basename string,
) (io.ReadCloser, error) {
	return es.gen.Open()
}

func (es *generatorExternalStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.New("generatorExternalStorage does not support seek")
}

func (es *generatorExternalStorage) Close() error {
	return nil
}

func (es *generatorExternalStorage) Size(ctx context.Context, basename string) (int64, error) {
	es.gen.maybeInitData()
	return int64(es.gen.size), nil
}

func (es *generatorExternalStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return errors.New("unsupported")
}

func (es *generatorExternalStorage) AppendWrite(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return errors.New("unsupported")
}

func (es *generatorExternalStorage) ListFiles(ctx context.Context, _ string) ([]string, error) {
	return nil, errors.New("unsupported")
}

func (es *generatorExternalStorage) Delete(ctx context.Context, basename string) error {
	return errors.New("unsupported")
}

// generatedStorage is a factory (localexternal.ExternalStorageFactory)
// that returns the underlying csvGenerators The file name of the
// generated file doesn't matter: the first time we attempt to get a
// generator for a file, we return the next generator on the list.
type generatedStorage struct {
	generators []*csvGenerator
	nextID     int
	nameIDMap  map[string]int
}

func newGeneratedStorage(gens ...*csvGenerator) *generatedStorage {
	return &generatedStorage{
		generators: gens,
		nextID:     0,
		nameIDMap:  make(map[string]int),
	}
}

// Returns the list of file names (URIs) suitable for this factory.
func (ses *generatedStorage) getGeneratorURIs() []interface{} {
	// Names do not matter; all that matters is the number of generators.
	res := make([]interface{}, len(ses.generators))
	for i := range ses.generators {
		res[i] = fmt.Sprintf("http://host.does.not.matter/filename_is_meaningless%d", i)
	}
	return res
}
