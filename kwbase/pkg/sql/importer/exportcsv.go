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
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/pkg/errors"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"
const exportFilePatternSQL = exportFilePatternPart + ".sql"

// newCSVWriterProcessor is used to make a new CSVWriter Processor
func newCSVWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.CSVWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {

	c := &csvWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type csvWriter struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.CSVWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

var csvValuePool = sync.Pool{
	New: func() interface{} {
		return &csvValue{}
	},
}

type csvValue struct {
	rows    []sqlbase.EncDatumRow
	numRows int64
}

func (cv *csvValue) Reset() {
	cv.rows = cv.rows[:0]
}

// Used for concurrent writing of CSV
type csvWriteFile struct {
	syncutil.Mutex
	chunk            int
	chunkLimit       int
	preChunkFileName string
	csvCh            chan *csvValue
}

func (sp *csvWriter) Push(ctx context.Context, res []byte) error {
	return nil
}

func (sp *csvWriter) IsShortCircuitForPgEncode() bool {
	return false
}

func (sp *csvWriter) NextPgWire() (val []byte, code int, err error) {
	return nil, 0, nil
}

func (sp *csvWriter) SupportPgWire() bool {
	return false
}

func (sp *csvWriter) Start(ctx context.Context) context.Context {
	return nil
}

// RunTS is the same as Run, but for kwdb processor.
func (sp *csvWriter) RunTS(ctx context.Context) {
	// TODO(xy): Maybe it will be useful in the future.
	// It should be called for time series export,
	// but it's never been called in flow.Run or flow.Start now.
	log.Error(ctx, "csvWriter's RunTS does not implement")
}

var _ execinfra.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []types.T {
	var res []types.T
	if sp.spec.IsTS {
		res = make([]types.T, len(sqlbase.ExportTsColumns))
		for i := range res {
			res[i] = *sqlbase.ExportTsColumns[i].Typ
		}
	} else {
		res = make([]types.T, len(sqlbase.ExportColumns))
		for i := range res {
			res[i] = *sqlbase.ExportColumns[i].Typ
		}
	}
	return res
}

func (sp *csvWriter) csvNewWriter() (*csv.Writer, *bytes.Buffer) {
	var buf bytes.Buffer
	nullsAs := ""
	if sp.spec.Options.NullEncoding != nil {
		nullsAs = *sp.spec.Options.NullEncoding
	}

	writer := csv.NewWriter(&buf)
	if sp.spec.Options.Comma != 0 {
		writer.Comma = sp.spec.Options.Comma
	}
	if sp.spec.Options.Enclosed != 0 {
		writer.Enclosed = sp.spec.Options.Enclosed
	}
	if sp.spec.Options.Escaped != 0 {
		writer.Escaped = sp.spec.Options.Escaped
	}
	if sp.spec.FileFormat == "SQL" {
		nullsAs = "NULL"
		writer.IsSQL = true
	}
	if nullsAs != "" {
		writer.Nullas = nullsAs
	}
	return writer, &buf
}

// csvColumnNameWriterWithLen written to specify the length of the column name.
func (sp *csvWriter) csvColumnNameWriterWithLen(colsNum int64) (*csv.Writer, *bytes.Buffer) {
	if len(sp.spec.ColNames) != 0 {
		csvRow := make([]string, colsNum)
		colNameWriter, colNameBuf := sp.csvNewWriter()
		for i, cn := range sp.spec.ColNames {
			csvRow[i] = cn
		}
		if err := colNameWriter.Write(csvRow, nil); err != nil {
			return nil, nil
		}
		colNameWriter.Flush()
		return colNameWriter, colNameBuf
	}
	return nil, nil
}

func (sp *csvWriter) csvColumnNameWriter() (*csv.Writer, *bytes.Buffer) {
	if len(sp.spec.ColNames) != 0 {
		typs := sp.input.OutputTypes()
		csvRow := make([]string, len(typs))
		colNameWriter, colNameBuf := sp.csvNewWriter()
		for i, cn := range sp.spec.ColNames {
			csvRow[i] = cn
		}
		if err := colNameWriter.Write(csvRow, nil); err != nil {
			return nil, nil
		}
		colNameWriter.Flush()
		return colNameWriter, colNameBuf
	}
	return nil, nil
}

// makeSQLData assemble data, tables, and column names into SQL statements
func makeSQLData(line []byte, colName []byte, prefix string) ([]byte, error) {
	insert := "INSERT INTO " + prefix + "(" + string(colName) + ")" + " VALUES (" + string(line) + ");\n"
	return []byte(insert), nil
}

// transferCharset transfer CSV	from UTF8 charset to GBK/GB18030/BIG5
func (sp *csvWriter) transferCharset(ctx context.Context, line []byte) ([]byte, error) {
	switch sp.spec.Options.Charset {
	case "GBK":
		utfLine, errE := pgwire.Utf8ToGbk(line)
		if errE != nil {
			log.Errorf(ctx, "transfer charset to %s failed, err is %s", sp.spec.Options.Charset, errE)
			return nil, errE
		}
		return utfLine, nil
	case "GB18030":
		utfLine, errE := pgwire.Utf8ToGb18030(line)
		if errE != nil {
			log.Errorf(ctx, "transfer charset to %s failed, err is %s", sp.spec.Options.Charset, errE)
			return nil, errE
		}
		return utfLine, nil
	case "BIG5":
		utfLine, errE := pgwire.Utf8ToBig5(line)
		if errE != nil {
			log.Errorf(ctx, "transfer charset to %s failed, err is %s", sp.spec.Options.Charset, errE)
			return nil, errE
		}
		return utfLine, nil
	default:
		return line, nil
	}
}

// Run is the main loop of the processor.
// It starts one goroutine to run input.NextRow() to get row values.
// It starts parallelNum goroutine to read row value from channel, convert and write down.
func (sp *csvWriter) Run(ctx context.Context) execinfra.RowStats {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter", int32(sp.flowCtx.NodeID))
	defer tracing.FinishSpan(span)

	var numRowsTotal, sizeTotal, numRowsOfFile, colsNum int64

	err := func() error {
		if sp.spec.OnlyMeta {
			return nil
		}
		parallelNum := runtime.NumCPU()
		cwf := &csvWriteFile{
			csvCh: make(chan *csvValue, parallelNum),
		}
		nullsAs := ""
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}
		pattern := exportFilePatternDefault
		if sp.spec.FileFormat == "SQL" {
			pattern = exportFilePatternSQL
		}
		if sp.spec.NamePattern != "" {
			pattern = sp.spec.NamePattern
		}
		typs := sp.input.OutputTypes()

		// get ExternalStorage
		conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination)
		if err != nil {
			return err
		}
		es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		defer es.Close()
		if int64(len(typs)) != sp.spec.ColsNum && sp.spec.ColsNum != 0 {
			colsNum = sp.spec.ColsNum
		} else {
			colsNum = int64(len(typs))
		}
		// write col names buf
		_, colNameBuf := sp.csvColumnNameWriterWithLen(colsNum)
		filename := getFileName(int(sp.flowCtx.EvalCtx.NodeID), 0, pattern)
		// transfer charset of col name
		if colNameBuf != nil && sp.spec.Options.Charset != "UTF-8" && sp.spec.Options.Charset != "" {
			Line, errE := sp.transferCharset(ctx, colNameBuf.Bytes())
			if errE != nil {
				return errors.Wrapf(errE, "transfer charset of col name from UTF8 to %s failed, err is %s", sp.spec.Options.Charset, errE)
			}
			colNameBuf.Reset()
			colNameBuf.Write(Line)
		}
		// first write column name
		cwf.Lock()
		if colNameBuf != nil && sp.spec.FileFormat != "SQL" {
			if err := es.AppendWrite(ctx, filename, bytes.NewReader(colNameBuf.Bytes())); err != nil {
				cwf.Unlock()
				return errors.Wrapf(err, "write file %s", filename)
			}
		}
		cwf.Unlock()

		/* 1. goroutine performs concurrent writes */
		process := func(ctx context.Context) error {
			// write data buf
			writer, buf := sp.csvNewWriter()
			_, bufSQL := sp.csvNewWriter()

			alloc := &sqlbase.DatumAlloc{}
			f := tree.NewFmtCtx(tree.FmtExport)
			defer f.Close()
			csvRow := make([]string, colsNum)

			for value := range cwf.csvCh {
				// receive data, then lock, block other goroutine
				cwf.Lock()
				chunk := cwf.chunk
				cwf.chunk++
				cwf.Unlock()
				buf.Reset()
				for rowID := range value.rows {
					// initialize a map to record the position of empty values in a csvRow.
					nilMap := make(map[int]struct{})
					for i, ed := range value.rows[rowID] {
						if ed.IsNull() {
							csvRow[i] = nullsAs
							nilMap[i] = struct{}{}
							continue
						}
						if err := ed.EnsureDecoded(&typs[i], alloc); err != nil {
							return err
						}
						ed.Datum.Format(f)
						if sp.spec.FileFormat == "SQL" {
							if sp.spec.ColumnTypes[i] == types.TimestampTZFamily ||
								sp.spec.ColumnTypes[i] == types.StringFamily ||
								sp.spec.ColumnTypes[i] == types.TimestampFamily ||
								sp.spec.ColumnTypes[i] == types.TimeFamily ||
								sp.spec.ColumnTypes[i] == types.TimeTZFamily {
								csvRow[i] = fieldSQLResolve(f.String())
							} else {
								csvRow[i] = f.String()
							}
						} else {
							csvRow[i] = f.String()
						}
						f.Reset()
					}
					if sp.spec.FileFormat == "SQL" {
						if err := writer.WriteSQL(csvRow, nilMap); err != nil {
							return err
						}
					} else {
						if err := writer.Write(csvRow, nilMap); err != nil {
							return err
						}
					}
					if sp.spec.FileFormat == "SQL" {
						writer.Flush()
						line, _ := makeSQLData(buf.Bytes(), colNameBuf.Bytes(), sp.spec.TablePrefix)
						bufSQL.Write(line)
						buf.Reset()
					}
				}
				writer.Flush()
				if sp.spec.FileFormat == "SQL" {
					buf.Write(bufSQL.Bytes())
				}
				// transfer data charset
				if sp.spec.Options.Charset != "UTF-8" && sp.spec.Options.Charset != "" {
					line, err := sp.transferCharset(ctx, buf.Bytes())
					if err != nil {
						return errors.Wrapf(err, "transfer charset of data from UTF8 to %s failed, err is %s", sp.spec.Options.Charset, err)
					}
					buf.Reset()
					buf.Write(line)
				}
				size := buf.Len()

				err = sp.writeExportFile(ctx, &writeExportParams{pattern, es, buf, colNameBuf, &numRowsOfFile, cwf, value, chunk})
				if err != nil {
					return err
				}
				atomic.AddInt64(&sizeTotal, int64(size))
				atomic.AddInt64(&numRowsTotal, value.numRows)
				csvValuePool.Put(value)
			}
			return nil
		}
		/* 2. start goroutine in parallel */
		group := ctxgroup.WithContext(ctx)

		group.GoCtx(func(ctx context.Context) error {
			return ctxgroup.GroupWorkers(ctx, int(parallelNum), func(ctx context.Context, id int) error {
				return process(ctx)
			})
		})

		/* 3. goroutine reads the data and writes it to csvValue chan */
		group.GoCtx(func(ctx context.Context) error {
			defer close(cwf.csvCh)
			sp.input.Start(ctx)
			input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)
			done := false
			for {
				var rowsCount int64
				c := csvValuePool.Get().(*csvValue)
				c.Reset()
				for {
					// 1 if ChunkRows less than 10W, a batch of ChunkRows will be send. write trhead will
					//    write file in ChunkRows.
					// 2 if ChunkRows greater than or equal 10W, a batch of 10W will be send. write thread will
					//    write file in ChunkRows.
					if sp.spec.ChunkRows <= sql.ExportChunkSizeDefault { // flag = sp.spec.ChunkRows < sql.ExportChunkSizeDefault
						if sp.spec.ChunkRows > 0 && rowsCount >= sp.spec.ChunkRows {
							break
						}
					} else {
						if rowsCount >= sql.ExportChunkSizeDefault {
							break
						}
					}

					row, err := input.NextRow()
					if err != nil {
						return err
					}
					if row == nil {
						done = true
						break
					}
					c.rows = append(c.rows, row.CopyLen(colsNum))
					rowsCount++
				}
				if rowsCount < 1 {
					break
				}
				c.numRows = rowsCount
				cwf.csvCh <- c
				if done {
					break
				}
			}
			return nil
		})
		err = group.Wait()
		if err != nil {
			return err
		}
		// select * 's tsTableReader will be sent to every node
		if numRowsTotal == 0 {
			return nil
		}

		fileNum := cwf.chunk
		unLimit := false
		if sp.spec.ChunkRows == 0 {
			unLimit = true
		}

		if unLimit {
			fileNum = 1
		} else {
			if sp.spec.ChunkRows <= sql.ExportChunkSizeDefault {
				fileNum = cwf.chunk
			} else {
				fileNum = cwf.chunkLimit
			}
		}
		return sp.sendResult(ctx, numRowsTotal, sizeTotal, fileNum)
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
	return execinfra.RowStats{}
}

// fieldSQLResolve used to handle special column contents when exporting SQL files
func fieldSQLResolve(field string) string {
	/*
					  _________________________________________________________________
				  	|           data                |              quote            |
				  	|-------------------------------|-------------------------------|
				  	|     end_enclose\n_qwe         |       'end_enclose\n_qwe'     |
				    |     end_enclose
				  	      _qwe                      |       E'end_enclose\n_qwe'    |
				  	|     end_enclose\n_qwe         |       'end_enclose\n_qwe'     |
						|     basic_string基础测试       |       'basic_string基础测试'   |
						|     "begin_enclose前          |       '"begin_enclose前'      |
						|     middle_enclose"中         |       'middle_enclose"中'     |
						|     end_enclose后"            |       'end_enclose后"'        |
						|     'begin_enclose前          |       E'\'begin_enclose前'    |
						|     middle_enclose'中         |       E'middle_enclose\'中'   |
						|     end_enclose后'            |       E'end_enclose后\''      |
						|     end_enclose
			            跨行                      |    E'end_enclose\n跨行'        |
						|     seprator,分隔符basic测试   |     'seprator,分隔符basic测试' |
						|     ,seprator分隔符前          |       ',seprator分隔符前'      |
						|     seprator,分隔符中          |       'seprator,分隔符中'      |
						|     seprator分隔符后,          |       'seprator分隔符后,'      |
						|     \escape基础测试            |       '\escape基础测试'        |
						|     escape基础\测试中           |       'escape基础\测试中'     |
						|     escape测试后\              |       'escape测试后\'         |
						|     end_'enclose
			            跨行                       |    'E'end_\'enclose\n跨行''  |
						|     end_'enclose
			            跨'asda
			            asd                        | E'end_\'enclose\n跨\'asda\nasd'|
						|     end_'enclose
			            跨行                        |       E'end_\'enclose\r跨行'   |
		        |     end_enclose\n跨行           |       'end_enclose\n跨行'      |

	*/
	field, hasLineBreakN := checkAndHandleLineBreakN(field)
	field, hasLineBreakR := checkAndHandleLineBreakR(field)
	field, hasSingleQuota := checkAndHandleSingleQuota(field)
	if hasLineBreakN || hasLineBreakR || hasSingleQuota {
		return "E'" + field + "'"
	}
	return "'" + field + "'"
}

// checkAndHandleLineBreakN Determine if there is a \n line break and process it
func checkAndHandleLineBreakN(field string) (string, bool) {
	var str string
	for {
		if strings.ContainsAny(field, "\n") {
			index := strings.IndexAny(field, "\n")
			str += field[:index] + "\\n"
			field = field[index+1:]
		} else {
			break
		}
	}
	if str == "" {
		return field, false
	}
	return str + field, true
}

// checkAndHandleLineBreakR Determine if there is a \r line break and process it
func checkAndHandleLineBreakR(field string) (string, bool) {
	var str string
	for {
		if strings.ContainsAny(field, "\r") {
			index := strings.IndexAny(field, "\r")
			str += field[:index] + "\\r"
			field = field[index+1:]
		} else {
			break
		}
	}
	if str == "" {
		return field, false
	}
	return str + field, true
}

// checkAndHandleSingleQuota return whether there are single quotes and the processed string with single quotes
func checkAndHandleSingleQuota(field string) (string, bool) {
	var str string
	for {
		if strings.ContainsAny(field, "'") {
			index := strings.IndexAny(field, "'")
			str += field[:index] + "\\" + field[index:index+1]
			field = field[index+1:]
		} else {
			break
		}
	}
	if str == "" {
		return field, false
	}
	return str + field, true
}

// sendResult is used to return the record of csv file for print
// rows is the csv files' rows count from this node
// chunk is the csv files' count from this node
// export format result column is in result_columns: ExportColumns or ExportTsColumns
// such as ExportColumns:
//
//	filename  | rows | node_id | file_num
//
// ------------+------+---------+-----------
//
//	meta.sql  |    1 |       1 |        1
//	TABLE tb1 |    2 |       5 |        1
//	TABLE tb1 |    1 |       4 |        1
//	TABLE tb1 |    1 |       1 |        1
//
// or such as ExportTsColumns:
//
//	result
//
// -----------
//
//	succeed
func (sp *csvWriter) sendResult(ctx context.Context, rows int64, size int64, chunk int) error {
	if sp.spec.IsTS {
		return nil
	}
	res := sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(
			types.String,
			tree.NewDString(sp.spec.QueryName),
		),
		sqlbase.DatumToEncDatum(
			types.Int,
			tree.NewDInt(tree.DInt(rows)),
		),
		sqlbase.DatumToEncDatum(
			types.Int,
			tree.NewDInt(tree.DInt(sp.flowCtx.EvalCtx.NodeID)),
		),
		sqlbase.DatumToEncDatum(
			types.Int,
			tree.NewDInt(tree.DInt(chunk)),
		),
	}

	cs, err := sp.out.EmitRow(ctx, res)
	if err != nil {
		return err
	}
	if cs != execinfra.NeedMoreRows {
		// TODO(dt): presumably this is because our recv already closed due to
		// another error... so do we really need another one?
		return errors.New("unexpected closure of consumer")
	}
	return nil
}

// writeExportFile params
type writeExportParams struct {
	pattern       string
	es            cloud.ExternalStorage
	buf           *bytes.Buffer
	colNameBuf    *bytes.Buffer
	numRowsOfFile *int64
	cwf           *csvWriteFile
	value         *csvValue
	chunk         int
}

// write export file
func (sp *csvWriter) writeExportFile(ctx context.Context, writePara *writeExportParams) error {
	var writeMt syncutil.Mutex
	var unLimit bool

	pattern := writePara.pattern
	es := writePara.es
	buf := writePara.buf
	colNameBuf := writePara.colNameBuf
	numRowsOfFile := writePara.numRowsOfFile
	cwf := writePara.cwf
	value := writePara.value
	chunk := writePara.chunk

	// if chunkRows is 0, means export unlimited rows
	if sp.spec.ChunkRows == 0 {
		unLimit = true
	}
	// if set export flag column_name, when write a file, first chunk write column name,
	// second and subsequent ones not write column name.
	if unLimit {
		// if unlimited file rows, for example: chunkSizse = 0, go here, write the same file every time.
		// for example : n1.0.csv
		filename := getFileName(int(sp.flowCtx.EvalCtx.NodeID), 0, pattern)
		// call append write interface. if wirte is thread safe , the lock mechanism can be removed.
		writeMt.Lock()
		// write column name, only write ones.
		if err := es.AppendWrite(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
			writeMt.Unlock()
			return errors.Wrapf(err, "write file %s", filename)
		}
		writeMt.Unlock()
	} else {
		// 1 if ChunkRows less 10W, then write different files according to ChunkRows.
		// 2 if ChunkRows greater than or equal 10W, a batch will received at 10W.
		// 3 if number of lines of current file greater than or equal ChunkRows,
		//   then create new file to write to.
		if sp.spec.ChunkRows <= sql.ExportChunkSizeDefault {
			// if limit file size, for example: chunkRows = 100, go here: write a file every chunk.
			// for example: n1.0.csv, n1.1.csv, n1.2.csv
			filename := getFileName(int(sp.flowCtx.EvalCtx.NodeID), chunk, pattern)
			// write col names
			if colNameBuf != nil && chunk != 0 {
				if err := es.AppendWrite(ctx, filename, bytes.NewReader(colNameBuf.Bytes())); err != nil {
					return errors.Wrapf(err, "write file %s", filename)
				}
			}
			// call appended write interface
			if err := es.AppendWrite(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
				return errors.Wrapf(err, "write file %s", filename)
			}
		} else {
			// count rows of current batch , for example: chunkRows = 30W, go here.
			cwf.Lock()
			atomic.AddInt64(numRowsOfFile, value.numRows)
			if *numRowsOfFile > sp.spec.ChunkRows || cwf.chunkLimit == 0 {
				// write new file
				chunkLimit := cwf.chunkLimit
				cwf.chunkLimit++
				filename := getFileName(int(sp.flowCtx.EvalCtx.NodeID), chunkLimit, pattern)
				getFileName(int(sp.flowCtx.EvalCtx.NodeID), chunkLimit, pattern)
				cwf.preChunkFileName = filename
				atomic.StoreInt64(numRowsOfFile, int64(value.numRows))
				cwf.Unlock()

				// write col names
				if colNameBuf != nil && chunkLimit != 0 {
					if err := es.AppendWrite(ctx, filename, bytes.NewReader(colNameBuf.Bytes())); err != nil {
						return errors.Wrapf(err, "write file %s", filename)
					}
				}

				if err := es.AppendWrite(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
					return errors.Wrapf(err, "write file %s", filename)
				}
			} else {
				// write pre file, need obtain lock
				filename := cwf.preChunkFileName
				cwf.Unlock()
				// call appended write interface
				if err := es.AppendWrite(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
					return errors.Wrapf(err, "write file %s", filename)
				}
			}
		}
	}
	return nil
}

// get file name of export
func getFileName(nodeID int, chunkID int, pattern string) string {
	part := fmt.Sprintf("n%d.%d", nodeID, chunkID)
	filename := strings.Replace(pattern, exportFilePatternPart, part, -1)
	return filename
}

func init() {
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
}
