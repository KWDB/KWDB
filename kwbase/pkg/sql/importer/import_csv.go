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
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	duck "github.com/duckdb-go-bindings"
)

// importFileInfo describes state specific to a file being imported.
type importFileInfo struct {
	dataFileIdx int32  // dataFileIdx is where the row data in the recordBatch came from.
	skip        int64  // Number of records to skip
	filename    string // source file whole path
	batchSize   int64  // source batch length for flush
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

type fileReader struct {
	io.Reader
	total   int64
	counter byteCounter
}

// readAndConvertFiles read csv file and put [][]string to recordBatch,
// then get data from recordBatch and Convert data into database internal types.(relational -> kvs timeSeries -> exprs)
func readAndConvertFiles(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	kvCh chan row.KVBatch,
) error {
	// 1.Get files resumePos from spec and skip completely finished file.
	dataFiles := spec.Uri
	parallelNums := int64(1)

	if spec.ResumePos != nil {
		// Delete files that were completely processed.
		for id := range spec.Uri {
			if seek, ok := spec.ResumePos[id]; ok && seek == math.MaxInt64 {
				delete(dataFiles, id)
			}
		}
	}
	// 2.Attempt to fetch total number of bytes for all files and split files to logical shards.
	// Split csv files are used to read csv files in parallel.
	fileSizes := make(map[int32]int64, len(dataFiles))
	fileSplitInfos := make(map[int32]partFileInfos, 1)
	for id, dataFile := range dataFiles {
		conf, err := cloud.ExternalStorageConfFromURI(dataFile)
		if err != nil {
			return err
		}
		es, err := flowCtx.Cfg.ExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			fileSplitInfos[id] = partFileInfos{partFileInfo{start: 0, end: 0}}
			break
		}
		// Split files to logical shards.
		pfs, err := splitCSVFile(ctx, flowCtx, dataFile, sz, parallelNums)
		if err != nil {
			return err
		}
		fileSplitInfos[id] = pfs
		fileSizes[id] = sz
	}

	// 3.Read dataFiles and convert dataFiles to KVs.
	done := ctx.Done()
	for dataFileIdx, dataFile := range dataFiles {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if err := func() error {
			conf, err := cloud.ExternalStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := flowCtx.Cfg.ExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			raw, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer raw.Close()

			fileReader := &fileReader{Reader: ioutil.NopCloser(&byteCounter{r: raw}), total: fileSizes[dataFileIdx], counter: byteCounter{r: raw}}
			opts := spec.Format.Csv
			csvReader := csv.NewReader(fileReader)
			if opts.Comma != 0 {
				csvReader.Comma = opts.Comma
			}
			csvReader.FieldsPerRecord = -1
			if opts.Enclosed != 0 {
				csvReader.Enclose = opts.Enclosed
			}
			if opts.Escaped != 0 {
				csvReader.Escape = opts.Escaped
			}
			if opts.NullEncoding != nil && *opts.NullEncoding != "" {
				csvReader.Nullif = *opts.NullEncoding
			}
			csvReader.Charset = opts.Charset
			resumePos := spec.ResumePos[dataFileIdx]
			if resumePos < int64(opts.Skip) {
				resumePos = int64(opts.Skip)
			}
			batchSize := int64(spec.Format.Csv.BatchRows)
			if batchSize <= 0 {
				batchSize = defaultBatchSize
			}
			fileInfo := &importFileInfo{
				dataFileIdx: dataFileIdx,
				skip:        resumePos,
				filename:    dataFile,
				batchSize:   batchSize,
			}
			recordInfo := &importerRecordInfo{
				recordCh: make(chan recordBatch, parallelNums),
			}
			if err = readAndParallelConvert(ctx, flowCtx, spec, fileInfo, recordInfo, *csvReader, kvCh, fileSplitInfos); err != nil {
				return errors.Wrap(err, dataFile)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// readAndParallelConvert reads the data produced by 'producer' and sends
// the data to a set of workers responsible for converting this data to the
// appropriate key/values.
func readAndParallelConvert(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	fileInfo *importFileInfo,
	recordInfo *importerRecordInfo,
	csvReader csv.Reader,
	kvCh chan row.KVBatch,
	fileSplitInfos map[int32]partFileInfos,
) error {
	group := ctxgroup.WithContext(ctx)
	tableDesc := spec.Table.Desc
	// Start consumers.
	// As many logical shards as a file contains, as many goroutines are opened.
	parallelism := 1
	minEmited := make([]int64, parallelism)
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "inputconverter", int32(flowCtx.NodeID))
		defer tracing.FinishSpan(span)
		return recordInfo.convertRecordToRelational(ctx, flowCtx, spec, fileInfo, tableDesc, 0, minEmited, kvCh)
	})
	// Read data from producer and send it to consumers.
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "inputconverter", int32(flowCtx.NodeID))
		defer tracing.FinishSpan(span)
		defer close(recordInfo.recordCh)
		return readCSVFile(ctx, flowCtx, fileInfo, recordInfo, csvReader, tableDesc, fileSplitInfos[fileInfo.dataFileIdx][0], parallelism, spec.Table.IntoCols)
	})
	return group.Wait()
}

// readCSVFile read data from csv file ,skip rows that have been specified
// by the user or rows that have already been imported.Check if the length
// of the CSV data and the length of the column are the same.Then put record
// to recordBatch.
func readCSVFile(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	fileInfo *importFileInfo,
	importer *importerRecordInfo,
	csvReader csv.Reader,
	tableDesc *sqlbase.TableDescriptor,
	pf partFileInfo,
	parallelism int,
	intoCols []string,
) error {
	var count int64
	conf, err := cloud.ExternalStorageConfFromURI(fileInfo.filename)
	if err != nil {
		return err
	}
	es, err := flowCtx.Cfg.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()
	f, err := es.Seek(ctx, "", pf.start, pf.end, io.SeekStart)
	if err != nil {
		return err
	}
	defer f.Close()
	csvReader.SetReader(ioutil.NopCloser(&byteCounter{r: f}))
	batchSize := fileInfo.batchSize
	rb := &recordBatch{
		data: make([]interface{}, 0, batchSize),
	}
	expectedColsLen := len(tableDesc.VisibleColumnsWithTagCol())
	if len(intoCols) > 0 {
		expectedColsLen = len(intoCols)
	}
	for {
		record, err := csvReader.Read()
		// file read finished
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return err
		}
		count++
		// Skip rows if needed.
		if count <= fileInfo.skip {
			continue
		}
		if len(record) == expectedColsLen {
			// Expected number of columns.
		} else if len(record) == expectedColsLen+1 && record[expectedColsLen] != nil && *record[expectedColsLen] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:expectedColsLen]
		} else {
			return newImportRowError(
				errors.Errorf("expected %d fields, got %d", expectedColsLen, len(record)),
				tree.StrRecord(record, csvReader.Comma),
				count)
		}
		if len(rb.data) == 0 {
			rb.startPos = count
		}
		rb.data = append(rb.data, record)
		if len(rb.data) == int(batchSize) {
			if err = importer.flushBatch(ctx, rb); err != nil {
				return err
			}
			// Clear rb.data after flush.
			rb.data = make([]interface{}, 0, cap(rb.data))
		}
		// Read csv file from shard's Start to shard's end.
		if csvReader.ReadSize() == (pf.end - pf.start) {
			break
		}
	}
	return importer.flushBatch(ctx, rb)
}

// recordBatch represents recordBatch of data to convert.
type recordBatch struct {
	data     []interface{}
	startPos int64
}

// importerRecordInfo is a helper to facilitate running input
// conversion using parallel workers.
type importerRecordInfo struct {
	recordCh chan recordBatch
	rejected chan string
}

// convertRecordToRelational convert record to kvs and send it to kvCh.
func (i *importerRecordInfo) convertRecordToRelational(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	fileInfo *importFileInfo,
	tableDesc *sqlbase.TableDescriptor,
	workerID int,
	minEmitted []int64,
	kvCh chan row.KVBatch,
) error {
	conv, err := row.NewDatumRowConverter(
		ctx, spec, tableDesc, flowCtx.EvalCtx.Copy(), kvCh)
	if err != nil {
		return err
	}
	conv.KvBatch.DataFileIdx = fileInfo.dataFileIdx
	if conv.EvalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	var rowNum int64
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	const precision = uint64(10 * time.Microsecond)
	timestamp := uint64(spec.WalltimeNanos-epoch) / precision
	conv.CompletedRowFn = func() int64 {
		m := emittedRowLowWatermark(workerID, rowNum, minEmitted)
		return m
	}
	for batch := range i.recordCh {
		for batchIdx, record := range batch.data {
			rowNum = batch.startPos + int64(batchIdx)
			if err := fillDatums(record, spec.Format.Csv, rowNum, conv); err != nil {
				return err
			}
			rowIndex := int64(timestamp) + rowNum
			if err := conv.Row(ctx, conv.KvBatch.DataFileIdx, rowIndex); err != nil {
				return newImportRowError(err, fmt.Sprintf("%v", record), rowNum)
			}
		}
	}
	return conv.SendBatch(ctx)
}

// flushBatch flushes currently accumulated data.
func (i *importerRecordInfo) flushBatch(ctx context.Context, rb *recordBatch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case i.recordCh <- *rb:
		return nil
	}
}

func fillDatums(
	row interface{}, opts roachpb.CSVOptions, rowNum int64, conv *row.DatumRowConverter,
) error {
	record := row.([]*string)
	datumIdx := 0

	for i, field := range record {
		// Skip over record entries corresponding to columns not in the target
		// columns specified by the user.
		if _, ok := conv.IsTargetCol[i]; !ok {
			continue
		}
		if field == nil {
			conv.Datums[datumIdx] = tree.DNull
		} else {
			var err error
			conv.Datums[datumIdx], err = sqlbase.ParseDatumStringAs(conv.VisibleColTypes[i], *field, conv.EvalCtx)
			if err != nil {
				col := conv.VisibleCols[i]
				return newImportRowError(
					errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
					tree.StrRecord(record, opts.Comma),
					rowNum)
			}
		}
		datumIdx++
	}
	return nil
}

// importRowError is an error type describing malformed import data.
type importRowError struct {
	err    error
	row    string
	rowNum int64
}

func (e *importRowError) Error() string {
	return fmt.Sprintf("error parsing row %d: %v (row: %q)", e.rowNum, e.err, e.row)
}

func newImportRowError(err error, row string, num int64) error {
	return &importRowError{
		err:    err,
		row:    row,
		rowNum: num,
	}
}

// Updates emitted row for the specified worker and returns
// low watermark for the emitted rows across all workers.
func emittedRowLowWatermark(workerID int, emittedRow int64, minEmitted []int64) int64 {
	atomic.StoreInt64(&minEmitted[workerID], emittedRow)

	for i := 0; i < len(minEmitted); i++ {
		if i != workerID {
			w := atomic.LoadInt64(&minEmitted[i])
			if w < emittedRow {
				emittedRow = w
			}
		}
	}

	return emittedRow
}

// splitCSVFile divides the data file into several logical shards based on
// the number of parallels, and the resulting number of logical shards may
// be slightly different from the specified number of parallels.
func splitCSVFile(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	dataFile string,
	dataFileSize int64,
	parallelNums int64,
) (partFileInfos, error) {
	if parallelNums == 1 {
		return partFileInfos{partFileInfo{start: 0, end: dataFileSize}}, nil
	}
	var start int64
	partSize := dataFileSize / parallelNums
	end := partSize
	var pfs partFileInfos
	for {
		conf, err := cloud.ExternalStorageConfFromURI(dataFile)
		if err != nil {
			return pfs, err
		}
		es, err := flowCtx.Cfg.ExternalStorage(ctx, conf)
		if err != nil {
			return pfs, err
		}
		defer es.Close()
		// Seek file to end offset.
		f, err := es.Seek(ctx, "", end, dataFileSize, io.SeekStart)
		if err != nil {
			return pfs, err
		}
		// ReadUntil read current line util end and return this line end offset.
		reader := csv.NewReader(f)
		end, err = reader.ReadUntil(end)
		if err != nil {
			return pfs, err
		}
		// Record a file shard's start and end int the partFileInfo.
		pf := partFileInfo{start: start, end: end}
		pfs = append(pfs, pf)
		if end == dataFileSize {
			break
		}
		start = end
		if end += partSize; end >= dataFileSize {
			end = dataFileSize
			pf := partFileInfo{start: start, end: end}
			pfs = append(pfs, pf)
			break
		}
	}
	return pfs, nil
}

// partFileInfos record a file's logical shards' info.
type partFileInfos []partFileInfo

// partFileInfo records a logical shard info.
type partFileInfo struct {
	start int64
	end   int64
}

func constructCopyAndRun(
	spec *execinfrapb.ReadImportDataSpec,
	flowCtx *execinfra.FlowCtx,
) (bool, int64, error) {
	if spec.Table.Desc.TableType != tree.ColumnBasedTable {
		return false, 0, nil
	}
	var buf bytes.Buffer
	buf.WriteString("copy ")
	//buf.WriteString(spec.Table.DbName)
	//buf.WriteString(".public.")
	buf.WriteString(spec.Table.Desc.Name)
	buf.WriteString(" from ")
	buf.WriteString("'")
	buf.WriteString(flowCtx.Cfg.Settings.ExternalIODir) /* ".../gitee.com/kwbasedb/kwbase/kwbase-data/extern" */

	// parse file name
	for _, v := range spec.Uri {
		uri, err := url.Parse(v)
		if err != nil {
			return false, 0, err
		}
		buf.WriteString(uri.Path)
	}
	buf.WriteString("' ")

	// copy SQL
	stmt := buf.String()

	var state duck.State
	var res duck.Result
	//db := duck.Database{}

	//path := flowCtx.Cfg.GetAPEngine().GetDBPath() + "/" + spec.Table.DbName
	//state = duck.Open(path, &db)
	//if state != duck.StateSuccess {
	//	return false, 0, errors.New("failed to opend the ap database")
	//}
	//defer duck.Close(&db)
	db := duck.Database{Ptr: flowCtx.Cfg.GetAPEngine().GetDBPtr()}

	if err := flowCtx.Cfg.GetAPEngine().CreateDB(spec.Table.DbName); err != nil {
		return false, 0, err
	}
	connection := duck.Connection{}
	state = duck.Connect(db, &connection)
	if state != duck.StateSuccess {
		return false, 0, errors.New("failed to connect to ap database")
	}
	defer duck.Disconnect(&connection)

	state = duck.Query(connection, "use "+spec.Table.DbName, &res)
	if state != duck.StateSuccess {
		fmt.Println("use database failed")
		errMsg := duck.ResultError(&res)
		return false, 0, pgerror.New(pgcode.Warning, errMsg)
	}

	start := time.Now()
	state = duck.Query(connection, stmt, &res)
	if state != duck.StateSuccess {
		fmt.Println("copy failed")
		errMsg := duck.ResultError(&res)
		return false, 0, pgerror.New(pgcode.Warning, errMsg)
	}
	//rowsCount := 0
	//if err := flowCtx.Cfg.GetAPEngine().ExecSqlForResult(stmt, &rowsCount); err != nil {
	//	return false, 0, err
	//}
	end := time.Now()
	diff := end.Sub(start)
	rowsCount := duck.RowsChanged(&res)

	fmt.Printf("copy %s , use time: %v, rows count: %v", spec.Table.Desc.Name, diff, rowsCount)
	fmt.Println()
	duck.DestroyResult(&res)

	return true, int64(rowsCount), nil
}
