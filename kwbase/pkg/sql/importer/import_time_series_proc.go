// Copyright (c) 2022-present, Shanghai Yunxi Technology Co., Ltd. All rights reserved.
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
	"net/url"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	oid "github.com/lib/pq/oid"
)

// RejectedFilename used to generate <uri/file>.reject by uri datafile
func RejectedFilename(datafile string, nodeID int, prefix int) (string, error) {
	parsedURI, err := url.Parse(datafile)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(parsedURI.Path)
	parsedURI.Path = fmt.Sprintf("%s/reject.n%d.%d.txt", dir, nodeID, prefix)
	return parsedURI.String(), nil
}

// runTimeSeriesImport real timeseries import Entry of sql import
func runTimeSeriesImport(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) (*roachpb.BulkOpSummary, error) {
	parallelNums := int64(spec.Format.Csv.Threads)
	numCPUs := int64(runtime.NumCPU()) * 2
	if parallelNums <= 0 {
		parallelNums = 1
		log.Warningf(ctx, "thread_concurrency <= 0, use 1 instead")
	} else if parallelNums >= numCPUs {
		parallelNums = numCPUs
		log.Warningf(ctx, "thread_concurrency over than nums of cpu, use system core nums %d", numCPUs)
	}
	// Get csv files info and Split csv file.
	_, fileSplitInfos, err := getFileSizeAndSplitFile(ctx, flowCtx, spec, parallelNums)
	if err != nil {
		return nil, err
	}
	// Init prettyCols and ComputeColumnSize.
	tsInfo, err := initPrettyColsAndComputeColumnSize(ctx, spec, flowCtx, fileSplitInfos, parallelNums)
	if err != nil {
		return nil, err
	}
	tsInfo.rejectedHandler = make(chan string, 10)
	gr := ctxgroup.WithContext(ctx)
	gr.GoCtx(func(ctx context.Context) error {
		return tsInfo.saveRejectedToFile(ctx, spec, flowCtx)
	})
	closeChan := make(chan struct{}, int(parallelNums))
	g := ctxgroup.WithContext(ctx)
	stopChan := make(chan bool, int(parallelNums))
	// Read csv file, convert it to datums and split datums by p_tag.
	g.GoCtx(func(ctx context.Context) error {
		defer func() {
			log.Infof(ctx, "[import] read from csv finished")
			close(closeChan)
			for _, ch := range tsInfo.datumsCh {
				close(ch)
			}
			close(stopChan)
		}()
		log.Infof(ctx, "[import] read from csv start")
		return tsInfo.readAndConvertTimeSeriesFiles(ctx, spec, flowCtx, stopChan)
	})
	// Receive datums, generate Payload and flush it to ts engine.
	g.GoCtx(func(ctx context.Context) error {
		defer func() {
			log.Infof(ctx, "[import] write to storage finished")
		}()
		log.Infof(ctx, "[import] write to storage start")
		return tsInfo.ingestDatums(ctx, closeChan)
	})
	err = g.Wait()
	// After waiting for the read-write thread to end, close the fault-tolerant thread
	log.Infof(ctx, "[import] close rejectedHandler")
	close(tsInfo.rejectedHandler)
	err = gr.Wait()
	if tsInfo.hasSwap {
		return nil, errors.Errorf("Import failed due to incorrect delimiter used, or the CSV file: %s, contains line breaks. "+
			"The imported database/table needs to be deleted and REIMPORT with thread_concurrency='1' or correct delimiter.", tsInfo.filename)
	}
	return &roachpb.BulkOpSummary{TimeSeriesCounts: tsInfo.result.seq, RejectedCounts: tsInfo.RejectedCount, AbandonCounts: tsInfo.AbandonCount}, err
}

// getFileSizeAndSplitFile read file info of spec execution and split file into part which option concurrency_thread assigned
func getFileSizeAndSplitFile(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	parallelNums int64,
) (map[int32]int64, map[int32]partFileInfos, error) {
	dataFiles := spec.Uri
	fileSizes := make(map[int32]int64, len(dataFiles))
	fileSplitInfos := make(map[int32]partFileInfos, len(dataFiles))
	for id, dataFile := range dataFiles {
		conf, err := cloud.ExternalStorageConfFromURI(dataFile)
		if err != nil {
			return fileSizes, fileSplitInfos, err
		}
		es, err := flowCtx.Cfg.ExternalStorage(ctx, conf)
		if err != nil {
			return fileSizes, fileSplitInfos, err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			break
		}
		// Split files to logical shards.
		pfs, err := splitCSVFile(ctx, flowCtx, dataFile, sz, parallelNums)
		if err != nil {
			return fileSizes, fileSplitInfos, err
		}
		fileSplitInfos[id] = pfs
		fileSizes[id] = sz
	}
	return fileSizes, fileSplitInfos, nil
}

// timeSeriesImportInfo is infos use throughout import
type timeSeriesImportInfo struct {
	flowCtx *execinfra.FlowCtx

	// infos from sql import and table desc
	colIndexs      map[int]int
	columns        []*sqlbase.ColumnDescriptor
	prettyCols     []*sqlbase.ColumnDescriptor
	primaryTagCols []*sqlbase.ColumnDescriptor
	pArgs          execbuilder.PayloadArgs
	dbID           sqlbase.ID
	tbID           sqlbase.ID
	hashNum        uint64
	txn            *kv.Txn

	// infos generate for fast import, and flags control for batch wait
	fileSplitInfos    map[int32]partFileInfos
	OptimizedDispatch bool
	autoShrink        bool
	logColumnID       int

	batchSize    int64
	parallelNums int64

	// lock for generated datums split to concurrency worker
	mu struct {
		syncutil.Mutex
		seq            int64
		pTagToWorkerID map[string]int64
	}

	// result for concurrency total counts
	result struct {
		syncutil.Mutex
		seq int64
	}
	// channels between readAndConvert -> buildPayloadAndSend
	datumsCh []chan datumsInfo
	// channel for accept rejected rows. from readAndConvert and buildPayloadAndSend
	rejectedHandler chan string
	// results for every spec
	RejectedCount int64
	AbandonCount  int64
	opts          roachpb.CSVOptions
	// for fast type check while converting string to datums ts col which maybe int64 or string.
	tsColTypeMap map[int]oid.Oid
	m1           syncutil.RWMutex
	writeWAL     bool
	// Does the CSV file contain line breaks
	hasSwap  bool
	filename string
}

// datumsInfo channel passed datums between readAndConvert and buildPayloadAndSend. which canbe assaigned by priVal
type datumsInfo struct {
	datums []tree.Datums
	priVal string
}

const defaultBatchSize = 500
const maxRPCPayloadLength = 4 << 30 // 4G

// initPrettyColsAndComputeColumnSize generate Infos from desc, check&compute fixed data
func initPrettyColsAndComputeColumnSize(
	ctx context.Context,
	spec *execinfrapb.ReadImportDataSpec,
	flowCtx *execinfra.FlowCtx,
	fileSplitInfos map[int32]partFileInfos,
	parallelNums int64,
) (*timeSeriesImportInfo, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*spec.Table.Desc)
	columns := immutDesc.VisibleColumnsWithTagColOrdered()
	colIndexs := make(map[int]int, len(columns))
	if len(spec.Table.IntoCols) > 0 {
		var newCols []*sqlbase.ColumnDescriptor
		bMap := make(map[string]int, len(spec.Table.IntoCols))
		// we should keep the order of the target columns here,
		// which is same like csv records' order
		for i, col := range spec.Table.IntoCols {
			bMap[col] = i
		}
		// even if we only import into some columns,
		// we also had to send all cols below
		for _, col := range columns {
			// target columns should set the true idx,
			// others columns should set -1 which means filling them with NULL.
			// If the other columns are NOT NULL, return NewNonNullViolationError in generateDatums below
			if idx, ok := bMap[col.Name]; ok {
				colIndexs[int(col.ID)] = idx
			} else {
				colIndexs[int(col.ID)] = -1
			}
			newCols = append(newCols, col)
		}
		columns = newCols
	} else {
		for i, col := range columns {
			colIndexs[int(col.ID)] = i
		}
	}

	var primaryTagCols, allTagCols, dataCols []*sqlbase.ColumnDescriptor
	haveOtherTag, haveDataCol := false, false

	for i := range columns {
		col := columns[i]
		// check col[0..len(columns)] which contains[data, tag, primary tag which assaigned to tag]
		// special primary tag col is tag col. also need to combine with tag
		if col.IsPrimaryTagCol() {
			primaryTagCols = append(primaryTagCols, col)
		}
		// col only can be data or tag col
		if col.IsTagCol() {
			allTagCols = append(allTagCols, col)
		} else {
			dataCols = append(dataCols, col)
		}
		// colIndexs[colID] = id which is columns[id]'s index
		if _, ok := colIndexs[int(col.ID)]; ok {
			if col.IsTagCol() {
				haveOtherTag = true
			} else {
				haveDataCol = true
			}
		} else {
			// get nothing from colIndexs. means columns's index col.ID had never been initial
			// while getting columns from table desc. and initial col with it .this branch should
			// never been step in. we can assaign every col to data or tag is fine.
			// but if we support not whole table import(column i, j, k from a..z) need this logic
			// we support the function in V2.0.3 soon
			colIndexs[int(col.ID)] = -1
		}
	}
	// in V2.0.2 import should perfectly match every col by tableDesc's columns. every column is needed
	if !haveDataCol {
		dataCols = dataCols[:0]
	}
	if !haveOtherTag && haveDataCol {
		allTagCols = allTagCols[:0]
	}

	// Integrate the arguments required by payload
	pArgs, err := execbuilder.BuildPayloadArgs(
		uint32(immutDesc.GetTsTable().TsVersion),
		primaryTagCols, allTagCols, dataCols,
	)
	if err != nil {
		return nil, err
	}

	autoShrink := false
	if spec.Format.Csv.AutoShrink {
		autoShrink = true
	}
	logColumnID := 0
	// logColumnID := int(spec.Format.Csv.LogColumn)
	// if logColumnID < 0 || logColumnID >= len(columns) {
	// 	log.Warningf(ctx, "valid log column ID, use column[0] to log")
	// 	logColumnID = 0
	// }
	dbID := spec.Table.Desc.ParentID
	tbID := spec.Table.Desc.ID
	hashNum := spec.Table.Desc.TsTable.HashNum
	pTagToWorkerID := make(map[string]int64)
	datumsCh := make([]chan datumsInfo, parallelNums)
	for i := 0; i < int(parallelNums); i++ {
		datumsCh[i] = make(chan datumsInfo, parallelNums)
	}
	txn := flowCtx.Cfg.DB.NewTxn(ctx, `import_ingest_ts_data`)
	singleColSize := execbuilder.PreComputePayloadSize(&pArgs, 1)
	maxBatchSize := int64(maxRPCPayloadLength / singleColSize)
	batchSize := int64(spec.Format.Csv.BatchRows)
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	} else if batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}
	t := &timeSeriesImportInfo{prettyCols: pArgs.PrettyCols, pArgs: pArgs, columns: columns, colIndexs: colIndexs,
		autoShrink: autoShrink, logColumnID: logColumnID, batchSize: batchSize, fileSplitInfos: fileSplitInfos,
		parallelNums: parallelNums, dbID: dbID, tbID: tbID, hashNum: hashNum, flowCtx: flowCtx,
		datumsCh: datumsCh, txn: txn, primaryTagCols: primaryTagCols,
		OptimizedDispatch: spec.OptimizedDispatch, writeWAL: spec.WriteWAL}
	t.mu.pTagToWorkerID = pTagToWorkerID
	t.tsColTypeMap = make(map[int]oid.Oid, pArgs.PTagNum+pArgs.AllTagNum+pArgs.DataColNum)
	return t, err
}

// readAndConvertTimeSeriesFiles concurrency worker maker
func (t *timeSeriesImportInfo) readAndConvertTimeSeriesFiles(
	ctx context.Context,
	spec *execinfrapb.ReadImportDataSpec,
	flowCtx *execinfra.FlowCtx,
	stopChan chan bool,
) error {
	dataFiles := spec.Uri
	done := ctx.Done()
	t.opts = spec.Format.Csv
	for dataFileIdx, dataFile := range dataFiles {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if err := ctxgroup.GroupWorkers(ctx, len(t.fileSplitInfos[dataFileIdx]), func(ctx context.Context, id int) error {
			return t.readAndConvertTimeSeriesFile(ctx, flowCtx, dataFile, t.fileSplitInfos[dataFileIdx][id], spec.Table.IntoCols, stopChan)
		}); err != nil {
			if t.filename != "" {
				log.Infof(ctx, "The CSV file: %s, contains line breaks, so import failed", t.filename)
				return nil
			}
			log.Infof(ctx, "[import] read file failed,file %s, errmsg %s", dataFile, err.Error())
		}
	}
	return nil
}

// saveRejectedToFile write to rejected files while accept err rows by importing
func (t *timeSeriesImportInfo) saveRejectedToFile(
	ctx context.Context, spec *execinfrapb.ReadImportDataSpec, flowCtx *execinfra.FlowCtx,
) error {
	oneOfURI := ""
	rejectedCount := int64(0)
	for _, fileName := range spec.Uri {
		if fileName != "" {
			oneOfURI = fileName
			break
		}
	}
	if oneOfURI == "" {
		return errors.Newf("get file from uri failed")
	}

	idx := 0
	var rejectRecoder cloud.ExternalStorage
	// switch rejected file writer
	reOpenRejectRecorder := func() error {
		// rejectRecoder.Close()
		rejectFile, err := RejectedFilename(oneOfURI, int(flowCtx.NodeID), idx)
		if err != nil {
			return err
		}
		rejectconf, err := cloud.ExternalStorageConfFromURI(rejectFile)
		if err != nil {
			return err
		}
		rejectRecoder, err = flowCtx.Cfg.ExternalStorage(ctx, rejectconf)
		if err != nil {
			return err
		}
		return nil
	}
	if err := reOpenRejectRecorder(); err != nil {
		return err
	}
	defer rejectRecoder.Close()
	var buf []byte
	for s := range t.rejectedHandler {
		if len(s) > 0 {
			buf = append(buf, s...)
			rejectedCount++
		}
		if len(buf) > defaultRejectedSize {
			if err := rejectRecoder.WriteFile(ctx, "", bytes.NewReader(buf)); err != nil {
				return err
			}
			idx++
			rejectRecoder.Close()
			if err := reOpenRejectRecorder(); err != nil {
				return err
			}
			buf = []byte{}
		}
	}
	if len(buf) > 0 {
		if err := rejectRecoder.WriteFile(ctx, "", bytes.NewReader(buf)); err != nil && len(buf) > 0 {
			return err
		}
	}
	t.RejectedCount = rejectedCount / 2
	return nil
}

// setType record colID type for fast convert
func (t *timeSeriesImportInfo) setType(id int, val oid.Oid) {
	t.m1.Lock()
	defer t.m1.Unlock()
	t.tsColTypeMap[id] = val
}

// getType read ts col type for fast convert
func (t *timeSeriesImportInfo) getType(id int) (oid.Oid, bool) {
	t.m1.RLock()
	defer t.m1.RUnlock()
	oid, ok := t.tsColTypeMap[id]
	return oid, ok
}

// recordBatch represents recordBatch of data to convert.
type recordDatums struct {
	datumsMap map[string][]tree.Datums
	count     int64
	batchSize int64
}

// append: add datums elements by priVal
func (r *recordDatums) append(datums tree.Datums, priVal string) {
	datumSlice, ok := r.datumsMap[priVal]
	if !ok {
		// TODO
		r.datumsMap[priVal] = make([]tree.Datums, 0, r.batchSize)
	}
	datumSlice = append(datumSlice, datums)
	r.datumsMap[priVal] = datumSlice
	r.count++
}

// flush: send or assagin primaryKey mapped datums to t.workerID.
// guarantee only one thread handle the primaryKey's datums. which
// also cause the worker may be starved when too much primaryKey in csv's batch data
func (r *recordDatums) flush(ctx context.Context, t *timeSeriesImportInfo) {
	for priVal, datums := range r.datumsMap {
		t.mu.Lock()
		workerID, ok := t.mu.pTagToWorkerID[priVal]
		if !ok {
			workerID = t.mu.seq % t.parallelNums
			t.mu.seq++
			t.mu.pTagToWorkerID[priVal] = workerID
		}
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			for _, datum := range datums {
				t.handleCoruptedResult(ctx, tree.ConvertDatumsToStr(datum, ','), ctx.Err())
			}
		default:
			t.datumsCh[workerID] <- datumsInfo{datums: datums, priVal: priVal}
		}
	}

	r.datumsMap = make(map[string][]tree.Datums)
	r.count = 0
}

// SetCsvOpt apply opt in roachpb params to csvReader
func SetCsvOpt(csvReader *csv.Reader, opts roachpb.CSVOptions) {
	if opts.Comma != 0 {
		csvReader.Comma = opts.Comma
	}
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
}

const (
	minimalBatchSize = 10000
	durationShrink   = 10
)

// readCSVFile read data from csv file ,skip rows that have been specified
// by the user or rows that have already been imported.Check if the length
// of the CSV data and the length of the column are the same.Then put record
// to recordBatch.
func (t *timeSeriesImportInfo) readAndConvertTimeSeriesFile(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	filename string,
	pf partFileInfo,
	intoCols []string,
	stopChan chan bool,
) error {
	var count int64
	conf, err := cloud.ExternalStorageConfFromURI(filename)
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
	fileReader := &fileReader{Reader: ioutil.NopCloser(&byteCounter{r: f}), total: pf.start - pf.end, counter: byteCounter{r: f}}
	csvReader := csv.NewReader(fileReader)
	SetCsvOpt(csvReader, t.opts)
	rb := &recordDatums{
		datumsMap: make(map[string][]tree.Datums),
		batchSize: t.batchSize,
	}
	expectedColsLen := len(t.columns)
	tickDuration := durationShrink * time.Second
	tick := time.NewTimer(tickDuration)
	avgBatchSize := int64(t.batchSize)
	everFlushed := false
	if len(intoCols) > 0 {
		expectedColsLen = len(intoCols)
	}
	for {
		select {
		case <-stopChan:
			return errors.Errorf("Other thread processed contains line breaks, stop all threads")
		default:
		}
		record, err := csvReader.Read()
		if csvReader.HasSwap {
			log.Warningf(ctx, "The line being processed by the current thread [%d] contains line breaks. "+
				"Stop all threads of IMPORT")
			t.hasSwap = true
			t.filename = filename
			stopChan <- true
			return errors.Errorf("The thread processed contains line breaks, stop all threads")
		}
		// file read finished
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			row := tree.StrRecord(record, ',')
			t.handleCoruptedResult(ctx, row, err)
			continue
		}
		count++
		if len(record) == expectedColsLen {
			// Expected number of columns.
		} else if len(record) == expectedColsLen+1 && record[expectedColsLen] != nil && *record[expectedColsLen] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:expectedColsLen]
		} else {
			row := tree.StrRecord(record, ',')
			t.handleCoruptedResult(ctx, row, newImportRowError(
				errors.Errorf("expected %d fields, got %d", expectedColsLen, len(record)),
				tree.StrRecord(record, csvReader.Comma),
				count))
			continue
		}
		// Covert record to datums and check data invalid.
		datums, priVal, err := t.generateDatums(flowCtx.EvalCtx, record)
		if err != nil {
			row := tree.StrRecord(record, ',')
			select {
			case <-ctx.Done():
				log.Warningf(ctx, "[import] context canceld while convert row %s", row)
				return nil
			default:
			}
			if errors.IsAny(err,
				context.DeadlineExceeded,
				context.Canceled) {
				log.Warningf(ctx, "[import] context canceld after convert row %s", row)
				return nil
			}
			// If rowsVal fail to generate payload, write rowsVal to the reject file.
			t.handleCoruptedResult(ctx, row, err)
		} else {
			rb.append(datums, priVal)
		}
		if rb.count > avgBatchSize {
			everFlushed = true
			rb.flush(ctx, t)
		}
		select {
		case <-tick.C:
			if avgBatchSize > minimalBatchSize && !everFlushed && t.autoShrink {
				avgBatchSize = (avgBatchSize + rb.count) / durationShrink
				t.batchSize = int64(avgBatchSize)
			}
			tick.Reset(tickDuration)
		default:
		}
		// Read csv file from shard's Start to shard's end.
		if csvReader.ReadSize() == (pf.end - pf.start) {
			break
		}
	}
	rb.flush(ctx, t)
	return nil
}

const oneByteRows = 8

// addResultCount add results for handleDedupResp
func (t *timeSeriesImportInfo) addResultCount(count int64) {
	atomic.AddInt64(&t.result.seq, count)
}

func (t *timeSeriesImportInfo) handleDedupResp(
	ctx context.Context,
	resp interface{},
	isResp bool,
	reqCount int64,
	datums []tree.Datums,
	priVal string,
) {
	var ruleType, succeedCount int64
	//var rowBitMap []byte
	if isResp {
		resp := resp.(*roachpb.TsRowPutResponse)
		ruleType, succeedCount, _ = resp.DedupRule, resp.Header().NumKeys, resp.DiscardBitmap
	} else {
		resp := resp.(tse.DedupResult)
		ruleType, succeedCount, _ = int64(resp.DedupRule), reqCount-int64(resp.DedupRows), resp.DiscardBitmap
	}
	switch ruleType {
	case int64(execinfrapb.DedupRule_TsReject):
		// reject dedup rule: all rows will abandon
		if succeedCount != 0 {
			t.addResultCount(reqCount)
			break
		}
		atomic.AddInt64(&t.AbandonCount, reqCount)
		// TODO(yangshuai):Evenly distributed module repair bitmap, Re record logs
		//for _ = range datums {
		//cols := datums[i]
		//t.recordAbandondDatums(ctx, priVal, cols, t.logColumnID, errors.Newf("import while Dedup reject"))
		//}
	case int64(execinfrapb.DedupRule_TsDiscard):
		// discard dedup rule: only map with bit 1 was abandon
		t.addResultCount(succeedCount)
		if succeedCount != reqCount {
			atomic.AddInt64(&t.AbandonCount, reqCount-succeedCount)
		}
		// TODO(yangshuai):Evenly distributed module repair bitmap, Re record logs
		//id := 0
		//for ok := range rowBitMap {
		//	if eightRow := rowBitMap[ok]; eightRow != 0 {
		//		binaryStr := fmt.Sprintf("%b", eightRow)
		//		for _, miniRowOk := range binaryStr {
		//			if miniRowOk == '1' {
		//				cols := datums[id]
		//				reqCount--
		//				t.recordAbandondDatums(ctx, priVal, cols, t.logColumnID, errors.Newf("import while Dedup discard"))
		//			}
		//			id++
		//		}
		//	} else {
		//		id += oneByteRows
		//	}
		//}
		//t.addResultCount(reqCount)
	default:
		// unknown dedup rule. only record all rows has been send to storage
		t.addResultCount(reqCount)
	}
}

// handleCoruptedResult record row to rejected file while dedup from storage
func (t *timeSeriesImportInfo) handleCoruptedResult(ctx context.Context, row string, err error) {
	log.Errorf(ctx, "[import] errors occurred while importing {data: %s, errmsg: %s", row, err.Error())
	select {
	case <-ctx.Done():
		return
	default:
	}
	if errors.IsAny(err,
		context.DeadlineExceeded,
		context.Canceled) {
		return
	}
	t.rejectedHandler <- row + "\n"
	t.rejectedHandler <- err.Error() + "\n"
}

func (t *timeSeriesImportInfo) ingest(
	ctx context.Context, datums []tree.Datums, workerID int,
) error {
	if len(datums) == 0 {
		return nil
	}

	// single-node KW_ENGINE_VERSION=1 (列存)(默认值为1)
	if t.flowCtx.EvalCtx.StartSinglenode && t.flowCtx.EvalCtx.Kwengineversion == "1" {
		payload, primaryTagVal, tolerantErr, err := t.BuildPayloadForTsImportStartSingleNode(
			t.flowCtx.EvalCtx,
			t.txn,
			datums,
			len(datums),
		)

		if err != nil {
			for i := range datums {
				cols := datums[i]
				rowString := tree.ConvertDatumsToStr(cols, ',')
				t.handleCoruptedResult(ctx, rowString, err)
			}
			return err
		}

		if len(tolerantErr) > 0 {
			for _, rowErrmap := range tolerantErr {
				for k, v := range rowErrmap.(map[string]error) {
					t.handleCoruptedResult(ctx, k, v)
				}
			}
		}
		resp, _, err := t.flowCtx.Cfg.TsEngine.PutData(uint64(t.tbID), [][]byte{payload}, uint64(0), t.writeWAL, nil)
		if err != nil {
			for i := range datums {
				cols := datums[i]
				rowString := tree.ConvertDatumsToStr(cols, ',')
				t.handleCoruptedResult(ctx, rowString, err)
			}
			return err
		}
		t.handleDedupResp(ctx, resp, false, int64(len(datums)), datums, string(primaryTagVal))
		return err
	}

	payloadNodeMap, tolerantErr, err := t.BuildPayloadForTsImportStartDistributeMode(
		t.flowCtx.EvalCtx,
		t.txn,
		datums,
		len(datums),
	)
	if err != nil {
		for i := range datums {
			cols := datums[i]
			rowString := tree.ConvertDatumsToStr(cols, ',')
			t.handleCoruptedResult(ctx, rowString, err)
		}
		return err
	}

	if len(tolerantErr) > 0 {
		for _, rowErrmap := range tolerantErr {
			for k, v := range rowErrmap.(map[string]error) {
				t.handleCoruptedResult(ctx, k, v)
			}
		}
	}

	// start && single-node KW_ENGINE_VERSION=2 (行存)
	if t.flowCtx.EvalCtx.StartSinglenode && t.flowCtx.EvalCtx.Kwengineversion == "2" {
		for _, val := range payloadNodeMap[int(t.flowCtx.EvalCtx.NodeID)].PerNodePayloads {
			resp, _, err := t.flowCtx.Cfg.TsEngine.PutData(uint64(t.tbID), [][]byte{val.Payload}, uint64(0), t.writeWAL, nil)
			if err != nil {
				for i := range datums {
					cols := datums[i]
					rowString := tree.ConvertDatumsToStr(cols, ',')
					t.handleCoruptedResult(ctx, rowString, err)
				}
				return err
			}
			t.handleDedupResp(ctx, resp, false, int64(len(datums)), datums, string(val.PrimaryTagKey))
			return err
		}
	}

	ba := t.txn.NewBatch()
	for _, val := range payloadNodeMap[int(t.flowCtx.EvalCtx.NodeID)].PerNodePayloads {
		ba.AddRawRequest(&roachpb.TsRowPutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    val.StartKey,
				EndKey: val.EndKey,
			},
			HeaderPrefix: val.Payload,
			Values:       val.RowBytes,
			Timestamps:   val.RowTimestamps,
			ValueSize:    val.ValueSize,
			CloseWAL:     !t.writeWAL,
			HashNum:      t.hashNum,
		})
		err = t.flowCtx.Cfg.TseDB.Run(ctx, ba)
		if err != nil {
			for i := range datums {
				cols := datums[i]
				rowString := tree.ConvertDatumsToStr(cols, ',')
				t.handleCoruptedResult(ctx, rowString, err)
			}
			return err
		}
		for respsID := range ba.RawResponse().Responses {
			resp := ba.RawResponse().Responses[respsID].GetInner().(*roachpb.TsRowPutResponse)
			t.handleDedupResp(ctx, resp, true, int64(len(datums)), datums, string(val.PrimaryTagKey))
		}
	}

	return nil
}

func (t *timeSeriesImportInfo) generateDatums(
	evalCtx tree.ParseTimeContext, record []*string,
) (tree.Datums, string, error) {
	inputDatums := make([]tree.Datum, len(t.columns))
	var priVal string
	var err error
	for i, col := range t.prettyCols {
		valIdx := t.colIndexs[int(col.ID)]
		if valIdx < 0 {
			// not input this col, but col is not nullable will occurred error
			// while in csv. appointed column with which cannot be null. returned anyway
			if !col.Nullable && valIdx == -1 {
				return nil, "", sqlbase.NewNonNullViolationError(col.Name)
			}
			continue
		}
		field := record[valIdx]
		// checks whether the input value is valid for target column.
		if field == nil {
			if col.Nullable {
				inputDatums[valIdx] = tree.DNull
			} else {
				return nil, priVal, errors.Errorf("column[%d] cannot be null", valIdx)
			}
		} else {
			switch (col.Type).Family() {
			case types.StringFamily, types.TimestampTZFamily:
				// for fast convert column TimestampTZ string to datums. get from record . if ever record . use fast convert int64/string to convert col
				if v, ok := t.getType(valIdx); ok && v != 0 {
					typ := types.OidToType[v]
					switch typ.Family() {
					case types.IntFamily:
						t1, err1 := strconv.ParseInt(*field, 10, 64)
						if err1 != nil {
							inputDatums[valIdx], err = tree.TSParseAndRequireString(col.Name, &col.Type, *field, evalCtx)
							t.setType(valIdx, inputDatums[valIdx].ResolvedType().Oid())
						} else {
							inputDatums[valIdx] = (*tree.DInt)(&t1)
						}
					default:
						inputDatums[valIdx], err = tree.TSParseAndRequireString(col.Name, &col.Type, *field, evalCtx)
					}
				} else {
					inputDatums[valIdx], err = tree.TSParseAndRequireString(col.Name, &col.Type, *field, evalCtx)
					t.setType(valIdx, inputDatums[valIdx].ResolvedType().Oid())
				}
			default:
				inputDatums[valIdx], err = tree.TSParseAndRequireString(col.Name, &col.Type, *field, evalCtx)
			}
		}
		if err != nil {
			return nil, priVal, err
		}
		if i < len(t.primaryTagCols) {
			priVal += sqlbase.DatumToString(inputDatums[valIdx])
		}
	}
	return inputDatums, priVal, err
}

// recordAbandondDatums log abandoned datums to log file and record count of abandoned rows
func (t *timeSeriesImportInfo) recordAbandondDatums(
	ctx context.Context, primaryKey string, start tree.Datums, otherkeyID int, err error,
) {
	var starts string
	if !reflect.ValueOf(start[otherkeyID]).IsNil() {
		starts = start[otherkeyID].String()
	}
	log.Errorf(ctx, "[import]write to storage error, err:%s {data:{%s***%s}}",
		err.Error(),
		primaryKey, starts)
	atomic.AddInt64(&t.AbandonCount, 1)
}

// ingestDatums must be closed after read goroutine, otherwise cannot exit
func (t *timeSeriesImportInfo) ingestDatums(ctx context.Context, closeChan chan struct{}) error {
	return ctxgroup.GroupWorkers(ctx, int(t.parallelNums), func(ctx context.Context, i int) error {
		defer func() {
			log.Infof(ctx, "[import] write to storage goroutine [%d] finished\n", i)
		}()
		datumsChan := t.datumsCh[i]
		tickDuration := durationShrink * time.Second
		tick := time.NewTimer(tickDuration)
		datumsMap := make(map[string][]tree.Datums)
		avgBatchSize := int(t.batchSize)
		for {
			select {
			case datumsInfo := <-datumsChan:
				// every time get datums from convert string to datum finished. assign to target priValrows. if need send to storage. then send.
				// otherwise put datums to map[priVal]
				datumSlice, ok := datumsMap[datumsInfo.priVal]
				if !ok {
					datumSlice = make([]tree.Datums, 0, t.batchSize)
				}
				datumSlice = append(datumSlice, datumsInfo.datums...)
				// fmt.Printf("datumSlice len %d\n",len(datumSlice))
				if len(datumSlice) > avgBatchSize {
					// log.Infof(ctx, "import debug info t.ingest by count %d", len(datumSlice))
					if err := t.ingest(ctx, datumSlice, i); err != nil {
						var starts, ends string
						if !reflect.ValueOf(datumSlice[0][t.logColumnID]).IsNil() {
							starts = datumSlice[0][t.logColumnID].String()
						}
						if !reflect.ValueOf(datumSlice[len(datumSlice)-1][t.logColumnID]).IsNil() {
							ends = datumSlice[len(datumSlice)-1][t.logColumnID].String()
						}
						log.Errorf(ctx, "[import]write to storage error, err:%s {data:{%s***%s}...{%s****%s} len: %d}",
							err.Error(),
							datumsInfo.priVal, starts,
							datumsInfo.priVal, ends,
							len(datumSlice))
					}
					datumSlice = make([]tree.Datums, 0, t.batchSize)
					tick.Reset(tickDuration)
				}
				datumsMap[datumsInfo.priVal] = datumSlice
				if len(datumsMap) > avgBatchSize {
					// ptag more than batch, every batch data maybe 1. make [][]payload to write
					for ptag, datums := range datumsMap {
						if len(datums) > 0 {
							if err := t.ingest(ctx, datums, i); err != nil {
								var starts, ends string
								if !reflect.ValueOf(datumSlice[0][t.logColumnID]).IsNil() {
									starts = datumSlice[0][t.logColumnID].String()
								}
								if !reflect.ValueOf(datumSlice[len(datumSlice)-1][t.logColumnID]).IsNil() {
									ends = datumSlice[len(datumSlice)-1][t.logColumnID].String()
								}
								log.Errorf(ctx, "[import]write to storage error, err:%s {data:{%s***%s}...{%s****%s} len: %d}",
									err.Error(),
									datumsInfo.priVal, starts,
									datumsInfo.priVal, ends,
									len(datumSlice))
							}
							datumsMap[ptag] = make([]tree.Datums, 0, t.batchSize)
						}
					}
				}
				if t.result.seq != 0 && t.result.seq/int64(avgBatchSize) == 0 {
					log.Infof(ctx, "%d rows has been write to storage", t.result.seq)
				}
			case <-tick.C:
				// 10s never send to storage. send all ptag datums ever got. and shrink batch by flag
				tickBatch := 0
				for priVal, datums := range datumsMap {
					datumsMap[priVal] = make([]tree.Datums, 0, len(datums))
					if len(datums) > 0 {
						tickBatch += len(datums)
						if err := t.ingest(ctx, datums, i); err != nil {
							var starts, ends string
							if !reflect.ValueOf(datums[0][t.logColumnID]).IsNil() {
								starts = datums[0][t.logColumnID].String()
							}
							if !reflect.ValueOf(datums[len(datums)-1][t.logColumnID]).IsNil() {
								ends = datums[len(datums)-1][t.logColumnID].String()
							}
							log.Errorf(ctx, "[import]write to storage error, err:%s {data:{%s***%s}...{%s****%s} len: %d}",
								err.Error(),
								priVal, starts,
								priVal, ends,
								len(datums))
						}
					}
				}
				if avgBatchSize > durationShrink && t.autoShrink {
					avgBatchSize = (avgBatchSize + tickBatch) / durationShrink
					tickBatch = 0
					t.batchSize = int64(avgBatchSize)
					log.Infof(ctx, "import debug info shrink batch, avgBatchSize=%d", avgBatchSize)
				}
				tick.Reset(tickDuration)
			case <-ctx.Done():
				return ctx.Err()
			case <-closeChan:
				// all datums read finished. which may remain in channel. get and send to storage
				for datumsInfo := range datumsChan {
					datumSlice, ok := datumsMap[datumsInfo.priVal]
					if !ok {
						datumSlice = make([]tree.Datums, 0, t.batchSize)
					}
					datumSlice = append(datumSlice, datumsInfo.datums...)
					datumsMap[datumsInfo.priVal] = datumSlice
				}
				for priVal, datums := range datumsMap {
					if err := t.ingest(ctx, datums, i); err != nil {
						var starts, ends string
						if !reflect.ValueOf(datums[0][t.logColumnID]).IsNil() {
							starts = datums[0][t.logColumnID].String()
						}
						if !reflect.ValueOf(datums[len(datums)-1][t.logColumnID]).IsNil() {
							ends = datums[len(datums)-1][t.logColumnID].String()
						}
						log.Errorf(ctx, "[import]write to storage error, err:%s {data:{%s***%s}...{%s****%s} len: %d}",
							err.Error(),
							priVal, starts,
							priVal, ends,
							len(datums))
					}
				}
				return nil
			}
		}
	})
}

// BuildPayloadForTsImportStartSingleNode construct payload for StartSingleNode import ts table.
func (t *timeSeriesImportInfo) BuildPayloadForTsImportStartSingleNode(
	evalCtx *tree.EvalContext, txn *kv.Txn, InputDatums []tree.Datums, rowNum int,
) ([]byte, []byte, []interface{}, error) {
	tsPayload := execbuilder.NewTsPayload()
	tsPayload.SetArgs(t.pArgs)
	tsPayload.SetHeader(execbuilder.PayloadHeader{
		TxnID:          txn.ID(),
		PayloadVersion: t.pArgs.PayloadVersion,
		DBID:           uint32(t.dbID),
		TBID:           uint64(t.tbID),
		TSVersion:      t.pArgs.TSVersion,
		RowNum:         uint32(rowNum),
	})

	return tsPayload.BuildRowsPayloadByDatums(InputDatums, rowNum, t.prettyCols, t.colIndexs, true, t.hashNum)
}

// BuildPayloadForTsImportStartDistributeMode construct payload of for StartDistributeMode import ts table.
func (t *timeSeriesImportInfo) BuildPayloadForTsImportStartDistributeMode(
	evalCtx *tree.EvalContext, txn *kv.Txn, InputDatums []tree.Datums, rowNum int,
) (map[int]*sqlbase.PayloadForDistTSInsert, []interface{}, error) {
	tsPayload := execbuilder.NewTsPayload()
	tsPayload.SetArgs(t.pArgs)
	tsPayload.SetHeader(execbuilder.PayloadHeader{
		TxnID:          txn.ID(),
		PayloadVersion: t.pArgs.PayloadVersion,
		DBID:           uint32(t.dbID),
		TBID:           uint64(t.tbID),
		TSVersion:      t.pArgs.TSVersion,
		RowNum:         uint32(rowNum),
	})

	return tsPayload.BuildRowBytesForTsImport(evalCtx, txn, InputDatums, rowNum, t.prettyCols, t.colIndexs, t.pArgs, uint32(t.dbID), uint32(t.tbID), t.hashNum, true)
}
