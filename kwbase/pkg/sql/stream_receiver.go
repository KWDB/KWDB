// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
)

const (
	// streamInsertBatch defines the number of rows inserted per batch.
	streamInsertBatch = 1000
	// streamInsertBatch defines the number of rows inserted per small batch.
	streamInsertSmallBatch = 100
	// initialEvalDelay is the duration to wait after receiving the first data point
	// before performing the initial evaluation.
	initialEvalDelay = 3 * time.Second
	// flushInterval is the flush interval.
	flushInterval = 100 * time.Millisecond

	// ParametersLimitation is the max parameters in BIND message of postgresql protocol(uint16)
	ParametersLimitation = 65535
)

// StreamReceiver is a thin wrapper around a RowContainer.
type StreamReceiver struct {
	// metadata contains stream metadata.
	metadata *cdcpb.StreamMetadata
	// parameters contains stream parameters.
	parameters *sqlutil.StreamParameters
	// options contains stream options.
	options *sqlutil.ParsedStreamOptions
	// resultTypes is the types of output types of source table.
	resultTypes []types.T
	// targetTypes is the types of target table.
	targetTypes []types.T

	// ctx is context.
	ctx context.Context
	// stopper used to manage goroutines.
	stopper *stop.Stopper
	// mutex used to lock rowContainer.
	mutex syncutil.RWMutex
	// rowContainer is the cache of data read to insert to target table.
	rowContainer *rowcontainer.RowContainer
	// rowsAffected is the number of successfully written rows.
	rowsAffected int
	// err save the error.
	err error

	// executor used to insert data.
	executor *InternalExecutor
	// flushCh executes a flush after receiving a signal.
	flushCh chan bool
	// cancel used cancel context.
	cancel func()

	// targetTable is target table name.
	targetTable string
	// targetColNames is target column name list.
	targetColNames []string
	// singleInsertStmt is the SQL for insert one row.
	singleInsertStmt string
	// batchInsertStmt is the SQL for insert batch rows.
	batchInsertStmt string
	// batchInsertStmt is the SQL for insert small batch rows.
	smallBatchInsertStmt string

	// originalLowWaterMark is original low-watermark.
	originalLowWaterMark int64
	// currentLowWaterMark is current low-watermark.
	currentLowWaterMark int64
	// batchSize is current batchsize
	batchSize int
	// evalDone define first eval
	evalOnce sync.Once
	// checkpointRowCount count rows of one checkpoint period.
	checkpointRowCount int
	// checkpointRowCount count rows of one flush period.
	flushRowCount int
}

// NewStreamResultWriter creates a new StreamReceiver.
func NewStreamResultWriter(
	ctx context.Context,
	metadata *cdcpb.StreamMetadata,
	parameters *sqlutil.StreamParameters,
	resultTypes []types.T,
	targetTypes []types.T,
	rowContainer *rowcontainer.RowContainer,
	execCfg *ExecutorConfig,
	stopper *stop.Stopper,
) *StreamReceiver {
	resultWriter := StreamReceiver{
		ctx:          ctx,
		metadata:     metadata,
		parameters:   parameters,
		resultTypes:  resultTypes,
		targetTypes:  targetTypes,
		rowContainer: rowContainer,
		executor:     execCfg.InternalExecutor,
		stopper:      stopper}

	resultWriter.init()
	return &resultWriter
}

func (srw *StreamReceiver) init() {
	srw.flushCh = make(chan bool, 2)

	ctx, cancel := context.WithCancel(srw.ctx)
	srw.cancel = cancel

	srw.options, _ = sqlutil.ParseStreamOpts(&srw.parameters.Options)

	srw.targetTable = fmt.Sprintf("%s.%s", srw.parameters.TargetTable.Database, srw.parameters.TargetTable.Table)
	srw.singleInsertStmt = constructBatchInsertStmt(srw.targetTable, srw.parameters.TargetTable.ColNames, 1)
	srw.batchInsertStmt = constructBatchInsertStmt(srw.targetTable, srw.parameters.TargetTable.ColNames, streamInsertBatch)
	srw.smallBatchInsertStmt = constructBatchInsertStmt(srw.targetTable, srw.parameters.TargetTable.ColNames, streamInsertSmallBatch)

	if srw.options.LowLatency {
		srw.batchSize = 1
	} else {
		srw.batchSize = streamInsertBatch
	}

	srw.checkpointRowCount = 0
	srw.flushRowCount = 0

	if err := srw.stopper.RunAsyncTask(ctx, "stream-receiver-async-task", func(ctx context.Context) {
		var checkpointTimer timeutil.Timer
		defer checkpointTimer.Stop()
		checkpointTimer.Reset(srw.options.CheckpointInterval)

		var flushTimer timeutil.Timer
		defer flushTimer.Stop()
		// low-latency mode start
		if srw.options.LowLatency {
			flushTimer.Reset(flushInterval)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case flush, _ := <-srw.flushCh:
				if flush {
					err := srw.flush()
					if err != nil {
						log.Errorf(srw.ctx, "failed to flush stream results with error: %v", err)
						srw.err = err
						return
					}
				}
			case <-checkpointTimer.C:
				checkpointTimer.Read = true
				if err := srw.checkpoint(); err != nil {
					log.Errorf(srw.ctx, "stream receiver checkpoint error: %v", err)
					srw.err = err
					return
				}
				checkpointTimer.Reset(srw.options.CheckpointInterval)
			case <-flushTimer.C:
				srw.mutex.RLock()
				currentCount := srw.rowContainer.Len()
				srw.mutex.RUnlock()
				flushTimer.Read = true
				// If no new data is received for two consecutive cycles, flush all data.
				if currentCount > 0 && currentCount < srw.batchSize && currentCount == srw.flushRowCount {
					if err := srw.flush(); err != nil {
						log.Errorf(srw.ctx, "stream receiver flush error: %v", err)
						srw.err = err
						return
					}
					srw.flushRowCount = 0
				} else {
					srw.flushRowCount = currentCount
				}
				flushTimer.Reset(flushInterval)
			}
		}
	}); err != nil {
		log.Errorf(ctx, "stream receiver async task error: %s", err)
		srw.err = err
		srw.cancel()
	}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (srw *StreamReceiver) IncrementRowsAffected(n int) {
	srw.rowsAffected += n
}

// AddPGResult implements the rowResultWriter interface.
func (srw *StreamReceiver) AddPGResult(_ context.Context, _ []byte) error {
	return nil
}

// IsCommandResult implements the rowResultWriter interface.
func (srw *StreamReceiver) IsCommandResult() bool {
	return false
}

// AddRow implements the rowResultWriter interface.
func (srw *StreamReceiver) AddRow(ctx context.Context, row tree.Datums) error {
	if srw.err != nil {
		return srw.err
	}

	if row == nil {
		return nil
	}

	srw.evalOnce.Do(func() {
		time.AfterFunc(initialEvalDelay, func() {
			select {
			case <-srw.ctx.Done():
				return
			default:
				srw.evaluateBatchSize()
			}
		})
	})

	var send bool
	srw.mutex.Lock()
	srw.checkpointRowCount++
	if srw.batchSize == 1 && srw.rowContainer.Len() == 0 {
		err := srw.writeRow(ctx, row)
		srw.mutex.Unlock()
		return err
	}

	if _, err := srw.rowContainer.AddRow(ctx, row); err != nil {
		srw.mutex.Unlock()
		return err
	}

	send = srw.rowContainer.Len()%srw.batchSize == 0
	srw.mutex.Unlock()

	if send {
		srw.flushCh <- true
	}

	return nil
}

// evaluateBatchSize evaluates the batch size of the current checkpoint period.
func (srw *StreamReceiver) evaluateBatchSize() {
	srw.mutex.RLock()
	defer srw.mutex.RUnlock()

	if srw.checkpointRowCount == 0 {
		return
	}

	originSize := srw.batchSize
	switch {
	case srw.checkpointRowCount >= streamInsertBatch &&
		len(srw.resultTypes)*streamInsertBatch < ParametersLimitation:
		// When the data volume within the checkpoint period exceeds streamInsertBatch
		// and the parameter does not exceed the limit of 65535, set the batch size to streamInsertBatch.
		srw.batchSize = streamInsertBatch
	case srw.checkpointRowCount >= streamInsertSmallBatch &&
		len(srw.resultTypes)*streamInsertSmallBatch < ParametersLimitation:
		// When the data volume within the checkpoint period is between streamInsertSmallBatch and streamInsertBatch,
		// and the parameter does not exceed the limit of 65535, set the batch size to streamInsertSmallBatch.
		srw.batchSize = streamInsertSmallBatch
	default:
		srw.batchSize = 1
	}

	if originSize != srw.batchSize {
		log.Infof(srw.ctx, "batch size evaluated: %d (rows in checkpoint: %d)", srw.batchSize, srw.checkpointRowCount)
	}

	srw.checkpointRowCount = 0
}

// SetError is part of the rowResultWriter interface.
func (srw *StreamReceiver) SetError(err error) {
	srw.err = err
}

// Err is part of the rowResultWriter interface.
func (srw *StreamReceiver) Err() error {
	return srw.err
}

// Close the StreamReceiver.
func (srw *StreamReceiver) Close() {
	srw.mutex.Lock()
	defer srw.mutex.Unlock()

	srw.rowContainer.Clear(srw.ctx)
	srw.rowContainer.Close(srw.ctx)
}

// writeRow write single row.
func (srw *StreamReceiver) writeRow(ctx context.Context, row tree.Datums) error {
	currentRow := make([]interface{}, srw.rowContainer.NumCols())
	for idx, colVal := range row {
		data, err := datumCheckAndConvert(srw.ctx, srw.parameters.TargetTable.IsTsTable, srw.targetTypes, idx, colVal)
		if err != nil {
			return err
		}

		currentRow[idx] = data
	}
	srw.extractsWaterMark(row[0])

	if _, err := srw.executor.Exec(ctx, "stream-receiver-persist-results-latency-mode",
		nil, srw.singleInsertStmt, currentRow...); err != nil {
		log.Errorf(
			ctx,
			"failed to write stream results to target table using low latency-mode, stream name: %s, error: %v",
			srw.metadata.Name, err,
		)
	}

	return nil
}

// flush writes the result from rowContainer to target table.
func (srw *StreamReceiver) flush() error {
	srw.mutex.Lock()
	defer srw.mutex.Unlock()

	var stmt string
	rowCount := srw.rowContainer.Len()

	if rowCount == 0 {
		return nil
	} else if rowCount == 1 || srw.batchSize == 1 {
		// When rowCount = 1 or batchSize = 1, write row by row
		stmt = srw.singleInsertStmt
	} else if rowCount < srw.batchSize {
		// when rowCount is between 1 and batchSize, dynamically generate SQL for writing;
		stmt = constructBatchInsertStmt(srw.targetTable, srw.parameters.TargetTable.ColNames, rowCount)
	} else if srw.batchSize == streamInsertSmallBatch {
		// when rowCount is greater than streamInsertSmallBatch, write in batches according to streamInsertSmallBatch.
		stmt = srw.smallBatchInsertStmt
		rowCount = srw.batchSize
	} else {
		// when rowCount is greater than streamInsertBatch, write in batches according to streamInsertBatch.
		stmt = srw.batchInsertStmt
		rowCount = srw.batchSize
	}

	currentBatch, err := srw.constructResultSet(rowCount)
	if err != nil {
		return err
	}

	// When rowCount = 1 or batchSize = 1, write row by row
	if rowCount == 1 || srw.batchSize == 1 {
		srw.persistBySingleRow(rowCount, currentBatch)

		return nil
	}

	// INSERT results using batch mode
	// INSERT INTO `srw.targetTable` VALUES ($1,$2,$3, ...), ($n,$n+1,$n+2, ...), ...
	if _, err := srw.executor.Exec(srw.ctx, "stream-receiver-persist-results-batch-mode",
		nil, stmt, currentBatch...); err != nil {
		log.Errorf(
			srw.ctx,
			"failed to write stream results to target table using batch mode, stream name: %s, error: %v",
			srw.metadata.Name, err,
		)
		srw.persistBySingleRow(rowCount, currentBatch)
	}

	return nil
}

func (srw *StreamReceiver) persistBySingleRow(rowCount int, currentBatch []interface{}) {
	colNum := srw.rowContainer.NumCols()
	errNum := 0
	// retry to INSERT results using single row mode
	for idx := 0; idx < rowCount; idx++ {
		start := idx * colNum
		end := start + colNum
		if _, err := srw.executor.Exec(srw.ctx, "stream-receiver-persist-results-single-mode",
			nil, srw.singleInsertStmt, currentBatch[start:end]...); err != nil {
			// ignore the error in single mode to avoid too many output messages.
			errNum++
		}
	}
	if errNum > 0 {
		log.Errorf(
			srw.ctx,
			"write stream results to target table using single-row mode, stream name: %s, failed rows: %d, total rows: %d",
			srw.metadata.Name, errNum, rowCount,
		)
	}
}

// checkpoint writes the data and update low-watermark.
func (srw *StreamReceiver) checkpoint() error {
	if err := srw.flush(); err != nil {
		return err
	}

	if err := srw.persistLowWatermark(); err != nil {
		return err
	}

	srw.evaluateBatchSize()

	return nil
}

// persistLowWatermark updates the low-watermark to system table.
func (srw *StreamReceiver) persistLowWatermark() error {
	if srw.currentLowWaterMark > srw.originalLowWaterMark {
		if _, err := srw.executor.ExecEx(
			srw.ctx,
			"stream-receiver-update-low-water-mark",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_cdc_watermark SET low_watermark = $1 
                                 WHERE table_id = $2 AND task_id = $3 AND task_type = $4 AND internal_type = $5`,
			srw.currentLowWaterMark,
			srw.parameters.SourceTableID,
			srw.metadata.ID,
			cdcpb.TSCDCInstanceType_Stream,
			waterMarkTypeRealtime,
		); err != nil {
			return err
		}

		srw.originalLowWaterMark = srw.currentLowWaterMark
	}

	return nil
}

// extractsWaterMark extracts waterMark from input datum.
func (srw *StreamReceiver) extractsWaterMark(timestamp tree.Datum) {
	var waterMark int64
	switch timestamp.ResolvedType().Oid() {
	case oid.T_timestamp:
		waterMark = timestamp.(*tree.DTimestamp).UnixMilli()
	case oid.T_timestamptz:
		waterMark = timestamp.(*tree.DTimestampTZ).UnixMilli()
	default:
		waterMark = sqlutil.InvalidWaterMark
	}

	if waterMark > srw.currentLowWaterMark {
		srw.currentLowWaterMark = waterMark
	}
}

// constructResultSet constructs result set from rowContainer.
func (srw *StreamReceiver) constructResultSet(rowCount int) ([]interface{}, error) {
	currentRow := make([]interface{}, srw.rowContainer.NumCols()*rowCount)
	paraIdx := 0

	for idx := 0; idx < rowCount; idx++ {
		row := srw.rowContainer.At(0)
		srw.rowContainer.PopFirst()
		for idx, colVal := range row {
			data, err := datumCheckAndConvert(srw.ctx, srw.parameters.TargetTable.IsTsTable, srw.targetTypes, idx, colVal)
			if err != nil {
				return nil, err
			}

			currentRow[paraIdx] = data
			paraIdx++
		}

		srw.extractsWaterMark(row[0])
	}

	return currentRow, nil
}

// RowsAffected returns either the number of times AddRow was called, or the
// sum of all n passed into IncrementRowsAffected.
func (srw *StreamReceiver) RowsAffected() int {
	return srw.rowsAffected
}

// AddPGComplete implements the rowResultWriter interface.
func (srw *StreamReceiver) AddPGComplete(_ string, _ tree.StatementType, _ int) {}

// datumCheckAndConvert check the datum ready to insert to target table.
func datumCheckAndConvert(
	ctx context.Context, isTsTable bool, targetTypes []types.T, colIdx int, colVal tree.Datum,
) (tree.Datum, error) {
	data := tree.DNull

	if isTsTable {
		switch targetTypes[colIdx].Family() {
		case types.TimestampTZFamily:
			switch colVal.ResolvedType().Family() {
			case types.TimestampTZFamily:
				data = colVal
			default:
			}

		case types.TimestampFamily:
			switch colVal.ResolvedType().Family() {
			case types.TimestampFamily:
				data = colVal
			default:
			}

		case types.IntFamily:
			switch colVal.ResolvedType().Family() {
			case types.IntFamily:
				data = colVal
			case types.DecimalFamily:
				val, err := colVal.(*tree.DDecimal).Int64()
				if err != nil {
					fVal, err := colVal.(*tree.DDecimal).Float64()
					if err != nil {
						return nil, err
					}
					val = int64(math.Round(fVal))
				}
				data = tree.NewDInt(tree.DInt(val))

			default:
			}
		case types.FloatFamily:
			switch colVal.ResolvedType().Family() {
			case types.FloatFamily:
				data = colVal
			case types.DecimalFamily:
				val, err := colVal.(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				data = tree.NewDFloat(tree.DFloat(val))
			default:
			}
		case types.StringFamily:
			switch colVal.ResolvedType().Family() {
			case types.StringFamily:
				data = colVal
			default:
			}
		case types.BoolFamily:
			switch colVal.ResolvedType().Family() {
			case types.BoolFamily:
				data = colVal
			default:
			}
		case types.BytesFamily:
			switch colVal.ResolvedType().Family() {
			case types.BytesFamily:
				data = colVal
			default:
			}
		case types.DecimalFamily:

		default:
		}
	} else {
		switch targetTypes[colIdx].Family() {
		case types.TimestampTZFamily:
			switch colVal.ResolvedType().Family() {
			case types.TimestampTZFamily:
				data = colVal
			default:
			}

		case types.TimestampFamily:
			switch colVal.ResolvedType().Family() {
			case types.TimestampFamily:
				data = colVal
			default:
			}
		case types.DecimalFamily:
			switch colVal.ResolvedType().Family() {
			case types.DecimalFamily:
				data = colVal
			case types.IntFamily:
				dd := &tree.DDecimal{}
				dd.SetInt64(int64(*colVal.(*tree.DInt)))
				data = dd
			case types.FloatFamily:
				dd := &tree.DDecimal{}
				_, err := dd.SetFloat64(float64(*colVal.(*tree.DFloat)))
				if err != nil {
					return nil, err
				}
				data = dd
			default:
			}

		case types.StringFamily:
			switch colVal.ResolvedType().Family() {
			case types.StringFamily:
				data = colVal
			default:
			}

		case types.BoolFamily:
			switch colVal.ResolvedType().Family() {
			case types.BoolFamily:
				data = colVal
			default:
			}
		case types.BytesFamily:
			switch colVal.ResolvedType().Family() {
			case types.BytesFamily:
				data = colVal
			default:
			}
		case types.IntFamily:
		case types.FloatFamily:
		default:

		}
	}

	if data == tree.DNull && colVal != tree.DNull {
		log.Errorf(
			ctx,
			"stream output type %q is not compatible with target table, target column type is %q",
			targetTypes[colIdx].Name(), colVal.ResolvedType().Name(),
		)
	}
	return data, nil
}

// constructBatchInsertStmt constructs SQL for insert to target table.
func constructBatchInsertStmt(tableName string, targetCols []string, rowNum int) string {
	colNum := len(targetCols)
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(fmt.Sprintf("%s(%s)", tableName, strings.Join(targetCols, ",")))
	sql.WriteString(" VALUES ")

	values := make([]string, colNum)
	parIdx := 1
	for idx := 0; idx < rowNum; idx++ {
		sql.WriteString("(")
		for colIdx := 0; colIdx < colNum; colIdx++ {
			values[colIdx] = fmt.Sprintf("$%d", parIdx)
			parIdx++
		}
		sql.WriteString(strings.Join(values, ","))
		sql.WriteString("),")
	}

	return sql.String()[0 : sql.Len()-1]
}
