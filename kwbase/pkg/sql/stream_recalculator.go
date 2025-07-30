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
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// waterMarkType is an enum of watermark types.
type waterMarkType int32

const (
	// waterMarkTypeRealtime is the type of realtime watermark.
	waterMarkTypeRealtime waterMarkType = 0
	// waterMarkTypeHistorical is the type of historical watermark.
	waterMarkTypeHistorical waterMarkType = 1
	// splitWindowUpdateMaxRequest is the maximum value of the split window in the queue.
	splitWindowUpdateMaxRequest = 100
)

// splitWindow defines the split window.
type splitWindow struct {
	// startPoint of the split window.
	startPoint tree.Datum
	// endPoint of the split window.
	endPoint tree.Datum

	// splitRow contents timestamp of split point + primary tags
	splitRow tree.Datums
}

// expiredScope defines the expired data that needs to be recalculated.
type expiredScope struct {
	// startPoint of the expired data.
	startPoint tree.Datum
	// endPoint of the expired data.
	endPoint tree.Datum
	// row contents the expired timestamp + primary tags.
	row tree.Datums
}

// splitWindowQueue defines the queue of splitWindow;
type splitWindowQueue struct {
	// splitWindows is the array of splitWindow.
	splitWindows []*splitWindow

	// mutex locks the array of splitWindow.
	mutex syncutil.Mutex
}

// IsEmpty check if the queue empty.
func (q *splitWindowQueue) IsEmpty() bool {
	return len(q.splitWindows) == 0
}

// Size return the queue length.
func (q *splitWindowQueue) Size() int {
	return len(q.splitWindows)
}

// EnqueueSplitWindow adds the splitWindow to the end of the queue.
func (q *splitWindowQueue) EnqueueSplitWindow(splitWindow *splitWindow) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.splitWindows = append(q.splitWindows, splitWindow)
}

// DequeueSplitWindow takes out a splitWindow from the top of the queue.
func (q *splitWindowQueue) DequeueSplitWindow() *splitWindow {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.IsEmpty() {
		return nil
	}

	splitWindow := q.splitWindows[0]
	q.splitWindows = q.splitWindows[1:]

	return splitWindow
}

// recalculatorStmts defines ths SQL for recalculated groupWindow.
type recalculatorStmts struct {
	singleInsertStmt                                string
	batchInsertStmt                                 string
	historicalRecordsProcessingStmt                 string
	recalculateStmt                                 string
	startTimestampStmtOfHistoricalRowsOnSourceTable string
	endTimestampStmtOfHistoricalRowsOnSourceTable   string
	historicalRowsOfLastWindowStmtOfOnSourceTable   string

	lastWindowBeginningStmtOnTargetTable  string
	splitWindowBeginningStmtOnTargetTable string
	splitWindowEndStmtOnTargetTable       string
	deleteScopeDetermineStmtOnTargetTable string
	deleteStmtOnTargetTable               string
	inWindowEndStmtOnTargetTable          string
}

// streamRecalculator is used for historical data processing and splitWindow recalculation in stream computing.
type streamRecalculator struct {
	recalculatorStmts
	// ctx is stream context.
	ctx context.Context
	// splitWindowQueue is the queue of splitWindows.
	splitWindowQueue splitWindowQueue
	// uncompletedSplitWindowQueue is the queue of uncompleted splitWindows.
	uncompletedSplitWindowQueue splitWindowQueue
	// notifyCh starts recalculating after receives signal.
	notifyCh chan bool
	// streamParameters is praameters of stream.
	streamParameters *sqlutil.StreamParameters
	// orderingColumnIDs are the primary tags IDs.
	orderingColumnIDs []uint32
	// tableID is ID of source table.
	tableID uint64
	// instanceID is stream ID.
	instanceID uint64
	// streamName is stream name.
	streamName string
	// isIncludePrimaryTag indicates that the source table has a primary tags.
	isIncludePrimaryTag bool
	// defaultStartTimestamp
	defaultStartTimestamp tree.Datum

	// cdcColNames is column names of the data from CDC.
	cdcColNames []string
	// cdcColTypes is column types of the data from CDC.
	cdcColTypes []types.T
	// targetColTypes is column names of target table.
	targetColTypes []types.T
	// targetTableName is the name of target table .
	targetTableName string
	// targetColNum is the number of target table.
	targetColNum int

	//flowCtx encompasses the configuration parameters needed.
	flowCtx *execinfra.FlowCtx
	// executor is the InternalExecutor for single node.
	executor sqlutil.InternalExecutor
	// distInternalExecutor is the InternalExecutor for cluster.
	distInternalExecutor sqlutil.InternalExecutor

	// expiredScopesMutex it the mutex of expiredScopes.
	expiredScopesMutex syncutil.Mutex
	// recalculateDelayRound defines the checkpoint period to delay for calculating.
	recalculateDelayRound int
	// expiredProcessDelayCounter defines the remaining checkpoint period to delay for calculating.
	expiredProcessDelayCounter int
	// expiredScopes save expired need to recalculate.
	expiredScopes map[string]*expiredScope

	// used to store the recalculation parameters for each round
	recalculateParams []interface{}
}

func newRecalculator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.StreamReaderSpec,
	paras *sqlutil.StreamParameters,
	recalculateDelayRound int,
) *streamRecalculator {
	sr := &streamRecalculator{
		ctx:                        ctx,
		notifyCh:                   make(chan bool, 1),
		streamParameters:           paras,
		orderingColumnIDs:          spec.OrderingColumnIDs,
		cdcColNames:                spec.CDCColumns.CDCColumnNames,
		cdcColTypes:                spec.CDCColumns.CDCTypes,
		targetColTypes:             spec.TargetColTypes,
		tableID:                    paras.SourceTableID,
		streamName:                 spec.Metadata.Name,
		instanceID:                 spec.Metadata.ID,
		isIncludePrimaryTag:        len(spec.OrderingColumnIDs) > 0,
		flowCtx:                    flowCtx,
		executor:                   flowCtx.Cfg.Executor,
		distInternalExecutor:       flowCtx.Cfg.CDCCoordinator.DistInternalExecutor(),
		recalculateDelayRound:      recalculateDelayRound,
		expiredProcessDelayCounter: recalculateDelayRound,
	}

	err := constructStmts(sr)
	if err != nil {
		return nil
	}

	sr.expiredScopes = make(map[string]*expiredScope)

	sr.defaultStartTimestamp = constructTimestampDatum(0, sr.targetColTypes[0])

	sr.recalculateParams = make([]interface{}, len(sr.orderingColumnIDs)+2)
	sr.targetTableName = fmt.Sprintf(
		"%s.%s", sr.streamParameters.TargetTable.Database, sr.streamParameters.TargetTable.Table,
	)
	sr.targetColNum = len(sr.targetColTypes)
	sr.singleInsertStmt = constructBatchInsertStmt(sr.targetTableName, sr.streamParameters.TargetTable.ColNames, 1)
	sr.batchInsertStmt = constructBatchInsertStmt(sr.targetTableName, sr.streamParameters.TargetTable.ColNames, streamInsertBatch)

	return sr
}

// Run Start listening notifyCh to recalculate.
func (sr *streamRecalculator) Run(ctx context.Context) error {
	for {
		if !sr.splitWindowQueue.IsEmpty() {
			splitWindow := sr.splitWindowQueue.DequeueSplitWindow()
			if splitWindow == nil {
				return nil
			}
			if err := sr.calculate(splitWindow); err != nil {
				return err
			}
		}

		select {
		case <-sr.notifyCh:
			err := sr.updateAllSplitWindowEndTimestamp()
			if err != nil {
				log.Errorf(sr.ctx, "failed to update the start/end timestamp of split window: %v", err)
				return err
			}

			sr.expiredScopesMutex.Lock()
			// process expired rows every 'recalculateDelayRounds(default is 10) checkpoint' cycles.
			if len(sr.expiredScopes) > 0 {
				if sr.expiredProcessDelayCounter <= 0 {
					sr.expiredProcessDelayCounter = sr.recalculateDelayRound
					for key, val := range sr.expiredScopes {
						if err := sr.handleSplitWindowWithScope(val); err != nil {
							return err
						}

						delete(sr.expiredScopes, key)
					}
				} else {
					sr.expiredProcessDelayCounter--
				}
			}
			sr.expiredScopesMutex.Unlock()

			break
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Notify Send a signal to notifyCh.
func (sr *streamRecalculator) Notify() {
	if len(sr.notifyCh) == 0 {
		sr.notifyCh <- true
	}
}

// addSplitWindow add the splitWindow to queue.
func (sr *streamRecalculator) addSplitWindow(splitWindow *splitWindow) {
	sr.splitWindowQueue.EnqueueSplitWindow(splitWindow)
	if len(sr.notifyCh) == 0 {
		sr.notifyCh <- true
	}
}

// calculate calculates the splitWindow.
func (sr *streamRecalculator) calculate(splitWindow *splitWindow) error {
	if splitWindow.endPoint != tree.DNull {
		if err := sr.recalculateRows(splitWindow, sr.recalculateParams); err != nil {
			return err
		}
	} else {
		if err := sr.updateSplitWindowTimestamp(splitWindow); err != nil {
			return err
		}
	}

	return nil
}

// recalculateRows recalculate the rows of the splitWindow.
func (sr *streamRecalculator) recalculateRows(
	splitWindow *splitWindow, params []interface{},
) error {
	params[0] = splitWindow.startPoint
	params[1] = splitWindow.endPoint

	for idx := 2; idx < len(params); idx++ {
		params[idx] = splitWindow.splitRow[idx-1]
	}

	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-recalculate-rows",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.recalculateStmt,
		params...,
	)

	if err != nil {
		return err
	}

	if len(rows) == 0 || len(rows[0]) < 2 || rows[0][0] == tree.DNull {
		return nil
	}

	// only stream query with agg functions need to filter the results.
	if sr.streamParameters.StreamSink.HasAgg {
		// if it has multiple rows that the window_end_point equals splitWindow.endPoint, only persist the first one.
		// the reason is the timestamp of 'w_end' is monotone increasing, the following results belong to the next agg window
		// that must be calculated by another agg window to avoid a new split window.
		filteredRows := make([]tree.Datums, 0)
		previousEndTimestamp := sr.defaultStartTimestamp
		for _, row := range rows {
			// the second output of the agg stream query is the close timestamp of the agg window.
			endTimestamp := row[1]
			if previousEndTimestamp.Compare(sr.flowCtx.EvalCtx, endTimestamp) == -1 {
				filteredRows = append(filteredRows, row)
			}
			previousEndTimestamp = endTimestamp
		}

		if err := sr.deleteRows(filteredRows, splitWindow); err != nil {
			log.Errorf(sr.ctx, "failed to delete the rows: %v", err)
		}

		return sr.persistResults(filteredRows)
	}

	return sr.persistResults(rows)
}

// updateAllSplitWindowEndTimestamp update the end timestamp of all SplitWindow.
func (sr *streamRecalculator) updateAllSplitWindowEndTimestamp() error {
	uncompletedCount := sr.uncompletedSplitWindowQueue.Size()

	// use SplitWindowUpdateMaxRequest to avoid submitting too many updateSplitWindowTimestamp requests tp ts the engine
	if splitWindowUpdateMaxRequest < uncompletedCount {
		uncompletedCount = splitWindowUpdateMaxRequest
	}

	var finalErr error
	for idx := 0; idx < uncompletedCount; idx++ {
		splitWindow := sr.uncompletedSplitWindowQueue.DequeueSplitWindow()
		if splitWindow == nil {
			break
		}

		err := sr.updateSplitWindowTimestamp(splitWindow)
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}

// updateSplitWindowTimestamp update the end timestamp of the SplitWindow.
func (sr *streamRecalculator) updateSplitWindowTimestamp(splitWindow *splitWindow) error {
	if splitWindow.startPoint != tree.DNull &&
		splitWindow.startPoint.Compare(sr.flowCtx.EvalCtx, sr.defaultStartTimestamp) == 0 {
		beginTimestamp, err := sr.extractSplitWindowBeginningTimestampOnTargetTable(splitWindow.splitRow)
		if err != nil {
			return err
		}

		if beginTimestamp != tree.DNull {
			splitWindow.startPoint = beginTimestamp
		}
	}

	endTimestamp, err := sr.extractSplitWindowEndTimestampOnTargetTable(splitWindow.splitRow)
	if err != nil {
		return err
	}

	if endTimestamp != tree.DNull {
		splitWindow.endPoint = endTimestamp
		sr.addSplitWindow(splitWindow)
	} else {
		sr.uncompletedSplitWindowQueue.EnqueueSplitWindow(splitWindow)
	}

	return nil
}

// HandleHistoryRows is used to recalculate all historical data in the source table
// and write it to the target table when there is no watermark.
func (sr *streamRecalculator) HandleHistoryRows() error {
	if !sr.streamParameters.TargetTable.IsTsTable {
		return nil
	}

	var historicalWaterMark int64
	var realtimeWaterMark int64
	var err error
	var ok bool

	lowWaterMark := sqlutil.InvalidWaterMark
	// handle the uncompleted processing of historical rows
	historicalWaterMark, ok, err = sr.loadLowWaterMark(waterMarkTypeHistorical)
	if err != nil {
		return err
	}

	if ok {
		lowWaterMark = historicalWaterMark
	} else {
		realtimeWaterMark, _, err = sr.loadLowWaterMark(waterMarkTypeRealtime)
		if err != nil {
			return err
		}
		lowWaterMark = realtimeWaterMark
	}

	lastRows, err := sr.extractLastTimestampOfHistoricalRows()
	if err != nil {
		return err
	}

	if len(lastRows) == 1 && len(lastRows[0]) > 0 && lastRows[0][0] == tree.DNull {
		return nil
	}

	if sr.streamParameters.StreamSink.HasAgg {
		// persist historical lowWaterMark to kwdb_cdc_watermark table.
		if !ok {
			err = sr.persistHistoricalLowWaterMark(lowWaterMark)
		}
		if err != nil {
			return err
		}
	}
	if lowWaterMark == sqlutil.InvalidWaterMark {
		// process all rows
		for _, row := range lastRows {
			err := sr.processHistoryRows(row)
			if err != nil {
				return err
			}
		}
	} else {
		firstRows, err := sr.extractFirstTimestampOfHistoricalRows(lowWaterMark)
		if err != nil {
			return err
		}

		if err := sr.processUnprocessedRows(firstRows, lastRows); err != nil {
			return err
		}
	}

	if sr.streamParameters.StreamSink.HasAgg {
		// handle the split agg window
		for _, row := range lastRows {
			if len(row) > 0 && row[0] == tree.DNull {
				continue
			}

			if err := sr.handleSplitWindow(row); err != nil {
				return err
			}
		}

		// delete historical lowWaterMark to kwdb_cdc_watermark table.
		err = sr.deleteHistoricalLowWaterMark()
		if err != nil {
			return err
		}
	}

	return nil
}

// HandleExpiredRows extracts the splitWindow from the original CDC result ant put it into splitWindowQueue
func (sr *streamRecalculator) HandleExpiredRows(expiredRow sqlbase.EncDatumRow) {
	if !sr.streamParameters.TargetTable.IsTsTable {
		return
	}

	if sr.streamParameters.StreamSink.HasAgg {
		sr.expiredScopesMutex.Lock()
		defer sr.expiredScopesMutex.Unlock()

		identity := sr.constructBucketIdentityFromEncDatumRow(expiredRow)
		scope, ok := sr.expiredScopes[identity]
		if !ok {
			row := make(tree.Datums, len(sr.streamParameters.TargetTable.PrimaryTagCols)+1)

			row[0] = expiredRow[0].Datum

			for idx, col := range sr.orderingColumnIDs {
				row[idx+1] = expiredRow[col].Datum
			}

			scope = &expiredScope{startPoint: expiredRow[0].Datum, endPoint: expiredRow[0].Datum, row: row}
			sr.expiredScopes[identity] = scope
			return
		}

		if expiredRow[0].Datum.Compare(sr.flowCtx.EvalCtx, scope.startPoint) == -1 {
			scope.startPoint = expiredRow[0].Datum
		}

		if expiredRow[0].Datum.Compare(sr.flowCtx.EvalCtx, scope.endPoint) == 1 {
			scope.endPoint = expiredRow[0].Datum
		}
	}

	return
}

// processHistoryRows process the historical rows using the original stream query
func (sr *streamRecalculator) processHistoryRows(row tree.Datums) error {
	params := make([]interface{}, len(row))

	for idx, col := range row {
		params[idx] = col
	}

	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-process-history-rows",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.historicalRecordsProcessingStmt,
		params...,
	)

	if err != nil {
		return err
	}

	return sr.persistResults(rows)
}

// processUnprocessedRows process the unprocessed rows using the original stream query
func (sr *streamRecalculator) processUnprocessedRows(
	firstRows []tree.Datums, lastRows []tree.Datums,
) error {
	var unprocessedSplitWindows []*splitWindow
	firstMap := make(map[string]tree.Datums)
	lastMap := make(map[string]tree.Datums)

	for _, row := range firstRows {
		firstMap[sr.constructBucketIdentity(row)] = row
	}

	for _, row := range lastRows {
		lastMap[sr.constructBucketIdentity(row)] = row
	}

	for key, lastRow := range lastMap {
		splitWindow := &splitWindow{
			startPoint: sr.defaultStartTimestamp,
			endPoint:   lastRow[0],
			splitRow:   lastRow,
		}

		if firstRow, ok := firstMap[key]; ok {
			beginningTimestamp, err := sr.extractSplitWindowBeginningTimestampOnTargetTable(firstRow)
			if err != nil {
				return err
			}

			if beginningTimestamp != tree.DNull {
				splitWindow.startPoint = beginningTimestamp
			}
		}

		unprocessedSplitWindows = append(unprocessedSplitWindows, splitWindow)
	}

	var err error
	params := make([]interface{}, len(sr.orderingColumnIDs)+2)
	for _, splitWindow := range unprocessedSplitWindows {
		err = sr.recalculateRows(splitWindow, params)
	}

	return err
}

// handleSplitWindow calculates the start and end times of the split window caused by the input row
// and puts the constructed split window into the queue.
func (sr *streamRecalculator) handleSplitWindow(row tree.Datums) error {
	beginningTimestamp, err := sr.extractSplitWindowBeginningTimestampOnTargetTable(row)
	if err != nil {
		return err
	}

	if beginningTimestamp == tree.DNull {
		beginningTimestamp = sr.defaultStartTimestamp
	}

	endTimestamp, err := sr.extractSplitWindowEndTimestampOnTargetTable(row)
	if err != nil {
		return err
	}

	splitWindow := &splitWindow{
		startPoint: beginningTimestamp,
		endPoint:   endTimestamp,
		splitRow:   row,
	}

	sr.addSplitWindow(splitWindow)

	return nil
}

// handleSplitWindowWithScope calculates the start and end times of the split window caused by the expired row
// and puts the constructed split window into the queue.
func (sr *streamRecalculator) handleSplitWindowWithScope(scope *expiredScope) error {
	scope.row[0] = scope.startPoint
	beginningTimestamp, err := sr.extractSplitWindowBeginningTimestampOnTargetTable(scope.row)
	if err != nil {
		return err
	}

	if beginningTimestamp == tree.DNull {
		beginningTimestamp = sr.defaultStartTimestamp
	}

	scope.row[0] = scope.endPoint
	endTimestamp, err := sr.extractSplitWindowEndTimestampOnTargetTable(scope.row)
	if err != nil {
		return err
	}

	splitWindow := &splitWindow{
		startPoint: beginningTimestamp,
		endPoint:   endTimestamp,
		splitRow:   scope.row,
	}

	sr.addSplitWindow(splitWindow)

	return nil
}

// persistHistoricalLowWaterMark persists the watermark of historical data.
func (sr *streamRecalculator) persistHistoricalLowWaterMark(watermark int64) error {
	if _, err := sr.executor.ExecEx(
		sr.ctx,
		"insert-stream-low-water-mark",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_cdc_watermark (table_id,task_id,task_type,internal_type,low_watermark)
VALUES ($1,$2,$3,$4,$5)`,
		sr.tableID,
		sr.instanceID,
		cdcpb.TSCDCInstanceType_Stream,
		waterMarkTypeHistorical,
		watermark,
	); err != nil {
		return err
	}

	return nil
}

// persistHistoricalLowWaterMark delete the watermark of historical data.
func (sr *streamRecalculator) deleteHistoricalLowWaterMark() error {
	if _, err := sr.executor.ExecEx(
		sr.ctx,
		"update-stream-low-water-mark",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`DELETE FROM system.kwdb_cdc_watermark WHERE table_id = $1 AND task_id = $2 AND task_type = $3 AND internal_type = $4`,
		sr.tableID,
		sr.instanceID,
		cdcpb.TSCDCInstanceType_Stream,
		waterMarkTypeHistorical,
	); err != nil {
		return err
	}

	return nil
}

// loadLowWaterMark load the watermark of historical data or real time data.
func (sr *streamRecalculator) loadLowWaterMark(typ waterMarkType) (int64, bool, error) {
	row, err := sr.executor.QueryRowEx(
		sr.ctx,
		"load-stream-water-mark",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT low_watermark FROM system.kwdb_cdc_watermark 
                     WHERE table_id = $1 AND task_id = $2 AND task_type = $3 AND internal_type = $4`,
		sr.tableID, sr.instanceID, cdcpb.TSCDCInstanceType_Stream, typ,
	)

	if err != nil {
		return sqlutil.InvalidWaterMark, false, err
	}

	if row != nil && len(row) == 1 {
		waterMark := int64(tree.MustBeDInt(row[0]))
		return waterMark, true, nil
	}

	return sqlutil.InvalidWaterMark, false, nil
}

// loadExpiredTime read the end time of the last window in the target table as the expiration time.
func (sr *streamRecalculator) loadExpiredTime(last time.Time) (time.Time, error) {
	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-load-expired-time",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			`SELECT %s FROM %s.%s order by %s desc limit 1`,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetEndColName,
		),
	)
	if err != nil {
		return last, err
	}

	if len(rows) == 0 {
		return last, nil
	}

	if rows[0][0] == tree.DNull {
		return last, nil
	}

	newLast := tree.MustBeDTimestampTZ(rows[0][0]).Time
	if last.After(newLast) {
		return last, nil
	}

	return newLast, nil
}

// loadRowsInLastWindow read rows from the source table
// that falls within the last window of the target table.
func (sr *streamRecalculator) loadRowsInLastWindow() ([]tree.Datums, error) {
	res := make([]tree.Datums, 0)
	if !sr.streamParameters.TargetTable.IsTsTable {
		return res, nil
	}

	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-extract-last-timestamp-of-last-window",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.lastWindowBeginningStmtOnTargetTable,
	)

	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return res, nil
	}

	for _, row := range rows {
		params := make([]interface{}, len(row))
		for idx, col := range row {
			params[idx] = col
		}

		pRows, err := sr.distInternalExecutor.QueryEx(
			sr.ctx,
			"stream-load-rows-after-begin-of-last-window",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			sr.historicalRowsOfLastWindowStmtOfOnSourceTable,
			params...,
		)

		if err != nil {
			return nil, err
		}

		res = append(res, pRows...)
	}

	return res, nil

}

// extractLastTimestampOfHistoricalRows extracts the last timestamp of historical rows from source table.
func (sr *streamRecalculator) extractLastTimestampOfHistoricalRows() ([]tree.Datums, error) {
	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-extract-last-timestamp-of-historical-rows",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.endTimestampStmtOfHistoricalRowsOnSourceTable,
	)

	if err != nil {
		return nil, err
	}

	return rows, nil
}

// extractFirstTimestampOfHistoricalRows extracts the first timestamp of historical rows from source table.
func (sr *streamRecalculator) extractFirstTimestampOfHistoricalRows(
	waterMark int64,
) ([]tree.Datums, error) {
	timestamp := constructTimestampDatum(waterMark, sr.cdcColTypes[0])

	rows, err := sr.distInternalExecutor.QueryEx(
		sr.ctx,
		"stream-extract-first-timestamp-of-historical-rows",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.startTimestampStmtOfHistoricalRowsOnSourceTable,
		timestamp,
	)

	if err != nil {
		return nil, err
	}

	return rows, nil
}

// extractSplitWindowBeginningTimestampOnTargetTable extracts the start time of the window
// where the split row resides from the target table.
func (sr *streamRecalculator) extractSplitWindowBeginningTimestampOnTargetTable(
	splitRow tree.Datums,
) (tree.Datum, error) {
	params := make([]interface{}, len(splitRow))

	for idx, col := range splitRow {
		params[idx] = col
	}

	row, err := sr.distInternalExecutor.QueryRowEx(
		sr.ctx,
		"stream-extract-split-window-beginning",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.splitWindowBeginningStmtOnTargetTable,
		params...,
	)

	if err != nil {
		return nil, err
	}

	return row[0], nil
}

// extractSplitWindowBeginningTimestampOnTargetTable extracts the end time of the window
// where the split row resides from the target table.
func (sr *streamRecalculator) extractSplitWindowEndTimestampOnTargetTable(
	splitRow tree.Datums,
) (tree.Datum, error) {
	params := make([]interface{}, len(splitRow))

	for idx, col := range splitRow {
		params[idx] = col
	}

	row, err := sr.distInternalExecutor.QueryRowEx(
		sr.ctx,
		"stream-extract-split-window-end",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.splitWindowEndStmtOnTargetTable,
		params...,
	)

	if err != nil {
		return nil, err
	}

	return row[0], nil
}

// persistResults writes the recalculated results to the target table and ignores errors
// if the written data does not meet the target table constraints.
func (sr *streamRecalculator) persistResults(rows []tree.Datums) error {
	numRows := len(rows)

	if numRows == 0 {
		return nil
	}

	batchNum := numRows / streamInsertBatch
	finalBatchSize := numRows - batchNum*streamInsertBatch

	currentBatch := make([]interface{}, sr.targetColNum*streamInsertBatch)
	rowIndex := 0
	for batchIdx := 0; batchIdx < batchNum; batchNum++ {
		paraIdx := 0
		for idx := 0; idx < streamInsertBatch; idx++ {
			row := rows[rowIndex]
			for colIdx, colVal := range row {
				data, err := datumCheckAndConvert(sr.ctx, sr.streamParameters.TargetTable.IsTsTable, sr.targetColTypes, colIdx, colVal)
				if err != nil {
					return err
				}
				currentBatch[paraIdx] = data
				paraIdx++
			}
			rowIndex++
		}

		// INSERT results using batch mode
		// INSERT INTO `srw.targetTable` VALUES ($1,$2,$3, ...), ($n,$n+1,$n+2, ...), ...
		if _, err := sr.executor.Exec(sr.ctx, "stream-persist-recalculate-results-batch-mode",
			nil, sr.batchInsertStmt, currentBatch...); err != nil {
			log.Errorf(
				sr.ctx,
				"failed to write stream results to target table using batch mode, stream name: %s, error: %v",
				sr.streamName, err,
			)

			// INSERT results using single row mode
			for idx := 0; idx < streamInsertBatch; idx++ {
				start := idx * sr.targetColNum
				end := start + sr.targetColNum

				if _, err := sr.executor.Exec(sr.ctx, "stream-persist-recalculate-results-single-mode",
					nil, sr.singleInsertStmt, currentBatch[start:end]...); err != nil {
					// ignore the error in single mode to avoid too many output messages.
				}
			}
		}
	}

	finalBatch := make([]interface{}, sr.targetColNum*finalBatchSize)
	finalStmt := constructBatchInsertStmt(sr.targetTableName, sr.streamParameters.TargetTable.ColNames, finalBatchSize)

	paraIdx := 0
	for idx := 0; idx < finalBatchSize; idx++ {
		row := rows[rowIndex]
		for colIdx, colVal := range row {
			data, err := datumCheckAndConvert(sr.ctx, sr.streamParameters.TargetTable.IsTsTable, sr.targetColTypes, colIdx, colVal)
			if err != nil {
				return err
			}
			finalBatch[paraIdx] = data
			paraIdx++
		}
		rowIndex++
	}

	// INSERT results using batch mode
	// INSERT INTO `srw.targetTable` VALUES ($1,$2,$3, ...), ($n,$n+1,$n+2, ...), ...
	if _, err := sr.executor.Exec(sr.ctx, "stream-persist-recalculate-results-batch-mode",
		nil, finalStmt, finalBatch...); err != nil {
		log.Errorf(
			sr.ctx,
			"failed to write stream results to target table using batch mode, stream name: %s, error: %v",
			sr.streamName, err,
		)
		// INSERT results using single row mode
		for idx := 0; idx < finalBatchSize; idx++ {
			start := idx * sr.targetColNum
			end := start + sr.targetColNum
			if _, err := sr.executor.Exec(sr.ctx, "stream-persist-recalculate-results-single-mode",
				nil, sr.singleInsertStmt, finalBatch[start:end]...); err != nil {
				// ignore the error in single mode to avoid too many output messages.
			}
		}
	}

	return nil
}

// deleteRows deletes data of the split window from the target table.
func (sr *streamRecalculator) deleteRows(rows []tree.Datums, splitWindow *splitWindow) error {
	params := make([]interface{}, len(sr.streamParameters.TargetTable.PrimaryTagCols)+2)
	var err error

	rowCount := len(rows)
	if rowCount == 0 {
		return nil
	}

	params[0], err = datumCheckAndConvert(sr.ctx, sr.isTsTable(), sr.targetColTypes, 0, rows[0][0])
	if err != nil {
		return err
	}

	params[1], err = datumCheckAndConvert(sr.ctx, sr.isTsTable(), sr.targetColTypes, 0, splitWindow.endPoint)
	if err != nil {
		return err
	}

	for idx := 1; idx < len(splitWindow.splitRow); idx++ {
		params[idx+1] = splitWindow.splitRow[idx]
	}

	invalidRows, err := sr.executor.QueryEx(
		sr.ctx,
		"stream-fetch-timestamp-of-invalid-records-in-target-table",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.deleteScopeDetermineStmtOnTargetTable,
		params...,
	)

	if err != nil {
		return errors.Wrap(err, "failed to delete invalid rows")
	}

	rowCount = len(invalidRows)
	if rowCount <= 0 {
		return nil
	}

	params[0], err = datumCheckAndConvert(sr.ctx, sr.isTsTable(), sr.targetColTypes, 0, invalidRows[0][0])
	if err != nil {
		return err
	}

	params[1], err = datumCheckAndConvert(sr.ctx, sr.isTsTable(), sr.targetColTypes, 0, invalidRows[rowCount-1][0])
	if err != nil {
		return err
	}

	_, err = sr.executor.ExecEx(
		sr.ctx,
		"stream-delete-invalid-records-in-target-table",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sr.deleteStmtOnTargetTable,
		params...,
	)

	if err != nil {
		return errors.Wrap(err, "failed to delete invalid rows")
	}

	return nil
}

// constructBucketIdentity construct identity from primary tag of tree.Datums.
func (sr *streamRecalculator) constructBucketIdentity(row tree.Datums) string {
	colNum := len(row)
	if colNum > 1 {
		buf := strings.Builder{}
		for idx := 1; idx < colNum; idx++ {
			dString := sqlbase.DatumToString(row[idx])
			buf.WriteString(fmt.Sprintf("%d:%s", len(dString), dString))
		}
		return buf.String()
	}

	return ""
}

// constructBucketIdentityFromEncDatumRow construct identity from primary tag of EncDatumRow.
func (sr *streamRecalculator) constructBucketIdentityFromEncDatumRow(
	row sqlbase.EncDatumRow,
) string {
	buf := strings.Builder{}

	for _, colIdx := range sr.orderingColumnIDs {
		dString := sqlbase.DatumToString(row[colIdx].Datum)
		buf.WriteString(fmt.Sprintf("%d:%s", len(dString), dString))
	}

	return buf.String()
}

// isTsTable returns whether the target table is a time-series table.
func (sr *streamRecalculator) isTsTable() bool {
	return sr.streamParameters.TargetTable.IsTsTable
}

// constructStmts constructs the SQL statements for window recalculation.
func constructStmts(sr *streamRecalculator) error {
	var sb strings.Builder

	sourceTable := sr.streamParameters.SourceTable
	originalQuery := sr.streamParameters.StreamSink.SQL

	// on CREATE STREAM phase, a 'WHERE 1=1' will be added if it has no 'WHERE' clause in the stream query.
	stmt := strings.Split(originalQuery, "WHERE")

	// stmt for historical rows, for example,
	// SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname
	// FROM benchmark.public.cpu  WHERE (k_timestamp <= $1, hostname =$2) AND  usage_user > 0
	// GROUP BY event_window(usage_user > 50, usage_system < 20), hostname
	sb.WriteString(stmt[0])

	timestampCondition := fmt.Sprintf(" WHERE (%s <= $1 ", sourceTable.TsColumnName)
	sb.WriteString(timestampCondition)

	primaryTagCondition := " AND %s = $%d "

	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+2))
		}
	}
	sb.WriteString(") AND ")
	sb.WriteString(stmt[1])

	sr.historicalRecordsProcessingStmt = sb.String()

	// stmt for unprocessed rows, for example,
	// SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname
	// FROM benchmark.public.cpu
	// WHERE (k_timestamp >= $1 AND k_timestamp <= $2  AND hostname = $3 ) AND  usage_user > 0
	// GROUP BY event_window(usage_user > 50, usage_system < 20), hostname
	sb.Reset()
	sb.WriteString(stmt[0])
	timestampCondition = fmt.Sprintf(" WHERE (%s >= $1 AND %s <= $2 ", sourceTable.TsColumnName, sourceTable.TsColumnName)
	sb.WriteString(timestampCondition)

	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+3))
		}
	}
	sb.WriteString(") AND ")
	sb.WriteString(stmt[1])

	sr.recalculateStmt = sb.String()

	originalFilter := sr.streamParameters.SourceTable.Filter

	// stmt to extract the timestamp of last record, for example,
	// SELECT last(k_timestamp), hostname FROM benchmark.cpu WHERE k_timestamp <= $1 GROUP BY hostname
	if sr.isIncludePrimaryTag {
		primaryTags := strings.Join(sr.streamParameters.SourceTable.PrimaryTagCols, ",")
		sr.startTimestampStmtOfHistoricalRowsOnSourceTable = fmt.Sprintf(
			`SELECT last(%s), %s FROM %s.%s WHERE %s AND %s <= $1 GROUP BY %s`,
			sr.streamParameters.SourceTable.TsColumnName,
			primaryTags,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			originalFilter,
			sr.streamParameters.SourceTable.TsColumnName,
			primaryTags,
		)
	} else {
		sr.startTimestampStmtOfHistoricalRowsOnSourceTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s AND %s <= $1 `,
			sr.streamParameters.SourceTable.TsColumnName,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			originalFilter,
			sr.streamParameters.SourceTable.TsColumnName,
		)
	}

	// stmt to extract the last timestamp, for example,
	// SELECT last(k_timestamp), hostname FROM benchmark.cpu GROUP BY hostname
	if sr.isIncludePrimaryTag {
		primaryTags := strings.Join(sr.streamParameters.SourceTable.PrimaryTagCols, ",")
		sr.endTimestampStmtOfHistoricalRowsOnSourceTable = fmt.Sprintf(
			`SELECT last(%s), %s FROM %s.%s WHERE %s GROUP BY %s`,
			sr.streamParameters.SourceTable.TsColumnName,
			primaryTags,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			originalFilter,
			primaryTags,
		)
	} else {
		sr.endTimestampStmtOfHistoricalRowsOnSourceTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s`,
			sr.streamParameters.SourceTable.TsColumnName,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			originalFilter,
		)
	}

	// stmt to read rows in last window,
	// SELECT * FROM benchmark.cpu WHERE hostname=$1
	sb.Reset()
	colNames := strings.Join(sr.cdcColNames, ",")
	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+2))
		}

		sr.historicalRowsOfLastWindowStmtOfOnSourceTable = fmt.Sprintf(
			`SELECT %s FROM %s.%s WHERE %s %s AND %s >= $1 ORDER BY %s`,
			colNames,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			originalFilter,
			sb.String(),
			sr.streamParameters.SourceTable.TsColumnName,
			sr.streamParameters.SourceTable.TsColumnName,
		)
	} else {
		sr.historicalRowsOfLastWindowStmtOfOnSourceTable = fmt.Sprintf(
			`SELECT %s FROM %s.%s WHERE %s AND %s >= $1 ORDER BY %s`,
			colNames,
			sr.streamParameters.SourceTable.Database,
			sr.streamParameters.SourceTable.Table,
			sr.streamParameters.SourceTable.TsColumnName,
			originalFilter,
			sr.streamParameters.SourceTable.TsColumnName,
		)
	}

	// stmt to extract the beginning timestamp of the last window from target table, for example,
	// SELECT last(w_begin),hostname FROM benchmark.cpu_avg group by hostname
	sb.Reset()
	if sr.isIncludePrimaryTag {
		primaryTags := strings.Join(sr.streamParameters.SourceTable.PrimaryTagCols, ",")
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			if idx > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(pTag)
		}

		sr.lastWindowBeginningStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s), %s FROM %s.%s GROUP BY %s`,
			sr.streamParameters.TargetStartColName,
			primaryTags,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sb.String(),
		)
	} else {
		sr.lastWindowBeginningStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s`,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
		)
	}

	// stmt to extract the beginning timestamp of the split window from target table, for example,
	// SELECT last(w_begin) FROM benchmark.cpu_avg WHERE w_begin <= $1 AND hostname = $2
	sb.Reset()
	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+2))
		}

		sr.splitWindowBeginningStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s <= $1 %s`,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
			sb.String(),
		)
	} else {
		sr.splitWindowBeginningStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s <= $1`,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
		)
	}

	// stmt to extract the ending timestamp of the close window from target table, for example,
	// SELECT last(w_end) FROM benchmark.cpu_avg WHERE w_end > $1 AND hostname = $2
	sb.Reset()
	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+3))
		}

		sr.inWindowEndStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s > $1 AND %s <= $2 %s`,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetStartColName,
			sb.String(),
		)
	} else {
		sr.inWindowEndStmtOnTargetTable = fmt.Sprintf(
			`SELECT last(%s) FROM %s.%s WHERE %s > $1 AND %s <= $2`,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetStartColName,
		)
	}

	// stmt to extract the end timestamp of the split window from target table, for example,
	// SELECT first(w_end) FROM benchmark.cpu_avg WHERE w_end > $1 AND hostname = $2
	sb.Reset()
	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+2))
		}

		sr.splitWindowEndStmtOnTargetTable = fmt.Sprintf(
			`SELECT first(%s) FROM %s.%s WHERE %s > $1 %s`,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetEndColName,
			sb.String(),
		)
	} else {
		sr.splitWindowEndStmtOnTargetTable = fmt.Sprintf(
			`SELECT first(%s) FROM %s.%s WHERE %s > $1`,
			sr.streamParameters.TargetEndColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetEndColName,
		)
	}

	// stmt to determine delete scopes and delete records from target table.
	// the DELETE statement on ts table cannot use WHERE clause includes metrics conditions,
	// so we have to run SELECT statement to determine the DELETE scopes and then DELETE them.
	// for example,
	// SELECT w_begin FROM benchmark.cpu_avg WHERE (w_begin >= $1 AND w_end <= $2) AND hostname = $3
	// DELETE FROM benchmark.cpu_avg WHERE w_begin >= $1 AND w_begin <= $2 AND hostname = $3
	sb.Reset()
	if sr.isIncludePrimaryTag {
		for idx, pTag := range sr.streamParameters.SourceTable.PrimaryTagCols {
			sb.WriteString(fmt.Sprintf(primaryTagCondition, pTag, idx+3))
		}

		sr.deleteScopeDetermineStmtOnTargetTable = fmt.Sprintf(
			`SELECT %s FROM %s.%s WHERE (%s >= $1 AND %s <= $2 ) %s`,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetEndColName,
			sb.String(),
		)

		sr.deleteStmtOnTargetTable = fmt.Sprintf(
			`DELETE FROM %s.%s WHERE %s >= $1 AND %s <= $2 %s`,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetStartColName,
			sb.String(),
		)
	} else {
		sr.deleteScopeDetermineStmtOnTargetTable = fmt.Sprintf(
			`SELECT %s FROM %s.%s WHERE %s >= $1 AND %s <= $2`,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetEndColName,
		)

		sr.deleteStmtOnTargetTable = fmt.Sprintf(
			`DELETE FROM %s.%s WHERE %s >= $1 AND %s <= $2`,
			sr.streamParameters.TargetTable.Database,
			sr.streamParameters.TargetTable.Table,
			sr.streamParameters.TargetStartColName,
			sr.streamParameters.TargetStartColName,
		)
	}

	return nil
}

// constructTimestampDatum constructs the tree.Datum using the input watermark (milliseconds)
func constructTimestampDatum(watermark int64, typ types.T) tree.Datum {
	switch typ.Family() {
	case types.TimestampFamily:
		return tree.MakeDTimestamp(
			timeutil.FromTimestamp(watermark, 3),
			tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()),
		)
	case types.TimestampTZFamily:
		return tree.MakeDTimestampTZ(
			timeutil.FromTimestamp(watermark, 3),
			tree.TimeFamilyPrecisionToRoundDuration(typ.Precision()),
		)
	default:
		return nil
	}
}
