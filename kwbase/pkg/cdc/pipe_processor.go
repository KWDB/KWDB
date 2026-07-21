//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package cdc

import (
	"context"
	"encoding/binary"
	gojson "encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

const (
	waterMarkProcName = `PipeWaterMark`

	// checkpointFactor is the factor used to determine how long to persist low-water mark.
	// Currently, we use TsPipeHeartbeatInterval as the base, it means that the checkpoint interval
	// is TsPipeHeartbeatInterval * checkpointFactor.
	checkpointFactor = 5
	eventBufferSize  = 4096

	defaultTimestampType = "TIMESTAMP"
	milliTimestampType   = "TIMESTAMP(3)"
	microTimestampType   = "TIMESTAMP(6)"
	nanoTimestampType    = "TIMESTAMP(9)"

	defaultTimestampTZType = "TIMESTAMPTZ"
	milliTimestampTZType   = "TIMESTAMPTZ(3)"
	microTimestampTZType   = "TIMESTAMPTZ(6)"
	nanoTimestampTZType    = "TIMESTAMPTZ(9)"
)

// TsPipeHeartbeatInterval indicates the heartbeat interval to synchronize low-water mark to pipe job.
var TsPipeHeartbeatInterval = settings.RegisterNonNegativeDurationSetting(
	"ts.pipe.heartbeat.interval",
	"the interval for pipe heartbeat to synchronize low-water mark",
	2*time.Second,
)

type timestampTransFunc func(ts time.Time) int64

func init() {
	rowexec.NewPipeProcessor = newPipeProcessor
	cdcpb.CheckSink = CheckSlink
}

// PipeProcessor is executed by pipeJob.
//  1. Read historical data and send it to Sink.
//  2. Register with CDC and collect the heartbeats of each node in the cluster from CDC to record the water level.
//  3. Regularly check and write the latest water level line into the system table.
type pipeProcessor struct {
	execinfra.ProcessorBase
	spec   *cdcpb.PipeWatermarkSpec
	sink   map[uint64]Sink
	table  *sqlbase.TableDescriptor
	buffer chan *cdcpb.TsChangeDataCaptureEvent
	errCh  chan error
	cancel func()

	mutex          syncutil.Mutex
	watermarkCache map[uint64]map[int32]*LocalWatermark
	// lowWaterMark saves low watermark of pipe
	lowWaterMark map[uint64]int64
	// highWaterMark saves high watermark of history
	highWaterMark map[uint64]int64
	minWatermark  int64

	// historicalSegmentInterval saves historical data segmentation times by table
	historicalSegmentInterval map[uint64]int64
	// needSplitHistory is true when historical data needs to be sent in segments
	needSplitHistory bool

	// unprocessedMap stores the watermark status for each table.
	// If true, the table has a watermark and needs to be resumed from checkpoint.
	unprocessedMap map[uint64]bool

	checkpointInterval time.Duration
	heartbeatTimeout   time.Duration

	// mu locks cdcClients
	mu syncutil.Mutex
	// cdcClients contains the CDC client of each node.
	cdcClients map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient

	// connMu locks function checkNodeChange.
	connMu syncutil.Mutex
	// grpcGroup used to connects to each node.
	grpcGroup ctxgroup.Group
	// startRealTime is true when all history data be sent.
	startRealTime bool
	// historyHighOSN is the start time of pipe job start.
	historyHighOSN int64
	// operatorMap is the map that contains operator supported by cdc.
	operatorMap map[string]struct{}
	// unregisterGossip used to unregister Gossip.
	unregisterGossip func()
}

// LocalWatermark stores local watermarks with received timestamp for each node in KaiwuDB cluster.
type LocalWatermark struct {
	LocalWatermark    int64
	ReceivedTimestamp int64
}

func newPipeProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *cdcpb.PipeWatermarkSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	tableCount := len(spec.Metadata.TableList)
	if tableCount == 0 {
		return nil, errors.New("pipe job need restart")
	}

	s := &pipeProcessor{spec: spec}
	if err := s.Init(
		s,
		post,
		nil,
		flowCtx,
		processorID,
		output,
		nil,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	if s.spec.Metadata.Publish == "" {
		s.spec.Metadata.Publish = cdcpb.EventInsert
	}

	s.buffer = make(chan *cdcpb.TsChangeDataCaptureEvent, eventBufferSize)
	s.watermarkCache = make(map[uint64]map[int32]*LocalWatermark)
	s.lowWaterMark = make(map[uint64]int64, tableCount)
	s.highWaterMark = make(map[uint64]int64, tableCount)
	s.unprocessedMap = make(map[uint64]bool, tableCount)
	s.minWatermark = cdcpb.InvalidWatermark
	s.historicalSegmentInterval = make(map[uint64]int64, tableCount)
	s.sink = make(map[uint64]Sink, tableCount)
	for _, table := range s.spec.Metadata.TableList {
		s.watermarkCache[table.TableID] = make(map[int32]*LocalWatermark)
		s.lowWaterMark[table.TableID] = cdcpb.InvalidWatermark
		s.highWaterMark[table.TableID] = cdcpb.InvalidWatermark
		s.historicalSegmentInterval[table.TableID] = 0
		if len(table.PrimaryTagColumnNames) == 0 {
			// When table metadata is missing, supplement the metadata
			ctx := context.Background()
			err := s.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				desc, err := sqlbase.GetTsTableDescByTableID(ctx, txn, uint32(table.TableID))
				if err != nil {
					return err
				}

				for _, col := range desc.Columns {
					if col.IsPrimaryTagCol() {
						table.PrimaryTagColumnNames = append(table.PrimaryTagColumnNames, col.Name)
						continue
					}
					if col.IsTagCol() && !col.IsPrimaryTagCol() {
						table.NormalTagColumnNames = append(table.NormalTagColumnNames, col.Name)
					}
				}

				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	heartbeatInterval := TsPipeHeartbeatInterval.Get(&s.FlowCtx.Cfg.Settings.SV)

	s.checkpointInterval = heartbeatInterval * checkpointFactor

	s.heartbeatTimeout = time.Duration(
		TsPipeSinkMaxRetries.Get(&s.FlowCtx.Cfg.Settings.SV) * heartbeatInterval.Nanoseconds())
	s.cdcClients = make(map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient)
	s.startRealTime = s.spec.Metadata.IgnoreHistory
	s.operatorMap = make(map[string]struct{})
	for _, op := range strings.Split(s.spec.Metadata.Publish, ",") {
		switch op {
		case cdcpb.EventAll:
			s.operatorMap[cdcpb.EventInsert] = struct{}{}
			s.operatorMap[cdcpb.EventUpdate] = struct{}{}
			s.operatorMap[cdcpb.EventDelete] = struct{}{}
			s.operatorMap[cdcpb.EventDDL] = struct{}{}
			break
		default:
			s.operatorMap[op] = struct{}{}
		}
	}

	return s, nil
}

// Start initializes the pipe job to process the historical records and handle the connection to pipe coordinator.
//  1. load the current low-water mark from pipe catalog table.
//  2. test if the Sink is available.
//  3. connect to the pipe coordinator.
//  4. check and send the historical records using async task.
func (s *pipeProcessor) Start(ctx context.Context) context.Context {
	var err error
	ctx, s.cancel = context.WithCancel(ctx)
	ctx = s.StartInternal(ctx, waterMarkProcName)
	s.errCh = make(chan error, 2)

	// load the low-water mark from pipe catalog table.
	if err = s.loadLowWaterMark(); err != nil {
		s.MoveToDraining(err)
		return ctx
	}

	s.connectCDC(ctx)

	if !s.spec.Metadata.IgnoreHistory {
		s.historyHighOSN = int64(s.FlowCtx.Cfg.TsIDGen.GetNextID())
		if err = s.sendDDLHistory(ctx); err != nil {
			s.MoveToDraining(err)
			return ctx
		}

		if s.spec.Metadata.Filter == "" {
			s.spec.Metadata.Filter = "1=1"
		}

		for i := range s.spec.Metadata.TableList {
			table := s.spec.Metadata.TableList[i]
			s.sink[table.TableID], err = CreateSink(
				ctx, s.spec.Metadata.Sink,
				int(TsPipeSinkMaxRetries.Get(&s.FlowCtx.Cfg.Settings.SV)),
				int(s.spec.Metadata.BufferSize),
				false)
			if err != nil {
				s.MoveToDraining(err)
				return ctx
			}

			// Evaluate whether historical data needs to be sent in segments.
			if err := s.evaluateHistoricalData(table, s.spec.Metadata.Filter); err != nil {
				s.MoveToDraining(err)
				return ctx
			}
		}

		if s.needSplitHistory {
			if err = s.sendHistoryParallel(ctx, &s.spec.Metadata); err != nil {
				log.Errorf(s.Ctx, "failed to send history in batches: %v", err)
				s.MoveToDraining(err)
				return ctx
			}
		}
	}

	if !s.startRealTime {
		s.startRealtimeProcessor()
	}

	if !s.spec.Metadata.IgnoreHistory {
		if s.needSplitHistory {
			s.historyHighOSN = int64(s.FlowCtx.Cfg.TsIDGen.GetNextID())
			// Set a one-hour tolerance time for out-of-order data,
			// and resend the inflight out-of-order data during CDC startup.
			for tableID := range s.historicalSegmentInterval {
				s.lowWaterMark[tableID] -= s.heartbeatTimeout.Nanoseconds()
				s.historicalSegmentInterval[tableID] = 0
			}
		}

		s.needSplitHistory = false
		if err = s.sendHistoryParallel(ctx, &s.spec.Metadata); err != nil {
			log.Errorf(s.Ctx, "failed to send history without batching: %v", err)
			s.MoveToDraining(err)
			return ctx
		}
	}

	log.Infof(s.Ctx, "successful to start pipe %q.", s.spec.Metadata.Name)

	return ctx
}

// getLowWatermark get LowWatermark with lock.
func (s *pipeProcessor) getLowWatermark(tableID uint64) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.lowWaterMark[tableID]
}

// setLowWatermark set LowWatermark with lock.
func (s *pipeProcessor) setLowWatermark(tableID uint64, watermark int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lowWaterMark[tableID] = watermark
}

// SendHistoryParallel reads and sends data in parallel according to the table.
func (s *pipeProcessor) sendDDLHistory(ctx context.Context) error {
	if _, ok := s.operatorMap[cdcpb.EventDDL]; ok {
		start := sqlbase.TSIDToTime(uint64(s.minWatermark))
		stmt := `SELECT pusher_id, database_name, schema_name, table_name, operation, op_time, statement
					FROM system.kwdb_unpush
					WHERE pusher_id=$1 AND type=$2 AND op_time>$3
          ORDER BY op_time ASC`
		rows, err := s.EvalCtx.InternalExecutor.Query(
			ctx, "select-pipe-unpush", s.EvalCtx.Txn,
			stmt, s.spec.Metadata.ID, sqlbase.CDCInstanceType_Pipe, start,
		)
		if err != nil {
			log.Errorf(ctx, "pipe[%s] selects unsend failed. %v", s.spec.Metadata.Name, err)
			return err
		}

		maxRetries := int(TsPipeSinkMaxRetries.Get(&s.FlowCtx.Cfg.Settings.SV))
		bufferSize := int(s.spec.Metadata.BufferSize)
		for _, row := range rows {
			dbName := string(tree.MustBeDString(row[1]))
			schemaName := string(tree.MustBeDString(row[2]))
			tableName := string(tree.MustBeDString(row[3]))
			ddlType := string(tree.MustBeDString(row[4]))
			osn := tree.MustBeDTimestamp(row[5]).UTC()
			stmt = string(tree.MustBeDString(row[6]))

			isSend := true
			// Filter out DDL statements where the OSN is less than the low-watermark.
			for _, table := range s.spec.Metadata.TableList {
				if table.Database == dbName && table.Schema == schemaName && table.Table == tableName {
					if s.lowWaterMark[table.TableID] >= osn.UnixNano() {
						isSend = false
						break
					}
				}
			}

			if !isSend {
				continue
			}

			if err = sendToPipeInner(
				ctx, dbName, schemaName, tableName, ddlType, stmt, s.spec.Metadata.Sink, bufferSize, maxRetries, osn,
			); err != nil {
				log.Errorf(ctx, "pipe[%s] sends unsend failed. %v", s.spec.Metadata.Name, err)
				return err
			}
		}

		if _, err = s.EvalCtx.InternalExecutor.Query(ctx, "delete-pipe-unpush", s.EvalCtx.Txn,
			`DELETE FROM system.kwdb_unpush WHERE pusher_id=$1 AND type=$2`,
			s.spec.Metadata.ID, sqlbase.CDCInstanceType_Pipe,
		); err != nil {
			log.Errorf(ctx, "pipe[%s] sends unsend failed. %v", s.spec.Metadata.Name, err)
			return err
		}
	}

	return nil
}

// SendHistoryParallel reads and sends data in parallel according to the table.
func (s *pipeProcessor) sendHistoryParallel(
	ctx context.Context, pipeMetadata *cdcpb.PipeMetadata,
) error {
	g := ctxgroup.WithContext(ctx)

	for i := range pipeMetadata.TableList {
		table := pipeMetadata.TableList[i]
		g.GoCtx(func(ctx context.Context) error {
			errConn := s.sendTableHistory(table, pipeMetadata.Filter)
			if errConn != nil {
				log.Errorf(s.Ctx, "failed to process historical records with error: %v", errConn)
			}

			return errConn
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// sendTableHistory fetches the historical records between low-water mark
// and the last data timestamp (aka, high-water mark) in watched TS Table.
func (s *pipeProcessor) sendTableHistory(detail *cdcpb.CDCTable, filter string) error {
	s.mutex.Lock()
	isUnprocessed := s.unprocessedMap[detail.TableID]
	s.mutex.Unlock()
	if isUnprocessed {
		if s.historyHighOSN < s.getLowWatermark(detail.TableID) {
			return nil
		}

		// unprocessed data
		s.mutex.Lock()
		s.highWaterMark[detail.TableID] = s.historyHighOSN
		s.mutex.Unlock()

	} else {
		// history data
		// extracts the timestamp of last record in watched TS table as the high-water mark of historical data.
		if err := s.loadHighWaterMark(detail, filter); err != nil {
			return err
		}
	}

	return s.sendHistoryLoop(detail, filter)
}

// SendHistoryLoop reads and sends historical data in batches according to the set number of rows,
// until the remaining number of historical data rows is less than one tenth of the set value
func (s *pipeProcessor) sendHistoryLoop(detail *cdcpb.CDCTable, filter string) error {
	transFuncList := make([]timestampTransFunc, len(detail.OutputColumnTypes)+1)
	for idx, typ := range detail.OutputColumnTypes {
		transFuncList[idx] = constructTimestampFunc(typ)
	}

	flushLimit := int(float64(s.spec.Metadata.BufferSize) * bufferFlushThreshold)

	buf := newPipeBuffer(genOutPutHead(detail, kafkaMgsKindSnapshot), flushLimit)
	send := func(isSync bool) error {
		if buf.count() == 0 {
			return nil
		}

		buf.end()
		err := s.sendValue(&cdcpb.TsChangeDataCaptureValue{
			Ts:      buf.maxTs,
			Val:     [][]byte{buf.bytes()},
			TableID: detail.TableID,
		}, isSync)
		if err != nil {
			return err
		}

		buf.reset()

		return nil
	}
	f := tree.NewFmtCtx(tree.FmtExport)
	totalNum := 0
	totalSucceed := 0
	s.mutex.Lock()
	isUnprocessed := s.unprocessedMap[detail.TableID]
	s.mutex.Unlock()

	for {
		if s.Ctx.Err() != nil {
			return s.Ctx.Err()
		}

		sendNum, succeed, ts, err := s.sendHistoryBatch(detail, filter, transFuncList, f, buf, send, isUnprocessed)
		if err != nil {
			return err
		}

		totalNum += sendNum
		totalSucceed += succeed
		log.Infof(
			s.Ctx, "send table %s.%s historical rows %d,total %d", detail.Database, detail.Table, totalSucceed, totalNum,
		)

		// If historical data is processed in batches, the low_watermark is updated by batch time.
		nextTs := s.getLowWatermark(detail.TableID) + s.historicalSegmentInterval[detail.TableID]
		if nextTs <= s.highWaterMark[detail.TableID] {
			ts = nextTs
		} else {
			ts = s.highWaterMark[detail.TableID] + 1
		}

		if sendNum > 0 && ts > s.getLowWatermark(detail.TableID) {
			if err = s.sink[detail.TableID].Flush(s.Ctx); err != nil {
				return err
			}
			if isUnprocessed {
				if err = s.persistLowWatermark(map[uint64]int64{
					detail.TableID: ts,
				}); err != nil {
					return err
				}
			}

		}
		s.setLowWatermark(detail.TableID, ts)

		// If the segment time is 0, or the time exceeds the high watermark, the historical data has been sent.
		if s.historicalSegmentInterval[detail.TableID] == 0 || ts > s.highWaterMark[detail.TableID] {
			break
		}
	}

	s.mutex.Lock()
	s.unprocessedMap[detail.TableID] = true
	s.mutex.Unlock()
	if err := s.persistLowWatermark(map[uint64]int64{
		detail.TableID: s.historyHighOSN,
	}); err != nil {
		return err
	}

	return nil
}

// sendHistoryBatch reads and sends a batch historical data.
func (s *pipeProcessor) sendHistoryBatch(
	detail *cdcpb.CDCTable,
	filter string,
	transFuncList []timestampTransFunc,
	fmtCtx *tree.FmtCtx,
	buf *pipeBuffer,
	send func(bool) error,
	isUnprocessed bool,
) (int, int, int64, error) {
	var tsIndex int
	var sb strings.Builder
	var inputTime int64
	succeed := 0
	highWater := int64(cdcpb.InvalidWatermark)

	if isUnprocessed {
		// there is 3 hidden column.
		tsIndex = 3 + len(detail.PrimaryTagColumnNames)
	}

	rows, err := s.queryHistoryBatch(detail, filter, isUnprocessed)
	if err != nil {
		return 0, succeed, cdcpb.InvalidWatermark, err
	}

	for i, row := range rows {
		if isUnprocessed {
			osn := uint64(tree.MustBeDInt(row[0]))
			opByte := []byte(tree.MustBeDBytes(row[1]))[0]
			switch opByte {
			case cdcpb.OperationInsert:
				if row[2] != tree.DNull {
					// Ignore the insert device(tag) and insert the device during the first data insertion.
					continue
				}
			case cdcpb.OperationUpdateNormalTag:
				// update normal tag
				sb.WriteString(fmt.Sprintf("UPDATE %s ", detail.GetFullTABLE()))
				isFirst := true
				outputColChanged := false
				for _, normalCol := range detail.NormalTagColumnNames {
					for colIdx, outCol := range detail.OutputColumnNames {
						if outCol == normalCol {
							outputColChanged = true
							if isFirst {
								sb.WriteString("SET")
								isFirst = false
							} else {
								sb.WriteString(",")
							}
							sb.WriteString(fmt.Sprintf(" %s = %s", outCol, row[tsIndex+1+colIdx]))
							break
						}
					}
				}
				if !outputColChanged {
					continue
				}
				for j, pTag := range detail.PrimaryTagColumnNames {
					if j == 0 {
						sb.WriteString(" WHERE")
					} else {
						sb.WriteString(" AND")
					}
					sb.WriteString(fmt.Sprintf(" %s = %s ", pTag, row[j+3].String()))
				}

				if err = s.sendStatement(detail, osn, cdcpb.EventUpdate, sb.String()); err != nil {
					return 0, succeed, cdcpb.InvalidWatermark, err
				}
				sb.Reset()

				continue
			case cdcpb.OperationDeleteTag:
				// delete primary tag
				sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE", detail.GetFullTABLE()))
				for j, pTag := range detail.PrimaryTagColumnNames {
					if j != 0 {
						sb.WriteString(" AND ")
					}
					sb.WriteString(fmt.Sprintf(" %s = %s ", pTag, row[j+3].String()))
				}
				if err = s.sendStatement(detail, osn, cdcpb.EventDelete, sb.String()); err != nil {
					return 0, succeed, cdcpb.InvalidWatermark, err
				}
				sb.Reset()
				continue
			case cdcpb.OperationDeleteMetric:
				// delete from range
				precision := detail.TsColumnPrecision
				precisionDatum := tree.TimeFamilyPrecisionToRoundDuration(precision)
				sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", detail.GetFullTABLE()))
				spanBytes := []byte(tree.MustBeDBytes(row[2]))
				var startInt, endInt int64
				if len(spanBytes) == 16 {
					startInt = int64(binary.LittleEndian.Uint64(spanBytes[0:8]))
					endInt = int64(binary.LittleEndian.Uint64(spanBytes[8:]))
					start := tree.MakeDTimestampTZ(timeutil.FromTimestamp(startInt, precision), precisionDatum)
					end := tree.MakeDTimestampTZ(timeutil.FromTimestamp(endInt, precision), precisionDatum)

					sb.WriteString(fmt.Sprintf(" %s >= %v AND %s <= %v",
						detail.TsColumn, start.String(), detail.TsColumn, end.String()))
				}

				for j, pTag := range detail.PrimaryTagColumnNames {
					sb.WriteString(fmt.Sprintf(" AND %s = %s", pTag, row[j+3].String()))
				}
				if err = s.sendStatement(detail, osn, cdcpb.EventDelete, sb.String()); err != nil {
					return 0, succeed, cdcpb.InvalidWatermark, err
				}
				sb.Reset()
				continue
			default:
				log.Warningf(s.Ctx, "unsupprorted operation %v", opByte)
				continue
			}
		}

		if row[tsIndex] != tree.DNull {
			inputTime = timeutil.ToUnixMilli((*row[tsIndex].(*tree.DTimestampTZ)).Time)
		}

		if i == 0 || highWater < inputTime {
			highWater = inputTime
		}

		jsonRow, err := s.buildJSON(row[tsIndex+1:], transFuncList)
		if err != nil {
			return 0, succeed, cdcpb.InvalidWatermark, err
		}
		jsonRow.Format(fmtCtx)
		length := len(fmtCtx.Bytes())

		if buf.flushLimit <= 0 {
			buf.push(highWater, fmtCtx.Bytes())
			if err = send(true); err != nil {
				return 0, succeed, cdcpb.InvalidWatermark, err
			}
			succeed++
			fmtCtx.Reset()
			continue
		} else if length > buf.flushLimit {
			return 0, succeed, cdcpb.InvalidWatermark, errMessageBytesExceedLimit
		}

		if buf.needFlush(length) {
			if err = send(true); err != nil {
				return 0, succeed, cdcpb.InvalidWatermark, err
			}
			succeed += buf.count()
		}

		buf.push(highWater, fmtCtx.Bytes())
		fmtCtx.Reset()
	}

	if err = send(false); err != nil {
		return 0, succeed, cdcpb.InvalidWatermark, err
	}
	succeed += buf.count()

	return len(rows), succeed, highWater, nil
}

// queryHistoryBatch reads a batch historical data.
func (s *pipeProcessor) queryHistoryBatch(
	detail *cdcpb.CDCTable, filter string, isUnprocessed bool,
) ([]tree.Datums, error) {
	var rows []tree.Datums
	var err error
	var params []interface{}

	queryFormat := "SELECT %s FROM %s WHERE %s"
	cols := detail.TsColumn + ", " + strings.Join(detail.OutputColumnNames, ",")
	spanCol := detail.TsColumn
	lowWater := s.getLowWatermark(detail.TableID)
	if isUnprocessed {
		cols = fmt.Sprintf(
			"%s, %s, %s, %s, %s",
			opt.HiddenOSNColumnName,
			opt.HiddenOperationColumnName,
			opt.HiddenEventColumnName,
			strings.Join(detail.PrimaryTagColumnNames, ","), cols)
		spanCol = opt.HiddenOSNColumnName
		queryFormat += " ORDER BY " + opt.HiddenOSNColumnName
		var eventFilter []string
		for key := range s.operatorMap {
			switch key {
			case cdcpb.EventInsert:
				eventFilter = append(eventFilter, "(("+filter+") AND _op = '\x01')")
			case cdcpb.EventDelete:
				eventFilter = append(eventFilter, "_op = '\x03' OR _op = '\x04'")
			case cdcpb.EventUpdate:
				eventFilter = append(eventFilter, "_op = '\x02'")
			}
		}
		if len(eventFilter) == 0 {
			// only DDL
			return rows, nil
		}

		filter = "( " + strings.Join(eventFilter, " OR ") + " )"
	} else {
		// history data without insert data, return empty.
		if _, ok := s.operatorMap[cdcpb.EventInsert]; !ok {
			return rows, nil
		}
	}

	if lowWater > InvalidWatermark {
		filter += fmt.Sprintf(" AND %s >= $1", spanCol)
		if isUnprocessed {
			params = append(params, lowWater)
		} else {
			params = append(params, timeutil.FromUnixMilli(lowWater).UTC())
		}

	}
	if s.historicalSegmentInterval[detail.TableID] > 0 {
		filter += fmt.Sprintf(" AND %s < $%d", spanCol, len(params)+1)
		if isUnprocessed {
			params = append(params, lowWater+s.historicalSegmentInterval[detail.TableID])
		} else {
			params = append(params, timeutil.FromUnixMilli(lowWater+s.historicalSegmentInterval[detail.TableID]).UTC())
		}
	}

	query := fmt.Sprintf(
		queryFormat,
		cols,
		detail.GetFullTABLE(),
		filter,
	)

	rows, err = s.FlowCtx.EvalCtx.InternalExecutor.Query(
		s.Ctx,
		"pipe-query-history",
		s.FlowCtx.Txn,
		query,
		params...,
	)
	if err != nil {
		return nil, err
	}

	log.Info(s.Ctx, "query historical rows from ", detail.Table, params, ", total:", len(rows))

	return rows, nil
}

// evaluateHistoricalData evaluates the amount of historical data,
// and if it exceeds the threshold, it processes in batches.
func (s *pipeProcessor) evaluateHistoricalData(detail *cdcpb.CDCTable, filter string) error {
	var err error
	lowWater := s.getLowWatermark(detail.TableID)

	if lowWater > InvalidWatermark {
		return s.evaluateUnprocessedData(detail, filter)
	}

	queryFormat := "SELECT count(*),first(%s),last(%s) FROM %s.%s WHERE %s"
	query := fmt.Sprintf(
		queryFormat,
		detail.TsColumn,
		detail.TsColumn,
		detail.Database,
		detail.Table,
		filter,
	)

	row, err := s.FlowCtx.EvalCtx.InternalExecutor.QueryRow(
		s.Ctx,
		"pipe-count-history",
		s.FlowCtx.Txn,
		query,
	)

	if err != nil {
		return err
	}

	totalRows := int64(tree.MustBeDInt(row[0]))
	if totalRows == 0 || row[1] == tree.DNull {
		return nil
	}

	firstTime := tree.MustBeDTimestampTZ(row[1])
	firstTs := firstTime.UnixMilli()
	lastTime := tree.MustBeDTimestampTZ(row[2])
	lastTs := lastTime.UnixMilli()
	if lastTs <= firstTs {
		return nil
	}

	batchRows := cdcpb.CalculateBatchRows(detail.OutputColumnTypes)

	// Divide the total rows by the historicalSnapshotMaxLimit to get the batch number,
	// and then divide the total time range by the batch number to get the time interval for each batch.
	s.splitBatch(totalRows, batchRows, lastTs, firstTs, detail, false)

	return nil
}

// evaluateUnprocessedData evaluates the amount of unprocessed data,
// and if it exceeds the threshold, it processes in batches.
func (s *pipeProcessor) evaluateUnprocessedData(detail *cdcpb.CDCTable, filter string) error {
	var err error
	lowWater := s.getLowWatermark(detail.TableID)
	queryFormat := "SELECT count(*),min(_osn),max(_osn) FROM %s.%s WHERE %s AND _op = '\x01' AND %s >= $1"
	query := fmt.Sprintf(
		queryFormat,
		detail.Database,
		detail.Table,
		filter,
		opt.HiddenOSNColumnName,
	)

	row, err := s.FlowCtx.EvalCtx.InternalExecutor.QueryRow(
		s.Ctx,
		"pipe-count-unprocessed",
		s.FlowCtx.Txn,
		query,
		lowWater,
	)

	if err != nil {
		return err
	}

	totalRows := int64(tree.MustBeDInt(row[0]))
	if totalRows == 0 {
		return nil
	}

	firstTs := int64(tree.MustBeDInt(row[1]))
	lastTs := int64(tree.MustBeDInt(row[2]))

	batchRows := cdcpb.CalculateBatchRows(detail.OutputColumnTypes)
	s.splitBatch(totalRows, batchRows, lastTs, firstTs, detail, true)

	return nil
}

// splitBatch divides the total rows by the historicalSnapshotMaxLimit to get the batch number,
// and then divide the total time range by the batch number to get the time interval for each batch.
func (s *pipeProcessor) splitBatch(
	totalRows, batchRows, lastTs, firstTs int64, detail *cdcpb.CDCTable, isUnprocessed bool,
) {
	if batchRows > 0 && totalRows > batchRows {
		batchNum := totalRows / batchRows
		if batchNum == 0 {
			return
		}

		tsSpan := lastTs - firstTs
		if tsSpan < batchNum {
			batchNum = tsSpan
		}

		s.historicalSegmentInterval[detail.TableID] = tsSpan / batchNum
		if s.historicalSegmentInterval[detail.TableID] > 0 {
			s.needSplitHistory = true
		}

		s.setLowWatermark(detail.TableID, firstTs)

		var start, end time.Time
		if isUnprocessed {
			start = timeutil.FromUnixNano(firstTs)
			end = timeutil.FromUnixNano(lastTs)
		} else {
			start = timeutil.FromUnixMilli(firstTs)
			end = timeutil.FromUnixMilli(lastTs)
		}

		log.Infof(s.Ctx, "pipe history need batched, table %s.%s, total %d, span %s-%s, batchsize %d, batchinterval %d",
			detail.Database,
			detail.Table,
			totalRows,
			start,
			end,
			batchNum,
			s.historicalSegmentInterval[detail.TableID],
		)
	}
}

// buildJSON encodes one output row to DJSON format. Meanwhile, use the customized timestamp transform functions to
// convert the timestamp/timestamptz value to int64 based on its precision.
func (s *pipeProcessor) buildJSON(
	args tree.Datums, timestampTransFuncList []timestampTransFunc,
) (tree.Datum, error) {
	builder := json.NewArrayBuilder(len(args))
	for idx, arg := range args {
		var data tree.Datum

		typ := arg.ResolvedType()
		switch typ.Oid() {
		case oid.T_timestamptz:
			val := tree.MustBeDTimestampTZ(arg)
			transFuncList := timestampTransFuncList[idx]
			if transFuncList != nil {
				data = tree.NewDInt(tree.DInt(transFuncList(val.UTC())))
			} else {
				data = arg
			}
		case oid.T_timestamp:
			val := tree.MustBeDTimestamp(arg)
			transFuncList := timestampTransFuncList[idx]
			if transFuncList != nil {
				data = tree.NewDInt(tree.DInt(transFuncList(val.UTC())))
			} else {
				data = arg
			}
		default:
			data = arg
		}
		j, err := tree.AsJSON(data, s.FlowCtx.EvalCtx.GetLocation())
		if err != nil {
			return nil, err
		}
		builder.Add(j)
	}

	return tree.NewDJSON(builder.Build()), nil
}

// constructTimestampFunc used for convert the timestamp/timestamptz value to int64 based on its precision.
func constructTimestampFunc(tpy string) timestampTransFunc {
	switch strings.ToUpper(tpy) {
	case milliTimestampType, milliTimestampTZType:
		return func(ts time.Time) int64 {
			return timeutil.ToUnixMilli(ts)
		}
	case microTimestampType, microTimestampTZType:
		return func(ts time.Time) int64 {
			return timeutil.ToUnixMicros(ts)
		}
	case nanoTimestampType, nanoTimestampTZType:
		return func(ts time.Time) int64 {
			return ts.UnixNano()
		}
	case defaultTimestampType, defaultTimestampTZType:
		return func(ts time.Time) int64 {
			return timeutil.ToUnixMilli(ts)
		}
	default:
		return nil
	}
}

// connectCDC connects the CDCCoordinator.
func (s *pipeProcessor) connectCDC(ctx context.Context) {
	s.grpcGroup = ctxgroup.WithContext(ctx)
	if err := s.checkNodeChange(); err != nil {
		s.errCh <- err
		s.cancel()

		return
	}

	s.unregisterGossip = s.FlowCtx.Cfg.Gossip.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyNodeIDPrefix),
		func(_ string, value roachpb.Value) {
			if err := s.checkNodeChange(); err != nil {
				log.Warningf(
					s.Ctx, "the checkNodeChange of pipe %s found error. %s", s.spec.Metadata.Name, err)
			}
		})
}

// startGrpc creates connections to each node and handles the incoming heartbeat message.
func (s *pipeProcessor) startRealtimeProcessor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startRealTime = true
	for _, table := range s.spec.Metadata.TableList {
		event := &cdcpb.TsChangeDataCaptureEvent{
			Start: &cdcpb.TsChangeDataCaptureStart{
				TableID:      table.TableID,
				InstanceID:   uint64(s.spec.JobID),
				InstanceType: sqlbase.CDCInstanceType_Pipe,
			},
		}

		for _, client := range s.cdcClients {
			if err := (*client).Send(event); err != nil {
				log.Warningf(s.Ctx, "startRealtimeProcessor %v", err)
			}
		}
	}
}

// createGrpcConn create a gRPC connection to CDC.
func (s *pipeProcessor) createGRPCConn(
	ctx context.Context, nodeID roachpb.NodeID, onDone func(),
) error {
	var client cdcpb.CDCCoordinator_TsCDCClient
	wgDone := false
	defer func() {
		if !wgDone {
			onDone()
		}
		s.mu.Lock()
		delete(s.cdcClients, nodeID)
		s.mu.Unlock()
	}()

	addr, err := s.FlowCtx.Cfg.Gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}

	conn, err := s.FlowCtx.Cfg.RPCContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}

	if client, err = cdcpb.NewCDCCoordinatorClient(conn).TsCDC(ctx); err != nil {
		return err
	}

	req := &cdcpb.TsChangeDataCaptureRequest{
		PipeMetadata: &s.spec.Metadata,
		InstanceType: sqlbase.CDCInstanceType_Pipe,
		InstanceID:   uint64(s.spec.JobID),
	}
	event := &cdcpb.TsChangeDataCaptureEvent{
		Request: req,
	}
	s.mu.Lock()
	if err = client.Send(event); err != nil {
		return err
	}

	s.cdcClients[nodeID] = &client
	s.mu.Unlock()
	taskNum := len(s.spec.Metadata.TableList)
	for {
		// receives the heartbeat messages from pipe coordinator.
		data, err := client.Recv()
		if err != nil {
			return err
		}

		if !wgDone && taskNum == 0 {
			if s.startRealTime {
				for _, table := range s.spec.Metadata.TableList {
					event = &cdcpb.TsChangeDataCaptureEvent{
						Start: &cdcpb.TsChangeDataCaptureStart{
							TableID:      table.TableID,
							InstanceID:   uint64(s.spec.JobID),
							InstanceType: sqlbase.CDCInstanceType_Pipe,
						},
					}
					if err = client.Send(event); err != nil {
						return err
					}
				}
			}
			onDone()
			wgDone = true
		} else if !wgDone {
			taskNum--
		}

		if !s.startRealTime {
			if err = s.sendMessage(data); err != nil {
				return err
			}
			continue
		}

		select {
		case s.buffer <- data:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// checkNodeChange check node changed of cluster.
func (s *pipeProcessor) checkNodeChange() error {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	nodeList, err := s.FlowCtx.Cfg.CDCCoordinator.LiveNodeIDList(s.Ctx)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for _, nodeID := range nodeList {
		id := nodeID
		s.mu.Lock()
		_, exist := s.cdcClients[id]
		s.mu.Unlock()

		if !exist {
			wg.Add(1)
			s.grpcGroup.GoCtx(func(ctx context.Context) error {
				errConn := s.createGRPCConn(ctx, id, func() {
					wg.Done()
				})
				// errConn includes:
				// 1. Connection errors, like "transport is closing"
				// 2. errors occurring during historical data transmission
				// 3. Reception of CDC errors or the "stopped successfully" stop command necessitates closing the pipe job.
				// 4. Context cancel error
				if sqlutil.ShouldLogError(errConn) {
					log.Errorf(s.Ctx, "stream internal gRPC connection for node %d is disconnected with error: %s",
						nodeID, errConn)
				}

				if strings.Contains(errConn.Error(), "stopped successfully") {
					s.errCh <- errConn
					s.cancel()

					return errConn
				}

				if strings.Contains(errConn.Error(), "transport is closing") {
					// resend data from low-watermark to now
					_ = s.sendHistoryParallel(ctx, &s.spec.Metadata)
				}

				return nil
			})
		}
	}
	wg.Wait()

	return nil
}

// Next processes the heartbeat messages coming from pipe coordinator.
// 1. initialize a checkpoint timer.
// 2. save the low-water mark to watermarkCache using nodeID.
// 3. for each checkpoint interval, compute the global low-water mark and persist it into pipe catalog table.
// 4. if meets the heartbeat timeout, stop the current pipe job.
func (s *pipeProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	var checkpointTimer timeutil.Timer
	defer checkpointTimer.Stop()

	checkpointTimer.Reset(s.checkpointInterval)

	for s.State == execinfra.StateRunning {
		select {
		case data, ok := <-s.buffer:
			if !ok || data == nil {
				break
			}

			err := s.sendMessage(data)
			if err != nil {
				s.MoveToDraining(err)
				break
			}
		case <-checkpointTimer.C:
			checkpointTimer.Read = true
			if err := s.checkNodeChange(); err != nil {
				s.MoveToDraining(err)
				break
			}

			if err := s.checkpoint(); err != nil {
				s.MoveToDraining(err)
				break
			}
			checkpointTimer.Reset(s.checkpointInterval)
		case err := <-s.errCh:
			s.MoveToDraining(err)
			break
		}
	}
	return nil, s.DrainHelper()
}

// sendMessage sends TsChangeDataCaptureEvent to Sink.
func (s *pipeProcessor) sendMessage(event *cdcpb.TsChangeDataCaptureEvent) error {
	var err error
	switch t := event.GetValue().(type) {
	case *cdcpb.TsChangeDataCaptureHeartbeat:
		receivedWatermark := t.LocalWaterMark
		nodeID := t.NodeID

		{
			s.mutex.Lock()
			defer s.mutex.Unlock()
			currentTime := timeutil.ToUnixMilli(timeutil.Now())
			if watermark, ok := s.watermarkCache[t.TableID][nodeID]; ok {
				if receivedWatermark != cdcpb.InvalidWatermark && watermark.LocalWatermark < receivedWatermark {
					watermark.LocalWatermark = receivedWatermark
				}
				watermark.ReceivedTimestamp = currentTime
			} else {
				s.watermarkCache[t.TableID][nodeID] = &LocalWatermark{LocalWatermark: receivedWatermark, ReceivedTimestamp: currentTime}
			}
		}

	case *cdcpb.TsChangeDataCaptureValue:
		err = s.sendValue(t, true)
	case *cdcpb.TsChangeDataCaptureStop:
		// sends STOP message to each node.
		req := &cdcpb.TsChangeDataCaptureStop{TableID: t.TableID, InstanceID: t.InstanceID, InstanceType: sqlbase.CDCInstanceType_Pipe}
		err = s.stopPipe(req)
	case *cdcpb.TsChangeDataCaptureError:
		log.VErrEventf(s.Ctx, 2, "TsChangeDataCaptureError: %s", t.Error.GoError())
		_ = s.stopPipe(nil)
		return t.Error.GoError()
	}

	return err
}

// stopPipe sends the STOP message to all active nodes.
func (s *pipeProcessor) stopPipe(_ *cdcpb.TsChangeDataCaptureStop) error {
	var finalErr error

	s.mu.Lock()
	// close and clean cdcClients
	for _, client := range s.cdcClients {
		err := (*client).CloseSend()
		if err != nil {
			finalErr = err
		}
	}
	s.cdcClients = make(map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient)
	defer s.mu.Unlock()

	if finalErr == nil {
		log.Error(s.Ctx, finalErr)
	}

	if !s.startRealTime {
		s.cancel()
	}

	return errors.New("stopped successfully")
}

// sendHeartbeatToCDC sends the heartbeat message to all active nodes.
func (s *pipeProcessor) sendHeartbeatToCDC() error {
	for nodeID, client := range s.cdcClients {
		event := &cdcpb.TsChangeDataCaptureEvent{
			Heartbeat: &cdcpb.TsChangeDataCaptureHeartbeat{
				NodeID: int32(nodeID),
			},
		}
		if err := (*client).Send(event); err != nil {
			return err
		}
	}

	return nil
}

// sendValue sends TsChangeDataCaptureValue to Sink.
func (s *pipeProcessor) sendValue(msg *cdcpb.TsChangeDataCaptureValue, isSync bool) error {
	if err := s.sink[msg.TableID].Send(s.Ctx, strconv.FormatInt(msg.Ts, 10), msg.Val[0]); err != nil {
		log.Errorf(s.Ctx, "failed to send value to sink: %v, length: %d", err, len(msg.Val[0]))
		return err
	}

	if isSync {
		if err := s.sink[msg.TableID].Flush(s.Ctx); err != nil {
			log.Errorf(s.Ctx, "failed to flush sink: %v, length: %d", err)
			return err
		}
	}

	return nil
}

// sendStatement sends statement to Sink.
func (s *pipeProcessor) sendStatement(
	detail *cdcpb.CDCTable, osn uint64, operation, stmt string,
) error {
	format := MessageFormat{
		Database:  detail.Database,
		Schema:    detail.Schema,
		Table:     detail.Table,
		Kind:      operation,
		Statement: stmt,
	}
	msg, err := gojson.Marshal(format)
	if err != nil {
		return err
	}

	err = s.sendValue(&cdcpb.TsChangeDataCaptureValue{
		Ts:      int64(osn),
		Val:     [][]byte{msg},
		TableID: detail.TableID,
		OSN:     osn,
	}, true)

	return err
}

// checkpoint reads the watermark from the cache, checks for connection timeout,
// and writes it to the system table.
func (s *pipeProcessor) checkpoint() error {
	lowWaterMark, err := s.extractGlobalLowWaterMark()
	if err != nil {
		log.Errorf(s.Ctx, "checkpoint error: %s", err)
		return err
	}

	if err = s.loadLowWaterMark(); err != nil {
		return err
	}

	if err = s.persistLowWatermark(lowWaterMark); err != nil {
		return err
	}

	if err = s.sendHeartbeatToCDC(); err != nil {
		return err
	}

	return nil
}

func (s *pipeProcessor) ConsumerDone() {
	s.MoveToDraining(nil /* err */)
}

func (s *pipeProcessor) ConsumerClosed() {
	s.close()
}

func (s *pipeProcessor) close() {
	s.InternalClose()

	if s.unregisterGossip != nil {
		s.unregisterGossip()
	}

	for _, table := range s.spec.Metadata.TableList {
		sink, ok := s.sink[table.TableID]
		if ok {
			_ = sink.Close()
		}
	}
}

// extractGlobalLowWaterMark computes the global low-water mark.
//  1. Use math.MinInt64 as InvalidWatermark, and it will be ignored when computing the global low-water mark.
//  2. Use the smallest local low-water mark as the global low-water mark.
//  3. If no data has been captured, the value of local low-water mark is InvalidWatermark.
//  4. If data has been captured by a gateway node, the value of local low-water mark rises to a non-zero value.
//  5. The valid local low-water mark will be sent to pipeProcessor using Heartbeat.
//  6. If the heartbeat call is returned successfully, the local low-water mark will be reset to InvalidWatermark.
//  7. The watermarkCache stores the local low-water marks received from each node,
//  8. The watermarkCache also records the receiving time of each local low-water mark.
//  9. Use TsPipeHeartbeatInterval * pipeSinkMaxRetries as the heartbeatTimeout.
//  10. If it's failed to receive valid low-water mark more than heartbeatTimeout, the pipeProcessor will return
//     an error and the current pipe job will be terminated.
func (s *pipeProcessor) extractGlobalLowWaterMark() (map[uint64]int64, error) {
	globalWaterMark := make(map[uint64]int64)
	currentTime := timeutil.ToUnixMilli(timeutil.Now())
	{
		s.mutex.Lock()
		defer s.mutex.Unlock()

		for tableID, watermarkMap := range s.watermarkCache {
			globalWaterMark[tableID] = math.MaxInt64
			for nodeID, watermark := range watermarkMap {
				ts := watermark.LocalWatermark

				if currentTime-watermark.ReceivedTimestamp > s.heartbeatTimeout.Milliseconds() {
					// return globalWaterMark, errors.Errorf(`the heartbeat of node '%d' is timeout`, nodeID)
					log.Infof(s.Ctx, `the heartbeat of node '%d' is timeout`, nodeID)
				}

				if ts == cdcpb.InvalidWatermark {
					continue
				}
				if ts < globalWaterMark[tableID] {
					globalWaterMark[tableID] = ts
					watermark.LocalWatermark = cdcpb.InvalidWatermark
				}
			}

			if globalWaterMark[tableID] == math.MaxInt64 {
				globalWaterMark[tableID] = cdcpb.InvalidWatermark
			}
		}
	}

	return globalWaterMark, nil
}

// persistLowWatermark saves LowWaterMark to system table.
func (s *pipeProcessor) persistLowWatermark(watermarkMap map[uint64]int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for tableID, watermark := range watermarkMap {
		// all the record timestamps of historical rows are smaller than s.highWaterMark and have been processed.
		// update global low-water mark using s.highWaterMark.
		if watermark == InvalidWatermark || watermark <= s.lowWaterMark[tableID] {
			continue
		}

		if _, err := s.FlowCtx.Cfg.Executor.ExecEx(
			s.Ctx,
			"update-water-mark",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_cdc_watermark SET low_watermark = $1 WHERE table_id = $2 AND task_id = $3 `,
			watermark,
			tableID,
			s.spec.Metadata.ID,
		); err != nil {
			return err
		}
		s.lowWaterMark[tableID] = watermark
	}

	return nil
}

// loadLowWaterMark loads LowWaterMark from system table.
func (s *pipeProcessor) loadLowWaterMark() error {
	rows, err := s.FlowCtx.Cfg.Executor.QueryEx(
		s.Ctx,
		"load-watermark",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT table_id, low_watermark FROM system.kwdb_cdc_watermark WHERE task_id = $1`,
		s.spec.Metadata.ID,
	)

	if err != nil {
		return errors.Errorf(`failed to fetch pipe %q.`, s.spec.Metadata.Name)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	for i, row := range rows {
		tableID := uint64(tree.MustBeDInt(row[0]))
		s.lowWaterMark[tableID] = int64(tree.MustBeDInt(row[1]))
		if i == 0 || s.minWatermark > s.lowWaterMark[tableID] {
			s.minWatermark = s.lowWaterMark[tableID]
		}

		if _, ok := s.unprocessedMap[tableID]; !ok {
			s.unprocessedMap[tableID] = s.lowWaterMark[tableID] > InvalidWatermark
		}
	}

	return nil
}

// loadHighWaterMark loads HighWaterMark from CDC table.
func (s *pipeProcessor) loadHighWaterMark(detail *cdcpb.CDCTable, filter string) error {
	highWaterMarkQueryFormat := "SELECT last_row(%s)::timestamp FROM %s.%s WHERE %s "
	highWaterQuery := fmt.Sprintf(
		highWaterMarkQueryFormat,
		detail.TsColumn,
		detail.Database,
		detail.Table,
		filter,
	)

	row, err := s.FlowCtx.EvalCtx.InternalExecutor.QueryRow(
		s.Ctx,
		"pipe-query-history-high-water",
		s.FlowCtx.Txn,
		highWaterQuery,
	)
	if err != nil {
		return err
	}

	if row[0] != tree.DNull {
		lastTs, ok := tree.AsDTimestamp(row[0])
		if !ok {
			return errors.Errorf("failed to extract last timestamp from history records")
		}
		s.mutex.Lock()
		s.highWaterMark[detail.TableID] = lastTs.UnixMilli()
		s.mutex.Unlock()
	}

	return nil
}

func (s *pipeProcessor) OutputTypes() []types.T {
	return []types.T{}
}

// InitProcessorProcedure init processor in procedure
func (s *pipeProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if s.EvalCtx.IsProcedure {
		if s.FlowCtx != nil {
			s.FlowCtx.Txn = txn
		}
		s.Closed = false
		s.State = execinfra.StateRunning
		s.Out.SetRowIdx(0)
	}
}

var _ execinfra.Processor = &pipeProcessor{}
var _ execinfra.RowSource = &pipeProcessor{}
