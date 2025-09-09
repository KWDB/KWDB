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
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/engine/tse"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	streamReaderProcName = `StreamReader`
)

func init() {
	rowexec.NewStreamReaderProcessor = newStreamReaderProcessor
}

// streamReaderProcessor is used to process execinfrapb.StreamReaderSpec.
// It is called by the stream job and is responsible for handling stream-computing
// of historical data and real-time data.
type streamReaderProcessor struct {
	execinfra.ProcessorBase
	// spec is the object processed by this processor.
	spec *execinfrapb.StreamReaderSpec
	// streamParameters is parameters of stream.
	streamParameters *sqlutil.StreamParameters
	// errCh is used to receive and uniformly handle all errors within the processor.
	errCh chan error
	// notifyCh receives signals for the next row of data.
	notifyCh chan bool
	// cancel is used to cancel the current processor.
	cancel func()

	// activeNodeList stores the IDs of active nodes.
	activeNodeList []int32
	// sourceTableID is source table ID.
	sourceTableID uint64
	// instanceID is stream ID.
	instanceID uint64
	// streamSink stores the information stream query.
	streamSink *sqlutil.StreamSink

	// streamOpts is options of stream.
	streamOpts *sqlutil.ParsedStreamOptions

	// waterMarkMutex is mutex of watermarkCache.
	waterMarkMutex syncutil.Mutex
	// watermarkCache stores the watermarks of all nodes.
	watermarkCache map[int32]*LocalWatermark
	// lowWaterMark is the low-watermark
	lowWaterMark int64
	// lowWaterMark is the high-watermark
	highWaterMark int64
	// heartbeatTimeout is the gRPC of CDC heartbeat timeout period.
	// Nodes that exceed this period are considered offline.
	heartbeatTimeout int64

	// expiredTime is the expiration time of data. Data before this time will be recalculated
	// or discarded according to user options.
	expiredTime time.Time
	// emitTimestamp is the dispatch time of data, which is updated with the latest watermark and checkpoint.
	// Newly received data that arrives after this time needs to be queued and will only be dispatched
	// when emitTimestamp exceeds its own timestamp. This mechanism is used to handle out-of-order data.
	emitTimestamp time.Time
	// ordering is used to sort the data in the rowBuffer.
	ordering sqlbase.ColumnOrdering

	// rowBuffer is used to cache the received data, sort it, and determine whether to expire or dispatch it.
	rowBuffer *streamReaderRowBuffer
	// recalculator is used to process historical data and expired data.
	recalculator *streamRecalculator
}

var _ execinfra.Processor = &streamReaderProcessor{}
var _ execinfra.RowSource = &streamReaderProcessor{}

// LocalWatermark stores local watermarks with received timestamp for each node in KaiwuDB cluster.
type LocalWatermark struct {
	// LocalWatermark stores local watermarks.
	LocalWatermark int64
	// ReceivedTimestamp stores the received timestamp.
	ReceivedTimestamp int64
}

// newStreamReaderProcessor return a new streamReaderProcessor.
func newStreamReaderProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.StreamReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	var err error
	s := &streamReaderProcessor{
		spec: spec, lowWaterMark: sqlutil.InvalidWaterMark,
	}
	if err := s.Init(
		s,
		post,
		spec.CDCColumns.CDCTypes,
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

	s.errCh = make(chan error, 2)
	s.notifyCh = make(chan bool, 1)
	s.watermarkCache = make(map[int32]*LocalWatermark)

	s.streamParameters, err = sqlutil.ParseStreamParameters(spec.Metadata.Parameters)
	if err != nil {
		return nil, err
	}

	streamOpts, err := sqlutil.ParseStreamOpts(&s.streamParameters.Options)
	if err != nil {
		return nil, err
	}

	s.streamSink = &s.streamParameters.StreamSink
	s.streamOpts = streamOpts

	s.heartbeatTimeout = sqlutil.TimeoutFactor * s.streamOpts.HeartbeatInterval.Milliseconds()
	if s.streamOpts.CheckpointInterval.Milliseconds() >= s.heartbeatTimeout {
		s.heartbeatTimeout = s.streamOpts.CheckpointInterval.Milliseconds()
	}

	s.ordering = make(sqlbase.ColumnOrdering, len(spec.OrderingColumnIDs)+1)

	for i := range spec.OrderingColumnIDs {
		s.ordering[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    int(spec.OrderingColumnIDs[i]),
			Direction: encoding.Ascending,
		}
	}

	s.ordering[len(spec.OrderingColumnIDs)] = sqlbase.ColumnOrderInfo{
		ColIdx:    0,
		Direction: encoding.Ascending,
	}

	s.expiredTime = timeutil.FromUnixMilli(sqlutil.InvalidWaterMark).UTC()
	s.emitTimestamp = timeutil.FromUnixMilli(sqlutil.InvalidWaterMark).UTC()

	return s, nil
}

// Start initializes the stream job to handle the connection to stream coordinator.
func (s *streamReaderProcessor) Start(ctx context.Context) context.Context {
	ctx, s.cancel = context.WithCancel(ctx)
	ctx = s.StartInternal(ctx, streamReaderProcName)
	streamPara, err := sqlutil.ParseStreamParameters(s.spec.Metadata.Parameters)
	if err != nil {
		s.MoveToDraining(err)
		return ctx
	}

	// check the status of stream job.
	var jobDetails jobspb.StreamDetails

	if s.spec.JobID != 0 {
		job, err := s.FlowCtx.Cfg.JobRegistry.LoadJob(ctx, s.spec.JobID)
		if err != nil {
			s.MoveToDraining(err)
			return ctx
		}

		jobDetails = job.Details().(jobspb.StreamDetails)
	} else {
		s.MoveToDraining(errors.Errorf("stream job with id %d does not exist", s.spec.JobID))
		return ctx
	}

	s.activeNodeList = jobDetails.ActiveNodeList
	s.sourceTableID = streamPara.SourceTableID
	s.instanceID = s.spec.Metadata.ID

	s.recalculator = newRecalculator(
		s.Ctx, s.FlowCtx, s.spec, streamPara, s.streamSink, s.streamOpts)
	s.rowBuffer = newStreamReaderRowBuffer(
		s.FlowCtx, s.spec, s.ordering, s.streamOpts, s.recalculator, s.streamSink.HasAgg)

	// process potential unprocessed rows, including historical rows
	if s.streamOpts.ProcessHistory {
		if err = s.recalculator.HandleHistoryRows(); err != nil {
			log.Errorf(s.Ctx, "failed to process historical rows with error: %s", err)
			s.errCh <- err
			s.cancel()
			s.notifyNext()
		}
	}

	var rowsResend []tree.Datums
	if s.streamSink.HasAgg {
		// Read the last window in the target table as the expiredTime.
		// Data before this expiredTime needs to be recalculated.
		s.expiredTime, err = s.recalculator.loadExpiredTime(s.expiredTime)
		if err != nil {
			s.MoveToDraining(err)
			return ctx
		}
		s.lowWaterMark = timeutil.ToUnixMilli(s.expiredTime)
		s.emitTimestamp = s.expiredTime

		if s.needResendLastWindow() {
			rowsResend, err = s.recalculator.loadRowsInLastWindow()
			if err != nil {
				s.MoveToDraining(err)
				return ctx
			}
		}
	}

	s.startAsyncTasks(ctx, s.spec.Metadata, jobDetails.ActiveNodeList)
	log.Infof(s.Ctx, "successful to start stream %q.", s.spec.Metadata.Name)

	if len(rowsResend) > 0 {
		// resend rows in last window,
		// prevent the split-windows of historical data and real-time data.
		if err = s.resendRowsInLastWindow(ctx, rowsResend); err != nil {
			s.errCh <- err
			s.cancel()
			s.notifyNext()
		}
	}

	return ctx
}

// resendRowsInLastWindow reads the data of the last window in the source table from the target table
// and sends it to the real-time streamAggregator
// to prevent the split-windows of historical data and real-time data.
func (s *streamReaderProcessor) resendRowsInLastWindow(
	ctx context.Context, rows []tree.Datums,
) error {
	for _, row := range rows {
		encRow := make(sqlbase.EncDatumRow, len(row))
		for i, col := range row {
			encRow[i] = sqlbase.EncDatum{Datum: col}
		}
		if err := s.rowBuffer.AddRowOutput(ctx, encRow); err != nil {
			return err
		}
	}

	return nil
}

// startAsyncTasks starts the following async tasks:
// 1. creates connections to each node to handle the incoming gRPC messages
// 2. start checkpointTask
// 3. start StreamRecalculator
func (s *streamReaderProcessor) startAsyncTasks(
	ctx context.Context, metadata *cdcpb.StreamMetadata, jobNodeList []int32,
) {
	nodeList, err := s.FlowCtx.Cfg.CDCCoordinator.LiveNodeIDList(ctx)
	if err != nil {
		s.errCh <- err
		s.cancel()
		s.notifyNext()
		return
	}

	if err = s.checkActiveNodeList(nodeList, jobNodeList); err != nil {
		s.errCh <- err
		s.cancel()
		s.notifyNext()
		return
	}

	if err = s.FlowCtx.Stopper().RunAsyncTask(ctx, "stream-streamReaderProcessor-poller", func(ctx context.Context) {
		g := ctxgroup.WithContext(ctx)

		for _, nodeID := range nodeList {
			id := nodeID
			g.GoCtx(func(ctx context.Context) error {
				_, errConn := s.createGrpcConn(ctx, metadata, id)
				if sqlutil.ShouldLogError(errConn) {
					log.Errorf(s.Ctx, "stream internal gRPC connection for node %d is disconnected with error: %s",
						nodeID, errConn)
				}
				return errConn
			})
		}

		g.GoCtx(func(ctx context.Context) error {
			checkpointErr := s.checkpointTask(ctx)
			if checkpointErr != nil {
				if sqlutil.ShouldLogError(checkpointErr) {
					log.Errorf(s.Ctx, "checkpoint task error: %s", checkpointErr)
				}
			}

			return checkpointErr
		})

		g.GoCtx(func(ctx context.Context) error {
			recalculateErr := s.recalculator.Run(ctx)
			if recalculateErr != nil {
				if sqlutil.ShouldLogError(recalculateErr) {
					log.Errorf(s.Ctx, "recalculate error: %s", recalculateErr)
				}
			}

			return recalculateErr
		})

		err = g.Wait()
		s.errCh <- err
		s.cancel()
		s.notifyNext()
	}); err != nil {
		log.Errorf(s.Ctx, "stream internal gRPC connections are disconnected with error: %s", err)
		s.errCh <- err
		s.cancel()
		s.notifyNext()
	}
}

// createGrpcConn create a grpc connection to CDCCoordinator.
func (s *streamReaderProcessor) createGrpcConn(
	ctx context.Context, metadata *cdcpb.StreamMetadata, nodeID roachpb.NodeID,
) (cdcpb.CDCCoordinator_StartTsCDCClient, error) {
	var stream cdcpb.CDCCoordinator_StartTsCDCClient

	addr, err := s.FlowCtx.Cfg.Gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := s.FlowCtx.Cfg.RPCContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}

	req := &cdcpb.TsChangeDataCaptureRequest{
		InstanceType:   cdcpb.TSCDCInstanceType_Stream,
		StreamMetadata: metadata,
		CDCColumns:     s.spec.CDCColumns,
	}

	if stream, err = cdcpb.NewCDCCoordinatorClient(conn).StartTsCDC(ctx, req); err != nil {
		return nil, err
	}

	for {
		// receive messages from CDC coordinator.
		data, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		err = s.handleMessage(data)
		if err != nil {
			return nil, err
		}
	}
}

// checkpointTask starts the checkpoint task.
func (s *streamReaderProcessor) checkpointTask(ctx context.Context) error {
	var checkpointTimer timeutil.Timer
	defer checkpointTimer.Stop()

	checkpointTimer.Reset(s.streamOpts.CheckpointInterval)

	for {
		select {
		case <-checkpointTimer.C:
			checkpointTimer.Read = true
			if err := s.checkpoint(); err != nil {
				return err
			}

			s.recalculator.Notify()
			checkpointTimer.Reset(s.streamOpts.CheckpointInterval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// checkActiveNodeList checks whether the currently active nodes are consistent with those at the last startup.
func (s *streamReaderProcessor) checkActiveNodeList(
	nodeList []roachpb.NodeID, jobNodeList []int32,
) error {
	if len(nodeList) != len(jobNodeList) {
		return errors.New("the active node in cluster have been changed")
	}

	for _, nodeID := range nodeList {
		isExist := false
		for _, JobNodeID := range jobNodeList {
			if JobNodeID == int32(nodeID) {
				isExist = true
				break
			}
		}
		if !isExist {
			return errors.New("the active node in cluster have been changed")
		}
	}

	return nil
}

// Next processes the captured rows coming from CDC coordinator.
func (s *streamReaderProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for s.State == execinfra.StateRunning {
		select {
		case err := <-s.errCh:
			// sends STOP message to each node.
			req := &cdcpb.TsChangeDataCaptureStop{
				TableID:      s.sourceTableID,
				InstanceID:   s.instanceID,
				InstanceType: cdcpb.TSCDCInstanceType_Stream,
			}
			_ = s.stopStream(req)

			s.MoveToDraining(err)
			break
		case <-s.notifyCh:
			if s.streamSink.HasAgg {
				// notified by checkpoint to trigger the stream aggregator's MAX_DELAY checking
				return nil, nil
			}
		default:
			hasRow, err := s.rowBuffer.HasOutputRow()
			if err != nil {
				s.MoveToDraining(err)
				break
			}
			if hasRow {
				row, err := s.rowBuffer.EncFirstRow()
				if err != nil {
					s.MoveToDraining(err)
					break
				}

				outRow := s.ProcessRowHelper(row)
				s.rowBuffer.Next()

				if outRow != nil {
					return outRow, nil
				}
			} else {
				// waiting for the next batch of CDC or notified by checkpoint
				select {
				case <-s.notifyCh:
					break
				}

				if s.streamSink.HasAgg {
					return nil, nil
				}
			}
		}
	}

	return nil, s.DrainHelper()
}

// ProcessRowHelper returns a row of data to the upper-level operator.
func (s *streamReaderProcessor) ProcessRowHelper(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
	outRow, ok, err := s.Out.ProcessRowForStream(s.PbCtx(), row)
	if err != nil {
		s.MoveToDraining(err)
		return nil
	}
	if !ok {
		s.MoveToDraining(nil /* err */)
	}

	return outRow
}

// handleMessage  is used to handle events sent by CDC.
func (s *streamReaderProcessor) handleMessage(event *cdcpb.TsChangeDataCaptureEvent) error {
	var err error
	switch t := event.GetValue().(type) {
	case *cdcpb.TsChangeDataCaptureHeartbeat:
		receivedWatermark := t.LocalWaterMark
		nodeID := t.NodeID

		{
			s.waterMarkMutex.Lock()
			defer s.waterMarkMutex.Unlock()
			currentTime := timeutil.ToUnixMilli(timeutil.Now())
			if watermark, ok := s.watermarkCache[nodeID]; ok {
				if receivedWatermark != sqlutil.InvalidWaterMark && watermark.LocalWatermark < receivedWatermark {
					watermark.LocalWatermark = receivedWatermark
				}
				watermark.ReceivedTimestamp = currentTime
			} else {
				s.watermarkCache[nodeID] = &LocalWatermark{LocalWatermark: receivedWatermark, ReceivedTimestamp: currentTime}
			}
		}

	case *cdcpb.TsChangeDataCaptureValue:
		s.rowBuffer.Lock()
		defer s.rowBuffer.Unlock()
		for _, rowData := range t.Val {
			row := make([]sqlbase.EncDatum, len(s.spec.CDCColumns.CDCColumnIDs))
			buf := rowData
			for idx, colType := range s.spec.CDCColumns.CDCTypes {
				var err error
				row[idx], buf, err = sqlbase.EncDatumFromBuffer(&colType, sqlbase.DatumEncoding_VALUE, buf)
				if err != nil {
					return err
				}
			}

			if err := s.rowBuffer.AddRowWithoutLock(s.Ctx, row); err != nil {
				return err
			}
		}

		s.notifyNext()
	case *cdcpb.TsChangeDataCaptureStop:
		// sends STOP message to each node.
		req := &cdcpb.TsChangeDataCaptureStop{
			TableID:      t.TableID,
			InstanceID:   t.InstanceID,
			InstanceType: cdcpb.TSCDCInstanceType_Stream,
		}
		err = s.stopStream(req)

		// rewrite the error message since it's a successful STOP case
		err = errors.Errorf("stream %q is stopped successfully", s.spec.Metadata.Name)
	case *cdcpb.TsChangeDataCaptureError:
		log.VErrEventf(s.Ctx, 2, "TsChangeDataCaptureError: %s", t.Error.GoError())
		return t.Error.GoError()
	}

	return err
}

// stopStream sends the STOP message to all active nodes.
func (s *streamReaderProcessor) stopStream(req *cdcpb.TsChangeDataCaptureStop) error {
	ctx := context.Background()
	var finalErr error
	for _, id := range s.activeNodeList {
		nodeID := roachpb.NodeID(id)
		addr, err := s.FlowCtx.Cfg.Gossip.GetNodeIDAddress(nodeID)
		if err != nil {
			finalErr = err
			// the target node is shutdown, skip it
			continue
		}

		if addr == nil {
			continue
		}

		conn, err := s.FlowCtx.Cfg.RPCContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).
			Connect(ctx)
		if err != nil {
			// the target node is shutdown, skip it
			finalErr = err
			continue
		}

		if _, err = cdcpb.NewCDCCoordinatorClient(conn).StopTsCDC(ctx, req); err != nil {
			finalErr = err
		}
	}
	return finalErr
}

// checkpoint sorts the existing received rows and emits the synced rows (beyond OptSyncTime) periodically
// it also breaks the waiting loop in the Next method
func (s *streamReaderProcessor) checkpoint() error {
	var err error

	defer s.notifyNext()

	lowWaterMark, err := s.extractGlobalHighWaterMark()
	if err != nil {
		return err
	}

	hasRow, err := s.rowBuffer.HasOutputRow()
	if err != nil {
		return err
	}

	if lowWaterMark != sqlutil.InvalidWaterMark && lowWaterMark > s.lowWaterMark {
		// If the watermark is updated, the dispatch time will be synchronously updated to (lowWaterMark - SyncTime).
		// The time interval between them is used for sorting out-of-order data.
		s.lowWaterMark = lowWaterMark
		waterMark := timeutil.FromUnixMilli(s.lowWaterMark).UTC()
		emitTimestamp := waterMark.Add(-s.streamOpts.SyncTime)
		if s.emitTimestamp.Before(emitTimestamp) {
			s.emitTimestamp = emitTimestamp
		}
	} else if s.rowBuffer.HasOrderRow() && !hasRow {
		// If the watermark is not updated all the time and has new data,
		// the emitTimestamp will be updated according to the checkpoint interval.
		waterMark := timeutil.FromUnixMilli(s.lowWaterMark).UTC()
		emitTimestamp := s.emitTimestamp.Add(s.streamOpts.CheckpointInterval)
		if emitTimestamp.Before(waterMark) {
			s.emitTimestamp = emitTimestamp
		} else {
			s.emitTimestamp = waterMark
		}
	}

	if !hasRow {
		// If there is new data, send it to the sorting buffer for sorting,
		// and output the data that has been sorted and has reached the emitTimestamp.
		err = s.rowBuffer.emitReceivedRowsToOutputBuffer(s.expiredTime, s.emitTimestamp)
		if err != nil {
			return err
		}
	}

	// The expiredTime is the emitTimestamp of the previous checkpoint.
	if s.expiredTime.Before(s.emitTimestamp) {
		s.expiredTime = s.emitTimestamp
	}

	return nil
}

// ConsumerDone implements the interface execinfra.RowSource.
func (s *streamReaderProcessor) ConsumerDone() {
	s.MoveToDraining(nil /* err */)
}

// ConsumerClosed implements the interface execinfra.RowSource.
func (s *streamReaderProcessor) ConsumerClosed() {
	s.close()
}

// close used to close streamReaderProcessor.
func (s *streamReaderProcessor) close() {
	s.notifyNext()

	if s.InternalClose() {
		s.rowBuffer.Close()
	}
}

// extractGlobalHighWaterMark computes the global low-water mark.
//  1. Use math.MinInt64 as InvalidWaterMark, and it will be ignored when computing the global high-water mark.
//  2. Use the biggest local low-water mark as the global high-water mark.
//  3. If no data has been captured, the value of local low-water mark is InvalidWaterMark.
//  4. If data has been captured by a gateway node, the value of local low-water mark rises to a non-zero value.
//  5. The valid local low-water mark will be sent to streamReaderProcessor using Heartbeat.
//  6. If the heartbeat call is returned successfully, the local low-water mark will be reset to InvalidWaterMark.
//  7. The watermarkCache stores the local low-water marks received from each node,
//  8. The watermarkCache also records the receiving time of each local low-water mark.
//  9. Use HeartbeatInterval * MaxRetries as the heartbeatTimeout.
//     10.If it failed to receive a valid low-water mark more than heartbeatTimeout, the streamReaderProcessor will
//     return an error and the current stream job will be terminated.
func (s *streamReaderProcessor) extractGlobalHighWaterMark() (int64, error) {
	var globalWaterMark int64 = math.MinInt64
	{
		s.waterMarkMutex.Lock()
		defer s.waterMarkMutex.Unlock()
		currentTime := timeutil.ToUnixMilli(timeutil.Now())

		for nodeID, watermark := range s.watermarkCache {
			newWatermark := watermark.LocalWatermark

			if currentTime-watermark.ReceivedTimestamp > s.heartbeatTimeout {
				log.Errorf(
					s.Ctx, "the heartbeat of node '%d' is timeout, last receive time: %d, current time: %d",
					nodeID, watermark.ReceivedTimestamp, currentTime,
				)
				return sqlutil.InvalidWaterMark, errors.Errorf(`the heartbeat of node '%d' is timeout`, nodeID)
			}

			if newWatermark == sqlutil.InvalidWaterMark {
				continue
			}
			if newWatermark > globalWaterMark {
				globalWaterMark = newWatermark
				watermark.LocalWatermark = sqlutil.InvalidWaterMark
			}
		}
	}
	if globalWaterMark != math.MinInt64 {
		return globalWaterMark, nil
	}

	return sqlutil.InvalidWaterMark, nil
}

// notifyNext notifies to process the next row of data.
func (s *streamReaderProcessor) notifyNext() {
	if len(s.notifyCh) == 0 {
		s.notifyCh <- true
	}
}

// InitProcessorProcedure init processor in procedure
func (s *streamReaderProcessor) InitProcessorProcedure(txn *kv.Txn) {
	if s.EvalCtx.IsProcedure {
		if s.FlowCtx != nil {
			s.FlowCtx.Txn = txn
		}
		s.Closed = false
		s.State = execinfra.StateRunning
		s.Out.SetRowIdx(0)
	}
}

// needResendLastWindow returns true when resent data can be used to resolve the split window
// between historical and real-time data.
// Relationship tables, time windows with sliding, and time series tables where deduplication rules
// is not "override" or "merge" are not supported.
func (s *streamReaderProcessor) needResendLastWindow() bool {
	if !s.streamSink.HasAgg ||
		!s.streamOpts.ProcessHistory ||
		!s.streamParameters.TargetTable.IsTsTable ||
		s.recalculator.isSlideTimeWindow() {
		return false
	}

	deRule := tse.DeDuplicateRule.Get(&s.FlowCtx.Cfg.Settings.SV)
	if deRule != "merge" && deRule != "override" {
		return false
	}

	return true
}
