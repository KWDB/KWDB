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
	"strings"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	defaultBufferSize   = 1024
	defaultHeartbeat    = 2 * time.Second
	defaultCheckTimeout = defaultHeartbeat * 5
	// BindParametersLimitation is the max parameters in BIND message of postgresql protocol(uint16)
	BindParametersLimitation = 65535
)

// realTimeSender implements the interface of tree.ValueGenerator to fetch and generate table of realtime data from cdc.
// ________________________            _____________________________                _________________________________
// | [cdc task goroutine] |						 |[realtime sender goroutine] |               |[subscription client goroutine]|
// |   receive cdc-data --|--eventsCh--|-->encodes and pushes data--|-->streamCh----|--->| write data from streamCh |
// |											|            |                            |     data <----|----|  to data field.          |
// |                      |            |                            |       |-------|--->  fetches data and returns |
// |______________________|            |___________________________ |               |_______________________________|
//
// 1. cdc task goroutine captures realtime cdc data and pushes it into eventsCh.
// 2. realtime sender goroutine receives the cdc data event from eventsCh, encodes and sends the data to streamCh.
// 3. the subscription client fetches data from streamCh.
type realTimeSender struct {
	ctx                  context.Context
	metaData             *cdcpb.PubMetadata
	param                *cdcpb.PubParameters
	execCfg              *sql.ExecutorConfig
	data                 tree.Datums
	streamGroup          ctxgroup.Group
	errCh                chan error
	closeCh              chan struct{}
	streamCh             chan tree.Datums
	eventsCh             chan *cdcpb.TsChangeDataCaptureEvent
	init                 bool
	alloc                sqlbase.DatumAlloc
	evalCtx              *tree.EvalContext
	cancel               func()
	subHeartbeatInterval time.Duration
	cdcTimeoutInterval   time.Duration
	tableCache           map[uint64]string
	mutex                syncutil.Mutex
	watermarkCache       map[uint64]map[int32]*LocalWatermark
	// lowWaterMark saves low watermark of pipe
	lowWaterMark map[uint64]int64
	// mu used to cdcClients map
	mu syncutil.Mutex
	// cdcClients contains the CDC client of each node.
	cdcClients map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient

	// mu used to checkNodeChange
	connMu syncutil.Mutex
	// unregisterGossip used to unregister Gossip.
	unregisterGossip func()
}

var _ tree.ValueGenerator = (*realTimeSender)(nil)

// ResolvedType implements tree.ValueGenerator interface.
func (s *realTimeSender) ResolvedType() *types.T {
	return cdcpb.PubSubFuncType
}

// Start implements tree.ValueGenerator interface.
func (s *realTimeSender) Start(ctx context.Context, txn *kv.Txn) error {

	ctx, s.cancel = context.WithCancel(ctx)
	s.ctx = ctx
	// errCh consumed by ValueGenerator and is signaled when go routines encounter error.
	s.errCh = make(chan error, 2)
	s.closeCh = make(chan struct{}, 2)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums, defaultBufferSize)

	// Events channel gets RangeFeedEvents and is consumed by ValueGenerator.
	s.eventsCh = make(chan *cdcpb.TsChangeDataCaptureEvent, defaultBufferSize)

	s.watermarkCache = make(map[uint64]map[int32]*LocalWatermark)
	s.lowWaterMark = make(map[uint64]int64, len(s.param.TableList))
	if err := s.loadLowWaterMark(); err != nil {
		return err
	}

	s.streamGroup = ctxgroup.WithContext(ctx)
	s.cdcClients = make(map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient)

	if len(s.param.TableList) == 1 && s.param.TableList[0].Filter != "" {
		// extract and fill in the column ids, metrics and tag filter expressions.
		var err error
		s.metaData.MetricsFilter, s.metaData.TagFilter, err = sql.MarshalCDCFilter(
			ctx, txn, security.RootUser, s.execCfg, &s.param.TableList[0])
		if err != nil {
			return err
		}
	}

	s.startSender(ctx)
	s.registerCDC()

	log.Infof(ctx, "publication %s start sender done.", s.metaData.Name)
	return nil
}

func (s *realTimeSender) registerCDC() {
	if err := s.checkNodeChange(); err != nil {
		s.errCh <- err
		s.cancel()

		return
	}

	s.unregisterGossip = s.execCfg.Gossip.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyNodeIDPrefix),
		func(_ string, value roachpb.Value) {
			if err := s.checkNodeChange(); err != nil {
				log.Warningf(
					s.ctx, "the checkNodeChange of publication %s found error. %s", s.metaData.Name, err)
			}
		})
}

func (s *realTimeSender) createCDCRegister(
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

	addr, err := s.execCfg.Gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}

	conn, err := s.execCfg.RPCContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}

	req := &cdcpb.TsChangeDataCaptureRequest{
		PubMetadata:  s.metaData,
		InstanceType: sqlbase.CDCInstanceType_Publication,
		InstanceID:   s.metaData.ID,
	}
	if client, err = cdcpb.NewCDCCoordinatorClient(conn).TsCDC(ctx); err != nil {
		return err
	}
	event := &cdcpb.TsChangeDataCaptureEvent{
		Request: req,
	}
	s.mu.Lock()
	if err = client.Send(event); err != nil {
		s.mu.Unlock()
		return err
	}

	s.cdcClients[nodeID] = &client
	s.mu.Unlock()
	taskNum := len(s.param.TableList)

	for {
		// receives the heartbeat messages from CDC coordinator.
		data, err := client.Recv()
		if err != nil {
			return err
		}

		if !wgDone && taskNum == 0 {
			for _, table := range s.param.TableList {
				event = &cdcpb.TsChangeDataCaptureEvent{
					Start: &cdcpb.TsChangeDataCaptureStart{
						TableID:      table.ID,
						InstanceID:   s.metaData.ID,
						InstanceType: sqlbase.CDCInstanceType_Publication,
					},
				}
				if err = client.Send(event); err != nil {
					return err
				}
			}
			onDone()
			wgDone = true
		} else if !wgDone {
			taskNum--
		}

		select {
		case s.eventsCh <- data:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// startSender starts the realtime sender and begins to send realtime data from cdc.
func (s *realTimeSender) startSender(_ context.Context) {
	type ctxGroupFn = func(ctx context.Context) error
	// withErrCapture wraps fn to capture and report error to the error channel.
	withErrCapture := func(fn ctxGroupFn) ctxGroupFn {
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err != nil {
				log.Errorf(ctx, "Start publication failed. event stream %s terminating with error %v", s.metaData.Name, err)
				select {
				case s.errCh <- err:
				default:
				}
			}
			return err
		}
	}

	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		return s.senderLoop(ctx)
	}))
}

// senderLoop is the main process to handle events.
// event cdcpb.TsChangeDataCaptureValue: send the data to subscription;
// event cdcpb.TsChangeDataCaptureHeartbeat: handle and update heartbeats from other cdc;
// event cdcpb.TsChangeDataCaptureStop: do nothing;
// event cdcpb.TsChangeDataCaptureError: log error messages and raises an error.
// It will also handle the heartbeat with the subscription,
// and check whether some publications from other nodes has been timeout.
func (s *realTimeSender) senderLoop(ctx context.Context) error {
	var subHeartbeatTimer, checkTimeoutTimer timeutil.Timer
	defer subHeartbeatTimer.Stop()
	defer checkTimeoutTimer.Stop()
	subHeartbeatTimer.Reset(s.subHeartbeatInterval)
	checkTimeoutTimer.Reset(s.cdcTimeoutInterval)

	for {
		select {
		case event, ok := <-s.eventsCh:
			if !ok || event == nil {
				return nil
			}

			switch t := event.GetValue().(type) {
			case *cdcpb.TsChangeDataCaptureValue:
				if err := s.sendValue(ctx, t); err != nil {
					return err
				}
			case *cdcpb.TsChangeDataCaptureHeartbeat:
				receivedWatermark := t.LocalWaterMark
				nodeID := t.NodeID

				{
					s.mutex.Lock()
					currentTime := timeutil.ToUnixMilli(timeutil.Now())
					if watermark, ok := s.watermarkCache[t.TableID][nodeID]; ok {
						if receivedWatermark != cdcpb.InvalidWatermark && watermark.LocalWatermark < receivedWatermark {
							watermark.LocalWatermark = receivedWatermark
						}
						watermark.ReceivedTimestamp = currentTime
					} else {
						if s.watermarkCache[t.TableID] == nil {
							s.watermarkCache[t.TableID] = make(map[int32]*LocalWatermark)
						}
						s.watermarkCache[t.TableID][nodeID] = &LocalWatermark{LocalWatermark: receivedWatermark, ReceivedTimestamp: currentTime}
					}
					s.mutex.Unlock()
				}

			case *cdcpb.TsChangeDataCaptureStop:
				return errors.New("pub dropped")
			case *cdcpb.TsChangeDataCaptureError:
				err := t.Error.GoError()
				log.VErrEventf(ctx, 2, "real time cdc of publication %s captures error: %s", s.metaData.Name, err)
				return err
			default:
				return errors.AssertionFailedf("unexpected event in real time cdc of publication %s", s.metaData.Name)
			}
		case <-ctx.Done():
			log.Warningf(ctx, "the real time sender of publication %s is closed", s.metaData.Name)
			return ctx.Err()
		case <-s.closeCh:
			log.Warningf(ctx, "the real time sender of publication %s is closed", s.metaData.Name)
			return nil
		case <-subHeartbeatTimer.C:
			subHeartbeatTimer.Read = true
			if err := s.sendSubHeartbeat(ctx); err != nil {
				return err
			}
			subHeartbeatTimer.Reset(s.subHeartbeatInterval)
		case <-checkTimeoutTimer.C:
			checkTimeoutTimer.Read = true
			if err := s.checkNodeChange(); err != nil {
				log.Warningf(ctx, "the real time check-node-change of publication %s found error. %s", s.metaData.Name, err)
			}

			if err := s.persistLowWatermark(); err != nil {
				log.Warningf(ctx, "the real time persist low-watermark of publication %s found error. %s", s.metaData.Name, err)
			}

			if err := s.sendHeartbeatToCDC(); err != nil {
				return err
			}
			checkTimeoutTimer.Reset(s.cdcTimeoutInterval)
		}
	}
}

// sendHeartbeatToCDC sends the heartbeat message to all active nodes.
func (s *realTimeSender) sendHeartbeatToCDC() error {
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

// Values implements tree.ValueGenerator interface.
func (s *realTimeSender) Values() tree.Datums {
	return s.data
}

// Next implements tree.ValueGenerator interface.
func (s *realTimeSender) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		log.Infof(ctx, "publication %s publish real-time data finished. %s", s.metaData.Name, ctx.Err())
		s.Close()
		return false, ctx.Err()
	case err := <-s.errCh:
		log.Errorf(ctx, "publication %s publish real-time data failed. error: %s", s.metaData.Name, err)
		s.Close()
		return false, err
	case s.data = <-s.streamCh:
		return true, nil
	}
}

// Close implements tree.ValueGenerator interface.
func (s *realTimeSender) Close() {
	if s.unregisterGossip != nil {
		s.unregisterGossip()
	}

	s.cancel()
	s.mu.Lock()

	// close and clean cdcClients
	for _, client := range s.cdcClients {
		err := (*client).CloseSend()
		if err != nil {
			log.Errorf(context.TODO(), "grpc close failed with error %v", err)
		}
	}

	s.cdcClients = make(map[roachpb.NodeID]*cdcpb.CDCCoordinator_TsCDCClient)
	s.mu.Unlock()

	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(context.TODO(), "publication: partition stream %s terminated with error %v", s.metaData.Name, err)
	}
}

// sendSubHeartbeat sends heartbeat message to the corresponding channel of realtime sender.
func (s *realTimeSender) sendSubHeartbeat(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.streamCh <- tree.Datums{
		tree.NewDString(""),
		tree.NewDString(cdcpb.EventHeartbeat),
		tree.NewDBytes("heartbeat"),
		tree.NewDInt(1),
		tree.NewDInt(1),
		tree.NewDInt(tree.DInt(InvalidWatermark)),
		tree.NewDString(s.execCfg.ClusterID().String()),
	}:
		return nil
	}
}

// sendValue sends the data from cdc to the corresponding channel of realtime sender.
func (s *realTimeSender) sendValue(ctx context.Context, val *cdcpb.TsChangeDataCaptureValue) error {
	tableName := s.getTableName(val.TableID)
	if tableName == "" {
		log.Warningf(ctx, "The tableID %d of CDC Value is not in publication", val.TableID)
		return nil
	}

	clusterID := s.execCfg.ClusterID().String()
	val.ClusterID = &clusterID
	operation := cdcpb.EventInsert
	format := cdcpb.FormatPGBindBinary
	if val.Operation != nil {
		operation = *val.Operation
	}
	if val.Format != nil {
		format = cdcpb.FormatType(*val.Format)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.streamCh <- tree.Datums{
		tree.NewDString(tableName),
		tree.NewDString(operation),
		tree.NewDBytes(tree.DBytes(val.Val[0])),
		tree.NewDInt(tree.DInt(format)),
		tree.NewDInt(tree.DInt(*val.RowNumber)),
		tree.NewDInt(tree.DInt(val.Ts)),
		tree.NewDString(*val.ClusterID),
	}:
		return nil
	}
}

// getTableName gets the table name by table id.
func (s *realTimeSender) getTableName(tableID uint64) string {
	name, ok := s.tableCache[tableID]
	if ok {
		return name
	}

	return ""
}

// checkCDCTimeout checks whether the publication from other nodes is timeout.
func (s *realTimeSender) extractGlobalLowWaterMark() (map[uint64]int64, error) {
	globalWaterMark := make(map[uint64]int64)
	currentTime := timeutil.ToUnixMilli(timeutil.Now())
	{
		s.mutex.Lock()
		defer s.mutex.Unlock()

		for tableID, watermarkMap := range s.watermarkCache {
			globalWaterMark[tableID] = math.MaxInt64
			for nodeID, watermark := range watermarkMap {
				ts := watermark.LocalWatermark

				if currentTime-watermark.ReceivedTimestamp > s.cdcTimeoutInterval.Milliseconds() {
					// return globalWaterMark, errors.Errorf(`the heartbeat of node '%d' is timeout`, nodeID)
					log.Infof(s.ctx, `the heartbeat of node '%d' is timeout`, nodeID)
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

// checkNodeChange check node changed of cluster.
func (s *realTimeSender) checkNodeChange() error {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	nodeList, err := s.execCfg.CDCCoordinator.LiveNodeIDList(s.ctx)
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
			s.streamGroup.GoCtx(func(ctx context.Context) error {
				errConn := s.createCDCRegister(ctx, id, func() {
					wg.Done()
				})

				if errConn != nil {
					log.Errorf(s.ctx, "stream internal gRPC connection for node %d is disconnected with error: %s",
						nodeID, errConn)
					return nil
				}

				return nil
			})
		}
	}
	wg.Wait()

	return nil
}

// loadLowWaterMark loads LowWaterMark from system table.
func (s *realTimeSender) loadLowWaterMark() error {
	rows, err := s.execCfg.InternalExecutor.QueryEx(
		s.ctx,
		"load-watermark",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT table_id, low_watermark FROM system.kwdb_cdc_watermark WHERE task_id = $1`,
		s.metaData.ID,
	)

	if err != nil {
		return errors.Errorf(`failed to fetch publication %q.`, s.metaData.Name)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, row := range rows {
		tableID := uint64(tree.MustBeDInt(row[0]))
		s.lowWaterMark[tableID] = int64(tree.MustBeDInt(row[1]))
		s.watermarkCache[tableID] = make(map[int32]*LocalWatermark)
	}

	return nil
}

// persistLowWatermark saves LowWaterMark to system table.
func (s *realTimeSender) persistLowWatermark() error {
	lowWaterMark, err := s.extractGlobalLowWaterMark()
	if err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for tableID, watermark := range lowWaterMark {
		if watermark == InvalidWatermark || watermark <= s.lowWaterMark[tableID] {
			continue
		}

		if _, err = s.execCfg.InternalExecutor.ExecEx(
			s.ctx,
			"update-water-mark",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.kwdb_cdc_watermark SET low_watermark = $1 WHERE table_id = $2 AND task_id = $3 `,
			watermark,
			tableID,
			s.metaData.ID,
		); err != nil {
			return err
		}
		s.lowWaterMark[tableID] = watermark
	}

	return nil
}

// MakePubRealTimeSender is to make a real time data sender for publication
func MakePubRealTimeSender(
	evalCtx *tree.EvalContext, pubName string, param []byte,
) (tree.ValueGenerator, error) {
	execCfg := evalCtx.Planner.(sql.PlanHookState).ExecCfg()
	pubMeta, err := getPublicationByName(evalCtx, execCfg.InternalExecutor, nil, pubName)
	if err != nil {
		return nil, err
	}
	var params cdcpb.PubParameters
	if err = gojson.Unmarshal(pubMeta.Parameters, &params); err != nil {
		return nil, err
	}
	var subParams cdcpb.SubParameters
	if err = gojson.Unmarshal(param, &subParams); err != nil {
		return nil, err
	}

	if err = execCfg.CDCCoordinator.CheckPubTasksCountAndSubscribed(evalCtx.Ctx(), sqlbase.CDCInstanceType_Publication, pubMeta, params); err != nil {
		return nil, err
	}

	s := &realTimeSender{
		metaData:             pubMeta,
		param:                &params,
		execCfg:              execCfg,
		evalCtx:              evalCtx,
		subHeartbeatInterval: time.Duration(params.PubOptions.SubTimeout) * time.Second,
		cdcTimeoutInterval:   defaultCheckTimeout,
	}

	s.tableCache = make(map[uint64]string, len(params.TableList))
	for _, table := range params.TableList {
		s.tableCache[table.ID] = table.GetFullName()
	}

	return s, nil
}

// historySender implements the interface of tree.ValueGenerator to fetch and generate table of history data.
// ________________________            _____________________________                _________________________________
// |[fetch data goroutine]|						 | [history sender goroutine] |               |[subscription client goroutine]|
// |fetch-historical-data-|->streamCh--|-->encodes and pushes data--|-->streamCh----|--->| write data from streamCh |
// |											|     ^      |                            |     data <----|----|  to data field.          |
// |                     -|--|  |      |                            |       |-------|--->  fetches data and returns |
// |______________________|  |  |      |___________________________ |               |_______________________________|
// ________________________  |  |
// |   [wait goroutine]   |  |  |
// | wait the fetch data<-|--|  |
// | goroutine to return	|     |
// |   |-->send nil  -----|-----|
// |______________________|
type historySender struct {
	metaData    *cdcpb.PubMetadata
	execCfg     *sql.ExecutorConfig
	param       *cdcpb.PubParameters
	data        tree.Datums
	streamGroup ctxgroup.Group
	doneChan    chan struct{}
	errCh       chan error
	streamCh    chan tree.Datums
	eventsCh    chan [][]byte
	init        bool
	alloc       sqlbase.DatumAlloc
	evalCtx     *tree.EvalContext
	tableCache  map[uint64]string
	buffer      *pgBindBuffer
	publishMap  map[string]struct{}

	// lowWatermark is the first timestamp of historical records.
	lowWatermark int64
	// highWatermark is the last timestamp of historical records.
	highWatermark int64
	// batchCount is the batch count. The records are split by row width.
	batchCount int64
	// batchInterval is the duration in milliseconds in each batch. It is calculated using the duration(last timestamp
	// subtracts first timestamp ) in milliseconds from all historical data divides batchCount.
	batchInterval int64
}

// ResolvedType implements tree.ValueGenerator interface.
func (h *historySender) ResolvedType() *types.T {
	return cdcpb.PubSubFuncType
}

// Start implements tree.ValueGenerator interface.
//  1. It starts a new goroutine to fetch historical data, and send the data to streamCh in historySender.
//  2. It starts another goroutine to wait the first goroutine to finish, and send nil to streamCh in historySender.
//  3. Start returns immediately. The main goroutine begins the Next to fetch data from streamCh in historySender,
//     and sends the data to subscription client.
func (h *historySender) Start(ctx context.Context, _ *kv.Txn) error {
	// errCh consumed by ValueGenerator and is signaled when go routines encounter error.
	h.errCh = make(chan error)

	// Stream channel receives datums to be sent to the consumer.
	h.streamCh = make(chan tree.Datums, defaultBufferSize)
	if len(h.param.TableList) < 1 {
		return errors.Errorf("no table(s) in param")
	}

	// start a new goroutine to fetch historical data and send the data to streamCh in historySender.
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		var err error
		for _, tableInfo := range h.param.TableList {
			err = h.sendValue(ctx, tableInfo)
			if err != nil {
				log.Errorf(ctx, "%s failed to process historical records with error: %v", h.metaData.Name, err)
				break
			}
		}
		return err
	})
	// run an async task to wait historical data sender goroutine(sendValue) to return. When wait() return,
	// send nil to channel which indicates the end of historical data.
	if err := h.execCfg.RPCContext.Stopper.RunAsyncTask(ctx, "publication-processor-poller", func(ctx context.Context) {
		err := g.Wait()
		if err != nil {
			log.Errorf(ctx, "%s failed to process historical records with error: %v", h.metaData.Name, err)
			h.errCh <- err
		}
		// send history data of all tables finished, and send nil to subscription.
		h.streamCh <- nil
	}); err != nil {
		return err
	}

	// return immediately, and then to Next() and Values()
	return nil
}

// Next implements tree.ValueGenerator interface.
func (h *historySender) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		log.Infof(ctx, "publication %s publish history data finished.", h.metaData.Name)
		return false, ctx.Err()
	case err := <-h.errCh:
		log.Infof(ctx, "publication %s publish history data failed. err:%s", h.metaData.Name, err.Error())
		return false, err
	case h.data = <-h.streamCh:
		if h.data == nil {
			return false, nil
		}
		return true, nil
	}
}

// Values implements tree.ValueGenerator interface.
func (h *historySender) Values() tree.Datums {
	return h.data
}

// Close implements tree.ValueGenerator interface.
func (h *historySender) Close() {
}

func (h *historySender) sendValue(ctx context.Context, tableInfo cdcpb.CDCTableInfo) error {
	// 1. retrieve table descriptor about current table, because ColumnDescriptor, oid is needed for encode
	tableDesc, err := sqlbase.GetTableDescFromID(h.evalCtx.Context, h.evalCtx.Txn, sqlbase.ID(tableInfo.ID))
	if err != nil {
		return err
	}
	clusterID := h.execCfg.ClusterID().String()
	colCount := len(tableDesc.Columns)
	bindBatchRows := BindParametersLimitation / colCount
	log.Infof(ctx, "publish historical data. table %s.%s, column count: %d, bind-batch-rows:%d", tableInfo.Database, tableInfo.Table, colCount, bindBatchRows)
	var colTypes = make(map[string]types.T, colCount)
	var tagColMap = make(map[string]bool, colCount)
	tsColumnName := tableDesc.Columns[0].Name
	for _, rowDesc := range tableDesc.Columns {
		colTypes[rowDesc.Name] = rowDesc.Type
		if rowDesc.IsPrimaryTagCol() {
			tagColMap[rowDesc.Name] = true
		} else if rowDesc.IsOrdinaryTagCol() {
			tagColMap[rowDesc.Name] = false
		}
	}

	// 2. query data from source table.
	// All table names have been checked and are legal.
	tableFullName, _ := tableInfo.GetFullTableName(h.evalCtx)
	h.buffer.setName(h.metaData.Name + tableFullName)
	columnsStr := strings.Join(tableInfo.ColNames, ",")

	filter := tableInfo.Filter
	if len(filter) == 0 {
		filter = "1=1"
	}

	var query string
	isHistory := tableInfo.LowWatermark == InvalidWatermark
	if isHistory {
		if _, ok := h.publishMap[cdcpb.EventInsert]; !ok {
			return nil
		}
		if err = h.evaluateHistoricalData(&tableInfo, filter); err != nil {
			return err
		}
		queryFormat := "SELECT %s, %s FROM %s WHERE %s"
		filter += fmt.Sprintf(" AND %s >=$1", tsColumnName)
		if h.batchInterval > 0 {
			filter += fmt.Sprintf(" AND %s < $2", tsColumnName)
		}
		query = fmt.Sprintf(queryFormat, tsColumnName, columnsStr, tableFullName, filter)
		lengthInBytes := 0
		lowWatermark := h.lowWatermark
		startLowWatermark := h.lowWatermark
		sentTotalNum := 0
		var loopIdx int64
		for loopIdx = 0; loopIdx <= h.batchCount; loopIdx++ {
			var params []interface{}
			tsStart := timeutil.FromUnixMilli(startLowWatermark).UTC()
			tsEnd := timeutil.FromUnixMilli(startLowWatermark + h.batchInterval).UTC()

			params = append(params, tsStart)
			if h.batchInterval > 0 {
				params = append(params, tsEnd)
			}
			startLowWatermark = startLowWatermark + h.batchInterval
			rows, err := h.execCfg.InternalExecutor.QueryEx(
				ctx,
				"pub-history-data",
				nil,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				query,
				params...,
			)
			if err != nil {
				return err
			}
			rowCount := len(rows)
			if rowCount == 0 {
				continue
			}

			// 3. each column in each row is encoded(getDataBinary)
			var res = make([][]byte, 0)
			sentRowCount := 0
			for _, row := range rows {
				ts := timeutil.ToUnixNano((row[0].(*tree.DTimestampTZ)).Time)
				if lowWatermark < ts {
					lowWatermark = ts
				}

				var rowBytes []byte
				for colIdx, pubColName := range tableInfo.ColNames {
					colDataType := colTypes[pubColName]
					colData := getDataBinary(row[colIdx+1], colDataType)
					res = append(res, colData)
					rowBytes = append(rowBytes, colData...)
				}
				lengthInBytes += len(rowBytes)
				sentRowCount++
				if h.buffer.needFlush(lengthInBytes) || sentRowCount == bindBatchRows {
					// 4. encoded rows is appended in buffer(pushBatch)
					h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
					h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
					sentTotalNum += sentRowCount
					res = make([][]byte, 0)
					lengthInBytes = 0
					sentRowCount = 0
				}
			}

			if sentRowCount == 0 {
				continue
			}
			// 4. encoded rows is appended in buffer(pushBatch)
			h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
			// 5. push encoded data into h.streamCh
			h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
			sentTotalNum += sentRowCount
			res = make([][]byte, 0)
			lengthInBytes = 0
			sentRowCount = 0
		}
	} else {
		err = h.PublishPausedValue(ctx, tableInfo, tagColMap, filter, tableFullName, columnsStr, colTypes, tsColumnName, tableDesc.Columns[0].Type.Precision(), bindBatchRows, clusterID)
		return err
	}
	return nil
}

// PublishPausedValue selects data during the subscription is paused, and sends the data to corresponding subscription.
func (h *historySender) PublishPausedValue(
	ctx context.Context,
	tableInfo cdcpb.CDCTableInfo,
	tagColMap map[string]bool,
	filter string,
	tableFullName string,
	columnsStr string,
	colTypes map[string]types.T,
	tsColumnName string,
	tsPrecision int32,
	bindBatchRows int,
	clusterID string,
) error {
	err := h.evaluatePausedData(&tableInfo, filter)
	if err != nil {
		return err
	}
	queryFormat := "SELECT %s, %s, %s, %s, %s, %s FROM %s WHERE %s ORDER BY %s ASC"
	filter += fmt.Sprintf(" AND %s >=$1", opt.HiddenOSNColumnName)
	if h.batchInterval > 0 {
		filter += fmt.Sprintf(" AND %s < $2", opt.HiddenOSNColumnName)
	}
	query := fmt.Sprintf(queryFormat,
		opt.HiddenOSNColumnName,
		opt.HiddenOperationColumnName,
		opt.HiddenEventColumnName,
		tsColumnName,
		strings.Join(tableInfo.PrimaryTagCols, ","),
		columnsStr,
		tableFullName,
		filter,
		opt.HiddenOSNColumnName)

	primaryTagOffset := 4
	pubedColOffset := primaryTagOffset + len(tableInfo.PrimaryTagCols)

	lengthInBytes := 0
	lowWatermark := h.lowWatermark
	startLowWatermark := h.lowWatermark
	sentTotalNum := 0
	var loopIdx int64
	for loopIdx = 0; loopIdx <= h.batchCount; loopIdx++ {
		var params []interface{}
		params = append(params, startLowWatermark)
		if h.batchInterval > 0 {
			params = append(params, startLowWatermark+h.batchInterval)
		}
		startLowWatermark = startLowWatermark + h.batchInterval
		rows, err := h.execCfg.InternalExecutor.QueryEx(
			ctx,
			"pub-paused-data",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			query,
			params...,
		)
		if err != nil {
			return err
		}
		rowCount := len(rows)
		if rowCount == 0 {
			continue
		}

		// 3. each column in each row is encoded(getDataBinary)
		var res = make([][]byte, 0)
		sentRowCount := 0
		var strBuilder strings.Builder
		for _, row := range rows {
			osn := int64(*row[0].(*tree.DInt))
			if lowWatermark < osn {
				lowWatermark = osn
			}
			op := []byte(*row[1].(*tree.DBytes))[0]
			switch op {
			case cdcpb.OperationInsert:
				if _, ok := h.publishMap[cdcpb.EventInsert]; !ok {
					continue
				}
				break
			case cdcpb.OperationUpdateNormalTag:
				if _, ok := h.publishMap[cdcpb.EventUpdate]; !ok {
					continue
				}
				var updateCols []string
				var whereClauses []string
				// construct update expression
				for idx, colName := range tableInfo.ColNames {
					if isPrimaryTag, ok := tagColMap[colName]; ok && !isPrimaryTag {
						// normal tag
						valueOfNormalTag := row[idx+pubedColOffset].String()
						updateCols = append(updateCols, colName+"="+valueOfNormalTag)
					}
				}
				if len(updateCols) == 0 {
					// no need to pub update statement, because the updated normal tag is not in publication.
					continue
				}
				for pIdx, pTag := range tableInfo.PrimaryTagCols {
					valOfPTag := row[pIdx+primaryTagOffset].String()
					whereClauses = append(whereClauses, pTag+"="+valOfPTag)
				}
				updateColsStr := strings.Join(updateCols, ",")
				whereClausesStr := strings.Join(whereClauses, " AND ")
				stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableFullName, updateColsStr, whereClausesStr)
				if sentRowCount > 0 {
					h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
					h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
					sentTotalNum += sentRowCount
					res = make([][]byte, 0)
					lengthInBytes = 0
					sentRowCount = 0
				}
				h.sendStatement(tableFullName, cdcpb.EventUpdate, stmt, 1, lowWatermark, clusterID)
				continue
			case cdcpb.OperationDeleteTag:
				if _, ok := h.publishMap[cdcpb.EventDelete]; !ok {
					continue
				}
				strBuilder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE 1=1", tableFullName))
				for pIdx, pTag := range tableInfo.PrimaryTagCols {
					strBuilder.WriteString(fmt.Sprintf(" AND %s = %s", pTag, row[pIdx+primaryTagOffset].String()))
				}
				stmt := strBuilder.String()
				if sentRowCount > 0 {
					h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
					h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
					sentTotalNum += sentRowCount
					res = make([][]byte, 0)
					lengthInBytes = 0
					sentRowCount = 0
				}
				h.sendStatement(tableFullName, cdcpb.EventDelete, stmt, 1, lowWatermark, clusterID)
				strBuilder.Reset()
				continue
			case cdcpb.OperationDeleteMetric:
				if _, ok := h.publishMap[cdcpb.EventDelete]; !ok {
					continue
				}
				precisionDatum := tree.TimeFamilyPrecisionToRoundDuration(tsPrecision)
				strBuilder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE 1=1 ", tableFullName))
				spanBytes := []byte(tree.MustBeDBytes(row[2]))
				var startInt, endInt int64
				if len(spanBytes) < 16 {
					log.Warningf(ctx, "the length of event [%d], in row is less than 16", len(spanBytes))
					continue
				}
				startInt = int64(binary.LittleEndian.Uint64(spanBytes[0:8]))
				endInt = int64(binary.LittleEndian.Uint64(spanBytes[8:]))
				start := tree.MakeDTimestampTZ(timeutil.FromTimestamp(startInt, tsPrecision), precisionDatum)
				end := tree.MakeDTimestampTZ(timeutil.FromTimestamp(endInt, tsPrecision), precisionDatum)

				strBuilder.WriteString(fmt.Sprintf("AND %s >= %v AND %s <= %v",
					tsColumnName, start.String(), tsColumnName, end.String()))

				for pIdx, pTag := range tableInfo.PrimaryTagCols {
					strBuilder.WriteString(fmt.Sprintf(" AND %s = %s", pTag, row[pIdx+primaryTagOffset].String()))
				}
				stmt := strBuilder.String()
				if sentRowCount > 0 {
					h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
					h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
					sentTotalNum += sentRowCount
					res = make([][]byte, 0)
					lengthInBytes = 0
					sentRowCount = 0
				}
				h.sendStatement(tableFullName, cdcpb.EventDelete, stmt, 1, lowWatermark, clusterID)
				strBuilder.Reset()
				continue
			default:
				return fmt.Errorf("invalid operation: %s", string(op))
			}

			var rowBytes []byte
			for colIdx, pubColName := range tableInfo.ColNames {
				colDataType := colTypes[pubColName]
				colData := getDataBinary(row[colIdx+pubedColOffset], colDataType)
				res = append(res, colData)
				rowBytes = append(rowBytes, colData...)
			}
			lengthInBytes += len(rowBytes)
			sentRowCount++
			if h.buffer.needFlush(lengthInBytes) || sentRowCount == bindBatchRows {
				// 4. encoded rows is appended in buffer(pushBatch)
				h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
				h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
				sentTotalNum += sentRowCount
				res = make([][]byte, 0)
				lengthInBytes = 0
				sentRowCount = 0
			}
		}

		if sentRowCount == 0 {
			continue
		}
		// 4. encoded rows is appended in buffer(pushBatch)
		h.buffer.pushBatch(lowWatermark, res, lengthInBytes)
		// 5. push encoded data into h.streamCh
		h.sendBatchData(tableFullName, cdcpb.EventSnapshot, h.buffer.bytes(), cdcpb.FormatPGBindBinary, sentRowCount, lowWatermark, h.execCfg.ClusterID().String())
		sentTotalNum += sentRowCount
		res = make([][]byte, 0)
		lengthInBytes = 0
		sentRowCount = 0
	}
	return nil
}

// sendStatement sends statement to channel historySender.streamCh for returning to subscription.
// tableFullName is the full name of table, namely database_name.schema_name.table_name.
// op is the operation of this row, enumeration of update, delete.
// stmt is the statement to be sent to subscription.
// rowNumber is the number of row to be sent to subscription. Here is always 1.
// lowWatermark is the watermark of the data sent to subscription.
// clusterID is the cluster id of publication.
func (h *historySender) sendStatement(
	tableFullName string, op string, stmt string, rowNumber int, lowWatermark int64, clusterID string,
) {
	h.streamCh <- tree.Datums{
		tree.NewDString(tableFullName),
		tree.NewDString(op),
		tree.NewDBytes(tree.DBytes(stmt)),
		tree.NewDInt(tree.DInt(cdcpb.FormatSQL)),
		tree.NewDInt(tree.DInt(rowNumber)),
		tree.NewDInt(tree.DInt(lowWatermark)),
		tree.NewDString(clusterID),
	}
}

// sendBatchData sends data in the buffer of historySender and resets the buffer.
func (h *historySender) sendBatchData(
	tableFullName string,
	op string,
	data []byte,
	format cdcpb.FormatType,
	rowNumber int,
	lowWatermark int64,
	clusterID string,
) {
	h.streamCh <- tree.Datums{
		tree.NewDString(tableFullName),
		tree.NewDString(op),
		tree.NewDBytes(tree.DBytes(data)),
		tree.NewDInt(tree.DInt(format)),
		tree.NewDInt(tree.DInt(rowNumber)),
		tree.NewDInt(tree.DInt(lowWatermark)),
		tree.NewDString(clusterID),
	}
	h.buffer.reset()
}

// evaluateHistoricalData evaluates the amount of historical data,
// and if it exceeds the threshold, processes the historical data in batches.
// It stores the batch count, timestamp interval of each batch, low watermark and high watermark of historical data
// in historySender.
// detail is the table metadata used by publication.
// filter is the filter of publication.
// Returns error when executing queries failed. Otherwise, return nil.
func (h *historySender) evaluateHistoricalData(detail *cdcpb.CDCTableInfo, filter string) error {
	var err error
	queryFormat := "SELECT count(*),first(%s),last(%s) FROM %s.%s WHERE %s"
	query := fmt.Sprintf(
		queryFormat,
		detail.TsColumnName,
		detail.TsColumnName,
		detail.Database,
		detail.Table,
		filter,
	)

	row, err := h.execCfg.InternalExecutor.QueryRow(
		h.evalCtx.Context,
		"count-history",
		nil,
		query,
	)

	if err != nil {
		return err
	}

	totalRows := int64(tree.MustBeDInt(row[0]))
	if totalRows == 0 || row[1] == tree.DNull {
		return nil
	}

	firstTime, _ := tree.AsDTimestampTZ(row[1])
	firstTs := firstTime.UnixMilli()
	lastTime, _ := tree.AsDTimestampTZ(row[2])
	lastTs := lastTime.UnixMilli()
	if lastTs < firstTs {
		return nil
	}

	h.lowWatermark = firstTs
	h.highWatermark = lastTs
	// calculate rows in each batch by row-width
	batchRows := cdcpb.CalculateBatchRows(detail.ColTypes)
	// Divide the total rows by the historicalSnapshotMaxLimit to get the batch number,
	// and then divide the total time range by the batch number to get the time interval for each batch.
	if totalRows > batchRows {
		batchNum := totalRows / batchRows
		if batchNum == 0 {
			return nil
		}
		tsSpan := lastTs - firstTs
		if batchNum > tsSpan {
			batchNum = tsSpan
		}
		h.batchInterval = tsSpan / batchNum
		h.batchCount = batchNum
		log.Infof(h.evalCtx.Ctx(),
			"publication [%s] needs to split historical data in batches. table: %s.%s, total rows: %d, timestamp span: %s-%s, batch size: %d, batch interval: %dns",
			h.metaData.Name,
			detail.Database,
			detail.Table,
			totalRows,
			firstTime.String(),
			lastTime.String(),
			batchNum,
			h.batchInterval)
	}
	return nil
}

// evaluatePausedData evaluates the amount of data during the paused time period,
// and if it exceeds the threshold, processes the data in batches.
// It stores the batch count, timestamp interval of each batch, low watermark and high watermark of data
// in historySender.
// detail is the table metadata used by publication.
// filter is the filter of publication.
// Returns error when executing queries failed. Otherwise, return nil.
func (h *historySender) evaluatePausedData(detail *cdcpb.CDCTableInfo, filter string) error {
	var err error
	queryFormat := "SELECT count(*) FROM %s.%s WHERE %s >= $1 AND %s < $2 AND %s"
	query := fmt.Sprintf(
		queryFormat,
		detail.Database,
		detail.Table,
		opt.HiddenOSNColumnName,
		opt.HiddenOSNColumnName,
		filter,
	)

	lowWatermark := detail.LowWatermark
	start := timeutil.FromUnixNano(lowWatermark).Add(-500 * time.Millisecond).UTC().UnixNano()
	end := timeutil.Now().Add(500 * time.Millisecond).UTC().UnixNano()
	row, err := h.execCfg.InternalExecutor.QueryRow(
		h.evalCtx.Context,
		"count-paused",
		nil,
		query,
		start,
		end,
	)

	if err != nil {
		return err
	}

	totalRows := int64(tree.MustBeDInt(row[0]))
	if totalRows == 0 {
		return nil
	}

	h.lowWatermark = start
	h.highWatermark = end
	// calculate rows in each batch by row-width
	batchRows := cdcpb.CalculateBatchRows(detail.ColTypes)
	// Divide the total rows by the historicalSnapshotMaxLimit to get the batch number,
	// and then divide the total time range by the batch number to get the time interval for each batch.
	if totalRows > batchRows {
		batchNum := totalRows / batchRows
		if batchNum == 0 {
			return nil
		}
		tsSpan := h.highWatermark - h.lowWatermark
		if batchNum > tsSpan {
			batchNum = tsSpan
		}
		h.batchInterval = tsSpan / batchNum
		h.batchCount = batchNum
		log.Infof(h.evalCtx.Ctx(),
			"publication [%s] needs to split paused data in batches. table: %s.%s, total rows: %d, timestamp span: %s-%s, batch size: %d, batch interval: %dms",
			h.metaData.Name,
			detail.Database,
			detail.Table,
			totalRows,
			timeutil.FromUnixNano(start),
			timeutil.FromUnixNano(end),
			batchNum,
			h.batchInterval)
	}
	return nil
}

var _ tree.ValueGenerator = (*historySender)(nil)

// MakePubHistorySender is to make a history data sender for publication.
func MakePubHistorySender(
	evalCtx *tree.EvalContext, pubName string, param []byte,
) (tree.ValueGenerator, error) {
	planHook := evalCtx.Planner.(sql.PlanHookState)
	execCfg := planHook.ExecCfg()
	pubMeta, err := getPublicationByName(evalCtx, execCfg.InternalExecutor, nil, pubName)
	if err != nil {
		return nil, err
	}

	var params cdcpb.PubParameters
	if err = gojson.Unmarshal(pubMeta.Parameters, &params); err != nil {
		return nil, err
	}
	var subParams cdcpb.SubParameters
	if err = gojson.Unmarshal(param, &subParams); err != nil {
		return nil, err
	}
	pubTableCnt := len(params.TableList)
	subTableCnt := len(subParams.TableList)
	if pubTableCnt != subTableCnt {
		return nil, errors.Errorf("publication %s publish %d tables, but subscribe %d tables", pubName, pubTableCnt, subTableCnt)
	}
	if err = execCfg.CDCCoordinator.CheckPubTasksCountAndSubscribed(evalCtx.Ctx(), sqlbase.CDCInstanceType_Publication, pubMeta, params); err != nil {
		return nil, err
	}

	var subWatermark = make(map[string]int64, subTableCnt)
	for _, subTable := range subParams.TableList {
		subTableName, err := subTable.GetFullTableName(evalCtx)
		if err != nil {
			return nil, err
		}
		subWatermark[subTableName] = subTable.LowWatermark
	}
	// if upgrade KaiwuDB from version 3.0.x to 3.1.x, there is no PrimaryTagCols and NormalTagCols.
	// Then need to supplement PrimaryTagCols and NormalTagCols.
	for idx, puTable := range params.TableList {
		if len(puTable.PrimaryTagCols) > 0 {
			break
		}
		tbName := tree.MakeTableNameWithSchema(tree.Name(puTable.Database), tree.Name(puTable.Schema), tree.Name(puTable.Table))
		tbDesc, err := planHook.ResolveMutableTableDescriptor(evalCtx.Ctx(), &tbName, true, sql.ResolveRequireTSTableDesc)
		if err != nil {
			return nil, err
		}
		var primaryTags, normalTags []string
		for _, col := range tbDesc.Columns {
			if col.IsPrimaryTagCol() {
				primaryTags = append(primaryTags, col.Name)
				continue
			}
			if !col.IsPrimaryTagCol() && col.IsTagCol() {
				normalTags = append(normalTags, col.Name)
			}
		}
		params.TableList[idx].PrimaryTagCols = primaryTags
		params.TableList[idx].NormalTagCols = normalTags
	}
	// supplement PrimaryTagCols and NormalTagCols finished above
	flushLimit := int(float64(params.PubOptions.BufferSize*1<<20) * bufferFlushThreshold)
	hs := &historySender{
		metaData:   pubMeta,
		param:      &params,
		execCfg:    execCfg,
		evalCtx:    evalCtx,
		buffer:     newPGBindBuffer("", flushLimit),
		publishMap: map[string]struct{}{},
	}
	for _, op := range strings.Split(params.PubOptions.Publish, ",") {
		switch op {
		case cdcpb.EventAll:
			hs.publishMap[cdcpb.EventInsert] = struct{}{}
			hs.publishMap[cdcpb.EventUpdate] = struct{}{}
			hs.publishMap[cdcpb.EventDelete] = struct{}{}
			break
		default:
			hs.publishMap[op] = struct{}{}
		}
	}

	hs.tableCache = make(map[uint64]string, pubTableCnt)
	for tableIdx, table := range params.TableList {
		// the published table must have database,schema
		tableFullName, _ := table.GetFullTableName(evalCtx)
		hs.tableCache[table.ID] = tableFullName
		waterMark, ok := subWatermark[tableFullName]
		if !ok {
			return nil, errors.Errorf("published table %s in %s is not subscribed", tableFullName, pubName)
		}
		params.TableList[tableIdx].LowWatermark = waterMark
	}

	return hs, nil
}

// getPublicationByName gets and returns metadata of publication with publication name.
func getPublicationByName(
	evalCtx *tree.EvalContext, exec *sql.InternalExecutor, txn *kv.Txn, pubName string,
) (*cdcpb.PubMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id,name,parameters,create_at,create_by,database_id
FROM system.kwdb_publications WHERE name = '%s'`, pubName)
	pub, err := getPublicationMeta(evalCtx, exec, txn, stmt)
	if err != nil {
		return nil, err
	}

	if pub == nil {
		return nil, errors.Errorf("publication %s does not exist", pubName)
	}

	return pub, nil
}

// getPublicationMeta executes the specified stmt and returns the metadata of publication.
func getPublicationMeta(
	evalCtx *tree.EvalContext, exec *sql.InternalExecutor, txn *kv.Txn, stmt string,
) (*cdcpb.PubMetadata, error) {
	ctx := evalCtx.Ctx()
	row, err := exec.QueryRowEx(
		ctx,
		"load-publication",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return nil, err
	}

	var metadata cdcpb.PubMetadata
	// pub does not exist
	if len(row) == 0 {
		return nil, nil
	}

	metadata.ID = uint64(tree.MustBeDInt(row[0]))
	metadata.Name = string(tree.MustBeDString(row[1]))
	metadata.Parameters = []byte(tree.MustBeDJSON(row[2]).JSON.String())
	var params cdcpb.PubParameters
	if err = gojson.Unmarshal(metadata.Parameters, &params); err != nil {
		return nil, err
	}
	var tbDesc *sql.MutableTableDescriptor
	for _, table := range params.TableList {
		tbName := tree.MakeTableNameWithSchema(tree.Name(table.Database), tree.Name(table.Schema), tree.Name(table.Table))
		tbDesc, err = evalCtx.Planner.(sql.PlanHookState).ResolveMutableTableDescriptor(ctx, &tbName, true, sql.ResolveRequireTSTableDesc)
		if err != nil {
			return nil, err
		}
		err = evalCtx.Planner.(sql.PlanHookState).CheckPrivilege(ctx, tbDesc, privilege.SELECT)
		if err != nil {
			return nil, err
		}
	}
	return &metadata, nil
}

// publicationImpl implements the cdcpb.Publications interface.
type publicationImpl struct{}

// GetPublicationInfo implements the cdcpb.Publications interface.
func (r *publicationImpl) GetPublicationInfo(
	evalCtx *tree.EvalContext, pubName string,
) ([]byte, error) {
	execCfg := evalCtx.Planner.(sql.PlanHookState).ExecCfg()
	pubMeta, err := getPublicationByName(evalCtx, execCfg.InternalExecutor, nil, pubName)
	if err != nil {
		return nil, err
	}
	return pubMeta.Parameters, nil
}

// SubscriptRealtime implements the cdcpb.Publications interface.
func (r *publicationImpl) SubscriptRealtime(
	evalCtx *tree.EvalContext, pubName string, param []byte,
) (tree.ValueGenerator, error) {
	return MakePubRealTimeSender(evalCtx, pubName, param)
}

// SubscriptHistory implements the cdcpb.Publications interface.
func (r *publicationImpl) SubscriptHistory(
	evalCtx *tree.EvalContext, pubName string, param []byte,
) (tree.ValueGenerator, error) {
	return MakePubHistorySender(evalCtx, pubName, param)
}

// GetPublicationImpl is to construct and return the instance of publicationImpl.
func GetPublicationImpl() cdcpb.Publications {
	return &publicationImpl{}
}

func init() {
	cdcpb.GetPublicationsHook = GetPublicationImpl
}
