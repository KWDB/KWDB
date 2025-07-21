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

// This file contains ts insert and ts insert..into..select.
// ts insert:
//
// ts insert into select:
// eg:
//           tree         |    field    |  description
//-----------------------+-------------+-----------------
//                       | distributed | true
//                       | vectorized  | false
//  TSInsertSelect       |             |
//   │                   | into        | t3
//   └── synchronizer    |             |
//        └── ts scan    |             |
//                       | ts-table    | t1
//                       | access mode | tableTableMeta
//
// 1. execture plan of select to get data;
// 2. convert rows to exprs as input of tsInsert;
// 3. make new plan of tsInsert and run to insert data.

package rowexec

import (
	"context"
	"encoding/binary"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type tsInserter struct {
	execinfra.ProcessorBase

	nodeID        roachpb.NodeID
	rowNums       []uint32
	payload       [][]byte
	primaryTagKey [][]byte
	notFirst      bool
	insertSuccess bool
	err           error

	dedupRule int64
	// dedupRows is the number of rows not inserted due to the deduplication rule.
	dedupRows int64
	// insertRows is the actual number of successfully inserted rows.
	insertRows int64

	payloadPrefix            [][]byte
	payloadForDistributeMode []*execinfrapb.PayloadForDistributeMode
}

var _ execinfra.Processor = &tsInserter{}
var _ execinfra.RowSource = &tsInserter{}

const tsInsertProcName = "ts insert"

func newTsInserter(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tsInsertSpec *execinfrapb.TsInsertProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsInserter, error) {
	tsi := &tsInserter{
		nodeID:                   flowCtx.NodeID,
		rowNums:                  tsInsertSpec.RowNums,
		payload:                  tsInsertSpec.PayLoad,
		primaryTagKey:            tsInsertSpec.PrimaryTagKey,
		payloadPrefix:            tsInsertSpec.PayloadPrefix,
		payloadForDistributeMode: tsInsertSpec.AllPayload,
	}
	if err := tsi.Init(
		tsi,
		post,
		[]types.T{*types.Int},
		flowCtx,
		processorID,
		output,
		nil,
		execinfra.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        nil,
			TrailingMetaCallback: nil,
		},
	); err != nil {
		return nil, err
	}
	return tsi, nil
}

// InitProcessorProcedure init processor in procedure
func (tri *tsInserter) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tri *tsInserter) Start(ctx context.Context) context.Context {

	if !tri.EvalCtx.StartDistributeMode {
		return startForSingleMode(ctx, tri)
	}
	return startForDistributeMode(ctx, tri)
}

func startForSingleMode(ctx context.Context, tri *tsInserter) context.Context {
	ctx = tri.StartInternal(ctx, tsInsertProcName)
	ba := tri.FlowCtx.Txn.NewBatch()
	ba.SetIsTsInsert(true)

	insertRowSum := 0
	for i, pl := range tri.payload {
		ba.AddRawRequest(&roachpb.TsPutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: tri.primaryTagKey[i],
			},
			Value: roachpb.Value{RawBytes: pl},
		})
		insertRowSum += int(tri.rowNums[i])
	}
	if err := tri.FlowCtx.Cfg.TseDB.Run(ctx, ba); err != nil {
		tri.insertSuccess = false
		nodeIDErr := errors.Newf("nodeID %d insert failed, fail reason: %s;", tri.nodeID, err.Error())
		log.Errorf(context.Background(), nodeIDErr.Error())
		tri.err = nodeIDErr
		return ctx
	}

	var entitiesAffected uint32
	var unorderedAffected uint32
	for _, rawResponse := range ba.RawResponse().Responses {
		if v, ok := rawResponse.Value.(*roachpb.ResponseUnion_TsPut); ok {
			tri.dedupRule = v.TsPut.DedupRule
			tri.dedupRows += v.TsPut.NumKeys
			entitiesAffected += v.TsPut.EntitiesAffected
			unorderedAffected += v.TsPut.UnorderedAffected
		}
	}
	tri.insertRows = int64(insertRowSum) - tri.dedupRows

	tri.insertSuccess = true
	// Here we need to notify the statistics table of changes in the number of rows.
	if NeedNotifyTsMutation(&tri.EvalCtx.Settings.SV) {
		if tableID := ExtractTableIDFromPayload(tri.payload); tableID > 0 {
			tri.FlowCtx.Cfg.StatsRefresher.NotifyTsMutation(sqlbase.ID(tableID), int(tri.insertRows),
				int(entitiesAffected), int(unorderedAffected))
		}
	}
	return ctx
}

const (
	// BothTagAndData TsPayload contains datums consists of both tag and metric data
	BothTagAndData = 0
	// OnlyData TsPayload contains datums consists of metric data only
	OnlyData = 1
	// OnlyTag TsPayload contains datums consists of tag only data
	OnlyTag = 2
)

// rowTypeOffset refer to func fillHeader.
const rowTypeOffset = 42

func startForDistributeMode(ctx context.Context, tri *tsInserter) context.Context {
	ctx = tri.StartInternal(ctx, tsInsertProcName)
	ba := tri.FlowCtx.Txn.NewBatch()
	ba.SetIsTsInsert(true)

	insertRowSum := 0
	for i, pl := range tri.payloadForDistributeMode {
		if tri.payloadPrefix[i][rowTypeOffset] == OnlyTag {
			ba.AddRawRequest(&roachpb.TsPutTagRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    pl.StartKey,
					EndKey: pl.EndKey,
				},
				Value: roachpb.Value{
					RawBytes: tri.payloadPrefix[i],
				},
			})
		} else {
			ba.AddRawRequest(&roachpb.TsRowPutRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    pl.StartKey,
					EndKey: pl.EndKey,
				},
				HeaderPrefix: tri.payloadPrefix[i],
				Values:       pl.Row,
				Timestamps:   pl.TimeStamps,
				ValueSize:    pl.ValueSize,
				HashNum:      pl.HashNum,
			})
		}
		insertRowSum += int(tri.rowNums[i])
	}
	if err := tri.FlowCtx.Cfg.TseDB.Run(ctx, ba); err != nil {
		tri.insertSuccess = false
		nodeIDErr := errors.Newf("nodeID %d insert failed, fail reason: %s;", tri.nodeID, err.Error())
		log.Errorf(context.Background(), nodeIDErr.Error())
		tri.err = nodeIDErr
		return ctx
	}

	var entitiesAffected uint32
	var unorderedAffected uint32
	for _, rawResponse := range ba.RawResponse().Responses {
		if v, ok := rawResponse.Value.(*roachpb.ResponseUnion_TsRowPut); ok {
			tri.dedupRule = v.TsRowPut.DedupRule
			tri.insertRows += v.TsRowPut.NumKeys
			entitiesAffected += v.TsRowPut.EntitiesAffected
			unorderedAffected += v.TsRowPut.UnorderedAffected
		} else if v, ok := rawResponse.Value.(*roachpb.ResponseUnion_TsPutTag); ok {
			tri.dedupRule = v.TsPutTag.DedupRule
			tri.insertRows += v.TsPutTag.NumKeys
		} else if v, ok := rawResponse.Value.(*roachpb.ResponseUnion_TsPut); ok {
			entitiesAffected += v.TsPut.EntitiesAffected
			unorderedAffected += v.TsPut.UnorderedAffected
		}
	}
	tri.dedupRows = int64(insertRowSum) - tri.insertRows

	tri.insertSuccess = true
	// Here we need to notify the statistics table of changes in the number of rows.
	if NeedNotifyTsMutation(&tri.EvalCtx.Settings.SV) {
		if tableID := ExtractTableIDFromPayload(tri.payloadPrefix); tableID > 0 {
			tri.FlowCtx.Cfg.StatsRefresher.NotifyTsMutation(sqlbase.ID(tableID), int(tri.insertRows),
				int(entitiesAffected), int(unorderedAffected))
		}
	}
	return ctx
}

// Next is part of the RowSource interface.
func (tri *tsInserter) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tri.notFirst {
		return nil, nil
	}
	tri.notFirst = true

	var rowNums uint32
	for _, value := range tri.rowNums {
		rowNums += value
	}

	tsInsertMeta := &execinfrapb.RemoteProducerMetadata_TSInsert{
		NumRow:        rowNums,
		InsertSuccess: tri.insertSuccess,
		DedupRule:     tri.dedupRule,
		DedupRows:     tri.dedupRows,
		InsertRows:    tri.insertRows,
	}
	if tri.err != nil {
		tsInsertMeta.InsertErr = tri.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsInsert: tsInsertMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tri *tsInserter) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tri.InternalClose()
}

// NeedNotifyTsMutation is used to check automatic stats are enabled.
func NeedNotifyTsMutation(sv *settings.Values) bool {
	if opt.TSOrderedTable.Get(sv) || stats.AutomaticTagStatisticsClusterMode.Get(sv) ||
		stats.AutomaticTsStatisticsClusterMode.Get(sv) {
		// Automatic stats are enabled.
		return true
	}
	return false
}

// ExtractTableIDFromPayload extracts the tableID from the given payload.
// The payload structure should match that created by BuildPayloadForTsInsert.
func ExtractTableIDFromPayload(payload [][]byte) uint64 {
	if len(payload) == 0 {
		return 0
	}

	payloadPrefix := payload[0]
	// Check if the payload has enough bytes.
	if len(payloadPrefix) < execbuilder.TSVersionOffset+execbuilder.TSVersionSize {
		return 0
	}

	// Extract 8 bytes from payload starting from TableIDOffset and convert to uint64.
	tableIDOffset := execbuilder.TableIDOffset
	tableIDEnd := tableIDOffset + execbuilder.TableIDSize
	tableID := binary.LittleEndian.Uint64(payloadPrefix[tableIDOffset:tableIDEnd])
	return tableID
}

type tsInsertSelecter struct {
	execinfra.ProcessorBase
	input execinfra.RowSource
	// the map of col id to col index
	colsMap map[int]int
	// insert table id
	targetTableID uint64
	// insert table name
	cName string
	// insert table type
	tableType int32

	rowAffectNum  int
	insertSuccess bool
	err           error
}

var _ execinfra.Processor = &tsInsertSelecter{}
var _ execinfra.RowSource = &tsInsertSelecter{}

const tsInsertSelectProcName = "ts insert select"

/* newTsInsertSelecter
 * @Description： convert TsInsertSelSpec to tsInsertSelecter;
 * @In tsInsertSelSpec: Spec of TsInsertSelectNode;
 * @In post: post of tsInsertSelectNode;
 * @Return tsInsertSelecter
 */
func newTsInsertSelecter(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	tsInsertSelSpec *execinfrapb.TsInsertSelSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsInsertSelecter, error) {
	colsMap := make(map[int]int, 0)
	for i, v := range tsInsertSelSpec.Cols {
		colsMap[int(v)] = int(tsInsertSelSpec.ColIdxs[i])
	}
	tis := &tsInsertSelecter{
		input:         input,
		targetTableID: tsInsertSelSpec.TargetTableId,
		colsMap:       colsMap,
		cName:         tsInsertSelSpec.ChildName,
		tableType:     tsInsertSelSpec.TableType}

	inputsToDrain := []execinfra.RowSource{tis.input}
	if tsInsertSelSpec.NotSetInputsToDrain {
		// if SQL terminated abnormally, ConsumerClosed need to close this exector and sub exector.
		// if inputsToDrain is nil, ConsumerClosed close this exector only
		inputsToDrain = nil
	}

	if err := tis.Init(
		tis,
		post,
		[]types.T{*types.Int},
		flowCtx,
		processorID,
		output,
		nil,
		execinfra.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        inputsToDrain,
			TrailingMetaCallback: nil,
		},
	); err != nil {
		return nil, err
	}
	return tis, nil
}

/* rowToRowClause
 * @Description： convert datums to valuesClause In tsInsert.
 * @In rows: result of select;
 * @Return exprs: param of ts insert
 */
func rowToRowClause(rows sqlbase.EncDatumRow) []tree.Expr {
	exprs := make([]tree.Expr, len(rows))
	if rows == nil {
		return exprs
	}

	for i, row := range rows {
		expr, err := row.Datum.DatumToExpr()
		if err != nil {
			panic(err)
		}
		exprs[i] = expr
	}

	return exprs
}

// TSInsertSelectBlockMemLimit is memory of block written at a time in ts insert select, units: MB
var TSInsertSelectBlockMemLimit = settings.RegisterPublicIntSetting(
	"sql.ts_insert_select.block_memory", "memory of block written at a time in ts insert select", 200,
)

var insBytes int64 = 1024 * 1024

// the number of rows written at a time in ts insert select
var threadCountLimit = 20

// the number of thread in ts insert select
type insThreadCount struct {
	count int
	mu    syncutil.Mutex
}

var insThreadCnt insThreadCount

// add count of thread
func (i *insThreadCount) addThread() {
	for {
		if i.count >= threadCountLimit {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.count++
}

// delete count of thread
func (i *insThreadCount) doneThread() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.count--
}

// InitProcessorProcedure init processor in procedure
func (tis *tsInsertSelecter) InitProcessorProcedure(txn *kv.Txn) {}

// Start eg: insert into ts_table2 select * from ts_table;
// tis.input is plan of 'select * from ts_table'.
// 1. execture tis.input to get data, throw error if wrong in executre.
// 2. convert data to exprs as input of tsInsert.
// 3. BuildInputForTSInsert to make payLoads for tsInsert.
// 4. MakeNewPlanAndRunForTsInsert to make new plan of exec, run plan of tsInsert to insert data,
// record error of tsInsert, return error if wrong.
func (tis *tsInsertSelecter) Start(ctx context.Context) context.Context {
	insThreadCnt.addThread()
	defer insThreadCnt.doneThread()
	ctx = tis.StartInternal(ctx, tsInsertSelectProcName)
	// Get TableDescriptor by tableID.
	insTable, _, err := sqlbase.GetTsTableDescFromID(ctx, tis.EvalCtx.Txn, sqlbase.ID(tis.targetTableID))
	if err != nil {
		tis.insertSuccess = false
		tis.err = err
		return ctx
	}

	insertColumns := make([]*sqlbase.ColumnDescriptor, 0)
	var pTag int
	var dataColNum int
	var rowWidth uint64
	for i := range insTable.Columns {
		if _, ok := tis.colsMap[int(insTable.Columns[i].ID)]; ok {
			rowWidth += insTable.Columns[i].TsCol.StorageLen
		}
		insertColumns = append(insertColumns, &insTable.Columns[i])
		if !insTable.Columns[i].IsTagCol() {
			dataColNum++
		}
		if insTable.Columns[i].IsPrimaryTagCol() {
			pTag = int(insTable.Columns[i].ID)
		}
	}
	if tis.tableType == int32(tree.InstanceTable) || tis.tableType == int32(tree.TemplateTable) {
		rowWidth += uint64(len(tis.cName))
	}

	// memory of block usage for ts insert
	blockMemLimit := TSInsertSelectBlockMemLimit.Get(&tis.EvalCtx.Settings.SV) * insBytes
	// calculate the number of block
	blockRowNumLimit := blockMemLimit / int64(rowWidth)

	tis.input.Start(ctx)
	alloc := &sqlbase.DatumAlloc{}
	insertRows := make(opt.RowsValue, blockRowNumLimit)
	var rows sqlbase.EncDatumRow
	exprs := make([]tree.Expr, 0)
	success := false
	affectRows := 0
	rowCount := 0
	for {
		// get data
		var meta *execinfrapb.ProducerMetadata
		rows, meta = tis.input.Next()
		if meta != nil {
			switch tis.Out.Output().Push(rows, meta) {
			case execinfra.NeedMoreRows:
				continue
			}
		}
		if rows == nil {
			insertRows = insertRows[0:rowCount]
			if len(insertRows) > 0 {
				success, affectRows, err = tis.runTSInsert(ctx, insertRows, insTable, insertColumns, pTag, dataColNum)
				tis.insertSuccess = success
				if !success {
					tis.err = err
					return ctx
				}
				tis.rowAffectNum += affectRows
			}
			tis.insertSuccess = true
			break
		}

		// constuct rows by output type
		for i, typ := range tis.Out.Output().Types() {
			err = rows[i].EnsureDecoded(&typ, alloc)
			if err != nil {
				tis.insertSuccess = false
				tis.err = err
				return ctx
			}
		}
		exprs = rowToRowClause(rows)
		insertRows[rowCount] = exprs
		rowCount++
		if int64(rowCount) >= blockRowNumLimit {
			success, affectRows, err = tis.runTSInsert(ctx, insertRows, insTable, insertColumns, pTag, dataColNum)
			tis.insertSuccess = success
			if !success {
				tis.err = err
				return ctx
			}
			tis.rowAffectNum += affectRows
			rowCount = 0
		}
	}
	return ctx
}

/* runTSInsert
 * @Description：1. build payload; 2. make plan to execute ts insert;
 * @In insertRows: ts insert data;
 * @In insTable: insert table;
 * @In insertColumns: insert columns
 * @In pTag: primary tag column
 * @In dataColNum: number of data columns
 * @Return 1: whether success, true/false
 * @Return 2: rowAffectNum
 * @Return 3: error
 */
func (tis *tsInsertSelecter) runTSInsert(
	ctx context.Context,
	insertRows opt.RowsValue,
	insTable *sqlbase.TableDescriptor,
	insertColumns []*sqlbase.ColumnDescriptor,
	pTag, dataColNum int,
) (bool, int, error) {
	// If the num of insert cols are more than num of table cols.
	if len(tis.colsMap) > len(insTable.Columns) {
		return false, 0, pgerror.Newf(
			pgcode.Syntax,
			"INSERT (row 1) has more expressions than target columns, %d expressions for %d targets",
			len(insTable.Columns), len(insertRows[0]))
	}

	var rowAffectNum int
	if len(insertRows) > 0 {
		// If table is child table, need add pTag to insertRows.
		if tis.tableType == int32(tree.InstanceTable) || tis.tableType == int32(tree.TemplateTable) {
			childRowsValue := make([]tree.Exprs, len(insertRows))
			childNamePriTag := tree.NewStrVal(tis.cName)
			for i := range childRowsValue {
				childRowsValue[i] = make(tree.Exprs, dataColNum)
				copy(childRowsValue[i], insertRows[i][0:dataColNum])
				childRowsValue[i] = append(childRowsValue[i], childNamePriTag)
			}
			tis.colsMap[pTag] = len(childRowsValue[0]) - 1
			insertRows = childRowsValue
		}

		// Builds the payload of each node according to the input value.
		payloadNodeMap, err := execbuilder.BuildInputForTSInsert(
			tis.EvalCtx,
			insertRows,
			insertColumns,
			tis.colsMap,
			uint32(insTable.ParentID),
			uint32(insTable.ID),
			insTable.TableType == tree.InstanceTable,
			uint32(insTable.TsTable.TsVersion),
			insTable.TsTable.HashNum,
		)
		if err != nil {
			return false, 0, err
		}

		// update Plan of Node with tsInsertNode to execute tsInsert
		rowAffectNum, err = tis.EvalCtx.Planner.MakeNewPlanAndRunForTsInsert(ctx, tis.EvalCtx, &payloadNodeMap)
		if err != nil {
			return false, 0, err
		}
	}
	return true, rowAffectNum, nil
}

// Next is part of the RowSource interface.
func (tis *tsInsertSelecter) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	tsInsertMeta := &execinfrapb.RemoteProducerMetadata_TSInsert{
		NumRow:        uint32(tis.rowAffectNum),
		InsertSuccess: tis.insertSuccess,
	}
	if tis.err != nil {
		tsInsertMeta.InsertErr = tis.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsInsert: tsInsertMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tis *tsInsertSelecter) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tis.InternalClose()
}
