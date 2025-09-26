// Copyright 2016 The Cockroach Authors.
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

package rowexec

import (
	"context"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tsDeleter struct {
	execinfra.ProcessorBase

	tsOperatorType execinfrapb.OperatorType

	tableID uint64
	hashNum uint64

	primaryTagKeys [][]byte
	primaryTags    [][]byte

	spans []execinfrapb.Span

	// Number of deleted rows
	deleteRow     uint64
	deleteSuccess bool
	notFirst      bool
	err           error
}

var _ execinfra.Processor = &tsDeleter{}
var _ execinfra.RowSource = &tsDeleter{}

const tsDeleteProcName = "ts insert"

func newTsDeleter(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tsDeleteSpec *execinfrapb.TsDeleteProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsDeleter, error) {
	td := &tsDeleter{
		tsOperatorType: tsDeleteSpec.TsOperator,
		tableID:        tsDeleteSpec.TableId,
		hashNum:        tsDeleteSpec.HashNum,
		primaryTagKeys: tsDeleteSpec.PrimaryTagKeys,
		primaryTags:    tsDeleteSpec.PrimaryTags,
	}
	td.spans = tsDeleteSpec.Spans

	if err := td.Init(
		td,
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
	return td, nil
}

// InitProcessorProcedure init processor in procedure
func (td *tsDeleter) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (td *tsDeleter) Start(ctx context.Context) context.Context {
	ctx = td.StartInternal(ctx, tsDeleteProcName)
	var err error
	deletedRow := uint64(0)

	ba := td.FlowCtx.Txn.NewBatch()
	OsnID := td.FlowCtx.Cfg.TsIDGen.GetNextID()
	switch td.tsOperatorType {
	case execinfrapb.OperatorType_TsDeleteData:
		hashPoints, err := api.GetHashPointByPrimaryTag(td.hashNum, td.primaryTags[0])
		if err != nil || len(hashPoints) == 0 {
			td.deleteSuccess = false
			td.err = err
			return ctx
		}
		startTs, endTs := getMinMaxTimestamp(td.spans)
		startKey := sqlbase.MakeTsRangeKey(sqlbase.ID(td.tableID), uint64(hashPoints[0]), startTs, td.hashNum)
		endKey := sqlbase.MakeTsRangeKey(sqlbase.ID(td.tableID), uint64(hashPoints[0]), endTs+1, td.hashNum)
		req := &roachpb.TsDeleteRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
			TableId:     td.tableID,
			PrimaryTags: td.primaryTags[0],
			OsnId:       OsnID,
		}
		req.TsSpans = make([]*roachpb.TsSpan, len(td.spans))
		for i := range td.spans {
			req.TsSpans[i] = &roachpb.TsSpan{TsStart: td.spans[i].StartTs, TsEnd: td.spans[i].EndTs}
		}
		ba.AddRawRequest(req)
		//fmt.Println("-----TsDeleteData-----")
		//fmt.Printf("startKey: %v, endKey: %v, TsSpan: %v\n", req.Key, req.EndKey, req.TsSpans)

		err = td.FlowCtx.Cfg.TseDB.Run(ctx, ba)
		if err == nil {
			for i := range ba.RawResponse().Responses {
				if v, ok := ba.RawResponse().Responses[i].Value.(*roachpb.ResponseUnion_TsDelete); ok {
					deletedRow += uint64(v.TsDelete.NumKeys)
				}
			}
		}
	case execinfrapb.OperatorType_TsDeleteMultiEntitiesData:
		startKey := sqlbase.MakeTsHashPointKey(sqlbase.ID(td.tableID), uint64(0), td.hashNum)
		endKey := sqlbase.MakeTsRangeKey(sqlbase.ID(td.tableID), td.hashNum, math.MaxInt64, td.hashNum)
		req := &roachpb.TsDeleteMultiEntitiesDataRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
			TableId: td.tableID,
			HashNum: td.hashNum,
			OsnId:   OsnID,
		}
		for _, span := range td.spans {
			req.TsSpans = append(req.TsSpans, &roachpb.TsSpan{TsStart: span.StartTs, TsEnd: span.EndTs})
		}
		//fmt.Println("-----DeleteMultiEntities-----")
		//fmt.Printf("startKey: %v, endKey: %v, TsSpan: %v\n", req.Key, req.EndKey, req.TsSpans)
		ba.AddRawRequest(req)

		err = td.FlowCtx.Cfg.TseDB.Run(ctx, ba)
		if err == nil {
			for i := range ba.RawResponse().Responses {
				if v, ok := ba.RawResponse().Responses[i].Value.(*roachpb.ResponseUnion_TsDeleteMultiEntitiesData); ok {
					deletedRow += uint64(v.TsDeleteMultiEntitiesData.NumKeys)
				}
			}
		}
	case execinfrapb.OperatorType_TsDeleteEntities:
		hashPoints, err := api.GetHashPointByPrimaryTag(td.hashNum, td.primaryTags[0])
		if err != nil || len(hashPoints) == 0 {
			td.deleteSuccess = false
			td.err = err
			return ctx
		}
		startKey := sqlbase.MakeTsHashPointKey(sqlbase.ID(td.tableID), uint64(hashPoints[0]), td.hashNum)
		endKey := sqlbase.MakeTsRangeKey(sqlbase.ID(td.tableID), uint64(hashPoints[0]), math.MaxInt64, td.hashNum)
		// delete data
		delDataReq := &roachpb.TsDeleteRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
			TableId:     td.tableID,
			PrimaryTags: td.primaryTags[0],
			TsSpans:     []*roachpb.TsSpan{{TsStart: math.MinInt64, TsEnd: math.MaxInt64}},
			OsnId:       OsnID,
		}
		//fmt.Println("-----DeleteEntities-----data")
		//fmt.Printf("startKey: %v, endKey: %v, TsSpan: %v\n", delDataReq.Key, delDataReq.EndKey, delDataReq.TsSpans)
		ba.AddRawRequest(delDataReq)
		err = td.FlowCtx.Cfg.TseDB.Run(ctx, ba)
		if err == nil {
			for i := range ba.RawResponse().Responses {
				if v, ok := ba.RawResponse().Responses[i].Value.(*roachpb.ResponseUnion_TsDelete); ok {
					deletedRow += uint64(v.TsDelete.NumKeys)
				}
			}
			ba2 := td.FlowCtx.Txn.NewBatch()
			// delete primary tag and ignore numKeys of Responses
			ba2.AddRawRequest(&roachpb.TsDeleteEntityRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    startKey,
					EndKey: endKey,
				},
				TableId:     td.tableID,
				PrimaryTags: td.primaryTags,
				OsnId:       OsnID,
			})
			err = td.FlowCtx.Cfg.TseDB.Run(ctx, ba2)
		}
	default:
		err = errors.Newf("the TsOperatorType is not supported, TsOperatorType:%s", td.tsOperatorType.String())
	}
	if err != nil {
		td.deleteSuccess = false
		td.err = err
		return ctx
	}
	td.deleteSuccess = true
	td.deleteRow = deletedRow
	return ctx
}

// Next is part of the RowSource interface.
func (td *tsDeleter) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if td.notFirst {
		return nil, nil
	}
	td.notFirst = true

	tsDeleteMeta := &execinfrapb.RemoteProducerMetadata_TSDelete{
		DeleteSuccess: td.deleteSuccess,
		DeleteRow:     td.deleteRow,
	}
	if td.err != nil {
		tsDeleteMeta.DeleteErr = td.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsDelete: tsDeleteMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (td *tsDeleter) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	td.InternalClose()
}

func getMinMaxTimestamp(tsSpans []execinfrapb.Span) (int64, int64) {
	var minTs int64 = math.MaxInt64
	var maxTs int64 = math.MinInt64
	for _, span := range tsSpans {
		if span.StartTs < minTs {
			minTs = span.StartTs
		}
		if span.EndTs > maxTs {
			maxTs = span.EndTs
		}
	}
	return minTs, maxTs
}
