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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

type tsTagUpdater struct {
	execinfra.ProcessorBase
	tsOperatorType execinfrapb.OperatorType
	tableID        uint64
	rangeGroupID   uint64
	primaryTagKeys [][]byte
	primaryTags    [][]byte
	tags           [][]byte
	osnID          uint64

	// Number of updated rows.
	updatedRow      uint64
	isUpdateSuccess bool
	notFirst        bool
	err             error

	startKey []byte
	endKey   []byte
}

var _ execinfra.Processor = &tsTagUpdater{}

var _ execinfra.RowSource = &tsTagUpdater{}

const tsTagUpdateProcName = "ts tag update"

func newTsTagUpdater(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tsTagUpdateSpec *execinfrapb.TsTagUpdateProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsTagUpdater, error) {
	tu := &tsTagUpdater{
		tsOperatorType: tsTagUpdateSpec.TsOperator,
		tableID:        tsTagUpdateSpec.TableId,
		rangeGroupID:   tsTagUpdateSpec.RangeGroupId,
		primaryTagKeys: tsTagUpdateSpec.PrimaryTagKeys,
		primaryTags:    tsTagUpdateSpec.PrimaryTags,
		tags:           tsTagUpdateSpec.Tags,
		startKey:       tsTagUpdateSpec.StartKey,
		endKey:         tsTagUpdateSpec.EndKey,
		osnID:          tsTagUpdateSpec.OsnId,
	}

	if err := tu.Init(
		tu,
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
	return tu, nil
}

// InitProcessorProcedure init processor in procedure
func (tu *tsTagUpdater) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tu *tsTagUpdater) Start(ctx context.Context) context.Context {
	ctx = tu.StartInternal(ctx, tsTagUpdateProcName)
	var err error
	updatedRow := uint64(0)

	ba := tu.FlowCtx.Txn.NewBatch()
	switch tu.tsOperatorType {
	case execinfrapb.OperatorType_TsUpdateTag:
		if tu.EvalCtx.StartDistributeMode {
			r := &roachpb.TsTagUpdateRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    tu.startKey,
					EndKey: tu.endKey,
				},
				TableId:      tu.tableID,
				PrimaryTags:  tu.primaryTags[0],
				RangeGroupId: tu.rangeGroupID,
				Tags:         tu.tags[0],
				OsnID:        tu.osnID,
			}
			ba.AddRawRequest(r)
		} else {
			r := &roachpb.TsTagUpdateRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: tu.primaryTagKeys[0],
				},
				TableId:      tu.tableID,
				PrimaryTags:  tu.primaryTags[0],
				RangeGroupId: tu.rangeGroupID,
				Tags:         tu.tags[0],
				OsnID:        tu.osnID,
			}

			ba.AddRawRequest(r)
		}
		err = tu.FlowCtx.Cfg.TseDB.Run(ctx, ba)
		if err == nil {
			if v, ok := ba.RawResponse().Responses[0].Value.(*roachpb.ResponseUnion_TsTagUpdate); ok {
				updatedRow = uint64(v.TsTagUpdate.NumKeys)
			}
		}
	default:
		err = pgerror.Newf(pgcode.FeatureNotSupported, "the TsOperatorType is not supported, TsOperatorType:%s", tu.tsOperatorType.String())
	}
	if err != nil {
		tu.isUpdateSuccess = false
		tu.err = err
		return ctx
	}
	tu.isUpdateSuccess = true
	// todo: need get updatedRow from batch
	tu.updatedRow = updatedRow
	return ctx
}

// Next is part of the RowSource interface.
func (tu *tsTagUpdater) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tu.notFirst {
		return nil, nil
	}
	tu.notFirst = true

	tsTagUpdateMeta := &execinfrapb.RemoteProducerMetadata_TSTagUpdate{
		UpdatedRow:      tu.updatedRow,
		IsUpdateSuccess: tu.isUpdateSuccess,
	}
	if tu.err != nil {
		tsTagUpdateMeta.UpdateErr = tu.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsTagUpdate: tsTagUpdateMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tu *tsTagUpdater) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tu.InternalClose()
}
