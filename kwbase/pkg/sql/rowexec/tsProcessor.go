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

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type tsProcessor struct {
	execinfra.ProcessorBase

	tsOperatorType execinfrapb.OperatorType
	dropMEInfo     []sqlbase.DeleteMeMsg

	notFirst bool
	success  bool
	err      error
}

var _ execinfra.Processor = &tsProcessor{}
var _ execinfra.RowSource = &tsProcessor{}

const tsProcName = "ts processor"

func newTsProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tdt *execinfrapb.TsProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsProcessor, error) {
	tct := &tsProcessor{
		tsOperatorType: tdt.TsOperator,
		dropMEInfo:     tdt.DropMEInfo,
	}
	if err := tct.Init(
		tct,
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
	return tct, nil
}

// Start is part of the RowSource interface.
func (tp *tsProcessor) Start(ctx context.Context) context.Context {
	ctx = tp.StartInternal(ctx, tsProcName)

	var err error
	var errPrefix string
	switch tp.tsOperatorType {
	case execinfrapb.OperatorType_TsDropDeleteEntities:
		errPrefix = "drop timeseries table failed, reason:%s"
		// TODO:drop instance table
		for _, toDrop := range tp.dropMEInfo {
			primaryTag := []byte(toDrop.TableName)

			if err != nil {
				break
			}
			if err != nil {
				break
			}
			_, err = tp.FlowCtx.Cfg.TsEngine.DeleteEntities(uint64(toDrop.TemplateID), 1, [][]byte{primaryTag}, true, 0)
			if err != nil {
				break
			}
		}
	case execinfrapb.OperatorType_TsDropTsTable:
		errPrefix = "drop timeseries table failed, reason:%s"
		for _, toDrop := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.DropTsTable(uint64(toDrop.TableID))
			if err != nil {
				break
			}
		}
	case execinfrapb.OperatorType_TsDeleteExpiredData:
		errPrefix = "Delete Expired Data Failed, reason:%s"
		for _, toDrop := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.DeleteExpiredData(uint64(toDrop.TableID), toDrop.StartTs, toDrop.EndTs)
			if err != nil {
				log.Errorf(context.Background(), "Delete Expired Data Failed, tableID:%d, reason:%s \n", toDrop.TableID, err.Error())
			}
		}
	case execinfrapb.OperatorType_TsCompressTsTable:
		errPrefix = "Compress TS Table Failed, reason:%s"
		for _, toDrop := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.CompressTsTable(uint64(toDrop.TableID), toDrop.CompressTs)
			if err != nil {
				log.Errorf(context.Background(), "Compress TS Table Failed, tableID:%d, reason:%s \n", toDrop.TableID, err.Error())
			}
		}
	case execinfrapb.OperatorType_TsManualCompressTable:
		errPrefix = "Compress data manually failed, reason:%s"
		for _, toDrop := range tp.dropMEInfo {
			if toDrop.IsTSTable {
				err = tp.FlowCtx.Cfg.TsEngine.CompressImmediately(ctx, uint64(toDrop.TableID))
				if err != nil {
					log.Errorf(ctx, "Compress Table Failed, tableID:%d, reason:%s", toDrop.TableID, err.Error())
				}
			}
		}

	case execinfrapb.OperatorType_TsAutonomy:
		errPrefix = "Autonomy TS Table Failed, reason:%s"
		for _, table := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.TsTableAutonomy(uint64(table.TableID))
			if err != nil {
				log.Errorf(context.Background(), "Autonomy TS Table Failed, tableID:%d, reason:%s \n", table.TableID, err.Error())
			}
		}
	case execinfrapb.OperatorType_TsVacuum:
		errPrefix = "Vacuum TS Table Failed, reason:%s"
		for _, table := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.VacuumTsTable(uint64(table.TableID), table.TsVersion)
			if err != nil {
				log.Errorf(context.Background(), "Vacuum TS Table Failed, tableID:%d, reason:%s \n", table.TableID, err.Error())
			}
		}
	case execinfrapb.OperatorType_TsCount:
		errPrefix = "Count TS Table Failed, reason:%s"
		for _, table := range tp.dropMEInfo {
			err = tp.FlowCtx.Cfg.TsEngine.CountTsTable(uint64(table.TableID))
			if err != nil {
				log.Errorf(context.Background(), "Count TS Table Failed, tableID:%d, reason:%s \n", table.TableID, err.Error())
			}
		}
	default:
		err = errors.Newf("the TsOperatorType is not supported, TsOperatorType:%s", tp.tsOperatorType.String())
	}

	if err != nil {
		tp.success = false
		tp.err = errors.Newf(errPrefix, err)
		return ctx
	}

	tp.success = true
	return ctx
}

// Next is part of the RowSource interface.
func (tp *tsProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tp.notFirst {
		return nil, nil
	}
	tp.notFirst = true
	tsDropTableMeta := &execinfrapb.RemoteProducerMetadata_TSPro{
		Success: tp.success,
	}
	if tp.err != nil {
		tsDropTableMeta.Err = tp.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsPro: tsDropTableMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tp *tsProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tp.InternalClose()
}
