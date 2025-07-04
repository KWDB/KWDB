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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

type tsCreateTable struct {
	execinfra.ProcessorBase

	meta    []byte
	tableID uint64
	hashNum uint64

	notFirst             bool
	createTaTableSuccess bool
	err                  error
}

var _ execinfra.Processor = &tsCreateTable{}
var _ execinfra.RowSource = &tsCreateTable{}

const tsCreateTableProcName = "ts create table"

func newCreateTsTable(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tst *execinfrapb.TsCreateTableProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsCreateTable, error) {
	tct := &tsCreateTable{tableID: tst.TsTableID, hashNum: tst.HashNum, meta: tst.Meta}
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

// InitProcessorProcedure init processor in procedure
func (tct *tsCreateTable) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tct *tsCreateTable) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, tsCreateTableProcName)
	//hashRouter, err := api.GetHashRouterWithTable(0, uint32(tct.tableID), true, tct.EvalCtx.Txn)
	//if err != nil {
	//	return ctx
	//}

	//TODO rangeGroup暂时保留，但对存储而言应该不再使用，只创建一个RangeGroup，GroupID=1,角色为leader
	//rangeGroups := hashRouter.GetGroupIDAndRoleOnNode(ctx, tct.EvalCtx.NodeID)
	rangeGroup := api.RangeGroup{
		RangeGroupID: 1,
		Type:         api.ReplicaType_LeaseHolder,
	}
	rangeGroups := make([]api.RangeGroup, 0)
	rangeGroups = append(rangeGroups, rangeGroup)

	log.Infof(ctx, "tct.tableID:", tct.tableID, "rangeGroups:", rangeGroups, "nodeID", tct.FlowCtx.Cfg.NodeID.Get())
	if len(rangeGroups) != 0 {
		if err := tct.FlowCtx.Cfg.TsEngine.CreateTsTable(tct.tableID, tct.hashNum, tct.meta, rangeGroups); err != nil {
			tct.createTaTableSuccess = false
			tct.err = err
			return ctx
		}
	}

	tct.createTaTableSuccess = true
	return ctx
}

// Next is part of the RowSource interface.
func (tct *tsCreateTable) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	tsCreateMeta := &execinfrapb.RemoteProducerMetadata_TSCreateTable{
		CreateSuccess: tct.createTaTableSuccess,
	}
	if tct.err != nil {
		tsCreateMeta.CreateErr = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsCreate: tsCreateMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *tsCreateTable) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
