// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

type apDropTable struct {
	execinfra.ProcessorBase

	dbName        string
	dropStatement string

	notFirst         bool
	dropTableSuccess bool
	err              error
}

var _ execinfra.Processor = &apDropTable{}
var _ execinfra.RowSource = &apDropTable{}

const apDropTableProcName = "ap drop table"

func newDropAPTable(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	apt *execinfrapb.APDropTableProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*apDropTable, error) {
	tct := &apDropTable{dropStatement: apt.DropStatement, dbName: apt.DbName}
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
func (tct *apDropTable) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tct *apDropTable) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, apDropTableProcName)

	log.Infof(ctx, "%s, nodeID:%d ", tct.dropStatement, tct.FlowCtx.Cfg.NodeID.Get())
	if err := tct.FlowCtx.Cfg.GetAPEngine().ExecSqlInDB(tct.dbName, tct.dropStatement); err != nil {
		tct.dropTableSuccess = false
		tct.err = err
		return ctx
	}
	tct.dropTableSuccess = true
	return ctx
}

// Next is part of the RowSource interface.
func (tct *apDropTable) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	apDropMeta := &execinfrapb.RemoteProducerMetadata_APDropTable{
		DropSuccess: tct.dropTableSuccess,
	}
	if tct.err != nil {
		apDropMeta.DropErr = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{APDropTable: apDropMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *apDropTable) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
