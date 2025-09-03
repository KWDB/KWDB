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

type apCreateTable struct {
	execinfra.ProcessorBase

	dbName          string
	createStatement string

	notFirst           bool
	createTableSuccess bool
	err                error
}

var _ execinfra.Processor = &apCreateTable{}
var _ execinfra.RowSource = &apCreateTable{}

const apCreateTableProcName = "ap create table"

func newCreateAPTable(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	apt *execinfrapb.APCreateTableProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*apCreateTable, error) {
	tct := &apCreateTable{dbName: apt.DbName, createStatement: apt.CreateStatement}
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
func (tct *apCreateTable) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tct *apCreateTable) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, apCreateTableProcName)

	log.Infof(ctx, "%s, nodeID:%d ", tct.createStatement, tct.FlowCtx.Cfg.NodeID.Get())
	if err := tct.FlowCtx.Cfg.GetAPEngine().ExecSqlInDB(tct.dbName, tct.createStatement); err != nil {
		tct.createTableSuccess = false
		tct.err = err
		return ctx
	}
	tct.createTableSuccess = true
	return ctx
}

// Next is part of the RowSource interface.
func (tct *apCreateTable) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	apCreateMeta := &execinfrapb.RemoteProducerMetadata_APCreateTable{
		CreateSuccess: tct.createTableSuccess,
	}
	if tct.err != nil {
		apCreateMeta.CreateErr = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{APCreateTable: apCreateMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *apCreateTable) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
