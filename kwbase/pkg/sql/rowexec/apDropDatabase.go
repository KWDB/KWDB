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

type apDropDatabase struct {
	execinfra.ProcessorBase

	currentDB string
	dropDB    string
	rm        bool

	notFirst            bool
	dropDatabaseSuccess bool
	err                 error
}

var _ execinfra.Processor = &apDropDatabase{}
var _ execinfra.RowSource = &apDropDatabase{}

const apDropDatabaseProcName = "ap drop database"

func newDropAPDatabase(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	apt *execinfrapb.APDropDatabaseProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*apDropDatabase, error) {
	tct := &apDropDatabase{currentDB: apt.CurrentDB, dropDB: apt.DropDB, rm: apt.Rm}
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
func (tct *apDropDatabase) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tct *apDropDatabase) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, apDropDatabaseProcName)

	log.Infof(ctx, "drop ap database %s, nodeID:%d ", tct.dropDB, tct.FlowCtx.Cfg.NodeID.Get())

	if err := tct.FlowCtx.Cfg.GetAPEngine().DropDatabase(tct.currentDB, tct.dropDB, tct.rm); err != nil {
		tct.dropDatabaseSuccess = false
		tct.err = err
		return ctx
	}

	tct.dropDatabaseSuccess = true
	return ctx
}

// Next is part of the RowSource interface.
func (tct *apDropDatabase) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	apDropMeta := &execinfrapb.RemoteProducerMetadata_APDropDatabase{
		DropSuccess: tct.dropDatabaseSuccess,
	}
	if tct.err != nil {
		apDropMeta.DropErr = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{APDropDatabase: apDropMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *apDropDatabase) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
