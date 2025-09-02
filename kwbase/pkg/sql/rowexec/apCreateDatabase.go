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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

type apCreateDatabase struct {
	execinfra.ProcessorBase

	db sqlbase.DatabaseDescriptor

	notFirst      bool
	createSuccess bool
	err           error
}

var _ execinfra.Processor = &apCreateDatabase{}
var _ execinfra.RowSource = &apCreateDatabase{}

const apCreateDatabaseProcName = "ap create database"

func newCreateAPDatabase(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	apd *execinfrapb.APCreateDatabaseProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*apCreateDatabase, error) {
	tct := &apCreateDatabase{db: apd.Database}
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
func (tct *apCreateDatabase) InitProcessorProcedure(txn *kv.Txn) {}

// Start is part of the RowSource interface.
func (tct *apCreateDatabase) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, apCreateDatabaseProcName)

	log.Infof(ctx, "create ap database:%s, nodeID:%d ", tct.db.Name, tct.FlowCtx.Cfg.NodeID.Get())
	if tct.db.ApDatabaseType == tree.ApDatabaseTypeMysql {
		attachStmt := fmt.Sprintf("ATTACH '%s' AS %s (TYPE mysql_scanner)", tct.db.AttachInfo, tct.db.Name)
		if err := tct.FlowCtx.Cfg.GetAPEngine().ExecSqlInDB(
			tct.FlowCtx.EvalCtx.SessionData.Database, attachStmt); err != nil {
			tct.createSuccess = false
			tct.err = err
			return ctx
		}
	} else {
		if err := tct.FlowCtx.Cfg.GetAPEngine().CreateDB(tct.db.Name); err != nil {
			tct.createSuccess = false
			tct.err = err
			return ctx
		}
	}
	tct.createSuccess = true
	return ctx
}

// Next is part of the RowSource interface.
func (tct *apCreateDatabase) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	apCreateMeta := &execinfrapb.RemoteProducerMetadata_APCreateDatabase{
		CreateSuccess: tct.createSuccess,
	}
	if tct.err != nil {
		apCreateMeta.CreateErr = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{APCreateDatabase: apCreateMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *apCreateDatabase) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
