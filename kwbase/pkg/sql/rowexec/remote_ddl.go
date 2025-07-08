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
)

type remoteDDL struct {
	execinfra.ProcessorBase
	ddlString string
	err       error
}

// InitProcessorProcedure init processor in procedure
func (r *remoteDDL) InitProcessorProcedure(txn *kv.Txn) {
	if r.EvalCtx.IsProcedure {
		if r.FlowCtx != nil {
			r.FlowCtx.Txn = txn
		}
		r.Closed = false
		r.State = execinfra.StateRunning
		r.Out.SetRowIdx(0)
	}
}

func (r *remoteDDL) Start(ctx context.Context) context.Context {
	_, err := r.FlowCtx.EvalCtx.InternalExecutor.QueryRow(ctx, "metadata-backfill", nil, r.ddlString)
	r.err = err
	return ctx
}

func (r *remoteDDL) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if r.State == execinfra.StateRunning {
		if r.err != nil {
			r.MoveToDraining(r.err)
			return nil, &execinfrapb.ProducerMetadata{Err: r.err}
		}
		r.MoveToDraining(nil)
	}
	return nil, r.DrainHelper()
}

func (r *remoteDDL) ConsumerClosed() {
	r.InternalClose()
}

func newRemoteCreateDDL(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.RemotePlanNodeSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*remoteDDL, error) {
	r := remoteDDL{}
	r.ddlString = spec.Query
	err := r.Init(
		&r,
		post,
		nil,
		flowCtx,
		processorID,
		output,
		nil,
		execinfra.ProcStateOpts{},
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}
