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
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"github.com/pkg/errors"
)

type tsAlterTable struct {
	execinfra.ProcessorBase

	tsOperator       execinfrapb.OperatorType
	columnMeta       []byte
	oriColumnMeta    []byte
	tableID          uint64
	idxID            uint32
	tagColumns       []uint32
	txnID            []byte
	currentTSVersion uint32
	newTSVersion     uint32

	partitionInterval uint64
	compressInterval  []byte
	vacuumInterval    []byte

	notFirst             bool
	alterTsColumnSuccess bool
	err                  error
	nodeID               int32
}

var _ execinfra.Processor = &tsAlterTable{}
var _ execinfra.RowSource = &tsAlterTable{}

const tsAlterColumnProcName = "ts alter column"

func newTsAlterColumn(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	tst *execinfrapb.TsAlterProSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tsAlterTable, error) {
	tat := &tsAlterTable{
		tsOperator:        tst.TsOperator,
		tableID:           tst.TsTableID,
		columnMeta:        tst.Column,
		oriColumnMeta:     tst.OriginalCol,
		idxID:             tst.TagIndexID,
		tagColumns:        tst.TagIndexColumns,
		txnID:             tst.TxnID,
		currentTSVersion:  tst.CurrentTSVersion,
		newTSVersion:      tst.NextTSVersion,
		partitionInterval: tst.PartitionInterval,
		compressInterval:  tst.CompressInterval,
		vacuumInterval:    tst.VacuumInterval,
	}
	if err := tat.Init(
		tat,
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
	return tat, nil
}

// Start is part of the RowSource interface.
func (tct *tsAlterTable) Start(ctx context.Context) context.Context {
	ctx = tct.StartInternal(ctx, tsAlterColumnProcName)
	tct.nodeID = int32(tct.FlowCtx.NodeID)
	switch tct.tsOperator {
	case execinfrapb.OperatorType_TsCreateTagIndex:
		if err := tct.FlowCtx.Cfg.TsEngine.TransBegin(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		if err := tct.FlowCtx.Cfg.TsEngine.CreateNormalTagIndex(tct.tableID, uint64(tct.idxID), tct.currentTSVersion, tct.newTSVersion, tct.txnID, tct.tagColumns); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsDropTagIndex:
		if err := tct.FlowCtx.Cfg.TsEngine.TransBegin(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		if err := tct.FlowCtx.Cfg.TsEngine.DropNormalTagIndex(tct.tableID, uint64(tct.idxID), tct.currentTSVersion, tct.newTSVersion, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsAddColumn:
		if err := tct.FlowCtx.Cfg.TsEngine.TransBegin(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		if err := tct.FlowCtx.Cfg.TsEngine.AddTSColumn(tct.tableID, tct.currentTSVersion, tct.newTSVersion, tct.txnID, tct.columnMeta); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsDropColumn:
		if err := tct.FlowCtx.Cfg.TsEngine.TransBegin(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		if err := tct.FlowCtx.Cfg.TsEngine.DropTSColumn(tct.tableID, tct.currentTSVersion, tct.newTSVersion, tct.txnID, tct.columnMeta); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsAlterPartitionInterval:
		if err := tct.FlowCtx.Cfg.TsEngine.AlterPartitionInterval(tct.tableID, tct.partitionInterval); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsCommit:
		if err := tct.FlowCtx.Cfg.TsEngine.TransCommit(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsRollback:
		if err := tct.FlowCtx.Cfg.TsEngine.TransRollback(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsAlterType:
		if err := tct.FlowCtx.Cfg.TsEngine.TransBegin(tct.tableID, tct.txnID); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		if err := tct.FlowCtx.Cfg.TsEngine.AlterTSColumnType(tct.tableID, tct.currentTSVersion, tct.newTSVersion, tct.txnID, tct.columnMeta, tct.oriColumnMeta); err != nil {
			tct.alterTsColumnSuccess = false
			tct.err = err
			return ctx
		}
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsAlterCompressInterval:
		tse.SetCompressInterval(tct.compressInterval)
		tct.alterTsColumnSuccess = true
	case execinfrapb.OperatorType_TsAlterVacuumInterval:
		tse.SetVacuumInterval(tct.vacuumInterval)
		tct.alterTsColumnSuccess = true
	default:
		tct.alterTsColumnSuccess = false
		tct.err = errors.New("unsupported TsOperator")
		return ctx
	}
	return ctx
}

// Next is part of the RowSource interface.
func (tct *tsAlterTable) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The timing operator only calls Next once.
	if tct.notFirst {
		return nil, nil
	}
	tct.notFirst = true
	tsAlterMeta := &execinfrapb.RemoteProducerMetadata_TSAlterColumn{
		AlterSuccess: tct.alterTsColumnSuccess,
		NodeID:       tct.nodeID,
	}
	tsAlterMeta.NodeIDMapErr = make(map[int32]string, 0)
	if tct.err != nil {
		tsAlterMeta.AlterErr = tct.err.Error()
		tsAlterMeta.NodeIDMapErr[tct.nodeID] = tct.err.Error()
	}
	return nil, &execinfrapb.ProducerMetadata{TsAlterColumn: tsAlterMeta}
}

// ConsumerClosed is part of the RowSource interface.
func (tct *tsAlterTable) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tct.InternalClose()
}
