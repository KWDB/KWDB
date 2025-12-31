// Copyright 2019 The Cockroach Authors.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
)

// runProcessorTest instantiates a processor with the provided spec, runs it
// with the given inputs, and asserts that the outputted rows are as expected.
func runProcessorTest(
	t *testing.T,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	inputTypes []types.T,
	inputRows sqlbase.EncDatumRows,
	outputTypes []types.T,
	expected sqlbase.EncDatumRows,
	txn *kv.Txn,
) {
	in := distsqlutils.NewRowBuffer(inputTypes, inputRows, distsqlutils.RowBufferArgs{})
	out := &distsqlutils.RowBuffer{}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Txn:     txn,
	}

	p, err := NewProcessor(
		context.Background(), &flowCtx, 0 /* processorID */, &core, &post,
		[]execinfra.RowSource{in}, []execinfra.RowReceiver{out}, []execinfra.LocalProcessor{})
	if err != nil {
		t.Fatal(err)
	}

	switch pt := p.(type) {
	case *joinReader:
		// Reduce batch size to exercise batching logic.
		pt.SetBatchSize(2 /* batchSize */)
	case *indexJoiner:
		//	Reduce batch size to exercise batching logic.
		pt.SetBatchSize(2 /* batchSize */)
	}

	p.Run(context.Background())
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	var res sqlbase.EncDatumRows
	for {
		row := out.NextNoMeta(t).Copy()
		if row == nil {
			break
		}
		res = append(res, row)
	}

	if result := res.String(outputTypes); result != expected.String(outputTypes) {
		t.Errorf(
			"invalid results: %s, expected %s'", result, expected.String(outputTypes))
	}
}

type rowsAccessor interface {
	getRows() *rowcontainer.DiskBackedRowContainer
}

func (s *sorterBase) getRows() *rowcontainer.DiskBackedRowContainer {
	return s.rows.(*rowcontainer.DiskBackedRowContainer)
}
