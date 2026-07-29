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

package colexec

import (
	"bytes"
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/colserde"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"
)

// TestSupportedSQLTypesIntegration tests that all SQL types supported by the
// vectorized engine are "actually supported." For each type, it creates a bunch
// of rows consisting of a single datum (possibly null), converts them into
// column batches, serializes and then deserializes these batches, and finally
// converts the deserialized batches back to rows which are compared with the
// original rows.
func TestSupportedSQLTypesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: diskMonitor,
		},
	}

	var da sqlbase.DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	for _, typ := range allSupportedSQLTypes {
		for _, numRows := range []int{
			// A few interesting sizes.
			1,
			coldata.BatchSize() - 1,
			coldata.BatchSize(),
			coldata.BatchSize() + 1,
		} {
			rows := make(sqlbase.EncDatumRows, numRows)
			for i := 0; i < numRows; i++ {
				rows[i] = make(sqlbase.EncDatumRow, 1)
				rows[i][0] = sqlbase.DatumToEncDatum(&typ, sqlbase.RandDatum(rng, &typ, true /* nullOk */))
			}
			typs := []types.T{typ}
			source := execinfra.NewRepeatableRowSource(typs, rows)

			columnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0 /* processorID */, source)
			require.NoError(t, err)

			coltyps, err := typeconv.FromColumnTypes(typs)
			require.NoError(t, err)
			c, err := colserde.NewArrowBatchConverter(coltyps)
			require.NoError(t, err)
			r, err := colserde.NewRecordBatchSerializer(coltyps)
			require.NoError(t, err)
			arrowOp := newArrowTestOperator(columnarizer, c, r)

			output := distsqlutils.NewRowBuffer(typs, nil /* rows */, distsqlutils.RowBufferArgs{})
			materializer, err := NewMaterializer(
				flowCtx,
				1, /* processorID */
				arrowOp,
				typs,
				&execinfrapb.PostProcessSpec{},
				output,
				nil, /* metadataSourcesQueue */
				nil, /* toClose */
				nil, /* outputStatsToTrace */
				nil, /* cancelFlow */
			)
			require.NoError(t, err)

			materializer.Start(ctx)
			materializer.Run(ctx)
			actualRows := output.GetRowsNoMeta(t)
			require.Equal(t, len(rows), len(actualRows))
			for rowIdx, expectedRow := range rows {
				require.Equal(t, len(expectedRow), len(actualRows[rowIdx]))
				cmp, err := expectedRow[0].Compare(&typ, &da, &evalCtx, &actualRows[rowIdx][0])
				require.NoError(t, err)
				require.Equal(t, 0, cmp)
			}
		}
	}
}

// arrowTestOperator is an Operator that takes in a coldata.Batch from its
// input, passes it through a chain of
// - converting to Arrow format
// - serializing
// - deserializing
// - converting from Arrow format
// and returns the resulting batch.
type arrowTestOperator struct {
	OneInputNode

	c *colserde.ArrowBatchConverter
	r *colserde.RecordBatchSerializer
}

var _ Operator = &arrowTestOperator{}

func newArrowTestOperator(
	input Operator, c *colserde.ArrowBatchConverter, r *colserde.RecordBatchSerializer,
) Operator {
	return &arrowTestOperator{
		OneInputNode: NewOneInputNode(input),
		c:            c,
		r:            r,
	}
}

func (a *arrowTestOperator) Init() {
	a.input.Init()
}

func (a *arrowTestOperator) Next(ctx context.Context) coldata.Batch {
	batchIn := a.input.Next(ctx)
	// Note that we don't need to handle zero-length batches in a special way.
	var buf bytes.Buffer
	arrowDataIn, err := a.c.BatchToArrow(batchIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	_, _, err = a.r.Serialize(&buf, arrowDataIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	var arrowDataOut []*array.Data
	if err := a.r.Deserialize(&arrowDataOut, buf.Bytes()); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	batchOut := testAllocator.NewMemBatchWithSize(nil, 0)
	if err := a.c.ArrowToBatch(arrowDataOut, batchOut); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return batchOut
}
