// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/apd"
)

const minOffset = 0
const maxOffset = 100
const probabilityOfNewNumber = 0.5

func testRangeMode(t *testing.T, count int) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	wfr := &WindowFrameRun{
		Rows:     makeIntSortedPartition(count),
		ArgsIdxs: []uint32{0},
	}
	wfr.PlusOp, wfr.MinusOp, _ = WindowFrameRangeOps{}.LookupImpl(types.Int, types.Int)
	testStartPreceding(t, evalCtx, wfr, types.Int)
	testStartFollowing(t, evalCtx, wfr, types.Int)
	testEndPreceding(t, evalCtx, wfr, types.Int)
	testEndFollowing(t, evalCtx, wfr, types.Int)

	wfr.Rows = makeFloatSortedPartition(count)
	wfr.PlusOp, wfr.MinusOp, _ = WindowFrameRangeOps{}.LookupImpl(types.Float, types.Float)
	testStartPreceding(t, evalCtx, wfr, types.Float)
	testStartFollowing(t, evalCtx, wfr, types.Float)
	testEndPreceding(t, evalCtx, wfr, types.Float)
	testEndFollowing(t, evalCtx, wfr, types.Float)

	wfr.Rows = makeDecimalSortedPartition(count)
	wfr.PlusOp, wfr.MinusOp, _ = WindowFrameRangeOps{}.LookupImpl(types.Decimal, types.Decimal)
	testStartPreceding(t, evalCtx, wfr, types.Decimal)
	testStartFollowing(t, evalCtx, wfr, types.Decimal)
	testEndPreceding(t, evalCtx, wfr, types.Decimal)
	testEndFollowing(t, evalCtx, wfr, types.Decimal)
}

func testStartPreceding(
	t *testing.T, evalCtx *EvalContext, wfr *WindowFrameRun, offsetType *types.T,
) {
	wfr.Frame = &WindowFrame{
		Mode:   RANGE,
		Bounds: WindowFrameBounds{StartBound: &WindowFrameBound{BoundType: OffsetPreceding}},
	}
	for offset := minOffset; offset < maxOffset; offset += rand.Intn(maxOffset / 10) {
		var typedOffset Datum
		switch offsetType.Family() {
		case types.IntFamily:
			typedOffset = NewDInt(DInt(offset))
		case types.FloatFamily:
			typedOffset = NewDFloat(DFloat(offset))
		case types.DecimalFamily:
			decimal := apd.Decimal{}
			decimal.SetInt64(int64(offset))
			typedOffset = &DDecimal{Decimal: decimal}
		default:
			panic("unsupported offset type")
		}
		wfr.StartBoundOffset = typedOffset
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			frameStartIdx, err := wfr.FrameStartIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			value, err := wfr.getValueByOffset(evalCtx.Ctx(), evalCtx, typedOffset, true /* negative */)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			for idx := 0; idx <= wfr.RowIdx; idx++ {
				valueAt, err := wfr.valueAt(evalCtx.Ctx(), idx)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value.Compare(evalCtx, valueAt) <= 0 {
					if idx != frameStartIdx {
						t.Errorf("FrameStartIdx returned wrong result on Preceding: expected %+v, found %+v", idx, frameStartIdx)
						t.Errorf("Search for %+v when wfr.RowIdx=%+v", value, wfr.RowIdx)
						t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
						panic("")
					}
					break
				}
			}
		}
	}
}

func testStartFollowing(
	t *testing.T, evalCtx *EvalContext, wfr *WindowFrameRun, offsetType *types.T,
) {
	wfr.Frame = &WindowFrame{
		Mode:   RANGE,
		Bounds: WindowFrameBounds{StartBound: &WindowFrameBound{BoundType: OffsetFollowing}, EndBound: &WindowFrameBound{BoundType: OffsetFollowing}},
	}
	for offset := minOffset; offset < maxOffset; offset += rand.Intn(maxOffset / 10) {
		var typedOffset Datum
		switch offsetType.Family() {
		case types.IntFamily:
			typedOffset = NewDInt(DInt(offset))
		case types.FloatFamily:
			typedOffset = NewDFloat(DFloat(offset))
		case types.DecimalFamily:
			decimal := apd.Decimal{}
			decimal.SetInt64(int64(offset))
			typedOffset = &DDecimal{Decimal: decimal}
		default:
			panic("unsupported offset type")
		}
		wfr.StartBoundOffset = typedOffset
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			frameStartIdx, err := wfr.FrameStartIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			value, err := wfr.getValueByOffset(evalCtx.Ctx(), evalCtx, typedOffset, false /* negative */)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			for idx := 0; idx <= wfr.PartitionSize(); idx++ {
				if idx == wfr.PartitionSize() {
					if idx != frameStartIdx {
						t.Errorf("FrameStartIdx returned wrong result on Following: expected %+v, found %+v", idx, frameStartIdx)
						t.Errorf("Search for %+v when wfr.RowIdx=%+v", value, wfr.RowIdx)
						t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
						panic("")
					}
					break
				}
				valueAt, err := wfr.valueAt(evalCtx.Ctx(), idx)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value.Compare(evalCtx, valueAt) <= 0 {
					if idx != frameStartIdx {
						t.Errorf("FrameStartIdx returned wrong result on Following: expected %+v, found %+v", idx, frameStartIdx)
						t.Errorf("Search for %+v when wfr.RowIdx=%+v", value, wfr.RowIdx)
						t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
						panic("")
					}
					break
				}
			}
		}
	}
}

func testEndPreceding(
	t *testing.T, evalCtx *EvalContext, wfr *WindowFrameRun, offsetType *types.T,
) {
	wfr.Frame = &WindowFrame{
		Mode:   RANGE,
		Bounds: WindowFrameBounds{StartBound: &WindowFrameBound{BoundType: OffsetPreceding}, EndBound: &WindowFrameBound{BoundType: OffsetPreceding}},
	}
	for offset := minOffset; offset < maxOffset; offset += rand.Intn(maxOffset / 10) {
		var typedOffset Datum
		switch offsetType.Family() {
		case types.IntFamily:
			typedOffset = NewDInt(DInt(offset))
		case types.FloatFamily:
			typedOffset = NewDFloat(DFloat(offset))
		case types.DecimalFamily:
			decimal := apd.Decimal{}
			decimal.SetInt64(int64(offset))
			typedOffset = &DDecimal{Decimal: decimal}
		default:
			panic("unsupported offset type")
		}
		wfr.EndBoundOffset = typedOffset
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			frameEndIdx, err := wfr.FrameEndIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			value, err := wfr.getValueByOffset(evalCtx.Ctx(), evalCtx, typedOffset, true /* negative */)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			for idx := wfr.PartitionSize() - 1; idx >= 0; idx-- {
				valueAt, err := wfr.valueAt(evalCtx.Ctx(), idx)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value.Compare(evalCtx, valueAt) >= 0 {
					if idx+1 != frameEndIdx {
						t.Errorf("FrameEndIdx returned wrong result on Preceding: expected %+v, found %+v", idx+1, frameEndIdx)
						t.Errorf("Search for %+v when wfr.RowIdx=%+v", value, wfr.RowIdx)
						t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
						panic("")
					}
					break
				}
			}
		}
	}
}

func testEndFollowing(
	t *testing.T, evalCtx *EvalContext, wfr *WindowFrameRun, offsetType *types.T,
) {
	wfr.Frame = &WindowFrame{
		Mode:   RANGE,
		Bounds: WindowFrameBounds{StartBound: &WindowFrameBound{BoundType: OffsetPreceding}, EndBound: &WindowFrameBound{BoundType: OffsetFollowing}},
	}
	for offset := minOffset; offset < maxOffset; offset += rand.Intn(maxOffset / 10) {
		var typedOffset Datum
		switch offsetType.Family() {
		case types.IntFamily:
			typedOffset = NewDInt(DInt(offset))
		case types.FloatFamily:
			typedOffset = NewDFloat(DFloat(offset))
		case types.DecimalFamily:
			decimal := apd.Decimal{}
			decimal.SetInt64(int64(offset))
			typedOffset = &DDecimal{Decimal: decimal}
		default:
			panic("unsupported offset type")
		}
		wfr.EndBoundOffset = typedOffset
		for wfr.RowIdx = 0; wfr.RowIdx < wfr.PartitionSize(); wfr.RowIdx++ {
			frameEndIdx, err := wfr.FrameEndIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			value, err := wfr.getValueByOffset(evalCtx.Ctx(), evalCtx, typedOffset, false /* negative */)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			for idx := wfr.PartitionSize() - 1; idx >= wfr.RowIdx; idx-- {
				valueAt, err := wfr.valueAt(evalCtx.Ctx(), idx)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value.Compare(evalCtx, valueAt) >= 0 {
					if idx+1 != frameEndIdx {
						t.Errorf("FrameEndIdx returned wrong result on Following: expected %+v, found %+v", idx+1, frameEndIdx)
						t.Errorf("Search for %+v when wfr.RowIdx=%+v", value, wfr.RowIdx)
						t.Errorf(partitionToString(evalCtx.Ctx(), wfr.Rows))
						panic("")
					}
					break
				}
			}
		}
	}
}

func makeIntSortedPartition(count int) indexedRows {
	partition := indexedRows{rows: make([]indexedRow, count)}
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	number := 0
	for idx := 0; idx < count; idx++ {
		if r.Float64() < probabilityOfNewNumber {
			number += r.Intn(10)
		}
		partition.rows[idx] = indexedRow{idx: idx, row: Datums{NewDInt(DInt(number))}}
	}
	return partition
}

func makeFloatSortedPartition(count int) indexedRows {
	partition := indexedRows{rows: make([]indexedRow, count)}
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	number := 0.0
	for idx := 0; idx < count; idx++ {
		if r.Float64() < probabilityOfNewNumber {
			number += r.Float64() * 10
		}
		partition.rows[idx] = indexedRow{idx: idx, row: Datums{NewDFloat(DFloat(number))}}
	}
	return partition
}

func makeDecimalSortedPartition(count int) indexedRows {
	partition := indexedRows{rows: make([]indexedRow, count)}
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	number := &DDecimal{}
	for idx := 0; idx < count; idx++ {
		tmp := apd.Decimal{}
		if r.Float64() < probabilityOfNewNumber {
			_, err := tmp.SetFloat64(r.Float64() * 10)
			if err != nil {
				panic(fmt.Sprintf("unexpected error: %v", err))
			}
			_, err = ExactCtx.Add(&number.Decimal, &number.Decimal, &tmp)
			if err != nil {
				panic(fmt.Sprintf("unexpected error: %v", err))
			}
		}
		value := &DDecimal{}
		_, err := tmp.SetFloat64(0)
		if err != nil {
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
		_, err = ExactCtx.Add(&value.Decimal, &number.Decimal, &tmp)
		if err != nil {
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
		partition.rows[idx] = indexedRow{idx: idx, row: Datums{value}}
	}
	return partition
}

func partitionToString(ctx context.Context, partition IndexedRows) string {
	var buffer bytes.Buffer
	var err error
	var row IndexedRow
	buffer.WriteString("\n")
	for idx := 0; idx < partition.Len(); idx++ {
		if row, err = partition.GetRow(ctx, idx); err != nil {
			return err.Error()
		}
		buffer.WriteString(fmt.Sprintf("%+v\n", row))
	}
	return buffer.String()
}

func TestRangeMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var counts = [...]int{1, 17, 42, 91}
	for _, count := range counts {
		testRangeMode(t, count)
	}
}

// indexedRows are rows with the corresponding indices.
type indexedRows struct {
	rows []indexedRow
}

// Len implements IndexedRows interface.
func (ir indexedRows) Len() int {
	return len(ir.rows)
}

// GetRow implements IndexedRows interface.
func (ir indexedRows) GetRow(_ context.Context, idx int) (IndexedRow, error) {
	return ir.rows[idx], nil
}

// indexedRow is a row with a corresponding index.
type indexedRow struct {
	idx int
	row Datums
}

// GetIdx implements IndexedRow interface.
func (ir indexedRow) GetIdx() int {
	return ir.idx
}

// GetDatum implements IndexedRow interface.
func (ir indexedRow) GetDatum(colIdx int) (Datum, error) {
	return ir.row[colIdx], nil
}

// GetDatums implements IndexedRow interface.
func (ir indexedRow) GetDatums(firstColIdx, lastColIdx int) (Datums, error) {
	return ir.row[firstColIdx:lastColIdx], nil
}

func (ir indexedRow) String() string {
	return fmt.Sprintf("%d: %s", ir.idx, ir.row.String())
}
