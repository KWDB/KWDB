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

package colexec

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	runTests(t, []tuples{{{1}, {2}, {nil}}}, tuples{{1, 2}, {2, 3}, {nil, nil}}, orderedVerifier,
		func(input []Operator) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, input[0], []types.T{*types.Int},
				"@1 + 1" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	runTests(t, []tuples{{{1, 2}, {3, 4}, {5, nil}}}, tuples{{1, 2, 3}, {3, 4, 7}, {5, nil, nil}},
		orderedVerifier,
		func(input []Operator) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, input[0], []types.T{*types.Int, *types.Int},
				"@1 + @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		})
}

func TestProjDivFloat64Float64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	runTests(t, []tuples{{{1.0, 2.0}, {3.0, 4.0}, {5.0, nil}}}, tuples{{1.0, 2.0, 0.5}, {3.0, 4.0, 0.75}, {5.0, nil, nil}},
		orderedVerifier,
		func(input []Operator) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, input[0], []types.T{*types.Float, *types.Float},
				"@1 / @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		})
}

func benchmarkProjPlusInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64})
	col := batch.ColVec(0).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		col[i] = 1
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := NewRepeatableBatchSource(testAllocator, batch)
	plusOp, err := createTestProjectingOperator(
		ctx, flowCtx, source, []types.T{*types.Int},
		"@1 + 1" /* projectingExpr */, false, /* canFallbackToRowexec */
	)
	require.NoError(b, err)
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkProjPlusInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkProjPlusInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func TestGetProjectionConstOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	binOp := tree.Mult
	var input Operator
	colIdx := 3
	constVal := 31.37
	constArg := tree.NewDFloat(tree.DFloat(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(
		testAllocator, types.Float, types.Float, coltypes.Float64,
		binOp, input, colIdx, constArg, outputIdx,
	)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultFloat64Float64ConstOp{
		projConstOpBase: projConstOpBase{
			OneInputNode: NewOneInputNode(op.(*projMultFloat64Float64ConstOp).input),
			allocator:    testAllocator,
			colIdx:       colIdx,
			outputIdx:    outputIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v,\nexpected %+v", op, expected)
	}
}

func TestGetProjectionConstMixedTypeOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	binOp := tree.GE
	var input Operator
	colIdx := 3
	constVal := int16(31)
	constArg := tree.NewDInt(tree.DInt(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(
		testAllocator, types.Int, types.Int2, coltypes.Int64,
		binOp, input, colIdx, constArg, outputIdx,
	)
	if err != nil {
		t.Error(err)
	}
	expected := &projGEInt64Int16ConstOp{
		projConstOpBase: projConstOpBase{
			OneInputNode: NewOneInputNode(op.(*projGEInt64Int16ConstOp).input),
			allocator:    testAllocator,
			colIdx:       colIdx,
			outputIdx:    outputIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v,\nexpected %+v", op, expected)
	}
}

// TestRandomComparisons runs binary comparisons against all scalar types
// (supported by the vectorized engine) with random non-null data verifying
// that the result of Datum.Compare matches the result of the exec projection.
func TestRandomComparisons(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	const numTuples = 2048
	rng, _ := randutil.NewPseudoRand()

	expected := make([]bool, numTuples)
	var da sqlbase.DatumAlloc
	lDatums := make([]tree.Datum, numTuples)
	rDatums := make([]tree.Datum, numTuples)
	for _, ct := range types.Scalar {
		if ct.Family() == types.DateFamily {
			// TODO(jordan): #40354 tracks failure to compare infinite dates.
			continue
		}
		typ := typeconv.FromColumnType(ct)
		if typ == coltypes.Unhandled {
			continue
		}
		typs := []coltypes.T{typ, typ, coltypes.Bool}
		bytesFixedLength := 0
		if ct.Family() == types.UuidFamily {
			bytesFixedLength = 16
		}
		b := testAllocator.NewMemBatchWithSize(typs, numTuples)
		lVec := b.ColVec(0)
		rVec := b.ColVec(1)
		ret := b.ColVec(2)
		coldata.RandomVec(rng, typ, bytesFixedLength, lVec, numTuples, 0)
		coldata.RandomVec(rng, typ, bytesFixedLength, rVec, numTuples, 0)
		for i := range lDatums {
			lDatums[i] = PhysicalTypeColElemToDatum(lVec, i, &da, ct)
			rDatums[i] = PhysicalTypeColElemToDatum(rVec, i, &da, ct)
		}
		for _, cmpOp := range []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE} {
			for i := range lDatums {
				cmp := lDatums[i].Compare(&evalCtx, rDatums[i])
				var b bool
				switch cmpOp {
				case tree.EQ:
					b = cmp == 0
				case tree.NE:
					b = cmp != 0
				case tree.LT:
					b = cmp < 0
				case tree.LE:
					b = cmp <= 0
				case tree.GT:
					b = cmp > 0
				case tree.GE:
					b = cmp >= 0
				}
				expected[i] = b
			}
			input := newChunkingBatchSource(typs, []coldata.Vec{lVec, rVec, ret}, numTuples)
			op, err := createTestProjectingOperator(
				ctx, flowCtx, input, []types.T{*ct, *ct},
				fmt.Sprintf("@1 %s @2", cmpOp), false, /* canFallbackToRowexec */
			)
			require.NoError(t, err)
			if err != nil {
				t.Fatal(err)
			}
			op.Init()
			var idx int
			for batch := op.Next(ctx); batch.Length() > 0; batch = op.Next(ctx) {
				for i := 0; i < batch.Length(); i++ {
					absIdx := idx + i
					assert.Equal(t, expected[absIdx], batch.ColVec(2).Bool()[i],
						"expected %s %s %s (%s[%d]) to be %t found %t", lDatums[absIdx], cmpOp, rDatums[absIdx], ct, absIdx,
						expected[absIdx], ret.Bool()[i])
				}
				idx += batch.Length()
			}
		}
	}
}

func TestGetProjectionOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ct := types.Int2
	binOp := tree.Mult
	var input Operator
	col1Idx := 5
	col2Idx := 7
	outputIdx := 9
	op, err := GetProjectionOperator(
		testAllocator, ct, ct, coltypes.Int16,
		binOp, input, col1Idx, col2Idx, outputIdx,
	)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultInt16Int16Op{
		projOpBase: projOpBase{
			OneInputNode: NewOneInputNode(op.(*projMultInt16Int16Op).input),
			allocator:    testAllocator,
			col1Idx:      col1Idx,
			col2Idx:      col2Idx,
			outputIdx:    outputIdx,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v,\nexpected %+v", op, expected)
	}
}

func benchmarkProjOp(
	b *testing.B,
	makeProjOp func(source *RepeatableBatchSource, intType coltypes.T) (Operator, error),
	useSelectionVector bool,
	hasNulls bool,
	intType coltypes.T,
) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{intType, intType})
	switch intType {
	case coltypes.Int64:
		col1 := batch.ColVec(0).Int64()
		col2 := batch.ColVec(1).Int64()
		for i := 0; i < coldata.BatchSize(); i++ {
			col1[i] = 1
			col2[i] = 1
		}
	case coltypes.Int32:
		col1 := batch.ColVec(0).Int32()
		col2 := batch.ColVec(1).Int32()
		for i := 0; i < coldata.BatchSize(); i++ {
			col1[i] = 1
			col2[i] = 1
		}
	default:
		b.Fatalf("unsupported type: %s", intType)
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(i)
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := NewRepeatableBatchSource(testAllocator, batch)
	op, err := makeProjOp(source, intType)
	require.NoError(b, err)
	op.Init()

	b.SetBytes(int64(8 * coldata.BatchSize() * 2))
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}

func BenchmarkProjOp(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	getInputTypesForColtype := func(intType coltypes.T) []types.T {
		switch intType {
		case coltypes.Int64:
			return []types.T{*types.Int, *types.Int}
		case coltypes.Int32:
			return []types.T{*types.Int4, *types.Int4}
		default:
			b.Fatalf("unsupported type: %s", intType)
			return nil
		}
	}
	projOpMap := map[string]func(*RepeatableBatchSource, coltypes.T) (Operator, error){
		"projPlusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, source, getInputTypesForColtype(intType),
				"@1 + @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		},
		"projMinusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, source, getInputTypesForColtype(intType),
				"@1 - @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		},
		"projMultIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, source, getInputTypesForColtype(intType),
				"@1 * @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		},
		"projDivIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) (Operator, error) {
			return createTestProjectingOperator(
				ctx, flowCtx, source, getInputTypesForColtype(intType),
				"@1 / @2" /* projectingExpr */, false, /* canFallbackToRowexec */
			)
		},
	}

	for projOp, makeProjOp := range projOpMap {
		for _, intType := range []coltypes.T{coltypes.Int64, coltypes.Int32} {
			for _, useSel := range []bool{true, false} {
				for _, hasNulls := range []bool{true, false} {
					b.Run(fmt.Sprintf("op=%s/type=%s/useSel=%t/hasNulls=%t",
						projOp, intType, useSel, hasNulls), func(b *testing.B) {
						benchmarkProjOp(b, makeProjOp, useSel, hasNulls, intType)
					})
				}
			}
		}
	}
}
