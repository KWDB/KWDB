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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFloatToIntCastBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	flowCtx, cleanup := createTestCastFlowCtx(ctx)
	defer cleanup()

	validMaxInt64Float := math.Nextafter(float64(math.MaxInt64), 0)
	testCases := []struct {
		name     string
		fromTyp  *types.T
		toTyp    *types.T
		input    tuples
		expected tuples
	}{
		{
			name:    "Float8ToInt2",
			fromTyp: types.Float,
			toTyp:   types.Int2,
			input: tuples{
				{float64(math.MinInt16) - 0.999},
				{float64(math.MinInt16)},
				{float64(math.MaxInt16)},
				{float64(math.MaxInt16) + 0.999},
			},
			expected: tuples{
				{float64(math.MinInt16) - 0.999, int16(math.MinInt16)},
				{float64(math.MinInt16), int16(math.MinInt16)},
				{float64(math.MaxInt16), int16(math.MaxInt16)},
				{float64(math.MaxInt16) + 0.999, int16(math.MaxInt16)},
			},
		},
		{
			name:    "Float4ToInt2",
			fromTyp: types.Float4,
			toTyp:   types.Int2,
			input: tuples{
				{float64(math.MinInt16)},
				{float64(math.MaxInt16)},
			},
			expected: tuples{
				{float64(math.MinInt16), int16(math.MinInt16)},
				{float64(math.MaxInt16), int16(math.MaxInt16)},
			},
		},
		{
			name:    "Float8ToInt4",
			fromTyp: types.Float,
			toTyp:   types.Int4,
			input: tuples{
				{float64(math.MinInt32) - 0.999},
				{float64(math.MinInt32)},
				{float64(math.MaxInt32)},
				{float64(math.MaxInt32) + 0.999},
			},
			expected: tuples{
				{float64(math.MinInt32) - 0.999, int32(math.MinInt32)},
				{float64(math.MinInt32), int32(math.MinInt32)},
				{float64(math.MaxInt32), int32(math.MaxInt32)},
				{float64(math.MaxInt32) + 0.999, int32(math.MaxInt32)},
			},
		},
		{
			name:    "Float4ToInt4",
			fromTyp: types.Float4,
			toTyp:   types.Int4,
			input: tuples{
				{-1024.999},
				{1024.999},
			},
			expected: tuples{
				{-1024.999, int32(-1024)},
				{1024.999, int32(1024)},
			},
		},
		{
			name:    "Float8ToInt8",
			fromTyp: types.Float,
			toTyp:   types.Int,
			input: tuples{
				{validMaxInt64Float},
			},
			expected: tuples{
				{validMaxInt64Float, int64(validMaxInt64Float)},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assertFloatToIntCastExpectedMatchesPerformCast(t, flowCtx.EvalCtx, tc.fromTyp, tc.toTyp, tc.input, tc.expected)
			runTests(t, []tuples{tc.input}, tc.expected, orderedVerifier,
				func(inputs []Operator) (Operator, error) {
					return createTestVectorizedCastOperator(ctx, flowCtx, inputs[0], tc.fromTyp, tc.toTyp)
				})
		})
	}
}

func TestFloatToIntCastOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	flowCtx, cleanup := createTestCastFlowCtx(ctx)
	defer cleanup()

	testCases := []struct {
		name    string
		fromTyp *types.T
		toTyp   *types.T
		value   float64
	}{
		{name: "Float8ToInt2Max", fromTyp: types.Float, toTyp: types.Int2, value: float64(math.MaxInt16) + 1},
		{name: "Float8ToInt2Min", fromTyp: types.Float, toTyp: types.Int2, value: float64(math.MinInt16) - 1},
		{name: "Float4ToInt2Max", fromTyp: types.Float4, toTyp: types.Int2, value: float64(math.MaxInt16) + 1},
		{name: "Float8ToInt4Max", fromTyp: types.Float, toTyp: types.Int4, value: float64(math.MaxInt32) + 1},
		{name: "Float8ToInt4Min", fromTyp: types.Float, toTyp: types.Int4, value: float64(math.MinInt32) - 1},
		{name: "Float4ToInt4Max", fromTyp: types.Float4, toTyp: types.Int4, value: float64(math.MaxInt32) + 1},
		{name: "Float8ToInt2NaN", fromTyp: types.Float, toTyp: types.Int2, value: math.NaN()},
		{name: "Float8ToInt2Inf", fromTyp: types.Float, toTyp: types.Int2, value: math.Inf(1)},
		{name: "Float8ToInt8Max", fromTyp: types.Float, toTyp: types.Int, value: float64(math.MaxInt64)},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := tree.PerformCast(flowCtx.EvalCtx, tree.NewDFloat(tree.DFloat(tc.value)), tc.toTyp)
			require.Error(t, err, "PerformCast should reject out-of-range float %v", tc.value)
			assertFloatToIntCastOutOfRange(ctx, t, flowCtx, tc.fromTyp, tc.toTyp, tc.value)
		})
	}
}

func TestRandomizedCast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	flowCtx, cleanup := createTestCastFlowCtx(ctx)
	defer cleanup()

	datumAsBool := func(d tree.Datum) interface{} {
		return bool(tree.MustBeDBool(d))
	}
	datumAsInt := func(d tree.Datum) interface{} {
		return int(tree.MustBeDInt(d))
	}
	datumAsFloat := func(d tree.Datum) interface{} {
		return float64(tree.MustBeDFloat(d))
	}
	datumAsDecimal := func(d tree.Datum) interface{} {
		return tree.MustBeDDecimal(d).Decimal
	}

	tc := []struct {
		fromTyp      *types.T
		fromPhysType func(tree.Datum) interface{}
		toTyp        *types.T
		toPhysType   func(tree.Datum) interface{}
		// Some types casting can fail, so retry if we
		// generate a datum that is unable to be casted.
		retryGeneration bool
	}{
		//bool -> t tests
		{types.Bool, datumAsBool, types.Bool, datumAsBool, false},
		{types.Bool, datumAsBool, types.Int, datumAsInt, false},
		{types.Bool, datumAsBool, types.Float, datumAsFloat, false},
		// decimal -> t tests
		{types.Decimal, datumAsDecimal, types.Bool, datumAsBool, false},
		// int -> t tests
		{types.Int, datumAsInt, types.Bool, datumAsBool, false},
		{types.Int, datumAsInt, types.Float, datumAsFloat, false},
		{types.Int, datumAsInt, types.Decimal, datumAsDecimal, false},
		// float -> t tests
		{types.Float, datumAsFloat, types.Bool, datumAsBool, false},
		// We can sometimes generate a float outside of the range of the integers,
		// so we want to retry with generation if that occurs.
		{types.Float, datumAsFloat, types.Int, datumAsInt, true},
		{types.Float, datumAsFloat, types.Decimal, datumAsDecimal, false},
	}

	rng, _ := randutil.NewPseudoRand()

	for _, c := range tc {
		t.Run(fmt.Sprintf("%sTo%s", c.fromTyp.String(), c.toTyp.String()), func(t *testing.T) {
			n := 100
			// Make an input vector of length n.
			input := tuples{}
			output := tuples{}
			for i := 0; i < n; i++ {
				// We don't allow any NULL datums to be generated, so disable
				// this ability in the RandDatum function.
				fromDatum := sqlbase.RandDatum(rng, c.fromTyp, false)
				var (
					toDatum tree.Datum
					err     error
				)
				toDatum, err = tree.PerformCast(flowCtx.EvalCtx, fromDatum, c.toTyp)
				if c.retryGeneration {
					for err != nil {
						// If we are allowed to retry, make a new datum and cast it on error.
						fromDatum = sqlbase.RandDatum(rng, c.fromTyp, false)
						toDatum, err = tree.PerformCast(flowCtx.EvalCtx, fromDatum, c.toTyp)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
				}
				input = append(input, tuple{c.fromPhysType(fromDatum)})
				output = append(output, tuple{c.fromPhysType(fromDatum), c.toPhysType(toDatum)})
			}
			runTests(t, []tuples{input}, output, orderedVerifier,
				func(input []Operator) (Operator, error) {
					return createTestCastOperator(ctx, flowCtx, input[0], c.fromTyp, c.toTyp)
				})
		})
	}
}

func BenchmarkCastOp(b *testing.B) {
	ctx := context.Background()
	flowCtx, cleanup := createTestCastFlowCtx(ctx)
	defer cleanup()
	rng, _ := randutil.NewPseudoRand()
	for _, typePair := range [][]types.T{
		{*types.Int, *types.Float},
		{*types.Int, *types.Decimal},
		{*types.Float, *types.Decimal},
	} {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(
					fmt.Sprintf("useSel=%t/hasNulls=%t/%s_to_%s",
						useSel, hasNulls, typePair[0].Name(), typePair[1].Name(),
					), func(b *testing.B) {
						fromType := typeconv.FromColumnType(&typePair[0])
						nullProbability := nullProbability
						if !hasNulls {
							nullProbability = 0
						}
						selectivity := selectivity
						if !useSel {
							selectivity = 1.0
						}
						batch := randomBatchWithSel(
							testAllocator, rng, []coltypes.T{fromType},
							coldata.BatchSize(), nullProbability, selectivity,
						)
						source := NewRepeatableBatchSource(testAllocator, batch)
						op, err := createTestCastOperator(ctx, flowCtx, source, &typePair[0], &typePair[1])
						require.NoError(b, err)
						b.SetBytes(int64(8 * coldata.BatchSize()))
						b.ResetTimer()
						op.Init()
						for i := 0; i < b.N; i++ {
							op.Next(ctx)
						}
					})
			}
		}
	}
}

func createTestCastFlowCtx(ctx context.Context) (*execinfra.FlowCtx, func()) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	return flowCtx, func() {
		evalCtx.Stop(ctx)
	}
}

func assertFloatToIntCastExpectedMatchesPerformCast(
	t *testing.T,
	evalCtx *tree.EvalContext,
	fromTyp *types.T,
	toTyp *types.T,
	input tuples,
	expected tuples,
) {
	t.Helper()
	require.Equal(t, len(input), len(expected))
	toPhysFn := typeconv.GetDatumToPhysicalFn(toTyp)
	for i := range input {
		fromVal, ok := input[i][0].(float64)
		require.True(t, ok, "input row %d must be float64", i)
		castDatum, castErr := tree.PerformCast(evalCtx, tree.NewDFloat(tree.DFloat(fromVal)), toTyp)
		require.NoError(t, castErr, "PerformCast failed for input row %d", i)
		gotPhys, err := toPhysFn(castDatum)
		require.NoError(t, err)
		require.Equal(t, expected[i][1], gotPhys, "PerformCast mismatch for input row %d", i)
	}
}

func assertFloatToIntCastOutOfRange(
	ctx context.Context,
	t *testing.T,
	flowCtx *execinfra.FlowCtx,
	fromTyp *types.T,
	toTyp *types.T,
	value float64,
) {
	t.Helper()
	runTestsWithFn(t, []tuples{{{value}}}, [][]coltypes.T{{coltypes.Float64}},
		func(t *testing.T, inputs []Operator) {
			op, err := createTestVectorizedCastOperator(ctx, flowCtx, inputs[0], fromTyp, toTyp)
			require.NoError(t, err)
			op.Init()
			err = execerror.CatchVectorizedRuntimeError(func() {
				for {
					b := op.Next(ctx)
					if b.Length() == 0 {
						break
					}
				}
			})
			require.True(t, errors.Is(err, tree.ErrIntOutOfRange), "expected ErrIntOutOfRange, got %v", err)
		})
}

// castTypeNameForTest returns the SQL type name used in cast expressions for
// tests. types.T.Name() is not sufficient because the parser maps the INT
// keyword to INT4, not INT8.
func castTypeNameForTest(t *types.T) string {
	return strings.ToLower(t.SQLString())
}

func createTestVectorizedCastOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, input Operator, fromTyp *types.T, toTyp *types.T,
) (Operator, error) {
	// Pure vectorized cast for float-to-int regression tests. Do not allow
	// rowexec fallback, which would mask colexec boundary bugs.
	return createTestProjectingOperator(
		ctx, flowCtx, input, []types.T{*fromTyp},
		fmt.Sprintf("@1::%s", castTypeNameForTest(toTyp)), false, /* canFallbackToRowexec */
	)
}

func createTestCastOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, input Operator, fromTyp *types.T, toTyp *types.T,
) (Operator, error) {
	// We currently don't support casting to decimal type (other than when
	// casting from decimal with the same precision), so we will allow falling
	// back to row-by-row engine.
	return createTestProjectingOperator(
		ctx, flowCtx, input, []types.T{*fromTyp},
		fmt.Sprintf("@1::%s", castTypeNameForTest(toTyp)), true, /* canFallbackToRowexec */
	)
}
