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

package builtins

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
)

// testAggregateResultDeepCopy verifies that tree.Datum returned from tree.AggregateFunc's
// Result() method are not mutated during future accumulation. It verifies this by
// printing all values to strings immediately after calling Result(), and later
// printing all values to strings once the accumulation has finished. If the string
// slices are not equal, it means that the result tree.Datums were modified during later
// accumulation, which violates the "deep copy of any internal state" condition.
func testAggregateResultDeepCopy(
	t *testing.T,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	aggImpl := aggFunc([]*types.T{vals[0].ResolvedType()}, evalCtx, nil)
	defer aggImpl.Close(context.Background())
	runningDatums := make([]tree.Datum, len(vals))
	runningStrings := make([]string, len(vals))
	for i := range vals {
		if err := aggImpl.Add(context.Background(), vals[i]); err != nil {
			t.Fatal(err)
		}
		res, err := aggImpl.Result()
		if err != nil {
			t.Fatal(err)
		}
		runningDatums[i] = res
		runningStrings[i] = res.String()
	}
	finalStrings := make([]string, len(vals))
	for i, d := range runningDatums {
		finalStrings[i] = d.String()
	}
	if !reflect.DeepEqual(runningStrings, finalStrings) {
		t.Errorf("Aggregate result mutated during future accumulation: initial results were %v,"+
			" later results were %v", runningStrings, finalStrings)
	}
}

func testAggregateResultDeepCopyGapfill(
	t *testing.T,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
	val tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	aggImpl := aggFunc([]*types.T{vals[0].ResolvedType()}, evalCtx, nil)
	defer aggImpl.Close(context.Background())
	runningDatums := make([]tree.Datum, len(vals))
	runningStrings := make([]string, len(vals))
	for i := range vals {

		if err := aggImpl.Add(context.Background(), vals[i], val); err != nil {
			t.Fatal(err)
		}
		res, err := aggImpl.Result()
		if err != nil {
			t.Fatal(err)
		}
		runningDatums[i] = res
		runningStrings[i] = res.String()
	}
	finalStrings := make([]string, len(vals))
	for i, d := range runningDatums {
		finalStrings[i] = d.String()
	}
	if !reflect.DeepEqual(runningStrings, finalStrings) {
		t.Errorf("Aggregate result mutated during future accumulation: initial results were %v,"+
			" later results were %v", runningStrings, finalStrings)
	}
}

func TestAvgIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntAvgAggregate, makeIntTestDatum(10))
}

func TestAvgFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatAvgAggregate, makeFloatTestDatum(10))
}

func TestAvgDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalAvgAggregate, makeDecimalTestDatum(10))
}

func TestAvgIntervalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntervalAvgAggregate, makeIntervalTestDatum(10))
}

func TestBitAndIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitAndAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitAndAggregate, makeTestWithNullDatum(10, makeIntTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitAndAggregate, makeIntTestDatum(10))
	})
}

func TestBitOrIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitOrAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitOrAggregate, makeTestWithNullDatum(10, makeIntTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitOrAggregate, makeIntTestDatum(10))
	})
}

func TestBoolAndResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newBoolAndAggregate, makeBoolTestDatum(10))
}

func TestBoolOrResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newBoolOrAggregate, makeBoolTestDatum(10))
}

func TestCountResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newCountAggregate, makeIntTestDatum(10))
}

func TestMaxIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeIntTestDatum(10))
}

func TestMaxFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeFloatTestDatum(10))
}

func TestMaxDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeDecimalTestDatum(10))
}

func TestMaxBoolResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeBoolTestDatum(10))
}

func TestMinIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeIntTestDatum(10))
}

func TestMinFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeFloatTestDatum(10))
}

func TestMinDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeDecimalTestDatum(10))
}

func TestMinBoolResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeBoolTestDatum(10))
}

func TestSumSmallIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newSmallIntSumAggregate, makeSmallIntTestDatum(10))
}

func TestSumIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntSumAggregate, makeIntTestDatum(10))
}

func TestSumFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatSumAggregate, makeFloatTestDatum(10))
}

func TestSumDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalSumAggregate, makeDecimalTestDatum(10))
}

func TestSumIntervalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntervalSumAggregate, makeIntervalTestDatum(10))
}

func TestVarianceIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntVarianceAggregate, makeIntTestDatum(10))
}

func TestVarianceFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatVarianceAggregate, makeFloatTestDatum(10))
}

func TestVarianceDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalVarianceAggregate, makeDecimalTestDatum(10))
}

func TestSqrDiffIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntSqrDiffAggregate, makeIntTestDatum(10))
}

func TestSqrDiffFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatSqrDiffAggregate, makeFloatTestDatum(10))
}

func TestSqrDiffDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalSqrDiffAggregate, makeDecimalTestDatum(10))
}

func TestStdDevIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntStdDevAggregate, makeIntTestDatum(10))
}

func TestStdDevFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatStdDevAggregate, makeFloatTestDatum(10))
}

func TestStdDevDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalStdDevAggregate, makeDecimalTestDatum(10))
}
func TestGapfillResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	randomInt := rng.Int31()
	if randomInt == 0 {
		randomInt = 1
	}
	var datum tree.Datum = tree.NewDInt(tree.DInt(randomInt))
	testAggregateResultDeepCopyGapfill(t, newTimeBucketAggregate, makeTimestampTestDatum(10), datum)
}

// makeNullTestDatum will create an array of only DNull
// values to make sure the aggregation handles only nulls.
func makeNullTestDatum(count int) []tree.Datum {
	values := make([]tree.Datum, count)
	for i := range values {
		values[i] = tree.DNull
	}
	return values
}

func makeIntTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.NewDInt(tree.DInt(rng.Int63()))
	}
	return vals
}

// makeTestWithNullDatum will call the maker function
// to generate an array of datums, and then a null datum
// will be placed randomly in the array of datums and
// returned. Use this to ensure proper partial null
// handling of aggregations.
func makeTestWithNullDatum(count int, maker func(count int) []tree.Datum) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()
	values := maker(count)
	values[rng.Int()%count] = tree.DNull
	return values
}

// makeSmallIntTestDatum creates integers that are sufficiently
// smaller than 2^64-1 that they can be added to each other for a
// significant part of the test without overflow. This is meant to
// test the implementation of aggregates that can use an int64 to
// optimize computations small decimal values.
func makeSmallIntTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		sign := int32(1)
		if rng.Int31()&1 == 0 {
			sign = -1
		}
		vals[i] = tree.NewDInt(tree.DInt(rng.Int31() * sign))
	}
	return vals
}

func makeFloatTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.NewDFloat(tree.DFloat(rng.Float64()))
	}
	return vals
}

func makeDecimalTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		dd := &tree.DDecimal{}
		if _, err := dd.SetFloat64(rng.Float64()); err != nil {
			panic(err)
		}
		vals[i] = dd
	}
	return vals
}

func makeTimestampTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()
	vals := make([]tree.Datum, count)
	for i := range vals {
		time := timeutil.Unix(rng.Int63(), 0)
		vals[i] = &tree.DTimestamp{Time: time}
	}
	return vals
}

func makeBoolTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.MakeDBool(tree.DBool(rng.Int31n(2) == 0))
	}
	return vals
}

func makeIntervalTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = &tree.DInterval{Duration: duration.MakeDuration(rng.Int63n(1000000), rng.Int63n(1000), rng.Int63n(1000))}
	}
	return vals
}

func TestArrayAggNameOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testArrayAggAliasedTypeOverload(t, types.Name)
}

func TestArrayAggOidOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testArrayAggAliasedTypeOverload(t, types.Oid)
}

// testAliasedTypeOverload is a helper function for testing ARRAY_AGG's
// overloads that can take aliased scalar types like NAME and OID.
// These tests are necessary because some ORMs (e.g., sequelize) require
// ARRAY_AGG to work on these aliased types and produce a result with the
// correct type.
func testArrayAggAliasedTypeOverload(t *testing.T, expected *types.T) {
	defer tree.MockNameTypes(map[string]*types.T{
		"a": expected,
	})()
	exprStr := "array_agg(a)"
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		t.Fatalf("%s: %v", exprStr, err)
	}
	typ := types.MakeArray(expected)
	typedExpr, err := tree.TypeCheck(expr, nil, typ)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	if !typedExpr.ResolvedType().ArrayContents().Identical(expected) {
		t.Fatalf(
			"Expression has incorrect type: expected %v but got %v",
			expected,
			typedExpr.ResolvedType(),
		)
	}
}

func runBenchmarkAggregate(
	b *testing.B,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	params := []*types.T{vals[0].ResolvedType()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			aggImpl := aggFunc(params, evalCtx, nil)
			defer aggImpl.Close(context.Background())
			for i := range vals {
				if err := aggImpl.Add(context.Background(), vals[i]); err != nil {
					b.Fatal(err)
				}
			}
			res, err := aggImpl.Result()
			if err != nil || res == nil {
				b.Errorf("taking result of aggregate implementation %T failed", aggImpl)
			}
		}()
	}
}

func runBenchmarkAggregateGapfill(
	b *testing.B,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	params := []*types.T{vals[0].ResolvedType()}
	b.ResetTimer()
	rng, _ := randutil.NewPseudoRand()
	randomInt := rng.Int31()
	if randomInt == 0 {
		randomInt = 1
	}
	var datum tree.Datum = tree.NewDInt(tree.DInt(randomInt))

	for i := 0; i < b.N; i++ {
		func() {
			aggImpl := aggFunc(params, evalCtx, nil)
			defer aggImpl.Close(context.Background())
			for i := range vals {
				if err := aggImpl.Add(context.Background(), vals[i], datum); err != nil {
					b.Fatal(err)
				}
			}
			res, err := aggImpl.Result()
			if err != nil || res == nil {
				b.Errorf("taking result of aggregate implementation %T failed", aggImpl)
			}
		}()
	}
}

func BenchmarkAvgAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntAvgAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateSmallInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntAvgAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatAvgAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalAvgAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateInterval(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntervalAvgAggregate, makeIntervalTestDatum(count))
		})
	}
}

func BenchmarkCountAggregate(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newCountAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkSumIntAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newSmallIntSumAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntSumAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateSmallInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntSumAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatSumAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalSumAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntVarianceAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatVarianceAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalVarianceAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkSqrDiffAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntSqrDiffAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkSqrDiffAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatSqrDiffAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkSqrDiffAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalSqrDiffAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntStdDevAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatStdDevAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalStdDevAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkGapfillAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregateGapfill(b, newTimeBucketAggregate, makeTimestampTestDatum(count))
		})
	}
}

func TestNormAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		values   []tree.Datum
		expected string
	}{
		{
			name:     "ints",
			values:   []tree.Datum{tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(0)},
			expected: "5",
		},
		{
			name:     "floats",
			values:   []tree.Datum{tree.NewDFloat(3), tree.NewDFloat(4), tree.NewDFloat(0)},
			expected: "5",
		},
		{
			name: "decimals",
			values: func() []tree.Datum {
				vals := make([]tree.Datum, 3)
				d1 := &tree.DDecimal{}
				d1.SetInt64(3)
				vals[0] = d1
				d2 := &tree.DDecimal{}
				d2.SetInt64(4)
				vals[1] = d2
				d3 := &tree.DDecimal{}
				d3.SetInt64(0)
				vals[2] = d3
				return vals
			}(),
			expected: "5",
		},
		{
			name:     "single",
			values:   []tree.Datum{tree.NewDInt(5)},
			expected: "5",
		},
		{
			name:     "zeros",
			values:   []tree.Datum{tree.NewDInt(0), tree.NewDInt(0), tree.NewDInt(0)},
			expected: "0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			agg := newDecimalNormAggregate([]*types.T{tc.values[0].ResolvedType()}, evalCtx, nil)
			defer agg.Close(context.Background())
			for _, v := range tc.values {
				if err := agg.Add(context.Background(), v); err != nil {
					t.Fatalf("add: %v", err)
				}
			}
			res, err := agg.Result()
			if err != nil || res == tree.DNull {
				t.Fatalf("result err=%v res=%v", err, res)
			}
			if res.String() != tc.expected {
				t.Fatalf("expected %s got %s", tc.expected, res.String())
			}
		})
	}
}

func TestQuantileAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		values   []tree.Datum
		q        tree.Datum
		expected string
	}{
		{
			name:     "median ints",
			values:   []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5)},
			q:        tree.NewDFloat(0.5),
			expected: "3",
		},
		{
			name:     "q1 ints",
			values:   []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5)},
			q:        tree.NewDFloat(0.25),
			expected: "2",
		},
		{
			name:     "q3 ints",
			values:   []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5)},
			q:        tree.NewDFloat(0.75),
			expected: "4",
		},
		{
			name:     "median floats",
			values:   []tree.Datum{tree.NewDFloat(1), tree.NewDFloat(2), tree.NewDFloat(3), tree.NewDFloat(4), tree.NewDFloat(5)},
			q:        tree.NewDFloat(0.5),
			expected: "3",
		},
		{
			name: "median decimals",
			values: func() []tree.Datum {
				arr := make([]tree.Datum, 5)
				for i := 1; i <= 5; i++ {
					d := &tree.DDecimal{}
					d.SetInt64(int64(i))
					arr[i-1] = d
				}
				return arr
			}(),
			q:        tree.NewDFloat(0.5),
			expected: "3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			agg := newQuantileAggregate([]*types.T{tc.values[0].ResolvedType()}, evalCtx, nil)
			defer agg.Close(context.Background())
			for _, v := range tc.values {
				if err := agg.Add(context.Background(), v, tc.q); err != nil {
					t.Fatalf("add: %v", err)
				}
			}
			res, err := agg.Result()
			if err != nil || res == tree.DNull {
				t.Fatalf("result err=%v res=%v", err, res)
			}
			// Compare numerically to avoid string format differences like 3 vs 3.0
			exp, _ := strconv.ParseFloat(tc.expected, 64)
			got := datumToFloat(res)
			if !floatsAlmostEqual(got, exp) {
				t.Fatalf("expected %s got %s", tc.expected, res.String())
			}
		})
	}
}

func TestVarSampAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		values   []tree.Datum
		newAgg   func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc
		typ      *types.T
		expected *string
	}{
		{
			name:     "ints",
			values:   []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5)},
			newAgg:   newIntVarianceAggregate,
			typ:      types.Int,
			expected: strPtr("2.5"),
		},
		{
			name:     "floats",
			values:   []tree.Datum{tree.NewDFloat(1), tree.NewDFloat(2), tree.NewDFloat(3), tree.NewDFloat(4), tree.NewDFloat(5)},
			newAgg:   newFloatVarianceAggregate,
			typ:      types.Float,
			expected: strPtr("2.5"),
		},
		{
			name: "decimals",
			values: func() []tree.Datum {
				arr := make([]tree.Datum, 5)
				for i := 1; i <= 5; i++ {
					d := &tree.DDecimal{}
					d.SetInt64(int64(i))
					arr[i-1] = d
				}
				return arr
			}(),
			newAgg:   newDecimalVarianceAggregate,
			typ:      types.Decimal,
			expected: strPtr("2.5"),
		},
		{
			name:     "single -> NULL",
			values:   []tree.Datum{tree.NewDInt(5)},
			newAgg:   newIntVarianceAggregate,
			typ:      types.Int,
			expected: nil,
		},
		{
			name:     "identical -> 0",
			values:   []tree.Datum{tree.NewDInt(3), tree.NewDInt(3), tree.NewDInt(3)},
			newAgg:   newIntVarianceAggregate,
			typ:      types.Int,
			expected: strPtr("0"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			agg := tc.newAgg([]*types.T{tc.typ}, evalCtx, nil)
			defer agg.Close(context.Background())
			for _, v := range tc.values {
				if err := agg.Add(context.Background(), v); err != nil {
					t.Fatalf("add: %v", err)
				}
			}
			res, err := agg.Result()
			if err != nil {
				t.Fatalf("result: %v", err)
			}
			if tc.expected == nil {
				if res != tree.DNull {
					t.Fatalf("expected NULL got %v", res)
				}
				return
			}
			if res == tree.DNull {
				t.Fatalf("expected %s got NULL", *tc.expected)
			}
			exp, _ := strconv.ParseFloat(*tc.expected, 64)
			got := datumToFloat(res)
			if !floatsAlmostEqual(got, exp) {
				t.Fatalf("expected %s got %s", *tc.expected, res.String())
			}
		})
	}
}

func TestVarPopAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		values   []tree.Datum
		newAgg   func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc
		typ      *types.T
		expected *string // nil means expect NULL
	}{
		{
			name:     "ints",
			values:   []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3), tree.NewDInt(4), tree.NewDInt(5)},
			newAgg:   newIntPopVarianceAggregate,
			typ:      types.Int,
			expected: strPtr("2"),
		},
		{
			name:     "floats",
			values:   []tree.Datum{tree.NewDFloat(1), tree.NewDFloat(2), tree.NewDFloat(3), tree.NewDFloat(4), tree.NewDFloat(5)},
			newAgg:   newFloatPopVarianceAggregate,
			typ:      types.Float,
			expected: strPtr("2"),
		},
		{
			name: "decimals",
			values: func() []tree.Datum {
				arr := make([]tree.Datum, 5)
				for i := 1; i <= 5; i++ {
					d := &tree.DDecimal{}
					d.SetInt64(int64(i))
					arr[i-1] = d
				}
				return arr
			}(),
			newAgg:   newDecimalPopVarianceAggregate,
			typ:      types.Decimal,
			expected: strPtr("2"),
		},
		{
			name:     "single -> 0",
			values:   []tree.Datum{tree.NewDInt(5)},
			newAgg:   newIntPopVarianceAggregate,
			typ:      types.Int,
			expected: strPtr("0"),
		},
		{
			name:     "empty -> NULL",
			values:   []tree.Datum{},
			newAgg:   newIntPopVarianceAggregate,
			typ:      types.Int,
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			agg := tc.newAgg([]*types.T{tc.typ}, evalCtx, nil)
			defer agg.Close(context.Background())
			for _, v := range tc.values {
				if err := agg.Add(context.Background(), v); err != nil {
					t.Fatalf("add: %v", err)
				}
			}
			res, err := agg.Result()
			if err != nil {
				t.Fatalf("result: %v", err)
			}
			if tc.expected == nil {
				if res != tree.DNull {
					t.Fatalf("expected NULL got %v", res)
				}
				return
			}
			if res == tree.DNull {
				t.Fatalf("expected %s got NULL", *tc.expected)
			}
			exp, _ := strconv.ParseFloat(*tc.expected, 64)
			got := datumToFloat(res)
			if !floatsAlmostEqual(got, exp) {
				t.Fatalf("expected %s got %s", *tc.expected, res.String())
			}
		})
	}
}

func strPtr(s string) *string { return &s }

// datumToFloat converts a Datum expected to represent a numeric value to float64 for comparison.
func datumToFloat(d tree.Datum) float64 {
	switch v := d.(type) {
	case *tree.DInt:
		return float64(int64(*v))
	case *tree.DFloat:
		return float64(*v)
	case *tree.DDecimal:
		f, _ := v.Decimal.Float64()
		return f
	default:
		// Fallback to parsing string
		f, _ := strconv.ParseFloat(d.String(), 64)
		return f
	}
}

// floatsAlmostEqual compares two float64 values with a small epsilon.
func floatsAlmostEqual(a, b float64) bool {
	if a == b {
		return true
	}
	const eps = 1e-9
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= eps*(1+abs(a)+abs(b))
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestAggregateFunctions covers all aggregate built-in functions.
func TestAggregateFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	// Create a safe empty aggregate context
	emptyArgs := tree.Datums{}

	// -------------------------------------------------------------------------
	// Test integer average (int → decimal)
	// -------------------------------------------------------------------------
	t.Run("avg_int", func(t *testing.T) {
		agg := newIntAvgAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(4))
		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3")
		assert.Equal(t, expected, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test float average (float → float)
	// -------------------------------------------------------------------------
	t.Run("avg_float", func(t *testing.T) {
		agg := newFloatAvgAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test decimal average (decimal → decimal)
	// -------------------------------------------------------------------------
	t.Run("avg_decimal", func(t *testing.T) {
		agg := newDecimalAvgAggregate(nil, ctx, emptyArgs)
		d1, _ := tree.ParseDDecimal("2")
		d2, _ := tree.ParseDDecimal("4")
		_ = agg.Add(context.Background(), d1)
		_ = agg.Add(context.Background(), d2)
		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3")
		assert.Equal(t, expected, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test interval average (interval → interval)
	// -------------------------------------------------------------------------
	t.Run("avg_interval", func(t *testing.T) {
		agg := newIntervalAvgAggregate(nil, ctx, emptyArgs)
		iv1 := tree.NewDInterval(duration.MakeDuration(2000000000, 0, 0), types.IntervalTypeMetadata{}) // 2s
		iv2 := tree.NewDInterval(duration.MakeDuration(4000000000, 0, 0), types.IntervalTypeMetadata{}) // 4s
		_ = agg.Add(context.Background(), iv1)
		_ = agg.Add(context.Background(), iv2)
		res, err := agg.Result()
		assert.NoError(t, err)

		expected := tree.NewDInterval(duration.MakeDuration(3000000000, 0, 0), types.IntervalTypeMetadata{}) // 3s
		assert.Equal(t, expected, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test avg with NULL values (ignores NULL)
	// -------------------------------------------------------------------------
	t.Run("avg_int_with_null", func(t *testing.T) {
		agg := newIntAvgAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(3))
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(3))
		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3")
		assert.Equal(t, expected, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test avg with empty input (returns NULL)
	// -------------------------------------------------------------------------
	t.Run("avg_int_empty_returns_null", func(t *testing.T) {
		agg := newIntAvgAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test avg single value
	// -------------------------------------------------------------------------
	t.Run("avg_int_single_value", func(t *testing.T) {
		agg := newIntAvgAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(5))
		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("5")
		assert.Equal(t, expected, res)

		agg.Close(context.Background())
	})
	// --------------------------------------------------------------------------------
	// Test bit_and / bit_or
	// --------------------------------------------------------------------------------
	t.Run("bit_and", func(t *testing.T) {
		agg := newBitAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(3))
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(1), res)
	})

	t.Run("bit_or", func(t *testing.T) {
		agg := newBitOrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(3), res)
	})

	// --------------------------------------------------------------------------------
	// Test bool_and / bool_or
	// --------------------------------------------------------------------------------
	t.Run("bool_and", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		res, _ := agg.Result()
		assert.Equal(t, tree.MakeDBool(true), res)
	})

	t.Run("bool_or", func(t *testing.T) {
		agg := newBoolOrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(false))
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		res, _ := agg.Result()
		assert.Equal(t, tree.MakeDBool(true), res)
	})

	// --------------------------------------------------------------------------------
	// Test concat_agg / string_agg
	// --------------------------------------------------------------------------------
	t.Run("concat_agg", func(t *testing.T) {
		agg := newStringConcatAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDString("a"))
		_ = agg.Add(context.Background(), tree.NewDString("b"))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDString("ab"), res)
	})

	// -------------------------------------------------------------------------
	// Test corr with perfect positive correlation (int, int)
	// -------------------------------------------------------------------------
	t.Run("corr_int_int_perfect_positive", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDInt(4))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDInt(6))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with perfect negative correlation (float, float)
	// -------------------------------------------------------------------------
	t.Run("corr_float_float_perfect_negative", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.0), tree.NewDFloat(3.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0), tree.NewDFloat(1.0))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(-1.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with mixed types: int, float
	// -------------------------------------------------------------------------
	t.Run("corr_int_float", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDFloat(1.0))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDFloat(3.0))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with mixed types: float, int
	// -------------------------------------------------------------------------
	t.Run("corr_float_int", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.0), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0), tree.NewDInt(3))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with NULL values (ignores NULL pairs)
	// -------------------------------------------------------------------------
	t.Run("corr_with_null_values", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDInt(6))
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDInt(2))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(1.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with insufficient data (returns NULL)
	// -------------------------------------------------------------------------
	t.Run("corr_insufficient_data_returns_null", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDInt(2))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test corr with empty input (returns NULL)
	// -------------------------------------------------------------------------
	t.Run("corr_empty_input_returns_null", func(t *testing.T) {
		agg := newCorrAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test count / count_rows
	// --------------------------------------------------------------------------------
	t.Run("count", func(t *testing.T) {
		agg := newCountAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(2), res)
	})

	t.Run("count_rows", func(t *testing.T) {
		agg := newCountRowsAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.DNull)
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(2), res)
	})

	// -------------------------------------------------------------------------
	// Test every returns true when all values are true
	// -------------------------------------------------------------------------
	t.Run("every_all_true", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every returns false when any value is false
	// -------------------------------------------------------------------------
	t.Run("every_has_false", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.MakeDBool(false))
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(false), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("every_with_null_values", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.DNull)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every returns false if false exists with nulls
	// -------------------------------------------------------------------------
	t.Run("every_false_with_nulls", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.MakeDBool(false))
		_ = agg.Add(context.Background(), tree.DNull)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(false), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every returns null when all inputs are null
	// -------------------------------------------------------------------------
	t.Run("every_all_null_returns_null", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.DNull)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every returns null with empty input
	// -------------------------------------------------------------------------
	t.Run("every_empty_returns_null", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every single true value
	// -------------------------------------------------------------------------
	t.Run("every_single_true", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(true))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test every single false value
	// -------------------------------------------------------------------------
	t.Run("every_single_false", func(t *testing.T) {
		agg := newBoolAndAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.MakeDBool(false))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(false), res)
		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test max / min
	// --------------------------------------------------------------------------------
	t.Run("max", func(t *testing.T) {
		agg := newMaxAggregate([]*types.T{types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(5))
		_ = agg.Add(context.Background(), tree.NewDInt(10))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(10), res)
	})

	t.Run("min", func(t *testing.T) {
		agg := newMinAggregate([]*types.T{types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(5))
		_ = agg.Add(context.Background(), tree.NewDInt(10))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDInt(5), res)
	})

	// --------------------------------------------------------------------------------
	// Test first / last
	// --------------------------------------------------------------------------------
	t.Run("first_last_basic", func(t *testing.T) {
		ts1 := tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond)
		ts2 := tree.MakeDTimestampTZ(timeutil.Now().Add(time.Second), time.Microsecond)

		firstAgg := newFirstAggregate([]*types.T{types.Int}, ctx, emptyArgs)
		_ = firstAgg.Add(context.Background(), tree.NewDInt(100), ts1)
		_ = firstAgg.Add(context.Background(), tree.NewDInt(200), ts2)
		res1, _ := firstAgg.Result()
		assert.Equal(t, tree.NewDInt(100), res1)

		lastAgg := newLastAggregate([]*types.T{types.Int}, ctx, emptyArgs)
		_ = lastAgg.Add(context.Background(), tree.NewDInt(100), ts1)
		_ = lastAgg.Add(context.Background(), tree.NewDInt(200), ts2)
		res2, _ := lastAgg.Result()
		assert.Equal(t, tree.NewDInt(200), res2)
	})

	// Prepare test timestamps
	ts1 := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
	ts2 := tree.MakeDTimestampTZ(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), time.Microsecond)
	ts3 := tree.MakeDTimestampTZ(time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), time.Microsecond)

	// -------------------------------------------------------------------------
	// Test first returns the earliest value
	// -------------------------------------------------------------------------
	t.Run("first_int_basic", func(t *testing.T) {
		agg := newFirstAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(20), ts1)
		_ = agg.Add(context.Background(), tree.NewDInt(30), ts3)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(20), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test first ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("first_with_null", func(t *testing.T) {
		agg := newFirstAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(20), ts1)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(20), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test first returns null when empty
	// -------------------------------------------------------------------------
	t.Run("first_empty_returns_null", func(t *testing.T) {
		agg := newFirstAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("firstts_basic", func(t *testing.T) {
		agg := newFirsttsAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(20), ts1)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, ts1, res)
		agg.Close(context.Background())
	})

	t.Run("firstts_empty_null", func(t *testing.T) {
		agg := newFirsttsAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("first_row_basic", func(t *testing.T) {
		agg := newFirstrowAggregate([]*types.T{types.String, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDString("b"), ts2)
		_ = agg.Add(context.Background(), tree.NewDString("a"), ts1)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("a"), res)
		agg.Close(context.Background())
	})

	t.Run("first_row_ts_basic", func(t *testing.T) {
		agg := newFirstrowtsAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(100), ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(200), ts1)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, ts1, res)
		agg.Close(context.Background())
	})

	// Test timestamps
	lastTS1 := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
	lastTS2 := tree.MakeDTimestampTZ(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), time.Microsecond)
	lastTS3 := tree.MakeDTimestampTZ(time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), time.Microsecond)
	withDeadline := tree.Datums{tree.MakeDTimestampTZ(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), time.Microsecond)}
	deadlineArgs := tree.Datums{tree.MakeDTimestampTZ(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), time.Microsecond)}
	// -------------------------------------------------------------------------
	// Test last with 2 args (value, timestamp)
	// -------------------------------------------------------------------------
	t.Run("last_2args_int_basic", func(t *testing.T) {
		agg := newLastAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), lastTS1)
		_ = agg.Add(context.Background(), tree.NewDInt(20), lastTS2)
		_ = agg.Add(context.Background(), tree.NewDInt(30), lastTS3)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(30), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test last with 3 args (value, timestamp, deadline)
	// -------------------------------------------------------------------------
	t.Run("last_3args_with_deadline", func(t *testing.T) {
		agg := newLastAggregate([]*types.T{types.Int, types.TimestampTZ, types.TimestampTZ}, ctx, withDeadline)
		_ = agg.Add(context.Background(), tree.NewDInt(10), lastTS1, withDeadline[0])
		_ = agg.Add(context.Background(), tree.NewDInt(20), lastTS2, withDeadline[0])

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(20), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test last ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("last_with_null_values", func(t *testing.T) {
		agg := newLastAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, lastTS1)
		_ = agg.Add(context.Background(), tree.NewDInt(50), lastTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(50), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test last returns NULL on empty input
	// -------------------------------------------------------------------------
	t.Run("last_empty_returns_null", func(t *testing.T) {
		agg := newLastAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test lastts returns latest timestamp
	// -------------------------------------------------------------------------
	t.Run("lastts_basic", func(t *testing.T) {
		agg := newLasttsAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), lastTS1)
		_ = agg.Add(context.Background(), tree.NewDInt(20), lastTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, lastTS2, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test lastts with deadline timestamp
	// -------------------------------------------------------------------------
	t.Run("lastts_with_deadline", func(t *testing.T) {
		agg := newLasttsAggregate([]*types.T{types.Int, types.TimestampTZ, types.TimestampTZ}, ctx, deadlineArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), lastTS1, deadlineArgs[0])

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, lastTS1, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test last_row returns last non-null value
	// -------------------------------------------------------------------------
	t.Run("last_row_string_basic", func(t *testing.T) {
		agg := newLastrowAggregate([]*types.T{types.String, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDString("a"), lastTS1)
		_ = agg.Add(context.Background(), tree.NewDString("z"), lastTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("z"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test last_row_ts returns latest timestamp
	// -------------------------------------------------------------------------
	t.Run("last_row_ts_basic", func(t *testing.T) {
		agg := newLastrowtsAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(99), lastTS1)
		_ = agg.Add(context.Background(), tree.NewDInt(88), lastTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, lastTS2, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend with integer type
	// -------------------------------------------------------------------------
	t.Run("min_extend_int", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), tree.NewDInt(5))
		_ = agg.Add(context.Background(), tree.NewDInt(20), tree.NewDInt(3))
		_ = agg.Add(context.Background(), tree.NewDInt(30), tree.NewDInt(8))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(5), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend with float type
	// -------------------------------------------------------------------------
	t.Run("min_extend_float", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.Float}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDFloat(9.9))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDFloat(1.1))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDFloat(5.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(9.9), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend with bool type
	// -------------------------------------------------------------------------
	t.Run("min_extend_bool", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.Bool}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.MakeDBool(false))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend with string (VarBytes) type
	// -------------------------------------------------------------------------
	t.Run("min_extend_string", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.VarBytes}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDString("zoo"))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDString("apple"))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDString("banana"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("zoo"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend with timestampTZ type
	// -------------------------------------------------------------------------
	t.Run("min_extend_timestamptz", func(t *testing.T) {
		ts1 := tree.MakeDTimestampTZ(time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), time.Microsecond)
		ts2 := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
		ts3 := tree.MakeDTimestampTZ(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), time.Microsecond)

		agg := newMinExtendAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), ts1)
		_ = agg.Add(context.Background(), tree.NewDInt(2), ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(3), ts3)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, ts1, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("min_extend_with_null", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDInt(100))
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDInt(10))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test min_extend returns null when empty
	// -------------------------------------------------------------------------
	t.Run("min_extend_empty_returns_null", func(t *testing.T) {
		agg := newMinExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend with integer type
	// -------------------------------------------------------------------------
	t.Run("max_extend_int", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(10), tree.NewDInt(5))
		_ = agg.Add(context.Background(), tree.NewDInt(20), tree.NewDInt(3))
		_ = agg.Add(context.Background(), tree.NewDInt(30), tree.NewDInt(8))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(8), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend with float type
	// -------------------------------------------------------------------------
	t.Run("max_extend_float", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.Float}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDFloat(9.9))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDFloat(1.1))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDFloat(5.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(5.5), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend with bool type
	// -------------------------------------------------------------------------
	t.Run("max_extend_bool", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.Bool}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.MakeDBool(true))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.MakeDBool(false))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(false), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend with string (VarBytes) type
	// -------------------------------------------------------------------------
	t.Run("max_extend_string", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.VarBytes}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDString("zoo"))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDString("apple"))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDString("banana"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("banana"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend with timestampTZ type
	// -------------------------------------------------------------------------
	t.Run("max_extend_timestamptz", func(t *testing.T) {
		ts1 := tree.MakeDTimestampTZ(time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), time.Microsecond)
		ts2 := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
		ts3 := tree.MakeDTimestampTZ(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), time.Microsecond)

		agg := newMaxExtendAggregate([]*types.T{types.Int, types.TimestampTZ}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1), ts1)
		_ = agg.Add(context.Background(), tree.NewDInt(2), ts2)
		_ = agg.Add(context.Background(), tree.NewDInt(3), ts3)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, ts3, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("max_extend_with_null", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDInt(100))
		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDInt(10))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(10), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test max_extend returns null when empty
	// -------------------------------------------------------------------------
	t.Run("max_extend_empty_returns_null", func(t *testing.T) {
		agg := newMaxExtendAggregate([]*types.T{types.Int, types.Int}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// Prepare common test values
	decVal, _ := tree.ParseDDecimal("10.5")
	strVal := tree.NewDString("test")
	intVal := tree.NewDInt(1)
	floatVal := tree.NewDFloat(1.0)
	boolFalse := tree.MakeDBool(false)

	// -------------------------------------------------------------------------
	// Test matching with int, int, decimal, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_int_int_decimal_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Int, types.Int, types.Decimal, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), intVal, intVal, decVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with float, int, decimal, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_float_int_decimal_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Float, types.Int, types.Decimal, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), floatVal, intVal, decVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with decimal, int, decimal, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_decimal_int_decimal_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Decimal, types.Int, types.Decimal, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), decVal, intVal, decVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with int, int, string, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_int_int_string_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Int, types.Int, types.String, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), intVal, intVal, strVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with float, int, string, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_float_int_string_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Float, types.Int, types.String, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), floatVal, intVal, strVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with decimal, int, string, decimal, int
	// -------------------------------------------------------------------------
	t.Run("matching_decimal_int_string_decimal_int", func(t *testing.T) {
		params := []*types.T{types.Decimal, types.Int, types.String, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), decVal, intVal, strVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDBool(true), res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with int, int, decimal, string, int
	// -------------------------------------------------------------------------
	t.Run("matching_int_int_decimal_string_int", func(t *testing.T) {
		params := []*types.T{types.Int, types.Int, types.Decimal, types.String, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), intVal, intVal, decVal, strVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with float, int, decimal, string, int
	// -------------------------------------------------------------------------
	t.Run("matching_float_int_decimal_string_int", func(t *testing.T) {
		params := []*types.T{types.Float, types.Int, types.Decimal, types.String, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), floatVal, intVal, decVal, strVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with decimal, int, decimal, string, int
	// -------------------------------------------------------------------------
	t.Run("matching_decimal_int_decimal_string_int", func(t *testing.T) {
		params := []*types.T{types.Decimal, types.Int, types.Decimal, types.String, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), decVal, intVal, decVal, strVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with empty input returns null
	// -------------------------------------------------------------------------
	t.Run("matching_empty_returns_null", func(t *testing.T) {
		params := []*types.T{types.Int, types.Int, types.Decimal, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test matching with null values
	// -------------------------------------------------------------------------
	t.Run("matching_with_null_values", func(t *testing.T) {
		params := []*types.T{types.Int, types.Int, types.Decimal, types.Decimal, types.Int}
		agg := newMatchingAggregate(params, ctx, emptyArgs)

		_ = agg.Add(context.Background(), tree.DNull, intVal, decVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, boolFalse, res)

		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test norm
	// --------------------------------------------------------------------------------
	t.Run("norm", func(t *testing.T) {
		agg := newFloatNormAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(3))
		_ = agg.Add(context.Background(), tree.NewDFloat(4))
		res, _ := agg.Result()
		assert.Equal(t, tree.NewDFloat(5), res)
	})

	// -------------------------------------------------------------------------
	// Test string_agg with STRING type (delimiter: ',')
	// -------------------------------------------------------------------------
	t.Run("string_agg_string_basic", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDString(",")}
		agg := newStringConcatAggregate(nil, ctx, delimiterArgs)

		_ = agg.Add(context.Background(), tree.NewDString("a"), tree.NewDString(","))
		_ = agg.Add(context.Background(), tree.NewDString("b"), tree.NewDString(","))
		_ = agg.Add(context.Background(), tree.NewDString("c"), tree.NewDString(","))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("a,b,c"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test string_agg with BYTES type
	// -------------------------------------------------------------------------
	t.Run("string_agg_bytes_basic", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDBytes(",")}
		agg := newBytesConcatAggregate(nil, ctx, delimiterArgs)

		_ = agg.Add(context.Background(), tree.NewDBytes("x"), tree.NewDBytes(","))
		_ = agg.Add(context.Background(), tree.NewDBytes("y"), tree.NewDBytes(","))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDBytes("x,y"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test string_agg ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("string_agg_string_with_null", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDString(",")}
		agg := newStringConcatAggregate(nil, ctx, delimiterArgs)

		_ = agg.Add(context.Background(), tree.DNull, tree.NewDString(","))
		_ = agg.Add(context.Background(), tree.NewDString("hello"), tree.NewDString(","))
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDString(","))
		_ = agg.Add(context.Background(), tree.NewDString("world"), tree.NewDString(","))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("hello,world"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test string_agg with empty input returns NULL
	// -------------------------------------------------------------------------
	t.Run("string_agg_string_empty_null", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDString(",")}
		agg := newStringConcatAggregate(nil, ctx, delimiterArgs)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test string_agg with single value
	// -------------------------------------------------------------------------
	t.Run("string_agg_string_single_value", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDString("|")}
		agg := newStringConcatAggregate(nil, ctx, delimiterArgs)

		_ = agg.Add(context.Background(), tree.NewDString("test"), tree.NewDString("|"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("test"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test string_agg bytes with null values
	// -------------------------------------------------------------------------
	t.Run("string_agg_bytes_with_null", func(t *testing.T) {
		delimiterArgs := tree.Datums{tree.NewDBytes(";")}
		agg := newBytesConcatAggregate(nil, ctx, delimiterArgs)

		_ = agg.Add(context.Background(), tree.NewDBytes("1"), tree.NewDBytes(";"))
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDBytes(";"))
		_ = agg.Add(context.Background(), tree.NewDBytes("2"), tree.NewDBytes(";"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDBytes("1;2"), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with integer (returns decimal)
	// -------------------------------------------------------------------------
	t.Run("sum_int_basic", func(t *testing.T) {
		agg := newIntSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(3))

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("6")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with float
	// -------------------------------------------------------------------------
	t.Run("sum_float_basic", func(t *testing.T) {
		agg := newFloatSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(7.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with decimal
	// -------------------------------------------------------------------------
	t.Run("sum_decimal_basic", func(t *testing.T) {
		agg := newDecimalSumAggregate(nil, ctx, emptyArgs)
		d1, _ := tree.ParseDDecimal("1.1")
		d2, _ := tree.ParseDDecimal("2.2")
		d3, _ := tree.ParseDDecimal("3.3")

		_ = agg.Add(context.Background(), d1)
		_ = agg.Add(context.Background(), d2)
		_ = agg.Add(context.Background(), d3)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("6.6")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with interval
	// -------------------------------------------------------------------------
	t.Run("sum_interval_basic", func(t *testing.T) {
		agg := newIntervalSumAggregate(nil, ctx, emptyArgs)
		iv1 := tree.NewDInterval(duration.MakeDuration(1000000000, 0, 0), types.IntervalTypeMetadata{}) // 1s
		iv2 := tree.NewDInterval(duration.MakeDuration(2000000000, 0, 0), types.IntervalTypeMetadata{}) // 2s

		_ = agg.Add(context.Background(), iv1)
		_ = agg.Add(context.Background(), iv2)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected := tree.NewDInterval(duration.MakeDuration(3000000000, 0, 0), types.IntervalTypeMetadata{}) // 3s
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("sum_int_with_null", func(t *testing.T) {
		agg := newIntSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(5))
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(5))

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("10")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with empty input returns NULL
	// -------------------------------------------------------------------------
	t.Run("sum_empty_returns_null", func(t *testing.T) {
		agg := newIntSumAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum with single value
	// -------------------------------------------------------------------------
	t.Run("sum_int_single_value", func(t *testing.T) {
		agg := newIntSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(100))

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("100")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sum float with null values
	// -------------------------------------------------------------------------
	t.Run("sum_float_with_null", func(t *testing.T) {
		agg := newFloatSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(5.0), res)
		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test sum / sum_int
	// --------------------------------------------------------------------------------
	t.Run("sum_int", func(t *testing.T) {
		agg := newIntSumAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		res, _ := agg.Result()
		assert.NotNil(t, res)
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff with float
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_float_basic", func(t *testing.T) {
		agg := newFloatSqrDiffAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0))

		res, err := agg.Result()
		assert.NoError(t, err)
		// (1-2)² + (2-2)² + (3-2)² = 1 + 0 + 1 = 2.0
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff with int (returns decimal)
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_int_basic", func(t *testing.T) {
		agg := newIntSqrDiffAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(3))

		res, err := agg.Result()
		assert.NoError(t, err)
		expected, _ := tree.ParseDDecimal("2")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff with decimal
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_decimal_basic", func(t *testing.T) {
		agg := newDecimalSqrDiffAggregate(nil, ctx, emptyArgs)
		d1, _ := tree.ParseDDecimal("1")
		d2, _ := tree.ParseDDecimal("2")
		d3, _ := tree.ParseDDecimal("3")

		_ = agg.Add(context.Background(), d1)
		_ = agg.Add(context.Background(), d2)
		_ = agg.Add(context.Background(), d3)

		res, err := agg.Result()
		assert.NoError(t, err)
		expected, _ := tree.ParseDDecimal("2")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_float_with_null", func(t *testing.T) {
		agg := newFloatSqrDiffAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1.0))
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff with single value returns 0
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_float_single_value_zero", func(t *testing.T) {
		agg := newFloatSqrDiffAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(5.0))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test sqrdiff with empty input returns NULL
	// -------------------------------------------------------------------------
	t.Run("sqrdiff_empty_returns_null", func(t *testing.T) {
		agg := newFloatSqrDiffAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// Test time values
	ts := tree.MakeDTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), time.Microsecond)
	tstz := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), time.Microsecond)

	// Correct interval format you provided
	//intervalVal := tree.NewDInterval(duration.MakeDuration(3600000000000, 0, 0), types.IntervalTypeMetadata{}) // 1 hour
	intWidth := tree.NewDInt(3600) // 1 hour in seconds

	// -------------------------------------------------------------------------
	// Test with Timestamp + String interval
	// -------------------------------------------------------------------------
	t.Run("time_bucket_timestamp_string", func(t *testing.T) {
		agg := newTimeBucketAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), ts, tree.NewDString("1h"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with TimestampTZ + String interval
	// -------------------------------------------------------------------------
	t.Run("time_bucket_timestamptz_string", func(t *testing.T) {
		agg := newTimestamptzBucketAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tstz, tree.NewDString("1h"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with Timestamp + Int width
	// -------------------------------------------------------------------------
	t.Run("time_bucket_timestamp_int", func(t *testing.T) {
		agg := newTimeBucketAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), ts, intWidth)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with TimestampTZ + Int width
	// -------------------------------------------------------------------------
	t.Run("time_bucket_timestamptz_int", func(t *testing.T) {
		agg := newTimestamptzBucketAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tstz, intWidth)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with NULL values
	// -------------------------------------------------------------------------
	t.Run("time_bucket_with_null", func(t *testing.T) {
		agg := newTimeBucketAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, tree.NewDString("1h"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test empty input returns null
	// -------------------------------------------------------------------------
	t.Run("time_bucket_empty_returns_null", func(t *testing.T) {
		agg := newTimeBucketAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.MakeDTimestamp(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), time.Microsecond), res)
		agg.Close(context.Background())
	})

	// Test with time unit (interval)
	timeUnitArgs := tree.Datums{
		tree.NewDInterval(duration.MakeDuration(1000000000, 0, 0), types.IntervalTypeMetadata{}),
	}

	// Test timestamps
	elapsedTS1 := tree.MakeDTimestamp(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), time.Microsecond)
	elapsedTS2 := tree.MakeDTimestamp(time.Date(2025, 1, 1, 12, 0, 10, 0, time.UTC), time.Microsecond)
	tstz = tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), time.Microsecond)
	tstz2 := tree.MakeDTimestampTZ(time.Date(2025, 1, 1, 12, 0, 10, 0, time.UTC), time.Microsecond)

	// -------------------------------------------------------------------------
	// Test elapsed with Timestamp + Interval
	// -------------------------------------------------------------------------
	t.Run("elapsed_timestamp_interval", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.Timestamp, types.Interval}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), elapsedTS1, timeUnitArgs[0])
		_ = agg.Add(context.Background(), elapsedTS2, timeUnitArgs[0])

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with TimestampTZ + Interval
	// -------------------------------------------------------------------------
	t.Run("elapsed_timestamptz_interval", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.TimestampTZ, types.Interval}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), tstz, timeUnitArgs[0])
		_ = agg.Add(context.Background(), tstz2, timeUnitArgs[0])

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with Timestamp only
	// -------------------------------------------------------------------------
	t.Run("elapsed_timestamp_only", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.Timestamp}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), elapsedTS1)
		_ = agg.Add(context.Background(), elapsedTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with TimestampTZ only
	// -------------------------------------------------------------------------
	t.Run("elapsed_timestamptz_only", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.TimestampTZ}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), tstz)
		_ = agg.Add(context.Background(), tstz2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with NULL values
	// -------------------------------------------------------------------------
	t.Run("elapsed_with_null_values", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.Timestamp}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), elapsedTS2)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with empty input returns NULL
	// -------------------------------------------------------------------------
	t.Run("elapsed_empty_returns_null", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.Timestamp}, ctx, timeUnitArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test elapsed with single value returns 0
	// -------------------------------------------------------------------------
	t.Run("elapsed_single_value_returns_zero", func(t *testing.T) {
		agg := newElapsedAggregate([]*types.T{types.Timestamp}, ctx, timeUnitArgs)
		_ = agg.Add(context.Background(), elapsedTS1)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.0), res)
		agg.Close(context.Background())
	})

	// Test data values
	intVal = tree.NewDInt(10)
	floatVal = tree.NewDFloat(20.5)

	// -------------------------------------------------------------------------
	// Test twa with Timestamp + Int
	// -------------------------------------------------------------------------
	t.Run("twa_timestamp_int", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.Timestamp, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), elapsedTS1, intVal)
		_ = agg.Add(context.Background(), elapsedTS2, intVal)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa with Timestamp + Float
	// -------------------------------------------------------------------------
	t.Run("twa_timestamp_float", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.Timestamp, types.Float}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), elapsedTS1, floatVal)
		_ = agg.Add(context.Background(), elapsedTS2, floatVal)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa with TimestampTZ + Int
	// -------------------------------------------------------------------------
	t.Run("twa_timestamptz_int", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.TimestampTZ, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tstz, intVal)
		_ = agg.Add(context.Background(), tstz2, intVal)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa with TimestampTZ + Float
	// -------------------------------------------------------------------------
	t.Run("twa_timestamptz_float", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.TimestampTZ, types.Float}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tstz, floatVal)
		_ = agg.Add(context.Background(), tstz2, floatVal)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa ignores NULL values
	// -------------------------------------------------------------------------
	t.Run("twa_with_null_values", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.Timestamp, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, intVal)
		_ = agg.Add(context.Background(), elapsedTS1, intVal)
		_ = agg.Add(context.Background(), elapsedTS2, tree.DNull)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotEqual(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa with single value returns NULL
	// -------------------------------------------------------------------------
	t.Run("twa_single_value_returns_null", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.Timestamp, types.Int}, ctx, emptyArgs)
		_ = agg.Add(context.Background(), elapsedTS1, intVal)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(10.0), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test twa with empty input returns NULL
	// -------------------------------------------------------------------------
	t.Run("twa_empty_returns_null", func(t *testing.T) {
		agg := newTwaAggregate([]*types.T{types.Timestamp, types.Int}, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// Basic test values
	intVal = tree.NewDInt(10)
	floatVal = tree.NewDFloat(10.5)
	decVal, _ = tree.ParseDDecimal("5.5")
	strPrev := tree.NewDString("prev")
	strLinear := tree.NewDString("linear")

	// -------------------------------------------------------------------------
	// 1. Int + Int (CONSTANT INT METHOD)
	// -------------------------------------------------------------------------
	t.Run("interpolate_int_int", func(t *testing.T) {
		// 1. 创建
		agg := newImputationAggregate(nil, ctx, nil).(*ImputationAggregate)

		// 2. 【关键】手动给 Aggfunc 赋一个空实现，避免 nil panic
		agg.Aggfunc = &mockEmptyAggregate{}

		// 3. 调用 Add（安全）
		err := agg.Add(context.Background(), intVal, intVal)
		assert.NoError(t, err)
		assert.Equal(t, ConstantIntMethod, agg.Exp)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// 2. Float + String (PREV METHOD)
	// -------------------------------------------------------------------------
	t.Run("interpolate_float_string_prev", func(t *testing.T) {
		agg := newImputationAggregate(nil, ctx, nil).(*ImputationAggregate)
		agg.Aggfunc = &mockEmptyAggregate{}

		err := agg.Add(context.Background(), floatVal, strPrev)
		assert.NoError(t, err)
		assert.Equal(t, PrevMethod, agg.Exp)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// 3. Float + String (LINEAR METHOD)
	// -------------------------------------------------------------------------
	t.Run("interpolate_float_string_linear", func(t *testing.T) {
		agg := newImputationAggregate(nil, ctx, nil).(*ImputationAggregate)
		agg.Aggfunc = &mockEmptyAggregate{}

		err := agg.Add(context.Background(), floatVal, strLinear)
		assert.NoError(t, err)
		assert.Equal(t, LinearMethod, agg.Exp)

		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// 5. 空值测试
	// -------------------------------------------------------------------------
	t.Run("interpolate_null_safe", func(t *testing.T) {
		agg := newImputationAggregate(nil, ctx, nil).(*ImputationAggregate)
		agg.Aggfunc = &mockEmptyAggregate{}

		err := agg.Add(context.Background(), tree.DNull, strPrev)
		assert.NoError(t, err)

		agg.Close(context.Background())
	})

	// Test values
	floatVal = tree.NewDFloat(1.0)
	decVal, _ = tree.ParseDDecimal("1.0")

	// -------------------------------------------------------------------------
	// Test final_variance with float
	// -------------------------------------------------------------------------
	t.Run("final_variance_float", func(t *testing.T) {
		agg := newFloatFinalVarianceAggregate(nil, ctx, emptyArgs)
		// 安全调用 Add，不调用 Result
		_ = agg.Add(context.Background(), floatVal, floatVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.1111111111111111), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test final_variance with decimal
	// -------------------------------------------------------------------------
	t.Run("final_variance_decimal", func(t *testing.T) {
		agg := newDecimalFinalVarianceAggregate(nil, ctx, emptyArgs)
		// 安全调用 Add，不调用 Result
		_ = agg.Add(context.Background(), decVal, decVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		dec, _ := tree.ParseDDecimal("0.11111111111111111111")
		assert.Equal(t, dec, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with NULL values
	// -------------------------------------------------------------------------
	t.Run("final_variance_with_null", func(t *testing.T) {
		agg := newFloatFinalVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, floatVal, intVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test empty
	// -------------------------------------------------------------------------
	t.Run("final_variance_empty", func(t *testing.T) {
		agg := newDecimalFinalVarianceAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// Test values
	floatVal = tree.NewDFloat(2.0)
	decVal, _ = tree.ParseDDecimal("2.0")
	countVal := tree.NewDInt(5)

	// -------------------------------------------------------------------------
	// Test final_stddev with float type
	// -------------------------------------------------------------------------
	t.Run("final_stddev_float", func(t *testing.T) {
		agg := newFloatFinalStdDevAggregate(nil, ctx, emptyArgs)
		// Safe Add call, no Result()
		_ = agg.Add(context.Background(), floatVal, floatVal, countVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(0.7071067811865476), res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test final_stddev with decimal type
	// -------------------------------------------------------------------------
	t.Run("final_stddev_decimal", func(t *testing.T) {
		agg := newDecimalFinalStdDevAggregate(nil, ctx, emptyArgs)
		// Safe Add call, no Result()
		_ = agg.Add(context.Background(), decVal, decVal, countVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		dec, _ := tree.ParseDDecimal("0.70710678118654752440")
		assert.Equal(t, dec, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test with NULL values
	// -------------------------------------------------------------------------
	t.Run("final_stddev_with_null", func(t *testing.T) {
		agg := newFloatFinalStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull, floatVal, countVal)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// Test empty input
	// -------------------------------------------------------------------------
	t.Run("final_stddev_empty", func(t *testing.T) {
		agg := newDecimalFinalStdDevAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test variance / stddev
	// --------------------------------------------------------------------------------
	t.Run("variance_stddev_basic", func(t *testing.T) {
		agg := newFloatVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDFloat(1))
		_ = agg.Add(context.Background(), tree.NewDFloat(2))
		_ = agg.Add(context.Background(), tree.NewDFloat(3))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	// Test values
	intVal = tree.NewDInt(5)
	floatVal = tree.NewDFloat(5.0)
	decVal, _ = tree.ParseDDecimal("5.0")
	tenInt := tree.NewDInt(10)
	tenFloat := tree.NewDFloat(10.0)
	tenDec, _ := tree.ParseDDecimal("10.0")

	// -------------------------------------------------------------------------
	// variance (sample variance)
	// -------------------------------------------------------------------------

	t.Run("variance_int", func(t *testing.T) {
		agg := newIntVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), intVal)
		_ = agg.Add(context.Background(), tenInt)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("12.5")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("variance_float", func(t *testing.T) {
		agg := newFloatVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(12.5), res)
		agg.Close(context.Background())
	})

	t.Run("variance_decimal", func(t *testing.T) {
		agg := newDecimalVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), decVal)
		_ = agg.Add(context.Background(), tenDec)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("12.5")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("variance_with_null", func(t *testing.T) {
		agg := newFloatVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(12.5), res)
		agg.Close(context.Background())
	})

	t.Run("variance_empty", func(t *testing.T) {
		agg := newIntVarianceAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// var_pop (population variance)
	// -------------------------------------------------------------------------

	t.Run("var_pop_int", func(t *testing.T) {
		agg := newIntPopVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), intVal)
		_ = agg.Add(context.Background(), tenInt)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("6.25")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("var_pop_float", func(t *testing.T) {
		agg := newFloatPopVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(6.25), res)
		agg.Close(context.Background())
	})

	t.Run("var_pop_decimal", func(t *testing.T) {
		agg := newDecimalPopVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), decVal)
		_ = agg.Add(context.Background(), tenDec)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("6.25")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("var_pop_with_null", func(t *testing.T) {
		agg := newFloatPopVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(6.25), res)
		agg.Close(context.Background())
	})

	t.Run("var_pop_empty", func(t *testing.T) {
		agg := newIntPopVarianceAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// var_samp (sample variance, same as variance)
	// -------------------------------------------------------------------------
	t.Run("var_samp_int", func(t *testing.T) {
		agg := newIntVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), intVal)
		_ = agg.Add(context.Background(), tenInt)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("12.5")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("var_samp_float", func(t *testing.T) {
		agg := newFloatVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(12.5), res)
		agg.Close(context.Background())
	})

	t.Run("var_samp_decimal", func(t *testing.T) {
		agg := newDecimalVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), decVal)
		_ = agg.Add(context.Background(), tenDec)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("12.5")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("var_samp_with_null", func(t *testing.T) {
		agg := newFloatVarianceAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(12.5), res)
		agg.Close(context.Background())
	})

	t.Run("var_samp_empty", func(t *testing.T) {
		agg := newIntVarianceAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// stddev / stddev_samp (sample standard deviation)
	// -------------------------------------------------------------------------
	t.Run("stddev_int", func(t *testing.T) {
		agg := newIntStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), intVal)
		_ = agg.Add(context.Background(), tenInt)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3.5355339059327376220")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("stddev_float", func(t *testing.T) {
		agg := newFloatStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(3.5355339059327378), res)
		agg.Close(context.Background())
	})

	t.Run("stddev_decimal", func(t *testing.T) {
		agg := newDecimalStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), decVal)
		_ = agg.Add(context.Background(), tenDec)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3.5355339059327376220")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	t.Run("stddev_with_null", func(t *testing.T) {
		agg := newFloatStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), floatVal)
		_ = agg.Add(context.Background(), tenFloat)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(3.5355339059327378), res)
		agg.Close(context.Background())
	})

	t.Run("stddev_empty", func(t *testing.T) {
		agg := newIntStdDevAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// stddev_samp uses the same implementation
	t.Run("stddev_samp_int", func(t *testing.T) {
		agg := newIntStdDevAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), intVal)
		_ = agg.Add(context.Background(), tenInt)

		res, err := agg.Result()
		assert.NoError(t, err)

		expected, _ := tree.ParseDDecimal("3.5355339059327376220")
		assert.Equal(t, expected, res)
		agg.Close(context.Background())
	})

	// -------------------------------------------------------------------------
	// xor_agg
	// -------------------------------------------------------------------------
	t.Run("xor_agg_int", func(t *testing.T) {
		agg := newIntXorAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))
		_ = agg.Add(context.Background(), tree.NewDInt(3))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(0), res)
		agg.Close(context.Background())
	})

	t.Run("xor_agg_int_null", func(t *testing.T) {
		agg := newIntXorAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDInt(2))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(3), res)
		agg.Close(context.Background())
	})

	t.Run("xor_agg_int_empty", func(t *testing.T) {
		agg := newIntXorAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("xor_agg_bytes", func(t *testing.T) {
		agg := newBytesXorAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDBytes(tree.DBytes([]byte{0x01})))
		_ = agg.Add(context.Background(), tree.NewDBytes(tree.DBytes([]byte{0x02})))
		_ = agg.Add(context.Background(), tree.NewDBytes(tree.DBytes([]byte{0x03})))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDBytes(tree.DBytes([]byte{0x00})), res)
		agg.Close(context.Background())
	})

	t.Run("xor_agg_bytes_null", func(t *testing.T) {
		agg := newBytesXorAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDBytes(tree.DBytes([]byte{0x01})))
		_ = agg.Add(context.Background(), tree.NewDBytes(tree.DBytes([]byte{0x02})))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDBytes(tree.DBytes([]byte{0x03})), res)
		agg.Close(context.Background())
	})

	t.Run("xor_agg_bytes_empty", func(t *testing.T) {
		agg := newBytesXorAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	// --------------------------------------------------------------------------------
	// Test json_agg / jsonb_agg
	// --------------------------------------------------------------------------------
	t.Run("json_agg", func(t *testing.T) {
		agg := newJSONAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.NewDInt(1))
		_ = agg.Add(context.Background(), tree.NewDString("test"))
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	// -------------------------------------------------------------------------
	// quantile aggregate function test
	// Calculates the specified quantile over a set of values
	// -------------------------------------------------------------------------
	t.Run("quantile_basic_float_values", func(t *testing.T) {
		agg := newQuantileAggregate(nil, ctx, emptyArgs).(*quantileAggregate)
		// Set quantile to 0.5 (median)
		agg.quantile = 0.5
		agg.quantileSet = true

		_ = agg.Add(context.Background(), tree.NewDFloat(1.0), tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(2.0), tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(3.0), tree.NewDFloat(0.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})

	t.Run("quantile_int4_values", func(t *testing.T) {
		agg := newQuantileAggregate(nil, ctx, emptyArgs).(*quantileAggregate)
		agg.quantile = 0.5
		agg.quantileSet = true

		_ = agg.Add(context.Background(), tree.NewDInt(1), tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDInt(2), tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDInt(3), tree.NewDFloat(0.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})

	t.Run("quantile_with_null_values", func(t *testing.T) {
		agg := newQuantileAggregate(nil, ctx, emptyArgs).(*quantileAggregate)
		agg.quantile = 0.5
		agg.quantileSet = true

		_ = agg.Add(context.Background(), tree.DNull, tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(10.0), tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), tree.NewDFloat(20.0), tree.NewDFloat(0.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(15.0), res)
		agg.Close(context.Background())
	})

	t.Run("quantile_empty_input_returns_null", func(t *testing.T) {
		agg := newQuantileAggregate(nil, ctx, emptyArgs).(*quantileAggregate)
		agg.quantile = 0.5
		agg.quantileSet = true

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("quantile_decimal_values", func(t *testing.T) {
		agg := newQuantileAggregate(nil, ctx, emptyArgs).(*quantileAggregate)
		agg.quantile = 0.5
		agg.quantileSet = true

		dec1, _ := tree.ParseDDecimal("1.0")
		dec2, _ := tree.ParseDDecimal("2.0")
		dec3, _ := tree.ParseDDecimal("3.0")

		_ = agg.Add(context.Background(), dec1, tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), dec2, tree.NewDFloat(0.5))
		_ = agg.Add(context.Background(), dec3, tree.NewDFloat(0.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(2.0), res)
		agg.Close(context.Background())
	})
	// -------------------------------------------------------------------------
	// AnyNotNull aggregate function test
	// Returns any not null value from input values
	// -------------------------------------------------------------------------
	t.Run("AnyNotNull_not_null_value", func(t *testing.T) {
		agg := newAnyNotNullAggregate(nil, ctx, emptyArgs)
		// Add a null value first
		_ = agg.Add(context.Background(), tree.DNull)
		// Add valid integer value
		_ = agg.Add(context.Background(), tree.NewDInt(100))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDInt(100), res)
		agg.Close(context.Background())
	})

	t.Run("AnyNotNull_all_null", func(t *testing.T) {
		agg := newAnyNotNullAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.DNull)

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("AnyNotNull_empty_input", func(t *testing.T) {
		agg := newAnyNotNullAggregate(nil, ctx, emptyArgs)
		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
		agg.Close(context.Background())
	})

	t.Run("AnyNotNull_string_value", func(t *testing.T) {
		agg := newAnyNotNullAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDString("test_value"))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("test_value"), res)
		agg.Close(context.Background())
	})

	t.Run("AnyNotNull_float_value", func(t *testing.T) {
		agg := newAnyNotNullAggregate(nil, ctx, emptyArgs)
		_ = agg.Add(context.Background(), tree.DNull)
		_ = agg.Add(context.Background(), tree.NewDFloat(18.5))

		res, err := agg.Result()
		assert.NoError(t, err)
		assert.Equal(t, tree.NewDFloat(18.5), res)
		agg.Close(context.Background())
	})
}

// ------------------------------
// 【必备】空 mock 聚合类，防止 nil 调用
// ------------------------------
type mockEmptyAggregate struct{}

func (m *mockEmptyAggregate) Add(_ context.Context, _ tree.Datum, _ ...tree.Datum) error { return nil }
func (m *mockEmptyAggregate) Result() (tree.Datum, error)                                { return tree.DNull, nil }

// Reset implements tree.AggregateFunc interface.
func (m *mockEmptyAggregate) Reset(ctx context.Context) {}

// Close is part of the tree.AggregateFunc interface.
func (m *mockEmptyAggregate) Close(ctx context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (m *mockEmptyAggregate) Size() int64 {
	return 0
}

// AggHandling is part of the tree.AggregateFunc interface.
func (m *mockEmptyAggregate) AggHandling() {
	// do nothing
}
