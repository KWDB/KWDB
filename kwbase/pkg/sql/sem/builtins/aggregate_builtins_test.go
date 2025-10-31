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

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
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
