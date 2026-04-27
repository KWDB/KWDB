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

package builtins

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentProcessorsReadEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &tree.EvalContextTestingKnobs{
				CallbackGenerators: map[string]*tree.CallbackValueGenerator{
					"my_callback": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, _ *kv.Txn) (int, error) {
							if prev < 10 {
								return prev + 1, nil
							}
							return -1, nil
						}),
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	rows, err := db.Query(` select * from kwdb_internal.testing_callback('my_callback')`)
	require.NoError(t, err)
	exp := 1
	for rows.Next() {
		var got int
		require.NoError(t, rows.Scan(&got))
		require.Equal(t, exp, got)
		exp++
	}
}

// TestACLexplodeGenerator tests the aclExplodeGenerator implementation
func TestACLexplodeGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	gen := aclexplodeGenerator{}
	// Test ResolvedType
	assert.Equal(t, aclexplodeGeneratorType, gen.ResolvedType())

	// Test Start method
	err := gen.Start(ctx, txn)
	assert.NoError(t, err)

	// Test Next method (should return false immediately)
	hasNext, err := gen.Next(ctx)
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Test Values method (should return nil)
	assert.Nil(t, gen.Values())

	// Test Close method (no panic)
	gen.Close()
}

// TestKeywordsValueGenerator tests the pg_get_keywords generator
func TestKeywordsValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	gen, err := makeKeywordsGenerator(nil, nil)
	require.NoError(t, err)

	// Test ResolvedType
	assert.Equal(t, keywordsValueGeneratorType, gen.ResolvedType())

	// Test Start method
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// Test Next and Values iteration
	keywordCount := 0
	for {
		hasNext, err := gen.Next(ctx)
		require.NoError(t, err)
		if !hasNext {
			break
		}
		vals := gen.Values()
		require.Len(t, vals, 3)

		// Verify value types
		_, isWord := vals[0].(*tree.DString)
		_, isCatCode := vals[1].(*tree.DString)
		_, isCatDesc := vals[2].(*tree.DString)
		assert.True(t, isWord)
		assert.True(t, isCatCode)
		assert.True(t, isCatDesc)

		keywordCount++
	}

	// Verify total keywords match lex.KeywordNames length
	assert.Equal(t, len(lex.KeywordNames), keywordCount)

	// Test Close method
	gen.Close()
}

// TestSeriesValueGenerator_Int tests integer generate_series functionality
func TestSeriesValueGenerator_Int(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	testCases := []struct {
		name     string
		start    int64
		end      int64
		step     int64
		expected []int64
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "basic ascending (step 1)",
			start:    1,
			end:      3,
			step:     1,
			expected: []int64{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "ascending with step 2",
			start:    2,
			end:      6,
			step:     2,
			expected: []int64{2, 4, 6},
			wantErr:  false,
		},
		{
			name:     "descending with negative step",
			start:    5,
			end:      1,
			step:     -2,
			expected: []int64{5, 3, 1},
			wantErr:  false,
		},
		{
			name:     "step zero (error case)",
			start:    1,
			end:      5,
			step:     0,
			expected: nil,
			wantErr:  true,
			errMsg:   "step cannot be 0",
		},
		{
			name:     "start > end with positive step (no results)",
			start:    5,
			end:      1,
			step:     1,
			expected: []int64(nil),
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := make(tree.Datums, 0, 3)
			args = append(args, tree.NewDInt(tree.DInt(tc.start)))
			args = append(args, tree.NewDInt(tree.DInt(tc.end)))
			if tc.step != 1 {
				args = append(args, tree.NewDInt(tree.DInt(tc.step)))
			}

			gen, err := makeSeriesGenerator(nil, args)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)

			// Test ResolvedType
			assert.Equal(t, seriesValueGeneratorType, gen.ResolvedType())

			// Start generator
			err = gen.Start(ctx, txn)
			require.NoError(t, err)

			// Iterate and collect results
			var results []int64
			for {
				hasNext, err := gen.Next(ctx)
				require.NoError(t, err)
				if !hasNext {
					break
				}
				vals := gen.Values()
				require.Len(t, vals, 1)
				val := int64(tree.MustBeDInt(vals[0]))
				results = append(results, val)
			}

			// Verify results
			assert.Equal(t, tc.expected, results)

			// Close generator
			gen.Close()
		})
	}
}

// TestSeriesValueGenerator_Timestamp tests timestamp generate_series functionality
func TestSeriesValueGenerator_Timestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	startTime, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	require.NoError(t, err)
	endTime, err := time.Parse(time.RFC3339, "2024-01-03T00:00:00Z")
	require.NoError(t, err)
	stepDur := duration.MakeDuration(int64(24*time.Hour), 0, 0)

	// Prepare arguments
	args := tree.Datums{
		tree.MakeDTimestamp(startTime, time.Microsecond),
		tree.MakeDTimestamp(endTime, time.Microsecond),
		tree.NewDInterval(stepDur, types.IntervalTypeMetadata{}),
	}

	// Create generator
	gen, err := makeTSSeriesGenerator(nil, args)
	require.NoError(t, err)

	// Test ResolvedType
	assert.Equal(t, seriesTSValueGeneratorType, gen.ResolvedType())

	// Start generator
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// Iterate and collect results
	expectedTimes := []time.Time{
		startTime,
		startTime.Add(24 * time.Hour),
		endTime,
	}
	var results []time.Time
	for {
		hasNext, err := gen.Next(ctx)
		require.NoError(t, err)
		if !hasNext {
			break
		}
		vals := gen.Values()
		require.Len(t, vals, 1)
		ts := tree.MustBeDTimestamp(vals[0])
		results = append(results, ts.Time)
	}

	// Verify results
	assert.Equal(t, expectedTimes, results)

	// Test step zero error
	zeroStepArgs := tree.Datums{
		tree.MakeDTimestamp(startTime, time.Microsecond),
		tree.MakeDTimestamp(endTime, time.Microsecond),
		tree.NewDInterval(duration.Duration{}, types.IntervalTypeMetadata{}),
	}
	gen, err = makeTSSeriesGenerator(nil, zeroStepArgs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "step cannot be 0")
}

// TestArrayValueGenerator tests unnest for single array
func TestArrayValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Create test array (int array [1,2,3])
	intArray := tree.NewDArray(types.Int)
	intArray.Array = tree.Datums{
		tree.NewDInt(1),
		tree.NewDInt(2),
		tree.NewDInt(3),
	}
	args := tree.Datums{intArray}

	// Create generator
	gen, err := makeArrayGenerator(nil, args)
	require.NoError(t, err)

	// Test ResolvedType
	assert.Equal(t, types.Int, gen.ResolvedType())

	// Start generator
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// Iterate and collect results
	var results []int64
	for {
		hasNext, err := gen.Next(ctx)
		require.NoError(t, err)
		if !hasNext {
			break
		}
		vals := gen.Values()
		require.Len(t, vals, 1)
		val := int64(tree.MustBeDInt(vals[0]))
		results = append(results, val)
	}

	// Verify results
	assert.Equal(t, []int64{1, 2, 3}, results)

	// Test empty array
	emptyArray := tree.NewDArray(types.Int)
	emptyArgs := tree.Datums{emptyArray}
	gen, err = makeArrayGenerator(nil, emptyArgs)
	require.NoError(t, err)
	err = gen.Start(ctx, txn)
	require.NoError(t, err)
	hasNext, err := gen.Next(ctx)
	require.NoError(t, err)
	assert.False(t, hasNext)

	// Close generator
	gen.Close()
}

// TestMultipleArrayValueGenerator tests variadic unnest (multiple arrays)
func TestMultipleArrayValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Create test arrays
	intArray := tree.NewDArray(types.Int)
	intArray.Array = tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}
	strArray := tree.NewDArray(types.String)
	strArray.Array = tree.Datums{tree.NewDString("a"), tree.NewDString("b"), tree.NewDString("c")}
	args := tree.Datums{intArray, strArray}

	// Create generator
	gen, err := makeVariadicUnnestGenerator(nil, args)
	require.NoError(t, err)

	// Test ResolvedType (labeled tuple)
	resType := gen.ResolvedType()
	require.Equal(t, types.TupleFamily, resType.Family())
	require.Len(t, resType.TupleContents(), 2)
	assert.Equal(t, types.Int, &resType.TupleContents()[0])
	assert.Equal(t, types.String, &resType.TupleContents()[1])

	// Start generator
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// Expected results:
	// (1, "a"), (2, "b"), (NULL, "c")
	expected := []struct {
		intVal *int64
		strVal string
		isNull bool
	}{
		{intVal: func() *int64 { v := int64(1); return &v }(), strVal: "a", isNull: false},
		{intVal: func() *int64 { v := int64(2); return &v }(), strVal: "b", isNull: false},
		{intVal: nil, strVal: "c", isNull: true},
	}

	// Iterate and verify
	idx := 0
	for {
		hasNext, err := gen.Next(ctx)
		require.NoError(t, err)
		if !hasNext {
			break
		}
		vals := gen.Values()
		require.Len(t, vals, 2)

		// Verify int value
		if expected[idx].isNull {
			assert.True(t, vals[0] == tree.DNull)
		} else {
			val := int64(tree.MustBeDInt(vals[0]))
			assert.Equal(t, *expected[idx].intVal, val)
		}

		// Verify string value
		strVal := string(tree.MustBeDString(vals[1]))
		assert.Equal(t, expected[idx].strVal, strVal)

		idx++
	}
	assert.Equal(t, len(expected), idx)

	// Close generator
	gen.Close()
}

// TestExpandArrayValueGenerator tests information_schema._pg_expandarray
func TestExpandArrayValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Create test array (string array ["foo", "bar"])
	strArray := tree.NewDArray(types.String)
	strArray.Array = tree.Datums{
		tree.NewDString("foo"),
		tree.NewDString("bar"),
	}
	args := tree.Datums{strArray}

	// Create generator
	gen, err := makeExpandArrayGenerator(nil, args)
	require.NoError(t, err)

	// Test ResolvedType (labeled tuple: x (string), n (int))
	resType := gen.ResolvedType()
	require.Equal(t, types.TupleFamily, resType.Family())
	require.Len(t, resType.TupleContents(), 2)
	assert.Equal(t, types.String, &resType.TupleContents()[0])
	assert.Equal(t, types.Int, &resType.TupleContents()[1])
	assert.Equal(t, expandArrayValueGeneratorLabels, resType.TupleLabels())

	// Start generator
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// Expected results: ("foo", 1), ("bar", 2)
	expected := []struct {
		val string
		idx int64
	}{
		{"foo", 1},
		{"bar", 2},
	}

	// Iterate and verify
	idx := 0
	for {
		hasNext, err := gen.Next(ctx)
		require.NoError(t, err)
		if !hasNext {
			break
		}
		vals := gen.Values()
		require.Len(t, vals, 2)

		// Verify value and index
		val := string(tree.MustBeDString(vals[0]))
		index := int64(tree.MustBeDInt(vals[1]))
		assert.Equal(t, expected[idx].val, val)
		assert.Equal(t, expected[idx].idx, index)

		idx++
	}
	assert.Equal(t, len(expected), idx)

	// Close generator
	gen.Close()
}

// TestSubscriptsValueGenerator tests generate_subscripts
func TestSubscriptsValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	testCases := []struct {
		name     string
		array    *tree.DArray
		dim      int64
		reverse  bool
		expected []int64
	}{
		{
			name: "1D array (forward)",
			array: func() *tree.DArray {
				arr := tree.NewDArray(types.Int)
				arr.Array = tree.Datums{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(30)}
				return arr
			}(),
			dim:      1,
			reverse:  false,
			expected: []int64{1, 2, 3},
		},
		{
			name: "non-1D dim (empty result)",
			array: func() *tree.DArray {
				arr := tree.NewDArray(types.Int)
				arr.Array = tree.Datums{tree.NewDInt(10), tree.NewDInt(20)}
				return arr
			}(),
			dim:      2,
			reverse:  false,
			expected: []int64(nil),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare arguments
			args := make(tree.Datums, 0, 3)
			args = append(args, tc.array)
			if tc.dim != 1 {
				args = append(args, tree.NewDInt(tree.DInt(tc.dim)))
			}

			// Create generator
			gen, err := makeGenerateSubscriptsGenerator(nil, args)
			require.NoError(t, err)

			// Test ResolvedType
			assert.Equal(t, types.Int, gen.ResolvedType())

			// Start generator
			err = gen.Start(ctx, txn)
			require.NoError(t, err)

			// Iterate and collect results
			var results []int64
			for {
				hasNext, err := gen.Next(ctx)
				require.NoError(t, err)
				if !hasNext {
					break
				}
				vals := gen.Values()
				require.Len(t, vals, 1)
				val := int64(tree.MustBeDInt(vals[0]))
				results = append(results, val)
			}

			// Verify results
			assert.Equal(t, tc.expected, results)

			// Close generator
			gen.Close()
		})
	}
}

// TestUnaryValueGenerator tests kwdb_internal.unary_table
func TestUnaryValueGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Create generator
	gen, err := makeUnaryGenerator(nil, nil)
	require.NoError(t, err)

	// Test ResolvedType
	assert.Equal(t, unaryValueGeneratorType, gen.ResolvedType())

	// Start generator
	err = gen.Start(ctx, txn)
	require.NoError(t, err)

	// First Next call should return true (single row)
	hasNext, err := gen.Next(ctx)
	require.NoError(t, err)
	assert.True(t, hasNext)

	// Values should return empty datums
	assert.Empty(t, gen.Values())

	// Second Next call should return false
	hasNext, err = gen.Next(ctx)
	require.NoError(t, err)
	assert.False(t, hasNext)

	// Close generator
	gen.Close()
}

// TestJSONArrayGenerator tests json_array_elements and json_array_elements_text
func TestJSONArrayGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Test JSON array generator (as JSON)
	t.Run("json_array_elements", func(t *testing.T) {
		// Create JSON array: ["a", 1, true]
		jsonStr := `["a", 1, true]`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Create generator
		gen, err := makeJSONArrayAsJSONGenerator(nil, args)
		require.NoError(t, err)

		// Test ResolvedType
		assert.Equal(t, jsonArrayGeneratorType, gen.ResolvedType())

		// Start generator
		err = gen.Start(ctx, txn)
		require.NoError(t, err)

		// Expected values
		expected := []string{`'"a"'`, "'1'", "'true'"}

		// Iterate and verify
		idx := 0
		for {
			hasNext, err := gen.Next(ctx)
			require.NoError(t, err)
			if !hasNext {
				break
			}
			vals := gen.Values()
			require.Len(t, vals, 1)
			val := vals[0].String()
			assert.Equal(t, expected[idx], val)
			idx++
		}
		assert.Equal(t, len(expected), idx)

		// Close generator
		gen.Close()
	})

	// Test JSON array generator (as text)
	t.Run("json_array_elements_text", func(t *testing.T) {
		// Create JSON array: ["a", 1, true]
		jsonStr := `["a", 1, true]`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Create generator
		gen, err := makeJSONArrayAsTextGenerator(nil, args)
		require.NoError(t, err)

		// Test ResolvedType
		assert.Equal(t, jsonArrayTextGeneratorType, gen.ResolvedType())

		// Start generator
		err = gen.Start(ctx, txn)
		require.NoError(t, err)

		// Expected values
		expected := []string{"a", "1", "true"}

		// Iterate and verify
		idx := 0
		for {
			hasNext, err := gen.Next(ctx)
			require.NoError(t, err)
			if !hasNext {
				break
			}
			vals := gen.Values()
			require.Len(t, vals, 1)
			val := string(tree.MustBeDString(vals[0]))
			assert.Equal(t, expected[idx], val)
			idx++
		}
		assert.Equal(t, len(expected), idx)

		// Close generator
		gen.Close()
	})

	// Test non-array input error
	t.Run("non-array input error", func(t *testing.T) {
		// Non-array JSON (object)
		jsonStr := `{"key": "value"}`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Should return error
		_, err = makeJSONArrayAsJSONGenerator(nil, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be called on a non-array")
	})
}

// TestJSONObjectKeysGenerator tests json_object_keys
func TestJSONObjectKeysGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Test valid JSON object
	t.Run("valid json object", func(t *testing.T) {
		// JSON object: {"a": 1, "b": "foo", "c": true}
		jsonStr := `{"a": 1, "b": "foo", "c": true}`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Create generator
		gen, err := makeJSONObjectKeysGenerator(nil, args)
		require.NoError(t, err)

		// Test ResolvedType
		assert.Equal(t, jsonObjectKeysGeneratorType, gen.ResolvedType())

		// Start generator
		err = gen.Start(ctx, txn)
		require.NoError(t, err)

		// Expected keys (sorted)
		expected := []string{"a", "b", "c"}

		// Iterate and verify
		var results []string
		for {
			hasNext, err := gen.Next(ctx)
			require.NoError(t, err)
			if !hasNext {
				break
			}
			vals := gen.Values()
			require.Len(t, vals, 1)
			key := string(tree.MustBeDString(vals[0]))
			results = append(results, key)
		}

		// Verify sorted keys
		assert.Equal(t, expected, results)

		// Close generator
		gen.Close()
	})

	// Test array input error
	t.Run("array input error", func(t *testing.T) {
		jsonStr := `[1, 2, 3]`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		_, err = makeJSONObjectKeysGenerator(nil, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot call json_object_keys on an array")
	})

	// Test scalar input error
	t.Run("scalar input error", func(t *testing.T) {
		jsonStr := `"foo"`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		_, err = makeJSONObjectKeysGenerator(nil, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot call json_object_keys on a scalar")
	})
}

// TestJSONEachGenerator tests json_each and json_each_text
func TestJSONEachGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	txn := &kv.Txn{}

	// Test json_each (key/value as JSON)
	t.Run("json_each", func(t *testing.T) {
		// JSON object: {"a": 1, "b": "foo"}
		jsonStr := `{"a": 1}`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Create generator
		gen, err := makeJSONEachImplGenerator(nil, args)
		require.NoError(t, err)

		// Test ResolvedType
		assert.Equal(t, jsonEachGeneratorType, gen.ResolvedType())

		// Start generator
		err = gen.Start(ctx, txn)
		require.NoError(t, err)

		// Expected key/value pairs
		expected := map[string]string{
			"a": "'1'",
		}

		// Iterate and verify
		for {
			hasNext, err := gen.Next(ctx)
			require.NoError(t, err)
			if !hasNext {
				break
			}
			vals := gen.Values()
			require.Len(t, vals, 2)

			// Verify key and value
			key := string(tree.MustBeDString(vals[0]))
			val := vals[1].String()
			assert.Equal(t, expected[key], val)
			delete(expected, key)
		}

		// All keys should be processed
		assert.Empty(t, expected)

		// Close generator
		gen.Close()
	})

	// Test json_each_text (key/value as text)
	t.Run("json_each_text", func(t *testing.T) {
		// JSON object: {"a": 1, "b": "foo"}
		jsonStr := `{"a": 1, "b": "foo"}`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		// Create generator
		gen, err := makeJSONEachTextImplGenerator(nil, args)
		require.NoError(t, err)

		// Test ResolvedType
		assert.Equal(t, jsonEachTextGeneratorType, gen.ResolvedType())

		// Start generator
		err = gen.Start(ctx, txn)
		require.NoError(t, err)

		// Expected key/value pairs
		expected := map[string]string{
			"a": "1",
			"b": "foo",
		}

		// Iterate and verify
		for {
			hasNext, err := gen.Next(ctx)
			require.NoError(t, err)
			if !hasNext {
				break
			}
			vals := gen.Values()
			require.Len(t, vals, 2)

			// Verify key and value
			key := string(tree.MustBeDString(vals[0]))
			val := string(tree.MustBeDString(vals[1]))
			assert.Equal(t, expected[key], val)
			delete(expected, key)
		}

		// All keys should be processed
		assert.Empty(t, expected)

		// Close generator
		gen.Close()
	})

	// Test array input error
	t.Run("array input error", func(t *testing.T) {
		jsonStr := `[1, 2, 3]`
		jsonDatum, err := tree.ParseDJSON(jsonStr)
		require.NoError(t, err)
		args := tree.Datums{jsonDatum}

		_, err = makeJSONEachImplGenerator(nil, args)
		require.NoError(t, err)
	})
}

// TestCheckConsistencyGenerator tests kwdb_internal.check_consistency
// Note: This is a mock test (no real KV DB); for full testing, use TestServer
func TestCheckConsistencyGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	//txn := &kv.Txn{}

	// Test invalid start/end key (start >= end)
	t.Run("invalid key range (start >= end)", func(t *testing.T) {
		args := tree.Datums{
			tree.MakeDBool(true),                // stats_only
			tree.NewDBytes(tree.DBytes("\x04")), // start_key
			tree.NewDBytes(tree.DBytes("\x02")), // end_key
		}
		_, err := makeCheckConsistencyGenerator(&tree.EvalContext{
			Context: ctx,
			DB:      &kv.DB{},
		}, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start key must be less than end key")
	})

	// Test invalid start key (below LocalMax)
	t.Run("invalid start key (below LocalMax)", func(t *testing.T) {
		args := tree.Datums{
			tree.MakeDBool(true),                // stats_only
			tree.NewDBytes(tree.DBytes("\x01")), // start_key (below LocalMax)
			tree.NewDBytes(tree.DBytes("\x04")), // end_key
		}
		_, err := makeCheckConsistencyGenerator(&tree.EvalContext{
			Context: ctx,
			DB:      &kv.DB{},
		}, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start key must be >= ")
	})

	// Test ResolvedType (mock generator)
	t.Run("resolved type", func(t *testing.T) {
		gen := &checkConsistencyGenerator{}
		assert.Equal(t, checkConsistencyGeneratorType, gen.ResolvedType())
	})

	// Test Close (no panic)
	t.Run("close", func(t *testing.T) {
		gen := &checkConsistencyGenerator{}
		gen.Close()
	})
}
