// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/arith"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range aggregates {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if !v.props.Impure {
			panic(fmt.Sprintf("%s: aggregate functions should all be impure, found %v", k, v))
		}
		if v.props.Class != tree.AggregateClass {
			panic(fmt.Sprintf("%s: aggregate functions should be marked with the tree.AggregateClass "+
				"function class, found %v", k, v))
		}
		for _, a := range v.overloads {
			if a.AggregateFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.AggregateFunc constructors, "+
					"found %v", k, a))
			}
			if a.WindowFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.WindowFunc constructors, "+
					"found %v", k, a))
			}
		}

		// The aggregate functions are considered "row dependent". This is
		// because each aggregate function application receives the set of
		// grouped rows as implicit parameter. It may have a different
		// value in every group, so it cannot be considered constant in
		// the context of a data source.
		v.props.NeedsRepeatedEvaluation = true

		builtins[k] = v
	}
}

func aggProps() tree.FunctionProperties {
	return tree.FunctionProperties{Class: tree.AggregateClass, Impure: true}
}

func aggPropsNullableArgs() tree.FunctionProperties {
	f := aggProps()
	f.NullableArgs = true
	return f
}

// aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
//
// See `aggregateFuncHolder` in the sql package.
//
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
//
// Some aggregate functions must handle nullable arguments, since normalizing
// an aggregate function call to NULL in the presence of a NULL argument may
// not be correct. There are two cases where an aggregate function must handle
// nullable arguments:
//  1. the aggregate function does not skip NULLs (e.g., ARRAY_AGG); and
//  2. the aggregate function does not return NULL when it aggregates no rows
//     (e.g., COUNT).
//
// For use in other packages, see AllAggregateBuiltinNames and
// GetBuiltinProperties().
// These functions are also identified with Class == tree.AggregateClass.
// The properties are reachable via tree.FunctionDefinition.
var aggregates = map[string]builtinDefinition{
	"array_agg": setProps(aggPropsNullableArgs(),
		arrayBuiltin(func(t *types.T) tree.Overload {
			return makeAggOverloadWithReturnType(
				[]*types.T{t},
				func(args []tree.TypedExpr) *types.T {
					if len(args) == 0 {
						return types.MakeArray(t)
					}
					// Whenever possible, use the expression's type, so we can properly
					// handle aliased types that don't explicitly have overloads.
					return types.MakeArray(args[0].ResolvedType())
				},
				newArrayAggregate,
				"Aggregates the selected values into an array.",
			)
		})),

	"avg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalAvgAggregate,
			"Calculates the average of the selected values."),
	),

	"bit_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newBitAndAggregate,
			"Calculates the bitwise AND of all non-null input values, or null if none."),
	),

	"bit_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newBitOrAggregate,
			"Calculates the bitwise OR of all non-null input values, or null if none."),
	),

	"bool_and": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	),

	"bool_or": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values."),
	),

	"concat_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values."),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values."),
		// TODO(eisen): support collated strings when the type system properly
		// supports parametric types.
	),

	"corr": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Int, types.Int}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Float, types.Int}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
		makeAggOverload([]*types.T{types.Int, types.Float}, types.Float, newCorrAggregate,
			"Calculates the correlation coefficient of the selected values."),
	),

	"count": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Int, newCountAggregate,
			"Calculates the number of selected elements."),
	),

	"count_rows": makeBuiltin(aggProps(),
		tree.Overload{
			Types:         tree.ArgTypes{},
			ReturnType:    tree.FixedReturnType(types.Int),
			AggregateFunc: newCountRowsAggregate,
			WindowFunc: func(params []*types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
				return newFramableAggregateWindow(
					newCountRowsAggregate(params, evalCtx, nil /* arguments */),
					func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
						return newCountRowsAggregate(params, evalCtx, arguments)
					},
				)
			},
			Info: "Calculates the number of rows.",
		},
	),

	"every": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	),

	"max": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the maximum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t}, tree.IdentityReturnType(0), newMaxAggregate, info,
			)
		}),

	"min": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t}, tree.IdentityReturnType(0), newMinAggregate, info,
			)
		}),

	"first": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the first selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(0), newFirstAggregate, info,
			)
		}),
	"firstts": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the timestamp of the first value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.FixedReturnType(types.TimestampTZ), newFirsttsAggregate, info,
			)
		}),
	"first_row": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the first selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(0), newFirstrowAggregate, info,
			)
		}),
	"first_row_ts": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the timestamp of the first value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.FixedReturnType(types.TimestampTZ), newFirstrowtsAggregate, info,
			)
		}),

	"last_row_ts": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the timestamp of the first value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.FixedReturnType(types.TimestampTZ), newLastrowtsAggregate, info,
			)
		}),
	"last": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the last selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ, types.TimestampTZ}, tree.IdentityReturnType(0), newLastAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the last selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(0), newLastAggregate, info,
			)
		},
	),
	"lastts": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the timestamp of the last value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ, types.TimestampTZ}, tree.FixedReturnType(types.TimestampTZ), newLasttsAggregate, info,
			)
		}),
	"last_row": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the last selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(0), newLastrowAggregate, info,
			)
		}),
	"min_extend": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Int}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Bool}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},

		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Char}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Float}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.VarBytes}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Timestamp}, tree.IdentityReturnType(1), newMinExtendAggregate, info,
			)
		},
	),

	"max_extend": collectOverloads(aggProps(), types.Scalar,
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Int}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Bool}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},

		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Char}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Float}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.VarBytes}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.TimestampTZ}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
		func(t *types.T) tree.Overload {
			info := "Identifies the minimum selected value."
			return makeAggOverloadWithReturnType(
				[]*types.T{t, types.Timestamp}, tree.IdentityReturnType(1), newMaxExtendAggregate, info,
			)
		},
	),
	"matching": makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Int, types.Int, types.Decimal, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Int, types.Decimal, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Int, types.Decimal, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Int, types.Int, types.String, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Int, types.String, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Int, types.String, types.Decimal, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Int, types.Int, types.Decimal, types.String, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Int, types.Decimal, types.String, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Int, types.Decimal, types.String, types.Int},
			types.Bool,
			newMatchingAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
	),

	"norm": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newDecimalNormAggregate,
			"Calculates the L2 norm (Euclidean norm) of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatNormAggregate,
			"Calculates the L2 norm (Euclidean norm) of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalNormAggregate,
			"Calculates the L2 norm (Euclidean norm) of the selected values."),
	),

	"string_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.String, types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
		makeAggOverload([]*types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
	),

	"sum_int": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Int, newSmallIntSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sum": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]*types.T{types.Interval}, types.Interval, newIntervalSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sqrdiff": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
	),

	"time_bucket_gapfill_internal": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Timestamp, types.String}, types.Timestamp, newTimeBucketAggregate,
			"TimeBucketGapfill"),
		makeAggOverload([]*types.T{types.TimestampTZ, types.String}, types.TimestampTZ, newTimestamptzBucketAggregate,
			"TimeBucketGapfill"),
		makeAggOverload([]*types.T{types.Timestamp, types.Int}, types.Timestamp, newTimeBucketAggregate,
			"TimeBucketGapfill"),
		makeAggOverload([]*types.T{types.TimestampTZ, types.Int}, types.TimestampTZ, newTimestamptzBucketAggregate,
			"TimeBucketGapfill"),
	),

	"elapsed": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Timestamp, types.Interval}, types.Float, newElapsedAggregate,
			"Calculates the continuous duration of the statistical period."),
		makeAggOverload([]*types.T{types.TimestampTZ, types.Interval}, types.Float, newElapsedAggregate,
			"Calculates the continuous duration of the statistical period."),
		makeAggOverload([]*types.T{types.Timestamp}, types.Float, newElapsedAggregate,
			"Calculates the continuous duration of the statistical period."),
		makeAggOverload([]*types.T{types.TimestampTZ}, types.Float, newElapsedAggregate,
			"Calculates the continuous duration of the statistical period."),
	),

	"twa": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Timestamp, types.Int}, types.Float, newTwaAggregate,
			"Calculates the time-weighted average of the statistical period."),
		makeAggOverload([]*types.T{types.Timestamp, types.Float}, types.Float, newTwaAggregate,
			"Calculates the time-weighted average of the statistical period."),
		makeAggOverload([]*types.T{types.TimestampTZ, types.Int}, types.Float, newTwaAggregate,
			"Calculates the time-weighted average of the statistical period."),
		makeAggOverload([]*types.T{types.TimestampTZ, types.Float}, types.Float, newTwaAggregate,
			"Calculates the time-weighted average of the statistical period."),
	),

	"interpolate": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int, types.Int}, types.Int, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Float, types.Int}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Decimal, types.Int}, types.Decimal, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Int, types.String}, types.Int, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Float, types.String}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Decimal, types.String}, types.Decimal, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Int, types.Float}, types.Int, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Decimal, types.Float}, types.Decimal, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Decimal, types.Decimal}, types.Decimal, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Int, types.Decimal}, types.Int, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Float, types.Decimal}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.String, types.String}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Timestamp, types.String}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.TimestampTZ, types.String}, types.Float, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.NChar, types.Int}, types.Int, newImputationAggregate,
			"Interpolate"),
		makeAggOverload([]*types.T{types.Bytes, types.String}, types.Int, newImputationAggregate,
			"Interpolate"),
	),

	// final_(variance|stddev) computes the global (variance|standard deviation)
	// from an arbitrary collection of local sums of squared difference from the mean.
	// Adapted from https://www.johndcook.com/blog/skewness_kurtosis and
	// https://gitee.com/kwbasedb/kwbase/pull/17728.

	// TODO(knz): The 3-argument final_variance and final_stddev are
	// only defined for internal use by distributed aggregations. They
	// are marked as "private" so as to not trigger panics from issue
	// #10495.

	// The input signature is: SQDIFF, SUM, COUNT
	"final_variance": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Decimal, types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
		),
	)),

	"final_stddev": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]*types.T{types.Decimal,
				types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]*types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
	)),

	"variance": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the variance of the selected values."),
	),

	"var_pop": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntPopVarianceAggregate,
			"Calculates the population variance of the selected values"),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalPopVarianceAggregate,
			"Calculates the population variance of the selected values"),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatPopVarianceAggregate,
			"Calculates the population variance of the selected values"),
	),

	"var_samp": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the sample variance of the selected values"),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the sample variance of the selected values"),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the sample variance of the selected values"),
	),

	// stddev is a historical alias for stddev_samp.
	"stddev":      makeStdDevBuiltin(),
	"stddev_samp": makeStdDevBuiltin(),

	"xor_agg": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Bytes}, types.Bytes, newBytesXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
		makeAggOverload([]*types.T{types.Int}, types.Int, newIntXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
	),

	"json_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"jsonb_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]*types.T{types.Any}, types.Jsonb, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"json_object_agg":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),
	"jsonb_object_agg": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),

	"quantile": makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int4, types.Float}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
		makeAggOverload([]*types.T{types.Float, types.Float}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
		makeAggOverload([]*types.T{types.Decimal, types.Float}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
		makeAggOverload([]*types.T{types.Int4, types.Decimal}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
		makeAggOverload([]*types.T{types.Float, types.Decimal}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
		makeAggOverload([]*types.T{types.Decimal, types.Decimal}, types.Float, newQuantileAggregate,
			"Calculates the specified quantile over a set of values."),
	),

	AnyNotNull: makePrivate(makeBuiltin(aggProps(),
		makeAggOverloadWithReturnType(
			[]*types.T{types.Any},
			tree.IdentityReturnType(0),
			newAnyNotNullAggregate,
			"Returns an arbitrary not-NULL value, or NULL if none exists.",
		))),
}

// AnyNotNull is the name of the aggregate returned by NewAnyNotNullAggregate.
const AnyNotNull = "any_not_null"

func makePrivate(b builtinDefinition) builtinDefinition {
	b.props.Private = true
	return b
}

func makeAggOverload(
	in []*types.T,
	ret *types.T,
	f func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	info string,
) tree.Overload {
	return makeAggOverloadWithReturnType(
		in,
		tree.FixedReturnType(ret),
		f,
		info,
	)
}

func makeAggOverloadWithReturnType(
	in []*types.T,
	retType tree.ReturnTyper,
	f func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	info string,
) tree.Overload {
	argTypes := make(tree.ArgTypes, len(in))
	for i, typ := range in {
		argTypes[i].Name = fmt.Sprintf("arg%d", i+1)
		argTypes[i].Typ = typ
	}

	return tree.Overload{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		Types:         argTypes,
		ReturnType:    retType,
		AggregateFunc: f,
		WindowFunc: func(params []*types.T, evalCtx *tree.EvalContext) tree.WindowFunc {
			aggWindowFunc := f(params, evalCtx, nil /* arguments */)
			switch w := aggWindowFunc.(type) {
			case *minAggregate:
				min := &slidingWindowFunc{}
				min.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return -a.Compare(evalCtx, b)
				})
				return min
			case *maxAggregate:
				max := &slidingWindowFunc{}
				max.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return max
			case *FirstAggregate:
				first := &slidingWindowFunc{}
				first.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return first
			case *FirstrowAggregate:
				firstrow := &slidingWindowFunc{}
				firstrow.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return firstrow
			case *LastAggregate:
				last := &slidingWindowFunc{}
				last.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return last
			case *LastrowAggregate:
				lastrow := &slidingWindowFunc{}
				lastrow.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return lastrow
			case *intSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *decimalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *floatSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *intervalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *avgAggregate:
				// w.agg is a sum aggregate.
				return &avgWindowFunc{sum: newSlidingWindowSumFunc(w.agg)}
			case *TimeBucketAggregate, *TimestamptzBucketAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *ImputationAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			}

			return newFramableAggregateWindow(
				aggWindowFunc,
				func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
					return f(params, evalCtx, arguments)
				},
			)
		},
		Info: info,
	}
}

func makeStdDevBuiltin() builtinDefinition {
	return makeBuiltin(aggProps(),
		makeAggOverload([]*types.T{types.Int}, types.Decimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]*types.T{types.Decimal}, types.Decimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]*types.T{types.Float}, types.Float, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
	)
}

var _ tree.AggregateFunc = &arrayAggregate{}
var _ tree.AggregateFunc = &avgAggregate{}
var _ tree.AggregateFunc = &corrAggregate{}
var _ tree.AggregateFunc = &countAggregate{}
var _ tree.AggregateFunc = &countRowsAggregate{}
var _ tree.AggregateFunc = &maxAggregate{}
var _ tree.AggregateFunc = &minAggregate{}
var _ tree.AggregateFunc = &FirstAggregate{}
var _ tree.AggregateFunc = &FirsttsAggregate{}
var _ tree.AggregateFunc = &FirstrowAggregate{}
var _ tree.AggregateFunc = &FirstrowtsAggregate{}
var _ tree.AggregateFunc = &LastrowtsAggregate{}
var _ tree.AggregateFunc = &LastAggregate{}
var _ tree.AggregateFunc = &LasttsAggregate{}
var _ tree.AggregateFunc = &LastrowAggregate{}
var _ tree.AggregateFunc = &matchingAggregate{}
var _ tree.AggregateFunc = &smallIntSumAggregate{}
var _ tree.AggregateFunc = &intSumAggregate{}
var _ tree.AggregateFunc = &decimalSumAggregate{}
var _ tree.AggregateFunc = &floatSumAggregate{}
var _ tree.AggregateFunc = &intervalSumAggregate{}
var _ tree.AggregateFunc = &intSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSqrDiffAggregate{}
var _ tree.AggregateFunc = &decimalSqrDiffAggregate{}
var _ tree.AggregateFunc = &floatSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &decimalSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &floatVarianceAggregate{}
var _ tree.AggregateFunc = &decimalVarianceAggregate{}
var _ tree.AggregateFunc = &floatPopVarianceAggregate{}
var _ tree.AggregateFunc = &decimalPopVarianceAggregate{}
var _ tree.AggregateFunc = &floatStdDevAggregate{}
var _ tree.AggregateFunc = &decimalStdDevAggregate{}
var _ tree.AggregateFunc = &anyNotNullAggregate{}
var _ tree.AggregateFunc = &concatAggregate{}
var _ tree.AggregateFunc = &boolAndAggregate{}
var _ tree.AggregateFunc = &boolOrAggregate{}
var _ tree.AggregateFunc = &bytesXorAggregate{}
var _ tree.AggregateFunc = &intXorAggregate{}
var _ tree.AggregateFunc = &jsonAggregate{}
var _ tree.AggregateFunc = &bitAndAggregate{}
var _ tree.AggregateFunc = &bitOrAggregate{}
var _ tree.AggregateFunc = &TimeBucketAggregate{}
var _ tree.AggregateFunc = &ImputationAggregate{}
var _ tree.AggregateFunc = &TimestamptzBucketAggregate{}
var _ tree.AggregateFunc = &quantileAggregate{}
var _ tree.AggregateFunc = &decimalNormAggregate{}
var _ tree.AggregateFunc = &floatNormAggregate{}

const sizeOfArrayAggregate = int64(unsafe.Sizeof(arrayAggregate{}))
const sizeOfAvgAggregate = int64(unsafe.Sizeof(avgAggregate{}))
const sizeOfCorrAggregate = int64(unsafe.Sizeof(corrAggregate{}))
const sizeOfCountAggregate = int64(unsafe.Sizeof(countAggregate{}))
const sizeOfCountRowsAggregate = int64(unsafe.Sizeof(countRowsAggregate{}))
const sizeOfMaxAggregate = int64(unsafe.Sizeof(maxAggregate{}))
const sizeOfMinAggregate = int64(unsafe.Sizeof(minAggregate{}))
const sizeOfFirstAggregate = int64(unsafe.Sizeof(FirstAggregate{}))
const sizeOfFirsttsAggregate = int64(unsafe.Sizeof(FirsttsAggregate{}))
const sizeOfFirstrowAggregate = int64(unsafe.Sizeof(FirstrowAggregate{}))
const sizeOfFirstrowtsAggregate = int64(unsafe.Sizeof(FirstrowtsAggregate{}))
const sizeOfLastrowtsAggregate = int64(unsafe.Sizeof(LastrowtsAggregate{}))
const sizeOfLastAggregate = int64(unsafe.Sizeof(LastAggregate{}))
const sizeOfLasttsAggregate = int64(unsafe.Sizeof(LasttsAggregate{}))
const sizeOfLastrowAggregate = int64(unsafe.Sizeof(LastrowAggregate{}))
const sizeOfMatchingAggregate = int64(unsafe.Sizeof(matchingAggregate{}))
const sizeOfSmallIntSumAggregate = int64(unsafe.Sizeof(smallIntSumAggregate{}))
const sizeOfIntSumAggregate = int64(unsafe.Sizeof(intSumAggregate{}))
const sizeOfDecimalSumAggregate = int64(unsafe.Sizeof(decimalSumAggregate{}))
const sizeOfFloatSumAggregate = int64(unsafe.Sizeof(floatSumAggregate{}))
const sizeOfIntervalSumAggregate = int64(unsafe.Sizeof(intervalSumAggregate{}))
const sizeOfIntSqrDiffAggregate = int64(unsafe.Sizeof(intSqrDiffAggregate{}))
const sizeOfFloatSqrDiffAggregate = int64(unsafe.Sizeof(floatSqrDiffAggregate{}))
const sizeOfDecimalSqrDiffAggregate = int64(unsafe.Sizeof(decimalSqrDiffAggregate{}))
const sizeOfFloatSumSqrDiffsAggregate = int64(unsafe.Sizeof(floatSumSqrDiffsAggregate{}))
const sizeOfDecimalSumSqrDiffsAggregate = int64(unsafe.Sizeof(decimalSumSqrDiffsAggregate{}))
const sizeOfFloatVarianceAggregate = int64(unsafe.Sizeof(floatVarianceAggregate{}))
const sizeOfDecimalVarianceAggregate = int64(unsafe.Sizeof(decimalVarianceAggregate{}))
const sizeOfFloatPopVarianceAggregate = int64(unsafe.Sizeof(floatPopVarianceAggregate{}))
const sizeOfDecimalPopVarianceAggregate = int64(unsafe.Sizeof(decimalPopVarianceAggregate{}))
const sizeOfFloatStdDevAggregate = int64(unsafe.Sizeof(floatStdDevAggregate{}))
const sizeOfDecimalStdDevAggregate = int64(unsafe.Sizeof(decimalStdDevAggregate{}))
const sizeOfAnyNotNullAggregate = int64(unsafe.Sizeof(anyNotNullAggregate{}))
const sizeOfConcatAggregate = int64(unsafe.Sizeof(concatAggregate{}))
const sizeOfBoolAndAggregate = int64(unsafe.Sizeof(boolAndAggregate{}))
const sizeOfBoolOrAggregate = int64(unsafe.Sizeof(boolOrAggregate{}))
const sizeOfBytesXorAggregate = int64(unsafe.Sizeof(bytesXorAggregate{}))
const sizeOfIntXorAggregate = int64(unsafe.Sizeof(intXorAggregate{}))
const sizeOfJSONAggregate = int64(unsafe.Sizeof(jsonAggregate{}))
const sizeOfBitAndAggregate = int64(unsafe.Sizeof(bitAndAggregate{}))
const sizeOfBitOrAggregate = int64(unsafe.Sizeof(bitOrAggregate{}))
const sizeOfTimeBucketAggregate = int64(unsafe.Sizeof(TimeBucketAggregate{}))
const sizeOfImputationAggregate = int64(unsafe.Sizeof(ImputationAggregate{}))
const sizeOfTimestamptzBucketAggregate = int64(unsafe.Sizeof(TimestamptzBucketAggregate{}))
const sizeOfElapsedAggregate = int64(unsafe.Sizeof(ElapsedAggregate{}))
const sizeOfTwaAggregate = int64(unsafe.Sizeof(TwaAggregate{}))
const sizeOfMaxExtendAggregate = int64(unsafe.Sizeof(MaxExtendAggregate{}))
const sizeOfMinExtendAggregate = int64(unsafe.Sizeof(MinExtendAggregate{}))
const sizeOfQuantileAggregate = int64(unsafe.Sizeof(quantileAggregate{}))
const sizeOfDecimalNormAggregate = int64(unsafe.Sizeof(decimalNormAggregate{}))
const sizeOfFloatNormAggregate = int64(unsafe.Sizeof(floatNormAggregate{}))

// singleDatumAggregateBase is a utility struct that helps aggregate builtins
// that store a single datum internally track their memory usage related to
// that single datum.
// It will reuse tree.EvalCtx.SingleDatumAggMemAccount when non-nil and will
// *not* close that account upon its closure; if it is nil, then a new memory
// account will be created specifically for this struct and that account will
// be closed upon this struct's closure.
type singleDatumAggregateBase struct {
	mode singleDatumAggregateBaseMode
	acc  *mon.BoundAccount
	// accountedFor indicates how much memory (in bytes) have been registered
	// with acc.
	accountedFor int64
}

// singleDatumAggregateBaseMode indicates the mode in which
// singleDatumAggregateBase operates with regards to resetting and closing
// behaviors.
type singleDatumAggregateBaseMode int

const (
	// sharedSingleDatumAggregateBaseMode is a mode in which the memory account
	// will be grown and shrunk according the corresponding aggregate builtin's
	// memory usage, but the account will never be cleared or closed. In this
	// mode, singleDatumAggregateBaseMode is *not* responsible for closing the
	// memory account.
	sharedSingleDatumAggregateBaseMode singleDatumAggregateBaseMode = iota
	// nonSharedSingleDatumAggregateBaseMode is a mode in which the memory
	// account is "owned" by singleDatumAggregateBase, so the account can be
	// cleared and closed by it. In fact, singleDatumAggregateBase is
	// responsible for the account's closure.
	nonSharedSingleDatumAggregateBaseMode
)

// makeSingleDatumAggregateBase makes a new singleDatumAggregateBase. If
// evalCtx has non-nil SingleDatumAggMemAccount field, then that memory account
// will be used by the new struct which will operate in "shared" mode
func makeSingleDatumAggregateBase(evalCtx *tree.EvalContext) singleDatumAggregateBase {
	if evalCtx.SingleDatumAggMemAccount == nil {
		newAcc := evalCtx.Mon.MakeBoundAccount()
		return singleDatumAggregateBase{
			mode: nonSharedSingleDatumAggregateBaseMode,
			acc:  &newAcc,
		}
	}
	return singleDatumAggregateBase{
		mode: sharedSingleDatumAggregateBaseMode,
		acc:  evalCtx.SingleDatumAggMemAccount,
	}
}

// updateMemoryUsage updates the memory account to reflect the new memory
// usage. If any memory has been previously registered with this struct, then
// the account is updated only by the delta between previous and new usages,
// otherwise, it is grown by newUsage.
func (b *singleDatumAggregateBase) updateMemoryUsage(ctx context.Context, newUsage int64) error {
	if err := b.acc.Grow(ctx, newUsage-b.accountedFor); err != nil {
		return err
	}
	b.accountedFor = newUsage
	return nil
}

func (b *singleDatumAggregateBase) reset(ctx context.Context) {
	switch b.mode {
	case sharedSingleDatumAggregateBaseMode:
		b.acc.Shrink(ctx, b.accountedFor)
		b.accountedFor = 0
	case nonSharedSingleDatumAggregateBaseMode:
		b.acc.Clear(ctx)
	default:
		panic(errors.Errorf("unexpected singleDatumAggregateBaseMode: %d", b.mode))
	}
}

func (b *singleDatumAggregateBase) close(ctx context.Context) {
	switch b.mode {
	case sharedSingleDatumAggregateBaseMode:
		b.acc.Shrink(ctx, b.accountedFor)
		b.accountedFor = 0
	case nonSharedSingleDatumAggregateBaseMode:
		b.acc.Close(ctx)
	default:
		panic(errors.Errorf("unexpected singleDatumAggregateBaseMode: %d", b.mode))
	}
}

// See NewAnyNotNullAggregate.
type anyNotNullAggregate struct {
	singleDatumAggregateBase

	val tree.Datum
}

// NewAnyNotNullAggregate returns an aggregate function that returns an
// arbitrary not-NULL value passed to Add (or NULL if no such value). This is
// particularly useful for "passing through" values for columns which we know
// are constant within any aggregation group (for example, the grouping columns
// themselves).
//
// Note that NULL values do not affect the result of the aggregation; this is
// important in a few different contexts:
//
//   - in distributed multi-stage aggregations, we can have a local stage with
//     multiple (parallel) instances feeding into a final stage. If some of the
//     instances see no rows, they emit a NULL into the final stage which needs
//     to be ignored.
//
//   - for query optimization, when moving aggregations across left joins (which
//     add NULL values).
func NewAnyNotNullAggregate(evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &anyNotNullAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		val:                      tree.DNull,
	}
}

func newAnyNotNullAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, datums tree.Datums,
) tree.AggregateFunc {
	return NewAnyNotNullAggregate(evalCtx, datums)
}

// Add sets the value to the passed datum.
func (a *anyNotNullAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.val == tree.DNull && datum != tree.DNull {
		a.val = datum
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the value most recently passed to Add.
func (a *anyNotNullAggregate) Result() (tree.Datum, error) {
	return a.val, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Reset(ctx context.Context) {
	a.val = tree.DNull
	a.reset(ctx)
}

// Close is no-op in aggregates using constant space.
func (a *anyNotNullAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *anyNotNullAggregate) Size() int64 {
	return sizeOfAnyNotNullAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *anyNotNullAggregate) AggHandling() {
	// do nothing
}

type arrayAggregate struct {
	arr *tree.DArray
	// Note that we do not embed singleDatumAggregateBase struct to help with
	// memory accounting because arrayAggregate stores multiple datums inside
	// of arr.
	acc mon.BoundAccount
}

func newArrayAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &arrayAggregate{
		arr: tree.NewDArray(params[0]),
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add accumulates the passed datum into the array.
func (a *arrayAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if err := a.acc.Grow(ctx, int64(datum.Size())); err != nil {
		return err
	}
	return a.arr.Append(datum)
}

// Result returns a copy of the array of all datums passed to Add.
func (a *arrayAggregate) Result() (tree.Datum, error) {
	if len(a.arr.Array) > 0 {
		arrCopy := *a.arr
		return &arrCopy, nil
	}
	return tree.DNull, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *arrayAggregate) Reset(ctx context.Context) {
	a.arr = tree.NewDArray(a.arr.ParamTyp)
	a.acc.Empty(ctx)
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *arrayAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *arrayAggregate) Size() int64 {
	return sizeOfArrayAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *arrayAggregate) AggHandling() {
	// do nothing
}

type avgAggregate struct {
	agg   tree.AggregateFunc
	count int
}

func newIntAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newIntSumAggregate(params, evalCtx, arguments)}
}
func newFloatAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newFloatSumAggregate(params, evalCtx, arguments)}
}
func newDecimalAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newDecimalSumAggregate(params, evalCtx, arguments)}
}
func newIntervalAvgAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &avgAggregate{agg: newIntervalSumAggregate(params, evalCtx, arguments)}
}

// Add accumulates the passed datum into the average.
func (a *avgAggregate) Add(ctx context.Context, datum tree.Datum, other ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if err := a.agg.Add(ctx, datum); err != nil {
		return err
	}
	a.count++
	return nil
}

// Result returns the average of all datums passed to Add.
func (a *avgAggregate) Result() (tree.Datum, error) {
	sum, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(a.count)), nil
	case *tree.DDecimal:
		count := apd.New(int64(a.count), 0)
		_, err := tree.DecimalCtx.Quo(&t.Decimal, &t.Decimal, count)
		return t, err
	case *tree.DInterval:
		return &tree.DInterval{Duration: t.Duration.Div(int64(a.count))}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected SUM result type: %s", t)
	}
}

// Reset implements tree.AggregateFunc interface.
func (a *avgAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *avgAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *avgAggregate) Size() int64 {
	return sizeOfAvgAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *avgAggregate) AggHandling() {
	// do nothing
}

type concatAggregate struct {
	singleDatumAggregateBase

	forBytes   bool
	sawNonNull bool
	delimiter  string // used for non window functions
	result     bytes.Buffer
}

func newBytesConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		forBytes:                 true,
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDBytes(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newStringConcatAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	concatAgg := &concatAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDString(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func (a *concatAggregate) Add(ctx context.Context, datum tree.Datum, others ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
	} else {
		delimiter := a.delimiter
		// If this is called as part of a window function, the delimiter is passed in
		// via the first element in others.
		if len(others) == 1 && others[0] != tree.DNull {
			if a.forBytes {
				delimiter = string(tree.MustBeDBytes(others[0]))
			} else {
				delimiter = string(tree.MustBeDString(others[0]))
			}
		} else if len(others) > 1 {
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if len(delimiter) > 0 {
			a.result.WriteString(delimiter)
		}
	}
	var arg string
	if a.forBytes {
		arg = string(tree.MustBeDBytes(datum))
	} else {
		arg = string(tree.MustBeDString(datum))
	}
	a.result.WriteString(arg)
	if err := a.updateMemoryUsage(ctx, int64(a.result.Cap())); err != nil {
		return err
	}
	return nil
}

func (a *concatAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	if a.forBytes {
		res := tree.DBytes(a.result.String())
		return &res, nil
	}
	res := tree.DString(a.result.String())
	return &res, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *concatAggregate) Reset(ctx context.Context) {
	a.sawNonNull = false
	a.result.Reset()
	// Note that a.result.Reset() does *not* release already allocated memory,
	// so we do not reset singleDatumAggregateBase.
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *concatAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *concatAggregate) Size() int64 {
	return sizeOfConcatAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *concatAggregate) AggHandling() {
	// do nothing
}

type bitAndAggregate struct {
	sawNonNull bool
	result     int64
}

func newBitAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitAndAggregate{}
}

// Add inserts one value into the running bitwise AND.
func (a *bitAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = int64(tree.MustBeDInt(datum))
		a.sawNonNull = true
		return nil
	}
	// This is not the first non-null datum, so we actually AND it with the
	// aggregate so far.
	a.result = a.result & int64(tree.MustBeDInt(datum))
	return nil
}

// Result returns the bitwise AND.
func (a *bitAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitAndAggregate) Size() int64 {
	return sizeOfBitAndAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *bitAndAggregate) AggHandling() {
	// do nothing
}

type bitOrAggregate struct {
	sawNonNull bool
	result     int64
}

func newBitOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &bitOrAggregate{}
}

// Add inserts one value into the running bitwise OR.
func (a *bitOrAggregate) Add(_ context.Context, datum tree.Datum, otherArgs ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		// This is the first non-null datum, so we simply store
		// the provided value for the aggregation.
		a.result = int64(tree.MustBeDInt(datum))
		a.sawNonNull = true
		return nil
	}
	// This is not the first non-null datum, so we actually OR it with the
	// aggregate so far.
	a.result = a.result | int64(tree.MustBeDInt(datum))
	return nil
}

// Result returns the bitwise OR.
func (a *bitOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bitOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *bitOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *bitOrAggregate) Size() int64 {
	return sizeOfBitOrAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *bitOrAggregate) AggHandling() {
	// do nothing
}

type boolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &boolAndAggregate{}
}

func (a *boolAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
		a.result = true
	}
	a.result = a.result && bool(*datum.(*tree.DBool))
	return nil
}

func (a *boolAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *boolAndAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) Size() int64 {
	return sizeOfBoolAndAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *boolAndAggregate) AggHandling() {
	// do nothing
}

type boolOrAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolOrAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &boolOrAggregate{}
}

func (a *boolOrAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true
	a.result = a.result || bool(*datum.(*tree.DBool))
	return nil
}

func (a *boolOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *boolOrAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.result = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) Size() int64 {
	return sizeOfBoolOrAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *boolOrAggregate) AggHandling() {
	// do nothing
}

// corrAggregate represents SQL:2003 correlation coefficient.
//
// n   be count of rows.
// sx  be the sum of the column of values of <independent variable expression>
// sx2 be the sum of the squares of values in the <independent variable expression> column
// sy  be the sum of the column of values of <dependent variable expression>
// sy2 be the sum of the squares of values in the <dependent variable expression> column
// sxy be the sum of the row-wise products of the value in the <independent variable expression>
//
//	column times the value in the <dependent variable expression> column.
//
// result:
//  1. If n*sx2 equals sx*sx, then the result is the null value.
//  2. If n*sy2 equals sy*sy, then the result is the null value.
//  3. Otherwise, the resut is SQRT(POWER(n*sxy-sx*sy,2) / ((n*sx2-sx*sx)*(n*sy2-sy*sy))).
//     If the exponent of the approximate mathematical result of the operation is not within
//     the implementation-defined exponent range for the result data type, then the result
//     is the null value.
type corrAggregate struct {
	n   int
	sx  float64
	sx2 float64
	sy  float64
	sy2 float64
	sxy float64
}

func newCorrAggregate([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc {
	return &corrAggregate{}
}

// Add implements tree.AggregateFunc interface.
func (a *corrAggregate) Add(_ context.Context, datumY tree.Datum, otherArgs ...tree.Datum) error {
	if datumY == tree.DNull {
		return nil
	}

	datumX := otherArgs[0]
	if datumX == tree.DNull {
		return nil
	}

	x, err := a.float64Val(datumX)
	if err != nil {
		return err
	}

	y, err := a.float64Val(datumY)
	if err != nil {
		return err
	}

	a.n++
	a.sx += x
	a.sy += y
	a.sx2 += x * x
	a.sy2 += y * y
	a.sxy += x * y

	if math.IsInf(a.sx, 0) ||
		math.IsInf(a.sx2, 0) ||
		math.IsInf(a.sy, 0) ||
		math.IsInf(a.sy2, 0) ||
		math.IsInf(a.sxy, 0) {
		return tree.ErrFloatOutOfRange
	}

	return nil
}

// Result implements tree.AggregateFunc interface.
func (a *corrAggregate) Result() (tree.Datum, error) {
	if a.n < 1 {
		return tree.DNull, nil
	}

	if a.sx2 == 0 || a.sy2 == 0 {
		return tree.DNull, nil
	}

	floatN := float64(a.n)

	numeratorX := floatN*a.sx2 - a.sx*a.sx
	if math.IsInf(numeratorX, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	numeratorY := floatN*a.sy2 - a.sy*a.sy
	if math.IsInf(numeratorY, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	numeratorXY := floatN*a.sxy - a.sx*a.sy
	if math.IsInf(numeratorXY, 0) {
		return tree.DNull, pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	}

	if numeratorX <= 0 || numeratorY <= 0 {
		return tree.DNull, nil
	}

	return tree.NewDFloat(tree.DFloat(numeratorXY / math.Sqrt(numeratorX*numeratorY))), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *corrAggregate) Reset(context.Context) {
	a.n = 0
	a.sx = 0
	a.sx2 = 0
	a.sy = 0
	a.sy2 = 0
	a.sxy = 0
}

// Close implements tree.AggregateFunc interface.
func (a *corrAggregate) Close(context.Context) {}

// Size implements tree.AggregateFunc interface.
func (a *corrAggregate) Size() int64 {
	return sizeOfCorrAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *corrAggregate) AggHandling() {
	// do nothing
}

func (a *corrAggregate) float64Val(datum tree.Datum) (float64, error) {
	switch val := datum.(type) {
	case *tree.DFloat:
		return float64(*val), nil
	case *tree.DInt:
		return float64(*val), nil
	default:
		return 0, fmt.Errorf("invalid type %v", val)
	}
}

type countAggregate struct {
	count int
}

func newCountAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &countAggregate{}
}

func (a *countAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.count++
	return nil
}

func (a *countAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *countAggregate) Reset(context.Context) {
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *countAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *countAggregate) Size() int64 {
	return sizeOfCountAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *countAggregate) AggHandling() {
	// do nothing
}

type countRowsAggregate struct {
	count int
}

func newCountRowsAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &countRowsAggregate{}
}

func (a *countRowsAggregate) Add(_ context.Context, _ tree.Datum, _ ...tree.Datum) error {
	a.count++
	return nil
}

func (a *countRowsAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *countRowsAggregate) Reset(context.Context) {
	a.count = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) Size() int64 {
	return sizeOfCountRowsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *countRowsAggregate) AggHandling() {
	// do nothing
}

// maxAggregate keeps track of the largest value passed to Add.
type maxAggregate struct {
	singleDatumAggregateBase

	max               tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMaxAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "max" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &maxAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *maxAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.max = datum
		return nil
	}
	c := a.max.Compare(a.evalCtx, datum)
	if c < 0 {
		a.max = datum
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *maxAggregate) Result() (tree.Datum, error) {
	if a.max == nil {
		return tree.DNull, nil
	}
	return a.max, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *maxAggregate) Reset(ctx context.Context) {
	a.max = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *maxAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *maxAggregate) Size() int64 {
	return sizeOfMaxAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *maxAggregate) AggHandling() {
	// do nothing
}

// minAggregate keeps track of the smallest value passed to Add.
type minAggregate struct {
	singleDatumAggregateBase

	min               tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMinAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &minAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *minAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
		a.min = datum
		return nil
	}
	c := a.min.Compare(a.evalCtx, datum)
	if c > 0 {
		a.min = datum
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *minAggregate) Result() (tree.Datum, error) {
	if a.min == nil {
		return tree.DNull, nil
	}
	return a.min, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *minAggregate) Reset(ctx context.Context) {
	a.min = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *minAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *minAggregate) Size() int64 {
	return sizeOfMinAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *minAggregate) AggHandling() {
	// do nothing
}

// add datum in last/lastts/last_row/first/firstts/first_row/
//
// param:
// - variableDatumSize: update the memory param
// - isCheckNull: Whether to check for null values
// - isLast: Whether to use the last related function for value judgment
// - inDatum: The value of the previous row of data, value of column
// - inTSDatum: The value of the previous row of data, value of timestamp column
// - datum: The value of the data in this line, first param
// - otherDatum: The value of the data in this line, other param
//
// first function: Determine the timestamp for each row. If it is smaller than the previous row, update the first data and check for null values
//
// firstts function: Determine the timestamp for each row. If it is smaller than the previous row, update the firstts data and check for null values
//
// first_row function: Determine the timestamp for each row. If it is smaller than the previous row, update the first data without checking for null values
//
// last function: Determine the timestamp for each row. If it is larger than the previous row, update the last data and check for null values
//
// lastts function: Determine the timestamp for each row. If it is larger than the previous row, update the last data and check for null values
//
// last_row function: Determine the timestamp for each row. If it is larger than the previous row,
//
//	update the lastrow data without checking for null values
func lastAndFirstAdd(
	evalCtx *tree.EvalContext,
	variableDatumSize, isCheckNull, isLast bool,
	inDatum, inTSDatum, datum, DeadlineTs tree.Datum,
	otherDatum ...tree.Datum,
) (tree.Datum, tree.Datum, bool) {
	if len(otherDatum) == 0 {
		if isCheckNull && datum == tree.DNull {
			return inDatum, inTSDatum, false
		}
		if inDatum == nil {
			return datum, inTSDatum, true
		}
		if variableDatumSize {
			return datum, inTSDatum, true
		}
		return datum, inTSDatum, false
	}
	tsDatum := otherDatum[0]
	if isLast && len(otherDatum) == 1 && DeadlineTs != nil {
		c1 := tsDatum.Compare(evalCtx, DeadlineTs)
		if c1 > 0 {
			return inDatum, inTSDatum, false
		}
	}
	if (isCheckNull && datum == tree.DNull) || tsDatum == tree.DNull {
		return inDatum, inTSDatum, false
	}

	if inDatum == nil {
		return datum, tsDatum, true
	}
	// Compare timestamp
	c := inTSDatum.Compare(evalCtx, tsDatum)
	if (isLast && c <= 0) || (!isLast && c > 0) {
		if variableDatumSize {
			return datum, tsDatum, true
		}
		return datum, tsDatum, false
	}
	return inDatum, inTSDatum, false
}

// FirstAggregate keeps track of the first value passed to Add.
type FirstAggregate struct {
	singleDatumAggregateBase

	first             tree.Datum
	firstts           tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newFirstAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &FirstAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		first:                    nil,
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *FirstAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	var isUpdateMem bool
	a.first, a.firstts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, true, false, a.first, a.firstts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *FirstAggregate) Result() (tree.Datum, error) {
	if a.first == nil {
		return tree.DNull, nil
	}
	return a.first, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *FirstAggregate) Reset(ctx context.Context) {
	a.first = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *FirstAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FirstAggregate) Size() int64 {
	return sizeOfFirstAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *FirstAggregate) AggHandling() {
	// do nothing
}

// FirsttsAggregate keeps track of the first value passed to Add.
type FirsttsAggregate struct {
	singleDatumAggregateBase

	firstts           tree.Datum
	first             tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newFirsttsAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &FirsttsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
		firstts:                  nil,
		first:                    nil,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *FirsttsAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	// for functions like timebucket gapfill interpolate, no ts is added for unordered timestamps
	var isUpdateMem bool
	a.first, a.firstts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, true, false, a.first, a.firstts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *FirsttsAggregate) Result() (tree.Datum, error) {
	if a.firstts == nil {
		return tree.DNull, nil
	}
	return a.firstts, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *FirsttsAggregate) Reset(ctx context.Context) {
	a.firstts = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *FirsttsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FirsttsAggregate) Size() int64 {
	return sizeOfFirsttsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *FirsttsAggregate) AggHandling() {
	// do nothing
}

// Context returns the evalCtx of ag.
func (a *FirsttsAggregate) Context() *tree.EvalContext {
	return a.evalCtx
}

// FirstrowAggregate keeps track of the first value passed to Add.
type FirstrowAggregate struct {
	singleDatumAggregateBase

	firstrow          tree.Datum
	firstrowts        tree.Datum
	evalCtx           *tree.EvalContext
	tsEvalCtx         *tree.EvalContext
	variableDatumSize bool
}

func newFirstrowAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &FirstrowAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *FirstrowAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	var isUpdateMem bool
	a.firstrow, a.firstrowts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, false, false, a.firstrow, a.firstrowts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *FirstrowAggregate) Result() (tree.Datum, error) {
	if a.firstrow == nil {
		return tree.DNull, nil
	}
	return a.firstrow, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *FirstrowAggregate) Reset(ctx context.Context) {
	a.firstrow = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *FirstrowAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FirstrowAggregate) Size() int64 {
	return sizeOfFirstrowAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *FirstrowAggregate) AggHandling() {
	// do nothing
}

// SetTsEvalCtx set the tsEvalCtx.
func (a *FirstrowAggregate) SetTsEvalCtx(tsEvalCtx *tree.EvalContext) {
	a.tsEvalCtx = tsEvalCtx
}

// FirstrowtsAggregate keeps track of the first value passed to Add.
type FirstrowtsAggregate struct {
	singleDatumAggregateBase

	firstrowts        tree.Datum
	firstrow          tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newFirstrowtsAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &FirstrowtsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
		firstrowts:               nil,
		firstrow:                 nil,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *FirstrowtsAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	// for functions like timebucket gapfill interpolate, no ts is added for unordered timestamps
	var isUpdateMem bool
	a.firstrow, a.firstrowts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, false, false, a.firstrow, a.firstrowts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *FirstrowtsAggregate) Result() (tree.Datum, error) {
	if a.firstrowts == nil {
		return tree.DNull, nil
	}
	return a.firstrowts, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *FirstrowtsAggregate) Reset(ctx context.Context) {
	a.firstrowts = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *FirstrowtsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FirstrowtsAggregate) Size() int64 {
	return sizeOfFirstrowtsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *FirstrowtsAggregate) AggHandling() {
	// do nothing
}

// Context returns the evalCtx of ag.
func (a *FirstrowtsAggregate) Context() *tree.EvalContext {
	return a.evalCtx
}

// LastAggregate keeps track of the first value passed to Add.
type LastAggregate struct {
	singleDatumAggregateBase

	last              tree.Datum
	lastts            tree.Datum
	DeadlineTs        tree.Datum
	evalCtx           *tree.EvalContext
	tsEvalCtx         *tree.EvalContext
	variableDatumSize bool
}

func newLastAggregate(
	params []*types.T, evalCtx *tree.EvalContext, deadlineTs tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	lastAgg := &LastAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
		last:                     nil,
		lastts:                   nil,
	}

	if len(deadlineTs) == 1 && deadlineTs[0] != tree.DNull {
		lastAgg.DeadlineTs = deadlineTs[0]
	} else if len(deadlineTs) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(deadlineTs)))
	}

	return lastAgg
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *LastAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	// for functions like timebucket gapfill interpolate, no ts is added for unordered timestamps
	var isUpdateMem bool
	a.last, a.lastts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, true, true, a.last, a.lastts, datum, a.DeadlineTs, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *LastAggregate) Result() (tree.Datum, error) {
	if a.last == nil {
		return tree.DNull, nil
	}
	return a.last, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *LastAggregate) Reset(ctx context.Context) {
	a.last = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *LastAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *LastAggregate) Size() int64 {
	return sizeOfLastAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *LastAggregate) AggHandling() {
	// do nothing
}

// SetTsEvalCtx set the tsEvalCtx.
func (a *LastAggregate) SetTsEvalCtx(tsEvalCtx *tree.EvalContext) {
	a.tsEvalCtx = tsEvalCtx
}

// LasttsAggregate keeps track of the first value passed to Add.
type LasttsAggregate struct {
	singleDatumAggregateBase

	lastts            tree.Datum
	last              tree.Datum
	DeadlineTs        tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newLasttsAggregate(
	params []*types.T, evalCtx *tree.EvalContext, deadlineTs tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	lasttsAgg := &LasttsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
		lastts:                   nil,
		last:                     nil,
	}

	if len(deadlineTs) == 1 && deadlineTs[0] != tree.DNull {
		lasttsAgg.DeadlineTs = deadlineTs[0]
	} else if len(deadlineTs) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(deadlineTs)))
	}

	return lasttsAgg
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *LasttsAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	// for functions like timebucket gapfill interpolate, no ts is added for unordered timestamps
	var isUpdateMem bool
	a.last, a.lastts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, true, true, a.last, a.lastts, datum, a.DeadlineTs, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *LasttsAggregate) Result() (tree.Datum, error) {
	if a.lastts == nil {
		return tree.DNull, nil
	}
	return a.lastts, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *LasttsAggregate) Reset(ctx context.Context) {
	a.lastts = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *LasttsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *LasttsAggregate) Size() int64 {
	return sizeOfLasttsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *LasttsAggregate) AggHandling() {
	// do nothing
}

// Context returns the evalCtx of ag.
func (a *LasttsAggregate) Context() *tree.EvalContext {
	return a.evalCtx
}

// LastrowAggregate keeps track of the first value passed to Add.
type LastrowAggregate struct {
	singleDatumAggregateBase

	lastrow           tree.Datum
	lastrowts         tree.Datum
	evalCtx           *tree.EvalContext
	tsEvalCtx         *tree.EvalContext
	variableDatumSize bool
}

func newLastrowAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &LastrowAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *LastrowAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	var isUpdateMem bool
	a.lastrow, a.lastrowts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, false, true, a.lastrow, a.lastrowts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *LastrowAggregate) Result() (tree.Datum, error) {
	if a.lastrow == nil {
		return tree.DNull, nil
	}
	return a.lastrow, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *LastrowAggregate) Reset(ctx context.Context) {
	a.lastrow = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *LastrowAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *LastrowAggregate) Size() int64 {
	return sizeOfLastrowAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *LastrowAggregate) AggHandling() {
	// do nothing
}

// SetTsEvalCtx set the tsEvalCtx.
func (a *LastrowAggregate) SetTsEvalCtx(tsEvalCtx *tree.EvalContext) {
	a.tsEvalCtx = tsEvalCtx
}

// LastrowtsAggregate keeps track of the first value passed to Add.
type LastrowtsAggregate struct {
	singleDatumAggregateBase

	lastrowts         tree.Datum
	lastrow           tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newLastrowtsAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &LastrowtsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
		lastrowts:                nil,
		lastrow:                  nil,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *LastrowtsAggregate) Add(
	ctx context.Context, datum tree.Datum, otherDatums ...tree.Datum,
) error {
	// for functions like timebucket gapfill interpolate, no ts is added for unordered timestamps
	var isUpdateMem bool
	a.lastrow, a.lastrowts, isUpdateMem = lastAndFirstAdd(a.evalCtx, a.variableDatumSize, false, true, a.lastrow, a.lastrowts, datum, nil, otherDatums...)
	if isUpdateMem {
		if err := a.updateMemoryUsage(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *LastrowtsAggregate) Result() (tree.Datum, error) {
	if a.lastrowts == nil {
		return tree.DNull, nil
	}
	return a.lastrowts, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *LastrowtsAggregate) Reset(ctx context.Context) {
	a.lastrowts = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *LastrowtsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *LastrowtsAggregate) Size() int64 {
	return sizeOfLastrowtsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *LastrowtsAggregate) AggHandling() {
	// do nothing
}

// Context returns the evalCtx of ag.
func (a *LastrowtsAggregate) Context() *tree.EvalContext {
	return a.evalCtx
}

// matchingAggregate is used to judge whether this column meet custom matching condition.
type matchingAggregate struct {
	evalCtx *tree.EvalContext

	boundCount int
	count      int
	matching   float64
	alarm      bool
}

// newMatchingAggregate method constructs a new matchingAggregate
func newMatchingAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &matchingAggregate{
		evalCtx: evalCtx,
	}
}

// Add implements tree.AggregateFunc interface.
func (a *matchingAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {

	value := firstArg
	var val tree.DDecimal
	err := val.SetString(value.String())
	if err != nil {
		return err
	}

	rule := int64(tree.MustBeDInt(otherArgs[0]))
	matching := float64(tree.MustBeDInt(otherArgs[3]))

	a.matching = matching / 100

	switch rule {
	case 0:
		if _, ok := otherArgs[1].(*tree.DString); ok {
			if val.Compare(a.evalCtx, otherArgs[2]) != 1 {
				a.boundCount++
			}
		} else {
			if _, ok := otherArgs[2].(*tree.DString); ok {
				if val.Compare(a.evalCtx, otherArgs[1]) != -1 {
					a.boundCount++
				}
			} else {
				if val.Compare(a.evalCtx, otherArgs[2]) != 1 && val.Compare(a.evalCtx, otherArgs[1]) != -1 {
					a.boundCount++
				}
			}
		}
	case 1:
		if _, ok := otherArgs[1].(*tree.DString); ok {
			if val.Compare(a.evalCtx, otherArgs[2]) != 1 {
				a.boundCount++
			}
		} else {
			if _, ok := otherArgs[2].(*tree.DString); ok {
				if val.Compare(a.evalCtx, otherArgs[1]) == 1 {
					a.boundCount++
				}
			} else {
				if val.Compare(a.evalCtx, otherArgs[2]) != 1 && val.Compare(a.evalCtx, otherArgs[1]) == 1 {
					a.boundCount++
				}
			}
		}
	case 2:
		if _, ok := otherArgs[1].(*tree.DString); ok {
			if val.Compare(a.evalCtx, otherArgs[2]) == -1 {
				a.boundCount++
			}
		} else {
			if _, ok := otherArgs[2].(*tree.DString); ok {
				if val.Compare(a.evalCtx, otherArgs[1]) != -1 {
					a.boundCount++
				}
			} else {
				if val.Compare(a.evalCtx, otherArgs[2]) == -1 && val.Compare(a.evalCtx, otherArgs[1]) != -1 {
					a.boundCount++
				}
			}
		}
	case 3:
		if _, ok := otherArgs[1].(*tree.DString); ok {
			if val.Compare(a.evalCtx, otherArgs[2]) == -1 {
				a.boundCount++
			}
		} else {
			if _, ok := otherArgs[2].(*tree.DString); ok {
				if val.Compare(a.evalCtx, otherArgs[1]) == 1 {
					a.boundCount++
				}
			} else {
				if val.Compare(a.evalCtx, otherArgs[2]) == -1 && val.Compare(a.evalCtx, otherArgs[1]) == 1 {
					a.boundCount++
				}
			}
		}
	default:
		return errors.Errorf("unexpected exclude rule: %d", rule)
	}

	a.count++
	nowMatching := float64(a.boundCount) / float64(a.count)
	//alarm := nowMatching >= a.matching
	//if alarm {
	//	a.alarm = 1
	//} else {
	//	a.alarm = 0
	//}
	//a.alarm = strconv.FormatBool(alarm)
	a.alarm = nowMatching >= a.matching

	return nil
}

func (a *matchingAggregate) Result() (tree.Datum, error) {
	return tree.MakeDBool(tree.DBool(a.alarm)), nil
}

func (a *matchingAggregate) Reset(context.Context) {
	a.alarm = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *matchingAggregate) Close(context.Context) {
}

// Size is part of the tree.AggregateFunc interface.
func (a *matchingAggregate) Size() int64 {
	return sizeOfMatchingAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *matchingAggregate) AggHandling() {
	// do nothing
}

type smallIntSumAggregate struct {
	sum         int64
	seenNonNull bool
}

func newSmallIntSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &smallIntSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *smallIntSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	var ok bool
	a.sum, ok = arith.AddWithOverflow(a.sum, int64(tree.MustBeDInt(datum)))
	if !ok {
		return tree.ErrIntOutOfRange
	}
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *smallIntSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Reset(context.Context) {
	a.sum = 0
	a.seenNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) Size() int64 {
	return sizeOfSmallIntSumAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *smallIntSumAggregate) AggHandling() {
	// set to true in order to return 0.
	a.seenNonNull = true
}

type intSumAggregate struct {
	singleDatumAggregateBase

	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	intSum      int64
	decSum      apd.Decimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
}

func newIntSumAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intSumAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add adds the value of the passed datum to the sum.
func (a *intSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	t := int64(tree.MustBeDInt(datum))
	if t != 0 {
		// The sum can be computed using a single int64 as long as the
		// result of the addition does not overflow.  However since Go
		// does not provide checked addition, we have to check for the
		// overflow explicitly.
		if !a.large {
			r, ok := arith.AddWithOverflow(a.intSum, t)
			if ok {
				a.intSum = r
			} else {
				// And overflow was detected; go to large integers, but keep the
				// sum computed so far.
				a.large = true
				a.decSum.SetFinite(a.intSum, 0)
			}
		}

		if a.large {
			a.tmpDec.SetFinite(t, 0)
			_, err := tree.ExactCtx.Add(&a.decSum, &a.decSum, &a.tmpDec)
			if err != nil {
				return err
			}
			if err := a.updateMemoryUsage(ctx, int64(tree.SizeOfDecimal(a.decSum))); err != nil {
				return err
			}
		}
	}
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *intSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	if a.large {
		dd.Set(&a.decSum)
	} else {
		dd.SetFinite(a.intSum, 0)
	}
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intSumAggregate) Reset(context.Context) {
	// We choose not to reset apd.Decimal's since they will be set to
	// appropriate values when overflow occurs - we simply force the aggregate
	// to use Go types (at least, at first). That's why we also not reset the
	// singleDatumAggregateBase.
	a.seenNonNull = false
	a.intSum = 0
	a.large = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) Size() int64 {
	return sizeOfIntSumAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *intSumAggregate) AggHandling() {
	// do nothing
}

type decimalSumAggregate struct {
	singleDatumAggregateBase

	sum        apd.Decimal
	sawNonNull bool
}

func newDecimalSumAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalSumAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add adds the value of the passed datum to the sum.
func (a *decimalSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DDecimal)
	_, err := tree.ExactCtx.Add(&a.sum, &a.sum, &t.Decimal)
	if err != nil {
		return err
	}

	if err := a.updateMemoryUsage(ctx, int64(tree.SizeOfDecimal(a.sum))); err != nil {
		return err
	}

	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *decimalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	dd.Set(&a.sum)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSumAggregate) Reset(ctx context.Context) {
	a.sum.SetFinite(0, 0)
	a.sawNonNull = false
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) Size() int64 {
	return sizeOfDecimalSumAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *decimalSumAggregate) AggHandling() {
	// do nothing
}

type floatSumAggregate struct {
	sum        float64
	sawNonNull bool
}

func newFloatSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &floatSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *floatSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DFloat)
	a.sum += float64(*t)
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *floatSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSumAggregate) Reset(context.Context) {
	a.sawNonNull = false
	a.sum = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) Size() int64 {
	return sizeOfFloatSumAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *floatSumAggregate) AggHandling() {
	// do nothing
}

type intervalSumAggregate struct {
	sum        duration.Duration
	sawNonNull bool
}

func newIntervalSumAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intervalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *intervalSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DInterval).Duration
	a.sum = a.sum.Add(t)
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *intervalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return &tree.DInterval{Duration: a.sum}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intervalSumAggregate) Reset(context.Context) {
	a.sum = a.sum.Sub(a.sum)
	a.sawNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) Size() int64 {
	return sizeOfIntervalSumAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *intervalSumAggregate) AggHandling() {
	// do nothing
}

// Read-only constants used for square difference computations.
var (
	decimalOne = apd.New(1, 0)
	decimalTwo = apd.New(2, 0)
)

type intSqrDiffAggregate struct {
	agg decimalSqrDiff
	// Used for passing int64s as *apd.Decimal values.
	tmpDec tree.DDecimal
}

func newIntSqrDiff(evalCtx *tree.EvalContext) decimalSqrDiff {
	return &intSqrDiffAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

func newIntSqrDiffAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return newIntSqrDiff(evalCtx)
}

// Count is part of the decimalSqrDiff interface.
func (a *intSqrDiffAggregate) Count() *apd.Decimal {
	return a.agg.Count()
}

// Tmp is part of the decimalSqrDiff interface.
func (a *intSqrDiffAggregate) Tmp() *apd.Decimal {
	return a.agg.Tmp()
}

func (a *intSqrDiffAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	a.tmpDec.SetFinite(int64(tree.MustBeDInt(datum)), 0)
	return a.agg.Add(ctx, &a.tmpDec)
}

func (a *intSqrDiffAggregate) Result() (tree.Datum, error) {
	return a.agg.Result()
}

// Reset implements tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) Size() int64 {
	return sizeOfIntSqrDiffAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *intSqrDiffAggregate) AggHandling() {
	// do nothing
}

type floatSqrDiffAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSqrDiff() floatSqrDiff {
	return &floatSqrDiffAggregate{}
}

func newFloatSqrDiffAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return newFloatSqrDiff()
}

// Count is part of the floatSqrDiff interface.
func (a *floatSqrDiffAggregate) Count() int64 {
	return a.count
}

func (a *floatSqrDiffAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	f := float64(*datum.(*tree.DFloat))

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

func (a *floatSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Reset(context.Context) {
	a.count = 0
	a.mean = 0
	a.sqrDiff = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) Size() int64 {
	return sizeOfFloatSqrDiffAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *floatSqrDiffAggregate) AggHandling() {
	// do nothing
}

// TimeBucketAggregate is for timebucketagg
type TimeBucketAggregate struct {
	Time       time.Time
	Timebucket tree.DInterval
}

func newTimeBucketAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &TimeBucketAggregate{}
}

// Add is for timebucket
func (a *TimeBucketAggregate) Add(
	_ context.Context, datum1 tree.Datum, datum2 ...tree.Datum,
) error {
	if _, ok := datum1.(tree.DNullExtern); ok {
		return pgerror.New(pgcode.InvalidParameterValue, "first arg can not be null")
	}
	value, ok := datum1.(*tree.DTimestamp)
	if !ok {
		return pgerror.New(pgcode.InvalidParameterValue, "first arg is error")
	}
	dInterval, err := GetTimeInterval(datum2[0])
	if err != nil {
		return err
	}
	a.Time = getNewTime(value.Time, dInterval, time.UTC)
	a.Timebucket = dInterval
	return nil
}

// Result is for timebucket
func (a *TimeBucketAggregate) Result() (tree.Datum, error) {
	return &tree.DTimestamp{Time: a.Time}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *TimeBucketAggregate) Reset(context.Context) {
	a.Timebucket = tree.DInterval{}
}

// Close is part of the tree.AggregateFunc interface.
func (a *TimeBucketAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *TimeBucketAggregate) Size() int64 {
	return sizeOfTimeBucketAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *TimeBucketAggregate) AggHandling() {
	// do nothing
}

// TimestamptzBucketAggregate is for timebucketagg
type TimestamptzBucketAggregate struct {
	Time       time.Time
	Timebucket tree.DInterval
	loc        *time.Location
}

func newTimestamptzBucketAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &TimestamptzBucketAggregate{loc: evalCtx.GetLocation()}
}

// Add is for timebucket
func (a *TimestamptzBucketAggregate) Add(
	_ context.Context, datum1 tree.Datum, datum2 ...tree.Datum,
) error {
	if _, ok := datum1.(tree.DNullExtern); ok {
		return pgerror.New(pgcode.InvalidParameterValue, "first arg can not be null")
	}
	value, ok := datum1.(*tree.DTimestampTZ)
	if !ok {
		return pgerror.New(pgcode.InvalidParameterValue, "first arg is error")
	}
	dInterval, err := GetTimeInterval(datum2[0])
	if err != nil {
		return err
	}
	a.Time = getNewTime(value.Time, dInterval, a.loc)
	a.Timebucket = dInterval
	return nil
}

// getNewTime returns the result of rounding oldTime down to a multiple of dInterval.
func getNewTime(oldTime time.Time, dInterval tree.DInterval, loc *time.Location) time.Time {
	newTime := time.Date(1, 1, 1, 0, 0, 0, 0, loc)
	if dInterval.Months != 0 {
		timeMonth := (oldTime.Year()-1)*12 + (int(oldTime.Month() - 1))
		newMonth := (int64(timeMonth) / dInterval.Months) * dInterval.Months
		newTime = newTime.AddDate(0, int(newMonth), 0)
		return newTime
	}
	var offSet int
	if loc != nil && loc != time.UTC {
		timeInLocation := oldTime.In(loc)
		_, offSet = timeInLocation.Zone()
		oldTime = oldTime.Add(time.Duration(offSet) * time.Second)
	}
	if dInterval.Days != 0 {
		newTime = oldTime.Truncate(time.Duration(dInterval.Days) * time.Hour * 24)
	}
	if dInterval.Nanos() != 0 {
		newTime = oldTime.Truncate(time.Duration(dInterval.Nanos()))
	}
	if loc != nil && loc != time.UTC {
		newTime = newTime.Add(-time.Duration(offSet) * time.Second)
	}
	return newTime
}

// GetTimeInterval converts input string or int value to interval.
// input:input datum for time_bucket or time_bucket_gapfill.
// output:DInterval converted by string or int.
func GetTimeInterval(datum tree.Datum) (tree.DInterval, error) {
	var valueStr string
	switch d := datum.(type) {
	case *tree.DString:
		valueStr = string(*d)
	case *tree.DInt:
		valueStr = strconv.Itoa(int(*d)) + "s"
	default:
		return tree.DInterval{}, pgerror.New(pgcode.InvalidParameterValue, "second arg is error")
	}
	// convert a string to a interval.
	dInterval, err := tree.ParseDInterval(valueStr)
	if err != nil {
		return tree.DInterval{}, err
	}
	if dInterval.AsFloat64() <= 0 {
		return tree.DInterval{}, pgerror.New(pgcode.InvalidParameterValue, "second arg should be a positive interval")
	}
	if err = checkTimeUnit(valueStr); err != nil {
		return tree.DInterval{}, err
	}
	return *dInterval, nil
}

// checkTimeUnit check input interval of time_bucket or time_bucket_gapfill is a composite time.
// only support positive integer and year(s)/yrs/yr/y, month(s)/mons/mon, week(s)/w,
// day(s)/d, hour(s)/hrs/hr/h, minute(s)/mins/min/m, second(s)/secs/sec/s,
// e.g., '2year', '3mon'.
func checkTimeUnit(str string) error {
	replaceMap := map[string]string{
		"milliseconds": "ms", "millisecond": "ms", "msecs": "ms", "msec": "ms",
		"second": "s", "secs": "s", "sec": "s", "seconds": "s",
		"minute": "m", "mins": "m", "min": "m", "minutes": "m",
		"hour": "h", "hrs": "h", "hr": "h", "hours": "h",
		"day": "d", "days": "d", "week": "w", "weeks": "w",
		"month": "n", "months": "n", "mon": "n", "mons": "n",
		"year": "y", "years": "y", "yrs": "y", "yr": "y",
	}
	replaceSlice := []string{
		"milliseconds", "millisecond", "msecs", "msec",
		"minutes", "seconds",
		"minute", "second", "months",
		"hours", "weeks", "years", "month",
		"secs", "mins", "hour", "days", "week", "mons", "year",
		"sec", "min", "hrs", "day", "mon", "yrs",
		"hr", "yr",
	}
	newStr := strings.ToLower(str)
	for _, unitOld := range replaceSlice {
		unitNew := replaceMap[unitOld]
		newStr = strings.ReplaceAll(newStr, unitOld, unitNew)
	}
	// the input of time interval must contain num value and unit.
	if len(newStr) < 2 {
		return pgerror.New(pgcode.InvalidParameterValue, "invalid input interval time")
	}
	unit := newStr[len(newStr)-1]
	if unit != 'y' && unit != 'n' && unit != 'w' && unit != 'd' && unit != 'h' && unit != 'm' && unit != 's' {
		return pgerror.New(pgcode.InvalidParameterValue, "wrong interval time unit for time_bucket or time_bucket_gapfill")
	}
	var numVal string
	if len(newStr) == 2 {
		numVal = string(newStr[0])
	} else {
		numVal = newStr[:len(newStr)-2]
	}
	// unsupported composite time.
	for _, s := range numVal {
		if s > '9' || s < '0' {
			return pgerror.New(pgcode.InvalidParameterValue, "invalid input interval time")
		}
	}
	return nil
}

func parseIntervalForSessionWindow(str string) (time.Duration, error) {
	replaceMap := map[string]string{
		"second": "s", "secs": "s", "sec": "s", "seconds": "s",
		"minute": "m", "mins": "m", "min": "m", "minutes": "m",
		"hour": "h", "hrs": "h", "hr": "h", "hours": "h",
		"day": "d", "days": "d",
		"week": "w", "weeks": "w",
	}
	replaceSlice := []string{
		"minutes", "seconds", "minute", "second",
		"hours", "secs", "mins", "hour", "days",
		"sec", "min", "hrs", "day", "hr",
		"weeks", "week",
	}
	newStr := strings.ToLower(str)
	for _, unitOld := range replaceSlice {
		unitNew := replaceMap[unitOld]
		newStr = strings.ReplaceAll(newStr, unitOld, unitNew)
	}
	// the input of time interval must contain num value and unit.
	if len(newStr) < 2 {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, "invalid input interval time (%s) for session_window", str)
	}
	var numVal string
	if len(newStr) == 2 {
		numVal = string(newStr[0])
	} else {
		numVal = newStr[:len(newStr)-1]
	}
	// unsupported composite time.
	d, err := strconv.Atoi(numVal)
	if err != nil || d < 0 {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, "invalid input interval time (%s) for session_window.", str)
	}
	unit := newStr[len(newStr)-1]
	var dur time.Duration
	switch unit {
	case 's':
		dur = time.Duration(d) * time.Second
	case 'm':
		dur = time.Duration(d) * time.Minute
	case 'h':
		dur = time.Duration(d) * time.Hour
	case 'd':
		dur = time.Duration(d) * time.Hour * 24
	case 'w':
		dur = time.Duration(d) * time.Hour * 24 * 7
	default:
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, "invalid input interval time (%s) for session_window.", str)
	}
	return dur, nil
}

// Result is for timebucket
func (a *TimestamptzBucketAggregate) Result() (tree.Datum, error) {
	return &tree.DTimestampTZ{Time: a.Time}, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *TimestamptzBucketAggregate) Reset(context.Context) {
	a.Timebucket = tree.DInterval{}
}

// Close is part of the tree.AggregateFunc interface.
func (a *TimestamptzBucketAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *TimestamptzBucketAggregate) Size() int64 {
	return sizeOfTimestamptzBucketAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *TimestamptzBucketAggregate) AggHandling() {
	// do nothing
}

// ImputationAggregate is for interpolateagg
type ImputationAggregate struct {
	Aggfunc    tree.AggregateFunc
	Time       time.Time
	Timebucket int64
	Exp        ImputationMethod

	OriginalType    *types.T
	IntConstant     int64
	FloatConstant   float64
	DecimalConstant *tree.DDecimal
}

// ImputationMethod is for interpolate method
type ImputationMethod int

const (
	//NullMethod is for interpolate method
	NullMethod ImputationMethod = iota
	//ConstantIntMethod is for interpolate method
	ConstantIntMethod
	//ConstantFloatMethod is for interpolate method
	ConstantFloatMethod
	//ConstantDecimalMethod is for interpolate method
	ConstantDecimalMethod
	//PrevMethod is for interpolate method
	PrevMethod
	//NextMethod is for interpolate method
	NextMethod
	//LinearMethod is for interpolate method
	LinearMethod
)

func newImputationAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &ImputationAggregate{}
}

// Add is for interpolate agg
func (a *ImputationAggregate) Add(
	ctx context.Context, datum1 tree.Datum, datum2 ...tree.Datum,
) error {
	if datum1 != tree.DNull {
		a.OriginalType = datum1.ResolvedType()
		if a.OriginalType == types.Decimal {
			return errors.Newf("interpolate not support originalColType is decimal.")
		}
	}
	//fmt.Printf("arg1 %T, arg2 %T\n", datum1.ResolvedType(), datum2[0].ResolvedType())
	switch v := datum2[0].(type) {
	case *tree.DInt:
		a.IntConstant = int64(*v)
		a.Exp = ConstantIntMethod
	case *tree.DFloat:
		a.FloatConstant = float64(*v)
		a.Exp = ConstantFloatMethod
	case *tree.DString:
		method := string(*v)
		if method == "null" {
			a.Exp = NullMethod
		} else if method == "prev" {
			a.Exp = PrevMethod
		} else if method == "next" {
			a.Exp = NextMethod
		} else if method == "linear" {
			a.Exp = LinearMethod
		} else {
			return errors.New("Interpolate/imputation method not supported, please check spelling")
		}
	case *tree.DDecimal:
		a.DecimalConstant = v
		a.Exp = ConstantDecimalMethod
	default:
		return errors.New("Second argument type must be int, float or string")
	}
	if len(datum2) > 1 {
		datum2 = datum2[1:]
	}
	return a.Aggfunc.Add(ctx, datum1, datum2...)
}

// Result is for interpolate agg
func (a *ImputationAggregate) Result() (tree.Datum, error) {
	return a.Aggfunc.Result()
}

// Reset implements tree.AggregateFunc interface.
func (a *ImputationAggregate) Reset(ctx context.Context) {
	a.Timebucket = 0
	if a.Aggfunc != nil {
		a.Aggfunc.Reset(ctx)
	}
}

// Close is part of the tree.AggregateFunc interface.
func (a *ImputationAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *ImputationAggregate) Size() int64 {
	return sizeOfImputationAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *ImputationAggregate) AggHandling() {
	// do nothing
}

type decimalSqrDiffAggregate struct {
	singleDatumAggregateBase

	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal
}

func newDecimalSqrDiff(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSqrDiffAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		ed:                       &ed,
	}
}

func newDecimalSqrDiffAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return newDecimalSqrDiff(evalCtx)
}

// Count is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSqrDiff interface.
func (a *decimalSqrDiffAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

func (a *decimalSqrDiffAggregate) Add(
	ctx context.Context, datum tree.Datum, _ ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	d := &datum.(*tree.DDecimal).Decimal

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.ed.Add(&a.count, &a.count, decimalOne)
	a.ed.Sub(&a.delta, d, &a.mean)
	a.ed.Quo(&a.tmp, &a.delta, &a.count)
	a.ed.Add(&a.mean, &a.mean, &a.tmp)
	a.ed.Sub(&a.tmp, d, &a.mean)
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, a.ed.Mul(&a.delta, &a.delta, &a.tmp))

	size := int64(tree.SizeOfDecimal(a.count) +
		tree.SizeOfDecimal(a.mean) +
		tree.SizeOfDecimal(a.sqrDiff) +
		tree.SizeOfDecimal(a.delta) +
		tree.SizeOfDecimal(a.tmp))
	if err := a.updateMemoryUsage(ctx, size); err != nil {
		return err
	}

	return a.ed.Err()
}

func (a *decimalSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	dd.Set(&a.sqrDiff)
	// Remove trailing zeros. Depending on the order in which the input
	// is processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Reset(ctx context.Context) {
	a.count.SetFinite(0, 0)
	a.mean.SetFinite(0, 0)
	a.sqrDiff.SetFinite(0, 0)
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) Size() int64 {
	return sizeOfDecimalSqrDiffAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *decimalSqrDiffAggregate) AggHandling() {
	// do nothing
}

type floatSumSqrDiffsAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSumSqrDiffs() floatSqrDiff {
	return &floatSumSqrDiffsAggregate{}
}

func (a *floatSumSqrDiffsAggregate) Count() int64 {
	return a.count
}

// The signature for the datums is:
//
//	SQRDIFF (float), SUM (float), COUNT(int)
func (a *floatSumSqrDiffsAggregate) Add(
	_ context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}

	sqrDiff := float64(*sqrDiffD.(*tree.DFloat))
	sum := float64(*sumD.(*tree.DFloat))
	count := int64(*countD.(*tree.DInt))

	mean := sum / float64(count)
	delta := mean - a.mean

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://gitee.com/kwbasedb/kwbase/pull/17728.
	totalCount, ok := arith.AddWithOverflow(a.count, count)
	if !ok {
		return pgerror.Newf(pgcode.NumericValueOutOfRange,
			"number of values in aggregate exceed max count of %d", math.MaxInt64,
		)
	}
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.sqrDiff += sqrDiff + delta*delta*float64(count)*float64(a.count)/float64(totalCount)
	a.count = totalCount
	a.mean += delta * float64(count) / float64(a.count)
	return nil
}

func (a *floatSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Reset(context.Context) {
	a.count = 0
	a.mean = 0
	a.sqrDiff = 0
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) Size() int64 {
	return sizeOfFloatSumSqrDiffsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *floatSumSqrDiffsAggregate) AggHandling() {
	// do nothing
}

type decimalSumSqrDiffsAggregate struct {
	singleDatumAggregateBase

	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	tmpCount apd.Decimal
	tmpMean  apd.Decimal
	delta    apd.Decimal
	tmp      apd.Decimal
}

func newDecimalSumSqrDiffs(evalCtx *tree.EvalContext) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	return &decimalSumSqrDiffsAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		ed:                       &ed,
	}
}

// Count is part of the decimalSqrDiff interface.
func (a *decimalSumSqrDiffsAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSumSqrDiffs interface.
func (a *decimalSumSqrDiffsAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

func (a *decimalSumSqrDiffsAggregate) Add(
	ctx context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}
	sqrDiff := &sqrDiffD.(*tree.DDecimal).Decimal
	sum := &sumD.(*tree.DDecimal).Decimal
	a.tmpCount.SetInt64(int64(*countD.(*tree.DInt)))

	a.ed.Quo(&a.tmpMean, sum, &a.tmpCount)
	a.ed.Sub(&a.delta, &a.tmpMean, &a.mean)

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://gitee.com/kwbasedb/kwbase/pull/17728.

	// This is logically equivalent to
	//   sqrDiff + delta * delta * tmpCount * a.count / (tmpCount + a.count)
	// where the expression is computed from RIGHT to LEFT.
	a.ed.Add(&a.tmp, &a.tmpCount, &a.count)
	a.ed.Quo(&a.tmp, &a.count, &a.tmp)
	a.ed.Mul(&a.tmp, &a.tmpCount, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Add(&a.tmp, sqrDiff, &a.tmp)
	// Update running squared difference.
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, &a.tmp)

	// Update total count.
	a.ed.Add(&a.count, &a.count, &a.tmpCount)

	// This is logically equivalent to
	//   delta * tmpCount / a.count
	// where the expression is computed from LEFT to RIGHT.
	// Note `a.count` is now the total count (includes tmpCount).
	a.ed.Mul(&a.tmp, &a.delta, &a.tmpCount)
	a.ed.Quo(&a.tmp, &a.tmp, &a.count)
	// Update running mean.
	a.ed.Add(&a.mean, &a.mean, &a.tmp)

	size := int64(tree.SizeOfDecimal(a.count) +
		tree.SizeOfDecimal(a.mean) +
		tree.SizeOfDecimal(a.sqrDiff) +
		tree.SizeOfDecimal(a.tmpCount) +
		tree.SizeOfDecimal(a.tmpMean) +
		tree.SizeOfDecimal(a.delta) +
		tree.SizeOfDecimal(a.tmp))
	if err := a.updateMemoryUsage(ctx, size); err != nil {
		return err
	}

	return a.ed.Err()
}

func (a *decimalSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Reset(ctx context.Context) {
	a.count.SetFinite(0, 0)
	a.mean.SetFinite(0, 0)
	a.sqrDiff.SetFinite(0, 0)
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) Size() int64 {
	return sizeOfDecimalSumSqrDiffsAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *decimalSumSqrDiffsAggregate) AggHandling() {
	// do nothing
}

type floatSqrDiff interface {
	tree.AggregateFunc
	Count() int64
}

type decimalSqrDiff interface {
	tree.AggregateFunc
	Count() *apd.Decimal
	Tmp() *apd.Decimal
}

type floatVarianceAggregate struct {
	agg floatSqrDiff
}

type decimalVarianceAggregate struct {
	agg decimalSqrDiff
}

type floatPopVarianceAggregate struct {
	agg floatSqrDiff
}

type decimalPopVarianceAggregate struct {
	agg decimalSqrDiff
}

// Both Variance and FinalVariance aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that Variance employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalVariance employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalVariance is used for local/final aggregation in distsql.
func newIntVarianceAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newIntSqrDiff(evalCtx)}
}

func newFloatVarianceAggregate(
	_ []*types.T, _ *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSqrDiff()}
}

func newDecimalVarianceAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

func newFloatFinalVarianceAggregate(
	_ []*types.T, _ *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatVarianceAggregate{agg: newFloatSumSqrDiffs()}
}

func newDecimalFinalVarianceAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalVarianceAggregate{agg: newDecimalSumSqrDiffs(evalCtx)}
}

func newIntPopVarianceAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalPopVarianceAggregate{agg: newIntSqrDiff(evalCtx)}
}

func newFloatPopVarianceAggregate(
	_ []*types.T, _ *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatPopVarianceAggregate{agg: newFloatSqrDiff()}
}

func newDecimalPopVarianceAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalPopVarianceAggregate{agg: newDecimalSqrDiff(evalCtx)}
}

// Add is part of the tree.AggregateFunc interface.
//
//	Variance: VALUE(float)
//	FinalVariance: SQRDIFF(float), SUM(float), COUNT(int)
func (a *floatVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
//
//	Variance: VALUE(int|decimal)
//	FinalVariance: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *decimalVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
func (a *floatPopVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
func (a *decimalPopVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result calculates the variance from the member square difference aggregator.
func (a *floatVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count() < 2 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	return tree.NewDFloat(tree.DFloat(float64(*sqrDiff.(*tree.DFloat)) / (float64(a.agg.Count()) - 1))), nil
}

// Result calculates the variance from the member square difference aggregator.
func (a *decimalVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count().Cmp(decimalTwo) < 0 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if _, err = tree.IntermediateCtx.Sub(a.agg.Tmp(), a.agg.Count(), decimalOne); err != nil {
		return nil, err
	}
	dd := &tree.DDecimal{}
	if _, err = tree.DecimalCtx.Quo(&dd.Decimal, &sqrDiff.(*tree.DDecimal).Decimal, a.agg.Tmp()); err != nil {
		return nil, err
	}
	// Remove trailing zeros. Depending on the order in which the input is
	// processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of
	// order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Result calculates the population variance.
func (a *floatPopVarianceAggregate) Result() (tree.Datum, error) {
	// Population variance requires at least 1 data point.
	if a.agg.Count() < 1 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil || sqrDiff == tree.DNull {
		return sqrDiff, err
	}

	// Divisor is N for population variance.
	divisor := float64(a.agg.Count())
	if divisor == 0 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(float64(*sqrDiff.(*tree.DFloat)) / divisor)), nil
}

// Result calculates the population variance.
func (a *decimalPopVarianceAggregate) Result() (tree.Datum, error) {
	// Population variance requires at least 1 data point.
	if a.agg.Count().Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil || sqrDiff == tree.DNull {
		return sqrDiff, err
	}

	// Divisor is N for population variance.
	divisor := a.agg.Count()
	if divisor.IsZero() {
		return tree.DNull, nil
	}

	dd := &tree.DDecimal{}
	if _, err = tree.DecimalCtx.Quo(&dd.Decimal, &sqrDiff.(*tree.DDecimal).Decimal, divisor); err != nil {
		return nil, err
	}
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) Size() int64 {
	return sizeOfFloatVarianceAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *floatVarianceAggregate) AggHandling() {
	// do nothing
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) Size() int64 {
	return sizeOfDecimalVarianceAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *decimalVarianceAggregate) AggHandling() {
	// do nothing
}

// Reset implements tree.AggregateFunc.
func (a *floatPopVarianceAggregate) Reset(ctx context.Context)   { a.agg.Reset(ctx) }
func (a *decimalPopVarianceAggregate) Reset(ctx context.Context) { a.agg.Reset(ctx) }

// Close implements tree.AggregateFunc.
func (a *floatPopVarianceAggregate) Close(ctx context.Context)   { a.agg.Close(ctx) }
func (a *decimalPopVarianceAggregate) Close(ctx context.Context) { a.agg.Close(ctx) }

// Size implements tree.AggregateFunc.
func (a *floatPopVarianceAggregate) Size() int64 {
	return sizeOfFloatPopVarianceAggregate + a.agg.Size()
}
func (a *decimalPopVarianceAggregate) Size() int64 {
	return sizeOfDecimalPopVarianceAggregate + a.agg.Size()
}

// AggHandling implements tree.AggregateFunc.
func (a *floatPopVarianceAggregate) AggHandling()   {}
func (a *decimalPopVarianceAggregate) AggHandling() {}

type floatStdDevAggregate struct {
	agg tree.AggregateFunc
}

type decimalStdDevAggregate struct {
	agg tree.AggregateFunc
}

// Both StdDev and FinalStdDev aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that StdDev employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalStdDev employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalStdDev is used for local/final aggregation in distsql.
func newIntStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newIntVarianceAggregate(params, evalCtx, arguments)}
}

func newFloatStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatVarianceAggregate(params, evalCtx, arguments)}
}

func newDecimalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalVarianceAggregate(params, evalCtx, arguments)}
}

func newFloatFinalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &floatStdDevAggregate{agg: newFloatFinalVarianceAggregate(params, evalCtx, arguments)}
}

func newDecimalFinalStdDevAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &decimalStdDevAggregate{agg: newDecimalFinalVarianceAggregate(params, evalCtx, arguments)}
}

// Add implements the tree.AggregateFunc interface.
// The signature of the datums is:
//
//	StdDev: VALUE(float)
//	FinalStdDev: SQRDIFF(float), SUM(float), COUNT(int)
func (a *floatStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
// The signature of the datums is:
//
//	StdDev: VALUE(int|decimal)
//	FinalStdDev: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *decimalStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result computes the square root of the variance aggregator.
func (a *floatStdDevAggregate) Result() (tree.Datum, error) {
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	return tree.NewDFloat(tree.DFloat(math.Sqrt(float64(*variance.(*tree.DFloat))))), nil
}

// Result computes the square root of the variance aggregator.
func (a *decimalStdDevAggregate) Result() (tree.Datum, error) {
	// TODO(richardwu): both decimalVarianceAggregate and
	// finalDecimalVarianceAggregate return a decimal result with
	// default tree.DecimalCtx precision. We want to be able to specify that the
	// varianceAggregate use tree.IntermediateCtx (with the extra precision)
	// since it is returning an intermediate value for stdDevAggregate (of
	// which we take the Sqrt).
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	varianceDec := variance.(*tree.DDecimal)
	_, err = tree.DecimalCtx.Sqrt(&varianceDec.Decimal, &varianceDec.Decimal)
	return varianceDec, err
}

// Reset implements tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) Size() int64 {
	return sizeOfFloatStdDevAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *floatStdDevAggregate) AggHandling() {
	// do nothing
}

// Reset implements tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Reset(ctx context.Context) {
	a.agg.Reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) Size() int64 {
	return sizeOfDecimalStdDevAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *decimalStdDevAggregate) AggHandling() {
	// do nothing
}

type bytesXorAggregate struct {
	singleDatumAggregateBase

	sum        []byte
	sawNonNull bool
}

func newBytesXorAggregate(
	_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &bytesXorAggregate{singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx)}
}

// Add inserts one value into the running xor.
func (a *bytesXorAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := []byte(*datum.(*tree.DBytes))
	if !a.sawNonNull {
		if err := a.updateMemoryUsage(ctx, int64(len(t))); err != nil {
			return err
		}
		a.sum = append([]byte(nil), t...)
	} else if len(a.sum) != len(t) {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"arguments to xor must all be the same length %d vs %d", len(a.sum), len(t),
		)
	} else {
		for i := range t {
			a.sum[i] = a.sum[i] ^ t[i]
		}
	}
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *bytesXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDBytes(tree.DBytes(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *bytesXorAggregate) Reset(ctx context.Context) {
	a.sum = nil
	a.sawNonNull = false
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) Size() int64 {
	return sizeOfBytesXorAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *bytesXorAggregate) AggHandling() {
	// do nothing
}

type intXorAggregate struct {
	sum        int64
	sawNonNull bool
}

func newIntXorAggregate(_ []*types.T, _ *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &intXorAggregate{}
}

// Add inserts one value into the running xor.
func (a *intXorAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	x := int64(*datum.(*tree.DInt))
	a.sum = a.sum ^ x
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *intXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *intXorAggregate) Reset(context.Context) {
	a.sum = 0
	a.sawNonNull = false
}

// Close is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) Size() int64 {
	return sizeOfIntXorAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *intXorAggregate) AggHandling() {
	// do nothing
}

type jsonAggregate struct {
	singleDatumAggregateBase

	loc        *time.Location
	builder    *json.ArrayBuilderWithCounter
	sawNonNull bool
}

func newJSONAggregate(_ []*types.T, evalCtx *tree.EvalContext, _ tree.Datums) tree.AggregateFunc {
	return &jsonAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		loc:                      evalCtx.GetLocation(),
		builder:                  json.NewArrayBuilderWithCounter(),
		sawNonNull:               false,
	}
}

// Add accumulates the transformed json into the JSON array.
func (a *jsonAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	j, err := tree.AsJSON(datum, a.loc)
	if err != nil {
		return err
	}
	a.builder.Add(j)
	if err = a.updateMemoryUsage(ctx, int64(a.builder.Size())); err != nil {
		return err
	}
	a.sawNonNull = true
	return nil
}

// Result returns an DJSON from the array of JSON.
func (a *jsonAggregate) Result() (tree.Datum, error) {
	if a.sawNonNull {
		return tree.NewDJSON(a.builder.Build()), nil
	}
	return tree.DNull, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *jsonAggregate) Reset(ctx context.Context) {
	a.builder = json.NewArrayBuilderWithCounter()
	a.sawNonNull = false
	a.reset(ctx)
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *jsonAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *jsonAggregate) Size() int64 {
	return sizeOfJSONAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *jsonAggregate) AggHandling() {
	// do nothing
}

// ElapsedAggregate stores the state of the aggregate calculation
// for the continuous time length.
type ElapsedAggregate struct {
	StartTime int64
	EndTime   int64

	TimeUnit        tree.Datum
	HasReceivedData bool
	Precision       int64
}

// newElapsedAggregate is used to create and initialize a TwaAggregate object.
func newElapsedAggregate(
	_ []*types.T, _ *tree.EvalContext, otherDatums tree.Datums,
) tree.AggregateFunc {
	elapsedAgg := &ElapsedAggregate{}
	if len(otherDatums) == 1 && otherDatums[0] != tree.DNull {
		elapsedAgg.TimeUnit = otherDatums[0]
	} else if len(otherDatums) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(otherDatums)))
	}

	return elapsedAgg
}

// Add is for elapsed agg
func (a *ElapsedAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	var tsKey int64
	switch t := datum.(type) {
	case *tree.DTimestamp:
		if a.Precision == 3 {
			tsKey = t.UnixMilli()
		} else if a.Precision == 6 {
			tsKey = t.UnixMicro()
		} else {
			tsKey = t.UnixNano()
		}
	case *tree.DTimestampTZ:
		if a.Precision == 3 {
			tsKey = t.UnixMilli()
		} else if a.Precision == 6 {
			tsKey = t.UnixMicro()
		} else {
			tsKey = t.UnixNano()
		}
	default:
		return pgerror.New(pgcode.DatatypeMismatch, "invalid first parameter type in elapsed function")
	}

	if !a.HasReceivedData {
		// Set the UpperInterval/DownInterval if it's not yet set
		a.StartTime = tsKey
		a.EndTime = tsKey
		a.HasReceivedData = true
	} else {
		// Set the UpperInterval/DownInterval to the latest datum
		if a.StartTime > tsKey {
			a.StartTime = tsKey
		}

		if a.EndTime < tsKey {
			a.EndTime = tsKey
		}
	}

	return nil
}

// Result is for elapsed agg
func (a *ElapsedAggregate) Result() (tree.Datum, error) {
	var result float64
	timeUnit, _ := a.TimeUnit.(*tree.DInterval)

	elapsedTime := a.EndTime - a.StartTime
	switch timeUnit.String() {
	case "'00:00:00.000000001'":
		if a.Precision == 3 {
			result = float64(elapsedTime) * float64(time.Millisecond)
		} else if a.Precision == 6 {
			result = float64(elapsedTime) * float64(time.Microsecond)
		} else {
			result = float64(elapsedTime)
		}
	case "'00:00:00.000001'":
		if a.Precision == 3 {
			result = float64(elapsedTime) * float64(time.Microsecond)
		} else if a.Precision == 6 {
			result = float64(elapsedTime)
		} else {
			result = float64(elapsedTime) / float64(time.Microsecond)
		}
	case "'00:00:00.001'":
		if a.Precision == 3 {
			result = float64(elapsedTime)
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / float64(time.Microsecond)
		} else {
			result = float64(elapsedTime) / float64(time.Millisecond)
		}
	case "'00:00:01'":
		if a.Precision == 3 {
			result = float64(elapsedTime) / float64(time.Microsecond)
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / float64(time.Millisecond)
		} else {
			result = float64(elapsedTime) / float64(time.Second)
		}
	case "'00:01:00'":
		if a.Precision == 3 {
			result = float64(elapsedTime) / (60 * float64(time.Microsecond))
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / (60 * float64(time.Millisecond))
		} else {
			result = float64(elapsedTime) / (60 * float64(time.Second))
		}
	case "'01:00:00'":
		if a.Precision == 3 {
			result = float64(elapsedTime) / (60 * 60 * float64(time.Microsecond))
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / (60 * 60 * float64(time.Millisecond))
		} else {
			result = float64(elapsedTime) / (60 * 60 * float64(time.Second))
		}
	case "'1 day'":
		if a.Precision == 3 {
			result = float64(elapsedTime) / (24 * 60 * 60 * float64(time.Microsecond))
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / (24 * 60 * 60 * float64(time.Millisecond))
		} else {
			result = float64(elapsedTime) / (24 * 60 * 60 * float64(time.Second))
		}
	case "'7 days'":
		if a.Precision == 3 {
			result = float64(elapsedTime) / (7 * 24 * 60 * 60 * float64(time.Microsecond))
		} else if a.Precision == 6 {
			result = float64(elapsedTime) / (7 * 24 * 60 * 60 * float64(time.Millisecond))
		} else {
			result = float64(elapsedTime) / (7 * 24 * 60 * 60 * float64(time.Second))
		}
	default:
		return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid time unit in elapsed function")
	}

	return tree.NewDFloat(tree.DFloat(result)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *ElapsedAggregate) Reset(ctx context.Context) {}

// Close is part of the tree.AggregateFunc interface.
func (a *ElapsedAggregate) Close(ctx context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *ElapsedAggregate) Size() int64 {
	return sizeOfElapsedAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *ElapsedAggregate) AggHandling() {
	// do nothing
}

// TwaAggregate is used to store the state of a time-weighted average (TWA) aggregate calculation.
type TwaAggregate struct {
	TotalArea       float64
	StartTime       int64
	EndTime         int64
	LastVal         float64
	HasReceivedData bool
	ConstVal        tree.Datum
	Precision       int64
}

// newTwaAggregate is used to create and initialize a TwaAggregate object.
func newTwaAggregate(_ []*types.T, _ *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
	twaAgg := &TwaAggregate{}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		twaAgg.ConstVal = arguments[0]
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}

	return twaAgg
}

// Add calculates the weighted value and calculates the start/end timestamp
func (a *TwaAggregate) Add(_ context.Context, datum tree.Datum, otherDatum ...tree.Datum) error {
	if (len(otherDatum) > 0 && otherDatum[0] == tree.DNull) || a.ConstVal != nil {
		return nil
	}

	var tsKey int64
	switch t := datum.(type) {
	case *tree.DTimestamp:
		if a.Precision == 3 {
			tsKey = t.UnixMilli()
		} else if a.Precision == 6 {
			tsKey = t.UnixMicro()
		} else {
			tsKey = t.UnixNano()
		}
	case *tree.DTimestampTZ:
		if a.Precision == 3 {
			tsKey = t.UnixMilli()
		} else if a.Precision == 6 {
			tsKey = t.UnixMicro()
		} else {
			tsKey = t.UnixNano()
		}
	default:
		return pgerror.New(pgcode.DatatypeMismatch, "invalid first parameter type in twa function")
	}

	var tsVal float64
	switch otherDatum[0].(type) {
	case *tree.DInt:
		tsVal = float64(tree.MustBeDInt(otherDatum[0]))
	case *tree.DFloat:
		tsVal = float64(tree.MustBeDFloat(otherDatum[0]))
	default:
		return pgerror.New(pgcode.DatatypeMismatch, "invalid second parameter type in twa function")
	}

	// Calculate area if not the first data point
	if !a.HasReceivedData {
		a.StartTime = tsKey
		a.HasReceivedData = true
	} else {
		a.TotalArea += twaGetArea(a.EndTime, tsKey, a.LastVal, tsVal)
	}

	if a.EndTime == tsKey {
		return pgerror.New(pgcode.InvalidParameterValue, "duplicate timestamps not allowed in twa function")
	}

	// Update last timestamp and value
	a.EndTime = tsKey
	a.LastVal = tsVal

	return nil
}

// Result calculates and returns the result of a time-weighted average
func (a *TwaAggregate) Result() (tree.Datum, error) {
	if a.ConstVal != nil {
		switch a.ConstVal.(type) {
		case *tree.DInt:
			return tree.NewDFloat(tree.DFloat(tree.MustBeDInt(a.ConstVal))), nil
		case *tree.DFloat:
			return tree.NewDFloat(tree.MustBeDFloat(a.ConstVal)), nil
		default:
			return nil, pgerror.New(pgcode.DatatypeMismatch, "invalid second parameter type in twa function")
		}
	}

	if !a.HasReceivedData {
		return tree.DNull, nil
	}

	var twaRes float64
	if a.StartTime == a.EndTime {
		twaRes = a.LastVal
	} else {
		twaRes = a.TotalArea / (2 * float64(a.EndTime-a.StartTime))
	}

	return tree.NewDFloat(tree.DFloat(twaRes)), nil
}

// Reset implements tree.AggregateFunc interface.
func (a *TwaAggregate) Reset(ctx context.Context) {}

// Close is part of the tree.AggregateFunc interface.
func (a *TwaAggregate) Close(ctx context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *TwaAggregate) Size() int64 {
	return sizeOfTwaAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *TwaAggregate) AggHandling() {
	// do nothing
}

// twaGetArea calculates the time-weighted value area based on the start and end timestamps
// and corresponding values
func twaGetArea(sKey, eKey int64, sVal, eVal float64) float64 {
	// If sVal/eVal is not a number value, return NaN
	if math.IsNaN(sVal) || math.IsNaN(eVal) {
		return math.NaN()
	}

	if (sVal >= 0 && eVal >= 0) || (sVal <= 0 && eVal <= 0) {
		deltaT := eKey - sKey
		return (sVal + eVal) * float64(deltaT)
	}

	// Using big.Float for high precision calculation
	sKeyBig, eKeyBig := new(big.Float).SetFloat64(float64(sKey)), new(big.Float).SetFloat64(float64(eKey))
	sValBig, eValBig := new(big.Float).SetFloat64(sVal), new(big.Float).SetFloat64(eVal)

	// Calculate x
	numerator := new(big.Float).Sub(new(big.Float).Mul(sKeyBig, eValBig), new(big.Float).Mul(eKeyBig, sValBig))
	denominator := new(big.Float).Sub(eValBig, sValBig)
	x := new(big.Float).Quo(numerator, denominator)

	// Calculate the final result
	part1 := new(big.Float).Mul(sValBig, new(big.Float).Sub(x, sKeyBig))
	part2 := new(big.Float).Mul(eValBig, new(big.Float).Sub(eKeyBig, x))
	result := new(big.Float).Add(part1, part2)

	// Convert result to float64
	res, _ := result.Float64()
	return res
}

// MaxExtendAggregate keeps track of the largest value passed to Add.
type MaxExtendAggregate struct {
	singleDatumAggregateBase

	max               tree.Datum
	result            tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMaxExtendAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "max" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &MaxExtendAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxExtendAggregate) Add(
	ctx context.Context, Datum tree.Datum, others ...tree.Datum,
) error {
	if Datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		if err := a.updateMemoryUsage(ctx, int64(Datum.Size())); err != nil {
			return err
		}
		a.max = Datum

		result := newResult(others)
		if result != nil {
			a.result = result
		}

		return nil
	}
	c := a.max.Compare(a.evalCtx, Datum)
	if c < 0 {
		a.max = Datum
		var result tree.Datum
		if len(others) == 1 {
			result = others[0]
		} else if len(others) > 1 {
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if result != nil {
			a.result = result
		}
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(Datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *MaxExtendAggregate) Result() (tree.Datum, error) {
	if a.result == nil {
		return tree.DNull, nil
	}
	return a.result, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *MaxExtendAggregate) Reset(ctx context.Context) {
	a.max = nil
	a.result = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *MaxExtendAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *MaxExtendAggregate) Size() int64 {
	return sizeOfMaxExtendAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *MaxExtendAggregate) AggHandling() {
	// do nothing
}

// MinExtendAggregate keeps track of the largest value passed to Add.
type MinExtendAggregate struct {
	singleDatumAggregateBase

	min               tree.Datum
	result            tree.Datum
	evalCtx           *tree.EvalContext
	variableDatumSize bool
}

func newMinExtendAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	_, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a variable size, the memory account will be
	// updated accordingly on every change to the current "min" value, but if
	// it has a fixed size, the memory account will be updated only on the
	// first non-null datum.
	return &MinExtendAggregate{
		singleDatumAggregateBase: makeSingleDatumAggregateBase(evalCtx),
		evalCtx:                  evalCtx,
		variableDatumSize:        variable,
	}
}

// Add sets the Min to the larger of the current Min or the passed datum.
func (a *MinExtendAggregate) Add(
	ctx context.Context, Datum tree.Datum, others ...tree.Datum,
) error {
	if Datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		if err := a.updateMemoryUsage(ctx, int64(Datum.Size())); err != nil {
			return err
		}
		a.min = Datum

		result := newResult(others)
		if result != nil {
			a.result = result
		}

		return nil
	}
	c := a.min.Compare(a.evalCtx, Datum)
	if c > 0 {
		a.min = Datum
		var result tree.Datum
		if len(others) == 1 {
			result = others[0]
		} else if len(others) > 1 {
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if result != nil {
			a.result = result
		}
		if a.variableDatumSize {
			if err := a.updateMemoryUsage(ctx, int64(Datum.Size())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *MinExtendAggregate) Result() (tree.Datum, error) {
	if a.result == nil {
		return tree.DNull, nil
	}
	return a.result, nil
}

// Reset implements tree.AggregateFunc interface.
func (a *MinExtendAggregate) Reset(ctx context.Context) {
	a.min = nil
	a.result = nil
	a.reset(ctx)
}

// Close is part of the tree.AggregateFunc interface.
func (a *MinExtendAggregate) Close(ctx context.Context) {
	a.close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *MinExtendAggregate) Size() int64 {
	return sizeOfMinExtendAggregate
}

// AggHandling is part of the tree.AggregateFunc interface.
func (a *MinExtendAggregate) AggHandling() {
	// do nothing
}

func newResult(others []tree.Datum) tree.Datum {
	var result tree.Datum
	if len(others) == 1 {
		result = others[0]
	} else if len(others) > 1 {
		panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
	}
	return result
}

// quantileAggregate stores all values in memory, sorts them, and calculates
// the requested quantile.
type quantileAggregate struct {
	evalCtx     *tree.EvalContext
	quantile    float64
	quantileSet bool
	values      []float64
	acc         mon.BoundAccount
}

// newQuantileAggregate is the constructor for the quantile aggregate function.
func newQuantileAggregate(
	params []*types.T, evalCtx *tree.EvalContext, arguments tree.Datums,
) tree.AggregateFunc {
	return &quantileAggregate{
		evalCtx: evalCtx,
		values:  make([]float64, 0),
		acc:     evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add gathers all non-null values.
func (a *quantileAggregate) Add(
	ctx context.Context, datum tree.Datum, otherArgs ...tree.Datum,
) error {
	// The quantile value is constant for all rows, so we only need to parse
	// and validate it once, on the first call to Add.
	if !a.quantileSet {
		if len(otherArgs) == 0 {
			return pgerror.New(pgcode.InvalidParameterValue, "quantile() requires a second argument for the quantile value")
		}
		quantileDatum := otherArgs[0]
		if quantileDatum == tree.DNull {
			return pgerror.New(pgcode.InvalidParameterValue, "quantile value cannot be NULL")
		}

		var qFloat float64
		switch v := tree.UnwrapDatum(a.evalCtx, quantileDatum).(type) {
		case *tree.DFloat:
			qFloat = float64(*v)
		case *tree.DInt:
			qFloat = float64(*v)
		case *tree.DDecimal:
			f, err := v.Float64()
			if err != nil {
				return pgerror.Newf(pgcode.InvalidParameterValue, "could not convert quantile to float: %v", err)
			}
			qFloat = f
		default:
			return pgerror.Newf(pgcode.DatatypeMismatch, "quantile argument must be a numeric type, not %s", quantileDatum.ResolvedType())
		}

		if qFloat < 0 || qFloat > 1 {
			return pgerror.Newf(pgcode.InvalidParameterValue, "quantile value %f is out of range [0, 1]", qFloat)
		}
		a.quantile = qFloat
		a.quantileSet = true
	}

	// Process the main value for aggregation.
	if datum == tree.DNull {
		return nil
	}

	var val float64
	switch t := tree.UnwrapDatum(a.evalCtx, datum).(type) {
	case *tree.DInt:
		val = float64(*t)
	case *tree.DFloat:
		val = float64(*t)
	case *tree.DDecimal:
		f, err := t.Float64()
		if err != nil {
			return err
		}
		val = f
	default:
		return pgerror.Newf(pgcode.DatatypeMismatch,
			"quantile() requires a numeric input for the first argument, got %s", datum.ResolvedType())
	}

	if err := a.acc.Grow(ctx, int64(unsafe.Sizeof(val))); err != nil {
		return err
	}
	a.values = append(a.values, val)
	return nil
}

// Result sorts the collected values and computes the quantile.
func (a *quantileAggregate) Result() (tree.Datum, error) {
	if len(a.values) == 0 {
		return tree.DNull, nil
	}

	sort.Float64s(a.values)

	n := float64(len(a.values))
	// Using linear interpolation method (R-7 in R's documentation).
	// h = (N-1) * q
	h := (n - 1) * a.quantile

	idx := int(h)
	frac := h - float64(idx)

	// If the index is the last element or the fractional part is zero,
	// no interpolation is needed.
	if idx >= len(a.values)-1 || frac == 0 {
		return tree.NewDFloat(tree.DFloat(a.values[idx])), nil
	}

	// Linear interpolation: v_i + (v_{i+1} - v_i) * h_frac
	lower := a.values[idx]
	upper := a.values[idx+1]
	result := lower + (upper-lower)*frac

	return tree.NewDFloat(tree.DFloat(result)), nil
}

// Reset implements the tree.AggregateFunc interface.
func (a *quantileAggregate) Reset(ctx context.Context) {
	a.values = a.values[:0]
	a.acc.Clear(ctx)
}

// Close implements the tree.AggregateFunc interface.
func (a *quantileAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size implements the tree.AggregateFunc interface.
func (a *quantileAggregate) Size() int64 {
	return sizeOfQuantileAggregate
}

// AggHandling implements the tree.AggregateFunc interface.
func (a *quantileAggregate) AggHandling() {}

type decimalNormAggregate struct {
	evalCtx      *tree.EvalContext
	sumOfSquares apd.Decimal
	sawNonNull   bool
	acc          mon.BoundAccount
}

// floatNormAggregate calculates the L2 norm for float inputs and returns a float.
type floatNormAggregate struct {
	evalCtx      *tree.EvalContext
	sumOfSquares float64
	sawNonNull   bool
	acc          mon.BoundAccount
}

func newDecimalNormAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &decimalNormAggregate{
		evalCtx: evalCtx,
		acc:     evalCtx.Mon.MakeBoundAccount(),
	}
}

func newFloatNormAggregate(
	params []*types.T, evalCtx *tree.EvalContext, _ tree.Datums,
) tree.AggregateFunc {
	return &floatNormAggregate{
		evalCtx: evalCtx,
		acc:     evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add accumulates the square of the datum into the sum.
func (a *decimalNormAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true

	var val apd.Decimal
	// Convert input datum to apd.Decimal for precise calculation.
	switch t := tree.UnwrapDatum(a.evalCtx, datum).(type) {
	case *tree.DInt:
		val.SetInt64(int64(*t))
	case *tree.DFloat:
		if _, err := val.SetFloat64(float64(*t)); err != nil {
			return err
		}
	case *tree.DDecimal:
		val.Set(&t.Decimal)
	default:
		return errors.Newf("unexpected type %s for norm()", datum.ResolvedType())
	}

	// Square the value: val * val
	squared := apd.Decimal{}
	if _, err := tree.DecimalCtx.Mul(&squared, &val, &val); err != nil {
		return err
	}

	// Add the squared value to the running sum.
	if _, err := tree.DecimalCtx.Add(&a.sumOfSquares, &a.sumOfSquares, &squared); err != nil {
		return err
	}

	// Account for memory usage.
	if err := a.acc.Grow(ctx, int64(tree.SizeOfDecimal(a.sumOfSquares))); err != nil {
		return err
	}

	return nil
}

// Add accumulates the square of the datum into the sum.
func (a *floatNormAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true
	val := float64(*datum.(*tree.DFloat))
	squared := val * val
	a.sumOfSquares += squared
	return nil
}

// Result computes the square root of the sum of squares.
func (a *decimalNormAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}

	var sqrtResult apd.Decimal
	_, err := tree.DecimalCtx.Sqrt(&sqrtResult, &a.sumOfSquares)
	if err != nil {
		return nil, err
	}
	dd := &tree.DDecimal{}
	dd.Set(&sqrtResult)

	// Return a DECIMAL to maintain precision.
	return dd, nil
}

// Result computes the square root of the sum of squares.
func (a *floatNormAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	result := math.Sqrt(a.sumOfSquares)
	return tree.NewDFloat(tree.DFloat(result)), nil
}

// Reset implements the tree.AggregateFunc interface.
func (a *decimalNormAggregate) Reset(ctx context.Context) {
	a.sawNonNull = false
	a.sumOfSquares.SetFinite(0, 0)
	a.acc.Clear(ctx)
}

// Reset implements the tree.AggregateFunc interface.
func (a *floatNormAggregate) Reset(ctx context.Context) {
	a.sawNonNull = false
	a.sumOfSquares = 0
	a.acc.Clear(ctx)
}

// Close implements the tree.AggregateFunc interface.
func (a *decimalNormAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Close implements the tree.AggregateFunc interface.
func (a *floatNormAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size implements the tree.AggregateFunc interface.
func (a *decimalNormAggregate) Size() int64 {
	return sizeOfDecimalNormAggregate
}

// Size implements the tree.AggregateFunc interface.
func (a *floatNormAggregate) Size() int64 {
	return sizeOfFloatNormAggregate
}

// AggHandling implements the tree.AggregateFunc interface.
func (a *decimalNormAggregate) AggHandling() {}

// AggHandling implements the tree.AggregateFunc interface.
func (a *floatNormAggregate) AggHandling() {}
