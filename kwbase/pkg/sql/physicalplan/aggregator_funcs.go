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

package physicalplan

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// FinalStageInfo is a wrapper around an aggregation function performed
// in the final stage of distributed aggregations that allows us to specify the
// corresponding inputs from the local aggregations by their indices in the LocalStage.
type FinalStageInfo struct {
	Fn execinfrapb.AggregatorSpec_Func
	// Specifies the ordered slice of outputs from local aggregations to propagate
	// as inputs to Fn. This must be ordered according to the underlying aggregate builtin
	// arguments signature found in aggregate_builtins.go.
	LocalIdxs []uint32
}

// DistAggregationInfo is a blueprint for planning distributed aggregations. It
// describes two stages - a local stage performs local aggregations wherever
// data is available and generates partial results, and a final stage aggregates
// the partial results from all data "partitions".
//
// The simplest example is SUM: the local stage computes the SUM of the items
// on each node, and a final stage SUMs those partial sums into a final sum.
// Similar functions are MIN, MAX, BOOL_AND, BOOL_OR.
//
// A less trivial example is COUNT: the local stage counts (COUNT), the final stage
// adds the counts (SUM_INT).
//
// A more complex example is AVG, for which we have to do *multiple*
// aggregations in each stage: we need to get a sum and a count, so the local
// stage does SUM and COUNT, and the final stage does SUM and SUM_INT. We also
// need an expression that takes these two values and generates the final AVG
// result.
type DistAggregationInfo struct {
	// The local stage consists of one or more aggregations. All aggregations have
	// the same input.
	LocalStage []FinalStageInfo

	// The final stage consists of one or more aggregations that take in an
	// arbitrary number of inputs from the local stages. The inputs are ordered and
	// mapped by the indices of the local aggregations in LocalStage (specified by
	// LocalIdxs).
	MiddleStage []FinalStageInfo
	FinalStage  []FinalStageInfo

	// An optional rendering expression used to obtain the final result; required
	// if there is more than one aggregation in each of the stages.
	//
	// Conceptually this is an expression that has access to the final stage
	// results (via IndexedVars), to be run as the PostProcessing step of the
	// final stage processor.  However, there are some complications:
	//   - this structure is a blueprint for aggregating inputs of different
	//     types, and in some cases the expression may be different depending on
	//     the types (see AVG below).
	//   - we support combining multiple "top level" aggregations into the same
	//     processors, so the correct indexing of the input variables is not
	//     predetermined.
	//
	// Instead of defining a canonical non-typed expression and then tweaking it
	// with visitors, we use a function that directly creates a typed expression
	// on demand. The expression will refer to the final stage results using
	// IndexedVars, with indices specified by varIdxs (1-1 mapping).
	FinalRendering func(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error)
}

// Convenient value for FinalStageInfo.LocalIdxs when there is only one aggregation
// function in each of the LocalStage and FinalStage. Otherwise, specify the explicit
// index corresponding to the local stage.
var passThroughLocalIdxs = []uint32{0}

// GetAvgRender get avg render expression
func GetAvgRender(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error) {
	if len(varIdxs) < 2 {
		panic(pgerror.New(pgcode.Internal, "fewer than two final aggregation values passed into final render"))
	}
	sum := h.IndexedVar(varIdxs[0])
	count := h.IndexedVar(varIdxs[1])

	expr := &tree.BinaryExpr{
		Operator: tree.Div,
		Left:     sum,
		Right:    count,
	}

	// There is no "FLOAT / INT" operator; cast the denominator to float in
	// this case. Note that there is a "DECIMAL / INT" operator, so we don't
	// need the same handling for that case.
	if sum.ResolvedType().Family() == types.FloatFamily {
		expr.Right = &tree.CastExpr{
			Expr: count,
			Type: types.Float,
		}
	}
	ctx := &tree.SemaContext{IVarContainer: h.Container()}
	return expr.TypeCheck(ctx, types.Any)
}

// DistAggregationTable is DistAggregationInfo look-up table. Functions that
// don't have an entry in the table are not optimized with a local stage.
var DistAggregationTable = map[execinfrapb.AggregatorSpec_Func]DistAggregationInfo{
	execinfrapb.AggregatorSpec_ANY_NOT_NULL: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_BIT_AND: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_BIT_OR: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BIT_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_BOOL_AND: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_BOOL_OR: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_BOOL_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_COUNT: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_COUNT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_COUNT_ROWS: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_COUNT_ROWS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_MAX: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_MIN: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},
	execinfrapb.AggregatorSpec_MAX_EXTEND: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_MAX_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_MAX_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MAX_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},
	execinfrapb.AggregatorSpec_MIN_EXTEND: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_MIN_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_MIN_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_MIN_EXTEND,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_SUM: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	execinfrapb.AggregatorSpec_XOR_AGG: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_XOR_AGG,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_XOR_AGG,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_XOR_AGG,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	// AVG is more tricky than the ones above; we need two intermediate values in
	// the local and final stages:
	//  - the local stage accumulates the SUM and the COUNT;
	//  - the final stage sums these partial results (SUM and SUM_INT);
	//  - a final rendering then divides the two results.
	//
	// At a high level, this is analogous to rewriting AVG(x) as SUM(x)/COUNT(x).
	execinfrapb.AggregatorSpec_AVG: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_COUNT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: []uint32{1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_SUM_INT,
				LocalIdxs: []uint32{1},
			},
		},
		FinalRendering: GetAvgRender,
	},

	// For VARIANCE/STDDEV the local stage consists of three aggregations,
	// and the final stage aggregation uses all three values.
	// respectively:
	//  - the local stage accumulates the SQRDIFF, SUM and the COUNT
	//  - the final stage calculates the FINAL_(VARIANCE|STDDEV)
	//
	// At a high level, this is analogous to rewriting VARIANCE(x) as
	// SQRDIFF(x)/(COUNT(x) - 1) (and STDDEV(x) as sqrt(VARIANCE(x))).
	execinfrapb.AggregatorSpec_VARIANCE: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SQRDIFF,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_COUNT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FINAL_VARIANCE,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
		// Instead of have a SUM_SQRDIFFS and SUM_INT (for COUNT) stage
		// for VARIANCE (and STDDEV) then tailoring a FinalRendering
		// stage specific to each, it is better to use a specific
		// FINAL_(VARIANCE|STDDEV) aggregation stage: - For underlying
		// Decimal results, it is not possible to reduce trailing zeros
		// since the expression is wrapped in IndexVar. Taking the
		// BinaryExpr Pow(0.5) for STDDEV would result in trailing
		// zeros which is not ideal.
		// TODO(richardwu): Consolidate FinalStage and FinalRendering:
		// have one or the other
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FINAL_VARIANCE,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},

	execinfrapb.AggregatorSpec_STDDEV: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_SQRDIFF,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_COUNT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FINAL_STDDEV,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FINAL_STDDEV,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},

	// LAST is more tricky than the ones above; we need two intermediate values in
	// the local and final stages:
	//  - the local stage accumulates the LASTTS and the LAST;
	//  - the final stage sums these partial results (SUM and SUM_INT);
	//
	// At a high level, this is analogous to rewriting LAST(x) as last(LASTTS(x), LAST(x)).
	execinfrapb.AggregatorSpec_LAST: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LASTTS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LASTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_LASTTS: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LASTTS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LASTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LASTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_LAST_ROW: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW_TS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_LAST_ROW_TS: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW_TS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_LAST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_FIRST: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRSTTS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRSTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_FIRSTTS: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRSTTS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRSTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRSTTS,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_FIRST_ROW: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW_TS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},

	execinfrapb.AggregatorSpec_FIRST_ROW_TS: {
		LocalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW,
				LocalIdxs: passThroughLocalIdxs,
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW_TS,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
		MiddleStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW,
				LocalIdxs: []uint32{0, 1},
			},
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        execinfrapb.AggregatorSpec_FIRST_ROW_TS,
				LocalIdxs: []uint32{0, 1},
			},
		},
	},
}
