// Copyright 2024 The KWDB Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"encoding/json"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

// arrowAggPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowAggregator.Expr. Its JSON shape mirrors the struct
// defined in the rowexec package; the JSON bytes are the only contract between
// the planner and the executor.
type arrowAggPlan struct {
	GroupCols []int            `json:"group_cols"`
	Aggs      []arrowAggExprJS `json:"aggs"`
}

type arrowAggExprJS struct {
	Func  string `json:"func"`
	Input int    `json:"input"` // -1 for COUNT(*)
}

// arrowSupportedCompareType reports whether the Arrow comparison/key kernels
// can handle the given type.
func arrowSupportedCompareType(t types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily, types.BytesFamily, types.DecimalFamily, types.TimestampTZFamily, types.TimestampFamily:
		return true
	}
	return false
}

// canArrowAggregate reports whether the final aggregator spec can be evaluated
// entirely by the Arrow compute engine (sum/min/max/mean(count avg)/count over
// int/float/decimal columns, count(*) over rows, with int/float/bool/string/
// decimal grouping columns, no distinct). The mean (AVG) kernel emits DECIMAL
// for integer/decimal inputs and FLOAT for floating-point inputs, matching
// SQL's AVG return types.
func canArrowAggregate(
	spec execinfrapb.AggregatorSpec, inTypes []types.T, engine tree.EngineType,
) bool {
	if engine == tree.EngineTypeTimeseries {
		return false
	}
	for _, a := range spec.Aggregations {
		if a.Distinct {
			return false
		}
		switch a.Func {
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_MIN, execinfrapb.AggregatorSpec_MAX, execinfrapb.AggregatorSpec_AVG:
			if len(a.ColIdx) != 1 || int(a.ColIdx[0]) >= len(inTypes) {
				return false
			}
			f := inTypes[a.ColIdx[0]].Family()
			switch a.Func {
			case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_AVG:
				// SUM/AVG are only meaningful for numeric inputs.
				if f != types.IntFamily && f != types.FloatFamily && f != types.DecimalFamily {
					return false
				}
			case execinfrapb.AggregatorSpec_MIN, execinfrapb.AggregatorSpec_MAX:
				// MIN/MAX are valid for any orderable type: numbers, decimals,
				// and timestamps.
				if f != types.IntFamily && f != types.FloatFamily && f != types.DecimalFamily &&
					f != types.TimestampTZFamily && f != types.TimestampFamily {
					return false
				}
			}

		case execinfrapb.AggregatorSpec_COUNT:
			if len(a.ColIdx) != 1 || int(a.ColIdx[0]) >= len(inTypes) {
				return false
			}
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			// COUNT(*) handled via count_all.
		default:
			return false
		}
	}
	for _, g := range spec.GroupCols {
		if int(g) >= len(inTypes) {
			return false
		}
		if !arrowSupportedCompareType(inTypes[g]) {
			return false
		}
	}
	return true
}

func buildArrowAggPlan(spec execinfrapb.AggregatorSpec) arrowAggPlan {
	groupCols := make([]int, len(spec.GroupCols))
	for i, g := range spec.GroupCols {
		groupCols[i] = int(g)
	}
	aggs := make([]arrowAggExprJS, len(spec.Aggregations))
	for i, a := range spec.Aggregations {
		switch a.Func {
		case execinfrapb.AggregatorSpec_SUM:
			aggs[i] = arrowAggExprJS{Func: "sum", Input: int(a.ColIdx[0])}
		case execinfrapb.AggregatorSpec_MIN:
			aggs[i] = arrowAggExprJS{Func: "min", Input: int(a.ColIdx[0])}
		case execinfrapb.AggregatorSpec_MAX:
			aggs[i] = arrowAggExprJS{Func: "max", Input: int(a.ColIdx[0])}
		case execinfrapb.AggregatorSpec_AVG:
			aggs[i] = arrowAggExprJS{Func: "mean", Input: int(a.ColIdx[0])}
		case execinfrapb.AggregatorSpec_COUNT:
			aggs[i] = arrowAggExprJS{Func: "count", Input: int(a.ColIdx[0])}
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			aggs[i] = arrowAggExprJS{Func: "count_all", Input: -1}
		}
	}
	return arrowAggPlan{GroupCols: groupCols, Aggs: aggs}
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

// arrowJoinPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowJoin.Expr.
type arrowJoinPlan struct {
	LeftKeys  []int  `json:"left_keys"`
	RightKeys []int  `json:"right_keys"`
	Type      string `json:"type"` // "inner", "left", "right" or "full"
	// OnFilter is the JSON-serialized Arrow filter plan for a non-equi onExpr
	// (§7.4). When non-empty the executor applies it as a post-filter over the
	// equi-join result. Its JSON shape mirrors arrowFilterPlan in the rowexec
	// package; the JSON bytes are the only contract between planner and executor.
	OnFilter string `json:"on_filter,omitempty"`
}

// arrowJoinInputCols returns the combined index-variable map for an arrow join's
// (left ++ right) input columns. It is used to gate and to plan a non-equi
// onExpr (§7.4): the left input occupies indices 0..nL-1 and the right input
// occupies indices nL..nL+nR-1, matching the merged Arrow record column layout.
func arrowJoinInputCols(nL, nR int) []int {
	idx := make([]int, 0, nL+nR)
	for i := 0; i < nL; i++ {
		idx = append(idx, i)
	}
	for j := 0; j < nR; j++ {
		idx = append(idx, nL+j)
	}
	return idx
}

func arrowJoinType(jt sqlbase.JoinType) (string, bool) {
	switch jt {
	case sqlbase.JoinType_INNER:
		return "inner", true
	case sqlbase.JoinType_LEFT_OUTER:
		return "left", true
	case sqlbase.JoinType_RIGHT_OUTER:
		return "right", true
	case sqlbase.JoinType_FULL_OUTER:
		return "full", true
	}
	return "", false
}

func arrowJoinKeyType(t types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily, types.BytesFamily:
		return true
	}
	return false
}

// canArrowJoin reports whether the equi-join described by (leftEq, rightEq) over
// the given join type and input column types can be evaluated by the Arrow
// compute engine. The equi-join keys must have an Arrow-supported datum type.
// A non-equi onExpr (if any) is handled separately by the planner: for inner
// joins it becomes a post-filter stage inside the arrow join processor, and for
// other join kinds the planner keeps the standard join engine.
func canArrowJoin(
	engine tree.EngineType,
	leftEq, rightEq []uint32,
	joinType sqlbase.JoinType,
	leftTypes, rightTypes []types.T,
) bool {
	if engine == tree.EngineTypeTimeseries {
		return false
	}
	if _, ok := arrowJoinType(joinType); !ok {
		return false
	}
	if len(leftEq) == 0 || len(leftEq) != len(rightEq) {
		return false
	}
	for _, c := range leftEq {
		if int(c) >= len(leftTypes) || !arrowJoinKeyType(leftTypes[c]) {
			return false
		}
	}
	for _, c := range rightEq {
		if int(c) >= len(rightTypes) || !arrowJoinKeyType(rightTypes[c]) {
			return false
		}
	}
	return true
}

// buildArrowJoinPlan assembles the JSON plan consumed by the executor. onFilter
// is the JSON-serialized non-equi onExpr filter plan (§7.4), or "" when the join
// has none.
func buildArrowJoinPlan(leftEq, rightEq []uint32, joinType sqlbase.JoinType, onFilter string) arrowJoinPlan {
	lk := make([]int, len(leftEq))
	for i, c := range leftEq {
		lk[i] = int(c)
	}
	rk := make([]int, len(rightEq))
	for i, c := range rightEq {
		rk[i] = int(c)
	}
	jt, _ := arrowJoinType(joinType)
	return arrowJoinPlan{LeftKeys: lk, RightKeys: rk, Type: jt, OnFilter: onFilter}
}

// arrowUnificationMarshal marshals an arrow plan struct into the Expression
// payload used by the processor cores.
func arrowUnificationMarshal(plan interface{}) (*execinfrapb.Expression, error) {
	b, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	return &execinfrapb.Expression{Expr: string(b)}, nil
}

var _ = physicalplan.ArrowAggregatorEnabled
