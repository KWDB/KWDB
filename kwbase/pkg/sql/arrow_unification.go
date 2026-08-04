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
	"encoding/base64"
	"encoding/json"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// noFilterColIdx marks a window function that has no filter clause.
const noFilterColIdx = -1

// containsInt reports whether target is present in list.
func containsInt(list []int, target int) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

// colIdxInts converts a repeated ColIdx field (protobuf uint32) into a plain
// []int for JSON serialization of multi-input aggregates (e.g. FINAL_VARIANCE).
func colIdxInts(cols []uint32) []int {
	out := make([]int, len(cols))
	for i, c := range cols {
		out[i] = int(c)
	}
	return out
}

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
	// OutTypes is the aggregator's raw output column types, serialized via
	// serializeArrowOutType, in the executor's emission order
	// ([group columns..., aggregate results...]). The executor advertises
	// these to downstream operators; the post-process render (e.g.
	// min(ts)::STRING) is applied on top just like the colexec aggregator.
	OutTypes []string `json:"out_types"`
}

type arrowAggExprJS struct {
	Func   string `json:"func"`
	Input  int    `json:"input"`  // -1 for COUNT(*)
	Inputs []int  `json:"inputs"` // multi-input aggs (e.g. final_variance consumes [SQRDIFF, SUM, COUNT])
}

// arrowSupportedCompareType reports whether the Arrow comparison/key kernels
// can handle the given type.
func arrowSupportedCompareType(t types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily, types.BytesFamily, types.DecimalFamily, types.TimestampTZFamily, types.TimestampFamily, types.UuidFamily, types.JsonFamily:
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
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_MIN, execinfrapb.AggregatorSpec_MAX, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_SUM_INT:
			if len(a.ColIdx) != 1 || int(a.ColIdx[0]) >= len(inTypes) {
				return false
			}
			f := inTypes[a.ColIdx[0]].Family()
			switch a.Func {
			case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_SUM_INT:
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
		case execinfrapb.AggregatorSpec_BOOL_AND, execinfrapb.AggregatorSpec_BOOL_OR:
			// BOOL_AND/BOOL_OR are computed by the Arrow min/max kernel over a
			// boolean column (min == AND, max == OR). The input must be a single
			// boolean column. SQL three-valued logic (all-NULL or empty group ->
			// NULL) is preserved because the Arrow min/max accumulator returns
			// null when nothing was set.
			if len(a.ColIdx) != 1 || int(a.ColIdx[0]) >= len(inTypes) {
				return false
			}
			if inTypes[a.ColIdx[0]].Family() != types.BoolFamily {
				return false
			}
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			// COUNT(*) handled via count_all.
		case execinfrapb.AggregatorSpec_SQRDIFF:
			// SQRDIFF (local stage of VARIANCE/STDDEV) is a single numerical
			// input; the Arrow sqrdiff kernel accumulates the running
			// sum-of-squared-differences (Welford) exactly like colexec.
			if len(a.ColIdx) != 1 || int(a.ColIdx[0]) >= len(inTypes) {
				return false
			}
			f := inTypes[a.ColIdx[0]].Family()
			if f != types.IntFamily && f != types.FloatFamily && f != types.DecimalFamily {
				return false
			}
		case execinfrapb.AggregatorSpec_FINAL_VARIANCE, execinfrapb.AggregatorSpec_FINAL_STDDEV:
			// FINAL_VARIANCE / FINAL_STDDEV (final stage of VARIANCE/STDDEV)
			// consume three inputs (SQRDIFF, SUM, COUNT) and merge partitions via
			// the parallel-variance formula, then divide by (count-1) for sample
			// or count for population. All three inputs are numeric.
			if len(a.ColIdx) != 3 {
				return false
			}
			for _, c := range a.ColIdx {
				if int(c) >= len(inTypes) {
					return false
				}
				f := inTypes[c].Family()
				if f != types.IntFamily && f != types.FloatFamily && f != types.DecimalFamily {
					return false
				}
			}
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			// Grouping-column pass-through; the grouping column itself is
			// validated via the GroupCols loop below. No per-row aggregation.
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

func buildArrowAggPlan(spec execinfrapb.AggregatorSpec, outTypes []types.T) arrowAggPlan {
	groupCols := make([]int, len(spec.GroupCols))
	for i, g := range spec.GroupCols {
		groupCols[i] = int(g)
	}
	aggs := make([]arrowAggExprJS, 0, len(spec.Aggregations))
	for _, a := range spec.Aggregations {
		switch a.Func {
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			// Grouping-column pass-through. When the aggregated column is one of
			// the grouping keys it is emitted by the executor as the materialized
			// group-key column, so skip it. When the group key has been elided
			// (e.g. "WHERE b = 7 GROUP BY b" makes b a constant, leaving
			// GroupCols empty) the ANY_NOT_NULL becomes a real output column and
			// must be emitted as an "ident" (pass-through) aggregate so the
			// result column count stays consistent with the planner's spec.
			col := int(a.ColIdx[0])
			if containsInt(groupCols, col) {
				continue
			}
			aggs = append(aggs, arrowAggExprJS{Func: "ident", Input: col})
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT:
			aggs = append(aggs, arrowAggExprJS{Func: "sum", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_MIN:
			aggs = append(aggs, arrowAggExprJS{Func: "min", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_MAX:
			aggs = append(aggs, arrowAggExprJS{Func: "max", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_BOOL_AND:
			// BOOL_AND == AND over booleans == Arrow min over a bool column.
			aggs = append(aggs, arrowAggExprJS{Func: "min", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_BOOL_OR:
			// BOOL_OR == OR over booleans == Arrow max over a bool column.
			aggs = append(aggs, arrowAggExprJS{Func: "max", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_AVG:
			aggs = append(aggs, arrowAggExprJS{Func: "mean", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_COUNT:
			aggs = append(aggs, arrowAggExprJS{Func: "count", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			aggs = append(aggs, arrowAggExprJS{Func: "count_all", Input: -1})
		case execinfrapb.AggregatorSpec_SQRDIFF:
			aggs = append(aggs, arrowAggExprJS{Func: "sqrdiff", Input: int(a.ColIdx[0])})
		case execinfrapb.AggregatorSpec_FINAL_VARIANCE:
			aggs = append(aggs, arrowAggExprJS{Func: "final_variance", Inputs: colIdxInts(a.ColIdx)})
		case execinfrapb.AggregatorSpec_FINAL_STDDEV:
			aggs = append(aggs, arrowAggExprJS{Func: "final_stddev", Inputs: colIdxInts(a.ColIdx)})
		}
	}
	// outTypes is the aggregator's *raw* output column types, in the same order
	// the executor emits them ([group columns..., aggregate results...]). The
	// Arrow processor must advertise these to downstream operators (e.g. a
	// sorter or a downstream render) rather than the post-processed types,
	// exactly like the colexec/rowexec aggregator uses its own outputTypes.
	enc := make([]string, len(outTypes))
	for i, t := range outTypes {
		enc[i] = serializeArrowOutType(t)
	}
	return arrowAggPlan{GroupCols: groupCols, Aggs: aggs, OutTypes: enc}
}

// serializeArrowOutType converts a KWDB type into a stable, JSON-safe string so
// the planner can ship the aggregator's raw output schema to the executor.
func serializeArrowOutType(t types.T) string {
	// base64(proto.Marshal) keeps precision/scale metadata that a plain
	// family-based reconstruction would lose (e.g. DECIMAL(p,s), VARCHAR(n)).
	b, err := t.Marshal()
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

// deserializeArrowOutType reverses serializeArrowOutType.
func deserializeArrowOutType(s string) (types.T, bool) {
	if s == "" {
		return types.T{}, false
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return types.T{}, false
	}
	var t types.T
	if err := t.Unmarshal(b); err != nil {
		return types.T{}, false
	}
	return t, true
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

// arrowJoinKeyType reports whether a column of the given datum type can be used
// as an equality key in an Arrow join. The Arrow join executor hashes/compares
// INT64/FLOAT64/BOOL/STRING/TIMESTAMP/DECIMAL128 arrow arrays (see
// arrow_join.go joinRowHash/arrValEqual), and buildArrowColumns in
// arrow_adapter.go materializes exactly those datum families into the matching
// arrow array types, so the key datum must unify into one of them. Bytes are
// deliberately excluded: a byte/string-family binary column cannot be
// represented as a comparable Arrow primitive, and the former BytesFamily
// allowance was a bug (it produced a degenerate STRING arrow column and
// silently mismatched keys).
func arrowJoinKeyType(t types.T) bool {
	switch t.Family() {
	case types.IntFamily, types.FloatFamily, types.BoolFamily, types.StringFamily,
		types.DecimalFamily, types.TimestampFamily, types.TimestampTZFamily:
		return true
	}
	return false
}

// canArrowMergeJoin reports whether a merge-join currently planned as a
// mergeJoiner can be evaluated by the Arrow compute engine instead. Unlike the
// hash-join path the merge-join path guarantees an ordering on its equality
// columns, but the Arrow (hash) join is order-independent and supports the same
// equi-join + outer-join semantics, so we route a pure equi-merge-join (or an
// inner merge-join with an arrow-evaluable onExpr) through Arrow whenever the
// keys are Arrow-evaluable. The Arrow join does not preserve the merge ordering;
// callers downgrade the output ordering accordingly (matching the standard
// hash-join behavior). The actual plan/core construction is done by the caller
// via buildArrowJoinPlan, exactly as for the hash-join path.
func canArrowMergeJoin(
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

// ---------------------------------------------------------------------------
// Sort (ORDER BY) — §4.2
// ---------------------------------------------------------------------------

// arrowSortPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowSorter.Expr. Columns are stream/plan column indices.
type arrowSortPlan struct {
	// Ordering is the full output ordering: each entry is a column index and a
	// direction (true = ascending, false = descending).
	Ordering []arrowSortCol `json:"ordering"`
	// MatchLen is the length of an already-sorted prefix of Ordering that the
	// sorter may skip (the planner's alreadyOrderedPrefix).
	MatchLen int `json:"match_len"`
	// Limit is the maximum number of rows to keep after sorting; -1 means no
	// limit.
	Limit int64 `json:"limit"`
	// Offset is the number of leading rows to drop after sorting; 0 means none.
	Offset int64 `json:"offset"`
}

type arrowSortCol struct {
	Col int  `json:"col"`
	Asc bool `json:"asc"`
}

// canArrowSort reports whether a sort over the given input types can be
// evaluated by the Arrow compute engine. We only support relational sorts whose
// key columns are Arrow-comparable; limit/offset and a non-zero matchLen are
// both accepted.
func canArrowSort(
	engine tree.EngineType, ordering sqlbase.ColumnOrdering, matchLen int, inTypes []types.T,
) bool {
	if engine == tree.EngineTypeTimeseries {
		return false
	}
	if matchLen < 0 || matchLen > len(ordering) {
		return false
	}
	for _, o := range ordering {
		if o.ColIdx < 0 || int(o.ColIdx) >= len(inTypes) {
			return false
		}
		if !arrowSupportedCompareType(inTypes[o.ColIdx]) {
			return false
		}
	}
	return true
}

// buildArrowSortPlan assembles the JSON plan consumed by the executor.
func buildArrowSortPlan(ordering sqlbase.ColumnOrdering, matchLen int, limit, offset int64) arrowSortPlan {
	ord := make([]arrowSortCol, len(ordering))
	for i, o := range ordering {
		ord[i] = arrowSortCol{Col: int(o.ColIdx), Asc: o.Direction == encoding.Ascending}
	}
	return arrowSortPlan{Ordering: ord, MatchLen: matchLen, Limit: limit, Offset: offset}
}

// ---------------------------------------------------------------------------
// Distinct (dedup) — §4.3
// ---------------------------------------------------------------------------

// arrowDistinctPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowDistinct.Expr. Columns are stream/plan column indices.
type arrowDistinctPlan struct {
	// DistinctCols are the columns that form the dedup key.
	DistinctCols []int `json:"distinct_cols"`
	// OrderedCols is a (possibly empty) subset of DistinctCols; when non-empty
	// the sorter only dedups within each run of equal OrderedCols values.
	OrderedCols []int `json:"ordered_cols"`
}

// canArrowDistinct reports whether a distinct over the given input types can be
// evaluated by the Arrow compute engine. The dedup key columns must be
// Arrow-comparable.
func canArrowDistinct(
	engine tree.EngineType, distinctCols, orderedCols []uint32, inTypes []types.T,
) bool {
	if engine == tree.EngineTypeTimeseries {
		return false
	}
	for _, c := range distinctCols {
		if int(c) >= len(inTypes) || !arrowSupportedCompareType(inTypes[c]) {
			return false
		}
	}
	for _, c := range orderedCols {
		if int(c) >= len(inTypes) || !arrowSupportedCompareType(inTypes[c]) {
			return false
		}
	}
	return true
}

// buildArrowDistinctPlan assembles the JSON plan consumed by the executor.
func buildArrowDistinctPlan(distinctCols, orderedCols []uint32) arrowDistinctPlan {
	dc := make([]int, len(distinctCols))
	for i, c := range distinctCols {
		dc[i] = int(c)
	}
	oc := make([]int, len(orderedCols))
	for i, c := range orderedCols {
		oc[i] = int(c)
	}
	return arrowDistinctPlan{DistinctCols: dc, OrderedCols: oc}
}

// ---------------------------------------------------------------------------
// Window (no-frame partition aggregates) — §4.10
// ---------------------------------------------------------------------------

// arrowWindowerPlan is the JSON-serialized plan carried inside
// ProcessorCoreUnion.ArrowWindower.Expr. Columns are stream/plan column indices.
type arrowWindowerPlan struct {
	// PartitionBy lists the columns that define the window partitions. Rows must
	// arrive ordered by these columns (the planner guarantees this, exactly as
	// the classic windower requires).
	PartitionBy []int `json:"partition_by"`
	// Fns lists the (supported subset of) window functions to compute.
	Fns []arrowWindowFnPlan `json:"fns"`
}

type arrowWindowFnPlan struct {
	// Func is one of sum/count/min/max/avg/bool_and/bool_or.
	Func string `json:"func"`
	// Input is the (single) argument column index.
	Input int `json:"input"`
	// Ordering lists the peer-defining order columns (may be empty, in which
	// case the whole partition is a single peer group).
	Ordering []int `json:"ordering"`
	// OutputIdx is where the window function's result is appended.
	OutputIdx int `json:"output_idx"`
}

// arrowWindowAggFunc maps an AggregatorSpec aggregate function to its Arrow
// window plan string. Returns "" for unsupported functions.
func arrowWindowAggFunc(f execinfrapb.AggregatorSpec_Func) string {
	switch f {
	case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT:
		return "sum"
	case execinfrapb.AggregatorSpec_COUNT, execinfrapb.AggregatorSpec_COUNT_ROWS:
		return "count"
	case execinfrapb.AggregatorSpec_MIN:
		return "min"
	case execinfrapb.AggregatorSpec_MAX:
		return "max"
	case execinfrapb.AggregatorSpec_AVG:
		return "avg"
	case execinfrapb.AggregatorSpec_BOOL_AND:
		return "bool_and"
	case execinfrapb.AggregatorSpec_BOOL_OR:
		return "bool_or"
	}
	return ""
}

// canArrowWindow reports whether the (subset of) window functions described by
// spec can be evaluated by the Arrow compute engine. Supported: every window
// function is a no-frame (default RANGE frame) aggregate over a single argument
// with no filter, mapping to one of sum/count/min/max/avg/bool_and/bool_or, and
// all referenced columns are Arrow-comparable. Everything else (explicit frames,
// ranking functions like row_number/rank, filters, multi-arg aggregates) is
// rejected and falls back to the classic windower.
// isDefaultRangeFrame reports whether the frame is the SQL default window frame
// for an aggregate window function: RANGE BETWEEN UNBOUNDED PRECEDING AND
// CURRENT ROW, with no offset. Over such a frame, the value of the window
// function for a row equals the running aggregate over all rows from the start
// of the partition up to (and including) the current peer group, which is
// exactly the semantics our minimal Arrow windower implements.
func isDefaultRangeFrame(f *execinfrapb.WindowerSpec_Frame) bool {
	if f == nil {
		return true
	}
	if f.Mode != execinfrapb.WindowerSpec_Frame_RANGE {
		return false
	}
	if f.Bounds.Start.BoundType != execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING {
		return false
	}
	if f.Bounds.End == nil || f.Bounds.End.BoundType != execinfrapb.WindowerSpec_Frame_CURRENT_ROW {
		return false
	}
	return true
}

func canArrowWindow(
	engine tree.EngineType, spec *execinfrapb.WindowerSpec, inTypes []types.T,
) bool {
	if engine == tree.EngineTypeTimeseries {
		return false
	}
	for _, c := range spec.PartitionBy {
		if int(c) >= len(inTypes) || !arrowSupportedCompareType(inTypes[c]) {
			return false
		}
	}
	for _, fn := range spec.WindowFns {
		if !isDefaultRangeFrame(fn.Frame) {
			// Only the default RANGE frame (aggregate window functions) is
			// supported in the minimal subset.
			return false
		}
		if fn.FilterColIdx != noFilterColIdx {
			return false
		}
		if fn.Func.AggregateFunc == nil {
			// Non-aggregate (ranking) window functions are not supported.
			return false
		}
		if arrowWindowAggFunc(*fn.Func.AggregateFunc) == "" {
			return false
		}
		if len(fn.ArgsIdxs) != 1 {
			return false
		}
		arg := int(fn.ArgsIdxs[0])
		if arg >= len(inTypes) || !arrowSupportedCompareType(inTypes[arg]) {
			return false
		}
		for _, o := range fn.Ordering.Columns {
			if int(o.ColIdx) >= len(inTypes) || !arrowSupportedCompareType(inTypes[o.ColIdx]) {
				return false
			}
		}
	}
	return true
}

// buildArrowWindowPlan assembles the JSON plan consumed by the executor.
func buildArrowWindowPlan(spec *execinfrapb.WindowerSpec) arrowWindowerPlan {
	pb := make([]int, len(spec.PartitionBy))
	for i, c := range spec.PartitionBy {
		pb[i] = int(c)
	}
	fns := make([]arrowWindowFnPlan, len(spec.WindowFns))
	for i, fn := range spec.WindowFns {
		ord := make([]int, len(fn.Ordering.Columns))
		for j, o := range fn.Ordering.Columns {
			ord[j] = int(o.ColIdx)
		}
		fns[i] = arrowWindowFnPlan{
			Func:      arrowWindowAggFunc(*fn.Func.AggregateFunc),
			Input:     int(fn.ArgsIdxs[0]),
			Ordering:  ord,
			OutputIdx: int(fn.OutputColIdx),
		}
	}
	return arrowWindowerPlan{PartitionBy: pb, Fns: fns}
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

// marshalArrowPlan 统一 Arrow plan 的序列化与降级约定。所有算子在构造
// Arrow core 前都走它：序列化成功返回 (expr, true)，失败返回 (nil, false)，
// 由调用方降级到行式 core。这取代 planner 中散落的
// `arrowUnificationMarshal(plan)` 调用，把「序列化失败即降级」的语义收口到
// 一处，消除 sort/distinct/window/filter/setop 各处不一致的出错处理。
func marshalArrowPlan(plan interface{}) (*execinfrapb.Expression, bool) {
	expr, err := arrowUnificationMarshal(plan)
	if err != nil {
		return nil, false
	}
	return expr, true
}
