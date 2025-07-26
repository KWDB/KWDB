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

package opt

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Operator describes the type of operation that a memo expression performs.
// Some operators are relational (join, select, project) and others are scalar
// (and, or, plus, variable).
type Operator uint16

// String returns the name of the operator as a string.
func (op Operator) String() string {
	if op >= Operator(len(opNames)-1) {
		return fmt.Sprintf("Operator(%d)", op)
	}
	return opNames[opNameIndexes[op]:opNameIndexes[op+1]]
}

// SyntaxTag returns the name of the operator using the SQL syntax that most
// closely matches it.
func (op Operator) SyntaxTag() string {
	// Handle any special cases where default codegen tag isn't best choice as
	// switch cases.
	switch op {
	default:
		// Use default codegen tag, which is mechanically derived from the
		// operator name.
		if op >= Operator(len(opNames)-1) {
			// Use UNKNOWN.
			op = 0
		}
		return opSyntaxTags[opSyntaxTagIndexes[op]:opSyntaxTagIndexes[op+1]]
	}
}

// ExprParam is the interface for walk through the expr tree
// consider to remove it and its references if possible
// used check white list and parallel execute
type ExprParam interface {
	// IsTargetExpr is used to identify the target expr to handle
	IsTargetExpr(self Expr) bool

	// NeedToHandleChild is used to check if children expr should be dealt with
	NeedToHandleChild() bool

	// HandleChildExpr deals with child expr
	HandleChildExpr(parent Expr, child Expr) bool
}

// Expr is a node in an expression tree. It offers methods to traverse and
// inspect the tree. Each node in the tree has an enumerated operator type, zero
// or more children, and an optional private value. The entire tree can be
// easily visited using a pattern like this:
//
//	var visit func(e Expr)
//	visit := func(e Expr) {
//	  for i, n := 0, e.ChildCount(); i < n; i++ {
//	    visit(e.Child(i))
//	  }
//	}
type Expr interface {
	// Op returns the operator type of the expression.
	Op() Operator

	// ChildCount returns the number of children of the expression.
	ChildCount() int

	// Child returns the nth child of the expression.
	Child(nth int) Expr

	// Walk all interface
	Walk(param ExprParam) bool

	// Private returns operator-specific data. Callers are expected to know the
	// type and format of the data, which will differ from operator to operator.
	// For example, an operator may choose to return one of its fields, or perhaps
	// a pointer to itself, or nil if there is nothing useful to return.
	Private() interface{}

	// String returns a human-readable string representation for the expression
	// that can be used for debugging and testing.
	String() string

	// IsTSEngine indicates whether exec on ts engine is supported
	IsTSEngine() bool

	// SetEngineTS set expr can push to ts engine
	SetEngineTS()

	// SimpleString returns a human-readable string representation for the expression
	// that can be used for debugging and testing. user can set flag
	SimpleString(flag int) string
}

// Exprs expr array
type Exprs []Expr

// ScalarID is the type of the memo-unique identifier given to every scalar
// expression.
type ScalarID int

// ScalarExpr is a scalar expression, which is an expression that returns a
// primitive-typed value like boolean or string rather than rows and columns.
type ScalarExpr interface {
	Expr

	// ID is a unique (within the context of a memo) ID that can be
	// used to define a total order over ScalarExprs.
	ID() ScalarID

	// DataType is the SQL type of the expression.
	DataType() *types.T

	// CheckConstDeductionEnabled check IsConstForLogicPlan
	CheckConstDeductionEnabled() bool

	// SetConstDeductionEnabled set IsConstForLogicPlan
	SetConstDeductionEnabled(flag bool)

	// Rebuild rebuild expr
	Rebuild() ScalarExpr
}

// MutableExpr is implemented by expressions that allow their children to be
// updated.
type MutableExpr interface {
	// SetChild updates the nth child of the expression to instead be the given
	// child expression.
	SetChild(nth int, child Expr)
}

// ComparisonOpMap maps from a semantic tree comparison operator type to an
// optimizer operator type.
var ComparisonOpMap [tree.NumComparisonOperators]Operator

// ComparisonOpReverseMap maps from an optimizer operator type to a semantic
// tree comparison operator type.
var ComparisonOpReverseMap = map[Operator]tree.ComparisonOperator{
	EqOp:             tree.EQ,
	LtOp:             tree.LT,
	GtOp:             tree.GT,
	LeOp:             tree.LE,
	GeOp:             tree.GE,
	NeOp:             tree.NE,
	InOp:             tree.In,
	NotInOp:          tree.NotIn,
	LikeOp:           tree.Like,
	NotLikeOp:        tree.NotLike,
	ILikeOp:          tree.ILike,
	NotILikeOp:       tree.NotILike,
	SimilarToOp:      tree.SimilarTo,
	NotSimilarToOp:   tree.NotSimilarTo,
	RegMatchOp:       tree.RegMatch,
	NotRegMatchOp:    tree.NotRegMatch,
	RegIMatchOp:      tree.RegIMatch,
	NotRegIMatchOp:   tree.NotRegIMatch,
	IsOp:             tree.IsNotDistinctFrom,
	IsNotOp:          tree.IsDistinctFrom,
	ContainsOp:       tree.Contains,
	JsonExistsOp:     tree.JSONExists,
	JsonSomeExistsOp: tree.JSONSomeExists,
	JsonAllExistsOp:  tree.JSONAllExists,
	OverlapsOp:       tree.Overlaps,
}

// BinaryOpReverseMap maps from an optimizer operator type to a semantic tree
// binary operator type.
var BinaryOpReverseMap = map[Operator]tree.BinaryOperator{
	BitandOp:        tree.Bitand,
	BitorOp:         tree.Bitor,
	BitxorOp:        tree.Bitxor,
	PlusOp:          tree.Plus,
	MinusOp:         tree.Minus,
	MultOp:          tree.Mult,
	DivOp:           tree.Div,
	FloorDivOp:      tree.FloorDiv,
	ModOp:           tree.Mod,
	PowOp:           tree.Pow,
	ConcatOp:        tree.Concat,
	LShiftOp:        tree.LShift,
	RShiftOp:        tree.RShift,
	FetchValOp:      tree.JSONFetchVal,
	FetchTextOp:     tree.JSONFetchText,
	FetchValPathOp:  tree.JSONFetchValPath,
	FetchTextPathOp: tree.JSONFetchTextPath,
}

// UnaryOpReverseMap maps from an optimizer operator type to a semantic tree
// unary operator type.
var UnaryOpReverseMap = map[Operator]tree.UnaryOperator{
	UnaryMinusOp:      tree.UnaryMinus,
	UnaryComplementOp: tree.UnaryComplement,
}

// AggregateOpReverseMap maps from an optimizer operator type to the name of an
// aggregation function.
var AggregateOpReverseMap = map[Operator]string{
	ArrayAggOp:          "array_agg",
	AvgOp:               "avg",
	BitAndAggOp:         "bit_and",
	BitOrAggOp:          "bit_or",
	BoolAndOp:           "bool_and",
	BoolOrOp:            "bool_or",
	ConcatAggOp:         "concat_agg",
	CountOp:             "count",
	CorrOp:              "corr",
	CountRowsOp:         "count_rows",
	MaxOp:               "max",
	MinOp:               "min",
	FirstOp:             "first",
	FirstTimeStampOp:    "firstts",
	FirstRowOp:          "first_row",
	FirstRowTimeStampOp: "first_row_ts",
	LastOp:              "last",
	LastTimeStampOp:     "lastts",
	LastRowOp:           "last_row",
	LastRowTimeStampOp:  "last_row_ts",
	MatchingOp:          "matching",
	SumIntOp:            "sum_int",
	SumOp:               "sum",
	SqrDiffOp:           "sqrdiff",
	VarianceOp:          "variance",
	StdDevOp:            "stddev",
	XorAggOp:            "xor_agg",
	JsonAggOp:           "json_agg",
	JsonbAggOp:          "jsonb_agg",
	StringAggOp:         "string_agg",
	ConstAggOp:          "any_not_null",
	ConstNotNullAggOp:   "any_not_null",
	AnyNotNullAggOp:     "any_not_null",
	TimeBucketGapfillOp: "time_bucket_gapfill_internal",
	ImputationOp:        "interpolate",
	ElapsedOp:           "elapsed",
	TwaOp:               "twa",
	MinExtendOp:         "min_extend",
	MaxExtendOp:         "max_extend",
}

// WindowOpReverseMap maps from an optimizer operator type to the name of a
// window function.
var WindowOpReverseMap = map[Operator]string{
	RankOp:        "rank",
	RowNumberOp:   "row_number",
	DenseRankOp:   "dense_rank",
	PercentRankOp: "percent_rank",
	CumeDistOp:    "cume_dist",
	NtileOp:       "ntile",
	LagOp:         "lag",
	LeadOp:        "lead",
	FirstValueOp:  "first_value",
	LastValueOp:   "last_value",
	NthValueOp:    "nth_value",
	DiffOp:        "diff",
}

// NegateOpMap maps from a comparison operator type to its negated operator
// type, as if the Not operator was applied to it. Some comparison operators,
// like Contains and JsonExists, do not have negated versions.
var NegateOpMap = map[Operator]Operator{
	EqOp:           NeOp,
	LtOp:           GeOp,
	GtOp:           LeOp,
	LeOp:           GtOp,
	GeOp:           LtOp,
	NeOp:           EqOp,
	InOp:           NotInOp,
	NotInOp:        InOp,
	LikeOp:         NotLikeOp,
	NotLikeOp:      LikeOp,
	ILikeOp:        NotILikeOp,
	NotILikeOp:     ILikeOp,
	SimilarToOp:    NotSimilarToOp,
	NotSimilarToOp: SimilarToOp,
	RegMatchOp:     NotRegMatchOp,
	NotRegMatchOp:  RegMatchOp,
	RegIMatchOp:    NotRegIMatchOp,
	NotRegIMatchOp: RegIMatchOp,
	IsOp:           IsNotOp,
	IsNotOp:        IsOp,
}

// BoolOperatorRequiresNotNullArgs returns true if the operator can never
// evaluate to true if one of its children is NULL.
func BoolOperatorRequiresNotNullArgs(op Operator) bool {
	switch op {
	case
		EqOp, LtOp, LeOp, GtOp, GeOp, NeOp,
		LikeOp, NotLikeOp, ILikeOp, NotILikeOp, SimilarToOp, NotSimilarToOp,
		RegMatchOp, NotRegMatchOp, RegIMatchOp, NotRegIMatchOp:
		return true
	}
	return false
}

// AggregateIgnoresNulls returns true if the given aggregate operator ignores
// rows where its first argument evaluates to NULL. In other words, it always
// evaluates to the same result even if those rows are filtered. For example:
//
//	SELECT string_agg(x, y)
//	FROM (VALUES ('foo', ','), ('bar', ','), (NULL, ',')) t(x, y)
//
// In this example, the NULL row can be removed from the input, and the
// string_agg function still returns the same result. Contrast this to the
// array_agg function:
//
//	SELECT array_agg(x)
//	FROM (VALUES ('foo'), (NULL), ('bar')) t(x)
//
// If the NULL row is removed here, array_agg returns {foo,bar} instead of
// {foo,NULL,bar}.
func AggregateIgnoresNulls(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, AvgOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConstNotNullAggOp, CorrOp, CountOp, ElapsedOp, FirstOp, FirstTimeStampOp, FirstRowOp, FirstRowTimeStampOp,
		LastRowTimeStampOp, LastOp, LastTimeStampOp, LastRowOp, MatchingOp, MaxOp, MaxExtendOp, MinOp, MinExtendOp,
		SqrDiffOp, StdDevOp, StringAggOp, SumOp, SumIntOp, TimeBucketGapfillOp, TwaOp, ImputationOp, VarianceOp, XorAggOp:
		return true

	case ArrayAggOp, ConcatAggOp, ConstAggOp, CountRowsOp, FirstAggOp, JsonAggOp,
		JsonbAggOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNullOnEmpty returns true if the given aggregate operator returns
// NULL when the input set contains no values. This group of aggregates turns
// out to be the inverse of AggregateIsNeverNull in practice.
func AggregateIsNullOnEmpty(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConcatAggOp, ConstAggOp, ConstNotNullAggOp, CorrOp, ElapsedOp, FirstAggOp, JsonAggOp, JsonbAggOp,
		MaxOp, MaxExtendOp, MatchingOp, MinOp, MinExtendOp, FirstOp, FirstTimeStampOp, FirstRowOp, FirstRowTimeStampOp,
		LastRowTimeStampOp, LastOp, LastTimeStampOp, LastRowOp, SqrDiffOp, StdDevOp, StringAggOp,
		SumOp, SumIntOp, TimeBucketGapfillOp, TwaOp, ImputationOp, VarianceOp, XorAggOp:
		return true

	case CountOp, CountRowsOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNeverNullOnNonNullInput returns true if the given aggregate
// operator never returns NULL when the input set contains at least one non-NULL
// value. This is true of most aggregates.
//
// For multi-input aggregations, returns true if the aggregate is never NULL
// when all inputs have at least a non-NULL value (though not necessarily on the
// same input row).
func AggregateIsNeverNullOnNonNullInput(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp,
		BitOrAggOp, BoolAndOp, BoolOrOp, ConcatAggOp, ConstAggOp,
		ConstNotNullAggOp, CountOp, CountRowsOp, FirstAggOp,
		JsonAggOp, JsonbAggOp, MaxOp, MatchingOp, MinOp,
		FirstOp, FirstTimeStampOp, FirstRowOp, FirstRowTimeStampOp,
		LastRowTimeStampOp, LastOp, LastTimeStampOp, LastRowOp, SqrDiffOp,
		StringAggOp, SumOp, SumIntOp, TimeBucketGapfillOp, ImputationOp, XorAggOp, ElapsedOp, TwaOp, MaxExtendOp, MinExtendOp:
		return true

	case VarianceOp, StdDevOp, CorrOp:
		// These aggregations return NULL if they are given a single not-NULL input.
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNeverNull returns true if the given aggregate operator never
// returns NULL, even if the input is empty, or one more more inputs are NULL.
func AggregateIsNeverNull(op Operator) bool {
	switch op {
	case CountOp, CountRowsOp:
		return true
	}
	return false
}

// OpaqueMetadata is an object stored in OpaqueRelExpr and passed
// through to the exec factory.
type OpaqueMetadata interface {
	ImplementsOpaqueMetadata()

	// String is a short description used when printing optimizer trees and when
	// forming error messages; it should be the SQL statement tag.
	String() string
}

func init() {
	for optOp, treeOp := range ComparisonOpReverseMap {
		ComparisonOpMap[treeOp] = optOp
	}
}
